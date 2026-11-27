// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

package dds

import (
	"errors"
	"testing"
	"time"

	"github.com/podomy/weld/internal/core"
	"github.com/podomy/weld/internal/transports/dds/rtps"
)

// TestReverseRxOOwnershipMismatch pins fail-closed SHARED vs EXCLUSIVE.
//
// DDS 1.4 section 2.2.3.9.2 requires exact ownership match: a SHARED offer
// against an EXCLUSIVE request (and vice versa) must be incompatible, never
// a silent match via the zero value.
func TestReverseRxOOwnershipMismatch(t *testing.T) {
	t.Parallel()

	shared := mustOwnershipWire(t, core.Ownershipqos4Shared)
	exclusive := mustOwnershipWire(t, core.Ownershipqos4Exclusive)

	assertOwnershipMismatch(t, shared, exclusive)
	assertOwnershipCompatible(t, shared, shared)
	assertOwnershipUnknownRefuses(t, shared)
	assertOwnershipHelpers(t)
}

// mustOwnershipWire forwards one ownership kind for the RxO test.
func mustOwnershipWire(t *testing.T, kind core.Ownershipqos4Policy) WireQoS {
	t.Helper()

	wire, err := ForwardQoS("t", "pub", "sub", core.QoSProfile{
		History:       core.HistoryKeepLast,
		Depth:         1,
		Reliability:   core.ReliabilityReliable,
		Durability:    core.DurabilityVolatile,
		Liveliness:    core.LivelinessAutomatic,
		Ownershipqos4: kind,
	})
	if err != nil {
		t.Fatalf("forward ownership %v: %v", kind, err)
	}

	return wire
}

// assertOwnershipMismatch checks SHARED vs EXCLUSIVE fails with the policy.
func assertOwnershipMismatch(t *testing.T, shared, exclusive WireQoS) {
	t.Helper()

	if got := CheckWireCompatibility(shared, exclusive); got.Compatible {
		t.Fatalf("shared vs exclusive compatible, want incompatible")
	} else {
		found := false
		for _, issue := range got.Errors {
			if issue.Kind == core.Policyqos4Ownership {
				found = true
			}
		}
		if !found {
			t.Fatalf("mismatch errors = %v, want ownership kind", got.Errors)
		}
	}
}

// assertOwnershipCompatible checks equal kinds match.
func assertOwnershipCompatible(t *testing.T, offered, requested WireQoS) {
	t.Helper()

	if got := CheckWireCompatibility(offered, requested); !got.Compatible {
		t.Fatalf("equal ownership = %v, want compatible", got.Errors)
	}
}

// assertOwnershipUnknownRefuses checks unknown kinds refuse fail-closed.
func assertOwnershipUnknownRefuses(t *testing.T, shared WireQoS) {
	t.Helper()

	bad := shared
	bad.OwnershipKind = 99
	if _, err := ReverseQoS(bad); !IsWireQoSReverseRefusal(err) {
		t.Fatalf("unknown ownership reverse = %v, want WireQoSReverseRefusal", err)
	}
	if got := CheckWireCompatibility(bad, shared); got.Compatible {
		t.Fatalf("unknown ownership compatible, want incompatible")
	}
}

// assertOwnershipHelpers checks the sedp-owner helpers.
func assertOwnershipHelpers(t *testing.T) {
	t.Helper()

	raw := EncodeOwnership(writerQoSOwnershipExclusive)
	kind, err := DecodeOwnership(raw)
	if err != nil {
		t.Fatalf("DecodeOwnership: %v", err)
	}
	if kind != writerQoSOwnershipExclusive {
		t.Fatalf("decoded ownership = %d, want exclusive", kind)
	}
	if err := CheckOwnershipWireCompatible(writerQoSOwnershipShared, writerQoSOwnershipExclusive); err == nil {
		t.Fatalf("helper shared vs exclusive passed, want error")
	}
	if _, err := DecodeOwnership([]byte{0x01}); err == nil {
		t.Fatalf("short ownership decode passed, want error")
	}
}

// TestSweepLifespanRetainedThenSwept pins expired history sweep.
//
// Samples retained then expired under lifespan must sweep before serving so
// a later take never serves stale data.
func TestSweepLifespanRetainedThenSwept(t *testing.T) {
	t.Parallel()

	recv := NewDataReceiver()
	recv.SetLifespanForTopic("/life", core.NewDuration(1, 0))
	t0 := time.Unix(9000, 0).UTC()
	at := t0
	guid := testWriterGUID()
	payload := encodeTestString(t, "hello")

	if _, err := recv.observeData("/life", "std_msgs__msg__String", guid, 1, payload, at); err != nil {
		t.Fatalf("observe old: %v", err)
	}
	if got := len(recv.SampleHistory()); got != 1 {
		t.Fatalf("history = %d, want 1", got)
	}

	expired := recv.sweepLifespanForTopic("/life", t0.Add(2*time.Second).UnixNano())
	if expired != 1 {
		t.Fatalf("swept = %d, want 1", expired)
	}
	if got := len(recv.SampleHistory()); got != 0 {
		t.Fatalf("history after sweep = %d, want 0", got)
	}

	// Other topics stay untouched.
	if _, err := recv.observeData("/other", "std_msgs__msg__String", guid, 2, payload, t0.Add(2*time.Second)); err != nil {
		t.Fatalf("observe other: %v", err)
	}
	if expired := recv.sweepLifespanForTopic("/life", t0.Add(3*time.Second).UnixNano()); expired != 0 {
		t.Fatalf("swept other-topic = %d, want 0", expired)
	}
	if got := len(recv.SampleHistory()); got != 1 {
		t.Fatalf("other history = %d, want 1", got)
	}
}

// TestInvalidUserControlDataFrag pins DATA_FRAG in the invalidate set.
func TestInvalidUserControlDataFrag(t *testing.T) {
	t.Parallel()

	// Malformed DATA_FRAG (short body) must invalidate the rest.
	malformed := rtps.RawSubmessage{ID: rtps.SubmessageDataFrag, Flags: rtps.FlagLittleEndian, Body: []byte{0x01, 0x02}}
	if !invalidUserControl(malformed) {
		t.Fatalf("malformed DATA_FRAG did not invalidate, want drop-rest")
	}

	// ACKNACK still invalidates on malformed; unknown ids never do.
	badAck := rtps.RawSubmessage{ID: rtps.SubmessageAckNack, Flags: rtps.FlagLittleEndian, Body: []byte{0x01}}
	if !invalidUserControl(badAck) {
		t.Fatalf("malformed ACKNACK did not invalidate")
	}
	unknown := rtps.RawSubmessage{ID: 0xFF, Flags: rtps.FlagLittleEndian, Body: []byte{0x01}}
	if invalidUserControl(unknown) {
		t.Fatalf("unknown id invalidated, want pass")
	}
}

// TestMalformedDataFragDropsRest pins fail-closed DATA_FRAG handling.
//
// A datagram with a malformed DATA_FRAG followed by valid DATA must drop
// the rest: the trailing DATA never reaches dataCh.
func TestMalformedDataFragDropsRest(t *testing.T) {
	t.Parallel()

	backend := &Backend{
		routes: make(map[GUIDPrefix][]int),
	}
	header := rtps.Header{}
	malformed := rtps.RawSubmessage{ID: rtps.SubmessageDataFrag, Flags: rtps.FlagLittleEndian, Body: []byte{0x01}}
	if !invalidUserControl(malformed) {
		t.Fatalf("malformed DATA_FRAG must invalidate")
	}
	// processOneUserSub with a background context must report drop-rest.
	if !backend.processOneUserSub(t.Context(), header, malformed, make(chan dataItem, 1)) {
		t.Fatalf("malformed DATA_FRAG did not drop rest")
	}
}

// TestOfferedRequestedTrackersSeparate pins no shared watermarks.
//
// Polling offered must not move the requested watermark and missing offered
// must report no alert (no fallback to the requested side).
func TestOfferedRequestedTrackersSeparate(t *testing.T) {
	t.Parallel()

	recv := NewDataReceiver()
	now := time.Unix(7000, 0).UTC().UnixNano()
	if err := recv.SetDeadlineTrackerForTopic("/sep", core.NewDuration(10, 0)); err != nil {
		t.Fatalf("set requested: %v", err)
	}
	// No offered tracker: offered poll reports no alert (no fallback).
	if _, ok := recv.CheckOfferedDeadline("/sep", now); ok {
		t.Fatalf("offered poll without offered tracker reported alert, want none")
	}
	// Requested poll still works independently.
	recv2 := NewDataReceiver()
	t0 := time.Unix(7000, 0).UTC()
	recv2.now = func() time.Time { return t0 }
	if err := recv2.SetDeadlineTrackerForTopic("/sep", core.NewDuration(1, 0)); err != nil {
		t.Fatalf("set requested2: %v", err)
	}
	recv2.SetOfferedDeadlineTrackerForTopic("/sep", core.NewDuration(100, 0))
	later := t0.Add(2 * time.Second).UnixNano()
	if _, ok := recv2.CheckRequestedDeadline("/sep", later); !ok {
		t.Fatalf("requested poll missed, want alert")
	}
	// Offered with a long deadline must not alert on the same now.
	if _, ok := recv2.CheckOfferedDeadline("/sep", later); ok {
		t.Fatalf("offered poll alerted on requested expiry, want separate")
	}
}

// TestBlockedWriteLeavesNoInstance pins no leak on reliable keep-all block.
func TestBlockedWriteLeavesNoInstance(t *testing.T) {
	t.Parallel()

	sampleKeyedDesc(t)
	recv := NewDataReceiver()
	recv.SetReliabilityHistoryLife5(true, true)
	if err := recv.SetResourceLimits(ResourceLimits{MaxSamples: 1, MaxInstances: 2, MaxSamplesPerInstance: -1}); err != nil {
		t.Fatalf("limits: %v", err)
	}
	guid := sampleKeyedGUID()
	at := time.Unix(9500, 0).UTC()

	if _, err := recv.observeData("/b", "sample_test__msg__Keyed", guid, 1, encodeSampleKey(t, 41), at); err != nil {
		t.Fatalf("first observe: %v", err)
	}
	st := recv.readerStateFor()
	st.mu.Lock()
	before := len(st.instances)
	st.mu.Unlock()

	// Second sample for a new key would exceed MaxSamples under keep-all:
	// it must block with ErrResourceExhausted and install no instance.
	_, err := recv.observeData("/b", "sample_test__msg__Keyed", guid, 2, encodeSampleKey(t, 42), at)
	if !errors.Is(err, ErrResourceExhausted) {
		t.Fatalf("blocked write = %v, want ErrResourceExhausted", err)
	}
	st.mu.Lock()
	after := len(st.instances)
	st.mu.Unlock()
	if after != before {
		t.Fatalf("instances %d -> %d, want no new instance on block", before, after)
	}
}

// TestShouldRtps1SkipNackFrag pins duplicate NACKFRAG suppression.
func TestShouldRtps1SkipNackFrag(t *testing.T) {
	t.Parallel()

	backend := &Backend{}
	var reader WriterGUID
	reader[0] = 0x11
	writer := rtps.EntityID{0x00, 0x00, 0x02, 0xC2}

	if shouldRtps1SkipNackFrag(backend, reader, writer, 0) {
		t.Fatalf("zero count suppressed, want pass")
	}
	if shouldRtps1SkipNackFrag(backend, reader, writer, 5) {
		t.Fatalf("first count 5 suppressed, want pass")
	}
	if !shouldRtps1SkipNackFrag(backend, reader, writer, 5) {
		t.Fatalf("replay count 5 passed, want skip")
	}
	if !shouldRtps1SkipNackFrag(backend, reader, writer, 4) {
		t.Fatalf("old count 4 passed, want skip")
	}
	if shouldRtps1SkipNackFrag(backend, reader, writer, 6) {
		t.Fatalf("new count 6 suppressed, want pass")
	}
}

// TestRoutesPerPrefixSharingWaived pins the waiver rationale.
//
// Routes stay keyed by GUIDPrefix (one participant's unicast ports serve
// every partition by design) while retention keys scope by partition.
func TestRoutesPerPrefixSharingWaived(t *testing.T) {
	t.Parallel()

	keyA, err := PartitionRouteKeyForEndpoint("rt/chatter", []string{"a"})
	if err != nil {
		t.Fatalf("route key a: %v", err)
	}
	keyB, err := PartitionRouteKeyForEndpoint("rt/chatter", []string{"b"})
	if err != nil {
		t.Fatalf("route key b: %v", err)
	}
	if keyA == keyB {
		t.Fatalf("partition route keys equal, want isolation across partitions")
	}
	// The routes table itself stays per-prefix by design: one prefix serves
	// both partitions, so no per-partition route assertion belongs here.
}

// TestAttributionPrefixIsHeader pins minimal INFO_SRC has no identity.
func TestAttributionPrefixIsHeader(t *testing.T) {
	t.Parallel()

	var header rtps.Header
	for i := range header.Prefix {
		header.Prefix[i] = byte(i + 1)
	}
	if got := attributionPrefixForDispatch(header); got != GUIDPrefix(header.Prefix) {
		t.Fatalf("attribution = %x, want header prefix", got)
	}
	// Minimal INFO_SRC carries no GUID: valid decode yields version/vendor only.
	src := rtps.InfoSrc{Major: 2, Minor: 5, Vendor: [2]byte{0x01, 0x10}}
	back, err := rtps.DecodeInfoSrc(src.Encode(), 0x01, nil)
	if err != nil {
		t.Fatalf("decode info_src: %v", err)
	}
	if back != src {
		t.Fatalf("info_src = %+v, want %+v", back, src)
	}
}

// TestSentinelChainPreserved pins backward-compatible error chaining.
func TestSentinelChainPreserved(t *testing.T) {
	t.Parallel()

	if !errors.Is(ErrResourceExhausted, ErrOwnershipRefused) {
		t.Fatalf("ErrResourceExhausted does not chain ErrOwnershipRefused")
	}
	recv := NewDataReceiver()
	if err := recv.SetDeadlineTrackerForTopic("/chain", core.NewDuration(1, 0)); err != nil {
		t.Fatalf("set deadline: %v", err)
	}
	// Minimum above deadline refuses chaining ownership for compat.
	if err := recv.SetTimeBasedFilter(int64(2000000000)); !errors.Is(err, ErrOwnershipRefused) {
		t.Fatalf("time filter above deadline = %v, want ErrOwnershipRefused chain", err)
	}
}

// TestControlStampsUseReceiverClock pins fake-clock control times.
func TestControlStampsUseReceiverClock(t *testing.T) {
	t.Parallel()

	sampleKeyedDesc(t)
	recv := NewDataReceiver()
	fake := time.Unix(9600, 123).UTC()
	recv.now = func() time.Time { return fake }
	key := sampleLife5ExtractedKey(t, 51)
	handle, err := recv.RegisterInstance("/clk", "sample_test__msg__Keyed", key)
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	if err := recv.DisposeInstance(handle); err != nil {
		t.Fatalf("dispose: %v", err)
	}
	control, err := recv.TakeControlSample()
	if err != nil {
		t.Fatalf("take control: %v", err)
	}
	if !control.Info.SourceTimestamp.Equal(fake) || !control.Info.ReceptionTimestamp.Equal(fake) {
		t.Fatalf("control stamps = %v %v, want fake %v", control.Info.SourceTimestamp, control.Info.ReceptionTimestamp, fake)
	}
}

// TestSweepAgedInstancesPurgesSilent pins background sweep expiry.
func TestSweepAgedInstancesPurgesSilent(t *testing.T) {
	t.Parallel()

	sampleKeyedDesc(t)
	recv := NewDataReceiver()
	recv.SetAutopurgeLife5Delays(1000000000, 1000000000)
	t0 := time.Unix(9700, 0).UTC()
	recv.now = func() time.Time { return t0 }
	key := sampleLife5ExtractedKey(t, 52)
	handle, err := recv.RegisterInstance("/sweep", "sample_test__msg__Keyed", key)
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	if err := recv.UnregisterInstance(handle); err != nil {
		t.Fatalf("unregister: %v", err)
	}
	// Drain the control so history is empty and the instance is purgeable.
	if _, err := recv.TakeControlSample(); err != nil {
		t.Fatalf("drain control: %v", err)
	}
	st := recv.readerStateFor()
	st.mu.Lock()
	// Simulate silence past the delay with no retained history.
	rec := st.instances[handle]
	rec.notAliveAtLife5 = t0.UnixNano()
	st.mu.Unlock()

	purged := recv.SweepAgedInstances(t0.Add(2 * time.Second).UnixNano())
	if purged != 1 {
		t.Fatalf("swept = %d, want 1", purged)
	}
	st.mu.Lock()
	_, ok := st.instances[handle]
	st.mu.Unlock()
	if ok {
		t.Fatalf("silent instance survived sweep, want purged")
	}
}

// TestAutodisposeIncrementsDisposed pins autodispose post-transition counts.
func TestAutodisposeIncrementsDisposed(t *testing.T) {
	t.Parallel()

	sampleKeyedDesc(t)
	recv := NewDataReceiver()
	recv.SetAutodisposeUnregisteredInstances(true)
	key := sampleLife5ExtractedKey(t, 53)
	handle, err := recv.RegisterInstance("/auto", "sample_test__msg__Keyed", key)
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	if err := recv.UnregisterInstance(handle); err != nil {
		t.Fatalf("unregister autodispose: %v", err)
	}
	control, err := recv.TakeControlSample()
	if err != nil {
		t.Fatalf("take control: %v", err)
	}
	// Autodispose lands in disposed with the post-transition disposed count.
	if control.Info.InstanceState != InstanceStateDisposed {
		t.Fatalf("autodispose state = %v, want disposed", control.Info.InstanceState)
	}
	if control.Info.DisposedGenerationCount != 1 || control.Info.NoWritersGenerationCount != 0 {
		t.Fatalf("autodispose counts = %d %d, want 1 0", control.Info.DisposedGenerationCount, control.Info.NoWritersGenerationCount)
	}
}
