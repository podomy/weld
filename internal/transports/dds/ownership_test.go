// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

package dds

import (
	"errors"
	"testing"
	"time"

	"github.com/podomy/weld/internal/core"
	"github.com/podomy/weld/internal/transports"
)

// ownershipWriters returns two ordered writer GUIDs for arbitration tests.
func ownershipWriters() (WriterGUID, WriterGUID) {
	first := testWriterGUID()
	second := testWriterGUID()
	second[0]++

	return first, second
}

// TestOwnershipSharedDeliversAll checks shared instances never arbitrate.
func TestOwnershipSharedDeliversAll(t *testing.T) {
	t.Parallel()

	recv := NewDataReceiver()
	first, second := ownershipWriters()
	var handle [16]byte
	handle[0] = 0x01

	err := recv.SetOwnershipKind(handle, OwnershipShared)
	if err != nil {
		t.Fatalf("set kind: %v", err)
	}

	if _, ok := recv.OfferOwnership(handle, first); !ok {
		t.Fatalf("shared first offer not deliverable")
	}

	owner, ok := recv.OfferOwnership(handle, second)
	if !ok {
		t.Fatalf("shared second offer not deliverable")
	}

	if owner != second {
		t.Fatalf("shared owner = %x, want the claimant", owner)
	}
}

// TestOwnershipExclusiveArbitrates checks strength wins with GUID tie-break.
func TestOwnershipExclusiveArbitrates(t *testing.T) {
	t.Parallel()

	recv := NewDataReceiver()
	first, second := ownershipWriters()
	var handle [16]byte
	handle[0] = 0x02

	err := recv.SetOwnershipKind(handle, OwnershipExclusive)
	if err != nil {
		t.Fatalf("set kind: %v", err)
	}

	recv.SetOwnershipStrength(first, 10)
	recv.SetOwnershipStrength(second, 20)

	owner, ok := recv.OfferOwnership(handle, first)
	if !ok {
		t.Fatalf("uncontested first offer not deliverable")
	}

	if owner != first {
		t.Fatalf("uncontested owner = %x, want first", owner)
	}

	owner, ok = recv.OfferOwnership(handle, second)
	if !ok {
		t.Fatalf("stronger offer not deliverable")
	}

	if owner != second {
		t.Fatalf("owner = %x, want second", owner)
	}

	if _, ok := recv.OfferOwnership(handle, first); ok {
		t.Fatalf("weaker offer deliverable under exclusive ownership")
	}

	stored, ok := recv.OwnershipOwner(handle)
	if !ok {
		t.Fatalf("no owner recorded")
	}

	if stored != second {
		t.Fatalf("stored owner = %x, want second", stored)
	}
}

// TestOwnershipTieBreaksOnGUID checks equal strength picks lowest GUID.
func TestOwnershipTieBreaksOnGUID(t *testing.T) {
	t.Parallel()

	recv := NewDataReceiver()
	first, second := ownershipWriters()
	var handle [16]byte
	handle[0] = 0x03

	err := recv.SetOwnershipKind(handle, OwnershipExclusive)
	if err != nil {
		t.Fatalf("set kind: %v", err)
	}

	recv.SetOwnershipStrength(first, 5)
	recv.SetOwnershipStrength(second, 5)

	_, _ = recv.OfferOwnership(handle, second)
	owner, ok := recv.OfferOwnership(handle, first)
	if !ok {
		t.Fatalf("tied lower GUID not deliverable")
	}

	if owner != first {
		t.Fatalf("tie owner = %x, want lowest GUID", owner)
	}

	if err := recv.SetOwnershipKind(handle, OwnershipUnknown); !errors.Is(err, ErrOwnershipRefused) {
		t.Fatalf("unknown kind = %v, want ErrOwnershipRefused", err)
	}
}

// TestOrderByReceptionSorts checks oldest-first with deterministic ties.
func TestOrderByReceptionSorts(t *testing.T) {
	t.Parallel()

	base := time.Unix(2000, 0).UTC()
	mk := func(offset int64, seq uint64) SampledMessage {
		var sample SampledMessage
		sample.Info.ReceptionTimestamp = base.Add(time.Duration(offset))
		sample.Info.SampleSequence = seq

		return sample
	}

	in := []SampledMessage{mk(30, 1), mk(10, 2), mk(20, 3)}
	got := OrderByReception(in)

	if got[0].Info.SampleSequence != 2 || got[1].Info.SampleSequence != 3 || got[2].Info.SampleSequence != 1 {
		t.Fatalf("order = %d %d %d, want 2 3 1", got[0].Info.SampleSequence, got[1].Info.SampleSequence, got[2].Info.SampleSequence)
	}
}

// TestTimeBasedFilterSeparates checks minimum separation per instance.
func TestTimeBasedFilterSeparates(t *testing.T) {
	t.Parallel()

	recv := NewDataReceiver()
	var handle [16]byte
	handle[0] = 0x04

	if err := recv.SetTimeBasedFilter(100); err != nil {
		t.Fatalf("set time filter: %v", err)
	}

	if !recv.AllowTimeBasedFilter(handle, 1000) {
		t.Fatalf("first sample filtered")
	}

	if recv.AllowTimeBasedFilter(handle, 1050) {
		t.Fatalf("sample inside separation passed")
	}

	if !recv.AllowTimeBasedFilter(handle, 1100) {
		t.Fatalf("sample at separation filtered")
	}

	if got := recv.TimeFilteredCount(); got != 1 {
		t.Fatalf("filtered = %d, want 1", got)
	}

	if err := recv.SetTimeBasedFilter(0); err != nil {
		t.Fatalf("disable time filter: %v", err)
	}

	if !recv.AllowTimeBasedFilter(handle, 1101) {
		t.Fatalf("disabled filter dropped a sample")
	}
}

// TestResourceLimitsEnforce checks caps plus admission refusal.
func TestResourceLimitsEnforce(t *testing.T) {
	t.Parallel()

	recv := NewDataReceiver()

	if err := recv.SetResourceLimits(ResourceLimits{MaxSamples: -2, MaxInstances: -1, MaxSamplesPerInstance: -1}); !errors.Is(err, ErrOwnershipRefused) {
		t.Fatalf("bad limits = %v, want ErrOwnershipRefused", err)
	}

	hookTestStringDesc(t)
	mustTakeStrings(t, recv, "/q", "hook_test__msg__Hooked", []string{"a", "b", "c"})

	err := recv.SetResourceLimits(ResourceLimits{MaxSamples: 2, MaxInstances: -1, MaxSamplesPerInstance: -1})
	if err != nil {
		t.Fatalf("set limits: %v", err)
	}

	st := recv.readerStateFor()
	st.mu.Lock()
	st.enforceSampleLimitLocked()
	st.mu.Unlock()

	if got := len(recv.SampleHistory()); got != 2 {
		t.Fatalf("history = %d, want 2", got)
	}

	if got := recv.ResourceViolations(); got != 1 {
		t.Fatalf("violations = %d, want 1", got)
	}

	err = recv.SetResourceLimits(ResourceLimits{MaxSamples: -1, MaxInstances: 1, MaxSamplesPerInstance: -1})
	if err != nil {
		t.Fatalf("set instance limit: %v", err)
	}

	if err := recv.AdmitInstance(); !errors.Is(err, ErrOwnershipRefused) {
		t.Fatalf("admit = %v, want ErrOwnershipRefused", err)
	}

	var handle [16]byte
	if len(recv.SampleHistory()) > 0 {
		handle = recv.SampleHistory()[0].Info.InstanceHandle
	}

	err = recv.SetResourceLimits(ResourceLimits{MaxSamples: -1, MaxInstances: -1, MaxSamplesPerInstance: 1})
	if err != nil {
		t.Fatalf("set per-instance limit: %v", err)
	}

	if err := recv.PerInstanceAdmit(handle); err == nil {
		t.Fatalf("per-instance admit passed over the bound")
	}
}

// TestOwnershipRxOMismatchFailsClosed checks kind mismatch drops delivery.
func TestOwnershipRxOMismatchFailsClosed(t *testing.T) {
	t.Parallel()

	recv := NewDataReceiver()
	var handle [16]byte
	handle[0] = 0xB0

	if err := recv.SetOwnershipKind(handle, OwnershipExclusive); err != nil {
		t.Fatalf("set reader kind: %v", err)
	}

	weaker := testWriterGUID()
	stronger := testWriterGUID()
	stronger[0]++

	recv.SetOwnershipStrength(weaker, 1)
	recv.SetOwnershipStrength(stronger, 100)

	if err := recv.SetOwnershipWriterKind(weaker, OwnershipShared); err != nil {
		t.Fatalf("set weaker writer kind: %v", err)
	}
	if err := recv.SetOwnershipWriterKind(stronger, OwnershipExclusive); err != nil {
		t.Fatalf("set stronger writer kind: %v", err)
	}

	if _, ok := recv.OfferOwnership(handle, weaker); ok {
		t.Fatalf("mismatched shared offer deliverable under exclusive reader")
	}

	owner, ok := recv.OfferOwnership(handle, stronger)
	if !ok {
		t.Fatalf("matched exclusive offer not deliverable")
	}
	if owner != stronger {
		t.Fatalf("owner = %x, want stronger", owner)
	}

	if recv.CheckOwnershipCompatible(handle, weaker) {
		t.Fatalf("mismatched writer reported compatible")
	}
	if !recv.CheckOwnershipCompatible(handle, stronger) {
		t.Fatalf("matched writer reported incompatible")
	}

	if err := recv.SetOwnershipWriterKind(weaker, OwnershipUnknown); !errors.Is(err, ErrOwnershipRefused) {
		t.Fatalf("unknown writer kind = %v, want ErrOwnershipRefused", err)
	}
}

// TestOwnershipFeedStatusWiresLivelinessDeadline checks fed status arbitrates.
func TestOwnershipFeedStatusWiresLivelinessDeadline(t *testing.T) {
	t.Parallel()

	recv := NewDataReceiver()
	first, second := ownershipWriters()
	var handle [16]byte
	handle[0] = 0xB1

	if err := recv.SetOwnershipKind(handle, OwnershipExclusive); err != nil {
		t.Fatalf("set kind: %v", err)
	}
	recv.SetOwnershipStrength(first, 50)
	recv.SetOwnershipStrength(second, 5)

	if _, ok := recv.OfferOwnership(handle, first); !ok {
		t.Fatalf("strong first not deliverable")
	}
	if _, ok := recv.OfferOwnership(handle, second); ok {
		t.Fatalf("weak second deliverable while strong owns")
	}

	recv.FeedOwnershipqos4Status(first, false, true)
	owner, ok := recv.OfferOwnership(handle, second)
	if !ok || owner != second {
		t.Fatalf("dead strong owner = %x %v, want second", owner, ok)
	}

	recv.FeedOwnershipqos4Status(first, true, true)
	recv.FeedOwnershipqos4Status(second, true, false)
	owner, ok = recv.OfferOwnership(handle, first)
	if !ok || owner != first {
		t.Fatalf("deadline-missed second owner = %x %v, want first", owner, ok)
	}
}

// TestLife5LegacyTakeEnforcesOwnershipAndTimeFilter pins the legacy path gating.
func TestLife5LegacyTakeEnforcesOwnershipAndTimeFilter(t *testing.T) {
	t.Parallel()

	recv := NewDataReceiver()
	handle := InstanceHandle("std_msgs__msg__String", nil)
	if err := recv.SetOwnershipKind(handle, OwnershipExclusive); err != nil {
		t.Fatalf("set kind: %v", err)
	}

	var w1, w2 WriterGUID
	w1[0] = 0x41
	w2[0] = 0x42
	recv.SetOwnershipStrength(w1, 1)
	recv.SetOwnershipStrength(w2, 10)
	if _, ok := recv.OfferOwnership(handle, w1); !ok {
		t.Fatalf("first offer not deliverable")
	}
	if _, ok := recv.OfferOwnership(handle, w2); !ok {
		t.Fatalf("second offer not deliverable")
	}

	sink := &recordingWriter{}
	pub := mustNewPublisher(t, w1, sink)
	mustPublish(t, pub, transports.OutgoingMessage{TopicName: "/legacyown", TypeName: "std_msgs__msg__String", Payload: encodeTestString(t, "mine")})
	if _, err := recv.Take(sink.lastFrame(t)); !errors.Is(err, ErrNoSample) {
		t.Fatalf("non-owner legacy take = %v, want ErrNoSample", err)
	}
	if got := len(recv.SampleHistory()); got != 0 {
		t.Fatalf("suppressed legacy history = %d, want 0", got)
	}

	tfRecv := NewDataReceiver()
	if err := tfRecv.SetTimeBasedFilter(int64(time.Second)); err != nil {
		t.Fatalf("set time filter: %v", err)
	}
	base := time.Unix(9100, 0).UTC()
	tfRecv.now = func() time.Time { return base }
	sink2 := &recordingWriter{}
	pub2 := mustNewPublisher(t, testWriterGUID(), sink2)
	mustPublish(t, pub2, transports.OutgoingMessage{TopicName: "/legacytf", TypeName: "std_msgs__msg__String", Payload: encodeTestString(t, "one")})
	if _, err := tfRecv.Take(sink2.lastFrame(t)); err != nil {
		t.Fatalf("first legacy take: %v", err)
	}
	tfRecv.now = func() time.Time { return base.Add(100 * time.Millisecond) }
	mustPublish(t, pub2, transports.OutgoingMessage{TopicName: "/legacytf", TypeName: "std_msgs__msg__String", Payload: encodeTestString(t, "two")})
	if _, err := tfRecv.Take(sink2.lastFrame(t)); !errors.Is(err, ErrNoSample) {
		t.Fatalf("filtered legacy take = %v, want ErrNoSample", err)
	}
	if got := tfRecv.TimeFilteredCount(); got != 1 {
		t.Fatalf("legacy filtered = %d, want 1", got)
	}
}

// TestLife5TimeFilterRefusesAboveDeadline checks minimum versus deadline RxO.
func TestLife5TimeFilterRefusesAboveDeadline(t *testing.T) {
	t.Parallel()

	recv := NewDataReceiver()
	if err := recv.SetDeadlineTrackerForTopic("/tfd", core.NewDuration(0, 500000000)); err != nil {
		t.Fatalf("set deadline: %v", err)
	}
	if err := recv.SetTimeBasedFilter(int64(1000000000)); !errors.Is(err, ErrOwnershipRefused) {
		t.Fatalf("minimum above deadline = %v, want ErrOwnershipRefused", err)
	}
	if err := recv.SetTimeBasedFilter(int64(100000000)); err != nil {
		t.Fatalf("consistent minimum refused: %v", err)
	}
}

// TestOwnershipResourceHistoryConsistencyRefuses checks limits versus depth.
func TestOwnershipResourceHistoryConsistencyRefuses(t *testing.T) {
	t.Parallel()

	recv := NewDataReceiver()
	if err := recv.SetResourceLimits(ResourceLimits{MaxSamples: 1, MaxInstances: -1, MaxSamplesPerInstance: 2}); !errors.Is(err, ErrOwnershipRefused) {
		t.Fatalf("max below per-instance = %v, want ErrOwnershipRefused", err)
	}
	if err := recv.SetHistoryDepthLife5(0); !errors.Is(err, ErrOwnershipRefused) {
		t.Fatalf("zero depth = %v, want ErrOwnershipRefused", err)
	}
	if err := recv.SetResourceLimits(ResourceLimits{MaxSamples: -1, MaxInstances: -1, MaxSamplesPerInstance: 1}); err != nil {
		t.Fatalf("set per-instance: %v", err)
	}
	recv.SetReliabilityHistoryLife5(false, false)
	if err := recv.SetHistoryDepthLife5(2); !errors.Is(err, ErrOwnershipRefused) {
		t.Fatalf("depth above per-instance = %v, want ErrOwnershipRefused", err)
	}
	if err := recv.SetHistoryDepthLife5(1); err != nil {
		t.Fatalf("consistent depth refused: %v", err)
	}
	if err := recv.SetResourceLimits(ResourceLimits{MaxSamples: 1, MaxInstances: -1, MaxSamplesPerInstance: 1}); err != nil {
		t.Fatalf("consistent limits refused: %v", err)
	}
}

// TestOwnershipOrderBySourceTimestampSorts checks source ordering with fallback.
func TestOwnershipOrderBySourceTimestampSorts(t *testing.T) {
	t.Parallel()

	base := time.Unix(9200, 0).UTC()
	mk := func(src, recv time.Time, seq uint64) SampledMessage {
		var sample SampledMessage
		sample.Info.SourceTimestamp = src
		sample.Info.ReceptionTimestamp = recv
		sample.Info.SampleSequence = seq
		return sample
	}
	in := []SampledMessage{
		mk(base.Add(3*time.Second), base.Add(3*time.Second), 3),
		mk(base.Add(time.Second), base.Add(2*time.Second), 1),
		mk(base.Add(2*time.Second), base.Add(time.Second), 2),
	}
	got := OrderBySourceTimestamp(in)
	if got[0].Info.SampleSequence != 1 || got[1].Info.SampleSequence != 2 || got[2].Info.SampleSequence != 3 {
		t.Fatalf("source order = %d %d %d, want 1 2 3", got[0].Info.SampleSequence, got[1].Info.SampleSequence, got[2].Info.SampleSequence)
	}

	zero := []SampledMessage{
		mk(time.Time{}, base.Add(2*time.Second), 2),
		mk(time.Time{}, base.Add(time.Second), 1),
	}
	gotZero := OrderBySourceTimestamp(zero)
	if gotZero[0].Info.SampleSequence != 1 {
		t.Fatalf("zero source fallback order wrong")
	}

	if err := NewDataReceiver().SetDestinationOrder(DestinationOrderUnknown); !errors.Is(err, ErrOwnershipRefused) {
		t.Fatalf("unknown destination order accepted, want refusal")
	}
}

// TestOwnershipUnsetWriterKindCompatible pins backward compat for writers
// with no recorded offered kind: unset writers match any reader kind per
// the SetOwnershipWriterKind contract, while an explicitly recorded
// EXCLUSIVE versus SHARED mismatch fails closed per DDS 1.4 2.2.3.9.
func TestOwnershipUnsetWriterKindCompatible(t *testing.T) {
	t.Parallel()

	recv := NewDataReceiver()
	var handle [16]byte
	handle[0] = 0xC1

	if err := recv.SetOwnershipKind(handle, OwnershipExclusive); err != nil {
		t.Fatalf("set reader kind: %v", err)
	}

	writer := testWriterGUID()
	if !recv.CheckOwnershipCompatible(handle, writer) {
		t.Fatalf("unset writer kind reported incompatible, want backward compat")
	}

	if err := recv.SetOwnershipWriterKind(writer, OwnershipShared); err != nil {
		t.Fatalf("set writer kind: %v", err)
	}
	if recv.CheckOwnershipCompatible(handle, writer) {
		t.Fatalf("explicit shared versus exclusive reported compatible, want fail-closed")
	}

	if err := recv.SetOwnershipWriterKind(writer, OwnershipExclusive); err != nil {
		t.Fatalf("set matching writer kind: %v", err)
	}
	if !recv.CheckOwnershipCompatible(handle, writer) {
		t.Fatalf("matching exclusive writer reported incompatible")
	}
}

// TestResourceExhaustedChainsOwnershipRefused pins the sentinel split:
// resource-limit refusals match ErrResourceExhausted for new callers and
// still match ErrOwnershipRefused for pre-split callers.
func TestResourceExhaustedChainsOwnershipRefused(t *testing.T) {
	t.Parallel()

	if !errors.Is(ErrResourceExhausted, ErrOwnershipRefused) {
		t.Fatalf("ErrResourceExhausted does not chain ErrOwnershipRefused")
	}

	recv := NewDataReceiver()
	err := recv.SetResourceLimits(ResourceLimits{MaxSamples: -2, MaxInstances: -1, MaxSamplesPerInstance: -1})
	if !errors.Is(err, ErrResourceExhausted) {
		t.Fatalf("bad limits = %v, want ErrResourceExhausted", err)
	}
	if !errors.Is(err, ErrOwnershipRefused) {
		t.Fatalf("bad limits = %v, want ErrOwnershipRefused chain", err)
	}
}
