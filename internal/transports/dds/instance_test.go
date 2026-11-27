// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

package dds

import (
	"errors"
	"testing"

	"github.com/podomy/weld/internal/transports"
	"github.com/podomy/weld/internal/types"
)

// sampleKeyedDesc registers a one-key fixture descriptor for instance tests.
//
// Registration is idempotent across parallel runs sharing one namespace.
func sampleKeyedDesc(t *testing.T) {
	t.Helper()

	members := []types.MessageMember{
		types.NewScalarMember("seq", types.RosTypeUint32, 0),
	}
	members[0].IsKey = true
	desc := types.NewMessageMembers("sample_test__msg", "Keyed", 8, members, nil, nil)

	err := types.RegisterMessageMembers(desc)
	if err != nil {
		_, lerr := types.LookupMessageMembers("sample_test__msg", "Keyed")
		if lerr != nil {
			t.Fatalf("register keyed descriptor: %v", err)
		}
	}
}

// encodeSampleKey encodes one uint32 key sample as XCDR1.
func encodeSampleKey(t *testing.T, seq uint32) []byte {
	t.Helper()

	enc := types.NewEncoder(true)
	enc.WriteUint32(seq)

	return enc.Bytes()
}

// sampleKeyedGUID returns a test GUID with a with-key writer kind.
func sampleKeyedGUID() WriterGUID {
	guid := testWriterGUID()
	guid[15] = keyedUserWriterKind

	return guid
}

// sampleLife5ExtractedKey encodes one uint32 key sample and returns its extracted key.
//
// RegisterInstance and LookupInstance take extracted ordered key bytes,
// not the full XCDR payload, so tests crossing the DATA path must
// register the extracted form to hit the same instance the take path
// resolves.
func sampleLife5ExtractedKey(t *testing.T, seq uint32) []byte {
	t.Helper()

	desc, err := types.LookupMessageMembers("sample_test__msg", "Keyed")
	if err != nil {
		t.Fatalf("lookup keyed descriptor: %v", err)
	}

	key, err := ExtractKeyBytes(encodeSampleKey(t, seq), desc)
	if err != nil {
		t.Fatalf("extract key: %v", err)
	}

	return key
}

// mustSampleKeyedFrame publishes one keyed sample and returns its frame.
func mustSampleKeyedFrame(t *testing.T, sink *recordingWriter, pub *Publisher, topic string, seq uint32) []byte {
	t.Helper()

	before := sink.frameCount()
	mustPublish(t, pub, transports.OutgoingMessage{TopicName: topic, TypeName: "sample_test__msg__Keyed", Payload: encodeSampleKey(t, seq)})

	return sink.frameAt(t, before)
}

// TestInstanceRegisterLookupKeyValue checks handle stability and key return.
func TestInstanceRegisterLookupKeyValue(t *testing.T) {
	t.Parallel()

	sampleKeyedDesc(t)
	recv := NewDataReceiver()

	key := encodeSampleKey(t, 7)
	handle, err := recv.RegisterInstance("/keys", "sample_test__msg__Keyed", key)
	if err != nil {
		t.Fatalf("register: %v", err)
	}

	found, ok := recv.LookupInstance("sample_test__msg__Keyed", key)
	if !ok {
		t.Fatalf("lookup missed a registered key")
	}

	if found != handle {
		t.Fatalf("lookup handle differs from register handle")
	}

	back, ok := recv.GetKeyValue(handle)
	if !ok {
		t.Fatalf("key value missed a registered handle")
	}

	if string(back) != string(key) {
		t.Fatalf("key value round trip mismatch")
	}

	if _, ok := recv.LookupInstance("sample_test__msg__Keyed", encodeSampleKey(t, 8)); ok {
		t.Fatalf("lookup hit an unknown key")
	}
}

// TestInstanceTakeAssignsStableHandles checks takes hash equal keys equally.
func TestInstanceTakeAssignsStableHandles(t *testing.T) {
	t.Parallel()

	sampleKeyedDesc(t)
	recv := NewDataReceiver()
	sink := &recordingWriter{}
	pub := mustNewPublisher(t, sampleKeyedGUID(), sink)

	first, err := recv.TakeSampled(mustSampleKeyedFrame(t, sink, pub, "/keys", 7))
	if err != nil {
		t.Fatalf("first take: %v", err)
	}

	second, err := recv.TakeSampled(mustSampleKeyedFrame(t, sink, pub, "/keys", 7))
	if err != nil {
		t.Fatalf("second take: %v", err)
	}

	if first.Info.InstanceHandle != second.Info.InstanceHandle {
		t.Fatalf("equal keys hash to different handles")
	}

	third, err := recv.TakeSampled(mustSampleKeyedFrame(t, sink, pub, "/keys", 9))
	if err != nil {
		t.Fatalf("third take: %v", err)
	}

	if third.Info.InstanceHandle == first.Info.InstanceHandle {
		t.Fatalf("different keys hash to the same handle")
	}
}

// TestInstanceDisposeSurfacesNotAlive checks dispose yields an invalid sample.
func TestInstanceDisposeSurfacesNotAlive(t *testing.T) {
	t.Parallel()

	sampleKeyedDesc(t)
	recv := NewDataReceiver()

	key := encodeSampleKey(t, 7)
	handle, err := recv.RegisterInstance("/keys", "sample_test__msg__Keyed", key)
	if err != nil {
		t.Fatalf("register: %v", err)
	}

	err = recv.DisposeInstance(handle)
	if err != nil {
		t.Fatalf("dispose: %v", err)
	}

	if got := recv.PendingControls(); got != 1 {
		t.Fatalf("pending controls = %d, want 1", got)
	}

	control, err := recv.TakeControlSample()
	if err != nil {
		t.Fatalf("take control: %v", err)
	}

	if control.Info.ValidData {
		t.Fatalf("control valid data = true, want false")
	}

	if control.Info.InstanceState != InstanceStateDisposed {
		t.Fatalf("control state = %v, want disposed", control.Info.InstanceState)
	}

	if control.Msg.TopicName != "/keys" {
		t.Fatalf("control topic = %q, want /keys", control.Msg.TopicName)
	}

	state, ok := recv.InstanceStateOf(handle)
	if !ok {
		t.Fatalf("instance state missed a disposed handle")
	}

	if state != InstanceStateDisposed {
		t.Fatalf("stored state = %v, want disposed", state)
	}

	if _, err := recv.TakeControlSample(); !errors.Is(err, ErrNoSample) {
		t.Fatalf("empty control take = %v, want ErrNoSample", err)
	}
}

// TestInstanceUnregisterSurfacesNoWriters checks unregister state and counts.
func TestInstanceUnregisterSurfacesNoWriters(t *testing.T) {
	t.Parallel()

	sampleKeyedDesc(t)
	recv := NewDataReceiver()

	key := encodeSampleKey(t, 3)
	handle, err := recv.RegisterInstance("/keys", "sample_test__msg__Keyed", key)
	if err != nil {
		t.Fatalf("register: %v", err)
	}

	err = recv.UnregisterInstance(handle)
	if err != nil {
		t.Fatalf("unregister: %v", err)
	}

	control, err := recv.TakeControlSample()
	if err != nil {
		t.Fatalf("take control: %v", err)
	}

	if control.Info.InstanceState != InstanceStateNoWriters {
		t.Fatalf("control state = %v, want no_writers", control.Info.InstanceState)
	}

	// DDS 1.4 2.2.2.5.1 counts transitions INTO not-alive: the unregister
	// control carries the post-transition count 1, not the pre-transition 0.
	if control.Info.NoWritersGenerationCount != 1 {
		t.Fatalf("no-writers count = %d, want 1 post-transition", control.Info.NoWritersGenerationCount)
	}
}

// mustFirstTransition unregisters or disposes once then drains control.
func mustFirstTransition(t *testing.T, recv *DataReceiver, handle [16]byte, disposeFirst bool) {
	t.Helper()

	var err error
	if disposeFirst {
		err = recv.DisposeInstance(handle)
	} else {
		err = recv.UnregisterInstance(handle)
	}
	if err != nil {
		t.Fatalf("first transition: %v", err)
	}

	if _, err := recv.TakeControlSample(); err != nil {
		t.Fatalf("take control: %v", err)
	}
}

// mustReviveAndDispose revives one instance then disposes and returns control.
func mustReviveAndDispose(t *testing.T, recv *DataReceiver, handle [16]byte, key []byte) SampledMessage {
	t.Helper()

	if _, err := recv.RegisterInstance("/keys", "sample_test__msg__Keyed", key); err != nil {
		t.Fatalf("revival register: %v", err)
	}

	if err := recv.DisposeInstance(handle); err != nil {
		t.Fatalf("dispose after revival: %v", err)
	}

	control, err := recv.TakeControlSample()
	if err != nil {
		t.Fatalf("take revival control: %v", err)
	}

	return control
}

// mustRevivalControl registers, transitions, revives, and returns control.
func mustRevivalControl(t *testing.T, key []byte, disposeFirst bool) SampledMessage {
	t.Helper()

	sampleKeyedDesc(t)
	recv := NewDataReceiver()

	handle, err := recv.RegisterInstance("/keys", "sample_test__msg__Keyed", key)
	if err != nil {
		t.Fatalf("register: %v", err)
	}

	mustFirstTransition(t, recv, handle, disposeFirst)

	control := mustReviveAndDispose(t, recv, handle, key)

	return control
}

// TestLife5InstanceRevivalCounts checks revival increments once.
func TestLife5InstanceRevivalCounts(t *testing.T) {
	t.Parallel()

	control := mustRevivalControl(t, encodeSampleKey(t, 3), false)

	if control.Info.NoWritersGenerationCount != 1 {
		t.Fatalf("revival no-writers count = %d, want 1", control.Info.NoWritersGenerationCount)
	}
}

// TestLife5RegisterLiveIdempotent checks re-registering alive adds no count.
func TestLife5RegisterLiveIdempotent(t *testing.T) {
	t.Parallel()

	sampleKeyedDesc(t)
	recv := NewDataReceiver()

	key := sampleLife5ExtractedKey(t, 11)
	handle, err := recv.RegisterInstance("/keys", "sample_test__msg__Keyed", key)
	if err != nil {
		t.Fatalf("register: %v", err)
	}

	if _, err := recv.RegisterInstance("/keys", "sample_test__msg__Keyed", key); err != nil {
		t.Fatalf("re-register live: %v", err)
	}

	st := recv.readerStateFor()
	st.mu.Lock()
	rec := st.instances[handle]
	st.mu.Unlock()

	if rec.disposedCount != 0 || rec.noWritersCount != 0 {
		t.Fatalf("live re-register counts = %d %d, want 0 0", rec.disposedCount, rec.noWritersCount)
	}

	if rec.view != ViewStateNew {
		t.Fatalf("live re-register view = %v, want new", rec.view)
	}
}

// TestLife5DisposeTwiceSnapshotsZero checks repeat dispose adds no count.
func TestLife5DisposeTwiceSnapshotsZero(t *testing.T) {
	t.Parallel()

	sampleKeyedDesc(t)
	recv := NewDataReceiver()

	key := sampleLife5ExtractedKey(t, 12)
	handle, err := recv.RegisterInstance("/keys", "sample_test__msg__Keyed", key)
	if err != nil {
		t.Fatalf("register: %v", err)
	}

	if err := recv.DisposeInstance(handle); err != nil {
		t.Fatalf("first dispose: %v", err)
	}

	if err := recv.DisposeInstance(handle); err != nil {
		t.Fatalf("second dispose: %v", err)
	}

	// Post-transition pinning per DDS 1.4 2.2.2.5.1: the first dispose
	// increments to 1 and carries 1; the repeat while already disposed adds
	// no new transition so it also carries 1.
	for i := range 2 {
		control, err := recv.TakeControlSample()
		if err != nil {
			t.Fatalf("take control %d: %v", i, err)
		}

		if control.Info.DisposedGenerationCount != 1 {
			t.Fatalf("control %d disposed count = %d, want 1 post-transition", i, control.Info.DisposedGenerationCount)
		}
	}
}

// TestLife5DataRebirthIncrementsDisposed checks DATA revival bumps disposed.
func TestLife5DataRebirthIncrementsDisposed(t *testing.T) {
	t.Parallel()

	sampleKeyedDesc(t)
	recv := NewDataReceiver()
	sink := &recordingWriter{}
	pub := mustNewPublisher(t, sampleKeyedGUID(), sink)

	key := sampleLife5ExtractedKey(t, 13)
	handle, err := recv.RegisterInstance("/keys", "sample_test__msg__Keyed", key)
	if err != nil {
		t.Fatalf("register: %v", err)
	}

	if err := recv.DisposeInstance(handle); err != nil {
		t.Fatalf("dispose: %v", err)
	}

	if _, err := recv.TakeControlSample(); err != nil {
		t.Fatalf("drain control: %v", err)
	}

	got, err := recv.TakeSampled(mustSampleKeyedFrame(t, sink, pub, "/keys", 13))
	if err != nil {
		t.Fatalf("rebirth take: %v", err)
	}

	if got.Info.DisposedGenerationCount != 1 {
		t.Fatalf("rebirth disposed count = %d, want 1", got.Info.DisposedGenerationCount)
	}

	if got.Info.NoWritersGenerationCount != 0 {
		t.Fatalf("rebirth no-writers count = %d, want 0", got.Info.NoWritersGenerationCount)
	}

	if got.Info.ViewState != ViewStateNew {
		t.Fatalf("rebirth view = %v, want new", got.Info.ViewState)
	}

	state, ok := recv.InstanceStateOf(handle)
	if !ok || state != InstanceStateAlive {
		t.Fatalf("rebirth state = %v %v, want alive", state, ok)
	}
}

// TestLife5DataRebirthIncrementsNoWriters checks DATA revival bumps no-writers.
func TestLife5DataRebirthIncrementsNoWriters(t *testing.T) {
	t.Parallel()

	sampleKeyedDesc(t)
	recv := NewDataReceiver()
	sink := &recordingWriter{}
	pub := mustNewPublisher(t, sampleKeyedGUID(), sink)

	key := sampleLife5ExtractedKey(t, 14)
	handle, err := recv.RegisterInstance("/keys", "sample_test__msg__Keyed", key)
	if err != nil {
		t.Fatalf("register: %v", err)
	}

	if err := recv.UnregisterInstance(handle); err != nil {
		t.Fatalf("unregister: %v", err)
	}

	if _, err := recv.TakeControlSample(); err != nil {
		t.Fatalf("drain control: %v", err)
	}

	got, err := recv.TakeSampled(mustSampleKeyedFrame(t, sink, pub, "/keys", 14))
	if err != nil {
		t.Fatalf("rebirth take: %v", err)
	}

	if got.Info.NoWritersGenerationCount != 1 {
		t.Fatalf("rebirth no-writers count = %d, want 1", got.Info.NoWritersGenerationCount)
	}

	if got.Info.DisposedGenerationCount != 0 {
		t.Fatalf("rebirth disposed count = %d, want 0", got.Info.DisposedGenerationCount)
	}

	if got.Info.ViewState != ViewStateNew {
		t.Fatalf("rebirth view = %v, want new", got.Info.ViewState)
	}
}

// TestLife5ExplicitReviveDisposed checks register revival keeps post-transition count.
func TestLife5ExplicitReviveDisposed(t *testing.T) {
	t.Parallel()

	sampleKeyedDesc(t)
	control := mustRevivalControl(t, sampleLife5ExtractedKey(t, 15), true)

	// Post-transition per DDS 1.4 2.2.2.5.1: first dispose carries 1,
	// revival keeps 1, second dispose increments to 2 and carries 2.
	if control.Info.DisposedGenerationCount != 2 {
		t.Fatalf("revival disposed count = %d, want 2", control.Info.DisposedGenerationCount)
	}
}

// TestInstanceUnknownHandle checks loud errors for unknown handles.
func TestInstanceUnknownHandle(t *testing.T) {
	t.Parallel()

	recv := NewDataReceiver()

	if _, ok := recv.LookupInstance("sample_test__msg__Keyed", encodeSampleKey(t, 1)); ok {
		t.Fatalf("lookup hit an empty registry")
	}

	if _, ok := recv.GetKeyValue([16]byte{0xFF}); ok {
		t.Fatalf("key value hit an empty registry")
	}

	if err := recv.DisposeInstance([16]byte{0xFF}); !errors.Is(err, ErrUnknownInstance) {
		t.Fatalf("dispose unknown = %v, want ErrUnknownInstance", err)
	}

	if err := recv.UnregisterInstance([16]byte{0xFF}); !errors.Is(err, ErrUnknownInstance) {
		t.Fatalf("unregister unknown = %v, want ErrUnknownInstance", err)
	}
}

// mustExclusivePair registers one keyed instance under exclusive ownership.
//
// The stronger writer w2 wins arbitration; both offers must deliver at
// setup so the test pins the owner before gating checks run.
func mustExclusivePair(t *testing.T, topic string, seq uint32, w1, w2 WriterGUID) (*DataReceiver, [16]byte) {
	t.Helper()

	sampleKeyedDesc(t)
	recv := NewDataReceiver()

	key := sampleLife5ExtractedKey(t, seq)
	handle, err := recv.RegisterInstance(topic, "sample_test__msg__Keyed", key)
	if err != nil {
		t.Fatalf("register: %v", err)
	}

	if err := recv.SetOwnershipKind(handle, OwnershipExclusive); err != nil {
		t.Fatalf("set kind: %v", err)
	}

	recv.SetOwnershipStrength(w1, 1)
	recv.SetOwnershipStrength(w2, 9)
	if _, ok := recv.OfferOwnership(handle, w1); !ok {
		t.Fatalf("first offer not deliverable")
	}
	if _, ok := recv.OfferOwnership(handle, w2); !ok {
		t.Fatalf("second offer not deliverable")
	}

	return recv, handle
}

// TestInstanceDisposeOwnerCheck pins exclusive dispose gating.
func TestInstanceDisposeOwnerCheck(t *testing.T) {
	t.Parallel()

	var w1, w2 WriterGUID
	w1[0] = 0x51
	w2[0] = 0x52

	recv, handle := mustExclusivePair(t, "/ownctl", 31, w1, w2)

	if err := recv.DisposeInstanceByWriter(handle, w1); !errors.Is(err, ErrOwnershipRefused) {
		t.Fatalf("non-owner dispose = %v, want ErrOwnershipRefused", err)
	}
	if err := recv.DisposeInstance(handle); !errors.Is(err, ErrOwnershipRefused) {
		t.Fatalf("handle-only dispose under exclusive = %v, want ErrOwnershipRefused", err)
	}
	if err := recv.DisposeInstanceByWriter(handle, w2); err != nil {
		t.Fatalf("owner dispose: %v", err)
	}
	if state, ok := recv.InstanceStateOf(handle); !ok || state != InstanceStateDisposed {
		t.Fatalf("owner dispose state = %v %v, want disposed", state, ok)
	}
}

// TestInstanceUnregisterOwnerCheck pins exclusive unregister gating.
func TestInstanceUnregisterOwnerCheck(t *testing.T) {
	t.Parallel()

	var w1, w2 WriterGUID
	w1[0] = 0x51
	w2[0] = 0x52

	recv, handle := mustExclusivePair(t, "/ownctl2", 32, w1, w2)

	if err := recv.UnregisterInstanceByWriter(handle, w1); !errors.Is(err, ErrOwnershipRefused) {
		t.Fatalf("non-owner unregister = %v, want ErrOwnershipRefused", err)
	}
	if err := recv.UnregisterInstance(handle); !errors.Is(err, ErrOwnershipRefused) {
		t.Fatalf("handle-only unregister under exclusive = %v, want ErrOwnershipRefused", err)
	}
	if err := recv.UnregisterInstanceByWriter(handle, w2); err != nil {
		t.Fatalf("owner unregister: %v", err)
	}
}

// TestInstanceAutodisposeUnregisteredDisposes pins the lifecycle rule.
func TestInstanceAutodisposeUnregisteredDisposes(t *testing.T) {
	t.Parallel()

	sampleKeyedDesc(t)
	recv := NewDataReceiver()
	recv.SetAutodisposeUnregisteredInstances(true)

	key := sampleLife5ExtractedKey(t, 33)
	handle, err := recv.RegisterInstance("/autod", "sample_test__msg__Keyed", key)
	if err != nil {
		t.Fatalf("register: %v", err)
	}

	if err := recv.UnregisterInstance(handle); err != nil {
		t.Fatalf("unregister with autodispose: %v", err)
	}

	state, ok := recv.InstanceStateOf(handle)
	if !ok || state != InstanceStateDisposed {
		t.Fatalf("autodispose state = %v %v, want disposed", state, ok)
	}

	control, err := recv.TakeControlSample()
	if err != nil {
		t.Fatalf("take control: %v", err)
	}
	if control.Info.InstanceState != InstanceStateDisposed {
		t.Fatalf("autodispose control = %v, want disposed", control.Info.InstanceState)
	}

	recv2 := NewDataReceiver()
	key2 := sampleLife5ExtractedKey(t, 34)
	handle2, err := recv2.RegisterInstance("/autod2", "sample_test__msg__Keyed", key2)
	if err != nil {
		t.Fatalf("register2: %v", err)
	}
	if err := recv2.UnregisterInstance(handle2); err != nil {
		t.Fatalf("unregister without autodispose: %v", err)
	}
	state2, _ := recv2.InstanceStateOf(handle2)
	if state2 != InstanceStateNoWriters {
		t.Fatalf("default unregister state = %v, want no_writers", state2)
	}
}

// TestInstanceGenerationCountsDisposed pins dispose generation counting.
func TestInstanceGenerationCountsDisposed(t *testing.T) {
	t.Parallel()

	sampleKeyedDesc(t)
	recv := NewDataReceiver()
	key := sampleLife5ExtractedKey(t, 35)
	handle, err := recv.RegisterInstance("/gen", "sample_test__msg__Keyed", key)
	if err != nil {
		t.Fatalf("register: %v", err)
	}

	if err := recv.DisposeInstance(handle); err != nil {
		t.Fatalf("dispose: %v", err)
	}
	first, err := recv.TakeControlSample()
	if err != nil {
		t.Fatalf("take first control: %v", err)
	}
	// Post-transition per DDS 1.4 2.2.2.5.1: the dispose control carries 1.
	if first.Info.DisposedGenerationCount != 1 || first.Info.NoWritersGenerationCount != 0 {
		t.Fatalf("post-transition control = %d %d, want 1 0", first.Info.DisposedGenerationCount, first.Info.NoWritersGenerationCount)
	}

	if _, err := recv.RegisterInstance("/gen", "sample_test__msg__Keyed", key); err != nil {
		t.Fatalf("revive: %v", err)
	}
	if err := recv.DisposeInstance(handle); err != nil {
		t.Fatalf("second dispose: %v", err)
	}
	second, err := recv.TakeControlSample()
	if err != nil {
		t.Fatalf("take second control: %v", err)
	}
	if second.Info.DisposedGenerationCount != 2 {
		t.Fatalf("revived dispose control = %d, want 2", second.Info.DisposedGenerationCount)
	}
}

// TestInstanceGenerationCountsNoWriters pins no-writers generation counting.
func TestInstanceGenerationCountsNoWriters(t *testing.T) {
	t.Parallel()

	sampleKeyedDesc(t)
	recv := NewDataReceiver()
	key := sampleLife5ExtractedKey(t, 36)
	handle, err := recv.RegisterInstance("/gen2", "sample_test__msg__Keyed", key)
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	if err := recv.UnregisterInstance(handle); err != nil {
		t.Fatalf("unregister: %v", err)
	}
	if _, err := recv.TakeControlSample(); err != nil {
		t.Fatalf("drain: %v", err)
	}
	if _, err := recv.RegisterInstance("/gen2", "sample_test__msg__Keyed", key); err != nil {
		t.Fatalf("revive: %v", err)
	}
	st := recv.readerStateFor()
	st.mu.Lock()
	rec := st.instances[handle]
	disposed, noWriters := rec.disposedCount, rec.noWritersCount
	st.mu.Unlock()
	if disposed != 0 || noWriters != 1 {
		t.Fatalf("revive from no-writers counts = %d %d, want 0 1", disposed, noWriters)
	}
}

// TestInstanceDisposeEmptyOffersFailsClosed pins fail-closed dispose when an
// exclusive instance has no claims at all: the legacy handle-only path
// denies without a proven owner, including the empty-offers case.
func TestInstanceDisposeEmptyOffersFailsClosed(t *testing.T) {
	t.Parallel()

	sampleKeyedDesc(t)
	recv := NewDataReceiver()

	key := sampleLife5ExtractedKey(t, 91)
	handle, err := recv.RegisterInstance("/ownctlempty", "sample_test__msg__Keyed", key)
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	if err := recv.SetOwnershipKind(handle, OwnershipExclusive); err != nil {
		t.Fatalf("set kind: %v", err)
	}

	if err := recv.DisposeInstance(handle); !errors.Is(err, ErrOwnershipRefused) {
		t.Fatalf("handle-only dispose with no offers = %v, want ErrOwnershipRefused", err)
	}
	if err := recv.UnregisterInstance(handle); !errors.Is(err, ErrOwnershipRefused) {
		t.Fatalf("handle-only unregister with no offers = %v, want ErrOwnershipRefused", err)
	}

	var stranger WriterGUID
	stranger[0] = 0x99
	if err := recv.DisposeInstanceByWriter(handle, stranger); !errors.Is(err, ErrOwnershipRefused) {
		t.Fatalf("by-writer dispose with no offers = %v, want ErrOwnershipRefused", err)
	}
}
