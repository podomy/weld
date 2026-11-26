// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

package dds

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/podomy/weld/internal/core"
	"github.com/podomy/weld/internal/transports"
)

// mustTakeStrings publishes and takes one sample per text on one receiver.
func mustTakeStrings(t *testing.T, recv *DataReceiver, topic, typeName string, texts []string) {
	t.Helper()

	sink := &recordingWriter{}
	pub := mustNewPublisher(t, testWriterGUID(), sink)

	for _, text := range texts {
		mustPublish(t, pub, transports.OutgoingMessage{TopicName: topic, TypeName: typeName, Payload: encodeTestString(t, text)})
	}

	for i := range texts {
		_, err := recv.Take(sink.frameAt(t, i))
		if err != nil {
			t.Fatalf("take %d: %v", i, err)
		}
	}
}

// TestReadNextKeepsHistory checks read returns without removing.
func TestReadNextKeepsHistory(t *testing.T) {
	t.Parallel()

	hookTestStringDesc(t)
	recv := NewDataReceiver()
	mustTakeStrings(t, recv, "/q", "hook_test__msg__Hooked", []string{"a", "b"})

	first, err := recv.ReadNext(-1, SampleStateAny, ViewStateAny, InstanceStateAny)
	if err != nil {
		t.Fatalf("read next: %v", err)
	}

	if len(first) != 2 {
		t.Fatalf("read = %d, want 2", len(first))
	}

	second, err := recv.ReadNext(-1, SampleStateAny, ViewStateAny, InstanceStateAny)
	if err != nil {
		t.Fatalf("second read: %v", err)
	}

	if len(second) != 2 {
		t.Fatalf("second read = %d, want 2", len(second))
	}
}

// TestReadNextMaskFiltersDelivered checks masks see delivered state.
func TestReadNextMaskFiltersDelivered(t *testing.T) {
	t.Parallel()

	hookTestStringDesc(t)
	recv := NewDataReceiver()
	mustTakeStrings(t, recv, "/q", "hook_test__msg__Hooked", []string{"a"})

	if _, err := recv.ReadNext(-1, SampleStateNotRead, ViewStateAny, InstanceStateAny); !errors.Is(err, ErrNoSample) {
		t.Fatalf("not-read read succeeded, want ErrNoSample")
	}

	got, err := recv.ReadNext(-1, SampleStateRead, ViewStateAny, InstanceStateAlive)
	if err != nil {
		t.Fatalf("read mask: %v", err)
	}

	if len(got) != 1 {
		t.Fatalf("read = %d, want 1", len(got))
	}
}

// TestTakeNextRemovesHistory checks take drains what it returns.
func TestTakeNextRemovesHistory(t *testing.T) {
	t.Parallel()

	hookTestStringDesc(t)
	recv := NewDataReceiver()
	mustTakeStrings(t, recv, "/q", "hook_test__msg__Hooked", []string{"a", "b", "c"})

	got, err := recv.TakeNext(2, SampleStateAny, ViewStateAny, InstanceStateAny)
	if err != nil {
		t.Fatalf("take next: %v", err)
	}

	if len(got) != 2 {
		t.Fatalf("take = %d, want 2", len(got))
	}

	rest, err := recv.TakeNext(-1, SampleStateAny, ViewStateAny, InstanceStateAny)
	if err != nil {
		t.Fatalf("rest take: %v", err)
	}

	if len(rest) != 1 {
		t.Fatalf("rest = %d, want 1", len(rest))
	}

	if _, err := recv.TakeNext(-1, SampleStateAny, ViewStateAny, InstanceStateAny); !errors.Is(err, ErrNoSample) {
		t.Fatalf("empty take = %v, want ErrNoSample", err)
	}
}

// TestTakeNextDrainsControlsFirst checks lifecycle edges surface first.
func TestTakeNextDrainsControlsFirst(t *testing.T) {
	t.Parallel()

	sampleKeyedDesc(t)
	recv := NewDataReceiver()

	key := encodeSampleKey(t, 7)
	handle, err := recv.RegisterInstance("/keys", "sample_test__msg__Keyed", key)
	if err != nil {
		t.Fatalf("register: %v", err)
	}

	mustTakeStrings(t, recv, "/keys", "std_msgs__msg__String", []string{"live"})

	err = recv.DisposeInstance(handle)
	if err != nil {
		t.Fatalf("dispose: %v", err)
	}

	got, err := recv.TakeNext(1, SampleStateAny, ViewStateAny, InstanceStateAny)
	if err != nil {
		t.Fatalf("take next: %v", err)
	}

	if got[0].Info.ValidData {
		t.Fatalf("first take valid = true, want control first")
	}

	if got[0].Info.InstanceState != InstanceStateDisposed {
		t.Fatalf("first take state = %v, want disposed", got[0].Info.InstanceState)
	}
}

// TestReadConditionTrigger checks trigger evaluation over history.
func TestReadConditionTrigger(t *testing.T) {
	t.Parallel()

	hookTestStringDesc(t)
	recv := NewDataReceiver()

	cond := NewReadCondition(SampleStateNotRead, ViewStateAny, InstanceStateAny)
	if cond.Trigger(recv) {
		t.Fatalf("trigger fired on empty history")
	}

	mustTakeStrings(t, recv, "/q", "hook_test__msg__Hooked", []string{"a"})

	read := NewReadCondition(SampleStateRead, ViewStateAny, InstanceStateAny)
	if !read.Trigger(recv) {
		t.Fatalf("read trigger missed delivered history")
	}

	got, err := read.Take(recv, -1)
	if err != nil {
		t.Fatalf("condition take: %v", err)
	}

	if len(got) != 1 {
		t.Fatalf("condition take = %d, want 1", len(got))
	}

	if cond.Trigger(recv) {
		t.Fatalf("trigger fired after history drained")
	}
}

// TestQueryConditionFiltersContent checks SQL-shaped filtering over history.
func TestQueryConditionFiltersContent(t *testing.T) {
	t.Parallel()

	desc := hookTestStringDesc(t)
	recv := NewDataReceiver()
	mustTakeStrings(t, recv, "/q", "hook_test__msg__Hooked", []string{"keep-me", "drop-me", "keep-me"})

	cond, err := NewQueryCondition("data = 'keep-me'", desc, SampleStateAny, ViewStateAny, InstanceStateAny)
	if err != nil {
		t.Fatalf("query condition: %v", err)
	}

	if !cond.Trigger(recv) {
		t.Fatalf("query trigger missed matching history")
	}

	got, err := cond.Read(recv, -1)
	if err != nil {
		t.Fatalf("query read: %v", err)
	}

	if len(got) != 2 {
		t.Fatalf("query read = %d, want 2", len(got))
	}

	none, err := NewQueryCondition("data = 'absent'", desc, SampleStateAny, ViewStateAny, InstanceStateAny)
	if err != nil {
		t.Fatalf("empty query: %v", err)
	}

	if none.Trigger(recv) {
		t.Fatalf("query trigger fired with no match")
	}

	if _, err := none.Read(recv, -1); !errors.Is(err, ErrNoSample) {
		t.Fatalf("empty query read = %v, want ErrNoSample", err)
	}
}

// TestQueryConditionFunc checks the predicate query shape.
func TestQueryConditionFunc(t *testing.T) {
	t.Parallel()

	hookTestStringDesc(t)
	recv := NewDataReceiver()
	mustTakeStrings(t, recv, "/q", "hook_test__msg__Hooked", []string{"a", "b"})

	cond := NewQueryConditionFunc(func(sample SampledMessage) bool {
		return sample.Info.SampleSequence == 2
	}, SampleStateAny, ViewStateAny, InstanceStateAny)

	got, err := cond.Take(recv, -1)
	if err != nil {
		t.Fatalf("predicate take: %v", err)
	}

	if len(got) != 1 || got[0].Info.SampleSequence != 2 {
		t.Fatalf("predicate take picked the wrong sample")
	}
}

// TestQueryConditionValidation checks constructor failures.
func TestQueryConditionValidation(t *testing.T) {
	t.Parallel()

	if _, err := NewQueryCondition("data = 'x'", nil, SampleStateAny, ViewStateAny, InstanceStateAny); err == nil {
		t.Fatalf("query without descriptor succeeded, want error")
	}

	if _, err := NewQueryCondition("data = ", hookTestStringDesc(t), SampleStateAny, ViewStateAny, InstanceStateAny); err == nil {
		t.Fatalf("bad expression succeeded, want error")
	}

	if _, err := (*QueryCondition)(nil).Read(NewDataReceiver(), -1); !errors.Is(err, ErrNoSample) {
		t.Fatalf("nil query read = %v, want ErrNoSample", err)
	}
}

// TestLife5TimeFilterSourceBasis checks time filtering uses source stamps.
func TestLife5TimeFilterSourceBasis(t *testing.T) {
	t.Parallel()

	recv := NewDataReceiver()
	var handle [16]byte
	handle[0] = 0xE1

	if err := recv.SetTimeBasedFilter(int64(1000000000)); err != nil {
		t.Fatalf("set time filter: %v", err)
	}

	st := recv.readerStateFor()
	st.mu.Lock()
	base := time.Unix(2000, 0).UTC()
	first := craftedSample(handle, WriterGUID{}, 1, base)
	first.Info.SourceTimestamp = base
	first.Info.ReceptionTimestamp = base.Add(10 * time.Second)
	second := craftedSample(handle, WriterGUID{}, 2, base.Add(10*time.Second))
	second.Info.SourceTimestamp = base.Add(100 * time.Millisecond)
	second.Info.ReceptionTimestamp = base.Add(10 * time.Second).Add(time.Second)
	st.appendHistoryLocked(first, false)
	st.appendHistoryLocked(second, false)
	st.mu.Unlock()

	got, err := recv.TakeNext(-1, SampleStateAny, ViewStateAny, InstanceStateAny)
	if err != nil {
		t.Fatalf("take next: %v", err)
	}

	if len(got) != 1 {
		t.Fatalf("delivered = %d, want 1 source filtered", len(got))
	}

	if got := recv.TimeFilteredCount(); got != 1 {
		t.Fatalf("filtered = %d, want 1", got)
	}
}

// TestLife5DeadlineConsistencyRefuses checks deadline below minimum fails.
func TestLife5DeadlineConsistencyRefuses(t *testing.T) {
	t.Parallel()

	recv := NewDataReceiver()
	if err := recv.SetTimeBasedFilter(int64(1000000000)); err != nil {
		t.Fatalf("set time filter: %v", err)
	}

	err := recv.SetDeadlineTrackerForTopic("/life5", core.NewDuration(0, 100000000))
	if err == nil {
		t.Fatalf("inconsistent deadline succeeded, want refusal")
	}

	err = recv.SetDeadlineTrackerForTopic("/life5ok", core.NewDuration(2, 0))
	if err != nil {
		t.Fatalf("consistent deadline refused: %v", err)
	}
}

// TestLife5ControlsNeverConsumeBudget checks controls bypass time filter.
func TestLife5ControlsNeverConsumeBudget(t *testing.T) {
	t.Parallel()

	sampleKeyedDesc(t)
	recv := NewDataReceiver()
	if err := recv.SetTimeBasedFilter(int64(1000000000)); err != nil {
		t.Fatalf("set time filter: %v", err)
	}

	key := encodeSampleKey(t, 7)
	handle, err := recv.RegisterInstance("/life5ctl", "sample_test__msg__Keyed", key)
	if err != nil {
		t.Fatalf("register: %v", err)
	}

	st := recv.readerStateFor()
	st.mu.Lock()
	base := time.Unix(3000, 0).UTC()
	first := craftedSample(handle, WriterGUID{}, 1, base)
	first.Info.SourceTimestamp = base
	first.Info.ReceptionTimestamp = base
	st.appendHistoryLocked(first, false)
	st.mu.Unlock()

	if _, err := recv.TakeNext(-1, SampleStateAny, ViewStateAny, InstanceStateAny); err != nil {
		t.Fatalf("first take next: %v", err)
	}

	if err := recv.DisposeInstance(handle); err != nil {
		t.Fatalf("dispose: %v", err)
	}

	if _, err := recv.TakeNext(1, SampleStateAny, ViewStateAny, InstanceStateAny); err != nil {
		t.Fatalf("control take next: %v", err)
	}

	st2 := recv.readerStateFor()
	st2.mu.Lock()
	second := craftedSample(handle, WriterGUID{}, 2, base.Add(1100*time.Millisecond))
	second.Info.SourceTimestamp = base.Add(1100 * time.Millisecond)
	second.Info.ReceptionTimestamp = base.Add(1100 * time.Millisecond)
	st2.appendHistoryLocked(second, false)
	st2.mu.Unlock()

	got, err := recv.TakeNext(-1, SampleStateAny, ViewStateAny, InstanceStateAny)
	if err != nil {
		t.Fatalf("second data take next: %v", err)
	}

	if len(got) != 1 {
		t.Fatalf("second data delivered = %d, want 1 controls never consume", len(got))
	}
}

// TestLife5QueryEvalDistinct checks eval errors surface apart from no-match.
func TestLife5QueryEvalDistinct(t *testing.T) {
	t.Parallel()

	desc := hookTestStringDesc(t)
	recv := NewDataReceiver()
	mustTakeStrings(t, recv, "/life5q", "hook_test__msg__Hooked", []string{"hello"})

	miss, err := NewQueryCondition("data = 'absent'", desc, SampleStateAny, ViewStateAny, InstanceStateAny)
	if err != nil {
		t.Fatalf("miss query: %v", err)
	}

	if _, err := miss.Read(recv, -1); !errors.Is(err, ErrNoSample) {
		t.Fatalf("miss read = %v, want ErrNoSample", err)
	}

	if errors.Is(err, ErrContentFilterEvalError) {
		t.Fatalf("miss read surfaced eval error, want distinct no-match")
	}

	bad, err := NewQueryCondition("nosuchfield = 1", desc, SampleStateAny, ViewStateAny, InstanceStateAny)
	if err != nil {
		t.Fatalf("bad query compile: %v", err)
	}

	if _, err := bad.Read(recv, -1); !errors.Is(err, ErrContentFilterEvalError) {
		t.Fatalf("eval read = %v, want ErrContentFilterEvalError distinct", err)
	}

	if errors.Is(err, ErrNoSample) && !errors.Is(err, ErrContentFilterEvalError) {
		t.Fatalf("eval read collapsed to no-match")
	}
}

// TestLife5RegisterAdmitsInstances checks MaxInstances refusal on register.
func TestLife5RegisterAdmitsInstances(t *testing.T) {
	t.Parallel()

	sampleKeyedDesc(t)
	recv := NewDataReceiver()

	err := recv.SetResourceLimits(ResourceLimits{MaxSamples: -1, MaxInstances: 1, MaxSamplesPerInstance: -1})
	if err != nil {
		t.Fatalf("set limits: %v", err)
	}

	if _, err := recv.RegisterInstance("/life5a", "sample_test__msg__Keyed", encodeSampleKey(t, 1)); err != nil {
		t.Fatalf("first register: %v", err)
	}

	if _, err := recv.RegisterInstance("/life5a", "sample_test__msg__Keyed", encodeSampleKey(t, 2)); !errors.Is(err, ErrOwnershipRefused) {
		t.Fatalf("second register = %v, want ErrOwnershipRefused admission", err)
	}
}

// TestLife5ListenerOncePerDelivery checks notify fires once per delivery.
func TestLife5ListenerOncePerDelivery(t *testing.T) {
	t.Parallel()

	recv := NewDataReceiver()
	var dataCalls atomic.Int64
	var aliveCalls atomic.Int64

	recv.AddListener("/life5n", &DataListener{
		OnData: func(sample SampledMessage) {
			dataCalls.Add(1)
		},
		OnLiveliness: func(handle [16]byte, alive bool) {
			aliveCalls.Add(1)
		},
	})

	sink := &recordingWriter{}
	pub := mustNewPublisher(t, testWriterGUID(), sink)
	mustPublish(t, pub, transports.OutgoingMessage{TopicName: "/life5n", TypeName: "std_msgs__msg__String", Payload: encodeTestString(t, "hi")})

	if _, err := recv.Take(sink.lastFrame(t)); err != nil {
		t.Fatalf("take: %v", err)
	}

	if got := dataCalls.Load(); got != 1 {
		t.Fatalf("data calls = %d, want 1 per delivery", got)
	}

	if got := aliveCalls.Load(); got != 1 {
		t.Fatalf("alive calls = %d, want 1 per edge", got)
	}

	mustPublish(t, pub, transports.OutgoingMessage{TopicName: "/life5n", TypeName: "std_msgs__msg__String", Payload: encodeTestString(t, "hi2")})

	if _, err := recv.Take(sink.lastFrame(t)); err != nil {
		t.Fatalf("second take: %v", err)
	}

	if got := dataCalls.Load(); got != 2 {
		t.Fatalf("data calls after second = %d, want 2", got)
	}

	if got := aliveCalls.Load(); got != 1 {
		t.Fatalf("alive calls after steady = %d, want 1 once per edge", got)
	}
}

// TestLife5OldGenerationCollectKeepsNew checks old-gen access never flips.
func TestLife5OldGenerationCollectKeepsNew(t *testing.T) {
	t.Parallel()

	recv := NewDataReceiver()
	var handle [16]byte
	handle[0] = 0xE4

	st := recv.readerStateFor()
	st.mu.Lock()
	st.instances[handle] = &instanceRecord{
		handle:        handle,
		typeIDL:       "std_msgs__msg__String",
		topic:         "/oldgen",
		view:          ViewStateNew,
		state:         InstanceStateAlive,
		disposedCount: 1,
	}
	base := time.Unix(8000, 0).UTC()
	old := craftedSample(handle, WriterGUID{}, 1, base)
	st.appendHistoryLocked(old, false)
	st.mu.Unlock()

	got, err := recv.TakeNext(-1, SampleStateAny, ViewStateAny, InstanceStateAny)
	if err != nil {
		t.Fatalf("take old generation: %v", err)
	}

	if len(got) != 1 {
		t.Fatalf("delivered = %d, want 1", len(got))
	}

	st2 := recv.readerStateFor()
	st2.mu.Lock()
	view := st2.instances[handle].view
	st2.mu.Unlock()

	if view != ViewStateNew {
		t.Fatalf("view after old-gen take = %v, want new", view)
	}

	st2.mu.Lock()
	fresh := craftedSample(handle, WriterGUID{}, 2, base.Add(time.Second))
	fresh.Info.DisposedGenerationCount = 1
	st2.appendHistoryLocked(fresh, false)
	st2.mu.Unlock()

	latest, err := recv.TakeNext(-1, SampleStateAny, ViewStateAny, InstanceStateAny)
	if err != nil {
		t.Fatalf("take current generation: %v", err)
	}

	if latest[0].Info.ViewState != ViewStateNew {
		t.Fatalf("current-gen snapshot = %v, want new pre-flip", latest[0].Info.ViewState)
	}

	st2.mu.Lock()
	after := st2.instances[handle].view
	st2.mu.Unlock()

	if after != ViewStateNotNew {
		t.Fatalf("view after current-gen take = %v, want not_new", after)
	}
}

// TestLife5ControlOldGenerationKeepsNew checks old-gen controls never flip.
func TestLife5ControlOldGenerationKeepsNew(t *testing.T) {
	t.Parallel()

	sampleKeyedDesc(t)
	recv := NewDataReceiver()

	key := sampleLife5ExtractedKey(t, 21)
	handle, err := recv.RegisterInstance("/ctlview", "sample_test__msg__Keyed", key)
	if err != nil {
		t.Fatalf("register: %v", err)
	}

	if err := recv.DisposeInstance(handle); err != nil {
		t.Fatalf("dispose: %v", err)
	}

	st := recv.readerStateFor()
	st.mu.Lock()
	rec := st.instances[handle]
	rec.state = InstanceStateAlive
	rec.disposedCount = 1
	rec.view = ViewStateNew
	st.mu.Unlock()

	control, err := recv.TakeControlSample()
	if err != nil {
		t.Fatalf("take old-gen control: %v", err)
	}

	if control.Info.DisposedGenerationCount != 0 {
		t.Fatalf("control disposed count = %d, want 0 old generation", control.Info.DisposedGenerationCount)
	}

	st.mu.Lock()
	view := st.instances[handle].view
	st.mu.Unlock()

	if view != ViewStateNew {
		t.Fatalf("view after old-gen control = %v, want new", view)
	}
}

// TestLife5ControlsSnapshotCurrentView checks controls carry live view.
func TestLife5ControlsSnapshotCurrentView(t *testing.T) {
	t.Parallel()

	sampleKeyedDesc(t)
	recv := NewDataReceiver()
	sink := &recordingWriter{}
	pub := mustNewPublisher(t, sampleKeyedGUID(), sink)

	key := sampleLife5ExtractedKey(t, 22)
	handle, err := recv.RegisterInstance("/ctlview2", "sample_test__msg__Keyed", key)
	if err != nil {
		t.Fatalf("register: %v", err)
	}

	if _, err := recv.TakeSampled(mustSampleKeyedFrame(t, sink, pub, "/ctlview2", 22)); err != nil {
		t.Fatalf("take data: %v", err)
	}

	if err := recv.DisposeInstance(handle); err != nil {
		t.Fatalf("dispose: %v", err)
	}

	control, err := recv.TakeControlSample()
	if err != nil {
		t.Fatalf("take control: %v", err)
	}

	if control.Info.ViewState != ViewStateNotNew {
		t.Fatalf("control view = %v, want not_new snapshot", control.Info.ViewState)
	}
}

// TestLife5ImmediateTakeFilteredDrops checks Take shares the verdict path.
func TestLife5ImmediateTakeFilteredDrops(t *testing.T) {
	t.Parallel()

	recv := NewDataReceiver()
	if err := recv.SetTimeBasedFilter(int64(time.Second)); err != nil {
		t.Fatalf("set time filter: %v", err)
	}

	base := time.Unix(8100, 0).UTC()
	recv.now = func() time.Time { return base }

	sink := &recordingWriter{}
	pub := mustNewPublisher(t, testWriterGUID(), sink)
	mustPublish(t, pub, transports.OutgoingMessage{TopicName: "/imm", TypeName: "std_msgs__msg__String", Payload: encodeTestString(t, "one")})
	if _, err := recv.Take(sink.lastFrame(t)); err != nil {
		t.Fatalf("first take: %v", err)
	}

	recv.now = func() time.Time { return base.Add(100 * time.Millisecond) }
	mustPublish(t, pub, transports.OutgoingMessage{TopicName: "/imm", TypeName: "std_msgs__msg__String", Payload: encodeTestString(t, "two")})
	if _, err := recv.Take(sink.lastFrame(t)); !errors.Is(err, ErrNoSample) {
		t.Fatalf("filtered take = %v, want ErrNoSample", err)
	}

	if got := recv.TimeFilteredCount(); got != 1 {
		t.Fatalf("filtered = %d, want 1", got)
	}

	if got := len(recv.SampleHistory()); got != 1 {
		t.Fatalf("history = %d, want 1 take-means-drop", got)
	}
}

// TestLife5TakeSampledFilteredDrops checks TakeSampled shares the verdict path.
func TestLife5TakeSampledFilteredDrops(t *testing.T) {
	t.Parallel()

	recv := NewDataReceiver()
	if err := recv.SetTimeBasedFilter(int64(time.Second)); err != nil {
		t.Fatalf("set time filter: %v", err)
	}

	base := time.Unix(8150, 0).UTC()
	recv.now = func() time.Time { return base }

	sink := &recordingWriter{}
	pub := mustNewPublisher(t, testWriterGUID(), sink)
	mustPublish(t, pub, transports.OutgoingMessage{TopicName: "/imms", TypeName: "std_msgs__msg__String", Payload: encodeTestString(t, "one")})
	if _, err := recv.TakeSampled(sink.lastFrame(t)); err != nil {
		t.Fatalf("first take: %v", err)
	}

	recv.now = func() time.Time { return base.Add(100 * time.Millisecond) }
	mustPublish(t, pub, transports.OutgoingMessage{TopicName: "/imms", TypeName: "std_msgs__msg__String", Payload: encodeTestString(t, "two")})
	if _, err := recv.TakeSampled(sink.lastFrame(t)); !errors.Is(err, ErrNoSample) {
		t.Fatalf("filtered take sampled = %v, want ErrNoSample", err)
	}

	if got := len(recv.SampleHistory()); got != 1 {
		t.Fatalf("history = %d, want 1 take-means-drop", got)
	}
}

// TestLife5TakeFromFilteredDrops checks TakeFrom shares the verdict path.
func TestLife5TakeFromFilteredDrops(t *testing.T) {
	t.Parallel()

	recv := NewDataReceiver()
	if err := recv.SetTimeBasedFilter(int64(time.Second)); err != nil {
		t.Fatalf("set time filter: %v", err)
	}

	base := time.Unix(8200, 0).UTC()
	recv.now = func() time.Time { return base }

	guid := testWriterGUID()
	sink := &recordingWriter{}
	pub := mustNewPublisher(t, guid, sink)
	mustPublish(t, pub, transports.OutgoingMessage{TopicName: "/immf", TypeName: "std_msgs__msg__String", Payload: encodeTestString(t, "one")})
	mustPublish(t, pub, transports.OutgoingMessage{TopicName: "/immf", TypeName: "std_msgs__msg__String", Payload: encodeTestString(t, "two")})

	payloadOne, err := MarshalRTPSPayload(sink.frameAt(t, 0))
	if err != nil {
		t.Fatalf("marshal one: %v", err)
	}

	payloadTwo, err := MarshalRTPSPayload(sink.frameAt(t, 1))
	if err != nil {
		t.Fatalf("marshal two: %v", err)
	}

	src := &sliceSource{frames: [][]byte{payloadOne, payloadTwo}, guids: []WriterGUID{guid, guid}}
	ctx := context.Background()

	if _, err := recv.TakeFrom(ctx, src); err != nil {
		t.Fatalf("first take from: %v", err)
	}

	recv.now = func() time.Time { return base.Add(100 * time.Millisecond) }
	if _, err := recv.TakeFrom(ctx, src); !errors.Is(err, ErrNoSample) {
		t.Fatalf("filtered take from = %v, want ErrNoSample", err)
	}

	if got := recv.TimeFilteredCount(); got != 1 {
		t.Fatalf("filtered = %d, want 1", got)
	}

	if got := len(recv.SampleHistory()); got != 1 {
		t.Fatalf("history = %d, want 1 take-means-drop", got)
	}
}

// TestLife5OwnershipSuppressesBeforeFirstAccess checks gated takes leave no trace.
func TestLife5OwnershipSuppressesBeforeFirstAccess(t *testing.T) {
	t.Parallel()

	recv := NewDataReceiver()
	handle := InstanceHandle("std_msgs__msg__String", nil)

	if err := recv.SetOwnershipKind(handle, OwnershipExclusive); err != nil {
		t.Fatalf("set kind: %v", err)
	}

	var w1, w2 WriterGUID
	w1[0] = 0x31
	w2[0] = 0x32
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
	mustPublish(t, pub, transports.OutgoingMessage{TopicName: "/ownfirst", TypeName: "std_msgs__msg__String", Payload: encodeTestString(t, "mine")})

	if _, err := recv.Take(sink.lastFrame(t)); !errors.Is(err, ErrNoSample) {
		t.Fatalf("suppressed take = %v, want ErrNoSample", err)
	}

	if got := len(recv.SampleHistory()); got != 0 {
		t.Fatalf("history = %d, want 0 take-means-drop", got)
	}

	st := recv.readerStateFor()
	st.mu.Lock()
	rec := st.instances[handle]
	st.mu.Unlock()

	if rec == nil {
		t.Fatalf("suppressed take installed no instance")
	}

	if rec.view != ViewStateNew {
		t.Fatalf("suppressed take view = %v, want new", rec.view)
	}
}

// TestLife5TimeFilterReceptionFallback checks zero source uses reception.
func TestLife5TimeFilterReceptionFallback(t *testing.T) {
	t.Parallel()

	recv := NewDataReceiver()
	var handle [16]byte
	handle[0] = 0xE5

	if err := recv.SetTimeBasedFilter(int64(time.Second)); err != nil {
		t.Fatalf("set time filter: %v", err)
	}

	st := recv.readerStateFor()
	st.mu.Lock()
	base := time.Unix(8300, 0).UTC()
	first := craftedSample(handle, WriterGUID{}, 1, base)
	first.Info.SourceTimestamp = time.Time{}
	second := craftedSample(handle, WriterGUID{}, 2, base.Add(100*time.Millisecond))
	second.Info.SourceTimestamp = time.Time{}
	st.appendHistoryLocked(first, false)
	st.appendHistoryLocked(second, false)
	st.mu.Unlock()

	got, err := recv.TakeNext(-1, SampleStateAny, ViewStateAny, InstanceStateAny)
	if err != nil {
		t.Fatalf("take next: %v", err)
	}

	if len(got) != 1 {
		t.Fatalf("delivered = %d, want 1 reception fallback", len(got))
	}

	if got := recv.TimeFilteredCount(); got != 1 {
		t.Fatalf("filtered = %d, want 1", got)
	}
}

// TestLife5WatermarkStaysOnFilteredTake checks filtered takes hold the mark.
func TestLife5WatermarkStaysOnFilteredTake(t *testing.T) {
	t.Parallel()

	recv := NewDataReceiver()
	if err := recv.SetTimeBasedFilter(int64(time.Second)); err != nil {
		t.Fatalf("set time filter: %v", err)
	}

	base := time.Unix(8400, 0).UTC()
	recv.now = func() time.Time { return base }

	sink := &recordingWriter{}
	pub := mustNewPublisher(t, testWriterGUID(), sink)

	mustPublish(t, pub, transports.OutgoingMessage{TopicName: "/wm", TypeName: "std_msgs__msg__String", Payload: encodeTestString(t, "one")})
	if _, err := recv.Take(sink.lastFrame(t)); err != nil {
		t.Fatalf("first take: %v", err)
	}

	recv.now = func() time.Time { return base.Add(100 * time.Millisecond) }
	mustPublish(t, pub, transports.OutgoingMessage{TopicName: "/wm", TypeName: "std_msgs__msg__String", Payload: encodeTestString(t, "two")})
	if _, err := recv.Take(sink.lastFrame(t)); !errors.Is(err, ErrNoSample) {
		t.Fatalf("filtered take = %v, want ErrNoSample", err)
	}

	recv.now = func() time.Time { return base.Add(time.Second) }
	mustPublish(t, pub, transports.OutgoingMessage{TopicName: "/wm", TypeName: "std_msgs__msg__String", Payload: encodeTestString(t, "three")})
	if _, err := recv.Take(sink.lastFrame(t)); err != nil {
		t.Fatalf("third take after separation: %v", err)
	}

	if got := recv.TimeFilteredCount(); got != 1 {
		t.Fatalf("filtered = %d, want 1 watermark never moved to dropped sample", got)
	}

	if got := len(recv.SampleHistory()); got != 2 {
		t.Fatalf("history = %d, want 2", got)
	}
}

// TestLife5QueryTakeEvalDistinct checks take eval errors stay distinct.
func TestLife5QueryTakeEvalDistinct(t *testing.T) {
	t.Parallel()

	desc := hookTestStringDesc(t)
	recv := NewDataReceiver()
	mustTakeStrings(t, recv, "/life5qt", "hook_test__msg__Hooked", []string{"hello"})

	miss, err := NewQueryCondition("data = 'absent'", desc, SampleStateAny, ViewStateAny, InstanceStateAny)
	if err != nil {
		t.Fatalf("miss query: %v", err)
	}

	if _, err := miss.Take(recv, -1); !errors.Is(err, ErrNoSample) {
		t.Fatalf("miss take = %v, want ErrNoSample", err)
	}

	recv2 := NewDataReceiver()
	mustTakeStrings(t, recv2, "/life5qt", "hook_test__msg__Hooked", []string{"hello"})

	bad, err := NewQueryCondition("nosuchfield = 1", desc, SampleStateAny, ViewStateAny, InstanceStateAny)
	if err != nil {
		t.Fatalf("bad query compile: %v", err)
	}

	if _, err := bad.Take(recv2, -1); !errors.Is(err, ErrContentFilterEvalError) {
		t.Fatalf("eval take = %v, want ErrContentFilterEvalError distinct", err)
	}
}

// TestLife5ReliableKeepAllBlocksPerInstance checks per-instance blocking.
func TestLife5ReliableKeepAllBlocksPerInstance(t *testing.T) {
	t.Parallel()

	recv := NewDataReceiver()

	err := recv.SetResourceLimits(ResourceLimits{MaxSamples: -1, MaxInstances: -1, MaxSamplesPerInstance: 1})
	if err != nil {
		t.Fatalf("set limits: %v", err)
	}

	recv.SetReliabilityHistoryLife5(true, true)

	sink := &recordingWriter{}
	pub := mustNewPublisher(t, testWriterGUID(), sink)
	mustPublish(t, pub, transports.OutgoingMessage{TopicName: "/life5rel1", TypeName: "std_msgs__msg__String", Payload: encodeTestString(t, "one")})
	if _, err := recv.Take(sink.lastFrame(t)); err != nil {
		t.Fatalf("first take: %v", err)
	}

	mustPublish(t, pub, transports.OutgoingMessage{TopicName: "/life5rel1", TypeName: "std_msgs__msg__String", Payload: encodeTestString(t, "two")})
	if _, err := recv.Take(sink.lastFrame(t)); !errors.Is(err, ErrOwnershipRefused) {
		t.Fatalf("second take = %v, want resource block", err)
	}

	if got := len(recv.SampleHistory()); got != 1 {
		t.Fatalf("history = %d, want 1 preserved, not evicted", got)
	}
}

// autopurgeDisposedHandle disposes one instance and returns its handle.
func autopurgeDisposedHandle(t *testing.T, recv *DataReceiver, topic, payload string) [16]byte {
	t.Helper()

	sink := &recordingWriter{}
	pub := mustNewPublisher(t, testWriterGUID(), sink)
	mustPublish(t, pub, transports.OutgoingMessage{TopicName: topic, TypeName: "std_msgs__msg__String", Payload: encodeTestString(t, payload)})

	if _, err := recv.Take(sink.lastFrame(t)); err != nil {
		t.Fatalf("take disposed topic: %v", err)
	}

	return recv.SampleHistory()[0].Info.InstanceHandle
}

// checkDisposedDelayPurged pins the short disposed delay purge.
func checkDisposedDelayPurged(t *testing.T, base time.Time) {
	t.Helper()

	disposedRecv := NewDataReceiver()
	disposedRecv.SetAutopurgeLife5Delays(int64(time.Hour), int64(time.Second))
	disposedRecv.now = func() time.Time { return base }

	disposedHandle := autopurgeDisposedHandle(t, disposedRecv, "/purged", "d")

	if err := disposedRecv.DisposeInstance(disposedHandle); err != nil {
		t.Fatalf("dispose: %v", err)
	}

	if _, err := disposedRecv.TakeNext(-1, SampleStateAny, ViewStateAny, InstanceStateAny); err != nil {
		t.Fatalf("drain disposed: %v", err)
	}

	disposedRecv.now = func() time.Time { return base.Add(2 * time.Second) }
	if _, err := disposedRecv.TakeNext(-1, SampleStateAny, ViewStateAny, InstanceStateAny); !errors.Is(err, ErrNoSample) {
		t.Fatalf("post-delay take = %v, want ErrNoSample", err)
	}

	if _, ok := disposedRecv.InstanceStateOf(disposedHandle); ok {
		t.Fatalf("disposed instance retained past its delay, want purge")
	}
}

// checkNoWriterDelayPinsSeparate pins no-writer retention across delays.
func checkNoWriterDelayPinsSeparate(t *testing.T, base time.Time) {
	t.Helper()

	writerRecv := NewDataReceiver()
	writerRecv.SetAutopurgeLife5Delays(int64(time.Hour), int64(time.Second))
	writerRecv.now = func() time.Time { return base }

	noWriterHandle := autopurgeDisposedHandle(t, writerRecv, "/purgen", "n")

	if err := writerRecv.UnregisterInstance(noWriterHandle); err != nil {
		t.Fatalf("unregister: %v", err)
	}

	if _, err := writerRecv.TakeNext(-1, SampleStateAny, ViewStateAny, InstanceStateAny); err != nil {
		t.Fatalf("drain nowriter: %v", err)
	}

	writerRecv.now = func() time.Time { return base.Add(2 * time.Second) }
	if _, err := writerRecv.TakeNext(-1, SampleStateAny, ViewStateAny, InstanceStateAny); !errors.Is(err, ErrNoSample) {
		t.Fatalf("early take = %v, want ErrNoSample", err)
	}

	if _, ok := writerRecv.InstanceStateOf(noWriterHandle); !ok {
		t.Fatalf("no-writer instance purged before its delay, want retain")
	}

	writerRecv.now = func() time.Time { return base.Add(2 * time.Hour) }
	if _, err := writerRecv.TakeNext(-1, SampleStateAny, ViewStateAny, InstanceStateAny); !errors.Is(err, ErrNoSample) {
		t.Fatalf("late take = %v, want ErrNoSample", err)
	}

	if _, ok := writerRecv.InstanceStateOf(noWriterHandle); ok {
		t.Fatalf("no-writer instance retained past its delay, want purge")
	}
}

// TestLife5AutopurgeDisposedDelayPinsSeparateDelays checks per-state delays.
func TestLife5AutopurgeDisposedDelayPinsSeparateDelays(t *testing.T) {
	t.Parallel()

	base := time.Unix(8500, 0).UTC()
	checkDisposedDelayPurged(t, base)
	checkNoWriterDelayPinsSeparate(t, base)
}

// TestLife5AutopurgeDropsControls checks purge removes queued controls.
func TestLife5AutopurgeDropsControls(t *testing.T) {
	t.Parallel()

	recv := NewDataReceiver()
	recv.SetAutopurgeLife5Delays(int64(time.Second), int64(time.Second))

	base := time.Unix(8600, 0).UTC()
	recv.now = func() time.Time { return base }

	sink := &recordingWriter{}
	pub := mustNewPublisher(t, testWriterGUID(), sink)
	mustPublish(t, pub, transports.OutgoingMessage{TopicName: "/purgectl", TypeName: "std_msgs__msg__String", Payload: encodeTestString(t, "hi")})
	if _, err := recv.Take(sink.lastFrame(t)); err != nil {
		t.Fatalf("take data: %v", err)
	}

	handle := recv.SampleHistory()[0].Info.InstanceHandle

	if err := recv.DisposeInstance(handle); err != nil {
		t.Fatalf("dispose: %v", err)
	}

	if _, err := recv.TakeNext(-1, SampleStateAny, ViewStateAny, InstanceStateAlive); err != nil {
		t.Fatalf("drain history: %v", err)
	}

	if got := recv.PendingControls(); got != 1 {
		t.Fatalf("pending controls = %d, want 1", got)
	}

	recv.now = func() time.Time { return base.Add(2 * time.Second) }
	if _, err := recv.TakeControlSample(); !errors.Is(err, ErrNoSample) {
		t.Fatalf("control take after purge = %v, want ErrNoSample", err)
	}

	if got := recv.PendingControls(); got != 0 {
		t.Fatalf("pending controls after purge = %d, want 0", got)
	}

	if _, ok := recv.InstanceStateOf(handle); ok {
		t.Fatalf("instance retained past delay with queued control, want purge")
	}
}

// TestLife5AutopurgeNeedsEmptyHistory checks purge only when history empty.
func TestLife5AutopurgeNeedsEmptyHistory(t *testing.T) {
	t.Parallel()

	recv := NewDataReceiver()
	recv.SetAutopurgeLife5Delays(int64(time.Second), int64(time.Second))

	base := time.Unix(5000, 0).UTC()
	recv.now = func() time.Time { return base }

	sink := &recordingWriter{}
	pub := mustNewPublisher(t, testWriterGUID(), sink)
	mustPublish(t, pub, transports.OutgoingMessage{TopicName: "/life5purge", TypeName: "std_msgs__msg__String", Payload: encodeTestString(t, "hi")})
	if _, err := recv.Take(sink.lastFrame(t)); err != nil {
		t.Fatalf("take data: %v", err)
	}

	history := recv.SampleHistory()
	if len(history) == 0 {
		t.Fatalf("history empty after take")
	}

	handle := history[0].Info.InstanceHandle

	// Alive with history must not purge even past delay.
	recv.now = func() time.Time { return base.Add(5 * time.Second) }
	if _, err := recv.TakeNext(1, SampleStateAny, ViewStateAny, InstanceStateAny); err != nil {
		t.Fatalf("drain history: %v", err)
	}

	if _, ok := recv.InstanceStateOf(handle); !ok {
		t.Fatalf("alive instance purged, want retain")
	}

	// Make not-alive with empty history (history drained, control taken).
	if err := recv.DisposeInstance(handle); err != nil {
		t.Fatalf("dispose: %v", err)
	}

	if _, err := recv.TakeControlSample(); err != nil {
		t.Fatalf("take dispose control: %v", err)
	}

	recv.now = func() time.Time { return base.Add(10 * time.Second) }
	if _, err := recv.TakeNext(1, SampleStateAny, ViewStateAny, InstanceStateAny); err == nil {
		t.Fatalf("take next succeeded after purge window, want ErrNoSample")
	}

	if _, ok := recv.InstanceStateOf(handle); ok {
		t.Fatalf("instance retained past autopurge with empty history, want purge")
	}
}

// TestLife5ReliableKeepAllBlocks checks blocking instead of silent evict.
func TestLife5ReliableKeepAllBlocks(t *testing.T) {
	t.Parallel()

	recv := NewDataReceiver()

	err := recv.SetResourceLimits(ResourceLimits{MaxSamples: 1, MaxInstances: -1, MaxSamplesPerInstance: -1})
	if err != nil {
		t.Fatalf("set limits: %v", err)
	}

	recv.SetReliabilityHistoryLife5(true, true)

	sink := &recordingWriter{}
	pub := mustNewPublisher(t, testWriterGUID(), sink)
	mustPublish(t, pub, transports.OutgoingMessage{TopicName: "/life5rel", TypeName: "std_msgs__msg__String", Payload: encodeTestString(t, "one")})
	if _, err := recv.Take(sink.lastFrame(t)); err != nil {
		t.Fatalf("first take: %v", err)
	}

	mustPublish(t, pub, transports.OutgoingMessage{TopicName: "/life5rel", TypeName: "std_msgs__msg__String", Payload: encodeTestString(t, "two")})
	if _, err := recv.Take(sink.lastFrame(t)); !errors.Is(err, ErrOwnershipRefused) {
		t.Fatalf("second take = %v, want resource block", err)
	}

	if got := len(recv.SampleHistory()); got != 1 {
		t.Fatalf("history = %d, want 1 preserved, not evicted", got)
	}

	// Best-effort still evicts.
	recv2 := NewDataReceiver()
	if err := recv2.SetResourceLimits(ResourceLimits{MaxSamples: 1, MaxInstances: -1, MaxSamplesPerInstance: -1}); err != nil {
		t.Fatalf("set limits2: %v", err)
	}

	sink2 := &recordingWriter{}
	pub2 := mustNewPublisher(t, testWriterGUID(), sink2)
	mustPublish(t, pub2, transports.OutgoingMessage{TopicName: "/life5rel2", TypeName: "std_msgs__msg__String", Payload: encodeTestString(t, "one")})
	if _, err := recv2.Take(sink2.lastFrame(t)); err != nil {
		t.Fatalf("first take2: %v", err)
	}

	mustPublish(t, pub2, transports.OutgoingMessage{TopicName: "/life5rel2", TypeName: "std_msgs__msg__String", Payload: encodeTestString(t, "two")})
	if _, err := recv2.Take(sink2.lastFrame(t)); err != nil {
		t.Fatalf("second take2 best-effort: %v", err)
	}

	if got := len(recv2.SampleHistory()); got != 1 {
		t.Fatalf("best-effort history = %d, want 1 evicted", got)
	}
}

// TestLife5TimeFilterSteadyStateDropsOnlyInsideWindow pins minimum_separation.
func TestLife5TimeFilterSteadyStateDropsOnlyInsideWindow(t *testing.T) {
	t.Parallel()

	recv := NewDataReceiver()
	var handle [16]byte
	handle[0] = 0xD0
	if err := recv.SetTimeBasedFilter(int64(time.Second)); err != nil {
		t.Fatalf("set filter: %v", err)
	}

	st := recv.readerStateFor()
	st.mu.Lock()
	base := time.Unix(9500, 0).UTC()
	st.appendHistoryLocked(craftedSample(handle, WriterGUID{}, 1, base), false)
	st.appendHistoryLocked(craftedSample(handle, WriterGUID{}, 2, base.Add(time.Second)), false)
	st.appendHistoryLocked(craftedSample(handle, WriterGUID{}, 3, base.Add(time.Second).Add(500*time.Millisecond)), false)
	st.mu.Unlock()

	got, err := recv.TakeNext(-1, SampleStateAny, ViewStateAny, InstanceStateAny)
	if err != nil {
		t.Fatalf("take next: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("delivered = %d, want 2 (boundary passes, inside drops)", len(got))
	}
	if got[0].Info.SampleSequence != 1 || got[1].Info.SampleSequence != 2 {
		t.Fatalf("steady order = %d %d, want 1 2", got[0].Info.SampleSequence, got[1].Info.SampleSequence)
	}
	if c := recv.TimeFilteredCount(); c != 1 {
		t.Fatalf("filtered = %d, want 1", c)
	}

	if _, err := recv.ReadNext(-1, SampleStateAny, ViewStateAny, InstanceStateAny); !errors.Is(err, ErrNoSample) {
		t.Fatalf("take-means-drop history retained, want ErrNoSample")
	}
}

// TestLife5DestinationOrderSourceTimestampCollect pins BY_SOURCE order.
func TestLife5DestinationOrderSourceTimestampCollect(t *testing.T) {
	t.Parallel()

	recv := NewDataReceiver()
	if err := recv.SetDestinationOrder(DestinationOrderBySourceTimestamp); err != nil {
		t.Fatalf("set order: %v", err)
	}
	var handle [16]byte
	handle[0] = 0xD1

	st := recv.readerStateFor()
	st.mu.Lock()
	base := time.Unix(9600, 0).UTC()
	newerRecv := craftedSample(handle, WriterGUID{}, 2, base.Add(time.Second))
	newerRecv.Info.SourceTimestamp = base.Add(3 * time.Second)
	newerRecv.Info.ReceptionTimestamp = base
	olderRecv := craftedSample(handle, WriterGUID{}, 1, base)
	olderRecv.Info.SourceTimestamp = base.Add(time.Second)
	olderRecv.Info.ReceptionTimestamp = base.Add(2 * time.Second)
	st.appendHistoryLocked(newerRecv, false)
	st.appendHistoryLocked(olderRecv, false)
	st.mu.Unlock()

	got, err := recv.TakeNext(-1, SampleStateAny, ViewStateAny, InstanceStateAny)
	if err != nil {
		t.Fatalf("take next: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("delivered = %d, want 2", len(got))
	}
	if got[0].Info.SampleSequence != 1 || got[1].Info.SampleSequence != 2 {
		t.Fatalf("source order = %d %d, want 1 2", got[0].Info.SampleSequence, got[1].Info.SampleSequence)
	}

	recv2 := NewDataReceiver()
	st2 := recv2.readerStateFor()
	st2.mu.Lock()
	st2.appendHistoryLocked(newerRecv, false)
	st2.appendHistoryLocked(olderRecv, false)
	st2.mu.Unlock()
	got2, err := recv2.TakeNext(-1, SampleStateAny, ViewStateAny, InstanceStateAny)
	if err != nil {
		t.Fatalf("reception take: %v", err)
	}
	if got2[0].Info.SampleSequence != 2 {
		t.Fatalf("default reception order wrong, got %d want 2 first", got2[0].Info.SampleSequence)
	}
}

// TestLife5ReliableBlocksHistoryFull pins blocking versus best-effort drop.
func TestLife5ReliableBlocksHistoryFull(t *testing.T) {
	t.Parallel()

	recv := NewDataReceiver()
	if err := recv.SetResourceLimits(ResourceLimits{MaxSamples: 1, MaxInstances: -1, MaxSamplesPerInstance: -1}); err != nil {
		t.Fatalf("set limits: %v", err)
	}
	recv.SetReliabilityHistoryLife5(true, true)

	sink := &recordingWriter{}
	pub := mustNewPublisher(t, testWriterGUID(), sink)
	mustPublish(t, pub, transports.OutgoingMessage{TopicName: "/blk", TypeName: "std_msgs__msg__String", Payload: encodeTestString(t, "one")})
	if _, err := recv.Take(sink.lastFrame(t)); err != nil {
		t.Fatalf("first take: %v", err)
	}
	mustPublish(t, pub, transports.OutgoingMessage{TopicName: "/blk", TypeName: "std_msgs__msg__String", Payload: encodeTestString(t, "two")})
	if _, err := recv.Take(sink.lastFrame(t)); !errors.Is(err, ErrOwnershipRefused) {
		t.Fatalf("reliable full = %v, want ErrOwnershipRefused block", err)
	}
	if got := len(recv.SampleHistory()); got != 1 {
		t.Fatalf("blocked history = %d, want 1 preserved", got)
	}
	if got := recv.ResourceViolations(); got != 1 {
		t.Fatalf("blocked violations = %d, want 1", got)
	}
}

// TestLife5HistoryDepthBoundsCollect pins KEEP_LAST depth enforcement.
func TestLife5HistoryDepthBoundsCollect(t *testing.T) {
	t.Parallel()

	recv := NewDataReceiver()
	recv.SetReliabilityHistoryLife5(false, false)
	if err := recv.SetHistoryDepthLife5(1); err != nil {
		t.Fatalf("set depth: %v", err)
	}

	sink := &recordingWriter{}
	pub := mustNewPublisher(t, testWriterGUID(), sink)
	mustPublish(t, pub, transports.OutgoingMessage{TopicName: "/depth", TypeName: "std_msgs__msg__String", Payload: encodeTestString(t, "one")})
	if _, err := recv.Take(sink.lastFrame(t)); err != nil {
		t.Fatalf("first take: %v", err)
	}
	mustPublish(t, pub, transports.OutgoingMessage{TopicName: "/depth", TypeName: "std_msgs__msg__String", Payload: encodeTestString(t, "two")})
	if _, err := recv.Take(sink.lastFrame(t)); err != nil {
		t.Fatalf("second take: %v", err)
	}
	if got := len(recv.SampleHistory()); got != 1 {
		t.Fatalf("depth history = %d, want 1", got)
	}
	if got := recv.ResourceViolations(); got != 1 {
		t.Fatalf("depth violations = %d, want 1", got)
	}
}
