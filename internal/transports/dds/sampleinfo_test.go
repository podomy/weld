// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

package dds

import (
	"testing"
	"time"

	"github.com/podomy/weld/internal/transports"
)

// mustSampleFrame publishes one message and returns its DATA frame.
func mustSampleFrame(t *testing.T, topic, typeName string, payload []byte) []byte {
	t.Helper()

	sink := &recordingWriter{}
	pub := mustNewPublisher(t, testWriterGUID(), sink)
	mustPublish(t, pub, transports.OutgoingMessage{TopicName: topic, TypeName: typeName, Payload: payload})

	return sink.lastFrame(t)
}

// TestTakeSampledReportsInfo checks the extended take carries sample info.
func TestTakeSampledReportsInfo(t *testing.T) {
	t.Parallel()

	recv := NewDataReceiver()
	frame := mustSampleFrame(t, "/chatter", "std_msgs__msg__String", encodeTestString(t, "hello"))

	got, err := recv.TakeSampled(frame)
	if err != nil {
		t.Fatalf("take sampled: %v", err)
	}

	if got.Msg.TopicName != "/chatter" {
		t.Fatalf("topic = %q, want /chatter", got.Msg.TopicName)
	}

	if !got.Info.ValidData {
		t.Fatalf("valid data = false, want true")
	}

	if got.Info.InstanceState != InstanceStateAlive {
		t.Fatalf("instance state = %v, want alive", got.Info.InstanceState)
	}

	if got.Info.ViewState != ViewStateNew {
		t.Fatalf("view state = %v, want new", got.Info.ViewState)
	}

	if got.Info.SampleSequence != FirstSequenceNumber {
		t.Fatalf("sequence = %d, want %d", got.Info.SampleSequence, FirstSequenceNumber)
	}

	if got.Info.ReceptionTimestamp.IsZero() {
		t.Fatalf("reception timestamp is zero")
	}
}

// TestTakeSampleInfoSecondViewNotNew checks view flips after first access.
func TestTakeSampleInfoSecondViewNotNew(t *testing.T) {
	t.Parallel()

	recv := NewDataReceiver()
	sink := &recordingWriter{}
	pub := mustNewPublisher(t, testWriterGUID(), sink)
	mustPublish(t, pub, transports.OutgoingMessage{TopicName: "/chatter", TypeName: "std_msgs__msg__String", Payload: encodeTestString(t, "one")})
	mustPublish(t, pub, transports.OutgoingMessage{TopicName: "/chatter", TypeName: "std_msgs__msg__String", Payload: encodeTestString(t, "two")})

	first, err := recv.TakeSampled(sink.frameAt(t, 0))
	if err != nil {
		t.Fatalf("first take: %v", err)
	}

	second, err := recv.Take(sink.frameAt(t, 1))
	if err != nil {
		t.Fatalf("second take: %v", err)
	}
	_ = second

	info, ok := recv.LastSampleInfo()
	if !ok {
		t.Fatalf("last sample info missing")
	}

	if info.ViewState != ViewStateNotNew {
		t.Fatalf("view state = %v, want not_new", info.ViewState)
	}

	if info.SampleSequence != 2 {
		t.Fatalf("sequence = %d, want 2", info.SampleSequence)
	}

	if first.Info.InstanceHandle != info.InstanceHandle {
		t.Fatalf("instance handle changed across samples on one topic")
	}
}

// TestSampleHistoryRetainsTakes checks the side channel keeps every take.
func TestSampleHistoryRetainsTakes(t *testing.T) {
	t.Parallel()

	recv := NewDataReceiver()
	sink := &recordingWriter{}
	pub := mustNewPublisher(t, testWriterGUID(), sink)

	for _, text := range []string{"a", "b", "c"} {
		mustPublish(t, pub, transports.OutgoingMessage{TopicName: "/hist", TypeName: "std_msgs__msg__String", Payload: encodeTestString(t, text)})
	}

	for i := range 3 {
		_, err := recv.Take(sink.frameAt(t, i))
		if err != nil {
			t.Fatalf("take %d: %v", i, err)
		}
	}

	history := recv.SampleHistory()
	if len(history) != 3 {
		t.Fatalf("history = %d, want 3", len(history))
	}

	for _, entry := range history {
		if entry.Info.SampleState != SampleStateRead {
			t.Fatalf("retained state = %v, want read", entry.Info.SampleState)
		}
	}
}

// TestLife5ArrivalNeverFlipsView checks observe leaves view alone.
//
// Arrival snapshots the live view into the info and never flips the
// record: only a delivered take or collect flips on access. Two back to
// back observations therefore both report new with the record new.
func TestLife5ArrivalNeverFlipsView(t *testing.T) {
	t.Parallel()

	recv := NewDataReceiver()
	at := time.Unix(9000, 0).UTC()

	first, err := recv.observeData("/arrive", "std_msgs__msg__String", testWriterGUID(), 1, encodeTestString(t, "one"), at)
	if err != nil {
		t.Fatalf("first observe: %v", err)
	}

	second, err := recv.observeData("/arrive", "std_msgs__msg__String", testWriterGUID(), 2, encodeTestString(t, "two"), at.Add(time.Second))
	if err != nil {
		t.Fatalf("second observe: %v", err)
	}

	if first.ViewState != ViewStateNew || second.ViewState != ViewStateNew {
		t.Fatalf("arrival views = %v %v, want new new", first.ViewState, second.ViewState)
	}

	st := recv.readerStateFor()
	st.mu.Lock()
	rec := st.instances[first.InstanceHandle]
	st.mu.Unlock()

	if rec == nil {
		t.Fatalf("arrival installed no instance")
	}

	if rec.view != ViewStateNew {
		t.Fatalf("arrival record view = %v, want new", rec.view)
	}
}

// TestSampleHistoryEmpty checks the side channel starts empty.
func TestSampleHistoryEmpty(t *testing.T) {
	t.Parallel()

	recv := NewDataReceiver()

	if _, ok := recv.LastSampleInfo(); ok {
		t.Fatalf("last info present before any take")
	}

	if got := recv.SampleHistory(); len(got) != 0 {
		t.Fatalf("history = %d, want 0", len(got))
	}
}

// TestLife5ViewFlipsOnAccessNotArrival pins DDS 2.2.2.5.1.8 view semantics.
func TestLife5ViewFlipsOnAccessNotArrival(t *testing.T) {
	t.Parallel()

	recv := NewDataReceiver()
	at := time.Unix(9300, 0).UTC()

	first, err := recv.observeData("/view", "std_msgs__msg__String", testWriterGUID(), 1, encodeTestString(t, "one"), at)
	if err != nil {
		t.Fatalf("first observe: %v", err)
	}
	if first.ViewState != ViewStateNew {
		t.Fatalf("arrival view = %v, want new", first.ViewState)
	}

	st := recv.readerStateFor()
	st.mu.Lock()
	rec := st.instances[first.InstanceHandle]
	before := rec.view
	st.mu.Unlock()
	if before != ViewStateNew {
		t.Fatalf("record view after arrival = %v, want new", before)
	}

	got, err := recv.ReadNext(-1, SampleStateAny, ViewStateAny, InstanceStateAny)
	if err != nil {
		t.Fatalf("read next: %v", err)
	}
	if got[0].Info.ViewState != ViewStateNew {
		t.Fatalf("delivered snapshot = %v, want new pre-flip", got[0].Info.ViewState)
	}

	st.mu.Lock()
	after := st.instances[first.InstanceHandle].view
	st.mu.Unlock()
	if after != ViewStateNotNew {
		t.Fatalf("record view after access = %v, want not_new", after)
	}

	base := time.Unix(9310, 0).UTC()
	recv2 := NewDataReceiver()
	if err := recv2.SetTimeBasedFilter(int64(time.Second)); err != nil {
		t.Fatalf("set time filter: %v", err)
	}
	recv2.now = func() time.Time { return base }
	sink := &recordingWriter{}
	pub := mustNewPublisher(t, testWriterGUID(), sink)
	mustPublish(t, pub, transports.OutgoingMessage{TopicName: "/viewf", TypeName: "std_msgs__msg__String", Payload: encodeTestString(t, "one")})
	if _, err := recv2.Take(sink.lastFrame(t)); err != nil {
		t.Fatalf("first take: %v", err)
	}
	recv2.now = func() time.Time { return base.Add(100 * time.Millisecond) }
	mustPublish(t, pub, transports.OutgoingMessage{TopicName: "/viewf", TypeName: "std_msgs__msg__String", Payload: encodeTestString(t, "two")})
	if _, err := recv2.Take(sink.lastFrame(t)); err == nil {
		t.Fatalf("filtered take passed, want drop")
	}
	st2Handle := recv2.SampleHistory()[0].Info.InstanceHandle
	st2 := recv2.readerStateFor()
	st2.mu.Lock()
	view := st2.instances[st2Handle].view
	st2.mu.Unlock()
	if view != ViewStateNotNew {
		t.Fatalf("view after filtered take = %v, want not_new (first delivery already viewed)", view)
	}
}

// TestLife5AutopurgeEvictsNotAlive pins reader autopurge per DDS 2.2.3.22.
func TestLife5AutopurgeEvictsNotAlive(t *testing.T) {
	t.Parallel()

	base := time.Unix(9400, 0).UTC()
	recv := NewDataReceiver()
	recv.SetAutopurgeLife5Delays(int64(time.Hour), int64(time.Second))
	sink := &recordingWriter{}
	pub := mustNewPublisher(t, testWriterGUID(), sink)
	mustPublish(t, pub, transports.OutgoingMessage{TopicName: "/ap", TypeName: "std_msgs__msg__String", Payload: encodeTestString(t, "hi")})
	recv.now = func() time.Time { return base }
	if _, err := recv.Take(sink.lastFrame(t)); err != nil {
		t.Fatalf("take: %v", err)
	}
	handle := recv.SampleHistory()[0].Info.InstanceHandle

	if err := recv.DisposeInstance(handle); err != nil {
		t.Fatalf("dispose: %v", err)
	}
	if _, err := recv.TakeControlSample(); err != nil {
		t.Fatalf("drain control: %v", err)
	}
	if _, err := recv.TakeNext(-1, SampleStateAny, ViewStateAny, InstanceStateAny); err != nil {
		t.Fatalf("drain history: %v", err)
	}

	recv.now = func() time.Time { return base.Add(2 * time.Second) }
	if _, err := recv.TakeNext(-1, SampleStateAny, ViewStateAny, InstanceStateAny); err == nil {
		t.Fatalf("take after purge window succeeded, want ErrNoSample")
	}
	if _, ok := recv.InstanceStateOf(handle); ok {
		t.Fatalf("disposed instance retained past delay with empty history")
	}

	recvN := NewDataReceiver()
	recvN.SetAutopurgeLife5Delays(int64(time.Hour), int64(time.Hour))
	recvN.now = func() time.Time { return base }
	sinkN := &recordingWriter{}
	pubN := mustNewPublisher(t, testWriterGUID(), sinkN)
	mustPublish(t, pubN, transports.OutgoingMessage{TopicName: "/apn", TypeName: "std_msgs__msg__String", Payload: encodeTestString(t, "hi")})
	if _, err := recvN.Take(sinkN.lastFrame(t)); err != nil {
		t.Fatalf("takeN: %v", err)
	}
	handleN := recvN.SampleHistory()[0].Info.InstanceHandle
	if err := recvN.UnregisterInstance(handleN); err != nil {
		t.Fatalf("unregister: %v", err)
	}
	recvN.now = func() time.Time { return base.Add(30 * time.Minute) }
	if _, err := recvN.TakeNext(-1, SampleStateAny, ViewStateAny, InstanceStateAny); err != nil {
		t.Fatalf("drainN: %v", err)
	}
	if _, ok := recvN.InstanceStateOf(handleN); !ok {
		t.Fatalf("no-writers purged before its delay")
	}
}

// TestSweepAgedInstancesTimeBased pins the on-demand time-based sweep
// helper: it purges nothing before the autopurge delay and reports plus
// removes exactly the expired not-alive instance after the delay, so a
// background sweep loop can drive expiry without waiting for reads.
func TestSweepAgedInstancesTimeBased(t *testing.T) {
	t.Parallel()

	base := time.Unix(9500, 0).UTC()
	recv := NewDataReceiver()
	recv.SetAutopurgeLife5Delays(int64(time.Hour), int64(time.Second))
	sink := &recordingWriter{}
	pub := mustNewPublisher(t, testWriterGUID(), sink)
	mustPublish(t, pub, transports.OutgoingMessage{TopicName: "/sweep", TypeName: "std_msgs__msg__String", Payload: encodeTestString(t, "hi")})
	recv.now = func() time.Time { return base }
	if _, err := recv.Take(sink.lastFrame(t)); err != nil {
		t.Fatalf("take: %v", err)
	}
	handle := recv.SampleHistory()[0].Info.InstanceHandle

	if err := recv.DisposeInstance(handle); err != nil {
		t.Fatalf("dispose: %v", err)
	}
	if _, err := recv.TakeControlSample(); err != nil {
		t.Fatalf("drain control: %v", err)
	}
	if _, err := recv.TakeNext(-1, SampleStateAny, ViewStateAny, InstanceStateAny); err != nil {
		t.Fatalf("drain history: %v", err)
	}

	if got := recv.SweepAgedInstances(base.Add(500 * time.Millisecond).UnixNano()); got != 0 {
		t.Fatalf("sweep before delay = %d, want 0", got)
	}
	if _, ok := recv.InstanceStateOf(handle); !ok {
		t.Fatalf("disposed instance swept before its delay")
	}

	if got := recv.SweepAgedInstances(base.Add(2 * time.Second).UnixNano()); got != 1 {
		t.Fatalf("sweep past delay = %d, want 1", got)
	}
	if _, ok := recv.InstanceStateOf(handle); ok {
		t.Fatalf("disposed instance retained past delay by sweep")
	}
}
