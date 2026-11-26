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

// TestListenerNotifyFansOut checks registration plus delivery plus removal.
func TestListenerNotifyFansOut(t *testing.T) {
	t.Parallel()

	recv := NewDataReceiver()
	var calls atomic.Int64

	remove := recv.AddListener("/chatter", &DataListener{
		OnData: func(sample SampledMessage) {
			calls.Add(1)
		},
	})

	if got := recv.ListenerCount("/chatter"); got != 1 {
		t.Fatalf("listener count = %d, want 1", got)
	}

	var sample SampledMessage
	sample.Msg.TopicName = "/chatter"
	recv.NotifyData("/chatter", sample)

	if got := calls.Load(); got != 1 {
		t.Fatalf("calls = %d, want 1", got)
	}

	remove()
	remove()

	if got := recv.ListenerCount("/chatter"); got != 0 {
		t.Fatalf("listener count after remove = %d, want 0", got)
	}

	recv.NotifyData("/chatter", sample)

	if got := calls.Load(); got != 1 {
		t.Fatalf("calls after remove = %d, want 1", got)
	}
}

// TestListenerPanicIsolated checks a failing listener never breaks fan-out.
func TestListenerPanicIsolated(t *testing.T) {
	t.Parallel()

	recv := NewDataReceiver()
	var calls atomic.Int64

	recv.AddListener("/chatter", &DataListener{
		OnData: func(sample SampledMessage) {
			panic("boom")
		},
	})
	recv.AddListener("/chatter", &DataListener{
		OnData: func(sample SampledMessage) {
			calls.Add(1)
		},
	})

	var sample SampledMessage
	recv.NotifyData("/chatter", sample)

	if got := calls.Load(); got != 1 {
		t.Fatalf("calls = %d, want 1", got)
	}
}

// TestListenerEdgeCallbacks checks liveliness plus deadline fan-out.
func TestListenerEdgeCallbacks(t *testing.T) {
	t.Parallel()

	recv := NewDataReceiver()
	var aliveCalls, deadlineCalls atomic.Int64

	recv.AddListener("/chatter", &DataListener{
		OnLiveliness: func(handle [16]byte, alive bool) {
			aliveCalls.Add(1)
		},
		OnDeadline: func(topic string) {
			if topic != "/chatter" {
				t.Errorf("deadline topic = %q, want /chatter", topic)
			}

			deadlineCalls.Add(1)
		},
	})

	var handle [16]byte
	recv.NotifyLiveliness("/chatter", handle, true)
	recv.NotifyDeadline("/chatter")

	if got := aliveCalls.Load(); got != 1 {
		t.Fatalf("liveliness calls = %d, want 1", got)
	}

	if got := deadlineCalls.Load(); got != 1 {
		t.Fatalf("deadline calls = %d, want 1", got)
	}
}

// TestLife5ListenerTakeNextFansEachDelivery checks one notify per sample.
func TestLife5ListenerTakeNextFansEachDelivery(t *testing.T) {
	t.Parallel()

	recv := NewDataReceiver()
	var calls atomic.Int64

	recv.AddListener("/fanout", &DataListener{
		OnData: func(sample SampledMessage) {
			calls.Add(1)
		},
	})

	sink := &recordingWriter{}
	pub := mustNewPublisher(t, testWriterGUID(), sink)
	mustPublish(t, pub, transports.OutgoingMessage{TopicName: "/fanout", TypeName: "std_msgs__msg__String", Payload: encodeTestString(t, "one")})
	mustPublish(t, pub, transports.OutgoingMessage{TopicName: "/fanout", TypeName: "std_msgs__msg__String", Payload: encodeTestString(t, "two")})

	if _, err := recv.Take(sink.frameAt(t, 0)); err != nil {
		t.Fatalf("first take: %v", err)
	}

	if _, err := recv.Take(sink.frameAt(t, 1)); err != nil {
		t.Fatalf("second take: %v", err)
	}

	if got := calls.Load(); got != 2 {
		t.Fatalf("data calls after takes = %d, want 1 per delivery", got)
	}

	got, err := recv.TakeNext(-1, SampleStateAny, ViewStateAny, InstanceStateAny)
	if err != nil {
		t.Fatalf("take next: %v", err)
	}

	if len(got) != 2 {
		t.Fatalf("take next = %d, want 2", len(got))
	}

	if got := calls.Load(); got != 4 {
		t.Fatalf("data calls after take next = %d, want 1 per delivery", got)
	}
}

// TestLife5ListenerDeadlineFiresOnce checks one notify per missed deadline.
func TestLife5ListenerDeadlineFiresOnce(t *testing.T) {
	t.Parallel()

	recv := NewDataReceiver()
	var calls atomic.Int64

	recv.AddListener("/dl", &DataListener{
		OnDeadline: func(topic string) {
			if topic != "/dl" {
				t.Errorf("deadline topic = %q, want /dl", topic)
			}

			calls.Add(1)
		},
	})

	t0 := time.Unix(8700, 0).UTC()
	recv.now = func() time.Time { return t0 }
	if err := recv.SetDeadlineTrackerForTopic("/dl", core.NewDuration(1, 0)); err != nil {
		t.Fatalf("set deadline: %v", err)
	}

	sink := &recordingWriter{}
	pub := mustNewPublisher(t, testWriterGUID(), sink)
	mustPublish(t, pub, transports.OutgoingMessage{TopicName: "/dl", TypeName: "std_msgs__msg__String", Payload: encodeTestString(t, "hi")})
	if _, err := recv.Take(sink.lastFrame(t)); err != nil {
		t.Fatalf("take: %v", err)
	}

	later := t0.Add(2 * time.Second).UnixNano()
	if _, ok := recv.CheckRequestedDeadline("/dl", later); !ok {
		t.Fatalf("check missed the lapsed deadline")
	}

	if got := calls.Load(); got != 1 {
		t.Fatalf("deadline calls = %d, want 1", got)
	}

	if _, ok := recv.CheckRequestedDeadline("/dl", later); ok {
		t.Fatalf("second check alerted in the same outage, want quiet")
	}

	if got := calls.Load(); got != 1 {
		t.Fatalf("deadline calls after quiet check = %d, want 1 once per edge", got)
	}
}

// TestLife5ListenerLivelinessEdgeOnce checks one notify per aliveness edge.
func TestLife5ListenerLivelinessEdgeOnce(t *testing.T) {
	t.Parallel()

	recv := NewDataReceiver()

	var calls atomic.Int64
	var lastAlive atomic.Bool
	lastAlive.Store(true)

	recv.AddListener("/edge", &DataListener{
		OnLiveliness: func(handle [16]byte, alive bool) {
			lastAlive.Store(alive)
			calls.Add(1)
		},
	})

	var guid WriterGUID
	guid[0] = 0xD2

	lease := core.NewDuration(0, 1)
	start := time.Now().Add(-time.Hour).UnixNano()
	tracker := core.NewLivelinessTracker("/edge", "dead-writer", core.LivelinessManualByTopic, lease, start)
	recv.SetLivelinessTracker(guid, &tracker)

	sink := &recordingWriter{}
	pub := mustNewPublisher(t, guid, sink)
	mustPublish(t, pub, transports.OutgoingMessage{TopicName: "/edge", TypeName: "std_msgs__msg__String", Payload: encodeTestString(t, "old")})
	if _, err := recv.Take(sink.lastFrame(t)); err == nil {
		t.Fatalf("first expired take passed, want drop")
	}

	mustPublish(t, pub, transports.OutgoingMessage{TopicName: "/edge", TypeName: "std_msgs__msg__String", Payload: encodeTestString(t, "older")})
	if _, err := recv.Take(sink.lastFrame(t)); err == nil {
		t.Fatalf("second expired take passed, want drop")
	}

	if got := calls.Load(); got != 1 {
		t.Fatalf("liveliness calls = %d, want 1 once per edge", got)
	}

	if lastAlive.Load() {
		t.Fatalf("last aliveness = true, want false edge")
	}

	if got := recv.LivelinessLostStatus().TotalCount; got != 0 {
		t.Fatalf("writer loss count = %d, want 0 reader drops never bump it", got)
	}
}

// TestListenerPollerDispatches checks the background poller invokes callbacks.
func TestListenerPollerDispatches(t *testing.T) {
	t.Parallel()

	recv := NewDataReceiver()
	var calls atomic.Int64

	recv.AddListener("/polled", &DataListener{
		OnData: func(sample SampledMessage) {
			calls.Add(1)
		},
	})

	var remaining atomic.Int64
	remaining.Store(3)

	var poller ListenerPoller
	err := poller.Start(context.Background(), recv, "/polled", func(ctx context.Context) (SampledMessage, error) {
		if remaining.Add(-1) < 0 {
			<-ctx.Done()

			return SampledMessage{}, ctx.Err()
		}

		var sample SampledMessage
		sample.Msg.TopicName = "/polled"

		return sample, nil
	})
	if err != nil {
		t.Fatalf("poller start: %v", err)
	}

	deadline := time.Now().Add(5 * time.Second)
	for calls.Load() != 3 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}

	if got := calls.Load(); got != 3 {
		t.Fatalf("calls = %d, want 3", got)
	}

	poller.Stop()

	stats := poller.Stats()
	if stats.Delivered != 3 {
		t.Fatalf("delivered = %d, want 3", stats.Delivered)
	}

	if stats.Running {
		t.Fatalf("poller still running after stop")
	}
}

// TestListenerPollerRejectsBadInput checks constructor validation.
func TestListenerPollerRejectsBadInput(t *testing.T) {
	t.Parallel()

	var poller ListenerPoller

	err := poller.Start(context.Background(), nil, "/x", func(ctx context.Context) (SampledMessage, error) {
		return SampledMessage{}, errors.New("dds: no poll")
	})
	if !errors.Is(err, ErrInvalidFrame) {
		t.Fatalf("nil receiver start = %v, want ErrInvalidFrame", err)
	}

	err = poller.Start(context.Background(), NewDataReceiver(), "/x", nil)
	if !errors.Is(err, ErrInvalidFrame) {
		t.Fatalf("nil poll start = %v, want ErrInvalidFrame", err)
	}

	poller.Stop()
}
