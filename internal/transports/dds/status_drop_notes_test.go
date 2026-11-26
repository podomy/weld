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

// TestTakeDropNotesRequestedDeadline checks deadline drops update status.
func TestTakeDropNotesRequestedDeadline(t *testing.T) {
	t.Parallel()

	recv := NewDataReceiver()
	t0 := time.Unix(5000, 0).UTC()
	recv.now = func() time.Time { return t0 }
	if err := recv.SetDeadlineTrackerForTopic("/d", core.NewDuration(1, 0)); err != nil {
		t.Fatalf("set deadline: %v", err)
	}

	sink := &recordingWriter{}
	pub := mustNewPublisher(t, testWriterGUID(), sink)
	mustPublish(t, pub, transports.OutgoingMessage{TopicName: "/d", TypeName: "std_msgs__msg__String", Payload: encodeTestString(t, "late")})

	recv.now = func() time.Time { return t0.Add(2 * time.Second) }
	if _, err := recv.Take(sink.lastFrame(t)); !errors.Is(err, ErrDeadlineMissed) {
		t.Fatalf("take = %v, want ErrDeadlineMissed", err)
	}

	status := recv.RequestedDeadlineMissedStatus()
	if status.TotalCount != 1 || status.TotalCountChange != 1 {
		t.Fatalf("status = %+v, want total 1 change 1", status)
	}
}

// TestTakeDropNotesLivelinessLost checks expiry drops update statuses.
func TestTakeDropNotesLivelinessLost(t *testing.T) {
	t.Parallel()

	recv := NewDataReceiver()
	var guid WriterGUID
	guid[0] = 0xD1

	lease := core.NewDuration(0, 1)
	start := time.Now().Add(-time.Hour).UnixNano()
	tracker := core.NewLivelinessTracker("/e", "dead-writer", core.LivelinessManualByTopic, lease, start)
	recv.SetLivelinessTracker(guid, &tracker)

	sink := &recordingWriter{}
	pub := mustNewPublisher(t, guid, sink)
	mustPublish(t, pub, transports.OutgoingMessage{TopicName: "/e", TypeName: "std_msgs__msg__String", Payload: encodeTestString(t, "ancient")})

	if _, err := recv.Take(sink.lastFrame(t)); !errors.Is(err, ErrLivelinessExpired) {
		t.Fatalf("take = %v, want ErrLivelinessExpired", err)
	}

	lost := recv.LivelinessLostStatus()
	if lost.TotalCount != 0 {
		t.Fatalf("lost = %+v, want total 0 reader drops never count writer loss", lost)
	}

	var handle [16]byte
	copy(handle[:], guid[:])
	changed := recv.LivelinessChangedStatus()
	if changed.NotAliveCount != 1 || changed.LastPublicationHandle != handle {
		t.Fatalf("changed = %+v, want one not-alive writer", changed)
	}
}

// TestCheckRequestedDeadlineAttributesTopicInstance checks status handle fix.
func TestCheckRequestedDeadlineAttributesTopicInstance(t *testing.T) {
	t.Parallel()

	recv := NewDataReceiver()
	t0 := time.Unix(6000, 0).UTC()
	recv.now = func() time.Time { return t0 }
	if err := recv.SetDeadlineTrackerForTopic("/h", core.NewDuration(1, 0)); err != nil {
		t.Fatalf("set deadline h: %v", err)
	}

	sink := &recordingWriter{}
	pub := mustNewPublisher(t, testWriterGUID(), sink)
	mustPublish(t, pub, transports.OutgoingMessage{TopicName: "/h", TypeName: "std_msgs__msg__String", Payload: encodeTestString(t, "hi")})

	if _, err := recv.Take(sink.lastFrame(t)); err != nil {
		t.Fatalf("take: %v", err)
	}

	history := recv.SampleHistory()
	if len(history) != 1 {
		t.Fatalf("history = %d, want 1", len(history))
	}

	later := t0.Add(2 * time.Second).UnixNano()
	if _, ok := recv.CheckRequestedDeadline("/h", later); !ok {
		t.Fatalf("check missed the lapsed deadline")
	}

	status := recv.RequestedDeadlineMissedStatus()
	if status.LastInstanceHandle != history[0].Info.InstanceHandle {
		t.Fatalf("last handle = %x, want %x", status.LastInstanceHandle, history[0].Info.InstanceHandle)
	}
}

// TestTakeSuccessNotesWriterAlive checks delivery marks the writer alive.
func TestTakeSuccessNotesWriterAlive(t *testing.T) {
	t.Parallel()

	recv := NewDataReceiver()
	sink := &recordingWriter{}
	guid := testWriterGUID()
	pub := mustNewPublisher(t, guid, sink)
	mustPublish(t, pub, transports.OutgoingMessage{TopicName: "/alive", TypeName: "std_msgs__msg__String", Payload: encodeTestString(t, "hi")})

	if _, err := recv.Take(sink.lastFrame(t)); err != nil {
		t.Fatalf("take: %v", err)
	}

	var handle [16]byte
	copy(handle[:], guid[:])
	changed := recv.LivelinessChangedStatus()
	if changed.AliveCount != 1 || changed.LastPublicationHandle != handle {
		t.Fatalf("changed = %+v, want one alive writer", changed)
	}
}
