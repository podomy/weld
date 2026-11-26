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

// TestParticipantExpiryGatesTake checks manual-by-participant gating on takes.
func TestParticipantExpiryGatesTake(t *testing.T) {
	t.Parallel()

	recv := NewDataReceiver()
	var guid WriterGUID
	for i := range 12 {
		guid[i] = byte(i + 0xA0)
	}

	name := participantNameForWriter(guid)
	t0 := time.Unix(8000, 0).UTC()
	recv.now = func() time.Time { return t0 }
	recv.SetParticipantLease(name, core.NewDuration(0, 1))

	if !recv.AssertParticipantAt(name, t0.UnixNano()) {
		t.Fatalf("assert did not install the participant")
	}

	sink := &recordingWriter{}
	pub := mustNewPublisher(t, guid, sink)
	mustPublish(t, pub, transports.OutgoingMessage{TopicName: "/pgate", TypeName: "std_msgs__msg__String", Payload: encodeTestString(t, "x")})

	recv.now = func() time.Time { return t0.Add(time.Hour) }
	if _, err := recv.Take(sink.lastFrame(t)); !errors.Is(err, ErrLivelinessExpired) {
		t.Fatalf("take = %v, want ErrLivelinessExpired from participant gate", err)
	}
}
