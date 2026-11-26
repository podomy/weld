// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

package dds

import (
	"errors"
	"testing"

	"github.com/podomy/weld/internal/transports"
)

// TestEvictWriterClearsDeliveredForGuidReuse checks GUID reuse restarts clean.
//
// EvictWriter must drop the delivered set alongside sequence tracking so a
// later writer reusing the GUID accepts old sequences as fresh instead of
// reporting stale duplicates.
func TestEvictWriterClearsDeliveredForGuidReuse(t *testing.T) {
	t.Parallel()

	recv := NewDataReceiver()
	guid := testWriterGUID()
	sink := &recordingWriter{}
	pub := mustNewPublisher(t, guid, sink)
	mustPublish(t, pub, transports.OutgoingMessage{TopicName: "/evict", TypeName: "std_msgs__msg__String", Payload: encodeTestString(t, "a")})
	mustPublish(t, pub, transports.OutgoingMessage{TopicName: "/evict", TypeName: "std_msgs__msg__String", Payload: encodeTestString(t, "b")})

	first := sink.frameAt(t, 0)
	second := sink.frameAt(t, 1)
	mustTake(t, recv, first)
	mustTake(t, recv, second)

	if _, err := recv.Take(first); !errors.Is(err, ErrStaleSequence) {
		t.Fatalf("duplicate before evict = %v, want ErrStaleSequence", err)
	}

	recv.EvictWriter(guid)

	if _, err := recv.Take(first); err != nil {
		t.Fatalf("reused guid take = %v, want accept after evict", err)
	}

	if got := recv.LostCount(); got != 0 {
		t.Fatalf("lost = %d, want 0 with no gaps", got)
	}

	if _, err := recv.Take(second); err != nil {
		t.Fatalf("second take after reuse = %v, want accept", err)
	}
}
