// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

package dds

import (
	"errors"
	"testing"

	"github.com/podomy/weld/internal/types"
)

// frameWithSeq builds one valid DATA frame for guid at seq.
func frameWithSeq(t *testing.T, guid WriterGUID, seq uint64, topic, typeName string, payload []byte) []byte {
	t.Helper()

	ddsTopic, err := TopicName(topic, false)
	if err != nil {
		t.Fatalf("topic name: %v", err)
	}

	envelope := types.MarshalMessage(payload, true)
	frame, err := encodeFrame(guid, seq, ddsTopic, typeName, uint32(len(payload)), envelope) //nolint:gosec // G115: test payload is tiny
	if err != nil {
		t.Fatalf("encode frame: %v", err)
	}

	return frame
}

// TestBelowWindowDuplicateDropsWithoutUncountingLost checks the window floor.
//
// A duplicate below the delivered window was already forgotten and must
// drop as stale without re-delivering or decrementing lost, while a repair
// inside the window still delivers and un-counts one loss.
func TestBelowWindowDuplicateDropsWithoutUncountingLost(t *testing.T) {
	t.Parallel()

	recv := NewDataReceiver()
	guid := testWriterGUID()
	payload := encodeTestString(t, "w")

	recv.mu.Lock()
	if recv.last == nil {
		recv.last = make(map[WriterGUID]uint64)
	}
	if recv.delivered == nil {
		recv.delivered = make(map[WriterGUID]map[uint64]struct{})
	}
	recv.last[guid] = 10000
	recv.lost = 3
	recv.delivered[guid] = map[uint64]struct{}{9998: {}, 9999: {}, 10000: {}}
	recv.mu.Unlock()

	below := frameWithSeq(t, guid, 100, "/w", "std_msgs__msg__String", payload)
	if _, err := recv.Take(below); !errors.Is(err, ErrStaleSequence) {
		t.Fatalf("below-window take = %v, want ErrStaleSequence", err)
	}

	if got := recv.LostCount(); got != 3 {
		t.Fatalf("lost after below-window duplicate = %d, want 3", got)
	}

	within := frameWithSeq(t, guid, 9997, "/w", "std_msgs__msg__String", payload)
	if _, err := recv.Take(within); err != nil {
		t.Fatalf("within-window repair = %v, want deliver", err)
	}

	if got := recv.LostCount(); got != 2 {
		t.Fatalf("lost after within-window repair = %d, want 2", got)
	}
}
