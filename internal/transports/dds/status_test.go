// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

package dds

import (
	"testing"
	"time"

	"github.com/podomy/weld/internal/core"
)

// TestOfferedDeadlineMissedCounts checks writer-side miss accounting.
func TestOfferedDeadlineMissedCounts(t *testing.T) {
	t.Parallel()

	recv := NewDataReceiver()
	var handle [16]byte
	handle[0] = 0x0A

	recv.NoteOfferedDeadlineMissed(handle)
	recv.NoteOfferedDeadlineMissed(handle)

	first := recv.OfferedDeadlineMissedStatus()
	if first.TotalCount != 2 || first.TotalCountChange != 2 {
		t.Fatalf("first status = %+v, want total 2 change 2", first)
	}

	if first.LastInstanceHandle != handle {
		t.Fatalf("last handle mismatch")
	}

	second := recv.OfferedDeadlineMissedStatus()
	if second.TotalCount != 2 || second.TotalCountChange != 0 {
		t.Fatalf("second status = %+v, want total 2 change 0", second)
	}
}

// TestCheckRequestedDeadlineCountsMisses checks tracker-driven accounting.
func TestCheckRequestedDeadlineCountsMisses(t *testing.T) {
	t.Parallel()

	recv := NewDataReceiver()
	now := time.Unix(1000, 0).UTC()
	recv.now = func() time.Time { return now }
	if err := recv.SetDeadlineTrackerForTopic("/d", core.NewDuration(1, 0)); err != nil {
		t.Fatalf("set deadline: %v", err)
	}

	later := now.Add(2 * time.Second).UnixNano()
	if _, ok := recv.CheckRequestedDeadline("/d", later); !ok {
		t.Fatalf("first check missed the lapsed deadline")
	}

	if _, ok := recv.CheckRequestedDeadline("/d", later); ok {
		t.Fatalf("second check re-alerted one outage")
	}

	status := recv.RequestedDeadlineMissedStatus()
	if status.TotalCount != 1 || status.TotalCountChange != 1 {
		t.Fatalf("status = %+v, want total 1 change 1", status)
	}

	if _, ok := recv.CheckRequestedDeadline("/missing", later); ok {
		t.Fatalf("check alerted without a tracker")
	}
}

// TestLivelinessChangedCountsEdges checks reader-side alive accounting.
func TestLivelinessChangedCountsEdges(t *testing.T) {
	t.Parallel()

	recv := NewDataReceiver()
	var first, second [16]byte
	first[0] = 0x01
	second[0] = 0x02

	recv.NoteWriterAlive(first, true)
	recv.NoteWriterAlive(first, true)
	recv.NoteWriterAlive(second, false)

	status := recv.LivelinessChangedStatus()
	if status.AliveCount != 1 || status.NotAliveCount != 1 {
		t.Fatalf("counts = %+v, want alive 1 not-alive 1", status)
	}

	if status.AliveCountChange != 1 || status.NotAliveCountChange != 1 {
		t.Fatalf("changes = %+v, want 1 and 1", status)
	}

	if status.LastPublicationHandle != second {
		t.Fatalf("last handle mismatch")
	}

	again := recv.LivelinessChangedStatus()
	if again.AliveCountChange != 0 || again.NotAliveCountChange != 0 {
		t.Fatalf("changes = %+v, want reset to 0", again)
	}

	recv.NoteWriterAlive(first, false)
	flipped := recv.LivelinessChangedStatus()
	if flipped.AliveCount != 0 || flipped.NotAliveCount != 2 {
		t.Fatalf("flipped counts = %+v, want alive 0 not-alive 2", flipped)
	}

	recv.ForgetWriter(first)
	dropped := recv.LivelinessChangedStatus()
	if dropped.NotAliveCount != 1 {
		t.Fatalf("after forget not-alive = %d, want 1", dropped.NotAliveCount)
	}
}

// TestLivelinessLostCounts checks writer-side loss accounting.
func TestLivelinessLostCounts(t *testing.T) {
	t.Parallel()

	recv := NewDataReceiver()
	recv.NoteLivelinessLost()
	recv.NoteLivelinessLost()

	first := recv.LivelinessLostStatus()
	if first.TotalCount != 2 || first.TotalCountChange != 2 {
		t.Fatalf("first status = %+v, want total 2 change 2", first)
	}

	second := recv.LivelinessLostStatus()
	if second.TotalCount != 2 || second.TotalCountChange != 0 {
		t.Fatalf("second status = %+v, want total 2 change 0", second)
	}
}
