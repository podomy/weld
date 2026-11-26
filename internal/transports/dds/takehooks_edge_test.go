// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

package dds

import (
	"errors"
	"testing"
	"time"

	"github.com/podomy/weld/internal/core"
)

// TestTakeHooksFilteredSampleSkipsReceipt pins that discarded samples prove nothing.
//
// A filter miss must return the filter reason without asserting automatic
// liveliness or anchoring the deadline, so filtered traffic from an
// expired writer stays expired instead of reviving it.
func TestTakeHooksFilteredSampleSkipsReceipt(t *testing.T) {
	t.Parallel()

	recv := NewDataReceiver()
	desc := hookTestStringDesc(t)

	filter, err := CompileContentFilter("data = 'keep-me'")
	if err != nil {
		t.Fatalf("compile filter: %v", err)
	}

	recv.SetContentFilter("/chatter", filter, desc)

	var guid WriterGUID
	guid[0] = 0xE9

	now := time.Now()
	lease := core.NewDuration(0, 1)
	tracker := core.NewLivelinessTracker("/chatter", "auto-writer", core.LivelinessAutomatic, lease, now.Add(-time.Hour).UnixNano())
	recv.SetLivelinessTracker(guid, &tracker)

	deadline := core.NewDuration(0, 1)
	dtracker := core.NewDeadlineTracker("/chatter", "reader", deadline, now.Add(-time.Hour).UnixNano())
	recv.SetDeadlineTracker("/chatter", &dtracker)

	snap := recv.snapshotTakeHooks("/chatter", guid)
	at := now.UnixNano()

	rej := evalTakeHooks(snap, "/chatter", "hook_test__msg__Hooked", encodeTestString(t, "drop-me"), guid, false, at)
	if rej == nil {
		t.Fatalf("filter miss passed, want rejection")
	}

	if rej.reason == nil || *rej.reason != DropReasonContentFilterMiss {
		t.Fatalf("miss reason = %v, want content-filter-miss", rej.reason)
	}

	if !errors.Is(rej.err, ErrContentFilterMiss) {
		t.Fatalf("miss error %v misses ErrContentFilterMiss", rej.err)
	}

	if got := snap.tracker.StateAt(at); got != core.LivelinessExpired {
		t.Fatalf("tracker state after filtered sample = %v, want expired", got)
	}

	if !snap.deadline.MissedAt(at) {
		t.Fatalf("deadline missed after filtered sample = false, want true")
	}
}
