// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package dds

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/podomy/weld/internal/core"
	"github.com/podomy/weld/internal/types"
)

// hookTestStringDesc registers a one-field String descriptor for hook tests.
//
// The descriptor carries a single string member named data, matching the
// XCDR1 String bytes from encodeTestString. Registration is idempotent
// across parallel runs sharing one namespace.
func hookTestStringDesc(t *testing.T) *types.MessageMembers {
	t.Helper()

	members := []types.MessageMember{
		types.NewStringMember("data", types.RosTypeString, 0, 0),
	}
	desc := types.NewMessageMembers("hook_test__msg", "Hooked", 16, members, nil, nil)

	err := types.RegisterMessageMembers(desc)
	if err != nil {
		existing, lerr := types.LookupMessageMembers("hook_test__msg", "Hooked")
		if lerr != nil {
			t.Fatalf("register hook descriptor: %v", err)
		}

		return existing
	}

	return desc
}

// TestTakeHooksFilterMissDropsWithReason checks a filter miss drops loudly.
func TestTakeHooksFilterMissDropsWithReason(t *testing.T) {
	t.Parallel()

	recv := NewDataReceiver()
	desc := hookTestStringDesc(t)

	filter, err := CompileContentFilter("data = 'keep-me'")
	if err != nil {
		t.Fatalf("compile filter: %v", err)
	}

	recv.SetContentFilter("/chatter", filter, desc)

	snap := recv.snapshotTakeHooks("/chatter", WriterGUID{})
	rej := evalTakeHooks(snap, "/chatter", "hook_test__msg__Hooked", encodeTestString(t, "drop-me"), WriterGUID{}, false)
	if rej == nil {
		t.Fatalf("filter miss passed, want rejection")
	}

	if rej.reason == nil || *rej.reason != DropReasonContentFilterMiss {
		t.Fatalf("miss reason = %v, want content-filter-miss", rej.reason)
	}

	if !errors.Is(rej.err, ErrContentFilterMiss) {
		t.Fatalf("miss error %v misses ErrContentFilterMiss", rej.err)
	}
}

// TestTakeHooksFilterPassDelivers checks a matching sample passes hooks.
func TestTakeHooksFilterPassDelivers(t *testing.T) {
	t.Parallel()

	recv := NewDataReceiver()
	desc := hookTestStringDesc(t)

	filter, err := CompileContentFilter("data = 'keep-me'")
	if err != nil {
		t.Fatalf("compile filter: %v", err)
	}

	recv.SetContentFilter("/chatter", filter, desc)

	snap := recv.snapshotTakeHooks("/chatter", WriterGUID{})
	if rej := evalTakeHooks(snap, "/chatter", "hook_test__msg__Hooked", encodeTestString(t, "keep-me"), WriterGUID{}, false); rej != nil {
		t.Fatalf("filter pass rejected: %v", rej.err)
	}
}

// TestTakeHooksNoFilterPasses checks the unwatched path is untouched.
func TestTakeHooksNoFilterPasses(t *testing.T) {
	t.Parallel()

	recv := NewDataReceiver()

	snap := recv.snapshotTakeHooks("/chatter", WriterGUID{})
	if rej := evalTakeHooks(snap, "/chatter", "std_msgs__msg__String", encodeTestString(t, "anything"), WriterGUID{}, false); rej != nil {
		t.Fatalf("unwatched sample rejected: %v", rej.err)
	}
}

// TestTakeHooksLivelinessExpiredDrops checks an expired writer drops loudly.
//
// Manual writers never ride free on data: the take path observes their
// true staleness instead of reviving them.
func TestTakeHooksLivelinessExpiredDrops(t *testing.T) {
	t.Parallel()

	recv := NewDataReceiver()

	var guid WriterGUID
	guid[0] = 0xE1

	lease := core.NewDuration(0, 1)
	tracker := core.NewLivelinessTracker("/chatter", "dead-writer", core.LivelinessManualByTopic, lease, time.Now().Add(-time.Hour).UnixNano())
	recv.SetLivelinessTracker(guid, &tracker)

	if got := recv.DropReasonCount(DropReasonLivelinessExpired); got != 0 {
		t.Fatalf("expiry drops before take = %d, want 0", got)
	}

	snap := recv.snapshotTakeHooks("/chatter", guid)
	rej := evalTakeHooks(snap, "/chatter", "std_msgs__msg__String", encodeTestString(t, "ancient"), guid, false)
	if rej == nil {
		t.Fatalf("expired writer passed, want drop")
	}

	if rej.reason == nil || *rej.reason != DropReasonLivelinessExpired {
		t.Fatalf("expiry reason = %v, want liveliness-expired", rej.reason)
	}

	if !errors.Is(rej.err, ErrLivelinessExpired) {
		t.Fatalf("expiry error %v misses ErrLivelinessExpired", rej.err)
	}
}

// TestTakeHooksLivelinessHoldUncounted checks a stale writer holds without counting.
func TestTakeHooksLivelinessHoldUncounted(t *testing.T) {
	t.Parallel()

	recv := NewDataReceiver()

	var guid WriterGUID
	guid[0] = 0xE2

	now := time.Now().UnixNano()
	lease := core.NewDuration(2, 0)
	tracker := core.NewLivelinessTracker("/chatter", "stale-writer", core.LivelinessManualByTopic, lease, now-10_000_000_000)
	if !tracker.AssertManualAt(now - 3_000_000_000) {
		t.Fatalf("manual assert failed")
	}
	recv.SetLivelinessTracker(guid, &tracker)

	snap := recv.snapshotTakeHooks("/chatter", guid)
	rej := evalTakeHooks(snap, "/chatter", "std_msgs__msg__String", encodeTestString(t, "stale"), guid, false)
	if rej == nil {
		t.Fatalf("stale writer passed, want hold")
	}

	if rej.reason != nil {
		t.Fatalf("hold carried reason %q, want uncounted hold", *rej.reason)
	}

	if !errors.Is(rej.err, ErrLivelinessHold) {
		t.Fatalf("hold error %v misses ErrLivelinessHold", rej.err)
	}

	if got := recv.DroppedCount(); got != 0 {
		t.Fatalf("hold counted %d drops, want 0", got)
	}
}

// TestTakeHooksAutomaticSampleRevives checks an automatic sample delivers.
//
// Receiving DATA from an automatic writer proves it wrote recently, so the
// sample revives the tracker and flows instead of dropping.
func TestTakeHooksAutomaticSampleRevives(t *testing.T) {
	t.Parallel()

	recv := NewDataReceiver()

	var guid WriterGUID
	guid[0] = 0xE3

	lease := core.NewDuration(0, 1)
	tracker := core.NewLivelinessTracker("/chatter", "auto-writer", core.LivelinessAutomatic, lease, time.Now().Add(-time.Hour).UnixNano())
	recv.SetLivelinessTracker(guid, &tracker)

	snap := recv.snapshotTakeHooks("/chatter", guid)
	if rej := evalTakeHooks(snap, "/chatter", "std_msgs__msg__String", encodeTestString(t, "fresh"), guid, false); rej != nil {
		t.Fatalf("automatic fresh sample rejected: %v", rej.err)
	}

	if got := recv.DroppedCount(); got != 0 {
		t.Fatalf("revived sample counted %d drops, want 0", got)
	}
}

// TestTakeHooksPlainNackLateJoinRequestsBaseOne checks first-sight gaps NACK.
func TestTakeHooksPlainNackLateJoinRequestsBaseOne(t *testing.T) {
	t.Parallel()

	recv := NewDataReceiver()

	var sent []uint64
	recv.SetNackSender(func(_ context.Context, _ WriterGUID, base uint64) error {
		sent = append(sent, base)
		return nil
	})

	var guid WriterGUID
	guid[0] = 0xF1

	ctx := context.Background()
	recv.notePlainSample(ctx, guid, 7)

	if len(sent) != 1 || sent[0] != 1 {
		t.Fatalf("late join NACKs = %v, want exactly [1]", sent)
	}

	recv.notePlainSample(ctx, guid, 8)
	recv.notePlainSample(ctx, guid, 9)

	for _, base := range sent {
		if base != 1 {
			t.Fatalf("steady NACK base = %d, want only initial 1", base)
		}
	}
}

// TestTakeHooksPlainNackGapAdvancesOnce checks later gaps NACK once per advance.
func TestTakeHooksPlainNackGapAdvancesOnce(t *testing.T) {
	t.Parallel()

	recv := NewDataReceiver()

	var sent []uint64
	recv.SetNackSender(func(_ context.Context, _ WriterGUID, base uint64) error {
		sent = append(sent, base)
		return nil
	})

	var guid WriterGUID
	guid[0] = 0xF2

	ctx := context.Background()
	recv.notePlainSample(ctx, guid, 1)
	recv.notePlainSample(ctx, guid, 2)
	recv.notePlainSample(ctx, guid, 5)

	if len(sent) != 1 || sent[0] != 3 {
		t.Fatalf("gap NACKs = %v, want exactly [3]", sent)
	}

	recv.notePlainSample(ctx, guid, 6)
	recv.notePlainSample(ctx, guid, 9)

	bases := append([]uint64(nil), sent...)
	if len(bases) != 2 || bases[0] != 3 || bases[1] != 7 {
		t.Fatalf("gap NACKs = %v, want [3 7]", bases)
	}
}

// TestTakeHooksPlainNackNoSenderSkips checks nil sender disables NACKs silently.
func TestTakeHooksPlainNackNoSenderSkips(t *testing.T) {
	t.Parallel()

	recv := NewDataReceiver()

	ctx := context.Background()

	var guid WriterGUID
	guid[0] = 0xF3

	recv.notePlainSample(ctx, guid, 42)

	if got := recv.DropReasonCount(DropReasonNackSendFailed); got != 0 {
		t.Fatalf("nack send failures = %d, want 0 with nil sender", got)
	}
}

// TestTakeHooksKeyedUnregisteredSkips checks unregistered types skip keyed handling.
func TestTakeHooksKeyedUnregisteredSkips(t *testing.T) {
	t.Parallel()

	recv := NewDataReceiver()

	var guid WriterGUID
	guid[15] = 0x02

	snap := recv.snapshotTakeHooks("/chatter", guid)
	if rej := evalTakeHooks(snap, "/chatter", "std_msgs__msg__String", encodeTestString(t, "plain"), guid, true); rej != nil {
		t.Fatalf("unregistered keyed sample rejected: %v", rej.err)
	}
}

// TestTakeHooksDeadlineMissedDrops checks past-deadline samples drop loudly.
func TestTakeHooksDeadlineMissedDrops(t *testing.T) {
	t.Parallel()

	recv := NewDataReceiver()

	deadline := core.NewDuration(0, 1)
	tracker := core.NewDeadlineTracker("/chatter", "reader", deadline, time.Now().Add(-time.Hour).UnixNano())
	recv.SetDeadlineTracker("/chatter", &tracker)

	snap := recv.snapshotTakeHooks("/chatter", WriterGUID{})
	rej := evalTakeHooks(snap, "/chatter", "std_msgs__msg__String", encodeTestString(t, "late"), WriterGUID{}, false)
	if rej == nil {
		t.Fatalf("past-deadline sample passed, want drop")
	}

	if rej.reason == nil || *rej.reason != DropReasonDeadlineMissed {
		t.Fatalf("deadline reason = %v, want deadline-missed", rej.reason)
	}

	if !errors.Is(rej.err, ErrDeadlineMissed) {
		t.Fatalf("deadline error %v misses ErrDeadlineMissed", rej.err)
	}
}
