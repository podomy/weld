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
	rej := evalTakeHooks(snap, "/chatter", "hook_test__msg__Hooked", encodeTestString(t, "drop-me"), WriterGUID{}, false, time.Now().UnixNano())
	if rej == nil {
		t.Fatalf("filter miss passed, want rejection")
	}

	if rej.reason == nil || *rej.reason != DropReasonContentFilterMiss {
		t.Fatalf("miss reason = %v, want content-filter-miss", rej.reason)
	}

	if !errors.Is(rej.err, ErrContentFilterMiss) {
		t.Fatalf("miss error %v misses ErrContentFilterMiss", rej.err)
	}

	missBefore := recv.DropReasonCount(DropReasonContentFilterMiss)
	dropsBefore := recv.DroppedCount()
	err = recv.countTakeHookReject(rej)
	if !errors.Is(err, ErrContentFilterMiss) {
		t.Fatalf("count miss = %v, want ErrContentFilterMiss", err)
	}
	if got := recv.DropReasonCount(DropReasonContentFilterMiss); got != missBefore+1 {
		t.Fatalf("miss count = %d, want %d", got, missBefore+1)
	}
	if got := recv.DroppedCount(); got != dropsBefore+1 {
		t.Fatalf("drops = %d, want %d", got, dropsBefore+1)
	}

	hold := &takeHookReject{err: ErrLivelinessHold}
	holdBefore := recv.DroppedCount()
	err = recv.countTakeHookReject(hold)
	if !errors.Is(err, ErrLivelinessHold) {
		t.Fatalf("count hold = %v, want ErrLivelinessHold", err)
	}
	if got := recv.DroppedCount(); got != holdBefore {
		t.Fatalf("hold counted drops = %d, want %d", got, holdBefore)
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
	if rej := evalTakeHooks(snap, "/chatter", "hook_test__msg__Hooked", encodeTestString(t, "keep-me"), WriterGUID{}, false, time.Now().UnixNano()); rej != nil {
		t.Fatalf("filter pass rejected: %v", rej.err)
	}
}

// TestTakeHooksNoFilterPasses checks the unwatched path is untouched.
func TestTakeHooksNoFilterPasses(t *testing.T) {
	t.Parallel()

	recv := NewDataReceiver()

	snap := recv.snapshotTakeHooks("/chatter", WriterGUID{})
	if rej := evalTakeHooks(snap, "/chatter", "std_msgs__msg__String", encodeTestString(t, "anything"), WriterGUID{}, false, time.Now().UnixNano()); rej != nil {
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
	rej := evalTakeHooks(snap, "/chatter", "std_msgs__msg__String", encodeTestString(t, "ancient"), guid, false, time.Now().UnixNano())
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
	rej := evalTakeHooks(snap, "/chatter", "std_msgs__msg__String", encodeTestString(t, "stale"), guid, false, time.Now().UnixNano())
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
	if rej := evalTakeHooks(snap, "/chatter", "std_msgs__msg__String", encodeTestString(t, "fresh"), guid, false, time.Now().UnixNano()); rej != nil {
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
	recv.ensureProvisional(guid, ProvisionalQoS{}, false)

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
	recv.ensureProvisional(guid, ProvisionalQoS{}, false)

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
	recv.ensureProvisional(guid, ProvisionalQoS{}, false)

	recv.notePlainSample(ctx, guid, 42)

	if got := recv.DropReasonCount(DropReasonNackSendFailed); got != 0 {
		t.Fatalf("nack send failures = %d, want 0 with nil sender", got)
	}
}

// TestTakeHooksKeyedUnregisteredDrops checks unregistered keyed types drop loudly fail-closed.
func TestTakeHooksKeyedUnregisteredDrops(t *testing.T) {
	t.Parallel()

	recv := NewDataReceiver()

	var guid WriterGUID
	guid[15] = 0x02

	snap := recv.snapshotTakeHooks("/chatter", guid)
	rej := evalTakeHooks(snap, "/chatter", "std_msgs__msg__String", encodeTestString(t, "plain"), guid, true, time.Now().UnixNano())
	if rej == nil {
		t.Fatalf("unregistered keyed sample passed, want loud drop")
	}

	if rej.reason == nil || *rej.reason != DropReasonKeyedDecodeFail {
		t.Fatalf("keyed unregistered reason = %v, want keyed-decode-fail", rej.reason)
	}

	if !errors.Is(rej.err, ErrKeyedDecodeFail) {
		t.Fatalf("keyed unregistered error %v misses ErrKeyedDecodeFail", rej.err)
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
	rej := evalTakeHooks(snap, "/chatter", "std_msgs__msg__String", encodeTestString(t, "late"), WriterGUID{}, false, time.Now().UnixNano())
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

// TestTakeHooksPlainNackGatesUnmatched pins matched gating for plain NACK.
//
// A sample from an unknown writer with no SEDP remote or provisional must
// not create plainNack state or send a NACK, consistent with isMatchedWriter.
func TestTakeHooksPlainNackGatesUnmatched(t *testing.T) {
	t.Parallel()

	recv := NewDataReceiver()

	var sent []uint64
	recv.SetNackSender(func(_ context.Context, _ WriterGUID, base uint64) error {
		sent = append(sent, base)
		return nil
	})

	var guid WriterGUID
	guid[0] = 0xF9

	ctx := context.Background()
	recv.notePlainSample(ctx, guid, 7)

	if len(sent) != 0 {
		t.Fatalf("unmatched NACKs = %v, want none", sent)
	}

	recv.mu.RLock()
	_, seen := recv.plainNack[guid]
	recv.mu.RUnlock()

	if seen {
		t.Fatalf("unmatched plainNack stored, want gate skip")
	}
}

// TestTakeHooksPlainNackFailedSendKeepsLastNacked pins no advance on failure.
//
// A failed NACK send must not advance lastNacked: the high-water moves only
// after a successful send so a later gap still repairs from the old base.
func TestTakeHooksPlainNackFailedSendKeepsLastNacked(t *testing.T) {
	t.Parallel()

	recv := NewDataReceiver()

	var guid WriterGUID
	guid[0] = 0xFA
	recv.ensureProvisional(guid, ProvisionalQoS{}, false)

	fail := errors.New("nack send fail")
	recv.SetNackSender(func(_ context.Context, _ WriterGUID, _ uint64) error {
		return fail //nolint:wrapcheck // test sender returns sentinel directly
	})

	ctx := context.Background()
	recv.notePlainSample(ctx, guid, 5)

	if got := recv.DropReasonCount(DropReasonNackSendFailed); got != 1 {
		t.Fatalf("nack failures = %d, want 1", got)
	}

	recv.mu.RLock()
	st := recv.plainNack[guid]
	recv.mu.RUnlock()

	if st == nil {
		t.Fatalf("plainNack missing after failed send")
	}

	if st.lastNacked != 0 {
		t.Fatalf("lastNacked = %d, want 0 after failed send", st.lastNacked)
	}
}

// TestTakeHooksLazyGateSkipsUnmatched pins lazy tracker gating.
//
// Snapshot hooks for an unknown writer with a liveliness contract but no
// matched remote must not install a tracker.
func TestTakeHooksLazyGateSkipsUnmatched(t *testing.T) {
	t.Parallel()

	recv := NewDataReceiver()
	recv.SetLivelinessTrackerForTopic("/gated", core.LivelinessAutomatic, core.NewDuration(10, 0), nil)

	var guid WriterGUID
	guid[0] = 0xFB

	snap := recv.snapshotTakeHooks("/gated", guid)
	if snap.tracker != nil {
		t.Fatalf("unmatched tracker installed, want gate skip")
	}

	recv.mu.RLock()
	_, seen := recv.lively[guid]
	recv.mu.RUnlock()

	if seen {
		t.Fatalf("unmatched lively stored, want gate skip")
	}
}

// TestTakeHooksLivelyCapEvicts pins bounded lazy tracker creation.
func TestTakeHooksLivelyCapEvicts(t *testing.T) {
	t.Parallel()

	recv := NewDataReceiver()
	recv.SetLivelinessTrackerForTopic("/capped", core.LivelinessAutomatic, core.NewDuration(10, 0), nil)

	for i := range livelyTrackerCap + 5 {
		var guid WriterGUID
		guid[0] = byte(i)
		guid[1] = byte(i >> 8)
		guid[2] = 0xC1
		recv.ensureProvisional(guid, ProvisionalQoS{}, false)
		snap := recv.snapshotTakeHooks("/capped", guid)
		if snap.tracker == nil {
			t.Fatalf("matched tracker missing at %d", i)
		}
	}

	recv.mu.RLock()
	n := len(recv.lively)
	recv.mu.RUnlock()

	if n > livelyTrackerCap {
		t.Fatalf("lively len = %d, want at most %d", n, livelyTrackerCap)
	}

	lastIdx := livelyTrackerCap + 4
	var last WriterGUID
	last[0] = byte(lastIdx & 0xFF)
	last[1] = byte((lastIdx >> 8) & 0xFF)
	last[2] = 0xC1
	if snap := recv.snapshotTakeHooks("/capped", last); snap.tracker == nil {
		t.Fatalf("newest tracker missing, want cap to evict old entries instead of dropping new ones")
	}
}

// TestSetLivelinessTrackerForTopicCapEvicts pins explicit cap enforcement.
//
// Bulk installs through the explicit writer list must hold the same cap as
// the lazy path with eviction instead of unbounded growth.
func TestSetLivelinessTrackerForTopicCapEvicts(t *testing.T) {
	t.Parallel()

	recv := NewDataReceiver()

	writers := make([]WriterGUID, 0, livelyTrackerCap+5)
	for i := range livelyTrackerCap + 5 {
		var guid WriterGUID
		guid[0] = byte(i)
		guid[1] = byte(i >> 8)
		guid[2] = 0xD1
		writers = append(writers, guid)
	}

	recv.SetLivelinessTrackerForTopic("/explicit", core.LivelinessAutomatic, core.NewDuration(10, 0), writers)

	recv.mu.RLock()
	n := len(recv.lively)
	recv.mu.RUnlock()

	if n > livelyTrackerCap {
		t.Fatalf("explicit lively len = %d, want at most %d", n, livelyTrackerCap)
	}

	lastIdx := livelyTrackerCap + 4
	var last WriterGUID
	last[0] = byte(lastIdx & 0xFF)
	last[1] = byte((lastIdx >> 8) & 0xFF)
	last[2] = 0xD1
	if snap := recv.snapshotTakeHooks("/explicit", last); snap.tracker == nil {
		t.Fatalf("newest explicit tracker missing, want cap to evict old entries instead of dropping new ones")
	}
}

// TestTakeHooksEvalUsesReceiverClock pins fake-clock evaluation.
//
// The hook chain must timestamp with the receiver clock, not the wall
// clock, so frozen-clock deployments expire leases by their own time.
// A deadline anchored in the far future misses only when the fake now
// advances past it; wall-clock evaluation would report no miss.
func TestTakeHooksEvalUsesReceiverClock(t *testing.T) {
	t.Parallel()

	recv := NewDataReceiver()
	frozen := time.Date(2099, time.January, 2, 3, 4, 5, 0, time.UTC)
	recv.now = func() time.Time {
		return frozen
	}
	recv.SetDeadlineTrackerForTopic("/clocked", core.NewDuration(10, 0))

	snap := recv.snapshotTakeHooks("/clocked", WriterGUID{})
	if snap.deadline == nil {
		t.Fatalf("deadline tracker missing")
	}

	fakeLate := frozen.Add(time.Hour).UnixNano()
	if rej := evalTakeHooks(snap, "/clocked", "std_msgs__msg__String", encodeTestString(t, "late"), WriterGUID{}, false, fakeLate); rej == nil {
		t.Fatalf("late sample passed under fake clock, want deadline miss")
	} else if rej.reason == nil || *rej.reason != DropReasonDeadlineMissed {
		t.Fatalf("late reason = %v, want deadline-missed", rej.reason)
	}
}
