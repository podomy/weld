// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package core

import (
	"strings"
	"testing"
)

// TestLiveliness pins the per-writer tracker plus pure gating.
func TestLiveliness(t *testing.T) {
	t.Run("state matrix", checkLivelinessStateMatrix)
	t.Run("lease boundaries", checkLivelinessLeaseBoundaries)
	t.Run("manual versus automatic", checkLivelinessManualAutomatic)
	t.Run("flapping suppression", checkLivelinessFlapping)
	t.Run("gating outcomes", checkLivelinessGating)
	t.Run("clock injection", checkLivelinessClockInjection)
	t.Run("matcher carry-through", checkLivelinessCarryThrough)
	t.Run("paired lease fold", checkLivelinessPairedFold)
}

// TestDeadlineEnforcementPeek pins the added pure peek APIs.
func TestDeadlineEnforcementPeek(t *testing.T) {
	t.Run("exceeded table", checkDeadlineExceededTable)
	t.Run("missed never latches", checkDeadlineMissedNoLatch)
	t.Run("elapsed peek", checkDeadlineElapsedPeek)
}

// checkLivelinessStateMatrix pins alive, stale, expired with hysteresis.
func checkLivelinessStateMatrix(t *testing.T) {
	t.Helper()
	lease := NewDuration(10, 0)
	tracker := NewLivelinessTracker("/scan", "talker", LivelinessAutomatic, lease, 0)
	if got := tracker.StateAt(0); got != LivelinessAlive {
		t.Fatalf("start state = %v, want alive", got)
	}
	if got := tracker.StateAt(5_000_000_000); got != LivelinessAlive {
		t.Fatalf("inside lease = %v, want alive", got)
	}
	if got := tracker.StateAt(10_000_000_000); got != LivelinessStale {
		t.Fatalf("at lease = %v, want stale", got)
	}
	if got := tracker.StateAt(15_000_000_000); got != LivelinessStale {
		t.Fatalf("inside hysteresis = %v, want stale", got)
	}
	if got := tracker.StateAt(20_000_000_000); got != LivelinessExpired {
		t.Fatalf("at double lease = %v, want expired", got)
	}
	if got := tracker.StateAt(100_000_000_000); got != LivelinessExpired {
		t.Fatalf("past double lease = %v, want expired", got)
	}
	if got := tracker.Generation(); got != 2 {
		t.Fatalf("generation = %d, want 2 (stale plus expired)", got)
	}
}

// checkLivelinessLeaseBoundaries pins exact second edges plus free leases.
func checkLivelinessLeaseBoundaries(t *testing.T) {
	t.Helper()
	lease := NewDuration(1, 0)
	tracker := NewLivelinessTracker("/t", "w", LivelinessAutomatic, lease, 0)
	if got := tracker.StateAt(999_999_999); got != LivelinessAlive {
		t.Fatalf("one ns under lease = %v, want alive", got)
	}
	if got := tracker.StateAt(1_000_000_000); got != LivelinessStale {
		t.Fatalf("exact lease = %v, want stale", got)
	}
	if got := tracker.StateAt(1_999_999_999); got != LivelinessStale {
		t.Fatalf("one ns under double = %v, want stale", got)
	}
	if got := tracker.StateAt(2_000_000_000); got != LivelinessExpired {
		t.Fatalf("exact double = %v, want expired", got)
	}
	for _, tc := range []struct {
		name  string
		lease Duration
	}{
		{"unspecified", UnspecifiedDuration()},
		{"infinite", InfiniteDuration()},
		{"sentinel", BestAvailableDuration()},
		{"negative", Duration{Seconds: -1}},
	} {
		free := NewLivelinessTracker("/t", "w", LivelinessAutomatic, tc.lease, 0)
		if got := free.StateAt(1_000_000_000_000); got != LivelinessAlive {
			t.Fatalf("%s lease must never expire, got %v", tc.name, got)
		}
		if got := free.Generation(); got != 0 {
			t.Fatalf("%s lease must never bump, got %d", tc.name, got)
		}
	}
}

// checkLivelinessManualAutomatic pins assert paths per kind.
func checkLivelinessManualAutomatic(t *testing.T) {
	t.Helper()
	lease := NewDuration(10, 0)
	auto := NewLivelinessTracker("/t", "w", LivelinessAutomatic, lease, 0)
	if !auto.RecordSampleAt(1_000_000_000) {
		t.Fatal("automatic must assert on sample")
	}
	if auto.AssertManualAt(2_000_000_000) {
		t.Fatal("automatic must reject manual assert")
	}
	if got := auto.StateAt(11_000_000_000); got != LivelinessStale {
		t.Fatalf("automatic last assert 1s plus 10s lease at 11s = %v, want stale", got)
	}
	manual := NewLivelinessTracker("/t", "w", LivelinessManualByTopic, lease, 0)
	if manual.RecordSampleAt(1_000_000_000) {
		t.Fatal("manual must not assert on sample")
	}
	if !manual.AssertManualAt(1_000_000_000) {
		t.Fatal("manual must assert manually")
	}
	if got := manual.StateAt(11_000_000_000); got != LivelinessStale {
		t.Fatalf("manual last assert 1s plus 10s lease at 11s = %v, want stale", got)
	}
	if !manual.AssertManualAt(12_000_000_000) {
		t.Fatal("manual recovery assert must succeed")
	}
	if got := manual.StateAt(12_000_000_000); got != LivelinessAlive {
		t.Fatalf("manual after assert = %v, want alive", got)
	}
	unresolved := NewLivelinessTracker("/t", "w", LivelinessSystemDefault, lease, 0)
	if unresolved.RecordSampleAt(1_000_000_000) {
		t.Fatal("unresolved kind must not assert on sample")
	}
	if unresolved.AssertManualAt(1_000_000_000) {
		t.Fatal("unresolved kind must not assert manually")
	}
}

// checkLivelinessFlapping pins one bump per edge plus quiet rebase.
//
//nolint:gocognit,cyclop // test pins stale, expiry, recovery, and rebase in one sequence, splitting would obscure intent
func checkLivelinessFlapping(t *testing.T) {
	t.Helper()
	lease := NewDuration(10, 0)
	tracker := NewLivelinessTracker("/t", "w", LivelinessAutomatic, lease, 0)
	if got := tracker.StateAt(15_000_000_000); got != LivelinessStale {
		t.Fatalf("hysteresis = %v, want stale", got)
	}
	if got := tracker.Generation(); got != 1 {
		t.Fatalf("stale generation = %d, want 1", got)
	}
	if got := tracker.StateAt(20_000_000_000); got != LivelinessExpired {
		t.Fatalf("expiry = %v, want expired", got)
	}
	base := tracker.Generation()
	if base != 2 {
		t.Fatalf("expiry generation = %d, want 2", base)
	}
	for _, at := range []int64{20_000_000_001, 25_000_000_000, 1_000_000_000_000} {
		if got := tracker.StateAt(at); got != LivelinessExpired {
			t.Fatalf("repeat at %d = %v, want expired", at, got)
		}
		if got := tracker.Generation(); got != base {
			t.Fatalf("repeat at %d bumped %d want %d", at, got, base)
		}
	}
	if !tracker.RecordSampleAt(30_000_000_000) {
		t.Fatal("recovery sample must assert")
	}
	if got := tracker.Generation(); got != base+1 {
		t.Fatalf("recovery generation = %d, want %d", got, base+1)
	}
	afterRecover := tracker.Generation()
	if !tracker.RecordSampleAt(31_000_000_000) {
		t.Fatal("sample while alive must assert")
	}
	if got := tracker.Generation(); got != afterRecover {
		t.Fatalf("sample while alive must not bump, got %d want %d", got, afterRecover)
	}
	beforeBack := tracker.Generation()
	if got := tracker.StateAt(1_000_000_000); got != LivelinessAlive {
		t.Fatalf("backward rebase = %v, want alive", got)
	}
	if got := tracker.Generation(); got != beforeBack {
		t.Fatalf("backward rebase must not bump, got %d want %d", got, beforeBack)
	}
	if got := tracker.LastAssertNanos(); got != 1_000_000_000 {
		t.Fatalf("rebase last assert = %d, want 1000000000", got)
	}
}

// checkLivelinessGating pins deliver, hold, drop with named policies.
//
//nolint:gocognit,cyclop // test pins deliver, hold, drop, and event breadth in one run, splitting would obscure intent
func checkLivelinessGating(t *testing.T) {
	t.Helper()
	deliver := GateDelivery(LivelinessAlive, false)
	if deliver.Decision != GateDeliver {
		t.Fatalf("alive fresh = %v, want deliver", deliver.Decision)
	}
	if deliver.Policy != "" || deliver.Reason != "" {
		t.Fatalf("deliver must carry no policy or reason, got %+v", deliver)
	}
	missed := GateDelivery(LivelinessAlive, true)
	if missed.Decision != GateDrop {
		t.Fatalf("alive missed = %v, want drop", missed.Decision)
	}
	if missed.Policy != PolicyDeadline {
		t.Fatalf("deadline miss policy = %q, want deadline", missed.Policy)
	}
	if !strings.Contains(missed.Reason, "deadline") {
		t.Fatalf("deadline miss reason must name deadline, got %q", missed.Reason)
	}
	hold := GateDelivery(LivelinessStale, false)
	if hold.Decision != GateHold {
		t.Fatalf("stale = %v, want hold", hold.Decision)
	}
	if hold.Policy != PolicyLeaseDuration {
		t.Fatalf("stale policy = %q, want lease_duration", hold.Policy)
	}
	if !strings.Contains(hold.Reason, "stale") && !strings.Contains(hold.Reason, "hold") {
		t.Fatalf("stale reason must say hold or stale, got %q", hold.Reason)
	}
	staleMissed := GateDelivery(LivelinessStale, true)
	if staleMissed.Decision != GateHold {
		t.Fatalf("stale plus deadline = %v, want hold (stale dominates)", staleMissed.Decision)
	}
	dropped := GateDelivery(LivelinessExpired, false)
	if dropped.Decision != GateDrop {
		t.Fatalf("expired = %v, want drop", dropped.Decision)
	}
	if dropped.Policy != PolicyLeaseDuration {
		t.Fatalf("expired policy = %q, want lease_duration", dropped.Policy)
	}
	if !strings.Contains(dropped.Reason, "stale") {
		t.Fatalf("expired reason must say stale, got %q", dropped.Reason)
	}
	both := GateDelivery(LivelinessExpired, true)
	if both.Decision != GateDrop || !strings.Contains(both.Reason, "stale") {
		t.Fatalf("expired plus deadline must drop stale, got %+v", both)
	}
	event, ok := NewGateMismatchEvent("/scan", "talker", "listener", dropped, "4s", "4s")
	if !ok {
		t.Fatal("drop must yield a loud event")
	}
	if event.Policy != PolicyLeaseDuration {
		t.Fatalf("gate event policy = %q, want lease_duration", event.Policy)
	}
	if _, ok := NewGateMismatchEvent("/scan", "talker", "listener", deliver, "", ""); ok {
		t.Fatal("deliver must yield no event")
	}
}

// checkLivelinessClockInjection pins clock func determinism.
func checkLivelinessClockInjection(t *testing.T) {
	t.Helper()
	lease := NewDuration(10, 0)
	tracker := NewLivelinessTracker("/t", "w", LivelinessAutomatic, lease, 0)
	now := int64(0)
	tracker.SetClock(func() int64 { return now })
	now = 5_000_000_000
	if got := tracker.StateNow(); got != LivelinessAlive {
		t.Fatalf("clock 5s = %v, want alive", got)
	}
	if !tracker.RecordSample() {
		t.Fatal("clock sample must assert for automatic")
	}
	if got := tracker.LastAssertNanos(); got != 5_000_000_000 {
		t.Fatalf("clock sample last = %d, want 5000000000", got)
	}
	now = 25_000_000_000
	if got := tracker.StateNow(); got != LivelinessExpired {
		t.Fatalf("clock 25s with last 5s = %v, want expired", got)
	}
	direct := NewLivelinessTracker("/t", "w", LivelinessAutomatic, lease, 5_000_000_000)
	if got := direct.StateAt(25_000_000_000); got != tracker.LatchedState() {
		t.Fatalf("clock path %v must match direct %v", tracker.LatchedState(), got)
	}
	plain := NewLivelinessTracker("/t", "w", LivelinessAutomatic, lease, 0)
	if got := plain.StateNow(); got != LivelinessAlive {
		t.Fatalf("nil clock must return latched alive, got %v", got)
	}
	if plain.RecordSample() {
		t.Fatal("nil clock sample must report false")
	}
	manual := NewLivelinessTracker("/t", "w", LivelinessManualByTopic, lease, 0)
	manualNow := int64(7_000_000_000)
	manual.SetClock(func() int64 { return manualNow })
	if !manual.AssertManual() {
		t.Fatal("clock manual assert must succeed")
	}
	if got := manual.LastAssertNanos(); got != 7_000_000_000 {
		t.Fatalf("clock manual last = %d, want 7000000000", got)
	}
	manual.SetClock(nil)
	if manual.AssertManual() {
		t.Fatal("nil clock manual must report false")
	}
}

// checkLivelinessCarryThrough pins RxO input exposure plus folds.
func checkLivelinessCarryThrough(t *testing.T) {
	t.Helper()
	offered := ResolveSystemDefaults(DefaultQoS())
	requested := offered
	requested.Deadline = NewDuration(0, 20000000)
	requested.Liveliness = LivelinessManualByTopic
	requested.LeaseDuration = NewDuration(4, 0)
	gotOfferedD, gotRequestedD := DeadlineRxOInputs(offered, requested)
	if gotOfferedD != offered.Deadline || gotRequestedD != requested.Deadline {
		t.Fatalf("deadline inputs = %v %v, want verbatim", gotOfferedD, gotRequestedD)
	}
	gotOfferedL, gotRequestedL := LivelinessRxOInputs(offered, requested)
	if gotOfferedL != offered.Liveliness || gotRequestedL != requested.Liveliness {
		t.Fatalf("liveliness inputs = %v %v, want verbatim", gotOfferedL, gotRequestedL)
	}
	gotOfferedLease, gotRequestedLease := LeaseRxOInputs(offered, requested)
	if gotOfferedLease != offered.LeaseDuration || gotRequestedLease != requested.LeaseDuration {
		t.Fatalf("lease inputs = %v %v, want verbatim", gotOfferedLease, gotRequestedLease)
	}
	if got := EffectiveDeadlineDuration(offered.Deadline, requested.Deadline); got != requested.Deadline {
		t.Fatalf("effective deadline = %v, want request %v", got, requested.Deadline)
	}
	if got := EffectiveDeadlineDuration(requested.Deadline, UnspecifiedDuration()); got != UnspecifiedDuration() {
		t.Fatalf("unconstrained request must yield unspecified, got %v", got)
	}
	if got := EffectiveDeadlineDuration(requested.Deadline, InfiniteDuration()); got != UnspecifiedDuration() {
		t.Fatalf("infinite request must yield unspecified, got %v", got)
	}
}

// checkLivelinessPairedFold pins offered versus requested lease folding.
func checkLivelinessPairedFold(t *testing.T) {
	t.Helper()
	finite4 := NewDuration(4, 0)
	finite9 := NewDuration(9, 0)
	if got := EffectiveLeaseDuration(finite4, finite9); got != finite4 {
		t.Fatalf("two finite must fold to min, got %v", got)
	}
	if got := EffectiveLeaseDuration(finite9, finite4); got != finite4 {
		t.Fatalf("two finite reversed must fold to min, got %v", got)
	}
	if got := EffectiveLeaseDuration(UnspecifiedDuration(), finite4); got != finite4 {
		t.Fatalf("default offered must yield request, got %v", got)
	}
	if got := EffectiveLeaseDuration(finite4, InfiniteDuration()); got != finite4 {
		t.Fatalf("infinite request must yield offer, got %v", got)
	}
	if got := EffectiveLeaseDuration(UnspecifiedDuration(), UnspecifiedDuration()); got != UnspecifiedDuration() {
		t.Fatalf("both default must yield unspecified, got %v", got)
	}
	if got := EffectiveLeaseDuration(BestAvailableDuration(), finite4); got != finite4 {
		t.Fatalf("sentinel offered must yield request, got %v", got)
	}
	if got := EffectiveLeaseDuration(Duration{Seconds: -1}, finite4); got != finite4 {
		t.Fatalf("negative offered must yield request, got %v", got)
	}
	paired := NewPairedLivelinessTracker("/t", "w", LivelinessAutomatic, finite9, finite4, 0)
	if paired.Lease() != finite4 {
		t.Fatalf("paired lease = %v, want 4s", paired.Lease())
	}
	if got := paired.StateAt(4_000_000_000); got != LivelinessStale {
		t.Fatalf("paired at min lease = %v, want stale", got)
	}
}

// checkDeadlineExceededTable pins the pure deadline exceed helper.
func checkDeadlineExceededTable(t *testing.T) {
	t.Helper()
	deadline := NewDuration(0, 100000000)
	if DeadlineExceeded(deadline, 99999999) {
		t.Fatal("one ns under deadline must not exceed")
	}
	if !DeadlineExceeded(deadline, 100000000) {
		t.Fatal("exact deadline must exceed")
	}
	for _, tc := range []struct {
		name string
		d    Duration
	}{
		{"unspecified", UnspecifiedDuration()},
		{"infinite", InfiniteDuration()},
		{"sentinel", BestAvailableDuration()},
		{"negative", Duration{Seconds: -1}},
	} {
		if DeadlineExceeded(tc.d, 1_000_000_000_000) {
			t.Fatalf("%s must never exceed", tc.name)
		}
	}
}

// checkDeadlineMissedNoLatch pins MissedAt never consumes the Check alert.
func checkDeadlineMissedNoLatch(t *testing.T) {
	t.Helper()
	tracker := NewDeadlineTracker("/scan", "listener", NewDuration(0, 100000000), 0)
	if tracker.MissedAt(50000000) {
		t.Fatal("fresh window must not miss")
	}
	if !tracker.MissedAt(100000000) {
		t.Fatal("exact deadline must miss")
	}
	if !tracker.MissedAt(150000000) {
		t.Fatal("repeat peek must still miss (no latch)")
	}
	if _, ok := tracker.Check(150000000); !ok {
		t.Fatal("peek must not consume the Check alert")
	}
	if _, ok := tracker.Check(160000000); ok {
		t.Fatal("repeat Check must stay quiet in one outage")
	}
	if tracker.MissedAt(50000000) {
		t.Fatal("backward time must never miss")
	}
}

// checkDeadlineElapsedPeek pins ElapsedAt never mutates the window.
func checkDeadlineElapsedPeek(t *testing.T) {
	t.Helper()
	tracker := NewDeadlineTracker("/scan", "listener", NewDuration(0, 100000000), 1000000000)
	if got := tracker.ElapsedAt(1050000000); got != 50000000 {
		t.Fatalf("elapsed = %d, want 50000000", got)
	}
	if got := tracker.ElapsedAt(1050000000); got != 50000000 {
		t.Fatalf("repeat elapsed must stay stable, got %d", got)
	}
	if _, ok := tracker.Check(1050000000); ok {
		t.Fatal("half window must not alert after peeks")
	}
	if got := tracker.ElapsedAt(500000000); got != -500000000 {
		t.Fatalf("backward elapsed = %d, want negative", got)
	}
	if _, ok := tracker.Check(500000000); ok {
		t.Fatal("backward peek must not alert")
	}
}
