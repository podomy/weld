// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package core

import (
	"strings"
	"sync"
	"testing"
)

// TestClock covers one domain per context, tick agreement, /clock
// publishing, sim agreement, jump callbacks, and deadline rebase.
func TestClock(t *testing.T) {
	t.Run("clock QoS", checkClockQoS)
	t.Run("sim second tick", checkSimSecondTick)
	t.Run("split join round trip", checkSplitJoinRoundTrip)
	t.Run("join errors", checkJoinErrors)
	t.Run("one domain per context", checkOneDomainPerContext)
	t.Run("fallback never silent", checkFallbackNeverSilent)
	t.Run("wall steady sim sources", checkWallSteadySimSources)
	t.Run("sim tick agreement", checkSimTickAgreement)
	t.Run("set domain", checkSetDomain)
	t.Run("jump callbacks ordered", checkJumpCallbacksOrdered)
	t.Run("jump deregistration", checkJumpDeregistration)
	t.Run("sim agreement loud", checkSimAgreementLoud)
	t.Run("goal domain loud", checkGoalDomainLoud)
	t.Run("rebase deadlines", checkRebaseDeadlines)
	t.Run("publish period", checkPublishPeriod)
	t.Run("clock message", checkClockMessage)
	t.Run("bag loop jump", checkBagLoopJump)
	t.Run("pause and multi rate", checkPauseAndMultiRate)
	t.Run("concurrent callbacks", checkConcurrentCallbacks)
}

// checkClockQoS pins /clock topic plus QoS.
func checkClockQoS(t *testing.T) {
	t.Helper()
	if ClockTopic != "/clock" {
		t.Fatalf("ClockTopic must be /clock, got %q", ClockTopic)
	}
	got := ClockQoS()
	if got.History != HistoryKeepLast || got.Depth != ClockHistoryDepth {
		t.Fatalf("clock must be keep last depth %d, got %+v", ClockHistoryDepth, got)
	}
	if got.Reliability != ReliabilityBestEffort || got.Durability != DurabilityVolatile {
		t.Fatalf("clock must be best effort volatile, got %+v", got)
	}
	err := Validate(ResolveSystemDefaults(got))
	if err != nil {
		t.Fatalf("clock QoS must validate: %v", err)
	}
	if ClockPublishPeriodNanos != 25000000 {
		t.Fatalf("clock period must pin 40 Hz at 25 ms, got %d", ClockPublishPeriodNanos)
	}
}

// checkSimSecondTick pins sim second 1234 is the same tick everywhere.
func checkSimSecondTick(t *testing.T) {
	t.Helper()
	if got := SimSecondToNanos(1234); got != 1234000000000 {
		t.Fatalf("sim second 1234 must be 1234000000000, got %d", got)
	}
	if got := SimSecondToNanos(0); got != 0 {
		t.Fatalf("zero seconds must be zero, got %d", got)
	}
	if got := SimSecondToNanos(-5); got != 0 {
		t.Fatalf("negative seconds must clamp to zero, got %d", got)
	}
	sec, nsec := SplitNanos(1234000000000)
	if sec != 1234 || nsec != 0 {
		t.Fatalf("1234000000000 must split to 1234 plus 0, got %d plus %d", sec, nsec)
	}
	joined, err := JoinNanos(1234, 0)
	if err != nil {
		t.Fatalf("join 1234 plus 0: %v", err)
	}
	if joined != 1234000000000 {
		t.Fatalf("join must invert split, got %d", joined)
	}
}

// checkSplitJoinRoundTrip pins exact round trips with no float.
func checkSplitJoinRoundTrip(t *testing.T) {
	t.Helper()
	for _, nanos := range []int64{1, 999999999, 1000000000, 1234000000000, 1999999999} {
		sec, nsec := SplitNanos(nanos)
		back, err := JoinNanos(sec, nsec)
		if err != nil {
			t.Fatalf("round trip %d: %v", nanos, err)
		}
		if back != nanos {
			t.Fatalf("round trip %d must be exact, got %d", nanos, back)
		}
	}
	if sec, nsec := SplitNanos(-10); sec != 0 || nsec != 0 {
		t.Fatalf("negative split must clamp to zero, got %d plus %d", sec, nsec)
	}
	if got := MustJoinNanos(1234, 0); got != 1234000000000 {
		t.Fatalf("must join 1234 must be 1234000000000, got %d", got)
	}
}

// checkJoinErrors pins malformed join input fails loudly.
func checkJoinErrors(t *testing.T) {
	t.Helper()
	_, err := JoinNanos(-1, 0)
	if err == nil {
		t.Fatal("negative seconds must fail join")
	}
	_, err = JoinNanos(1, 1000000000)
	if err == nil {
		t.Fatal("nanoseconds above one second must fail join")
	}
	func() {
		defer func() {
			r := recover()
			if r == nil {
				t.Fatal("must join malformed must panic")
			}
		}()
		MustJoinNanos(-1, 0)
	}()
}

// checkOneDomainPerContext pins no globals with independent states.
func checkOneDomainPerContext(t *testing.T) {
	t.Helper()
	a := NewClockState(ClockWall, 1000, 2000)
	b := NewClockState(ClockSim, 3000, 4000)
	a.SetWallTime(1111)
	if got := b.WallNow(); got != 3000 {
		t.Fatalf("contexts must not leak wall, got %d", got)
	}
	if a.Domain() != ClockWall || b.Domain() != ClockSim {
		t.Fatal("domains must round trip per context")
	}
	_ = b.SetSimTime(5000)
	if got := a.Now(); got != 1111 {
		t.Fatalf("sim tick on b must not move a, got %d", got)
	}
}

// checkFallbackNeverSilent pins construction fallback is observable.
func checkFallbackNeverSilent(t *testing.T) {
	t.Helper()
	state := NewClockState(ClockSim, 777, 888)
	if !state.UsingFallback() {
		t.Fatal("sim without sample must report fallback")
	}
	if state.HasSimTime() {
		t.Fatal("fresh sim must have no sample")
	}
	if got := state.Now(); got != 777 {
		t.Fatalf("fallback Now must be wall 777, got %d", got)
	}
	jump := state.SetSimTime(1000)
	_ = jump
	if state.UsingFallback() {
		t.Fatal("sim with sample must not report fallback")
	}
	if got := state.Now(); got != 1000 {
		t.Fatalf("valid sim Now must be sim, got %d", got)
	}
}

// checkWallSteadySimSources pins explicit sources plus Now selection.
func checkWallSteadySimSources(t *testing.T) {
	t.Helper()
	wall := NewClockState(ClockWall, 100, 200)
	if got := wall.Now(); got != 100 {
		t.Fatalf("wall domain Now must be wall, got %d", got)
	}
	steady := NewClockState(ClockSteady, 100, 200)
	if got := steady.Now(); got != 200 {
		t.Fatalf("steady domain Now must be steady, got %d", got)
	}
	sim := NewClockState(ClockSim, 100, 200)
	_ = sim.SetSimTime(300)
	if got := sim.Now(); got != 300 {
		t.Fatalf("sim domain Now must be sim, got %d", got)
	}
	if got, ok := sim.SimNow(); !ok || got != 300 {
		t.Fatalf("SimNow must be 300 valid, got %d %t", got, ok)
	}
	wall.SetWallTime(150)
	if got := wall.WallNow(); got != 150 {
		t.Fatalf("SetWallTime must stick, got %d", got)
	}
	steady.SetSteadyTime(250)
	if got := steady.SteadyNow(); got != 250 {
		t.Fatalf("SetSteadyTime must stick, got %d", got)
	}
	wall.AdvanceWall(10)
	if got := wall.WallNow(); got != 160 {
		t.Fatalf("AdvanceWall must add, got %d", got)
	}
	steady.AdvanceSteady(10)
	if got := steady.SteadyNow(); got != 260 {
		t.Fatalf("AdvanceSteady must add, got %d", got)
	}
	if wall.UseSimTime() {
		t.Fatal("wall must not report sim time")
	}
	if !sim.UseSimTime() {
		t.Fatal("sim must report sim time")
	}
}

// checkSimTickAgreement pins 40 Hz ticks with exact nanos.
func checkSimTickAgreement(t *testing.T) {
	t.Helper()
	if !ShouldPublishClock(0, 25000000) {
		t.Fatal("25 ms gap must publish at 40 Hz")
	}
	if ShouldPublishClock(0, 24999999) {
		t.Fatal("gap below period must not publish")
	}
	state := NewClockState(ClockSim, 0, 0)
	_ = state.SetSimTime(SimSecondToNanos(1234))
	sec, nsec := state.ClockMessageNow()
	if sec != 1234 || nsec != 0 {
		t.Fatalf("/clock message for sim 1234 must be 1234 plus 0, got %d plus %d", sec, nsec)
	}
}

// checkSetDomain pins activation jumps plus invalid rejection.
func checkSetDomain(t *testing.T) {
	t.Helper()
	state := NewClockState(ClockWall, 1000, 1000)
	_ = state.SetSimTime(2000)
	assertWallToSim(t, state)
	assertSameDomainQuiet(t, state)
	assertSimToWall(t, state)
	assertInvalidDomainFails(t, state)
	assertUseSimTimeToggles(t, state)
	assertSteadyDomain(t, state)
}

// assertWallToSim pins wall to sim flags activation with sim active.
func assertWallToSim(t *testing.T, state *ClockState) {
	t.Helper()
	jump, err := state.SetDomain(ClockSim)
	if err != nil {
		t.Fatalf("set domain sim: %v", err)
	}
	if !jump.ActivationChanged {
		t.Fatal("wall to sim must flag activation")
	}
	if !jump.SimActive {
		t.Fatal("sim domain must report sim active")
	}
}

// assertSameDomainQuiet pins repeat domain generates no jump.
func assertSameDomainQuiet(t *testing.T, state *ClockState) {
	t.Helper()
	again, err := state.SetDomain(ClockSim)
	if err != nil {
		t.Fatalf("same domain: %v", err)
	}
	if again.ActivationChanged {
		t.Fatal("same domain must not flag activation")
	}
}

// assertSimToWall pins sim to wall flags activation.
func assertSimToWall(t *testing.T, state *ClockState) {
	t.Helper()
	back, err := state.SetDomain(ClockWall)
	if err != nil {
		t.Fatalf("set domain wall: %v", err)
	}
	if !back.ActivationChanged {
		t.Fatal("sim to wall must flag activation")
	}
}

// assertInvalidDomainFails pins invalid domain is an error.
func assertInvalidDomainFails(t *testing.T, state *ClockState) {
	t.Helper()
	if _, err := state.SetDomain(ClockDomain(99)); err == nil {
		t.Fatal("invalid domain must fail")
	}
}

// assertUseSimTimeToggles pins convenience wrapper flags activation.
func assertUseSimTimeToggles(t *testing.T, state *ClockState) {
	t.Helper()
	useSim := state.SetUseSimTime(true)
	if !useSim.ActivationChanged {
		t.Fatal("SetUseSimTime true must flag activation")
	}
	useWall := state.SetUseSimTime(false)
	if !useWall.ActivationChanged {
		t.Fatal("SetUseSimTime false must flag activation")
	}
}

// assertSteadyDomain pins steady reports no sim active.
func assertSteadyDomain(t *testing.T, state *ClockState) {
	t.Helper()
	steadyJump, err := state.SetDomain(ClockSteady)
	if err != nil {
		t.Fatalf("set domain steady: %v", err)
	}
	if steadyJump.SimActive {
		t.Fatal("steady must not report sim active")
	}
}

// checkJumpCallbacksOrdered pins registration order invocation.
func checkJumpCallbacksOrdered(t *testing.T) {
	t.Helper()
	state := NewClockState(ClockSim, 0, 0)
	var order []int
	var mu sync.Mutex
	_, err := state.RegisterJumpCallback(nil)
	if err == nil {
		t.Fatal("nil callback must fail registration")
	}
	_, err = state.RegisterJumpCallback(func(ClockJump) {
		mu.Lock()
		defer mu.Unlock()
		order = append(order, 1)
	})
	if err != nil {
		t.Fatalf("register first: %v", err)
	}
	_, err = state.RegisterJumpCallback(func(ClockJump) {
		mu.Lock()
		defer mu.Unlock()
		order = append(order, 2)
	})
	if err != nil {
		t.Fatalf("register second: %v", err)
	}
	_ = state.SetSimTime(1000)
	_ = state.SetSimTime(2000)
	mu.Lock()
	defer mu.Unlock()
	if len(order) != 4 || order[0] != 1 || order[1] != 2 || order[2] != 1 || order[3] != 2 {
		t.Fatalf("callbacks must run in registration order per jump, got %+v", order)
	}
}

// checkJumpDeregistration pins ordered deregistration with quiet double.
func checkJumpDeregistration(t *testing.T) {
	t.Helper()
	state := NewClockState(ClockSim, 0, 0)
	var first, second int
	id1, err := state.RegisterJumpCallback(func(ClockJump) { first++ })
	if err != nil {
		t.Fatalf("register first: %v", err)
	}
	id2, err := state.RegisterJumpCallback(func(ClockJump) { second++ })
	if err != nil {
		t.Fatalf("register second: %v", err)
	}
	_ = id2
	state.UnregisterJumpCallback(id1)
	_ = state.SetSimTime(1000)
	if first != 0 || second != 1 {
		t.Fatalf("deregistered first must not run, got %d %d", first, second)
	}
	state.UnregisterJumpCallback(id1)
	state.UnregisterJumpCallback(9999)
	_ = state.SetSimTime(2000)
	if second != 2 {
		t.Fatalf("survivor must keep running, got %d", second)
	}
}

// checkSimAgreementLoud pins loud mismatch on sim flag disagreement.
func checkSimAgreementLoud(t *testing.T) {
	t.Helper()
	if err := CheckUseSimTimeAgreement(true, true); err != nil {
		t.Fatalf("matching flags must pass: %v", err)
	}
	err := CheckUseSimTimeAgreement(true, false)
	if err == nil || !strings.Contains(err.Error(), "use_sim_time mismatch") {
		t.Fatalf("mismatch must be loud, got %v", err)
	}
	if !strings.Contains(err.Error(), "local true") || !strings.Contains(err.Error(), "remote false") {
		t.Fatalf("mismatch must carry both values, got %q", err.Error())
	}
	sim := NewClockState(ClockSim, 0, 0)
	err = sim.CheckPeerAgreement(ClockWall)
	if err == nil || !strings.Contains(err.Error(), "mismatch") {
		t.Fatalf("sim versus wall peer must mismatch, got %v", err)
	}
	wall := NewClockState(ClockWall, 0, 0)
	if err := wall.CheckPeerAgreement(ClockWall); err != nil {
		t.Fatalf("wall versus wall must pass: %v", err)
	}
	steady := NewClockState(ClockSteady, 0, 0)
	if err := steady.CheckPeerAgreement(ClockWall); err == nil {
		t.Fatal("steady versus wall must mismatch")
	}
}

// checkGoalDomainLoud pins wall stamp against sim surfaces explicitly.
func checkGoalDomainLoud(t *testing.T) {
	t.Helper()
	sim := NewClockState(ClockSim, 0, 0)
	_ = sim.SetSimTime(5000)
	err := sim.CheckGoal("nav", "goal-1", 4000, ClockWall)
	if err == nil {
		t.Fatal("wall goal against sim must fail")
	}
	if !strings.Contains(err.Error(), "goal-1") || !strings.Contains(err.Error(), "clock mismatch") {
		t.Fatalf("mismatch must carry goal plus clock, got %q", err.Error())
	}
	if !strings.Contains(err.Error(), "4000") || !strings.Contains(err.Error(), "sim") {
		t.Fatalf("mismatch must carry stamp plus domain, got %q", err.Error())
	}
	if err := sim.CheckGoal("nav", "goal-1", 4000, ClockSim); err != nil {
		t.Fatalf("sim goal against sim must pass: %v", err)
	}
	wall := NewClockState(ClockWall, 6000, 6000)
	err = wall.CheckGoal("nav", "goal-2", 6000, ClockSim)
	if err == nil {
		t.Fatal("sim goal against wall must fail")
	}
}

// checkRebaseDeadlines pins backward jumps rebase trackers.
func checkRebaseDeadlines(t *testing.T) {
	t.Helper()
	tracker := NewDeadlineTracker("/scan", "listener", NewDuration(0, 100000000), 1000000000)
	RebaseDeadlinesOnJump(ClockJump{FromNanos: 1000000000, ToNanos: 1100000000, SimActive: true}, &tracker)
	if _, ok := tracker.Check(1050000000); ok {
		t.Fatal("forward jump must not rebase")
	}
	RebaseDeadlinesOnJump(ClockJump{FromNanos: 1150000000, ToNanos: 900000000, Backward: true, SimActive: true}, &tracker)
	if _, ok := tracker.Check(950000000); ok {
		t.Fatal("rebased window must not alert early")
	}
	if _, ok := tracker.Check(1000000000); !ok {
		t.Fatal("rebased window must alert after a full deadline")
	}
	RebaseDeadlinesOnJump(ClockJump{FromNanos: 1000000000, ToNanos: 1000000000, ActivationChanged: true, SimActive: true}, &tracker)
	RebaseDeadlinesOnJump(ClockJump{FromNanos: 1000000000, ToNanos: 1100000000, SimActive: true}, nil, nil)
}

// checkPublishPeriod pins 40 Hz gating with no burst on rewind.
func checkPublishPeriod(t *testing.T) {
	t.Helper()
	if ShouldPublishClock(1000, 900) {
		t.Fatal("backward time must not publish")
	}
	if !ShouldPublishClock(0, ClockPublishPeriodNanos) {
		t.Fatal("exact period gap must publish")
	}
	if ShouldPublishClock(1000, 1000+ClockPublishPeriodNanos-1) {
		t.Fatal("gap below period must not publish")
	}
}

// checkClockMessage pins /clock message splits without float.
func checkClockMessage(t *testing.T) {
	t.Helper()
	state := NewClockState(ClockWall, 1234000000000, 0)
	sec, nsec := state.ClockMessageNow()
	if sec != 1234 || nsec != 0 {
		t.Fatalf("wall 1234000000000 must message as 1234 plus 0, got %d plus %d", sec, nsec)
	}
	sim := NewClockState(ClockSim, 0, 0)
	_ = sim.SetSimTime(1500000000)
	sec, nsec = sim.ClockMessageNow()
	if sec != 1 || nsec != 500000000 {
		t.Fatalf("sim 1.5 s must message as 1 plus 500 ms, got %d plus %d", sec, nsec)
	}
	fallback := NewClockState(ClockSim, 777, 0)
	sec, _ = fallback.ClockMessageNow()
	if sec != 0 {
		t.Fatalf("fallback 777 ns must message as zero seconds, got %d", sec)
	}
}

// checkBagLoopJump pins 1000 to 900 backward jump observed by all timers.
func checkBagLoopJump(t *testing.T) {
	t.Helper()
	state := NewClockState(ClockSim, 0, 0)
	var seen []ClockJump
	var mu sync.Mutex
	_, err := state.RegisterJumpCallback(func(j ClockJump) {
		mu.Lock()
		defer mu.Unlock()
		seen = append(seen, j)
	})
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	_, err = state.RegisterJumpCallback(func(j ClockJump) {
		mu.Lock()
		defer mu.Unlock()
		seen = append(seen, j)
	})
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	first := state.SetSimTime(SimSecondToNanos(1000))
	if first.Backward {
		t.Fatal("first sim tick must not be backward")
	}
	second := state.SetSimTime(SimSecondToNanos(900))
	if !second.Backward {
		t.Fatal("bag loop 1000 to 900 must flag backward")
	}
	if second.FromNanos != SimSecondToNanos(1000) || second.ToNanos != SimSecondToNanos(900) {
		t.Fatalf("jump must carry from 1000 to 900, got %+v", second)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(seen) != 4 {
		t.Fatalf("two timers times two ticks must observe four callbacks, got %d", len(seen))
	}
	last := seen[2]
	if !last.Backward || last.ToNanos != SimSecondToNanos(900) {
		t.Fatalf("observers must see the same backward jump, got %+v", last)
	}
}

// checkPauseAndMultiRate pins pauses plus independent rates.
func checkPauseAndMultiRate(t *testing.T) {
	t.Helper()
	paused := NewClockState(ClockSim, 0, 0)
	_ = paused.SetSimTime(5000)
	still := paused.Now()
	paused.SleepWall(2000000)
	if got := paused.Now(); got != still {
		t.Fatalf("pause must hold sim tick without new samples, got %d want %d", got, still)
	}
	fast := NewClockState(ClockWall, 0, 0)
	slow := NewClockState(ClockWall, 0, 0)
	fast.SetWallTime(100000000)
	slow.SetWallTime(100000000)
	if !ShouldPublishClock(0, fast.Now()) {
		t.Fatal("fast clock at 100 ms must publish")
	}
	if !ShouldPublishClock(0, slow.Now()) {
		t.Fatal("slow clock at 100 ms must publish")
	}
	fast.AdvanceWall(25000000)
	if !ShouldPublishClock(100000000, fast.Now()) {
		t.Fatal("fast 25 ms step must publish")
	}
	if ShouldPublishClock(100000000, slow.Now()) {
		t.Fatal("slow without advance must not publish")
	}
}

// checkConcurrentCallbacks pins concurrent register plus jump stays race free.
func checkConcurrentCallbacks(t *testing.T) {
	t.Helper()
	state := NewClockState(ClockSim, 0, 0)
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			_, err := state.RegisterJumpCallback(func(ClockJump) {})
			if err != nil {
				t.Error(err)
			}
			_ = state.SetSimTime(1000)
			_ = state.Now()
			_ = state.Domain()
		})
	}
	wg.Wait()
}
