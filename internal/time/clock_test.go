// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package clock_test

import (
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	clock "github.com/podomy/weld/internal/time"
)

// manualTime is a controllable nanoseconds source for tests.
type manualTime struct {
	// v is the current reading.
	v int64
}

// now returns the current manual value.
func (m *manualTime) now() int64 {
	return m.v
}

// fn returns now as a source function.
func (m *manualTime) fn() func() int64 {
	return m.now
}

// constFn returns a source function fixed at v.
func constFn(v int64) func() int64 {
	return func() int64 {
		return v
	}
}

// testSources bundles controllable sources with their domain.
type testSources struct {
	// wall feeds the wall timeline.
	wall *manualTime
	// steady feeds the steady timeline.
	steady *manualTime
	// sim feeds the sim timeline.
	sim *manualTime
	// domain is the domain under test.
	domain *clock.Domain
}

// newWallDomain builds a wall active domain on controllable sources.
func newWallDomain(t *testing.T) *testSources {
	t.Helper()
	s := &testSources{wall: &manualTime{v: 1000}, steady: &manualTime{v: 2000}, sim: &manualTime{v: 3000}}
	d, err := clock.NewDomain(clock.Options{
		WallFn:                     s.wall.fn(),
		SteadyFn:                   s.steady.fn(),
		SimFn:                      s.sim.fn(),
		JumpForwardThresholdNanos:  100,
		JumpBackwardThresholdNanos: 10,
	})
	if err != nil {
		t.Fatalf("new domain: %v", err)
	}
	s.domain = d
	return s
}

// newSimDomain builds a sim active domain on controllable sources.
func newSimDomain(t *testing.T) *testSources {
	t.Helper()
	s := &testSources{wall: &manualTime{v: 1000}, steady: &manualTime{v: 2000}, sim: &manualTime{v: 3000}}
	d, err := clock.NewDomain(clock.Options{
		WallFn:                     s.wall.fn(),
		SteadyFn:                   s.steady.fn(),
		SimFn:                      s.sim.fn(),
		UseSimTime:                 true,
		JumpForwardThresholdNanos:  100,
		JumpBackwardThresholdNanos: 10,
	})
	if err != nil {
		t.Fatalf("new domain: %v", err)
	}
	s.domain = d
	return s
}

// deferRemoveCallback deregisters id, reporting errors.
func deferRemoveCallback(t *testing.T, d *clock.Domain, id uint64) {
	t.Helper()
	if err := d.RemoveJumpCallback(id); err != nil {
		t.Errorf("remove callback: %v", err)
	}
}

// fireFunc returns a callback recording events into taps.
func fireFunc(taps *[]clock.JumpEvent) clock.JumpCallback {
	return func(ev clock.JumpEvent) {
		*taps = append(*taps, ev)
	}
}

// closeTimer closes tm, reporting errors.
func closeTimer(t *testing.T, tm *clock.Timer) {
	t.Helper()
	if err := tm.Close(); err != nil {
		t.Errorf("close: %v", err)
	}
}

// mustTimer builds a timer or fails the test.
func mustTimer(t *testing.T, d *clock.Domain, opts clock.TimerOptions) *clock.Timer {
	t.Helper()
	noop := func(clock.FireContext) {}
	tm, err := clock.NewTimer(d, opts, noop)
	if err != nil {
		t.Fatalf("new timer: %v", err)
	}
	return tm
}

// TestSourceKindString pins the timeline names.
func TestSourceKindString(t *testing.T) {
	t.Parallel()
	cases := map[clock.SourceKind]string{
		clock.KindUnspecified: "unspecified",
		clock.KindWall:        "wall",
		clock.KindSteady:      "steady",
		clock.KindSim:         "sim",
	}
	for kind, want := range cases {
		if got := kind.String(); got != want {
			t.Fatalf("kind %d = %q, want %q", int(kind), got, want)
		}
	}
	if got := clock.SourceKind(99).String(); got != "SourceKind(99)" {
		t.Fatalf("unknown kind = %q", got)
	}
}

// TestOverloadPolicyString pins the policy names.
func TestOverloadPolicyString(t *testing.T) {
	t.Parallel()
	cases := map[clock.OverloadPolicy]string{
		clock.OverloadInvalid:  "invalid",
		clock.OverloadCoalesce: "coalesce",
		clock.OverloadCatchUp:  "catch_up",
	}
	for policy, want := range cases {
		if got := policy.String(); got != want {
			t.Fatalf("policy %d = %q, want %q", int(policy), got, want)
		}
	}
	if got := clock.OverloadPolicy(99).String(); got != "OverloadPolicy(99)" {
		t.Fatalf("unknown policy = %q", got)
	}
}

// TestNewDomainDefaults pins the zero Options behavior.
func TestNewDomainDefaults(t *testing.T) {
	t.Parallel()
	d, err := clock.NewDomain(clock.Options{})
	if err != nil {
		t.Fatalf("new domain: %v", err)
	}
	if d.UseSimTime() {
		t.Fatal("zero options must not select sim time")
	}
	if d.ActiveKind() != clock.KindWall {
		t.Fatalf("active = %s, want wall", d.ActiveKind())
	}
	if d.Fallback() != "" {
		t.Fatalf("fallback = %q, want empty", d.Fallback())
	}
	if d.FallbackActive() {
		t.Fatal("fallback must be inactive without sim time")
	}
	if d.FallbackKind() != clock.KindSteady {
		t.Fatalf("fallback kind = %s, want steady", d.FallbackKind())
	}
	if d.JumpForwardThreshold() != clock.DefaultJumpForwardThresholdNanos {
		t.Fatalf("forward threshold = %d", d.JumpForwardThreshold())
	}
	if d.JumpBackwardThreshold() != clock.DefaultJumpBackwardThresholdNanos {
		t.Fatalf("backward threshold = %d", d.JumpBackwardThreshold())
	}
	if d.Generation() != 0 {
		t.Fatalf("generation = %d, want 0", d.Generation())
	}
	if d.SimAvailable() {
		t.Fatal("sim must be unavailable without source or set")
	}
	if _, ok := d.SimNow(); ok {
		t.Fatal("sim read must report unavailable")
	}
}

// TestNewDomainRejectsBadConfig pins loud construction errors.
func TestNewDomainRejectsBadConfig(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		opts clock.Options
	}{
		{"negative forward", clock.Options{JumpForwardThresholdNanos: -1}},
		{"negative backward", clock.Options{JumpBackwardThresholdNanos: -2}},
		{"sim fallback", clock.Options{FallbackKind: clock.KindSim}},
		{"invalid fallback", clock.Options{FallbackKind: clock.SourceKind(9)}},
	}
	for _, tc := range cases {
		_, err := clock.NewDomain(tc.opts)
		if err == nil {
			t.Fatalf("%s succeeded, want error", tc.name)
		}
	}
}

// TestDomainFallbackLoud pins the construction time fallback is never silent.
func TestDomainFallbackLoud(t *testing.T) {
	t.Parallel()
	steady := &manualTime{v: 7000}
	d, err := clock.NewDomain(clock.Options{
		SteadyFn:   steady.fn(),
		UseSimTime: true,
	})
	if err != nil {
		t.Fatalf("new domain: %v", err)
	}
	if d.Fallback() == "" {
		t.Fatal("fallback reason must be loud")
	}
	if !strings.Contains(d.Fallback(), "use_sim_time") {
		t.Fatalf("fallback %q must name use_sim_time", d.Fallback())
	}
	if d.ActiveKind() != clock.KindSteady {
		t.Fatalf("active = %s, want steady fallback", d.ActiveKind())
	}
	if !d.FallbackActive() {
		t.Fatal("fallback must be active before first sim input")
	}
	if d.FallbackCount() == 0 {
		t.Fatal("fallback count must be nonzero from the construction seed")
	}
	if !strings.Contains(d.Describe(), "fallback") {
		t.Fatalf("describe %q must carry the fallback", d.Describe())
	}
	checkFallbackFirstSim(t, d)
}

// checkFallbackFirstSim pins the first sim input ends fallback loudly.
func checkFallbackFirstSim(t *testing.T, d *clock.Domain) {
	t.Helper()
	ev, jumped, err := d.SetSimTime(5000000000)
	if err != nil {
		t.Fatalf("set sim time: %v", err)
	}
	if !jumped || !ev.Forward {
		t.Fatalf("first sim input must fire a forward jump, got %+v %t", ev, jumped)
	}
	if d.FallbackActive() {
		t.Fatal("fallback must clear once sim arrives")
	}
	if d.ActiveKind() != clock.KindSim {
		t.Fatalf("active = %s, want sim", d.ActiveKind())
	}
	if d.Fallback() == "" {
		t.Fatal("fallback history must stay sticky after sim arrives")
	}
}

// TestSetSimTimeExternal pins external sim sources reject manual drives.
func TestSetSimTimeExternal(t *testing.T) {
	t.Parallel()
	s := newSimDomain(t)
	_, _, err := s.domain.SetSimTime(42)
	if !errors.Is(err, clock.ErrExternalSim) {
		t.Fatalf("set sim time = %v, want ErrExternalSim", err)
	}
}

// TestDomainExplicitSources pins the three timeline readings and switching.
func TestDomainExplicitSources(t *testing.T) {
	t.Parallel()
	s := newWallDomain(t)
	if got := s.domain.Now(); got != 1000 {
		t.Fatalf("now = %d, want wall 1000", got)
	}
	if got := s.domain.WallNow(); got != 1000 {
		t.Fatalf("wall = %d", got)
	}
	if got := s.domain.SteadyNow(); got != 2000 {
		t.Fatalf("steady = %d", got)
	}
	if got, ok := s.domain.SimNow(); !ok || got != 3000 {
		t.Fatalf("sim = %d %t", got, ok)
	}
	checkActivationSwitch(t, s)
}

// checkActivationSwitch pins sim activation and deactivation notify.
func checkActivationSwitch(t *testing.T, s *testSources) {
	t.Helper()
	ev, switched := s.domain.SetUseSimTime(true)
	if !switched {
		t.Fatal("activation must report a switch")
	}
	if !ev.SimActivationChange {
		t.Fatalf("activation must flag change, got %+v", ev)
	}
	if s.domain.Now() != 3000 {
		t.Fatalf("now = %d, want sim 3000", s.domain.Now())
	}
	if _, switched := s.domain.SetUseSimTime(true); switched {
		t.Fatal("repeated set must report no switch")
	}
	ev, switched = s.domain.SetUseSimTime(false)
	if !switched || !ev.SimActivationChange {
		t.Fatalf("deactivation must switch with change flag, got %+v %t", ev, switched)
	}
	if s.domain.Generation() != 2 {
		t.Fatalf("generation = %d, want 2 for two switches", s.domain.Generation())
	}
}

// TestJumpCallbacksOrdered pins ordered fire plus ordered deregistration.
func TestJumpCallbacksOrdered(t *testing.T) {
	t.Parallel()
	s := newSimDomain(t)
	var order []uint64
	add := func(id uint64) uint64 {
		num := id
		cbID, err := s.domain.AddJumpCallback(func(clock.JumpEvent) {
			order = append(order, num)
		})
		if err != nil {
			t.Fatalf("add callback: %v", err)
		}
		return cbID
	}
	first := add(1)
	second := add(2)
	third := add(3)
	if _, err := s.domain.AddJumpCallback(nil); !errors.Is(err, clock.ErrNilJumpCallback) {
		t.Fatalf("nil add = %v", err)
	}
	if err := s.domain.RemoveJumpCallback(999); !errors.Is(err, clock.ErrUnknownJumpCallback) {
		t.Fatalf("unknown remove = %v", err)
	}
	if err := s.domain.RemoveJumpCallback(second); err != nil {
		t.Fatalf("remove middle: %v", err)
	}
	s.sim.v = 5000
	if _, jumped := s.domain.Poll(); !jumped {
		t.Fatal("jump must fire")
	}
	assertOrder(t, order, []uint64{1, 3})
	if err := s.domain.RemoveJumpCallback(first); err != nil {
		t.Fatalf("remove first: %v", err)
	}
	order = nil
	s.sim.v = 9000
	if _, jumped := s.domain.Poll(); !jumped {
		t.Fatal("second jump must fire")
	}
	assertOrder(t, order, []uint64{3})
	_ = third
}

// assertOrder pins the observed callback order.
func assertOrder(t *testing.T, got, want []uint64) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("order = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("order = %v, want %v", got, want)
		}
	}
}

// TestPollThresholds pins quiet steps stay silent without drift.
func TestPollThresholds(t *testing.T) {
	t.Parallel()
	t.Run("forward", checkForwardThreshold)
	t.Run("backward", checkBackwardThreshold)
}

// checkForwardThreshold pins sub threshold forward steps never fire.
func checkForwardThreshold(t *testing.T) {
	t.Helper()
	s := newSimDomain(t)
	var taps []clock.JumpEvent
	id, err := s.domain.AddJumpCallback(fireFunc(&taps))
	if err != nil {
		t.Fatalf("add callback: %v", err)
	}
	defer deferRemoveCallback(t, s.domain, id)
	for range 5 {
		s.sim.v += 20
		if _, jumped := s.domain.Poll(); jumped {
			t.Fatal("20ns steps must stay below the 100ns forward threshold")
		}
	}
	if len(taps) != 0 {
		t.Fatalf("taps = %d, want 0 after quiet steps", len(taps))
	}
	s.sim.v += 100
	if _, jumped := s.domain.Poll(); !jumped {
		t.Fatal("100ns step must fire at the threshold")
	}
	if len(taps) != 1 || !taps[0].Forward {
		t.Fatalf("taps = %+v, want one forward jump", taps)
	}
}

// checkBackwardThreshold pins backward jumps invalidate caches once.
func checkBackwardThreshold(t *testing.T) {
	t.Helper()
	s := newSimDomain(t)
	s.sim.v -= 5
	if _, jumped := s.domain.Poll(); jumped {
		t.Fatal("5ns backward step must stay below the 10ns threshold")
	}
	if s.domain.Generation() != 0 {
		t.Fatalf("generation = %d, want 0 with no backward jump", s.domain.Generation())
	}
	s.sim.v -= 50
	ev, jumped := s.domain.Poll()
	if !jumped || !ev.Backward {
		t.Fatalf("backward step must fire, got %+v %t", ev, jumped)
	}
	if s.domain.Generation() != 1 {
		t.Fatalf("generation = %d, want 1 after backward jump", s.domain.Generation())
	}
}

// TestPollAllocationFree pins the fire path allocates nothing.
//
// AllocsPerRun refuses parallel tests, so this test stays sequential.
func TestPollAllocationFree(t *testing.T) {
	fake := clock.NewFake(1000)
	d, err := clock.NewDomain(clock.Options{
		SimFn:                      fake.NowFunc(),
		UseSimTime:                 true,
		JumpForwardThresholdNanos:  5,
		JumpBackwardThresholdNanos: 5,
	})
	if err != nil {
		t.Fatalf("new domain: %v", err)
	}
	var count atomic.Uint64
	id, err := d.AddJumpCallback(func(clock.JumpEvent) {
		count.Add(1)
	})
	if err != nil {
		t.Fatalf("add callback: %v", err)
	}
	defer deferRemoveCallback(t, d, id)
	if allocs := testing.AllocsPerRun(200, func() {
		_, _ = d.Poll()
	}); allocs != 0 {
		t.Fatalf("quiet poll allocs = %v, want 0", allocs)
	}
	var advFailed atomic.Bool
	if allocs := testing.AllocsPerRun(50, func() {
		_, advErr := fake.AdvanceSim(10)
		if advErr != nil {
			advFailed.Store(true)
		}
		_, _ = d.Poll()
	}); allocs != 0 {
		t.Fatalf("firing poll allocs = %v, want 0", allocs)
	}
	if advFailed.Load() {
		t.Fatal("advance failed during alloc test")
	}
	if count.Load() == 0 {
		t.Fatal("firing polls must invoke callbacks")
	}
}

// TestTimerInit pins init reads now, last plus next, and canceled state.
func TestTimerInit(t *testing.T) {
	t.Parallel()
	s := newWallDomain(t)
	tm := mustTimer(t, s.domain, clock.TimerOptions{Overload: clock.OverloadCatchUp, Autostart: true, PeriodNanos: 100})
	if tm.Kind() != clock.KindSteady {
		t.Fatalf("kind = %s, want steady default", tm.Kind())
	}
	if tm.LastCall() != 2000 || tm.NextCall() != 2100 {
		t.Fatalf("last = %d next = %d, want 2000 and 2100", tm.LastCall(), tm.NextCall())
	}
	if tm.Canceled() {
		t.Fatal("autostart timer must not be canceled")
	}
	parked := mustTimer(t, s.domain, clock.TimerOptions{Kind: clock.KindWall, Overload: clock.OverloadCoalesce, PeriodNanos: 50})
	if !parked.Canceled() {
		t.Fatal("non autostart timer must be canceled")
	}
	if parked.LastCall() != 1000 || parked.NextCall() != 1050 {
		t.Fatalf("wall last = %d next = %d", parked.LastCall(), parked.NextCall())
	}
	if err := tm.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if err := parked.Close(); err != nil {
		t.Fatalf("close parked: %v", err)
	}
}

// TestTimerInitRejects pins loud timer construction errors.
func TestTimerInitRejects(t *testing.T) {
	t.Parallel()
	s := newWallDomain(t)
	noop := func(clock.FireContext) {}
	cases := []struct {
		name string
		opts clock.TimerOptions
	}{
		{"negative period", clock.TimerOptions{Overload: clock.OverloadCatchUp, PeriodNanos: -1}},
		{"invalid overload", clock.TimerOptions{PeriodNanos: 10, Overload: clock.OverloadInvalid}},
		{"invalid kind", clock.TimerOptions{Kind: clock.KindSim + 9, PeriodNanos: 10, Overload: clock.OverloadCatchUp}},
	}
	for _, tc := range cases {
		if _, err := clock.NewTimer(s.domain, tc.opts, noop); err == nil {
			t.Fatalf("%s succeeded, want error", tc.name)
		}
	}
	if _, err := clock.NewTimer(nil, clock.TimerOptions{Overload: clock.OverloadCatchUp}, noop); !errors.Is(err, clock.ErrNilDomain) {
		t.Fatalf("nil domain = %v", err)
	}
	if _, err := clock.NewTimer(s.domain, clock.TimerOptions{Overload: clock.OverloadCatchUp}, nil); !errors.Is(err, clock.ErrNilTimerCallback) {
		t.Fatalf("nil callback = %v", err)
	}
}

// TestTimerCall pins readiness, times, and backlog accounting.
func TestTimerCall(t *testing.T) {
	t.Parallel()
	t.Run("catch up first fire", checkCatchUpFirstFire)
	t.Run("catch up drains", checkCatchUpDrain)
	t.Run("coalesce drops loud", checkTimerCoalesce)
	t.Run("zero period ready", checkTimerZeroPeriod)
	t.Run("canceled rejects", checkTimerCanceledCall)
}

// newCatchUpTimer builds an autostart 100ns catch up timer on steady 2000.
func newCatchUpTimer(t *testing.T, s *testSources, got *[]clock.FireContext) *clock.Timer {
	t.Helper()
	tm, err := clock.NewTimer(s.domain, clock.TimerOptions{Overload: clock.OverloadCatchUp, Autostart: true, PeriodNanos: 100}, func(ctx clock.FireContext) {
		*got = append(*got, ctx)
	})
	if err != nil {
		t.Fatalf("new timer: %v", err)
	}
	return tm
}

// checkCatchUpFirstFire pins readiness and exact first fire times.
func checkCatchUpFirstFire(t *testing.T) {
	t.Helper()
	s := newWallDomain(t)
	var got []clock.FireContext
	tm := newCatchUpTimer(t, s, &got)
	defer closeTimer(t, tm)
	s.steady.v = 2050
	if tm.IsReady() {
		t.Fatal("timer must not be ready at 2050 with next 2100")
	}
	if _, err := tm.Call(); !errors.Is(err, clock.ErrTimerNotReady) {
		t.Fatalf("early call = %v, want ErrTimerNotReady", err)
	}
	s.steady.v = 2100
	ctx, err := tm.Call()
	if err != nil {
		t.Fatalf("call: %v", err)
	}
	if ctx.ExpectedNanos != 2100 || ctx.ActualNanos != 2100 || ctx.SinceLastNanos != 100 || ctx.Backlog != 0 {
		t.Fatalf("fire = %+v", ctx)
	}
	if len(got) != 1 {
		t.Fatalf("callbacks = %d, want 1", len(got))
	}
}

// checkCatchUpDrain pins one period steps with visible backlog.
func checkCatchUpDrain(t *testing.T) {
	t.Helper()
	s := newWallDomain(t)
	var got []clock.FireContext
	tm := newCatchUpTimer(t, s, &got)
	defer closeTimer(t, tm)
	s.steady.v = 2550
	ctx, err := tm.Call()
	if err != nil {
		t.Fatalf("late call: %v", err)
	}
	if ctx.Backlog != 3 {
		t.Fatalf("backlog = %d, want 3", ctx.Backlog)
	}
	if tm.Dropped() != 0 {
		t.Fatalf("dropped = %d, want 0 for catch up", tm.Dropped())
	}
	drainTimer(t, tm, 4)
	if tm.IsReady() {
		t.Fatal("timer must drain exactly to unready")
	}
	if tm.Fires() != 5 {
		t.Fatalf("fires = %d, want 5", tm.Fires())
	}
	if len(got) != 5 {
		t.Fatalf("callbacks = %d, want 5", len(got))
	}
}

// checkTimerCoalesce pins skip ahead with counted drops.
func checkTimerCoalesce(t *testing.T) {
	t.Helper()
	s := newWallDomain(t)
	tm := mustTimer(t, s.domain, clock.TimerOptions{Overload: clock.OverloadCoalesce, Autostart: true, PeriodNanos: 100})
	defer closeTimer(t, tm)
	s.steady.v = 2550
	ctx, err := tm.Call()
	if err != nil {
		t.Fatalf("call: %v", err)
	}
	if ctx.ExpectedNanos != 2100 {
		t.Fatalf("expected = %d, want 2100", ctx.ExpectedNanos)
	}
	if tm.Dropped() != 4 {
		t.Fatalf("dropped = %d, want 4", tm.Dropped())
	}
	if tm.NextCall() != 2650 {
		t.Fatalf("next = %d, want now plus period 2650", tm.NextCall())
	}
	if tm.Backlog() != 0 {
		t.Fatalf("backlog = %d, want 0 after coalesce", tm.Backlog())
	}
}

// checkTimerZeroPeriod pins zero period timers are always ready.
func checkTimerZeroPeriod(t *testing.T) {
	t.Helper()
	s := newWallDomain(t)
	tm := mustTimer(t, s.domain, clock.TimerOptions{Overload: clock.OverloadCatchUp, Autostart: true})
	defer closeTimer(t, tm)
	if !tm.IsReady() {
		t.Fatal("zero period must be ready")
	}
	first, err := tm.Call()
	if err != nil {
		t.Fatalf("call: %v", err)
	}
	if first.ExpectedNanos != 2000 || first.SinceLastNanos != 0 {
		t.Fatalf("first fire = %+v", first)
	}
	s.steady.v = 2007
	second, err := tm.Call()
	if err != nil {
		t.Fatalf("second call: %v", err)
	}
	if second.ExpectedNanos != 2007 || second.SinceLastNanos != 7 {
		t.Fatalf("second fire = %+v", second)
	}
}

// checkTimerCanceledCall pins canceled timers reject calls.
func checkTimerCanceledCall(t *testing.T) {
	t.Helper()
	s := newWallDomain(t)
	tm := mustTimer(t, s.domain, clock.TimerOptions{Kind: clock.KindWall, Overload: clock.OverloadCatchUp, Autostart: true, PeriodNanos: 10})
	defer closeTimer(t, tm)
	tm.Cancel()
	if tm.IsReady() {
		t.Fatal("canceled timer must not be ready")
	}
	if _, err := tm.Call(); !errors.Is(err, clock.ErrTimerCanceled) {
		t.Fatalf("call = %v, want ErrTimerCanceled", err)
	}
	if tm.Backlog() != 0 {
		t.Fatalf("canceled backlog = %d, want 0", tm.Backlog())
	}
}

// TestTimerReset pins reset re-arms, clears canceled, guards, and notifies.
func TestTimerReset(t *testing.T) {
	t.Parallel()
	s := newWallDomain(t)
	var resets uint64
	tm := mustTimer(t, s.domain, clock.TimerOptions{Overload: clock.OverloadCatchUp, Autostart: true, PeriodNanos: 100})
	defer closeTimer(t, tm)
	tm.SetResetCallback(func() {
		resets++
	})
	tm.Cancel()
	s.steady.v = 2500
	tm.Reset()
	if tm.Canceled() {
		t.Fatal("reset must clear canceled")
	}
	if tm.NextCall() != 2600 {
		t.Fatalf("next = %d, want now plus period 2600", tm.NextCall())
	}
	if tm.GuardCount() != 1 {
		t.Fatalf("guards = %d, want 1", tm.GuardCount())
	}
	if resets != 1 {
		t.Fatalf("resets = %d, want 1", resets)
	}
	tm.SetResetCallback(nil)
	tm.Reset()
	if resets != 1 {
		t.Fatalf("cleared reset must not fire, got %d", resets)
	}
	if tm.GuardCount() != 2 {
		t.Fatalf("guards = %d, want 2", tm.GuardCount())
	}
}

// TestTimerStart pins start re-arms parked timers only.
func TestTimerStart(t *testing.T) {
	t.Parallel()
	s := newWallDomain(t)
	tm := mustTimer(t, s.domain, clock.TimerOptions{Overload: clock.OverloadCatchUp, Autostart: true, PeriodNanos: 100})
	defer closeTimer(t, tm)
	if tm.Start() {
		t.Fatal("start on a running timer must report false")
	}
	tm.Cancel()
	s.steady.v = 2900
	if !tm.Start() {
		t.Fatal("start on a canceled timer must report true")
	}
	if tm.NextCall() != 3000 {
		t.Fatalf("next = %d, want now plus period 3000", tm.NextCall())
	}
}

// TestTimerPeriodCallback pins period and callback setters.
func TestTimerPeriodCallback(t *testing.T) {
	t.Parallel()
	s := newWallDomain(t)
	tm := mustTimer(t, s.domain, clock.TimerOptions{Overload: clock.OverloadCatchUp, Autostart: true, PeriodNanos: 100})
	defer closeTimer(t, tm)
	if tm.Period() != 100 || tm.Overload() != clock.OverloadCatchUp {
		t.Fatalf("period = %d overload = %s", tm.Period(), tm.Overload())
	}
	if err := tm.SetPeriod(-5); err == nil {
		t.Fatal("negative period must fail")
	}
	if tm.Period() != 100 {
		t.Fatalf("period = %d, want unchanged 100", tm.Period())
	}
	if err := tm.SetPeriod(250); err != nil {
		t.Fatalf("set period: %v", err)
	}
	if err := tm.SetCallback(nil); !errors.Is(err, clock.ErrNilTimerCallback) {
		t.Fatalf("nil callback = %v", err)
	}
	var fires uint64
	if err := tm.SetCallback(func(clock.FireContext) {
		fires++
	}); err != nil {
		t.Fatalf("set callback: %v", err)
	}
	s.steady.v = 2100
	if _, err := tm.Call(); err != nil {
		t.Fatalf("call: %v", err)
	}
	if fires != 1 {
		t.Fatalf("fires = %d, want 1", fires)
	}
}

// TestTimerJumpHandling pins credit restore, guard on pass, and rebase.
func TestTimerJumpHandling(t *testing.T) {
	t.Parallel()
	t.Run("forward pass guards", checkForwardJumpRebase)
	t.Run("backward past last resets", checkBackwardJumpRebase)
	t.Run("canceled ignores", checkCanceledIgnoresJumps)
	t.Run("steady ignores sim", checkSteadyIgnoresSim)
}

// newSimTimerPair builds settled sim plus steady timers at sim 3000.
func newSimTimerPair(t *testing.T, s *testSources) (*clock.Timer, *clock.Timer) {
	t.Helper()
	if _, jumped := s.domain.Poll(); jumped {
		t.Fatal("settle poll without motion must not fire")
	}
	simTimer := mustTimer(t, s.domain, clock.TimerOptions{Kind: clock.KindSim, Overload: clock.OverloadCatchUp, Autostart: true, PeriodNanos: 100})
	steadyTimer := mustTimer(t, s.domain, clock.TimerOptions{Kind: clock.KindSteady, Overload: clock.OverloadCatchUp, Autostart: true, PeriodNanos: 1000})
	return simTimer, steadyTimer
}

// closeTimerPair closes both timers, reporting errors.
func closeTimerPair(t *testing.T, first, second *clock.Timer) {
	t.Helper()
	if err := first.Close(); err != nil {
		t.Errorf("close first: %v", err)
	}
	if err := second.Close(); err != nil {
		t.Errorf("close second: %v", err)
	}
}

// checkForwardJumpRebase pins credit restore plus guard on pass.
func checkForwardJumpRebase(t *testing.T) {
	t.Helper()
	s := newSimDomain(t)
	simTimer, steadyTimer := newSimTimerPair(t, s)
	defer closeTimerPair(t, simTimer, steadyTimer)
	s.sim.v = 3250
	ev, jumped := s.domain.Poll()
	if !jumped || !ev.Forward {
		t.Fatalf("forward step must fire, got %+v %t", ev, jumped)
	}
	if simTimer.GuardCount() != 1 {
		t.Fatalf("forward pass must guard once, got %d", simTimer.GuardCount())
	}
	if simTimer.NextCall() != 3350 {
		t.Fatalf("next = %d, want to plus credit 3350", simTimer.NextCall())
	}
}

// checkBackwardJumpRebase pins reset to now plus period past last call.
func checkBackwardJumpRebase(t *testing.T) {
	t.Helper()
	s := newSimDomain(t)
	simTimer, steadyTimer := newSimTimerPair(t, s)
	defer closeTimerPair(t, simTimer, steadyTimer)
	s.sim.v = 2900
	ev, jumped := s.domain.Poll()
	if !jumped || !ev.Backward {
		t.Fatalf("backward step must fire, got %+v %t", ev, jumped)
	}
	if simTimer.NextCall() != 3000 {
		t.Fatalf("backward past last must reset to now plus period, got %d", simTimer.NextCall())
	}
	if s.domain.Generation() != 1 {
		t.Fatalf("generation = %d, want 1", s.domain.Generation())
	}
}

// checkCanceledIgnoresJumps pins canceled timers skip rebasing.
func checkCanceledIgnoresJumps(t *testing.T) {
	t.Helper()
	s := newSimDomain(t)
	simTimer, steadyTimer := newSimTimerPair(t, s)
	defer closeTimerPair(t, simTimer, steadyTimer)
	simTimer.Cancel()
	before := simTimer.NextCall()
	s.sim.v = 2600
	if _, jumped := s.domain.Poll(); !jumped {
		t.Fatal("jump must fire")
	}
	if simTimer.NextCall() != before {
		t.Fatal("canceled timer must ignore jumps")
	}
}

// checkSteadyIgnoresSim pins steady timers survive sim jumps untouched.
func checkSteadyIgnoresSim(t *testing.T) {
	t.Helper()
	s := newSimDomain(t)
	simTimer, steadyTimer := newSimTimerPair(t, s)
	defer closeTimerPair(t, simTimer, steadyTimer)
	steadyNext := steadyTimer.NextCall()
	s.sim.v = 3250
	if _, jumped := s.domain.Poll(); !jumped {
		t.Fatal("forward step must fire")
	}
	if steadyTimer.NextCall() != steadyNext {
		t.Fatal("steady timer must ignore sim jumps")
	}
}

// TestTimerClose pins ordered deregistration and terminal calls.
func TestTimerClose(t *testing.T) {
	t.Parallel()
	s := newSimDomain(t)
	var taps []string
	_, err := s.domain.AddJumpCallback(func(clock.JumpEvent) {
		taps = append(taps, "probe")
	})
	if err != nil {
		t.Fatalf("add probe: %v", err)
	}
	tm := mustTimer(t, s.domain, clock.TimerOptions{Kind: clock.KindSim, Overload: clock.OverloadCatchUp, Autostart: true, PeriodNanos: 100})
	before := tm.NextCall()
	if err := tm.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if err := tm.Close(); err != nil {
		t.Fatalf("second close must be idempotent: %v", err)
	}
	if _, err := tm.Call(); !errors.Is(err, clock.ErrTimerClosed) {
		t.Fatalf("call after close = %v", err)
	}
	s.sim.v = 9000
	if _, jumped := s.domain.Poll(); !jumped {
		t.Fatal("jump must fire")
	}
	if tm.NextCall() != before {
		t.Fatal("closed timer must ignore jumps")
	}
	if len(taps) != 1 || taps[0] != "probe" {
		t.Fatalf("probe taps = %v, want one probe firing", taps)
	}
}

// TestSimAgreement pins loud use_sim_time mismatch errors.
func TestSimAgreement(t *testing.T) {
	t.Parallel()
	s := newSimDomain(t)
	if err := s.domain.CheckActionSimAgreement(true, "navigate_to_pose"); err != nil {
		t.Fatalf("matching flags must pass: %v", err)
	}
	err := s.domain.CheckActionSimAgreement(false, "navigate_to_pose")
	if err == nil {
		t.Fatal("flag mismatch must fail")
	}
	if !clock.IsSimMismatch(err) {
		t.Fatalf("error = %v, want SimMismatchError", err)
	}
	assertMismatchText(t, err, "navigate_to_pose", "client_use_sim_time=false", "domain_use_sim_time=true", "goal_stamp_ns=-1")
	if err := s.domain.CheckGoalStamp(3000, clock.KindSim, "navigate_to_pose"); err != nil {
		t.Fatalf("sim stamp on sim domain must pass: %v", err)
	}
	err = s.domain.CheckGoalStamp(123456, clock.KindWall, "navigate_to_pose")
	if err == nil {
		t.Fatal("wall stamp on sim domain must fail")
	}
	assertMismatchText(t, err, "navigate_to_pose", "goal_stamp_ns=123456", "stamp_kind=wall", "active_kind=sim")
	err = s.domain.CheckGoalStamp(99, clock.KindUnspecified, "navigate_to_pose")
	if err == nil {
		t.Fatal("unspecified stamp kind must fail")
	}
	if clock.IsSimMismatch(nil) {
		t.Fatal("nil must not report mismatch")
	}
	if clock.IsSimMismatch(errors.New("plain")) {
		t.Fatal("plain error must not report mismatch")
	}
}

// assertMismatchText pins the loud fields inside a mismatch error.
func assertMismatchText(t *testing.T, err error, wants ...string) {
	t.Helper()
	for _, want := range wants {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("error %q misses %q", err.Error(), want)
		}
	}
}

// TestCacheGuard pins backward jumps invalidate while forward motion is warm.
func TestCacheGuard(t *testing.T) {
	t.Parallel()
	s := newSimDomain(t)
	var guard clock.CacheGuard
	guard.Observe(s.domain.Generation())
	if guard.Stale(s.domain.Generation()) {
		t.Fatal("fresh guard must not be stale")
	}
	s.sim.v = 3200
	if _, jumped := s.domain.Poll(); !jumped {
		t.Fatal("forward step must fire")
	}
	if guard.Stale(s.domain.Generation()) {
		t.Fatal("forward jumps must keep caches warm")
	}
	s.sim.v = 3100
	if _, jumped := s.domain.Poll(); !jumped {
		t.Fatal("backward step must fire")
	}
	if !guard.Stale(s.domain.Generation()) {
		t.Fatal("backward jump must invalidate caches")
	}
	guard.Observe(s.domain.Generation())
	if guard.Stale(s.domain.Generation()) {
		t.Fatal("observed guard must be clean")
	}
}

// TestFakePrograms pins one-shot jumps, pauses, and stale pruning.
func TestFakePrograms(t *testing.T) {
	t.Parallel()
	t.Run("jump stops remainder", checkFakeJumpStops)
	t.Run("pause freezes", checkFakePause)
	t.Run("stale prunes", checkFakeStalePrunes)
	t.Run("tie jumps first", checkFakeTieJumpFirst)
	t.Run("rejects bad input", checkFakeRejects)
}

// checkFakeJumpStops pins a jump teleports and discards the remainder.
func checkFakeJumpStops(t *testing.T) {
	t.Helper()
	fake := clock.NewFake(0)
	fake.ProgramJump(50, 500)
	got, err := fake.AdvanceSim(1000)
	if err != nil {
		t.Fatalf("advance: %v", err)
	}
	if got != 500 {
		t.Fatalf("now = %d, want jump target 500 not 1000", got)
	}
	if fake.PendingJumps() != 0 || fake.JumpsFired() != 1 {
		t.Fatalf("pending = %d fired = %d", fake.PendingJumps(), fake.JumpsFired())
	}
	got, err = fake.AdvanceSim(10)
	if err != nil {
		t.Fatalf("second advance: %v", err)
	}
	if got != 510 {
		t.Fatalf("now = %d, want 510", got)
	}
}

// checkFakePause pins pauses stop exactly and freeze.
func checkFakePause(t *testing.T) {
	t.Helper()
	fake := clock.NewFake(0)
	fake.ProgramPause(70)
	got, err := fake.AdvanceSim(100)
	if err != nil {
		t.Fatalf("advance: %v", err)
	}
	if got != 70 {
		t.Fatalf("now = %d, want pause time 70", got)
	}
	if !fake.Paused() || fake.PausesHit() != 1 {
		t.Fatalf("paused = %t hits = %d", fake.Paused(), fake.PausesHit())
	}
	got, err = fake.AdvanceSim(100)
	if err != nil {
		t.Fatalf("paused advance: %v", err)
	}
	if got != 70 {
		t.Fatalf("paused now = %d, want 70", got)
	}
	fake.Resume()
	got, err = fake.AdvanceSim(10)
	if err != nil {
		t.Fatalf("resume advance: %v", err)
	}
	if got != 80 {
		t.Fatalf("resumed now = %d, want 80", got)
	}
	fake.ClearPrograms()
	if fake.PendingPauses() != 0 {
		t.Fatal("clear must drop pending pauses")
	}
}

// checkFakeStalePrunes pins teleports strand old triggers.
func checkFakeStalePrunes(t *testing.T) {
	t.Helper()
	fake := clock.NewFake(0)
	fake.ProgramJump(10, 90)
	fake.Set(50)
	got, err := fake.AdvanceSim(10)
	if err != nil {
		t.Fatalf("advance: %v", err)
	}
	if got != 60 {
		t.Fatalf("now = %d, want 60 with stale jump pruned", got)
	}
	if fake.PendingJumps() != 0 || fake.JumpsFired() != 0 {
		t.Fatalf("pending = %d fired = %d, want stale dropped unfired", fake.PendingJumps(), fake.JumpsFired())
	}
}

// checkFakeTieJumpFirst pins jump wins same time ties.
func checkFakeTieJumpFirst(t *testing.T) {
	t.Helper()
	fake := clock.NewFake(0)
	fake.ProgramJump(100, 50)
	fake.ProgramPause(100)
	got, err := fake.AdvanceSim(100)
	if err != nil {
		t.Fatalf("advance: %v", err)
	}
	if got != 50 {
		t.Fatalf("now = %d, want jump target 50", got)
	}
	if fake.Paused() {
		t.Fatal("tie must resolve to the jump, not the pause")
	}
	if fake.PendingPauses() != 1 {
		t.Fatalf("pending pauses = %d, want pause still armed", fake.PendingPauses())
	}
}

// checkFakeRejects pins bad fake inputs fail loudly.
func checkFakeRejects(t *testing.T) {
	t.Helper()
	fake := clock.NewFake(0)
	if _, err := fake.AdvanceSim(-1); err == nil {
		t.Fatal("negative sim advance must fail")
	}
	if _, err := fake.AdvanceWall(-1); err == nil {
		t.Fatal("negative wall advance must fail")
	}
	for _, rate := range []float64{-1} {
		if err := fake.SetRate(rate); err == nil {
			t.Fatalf("rate %v must fail", rate)
		}
	}
	if err := fake.SetRate(0); err != nil {
		t.Fatalf("zero rate must pass: %v", err)
	}
	if fake.Rate() != 0 {
		t.Fatalf("rate = %v, want 0", fake.Rate())
	}
	got, err := fake.AdvanceWall(100)
	if err != nil {
		t.Fatalf("zero rate advance: %v", err)
	}
	if got != 0 {
		t.Fatalf("zero rate now = %d, want frozen 0", got)
	}
	if _, _, err := fake.ApplyTo(nil); !errors.Is(err, clock.ErrNilDomain) {
		t.Fatalf("nil apply = %v", err)
	}
}

// TestFakeMultiRate pins one wall driver across different rates.
func TestFakeMultiRate(t *testing.T) {
	t.Parallel()
	slow := clock.NewFake(0)
	fast := clock.NewFake(0)
	if err := fast.SetRate(2); err != nil {
		t.Fatalf("set rate: %v", err)
	}
	slowNow, err := slow.AdvanceWall(100)
	if err != nil {
		t.Fatalf("slow advance: %v", err)
	}
	fastNow, err := fast.AdvanceWall(100)
	if err != nil {
		t.Fatalf("fast advance: %v", err)
	}
	if slowNow != 100 || fastNow != 200 {
		t.Fatalf("slow = %d fast = %d, want 100 and 200", slowNow, fastNow)
	}
	if slow.Advances() != 1 || fast.Advances() != 1 {
		t.Fatal("advance counters must count")
	}
}

// chaosRig bundles a fake driven manual sim domain with its timers.
type chaosRig struct {
	// fake drives the domain sim time.
	fake *clock.Fake
	// domain is the manual sim domain under test.
	domain *clock.Domain
	// guard tracks cache validity across jumps.
	guard clock.CacheGuard
	// simTimer is the 100ms sim bound catch up timer.
	simTimer *clock.Timer
	// steadyTimer is the 1s steady bound timer.
	steadyTimer *clock.Timer
	// steadyNext is the steady schedule, untouched by sim jumps.
	steadyNext int64
}

// newChaosRig builds a synced fake driven rig with both timers armed.
func newChaosRig(t *testing.T) *chaosRig {
	t.Helper()
	rig := &chaosRig{fake: clock.NewFake(0)}
	steady := &manualTime{v: 7000000000}
	d, err := clock.NewDomain(clock.Options{
		WallFn:                     constFn(0),
		SteadyFn:                   steady.fn(),
		UseSimTime:                 true,
		JumpForwardThresholdNanos:  1000000000,
		JumpBackwardThresholdNanos: 1000,
	})
	if err != nil {
		t.Fatalf("new domain: %v", err)
	}
	rig.domain = d
	if _, jumped, err := rig.fake.ApplyTo(d); err != nil {
		t.Fatalf("sync sim: %v", err)
	} else if !jumped {
		t.Fatal("first sim input after fallback must fire")
	}
	rig.guard.Observe(d.Generation())
	rig.simTimer = mustTimer(t, d, clock.TimerOptions{Kind: clock.KindSim, Overload: clock.OverloadCatchUp, Autostart: true, PeriodNanos: 100000000})
	rig.steadyTimer = mustTimer(t, d, clock.TimerOptions{Overload: clock.OverloadCatchUp, Autostart: true, PeriodNanos: 1000000000})
	rig.steadyNext = rig.steadyTimer.NextCall()
	return rig
}

// closeChaosRig closes both rig timers, reporting errors.
func closeChaosRig(t *testing.T, rig *chaosRig) {
	t.Helper()
	if err := rig.simTimer.Close(); err != nil {
		t.Errorf("close sim timer: %v", err)
	}
	if err := rig.steadyTimer.Close(); err != nil {
		t.Errorf("close steady timer: %v", err)
	}
}

// TestBagLoopChaos pins a deterministic bag loop driving the domain.
func TestBagLoopChaos(t *testing.T) {
	t.Parallel()
	rig := newChaosRig(t)
	defer closeChaosRig(t, rig)
	checkSteadyDriveDrain(t, rig)
	checkBagLoopRebase(t, rig)
	checkResumePauseHold(t, rig)
}

// checkSteadyDriveDrain pins quiet advance plus exact catch up drain.
func checkSteadyDriveDrain(t *testing.T, rig *chaosRig) {
	t.Helper()
	driveMillis(t, rig.fake, rig.domain, 55, 10)
	if rig.simTimer.Fires() != 0 {
		t.Fatalf("fires = %d before drain, want 0", rig.simTimer.Fires())
	}
	drainTimer(t, rig.simTimer, 5)
	if rig.simTimer.Dropped() != 0 {
		t.Fatalf("dropped = %d, want catch up without drops", rig.simTimer.Dropped())
	}
}

// checkBagLoopRebase pins the loop invalidates caches and re-arms sim.
func checkBagLoopRebase(t *testing.T, rig *chaosRig) {
	t.Helper()
	rig.fake.ProgramJump(600000000, 100000000)
	loopTo(t, rig.fake, rig.domain, 50000000)
	if !rig.guard.Stale(rig.domain.Generation()) {
		t.Fatal("bag loop must invalidate caches")
	}
	if rig.simTimer.NextCall() != 200000000 {
		t.Fatalf("looped next = %d, want now plus period 200ms", rig.simTimer.NextCall())
	}
	if rig.simTimer.IsReady() {
		t.Fatal("rebased timer must not stall ready")
	}
	if rig.steadyTimer.NextCall() != rig.steadyNext {
		t.Fatal("steady timer must survive the sim loop untouched")
	}
}

// checkResumePauseHold pins warm resume, exact totals, and held pauses.
func checkResumePauseHold(t *testing.T, rig *chaosRig) {
	t.Helper()
	rig.guard.Observe(rig.domain.Generation())
	driveMillis(t, rig.fake, rig.domain, 10, 10)
	drainTimer(t, rig.simTimer, 1)
	if rig.simTimer.Fires() != 6 {
		t.Fatalf("fires = %d, want 6 total", rig.simTimer.Fires())
	}
	if rig.guard.Stale(rig.domain.Generation()) {
		t.Fatal("steady advance must keep caches warm")
	}
	rig.fake.Pause()
	held, err := rig.fake.AdvanceWall(1000000000)
	if err != nil {
		t.Fatalf("paused advance: %v", err)
	}
	if held != 200000000 {
		t.Fatalf("paused now = %d, want held 200ms", held)
	}
	ev, jumped, err := rig.fake.ApplyTo(rig.domain)
	if err != nil {
		t.Fatalf("apply paused: %v", err)
	}
	if jumped {
		t.Fatalf("held time must not jump, got %+v", ev)
	}
}

// driveMillis advances the fake in count steps and applies each to the domain.
func driveMillis(t *testing.T, fake *clock.Fake, d *clock.Domain, count, step int64) {
	t.Helper()
	for range count {
		now, err := fake.AdvanceSim(step * 1000000)
		if err != nil {
			t.Fatalf("advance to %d: %v", now, err)
		}
		_, jumped, err := fake.ApplyTo(d)
		if err != nil {
			t.Fatalf("apply %d: %v", now, err)
		}
		if jumped {
			t.Fatalf("steady drive must not jump at %d", now)
		}
	}
}

// loopTo advances once into a programmed loop and pins the backward jump.
func loopTo(t *testing.T, fake *clock.Fake, d *clock.Domain, step int64) {
	t.Helper()
	now, err := fake.AdvanceSim(step)
	if err != nil {
		t.Fatalf("loop advance: %v", err)
	}
	if now != 100000000 {
		t.Fatalf("loop now = %d, want loop target 100ms", now)
	}
	ev, jumped, err := fake.ApplyTo(d)
	if err != nil {
		t.Fatalf("apply loop: %v", err)
	}
	if !jumped || !ev.Backward {
		t.Fatalf("loop must fire backward, got %+v %t", ev, jumped)
	}
	if fake.JumpsFired() != 1 {
		t.Fatalf("jumps fired = %d, want 1", fake.JumpsFired())
	}
}

// drainTimer calls until unready and pins the served count.
func drainTimer(t *testing.T, tm *clock.Timer, want int) {
	t.Helper()
	got := 0
	for tm.IsReady() {
		if _, err := tm.Call(); err != nil {
			t.Fatalf("drain call: %v", err)
		}
		got++
		if got > want+10 {
			t.Fatalf("drain runaway, want %d", want)
		}
	}
	if got != want {
		t.Fatalf("drained = %d, want %d", got, want)
	}
}

// TestDomainConcurrent pins concurrent reads, polls, and registry churn.
func TestDomainConcurrent(t *testing.T) {
	t.Parallel()
	d, err := clock.NewDomain(clock.Options{
		WallFn:                     constFn(1000),
		SteadyFn:                   constFn(2000),
		JumpForwardThresholdNanos:  100,
		JumpBackwardThresholdNanos: 10,
	})
	if err != nil {
		t.Fatalf("new domain: %v", err)
	}
	var wg sync.WaitGroup
	var fired atomic.Uint64
	probe := func(clock.JumpEvent) {
		fired.Add(1)
	}
	for range 4 {
		wg.Go(func() {
			hammerDomain(t, d, probe)
		})
	}
	tm, err := clock.NewTimer(d, clock.TimerOptions{Overload: clock.OverloadCatchUp, Autostart: true}, func(clock.FireContext) {
		fired.Add(1)
	})
	if err != nil {
		t.Fatalf("new timer: %v", err)
	}
	for range 4 {
		wg.Go(func() {
			hammerTimer(t, tm)
		})
	}
	wg.Wait()
	if err := tm.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if tm.Fires() == 0 {
		t.Fatal("concurrent zero period calls must fire")
	}
}

// hammerDomain churns reads, polls, and registry add plus remove.
func hammerDomain(t *testing.T, d *clock.Domain, probe clock.JumpCallback) {
	t.Helper()
	for range 50 {
		_ = d.Now()
		_, _ = d.Poll()
		id, err := d.AddJumpCallback(probe)
		if err != nil {
			t.Errorf("add callback: %v", err)
			return
		}
		if err := d.RemoveJumpCallback(id); err != nil {
			t.Errorf("remove callback: %v", err)
			return
		}
	}
}

// hammerTimer hammers zero period calls plus state reads.
func hammerTimer(t *testing.T, tm *clock.Timer) {
	t.Helper()
	for range 50 {
		_, err := tm.Call()
		if err != nil {
			t.Errorf("concurrent call: %v", err)
			return
		}
		_ = tm.IsReady()
		_ = tm.Backlog()
	}
}
