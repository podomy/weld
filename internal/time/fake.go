// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package clock

import (
	"fmt"
	"math"
	"slices"
	"sync"
)

// ProgrammedJump is one one-shot fake time jump.
//
// When a forward advance reaches or passes AtNanos, fake time teleports
// to ToNanos and the program is consumed. Backward ToNanos values model
// bag loops; forward values model leaps. The advance stops at the jump
// and discards its remainder so each crossing stays deterministic.
type ProgrammedJump struct {
	// AtNanos arms the jump when an advance reaches this time.
	AtNanos int64
	// ToNanos is the fake time after the jump.
	ToNanos int64
}

// Fake is a deterministic manual clock for chaos tests.
//
// It drives Domain sim time through NowFunc plus ApplyTo, covering bag
// loops with backward jumps, pauses, and multi rate sim where several
// fakes with different rates advance under one shared wall driver.
// Programmed triggers are one-shot and sorted; ties between a jump and
// a pause at the same time resolve to the jump first. Fake methods are
// safe for concurrent use. Do not copy a Fake after first use.
type Fake struct {
	// mu guards all fields below.
	mu sync.Mutex
	// now is the current fake time in nanoseconds.
	now int64
	// rate scales AdvanceWall steps. Zero freezes time.
	rate float64
	// paused freezes advances until Resume.
	paused bool
	// jumps holds pending one-shot jumps sorted by AtNanos.
	jumps []ProgrammedJump
	// pauses holds pending one-shot pause times sorted ascending.
	pauses []int64
	// jumpsFired counts consumed jumps.
	jumpsFired uint64
	// pausesHit counts consumed pause triggers.
	pausesHit uint64
	// advances counts AdvanceWall plus AdvanceSim calls.
	advances uint64
}

// NewFake builds a running fake at startNanos with rate one.
func NewFake(startNanos int64) *Fake {
	return &Fake{now: startNanos, rate: 1}
}

// Now returns the current fake time in nanoseconds.
func (f *Fake) Now() int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.now
}

// NowFunc returns a source function reading this fake for Options.SimFn.
func (f *Fake) NowFunc() func() int64 {
	return f.Now
}

// Set teleports fake time without processing triggers.
//
// Teleports model bag seeks and loop points; crossing triggers belong
// to AdvanceWall and AdvanceSim. Stale triggers at or behind the new
// time prune on the next advance.
func (f *Fake) Set(nanos int64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.now = nanos
}

// SetRate replaces the AdvanceWall rate multiplier.
//
// NaN, infinite, and negative rates are rejected; negative motion
// belongs to Set and programmed backward jumps. Zero freezes advances
// while staying distinct from Pause.
func (f *Fake) SetRate(rate float64) error {
	if math.IsNaN(rate) || math.IsInf(rate, 0) {
		return fmt.Errorf("clock: set fake rate %v: rate must be finite", rate)
	}
	if rate < 0 {
		return fmt.Errorf("clock: set fake rate %v: negative rates are rejected; program a backward jump instead", rate)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.rate = rate
	return nil
}

// Rate returns the AdvanceWall rate multiplier.
func (f *Fake) Rate() float64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.rate
}

// Pause freezes advances until Resume. Paused advances count but do not move.
func (f *Fake) Pause() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.paused = true
}

// Resume unfreezes advances.
func (f *Fake) Resume() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.paused = false
}

// Paused reports whether advances are frozen.
func (f *Fake) Paused() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.paused
}

// ProgramJump arms a one-shot jump firing when an advance reaches atNanos.
func (f *Fake) ProgramJump(atNanos, toNanos int64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	at := 0
	for at < len(f.jumps) && f.jumps[at].AtNanos <= atNanos {
		at++
	}
	f.jumps = slices.Insert(f.jumps, at, ProgrammedJump{AtNanos: atNanos, ToNanos: toNanos})
}

// ProgramPause arms a one-shot pause firing when an advance reaches atNanos.
func (f *Fake) ProgramPause(atNanos int64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	at := 0
	for at < len(f.pauses) && f.pauses[at] <= atNanos {
		at++
	}
	f.pauses = slices.Insert(f.pauses, at, atNanos)
}

// PendingJumps returns the count of armed jumps.
func (f *Fake) PendingJumps() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.jumps)
}

// PendingPauses returns the count of armed pause triggers.
func (f *Fake) PendingPauses() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.pauses)
}

// JumpsFired counts consumed programmed jumps.
func (f *Fake) JumpsFired() uint64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.jumpsFired
}

// PausesHit counts consumed pause triggers.
func (f *Fake) PausesHit() uint64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.pausesHit
}

// Advances counts AdvanceWall plus AdvanceSim calls, including paused ones.
func (f *Fake) Advances() uint64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.advances
}

// ClearPrograms drops all pending jumps and pauses without firing.
func (f *Fake) ClearPrograms() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.jumps = nil
	f.pauses = nil
}

// AdvanceWall moves fake time by wallDeltaNanos scaled by the rate.
//
// Negative deltas are rejected; backward motion belongs to Set. Paused
// fakes stay put. At most one programmed trigger fires per call: jumps
// teleport and stop with the remainder discarded, pauses stop exactly
// at the trigger time and freeze the fake. It returns the new fake time.
func (f *Fake) AdvanceWall(wallDeltaNanos int64) (int64, error) {
	if wallDeltaNanos < 0 {
		return 0, fmt.Errorf("clock: advance wall %d: negative deltas are rejected; use Set for backward motion", wallDeltaNanos)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.advances++
	if f.paused {
		return f.now, nil
	}
	return f.stepLocked(scaleStep(wallDeltaNanos, f.rate)), nil
}

// AdvanceSim moves fake time by simDeltaNanos without rate scaling.
//
// It shares the trigger rules of AdvanceWall: negative deltas rejected,
// paused fakes stay put, at most one trigger per call. Multi rate sim
// comes from advancing several fakes with different rates under one
// shared wall driver. It returns the new fake time.
func (f *Fake) AdvanceSim(simDeltaNanos int64) (int64, error) {
	if simDeltaNanos < 0 {
		return 0, fmt.Errorf("clock: advance sim %d: negative deltas are rejected; use Set for backward motion", simDeltaNanos)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.advances++
	if f.paused {
		return f.now, nil
	}
	return f.stepLocked(simDeltaNanos), nil
}

// ApplyTo drives domain sim time from this fake and returns the Poll outcome.
//
// It errors on a nil domain and wraps the external sim source error when
// the domain refuses SetSimTime. Chaos harnesses call ApplyTo after every
// advance so bag loops, pauses, and rate changes reach domain callbacks.
func (f *Fake) ApplyTo(d *Domain) (JumpEvent, bool, error) {
	if d == nil {
		return JumpEvent{}, false, ErrNilDomain
	}
	ev, jumped, err := d.SetSimTime(f.Now())
	if err != nil {
		return JumpEvent{}, false, fmt.Errorf("clock: fake apply to domain: %w", err)
	}
	return ev, jumped, nil
}

// scaleStep returns wallDeltaNanos scaled by rate, clamped to the int64
// range. Raw float conversion of out of range values is implementation
// defined, so the clamp happens in float64 before converting.
func scaleStep(wallDeltaNanos int64, rate float64) int64 {
	scaled := rate * float64(wallDeltaNanos)
	if scaled >= 9223372036854775808.0 {
		return math.MaxInt64
	}
	if scaled <= -9223372036854775808.0 {
		return math.MinInt64
	}
	return int64(scaled)
}

// stepLocked moves forward by step, firing at most one trigger.
//
// The fake holds its lock; triggers must be pruned and sorted on entry.
// A jump teleports and stops, a pause stops exactly at its time and
// freezes, otherwise time lands on the target. Jump wins ties.
func (f *Fake) stepLocked(step int64) int64 {
	from := f.now
	target := saturatingAdd(from, step)
	f.pruneStaleLocked(from)
	if len(f.jumps) > 0 && f.jumps[0].AtNanos <= target {
		prog := f.jumps[0]
		f.jumps = slices.Delete(f.jumps, 0, 1)
		f.jumpsFired++
		f.now = prog.ToNanos
		return f.now
	}
	if len(f.pauses) > 0 && f.pauses[0] <= target {
		at := f.pauses[0]
		f.pauses = slices.Delete(f.pauses, 0, 1)
		f.pausesHit++
		f.paused = true
		f.now = at
		return f.now
	}
	f.now = target
	return f.now
}

// pruneStaleLocked drops triggers at or behind from.
//
// Teleports can strand triggers behind the new time; those crossings
// will never happen, so they prune instead of leaking. Pending counts
// expose the drop on the next advance.
func (f *Fake) pruneStaleLocked(from int64) {
	f.jumps = slices.DeleteFunc(f.jumps, func(p ProgrammedJump) bool {
		return p.AtNanos <= from
	})
	f.pauses = slices.DeleteFunc(f.pauses, func(at int64) bool {
		return at <= from
	})
}
