// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package sdk

import (
	"errors"
	"fmt"
	"sort"
	"sync"
)

// Domain names the time source of one clock context. Each context owns
// exactly one domain; wall and sim must agree between peers.
type Domain int

const (
	// DomainWall is wall time.
	DomainWall Domain = iota
	// DomainSim is simulated time from the clock topic.
	DomainSim
	// DomainSteady is monotonic time for local measurement.
	DomainSteady
)

// String renders the domain in lowercase form.
func (d Domain) String() string {
	switch d {
	case DomainWall:
		return "wall"
	case DomainSim:
		return "sim"
	case DomainSteady:
		return "steady"
	default:
		return "invalid"
	}
}

// isValidDomain reports whether d is a concrete domain.
func isValidDomain(d Domain) bool {
	return d == DomainWall || d == DomainSim || d == DomainSteady
}

// Jump describes one observed time step.
type Jump struct {
	// FromNanos is the previous tick.
	FromNanos int64
	// ToNanos is the new tick.
	ToNanos int64
	// Backward is true when the new tick precedes the old one.
	Backward bool
	// ActivationChanged is true when the domain moved to or from sim.
	ActivationChanged bool
	// SimActive reports whether sim is active after the step.
	SimActive bool
}

// JumpCallback observes one clock jump. Callbacks run in registration
// order without holding the clock lock.
type JumpCallback func(Jump)

// Clock is one context clock with wall, steady, and sim sources.
//
// REVIEW: overload policy and timer binding are deferred; the clock only
// serves ticks, jump callbacks, and peer agreement. Tests drive it with
// SetWallTime, SetSteadyTime, and SetSimTime instead of wall reads.
type Clock struct {
	// mu guards all fields below.
	mu sync.Mutex
	// domain selects the active source.
	domain Domain
	// simNanos is the last sim tick.
	simNanos int64
	// simValid reports whether a sim sample arrived.
	simValid bool
	// wallNanos is the wall source tick.
	wallNanos int64
	// steadyNanos is the steady source tick.
	steadyNanos int64
	// callbacks holds ordered jump observers by id.
	callbacks map[uint64]JumpCallback
	// nextID numbers callback ids.
	nextID uint64
}

// NewClock returns a clock with explicit wall and steady fallbacks.
//
// An invalid domain normalizes to wall; negative ticks clamp to
// zero. Sim ticks arrive through SetSimTime.
func NewClock(domain Domain, wallNanos, steadyNanos int64) *Clock {
	if !isValidDomain(domain) {
		domain = DomainWall
	}
	if wallNanos < 0 {
		wallNanos = 0
	}
	if steadyNanos < 0 {
		steadyNanos = 0
	}
	return &Clock{
		domain:      domain,
		wallNanos:   wallNanos,
		steadyNanos: steadyNanos,
		callbacks:   make(map[uint64]JumpCallback),
	}
}

// Domain returns the active clock domain.
func (d *Clock) Domain() Domain {
	if d == nil {
		return DomainWall
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.domain
}

// UseSimTime reports whether sim time is active.
func (d *Clock) UseSimTime() bool {
	if d == nil {
		return false
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.domain == DomainSim
}

// HasSimTime reports whether a sim sample arrived.
func (d *Clock) HasSimTime() bool {
	if d == nil {
		return false
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.simValid
}

// UsingFallback reports whether Now returns the wall fallback while the
// domain is sim but no sim sample arrived.
func (d *Clock) UsingFallback() bool {
	if d == nil {
		return false
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.domain == DomainSim && !d.simValid
}

// WallNow returns the wall source tick.
func (d *Clock) WallNow() int64 {
	if d == nil {
		return 0
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.wallNanos
}

// SteadyNow returns the steady source tick.
func (d *Clock) SteadyNow() int64 {
	if d == nil {
		return 0
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.steadyNanos
}

// SimNow returns the sim source tick plus validity.
func (d *Clock) SimNow() (int64, bool) {
	if d == nil {
		return 0, false
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.simNanos, d.simValid
}

// Now returns the active tick: sim when valid, else steady, else wall.
func (d *Clock) Now() int64 {
	if d == nil {
		return 0
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.nowLocked()
}

// nowLocked returns the active tick assuming the lock is held.
func (d *Clock) nowLocked() int64 {
	if d.domain == DomainSim {
		if d.simValid {
			return d.simNanos
		}
		return d.wallNanos
	}
	if d.domain == DomainSteady {
		return d.steadyNanos
	}
	return d.wallNanos
}

// SetWallTime replaces the wall source tick for deterministic tests.
func (d *Clock) SetWallTime(nanos int64) {
	if d == nil {
		return
	}
	if nanos < 0 {
		nanos = 0
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	d.wallNanos = nanos
}

// SetSteadyTime replaces the steady source tick for deterministic tests.
func (d *Clock) SetSteadyTime(nanos int64) {
	if d == nil {
		return
	}
	if nanos < 0 {
		nanos = 0
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	d.steadyNanos = nanos
}

// AdvanceWall moves the wall source forward by delta with saturation.
func (d *Clock) AdvanceWall(delta int64) {
	if d == nil {
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	d.wallNanos = addSaturating(d.wallNanos, delta)
	if d.wallNanos < 0 {
		d.wallNanos = 0
	}
}

// AdvanceSteady moves the steady source forward by delta with saturation.
func (d *Clock) AdvanceSteady(delta int64) {
	if d == nil {
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	d.steadyNanos = addSaturating(d.steadyNanos, delta)
	if d.steadyNanos < 0 {
		d.steadyNanos = 0
	}
}

// SetSimTime records one sim tick and notifies callbacks in order.
func (d *Clock) SetSimTime(nanos int64) Jump {
	if d == nil {
		return Jump{}
	}
	if nanos < 0 {
		nanos = 0
	}
	d.mu.Lock()
	from := d.simNanos
	wasValid := d.simValid
	d.simNanos = nanos
	d.simValid = true
	jump := Jump{FromNanos: from, ToNanos: nanos, SimActive: d.domain == DomainSim}
	if wasValid && nanos < from {
		jump.Backward = true
	}
	cbs := d.snapshotCallbacksLocked()
	d.mu.Unlock()
	d.invokeCallbacks(cbs, jump)
	return jump
}

// SetDomain switches the active domain with jump notification.
func (d *Clock) SetDomain(domain Domain) (Jump, error) {
	if d == nil {
		return Jump{}, errors.New("sdk: set domain on nil clock") //nolint:wrapcheck // new error, nothing to wrap
	}
	if !isValidDomain(domain) {
		return Jump{}, fmt.Errorf("sdk: set domain: invalid clock domain %d", int(domain))
	}
	d.mu.Lock()
	if d.domain == domain {
		now := d.nowLocked()
		active := d.domain == DomainSim
		d.mu.Unlock()
		return Jump{FromNanos: now, ToNanos: now, SimActive: active}, nil
	}
	from := d.nowLocked()
	wasSim := d.domain == DomainSim
	isSim := domain == DomainSim
	d.domain = domain
	to := d.nowLocked()
	jump := Jump{FromNanos: from, ToNanos: to, SimActive: isSim}
	if wasSim != isSim {
		jump.ActivationChanged = true
	}
	if to < from {
		jump.Backward = true
	}
	cbs := d.snapshotCallbacksLocked()
	d.mu.Unlock()
	d.invokeCallbacks(cbs, jump)
	return jump, nil
}

// SetUseSimTime enables or disables sim time with jump notification.
func (d *Clock) SetUseSimTime(enabled bool) Jump {
	if d == nil {
		return Jump{SimActive: enabled}
	}
	want := DomainWall
	if enabled {
		want = DomainSim
	}
	jump, err := d.SetDomain(want)
	if err != nil {
		return Jump{SimActive: enabled}
	}
	return jump
}

// snapshotCallbacksLocked copies callbacks in id order assuming lock held.
func (d *Clock) snapshotCallbacksLocked() []JumpCallback {
	if len(d.callbacks) == 0 {
		return nil
	}
	ids := make([]uint64, 0, len(d.callbacks))
	for id := range d.callbacks {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	out := make([]JumpCallback, 0, len(ids))
	for _, id := range ids {
		out = append(out, d.callbacks[id])
	}
	return out
}

// invokeCallbacks runs callbacks without holding the clock lock.
func (d *Clock) invokeCallbacks(cbs []JumpCallback, jump Jump) {
	for _, cb := range cbs {
		if cb == nil {
			continue
		}
		cb(jump)
	}
}

// RegisterJumpCallback adds one ordered jump observer.
func (d *Clock) RegisterJumpCallback(cb JumpCallback) (uint64, error) {
	if d == nil {
		return 0, errors.New("sdk: register jump callback on nil clock") //nolint:wrapcheck // new error, nothing to wrap
	}
	if cb == nil {
		return 0, errors.New("sdk: register jump callback: callback is nil") //nolint:wrapcheck // new error, nothing to wrap
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	d.nextID++
	id := d.nextID
	d.callbacks[id] = cb
	return id, nil
}

// UnregisterJumpCallback removes one jump observer by id. Unknown ids are
// a no op so double deregistration during teardown stays quiet.
func (d *Clock) UnregisterJumpCallback(id uint64) {
	if d == nil {
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	delete(d.callbacks, id)
}

// CheckPeerAgreement enforces sim agreement with one peer domain.
func (d *Clock) CheckPeerAgreement(peer Domain) error {
	if d == nil {
		return errors.New("sdk: peer agreement on nil clock") //nolint:wrapcheck // new error, nothing to wrap
	}
	d.mu.Lock()
	local := d.domain
	d.mu.Unlock()
	if local == peer {
		return nil
	}
	return fmt.Errorf("sdk: clock: domain mismatch: local %s remote %s", local.String(), peer.String())
}

// addSaturating adds two nanos with saturation at the int64 range.
func addSaturating(a, b int64) int64 {
	const maxInt64 = int64(^uint64(0) >> 1)
	const minInt64 = -maxInt64 - 1
	if b > 0 && a > maxInt64-b {
		return maxInt64
	}
	if b < 0 && a < minInt64-b {
		return minInt64
	}
	return a + b
}

// FakeClock is a deterministic manual clock for chaos tests.
//
// REVIEW: rate scaling and programmed jumps are deferred; Set teleports
// and Advance moves forward only. One rate of one keeps tests exact.
type FakeClock struct {
	// mu guards all fields below.
	mu sync.Mutex
	// now is the current fake time in nanoseconds.
	now int64
	// paused freezes advances until Resume.
	paused bool
	// advances counts Advance calls including paused ones.
	advances uint64
}

// NewFakeClock returns a running fake at startNanos.
func NewFakeClock(startNanos int64) *FakeClock {
	if startNanos < 0 {
		startNanos = 0
	}
	return &FakeClock{now: startNanos}
}

// Now returns the current fake time in nanoseconds.
func (f *FakeClock) Now() int64 {
	if f == nil {
		return 0
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.now
}

// NowFunc returns a source function reading this fake.
func (f *FakeClock) NowFunc() func() int64 {
	return f.Now
}

// Set teleports fake time without processing triggers.
func (f *FakeClock) Set(nanos int64) {
	if f == nil {
		return
	}
	if nanos < 0 {
		nanos = 0
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.now = nanos
}

// Advance moves fake time forward by delta. Negative deltas are rejected;
// backward motion belongs to Set.
func (f *FakeClock) Advance(delta int64) (int64, error) {
	if f == nil {
		return 0, errors.New("sdk: advance nil fake clock") //nolint:wrapcheck // new error, nothing to wrap
	}
	if delta < 0 {
		return 0, fmt.Errorf("sdk: advance fake clock %d: negative deltas are rejected; use Set", delta)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.advances++
	if f.paused {
		return f.now, nil
	}
	f.now = addSaturating(f.now, delta)
	return f.now, nil
}

// Pause freezes advances until Resume.
func (f *FakeClock) Pause() {
	if f == nil {
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.paused = true
}

// Resume unfreezes advances.
func (f *FakeClock) Resume() {
	if f == nil {
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.paused = false
}

// Paused reports whether advances are frozen.
func (f *FakeClock) Paused() bool {
	if f == nil {
		return false
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.paused
}

// Advances counts Advance calls including paused ones.
func (f *FakeClock) Advances() uint64 {
	if f == nil {
		return 0
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.advances
}
