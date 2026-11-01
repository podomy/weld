// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package core

import (
	"errors"
	"fmt"
	"math"
	"slices"
	"sync"
	"time"
)

// Time as one domain per context.
//
// Each context owns exactly one ClockState wrapping one ClockDomain value
// defined in action.go; there is no shared global state. The state carries
// explicit wall, steady, and sim sources plus a construction time wall
// fallback that is never silent: when the domain is sim but no sim sample
// has arrived, Now returns the fallback wall and UsingFallback reports true.
// Sim second 1234 means tick 1234000000000 on every peer: JoinNanos and
// SplitNanos convert without float, so Weld and rclcpp agree bit for bit.
// Jump callbacks are ordered by registration id and invoked without holding
// the state lock, so TF caches can clear without deadlock. Overload policy
// is explicit: ShouldPublishClock gates /clock at ClockPublishPeriodNanos.
// Deterministic tests drive the state with SetWallTime, SetSteadyTime, and
// SetSimTime instead of wall reads. Sim agreement reuses ValidateGoalClock
// for loud mismatch errors, and deadline rebase drives DeadlineTracker.
//
// Layer boundary: internal/time holds the generic Domain, Timer, and Fake
// clock with no Weld dependencies and never imports internal/core. This
// file holds Weld wiring only: ClockState wrapping ClockDomain, sim
// agreement, deadline rebase, and TF jump handling. Shared nanos math in
// this package lives in tfAddSaturating defined here; TF callers use that
// single definition and define no local copy.

// ClockTopic carries sim time for nodes with sim time enabled.
const ClockTopic = "/clock"

// ClockHistoryDepth is the wire depth for the clock topic.
//
// One sample suffices because only the newest tick matters.
const ClockHistoryDepth uint64 = 1

// ClockPublishPeriodNanos spaces /clock ticks at 40 Hz.
//
// Forty ticks per second equals one tick per 25 ms, which matches bag replay
// publishing at 40 Hz or on topic.
const ClockPublishPeriodNanos int64 = 25000000

// ClockQoS returns the profile Weld uses for /clock.
//
// Keep last depth one with best effort volatile matches the rclcpp clock
// QoS: newest tick wins, late joiners take one sample, loss is acceptable
// because the next tick follows within one period.
func ClockQoS() QoSProfile {
	return QoSProfile{
		History:     HistoryKeepLast,
		Depth:       ClockHistoryDepth,
		Reliability: ReliabilityBestEffort,
		Durability:  DurabilityVolatile,
		Liveliness:  LivelinessAutomatic,
	}
}

// SimSecondToNanos converts whole sim seconds to nanoseconds exactly.
//
// Sim second 1234 returns 1234000000000, the same tick in Weld and in
// rclcpp, because the conversion uses integer multiply with no float.
// Totals beyond max int64 saturate to max int64. Negative input clamps to
// zero because clock ticks are never negative.
func SimSecondToNanos(sec int64) int64 {
	if sec <= 0 {
		return 0
	}
	if sec > maxSeconds {
		return math.MaxInt64
	}
	return sec * nanosPerSecond
}

// SplitNanos splits a tick into seconds plus nanoseconds for /clock.
//
// The pair round trips through JoinNanos exactly, so 1234000000000 splits to
// 1234 seconds plus zero nanos on every peer. Negative input clamps to zero
// because ticks are never negative.
func SplitNanos(nanos int64) (int32, uint32) {
	if nanos <= 0 {
		return 0, 0
	}
	sec := nanos / nanosPerSecond
	rem := nanos % nanosPerSecond
	if sec > math.MaxInt32 {
		return math.MaxInt32, uint32(nanosPerSecond - 1)
	}
	return int32(sec), uint32(rem)
}

// JoinNanos joins seconds plus nanoseconds into one tick exactly.
//
// It inverts SplitNanos: JoinNanos(1234, 0) returns 1234000000000 on every
// peer. Values beyond max int64 saturate to max int64. Negative seconds
// and nanoseconds at or above one second are rejected as malformed instead
// of carrying silently.
func JoinNanos(sec int32, nsec uint32) (int64, error) {
	if sec < 0 {
		return 0, fmt.Errorf("core: join nanos: negative seconds %d", sec)
	}
	if nsec >= nanosPerSecond {
		return 0, fmt.Errorf("core: join nanos: nanoseconds %d above one second", nsec)
	}
	total := int64(sec)*nanosPerSecond + int64(nsec)
	if total < 0 {
		return math.MaxInt64, nil
	}
	return total, nil
}

// MustJoinNanos joins seconds plus nanoseconds and panics on malformed input.
//
// It exists for tests and static table setup where inputs are known valid.
// Production code calls JoinNanos and handles the error.
func MustJoinNanos(sec int32, nsec uint32) int64 {
	total, err := JoinNanos(sec, nsec)
	if err != nil {
		panic(err.Error())
	}
	return total
}

// ClockJump describes one observed time step.
//
// FromNanos is the previous tick, ToNanos is the new tick. Backward is true
// when ToNanos precedes FromNanos. ActivationChanged is true when the domain
// moved to or from sim. SimActive reports whether sim is active after the
// step.
type ClockJump struct {
	FromNanos         int64
	ToNanos           int64
	Backward          bool
	ActivationChanged bool
	SimActive         bool
}

// JumpCallback observes one clock jump.
//
// Callbacks run in registration order without holding the state lock, so
// they must not call back into register or unregister to avoid races; queue
// the request and apply it after the jump returns. Callbacks should be
// allocation free on the jump path: registration may allocate once, but the
// jump path must not allocate per tick.
type JumpCallback func(ClockJump)

// ClockState is one context clock with wall, steady, and sim sources.
//
// It wraps one ClockDomain value from action.go so sim agreement reuses the
// same enum and ValidateGoalClock. The zero value is unusable because the
// callback map is nil; build states with NewClockState and do not copy a
// state after first use. All methods are safe for concurrent use.
type ClockState struct {
	mu          sync.Mutex
	domain      ClockDomain
	simNanos    int64
	simValid    bool
	wallNanos   int64
	steadyNanos int64
	callbacks   map[uint64]JumpCallback
	nextID      uint64
}

// NewClockState returns a state with explicit wall plus steady fallbacks.
// An invalid domain fails loudly with a panic because the domain selects the
// active source and silent fallback to wall would hide misconfiguration.
//
// The domain selects the active source: sim reads sim ticks when valid,
// steady reads the steady source, wall reads the wall source. When the
// domain is sim but no sim sample has arrived, Now returns wallNanos and
// UsingFallback reports true: the fallback is explicit, never silent.
func NewClockState(domain ClockDomain, wallNanos, steadyNanos int64) *ClockState {
	if !isValidClockDomain(domain) {
		panic("core: new clock state: invalid clock domain")
	}
	if wallNanos < 0 {
		wallNanos = 0
	}
	if steadyNanos < 0 {
		steadyNanos = 0
	}
	return &ClockState{
		domain:      domain,
		wallNanos:   wallNanos,
		steadyNanos: steadyNanos,
		callbacks:   make(map[uint64]JumpCallback),
	}
}

// Domain returns the active clock domain.
func (d *ClockState) Domain() ClockDomain {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.domain
}

// UseSimTime reports whether sim time is active.
func (d *ClockState) UseSimTime() bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.domain == ClockSim
}

// HasSimTime reports whether a sim sample has arrived.
func (d *ClockState) HasSimTime() bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.simValid
}

// UsingFallback reports whether Now currently returns the wall fallback
// while the domain is sim but no sim sample has arrived.
func (d *ClockState) UsingFallback() bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.domain == ClockSim && !d.simValid
}

// WallNow returns the wall source tick.
func (d *ClockState) WallNow() int64 {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.wallNanos
}

// SteadyNow returns the steady source tick.
func (d *ClockState) SteadyNow() int64 {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.steadyNanos
}

// SimNow returns the sim source tick plus validity.
func (d *ClockState) SimNow() (int64, bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.simNanos, d.simValid
}

// Now returns the active tick.
//
// Sim domain with valid sim returns sim, steady domain returns steady, else
// wall. Sim domain without valid sim returns the construction time wall
// fallback with UsingFallback true, so the fallback stays observable.
func (d *ClockState) Now() int64 {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.nowLocked()
}

// nowLocked returns the active tick assuming the lock is held.
func (d *ClockState) nowLocked() int64 {
	if d.domain == ClockSim {
		if d.simValid {
			return d.simNanos
		}
		return d.wallNanos
	}
	if d.domain == ClockSteady {
		return d.steadyNanos
	}
	return d.wallNanos
}

// SetWallTime replaces the wall source tick for deterministic tests.
func (d *ClockState) SetWallTime(nanos int64) {
	if nanos < 0 {
		nanos = 0
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	d.wallNanos = nanos
}

// SetSteadyTime replaces the steady source tick for deterministic tests.
func (d *ClockState) SetSteadyTime(nanos int64) {
	if nanos < 0 {
		nanos = 0
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	d.steadyNanos = nanos
}

// AdvanceWall moves the wall source forward by delta.
func (d *ClockState) AdvanceWall(delta int64) {
	d.mu.Lock()
	defer d.mu.Unlock()
	next := tfAddSaturating(d.wallNanos, delta)
	next = max(next, 0)
	d.wallNanos = next
}

// AdvanceSteady moves the steady source forward by delta.
func (d *ClockState) AdvanceSteady(delta int64) {
	d.mu.Lock()
	defer d.mu.Unlock()
	next := tfAddSaturating(d.steadyNanos, delta)
	next = max(next, 0)
	d.steadyNanos = next
}

// SetSimTime records one /clock tick and notifies callbacks in order.
//
// The jump compares against the previous sim tick when valid: a backward
// step sets Backward, which drives TF dynamic clears and deadline rebases
// through RebaseDeadlinesOnJump. SimActive reflects whether the domain is
// sim after the step. Callbacks run without holding the state lock.
func (d *ClockState) SetSimTime(nanos int64) ClockJump {
	if nanos < 0 {
		nanos = 0
	}
	d.mu.Lock()
	from := d.simNanos
	wasValid := d.simValid
	d.simNanos = nanos
	d.simValid = true
	jump := ClockJump{
		FromNanos: from,
		ToNanos:   nanos,
		SimActive: d.domain == ClockSim,
	}
	if wasValid && nanos < from {
		jump.Backward = true
	}
	cbs := d.snapshotCallbacksLocked()
	d.mu.Unlock()
	d.invokeCallbacks(cbs, jump)
	return jump
}

// SetDomain switches the active domain with jump notification.
//
// Moving to or from sim generates a jump with ActivationChanged set, which
// clears TF dynamics through HandleClockJump even when the tick does not
// move backward. Setting the same domain twice generates no jump and invokes
// no callbacks. An invalid domain is an error and changes nothing.
func (d *ClockState) SetDomain(domain ClockDomain) (ClockJump, error) {
	if !isValidClockDomain(domain) {
		return ClockJump{}, fmt.Errorf("core: set domain: invalid clock domain %d", int(domain))
	}
	d.mu.Lock()
	if d.domain == domain {
		now := d.nowLocked()
		active := d.domain == ClockSim
		d.mu.Unlock()
		return ClockJump{FromNanos: now, ToNanos: now, SimActive: active}, nil
	}
	from := d.nowLocked()
	wasSim := d.domain == ClockSim
	isSim := domain == ClockSim
	d.domain = domain
	to := d.nowLocked()
	jump := ClockJump{
		FromNanos: from,
		ToNanos:   to,
		SimActive: isSim,
	}
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
//
// True selects the sim domain, false selects the wall domain. It is a
// convenience wrapper around SetDomain for the common two domain case;
// steady domain users call SetDomain directly. The returned jump follows
// the SetDomain rules. It never fails because both targets are valid.
func (d *ClockState) SetUseSimTime(enabled bool) ClockJump {
	want := ClockWall
	if enabled {
		want = ClockSim
	}
	jump, err := d.SetDomain(want)
	if err != nil {
		return ClockJump{SimActive: enabled}
	}
	return jump
}

// snapshotCallbacksLocked copies callbacks in id order assuming lock held.
func (d *ClockState) snapshotCallbacksLocked() []JumpCallback {
	if len(d.callbacks) == 0 {
		return nil
	}
	ids := make([]uint64, 0, len(d.callbacks))
	for id := range d.callbacks {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	out := make([]JumpCallback, 0, len(ids))
	for _, id := range ids {
		out = append(out, d.callbacks[id])
	}
	return out
}

// invokeCallbacks runs callbacks without holding the state lock.
func (d *ClockState) invokeCallbacks(cbs []JumpCallback, jump ClockJump) {
	for _, cb := range cbs {
		if cb == nil {
			continue
		}
		cb(jump)
	}
}

// RegisterJumpCallback adds one ordered jump observer.
//
// Ids increase monotonically so invocation order equals registration order.
// A nil callback is an error.
func (d *ClockState) RegisterJumpCallback(cb JumpCallback) (uint64, error) {
	if cb == nil {
		return 0, errors.New("core: register jump callback: callback is nil") //nolint:wrapcheck // new error, nothing to wrap
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	d.nextID++
	id := d.nextID
	d.callbacks[id] = cb
	return id, nil
}

// UnregisterJumpCallback removes one jump observer by id.
//
// Unknown ids are a no op so double deregistration during teardown stays
// quiet. Survivors keep their relative order.
func (d *ClockState) UnregisterJumpCallback(id uint64) {
	d.mu.Lock()
	defer d.mu.Unlock()
	delete(d.callbacks, id)
}

// CheckPeerAgreement enforces sim agreement with one peer domain.
//
// Matching domains return nil. A mismatch returns the loud ValidateGoalClock
// style error through CheckUseSimTimeAgreement on the sim flags, so action
// clients and bag replay surface the disagreement instead of freezing.
func (d *ClockState) CheckPeerAgreement(peer ClockDomain) error {
	d.mu.Lock()
	local := d.domain
	d.mu.Unlock()
	localSim := local == ClockSim
	peerSim := peer == ClockSim
	if localSim == peerSim && local == peer {
		return nil
	}
	// Steady versus wall is also a disagreement: domains must match exactly.
	// Report through the sim flag error when sim is involved, else a direct
	// domain mismatch so every disagreement is loud.
	if localSim != peerSim {
		return CheckUseSimTimeAgreement(localSim, peerSim)
	}
	return fmt.Errorf("core: clock: domain mismatch: local %s remote %s", local.String(), peer.String())
}

// CheckUseSimTimeAgreement enforces agreement between two sim flags.
//
// Matching flags return nil; a mismatch returns a loud error carrying both
// values for logs, metrics, and CLI.
func CheckUseSimTimeAgreement(localUseSim, remoteUseSim bool) error {
	if localUseSim == remoteUseSim {
		return nil
	}
	return fmt.Errorf("core: clock: use_sim_time mismatch: local %t remote %t", localUseSim, remoteUseSim)
}

// CheckGoal validates one goal stamp against the active domain.
//
// It delegates to ValidateGoalClock so wall stamps against sim, sim stamps
// against wall, and steady mismatches all surface as ClockMismatchError with
// server, goal, stamp, and both domains, never freezing silently.
func (d *ClockState) CheckGoal(server, goalID string, stampNanos int64, goalDomain ClockDomain) error {
	d.mu.Lock()
	serverDomain := d.domain
	d.mu.Unlock()
	err := ValidateGoalClock(server, goalID, stampNanos, goalDomain, serverDomain)
	if err != nil {
		return fmt.Errorf("core: clock state: %w", err)
	}
	return nil
}

// RebaseDeadlinesOnJump rebases deadline trackers after a backward jump and
// returns any alerts that fire during the rebase.
//
// DeadlineTracker.Check already rebases on backward time by resetting to
// now, so this helper drives each tracker with the jump target when the
// jump is backward or carries an activation change. Forward jumps leave
// trackers untouched. Nil trackers are skipped. Alerts are propagated to the
// caller instead of discarded so monitoring stays loud.
func RebaseDeadlinesOnJump(jump ClockJump, trackers ...*DeadlineTracker) []DeadlineAlert {
	if !jump.Backward && !jump.ActivationChanged {
		return nil
	}
	var out []DeadlineAlert
	for _, tracker := range trackers {
		if tracker == nil {
			continue
		}
		alert, ok := tracker.Check(jump.ToNanos)
		if ok {
			out = append(out, alert)
		}
	}
	return out
}

// ShouldPublishClock reports whether a /clock tick is due.
//
// It returns true when now minus last reaches ClockPublishPeriodNanos,
// pinning 40 Hz publishing. Backward time returns false so a bag loop does
// not burst ticks.
func ShouldPublishClock(lastPublishNanos, nowNanos int64) bool {
	if nowNanos < lastPublishNanos {
		return false
	}
	return nowNanos-lastPublishNanos >= ClockPublishPeriodNanos
}

// ClockMessageNow returns the current tick split for /clock publishing.
//
// It reads Now and splits without float, so sim second 1234 publishes as
// 1234 seconds plus zero nanos on every peer.
func (d *ClockState) ClockMessageNow() (int32, uint32) {
	return SplitNanos(d.Now())
}

// SleepWall pauses the caller on wall time between TF polls.
//
// It exists so LookupWithTimeout can poll without spinning while timeout
// accounting stays on the buffer clock. Nonpositive durations return
// immediately.
func (d *ClockState) SleepWall(nanos int64) {
	if nanos <= 0 {
		return
	}
	time.Sleep(time.Duration(nanos))
}

// tfAddSaturating adds two nanos with saturation at the int64 range.
//
// Positive overflow saturates to max int64 and negative overflow saturates
// to min int64. It is the single shared helper for clock advances and TF
// deadlines in this package.
func tfAddSaturating(a, b int64) int64 {
	if b > 0 && a > math.MaxInt64-b {
		return math.MaxInt64
	}
	if b < 0 && a < math.MinInt64-b {
		return math.MinInt64
	}
	return a + b
}
