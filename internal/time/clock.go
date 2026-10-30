// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

// Package clock implements one generic clock domain per context.
//
// A Domain binds three explicit sources, wall plus steady plus sim, and
// serves one active timeline. With UseSimTime false the active timeline
// is wall. With UseSimTime true the active timeline is sim while a sim
// source is available, and a declared construction time fallback
// otherwise. The fallback is never silent: Fallback records the reason,
// FallbackCount counts every read it serves, FallbackActive reports the
// current state, and Describe includes the reason.
//
// Steady timers are the default: TimerOptions with an unspecified kind
// binds steady, which never follows jumps on other timelines. Per
// subsystem rate choices stay declared policy by binding each timer to
// an explicit kind instead of leaking one timeline into all users.
//
// Jump callbacks fire synchronously in registration order on every Poll,
// SetSimTime, and SetUseSimTime that observes a jump. Registration uses
// copy on write snapshots, so firing never allocates and callbacks may
// add or remove other callbacks safely. Callbacks must not drive time
// from inside the callback. Backward jumps and sim activation changes
// bump Generation so caches such as TF buffers invalidate; CacheGuard
// carries the last clean generation for that check.
//
// Overload policy is explicit: NewTimer rejects an unspecified policy,
// coalescing counts every dropped firing in Dropped, and catch up keeps
// missed firings visible in Backlog until drained. Timers never block:
// all state is atomic and firing holds no lock, so wall timers cannot
// hang pooled supervision under load. Do not copy a Domain after first
// use; share it by pointer, one per context.
//
// Layer boundary: this package holds the generic Domain, Timer, and Fake
// clock with no Weld dependencies and never imports internal/core. Weld
// wiring lives in internal/core/clock.go as ClockState wrapping
// ClockDomain with sim agreement, deadline rebase, and TF jump handling.
package clock

import (
	"errors"
	"fmt"
	"math"
	"sync/atomic"
	stdtime "time"
)

// Sentinel errors for terminal timer and registry states.
var (
	// ErrTimerCanceled reports a Call on a canceled timer.
	ErrTimerCanceled = errors.New("clock: timer is canceled")
	// ErrTimerNotReady reports a Call before the next call time.
	ErrTimerNotReady = errors.New("clock: timer is not ready")
	// ErrTimerClosed reports use of a Timer after Close.
	ErrTimerClosed = errors.New("clock: timer is closed")
	// ErrUnknownJumpCallback reports removal of an unknown callback id.
	ErrUnknownJumpCallback = errors.New("clock: unknown jump callback id")
	// ErrNilJumpCallback reports registration of a nil jump callback.
	ErrNilJumpCallback = errors.New("clock: jump callback is nil")
	// ErrNilTimerCallback reports a nil timer fire callback.
	ErrNilTimerCallback = errors.New("clock: timer callback is nil")
	// ErrNilDomain reports a nil Domain where one is required.
	ErrNilDomain = errors.New("clock: domain is nil")
	// ErrExternalSim reports SetSimTime on a domain with an external sim source.
	ErrExternalSim = errors.New("clock: sim source is external; drive it directly")
)

// Default jump thresholds applied when Options carries zeros.
//
// The forward default exceeds worst case spin gaps so normal scheduling
// jitter never fires, while staying far below the one second ROS
// default. The backward default is tight: any real backward step on a
// monotonic or sim timeline indicates a loop or NTP correction and must
// clear caches. Callers needing exact behavior pass explicit positive
// values; zero always means these defaults, and one nanosecond means
// fire on any motion.
const (
	// DefaultJumpForwardThresholdNanos fires forward jumps at 100 ms and above.
	DefaultJumpForwardThresholdNanos = int64(100000000)
	// DefaultJumpBackwardThresholdNanos fires backward jumps at 1 ms and above.
	DefaultJumpBackwardThresholdNanos = int64(1000000)
)

// DefaultFallbackKind serves reads while sim is requested but unavailable.
const DefaultFallbackKind = KindSteady

// SourceKind names one clock timeline in a domain.
type SourceKind int

// Clock timelines. KindUnspecified resolves to steady for timers and is
// rejected for fallback kinds; it always mismatches in CheckGoalStamp so
// stamp timelines stay explicit.
const (
	// KindUnspecified means no timeline was selected.
	KindUnspecified SourceKind = iota
	// KindWall is wall time, the active timeline without sim time.
	KindWall
	// KindSteady is monotonic time, the default timer timeline.
	KindSteady
	// KindSim is simulated time, active while UseSimTime holds.
	KindSim
)

// String renders the timeline in lowercase form.
func (k SourceKind) String() string {
	switch k {
	case KindUnspecified:
		return "unspecified"
	case KindWall:
		return "wall"
	case KindSteady:
		return "steady"
	case KindSim:
		return "sim"
	default:
		return fmt.Sprintf("SourceKind(%d)", int(k))
	}
}

// OverloadPolicy names the timer behavior when firings fall behind.
type OverloadPolicy int

// Timer overload policies. There is no default: NewTimer rejects
// OverloadInvalid so backlog handling stays declared policy, never an
// accident. Silent coalescing that hides backlog is forbidden; both
// policies keep it observable.
const (
	// OverloadInvalid means no policy was declared and is rejected.
	OverloadInvalid OverloadPolicy = iota
	// OverloadCoalesce skips to now plus period and counts skipped firings in Dropped.
	OverloadCoalesce
	// OverloadCatchUp advances one period per Call and keeps missed firings visible in Backlog.
	OverloadCatchUp
)

// String renders the policy in lowercase form.
func (p OverloadPolicy) String() string {
	switch p {
	case OverloadInvalid:
		return "invalid"
	case OverloadCoalesce:
		return "coalesce"
	case OverloadCatchUp:
		return "catch_up"
	default:
		return fmt.Sprintf("OverloadPolicy(%d)", int(p))
	}
}

// JumpEvent describes one detected time jump on the active timeline.
type JumpEvent struct {
	// Kind is the timeline that jumped.
	Kind SourceKind
	// FromNanos is the active reading before the jump.
	FromNanos int64
	// ToNanos is the active reading after the jump.
	ToNanos int64
	// DeltaNanos is ToNanos minus FromNanos.
	DeltaNanos int64
	// Forward is true for a forward jump past the threshold.
	Forward bool
	// Backward is true for a backward jump past the threshold.
	Backward bool
	// SimActivationChange is true when UseSimTime flipped. Activation
	// events always notify and always invalidate caches, even when the
	// two timelines read the same value.
	SimActivationChange bool
}

// String renders the event as one log line.
func (e JumpEvent) String() string {
	return fmt.Sprintf("clock: jump kind=%s from_ns=%d to_ns=%d delta_ns=%d forward=%t backward=%t activation=%t",
		e.Kind, e.FromNanos, e.ToNanos, e.DeltaNanos, e.Forward, e.Backward, e.SimActivationChange)
}

// JumpCallback observes time jumps in registration order.
//
// Callbacks run synchronously under Poll, SetSimTime, or SetUseSimTime.
// They must be short and must not panic, block, or drive domain time;
// driving time from inside a callback recurses into firing. Adding or
// removing callbacks from inside a callback is safe and affects the
// next firing, never the one in progress.
type JumpCallback func(ev JumpEvent)

// jumpRegistration is one ordered jump callback entry.
type jumpRegistration struct {
	// id is the registration id returned by AddJumpCallback.
	id uint64
	// fn is the callback to invoke.
	fn JumpCallback
}

// jumpRegistry is an immutable snapshot of jump callbacks in order.
//
// Registrations copy on write and swap the snapshot atomically, so the
// fire path loads one pointer and iterates with no lock and no
// allocation. Registration cost stays on the rare write path.
type jumpRegistry struct {
	// entries holds callbacks in registration order.
	entries []jumpRegistration
}

// FireContext carries one timer firing to the typed callback.
type FireContext struct {
	// ExpectedNanos is the scheduled call time.
	ExpectedNanos int64
	// ActualNanos is the clock reading at the call.
	ActualNanos int64
	// SinceLastNanos is ActualNanos minus the previous call time. It is
	// negative when a backward jump intervened since the last call.
	SinceLastNanos int64
	// Backlog is the remaining owed firings after this call.
	Backlog uint64
}

// TimerOptions declares one timer. Overload must be explicit; the zero
// Options value is rejected because OverloadInvalid is an error.
type TimerOptions struct {
	// Kind selects the timeline. KindUnspecified selects steady, the default.
	Kind SourceKind
	// PeriodNanos is the timer period. Zero means always ready.
	PeriodNanos int64
	// Overload selects coalesce or catch up. OverloadInvalid is rejected.
	Overload OverloadPolicy
	// Autostart arms the timer at construction when true.
	Autostart bool
}

// timerCallbacks holds the timer fire and reset callbacks.
//
// The pointer swaps atomically, so firing loads once and invokes with
// no lock and no per fire allocation. User payloads stay behind the
// closure by reference; the timer never deep copies callback data.
type timerCallbacks struct {
	// fire runs on every Call that passes readiness.
	fire func(FireContext)
	// reset runs on every Reset. Nil means no reset callback.
	reset func()
}

// Options declares one clock domain. The zero value is usable: real
// wall plus steady sources, wall active, default jump thresholds.
type Options struct {
	// WallFn reads wall time in nanoseconds. Nil selects the real wall clock.
	WallFn func() int64
	// SteadyFn reads monotonic time in nanoseconds. Nil selects a real
	// monotonic clock starting at zero at construction.
	SteadyFn func() int64
	// SimFn reads sim time in nanoseconds. Nil selects manual sim time
	// driven by SetSimTime, which starts unavailable until the first set.
	SimFn func() int64
	// UseSimTime selects sim as the active timeline while available.
	UseSimTime bool
	// JumpForwardThresholdNanos fires forward jumps at this delta and
	// above. Zero selects the default; negative is rejected.
	JumpForwardThresholdNanos int64
	// JumpBackwardThresholdNanos fires backward jumps at this magnitude
	// and above. Zero selects the default; negative is rejected.
	JumpBackwardThresholdNanos int64
	// FallbackKind serves reads while sim is requested but unavailable.
	// KindUnspecified selects steady. Wall is the only other valid value.
	FallbackKind SourceKind
}

// Domain is one clock domain: explicit wall, steady, and sim sources
// with one active timeline, ordered jump callbacks, and loud sim
// agreement checks. All methods are safe for concurrent use. Do not
// copy a Domain after first use.
type Domain struct {
	// wallFn reads wall nanoseconds.
	wallFn func() int64
	// steadyFn reads monotonic nanoseconds.
	steadyFn func() int64
	// simFn reads sim nanoseconds, or nil for manual sim.
	simFn func() int64
	// simManual holds manual sim time for SetSimTime domains.
	simManual atomic.Int64
	// simEverSet reports the first SetSimTime arrived.
	simEverSet atomic.Bool
	// useSimTime selects sim as the active timeline.
	useSimTime atomic.Bool
	// forwardThreshold fires forward jumps at this delta and above.
	forwardThreshold int64
	// backwardThreshold fires backward jumps at this magnitude and above.
	backwardThreshold int64
	// fallbackKind serves reads while sim is unavailable.
	fallbackKind SourceKind
	// fallbackReason records why construction fell back, empty when it did not.
	fallbackReason string
	// fallbackCount counts active reads served from fallback.
	fallbackCount atomic.Uint64
	// lastActive holds the last observed active reading for jump detection.
	lastActive atomic.Int64
	// generation bumps on every backward jump and activation change.
	generation atomic.Uint64
	// forwardCount counts fired forward jumps.
	forwardCount atomic.Uint64
	// backwardCount counts fired backward jumps.
	backwardCount atomic.Uint64
	// callbacks holds the copy on write jump registry snapshot.
	callbacks atomic.Pointer[jumpRegistry]
	// nextCallbackID issues jump callback ids from one upward.
	nextCallbackID atomic.Uint64
}

// realWallNow reads the real wall clock in nanoseconds.
func realWallNow() int64 {
	return stdtime.Now().UnixNano()
}

// makeSteadyNow returns a real monotonic clock starting at zero.
func makeSteadyNow() func() int64 {
	start := stdtime.Now()
	return func() int64 {
		return stdtime.Since(start).Nanoseconds()
	}
}

// saturatingAdd returns a plus b clamped to the int64 range.
func saturatingAdd(a, b int64) int64 {
	if b > 0 && a > math.MaxInt64-b {
		return math.MaxInt64
	}
	if b < 0 && a < math.MinInt64-b {
		return math.MinInt64
	}
	return a + b
}

// NewDomain builds one clock domain from opts.
//
// Nil wall and steady sources select real clocks, which are explicit
// defaults rather than a fallback. Nil sim selects manual sim time
// driven by SetSimTime. Requesting sim time without a sim source is not
// an error: construction falls back to the fallback kind and records
// the reason loudly in Fallback, so silence is impossible. The last
// observed reading seeds from the initial active read, which counts one
// fallback read when fallback is already active.
func NewDomain(opts Options) (*Domain, error) {
	forward := opts.JumpForwardThresholdNanos
	if forward == 0 {
		forward = DefaultJumpForwardThresholdNanos
	}
	if forward < 0 {
		return nil, fmt.Errorf("clock: new domain: negative forward jump threshold %d", forward)
	}
	backward := opts.JumpBackwardThresholdNanos
	if backward == 0 {
		backward = DefaultJumpBackwardThresholdNanos
	}
	if backward < 0 {
		return nil, fmt.Errorf("clock: new domain: negative backward jump threshold %d", backward)
	}
	fallback := opts.FallbackKind
	if fallback == KindUnspecified {
		fallback = DefaultFallbackKind
	}
	if fallback != KindWall && fallback != KindSteady {
		return nil, fmt.Errorf("clock: new domain: invalid fallback kind %s", fallback)
	}
	wall := opts.WallFn
	if wall == nil {
		wall = realWallNow
	}
	steady := opts.SteadyFn
	if steady == nil {
		steady = makeSteadyNow()
	}
	d := &Domain{
		wallFn:            wall,
		steadyFn:          steady,
		simFn:             opts.SimFn,
		forwardThreshold:  forward,
		backwardThreshold: backward,
		fallbackKind:      fallback,
	}
	d.useSimTime.Store(opts.UseSimTime)
	if opts.UseSimTime && opts.SimFn == nil {
		d.fallbackReason = "clock: use_sim_time requested without sim source; serving fallback until first SetSimTime"
	}
	d.lastActive.Store(d.Now())
	return d, nil
}

// UseSimTime reports whether sim is the selected active timeline.
func (d *Domain) UseSimTime() bool {
	return d.useSimTime.Load()
}

// SetUseSimTime flips the selected timeline and always notifies.
//
// Activation and deactivation always fire jump callbacks and always bump
// Generation, even when both timelines read the same value, so caches
// invalidate on every switch and backward sim resets cannot stall
// control nodes behind stale entries. It returns the activation event
// and true, or false when the value did not change.
func (d *Domain) SetUseSimTime(v bool) (JumpEvent, bool) {
	old := d.useSimTime.Load()
	_, oldNow := d.activeRead()
	if old == v {
		return JumpEvent{}, false
	}
	d.useSimTime.Store(v)
	newKind, newNow := d.activeRead()
	ev := JumpEvent{
		Kind:                newKind,
		FromNanos:           oldNow,
		ToNanos:             newNow,
		DeltaNanos:          newNow - oldNow,
		SimActivationChange: true,
	}
	switch {
	case newNow > oldNow:
		ev.Forward = true
		d.forwardCount.Add(1)
	case newNow < oldNow:
		ev.Backward = true
		d.backwardCount.Add(1)
	default:
	}
	d.lastActive.Store(newNow)
	d.generation.Add(1)
	d.fire(ev)
	return ev, true
}

// ActiveKind returns the currently served timeline: sim while selected
// and available, the fallback kind while selected but unavailable, or
// wall otherwise. Querying while fallback serves counts one fallback
// read, documented under FallbackCount.
func (d *Domain) ActiveKind() SourceKind {
	kind, _ := d.activeRead()
	return kind
}

// Now returns the active timeline reading in nanoseconds.
func (d *Domain) Now() int64 {
	_, now := d.activeRead()
	return now
}

// WallNow returns the wall source reading in nanoseconds.
func (d *Domain) WallNow() int64 {
	return d.wallFn()
}

// SteadyNow returns the steady source reading in nanoseconds.
func (d *Domain) SteadyNow() int64 {
	return d.steadyFn()
}

// SimNow returns the sim source reading and whether one is available.
//
// Manual sim domains report false until the first SetSimTime. Callers
// needing the fallback value instead use Now with UseSimTime set.
func (d *Domain) SimNow() (int64, bool) {
	if d.simFn != nil {
		return d.simFn(), true
	}
	if d.simEverSet.Load() {
		return d.simManual.Load(), true
	}
	return 0, false
}

// SimAvailable reports whether a sim reading exists right now.
func (d *Domain) SimAvailable() bool {
	_, ok := d.SimNow()
	return ok
}

// Fallback returns the construction time fallback reason, or empty when
// construction needed no fallback. The record is sticky: it stays after
// sim arrives so the fallback stays visible in logs.
func (d *Domain) Fallback() string {
	return d.fallbackReason
}

// FallbackKind returns the timeline serving reads while sim is unavailable.
func (d *Domain) FallbackKind() SourceKind {
	return d.fallbackKind
}

// FallbackActive reports whether the fallback currently serves reads.
func (d *Domain) FallbackActive() bool {
	if !d.useSimTime.Load() {
		return false
	}
	return !d.SimAvailable()
}

// FallbackCount counts active reads served from fallback, including the
// construction seed and ActiveKind queries. It only moves while
// fallback serves, so any nonzero value with a fallback reason is loud.
func (d *Domain) FallbackCount() uint64 {
	return d.fallbackCount.Load()
}

// Generation returns the cache generation, bumped on every backward
// jump and every sim activation change. Caches holding an older value
// must clear; see CacheGuard.
func (d *Domain) Generation() uint64 {
	return d.generation.Load()
}

// JumpsForward counts fired forward jumps, including activations that moved forward.
func (d *Domain) JumpsForward() uint64 {
	return d.forwardCount.Load()
}

// JumpsBackward counts fired backward jumps, including activations that moved backward.
func (d *Domain) JumpsBackward() uint64 {
	return d.backwardCount.Load()
}

// JumpsTotal counts all fired jump events.
func (d *Domain) JumpsTotal() uint64 {
	return d.forwardCount.Load() + d.backwardCount.Load()
}

// JumpForwardThreshold returns the configured forward jump threshold in nanoseconds.
func (d *Domain) JumpForwardThreshold() int64 {
	return d.forwardThreshold
}

// JumpBackwardThreshold returns the configured backward jump threshold in nanoseconds.
func (d *Domain) JumpBackwardThreshold() int64 {
	return d.backwardThreshold
}

// Describe renders the domain as one loud status line for logs and errors.
func (d *Domain) Describe() string {
	useSim := d.useSimTime.Load()
	kind, now := d.activeRead()
	_, simAvailable := d.SimNow()
	return fmt.Sprintf("clock: active=%s use_sim_time=%t sim_available=%t fallback=%q generation=%d now_ns=%d",
		kind, useSim, simAvailable, d.fallbackReason, d.generation.Load(), now)
}

// Poll compares the active reading against the last observation and
// fires callbacks in registration order when the delta passes a
// threshold. Deltas below threshold update the baseline without firing
// so jitter never accumulates into a phantom jump. Backward jumps bump
// Generation before firing so caches observed inside callbacks already
// report stale. Poll allocates nothing on either path. Hosts poll once
// per spin; SetSimTime and SetUseSimTime poll internally.
func (d *Domain) Poll() (JumpEvent, bool) {
	kind, now := d.activeRead()
	last := d.lastActive.Load()
	if now == last {
		return JumpEvent{}, false
	}
	delta := now - last
	d.lastActive.Store(now)
	if delta > 0 && delta < d.forwardThreshold {
		return JumpEvent{}, false
	}
	if delta < 0 && delta > -d.backwardThreshold {
		return JumpEvent{}, false
	}
	ev := JumpEvent{Kind: kind, FromNanos: last, ToNanos: now, DeltaNanos: delta}
	if delta > 0 {
		ev.Forward = true
		d.forwardCount.Add(1)
	} else {
		ev.Backward = true
		d.backwardCount.Add(1)
		d.generation.Add(1)
	}
	d.fire(ev)
	return ev, true
}

// SetSimTime drives manual sim time and polls for jumps.
//
// It errors on domains with an external SimFn, which must be driven
// directly instead. Otherwise it stores the value, marks sim available,
// and returns the Poll outcome: the jump event and true when the step
// passed a threshold.
func (d *Domain) SetSimTime(nanos int64) (JumpEvent, bool, error) {
	if d.simFn != nil {
		return JumpEvent{}, false, fmt.Errorf("clock: set sim time %d: %w", nanos, ErrExternalSim)
	}
	d.simManual.Store(nanos)
	d.simEverSet.Store(true)
	ev, jumped := d.Poll()
	return ev, jumped, nil
}

// AddJumpCallback registers a jump callback and returns its id.
//
// Ids issue monotonically from one upward. Registration allocates on the
// rare write path only; firing never allocates. Callbacks fire in
// registration order. A nil callback is rejected.
func (d *Domain) AddJumpCallback(fn JumpCallback) (uint64, error) {
	if fn == nil {
		return 0, ErrNilJumpCallback //nolint:wrapcheck // sentinel error, nothing to wrap
	}
	id := d.nextCallbackID.Add(1)
	for {
		old := d.callbacks.Load()
		var entries []jumpRegistration
		if old != nil {
			entries = old.entries
		}
		next := make([]jumpRegistration, len(entries)+1)
		copy(next, entries)
		next[len(entries)] = jumpRegistration{id: id, fn: fn}
		if d.callbacks.CompareAndSwap(old, &jumpRegistry{entries: next}) {
			return id, nil
		}
	}
}

// RemoveJumpCallback deregisters the callback id and keeps the order of
// the rest. Removing from inside a firing callback is safe: it affects
// the next firing, never the one in progress. Unknown ids error loudly.
func (d *Domain) RemoveJumpCallback(id uint64) error {
	for {
		old := d.callbacks.Load()
		if old == nil {
			return fmt.Errorf("clock: remove jump callback %d: %w", id, ErrUnknownJumpCallback)
		}
		at := -1
		for i, entry := range old.entries {
			if entry.id == id {
				at = i
				break
			}
		}
		if at < 0 {
			return fmt.Errorf("clock: remove jump callback %d: %w", id, ErrUnknownJumpCallback)
		}
		next := make([]jumpRegistration, len(old.entries)-1)
		copy(next, old.entries[:at])
		copy(next[at:], old.entries[at+1:])
		if d.callbacks.CompareAndSwap(old, &jumpRegistry{entries: next}) {
			return nil
		}
	}
}

// activeRead returns the active kind plus its reading in one step.
//
// Sim selected and available reads sim. Sim selected but unavailable
// counts one fallback read and serves the fallback kind. Otherwise it
// reads wall. Source functions must be cheap and side effect free; they
// run on every read.
func (d *Domain) activeRead() (SourceKind, int64) {
	if d.useSimTime.Load() {
		if v, ok := d.SimNow(); ok {
			return KindSim, v
		}
		d.fallbackCount.Add(1)
		return d.fallbackKind, d.readFallback()
	}
	return KindWall, d.wallFn()
}

// readKind returns the reading for an explicit timeline.
//
// A sim read without an available sim source counts one fallback read
// and serves the fallback kind, the same loud rule as active reads.
// Unspecified and invalid kinds defensively read wall; constructors
// validate kinds so this path only serves programming errors.
func (d *Domain) readKind(kind SourceKind) int64 {
	switch kind {
	case KindWall:
		return d.wallFn()
	case KindSteady:
		return d.steadyFn()
	case KindSim:
		if v, ok := d.SimNow(); ok {
			return v
		}
		d.fallbackCount.Add(1)
		return d.readFallback()
	case KindUnspecified:
		return d.wallFn()
	default:
		return d.wallFn()
	}
}

// readFallback returns the fallback timeline reading.
func (d *Domain) readFallback() int64 {
	if d.fallbackKind == KindWall {
		return d.wallFn()
	}
	return d.steadyFn()
}

// fire invokes jump callbacks in registration order without allocating.
//
// The snapshot load is atomic and lock free, so concurrent Add and
// Remove race safely with firing and the fire path allocates nothing.
func (d *Domain) fire(ev JumpEvent) {
	reg := d.callbacks.Load()
	if reg == nil {
		return
	}
	for _, entry := range reg.entries {
		entry.fn(ev)
	}
}

// SimMismatchError is the loud error for use_sim_time disagreement.
//
// It carries the action name, both sides of the agreement, the goal
// stamp with its timeline, and the active domain timeline with its
// reading, so logs, metrics, and CLI can repeat the full context.
// Silence on clock disagreement is a defect, never a state.
type SimMismatchError struct {
	// Action names the action client or goal owner owning the stamp.
	Action string
	// ClientUseSimTime is the client side use_sim_time flag.
	ClientUseSimTime bool
	// DomainUseSimTime is the domain side use_sim_time flag.
	DomainUseSimTime bool
	// GoalStampNanos is the goal stamp, or minus one for pure flag mismatch.
	GoalStampNanos int64
	// StampKind is the timeline the stamp was taken from.
	StampKind SourceKind
	// ActiveKind is the domain timeline active at the check.
	ActiveKind SourceKind
	// NowNanos is the active timeline reading at the check.
	NowNanos int64
}

// Error renders the mismatch as one loud line with both flags, the goal
// stamp and its timeline, and the active domain timeline and reading.
func (e *SimMismatchError) Error() string {
	return fmt.Sprintf("clock: sim time mismatch action=%s client_use_sim_time=%t domain_use_sim_time=%t goal_stamp_ns=%d stamp_kind=%s active_kind=%s now_ns=%d",
		e.Action, e.ClientUseSimTime, e.DomainUseSimTime, e.GoalStampNanos, e.StampKind, e.ActiveKind, e.NowNanos)
}

// IsSimMismatch reports whether err carries a SimMismatchError.
func IsSimMismatch(err error) bool {
	var mismatch *SimMismatchError
	return errors.As(err, &mismatch)
}

// CheckActionSimAgreement rejects action clients whose use_sim_time flag
// disagrees with the domain. Matching flags pass with no allocation.
func (d *Domain) CheckActionSimAgreement(clientUseSimTime bool, action string) error {
	domainUseSim := d.useSimTime.Load()
	if clientUseSimTime == domainUseSim {
		return nil
	}
	kind, now := d.activeRead()
	return &SimMismatchError{
		Action:           action,
		ClientUseSimTime: clientUseSimTime,
		DomainUseSimTime: domainUseSim,
		GoalStampNanos:   -1,
		StampKind:        KindUnspecified,
		ActiveKind:       kind,
		NowNanos:         now,
	}
}

// CheckGoalStamp rejects goal stamps taken from a different timeline
// than the active one, so wall clock stamps against sim time surface as
// an explicit error with goal stamp and active clock domain instead of
// freezing silently. The stamp kind must be explicit: KindUnspecified
// always mismatches.
func (d *Domain) CheckGoalStamp(stampNanos int64, stampKind SourceKind, action string) error {
	kind, now := d.activeRead()
	if stampKind == kind {
		return nil
	}
	return &SimMismatchError{
		Action:           action,
		ClientUseSimTime: d.useSimTime.Load(),
		DomainUseSimTime: d.useSimTime.Load(),
		GoalStampNanos:   stampNanos,
		StampKind:        stampKind,
		ActiveKind:       kind,
		NowNanos:         now,
	}
}

// CacheGuard tracks the domain generation for one cache.
//
// Backward jumps and sim activation changes bump the domain generation.
// A cache records Observe after clearing and checks Stale before use;
// stale means clear again. Forward jumps never bump the generation, so
// warm caches survive forward time motion.
type CacheGuard struct {
	// seen is the generation observed at the last clear.
	seen uint64
}

// Observe records generation as the last clean state after clearing.
func (g *CacheGuard) Observe(generation uint64) {
	g.seen = generation
}

// Stale reports whether generation moved since the last Observe.
func (g *CacheGuard) Stale(generation uint64) bool {
	return g.seen != generation
}

// Timer is one clock bound periodic timer with explicit overload
// policy. All state is atomic and firing holds no lock, so IsReady and
// Call never block and wall timers cannot hang pooled supervision under
// load. A timer registers one ordered domain jump callback at
// construction and deregisters it at Close. Do not copy a Timer after
// first use.
type Timer struct {
	// domain owns the timelines this timer reads.
	domain *Domain
	// kind is the bound timeline, never unspecified after construction.
	kind SourceKind
	// overload is the immutable backlog policy.
	overload OverloadPolicy
	// period holds the period in nanoseconds. Zero means always ready.
	period atomic.Int64
	// canceled rejects Call while true.
	canceled atomic.Bool
	// closed rejects Call and jump handling after Close.
	closed atomic.Bool
	// lastCall holds the last firing time.
	lastCall atomic.Int64
	// nextCall holds the next scheduled time.
	nextCall atomic.Int64
	// fires counts served firings.
	fires atomic.Uint64
	// dropped counts coalesced firings.
	dropped atomic.Uint64
	// guardCount counts guard triggers from Reset and forward jumps.
	guardCount atomic.Uint64
	// callbacks holds the fire and reset callbacks.
	callbacks atomic.Pointer[timerCallbacks]
	// jumpID is the domain jump registration for Close.
	jumpID uint64
}

// NewTimer builds a timer bound to opts.Kind reading the domain clock.
//
// Init reads the current time from the clock, sets last call to now and
// next call to now plus period with saturating addition, and sets
// canceled to the inverse of autostart. An unspecified kind selects
// steady, the default. An invalid kind, a negative period, an invalid
// overload policy, a nil domain, or a nil fire callback is rejected
// loudly. The reset callback is set later with SetResetCallback.
func NewTimer(d *Domain, opts TimerOptions, fire func(FireContext)) (*Timer, error) {
	if d == nil {
		return nil, ErrNilDomain //nolint:wrapcheck // sentinel error, nothing to wrap
	}
	if fire == nil {
		return nil, ErrNilTimerCallback //nolint:wrapcheck // sentinel error, nothing to wrap
	}
	kind := opts.Kind
	if kind == KindUnspecified {
		kind = KindSteady
	}
	if kind != KindWall && kind != KindSteady && kind != KindSim {
		return nil, fmt.Errorf("clock: new timer: invalid kind %s", kind)
	}
	if opts.PeriodNanos < 0 {
		return nil, fmt.Errorf("clock: new timer: negative period %d", opts.PeriodNanos)
	}
	if opts.Overload != OverloadCoalesce && opts.Overload != OverloadCatchUp {
		return nil, fmt.Errorf("clock: new timer: invalid overload policy %s", opts.Overload)
	}
	t := &Timer{domain: d, kind: kind, overload: opts.Overload}
	t.period.Store(opts.PeriodNanos)
	t.canceled.Store(!opts.Autostart)
	t.callbacks.Store(&timerCallbacks{fire: fire})
	now := d.readKind(kind)
	t.lastCall.Store(now)
	t.nextCall.Store(saturatingAdd(now, opts.PeriodNanos))
	id, err := d.AddJumpCallback(t.handleJump)
	if err != nil {
		return nil, fmt.Errorf("clock: new timer: add jump callback: %w", err)
	}
	t.jumpID = id
	return t, nil
}

// Kind returns the bound timeline.
func (t *Timer) Kind() SourceKind {
	return t.kind
}

// Overload returns the declared backlog policy.
func (t *Timer) Overload() OverloadPolicy {
	return t.overload
}

// Period returns the period in nanoseconds.
func (t *Timer) Period() int64 {
	return t.period.Load()
}

// SetPeriod replaces the period. Negative values are rejected and leave
// the running period untouched.
func (t *Timer) SetPeriod(periodNanos int64) error {
	if periodNanos < 0 {
		return fmt.Errorf("clock: set timer period %d: negative periods are rejected", periodNanos)
	}
	t.period.Store(periodNanos)
	return nil
}

// Canceled reports whether the timer rejects Call.
func (t *Timer) Canceled() bool {
	return t.canceled.Load()
}

// Cancel parks the timer; Call rejects until Start or Reset.
func (t *Timer) Cancel() {
	t.canceled.Store(true)
}

// Start arms a canceled timer and re-arms its schedule to now plus
// period so a long parked timer never fires a stale backlog. It
// returns false when the timer was already running.
func (t *Timer) Start() bool {
	if !t.canceled.Load() {
		return false
	}
	now := t.domain.readKind(t.kind)
	t.nextCall.Store(saturatingAdd(now, t.period.Load()))
	t.canceled.Store(false)
	return true
}

// SetCallback replaces the fire callback. Nil is rejected.
func (t *Timer) SetCallback(fire func(FireContext)) error {
	if fire == nil {
		return ErrNilTimerCallback //nolint:wrapcheck // sentinel error, nothing to wrap
	}
	for {
		old := t.callbacks.Load()
		var reset func()
		if old != nil {
			reset = old.reset
		}
		if t.callbacks.CompareAndSwap(old, &timerCallbacks{fire: fire, reset: reset}) {
			return nil
		}
	}
}

// SetResetCallback replaces the reset callback. Nil clears it.
func (t *Timer) SetResetCallback(reset func()) {
	for {
		old := t.callbacks.Load()
		var fire func(FireContext)
		if old != nil {
			fire = old.fire
		}
		if t.callbacks.CompareAndSwap(old, &timerCallbacks{fire: fire, reset: reset}) {
			return
		}
	}
}

// IsReady reports whether the next call time passed. Zero period timers
// are always ready. Canceled and closed timers are never ready.
func (t *Timer) IsReady() bool {
	if t.canceled.Load() || t.closed.Load() {
		return false
	}
	return t.IsReadyAt(t.domain.readKind(t.kind))
}

// IsReadyAt reports readiness at an explicit now without reading the clock.
func (t *Timer) IsReadyAt(now int64) bool {
	if t.canceled.Load() || t.closed.Load() {
		return false
	}
	if t.period.Load() == 0 {
		return true
	}
	return now >= t.nextCall.Load()
}

// Call serves one firing when ready and errors otherwise.
//
// Canceled timers and closed timers are rejected before the clock is
// read. Unready timers report ErrTimerNotReady. A ready call advances
// next call per the overload policy, fills expected plus actual call
// times with time since last call, counts the firing, and invokes the
// typed callback without holding a lock. Call is race free but may
// double fire under concurrent callers; drive each timer from one loop.
func (t *Timer) Call() (FireContext, error) {
	if t.closed.Load() {
		return FireContext{}, ErrTimerClosed //nolint:wrapcheck // sentinel error, nothing to wrap
	}
	if t.canceled.Load() {
		return FireContext{}, ErrTimerCanceled //nolint:wrapcheck // sentinel error, nothing to wrap
	}
	now := t.domain.readKind(t.kind)
	return t.callAt(now)
}

// callAt serves one firing at an explicit now for tests and Call.
func (t *Timer) callAt(now int64) (FireContext, error) {
	period := t.period.Load()
	var expected int64
	if period == 0 {
		expected = now
	} else {
		next := t.nextCall.Load()
		if now < next {
			return FireContext{}, ErrTimerNotReady //nolint:wrapcheck // sentinel error, nothing to wrap
		}
		expected = next
	}
	last := t.lastCall.Load()
	t.advanceNext(now, period)
	t.lastCall.Store(now)
	t.fires.Add(1)
	ctx := FireContext{
		ExpectedNanos:  expected,
		ActualNanos:    now,
		SinceLastNanos: now - last,
		Backlog:        t.backlogAt(now),
	}
	cb := t.callbacks.Load()
	if cb != nil && cb.fire != nil {
		cb.fire(ctx)
	}
	return ctx, nil
}

// advanceNext moves the next call forward after a firing at now.
//
// Coalesce skips to now plus period and counts every skipped firing in
// Dropped so the skip stays observable. Catch up advances exactly one
// period so missed firings stay visible in Backlog until drained.
func (t *Timer) advanceNext(now, period int64) {
	if period <= 0 {
		t.nextCall.Store(now)
		return
	}
	next := t.nextCall.Load()
	if t.overload == OverloadCoalesce {
		if missed := (now - next) / period; missed > 0 {
			t.dropped.Add(uint64(missed))
		}
		t.nextCall.Store(saturatingAdd(now, period))
		return
	}
	t.nextCall.Store(saturatingAdd(next, period))
}

// Reset re-arms the timer to now plus period, clears canceled, triggers
// the guard condition, and fires the reset callback when set.
func (t *Timer) Reset() {
	now := t.domain.readKind(t.kind)
	t.nextCall.Store(saturatingAdd(now, t.period.Load()))
	t.canceled.Store(false)
	t.guardCount.Add(1)
	cb := t.callbacks.Load()
	if cb != nil && cb.reset != nil {
		cb.reset()
	}
}

// Close deregisters the timer jump callback in order and parks the
// timer. Close is idempotent: repeats return nil. Call after Close
// reports ErrTimerClosed.
func (t *Timer) Close() error {
	if t.closed.Swap(true) {
		return nil
	}
	err := t.domain.RemoveJumpCallback(t.jumpID)
	if err != nil {
		return fmt.Errorf("clock: close timer: %w", err)
	}
	return nil
}

// Fires counts served firings.
func (t *Timer) Fires() uint64 {
	return t.fires.Load()
}

// Dropped counts coalesced firings skipped by the coalesce policy.
// Catch up timers never drop, so nonzero here always names the policy.
func (t *Timer) Dropped() uint64 {
	return t.dropped.Load()
}

// GuardCount counts guard triggers from Reset and forward jumps.
func (t *Timer) GuardCount() uint64 {
	return t.guardCount.Load()
}

// Backlog returns the owed firings still behind the clock: zero when
// unready, canceled, closed, or zero period.
func (t *Timer) Backlog() uint64 {
	return t.backlogAt(t.domain.readKind(t.kind))
}

// BacklogAt returns owed firings at an explicit now without reading the clock.
func (t *Timer) BacklogAt(now int64) uint64 {
	return t.backlogAt(now)
}

// backlogAt computes owed firings behind now.
func (t *Timer) backlogAt(now int64) uint64 {
	if t.canceled.Load() || t.closed.Load() {
		return 0
	}
	period := t.period.Load()
	if period <= 0 {
		return 0
	}
	missed := (now - t.nextCall.Load()) / period
	if missed <= 0 {
		return 0
	}
	return uint64(missed)
}

// LastCall returns the last firing time in nanoseconds.
func (t *Timer) LastCall() int64 {
	return t.lastCall.Load()
}

// NextCall returns the next scheduled time in nanoseconds.
func (t *Timer) NextCall() int64 {
	return t.nextCall.Load()
}

// handleJump rebases the timer across a domain time jump.
//
// It saves next minus from as credit before the jump and restores it
// after: next becomes to plus credit, preserving phase. A forward jump
// passing the next call triggers the guard. A backward jump past the
// last call time resets to now plus period so control nodes never stall
// behind a stale schedule. Jumps on other timelines, canceled timers,
// and closed timers are ignored.
func (t *Timer) handleJump(ev JumpEvent) {
	if ev.Kind != t.kind {
		return
	}
	if t.canceled.Load() || t.closed.Load() {
		return
	}
	period := t.period.Load()
	next := t.nextCall.Load()
	if ev.Backward {
		if ev.ToNanos < t.lastCall.Load() {
			t.nextCall.Store(saturatingAdd(ev.ToNanos, period))
			return
		}
		t.nextCall.Store(saturatingAdd(ev.ToNanos, next-ev.FromNanos))
		return
	}
	if ev.FromNanos < next && next <= ev.ToNanos {
		t.guardCount.Add(1)
	}
	t.nextCall.Store(saturatingAdd(ev.ToNanos, next-ev.FromNanos))
}
