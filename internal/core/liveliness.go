// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package core

import (
	"sync"
)

// LivelinessState names one per-writer aliveness stage.
//
// Alive means the last assert is inside the lease. Stale means the lease
// elapsed once and the reader holds samples awaiting confirm. Expired means
// the hysteresis band elapsed with no assert and the writer is gone: the
// datapath must drop, never deliver ancient samples silently. Transitions
// bump generation once; repeats in one stage never bump.
type LivelinessState int

// Per-writer liveliness stages.
const (
	// LivelinessAlive means the last assert is inside the lease.
	LivelinessAlive LivelinessState = iota
	// LivelinessStale means the lease elapsed but hysteresis has not.
	LivelinessStale
	// LivelinessExpired means lease plus hysteresis elapsed with no assert.
	LivelinessExpired
)

// String renders the stage in lowercase form.
func (s LivelinessState) String() string {
	switch s {
	case LivelinessAlive:
		return "alive"
	case LivelinessStale:
		return "stale"
	case LivelinessExpired:
		return "expired"
	default:
		return "invalid"
	}
}

// GateDecision names one delivery gating outcome.
type GateDecision int

// Delivery gating decisions.
const (
	// GateDeliver means the sample may flow to the reader.
	GateDeliver GateDecision = iota
	// GateHold means the writer is stale: buffer, never deliver, never drop.
	GateHold
	// GateDrop means the sample must not flow and the reason is loud.
	GateDrop
)

// String renders the decision in lowercase form.
func (d GateDecision) String() string {
	switch d {
	case GateDeliver:
		return "deliver"
	case GateHold:
		return "hold"
	case GateDrop:
		return "drop"
	default:
		return "invalid"
	}
}

// GateOutcome is one pure delivery gating answer with its loud reason.
//
// Deliver carries no policy and no reason. Hold and Drop carry the policy
// to count plus a loud reason: stale or expired writers name lease
// duration, deadline misses name deadline. Callers count outcome.Policy
// through MismatchCounter for logs, metrics, and CLI.
type GateOutcome struct {
	// Decision is deliver, hold, or drop.
	Decision GateDecision
	// Reason is the loud human readable cause, empty on deliver.
	Reason string
	// Policy is the policy to count, empty on deliver.
	Policy PolicyKind
}

// String renders the outcome as one log and CLI line.
func (o GateOutcome) String() string {
	if o.Decision == GateDeliver {
		return "deliver"
	}
	return o.Decision.String() + " policy=" + string(o.Policy) + ": " + o.Reason
}

// GateDelivery decides deliver versus hold versus drop for one sample.
//
// It is pure: it reads only the writer stage plus the deadline miss flag
// and never touches clocks or counters. Priority is expired, then stale,
// then deadline miss, then deliver. Expired always drops with a distinct
// stale reason naming lease duration so a dead writer never serves ancient
// samples silently. Stale always holds naming lease duration. Deadline
// misses drop naming deadline. Fresh and alive delivers.
func GateDelivery(writer LivelinessState, deadlineMissed bool) GateOutcome {
	if writer == LivelinessExpired {
		return GateOutcome{
			Decision: GateDrop,
			Reason:   "stale sample: writer expired, lease elapsed without assert",
			Policy:   PolicyLeaseDuration,
		}
	}
	if writer == LivelinessStale {
		return GateOutcome{
			Decision: GateHold,
			Reason:   "hold sample: writer stale, awaiting liveliness confirm",
			Policy:   PolicyLeaseDuration,
		}
	}
	if deadlineMissed {
		return GateOutcome{
			Decision: GateDrop,
			Reason:   "drop sample: deadline missed",
			Policy:   PolicyDeadline,
		}
	}
	return GateOutcome{Decision: GateDeliver}
}

// NewGateMismatchEvent maps one hold or drop outcome to a loud mismatch event.
//
// Deliver yields no event. Hold and Drop yield one event carrying topic
// plus endpoint pair plus outcome policy for logs, metrics, and CLI.
// Offered and Requested carry the already rendered values for the policy.
func NewGateMismatchEvent(topic, endpoint string, outcome GateOutcome, offered, requested string) (MismatchEvent, bool) {
	if outcome.Decision == GateDeliver {
		return MismatchEvent{}, false
	}
	return MismatchEvent{
		Topic:      topic,
		Publisher:  endpoint,
		Subscriber: endpoint,
		Policy:     outcome.Policy,
		Offered:    offered,
		Requested:  requested,
		Severity:   SeverityError,
		Detail:     outcome.Reason,
	}, true
}

// DeadlineRxOInputs exposes the deadline pair the matcher compares.
//
// It returns offered deadline then requested deadline verbatim so the
// datapath carries the same inputs CheckCompatibility uses without
// importing matcher internals.
func DeadlineRxOInputs(offered, requested QoSProfile) (Duration, Duration) {
	return offered.Deadline, requested.Deadline
}

// LivelinessRxOInputs exposes the liveliness pair the matcher compares.
//
// It returns offered liveliness then requested liveliness verbatim so the
// datapath carries the same inputs CheckCompatibility uses.
func LivelinessRxOInputs(offered, requested QoSProfile) (LivelinessPolicy, LivelinessPolicy) {
	return offered.Liveliness, requested.Liveliness
}

// LeaseRxOInputs exposes the lease pair the matcher compares.
//
// It returns offered lease then requested lease verbatim so the datapath
// carries the same inputs CheckCompatibility uses.
func LeaseRxOInputs(offered, requested QoSProfile) (Duration, Duration) {
	return offered.LeaseDuration, requested.LeaseDuration
}

// EffectiveLeaseDuration folds offered versus requested lease to one expiry.
//
// Unspecified, infinite, best available sentinel, and negative malformed
// inputs count as no constraint, matching the matcher rule that a
// requested unspecified or infinite always flows and matching never
// compares magnitudes beyond default. Both unconstrained yields
// unspecified, which never expires. One finite side yields that side. Two
// finite sides yield the minimum so a dead writer turns stale at the
// earliest promise. Malformed negative stays quiet here and fails loudly
// in Validate instead of skewing the fold.
func EffectiveLeaseDuration(offered, requested Duration) Duration {
	offeredFree := livelinessNoConstraint(offered)
	requestedFree := livelinessNoConstraint(requested)
	if offeredFree && requestedFree {
		return UnspecifiedDuration()
	}
	if offeredFree {
		return requested
	}
	if requestedFree {
		return offered
	}
	return MinDuration(offered, requested)
}

// EffectiveDeadlineDuration folds offered versus requested deadline to one contract.
//
// Requested unconstrained (unspecified, infinite, sentinel, or negative
// malformed) yields unspecified, which never misses: a reader with no
// freshness expectation has nothing to enforce. A finite request yields
// the request: the reader expectation governs enforcement while the offer
// governs matching only. Offered input is carried for call site symmetry
// and ignored beyond documentation.
func EffectiveDeadlineDuration(offered, requested Duration) Duration {
	if livelinessNoConstraint(requested) {
		return UnspecifiedDuration()
	}
	return requested
}

// livelinessNoConstraint reports whether d carries no enforcement bound.
func livelinessNoConstraint(d Duration) bool {
	if d.IsUnspecified() {
		return true
	}
	if d.IsInfinite() {
		return true
	}
	if IsBestAvailableDuration(d) {
		return true
	}
	return d.Seconds < 0
}

// LivelinessTracker watches one writer aliveness contract with lease expiry.
//
// Kind selects the assert path: automatic asserts on every sample through
// RecordSampleAt, manual by topic asserts only through AssertManualAt. Hysteresis
// separates stale from expired: stale at one lease, expired at two leases,
// so one late assert recovers from stale without flapping expired. Every
// stage change bumps generation once; repeats in one stage never bump, so
// one outage yields one stale plus one expired edge, never one per poll.
// Backward time rebases quietly to alive with no bump, matching
// DeadlineTracker rebase and graph stale sequence rules. Times are caller
// supplied nanoseconds for determinism; SetClock installs an optional wall
// source for Now variants. The zero value is quiet with an unspecified
// lease that never expires. Do not copy a tracker after first use.
type LivelinessTracker struct {
	mu              sync.Mutex
	topic           string
	writer          string
	kind            LivelinessPolicy
	lease           Duration
	lastAssertNanos int64
	state           LivelinessState
	generation      uint64
	clock           func() int64
}

// NewLivelinessTracker starts tracking writer on topic with kind and lease.
//
// StartNanos is the last assert time; callers pass the creation tick for
// determinism. Resolve profiles with ResolveSystemDefaults before building
// the tracker so markers never reach enforcement silently.
func NewLivelinessTracker(topic, writer string, kind LivelinessPolicy, lease Duration, startNanos int64) LivelinessTracker {
	return LivelinessTracker{topic: topic, writer: writer, kind: kind, lease: lease, lastAssertNanos: startNanos, state: LivelinessAlive}
}

// NewPairedLivelinessTracker starts tracking with the folded offered versus requested lease.
//
// The stored lease is EffectiveLeaseDuration(offeredLease, requestedLease)
// so enforcement uses the same inputs the matcher compares. Kind, topic,
// writer, and start time pass through to NewLivelinessTracker.
func NewPairedLivelinessTracker(topic, writer string, kind LivelinessPolicy, offeredLease, requestedLease Duration, startNanos int64) LivelinessTracker {
	return NewLivelinessTracker(topic, writer, kind, EffectiveLeaseDuration(offeredLease, requestedLease), startNanos)
}

// Topic returns the tracked topic name.
func (t *LivelinessTracker) Topic() string {
	return t.topic
}

// Writer returns the tracked writer name.
func (t *LivelinessTracker) Writer() string {
	return t.writer
}

// Kind returns the tracked liveliness kind.
func (t *LivelinessTracker) Kind() LivelinessPolicy {
	return t.kind
}

// Lease returns the enforced lease duration.
func (t *LivelinessTracker) Lease() Duration {
	return t.lease
}

// LastAssertNanos returns the last assert tick.
func (t *LivelinessTracker) LastAssertNanos() int64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.lastAssertNanos
}

// Generation returns the transition count for flapping suppression.
func (t *LivelinessTracker) Generation() uint64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.generation
}

// LatchedState returns the last computed stage without advancing time.
func (t *LivelinessTracker) LatchedState() LivelinessState {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.state
}

// SetClock installs the wall source for Now variants; nil clears it.
func (t *LivelinessTracker) SetClock(clock func() int64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.clock = clock
}

// AssertManualAt records one manual by topic assert at nowNanos.
//
// It asserts only when kind is manual by topic and returns false otherwise
// without touching state, so automatic writers never take the manual path.
// A manual assert always moves last assert to now, even on backward time,
// because an explicit assert is new truth. Recovery to alive bumps
// generation once; asserts while alive never bump.
func (t *LivelinessTracker) AssertManualAt(nowNanos int64) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.kind != LivelinessManualByTopic {
		return false
	}
	t.lastAssertNanos = nowNanos
	if t.state != LivelinessAlive {
		t.state = LivelinessAlive
		t.generation++
	}
	return true
}

// RecordSampleAt asserts automatic liveliness on one sample at nowNanos.
//
// It asserts only when kind is automatic and returns false otherwise
// without touching state, so manual writers need AssertManualAt and never
// ride free on data. Semantics match AssertManualAt: last assert moves to
// now even on backward time, recovery bumps once, samples while alive
// never bump.
func (t *LivelinessTracker) RecordSampleAt(nowNanos int64) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.kind != LivelinessAutomatic {
		return false
	}
	t.lastAssertNanos = nowNanos
	if t.state != LivelinessAlive {
		t.state = LivelinessAlive
		t.generation++
	}
	return true
}

// StateAt returns the writer stage at nowNanos, advancing with hysteresis.
//
// Unconstrained leases (unspecified, infinite, sentinel, negative) always
// report alive. Backward time rebases last assert to now and reports alive
// quietly with no bump. Otherwise elapsed under one lease is alive, under
// two leases is stale, at or past two leases is expired. Each stage change
// bumps generation once; repeats in one stage never bump.
func (t *LivelinessTracker) StateAt(nowNanos int64) LivelinessState {
	t.mu.Lock()
	defer t.mu.Unlock()
	if livelinessNoConstraint(t.lease) {
		t.applyStateLocked(LivelinessAlive)
		return t.state
	}
	if nowNanos < t.lastAssertNanos {
		t.lastAssertNanos = nowNanos
		t.state = LivelinessAlive
		return t.state
	}
	total := t.lease.TotalNanoseconds()
	elapsed := nowNanos - t.lastAssertNanos
	want := LivelinessExpired
	if elapsed < total {
		want = LivelinessAlive
	} else if elapsed < livelinessDoubleLocked(total) {
		want = LivelinessStale
	}
	t.applyStateLocked(want)
	return t.state
}

// AssertManual records one manual assert using the installed clock.
//
// It returns false when no clock is set or kind is not manual by topic.
// Tests prefer AssertManualAt for determinism.
func (t *LivelinessTracker) AssertManual() bool {
	now, ok := t.clockNow()
	if !ok {
		return false
	}
	return t.AssertManualAt(now)
}

// RecordSample records one automatic sample using the installed clock.
//
// It returns false when no clock is set or kind is not automatic. Tests
// prefer RecordSampleAt for determinism.
func (t *LivelinessTracker) RecordSample() bool {
	now, ok := t.clockNow()
	if !ok {
		return false
	}
	return t.RecordSampleAt(now)
}

// StateNow returns the writer stage using the installed clock.
//
// It advances like StateAt. With no clock it returns the latched stage
// without advancing. Tests prefer StateAt for determinism.
func (t *LivelinessTracker) StateNow() LivelinessState {
	now, ok := t.clockNow()
	if !ok {
		return t.LatchedState()
	}
	return t.StateAt(now)
}

// GenerationNow returns the generation after advancing with the clock.
//
// With no clock it returns the latched generation. It exists so clock
// driven readers observe expiry edges without calling StateNow first.
func (t *LivelinessTracker) GenerationNow() uint64 {
	now, ok := t.clockNow()
	if !ok {
		return t.Generation()
	}
	_ = t.StateAt(now)
	return t.Generation()
}

// clockNow reads the installed clock without holding the tracker lock.
func (t *LivelinessTracker) clockNow() (int64, bool) {
	t.mu.Lock()
	clock := t.clock
	t.mu.Unlock()
	if clock == nil {
		return 0, false
	}
	return clock(), true
}

// applyStateLocked moves to want with one generation bump on change.
func (t *LivelinessTracker) applyStateLocked(want LivelinessState) {
	if t.state == want {
		return
	}
	t.state = want
	t.generation++
}

// livelinessDoubleLocked returns twice total saturating at max int64.
func livelinessDoubleLocked(total int64) int64 {
	const maxInt64 = int64(^uint64(0) >> 1)
	if total > maxInt64/2 {
		return maxInt64
	}
	return total * 2
}
