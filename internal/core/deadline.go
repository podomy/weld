// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package core

import (
	"fmt"
	"math"
	"sync"
)

// DeadlineTracker watches one freshness contract without touching matching.
//
// A missed deadline fires one alert per outage carrying topic plus endpoint
// names; it never unmatches the pair. Repeat checks during the same outage
// stay quiet until a sample restarts the window, so one bad spell yields
// one alert and not one per sample. Times are caller supplied nanoseconds
// so tests stay deterministic; no clock is read here. The zero value is
// usable but quiet: empty names with an unspecified deadline never alerts.
// Contains a mutex guarding lastNanos plus the outage flag for concurrent
// MarkSeen plus Check; do not copy a tracker after first use (vet copylocks
// enforces this) and use a pointer. All methods use pointer receivers to
// avoid copying the lock.
type DeadlineTracker struct {
	mu        sync.Mutex
	topic     string
	endpoint  string
	deadline  Duration
	lastNanos int64
	alerted   bool
}

// NewDeadlineTracker starts tracking topic plus endpoint against deadline,
// with the last sample seen at startNanos. Returns a value containing a
// mutex by design like NewLivelinessTracker: callers hold it without copying
// after first use and use pointer receivers; switching to a pointer return
// would ripple through transport call sites, so the value form is kept.
func NewDeadlineTracker(topic, endpoint string, deadline Duration, startNanos int64) DeadlineTracker {
	return DeadlineTracker{topic: topic, endpoint: endpoint, deadline: deadline, lastNanos: startNanos}
}

// Topic returns the tracked topic name.
func (t *DeadlineTracker) Topic() string {
	return t.topic
}

// Endpoint returns the tracked endpoint name.
func (t *DeadlineTracker) Endpoint() string {
	return t.endpoint
}

// Deadline returns the tracked freshness contract.
func (t *DeadlineTracker) Deadline() Duration {
	return t.deadline
}

// MarkSeen records a sample at nowNanos, restarting the deadline window.
//
// Backward samples are ignored and never rewind the window: if nowNanos is
// earlier than the last seen time the call is a no op. A forward sample
// clears the per outage alert flag so the next lapse alerts once. Clock
// jumps rebase through Check, not here. Safe for concurrent use with Check.
func (t *DeadlineTracker) MarkSeen(nowNanos int64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if nowNanos < t.lastNanos {
		return
	}
	t.lastNanos = nowNanos
	t.alerted = false
}

// Check reports a DeadlineAlert when the window elapsed without a sample.
//
// Only the first Check in an outage alerts; later Checks in the same outage
// stay quiet until MarkSeen or a backward rebase clears the flag, so one
// bad spell yields one alert and then clears within one good period. A
// quiet Check inside the window also clears the flag so a clock rewind to
// the window recovers even when the target equals the last sample time. A
// best available sentinel never alerts, mirroring MissedAt. Unspecified
// deadlines never alert. Infinite deadlines in practice never
// alert because elapsed time stays below max int64. Negative deadlines are
// malformed and rejected by Validate; Check treats them as unspecified and
// never alerts so malformed input stays quiet here instead of firing false
// alarms. A backward time jump rebases lastNanos to nowNanos, clears the
// outage flag, and reports no alert, so the next forward window recovers
// without unmatching instead of carrying a stale future timestamp. Safe
// for concurrent use with MarkSeen.
func (t *DeadlineTracker) Check(nowNanos int64) (DeadlineAlert, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.deadline.IsUnspecified() {
		return DeadlineAlert{}, false
	}
	if t.deadline.Seconds < 0 {
		return DeadlineAlert{}, false
	}
	if IsBestAvailableDuration(t.deadline) {
		return DeadlineAlert{}, false
	}
	if t.deadline.IsInfinite() {
		return DeadlineAlert{}, false
	}
	if nowNanos < t.lastNanos {
		t.lastNanos = nowNanos
		t.alerted = false
		return DeadlineAlert{}, false
	}
	elapsed := saturatingSub(nowNanos, t.lastNanos)
	if elapsed < t.deadline.TotalNanoseconds() {
		t.alerted = false
		return DeadlineAlert{}, false
	}
	if t.alerted {
		return DeadlineAlert{}, false
	}
	t.alerted = true
	return DeadlineAlert{Topic: t.topic, Endpoint: t.endpoint, Deadline: t.deadline, ElapsedNanos: elapsed}, true
}

// DeadlineExceeded reports whether elapsedNanos reaches deadline without state.
//
// Unspecified, infinite, best available sentinel, and negative malformed
// deadlines never exceed, matching Check quiet rules. It is pure for the
// gating path: it never fires, never latches, and never rebases.
func DeadlineExceeded(deadline Duration, elapsedNanos int64) bool {
	if deadline.IsUnspecified() {
		return false
	}
	if deadline.Seconds < 0 {
		return false
	}
	if IsBestAvailableDuration(deadline) {
		return false
	}
	if deadline.IsInfinite() {
		return false
	}
	return elapsedNanos >= deadline.TotalNanoseconds()
}

// ElapsedAt returns the silence observed at nowNanos without mutating state.
//
// Unlike Check it never fires, never latches, and never rebases: backward
// time yields a negative elapsed so callers see the rewind. Tests prefer
// this peek for gating assertions. Elapsed uses saturating subtraction
// mirroring tfAddSaturating so near-range edges cannot overflow.
func (t *DeadlineTracker) ElapsedAt(nowNanos int64) int64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	return saturatingSub(nowNanos, t.lastNanos)
}

// MissedAt reports whether the deadline window elapsed at nowNanos.
//
// It never fires, never latches, and never rebases, so repeated calls are
// stable and gating stays pure. Unspecified, infinite, sentinel, and
// negative deadlines never miss. Backward time never misses. Elapsed uses
// saturating subtraction mirroring tfAddSaturating.
func (t *DeadlineTracker) MissedAt(nowNanos int64) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.deadline.IsUnspecified() {
		return false
	}
	if t.deadline.Seconds < 0 {
		return false
	}
	if IsBestAvailableDuration(t.deadline) {
		return false
	}
	if t.deadline.IsInfinite() {
		return false
	}
	if nowNanos < t.lastNanos {
		return false
	}
	return saturatingSub(nowNanos, t.lastNanos) >= t.deadline.TotalNanoseconds()
}

// saturatingSub returns a minus b saturating at the int64 range.
//
// Positive overflow saturates to max int64 and negative overflow saturates
// to min int64. It mirrors tfAddSaturating for elapsed math in this file so
// near-range fake clocks cannot overflow the subtraction.
func saturatingSub(a, b int64) int64 {
	if b > 0 && a < math.MinInt64+b {
		return math.MinInt64
	}
	if b < 0 && a > math.MaxInt64+b {
		return math.MaxInt64
	}
	return a - b
}

// DeadlineAlert is one fired freshness alarm with topic plus endpoint names.
//
// This is a single endpoint freshness signal, not an RxO pair mismatch, so
// it carries no PolicyKind and no severity field: the policy is always
// deadline and the weight is always error equivalent. Pair mismatches use
// MismatchEvent, which carries policy plus severity across logs, metrics,
// and CLI.
type DeadlineAlert struct {
	// Topic is the topic whose freshness contract lapsed.
	Topic string
	// Endpoint is the endpoint that missed its deadline.
	Endpoint string
	// Deadline is the contract that lapsed.
	Deadline Duration
	// ElapsedNanos is the silence observed when the alert fired.
	ElapsedNanos int64
}

// String renders the alert as one log and CLI line.
func (a DeadlineAlert) String() string {
	return fmt.Sprintf("deadline missed topic=%s endpoint=%s deadline=%s elapsed_ns=%d",
		a.Topic, a.Endpoint, a.Deadline.String(), a.ElapsedNanos)
}
