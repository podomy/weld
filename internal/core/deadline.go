// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package core

import (
	"fmt"
	"sync"
)

// DeadlineTracker watches one freshness contract without touching matching.
//
// A missed deadline fires an alert carrying topic plus endpoint names; it
// never unmatches the pair. Times are caller supplied nanoseconds so tests
// stay deterministic; no clock is read here. The zero value is usable but
// quiet: empty names with an unspecified deadline never alerts. Contains a
// mutex guarding lastNanos for concurrent MarkSeen plus Check; do not copy
// a tracker after first use (vet copylocks enforces this) and use a pointer.
// All methods use pointer receivers to avoid copying the lock.
type DeadlineTracker struct {
	mu        sync.Mutex
	topic     string
	endpoint  string
	deadline  Duration
	lastNanos int64
}

// NewDeadlineTracker starts tracking topic plus endpoint against deadline,
// with the last sample seen at startNanos.
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
// earlier than the last seen time the call is a no op. Clock jumps rebase
// through Check, not here. Safe for concurrent use with Check.
func (t *DeadlineTracker) MarkSeen(nowNanos int64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if nowNanos < t.lastNanos {
		return
	}
	t.lastNanos = nowNanos
}

// Check reports a DeadlineAlert when the window elapsed without a sample.
//
// Unspecified deadlines never alert. Infinite deadlines in practice never
// alert because elapsed time stays below max int64. Negative deadlines are
// malformed and rejected by Validate; Check treats them as unspecified and
// never alerts so malformed input stays quiet here instead of firing false
// alarms. A backward time jump rebases lastNanos to nowNanos and reports no
// alert, so the next forward window recovers instead of carrying a stale
// future timestamp. Safe for concurrent use with MarkSeen.
func (t *DeadlineTracker) Check(nowNanos int64) (DeadlineAlert, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.deadline.IsUnspecified() {
		return DeadlineAlert{}, false
	}
	if t.deadline.Seconds < 0 {
		return DeadlineAlert{}, false
	}
	if t.deadline.IsInfinite() {
		return DeadlineAlert{}, false
	}
	if nowNanos < t.lastNanos {
		t.lastNanos = nowNanos
		return DeadlineAlert{}, false
	}
	elapsed := nowNanos - t.lastNanos
	if elapsed < t.deadline.TotalNanoseconds() {
		return DeadlineAlert{}, false
	}
	return DeadlineAlert{Topic: t.topic, Endpoint: t.endpoint, Deadline: t.deadline, ElapsedNanos: elapsed}, true
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
