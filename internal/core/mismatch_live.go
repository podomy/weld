// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package core

import (
	"fmt"
	"strings"
	"sync"
)

// MismatchLogFunc receives one loud log line per mismatch event.
//
// Reporters call it once per event in emission order. The reporter
// serializes calls with its own mutex, so a plain slice append or a single
// ordered writer works without extra locking by the caller.
type MismatchLogFunc func(line string)

// LiveMismatchReporter wires loud mismatch events end to end across logs
// plus metrics plus CLI.
//
// Emit expands one offered versus requested pair through
// CompatibilityEvents, records every event into the counter, and logs every
// event through the log sink, then returns the events for CLI display. A
// compatible pair yields no events and records nothing; an incompatible
// pair never stays silent in any channel. Reliable versus best effort and
// volatile versus transient local each raise one error event with their
// policy kind through this path.
//
// The reporter owns no global state: the counter is caller owned and the
// log sink is caller provided. Do not copy a reporter after first use.
type LiveMismatchReporter struct {
	mu      sync.Mutex
	counter *MismatchCounter
	log     MismatchLogFunc
}

// NewLiveMismatchReporter returns a reporter that records into counter and
// logs through log.
//
// A nil counter allocates a fresh one so every event still counts. A nil
// log disables the log channel only; events still count and still return
// for CLI display, so callers wanting the full loud path must pass a sink.
func NewLiveMismatchReporter(counter *MismatchCounter, log MismatchLogFunc) *LiveMismatchReporter {
	if counter == nil {
		counter = NewMismatchCounter()
	}
	return &LiveMismatchReporter{counter: counter, log: log}
}

// Counter returns the reporter metrics counter.
//
// It allocates lazily so a zero value reporter still counts instead of
// dropping events silently. A nil receiver returns nil.
func (r *LiveMismatchReporter) Counter() *MismatchCounter {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.counter == nil {
		r.counter = NewMismatchCounter()
	}
	return r.counter
}

// Emit checks one live pair and emits every mismatch loudly.
//
// It expands offered versus requested through CompatibilityEvents with the
// given topic plus endpoint pair, records each event into the counter, logs
// each event LogLine through the sink, and returns the events for CLI
// display. Compatible pairs return no events and touch neither counter nor
// log. A nil receiver still computes and returns the events without
// recording or logging so callers never panic.
func (r *LiveMismatchReporter) Emit(topic, publisher, subscriber string, offered, requested QoSProfile) []MismatchEvent {
	events := CompatibilityEvents(topic, publisher, subscriber, offered, requested)
	if r == nil {
		return events
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.counter == nil {
		r.counter = NewMismatchCounter()
	}
	for _, e := range events {
		r.counter.Record(e)
		if r.log != nil {
			r.log(e.LogLine())
		}
	}
	return events
}

// EmitDeadlineAlert logs one freshness alert loudly and returns its line.
//
// Deadline misses are monitoring, never unmatch: the alert carries topic
// plus endpoint names with no policy kind, unlike MismatchEvent which
// carries policy plus severity across logs, metrics, and CLI. A nil
// receiver returns the line without logging so callers never panic.
func (r *LiveMismatchReporter) EmitDeadlineAlert(alert DeadlineAlert) string {
	line := alert.String()
	if r == nil {
		return line
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.log != nil {
		r.log(line)
	}
	return line
}

// ShouldDeliver reports whether offered can flow to requested on live pairing.
//
// It returns CheckCompatibility(offered, requested).Compatible. Incompatible
// pairs must stay silent. History, depth, lifespan, and the avoidance flag
// never affect the result.
func ShouldDeliver(offered, requested QoSProfile) bool {
	report := CheckCompatibility(offered, requested)
	return report.Compatible
}

// CheckPair emits one live pair loudly and reports whether it can flow.
//
// It expands offered versus requested through Emit with the given topic
// plus endpoint pair, records every event into the counter, and logs every
// event. Compatible pairs return true with no events. Incompatible pairs
// return false so callers stay silent, while the returned events carry
// topic plus endpoint pair plus policy kind for logs, metrics, and CLI. A
// nil receiver still computes events and returns the deliver flag without
// recording or logging.
func (r *LiveMismatchReporter) CheckPair(topic, publisher, subscriber string, offered, requested QoSProfile) (bool, []MismatchEvent) {
	events := r.Emit(topic, publisher, subscriber, offered, requested)
	deliver := true
	for _, e := range events {
		if e.Severity == SeverityError {
			deliver = false
			break
		}
	}
	return deliver, events
}

// CheckEndpoints emits one endpoint pair loudly and reports whether it can flow.
//
// It uses publisher Topic plus publisher Name plus subscriber Name with
// publisher ActualQoS as offered and subscriber ActualQoS as requested.
// Nil endpoints return false with no events. Incompatible pairs stay silent
// with loud events carrying topic plus endpoint pair plus policy kind.
// Compatible transient local pairs keep history for late joiners; callers
// gate HistoryCache snapshot serving on the returned flag and keep
// resolve once semantics through ActualQoS.
func (r *LiveMismatchReporter) CheckEndpoints(publisher, subscriber *Endpoint) (bool, []MismatchEvent) {
	if publisher == nil || subscriber == nil {
		return false, nil
	}
	topic := publisher.Topic()
	if topic == "" {
		topic = subscriber.Topic()
	}
	return r.CheckPair(topic, publisher.Name(), subscriber.Name(), publisher.ActualQoS(), subscriber.ActualQoS())
}

// mismatchPolicyOrder lists RxO policy kinds in stable exposition order.
func mismatchPolicyOrder() []PolicyKind {
	return []PolicyKind{
		PolicyReliability,
		PolicyDurability,
		PolicyDeadline,
		PolicyLiveliness,
		PolicyLeaseDuration,
	}
}

// FormatMismatchMetrics renders metrics exposition text with counts plus
// policy kinds.
//
// It emits one weld_mismatch line per RxO policy kind in stable order plus
// a weld_mismatch_total line, so logs, metrics, and CLI share one shape
// with monotonically increasing counters. A nil counter renders all zeros.
func FormatMismatchMetrics(counter *MismatchCounter) string {
	var b strings.Builder
	for _, kind := range mismatchPolicyOrder() {
		_, _ = fmt.Fprintf(&b, "weld_mismatch{policy=%q} %d\n", string(kind), counter.Count(kind)) //nolint:errcheck // strings.Builder never fails
	}
	_, _ = fmt.Fprintf(&b, "weld_mismatch_total %d\n", counter.Total()) //nolint:errcheck // strings.Builder never fails
	return b.String()
}
