// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package core

import (
	"fmt"
	"sync"
)

// MismatchSeverity names the channel weight of a mismatch event.
type MismatchSeverity string

// Mismatch severities.
const (
	// SeverityError marks an incompatibility: the pair cannot flow.
	SeverityError MismatchSeverity = "error"
	// SeverityWarning marks an unresolved marker: the pair flows but
	// something skipped explicit resolution.
	SeverityWarning MismatchSeverity = "warning"
)

// MismatchEvent is one loud QoS disagreement with topic, endpoint pair, and
// policy kind.
//
// Events feed three channels with one value: logs through LogLine, metrics
// through MismatchCounter, and CLI doctor checks through String. Silence in
// any channel is a defect, never a state.
type MismatchEvent struct {
	// Topic is the topic whose endpoints disagree.
	Topic string
	// Publisher is the offering endpoint name.
	Publisher string
	// Subscriber is the requesting endpoint name.
	Subscriber string
	// Policy is the RxO policy kind that mismatched.
	Policy PolicyKind
	// Offered is the publisher side value in lowercase form.
	Offered string
	// Requested is the subscriber side value in lowercase form.
	Requested string
	// Severity is error for incompatibilities, warning for markers.
	Severity MismatchSeverity
	// Detail is the human readable reason.
	Detail string
}

// CompatibilityEvents expands a CheckCompatibility report into per policy
// mismatch events carrying topic, endpoint pair, and policy kind.
//
// Errors become error severity events; warnings become warning severity
// events. A clean report yields no events.
func CompatibilityEvents(topic, publisher, subscriber string, offered, requested QoSProfile) []MismatchEvent {
	report := CheckCompatibility(offered, requested)
	var events []MismatchEvent
	for _, issue := range report.Errors {
		events = append(events, MismatchEvent{
			Topic:      topic,
			Publisher:  publisher,
			Subscriber: subscriber,
			Policy:     issue.Kind,
			Offered:    policyValue(issue.Kind, offered),
			Requested:  policyValue(issue.Kind, requested),
			Severity:   SeverityError,
			Detail:     issue.Detail,
		})
	}
	for _, issue := range report.Warnings {
		events = append(events, MismatchEvent{
			Topic:      topic,
			Publisher:  publisher,
			Subscriber: subscriber,
			Policy:     issue.Kind,
			Offered:    policyValue(issue.Kind, offered),
			Requested:  policyValue(issue.Kind, requested),
			Severity:   SeverityWarning,
			Detail:     issue.Detail,
		})
	}
	return events
}

// policyValue renders the named policy of p in lowercase form.
func policyValue(kind PolicyKind, p QoSProfile) string {
	switch kind {
	case PolicyReliability:
		return p.Reliability.String()
	case PolicyDurability:
		return p.Durability.String()
	case PolicyDeadline:
		return p.Deadline.String()
	case PolicyLiveliness:
		return p.Liveliness.String()
	case PolicyLeaseDuration:
		return p.LeaseDuration.String()
	default:
		return "invalid"
	}
}

// String renders the event as one CLI doctor line.
func (e MismatchEvent) String() string {
	return fmt.Sprintf("topic=%s publisher=%s subscriber=%s policy=%s offered=%s requested=%s severity=%s: %s",
		e.Topic, e.Publisher, e.Subscriber, string(e.Policy), e.Offered, e.Requested, string(e.Severity), e.Detail)
}

// LogLine renders the event as one structured log line.
func (e MismatchEvent) LogLine() string {
	return "qos mismatch " + e.String()
}

// MismatchCounter holds monotonically increasing mismatch counts per policy
// kind plus a total.
//
// The counter is caller owned: core keeps no global state. Wire Record into
// the metrics exposition outside core; the shape here is the single agreed
// counting point for logs, metrics, and CLI. The zero value is usable: the
// map lazy initializes on first Record, so var c MismatchCounter works
// without NewMismatchCounter. Do not copy a counter after first use.
type MismatchCounter struct {
	mu       sync.Mutex
	byPolicy map[PolicyKind]uint64
	total    uint64
}

// NewMismatchCounter returns an empty counter.
func NewMismatchCounter() *MismatchCounter {
	return &MismatchCounter{byPolicy: make(map[PolicyKind]uint64)}
}

// Record counts one mismatch event, monotonically and per policy kind.
// A nil or zero value counter initializes lazily and stays safe for
// concurrent use through its mutex.
func (c *MismatchCounter) Record(e MismatchEvent) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.byPolicy == nil {
		c.byPolicy = make(map[PolicyKind]uint64)
	}
	c.byPolicy[e.Policy]++
	c.total++
}

// Count returns the recorded events for one policy kind.
// A nil counter or a zero value with no records reports zero.
func (c *MismatchCounter) Count(kind PolicyKind) uint64 {
	if c == nil {
		return 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.byPolicy[kind]
}

// Total returns all recorded mismatch events.
// A nil counter reports zero.
func (c *MismatchCounter) Total() uint64 {
	if c == nil {
		return 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.total
}

// noCopy prevents copying after first use and triggers vet copylocks on copies.
type noCopy struct{}

// Lock marks noCopy as a lock for vet copylocks.
func (*noCopy) Lock() {}

// Unlock marks noCopy as a lock for vet copylocks.
func (*noCopy) Unlock() {}
