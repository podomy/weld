// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

// Package logging holds the DDS Security Logging plugin per 9.6.
package logging

import (
	"fmt"
	"log"
	"sync"
	"time"
)

// Event names one auditable security outcome per INV-10.
type Event struct {
	// Op names the check or operation.
	Op string
	// Identity names the peer or endpoint.
	Identity string
	// Allowed is true for grants and false for denials.
	Allowed bool
	// Reason explains the outcome.
	Reason string
	// At records when the event was recorded in UTC.
	At time.Time
	// Seq is the record sequence starting at 1, never reused.
	Seq uint64
}

// MaxLogEntries bounds retained audit events before oldest-first eviction.
//
// The bound keeps a long-lived suite from growing memory without
// limit. Evicted events are already emitted to the process log at
// record time, so eviction drops only the in-memory copy.
const MaxLogEntries = 4096

// Plugin records allow and deny events for audit.
type Plugin struct {
	// mu guards entries, listener, nextSeq, dropped, and tombstone.
	mu sync.Mutex
	// enabled selects emission. Disabled plugins drop silently.
	enabled bool
	// entries holds recorded events in order.
	entries []Event
	// listener observes each event, nil for none.
	listener func(Event)
	// nextSeq is the next event sequence, starting at 1.
	nextSeq uint64
	// dropped counts evicted events past the cap.
	dropped uint64
	// tombstone marks the first eviction with its sequence, nil before overflow.
	tombstone *Event
}

// New returns a disabled plugin.
func New() *Plugin {
	return &Plugin{}
}

// SetLogOptions enables or disables emission.
func (p *Plugin) SetLogOptions(enabled bool) {
	if p == nil {
		return
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	p.enabled = enabled
}

// EnableLogging enables emission.
func (p *Plugin) EnableLogging() {
	p.SetLogOptions(true)
}

// SetListener installs the event observer.
func (p *Plugin) SetListener(fn func(Event)) {
	if p == nil {
		return
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	p.listener = fn
}

// Log records one event when enabled.
//
// Sequence numbers increase without reuse starting at 1. When the buffer
// is full the oldest event evicts and a tombstone marker records the
// first eviction sequence plus the total dropped count; the tombstone
// stays visible via Tombstone until the plugin resets.
func (p *Plugin) Log(op, identity string, allowed bool, reason string) {
	if p == nil {
		return
	}

	p.mu.Lock()
	enabled := p.enabled
	listener := p.listener
	p.mu.Unlock()

	if !enabled {
		return
	}

	p.mu.Lock()
	p.nextSeq++
	event := Event{Op: op, Identity: identity, Allowed: allowed, Reason: reason, At: time.Now().UTC(), Seq: p.nextSeq}
	evicted := false
	if len(p.entries) >= MaxLogEntries {
		evictedSeq := p.entries[0].Seq
		copy(p.entries, p.entries[1:])
		p.entries[len(p.entries)-1] = event
		p.dropped++
		evicted = true
		if p.tombstone == nil {
			marker := Event{Op: "audit-overflow", Identity: "", Allowed: false, Reason: fmt.Sprintf("evicted oldest seq=%d dropped=%d", evictedSeq, p.dropped), At: time.Now().UTC(), Seq: evictedSeq}
			p.tombstone = &marker
		} else {
			p.tombstone.Reason = fmt.Sprintf("evicted oldest seq=%d dropped=%d", evictedSeq, p.dropped)
		}
	} else {
		p.entries = append(p.entries, event)
	}
	dropped := p.dropped
	seq := event.Seq
	p.mu.Unlock()

	log.Printf("security: audit seq=%d op=%s identity=%s allowed=%v reason=%s", seq, op, identity, allowed, reason)

	if evicted {
		log.Printf("security: audit log overflow dropped=%d seq=%d tombstone=seq=%d", dropped, seq, seq)
	}

	if listener != nil {
		listener(event)
	}
}

// Tombstone returns the overflow marker after eviction, if any.
//
// The marker carries Op audit-overflow, the first evicted sequence, and
// the total dropped count in its reason. It is nil before any eviction.
func (p *Plugin) Tombstone() (Event, bool) {
	if p == nil {
		return Event{}, false
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if p.tombstone == nil {
		return Event{}, false
	}

	return *p.tombstone, true
}

// Entries returns a copy of recorded events.
func (p *Plugin) Entries() []Event {
	if p == nil {
		return nil
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	out := make([]Event, len(p.entries))
	copy(out, p.entries)

	return out
}

// Count returns the number of recorded events.
func (p *Plugin) Count() int {
	if p == nil {
		return 0
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	return len(p.entries)
}

// Droppedcry9 returns the number of evicted events past the cap.
func (p *Plugin) Droppedcry9() uint64 {
	if p == nil {
		return 0
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	return p.dropped
}

// Summary counts allowed versus denied events.
func (p *Plugin) Summary() (allowed, denied int) {
	for _, event := range p.Entries() {
		if event.Allowed {
			allowed++
		} else {
			denied++
		}
	}

	return allowed, denied
}

// Format renders one event for logs without secrets.
func Format(event Event) string {
	return fmt.Sprintf("at=%s seq=%d op=%s identity=%s allowed=%v reason=%s", event.At.UTC().Format(time.RFC3339), event.Seq, event.Op, event.Identity, event.Allowed, event.Reason)
}
