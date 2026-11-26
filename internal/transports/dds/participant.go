// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

package dds

import (
	"fmt"
	"sync"

	"github.com/podomy/weld/internal/core"
)

// Manual-by-participant liveliness with assert paths, DDS 1.4 section 2.2.3.8.
//
// Core tracks automatic plus manual by topic only, and ReverseQoS still
// refuses wire kind 1 loudly (pinned by writerqos tests). This file adds
// the participant path without touching either: ParseWireLivelinessKind
// names all three wire kinds, and ParticipantState tracks per-participant
// leases where DATA never asserts and only AssertParticipantAt advances
// the lease. Delivery gating reuses core.GateDelivery so participant
// writers hold and drop exactly like other manual writers.

// Wire liveliness kinds per DDS 1.4 section 2.2.3.8.
const (
	// wireLivelinessAutomatic asserts without user action.
	wireLivelinessAutomatic uint32 = 0
	// wireLivelinessManualByParticipant needs a participant assert.
	wireLivelinessManualByParticipant uint32 = 1
	// wireLivelinessManualByTopic needs a per-topic assert.
	wireLivelinessManualByTopic uint32 = 2
)

// ParseWireLivelinessKind names one wire liveliness kind.
//
// Unlike ReverseQoS it accepts manual by participant: kind 1 maps to the
// participant tracker here instead of refusing. Unknown kinds report an
// error joining the refusal sentinel so callers fail closed.
func ParseWireLivelinessKind(kind uint32) (string, error) {
	switch kind {
	case wireLivelinessAutomatic:
		return "automatic", nil
	case wireLivelinessManualByParticipant:
		return "manual_by_participant", nil
	case wireLivelinessManualByTopic:
		return "manual_by_topic", nil
	default:
		return "", fmt.Errorf("dds: wire liveliness kind %d is unknown: %w", kind, ErrLivelinessExpired)
	}
}

// IsManualByParticipant reports whether a wire kind needs participant assert.
func IsManualByParticipant(kind uint32) bool {
	return kind == wireLivelinessManualByParticipant
}

// participantRecord is one tracked participant lease.
type participantRecord struct {
	// lastAssert is the newest assert tick in nanoseconds.
	lastAssert int64
	// state is the last computed stage.
	state core.LivelinessState
	// generation counts stage transitions.
	generation uint64
	// seen is true once an assert arrived.
	seen bool
}

// participantState tracks manual-by-participant leases per participant.
type participantState struct {
	mu           sync.Mutex
	leases       map[string]core.Duration
	participants map[string]*participantRecord
}

// newParticipantState returns an empty participant tracker.
func newParticipantState() *participantState {
	return &participantState{
		leases:       make(map[string]core.Duration),
		participants: make(map[string]*participantRecord),
	}
}

// SetParticipantLease records the lease for one participant.
//
// A zero participant name is ignored so empty reports never install a
// tracker.
func (r *DataReceiver) SetParticipantLease(participant string, lease core.Duration) {
	if participant == "" {
		return
	}

	st := r.readerStateFor().participant
	st.mu.Lock()
	defer st.mu.Unlock()

	st.leases[participant] = lease
	if _, ok := st.participants[participant]; !ok {
		st.participants[participant] = &participantRecord{state: core.LivelinessAlive}
	}
}

// AssertParticipantAt records one manual assert for a participant at now.
//
// It returns false for unknown participants without touching state, so
// stray asserts never install trackers. Recovery to alive bumps the
// generation once; asserts while alive never bump.
func (r *DataReceiver) AssertParticipantAt(participant string, now int64) bool {
	st := r.readerStateFor().participant
	st.mu.Lock()
	defer st.mu.Unlock()

	rec, ok := st.participants[participant]
	if !ok {
		return false
	}

	rec.lastAssert = now
	rec.seen = true

	if rec.state != core.LivelinessAlive {
		rec.state = core.LivelinessAlive
		rec.generation++
	}

	return true
}

// AssertParticipant records one manual assert at the receiver clock.
func (r *DataReceiver) AssertParticipant(participant string) bool {
	return r.AssertParticipantAt(participant, r.receiverNow()().UnixNano())
}

// ParticipantState returns the lease stage for one participant at now.
//
// Unknown participants report alive so unregistered writers pass through
// instead of dropping on a missing lease. Unconstrained leases always
// report alive. Backward time rebases quietly to alive.
func (r *DataReceiver) ParticipantState(participant string, now int64) core.LivelinessState {
	st := r.readerStateFor().participant
	st.mu.Lock()
	defer st.mu.Unlock()

	rec, ok := st.participants[participant]
	if !ok || !rec.seen {
		return core.LivelinessAlive
	}

	lease := st.leases[participant]
	if lease.IsUnspecified() || lease.IsInfinite() {
		rec.state = core.LivelinessAlive

		return rec.state
	}

	if lease.Seconds < 0 {
		rec.state = core.LivelinessAlive

		return rec.state
	}

	if now < rec.lastAssert {
		rec.lastAssert = now
		rec.state = core.LivelinessAlive

		return rec.state
	}

	total := lease.TotalNanoseconds()
	elapsed := now - rec.lastAssert
	want := core.LivelinessExpired

	if elapsed < total {
		want = core.LivelinessAlive
	} else if elapsed < 2*total {
		want = core.LivelinessStale
	}

	if want != rec.state {
		rec.state = want
		rec.generation++
	}

	return rec.state
}

// ParticipantGeneration returns the transition count for one participant.
func (r *DataReceiver) ParticipantGeneration(participant string) uint64 {
	st := r.readerStateFor().participant
	st.mu.Lock()
	defer st.mu.Unlock()

	rec, ok := st.participants[participant]
	if !ok {
		return 0
	}

	return rec.generation
}

// GateParticipantDelivery gates one sample by participant lease state.
//
// It reuses core.GateDelivery so participant writers hold while stale and
// drop when expired exactly like other manual writers.
func GateParticipantDelivery(state core.LivelinessState, deadlineMissed bool) core.GateOutcome {
	return core.GateDelivery(state, deadlineMissed)
}
