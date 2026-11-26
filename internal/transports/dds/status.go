// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

package dds

import (
	"sync"

	"github.com/podomy/weld/internal/core"
)

// Deadline and liveliness statuses with counters, DDS 1.4 section 2.2.4.
//
// Core trackers detect misses and expiries; these monitors count them into
// DDS status shapes with total plus change counts and last-instance
// handles. Take snapshots reset the change count while keeping the total,
// matching DDS get_status semantics.

// OfferedDeadlineMissedStatus counts writer-side deadline misses.
type OfferedDeadlineMissedStatus struct {
	// TotalCount counts misses since the monitor started.
	TotalCount int64
	// TotalCountChange counts misses since the last snapshot.
	TotalCountChange int64
	// LastInstanceHandle is the instance of the most recent miss.
	LastInstanceHandle [16]byte
}

// RequestedDeadlineMissedStatus counts reader-side deadline misses.
type RequestedDeadlineMissedStatus struct {
	// TotalCount counts misses since the monitor started.
	TotalCount int64
	// TotalCountChange counts misses since the last snapshot.
	TotalCountChange int64
	// LastInstanceHandle is the instance of the most recent miss.
	LastInstanceHandle [16]byte
}

// deadlineMissMonitor counts deadline misses with change tracking.
type deadlineMissMonitor struct {
	mu     sync.Mutex
	total  int64
	change int64
	last   [16]byte
}

// newDeadlineMissMonitor returns an empty miss monitor.
func newDeadlineMissMonitor() *deadlineMissMonitor {
	return &deadlineMissMonitor{}
}

// note records one miss for an instance handle.
func (m *deadlineMissMonitor) note(handle [16]byte) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.total++
	m.change++
	m.last = handle
}

// offeredSnapshot snapshots the monitor as an offered status and resets change.
func (m *deadlineMissMonitor) offeredSnapshot() OfferedDeadlineMissedStatus {
	m.mu.Lock()
	defer m.mu.Unlock()

	out := OfferedDeadlineMissedStatus{TotalCount: m.total, TotalCountChange: m.change, LastInstanceHandle: m.last}
	m.change = 0

	return out
}

// requestedSnapshot snapshots the monitor as a requested status and resets change.
func (m *deadlineMissMonitor) requestedSnapshot() RequestedDeadlineMissedStatus {
	m.mu.Lock()
	defer m.mu.Unlock()

	out := RequestedDeadlineMissedStatus{TotalCount: m.total, TotalCountChange: m.change, LastInstanceHandle: m.last}
	m.change = 0

	return out
}

// NoteOfferedDeadlineMissed records one writer-side deadline miss.
func (r *DataReceiver) NoteOfferedDeadlineMissed(handle [16]byte) {
	r.readerStateFor().offeredMiss.note(handle)
}

// OfferedDeadlineMissedStatus snapshots the writer-side miss status.
func (r *DataReceiver) OfferedDeadlineMissedStatus() OfferedDeadlineMissedStatus {
	return r.readerStateFor().offeredMiss.offeredSnapshot()
}

// NoteRequestedDeadlineMissed records one reader-side deadline miss.
func (r *DataReceiver) NoteRequestedDeadlineMissed(handle [16]byte) {
	r.readerStateFor().requestedMiss.note(handle)
}

// RequestedDeadlineMissedStatus snapshots the reader-side miss status.
func (r *DataReceiver) RequestedDeadlineMissedStatus() RequestedDeadlineMissedStatus {
	return r.readerStateFor().requestedMiss.requestedSnapshot()
}

// CheckRequestedDeadline polls the topic deadline tracker and counts misses.
//
// It runs tracker.Check at now and records one requested miss for the
// topic instance on alert. It resolves the instance by topic without
// building an implicit key from two topic names, so the handle always
// names the alerted topic instance. It reports the alert for listener
// fan-out. A missing tracker means no contract and reports no alert.
func (r *DataReceiver) CheckRequestedDeadline(topic string, now int64) (core.DeadlineAlert, bool) {
	r.mu.RLock()
	tracker := r.deadlines[topic]
	r.mu.RUnlock()

	if tracker == nil {
		return core.DeadlineAlert{}, false
	}

	alert, ok := tracker.Check(now)
	if !ok {
		return core.DeadlineAlert{}, false
	}

	handle := r.instanceHandleForTopic(topic)

	r.NoteRequestedDeadlineMissed(handle)
	r.NotifyDeadline(topic)

	return alert, true
}

// CheckRequestedDeadlineForType polls the deadline tracker and counts misses.
//
// It shares CheckRequestedDeadline polling but attributes the miss to the
// implicit instance for the topic plus IDL type, matching DDS generation
// accounting where the handle names the typed instance. A missing tracker
// reports no alert.
func (r *DataReceiver) CheckRequestedDeadlineForType(topic, typeIDL string, now int64) (core.DeadlineAlert, bool) {
	r.mu.RLock()
	tracker := r.deadlines[topic]
	r.mu.RUnlock()

	if tracker == nil {
		return core.DeadlineAlert{}, false
	}

	alert, ok := tracker.Check(now)
	if !ok {
		return core.DeadlineAlert{}, false
	}

	st := r.readerStateFor()
	st.mu.Lock()
	handle := st.keyIndex[implicitInstanceKey(topic, typeIDL)]
	st.mu.Unlock()

	r.NoteRequestedDeadlineMissed(handle)
	r.NotifyDeadline(topic)

	return alert, true
}

// instanceHandleForTopic resolves the status handle for one topic.
//
// It returns the first tracked instance on the topic else zero, so misses
// attribute without creating records on the poll path.
func (r *DataReceiver) instanceHandleForTopic(topic string) [16]byte {
	st := r.readerStateFor()
	st.mu.Lock()
	defer st.mu.Unlock()

	for _, rec := range st.instances {
		if rec.topic == topic {
			return rec.handle
		}
	}

	return [16]byte{}
}

// LivelinessLostStatus counts writer-side transitions to not alive.
type LivelinessLostStatus struct {
	// TotalCount counts losses since the monitor started.
	TotalCount int64
	// TotalCountChange counts losses since the last snapshot.
	TotalCountChange int64
}

// livelinessLostMonitor counts writer-side liveliness losses.
type livelinessLostMonitor struct {
	mu     sync.Mutex
	total  int64
	change int64
}

// newLivelinessLostMonitor returns an empty loss monitor.
func newLivelinessLostMonitor() *livelinessLostMonitor {
	return &livelinessLostMonitor{}
}

// note records one loss.
func (m *livelinessLostMonitor) note() {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.total++
	m.change++
}

// snapshot reports the loss status and resets change.
func (m *livelinessLostMonitor) snapshot() LivelinessLostStatus {
	m.mu.Lock()
	defer m.mu.Unlock()

	out := LivelinessLostStatus{TotalCount: m.total, TotalCountChange: m.change}
	m.change = 0

	return out
}

// NoteLivelinessLost records one writer-side transition to not alive.
func (r *DataReceiver) NoteLivelinessLost() {
	r.readerStateFor().lost.note()
}

// LivelinessLostStatus snapshots the writer-side loss status.
func (r *DataReceiver) LivelinessLostStatus() LivelinessLostStatus {
	return r.readerStateFor().lost.snapshot()
}

// LivelinessChangedStatus carries reader-side alive counts.
type LivelinessChangedStatus struct {
	// AliveCount counts currently alive writers.
	AliveCount int64
	// NotAliveCount counts currently not-alive writers.
	NotAliveCount int64
	// AliveCountChange counts alive transitions since the last snapshot.
	AliveCountChange int64
	// NotAliveCountChange counts not-alive transitions since the snapshot.
	NotAliveCountChange int64
	// LastPublicationHandle is the writer of the most recent change.
	LastPublicationHandle [16]byte
}

// livelinessChangedMonitor tracks reader-side alive counts.
type livelinessChangedMonitor struct {
	mu           sync.Mutex
	alive        map[[16]byte]bool
	aliveNow     int64
	notAlive     int64
	aliveDiff    int64
	notAliveDiff int64
	last         [16]byte
}

// newLivelinessChangedMonitor returns an empty changed monitor.
func newLivelinessChangedMonitor() *livelinessChangedMonitor {
	return &livelinessChangedMonitor{alive: make(map[[16]byte]bool)}
}

// set records one writer aliveness edge, counting only on change.
//
// It reports true when the edge changed counts, false for repeats.
func (m *livelinessChangedMonitor) set(handle [16]byte, alive bool) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.alive == nil {
		m.alive = make(map[[16]byte]bool)
	}

	prev, seen := m.alive[handle]
	if seen && prev == alive {
		return false
	}

	m.alive[handle] = alive
	m.last = handle

	if !seen {
		if alive {
			m.aliveNow++
			m.aliveDiff++
		} else {
			m.notAlive++
			m.notAliveDiff++
		}

		return true
	}

	if alive {
		m.aliveNow++
		m.notAlive--
		m.aliveDiff++
	} else {
		m.aliveNow--
		m.notAlive++
		m.notAliveDiff++
	}

	return true
}

// remove drops one writer without counting an edge.
func (m *livelinessChangedMonitor) remove(handle [16]byte) {
	m.mu.Lock()
	defer m.mu.Unlock()

	prev, seen := m.alive[handle]
	if !seen {
		return
	}

	delete(m.alive, handle)

	if prev {
		m.aliveNow--
	} else {
		m.notAlive--
	}
}

// snapshot reports the changed status and resets change counts.
func (m *livelinessChangedMonitor) snapshot() LivelinessChangedStatus {
	m.mu.Lock()
	defer m.mu.Unlock()

	out := LivelinessChangedStatus{
		AliveCount:            m.aliveNow,
		NotAliveCount:         m.notAlive,
		AliveCountChange:      m.aliveDiff,
		NotAliveCountChange:   m.notAliveDiff,
		LastPublicationHandle: m.last,
	}
	m.aliveDiff = 0
	m.notAliveDiff = 0

	return out
}

// NoteWriterAlive records one writer aliveness edge for the changed status.
//
// Repeated reports for the same state never bump change counts. It reports
// true when the edge changed counts, so callers notify once per edge.
func (r *DataReceiver) NoteWriterAlive(handle [16]byte, alive bool) bool {
	return r.readerStateFor().changed.set(handle, alive)
}

// ForgetWriter drops one writer from the alive counts without an edge.
func (r *DataReceiver) ForgetWriter(handle [16]byte) {
	r.readerStateFor().changed.remove(handle)
}

// LivelinessChangedStatus snapshots the reader-side alive status.
func (r *DataReceiver) LivelinessChangedStatus() LivelinessChangedStatus {
	return r.readerStateFor().changed.snapshot()
}
