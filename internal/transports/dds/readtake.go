// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

package dds

import (
	"errors"
	"fmt"

	"github.com/podomy/weld/internal/types"
)

// Read and take variants with conditions, DDS 1.4 sections 2.2.2.5.8-9.
//
// ReadNext returns matching retained samples without removing them and
// marks them read; TakeNext removes what it returns. Both drain pending
// NOT_ALIVE control samples before history so lifecycle edges surface
// first. ReadCondition filters on masks; QueryCondition adds a content
// filter compiled through CompileContentFilter plus an optional Go
// predicate for fields outside the CDR payload.

// ErrNoSample reports a read or take with no matching sample.
var ErrNoSample = errors.New("dds: no sample")

// maxTakeUnlimited caps one read or take batch; non-positive means all.
func normalizeMaxSamples(maxSamples int) int {
	if maxSamples <= 0 {
		return -1
	}

	return maxSamples
}

// matchesMasks reports whether one stored sample matches all three masks.
func matchesMasks(entry *storedSample, sampleMask SampleStateKind, viewMask ViewStateKind, instanceMask InstanceStateKind) bool {
	state := SampleStateRead
	if !entry.read {
		state = SampleStateNotRead
	}

	if sampleMask != SampleStateAny && state != sampleMask {
		return false
	}

	view := entry.sample.Info.ViewState
	if viewMask != ViewStateAny && view != viewMask {
		return false
	}

	instance := entry.sample.Info.InstanceState
	if instanceMask != InstanceStateAny && (instance&instanceMask) == 0 {
		return false
	}

	return true
}

// collectMatching returns up to maxSamples matching history entries oldest first.
//
// Control samples drain first in FIFO order; each drained control counts
// against maxSamples. Read marks returned history read without removing;
// take removes returned history. The returned batch is ordered per the
// destination order rule (reception by default, source timestamp when
// BY_SOURCE_TIMESTAMP) and carries per-collection ranks per DDS 1.4 section
// 2.2.2.5.1.6. The caller must hold st.mu.
func (st *readerState) collectMatching(sampleMask SampleStateKind, viewMask ViewStateKind, instanceMask InstanceStateKind, match func(SampledMessage) bool, maxSamples int, take bool) []SampledMessage {
	limit := normalizeMaxSamples(maxSamples)
	out := make([]SampledMessage, 0)

	out = st.drainControlsLocked(out, sampleMask, viewMask, instanceMask, match, limit)
	out = st.scanHistoryLocked(out, sampleMask, viewMask, instanceMask, match, limit, take)
	out = st.orderLife5Locked(out)
	st.assignRanksLocked(out)

	return out
}

// liveLife5ViewLocked reports the live view for one handle.
//
// It returns the stored fallback when no instance tracks the handle.
func (st *readerState) liveLife5ViewLocked(handle [16]byte, fallback ViewStateKind) ViewStateKind {
	if rec, ok := st.instances[handle]; ok {
		return rec.view
	}

	return fallback
}

// flipLife5ViewForControlLocked marks one control delivery as viewed.
//
// It flips the instance and retained samples to not new when the control
// belongs to the most recent generation.
func (st *readerState) flipLife5ViewForControlLocked(sample SampledMessage, kept []SampledMessage) {
	rec, ok := st.instances[sample.Info.InstanceHandle]
	if !ok {
		return
	}

	sampleSum := sample.Info.DisposedGenerationCount + sample.Info.NoWritersGenerationCount
	recSum := rec.disposedCount + rec.noWritersCount
	if sampleSum != recSum {
		return
	}

	rec.view = ViewStateNotNew
	for _, entry := range st.history {
		if entry.sample.Info.InstanceHandle == sample.Info.InstanceHandle {
			entry.sample.Info.ViewState = ViewStateNotNew
		}
	}

	for i := range kept {
		if kept[i].Info.InstanceHandle == sample.Info.InstanceHandle {
			kept[i].Info.ViewState = ViewStateNotNew
		}
	}
}

// drainControlsLocked appends matching control samples up to limit.
//
// Non-matching controls are skipped in place so a non-matching head never
// blocks a matching tail; FIFO order is preserved for both the returned
// batch and the retained queue. View masks compare live instance view and
// returned views snapshot live before flipping to not new. The caller must
// hold st.mu.
func (st *readerState) drainControlsLocked(out []SampledMessage, sampleMask SampleStateKind, viewMask ViewStateKind, instanceMask InstanceStateKind, match func(SampledMessage) bool, limit int) []SampledMessage {
	kept := make([]SampledMessage, 0, len(st.controls))
	for _, next := range st.controls {
		if limit >= 0 && len(out) >= limit {
			kept = append(kept, next)

			continue
		}
		liveView := st.liveLife5ViewLocked(next.Info.InstanceHandle, next.Info.ViewState)
		if viewMask != ViewStateAny && liveView != viewMask {
			kept = append(kept, next)

			continue
		}
		if !controlMatches(next.Info, sampleMask, ViewStateAny, instanceMask) {
			kept = append(kept, next)

			continue
		}
		if match != nil && !match(next) {
			kept = append(kept, next)

			continue
		}
		// Controls never consume time-filter budget per DDS 1.4 2.2.3.12.
		next.Info.ViewState = liveView
		st.flipLife5ViewForControlLocked(next, kept)
		out = append(out, next)
	}
	for i := range st.controls {
		st.controls[i] = SampledMessage{}
	}
	st.controls = kept

	return out
}

// controlMatches reports whether one control info matches all three masks.
func controlMatches(info SampleInfo, sampleMask SampleStateKind, viewMask ViewStateKind, instanceMask InstanceStateKind) bool {
	state := SampleStateRead
	if info.SampleState == SampleStateNotRead {
		state = SampleStateNotRead
	}

	if sampleMask != SampleStateAny && state != sampleMask {
		return false
	}

	if viewMask != ViewStateAny && info.ViewState != viewMask {
		return false
	}

	return instanceMask == InstanceStateAny || (info.InstanceState&instanceMask) != 0
}

// historyVerdict selects keep, drop, or deliver for one history entry.
type historyVerdict int

// History verdicts for scan paths.
const (
	// historyKeep retains the entry without delivering it.
	historyKeep historyVerdict = iota
	// historyDrop removes the entry without delivering it.
	historyDrop
	// historyDeliver appends the entry to the output batch.
	historyDeliver
)

// timeLife5PassForEntryLocked checks time filtering for one history entry.
//
// Already-read entries skip the check since their delivery already
// advanced the watermark. Unread entries advance on pass and count
// filtered on fail. The time basis is the source timestamp when known,
// else the reception time: source order reflects the writer cadence
// named by DDS 1.4 section 2.2.3.12, while v1 DATA frames and plain
// samples carry no source stamp and fall back to reception. Only
// deliverable entries reach here: ownership gating runs first, so a
// suppressed sample never moves the watermark, and controls never reach
// this path.
func (st *readerState) timeLife5PassForEntryLocked(entry *storedSample, take bool) historyVerdict {
	if entry.read {
		return historyDeliver
	}

	nowNanos := entry.sample.Info.ReceptionTimestamp.UnixNano()
	if !entry.sample.Info.SourceTimestamp.IsZero() {
		nowNanos = entry.sample.Info.SourceTimestamp.UnixNano()
	}

	if st.timeFilterPassLocked(entry.sample.Info.InstanceHandle, nowNanos) {
		return historyDeliver
	}

	if take {
		return historyDrop
	}

	return historyKeep
}

// verdictHistoryEntry applies masks, query, ownership, time filter, and
// per-instance bounds for one entry. Time-filtered takes drop while reads
// keep for a later take. View masks compare live instance view. The caller
// must hold st.mu.
func (st *readerState) verdictHistoryEntry(entry *storedSample, out []SampledMessage, sampleMask SampleStateKind, viewMask ViewStateKind, instanceMask InstanceStateKind, match func(SampledMessage) bool, take bool) historyVerdict {
	liveView := entry.sample.Info.ViewState
	if rec, ok := st.instances[entry.sample.Info.InstanceHandle]; ok {
		liveView = rec.view
	}
	if viewMask != ViewStateAny && liveView != viewMask {
		return historyKeep
	}
	if !matchesMasks(entry, sampleMask, ViewStateAny, instanceMask) {
		return historyKeep
	}

	if match != nil && !match(entry.sample) {
		return historyKeep
	}

	if !st.ownershipDeliverableLocked(entry.sample) {
		return historyKeep
	}

	if verdict := st.timeLife5PassForEntryLocked(entry, take); verdict != historyDeliver {
		return verdict
	}

	if st.perInstanceOutputBoundedLocked(out, entry.sample.Info.InstanceHandle) {
		return historyKeep
	}

	return historyDeliver
}

// deliverLife5TakeLocked appends one take delivery and flips view.
//
// It snapshots live view before flipping to not new on most-recent
// generations.
func (st *readerState) deliverLife5TakeLocked(entry *storedSample, out []SampledMessage, kept []*storedSample) ([]SampledMessage, []*storedSample) {
	liveView := st.liveLife5ViewLocked(entry.sample.Info.InstanceHandle, entry.sample.Info.ViewState)
	delivered := entry.sample
	delivered.Info.ViewState = liveView
	out = append(out, delivered)
	st.flipLife5ViewForHistoryLocked(entry)

	return out, kept
}

// deliverLife5ReadLocked appends one read copy and flips view.
//
// It marks the stored entry read while the copy carries read state.
func (st *readerState) deliverLife5ReadLocked(entry *storedSample, out []SampledMessage, kept []*storedSample) ([]SampledMessage, []*storedSample) {
	liveView := st.liveLife5ViewLocked(entry.sample.Info.InstanceHandle, entry.sample.Info.ViewState)
	copied := entry.sample
	copied.Info.SampleState = SampleStateRead
	copied.Info.ViewState = liveView
	out = append(out, copied)
	entry.read = true
	kept = append(kept, entry)
	st.flipLife5ViewForHistoryLocked(entry)

	return out, kept
}

// flipLife5ViewForHistoryLocked flips view on most-recent deliveries.
func (st *readerState) flipLife5ViewForHistoryLocked(entry *storedSample) {
	rec, ok := st.instances[entry.sample.Info.InstanceHandle]
	if !ok {
		return
	}

	sampleSum := entry.sample.Info.DisposedGenerationCount + entry.sample.Info.NoWritersGenerationCount
	recSum := rec.disposedCount + rec.noWritersCount
	if sampleSum != recSum {
		return
	}

	rec.view = ViewStateNotNew
	if entry.sample.Info.ViewState == ViewStateNew {
		entry.sample.Info.ViewState = ViewStateNotNew
	}
}

// flipLife5DeliveredTakeLocked flips view for one just-delivered immediate take.
//
// Immediate takes are access like the collect path: after ownership plus
// time-filter gating passes, the instance counts as viewed for
// most-recent generations only. Older generations never flip. The stored
// copy flips with the record so retained history matches live view. The
// caller must hold st.mu.
func (st *readerState) flipLife5DeliveredTakeLocked(info SampleInfo) {
	rec, ok := st.instances[info.InstanceHandle]
	if !ok {
		return
	}

	sampleSum := info.DisposedGenerationCount + info.NoWritersGenerationCount
	if sampleSum != rec.disposedCount+rec.noWritersCount {
		return
	}

	rec.view = ViewStateNotNew

	for _, entry := range st.history {
		if entry == nil {
			continue
		}

		if entry.sample.Info.InstanceHandle == info.InstanceHandle &&
			entry.sample.Info.SampleSequence == info.SampleSequence &&
			entry.sample.Info.ViewState == ViewStateNew {
			entry.sample.Info.ViewState = ViewStateNotNew
		}
	}
}

// scanHistoryLocked appends matching history entries up to limit.
//
// Read marks the returned copies read without mutating storage: the stored
// entry keeps its NOT_READ info while the copy carries READ, so
// TakeSampled reporting and SampleHistory stay on the pre-take state and
// only the read flag tracks delivery. Take drops returned history. The
// caller must hold st.mu.
func (st *readerState) scanHistoryLocked(out []SampledMessage, sampleMask SampleStateKind, viewMask ViewStateKind, instanceMask InstanceStateKind, match func(SampledMessage) bool, limit int, take bool) []SampledMessage {
	kept := st.history[:0]

	for _, entry := range st.history {
		if limit >= 0 && len(out) >= limit {
			kept = append(kept, entry)

			continue
		}

		verdict := st.verdictHistoryEntry(entry, out, sampleMask, viewMask, instanceMask, match, take)
		if verdict == historyDrop {
			continue
		}

		if verdict == historyKeep {
			kept = append(kept, entry)

			continue
		}

		if take {
			out, _ = st.deliverLife5TakeLocked(entry, out, kept)

			continue
		}

		out, kept = st.deliverLife5ReadLocked(entry, out, kept)
	}

	if take {
		for i := len(kept); i < len(st.history); i++ {
			st.history[i] = nil
		}

		st.history = kept
	}

	return out
}

// ReadNext returns up to maxSamples matching retained samples without removing.
//
// A non-positive maxSamples means unlimited. It reports ErrNoSample when
// nothing matches. Returned history is marked read.
func (r *DataReceiver) ReadNext(maxSamples int, sampleMask SampleStateKind, viewMask ViewStateKind, instanceMask InstanceStateKind) ([]SampledMessage, error) {
	return r.collectAndNotifyLife5(sampleMask, viewMask, instanceMask, maxSamples, false, "dds: read next")
}

// TakeNext returns up to maxSamples matching retained samples and removes them.
//
// A non-positive maxSamples means unlimited. It reports ErrNoSample when
// nothing matches.
func (r *DataReceiver) TakeNext(maxSamples int, sampleMask SampleStateKind, viewMask ViewStateKind, instanceMask InstanceStateKind) ([]SampledMessage, error) {
	return r.collectAndNotifyLife5(sampleMask, viewMask, instanceMask, maxSamples, true, "dds: take next")
}

// collectAndNotifyLife5 collects matching samples, notifies once per delivery,
// and reports ErrNoSample when empty.
func (r *DataReceiver) collectAndNotifyLife5(sampleMask SampleStateKind, viewMask ViewStateKind, instanceMask InstanceStateKind, maxSamples int, take bool, op string) ([]SampledMessage, error) {
	nowNanos := r.receiverNow()().UnixNano()
	r.sweepAllLifespans(nowNanos)
	st := r.readerStateFor()
	st.mu.Lock()
	st.purgeLife5NotAliveLocked(nowNanos)

	out := st.collectMatching(sampleMask, viewMask, instanceMask, nil, maxSamples, take)
	if len(out) == 0 {
		st.mu.Unlock()

		return nil, fmt.Errorf("%s: %w", op, ErrNoSample)
	}

	st.mu.Unlock()

	for _, sample := range out {
		r.NotifyData(sample.Msg.TopicName, sample)
	}

	return out, nil
}

// ReadCondition filters retained samples on sample, view, and instance masks.
//
// A nil condition matches everything. Conditions are stateless values:
// Trigger and Read evaluate against the current receiver history.
type ReadCondition struct {
	// SampleMask selects read versus not-read samples.
	SampleMask SampleStateKind
	// ViewMask selects new versus not-new views.
	ViewMask ViewStateKind
	// InstanceMask selects alive versus not-alive instances.
	InstanceMask InstanceStateKind
}

// NewReadCondition returns a mask filter for retained samples.
func NewReadCondition(sampleMask SampleStateKind, viewMask ViewStateKind, instanceMask InstanceStateKind) ReadCondition {
	return ReadCondition{SampleMask: sampleMask, ViewMask: viewMask, InstanceMask: instanceMask}
}

// triggerLife5MatchesLocked reports whether one history entry matches.
//
// View masks compare live instance view.
func (st *readerState) triggerLife5MatchesLocked(entry *storedSample, sampleMask SampleStateKind, viewMask ViewStateKind, instanceMask InstanceStateKind) bool {
	liveView := st.liveLife5ViewLocked(entry.sample.Info.InstanceHandle, entry.sample.Info.ViewState)
	if viewMask != ViewStateAny && liveView != viewMask {
		return false
	}

	return matchesMasks(entry, sampleMask, ViewStateAny, instanceMask)
}

// Trigger reports whether any retained sample matches the condition.
//
// View masks compare live instance view so a viewed instance no longer
// matches new-only triggers.
func (c ReadCondition) Trigger(r *DataReceiver) bool {
	st := r.readerStateFor()
	st.mu.Lock()
	defer st.mu.Unlock()

	for _, entry := range st.history {
		if st.triggerLife5MatchesLocked(entry, c.SampleMask, c.ViewMask, c.InstanceMask) {
			return true
		}
	}

	for _, control := range st.controls {
		state := SampleStateRead
		if control.Info.SampleState == SampleStateNotRead {
			state = SampleStateNotRead
		}
		liveView := control.Info.ViewState
		if rec, ok := st.instances[control.Info.InstanceHandle]; ok {
			liveView = rec.view
		}

		if (c.SampleMask == SampleStateAny || state == c.SampleMask) &&
			(c.ViewMask == ViewStateAny || liveView == c.ViewMask) &&
			(c.InstanceMask == InstanceStateAny || control.Info.InstanceState == c.InstanceMask) {
			return true
		}
	}

	return false
}

// Read returns up to maxSamples matching the condition without removing.
func (c ReadCondition) Read(r *DataReceiver, maxSamples int) ([]SampledMessage, error) {
	return r.collectAndNotifyLife5(c.SampleMask, c.ViewMask, c.InstanceMask, maxSamples, false, "dds: read condition")
}

// Take returns up to maxSamples matching the condition and removes them.
func (c ReadCondition) Take(r *DataReceiver, maxSamples int) ([]SampledMessage, error) {
	return r.collectAndNotifyLife5(c.SampleMask, c.ViewMask, c.InstanceMask, maxSamples, true, "dds: read condition take")
}

// QueryCondition filters retained samples on masks plus content.
//
// The expression compiles through CompileContentFilter against desc, so
// query syntax matches subscription filtering exactly. Predicate adds an
// optional Go check over the full sampled message for fields outside the
// CDR payload, such as handles or timestamps; nil means no extra check.
type QueryCondition struct {
	// ReadCondition carries the state masks.
	ReadCondition
	// filter is the compiled content expression, nil for masks only.
	filter *ContentFilter
	// desc describes the filtered type for field reads.
	desc *types.MessageMembers
	// predicate is the optional extra Go check, nil for none.
	predicate func(SampledMessage) bool
}

// NewQueryCondition compiles one query over retained samples.
//
// Expression parameters bind before parsing per CompileContentFilter. A
// nil desc with a non-empty expression reports an error: filtering
// without type shape would silently misroute.
func NewQueryCondition(expr string, desc *types.MessageMembers, sampleMask SampleStateKind, viewMask ViewStateKind, instanceMask InstanceStateKind, params ...string) (*QueryCondition, error) {
	cond := &QueryCondition{ReadCondition: NewReadCondition(sampleMask, viewMask, instanceMask)}

	if expr == "" {
		return cond, nil
	}

	if desc == nil {
		return nil, fmt.Errorf("dds: query condition without descriptor: %w", ErrContentFilterEvalError)
	}

	filter, err := CompileContentFilter(expr, params...)
	if err != nil {
		return nil, err
	}

	cond.filter = filter
	cond.desc = desc

	return cond, nil
}

// NewQueryConditionFunc returns a mask plus Go-predicate query.
func NewQueryConditionFunc(predicate func(SampledMessage) bool, sampleMask SampleStateKind, viewMask ViewStateKind, instanceMask InstanceStateKind) *QueryCondition {
	return &QueryCondition{
		ReadCondition: NewReadCondition(sampleMask, viewMask, instanceMask),
		predicate:     predicate,
	}
}

// matches runs the filter plus predicate for one sample.
func (c *QueryCondition) matches(sample SampledMessage) bool {
	if c == nil {
		return true
	}

	if c.filter != nil {
		matched, _, err := c.filter.Apply(sample.Msg.Payload, c.desc)
		if err != nil {
			return false
		}

		if !matched {
			return false
		}
	}

	if c.predicate != nil && !c.predicate(sample) {
		return false
	}

	return true
}

// queryLife5ViewPassLocked reports whether one sample passes the view mask.
//
// It compares live instance view so viewed instances no longer match
// new-only triggers.
func (st *readerState) queryLife5ViewPassLocked(handle [16]byte, fallback ViewStateKind, want ViewStateKind) bool {
	if want == ViewStateAny {
		return true
	}

	return st.liveLife5ViewLocked(handle, fallback) == want
}

// Trigger reports whether any retained sample matches masks plus query.
func (c *QueryCondition) Trigger(r *DataReceiver) bool {
	if c == nil {
		return false
	}

	st := r.readerStateFor()
	st.mu.Lock()
	defer st.mu.Unlock()

	for _, entry := range st.history {
		if !st.queryLife5ViewPassLocked(entry.sample.Info.InstanceHandle, entry.sample.Info.ViewState, c.ViewMask) {
			continue
		}
		probed := *c
		probed.ViewMask = ViewStateAny
		if queryStoredMatches(&probed, entry.sample, entry.read) {
			return true
		}
	}

	for _, control := range st.controls {
		if !st.queryLife5ViewPassLocked(control.Info.InstanceHandle, control.Info.ViewState, c.ViewMask) {
			continue
		}
		read := control.Info.SampleState != SampleStateNotRead
		probed := *c
		probed.ViewMask = ViewStateAny
		if queryStoredMatches(&probed, control, read) {
			return true
		}
	}

	return false
}

// queryStoredMatches reports whether one stored sample matches the query.
func queryStoredMatches(c *QueryCondition, sample SampledMessage, read bool) bool {
	state := SampleStateRead
	if !read {
		state = SampleStateNotRead
	}

	if c.SampleMask != SampleStateAny && state != c.SampleMask {
		return false
	}

	if c.ViewMask != ViewStateAny && sample.Info.ViewState != c.ViewMask {
		return false
	}

	if c.InstanceMask != InstanceStateAny && (sample.Info.InstanceState&c.InstanceMask) == 0 {
		return false
	}

	return c.matches(sample)
}

// Read returns up to maxSamples matching masks plus query without removing.
//
// Eval failures surface as content-filter eval errors distinct from
// no-match, so callers tell malformed samples apart from quiet filtering.
func (c *QueryCondition) Read(r *DataReceiver, maxSamples int) ([]SampledMessage, error) {
	return r.queryCollectLife5(c, maxSamples, false, "dds: query condition read")
}

// queryCollectLife5 collects query samples, surfaces eval errors distinctly,
// notifies once per delivery, and reports no-match when empty.
func (r *DataReceiver) queryCollectLife5(c *QueryCondition, maxSamples int, take bool, op string) ([]SampledMessage, error) {
	if c == nil {
		return nil, fmt.Errorf("%s on nil: %w", op, ErrNoSample)
	}

	st := r.readerStateFor()
	nowNanos := r.receiverNow()().UnixNano()
	r.sweepAllLifespans(nowNanos)
	st.mu.Lock()
	st.purgeLife5NotAliveLocked(nowNanos)

	out := st.collectMatching(c.SampleMask, c.ViewMask, c.InstanceMask, c.matches, maxSamples, take)
	if len(out) == 0 {
		if err := st.queryEvalLife5Err(c); err != nil {
			st.mu.Unlock()

			return nil, err
		}

		st.mu.Unlock()

		return nil, fmt.Errorf("%s: %w", op, ErrNoSample)
	}

	st.mu.Unlock()

	for _, sample := range out {
		r.NotifyData(sample.Msg.TopicName, sample)
	}

	return out, nil
}

// queryEvalLife5Err reports the first content-filter eval failure among
// mask-matching history and controls, or nil when none failed.
//
// It lets query reads surface eval errors distinctly from quiet no-match.
func (st *readerState) queryEvalLife5Err(c *QueryCondition) error {
	if c == nil || c.filter == nil {
		return nil
	}

	if err := st.queryLife5HistoryErrLocked(c); err != nil {
		return err
	}

	return st.queryLife5ControlsErrLocked(c)
}

// queryLife5MasksPassLocked reports whether one sample passes query masks.
//
// View masks compare live instance view.
func (st *readerState) queryLife5MasksPassLocked(handle [16]byte, view ViewStateKind, instanceState InstanceStateKind, read bool, c *QueryCondition) bool {
	if !st.queryLife5ViewPassLocked(handle, view, c.ViewMask) {
		return false
	}

	state := SampleStateRead
	if !read {
		state = SampleStateNotRead
	}

	if c.SampleMask != SampleStateAny && state != c.SampleMask {
		return false
	}

	return c.InstanceMask == InstanceStateAny || (instanceState&c.InstanceMask) != 0
}

// queryLife5HistoryErrLocked scans history for the first eval failure.
func (st *readerState) queryLife5HistoryErrLocked(c *QueryCondition) error {
	for _, entry := range st.history {
		if !st.queryLife5MasksPassLocked(entry.sample.Info.InstanceHandle, entry.sample.Info.ViewState, entry.sample.Info.InstanceState, entry.read, c) {
			continue
		}
		_, _, err := c.filter.Apply(entry.sample.Msg.Payload, c.desc)
		if err != nil && errors.Is(err, ErrContentFilterEvalError) {
			return fmt.Errorf("dds: query condition eval: %w", err)
		}
	}

	return nil
}

// queryLife5ControlsErrLocked scans controls for the first eval failure.
func (st *readerState) queryLife5ControlsErrLocked(c *QueryCondition) error {
	for _, control := range st.controls {
		read := control.Info.SampleState != SampleStateNotRead
		if !st.queryLife5MasksPassLocked(control.Info.InstanceHandle, control.Info.ViewState, control.Info.InstanceState, read, c) {
			continue
		}
		_, _, err := c.filter.Apply(control.Msg.Payload, c.desc)
		if err != nil && errors.Is(err, ErrContentFilterEvalError) {
			return fmt.Errorf("dds: query condition eval: %w", err)
		}
	}

	return nil
}

// Take returns up to maxSamples matching masks plus query and removes them.
//
// Eval failures surface distinctly from no-match like Read.
func (c *QueryCondition) Take(r *DataReceiver, maxSamples int) ([]SampledMessage, error) {
	return r.queryCollectLife5(c, maxSamples, true, "dds: query condition take")
}
