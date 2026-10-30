// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package dds

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/podomy/weld/internal/core"
	"github.com/podomy/weld/internal/types"
)

// ErrLivelinessHold reports a sample held because its writer is stale.
//
// A hold is neither a delivery nor a drop: the writer may recover, and the
// sample must not count against drop dashboards. It propagates from take
// paths without counting.
var ErrLivelinessHold = errors.New("dds: liveliness hold")

// contentFilterEntry pairs one compiled filter with its type descriptor.
//
// The descriptor comes from the type registry at subscribe time so the
// take path never parses type metadata per sample.
type contentFilterEntry struct {
	// filter is the compiled expression, evaluated per sample.
	filter *ContentFilter
	// desc describes the subscribed type for field reads.
	desc *types.MessageMembers
}

// SetContentFilter registers a compiled content filter for one ROS topic.
//
// A nil filter clears the entry. It self-locks and is safe for concurrent
// use. Registration only affects future takes; in-flight takes keep the
// entry they loaded.
func (r *DataReceiver) SetContentFilter(topic string, filter *ContentFilter, desc *types.MessageMembers) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.contentFilters == nil {
		r.contentFilters = make(map[string]*contentFilterEntry)
	}

	if filter == nil {
		delete(r.contentFilters, topic)

		return
	}

	r.contentFilters[topic] = &contentFilterEntry{filter: filter, desc: desc}
}

// ClearContentFilter removes the content filter for one ROS topic.
func (r *DataReceiver) ClearContentFilter(topic string) {
	r.SetContentFilter(topic, nil, nil)
}

// SetLivelinessTracker registers a liveliness tracker for one writer.
//
// The tracker is consulted on every take from that writer: automatic
// writers record each sample, and expired writers drop loudly while stale
// writers hold without counting. A nil tracker clears the entry.
func (r *DataReceiver) SetLivelinessTracker(guid WriterGUID, tracker *core.LivelinessTracker) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.lively == nil {
		r.lively = make(map[WriterGUID]*core.LivelinessTracker)
	}

	if tracker == nil {
		delete(r.lively, guid)

		return
	}

	r.lively[guid] = tracker
}

// SetDeadlineTracker registers a deadline tracker for one ROS topic.
//
// Deadline misses drop loudly naming the deadline policy. A nil tracker
// clears the entry.
func (r *DataReceiver) SetDeadlineTracker(topic string, tracker *core.DeadlineTracker) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.deadlines == nil {
		r.deadlines = make(map[string]*core.DeadlineTracker)
	}

	if tracker == nil {
		delete(r.deadlines, topic)

		return
	}

	r.deadlines[topic] = tracker
}

// lookupDescriptorForIDL resolves a type descriptor from an IDL name.
//
// The IDL "pkg__msg__Name" splits into namespace "pkg__msg" and name
// "Name", matching RegisterMessageMembers convention. Unregistered types
// report an error so callers skip descriptor-dependent handling and keep
// plain delivery.
func lookupDescriptorForIDL(typeIDL string) (*types.MessageMembers, error) {
	idx := strings.LastIndex(typeIDL, "__")
	if idx <= 0 || idx+2 >= len(typeIDL) {
		return nil, fmt.Errorf("dds: idl %q has no namespace split", typeIDL)
	}

	desc, err := types.LookupMessageMembers(typeIDL[:idx], typeIDL[idx+2:])
	if err != nil {
		return nil, fmt.Errorf("dds: lookup descriptor %q: %w", typeIDL, err)
	}

	return desc, nil
}

// writerGUIDKeyed reports whether a writer GUID carries a with-key entity.
//
// The kind byte is the last entity octet: 0x02 marks a keyed writer and
// 0x07 a keyed reader, mirroring EndpointKeyed without an SEDP lookup so
// the take hot path pays no snapshot cost. Unkeyed writers never reach
// keyed handling.
func writerGUIDKeyed(guid WriterGUID) bool {
	kind := guid[15]

	return kind == keyedUserWriterKind || kind == keyedUserReaderKind
}

// countTakeHookReject counts one reasoned hook rejection and wraps it.
//
// Holds carry no reason and propagate uncounted: a hold is neither a
// delivery nor a drop. Reasoned rejections count exactly once under their
// cause so dashboards blame filtering, keying, deadline, or expiry.
func (r *DataReceiver) countTakeHookReject(rej *takeHookReject) error {
	if rej == nil {
		return fmt.Errorf("dds: take hook nil rejection: %w", ErrInvalidFrame)
	}

	if rej.reason != nil {
		r.countDropReason(*rej.reason)
	}

	return rej.err
}

// plainNackState tracks per-writer plain-path repair state.
type plainNackState struct {
	// seen marks writers observed at least once on the plain path.
	seen bool
	// maxSeen is the highest sequence observed, gaps below it NACK once.
	maxSeen uint64
	// lastNacked is the highest NACK base already requested.
	lastNacked uint64
}

// plainNackBaseForSeen computes the NACK base for an already seen writer.
func plainNackBaseForSeen(st *plainNackState, seq uint64) uint64 {
	if seq > st.maxSeen+1 && st.maxSeen+1 > st.lastNacked {
		return st.maxSeen + 1
	}

	return 0
}

// plainNackBaseForFirst records a first sighting and computes the NACK base.
func plainNackBaseForFirst(st *plainNackState, seq uint64) uint64 {
	st.seen = true
	st.maxSeen = seq

	if seq > 1 {
		return 1
	}

	return 0
}

// plainNackNextBase updates tracker state for one sequence and reports base.
func plainNackNextBase(st *plainNackState, seq uint64) uint64 {
	if !st.seen {
		return plainNackBaseForFirst(st, seq)
	}

	base := plainNackBaseForSeen(st, seq)
	if seq > st.maxSeen {
		st.maxSeen = seq
	}

	return base
}

// notePlainSample tracks one plain vendor sample and NACKs gaps best effort.
//
// First sight of a writer at seq K above 1 means 1 through K minus 1
// were missed, typically a late join: NACK base 1 immediately. Later
// gaps NACK the new base once per advance, so repair converges without
// storms. Delivery never blocks on this: the sample always flows and
// NACK send failures only bump demux errors. Unparseable seq skips
// tracking. It self-locks and is safe for concurrent use.
func (r *DataReceiver) notePlainSample(ctx context.Context, guid WriterGUID, seq uint64) {
	if seq == 0 {
		return
	}

	r.mu.Lock()
	if r.plainNack == nil {
		r.plainNack = make(map[WriterGUID]*plainNackState)
	}

	st, ok := r.plainNack[guid]
	if !ok {
		st = &plainNackState{}
		r.plainNack[guid] = st
	}

	base := plainNackNextBase(st, seq)

	if base == 0 || base <= st.lastNacked {
		r.mu.Unlock()
		return
	}

	st.lastNacked = base
	send := r.nackSender
	r.mu.Unlock()

	if send == nil {
		return
	}

	if err := send(ctx, guid, base); err != nil {
		r.countDropReason(DropReasonNackSendFailed)
	}
}

// SetNackSender installs the ACKNACK send hook for plain-path gaps.
//
// The backend wires its user-control send here at join. A nil sender
// disables gap NACKs while delivery flows unchanged.
func (r *DataReceiver) SetNackSender(send func(ctx context.Context, guid WriterGUID, base uint64) error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.nackSender = send
}

// takeHookReject names one take-path rejection with its drop reason.
//
// A nil reason means the sample passes all hooks. Counting stays with the
// caller so framed and plain paths count exactly once under the right
// reason: callers pass the reason to countDropReason, except holds which
// must not count.
type takeHookReject struct {
	// reason is the machine readable drop cause, nil for holds.
	reason *DropReason
	// err is the loud error joining the cause sentinel.
	err error
}

// takeHookSnapshot carries one sample's hook state with no locks held.
//
// Snapshots load under a short RLock and evaluate lock-free: tracker,
// deadline, and filter methods self-lock internally. This split keeps the
// take hot path deadlock-free, since takeDecoded holds the receiver Lock
// across sequencing and sync.RWMutex is not reentrant.
type takeHookSnapshot struct {
	// filter is the topic content filter entry, nil when unfiltered.
	filter *contentFilterEntry
	// tracker is the writer liveliness tracker, nil when untracked.
	tracker *core.LivelinessTracker
	// deadline is the topic deadline tracker, nil when untracked.
	deadline *core.DeadlineTracker
}

// snapshotTakeHooks loads one sample's hook state with lazy tracker creation.
//
// The fast path is one short RLock. When a topic carries a liveliness
// config but the writer has no tracker yet, the tracker is created under
// a full Lock so writers discovered after subscribe are still enforced
// from their first take.
func (r *DataReceiver) snapshotTakeHooks(topic string, guid WriterGUID) takeHookSnapshot {
	r.mu.RLock()
	snap := takeHookSnapshot{
		filter:   r.contentFilters[topic],
		tracker:  r.lively[guid],
		deadline: r.deadlines[topic],
	}
	cfg, hasCfg := r.livelyCfg[topic]
	r.mu.RUnlock()

	if hasCfg && snap.tracker == nil {
		r.mu.Lock()
		if r.lively == nil {
			r.lively = make(map[WriterGUID]*core.LivelinessTracker)
		}
		if _, ok := r.lively[guid]; !ok {
			tracker := core.NewLivelinessTracker(topic, writerGUIDLabel(guid), cfg.kind, cfg.lease, time.Now().UnixNano())
			r.lively[guid] = &tracker
		}
		snap.tracker = r.lively[guid]
		r.mu.Unlock()
	}

	return snap
}

// SetLivelinessTrackerForTopic records the liveliness contract for one
// topic and installs trackers for currently matched writers.
//
// Writers discovered later get trackers lazily on first take, so the
// contract holds without re-subscribing. A nil lease config clears the
// contract; explicit nil writer lists only store the config.
func (r *DataReceiver) SetLivelinessTrackerForTopic(topic string, kind core.LivelinessPolicy, lease core.Duration, writers []WriterGUID) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.livelyCfg == nil {
		r.livelyCfg = make(map[string]livelyConfig)
	}

	r.livelyCfg[topic] = livelyConfig{kind: kind, lease: lease}

	if r.lively == nil {
		r.lively = make(map[WriterGUID]*core.LivelinessTracker)
	}

	for _, guid := range writers {
		if _, ok := r.lively[guid]; ok {
			continue
		}

		tracker := core.NewLivelinessTracker(topic, writerGUIDLabel(guid), kind, lease, time.Now().UnixNano())
		r.lively[guid] = &tracker
	}
}

// SetDeadlineTrackerForTopic records the deadline contract for one topic.
//
// Missed deadlines drop loudly naming the deadline policy. A zero or
// unspecified deadline disables the check while keeping the tracker.
func (r *DataReceiver) SetDeadlineTrackerForTopic(topic string, deadline core.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.deadlines == nil {
		r.deadlines = make(map[string]*core.DeadlineTracker)
	}

	tracker := core.NewDeadlineTracker(topic, "reader", deadline, time.Now().UnixNano())
	r.deadlines[topic] = &tracker
}

// writerGUIDLabel renders a writer GUID for tracker naming.
func writerGUIDLabel(guid WriterGUID) string {
	return hex.EncodeToString(guid[:])
}

// evalTakeHooks runs content filtering, keyed extraction, and liveliness
// gating for one validated sample against a snapshot.
//
// It never counts and never takes receiver locks: the caller counts
// reasoned drops exactly once and propagates holds uncounted. Order is
// filter, then keyed, then liveliness, so dashboards blame the earliest
// applicable cause. A nil return means deliver. It never panics.
//
//nolint:unparam // topic documents the sampled topic for call-site symmetry though the snapshot already carries the filter
func evalTakeHooks(snap takeHookSnapshot, topic, typeIDL string, payload []byte, guid WriterGUID, keyed bool) *takeHookReject {
	if snap.filter != nil && snap.filter.filter != nil {
		if rej := applyContentFilterHook(snap.filter, payload); rej != nil {
			return rej
		}
	}

	if keyed {
		if rej := applyKeyedHook(typeIDL, payload); rej != nil {
			return rej
		}
	}

	return evalLivelinessHook(snap)
}

// applyContentFilterHook evaluates one compiled filter on a raw payload.
//
// Misses and eval errors map to their distinct drop reasons with the
// matching sentinel. A nil descriptor is an eval error, never a pass:
// filtering without type shape would silently misroute.
func applyContentFilterHook(entry *contentFilterEntry, payload []byte) *takeHookReject {
	if entry.desc == nil {
		reason := DropReasonContentFilterEvalError

		return &takeHookReject{
			reason: &reason,
			err:    fmt.Errorf("dds: content filter take without descriptor: %w", ErrContentFilterEvalError),
		}
	}

	ok, reason, err := entry.filter.Apply(payload, entry.desc)
	if err != nil {
		r := DropReasonContentFilterEvalError
		if reason == DropReasonContentFilterMiss {
			r = reason
		}

		return &takeHookReject{reason: &r, err: err}
	}

	if !ok {
		r := reason

		return &takeHookReject{
			reason: &r,
			err:    fmt.Errorf("dds: content filter take miss: %w", ErrContentFilterMiss),
		}
	}

	return nil
}

// applyKeyedHook extracts the instance key for a keyed endpoint sample.
//
// Keyed extraction proves the sample carries a well-formed key; the
// handle itself routes instances upstream. Truncated or malformed keys
// drop loudly with the keyed reason. Endpoints that are not keyed never
// reach here, so unkeyed flow is byte-identical with or without this hook.
func applyKeyedHook(typeIDL string, payload []byte) *takeHookReject {
	desc, err := lookupDescriptorForIDL(typeIDL)
	if err != nil {
		return nil
	}

	if _, err := ExtractKeyBytes(payload, desc); err != nil {
		reason := DropReasonKeyedDecodeFail

		return &takeHookReject{
			reason: &reason,
			err:    fmt.Errorf("dds: take keyed extract: %w", errors.Join(ErrKeyedDecodeFail, err)),
		}
	}

	return nil
}

// evalLivelinessHook records the sample and gates delivery by writer state.
//
// Automatic writers record each sample; manual writers only assert through
// the explicit API. Expired writers drop loudly naming lease duration.
// Stale writers hold without counting. Missing trackers or topics pass
// through untouched, preserving the unwatched take path exactly. The
// snapshot carries preloaded pointers, so this never takes receiver locks.
func evalLivelinessHook(snap takeHookSnapshot) *takeHookReject {
	now := time.Now().UnixNano()

	deadlineMissed := false
	if snap.deadline != nil {
		deadlineMissed = snap.deadline.MissedAt(now)
	}

	if snap.tracker == nil {
		if deadlineMissed {
			reason := DropReasonDeadlineMissed

			return &takeHookReject{
				reason: &reason,
				err:    fmt.Errorf("dds: take deadline missed: %w", ErrDeadlineMissed),
			}
		}

		return nil
	}

	snap.tracker.RecordSampleAt(now)
	outcome := core.GateDelivery(snap.tracker.StateAt(now), deadlineMissed)

	switch outcome.Decision {
	case core.GateDeliver:
		return nil
	case core.GateHold:
		return &takeHookReject{err: fmt.Errorf("dds: take held stale writer: %w", ErrLivelinessHold)}
	case core.GateDrop:
		reason := DropReasonLivelinessExpired

		return &takeHookReject{
			reason: &reason,
			err:    fmt.Errorf("dds: take expired writer: %w", errors.Join(ErrLivelinessExpired, errors.New(outcome.Reason))),
		}
	default:
		reason := DropReasonLivelinessExpired

		return &takeHookReject{
			reason: &reason,
			err:    fmt.Errorf("dds: take expired writer: %w", errors.Join(ErrLivelinessExpired, errors.New(outcome.Reason))),
		}
	}
}
