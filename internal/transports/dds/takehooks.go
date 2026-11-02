// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package dds

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"log"
	"strings"

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
// writers hold without counting. A nil tracker clears the entry. Inserts
// hold livelyTrackerCap with arbitrary eviction like the lazy path so
// bulk explicit registration cannot grow the map without bound; unlike
// lazy creation, explicit registration trusts the caller and skips the
// matched-writer gate so trackers can be installed before discovery
// completes.
func (r *DataReceiver) SetLivelinessTracker(guid WriterGUID, tracker *core.LivelinessTracker) {
	r.mu.Lock()

	if r.lively == nil {
		r.lively = make(map[WriterGUID]*core.LivelinessTracker)
	}

	if tracker == nil {
		delete(r.lively, guid)
		r.mu.Unlock()

		return
	}

	var evicted WriterGUID
	evictedOk := false
	if _, ok := r.lively[guid]; !ok && len(r.lively) >= livelyTrackerCap {
		evicted, evictedOk = r.evictLivelyLocked()
	}

	r.lively[guid] = tracker
	r.mu.Unlock()

	if evictedOk {
		log.Printf("dds: lively table full evict guid=%x", evicted)
	}
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
// the take hot path pays no snapshot cost. Kind bytes with 0xC0 bits set
// are builtin entities (see the SEDP 0xC2 writers in sedp.go) and never
// count as keyed. Unkeyed writers never reach keyed handling.
func writerGUIDKeyed(guid WriterGUID) bool {
	kind := guid[15]
	if kind&0xC0 == 0xC0 {
		return false
	}

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

// capPlainNackLocked holds the plain NACK table at the lively cap.
//
// It returns the evicted writer, if any, for the caller to log after
// unlocking: logging never holds the receiver lock. The eviction is an
// arbitrary map entry, not the oldest; the cap bounds memory, not
// fairness. The caller must hold r.mu.
func (r *DataReceiver) capPlainNackLocked(except WriterGUID) (WriterGUID, bool) {
	if _, seen := r.plainNack[except]; seen {
		return WriterGUID{}, false
	}

	if len(r.plainNack) < livelyTrackerCap {
		return WriterGUID{}, false
	}

	for victim := range r.plainNack {
		delete(r.plainNack, victim)

		return victim, true
	}

	return WriterGUID{}, false
}

// plainSampleBaseLocked inserts one plain tracker and computes its NACK base.
//
// First sight of a writer at seq K above 1 means 1 through K minus 1
// were missed, typically a late join: NACK base 1 immediately. Later
// gaps NACK the new base once per advance, so repair converges without
// storms. The caller must hold r.mu for write and must have matched the
// writer under the same lock. It returns the NACK base with the send hook
// plus any evicted writer for post-unlock logging. A zero base means no
// NACK: either the sequence needs none or it does not advance the
// high-water. Insertion gates on matched writers: unknown GUIDs skip
// tracking without growing plainNack. New keys hold livelyTrackerCap with
// arbitrary eviction for post-unlock logging.
func (r *DataReceiver) plainSampleBaseLocked(guid WriterGUID, seq uint64) (uint64, func(ctx context.Context, guid WriterGUID, base uint64) error, WriterGUID, bool) {
	if r.plainNack == nil {
		r.plainNack = make(map[WriterGUID]*plainNackState)
	}

	var victim WriterGUID
	evicted := false
	st, ok := r.plainNack[guid]
	if !ok {
		victim, evicted = r.capPlainNackLocked(guid)
		st = &plainNackState{}
		r.plainNack[guid] = st
	}

	base := plainNackNextBase(st, seq)
	if base == 0 || base <= st.lastNacked {
		return 0, nil, victim, evicted
	}

	return base, r.nackSender, victim, evicted
}

// plainSampleAckLocked advances one plain high-water after a send.
//
// The caller must hold r.mu for write. Smaller or equal bases never
// regress the high-water, so reordered sends stay harmless.
func (r *DataReceiver) plainSampleAckLocked(guid WriterGUID, base uint64) {
	if current, seen := r.plainNack[guid]; seen && base > current.lastNacked {
		current.lastNacked = base
	}
}

// notePlainSample tracks one plain vendor sample and NACKs gaps best effort.
//
// Delivery never blocks on this: the sample always flows and NACK send
// failures only bump demux errors. The high-water lastNacked advances only
// after a successful send, and a nil sender never advances it, so failed or
// disabled NACKs do not mask the gap. It self-locks and is safe for
// concurrent use. It delegates locked insert plus base computation to
// plainSampleBaseLocked so the cyclomatic cost stays under the cap, logs
// evictions after unlock, sends without holding the receiver lock, then
// advances the high-water.
func (r *DataReceiver) notePlainSample(ctx context.Context, guid WriterGUID, seq uint64) {
	if seq == 0 {
		return
	}

	if !r.isMatchedWriter(guid) {
		return
	}

	r.mu.Lock()
	if !r.isMatchedWriterLocked(guid) {
		r.mu.Unlock()
		return
	}
	base, send, victim, evicted := r.plainSampleBaseLocked(guid, seq)
	r.mu.Unlock()
	if evicted {
		log.Printf("dds: plain nack table full evict guid=%x", victim)
	}

	if base == 0 {
		return
	}

	if send == nil {
		return
	}

	err := send(ctx, guid, base)
	if err != nil {
		r.countDropReason(DropReasonNackSendFailed)

		return
	}

	r.mu.Lock()
	r.plainSampleAckLocked(guid, base)
	r.mu.Unlock()
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

// livelyTrackerCap bounds the lazy tracker map under writer churn.
//
// Creation is rare (one per matched writer) but unauthenticated GUIDs must
// never grow the map without bound; at cap one entry is evicted to make
// room. The bound matches provisionalCap rationale: discovery churn stays
// bounded while legitimate writers keep enforcement.
const livelyTrackerCap = 1024

// isMatchedWriter reports whether a writer GUID names a known writer.
//
// It returns true for confirmed SEDP remotes plus provisional placeholders
// in SEDP or the receiver fast-path map. Unknown GUIDs return false so
// lazy tracker creation never admits spoofed writers without a matching
// SEDP announce. It self-locks and is safe for concurrent use.
func (r *DataReceiver) isMatchedWriter(guid WriterGUID) bool {
	if _, ok := r.remoteForGUID(guid); ok {
		return true
	}
	if _, ok := r.ProvisionalForGUID(guid); ok {
		return true
	}

	return false
}

// isMatchedWriterLocked reports whether a writer GUID names a known writer.
//
// It shares the isMatchedWriter rule without taking r.mu: the caller must
// hold r.mu for write, so a match observed here cannot change before the
// caller acts on it. Never call the locking variant while holding the
// write lock; Go RWMutex read-locking under a held write lock deadlocks.
func (r *DataReceiver) isMatchedWriterLocked(guid WriterGUID) bool {
	if _, ok := r.remoteForGUIDLocked(guid); ok {
		return true
	}
	_, ok := r.provisionalForGUIDLocked(guid)

	return ok
}

// evictLivelyLocked drops one tracker to hold the cap. Caller holds r.mu.
//
// Eviction picks an arbitrary entry: the cap bounds memory, not fairness.
// It returns the evicted writer so the caller logs loudly after unlocking;
// logging never holds the receiver lock.
func (r *DataReceiver) evictLivelyLocked() (WriterGUID, bool) {
	for id := range r.lively {
		delete(r.lively, id)

		return id, true
	}

	return WriterGUID{}, false
}

// ensureLivelyLocked installs the liveliness tracker for one matched
// writer when the topic carries a contract, evicting an arbitrary entry
// on cap since the bound is memory, not fairness.
//
// The match is re-checked under the held write lock so a writer removed
// between the snapshot and this call never installs a tracker. It returns
// the tracker plus any evicted writer for the caller to log after unlock;
// logging never holds the receiver lock. The caller supplies nowNanos
// captured before locking so tracker start times never read shared clock
// state under the write lock.
func (r *DataReceiver) ensureLivelyLocked(topic string, guid WriterGUID, cfg livelyConfig, nowNanos int64) (*core.LivelinessTracker, WriterGUID, bool) {
	if !r.isMatchedWriterLocked(guid) {
		return nil, WriterGUID{}, false
	}
	if r.lively == nil {
		r.lively = make(map[WriterGUID]*core.LivelinessTracker)
	}
	if _, ok := r.lively[guid]; !ok {
		var evicted WriterGUID
		evictedOk := false
		if len(r.lively) >= livelyTrackerCap {
			evicted, evictedOk = r.evictLivelyLocked()
		}
		tracker := core.NewLivelinessTracker(topic, writerGUIDLabel(guid), cfg.kind, cfg.lease, nowNanos)
		r.lively[guid] = &tracker
		if evictedOk {
			return &tracker, evicted, true
		}
	}
	return r.lively[guid], WriterGUID{}, false
}

// snapshotTakeHooks loads one sample's hook state with lazy tracker creation.
//
// The fast path is one short RLock. When a topic carries a liveliness
// config but the writer has no tracker yet, creation is gated on a matched
// writer (a confirmed SEDP remote or a provisional placeholder): unknown
// GUIDs never create trackers, so unauthenticated traffic cannot grow the
// map without a matching SEDP announce. At cap one entry is evicted to
// keep the map bounded. Writers discovered after subscribe are still
// enforced from their first take. Lazy creation delegates to
// ensureLivelyLocked, which evicts an arbitrary entry on cap since the
// bound is memory, not fairness.
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
		matched := r.isMatchedWriter(guid)
		if !matched {
			return snap
		}
		nowNanos := r.receiverNow()().UnixNano()
		r.mu.Lock()
		tracker, evicted, evictedOk := r.ensureLivelyLocked(topic, guid, cfg, nowNanos)
		snap.tracker = tracker
		r.mu.Unlock()
		if evictedOk {
			log.Printf("dds: lively table full evict guid=%x", evicted)
		}
	}

	return snap
}

// SetLivelinessTrackerForTopic records the liveliness contract for one
// topic and installs trackers for currently matched writers.
//
// Writers discovered later get trackers lazily on first take, so the
// contract holds without re-subscribing. Reconfiguration updates existing
// writers in place through Reconfigure so a lease or kind change applies
// without resetting outage history. Explicit nil writer lists only store
// the config. Explicit installs hold livelyTrackerCap with arbitrary
// eviction like the lazy path so bulk installs cannot grow the map without
// bound; victims log loudly after unlock, never under the receiver lock.
func (r *DataReceiver) SetLivelinessTrackerForTopic(topic string, kind core.LivelinessPolicy, lease core.Duration, writers []WriterGUID) {
	nowNanos := r.receiverNow()().UnixNano()
	r.mu.Lock()

	if r.livelyCfg == nil {
		r.livelyCfg = make(map[string]livelyConfig)
	}

	r.livelyCfg[topic] = livelyConfig{kind: kind, lease: lease}

	if r.lively == nil {
		r.lively = make(map[WriterGUID]*core.LivelinessTracker)
	}

	var evicted []WriterGUID
	for _, guid := range writers {
		if existing, ok := r.lively[guid]; ok {
			existing.Reconfigure(kind, lease)

			continue
		}

		if len(r.lively) >= livelyTrackerCap {
			if victim, ok := r.evictLivelyLocked(); ok {
				evicted = append(evicted, victim)
			}
		}

		tracker := core.NewLivelinessTracker(topic, writerGUIDLabel(guid), kind, lease, nowNanos)
		r.lively[guid] = &tracker
	}

	r.mu.Unlock()

	for _, victim := range evicted {
		log.Printf("dds: lively table full evict guid=%x", victim)
	}
}

// UnsetLivelinessTrackerForTopic removes the liveliness contract for one
// topic plus every tracker installed for that topic.
//
// It deletes the per-topic config and scans the lively map for trackers
// whose topic matches, so lazy trackers created on first take do not
// outlive the subscription. Writers on other topics are untouched. It
// self-locks and is safe for concurrent use.
func (r *DataReceiver) UnsetLivelinessTrackerForTopic(topic string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	delete(r.livelyCfg, topic)

	for guid, tracker := range r.lively {
		if tracker == nil {
			delete(r.lively, guid)

			continue
		}
		if tracker.Topic() == topic {
			delete(r.lively, guid)
		}
	}
}

// SetDeadlineTrackerForTopic records the deadline contract for one topic.
//
// Missed deadlines drop loudly naming the deadline policy. A zero or
// unspecified deadline disables the check while keeping the tracker.
func (r *DataReceiver) SetDeadlineTrackerForTopic(topic string, deadline core.Duration) {
	nowNanos := r.receiverNow()().UnixNano()
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.deadlines == nil {
		r.deadlines = make(map[string]*core.DeadlineTracker)
	}

	tracker := core.NewDeadlineTracker(topic, "reader", deadline, nowNanos)
	r.deadlines[topic] = &tracker
}

// writerGUIDLabel renders a writer GUID for tracker naming.
func writerGUIDLabel(guid WriterGUID) string {
	return hex.EncodeToString(guid[:])
}

// evalTakeHooks runs content filtering, keyed extraction, and liveliness
// gating for one validated sample against a snapshot.
//
// Receipt asserts liveliness and anchors the deadline even when the sample
// is later filtered: the assert runs before filter evaluation so filtered
// traffic still proves the writer alive. An expired writer sending only
// filtered traffic therefore surfaces as a filter miss, not expiry.
// The deadline miss flag is captured before anchoring, so the current
// sample still drops on a miss instead of hiding behind its own anchor.
// It never counts and never takes receiver locks: the caller counts
// reasoned drops exactly once and propagates holds uncounted. Order after
// the assert is filter, then keyed, then liveliness gate, so dashboards
// blame the earliest applicable cause. A nil return means deliver. It
// never panics.
//
//nolint:unparam // topic documents the sampled topic for call-site symmetry though the snapshot already carries the filter
func evalTakeHooks(snap takeHookSnapshot, topic, typeIDL string, payload []byte, guid WriterGUID, keyed bool, now int64) *takeHookReject {
	deadlineMissed := assertTakeReceipt(snap, now)

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

	return evalLivelinessHook(snap, deadlineMissed, now)
}

// assertTakeReceipt records one receipt against liveliness and deadline.
//
// Automatic writers assert through RecordSampleAt; manual writers never ride
// free on data. The deadline miss flag is captured before MarkSeen so the
// caller gates on the pre-receipt miss while the anchor still moves forward
// for filtered traffic. It reports the pre-anchor miss for the gate step.
func assertTakeReceipt(snap takeHookSnapshot, now int64) bool {
	var missed bool
	if snap.deadline != nil {
		missed = snap.deadline.MissedAt(now)
		snap.deadline.MarkSeen(now)
	}
	if snap.tracker != nil {
		snap.tracker.RecordSampleAt(now)
	}
	return missed
}

// applyContentFilterHook evaluates one compiled filter on a raw payload.
//
// Misses and eval errors map to their distinct drop reasons with the
// matching sentinel. A nil descriptor is an eval error, never a pass:
// filtering without type shape would silently misroute. Apply never
// returns false with a nil error (misses carry ErrContentFilterMiss),
// so the error branch below is the sole rejection path.
func applyContentFilterHook(entry *contentFilterEntry, payload []byte) *takeHookReject {
	if entry.desc == nil {
		reason := DropReasonContentFilterEvalError

		return &takeHookReject{
			reason: &reason,
			err:    fmt.Errorf("dds: content filter take without descriptor: %w", ErrContentFilterEvalError),
		}
	}

	_, reason, err := entry.filter.Apply(payload, entry.desc)
	if err != nil {
		r := DropReasonContentFilterEvalError
		if reason == DropReasonContentFilterMiss {
			r = reason
		}

		return &takeHookReject{reason: &r, err: err}
	}

	return nil
}

// applyKeyedHook extracts the instance key for a keyed endpoint sample.
//
// Keyed extraction proves the sample carries a well-formed key; the
// handle itself routes instances upstream. Truncated or malformed keys
// drop loudly with the keyed reason. A keyed sample whose type is not
// registered fails closed with the same reason instead of delivering
// unkeyed: silent unkeyed delivery would misroute a keyed instance.
// Endpoints that are not keyed never reach here, so unkeyed flow is
// byte-identical with or without this hook.
func applyKeyedHook(typeIDL string, payload []byte) *takeHookReject {
	desc, err := lookupDescriptorForIDL(typeIDL)
	if err != nil {
		reason := DropReasonKeyedDecodeFail

		return &takeHookReject{
			reason: &reason,
			err:    fmt.Errorf("dds: take keyed unregistered type %q: %w", typeIDL, errors.Join(ErrKeyedDecodeFail, err)),
		}
	}

	_, err = ExtractKeyBytes(payload, desc)
	if err != nil {
		reason := DropReasonKeyedDecodeFail

		return &takeHookReject{
			reason: &reason,
			err:    fmt.Errorf("dds: take keyed extract: %w", errors.Join(ErrKeyedDecodeFail, err)),
		}
	}

	return nil
}

// evalLivelinessHook gates delivery by writer state without re-asserting.
//
// The caller must have run assertTakeReceipt first with the same now, so the
// tracker state already reflects this receipt and the deadline already
// anchored. It uses the pre-anchor miss flag so the current sample still
// drops on a miss. Missing trackers pass through unless the captured miss
// says otherwise. Out-of-range gate decisions fail closed with a loud drop.
func evalLivelinessHook(snap takeHookSnapshot, deadlineMissed bool, now int64) *takeHookReject {
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
			err:    fmt.Errorf("dds: take invalid gate decision %d: %w", int(outcome.Decision), errors.Join(ErrLivelinessExpired, errors.New("invalid gate decision"))),
		}
	}
}
