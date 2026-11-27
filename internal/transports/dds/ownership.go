// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

package dds

import (
	"bytes"
	"errors"
	"fmt"
	"sort"
	"sync"

	"github.com/podomy/weld/internal/core"
)

// Ownership, destination order, time-based filter, and resource limits,
// DDS 1.4 sections 2.2.3.9.2, 2.2.3.9, 2.2.3.10, and 2.2.3.19.
//
// Ownership arbitration picks the exclusive owner per instance by highest
// strength with lowest GUID breaking ties. Destination order sorts by
// reception by default and by source timestamp when DestinationOrder is
// BY_SOURCE_TIMESTAMP. The time filter enforces minimum separation per
// instance in steady state: only samples inside the separation window
// drop, passing samples move the watermark and filtered takes never move
// it. Resource limits bound retained history with MaxSamples eviction plus
// admission checks for instances. Enforcement applies on every path: the
// legacy take path gates through ownershipDeliverableLocked and
// timeFilterPassLocked via deliverableLife5Take exactly like the
// read/take-next collect paths.

// OwnershipKind names the instance ownership rule.
type OwnershipKind int

// Ownership kinds.
const (
	// OwnershipUnknown marks an unresolved kind and is rejected by Validate.
	OwnershipUnknown OwnershipKind = iota
	// OwnershipShared delivers every writer for the instance.
	OwnershipShared
	// OwnershipExclusive delivers only the arbitrated owner.
	OwnershipExclusive
)

// String renders the ownership kind in lowercase form.
func (k OwnershipKind) String() string {
	switch k {
	case OwnershipShared:
		return "shared"
	case OwnershipExclusive:
		return "exclusive"
	case OwnershipUnknown:
		return "unknown"
	default:
		return "invalid"
	}
}

// Validate reports whether the kind is usable for arbitration.
func (k OwnershipKind) Validate() error {
	switch k {
	case OwnershipShared, OwnershipExclusive:
		return nil
	case OwnershipUnknown:
		return fmt.Errorf("dds: ownership unknown kind: %w", ErrOwnershipRefused)
	default:
		return fmt.Errorf("dds: ownership kind %d is invalid: %w", int(k), ErrOwnershipRefused)
	}
}

// ErrOwnershipRefused reports an unusable ownership setting.
var ErrOwnershipRefused = errors.New("dds: ownership refused")

// ErrResourceExhausted reports a resource limit refusal distinct from arbitration.
//
// Ownership arbitration keeps ErrOwnershipRefused; blocking and admission
// paths (MaxSamples, MaxInstances, MaxSamplesPerInstance, history depth)
// join this sentinel so callers can distinguish exhaustion from arbitration.
// It chains ErrOwnershipRefused so refusals pre-dating the split still match
// errors.Is with either sentinel; new code should match ErrResourceExhausted.
// Time-filter versus deadline consistency errors (checkLife5DeadlineConsistent,
// SetTimeBasedFilter) deliberately keep chaining ErrOwnershipRefused rather
// than splitting to a new sentinel: pre-split callers match
// errors.Is(err, ErrOwnershipRefused) for every config refusal, and a split
// would silently break those contracts. Keeping the chain preserves
// backward compat while ErrResourceExhausted stays the preferred match for
// resource cases.
// TODO backend agent - remap datapath resource errors to ErrResourceExhausted
// where datapath currently matches ErrOwnershipRefused.
var ErrResourceExhausted = fmt.Errorf("%w: resource exhausted", ErrOwnershipRefused)

// DestinationOrderKind names the reader destination order rule.
type DestinationOrderKind int

// Destination order kinds.
const (
	// DestinationOrderUnknown marks an unresolved order and is rejected by Validate.
	DestinationOrderUnknown DestinationOrderKind = iota
	// DestinationOrderByReception orders by reception timestamp.
	DestinationOrderByReception
	// DestinationOrderBySourceTimestamp orders by source timestamp.
	DestinationOrderBySourceTimestamp
)

// String renders the destination order kind in lowercase form.
func (k DestinationOrderKind) String() string {
	switch k {
	case DestinationOrderByReception:
		return "by_reception"
	case DestinationOrderBySourceTimestamp:
		return "by_source_timestamp"
	case DestinationOrderUnknown:
		return "unknown"
	default:
		return "invalid"
	}
}

// Validate reports whether the kind is usable for ordering.
func (k DestinationOrderKind) Validate() error {
	switch k {
	case DestinationOrderByReception, DestinationOrderBySourceTimestamp:
		return nil
	case DestinationOrderUnknown:
		return fmt.Errorf("dds: destination order unknown kind: %w", ErrOwnershipRefused)
	default:
		return fmt.Errorf("dds: destination order kind %d is invalid: %w", int(k), ErrOwnershipRefused)
	}
}

// ownershipState holds kinds plus winning offers per instance.
type ownershipState struct {
	mu             sync.Mutex
	kinds          map[[16]byte]OwnershipKind
	writerKinds    map[WriterGUID]OwnershipKind
	offers         map[[16]byte]map[WriterGUID]int32
	strength       map[WriterGUID]int32
	alive          map[WriterGUID]bool
	aliveSet       map[WriterGUID]bool
	deadlineMissed map[WriterGUID]bool
}

// newOwnershipState returns an empty arbitrator defaulting to shared.
func newOwnershipState() *ownershipState {
	return &ownershipState{
		kinds:          make(map[[16]byte]OwnershipKind),
		writerKinds:    make(map[WriterGUID]OwnershipKind),
		offers:         make(map[[16]byte]map[WriterGUID]int32),
		strength:       make(map[WriterGUID]int32),
		alive:          make(map[WriterGUID]bool),
		aliveSet:       make(map[WriterGUID]bool),
		deadlineMissed: make(map[WriterGUID]bool),
	}
}

// SetOwnershipKind records the ownership rule for one instance handle.
func (r *DataReceiver) SetOwnershipKind(handle [16]byte, kind OwnershipKind) error {
	err := kind.Validate()
	if err != nil {
		return err
	}

	st := r.readerStateFor().ownership
	st.mu.Lock()
	defer st.mu.Unlock()

	st.kinds[handle] = kind

	return nil
}

// SetOwnershipStrength records the strength for one writer GUID.
func (r *DataReceiver) SetOwnershipStrength(writer WriterGUID, strength int32) {
	st := r.readerStateFor().ownership
	st.mu.Lock()
	defer st.mu.Unlock()

	st.strength[writer] = strength
}

// SetOwnershipWriterKind records the offered ownership kind for one writer.
//
// The reader kind from SetOwnershipKind and the writer kind must match per
// DDS 1.4 section 2.2.3.9 RxO: EXCLUSIVE versus SHARED mismatch is
// incompatible and fails closed with no delivery. Writers with no recorded
// kind count as compatible for backward compat. Unknown kinds are refused.
func (r *DataReceiver) SetOwnershipWriterKind(writer WriterGUID, kind OwnershipKind) error {
	err := kind.Validate()
	if err != nil {
		return err
	}

	st := r.readerStateFor().ownership
	st.mu.Lock()
	defer st.mu.Unlock()

	if st.writerKinds == nil {
		st.writerKinds = make(map[WriterGUID]OwnershipKind)
	}
	st.writerKinds[writer] = kind

	return nil
}

// ownershipCompatibleLocked reports whether one writer kind matches the reader kind.
//
// Writers with no recorded kind count as compatible: without offered QoS
// on file the reader cannot fail closed, so unset writers match any reader
// kind for backward compat (matching SetOwnershipWriterKind). Explicitly
// recorded kinds must equal the reader kind per DDS 1.4 section 2.2.3.9 RxO.
// Unknown writer kinds fail closed. Unset reader kinds default to shared
// like OfferOwnership. The caller must hold st.mu.
func ownershipCompatibleLocked(st *ownershipState, handle [16]byte, writer WriterGUID) bool {
	if st == nil {
		return true
	}

	readerKind := st.kinds[handle]
	if readerKind == OwnershipUnknown {
		readerKind = OwnershipShared
	}

	writerKind, ok := st.writerKinds[writer]
	if !ok {
		return true
	}
	if writerKind == OwnershipUnknown {
		return false
	}

	return writerKind == readerKind
}

// CheckOwnershipCompatible reports whether one writer may deliver to one instance.
//
// It fails closed on an explicitly recorded EXCLUSIVE versus SHARED
// mismatch per DDS 1.4 section 2.2.3.9; writers with no recorded kind count
// as compatible so unconfigured writers never stall exclusive arbitration.
func (r *DataReceiver) CheckOwnershipCompatible(handle [16]byte, writer WriterGUID) bool {
	st := r.readerStateFor().ownership
	st.mu.Lock()
	defer st.mu.Unlock()

	return ownershipCompatibleLocked(st, handle, writer)
}

// FeedOwnershipqos4Status records liveliness plus deadline inputs for arbitration.
//
// Datapath owners should call this on every take with the writer liveliness
// stage (alive true when GateDelivery would not drop for liveliness) and the
// deadline-clean flag (true when snapshotTakeHooks reports no deadline miss)
// so exclusive arbitration drops dead or deadline-missed writers. It is the
// additive datapath hook for SetOwnershipqos4Alive and
// SetOwnershipqos4DeadlineMissed: TODO datapath/takehooks owner - call
// FeedOwnershipqos4Status(guid, alive, deadlineClean) in takeDecoded and
// tryPlainFallbacks after evalTakeHooks, deriving alive from the liveliness
// gate outcome and deadlineClean from the deadline miss flag.
func (r *DataReceiver) FeedOwnershipqos4Status(writer WriterGUID, alive, deadlineClean bool) {
	r.SetOwnershipqos4Alive(writer, alive)
	r.SetOwnershipqos4DeadlineMissed(writer, !deadlineClean)
}

// SetOwnershipqos4Alive records aliveness for one writer for arbitration.
func (r *DataReceiver) SetOwnershipqos4Alive(writer WriterGUID, alive bool) {
	st := r.readerStateFor().ownership
	st.mu.Lock()
	defer st.mu.Unlock()

	if st.alive == nil {
		st.alive = make(map[WriterGUID]bool)
	}
	if st.aliveSet == nil {
		st.aliveSet = make(map[WriterGUID]bool)
	}
	st.alive[writer] = alive
	st.aliveSet[writer] = true
}

// SetOwnershipqos4DeadlineMissed records deadline miss for one writer for arbitration.
func (r *DataReceiver) SetOwnershipqos4DeadlineMissed(writer WriterGUID, missed bool) {
	st := r.readerStateFor().ownership
	st.mu.Lock()
	defer st.mu.Unlock()

	if st.deadlineMissed == nil {
		st.deadlineMissed = make(map[WriterGUID]bool)
	}
	st.deadlineMissed[writer] = missed
}

// ownershipqos4EligibleLocked reports whether one writer may own the instance.
func ownershipqos4EligibleLocked(st *ownershipState, writer WriterGUID) bool {
	if st == nil {
		return true
	}
	if st.aliveSet != nil {
		if _, ok := st.aliveSet[writer]; ok {
			if !st.alive[writer] {
				return false
			}
		}
	}
	if st.deadlineMissed != nil {
		if st.deadlineMissed[writer] {
			return false
		}
	}
	return true
}

// exclusiveWinnerLocked selects the exclusive owner by strength.
//
// Highest strength among eligible writers wins with lowest GUID breaking
// ties. Eligibility requires liveliness plus deadline cleanliness and RxO
// kind match: writers with a mismatched offered kind never win and fail
// closed. It reports false when no writer is eligible.
func exclusiveWinnerLocked(st *ownershipState, handle [16]byte, claims map[WriterGUID]int32) (WriterGUID, bool) {
	var winner WriterGUID
	var best int32
	first := true

	for guid, offer := range claims {
		if !ownershipqos4EligibleLocked(st, guid) {
			continue
		}
		if !ownershipCompatibleLocked(st, handle, guid) {
			continue
		}
		if first || offer > best || (offer == best && bytes.Compare(guid[:], winner[:]) < 0) {
			winner, best, first = guid, offer, false
		}
	}

	if first {
		return WriterGUID{}, false
	}

	return winner, true
}

// sharedWinner reports the lowest GUID claimant for determinism.
func sharedWinner(claims map[WriterGUID]int32) WriterGUID {
	var winner WriterGUID
	first := true

	for guid := range claims {
		if first || bytes.Compare(guid[:], winner[:]) < 0 {
			winner = guid
			first = false
		}
	}

	return winner
}

// OfferOwnership records one writer claim and reports deliverability.
//
// Shared instances always deliver when the writer kind matches; a mismatched
// offered kind fails closed with no delivery per DDS 1.4 section 2.2.3.9 RxO.
// Exclusive instances deliver only for the current owner: highest strength
// among alive, deadline-clean, and kind-compatible writers wins, and the
// lowest GUID breaks ties deterministically per DDS 1.4 section 2.2.3.9.2.
// The owner output names the winner.
func (r *DataReceiver) OfferOwnership(handle [16]byte, writer WriterGUID) (owner WriterGUID, deliverable bool) {
	st := r.readerStateFor().ownership
	st.mu.Lock()
	defer st.mu.Unlock()

	kind := st.kinds[handle]
	if kind == OwnershipUnknown {
		kind = OwnershipShared
	}

	strength := st.strength[writer]

	claims, ok := st.offers[handle]
	if !ok {
		claims = make(map[WriterGUID]int32)
		st.offers[handle] = claims
	}

	claims[writer] = strength

	if !ownershipCompatibleLocked(st, handle, writer) {
		return WriterGUID{}, false
	}

	if kind == OwnershipShared {
		return writer, true
	}

	winner, ok := exclusiveWinnerLocked(st, handle, claims)
	if !ok {
		return WriterGUID{}, false
	}

	return winner, winner == writer
}

// OwnershipOwner reports the current exclusive owner for one instance.
//
// It reports false when no claim exists. Shared instances report the
// lowest GUID claimant for determinism: shared delivery still flows to
// every writer, but the owner query must not depend on map iteration
// order. Exclusive instances report the highest strength among alive,
// deadline-clean, and kind-compatible writers with lowest GUID breaking
// ties per DDS 1.4 section 2.2.3.9.2. Writers with no liveliness, deadline,
// or writer-kind record count as eligible for backward compat. No eligible
// writer reports false. Kind-mismatched writers never win.
func (r *DataReceiver) OwnershipOwner(handle [16]byte) (WriterGUID, bool) {
	st := r.readerStateFor().ownership
	st.mu.Lock()
	defer st.mu.Unlock()

	claims, ok := st.offers[handle]
	if !ok || len(claims) == 0 {
		return WriterGUID{}, false
	}

	kind := st.kinds[handle]

	if kind == OwnershipShared {
		return sharedWinner(claims), true
	}

	winner, ok := exclusiveWinnerLocked(st, handle, claims)
	if !ok {
		return WriterGUID{}, false
	}

	return winner, true
}

// OrderByReception sorts samples oldest reception first, stable on ties.
//
// Ties break on publication handle bytes then sample sequence so the order
// stays deterministic across writers. It sorts in place and returns the
// slice.
func OrderByReception(samples []SampledMessage) []SampledMessage {
	sort.SliceStable(samples, func(i, j int) bool {
		a := samples[i].Info.ReceptionTimestamp
		b := samples[j].Info.ReceptionTimestamp

		if !a.Equal(b) {
			return a.Before(b)
		}

		pa := samples[i].Info.PublicationHandle
		pb := samples[j].Info.PublicationHandle

		if pa != pb {
			return bytes.Compare(pa[:], pb[:]) < 0
		}

		return samples[i].Info.SampleSequence < samples[j].Info.SampleSequence
	})

	return samples
}

// OrderBySourceTimestamp sorts samples oldest source first, stable on ties.
//
// Zero source stamps fall back to reception so v1 DATA frames and plain
// samples without a sender time still order deterministically. Ties break on
// reception, then publication handle bytes, then sample sequence. It sorts in
// place and returns the slice per DDS 1.4 section 2.2.3.17
// BY_SOURCE_TIMESTAMP destination order.
func OrderBySourceTimestamp(samples []SampledMessage) []SampledMessage {
	sort.SliceStable(samples, func(i, j int) bool {
		a := samples[i].Info.SourceTimestamp
		b := samples[j].Info.SourceTimestamp
		if a.IsZero() {
			a = samples[i].Info.ReceptionTimestamp
		}
		if b.IsZero() {
			b = samples[j].Info.ReceptionTimestamp
		}

		if !a.Equal(b) {
			return a.Before(b)
		}

		ra := samples[i].Info.ReceptionTimestamp
		rb := samples[j].Info.ReceptionTimestamp
		if !ra.Equal(rb) {
			return ra.Before(rb)
		}

		pa := samples[i].Info.PublicationHandle
		pb := samples[j].Info.PublicationHandle

		if pa != pb {
			return bytes.Compare(pa[:], pb[:]) < 0
		}

		return samples[i].Info.SampleSequence < samples[j].Info.SampleSequence
	})

	return samples
}

// SetDestinationOrder records the reader destination order rule.
//
// BY_RECEPTION orders collect batches by reception timestamp;
// BY_SOURCE_TIMESTAMP orders by source timestamp with reception fallback per
// DDS 1.4 section 2.2.3.17. Unknown kinds are refused and keep the previous
// rule. The default is BY_RECEPTION.
func (r *DataReceiver) SetDestinationOrder(kind DestinationOrderKind) error {
	err := kind.Validate()
	if err != nil {
		return err
	}

	st := r.readerStateFor()
	st.mu.Lock()
	defer st.mu.Unlock()

	st.destinationOrderLife5 = kind

	return nil
}

// timeFilterState enforces minimum separation per instance.
type timeFilterState struct {
	mu       sync.Mutex
	minimum  int64
	last     map[[16]byte]int64
	filtered uint64
}

// newTimeFilterState returns a filter with the given minimum separation in nanos.
func newTimeFilterState(minimum int64) *timeFilterState {
	return &timeFilterState{minimum: minimum, last: make(map[[16]byte]int64)}
}

// SetTimeBasedFilter records the minimum separation in nanoseconds.
//
// A non-positive separation disables filtering; every sample passes. It
// refuses a minimum above any installed deadline per DDS 1.4 sections
// 2.2.3.7 and 2.2.3.12, which require deadline period >= minimum_separation,
// and keeps the previous minimum on refusal.
func (r *DataReceiver) SetTimeBasedFilter(minimumNanos int64) error {
	r.mu.RLock()
	deadlines := make([]core.Duration, 0, len(r.deadlines))
	for _, tracker := range r.deadlines {
		if tracker != nil {
			deadlines = append(deadlines, tracker.Deadline())
		}
	}
	r.mu.RUnlock()

	for _, deadline := range deadlines {
		if deadline.IsUnspecified() || deadline.IsInfinite() {
			continue
		}
		if minimumNanos > 0 && deadline.TotalNanoseconds() < minimumNanos {
			return fmt.Errorf("dds: time filter minimum %d above deadline %d: %w", minimumNanos, deadline.TotalNanoseconds(), ErrOwnershipRefused)
		}
	}

	st := r.readerStateFor().timeFilter
	st.mu.Lock()
	defer st.mu.Unlock()

	st.minimum = minimumNanos

	return nil
}

// AllowTimeBasedFilter reports whether a sample at now passes the filter.
//
// The first sample for an instance always passes. A passing sample moves
// the watermark; a filtered sample leaves it. Filtered samples count for
// dashboards.
func (r *DataReceiver) AllowTimeBasedFilter(handle [16]byte, now int64) bool {
	st := r.readerStateFor().timeFilter
	st.mu.Lock()
	defer st.mu.Unlock()

	if st.minimum <= 0 {
		return true
	}

	last, seen := st.last[handle]
	if !seen {
		st.last[handle] = now

		return true
	}

	if now-last < st.minimum {
		st.filtered++

		return false
	}

	st.last[handle] = now

	return true
}

// TimeFilteredCount returns the count of time-filtered samples.
func (r *DataReceiver) TimeFilteredCount() uint64 {
	st := r.readerStateFor().timeFilter
	st.mu.Lock()
	defer st.mu.Unlock()

	return st.filtered
}

// ResourceLimits bounds retained samples and instances, DDS 1.4 section 2.2.3.19.
//
// Negative values mean unlimited. MaxSamples caps retained history with
// oldest-first eviction; MaxInstances caps tracked instances with loud
// admission refusal; MaxSamplesPerInstance caps per-instance history.
type ResourceLimits struct {
	// MaxSamples caps retained samples, negative for unlimited.
	MaxSamples int64
	// MaxInstances caps tracked instances, negative for unlimited.
	MaxInstances int64
	// MaxSamplesPerInstance caps per-instance history, negative for unlimited.
	MaxSamplesPerInstance int64
}

// UnlimitedResources returns limits with every bound disabled.
func UnlimitedResources() ResourceLimits {
	return ResourceLimits{MaxSamples: -1, MaxInstances: -1, MaxSamplesPerInstance: -1}
}

// Validate rejects limits below -1, which encode nothing.
func (l ResourceLimits) Validate() error {
	if l.MaxSamples < -1 {
		return fmt.Errorf("dds: resource max samples %d below unlimited: %w", l.MaxSamples, ErrResourceExhausted)
	}

	if l.MaxInstances < -1 {
		return fmt.Errorf("dds: resource max instances %d below unlimited: %w", l.MaxInstances, ErrResourceExhausted)
	}

	if l.MaxSamplesPerInstance < -1 {
		return fmt.Errorf("dds: resource max samples per instance %d below unlimited: %w", l.MaxSamplesPerInstance, ErrResourceExhausted)
	}

	if l.MaxSamples >= 0 && l.MaxSamplesPerInstance >= 0 && l.MaxSamples < l.MaxSamplesPerInstance {
		return fmt.Errorf("dds: resource max samples %d below per-instance %d: %w", l.MaxSamples, l.MaxSamplesPerInstance, ErrResourceExhausted)
	}

	return nil
}

// checkHistoryDepthConsistentLocked reports whether one depth fits the limits.
//
// KEEP_ALL ignores depth; KEEP_LAST depths must fit inside MaxSamples and
// MaxSamplesPerInstance when those bounds are set. Unlimited depth always
// fits. The caller must hold st.mu.
func checkHistoryDepthConsistentLocked(st *readerState, depth int64, limits ResourceLimits) error {
	if depth < 0 {
		return nil
	}
	if depth == 0 {
		return fmt.Errorf("dds: history depth 0 invalid: %w", ErrResourceExhausted)
	}
	if st != nil && st.keepAllLife5 {
		return nil
	}
	if limits.MaxSamplesPerInstance >= 0 && depth > limits.MaxSamplesPerInstance {
		return fmt.Errorf("dds: history depth %d above max samples per instance %d: %w", depth, limits.MaxSamplesPerInstance, ErrResourceExhausted)
	}
	if limits.MaxSamples >= 0 && depth > limits.MaxSamples {
		return fmt.Errorf("dds: history depth %d above max samples %d: %w", depth, limits.MaxSamples, ErrResourceExhausted)
	}

	return nil
}

// SetHistoryDepthLife5 records the KEEP_LAST history depth.
//
// Negative means unlimited; zero is refused. Depths above the installed
// resource bounds are refused per DDS 1.4 sections 2.2.3.18 and 2.2.3.19 so
// history never outgrows resources. KEEP_ALL ignores depth.
func (r *DataReceiver) SetHistoryDepthLife5(depth int64) error {
	if depth < -1 || depth == 0 {
		return fmt.Errorf("dds: history depth %d invalid: %w", depth, ErrResourceExhausted)
	}

	st := r.readerStateFor()
	st.mu.Lock()
	defer st.mu.Unlock()

	if err := checkHistoryDepthConsistentLocked(st, depth, st.limits); err != nil {
		return err
	}

	st.historyDepthLife5 = depth

	return nil
}

// enforceHistoryDepthLocked evicts oldest history past the KEEP_LAST depth.
//
// Unlimited depths and KEEP_ALL never evict; depth is ignored for KEEP_ALL.
// The caller must hold st.mu.
func (st *readerState) enforceHistoryDepthLocked() {
	if st == nil {
		return
	}
	if st.keepAllLife5 {
		return
	}
	depth := st.historyDepthLife5
	if depth < 0 {
		return
	}

	totals := make(map[[16]byte]int64, len(st.history))
	for _, entry := range st.history {
		totals[entry.sample.Info.InstanceHandle]++
	}

	skip := make(map[[16]byte]int64, len(totals))
	for handle, total := range totals {
		if total > depth {
			skip[handle] = total - depth
		}
	}
	if len(skip) == 0 {
		return
	}

	kept := st.history[:0]
	for _, entry := range st.history {
		handle := entry.sample.Info.InstanceHandle
		if skip[handle] > 0 {
			skip[handle]--
			st.violations++

			continue
		}
		kept = append(kept, entry)
	}

	for i := len(kept); i < len(st.history); i++ {
		st.history[i] = nil
	}

	st.history = kept
}

// orderLife5Locked orders one batch per the destination order rule.
//
// BY_SOURCE_TIMESTAMP sorts by source timestamp with reception fallback;
// all other rules sort by reception. The caller must hold st.mu only for
// reading the rule; the sort itself needs no locks.
func (st *readerState) orderLife5Locked(out []SampledMessage) []SampledMessage {
	if st != nil && st.destinationOrderLife5 == DestinationOrderBySourceTimestamp {
		return OrderBySourceTimestamp(out)
	}

	return OrderByReception(out)
}

// SetResourceLimits installs the history bounds for the receiver.
//
// MaxSamples evicts oldest-first on append; instance bounds surface
// through AdmitInstance and PerInstanceAdmit. Invalid limits report an
// error and keep the previous bounds. History depth consistency is
// enforced: MaxSamples below MaxSamplesPerInstance and KEEP_LAST depths
// above either bound are refused per DDS 1.4 sections 2.2.3.18 and 2.2.3.19.
func (r *DataReceiver) SetResourceLimits(limits ResourceLimits) error {
	err := limits.Validate()
	if err != nil {
		return err
	}

	st := r.readerStateFor()
	st.mu.Lock()
	defer st.mu.Unlock()

	if err := checkHistoryDepthConsistentLocked(st, st.historyDepthLife5, limits); err != nil {
		return err
	}

	st.limits = limits

	return nil
}

// AdmitInstance checks one new instance against the MaxInstances bound.
//
// It reports an error joining ErrResourceExhausted when the bound is
// reached. Callers admit before RegisterInstance for strict enforcement;
// the observe path stays permissive so delivery never drops on churn.
func (r *DataReceiver) AdmitInstance() error {
	st := r.readerStateFor()
	st.mu.Lock()
	defer st.mu.Unlock()

	bound := st.limits.MaxInstances
	if bound < 0 {
		return nil
	}

	if int64(len(st.instances)) >= bound {
		return fmt.Errorf("dds: resource max instances %d reached: %w", bound, ErrResourceExhausted)
	}

	return nil
}

// PerInstanceDepth returns the retained history depth for one instance.
func (r *DataReceiver) PerInstanceDepth(handle [16]byte) int64 {
	st := r.readerStateFor()
	st.mu.Lock()
	defer st.mu.Unlock()

	var depth int64

	for _, entry := range st.history {
		if entry.sample.Info.InstanceHandle == handle {
			depth++
		}
	}

	return depth
}

// PerInstanceAdmit checks one sample against the per-instance bound.
//
// It reports an error joining ErrResourceExhausted when the instance
// already holds the bound. Unlimited bounds always admit.
func (r *DataReceiver) PerInstanceAdmit(handle [16]byte) error {
	st := r.readerStateFor()
	st.mu.Lock()
	defer st.mu.Unlock()

	bound := st.limits.MaxSamplesPerInstance
	if bound < 0 {
		return nil
	}

	var depth int64

	for _, entry := range st.history {
		if entry.sample.Info.InstanceHandle == handle {
			depth++
		}
	}

	if depth >= bound {
		return fmt.Errorf("dds: resource max samples per instance %d reached: %w", bound, ErrResourceExhausted)
	}

	return nil
}

// deliverableForWinnerLocked reports whether the sample writer is the winner.
func deliverableForWinnerLocked(sample SampledMessage, winner WriterGUID) bool {
	var writer WriterGUID
	copy(writer[:], sample.Info.PublicationHandle[:])

	return writer == winner
}

// ownershipDeliverableLocked reports whether one history sample may flow.
//
// Shared instances always deliver when the writer kind matches; mismatched
// kinds fail closed per DDS 1.4 section 2.2.3.9 RxO. Exclusive instances
// deliver only when the sample writer is the arbitrated owner among alive,
// deadline-clean, and kind-compatible writers; samples with no recorded
// claims deliver so unconfigured instances never stall. Writers with no
// liveliness, deadline, or writer-kind record count as eligible. When no
// writer is eligible the sample does not flow. The caller must hold st.mu;
// only the ownership sublock is taken here.
func (st *readerState) ownershipDeliverableLocked(sample SampledMessage) bool {
	if st == nil || st.ownership == nil {
		return true
	}

	var writer WriterGUID
	copy(writer[:], sample.Info.PublicationHandle[:])

	st.ownership.mu.Lock()
	defer st.ownership.mu.Unlock()

	if !ownershipCompatibleLocked(st.ownership, sample.Info.InstanceHandle, writer) {
		return false
	}

	kind := st.ownership.kinds[sample.Info.InstanceHandle]
	if kind == OwnershipUnknown {
		kind = OwnershipShared
	}
	if kind != OwnershipExclusive {
		return true
	}

	claims, ok := st.ownership.offers[sample.Info.InstanceHandle]
	if !ok || len(claims) == 0 {
		return true
	}

	winner, ok := exclusiveWinnerLocked(st.ownership, sample.Info.InstanceHandle, claims)
	if !ok {
		return false
	}

	return deliverableForWinnerLocked(sample, winner)
}

// timeFilterPassLocked reports whether one sample passes minimum separation.
//
// It shares AllowTimeBasedFilter logic without reentering through the
// receiver so scan paths holding st.mu never deadlock. The first sample
// for an instance always passes. The caller must hold st.mu; only the
// filter sublock is taken here.
func (st *readerState) timeFilterPassLocked(handle [16]byte, now int64) bool {
	if st == nil || st.timeFilter == nil {
		return true
	}

	tf := st.timeFilter
	tf.mu.Lock()
	defer tf.mu.Unlock()

	if tf.minimum <= 0 {
		return true
	}
	if tf.last == nil {
		tf.last = make(map[[16]byte]int64)
	}

	last, seen := tf.last[handle]
	if !seen {
		tf.last[handle] = now

		return true
	}
	if now-last < tf.minimum {
		tf.filtered++

		return false
	}

	tf.last[handle] = now

	return true
}

// perInstanceOutputBoundedLocked reports whether one instance already
// filled its per-instance bound in the output batch.
//
// Unlimited bounds never bound. It keeps retention bounded in the
// returned collection without hiding every sample once storage overflows.
// The caller must hold st.mu.
func (st *readerState) perInstanceOutputBoundedLocked(out []SampledMessage, handle [16]byte) bool {
	if st == nil {
		return false
	}

	bound := st.limits.MaxSamplesPerInstance
	if bound < 0 {
		return false
	}

	var depth int64
	for _, sample := range out {
		if sample.Info.InstanceHandle == handle {
			depth++
		}
	}

	return depth >= bound
}

// INTERFACE REQUESTS for owners of other layers (not this batch):
//
// writerqos owner (internal/transports/dds/writerqos.go, forbidden here):
// forward core.Ownershipqos4Shared to wire SHARED kind 0 and
// core.Ownershipqos4Exclusive to wire EXCLUSIVE kind 1; reverse the same;
// define the OWNERSHIP PID per DDS-RTPS and carry it in WireQoS.OwnershipKind
// plus encode/parse plus CheckWireCompatibility. Durability kinds
// core.Durabilityqos4Transient to wire 2 and core.Durabilityqos4Persistent
// to wire 3 already have wire constants writerQoSDurabilityTransient and
// writerQoSDurabilityPersistent; allow them in forward plus reverse plus
// announce checks. Liveliness kind 1 stays refused in writerqos as a
// separate layer; core matcher already orders all three kinds.
//
// datapath owner (internal/transports/dds/datapath.go and takehooks.go,
// forbidden here): feed arbitration on every take by calling
// FeedOwnershipqos4Status with the writer liveliness stage (alive true when
// GateDelivery would not drop for liveliness) and the deadline-clean flag
// from snapshotTakeHooks. Lifespan expiry sweeps stay in datapath;
// proposed helper: func SweepLifespan(now int64) with status feeds, not in
// this file.
