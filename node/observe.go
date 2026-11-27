// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

package node

import (
	"github.com/podomy/weld/internal/core"
)

// MismatchEvent is one loud QoS disagreement with topic, endpoint pair, and
// policy kind. Silence in any channel is a defect, never a state.
type MismatchEvent = core.MismatchEvent

// MismatchSeverity names the weight of a mismatch event.
type MismatchSeverity = core.MismatchSeverity

const (
	// SeverityError marks incompatibilities that block delivery.
	SeverityError = core.SeverityError
	// SeverityWarning marks unresolved markers that need ordering fixes.
	SeverityWarning = core.SeverityWarning
)

// PolicyKind names one RxO policy in compatibility results and mismatch events.
type PolicyKind = core.PolicyKind

const (
	// PolicyReliability marks reliability incompatibilities.
	PolicyReliability = core.PolicyReliability
	// PolicyDurability marks durability incompatibilities.
	PolicyDurability = core.PolicyDurability
	// PolicyDeadline marks deadline incompatibilities.
	PolicyDeadline = core.PolicyDeadline
	// PolicyLiveliness marks liveliness incompatibilities.
	PolicyLiveliness = core.PolicyLiveliness
	// PolicyLeaseDuration marks lease duration incompatibilities.
	PolicyLeaseDuration = core.PolicyLeaseDuration
)

// MismatchCounter holds monotonically increasing mismatch counts per policy
// kind plus a total. The counter is caller owned: core keeps no global state.
// The zero value is usable without NewMismatchCounter.
type MismatchCounter = core.MismatchCounter

// DeadlineTracker watches one freshness contract without touching matching. A
// missed deadline fires one alert per outage carrying topic plus endpoint
// names; it never unmatches the pair. Do not copy after first use.
type DeadlineTracker = core.DeadlineTracker

// DeadlineAlert is one fired freshness alarm with topic plus endpoint names.
type DeadlineAlert = core.DeadlineAlert

// LivelinessState names one per-writer aliveness stage.
type LivelinessState = core.LivelinessState

const (
	// LivelinessAlive means the last assert is inside the lease.
	LivelinessAlive = core.LivelinessAlive
	// LivelinessStale means the lease elapsed but hysteresis has not.
	LivelinessStale = core.LivelinessStale
	// LivelinessExpired means lease plus hysteresis elapsed with no assert.
	LivelinessExpired = core.LivelinessExpired
)

// LivelinessTracker watches one writer aliveness contract with lease expiry.
// The zero value is quiet with an unspecified lease that never expires. Do
// not copy a tracker after first use.
type LivelinessTracker = core.LivelinessTracker

// GateDecision names one delivery gating outcome.
type GateDecision = core.GateDecision

const (
	// GateDeliver means the sample may flow to the reader.
	GateDeliver = core.GateDeliver
	// GateHold means the writer is stale: buffer, never deliver, never drop.
	GateHold = core.GateHold
	// GateDrop means the sample must not flow and the reason is loud.
	GateDrop = core.GateDrop
)

// GateOutcome is one pure delivery gating answer with its loud reason.
// Deliver carries no policy and no reason.
type GateOutcome = core.GateOutcome

// HistoryCache is a bounded oldest first retention buffer with a lost counter.
// Push drops the oldest sample once the cache is full and counts every drop
// in Lost. The zero value is unusable; build with NewHistoryCache.
type HistoryCache[T any] = core.HistoryCache[T]

// CompatibilityEvents expands a CheckCompatibility report into per policy
// mismatch events carrying topic, endpoint pair, and policy kind. A clean
// report yields no events.
func CompatibilityEvents(topic, publisher, subscriber string, offered, requested QoSProfile) []MismatchEvent {
	return core.CompatibilityEvents(topic, publisher, subscriber, offered, requested)
}

// NewMismatchCounter returns an empty counter. The zero value is usable too.
func NewMismatchCounter() *MismatchCounter {
	return core.NewMismatchCounter()
}

// NewDeadlineTracker starts tracking topic plus endpoint against deadline,
// with the last sample seen at startNanos.
func NewDeadlineTracker(topic, endpoint string, deadline Duration, startNanos int64) DeadlineTracker {
	return core.NewDeadlineTracker(topic, endpoint, deadline, startNanos)
}

// NewLivelinessTracker starts tracking writer on topic with kind and lease.
// StartNanos is the last assert time; callers pass the creation tick for
// determinism.
func NewLivelinessTracker(topic, writer string, kind LivelinessPolicy, lease Duration, startNanos int64) LivelinessTracker {
	return core.NewLivelinessTracker(topic, writer, kind, lease, startNanos)
}

// NewPairedLivelinessTracker starts tracking with the folded offered versus
// requested lease, so enforcement uses the same inputs the matcher compares.
func NewPairedLivelinessTracker(topic, writer string, kind LivelinessPolicy, offeredLease, requestedLease Duration, startNanos int64) LivelinessTracker {
	return core.NewPairedLivelinessTracker(topic, writer, kind, offeredLease, requestedLease, startNanos)
}

// GateDelivery decides deliver versus hold versus drop for one sample. It is
// pure: it reads only the writer stage plus the deadline miss flag. Priority
// is invalid state, then expired, then stale, then deadline miss, then deliver.
func GateDelivery(writer LivelinessState, deadlineMissed bool) GateOutcome {
	return core.GateDelivery(writer, deadlineMissed)
}

// NewHistoryCache returns an empty cache holding at most capacity samples.
// Capacity must come from RetentionCapacity. Zero panics by design so an
// unresolved profile can never silently become a capacity of one.
func NewHistoryCache[T any](capacity uint64) *HistoryCache[T] {
	return core.NewHistoryCache[T](capacity)
}

const (
	// Policyqos4Ownership marks ownership incompatibilities.
	Policyqos4Ownership = core.Policyqos4Ownership
	// PolicyPresentation marks presentation incompatibilities.
	PolicyPresentation = core.PolicyPresentation
	// PolicyLatencyBudget marks latency budget incompatibilities.
	PolicyLatencyBudget = core.PolicyLatencyBudget
)

// LiveMismatchReporter wires loud mismatch events end to end across logs plus
// metrics plus CLI. See core.LiveMismatchReporter.
type LiveMismatchReporter = core.LiveMismatchReporter

// MismatchLogFunc receives one loud log line per mismatch event.
type MismatchLogFunc = core.MismatchLogFunc

// NewLiveMismatchReporter returns a reporter that records into counter and logs through log.
func NewLiveMismatchReporter(counter *MismatchCounter, log MismatchLogFunc) *LiveMismatchReporter {
	return core.NewLiveMismatchReporter(counter, log)
}

// ShouldDeliver reports whether offered can flow to requested on live pairing.
func ShouldDeliver(offered, requested QoSProfile) bool {
	return core.ShouldDeliver(offered, requested)
}

// PairShouldDeliver reports whether offered publisher profile can flow to requested subscription profile.
func PairShouldDeliver(offered, requested QoSProfile) bool {
	return core.PairShouldDeliver(offered, requested)
}

// ShouldRetainForLateJoiner reports whether offered history must be kept for late joiners.
func ShouldRetainForLateJoiner(offered, requested QoSProfile) bool {
	return core.ShouldRetainForLateJoiner(offered, requested)
}

// DeadlineExceeded reports whether elapsedNanos reaches deadline without state.
func DeadlineExceeded(deadline Duration, elapsedNanos int64) bool {
	return core.DeadlineExceeded(deadline, elapsedNanos)
}

// DeadlineRxOInputs exposes the deadline pair the matcher compares.
func DeadlineRxOInputs(offered, requested QoSProfile) (Duration, Duration) {
	return core.DeadlineRxOInputs(offered, requested)
}

// LeaseRxOInputs exposes the lease pair the matcher compares.
func LeaseRxOInputs(offered, requested QoSProfile) (Duration, Duration) {
	return core.LeaseRxOInputs(offered, requested)
}

// LivelinessRxOInputs exposes the liveliness pair the matcher compares.
func LivelinessRxOInputs(offered, requested QoSProfile) (LivelinessPolicy, LivelinessPolicy) {
	return core.LivelinessRxOInputs(offered, requested)
}

// EffectiveDeadlineDuration folds offered versus requested deadline to one contract.
func EffectiveDeadlineDuration(offered, requested Duration) Duration {
	return core.EffectiveDeadlineDuration(offered, requested)
}

// EffectiveLeaseDuration folds offered versus requested lease to one expiry.
func EffectiveLeaseDuration(offered, requested Duration) Duration {
	return core.EffectiveLeaseDuration(offered, requested)
}

// NewGateMismatchEvent maps one hold or drop outcome to a loud mismatch event.
func NewGateMismatchEvent(topic, publisher, subscriber string, outcome GateOutcome, offered, requested string) (MismatchEvent, bool) {
	return core.NewGateMismatchEvent(topic, publisher, subscriber, outcome, offered, requested)
}

// FormatMismatchMetrics renders metrics exposition text with counts plus policy kinds.
func FormatMismatchMetrics(counter *MismatchCounter) string {
	return core.FormatMismatchMetrics(counter)
}

// DefaultTypeConsistencyEnforcement returns allow coercion per the spec default.
func DefaultTypeConsistencyEnforcement() TypeConsistencyEnforcementQosPolicy {
	return core.DefaultTypeConsistencyEnforcement()
}
