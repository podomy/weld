// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

// Package node is the public Go surface over the transport free core.
//
// It wraps internal core with thin aliases and delegating constructors so
// external programs use Weld without importing internal paths. No logic
// lives here: matching, routing, and state machines stay in core. This
// package never imports bus or transports.
package node

import (
	"fmt"

	"github.com/podomy/weld/internal/core"
)

// QoSProfile is the RMW QoS profile: history plus depth, reliability,
// durability, deadline, lifespan, liveliness, lease duration, and the
// namespace avoidance flag. See core.QoSProfile.
type QoSProfile = core.QoSProfile

// HistoryPolicy names the history kind of a QoS profile.
type HistoryPolicy = core.HistoryPolicy

const (
	// HistorySystemDefault means not set; ResolveSystemDefaults maps it to keep last.
	HistorySystemDefault = core.HistorySystemDefault
	// HistoryKeepLast keeps the newest Depth samples.
	HistoryKeepLast = core.HistoryKeepLast
	// HistoryKeepAll keeps every sample up to the core retention bound.
	HistoryKeepAll = core.HistoryKeepAll
	// HistoryUnknown marks an unrecognized policy and is rejected by Validate.
	HistoryUnknown = core.HistoryUnknown
)

// ReliabilityPolicy names the reliability kind of a QoS profile.
type ReliabilityPolicy = core.ReliabilityPolicy

const (
	// ReliabilitySystemDefault means not set; ResolveSystemDefaults maps it to reliable.
	ReliabilitySystemDefault = core.ReliabilitySystemDefault
	// ReliabilityReliable retries until delivery.
	ReliabilityReliable = core.ReliabilityReliable
	// ReliabilityBestEffort sends once without retry.
	ReliabilityBestEffort = core.ReliabilityBestEffort
	// ReliabilityBestAvailable resolves once at endpoint creation from visible peers.
	ReliabilityBestAvailable = core.ReliabilityBestAvailable
	// ReliabilityUnknown marks an unrecognized policy and is rejected by Validate.
	ReliabilityUnknown = core.ReliabilityUnknown
)

// DurabilityPolicy names the durability kind of a QoS profile.
type DurabilityPolicy = core.DurabilityPolicy

const (
	// DurabilitySystemDefault means not set; ResolveSystemDefaults maps it to volatile.
	DurabilitySystemDefault = core.DurabilitySystemDefault
	// DurabilityTransientLocal keeps history for late joiners.
	DurabilityTransientLocal = core.DurabilityTransientLocal
	// DurabilityVolatile keeps no history for late joiners.
	DurabilityVolatile = core.DurabilityVolatile
	// DurabilityBestAvailable resolves once at endpoint creation from visible peers.
	DurabilityBestAvailable = core.DurabilityBestAvailable
	// DurabilityUnknown marks an unrecognized policy and is rejected by Validate.
	DurabilityUnknown = core.DurabilityUnknown
)

// LivelinessPolicy names the liveliness kind of a QoS profile.
type LivelinessPolicy = core.LivelinessPolicy

const (
	// LivelinessSystemDefault means not set; ResolveSystemDefaults maps it to automatic.
	LivelinessSystemDefault = core.LivelinessSystemDefault
	// LivelinessAutomatic asserts liveliness without user action.
	LivelinessAutomatic = core.LivelinessAutomatic
	// LivelinessManualByTopic requires the user to assert liveliness per topic.
	LivelinessManualByTopic = core.LivelinessManualByTopic
	// LivelinessBestAvailable resolves once at endpoint creation from visible peers.
	LivelinessBestAvailable = core.LivelinessBestAvailable
	// LivelinessUnknown marks an unrecognized policy and is rejected by Validate.
	LivelinessUnknown = core.LivelinessUnknown
)

// Duration is the RMW style time base: seconds plus nanoseconds. The zero
// value means unspecified, which is the default for deadline, lifespan, and
// lease duration. See core.Duration.
type Duration = core.Duration

// CompatibilityIssue is one error or warning from a compatibility check,
// tagged with its policy kind for loud mismatch reporting.
type CompatibilityIssue = core.CompatibilityIssue

// CompatibilityReport is the result of an offered versus requested check.
type CompatibilityReport = core.CompatibilityReport

// DefaultQoS returns keep last 10 reliable volatile automatic, the standard default.
func DefaultQoS() QoSProfile {
	return core.DefaultQoS()
}

// SensorDataQoS returns keep last 5 best effort volatile automatic for sensor streams.
func SensorDataQoS() QoSProfile {
	return core.SensorDataQoS()
}

// ParametersQoS returns keep last 1000 reliable volatile automatic for parameters.
func ParametersQoS() QoSProfile {
	return core.ParametersQoS()
}

// ServicesDefaultQoS returns keep last 10 reliable volatile automatic for services.
func ServicesDefaultQoS() QoSProfile {
	return core.ServicesDefaultQoS()
}

// SystemDefaultQoS returns the all system default profile. It must pass
// through ResolveSystemDefaults before entity creation.
func SystemDefaultQoS() QoSProfile {
	return core.SystemDefaultQoS()
}

// BestAvailableQoS returns keep last 10 with best available reliability,
// durability, and liveliness markers for one-shot endpoint resolution.
func BestAvailableQoS() QoSProfile {
	return core.BestAvailableQoS()
}

// FacadeAdvertisedQoS returns the profile Weld advertises on the wire:
// reliable plus transient local with a bounded keep last.
func FacadeAdvertisedQoS() QoSProfile {
	return core.FacadeAdvertisedQoS()
}

// ResolveSystemDefaults returns p with every system default replaced by the
// concrete core default, so vendors converge before entity creation.
func ResolveSystemDefaults(p QoSProfile) QoSProfile {
	return core.ResolveSystemDefaults(p)
}

// Validate rejects profiles that must never reach entity creation: unknown
// policies, invalid enum values, unresolved system defaults, unresolved best
// available markers, keep last with depth zero, negative durations, and
// nanoseconds at or above one second.
func Validate(p QoSProfile) error {
	err := core.Validate(p)
	if err != nil {
		return fmt.Errorf("node: validate: %w", err)
	}
	return nil
}

// CheckCompatibility applies the shared DDS truth table to an offered
// publisher profile versus a requested subscription profile.
func CheckCompatibility(offered, requested QoSProfile) CompatibilityReport {
	return core.CheckCompatibility(offered, requested)
}

// RetentionCapacity returns the history cache bound for a resolved profile:
// depth for keep last clamped to MaxHistoryCacheEntries, the retention bound
// for keep all. Call ResolveSystemDefaults first.
func RetentionCapacity(p QoSProfile) (uint64, error) {
	capacity, err := core.RetentionCapacity(p)
	if err != nil {
		return 0, fmt.Errorf("node: retention capacity: %w", err)
	}
	return capacity, nil
}

// UnspecifiedDuration returns the zero duration, meaning no constraint.
func UnspecifiedDuration() Duration {
	return core.UnspecifiedDuration()
}

// DeliveryBehavior describes how core carries bytes behind the facade.
// See core.DeliveryBehavior.
type DeliveryBehavior = core.DeliveryBehavior

// FacadeDeliveryBehavior returns the core delivery behavior behind the facade.
func FacadeDeliveryBehavior() DeliveryBehavior {
	return core.FacadeDeliveryBehavior()
}

// LatencyBudgetQosPolicy is the latency budget QoS policy. See core.LatencyBudgetQosPolicy.
type LatencyBudgetQosPolicy = core.LatencyBudgetQosPolicy

// Ownershipqos4Policy names the ownership kind of a QoS profile.
type Ownershipqos4Policy = core.Ownershipqos4Policy

const (
	// Ownershipqos4Shared delivers every writer for the instance.
	Ownershipqos4Shared = core.Ownershipqos4Shared
	// Ownershipqos4Exclusive delivers only the arbitrated owner.
	Ownershipqos4Exclusive = core.Ownershipqos4Exclusive
	// Ownershipqos4SystemDefault means not set; ResolveSystemDefaults maps it to shared.
	Ownershipqos4SystemDefault = core.Ownershipqos4SystemDefault
	// Ownershipqos4BestAvailable resolves once at endpoint creation.
	Ownershipqos4BestAvailable = core.Ownershipqos4BestAvailable
	// Ownershipqos4Unknown marks an unrecognized policy and is rejected by Validate.
	Ownershipqos4Unknown = core.Ownershipqos4Unknown
)

// PresentationAccessScope names the presentation access scope kind.
type PresentationAccessScope = core.PresentationAccessScope

const (
	// PresentationInstance scopes coherent and ordered access per instance.
	PresentationInstance = core.PresentationInstance
	// PresentationTopic scopes access across one topic.
	PresentationTopic = core.PresentationTopic
	// PresentationGroup scopes access across the publisher or subscriber group.
	PresentationGroup = core.PresentationGroup
)

// PresentationQosPolicy is the presentation QoS policy. See core.PresentationQosPolicy.
type PresentationQosPolicy = core.PresentationQosPolicy

// TypeConsistencyEnforcementKind names the enforcement kind.
type TypeConsistencyEnforcementKind = core.TypeConsistencyEnforcementKind

const (
	// TypeConsistencyDisallowCoercion requires exact minimal equivalence.
	TypeConsistencyDisallowCoercion = core.TypeConsistencyDisallowCoercion
	// TypeConsistencyAllowCoercion permits coercion via assignability.
	TypeConsistencyAllowCoercion = core.TypeConsistencyAllowCoercion
)

// TypeConsistencyEnforcementQosPolicy is the enforcement policy. See core.TypeConsistencyEnforcementQosPolicy.
type TypeConsistencyEnforcementQosPolicy = core.TypeConsistencyEnforcementQosPolicy

const (
	// Durabilityqos4Transient keeps data in memory beyond the writer lifecycle.
	Durabilityqos4Transient = core.Durabilityqos4Transient
	// Durabilityqos4Persistent keeps data on permanent storage.
	Durabilityqos4Persistent = core.Durabilityqos4Persistent
)

// Livelinessqos4ManualByParticipant requires a participant assert to deduce all entities alive.
const Livelinessqos4ManualByParticipant = core.Livelinessqos4ManualByParticipant

// ParameterEventsQoS returns keep last 1000 reliable volatile automatic for parameter events.
func ParameterEventsQoS() QoSProfile {
	return core.ParameterEventsQoS()
}

// RosoutQoS returns keep last 1000 reliable transient local automatic with a 10 second lifespan.
func RosoutQoS() QoSProfile {
	return core.RosoutQoS()
}

// UnknownQoS returns history unknown with depth 0 and unknown policies for wire input.
func UnknownQoS() QoSProfile {
	return core.UnknownQoS()
}

// ClockQoS returns the profile Weld uses for /clock: keep last depth one best effort volatile.
func ClockQoS() QoSProfile {
	return core.ClockQoS()
}

// NewDuration builds a duration from seconds plus nanoseconds.
func NewDuration(sec int64, nsec uint32) Duration {
	return core.NewDuration(sec, nsec)
}

// DurationFromNanoseconds builds a duration from a total nanosecond count.
func DurationFromNanoseconds(total int64) Duration {
	return core.DurationFromNanoseconds(total)
}

// InfiniteDuration returns a duration with total nanoseconds at max int64.
func InfiniteDuration() Duration {
	return core.InfiniteDuration()
}

// MaxDuration returns the slower (larger total) of a and b.
func MaxDuration(a, b Duration) Duration {
	return core.MaxDuration(a, b)
}

// MinDuration returns the faster (smaller total) of a and b.
func MinDuration(a, b Duration) Duration {
	return core.MinDuration(a, b)
}

// JoinNanos joins seconds plus nanoseconds into one tick exactly.
func JoinNanos(sec int32, nsec uint32) (int64, error) {
	total, err := core.JoinNanos(sec, nsec)
	if err != nil {
		return 0, fmt.Errorf("node: join nanos: %w", err)
	}
	return total, nil
}

// MustJoinNanos joins seconds plus nanoseconds and panics on malformed input.
func MustJoinNanos(sec int32, nsec uint32) int64 {
	return core.MustJoinNanos(sec, nsec)
}

// SplitNanos splits a tick into seconds plus nanoseconds for /clock.
func SplitNanos(nanos int64) (int32, uint32) {
	return core.SplitNanos(nanos)
}

// SimSecondToNanos converts whole sim seconds to nanoseconds exactly.
func SimSecondToNanos(sec int64) int64 {
	return core.SimSecondToNanos(sec)
}

// IsBestAvailableDuration reports whether d is the best available sentinel.
func IsBestAvailableDuration(d Duration) bool {
	return core.IsBestAvailableDuration(d)
}

// BestAvailableDuration returns the sentinel carried by the best available preset.
func BestAvailableDuration() Duration {
	return core.BestAvailableDuration()
}

const (
	// FacadeHistoryDepth is the bounded keep last depth Weld advertises on the wire.
	FacadeHistoryDepth = core.FacadeHistoryDepth
	// KeepAllRetentionBound is the retention bound applied to keep all histories.
	KeepAllRetentionBound = core.KeepAllRetentionBound
	// MaxHistoryCacheEntries caps every history cache entry count.
	MaxHistoryCacheEntries = core.MaxHistoryCacheEntries
	// MaxReasonLength caps the joined compatibility reason string.
	MaxReasonLength = core.MaxReasonLength
	// DefaultHistoryDepth is the keep last depth used when no depth is set.
	DefaultHistoryDepth = core.DefaultHistoryDepth
)
