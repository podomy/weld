// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package core

import "strings"

// HistoryPolicy names the history kind of a QoS profile.
type HistoryPolicy int

// History policies. Best available has no history marker: the preset uses
// keep last with DefaultHistoryDepth.
const (
	// HistorySystemDefault means not set; ResolveSystemDefaults maps it to keep last.
	HistorySystemDefault HistoryPolicy = iota
	// HistoryKeepLast keeps the newest Depth samples.
	HistoryKeepLast
	// HistoryKeepAll keeps every sample up to the core retention bound.
	HistoryKeepAll
	// HistoryUnknown marks an unrecognized policy and is rejected by Validate.
	HistoryUnknown
)

// String renders the policy in lowercase RMW form.
func (p HistoryPolicy) String() string {
	switch p {
	case HistorySystemDefault:
		return "system_default"
	case HistoryKeepLast:
		return "keep_last"
	case HistoryKeepAll:
		return "keep_all"
	case HistoryUnknown:
		return "unknown"
	default:
		return "invalid"
	}
}

// ReliabilityPolicy names the reliability kind of a QoS profile.
type ReliabilityPolicy int

// Reliability policies.
const (
	// ReliabilitySystemDefault means not set; ResolveSystemDefaults maps it to reliable.
	ReliabilitySystemDefault ReliabilityPolicy = iota
	// ReliabilityReliable retries until delivery.
	ReliabilityReliable
	// ReliabilityBestEffort sends once without retry.
	ReliabilityBestEffort
	// ReliabilityBestAvailable resolves once at endpoint creation from
	// visible peers: reliable when at least one peer exists and all are
	// reliable, else best effort.
	ReliabilityBestAvailable
	// ReliabilityUnknown marks an unrecognized policy and is rejected by Validate.
	ReliabilityUnknown
)

// String renders the policy in lowercase RMW form.
func (p ReliabilityPolicy) String() string {
	switch p {
	case ReliabilitySystemDefault:
		return "system_default"
	case ReliabilityReliable:
		return "reliable"
	case ReliabilityBestEffort:
		return "best_effort"
	case ReliabilityBestAvailable:
		return "best_available"
	case ReliabilityUnknown:
		return "unknown"
	default:
		return "invalid"
	}
}

// DurabilityPolicy names the durability kind of a QoS profile.
type DurabilityPolicy int

// Durability policies.
const (
	// DurabilitySystemDefault means not set; ResolveSystemDefaults maps it to volatile.
	DurabilitySystemDefault DurabilityPolicy = iota
	// DurabilityTransientLocal keeps history for late joiners.
	DurabilityTransientLocal
	// DurabilityVolatile keeps no history for late joiners.
	DurabilityVolatile
	// DurabilityBestAvailable resolves once at endpoint creation from visible
	// peers: transient local when all visible peers are transient local,
	// else volatile.
	DurabilityBestAvailable
	// DurabilityUnknown marks an unrecognized policy and is rejected by Validate.
	DurabilityUnknown
)

// String renders the policy in lowercase RMW form.
func (p DurabilityPolicy) String() string {
	switch p {
	case DurabilitySystemDefault:
		return "system_default"
	case DurabilityTransientLocal:
		return "transient_local"
	case DurabilityVolatile:
		return "volatile"
	case DurabilityBestAvailable:
		return "best_available"
	case DurabilityUnknown:
		return "unknown"
	default:
		return "invalid"
	}
}

// LivelinessPolicy names the liveliness kind of a QoS profile.
type LivelinessPolicy int

// Liveliness policies. Core models automatic plus manual by topic, the two
// kinds on the standard ROS wire.
const (
	// LivelinessSystemDefault means not set; ResolveSystemDefaults maps it to automatic.
	LivelinessSystemDefault LivelinessPolicy = iota
	// LivelinessAutomatic asserts liveliness without user action.
	LivelinessAutomatic
	// LivelinessManualByTopic requires the user to assert liveliness per topic.
	LivelinessManualByTopic
	// LivelinessBestAvailable resolves once at endpoint creation from visible
	// peers, following the subscriber and publisher rules in endpoint.go.
	LivelinessBestAvailable
	// LivelinessUnknown marks an unrecognized policy and is rejected by Validate.
	LivelinessUnknown
)

// String renders the policy in lowercase RMW form.
func (p LivelinessPolicy) String() string {
	switch p {
	case LivelinessSystemDefault:
		return "system_default"
	case LivelinessAutomatic:
		return "automatic"
	case LivelinessManualByTopic:
		return "manual_by_topic"
	case LivelinessBestAvailable:
		return "best_available"
	case LivelinessUnknown:
		return "unknown"
	default:
		return "invalid"
	}
}

// QoSProfile is the RMW QoS profile: history plus depth, reliability,
// durability, deadline, lifespan, liveliness, lease duration, and the
// namespace avoidance flag.
//
// Deadline is publisher only as an offer (freshness promise) and subscriber
// only as a request (freshness expectation). Lifespan is publisher only and
// names message expiry. Liveliness names endpoint aliveness with lease
// duration naming how long silence means gone. The avoidance flag skips the
// rt prefix for native DDS topics; it never affects matching and is never
// resolved, so actual getters expose it unchanged.
type QoSProfile struct {
	// History is keep last, keep all, system default, or unknown.
	History HistoryPolicy
	// Depth is meaningful for keep last only. Zero means not set.
	Depth uint64
	// Reliability is reliable, best effort, system default, best available, or unknown.
	Reliability ReliabilityPolicy
	// Durability is transient local, volatile, system default, best available, or unknown.
	Durability DurabilityPolicy
	// Deadline is the freshness contract. Unspecified means the default.
	Deadline Duration
	// Lifespan is publisher only message expiry. Unspecified means no expiry.
	Lifespan Duration
	// Liveliness is automatic, manual by topic, system default, best available, or unknown.
	Liveliness LivelinessPolicy
	// LeaseDuration names how long silence means gone. Unspecified means the default.
	LeaseDuration Duration
	// AvoidROSNamespaceConventions skips the rt prefix for native DDS topics.
	AvoidROSNamespaceConventions bool
}

// DefaultQoS returns keep last 10 reliable volatile automatic, the standard default.
//
// Liveliness is concrete automatic so the preset validates without a prior
// ResolveSystemDefaults call. Only SystemDefaultQoS carries system default.
func DefaultQoS() QoSProfile {
	return QoSProfile{
		History:     HistoryKeepLast,
		Depth:       DefaultHistoryDepth,
		Reliability: ReliabilityReliable,
		Durability:  DurabilityVolatile,
		Liveliness:  LivelinessAutomatic,
	}
}

// SensorDataQoS returns keep last 5 best effort volatile automatic for sensor streams.
//
// Liveliness is concrete automatic so the preset validates without a prior
// ResolveSystemDefaults call.
func SensorDataQoS() QoSProfile {
	return QoSProfile{
		History:     HistoryKeepLast,
		Depth:       5,
		Reliability: ReliabilityBestEffort,
		Durability:  DurabilityVolatile,
		Liveliness:  LivelinessAutomatic,
	}
}

// ParametersQoS returns keep last 1000 reliable volatile automatic for parameters.
//
// Liveliness is concrete automatic so the preset validates without a prior
// ResolveSystemDefaults call.
func ParametersQoS() QoSProfile {
	return QoSProfile{
		History:     HistoryKeepLast,
		Depth:       1000,
		Reliability: ReliabilityReliable,
		Durability:  DurabilityVolatile,
		Liveliness:  LivelinessAutomatic,
	}
}

// ServicesDefaultQoS returns keep last 10 reliable volatile automatic for services.
//
// Services resolve best available per field at endpoint creation: each marker
// field takes this profile, explicit non marker fields survive (see
// resolveServiceBestAvailable in endpoint.go), not whole profile replacement.
// Liveliness is concrete automatic so the preset validates without a prior
// ResolveSystemDefaults call.
func ServicesDefaultQoS() QoSProfile {
	return QoSProfile{
		History:     HistoryKeepLast,
		Depth:       DefaultHistoryDepth,
		Reliability: ReliabilityReliable,
		Durability:  DurabilityVolatile,
		Liveliness:  LivelinessAutomatic,
	}
}

// ParameterEventsQoS returns keep last 1000 reliable volatile automatic for parameter events.
//
// Liveliness is concrete automatic so the preset validates without a prior
// ResolveSystemDefaults call.
func ParameterEventsQoS() QoSProfile {
	return QoSProfile{
		History:     HistoryKeepLast,
		Depth:       1000,
		Reliability: ReliabilityReliable,
		Durability:  DurabilityVolatile,
		Liveliness:  LivelinessAutomatic,
	}
}

// RosoutQoS returns keep last 1000 reliable transient local automatic with a
// 10 second lifespan for rosout logging.
//
// Liveliness is concrete automatic so the preset validates without a prior
// ResolveSystemDefaults call.
func RosoutQoS() QoSProfile {
	return QoSProfile{
		History:     HistoryKeepLast,
		Depth:       1000,
		Reliability: ReliabilityReliable,
		Durability:  DurabilityTransientLocal,
		Lifespan:    NewDuration(10, 0),
		Liveliness:  LivelinessAutomatic,
	}
}

// SystemDefaultQoS returns the all system default profile with depth 0
// meaning not set and all durations unspecified.
//
// It must pass through ResolveSystemDefaults before entity creation.
func SystemDefaultQoS() QoSProfile {
	return QoSProfile{
		History:     HistorySystemDefault,
		Depth:       0,
		Reliability: ReliabilitySystemDefault,
		Durability:  DurabilitySystemDefault,
		Liveliness:  LivelinessSystemDefault,
	}
}

// BestAvailableQoS returns keep last 10 with best available reliability,
// durability, and liveliness markers and infinite minus one durations.
//
// Endpoint creation resolves each marker once from visible peers and records
// the peer set; later discovery never updates the stored profile. Lifespan
// carries the sentinel too so hasBestAvailable sees the preset even when
// only durations differ; the service path then merges per field (each marker
// takes the services default, explicit fields survive) and lifespan resolves
// to unspecified because expiry is publisher local. Lifespan never warns in
// matching; Validate still rejects a surviving sentinel loudly so it can
// never reach the wire or entity creation.
func BestAvailableQoS() QoSProfile {
	sentinel := BestAvailableDuration()
	return QoSProfile{
		History:       HistoryKeepLast,
		Depth:         DefaultHistoryDepth,
		Reliability:   ReliabilityBestAvailable,
		Durability:    DurabilityBestAvailable,
		Deadline:      sentinel,
		Lifespan:      sentinel,
		Liveliness:    LivelinessBestAvailable,
		LeaseDuration: sentinel,
	}
}

// UnknownQoS returns history unknown with depth 0 and unknown policies.
//
// It exists so unknown wire input has a representation. Validate rejects it
// and CheckCompatibility warns on it; it must never reach entity creation.
func UnknownQoS() QoSProfile {
	return QoSProfile{
		History:     HistoryUnknown,
		Depth:       0,
		Reliability: ReliabilityUnknown,
		Durability:  DurabilityUnknown,
		Liveliness:  LivelinessUnknown,
	}
}

// PolicyKind names one RxO policy in compatibility results and mismatch events.
type PolicyKind string

// RxO policy kinds. History, depth, lifespan, and the avoidance flag never
// affect matching and have no kind here.
const (
	// PolicyReliability marks reliability incompatibilities.
	PolicyReliability PolicyKind = "reliability"
	// PolicyDurability marks durability incompatibilities.
	PolicyDurability PolicyKind = "durability"
	// PolicyDeadline marks deadline incompatibilities.
	PolicyDeadline PolicyKind = "deadline"
	// PolicyLiveliness marks liveliness incompatibilities.
	PolicyLiveliness PolicyKind = "liveliness"
	// PolicyLeaseDuration marks lease duration incompatibilities.
	PolicyLeaseDuration PolicyKind = "lease_duration"
)

// CompatibilityIssue is one error or warning from a compatibility check,
// tagged with its policy kind for loud mismatch reporting.
type CompatibilityIssue struct {
	// Kind is the RxO policy this issue belongs to.
	Kind PolicyKind
	// Detail is the human readable reason in lowercase RMW value form.
	Detail string
}

// String renders the issue as kind plus detail for reason joining.
func (i CompatibilityIssue) String() string {
	return string(i.Kind) + ": " + i.Detail
}

// CompatibilityReport is the result of an offered versus requested check.
//
// Compatible is false when any error is present. Warnings appear only when
// no error is present. History, depth, lifespan, and the avoidance flag
// never contribute issues. Reports never silently unmatch: every mismatch is
// returned loudly as an Error, and runtime deadline or liveliness loss is
// separately signaled with alerts in deadline.go. Facade monitoring surfaces the
// alerts while this truth table owns the matching errors, so callers must
// surface both.
type CompatibilityReport struct {
	// Compatible is true when the offered profile satisfies the request.
	Compatible bool
	// Errors holds one entry per incompatible RxO policy.
	Errors []CompatibilityIssue
	// Warnings holds one entry per unresolved marker, only when Errors is empty.
	Warnings []CompatibilityIssue
}

// Reason joins errors before warnings with semicolons, truncated to
// MaxReasonLength bytes. Empty when the pair is clean. Reason is the only
// bounded text path required by the shared DDS truth table port; ErrorText
// and WarningText truncate to the same bound for the same reason.
func (r CompatibilityReport) Reason() string {
	var parts []string
	for _, issue := range r.Errors {
		parts = append(parts, issue.String())
	}
	for _, issue := range r.Warnings {
		parts = append(parts, issue.String())
	}
	joined := strings.Join(parts, "; ")
	return truncateText(joined)
}

// ErrorText joins only the error issues with semicolons, truncated to
// MaxReasonLength bytes like Reason.
func (r CompatibilityReport) ErrorText() string {
	return truncateText(joinIssues(r.Errors))
}

// WarningText joins only the warning issues with semicolons, truncated to
// MaxReasonLength bytes like Reason.
func (r CompatibilityReport) WarningText() string {
	return truncateText(joinIssues(r.Warnings))
}

// truncateText caps text to MaxReasonLength bytes.
func truncateText(s string) string {
	if len(s) > MaxReasonLength {
		return s[:MaxReasonLength]
	}
	return s
}

// joinIssues renders issues as kind plus detail joined with semicolons.
func joinIssues(issues []CompatibilityIssue) string {
	var parts []string
	for _, issue := range issues {
		parts = append(parts, issue.String())
	}
	return strings.Join(parts, "; ")
}
