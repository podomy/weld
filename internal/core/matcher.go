// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

// Package core implements transport free matching, QoS, routing, and state
// machines for Weld.
//
// This file owns the RMW QoS profile model and the offered versus requested
// (RxO) compatibility truth table shared by every transport backend.
// internal/core never imports internal/transports: matching lives here once
// so DDS, Zenoh, and MQTT backends behave identically by construction.
//
// The profile answers only the matching question. Runtime behavior such as
// backpressure, bounded retention, overflow drops, and deadline alerts lives
// in facade.go, retention.go, and deadline.go. Transports translate between this model and vendor structures
// at the border and must never invent their own matching rules.
package core

import (
	"fmt"
	"strings"
)

// MaxReasonLength and DefaultHistoryDepth live here in matcher.go (home); qos_types.go presets and report helpers use them from here.

// MaxReasonLength caps the joined compatibility reason string.
//
// It ports the suggested 2048 byte reason buffer from the shared DDS truth
// table into Go: Reason truncates to this many bytes.
const MaxReasonLength = 2048

// DefaultHistoryDepth is the keep last depth used when no depth is set.
//
// Depth zero means not set. CycloneDDS maps an unset keep last depth to 1
// and Zenoh maps zero to 42; core normalizes to 10 before entity creation so
// every vendor converges, and ActualQoS exposes the resolved value.
const DefaultHistoryDepth = 10

// ResolveSystemDefaults returns p with every system default replaced by the
// concrete core default, so vendors converge before entity creation.
//
// Mapping: history system default becomes keep last; keep last with depth
// zero becomes DefaultHistoryDepth (CycloneDDS would use 1 and Zenoh would
// use 42; core normalizes to 10 and ActualQoS exposes the resolved value);
// reliability becomes reliable; durability becomes volatile; liveliness
// becomes automatic; unspecified deadline and unspecified lease duration
// become infinite, which is the wire encoding of no constraint. Transports
// must map infinite deadline and lease to their vendor infinite and must not
// invent a separate default encoding, so core profiles interoperate after
// this call. Keep all depth is preserved verbatim but ignored. Best available
// and unknown markers pass through for endpoint resolution and validation.
// Unspecified lifespan passes through untouched because lifespan is publisher
// side expiry, not an RxO promise. The avoidance flag is never resolved, per
// the actual getter rule.
func ResolveSystemDefaults(p QoSProfile) QoSProfile {
	out := p
	if out.History == HistorySystemDefault {
		out.History = HistoryKeepLast
	}
	if out.History == HistoryKeepLast && out.Depth == 0 {
		out.Depth = DefaultHistoryDepth
	}
	if out.Reliability == ReliabilitySystemDefault {
		out.Reliability = ReliabilityReliable
	}
	if out.Durability == DurabilitySystemDefault {
		out.Durability = DurabilityVolatile
	}
	if out.Liveliness == LivelinessSystemDefault {
		out.Liveliness = LivelinessAutomatic
	}
	if out.Deadline.IsUnspecified() {
		out.Deadline = InfiniteDuration()
	}
	if out.LeaseDuration.IsUnspecified() {
		out.LeaseDuration = InfiniteDuration()
	}
	return out
}

// Validate rejects profiles that must never reach entity creation: unknown
// policies, invalid enum values, unresolved system defaults, unresolved best
// available markers including sentinel durations, keep last with depth zero,
// negative durations, and nanoseconds at or above one second.
//
// Keep all accepts any depth including zero because depth is ignored for keep
// all. Callers resolve with ResolveSystemDefaults and endpoint creation
// first; Validate enforces that order loudly instead of failing late per vendor.
// Nanosecond overflow must normalize with NewDuration before validation;
// a literal Duration with Nanoseconds >= 1e9 is malformed and rejected.
func Validate(p QoSProfile) error {
	var problems []string
	problems = append(problems, validateHistory(p)...)
	problems = append(problems, validateReliability(p)...)
	problems = append(problems, validateDurability(p)...)
	problems = append(problems, validateLiveliness(p)...)
	problems = append(problems, validateDurations(p)...)
	if len(problems) == 0 {
		return nil
	}
	return fmt.Errorf("core: invalid QoS profile: %s", strings.Join(problems, "; "))
}

// validateHistory rejects unresolved, unknown, and invalid history plus
// keep last with depth zero meaning not set.
func validateHistory(p QoSProfile) []string {
	var problems []string
	switch p.History {
	case HistoryKeepLast:
		if p.Depth == 0 {
			problems = append(problems, "history: keep last depth 0 means not set; call ResolveSystemDefaults first")
		}
	case HistoryKeepAll:
	case HistorySystemDefault:
		problems = append(problems, "history: system default is not resolved; call ResolveSystemDefaults first")
	case HistoryUnknown:
		problems = append(problems, "history: unknown policy is not supported")
	default:
		problems = append(problems, fmt.Sprintf("history: invalid policy %d", p.History))
	}
	return problems
}

// validateReliability rejects unresolved markers and invalid reliability values.
func validateReliability(p QoSProfile) []string {
	var problems []string
	switch p.Reliability {
	case ReliabilityReliable, ReliabilityBestEffort:
	case ReliabilitySystemDefault:
		problems = append(problems, "reliability: system default is not resolved; call ResolveSystemDefaults first")
	case ReliabilityBestAvailable:
		problems = append(problems, "reliability: best available is not resolved; resolve at endpoint creation first")
	case ReliabilityUnknown:
		problems = append(problems, "reliability: unknown policy is not supported")
	default:
		problems = append(problems, fmt.Sprintf("reliability: invalid policy %d", p.Reliability))
	}
	return problems
}

// validateDurability rejects unresolved markers and invalid durability values.
func validateDurability(p QoSProfile) []string {
	var problems []string
	switch p.Durability {
	case DurabilityTransientLocal, DurabilityVolatile:
	case DurabilitySystemDefault:
		problems = append(problems, "durability: system default is not resolved; call ResolveSystemDefaults first")
	case DurabilityBestAvailable:
		problems = append(problems, "durability: best available is not resolved; resolve at endpoint creation first")
	case DurabilityUnknown:
		problems = append(problems, "durability: unknown policy is not supported")
	default:
		problems = append(problems, fmt.Sprintf("durability: invalid policy %d", p.Durability))
	}
	return problems
}

// validateLiveliness rejects unresolved markers and invalid liveliness values.
func validateLiveliness(p QoSProfile) []string {
	var problems []string
	switch p.Liveliness {
	case LivelinessAutomatic, LivelinessManualByTopic:
	case LivelinessSystemDefault:
		problems = append(problems, "liveliness: system default is not resolved; call ResolveSystemDefaults first")
	case LivelinessBestAvailable:
		problems = append(problems, "liveliness: best available is not resolved; resolve at endpoint creation first")
	case LivelinessUnknown:
		problems = append(problems, "liveliness: unknown policy is not supported")
	default:
		problems = append(problems, fmt.Sprintf("liveliness: invalid policy %d", p.Liveliness))
	}
	return problems
}

// validateDurations rejects negative seconds, nanoseconds at or above one
// second, and surviving best available sentinels on deadline, lifespan, and
// lease duration.
func validateDurations(p QoSProfile) []string {
	var problems []string
	if p.Deadline.Seconds < 0 {
		problems = append(problems, "deadline: negative seconds are malformed")
	}
	if p.Deadline.Nanoseconds >= nanosPerSecond {
		problems = append(problems, "deadline: nanoseconds >= 1e9 are malformed; normalize with NewDuration first")
	}
	if p.Lifespan.Seconds < 0 {
		problems = append(problems, "lifespan: negative seconds are malformed")
	}
	if p.Lifespan.Nanoseconds >= nanosPerSecond {
		problems = append(problems, "lifespan: nanoseconds >= 1e9 are malformed; normalize with NewDuration first")
	}
	if p.LeaseDuration.Seconds < 0 {
		problems = append(problems, "lease duration: negative seconds are malformed")
	}
	if p.LeaseDuration.Nanoseconds >= nanosPerSecond {
		problems = append(problems, "lease duration: nanoseconds >= 1e9 are malformed; normalize with NewDuration first")
	}
	if IsBestAvailableDuration(p.Deadline) {
		problems = append(problems, "deadline: best available is not resolved; resolve at endpoint creation first")
	}
	if IsBestAvailableDuration(p.Lifespan) {
		problems = append(problems, "lifespan: best available is not resolved; resolve at endpoint creation first")
	}
	if IsBestAvailableDuration(p.LeaseDuration) {
		problems = append(problems, "lease duration: best available is not resolved; resolve at endpoint creation first")
	}
	return problems
}

// CheckCompatibility applies the shared DDS truth table to an offered
// publisher profile versus a requested subscription profile.
//
// The six incompatible combinations each yield an error: best effort offered
// with reliable requested; volatile offered with transient local requested;
// default deadline offered with non default requested; requested deadline
// faster than offered; automatic liveliness offered with manual by topic
// requested; default lease offered with non default requested. A requested
// infinite deadline or lease means no constraint and always flows, while an
// offered infinite stays slow and is incompatible with a finite request. All
// other DDS combinations pass, including reliable to best effort, transient
// local to volatile, equal values, and reverse liveliness. History, depth,
// lifespan, and the avoidance flag are never checked.
//
// Best available sentinel durations skip the error check and warn instead,
// so unresolved profiles warn rather than error on durations. Unresolved
// markers (system default, unknown, best available, and any other
// non concrete enum value) on the RxO policies yield warnings, but only
// when no error is present. Callers normally resolve with
// ResolveSystemDefaults and endpoint creation first, so a warning means
// something skipped that order. Callers should Validate before matching;
// CheckCompatibility still guards malformed negative deadline and lease
// durations loudly as errors so skipped validation can never flow silently.
// Matching never silently unmatches: errors are returned loudly and runtime
// deadline or liveliness loss uses alerts, so facades surface both.
func CheckCompatibility(offered, requested QoSProfile) CompatibilityReport {
	report := CompatibilityReport{Compatible: true}
	checkNegativeDurations(offered, requested, &report)
	checkReliability(offered, requested, &report)
	checkDurability(offered, requested, &report)
	checkDeadline(offered, requested, &report)
	checkLiveliness(offered, requested, &report)
	checkLease(offered, requested, &report)
	if len(report.Errors) > 0 {
		report.Compatible = false
		return report
	}
	warnUnresolvedMarkers(offered, requested, &report)
	return report
}

// checkNegativeDurations records an error when either side carries a
// malformed negative deadline or lease duration. Lifespan never participates
// in matching so it is ignored here and rejected loudly by Validate instead.
// This guard runs first so skipped validation still fails loudly in matching.
func checkNegativeDurations(offered, requested QoSProfile, report *CompatibilityReport) {
	if offered.Deadline.Seconds < 0 || requested.Deadline.Seconds < 0 {
		report.Errors = append(report.Errors, CompatibilityIssue{
			Kind:   PolicyDeadline,
			Detail: "malformed negative deadline seconds; call Validate first",
		})
	}
	if offered.LeaseDuration.Seconds < 0 || requested.LeaseDuration.Seconds < 0 {
		report.Errors = append(report.Errors, CompatibilityIssue{
			Kind:   PolicyLeaseDuration,
			Detail: "malformed negative lease duration seconds; call Validate first",
		})
	}
}

// checkReliability records an error when a best effort publisher meets a
// reliable subscription.
func checkReliability(offered, requested QoSProfile, report *CompatibilityReport) {
	if offered.Reliability == ReliabilityBestEffort && requested.Reliability == ReliabilityReliable {
		report.Errors = append(report.Errors, CompatibilityIssue{
			Kind:   PolicyReliability,
			Detail: "offered best_effort cannot satisfy requested reliable",
		})
	}
}

// checkDurability records an error when a volatile publisher meets a
// transient local subscription.
func checkDurability(offered, requested QoSProfile, report *CompatibilityReport) {
	if offered.Durability == DurabilityVolatile && requested.Durability == DurabilityTransientLocal {
		report.Errors = append(report.Errors, CompatibilityIssue{
			Kind:   PolicyDurability,
			Detail: "offered volatile keeps no history for requested transient_local",
		})
	}
}

// checkDeadline records an error when a default deadline publisher meets a
// non default request, or when the requested deadline is faster than the
// offered one. A faster publisher always satisfies a slower request. A
// requested unspecified or infinite means no constraint and always flows,
// while an offered infinite stays slow. Best available sentinels on either
// side skip the error check so the pair warns instead; the marker is checked
// before any String call so sentinel values never leak into error text.
func checkDeadline(offered, requested QoSProfile, report *CompatibilityReport) {
	if IsBestAvailableDuration(offered.Deadline) || IsBestAvailableDuration(requested.Deadline) {
		return
	}
	if requested.Deadline.IsUnspecified() || requested.Deadline.IsInfinite() {
		return
	}
	switch {
	case offered.Deadline.IsUnspecified() && !requested.Deadline.IsUnspecified():
		report.Errors = append(report.Errors, CompatibilityIssue{
			Kind:   PolicyDeadline,
			Detail: "offered default deadline cannot satisfy requested deadline " + requested.Deadline.String(),
		})
	case !offered.Deadline.IsUnspecified() && !requested.Deadline.IsUnspecified() &&
		requested.Deadline.Compare(offered.Deadline) < 0:
		report.Errors = append(report.Errors, CompatibilityIssue{
			Kind: PolicyDeadline,
			Detail: "requested deadline " + requested.Deadline.String() +
				" is faster than offered deadline " + offered.Deadline.String(),
		})
	default:
	}
}

// checkLiveliness records an error when an automatic publisher meets a
// manual by topic subscription. The reverse always flows.
func checkLiveliness(offered, requested QoSProfile, report *CompatibilityReport) {
	if offered.Liveliness == LivelinessAutomatic && requested.Liveliness == LivelinessManualByTopic {
		report.Errors = append(report.Errors, CompatibilityIssue{
			Kind:   PolicyLiveliness,
			Detail: "offered automatic cannot satisfy requested manual_by_topic",
		})
	}
}

// checkLease records an error when a default lease publisher meets a non
// default request. Magnitudes are never compared beyond that. A requested
// unspecified or infinite means no constraint and always flows, while an
// offered unspecified or infinite is the default and cannot satisfy a finite
// request. Best available sentinels on either side skip the error check so
// the pair warns instead; the marker is checked before any String call so
// sentinel values never leak into error text.
func checkLease(offered, requested QoSProfile, report *CompatibilityReport) {
	if IsBestAvailableDuration(offered.LeaseDuration) || IsBestAvailableDuration(requested.LeaseDuration) {
		return
	}
	if requested.LeaseDuration.IsUnspecified() || requested.LeaseDuration.IsInfinite() {
		return
	}
	if offered.LeaseDuration.IsUnspecified() || offered.LeaseDuration.IsInfinite() {
		report.Errors = append(report.Errors, CompatibilityIssue{
			Kind:   PolicyLeaseDuration,
			Detail: "offered default lease cannot satisfy requested lease " + requested.LeaseDuration.String(),
		})
	}
}

// warnUnresolvedMarkers records one warning per RxO policy carrying an
// unresolved marker on either side, including best available sentinel
// durations for deadline and lease. History, depth, lifespan, and the
// avoidance flag never warn: they do not affect matching. Lifespan sentinels
// stay silent here by rule and are rejected loudly by Validate instead.
func warnUnresolvedMarkers(offered, requested QoSProfile, report *CompatibilityReport) {
	warnMarker(report, PolicyReliability, offered.Reliability.String(), requested.Reliability.String(),
		isReliabilityMarker(offered.Reliability) || isReliabilityMarker(requested.Reliability))
	warnMarker(report, PolicyDurability, offered.Durability.String(), requested.Durability.String(),
		isDurabilityMarker(offered.Durability) || isDurabilityMarker(requested.Durability))
	warnMarker(report, PolicyLiveliness, offered.Liveliness.String(), requested.Liveliness.String(),
		isLivelinessMarker(offered.Liveliness) || isLivelinessMarker(requested.Liveliness))
	if IsBestAvailableDuration(offered.Deadline) || IsBestAvailableDuration(requested.Deadline) {
		warnMarker(report, PolicyDeadline, offered.Deadline.String(), requested.Deadline.String(), true)
	}
	if IsBestAvailableDuration(offered.LeaseDuration) || IsBestAvailableDuration(requested.LeaseDuration) {
		warnMarker(report, PolicyLeaseDuration, offered.LeaseDuration.String(), requested.LeaseDuration.String(), true)
	}
}

// warnMarker appends one warning when unresolved is true.
func warnMarker(report *CompatibilityReport, kind PolicyKind, offered, requested string, unresolved bool) {
	if !unresolved {
		return
	}
	report.Warnings = append(report.Warnings, CompatibilityIssue{
		Kind:   kind,
		Detail: "unresolved policy offered " + offered + " requested " + requested + "; resolve before entity creation",
	})
}

// isReliabilityMarker reports whether p still needs resolution: any non
// concrete value, including system default, unknown, best available, and
// invalid enums.
func isReliabilityMarker(p ReliabilityPolicy) bool {
	return p != ReliabilityReliable && p != ReliabilityBestEffort
}

// isDurabilityMarker reports whether p still needs resolution: any non
// concrete value, including system default, unknown, best available, and
// invalid enums.
func isDurabilityMarker(p DurabilityPolicy) bool {
	return p != DurabilityTransientLocal && p != DurabilityVolatile
}

// isLivelinessMarker reports whether p still needs resolution: any non
// concrete value, including system default, unknown, best available, and
// invalid enums.
func isLivelinessMarker(p LivelinessPolicy) bool {
	return p != LivelinessAutomatic && p != LivelinessManualByTopic
}
