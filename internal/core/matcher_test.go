// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package core

import (
	"math"
	"strings"
	"testing"
)

// TestMatcher covers presets, the six DDS error combos, allowed reverses,
// infinite requested flows, multi-error ordering, warnings, system default
// resolution, validation, and duration handling.
func TestMatcher(t *testing.T) {
	t.Run("presets", checkPresets)
	t.Run("six error combos", checkSixErrorCombos)
	t.Run("reverse combos flow", checkReverseCombosFlow)
	t.Run("infinite requested flows", checkInfiniteRequestedFlows)
	t.Run("multi-error ordering", checkMultiErrorOrdering)
	t.Run("best available warnings", checkBestAvailableWarnings)
	t.Run("never checks history depth lifespan avoid", checkNeverCheckedFields)
	t.Run("warnings", checkWarnings)
	t.Run("invalid enums warn", checkInvalidEnumsWarn)
	t.Run("negative compatibility", checkNegativeCompatibility)
	t.Run("resolve system defaults", checkResolveSystemDefaults)
	t.Run("validate", checkValidate)
	t.Run("durations", checkDurations)
	t.Run("reason truncation", checkReasonTruncation)
}

// checkPresets pins every preset value. Concrete presets carry automatic
// liveliness so they validate without prior Resolve; only SystemDefaultQoS
// carries system default.
func checkPresets(t *testing.T) {
	t.Helper()
	cases := presetCases()
	assertPresetValues(t, cases)
	assertConcretePresetsValidate(t, cases)
	assertBestAvailablePreset(t)
}

// presetCase is one preset expectation.
type presetCase struct {
	name string
	got  QoSProfile
	want QoSProfile
}

// presetCases returns every preset expectation.
func presetCases() []presetCase {
	return []presetCase{
		{"default", DefaultQoS(), QoSProfile{History: HistoryKeepLast, Depth: 10, Reliability: ReliabilityReliable, Durability: DurabilityVolatile, Liveliness: LivelinessAutomatic}},
		{"sensor data", SensorDataQoS(), QoSProfile{History: HistoryKeepLast, Depth: 5, Reliability: ReliabilityBestEffort, Durability: DurabilityVolatile, Liveliness: LivelinessAutomatic}},
		{"parameters", ParametersQoS(), QoSProfile{History: HistoryKeepLast, Depth: 1000, Reliability: ReliabilityReliable, Durability: DurabilityVolatile, Liveliness: LivelinessAutomatic}},
		{"services default", ServicesDefaultQoS(), QoSProfile{History: HistoryKeepLast, Depth: 10, Reliability: ReliabilityReliable, Durability: DurabilityVolatile, Liveliness: LivelinessAutomatic}},
		{"parameter events", ParameterEventsQoS(), QoSProfile{History: HistoryKeepLast, Depth: 1000, Reliability: ReliabilityReliable, Durability: DurabilityVolatile, Liveliness: LivelinessAutomatic}},
		{"rosout", RosoutQoS(), QoSProfile{History: HistoryKeepLast, Depth: 1000, Reliability: ReliabilityReliable, Durability: DurabilityTransientLocal, Lifespan: NewDuration(10, 0), Liveliness: LivelinessAutomatic}},
		{"system default", SystemDefaultQoS(), QoSProfile{History: HistorySystemDefault, Reliability: ReliabilitySystemDefault, Durability: DurabilitySystemDefault, Liveliness: LivelinessSystemDefault}},
		{"unknown", UnknownQoS(), QoSProfile{History: HistoryUnknown, Reliability: ReliabilityUnknown, Durability: DurabilityUnknown, Liveliness: LivelinessUnknown}},
	}
}

// assertPresetValues pins preset values match expectations.
func assertPresetValues(t *testing.T, cases []presetCase) {
	t.Helper()
	for _, tc := range cases {
		if tc.got != tc.want {
			t.Fatalf("%s: got %+v want %+v", tc.name, tc.got, tc.want)
		}
	}
}

// assertConcretePresetsValidate pins concrete presets validate without resolve.
func assertConcretePresetsValidate(t *testing.T, cases []presetCase) {
	t.Helper()
	for _, tc := range cases {
		if isUnresolvedPresetName(tc.name) {
			continue
		}
		if err := Validate(tc.got); err != nil {
			t.Fatalf("%s preset must validate without resolve: %v", tc.name, err)
		}
	}
}

// isUnresolvedPresetName reports whether name carries unresolved markers.
func isUnresolvedPresetName(name string) bool {
	return name == "system default" || name == "unknown"
}

// assertBestAvailablePreset pins the best available preset markers.
func assertBestAvailablePreset(t *testing.T) {
	t.Helper()
	sentinel := BestAvailableDuration()
	got := BestAvailableQoS()
	assertBestAvailableHistory(t, got)
	assertBestAvailableMarkers(t, got)
	if got.Deadline != sentinel || got.LeaseDuration != sentinel || got.Lifespan != sentinel {
		t.Fatalf("best available durations: got %+v", got)
	}
}

// assertBestAvailableHistory pins best available history depth.
func assertBestAvailableHistory(t *testing.T, got QoSProfile) {
	t.Helper()
	if got.History != HistoryKeepLast || got.Depth != DefaultHistoryDepth {
		t.Fatalf("best available history: got %+v", got)
	}
}

// assertBestAvailableMarkers pins best available policy markers.
func assertBestAvailableMarkers(t *testing.T, got QoSProfile) {
	t.Helper()
	if got.Reliability != ReliabilityBestAvailable || got.Durability != DurabilityBestAvailable || got.Liveliness != LivelinessBestAvailable {
		t.Fatalf("best available markers: got %+v", got)
	}
}

// resolvedBase returns a fully resolved profile for table tests so no
// warnings pollute the error assertions.
func resolvedBase(t *testing.T) QoSProfile {
	t.Helper()
	base := ResolveSystemDefaults(DefaultQoS())
	err := Validate(base)
	if err != nil {
		t.Fatalf("resolved default must validate: %v", err)
	}
	return base
}

// checkSixErrorCombos asserts each of the six shared DDS truth table errors.
func checkSixErrorCombos(t *testing.T) {
	t.Helper()
	base := resolvedBase(t)
	cases := []struct {
		name      string
		offered   QoSProfile
		requested QoSProfile
		kind      PolicyKind
	}{
		{"best effort offered reliable requested", withReliability(base, ReliabilityBestEffort), withReliability(base, ReliabilityReliable), PolicyReliability},
		{"volatile offered transient local requested", withDurability(base, DurabilityVolatile), withDurability(base, DurabilityTransientLocal), PolicyDurability},
		{"default deadline offered non default requested", base, withDeadline(base, NewDuration(0, 100000000)), PolicyDeadline},
		{"unspecified deadline offered finite requested", withDeadline(base, UnspecifiedDuration()), withDeadline(base, NewDuration(0, 100000000)), PolicyDeadline},
		{"slower offered deadline", withDeadline(base, NewDuration(0, 100000000)), withDeadline(base, NewDuration(0, 10000000)), PolicyDeadline},
		{"automatic offered manual requested", base, withLiveliness(base, LivelinessManualByTopic), PolicyLiveliness},
		{"default lease offered non default requested", base, withLease(base, NewDuration(4, 0)), PolicyLeaseDuration},
		{"unspecified lease offered finite requested", withLease(base, UnspecifiedDuration()), withLease(base, NewDuration(4, 0)), PolicyLeaseDuration},
	}
	for _, tc := range cases {
		report := CheckCompatibility(tc.offered, tc.requested)
		if report.Compatible {
			t.Fatalf("%s: expected incompatible", tc.name)
		}
		if len(report.Errors) != 1 {
			t.Fatalf("%s: expected one error, got %+v", tc.name, report.Errors)
		}
		if report.Errors[0].Kind != tc.kind {
			t.Fatalf("%s: error kind got %s want %s", tc.name, report.Errors[0].Kind, tc.kind)
		}
		if len(report.Warnings) != 0 {
			t.Fatalf("%s: warnings must be empty when errors exist, got %+v", tc.name, report.Warnings)
		}
		if !strings.Contains(report.Reason(), string(tc.kind)) {
			t.Fatalf("%s: reason %q misses kind %s", tc.name, report.Reason(), tc.kind)
		}
		if !strings.Contains(report.ErrorText(), string(tc.kind)) {
			t.Fatalf("%s: error text %q misses kind %s", tc.name, report.ErrorText(), tc.kind)
		}
		if report.WarningText() != "" {
			t.Fatalf("%s: warning text must be empty, got %q", tc.name, report.WarningText())
		}
	}
}

// checkReverseCombosFlow asserts every allowed reverse combination passes.
func checkReverseCombosFlow(t *testing.T) {
	t.Helper()
	base := resolvedBase(t)
	faster := withDeadline(base, NewDuration(0, 10000000))
	slower := withDeadline(base, NewDuration(0, 100000000))
	manual := withLiveliness(base, LivelinessManualByTopic)
	cases := []struct {
		name      string
		offered   QoSProfile
		requested QoSProfile
	}{
		{"reliable offered best effort requested", base, withReliability(base, ReliabilityBestEffort)},
		{"transient local offered volatile requested", withDurability(base, DurabilityTransientLocal), base},
		{"equal profiles", base, base},
		{"reverse liveliness", manual, base},
		{"equal manual liveliness", manual, manual},
		{"faster offered deadline", faster, slower},
		{"equal deadlines", slower, slower},
		{"finite offered default requested deadline", slower, base},
		{"finite offered default requested lease", withLease(base, NewDuration(4, 0)), base},
		{"equal leases", withLease(base, NewDuration(4, 0)), withLease(base, NewDuration(4, 0))},
		{"infinite offered infinite requested deadline", withDeadline(base, InfiniteDuration()), withDeadline(base, InfiniteDuration())},
	}
	for _, tc := range cases {
		report := CheckCompatibility(tc.offered, tc.requested)
		if !report.Compatible {
			t.Fatalf("%s: expected compatible, got %q", tc.name, report.Reason())
		}
		if len(report.Errors) != 0 || len(report.Warnings) != 0 {
			t.Fatalf("%s: expected no issues, got %+v", tc.name, report)
		}
		if report.Reason() != "" {
			t.Fatalf("%s: expected empty reason, got %q", tc.name, report.Reason())
		}
	}
}

// checkNeverCheckedFields asserts history, depth, lifespan, and the avoid
// flag differences never produce errors or warnings.
func checkNeverCheckedFields(t *testing.T) {
	t.Helper()
	base := resolvedBase(t)
	other := base
	other.History = HistoryKeepAll
	other.Depth = 1
	other.Lifespan = NewDuration(30, 0)
	other.AvoidROSNamespaceConventions = true
	for _, pair := range [][2]QoSProfile{{base, other}, {other, base}} {
		report := CheckCompatibility(pair[0], pair[1])
		if !report.Compatible {
			t.Fatalf("history/depth/lifespan/avoid must never block: %q", report.Reason())
		}
		if len(report.Errors) != 0 || len(report.Warnings) != 0 {
			t.Fatalf("history/depth/lifespan/avoid must never warn: %+v", report)
		}
	}
}

// checkWarnings asserts unknown markers warn without blocking.
func checkWarnings(t *testing.T) {
	t.Helper()
	unknown := UnknownQoS()
	report := CheckCompatibility(unknown, unknown)
	if !report.Compatible {
		t.Fatalf("unknown versus unknown must stay compatible, got %q", report.Reason())
	}
	if len(report.Errors) != 0 {
		t.Fatalf("unknown versus unknown must not error, got %+v", report.Errors)
	}
	if len(report.Warnings) == 0 {
		t.Fatal("unknown versus unknown must warn")
	}
	if !strings.Contains(report.Reason(), string(PolicyReliability)) {
		t.Fatalf("warning reason must carry policy kind, got %q", report.Reason())
	}

	strict := resolvedBase(t)
	report = CheckCompatibility(unknown, strict)
	if !report.Compatible || len(report.Errors) != 0 || len(report.Warnings) == 0 {
		t.Fatalf("unknown publisher against strict subscription must warn only, got %+v", report)
	}

	report = CheckCompatibility(SystemDefaultQoS(), SystemDefaultQoS())
	if !report.Compatible || len(report.Errors) != 0 || len(report.Warnings) == 0 {
		t.Fatalf("system defaults must warn only, got %+v", report)
	}
}

// checkInvalidEnumsWarn asserts an invalid enum value stays compatible but
// warns, because any non concrete policy counts as an unresolved marker.
func checkInvalidEnumsWarn(t *testing.T) {
	t.Helper()
	base := resolvedBase(t)
	cases := []struct {
		name      string
		offered   QoSProfile
		requested QoSProfile
		kind      PolicyKind
	}{
		{"invalid reliability offered", withReliability(base, ReliabilityPolicy(99)), base, PolicyReliability},
		{"invalid reliability requested", base, withReliability(base, ReliabilityPolicy(99)), PolicyReliability},
		{"invalid durability offered", withDurability(base, DurabilityPolicy(99)), base, PolicyDurability},
		{"invalid durability requested", base, withDurability(base, DurabilityPolicy(99)), PolicyDurability},
		{"invalid liveliness offered", withLiveliness(base, LivelinessPolicy(99)), base, PolicyLiveliness},
		{"invalid liveliness requested", base, withLiveliness(base, LivelinessPolicy(99)), PolicyLiveliness},
	}
	for _, tc := range cases {
		report := CheckCompatibility(tc.offered, tc.requested)
		if !report.Compatible {
			t.Fatalf("%s must stay compatible, got %q", tc.name, report.Reason())
		}
		if len(report.Errors) != 0 {
			t.Fatalf("%s must not error, got %+v", tc.name, report.Errors)
		}
		if len(report.Warnings) == 0 {
			t.Fatalf("%s must warn, got %+v", tc.name, report)
		}
		found := false
		for _, w := range report.Warnings {
			if w.Kind == tc.kind {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("%s must warn on %s, got %+v", tc.name, tc.kind, report.Warnings)
		}
	}
}

// checkNegativeCompatibility asserts malformed negative deadline and lease
// durations fail loudly in matching on either side, even without Validate.
func checkNegativeCompatibility(t *testing.T) {
	t.Helper()
	base := resolvedBase(t)
	cases := []struct {
		name      string
		offered   QoSProfile
		requested QoSProfile
		kind      PolicyKind
	}{
		{"negative offered deadline", withDeadline(base, Duration{Seconds: -1}), base, PolicyDeadline},
		{"negative requested deadline", base, withDeadline(base, Duration{Seconds: -1}), PolicyDeadline},
		{"negative offered lease", withLease(base, Duration{Seconds: -2}), base, PolicyLeaseDuration},
		{"negative requested lease", base, withLease(base, Duration{Seconds: -2}), PolicyLeaseDuration},
	}
	for _, tc := range cases {
		report := CheckCompatibility(tc.offered, tc.requested)
		if report.Compatible {
			t.Fatalf("%s must be incompatible", tc.name)
		}
		if len(report.Errors) == 0 {
			t.Fatalf("%s must error, got %+v", tc.name, report)
		}
		if report.Errors[0].Kind != tc.kind {
			t.Fatalf("%s error kind got %s want %s: %+v", tc.name, report.Errors[0].Kind, tc.kind, report.Errors)
		}
		if len(report.Warnings) != 0 {
			t.Fatalf("%s warnings must be empty when errors exist, got %+v", tc.name, report.Warnings)
		}
	}
}

// checkResolveSystemDefaults pins the explicit resolution mapping and the
// documented depth zero normalization: core uses DefaultHistoryDepth where
// CycloneDDS would use 1 and Zenoh would use 42. Unspecified deadline and
// lease become infinite, which is the wire encoding of no constraint.
func checkResolveSystemDefaults(t *testing.T) {
	t.Helper()
	assertSystemDefaultMapping(t)
	assertDepthNormalization(t)
	assertKeepAllPreserved(t)
	assertMarkerPassthrough(t)
	assertResolvePreservesFlags(t)
}

// assertSystemDefaultMapping pins system default to concrete mapping.
func assertSystemDefaultMapping(t *testing.T) {
	t.Helper()
	got := ResolveSystemDefaults(SystemDefaultQoS())
	want := QoSProfile{History: HistoryKeepLast, Depth: DefaultHistoryDepth, Reliability: ReliabilityReliable, Durability: DurabilityVolatile, Liveliness: LivelinessAutomatic, Deadline: InfiniteDuration(), LeaseDuration: InfiniteDuration()}
	if got != want {
		t.Fatalf("resolve system default: got %+v want %+v", got, want)
	}
	if DefaultHistoryDepth != 10 {
		t.Fatalf("default depth changed: got %d", DefaultHistoryDepth)
	}
}

// assertDepthNormalization pins keep last depth zero normalization.
func assertDepthNormalization(t *testing.T) {
	t.Helper()
	zeroDepth := QoSProfile{History: HistoryKeepLast, Reliability: ReliabilityReliable, Durability: DurabilityVolatile, Liveliness: LivelinessAutomatic}
	if ResolveSystemDefaults(zeroDepth).Depth != DefaultHistoryDepth {
		t.Fatalf("keep last depth zero must normalize to %d", DefaultHistoryDepth)
	}
}

// assertKeepAllPreserved pins keep all depth is preserved.
func assertKeepAllPreserved(t *testing.T) {
	t.Helper()
	keepAll := QoSProfile{History: HistoryKeepAll, Depth: 7, Reliability: ReliabilityReliable, Durability: DurabilityVolatile, Liveliness: LivelinessAutomatic}
	if out := ResolveSystemDefaults(keepAll); out.Depth != 7 {
		t.Fatalf("keep all depth is ignored but preserved: got %+v", out)
	}
}

// assertMarkerPassthrough pins best available and unknown pass through.
func assertMarkerPassthrough(t *testing.T) {
	t.Helper()
	marked := BestAvailableQoS()
	if out := ResolveSystemDefaults(marked); out != marked {
		t.Fatalf("best available markers must pass through: got %+v", out)
	}
	assertUnknownPassthrough(t)
}

// assertUnknownPassthrough pins unknown markers pass through with infinite durations.
func assertUnknownPassthrough(t *testing.T) {
	t.Helper()
	unknownOut := ResolveSystemDefaults(UnknownQoS())
	if unknownOut.History != HistoryUnknown || unknownOut.Reliability != ReliabilityUnknown || unknownOut.Durability != DurabilityUnknown || unknownOut.Liveliness != LivelinessUnknown {
		t.Fatalf("unknown markers must pass through: got %+v", unknownOut)
	}
	if !unknownOut.Deadline.IsInfinite() || !unknownOut.LeaseDuration.IsInfinite() {
		t.Fatalf("unknown unspecified durations must normalize to infinite: got %+v", unknownOut)
	}
}

// assertResolvePreservesFlags pins avoid, lifespan, infinite pass through.
func assertResolvePreservesFlags(t *testing.T) {
	t.Helper()
	avoided := resolvedBase(t)
	avoided.AvoidROSNamespaceConventions = true
	if out := ResolveSystemDefaults(avoided); !out.AvoidROSNamespaceConventions {
		t.Fatal("avoid flag must pass through unresolved")
	}
	assertLifespanPassthrough(t)
	assertInfinitePassthrough(t)
}

// assertLifespanPassthrough pins unspecified lifespan passes through.
func assertLifespanPassthrough(t *testing.T) {
	t.Helper()
	lifespanUnset := QoSProfile{History: HistoryKeepLast, Depth: 10, Reliability: ReliabilityReliable, Durability: DurabilityVolatile, Liveliness: LivelinessAutomatic}
	if out := ResolveSystemDefaults(lifespanUnset); !out.Lifespan.IsUnspecified() {
		t.Fatalf("unspecified lifespan must pass through untouched: got %+v", out)
	}
}

// assertInfinitePassthrough pins infinite deadline and lease pass through.
func assertInfinitePassthrough(t *testing.T) {
	t.Helper()
	alreadyInfinite := withDeadline(withLease(resolvedBase(t), InfiniteDuration()), InfiniteDuration())
	if out := ResolveSystemDefaults(alreadyInfinite); out != alreadyInfinite {
		t.Fatalf("infinite deadline and lease must pass through: got %+v", out)
	}
}

// checkValidate pins early rejection of unknown, unresolved, invalid enums,
// sentinel durations, bad depth, and negative durations.
func checkValidate(t *testing.T) {
	t.Helper()
	assertResolvedValidates(t)
	assertBestAvailableFailsValidate(t)
	assertSentinelDurationsFailValidate(t)
	assertUnknownSystemDefaultFailValidate(t)
	assertInvalidEnumsFailValidate(t)
	assertDepthRulesValidate(t)
	assertNegativeDurationsFailValidate(t)
	assertOversizedDurationsFailValidate(t)
	assertMultiErrorValidate(t)
}

// assertResolvedValidates pins resolved default validates.
func assertResolvedValidates(t *testing.T) {
	t.Helper()
	if err := Validate(resolvedBase(t)); err != nil {
		t.Fatalf("resolved default must validate: %v", err)
	}
}

// assertBestAvailableFailsValidate pins unresolved best available fails.
func assertBestAvailableFailsValidate(t *testing.T) {
	t.Helper()
	best := BestAvailableQoS()
	if err := Validate(ResolveSystemDefaults(best)); err == nil {
		t.Fatal("unresolved best available must fail validation")
	}
}

// assertSentinelDurationsFailValidate pins sentinel durations fail.
func assertSentinelDurationsFailValidate(t *testing.T) {
	t.Helper()
	for _, tc := range sentinelDurationCases() {
		probe := resolvedBase(t)
		tc.set(&probe)
		assertSentinelFails(t, probe, tc.field)
	}
}

// sentinelDurationCase sets one sentinel duration.
type sentinelDurationCase struct {
	field string
	set   func(*QoSProfile)
}

// sentinelDurationCases returns deadline, lifespan, lease setters.
func sentinelDurationCases() []sentinelDurationCase {
	return []sentinelDurationCase{
		{"deadline", func(p *QoSProfile) { p.Deadline = BestAvailableDuration() }},
		{"lifespan", func(p *QoSProfile) { p.Lifespan = BestAvailableDuration() }},
		{"lease", func(p *QoSProfile) { p.LeaseDuration = BestAvailableDuration() }},
	}
}

// assertSentinelFails pins one sentinel profile fails naming the field.
func assertSentinelFails(t *testing.T, probe QoSProfile, field string) {
	t.Helper()
	err := Validate(probe)
	if err == nil {
		t.Fatalf("sentinel %s must fail validation", field)
	}
	if !strings.Contains(strings.ToLower(err.Error()), field) {
		t.Fatalf("sentinel %s error must name the field, got %q", field, err.Error())
	}
}

// assertUnknownSystemDefaultFailValidate pins unknown and system default fail.
func assertUnknownSystemDefaultFailValidate(t *testing.T) {
	t.Helper()
	if err := Validate(UnknownQoS()); err == nil {
		t.Fatal("unknown must fail validation")
	}
	if err := Validate(SystemDefaultQoS()); err == nil {
		t.Fatal("system default must fail validation")
	}
}

// assertInvalidEnumsFailValidate pins invalid enums fail.
func assertInvalidEnumsFailValidate(t *testing.T) {
	t.Helper()
	for _, tc := range invalidEnumCases() {
		probe := tc.mut(resolvedBase(t))
		if err := Validate(probe); err == nil {
			t.Fatalf("%s must fail validation", tc.name)
		}
	}
}

// invalidEnumCase mutates one policy to invalid.
type invalidEnumCase struct {
	name string
	mut  func(QoSProfile) QoSProfile
}

// invalidEnumCases returns history, reliability, durability, liveliness mutators.
func invalidEnumCases() []invalidEnumCase {
	return []invalidEnumCase{
		{"invalid history", func(p QoSProfile) QoSProfile { p.History = HistoryPolicy(99); return p }},
		{"invalid reliability", func(p QoSProfile) QoSProfile { p.Reliability = ReliabilityPolicy(99); return p }},
		{"invalid durability", func(p QoSProfile) QoSProfile { p.Durability = DurabilityPolicy(99); return p }},
		{"invalid liveliness", func(p QoSProfile) QoSProfile { p.Liveliness = LivelinessPolicy(99); return p }},
	}
}

// assertDepthRulesValidate pins keep last zero fails and keep all zero passes.
func assertDepthRulesValidate(t *testing.T) {
	t.Helper()
	zeroDepth := QoSProfile{History: HistoryKeepLast, Reliability: ReliabilityReliable, Durability: DurabilityVolatile, Liveliness: LivelinessAutomatic}
	if err := Validate(zeroDepth); err == nil {
		t.Fatal("keep last depth zero must fail validation")
	}
	keepAllZero := QoSProfile{History: HistoryKeepAll, Depth: 0, Reliability: ReliabilityReliable, Durability: DurabilityVolatile, Liveliness: LivelinessAutomatic}
	if err := Validate(keepAllZero); err != nil {
		t.Fatalf("keep all depth zero must validate because depth is ignored: %v", err)
	}
}

// assertNegativeDurationsFailValidate pins negative durations fail.
func assertNegativeDurationsFailValidate(t *testing.T) {
	t.Helper()
	negative := resolvedBase(t)
	negative.Deadline = Duration{Seconds: -1}
	if err := Validate(negative); err == nil {
		t.Fatal("negative deadline must fail validation")
	}
	negativeLifespan := resolvedBase(t)
	negativeLifespan.Lifespan = Duration{Seconds: -2, Nanoseconds: 100}
	if err := Validate(negativeLifespan); err == nil {
		t.Fatal("negative lifespan must fail validation")
	}
	negativeLease := resolvedBase(t)
	negativeLease.LeaseDuration = Duration{Seconds: -3}
	if err := Validate(negativeLease); err == nil {
		t.Fatal("negative lease must fail validation")
	}
}

// assertOversizedDurationsFailValidate pins nanoseconds >= 1e9 fail.
func assertOversizedDurationsFailValidate(t *testing.T) {
	t.Helper()
	oversizedDeadline := resolvedBase(t)
	oversizedDeadline.Deadline = Duration{Seconds: 1, Nanoseconds: 2000000000}
	if err := Validate(oversizedDeadline); err == nil {
		t.Fatal("deadline nanoseconds >= 1e9 must fail validation")
	}
	oversizedLifespan := resolvedBase(t)
	oversizedLifespan.Lifespan = Duration{Seconds: 1, Nanoseconds: 2000000000}
	if err := Validate(oversizedLifespan); err == nil {
		t.Fatal("lifespan nanoseconds >= 1e9 must fail validation")
	}
	oversizedLease := resolvedBase(t)
	oversizedLease.LeaseDuration = Duration{Seconds: 1, Nanoseconds: 2000000000}
	if err := Validate(oversizedLease); err == nil {
		t.Fatal("lease nanoseconds >= 1e9 must fail validation")
	}
}

// assertMultiErrorValidate pins multi problem profile names every field.
func assertMultiErrorValidate(t *testing.T) {
	t.Helper()
	multi := QoSProfile{History: HistoryUnknown, Reliability: ReliabilityPolicy(99), Durability: DurabilitySystemDefault, Liveliness: LivelinessSystemDefault, Deadline: Duration{Seconds: -1}}
	err := Validate(multi)
	if err == nil {
		t.Fatal("multi problem profile must fail validation")
	}
	assertMultiErrorNames(t, err.Error())
}

// assertMultiErrorNames pins multi error names every policy.
func assertMultiErrorNames(t *testing.T, msg string) {
	t.Helper()
	for _, want := range []string{"history", "reliability", "durability", "liveliness", "deadline"} {
		if !strings.Contains(msg, want) {
			t.Fatalf("multi error must name %s, got %q", want, msg)
		}
	}
}

// checkDurations pins the seconds plus nanoseconds time base.
func checkDurations(t *testing.T) {
	t.Helper()
	assertDurationSentinels(t)
	assertDurationFromNanos(t)
	assertDurationCarry(t)
	assertDurationStrings(t)
	assertDurationMinMax(t)
	assertDurationSaturation(t)
}

// assertDurationSentinels pins unspecified, infinite, best available totals.
func assertDurationSentinels(t *testing.T) {
	t.Helper()
	if UnspecifiedDuration() != (Duration{}) {
		t.Fatal("unspecified must be zero")
	}
	if InfiniteDuration().TotalNanoseconds() != math.MaxInt64 {
		t.Fatalf("infinite must total max int64, got %d", InfiniteDuration().TotalNanoseconds())
	}
	if BestAvailableDuration().TotalNanoseconds() != math.MaxInt64-1 {
		t.Fatalf("best available sentinel must be infinite minus one, got %d", BestAvailableDuration().TotalNanoseconds())
	}
	if !IsBestAvailableDuration(BestAvailableDuration()) {
		t.Fatal("sentinel must satisfy its own predicate")
	}
	if IsBestAvailableDuration(InfiniteDuration()) {
		t.Fatal("infinite must not satisfy the sentinel predicate")
	}
}

// assertDurationFromNanos pins DurationFromNanoseconds mapping.
func assertDurationFromNanos(t *testing.T) {
	t.Helper()
	if DurationFromNanoseconds(0) != UnspecifiedDuration() {
		t.Fatal("zero totals must normalize to unspecified")
	}
	negativeTotal := DurationFromNanoseconds(-5)
	if negativeTotal != (Duration{Seconds: -1}) {
		t.Fatalf("negative totals must be malformed, got %+v", negativeTotal)
	}
	if err := Validate(withDeadline(resolvedBase(t), negativeTotal)); err == nil {
		t.Fatal("negative total duration must fail validation")
	}
	if !strings.Contains(negativeTotal.String(), "invalid") {
		t.Fatalf("negative total string must mark invalid, got %q", negativeTotal.String())
	}
	if DurationFromNanoseconds(math.MaxInt64) != InfiniteDuration() {
		t.Fatal("max int64 input must yield infinite")
	}
}

// assertDurationCarry pins nanosecond overflow and negative carry.
func assertDurationCarry(t *testing.T) {
	t.Helper()
	carried := NewDuration(1, 2500000000)
	if carried != (Duration{Seconds: 3, Nanoseconds: 500000000}) {
		t.Fatalf("nanosecond overflow must carry: got %+v", carried)
	}
	if NewDuration(math.MaxInt64, 999999999) != InfiniteDuration() {
		t.Fatal("overflowing construction must saturate to infinite")
	}
	negativeCarry := NewDuration(-1, 2500000000)
	if negativeCarry.Seconds != -1 {
		t.Fatalf("negative seconds must not carry into positive: got %+v", negativeCarry)
	}
	if err := Validate(withDeadline(resolvedBase(t), negativeCarry)); err == nil {
		t.Fatal("no carry negative must still fail validation")
	}
}

// assertDurationStrings pins documented string forms.
func assertDurationStrings(t *testing.T) {
	t.Helper()
	if NewDuration(10, 0).String() != "10s" || UnspecifiedDuration().String() != "unspecified" || InfiniteDuration().String() != "infinite" {
		t.Fatal("duration strings must use the documented forms")
	}
	if BestAvailableDuration().String() != "best_available" {
		t.Fatalf("sentinel string must be best_available, got %q", BestAvailableDuration().String())
	}
	assertNegativeDurationString(t)
}

// assertNegativeDurationString pins negative renders invalid without dot.
func assertNegativeDurationString(t *testing.T) {
	t.Helper()
	negativeString := Duration{Seconds: -1, Nanoseconds: 500000000}.String()
	if strings.Contains(negativeString, ".") {
		t.Fatalf("negative string must avoid ambiguous decimal form, got %q", negativeString)
	}
	if !strings.Contains(negativeString, "invalid") {
		t.Fatalf("negative string must mark invalid, got %q", negativeString)
	}
}

// assertDurationMinMax pins min and max pick faster and slower.
func assertDurationMinMax(t *testing.T) {
	t.Helper()
	if MinDuration(NewDuration(1, 0), NewDuration(2, 0)) != NewDuration(1, 0) {
		t.Fatal("min must pick the faster duration")
	}
	if MaxDuration(NewDuration(1, 0), NewDuration(2, 0)) != NewDuration(2, 0) {
		t.Fatal("max must pick the slower duration")
	}
}

// assertDurationSaturation pins saturating compare and totals.
func assertDurationSaturation(t *testing.T) {
	t.Helper()
	hugeA := Duration{Seconds: maxSeconds + 100, Nanoseconds: 0}
	hugeB := Duration{Seconds: maxSeconds + 200, Nanoseconds: 0}
	if hugeA.Compare(hugeB) != 0 {
		t.Fatal("saturating compare must report equal for totals above max int64")
	}
	if !MinDuration(hugeA, hugeB).IsInfinite() {
		t.Fatal("saturating min must report infinite")
	}
	if !MaxDuration(hugeA, hugeB).IsInfinite() {
		t.Fatal("saturating max must report infinite")
	}
	negHuge := Duration{Seconds: math.MinInt64}
	if got := negHuge.TotalNanoseconds(); got != math.MinInt64 {
		t.Fatalf("huge negative total must saturate to min int64, got %d", got)
	}
}

// checkReasonTruncation pins the 2048 byte reason buffer port on all three texts.
func checkReasonTruncation(t *testing.T) {
	t.Helper()
	report := CompatibilityReport{
		Compatible: false,
		Errors:     []CompatibilityIssue{{Kind: PolicyReliability, Detail: strings.Repeat("x", 3000)}},
	}
	if got := len(report.Reason()); got != MaxReasonLength {
		t.Fatalf("reason must truncate to %d, got %d", MaxReasonLength, got)
	}
	if got := len(report.ErrorText()); got != MaxReasonLength {
		t.Fatalf("error text must truncate to %d, got %d", MaxReasonLength, got)
	}
	warnReport := CompatibilityReport{
		Compatible: true,
		Warnings:   []CompatibilityIssue{{Kind: PolicyReliability, Detail: strings.Repeat("y", 3000)}},
	}
	if got := len(warnReport.Reason()); got != MaxReasonLength {
		t.Fatalf("warning reason must truncate to %d, got %d", MaxReasonLength, got)
	}
	if got := len(warnReport.WarningText()); got != MaxReasonLength {
		t.Fatalf("warning text must truncate to %d, got %d", MaxReasonLength, got)
	}
}

// checkInfiniteRequestedFlows asserts a requested infinite means no constraint
// while an offered infinite stays slow.
func checkInfiniteRequestedFlows(t *testing.T) {
	t.Helper()
	base := resolvedBase(t)
	finiteDeadline := NewDuration(0, 100000000)
	finiteLease := NewDuration(4, 0)
	flows := []struct {
		name      string
		offered   QoSProfile
		requested QoSProfile
	}{
		{"finite offered infinite requested deadline", withDeadline(base, finiteDeadline), withDeadline(base, InfiniteDuration())},
		{"infinite offered infinite requested deadline", withDeadline(base, InfiniteDuration()), withDeadline(base, InfiniteDuration())},
		{"unspecified offered infinite requested deadline", withDeadline(base, UnspecifiedDuration()), withDeadline(base, InfiniteDuration())},
		{"finite offered unspecified requested deadline", withDeadline(base, finiteDeadline), withDeadline(base, UnspecifiedDuration())},
		{"finite offered infinite requested lease", withLease(base, finiteLease), withLease(base, InfiniteDuration())},
		{"infinite offered infinite requested lease", withLease(base, InfiniteDuration()), withLease(base, InfiniteDuration())},
		{"unspecified offered infinite requested lease", withLease(base, UnspecifiedDuration()), withLease(base, InfiniteDuration())},
		{"unspecified offered unspecified requested lease", withLease(base, UnspecifiedDuration()), withLease(base, UnspecifiedDuration())},
	}
	for _, tc := range flows {
		report := CheckCompatibility(tc.offered, tc.requested)
		if !report.Compatible {
			t.Fatalf("%s: expected compatible, got %q", tc.name, report.Reason())
		}
		if len(report.Errors) != 0 {
			t.Fatalf("%s: expected no errors, got %+v", tc.name, report.Errors)
		}
	}
	offeredSlowDeadline := withDeadline(base, InfiniteDuration())
	requestedFastDeadline := withDeadline(base, finiteDeadline)
	report := CheckCompatibility(offeredSlowDeadline, requestedFastDeadline)
	if report.Compatible {
		t.Fatal("infinite offered deadline must stay slow against finite request")
	}
	if len(report.Errors) != 1 || report.Errors[0].Kind != PolicyDeadline {
		t.Fatalf("slow offered deadline must error on deadline, got %+v", report.Errors)
	}
	offeredSlowLease := withLease(base, InfiniteDuration())
	requestedFastLease := withLease(base, finiteLease)
	report = CheckCompatibility(offeredSlowLease, requestedFastLease)
	if report.Compatible {
		t.Fatal("infinite offered lease must not satisfy finite request")
	}
	if len(report.Errors) != 1 || report.Errors[0].Kind != PolicyLeaseDuration {
		t.Fatalf("slow offered lease must error on lease, got %+v", report.Errors)
	}
}

// checkMultiErrorOrdering asserts every incompatible policy errors once in
// reliability, durability, deadline, liveliness, lease order.
func checkMultiErrorOrdering(t *testing.T) {
	t.Helper()
	base := resolvedBase(t)
	offered := base
	offered.Reliability = ReliabilityBestEffort
	offered.Durability = DurabilityVolatile
	offered.Deadline = InfiniteDuration()
	offered.Liveliness = LivelinessAutomatic
	offered.LeaseDuration = InfiniteDuration()
	requested := base
	requested.Reliability = ReliabilityReliable
	requested.Durability = DurabilityTransientLocal
	requested.Deadline = NewDuration(0, 10000000)
	requested.Liveliness = LivelinessManualByTopic
	requested.LeaseDuration = NewDuration(4, 0)
	report := CheckCompatibility(offered, requested)
	if report.Compatible {
		t.Fatal("all bad pair must be incompatible")
	}
	want := []PolicyKind{PolicyReliability, PolicyDurability, PolicyDeadline, PolicyLiveliness, PolicyLeaseDuration}
	if len(report.Errors) != len(want) {
		t.Fatalf("expected %d errors, got %+v", len(want), report.Errors)
	}
	for i, kind := range want {
		if report.Errors[i].Kind != kind {
			t.Fatalf("error %d got %s want %s: %+v", i, report.Errors[i].Kind, kind, report.Errors)
		}
	}
	if len(report.Warnings) != 0 {
		t.Fatalf("warnings must be empty when errors exist, got %+v", report.Warnings)
	}
	reason := report.Reason()
	last := -1
	for _, kind := range want {
		at := strings.Index(reason, string(kind))
		if at < 0 {
			t.Fatalf("reason %q misses kind %s", reason, kind)
		}
		if at < last {
			t.Fatalf("reason %q out of order at kind %s", reason, kind)
		}
		last = at
	}
}

// checkBestAvailableWarnings asserts sentinel durations warn rather than
// error in both directions against a resolved profile.
func checkBestAvailableWarnings(t *testing.T) {
	t.Helper()
	strict := resolvedBase(t)
	best := BestAvailableQoS()
	for _, pair := range []struct {
		name      string
		offered   QoSProfile
		requested QoSProfile
	}{
		{"best offered strict requested", best, strict},
		{"strict offered best requested", strict, best},
	} {
		report := CheckCompatibility(pair.offered, pair.requested)
		if !report.Compatible {
			t.Fatalf("%s must stay compatible, got %q", pair.name, report.Reason())
		}
		if len(report.Errors) != 0 {
			t.Fatalf("%s must not error, got %+v", pair.name, report.Errors)
		}
		if len(report.Warnings) != 5 {
			t.Fatalf("%s must warn five times, got %+v", pair.name, report.Warnings)
		}
		found := map[PolicyKind]bool{}
		for _, w := range report.Warnings {
			found[w.Kind] = true
		}
		for _, kind := range []PolicyKind{PolicyReliability, PolicyDurability, PolicyLiveliness, PolicyDeadline, PolicyLeaseDuration} {
			if !found[kind] {
				t.Fatalf("%s must warn on %s, got %+v", pair.name, kind, report.Warnings)
			}
		}
	}
}

// withReliability returns p with reliability replaced.
func withReliability(p QoSProfile, r ReliabilityPolicy) QoSProfile {
	p.Reliability = r
	return p
}

// withDurability returns p with durability replaced.
func withDurability(p QoSProfile, d DurabilityPolicy) QoSProfile {
	p.Durability = d
	return p
}

// withDeadline returns p with deadline replaced.
func withDeadline(p QoSProfile, d Duration) QoSProfile {
	p.Deadline = d
	return p
}

// withLease returns p with lease duration replaced.
func withLease(p QoSProfile, d Duration) QoSProfile {
	p.LeaseDuration = d
	return p
}

// withLiveliness returns p with liveliness replaced.
func withLiveliness(p QoSProfile, l LivelinessPolicy) QoSProfile {
	p.Liveliness = l
	return p
}
