// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

package dds

import (
	"encoding/hex"
	"errors"
	"strings"
	"testing"

	"github.com/podomy/weld/internal/core"
	"github.com/podomy/weld/internal/transports/dds/rtps"
)

// writerQoSTestTopic plus peers scope every golden and refusal.
const (
	writerQoSTestTopic      = "/chatter"
	writerQoSTestPublisher  = "weld-pub"
	writerQoSTestSubscriber = "ros-echo"
)

// writerQoSPresetCase pairs a name with a wire-ready profile.
type writerQoSPresetCase struct {
	// name identifies the preset in failures.
	name string
	// profile is the resolved input to ForwardQoS.
	profile core.QoSProfile
}

// writerQoSPresets returns the full named preset matrix.
//
// System default and best available enter in resolved form: the raw
// markers refuse loudly, so the test resolves them exactly as endpoint
// creation would before translation.
func writerQoSPresets(t *testing.T) []writerQoSPresetCase {
	t.Helper()

	sysResolved := core.ResolveSystemDefaults(core.SystemDefaultQoS())

	baPub, err := core.CreateEndpoint(writerQoSTestTopic, writerQoSTestPublisher, core.EndpointPublisher, core.BestAvailableQoS(), nil, false)
	if err != nil {
		t.Fatalf("resolve best available publisher: %v", err)
	}

	return []writerQoSPresetCase{
		{name: "sensor-data", profile: core.SensorDataQoS()},
		{name: "parameters", profile: core.ParametersQoS()},
		{name: "default", profile: core.DefaultQoS()},
		{name: "services-default", profile: core.ServicesDefaultQoS()},
		{name: "parameter-events", profile: core.ParameterEventsQoS()},
		{name: "rosout", profile: core.RosoutQoS()},
		{name: "system-default-resolved", profile: sysResolved},
		{name: "best-available-resolved", profile: baPub.ActualQoS()},
	}
}

// writerQoSExplicitKeepAll exercises keep all plus manual liveliness plus finite durations.
func writerQoSExplicitKeepAll() core.QoSProfile {
	return core.QoSProfile{
		History:       core.HistoryKeepAll,
		Depth:         7,
		Reliability:   core.ReliabilityBestEffort,
		Durability:    core.DurabilityVolatile,
		Deadline:      core.NewDuration(2, 0),
		Lifespan:      core.NewDuration(0, 500000000),
		Liveliness:    core.LivelinessManualByTopic,
		LeaseDuration: core.NewDuration(5, 0),
	}
}

// writerQoSExplicitFinite exercises keep last plus manual liveliness plus finite durations.
func writerQoSExplicitFinite() core.QoSProfile {
	return core.QoSProfile{
		History:       core.HistoryKeepLast,
		Depth:         42,
		Reliability:   core.ReliabilityReliable,
		Durability:    core.DurabilityTransientLocal,
		Deadline:      core.NewDuration(1, 250000000),
		Lifespan:      core.NewDuration(0, 1),
		Liveliness:    core.LivelinessManualByTopic,
		LeaseDuration: core.NewDuration(9, 750000000),
	}
}

// writerQoSGolden holds the exact expected PID value bytes per preset.
type writerQoSGolden struct {
	// reliability is the exact PID 0x001a value bytes.
	reliability string
	// history is the exact PID 0x0040 value bytes.
	history string
	// durability is the exact PID 0x001d value bytes.
	durability string
	// deadline is the exact PID 0x0023 value bytes.
	deadline string
	// lifespan is the exact PID 0x002b value bytes.
	lifespan string
	// liveliness is the exact PID 0x001b value bytes.
	liveliness string
	// presentation is the exact PID 0x0021 value bytes.
	presentation string
	// latency is the exact PID 0x0027 value bytes.
	latency string
}

// writerQoSGoldens pins every named preset plus the explicit keep all profile.
func writerQoSGoldens() map[string]writerQoSGolden {
	const (
		relReliable   = "02000000ffffff7fffffffff"
		relBestEffort = "010000000000000000000000"
		histKL5       = "0000000005000000"
		histKL10      = "000000000a000000"
		histKL1000    = "00000000e8030000"
		histKeepAll   = "0100000000000000"
		durVolatile   = "00000000"
		durTL         = "01000000"
		infDur        = "ffffff7fffffffff"
		life10s       = "0a00000000000000"
		liveAutoInf   = "00000000ffffff7fffffffff"
	)

	return map[string]writerQoSGolden{
		"sensor-data": {
			reliability: relBestEffort, history: histKL5, durability: durVolatile,
			deadline: infDur, lifespan: infDur, liveliness: liveAutoInf,
			presentation: "000000000000000000000000", latency: "0000000000000000",
		},
		"parameters": {
			reliability: relReliable, history: histKL1000, durability: durVolatile,
			deadline: infDur, lifespan: infDur, liveliness: liveAutoInf,
			presentation: "000000000000000000000000", latency: "0000000000000000",
		},
		"default": {
			reliability: relReliable, history: histKL10, durability: durVolatile,
			deadline: infDur, lifespan: infDur, liveliness: liveAutoInf,
			presentation: "000000000000000000000000", latency: "0000000000000000",
		},
		"services-default": {
			reliability: relReliable, history: histKL10, durability: durVolatile,
			deadline: infDur, lifespan: infDur, liveliness: liveAutoInf,
			presentation: "000000000000000000000000", latency: "0000000000000000",
		},
		"parameter-events": {
			reliability: relReliable, history: histKL1000, durability: durVolatile,
			deadline: infDur, lifespan: infDur, liveliness: liveAutoInf,
			presentation: "000000000000000000000000", latency: "0000000000000000",
		},
		"rosout": {
			reliability: relReliable, history: histKL1000, durability: durTL,
			deadline: infDur, lifespan: life10s, liveliness: liveAutoInf,
			presentation: "000000000000000000000000", latency: "0000000000000000",
		},
		"system-default-resolved": {
			reliability: relReliable, history: histKL10, durability: durVolatile,
			deadline: infDur, lifespan: infDur, liveliness: liveAutoInf,
			presentation: "000000000000000000000000", latency: "0000000000000000",
		},
		"best-available-resolved": {
			reliability: relReliable, history: histKL10, durability: durTL,
			deadline: infDur, lifespan: infDur, liveliness: liveAutoInf,
			presentation: "000000000000000000000000", latency: "0000000000000000",
		},
		"explicit-keep-all": {
			reliability: relBestEffort, history: histKeepAll, durability: durVolatile,
			deadline:     "0200000000000000",
			lifespan:     "000000000065cd1d",
			liveliness:   "020000000500000000000000",
			presentation: "000000000000000000000000", latency: "0000000000000000",
		},
	}
}

// writerQoSParamHex returns the hex of one PID value from encoded params.
func writerQoSParamHex(t *testing.T, wire []byte, id uint16) string {
	t.Helper()

	params, err := rtps.DecodeParams(wire)
	if err != nil {
		t.Fatalf("decode params: %v", err)
	}

	for _, param := range params {
		if param.ID == id {
			return hex.EncodeToString(param.Value)
		}
	}

	t.Fatalf("param %#04x missing", id)

	return ""
}

// checkForwardQoSGoldenWire pins the exact PID bytes for one encoded preset.
func checkForwardQoSGoldenWire(t *testing.T, wire []byte, golden writerQoSGolden) {
	t.Helper()
	if got := writerQoSParamHex(t, wire, rtps.PIDReliability); got != golden.reliability {
		t.Fatalf("reliability bytes = %s, want %s", got, golden.reliability)
	}

	if got := writerQoSParamHex(t, wire, rtps.PIDHistory); got != golden.history {
		t.Fatalf("history bytes = %s, want %s", got, golden.history)
	}

	if got := writerQoSParamHex(t, wire, rtps.PIDDurability); got != golden.durability {
		t.Fatalf("durability bytes = %s, want %s", got, golden.durability)
	}

	if got := writerQoSParamHex(t, wire, writerQoSPIDDeadline); got != golden.deadline {
		t.Fatalf("deadline bytes = %s, want %s", got, golden.deadline)
	}

	if got := writerQoSParamHex(t, wire, writerQoSPIDLifespan); got != golden.lifespan {
		t.Fatalf("lifespan bytes = %s, want %s", got, golden.lifespan)
	}

	if got := writerQoSParamHex(t, wire, writerQoSPIDLiveliness); got != golden.liveliness {
		t.Fatalf("liveliness bytes = %s, want %s", got, golden.liveliness)
	}

	if got := writerQoSParamHex(t, wire, writerQoSPIDPresentation); got != golden.presentation {
		t.Fatalf("presentation bytes = %s, want %s", got, golden.presentation)
	}

	if got := writerQoSParamHex(t, wire, writerQoSPIDLatencyBudget); got != golden.latency {
		t.Fatalf("latency bytes = %s, want %s", got, golden.latency)
	}
}

// runForwardQoSGoldenCase runs one preset golden case by name.
func runForwardQoSGoldenCase(t *testing.T, c writerQoSPresetCase, goldens map[string]writerQoSGolden) {
	t.Helper()
	q, err := ForwardQoS(writerQoSTestTopic, writerQoSTestPublisher, writerQoSTestSubscriber, c.profile)
	if err != nil {
		t.Fatalf("ForwardQoS %s: %v", c.name, err)
	}

	wire, err := rtps.EncodeParams(q.Params())
	if err != nil {
		t.Fatalf("encode %s: %v", c.name, err)
	}

	golden, ok := goldens[c.name]
	if c.name == "explicit-finite" {
		if !ok {
			checkExplicitFiniteBytes(t, wire)
		}

		return
	}

	if !ok {
		t.Fatalf("no golden for %s", c.name)
	}

	checkForwardQoSGoldenWire(t, wire, golden)
}

// TestForwardQoSPresetGoldens pins the exact PID bytes per preset.
func TestForwardQoSPresetGoldens(t *testing.T) {
	cases := writerQoSPresets(t)
	cases = append(cases,
		writerQoSPresetCase{name: "explicit-keep-all", profile: writerQoSExplicitKeepAll()},
		writerQoSPresetCase{name: "explicit-finite", profile: writerQoSExplicitFinite()},
	)

	goldens := writerQoSGoldens()

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			runForwardQoSGoldenCase(t, c, goldens)
		})
	}
}

// checkExplicitFiniteBytes pins the finite duration PID bytes by hand.
func checkExplicitFiniteBytes(t *testing.T, wire []byte) {
	t.Helper()

	// Deadline 1.25s is 0x00000001 seconds plus 250000000 (0x0EE6B280) nanos.
	if got := writerQoSParamHex(t, wire, writerQoSPIDDeadline); got != "0100000080b2e60e" {
		t.Fatalf("deadline bytes = %s, want 0100000080b2e60e", got)
	}

	// Lifespan 1ns.
	if got := writerQoSParamHex(t, wire, writerQoSPIDLifespan); got != "0000000001000000" {
		t.Fatalf("lifespan bytes = %s, want 0000000001000000", got)
	}

	// Manual by topic plus 9.75s lease: 9 seconds plus 750000000 (0x2CB41780) nanos.
	if got := writerQoSParamHex(t, wire, writerQoSPIDLiveliness); got != "02000000090000008017b42c" {
		t.Fatalf("liveliness bytes = %s, want 02000000090000008017b42c", got)
	}

	if got := writerQoSParamHex(t, wire, rtps.PIDHistory); got != "000000002a000000" {
		t.Fatalf("history bytes = %s, want 000000002a000000", got)
	}
}

// TestForwardQoSFramingGolden pins the full encoded param list for default.
func TestForwardQoSFramingGolden(t *testing.T) {
	q, err := ForwardQoS(writerQoSTestTopic, writerQoSTestPublisher, writerQoSTestSubscriber, core.DefaultQoS())
	if err != nil {
		t.Fatalf("ForwardQoS default: %v", err)
	}

	encoded, err := rtps.EncodeParams(q.Params())
	if err != nil {
		t.Fatalf("encode framing golden: %v", err)
	}
	got := hex.EncodeToString(encoded)
	want := "1a000c00" + "02000000ffffff7fffffffff" +
		"40000800" + "000000000a000000" +
		"1d000400" + "00000000" +
		"23000800" + "ffffff7fffffffff" +
		"2b000800" + "ffffff7fffffffff" +
		"1b000c00" + "00000000ffffff7fffffffff" +
		"21000c00" + "000000000000000000000000" +
		"27000800" + "0000000000000000" +
		"11000400" + "00000000" +
		"01000000"

	if got != want {
		t.Fatalf("framing golden mismatch:\n got %s\nwant %s", got, want)
	}
}

// writerQoSRefusalCase pairs an untranslatable profile with its policy.
type writerQoSRefusalCase struct {
	// name identifies the case in failures.
	name string
	// profile is the refused input.
	profile core.QoSProfile
	// policy is the expected refusing policy kind.
	policy WriterPolicyKind
}

// writerQoSRefusalCases covers every loud refusal path.
func writerQoSRefusalCases() []writerQoSRefusalCase {
	base := core.QoSProfile{
		History:       core.HistoryKeepLast,
		Depth:         10,
		Reliability:   core.ReliabilityReliable,
		Durability:    core.DurabilityVolatile,
		Liveliness:    core.LivelinessAutomatic,
		LeaseDuration: core.InfiniteDuration(),
	}

	cases := []writerQoSRefusalCase{
		{name: "system-default-reliability", policy: core.PolicyReliability},
		{name: "unknown-reliability", policy: core.PolicyReliability},
		{name: "best-available-durability", policy: core.PolicyDurability},
		{name: "unknown-durability", policy: core.PolicyDurability},
		{name: "unknown-history", policy: WriterPolicyHistory},
		{name: "keep-last-depth-zero", policy: WriterPolicyHistory},
		{name: "keep-last-depth-huge", policy: WriterPolicyHistory},
		{name: "system-default-liveliness", policy: core.PolicyLiveliness},
		{name: "unknown-liveliness", policy: core.PolicyLiveliness},
		{name: "negative-deadline", policy: core.PolicyDeadline},
		{name: "sentinel-deadline", policy: core.PolicyDeadline},
		{name: "deadline-above-wire-range", policy: core.PolicyDeadline},
		{name: "negative-lifespan", policy: WriterPolicyLifespan},
		{name: "lease-nanos-overflow", policy: core.PolicyLeaseDuration},
		{name: "sentinel-lease", policy: core.PolicyLeaseDuration},
	}

	profiles := []core.QoSProfile{
		func() core.QoSProfile { p := base; p.Reliability = core.ReliabilitySystemDefault; return p }(),
		func() core.QoSProfile { p := base; p.Reliability = core.ReliabilityUnknown; return p }(),
		func() core.QoSProfile { p := base; p.Durability = core.DurabilityBestAvailable; return p }(),
		func() core.QoSProfile { p := base; p.Durability = core.DurabilityUnknown; return p }(),
		func() core.QoSProfile { p := base; p.History = core.HistoryUnknown; return p }(),
		func() core.QoSProfile { p := base; p.Depth = 0; return p }(),
		func() core.QoSProfile { p := base; p.Depth = 1 << 33; return p }(),
		func() core.QoSProfile { p := base; p.Liveliness = core.LivelinessSystemDefault; return p }(),
		func() core.QoSProfile { p := base; p.Liveliness = core.LivelinessUnknown; return p }(),
		func() core.QoSProfile { p := base; p.Deadline = core.Duration{Seconds: -1}; return p }(),
		func() core.QoSProfile { p := base; p.Deadline = core.BestAvailableDuration(); return p }(),
		func() core.QoSProfile {
			p := base
			p.Deadline = core.Duration{Seconds: int64(writerQoSMaxFiniteSeconds) + 1}
			return p
		}(),
		func() core.QoSProfile { p := base; p.Lifespan = core.Duration{Seconds: -2}; return p }(),
		func() core.QoSProfile {
			p := base
			p.LeaseDuration = core.Duration{Seconds: 1, Nanoseconds: 1000000000}
			return p
		}(),
		func() core.QoSProfile { p := base; p.LeaseDuration = core.BestAvailableDuration(); return p }(),
	}

	for i := range cases {
		cases[i].profile = profiles[i]
	}

	return cases
}

// TestForwardQoSLoudRefusals proves every refusal names topic, pair, and policy.
//
//nolint:gocognit // test pins every refusal policy in one run, splitting would obscure intent
func TestForwardQoSLoudRefusals(t *testing.T) {
	for _, c := range writerQoSRefusalCases() {
		t.Run(c.name, func(t *testing.T) {
			_, err := ForwardQoS(writerQoSTestTopic, writerQoSTestPublisher, writerQoSTestSubscriber, c.profile)
			if err == nil {
				t.Fatalf("ForwardQoS %s succeeded, want loud refusal with policy %s", c.name, string(c.policy))
			}

			if !IsWireQoSRefusal(err) {
				t.Fatalf("ForwardQoS %s error type %T, want *WireQoSRefusal: %v", c.name, err, err)
			}

			var refusal *WireQoSRefusal
			if !errors.As(err, &refusal) {
				t.Fatalf("ForwardQoS %s refusal unwrap failed: %v", c.name, err)
			}

			if refusal.Topic != writerQoSTestTopic {
				t.Fatalf("refusal topic = %q, want %q", refusal.Topic, writerQoSTestTopic)
			}

			if refusal.Publisher != writerQoSTestPublisher {
				t.Fatalf("refusal publisher = %q, want %q", refusal.Publisher, writerQoSTestPublisher)
			}

			if refusal.Subscriber != writerQoSTestSubscriber {
				t.Fatalf("refusal subscriber = %q, want %q", refusal.Subscriber, writerQoSTestSubscriber)
			}

			if refusal.Policy != c.policy {
				t.Fatalf("refusal policy = %s, want %s", string(refusal.Policy), string(c.policy))
			}

			msg := err.Error()
			for _, want := range []string{writerQoSTestTopic, writerQoSTestPublisher, writerQoSTestSubscriber, string(c.policy)} {
				if !strings.Contains(msg, want) {
					t.Fatalf("refusal message misses %q: %s", want, msg)
				}
			}
		})
	}
}

// TestReverseQoSRoundTrip proves forward, parse, and reverse compose per preset.
//
//nolint:gocognit // test pins forward parse reverse round trip in one run, splitting would obscure intent
func TestReverseQoSRoundTrip(t *testing.T) {
	cases := writerQoSPresets(t)
	cases = append(cases,
		writerQoSPresetCase{name: "explicit-keep-all", profile: writerQoSExplicitKeepAll()},
		writerQoSPresetCase{name: "explicit-finite", profile: writerQoSExplicitFinite()},
	)

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			q, err := ForwardQoS(writerQoSTestTopic, writerQoSTestPublisher, writerQoSTestSubscriber, c.profile)
			if err != nil {
				t.Fatalf("ForwardQoS %s: %v", c.name, err)
			}

			parsed, err := ParseWireQoS(q.Params())
			if err != nil {
				t.Fatalf("ParseWireQoS %s: %v", c.name, err)
			}

			if parsed != q {
				t.Fatalf("parse round trip changed wire QoS:\n got %+v\nwant %+v", parsed, q)
			}

			back, err := ReverseQoS(parsed)
			if err != nil {
				t.Fatalf("ReverseQoS %s: %v", c.name, err)
			}

			want := normalizeForWire(c.profile)
			if back != want {
				t.Fatalf("reverse round trip mismatch:\n got %+v\nwant %+v", back, want)
			}
		})
	}
}

// normalizeForWire maps unspecified durations to infinite, matching the
// forward rule, and drops keep all depth, which the wire ignores.
func normalizeForWire(p core.QoSProfile) core.QoSProfile {
	out := p

	if out.Deadline.IsUnspecified() {
		out.Deadline = core.InfiniteDuration()
	}

	if out.Lifespan.IsUnspecified() {
		out.Lifespan = core.InfiniteDuration()
	}

	if out.LeaseDuration.IsUnspecified() {
		out.LeaseDuration = core.InfiniteDuration()
	}

	if out.History == core.HistoryKeepAll {
		out.Depth = 0
	}

	return out
}

// TestReverseQoSUnknownKinds proves unknown wire kinds refuse loudly, never silently.
//
// Transient, persistent, and manual-by-participant now map directly per DDS
// 1.4 sections 2.2.3.4 and 2.2.3.11; only truly unknown kinds refuse.
//
//nolint:gocognit // test pins unknown wire kind mapping in one run, splitting would obscure intent
func TestReverseQoSUnknownKinds(t *testing.T) {
	base, err := ForwardQoS(writerQoSTestTopic, writerQoSTestPublisher, writerQoSTestSubscriber, core.DefaultQoS())
	if err != nil {
		t.Fatalf("ForwardQoS default: %v", err)
	}

	t.Run("transient-durability-maps", func(t *testing.T) {
		q := base
		q.DurabilityKind = writerQoSDurabilityTransient

		got, err := ReverseQoS(q)
		if err != nil {
			t.Fatalf("ReverseQoS transient: %v", err)
		}
		if got.Durability != core.Durabilityqos4Transient {
			t.Fatalf("transient durability = %v, want transient", got.Durability)
		}
	})

	t.Run("persistent-durability-maps", func(t *testing.T) {
		q := base
		q.DurabilityKind = writerQoSDurabilityPersistent

		got, err := ReverseQoS(q)
		if err != nil {
			t.Fatalf("ReverseQoS persistent: %v", err)
		}
		if got.Durability != core.Durabilityqos4Persistent {
			t.Fatalf("persistent durability = %v, want persistent", got.Durability)
		}
	})

	t.Run("manual-by-participant-maps", func(t *testing.T) {
		q := base
		q.LivelinessKind = writerQoSLivelinessManualByParticipant

		got, err := ReverseQoS(q)
		if err != nil {
			t.Fatalf("ReverseQoS manual by participant: %v", err)
		}
		if got.Liveliness != core.Livelinessqos4ManualByParticipant {
			t.Fatalf("manual by participant liveliness = %v, want manual_by_participant", got.Liveliness)
		}
	})

	unknownKinds := []struct {
		name   string
		mutate func(*WireQoS)
		policy WriterPolicyKind
	}{
		{name: "reliability", mutate: func(q *WireQoS) { q.ReliabilityKind = 9 }, policy: core.PolicyReliability},
		{name: "durability", mutate: func(q *WireQoS) { q.DurabilityKind = 9 }, policy: core.PolicyDurability},
		{name: "history", mutate: func(q *WireQoS) { q.HistoryKind = 9 }, policy: WriterPolicyHistory},
		{name: "liveliness", mutate: func(q *WireQoS) { q.LivelinessKind = 9 }, policy: core.PolicyLiveliness},
		{name: "presentation-scope", mutate: func(q *WireQoS) { q.PresentationScope = 9 }, policy: core.PolicyPresentation},
		{name: "presentation-flags", mutate: func(q *WireQoS) { q.PresentationCoherent = 2 }, policy: core.PolicyPresentation},
		{name: "keep-last-depth-zero", mutate: func(q *WireQoS) { q.Depth = 0 }, policy: WriterPolicyHistory},
	}

	for _, c := range unknownKinds {
		t.Run("unknown-"+c.name, func(t *testing.T) {
			q := base
			c.mutate(&q)

			_, err := ReverseQoS(q)
			if err == nil {
				t.Fatalf("ReverseQoS unknown %s succeeded, want loud error", c.name)
			}

			if !IsWireQoSReverseRefusal(err) {
				t.Fatalf("ReverseQoS unknown %s error type %T, want *WireQoSReverseRefusal: %v", c.name, err, err)
			}

			if !strings.Contains(err.Error(), string(c.policy)) {
				t.Fatalf("reverse error misses policy %s: %v", string(c.policy), err)
			}
		})
	}
}

// TestWriterQoSParseRules pins required, defaulted, and tolerated params.
//
//nolint:gocognit,cyclop // test pins parse required default and tolerated rules in one run, splitting would obscure intent
func TestWriterQoSParseRules(t *testing.T) {
	base, err := ForwardQoS(writerQoSTestTopic, writerQoSTestPublisher, writerQoSTestSubscriber, core.DefaultQoS())
	if err != nil {
		t.Fatalf("ForwardQoS default: %v", err)
	}

	t.Run("missing-reliability-refuses", func(t *testing.T) {
		params := dropParam(base.Params(), rtps.PIDReliability)

		_, err := ParseWireQoS(params)
		if err == nil {
			t.Fatal("ParseWireQoS without reliability succeeded, want loud error")
		}

		if !IsWireQoSParseRefusal(err) {
			t.Fatalf("parse error type %T, want *WireQoSParseRefusal: %v", err, err)
		}

		if !strings.Contains(err.Error(), string(core.PolicyReliability)) {
			t.Fatalf("parse error misses reliability policy: %v", err)
		}
	})

	t.Run("missing-durability-refuses", func(t *testing.T) {
		params := dropParam(base.Params(), rtps.PIDDurability)

		_, err := ParseWireQoS(params)
		if err == nil {
			t.Fatal("ParseWireQoS without durability succeeded, want loud error")
		}

		if !IsWireQoSParseRefusal(err) {
			t.Fatalf("parse error type %T, want *WireQoSParseRefusal: %v", err, err)
		}

		if !strings.Contains(err.Error(), string(core.PolicyDurability)) {
			t.Fatalf("parse error misses durability policy: %v", err)
		}
	})

	t.Run("missing-history-refuses", func(t *testing.T) {
		params := dropParam(base.Params(), rtps.PIDHistory)

		_, err := ParseWireQoS(params)
		if err == nil {
			t.Fatal("ParseWireQoS without history succeeded, want loud error")
		}

		if !IsWireQoSParseRefusal(err) {
			t.Fatalf("parse error type %T, want *WireQoSParseRefusal: %v", err, err)
		}

		if !strings.Contains(err.Error(), string(WriterPolicyHistory)) {
			t.Fatalf("parse error misses history policy: %v", err)
		}
	})

	t.Run("missing-optional-defaults-infinite", func(t *testing.T) {
		params := dropParam(base.Params(), writerQoSPIDDeadline)
		params = dropParam(params, writerQoSPIDLifespan)
		params = dropParam(params, writerQoSPIDLiveliness)

		parsed, err := ParseWireQoS(params)
		if err != nil {
			t.Fatalf("ParseWireQoS without optional PIDs: %v", err)
		}

		if !parsed.Deadline.IsInfinite() || !parsed.Lifespan.IsInfinite() || !parsed.LeaseDuration.IsInfinite() {
			t.Fatalf("optional defaults not infinite: %+v", parsed)
		}
	})

	t.Run("vendor-params-tolerated", func(t *testing.T) {
		params := append(base.Params(), rtps.Param{ID: 0x8003, Value: []byte{0x01, 0x02, 0x03, 0x04}})

		parsed, err := ParseWireQoS(params)
		if err != nil {
			t.Fatalf("ParseWireQoS with vendor PID: %v", err)
		}

		if parsed != base {
			t.Fatalf("vendor PID changed parse:\n got %+v\nwant %+v", parsed, base)
		}
	})

	t.Run("first-wins-on-duplicates", func(t *testing.T) {
		params := base.Params()
		dup := rtps.Param{ID: rtps.PIDDurability, Value: []byte{0x00, 0x00, 0x00, 0x00}}
		params = append([]rtps.Param{dup}, params...)

		parsed, err := ParseWireQoS(params)
		if err != nil {
			t.Fatalf("ParseWireQoS with duplicate: %v", err)
		}

		if parsed.DurabilityKind != sedpDurabilityVolatile {
			t.Fatalf("duplicate durability = %d, want first-wins volatile", parsed.DurabilityKind)
		}
	})

	t.Run("short-reliability-refuses", func(t *testing.T) {
		params := base.Params()
		for i := range params {
			if params[i].ID == rtps.PIDReliability {
				params[i].Value = params[i].Value[:8]
			}
		}

		_, err := ParseWireQoS(params)
		if err == nil {
			t.Fatal("ParseWireQoS with short reliability succeeded, want loud error")
		}
	})
}

// dropParam removes one PID from a param list.
func dropParam(params []rtps.Param, id uint16) []rtps.Param {
	out := make([]rtps.Param, 0, len(params))

	for _, param := range params {
		if param.ID == id {
			continue
		}

		out = append(out, param)
	}

	return out
}

// TestWriterQoSEquivalenceMatrix proves translated offers match like facade offers.
//
//nolint:gocognit // test pins translated offer equivalence matrix in one run, splitting would obscure intent
func TestWriterQoSEquivalenceMatrix(t *testing.T) {
	profiles := []core.QoSProfile{normalizeForWire(core.FacadeAdvertisedQoS())}
	for _, c := range writerQoSPresets(t) {
		profiles = append(profiles, normalizeForWire(c.profile))
	}

	profiles = append(profiles, writerQoSExplicitKeepAll(), writerQoSExplicitFinite())

	names := []string{"facade", "sensor-data", "parameters", "default", "services-default", "parameter-events", "rosout", "system-default-resolved", "best-available-resolved", "explicit-keep-all", "explicit-finite"}

	if len(profiles) != len(names) {
		t.Fatalf("profile count %d != name count %d", len(profiles), len(names))
	}

	for i, offered := range profiles {
		wireOffered, err := ForwardQoS(writerQoSTestTopic, writerQoSTestPublisher, writerQoSTestSubscriber, offered)
		if err != nil {
			t.Fatalf("ForwardQoS offered %s: %v", names[i], err)
		}

		for j, requested := range profiles {
			wireRequested, err := ForwardQoS(writerQoSTestTopic, writerQoSTestPublisher, writerQoSTestSubscriber, requested)
			if err != nil {
				t.Fatalf("ForwardQoS requested %s: %v", names[j], err)
			}

			want := core.CheckCompatibility(offered, requested)
			got := CheckWireCompatibility(wireOffered, wireRequested)

			if got.Compatible != want.Compatible {
				t.Fatalf("pair offered=%s requested=%s: wire compatible=%v, want core compatible=%v\nwire errors: %s\ncore errors: %s",
					names[i], names[j], got.Compatible, want.Compatible, got.ErrorText(), want.ErrorText())
			}

			if got.ErrorText() != want.ErrorText() {
				t.Fatalf("pair offered=%s requested=%s: wire errors %q != core errors %q",
					names[i], names[j], got.ErrorText(), want.ErrorText())
			}

			if got.WarningText() != want.WarningText() {
				t.Fatalf("pair offered=%s requested=%s: wire warnings %q != core warnings %q",
					names[i], names[j], got.WarningText(), want.WarningText())
			}
		}
	}
}

// TestWriterQoSRawVerdictEquivalence proves raw presets match identically post translation.
func TestWriterQoSRawVerdictEquivalence(t *testing.T) {
	var raw []core.QoSProfile

	raw = append(raw, core.FacadeAdvertisedQoS())
	for _, c := range writerQoSPresets(t) {
		raw = append(raw, c.profile)
	}

	for i, offered := range raw {
		wireOffered, err := ForwardQoS(writerQoSTestTopic, writerQoSTestPublisher, writerQoSTestSubscriber, offered)
		if err != nil {
			t.Fatalf("ForwardQoS offered %d: %v", i, err)
		}

		for j, requested := range raw {
			wireRequested, err := ForwardQoS(writerQoSTestTopic, writerQoSTestPublisher, writerQoSTestSubscriber, requested)
			if err != nil {
				t.Fatalf("ForwardQoS requested %d: %v", j, err)
			}

			rawReport := core.CheckCompatibility(offered, requested)
			wireReport := CheckWireCompatibility(wireOffered, wireRequested)

			if wireReport.Compatible != rawReport.Compatible {
				t.Fatalf("pair %d/%d: wire compatible=%v, raw core compatible=%v (wire errors: %s; raw errors: %s)",
					i, j, wireReport.Compatible, rawReport.Compatible, wireReport.ErrorText(), rawReport.ErrorText())
			}
		}
	}
}

// TestParseWireQoSEndianRejectsBigEndian pins loud BE refusal.
func TestParseWireQoSEndianRejectsBigEndian(t *testing.T) {
	t.Parallel()

	q, err := ForwardQoS(writerQoSTestTopic, writerQoSTestPublisher, writerQoSTestSubscriber, writerQoSExplicitKeepAll())
	if err != nil {
		t.Fatalf("forward qos: %v", err)
	}
	if _, err := ParseWireQoSEndian(q.Params(), true); err != nil {
		t.Fatalf("LE parse: %v", err)
	}
	params := q.Params()
	_, err = ParseWireQoSEndian(params, false)
	if !IsWireQoSParseRefusal(err) {
		t.Fatalf("BE parse = %v, want WireQoSParseRefusal", err)
	}
}

// TestWireQoSsedp3BlankPeerMatrix pins facade versus spec-default asymmetry.
//
// The facade announces reliable transient-local depth 100 while absent QoS
// decodes to DDS spec defaults (best-effort volatile depth 1), so the
// blank-peer matrix is asymmetric by design: a facade writer satisfies a
// blank reader, but a blank writer cannot satisfy a facade reliable reader.
func TestWireQoSsedp3BlankPeerMatrix(t *testing.T) {
	t.Parallel()

	facadeWriter := sedp3FacadeWireQoSsedp3(false)
	if facadeWriter.ReliabilityKind != sedpReliabilityReliable || facadeWriter.DurabilityKind != sedpDurabilityTransientLocal || facadeWriter.Depth != sedpHistoryDepth {
		t.Fatalf("facade writer = %+v, want reliable transient depth %d", facadeWriter, sedpHistoryDepth)
	}

	facadeReader := sedp3FacadeWireQoSsedp3(true)
	if facadeReader.DurabilityKind != sedpDurabilityVolatile {
		t.Fatalf("facade reader durability = %d, want volatile", facadeReader.DurabilityKind)
	}

	blank, err := ParseWireQoSsedp3WithDefaults(nil, true)
	if err != nil {
		t.Fatalf("blank defaults: %v", err)
	}

	if blank.ReliabilityKind != writerQoSReliabilityBestEffort || blank.DurabilityKind != sedpDurabilityVolatile || blank.Depth != 1 {
		t.Fatalf("blank = %+v, want best-effort volatile depth 1", blank)
	}

	if got := CheckWireCompatibility(facadeWriter, blank); !got.Compatible {
		t.Fatalf("facade writer vs blank reader incompatible, want compatible: %+v", got)
	}

	if got := CheckWireCompatibility(blank, facadeReader); got.Compatible {
		t.Fatalf("blank writer vs facade reader compatible, want RxO mismatch")
	}
}

// wireQoSTransientPair returns an offered plus requested WireQoS pair with
// the given durability kinds for caps-aware tests. ForwardQoS never
// announces transient or persistent, so the kinds are set directly on an
// otherwise valid default translation.
func wireQoSTransientPair(t *testing.T, offeredKind, requestedKind uint32) (WireQoS, WireQoS) {
	t.Helper()
	base, err := ForwardQoS(writerQoSTestTopic, writerQoSTestPublisher, writerQoSTestSubscriber, core.DefaultQoS())
	if err != nil {
		t.Fatalf("ForwardQoS default: %v", err)
	}
	base.DurabilityKind = offeredKind
	peer := base
	peer.DurabilityKind = requestedKind
	return base, peer
}

// TestCheckWireCompatibilityStrictRefusesTransient pins the plain path keeps
// refusing wire transient and persistent kinds loudly.
func TestCheckWireCompatibilityStrictRefusesTransient(t *testing.T) {
	t.Parallel()
	for _, kind := range []uint32{writerQoSDurabilityTransient, writerQoSDurabilityPersistent} {
		offered, requested := wireQoSTransientPair(t, kind, kind)
		report := CheckWireCompatibility(offered, requested)
		if !report.Compatible {
			t.Fatalf("strict path with durability kind %d incompatible, want compatible: %q", kind, report.Reason())
		}
	}
}

// TestCheckWireCompatibilityDurabilityServiceTransientEnforces pins caps
// decide transient pairs on the caps-aware path.
func TestCheckWireCompatibilityDurabilityServiceTransientEnforces(t *testing.T) {
	t.Parallel()
	offered, requested := wireQoSTransientPair(t, writerQoSDurabilityTransient, writerQoSDurabilityTransient)
	matching := CheckWireCompatibility(offered, requested, DefaultDurabilityServiceQoS(), DefaultDurabilityServiceQoS())
	if !matching.Compatible {
		t.Fatalf("matching transient caps must stay compatible, got %q", matching.Reason())
	}
	deepRequest := DefaultDurabilityServiceQoS()
	deepRequest.HistoryDepth = 10
	mismatch := CheckWireCompatibility(offered, requested, DefaultDurabilityServiceQoS(), deepRequest)
	if mismatch.Compatible {
		t.Fatal("transient depth 1 versus 10 must mismatch on service")
	}
	found := false
	for _, e := range mismatch.Errors {
		if e.Kind == core.PolicyDurability {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("transient caps mismatch must error on durability, got %+v", mismatch.Errors)
	}
	named := CheckWireCompatibilityWithDurabilityService(offered, requested, DefaultDurabilityServiceQoS(), deepRequest)
	if named.Compatible {
		t.Fatal("named caps path must mismatch like the variadic path")
	}
}

// TestCheckWireCompatibilityDurabilityServicePersistentEnforces pins caps
// decide persistent pairs on the caps-aware path.
func TestCheckWireCompatibilityDurabilityServicePersistentEnforces(t *testing.T) {
	t.Parallel()
	offered, requested := wireQoSTransientPair(t, writerQoSDurabilityPersistent, writerQoSDurabilityPersistent)
	matching := CheckWireCompatibility(offered, requested, DefaultDurabilityServiceQoS(), DefaultDurabilityServiceQoS())
	if !matching.Compatible {
		t.Fatalf("matching persistent caps must stay compatible, got %q", matching.Reason())
	}
	deepRequest := DefaultDurabilityServiceQoS()
	deepRequest.HistoryDepth = 10
	if got := CheckWireCompatibility(offered, requested, DefaultDurabilityServiceQoS(), deepRequest); got.Compatible {
		t.Fatal("persistent depth 1 versus 10 must mismatch on service")
	}
}

// TestCheckWireCompatibilityVolatileBypassIgnoresCaps pins volatile and
// transient local pairs ignore caps on the caps-aware path.
func TestCheckWireCompatibilityVolatileBypassIgnoresCaps(t *testing.T) {
	t.Parallel()
	badCaps := DurabilityServiceQoS{HistoryKind: 99}
	offered, err := ForwardQoS(writerQoSTestTopic, writerQoSTestPublisher, writerQoSTestSubscriber, core.DefaultQoS())
	if err != nil {
		t.Fatalf("ForwardQoS default: %v", err)
	}
	transientLocal, err := ForwardQoS(writerQoSTestTopic, writerQoSTestPublisher, writerQoSTestSubscriber, core.RosoutQoS())
	if err != nil {
		t.Fatalf("ForwardQoS rosout: %v", err)
	}
	report := CheckWireCompatibility(transientLocal, offered, badCaps, badCaps)
	if !report.Compatible {
		t.Fatalf("transient local versus volatile must ignore caps, got %q", report.Reason())
	}
	volatileOffered, err := ForwardQoS(writerQoSTestTopic, writerQoSTestPublisher, writerQoSTestSubscriber, core.DefaultQoS())
	if err != nil {
		t.Fatalf("ForwardQoS default volatile: %v", err)
	}
	if got := CheckWireCompatibility(volatileOffered, offered, badCaps, badCaps); !got.Compatible {
		t.Fatalf("volatile versus volatile must ignore caps, got %q", got.Reason())
	}
}

// TestCheckWireCompatibilityOddCapsFailsClosed pins odd caps counts never match silently.
func TestCheckWireCompatibilityOddCapsFailsClosed(t *testing.T) {
	t.Parallel()
	offered, err := ForwardQoS(writerQoSTestTopic, writerQoSTestPublisher, writerQoSTestSubscriber, core.DefaultQoS())
	if err != nil {
		t.Fatalf("ForwardQoS default: %v", err)
	}
	for _, caps := range [][]DurabilityServiceQoS{
		{DefaultDurabilityServiceQoS()},
		{DefaultDurabilityServiceQoS(), DefaultDurabilityServiceQoS(), DefaultDurabilityServiceQoS()},
	} {
		report := CheckWireCompatibility(offered, offered, caps...)
		if report.Compatible {
			t.Fatalf("caps count %d compatible, want fail-closed", len(caps))
		}
		if len(report.Errors) == 0 || report.Errors[0].Kind != core.PolicyDurability {
			t.Fatalf("caps count %d must error on durability, got %+v", len(caps), report.Errors)
		}
	}
}

// TestForwardQoSAllDurabilityKinds pins all four durability kinds announce.
func TestForwardQoSAllDurabilityKinds(t *testing.T) {
	t.Parallel()
	base := core.DefaultQoS()
	kinds := []struct {
		policy core.DurabilityPolicy
		wire   uint32
	}{
		{core.DurabilityVolatile, sedpDurabilityVolatile},
		{core.DurabilityTransientLocal, sedpDurabilityTransientLocal},
		{core.Durabilityqos4Transient, writerQoSDurabilityTransient},
		{core.Durabilityqos4Persistent, writerQoSDurabilityPersistent},
	}
	for _, k := range kinds {
		p := base
		p.Durability = k.policy
		q, err := ForwardQoS(writerQoSTestTopic, writerQoSTestPublisher, writerQoSTestSubscriber, p)
		if err != nil {
			t.Fatalf("ForwardQoS durability %v: %v", k.policy, err)
		}
		if q.DurabilityKind != k.wire {
			t.Fatalf("durability %v wire = %d, want %d", k.policy, q.DurabilityKind, k.wire)
		}
		if err := q.Validate(); err != nil {
			t.Fatalf("Validate durability %v: %v", k.policy, err)
		}
		back, err := ReverseQoS(q)
		if err != nil {
			t.Fatalf("ReverseQoS durability %v: %v", k.policy, err)
		}
		if back.Durability != k.policy {
			t.Fatalf("reverse durability = %v, want %v", back.Durability, k.policy)
		}
	}
}

// TestForwardQoSAllLivelinessKinds pins all three liveliness kinds announce.
func TestForwardQoSAllLivelinessKinds(t *testing.T) {
	t.Parallel()
	base := core.DefaultQoS()
	kinds := []struct {
		policy core.LivelinessPolicy
		wire   uint32
	}{
		{core.LivelinessAutomatic, writerQoSLivelinessAutomatic},
		{core.Livelinessqos4ManualByParticipant, writerQoSLivelinessManualByParticipant},
		{core.LivelinessManualByTopic, writerQoSLivelinessManualByTopic},
	}
	for _, k := range kinds {
		p := base
		p.Liveliness = k.policy
		p.LeaseDuration = core.InfiniteDuration()
		q, err := ForwardQoS(writerQoSTestTopic, writerQoSTestPublisher, writerQoSTestSubscriber, p)
		if err != nil {
			t.Fatalf("ForwardQoS liveliness %v: %v", k.policy, err)
		}
		if q.LivelinessKind != k.wire {
			t.Fatalf("liveliness %v wire = %d, want %d", k.policy, q.LivelinessKind, k.wire)
		}
		if err := q.Validate(); err != nil {
			t.Fatalf("Validate liveliness %v: %v", k.policy, err)
		}
		back, err := ReverseQoS(q)
		if err != nil {
			t.Fatalf("ReverseQoS liveliness %v: %v", k.policy, err)
		}
		if back.Liveliness != k.policy {
			t.Fatalf("reverse liveliness = %v, want %v", back.Liveliness, k.policy)
		}
	}
}

// TestWriterQoSPresentationLatencyRoundTrip pins PID 0x0021 and 0x0027 encode plus parse.
func TestWriterQoSPresentationLatencyRoundTrip(t *testing.T) {
	t.Parallel()
	checkPresentationPIDs(t)
	q := forwardPresentationFixture(t)
	parsed := parsePresentationFixture(t, q)
	checkPresentationReverse(t, parsed)
	checkPresentationHelpers(t)
}

// checkPresentationPIDs pins presentation plus latency PIDs.
func checkPresentationPIDs(t *testing.T) {
	t.Helper()
	if writerQoSPIDPresentation != 0x0021 {
		t.Fatalf("presentation PID = %#04x, want 0x0021 per DDSI-RTPS Table 9.13", writerQoSPIDPresentation)
	}
	if writerQoSPIDLatencyBudget != 0x0027 {
		t.Fatalf("latency PID = %#04x, want 0x0027 per DDSI-RTPS Table 9.13", writerQoSPIDLatencyBudget)
	}
}

// forwardPresentationFixture builds wire QoS with group plus latency.
func forwardPresentationFixture(t *testing.T) WireQoS {
	t.Helper()
	p := core.DefaultQoS()
	p.Presentation = core.PresentationQosPolicy{AccessScope: core.PresentationGroup, CoherentAccess: true, OrderedAccess: true}
	p.LatencyBudget = core.LatencyBudgetQosPolicy{Duration: core.NewDuration(2, 500000000)}
	q, err := ForwardQoS(writerQoSTestTopic, writerQoSTestPublisher, writerQoSTestSubscriber, p)
	if err != nil {
		t.Fatalf("ForwardQoS presentation: %v", err)
	}
	if q.PresentationScope != 2 || q.PresentationCoherent != 1 || q.PresentationOrdered != 1 {
		t.Fatalf("presentation wire = %d %d %d, want 2 1 1", q.PresentationScope, q.PresentationCoherent, q.PresentationOrdered)
	}
	return q
}

// parsePresentationFixture parses wire QoS params.
func parsePresentationFixture(t *testing.T, q WireQoS) WireQoS {
	t.Helper()
	parsed, err := ParseWireQoS(q.Params())
	if err != nil {
		t.Fatalf("ParseWireQoS presentation: %v", err)
	}
	if parsed.PresentationScope != 2 || parsed.PresentationCoherent != 1 || parsed.PresentationOrdered != 1 {
		t.Fatalf("parsed presentation = %d %d %d, want 2 1 1", parsed.PresentationScope, parsed.PresentationCoherent, parsed.PresentationOrdered)
	}
	return parsed
}

// checkPresentationReverse verifies reverse mapping.
func checkPresentationReverse(t *testing.T, parsed WireQoS) {
	t.Helper()
	back, err := ReverseQoS(parsed)
	if err != nil {
		t.Fatalf("ReverseQoS presentation: %v", err)
	}
	if back.Presentation.AccessScope != core.PresentationGroup || !back.Presentation.CoherentAccess || !back.Presentation.OrderedAccess {
		t.Fatalf("reverse presentation = %+v, want group coherent ordered", back.Presentation)
	}
	if back.LatencyBudget.Duration != core.NewDuration(2, 500000000) {
		t.Fatalf("reverse latency = %v, want 2.5s", back.LatencyBudget.Duration)
	}
}

// checkPresentationHelpers verifies helper round trips.
func checkPresentationHelpers(t *testing.T) {
	t.Helper()
	scope, coherent, ordered, err := DecodePresentation(EncodePresentation(2, 1, 0))
	if err != nil {
		t.Fatalf("presentation helper round trip: %v", err)
	}
	if scope != 2 || coherent != 1 || ordered != 0 {
		t.Fatalf("helper presentation = %d %d %d, want 2 1 0", scope, coherent, ordered)
	}
	dur, err := DecodeLatencyBudget(EncodeLatencyBudget(WireDuration{Seconds: 3}))
	if err != nil {
		t.Fatalf("latency helper round trip: %v", err)
	}
	if dur.Seconds != 3 {
		t.Fatalf("helper latency seconds = %d, want 3", dur.Seconds)
	}
	if _, _, _, err := DecodePresentation([]byte{1, 2, 3}); err == nil {
		t.Fatal("short presentation decoded, want loud refusal")
	}
}
