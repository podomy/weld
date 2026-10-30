// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package core

import (
	"strings"
	"testing"
)

// TestMismatchLive wires loud emission across logs plus metrics plus CLI shapes.
func TestMismatchLive(t *testing.T) {
	checkLiveReliabilityMismatch(t)
	checkLiveDurabilityMismatch(t)
	checkLiveCompatibleEmitsNothing(t)
	checkLiveCountsPlusPolicyKinds(t)
	checkLiveDeadlineMonitoring(t)
	checkLiveReporterNilSafety(t)
}

// liveCapture returns a reporter with a fresh counter plus a lines pointer
// capturing every logged line.
func liveCapture() (*LiveMismatchReporter, *[]string) {
	var lines []string
	r := NewLiveMismatchReporter(nil, func(line string) {
		lines = append(lines, line)
	})
	return r, &lines
}

// liveBase returns the resolved default profile both sides flow with.
func liveBase() QoSProfile {
	return ResolveSystemDefaults(DefaultQoS())
}

// checkLiveReliabilityMismatch pins best effort versus reliable as a loud error.
func checkLiveReliabilityMismatch(t *testing.T) {
	t.Helper()
	base := liveBase()
	offered := base
	offered.Reliability = ReliabilityBestEffort
	r, lines := liveCapture()
	events := r.Emit("/scan", "talker", "listener", offered, base)
	if len(events) != 1 {
		t.Fatalf("reliability pair must yield one event, got %+v", events)
	}
	e := events[0]
	if e.Policy != PolicyReliability {
		t.Fatalf("policy = %q, want %q", e.Policy, PolicyReliability)
	}
	if e.Severity != SeverityError {
		t.Fatalf("severity = %q, want error", e.Severity)
	}
	assertLiveCarries(t, e)
	if got := r.Counter().Count(PolicyReliability); got != 1 {
		t.Fatalf("reliability count = %d, want 1", got)
	}
	if got := r.Counter().Total(); got != 1 {
		t.Fatalf("total = %d, want 1", got)
	}
	if len(*lines) != 1 {
		t.Fatalf("log lines = %d, want 1", len(*lines))
	}
	assertLiveLogCarries(t, (*lines)[0], PolicyReliability)
	metrics := FormatMismatchMetrics(r.Counter())
	assertLiveMetricsCarries(t, metrics, PolicyReliability, 1, 1)
}

// checkLiveDurabilityMismatch pins volatile versus transient local as a loud error.
func checkLiveDurabilityMismatch(t *testing.T) {
	t.Helper()
	base := liveBase()
	requested := base
	requested.Durability = DurabilityTransientLocal
	r, lines := liveCapture()
	events := r.Emit("/scan", "talker", "listener", base, requested)
	if len(events) != 1 {
		t.Fatalf("durability pair must yield one event, got %+v", events)
	}
	e := events[0]
	if e.Policy != PolicyDurability {
		t.Fatalf("policy = %q, want %q", e.Policy, PolicyDurability)
	}
	if e.Severity != SeverityError {
		t.Fatalf("severity = %q, want error", e.Severity)
	}
	assertLiveCarries(t, e)
	if got := r.Counter().Count(PolicyDurability); got != 1 {
		t.Fatalf("durability count = %d, want 1", got)
	}
	if len(*lines) != 1 {
		t.Fatalf("log lines = %d, want 1", len(*lines))
	}
	assertLiveLogCarries(t, (*lines)[0], PolicyDurability)
}

// checkLiveCompatibleEmitsNothing pins clean pairs record nothing anywhere.
func checkLiveCompatibleEmitsNothing(t *testing.T) {
	t.Helper()
	base := liveBase()
	r, lines := liveCapture()
	events := r.Emit("/scan", "talker", "listener", base, base)
	if len(events) != 0 {
		t.Fatalf("compatible pair must emit nothing, got %+v", events)
	}
	if got := r.Counter().Total(); got != 0 {
		t.Fatalf("compatible total = %d, want 0", got)
	}
	if len(*lines) != 0 {
		t.Fatalf("compatible log lines = %d, want 0", len(*lines))
	}
}

// livePolicyCase is one single policy incompatibility expectation.
type livePolicyCase struct {
	name      string
	offered   QoSProfile
	requested QoSProfile
	policy    PolicyKind
}

// livePolicyCases returns one incompatible pair per RxO policy kind.
func livePolicyCases(base QoSProfile) []livePolicyCase {
	withReliabilityOffer := base
	withReliabilityOffer.Reliability = ReliabilityBestEffort
	withReliabilityRequest := base
	withDurabilityRequest := base
	withDurabilityRequest.Durability = DurabilityTransientLocal
	withDeadlineRequest := base
	withDeadlineRequest.Deadline = NewDuration(0, 20000000)
	withLivelinessOffer := base
	withLivelinessRequest := base
	withLivelinessRequest.Liveliness = LivelinessManualByTopic
	withLeaseRequest := base
	withLeaseRequest.LeaseDuration = NewDuration(4, 0)
	return []livePolicyCase{
		{"reliability", withReliabilityOffer, withReliabilityRequest, PolicyReliability},
		{"durability", base, withDurabilityRequest, PolicyDurability},
		{"deadline", base, withDeadlineRequest, PolicyDeadline},
		{"liveliness", withLivelinessOffer, withLivelinessRequest, PolicyLiveliness},
		{"lease", base, withLeaseRequest, PolicyLeaseDuration},
	}
}

// checkLiveCountsPlusPolicyKinds pins every policy counts once with total five.
func checkLiveCountsPlusPolicyKinds(t *testing.T) {
	t.Helper()
	base := liveBase()
	cases := livePolicyCases(base)
	r, lines := liveCapture()
	for _, tc := range cases {
		events := r.Emit("/scan", "talker", "listener", tc.offered, tc.requested)
		if len(events) != 1 {
			t.Fatalf("%s must yield one event, got %+v", tc.name, events)
		}
		if events[0].Policy != tc.policy {
			t.Fatalf("%s policy = %q, want %q", tc.name, events[0].Policy, tc.policy)
		}
		if events[0].Severity != SeverityError {
			t.Fatalf("%s severity = %q, want error", tc.name, events[0].Severity)
		}
		assertLiveCarries(t, events[0])
	}
	for _, tc := range cases {
		if got := r.Counter().Count(tc.policy); got != 1 {
			t.Fatalf("%s count = %d, want 1", tc.name, got)
		}
	}
	if got := r.Counter().Total(); got != 5 {
		t.Fatalf("total = %d, want 5", got)
	}
	if len(*lines) != 5 {
		t.Fatalf("log lines = %d, want 5", len(*lines))
	}
	metrics := FormatMismatchMetrics(r.Counter())
	want := "weld_mismatch{policy=\"reliability\"} 1\n" +
		"weld_mismatch{policy=\"durability\"} 1\n" +
		"weld_mismatch{policy=\"deadline\"} 1\n" +
		"weld_mismatch{policy=\"liveliness\"} 1\n" +
		"weld_mismatch{policy=\"lease_duration\"} 1\n" +
		"weld_mismatch_total 5\n"
	if metrics != want {
		t.Fatalf("metrics exposition = %q, want %q", metrics, want)
	}
}

// checkLiveDeadlineMonitoring pins alerts as monitoring plus deadline errors as unmatch.
func checkLiveDeadlineMonitoring(t *testing.T) {
	t.Helper()
	tracker := NewDeadlineTracker("/scan", "listener", NewDuration(0, 100000000), 0)
	alert, ok := tracker.Check(200000000)
	if !ok {
		t.Fatal("missed deadline must alert")
	}
	r, lines := liveCapture()
	line := r.EmitDeadlineAlert(alert)
	if !strings.Contains(line, "/scan") || !strings.Contains(line, "listener") {
		t.Fatalf("alert line must carry topic plus endpoint, got %q", line)
	}
	if len(*lines) != 1 || (*lines)[0] != line {
		t.Fatalf("alert must log loudly once, got %v", *lines)
	}
	base := liveBase()
	requested := base
	requested.Deadline = NewDuration(0, 20000000)
	events := r.Emit("/scan", "talker", "listener", base, requested)
	found := false
	for _, e := range events {
		if e.Policy == PolicyDeadline && e.Severity == SeverityError {
			found = true
		}
	}
	if !found {
		t.Fatalf("deadline mismatch must raise an error event, got %+v", events)
	}
}

// checkLiveReporterNilSafety pins nil receivers never panic and never go silent.
func checkLiveReporterNilSafety(t *testing.T) {
	t.Helper()
	base := liveBase()
	offered := base
	offered.Reliability = ReliabilityBestEffort
	var r *LiveMismatchReporter
	events := r.Emit("/scan", "talker", "listener", offered, base)
	if len(events) != 1 {
		t.Fatalf("nil reporter must still return events, got %+v", events)
	}
	if r.Counter() != nil {
		t.Fatal("nil reporter counter must be nil")
	}
	line := r.EmitDeadlineAlert(DeadlineAlert{Topic: "/t", Endpoint: "e"})
	if !strings.Contains(line, "/t") {
		t.Fatalf("nil reporter alert line = %q, want topic", line)
	}
	quiet := NewLiveMismatchReporter(nil, nil)
	events = quiet.Emit("/scan", "talker", "listener", offered, base)
	if len(events) != 1 {
		t.Fatalf("nil log reporter must still return events, got %+v", events)
	}
	if got := quiet.Counter().Total(); got != 1 {
		t.Fatalf("nil log reporter total = %d, want 1", got)
	}
	metrics := FormatMismatchMetrics(nil)
	if !strings.Contains(metrics, "weld_mismatch_total 0") {
		t.Fatalf("nil counter metrics = %q, want zero total", metrics)
	}
}

// assertLiveCarries pins one event carries topic, pair, and policy across shapes.
func assertLiveCarries(t *testing.T, e MismatchEvent) {
	t.Helper()
	if e.Topic != "/scan" || e.Publisher != "talker" || e.Subscriber != "listener" {
		t.Fatalf("event must carry topic plus endpoint pair, got %+v", e)
	}
	if !strings.Contains(e.String(), string(e.Policy)) || !strings.Contains(e.LogLine(), string(e.Policy)) {
		t.Fatalf("event shapes must carry policy kind, got %+v", e)
	}
	if !strings.Contains(e.String(), "/scan") || !strings.Contains(e.LogLine(), "talker") {
		t.Fatalf("event shapes must carry names, got %+v", e)
	}
}

// assertLiveLogCarries pins one log line carries topic, pair, and policy.
func assertLiveLogCarries(t *testing.T, line string, policy PolicyKind) {
	t.Helper()
	for _, want := range []string{"/scan", "talker", "listener", string(policy)} {
		if !strings.Contains(line, want) {
			t.Fatalf("log line misses %q: %q", want, line)
		}
	}
}

// assertLiveMetricsCarries pins metrics text carries one policy count plus total.
func assertLiveMetricsCarries(t *testing.T, metrics string, policy PolicyKind, count, total uint64) {
	t.Helper()
	wantPolicy := "weld_mismatch{policy=\"" + string(policy) + "\"} 1"
	if count == 1 && !strings.Contains(metrics, wantPolicy) {
		t.Fatalf("metrics miss %q:\n%s", wantPolicy, metrics)
	}
	wantTotal := "weld_mismatch_total 1"
	if total == 1 && !strings.Contains(metrics, wantTotal) {
		t.Fatalf("metrics miss %q:\n%s", wantTotal, metrics)
	}
}
