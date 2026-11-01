// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package main

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/podomy/weld/internal/core"
)

// doctorTestTransientLocal returns a reliable transient local profile.
func doctorTestTransientLocal() core.QoSProfile {
	p := topicTestQoS()
	p.Durability = core.DurabilityTransientLocal
	return p
}

// doctorTestMismatchSnapshot returns one topic with a best effort publisher
// against a reliable subscriber for loud mismatch tests.
func doctorTestMismatchSnapshot() graphSnapshot {
	return buildGraphSnapshot([]topicEndpoint{
		newTopicEndpointWithQoS("pub", "/scan", "t", "h", false, topicTestBestEffort()),
		newTopicEndpointWithQoS("sub", "/scan", "t", "h", true, topicTestQoS()),
	}, 0)
}

// TestBuildDoctorReportReliabilityMismatch checks best effort versus reliable raises a loud error.
func TestBuildDoctorReportReliabilityMismatch(t *testing.T) {
	report := buildDoctorReport(doctorTestMismatchSnapshot(), nil)
	if len(report.Mismatches) != 1 {
		t.Fatalf("mismatches = %d, want 1: %+v", len(report.Mismatches), report.Mismatches)
	}
	m := report.Mismatches[0]
	if m.Topic != "/scan" || m.Publisher != "pub" || m.Subscriber != "sub" {
		t.Fatalf("mismatch must carry topic plus pair, got %+v", m)
	}
	if m.Policy != core.PolicyReliability {
		t.Fatalf("policy = %q, want reliability", m.Policy)
	}
	if m.Severity != core.SeverityError {
		t.Fatalf("severity = %q, want error", m.Severity)
	}
	if !doctorHasErrors(report) {
		t.Fatal("report with an error event must have errors")
	}
	if doctorIsClean(report) {
		t.Fatal("mismatched report must not be clean")
	}
	if got := report.Counter.Count(core.PolicyReliability); got != 1 {
		t.Fatalf("reliability count = %d, want 1", got)
	}
	if got := report.Counter.Total(); got != 1 {
		t.Fatalf("total = %d, want 1", got)
	}
}

// TestBuildDoctorReportDurabilityMismatch checks volatile versus transient local raises a loud error.
func TestBuildDoctorReportDurabilityMismatch(t *testing.T) {
	snap := buildGraphSnapshot([]topicEndpoint{
		newTopicEndpointWithQoS("pub", "/scan", "t", "h", false, topicTestQoS()),
		newTopicEndpointWithQoS("sub", "/scan", "t", "h", true, doctorTestTransientLocal()),
	}, 0)
	report := buildDoctorReport(snap, core.NewMismatchCounter())
	if len(report.Mismatches) != 1 {
		t.Fatalf("mismatches = %d, want 1: %+v", len(report.Mismatches), report.Mismatches)
	}
	m := report.Mismatches[0]
	if m.Policy != core.PolicyDurability {
		t.Fatalf("policy = %q, want durability", m.Policy)
	}
	if m.Severity != core.SeverityError {
		t.Fatalf("severity = %q, want error", m.Severity)
	}
	if m.Topic != "/scan" || m.Publisher != "pub" || m.Subscriber != "sub" {
		t.Fatalf("mismatch must carry topic plus pair, got %+v", m)
	}
	if got := report.Counter.Count(core.PolicyDurability); got != 1 {
		t.Fatalf("durability count = %d, want 1", got)
	}
}

// TestBuildDoctorReportCleanForFacadePairs checks facade pairs stay clean.
func TestBuildDoctorReportCleanForFacadePairs(t *testing.T) {
	snap := buildGraphSnapshot([]topicEndpoint{
		newTopicEndpointWithQoS("weld-pub", "/chatter", "std_msgs__msg__String", "h1", false, core.FacadeAdvertisedQoS()),
		newTopicEndpointWithQoS("ros-sub", "/chatter", "std_msgs__msg__String", "h1", true, topicTestQoS()),
	}, 0)
	report := buildDoctorReport(snap, nil)
	if len(report.Mismatches) != 0 {
		t.Fatalf("facade pairs must stay clean, got %+v", report.Mismatches)
	}
	if doctorHasErrors(report) {
		t.Fatal("clean report must have no errors")
	}
	if !doctorIsClean(report) {
		t.Fatal("facade pairs must be clean")
	}
	if got := report.Counter.Total(); got != 0 {
		t.Fatalf("clean total = %d, want 0", got)
	}
}

// TestFormatDoctorReportTopicInfoDisplay checks report carries topic info plus loud lines plus metrics.
func TestFormatDoctorReportTopicInfoDisplay(t *testing.T) {
	report := buildDoctorReport(doctorTestMismatchSnapshot(), nil)
	out := formatDoctorReport(report)
	for _, want := range []string{"Doctor:", "Topic: /scan", "gid=pub", "gid=sub", "reliability", "topic=/scan", "publisher=pub", "subscriber=sub", "Metrics:", "weld_mismatch_total 1"} {
		if !strings.Contains(out, want) {
			t.Fatalf("report misses %q:\n%s", want, out)
		}
	}
	if !strings.Contains(out, "weld_mismatch{policy=\"reliability\"} 1") {
		t.Fatalf("report misses reliability metric:\n%s", out)
	}
	clean := buildDoctorReport(buildGraphSnapshot([]topicEndpoint{
		newTopicEndpointWithQoS("pub", "/scan", "t", "h", false, topicTestQoS()),
		newTopicEndpointWithQoS("sub", "/scan", "t", "h", true, topicTestQoS()),
	}, 0), nil)
	cleanOut := formatDoctorReport(clean)
	if !strings.Contains(cleanOut, "Mismatches: none") {
		t.Fatalf("clean report misses none line:\n%s", cleanOut)
	}
	if !strings.Contains(cleanOut, "weld_mismatch_total 0") {
		t.Fatalf("clean report misses zero total:\n%s", cleanOut)
	}
}

// TestFormatDoctorMetricsCounters checks metrics order plus counts.
func TestFormatDoctorMetricsCounters(t *testing.T) {
	snap := buildGraphSnapshot([]topicEndpoint{
		newTopicEndpointWithQoS("pub", "/scan", "t", "h", false, topicTestBestEffort()),
		newTopicEndpointWithQoS("sub", "/scan", "t", "h", true, doctorTestTransientLocal()),
	}, 0)
	report := buildDoctorReport(snap, nil)
	if got := report.Counter.Total(); got != 2 {
		t.Fatalf("total = %d, want 2", got)
	}
	out := formatDoctorMetrics(report.Counter)
	reliability := strings.Index(out, "weld_mismatch{policy=\"reliability\"} 1")
	durability := strings.Index(out, "weld_mismatch{policy=\"durability\"} 1")
	total := strings.Index(out, "weld_mismatch_total 2")
	if reliability < 0 || durability < 0 || total < 0 {
		t.Fatalf("metrics miss counters:\n%s", out)
	}
	if reliability >= durability || durability >= total {
		t.Fatalf("metrics out of order:\n%s", out)
	}
}

// TestRunDoctorCheckFailsOnMismatch checks --check exits 1 with loud output.
func TestRunDoctorCheckFailsOnMismatch(t *testing.T) {
	fb := &fakeTopicBackend{hasSnap: true, snapshot: doctorTestMismatchSnapshot()}
	stubTopicBackend(t, fb)
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	code := runDoctorCommand(context.Background(), []string{"--check", "--domain", "42"}, &stdout, &stderr)
	if code != 1 {
		t.Fatalf("check code = %d, want 1\nstderr:\n%s", code, stderr.String())
	}
	for _, want := range []string{"Topic: /scan", "gid=pub", "Metrics:", "weld_mismatch_total"} {
		if !strings.Contains(stdout.String(), want) {
			t.Fatalf("check stdout misses %q:\n%s", want, stdout.String())
		}
	}
	for _, want := range []string{"weld:", "topic=/scan", string(core.PolicyReliability), "doctor check failed"} {
		if !strings.Contains(stderr.String(), want) {
			t.Fatalf("check stderr misses %q:\n%s", want, stderr.String())
		}
	}
}

// TestRunDoctorCheckClean checks --check exits 0 without loud lines.
func TestRunDoctorCheckClean(t *testing.T) {
	fb := &fakeTopicBackend{hasSnap: true, snapshot: buildGraphSnapshot([]topicEndpoint{
		newTopicEndpointWithQoS("weld-pub", "/chatter", "std_msgs__msg__String", "h1", false, core.FacadeAdvertisedQoS()),
		newTopicEndpointWithQoS("ros-sub", "/chatter", "std_msgs__msg__String", "h1", true, topicTestQoS()),
	}, 0)}
	stubTopicBackend(t, fb)
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	code := runDoctorCommand(context.Background(), []string{"--check", "--domain", "42"}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("clean check code = %d, want 0\nstderr:\n%s", code, stderr.String())
	}
	if !strings.Contains(stdout.String(), "Mismatches: none") {
		t.Fatalf("clean check misses none line:\n%s", stdout.String())
	}
	if stderr.Len() != 0 {
		t.Fatalf("clean check stderr = %q, want silence", stderr.String())
	}
}

// TestRunDoctorReportListsEntities checks default run lists topics with metrics.
func TestRunDoctorReportListsEntities(t *testing.T) {
	fb := &fakeTopicBackend{hasSnap: true, snapshot: doctorTestMismatchSnapshot()}
	stubTopicBackend(t, fb)
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	code := runDoctorCommand(context.Background(), []string{"--domain", "42"}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("report code = %d, want 0\nstderr:\n%s", code, stderr.String())
	}
	for _, want := range []string{"Topic: /scan", "gid=pub", "gid=sub", "Mismatches (1):", "weld_mismatch_total 1"} {
		if !strings.Contains(stdout.String(), want) {
			t.Fatalf("report stdout misses %q:\n%s", want, stdout.String())
		}
	}
	if !strings.Contains(stderr.String(), "weld:") {
		t.Fatalf("report stderr misses loud lines:\n%s", stderr.String())
	}
}

// TestRunDoctorHelpAndUsage checks help plus usage failures.
func TestRunDoctorHelpAndUsage(t *testing.T) {
	fb := &fakeTopicBackend{hasSnap: true}
	stubTopicBackend(t, fb)
	var helpOut bytes.Buffer
	var helpErr bytes.Buffer
	code := runDoctorCommand(context.Background(), []string{"-h"}, &helpOut, &helpErr)
	if code != 0 {
		t.Fatalf("help code = %d, want 0", code)
	}
	if !strings.Contains(helpOut.String(), "Usage:") {
		t.Fatalf("help misses Usage:\n%s", helpOut.String())
	}
	var posOut bytes.Buffer
	var posErr bytes.Buffer
	code = runDoctorCommand(context.Background(), []string{"/extra"}, &posOut, &posErr)
	if code != 2 {
		t.Fatalf("positional code = %d, want 2", code)
	}
	var domainOut bytes.Buffer
	var domainErr bytes.Buffer
	code = runDoctorCommand(context.Background(), []string{"--domain", "99999"}, &domainOut, &domainErr)
	if code != 2 {
		t.Fatalf("bad domain code = %d, want 2", code)
	}
}
