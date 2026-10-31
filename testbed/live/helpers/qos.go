// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package helpers

import (
	"testing"

	"github.com/podomy/weld/internal/core"
)

// ResolveQoSDirection maps direction to offered plus requested plus names.
//
// Forward uses the ROS profile as offered and the facade as requested with
// ros-pub and weld-echo endpoints. Reverse swaps them with weld-pub and
// ros-echo endpoints. The label is unused beyond docs; callers pass the
// direction string from their case table.
func ResolveQoSDirection(direction string, rosProfile, facade core.QoSProfile) (core.QoSProfile, core.QoSProfile, string, string) {
	if direction == "forward" {
		return rosProfile, facade, "ros-pub", "weld-echo"
	}
	return facade, rosProfile, "weld-pub", "ros-echo"
}

// CheckQoSCompatible asserts a compatible report has no errors.
func CheckQoSCompatible(t *testing.T, name string, report core.CompatibilityReport) {
	t.Helper()
	if !report.Compatible {
		t.Fatalf("case %s: want compatible, got errors: %s", name, report.ErrorText())
	}
	if len(report.Errors) != 0 {
		t.Fatalf("case %s: compatible with %d errors, want zero", name, len(report.Errors))
	}
}

// CheckQoSIncompatible asserts an incompatible report names the policy loudly.
func CheckQoSIncompatible(t *testing.T, name string, report core.CompatibilityReport, offered, requested core.QoSProfile, publisher, subscriber string, wantPolicy core.PolicyKind, logPrefix string, domain int, runID string) {
	t.Helper()
	if report.Compatible {
		t.Fatalf("case %s: want incompatible, got compatible", name)
	}
	if len(report.Errors) == 0 {
		t.Fatalf("case %s: incompatible with no errors, want explicit %s error", name, string(wantPolicy))
	}
	found := false
	for _, issue := range report.Errors {
		if issue.Kind == wantPolicy {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("case %s: errors %s miss policy %s", name, report.ErrorText(), string(wantPolicy))
	}
	events := core.CompatibilityEvents("/chatter", publisher, subscriber, offered, requested)
	if len(events) == 0 {
		t.Fatalf("case %s: no loud events for incompatible pair", name)
	}
	for _, ev := range events {
		t.Logf("%s mismatch domain=%d run=%s topic=%s publisher=%s subscriber=%s policy=%s offered=%s requested=%s: %s", logPrefix, domain, runID, ev.Topic, ev.Publisher, ev.Subscriber, string(ev.Policy), ev.Offered, ev.Requested, ev.Detail)
	}
}
