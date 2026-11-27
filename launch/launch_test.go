// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

package launch_test

import (
	"testing"

	"github.com/podomy/weld/launch"
)

// mustParseTestArgs parses the fixed test argv or fails the test.
func mustParseTestArgs(t *testing.T) launch.ROSArgs {
	t.Helper()

	argv := []string{
		"--ros-args",
		"-r", "__node:=talker",
		"-r", "__ns:=/ns",
		"-p", "speed:=2",
		"-r", "old:=/new",
	}
	got, err := launch.ParseROSArgs(argv)
	if err != nil {
		t.Fatalf("parse ros args: %v", err)
	}

	return got
}

// checkNodeName verifies the parsed node name.
func checkNodeName(t *testing.T, got launch.ROSArgs) {
	t.Helper()

	if !got.HasNodeName || got.NodeName != "talker" {
		t.Fatalf("node = %q %v, want talker true", got.NodeName, got.HasNodeName)
	}
}

// checkNamespace verifies the parsed namespace.
func checkNamespace(t *testing.T, got launch.ROSArgs) {
	t.Helper()

	if !got.HasNamespace || got.Namespace != "/ns" {
		t.Fatalf("namespace = %q %v, want /ns true", got.Namespace, got.HasNamespace)
	}
}

// checkSpeedParam verifies the parsed speed override.
func checkSpeedParam(t *testing.T, got launch.ROSArgs) {
	t.Helper()

	speed, ok := got.Params["speed"]
	if !ok {
		t.Fatal("speed override missing")
	}
	if speed.Integer != 2 {
		t.Fatalf("speed = %+v, want integer 2", speed)
	}
}

// checkRemapRule verifies the single remap rule.
func checkRemapRule(t *testing.T, got launch.ROSArgs) {
	t.Helper()

	if len(got.Remaps) != 1 || got.Remaps[0].From != "old" {
		t.Fatalf("remaps = %+v, want one old rule", got.Remaps)
	}
}

// TestParseROSArgs checks node plus namespace plus param parsing.
func TestParseROSArgs(t *testing.T) {
	got := mustParseTestArgs(t)
	checkNodeName(t, got)
	checkNamespace(t, got)
	checkSpeedParam(t, got)
	checkRemapRule(t, got)
}

// TestParseROSArgsRejectsBadFlag checks unknown flags fail loudly.
func TestParseROSArgsRejectsBadFlag(t *testing.T) {
	_, err := launch.ParseROSArgs([]string{"--ros-args", "--unknown", "x"})
	if err == nil {
		t.Fatal("unknown flag accepted")
	}
}
