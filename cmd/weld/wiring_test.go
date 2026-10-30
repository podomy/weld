// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package main

import (
	"bytes"
	"context"
	"strings"
	"testing"
)

// wiringRun runs the top level dispatcher and returns code plus outputs.
func wiringRun(args []string) (int, string, string) {
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	code := run(context.Background(), args, &stdout, &stderr)
	return code, stdout.String(), stderr.String()
}

// TestRunTopHelpExitsZero checks top level help exits 0 with usage.
func TestRunTopHelpExitsZero(t *testing.T) {
	for _, args := range [][]string{{"-h"}, {"--help"}, {"help"}} {
		code, stdout, _ := wiringRun(args)
		if code != 0 {
			t.Fatalf("args %q code = %d, want 0", args, code)
		}
		if !strings.Contains(stdout, "Usage:") {
			t.Fatalf("args %q stdout misses Usage: %q", args, stdout)
		}
	}
}

// TestRunUsageListsVerbs checks usage lists every wired verb with an example.
func TestRunUsageListsVerbs(t *testing.T) {
	var buf bytes.Buffer
	printUsage(&buf)
	out := buf.String()
	for _, want := range []string{
		"topic list",
		"topic info",
		"service call",
		"doctor",
		"params",
		"launch",
		"weld topic list",
		"weld topic info",
		"weld service call",
		"weld doctor",
		"weld params",
		"weld launch",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("usage misses %q:\n%s", want, out)
		}
	}
}

// TestRunVerbsDispatchHelp checks each verb dispatches and help exits 0.
func TestRunVerbsDispatchHelp(t *testing.T) {
	cases := [][]string{
		{"echo", "-h"},
		{"pub", "-h"},
		{"topic", "-h"},
		{"service", "-h"},
		{"doctor", "-h"},
		{"params", "-h"},
		{"launch", "-h"},
	}
	for _, args := range cases {
		code, stdout, _ := wiringRun(args)
		if code != 0 {
			t.Fatalf("args %q code = %d, want 0", args, code)
		}
		if !strings.Contains(stdout, "Usage:") {
			t.Fatalf("args %q stdout misses Usage: %q", args, stdout)
		}
	}
}

// TestRunUnknownSubcommand checks unknown verbs fail with usage and weld prefix.
func TestRunUnknownSubcommand(t *testing.T) {
	code, _, stderr := wiringRun([]string{"nope"})
	if code != 2 {
		t.Fatalf("unknown code = %d, want 2", code)
	}
	if !strings.Contains(stderr, "weld:") {
		t.Fatalf("unknown stderr misses weld: prefix: %q", stderr)
	}
}
