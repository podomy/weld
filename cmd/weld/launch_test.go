// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package main

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestRunLaunchForwarding checks CLI argv appends with last wins.
func TestRunLaunchForwarding(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := runLaunchCommand(context.Background(), []string{
		"--node", "base", "--namespace", "/a",
		"--ros-args", "-p", "speed:=1", "-p", "speed:=2", "-r", "/old:=/new",
	}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("code = %d, stderr = %q", code, stderr.String())
	}
	out := stdout.String()
	if !strings.Contains(out, "node: /a/base") {
		t.Fatalf("output misses FQN:\n%s", out)
	}
	if !strings.Contains(out, "/old -> /new") {
		t.Fatalf("output misses remap:\n%s", out)
	}
	if !strings.Contains(out, "speed: 2") {
		t.Fatalf("output misses last wins speed:\n%s", out)
	}
}

// TestRunLaunchNamespaces checks __ns absolute resolution and push.
func TestRunLaunchNamespaces(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := runLaunchCommand(context.Background(), []string{
		"--node", "talker", "--ros-args", "-r", "__ns:=ns/sub",
	}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("code = %d, stderr = %q", code, stderr.String())
	}
	if !strings.Contains(stdout.String(), "node: /ns/sub/talker") {
		t.Fatalf("absolute ns failed:\n%s", stdout.String())
	}
}

// TestRunLaunchRewrite checks per namespace rewrites with autostart.
func TestRunLaunchRewrite(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := runLaunchCommand(context.Background(), []string{
		"--node", "nav", "--namespace", "/r1", "--rewrite", "speed:=2.5",
		"--rewrite", "zone.clear:=true", "--autostart",
		"--ros-args", "-p", "speed:=1.0",
	}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("code = %d, stderr = %q", code, stderr.String())
	}
	out := stdout.String()
	if !strings.Contains(out, "speed: 2.5") {
		t.Fatalf("rewrite must win:\n%s", out)
	}
	if !strings.Contains(out, "autostart: true") {
		t.Fatalf("autostart forced:\n%s", out)
	}
	if !strings.Contains(out, "zone.clear: true") {
		t.Fatalf("zone rewrite missing:\n%s", out)
	}
}

// TestRunLaunchSDF checks SDF appends with sim time forced on.
func TestRunLaunchSDF(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := runLaunchCommand(context.Background(), []string{
		"--node", "sim", "--sdf", "robot.sdf",
		"--ros-args", "-p", "use_sim_time:=false",
	}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("code = %d, stderr = %q", code, stderr.String())
	}
	out := stdout.String()
	if !strings.Contains(out, "use_sim_time: true") {
		t.Fatalf("sim time forced on:\n%s", out)
	}
	if !strings.Contains(out, "sdf: robot.sdf") {
		t.Fatalf("sdf path missing:\n%s", out)
	}
}

// TestRunLaunchParamsFile checks rewritten YAML per namespace from files.
func TestRunLaunchParamsFile(t *testing.T) {
	content := "nav:\n  ros__parameters:\n    speed: 1.0\n/**:\n  ros__parameters:\n    use_sim_time: true\n"
	path := filepath.Join(t.TempDir(), "nav.yaml")
	err := os.WriteFile(path, []byte(content), 0o600)
	if err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	var stdout, stderr bytes.Buffer
	code := runLaunchCommand(context.Background(), []string{
		"--node", "nav", "--params-file", path,
	}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("code = %d, stderr = %q", code, stderr.String())
	}
	out := stdout.String()
	if !strings.Contains(out, "ros__parameters:") {
		t.Fatalf("dump form missing:\n%s", out)
	}
	if !strings.Contains(out, "use_sim_time: true") {
		t.Fatalf("wildcard file param missing:\n%s", out)
	}
}

// TestRunLaunchUsage checks missing node and help paths.
func TestRunLaunchUsage(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := runLaunchCommand(context.Background(), []string{}, &stdout, &stderr)
	if code != 2 {
		t.Fatalf("missing node code = %d, want 2", code)
	}
	var helpOut bytes.Buffer
	code = runLaunchCommand(context.Background(), []string{"--help"}, &helpOut, &stderr)
	if code != 0 {
		t.Fatalf("help code = %d, want 0", code)
	}
	if !strings.Contains(helpOut.String(), "Usage: weld launch") {
		t.Fatalf("help missing usage:\n%s", helpOut.String())
	}
	cancelled, stop := context.WithCancel(context.Background())
	stop()
	var cancelledOut bytes.Buffer
	var cancelledErr bytes.Buffer
	code = runLaunchCommand(cancelled, []string{"--node", "x"}, &cancelledOut, &cancelledErr)
	if code != 1 {
		t.Fatalf("cancelled code = %d, want 1", code)
	}
	if cancelledOut.Len() != 0 {
		t.Fatalf("cancelled must print nothing to stdout, have %q", cancelledOut.String())
	}
}
