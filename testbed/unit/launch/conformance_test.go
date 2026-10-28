// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

// Package unit_launch pins weld launch argument acceptance with fixtures.
//
// Runner gap: the launch path helper runLaunchCommand lives in package
// main of the separate cmd-weld module, so this package cannot import it
// without product changes. The test shells out to go run on cmd/weld
// with GOWORK=off, following cmd/weld/go.mod, then diffs the canonical
// dump section of stdout against expected files. No live ROS is needed.
package unit_launch

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"sort"
	"strings"
	"testing"
	"time"
)

// launchTimeout bounds one weld launch shell out including compilation.
const launchTimeout = 2 * time.Minute

// launchCase describes one weld launch invocation plus expectations.
type launchCase struct {
	// name is the subtest name.
	name string
	// args are argv after weld launch, with fixture paths resolved.
	args []string
	// node is the expected fully qualified node name.
	node string
	// remaps holds expected sorted remap lines without indent.
	remaps []string
	// sdf is the expected sdf path line value, empty when no SDF flag.
	sdf string
	// expected names the expected canonical dump fixture file.
	expected string
}

// corpusDir returns the directory holding this test plus fixtures.
func corpusDir(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatalf("runtime caller unavailable")
	}
	return filepath.Dir(file)
}

// repoRoot returns the repository root holding cmd/weld.
func repoRoot(t *testing.T) string {
	t.Helper()
	dir := corpusDir(t)
	for {
		info, err := os.Stat(filepath.Join(dir, "cmd", "weld"))
		if err == nil && info.IsDir() {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatalf("repo root with cmd/weld not found above %s", dir)
		}
		dir = parent
	}
}

// runWeldLaunch shells out to the existing weld launch command.
func runWeldLaunch(t *testing.T, ctx context.Context, args []string) string {
	t.Helper()
	root := repoRoot(t)
	argv := append([]string{"run", ".", "launch"}, args...)
	cmd := exec.CommandContext(ctx, "go", argv...) //nolint:gosec // G204: test harness runs fixed go run argv from code, no user input
	cmd.Dir = filepath.Join(root, "cmd", "weld")
	env := os.Environ()
	env = append(env, "GOWORK=off", "ROS_LOG_DIR=")
	cmd.Env = env
	out, err := cmd.Output()
	if err != nil {
		var exit *exec.ExitError
		if errors.As(err, &exit) {
			t.Fatalf("weld launch %v: %v stderr %q", args, err, string(exit.Stderr))
		}
		t.Fatalf("weld launch %v: %v", args, err)
	}
	return string(out)
}

// checkHeader asserts node plus remap plus sdf lines in stdout.
func checkHeader(t *testing.T, stdout string, tc launchCase) {
	t.Helper()
	lines := strings.Split(stdout, "\n")
	checkHeaderNode(t, stdout, lines, tc)
	checkHeaderRemaps(t, lines, tc)
	checkHeaderSDF(t, stdout, lines, tc)
	checkHeaderNoLogDir(t, stdout, lines)
}

// checkHeaderNode asserts the node line is first.
func checkHeaderNode(t *testing.T, stdout string, lines []string, tc launchCase) {
	t.Helper()
	if len(lines) < 2 {
		t.Fatalf("stdout too short:\n%s", stdout)
	}
	if lines[0] != "node: "+tc.node {
		t.Fatalf("node line = %q, want %q", lines[0], "node: "+tc.node)
	}
}

// checkHeaderRemaps asserts the remap count line plus each remap line.
func checkHeaderRemaps(t *testing.T, lines []string, tc launchCase) {
	t.Helper()
	wantRemaps := "remaps: " + itoa(len(tc.remaps))
	if lines[1] != wantRemaps {
		t.Fatalf("remaps line = %q, want %q", lines[1], wantRemaps)
	}
	for i, want := range tc.remaps {
		got := ""
		if 2+i < len(lines) {
			got = lines[2+i]
		}
		if got != "  "+want {
			t.Fatalf("remap %d = %q, want %q", i, got, "  "+want)
		}
	}
}

// checkHeaderSDF asserts the sdf line is present when the case sets one.
func checkHeaderSDF(t *testing.T, stdout string, lines []string, tc launchCase) {
	t.Helper()
	if tc.sdf == "" {
		return
	}
	want := "sdf: " + tc.sdf
	if !slices.Contains(lines, want) {
		t.Fatalf("stdout misses %q:\n%s", want, stdout)
	}
}

// checkHeaderNoLogDir asserts stdout carries no log_dir with blank ROS_LOG_DIR.
func checkHeaderNoLogDir(t *testing.T, stdout string, lines []string) {
	t.Helper()
	for _, line := range lines {
		if strings.HasPrefix(line, "log_dir: ") {
			t.Fatalf("stdout must not carry log_dir with blank ROS_LOG_DIR:\n%s", stdout)
		}
	}
}

// dumpSection extracts the canonical dump block for node from stdout.
func dumpSection(t *testing.T, stdout, node string) string {
	t.Helper()
	lines := strings.Split(stdout, "\n")
	start := -1
	for i, line := range lines {
		if line == node+":" {
			start = i
			break
		}
	}
	if start < 0 {
		t.Fatalf("stdout misses dump header %q:\n%s", node+":", stdout)
	}
	end := len(lines)
	for i := start; i < len(lines); i++ {
		if strings.HasPrefix(lines[i], "sdf: ") || strings.HasPrefix(lines[i], "log_dir: ") {
			end = i
			break
		}
	}
	section := strings.Join(lines[start:end], "\n")
	section = strings.TrimRight(section, "\n") + "\n"
	return section
}

// checkSortedKeys asserts dump param keys are sorted.
func checkSortedKeys(t *testing.T, dump string) {
	t.Helper()
	lines := strings.Split(dump, "\n")
	var keys []string
	for _, line := range lines {
		if !strings.HasPrefix(line, "    ") {
			continue
		}
		trimmed := strings.TrimSpace(line)
		if trimmed == "" || trimmed == "{}" {
			continue
		}
		name, _, ok := strings.Cut(trimmed, ":")
		if !ok {
			t.Fatalf("dump line %q misses colon", line)
		}
		keys = append(keys, strings.TrimSpace(name))
	}
	if !sort.StringsAreSorted(keys) {
		t.Fatalf("dump keys %v are not sorted", keys)
	}
}

// itoa renders a small non-negative integer without imports.
func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var digits []byte
	for n > 0 {
		digits = append([]byte{byte('0' + n%10)}, digits...)
		n /= 10
	}
	return string(digits)
}

// TestLaunchCorpusAcceptance runs weld launch over fixtures and diffs dumps.
func TestLaunchCorpusAcceptance(t *testing.T) {
	dir := corpusDir(t)
	remapParams := filepath.Join(dir, "node_remap.yaml")
	sharedParams := filepath.Join(dir, "shared_nodes.yaml")
	sdfPath := filepath.Join(dir, "sim_robot.sdf")
	cases := []launchCase{
		{
			name:     "remap plus namespaced params",
			args:     []string{"--node", "talker", "--namespace", "/ns", "--params-file", remapParams, "--ros-args", "-r", "chatter:=/out", "-r", "/old:=/new", "-p", "speed:=2.0"},
			node:     "/ns/talker",
			remaps:   []string{"/old -> /new", "chatter -> /out"},
			expected: "node_remap.expected",
		},
		{
			name:     "shared file nav override",
			args:     []string{"--node", "nav", "--namespace", "/r1", "--params-file", sharedParams, "--ros-args", "-p", "speed:=2.0"},
			node:     "/r1/nav",
			expected: "shared_nav.expected",
		},
		{
			name:     "shared file mapper default",
			args:     []string{"--node", "mapper", "--namespace", "/r1", "--params-file", sharedParams},
			node:     "/r1/mapper",
			expected: "shared_mapper.expected",
		},
		{
			name:     "sdf forces sim time",
			args:     []string{"--node", "sim", "--sdf", sdfPath, "--ros-args", "-p", "use_sim_time:=false", "-p", "rate:=10"},
			node:     "/sim",
			sdf:      sdfPath,
			expected: "sim.expected",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), launchTimeout)
			defer cancel()
			stdout := runWeldLaunch(t, ctx, tc.args)
			checkHeader(t, stdout, tc)
			got := dumpSection(t, stdout, tc.node)
			raw, err := os.ReadFile(filepath.Join(dir, tc.expected)) //nolint:gosec // G304: test reads fixture from its own dir by fixed case name
			if err != nil {
				t.Fatalf("read expected %s: %v", tc.expected, err)
			}
			want := string(raw)
			if got != want {
				t.Fatalf("dump mismatch for %s:\n got:\n%s want:\n%s", tc.node, got, want)
			}
			checkSortedKeys(t, got)
		})
	}
}
