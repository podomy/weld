// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

// Package unit_launch pins weld launch argument acceptance with fixtures.
//
// The launch path builds node options in process through internal/launch
// instead of shelling out to the removed cmd/weld helper, then diffs the
// canonical dump section of the rendered output against expected files.
// No live ROS is needed.
package unit_launch

import (
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"sort"
	"strings"
	"testing"

	"github.com/podomy/weld/internal/core"
	"github.com/podomy/weld/internal/launch"
)

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

// launchFlags holds parsed flags left of --ros-args.
type launchFlags struct {
	// node is the base node name.
	node string
	// namespace is the node namespace.
	namespace string
	// paramsFiles lists params files in order.
	paramsFiles []string
	// sdf is the SDF file path.
	sdf string
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

// splitLaunchArgs splits args at the first --ros-args marker.
func splitLaunchArgs(args []string) ([]string, []string) {
	for i, a := range args {
		if a == "--ros-args" {
			return args[:i], args[i:]
		}
	}
	return args, []string{}
}

// parseLaunchFlags parses flags left of --ros-args.
func parseLaunchFlags(t *testing.T, left []string) launchFlags {
	t.Helper()
	var cfg launchFlags
	i := 0
	for i < len(left) {
		tok := left[i]
		next, ok := consumeLaunchFlag(t, &cfg, left, i, tok)
		if !ok {
			t.Fatalf("unknown launch flag %q", tok)
		}
		i = next
	}
	return cfg
}

// consumeLaunchFlag handles one left side flag at position i.
func consumeLaunchFlag(t *testing.T, cfg *launchFlags, left []string, i int, tok string) (int, bool) {
	t.Helper()
	if next, ok := consumeLaunchString(t, &cfg.node, left, i, tok, "node"); ok {
		return next, true
	}
	if next, ok := consumeLaunchString(t, &cfg.namespace, left, i, tok, "namespace"); ok {
		return next, true
	}
	if next, ok := consumeLaunchString(t, &cfg.sdf, left, i, tok, "sdf"); ok {
		return next, true
	}
	if next, ok := consumeLaunchRepeated(t, &cfg.paramsFiles, left, i, tok, "params-file"); ok {
		return next, true
	}
	return 0, false
}

// consumeLaunchString handles one --name value or --name=value flag.
func consumeLaunchString(t *testing.T, dst *string, left []string, i int, tok, name string) (int, bool) {
	t.Helper()
	if tok == "--"+name {
		if i+1 >= len(left) {
			t.Fatalf("launch flag --%s needs a value", name)
		}
		*dst = left[i+1]
		return i + 2, true
	}
	rest, ok := strings.CutPrefix(tok, "--"+name+"=")
	if ok {
		*dst = rest
		return i + 1, true
	}
	return 0, false
}

// consumeLaunchRepeated handles one repeatable --name value flag.
func consumeLaunchRepeated(t *testing.T, dst *[]string, left []string, i int, tok, name string) (int, bool) {
	t.Helper()
	if tok == "--"+name {
		if i+1 >= len(left) {
			t.Fatalf("launch flag --%s needs a value", name)
		}
		*dst = append(*dst, left[i+1])
		return i + 2, true
	}
	rest, ok := strings.CutPrefix(tok, "--"+name+"=")
	if ok {
		*dst = append(*dst, rest)
		return i + 1, true
	}
	return 0, false
}

// resolveLaunchParams loads YAML files and merges files plus CLI.
func resolveLaunchParams(t *testing.T, opts launch.NodeOptions) map[string]core.ParamValue {
	t.Helper()
	fqn, err := launch.ResolveNodeFQN(opts.NodeName, opts.Namespace)
	if err != nil {
		t.Fatalf("resolve node %q: %v", opts.NodeName, err)
	}
	var sections []map[string]map[string]core.ParamValue
	for _, path := range opts.ParamsFiles {
		loaded, err := launch.LoadParamsFile(path)
		if err != nil {
			t.Fatalf("load params file %q: %v", path, err)
		}
		sections = append(sections, loaded)
	}
	merged := launch.MergeFileSections(sections)
	resolved := launch.ResolveNodeParams(fqn, merged, opts.Params, nil)
	return resolved
}

// renderLaunchStdout runs the launch pipeline in process and renders stdout.
func renderLaunchStdout(t *testing.T, args []string) string {
	t.Helper()
	t.Setenv(launch.ROSLogDirEnv, "")
	left, right := splitLaunchArgs(args)
	cfg := parseLaunchFlags(t, left)
	base, err := launch.NewNodeOptions(cfg.node, cfg.namespace, cfg.node == "")
	if err != nil {
		t.Fatalf("launch options: %v", err)
	}
	base.ParamsFiles = append([]string{}, cfg.paramsFiles...)
	forwarded, err := launch.ForwardArgs(base, right)
	if err != nil {
		t.Fatalf("launch forward args: %v", err)
	}
	if strings.TrimSpace(forwarded.NodeName) == "" {
		t.Fatalf("weld: launch needs --node or -r __node:=name")
	}
	fqn, err := launch.ResolveNodeFQN(forwarded.NodeName, forwarded.Namespace)
	if err != nil {
		t.Fatalf("resolve node: %v", err)
	}
	resolved := resolveLaunchParams(t, forwarded)
	final, err := launch.RewriteParamsForNamespace(resolved, forwarded.Namespace, map[string]string{}, false)
	if err != nil {
		t.Fatalf("rewrite params: %v", err)
	}
	sdfPath := strings.TrimSpace(cfg.sdf)
	if sdfPath != "" {
		validated, err := launch.AppendSDFParams(forwarded, sdfPath)
		if err != nil {
			t.Fatalf("append sdf params: %v", err)
		}
		final[launch.SimTimeParam] = validated.Params[launch.SimTimeParam]
	}
	dump, err := launch.CanonicalDump(fqn, final)
	if err != nil {
		t.Fatalf("canonical dump: %v", err)
	}
	return formatLaunchStdout(fqn, forwarded.Remaps, dump, sdfPath)
}

// formatLaunchStdout renders node, remaps, dump, SDF note, and log dir.
func formatLaunchStdout(fqn string, remaps []launch.RemapRule, dump, sdfPath string) string {
	var b strings.Builder
	b.WriteString("node: " + fqn + "\n")                 //nolint:errcheck // strings.Builder writes never fail
	b.WriteString("remaps: " + itoa(len(remaps)) + "\n") //nolint:errcheck // strings.Builder writes never fail
	sorted := append([]launch.RemapRule{}, remaps...)
	sort.Slice(sorted, func(i, j int) bool {
		if sorted[i].From == sorted[j].From {
			return sorted[i].To < sorted[j].To
		}
		return sorted[i].From < sorted[j].From
	})
	for _, r := range sorted {
		b.WriteString("  " + r.From + " -> " + r.To + "\n") //nolint:errcheck // strings.Builder writes never fail
	}
	b.WriteString(dump) //nolint:errcheck // strings.Builder writes never fail
	if strings.TrimSpace(sdfPath) != "" {
		b.WriteString("sdf: " + strings.TrimSpace(sdfPath) + "\n") //nolint:errcheck // strings.Builder writes never fail
	}
	dir, ok := launch.ResolveLogDir()
	if ok {
		b.WriteString("log_dir: " + dir + "\n") //nolint:errcheck // strings.Builder writes never fail
	}
	return b.String()
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
			stdout := renderLaunchStdout(t, tc.args)
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
