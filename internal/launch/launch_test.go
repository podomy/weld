// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

//nolint:cyclop // launch tests hold many sequential harness assertions, high average reflects breadth
package launch

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/podomy/weld/internal/core"
)

// TestParseROSArgsOrder checks flags parse in any order with last wins.
//
//nolint:cyclop // order test pins many sequential flag shapes in one run, splitting would obscure intent
func TestParseROSArgsOrder(t *testing.T) {
	argv := []string{
		"--ros-args", "-p", "speed:=1", "-r", "old:=/new",
		"-p", "speed:=2", "-r", "__node:=talker", "--params-file", "a.yaml",
		"-r", "__ns:=/ns", "--params-file=extra.yaml",
	}
	got, err := ParseROSArgs(argv)
	if err != nil {
		t.Fatalf("ParseROSArgs: %v", err)
	}
	if !got.HasNodeName || got.NodeName != "talker" {
		t.Fatalf("node = %q %v, want talker true", got.NodeName, got.HasNodeName)
	}
	if !got.HasNamespace || got.Namespace != "/ns" {
		t.Fatalf("ns = %q %v, want /ns true", got.Namespace, got.HasNamespace)
	}
	speed, ok := got.Params["speed"]
	if !ok {
		t.Fatal("speed override missing")
	}
	if speed.Type != core.ParamTypeInteger || speed.Integer != 2 {
		t.Fatalf("speed last wins = %+v, want integer 2", speed)
	}
	if len(got.ParamsFiles) != 2 || got.ParamsFiles[0] != "a.yaml" {
		t.Fatalf("params files = %v, want [a.yaml extra.yaml]", got.ParamsFiles)
	}
	if len(got.Remaps) != 1 || got.Remaps[0].From != "old" {
		t.Fatalf("remaps = %+v, want one old rule", got.Remaps)
	}
}

// TestParseROSArgsTerminator checks -- ends ROS parsing.
func TestParseROSArgsTerminator(t *testing.T) {
	got, err := ParseROSArgs([]string{"--ros-args", "-p", "a:=1", "--", "-p", "b:=2"})
	if err != nil {
		t.Fatalf("ParseROSArgs: %v", err)
	}
	if _, ok := got.Params["a"]; !ok {
		t.Fatal("a must be parsed")
	}
	if _, ok := got.Params["b"]; ok {
		t.Fatal("b after -- must be ignored")
	}
}

// TestParseROSArgsErrors checks loud failures for bad tokens.
func TestParseROSArgsErrors(t *testing.T) {
	cases := [][]string{
		{"--ros-args", "-r"},
		{"--ros-args", "-p"},
		{"--ros-args", "--params-file"},
		{"--ros-args", "-r", "no-sep"},
		{"--ros-args", "-p", "no-sep"},
		{"--ros-args", "--unknown", "x"},
		{"--ros-args", "stray"},
		{"--ros-args", "-r", "__node:=bad-name!"},
		{"--ros-args", "-r", "__other:=x"},
	}
	for i, argv := range cases {
		_, err := ParseROSArgs(argv)
		if err == nil {
			t.Fatalf("case %d %v must fail", i, argv)
		}
		if !strings.HasPrefix(err.Error(), "launch:") {
			t.Fatalf("case %d error = %q, want launch: prefix", i, err.Error())
		}
	}
}

// TestParseParamDict checks dict literals flatten with dots.
func TestParseParamDict(t *testing.T) {
	got, err := ParseROSArgs([]string{"--ros-args", "-p", "ctrl:={p: 1, d: 2.5}"})
	if err != nil {
		t.Fatalf("ParseROSArgs: %v", err)
	}
	p, ok := got.Params["ctrl.p"]
	if !ok || p.Type != core.ParamTypeInteger || p.Integer != 1 {
		t.Fatalf("ctrl.p = %+v %v, want integer 1", p, ok)
	}
	d, ok := got.Params["ctrl.d"]
	if !ok || d.Type != core.ParamTypeDouble {
		t.Fatalf("ctrl.d = %+v %v, want double", d, ok)
	}
}

// TestForwardArgsAppends checks CLI argv appends with CLI winning.
func TestForwardArgsAppends(t *testing.T) {
	base, err := NewNodeOptions("base", "/a", false)
	if err != nil {
		t.Fatalf("NewNodeOptions: %v", err)
	}
	base.Params["speed"] = core.NewIntegerParam(1)
	base.Remaps = []RemapRule{{From: "/old", To: "/base"}}
	base.ParamsFiles = []string{"base.yaml"}
	got, err := ForwardArgs(base, []string{"--ros-args", "-p", "speed:=9", "-r", "/old:=/cli", "--params-file", "cli.yaml"})
	if err != nil {
		t.Fatalf("ForwardArgs: %v", err)
	}
	if got.Params["speed"].Integer != 9 {
		t.Fatalf("CLI must win speed, have %+v", got.Params["speed"])
	}
	if len(got.Remaps) != 1 || got.Remaps[0].To != "/cli" {
		t.Fatalf("CLI remap must win, have %+v", got.Remaps)
	}
	if len(got.ParamsFiles) != 2 || got.ParamsFiles[1] != "cli.yaml" {
		t.Fatalf("files append, have %v", got.ParamsFiles)
	}
	if base.Params["speed"].Integer != 1 {
		t.Fatal("base must not be mutated")
	}
}

// TestNamespaces checks absolute resolution and push.
func TestNamespaces(t *testing.T) {
	fqn, err := ResolveNodeFQN("talker", "ns/sub")
	if err != nil {
		t.Fatalf("ResolveNodeFQN: %v", err)
	}
	if fqn != "/ns/sub/talker" {
		t.Fatalf("fqn = %q, want /ns/sub/talker", fqn)
	}
	root, err := ResolveNodeFQN("talker", "")
	if err != nil {
		t.Fatalf("ResolveNodeFQN root: %v", err)
	}
	if root != "/talker" {
		t.Fatalf("root fqn = %q, want /talker", root)
	}
	pushed, err := ApplyNamespacePush("/a", "b")
	if err != nil {
		t.Fatalf("ApplyNamespacePush: %v", err)
	}
	if pushed != "/a/b" {
		t.Fatalf("pushed = %q, want /a/b", pushed)
	}
	replaced, err := ApplyNamespacePush("/a", "/c")
	if err != nil {
		t.Fatalf("ApplyNamespacePush replace: %v", err)
	}
	if replaced != "/c" {
		t.Fatalf("replaced = %q, want /c", replaced)
	}
	if _, err := ResolveNodeFQN("bad name", "/ns"); err == nil {
		t.Fatal("bad node name must fail")
	}
	if _, err := NormalizeNamespace("/bad segment"); err == nil {
		t.Fatal("bad namespace must fail")
	}
}

// TestRemapFirstMatch checks global plus local first match wins.
func TestRemapFirstMatch(t *testing.T) {
	global := []RemapRule{{From: "/a", To: "/g"}, {From: "/b", To: "/g2"}}
	local := []RemapRule{{From: "/a", To: "/l"}, {From: "/c", To: "/l3"}}
	got, err := ResolveRemap("/a", global, local)
	if err != nil {
		t.Fatalf("ResolveRemap: %v", err)
	}
	if got != "/g" {
		t.Fatalf("global first match = %q, want /g", got)
	}
	got, err = ResolveRemap("/c", global, local)
	if err != nil {
		t.Fatalf("ResolveRemap: %v", err)
	}
	if got != "/l3" {
		t.Fatalf("local match = %q, want /l3", got)
	}
	got, err = ResolveRemap("/missing", global, local)
	if err != nil {
		t.Fatalf("ResolveRemap: %v", err)
	}
	if got != "/missing" {
		t.Fatalf("missing passes through, have %q", got)
	}
	err = ValidateRemapRule(RemapRule{From: "", To: "/x"})
	if err == nil {
		t.Fatal("empty from must fail")
	}
}

// TestRewriteParams checks namespace rewrites with autostart and zones.
func TestRewriteParams(t *testing.T) {
	base := map[string]core.ParamValue{
		"speed":     core.NewDoubleParam(1.0),
		"autostart": core.NewBoolParam(false),
	}
	rewrites := map[string]string{
		"speed":              "2.5",
		"zone.clear_costmap": "true",
	}
	got, err := RewriteParamsForNamespace(base, "/ns", rewrites, true)
	if err != nil {
		t.Fatalf("RewriteParamsForNamespace: %v", err)
	}
	if got["speed"].Double != 2.5 {
		t.Fatalf("speed rewrite = %+v", got["speed"])
	}
	if !got["zone.clear_costmap"].Bool {
		t.Fatalf("zone rewrite = %+v", got["zone.clear_costmap"])
	}
	if !got[AutostartParam].Bool {
		t.Fatalf("autostart forced = %+v", got[AutostartParam])
	}
	if base["speed"].Double != 1.0 {
		t.Fatal("base must not be mutated")
	}
	if _, err := RewriteParamsForNamespace(base, "/bad segment", nil, false); err == nil {
		t.Fatal("bad namespace must fail")
	}
}

// TestAppendSDF checks SDF appends with sim time forced on.
func TestAppendSDF(t *testing.T) {
	base, err := NewNodeOptions("sim", "/", false)
	if err != nil {
		t.Fatalf("NewNodeOptions: %v", err)
	}
	base.Params[SimTimeParam] = core.NewBoolParam(false)
	got, err := AppendSDFParams(base, "robot.sdf")
	if err != nil {
		t.Fatalf("AppendSDFParams: %v", err)
	}
	if len(got.ParamsFiles) != 1 || got.ParamsFiles[0] != "robot.sdf" {
		t.Fatalf("sdf files = %v", got.ParamsFiles)
	}
	if !got.Params[SimTimeParam].Bool {
		t.Fatal("sim time must be forced true")
	}
	if _, err := AppendSDFParams(base, "  "); err == nil {
		t.Fatal("empty sdf path must fail")
	}
}

// TestAutoDeclare checks undeclared collection and merge.
func TestAutoDeclare(t *testing.T) {
	declared := map[string]core.ParamValue{"known": core.NewIntegerParam(1)}
	overrides := map[string]core.ParamValue{
		"known": core.NewIntegerParam(2),
		"extra": core.NewStringParam("hi"),
	}
	missing := UndeclaredParams(declared, overrides)
	if len(missing) != 1 || missing[0] != "extra" {
		t.Fatalf("undeclared = %v, want [extra]", missing)
	}
	merged := AutoDeclareUndeclared(declared, overrides)
	if merged["known"].Integer != 1 {
		t.Fatalf("declared keeps value, have %+v", merged["known"])
	}
	if merged["extra"].String != "hi" {
		t.Fatalf("extra auto declared, have %+v", merged["extra"])
	}
}

// TestParamsFile checks flattening, substitution, and collapse.
func TestParamsFile(t *testing.T) {
	t.Setenv("WELD_TEST_SPEED", "3.5")
	content := "my_node:\n" +
		"  ros__parameters:\n" +
		"    ctrl:\n" +
		"      p: 1\n" +
		"      d: $(env WELD_TEST_SPEED 1.0)\n" +
		"    tags: [a, b]\n" +
		"/**:\n" +
		"  ros__parameters:\n" +
		"    use_sim_time: true\n" +
		"    speed: 1.0\n"
	sections, err := ParseParamsContent([]byte(content))
	if err != nil {
		t.Fatalf("ParseParamsContent: %v", err)
	}
	flat, ok := sections["my_node"]
	if !ok {
		t.Fatalf("sections keys = %v", sections)
	}
	if _, ok := flat["ctrl.p"]; !ok {
		t.Fatalf("dotted flatten missing, have %v", flat)
	}
	cli := map[string]core.ParamValue{"speed": core.NewDoubleParam(9.0)}
	resolved := ResolveNodeParams("/my_node", sections, cli, nil)
	if resolved["speed"].Double != 9.0 {
		t.Fatalf("CLI beats file, have %+v", resolved["speed"])
	}
	if !resolved["use_sim_time"].Bool {
		t.Fatalf("wildcard applies, have %+v", resolved)
	}
	if resolved["ctrl.d"].Double != 3.5 {
		t.Fatalf("env substitution = %+v", resolved["ctrl.d"])
	}
	if _, ok := resolved["tags"]; !ok {
		t.Fatal("array param missing")
	}
}

// TestParamsFileUniformTyping checks mixed arrays fail.
func TestParamsFileUniformTyping(t *testing.T) {
	content := "n:\n  ros__parameters:\n    mixed: [1, hello]\n"
	_, err := ParseParamsContent([]byte(content))
	if err == nil {
		t.Fatal("mixed array typing must fail")
	}
	if !strings.HasPrefix(err.Error(), "launch:") {
		t.Fatalf("error = %q, want launch: prefix", err.Error())
	}
}

// TestLoadParamsFileRoundTrip checks file IO plus startup batching.
func TestLoadParamsFileRoundTrip(t *testing.T) {
	content := "talker:\n  ros__parameters:\n    text: hello\n"
	path := filepath.Join(t.TempDir(), "params.yaml")
	err := os.WriteFile(path, []byte(content), 0o600)
	if err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	sections, err := LoadParamsFile(path)
	if err != nil {
		t.Fatalf("LoadParamsFile: %v", err)
	}
	resolved := ResolveNodeParams("/talker", sections, nil, nil)
	if resolved["text"].String != "hello" {
		t.Fatalf("text = %+v", resolved["text"])
	}
	store, err := core.NewConfigStore(core.ConfigStoreOptions{AllowUndeclared: false})
	if err != nil {
		t.Fatalf("NewConfigStore: %v", err)
	}
	event, err := store.ApplyStartup(nil, resolved, nil)
	if err != nil {
		t.Fatalf("ApplyStartup: %v", err)
	}
	if event.Source != "startup" {
		t.Fatalf("source = %q, want startup", event.Source)
	}
	if len(event.Changes) != 1 {
		t.Fatalf("one batched event must carry 1 change, have %d", len(event.Changes))
	}
	if _, err := LoadParamsFile(""); err == nil {
		t.Fatal("empty path must fail")
	}
}

// TestCanonicalDump checks dump form with sorted keys.
func TestCanonicalDump(t *testing.T) {
	params := map[string]core.ParamValue{
		"speed": core.NewDoubleParam(2.5),
		"name":  core.NewStringParam("hi"),
		"flag":  core.NewBoolParam(true),
	}
	dump, err := CanonicalDump("/ns/talker", params)
	if err != nil {
		t.Fatalf("CanonicalDump: %v", err)
	}
	if !strings.Contains(dump, "/ns/talker:\n") {
		t.Fatalf("dump misses node header:\n%s", dump)
	}
	if !strings.Contains(dump, "ros__parameters:\n") {
		t.Fatalf("dump misses ros__parameters:\n%s", dump)
	}
	flagIdx := strings.Index(dump, "flag:")
	nameIdx := strings.Index(dump, "name:")
	speedIdx := strings.Index(dump, "speed:")
	if flagIdx >= nameIdx || nameIdx >= speedIdx {
		t.Fatalf("dump keys not sorted:\n%s", dump)
	}
	if _, err := CanonicalDump("", params); err == nil {
		t.Fatal("empty node must fail")
	}
}

// TestPlanFQNs checks per node namespaces and duplicates.
func TestPlanFQNs(t *testing.T) {
	a, err := NewNodeOptions("a", "/x", false)
	if err != nil {
		t.Fatalf("NewNodeOptions: %v", err)
	}
	b, err := NewNodeOptions("b", "/y", false)
	if err != nil {
		t.Fatalf("NewNodeOptions: %v", err)
	}
	plan := LaunchPlan{Nodes: []NodeOptions{a, b}}
	fqns, err := ResolvePlanFQNs(plan)
	if err != nil {
		t.Fatalf("ResolvePlanFQNs: %v", err)
	}
	if len(fqns) != 2 || fqns[0] != "/x/a" || fqns[1] != "/y/b" {
		t.Fatalf("fqns = %v", fqns)
	}
	dup := LaunchPlan{Nodes: []NodeOptions{a, a}}
	if _, err := ResolvePlanFQNs(dup); err == nil {
		t.Fatal("duplicate FQN must fail")
	}
}

// TestLogDir checks ROS_LOG_DIR handling.
func TestLogDir(t *testing.T) {
	t.Setenv(ROSLogDirEnv, "/tmp/weld-logs")
	dir, ok := ResolveLogDir()
	if !ok || dir != "/tmp/weld-logs" {
		t.Fatalf("log dir = %q %v", dir, ok)
	}
	t.Setenv(ROSLogDirEnv, "   ")
	if _, ok := ResolveLogDir(); ok {
		t.Fatal("blank log dir must report unset")
	}
}
