// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package core

import (
	"errors"
	"strings"
	"sync"
	"testing"
)

// TestConfig covers the versioned store: declare, get, set, describe,
// list, events, overrides, YAML schema validation, remap, and versioned
// watches with history.
func TestConfig(t *testing.T) {
	t.Run("declare get describe", checkDeclareGetDescribe)
	t.Run("declare duplicate", checkDeclareDuplicate)
	t.Run("declare validation", checkDeclareValidation)
	t.Run("get missing", checkGetMissing)
	t.Run("set version event", checkSetVersionEvent)
	t.Run("set type mismatch", checkSetTypeMismatch)
	t.Run("set read only", checkSetReadOnly)
	t.Run("set undeclared", checkSetUndeclared)
	t.Run("atomic all or nothing", checkAtomicAllOrNothing)
	t.Run("validator splits from apply", checkValidatorSplitsFromApply)
	t.Run("apply runs on success", checkApplyRunsOnSuccess)
	t.Run("startup batching precedence", checkStartupBatchingPrecedence)
	t.Run("startup empty", checkStartupEmpty)
	t.Run("wildcard fqn precedence", checkWildcardFQNPrecedence)
	t.Run("flatten dotted", checkFlattenDotted)
	t.Run("uniform array typing", checkUniformArrayTyping)
	t.Run("override literal", checkOverrideLiteral)
	t.Run("override parse", checkOverrideParse)
	t.Run("overrides auto declare", checkOverridesAutoDeclare)
	t.Run("remap pure first match", checkRemapPureFirstMatch)
	t.Run("remap at creation", checkRemapAtCreation)
	t.Run("remap invalid creation", checkRemapInvalidCreation)
	t.Run("remap fixed", checkRemapFixed)
	t.Run("watch versioned history", checkWatchVersionedHistory)
	t.Run("describe list dump", checkDescribeListDump)
	t.Run("schema validation", checkSchemaValidation)
	t.Run("dump load round trip", checkDumpLoadRoundTrip)
	t.Run("node scoped expansion", checkNodeScopedExpansion)
	t.Run("name validation", checkNameValidation)
	t.Run("concurrent sets", checkConcurrentSets)
}

// mustStore returns a store or fails the test.
func mustStore(t *testing.T, opts ConfigStoreOptions) *ConfigStore {
	t.Helper()
	store, err := NewConfigStore(opts)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	return store
}

// mustDeclare declares or fails the test.
func mustDeclare(t *testing.T, s *ConfigStore, name string, v ParamValue, d ParamDescriptor) {
	t.Helper()
	err := s.Declare(name, v, d)
	if err != nil {
		t.Fatalf("declare %q: %v", name, err)
	}
}

// checkDeclareGetDescribe pins declare then get and describe round trip.
func checkDeclareGetDescribe(t *testing.T) {
	t.Helper()
	s := mustStore(t, ConfigStoreOptions{})
	mustDeclare(t, s, "max_vel_x", NewDoubleParam(1.5), ParamDescriptor{Type: ParamTypeDouble, Description: "meters per second"})
	got, err := s.Get("max_vel_x")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if !got.Equal(NewDoubleParam(1.5)) {
		t.Fatalf("get mismatch: %+v", got)
	}
	desc, err := s.Describe("max_vel_x")
	if err != nil {
		t.Fatalf("describe: %v", err)
	}
	if desc.Type != ParamTypeDouble || desc.Description != "meters per second" {
		t.Fatalf("describe mismatch: %+v", desc)
	}
	if got := s.Version(); got != 1 {
		t.Fatalf("version must be 1 after declare, got %d", got)
	}
}

// checkDeclareDuplicate pins re-declare fails without moving version.
func checkDeclareDuplicate(t *testing.T) {
	t.Helper()
	s := mustStore(t, ConfigStoreOptions{})
	mustDeclare(t, s, "gain", NewDoubleParam(1.0), ParamDescriptor{Type: ParamTypeDouble})
	before := s.Version()
	err := s.Declare("gain", NewDoubleParam(2.0), ParamDescriptor{Type: ParamTypeDouble})
	if err == nil {
		t.Fatal("re-declare succeeded, want error")
	}
	if got := s.Version(); got != before {
		t.Fatalf("failed declare moved version to %d", got)
	}
}

// checkDeclareValidation pins wrong type and out of range fail declare.
func checkDeclareValidation(t *testing.T) {
	t.Helper()
	s := mustStore(t, ConfigStoreOptions{})
	err := s.Declare("speed", NewStringParam("fast"), ParamDescriptor{Type: ParamTypeDouble})
	if err == nil {
		t.Fatal("wrong type declare succeeded, want error")
	}
	desc := ParamDescriptor{Type: ParamTypeInteger, HasIntegerRange: true, IntegerMin: 0, IntegerMax: 10}
	err = s.Declare("count", NewIntegerParam(99), desc)
	if err == nil {
		t.Fatal("out of range declare succeeded, want error")
	}
	if got := s.Version(); got != 0 {
		t.Fatalf("failed declares must not move version, got %d", got)
	}
}

// checkGetMissing pins unknown names fail get and describe.
func checkGetMissing(t *testing.T) {
	t.Helper()
	s := mustStore(t, ConfigStoreOptions{})
	_, err := s.Get("nope")
	if err == nil {
		t.Fatal("get missing succeeded, want error")
	}
	_, err = s.Describe("nope")
	if err == nil {
		t.Fatal("describe missing succeeded, want error")
	}
}

// checkSetVersionEvent pins set bumps version and emits one event.
func checkSetVersionEvent(t *testing.T) {
	t.Helper()
	s := mustStore(t, ConfigStoreOptions{})
	mustDeclare(t, s, "gain", NewDoubleParam(1.0), ParamDescriptor{Type: ParamTypeDouble})
	_, ch, cancel := s.Watch()
	defer cancel()
	before := s.Version()
	err := s.Set("gain", NewDoubleParam(2.0))
	if err != nil {
		t.Fatalf("set: %v", err)
	}
	if got := s.Version(); got != before+1 {
		t.Fatalf("set must bump version by 1, got %d want %d", got, before+1)
	}
	got, err := s.Get("gain")
	if err != nil {
		t.Fatalf("get after set: %v", err)
	}
	if !got.Equal(NewDoubleParam(2.0)) {
		t.Fatalf("set value mismatch: %+v", got)
	}
	select {
	case event := <-ch:
		if event.Version != s.Version() || len(event.Changes) != 1 || event.Source != "set" {
			t.Fatalf("event mismatch: %+v", event)
		}
	default:
		t.Fatal("set must emit one event")
	}
}

// checkSetTypeMismatch pins wrong type fails without mutation.
func checkSetTypeMismatch(t *testing.T) {
	t.Helper()
	s := mustStore(t, ConfigStoreOptions{})
	mustDeclare(t, s, "max_vel_x", NewDoubleParam(1.5), ParamDescriptor{Type: ParamTypeDouble})
	before := s.Version()
	err := s.Set("max_vel_x", NewStringParam("fast"))
	if err == nil {
		t.Fatal("string for double succeeded, want schema rejection")
	}
	got, err := s.Get("max_vel_x")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if !got.Equal(NewDoubleParam(1.5)) {
		t.Fatalf("rejected set mutated value: %+v", got)
	}
	if got := s.Version(); got != before {
		t.Fatalf("rejected set moved version to %d", got)
	}
}

// checkSetReadOnly pins read-only rejects set after declare.
func checkSetReadOnly(t *testing.T) {
	t.Helper()
	s := mustStore(t, ConfigStoreOptions{})
	mustDeclare(t, s, "frame_id", NewStringParam("map"), ParamDescriptor{Type: ParamTypeString, ReadOnly: true})
	err := s.Set("frame_id", NewStringParam("odom"))
	if err == nil {
		t.Fatal("read-only set succeeded, want error")
	}
	got, err := s.Get("frame_id")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if !got.Equal(NewStringParam("map")) {
		t.Fatalf("read-only set mutated value: %+v", got)
	}
}

// checkSetUndeclared pins the allow flag controls auto declare.
func checkSetUndeclared(t *testing.T) {
	t.Helper()
	closed := mustStore(t, ConfigStoreOptions{})
	err := closed.Set("fresh", NewIntegerParam(1))
	if err == nil {
		t.Fatal("closed set on undeclared succeeded, want error")
	}
	open := mustStore(t, ConfigStoreOptions{AllowUndeclared: true})
	err = open.Set("fresh", NewIntegerParam(1))
	if err != nil {
		t.Fatalf("open set on undeclared: %v", err)
	}
	got, err := open.Get("fresh")
	if err != nil {
		t.Fatalf("get auto-declared: %v", err)
	}
	if !got.Equal(NewIntegerParam(1)) {
		t.Fatalf("auto-declared value mismatch: %+v", got)
	}
	desc, err := open.Describe("fresh")
	if err != nil {
		t.Fatalf("describe auto-declared: %v", err)
	}
	if desc.Type != ParamTypeInteger {
		t.Fatalf("auto-declared type must be integer, got %s", desc.Type)
	}
}

// checkAtomicAllOrNothing pins multi-set validates first and commits once.
func checkAtomicAllOrNothing(t *testing.T) {
	t.Helper()
	s := mustStore(t, ConfigStoreOptions{})
	mustDeclare(t, s, "a", NewIntegerParam(1), ParamDescriptor{Type: ParamTypeInteger})
	mustDeclare(t, s, "b", NewIntegerParam(2), ParamDescriptor{Type: ParamTypeInteger})
	before := s.Version()
	_, err := s.SetAtomically(map[string]ParamValue{"a": NewIntegerParam(10), "b": NewStringParam("bad")})
	if err == nil {
		t.Fatal("mixed batch succeeded, want atomic rejection")
	}
	for _, tc := range []struct {
		name string
		want ParamValue
	}{
		{"a", NewIntegerParam(1)},
		{"b", NewIntegerParam(2)},
	} {
		got, err := s.Get(tc.name)
		if err != nil {
			t.Fatalf("get %q: %v", tc.name, err)
		}
		if !got.Equal(tc.want) {
			t.Fatalf("%q mutated to %+v", tc.name, got)
		}
	}
	if got := s.Version(); got != before {
		t.Fatalf("rejected batch moved version to %d", got)
	}
	event, err := s.SetAtomically(map[string]ParamValue{"a": NewIntegerParam(10), "b": NewIntegerParam(20)})
	if err != nil {
		t.Fatalf("valid batch: %v", err)
	}
	if len(event.Changes) != 2 {
		t.Fatalf("batch must carry two changes, got %+v", event)
	}
	if got := s.Version(); got != before+1 {
		t.Fatalf("batch must bump version once, got %d want %d", got, before+1)
	}
	if event.Changes[0].Name != "a" || event.Changes[1].Name != "b" {
		t.Fatalf("changes must sort by name, got %+v", event.Changes)
	}
}

// checkValidatorSplitsFromApply pins validator rejection skips apply hooks,
// version, events, and history.
func checkValidatorSplitsFromApply(t *testing.T) {
	t.Helper()
	s := mustStore(t, ConfigStoreOptions{})
	mustDeclare(t, s, "max_vel_x", NewDoubleParam(1.0), ParamDescriptor{Type: ParamTypeDouble})
	applied := 0
	err := s.AddValidator("max_vel_x", func(_, newValue ParamValue) error {
		if newValue.Double > 5.0 {
			return errors.New("too fast")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("add validator: %v", err)
	}
	err = s.AddApplyHook("max_vel_x", func(_, _ ParamValue) {
		applied++
	})
	if err != nil {
		t.Fatalf("add hook: %v", err)
	}
	before := s.Version()
	histBefore := len(s.History())
	err = s.Set("max_vel_x", NewDoubleParam(99.0))
	if err == nil {
		t.Fatal("validator rejection succeeded, want error")
	}
	if applied != 0 {
		t.Fatal("apply ran after validator rejection")
	}
	if got := s.Version(); got != before {
		t.Fatalf("rejected set moved version to %d", got)
	}
	if got := len(s.History()); got != histBefore {
		t.Fatalf("rejected set appended history, got %d want %d", got, histBefore)
	}
	got, err := s.Get("max_vel_x")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if !got.Equal(NewDoubleParam(1.0)) {
		t.Fatalf("rejected set mutated value: %+v", got)
	}
}

// checkApplyRunsOnSuccess pins hooks run once after a validated commit.
func checkApplyRunsOnSuccess(t *testing.T) {
	t.Helper()
	s := mustStore(t, ConfigStoreOptions{})
	mustDeclare(t, s, "gain", NewDoubleParam(1.0), ParamDescriptor{Type: ParamTypeDouble})
	var seenOld, seenNew ParamValue
	calls := 0
	err := s.AddValidator("gain", func(_, _ ParamValue) error {
		return nil
	})
	if err != nil {
		t.Fatalf("add validator: %v", err)
	}
	err = s.AddApplyHook("gain", func(oldValue, newValue ParamValue) {
		calls++
		seenOld = oldValue
		seenNew = newValue
	})
	if err != nil {
		t.Fatalf("add hook: %v", err)
	}
	err = s.Set("gain", NewDoubleParam(2.0))
	if err != nil {
		t.Fatalf("set: %v", err)
	}
	if calls != 1 {
		t.Fatalf("apply calls = %d, want 1", calls)
	}
	if !seenOld.Equal(NewDoubleParam(1.0)) || !seenNew.Equal(NewDoubleParam(2.0)) {
		t.Fatalf("hook values mismatch old=%+v new=%+v", seenOld, seenNew)
	}
}

// checkStartupBatchingPrecedence pins files < CLI < code with one event.
//
//nolint:cyclop // startup test sequences merge, version, and watch checks in one run, splitting would obscure intent
func checkStartupBatchingPrecedence(t *testing.T) {
	t.Helper()
	s := mustStore(t, ConfigStoreOptions{})
	_, ch, cancel := s.Watch()
	defer cancel()
	before := s.Version()
	files := map[string]ParamValue{"gain": NewDoubleParam(1.0), "extra": NewIntegerParam(1)}
	cli := map[string]ParamValue{"gain": NewDoubleParam(2.0)}
	code := map[string]ParamValue{"gain": NewDoubleParam(3.0)}
	event, err := s.ApplyStartup(files, cli, code)
	if err != nil {
		t.Fatalf("startup: %v", err)
	}
	if len(event.Changes) != 2 {
		t.Fatalf("startup must batch two params, got %+v", event)
	}
	if event.Source != "startup" {
		t.Fatalf("startup source = %q, want startup", event.Source)
	}
	if got := s.Version(); got != before+1 {
		t.Fatalf("startup must bump version once, got %d want %d", got, before+1)
	}
	got, err := s.Get("gain")
	if err != nil {
		t.Fatalf("get gain: %v", err)
	}
	if !got.Equal(NewDoubleParam(3.0)) {
		t.Fatalf("code must win precedence, got %+v", got)
	}
	select {
	case first := <-ch:
		if first.Version != s.Version() {
			t.Fatalf("watch version = %d, want %d", first.Version, s.Version())
		}
		select {
		case extra := <-ch:
			t.Fatalf("startup must emit one event, not N: second %+v", extra)
		default:
		}
	default:
		t.Fatal("startup must emit one batched event")
	}
	hist := s.HistorySince(before)
	if len(hist) != 2 {
		t.Fatalf("history must hold two entries, got %+v", hist)
	}
	for _, h := range hist {
		if h.Version != s.Version() {
			t.Fatalf("batch entries must share version, got %+v", hist)
		}
	}
}

// checkStartupEmpty pins empty startup emits no bump.
func checkStartupEmpty(t *testing.T) {
	t.Helper()
	s := mustStore(t, ConfigStoreOptions{})
	before := s.Version()
	event, err := s.ApplyStartup(nil, nil, nil)
	if err != nil {
		t.Fatalf("empty startup: %v", err)
	}
	if len(event.Changes) != 0 {
		t.Fatalf("empty startup must carry no changes, got %+v", event)
	}
	if got := s.Version(); got != before {
		t.Fatalf("empty startup moved version to %d", got)
	}
}

// checkWildcardFQNPrecedence pins fully qualified beats wildcard.
func checkWildcardFQNPrecedence(t *testing.T) {
	t.Helper()
	files := map[string]map[string]ParamValue{
		"*":            {"gain": NewDoubleParam(1.0), "extra": NewIntegerParam(1)},
		"ctrl.server":  {"gain": NewDoubleParam(9.0)},
		"/ctrl/server": {"gain": NewDoubleParam(9.0)},
		"other.node":   {"gain": NewDoubleParam(2.0)},
	}
	merged := CollapseParamFiles(files, "ctrl.server")
	got, ok := merged["gain"]
	if !ok {
		t.Fatal("collapsed gain missing")
	}
	if !got.Equal(NewDoubleParam(9.0)) {
		t.Fatalf("FQN must beat wildcard, got %+v", got)
	}
	if _, ok := merged["extra"]; !ok {
		t.Fatal("wildcard extra must survive")
	}
	wildOnly := CollapseParamFiles(map[string]map[string]ParamValue{
		"/*": {"gain": NewDoubleParam(4.0)},
	}, "ctrl.server")
	if !wildOnly["gain"].Equal(NewDoubleParam(4.0)) {
		t.Fatalf("slash wildcard must apply, got %+v", wildOnly["gain"])
	}
}

// checkFlattenDotted pins nested maps flatten with dotted keys.
func checkFlattenDotted(t *testing.T) {
	t.Helper()
	nested := map[string]any{
		"controller": map[string]any{
			"gain":  2.0,
			"limit": 5,
		},
		"frame": "map",
	}
	flat, err := FlattenNestedParams(nested, "")
	if err != nil {
		t.Fatalf("flatten: %v", err)
	}
	if !flat["controller.gain"].Equal(NewDoubleParam(2.0)) {
		t.Fatalf("dotted gain mismatch: %+v", flat["controller.gain"])
	}
	if !flat["controller.limit"].Equal(NewIntegerParam(5)) {
		t.Fatalf("dotted limit mismatch: %+v", flat["controller.limit"])
	}
	if !flat["frame"].Equal(NewStringParam("map")) {
		t.Fatalf("frame mismatch: %+v", flat["frame"])
	}
	prefixed, err := FlattenNestedParams(map[string]any{"gain": 1.0}, "ctrl")
	if err != nil {
		t.Fatalf("prefixed flatten: %v", err)
	}
	if _, ok := prefixed["ctrl.gain"]; !ok {
		t.Fatalf("prefix join missing, got %+v", prefixed)
	}
}

// checkUniformArrayTyping pins mixed arrays fail.
func checkUniformArrayTyping(t *testing.T) {
	t.Helper()
	_, err := ParamValueFromAny([]any{1, "two"})
	if err == nil {
		t.Fatal("mixed array accepted, want uniform typing error")
	}
	_, err = ParamValueFromAny([]any{})
	if err == nil {
		t.Fatal("empty untyped array accepted, want error")
	}
	good, err := ParamValueFromAny([]any{1, 2, 3})
	if err != nil {
		t.Fatalf("uniform int array: %v", err)
	}
	if !good.Equal(NewIntegerArrayParam([]int64{1, 2, 3})) {
		t.Fatalf("int array mismatch: %+v", good)
	}
}

// checkOverrideLiteral pins YAML scalar and array literal parsing.
func checkOverrideLiteral(t *testing.T) {
	t.Helper()
	cases := []struct {
		text string
		want ParamValue
	}{
		{"true", NewBoolParam(true)},
		{"42", NewIntegerParam(42)},
		{"1.5", NewDoubleParam(1.5)},
		{"hello", NewStringParam("hello")},
		{`"hello"`, NewStringParam("hello")},
		{"[1, 2]", NewIntegerArrayParam([]int64{1, 2})},
		{"[true, false]", NewBoolArrayParam([]bool{true, false})},
	}
	for _, tc := range cases {
		got, err := ParseOverrideLiteral(tc.text)
		if err != nil {
			t.Fatalf("%q: %v", tc.text, err)
		}
		if !got.Equal(tc.want) {
			t.Fatalf("%q = %+v, want %+v", tc.text, got, tc.want)
		}
	}
	if _, err := ParseOverrideLiteral("[1, true]"); err == nil {
		t.Fatal("mixed literal array accepted, want error")
	}
	if _, err := ParseOverrideLiteral("{a: 1}"); err == nil {
		t.Fatal("dict literal accepted, want files-only error")
	}
	if _, err := ParseOverrideLiteral(""); err == nil {
		t.Fatal("empty literal accepted, want error")
	}
}

// checkOverrideParse pins name:=literal splitting and name checks.
func checkOverrideParse(t *testing.T) {
	t.Helper()
	name, value, err := ParseParamOverride("max_vel_x:=1.5")
	if err != nil {
		t.Fatalf("parse override: %v", err)
	}
	if name != "max_vel_x" || !value.Equal(NewDoubleParam(1.5)) {
		t.Fatalf("override mismatch name=%q value=%+v", name, value)
	}
	if _, _, err := ParseParamOverride("no-separator"); err == nil {
		t.Fatal("missing := accepted, want error")
	}
	if _, _, err := ParseParamOverride("bad-name!:=1"); err == nil {
		t.Fatal("bad name accepted, want error")
	}
}

// checkOverridesAutoDeclare pins overrides declare missing names.
func checkOverridesAutoDeclare(t *testing.T) {
	t.Helper()
	s := mustStore(t, ConfigStoreOptions{})
	event, err := s.ApplyOverrides(map[string]ParamValue{"fresh": NewIntegerParam(7)})
	if err != nil {
		t.Fatalf("overrides: %v", err)
	}
	if event.Source != "override" || len(event.Changes) != 1 {
		t.Fatalf("override event mismatch: %+v", event)
	}
	got, err := s.Get("fresh")
	if err != nil {
		t.Fatalf("get auto-declared override: %v", err)
	}
	if !got.Equal(NewIntegerParam(7)) {
		t.Fatalf("override value mismatch: %+v", got)
	}
}

// checkRemapPureFirstMatch pins pure first-match across global plus local.
func checkRemapPureFirstMatch(t *testing.T) {
	t.Helper()
	global := []RemapRule{{From: "gain", To: "tuned.gain"}, {From: "gain", To: "second"}}
	local := []RemapRule{{From: "gain", To: "local.gain"}}
	got := ApplyRemap("gain", global, local)
	if got != "tuned.gain" {
		t.Fatalf("first global must win, got %q", got)
	}
	got = ApplyRemap("gain", nil, local)
	if got != "local.gain" {
		t.Fatalf("local must apply alone, got %q", got)
	}
	got = ApplyRemap("other", global, local)
	if got != "other" {
		t.Fatalf("unmatched must pass through, got %q", got)
	}
	beforeGlobal := append([]RemapRule(nil), global...)
	beforeLocal := append([]RemapRule(nil), local...)
	_ = ApplyRemap("gain", global, local)
	if len(global) != len(beforeGlobal) || len(local) != len(beforeLocal) {
		t.Fatal("ApplyRemap mutated its inputs")
	}
}

// checkRemapAtCreation pins the store rewrites through the fixed table.
func checkRemapAtCreation(t *testing.T) {
	t.Helper()
	s := mustStore(t, ConfigStoreOptions{
		GlobalRemaps: []RemapRule{{From: "gain", To: "tuned.gain"}},
		LocalRemaps:  []RemapRule{{From: "gain", To: "local.gain"}},
	})
	mustDeclare(t, s, "gain", NewDoubleParam(1.0), ParamDescriptor{Type: ParamTypeDouble})
	got, err := s.Get("tuned.gain")
	if err != nil {
		t.Fatalf("remapped get: %v", err)
	}
	if !got.Equal(NewDoubleParam(1.0)) {
		t.Fatalf("remapped value mismatch: %+v", got)
	}
	if names := s.List(""); len(names) != 1 || names[0] != "tuned.gain" {
		t.Fatalf("stored name must be rewritten, got %v", names)
	}
	if got := s.Remapped("gain"); got != "tuned.gain" {
		t.Fatalf("first global must win at creation, got %q", got)
	}
}

// checkRemapInvalidCreation pins bad rules fail NewConfigStore.
func checkRemapInvalidCreation(t *testing.T) {
	t.Helper()
	_, err := NewConfigStore(ConfigStoreOptions{GlobalRemaps: []RemapRule{{From: "", To: "gain"}}})
	if err == nil {
		t.Fatal("empty remap from accepted, want error")
	}
	_, err = NewConfigStore(ConfigStoreOptions{LocalRemaps: []RemapRule{{From: "gain", To: "bad-name!"}}})
	if err == nil {
		t.Fatal("bad remap to accepted, want error")
	}
}

// checkRemapFixed pins no post-creation remap update path exists by
// asserting the table observed at creation stays stable.
func checkRemapFixed(t *testing.T) {
	t.Helper()
	s := mustStore(t, ConfigStoreOptions{GlobalRemaps: []RemapRule{{From: "a", To: "b"}}})
	first := s.Remapped("a")
	second := s.Remapped("a")
	if first != "b" || second != "b" {
		t.Fatalf("remap must stay fixed at creation, got %q and %q", first, second)
	}
}

// checkWatchVersionedHistory pins watches are versioned with replay.
func checkWatchVersionedHistory(t *testing.T) {
	t.Helper()
	s := mustStore(t, ConfigStoreOptions{AllowUndeclared: true})
	since, ch, cancel := s.Watch()
	defer cancel()
	_, err := s.SetAtomically(map[string]ParamValue{"a": NewIntegerParam(2), "b": NewIntegerParam(3)})
	if err != nil {
		t.Fatalf("batch: %v", err)
	}
	select {
	case event := <-ch:
		if len(event.Changes) != 2 {
			t.Fatalf("watch must deliver one batched event with two changes, got %+v", event)
		}
		replay := s.HistorySince(since)
		if len(replay) != 2 {
			t.Fatalf("history replay must hold two entries, got %+v", replay)
		}
		if replay[0].Version != event.Version || replay[1].Version != event.Version {
			t.Fatalf("batch history must share event version, got %+v", replay)
		}
	default:
		t.Fatal("watch must deliver the batch")
	}
	empty := s.HistorySince(s.Version())
	if len(empty) != 0 {
		t.Fatalf("history since current must be empty, got %+v", empty)
	}
}

// checkDescribeListDump pins describe, prefix list sorting, and dump copy.
func checkDescribeListDump(t *testing.T) {
	t.Helper()
	s := mustStore(t, ConfigStoreOptions{AllowUndeclared: true})
	err := s.Set("ctrl.gain", NewDoubleParam(1.0))
	if err != nil {
		t.Fatalf("set ctrl.gain: %v", err)
	}
	err = s.Set("ctrl.limit", NewIntegerParam(5))
	if err != nil {
		t.Fatalf("set ctrl.limit: %v", err)
	}
	err = s.Set("other", NewBoolParam(true))
	if err != nil {
		t.Fatalf("set other: %v", err)
	}
	desc, err := s.Describe("ctrl.gain")
	if err != nil {
		t.Fatalf("describe: %v", err)
	}
	if desc.Type != ParamTypeDouble {
		t.Fatalf("describe type = %s, want double", desc.Type)
	}
	names := s.List("ctrl")
	if len(names) != 2 || names[0] != "ctrl.gain" || names[1] != "ctrl.limit" {
		t.Fatalf("prefix list must sort, got %v", names)
	}
	all := s.List("")
	if len(all) != 3 {
		t.Fatalf("full list must hold three, got %v", all)
	}
	dump := s.Dump()
	dump["ctrl.gain"] = NewDoubleParam(99.0)
	got, err := s.Get("ctrl.gain")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if !got.Equal(NewDoubleParam(1.0)) {
		t.Fatalf("dump mutation aliased store: %+v", got)
	}
}

// checkSchemaValidation pins missing, wrong type, unknown, and range.
func checkSchemaValidation(t *testing.T) {
	t.Helper()
	schema := ParamSchema{
		"max_vel_x": {Type: ParamTypeDouble, HasDoubleRange: true, DoubleMin: 0, DoubleMax: 5},
		"joint":     {Type: ParamTypeString},
	}
	good := map[string]ParamValue{"max_vel_x": NewDoubleParam(1.5), "joint": NewStringParam("shoulder")}
	err := schema.ValidateValues(good)
	if err != nil {
		t.Fatalf("good values: %v", err)
	}
	badType := map[string]ParamValue{"max_vel_x": NewStringParam("fast"), "joint": NewStringParam("elbow")}
	err = schema.ValidateValues(badType)
	if err == nil || !strings.Contains(err.Error(), "max_vel_x") {
		t.Fatalf("string for double must fail with name, got %v", err)
	}
	missing := map[string]ParamValue{"max_vel_x": NewDoubleParam(1.0)}
	err = schema.ValidateValues(missing)
	if err == nil || !strings.Contains(err.Error(), "missing") {
		t.Fatalf("missing joint must fail, got %v", err)
	}
	unknown := map[string]ParamValue{"max_vel_x": NewDoubleParam(1.0), "joint": NewStringParam("a"), "extra": NewIntegerParam(1)}
	err = schema.ValidateValues(unknown)
	if err == nil || !strings.Contains(err.Error(), "unknown") {
		t.Fatalf("unknown joint must fail, got %v", err)
	}
	outOfRange := map[string]ParamValue{"max_vel_x": NewDoubleParam(99.0), "joint": NewStringParam("a")}
	err = schema.ValidateValues(outOfRange)
	if err == nil || !strings.Contains(err.Error(), "out of range") {
		t.Fatalf("wrong units must fail range, got %v", err)
	}
	s := mustStore(t, ConfigStoreOptions{})
	_, err = s.ApplyStartup(good, nil, nil)
	if err != nil {
		t.Fatalf("startup good: %v", err)
	}
	err = s.ValidateSchema(schema)
	if err != nil {
		t.Fatalf("store schema: %v", err)
	}
}

// checkDumpLoadRoundTrip pins dump feeds back through a batch unchanged.
func checkDumpLoadRoundTrip(t *testing.T) {
	t.Helper()
	s := mustStore(t, ConfigStoreOptions{})
	_, err := s.ApplyStartup(map[string]ParamValue{"a": NewIntegerParam(1), "b": NewStringParam("x")}, nil, nil)
	if err != nil {
		t.Fatalf("startup: %v", err)
	}
	dump := s.Dump()
	other := mustStore(t, ConfigStoreOptions{})
	_, err = other.ApplyStartup(dump, nil, nil)
	if err != nil {
		t.Fatalf("load dump: %v", err)
	}
	for name, want := range dump {
		got, err := other.Get(name)
		if err != nil {
			t.Fatalf("get %q: %v", name, err)
		}
		if !got.Equal(want) {
			t.Fatalf("%q round trip mismatch: got %+v want %+v", name, got, want)
		}
	}
}

// checkNodeScopedExpansion pins relative names join the namespace.
func checkNodeScopedExpansion(t *testing.T) {
	t.Helper()
	got, err := ExpandNodeScopedParam("gain", "ctrl.server")
	if err != nil {
		t.Fatalf("expand: %v", err)
	}
	if got != "ctrl.server.gain" {
		t.Fatalf("expanded = %q, want ctrl.server.gain", got)
	}
	got, err = ExpandNodeScopedParam("ctrl.server.gain", "other")
	if err != nil {
		t.Fatalf("scoped passthrough: %v", err)
	}
	if got != "ctrl.server.gain" {
		t.Fatalf("scoped must pass through, got %q", got)
	}
	if _, err := ExpandNodeScopedParam("", "ns"); err == nil {
		t.Fatal("empty name accepted, want error")
	}
}

// checkNameValidation pins bad names fail and full names pass.
func checkNameValidation(t *testing.T) {
	t.Helper()
	for _, bad := range []string{"", ".lead", "trail.", "a..b", "bad-name!", "has space"} {
		err := ValidateParamName(bad)
		if err == nil {
			t.Fatalf("bad name %q accepted, want error", bad)
		}
	}
	for _, good := range []string{"gain", "ctrl.gain", "a1.b2_c3"} {
		err := ValidateParamName(good)
		if err != nil {
			t.Fatalf("good name %q: %v", good, err)
		}
	}
	err := ValidateRemapRule(RemapRule{From: "a", To: "b"})
	if err != nil {
		t.Fatalf("good remap: %v", err)
	}
	long := strings.Repeat("a", 256)
	err = ValidateParamName(long)
	if err == nil {
		t.Fatal("overlong name accepted, want error")
	}
}

// checkConcurrentSets pins concurrent batches stay race free and versioned.
func checkConcurrentSets(t *testing.T) {
	t.Helper()
	s := mustStore(t, ConfigStoreOptions{AllowUndeclared: true})
	var wg sync.WaitGroup
	for i := range 8 {
		wg.Go(func() {
			_ = s.Set(strings.Join([]string{"p", string(rune('a' + i))}, "."), NewIntegerParam(int64(i))) //nolint:errcheck,gosec // errcheck valid names succeed with version check; gosec G115 safe i 0..7 so 97..104 fits rune
			_, _ = s.Get("p.a")                                                                           //nolint:errcheck // racy read may miss p.a before it is set, intentionally ignored
			_ = s.List("")
			_ = s.History()
		})
	}
	wg.Wait()
	if got := s.Version(); got != 8 {
		t.Fatalf("concurrent sets must total 8 versions, got %d", got)
	}
}
