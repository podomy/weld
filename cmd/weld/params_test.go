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

	"github.com/podomy/weld/internal/transports/dds"
)

// paramsTestStore returns a fresh store for CLI tests.
func paramsTestStore() *dds.MemoryStore {
	return dds.NewMemoryStore("/weld")
}

// paramsRun runs params command and returns exit code plus outputs.
func paramsRun(args []string, store *dds.MemoryStore) (int, string, string) {
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	code := runParamsCommand(context.Background(), args, &stdout, &stderr, store)
	return code, stdout.String(), stderr.String()
}

// TestParamsDeclareGet checks declare then get round trip.
func TestParamsDeclareGet(t *testing.T) {
	store := paramsTestStore()
	code, stdout, stderr := paramsRun([]string{"declare", "--type", "integer", "my_int", "42"}, store)
	if code != 0 {
		t.Fatalf("declare code = %d, want 0: stdout=%q stderr=%q", code, stdout, stderr)
	}
	if !strings.Contains(stdout, "my_int=42") {
		t.Fatalf("declare stdout = %q, want my_int=42", stdout)
	}
	code, stdout, stderr = paramsRun([]string{"get", "my_int"}, store)
	if code != 0 {
		t.Fatalf("get code = %d: %q %q", code, stdout, stderr)
	}
	if !strings.Contains(stdout, "my_int=42") {
		t.Fatalf("get stdout = %q, want my_int=42", stdout)
	}
}

// TestParamsDeclareDuplicate checks second declare fails with exit 1.
func TestParamsDeclareDuplicate(t *testing.T) {
	store := paramsTestStore()
	code, _, _ := paramsRun([]string{"declare", "--type", "bool", "dup", "true"}, store)
	if code != 0 {
		t.Fatalf("first declare code = %d, want 0", code)
	}
	code, _, stderr := paramsRun([]string{"declare", "--type", "bool", "dup", "false"}, store)
	if code != 1 {
		t.Fatalf("duplicate code = %d, want 1", code)
	}
	if !strings.HasPrefix(strings.TrimSpace(stderr), "weld:") {
		t.Fatalf("duplicate stderr = %q, want weld prefix", stderr)
	}
}

// TestParamsGetMissing checks missing get fails with exit 1.
func TestParamsGetMissing(t *testing.T) {
	store := paramsTestStore()
	code, _, stderr := paramsRun([]string{"get", "absent"}, store)
	if code != 1 {
		t.Fatalf("missing get code = %d, want 1", code)
	}
	if !strings.Contains(stderr, "not set") {
		t.Fatalf("missing stderr = %q, want not set", stderr)
	}
}

// TestParamsSet checks set updates value.
func TestParamsSet(t *testing.T) {
	store := paramsTestStore()
	code, _, _ := paramsRun([]string{"declare", "--type", "integer", "v", "1"}, store)
	if code != 0 {
		t.Fatalf("declare code = %d", code)
	}
	code, stdout, _ := paramsRun([]string{"set", "--type", "integer", "v", "2"}, store)
	if code != 0 {
		t.Fatalf("set code = %d, stdout=%q", code, stdout)
	}
	if !strings.Contains(stdout, "v=2") {
		t.Fatalf("set stdout = %q, want v=2", stdout)
	}
	code, stdout, _ = paramsRun([]string{"get", "v"}, store)
	if code != 0 || !strings.Contains(stdout, "v=2") {
		t.Fatalf("get after set = %d %q", code, stdout)
	}
}

// TestParamsSetAtomic checks atomic flag path.
func TestParamsSetAtomic(t *testing.T) {
	store := paramsTestStore()
	code, _, _ := paramsRun([]string{"set", "--type", "string", "--atomic", "a", "hello"}, store)
	if code != 0 {
		t.Fatalf("atomic set code = %d", code)
	}
	code, stdout, _ := paramsRun([]string{"get", "a"}, store)
	if code != 0 || !strings.Contains(stdout, "hello") {
		t.Fatalf("atomic get = %d %q", code, stdout)
	}
}

// TestParamsDescribe checks descriptor output.
func TestParamsDescribe(t *testing.T) {
	store := paramsTestStore()
	code, _, _ := paramsRun([]string{"declare", "--type", "bool", "--description", "flag help", "d", "true"}, store)
	if code != 0 {
		t.Fatalf("declare code = %d", code)
	}
	code, stdout, _ := paramsRun([]string{"describe", "d"}, store)
	if code != 0 {
		t.Fatalf("describe code = %d", code)
	}
	if !strings.Contains(stdout, "d:") || !strings.Contains(stdout, "bool") {
		t.Fatalf("describe stdout = %q", stdout)
	}
}

// TestParamsList checks names and types output.
func TestParamsList(t *testing.T) {
	store := paramsTestStore()
	_, _, _ = paramsRun([]string{"declare", "--type", "double", "ctrl.max", "1.0"}, store)
	_, _, _ = paramsRun([]string{"declare", "--type", "double", "ctrl.min", "0.0"}, store)
	_, _, _ = paramsRun([]string{"declare", "--type", "bool", "other", "true"}, store)
	code, stdout, _ := paramsRun([]string{"list"}, store)
	if code != 0 {
		t.Fatalf("list code = %d", code)
	}
	if !strings.Contains(stdout, "ctrl.max") || !strings.Contains(stdout, "other") {
		t.Fatalf("list stdout = %q", stdout)
	}
	code, stdout, _ = paramsRun([]string{"list", "--types"}, store)
	if code != 0 {
		t.Fatalf("list types code = %d", code)
	}
	if !strings.Contains(stdout, "(double)") || !strings.Contains(stdout, "(bool)") {
		t.Fatalf("list types stdout = %q", stdout)
	}
	code, stdout, _ = paramsRun([]string{"list", "--prefixes", "ctrl"}, store)
	if code != 0 {
		t.Fatalf("list prefixes code = %d", code)
	}
	if strings.Contains(stdout, "other") {
		t.Fatalf("filtered list contains other: %q", stdout)
	}
	if !strings.Contains(stdout, "ctrl.max") {
		t.Fatalf("filtered list misses ctrl.max: %q", stdout)
	}
}

// TestParamsEvents checks events record declare and set.
func TestParamsEvents(t *testing.T) {
	store := paramsTestStore()
	_, _, _ = paramsRun([]string{"declare", "--type", "integer", "e", "1"}, store)
	_, _, _ = paramsRun([]string{"set", "--type", "integer", "e", "2"}, store)
	code, stdout, _ := paramsRun([]string{"events"}, store)
	if code != 0 {
		t.Fatalf("events code = %d", code)
	}
	if !strings.Contains(stdout, "new=1") && !strings.Contains(stdout, "new=") {
		t.Fatalf("events stdout = %q, want new count", stdout)
	}
	if !strings.Contains(stdout, "changed=") {
		t.Fatalf("events stdout = %q, want changed count", stdout)
	}
}

// TestParamsValueTypes checks all type parses.
func TestParamsValueTypes(t *testing.T) {
	cases := []struct {
		typeName string
		text     string
		check    string
	}{
		{"bool", "true", "true"},
		{"integer", "-7", "-7"},
		{"double", "2.5", "2.5"},
		{"string", "hello world", "hello world"},
		{"bool_array", "true,false", "[true, false]"},
		{"integer_array", "1,2,3", "[1, 2, 3]"},
		{"double_array", "1.5,2.5", "[1.5, 2.5]"},
		{"string_array", "a,b", "[a, b]"},
		{"byte_array", "1,2,255", "[1, 2, 255]"},
	}
	for _, tc := range cases {
		store := paramsTestStore()
		name := "p_" + strings.ReplaceAll(tc.typeName, "_", "")
		code, _, stderr := paramsRun([]string{"declare", "--type", tc.typeName, name, tc.text}, store)
		if code != 0 {
			t.Fatalf("declare %s code = %d: %q", tc.typeName, code, stderr)
		}
		code, stdout, _ := paramsRun([]string{"get", name}, store)
		if code != 0 {
			t.Fatalf("get %s code = %d", tc.typeName, code)
		}
		if !strings.Contains(stdout, tc.check) {
			t.Fatalf("type %s stdout = %q, want %q", tc.typeName, stdout, tc.check)
		}
	}
}

// TestParamsInvalidType checks unknown type fails with exit 2.
func TestParamsInvalidType(t *testing.T) {
	store := paramsTestStore()
	code, _, stderr := paramsRun([]string{"declare", "--type", "bogus", "x", "1"}, store)
	if code != 2 {
		t.Fatalf("bogus type code = %d, want 2", code)
	}
	if !strings.HasPrefix(strings.TrimSpace(stderr), "weld:") {
		t.Fatalf("bogus stderr = %q, want weld prefix", stderr)
	}
}

// TestParamsUsage checks help and unknown verb.
func TestParamsUsage(t *testing.T) {
	store := paramsTestStore()
	code, stdout, _ := paramsRun([]string{"--help"}, store)
	if code != 0 {
		t.Fatalf("help code = %d, want 0", code)
	}
	if !strings.Contains(stdout, "Usage:") {
		t.Fatalf("help stdout misses Usage: %q", stdout)
	}
	code, _, _ = paramsRun([]string{"declare", "--help"}, store)
	if code != 0 {
		t.Fatalf("declare help code = %d, want 0", code)
	}
	code, _, stderr := paramsRun([]string{"bogus"}, store)
	if code != 2 {
		t.Fatalf("unknown verb code = %d, want 2", code)
	}
	if !strings.Contains(stderr, "unknown params subcommand") {
		t.Fatalf("unknown stderr = %q", stderr)
	}
	code, _, _ = paramsRun([]string{}, store)
	if code != 2 {
		t.Fatalf("empty code = %d, want 2", code)
	}
}

// TestParamsDumpLoadRoundTrip checks dump to load preserves values.
// declareDumpFixture declares four typed params for dump tests.
func declareDumpFixture(t *testing.T, store *dds.MemoryStore) {
	t.Helper()

	_, _, _ = paramsRun([]string{"declare", "--type", "bool", "flag", "true"}, store)
	_, _, _ = paramsRun([]string{"declare", "--type", "integer", "count", "42"}, store)
	_, _, _ = paramsRun([]string{"declare", "--type", "double", "ratio", "2.5"}, store)
	_, _, _ = paramsRun([]string{"declare", "--type", "string", "name", "weld"}, store)
}

// checkDumpFile dumps store to path and checks header plus flag.
func checkDumpFile(t *testing.T, store *dds.MemoryStore, path string) {
	t.Helper()

	code, _, stderr := paramsRun([]string{"dump", "--file", path}, store)
	if code != 0 {
		t.Fatalf("dump code = %d: %q", code, stderr)
	}
	data, err := os.ReadFile(path) //nolint:gosec // test reads file it just wrote
	if err != nil {
		t.Fatalf("read dump: %v", err)
	}
	text := string(data)
	if !strings.Contains(text, "ros__parameters:") {
		t.Fatalf("dump misses ros__parameters:\n%s", text)
	}
	if !strings.Contains(text, "flag: true") {
		t.Fatalf("dump misses flag:\n%s", text)
	}
}

// checkLoadedValue gets one param and checks it contains want.
func checkLoadedValue(t *testing.T, store *dds.MemoryStore, name, want string) {
	t.Helper()

	code, stdout, _ := paramsRun([]string{"get", name}, store)
	if code != 0 || !strings.Contains(stdout, want) {
		t.Fatalf("loaded %s = %d %q, want %q", name, code, stdout, want)
	}
}

func TestParamsDumpLoadRoundTrip(t *testing.T) {
	first := paramsTestStore()
	declareDumpFixture(t, first)
	dir := t.TempDir()
	path := filepath.Join(dir, "params.yaml")
	checkDumpFile(t, first, path)
	second := paramsTestStore()
	code, _, stderr := paramsRun([]string{"load", "--file", path}, second)
	if code != 0 {
		t.Fatalf("load code = %d: %q", code, stderr)
	}
	checkLoadedValue(t, second, "flag", "true")
	checkLoadedValue(t, second, "count", "42")
	checkLoadedValue(t, second, "ratio", "2.5")
	checkLoadedValue(t, second, "name", "weld")
}

// TestParamsDumpStdout checks dump to stdout contains header.
func TestParamsDumpStdout(t *testing.T) {
	store := paramsTestStore()
	_, _, _ = paramsRun([]string{"declare", "--type", "integer", "solo", "7"}, store)
	code, stdout, _ := paramsRun([]string{"dump"}, store)
	if code != 0 {
		t.Fatalf("dump stdout code = %d", code)
	}
	if !strings.Contains(stdout, "/weld:") || !strings.Contains(stdout, "solo: 7") {
		t.Fatalf("dump stdout = %q", stdout)
	}
}

// TestParamsNodeValidation checks bad node fails with exit 2.
func TestParamsNodeValidation(t *testing.T) {
	store := paramsTestStore()
	code, _, stderr := paramsRun([]string{"declare", "--type", "integer", "--node", "relative", "x", "1"}, store)
	if code != 2 {
		t.Fatalf("relative node code = %d, want 2", code)
	}
	if !strings.HasPrefix(strings.TrimSpace(stderr), "weld:") {
		t.Fatalf("node stderr = %q, want weld prefix", stderr)
	}
}
