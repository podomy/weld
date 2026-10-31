// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package dds

import (
	"bytes"
	"strings"
	"testing"

	"github.com/podomy/weld/internal/transports/dds/rtps"
)

// TestPartitionValidateName pins the allowed charset and length rules.
func TestPartitionValidateName(t *testing.T) {
	t.Parallel()

	valid := []string{
		"",
		"A",
		"sensor_data",
		"with space",
		"with/slash",
		"with.dot",
		"with-dash",
		"sensor_*",
		"sensor_?",
		"a[bc]",
		`back\slash`,
		strings.Repeat("x", MaxPartitionNameLen),
	}

	for _, name := range valid {
		err := ValidatePartitionName(name)
		if err != nil {
			t.Errorf("ValidatePartitionName(%q) = %v, want nil", name, err)
		}
	}

	invalid := map[string]string{
		"has,comma":    "reserved comma",
		"has\x00nul":   "NUL terminator",
		"has\nnewline": "control byte",
		"has\ttab":     "control byte",
		"has\x7fdel":   "DEL byte",
		"caf\xc3\xa9":  "non-ASCII bytes",
		strings.Repeat("y", MaxPartitionNameLen+1): "overlong",
	}

	for name, why := range invalid {
		err := ValidatePartitionName(name)
		if err == nil {
			t.Errorf("ValidatePartitionName(%q) = nil, want error for %s", name, why)
		}
	}
}

// TestPartitionValidateList pins count and cumulative length caps.
func TestPartitionValidateList(t *testing.T) {
	t.Parallel()

	err := ValidatePartitionList(nil)
	if err != nil {
		t.Fatalf("ValidatePartitionList(nil) = %v, want nil", err)
	}

	err = ValidatePartitionList([]string{"", "a"})
	if err != nil {
		t.Fatalf("ValidatePartitionList default plus one = %v, want nil", err)
	}

	many := make([]string, MaxPartitionCount+1)
	for i := range many {
		many[i] = "a"
	}
	err = ValidatePartitionList(many)
	if err == nil {
		t.Fatalf("ValidatePartitionList(%d names) = nil, want count error", len(many))
	}

	big := []string{strings.Repeat("a", 200), strings.Repeat("b", 100)}
	err = ValidatePartitionList(big)
	if err == nil {
		t.Fatalf("ValidatePartitionList cumulative 300 = nil, want cumulative error")
	}

	bad := []string{"ok", "has,comma"}
	err = ValidatePartitionList(bad)
	if err == nil {
		t.Fatalf("ValidatePartitionList with comma = nil, want error")
	}
}

// TestPartitionPatternMatch pins * and ? semantics plus literal brackets.
func TestPartitionPatternMatch(t *testing.T) {
	t.Parallel()

	cases := []struct {
		pattern string
		name    string
		want    bool
	}{
		{"abc", "abc", true},
		{"abc", "abd", false},
		{"abc", "abcd", false},
		{"ABC", "abc", false},
		{"*", "anything", true},
		{"*", "", true},
		{"", "", true},
		{"", "a", false},
		{"sensor_*", "sensor_data", true},
		{"sensor_*", "sensor_", true},
		{"sensor_*", "other_data", false},
		{"*_data", "sensor_data", true},
		{"s*o*d*a", "sensor_data", true},
		{"?", "a", true},
		{"?", "", false},
		{"?", "ab", false},
		{"ab?", "abc", true},
		{"ab?", "ab", false},
		{"a?c", "abc", true},
		{"a[bc]", "a[bc]", true},
		{"a[bc]", "ab", false},
		{`a\b`, `a\b`, true},
		{`a\b`, "ab", false},
	}

	for _, tc := range cases {
		got := PartitionPatternMatch(tc.pattern, tc.name)
		if got != tc.want {
			t.Errorf("PartitionPatternMatch(%q, %q) = %v, want %v", tc.pattern, tc.name, got, tc.want)
		}
	}
}

// TestPartitionMatchMatrix pins empty-vs-default and multi-partition rules.
func TestPartitionMatchMatrix(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name      string
		requested []string
		offered   []string
		want      bool
	}{
		{"empty meets empty on default", nil, nil, true},
		{"empty meets explicit default", nil, []string{""}, true},
		{"explicit default meets empty", []string{""}, nil, true},
		{"empty misses named", nil, []string{"A"}, false},
		{"named misses empty", []string{"A"}, nil, false},
		{"same single", []string{"A"}, []string{"A"}, true},
		{"different single", []string{"A"}, []string{"B"}, false},
		{"requested subset of offered", []string{"A"}, []string{"A", "B"}, true},
		{"offered subset of requested", []string{"A", "B"}, []string{"B"}, true},
		{"disjoint multi", []string{"A", "B"}, []string{"C", "D"}, false},
		{"case sensitive", []string{"a"}, []string{"A"}, false},
		{"requested pattern meets offered literal", []string{"sensor_*"}, []string{"sensor_data"}, true},
		{"requested literal meets offered pattern", []string{"sensor_data"}, []string{"sensor_*"}, true},
		{"pattern misses literal", []string{"other_*"}, []string{"sensor_data"}, false},
		{"star meets default", []string{"*"}, []string{""}, true},
		{"star meets named", []string{"*"}, []string{"B"}, true},
		{"question meets one char", []string{"?"}, []string{"B"}, true},
		{"question misses empty", []string{"?"}, []string{""}, false},
	}

	for _, tc := range cases {
		got := PartitionsMatch(tc.requested, tc.offered)
		if got != tc.want {
			t.Errorf("%s: PartitionsMatch(%q, %q) = %v, want %v", tc.name, tc.requested, tc.offered, got, tc.want)
		}
	}
}

// TestPartitionMatchOutcome pins the named mismatch reason shape.
func TestPartitionMatchOutcome(t *testing.T) {
	t.Parallel()

	hit := MatchPartitions([]string{"B"}, []string{"B"})
	if !hit.Matched || hit.Reason != PartitionMismatchNone {
		t.Fatalf("match = %+v, want matched with empty reason", hit)
	}

	miss := MatchPartitions([]string{"B"}, []string{"A"})
	if miss.Matched {
		t.Fatalf("mismatch matched, want no match")
	}
	if miss.Reason != PartitionMismatchNoCommonPartition {
		t.Fatalf("reason = %q, want %q", miss.Reason, PartitionMismatchNoCommonPartition)
	}
	if !strings.Contains(miss.Detail, `"A"`) || !strings.Contains(miss.Detail, `"B"`) {
		t.Fatalf("detail = %q, want both normalized lists", miss.Detail)
	}

	def := MatchPartitions(nil, nil)
	if !def.Matched {
		t.Fatalf("default match = %+v, want matched", def)
	}
}

// TestPartitionEncodeGolden pins the Cyclone-compatible parameter bytes.
func TestPartitionEncodeGolden(t *testing.T) {
	t.Parallel()

	got, err := EncodePartitionQoS([]string{"A"})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	want := []byte{
		0x01, 0x00, 0x00, 0x00,
		0x04, 0x00, 0x00, 0x00,
		0x41, 0x00, 0x00, 0x00,
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("encode [A] = % X, want % X", got, want)
	}

	got, err = EncodePartitionQoS([]string{"ab", "c*"})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	want = []byte{
		0x02, 0x00, 0x00, 0x00,
		0x04, 0x00, 0x00, 0x00,
		0x61, 0x62, 0x00, 0x00,
		0x04, 0x00, 0x00, 0x00,
		0x63, 0x2a, 0x00, 0x00,
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("encode [ab c*] = % X, want % X", got, want)
	}

	got, err = EncodePartitionQoS(nil)
	if err != nil {
		t.Fatalf("encode nil: %v", err)
	}
	if !bytes.Equal(got, []byte{0x00, 0x00, 0x00, 0x00}) {
		t.Fatalf("encode nil = % X, want zero count", got)
	}

	if _, err = EncodePartitionQoS([]string{"has,comma"}); err == nil {
		t.Fatalf("encode comma = nil, want validation error")
	}
}

// TestPartitionDecodeRoundTrip pins decode of both pad conventions.
//
//nolint:gocognit,cyclop // test pins pad conventions and malformed values in one run, splitting would obscure intent
func TestPartitionDecodeRoundTrip(t *testing.T) {
	t.Parallel()

	lists := [][]string{
		nil,
		{""},
		{"A"},
		{"ab", "c*"},
		{"sensor_data", "other"},
	}

	for _, list := range lists {
		raw, err := EncodePartitionQoS(list)
		if err != nil {
			t.Fatalf("encode %q: %v", list, err)
		}
		back, err := DecodePartitionQoS(raw)
		if err != nil {
			t.Fatalf("decode %q: %v", list, err)
		}
		if len(list) == 0 && len(back) != 0 {
			t.Fatalf("round trip %q gave %q", list, back)
		}
		for i := range list {
			if back[i] != list[i] {
				t.Fatalf("round trip %q gave %q", list, back)
			}
		}
	}

	// Standard excludes-pad layout for "abcd" (length 5, external pad 3).
	standard := []byte{
		0x01, 0x00, 0x00, 0x00,
		0x05, 0x00, 0x00, 0x00,
		0x61, 0x62, 0x63, 0x64, 0x00, 0x00, 0x00, 0x00,
	}
	back, err := DecodePartitionQoS(standard)
	if err != nil {
		t.Fatalf("decode standard layout: %v", err)
	}
	if len(back) != 1 || back[0] != "abcd" {
		t.Fatalf("decode standard layout = %q, want [abcd]", back)
	}

	bad := map[string][]byte{
		"short count":    {0x01, 0x00},
		"missing NUL":    {0x01, 0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x41, 0x42, 0x43, 0x44},
		"zero length":    {0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
		"truncated":      {0x01, 0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x41},
		"trailing bytes": {0x00, 0x00, 0x00, 0x00, 0xFF},
		"count over cap": {0x41, 0x00, 0x00, 0x00},
		"comma name":     {0x01, 0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x61, 0x2c, 0x00, 0x00},
	}

	for name, raw := range bad {
		_, err := DecodePartitionQoS(raw)
		if err == nil {
			t.Errorf("decode %s = nil, want error", name)
		}
	}
}

// TestPartitionParamRoundTrip pins the PID through the shared RTPS codec.
func TestPartitionParamRoundTrip(t *testing.T) {
	t.Parallel()

	param, err := PartitionParam([]string{"A", "B"})
	if err != nil {
		t.Fatalf("param: %v", err)
	}
	if param.ID != PIDPartition {
		t.Fatalf("param id = %#04x, want %#04x", param.ID, PIDPartition)
	}
	if PIDPartition != 0x0029 {
		t.Fatalf("PIDPartition = %#04x, want DDSI-RTPS PID_PARTITION 0x0029", PIDPartition)
	}

	raw := rtps.EncodeParams([]rtps.Param{param})
	back, err := rtps.DecodeParams(raw)
	if err != nil {
		t.Fatalf("rtps round trip: %v", err)
	}

	names, err := FindPartitionParam(back)
	if err != nil {
		t.Fatalf("find: %v", err)
	}
	if len(names) != 2 || names[0] != "A" || names[1] != "B" {
		t.Fatalf("find = %q, want [A B]", names)
	}

	absent, err := FindPartitionParam(nil)
	if err != nil || absent != nil {
		t.Fatalf("find absent = %q, %v, want nil, nil", absent, err)
	}
}

// TestPartitionRouteKey pins discovery key isolation across partitions.
func TestPartitionRouteKey(t *testing.T) {
	t.Parallel()

	base := PartitionRouteKey("rt/chatter", nil)
	def := PartitionRouteKey("rt/chatter", []string{""})
	if base != def {
		t.Fatalf("empty vs explicit default differ: %q vs %q", base, def)
	}

	other := PartitionRouteKey("rt/chatter", []string{"A"})
	if base == other {
		t.Fatalf("default and partition A share key %q", base)
	}

	second := PartitionRouteKey("rt/chatter", []string{"B"})
	if other == second {
		t.Fatalf("partitions A and B share key %q", other)
	}

	reordered := PartitionRouteKey("rt/chatter", []string{"B", "A"})
	ordered := PartitionRouteKey("rt/chatter", []string{"A", "B"})
	if reordered != ordered {
		t.Fatalf("order changes key: %q vs %q", reordered, ordered)
	}

	dup := PartitionRouteKey("rt/chatter", []string{"A", "A"})
	if dup != other {
		t.Fatalf("duplicate names change key: %q vs %q", dup, other)
	}

	topic := PartitionRouteKey("rt/other", []string{"A"})
	if topic == other {
		t.Fatalf("different topics share key %q", other)
	}
}
