// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package dds

import (
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/podomy/weld/internal/types"
)

// stringFilterDesc is a std_msgs String style descriptor with one field.
func stringFilterDesc() *types.MessageMembers {
	return &types.MessageMembers{
		Namespace:   "std_msgs__msg",
		Name:        "String",
		MemberCount: 1,
		Members: []types.MessageMember{
			{Name: "data", TypeID: types.RosTypeString},
		},
	}
}

// encodeFilterString encodes one string sample as XCDR1.
func encodeFilterString(t *testing.T, s string) []byte {
	t.Helper()

	enc := types.NewEncoder(true)

	err := enc.WriteString(s)
	if err != nil {
		t.Fatalf("encode filter string: %v", err)
	}

	return enc.Bytes()
}

// imuFilterDesc is a sensor_msgs Imu style descriptor with nested messages,
// fixed covariance arrays, and scalar fields after the arrays.
func imuFilterDesc() *types.MessageMembers {
	timeMembers := &types.MessageMembers{
		Namespace:   "builtin_interfaces__msg",
		Name:        "Time",
		MemberCount: 2,
		Members: []types.MessageMember{
			{Name: "sec", TypeID: types.RosTypeInt32},
			{Name: "nanosec", TypeID: types.RosTypeUint32},
		},
	}
	headerMembers := &types.MessageMembers{
		Namespace:   "std_msgs__msg",
		Name:        "Header",
		MemberCount: 2,
		Members: []types.MessageMember{
			{Name: "stamp", TypeID: types.RosTypeMessage, Members: timeMembers},
			{Name: "frame_id", TypeID: types.RosTypeString},
		},
	}
	orient := &types.MessageMember{
		Name:   "orientation",
		TypeID: types.RosTypeMessage,
		Members: &types.MessageMembers{
			Namespace:   "geometry_msgs__msg",
			Name:        "Quaternion",
			MemberCount: 4,
			Members: []types.MessageMember{
				{Name: "x", TypeID: types.RosTypeDouble},
				{Name: "y", TypeID: types.RosTypeDouble},
				{Name: "z", TypeID: types.RosTypeDouble},
				{Name: "w", TypeID: types.RosTypeDouble},
			},
		},
	}
	angular := &types.MessageMember{
		Name:   "angular_velocity",
		TypeID: types.RosTypeMessage,
		Members: &types.MessageMembers{
			Namespace:   "geometry_msgs__msg",
			Name:        "Vector3",
			MemberCount: 3,
			Members: []types.MessageMember{
				{Name: "x", TypeID: types.RosTypeDouble},
				{Name: "y", TypeID: types.RosTypeDouble},
				{Name: "z", TypeID: types.RosTypeDouble},
			},
		},
	}
	linear := &types.MessageMember{
		Name:   "linear_acceleration",
		TypeID: types.RosTypeMessage,
		Members: &types.MessageMembers{
			Namespace:   "geometry_msgs__msg",
			Name:        "Vector3",
			MemberCount: 3,
			Members: []types.MessageMember{
				{Name: "x", TypeID: types.RosTypeDouble},
				{Name: "y", TypeID: types.RosTypeDouble},
				{Name: "z", TypeID: types.RosTypeDouble},
			},
		},
	}

	return &types.MessageMembers{
		Namespace:   "sensor_msgs__msg",
		Name:        "Imu",
		MemberCount: 8,
		Members: []types.MessageMember{
			{Name: "header", TypeID: types.RosTypeMessage, Members: headerMembers},
			*orient,
			{Name: "orientation_covariance", TypeID: types.RosTypeDouble, IsArray: true, ArraySize: 9},
			{Name: "temperature", TypeID: types.RosTypeDouble},
			{Name: "label", TypeID: types.RosTypeString},
			{Name: "ok", TypeID: types.RosTypeBoolean},
			*angular,
			*linear,
		},
	}
}

// encodeFilterImu encodes one Imu style sample as XCDR1 in descriptor order.
//
//nolint:unparam // frameID stays constant in current goldens but callers vary it by design
func encodeFilterImu(t *testing.T, frameID, label string, temp, ox float64, ok bool) []byte {
	t.Helper()

	enc := types.NewEncoder(true)
	enc.WriteInt32(123)
	enc.WriteUint32(456)

	err := enc.WriteString(frameID)
	if err != nil {
		t.Fatalf("encode imu frame: %v", err)
	}

	enc.WriteFloat64(ox)
	enc.WriteFloat64(0)
	enc.WriteFloat64(0)
	enc.WriteFloat64(1)

	for range 9 {
		enc.WriteFloat64(-1)
	}

	enc.WriteFloat64(temp)

	err = enc.WriteString(label)
	if err != nil {
		t.Fatalf("encode imu label: %v", err)
	}

	enc.WriteBool(ok)

	for range 3 {
		enc.WriteFloat64(0)
	}

	for range 3 {
		enc.WriteFloat64(9.81)
	}

	return enc.Bytes()
}

// evalFilter is a test helper compiling expr and evaluating payload.
func evalFilter(t *testing.T, expr string, payload []byte, desc *types.MessageMembers) (bool, error) {
	t.Helper()

	f, err := CompileContentFilter(expr)
	if err != nil {
		t.Fatalf("compile %q: %v", expr, err)
	}

	return f.Evaluate(payload, desc)
}

// TestContentFilterParseMatrix pins grammar coverage and error positions.
func TestContentFilterParseMatrix(t *testing.T) {
	valid := []string{
		"data = 'hello'",
		"data <> 'hello'",
		"data != 'hello'",
		"count < 10",
		"count <= 10",
		"count > 10",
		"count >= 10",
		"a = 1 OR b = 2 AND c = 3",
		"NOT a = 1",
		"NOT NOT a = 1",
		"(a = 1 OR b = 2) AND c = 3",
		"((a = 1))",
		"data LIKE 'hello%'",
		"data NOT LIKE 'hello%'",
		"NOT data LIKE 'x'",
		"temp BETWEEN 1.5 AND 2.5",
		"temp NOT BETWEEN 1 AND 5",
		"NOT temp BETWEEN 1 AND 5",
		"label IN ('a', 'b', 'c')",
		"label NOT IN ('a')",
		"n IN (1, 2, 3)",
		"header.frame_id = 'base_link'",
		"a.b.c.d = 1",
		"ok = TRUE",
		"ok = true",
		"ok <> FALSE",
		"ok",
		"NOT ok",
		"a = -5",
		"a = +5",
		"a = -2.5e3",
		"a = 1E2",
		"data = 'it''s'",
		"a = 1 and b = 2 Or NOT c Between 1 and 2",
		"x = NULL",
	}

	for _, expr := range valid {
		_, err := CompileContentFilter(expr)
		if err != nil {
			t.Errorf("valid %q failed: %v", expr, err)
		}
	}

	invalid := []struct {
		expr string
		hint string
	}{
		{"", "empty"},
		{"   ", "blank"},
		{"data =", "trailing operator"},
		{"= 'x'", "leading operator"},
		{"(data = 'x'", "unclosed paren"},
		{"data = 'x'))", "extra paren"},
		{"data = 'x", "unterminated string"},
		{"data ~ 'x'", "bad character"},
		{"n IN ()", "empty IN"},
		{"n IN (1, 2", "unclosed IN"},
		{"n BETWEEN 1 2", "BETWEEN missing AND"},
		{"n BETWEEN 1 AND", "BETWEEN missing high"},
		{"data = ", "missing operand"},
		{"data .. x = 1", "double dot"},
		{".data = 1", "leading dot"},
		{"data. = 1", "trailing dot"},
		{"data = 1 extra", "trailing input"},
		{"NOT", "bare NOT"},
		{"a = 1 AND", "trailing AND"},
		{"a = 1e", "bad exponent"},
		{"a = -", "sign without number"},
		{"a NOT BOGUS 1", "NOT before junk"},
	}

	for _, tc := range invalid {
		_, err := CompileContentFilter(tc.expr)
		if err == nil {
			t.Errorf("invalid %q (%s) parsed without error", tc.expr, tc.hint)

			continue
		}

		if !errors.Is(err, ErrContentFilterParse) {
			t.Errorf("invalid %q (%s) error %v misses parse sentinel", tc.expr, tc.hint, err)
		}

		var perr *ParseError
		if !errors.As(err, &perr) {
			t.Errorf("invalid %q (%s) error %v misses position", tc.expr, tc.hint, err)

			continue
		}

		if perr.Pos < 0 || perr.Pos > len(tc.expr) {
			t.Errorf("invalid %q (%s) position %d out of range", tc.expr, tc.hint, perr.Pos)
		}

		if !strings.Contains(perr.Error(), "byte") {
			t.Errorf("invalid %q (%s) error text %q names no position", tc.expr, tc.hint, perr.Error())
		}
	}
}

// TestContentFilterPrecedence pins AND over OR plus paren override.
func TestContentFilterPrecedence(t *testing.T) {
	desc := stringFilterDesc()
	hit := encodeFilterString(t, "b")

	// AND binds tighter: true OR (false AND false) is true.
	matched, err := evalFilter(t, "data = 'b' OR data = 'x' AND data = 'y'", hit, desc)
	if err != nil {
		t.Fatalf("precedence eval: %v", err)
	}

	if !matched {
		t.Fatalf("AND-over-OR precedence gave miss, want match")
	}

	// Parens override: (true OR false) AND false is false.
	matched, err = evalFilter(t, "(data = 'b' OR data = 'x') AND data = 'y'", hit, desc)
	if err != nil {
		t.Fatalf("paren eval: %v", err)
	}

	if matched {
		t.Fatalf("paren override gave match, want miss")
	}

	// NOT binds tightest: NOT false AND false is (NOT false) AND false.
	matched, err = evalFilter(t, "NOT data = 'x' AND data = 'y'", hit, desc)
	if err != nil {
		t.Fatalf("not precedence eval: %v", err)
	}

	if matched {
		t.Fatalf("NOT precedence gave match, want miss")
	}
}

// TestContentFilterStringGoldens pins string operator behavior.
func TestContentFilterStringGoldens(t *testing.T) {
	desc := stringFilterDesc()
	hello := encodeFilterString(t, "hello-world")
	other := encodeFilterString(t, "goodbye")

	cases := []struct {
		expr  string
		want  bool
		pay   []byte
		label string
	}{
		{"data = 'hello-world'", true, hello, "eq match"},
		{"data = 'goodbye'", false, hello, "eq miss"},
		{"data <> 'goodbye'", true, hello, "ne match"},
		{"data < 'hello-world'", false, hello, "lt equal"},
		{"data < 'hellp'", true, hello, "lt match"},
		{"data >= 'hello-world'", true, hello, "ge equal"},
		{"data LIKE 'hello%'", true, hello, "like prefix"},
		{"data LIKE '%o-w%ld'", true, hello, "like infixes"},
		{"data LIKE 'hello_world'", true, hello, "like underscore"},
		{"data LIKE 'hello'", false, hello, "like full miss"},
		{"data LIKE '%'", true, hello, "like star all"},
		{"data NOT LIKE 'good%'", true, hello, "not like match"},
		{"data IN ('a', 'hello-world')", true, hello, "in match"},
		{"data IN ('a', 'b')", false, hello, "in miss"},
		{"data NOT IN ('a', 'b')", true, hello, "not in match"},
		{"data BETWEEN 'a' AND 'z'", true, hello, "between strings"},
		{"data BETWEEN 'z' AND 'zz'", false, hello, "between miss"},
		{"NOT data = 'goodbye'", true, hello, "not miss operand"},
		{"data = 'goodbye'", true, other, "other payload"},
		{"data = ''", false, hello, "empty literal miss"},
	}

	for _, tc := range cases {
		matched, err := evalFilter(t, tc.expr, tc.pay, desc)
		if err != nil {
			t.Errorf("%s: eval %q: %v", tc.label, tc.expr, err)

			continue
		}

		if matched != tc.want {
			t.Errorf("%s: eval %q = %v, want %v", tc.label, tc.expr, matched, tc.want)
		}
	}
}

// TestContentFilterImuGoldens pins nested paths plus array skipping.
func TestContentFilterImuGoldens(t *testing.T) {
	desc := imuFilterDesc()
	payload := encodeFilterImu(t, "base_link", "imu_front", 21.5, 0.7, true)

	cases := []struct {
		expr  string
		want  bool
		label string
	}{
		{"header.frame_id = 'base_link'", true, "nested string"},
		{"header.stamp.sec = 123", true, "deep int"},
		{"header.stamp.nanosec = 456", true, "deep uint"},
		{"orientation.x > 0.5", true, "nested float"},
		{"orientation.x BETWEEN -1.0 AND 1.0", true, "between floats"},
		{"temperature > 20.0 AND label LIKE 'imu%'", true, "and like"},
		{"temperature < 20.0 OR ok = TRUE", true, "or bool"},
		{"ok", true, "bare bool"},
		{"NOT ok", false, "not bool"},
		{"label IN ('imu_front', 'imu_rear')", true, "in strings"},
		{"linear_acceleration.z = 9.81", true, "field after bool"},
		{"angular_velocity.y = 0", true, "integer literal for float"},
		{"temperature > 100.0", false, "numeric miss"},
		{"header.frame_id = 'odom'", false, "nested miss"},
		{"orientation.x > 0.5 AND header.frame_id = 'odom'", false, "and miss"},
		{"temperature NOT BETWEEN 20.0 AND 22.0", false, "not between miss"},
	}

	for _, tc := range cases {
		matched, err := evalFilter(t, tc.expr, payload, desc)
		if err != nil {
			t.Errorf("%s: eval %q: %v", tc.label, tc.expr, err)

			continue
		}

		if matched != tc.want {
			t.Errorf("%s: eval %q = %v, want %v", tc.label, tc.expr, matched, tc.want)
		}
	}

	// The label read sits after the 9 element covariance array, so a match
	// here proves the array skip kept later offsets exact.
	matched, err := evalFilter(t, "label = 'imu_front'", payload, desc)
	if err != nil {
		t.Fatalf("post-array field eval: %v", err)
	}

	if !matched {
		t.Fatalf("post-array field gave miss, want match")
	}
}

// TestContentFilterEvalErrors pins eval failures for bad fields and types.
func TestContentFilterEvalErrors(t *testing.T) {
	desc := imuFilterDesc()
	payload := encodeFilterImu(t, "base_link", "imu_front", 21.5, 0.7, true)

	badExprs := []struct {
		expr  string
		label string
	}{
		{"nosuchfield = 1", "unknown field"},
		{"header.nosuchfield = 1", "unknown nested field"},
		{"orientation_covariance = 1", "array field"},
		{"label > 3", "string versus number"},
		{"temperature = 'hot'", "number versus string"},
		{"ok > TRUE", "bool ordering"},
		{"label = TRUE", "string versus bool"},
		{"temperature = NULL", "null comparison"},
		{"label LIKE 5", "like non-string pattern"},
		{"temperature LIKE '2%'", "like non-string field"},
		{"temperature", "bare number predicate"},
		{"label", "bare string predicate"},
		{"label IN ('a', 5)", "in mixed list"},
	}

	for _, tc := range badExprs {
		f, err := CompileContentFilter(tc.expr)
		if err != nil {
			t.Errorf("%s: compile %q: %v", tc.label, tc.expr, err)

			continue
		}

		_, err = f.Evaluate(payload, desc)
		if err == nil {
			t.Errorf("%s: eval %q succeeded, want eval error", tc.label, tc.expr)

			continue
		}

		if !errors.Is(err, ErrContentFilterEvalError) {
			t.Errorf("%s: eval %q error %v misses eval sentinel", tc.label, tc.expr, err)
		}
	}

	// Truncated payloads yield eval errors, never success.
	for _, cut := range []int{0, 1, 4, 5, 10} {
		f, err := CompileContentFilter("temperature > 1.0")
		if err != nil {
			t.Fatalf("compile truncated probe: %v", err)
		}

		_, err = f.Evaluate(payload[:cut], desc)
		if err == nil {
			t.Errorf("truncated len %d evaluated without error", cut)
		}
	}

	// Nil descriptor and nil filter are eval errors.
	f, err := CompileContentFilter("ok = TRUE")
	if err != nil {
		t.Fatalf("compile nil probe: %v", err)
	}

	_, err = f.Evaluate(payload, nil)
	if err == nil {
		t.Errorf("nil descriptor evaluated without error")
	}

	var nilFilter *ContentFilter

	_, err = nilFilter.Evaluate(payload, desc)
	if err == nil {
		t.Errorf("nil filter evaluated without error")
	}
}

// TestContentFilterApply pins drop reason classification.
//
//nolint:cyclop // test pins match, miss, and eval-error classification in one run, splitting would obscure intent
func TestContentFilterApply(t *testing.T) {
	desc := stringFilterDesc()
	hello := encodeFilterString(t, "hello")

	f, err := CompileContentFilter("data LIKE 'hello%'")
	if err != nil {
		t.Fatalf("compile apply filter: %v", err)
	}

	matched, reason, err := f.Apply(hello, desc)
	if err != nil {
		t.Fatalf("apply match: %v", err)
	}

	if !matched || reason != "" {
		t.Fatalf("apply match = %v reason %q, want true plus empty", matched, reason)
	}

	other := encodeFilterString(t, "goodbye")

	matched, reason, err = f.Apply(other, desc)
	if err == nil {
		t.Fatalf("apply miss returned nil error")
	}

	if matched || reason != DropReasonContentFilterMiss {
		t.Fatalf("apply miss = %v reason %q, want miss reason", matched, reason)
	}

	if !errors.Is(err, ErrContentFilterMiss) {
		t.Fatalf("apply miss error %v misses miss sentinel", err)
	}

	matched, reason, err = f.Apply(other[:3], desc)
	if err == nil {
		t.Fatalf("apply truncated returned nil error")
	}

	if matched || reason != DropReasonContentFilterEvalError {
		t.Fatalf("apply truncated = %v reason %q, want eval error reason", matched, reason)
	}

	if !errors.Is(err, ErrContentFilterEvalError) {
		t.Fatalf("apply truncated error %v misses eval sentinel", err)
	}
}

// TestContentFilterConcurrent pins parallel evaluation without races.
func TestContentFilterConcurrent(t *testing.T) {
	desc := imuFilterDesc()
	payload := encodeFilterImu(t, "base_link", "imu_front", 21.5, 0.7, true)

	f, err := CompileContentFilter("temperature > 20.0 AND header.frame_id LIKE 'base%'")
	if err != nil {
		t.Fatalf("compile concurrent filter: %v", err)
	}

	var wg sync.WaitGroup

	for range 8 {
		wg.Go(func() {
			for range 200 {
				matched, err := f.Evaluate(payload, desc)
				if err != nil {
					t.Errorf("concurrent eval: %v", err)

					return
				}

				if !matched {
					t.Errorf("concurrent eval gave miss, want match")

					return
				}
			}
		})
	}

	wg.Wait()
}

// TestContentFilterTruncatedSweep proves no panic over every truncation.
func TestContentFilterTruncatedSweep(t *testing.T) {
	desc := imuFilterDesc()
	full := encodeFilterImu(t, "base_link", "imu_front", 21.5, 0.7, true)

	f, err := CompileContentFilter("temperature > 20.0 AND label LIKE 'imu%' AND ok = TRUE")
	if err != nil {
		t.Fatalf("compile sweep filter: %v", err)
	}

	matched, err := f.Evaluate(full, desc)
	if err != nil {
		t.Fatalf("full payload eval: %v", err)
	}

	if !matched {
		t.Fatalf("full payload gave miss, want match")
	}

	// Trailing pad is at most 3 bytes, so every shorter cut removes field
	// bytes and must fail evaluation instead of matching or panicking.
	for cut := range full {
		cutPayload := full[:cut]

		_, err = f.Evaluate(cutPayload, desc)
		if cut < len(full)-3 && err == nil {
			t.Fatalf("truncation len %d evaluated without error", cut)
		}
	}

	// Deterministic corruption sweep: flip bytes with an LCG and require no
	// panic. Corrupt samples may still evaluate when the wire stays valid.
	state := uint32(0x12345678)

	for range 300 {
		corrupt := append([]byte(nil), full...)

		for range 4 {
			state = state*1664525 + 1013904223
			corrupt[int(state>>16)%len(corrupt)] ^= byte(state >> 8) //nolint:gosec // G115: LCG byte extraction truncates intentionally for corruption sweep
		}

		if _, err := f.Evaluate(corrupt, desc); err != nil {
			continue
		}
	}
}
