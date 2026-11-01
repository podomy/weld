// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package core

import (
	"errors"
	"fmt"
	"maps"
	"math"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

// configWatchBuffer sizes each watcher channel. Broadcast never blocks:
// a full channel evicts its oldest event before the new one is queued,
// and the evicted range stays recoverable through HistorySince.
// Evictions and drops increment BroadcastDrops loudly so overflow is never silent.
const configWatchBuffer = 16

// configHistoryLimit bounds retained history to the last entries.
// Overflow drops the oldest entries and counts each drop in HistoryDrops
// so long runs stay bounded while remaining observable.
const configHistoryLimit = 10000

// ParamType names the kind of a parameter value.
type ParamType int

// Parameter types.
const (
	// ParamTypeNotSet means no value. Set rejects it; declare accepts it
	// only with a NotSet descriptor.
	ParamTypeNotSet ParamType = iota
	// ParamTypeBool holds a boolean.
	ParamTypeBool
	// ParamTypeInteger holds an int64.
	ParamTypeInteger
	// ParamTypeDouble holds a float64.
	ParamTypeDouble
	// ParamTypeString holds a string.
	ParamTypeString
	// ParamTypeByteArray holds opaque bytes.
	ParamTypeByteArray
	// ParamTypeBoolArray holds booleans with uniform typing.
	ParamTypeBoolArray
	// ParamTypeIntegerArray holds int64 values with uniform typing.
	ParamTypeIntegerArray
	// ParamTypeDoubleArray holds float64 values with uniform typing.
	ParamTypeDoubleArray
	// ParamTypeStringArray holds strings with uniform typing.
	ParamTypeStringArray
)

// String renders the type in lowercase form.
func (t ParamType) String() string {
	switch t {
	case ParamTypeNotSet:
		return "not_set"
	case ParamTypeBool:
		return "bool"
	case ParamTypeInteger:
		return "integer"
	case ParamTypeDouble:
		return "double"
	case ParamTypeString:
		return "string"
	case ParamTypeByteArray:
		return "byte_array"
	case ParamTypeBoolArray:
		return "bool_array"
	case ParamTypeIntegerArray:
		return "integer_array"
	case ParamTypeDoubleArray:
		return "double_array"
	case ParamTypeStringArray:
		return "string_array"
	default:
		return "invalid"
	}
}

// ParamValue is one typed parameter value.
//
// The zero value carries ParamTypeNotSet. Slices are owned: the store
// clones on entry and on exit so caller mutation cannot alias internals.
type ParamValue struct {
	// Type names the active field.
	Type ParamType
	// Bool holds the value when Type is ParamTypeBool.
	Bool bool
	// Integer holds the value when Type is ParamTypeInteger.
	Integer int64
	// Double holds the value when Type is ParamTypeDouble.
	Double float64
	// String holds the value when Type is ParamTypeString.
	String string
	// Bytes holds the value when Type is ParamTypeByteArray.
	Bytes []byte
	// Bools holds the value when Type is ParamTypeBoolArray.
	Bools []bool
	// Integers holds the value when Type is ParamTypeIntegerArray.
	Integers []int64
	// Doubles holds the value when Type is ParamTypeDoubleArray.
	Doubles []float64
	// Strings holds the value when Type is ParamTypeStringArray.
	Strings []string
}

// NewBoolParam returns a boolean parameter value.
func NewBoolParam(v bool) ParamValue {
	return ParamValue{Type: ParamTypeBool, Bool: v}
}

// NewIntegerParam returns an integer parameter value.
func NewIntegerParam(v int64) ParamValue {
	return ParamValue{Type: ParamTypeInteger, Integer: v}
}

// NewDoubleParam returns a double parameter value.
func NewDoubleParam(v float64) ParamValue {
	return ParamValue{Type: ParamTypeDouble, Double: v}
}

// NewStringParam returns a string parameter value.
func NewStringParam(v string) ParamValue {
	return ParamValue{Type: ParamTypeString, String: v}
}

// NewByteArrayParam returns a byte array parameter value with a copy.
func NewByteArrayParam(v []byte) ParamValue {
	out := make([]byte, len(v))
	copy(out, v)
	return ParamValue{Type: ParamTypeByteArray, Bytes: out}
}

// NewBoolArrayParam returns a boolean array parameter value with a copy.
func NewBoolArrayParam(v []bool) ParamValue {
	out := make([]bool, len(v))
	copy(out, v)
	return ParamValue{Type: ParamTypeBoolArray, Bools: out}
}

// NewIntegerArrayParam returns an integer array parameter value with a copy.
func NewIntegerArrayParam(v []int64) ParamValue {
	out := make([]int64, len(v))
	copy(out, v)
	return ParamValue{Type: ParamTypeIntegerArray, Integers: out}
}

// NewDoubleArrayParam returns a double array parameter value with a copy.
func NewDoubleArrayParam(v []float64) ParamValue {
	out := make([]float64, len(v))
	copy(out, v)
	return ParamValue{Type: ParamTypeDoubleArray, Doubles: out}
}

// NewStringArrayParam returns a string array parameter value with a copy.
func NewStringArrayParam(v []string) ParamValue {
	out := make([]string, len(v))
	copy(out, v)
	return ParamValue{Type: ParamTypeStringArray, Strings: out}
}

// Equal reports whether a and b carry the same type and value.
//
//nolint:gocognit,cyclop // equality spans all scalar plus array forms in one comparison, splitting would obscure intent
func (a ParamValue) Equal(b ParamValue) bool {
	if a.Type != b.Type {
		return false
	}
	switch a.Type {
	case ParamTypeNotSet:
		return true
	case ParamTypeBool:
		return a.Bool == b.Bool
	case ParamTypeInteger:
		return a.Integer == b.Integer
	case ParamTypeDouble:
		return a.Double == b.Double
	case ParamTypeString:
		return a.String == b.String
	case ParamTypeByteArray:
		if len(a.Bytes) != len(b.Bytes) {
			return false
		}
		for i := range a.Bytes {
			if a.Bytes[i] != b.Bytes[i] {
				return false
			}
		}
		return true
	case ParamTypeBoolArray:
		if len(a.Bools) != len(b.Bools) {
			return false
		}
		for i := range a.Bools {
			if a.Bools[i] != b.Bools[i] {
				return false
			}
		}
		return true
	case ParamTypeIntegerArray:
		if len(a.Integers) != len(b.Integers) {
			return false
		}
		for i := range a.Integers {
			if a.Integers[i] != b.Integers[i] {
				return false
			}
		}
		return true
	case ParamTypeDoubleArray:
		if len(a.Doubles) != len(b.Doubles) {
			return false
		}
		for i := range a.Doubles {
			if a.Doubles[i] != b.Doubles[i] {
				return false
			}
		}
		return true
	case ParamTypeStringArray:
		if len(a.Strings) != len(b.Strings) {
			return false
		}
		for i := range a.Strings {
			if a.Strings[i] != b.Strings[i] {
				return false
			}
		}
		return true
	default:
		return false
	}
}

// cloneParamValue returns a deep copy so slices never alias.
func cloneParamValue(v ParamValue) ParamValue {
	switch v.Type { //nolint:exhaustive // default safely returns scalar and NotSet values unchanged with no slices to copy
	case ParamTypeByteArray:
		out := make([]byte, len(v.Bytes))
		copy(out, v.Bytes)
		v.Bytes = out
	case ParamTypeBoolArray:
		out := make([]bool, len(v.Bools))
		copy(out, v.Bools)
		v.Bools = out
	case ParamTypeIntegerArray:
		out := make([]int64, len(v.Integers))
		copy(out, v.Integers)
		v.Integers = out
	case ParamTypeDoubleArray:
		out := make([]float64, len(v.Doubles))
		copy(out, v.Doubles)
		v.Doubles = out
	case ParamTypeStringArray:
		out := make([]string, len(v.Strings))
		copy(out, v.Strings)
		v.Strings = out
	default:
	}
	return v
}

// ParamDescriptor describes one parameter: its type, documentation, and
// optional numeric range. A NotSet type means unconstrained: any concrete
// value validates.
type ParamDescriptor struct {
	// Type is the required value type, or NotSet for unconstrained.
	Type ParamType
	// Description documents the parameter for describe output.
	Description string
	// ReadOnly rejects Set after Declare while still allowing Declare.
	ReadOnly bool
	// HasIntegerRange enables IntegerMin and IntegerMax for integers.
	HasIntegerRange bool
	// IntegerMin is the inclusive lower bound for integers.
	IntegerMin int64
	// IntegerMax is the inclusive upper bound for integers.
	IntegerMax int64
	// HasDoubleRange enables DoubleMin and DoubleMax for doubles.
	HasDoubleRange bool
	// DoubleMin is the inclusive lower bound for doubles.
	DoubleMin float64
	// DoubleMax is the inclusive upper bound for doubles.
	DoubleMax float64
}

// isValidParamType reports whether t is a concrete descriptor or value type.
func isValidParamType(t ParamType) bool {
	switch t {
	case ParamTypeNotSet,
		ParamTypeBool,
		ParamTypeInteger,
		ParamTypeDouble,
		ParamTypeString,
		ParamTypeByteArray,
		ParamTypeBoolArray,
		ParamTypeIntegerArray,
		ParamTypeDoubleArray,
		ParamTypeStringArray:
		return true
	default:
		return false
	}
}

// validateValue checks type and range without touching read-only state.
// Read-only is enforced by the store, which knows whether the entry is new.
// A NotSet value validates only against a NotSet descriptor, so legal stored
// NotSet state passes its own schema validation. Invalid enum members fail
// loudly. NaN fails an enabled double range explicitly because comparisons
// never match NaN.
func (d ParamDescriptor) validateValue(v ParamValue) error {
	if !isValidParamType(v.Type) {
		return fmt.Errorf("core: config: value type %d is invalid", int(v.Type))
	}
	if !isValidParamType(d.Type) {
		return fmt.Errorf("core: config: descriptor type %d is invalid", int(d.Type))
	}
	if v.Type == ParamTypeNotSet {
		if d.Type == ParamTypeNotSet {
			return nil
		}
		return errors.New("core: config: value type not_set is not concrete") //nolint:wrapcheck // new error, nothing to wrap
	}
	if d.Type != ParamTypeNotSet && v.Type != d.Type {
		return fmt.Errorf("core: config: type mismatch: have %s, want %s", v.Type, d.Type)
	}
	if err := d.checkIntegerRange(v); err != nil {
		return err
	}
	if err := d.checkDoubleRange(v); err != nil {
		return err
	}
	return nil
}

// checkIntegerRange enforces an enabled integer range on one value.
func (d ParamDescriptor) checkIntegerRange(v ParamValue) error {
	if v.Type != ParamTypeInteger || !d.HasIntegerRange {
		return nil
	}
	if v.Integer < d.IntegerMin || v.Integer > d.IntegerMax {
		return fmt.Errorf("core: config: integer %d out of range [%d, %d]", v.Integer, d.IntegerMin, d.IntegerMax)
	}
	return nil
}

// checkDoubleRange enforces an enabled double range on one value.
//
// NaN fails explicitly because comparisons never match NaN.
func (d ParamDescriptor) checkDoubleRange(v ParamValue) error {
	if v.Type != ParamTypeDouble || !d.HasDoubleRange {
		return nil
	}
	if math.IsNaN(v.Double) {
		return fmt.Errorf("core: config: double NaN is not in range [%v, %v]", d.DoubleMin, d.DoubleMax)
	}
	if v.Double < d.DoubleMin || v.Double > d.DoubleMax {
		return fmt.Errorf("core: config: double %v out of range [%v, %v]", v.Double, d.DoubleMin, d.DoubleMax)
	}
	return nil
}

// ParamSchema maps parameter names to their descriptors for pre-launch
// validation. Keys are final (post-remap) dotted names.
type ParamSchema map[string]ParamDescriptor

// ValidateValues checks raw values against the schema before launch without
// touching a store. It reports missing entries, unknown entries, and type
// or range violations joined with semicolons.
func (s ParamSchema) ValidateValues(values map[string]ParamValue) error {
	var problems []string
	names := make([]string, 0, len(values))
	for name := range values {
		names = append(names, name)
	}
	sort.Strings(names)
	schemaNames := make([]string, 0, len(s))
	for name := range s {
		schemaNames = append(schemaNames, name)
	}
	sort.Strings(schemaNames)
	for _, name := range schemaNames {
		desc := s[name]
		v, ok := values[name]
		if !ok {
			problems = append(problems, "missing parameter "+name)
			continue
		}
		err := desc.validateValue(v)
		if err != nil {
			problems = append(problems, name+": "+err.Error())
		}
	}
	for _, name := range names {
		if _, ok := s[name]; !ok {
			problems = append(problems, "unknown parameter "+name)
		}
	}
	if len(problems) == 0 {
		return nil
	}
	return errors.New("core: config: schema: " + strings.Join(problems, "; ")) //nolint:wrapcheck // joined new error, nothing to wrap
}

// RemapRule rewrites one parameter name to another at creation time.
type RemapRule struct {
	// From is the input name.
	From string
	// To is the rewritten name.
	To string
}

// ValidateParamName checks dotted parameter name shape: non-empty, at most
// 255 bytes, dot-separated segments each matching [A-Za-z_][A-Za-z0-9_]*.
func ValidateParamName(name string) error {
	if name == "" {
		return errors.New("core: config: parameter name is empty") //nolint:wrapcheck // new error, nothing to wrap
	}
	if len(name) > 255 {
		return fmt.Errorf("core: config: parameter name %q exceeds 255 bytes", name)
	}
	if strings.HasPrefix(name, ".") || strings.HasSuffix(name, ".") || strings.Contains(name, "..") {
		return fmt.Errorf("core: config: parameter name %q has empty segment", name)
	}
	for segment := range strings.SplitSeq(name, ".") {
		err := validateParamSegment(segment)
		if err != nil {
			return fmt.Errorf("core: config: parameter name %q: %w", name, err)
		}
	}
	return nil
}

// validateParamSegment checks one dot-separated segment.
func validateParamSegment(segment string) error {
	if segment == "" {
		return errors.New("core: config: empty name segment") //nolint:wrapcheck // new error, nothing to wrap
	}
	for i := range len(segment) {
		c := segment[i]
		ok := c == '_' || (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (i > 0 && c >= '0' && c <= '9')
		if !ok {
			return fmt.Errorf("core: config: bad character %q in segment %q", c, segment)
		}
	}
	return nil
}

// ValidateRemapRule checks that both ends are valid parameter names.
func ValidateRemapRule(r RemapRule) error {
	err := ValidateParamName(r.From)
	if err != nil {
		return fmt.Errorf("core: config: remap from: %w", err)
	}
	err = ValidateParamName(r.To)
	if err != nil {
		return fmt.Errorf("core: config: remap to: %w", err)
	}
	return nil
}

// ApplyRemap rewrites name through the creation-time table. Global rules
// are checked in order first, then local rules in order; the first exact
// match wins. The function is pure: it never mutates its inputs.
func ApplyRemap(name string, global, local []RemapRule) string {
	for _, r := range global {
		if r.From == name {
			return r.To
		}
	}
	for _, r := range local {
		if r.From == name {
			return r.To
		}
	}
	return name
}

// ExpandNodeScopedParam expands a relative parameter name against a
// dot-separated namespace. Names containing a dot are already scoped and
// returned unchanged after validation. Single-segment names join as
// namespace plus dot plus name.
func ExpandNodeScopedParam(name, namespace string) (string, error) {
	if name == "" {
		return "", errors.New("core: config: parameter name is empty") //nolint:wrapcheck // new error, nothing to wrap
	}
	if strings.Contains(name, ".") {
		err := ValidateParamName(name)
		if err != nil {
			return "", err
		}
		return name, nil
	}
	err := ValidateParamName(name)
	if err != nil {
		return "", err
	}
	trimmed := strings.Trim(namespace, ".")
	if trimmed == "" {
		return name, nil
	}
	expanded := trimmed + "." + name
	err = ValidateParamName(expanded)
	if err != nil {
		return "", err
	}
	return expanded, nil
}

// normalizeNodeKey maps file top-level keys to canonical dotted form:
// surrounding whitespace and slashes trimmed, inner slashes become dots,
// empty maps to wildcard. "*" and "/*" both normalize to "*".
func normalizeNodeKey(key string) string {
	trimmed := strings.TrimSpace(key)
	trimmed = strings.Trim(trimmed, "/")
	trimmed = strings.ReplaceAll(trimmed, "/", ".")
	trimmed = strings.Trim(trimmed, ".")
	if trimmed == "" || trimmed == "*" {
		return "*"
	}
	return trimmed
}

// CollapseParamFiles merges wildcard and node-specific file sections. The
// wildcard section ("*" or "/*") applies first; the nodeFQN section
// overwrites per key so fully qualified names beat wildcards. Keys are
// normalized with normalizeNodeKey before comparison. Duplicate section keys
// that normalize to the same entry fail loudly naming both originals when
// their contents differ; identical duplicates resolve deterministically in
// sorted key order.
func CollapseParamFiles(files map[string]map[string]ParamValue, nodeFQN string) map[string]ParamValue {
	out, err := CollapseParamFilesChecked(files, nodeFQN)
	if err != nil {
		panic(err.Error())
	}
	return out
}

// collapseSectionsForNode selects the wildcard and node-specific sections
// for one node from normalized file sections.
func collapseSectionsForNode(sections map[string]map[string]ParamValue, want string) (wildcard, specific map[string]ParamValue) {
	if sec, ok := sections["*"]; ok {
		wildcard = sec
	}
	if want != "*" {
		if sec, ok := sections[want]; ok {
			specific = sec
		}
	}
	return wildcard, specific
}

// CollapseParamFilesChecked merges file sections and reports duplicate
// normalized section keys loudly instead of overwriting nondeterministically.
func CollapseParamFilesChecked(files map[string]map[string]ParamValue, nodeFQN string) (map[string]ParamValue, error) {
	out := make(map[string]ParamValue)
	if files == nil {
		return out, nil
	}
	want := normalizeNodeKey(nodeFQN)
	keys := make([]string, 0, len(files))
	for key := range files {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	origins := make(map[string]string)
	sections := make(map[string]map[string]ParamValue)
	for _, key := range keys {
		norm := normalizeNodeKey(key)
		if norm != "*" && (want == "*" || norm != want) {
			continue
		}
		if first, ok := origins[norm]; ok {
			if !paramSectionsEqual(sections[norm], files[key]) {
				return nil, fmt.Errorf("core: config: collapse collision %q and %q both normalize to %q", first, key, norm)
			}
			continue
		}
		origins[norm] = key
		sections[norm] = files[key]
	}
	wildcard, specific := collapseSectionsForNode(sections, want)
	for k, v := range wildcard {
		out[k] = cloneParamValue(v)
	}
	for k, v := range specific {
		out[k] = cloneParamValue(v)
	}
	return out, nil
}

// paramSectionsEqual reports whether two file sections carry equal values.
func paramSectionsEqual(a, b map[string]ParamValue) bool {
	if len(a) != len(b) {
		return false
	}
	for k, va := range a {
		vb, ok := b[k]
		if !ok {
			return false
		}
		if !va.Equal(vb) {
			return false
		}
	}
	return true
}

// ParamValueFromAny converts a decoded YAML-like scalar or uniform array to
// a ParamValue. Maps must be flattened with FlattenNestedParams first.
// Supported: bool, all int and uint widths, float32 and float64, string,
// []byte, typed slices, and []any with uniform element typing. Mixed or
// empty untyped arrays fail so uniform array typing is enforced.
//
//nolint:cyclop // conversion spans all int, uint, float, and array forms in one switch, splitting would obscure intent
func ParamValueFromAny(v any) (ParamValue, error) {
	switch t := v.(type) {
	case bool:
		return NewBoolParam(t), nil
	case int:
		return NewIntegerParam(int64(t)), nil
	case int8:
		return NewIntegerParam(int64(t)), nil
	case int16:
		return NewIntegerParam(int64(t)), nil
	case int32:
		return NewIntegerParam(int64(t)), nil
	case int64:
		return NewIntegerParam(t), nil
	case uint:
		return uintToParam(uint64(t))
	case uint8:
		return NewIntegerParam(int64(t)), nil
	case uint16:
		return NewIntegerParam(int64(t)), nil
	case uint32:
		return NewIntegerParam(int64(t)), nil
	case uint64:
		return uintToParam(t)
	case float32:
		return NewDoubleParam(float64(t)), nil
	case float64:
		return NewDoubleParam(t), nil
	case string:
		return NewStringParam(t), nil
	case []byte:
		return NewByteArrayParam(t), nil
	case []bool:
		return NewBoolArrayParam(t), nil
	case []int:
		return intSliceToParam(t)
	case []int64:
		return NewIntegerArrayParam(t), nil
	case []float64:
		return NewDoubleArrayParam(t), nil
	case []string:
		return NewStringArrayParam(t), nil
	case []any:
		return anySliceToParam(t)
	default:
		return ParamValue{}, fmt.Errorf("core: config: unsupported value type %T", v)
	}
}

// uintToParam converts an unsigned width to an integer param with overflow check.
func uintToParam(v uint64) (ParamValue, error) {
	const maxInt64 = int64(^uint64(0) >> 1)
	if v > uint64(maxInt64) {
		return ParamValue{}, fmt.Errorf("core: config: unsigned value %d overflows int64", v)
	}
	return NewIntegerParam(int64(v)), nil
}

// intSliceToParam converts []int to an integer array param.
func intSliceToParam(v []int) (ParamValue, error) {
	out := make([]int64, len(v))
	for i, e := range v {
		out[i] = int64(e)
	}
	return NewIntegerArrayParam(out), nil
}

// anySliceToParam converts []any with uniform element typing to an array param.
func anySliceToParam(v []any) (ParamValue, error) {
	if len(v) == 0 {
		return ParamValue{}, errors.New("core: config: empty untyped array has no element type; use a typed empty array") //nolint:wrapcheck // new error, nothing to wrap
	}
	first, err := ParamValueFromAny(v[0])
	if err != nil {
		return ParamValue{}, err
	}
	switch first.Type { //nolint:exhaustive // default safely rejects non-scalar array element types with uniform typing error
	case ParamTypeBool:
		return anySliceToBoolArray(v)
	case ParamTypeInteger:
		return anySliceToIntegerArray(v)
	case ParamTypeDouble:
		return anySliceToDoubleArray(v)
	case ParamTypeString:
		return anySliceToStringArray(v)
	default:
		return ParamValue{}, fmt.Errorf("core: config: array element type %s is not uniform scalar", first.Type)
	}
}

// anySliceToBoolArray converts a uniform boolean slice.
func anySliceToBoolArray(v []any) (ParamValue, error) {
	out := make([]bool, len(v))
	for i, e := range v {
		b, ok := e.(bool)
		if !ok {
			return ParamValue{}, fmt.Errorf("core: config: mixed array typing at index %d: want bool", i)
		}
		out[i] = b
	}
	return NewBoolArrayParam(out), nil
}

// anySliceToIntegerArray converts a uniform integer slice.
func anySliceToIntegerArray(v []any) (ParamValue, error) {
	out := make([]int64, len(v))
	for i, e := range v {
		p, err := ParamValueFromAny(e)
		if err != nil {
			return ParamValue{}, err
		}
		if p.Type != ParamTypeInteger {
			return ParamValue{}, fmt.Errorf("core: config: mixed array typing at index %d: want integer", i)
		}
		out[i] = p.Integer
	}
	return NewIntegerArrayParam(out), nil
}

// anySliceToDoubleArray converts a uniform double slice.
func anySliceToDoubleArray(v []any) (ParamValue, error) {
	out := make([]float64, len(v))
	for i, e := range v {
		p, err := ParamValueFromAny(e)
		if err != nil {
			return ParamValue{}, err
		}
		if p.Type != ParamTypeDouble && p.Type != ParamTypeInteger {
			return ParamValue{}, fmt.Errorf("core: config: mixed array typing at index %d: want double", i)
		}
		if p.Type == ParamTypeInteger {
			out[i] = float64(p.Integer)
			continue
		}
		out[i] = p.Double
	}
	return NewDoubleArrayParam(out), nil
}

// anySliceToStringArray converts a uniform string slice.
func anySliceToStringArray(v []any) (ParamValue, error) {
	out := make([]string, len(v))
	for i, e := range v {
		s, ok := e.(string)
		if !ok {
			return ParamValue{}, fmt.Errorf("core: config: mixed array typing at index %d: want string", i)
		}
		out[i] = s
	}
	return NewStringArrayParam(out), nil
}

// FlattenNestedParams flattens nested YAML-like maps to dotted parameter
// names. Nested maps recurse with dot-joined prefixes; scalars convert with
// ParamValueFromAny. Keys already containing dots are kept verbatim as one
// segment join. There are no fixed size buffers: output grows by append.
func FlattenNestedParams(nested map[string]any, prefix string) (map[string]ParamValue, error) {
	out := make(map[string]ParamValue)
	err := flattenInto(nested, prefix, out)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// flattenInto recurses one map level into out.
//
//nolint:gocognit // flattening sequences key sorting, nesting, conversion, and validation in one traversal, splitting would obscure intent
func flattenInto(nested map[string]any, prefix string, out map[string]ParamValue) error {
	keys := make([]string, 0, len(nested))
	for k := range nested {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		if k == "" {
			return errors.New("core: config: empty YAML key") //nolint:wrapcheck // new error, nothing to wrap
		}
		var full string
		if prefix != "" {
			full = prefix + "." + k
		} else {
			full = k
		}
		sub, ok := nested[k].(map[string]any)
		if ok {
			err := flattenInto(sub, full, out)
			if err != nil {
				return err
			}
			continue
		}
		v, err := ParamValueFromAny(nested[k])
		if err != nil {
			return fmt.Errorf("core: config: key %q: %w", full, err)
		}
		err = ValidateParamName(full)
		if err != nil {
			return err
		}
		if _, exists := out[full]; exists {
			return fmt.Errorf("core: config: flatten collision %q from %q collides with existing entry", full, k)
		}
		out[full] = v
	}
	return nil
}

// ParseOverrideLiteral parses one YAML scalar or array literal for -p
// name:=value overrides. Quoted text stays a string; true and false are
// booleans; integers parse before doubles; [a, b] arrays require uniform
// element typing. Maps are rejected: dicts travel through params files.
func ParseOverrideLiteral(text string) (ParamValue, error) {
	trimmed := strings.TrimSpace(text)
	if trimmed == "" {
		return ParamValue{}, errors.New("core: config: override value is empty") //nolint:wrapcheck // new error, nothing to wrap
	}
	if len(trimmed) >= 2 && trimmed[0] == '{' {
		return ParamValue{}, errors.New("core: config: dict literals travel through params files, not -p overrides") //nolint:wrapcheck // new error, nothing to wrap
	}
	if unquoted, ok := cutQuoted(trimmed); ok {
		return NewStringParam(unquoted), nil
	}
	if trimmed == "true" {
		return NewBoolParam(true), nil
	}
	if trimmed == "false" {
		return NewBoolParam(false), nil
	}
	if strings.HasPrefix(trimmed, "[") {
		return parseOverrideArray(trimmed)
	}
	if iv, err := strconv.ParseInt(trimmed, 10, 64); err == nil {
		return NewIntegerParam(iv), nil
	}
	if fv, err := strconv.ParseFloat(trimmed, 64); err == nil {
		return NewDoubleParam(fv), nil
	}
	return NewStringParam(trimmed), nil
}

// cutQuoted strips one pair of matching single or double quotes.
func cutQuoted(s string) (string, bool) {
	if len(s) >= 2 && ((s[0] == '"' && s[len(s)-1] == '"') || (s[0] == '\'' && s[len(s)-1] == '\'')) {
		return s[1 : len(s)-1], true
	}
	return "", false
}

// parseOverrideArray parses [a, b] with uniform element typing.
//
//nolint:gocognit,cyclop // array parsing sequences literal splitting, element typing, and int-double coercion in one pass, splitting would obscure intent
func parseOverrideArray(text string) (ParamValue, error) {
	if !strings.HasSuffix(text, "]") {
		return ParamValue{}, fmt.Errorf("core: config: unterminated array literal %q", text)
	}
	inner := strings.TrimSpace(text[1 : len(text)-1])
	if inner == "" {
		return ParamValue{}, errors.New("core: config: empty array literal has no element type") //nolint:wrapcheck // new error, nothing to wrap
	}
	parts := strings.Split(inner, ",")
	values := make([]ParamValue, 0, len(parts))
	for _, p := range parts {
		v, err := ParseOverrideLiteral(p)
		if err != nil {
			return ParamValue{}, err
		}
		values = append(values, v)
	}
	first := values[0].Type
	for i, v := range values[1:] {
		coerced := v.Type
		if first == ParamTypeDouble && coerced == ParamTypeInteger {
			continue
		}
		if coerced != first {
			return ParamValue{}, fmt.Errorf("core: config: mixed array typing at index %d", i+1)
		}
	}
	switch first { //nolint:exhaustive // default safely rejects non-scalar override element types with scalar-only error
	case ParamTypeBool:
		out := make([]bool, len(values))
		for i, v := range values {
			out[i] = v.Bool
		}
		return NewBoolArrayParam(out), nil
	case ParamTypeInteger:
		out := make([]int64, len(values))
		for i, v := range values {
			out[i] = v.Integer
		}
		return NewIntegerArrayParam(out), nil
	case ParamTypeDouble:
		out := make([]float64, len(values))
		for i, v := range values {
			if v.Type == ParamTypeInteger {
				out[i] = float64(v.Integer)
				continue
			}
			out[i] = v.Double
		}
		return NewDoubleArrayParam(out), nil
	case ParamTypeString:
		out := make([]string, len(values))
		for i, v := range values {
			out[i] = v.String
		}
		return NewStringArrayParam(out), nil
	default:
		return ParamValue{}, fmt.Errorf("core: config: array element type %s is not a scalar override", first)
	}
}

// ParseParamOverride splits name:=literal for -p overrides, validating the
// name and parsing the literal with ParseOverrideLiteral.
func ParseParamOverride(text string) (string, ParamValue, error) {
	name, literal, ok := strings.Cut(text, ":=")
	if !ok {
		return "", ParamValue{}, fmt.Errorf("core: config: override %q misses :=", text)
	}
	name = strings.TrimSpace(name)
	err := ValidateParamName(name)
	if err != nil {
		return "", ParamValue{}, err
	}
	value, err := ParseOverrideLiteral(literal)
	if err != nil {
		return "", ParamValue{}, err
	}
	return name, value, nil
}

// ConfigChange is one parameter delta inside an event.
type ConfigChange struct {
	// Name is the final post-remap parameter name.
	Name string
	// OldValue is the previous value. Valid only when HadOld is true.
	OldValue ParamValue
	// NewValue is the applied value.
	NewValue ParamValue
	// HadOld is false for newly declared entries.
	HadOld bool
}

// ConfigEvent is one versioned batch: exactly one per successful mutation
// batch, so startup with N parameters emits one event, not N.
type ConfigEvent struct {
	// Version is the store version after the batch.
	Version uint64
	// Source names the origin: declare, set, startup, override, or load.
	Source string
	// Changes lists applied deltas sorted by name for determinism.
	Changes []ConfigChange
}

// ConfigHistoryEntry records one parameter delta at one version. Entries
// sharing a version belong to the same atomic batch.
type ConfigHistoryEntry struct {
	// Version is the store version after the batch.
	Version uint64
	// Name is the final post-remap parameter name.
	Name string
	// OldValue is the previous value. Valid only when HadOld is true.
	OldValue ParamValue
	// NewValue is the applied value.
	NewValue ParamValue
	// HadOld is false for newly declared entries.
	HadOld bool
	// Source names the origin batch.
	Source string
}

// ConfigValidator validates a pending set and may reject it. Rejection
// aborts the whole batch before any apply hook runs and before the version
// moves.
type ConfigValidator func(oldValue, newValue ParamValue) error

// ConfigApplyHook runs after a validated batch commits. Hooks never reject:
// validation already passed, so hooks observe only.
type ConfigApplyHook func(oldValue, newValue ParamValue)

// ConfigStoreOptions selects store behavior at creation. Remap tables are
// fixed here and cannot update after creation.
type ConfigStoreOptions struct {
	// AllowUndeclared permits Set to auto-declare unknown names.
	AllowUndeclared bool
	// GlobalRemaps apply before LocalRemaps; first match wins.
	GlobalRemaps []RemapRule
	// LocalRemaps apply after GlobalRemaps; first match wins.
	LocalRemaps []RemapRule
}

// configEntry is one stored parameter.
type configEntry struct {
	value ParamValue
	desc  ParamDescriptor
}

// configWatcher is one subscription.
type configWatcher struct {
	ch chan ConfigEvent
}

// ConfigStore is a versioned parameter store with schema, validation,
// atomic batches, watches, and history. The zero value is unusable; build
// with NewConfigStore. Do not copy after first use.
// History is bounded to the last configHistoryLimit entries; overflow drops
// the oldest entries and counts each drop in HistoryDrops so long runs stay
// bounded while remaining observable. Watch broadcast never holds the write
// lock while sending; each watcher is checked plus sent under a read lock
// with eviction counted in BroadcastDrops.
type ConfigStore struct {
	noCopy      noCopy
	mu          sync.RWMutex
	params      map[string]*configEntry
	version     uint64
	history     []ConfigHistoryEntry
	historyDrop uint64
	watchers    map[uint64]*configWatcher
	nextWatch   uint64
	validators  map[string][]ConfigValidator
	hooks       map[string][]ConfigApplyHook
	global      []RemapRule
	local       []RemapRule
	allowNew    bool
	drops       atomic.Uint64
}

// BroadcastDrops returns the monotonic count of watcher broadcast evictions
// plus drops. A full channel evicts its oldest event; the evicted range stays
// recoverable through HistorySince.
func (s *ConfigStore) BroadcastDrops() uint64 {
	return s.drops.Load()
}

// HistoryDrops returns the monotonic count of history entries dropped by the
// configHistoryLimit bound. Overflow drops the oldest entries.
func (s *ConfigStore) HistoryDrops() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.historyDrop
}

// pendingValidator pairs one validator with its snapshot inputs.
type pendingValidator struct {
	name string
	old  ParamValue
	new  ParamValue
	fn   ConfigValidator
}

// NewConfigStore returns an empty store with fixed remap tables. Rules are
// validated eagerly: an invalid rule fails creation. Tables are copied so
// caller mutation cannot rewrite them.
func NewConfigStore(opts ConfigStoreOptions) (*ConfigStore, error) {
	for _, r := range opts.GlobalRemaps {
		err := ValidateRemapRule(r)
		if err != nil {
			return nil, err
		}
	}
	for _, r := range opts.LocalRemaps {
		err := ValidateRemapRule(r)
		if err != nil {
			return nil, err
		}
	}
	global := make([]RemapRule, len(opts.GlobalRemaps))
	copy(global, opts.GlobalRemaps)
	local := make([]RemapRule, len(opts.LocalRemaps))
	copy(local, opts.LocalRemaps)
	return &ConfigStore{
		params:     make(map[string]*configEntry),
		watchers:   make(map[uint64]*configWatcher),
		validators: make(map[string][]ConfigValidator),
		hooks:      make(map[string][]ConfigApplyHook),
		global:     global,
		local:      local,
		allowNew:   opts.AllowUndeclared,
	}, nil
}

// remappedLocked applies the fixed creation-time table. Callers hold at
// least a read lock; the tables never mutate after creation.
func (s *ConfigStore) remappedLocked(name string) string {
	return ApplyRemap(name, s.global, s.local)
}

// Remapped exposes the creation-time rewrite for one name. It is pure
// apart from a read lock guarding table publication.
func (s *ConfigStore) Remapped(name string) string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.remappedLocked(name)
}

// Declare adds a parameter with its descriptor. The name is remapped first.
// Re-declaring fails. The value must validate against the descriptor unless
// both are NotSet. One declare is one versioned event.
// Validators for the declare run outside the write lock against a read
// snapshot with optimistic generation; a concurrent mutation aborts loudly.
// Validators must still avoid deep reentrancy.
// declareNotSetFastPath inserts a NotSet value plus NotSet descriptor
// directly. It reports handled when the fast path applied; otherwise the
// caller falls through to the validated declare path.
func (s *ConfigStore) declareNotSetFastPath(name string, value ParamValue, desc ParamDescriptor) (string, bool, error) {
	s.mu.Lock()
	final := s.remappedLocked(name)
	err := ValidateParamName(final)
	if err != nil {
		s.mu.Unlock()
		return "", false, err
	}
	if _, ok := s.params[final]; ok {
		s.mu.Unlock()
		return "", false, fmt.Errorf("core: config: declare %q: already declared", final)
	}
	if value.Type != ParamTypeNotSet || desc.Type != ParamTypeNotSet {
		s.mu.Unlock()
		return final, false, nil
	}
	event := ConfigEvent{Version: s.version + 1, Source: "declare"}
	event.Changes = []ConfigChange{{Name: final, NewValue: cloneParamValue(value), HadOld: false}}
	s.params[final] = &configEntry{value: cloneParamValue(value), desc: desc}
	s.version = event.Version
	s.history = append(s.history, ConfigHistoryEntry{
		Version: event.Version, Name: final, NewValue: cloneParamValue(value), HadOld: false, Source: "declare",
	})
	s.trimHistoryLocked()
	s.mu.Unlock()
	s.broadcast(event)
	return final, true, nil
}

func (s *ConfigStore) Declare(name string, value ParamValue, desc ParamDescriptor) error {
	err := ValidateParamName(name)
	if err != nil {
		return err
	}
	_, handled, err := s.declareNotSetFastPath(name, value, desc)
	if err != nil || handled {
		return err
	}
	// Remap tables are fixed at creation, so resolving outside the lock is
	// safe; the retry loop below re-checks existence under the lock.
	final := ApplyRemap(name, s.global, s.local)
	err = ValidateParamName(final)
	if err != nil {
		return err
	}
	err = desc.validateValue(value)
	if err != nil {
		return fmt.Errorf("core: config: declare %q: %w", final, err)
	}
	if desc.Type == ParamTypeNotSet {
		desc.Type = value.Type
	}
	descs := map[string]ParamDescriptor{final: desc}
	return s.commitDeclareBatch(name, final, value, descs)
}

// commitDeclareBatch commits one validated declare, retrying on concurrent
// mutation. It re-checks absence on every attempt so a racing declare fails
// loudly instead of overwriting. name is the pre-remap input name; final is
// its remapped form.
func (s *ConfigStore) commitDeclareBatch(name, final string, value ParamValue, descs map[string]ParamDescriptor) error {
	for range configCommitRetries {
		s.mu.Lock()
		if _, ok := s.params[final]; ok {
			s.mu.Unlock()
			return fmt.Errorf("core: config: declare %q: already declared", final)
		}
		event, hooks, pending, err := s.prepareBatchLocked(map[string]ParamValue{name: value}, descs, "declare", true)
		if err != nil {
			s.mu.Unlock()
			return err
		}
		gen := s.version
		s.mu.Unlock()
		err = runPendingValidators(pending)
		if err != nil {
			return err
		}
		s.mu.Lock()
		if _, ok := s.params[final]; ok {
			s.mu.Unlock()
			return fmt.Errorf("core: config: declare %q: already declared", final)
		}
		if s.version != gen {
			s.mu.Unlock()
			continue
		}
		s.commitPreparedLocked(event, descs)
		s.mu.Unlock()
		s.broadcast(event)
		s.runHooks(hooks, event)
		return nil
	}
	return fmt.Errorf("core: config: declare %q: batch aborted after %d concurrent mutations", final, configCommitRetries)
}

// Get returns a copy of one parameter. The name is remapped first.
func (s *ConfigStore) Get(name string) (ParamValue, error) {
	err := ValidateParamName(name)
	if err != nil {
		return ParamValue{}, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	final := s.remappedLocked(name)
	entry, ok := s.params[final]
	if !ok {
		return ParamValue{}, fmt.Errorf("core: config: get %q: not declared", final)
	}
	return cloneParamValue(entry.value), nil
}

// Describe returns a copy of one parameter descriptor. The name is remapped.
func (s *ConfigStore) Describe(name string) (ParamDescriptor, error) {
	err := ValidateParamName(name)
	if err != nil {
		return ParamDescriptor{}, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	final := s.remappedLocked(name)
	entry, ok := s.params[final]
	if !ok {
		return ParamDescriptor{}, fmt.Errorf("core: config: describe %q: not declared", final)
	}
	return entry.desc, nil
}

// List returns stored final names with the given dot prefix, sorted.
// An empty prefix lists everything.
func (s *ConfigStore) List(prefix string) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []string
	for name := range s.params {
		if prefix == "" || name == prefix || strings.HasPrefix(name, prefix+".") {
			out = append(out, name)
		}
	}
	sort.Strings(out)
	if out == nil {
		return []string{}
	}
	return out
}

// Version returns the current store version.
func (s *ConfigStore) Version() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.version
}

// History returns a copy of every entry oldest first.
func (s *ConfigStore) History() []ConfigHistoryEntry {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]ConfigHistoryEntry, len(s.history))
	for i, h := range s.history {
		out[i] = cloneHistoryEntry(h)
	}
	return out
}

// HistorySince returns entries with version above since, oldest first.
func (s *ConfigStore) HistorySince(since uint64) []ConfigHistoryEntry {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []ConfigHistoryEntry
	for _, h := range s.history {
		if h.Version > since {
			out = append(out, cloneHistoryEntry(h))
		}
	}
	if out == nil {
		return []ConfigHistoryEntry{}
	}
	return out
}

// cloneHistoryEntry deep copies slice-carrying values.
func cloneHistoryEntry(h ConfigHistoryEntry) ConfigHistoryEntry {
	h.OldValue = cloneParamValue(h.OldValue)
	h.NewValue = cloneParamValue(h.NewValue)
	return h
}

// cloneEvent deep copies every change for isolated watcher delivery.
func cloneEvent(e ConfigEvent) ConfigEvent {
	changes := make([]ConfigChange, len(e.Changes))
	for i, c := range e.Changes {
		changes[i] = ConfigChange{
			Name:     c.Name,
			OldValue: cloneParamValue(c.OldValue),
			NewValue: cloneParamValue(c.NewValue),
			HadOld:   c.HadOld,
		}
	}
	return ConfigEvent{Version: e.Version, Source: e.Source, Changes: changes}
}

// Watch subscribes to future events. The returned version is the current
// store version; HistorySince replays anything missed. The channel is
// buffered and never blocks the store: a full channel evicts its oldest
// event and counts the eviction in BroadcastDrops. Call cancel to unsubscribe;
// cancel closes the channel. Consumers must tolerate non-monotonic event
// order across concurrent batches: broadcast checks plus sends per watcher
// under a read lock without holding the write lock.
func (s *ConfigStore) Watch() (uint64, <-chan ConfigEvent, func()) {
	s.mu.Lock()
	defer s.mu.Unlock()
	id := s.nextWatch
	s.nextWatch++
	ch := make(chan ConfigEvent, configWatchBuffer)
	s.watchers[id] = &configWatcher{ch: ch}
	cancel := func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		if w, ok := s.watchers[id]; ok {
			delete(s.watchers, id)
			close(w.ch)
		}
	}
	return s.version, ch, cancel
}

// AddValidator registers an on-set validator for one parameter. Validators
// run in registration order outside the write lock against a read snapshot
// with optimistic generation; the first error aborts the batch before apply
// hooks run and before the version moves. A concurrent mutation between
// validation and commit aborts loudly. Validators must still avoid deep
// reentrancy: they must not call back into the store.
func (s *ConfigStore) AddValidator(name string, fn ConfigValidator) error {
	err := ValidateParamName(name)
	if err != nil {
		return err
	}
	if fn == nil {
		return fmt.Errorf("core: config: validator for %q is nil", name)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	final := s.remappedLocked(name)
	s.validators[final] = append(s.validators[final], fn)
	return nil
}

// AddApplyHook registers a post-set hook for one parameter. Hooks run after
// a validated batch commits, in registration order, and never reject.
func (s *ConfigStore) AddApplyHook(name string, fn ConfigApplyHook) error {
	err := ValidateParamName(name)
	if err != nil {
		return err
	}
	if fn == nil {
		return fmt.Errorf("core: config: apply hook for %q is nil", name)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	final := s.remappedLocked(name)
	s.hooks[final] = append(s.hooks[final], fn)
	return nil
}

// Set writes one parameter with source "set".
func (s *ConfigStore) Set(name string, value ParamValue) error {
	_, err := s.SetWithSource(map[string]ParamValue{name: value}, "set")
	if err != nil {
		return err
	}
	return nil
}

// SetAtomically writes every entry or none with source "set" in one
// versioned event.
func (s *ConfigStore) SetAtomically(updates map[string]ParamValue) (ConfigEvent, error) {
	return s.SetWithSource(updates, "set")
}

// SetWithSource writes every entry or none with the given source label in
// one versioned event. Unknown names auto-declare only when the store
// allows undeclared parameters. Validators run outside the write lock
// against a read snapshot; a concurrent mutation aborts loudly.
func (s *ConfigStore) SetWithSource(updates map[string]ParamValue, source string) (ConfigEvent, error) {
	if source == "" {
		return ConfigEvent{}, errors.New("core: config: source is empty") //nolint:wrapcheck // new error, nothing to wrap
	}
	// allowNew is set once at construction and never mutates, so reading it
	// without the lock is safe.
	event, hooks, err := s.commitBatchWithValidators(updates, nil, source, s.allowNew)
	if err != nil {
		return ConfigEvent{}, err
	}
	if len(event.Changes) == 0 {
		return event, nil
	}
	s.broadcast(event)
	s.runHooks(hooks, event)
	return event, nil
}

// ApplyOverrides writes CLI-style overrides in one versioned event with
// source "override". Missing names always auto-declare here, matching node
// options where overrides declare from the command line. Validators run
// outside the write lock with optimistic generation.
func (s *ConfigStore) ApplyOverrides(overrides map[string]ParamValue) (ConfigEvent, error) {
	event, hooks, err := s.commitBatchWithValidators(overrides, nil, "override", true)
	if err != nil {
		return ConfigEvent{}, err
	}
	if len(event.Changes) == 0 {
		return event, nil
	}
	s.broadcast(event)
	s.runHooks(hooks, event)
	return event, nil
}

// ApplyStartup merges files, CLI, and code with fixed precedence
// files < CLI < code in one versioned event with source "startup".
// Missing names auto-declare from overrides regardless of the store flag.
// An empty merge emits no version bump and an empty event. Validators run
// outside the write lock with optimistic generation.
func (s *ConfigStore) ApplyStartup(files, cli, code map[string]ParamValue) (ConfigEvent, error) {
	merged := make(map[string]ParamValue)
	maps.Copy(merged, files)
	maps.Copy(merged, cli)
	maps.Copy(merged, code)
	if len(merged) == 0 {
		s.mu.RLock()
		version := s.version
		s.mu.RUnlock()
		return ConfigEvent{Version: version, Source: "startup", Changes: []ConfigChange{}}, nil
	}
	event, hooks, err := s.commitBatchWithValidators(merged, nil, "startup", true)
	if err != nil {
		return ConfigEvent{}, err
	}
	if len(event.Changes) == 0 {
		return event, nil
	}
	s.broadcast(event)
	s.runHooks(hooks, event)
	return event, nil
}

// Dump returns a copy of every stored value for canonical dump output.
// The map is freshly allocated so caller mutation cannot alias the store.
// Values format stays compatible; use DumpDescriptors plus LoadDump to round
// trip descriptors. Dump version is v1 values only.
func (s *ConfigStore) Dump() map[string]ParamValue {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make(map[string]ParamValue, len(s.params))
	for k, e := range s.params {
		out[k] = cloneParamValue(e.value)
	}
	return out
}

// ConfigDumpEntry pairs one stored value with its descriptor for round trips.
type ConfigDumpEntry struct {
	// Value is the stored parameter value.
	Value ParamValue
	// Descriptor is the stored parameter descriptor.
	Descriptor ParamDescriptor
}

// DumpDescriptors returns a copy of every stored descriptor for dump round
// trips. The map is freshly allocated so caller mutation cannot alias the store.
func (s *ConfigStore) DumpDescriptors() map[string]ParamDescriptor {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make(map[string]ParamDescriptor, len(s.params))
	for k, e := range s.params {
		out[k] = e.desc
	}
	return out
}

// DumpAll returns values plus descriptors so a dump round trip preserves
// read-only flags, ranges, and descriptions. The map is freshly allocated.
func (s *ConfigStore) DumpAll() map[string]ConfigDumpEntry {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make(map[string]ConfigDumpEntry, len(s.params))
	for k, e := range s.params {
		out[k] = ConfigDumpEntry{Value: cloneParamValue(e.value), Descriptor: e.desc}
	}
	return out
}

// LoadDump restores values plus descriptors atomically with source "load".
// Missing names declare with the supplied descriptors; existing names set
// through validation. Validators run outside the write lock with optimistic
// generation like other batches.
func (s *ConfigStore) LoadDump(dump map[string]ConfigDumpEntry, source string) (ConfigEvent, error) {
	if source == "" {
		return ConfigEvent{}, errors.New("core: config: source is empty") //nolint:wrapcheck // new error, nothing to wrap
	}
	values := make(map[string]ParamValue, len(dump))
	descs := make(map[string]ParamDescriptor, len(dump))
	for name, entry := range dump {
		err := ValidateParamName(name)
		if err != nil {
			return ConfigEvent{}, err
		}
		values[name] = entry.Value
		descs[name] = entry.Descriptor
	}
	event, hooks, err := s.commitBatchWithValidators(values, descs, source, true)
	if err != nil {
		return ConfigEvent{}, err
	}
	if len(event.Changes) == 0 {
		return event, nil
	}
	s.broadcast(event)
	s.runHooks(hooks, event)
	return event, nil
}

// ValidateSchema checks the store snapshot against the schema: every schema
// key must exist, no extra keys may exist, and every value must satisfy its
// descriptor. Problems join with semicolons.
func (s *ConfigStore) ValidateSchema(schema ParamSchema) error {
	s.mu.RLock()
	snapshot := make(map[string]ParamValue, len(s.params))
	for k, e := range s.params {
		snapshot[k] = cloneParamValue(e.value)
	}
	s.mu.RUnlock()
	return schema.ValidateValues(snapshot)
}

// hookCall pairs one hook with its old and new values.
type hookCall struct {
	fn  ConfigApplyHook
	old ParamValue
	new ParamValue
}

// runPendingValidators runs collected validators outside the write lock.
// Validators receive snapshot copies; they must still avoid deep reentrancy.
func runPendingValidators(pending []pendingValidator) error {
	for _, p := range pending {
		err := p.fn(cloneParamValue(p.old), cloneParamValue(p.new))
		if err != nil {
			return fmt.Errorf("core: config: set %q rejected by validator: %w", p.name, err)
		}
	}
	return nil
}

// configCommitRetries bounds optimistic-concurrency retries for one batch.
//
// Concurrent batches re-prepare against the fresh store, so distinct-key
// writes converge instead of aborting each other. Sustained contention past
// the bound fails loudly instead of spinning.
const configCommitRetries = 64

// commitBatchWithValidators prepares, validates outside the write lock, and
// commits one batch, retrying when a concurrent mutation lands mid-batch.
// Validators run on snapshots per attempt. A validator rejection fails
// immediately; only concurrent-mutation races retry. Empty batches commit
// nothing and return the current version.
func (s *ConfigStore) commitBatchWithValidators(updates map[string]ParamValue, descs map[string]ParamDescriptor, source string, autoDeclare bool) (ConfigEvent, []hookCall, error) {
	var aborted error
	for range configCommitRetries {
		s.mu.Lock()
		event, hooks, pending, err := s.prepareBatchLocked(updates, descs, source, autoDeclare)
		if err != nil {
			s.mu.Unlock()
			return ConfigEvent{}, nil, err
		}
		gen := s.version
		s.mu.Unlock()
		err = runPendingValidators(pending)
		if err != nil {
			return ConfigEvent{}, nil, err
		}
		s.mu.Lock()
		if s.version != gen {
			s.mu.Unlock()
			aborted = fmt.Errorf("core: config: %s: concurrent mutation", source)
			continue
		}
		if len(event.Changes) == 0 {
			version := s.version
			s.mu.Unlock()
			return ConfigEvent{Version: version, Source: source, Changes: []ConfigChange{}}, nil, nil
		}
		s.commitPreparedLocked(event, descs)
		s.mu.Unlock()
		return event, hooks, nil
	}
	return ConfigEvent{}, nil, fmt.Errorf("core: config: %s: batch aborted after %d concurrent mutations: %w", source, configCommitRetries, aborted)
}

// prepareBatchLocked validates every update without mutating state and
// builds the event plus hook list plus pending validators. Remapping sorts
// deterministically and fails loudly naming both originals when two inputs
// collide on one final name. Type checks and read-only checks run here;
// validators are collected here and run outside the write lock by the caller
// with optimistic generation, so a late failure can never leave a partial
// batch. The caller holds the write lock.
//
// remapBatchLocked validates and remaps batch names deterministically.
//
// It sorts inputs, validates each raw and final name, and fails loudly
// naming both originals when two inputs collide on one final name.
func (s *ConfigStore) remapBatchLocked(updates map[string]ParamValue) (map[string]ParamValue, error) {
	names := make([]string, 0, len(updates))
	for name := range updates {
		names = append(names, name)
	}
	sort.Strings(names)
	remapped := make(map[string]ParamValue, len(updates))
	origins := make(map[string]string, len(updates))
	for _, name := range names {
		v := updates[name]
		err := ValidateParamName(name)
		if err != nil {
			return nil, err
		}
		final := s.remappedLocked(name)
		err = ValidateParamName(final)
		if err != nil {
			return nil, err
		}
		if first, ok := origins[final]; ok {
			return nil, fmt.Errorf("core: config: remap collision %q and %q both map to %q", first, name, final)
		}
		origins[final] = name
		remapped[final] = v
	}
	return remapped, nil
}

func (s *ConfigStore) prepareBatchLocked(
	updates map[string]ParamValue,
	descs map[string]ParamDescriptor,
	source string,
	autoDeclare bool,
) (ConfigEvent, []hookCall, []pendingValidator, error) {
	remapped, err := s.remapBatchLocked(updates)
	if err != nil {
		return ConfigEvent{}, nil, nil, err
	}
	ordered := make([]string, 0, len(remapped))
	for name := range remapped {
		ordered = append(ordered, name)
	}
	sort.Strings(ordered)
	next := s.version + 1
	var changes []ConfigChange
	var hooks []hookCall
	var pending []pendingValidator
	for _, name := range ordered {
		newValue := cloneParamValue(remapped[name])
		entry, ok := s.params[name]
		if !ok {
			err := s.validateNewLocked(name, newValue, descs, autoDeclare)
			if err != nil {
				return ConfigEvent{}, nil, nil, err
			}
			changes = append(changes, ConfigChange{Name: name, NewValue: newValue, HadOld: false})
			for _, fn := range s.hooks[name] {
				hooks = append(hooks, hookCall{fn: fn, old: ParamValue{}, new: cloneParamValue(newValue)})
			}
			continue
		}
		hookVals, pend, err := s.validateExistingNoValidatorsLocked(name, entry, newValue)
		if err != nil {
			return ConfigEvent{}, nil, nil, err
		}
		pending = append(pending, pend...)
		old := cloneParamValue(entry.value)
		changes = append(changes, ConfigChange{Name: name, OldValue: old, NewValue: newValue, HadOld: true})
		for _, fn := range hookVals {
			hooks = append(hooks, hookCall{fn: fn, old: cloneParamValue(old), new: cloneParamValue(newValue)})
		}
	}
	event := ConfigEvent{Version: next, Source: source, Changes: changes}
	return event, hooks, pending, nil
}

// validateNewLocked checks a write to an undeclared name. Auto-declare
// accepts any concrete value; NotSet needs a NotSet descriptor to validate.
// Otherwise the write fails. A declare-time descriptor, when supplied, must
// still validate.
func (s *ConfigStore) validateNewLocked(
	name string,
	newValue ParamValue,
	descs map[string]ParamDescriptor,
	autoDeclare bool,
) error {
	if desc, ok := descs[name]; ok {
		err := desc.validateValue(newValue)
		if err != nil {
			return fmt.Errorf("core: config: declare %q: %w", name, err)
		}
		return nil
	}
	if newValue.Type == ParamTypeNotSet {
		return fmt.Errorf("core: config: set %q: value type not_set is not concrete", name)
	}
	if !autoDeclare {
		return fmt.Errorf("core: config: set %q: not declared and undeclared parameters are not allowed", name)
	}
	return nil
}

// validateExistingNoValidatorsLocked checks type and read-only for a write
// to a declared name and collects validators to run outside the write lock.
// The caller holds the write lock; validators run later against snapshots.
func (s *ConfigStore) validateExistingNoValidatorsLocked(name string, entry *configEntry, newValue ParamValue) ([]ConfigApplyHook, []pendingValidator, error) {
	if entry.desc.ReadOnly {
		return nil, nil, fmt.Errorf("core: config: set %q: parameter is read only", name)
	}
	err := entry.desc.validateValue(newValue)
	if err != nil {
		return nil, nil, fmt.Errorf("core: config: set %q: %w", name, err)
	}
	old := cloneParamValue(entry.value)
	var pending []pendingValidator
	for _, fn := range s.validators[name] {
		pending = append(pending, pendingValidator{name: name, old: old, new: cloneParamValue(newValue), fn: fn})
	}
	return s.hooks[name], pending, nil
}

// commitPreparedLocked applies a validated event: one version bump, one
// history entry per change sharing that version, and stored value swaps.
// Declared descriptors in descs pin new entries; auto-declared entries
// infer an unconstrained descriptor from the value. The caller holds the
// write lock and has already validated. History is trimmed to the last
// configHistoryLimit entries with oldest drops counted loudly.
func (s *ConfigStore) commitPreparedLocked(event ConfigEvent, descs map[string]ParamDescriptor) {
	s.version = event.Version
	for _, c := range event.Changes {
		entry, ok := s.params[c.Name]
		if !ok {
			desc := ParamDescriptor{Type: c.NewValue.Type}
			if d, has := descs[c.Name]; has {
				desc = d
			}
			s.params[c.Name] = &configEntry{
				value: cloneParamValue(c.NewValue),
				desc:  desc,
			}
		} else {
			entry.value = cloneParamValue(c.NewValue)
		}
		s.history = append(s.history, ConfigHistoryEntry{
			Version:  event.Version,
			Name:     c.Name,
			OldValue: cloneParamValue(c.OldValue),
			NewValue: cloneParamValue(c.NewValue),
			HadOld:   c.HadOld,
			Source:   event.Source,
		})
	}
	s.trimHistoryLocked()
}

// trimHistoryLocked drops oldest history beyond configHistoryLimit and counts
// loudly. The caller holds the write lock.
func (s *ConfigStore) trimHistoryLocked() {
	if len(s.history) <= configHistoryLimit {
		return
	}
	overflow := len(s.history) - configHistoryLimit
	for i := range overflow {
		s.history[i] = ConfigHistoryEntry{}
	}
	s.history = append([]ConfigHistoryEntry(nil), s.history[overflow:]...)
	s.historyDrop += uint64(overflow) //nolint:gosec // overflow is a non-negative length difference fitting uint64
}

// broadcast delivers the event to every watcher without holding the write
// lock. Each watcher is checked plus sent under a read lock with a
// per-watcher nonblocking send; a full channel evicts its oldest event and
// counts loudly in BroadcastDrops, and a second full send drops loudly.
// The evicted range stays available through HistorySince. Never sends on a
// closed channel because cancel deletes plus closes under the write lock,
// which waits for the read lock.
func (s *ConfigStore) broadcast(event ConfigEvent) {
	s.mu.RLock()
	ids := make([]uint64, 0, len(s.watchers))
	for id := range s.watchers {
		ids = append(ids, id)
	}
	s.mu.RUnlock()
	for _, id := range ids {
		s.mu.RLock()
		w, ok := s.watchers[id]
		if !ok {
			s.mu.RUnlock()
			continue
		}
		ch := w.ch
		deliver := cloneEvent(event)
		select {
		case ch <- deliver:
			s.mu.RUnlock()
		default:
			select {
			case <-ch:
				s.drops.Add(1)
			default:
			}
			select {
			case ch <- deliver:
				s.mu.RUnlock()
			default:
				s.drops.Add(1)
				s.mu.RUnlock()
			}
		}
	}
}

// runHooks executes apply hooks after commit in batch order. Validation
// already passed, so hooks never run after a rejection.
func (s *ConfigStore) runHooks(hooks []hookCall, _ ConfigEvent) {
	for _, h := range hooks {
		h.fn(cloneParamValue(h.old), cloneParamValue(h.new))
	}
}
