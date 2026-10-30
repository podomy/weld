// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package dds

import (
	"bytes"
	"strings"
	"sync"
	"testing"
)

// paramsTestGUID returns a fixed service GUID for wire tests.
func paramsTestGUID() [8]byte {
	var guid [8]byte
	for i := range guid {
		guid[i] = byte(i + 1)
	}
	return guid
}

// mustDeclareParams declares one param or fails the test.
func mustDeclareParams(t *testing.T, store *MemoryStore, name string, value ParamValue) {
	t.Helper()
	param := Parameter{Name: name, Value: value}
	desc := ParamDescriptor{Name: name, Type: value.Type}
	err := store.Declare(param, desc)
	if err != nil {
		t.Fatalf("declare %q: %v", name, err)
	}
}

// TestParamTypeCodes checks type codes match rcl_interfaces.
func TestParamTypeCodes(t *testing.T) {
	t.Parallel()
	cases := map[uint8]string{
		ParamNotSet:       "not_set",
		ParamBool:         "bool",
		ParamInteger:      "integer",
		ParamDouble:       "double",
		ParamString:       "string",
		ParamByteArray:    "byte_array",
		ParamBoolArray:    "bool_array",
		ParamIntegerArray: "integer_array",
		ParamDoubleArray:  "double_array",
		ParamStringArray:  "string_array",
	}
	for code, want := range cases {
		if code > ParamStringArray {
			t.Fatalf("code %d above maximum", code)
		}
		if want == "" {
			t.Fatalf("code %d has empty name", code)
		}
	}
	if ParamNotSet != 0 {
		t.Fatalf("not set = %d, want 0", ParamNotSet)
	}
	if ParamStringArray != 9 {
		t.Fatalf("string array = %d, want 9", ParamStringArray)
	}
}

// TestParamServiceNames checks six services for one node.
func TestParamServiceNames(t *testing.T) {
	t.Parallel()
	services, err := ParamServiceNames("/weld")
	if err != nil {
		t.Fatalf("service names: %v", err)
	}
	if services.Describe != "/weld/describe_parameters" {
		t.Fatalf("describe = %q", services.Describe)
	}
	if services.Get != "/weld/get_parameters" {
		t.Fatalf("get = %q", services.Get)
	}
	if services.GetTypes != "/weld/get_parameter_types" {
		t.Fatalf("get types = %q", services.GetTypes)
	}
	if services.List != "/weld/list_parameters" {
		t.Fatalf("list = %q", services.List)
	}
	if services.Set != "/weld/set_parameters" {
		t.Fatalf("set = %q", services.Set)
	}
	if services.SetAtomically != "/weld/set_parameters_atomically" {
		t.Fatalf("set atomically = %q", services.SetAtomically)
	}
	if ParamEventsTopic != "/parameter_events" {
		t.Fatalf("events topic = %q, want /parameter_events", ParamEventsTopic)
	}
	_, err = ParamServiceNames("weld")
	if err == nil {
		t.Fatalf("relative node accepted, want error")
	}
	_, err = ParamServiceNames("")
	if err == nil {
		t.Fatalf("empty node accepted, want error")
	}
}

// TestParamTopics checks request and reply mapping.
func TestParamTopics(t *testing.T) {
	t.Parallel()
	services, err := ParamServiceNames("/weld")
	if err != nil {
		t.Fatalf("service names: %v", err)
	}
	names := []string{services.Describe, services.Get, services.GetTypes, services.List, services.Set, services.SetAtomically}
	for _, name := range names {
		req, err := ParamRequestTopic(name)
		if err != nil {
			t.Fatalf("request topic %q: %v", name, err)
		}
		kindReq, _, err := ParseTopicName(req)
		if err != nil {
			t.Fatalf("parse request %q: %v", req, err)
		}
		_ = kindReq
		rep, err := ParamReplyTopic(name)
		if err != nil {
			t.Fatalf("reply topic %q: %v", name, err)
		}
		_, kindRep, err := ParseTopicName(rep)
		if err != nil {
			t.Fatalf("parse reply %q: %v", rep, err)
		}
		if kindRep != TopicKindReply {
			t.Fatalf("reply %q kind = %q, want reply", rep, kindRep)
		}
	}
}

// TestMemoryStoreDeclareGet checks declare then get round trip.
func TestMemoryStoreDeclareGet(t *testing.T) {
	t.Parallel()
	store := NewMemoryStore("/weld")
	mustDeclareParams(t, store, "my_int", ParamValue{Type: ParamInteger, Integer: 42})
	value, ok := store.Get("my_int")
	if !ok {
		t.Fatalf("get missing my_int")
	}
	if value.Type != ParamInteger || value.Integer != 42 {
		t.Fatalf("get = %+v, want integer 42", value)
	}
	missing, ok := store.Get("absent")
	if ok {
		t.Fatalf("absent present with %+v", missing)
	}
	if missing.Type != ParamNotSet {
		t.Fatalf("absent type = %d, want not set", missing.Type)
	}
	if store.NodeName() != "/weld" {
		t.Fatalf("node = %q, want /weld", store.NodeName())
	}
}

// TestMemoryStoreDeclareDuplicate checks second declare fails.
func TestMemoryStoreDeclareDuplicate(t *testing.T) {
	t.Parallel()
	store := NewMemoryStore("/weld")
	mustDeclareParams(t, store, "dup", ParamValue{Type: ParamBool, Bool: true})
	err := store.Declare(Parameter{Name: "dup", Value: ParamValue{Type: ParamBool, Bool: false}}, ParamDescriptor{Name: "dup", Type: ParamBool})
	if err == nil {
		t.Fatalf("duplicate declare succeeded, want error")
	}
}

// TestMemoryStoreDeclareRejectsBad checks invalid names and types fail.
func TestMemoryStoreDeclareRejectsBad(t *testing.T) {
	t.Parallel()
	store := NewMemoryStore("/weld")
	cases := []Parameter{
		{Name: "", Value: ParamValue{Type: ParamBool}},
		{Name: "has space", Value: ParamValue{Type: ParamBool}},
		{Name: "has/slash", Value: ParamValue{Type: ParamBool}},
		{Name: ".leading", Value: ParamValue{Type: ParamBool}},
		{Name: "ok", Value: ParamValue{Type: 99}},
	}
	for _, param := range cases {
		err := store.Declare(param, ParamDescriptor{Name: param.Name, Type: param.Value.Type})
		if err == nil {
			t.Fatalf("declare %+v succeeded, want error", param)
		}
	}
}

// TestMemoryStoreSetAutoDeclare checks set on missing auto declares.
func TestMemoryStoreSetAutoDeclare(t *testing.T) {
	t.Parallel()
	store := NewMemoryStore("/weld")
	results := store.SetMany([]Parameter{{Name: "auto", Value: ParamValue{Type: ParamString, String: "hi"}}})
	if len(results) != 1 || !results[0].Successful {
		t.Fatalf("auto declare set = %+v, want success", results)
	}
	value, ok := store.Get("auto")
	if !ok || value.String != "hi" {
		t.Fatalf("auto get = %+v %v, want hi", value, ok)
	}
	events := store.Events()
	if len(events) == 0 {
		t.Fatalf("no events after auto declare")
	}
}

// TestMemoryStoreReadOnly checks read only blocks changes.
func TestMemoryStoreReadOnly(t *testing.T) {
	t.Parallel()
	store := NewMemoryStore("/weld")
	err := store.Declare(Parameter{Name: "ro", Value: ParamValue{Type: ParamInteger, Integer: 1}}, ParamDescriptor{Name: "ro", Type: ParamInteger, ReadOnly: true})
	if err != nil {
		t.Fatalf("declare ro: %v", err)
	}
	results := store.SetMany([]Parameter{{Name: "ro", Value: ParamValue{Type: ParamInteger, Integer: 2}}})
	if len(results) != 1 || results[0].Successful {
		t.Fatalf("read only set succeeded, want failure: %+v", results)
	}
	value, _ := store.Get("ro")
	if value.Integer != 1 {
		t.Fatalf("ro changed to %d, want 1", value.Integer)
	}
}

// TestMemoryStoreDynamicTyping checks type change rules.
func TestMemoryStoreDynamicTyping(t *testing.T) {
	t.Parallel()
	store := NewMemoryStore("/weld")
	err := store.Declare(Parameter{Name: "fixed", Value: ParamValue{Type: ParamInteger, Integer: 1}}, ParamDescriptor{Name: "fixed", Type: ParamInteger})
	if err != nil {
		t.Fatalf("declare fixed: %v", err)
	}
	results := store.SetMany([]Parameter{{Name: "fixed", Value: ParamValue{Type: ParamString, String: "no"}}})
	if results[0].Successful {
		t.Fatalf("fixed type change succeeded without dynamic typing")
	}
	flex := NewMemoryStore("/flex")
	err = flex.Declare(Parameter{Name: "dyn", Value: ParamValue{Type: ParamInteger, Integer: 1}}, ParamDescriptor{Name: "dyn", Type: ParamInteger, DynamicTyping: true})
	if err != nil {
		t.Fatalf("declare dyn: %v", err)
	}
	results = flex.SetMany([]Parameter{{Name: "dyn", Value: ParamValue{Type: ParamString, String: "yes"}}})
	if !results[0].Successful {
		t.Fatalf("dynamic type change failed: %+v", results[0])
	}
}

// TestMemoryStoreRanges checks range enforcement.
func TestMemoryStoreRanges(t *testing.T) {
	t.Parallel()
	store := NewMemoryStore("/weld")
	intDesc := ParamDescriptor{Name: "bounded", Type: ParamInteger, IntegerRange: []IntegerRange{{From: 0, To: 10}}}
	err := store.Declare(Parameter{Name: "bounded", Value: ParamValue{Type: ParamInteger, Integer: 5}}, intDesc)
	if err != nil {
		t.Fatalf("declare bounded: %v", err)
	}
	results := store.SetMany([]Parameter{{Name: "bounded", Value: ParamValue{Type: ParamInteger, Integer: 99}}})
	if results[0].Successful {
		t.Fatalf("out of range set succeeded")
	}
	results = store.SetMany([]Parameter{{Name: "bounded", Value: ParamValue{Type: ParamInteger, Integer: 7}}})
	if !results[0].Successful {
		t.Fatalf("in range set failed: %+v", results[0])
	}
	doubleStore := NewMemoryStore("/weld")
	doubleDesc := ParamDescriptor{Name: "d", Type: ParamDouble, FloatingPointRange: []FloatingPointRange{{From: 0.0, To: 1.0}}}
	err = doubleStore.Declare(Parameter{Name: "d", Value: ParamValue{Type: ParamDouble, Double: 0.5}}, doubleDesc)
	if err != nil {
		t.Fatalf("declare double: %v", err)
	}
	results = doubleStore.SetMany([]Parameter{{Name: "d", Value: ParamValue{Type: ParamDouble, Double: 9.0}}})
	if results[0].Successful {
		t.Fatalf("out of range double succeeded")
	}
}

// TestMemoryStoreSetAtomically checks all or none.
func TestMemoryStoreSetAtomically(t *testing.T) {
	t.Parallel()
	store := NewMemoryStore("/weld")
	err := store.Declare(Parameter{Name: "a", Value: ParamValue{Type: ParamInteger, Integer: 1}}, ParamDescriptor{Name: "a", Type: ParamInteger, ReadOnly: true})
	if err != nil {
		t.Fatalf("declare a: %v", err)
	}
	mustDeclareParams(t, store, "b", ParamValue{Type: ParamInteger, Integer: 2})
	result := store.SetAtomically([]Parameter{
		{Name: "a", Value: ParamValue{Type: ParamInteger, Integer: 10}},
		{Name: "b", Value: ParamValue{Type: ParamInteger, Integer: 20}},
	})
	if result.Successful {
		t.Fatalf("atomic with read only succeeded")
	}
	value, _ := store.Get("b")
	if value.Integer != 2 {
		t.Fatalf("atomic changed b to %d, want 2", value.Integer)
	}
	result = store.SetAtomically([]Parameter{
		{Name: "b", Value: ParamValue{Type: ParamInteger, Integer: 20}},
		{Name: "c", Value: ParamValue{Type: ParamString, String: "new"}},
	})
	if !result.Successful {
		t.Fatalf("atomic valid failed: %+v", result)
	}
	value, _ = store.Get("b")
	if value.Integer != 20 {
		t.Fatalf("atomic b = %d, want 20", value.Integer)
	}
}

// TestMemoryStoreDelete checks NOT_SET deletes.
func TestMemoryStoreDelete(t *testing.T) {
	t.Parallel()
	store := NewMemoryStore("/weld")
	mustDeclareParams(t, store, "gone", ParamValue{Type: ParamString, String: "bye"})
	results := store.SetMany([]Parameter{{Name: "gone", Value: ParamValue{Type: ParamNotSet}}})
	if !results[0].Successful {
		t.Fatalf("delete failed: %+v", results[0])
	}
	_, ok := store.Get("gone")
	if ok {
		t.Fatalf("deleted param still present")
	}
	events := store.Events()
	found := false
	for _, e := range events {
		for _, p := range e.DeletedParams {
			if p.Name == "gone" {
				found = true
			}
		}
	}
	if !found {
		t.Fatalf("delete event missing")
	}
}

// TestMemoryStoreList checks prefixes and depth.
func TestMemoryStoreList(t *testing.T) {
	t.Parallel()
	store := NewMemoryStore("/weld")
	mustDeclareParams(t, store, "ctrl.max_vel", ParamValue{Type: ParamDouble, Double: 1.0})
	mustDeclareParams(t, store, "ctrl.min_vel", ParamValue{Type: ParamDouble, Double: 0.0})
	mustDeclareParams(t, store, "other", ParamValue{Type: ParamBool, Bool: true})
	all := store.List(nil, ParamDepthRecursive)
	if len(all.Names) != 3 {
		t.Fatalf("all names = %v, want 3", all.Names)
	}
	filtered := store.List([]string{"ctrl"}, ParamDepthRecursive)
	if len(filtered.Names) != 2 {
		t.Fatalf("ctrl names = %v, want 2", filtered.Names)
	}
	shallow := store.List([]string{""}, 1)
	if len(shallow.Names) == 0 {
		t.Fatalf("shallow list empty")
	}
}

// TestMemoryStoreDescribe checks descriptors for missing.
func TestMemoryStoreDescribe(t *testing.T) {
	t.Parallel()
	store := NewMemoryStore("/weld")
	mustDeclareParams(t, store, "known", ParamValue{Type: ParamBool, Bool: true})
	descs := store.Describe([]string{"known", "missing"})
	if len(descs) != 2 {
		t.Fatalf("descs = %d, want 2", len(descs))
	}
	if descs[0].Type != ParamBool {
		t.Fatalf("known type = %d, want bool", descs[0].Type)
	}
	if descs[1].Type != ParamNotSet {
		t.Fatalf("missing type = %d, want not set", descs[1].Type)
	}
}

// paramsSampleValues returns one value per type for wire tests.
func paramsSampleValues() []ParamValue {
	return []ParamValue{
		{Type: ParamNotSet},
		{Type: ParamBool, Bool: true},
		{Type: ParamInteger, Integer: -42},
		{Type: ParamDouble, Double: 3.25},
		{Type: ParamString, String: "hello"},
		{Type: ParamByteArray, ByteArray: []byte{1, 2, 3}},
		{Type: ParamBoolArray, BoolArray: []bool{true, false}},
		{Type: ParamIntegerArray, IntegerArray: []int64{1, -2, 3}},
		{Type: ParamDoubleArray, DoubleArray: []float64{1.5, -2.25}},
		{Type: ParamStringArray, StringArray: []string{"a", "b"}},
	}
}

// checkParamValueEqual fails when values differ.
//
//nolint:gocognit,cyclop // test helper pins all ten parameter value shapes in one helper, splitting would obscure intent
func checkParamValueEqual(t *testing.T, got, want ParamValue) {
	t.Helper()
	if got.Type != want.Type {
		t.Fatalf("type = %d, want %d", got.Type, want.Type)
	}
	switch want.Type {
	case ParamBool:
		if got.Bool != want.Bool {
			t.Fatalf("bool = %v, want %v", got.Bool, want.Bool)
		}
	case ParamInteger:
		if got.Integer != want.Integer {
			t.Fatalf("integer = %d, want %d", got.Integer, want.Integer)
		}
	case ParamDouble:
		if got.Double != want.Double {
			t.Fatalf("double = %v, want %v", got.Double, want.Double)
		}
	case ParamString:
		if got.String != want.String {
			t.Fatalf("string = %q, want %q", got.String, want.String)
		}
	case ParamByteArray:
		if !bytes.Equal(got.ByteArray, want.ByteArray) {
			t.Fatalf("bytes = %v, want %v", got.ByteArray, want.ByteArray)
		}
	case ParamBoolArray:
		if len(got.BoolArray) != len(want.BoolArray) {
			t.Fatalf("bool array len = %d, want %d", len(got.BoolArray), len(want.BoolArray))
		}
		for i := range want.BoolArray {
			if got.BoolArray[i] != want.BoolArray[i] {
				t.Fatalf("bool array %d mismatch", i)
			}
		}
	case ParamIntegerArray:
		if len(got.IntegerArray) != len(want.IntegerArray) {
			t.Fatalf("int array len mismatch")
		}
		for i := range want.IntegerArray {
			if got.IntegerArray[i] != want.IntegerArray[i] {
				t.Fatalf("int array %d = %d, want %d", i, got.IntegerArray[i], want.IntegerArray[i])
			}
		}
	case ParamDoubleArray:
		if len(got.DoubleArray) != len(want.DoubleArray) {
			t.Fatalf("double array len mismatch")
		}
		for i := range want.DoubleArray {
			if got.DoubleArray[i] != want.DoubleArray[i] {
				t.Fatalf("double array %d mismatch", i)
			}
		}
	case ParamStringArray:
		if len(got.StringArray) != len(want.StringArray) {
			t.Fatalf("string array len mismatch")
		}
		for i := range want.StringArray {
			if got.StringArray[i] != want.StringArray[i] {
				t.Fatalf("string array %d = %q, want %q", i, got.StringArray[i], want.StringArray[i])
			}
		}
	}
}

// TestParamValueWireRoundTrip checks all types in both orders.
func TestParamValueWireRoundTrip(t *testing.T) {
	t.Parallel()
	guid := paramsTestGUID()
	for _, le := range []bool{true, false} {
		values := paramsSampleValues()
		wire, err := EncodeGetParametersResponse(values, guid, 77, le)
		if err != nil {
			t.Fatalf("le=%v encode: %v", le, err)
		}
		back, gotGUID, seq, err := DecodeGetParametersResponse(wire)
		if err != nil {
			t.Fatalf("le=%v decode: %v", le, err)
		}
		if gotGUID != guid {
			t.Fatalf("le=%v guid mismatch", le)
		}
		if seq != 77 {
			t.Fatalf("le=%v seq = %d, want 77", le, seq)
		}
		if len(back) != len(values) {
			t.Fatalf("le=%v values = %d, want %d", le, len(back), len(values))
		}
		for i := range values {
			checkParamValueEqual(t, back[i], values[i])
		}
	}
}

// TestGetParametersWire checks request and serve path.
//
//nolint:gocognit // test pins get request, serve, and response decoding for both endians in one run, splitting would obscure intent
func TestGetParametersWire(t *testing.T) {
	t.Parallel()
	store := NewMemoryStore("/weld")
	mustDeclareParams(t, store, "a", ParamValue{Type: ParamInteger, Integer: 1})
	mustDeclareParams(t, store, "b", ParamValue{Type: ParamString, String: "x"})
	guid := paramsTestGUID()
	for _, le := range []bool{true, false} {
		req, err := EncodeGetParametersRequest([]string{"a", "b", "missing"}, guid, 11, le)
		if err != nil {
			t.Fatalf("le=%v encode req: %v", le, err)
		}
		names, gotGUID, seq, err := DecodeGetParametersRequest(req)
		if err != nil {
			t.Fatalf("le=%v decode req: %v", le, err)
		}
		if len(names) != 3 || gotGUID != guid || seq != 11 {
			t.Fatalf("le=%v req mismatch", le)
		}
		resp, err := ServeGetParameters(store, req, le)
		if err != nil {
			t.Fatalf("le=%v serve: %v", le, err)
		}
		values, _, _, err := DecodeGetParametersResponse(resp)
		if err != nil {
			t.Fatalf("le=%v decode resp: %v", le, err)
		}
		if len(values) != 3 {
			t.Fatalf("le=%v values = %d, want 3", le, len(values))
		}
		if values[2].Type != ParamNotSet {
			t.Fatalf("le=%v missing type = %d, want not set", le, values[2].Type)
		}
	}
}

// TestGetParameterTypesWire checks type codes round trip.
func TestGetParameterTypesWire(t *testing.T) {
	t.Parallel()
	store := NewMemoryStore("/weld")
	mustDeclareParams(t, store, "flag", ParamValue{Type: ParamBool, Bool: true})
	guid := paramsTestGUID()
	for _, le := range []bool{true, false} {
		req, err := EncodeGetParameterTypesRequest([]string{"flag", "absent"}, guid, 12, le)
		if err != nil {
			t.Fatalf("le=%v encode: %v", le, err)
		}
		resp, err := ServeGetParameterTypes(store, req, le)
		if err != nil {
			t.Fatalf("le=%v serve: %v", le, err)
		}
		codes, _, _, err := DecodeGetParameterTypesResponse(resp)
		if err != nil {
			t.Fatalf("le=%v decode: %v", le, err)
		}
		if len(codes) != 2 || codes[0] != ParamBool || codes[1] != ParamNotSet {
			t.Fatalf("le=%v codes = %v", le, codes)
		}
	}
}

// TestSetParametersWire checks independent set round trip.
func TestSetParametersWire(t *testing.T) {
	t.Parallel()
	store := NewMemoryStore("/weld")
	guid := paramsTestGUID()
	for _, le := range []bool{true, false} {
		params := []Parameter{
			{Name: "x", Value: ParamValue{Type: ParamInteger, Integer: 5}},
			{Name: "y", Value: ParamValue{Type: ParamString, String: "hi"}},
		}
		req, err := EncodeSetParametersRequest(params, guid, 13, le)
		if err != nil {
			t.Fatalf("le=%v encode: %v", le, err)
		}
		resp, err := ServeSetParameters(store, req, le)
		if err != nil {
			t.Fatalf("le=%v serve: %v", le, err)
		}
		results, _, _, err := DecodeSetParametersResponse(resp)
		if err != nil {
			t.Fatalf("le=%v decode: %v", le, err)
		}
		if len(results) != 2 || !results[0].Successful || !results[1].Successful {
			t.Fatalf("le=%v results = %+v", le, results)
		}
	}
}

// TestSetAtomicallyWire checks atomic serve round trip.
func TestSetAtomicallyWire(t *testing.T) {
	t.Parallel()
	store := NewMemoryStore("/weld")
	mustDeclareParams(t, store, "keep", ParamValue{Type: ParamInteger, Integer: 1})
	guid := paramsTestGUID()
	for _, le := range []bool{true, false} {
		params := []Parameter{{Name: "keep", Value: ParamValue{Type: ParamInteger, Integer: 2}}}
		req, err := EncodeSetAtomicallyRequest(params, guid, 14, le)
		if err != nil {
			t.Fatalf("le=%v encode: %v", le, err)
		}
		resp, err := ServeSetAtomically(store, req, le)
		if err != nil {
			t.Fatalf("le=%v serve: %v", le, err)
		}
		result, _, _, err := DecodeSetAtomicallyResponse(resp)
		if err != nil {
			t.Fatalf("le=%v decode: %v", le, err)
		}
		if !result.Successful {
			t.Fatalf("le=%v atomic failed", le)
		}
	}
}

// TestListParametersWire checks list serve round trip.
func TestListParametersWire(t *testing.T) {
	t.Parallel()
	store := NewMemoryStore("/weld")
	mustDeclareParams(t, store, "ctrl.a", ParamValue{Type: ParamBool, Bool: true})
	guid := paramsTestGUID()
	for _, le := range []bool{true, false} {
		req, err := EncodeListParametersRequest([]string{"ctrl"}, 0, guid, 15, le)
		if err != nil {
			t.Fatalf("le=%v encode: %v", le, err)
		}
		prefixes, depth, gotGUID, seq, err := DecodeListParametersRequest(req)
		if err != nil {
			t.Fatalf("le=%v decode req: %v", le, err)
		}
		if len(prefixes) != 1 || depth != 0 || gotGUID != guid || seq != 15 {
			t.Fatalf("le=%v req mismatch", le)
		}
		resp, err := ServeListParameters(store, req, le)
		if err != nil {
			t.Fatalf("le=%v serve: %v", le, err)
		}
		result, _, _, err := DecodeListParametersResponse(resp)
		if err != nil {
			t.Fatalf("le=%v decode resp: %v", le, err)
		}
		if len(result.Names) != 1 || result.Names[0] != "ctrl.a" {
			t.Fatalf("le=%v names = %v", le, result.Names)
		}
	}
}

// TestDescribeParametersWire checks describe serve round trip.
func TestDescribeParametersWire(t *testing.T) {
	t.Parallel()
	store := NewMemoryStore("/weld")
	mustDeclareParams(t, store, "d", ParamValue{Type: ParamDouble, Double: 1.0})
	guid := paramsTestGUID()
	for _, le := range []bool{true, false} {
		req, err := EncodeDescribeParametersRequest([]string{"d"}, guid, 16, le)
		if err != nil {
			t.Fatalf("le=%v encode: %v", le, err)
		}
		resp, err := ServeDescribeParameters(store, req, le)
		if err != nil {
			t.Fatalf("le=%v serve: %v", le, err)
		}
		descs, _, _, err := DecodeDescribeParametersResponse(resp)
		if err != nil {
			t.Fatalf("le=%v decode: %v", le, err)
		}
		if len(descs) != 1 || descs[0].Name != "d" || descs[0].Type != ParamDouble {
			t.Fatalf("le=%v descs = %+v", le, descs)
		}
	}
}

// TestDescriptorWireBound checks range bound enforcement on wire.
func TestDescriptorWireBound(t *testing.T) {
	t.Parallel()
	guid := paramsTestGUID()
	bad := ParamDescriptor{Name: "bad", Type: ParamInteger, IntegerRange: []IntegerRange{{From: 0, To: 1}, {From: 2, To: 3}}}
	_, err := EncodeDescribeParametersResponse([]ParamDescriptor{bad}, guid, 1, true)
	if err == nil {
		t.Fatalf("over bound descriptor encoded, want error")
	}
}

// TestParameterEventWire checks event round trip.
func TestParameterEventWire(t *testing.T) {
	t.Parallel()
	event := ParameterEvent{
		Node:         "/weld",
		StampSec:     1,
		StampNanosec: 2,
		NewParams:    []Parameter{{Name: "n", Value: ParamValue{Type: ParamBool, Bool: true}}},
		ChangedParams: []Parameter{
			{Name: "c", Value: ParamValue{Type: ParamInteger, Integer: 3}},
		},
		DeletedParams: []Parameter{{Name: "gone", Value: ParamValue{Type: ParamNotSet}}},
	}
	for _, le := range []bool{true, false} {
		wire, err := EncodeParameterEvent(event, le)
		if err != nil {
			t.Fatalf("le=%v encode: %v", le, err)
		}
		back, err := DecodeParameterEvent(wire)
		if err != nil {
			t.Fatalf("le=%v decode: %v", le, err)
		}
		if back.Node != event.Node || len(back.NewParams) != 1 || len(back.ChangedParams) != 1 || len(back.DeletedParams) != 1 {
			t.Fatalf("le=%v event mismatch %+v", le, back)
		}
		checkParamValueEqual(t, back.NewParams[0].Value, event.NewParams[0].Value)
	}
	_, err := EncodeParameterEvent(ParameterEvent{Node: "relative"}, true)
	if err == nil {
		t.Fatalf("relative node encoded, want error")
	}
}

// TestParamWireRejectsTrailing checks strict consumption.
func TestParamWireRejectsTrailing(t *testing.T) {
	t.Parallel()
	guid := paramsTestGUID()
	req, err := EncodeGetParametersRequest([]string{"a"}, guid, 1, true)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	bad := append(append([]byte(nil), req...), 0x01)
	_, _, _, err = DecodeGetParametersRequest(bad)
	if err == nil {
		t.Fatalf("trailing byte accepted, want error")
	}
}

// TestMemoryStoreConcurrent checks concurrent declare and set stay safe.
func TestMemoryStoreConcurrent(t *testing.T) {
	t.Parallel()
	store := NewMemoryStore("/weld")
	var wg sync.WaitGroup
	for i := range 8 {
		wg.Go(func() {
			name := strings.Repeat("p", 1) + string(rune('a'+i)) //nolint:gosec // G115: loop index 0..7 fits rune
			if err := store.Declare(Parameter{Name: name, Value: ParamValue{Type: ParamInteger, Integer: int64(i)}}, ParamDescriptor{Name: name, Type: ParamInteger}); err != nil {
				t.Errorf("declare %q: %v", name, err)
			}
			_ = store.SetMany([]Parameter{{Name: name, Value: ParamValue{Type: ParamInteger, Integer: int64(i + 1)}}})
		})
	}
	wg.Wait()
	result := store.List(nil, ParamDepthRecursive)
	if len(result.Names) == 0 {
		t.Fatalf("concurrent store empty")
	}
}
