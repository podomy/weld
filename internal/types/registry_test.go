// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package types

import (
	"strings"
	"sync"
	"testing"
)

// registerTimeHeader registers Time plus Header individuals for closure tests.
func registerTimeHeader(t *testing.T, r *Registry) {
	t.Helper()
	timeDesc := IndividualTypeDescription{
		TypeName: "builtin_interfaces/msg/Time",
		Fields: []Field{
			{Name: "sec", Type: FieldType{TypeID: FieldTypeInt32}},
			{Name: "nanosec", Type: FieldType{TypeID: FieldTypeUInt32}},
		},
	}
	err := r.RegisterType(timeDesc)
	if err != nil {
		t.Fatalf("RegisterType Time failed: %v", err)
	}
	headerDesc := IndividualTypeDescription{
		TypeName: "std_msgs/msg/Header",
		Fields: []Field{
			{Name: "stamp", Type: FieldType{TypeID: FieldTypeNestedType, NestedTypeName: "builtin_interfaces/msg/Time"}},
			{Name: "frame_id", Type: FieldType{TypeID: FieldTypeString}},
		},
	}
	err = r.RegisterType(headerDesc)
	if err != nil {
		t.Fatalf("RegisterType Header failed: %v", err)
	}
}

// TestRegistryRegisterLookup checks register plus lookup round trip.
func TestRegistryRegisterLookup(t *testing.T) {
	r := NewRegistry()
	desc := IndividualTypeDescription{
		TypeName: "std_msgs/msg/String",
		Fields: []Field{
			{Name: "data", Type: FieldType{TypeID: FieldTypeString}},
		},
	}
	err := r.RegisterType(desc)
	if err != nil {
		t.Fatalf("RegisterType failed: %v", err)
	}
	got, ok := r.LookupType("std_msgs/msg/String")
	if !ok {
		t.Fatal("LookupType missing after register")
	}
	if got.TypeName != desc.TypeName || len(got.Fields) != 1 || got.Fields[0].Name != "data" {
		t.Fatalf("LookupType mismatch: %+v", got)
	}
	src := TypeSource{
		TypeName:        "std_msgs/msg/String",
		Encoding:        EncodingMsg,
		RawFileContents: "string data",
	}
	err = r.RegisterSource(src)
	if err != nil {
		t.Fatalf("RegisterSource failed: %v", err)
	}
	gotSrc, ok := r.LookupSource("std_msgs/msg/String")
	if !ok {
		t.Fatal("LookupSource missing after register")
	}
	if gotSrc.Encoding != EncodingMsg || gotSrc.RawFileContents != "string data" {
		t.Fatalf("LookupSource mismatch: %+v", gotSrc)
	}
}

// TestRegistryTransitiveClosureSorted checks sorted closure with recursion.
func TestRegistryTransitiveClosureSorted(t *testing.T) {
	r := NewRegistry()
	err := r.RegisterType(IndividualTypeDescription{
		TypeName: "pkg/msg/C",
		Fields:   []Field{{Name: "x", Type: FieldType{TypeID: FieldTypeInt32}}},
	})
	if err != nil {
		t.Fatalf("RegisterType C failed: %v", err)
	}
	err = r.RegisterType(IndividualTypeDescription{
		TypeName: "pkg/msg/B",
		Fields: []Field{
			{Name: "c", Type: FieldType{TypeID: FieldTypeNestedType, NestedTypeName: "pkg/msg/C"}},
		},
	})
	if err != nil {
		t.Fatalf("RegisterType B failed: %v", err)
	}
	err = r.RegisterType(IndividualTypeDescription{
		TypeName: "pkg/msg/A",
		Fields: []Field{
			{Name: "z", Type: FieldType{TypeID: FieldTypeNestedType, NestedTypeName: "pkg/msg/C"}},
			{Name: "b", Type: FieldType{TypeID: FieldTypeNestedType, NestedTypeName: "pkg/msg/B"}},
		},
	})
	if err != nil {
		t.Fatalf("RegisterType A failed: %v", err)
	}
	full, err := r.TransitiveClosure("pkg/msg/A")
	if err != nil {
		t.Fatalf("TransitiveClosure failed: %v", err)
	}
	if len(full.Referenced) != 2 {
		t.Fatalf("referenced count = %d, want 2", len(full.Referenced))
	}
	if full.Referenced[0].TypeName != "pkg/msg/B" || full.Referenced[1].TypeName != "pkg/msg/C" {
		t.Fatalf("unsorted refs: %q, %q", full.Referenced[0].TypeName, full.Referenced[1].TypeName)
	}
	for _, ref := range full.Referenced {
		if ref.TypeName == "pkg/msg/A" {
			t.Fatal("self reference included in closure")
		}
	}
}

// TestRegistryNameCap checks 255 byte acceptance and 256 byte rejection.
func TestRegistryNameCap(t *testing.T) {
	r := NewRegistry()
	okName := strings.Repeat("a", MaxTypeNameLength)
	err := r.RegisterType(IndividualTypeDescription{
		TypeName: okName,
		Fields:   []Field{},
	})
	if err != nil {
		t.Fatalf("255 byte name rejected: %v", err)
	}
	badName := strings.Repeat("a", MaxTypeNameLength+1)
	err = r.RegisterType(IndividualTypeDescription{
		TypeName: badName,
		Fields:   []Field{},
	})
	if err == nil {
		t.Fatal("256 byte name accepted, want rejection")
	}
	err = r.RegisterSource(TypeSource{
		TypeName:        badName,
		Encoding:        EncodingMsg,
		RawFileContents: "x",
	})
	if err == nil {
		t.Fatal("256 byte source name accepted, want rejection")
	}
	err = r.RegisterSource(TypeSource{
		TypeName:        "pkg/msg/Valid",
		Encoding:        "bogus",
		RawFileContents: "x",
	})
	if err == nil {
		t.Fatal("bogus encoding accepted, want rejection")
	}
}

// TestRegistryHashMatchesClosure checks registry hash equals direct hash.
func TestRegistryHashMatchesClosure(t *testing.T) {
	r := NewRegistry()
	registerTimeHeader(t, r)
	h, err := r.ComputeHash("std_msgs/msg/Header")
	if err != nil {
		t.Fatalf("ComputeHash failed: %v", err)
	}
	full, err := r.TransitiveClosure("std_msgs/msg/Header")
	if err != nil {
		t.Fatalf("TransitiveClosure failed: %v", err)
	}
	direct, err := ComputeTypeHash(full)
	if err != nil {
		t.Fatalf("ComputeTypeHash failed: %v", err)
	}
	if !h.Equal(direct) {
		t.Fatalf("registry hash %s differs from direct %s", h.String(), direct.String())
	}
	got, err := r.LookupWithHash("std_msgs/msg/Header", h)
	if err != nil {
		t.Fatalf("LookupWithHash failed: %v", err)
	}
	if got.Description.TypeName != "std_msgs/msg/Header" {
		t.Fatalf("LookupWithHash root = %q", got.Description.TypeName)
	}
}

// TestRegistryFullKeyedByNamePlusHash checks RegisterFull plus LookupFull.
func TestRegistryFullKeyedByNamePlusHash(t *testing.T) {
	r := NewRegistry()
	full := headerDescription()
	sources := []TypeSource{
		{TypeName: "std_msgs/msg/Header", Encoding: EncodingMsg, RawFileContents: "stamp frame_id"},
		{TypeName: "builtin_interfaces/msg/Time", Encoding: EncodingMsg, RawFileContents: "sec nanosec"},
	}
	h, err := r.RegisterFull(full, sources)
	if err != nil {
		t.Fatalf("RegisterFull failed: %v", err)
	}
	back, ok := r.LookupFull("std_msgs/msg/Header", h)
	if !ok {
		t.Fatal("LookupFull missed stored description")
	}
	if len(back.Referenced) != 1 {
		t.Fatalf("LookupFull refs = %d, want 1", len(back.Referenced))
	}
	var zero TypeHash
	_, ok = r.LookupFull("std_msgs/msg/Header", zero)
	if ok {
		t.Fatal("LookupFull hit wrong hash, want miss")
	}
}

// TestRegistryDuplicateFields checks duplicate field plus ref plus root rejection.
func TestRegistryDuplicateFields(t *testing.T) {
	dupFields := IndividualTypeDescription{
		TypeName: "pkg/msg/Dup",
		Fields: []Field{
			{Name: "x", Type: FieldType{TypeID: FieldTypeInt32}},
			{Name: "x", Type: FieldType{TypeID: FieldTypeInt32}},
		},
	}
	err := NewRegistry().RegisterType(dupFields)
	if err == nil {
		t.Fatal("duplicate field names accepted, want rejection")
	}
	dupRefs := TypeDescription{
		Description: IndividualTypeDescription{
			TypeName: "pkg/msg/A",
			Fields:   []Field{{Name: "v", Type: FieldType{TypeID: FieldTypeInt32}}},
		},
		Referenced: []IndividualTypeDescription{
			{TypeName: "pkg/msg/B", Fields: []Field{{Name: "x", Type: FieldType{TypeID: FieldTypeInt32}}}},
			{TypeName: "pkg/msg/B", Fields: []Field{{Name: "y", Type: FieldType{TypeID: FieldTypeInt32}}}},
		},
	}
	err = dupRefs.Validate()
	if err == nil {
		t.Fatal("duplicate referenced names accepted, want rejection")
	}
	rootInRefs := TypeDescription{
		Description: IndividualTypeDescription{
			TypeName: "pkg/msg/A",
			Fields:   []Field{{Name: "v", Type: FieldType{TypeID: FieldTypeInt32}}},
		},
		Referenced: []IndividualTypeDescription{
			{TypeName: "pkg/msg/A", Fields: []Field{{Name: "v", Type: FieldType{TypeID: FieldTypeInt32}}}},
		},
	}
	err = rootInRefs.Validate()
	if err == nil {
		t.Fatal("root in referenced accepted, want rejection")
	}
	unsorted := TypeDescription{
		Description: IndividualTypeDescription{
			TypeName: "pkg/msg/A",
			Fields:   []Field{{Name: "v", Type: FieldType{TypeID: FieldTypeInt32}}},
		},
		Referenced: []IndividualTypeDescription{
			{TypeName: "pkg/msg/C", Fields: []Field{{Name: "x", Type: FieldType{TypeID: FieldTypeInt32}}}},
			{TypeName: "pkg/msg/B", Fields: []Field{{Name: "x", Type: FieldType{TypeID: FieldTypeInt32}}}},
		},
	}
	err = unsorted.Validate()
	if err == nil {
		t.Fatal("unsorted refs accepted, want rejection")
	}
}

// TestRegistryTypeSourceEmptyException checks dynamic empty allowance.
func TestRegistryTypeSourceEmptyException(t *testing.T) {
	dyn := TypeSource{TypeName: "pkg/msg/X", Encoding: EncodingDynamic}
	err := dyn.Validate()
	if err != nil {
		t.Fatalf("dynamic empty rejected: %v", err)
	}
	imp := TypeSource{TypeName: "pkg/msg/X", Encoding: EncodingImplicit}
	err = imp.Validate()
	if err != nil {
		t.Fatalf("implicit empty rejected: %v", err)
	}
	msg := TypeSource{TypeName: "pkg/msg/X", Encoding: EncodingMsg}
	err = msg.Validate()
	if err == nil {
		t.Fatal("msg empty accepted, want rejection")
	}
	idl := TypeSource{TypeName: "pkg/msg/X", Encoding: EncodingIDL, RawFileContents: "data"}
	err = idl.Validate()
	if err != nil {
		t.Fatalf("idl non empty rejected: %v", err)
	}
}

// TestRegistryDanglingNested checks missing nested type errors.
func TestRegistryDanglingNested(t *testing.T) {
	r := NewRegistry()
	err := r.RegisterType(IndividualTypeDescription{
		TypeName: "pkg/msg/A",
		Fields: []Field{
			{Name: "b", Type: FieldType{TypeID: FieldTypeNestedType, NestedTypeName: "pkg/msg/Missing"}},
		},
	})
	if err != nil {
		t.Fatalf("RegisterType failed: %v", err)
	}
	_, err = r.TransitiveClosure("pkg/msg/A")
	if err == nil {
		t.Fatal("dangling nested closure accepted, want rejection")
	}
	_, err = r.ComputeHash("pkg/msg/A")
	if err == nil {
		t.Fatal("dangling nested hash accepted, want rejection")
	}
	_, err = r.LookupWithHash("pkg/msg/A", UnsetTypeHash)
	if err == nil {
		t.Fatal("dangling nested lookup accepted, want rejection")
	}
}

// TestRegistryCycleAB checks two node cycle terminates without duplication.
func TestRegistryCycleAB(t *testing.T) {
	r := NewRegistry()
	err := r.RegisterType(IndividualTypeDescription{
		TypeName: "pkg/msg/A",
		Fields: []Field{
			{Name: "b", Type: FieldType{TypeID: FieldTypeNestedType, NestedTypeName: "pkg/msg/B"}},
		},
	})
	if err != nil {
		t.Fatalf("RegisterType A failed: %v", err)
	}
	err = r.RegisterType(IndividualTypeDescription{
		TypeName: "pkg/msg/B",
		Fields: []Field{
			{Name: "a", Type: FieldType{TypeID: FieldTypeNestedType, NestedTypeName: "pkg/msg/A"}},
		},
	})
	if err != nil {
		t.Fatalf("RegisterType B failed: %v", err)
	}
	full, err := r.TransitiveClosure("pkg/msg/A")
	if err != nil {
		t.Fatalf("TransitiveClosure cycle failed: %v", err)
	}
	if len(full.Referenced) != 1 || full.Referenced[0].TypeName != "pkg/msg/B" {
		t.Fatalf("cycle refs = %+v, want [B]", full.Referenced)
	}
}

// TestRegistryDiamondDedup checks diamond dependencies dedup to one copy.
func TestRegistryDiamondDedup(t *testing.T) {
	r := NewRegistry()
	descs := []IndividualTypeDescription{
		{TypeName: "pkg/msg/D", Fields: []Field{{Name: "x", Type: FieldType{TypeID: FieldTypeInt32}}}},
		{TypeName: "pkg/msg/B", Fields: []Field{{Name: "d", Type: FieldType{TypeID: FieldTypeNestedType, NestedTypeName: "pkg/msg/D"}}}},
		{TypeName: "pkg/msg/C", Fields: []Field{{Name: "d", Type: FieldType{TypeID: FieldTypeNestedType, NestedTypeName: "pkg/msg/D"}}}},
		{
			TypeName: "pkg/msg/A",
			Fields: []Field{
				{Name: "b", Type: FieldType{TypeID: FieldTypeNestedType, NestedTypeName: "pkg/msg/B"}},
				{Name: "c", Type: FieldType{TypeID: FieldTypeNestedType, NestedTypeName: "pkg/msg/C"}},
			},
		},
	}
	for _, d := range descs {
		err := r.RegisterType(d)
		if err != nil {
			t.Fatalf("RegisterType %q failed: %v", d.TypeName, err)
		}
	}
	full, err := r.TransitiveClosure("pkg/msg/A")
	if err != nil {
		t.Fatalf("TransitiveClosure failed: %v", err)
	}
	if len(full.Referenced) != 3 {
		t.Fatalf("diamond refs = %d, want 3", len(full.Referenced))
	}
	if full.Referenced[0].TypeName != "pkg/msg/B" || full.Referenced[1].TypeName != "pkg/msg/C" || full.Referenced[2].TypeName != "pkg/msg/D" {
		t.Fatalf("diamond order wrong: %+v", full.Referenced)
	}
}

// TestRegistryOverwriteConflict checks conflicting redefinition is rejected.
func TestRegistryOverwriteConflict(t *testing.T) {
	r := NewRegistry()
	first := IndividualTypeDescription{
		TypeName: "pkg/msg/X",
		Fields:   []Field{{Name: "a", Type: FieldType{TypeID: FieldTypeInt32}}},
	}
	err := r.RegisterType(first)
	if err != nil {
		t.Fatalf("RegisterType first failed: %v", err)
	}
	err = r.RegisterType(first)
	if err != nil {
		t.Fatalf("identical redefinition rejected: %v", err)
	}
	second := IndividualTypeDescription{
		TypeName: "pkg/msg/X",
		Fields:   []Field{{Name: "b", Type: FieldType{TypeID: FieldTypeInt32}}},
	}
	err = r.RegisterType(second)
	if err == nil {
		t.Fatal("conflicting redefinition accepted, want rejection")
	}
	full := TypeDescription{
		Description: first,
		Referenced:  []IndividualTypeDescription{},
	}
	_, err = r.RegisterFull(full, nil)
	if err != nil {
		t.Fatalf("RegisterFull identical failed: %v", err)
	}
	conflict := TypeDescription{
		Description: second,
		Referenced:  []IndividualTypeDescription{},
	}
	_, err = r.RegisterFull(conflict, nil)
	if err == nil {
		t.Fatal("RegisterFull conflict accepted, want rejection")
	}
}

// TestRegistryUnsortedRegisterFull checks unsorted input stores sorted hash.
func TestRegistryUnsortedRegisterFull(t *testing.T) {
	r := NewRegistry()
	mkref := func(name string) IndividualTypeDescription {
		return IndividualTypeDescription{
			TypeName: name,
			Fields:   []Field{{Name: "x", Type: FieldType{TypeID: FieldTypeInt32}}},
		}
	}
	unsorted := TypeDescription{
		Description: IndividualTypeDescription{
			TypeName: "pkg/msg/A",
			Fields: []Field{
				{Name: "b", Type: FieldType{TypeID: FieldTypeNestedType, NestedTypeName: "pkg/msg/B"}},
				{Name: "c", Type: FieldType{TypeID: FieldTypeNestedType, NestedTypeName: "pkg/msg/C"}},
			},
		},
		Referenced: []IndividualTypeDescription{mkref("pkg/msg/C"), mkref("pkg/msg/B")},
	}
	h, err := r.RegisterFull(unsorted, nil)
	if err != nil {
		t.Fatalf("RegisterFull unsorted failed: %v", err)
	}
	sorted := TypeDescription{
		Description: unsorted.Description,
		Referenced:  []IndividualTypeDescription{mkref("pkg/msg/B"), mkref("pkg/msg/C")},
	}
	want, err := ComputeTypeHash(sorted)
	if err != nil {
		t.Fatalf("ComputeTypeHash sorted failed: %v", err)
	}
	if !h.Equal(want) {
		t.Fatalf("unsorted hash %s differs from sorted %s", h.String(), want.String())
	}
	back, ok := r.LookupFull("pkg/msg/A", h)
	if !ok {
		t.Fatal("LookupFull missed unsorted register")
	}
	if len(back.Referenced) != 2 || back.Referenced[0].TypeName != "pkg/msg/B" {
		t.Fatalf("stored refs not sorted: %+v", back.Referenced)
	}
}

// TestRegistryIncompleteClosure checks RegisterFull rejects dangling names.
func TestRegistryIncompleteClosure(t *testing.T) {
	r := NewRegistry()
	desc := TypeDescription{
		Description: IndividualTypeDescription{
			TypeName: "pkg/msg/A",
			Fields: []Field{
				{Name: "b", Type: FieldType{TypeID: FieldTypeNestedType, NestedTypeName: "pkg/msg/Missing"}},
			},
		},
		Referenced: []IndividualTypeDescription{},
	}
	_, err := r.RegisterFull(desc, nil)
	if err == nil {
		t.Fatal("incomplete closure accepted, want rejection")
	}
}

// TestRegistryCopyIsolation checks lookups return copies.
func TestRegistryCopyIsolation(t *testing.T) {
	r := NewRegistry()
	registerTimeHeader(t, r)
	got, ok := r.LookupType("builtin_interfaces/msg/Time")
	if !ok {
		t.Fatal("LookupType missed Time")
	}
	got.Fields[0].Name = "mutated"
	again, ok := r.LookupType("builtin_interfaces/msg/Time")
	if !ok {
		t.Fatal("LookupType missed Time second time")
	}
	if again.Fields[0].Name != "sec" {
		t.Fatalf("store mutated via lookup copy: %q", again.Fields[0].Name)
	}
	full, err := r.TransitiveClosure("std_msgs/msg/Header")
	if err != nil {
		t.Fatalf("TransitiveClosure failed: %v", err)
	}
	full.Referenced[0].TypeName = "mutated"
	againFull, err := r.TransitiveClosure("std_msgs/msg/Header")
	if err != nil {
		t.Fatalf("TransitiveClosure second failed: %v", err)
	}
	if againFull.Referenced[0].TypeName == "mutated" {
		t.Fatal("closure store mutated via copy")
	}
	h, err := r.ComputeHash("std_msgs/msg/Header")
	if err != nil {
		t.Fatalf("ComputeHash failed: %v", err)
	}
	stored, err := r.LookupWithHash("std_msgs/msg/Header", h)
	if err != nil {
		t.Fatalf("LookupWithHash failed: %v", err)
	}
	stored.Description.TypeName = "mutated"
	check, err := r.LookupWithHash("std_msgs/msg/Header", h)
	if err != nil {
		t.Fatalf("LookupWithHash second failed: %v", err)
	}
	if check.Description.TypeName != "std_msgs/msg/Header" {
		t.Fatal("registry mutated via LookupWithHash copy")
	}
}

// TestRegistryLookupWithHashMiss checks mismatch plus unknown distinction.
func TestRegistryLookupWithHashMiss(t *testing.T) {
	r := NewRegistry()
	registerTimeHeader(t, r)
	h, err := r.ComputeHash("std_msgs/msg/Header")
	if err != nil {
		t.Fatalf("ComputeHash failed: %v", err)
	}
	var zero TypeHash
	_, err = r.LookupWithHash("std_msgs/msg/Header", zero)
	if err == nil {
		t.Fatal("LookupWithHash zero accepted, want mismatch")
	}
	if !strings.Contains(err.Error(), "mismatch") {
		t.Fatalf("mismatch error misses word mismatch: %v", err)
	}
	_, err = r.LookupWithHash("pkg/msg/Unknown", h)
	if err == nil {
		t.Fatal("LookupWithHash unknown accepted, want error")
	}
	if strings.Contains(err.Error(), "mismatch") {
		t.Fatalf("unknown error looks like mismatch: %v", err)
	}
}

// TestRegistrySourceOverwriteWins checks source overwrite wins plus idempotence.
func TestRegistrySourceOverwriteWins(t *testing.T) {
	r := NewRegistry()
	desc := IndividualTypeDescription{
		TypeName: "pkg/msg/X",
		Fields:   []Field{{Name: "v", Type: FieldType{TypeID: FieldTypeInt32}}},
	}
	err := r.RegisterType(desc)
	if err != nil {
		t.Fatalf("RegisterType failed: %v", err)
	}
	got, ok := r.LookupSource("pkg/msg/X")
	if !ok {
		t.Fatal("LookupSource missed default dynamic source")
	}
	if got.Encoding != EncodingDynamic {
		t.Fatalf("default source encoding = %q, want dynamic", string(got.Encoding))
	}
	first := TypeSource{
		TypeName:        "pkg/msg/X",
		Encoding:        EncodingMsg,
		RawFileContents: "int32 v",
	}
	err = r.RegisterSource(first)
	if err != nil {
		t.Fatalf("RegisterSource first failed: %v", err)
	}
	got, ok = r.LookupSource("pkg/msg/X")
	if !ok {
		t.Fatal("LookupSource missed after first source")
	}
	if got.RawFileContents != "int32 v" || got.Encoding != EncodingMsg {
		t.Fatalf("source mismatch after first: %+v", got)
	}
	err = r.RegisterSource(first)
	if err != nil {
		t.Fatalf("identical source re-register failed: %v", err)
	}
	second := TypeSource{
		TypeName:        "pkg/msg/X",
		Encoding:        EncodingMsg,
		RawFileContents: "int32 v2",
	}
	err = r.RegisterSource(second)
	if err != nil {
		t.Fatalf("RegisterSource overwrite failed: %v", err)
	}
	got, ok = r.LookupSource("pkg/msg/X")
	if !ok {
		t.Fatal("LookupSource missed after overwrite")
	}
	if got.RawFileContents != "int32 v2" {
		t.Fatalf("overwrite did not win: %+v", got)
	}
}

// TestRegistryLookupFullSourceIsolation checks LookupFull plus LookupSource copies.
func TestRegistryLookupFullSourceIsolation(t *testing.T) {
	r := NewRegistry()
	full := headerDescription()
	sources := []TypeSource{
		{TypeName: "std_msgs/msg/Header", Encoding: EncodingMsg, RawFileContents: "stamp frame_id"},
		{TypeName: "builtin_interfaces/msg/Time", Encoding: EncodingMsg, RawFileContents: "sec nanosec"},
	}
	h, err := r.RegisterFull(full, sources)
	if err != nil {
		t.Fatalf("RegisterFull failed: %v", err)
	}
	back, ok := r.LookupFull("std_msgs/msg/Header", h)
	if !ok {
		t.Fatal("LookupFull missed stored description")
	}
	back.Description.TypeName = "mutated"
	back.Description.Fields[0].Name = "mutated"
	back.Referenced[0].TypeName = "mutated"
	again, ok := r.LookupFull("std_msgs/msg/Header", h)
	if !ok {
		t.Fatal("LookupFull missed second lookup")
	}
	if again.Description.TypeName != "std_msgs/msg/Header" {
		t.Fatalf("LookupFull store mutated via description: %q", again.Description.TypeName)
	}
	if again.Description.Fields[0].Name != "stamp" {
		t.Fatalf("LookupFull store mutated via field: %q", again.Description.Fields[0].Name)
	}
	if again.Referenced[0].TypeName == "mutated" {
		t.Fatal("LookupFull store mutated via referenced")
	}
	src, ok := r.LookupSource("std_msgs/msg/Header")
	if !ok {
		t.Fatal("LookupSource missed")
	}
	src.Encoding = EncodingIDL
	src.RawFileContents = "mutated"
	check, ok := r.LookupSource("std_msgs/msg/Header")
	if !ok {
		t.Fatal("LookupSource missed second lookup")
	}
	if check.Encoding != EncodingMsg || check.RawFileContents == "mutated" {
		t.Fatalf("LookupSource store mutated via copy: %+v", check)
	}
}

// TestRegistryConcurrentFull checks concurrent full plus hash plus source safety.
// checkConcurrentFullOnce runs one concurrent iteration.
func checkConcurrentFullOnce(t *testing.T, r *Registry, full TypeDescription, sources []TypeSource, h TypeHash) {
	t.Helper()
	_, err := r.RegisterFull(full, sources)
	if err != nil {
		t.Errorf("concurrent RegisterFull failed: %v", err)
		return
	}
	checkConcurrentFullReads(t, r, h)
	src := TypeSource{
		TypeName:        "std_msgs/msg/Header",
		Encoding:        EncodingMsg,
		RawFileContents: "stamp frame_id",
	}
	err = r.RegisterSource(src)
	if err != nil {
		t.Errorf("concurrent RegisterSource failed: %v", err)
		return
	}
	if _, ok := r.LookupSource("std_msgs/msg/Header"); !ok {
		t.Errorf("concurrent LookupSource missed")
	}
}

// checkConcurrentFullReads verifies concurrent reads.
func checkConcurrentFullReads(t *testing.T, r *Registry, h TypeHash) {
	t.Helper()
	if _, ok := r.LookupFull("std_msgs/msg/Header", h); !ok {
		t.Errorf("concurrent LookupFull missed")
	}
	if _, err := r.ComputeHash("std_msgs/msg/Header"); err != nil {
		t.Errorf("concurrent ComputeHash failed: %v", err)
	}
	if _, err := r.LookupWithHash("std_msgs/msg/Header", h); err != nil {
		t.Errorf("concurrent LookupWithHash failed: %v", err)
	}
}

func TestRegistryConcurrentFull(t *testing.T) {
	r := NewRegistry()
	full := headerDescription()
	sources := []TypeSource{
		{TypeName: "std_msgs/msg/Header", Encoding: EncodingMsg, RawFileContents: "stamp frame_id"},
		{TypeName: "builtin_interfaces/msg/Time", Encoding: EncodingMsg, RawFileContents: "sec nanosec"},
	}
	h, err := r.RegisterFull(full, sources)
	if err != nil {
		t.Fatalf("RegisterFull failed: %v", err)
	}
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			checkConcurrentFullOnce(t, r, full, sources, h)
		})
	}
	wg.Wait()
	back, ok := r.LookupFull("std_msgs/msg/Header", h)
	if !ok {
		t.Fatal("LookupFull missed after concurrent use")
	}
	if len(back.Referenced) != 1 {
		t.Fatalf("concurrent refs = %d, want 1", len(back.Referenced))
	}
}

// TestRegistryConcurrent checks concurrent register plus lookup safety.
func TestRegistryConcurrent(t *testing.T) {
	r := NewRegistry()
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			desc := IndividualTypeDescription{
				TypeName: "pkg/msg/Shared",
				Fields:   []Field{{Name: "x", Type: FieldType{TypeID: FieldTypeInt32}}},
			}
			err := r.RegisterType(desc)
			if err != nil {
				t.Errorf("concurrent RegisterType failed: %v", err)
				return
			}
			if _, ok := r.LookupType("pkg/msg/Shared"); !ok {
				t.Errorf("concurrent LookupType missed")
			}
			if _, err := r.TransitiveClosure("pkg/msg/Shared"); err != nil {
				t.Errorf("concurrent TransitiveClosure failed: %v", err)
			}
		})
	}
	wg.Wait()
	if r.Len() != 1 {
		t.Fatalf("concurrent len = %d, want 1", r.Len())
	}
}
