// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

package types

import (
	"strings"
	"testing"
)

// TestMapxt6aPortableKeys pins the portable key whitelist.
func TestMapxt6aPortableKeys(t *testing.T) {
	t.Run("description", checkMapxt6aDescriptionKeys)
	t.Run("ros", checkMapxt6aRosKeys)
	t.Run("wrappers", checkMapxt6aWrapperKeys)
}

// checkMapxt6aDescriptionKeys verifies portable description key bases.
func checkMapxt6aDescriptionKeys(t *testing.T) {
	t.Helper()
	good := []FieldTypeID{
		FieldTypeInt8, FieldTypeUInt8, FieldTypeInt16, FieldTypeUInt16,
		FieldTypeInt32, FieldTypeUInt32, FieldTypeInt64, FieldTypeUInt64,
		FieldTypeString, FieldTypeWString, FieldTypeFixedString, FieldTypeFixedWString,
		FieldTypeBoundedString, FieldTypeBoundedWString,
	}
	for _, id := range good {
		if !IsPortablext6aMapKey(id) {
			t.Fatalf("portable key %d rejected", uint8(id))
		}
	}
	bad := []FieldTypeID{
		FieldTypeFloat, FieldTypeDouble, FieldTypeBoolean, FieldTypeChar,
		FieldTypeWChar, FieldTypeByte, FieldTypeNestedType,
	}
	for _, id := range bad {
		if IsPortablext6aMapKey(id) {
			t.Fatalf("non-portable key %d accepted", uint8(id))
		}
	}
}

// checkMapxt6aRosKeys verifies portable introspection key kinds.
func checkMapxt6aRosKeys(t *testing.T) {
	t.Helper()
	for _, kind := range []RosTypeID{RosTypeInt32, RosTypeUint64, RosTypeOctet, RosTypeString, RosTypeWString} {
		if !IsPortablext6aRosMapKey(kind) {
			t.Fatalf("portable ros key %s rejected", kind.String())
		}
	}
	for _, kind := range []RosTypeID{RosTypeFloat, RosTypeBoolean, RosTypeMessage, RosTypeChar, RosTypeWChar} {
		if IsPortablext6aRosMapKey(kind) {
			t.Fatalf("non-portable ros key %s accepted", kind.String())
		}
	}
}

// checkMapxt6aWrapperKeys rejects array and sequence key wrappers.
func checkMapxt6aWrapperKeys(t *testing.T) {
	t.Helper()
	arr, ok := ArrayOf(FieldTypeInt32)
	if !ok {
		t.Fatal("ArrayOf Int32 rejected")
	}
	if IsPortablext6aMapKey(arr) {
		t.Fatal("array of int accepted as key, want rejection")
	}
	seq, ok := UnboundedSequenceOf(FieldTypeString)
	if !ok {
		t.Fatal("UnboundedSequenceOf String rejected")
	}
	if IsPortablext6aMapKey(seq) {
		t.Fatal("string sequence accepted as key, want rejection")
	}
}

// TestMapxt6aSortedKeys pins strictly increasing numeric key order checks.
//
// Numeric order is canonical per 7.2.3.2.2.1 rules 14 to 16: LE and BE agree
// on value order, unlike byte-lexicographic order which is endian-dependent
// for multi-byte keys. The deprecated byte helper is deleted.
func TestMapxt6aSortedKeys(t *testing.T) {
	err := CheckSortedxt6aMapKeysNumeric(RosTypeUint8, nil, true)
	if err != nil {
		t.Fatalf("empty keys rejected: %v", err)
	}
	err = CheckSortedxt6aMapKeysNumeric(RosTypeUint8, [][]byte{{0x01}}, true)
	if err != nil {
		t.Fatalf("single key rejected: %v", err)
	}
	err = CheckSortedxt6aMapKeysNumeric(RosTypeUint8, [][]byte{{0x01}, {0x02}, {0x03}}, true)
	if err != nil {
		t.Fatalf("sorted keys rejected: %v", err)
	}
	err = CheckSortedxt6aMapKeysNumeric(RosTypeUint8, [][]byte{{0x02}, {0x01}}, true)
	if err == nil {
		t.Fatal("unsorted keys accepted, want rejection")
	}
	err = CheckSortedxt6aMapKeysNumeric(RosTypeUint8, [][]byte{{0x01}, {0x01}}, true)
	if err == nil {
		t.Fatal("duplicate keys accepted, want rejection")
	}
	// LE and BE agree on numeric order for multi-byte keys.
	leKeys := [][]byte{{0x01, 0x00, 0x00, 0x00}, {0x02, 0x00, 0x00, 0x00}}
	beKeys := [][]byte{{0x00, 0x00, 0x00, 0x01}, {0x00, 0x00, 0x00, 0x02}}
	if err := CheckSortedxt6aMapKeysNumeric(RosTypeInt32, leKeys, true); err != nil {
		t.Fatalf("le numeric order rejected: %v", err)
	}
	if err := CheckSortedxt6aMapKeysNumeric(RosTypeInt32, beKeys, false); err != nil {
		t.Fatalf("be numeric order rejected: %v", err)
	}
}

// TestUnitsxt6aSyntax pins unit string validation vectors.
func TestUnitsxt6aSyntax(t *testing.T) {
	for _, s := range []string{"", "degrees", "meter; m", "watt; W; J/s", "__ANY__", " meter ;m ", "km/h"} {
		err := Validatext6aUnits(s)
		if err != nil {
			t.Fatalf("units %q rejected: %v", s, err)
		}
	}
	for _, s := range []string{"a;", ";a", "a;;b", "a\x01b", "a\x7fb", "meter; m;"} {
		err := Validatext6aUnits(s)
		if err == nil {
			t.Fatalf("units %q accepted, want rejection", s)
		}
	}
}

// TestUnitsxt6aHashIgnored pins units as additive metadata.
func TestUnitsxt6aHashIgnored(t *testing.T) {
	base := int32Description()
	withUnits := int32Description()
	withUnits.Description.Units = "meter; m"
	withUnits.Description.Fields[0].Units = "m"
	withUnits.Description.Fields[0].MemberID = 1
	withUnits.Description.Fields[0].Optional = false
	h1, err := ComputeTypeHash(base)
	if err != nil {
		t.Fatalf("ComputeTypeHash base failed: %v", err)
	}
	h2, err := ComputeTypeHash(withUnits)
	if err != nil {
		t.Fatalf("ComputeTypeHash units failed: %v", err)
	}
	if !h1.Equal(h2) {
		t.Fatalf("units changed hash: %s vs %s", h1.String(), h2.String())
	}
	s, err := CanonicalJSON(withUnits)
	if err != nil {
		t.Fatalf("CanonicalJSON failed: %v", err)
	}
	if strings.Contains(s, "units") || strings.Contains(s, "member_id") {
		t.Fatalf("canonical JSON leaks metadata: %s", s)
	}
}

// TestMemberxt6aMeta pins member id, optional, and extensibility rules.
func TestMemberxt6aMeta(t *testing.T) {
	badID := Field{Name: "x", Type: FieldType{TypeID: FieldTypeInt32}, MemberID: maxxt6aMemberID + 1}
	err := badID.Validate()
	if err == nil {
		t.Fatal("oversize member id accepted, want rejection")
	}
	optDefault := Field{Name: "x", Type: FieldType{TypeID: FieldTypeInt32}, Optional: true, DefaultValue: "1"}
	err = optDefault.Validate()
	if err == nil {
		t.Fatal("optional with default accepted, want rejection")
	}
	opt := Field{Name: "x", Type: FieldType{TypeID: FieldTypeInt32}, Optional: true}
	err = opt.Validate()
	if err != nil {
		t.Fatalf("plain optional rejected: %v", err)
	}
	dup := IndividualTypeDescription{
		TypeName: "pkg/msg/Dup",
		Fields: []Field{
			{Name: "a", Type: FieldType{TypeID: FieldTypeInt32}, MemberID: 3},
			{Name: "b", Type: FieldType{TypeID: FieldTypeInt32}, MemberID: 3},
		},
	}
	err = dup.Validate()
	if err == nil {
		t.Fatal("duplicate member ids accepted, want rejection")
	}
	zeros := IndividualTypeDescription{
		TypeName: "pkg/msg/Z",
		Fields: []Field{
			{Name: "a", Type: FieldType{TypeID: FieldTypeInt32}},
			{Name: "b", Type: FieldType{TypeID: FieldTypeInt32}},
		},
	}
	err = zeros.Validate()
	if err != nil {
		t.Fatalf("unset member ids rejected: %v", err)
	}
	badExt := IndividualTypeDescription{TypeName: "pkg/msg/E", Extensibility: Extensibility(9)}
	err = badExt.Validate()
	if err == nil {
		t.Fatal("bad extensibility accepted, want rejection")
	}
	withExt := IndividualTypeDescription{TypeName: "pkg/msg/M", Extensibility: ExtMutable}
	err = withExt.Validate()
	if err != nil {
		t.Fatalf("mutable extensibility rejected: %v", err)
	}
}

// TestEnumxt6aWidths pins bound selected width tables.
func TestEnumxt6aWidths(t *testing.T) {
	for bound, want := range map[uint32]int{1: 1, 8: 1, 9: 2, 16: 2, 17: 4, 32: 4} {
		got, err := Enumxt6aWidth(bound)
		if err != nil {
			t.Fatalf("enum bound %d rejected: %v", bound, err)
		}
		if got != want {
			t.Fatalf("enum bound %d width %d, want %d", bound, got, want)
		}
	}
	for _, bound := range []uint32{0, 33, 64} {
		_, err := Enumxt6aWidth(bound)
		if err == nil {
			t.Fatalf("enum bound %d accepted, want rejection", bound)
		}
	}
	dup := Enumxt6aType{BitBound: 8, Literals: []Enumxt6aLiteral{{Name: "A", Value: 0}, {Name: "A", Value: 1}}}
	err := dup.Validate()
	if err == nil {
		t.Fatal("duplicate enum name accepted, want rejection")
	}
	dupVal := Enumxt6aType{BitBound: 8, Literals: []Enumxt6aLiteral{{Name: "A", Value: 0}, {Name: "B", Value: 0}}}
	err = dupVal.Validate()
	if err == nil {
		t.Fatal("duplicate enum value accepted, want rejection")
	}
	good := Enumxt6aType{BitBound: 16, Literals: []Enumxt6aLiteral{{Name: "A", Value: 0}, {Name: "B", Value: 3}}}
	err = good.Validate()
	if err != nil {
		t.Fatalf("valid enum rejected: %v", err)
	}
	w, err := good.Width()
	if err != nil {
		t.Fatalf("enum width failed: %v", err)
	}
	if w != 2 {
		t.Fatalf("enum width %d, want 2", w)
	}
}

// TestBitmaskxt6aWidths pins Table 31 width selection.
func TestBitmaskxt6aWidths(t *testing.T) {
	for bound, want := range map[uint32]int{1: 1, 8: 1, 9: 2, 16: 2, 17: 4, 32: 4, 33: 8, 64: 8} {
		got, err := Bitmaskxt6aWidth(bound)
		if err != nil {
			t.Fatalf("bitmask bound %d rejected: %v", bound, err)
		}
		if got != want {
			t.Fatalf("bitmask bound %d width %d, want %d", bound, got, want)
		}
	}
	for _, bound := range []uint32{0, 65} {
		_, err := Bitmaskxt6aWidth(bound)
		if err == nil {
			t.Fatalf("bitmask bound %d accepted, want rejection", bound)
		}
	}
	outside := Bitmaskxt6aType{BitBound: 8, Flags: []Bitmaskxt6aFlag{{Name: "F", Position: 8}}}
	err := outside.Validate()
	if err == nil {
		t.Fatal("flag outside bound accepted, want rejection")
	}
	good := Bitmaskxt6aType{BitBound: 16, Flags: []Bitmaskxt6aFlag{{Name: "A", Position: 0}, {Name: "B", Position: 15}}}
	err = good.Validate()
	if err != nil {
		t.Fatalf("valid bitmask rejected: %v", err)
	}
	w, err := good.Width()
	if err != nil {
		t.Fatalf("bitmask width failed: %v", err)
	}
	if w != 2 {
		t.Fatalf("bitmask width %d, want 2", w)
	}
}

// TestKeyHolderxt6aProjection pins pure bytes key holder projection.
func TestKeyHolderxt6aProjection(t *testing.T) {
	chunks := map[string][]byte{
		"seq":   {0x01, 0x02},
		"count": {0x03},
	}
	got, err := Projectxt6aKeyHolder(chunks, []string{"seq", "count"})
	if err != nil {
		t.Fatalf("projection failed: %v", err)
	}
	if len(got) != 3 || got[0] != 0x01 || got[1] != 0x02 || got[2] != 0x03 {
		t.Fatalf("projection bytes wrong: % x", got)
	}
	_, err = Projectxt6aKeyHolder(chunks, []string{"seq", "missing"})
	if err == nil {
		t.Fatal("missing path accepted, want rejection")
	}
	_, err = Projectxt6aKeyHolder(chunks, nil)
	if err == nil {
		t.Fatal("empty paths accepted, want rejection")
	}
	key := ComputeKeyHash(got)
	if key[0] != 0x01 || key[1] != 0x02 || key[2] != 0x03 || key[3] != 0x00 {
		t.Fatalf("projected keyhash wrong: %x", key)
	}
}

// TestEquivalencext6aHash pins the EQM14_ model hash.
func TestEquivalencext6aHash(t *testing.T) {
	h1, err := ComputeEquivalencext6aHash(int32Description())
	if err != nil {
		t.Fatalf("equivalence failed: %v", err)
	}
	if len(h1.Digest) != 14 {
		t.Fatalf("equivalence digest %d bytes, want 14", len(h1.Digest))
	}
	back, err := ParseEquivalencext6aHash(h1.String())
	if err != nil {
		t.Fatalf("parse equivalence failed: %v", err)
	}
	if !h1.Equal(back) {
		t.Fatal("equivalence round trip mismatch")
	}
	h2, err := ComputeEquivalencext6aHash(timeDescription())
	if err != nil {
		t.Fatalf("equivalence time failed: %v", err)
	}
	if h1.Equal(h2) {
		t.Fatal("distinct types share equivalence hash")
	}
	_, err = ParseEquivalencext6aHash("RIHS01_bogus")
	if err == nil {
		t.Fatal("bad prefix accepted, want rejection")
	}
	_, err = ParseEquivalencext6aHash("EQM14_short")
	if err == nil {
		t.Fatal("short hash accepted, want rejection")
	}
}

// TestRegistryxt6aDualHash pins the dual hash index behavior.
func TestRegistryxt6aDualHash(t *testing.T) {
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
	want, err := ComputeTypeHash(TypeDescription{Description: full.Description, Referenced: full.Referenced})
	if err != nil {
		t.Fatalf("direct hash failed: %v", err)
	}
	if !h.Equal(want) {
		t.Fatalf("RIHS01 changed: %s vs %s", h.String(), want.String())
	}
	eq, err := r.ComputeEquivalencext6a("std_msgs/msg/Header")
	if err != nil {
		t.Fatalf("ComputeEquivalence failed: %v", err)
	}
	direct, err := ComputeEquivalencext6aHash(full)
	if err != nil {
		t.Fatalf("direct equivalence failed: %v", err)
	}
	if !eq.Equal(direct) {
		t.Fatal("registry equivalence differs from direct")
	}
	back, ok := r.LookupFullByEquivalencext6a("std_msgs/msg/Header", eq)
	if !ok {
		t.Fatal("LookupFullByEquivalence missed")
	}
	if back.Description.TypeName != "std_msgs/msg/Header" {
		t.Fatalf("equivalence lookup root %q", back.Description.TypeName)
	}
	var zero Equivalencext6aHash
	_, ok = r.LookupFullByEquivalencext6a("std_msgs/msg/Header", zero)
	if ok {
		t.Fatal("equivalence lookup hit zero hash, want miss")
	}
	_, ok = r.LookupFull("std_msgs/msg/Header", h)
	if !ok {
		t.Fatal("RIHS01 LookupFull missed after dual index")
	}
}
