// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package types

import (
	"strings"
	"testing"
)

// int32Description returns the canonical Int32 test description.
func int32Description() TypeDescription {
	return TypeDescription{
		Description: IndividualTypeDescription{
			TypeName: "std_msgs/msg/Int32",
			Fields: []Field{
				{
					Name: "data",
					Type: FieldType{
						TypeID:         FieldTypeInt32,
						Capacity:       0,
						StringCapacity: 0,
						NestedTypeName: "",
					},
				},
			},
		},
		Referenced: []IndividualTypeDescription{},
	}
}

// timeDescription returns the canonical Time test description.
func timeDescription() TypeDescription {
	return TypeDescription{
		Description: IndividualTypeDescription{
			TypeName: "builtin_interfaces/msg/Time",
			Fields: []Field{
				{
					Name: "sec",
					Type: FieldType{TypeID: FieldTypeInt32},
				},
				{
					Name: "nanosec",
					Type: FieldType{TypeID: FieldTypeUInt32},
				},
			},
		},
		Referenced: []IndividualTypeDescription{},
	}
}

// headerDescription returns the canonical Header test description.
func headerDescription() TypeDescription {
	timeITD := timeDescription().Description
	return TypeDescription{
		Description: IndividualTypeDescription{
			TypeName: "std_msgs/msg/Header",
			Fields: []Field{
				{
					Name: "stamp",
					Type: FieldType{
						TypeID:         FieldTypeNestedType,
						NestedTypeName: "builtin_interfaces/msg/Time",
					},
				},
				{
					Name: "frame_id",
					Type: FieldType{TypeID: FieldTypeString},
				},
			},
		},
		Referenced: []IndividualTypeDescription{timeITD},
	}
}

// TestHashStringRoundTrip checks string form length plus prefix plus parse back.
func TestHashStringRoundTrip(t *testing.T) {
	h, err := ComputeTypeHash(int32Description())
	if err != nil {
		t.Fatalf("ComputeTypeHash failed: %v", err)
	}
	s := h.String()
	if len(s) != HashStringLength {
		t.Fatalf("hash string length = %d, want %d", len(s), HashStringLength)
	}
	if !strings.HasPrefix(s, HashPrefix) {
		t.Fatalf("hash string %q misses prefix %q", s, HashPrefix)
	}
	back, err := ParseTypeHash(s)
	if err != nil {
		t.Fatalf("ParseTypeHash failed: %v", err)
	}
	if !h.Equal(back) {
		t.Fatalf("round trip mismatch: %v vs %v", h, back)
	}
}

// TestHashRejectBadVersionLength checks that bad inputs are rejected.
func TestHashRejectBadVersionLength(t *testing.T) {
	good, err := ComputeTypeHash(int32Description())
	if err != nil {
		t.Fatalf("ComputeTypeHash failed: %v", err)
	}
	goodStr := good.String()
	cases := []string{
		"short",
		goodStr[:len(goodStr)-1],
		goodStr + "0",
		"RIHS00_" + goodStr[len(HashPrefix):],
		"RIHS02_" + goodStr[len(HashPrefix):],
		"XXXX01_" + goodStr[len(HashPrefix):],
		strings.ToUpper(goodStr),
		HashPrefix + strings.Repeat("zz", 32),
	}
	for i, c := range cases {
		_, err = ParseTypeHash(c)
		if err == nil {
			t.Fatalf("case %d %q accepted, want rejection", i, c)
		}
	}
}

// TestHashUnsetEncoding checks empty string maps to unset and back.
func TestHashUnsetEncoding(t *testing.T) {
	if UnsetTypeHash.String() != "" {
		t.Fatalf("unset String = %q, want empty", UnsetTypeHash.String())
	}
	if !UnsetTypeHash.IsUnset() {
		t.Fatal("unset IsUnset = false, want true")
	}
	h, err := ParseTypeHash("")
	if err != nil {
		t.Fatalf("ParseTypeHash empty failed: %v", err)
	}
	if !h.IsUnset() {
		t.Fatalf("Parse empty = %+v, want unset", h)
	}
}

// TestHashDefaultsDeletedDeterminism checks defaults never affect the digest.
func TestHashDefaultsDeletedDeterminism(t *testing.T) {
	base := int32Description()
	alt := int32Description()
	alt.Description.Fields[0].DefaultValue = "42"
	h1, err := ComputeTypeHash(base)
	if err != nil {
		t.Fatalf("ComputeTypeHash base failed: %v", err)
	}
	h2, err := ComputeTypeHash(alt)
	if err != nil {
		t.Fatalf("ComputeTypeHash alt failed: %v", err)
	}
	if !h1.Equal(h2) {
		t.Fatalf("default value changed hash: %s vs %s", h1.String(), h2.String())
	}
	s, err := CanonicalJSON(alt)
	if err != nil {
		t.Fatalf("CanonicalJSON failed: %v", err)
	}
	if strings.Contains(s, "default_value") {
		t.Fatalf("canonical JSON contains default_value: %s", s)
	}
}

// TestHashReferenceInt32 checks the Jazzy reference hash for Int32.
func TestHashReferenceInt32(t *testing.T) {
	h, err := ComputeTypeHash(int32Description())
	if err != nil {
		t.Fatalf("ComputeTypeHash failed: %v", err)
	}
	want := "RIHS01_b6578ded3c58c626cfe8d1a6fb6e04f706f97e9f03d2727c9ff4e74b1cef0deb"
	if h.String() != want {
		t.Fatalf("Int32 hash = %s, want %s", h.String(), want)
	}
}

// TestHashReferenceTime checks the Jazzy reference hash for Time.
func TestHashReferenceTime(t *testing.T) {
	h, err := ComputeTypeHash(timeDescription())
	if err != nil {
		t.Fatalf("ComputeTypeHash failed: %v", err)
	}
	want := "RIHS01_b106235e25a4c5ed35098aa0a61a3ee9c9b18d197f398b0e4206cea9acf9c197"
	if h.String() != want {
		t.Fatalf("Time hash = %s, want %s", h.String(), want)
	}
}

// TestHashReferenceHeader checks the Jazzy reference hash for Header.
func TestHashReferenceHeader(t *testing.T) {
	h, err := ComputeTypeHash(headerDescription())
	if err != nil {
		t.Fatalf("ComputeTypeHash failed: %v", err)
	}
	want := "RIHS01_f49fb3ae2cf070f793645ff749683ac6b06203e41c891e17701b1cb597ce6a01"
	if h.String() != want {
		t.Fatalf("Header hash = %s, want %s", h.String(), want)
	}
}

// TestHashCanonicalSeparators checks comma space and colon space separators.
func TestHashCanonicalSeparators(t *testing.T) {
	s, err := CanonicalJSON(int32Description())
	if err != nil {
		t.Fatalf("CanonicalJSON failed: %v", err)
	}
	want := `{"type_description": {"type_name": "std_msgs/msg/Int32", "fields": [{"name": "data", "type": {"type_id": 6, "capacity": 0, "string_capacity": 0, "nested_type_name": ""}}]}, "referenced_type_descriptions": []}`
	if s != want {
		t.Fatalf("canonical JSON mismatch:\n got %s\nwant %s", s, want)
	}
}

// TestHashStaticTable checks 8 bytes per row over 4 rows and reassembly.
func TestHashStaticTable(t *testing.T) {
	h, err := ComputeTypeHash(int32Description())
	if err != nil {
		t.Fatalf("ComputeTypeHash failed: %v", err)
	}
	rows := h.ByteRows()
	var flat [32]byte
	for r := range 4 {
		copy(flat[r*8:(r+1)*8], rows[r][:])
	}
	if flat != h.Digest {
		t.Fatal("ByteRows reassembly differs from digest")
	}
	table := h.CTable()
	lines := strings.Split(strings.TrimSpace(table), "\n")
	if len(lines) != 6 {
		t.Fatalf("CTable lines = %d, want 6 with braces plus 4 rows", len(lines))
	}
}

// TestHashCTableGolden checks the exact C table for the Int32 digest.
func TestHashCTableGolden(t *testing.T) {
	h, err := ComputeTypeHash(int32Description())
	if err != nil {
		t.Fatalf("ComputeTypeHash failed: %v", err)
	}
	got := h.CTable()
	want := "{\n" +
		"    0xb6, 0x57, 0x8d, 0xed, 0x3c, 0x58, 0xc6, 0x26,\n" +
		"    0xcf, 0xe8, 0xd1, 0xa6, 0xfb, 0x6e, 0x04, 0xf7,\n" +
		"    0x06, 0xf9, 0x7e, 0x9f, 0x03, 0xd2, 0x72, 0x7c,\n" +
		"    0x9f, 0xf4, 0xe7, 0x4b, 0x1c, 0xef, 0x0d, 0xeb\n" +
		"}"
	if got != want {
		t.Fatalf("CTable golden mismatch:\n got %q\nwant %q", got, want)
	}
	if strings.Contains(got, ",,") {
		t.Fatalf("CTable contains double comma: %q", got)
	}
}

// TestHashASCIIEscapes checks ASCII control plus quote escaping in JSON strings.
func TestHashASCIIEscapes(t *testing.T) {
	var b strings.Builder
	writeJSONString(&b, "a\"b\\c")
	if b.String() != `"a\"b\\c"` {
		t.Fatalf("quote escape = %q", b.String())
	}
	b.Reset()
	writeJSONString(&b, "x\ny\tz\rb\fa\b")
	if b.String() != `"x\ny\tz\rb\fa\b"` {
		t.Fatalf("control escape = %q", b.String())
	}
	b.Reset()
	writeJSONString(&b, "\x01\x7f")
	if b.String() != `"\u0001\u007f"` {
		t.Fatalf("ascii escape = %q", b.String())
	}
	b.Reset()
	writeJSONString(&b, "caf\u00e9")
	if b.String() != `"caf\u00e9"` {
		t.Fatalf("latin escape = %q", b.String())
	}
	b.Reset()
	writeJSONString(&b, "\U0001f600")
	if b.String() != `"\ud83d\ude00"` {
		t.Fatalf("surrogate escape = %q", b.String())
	}
}

// TestHashNilVsEmptyCanonicalEquality checks nil and empty slices encode equally.
func TestHashNilVsEmptyCanonicalEquality(t *testing.T) {
	withEmpty := int32Description()
	withNil := TypeDescription{
		Description: IndividualTypeDescription{
			TypeName: "std_msgs/msg/Int32",
			Fields: []Field{
				{Name: "data", Type: FieldType{TypeID: FieldTypeInt32}},
			},
		},
	}
	withNil.Description.Fields = nil
	withNil.Description.Fields = append(withNil.Description.Fields, Field{Name: "data", Type: FieldType{TypeID: FieldTypeInt32}})
	withNil.Referenced = nil
	sEmpty, err := CanonicalJSON(withEmpty)
	if err != nil {
		t.Fatalf("CanonicalJSON empty failed: %v", err)
	}
	sNil, err := CanonicalJSON(withNil)
	if err != nil {
		t.Fatalf("CanonicalJSON nil failed: %v", err)
	}
	if sEmpty != sNil {
		t.Fatalf("nil vs empty canonical differs:\n empty %s\n nil %s", sEmpty, sNil)
	}
	hEmpty, err := ComputeTypeHash(withEmpty)
	if err != nil {
		t.Fatalf("ComputeTypeHash empty failed: %v", err)
	}
	hNil, err := ComputeTypeHash(withNil)
	if err != nil {
		t.Fatalf("ComputeTypeHash nil failed: %v", err)
	}
	if !hEmpty.Equal(hNil) {
		t.Fatal("nil vs empty hash differs")
	}
	blank := TypeDescription{
		Description: IndividualTypeDescription{TypeName: "pkg/msg/Blank"},
		Referenced:  nil,
	}
	blank.Description.Fields = nil
	sBlank, err := CanonicalJSON(blank)
	if err != nil {
		t.Fatalf("CanonicalJSON blank failed: %v", err)
	}
	if !strings.Contains(sBlank, `"fields": []`) {
		t.Fatalf("blank fields not empty array: %s", sBlank)
	}
}

// TestHashUnsortedRefsDeterminism checks caller order never affects the hash.
func TestHashUnsortedRefsDeterminism(t *testing.T) {
	mkref := func(name string) IndividualTypeDescription {
		return IndividualTypeDescription{
			TypeName: name,
			Fields:   []Field{{Name: "x", Type: FieldType{TypeID: FieldTypeInt32}}},
		}
	}
	base := TypeDescription{
		Description: IndividualTypeDescription{
			TypeName: "pkg/msg/A",
			Fields:   []Field{{Name: "v", Type: FieldType{TypeID: FieldTypeInt32}}},
		},
		Referenced: []IndividualTypeDescription{mkref("pkg/msg/B"), mkref("pkg/msg/C")},
	}
	swapped := TypeDescription{
		Description: base.Description,
		Referenced:  []IndividualTypeDescription{mkref("pkg/msg/C"), mkref("pkg/msg/B")},
	}
	s1, err := CanonicalJSON(base)
	if err != nil {
		t.Fatalf("CanonicalJSON base failed: %v", err)
	}
	s2, err := CanonicalJSON(swapped)
	if err != nil {
		t.Fatalf("CanonicalJSON swapped failed: %v", err)
	}
	if s1 != s2 {
		t.Fatalf("unsorted refs not deterministic:\n %s\n %s", s1, s2)
	}
	h1, err := ComputeTypeHash(base)
	if err != nil {
		t.Fatalf("ComputeTypeHash base failed: %v", err)
	}
	h2, err := ComputeTypeHash(swapped)
	if err != nil {
		t.Fatalf("ComputeTypeHash swapped failed: %v", err)
	}
	if !h1.Equal(h2) {
		t.Fatal("unsorted refs hash differs")
	}
}

// TestHashVersionedBytesEqual checks versioned bytes plus cross version inequality.
func TestHashVersionedBytesEqual(t *testing.T) {
	h, err := ComputeTypeHash(int32Description())
	if err != nil {
		t.Fatalf("ComputeTypeHash failed: %v", err)
	}
	vb := h.VersionedBytes()
	if vb[0] != HashVersion1 {
		t.Fatalf("version byte = %d, want %d", vb[0], HashVersion1)
	}
	var digest [32]byte
	copy(digest[:], vb[1:])
	if digest != h.Digest {
		t.Fatal("VersionedBytes digest differs")
	}
	other := TypeHash{Version: HashVersionUnset, Digest: h.Digest}
	if h.Equal(other) {
		t.Fatal("cross version Equal true, want false")
	}
	if UnsetTypeHash.Equal(TypeHash{}) != true {
		t.Fatal("zero hashes should be equal")
	}
	parsed, err := ParseTypeHash(h.String())
	if err != nil {
		t.Fatalf("ParseTypeHash failed: %v", err)
	}
	if !h.Equal(parsed) {
		t.Fatal("parsed hash not equal")
	}
}

// checkFieldTypeValidIDs verifies accepted IDs.
func checkFieldTypeValidIDs(t *testing.T) {
	t.Helper()
	valid := []FieldTypeID{1, 2, 22, 49, 70, 97, 118, 145, 166}
	for _, id := range valid {
		checkFieldTypeValidID(t, id)
	}
}

// checkFieldTypeValidID verifies one accepted ID.
func checkFieldTypeValidID(t *testing.T, id FieldTypeID) {
	t.Helper()
	ft := FieldType{TypeID: id}
	if id == FieldTypeNestedType || id == 49 || id == 97 || id == 145 {
		ft.NestedTypeName = "pkg/msg/X"
	}
	if ft.TypeID.IsArray() || ft.TypeID.IsBoundedSequence() {
		ft.Capacity = 4
	}
	err := ft.Validate()
	if err != nil {
		t.Fatalf("valid id %d rejected: %v", uint8(id), err)
	}
}

// rejectedFieldTypeIDs builds the gap ID list.
func rejectedFieldTypeIDs() []FieldTypeID {
	rejected := []FieldTypeID{0}
	for v := 23; v <= 48; v++ {
		rejected = append(rejected, FieldTypeID(v))
	}
	for v := 71; v <= 96; v++ {
		rejected = append(rejected, FieldTypeID(v))
	}
	for v := 119; v <= 144; v++ {
		rejected = append(rejected, FieldTypeID(v))
	}
	for v := 167; v <= 255; v++ {
		rejected = append(rejected, FieldTypeID(v))
	}
	return rejected
}

// checkFieldTypeRejectedIDs verifies rejected IDs fail.
func checkFieldTypeRejectedIDs(t *testing.T) {
	t.Helper()
	for _, id := range rejectedFieldTypeIDs() {
		ft := FieldType{TypeID: id, NestedTypeName: "pkg/msg/X"}
		err := ft.Validate()
		if err == nil {
			t.Fatalf("invalid id %d accepted, want rejection", uint8(id))
		}
	}
}

// TestFieldTypeIDValidation checks the numeric union plus gap rejection.
func TestFieldTypeIDValidation(t *testing.T) {
	t.Run("valid", checkFieldTypeValidIDs)
	t.Run("rejected", checkFieldTypeRejectedIDs)
}

// TestFieldTypeCapacityNested checks scalar capacity plus nested name rules.
func TestFieldTypeCapacityNested(t *testing.T) {
	scalar := FieldType{TypeID: FieldTypeInt32, Capacity: 1}
	err := scalar.Validate()
	if err == nil {
		t.Fatal("scalar with capacity accepted, want rejection")
	}
	nestedMissing := FieldType{TypeID: FieldTypeNestedType}
	err = nestedMissing.Validate()
	if err == nil {
		t.Fatal("nested without name accepted, want rejection")
	}
	nonNestedNamed := FieldType{TypeID: FieldTypeInt32, NestedTypeName: "pkg/msg/X"}
	err = nonNestedNamed.Validate()
	if err == nil {
		t.Fatal("non nested with name accepted, want rejection")
	}
	arr, ok := ArrayOf(FieldTypeNestedType)
	if !ok {
		t.Fatal("ArrayOf nested base rejected")
	}
	arrType := FieldType{TypeID: arr}
	err = arrType.Validate()
	if err == nil {
		t.Fatal("array nested without name accepted, want rejection")
	}
	arrNamed := FieldType{TypeID: arr, NestedTypeName: "pkg/msg/X", Capacity: 4}
	err = arrNamed.Validate()
	if err != nil {
		t.Fatalf("array nested named rejected: %v", err)
	}
}

// TestFieldTypeCapacityStringCapacityTable checks capacity edges for all kinds.
func TestFieldTypeCapacityStringCapacityTable(t *testing.T) {
	arrayInt32, ok := ArrayOf(FieldTypeInt32)
	if !ok {
		t.Fatal("ArrayOf Int32 rejected")
	}
	boundedInt32, ok := BoundedSequenceOf(FieldTypeInt32)
	if !ok {
		t.Fatal("BoundedSequenceOf Int32 rejected")
	}
	unboundedInt32, ok := UnboundedSequenceOf(FieldTypeInt32)
	if !ok {
		t.Fatal("UnboundedSequenceOf Int32 rejected")
	}
	arrayString, ok := ArrayOf(FieldTypeString)
	if !ok {
		t.Fatal("ArrayOf String rejected")
	}
	unboundedString, ok := UnboundedSequenceOf(FieldTypeString)
	if !ok {
		t.Fatal("UnboundedSequenceOf String rejected")
	}
	boundedStringSeq, ok := BoundedSequenceOf(FieldTypeBoundedString)
	if !ok {
		t.Fatal("BoundedSequenceOf BoundedString rejected")
	}
	cases := []struct {
		name    string
		field   FieldType
		wantErr bool
	}{
		{name: "scalar zero", field: FieldType{TypeID: FieldTypeInt32}, wantErr: false},
		{name: "scalar capacity", field: FieldType{TypeID: FieldTypeInt32, Capacity: 1}, wantErr: true},
		{name: "scalar nonstring stringcap", field: FieldType{TypeID: FieldTypeInt32, StringCapacity: 1}, wantErr: true},
		{name: "array missing capacity", field: FieldType{TypeID: arrayInt32, Capacity: 0}, wantErr: true},
		{name: "array with capacity", field: FieldType{TypeID: arrayInt32, Capacity: 3}, wantErr: false},
		{name: "array nonstring stringcap", field: FieldType{TypeID: arrayInt32, Capacity: 3, StringCapacity: 1}, wantErr: true},
		{name: "bounded missing capacity", field: FieldType{TypeID: boundedInt32, Capacity: 0}, wantErr: true},
		{name: "bounded with capacity", field: FieldType{TypeID: boundedInt32, Capacity: 3}, wantErr: false},
		{name: "unbounded zero", field: FieldType{TypeID: unboundedInt32, Capacity: 0}, wantErr: false},
		{name: "unbounded with capacity", field: FieldType{TypeID: unboundedInt32, Capacity: 3}, wantErr: true},
		{name: "unbounded nonstring stringcap", field: FieldType{TypeID: unboundedInt32, Capacity: 0, StringCapacity: 2}, wantErr: true},
		{name: "scalar string zero", field: FieldType{TypeID: FieldTypeString}, wantErr: false},
		{name: "scalar string bounded cap", field: FieldType{TypeID: FieldTypeString, StringCapacity: 5}, wantErr: false},
		{name: "scalar string carries capacity", field: FieldType{TypeID: FieldTypeString, Capacity: 1}, wantErr: true},
		{name: "scalar bounded string cap", field: FieldType{TypeID: FieldTypeBoundedString, StringCapacity: 5}, wantErr: false},
		{name: "array string full", field: FieldType{TypeID: arrayString, Capacity: 3, StringCapacity: 5}, wantErr: false},
		{name: "array string missing capacity", field: FieldType{TypeID: arrayString, Capacity: 0, StringCapacity: 5}, wantErr: true},
		{name: "unbounded string full", field: FieldType{TypeID: unboundedString, Capacity: 0, StringCapacity: 5}, wantErr: false},
		{name: "unbounded string carries capacity", field: FieldType{TypeID: unboundedString, Capacity: 2, StringCapacity: 5}, wantErr: true},
		{name: "bounded string seq", field: FieldType{TypeID: boundedStringSeq, Capacity: 3, StringCapacity: 5}, wantErr: false},
		{name: "nested stringcap", field: FieldType{TypeID: FieldTypeNestedType, NestedTypeName: "pkg/msg/X", StringCapacity: 1}, wantErr: true},
	}
	for _, tc := range cases {
		err := tc.field.Validate()
		if tc.wantErr && err == nil {
			t.Fatalf("%s accepted, want rejection", tc.name)
		}
		if !tc.wantErr && err != nil {
			t.Fatalf("%s rejected: %v", tc.name, err)
		}
	}
}

// checkArrayOfOffsets verifies offset derivation.
func checkArrayOfOffsets(t *testing.T) {
	t.Helper()
	id, ok := ArrayOf(0)
	if ok {
		t.Fatalf("ArrayOf(0) ok true with %d, want false", uint8(id))
	}
	if id == 48 {
		t.Fatal("ArrayOf(0) equals 48 gap value")
	}
	id, ok = ArrayOf(FieldTypeInt32)
	if !ok || id != 54 {
		t.Fatalf("ArrayOf(Int32) = %d,%v want 54,true", uint8(id), ok)
	}
	id, ok = BoundedSequenceOf(FieldTypeString)
	if !ok || id != FieldTypeString+FieldTypeBoundedSequenceOffset {
		t.Fatalf("BoundedSequenceOf(String) = %d,%v", uint8(id), ok)
	}
	id, ok = UnboundedSequenceOf(FieldTypeDouble)
	if !ok || id != FieldTypeDouble+FieldTypeUnboundedSequenceOffset {
		t.Fatalf("UnboundedSequenceOf(Double) = %d,%v", uint8(id), ok)
	}
}

// checkArrayOfRejected verifies invalid bases are rejected.
func checkArrayOfRejected(t *testing.T) {
	t.Helper()
	_, ok := ArrayOf(23)
	if ok {
		t.Fatal("ArrayOf(23) accepted, want rejection")
	}
	_, ok = BoundedSequenceOf(0)
	if ok {
		t.Fatal("BoundedSequenceOf(0) accepted, want rejection")
	}
	_, ok = UnboundedSequenceOf(255)
	if ok {
		t.Fatal("UnboundedSequenceOf(255) accepted, want rejection")
	}
}

// checkArrayOfKinds verifies array kind predicates.
func checkArrayOfKinds(t *testing.T) {
	t.Helper()
	var arr FieldTypeID = 49
	if !arr.IsArray() {
		t.Fatal("49 IsArray false, want true")
	}
	if arr.IsBoundedSequence() || arr.IsUnboundedSequence() {
		t.Fatal("49 wrong sequence kind")
	}
	if arr.Base() != FieldTypeNestedType {
		t.Fatalf("49 Base = %d, want 1", uint8(arr.Base()))
	}
	if !arr.IsNested() {
		t.Fatal("49 IsNested false, want true")
	}
	var gap FieldTypeID = 48
	if gap.IsArray() || gap.IsBoundedSequence() || gap.IsUnboundedSequence() {
		t.Fatal("48 gap reports array kind")
	}
	if gap.Base() != 48 {
		t.Fatalf("48 Base = %d, want unchanged 48", uint8(gap.Base()))
	}
}

// TestArrayOfHelpers checks offset derivation plus base validation.
func TestArrayOfHelpers(t *testing.T) {
	t.Run("offsets", checkArrayOfOffsets)
	t.Run("rejected", checkArrayOfRejected)
	t.Run("kinds", checkArrayOfKinds)
}
