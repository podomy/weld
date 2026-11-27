// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1
package types

import (
	"bytes"
	"testing"
)

// checkTypeIdentifierPrimitive verifies primitive round trip.
func checkTypeIdentifierPrimitive(t *testing.T, le bool) {
	t.Helper()
	id := TypeIdentifier{Kind: TKInt32}
	raw, err := MarshalTypeIdentifier(id, le)
	if err != nil {
		t.Fatalf("le=%v marshal primitive failed: %v", le, err)
	}
	back, err := UnmarshalTypeIdentifier(raw, le)
	if err != nil {
		t.Fatalf("le=%v unmarshal primitive failed: %v", le, err)
	}
	if back.Kind != TKInt32 {
		t.Fatalf("le=%v kind %#02x want %#02x", le, back.Kind, TKInt32)
	}
}

// checkTypeIdentifierString verifies string bound round trip.
func checkTypeIdentifierString(t *testing.T, le bool) {
	t.Helper()
	id := TypeIdentifier{Kind: TKString8, Bound: 64}
	raw, err := MarshalTypeIdentifier(id, le)
	if err != nil {
		t.Fatalf("le=%v marshal string failed: %v", le, err)
	}
	back, err := UnmarshalTypeIdentifier(raw, le)
	if err != nil {
		t.Fatalf("le=%v unmarshal string failed: %v", le, err)
	}
	if back.Bound != 64 {
		t.Fatalf("le=%v bound %d want 64", le, back.Bound)
	}
}

// checkTypeIdentifierHash verifies hash round trip plus EK_BOTH reject.
func checkTypeIdentifierHash(t *testing.T, le bool) {
	t.Helper()
	var h [14]byte
	for i := range h {
		h[i] = byte(i + 1)
	}
	id := TypeIdentifier{Kind: EKMinimal, Hash: h}
	raw, err := MarshalTypeIdentifier(id, le)
	if err != nil {
		t.Fatalf("le=%v marshal hash failed: %v", le, err)
	}
	back, err := UnmarshalTypeIdentifier(raw, le)
	if err != nil {
		t.Fatalf("le=%v unmarshal hash failed: %v", le, err)
	}
	if back.Hash != h {
		t.Fatalf("le=%v hash mismatch", le)
	}
	both := TypeIdentifier{Kind: EKBoth}
	rawBoth, err := MarshalTypeIdentifier(both, le)
	if err != nil {
		t.Fatalf("le=%v marshal both failed: %v", le, err)
	}
	_, err = UnmarshalTypeIdentifier(rawBoth, le)
	if err == nil {
		t.Fatalf("le=%v EK_BOTH must be rejected", le)
	}
}

// checkTypeIdentifierExtended verifies unknown kinds survive verbatim.
func checkTypeIdentifierExtended(t *testing.T, le bool) {
	t.Helper()
	id := TypeIdentifier{Kind: 0xC0, Extended: []byte{0xDE, 0xAD}}
	raw, err := MarshalTypeIdentifier(id, le)
	if err != nil {
		t.Fatalf("le=%v marshal extended failed: %v", le, err)
	}
	back, err := UnmarshalTypeIdentifier(raw, le)
	if err != nil {
		t.Fatalf("le=%v unmarshal extended failed: %v", le, err)
	}
	if !bytes.Equal(back.Extended, id.Extended) {
		t.Fatalf("le=%v extended mismatch", le)
	}
}

// TestTypeIdentifierSpec pins discriminator plus bound plus hash framing.
func TestTypeIdentifierSpec(t *testing.T) {
	for _, le := range []bool{true, false} {
		checkTypeIdentifierPrimitive(t, le)
		checkTypeIdentifierString(t, le)
		checkTypeIdentifierHash(t, le)
		checkTypeIdentifierExtended(t, le)
	}
}

// minimalFixture builds one minimal object for round trip tests.
func minimalFixture() TypeObject {
	h := ComputeMinimalNameHash("a")
	return TypeObject{Minimal: &MinimalTypeObject{
		TypeName: "::S",
		Ext:      ExtAppendable,
		Members:  []MinimalMember{{MemberID: 1, Kind: TKInt32, NameHash: h}},
	}}
}

// completeFixture builds one complete object for round trip tests.
func completeFixture() TypeObject {
	return TypeObject{Complete: &CompleteTypeObject{
		TypeName: "::S",
		Ext:      ExtAppendable,
		Members:  []CompleteMember{{MemberID: 1, Kind: TKInt32, Name: "a"}},
	}}
}

// checkTypeObjectDHeader verifies the object opens with a DHEADER.
func checkTypeObjectDHeader(t *testing.T, le bool, obj TypeObject) {
	t.Helper()
	raw, err := MarshalTypeObject(obj, le)
	if err != nil {
		t.Fatalf("le=%v marshal failed: %v", le, err)
	}
	if len(raw) < 5 {
		t.Fatalf("le=%v object too short %d", le, len(raw))
	}
	disc, err := obj.Discriminator()
	if err != nil {
		t.Fatalf("le=%v discriminator failed: %v", le, err)
	}
	if raw[4] != disc {
		t.Fatalf("le=%v discriminator %#02x want %#02x", le, raw[4], disc)
	}
	back, err := UnmarshalTypeObject(raw, le)
	if err != nil {
		t.Fatalf("le=%v unmarshal failed: %v", le, err)
	}
	raw2, err := MarshalTypeObject(back, le)
	if err != nil {
		t.Fatalf("le=%v remarshal failed: %v", le, err)
	}
	if !bytes.Equal(raw, raw2) {
		t.Fatalf("le=%v round trip mismatch", le)
	}
}

// checkTypeObjectUnknown verifies unknown equivalence kinds fail.
func checkTypeObjectUnknown(t *testing.T, le bool) {
	t.Helper()
	raw := []byte{0x01, 0x00, 0x00, 0x00, 0xAB}
	_, err := UnmarshalTypeObject(raw, le)
	if err == nil {
		t.Fatalf("le=%v unknown kind must fail", le)
	}
}

// TestTypeObjectSpec pins DHEADER framing plus round trip.
func TestTypeObjectSpec(t *testing.T) {
	for _, le := range []bool{true, false} {
		checkTypeObjectDHeader(t, le, minimalFixture())
		checkTypeObjectDHeader(t, le, completeFixture())
		checkTypeObjectUnknown(t, le)
		checkTypeObjectStrict(t, le)
	}
}

// checkTypeObjectStrict pins fail-closed framing: duplicates and trailing fail.
func checkTypeObjectStrict(t *testing.T, le bool) {
	t.Helper()
	dup := TypeObject{Minimal: &MinimalTypeObject{
		TypeName: "D",
		Ext:      ExtAppendable,
		Members: []MinimalMember{
			{MemberID: 2, Kind: TKInt32},
			{MemberID: 2, Kind: TKInt32},
		},
	}}
	rawDup, err := MarshalTypeObject(dup, le)
	if err != nil {
		t.Fatalf("le=%v marshal dup failed: %v", le, err)
	}
	if _, err := UnmarshalTypeObject(rawDup, le); err == nil {
		t.Fatalf("le=%v duplicate member id accepted, want rejection", le)
	}
	raw, err := MarshalTypeObject(minimalFixture(), le)
	if err != nil {
		t.Fatalf("le=%v marshal failed: %v", le, err)
	}
	trailing := append(append([]byte(nil), raw...), 0x00)
	if _, err := UnmarshalTypeObject(trailing, le); err == nil {
		t.Fatalf("le=%v trailing byte accepted, want rejection", le)
	}
	// Cross-endian hint still decodes via detection.
	rawBE, err := MarshalTypeObject(minimalFixture(), !le)
	if err != nil {
		t.Fatalf("le=%v marshal opposite failed: %v", le, err)
	}
	if _, err := UnmarshalTypeObject(rawBE, le); err != nil {
		t.Fatalf("le=%v cross-endian detect failed: %v", le, err)
	}
}

// TestMinimalNameHashMD5 pins MD5 first-4 rule per 7.3.3.5.
func TestMinimalNameHashMD5(t *testing.T) {
	got := ComputeMinimalNameHash("abcd")
	// MD5("abcd") = e2fc714c4727ee9395f324cd2e7f331f, first 4 = e2 fc 71 4c.
	var want [4]byte
	want[0] = 0xe2
	want[1] = 0xfc
	want[2] = 0x71
	want[3] = 0x4c
	if got != want {
		t.Fatalf("name hash %x want %x", got, want)
	}
	var raw [4]byte
	copy(raw[:], "abcd")
	if got == raw {
		t.Fatal("MD5 hash equals raw prefix, want distinct")
	}
	other := ComputeMinimalNameHash("abcde")
	if got == other {
		t.Fatal("distinct names share hash prefix")
	}
}

// TestMemberDetailPreserved pins bounds, nested, units, extensibility, must-understand.
func TestMemberDetailPreserved(t *testing.T) {
	for _, le := range []bool{true, false} {
		checkDetailBounds(t, le)
		checkDetailNestedUnitsExt(t, le)
		checkDetailMustUnderstand(t, le)
		checkDetailZeroID(t, le)
	}
}

// cloneDetailField copies one-field descriptions for mutation tests.
func cloneDetailField(base IndividualTypeDescription, mutate func(*Field)) IndividualTypeDescription {
	out := IndividualTypeDescription{
		TypeName:      base.TypeName,
		Extensibility: base.Extensibility,
		Units:         base.Units,
		Fields:        make([]Field, len(base.Fields)),
	}
	copy(out.Fields, base.Fields)
	mutate(&out.Fields[0])
	return out
}

// detailBase returns the shared base description for detail tests.
func detailBase() IndividualTypeDescription {
	return IndividualTypeDescription{
		TypeName:      "pkg/msg/D",
		Extensibility: ExtAppendable,
		Fields: []Field{
			{Name: "x", Type: FieldType{TypeID: FieldTypeInt32}, MemberID: 1, Units: "m"},
		},
	}
}

// checkDetailBounds pins bound preservation in minimal hash.
func checkDetailBounds(t *testing.T, le bool) {
	t.Helper()
	base := detailBase()
	bounded := IndividualTypeDescription{
		TypeName:      "pkg/msg/D",
		Extensibility: ExtAppendable,
		Fields: []Field{
			{Name: "x", Type: FieldType{TypeID: FieldTypeBoundedString, StringCapacity: 8}, MemberID: 1, Units: "m"},
		},
	}
	hBaseMin, err := wireHashForIndividual(base, true, le)
	if err != nil {
		t.Fatalf("le=%v base min hash failed: %v", le, err)
	}
	hBoundMin, err := wireHashForIndividual(bounded, true, le)
	if err != nil {
		t.Fatalf("le=%v bounded min hash failed: %v", le, err)
	}
	if hBaseMin.Equal(hBoundMin) {
		t.Fatalf("le=%v bounds not preserved in minimal hash", le)
	}
}

// checkDetailNestedUnitsExt pins nested, units, extensibility preservation.
func checkDetailNestedUnitsExt(t *testing.T, le bool) {
	t.Helper()
	base := detailBase()
	hBaseMin, err := wireHashForIndividual(base, true, le)
	if err != nil {
		t.Fatalf("le=%v base hash failed: %v", le, err)
	}
	nested := cloneDetailField(base, func(f *Field) {
		f.Type = FieldType{TypeID: FieldTypeNestedType, NestedTypeName: "pkg/msg/N"}
	})
	hNested, err := wireHashForIndividual(nested, true, le)
	if err != nil {
		t.Fatalf("le=%v nested hash failed: %v", le, err)
	}
	if hBaseMin.Equal(hNested) {
		t.Fatalf("le=%v nested not preserved", le)
	}
	units := cloneDetailField(base, func(f *Field) {
		f.Units = "s"
	})
	hUnits, err := wireHashForIndividual(units, true, le)
	if err != nil {
		t.Fatalf("le=%v units hash failed: %v", le, err)
	}
	if hBaseMin.Equal(hUnits) {
		t.Fatalf("le=%v units not preserved", le)
	}
	ext := base
	ext.Extensibility = ExtMutable
	hExt, err := wireHashForIndividual(ext, true, le)
	if err != nil {
		t.Fatalf("le=%v ext hash failed: %v", le, err)
	}
	if hBaseMin.Equal(hExt) {
		t.Fatalf("le=%v extensibility not preserved", le)
	}
}

// checkDetailMustUnderstand pins must-understand in complete wire.
func checkDetailMustUnderstand(t *testing.T, le bool) {
	t.Helper()
	base := detailBase()
	mu := cloneDetailField(base, func(f *Field) {
		f.MustUnderstand = true
	})
	hMU, err := wireHashForIndividual(mu, false, le)
	if err != nil {
		t.Fatalf("le=%v must-understand hash failed: %v", le, err)
	}
	hBaseComplete, err := wireHashForIndividual(base, false, le)
	if err != nil {
		t.Fatalf("le=%v base complete hash failed: %v", le, err)
	}
	if hMU.Equal(hBaseComplete) {
		t.Fatalf("le=%v must-understand not preserved in complete", le)
	}
	complete := individualToCompleteTypeObject(mu)
	raw, err := MarshalTypeObject(TypeObject{Complete: &complete}, le)
	if err != nil {
		t.Fatalf("le=%v marshal complete failed: %v", le, err)
	}
	back, err := UnmarshalTypeObject(raw, le)
	if err != nil {
		t.Fatalf("le=%v unmarshal complete failed: %v", le, err)
	}
	if back.Complete == nil || !back.Complete.Members[0].MustUnderstand {
		t.Fatalf("le=%v must-understand lost on wire", le)
	}
}

// checkDetailZeroID pins explicit member ID 0 round trip.
func checkDetailZeroID(t *testing.T, le bool) {
	t.Helper()
	zero := TypeObject{Minimal: &MinimalTypeObject{
		TypeName: "Z",
		Ext:      ExtAppendable,
		Members:  []MinimalMember{{MemberID: 0, Kind: TKInt32, NameHash: ComputeMinimalNameHash("z")}},
	}}
	rawZero, err := MarshalTypeObject(zero, le)
	if err != nil {
		t.Fatalf("le=%v marshal zero id failed: %v", le, err)
	}
	backZero, err := UnmarshalTypeObject(rawZero, le)
	if err != nil {
		t.Fatalf("le=%v unmarshal zero id failed: %v", le, err)
	}
	if backZero.Minimal == nil || backZero.Minimal.Members[0].MemberID != 0 {
		t.Fatalf("le=%v member id 0 lost", le)
	}
}
