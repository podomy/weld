// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1
package types

import (
	"testing"
)

// minimalWireObject builds one minimal TypeObject bytes for hash tests.
func minimalWireObject(t *testing.T, le bool) []byte {
	t.Helper()
	h := ComputeMinimalNameHash("abcd")
	obj := TypeObject{Minimal: &MinimalTypeObject{
		TypeName: "::S",
		Ext:      ExtAppendable,
		Members:  []MinimalMember{{MemberID: 1, Kind: TKInt32, NameHash: h}},
	}}
	raw, err := MarshalTypeObject(obj, le)
	if err != nil {
		t.Fatalf("le=%v marshal minimal failed: %v", le, err)
	}
	return raw
}

// checkWireHashPins verifies wire and model hashes differ plus RIHS01 stable.
//
// The wire hash is canonical: LE and BE TypeObjects hash identically via
// little-endian canonical bytes per 7.3.3.10. Transport seq bytes differ
// by endian, but ComputeWireEquivalenceHash canonicalizes.
func checkWireHashPins(t *testing.T, le bool) {
	t.Helper()
	raw := minimalWireObject(t, le)
	wire2, err := ComputeWireEquivalenceHash([][]byte{raw}, le)
	if err != nil {
		t.Fatalf("le=%v wire2 failed: %v", le, err)
	}
	if wire2.String()[:5] != "EQ14_" {
		t.Fatalf("le=%v wire prefix %q", le, wire2.String())
	}
	back, err := ParseEquivalenceHash(wire2.String())
	if err != nil {
		t.Fatalf("le=%v parse wire failed: %v", le, err)
	}
	if !wire2.Equal(back) {
		t.Fatalf("le=%v wire round trip mismatch", le)
	}
	desc := int32Description()
	model, err := ComputeEquivalencext6aHash(desc)
	if err != nil {
		t.Fatalf("le=%v model hash failed: %v", le, err)
	}
	if wire2.Digest == model.Digest {
		t.Fatalf("le=%v wire and model must differ", le)
	}
	rihs, err := ComputeTypeHash(desc)
	if err != nil {
		t.Fatalf("le=%v rihs failed: %v", le, err)
	}
	want := "RIHS01_b6578ded3c58c626cfe8d1a6fb6e04f706f97e9f03d2727c9ff4e74b1cef0deb"
	if rihs.String() != want {
		t.Fatalf("le=%v rihs %s want %s", le, rihs.String(), want)
	}
}

// checkSeqSplit verifies sequence split recovers elements.
func checkSeqSplit(t *testing.T, le bool) {
	t.Helper()
	a := minimalWireObject(t, le)
	b := minimalWireObject(t, le)
	seq, err := MarshalTypeObjectSeqCDR([][]byte{a, b}, le)
	if err != nil {
		t.Fatalf("le=%v marshal two failed: %v", le, err)
	}
	parts, err := ParseTypeObjectSeqCDR(seq)
	if err != nil {
		t.Fatalf("le=%v parse seq failed: %v", le, err)
	}
	if len(parts) != 2 {
		t.Fatalf("le=%v parts %d want 2", le, len(parts))
	}
}

// TestEquivalenceHashWire pins MD5 over CDR plus RIHS01 stability.
func TestEquivalenceHashWire(t *testing.T) {
	for _, le := range []bool{true, false} {
		checkWireHashPins(t, le)
		checkSeqSplit(t, le)
	}
	checkEquivalencePrefixDistinct(t)
	checkWireHashCanonical(t)
}

// checkWireHashCanonical pins one hash regardless of LE/BE per 7.3.3.10.
func checkWireHashCanonical(t *testing.T) {
	t.Helper()
	ind := IndividualTypeDescription{
		TypeName:      "pkg/msg/C",
		Extensibility: ExtAppendable,
		Fields:        []Field{{Name: "v", Type: FieldType{TypeID: FieldTypeInt32}, MemberID: 1}},
	}
	checkCanonicalMin(t, ind)
	checkCanonicalComplete(t, ind)
	checkCanonicalRaw(t, ind)
}

// checkCanonicalMin pins minimal LE versus BE equality.
func checkCanonicalMin(t *testing.T, ind IndividualTypeDescription) {
	t.Helper()
	hLEMin, err := wireHashForIndividual(ind, true, true)
	if err != nil {
		t.Fatalf("le min hash failed: %v", err)
	}
	hBEMin, err := wireHashForIndividual(ind, true, false)
	if err != nil {
		t.Fatalf("be min hash failed: %v", err)
	}
	if !hLEMin.Equal(hBEMin) {
		t.Fatalf("minimal LE %s vs BE %s differ, want canonical equal", hLEMin.String(), hBEMin.String())
	}
}

// checkCanonicalComplete pins complete LE versus BE equality.
func checkCanonicalComplete(t *testing.T, ind IndividualTypeDescription) {
	t.Helper()
	hLEComplete, err := wireHashForIndividual(ind, false, true)
	if err != nil {
		t.Fatalf("le complete hash failed: %v", err)
	}
	hBEComplete, err := wireHashForIndividual(ind, false, false)
	if err != nil {
		t.Fatalf("be complete hash failed: %v", err)
	}
	if !hLEComplete.Equal(hBEComplete) {
		t.Fatalf("complete LE %s vs BE %s differ", hLEComplete.String(), hBEComplete.String())
	}
}

// checkCanonicalRaw pins raw object canonicalization.
func checkCanonicalRaw(t *testing.T, ind IndividualTypeDescription) {
	t.Helper()
	rawLE, err := MarshalTypeObject(TypeObject{Minimal: func() *MinimalTypeObject { m := individualToMinimalTypeObject(ind); return &m }()}, true)
	if err != nil {
		t.Fatalf("marshal LE failed: %v", err)
	}
	rawBE, err := MarshalTypeObject(TypeObject{Minimal: func() *MinimalTypeObject { m := individualToMinimalTypeObject(ind); return &m }()}, false)
	if err != nil {
		t.Fatalf("marshal BE failed: %v", err)
	}
	hRawLE, err := ComputeWireEquivalenceHash([][]byte{rawLE}, true)
	if err != nil {
		t.Fatalf("hash LE raw failed: %v", err)
	}
	hRawBE, err := ComputeWireEquivalenceHash([][]byte{rawBE}, false)
	if err != nil {
		t.Fatalf("hash BE raw failed: %v", err)
	}
	if !hRawLE.Equal(hRawBE) {
		t.Fatalf("raw LE %s vs BE %s differ", hRawLE.String(), hRawBE.String())
	}
	hLEMin, err := wireHashForIndividual(ind, true, true)
	if err != nil {
		t.Fatalf("individual hash failed: %v", err)
	}
	if !hRawLE.Equal(hLEMin) {
		t.Fatal("raw canonical differs from individual canonical")
	}
}

// checkEquivalencePrefixDistinct pins distinct model versus wire prefixes.
//
// Model hashes use EQM14_ while wire hashes use EQ14_ so a string never
// parses as the wrong kind. Cross-prefix input must fail in both parsers.
func checkEquivalencePrefixDistinct(t *testing.T) {
	t.Helper()
	desc := int32Description()
	model, err := ComputeEquivalencext6aHash(desc)
	if err != nil {
		t.Fatalf("model hash failed: %v", err)
	}
	if len(model.String()) < 6 || model.String()[:6] != "EQM14_" {
		t.Fatalf("model prefix %q want EQM14_", model.String())
	}
	back, err := ParseEquivalencext6aHash(model.String())
	if err != nil {
		t.Fatalf("model round trip failed: %v", err)
	}
	if !model.Equal(back) {
		t.Fatal("model round trip mismatch")
	}
	raw := minimalWireObject(t, true)
	seq, err := MarshalTypeObjectSeqCDR([][]byte{raw}, true)
	if err != nil {
		t.Fatalf("marshal seq failed: %v", err)
	}
	wire := ComputeEquivalenceHash(seq)
	if len(wire.String()) < 5 || wire.String()[:5] != "EQ14_" {
		t.Fatalf("wire prefix %q want EQ14_", wire.String())
	}
	if _, err := ParseEquivalenceHash(model.String()); err == nil {
		t.Fatal("wire parser accepted model prefix, want rejection")
	}
	if _, err := ParseEquivalencext6aHash(wire.String()); err == nil {
		t.Fatal("model parser accepted wire prefix, want rejection")
	}
}
