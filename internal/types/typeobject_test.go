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
	var h [4]byte
	copy(h[:], "abcd")
	return TypeObject{Minimal: &MinimalTypeObject{
		TypeName: "::S",
		Members:  []MinimalMember{{MemberID: 1, Kind: TKInt32, NameHash: h}},
	}}
}

// completeFixture builds one complete object for round trip tests.
func completeFixture() TypeObject {
	return TypeObject{Complete: &CompleteTypeObject{
		TypeName: "::S",
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
