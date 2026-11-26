// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1
package types

import (
	"testing"
)

// minimalWireObject builds one minimal TypeObject bytes for hash tests.
func minimalWireObject(t *testing.T, le bool) []byte {
	t.Helper()
	var h [4]byte
	copy(h[:], "abcd")
	obj := TypeObject{Minimal: &MinimalTypeObject{
		TypeName: "::S",
		Members:  []MinimalMember{{MemberID: 1, Kind: TKInt32, NameHash: h}},
	}}
	raw, err := MarshalTypeObject(obj, le)
	if err != nil {
		t.Fatalf("le=%v marshal minimal failed: %v", le, err)
	}
	return raw
}

// checkWireHashPins verifies wire and model hashes differ plus RIHS01 stable.
func checkWireHashPins(t *testing.T, le bool) {
	t.Helper()
	raw := minimalWireObject(t, le)
	seq, err := MarshalTypeObjectSeqCDR([][]byte{raw}, le)
	if err != nil {
		t.Fatalf("le=%v marshal seq failed: %v", le, err)
	}
	wire := ComputeEquivalenceHash(seq)
	if wire.String()[:5] != "EQ14_" {
		t.Fatalf("le=%v wire prefix %q", le, wire.String())
	}
	back, err := ParseEquivalenceHash(wire.String())
	if err != nil {
		t.Fatalf("le=%v parse wire failed: %v", le, err)
	}
	if !wire.Equal(back) {
		t.Fatalf("le=%v wire round trip mismatch", le)
	}
	desc := int32Description()
	model, err := ComputeEquivalencext6aHash(desc)
	if err != nil {
		t.Fatalf("le=%v model hash failed: %v", le, err)
	}
	if wire.Digest == model.Digest {
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
	wire2, err := ComputeWireEquivalenceHash([][]byte{raw}, le)
	if err != nil {
		t.Fatalf("le=%v wire2 failed: %v", le, err)
	}
	if !wire.Equal(wire2) {
		t.Fatalf("le=%v direct versus seq hash mismatch", le)
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
