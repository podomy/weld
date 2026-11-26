// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1
package types

import (
	"testing"
)

// checkTryDiscard verifies discard branch.
func checkTryDiscard(t *testing.T) {
	t.Helper()
	got, discard, err := ApplyTryConstructString(TryConstructDiscard, "abcdef", 3)
	if err != nil {
		t.Fatalf("discard failed: %v", err)
	}
	if !discard || got != "" {
		t.Fatalf("discard got %q discard %v", got, discard)
	}
}

// checkTryDefault verifies default branch.
func checkTryDefault(t *testing.T) {
	t.Helper()
	got, discard, err := ApplyTryConstructString(TryConstructUseDefault, "abcdef", 3)
	if err != nil {
		t.Fatalf("default failed: %v", err)
	}
	if discard || got != "" {
		t.Fatalf("default got %q discard %v", got, discard)
	}
}

// checkTryTrim verifies trim plus in-bounds branches.
func checkTryTrim(t *testing.T) {
	t.Helper()
	got, discard, err := ApplyTryConstructString(TryConstructTrim, "abcdef", 3)
	if err != nil {
		t.Fatalf("trim failed: %v", err)
	}
	if discard || got != "abc" {
		t.Fatalf("trim got %q discard %v", got, discard)
	}
	got, discard, err = ApplyTryConstructString(TryConstructTrim, "ab", 3)
	if err != nil || discard || got != "ab" {
		t.Fatalf("in-bounds trim changed value")
	}
}

// checkTryString verifies discard plus default plus trim branches.
func checkTryString(t *testing.T) {
	t.Helper()
	t.Run("discard", func(t *testing.T) { checkTryDiscard(t) })
	t.Run("default", func(t *testing.T) { checkTryDefault(t) })
	t.Run("trim", func(t *testing.T) { checkTryTrim(t) })
}

// checkTrySequence verifies sequence policy branches.
func checkTrySequence(t *testing.T) {
	t.Helper()
	_, discard, err := ApplyTryConstructSequence(TryConstructDiscard, 8, 4)
	if err != nil {
		t.Fatalf("discard seq failed: %v", err)
	}
	if !discard {
		t.Fatalf("discard seq must discard")
	}
	n, discard, err := ApplyTryConstructSequence(TryConstructUseDefault, 8, 4)
	if err != nil {
		t.Fatalf("default seq failed: %v", err)
	}
	if discard || n != 0 {
		t.Fatalf("default seq got %d discard %v", n, discard)
	}
	n, discard, err = ApplyTryConstructSequence(TryConstructTrim, 8, 4)
	if err != nil {
		t.Fatalf("trim seq failed: %v", err)
	}
	if discard || n != 4 {
		t.Fatalf("trim seq got %d discard %v", n, discard)
	}
}

// checkAppendableBound verifies bound enforcement on decode.
func checkAppendableBound(t *testing.T, le bool) {
	t.Helper()
	enc := NewEncoder(le)
	err := enc.WriteStructxt6bDelimited(func() error {
		enc.WriteUint32(1)
		enc.WriteUint32(2)
		return nil
	})
	if err != nil {
		t.Fatalf("le=%v delimited write failed: %v", le, err)
	}
	wire := enc.Bytes()
	dec, err := NewDecoder(wire)
	if err != nil {
		t.Fatalf("le=%v decode failed: %v", le, err)
	}
	size, err := dec.ReadDHeaderWithBound(100)
	if err != nil {
		t.Fatalf("le=%v bound 100 failed: %v", le, err)
	}
	if size != 8 {
		t.Fatalf("le=%v size %d want 8", le, size)
	}
	dec2, err := NewDecoder(wire)
	if err != nil {
		t.Fatalf("le=%v decode2 failed: %v", le, err)
	}
	_, err = dec2.ReadDHeaderWithBound(4)
	if err == nil {
		t.Fatalf("le=%v over bound must fail", le)
	}
	if dec2.Dropped() == 0 {
		t.Fatalf("le=%v over bound must count a drop", le)
	}
}

// TestExtensiblePolicy pins try-construct plus appendable bounds.
func TestExtensiblePolicy(t *testing.T) {
	t.Run("string", checkTryString)
	t.Run("sequence", checkTrySequence)
	for _, le := range []bool{true, false} {
		checkAppendableBound(t, le)
	}
}

// seqRefxt6a builds one int32 sequence descriptor for bridge tests.
func seqRefxt6a(bound uint32) TypeRefxt6a {
	return TypeRefxt6a{
		Kind:  Kindxt6aSequence,
		Bound: bound,
		Elem:  &TypeRefxt6a{Kind: Kindxt6aPrimitive, Base: FieldTypeInt32},
	}
}

// structRefxt6a builds one appendable struct descriptor for bridge tests.
func structRefxt6a(names ...string) TypeRefxt6a {
	members := make([]Structxt6aMember, len(names))
	for i, name := range names {
		members[i] = Structxt6aMember{Name: name, MemberID: uint32(i), Type: FieldTypeInt32}
	}

	return TypeRefxt6a{
		Kind:   Kindxt6aStruct,
		Struct: &Structxt6aShape{Ext: ExtAppendable, Members: members},
	}
}

// checkConsistencyBridgeExact pins EXACT_TYPE (disallow) matching.
func checkConsistencyBridgeExact(t *testing.T) {
	t.Helper()

	disallow := TypeConsistencyxt6aPolicy{Kind: Consistencyxt6aDisallowCoercion}
	same := TypeRefxt6a{Kind: Kindxt6aPrimitive, Base: FieldTypeInt32}

	err := CheckTypeConsistencyxt6a(disallow, same, same, "I32", "I32")
	if err != nil {
		t.Fatalf("exact identical primitives rejected: %v", err)
	}

	if !MinimalEquivalentxt6a(same, same, Defaultxt6aConsistency()) {
		t.Fatal("identical primitives must be minimally equivalent")
	}

	wide := seqRefxt6a(10)
	narrow := seqRefxt6a(5)

	err = CheckTypeConsistencyxt6a(disallow, wide, narrow, "Seq10", "Seq5")
	if err == nil {
		t.Fatal("exact widened sequence accepted, want rejection")
	}

	if MinimalEquivalentxt6a(wide, narrow, Defaultxt6aConsistency()) {
		t.Fatal("widened sequences must not be minimally equivalent")
	}

	a := structRefxt6a("x", "y")
	b := structRefxt6a("x", "y")

	err = CheckTypeConsistencyxt6a(disallow, a, b, "S", "S")
	if err != nil {
		t.Fatalf("exact identical structs rejected: %v", err)
	}

	renamed := structRefxt6a("x", "z")

	err = CheckTypeConsistencyxt6a(disallow, renamed, b, "S", "S")
	if err == nil {
		t.Fatal("exact renamed struct accepted, want rejection")
	}
}

// checkConsistencyBridgeMinimal pins MINIMAL (allow) assignability.
func checkConsistencyBridgeMinimal(t *testing.T) {
	t.Helper()

	allow := Defaultxt6aConsistency()
	wide := seqRefxt6a(10)
	narrow := seqRefxt6a(5)

	err := CheckTypeConsistencyxt6a(allow, wide, narrow, "Seq10", "Seq5")
	if err != nil {
		t.Fatalf("minimal widened reader rejected: %v", err)
	}

	err = CheckTypeConsistencyxt6a(allow, narrow, wide, "Seq5", "Seq10")
	if err == nil {
		t.Fatal("minimal narrowed reader accepted, want rejection")
	}

	mismatch := TypeRefxt6a{Kind: Kindxt6aPrimitive, Base: FieldTypeInt64}
	plain := TypeRefxt6a{Kind: Kindxt6aPrimitive, Base: FieldTypeInt32}

	err = CheckTypeConsistencyxt6a(allow, plain, mismatch, "I32", "I64")
	if err == nil {
		t.Fatal("minimal primitive mismatch accepted, want rejection")
	}

	reader := TypeRefxt6a{Kind: Kindxt6aString8, Bound: 5}
	writer := TypeRefxt6a{Kind: Kindxt6aString8, Bound: 10}

	err = CheckTypeConsistencyxt6a(allow, reader, writer, "Str5", "Str10")
	if err == nil {
		t.Fatal("minimal narrow string accepted, want rejection")
	}

	relaxed := allow
	relaxed.IgnoreStringBounds = true

	err = CheckTypeConsistencyxt6a(relaxed, reader, writer, "Str5", "Str10")
	if err != nil {
		t.Fatalf("minimal ignored string bounds rejected: %v", err)
	}
}

// TestExtensibleConsistencyBridge pins step 1 evaluation from descriptors.
func TestExtensibleConsistencyBridge(t *testing.T) {
	t.Run("exact", checkConsistencyBridgeExact)
	t.Run("minimal", checkConsistencyBridgeMinimal)
}
