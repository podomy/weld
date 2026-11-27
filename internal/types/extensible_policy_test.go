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
		checkFramedPerKind(t, le)
		checkTryConstructRealPaths(t, le)
	}
}

// checkFramedPerKind pins final/plain, appendable/delimited, mutable/PL outer framing.
func checkFramedPerKind(t *testing.T, le bool) {
	t.Helper()
	checkFramedFinal(t, le)
	checkFramedAppendableV2(t, le)
	checkFramedAppendableV1(t, le)
	checkFramedMutableV2(t, le)
	checkFramedMutableV1(t, le)
	checkFramedBound(t, le)
}

// checkFramedFinal pins final plain framing.
//
//nolint:dupl // framing helpers share wire shape by design; splitting would obscure per-kind pins
func checkFramedFinal(t *testing.T, le bool) {
	t.Helper()
	fin := NewEncoderWithExtVersioned(ExtFinal, XCDRVersion2, le)
	err := fin.WriteStructFramed(ExtFinal, XCDRVersion2, func() error {
		fin.WriteUint32(0x01020304)
		return nil
	})
	if err != nil {
		t.Fatalf("le=%v final framed write failed: %v", le, err)
	}
	wireFin := fin.Bytes()
	decFin, err := NewDecoder(wireFin)
	if err != nil {
		t.Fatalf("le=%v final decode failed: %v", le, err)
	}
	err = decFin.ReadStructFramed(ExtFinal, XCDRVersion2, 0, func(size uint32) error {
		if size != 0 {
			t.Fatalf("le=%v final size %d want 0", le, size)
		}
		v, err := decFin.ReadUint32()
		if err != nil {
			return err
		}
		if v != 0x01020304 {
			t.Fatalf("le=%v final body %#x", le, v)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("le=%v final framed read failed: %v", le, err)
	}
}

// checkFramedAppendableV2 pins appendable delimited framing.
//
//nolint:dupl // framing helpers share wire shape by design; splitting would obscure per-kind pins
func checkFramedAppendableV2(t *testing.T, le bool) {
	t.Helper()
	app := NewEncoderWithExtVersioned(ExtAppendable, XCDRVersion2, le)
	err := app.WriteStructFramed(ExtAppendable, XCDRVersion2, func() error {
		app.WriteUint32(0x0A0B0C0D)
		return nil
	})
	if err != nil {
		t.Fatalf("le=%v appendable write failed: %v", le, err)
	}
	wireApp := app.Bytes()
	decApp, err := NewDecoder(wireApp)
	if err != nil {
		t.Fatalf("le=%v appendable decode failed: %v", le, err)
	}
	err = decApp.ReadStructFramed(ExtAppendable, XCDRVersion2, 0, func(size uint32) error {
		if size != 4 {
			t.Fatalf("le=%v appendable size %d want 4", le, size)
		}
		v, err := decApp.ReadUint32()
		if err != nil {
			return err
		}
		if v != 0x0A0B0C0D {
			t.Fatalf("le=%v appendable body %#x", le, v)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("le=%v appendable framed read failed: %v", le, err)
	}
}

// checkFramedAppendableV1 pins V1 appendable plain framing.
func checkFramedAppendableV1(t *testing.T, le bool) {
	t.Helper()
	appV1 := NewEncoderWithExtVersioned(ExtAppendable, XCDRVersion1, le)
	err := appV1.WriteStructFramed(ExtAppendable, XCDRVersion1, func() error {
		appV1.WriteUint32(1)
		return nil
	})
	if err != nil {
		t.Fatalf("le=%v appendable V1 write failed: %v", le, err)
	}
	wireAppV1 := appV1.Bytes()
	decAppV1, err := NewDecoder(wireAppV1)
	if err != nil {
		t.Fatalf("le=%v appendable V1 decode failed: %v", le, err)
	}
	err = decAppV1.ReadStructFramed(ExtAppendable, XCDRVersion1, 0, func(size uint32) error {
		if size != 0 {
			t.Fatalf("le=%v appendable V1 size %d want 0", le, size)
		}
		if _, err := decAppV1.ReadUint32(); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		t.Fatalf("le=%v appendable V1 read failed: %v", le, err)
	}
}

// checkFramedMutableV2 pins mutable PL framing.
func checkFramedMutableV2(t *testing.T, le bool) {
	t.Helper()
	mut := NewEncoderWithExtVersioned(ExtMutable, XCDRVersion2, le)
	err := mut.WriteStructFramed(ExtMutable, XCDRVersion2, func() error {
		if err := mut.WriteEMHeader(1, false, 4); err != nil {
			return err
		}
		mut.WriteUint32(0xDEADBEEF)
		return nil
	})
	if err != nil {
		t.Fatalf("le=%v mutable write failed: %v", le, err)
	}
	wireMut := mut.Bytes()
	decMut, err := NewDecoder(wireMut)
	if err != nil {
		t.Fatalf("le=%v mutable decode failed: %v", le, err)
	}
	err = decMut.ReadStructFramed(ExtMutable, XCDRVersion2, 0, func(size uint32) error {
		hdr, err := decMut.ReadEMHeader()
		if err != nil {
			return err
		}
		if hdr.MemberID != 1 {
			t.Fatalf("le=%v mutable member %d", le, hdr.MemberID)
		}
		v, err := decMut.ReadUint32()
		if err != nil {
			return err
		}
		if v != 0xDEADBEEF {
			t.Fatalf("le=%v mutable body %#x", le, v)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("le=%v mutable framed read failed: %v", le, err)
	}
}

// checkFramedMutableV1 pins V1 mutable PID framing.
func checkFramedMutableV1(t *testing.T, le bool) {
	t.Helper()
	mutV1 := NewEncoderWithExtVersioned(ExtMutable, XCDRVersion1, le)
	err := mutV1.WriteStructFramed(ExtMutable, XCDRVersion1, func() error {
		if err := mutV1.WritePIDMember(1, false, []byte{0xAA}); err != nil {
			return err
		}
		return mutV1.WritePIDListEnd()
	})
	if err != nil {
		t.Fatalf("le=%v mutable V1 write failed: %v", le, err)
	}
	wireMutV1 := mutV1.Bytes()
	decMutV1, err := NewDecoder(wireMutV1)
	if err != nil {
		t.Fatalf("le=%v mutable V1 decode failed: %v", le, err)
	}
	err = decMutV1.ReadStructFramed(ExtMutable, XCDRVersion1, 0, func(size uint32) error {
		known := map[uint32]bool{1: true}
		hdr, done, err := decMutV1.SkipPIDMemberContent(known)
		if err != nil {
			return err
		}
		if done || hdr.memberID != 1 {
			t.Fatalf("le=%v mutable V1 member %+v done %v", le, hdr, done)
		}
		if err := decMutV1.SkipBytes(uint64(hdr.size)); err != nil {
			return err
		}
		_, done, err = decMutV1.SkipPIDMemberContent(known)
		if err != nil || !done {
			t.Fatalf("le=%v mutable V1 sentinel missing", le)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("le=%v mutable V1 read failed: %v", le, err)
	}
}

// checkFramedBound pins appendable bound enforcement.
func checkFramedBound(t *testing.T, le bool) {
	t.Helper()
	boundEnc := NewEncoderWithExtVersioned(ExtAppendable, XCDRVersion2, le)
	err := boundEnc.WriteStructFramed(ExtAppendable, XCDRVersion2, func() error {
		boundEnc.WriteUint32(1)
		boundEnc.WriteUint32(2)
		return nil
	})
	if err != nil {
		t.Fatalf("le=%v bound enc failed: %v", le, err)
	}
	decBound, err := NewDecoder(boundEnc.Bytes())
	if err != nil {
		t.Fatalf("le=%v bound decode failed: %v", le, err)
	}
	if err := decBound.ReadStructFramed(ExtAppendable, XCDRVersion2, 4, func(size uint32) error { return nil }); err == nil {
		t.Fatalf("le=%v appendable bound exceed accepted", le)
	}
}

// checkTryConstructRealPaths pins TRIM/USE_DEFAULT plus bound plus unknown must-understand on real decode.
func checkTryConstructRealPaths(t *testing.T, le bool) {
	t.Helper()
	checkTryStringReal(t, le)
	checkTrySequenceReal(t, le)
	checkBoundExceedReal(t, le)
	checkUnknownMustUnderstandReal(t, le)
}

// checkTryStringReal verifies TRIM and USE_DEFAULT strings on decode.
func checkTryStringReal(t *testing.T, le bool) {
	t.Helper()
	enc := NewEncoder(le)
	if err := enc.WriteString("abcdef"); err != nil {
		t.Fatalf("le=%v write long string failed: %v", le, err)
	}
	dec, err := NewDecoder(enc.Bytes())
	if err != nil {
		t.Fatalf("le=%v decode failed: %v", le, err)
	}
	got, discard, err := dec.ReadBoundedStringWithTryConstruct(TryConstructTrim, 3)
	if err != nil || discard || got != "abc" {
		t.Fatalf("le=%v trim string got %q discard %v err %v", le, got, discard, err)
	}
	enc2 := NewEncoder(le)
	if err := enc2.WriteString("abcdef"); err != nil {
		t.Fatalf("le=%v write2 failed: %v", le, err)
	}
	dec2, err := NewDecoder(enc2.Bytes())
	if err != nil {
		t.Fatalf("le=%v decode2 failed: %v", le, err)
	}
	got, discard, err = dec2.ReadBoundedStringWithTryConstruct(TryConstructUseDefault, 3)
	if err != nil || discard || got != "" {
		t.Fatalf("le=%v default string got %q discard %v", le, got, discard)
	}
}

// checkTrySequenceReal verifies DISCARD, TRIM, USE_DEFAULT sequences on decode.
func checkTrySequenceReal(t *testing.T, le bool) {
	t.Helper()
	checkDiscardSequenceReal(t, le)
	checkTrimSequenceReal(t, le)
	checkDefaultSequenceReal(t, le)
}

// checkDiscardSequenceReal verifies DISCARD reports discard.
func checkDiscardSequenceReal(t *testing.T, le bool) {
	t.Helper()
	enc3 := NewEncoder(le)
	if err := enc3.WriteSequenceLength(8); err != nil {
		t.Fatalf("le=%v seq len failed: %v", le, err)
	}
	dec3, err := NewDecoder(enc3.Bytes())
	if err != nil {
		t.Fatalf("le=%v decode3 failed: %v", le, err)
	}
	_, discard, err := dec3.ReadBoundedSequenceLengthWithTryConstruct(TryConstructDiscard, 4)
	if err != nil || !discard {
		t.Fatalf("le=%v discard seq must discard", le)
	}
}

// checkTrimSequenceReal verifies TRIM returns bound.
func checkTrimSequenceReal(t *testing.T, le bool) {
	t.Helper()
	encTrim := NewEncoder(le)
	if err := encTrim.WriteSequenceLength(8); err != nil {
		t.Fatalf("le=%v trim seq len failed: %v", le, err)
	}
	for range 8 {
		encTrim.WriteUint8(0xAA)
	}
	decTrim, err := NewDecoder(encTrim.Bytes())
	if err != nil {
		t.Fatalf("le=%v trim decode failed: %v", le, err)
	}
	n, discard, err := decTrim.ReadBoundedSequenceLengthWithTryConstruct(TryConstructTrim, 4)
	if err != nil || discard || n != 4 {
		t.Fatalf("le=%v trim seq got %d discard %v err %v", le, n, discard, err)
	}
}

// checkDefaultSequenceReal verifies USE_DEFAULT returns zero.
func checkDefaultSequenceReal(t *testing.T, le bool) {
	t.Helper()
	encDef := NewEncoder(le)
	if err := encDef.WriteSequenceLength(8); err != nil {
		t.Fatalf("le=%v default seq len failed: %v", le, err)
	}
	for range 8 {
		encDef.WriteUint8(0xBB)
	}
	decDef, err := NewDecoder(encDef.Bytes())
	if err != nil {
		t.Fatalf("le=%v default decode failed: %v", le, err)
	}
	n, discard, err := decDef.ReadBoundedSequenceLengthWithTryConstruct(TryConstructUseDefault, 4)
	if err != nil || discard || n != 0 {
		t.Fatalf("le=%v default seq got %d discard %v", le, n, discard)
	}
}

// checkBoundExceedReal verifies over-bound fails without policy.
func checkBoundExceedReal(t *testing.T, le bool) {
	t.Helper()
	enc4 := NewEncoder(le)
	if err := enc4.WriteSequenceLength(8); err != nil {
		t.Fatalf("le=%v seq len4 failed: %v", le, err)
	}
	dec4, err := NewDecoder(enc4.Bytes())
	if err != nil {
		t.Fatalf("le=%v decode4 failed: %v", le, err)
	}
	if _, err := dec4.ReadBoundedSequenceLength(4); err == nil {
		t.Fatalf("le=%v over-bound sequence accepted", le)
	}
}

// checkUnknownMustUnderstandReal verifies unknown must-understand fails.
func checkUnknownMustUnderstandReal(t *testing.T, le bool) {
	t.Helper()
	enc5 := NewEncoderWithExtVersioned(ExtAppendable, XCDRVersion2, le)
	err := enc5.WriteStructFramed(ExtAppendable, XCDRVersion2, func() error {
		if err := enc5.WriteEMHeader(99, true, 4); err != nil {
			return err
		}
		enc5.WriteUint32(0)
		return nil
	})
	if err != nil {
		t.Fatalf("le=%v unknown enc failed: %v", le, err)
	}
	dec5, err := NewDecoder(enc5.Bytes())
	if err != nil {
		t.Fatalf("le=%v unknown decode failed: %v", le, err)
	}
	err = dec5.ReadStructFramed(ExtAppendable, XCDRVersion2, 0, func(size uint32) error {
		hdr, err := dec5.ReadEMHeader()
		if err != nil {
			return err
		}
		return SkipUnknownxt6bMember(dec5, hdr.MustUnderstand, hdr.MemberID, hdr.Size)
	})
	if err == nil {
		t.Fatalf("le=%v unknown must-understand accepted", le)
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
		Ext:    ExtAppendable,
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
