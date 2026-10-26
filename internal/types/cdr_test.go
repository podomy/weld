// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later
package types

import (
	"bytes"
	"math"
	"testing"
)

// writeLEPrimitives writes all LE test primitives.
func writeLEPrimitives(enc *Encoder) {
	enc.WriteUint8(0xAB)
	enc.WriteInt8(-5)
	enc.WriteBool(true)
	enc.WriteBool(false)
	enc.WriteChar(-12)
	enc.WriteWchar(0x03A9)
	enc.WriteInt16(-1234)
	enc.WriteUint16(60000)
	enc.WriteInt32(-123456)
	enc.WriteUint32(3000000000)
	enc.WriteInt64(-1234567890123)
	enc.WriteUint64(1234567890123)
	enc.WriteFloat32(1.5)
	enc.WriteFloat64(-2.75)
}

// checkLEBytes verifies byte sized LE reads.
func checkLEBytes(t *testing.T, dec *Decoder) {
	t.Helper()
	v8, err := dec.ReadUint8()
	if err != nil {
		t.Fatalf("ReadUint8 failed: %v", err)
	}
	if v8 != 0xAB {
		t.Fatalf("uint8 mismatch: %x", v8)
	}
	i8, err := dec.ReadInt8()
	if err != nil {
		t.Fatalf("ReadInt8 failed: %v", err)
	}
	if i8 != -5 {
		t.Fatalf("int8 mismatch: %d", i8)
	}
	b1, err := dec.ReadBool()
	if err != nil {
		t.Fatalf("ReadBool failed: %v", err)
	}
	if b1 != true {
		t.Fatalf("bool true mismatch")
	}
	b2, err := dec.ReadBool()
	if err != nil {
		t.Fatalf("ReadBool failed: %v", err)
	}
	if b2 != false {
		t.Fatalf("bool false mismatch")
	}
}

// checkLECharWchar verifies char reads.
func checkLECharWchar(t *testing.T, dec *Decoder) {
	t.Helper()
	ch, err := dec.ReadChar()
	if err != nil {
		t.Fatalf("ReadChar failed: %v", err)
	}
	if ch != -12 {
		t.Fatalf("char mismatch: %d", ch)
	}
	wc, err := dec.ReadWchar()
	if err != nil {
		t.Fatalf("ReadWchar failed: %v", err)
	}
	if wc != 0x03A9 {
		t.Fatalf("wchar mismatch: %x", wc)
	}
}

// checkLESmallReads verifies small LE reads.
func checkLESmallReads(t *testing.T, dec *Decoder) {
	t.Helper()
	checkLEBytes(t, dec)
	checkLECharWchar(t, dec)
}

// checkLEMediumReads verifies medium LE reads.
func checkLEMediumReads(t *testing.T, dec *Decoder) {
	t.Helper()
	i16, err := dec.ReadInt16()
	if err != nil {
		t.Fatalf("ReadInt16 failed: %v", err)
	}
	if i16 != -1234 {
		t.Fatalf("int16 mismatch: %d", i16)
	}
	u16, err := dec.ReadUint16()
	if err != nil {
		t.Fatalf("ReadUint16 failed: %v", err)
	}
	if u16 != 60000 {
		t.Fatalf("uint16 mismatch: %d", u16)
	}
	i32, err := dec.ReadInt32()
	if err != nil {
		t.Fatalf("ReadInt32 failed: %v", err)
	}
	if i32 != -123456 {
		t.Fatalf("int32 mismatch: %d", i32)
	}
	u32, err := dec.ReadUint32()
	if err != nil {
		t.Fatalf("ReadUint32 failed: %v", err)
	}
	if u32 != 3000000000 {
		t.Fatalf("uint32 mismatch: %d", u32)
	}
}

// checkLELargeReads verifies large LE reads and finish.
func checkLELargeReads(t *testing.T, dec *Decoder) {
	t.Helper()
	i64, err := dec.ReadInt64()
	if err != nil {
		t.Fatalf("ReadInt64 failed: %v", err)
	}
	if i64 != -1234567890123 {
		t.Fatalf("int64 mismatch: %d", i64)
	}
	u64, err := dec.ReadUint64()
	if err != nil {
		t.Fatalf("ReadUint64 failed: %v", err)
	}
	if u64 != 1234567890123 {
		t.Fatalf("uint64 mismatch: %d", u64)
	}
	f32, err := dec.ReadFloat32()
	if err != nil {
		t.Fatalf("ReadFloat32 failed: %v", err)
	}
	if f32 != 1.5 {
		t.Fatalf("float32 mismatch: %v", f32)
	}
	f64, err := dec.ReadFloat64()
	if err != nil {
		t.Fatalf("ReadFloat64 failed: %v", err)
	}
	if f64 != -2.75 {
		t.Fatalf("float64 mismatch: %v", f64)
	}
	err = dec.Finish()
	if err != nil {
		t.Fatalf("Finish found trailing bytes: %v", err)
	}
}

// TestCdrPrimitivesLERoundTrip checks LE encoding of core primitives.
func TestCdrPrimitivesLERoundTrip(t *testing.T) {
	enc := NewEncoder(true)
	writeLEPrimitives(enc)
	wire := enc.Bytes()
	if len(wire)%4 != 0 {
		t.Fatalf("wire length %d not padded to 4", len(wire))
	}
	dec, err := NewDecoder(wire)
	if err != nil {
		t.Fatalf("NewDecoder failed: %v", err)
	}
	if dec.LittleEndian() != true {
		t.Fatalf("expected little endian")
	}
	t.Run("small", func(t *testing.T) { checkLESmallReads(t, dec) })
	t.Run("medium", func(t *testing.T) { checkLEMediumReads(t, dec) })
	t.Run("large", func(t *testing.T) { checkLELargeReads(t, dec) })
}

// checkBEInts verifies BE int reads.
func checkBEInts(t *testing.T, dec *Decoder) {
	t.Helper()
	i32, err := dec.ReadInt32()
	if err != nil {
		t.Fatalf("ReadInt32 failed: %v", err)
	}
	if i32 != -42 {
		t.Fatalf("int32 mismatch: %d", i32)
	}
	u32, err := dec.ReadUint32()
	if err != nil {
		t.Fatalf("ReadUint32 failed: %v", err)
	}
	if u32 != 42 {
		t.Fatalf("uint32 mismatch: %d", u32)
	}
}

// checkBEFloats verifies BE float and tail reads.
func checkBEFloats(t *testing.T, dec *Decoder) {
	t.Helper()
	f64, err := dec.ReadFloat64()
	if err != nil {
		t.Fatalf("ReadFloat64 failed: %v", err)
	}
	if f64 != math.Pi {
		t.Fatalf("float64 mismatch: %v", f64)
	}
	i16, err := dec.ReadInt16()
	if err != nil {
		t.Fatalf("ReadInt16 failed: %v", err)
	}
	if i16 != 1234 {
		t.Fatalf("int16 mismatch: %d", i16)
	}
	wc, err := dec.ReadWchar()
	if err != nil {
		t.Fatalf("ReadWchar failed: %v", err)
	}
	if wc != 0x00E9 {
		t.Fatalf("wchar mismatch: %x", wc)
	}
	err = dec.Finish()
	if err != nil {
		t.Fatalf("Finish found trailing bytes: %v", err)
	}
}

// TestCdrPrimitivesBERoundTrip checks BE encoding of core primitives.
func TestCdrPrimitivesBERoundTrip(t *testing.T) {
	enc := NewEncoder(false)
	enc.WriteInt32(-42)
	enc.WriteUint32(42)
	enc.WriteFloat64(math.Pi)
	enc.WriteInt16(1234)
	enc.WriteWchar(0x00E9)
	wire := enc.Bytes()
	if wire[1] != 0x00 {
		t.Fatalf("expected BE flag 0x00 got 0x%02x", wire[1])
	}
	dec, err := NewDecoder(wire)
	if err != nil {
		t.Fatalf("NewDecoder failed: %v", err)
	}
	if dec.LittleEndian() != false {
		t.Fatalf("expected big endian")
	}
	t.Run("ints", func(t *testing.T) { checkBEInts(t, dec) })
	t.Run("floats", func(t *testing.T) { checkBEFloats(t, dec) })
}

// TestCdrStringPadding checks length prefix, NUL, and 4 byte pad.
func TestCdrStringPadding(t *testing.T) {
	for _, le := range []bool{true, false} {
		enc := NewEncoder(le)
		err := enc.WriteString("hi")
		if err != nil {
			t.Fatalf("le=%v WriteString failed: %v", le, err)
		}
		wire := enc.Bytes()
		if len(wire) != 12 {
			t.Fatalf("le=%v string wire length %d want 12", le, len(wire))
		}
		dec, err := NewDecoder(wire)
		if err != nil {
			t.Fatalf("NewDecoder failed: %v", err)
		}
		s, err := dec.ReadString()
		if err != nil {
			t.Fatalf("ReadString failed: %v", err)
		}
		if s != "hi" {
			t.Fatalf("string mismatch: %q", s)
		}
		err = dec.Finish()
		if err != nil {
			t.Fatalf("Finish found trailing bytes: %v", err)
		}
	}
}

// TestCdrStringEmpty checks empty string encodes length 1 plus NUL.
func TestCdrStringEmpty(t *testing.T) {
	for _, le := range []bool{true, false} {
		enc := NewEncoder(le)
		err := enc.WriteString("")
		if err != nil {
			t.Fatalf("le=%v WriteString failed: %v", le, err)
		}
		wire := enc.Bytes()
		dec, err := NewDecoder(wire)
		if err != nil {
			t.Fatalf("NewDecoder failed: %v", err)
		}
		s, err := dec.ReadString()
		if err != nil {
			t.Fatalf("ReadString failed: %v", err)
		}
		if s != "" {
			t.Fatalf("le=%v empty string mismatch: %q", le, s)
		}
	}
}

// checkBoundedRoundTrip verifies bounded truncation for string or wstring.
func checkBoundedRoundTrip(t *testing.T, isWString bool) {
	t.Helper()
	enc := NewEncoder(true)
	var truncated bool
	var err error
	if isWString {
		truncated, err = enc.WriteBoundedWString("abcdef", 3)
		if err != nil {
			t.Fatalf("WriteBoundedWString failed: %v", err)
		}
	} else {
		truncated, err = enc.WriteBoundedString("abcdef", 3)
		if err != nil {
			t.Fatalf("WriteBoundedString failed: %v", err)
		}
	}
	if truncated != true {
		t.Fatalf("expected truncated true")
	}
	wire := enc.Bytes()
	checkBoundedExact(t, wire, isWString)
	checkBoundedOverlong(t, wire, isWString)
}

// checkBoundedExactWString verifies exact wstring read.
func checkBoundedExactWString(t *testing.T, dec *Decoder) {
	t.Helper()
	s, err := dec.ReadBoundedWString(3)
	if err != nil {
		t.Fatalf("ReadBoundedWString failed: %v", err)
	}
	if s != "abc" {
		t.Fatalf("bounded wstring mismatch %q", s)
	}
}

// checkBoundedExactString verifies exact string read.
func checkBoundedExactString(t *testing.T, dec *Decoder) {
	t.Helper()
	s, err := dec.ReadBoundedString(3)
	if err != nil {
		t.Fatalf("ReadBoundedString failed: %v", err)
	}
	if s != "abc" {
		t.Fatalf("bounded string mismatch: %q", s)
	}
}

// checkBoundedExact verifies exact bound read.
func checkBoundedExact(t *testing.T, wire []byte, isWString bool) {
	t.Helper()
	dec, err := NewDecoder(wire)
	if err != nil {
		t.Fatalf("NewDecoder failed: %v", err)
	}
	if isWString {
		checkBoundedExactWString(t, dec)
	} else {
		checkBoundedExactString(t, dec)
	}
}

// checkBoundedOverlongWString verifies wstring overlong is rejected.
func checkBoundedOverlongWString(t *testing.T, dec *Decoder) {
	t.Helper()
	_, err := dec.ReadBoundedWString(2)
	if err == nil {
		t.Fatalf("expected bound error for overlong wstring")
	}
	if dec.Dropped() == 0 {
		t.Fatalf("expected drop on wstring bound violation")
	}
}

// checkBoundedOverlongString verifies string overlong is rejected.
func checkBoundedOverlongString(t *testing.T, dec *Decoder) {
	t.Helper()
	_, err := dec.ReadBoundedString(2)
	if err == nil {
		t.Fatalf("expected bound error for overlong string")
	}
	if dec.Dropped() == 0 {
		t.Fatalf("expected drop counter increment on bound violation")
	}
}

// checkBoundedOverlong verifies overlong bound is rejected with drop.
func checkBoundedOverlong(t *testing.T, wire []byte, isWString bool) {
	t.Helper()
	dec2, err := NewDecoder(wire)
	if err != nil {
		t.Fatalf("NewDecoder failed: %v", err)
	}
	if isWString {
		checkBoundedOverlongWString(t, dec2)
	} else {
		checkBoundedOverlongString(t, dec2)
	}
}

// TestCdrBoundedString checks bound enforcement on write and read.
func TestCdrBoundedString(t *testing.T) {
	checkBoundedRoundTrip(t, false)
}

// checkFixedArrayEncode verifies encode for one endian.
func checkFixedArrayEncode(t *testing.T, cov []float64, le bool) []byte {
	t.Helper()
	enc := NewEncoder(le)
	enc.Align(8)
	start := enc.Len()
	if start%8 != 0 {
		t.Fatalf("le=%v fixed array start %d not 8 aligned", le, start)
	}
	for _, v := range cov {
		enc.WriteFloat64(v)
	}
	wire := enc.Bytes()
	if len(wire)%4 != 0 {
		t.Fatalf("wire length %d not padded", len(wire))
	}
	return wire
}

// checkFixedArrayDecode verifies decode for one endian.
func checkFixedArrayDecode(t *testing.T, wire []byte, cov []float64, le bool) {
	t.Helper()
	dec, err := NewDecoder(wire)
	if err != nil {
		t.Fatalf("NewDecoder failed: %v", err)
	}
	err = dec.Align(8)
	if err != nil {
		t.Fatalf("Align failed: %v", err)
	}
	got := make([]float64, 36)
	for i := range got {
		v, err := dec.ReadFloat64()
		if err != nil {
			t.Fatalf("ReadFloat64 %d failed: %v", i, err)
		}
		got[i] = v
	}
	for i := range cov {
		if got[i] != cov[i] {
			t.Fatalf("le=%v cov[%d] %v want %v", le, i, got[i], cov[i])
		}
	}
	err = dec.Finish()
	if err != nil {
		t.Fatalf("Finish found trailing bytes: %v", err)
	}
}

// checkFixedArrayEndian runs encode plus decode for one endian.
func checkFixedArrayEndian(t *testing.T, cov []float64, le bool) {
	t.Helper()
	wire := checkFixedArrayEncode(t, cov, le)
	checkFixedArrayDecode(t, wire, cov, le)
}

// TestCdrFixedArrayAlignment checks inline float64[36] with 8 byte align.
func TestCdrFixedArrayAlignment(t *testing.T) {
	cov := make([]float64, 36)
	for i := range cov {
		cov[i] = float64(i) * 0.5
	}
	for _, le := range []bool{true, false} {
		checkFixedArrayEndian(t, cov, le)
	}
}

// checkSequencePrefixRoundTrip verifies one endian round trip.
func checkSequencePrefixRoundTrip(t *testing.T, le bool) {
	t.Helper()
	vals := []uint32{1, 2, 3, 3000000000}
	enc := NewEncoder(le)
	err := enc.WriteSequenceLength(len(vals))
	if err != nil {
		t.Fatalf("le=%v WriteSequenceLength failed: %v", le, err)
	}
	for _, v := range vals {
		enc.WriteUint32(v)
	}
	wire := enc.Bytes()
	dec, err := NewDecoder(wire)
	if err != nil {
		t.Fatalf("NewDecoder failed: %v", err)
	}
	n, err := dec.ReadSequenceLength()
	if err != nil {
		t.Fatalf("ReadSequenceLength failed: %v", err)
	}
	if n != len(vals) {
		t.Fatalf("sequence length %d want %d", n, len(vals))
	}
	checkSequencePrefixValues(t, dec, vals)
	err = dec.Finish()
	if err != nil {
		t.Fatalf("le=%v Finish found trailing bytes: %v", le, err)
	}
}

// checkSequencePrefixValues verifies sequence values.
func checkSequencePrefixValues(t *testing.T, dec *Decoder, vals []uint32) {
	t.Helper()
	for i, want := range vals {
		v, err := dec.ReadUint32()
		if err != nil {
			t.Fatalf("ReadUint32 %d failed: %v", i, err)
		}
		if v != want {
			t.Fatalf("seq[%d] %d want %d", i, v, want)
		}
	}
}

// TestCdrSequencePrefix checks unbounded sequences use uint32 prefix.
func TestCdrSequencePrefix(t *testing.T) {
	for _, le := range []bool{true, false} {
		checkSequencePrefixRoundTrip(t, le)
	}
}

// TestCdrOctetSequence checks bulk byte sequence round trip.
func TestCdrOctetSequence(t *testing.T) {
	for _, le := range []bool{true, false} {
		raw := []byte{0x01, 0x02, 0x03, 0x04, 0x05}
		enc := NewEncoder(le)
		err := enc.WriteOctetSequence(raw)
		if err != nil {
			t.Fatalf("le=%v WriteOctetSequence failed: %v", le, err)
		}
		wire := enc.Bytes()
		dec, err := NewDecoder(wire)
		if err != nil {
			t.Fatalf("NewDecoder failed: %v", err)
		}
		got, err := dec.ReadOctetSequence()
		if err != nil {
			t.Fatalf("ReadOctetSequence failed: %v", err)
		}
		if !bytes.Equal(got, raw) {
			t.Fatalf("le=%v octet sequence mismatch: %v", le, got)
		}
	}
}

// drainTruncatedDecoder drains truncated decoder allowing failures.
func drainTruncatedDecoder(t *testing.T, dec *Decoder) {
	t.Helper()
	if _, err := dec.ReadUint32(); err != nil {
		t.Logf("truncated uint32 read: %v", err)
	}
	if _, err := dec.ReadString(); err != nil {
		t.Logf("truncated string read: %v", err)
	}
	if _, err := dec.ReadFloat64(); err != nil {
		t.Logf("truncated float64 read: %v", err)
	}
	if err := dec.Finish(); err != nil {
		t.Logf("truncated finish: %v", err)
	}
}

// checkTruncatedPrefix exercises one truncated prefix without panic.
func checkTruncatedPrefix(t *testing.T, full []byte, i int) {
	t.Helper()
	trunc := full[:i]
	func() {
		defer func() {
			r := recover()
			if r != nil {
				t.Fatalf("panic on truncated len %d: %v", i, r)
			}
		}()
		dec, err := NewDecoder(trunc)
		if err != nil {
			if dec.Dropped() == 0 {
				t.Fatalf("len %d: drop not counted on header error", i)
			}
			return
		}
		drainTruncatedDecoder(t, dec)
	}()
}

// checkTruncatedFuzz fuzzes all truncated prefixes.
func checkTruncatedFuzz(t *testing.T, full []byte) {
	t.Helper()
	for i := range full {
		checkTruncatedPrefix(t, full, i)
	}
}

// checkTruncatedEmpty verifies empty buffers are rejected.
func checkTruncatedEmpty(t *testing.T) {
	t.Helper()
	_, err := NewDecoder(nil)
	if err == nil {
		t.Fatalf("expected error on empty buffer")
	}
	_, err = NewDecoder([]byte{})
	if err == nil {
		t.Fatalf("expected error on empty buffer")
	}
}

// checkTruncatedSequenceBody verifies truncated sequence body drops.
func checkTruncatedSequenceBody(t *testing.T) {
	t.Helper()
	enc2 := NewEncoder(true)
	err := enc2.WriteSequenceLength(1000)
	if err != nil {
		t.Fatalf("WriteSequenceLength failed: %v", err)
	}
	wire2 := enc2.Bytes()
	dec2, err := NewDecoder(wire2)
	if err != nil {
		t.Fatalf("NewDecoder failed: %v", err)
	}
	_, err = dec2.ReadSequenceLength()
	if err != nil {
		t.Fatalf("ReadSequenceLength should accept prefix: %v", err)
	}
	_, err = dec2.ReadUint32()
	if err == nil {
		t.Fatalf("expected truncation error reading past sequence")
	}
	if dec2.Dropped() == 0 {
		t.Fatalf("expected drop counter on truncated sequence body")
	}
}

// checkTruncatedHugePrefix verifies huge prefix cap error.
func checkTruncatedHugePrefix(t *testing.T) {
	t.Helper()
	enc3 := NewEncoder(true)
	err := enc3.WriteSequenceLength(1 << 30)
	if err != nil {
		t.Fatalf("WriteSequenceLength should allow over cap prefix: %v", err)
	}
	wire3 := enc3.Bytes()
	dec3, err := NewDecoder(wire3)
	if err != nil {
		t.Fatalf("NewDecoder failed: %v", err)
	}
	_, err = dec3.ReadSequenceLength()
	if err == nil {
		t.Fatalf("expected cap error on huge sequence prefix")
	}
	if dec3.Dropped() == 0 {
		t.Fatalf("expected drop counter on huge sequence prefix")
	}
}

// TestCdrTruncatedDropsWithoutPanic fuzzes truncated buffers for no panic.
func TestCdrTruncatedDropsWithoutPanic(t *testing.T) {
	enc := NewEncoder(true)
	enc.WriteUint32(0xDEADBEEF)
	err := enc.WriteString("hello world")
	if err != nil {
		t.Fatalf("WriteString failed: %v", err)
	}
	enc.WriteFloat64(3.25)
	full := enc.Bytes()
	t.Run("fuzz", func(t *testing.T) { checkTruncatedFuzz(t, full) })
	t.Run("empty", checkTruncatedEmpty)
	t.Run("sequence", checkTruncatedSequenceBody)
	t.Run("huge", checkTruncatedHugePrefix)
}

// TestCdrExtraFieldDrop checks trailing bytes count as a drop.
func TestCdrExtraFieldDrop(t *testing.T) {
	enc := NewEncoder(true)
	enc.WriteUint32(7)
	wire := enc.Bytes()
	wire = append(wire, 0x01, 0x02, 0x03, 0x04)
	dec, err := NewDecoder(wire)
	if err != nil {
		t.Fatalf("NewDecoder failed: %v", err)
	}
	v, err := dec.ReadUint32()
	if err != nil {
		t.Fatalf("ReadUint32 failed: %v", err)
	}
	if v != 7 {
		t.Fatalf("value mismatch: %d", v)
	}
	err = dec.Finish()
	if err == nil {
		t.Fatalf("expected extra field error on trailing bytes")
	}
	if dec.Dropped() == 0 {
		t.Fatalf("expected drop counter on extra field")
	}
}

// TestCdrBoolValidation checks bool bytes outside 0 and 1 are rejected.
func TestCdrBoolValidation(t *testing.T) {
	enc := NewEncoder(true)
	enc.WriteUint8(0x02)
	wire := enc.Bytes()
	dec, err := NewDecoder(wire)
	if err != nil {
		t.Fatalf("NewDecoder failed: %v", err)
	}
	_, err = dec.ReadBool()
	if err == nil {
		t.Fatalf("expected error on bool byte 0x02")
	}
	if dec.Dropped() == 0 {
		t.Fatalf("expected drop counter on bad bool")
	}
}

// TestCdrEmptyMessageDummyByte checks header only encodes one dummy byte.
func TestCdrEmptyMessageDummyByte(t *testing.T) {
	enc := NewEncoder(true)
	wire := enc.Bytes()
	if len(wire) != 8 {
		t.Fatalf("empty wire length %d want 8 (header plus dummy plus pad)", len(wire))
	}
	if wire[4] != 0x00 {
		t.Fatalf("dummy byte 0x%02x want 0x00", wire[4])
	}
	for _, b := range wire[5:] {
		if b != 0x00 {
			t.Fatalf("tail byte 0x%02x want zero", b)
		}
	}
}

// serviceGoldenWires builds LE and BE golden wires.
func serviceGoldenWires(t *testing.T, guid [8]byte, seq uint64, payload, wantLE, wantBE []byte) ([]byte, []byte) {
	t.Helper()
	encLE := NewEncoder(true)
	err := encLE.WriteServiceHeader(guid, seq)
	if err != nil {
		t.Fatalf("WriteServiceHeader LE failed: %v", err)
	}
	for _, b := range payload {
		encLE.WriteUint8(b)
	}
	wireLE := encLE.Bytes()
	if !bytes.Equal(wireLE, wantLE) {
		t.Fatalf("LE service golden mismatch got %v want %v", wireLE, wantLE)
	}
	encBE := NewEncoder(false)
	err = encBE.WriteServiceHeader(guid, seq)
	if err != nil {
		t.Fatalf("WriteServiceHeader BE failed: %v", err)
	}
	for _, b := range payload {
		encBE.WriteUint8(b)
	}
	wireBE := encBE.Bytes()
	if !bytes.Equal(wireBE, wantBE) {
		t.Fatalf("BE service golden mismatch got %v want %v", wireBE, wantBE)
	}
	return wireLE, wireBE
}

// checkServiceMarshalGolden verifies marshal equals golden.
func checkServiceMarshalGolden(t *testing.T, payload []byte, guid [8]byte, seq uint64, wantLE, wantBE []byte) {
	t.Helper()
	wireMarshalLE := MarshalServiceMessage(payload, guid, seq, true)
	if !bytes.Equal(wireMarshalLE, wantLE) {
		t.Fatalf("marshal LE golden mismatch got %v want %v", wireMarshalLE, wantLE)
	}
	wireMarshalBE := MarshalServiceMessage(payload, guid, seq, false)
	if !bytes.Equal(wireMarshalBE, wantBE) {
		t.Fatalf("marshal BE golden mismatch got %v want %v", wireMarshalBE, wantBE)
	}
}

// checkServiceGoldenDecode verifies decode rebasing for one wire.
func checkServiceGoldenDecode(t *testing.T, wire []byte, guid [8]byte, seq uint64, le bool) {
	t.Helper()
	label := "BE"
	if le {
		label = "LE"
	}
	dec, err := NewDecoder(wire)
	if err != nil {
		t.Fatalf("NewDecoder %s failed: %v", label, err)
	}
	gotGUID, gotSeq, err := dec.ReadServiceHeader()
	if err != nil {
		t.Fatalf("ReadServiceHeader %s failed: %v", label, err)
	}
	if gotGUID != guid {
		t.Fatalf("%s guid mismatch", label)
	}
	if gotSeq != seq {
		t.Fatalf("%s seq mismatch %x want %x", label, gotSeq, seq)
	}
	if dec.Offset() != ServiceEnvelopeSize {
		t.Fatalf("%s user payload not rebased at 20, off %d", label, dec.Offset())
	}
}

// TestCdrServiceSeqGoldenBytes pins the packed 8+8 service layout.
func TestCdrServiceSeqGoldenBytes(t *testing.T) {
	var guid [8]byte
	for i := range guid {
		guid[i] = byte(i + 1)
	}
	seq := uint64(0x0102030405060708)
	payload := []byte{0x0A, 0x0B, 0x0C}
	wantLE := []byte{
		0x00, 0x01, 0x00, 0x00,
		0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
		0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01,
		0x0A, 0x0B, 0x0C, 0x00,
	}
	wantBE := []byte{
		0x00, 0x00, 0x00, 0x00,
		0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
		0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
		0x0A, 0x0B, 0x0C, 0x00,
	}
	wireLE, wireBE := serviceGoldenWires(t, guid, seq, payload, wantLE, wantBE)
	t.Run("marshal", func(t *testing.T) { checkServiceMarshalGolden(t, payload, guid, seq, wantLE, wantBE) })
	t.Run("decodeLE", func(t *testing.T) { checkServiceGoldenDecode(t, wireLE, guid, seq, true) })
	t.Run("decodeBE", func(t *testing.T) { checkServiceGoldenDecode(t, wireBE, guid, seq, false) })
}

// checkSequenceGolden verifies sequence golden for one endian.
func checkSequenceGolden(t *testing.T, le bool, want []byte) []byte {
	t.Helper()
	enc := NewEncoder(le)
	err := enc.WriteSequenceLength(2)
	if err != nil {
		t.Fatalf("WriteSequenceLength failed: %v", err)
	}
	enc.WriteUint32(1)
	enc.WriteUint32(2)
	wire := enc.Bytes()
	if !bytes.Equal(wire, want) {
		if le {
			t.Fatalf("LE sequence golden mismatch got %v want %v", wire, want)
		} else {
			t.Fatalf("BE sequence golden mismatch got %v want %v", wire, want)
		}
	}
	return wire
}

// checkOctetGolden verifies octet golden for one endian.
func checkOctetGolden(t *testing.T, le bool, want []byte) {
	t.Helper()
	enc := NewEncoder(le)
	err := enc.WriteOctetSequence([]byte{0x01, 0x02, 0x03})
	if err != nil {
		t.Fatalf("WriteOctetSequence failed: %v", err)
	}
	wire := enc.Bytes()
	if !bytes.Equal(wire, want) {
		if le {
			t.Fatalf("LE octet golden mismatch got %v want %v", wire, want)
		} else {
			t.Fatalf("BE octet golden mismatch got %v want %v", wire, want)
		}
	}
}

// checkSequenceBETrailing verifies unfinished BE sequence drops.
func checkSequenceBETrailing(t *testing.T, wireBE []byte) {
	t.Helper()
	decBE, err := NewDecoder(wireBE)
	if err != nil {
		t.Fatalf("NewDecoder BE failed: %v", err)
	}
	n, err := decBE.ReadSequenceLength()
	if err != nil {
		t.Fatalf("ReadSequenceLength BE failed: %v", err)
	}
	if n != 2 {
		t.Fatalf("BE sequence length %d want 2", n)
	}
	err = decBE.Finish()
	if err == nil {
		t.Fatalf("expected trailing error on unfinished BE sequence")
	}
	if decBE.Dropped() == 0 {
		t.Fatalf("expected drop on unfinished BE sequence")
	}
}

// TestCdrSequenceBEGolden checks BE golden bytes for sequences and octets.
func TestCdrSequenceBEGolden(t *testing.T) {
	wantLE := []byte{
		0x00, 0x01, 0x00, 0x00,
		0x02, 0x00, 0x00, 0x00,
		0x01, 0x00, 0x00, 0x00,
		0x02, 0x00, 0x00, 0x00,
	}
	wantBE := []byte{
		0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x02,
		0x00, 0x00, 0x00, 0x01,
		0x00, 0x00, 0x00, 0x02,
	}
	wantOctLE := []byte{
		0x00, 0x01, 0x00, 0x00,
		0x03, 0x00, 0x00, 0x00,
		0x01, 0x02, 0x03, 0x00,
	}
	wantOctBE := []byte{
		0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x03,
		0x01, 0x02, 0x03, 0x00,
	}
	t.Run("seqLE", func(t *testing.T) { checkSequenceGolden(t, true, wantLE) })
	t.Run("seqBE", func(t *testing.T) { checkSequenceGolden(t, false, wantBE) })
	t.Run("octLE", func(t *testing.T) { checkOctetGolden(t, true, wantOctLE) })
	t.Run("octBE", func(t *testing.T) { checkOctetGolden(t, false, wantOctBE) })
	wireBE := checkSequenceGolden(t, false, wantBE)
	checkSequenceBETrailing(t, wireBE)
}

// TestCdrWStringRoundTrip checks wstring encode and decode both orders.
func TestCdrWStringRoundTrip(t *testing.T) {
	for _, le := range []bool{true, false} {
		enc := NewEncoder(le)
		err := enc.WriteWString("hi")
		if err != nil {
			t.Fatalf("le=%v WriteWString failed: %v", le, err)
		}
		wire := enc.Bytes()
		if len(wire)%4 != 0 {
			t.Fatalf("le=%v wstring wire %d not padded", le, len(wire))
		}
		dec, err := NewDecoder(wire)
		if err != nil {
			t.Fatalf("NewDecoder failed: %v", err)
		}
		s, err := dec.ReadWString()
		if err != nil {
			t.Fatalf("ReadWString failed: %v", err)
		}
		if s != "hi" {
			t.Fatalf("le=%v wstring mismatch %q", le, s)
		}
		err = dec.Finish()
		if err != nil {
			t.Fatalf("Finish failed: %v", err)
		}
	}
}

// TestCdrWStringBounded checks wstring bound truncation on rune boundary.
func TestCdrWStringBounded(t *testing.T) {
	checkBoundedRoundTrip(t, true)
}

// TestCdrWStringMissingNUL checks wstring without terminator drops.
func TestCdrWStringMissingNUL(t *testing.T) {
	for _, le := range []bool{true, false} {
		enc := NewEncoder(le)
		err := enc.WriteWString("hi")
		if err != nil {
			t.Fatalf("le=%v WriteWString failed: %v", le, err)
		}
		wire := enc.Bytes()
		wire[len(wire)-3] = 0x41
		dec, err := NewDecoder(wire)
		if err != nil {
			t.Fatalf("NewDecoder failed: %v", err)
		}
		_, err = dec.ReadWString()
		if err == nil {
			t.Fatalf("le=%v expected missing NUL error", le)
		}
		if dec.Dropped() == 0 {
			t.Fatalf("le=%v expected drop on missing wstring NUL", le)
		}
	}
}

// TestCdrBoundedStringNonASCII checks truncation never splits UTF-8.
func TestCdrBoundedStringNonASCII(t *testing.T) {
	s := "a\u2705b"
	enc := NewEncoder(true)
	truncated, err := enc.WriteBoundedString(s, 2)
	if err != nil {
		t.Fatalf("WriteBoundedString failed: %v", err)
	}
	if truncated != true {
		t.Fatalf("expected truncated true for non-ASCII bound")
	}
	wire := enc.Bytes()
	dec, err := NewDecoder(wire)
	if err != nil {
		t.Fatalf("NewDecoder failed: %v", err)
	}
	got, err := dec.ReadString()
	if err != nil {
		t.Fatalf("ReadString failed: %v", err)
	}
	if got != "a\u2705" {
		t.Fatalf("non-ASCII truncation mismatch %q want %q", got, "a\u2705")
	}
	enc2 := NewEncoder(true)
	truncated, err = enc2.WriteBoundedWString(s, 2)
	if err != nil {
		t.Fatalf("WriteBoundedWString failed: %v", err)
	}
	if truncated != true {
		t.Fatalf("expected wstring truncated true")
	}
	wire2 := enc2.Bytes()
	dec2, err := NewDecoder(wire2)
	if err != nil {
		t.Fatalf("NewDecoder failed: %v", err)
	}
	got2, err := dec2.ReadWString()
	if err != nil {
		t.Fatalf("ReadWString failed: %v", err)
	}
	if got2 != "a\u2705" {
		t.Fatalf("wstring non-ASCII mismatch %q", got2)
	}
}

// checkNegativeWriteBounds verifies negative write bounds do not mutate.
func checkNegativeWriteBounds(t *testing.T, enc *Encoder, before int) {
	t.Helper()
	_, err := enc.WriteBoundedString("hi", -1)
	if err == nil {
		t.Fatalf("expected error on negative string bound")
	}
	if enc.Len() != before {
		t.Fatalf("buffer mutated on negative bound")
	}
	_, err = enc.WriteBoundedWString("hi", -2)
	if err == nil {
		t.Fatalf("expected error on negative wstring bound")
	}
	if enc.Len() != before {
		t.Fatalf("buffer mutated on negative wstring bound")
	}
	err = enc.WriteBoundedSequenceLength(1, -1)
	if err == nil {
		t.Fatalf("expected error on negative sequence bound")
	}
	if enc.Len() != before {
		t.Fatalf("buffer mutated on negative sequence bound")
	}
}

// checkNegativeReadBounds verifies negative read bounds count drops.
func checkNegativeReadBounds(t *testing.T, wire []byte) {
	t.Helper()
	dec, err := NewDecoder(wire)
	if err != nil {
		t.Fatalf("NewDecoder failed: %v", err)
	}
	_, err = dec.ReadBoundedString(-1)
	if err == nil {
		t.Fatalf("expected error on negative read bound")
	}
	if dec.Dropped() == 0 {
		t.Fatalf("expected drop on negative read bound")
	}
	dec2, err := NewDecoder(wire)
	if err != nil {
		t.Fatalf("NewDecoder failed: %v", err)
	}
	_, err = dec2.ReadBoundedWString(-1)
	if err == nil {
		t.Fatalf("expected error on negative wstring read bound")
	}
	if dec2.Dropped() == 0 {
		t.Fatalf("expected drop on negative wstring read bound")
	}
	dec3, err := NewDecoder(wire)
	if err != nil {
		t.Fatalf("NewDecoder failed: %v", err)
	}
	_, err = dec3.ReadBoundedSequenceLength(-1)
	if err == nil {
		t.Fatalf("expected error on negative sequence read bound")
	}
	if dec3.Dropped() == 0 {
		t.Fatalf("expected drop on negative sequence read bound")
	}
}

// TestCdrNegativeBound checks negative bounds error without mutation.
func TestCdrNegativeBound(t *testing.T) {
	enc := NewEncoder(true)
	before := enc.Len()
	t.Run("write", func(t *testing.T) { checkNegativeWriteBounds(t, enc, before) })
	err := enc.WriteString("hi")
	if err != nil {
		t.Fatalf("WriteString failed: %v", err)
	}
	wire := enc.Bytes()
	t.Run("read", func(t *testing.T) { checkNegativeReadBounds(t, wire) })
}

// TestCdrNegativeSequenceLength checks negative lengths return errors.
func TestCdrNegativeSequenceLength(t *testing.T) {
	enc := NewEncoder(true)
	before := enc.Len()
	err := enc.WriteSequenceLength(-1)
	if err == nil {
		t.Fatalf("expected error on negative sequence length")
	}
	if enc.Len() != before {
		t.Fatalf("buffer mutated on negative sequence length")
	}
	err = enc.WriteBoundedSequenceLength(-1, 3)
	if err == nil {
		t.Fatalf("expected error on negative bounded sequence length")
	}
	if enc.Len() != before {
		t.Fatalf("buffer mutated on negative bounded length")
	}
}

// checkStringPadZero verifies zero length is rejected.
func checkStringPadZero(t *testing.T, le bool) {
	t.Helper()
	enc := NewEncoder(le)
	enc.WriteUint32(0)
	wireZero := enc.Bytes()
	decZero, err := NewDecoder(wireZero)
	if err != nil {
		t.Fatalf("le=%v NewDecoder failed: %v", le, err)
	}
	_, err = decZero.ReadString()
	if err == nil {
		t.Fatalf("le=%v expected error on zero string length", le)
	}
	if decZero.Dropped() == 0 {
		t.Fatalf("le=%v expected drop on zero length", le)
	}
}

// checkStringPadMissingNUL verifies missing NUL is rejected.
func checkStringPadMissingNUL(t *testing.T, le bool) {
	t.Helper()
	enc2 := NewEncoder(le)
	err := enc2.WriteString("hi")
	if err != nil {
		t.Fatalf("le=%v WriteString failed: %v", le, err)
	}
	wire := enc2.Bytes()
	wire[10] = 0x41
	dec, err := NewDecoder(wire)
	if err != nil {
		t.Fatalf("le=%v NewDecoder failed: %v", le, err)
	}
	_, err = dec.ReadString()
	if err == nil {
		t.Fatalf("le=%v expected error on missing NUL", le)
	}
	if dec.Dropped() == 0 {
		t.Fatalf("le=%v expected drop on missing NUL", le)
	}
}

// checkStringPadCap verifies huge prefix cap is rejected.
func checkStringPadCap(t *testing.T, le bool) {
	t.Helper()
	enc3 := NewEncoder(le)
	enc3.WriteUint32(uint32(maxStringBytes + 1))
	wireCap := enc3.Bytes()
	decCap, err := NewDecoder(wireCap)
	if err != nil {
		t.Fatalf("le=%v NewDecoder failed: %v", le, err)
	}
	_, err = decCap.ReadString()
	if err == nil {
		t.Fatalf("le=%v expected cap error on huge string prefix", le)
	}
	if decCap.Dropped() == 0 {
		t.Fatalf("le=%v expected drop on huge string prefix", le)
	}
}

// checkStringPadTruncated verifies truncated pad is rejected.
func checkStringPadTruncated(t *testing.T, le bool) {
	t.Helper()
	enc4 := NewEncoder(le)
	err := enc4.WriteString("hi")
	if err != nil {
		t.Fatalf("le=%v WriteString failed: %v", le, err)
	}
	full := enc4.Bytes()
	truncatedPad := full[:len(full)-1]
	decPad, err := NewDecoder(truncatedPad)
	if err != nil {
		t.Fatalf("le=%v NewDecoder failed: %v", le, err)
	}
	_, err = decPad.ReadString()
	if err == nil {
		t.Fatalf("le=%v expected error on truncated string pad", le)
	}
	if decPad.Dropped() == 0 {
		t.Fatalf("le=%v expected drop on truncated pad", le)
	}
}

// checkStringPadEndian runs all pad checks for one endian.
func checkStringPadEndian(t *testing.T, le bool) {
	t.Helper()
	checkStringPadZero(t, le)
	checkStringPadMissingNUL(t, le)
	checkStringPadCap(t, le)
	checkStringPadTruncated(t, le)
}

// TestCdrStringPadCases checks length 0, NUL, cap, and pad errors.
func TestCdrStringPadCases(t *testing.T) {
	for _, le := range []bool{true, false} {
		checkStringPadEndian(t, le)
	}
}

// checkHugeWStringCap verifies huge wstring cap.
func checkHugeWStringCap(t *testing.T, le bool) {
	t.Helper()
	enc := NewEncoder(le)
	enc.WriteUint32(uint32(maxWStringUnits + 1))
	wire := enc.Bytes()
	dec, err := NewDecoder(wire)
	if err != nil {
		t.Fatalf("le=%v NewDecoder failed: %v", le, err)
	}
	_, err = dec.ReadWString()
	if err == nil {
		t.Fatalf("le=%v expected cap error on huge wstring prefix", le)
	}
	if dec.Dropped() == 0 {
		t.Fatalf("le=%v expected drop on huge wstring prefix", le)
	}
}

// checkHugeSequenceCap verifies huge sequence cap.
func checkHugeSequenceCap(t *testing.T, le bool) {
	t.Helper()
	enc2 := NewEncoder(le)
	err := enc2.WriteSequenceLength(maxSequenceElements + 1)
	if err != nil {
		t.Fatalf("le=%v WriteSequenceLength should allow over cap: %v", le, err)
	}
	wire2 := enc2.Bytes()
	dec2, err := NewDecoder(wire2)
	if err != nil {
		t.Fatalf("le=%v NewDecoder failed: %v", le, err)
	}
	_, err = dec2.ReadSequenceLength()
	if err == nil {
		t.Fatalf("le=%v expected cap error on huge sequence prefix", le)
	}
	if dec2.Dropped() == 0 {
		t.Fatalf("le=%v expected drop on huge sequence prefix", le)
	}
}

// checkHugeStringCap verifies huge string cap.
func checkHugeStringCap(t *testing.T, le bool) {
	t.Helper()
	enc3 := NewEncoder(le)
	enc3.WriteUint32(uint32(maxStringBytes + 1))
	wire3 := enc3.Bytes()
	dec3, err := NewDecoder(wire3)
	if err != nil {
		t.Fatalf("le=%v NewDecoder failed: %v", le, err)
	}
	_, err = dec3.ReadString()
	if err == nil {
		t.Fatalf("le=%v expected cap error on huge string prefix", le)
	}
	if dec3.Dropped() == 0 {
		t.Fatalf("le=%v expected drop on huge string prefix", le)
	}
}

// checkHugePrefixEndian runs all huge cap checks for one endian.
func checkHugePrefixEndian(t *testing.T, le bool) {
	t.Helper()
	checkHugeWStringCap(t, le)
	checkHugeSequenceCap(t, le)
	checkHugeStringCap(t, le)
}

// TestCdrHugePrefixCaps checks huge wstring and sequence caps.
func TestCdrHugePrefixCaps(t *testing.T) {
	for _, le := range []bool{true, false} {
		checkHugePrefixEndian(t, le)
	}
}

// TestCdrInvalidAlign checks invalid alignment handling.
func TestCdrInvalidAlign(t *testing.T) {
	enc := NewEncoder(true)
	enc.WriteUint8(0x01)
	before := enc.Len()
	enc.Align(3)
	if enc.Len() != before {
		t.Fatalf("invalid Align mutated encoder")
	}
	enc.Align(0)
	if enc.Len() != before {
		t.Fatalf("invalid Align 0 mutated encoder")
	}
	wire := enc.Bytes()
	dec, err := NewDecoder(wire)
	if err != nil {
		t.Fatalf("NewDecoder failed: %v", err)
	}
	dropsBefore := dec.Dropped()
	err = dec.Align(3)
	if err == nil {
		t.Fatalf("expected error on invalid align")
	}
	if dec.Dropped() != dropsBefore {
		t.Fatalf("invalid align must not count a drop")
	}
	err = dec.Align(0)
	if err == nil {
		t.Fatalf("expected error on invalid align 0")
	}
	if dec.Dropped() != dropsBefore {
		t.Fatalf("invalid align 0 must not count a drop")
	}
}

// TestCdrBytesTwice checks Bytes purity and Len accounting.
func TestCdrBytesTwice(t *testing.T) {
	enc := NewEncoder(true)
	enc.WriteUint32(0x12345678)
	before := enc.Len()
	first := enc.Bytes()
	afterFirst := enc.Len()
	second := enc.Bytes()
	if before != afterFirst {
		t.Fatalf("Len changed after Bytes %d vs %d", before, afterFirst)
	}
	if afterFirst != enc.Len() {
		t.Fatalf("Len changed after second Bytes")
	}
	if !bytes.Equal(first, second) {
		t.Fatalf("Bytes twice mismatch %v vs %v", first, second)
	}
	enc.WriteUint8(0xFF)
	third := enc.Bytes()
	if bytes.Equal(second, third) {
		t.Fatalf("encoder not usable after Bytes")
	}
	if enc.Len() == before {
		t.Fatalf("Len did not grow after post Bytes write")
	}
}

// checkBoundedSequenceValid verifies valid bounded sequence round trip.
func checkBoundedSequenceValid(t *testing.T, le bool) {
	t.Helper()
	enc := NewEncoder(le)
	err := enc.WriteBoundedSequenceLength(3, 3)
	if err != nil {
		t.Fatalf("le=%v WriteBoundedSequenceLength failed: %v", le, err)
	}
	enc.WriteUint32(10)
	enc.WriteUint32(20)
	enc.WriteUint32(30)
	wire := enc.Bytes()
	dec, err := NewDecoder(wire)
	if err != nil {
		t.Fatalf("le=%v NewDecoder failed: %v", le, err)
	}
	n, err := dec.ReadBoundedSequenceLength(3)
	if err != nil {
		t.Fatalf("le=%v ReadBoundedSequenceLength failed: %v", le, err)
	}
	if n != 3 {
		t.Fatalf("le=%v bounded length %d want 3", le, n)
	}
	checkBoundedSequenceValues(t, dec, le)
	err = dec.Finish()
	if err != nil {
		t.Fatalf("le=%v Finish failed: %v", le, err)
	}
}

// checkBoundedSequenceValues verifies bounded values.
func checkBoundedSequenceValues(t *testing.T, dec *Decoder, le bool) {
	t.Helper()
	for i, want := range []uint32{10, 20, 30} {
		v, err := dec.ReadUint32()
		if err != nil {
			t.Fatalf("le=%v ReadUint32 %d failed: %v", le, i, err)
		}
		if v != want {
			t.Fatalf("le=%v seq[%d] %d want %d", le, i, v, want)
		}
	}
}

// checkBoundedSequenceOverlong verifies overlong write and read.
func checkBoundedSequenceOverlong(t *testing.T, le bool) {
	t.Helper()
	enc2 := NewEncoder(le)
	err := enc2.WriteBoundedSequenceLength(4, 3)
	if err == nil {
		t.Fatalf("le=%v expected bound error on overlong write", le)
	}
	if enc2.Len() != HeaderSize {
		t.Fatalf("le=%v overlong bounded write mutated buffer", le)
	}
	enc3 := NewEncoder(le)
	err = enc3.WriteSequenceLength(4)
	if err != nil {
		t.Fatalf("le=%v WriteSequenceLength failed: %v", le, err)
	}
	wire3 := enc3.Bytes()
	dec3, err := NewDecoder(wire3)
	if err != nil {
		t.Fatalf("le=%v NewDecoder failed: %v", le, err)
	}
	_, err = dec3.ReadBoundedSequenceLength(3)
	if err == nil {
		t.Fatalf("le=%v expected bound error on overlong read", le)
	}
	if dec3.Dropped() == 0 {
		t.Fatalf("le=%v expected drop on overlong bounded read", le)
	}
}

// checkBoundedSequenceEndian runs both checks for one endian.
func checkBoundedSequenceEndian(t *testing.T, le bool) {
	t.Helper()
	checkBoundedSequenceValid(t, le)
	checkBoundedSequenceOverlong(t, le)
}

// TestCdrBoundedSequence checks bounded sequence length helpers.
func TestCdrBoundedSequence(t *testing.T) {
	for _, le := range []bool{true, false} {
		checkBoundedSequenceEndian(t, le)
	}
}

// checkBoundedZeroValues verifies zero bound values.
func checkBoundedZeroValues(t *testing.T, dec *Decoder, le bool) {
	t.Helper()
	for i, want := range []uint32{11, 22} {
		v, err := dec.ReadUint32()
		if err != nil {
			t.Fatalf("le=%v ReadUint32 %d failed: %v", le, i, err)
		}
		if v != want {
			t.Fatalf("le=%v seq[%d] %d want %d", le, i, v, want)
		}
	}
}

// checkBoundedZeroEndian verifies zero bound for one endian.
func checkBoundedZeroEndian(t *testing.T, le bool) {
	t.Helper()
	enc := NewEncoder(le)
	err := enc.WriteBoundedSequenceLength(2, 0)
	if err != nil {
		t.Fatalf("le=%v WriteBoundedSequenceLength bound 0 failed: %v", le, err)
	}
	enc.WriteUint32(11)
	enc.WriteUint32(22)
	wire := enc.Bytes()
	dec, err := NewDecoder(wire)
	if err != nil {
		t.Fatalf("le=%v NewDecoder failed: %v", le, err)
	}
	n, err := dec.ReadBoundedSequenceLength(0)
	if err != nil {
		t.Fatalf("le=%v ReadBoundedSequenceLength bound 0 failed: %v", le, err)
	}
	if n != 2 {
		t.Fatalf("le=%v bound 0 length %d want 2", le, n)
	}
	checkBoundedZeroValues(t, dec, le)
	err = dec.Finish()
	if err != nil {
		t.Fatalf("le=%v Finish failed: %v", le, err)
	}
}

// TestCdrBoundedZeroUnbounded checks bound zero means unbounded.
func TestCdrBoundedZeroUnbounded(t *testing.T) {
	for _, le := range []bool{true, false} {
		checkBoundedZeroEndian(t, le)
	}
}

// checkBoundedTruncatedEndian verifies truncated body for one endian.
func checkBoundedTruncatedEndian(t *testing.T, le bool) {
	t.Helper()
	enc := NewEncoder(le)
	err := enc.WriteBoundedSequenceLength(3, 3)
	if err != nil {
		t.Fatalf("le=%v WriteBoundedSequenceLength failed: %v", le, err)
	}
	enc.WriteUint32(1)
	enc.WriteUint32(2)
	enc.WriteUint32(3)
	full := enc.Bytes()
	trunc := full[:len(full)-4]
	dec, err := NewDecoder(trunc)
	if err != nil {
		t.Fatalf("le=%v NewDecoder failed: %v", le, err)
	}
	n, err := dec.ReadBoundedSequenceLength(3)
	if err != nil {
		t.Fatalf("le=%v ReadBoundedSequenceLength failed: %v", le, err)
	}
	if n != 3 {
		t.Fatalf("le=%v bounded length %d want 3", le, n)
	}
	_, err = dec.ReadUint32()
	if err != nil {
		t.Fatalf("le=%v first element failed: %v", le, err)
	}
	_, err = dec.ReadUint32()
	if err != nil {
		t.Fatalf("le=%v second element failed: %v", le, err)
	}
	checkBoundedTruncatedTail(t, dec, le)
}

// checkBoundedTruncatedTail verifies tail truncation drops.
func checkBoundedTruncatedTail(t *testing.T, dec *Decoder, le bool) {
	t.Helper()
	_, err := dec.ReadUint32()
	if err == nil {
		t.Fatalf("le=%v expected truncation on bounded body", le)
	}
	if dec.Dropped() == 0 {
		t.Fatalf("le=%v expected drop on truncated bounded body", le)
	}
}

// TestCdrBoundedTruncatedBody checks truncated bounded body drops.
func TestCdrBoundedTruncatedBody(t *testing.T) {
	for _, le := range []bool{true, false} {
		checkBoundedTruncatedEndian(t, le)
	}
}

// checkEncoderCheckedSequence verifies checked sequence caps.
func checkEncoderCheckedSequence(t *testing.T) {
	t.Helper()
	enc := NewEncoder(true)
	err := enc.WriteSequenceLengthChecked(maxSequenceElements + 1)
	if err == nil {
		t.Fatalf("expected cap error on checked sequence length")
	}
	if enc.Len() != HeaderSize {
		t.Fatalf("checked over cap mutated buffer")
	}
	err = enc.WriteSequenceLengthChecked(3)
	if err != nil {
		t.Fatalf("WriteSequenceLengthChecked failed: %v", err)
	}
	if enc.Len() != HeaderSize+4 {
		t.Fatalf("checked valid length not written")
	}
	enc2 := NewEncoder(true)
	err = enc2.WriteOctetSequenceChecked(make([]byte, maxSequenceElements+1))
	if err == nil {
		t.Fatalf("expected cap error on checked octet sequence")
	}
}

// checkEncoderCheckedString verifies checked string caps.
func checkEncoderCheckedString(t *testing.T) {
	t.Helper()
	enc3 := NewEncoder(true)
	big := make([]byte, maxStringBytes+1)
	for i := range big {
		big[i] = 'a'
	}
	err := enc3.WriteStringChecked(string(big))
	if err == nil {
		t.Fatalf("expected cap error on checked string")
	}
	if enc3.Len() != HeaderSize {
		t.Fatalf("checked over cap string mutated buffer")
	}
}

// checkEncoderUnboundedCap verifies unbounded over cap decodes with drop.
func checkEncoderUnboundedCap(t *testing.T) {
	t.Helper()
	enc4 := NewEncoder(true)
	err := enc4.WriteSequenceLength(maxSequenceElements + 1)
	if err != nil {
		t.Fatalf("unbounded over cap should encode: %v", err)
	}
	wire := enc4.Bytes()
	dec, err := NewDecoder(wire)
	if err != nil {
		t.Fatalf("NewDecoder failed: %v", err)
	}
	_, err = dec.ReadSequenceLength()
	if err == nil {
		t.Fatalf("expected decode cap error on cap plus 1 prefix")
	}
	if dec.Dropped() == 0 {
		t.Fatalf("expected drop on cap plus 1 decode")
	}
}

// checkEncoderOverflowWString verifies overflow and wstring checked.
func checkEncoderOverflowWString(t *testing.T) {
	t.Helper()
	enc5 := NewEncoder(true)
	err := enc5.WriteSequenceLength(int(uint64(math.MaxUint32) + 1))
	if err == nil {
		t.Fatalf("expected overflow error on huge sequence length")
	}
	if enc5.Len() != HeaderSize {
		t.Fatalf("overflow write mutated buffer")
	}
	enc6 := NewEncoder(true)
	err = enc6.WriteWStringChecked("hi")
	if err != nil {
		t.Fatalf("WriteWStringChecked valid failed: %v", err)
	}
	wire6 := enc6.Bytes()
	dec6, err := NewDecoder(wire6)
	if err != nil {
		t.Fatalf("NewDecoder failed: %v", err)
	}
	got6, err := dec6.ReadWString()
	if err != nil {
		t.Fatalf("ReadWString failed: %v", err)
	}
	if got6 != "hi" {
		t.Fatalf("checked wstring mismatch %q", got6)
	}
}

// TestCdrEncoderCapsChecked checks checked writes enforce caps.
func TestCdrEncoderCapsChecked(t *testing.T) {
	t.Run("sequence", checkEncoderCheckedSequence)
	t.Run("string", checkEncoderCheckedString)
	t.Run("unbounded", checkEncoderUnboundedCap)
	t.Run("overflow", checkEncoderOverflowWString)
}

// checkAstralSingle verifies single astral fits bound 1.
func checkAstralSingle(t *testing.T, le bool, astral string) {
	t.Helper()
	enc := NewEncoder(le)
	truncated, err := enc.WriteBoundedWString(astral, 1)
	if err != nil {
		t.Fatalf("le=%v WriteBoundedWString failed: %v", le, err)
	}
	if truncated {
		t.Fatalf("le=%v astral bound 1 should not truncate", le)
	}
	wire := enc.Bytes()
	dec, err := NewDecoder(wire)
	if err != nil {
		t.Fatalf("le=%v NewDecoder failed: %v", le, err)
	}
	got, err := dec.ReadBoundedWString(1)
	if err != nil {
		t.Fatalf("le=%v ReadBoundedWString failed: %v", le, err)
	}
	if got != astral {
		t.Fatalf("le=%v astral mismatch %q", le, got)
	}
}

// checkAstralDouble verifies double astral truncates to one.
func checkAstralDouble(t *testing.T, le bool, astral string) {
	t.Helper()
	enc2 := NewEncoder(le)
	truncated, err := enc2.WriteBoundedWString(astral+astral, 1)
	if err != nil {
		t.Fatalf("le=%v WriteBoundedWString double failed: %v", le, err)
	}
	if !truncated {
		t.Fatalf("le=%v expected truncate for two astral with bound 1", le)
	}
	wire2 := enc2.Bytes()
	dec2, err := NewDecoder(wire2)
	if err != nil {
		t.Fatalf("le=%v NewDecoder failed: %v", le, err)
	}
	got2, err := dec2.ReadWString()
	if err != nil {
		t.Fatalf("le=%v ReadWString failed: %v", le, err)
	}
	if got2 != astral {
		t.Fatalf("le=%v truncated astral mismatch %q", le, got2)
	}
}

// checkAstralEndian runs both astral checks for one endian.
func checkAstralEndian(t *testing.T, le bool, astral string) {
	t.Helper()
	checkAstralSingle(t, le, astral)
	checkAstralDouble(t, le, astral)
}

// TestCdrWStringAstralBound checks rune vs unit bound for astral runes.
func TestCdrWStringAstralBound(t *testing.T) {
	astral := "\U0001F600"
	if countRunes(astral) != 1 {
		t.Fatalf("astral rune count %d want 1", countRunes(astral))
	}
	for _, le := range []bool{true, false} {
		checkAstralEndian(t, le, astral)
	}
}

// checkTolerantInterior verifies interior NUL preservation.
func checkTolerantInterior(t *testing.T, le bool) {
	t.Helper()
	enc := NewEncoder(le)
	interior := "a\x00b"
	err := enc.WriteString(interior)
	if err != nil {
		t.Fatalf("le=%v WriteString failed: %v", le, err)
	}
	wire := enc.Bytes()
	dec, err := NewDecoder(wire)
	if err != nil {
		t.Fatalf("le=%v NewDecoder failed: %v", le, err)
	}
	got, err := dec.ReadString()
	if err != nil {
		t.Fatalf("le=%v ReadString failed: %v", le, err)
	}
	if got != interior {
		t.Fatalf("le=%v interior NUL not preserved %q", le, got)
	}
	err = dec.Finish()
	if err != nil {
		t.Fatalf("le=%v Finish failed: %v", le, err)
	}
}

// checkTolerantInvalid verifies invalid UTF-8 preservation.
func checkTolerantInvalid(t *testing.T, le bool) {
	t.Helper()
	enc2 := NewEncoder(le)
	invalid := "\xff\xfe"
	err := enc2.WriteString(invalid)
	if err != nil {
		t.Fatalf("le=%v WriteString invalid failed: %v", le, err)
	}
	wire2 := enc2.Bytes()
	dec2, err := NewDecoder(wire2)
	if err != nil {
		t.Fatalf("le=%v NewDecoder failed: %v", le, err)
	}
	got2, err := dec2.ReadString()
	if err != nil {
		t.Fatalf("le=%v ReadString invalid failed: %v", le, err)
	}
	if got2 != invalid {
		t.Fatalf("le=%v invalid UTF-8 not preserved %q", le, got2)
	}
}

// checkTolerantEndian runs both tolerant checks for one endian.
func checkTolerantEndian(t *testing.T, le bool) {
	t.Helper()
	checkTolerantInterior(t, le)
	checkTolerantInvalid(t, le)
}

// checkTolerantLoneSurrogate verifies lone surrogate maps to replacement.
func checkTolerantLoneSurrogate(t *testing.T) {
	t.Helper()
	// Lone high surrogate 0xD800 plus NUL decodes to U+FFFD.
	// Build LE wire by hand: header plus length 2 plus units.
	wireLone := []byte{0x00, 0x01, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00, 0xD8, 0x00, 0x00}
	dec, err := NewDecoder(wireLone)
	if err != nil {
		t.Fatalf("NewDecoder failed: %v", err)
	}
	got, err := dec.ReadWString()
	if err != nil {
		t.Fatalf("ReadWString lone surrogate failed: %v", err)
	}
	if got != "\uFFFD" {
		t.Fatalf("lone surrogate did not map to U+FFFD, got %q", got)
	}
}

// TestCdrStringTolerantPolicy checks interior NUL and encoding tolerance.
func TestCdrStringTolerantPolicy(t *testing.T) {
	for _, le := range []bool{true, false} {
		checkTolerantEndian(t, le)
	}
	t.Run("lone", checkTolerantLoneSurrogate)
}

// checkServiceDoubleWrite verifies double header is rejected.
func checkServiceDoubleWrite(t *testing.T, le bool, guid [8]byte) {
	t.Helper()
	enc := NewEncoder(le)
	err := enc.WriteServiceHeader(guid, 1)
	if err != nil {
		t.Fatalf("le=%v first WriteServiceHeader failed: %v", le, err)
	}
	before := enc.Len()
	err = enc.WriteServiceHeader(guid, 2)
	if err == nil {
		t.Fatalf("le=%v expected error on double service header", le)
	}
	if enc.Len() != before {
		t.Fatalf("le=%v double header mutated buffer", le)
	}
	enc2 := NewEncoder(le)
	enc2.WriteUint32(9)
	err = enc2.WriteServiceHeader(guid, 1)
	if err == nil {
		t.Fatalf("le=%v expected error on header after user field", le)
	}
}

// checkServiceDoubleRead verifies late header is rejected with drop.
func checkServiceDoubleRead(t *testing.T, le bool, guid [8]byte) {
	t.Helper()
	enc := NewEncoder(le)
	err := enc.WriteServiceHeader(guid, 1)
	if err != nil {
		t.Fatalf("le=%v WriteServiceHeader failed: %v", le, err)
	}
	dec, err := NewDecoder(enc.Bytes())
	if err != nil {
		t.Fatalf("le=%v NewDecoder failed: %v", le, err)
	}
	_, _, err = dec.ReadServiceHeader()
	if err != nil {
		t.Fatalf("le=%v ReadServiceHeader failed: %v", le, err)
	}
	_, err = dec.ReadUint8()
	if err != nil {
		t.Fatalf("le=%v ReadUint8 failed: %v", le, err)
	}
	_, _, err = dec.ReadServiceHeader()
	if err == nil {
		t.Fatalf("le=%v expected error on late ReadServiceHeader", le)
	}
	if dec.Dropped() == 0 {
		t.Fatalf("le=%v expected drop on late service header", le)
	}
}

// checkServiceDoubleEndian runs both double checks for one endian.
func checkServiceDoubleEndian(t *testing.T, le bool) {
	t.Helper()
	var guid [8]byte
	for i := range guid {
		guid[i] = byte(i + 1)
	}
	checkServiceDoubleWrite(t, le, guid)
	checkServiceDoubleRead(t, le, guid)
}

// TestCdrServiceDoubleCall checks once only service header misuse.
func TestCdrServiceDoubleCall(t *testing.T) {
	for _, le := range []bool{true, false} {
		checkServiceDoubleEndian(t, le)
	}
}

// checkAlignServiceEncode verifies encode and pad for one endian.
func checkAlignServiceEncode(t *testing.T, le bool, guid [8]byte, seq uint64) []byte {
	t.Helper()
	enc := NewEncoder(le)
	err := enc.WriteServiceHeader(guid, seq)
	if err != nil {
		t.Fatalf("le=%v WriteServiceHeader failed: %v", le, err)
	}
	if enc.Len() != ServiceEnvelopeSize {
		t.Fatalf("le=%v service len %d want %d", le, enc.Len(), ServiceEnvelopeSize)
	}
	enc.Align(8)
	if enc.Len() != 24 {
		t.Fatalf("le=%v align 8 after service gives %d want 24", le, enc.Len())
	}
	enc.WriteUint64(0x1122334455667788)
	wire := enc.Bytes()
	if len(wire) != 32 {
		t.Fatalf("le=%v wire len %d want 32", le, len(wire))
	}
	if wire[20] != 0x00 || wire[21] != 0x00 || wire[22] != 0x00 || wire[23] != 0x00 {
		t.Fatalf("le=%v align pad not zero %v", le, wire[20:24])
	}
	return wire
}

// checkAlignServiceDecode verifies decode for one endian.
func checkAlignServiceDecode(t *testing.T, wire []byte, le bool, guid [8]byte, seq uint64) {
	t.Helper()
	dec, err := NewDecoder(wire)
	if err != nil {
		t.Fatalf("le=%v NewDecoder failed: %v", le, err)
	}
	gotGUID, gotSeq, err := dec.ReadServiceHeader()
	if err != nil {
		t.Fatalf("le=%v ReadServiceHeader failed: %v", le, err)
	}
	if gotGUID != guid || gotSeq != seq {
		t.Fatalf("le=%v service header mismatch", le)
	}
	err = dec.Align(8)
	if err != nil {
		t.Fatalf("le=%v Align 8 failed: %v", le, err)
	}
	if dec.Offset() != 24 {
		t.Fatalf("le=%v decode offset %d want 24", le, dec.Offset())
	}
	v, err := dec.ReadUint64()
	if err != nil {
		t.Fatalf("le=%v ReadUint64 failed: %v", le, err)
	}
	if v != 0x1122334455667788 {
		t.Fatalf("le=%v uint64 mismatch %x", le, v)
	}
	err = dec.Finish()
	if err != nil {
		t.Fatalf("le=%v Finish failed: %v", le, err)
	}
}

// checkAlignServiceEndian runs both checks for one endian.
func checkAlignServiceEndian(t *testing.T, le bool) {
	t.Helper()
	var guid [8]byte
	for i := range guid {
		guid[i] = byte(i + 1)
	}
	seq := uint64(0x0102030405060708)
	wire := checkAlignServiceEncode(t, le, guid, seq)
	checkAlignServiceDecode(t, wire, le, guid, seq)
}

// TestCdrAlignServicePrefix8 checks Align 8 after service prefix.
func TestCdrAlignServicePrefix8(t *testing.T) {
	for _, le := range []bool{true, false} {
		checkAlignServiceEndian(t, le)
	}
}

// TestCdrNeedNegative checks defensive negative read size.
func TestCdrNeedNegative(t *testing.T) {
	enc := NewEncoder(true)
	enc.WriteUint32(1)
	wire := enc.Bytes()
	dec, err := NewDecoder(wire)
	if err != nil {
		t.Fatalf("NewDecoder failed: %v", err)
	}
	dropsBefore := dec.Dropped()
	err = dec.need(-1)
	if err == nil {
		t.Fatalf("expected error on negative need")
	}
	if dec.Dropped() != dropsBefore+1 {
		t.Fatalf("negative need must count a drop")
	}
}
