// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1
package types

import (
	"bytes"
	"testing"
)

// checkV2Header verifies V2 IDs for appendable plus mutable.
func checkV2Header(t *testing.T, le bool) {
	t.Helper()
	app := EncodeHeaderForExt(ExtAppendable, le)
	parsed, err := ParseEncapsulationxt6b(app)
	if err != nil {
		t.Fatalf("le=%v parse appendable failed: %v", le, err)
	}
	if parsed.Version != XCDRVersion2 || parsed.Extensibility != ExtAppendable {
		t.Fatalf("le=%v appendable %+v", le, parsed)
	}
	mut := EncodeHeaderForExt(ExtMutable, le)
	parsedMut, err := ParseEncapsulationxt6b(mut)
	if err != nil {
		t.Fatalf("le=%v parse mutable failed: %v", le, err)
	}
	if parsedMut.Version != XCDRVersion2 {
		t.Fatalf("le=%v mutable version %d", le, int(parsedMut.Version))
	}
	fin := EncodeHeaderForExt(ExtFinal, le)
	if fin != EncodeHeader(le) {
		t.Fatalf("le=%v final header mismatch", le)
	}
	enc := NewEncoderWithExt(ExtAppendable, le)
	if enc.Version() != XCDRVersion2 || enc.Extensibility() != ExtAppendable {
		t.Fatalf("le=%v encoder version %+v", le, enc)
	}
	wire := enc.Bytes()
	dec, err := NewDecoder(wire)
	if err != nil {
		t.Fatalf("le=%v decode V2 failed: %v", le, err)
	}
	if dec.Version() != XCDRVersion2 {
		t.Fatalf("le=%v decoder version %d", le, int(dec.Version()))
	}
}

// checkV2Message verifies MarshalMessageWithExt V2 bytes.
func checkV2Message(t *testing.T, le bool) {
	t.Helper()
	payload := []byte{0x01, 0x02}
	v1 := MarshalMessage(payload, le)
	v2 := MarshalMessageWithExt(payload, ExtAppendable, le)
	if bytes.Equal(v1, v2) {
		t.Fatalf("le=%v V2 message equals V1", le)
	}
	var m Metrics
	_, gotLE, err := UnmarshalMessage(v2, &m)
	if err != nil {
		t.Fatalf("le=%v unmarshal V2 failed: %v", le, err)
	}
	if gotLE != le {
		t.Fatalf("le=%v endian mismatch", le)
	}
	guid := [8]byte{1, 2, 3, 4, 5, 6, 7, 8}
	svcV2 := MarshalServiceMessageWithExt(payload, guid, 9, ExtMutable, le)
	svcV1 := MarshalServiceMessage(payload, guid, 9, le)
	if bytes.Equal(svcV1, svcV2) {
		t.Fatalf("le=%v V2 service equals V1", le)
	}
}

// checkV2Align verifies 8 byte values cap at 4 under V2.
func checkV2Align(t *testing.T, le bool) {
	t.Helper()
	enc := NewEncoderWithExt(ExtAppendable, le)
	enc.WriteUint8(0xAA)
	enc.WriteUint64(0x0102030405060708)
	wire := enc.Bytes()
	dec, err := NewDecoder(wire)
	if err != nil {
		t.Fatalf("le=%v decode failed: %v", le, err)
	}
	_, err = dec.ReadUint8()
	if err != nil {
		t.Fatalf("le=%v read u8 failed: %v", le, err)
	}
	v, err := dec.ReadUint64()
	if err != nil {
		t.Fatalf("le=%v read u64 failed: %v", le, err)
	}
	if v != 0x0102030405060708 {
		t.Fatalf("le=%v u64 %#x", le, v)
	}
	err = dec.Finish()
	if err != nil {
		t.Fatalf("le=%v finish failed: %v", le, err)
	}
}

// TestXCDR2Emit pins V2 headers plus versioned alignment.
func TestXCDR2Emit(t *testing.T) {
	for _, le := range []bool{true, false} {
		checkV2Header(t, le)
		checkV2Message(t, le)
		checkV2Align(t, le)
	}
}

// checkFixedDecimal verifies decimal round trip plus reject.
//
// "-12.34" carries digits=4 scale=2 per IDL fixed<4,2> and encodes as pure
// BCD 0x01 0x23 0x4D per DDS-XTypes 7.2.3.4 with no count prefix. The
// payload after the 4 byte header is exactly 3 octets; any count prefix
// would add 8 bytes and fail this pin.
func checkFixedDecimal(t *testing.T, le bool) {
	t.Helper()
	enc := NewEncoder(le)
	err := enc.WriteFixedDecimal("-12.34")
	if err != nil {
		t.Fatalf("le=%v write fixed failed: %v", le, err)
	}
	wire := enc.Bytes()
	// Header 4 plus BCD 3 is 7, padded to 8. The old prefix form was
	// header 4 plus counts 8 plus BCD 3 padded to 16, so 8 pins pure BCD.
	if len(wire) != HeaderSize+4 {
		t.Fatalf("le=%v fixed wire len %d, want %d for pure BCD", le, len(wire), HeaderSize+4)
	}
	dec, err := NewDecoder(wire)
	if err != nil {
		t.Fatalf("le=%v decode failed: %v", le, err)
	}
	got, err := dec.ReadFixedDecimal(4, 2)
	if err != nil {
		t.Fatalf("le=%v read fixed failed: %v", le, err)
	}
	if got != "-12.34" {
		t.Fatalf("le=%v fixed %q", le, got)
	}
	err = dec.Finish()
	if err != nil {
		t.Fatalf("le=%v fixed finish failed: %v", le, err)
	}
	enc2 := NewEncoder(le)
	err = enc2.WriteFixedDecimal("12..3")
	if err == nil {
		t.Fatalf("le=%v bad fixed must fail", le)
	}
	decBad, err := NewDecoder(wire)
	if err != nil {
		t.Fatalf("le=%v decode bad failed: %v", le, err)
	}
	_, err = decBad.ReadFixedDecimal(4, 5)
	if err == nil {
		t.Fatalf("le=%v scale above digits must fail", le)
	}
	_, err = decBad.ReadFixedDecimal(0, 0)
	if err == nil {
		t.Fatalf("le=%v zero digits must fail", le)
	}
}

// checkEnumRoundTrip verifies enum widths.
func checkEnumRoundTrip(t *testing.T, le bool, enc *Encoder) {
	t.Helper()
	err := enc.WriteEnum(8, 5)
	if err != nil {
		t.Fatalf("le=%v write enum failed: %v", le, err)
	}
	err = enc.WriteEnum(16, 300)
	if err != nil {
		t.Fatalf("le=%v write enum16 failed: %v", le, err)
	}
}

// checkBitmaskRoundTrip verifies bitmask widths.
func checkBitmaskRoundTrip(t *testing.T, le bool, enc *Encoder) {
	t.Helper()
	err := enc.WriteBitmask(8, 0xAB)
	if err != nil {
		t.Fatalf("le=%v write bitmask failed: %v", le, err)
	}
	err = enc.WriteBitmask(64, 0x0102030405060708)
	if err != nil {
		t.Fatalf("le=%v write bitmask64 failed: %v", le, err)
	}
}

// checkEnumBitmaskRead verifies enum plus bitmask reads.
func checkEnumBitmaskRead(t *testing.T, le bool, dec *Decoder) {
	t.Helper()
	v1, err := dec.ReadEnum(8)
	if err != nil {
		t.Fatalf("le=%v read enum failed: %v", le, err)
	}
	if v1 != 5 {
		t.Fatalf("le=%v enum %d", le, v1)
	}
	v2, err := dec.ReadEnum(16)
	if err != nil {
		t.Fatalf("le=%v read enum16 failed: %v", le, err)
	}
	if v2 != 300 {
		t.Fatalf("le=%v enum16 %d", le, v2)
	}
	b1, err := dec.ReadBitmask(8)
	if err != nil {
		t.Fatalf("le=%v read bitmask failed: %v", le, err)
	}
	if b1 != 0xAB {
		t.Fatalf("le=%v bitmask %#x", le, b1)
	}
	b8, err := dec.ReadBitmask(64)
	if err != nil {
		t.Fatalf("le=%v read bitmask64 failed: %v", le, err)
	}
	if b8 != 0x0102030405060708 {
		t.Fatalf("le=%v bitmask64 %#x", le, b8)
	}
}

// checkEnumBitmask verifies enum plus bitmask widths.
func checkEnumBitmask(t *testing.T, le bool) {
	t.Helper()
	enc := NewEncoder(le)
	checkEnumRoundTrip(t, le, enc)
	checkBitmaskRoundTrip(t, le, enc)
	wire := enc.Bytes()
	dec, err := NewDecoder(wire)
	if err != nil {
		t.Fatalf("le=%v decode failed: %v", le, err)
	}
	checkEnumBitmaskRead(t, le, dec)
}

// checkMapOrder verifies deterministic sorting plus order checks.
func checkMapOrder(t *testing.T) {
	t.Helper()
	keys := []any{int32(3), int32(1), int32(2)}
	sorted, err := SortMapKeysxt6b(RosTypeInt32, keys)
	if err != nil {
		t.Fatalf("sort failed: %v", err)
	}
	if len(sorted) != 3 {
		t.Fatalf("sorted len %d", len(sorted))
	}
	first, ok := sorted[0].(int32)
	if !ok {
		t.Fatalf("sorted type %T", sorted[0])
	}
	if first != 1 {
		t.Fatalf("sorted %+v", sorted)
	}
	err = CheckMapKeysxt6bOrdered(RosTypeInt32, sorted)
	if err != nil {
		t.Fatalf("ordered check failed: %v", err)
	}
	err = CheckMapKeysxt6bOrdered(RosTypeInt32, keys)
	if err == nil {
		t.Fatalf("unordered must fail")
	}
	dup := []any{int32(1), int32(1)}
	_, err = SortMapKeysxt6b(RosTypeInt32, dup)
	if err == nil {
		t.Fatalf("duplicate sort must fail")
	}
	strKeys := []any{"b", "a"}
	sortedStr, err := SortMapKeysxt6b(RosTypeString, strKeys)
	if err != nil {
		t.Fatalf("string sort failed: %v", err)
	}
	firstStr, ok := sortedStr[0].(string)
	if !ok {
		t.Fatalf("string sorted type %T", sortedStr[0])
	}
	if firstStr != "a" {
		t.Fatalf("string sorted %+v", sortedStr)
	}
}

// TestXTypesValues pins fixed plus enum plus bitmask plus map order.
func TestXTypesValues(t *testing.T) {
	for _, le := range []bool{true, false} {
		checkFixedDecimal(t, le)
		checkEnumBitmask(t, le)
	}
	checkMapOrder(t)
}

// TestEnumRangeFailClosed pins signed-only enum ranges per Table 30.
//
// ReadEnum uses signed reads, so unsigned-only values must fail at encode
// instead of aliasing on decode: 200 does not fit int8 and 40000 does not
// fit int16.
func TestEnumRangeFailClosed(t *testing.T) {
	for _, le := range []bool{true, false} {
		enc := NewEncoder(le)
		if err := enc.WriteEnum(8, 200); err == nil {
			t.Fatalf("le=%v enum8 200 accepted, want rejection", le)
		}
		if err := enc.WriteEnum(16, 40000); err == nil {
			t.Fatalf("le=%v enum16 40000 accepted, want rejection", le)
		}
		if err := enc.WriteEnum(8, 127); err != nil {
			t.Fatalf("le=%v enum8 127 rejected: %v", le, err)
		}
		if err := enc.WriteEnum(16, -32768); err != nil {
			t.Fatalf("le=%v enum16 min rejected: %v", le, err)
		}
	}
}
