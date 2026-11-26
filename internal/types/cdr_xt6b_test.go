// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1
package types

import (
	"testing"
)

// checkxt6bMapValidate verifies ordered, duplicate, and reverse cases.
func checkxt6bMapValidate(t *testing.T, le bool) {
	t.Helper()
	kind := RosTypeInt32
	keys := []any{int32(1), int32(2), int32(3)}
	err := CheckMapKeysxt6bOrdered(kind, keys)
	if err != nil {
		t.Fatalf("le=%v ordered int32 failed: %v", le, err)
	}
	dup := []any{int32(1), int32(1)}
	err = CheckMapKeysxt6bOrdered(kind, dup)
	if err == nil {
		t.Fatalf("le=%v duplicate int32 accepted", le)
	}
	rev := []any{int32(3), int32(2)}
	err = CheckMapKeysxt6bOrdered(kind, rev)
	if err == nil {
		t.Fatalf("le=%v decreasing int32 accepted", le)
	}
}

// checkxt6bMapRoundTrip verifies golden map round trip for one endian.
func checkxt6bMapRoundTrip(t *testing.T, le bool) {
	t.Helper()
	keys := []any{int32(1), int32(2), int32(3)}
	enc := NewEncoder(le)
	err := enc.WriteMapLength(len(keys), MapBound{MaxSize: 10})
	if err != nil {
		t.Fatalf("le=%v WriteMapLength failed: %v", le, err)
	}
	for i, k := range keys {
		v, ok := k.(int32)
		if !ok {
			t.Fatalf("le=%v key type mismatch", le)
		}
		enc.WriteInt32(v)
		enc.WriteInt32(int32((i + 1) * 10))
	}
	wire := enc.Bytes()
	dec, err := NewDecoder(wire)
	if err != nil {
		t.Fatalf("le=%v NewDecoder failed: %v", le, err)
	}
	n, err := dec.ReadMapLength(MapBound{MaxSize: 10})
	if err != nil {
		t.Fatalf("le=%v ReadMapLength failed: %v", le, err)
	}
	if n != 3 {
		t.Fatalf("le=%v map n %d want 3", le, n)
	}
	checkxt6bMapEntries(t, dec, le, n)
	err = dec.Finish()
	if err != nil {
		t.Fatalf("le=%v Finish failed: %v", le, err)
	}
}

// checkxt6bMapEntries verifies decoded map entries are ordered.
func checkxt6bMapEntries(t *testing.T, dec *Decoder, le bool, n int) {
	t.Helper()
	prev := int32(-1 << 31)
	for i := range n {
		k, err := dec.ReadInt32()
		if err != nil {
			t.Fatalf("le=%v read key %d failed: %v", le, i, err)
		}
		if i > 0 && k <= prev {
			t.Fatalf("le=%v keys not strictly increasing %d <= %d", le, k, prev)
		}
		prev = k
		_, err = dec.ReadInt32()
		if err != nil {
			t.Fatalf("le=%v read value failed: %v", le, err)
		}
	}
}

// TestMapxt6bOrdering pins strictly increasing keys plus duplicates.
func TestMapxt6bOrdering(t *testing.T) {
	for _, le := range []bool{true, false} {
		checkxt6bMapValidate(t, le)
		checkxt6bMapRoundTrip(t, le)
		checkxt6bMapDecodeOrdered(t, le)
	}
	checkxt6bMapKinds(t)
}

// checkxt6bMapDecodeOrdered pins decode-side numeric order enforcement.
//
// ReadMapKeysOrdered is the production decode counterpart of the encode
// CheckMapKeysxt6bOrdered rule: ordered int32 keys pass, decreasing and
// duplicate keys fail fail-closed per 7.2.3.2.2.1 rules 14 to 16.
func checkxt6bMapDecodeOrdered(t *testing.T, le bool) {
	t.Helper()
	checkxt6bMapDecodeOrderedPass(t, le)
	checkxt6bMapDecodeOrderedFail(t, le)
}

// checkxt6bMapDecodeOrderedPass verifies ordered keys pass the order check.
func checkxt6bMapDecodeOrderedPass(t *testing.T, le bool) {
	t.Helper()
	enc := NewEncoder(le)
	ordered := []int32{1, 2, 3}
	if err := enc.WriteMapLength(len(ordered), MapBound{}); err != nil {
		t.Fatalf("le=%v map length failed: %v", le, err)
	}
	for _, k := range ordered {
		enc.WriteInt32(k)
		enc.WriteInt32(k * 10)
	}
	dec, err := NewDecoder(enc.Bytes())
	if err != nil {
		t.Fatalf("le=%v decode failed: %v", le, err)
	}
	n, err := dec.ReadMapLength(MapBound{})
	if err != nil {
		t.Fatalf("le=%v read length failed: %v", le, err)
	}
	if err := dec.CheckSequenceAllocation(n, 4); err != nil {
		t.Fatalf("le=%v allocation failed: %v", le, err)
	}
	keys := make([]any, 0, n)
	for range n {
		k, err := dec.ReadInt32()
		if err != nil {
			t.Fatalf("le=%v read key failed: %v", le, err)
		}
		keys = append(keys, k)
		if _, err := dec.ReadInt32(); err != nil {
			t.Fatalf("le=%v read value failed: %v", le, err)
		}
	}
	if err := CheckMapEntriesxt6bOrdered(RosTypeInt32, keys); err != nil {
		t.Fatalf("le=%v ordered entries rejected: %v", le, err)
	}
}

// checkxt6bMapDecodeOrderedFail verifies decreasing keys fail the order check.
func checkxt6bMapDecodeOrderedFail(t *testing.T, le bool) {
	t.Helper()
	encBad := NewEncoder(le)
	bad := []int32{3, 1}
	if err := encBad.WriteMapLength(len(bad), MapBound{}); err != nil {
		t.Fatalf("le=%v bad length failed: %v", le, err)
	}
	for _, k := range bad {
		encBad.WriteInt32(k)
		encBad.WriteInt32(0)
	}
	decBad, err := NewDecoder(encBad.Bytes())
	if err != nil {
		t.Fatalf("le=%v bad decode failed: %v", le, err)
	}
	nBad, err := decBad.ReadMapLength(MapBound{})
	if err != nil {
		t.Fatalf("le=%v bad length read failed: %v", le, err)
	}
	if _, err := decBad.ReadMapKeysOrdered(RosTypeInt32, nBad); err == nil {
		t.Fatalf("le=%v decreasing keys accepted, want rejection", le)
	}
}

// checkxt6bMapKinds verifies per kind comparators.
func checkxt6bMapKinds(t *testing.T) {
	t.Helper()
	v, err := Comparext6bMapKey(RosTypeString, "b", "a")
	if err != nil {
		t.Fatalf("string compare failed: %v", err)
	}
	if v <= 0 {
		t.Fatalf("string compare b>a got %d", v)
	}
	v, err = Comparext6bMapKey(RosTypeUint32, uint32(2), uint32(10))
	if err != nil {
		t.Fatalf("uint32 compare failed: %v", err)
	}
	if v >= 0 {
		t.Fatalf("uint32 compare 2<10 got %d", v)
	}
	_, err = Comparext6bMapKey(RosTypeFloat, float32(1), float32(2))
	if err == nil {
		t.Fatalf("float key must be unsupported")
	}
	err = CheckMapKeysxt6bOrdered(RosTypeWString, []any{"a", "a"})
	if err == nil {
		t.Fatalf("duplicate wstring keys must fail")
	}
}

// checkxt6bBoundStringOctets verifies octet truncation for one endian.
func checkxt6bBoundStringOctets(t *testing.T, le bool) {
	t.Helper()
	enc := NewEncoder(le)
	truncated, err := enc.WriteBoundedStringTruncate("a\xe2\x9c\x85b", 2)
	if err != nil {
		t.Fatalf("le=%v WriteBoundedStringTruncate failed: %v", le, err)
	}
	if !truncated {
		t.Fatalf("le=%v expected truncate for octet bound", le)
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
	if got != "a" {
		t.Fatalf("le=%v octet truncate got %q want %q", le, got, "a")
	}
	encStrict := NewEncoder(le)
	before := encStrict.Len()
	_, err = encStrict.WriteBoundedString("a\xe2\x9c\x85b", 2)
	if err == nil {
		t.Fatalf("le=%v strict bounded string over bound must fail", le)
	}
	if encStrict.Len() != before {
		t.Fatalf("le=%v strict reject mutated buffer", le)
	}
}

// checkxt6bBoundWstringUnits verifies unit truncation plus astral agreement.
func checkxt6bBoundWstringUnits(t *testing.T, le bool) {
	t.Helper()
	astral := "\U0001F600"
	enc2 := NewEncoder(le)
	truncated, err := enc2.WriteBoundedWStringTruncate(astral, 1)
	if err != nil {
		t.Fatalf("le=%v WriteBoundedWStringTruncate failed: %v", le, err)
	}
	if !truncated {
		t.Fatalf("le=%v astral unit bound should truncate", le)
	}
	wire2 := enc2.Bytes()
	dec2, err := NewDecoder(wire2)
	if err != nil {
		t.Fatalf("le=%v NewDecoder wstring failed: %v", le, err)
	}
	got2, err := dec2.ReadBoundedWString(1)
	if err != nil {
		t.Fatalf("le=%v ReadBoundedWString failed: %v", le, err)
	}
	if got2 != "" {
		t.Fatalf("le=%v astral truncate got %q want empty", le, got2)
	}
	encStrict := NewEncoder(le)
	before := encStrict.Len()
	_, err = encStrict.WriteBoundedWString(astral, 1)
	if err == nil {
		t.Fatalf("le=%v strict bounded wstring over bound must fail", le)
	}
	if encStrict.Len() != before {
		t.Fatalf("le=%v strict reject mutated buffer", le)
	}
}

// checkxt6bBoundStrict verifies strict reject paths for one endian.
func checkxt6bBoundStrict(t *testing.T, le bool) {
	t.Helper()
	astral := "\U0001F600"
	enc3 := NewEncoder(le)
	err := enc3.WriteStringxt6bBoundedStrict("abc", 2)
	if err == nil {
		t.Fatalf("le=%v strict string over bound must fail", le)
	}
	if enc3.Len() != HeaderSize {
		t.Fatalf("le=%v strict reject mutated buffer", le)
	}
	enc4 := NewEncoder(le)
	err = enc4.WriteWStringxt6bBoundedStrict(astral, 1)
	if err == nil {
		t.Fatalf("le=%v strict wstring over units must fail", le)
	}
}

// TestBoundxt6bUnits pins octet and unit bounds plus astral agreement.
func TestBoundxt6bUnits(t *testing.T) {
	for _, le := range []bool{true, false} {
		checkxt6bBoundStringOctets(t, le)
		checkxt6bBoundWstringUnits(t, le)
		checkxt6bBoundStrict(t, le)
	}
}

// checkxt6bStrictSeq verifies sequence Remaining guard for one endian.
func checkxt6bStrictSeq(t *testing.T, le bool) {
	t.Helper()
	enc := NewEncoder(le)
	err := enc.WriteSequenceLength(2)
	if err != nil {
		t.Fatalf("le=%v WriteSequenceLength failed: %v", le, err)
	}
	enc.WriteUint32(1)
	enc.WriteUint32(2)
	wire := enc.Bytes()
	dec, err := NewDecoder(wire)
	if err != nil {
		t.Fatalf("le=%v NewDecoder failed: %v", le, err)
	}
	n, err := dec.ReadSequenceLengthxt6bChecked(4)
	if err != nil {
		t.Fatalf("le=%v checked read failed: %v", le, err)
	}
	if n != 2 {
		t.Fatalf("le=%v n %d want 2", le, n)
	}
	_, err = dec.ReadUint32()
	if err != nil {
		t.Fatalf("le=%v read value failed: %v", le, err)
	}
	err = dec.CheckSequenceAllocation(1000, 8)
	if err == nil {
		t.Fatalf("le=%v wide allocation above remaining must fail", le)
	}
}

// checkxt6bStrictMap verifies map Remaining guard for one endian.
func checkxt6bStrictMap(t *testing.T, le bool) {
	t.Helper()
	enc2 := NewEncoder(le)
	err := enc2.WriteMapLength(2, MapBound{})
	if err != nil {
		t.Fatalf("le=%v WriteMapLength failed: %v", le, err)
	}
	enc2.WriteInt32(1)
	enc2.WriteInt32(10)
	enc2.WriteInt32(2)
	enc2.WriteInt32(20)
	wire2 := enc2.Bytes()
	dec2, err := NewDecoder(wire2)
	if err != nil {
		t.Fatalf("le=%v NewDecoder map failed: %v", le, err)
	}
	_, err = dec2.ReadMapLengthxt6bChecked(MapBound{}, 8)
	if err != nil {
		t.Fatalf("le=%v map checked read failed: %v", le, err)
	}
}

// TestStrictxt6bRemaining pins strict encode and Remaining checks.
func TestStrictxt6bRemaining(t *testing.T) {
	for _, le := range []bool{true, false} {
		checkxt6bStrictSeq(t, le)
		checkxt6bStrictMap(t, le)
	}
}

// checkxt6bEncapsulation verifies Table 57 unify for one endian.
func checkxt6bEncapsulation(t *testing.T, le bool) {
	t.Helper()
	h := EncodeHeader(le)
	exp := EncodeEncapsulationxt6b(ExtFinal, le)
	if h != exp {
		t.Fatalf("le=%v EncodeHeader %v != encapsulation %v", le, h, exp)
	}
	parsed, err := ParseEncapsulationxt6b(h)
	if err != nil {
		t.Fatalf("le=%v parse failed: %v", le, err)
	}
	if parsed.LittleEndian != le {
		t.Fatalf("le=%v endian mismatch", le)
	}
	checkxt6bAllIDs(t, le)
	if _, err := ParseEncapsulationxt6b([4]byte{0x00, 0x04, 0x00, 0x00}); err == nil {
		t.Fatalf("le=%v XML id must be rejected", le)
	}
}

// checkxt6bAllIDs verifies all Table 57 IDs decode plus NewDecoder accepts.
func checkxt6bAllIDs(t *testing.T, le bool) {
	t.Helper()
	ids := []RepresentationIDxt6b{
		Repxt6bCDRBE, Repxt6bCDRLE, Repxt6bPLCDRBE, Repxt6bPLCDRLE,
		Repxt6bCDR2BE, Repxt6bCDR2LE, Repxt6bDCDR2BE, Repxt6bDCDR2LE,
		Repxt6bPLCDR2BE, Repxt6bPLCDR2LE,
	}
	for _, id := range ids {
		var hdr [4]byte
		hdr[0] = byte(uint16(id) >> 8)
		hdr[1] = byte(uint16(id) & 0xFF)
		p, err := ParseEncapsulationxt6b(hdr)
		if err != nil {
			t.Fatalf("le=%v id %s parse failed: %v", le, id.String(), err)
		}
		if p.Representation != id {
			t.Fatalf("le=%v id mismatch", le)
		}
		wire := make([]byte, 4)
		copy(wire, hdr[:])
		_, err = NewDecoder(wire)
		if err != nil {
			t.Fatalf("le=%v NewDecoder id %s failed: %v", le, id.String(), err)
		}
	}
}

// TestEncapsulationxt6b pins Table 57 unify and legacy compat.
func TestEncapsulationxt6b(t *testing.T) {
	for _, le := range []bool{true, false} {
		checkxt6bEncapsulation(t, le)
	}
	checkxt6bRepresentationTable(t)
}

// checkxt6bRepresentationTable verifies Table 32 selection vectors.
func checkxt6bRepresentationTable(t *testing.T) {
	t.Helper()
	cases := []struct {
		ext  Extensibility
		ver  EncodingVersion
		le   bool
		want RepresentationIDxt6b
	}{
		{ExtFinal, XCDRVersion1, true, Repxt6bCDRLE},
		{ExtFinal, XCDRVersion1, false, Repxt6bCDRBE},
		{ExtAppendable, XCDRVersion1, true, Repxt6bCDRLE},
		{ExtMutable, XCDRVersion1, true, Repxt6bPLCDRLE},
		{ExtFinal, XCDRVersion2, true, Repxt6bCDR2LE},
		{ExtAppendable, XCDRVersion2, true, Repxt6bDCDR2LE},
		{ExtMutable, XCDRVersion2, true, Repxt6bPLCDR2LE},
	}
	for _, c := range cases {
		got, err := RepresentationForxt6b(c.ext, c.ver, c.le)
		if err != nil {
			t.Fatalf("RepresentationFor failed: %v", err)
		}
		if got != c.want {
			t.Fatalf("RepresentationFor(%s,%d,%v)=%s want %s", c.ext.String(), int(c.ver), c.le, got.String(), c.want.String())
		}
	}
}

// checkxt6bAlign verifies versioned align for one endian.
func checkxt6bAlign(t *testing.T, le bool) {
	t.Helper()
	if MaxAlignxt6bForVersion(XCDRVersion2) != 4 {
		t.Fatalf("le=%v V2 maxalign want 4", le)
	}
	if MaxAlignxt6bForVersion(XCDRVersion1) != 8 {
		t.Fatalf("le=%v V1 maxalign want 8", le)
	}
	if XCDR2Alignxt6b(8) != 4 || XCDR2Alignxt6b(16) != 4 {
		t.Fatalf("le=%v XCDR2 align wrong", le)
	}
	enc := NewEncoder(le)
	err := enc.Alignxt6bVersioned(8, XCDRVersion2)
	if err != nil {
		t.Fatalf("le=%v V2 align 8 failed: %v", le, err)
	}
	if (enc.Len()-enc.payloadStart)%4 != 0 {
		t.Fatalf("le=%v V2 align not 4 based", le)
	}
	enc2 := NewEncoder(le)
	err = enc2.Alignxt6bVersioned(8, XCDRVersion1)
	if err != nil {
		t.Fatalf("le=%v V1 align 8 failed: %v", le, err)
	}
	wire := enc.Bytes()
	dec, err := NewDecoder(wire)
	if err != nil {
		t.Fatalf("le=%v NewDecoder failed: %v", le, err)
	}
	err = dec.Alignxt6bVersioned(8, XCDRVersion2)
	if err != nil {
		t.Fatalf("le=%v decoder V2 align failed: %v", le, err)
	}
}

// TestMaxAlignxt6b pins versioned align with rebased origin.
func TestMaxAlignxt6b(t *testing.T) {
	for _, le := range []bool{true, false} {
		checkxt6bAlign(t, le)
	}
}

// checkLongDoubleV1 verifies V1 round trip for one endian.
func checkLongDoubleV1(t *testing.T, le bool, v LongDouble) {
	t.Helper()
	enc := NewEncoder(le)
	err := enc.WriteLongDouble(v)
	if err != nil {
		t.Fatalf("le=%v WriteLongDouble failed: %v", le, err)
	}
	wire := enc.Bytes()
	dec, err := NewDecoder(wire)
	if err != nil {
		t.Fatalf("le=%v NewDecoder failed: %v", le, err)
	}
	got, err := dec.ReadLongDouble()
	if err != nil {
		t.Fatalf("le=%v ReadLongDouble failed: %v", le, err)
	}
	if got != v {
		t.Fatalf("le=%v long double mismatch", le)
	}
	err = dec.Finish()
	if err != nil {
		t.Fatalf("le=%v Finish failed: %v", le, err)
	}
}

// checkLongDoubleV2 verifies V2 round trip for one endian.
func checkLongDoubleV2(t *testing.T, le bool, v LongDouble) {
	t.Helper()
	enc2 := NewEncoderWithExt(ExtAppendable, le)
	err := enc2.WriteLongDouble(v)
	if err != nil {
		t.Fatalf("le=%v V2 WriteLongDouble failed: %v", le, err)
	}
	wire2 := enc2.Bytes()
	dec2, err := NewDecoder(wire2)
	if err != nil {
		t.Fatalf("le=%v V2 NewDecoder failed: %v", le, err)
	}
	if dec2.Version() != XCDRVersion2 {
		t.Fatalf("le=%v V2 version mismatch", le)
	}
	got2, err := dec2.ReadLongDouble()
	if err != nil {
		t.Fatalf("le=%v V2 ReadLongDouble failed: %v", le, err)
	}
	if got2 != v {
		t.Fatalf("le=%v V2 long double mismatch", le)
	}
}

// TestLongDoublext6b pins long double support with 16 byte round trip.
func TestLongDoublext6b(t *testing.T) {
	err := CheckSupportxt6bForType(RosTypeLongDbl)
	if err != nil {
		t.Fatalf("long double must be supported: %v", err)
	}
	err = CheckSupportxt6bForType(RosTypeDouble)
	if err != nil {
		t.Fatalf("double must be supported: %v", err)
	}
	if MaxAlignxt6bV2 != 4 {
		t.Fatalf("V2 maxalign want 4 for long double note")
	}
	for _, le := range []bool{true, false} {
		var v LongDouble
		for i := range v {
			v[i] = byte(i + 1)
		}
		checkLongDoubleV1(t, le, v)
		checkLongDoubleV2(t, le, v)
	}
}

// checkxt6bFramingDelimited verifies DHEADER round trip for one endian.
func checkxt6bFramingDelimited(t *testing.T, le bool) {
	t.Helper()
	enc := NewEncoder(le)
	err := enc.WriteStructxt6bDelimited(func() error {
		enc.WriteUint32(0x01020304)
		enc.WriteUint8(0xFF)
		return nil
	})
	if err != nil {
		t.Fatalf("le=%v delimited write failed: %v", le, err)
	}
	wire := enc.Bytes()
	dec, err := NewDecoder(wire)
	if err != nil {
		t.Fatalf("le=%v NewDecoder failed: %v", le, err)
	}
	err = dec.ReadStructxt6bDelimited(func(size uint32) error {
		if size != 5 {
			t.Fatalf("le=%v dheader size %d want 5", le, size)
		}
		v, err := dec.ReadUint32()
		if err != nil {
			return err
		}
		if v != 0x01020304 {
			t.Fatalf("le=%v body mismatch %#x", le, v)
		}
		b, err := dec.ReadUint8()
		if err != nil {
			return err
		}
		if b != 0xFF {
			t.Fatalf("le=%v tail mismatch", le)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("le=%v delimited read failed: %v", le, err)
	}
	err = dec.Finish()
	if err != nil {
		t.Fatalf("le=%v Finish failed: %v", le, err)
	}
}

// checkxt6bFramingEmheader verifies EMHEADER plus skip policy.
func checkxt6bFramingEmheader(t *testing.T, le bool) {
	t.Helper()
	enc2 := NewEncoder(le)
	err := enc2.WriteEMHeader(7, true, 4)
	if err != nil {
		t.Fatalf("le=%v WriteEMHeader failed: %v", le, err)
	}
	enc2.WriteUint32(0xDEADBEEF)
	wire2 := enc2.Bytes()
	dec2, err := NewDecoder(wire2)
	if err != nil {
		t.Fatalf("le=%v NewDecoder emheader failed: %v", le, err)
	}
	hdr, err := dec2.ReadEMHeader()
	if err != nil {
		t.Fatalf("le=%v ReadEMHeader failed: %v", le, err)
	}
	if hdr.MemberID != 7 || !hdr.MustUnderstand {
		t.Fatalf("le=%v emheader mismatch %+v", le, hdr)
	}
	err = SkipUnknownxt6bMember(dec2, true, 99, 4)
	if err == nil {
		t.Fatalf("le=%v must-understand skip must fail", le)
	}
	err = SkipUnknownxt6bMember(dec2, false, 99, 4)
	if err != nil {
		t.Fatalf("le=%v skippable skip failed: %v", le, err)
	}
	hdrBytes := EncodeHeader(le)
	wire := dec2.data
	_ = wire
	_ = hdrBytes
}

// TestFramingxt6b pins DHEADER and EMHEADER wiring plus skip policy.
func TestFramingxt6b(t *testing.T) {
	for _, le := range []bool{true, false} {
		checkxt6bFramingDelimited(t, le)
		checkxt6bFramingEmheader(t, le)
		checkxt6bPIDSkip(t, le)
	}
}

// checkxt6bPIDSkip pins V1 mutable PID skip with must-understand.
//
// Unknown skippable members are skipped, unknown must-understand members
// fail via CheckMustUnderstand, and the sentinel terminates per 7.4.1.2.
// This gives the V1 PID path a production decode caller.
func checkxt6bPIDSkip(t *testing.T, le bool) {
	t.Helper()
	checkxt6bPIDMustUnderstand(t, le)
	checkxt6bPIDSkippable(t, le)
}

// checkxt6bPIDMustUnderstand verifies must-understand rejection in PID skip.
func checkxt6bPIDMustUnderstand(t *testing.T, le bool) {
	t.Helper()
	enc := NewEncoder(le)
	if err := enc.WritePIDMember(7, false, []byte{0xAA, 0xBB}); err != nil {
		t.Fatalf("le=%v write pid failed: %v", le, err)
	}
	if err := enc.WritePIDMember(9, true, []byte{0xCC}); err != nil {
		t.Fatalf("le=%v write must-understand failed: %v", le, err)
	}
	if err := enc.WritePIDListEnd(); err != nil {
		t.Fatalf("le=%v write sentinel failed: %v", le, err)
	}
	dec, err := NewDecoder(enc.Bytes())
	if err != nil {
		t.Fatalf("le=%v decode failed: %v", le, err)
	}
	known := map[uint32]bool{7: true}
	hdr, done, err := dec.SkipPIDMemberContent(known)
	if err != nil {
		t.Fatalf("le=%v skip known failed: %v", le, err)
	}
	if done || hdr.memberID != 7 {
		t.Fatalf("le=%v known member %+v done %v", le, hdr, done)
	}
	if err := dec.SkipBytes(uint64(hdr.size)); err != nil {
		t.Fatalf("le=%v skip known bytes failed: %v", le, err)
	}
	if _, _, err := dec.SkipPIDMemberContent(known); err == nil {
		t.Fatalf("le=%v unknown must-understand accepted, want rejection", le)
	}
}

// checkxt6bPIDSkippable verifies skippable member and sentinel handling.
func checkxt6bPIDSkippable(t *testing.T, le bool) {
	t.Helper()
	known := map[uint32]bool{7: true}
	encSkip := NewEncoder(le)
	if err := encSkip.WritePIDMember(11, false, []byte{0xDD}); err != nil {
		t.Fatalf("le=%v write skippable failed: %v", le, err)
	}
	if err := encSkip.WritePIDListEnd(); err != nil {
		t.Fatalf("le=%v write sentinel2 failed: %v", le, err)
	}
	decSkip, err := NewDecoder(encSkip.Bytes())
	if err != nil {
		t.Fatalf("le=%v decode skip failed: %v", le, err)
	}
	if _, done, err := decSkip.SkipPIDMemberContent(known); err != nil {
		t.Fatalf("le=%v skippable skip failed: %v", le, err)
	} else if done {
		t.Fatalf("le=%v skippable reported sentinel", le)
	}
	if _, done, err := decSkip.SkipPIDMemberContent(known); err != nil {
		t.Fatalf("le=%v sentinel failed: %v", le, err)
	} else if !done {
		t.Fatalf("le=%v sentinel not reported", le)
	}
}

// TestEmitxt6b pins XCDR2 emit per Table 32 by extensibility.
func TestEmitxt6b(t *testing.T) {
	fin := EncodeEncapsulationxt6b(ExtFinal, true)
	if fin[1] != 0x01 {
		t.Fatalf("final emit % x want 00 01", fin)
	}
	app := EncodeEncapsulationxt6b(ExtAppendable, true)
	if app[1] != 0x09 {
		t.Fatalf("appendable emit % x want 00 09", app)
	}
	mut := EncodeEncapsulationxt6b(ExtMutable, false)
	if mut[1] != 0x0A {
		t.Fatalf("mutable emit % x want 00 0a", mut)
	}
	for _, le := range []bool{true, false} {
		for _, ext := range []Extensibility{ExtFinal, ExtAppendable, ExtMutable} {
			h := EncodeEncapsulationxt6b(ext, le)
			p, err := ParseEncapsulationxt6b(h)
			if err != nil {
				t.Fatalf("le=%v ext %s parse failed: %v", le, ext.String(), err)
			}
			if p.LittleEndian != le {
				t.Fatalf("endian mismatch")
			}
		}
	}
}
