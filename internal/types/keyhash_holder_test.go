// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1
package types

import (
	"bytes"
	"testing"
)

// keyHolderFixture builds two key fields in declaration order.
func keyHolderFixture() []KeyField {
	return []KeyField{
		{Name: "seq", Kind: RosTypeUint32, Value: uint32(7)},
		{Name: "label", Kind: RosTypeString, Value: "hi"},
	}
}

// checkHolderSteps verifies holder CDR plus hash steps.
func checkHolderSteps(t *testing.T, le bool) {
	t.Helper()
	fields := keyHolderFixture()
	raw, err := SerializeKeyHolder(fields, le)
	if err != nil {
		t.Fatalf("le=%v serialize failed: %v", le, err)
	}
	if len(raw) == 0 {
		t.Fatalf("le=%v empty holder", le)
	}
	chunks, err := EncodeKeyFieldChunks(fields, le)
	if err != nil {
		t.Fatalf("le=%v chunks failed: %v", le, err)
	}
	projected, err := Projectxt6aKeyHolder(chunks, []string{"seq", "label"})
	if err != nil {
		t.Fatalf("le=%v project failed: %v", le, err)
	}
	if !bytes.Equal(raw, projected) {
		t.Fatalf("le=%v holder versus project mismatch", le)
	}
	short := ComputeKeyHash(raw)
	// Short holders copy with zero pad; verify prefix plus pad.
	if !bytes.Equal(short[:len(raw)], raw) {
		t.Fatalf("le=%v short hash prefix mismatch", le)
	}
	for _, b := range short[len(raw):] {
		if b != 0 {
			t.Fatalf("le=%v short hash pad not zero", le)
		}
	}
	longRaw := bytes.Repeat([]byte{0xAB}, KeyHashSize+1)
	longHash := ComputeKeyHash(longRaw)
	md5Hash := KeyHashMD5(longRaw)
	if longHash != md5Hash {
		t.Fatalf("le=%v long key must be MD5", le)
	}
}

// checkHolderMissing verifies missing paths fail loudly.
func checkHolderMissing(t *testing.T) {
	t.Helper()
	_, err := SerializeKeyHolder(nil, true)
	if err == nil {
		t.Fatalf("empty holder must fail")
	}
	_, err = Projectxt6aKeyHolder(map[string][]byte{}, []string{})
	if err == nil {
		t.Fatalf("empty paths must fail")
	}
	// Float, double, long-double, and nested message keys are supported
	// per 7.6.8.2; they must serialize, not fail.
	for _, f := range []KeyField{
		{Name: "x", Kind: RosTypeFloat, Value: float32(1)},
		{Name: "x", Kind: RosTypeDouble, Value: float64(1)},
		{Name: "x", Kind: RosTypeLongDbl, Value: LongDouble{1}},
		{Name: "x", Kind: RosTypeMessage, Value: []KeyField{{Name: "a", Kind: RosTypeUint32, Value: uint32(1)}}},
	} {
		_, err = SerializeKeyHolder([]KeyField{f}, true)
		if err != nil {
			t.Fatalf("key kind %s must serialize: %v", f.Kind.String(), err)
		}
	}
	_, err = SerializeKeyHolder([]KeyField{{Name: "x", Kind: RosTypeID(99), Value: uint32(1)}}, true)
	if err == nil {
		t.Fatalf("unknown key kind must fail")
	}
}

// TestKeyHolderSerialization pins holder CDR plus hash steps.
func TestKeyHolderSerialization(t *testing.T) {
	for _, le := range []bool{true, false} {
		checkHolderSteps(t, le)
		checkHolderInterFieldPad(t, le)
	}
	checkHolderMissing(t)
	checkKeyUint64Negative(t)
}

// checkHolderInterFieldPad pins single-encoder alignment for nested and long-double.
func checkHolderInterFieldPad(t *testing.T, le bool) {
	t.Helper()
	fields := []KeyField{
		{Name: "a", Kind: RosTypeUint8, Value: uint8(1)},
		{Name: "b", Kind: RosTypeUint32, Value: uint32(0x01020304)},
		{Name: "c", Kind: RosTypeDouble, Value: float64(1)},
		{Name: "d", Kind: RosTypeLongDbl, Value: LongDouble{1, 2, 3}},
		{Name: "e", Kind: RosTypeMessage, Value: []KeyField{
			{Name: "x", Kind: RosTypeUint8, Value: uint8(2)},
			{Name: "y", Kind: RosTypeUint64, Value: uint64(9)},
		}},
	}
	raw, err := SerializeKeyHolder(fields, le)
	if err != nil {
		t.Fatalf("le=%v serialize pad fixture failed: %v", le, err)
	}
	chunks, err := EncodeKeyFieldChunks(fields, le)
	if err != nil {
		t.Fatalf("le=%v chunks failed: %v", le, err)
	}
	projected, err := Projectxt6aKeyHolder(chunks, []string{"a", "b", "c", "d", "e"})
	if err != nil {
		t.Fatalf("le=%v project failed: %v", le, err)
	}
	if len(raw) == 0 || len(projected) != len(raw) {
		t.Fatalf("le=%v holder versus project len %d vs %d", le, len(projected), len(raw))
	}
	for i := range raw {
		if raw[i] != projected[i] {
			t.Fatalf("le=%v holder byte %d %x vs %x", le, i, raw[i], projected[i])
		}
	}
}

// checkKeyUint64Negative pins unsigned reject plus signed sign-extension.
func checkKeyUint64Negative(t *testing.T) {
	t.Helper()
	if _, ok := toUint64(int(-1)); ok {
		t.Fatal("toUint64 negative int accepted, want rejection")
	}
	if _, ok := toUint64(int32(-1)); ok {
		t.Fatal("toUint64 negative int32 accepted, want rejection")
	}
	if _, ok := toUint64(int64(-1)); ok {
		t.Fatal("toUint64 negative int64 accepted, want rejection")
	}
	v, ok := toInt64(int32(-1))
	if !ok || v != -1 {
		t.Fatalf("toInt64 int32 -1 got %d %v, want -1 true", v, ok)
	}
	if _, ok := toInt64(uint64(0x7FFFFFFFFFFFFFFF)); !ok {
		t.Fatal("toInt64 max int64 rejected")
	}
	if _, ok := toInt64(uint64(0x8000000000000000)); ok {
		t.Fatal("toInt64 overflow uint64 accepted, want rejection")
	}
	if _, ok := toInt64(uint64(0xFFFFFFFFFFFFFFFF)); ok {
		t.Fatal("toInt64 max uint64 accepted, want rejection")
	}
	_, err := SerializeKeyHolder([]KeyField{{Name: "u", Kind: RosTypeUint64, Value: int(-1)}}, true)
	if err == nil {
		t.Fatal("unsigned key negative accepted, want rejection")
	}
	raw, err := SerializeKeyHolder([]KeyField{{Name: "s", Kind: RosTypeInt64, Value: int32(-1)}}, true)
	if err != nil {
		t.Fatalf("signed key -1 rejected: %v", err)
	}
	if len(raw) != 8 {
		t.Fatalf("signed key len %d want 8", len(raw))
	}
}
