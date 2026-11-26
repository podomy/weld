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
	}
	checkHolderMissing(t)
}
