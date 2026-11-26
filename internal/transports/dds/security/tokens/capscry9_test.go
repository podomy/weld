// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

package tokens

import (
	"bytes"
	"errors"
	"strings"
	"testing"
)

// TestCapscry9StringUnification pins 4096 string caps on both codecs.
func TestCapscry9StringUnification(t *testing.T) {
	t.Parallel()

	if maxStringLen != 4096 || maxSpecString != 4096 {
		t.Fatalf("string caps must unify at 4096, got %d and %d", maxStringLen, maxSpecString)
	}

	atCap := strings.Repeat("a", 4096)

	encoded, err := encodeString(nil, atCap)
	if err != nil {
		t.Fatalf("codec string at cap must encode: %v", err)
	}

	decoded, _, err := decodeString(encoded, 0)
	if err != nil {
		t.Fatalf("codec string at cap must decode: %v", err)
	}

	if decoded != atCap {
		t.Fatalf("codec string roundtrip mismatch")
	}

	aboveCap := strings.Repeat("b", 4097)

	if _, err := encodeString(nil, aboveCap); !errors.Is(err, ErrTokenTooLarge) {
		t.Fatalf("codec string above cap must fail closed, got %v", err)
	}

	var w specWriter

	err = w.str(atCap)
	if err != nil {
		t.Fatalf("spec string at cap must encode: %v", err)
	}

	r := &specReader{raw: w.out}

	back, err := r.str()
	if err != nil {
		t.Fatalf("spec string at cap must decode: %v", err)
	}

	if back != atCap {
		t.Fatalf("spec string roundtrip mismatch")
	}

	var w2 specWriter

	if err := w2.str(aboveCap); !errors.Is(err, ErrTokenTooLarge) {
		t.Fatalf("spec string above cap must fail closed, got %v", err)
	}
}

// TestCapscry9HolderUnification pins 8 holder caps on both codecs.
func TestCapscry9HolderUnification(t *testing.T) {
	t.Parallel()

	if maxTokens != 8 || maxSpecHolders != 8 {
		t.Fatalf("holder caps must unify at 8, got %d and %d", maxTokens, maxSpecHolders)
	}

	checkTokencry9SeqCap(t)
	checkGenericcry9HoldersCap(t)
}

// checkTokencry9SeqCap pins the compact token sequence cap.
func checkTokencry9SeqCap(t *testing.T) {
	t.Helper()

	material := KeyMaterial{
		TransformationKind: 4,
		MasterSalt:         bytes.Repeat([]byte{0x11}, 32),
		SenderKeyID:        1,
		MasterSenderKey:    bytes.Repeat([]byte{0x12}, 32),
	}

	atCap := make([]KeyMaterial, 8)
	for i := range atCap {
		atCap[i] = material
	}

	encoded, err := EncodeCryptoTokenSeq(atCap)
	if err != nil {
		t.Fatalf("token seq at cap must encode: %v", err)
	}

	back, err := DecodeCryptoTokenSeq(encoded)
	if err != nil {
		t.Fatalf("token seq at cap must decode: %v", err)
	}

	if len(back) != 8 {
		t.Fatalf("token seq at cap %d, want 8", len(back))
	}

	aboveCap := make([]KeyMaterial, 9)
	for i := range aboveCap {
		aboveCap[i] = material
	}

	if _, err := EncodeCryptoTokenSeq(aboveCap); !errors.Is(err, ErrTokenTooLarge) {
		t.Fatalf("token seq above cap must fail closed, got %v", err)
	}
}

// checkGenericcry9HoldersCap pins the spec generic holder cap.
func checkGenericcry9HoldersCap(t *testing.T) {
	t.Helper()

	holders := make([]DataHolder, 8)
	for i := range holders {
		holders[i] = DataHolder{ClassID: TokenCrypto}
	}

	msg := GenericMessage{ClassID: ClassAuthMsg, Data: holders}

	framed, err := EncodeGenericPayload(msg)
	if err != nil {
		t.Fatalf("generic holders at cap must encode: %v", err)
	}

	parsed, err := DecodeGenericPayload(framed)
	if err != nil {
		t.Fatalf("generic holders at cap must decode: %v", err)
	}

	if len(parsed.Data) != 8 {
		t.Fatalf("generic holders at cap %d, want 8", len(parsed.Data))
	}

	big := GenericMessage{ClassID: ClassAuthMsg, Data: make([]DataHolder, 9)}
	for i := range big.Data {
		big.Data[i] = DataHolder{ClassID: TokenCrypto}
	}

	if _, err := EncodeGenericPayload(big); !errors.Is(err, ErrTokenTooLarge) {
		t.Fatalf("generic holders above cap must fail closed, got %v", err)
	}
}
