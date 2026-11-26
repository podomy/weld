// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

package tokens

import (
	"bytes"
	"testing"
)

// TestGapsVolatileSpecRoundtrip pins spec GenericMessage volatile framing.
func TestGapsVolatileSpecRoundtrip(t *testing.T) {
	t.Parallel()

	var dest, src [16]byte
	for i := range dest {
		dest[i] = byte(0xA0 + i)
		src[i] = byte(i)
	}

	envelope := VolatileEnvelope{
		DestinationGUID: dest,
		SourceGUID:      src,
		Tokens: []KeyMaterial{{
			TransformationKind: 4,
			MasterSalt:         bytes.Repeat([]byte{0x11}, 32),
			SenderKeyID:        9,
			MasterSenderKey:    bytes.Repeat([]byte{0x12}, 32),
			ReceiverKeyID:      3,
			MasterReceiverKey:  bytes.Repeat([]byte{0x13}, 32),
		}},
	}

	raw, err := EncodeVolatile(envelope)
	if err != nil {
		t.Fatalf("encode volatile spec: %v", err)
	}

	msg, err := DecodeGenericPayload(raw)
	if err != nil {
		t.Fatalf("spec payload must decode as GenericMessage, got %v", err)
	}

	if msg.ClassID != ClassParticipantTokens {
		t.Fatalf("class %q, want %q", msg.ClassID, ClassParticipantTokens)
	}

	back, err := DecodeVolatile(raw)
	if err != nil {
		t.Fatalf("decode volatile: %v", err)
	}

	if back.DestinationGUID != dest || back.SourceGUID != src {
		t.Fatalf("guid mismatch after spec roundtrip")
	}

	if len(back.Tokens) != 1 || back.Tokens[0].ReceiverKeyID != 3 {
		t.Fatalf("receiver material must survive spec roundtrip")
	}
}

// TestGapsVolatileCompatDecode pins legacy compact decoding.
func TestGapsVolatileCompatDecode(t *testing.T) {
	t.Parallel()

	var dest, src [16]byte
	for i := range dest {
		dest[i] = byte(0xB0 + i)
		src[i] = byte(0x10 + i)
	}

	material := KeyMaterial{
		TransformationKind: 4,
		MasterSalt:         bytes.Repeat([]byte{0x21}, 32),
		SenderKeyID:        4,
		MasterSenderKey:    bytes.Repeat([]byte{0x22}, 32),
	}

	seq, err := EncodeCryptoTokenSeq([]KeyMaterial{material})
	if err != nil {
		t.Fatalf("encode seq: %v", err)
	}

	legacy := append([]byte(nil), dest[:]...)
	legacy = append(legacy, src[:]...)
	legacy, err = encodeBytes(legacy, seq)
	if err != nil {
		t.Fatalf("wrap legacy: %v", err)
	}

	back, err := DecodeVolatile(legacy)
	if err != nil {
		t.Fatalf("legacy decode must still parse, got %v", err)
	}

	if back.DestinationGUID != dest || len(back.Tokens) != 1 {
		t.Fatalf("legacy envelope mismatch")
	}
}

// TestGapsVolatileWriterClass pins writer and reader exchange classes.
func TestGapsVolatileWriterClass(t *testing.T) {
	t.Parallel()

	var dest, src [16]byte

	envelope := VolatileEnvelope{
		DestinationGUID: dest,
		SourceGUID:      src,
		Tokens: []KeyMaterial{{
			TransformationKind: 4,
			MasterSalt:         bytes.Repeat([]byte{0x31}, 32),
			SenderKeyID:        1,
			MasterSenderKey:    bytes.Repeat([]byte{0x32}, 32),
		}},
	}

	for _, class := range []string{ClassWriterTokens, ClassReaderTokens} {
		raw, err := EncodeVolatileSpec(envelope, class)
		if err != nil {
			t.Fatalf("encode %s: %v", class, err)
		}

		back, err := DecodeVolatile(raw)
		if err != nil {
			t.Fatalf("decode %s: %v", class, err)
		}

		if len(back.Tokens) != 1 {
			t.Fatalf("class %s must roundtrip one token", class)
		}
	}
}
