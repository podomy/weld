// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

package crypto

import (
	"bytes"
	"encoding/hex"
	"errors"
	"testing"
)

// TestKxcry9HKDFVectors pins HKDF-routed Kx derivation per 10.5.2.1.2.
func TestKxcry9HKDFVectors(t *testing.T) {
	t.Parallel()

	c1 := bytes.Repeat([]byte{0x11}, 32)
	c2 := bytes.Repeat([]byte{0x22}, 32)
	secret := bytes.Repeat([]byte{0x33}, 32)

	salt, key, err := DeriveKxKeys(c1, c2, secret)
	if err != nil {
		t.Fatalf("derive kx: %v", err)
	}

	wantSalt, err := hex.DecodeString("a3002a709cb7d280c43e8032315fc7532818326c4eb2ca30b60be9c5d6ce8a8c")
	if err != nil {
		t.Fatalf("decode want salt: %v", err)
	}

	wantKey, err := hex.DecodeString("028ba6b73119997ef922c75cbd28aa96ef21e023a580b95c31f9e6dd8557d515")
	if err != nil {
		t.Fatalf("decode want key: %v", err)
	}

	if !bytes.Equal(salt, wantSalt) {
		t.Fatalf("kx salt mismatch: got %x want %x", salt, wantSalt)
	}

	if !bytes.Equal(key, wantKey) {
		t.Fatalf("kx key mismatch: got %x want %x", key, wantKey)
	}

	var a [32]byte

	var b [32]byte

	for i := range a {
		a[i] = byte(i)
		b[i] = byte(0x80 + i)
	}

	salt2, key2, err := DeriveKxKeys(a[:], b[:], []byte("handshake-secret-32-bytes-padded!!"))
	if err != nil {
		t.Fatalf("derive kx2: %v", err)
	}

	wantSalt2, err := hex.DecodeString("ed874c3f8b6258cdad61277517575ae708c495db5624fb582b185bcf796ff140")
	if err != nil {
		t.Fatalf("decode want salt2: %v", err)
	}

	wantKey2, err := hex.DecodeString("4dbf1930c4cd33993bafb6899db10e3eb0d0a34ae65ea521d1538693c47d2dea")
	if err != nil {
		t.Fatalf("decode want key2: %v", err)
	}

	if !bytes.Equal(salt2, wantSalt2) {
		t.Fatalf("kx salt2 mismatch: got %x", salt2)
	}

	if !bytes.Equal(key2, wantKey2) {
		t.Fatalf("kx key2 mismatch: got %x", key2)
	}
}

// TestKxcry9BothPartiesAgree pins identical outputs for shared inputs.
func TestKxcry9BothPartiesAgree(t *testing.T) {
	t.Parallel()

	c1 := bytes.Repeat([]byte{0x44}, 32)
	c2 := bytes.Repeat([]byte{0x45}, 32)
	secret := bytes.Repeat([]byte{0x46}, 32)

	saltA, keyA, err := DeriveKxKeys(c1, c2, secret)
	if err != nil {
		t.Fatalf("derive A: %v", err)
	}

	saltB, keyB, err := DeriveKxKeys(c1, c2, secret)
	if err != nil {
		t.Fatalf("derive B: %v", err)
	}

	if !bytes.Equal(saltA, saltB) || !bytes.Equal(keyA, keyB) {
		t.Fatalf("both parties must agree")
	}

	if len(saltA) != 32 || len(keyA) != 32 {
		t.Fatalf("kx outputs must be 32 bytes, got %d %d", len(saltA), len(keyA))
	}

	if bytes.Equal(saltA, keyA) {
		t.Fatalf("salt and key must differ")
	}
}

// TestKxcry9RejectsBadInputs pins fail-closed challenge and secret checks.
func TestKxcry9RejectsBadInputs(t *testing.T) {
	t.Parallel()

	good := bytes.Repeat([]byte{0x11}, 32)

	if _, _, err := DeriveKxKeys(good[:31], good, good); !errors.Is(err, ErrTransformInvalid) {
		t.Fatalf("short challenge1 must fail, got %v", err)
	}

	if _, _, err := DeriveKxKeys(good, good[:31], good); !errors.Is(err, ErrTransformInvalid) {
		t.Fatalf("short challenge2 must fail, got %v", err)
	}

	if _, _, err := DeriveKxKeys(good, good, nil); !errors.Is(err, ErrTransformInvalid) {
		t.Fatalf("empty secret must fail, got %v", err)
	}
}
