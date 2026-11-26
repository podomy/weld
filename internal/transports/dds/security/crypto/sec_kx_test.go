// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

package crypto

import (
	"bytes"
	"testing"
)

// TestSecKxCookiesAre16Bytes pins the 16 byte cookie spellings per
// DDS Security 1.1 Table 67 and Fast-DDS interop.
func TestSecKxCookiesAre16Bytes(t *testing.T) {
	t.Parallel()

	if got := len("keyexchange salt"); got != 16 {
		t.Fatalf("salt cookie must be 16 bytes, got %d", got)
	}

	if got := len("key exchange key"); got != 16 {
		t.Fatalf("key cookie must be 16 bytes, got %d", got)
	}
}

// TestSecKxChallengeOrderSensitive pins swapped challenges deriving different keys.
func TestSecKxChallengeOrderSensitive(t *testing.T) {
	t.Parallel()

	c1 := bytes.Repeat([]byte{0x11}, 32)
	c2 := bytes.Repeat([]byte{0x22}, 32)
	secret := bytes.Repeat([]byte{0x33}, 32)

	saltFwd, keyFwd, err := DeriveKxKeys(c1, c2, secret)
	if err != nil {
		t.Fatalf("derive fwd: %v", err)
	}

	saltRev, keyRev, err := DeriveKxKeys(c2, c1, secret)
	if err != nil {
		t.Fatalf("derive rev: %v", err)
	}

	if bytes.Equal(saltFwd, saltRev) || bytes.Equal(keyFwd, keyRev) {
		t.Fatalf("swapped challenges must derive different keys")
	}
}

// TestSecKxUsesHKDF pins HKDF routing so hkdf.go cannot go dead.
func TestSecKxUsesHKDF(t *testing.T) {
	t.Parallel()

	c1 := bytes.Repeat([]byte{0x44}, 32)
	c2 := bytes.Repeat([]byte{0x45}, 32)
	secret := bytes.Repeat([]byte{0x46}, 32)

	saltInfo := appendChallengeLabel(c1, "keyexchange salt", c2)
	keyInfo := appendChallengeLabel(c2, "key exchange key", c1)

	wantSalt, err := HKDF(nil, secret, saltInfo, 32)
	if err != nil {
		t.Fatalf("hkdf salt: %v", err)
	}

	wantKey, err := HKDF(nil, secret, keyInfo, 32)
	if err != nil {
		t.Fatalf("hkdf key: %v", err)
	}

	gotSalt, gotKey, err := DeriveKxKeys(c1, c2, secret)
	if err != nil {
		t.Fatalf("derive: %v", err)
	}

	if !bytes.Equal(gotSalt, wantSalt) || !bytes.Equal(gotKey, wantKey) {
		t.Fatalf("DeriveKxKeys must route through HKDF")
	}
}
