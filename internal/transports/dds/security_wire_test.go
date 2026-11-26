// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

package dds

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"testing"

	"github.com/podomy/weld/internal/transports/dds/rtps"
	"github.com/podomy/weld/internal/transports/dds/security/crypto"
)

// TestGHASHMatchesStdlib pins GMAC creation against AES-GCM verification.
func TestGHASHMatchesStdlib(t *testing.T) {
	t.Parallel()

	key := bytes.Repeat([]byte{0x0b}, 32)

	var iv [secIVLen]byte

	for i := range iv {
		iv[i] = byte(i)
	}

	aads := [][]byte{
		nil,
		{},
		bytes.Repeat([]byte{0x31}, 24),
	}

	bodies := [][]byte{
		{},
		{0x01},
		bytes.Repeat([]byte{0x02}, 15),
		bytes.Repeat([]byte{0x03}, 16),
		bytes.Repeat([]byte{0x04}, 17),
		bytes.Repeat([]byte{0x05}, 100),
	}

	for _, body := range bodies {
		for _, aad := range aads {
			checkGHASHVector(t, key, iv[:], body, aad)
		}
	}
}

// checkGHASHVector checks one GMAC vector against stdlib AES-GCM.
func checkGHASHVector(t *testing.T, key, iv, body, aad []byte) {
	t.Helper()

	tag, err := secGMACTag(key, iv, body, aad)
	if err != nil {
		t.Fatalf("tag: %v", err)
	}

	if len(tag) != secTagLen {
		t.Fatalf("tag length %d", len(tag))
	}

	block, err := aes.NewCipher(key)
	if err != nil {
		t.Fatalf("cipher: %v", err)
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		t.Fatalf("gcm: %v", err)
	}

	signed := append(append([]byte(nil), body...), tag...)

	if _, err := gcm.Open(nil, iv, signed, aad); err != nil {
		t.Fatalf("stdlib verify body %d aad %d: %v", len(body), len(aad), err)
	}

	wrongAAD := append(append([]byte(nil), aad...), 0x00)
	if _, err := gcm.Open(nil, iv, signed, wrongAAD); err == nil && len(body)+len(aad) != 0 {
		t.Fatalf("wrong aad must fail body %d", len(body))
	}
}

// TestSECTripleRoundtrip pins SIGN plus ENCRYPT submessage wrapping.
func TestSECTripleRoundtrip(t *testing.T) {
	t.Parallel()

	key := bytes.Repeat([]byte{0x0c}, 32)
	salt := bytes.Repeat([]byte{0x0d}, 32)

	inner := rtps.RawSubmessage{ID: rtps.SubmessageData, Flags: rtps.FlagLittleEndian, Body: bytes.Repeat([]byte{0x0e}, 48)}

	framed := secFrameSubmessage(inner)

	for _, kind := range []uint32{secKindAES256GMAC, secKindAES256GCM} {
		sessionKey := mustSECSessionKey(t, key, salt, 5)

		wrapped, err := secWrapSubmessage(kind, 9, 5, 11, sessionKey, framed)
		if err != nil {
			t.Fatalf("wrap kind %d: %v", kind, err)
		}

		first, second, third, err := secSplitTriple(wrapped)
		if err != nil {
			t.Fatalf("split kind %d: %v", kind, err)
		}

		opened, err := secUnwrapTriple(first, second, third, func(transformID, session uint32) ([]byte, []byte, error) {
			if transformID != 9 || session != 5 {
				t.Fatalf("lookup id %d session %d", transformID, session)
			}

			return key, salt, nil
		})
		if err != nil {
			t.Fatalf("unwrap kind %d: %v", kind, err)
		}

		if !bytes.Equal(opened, framed) {
			t.Fatalf("kind %d roundtrip mismatch", kind)
		}
	}
}

// mustSECSessionKey derives one test session key.
func mustSECSessionKey(t *testing.T, key, salt []byte, session uint32) []byte {
	t.Helper()

	sessionKey, err := crypto.DeriveSessionKey(key, salt, session, "SessionKey")
	if err != nil {
		t.Fatalf("session key: %v", err)
	}

	return sessionKey
}

// TestSECTripleRoundtripAES128 pins 128-bit kinds with 32-byte salts.
//
// The master salt stays 32 bytes for both key sizes while master and
// session keys are 16 bytes for AES-128, matching the keystore and
// DeriveSessionKey contracts.
func TestSECTripleRoundtripAES128(t *testing.T) {
	t.Parallel()

	key := bytes.Repeat([]byte{0x0c}, 16)
	salt := bytes.Repeat([]byte{0x0d}, 32)

	inner := rtps.RawSubmessage{ID: rtps.SubmessageData, Flags: rtps.FlagLittleEndian, Body: bytes.Repeat([]byte{0x0e}, 48)}

	framed := secFrameSubmessage(inner)

	for _, kind := range []uint32{secKindAES128GMAC, secKindAES128GCM} {
		full := mustSECSessionKey(t, key, salt, 5)

		sessionKey := full[:16]

		wrapped, err := secWrapSubmessage(kind, 9, 5, 11, sessionKey, framed)
		if err != nil {
			t.Fatalf("wrap kind %d: %v", kind, err)
		}

		first, second, third, err := secSplitTriple(wrapped)
		if err != nil {
			t.Fatalf("split kind %d: %v", kind, err)
		}

		opened, err := secUnwrapTriple(first, second, third, func(transformID, session uint32) ([]byte, []byte, error) {
			if transformID != 9 || session != 5 {
				t.Fatalf("lookup id %d session %d", transformID, session)
			}

			return key, salt, nil
		})
		if err != nil {
			t.Fatalf("unwrap kind %d: %v", kind, err)
		}

		if !bytes.Equal(opened, framed) {
			t.Fatalf("kind %d roundtrip mismatch", kind)
		}
	}
}

// TestSECTripleRejects pins fail-closed unwrapping.
func TestSECTripleRejects(t *testing.T) {
	t.Parallel()

	key := bytes.Repeat([]byte{0x0c}, 32)
	salt := bytes.Repeat([]byte{0x0d}, 32)

	inner := rtps.RawSubmessage{ID: rtps.SubmessageData, Flags: rtps.FlagLittleEndian, Body: []byte{0x01, 0x02}}

	framed := secFrameSubmessage(inner)
	sessionKey := mustSECSessionKey(t, key, salt, 1)

	wrapped, err := secWrapSubmessage(secKindAES256GMAC, 1, 1, 1, sessionKey, framed)
	if err != nil {
		t.Fatalf("wrap: %v", err)
	}

	first, second, third, err := secSplitTriple(wrapped)
	if err != nil {
		t.Fatalf("split: %v", err)
	}

	lookup := func(_, session uint32) ([]byte, []byte, error) {
		return key, salt, nil
	}

	tampered := append([]byte(nil), second...)
	tampered[len(tampered)-1] ^= 0x01

	if _, err := secUnwrapTriple(first, tampered, third, lookup); err == nil {
		t.Fatalf("tampered body must fail")
	}

	tamperedTag := append([]byte(nil), third...)
	tamperedTag[len(tamperedTag)-2] ^= 0x01

	if _, err := secUnwrapTriple(first, second, tamperedTag, lookup); err == nil {
		t.Fatalf("tampered tag must fail")
	}

	tamperedPrefix := append([]byte(nil), first...)
	tamperedPrefix[len(tamperedPrefix)-1] ^= 0x01

	if _, err := secUnwrapTriple(tamperedPrefix, second, third, lookup); err == nil {
		t.Fatalf("tampered prefix must fail with prefix AAD")
	}

	wrongLookup := func(transformID, session uint32) ([]byte, []byte, error) {
		return bytes.Repeat([]byte{0xff}, 32), salt, nil
	}

	if _, err := secUnwrapTriple(first, second, third, wrongLookup); err == nil {
		t.Fatalf("wrong key must fail")
	}

	if _, _, _, err := secSplitTriple(wrapped[:len(wrapped)-1]); err == nil {
		t.Fatalf("truncated triple must fail")
	}
}
