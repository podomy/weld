// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

package dds

import (
	"bytes"
	"testing"

	"github.com/podomy/weld/internal/transports/dds/rtps"
	"github.com/podomy/weld/internal/transports/dds/security/crypto"
)

// TestSecOriginBoundRoundtrip pins origin-bound seal plus open per 10.5.3.3.
//
// Origin-bound seals verify only on origin-bound decoders with the same
// origin; prefix-only decoders must fail closed on them.
func TestSecOriginBoundRoundtrip(t *testing.T) {
	t.Parallel()

	key := bytes.Repeat([]byte{0x0c}, 32)
	salt := bytes.Repeat([]byte{0x0d}, 32)
	inner := rtps.RawSubmessage{ID: rtps.SubmessageData, Flags: rtps.FlagLittleEndian, Body: bytes.Repeat([]byte{0x0e}, 48)}
	framed := secFrameSubmessage(inner)

	var origin [16]byte
	origin[0] = 0xA1
	origin[1] = 0xB2

	for _, kind := range []uint32{secKindAES256GMAC, secKindAES256GCM} {
		sessionKey := mustSECSessionKey(t, key, salt, 5)
		wrapped, err := secWrapSubmessageWithOrigin(kind, 9, 5, 11, sessionKey, origin, framed)
		if err != nil {
			t.Fatalf("wrap kind %d: %v", kind, err)
		}
		first, second, third, err := secSplitTriple(wrapped)
		if err != nil {
			t.Fatalf("split kind %d: %v", kind, err)
		}
		opened, err := secUnwrapTripleWithOrigin(first, second, third, origin, func(transformID, session uint32) ([]byte, []byte, error) {
			return key, salt, nil
		})
		if err != nil {
			t.Fatalf("origin open kind %d: %v", kind, err)
		}
		if !bytes.Equal(opened, framed) {
			t.Fatalf("kind %d roundtrip mismatch", kind)
		}
		// Prefix-only open must fail closed on origin-bound seals.
		if _, err := secUnwrapTriple(first, second, third, func(transformID, session uint32) ([]byte, []byte, error) {
			return key, salt, nil
		}); err == nil {
			t.Fatalf("kind %d prefix-only open of origin seal passed, want fail", kind)
		}
		// Wrong origin must fail closed.
		var wrong [16]byte
		wrong[0] = 0xFF
		if _, err := secUnwrapTripleWithOrigin(first, second, third, wrong, func(transformID, session uint32) ([]byte, []byte, error) {
			return key, salt, nil
		}); err == nil {
			t.Fatalf("kind %d wrong origin opened, want fail", kind)
		}
	}
}

// TestSecPrefixOnlyCrossVerifyFails pins prefix-only seals verify only on prefix-only decoders.
//
// Origin-bound decoders must fail closed on prefix-only seals, mirroring
// the origin-bound direction, so the two domains never cross-verify.
func TestSecPrefixOnlyCrossVerifyFails(t *testing.T) {
	t.Parallel()

	key := bytes.Repeat([]byte{0x0c}, 32)
	salt := bytes.Repeat([]byte{0x0d}, 32)
	inner := rtps.RawSubmessage{ID: rtps.SubmessageData, Flags: rtps.FlagLittleEndian, Body: []byte{0x01, 0x02}}
	framed := secFrameSubmessage(inner)
	sessionKey := mustSECSessionKey(t, key, salt, 1)

	for _, kind := range []uint32{secKindAES256GMAC, secKindAES256GCM} {
		wrapped, err := secWrapSubmessage(kind, 1, 1, 1, sessionKey, framed)
		if err != nil {
			t.Fatalf("prefix wrap kind %d: %v", kind, err)
		}
		first, second, third, err := secSplitTriple(wrapped)
		if err != nil {
			t.Fatalf("split kind %d: %v", kind, err)
		}
		var origin [16]byte
		origin[0] = 0xA1
		if _, err := secUnwrapTripleWithOrigin(first, second, third, origin, func(transformID, session uint32) ([]byte, []byte, error) {
			return key, salt, nil
		}); err == nil {
			t.Fatalf("kind %d origin open of prefix seal passed, want fail", kind)
		}
		if _, err := secUnwrapTriple(first, second, third, func(transformID, session uint32) ([]byte, []byte, error) {
			return key, salt, nil
		}); err != nil {
			t.Fatalf("prefix open kind %d: %v", kind, err)
		}
	}
}

// TestSecOriginBoundKeystoreRoundtrip pins decryptable payload with KeyStore session keys.
//
// It stores one master record in a KeyStore, derives the session key,
// seals with origin-bound AAD, and opens with a lookup from the same
// store, proving the egress path delivers decryptable payloads.
func TestSecOriginBoundKeystoreRoundtrip(t *testing.T) {
	t.Parallel()

	var origin [16]byte
	origin[0] = 0xC3

	store := crypto.NewKeyStore()
	masterKey := bytes.Repeat([]byte{0x0b}, 16)
	masterSalt := bytes.Repeat([]byte{0x0d}, 32)
	err := store.Store(crypto.MasterRecord{
		Sender:    origin,
		KeyID:     1,
		Revision:  1,
		Kind:      crypto.TransformAES128GCM,
		Salt:      masterSalt,
		SenderKey: masterKey,
		Session:   9,
	})
	if err != nil {
		t.Fatalf("store: %v", err)
	}
	rec, err := store.Lookup(origin, 1, 1)
	if err != nil {
		t.Fatalf("lookup: %v", err)
	}
	session, suffix, err := store.AllocateSuffix(origin, 1, 1)
	if err != nil {
		t.Fatalf("allocate: %v", err)
	}
	sessionKey, err := crypto.DeriveSessionKey(rec.SenderKey, rec.Salt, session, "SessionKey")
	if err != nil {
		t.Fatalf("derive: %v", err)
	}
	if len(sessionKey) > 16 {
		sessionKey = sessionKey[:16]
	}
	inner := rtps.RawSubmessage{ID: rtps.SubmessageData, Flags: rtps.FlagLittleEndian, Body: []byte{0xDE, 0xAD, 0xBE, 0xEF}}
	framed := secFrameSubmessage(inner)
	sealed, err := sealFramedWithOrigin(secKindAES128GCM, 1, session, suffix, sessionKey, origin, framed)
	if err != nil {
		t.Fatalf("seal: %v", err)
	}
	first, second, third, err := secSplitTriple(sealed)
	if err != nil {
		t.Fatalf("split: %v", err)
	}
	opened, err := secUnwrapTripleWithOrigin(first, second, third, origin, func(transformID, session uint32) ([]byte, []byte, error) {
		rev, ok := store.LatestRevision(origin, transformID)
		if !ok {
			t.Fatalf("latest revision missing")
		}
		r, err := store.Lookup(origin, transformID, rev)
		if err != nil {
			t.Fatalf("lookup open: %v", err)
		}
		return r.SenderKey, r.Salt, nil
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if !bytes.Equal(opened, framed) {
		t.Fatalf("keystore roundtrip mismatch")
	}
}
