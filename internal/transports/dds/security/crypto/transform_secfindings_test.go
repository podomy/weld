// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

package crypto

import (
	"bytes"
	"testing"
)

// TestTransformOriginAADBinds pins 10.5.3.3: origin binds into AAD.
//
//nolint:cyclop // same-origin, cross-origin, and legacy branches are each asserted
func TestTransformOriginAADBinds(t *testing.T) {
	t.Parallel()
	storeA := NewKeyStore()
	storeB := NewKeyStore()
	var sender [16]byte
	sender[0] = 0x71
	salt := bytes.Repeat([]byte{0x0d}, 32)
	key := bytes.Repeat([]byte{0x0c}, 32)
	rec := MasterRecord{Sender: sender, KeyID: 5, Revision: 1, Kind: TransformAES256GCM, Salt: salt, SenderKey: key, Session: 1}
	if err := storeA.Store(rec); err != nil {
		t.Fatalf("store A: %v", err)
	}
	if err := storeB.Store(rec); err != nil {
		t.Fatalf("store B: %v", err)
	}
	encA, err := NewTransform(storeA)
	if err != nil {
		t.Fatalf("new transform A: %v", err)
	}
	decB, err := NewTransform(storeB)
	if err != nil {
		t.Fatalf("new transform B: %v", err)
	}
	var origin [16]byte
	copy(origin[:], sender[:])
	var other [16]byte
	other[0] = 0xFF
	header, ct, err := encA.EncodeWithOrigin(sender, 5, 1, []byte("origin-bound"), origin, nil)
	if err != nil {
		t.Fatalf("encode with origin: %v", err)
	}
	plain, err := decB.DecodeWithOrigin(sender, 5, 1, header, ct, origin, nil)
	if err != nil {
		t.Fatalf("decode same origin: %v", err)
	}
	if !bytes.Equal(plain, []byte("origin-bound")) {
		t.Fatalf("roundtrip mismatch")
	}
	decC, err := NewTransform(storeB)
	if err != nil {
		t.Fatalf("new transform C: %v", err)
	}
	if _, err := decC.DecodeWithOrigin(sender, 5, 1, header, ct, other, nil); err == nil {
		t.Fatalf("cross-origin verify allowed, want fail")
	}
	// Legacy header-only path still roundtrips.
	header2, ct2, err := encA.Encode(sender, 5, 1, []byte("legacy"), nil)
	if err != nil {
		t.Fatalf("legacy encode: %v", err)
	}
	decD, err := NewTransform(storeB)
	if err != nil {
		t.Fatalf("new transform D: %v", err)
	}
	// Fresh decoders avoid replay claims across cases.
	if _, err := decD.Decode(sender, 5, 1, header2, ct2, nil); err != nil {
		t.Fatalf("legacy roundtrip: %v", err)
	}
}
