// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

package crypto

import (
	"bytes"
	"errors"
	"testing"

	"github.com/podomy/weld/internal/transports/dds/security/tokens"
)

// TestKxSealRoundtrip pins 9.5.1.9 plus 10.5.2.1.2: volatile seals with Kx.
//
//nolint:cyclop // seal, open, fail-closed branches are each asserted
func TestKxSealRoundtrip(t *testing.T) {
	t.Parallel()
	storeA := NewKeyStore()
	storeB := NewKeyStore()
	var sender, remote [16]byte
	sender[0] = 0x51
	remote[0] = 0x52
	salt := bytes.Repeat([]byte{0x56}, 32)
	key := bytes.Repeat([]byte{0x57}, 32)
	if err := storeA.Store(MasterRecord{Sender: sender, KeyID: 9, Revision: 1, Kind: TransformAES256GCM, Salt: salt, SenderKey: key, Session: 1}); err != nil {
		t.Fatalf("store A: %v", err)
	}
	exchA, err := NewKeyExchange(storeA, func([16]byte) bool { return true })
	if err != nil {
		t.Fatalf("new A: %v", err)
	}
	exchB, err := NewKeyExchange(storeB, func([16]byte) bool { return true })
	if err != nil {
		t.Fatalf("new B: %v", err)
	}
	c1 := bytes.Repeat([]byte{0x01}, 32)
	c2 := bytes.Repeat([]byte{0x02}, 32)
	secret := []byte("handshake-secret-32-bytes-padded!!")
	if err := exchA.EstablishKx(remote, c1, c2, secret); err != nil {
		t.Fatalf("establish A: %v", err)
	}
	if err := exchB.EstablishKx(sender, c1, c2, secret); err != nil {
		t.Fatalf("establish B: %v", err)
	}
	envelope, err := exchA.CreateLocalTokens(sender, remote, 9, 1)
	if err != nil {
		t.Fatalf("create tokens: %v", err)
	}
	// Wire must seal: raw EncodeVolatile in clear is forbidden on this path.
	sealed, err := exchA.SealVolatile(envelope)
	if err != nil {
		t.Fatalf("seal: %v", err)
	}
	if bytes.Contains(sealed, key) {
		t.Fatalf("sealed envelope leaks key material in clear")
	}
	opened, err := exchB.OpenVolatile(sealed, sender)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if opened.SourceGUID != sender || opened.DestinationGUID != remote {
		t.Fatalf("opened GUIDs mismatch")
	}
	// No Kx session fails closed instead of sending clear.
	bare, err := NewKeyExchange(NewKeyStore(), func([16]byte) bool { return true })
	if err != nil {
		t.Fatalf("new bare: %v", err)
	}
	if _, err := bare.SealVolatile(envelope); !errors.Is(err, ErrNoKxSession) {
		t.Fatalf("seal without kx = %v, want ErrNoKxSession", err)
	}
	if _, err := bare.CreateSealedLocalTokens(sender, remote, 9, 1); err == nil {
		t.Fatalf("sealed create without material allowed, want deny")
	}
	// Wrong source fails closed.
	if _, err := exchB.OpenVolatile(sealed, remote); err == nil {
		t.Fatalf("open with wrong source allowed, want deny")
	}
	_ = tokens.TokenIdentity
}

// TestRolloverHoldSignal pins hold-before-use per 9.5.1.9.
func TestRolloverHoldSignal(t *testing.T) {
	t.Parallel()
	store := NewKeyStoreWithLimit(1)
	var sender, remote [16]byte
	sender[0] = 0x61
	remote[0] = 0x62
	salt := bytes.Repeat([]byte{0x56}, 32)
	key := bytes.Repeat([]byte{0x57}, 32)
	if err := store.Store(MasterRecord{Sender: sender, KeyID: 3, Revision: 1, Kind: TransformAES256GCM, Salt: salt, SenderKey: key, Session: 1}); err != nil {
		t.Fatalf("store: %v", err)
	}
	exch, err := NewKeyExchange(store, func([16]byte) bool { return true })
	if err != nil {
		t.Fatalf("new exchange: %v", err)
	}
	held, _, err := exch.RolloverHoldSignal(sender, remote, 3, 1)
	if err != nil {
		t.Fatalf("hold query: %v", err)
	}
	if held {
		t.Fatalf("fresh session holds, want no hold")
	}
	if err := store.ObserveBlocks(sender, 3, 1, 1); err != nil {
		t.Fatalf("observe blocks: %v", err)
	}
	held, signal, err := exch.RolloverHoldSignal(sender, remote, 3, 1)
	if err != nil {
		t.Fatalf("hold pending: %v", err)
	}
	if !held {
		t.Fatalf("bound reached without hold, want hold")
	}
	if len(signal.Tokens) == 0 {
		t.Fatalf("hold signal carries no tokens")
	}
	if RolloverHoldTimeoutcry9 <= 0 {
		t.Fatalf("hold timeout non-positive")
	}
}
