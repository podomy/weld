// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

package crypto

import (
	"bytes"
	"testing"
)

// TestReceivercry9Stored pins acceptance of receiver material per 7.3.20.
func TestReceivercry9Refused(t *testing.T) {
	t.Parallel()

	sender := auditSender(0x61)
	store := NewKeyStore()

	withBoth := MasterRecord{
		Sender: sender, KeyID: 11, Revision: 1, Kind: TransformAES256GCM,
		Salt: bytes.Repeat([]byte{0x41}, 32), SenderKey: bytes.Repeat([]byte{0x42}, 32),
		ReceiverKeyID: 7, ReceiverKey: bytes.Repeat([]byte{0x43}, 32), Session: 1,
	}

	if err := store.Store(withBoth); err != nil {
		t.Fatalf("store with receiver key must accept, got %v", err)
	}

	onlyID := MasterRecord{
		Sender: sender, KeyID: 11, Revision: 2, Kind: TransformAES256GCM,
		Salt: bytes.Repeat([]byte{0x41}, 32), SenderKey: bytes.Repeat([]byte{0x42}, 32),
		ReceiverKeyID: 1, Session: 1,
	}

	if err := store.Store(onlyID); err == nil {
		t.Fatalf("store with receiver id but no key must refuse")
	}

	badLen := MasterRecord{
		Sender: sender, KeyID: 11, Revision: 3, Kind: TransformAES256GCM,
		Salt: bytes.Repeat([]byte{0x41}, 32), SenderKey: bytes.Repeat([]byte{0x42}, 32),
		ReceiverKey: bytes.Repeat([]byte{0x43}, 7), ReceiverKeyID: 2, Session: 1,
	}

	if err := store.Store(badLen); err == nil {
		t.Fatalf("store with short receiver key must refuse")
	}

	stored, err := store.Lookup(sender, 11, 1)
	if err != nil {
		t.Fatalf("lookup receiver record: %v", err)
	}

	if len(stored.ReceiverKey) != 32 || stored.ReceiverKeyID != 7 {
		t.Fatalf("receiver material must round-trip, got id %d len %d", stored.ReceiverKeyID, len(stored.ReceiverKey))
	}
}

// TestReceivercry9DecryptFallback pins receiver-key decrypt per 7.3.20.
func TestReceivercry9DecryptFallback(t *testing.T) {
	t.Parallel()

	sender := auditSender(0x66)
	senderKey := bytes.Repeat([]byte{0x42}, 32)
	receiverKey := bytes.Repeat([]byte{0x43}, 32)
	salt := bytes.Repeat([]byte{0x41}, 32)

	encodeStore := NewKeyStore()
	if err := encodeStore.Store(MasterRecord{
		Sender: sender, KeyID: 21, Revision: 1, Kind: TransformAES256GCM,
		Salt: append([]byte(nil), salt...), SenderKey: append([]byte(nil), receiverKey...), Session: 1,
	}); err != nil {
		t.Fatalf("store encode side: %v", err)
	}

	encodeTransform, err := NewTransform(encodeStore)
	if err != nil {
		t.Fatalf("new encode transform: %v", err)
	}

	header, ciphertext, err := encodeTransform.Encode(sender, 21, 1, []byte("receiver-key payload"), nil)
	if err != nil {
		t.Fatalf("encode with receiver key bytes: %v", err)
	}

	decodeStore := NewKeyStore()
	if err := decodeStore.Store(MasterRecord{
		Sender: sender, KeyID: 21, Revision: 1, Kind: TransformAES256GCM,
		Salt: append([]byte(nil), salt...), SenderKey: append([]byte(nil), senderKey...),
		ReceiverKeyID: 7, ReceiverKey: append([]byte(nil), receiverKey...), Session: 1,
	}); err != nil {
		t.Fatalf("store decode side: %v", err)
	}

	decodeTransform, err := NewTransform(decodeStore)
	if err != nil {
		t.Fatalf("new decode transform: %v", err)
	}

	plain, err := decodeTransform.Decode(sender, 21, 1, header, ciphertext, nil)
	if err != nil {
		t.Fatalf("decode must fall back to receiver key: %v", err)
	}

	if string(plain) != "receiver-key payload" {
		t.Fatalf("plaintext %q mismatch", plain)
	}
}

// TestReceivercry9ExchangeInstalls pins volatile install with receiver keys.
func TestReceivercry9ExchangeRefuses(t *testing.T) {
	t.Parallel()

	storeA := NewKeyStore()
	storeB := NewKeyStore()
	sender := auditSender(0x62)

	var remote [16]byte

	copy(remote[:], bytes.Repeat([]byte{0x63}, 16))

	err := storeA.Store(MasterRecord{
		Sender: sender, KeyID: 12, Revision: 1, Kind: TransformAES256GCM,
		Salt: bytes.Repeat([]byte{0x41}, 32), SenderKey: bytes.Repeat([]byte{0x42}, 32),
		ReceiverKeyID: 5, ReceiverKey: bytes.Repeat([]byte{0x44}, 32), Session: 1,
	})
	if err != nil {
		t.Fatalf("store sender with receiver key: %v", err)
	}

	exchA, err := NewKeyExchange(storeA, func([16]byte) bool { return true })
	if err != nil {
		t.Fatalf("new exchange A: %v", err)
	}

	exchB, err := NewKeyExchange(storeB, func([16]byte) bool { return true })
	if err != nil {
		t.Fatalf("new exchange B: %v", err)
	}

	envelope, err := exchA.CreateLocalTokens(sender, remote, 12, 1)
	if err != nil {
		t.Fatalf("create tokens: %v", err)
	}

	if len(envelope.Tokens[0].MasterReceiverKey) != 32 || envelope.Tokens[0].ReceiverKeyID != 5 {
		t.Fatalf("volatile envelope must carry receiver material")
	}

	err = exchB.SetRemoteTokens(envelope, remote)
	if err != nil {
		t.Fatalf("receiver material must install, got %v", err)
	}

	stored, err := storeB.Lookup(sender, 12, 1)
	if err != nil {
		t.Fatalf("lookup installed receiver record: %v", err)
	}

	if len(stored.ReceiverKey) != 32 || stored.ReceiverKeyID != 5 {
		t.Fatalf("installed record must keep receiver material")
	}
}
