// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

package crypto

import (
	"bytes"
	"errors"
	"sync"
	"testing"

	"github.com/podomy/weld/internal/transports/dds/security/tokens"
)

// auditMaster returns deterministic key material for replay tests.
func auditMaster(sender [16]byte, keyID, revision uint32) MasterRecord {
	return MasterRecord{
		Sender:    sender,
		KeyID:     keyID,
		Revision:  revision,
		Kind:      TransformAES256GCM,
		Salt:      bytes.Repeat([]byte{0x41}, 32),
		SenderKey: bytes.Repeat([]byte{0x42}, 32),
		Session:   1,
	}
}

// auditSender returns a fixed sender GUID for tests.
func auditSender(fill byte) [16]byte {
	var sender [16]byte

	copy(sender[:], bytes.Repeat([]byte{fill}, 16))

	return sender
}

// TestAuditConcurrentReplaySingleDelivery pins one delivery under races.
func TestAuditConcurrentReplaySingleDelivery(t *testing.T) {
	t.Parallel()

	store := NewKeyStore()
	sender := auditSender(0x51)

	err := store.Store(auditMaster(sender, 1, 1))
	if err != nil {
		t.Fatalf("store master: %v", err)
	}

	transform, err := NewTransform(store)
	if err != nil {
		t.Fatalf("new transform: %v", err)
	}

	header, ciphertext, err := transform.Encode(sender, 1, 1, []byte("race payload"), nil)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	const racers = 16

	var arrived int

	var mu sync.Mutex

	var group sync.WaitGroup

	for range racers {
		group.Go(func() {
			plain, derr := transform.Decode(sender, 1, 1, header, ciphertext, nil)
			if derr != nil {
				if !errors.Is(derr, ErrReplay) {
					t.Errorf("loser must see replay, got %v", derr)
				}

				return
			}

			if string(plain) != "race payload" {
				t.Errorf("winner plaintext %q mismatch", plain)

				return
			}

			mu.Lock()
			arrived++
			mu.Unlock()
		})
	}

	group.Wait()

	if arrived != 1 {
		t.Fatalf("exactly one delivery must win, got %d", arrived)
	}
}

// TestAuditSeenMapBounded pins oldest-first eviction past the cap.
func TestAuditSeenMapBounded(t *testing.T) {
	t.Parallel()

	store := NewKeyStore()
	sender := auditSender(0x52)

	err := store.Store(auditMaster(sender, 1, 1))
	if err != nil {
		t.Fatalf("store master: %v", err)
	}

	transform, err := NewTransform(store)
	if err != nil {
		t.Fatalf("new transform: %v", err)
	}

	for i := range MaxSeenEntries + 128 {
		header, ciphertext, err := transform.Encode(sender, 1, 1, []byte("bounded"), nil)
		if err != nil {
			t.Fatalf("encode %d: %v", i, err)
		}

		_, err = transform.Decode(sender, 1, 1, header, ciphertext, nil)
		if err != nil {
			t.Fatalf("decode %d: %v", i, err)
		}
	}

	if got := transform.SeenCount(); got != MaxSeenEntries {
		t.Fatalf("seen count %d must equal the cap %d", got, MaxSeenEntries)
	}

	header, ciphertext, err := transform.Encode(sender, 1, 1, []byte("still works"), nil)
	if err != nil {
		t.Fatalf("encode past cap: %v", err)
	}

	plain, err := transform.Decode(sender, 1, 1, header, ciphertext, nil)
	if err != nil {
		t.Fatalf("decode past cap: %v", err)
	}

	if string(plain) != "still works" {
		t.Fatalf("plaintext %q mismatch", plain)
	}

	if got := transform.SeenCount(); got != MaxSeenEntries {
		t.Fatalf("seen count %d must stay at the cap %d", got, MaxSeenEntries)
	}
}

// TestAuditUnregisterRemovesAllRevisions pins full participant teardown.
func TestAuditUnregisterRemovesAllRevisions(t *testing.T) {
	t.Parallel()

	store := NewKeyStore()
	sender := auditSender(0x53)

	factory, err := NewKeyFactory(store)
	if err != nil {
		t.Fatalf("new factory: %v", err)
	}

	for _, revision := range []uint32{1, 2, 3} {
		record := auditMaster(sender, 7, revision)

		err = store.Store(record)
		if err != nil {
			t.Fatalf("store revision %d: %v", revision, err)
		}
	}

	if !factory.UnregisterParticipant(sender, 7, 1) {
		t.Fatalf("unregister must report removals")
	}

	for _, revision := range []uint32{1, 2, 3} {
		_, err = store.Lookup(sender, 7, revision)
		if !errors.Is(err, ErrUnknownKey) {
			t.Fatalf("revision %d must be gone, got %v", revision, err)
		}
	}

	if factory.UnregisterParticipant(sender, 7, 1) {
		t.Fatalf("second unregister must report nothing removed")
	}
}

// auditExchangeEnds builds one sender plus receiver exchange pair.
type auditExchangeEnds struct {
	storeA   *KeyStore
	storeB   *KeyStore
	sender   [16]byte
	remote   [16]byte
	salt     []byte
	key      []byte
	exchA    *KeyExchange
	exchB    *KeyExchange
	envelope tokens.VolatileEnvelope
}

// auditExchangeSetup installs revision 1 on both ends of the exchange.
func auditExchangeSetup(t *testing.T) *auditExchangeEnds {
	t.Helper()

	ends := &auditExchangeEnds{
		storeA: NewKeyStore(),
		storeB: NewKeyStore(),
		sender: auditSender(0x54),
		salt:   bytes.Repeat([]byte{0x56}, 32),
		key:    bytes.Repeat([]byte{0x57}, 32),
	}

	copy(ends.remote[:], bytes.Repeat([]byte{0x55}, 16))

	err := ends.storeA.Store(MasterRecord{
		Sender: ends.sender, KeyID: 9, Revision: 1, Kind: TransformAES256GCM,
		Salt: append([]byte(nil), ends.salt...), SenderKey: append([]byte(nil), ends.key...), Session: 1,
	})
	if err != nil {
		t.Fatalf("store revision 1: %v", err)
	}

	ends.exchA, err = NewKeyExchange(ends.storeA, func([16]byte) bool { return true })
	if err != nil {
		t.Fatalf("new exchange A: %v", err)
	}

	ends.exchB, err = NewKeyExchange(ends.storeB, func([16]byte) bool { return true })
	if err != nil {
		t.Fatalf("new exchange B: %v", err)
	}

	ends.envelope, err = ends.exchA.CreateLocalTokens(ends.sender, ends.remote, 9, 1)
	if err != nil {
		t.Fatalf("create tokens: %v", err)
	}

	err = ends.exchB.SetRemoteTokens(ends.envelope, ends.remote)
	if err != nil {
		t.Fatalf("set remote revision 1: %v", err)
	}

	if got := auditLatestRevision(t, ends.storeB, ends.sender, 9); got != 1 {
		t.Fatalf("first install must land at revision 1, got %d", got)
	}

	return ends
}

// auditRotateSender stores one rotated revision on the sender side.
func auditRotateSender(t *testing.T, ends *auditExchangeEnds, key []byte, revision uint32) {
	t.Helper()

	err := ends.storeA.Store(MasterRecord{
		Sender: ends.sender, KeyID: 9, Revision: revision, Kind: TransformAES256GCM,
		Salt: append([]byte(nil), ends.salt...), SenderKey: append([]byte(nil), key...), Session: 1,
	})
	if err != nil {
		t.Fatalf("store revision %d: %v", revision, err)
	}
}

// auditLatestRevision reads the highest stored revision for assertions.
//
//nolint:unparam // rotation fixtures pin key 9; the parameter keeps the helper general.
func auditLatestRevision(t *testing.T, store *KeyStore, sender [16]byte, keyID uint32) uint32 {
	t.Helper()

	revision, ok := store.LatestRevision(sender, keyID)
	if !ok {
		t.Fatalf("no revision stored for key %d", keyID)
	}

	return revision
}

// auditStageReceivercry9 installs one staged revision via Revise plus Activate.
func auditStageReceivercry9(t *testing.T, store *KeyStore, sender [16]byte, salt, key []byte, revision uint32) {
	t.Helper()

	factory, err := NewKeyFactory(store)
	if err != nil {
		t.Fatalf("new factory: %v", err)
	}

	staged := tokens.KeyMaterial{
		TransformationKind: TransformAES256GCM,
		MasterSalt:         append([]byte(nil), salt...),
		SenderKeyID:        9,
		MasterSenderKey:    append([]byte(nil), key...),
	}

	err = factory.Revise(sender, revision, staged)
	if err != nil {
		t.Fatalf("revise receiver: %v", err)
	}

	err = factory.Activate(sender, 9, revision)
	if err != nil {
		t.Fatalf("activate receiver: %v", err)
	}
}

// TestAuditExchangeRotationAdvances pins staged receiver revision agreement.
func TestAuditExchangeRotationAdvances(t *testing.T) {
	t.Parallel()

	ends := auditExchangeSetup(t)

	// Novel unsolicited bytes never bump latest: without prior staging
	// the rotated revision must refuse and leave revision 1 intact.
	rotated := bytes.Repeat([]byte{0x58}, 32)

	auditRotateSender(t, ends, rotated, 2)

	envelope, err := ends.exchA.CreateLocalTokens(ends.sender, ends.remote, 9, 2)
	if err != nil {
		t.Fatalf("create rotated tokens: %v", err)
	}

	err = ends.exchB.SetRemoteTokens(envelope, ends.remote)
	if err == nil {
		t.Fatalf("unstaged rotation must refuse loudly")
	}

	if got := auditLatestRevision(t, ends.storeB, ends.sender, 9); got != 1 {
		t.Fatalf("refused rotation must keep revision 1, got %d", got)
	}

	if _, err = ends.storeB.Lookup(ends.sender, 9, 2); !errors.Is(err, ErrUnknownKey) {
		t.Fatalf("revision 2 must not exist before staging, got %v", err)
	}

	// Staged revisions accept: Revise then Activate installs revision 2
	// on the receiver, and the volatile retransmit reinstalls it
	// identically.
	auditStageReceivercry9(t, ends.storeB, ends.sender, ends.salt, rotated, 2)

	err = ends.exchB.SetRemoteTokens(envelope, ends.remote)
	if err != nil {
		t.Fatalf("staged rotation must install idempotently: %v", err)
	}

	if got := auditLatestRevision(t, ends.storeB, ends.sender, 9); got != 2 {
		t.Fatalf("staged rotation must install at revision 2, got %d", got)
	}

	stored, err := ends.storeB.Lookup(ends.sender, 9, 2)
	if err != nil {
		t.Fatalf("lookup revision 2: %v", err)
	}

	if !bytes.Equal(stored.SenderKey, rotated) {
		t.Fatalf("revision 2 must carry rotated key material")
	}

	// A retransmit of identical material must reinstall the same revision
	// instead of minting a third one.
	err = ends.exchB.SetRemoteTokens(envelope, ends.remote)
	if err != nil {
		t.Fatalf("retransmit must install idempotently: %v", err)
	}

	if got := auditLatestRevision(t, ends.storeB, ends.sender, 9); got != 2 {
		t.Fatalf("retransmit must keep revision 2, got %d", got)
	}

	if _, err = ends.storeB.Lookup(ends.sender, 9, 3); !errors.Is(err, ErrUnknownKey) {
		t.Fatalf("revision 3 must not exist, got %v", err)
	}
}

// TestAuditExchangeVolatileDecode pins decode under propagated revisions.
func TestAuditExchangeVolatileDecode(t *testing.T) {
	t.Parallel()

	storeA := NewKeyStore()
	storeB := NewKeyStore()
	sender := auditSender(0x59)

	var remote [16]byte

	copy(remote[:], bytes.Repeat([]byte{0x5A}, 16))

	salt := bytes.Repeat([]byte{0x5B}, 32)
	key := bytes.Repeat([]byte{0x5C}, 32)

	err := storeA.Store(MasterRecord{
		Sender: sender, KeyID: 4, Revision: 1, Kind: TransformAES256GCM,
		Salt: append([]byte(nil), salt...), SenderKey: append([]byte(nil), key...), Session: 1,
	})
	if err != nil {
		t.Fatalf("store master: %v", err)
	}

	exchangeA, err := NewKeyExchange(storeA, func([16]byte) bool { return true })
	if err != nil {
		t.Fatalf("new exchange A: %v", err)
	}

	exchangeB, err := NewKeyExchange(storeB, func([16]byte) bool { return true })
	if err != nil {
		t.Fatalf("new exchange B: %v", err)
	}

	envelope, err := exchangeA.CreateLocalTokens(sender, remote, 4, 1)
	if err != nil {
		t.Fatalf("create tokens: %v", err)
	}

	err = exchangeB.SetRemoteTokens(envelope, remote)
	if err != nil {
		t.Fatalf("set remote: %v", err)
	}

	transformA, err := NewTransform(storeA)
	if err != nil {
		t.Fatalf("new transform A: %v", err)
	}

	transformB, err := NewTransform(storeB)
	if err != nil {
		t.Fatalf("new transform B: %v", err)
	}

	header, ciphertext, err := transformA.Encode(sender, 4, 1, []byte("revision decode"), nil)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	plain, err := transformB.Decode(sender, 4, 1, header, ciphertext, nil)
	if err != nil {
		t.Fatalf("decode under revision 1: %v", err)
	}

	if string(plain) != "revision decode" {
		t.Fatalf("plaintext %q mismatch", plain)
	}
}
