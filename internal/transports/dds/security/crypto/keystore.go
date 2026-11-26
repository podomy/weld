// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

// Package crypto holds session keys and AES-GCM transforms.
package crypto

import (
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
)

// Sentinel errors for keystore failures.
var (
	// ErrUnknownKey reports a decode with no matching key.
	ErrUnknownKey = errors.New("crypto: unknown key")
	// ErrInvalidKey reports a malformed master record.
	ErrInvalidKey = errors.New("crypto: invalid key")
)

// Transform ids for the negotiated AES-GCM modes.
const (
	// TransformAES128GCM selects 128-bit AES-GCM.
	TransformAES128GCM uint32 = 1
	// TransformAES256GCM selects 256-bit AES-GCM.
	TransformAES256GCM uint32 = 2
	// defaultMaxBlocks bounds one session before forced rollover.
	defaultMaxBlocks uint64 = 1 << 24
)

// MasterRecord holds one master key tuple per 7.3.19 and 7.3.20.
type MasterRecord struct {
	// Sender names the key owner GUID.
	Sender [16]byte
	// KeyID is the transformation key id.
	KeyID uint32
	// Revision is the transformation key revision.
	Revision uint32
	// Kind selects the AES-GCM transform.
	Kind uint32
	// Salt is the 32 byte master salt.
	Salt []byte
	// SenderKey is the 16 or 32 byte master sender key.
	SenderKey []byte
	// ReceiverKeyID identifies the receiver specific key.
	ReceiverKeyID uint32
	// ReceiverKey holds the receiver specific key, empty when unused.
	ReceiverKey []byte
	// Session is the current session id for derivation.
	Session uint32
	// NextSuffix is the next 8 byte suffix for the IV.
	NextSuffix uint64
	// Blocks counts encoded blocks in the current session.
	Blocks uint64
}

// zeroBytes clears key material in place.
//
// Go offers no locked or pinned memory in the standard library, so
// copies made by Lookup, AllocateSuffix, or the garbage collector can
// linger until collection. This clears the bytes the store owns on
// every explicit removal; it cannot reach copies already handed out.
func zeroBytes(buf []byte) {
	for i := range buf {
		buf[i] = 0
	}
}

// zeroRecord clears the secret fields of one stored record.
func zeroRecord(record *MasterRecord) {
	if record == nil {
		return
	}

	zeroBytes(record.Salt)
	zeroBytes(record.SenderKey)
	zeroBytes(record.ReceiverKey)
}

// keyIndex names one master entry for the store map.
func keyIndex(sender [16]byte, keyID, revision uint32) string {
	var raw [24]byte

	copy(raw[:16], sender[:])
	binary.BigEndian.PutUint32(raw[16:20], keyID)
	binary.BigEndian.PutUint32(raw[20:24], revision)

	return string(raw[:])
}

// validateMaster checks salt and key sizes fail-closed.
//
// Receiver material per 7.3.20 is accepted and stored: an empty receiver
// key requires a zero receiver id, while a non-empty receiver key must
// be 16 or 32 bytes. Transform Encode always uses the sender key;
// Decode uses the sender key and falls back to the receiver key when the
// sender-key GCM open fails, so receiver-specific material decrypts
// where appropriate without silently dropping peer intent.
func validateMaster(record MasterRecord) error {
	if len(record.Salt) != 32 {
		return fmt.Errorf("crypto: salt need 32 have %d: %w", len(record.Salt), ErrInvalidKey)
	}

	if len(record.SenderKey) != 16 && len(record.SenderKey) != 32 {
		return fmt.Errorf("crypto: sender key need 16 or 32 have %d: %w", len(record.SenderKey), ErrInvalidKey)
	}

	if record.Kind != TransformAES128GCM && record.Kind != TransformAES256GCM {
		return fmt.Errorf("crypto: transform kind %d: %w", record.Kind, ErrInvalidKey)
	}

	if len(record.ReceiverKey) == 0 {
		if record.ReceiverKeyID != 0 {
			return fmt.Errorf("crypto: receiver id without receiver key: %w", ErrInvalidKey)
		}

		return nil
	}

	if len(record.ReceiverKey) != 16 && len(record.ReceiverKey) != 32 {
		return fmt.Errorf("crypto: receiver key need 16 or 32 have %d: %w", len(record.ReceiverKey), ErrInvalidKey)
	}

	return nil
}

// KeyStore holds master keys plus session allocation state.
type KeyStore struct {
	// mu guards every map below.
	mu sync.Mutex
	// masters maps index to master record.
	masters map[string]*MasterRecord
	// maxBlocks bounds one session before rollover.
	maxBlocks uint64
}

// NewKeyStore returns an empty store with the default block bound.
func NewKeyStore() *KeyStore {
	return &KeyStore{masters: make(map[string]*MasterRecord), maxBlocks: defaultMaxBlocks}
}

// NewKeyStoreWithLimit returns a store with an explicit block bound.
func NewKeyStoreWithLimit(maxBlocks uint64) *KeyStore {
	if maxBlocks == 0 {
		maxBlocks = defaultMaxBlocks
	}

	return &KeyStore{masters: make(map[string]*MasterRecord), maxBlocks: maxBlocks}
}

// Store installs one master record, copying key bytes.
func (s *KeyStore) Store(record MasterRecord) error {
	err := validateMaster(record)
	if err != nil {
		return err
	}

	cloned := record
	cloned.Salt = append([]byte(nil), record.Salt...)
	cloned.SenderKey = append([]byte(nil), record.SenderKey...)
	cloned.ReceiverKey = append([]byte(nil), record.ReceiverKey...)

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.masters == nil {
		s.masters = make(map[string]*MasterRecord)
	}

	s.masters[keyIndex(record.Sender, record.KeyID, record.Revision)] = &cloned

	return nil
}

// Lookup returns a copy of one master record for decode.
func (s *KeyStore) Lookup(sender [16]byte, keyID, revision uint32) (MasterRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	record, ok := s.masters[keyIndex(sender, keyID, revision)]
	if !ok {
		return MasterRecord{}, fmt.Errorf("crypto: no key for sender key %d revision %d: %w", keyID, revision, ErrUnknownKey)
	}

	cloned := *record
	cloned.Salt = append([]byte(nil), record.Salt...)
	cloned.SenderKey = append([]byte(nil), record.SenderKey...)
	cloned.ReceiverKey = append([]byte(nil), record.ReceiverKey...)

	return cloned, nil
}

// Remove deletes one master record and reports whether it existed.
//
// Removal zeroizes the stored salt plus sender and receiver keys
// before dropping the entry. Copies previously returned by Lookup
// cannot be reached and linger until garbage collection.
func (s *KeyStore) Remove(sender [16]byte, keyID, revision uint32) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	record, ok := s.masters[keyIndex(sender, keyID, revision)]
	if !ok {
		return false
	}

	zeroRecord(record)
	delete(s.masters, keyIndex(sender, keyID, revision))

	return true
}

// RemoveAllRevisions deletes every revision of one sender key.
//
// It zeroizes each removed record and returns the count removed, so
// participant teardown leaves no stale revision behind to decrypt
// traffic after eviction.
func (s *KeyStore) RemoveAllRevisions(sender [16]byte, keyID uint32) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	removed := 0

	for index, record := range s.masters {
		stored, ok := senderKeyOf(index)
		if !ok || stored != sender {
			continue
		}

		if keyIDOf(index) != keyID {
			continue
		}

		zeroRecord(record)
		delete(s.masters, index)
		removed++
	}

	return removed
}

// senderKeyOf extracts the sender GUID from one store index.
func senderKeyOf(index string) ([16]byte, bool) {
	var sender [16]byte

	if len(index) != 24 {
		return sender, false
	}

	copy(sender[:], index[:16])

	return sender, true
}

// keyIDOf extracts the key id from one store index.
func keyIDOf(index string) uint32 {
	if len(index) != 24 {
		return 0
	}

	return binary.BigEndian.Uint32([]byte(index[16:20]))
}

// LatestRevision reports the highest stored revision of one sender key.
func (s *KeyStore) LatestRevision(sender [16]byte, keyID uint32) (uint32, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var latest uint32

	found := false

	for index := range s.masters {
		stored, ok := senderKeyOf(index)
		if !ok || stored != sender {
			continue
		}

		if keyIDOf(index) != keyID {
			continue
		}

		revision := binary.BigEndian.Uint32([]byte(index[20:24]))
		if !found || revision > latest {
			latest = revision
			found = true
		}
	}

	return latest, found
}

// AllocateSuffix reserves the next session plus suffix for encode.
//
// Volume rotation is automatic: when the block counter exceeds the
// session bound a new session id is allocated and the counter resets.
// No token exchange is needed because receivers derive from master
// plus session id. Membership revision uses Revise plus Activate,
// never this path.
func (s *KeyStore) AllocateSuffix(sender [16]byte, keyID, revision uint32) (uint32, uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	record, ok := s.masters[keyIndex(sender, keyID, revision)]
	if !ok {
		return 0, 0, fmt.Errorf("crypto: allocate without key %d revision %d: %w", keyID, revision, ErrUnknownKey)
	}

	if record.Blocks >= s.maxBlocks {
		record.Session++
		if record.Session == 0 {
			record.Session = 1
		}

		record.NextSuffix = 0
		record.Blocks = 0
	}

	session := record.Session
	suffix := record.NextSuffix
	record.NextSuffix++
	record.Blocks++

	return session, suffix, nil
}

// ObserveBlocks adds encoded blocks without allocating, for tests.
func (s *KeyStore) ObserveBlocks(sender [16]byte, keyID, revision uint32, blocks uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	record, ok := s.masters[keyIndex(sender, keyID, revision)]
	if !ok {
		return fmt.Errorf("crypto: observe without key: %w", ErrUnknownKey)
	}

	record.Blocks += blocks

	return nil
}

// RolloverPendingcry9 reports whether the next encode will roll session.
//
// Volume rotation is automatic inside AllocateSuffix, so the transport
// polls this before encode to decide when to distribute session state.
// It returns true when the block counter reached the session bound.
// Missing keys fail closed with ErrUnknownKey.
//
// INTERFACE REQUEST: transport owns the SEC_PREFIX wrap path (backend.go
// plus spdp.go, not this package). Proposed wiring: in the wrap path
// that calls Transform.Encode, call RolloverPendingcry9(sender, keyID,
// revision) before encode; when true, emit a session-rollover signal on
// the volatile channel after encode so receivers can observe the new
// session id. Do not add distribution here; this package only reports.
func (s *KeyStore) RolloverPendingcry9(sender [16]byte, keyID, revision uint32) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	record, ok := s.masters[keyIndex(sender, keyID, revision)]
	if !ok {
		return false, fmt.Errorf("crypto: rollover query without key %d revision %d: %w", keyID, revision, ErrUnknownKey)
	}

	return record.Blocks >= s.maxBlocks, nil
}
