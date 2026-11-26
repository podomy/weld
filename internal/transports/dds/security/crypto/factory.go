// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

package crypto

import (
	"errors"
	"fmt"

	"github.com/podomy/weld/internal/transports/dds/security/tokens"
)

// Sentinel errors for factory failures.
var (
	// ErrFactoryInvalid reports a bad factory operation.
	ErrFactoryInvalid = errors.New("crypto: invalid factory operation")
)

// KeyFactory registers entity keys per 9.5.1.8.
type KeyFactory struct {
	// store holds master keys for local plus matched remotes.
	store *KeyStore
	// pending holds revised material awaiting activation.
	pending map[string]MasterRecord
	// active maps entity index to active revision.
	active map[string]uint32
}

// NewKeyFactory returns a factory over one store.
func NewKeyFactory(store *KeyStore) (*KeyFactory, error) {
	if store == nil {
		return nil, fmt.Errorf("crypto: factory without store: %w", ErrFactoryInvalid)
	}

	return &KeyFactory{store: store, pending: make(map[string]MasterRecord), active: make(map[string]uint32)}, nil
}

// Store returns the backing key store.
func (f *KeyFactory) Store() *KeyStore {
	return f.store
}

// localKey builds a master record from token material.
func localKey(sender [16]byte, revision uint32, material tokens.KeyMaterial) (MasterRecord, error) {
	if len(material.MasterSalt) != 32 {
		return MasterRecord{}, fmt.Errorf("crypto: register salt need 32: %w", ErrInvalidKey)
	}

	return MasterRecord{
		Sender:      sender,
		KeyID:       material.SenderKeyID,
		Revision:    revision,
		Kind:        material.TransformationKind,
		Salt:        append([]byte(nil), material.MasterSalt...),
		SenderKey:   append([]byte(nil), material.MasterSenderKey...),
		ReceiverKey: append([]byte(nil), material.MasterReceiverKey...),
		Session:     1,
	}, nil
}

// RegisterLocalParticipant installs the local participant master key.
func (f *KeyFactory) RegisterLocalParticipant(sender [16]byte, material tokens.KeyMaterial, revision uint32) error {
	record, err := localKey(sender, revision, material)
	if err != nil {
		return err
	}

	if f.active == nil {
		f.active = make(map[string]uint32)
	}

	err = f.store.Store(record)
	if err != nil {
		return fmt.Errorf("crypto: register local participant: %w", err)
	}

	f.active[string(sender[:])+"p"] = revision

	return nil
}

// RegisterMatchedRemoteParticipant binds the shared secret derivation input.
//
// INV-5: no crypto token is created for or sent to a peer whose
// handshake has not completed. Callers must gate on handshake state
// before invoking this path; the store itself never authenticates.
func (f *KeyFactory) RegisterMatchedRemoteParticipant(remote [16]byte, keyID, revision uint32, record MasterRecord) error {
	if f.store == nil {
		return fmt.Errorf("crypto: register without store: %w", ErrFactoryInvalid)
	}

	record.Sender = remote
	record.KeyID = keyID
	record.Revision = revision

	err := f.store.Store(record)
	if err != nil {
		return fmt.Errorf("crypto: register matched remote: %w", err)
	}

	return nil
}

// RegisterLocalDatawriter installs one local writer master key.
func (f *KeyFactory) RegisterLocalDatawriter(sender [16]byte, material tokens.KeyMaterial, revision uint32) error {
	record, err := localKey(sender, revision, material)
	if err != nil {
		return err
	}

	err = f.store.Store(record)
	if err != nil {
		return fmt.Errorf("crypto: register local writer: %w", err)
	}

	return nil
}

// RegisterLocalDatareader installs one local reader master key.
func (f *KeyFactory) RegisterLocalDatareader(sender [16]byte, material tokens.KeyMaterial, revision uint32) error {
	record, err := localKey(sender, revision, material)
	if err != nil {
		return err
	}

	err = f.store.Store(record)
	if err != nil {
		return fmt.Errorf("crypto: register local reader: %w", err)
	}

	return nil
}

// UnregisterParticipant removes all revisions for one participant.
//
// Every revision stored under the sender key is zeroized and dropped:
// leaving any revision behind would let undecryptable traffic linger
// or, worse, let a stale key keep decoding after eviction. The key id
// and revision arguments select the key family; all revisions of that
// family are removed. It reports whether anything was removed.
func (f *KeyFactory) UnregisterParticipant(sender [16]byte, keyID, revision uint32) bool {
	_ = revision

	return f.store.RemoveAllRevisions(sender, keyID) > 0
}

// Revise stages revised master material without activating it.
//
// Encode keeps using the old revision until ActivateKeyRevision runs.
func (f *KeyFactory) Revise(sender [16]byte, revision uint32, material tokens.KeyMaterial) error {
	record, err := localKey(sender, revision, material)
	if err != nil {
		return err
	}

	if f.pending == nil {
		f.pending = make(map[string]MasterRecord)
	}

	f.pending[keyIndex(sender, record.KeyID, revision)] = record

	return nil
}

// Activate installs staged material so later encodes use the new revision.
func (f *KeyFactory) Activate(sender [16]byte, keyID, revision uint32) error {
	key := keyIndex(sender, keyID, revision)

	record, ok := f.pending[key]
	if !ok {
		return fmt.Errorf("crypto: activate without revised material: %w", ErrFactoryInvalid)
	}

	err := f.store.Store(record)
	if err != nil {
		return fmt.Errorf("crypto: activate store: %w", err)
	}

	delete(f.pending, key)

	return nil
}

// HasPendingcry9 reports whether staged material waits for activation.
//
// The membership hook polls this to decide when a Revise needs an
// Activate. It returns false for a nil factory.
//
// INTERFACE REQUEST: membership lives in sedp.go plus backend.go, not
// this package. Proposed wiring: when the access governance rule for
// the domain has EnableKeyRevision true and the SEDP index evicts or
// admits a matched remote endpoint, call
// factory.Revise(sender, nextRevision, freshMaterial) then distribute
// the new tokens on the volatile channel, then call
// factory.Activate(sender, keyID, nextRevision) on local ack. Do not
// call Activate without a prior Revise; it fails closed. Do not touch
// transport files from this package.
func (f *KeyFactory) HasPendingcry9(sender [16]byte, keyID, revision uint32) bool {
	if f == nil {
		return false
	}

	_, ok := f.pending[keyIndex(sender, keyID, revision)]

	return ok
}
