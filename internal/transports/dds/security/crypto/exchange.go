// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

package crypto

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/podomy/weld/internal/transports/dds/security/tokens"
)

// Sentinel errors for exchange failures.
var (
	// ErrDestinationMismatch reports a token for another participant.
	ErrDestinationMismatch = errors.New("crypto: destination mismatch")
	// ErrUnauthenticatedPeer reports exchange before handshake completion.
	ErrUnauthenticatedPeer = errors.New("crypto: unauthenticated peer")
	// ErrNoKxSession reports volatile exchange without a Kx seal session.
	ErrNoKxSession = errors.New("crypto: no kx session")
)

// KeyExchange moves key material over the volatile channel per 9.5.1.9.
type KeyExchange struct {
	// store installs received material for later decode.
	store *KeyStore
	// authenticated reports whether a peer completed the handshake.
	authenticated func(remote [16]byte) bool
	// mu guards kx sessions.
	mu sync.Mutex
	// kx holds Kx seal keys per destination per 10.5.2.1.2.
	kx map[[16]byte]kxKeys
}

// kxKeys holds one volatile seal session per 10.5.2.1.2.
type kxKeys struct {
	// salt is the 32 byte Kx salt.
	salt []byte
	// key is the 32 byte Kx seal key.
	key []byte
}

// NewKeyExchange returns an exchange over one store.
func NewKeyExchange(store *KeyStore, authenticated func(remote [16]byte) bool) (*KeyExchange, error) {
	if store == nil {
		return nil, fmt.Errorf("crypto: exchange without store: %w", ErrFactoryInvalid)
	}

	if authenticated == nil {
		authenticated = func([16]byte) bool { return false }
	}

	return &KeyExchange{store: store, authenticated: authenticated, kx: make(map[[16]byte]kxKeys)}, nil
}

// EstablishKx derives and stores one Kx seal session per 10.5.2.1.2.
//
// Challenge inputs plus the handshake secret derive the salt plus key via
// DeriveKxKeys; both peers deriving from shared inputs seal compatible
// envelopes. It fails closed on bad inputs without touching existing state.
func (e *KeyExchange) EstablishKx(destination [16]byte, challenge1, challenge2, secret []byte) error {
	salt, key, err := DeriveKxKeys(challenge1, challenge2, secret)
	if err != nil {
		return fmt.Errorf("crypto: establish kx: %w", err)
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.kx == nil {
		e.kx = make(map[[16]byte]kxKeys)
	}
	e.kx[destination] = kxKeys{salt: salt, key: key}
	return nil
}

// HasKx reports whether a Kx seal session exists for destination.
func (e *KeyExchange) HasKx(destination [16]byte) bool {
	if e == nil {
		return false
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	_, ok := e.kx[destination]
	return ok
}

// CreateSealedLocalTokens builds and Kx-seals one volatile envelope.
//
// It wraps CreateLocalTokens then SealVolatile; missing authentication
// or a missing Kx session fails closed without emitting clear key
// material per 9.5.1.9 and 10.5.2.1.2.
func (e *KeyExchange) CreateSealedLocalTokens(source, destination [16]byte, keyID, revision uint32) ([]byte, error) {
	envelope, err := e.CreateLocalTokens(source, destination, keyID, revision)
	if err != nil {
		return nil, err
	}
	sealed, err := e.SealVolatile(envelope)
	if err != nil {
		return nil, err
	}
	return sealed, nil
}

// SealVolatile seals one volatile envelope with the Kx key per 10.5.2.1.2.
//
// The envelope encodes via tokens.EncodeVolatile, then AES-GCM seals it
// with the destination Kx key and a fresh random nonce; output is nonce
// plus ciphertext. GUIDs verify after open fail-closed. Missing Kx
// sessions fail closed with ErrNoKxSession instead of sending clear.
func (e *KeyExchange) SealVolatile(envelope tokens.VolatileEnvelope) ([]byte, error) {
	e.mu.Lock()
	keys, ok := e.kx[envelope.DestinationGUID]
	e.mu.Unlock()
	if !ok {
		return nil, fmt.Errorf("crypto: seal volatile without kx for peer: %w", ErrNoKxSession)
	}
	plain, err := tokens.EncodeVolatile(envelope)
	if err != nil {
		return nil, fmt.Errorf("crypto: seal volatile encode: %w", err)
	}
	block, err := aes.NewCipher(keys.key)
	if err != nil {
		return nil, fmt.Errorf("crypto: seal volatile cipher: %w", err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("crypto: seal volatile gcm: %w", err)
	}
	var nonce [12]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return nil, fmt.Errorf("crypto: seal volatile nonce: %w", err)
	}
	sealed := gcm.Seal(nil, nonce[:], plain, nil)
	out := make([]byte, 0, len(nonce)+len(sealed))
	out = append(out, nonce[:]...)
	out = append(out, sealed...)
	return out, nil
}

// OpenVolatile opens one Kx-sealed volatile envelope per 10.5.2.1.2.
//
// It selects the Kx key by the expected source peer, opens the
// nonce-prefixed ciphertext, and decodes the envelope. Source mismatch
// or missing Kx sessions fail closed without installing keys.
func (e *KeyExchange) OpenVolatile(sealed []byte, expectedSource [16]byte) (tokens.VolatileEnvelope, error) {
	var out tokens.VolatileEnvelope
	if len(sealed) < 12 {
		return out, fmt.Errorf("crypto: open volatile truncated: %w", ErrNoKxSession)
	}
	e.mu.Lock()
	keys, ok := e.kx[expectedSource]
	e.mu.Unlock()
	if !ok {
		return out, fmt.Errorf("crypto: open volatile without kx for peer: %w", ErrNoKxSession)
	}
	var nonce [12]byte
	copy(nonce[:], sealed[:12])
	block, err := aes.NewCipher(keys.key)
	if err != nil {
		return out, fmt.Errorf("crypto: open volatile cipher: %w", err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return out, fmt.Errorf("crypto: open volatile gcm: %w", err)
	}
	plain, err := gcm.Open(nil, nonce[:], sealed[12:], nil)
	if err != nil {
		return out, fmt.Errorf("crypto: open volatile: %w", err)
	}
	decoded, err := tokens.DecodeVolatile(plain)
	if err != nil {
		return out, fmt.Errorf("crypto: open volatile decode: %w", err)
	}
	if decoded.SourceGUID != expectedSource {
		return out, fmt.Errorf("crypto: volatile source mismatch: %w", ErrDestinationMismatch)
	}
	return decoded, nil
}

// RolloverHoldSignal builds a hold-before-use signal when rollover pends.
//
// Per 9.5.1.9 the sender distributes the rollover signal on the volatile
// channel and holds the new session until peer ack or
// RolloverHoldTimeoutcry9. It reports held plus the signal envelope when
// store.RolloverPendingcry9 is true; held false needs no hold. Missing
// keys fail closed.
func (e *KeyExchange) RolloverHoldSignal(source, destination [16]byte, keyID, revision uint32) (bool, tokens.VolatileEnvelope, error) {
	pending, err := e.store.RolloverPendingcry9(source, keyID, revision)
	if err != nil {
		return false, tokens.VolatileEnvelope{}, err
	}
	if !pending {
		return false, tokens.VolatileEnvelope{}, nil
	}
	signal, err := e.CreateRolloverSignalcry9(source, destination, keyID, revision)
	if err != nil {
		return false, tokens.VolatileEnvelope{}, err
	}
	return true, signal, nil
}

// CreateLocalTokens wraps one master record for the volatile channel.
//
// INV-5: the caller must have completed the handshake for destination.
// Per 9.5.1.9 and 10.5.2.1.2 the wire distribution must seal via
// SealVolatile with the Kx session; SealVolatile fails closed with
// ErrNoKxSession instead of sending clear. This path never precedes
// registerMatchedRemote.
func (e *KeyExchange) CreateLocalTokens(source, destination [16]byte, keyID, revision uint32) (tokens.VolatileEnvelope, error) {
	var out tokens.VolatileEnvelope

	if !e.authenticated(destination) {
		return out, fmt.Errorf("crypto: create tokens for unauthenticated peer: %w", ErrUnauthenticatedPeer)
	}

	master, err := e.store.Lookup(source, keyID, revision)
	if err != nil {
		return out, err
	}

	material := tokens.KeyMaterial{
		TransformationKind: master.Kind,
		MasterSalt:         append([]byte(nil), master.Salt...),
		SenderKeyID:        master.KeyID,
		MasterSenderKey:    append([]byte(nil), master.SenderKey...),
		ReceiverKeyID:      master.ReceiverKeyID,
		MasterReceiverKey:  append([]byte(nil), master.ReceiverKey...),
	}

	out = tokens.VolatileEnvelope{
		DestinationGUID: destination,
		SourceGUID:      source,
		Tokens:          []tokens.KeyMaterial{material},
	}

	return out, nil
}

// SetRemoteTokens installs tokens from one volatile envelope.
//
// Cross-peer replays drop loudly when the destination names another
// participant. Volatile messages to unauthenticated peers drop loudly
// per INV-5. Neither path installs keys.
func (e *KeyExchange) SetRemoteTokens(envelope tokens.VolatileEnvelope, local [16]byte) error {
	if envelope.DestinationGUID != local {
		return fmt.Errorf("crypto: token destination mismatch: %w", ErrDestinationMismatch)
	}

	if !e.authenticated(envelope.SourceGUID) {
		return fmt.Errorf("crypto: volatile tokens from unauthenticated peer: %w", ErrUnauthenticatedPeer)
	}

	for i := range envelope.Tokens {
		material := envelope.Tokens[i]

		revision, err := e.nextRevision(envelope.SourceGUID, material)
		if err != nil {
			return fmt.Errorf("crypto: set remote token %d: %w", i, err)
		}

		record := MasterRecord{
			Sender:        envelope.SourceGUID,
			KeyID:         material.SenderKeyID,
			Revision:      revision,
			Kind:          material.TransformationKind,
			Salt:          append([]byte(nil), material.MasterSalt...),
			SenderKey:     append([]byte(nil), material.MasterSenderKey...),
			ReceiverKeyID: material.ReceiverKeyID,
			ReceiverKey:   append([]byte(nil), material.MasterReceiverKey...),
			Session:       1,
		}

		err = validateMaster(record)
		if err != nil {
			return fmt.Errorf("crypto: set remote token %d: %w", i, err)
		}

		err = e.store.Store(record)
		if err != nil {
			return fmt.Errorf("crypto: set remote token %d store: %w", i, err)
		}
	}

	return nil
}

// nextRevision selects the install revision for incoming key material.
//
// The first install lands at revision 1. A retransmit carrying
// material identical to the latest stored revision reinstalls that
// same revision idempotently. Novel unsolicited bytes never bump
// latest: they fail closed so an attacker cannot mint revisions.
// Legitimate rotation stages through factory Revise then Activate on
// the receiver store first; the volatile retransmit then matches the
// staged latest and reinstalls identically.
func (e *KeyExchange) nextRevision(sender [16]byte, material tokens.KeyMaterial) (uint32, error) {
	latest, ok := e.store.LatestRevision(sender, material.SenderKeyID)
	if !ok {
		return 1, nil
	}

	existing, err := e.store.Lookup(sender, material.SenderKeyID, latest)
	if err != nil {
		return 0, fmt.Errorf("crypto: read latest revision: %w", err)
	}

	if masterMatches(existing, material) {
		return latest, nil
	}

	return 0, fmt.Errorf("crypto: unsolicited revision material for key %d: %w", material.SenderKeyID, ErrFactoryInvalid)
}

// masterMatches reports whether stored and incoming material agree.
func masterMatches(stored MasterRecord, incoming tokens.KeyMaterial) bool {
	if stored.Kind != incoming.TransformationKind {
		return false
	}

	if stored.ReceiverKeyID != incoming.ReceiverKeyID {
		return false
	}

	if !bytes.Equal(stored.Salt, incoming.MasterSalt) {
		return false
	}

	if !bytes.Equal(stored.SenderKey, incoming.MasterSenderKey) {
		return false
	}

	return bytes.Equal(stored.ReceiverKey, incoming.MasterReceiverKey)
}

// CreateLocalParticipantTokens names the participant token path.
func (e *KeyExchange) CreateLocalParticipantTokens(source, destination [16]byte, keyID, revision uint32) (tokens.VolatileEnvelope, error) {
	envelope, err := e.CreateLocalTokens(source, destination, keyID, revision)
	if err != nil {
		return tokens.VolatileEnvelope{}, fmt.Errorf("crypto: create participant tokens: %w", err)
	}

	return envelope, nil
}

// RolloverHoldTimeoutcry9 bounds holding a new session before use.
//
// A sender that rolls its session must distribute the rollover signal on
// the volatile channel and hold the new session until the peer acks or
// this timeout elapses; only then may it encode under the new session.
// Receivers derive from master plus session id, so the signal carries
// the current revision material and the sender retries until ack.
const RolloverHoldTimeoutcry9 = 5 * time.Second

// CreateRolloverSignalcry9 builds the volatile signal announcing new material.
//
// It wraps CreateLocalTokens for the active revision; callers emit the
// envelope on the volatile channel before encoding under rolled sessions
// or revised keys, and hold the new keys until peer ack or timeout per
// 9.5.1.9. Membership rekey when governance EnableKeyRevision is true
// stages via factory Revise, distributes via this signal, then Activates
// on ack.
func (e *KeyExchange) CreateRolloverSignalcry9(source, destination [16]byte, keyID, revision uint32) (tokens.VolatileEnvelope, error) {
	envelope, err := e.CreateLocalTokens(source, destination, keyID, revision)
	if err != nil {
		return tokens.VolatileEnvelope{}, fmt.Errorf("crypto: rollover signal: %w", err)
	}

	return envelope, nil
}

// SetRemoteParticipantTokens names the participant install path.
func (e *KeyExchange) SetRemoteParticipantTokens(envelope tokens.VolatileEnvelope, local [16]byte) error {
	err := e.SetRemoteTokens(envelope, local)
	if err != nil {
		return fmt.Errorf("crypto: set remote participant tokens: %w", err)
	}

	return nil
}

// CreateLocalDatawriterTokens names the writer token path.
func (e *KeyExchange) CreateLocalDatawriterTokens(source, destination [16]byte, keyID, revision uint32) (tokens.VolatileEnvelope, error) {
	envelope, err := e.CreateLocalTokens(source, destination, keyID, revision)
	if err != nil {
		return tokens.VolatileEnvelope{}, fmt.Errorf("crypto: create writer tokens: %w", err)
	}

	return envelope, nil
}

// SetRemoteDatawriterTokens names the writer install path.
func (e *KeyExchange) SetRemoteDatawriterTokens(envelope tokens.VolatileEnvelope, local [16]byte) error {
	err := e.SetRemoteTokens(envelope, local)
	if err != nil {
		return fmt.Errorf("crypto: set remote writer tokens: %w", err)
	}

	return nil
}
