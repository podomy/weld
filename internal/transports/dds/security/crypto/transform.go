// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

package crypto

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"

	"github.com/podomy/weld/internal/transports/dds/security/tokens"
)

// Sentinel errors for transform failures.
var (
	// ErrReplay reports a reused session plus suffix nonce.
	ErrReplay = errors.New("crypto: replay")
	// ErrTransformInvalid reports a malformed header or key size.
	ErrTransformInvalid = errors.New("crypto: invalid transform")
)

// DeriveKxKeys derives the volatile channel keys per DDS Security 10.5.2.1.2
// (Table 67 in 1.1: master_salt and master_sender_key for the
// BuiltinParticipantVolatileMessageSecure channel).
//
// Both outputs route through the RFC 5869 HKDF in hkdf.go with the
// handshake secret as IKM and challenge plus label framing as info:
// master_salt info is challenge1 plus "keyexchange salt" plus challenge2,
// sender key info is challenge2 plus "key exchange key" plus challenge1.
// Cookie spellings are 16 bytes each and match the Fast-DDS
// AESGCMGMAC_KeyFactory interop: "keyexchange salt" (no space between
// key and exchange) for the salt and "key exchange key" (spaces) for the
// sender key. Order is fixed: salt binds challenge1 first, sender key
// binds challenge2 first, so swapped challenges derive different keys.
// A nil HKDF salt selects RFC 5869 zero salt. Interop vectors in
// kxcry9_test pin both outputs for fixed challenges plus secret; both
// parties derive identical keys from shared inputs and challenge
// sensitivity is pinned. Fixed 32 byte outputs feed AES-256-GCM; the
// receiver key stays zero here and arrives later over the volatile
// channel per 7.3.20.
func DeriveKxKeys(challenge1, challenge2, secret []byte) ([]byte, []byte, error) {
	if len(challenge1) != 32 || len(challenge2) != 32 {
		return nil, nil, fmt.Errorf("crypto: challenges need 32: %w", ErrTransformInvalid)
	}

	if len(secret) == 0 {
		return nil, nil, fmt.Errorf("crypto: empty handshake secret: %w", ErrTransformInvalid)
	}

	saltInfo := appendChallengeLabel(challenge1, "keyexchange salt", challenge2)

	salt, err := HKDF(nil, secret, saltInfo, 32)
	if err != nil {
		return nil, nil, fmt.Errorf("crypto: derive kx salt: %w", err)
	}

	keyInfo := appendChallengeLabel(challenge2, "key exchange key", challenge1)

	key, err := HKDF(nil, secret, keyInfo, 32)
	if err != nil {
		return nil, nil, fmt.Errorf("crypto: derive kx key: %w", err)
	}

	return salt, key, nil
}

// appendChallengeLabel builds challenge plus label plus challenge bytes.
func appendChallengeLabel(first []byte, label string, second []byte) []byte {
	out := make([]byte, 0, len(first)+len(label)+len(second))
	out = append(out, first...)
	out = append(out, label...)
	out = append(out, second...)

	return out
}

// DeriveSessionKey derives one session key per 10.5.3.3.3.
//
// sessionKey is HMAC256(masterKey, label plus masterSalt plus session).
// The session id encodes big-endian. Callers truncate for 128-bit use.
func DeriveSessionKey(masterKey, masterSalt []byte, session uint32, label string) ([]byte, error) {
	if len(masterKey) != 16 && len(masterKey) != 32 {
		return nil, fmt.Errorf("crypto: master key need 16 or 32 have %d: %w", len(masterKey), ErrTransformInvalid)
	}

	if len(masterSalt) != 32 {
		return nil, fmt.Errorf("crypto: master salt need 32 have %d: %w", len(masterSalt), ErrTransformInvalid)
	}

	mac := hmac.New(sha256.New, masterKey)
	_, _ = mac.Write([]byte(label)) //nolint:errcheck // hash writes never fail
	_, _ = mac.Write(masterSalt)    //nolint:errcheck // hash writes never fail

	var raw [4]byte

	binary.BigEndian.PutUint32(raw[:], session)
	_, _ = mac.Write(raw[:]) //nolint:errcheck // hash writes never fail

	return mac.Sum(nil), nil
}

// sessionNonce builds the 12 byte GCM nonce from session plus suffix.
func sessionNonce(session uint32, suffix uint64) [12]byte {
	var out [12]byte

	binary.BigEndian.PutUint32(out[:4], session)
	binary.BigEndian.PutUint64(out[4:12], suffix)

	return out
}

// MaxSeenEntries bounds the replay window before oldest-first eviction.
//
// The bound keeps a busy receiver from growing memory without limit.
// Eviction reopens the replay window for the evicted nonce, so the
// value stays large enough that reuse of an evicted session plus
// suffix pair is impractical while the key is live.
const MaxSeenEntries = 1 << 16

// Transform encrypts DATA payloads with AES-GCM session keys.
type Transform struct {
	// store holds master keys for encode plus decode.
	store *KeyStore
	// mu guards seen nonces for replay detection.
	mu sync.Mutex
	// seen tracks decoded nonces per sender plus key plus session.
	seen map[string]struct{}
	// order holds seen keys oldest first for eviction.
	order []string
	// start is the first live entry in order.
	start int
}

// NewTransform returns a transform over one store.
func NewTransform(store *KeyStore) (*Transform, error) {
	if store == nil {
		return nil, fmt.Errorf("crypto: transform without store: %w", ErrTransformInvalid)
	}

	return &Transform{store: store, seen: make(map[string]struct{})}, nil
}

// seenKey names one decoded nonce for replay detection.
func seenKey(sender [16]byte, keyID, revision, session uint32, suffix uint64) string {
	var raw [36]byte

	copy(raw[:16], sender[:])
	binary.BigEndian.PutUint32(raw[16:20], keyID)
	binary.BigEndian.PutUint32(raw[20:24], revision)
	binary.BigEndian.PutUint32(raw[24:28], session)
	binary.BigEndian.PutUint64(raw[28:36], suffix)

	return string(raw[:])
}

// gcmForKey returns one AES-GCM handle for a session key.
func gcmForKey(sessionKey []byte, kind uint32) (cipher.AEAD, error) {
	key := sessionKey
	if kind == TransformAES128GCM && len(sessionKey) > 16 {
		key = sessionKey[:16]
	}

	if kind == TransformAES256GCM && len(sessionKey) != 32 {
		return nil, fmt.Errorf("crypto: AES256 session key need 32 have %d: %w", len(sessionKey), ErrTransformInvalid)
	}

	if kind == TransformAES128GCM && len(key) != 16 {
		return nil, fmt.Errorf("crypto: AES128 session key need 16 have %d: %w", len(key), ErrTransformInvalid)
	}

	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("crypto: new cipher: %w", err)
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("crypto: new gcm: %w", err)
	}

	return gcm, nil
}

// Encode encrypts one payload under the sender master key.
//
// It allocates the next session plus suffix, derives the session key,
// seals with the 16 byte header as additional data, and returns header
// plus ciphertext. Missing keys fail closed with ErrUnknownKey.
func (t *Transform) Encode(sender [16]byte, keyID, revision uint32, plaintext, aad []byte) ([]byte, []byte, error) {
	master, err := t.store.Lookup(sender, keyID, revision)
	if err != nil {
		return nil, nil, err
	}

	session, suffix, err := t.store.AllocateSuffix(sender, keyID, revision)
	if err != nil {
		return nil, nil, err
	}

	label := "SessionKey"

	sessionKey, err := DeriveSessionKey(master.SenderKey, master.Salt, session, label)
	if err != nil {
		return nil, nil, err
	}

	gcm, err := gcmForKey(sessionKey, master.Kind)
	if err != nil {
		return nil, nil, err
	}

	var suffixBytes [8]byte

	binary.BigEndian.PutUint64(suffixBytes[:], suffix)

	header := tokens.CryptoHeader{TransformationKind: master.Kind, SessionID: session, IVSuffix: suffixBytes}
	headerBytes := tokens.EncodeCryptoHeader(header)

	nonce := sessionNonce(session, suffix)

	fullAAD := append(append([]byte(nil), headerBytes...), aad...)
	ciphertext := gcm.Seal(nil, nonce[:], plaintext, fullAAD)

	return headerBytes, ciphertext, nil
}

// Decode decrypts one payload, failing closed on missing or replayed keys.
//
// Unknown key ids drop loudly with ErrUnknownKey so callers bump the
// secured-transform counter family. Reused nonces drop with ErrReplay.
// Tampered tags fail inside AES-GCM Open. When the sender-key open fails
// and receiver-specific material is stored, it retries once with the
// receiver key per 7.3.20; both failures drop without plaintext and the
// nonce claim releases because no plaintext was delivered.
func (t *Transform) Decode(sender [16]byte, keyID, revision uint32, headerBytes, ciphertext, aad []byte) ([]byte, error) {
	header, err := tokens.DecodeCryptoHeader(headerBytes)
	if err != nil {
		return nil, fmt.Errorf("crypto: decode header: %w", err)
	}

	master, err := t.store.Lookup(sender, keyID, revision)
	if err != nil {
		return nil, err
	}

	if header.TransformationKind != master.Kind {
		return nil, fmt.Errorf("crypto: transform kind %d against key kind %d: %w", header.TransformationKind, master.Kind, ErrTransformInvalid)
	}

	suffix := binary.BigEndian.Uint64(header.IVSuffix[:])

	key := seenKey(sender, keyID, revision, header.SessionID, suffix)

	// The check plus insert hold one critical section: the nonce is
	// claimed before decryption, so two concurrent decodes of the
	// same bytes cannot both pass the check. A failed decryption
	// releases the claim because no plaintext was delivered.
	t.mu.Lock()
	if _, seen := t.seen[key]; seen {
		t.mu.Unlock()

		return nil, fmt.Errorf("crypto: replay session %d suffix %d: %w", header.SessionID, suffix, ErrReplay)
	}

	t.seen[key] = struct{}{}
	t.order = append(t.order, key)
	t.evictLocked()
	t.mu.Unlock()

	nonce := sessionNonce(header.SessionID, suffix)
	fullAAD := append(append([]byte(nil), headerBytes...), aad...)

	plaintext, err := openWithKey(master.SenderKey, master.Salt, master.Kind, header.SessionID, nonce, fullAAD, ciphertext)
	if err == nil {
		return plaintext, nil
	}

	senderErr := err

	if len(master.ReceiverKey) != 0 {
		plaintext, err = openWithKey(master.ReceiverKey, master.Salt, master.Kind, header.SessionID, nonce, fullAAD, ciphertext)
		if err == nil {
			return plaintext, nil
		}
	}

	t.releaseClaim(key)

	return nil, fmt.Errorf("crypto: gcm open: %w", senderErr)
}

// openWithKey derives one session key and opens one payload.
func openWithKey(keyBytes, salt []byte, kind, session uint32, nonce [12]byte, fullAAD, ciphertext []byte) ([]byte, error) {
	sessionKey, err := DeriveSessionKey(keyBytes, salt, session, "SessionKey")
	if err != nil {
		return nil, fmt.Errorf("crypto: derive session key: %w", err)
	}

	gcm, err := gcmForKey(sessionKey, kind)
	if err != nil {
		return nil, fmt.Errorf("crypto: session cipher: %w", err)
	}

	out, err := gcm.Open(nil, nonce[:], ciphertext, fullAAD)
	if err != nil {
		return nil, fmt.Errorf("crypto: gcm open: %w", err)
	}

	return out, nil
}

// evictLocked drops oldest nonces past the bound; caller holds mu.
func (t *Transform) evictLocked() {
	for len(t.order)-t.start > MaxSeenEntries {
		delete(t.seen, t.order[t.start])
		t.order[t.start] = ""
		t.start++
	}

	if t.start > 4096 && t.start*2 > len(t.order) {
		kept := append([]string(nil), t.order[t.start:]...)
		t.order = kept
		t.start = 0
	}
}

// releaseClaim drops one nonce claim after a failed decryption.
func (t *Transform) releaseClaim(key string) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if _, ok := t.seen[key]; !ok {
		return
	}

	delete(t.seen, key)

	for i := t.start; i < len(t.order); i++ {
		if t.order[i] == key {
			t.order[i] = ""
		}
	}
}

// SeenCount reports the number of tracked nonces for audit.
func (t *Transform) SeenCount() int {
	if t == nil {
		return 0
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	return len(t.seen)
}

// INTERFACE REQUEST: origin AAD binds the sender GUID into the GCM
// additional data so a ciphertext cut from one origin cannot verify
// under another. This package already carries headerBytes plus aad
// through Encode and Decode, but the submessage paths in backend.go
// and spdp.go (not this package) currently seal and open with an
// empty AAD. Proposed exact design for the owning batch:
// func originAADcry9(origin [16]byte, headerBytes, externalAAD []byte) []byte
// returning origin[:] plus headerBytes plus externalAAD, then
// backend.go secWrapSubmessage calls gcm.Seal with originAADcry9 as
// the aad, and secUnwrapSplit plus secOpenEncryptedBody plus
// secVerifySignedBody call gcm.Open with the same originAADcry9.
// Origin comes from the SEC_PREFIX sender key lookup, never from the
// body. Mismatched origins fail closed inside GCM Open.
//
// INTERFACE REQUEST: SRTPS egress protects whole RTPS messages with
// SRTPS_PREFIX plus body plus SRTPS_POSTFIX, distinct from the
// submessage SEC_PREFIX triple this package models. Proposed exact
// design for the owning batch in backend.go:
// func secWrapSRTPSmessagecry9(kind, transformID, session uint32, suffix uint64, sessionKey, plainMessage []byte) ([]byte, error)
// sealing the full framed RTPS message with originAADcry9 as aad,
// and func secUnwrapSRTPSmessagecry9(framedMessage []byte, lookup func(transformID, session uint32) (key, salt []byte, err error), origin [16]byte) ([]byte, error)
// opening it. Session keys derive via DeriveSessionKey as today. Do
// not touch backend.go or spdp.go from this package.
