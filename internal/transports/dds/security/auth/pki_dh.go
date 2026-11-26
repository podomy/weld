// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

// Package auth holds the ECDHE handshake over the stateless channel.
package auth

import (
	"crypto"
	"crypto/ecdh"
	"crypto/ecdsa"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"errors"
	"fmt"
)

// Sentinel errors for handshake failures.
var (
	// ErrHandshakeFailed reports a failed handshake step.
	ErrHandshakeFailed = errors.New("auth: handshake failed")
	// ErrSignatureUnverified reports that no document signature verified.
	ErrSignatureUnverified = errors.New("auth: signature unverified")
	// ErrUnknownAlgorithm reports a peer requiring an unsupported algorithm.
	ErrUnknownAlgorithm = errors.New("auth: unknown algorithm")
	// ErrStaleHandshake reports a replayed handshake message.
	ErrStaleHandshake = errors.New("auth: stale handshake")
	// ErrForgedHandshake reports a challenge echo mismatch.
	ErrForgedHandshake = errors.New("auth: forged handshake")
)

// ValidationResult names the 9.3.2.11 handshake outcome.
//
// Signature verification stays REJECT-ALL until Phase 4: no identity
// certificate chain verifies yet, so flag-on full-auth networks cannot
// complete authentication. The ECDHE secret below still derives for
// loopback tests, but production decisions must treat anything short of
// a verified signature as failure and never fake VALIDATION_OK.
type ValidationResult int

// Handshake validation outcomes.
const (
	// ValidationFailed denies the peer.
	ValidationFailed ValidationResult = iota
	// ValidationPendingRequest waits for the peer request.
	ValidationPendingRequest
	// ValidationPendingMessage waits for the peer reply or final.
	ValidationPendingMessage
	// ValidationOKFinalMessage completes the initiator side.
	ValidationOKFinalMessage
	// ValidationOK completes the replier side.
	ValidationOK
)

// Supported key agreement names. MODP has no verified stdlib support,
// so peers requiring MODP-only fail closed per INV-8.
const (
	// KAgreeP256 selects ECDHE on P-256.
	KAgreeP256 = "ECDHE-CEUM+P256"
	// KAgreeP384 selects ECDHE on P-384.
	KAgreeP384 = "ECDHE-CEUM+P384"
	// KAgreeMODP is rejected; no DH implementation is invented here.
	KAgreeMODP = "DHE+MODP-2048-256"
)

// curveForName returns the ECDH curve for a supported agreement name.
//
// Both the historical Weld names and the OMG wire names from section
// 10.5 are accepted so Weld peers and CycloneDDS peers negotiate the
// same group.
func curveForName(name string) (ecdh.Curve, error) {
	switch name {
	case KAgreeP256, "ECDH+prime256v1-CEUM":
		return ecdh.P256(), nil
	case KAgreeP384:
		return ecdh.P384(), nil
	default:
		return nil, fmt.Errorf("auth: key agreement %q: %w", name, ErrUnknownAlgorithm)
	}
}

// Authenticator holds one participant ECDHE identity.
type Authenticator struct {
	// curve pins the ECDH group for this identity.
	curve ecdh.Curve
	// private holds the local ephemeral key. It never leaves this struct
	// and is never logged or stored.
	private *ecdh.PrivateKey
	// name records the agreement algorithm for tokens.
	name string
}

// NewAuthenticator generates one ephemeral ECDHE identity.
//
// An empty agreement name fails closed through curveForName instead of
// defaulting: callers must name the group they can complete.
func NewAuthenticator(kagree string) (*Authenticator, error) {
	curve, err := curveForName(kagree)
	if err != nil {
		return nil, err
	}

	private, err := curve.GenerateKey(rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("auth: generate key: %w", err)
	}

	return &Authenticator{curve: curve, private: private, name: kagree}, nil
}

// kagreeCurveID returns the canonical group for one agreement name.
//
// Both the historical Weld names and the OMG wire names map to the same
// group, so negotiation compares groups instead of strings: a P-256 peer
// offering either spelling matches a P-256 identity, while a P-384 offer
// against P-256 keys fails even though both names are known.
func kagreeCurveID(name string) (string, error) {
	switch name {
	case KAgreeP256, "ECDH+prime256v1-CEUM":
		return "P-256", nil
	case KAgreeP384:
		return "P-384", nil
	default:
		return "", fmt.Errorf("auth: key agreement %q: %w", name, ErrUnknownAlgorithm)
	}
}

// PublicKey returns the local ECDHE public key bytes.
func (a *Authenticator) PublicKey() ([]byte, error) {
	if a == nil || a.private == nil {
		return nil, fmt.Errorf("auth: public key without identity: %w", ErrHandshakeFailed)
	}

	return a.private.PublicKey().Bytes(), nil
}

// KAgreeName returns the agreement algorithm name.
func (a *Authenticator) KAgreeName() string {
	if a == nil {
		return KAgreeP256
	}

	return a.name
}

// SharedSecret derives the ECDHE secret against one peer public key.
//
// The peer bytes must decode on the local curve. Errors fail closed.
func (a *Authenticator) SharedSecret(peerPublic []byte) ([]byte, error) {
	if a == nil || a.private == nil {
		return nil, fmt.Errorf("auth: shared secret without identity: %w", ErrHandshakeFailed)
	}

	peer, err := a.curve.NewPublicKey(peerPublic)
	if err != nil {
		return nil, fmt.Errorf("auth: decode peer key: %w", errors.Join(err, ErrHandshakeFailed))
	}

	secret, err := a.private.ECDH(peer)
	if err != nil {
		return nil, fmt.Errorf("auth: derive secret: %w", errors.Join(err, ErrHandshakeFailed))
	}

	return secret, nil
}

// NewChallenge returns one 256-bit random nonce.
func NewChallenge() ([32]byte, error) {
	var out [32]byte

	n, err := rand.Read(out[:])
	if err != nil {
		return out, fmt.Errorf("auth: random challenge: %w", err)
	}

	if n != len(out) {
		return out, fmt.Errorf("auth: short random read %d: %w", n, ErrHandshakeFailed)
	}

	return out, nil
}

// CheckAlgorithm fails closed on MODP-only or unknown agreement names.
func CheckAlgorithm(name string) error {
	_, err := curveForName(name)
	if err != nil {
		return err
	}

	if name == KAgreeMODP {
		return fmt.Errorf("auth: MODP has no verified stdlib support: %w", ErrUnknownAlgorithm)
	}

	return nil
}

// VerifySignature verifies raw handshake bytes with an identity certificate.
//
// The first argument carries a DER or PEM identity certificate, the
// second the signed bytes, and the third the signature. RSA verifies
// PKCS1v15 SHA-256 and ECDSA verifies ASN1 SHA-256. Chain building,
// expiry, and key usage are owned by ValidateIdentity in x509.go; this
// helper only proves possession of the certified key. Nil or malformed
// inputs fail closed with ErrSignatureUnverified.
func VerifySignature(certDER, data, signature []byte) error {
	if len(certDER) == 0 || len(data) == 0 || len(signature) == 0 {
		return fmt.Errorf("auth: signature inputs must not be empty: %w", ErrSignatureUnverified)
	}

	cert, err := ParseCertificatePEM(certDER)
	if err != nil {
		return err
	}

	digest := sha256.Sum256(data)

	switch pub := cert.PublicKey.(type) {
	case *rsa.PublicKey:
		err := rsa.VerifyPKCS1v15(pub, crypto.SHA256, digest[:], signature)
		if err != nil {
			return fmt.Errorf("auth: rsa handshake signature mismatch: %w", errors.Join(err, ErrSignatureUnverified))
		}

		return nil
	case *ecdsa.PublicKey:
		if !ecdsa.VerifyASN1(pub, digest[:], signature) {
			return fmt.Errorf("auth: ecdsa handshake signature mismatch: %w", ErrSignatureUnverified)
		}

		return nil
	default:
		return fmt.Errorf("auth: unsupported handshake key type: %w", ErrSignatureUnverified)
	}
}

// VerifyChallengeEcho rejects forged challenge echoes loudly.
func VerifyChallengeEcho(sent, echoed [32]byte) error {
	if sent != echoed {
		return fmt.Errorf("auth: challenge echo mismatch: %w", ErrForgedHandshake)
	}

	return nil
}
