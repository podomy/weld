// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

// Package auth holds the ECDHE handshake over the stateless channel.
package auth

import (
	"crypto/sha256"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"time"
)

// Sentinel errors for identity failures.
var (
	// ErrIdentityUnverified reports a failed X.509 path validation.
	ErrIdentityUnverified = errors.New("auth: identity unverified")
	// ErrIdentityExpired reports an expired identity outside its window.
	ErrIdentityExpired = errors.New("auth: identity expired")
	// ErrIdentityKeyUsage reports a forbidden key usage.
	ErrIdentityKeyUsage = errors.New("auth: identity key usage")
)

// ParseCertificatePEM decodes one PEM or DER certificate.
func ParseCertificatePEM(data []byte) (*x509.Certificate, error) {
	if block, _ := pem.Decode(data); block != nil {
		cert, err := x509.ParseCertificate(block.Bytes)
		if err != nil {
			return nil, fmt.Errorf("auth: parse certificate pem: %w", errors.Join(err, ErrIdentityUnverified))
		}

		return cert, nil
	}

	cert, err := x509.ParseCertificate(data)
	if err != nil {
		return nil, fmt.Errorf("auth: parse certificate der: %w", errors.Join(err, ErrIdentityUnverified))
	}

	return cert, nil
}

// BuildCAPool builds a trust pool from PEM certificates.
func BuildCAPool(caPEM []byte) (*x509.CertPool, error) {
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caPEM) {
		return nil, fmt.Errorf("auth: no CA certificates parsed: %w", ErrIdentityUnverified)
	}

	return pool, nil
}

// CertificateSubjectFullauth7 renders the canonical full DN for binding.
//
// It always returns the full subject string, so same CN values with
// different organizations differ here. All grant lookups must use this
// full form per 9.4; CN-only matching would conflate colliding CNs and
// is removed fail-closed.
func CertificateSubjectFullauth7(cert *x509.Certificate) string {
	if cert == nil {
		return ""
	}

	return cert.Subject.String()
}

// ValidateIdentity validates one leaf chain to a configured CA.
//
// It builds a chain to caPool with stdlib x509.Verify, enforces expiry
// against now, requires digitalSignature key usage, requires an EKU
// permitting client or server auth when the leaf carries one, rejects
// CA certificates as identities, and checks hostname when expectedDNS
// is set. Failures are fail-closed with ErrIdentityUnverified.
func ValidateIdentity(cert *x509.Certificate, caPool *x509.CertPool, now time.Time, expectedDNS string) error {
	err := checkIdentityInputs(cert, caPool)
	if err != nil {
		return err
	}

	if now.IsZero() {
		now = time.Now()
	}

	err = checkIdentityWindow(cert, now)
	if err != nil {
		return err
	}

	err = checkIdentityUsage(cert)
	if err != nil {
		return err
	}

	err = checkIdentityChain(cert, caPool, now, expectedDNS)
	if err != nil {
		return err
	}

	return nil
}

// checkIdentityInputs rejects nil certificates and pools.
func checkIdentityInputs(cert *x509.Certificate, caPool *x509.CertPool) error {
	if cert == nil {
		return fmt.Errorf("auth: nil identity certificate: %w", ErrIdentityUnverified)
	}

	if caPool == nil {
		return fmt.Errorf("auth: nil trust pool: %w", ErrIdentityUnverified)
	}

	return nil
}

// checkIdentityWindow enforces validity dates against now.
func checkIdentityWindow(cert *x509.Certificate, now time.Time) error {
	if now.Before(cert.NotBefore) || now.After(cert.NotAfter) {
		return fmt.Errorf("auth: identity outside validity window: %w", errors.Join(ErrIdentityExpired, ErrIdentityUnverified))
	}

	return nil
}

// checkIdentityUsage enforces leaf key usages.
func checkIdentityUsage(cert *x509.Certificate) error {
	if cert.IsCA {
		return fmt.Errorf("auth: CA certificate is not an identity: %w", errors.Join(ErrIdentityKeyUsage, ErrIdentityUnverified))
	}

	if cert.KeyUsage&x509.KeyUsageDigitalSignature == 0 {
		return fmt.Errorf("auth: identity lacks digitalSignature usage: %w", errors.Join(ErrIdentityKeyUsage, ErrIdentityUnverified))
	}

	if len(cert.ExtKeyUsage) == 0 {
		return nil
	}

	for _, usage := range cert.ExtKeyUsage {
		if usage == x509.ExtKeyUsageClientAuth || usage == x509.ExtKeyUsageServerAuth || usage == x509.ExtKeyUsageAny {
			return nil
		}
	}

	return fmt.Errorf("auth: identity EKU forbids client or server auth: %w", errors.Join(ErrIdentityKeyUsage, ErrIdentityUnverified))
}

// checkIdentityChain builds the chain and checks hostname when set.
func checkIdentityChain(cert *x509.Certificate, caPool *x509.CertPool, now time.Time, expectedDNS string) error {
	opts := x509.VerifyOptions{
		Roots:       caPool,
		CurrentTime: now,
		KeyUsages:   []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
	}
	if expectedDNS != "" {
		opts.DNSName = expectedDNS
	}

	_, err := cert.Verify(opts)
	if err != nil {
		return fmt.Errorf("auth: identity chain: %w", errors.Join(err, ErrIdentityUnverified))
	}

	if expectedDNS == "" {
		return nil
	}

	err = cert.VerifyHostname(expectedDNS)
	if err != nil {
		return fmt.Errorf("auth: identity hostname: %w", errors.Join(err, ErrIdentityUnverified))
	}

	return nil
}

// adjustedGUIDFlag marks adjusted GUID prefixes per section 10.5.
const adjustedGUIDFlag byte = 0x80

// AdjustedGUIDPrefix derives the participant GUID prefix for one certificate.
//
// It mirrors the CycloneDDS adjusted GUID computation: the high six
// bytes hash the DER subject name with the flag bit folded in, and the
// low six bytes hash a zero candidate prefix. CycloneDDS checks the
// handshake c.pdata key against this value computed with a zero
// candidate, so our announced pdata must carry the same prefix.
func AdjustedGUIDPrefix(cert *x509.Certificate) ([12]byte, error) {
	var out [12]byte

	if cert == nil {
		return out, fmt.Errorf("auth: adjusted guid without certificate: %w", ErrIdentityUnverified)
	}

	high := sha256.Sum256(cert.RawSubject)

	var zeros [12]byte

	low := sha256.Sum256(zeros[:])

	flag := adjustedGUIDFlag

	for i := range 6 {
		out[i] = flag | high[i]>>1
		flag = high[i] << 7
	}

	copy(out[6:12], low[:6])

	return out, nil
}
