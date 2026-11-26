// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

package tokens_test

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"math/big"
	"testing"
	"time"

	"github.com/podomy/weld/internal/transports/dds/security/access"
	"github.com/podomy/weld/internal/transports/dds/security/auth"
)

// dsigcry9CA generates one self-signed CA for envelope refusal tests.
func dsigcry9CA(t *testing.T) (*x509.Certificate, *ecdsa.PrivateKey, []byte) {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate CA key: %v", err)
	}

	template := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "weld-test-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
	}

	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create CA: %v", err)
	}

	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("parse CA: %v", err)
	}

	pemBytes := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})

	return cert, key, pemBytes
}

// dsigcry9Leaf generates one signing leaf for envelope refusal tests.
func dsigcry9Leaf(t *testing.T, ca *x509.Certificate, caKey *ecdsa.PrivateKey) (*x509.Certificate, *ecdsa.PrivateKey) {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate leaf key: %v", err)
	}

	template := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: "signer"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
	}

	der, err := x509.CreateCertificate(rand.Reader, template, ca, &key.PublicKey, caKey)
	if err != nil {
		t.Fatalf("create leaf: %v", err)
	}

	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("parse leaf: %v", err)
	}

	return cert, key
}

// dsigcry9Payload returns unsigned governance for domain zero.
func dsigcry9Payload() []byte {
	return []byte(`<dds><domain_access_rules><domain_rule><domains><id>0</id></domains>` +
		`<allow_unauthenticated_participants>false</allow_unauthenticated_participants>` +
		`<enable_join_access_control>true</enable_join_access_control>` +
		`<discovery_protection_kind>SIGN</discovery_protection_kind>` +
		`<liveliness_protection_kind>SIGN</liveliness_protection_kind>` +
		`<rtps_protection_kind>SIGN</rtps_protection_kind>` +
		`<allowed_crypto_algorithms><algorithm>ECDHE-CEUM+P256</algorithm><algorithm>AES256+GCM</algorithm><algorithm>ECDSA+P256+SHA256</algorithm></allowed_crypto_algorithms>` +
		`<topic_access_rules><topic_rule>` +
		`<topic_expression>rt/chatter</topic_expression>` +
		`<enable_discovery_protection>true</enable_discovery_protection>` +
		`<enable_liveliness_protection>true</enable_liveliness_protection>` +
		`<enable_read_access_control>true</enable_read_access_control>` +
		`<enable_write_access_control>true</enable_write_access_control>` +
		`<metadata_protection_kind>SIGN</metadata_protection_kind>` +
		`<data_protection_kind>ENCRYPT</data_protection_kind>` +
		`</topic_rule></topic_access_rules>` +
		`</domain_rule></domain_access_rules></dds>`)
}

// dsigcry9Signed signs one governance payload for refusal tests.
func dsigcry9Signed(t *testing.T) ([]byte, []byte) {
	t.Helper()

	ca, caKey, caPEM := dsigcry9CA(t)
	cert, key := dsigcry9Leaf(t, ca, caKey)

	signed, err := access.SignEnvelope(dsigcry9Payload(), cert, key)
	if err != nil {
		t.Fatalf("sign envelope: %v", err)
	}

	pool, err := auth.BuildCAPool(caPEM)
	if err != nil {
		t.Fatalf("build pool: %v", err)
	}

	back, err := access.VerifyGovernanceDocument(signed, pool)
	if err != nil {
		t.Fatalf("verify baseline: %v", err)
	}

	if back == nil {
		t.Fatalf("baseline verify must return a document")
	}

	_ = ca

	return signed, caPEM
}

// TestDsigcry9PSSRefused pins RSA-PSS signature refusal.
func TestDsigcry9PSSRefused(t *testing.T) {
	t.Parallel()

	signed, caPEM := dsigcry9Signed(t)

	checkTamperedcry9Refused(t, signed, caPEM, access.DSIGSigECDSASHA256, "http://www.w3.org/2007/05/xmldsig-more#sha256-rsa-MGF1", "RSA-PSS method")
}

// TestDsigcry9SHA512Refused pins SHA-512 digest refusal.
func TestDsigcry9SHA512Refused(t *testing.T) {
	t.Parallel()

	signed, caPEM := dsigcry9Signed(t)

	checkTamperedcry9Refused(t, signed, caPEM, access.DSIGDigestSHA256, "http://www.w3.org/2001/04/xmlenc#sha512", "SHA-512 digest")
}

// checkTamperedcry9Refused verifies one tampered envelope fails closed.
func checkTamperedcry9Refused(t *testing.T, signed, caPEM []byte, oldURI, newURI, what string) {
	t.Helper()

	pool, err := auth.BuildCAPool(caPEM)
	if err != nil {
		t.Fatalf("build pool: %v", err)
	}

	tampered := bytes.Replace(signed, []byte(oldURI), []byte(newURI), 1)
	if bytes.Equal(tampered, signed) {
		t.Fatalf("signed envelope must carry %q", oldURI)
	}

	_, err = access.VerifyGovernanceDocument(tampered, pool)
	if !errors.Is(err, access.ErrSignatureUnverified) {
		t.Fatalf("%s must refuse loudly, got %v", what, err)
	}
}
