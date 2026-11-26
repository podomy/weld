// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

package security

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/pem"
	"errors"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/podomy/weld/internal/transports/dds/security/access"
	"github.com/podomy/weld/internal/transports/dds/security/auth"
)

// auditCA generates one self-signed CA for suite tests.
func auditCA(t *testing.T, cn string) (*x509.Certificate, *ecdsa.PrivateKey, []byte) {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate CA key: %v", err)
	}

	template := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: cn},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign | x509.KeyUsageDigitalSignature,
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

// auditLeaf generates one leaf certificate for suite tests.
func auditLeaf(t *testing.T, ca *x509.Certificate, caKey *ecdsa.PrivateKey, cn string, serial int64) (*x509.Certificate, *ecdsa.PrivateKey, []byte) {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate leaf key: %v", err)
	}

	template := &x509.Certificate{
		SerialNumber: big.NewInt(serial),
		Subject:      pkix.Name{CommonName: cn},
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

	pemBytes := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})

	return cert, key, pemBytes
}

// auditValidity bounds test grants around now in RFC 3339.
func auditValidity() (string, string) {
	before := time.Now().Add(-time.Hour).UTC().Format(time.RFC3339)
	after := time.Now().Add(24 * time.Hour).UTC().Format(time.RFC3339)

	return before, after
}

// auditWriteFile writes one fixture file.
func auditWriteFile(t *testing.T, dir, name string, content []byte) string {
	t.Helper()

	path := filepath.Join(dir, name)

	err := os.WriteFile(path, content, 0o600)
	if err != nil {
		t.Fatalf("write fixture: %v", err)
	}

	return path
}

// auditGovernance returns a two-domain governance payload.
//
// Domain 0 signs; domain 5 encrypts discovery. Both carry the same
// ECDHE plus AES algorithm list so algorithm gating never decides
// these fixtures.
func auditGovernance() []byte {
	rule := func(id, discovery string) string {
		return `<domain_rule><domains><id>` + id + `</id></domains>` +
			`<allow_unauthenticated_participants>false</allow_unauthenticated_participants>` +
			`<enable_join_access_control>true</enable_join_access_control>` +
			`<discovery_protection_kind>` + discovery + `</discovery_protection_kind>` +
			`<liveliness_protection_kind>SIGN</liveliness_protection_kind>` +
			`<rtps_protection_kind>SIGN</rtps_protection_kind>` +
			`<allowed_crypto_algorithms><algorithm>ECDHE-CEUM+P256</algorithm><algorithm>AES256+GCM</algorithm></allowed_crypto_algorithms>` +
			`<topic_access_rules><topic_rule>` +
			`<topic_expression>rt/chatter</topic_expression>` +
			`<enable_discovery_protection>true</enable_discovery_protection>` +
			`<enable_liveliness_protection>true</enable_liveliness_protection>` +
			`<enable_read_access_control>true</enable_read_access_control>` +
			`<enable_write_access_control>true</enable_write_access_control>` +
			`<metadata_protection_kind>SIGN</metadata_protection_kind>` +
			`<data_protection_kind>ENCRYPT</data_protection_kind>` +
			`</topic_rule></topic_access_rules></domain_rule>`
	}

	return []byte(`<dds><domain_access_rules>` + rule("0", "SIGN") + rule("5", "ENCRYPT") + `</domain_access_rules></dds>`)
}

// auditPermissions returns grants for local plus peer over domain zero.
func auditPermissions() []byte {
	before, after := auditValidity()

	grant := func(name, subject string) string {
		return `<grant name="` + name + `"><subject_name>` + subject + `</subject_name>` +
			`<validity><not_before>` + before + `</not_before><not_after>` + after + `</not_after></validity>` +
			`<allow_rule><domains><id>0</id></domains><topics><topic>rt/chatter</topic></topics></allow_rule>` +
			`</grant>`
	}

	return []byte(`<permissions>` + grant("local", "CN=local") + grant("peer", "CN=peer") + `</permissions>`)
}

// auditSuiteOptions selects CA material for one suite build.
type auditSuiteOptions struct {
	// identityCA is the trust bundle for identity certificates.
	identityCA []byte
	// permissionsCA is the trust bundle for signed documents.
	permissionsCA []byte
	// crlPath is the optional revocation file path.
	crlPath string
}

// auditSuite builds an enabled suite over self-signed multi-domain fixtures.
func auditSuite(t *testing.T, ca *x509.Certificate, caKey *ecdsa.PrivateKey, localPEM []byte, opts *auditSuiteOptions) *Suite {
	t.Helper()

	govSigned, err := access.SignEnvelope(auditGovernance(), ca, caKey)
	if err != nil {
		t.Fatalf("sign governance: %v", err)
	}

	permsSigned, err := access.SignEnvelope(auditPermissions(), ca, caKey)
	if err != nil {
		t.Fatalf("sign permissions: %v", err)
	}

	return auditSuiteSigned(t, localPEM, govSigned, permsSigned, opts)
}

// auditSuiteSigned builds an enabled suite over caller-signed fixtures.
func auditSuiteSigned(t *testing.T, localPEM, govSigned, permsSigned []byte, opts *auditSuiteOptions) *Suite {
	t.Helper()

	dir := t.TempDir()

	caPath := auditWriteFile(t, dir, "ca.pem", opts.permissionsCA)
	certPath := auditWriteFile(t, dir, "cert.pem", localPEM)
	keyPath := auditWriteFile(t, dir, "key.pem", localPEM)
	govPath := auditWriteFile(t, dir, "governance.xml", govSigned)
	permsPath := auditWriteFile(t, dir, "permissions.xml", permsSigned)

	cfg := SuiteConfig{
		Enabled:                 true,
		IdentityCertificatePath: certPath,
		IdentityPrivateKeyPath:  keyPath,
		PermissionsCAPath:       caPath,
		GovernancePath:          govPath,
		PermissionsDocumentPath: permsPath,
		CRLPath:                 opts.crlPath,
	}

	if len(opts.identityCA) != 0 {
		identityPath := auditWriteFile(t, dir, "identity-ca.pem", opts.identityCA)
		cfg.IdentityCAPath = identityPath
	}

	suite, err := NewSuite(cfg)
	if err != nil {
		t.Fatalf("new suite: %v", err)
	}

	return suite
}

// auditPeerDescriptor returns a secure descriptor in one domain.
func auditPeerDescriptor(domain uint32) PeerDescriptor {
	return PeerDescriptor{
		Identity: "audit-peer",
		Class:    PeerRequiresCrypto,
		Properties: map[string]string{
			"dds.sec.auth.plugin":                   "DDS:Auth:PKI-DH",
			"dds.sec.access.plugin":                 "DDS:Access:Permissions",
			"dds.sec.crypto.plugin":                 "DDS:Crypto:AES-GCM-GMAC",
			"dds.sec.crypto.key_exchange_algorithm": "ECDHE-CEUM+P256",
		},
		DomainID: domain,
	}
}

// TestAuditAuthenticatePeerBinding pins caller identity to the certificate.
func TestAuditAuthenticatePeerBinding(t *testing.T) {
	t.Parallel()

	ca, caKey, caPEM := auditCA(t, "audit-bind-ca")
	_, _, localPEM := auditLeaf(t, ca, caKey, "local", 10)
	peerCert, _, peerPEM := auditLeaf(t, ca, caKey, "peer", 11)
	_, _, otherPEM := auditLeaf(t, ca, caKey, "other", 12)

	suite := auditSuite(t, ca, caKey, localPEM, &auditSuiteOptions{permissionsCA: caPEM})

	prefix, err := auth.AdjustedGUIDPrefix(peerCert)
	if err != nil {
		t.Fatalf("adjusted prefix: %v", err)
	}

	guidHex := hex.EncodeToString(prefix[:])

	err = suite.AuthenticatePeer(guidHex, peerPEM)
	if err != nil {
		t.Fatalf("GUID identity matching the cert must authenticate: %v", err)
	}

	// One hex digit flipped keeps GUID form but breaks the binding.
	badHex := guidHex[:len(guidHex)-1] + "0"
	if badHex == guidHex {
		badHex = guidHex[:len(guidHex)-1] + "1"
	}

	err = suite.AuthenticatePeer(badHex, peerPEM)
	if !errors.Is(err, ErrNotAllowedBySecurity) {
		t.Fatalf("GUID identity mismatch must refuse, got %v", err)
	}

	err = suite.AuthenticatePeer("audit-opaque", otherPEM)
	if err != nil {
		t.Fatalf("opaque first bind must authenticate: %v", err)
	}

	err = suite.AuthenticatePeer("audit-opaque", otherPEM)
	if err != nil {
		t.Fatalf("same certificate re-auth must stay idempotent: %v", err)
	}

	err = suite.AuthenticatePeer("audit-opaque", peerPEM)
	if !errors.Is(err, ErrNotAllowedBySecurity) {
		t.Fatalf("re-auth under a different cert must refuse displacement, got %v", err)
	}

	err = suite.AuthenticatePeer("audit-second", otherPEM)
	if !errors.Is(err, ErrNotAllowedBySecurity) {
		t.Fatalf("one certificate under a second identity must refuse, got %v", err)
	}
}

// auditDomainSuite builds one authenticated two-domain suite for tests.
func auditDomainSuite(t *testing.T) *Suite {
	t.Helper()

	ca, caKey, caPEM := auditCA(t, "audit-domain-ca")
	_, _, localPEM := auditLeaf(t, ca, caKey, "local", 20)
	_, _, peerPEM := auditLeaf(t, ca, caKey, "peer", 21)

	suite := auditSuite(t, ca, caKey, localPEM, &auditSuiteOptions{permissionsCA: caPEM})

	err := suite.AuthenticatePeer("audit-peer", peerPEM)
	if err != nil {
		t.Fatalf("authenticate peer: %v", err)
	}

	return suite
}

// TestAuditMultiDomainDecisions pins domain threading through decisions.
func TestAuditMultiDomainDecisions(t *testing.T) {
	t.Parallel()

	suite := auditDomainSuite(t)

	err := suite.CheckRemoteEndpoint(auditPeerDescriptor(0), "rt/chatter", false)
	if err != nil {
		t.Fatalf("domain 0 endpoint must allow: %v", err)
	}

	err = suite.CheckRemoteEndpoint(auditPeerDescriptor(5), "rt/chatter", false)
	if !errors.Is(err, ErrNotAllowedBySecurity) {
		t.Fatalf("domain 5 endpoint must deny without grant coverage, got %v", err)
	}

	err = suite.CheckCreateEndpointInDomain("local", "rt/chatter", false, 0)
	if err != nil {
		t.Fatalf("domain 0 create must allow: %v", err)
	}

	err = suite.CheckCreateEndpointInDomain("local", "rt/chatter", false, 5)
	if !errors.Is(err, ErrNotAllowedBySecurity) {
		t.Fatalf("domain 5 create must deny without grant coverage, got %v", err)
	}

	err = suite.CheckLocalDataWriterMatchInDomain("local", "audit-peer", "rt/chatter", 0)
	if err != nil {
		t.Fatalf("domain 0 writer match must allow: %v", err)
	}

	err = suite.CheckLocalDataWriterMatchInDomain("local", "audit-peer", "rt/chatter", 5)
	if !errors.Is(err, ErrNotAllowedBySecurity) {
		t.Fatalf("domain 5 writer match must deny without grant coverage, got %v", err)
	}
}

// TestAuditMultiDomainConfigs pins domain threading through config getters.
func TestAuditMultiDomainConfigs(t *testing.T) {
	t.Parallel()

	suite := auditDomainSuite(t)

	participantZero, err := suite.GetParticipantSecurityConfigInDomain("audit-peer", 0)
	if err != nil {
		t.Fatalf("domain 0 participant config must decide: %v", err)
	}

	participantFive, err := suite.GetParticipantSecurityConfigInDomain("audit-peer", 5)
	if err != nil {
		t.Fatalf("domain 5 participant config must decide: %v", err)
	}

	if participantZero.DiscoveryProtection != "SIGN" {
		t.Fatalf("domain 0 discovery %q, want SIGN", participantZero.DiscoveryProtection)
	}

	if participantFive.DiscoveryProtection != "ENCRYPT" {
		t.Fatalf("domain 5 discovery %q, want ENCRYPT", participantFive.DiscoveryProtection)
	}

	topicFive, err := suite.GetTopicSecurityConfigInDomain("rt/chatter", 5)
	if err != nil {
		t.Fatalf("domain 5 topic config must decide: %v", err)
	}

	if topicFive.DataProtection != "ENCRYPT" {
		t.Fatalf("domain 5 data %q, want ENCRYPT", topicFive.DataProtection)
	}

	endpointZero, err := suite.GetEndpointSecurityConfigInDomain("ep-0", "rt/chatter", false, 0)
	if err != nil {
		t.Fatalf("domain 0 endpoint config must decide: %v", err)
	}

	if !endpointZero.Allowed {
		t.Fatalf("domain 0 endpoint config must allow")
	}

	_, err = suite.GetEndpointSecurityConfigInDomain("ep-5", "rt/chatter", false, 5)
	if !errors.Is(err, ErrNotAllowedBySecurity) {
		t.Fatalf("domain 5 endpoint config must deny without a grant, got %v", err)
	}
}

// TestAuditSplitCA pins distinct identity and permissions trust roots.
func TestAuditSplitCA(t *testing.T) {
	t.Parallel()

	identityCA, identityKey, identityPEM := auditCA(t, "audit-identity-ca")
	permsCA, permsKey, permsPEM := auditCA(t, "audit-permissions-ca")

	_, _, localPEM := auditLeaf(t, identityCA, identityKey, "local", 30)
	signer, signerKey, _ := auditLeaf(t, permsCA, permsKey, "signer", 31)
	_, _, peerPEM := auditLeaf(t, identityCA, identityKey, "peer", 32)

	govSigned, err := access.SignEnvelope(auditGovernance(), signer, signerKey)
	if err != nil {
		t.Fatalf("sign governance: %v", err)
	}

	permsSigned, err := access.SignEnvelope(auditPermissions(), signer, signerKey)
	if err != nil {
		t.Fatalf("sign permissions: %v", err)
	}

	suite := auditSuiteSigned(t, localPEM, govSigned, permsSigned, &auditSuiteOptions{
		identityCA:    identityPEM,
		permissionsCA: permsPEM,
	})

	err = suite.AuthenticatePeer("audit-peer", peerPEM)
	if err != nil {
		t.Fatalf("split-CA peer must authenticate: %v", err)
	}

	err = suite.CheckRemoteParticipant(auditPeerDescriptor(0))
	if err != nil {
		t.Fatalf("split-CA participant must allow: %v", err)
	}

	err = suite.CheckRemoteEndpoint(auditPeerDescriptor(0), "rt/chatter", false)
	if err != nil {
		t.Fatalf("split-CA endpoint must allow: %v", err)
	}

	// Without the identity CA the identity chain cannot build, so the
	// fallback to the permissions CA fails the load closed.
	single := auditSuiteSigned(t, localPEM, govSigned, permsSigned, &auditSuiteOptions{
		permissionsCA: permsPEM,
	})

	err = single.AuthenticatePeer("audit-peer", peerPEM)
	if !errors.Is(err, ErrNotAllowedBySecurity) {
		t.Fatalf("identity chaining to the wrong CA must refuse, got %v", err)
	}
}

// TestAuditRevokedPeerRefused pins CRL enforcement on authentication.
func TestAuditRevokedPeerRefused(t *testing.T) {
	t.Parallel()

	ca, caKey, caPEM := auditCA(t, "audit-revoke-ca")
	_, _, localPEM := auditLeaf(t, ca, caKey, "local", 40)
	_, _, peerPEM := auditLeaf(t, ca, caKey, "peer", 41)

	crlDER, err := x509.CreateRevocationList(rand.Reader, &x509.RevocationList{
		Number:     big.NewInt(1),
		ThisUpdate: time.Now().Add(-time.Hour),
		NextUpdate: time.Now().Add(24 * time.Hour),
		RevokedCertificateEntries: []x509.RevocationListEntry{
			{SerialNumber: big.NewInt(41), RevocationTime: time.Now()},
		},
	}, ca, caKey)
	if err != nil {
		t.Fatalf("create CRL: %v", err)
	}

	dir := t.TempDir()
	crlPath := auditWriteFile(t, dir, "crl.pem", pem.EncodeToMemory(&pem.Block{Type: "X509 CRL", Bytes: crlDER}))

	revoked := auditSuite(t, ca, caKey, localPEM, &auditSuiteOptions{permissionsCA: caPEM, crlPath: crlPath})

	err = revoked.AuthenticatePeer("audit-peer", peerPEM)
	if !errors.Is(err, ErrNotAllowedBySecurity) {
		t.Fatalf("revoked peer must refuse, got %v", err)
	}

	clean := auditSuite(t, ca, caKey, localPEM, &auditSuiteOptions{permissionsCA: caPEM})

	err = clean.AuthenticatePeer("audit-peer", peerPEM)
	if err != nil {
		t.Fatalf("peer without revocation data must authenticate: %v", err)
	}

	// A CRL signed by an unknown authority fails the load closed.
	otherCA, otherKey, _ := auditCA(t, "audit-revoke-other")

	foreignDER, err := x509.CreateRevocationList(rand.Reader, &x509.RevocationList{
		Number:     big.NewInt(1),
		ThisUpdate: time.Now().Add(-time.Hour),
		NextUpdate: time.Now().Add(24 * time.Hour),
		RevokedCertificateEntries: []x509.RevocationListEntry{
			{SerialNumber: big.NewInt(41), RevocationTime: time.Now()},
		},
	}, otherCA, otherKey)
	if err != nil {
		t.Fatalf("create foreign CRL: %v", err)
	}

	foreignPath := auditWriteFile(t, dir, "foreign.pem", pem.EncodeToMemory(&pem.Block{Type: "X509 CRL", Bytes: foreignDER}))

	foreign := auditSuite(t, ca, caKey, localPEM, &auditSuiteOptions{permissionsCA: caPEM, crlPath: foreignPath})

	err = foreign.AuthenticatePeer("audit-peer", peerPEM)
	if !errors.Is(err, ErrNotAllowedBySecurity) {
		t.Fatalf("unverifiable CRL must fail closed, got %v", err)
	}
}

// TestAuditAlgorithmEmptyListDenied pins fail-closed algorithm gating.
func TestAuditAlgorithmEmptyListDenied(t *testing.T) {
	t.Parallel()

	ca, caKey, caPEM := auditCA(t, "audit-algo-ca")
	_, _, localPEM := auditLeaf(t, ca, caKey, "local", 50)
	_, _, peerPEM := auditLeaf(t, ca, caKey, "peer", 51)

	bareGovernance := []byte(`<dds><domain_access_rules><domain_rule>` +
		`<domains><id>0</id></domains>` +
		`<allow_unauthenticated_participants>false</allow_unauthenticated_participants>` +
		`<enable_join_access_control>true</enable_join_access_control>` +
		`<discovery_protection_kind>SIGN</discovery_protection_kind>` +
		`<liveliness_protection_kind>SIGN</liveliness_protection_kind>` +
		`<rtps_protection_kind>SIGN</rtps_protection_kind>` +
		`<topic_access_rules><topic_rule>` +
		`<topic_expression>rt/chatter</topic_expression>` +
		`<enable_discovery_protection>true</enable_discovery_protection>` +
		`<enable_liveliness_protection>true</enable_liveliness_protection>` +
		`<enable_read_access_control>true</enable_read_access_control>` +
		`<enable_write_access_control>true</enable_write_access_control>` +
		`<metadata_protection_kind>SIGN</metadata_protection_kind>` +
		`<data_protection_kind>ENCRYPT</data_protection_kind>` +
		`</topic_rule></topic_access_rules></domain_rule></domain_access_rules></dds>`)

	signer, signerKey, _ := auditLeaf(t, ca, caKey, "local", 52)

	govSigned, err := access.SignEnvelope(bareGovernance, signer, signerKey)
	if err != nil {
		t.Fatalf("sign governance: %v", err)
	}

	permsSigned, err := access.SignEnvelope(auditPermissions(), signer, signerKey)
	if err != nil {
		t.Fatalf("sign permissions: %v", err)
	}

	suite := auditSuiteSigned(t, localPEM, govSigned, permsSigned, &auditSuiteOptions{permissionsCA: caPEM})

	err = suite.AuthenticatePeer("audit-peer", peerPEM)
	if err != nil {
		t.Fatalf("authenticate peer: %v", err)
	}

	// An empty rule allow list admits nothing even when the peer names
	// a known algorithm.
	err = suite.CheckRemoteParticipant(auditPeerDescriptor(0))
	if !errors.Is(err, ErrNotAllowedBySecurity) {
		t.Fatalf("empty algorithm list must deny, got %v", err)
	}

	// A peer announcing no algorithm cannot satisfy a rule that names
	// algorithms either.
	govSigned, err = access.SignEnvelope(auditGovernance(), signer, signerKey)
	if err != nil {
		t.Fatalf("sign governance: %v", err)
	}

	listed := auditSuiteSigned(t, localPEM, govSigned, permsSigned, &auditSuiteOptions{permissionsCA: caPEM})

	err = listed.AuthenticatePeer("audit-peer", peerPEM)
	if err != nil {
		t.Fatalf("authenticate peer: %v", err)
	}

	silent := auditPeerDescriptor(0)
	delete(silent.Properties, "dds.sec.crypto.key_exchange_algorithm")

	err = listed.CheckRemoteParticipant(silent)
	if !errors.Is(err, ErrNotAllowedBySecurity) {
		t.Fatalf("peer without an algorithm must deny, got %v", err)
	}

	err = listed.CheckRemoteParticipant(auditPeerDescriptor(0))
	if err != nil {
		t.Fatalf("peer naming a listed algorithm must allow: %v", err)
	}
}

// TestAuditJoinAccessControlDisabledAdmitsAuthenticated pins join gating.
//
// With enable_join_access_control false, an authenticated peer joins
// without grant checks. Unauthenticated peers still need
// allow_unauthenticated_participants, preserving the asymmetric posture.
func TestAuditJoinAccessControlDisabledAdmitsAuthenticated(t *testing.T) {
	t.Parallel()

	ca, caKey, caPEM := auditCA(t, "audit-join-ca")
	_, _, localPEM := auditLeaf(t, ca, caKey, "local", 60)
	_, _, peerPEM := auditLeaf(t, ca, caKey, "peer", 61)

	rule := `<domain_rule><domains><id>0</id></domains>` +
		`<allow_unauthenticated_participants>false</allow_unauthenticated_participants>` +
		`<enable_join_access_control>false</enable_join_access_control>` +
		`<discovery_protection_kind>SIGN</discovery_protection_kind>` +
		`<liveliness_protection_kind>SIGN</liveliness_protection_kind>` +
		`<rtps_protection_kind>SIGN</rtps_protection_kind>` +
		`<allowed_crypto_algorithms><algorithm>ECDHE-CEUM+P256</algorithm><algorithm>AES256+GCM</algorithm></allowed_crypto_algorithms>` +
		`<topic_access_rules><topic_rule>` +
		`<topic_expression>rt/chatter</topic_expression>` +
		`<enable_discovery_protection>true</enable_discovery_protection>` +
		`<enable_liveliness_protection>true</enable_liveliness_protection>` +
		`<enable_read_access_control>true</enable_read_access_control>` +
		`<enable_write_access_control>true</enable_write_access_control>` +
		`<metadata_protection_kind>SIGN</metadata_protection_kind>` +
		`<data_protection_kind>ENCRYPT</data_protection_kind>` +
		`</topic_rule></topic_access_rules></domain_rule>`
	gov := []byte(`<dds><domain_access_rules>` + rule + `</domain_access_rules></dds>`)

	before, after := auditValidity()
	localOnly := []byte(`<permissions><grant name="local"><subject_name>CN=local</subject_name>` +
		`<validity><not_before>` + before + `</not_before><not_after>` + after + `</not_after></validity>` +
		`<allow_rule><domains><id>0</id></domains><topics><topic>rt/chatter</topic></topics></allow_rule>` +
		`</grant></permissions>`)

	govSigned, err := access.SignEnvelope(gov, ca, caKey)
	if err != nil {
		t.Fatalf("sign governance: %v", err)
	}

	permsSigned, err := access.SignEnvelope(localOnly, ca, caKey)
	if err != nil {
		t.Fatalf("sign permissions: %v", err)
	}

	suite := auditSuiteSigned(t, localPEM, govSigned, permsSigned, &auditSuiteOptions{permissionsCA: caPEM})

	err = suite.AuthenticatePeer("audit-peer", peerPEM)
	if err != nil {
		t.Fatalf("authenticate peer: %v", err)
	}

	// No peer grant exists, yet join control is off, so admission allows.
	err = suite.CheckRemoteParticipant(auditPeerDescriptor(0))
	if err != nil {
		t.Fatalf("authenticated peer must join with join control off: %v", err)
	}

	// Without authentication the closed rule still refuses.
	plain := auditSuiteSigned(t, localPEM, govSigned, permsSigned, &auditSuiteOptions{permissionsCA: caPEM})

	err = plain.CheckRemoteParticipant(auditPeerDescriptor(0))
	if !errors.Is(err, ErrNotAllowedBySecurity) {
		t.Fatalf("unauthenticated peer must refuse with join control off, got %v", err)
	}
}

// TestAuditBlanketDenyRefusesJoin pins deny-overrides-allow at join.
//
// A domain-wide deny with no topic scope refuses participant admission
// even when an allow rule covers the domain. Topic-scoped denies never
// match the topicless join check.
func TestAuditBlanketDenyRefusesJoin(t *testing.T) {
	t.Parallel()

	ca, caKey, caPEM := auditCA(t, "audit-deny-ca")
	_, _, localPEM := auditLeaf(t, ca, caKey, "local", 70)
	_, _, peerPEM := auditLeaf(t, ca, caKey, "peer", 71)

	before, after := auditValidity()
	grant := func(name, subject, extra string) string {
		return `<grant name="` + name + `"><subject_name>` + subject + `</subject_name>` +
			`<validity><not_before>` + before + `</not_before><not_after>` + after + `</not_after></validity>` +
			`<allow_rule><domains><id>0</id></domains><topics><topic>rt/chatter</topic></topics></allow_rule>` +
			extra + `</grant>`
	}
	perms := []byte(`<permissions>` +
		grant("local", "CN=local", "") +
		grant("peer", "CN=peer", `<deny_rule><domains><id>0</id></domains></deny_rule>`) +
		`</permissions>`)

	govSigned, err := access.SignEnvelope(auditGovernance(), ca, caKey)
	if err != nil {
		t.Fatalf("sign governance: %v", err)
	}

	permsSigned, err := access.SignEnvelope(perms, ca, caKey)
	if err != nil {
		t.Fatalf("sign permissions: %v", err)
	}

	suite := auditSuiteSigned(t, localPEM, govSigned, permsSigned, &auditSuiteOptions{permissionsCA: caPEM})

	err = suite.AuthenticatePeer("audit-peer", peerPEM)
	if err != nil {
		t.Fatalf("authenticate peer: %v", err)
	}

	err = suite.CheckRemoteParticipant(auditPeerDescriptor(0))
	if !errors.Is(err, ErrNotAllowedBySecurity) {
		t.Fatalf("blanket deny must refuse join, got %v", err)
	}
}

// TestAuditOriginAuthKindsRefuseJoinAndMatch pins fail-closed origin kinds.
//
// Governance demanding SIGN_WITH_ORIGIN_AUTHENTICATION or
// ENCRYPT_WITH_ORIGIN_AUTHENTICATION must refuse admission instead of
// silently downgrading to plain SIGN or ENCRYPT, until origin identity
// is bound into the crypto AAD.
func TestAuditOriginAuthKindsRefuseJoinAndMatch(t *testing.T) {
	t.Parallel()

	ca, caKey, caPEM := auditCA(t, "audit-origin-ca")
	_, _, localPEM := auditLeaf(t, ca, caKey, "local", 80)
	_, _, peerPEM := auditLeaf(t, ca, caKey, "peer", 81)

	rule := `<domain_rule><domains><id>0</id></domains>` +
		`<allow_unauthenticated_participants>false</allow_unauthenticated_participants>` +
		`<enable_join_access_control>true</enable_join_access_control>` +
		`<discovery_protection_kind>SIGN_WITH_ORIGIN_AUTHENTICATION</discovery_protection_kind>` +
		`<liveliness_protection_kind>SIGN</liveliness_protection_kind>` +
		`<rtps_protection_kind>SIGN</rtps_protection_kind>` +
		`<allowed_crypto_algorithms><algorithm>ECDHE-CEUM+P256</algorithm><algorithm>AES256+GCM</algorithm></allowed_crypto_algorithms>` +
		`<topic_access_rules><topic_rule>` +
		`<topic_expression>rt/chatter</topic_expression>` +
		`<enable_discovery_protection>true</enable_discovery_protection>` +
		`<enable_liveliness_protection>true</enable_liveliness_protection>` +
		`<enable_read_access_control>true</enable_read_access_control>` +
		`<enable_write_access_control>true</enable_write_access_control>` +
		`<metadata_protection_kind>SIGN</metadata_protection_kind>` +
		`<data_protection_kind>ENCRYPT_WITH_ORIGIN_AUTHENTICATION</data_protection_kind>` +
		`</topic_rule></topic_access_rules></domain_rule>`
	gov := []byte(`<dds><domain_access_rules>` + rule + `</domain_access_rules></dds>`)

	govSigned, err := access.SignEnvelope(gov, ca, caKey)
	if err != nil {
		t.Fatalf("sign governance: %v", err)
	}

	permsSigned, err := access.SignEnvelope(auditPermissions(), ca, caKey)
	if err != nil {
		t.Fatalf("sign permissions: %v", err)
	}

	suite := auditSuiteSigned(t, localPEM, govSigned, permsSigned, &auditSuiteOptions{permissionsCA: caPEM})

	err = suite.AuthenticatePeer("audit-peer", peerPEM)
	if err != nil {
		t.Fatalf("authenticate peer: %v", err)
	}

	err = suite.CheckRemoteParticipant(auditPeerDescriptor(0))
	if !errors.Is(err, ErrNotAllowedBySecurity) {
		t.Fatalf("origin discovery kind must refuse join, got %v", err)
	}

	err = suite.CheckRemoteEndpoint(auditPeerDescriptor(0), "rt/chatter", false)
	if !errors.Is(err, ErrNotAllowedBySecurity) {
		t.Fatalf("origin data kind must refuse endpoint, got %v", err)
	}

	err = suite.CheckCreateEndpoint("local-participant", "rt/chatter", false)
	if !errors.Is(err, ErrNotAllowedBySecurity) {
		t.Fatalf("origin data kind must refuse local create, got %v", err)
	}
}

// TestAuditDirectionPublishSubscribeacc8 pins directional endpoint decisions.
func TestAuditDirectionPublishSubscribeacc8(t *testing.T) {
	t.Parallel()

	ca, caKey, caPEM := auditCA(t, "audit-dir-ca")
	_, _, localPEM := auditLeaf(t, ca, caKey, "local", 90)
	_, _, peerPEM := auditLeaf(t, ca, caKey, "peer", 91)

	before, after := auditValidity()
	perms := []byte(`<permissions><grant name="pub-only"><subject_name>CN=peer</subject_name>` +
		`<validity><not_before>` + before + `</not_before><not_after>` + after + `</not_after></validity>` +
		`<allow_rule><domains><id>0</id></domains>` +
		`<publish><topics><topic>rt/chatter</topic></topics></publish>` +
		`</allow_rule></grant>` +
		`<grant name="local"><subject_name>CN=local</subject_name>` +
		`<validity><not_before>` + before + `</not_before><not_after>` + after + `</not_after></validity>` +
		`<allow_rule><domains><id>0</id></domains><topics><topic>rt/chatter</topic></topics></allow_rule>` +
		`</grant></permissions>`)

	govSigned, err := access.SignEnvelope(auditGovernance(), ca, caKey)
	if err != nil {
		t.Fatalf("sign governance: %v", err)
	}

	permsSigned, err := access.SignEnvelope(perms, ca, caKey)
	if err != nil {
		t.Fatalf("sign permissions: %v", err)
	}

	suite := auditSuiteSigned(t, localPEM, govSigned, permsSigned, &auditSuiteOptions{permissionsCA: caPEM})

	err = suite.AuthenticatePeer("audit-peer", peerPEM)
	if err != nil {
		t.Fatalf("authenticate peer: %v", err)
	}

	err = suite.CheckRemoteEndpoint(auditPeerDescriptor(0), "rt/chatter", false)
	if err != nil {
		t.Fatalf("publish-only grant must allow publish: %v", err)
	}

	err = suite.CheckRemoteEndpoint(auditPeerDescriptor(0), "rt/chatter", true)
	if !errors.Is(err, ErrNotAllowedBySecurity) {
		t.Fatalf("publish-only grant must deny subscribe, got %v", err)
	}
}

// TestAuditLocalScopedTagGateacc8 pins nil-scope deny with supplied-scope allow.
func TestAuditLocalScopedTagGateacc8(t *testing.T) {
	t.Parallel()

	ca, caKey, caPEM := auditCA(t, "audit-local-scope-ca")
	_, _, localPEM := auditLeaf(t, ca, caKey, "local", 100)

	before, after := auditValidity()
	tagged := []byte(`<permissions><grant name="local-tagged"><subject_name>CN=local</subject_name>` +
		`<validity><not_before>` + before + `</not_before><not_after>` + after + `</not_after></validity>` +
		`<allow_rule><domains><id>0</id></domains><topics><topic>rt/chatter</topic></topics>` +
		`<data_tags><tag>secret</tag></data_tags></allow_rule>` +
		`</grant></permissions>`)

	govSigned, err := access.SignEnvelope(auditGovernance(), ca, caKey)
	if err != nil {
		t.Fatalf("sign governance: %v", err)
	}

	permsSigned, err := access.SignEnvelope(tagged, ca, caKey)
	if err != nil {
		t.Fatalf("sign permissions: %v", err)
	}

	suite := auditSuiteSigned(t, localPEM, govSigned, permsSigned, &auditSuiteOptions{permissionsCA: caPEM})

	err = suite.CheckCreateEndpointInDomainacc8Scoped("local", "rt/chatter", false, 0, nil, nil)
	if !errors.Is(err, ErrNotAllowedBySecurity) {
		t.Fatalf("tag-gated grant must deny nil-scope local create, got %v", err)
	}

	err = suite.CheckCreateEndpointInDomainacc8Scoped("local", "rt/chatter", false, 0, nil, []string{"secret"})
	if err != nil {
		t.Fatalf("tag-gated grant must allow supplied-scope local create: %v", err)
	}

	err = suite.CheckCreateParticipantInDomainacc8Scoped("local", 0, nil, nil)
	if !errors.Is(err, ErrNotAllowedBySecurity) {
		t.Fatalf("tag-gated grant must deny nil-scope participant create, got %v", err)
	}

	err = suite.CheckCreateParticipantInDomainacc8Scoped("local", 0, nil, []string{"secret"})
	if err != nil {
		t.Fatalf("tag-gated grant must allow supplied-scope participant create: %v", err)
	}
}

// TestAuditReloadFlipsDecisionacc8 pins rotated files flipping a decision.
func TestAuditReloadFlipsDecisionacc8(t *testing.T) {
	t.Parallel()

	ca, caKey, caPEM := auditCA(t, "audit-reload-ca")
	_, _, localPEM := auditLeaf(t, ca, caKey, "local", 110)
	_, _, peerPEM := auditLeaf(t, ca, caKey, "peer", 111)

	dir := t.TempDir()
	caPath := auditWriteFile(t, dir, "ca.pem", caPEM)
	certPath := auditWriteFile(t, dir, "cert.pem", localPEM)
	keyPath := auditWriteFile(t, dir, "key.pem", localPEM)

	govSigned, err := access.SignEnvelope(auditGovernance(), ca, caKey)
	if err != nil {
		t.Fatalf("sign governance: %v", err)
	}

	govPath := auditWriteFile(t, dir, "governance.xml", govSigned)

	before, after := auditValidity()
	allowPerms := []byte(`<permissions><grant name="peer"><subject_name>CN=peer</subject_name>` +
		`<validity><not_before>` + before + `</not_before><not_after>` + after + `</not_after></validity>` +
		`<allow_rule><domains><id>0</id></domains><topics><topic>rt/chatter</topic></topics></allow_rule>` +
		`</grant></permissions>`)
	allowSigned, err := access.SignEnvelope(allowPerms, ca, caKey)
	if err != nil {
		t.Fatalf("sign allow: %v", err)
	}

	permsPath := auditWriteFile(t, dir, "permissions.xml", allowSigned)

	suite, err := NewSuite(SuiteConfig{
		Enabled:                 true,
		IdentityCertificatePath: certPath,
		IdentityPrivateKeyPath:  keyPath,
		PermissionsCAPath:       caPath,
		GovernancePath:          govPath,
		PermissionsDocumentPath: permsPath,
	})
	if err != nil {
		t.Fatalf("new suite: %v", err)
	}

	err = suite.AuthenticatePeer("audit-peer", peerPEM)
	if err != nil {
		t.Fatalf("authenticate peer: %v", err)
	}

	err = suite.CheckRemoteEndpoint(auditPeerDescriptor(0), "rt/chatter", false)
	if err != nil {
		t.Fatalf("allow perms must allow: %v", err)
	}

	denyPerms := []byte(`<permissions><grant name="peer"><subject_name>CN=peer</subject_name>` +
		`<validity><not_before>` + before + `</not_before><not_after>` + after + `</not_after></validity>` +
		`<allow_rule><domains><id>0</id></domains><topics><topic>rt/other</topic></topics></allow_rule>` +
		`</grant></permissions>`)
	denySigned, err := access.SignEnvelope(denyPerms, ca, caKey)
	if err != nil {
		t.Fatalf("sign deny: %v", err)
	}

	err = os.WriteFile(permsPath, denySigned, 0o600)
	if err != nil {
		t.Fatalf("rotate perms: %v", err)
	}

	err = suite.Reloadacc8Cache()
	if err != nil {
		t.Fatalf("reload: %v", err)
	}

	err = suite.CheckRemoteEndpoint(auditPeerDescriptor(0), "rt/chatter", false)
	if !errors.Is(err, ErrNotAllowedBySecurity) {
		t.Fatalf("rotated files must flip to deny, got %v", err)
	}

	err = os.WriteFile(permsPath, allowSigned, 0o600)
	if err != nil {
		t.Fatalf("rotate back: %v", err)
	}

	suite.mu.Lock()
	suite.verifiedAt = time.Now().Add(-2 * verifiedStaleAfteracc8)
	suite.mu.Unlock()

	err = suite.CheckRemoteEndpoint(auditPeerDescriptor(0), "rt/chatter", false)
	if err != nil {
		t.Fatalf("stale bound must auto-reload to allow: %v", err)
	}
}

// TestAuditJoinEdgeMatrixacc8 pins unauthenticated open plus tag-scoped joins.
func TestAuditJoinEdgeMatrixacc8(t *testing.T) {
	t.Parallel()

	ca, caKey, caPEM := auditCA(t, "audit-join-edge-ca")
	_, _, localPEM := auditLeaf(t, ca, caKey, "local", 120)

	openRule := `<domain_rule><domains><id>0</id></domains>` +
		`<allow_unauthenticated_participants>true</allow_unauthenticated_participants>` +
		`<enable_join_access_control>false</enable_join_access_control>` +
		`<discovery_protection_kind>NONE</discovery_protection_kind>` +
		`<liveliness_protection_kind>NONE</liveliness_protection_kind>` +
		`<rtps_protection_kind>NONE</rtps_protection_kind>` +
		`<topic_access_rules><topic_rule>` +
		`<topic_expression>rt/open</topic_expression>` +
		`<enable_discovery_protection>false</enable_discovery_protection>` +
		`<enable_liveliness_protection>false</enable_liveliness_protection>` +
		`<enable_read_access_control>false</enable_read_access_control>` +
		`<enable_write_access_control>false</enable_write_access_control>` +
		`<metadata_protection_kind>NONE</metadata_protection_kind>` +
		`<data_protection_kind>NONE</data_protection_kind>` +
		`</topic_rule></topic_access_rules></domain_rule>`
	openGov := []byte(`<dds><domain_access_rules>` + openRule + `</domain_access_rules></dds>`)

	before, after := auditValidity()
	localOnly := []byte(`<permissions><grant name="local"><subject_name>CN=local</subject_name>` +
		`<validity><not_before>` + before + `</not_before><not_after>` + after + `</not_after></validity>` +
		`<allow_rule><domains><id>0</id></domains><topics><topic>rt/open</topic></topics></allow_rule>` +
		`</grant></permissions>`)

	govSigned, err := access.SignEnvelope(openGov, ca, caKey)
	if err != nil {
		t.Fatalf("sign gov: %v", err)
	}

	permsSigned, err := access.SignEnvelope(localOnly, ca, caKey)
	if err != nil {
		t.Fatalf("sign perms: %v", err)
	}

	suite := auditSuiteSigned(t, localPEM, govSigned, permsSigned, &auditSuiteOptions{permissionsCA: caPEM})

	plain := PeerDescriptor{
		Identity:   "plain-peer",
		Class:      PeerUnsecured,
		Properties: map[string]string{},
		DomainID:   0,
	}

	err = suite.CheckRemoteParticipant(plain)
	if err != nil {
		t.Fatalf("unauthenticated join-off open must allow: %v", err)
	}

	taggedRule := `<domain_rule><domains><id>0</id><tag>cell</tag></domains>` +
		`<allow_unauthenticated_participants>true</allow_unauthenticated_participants>` +
		`<enable_join_access_control>false</enable_join_access_control>` +
		`<discovery_protection_kind>NONE</discovery_protection_kind>` +
		`<liveliness_protection_kind>NONE</liveliness_protection_kind>` +
		`<rtps_protection_kind>NONE</rtps_protection_kind>` +
		`<topic_access_rules><topic_rule>` +
		`<topic_expression>rt/open</topic_expression>` +
		`<enable_discovery_protection>false</enable_discovery_protection>` +
		`<enable_liveliness_protection>false</enable_liveliness_protection>` +
		`<enable_read_access_control>false</enable_read_access_control>` +
		`<enable_write_access_control>false</enable_write_access_control>` +
		`<metadata_protection_kind>NONE</metadata_protection_kind>` +
		`<data_protection_kind>NONE</data_protection_kind>` +
		`</topic_rule></topic_access_rules></domain_rule>`
	taggedGov := []byte(`<dds><domain_access_rules>` + taggedRule + `</domain_access_rules></dds>`)

	taggedGovSigned, err := access.SignEnvelope(taggedGov, ca, caKey)
	if err != nil {
		t.Fatalf("sign tagged gov: %v", err)
	}

	taggedSuite := auditSuiteSigned(t, localPEM, taggedGovSigned, permsSigned, &auditSuiteOptions{permissionsCA: caPEM})

	empty := PeerDescriptor{
		Identity:   "tag-empty",
		Class:      PeerUnsecured,
		Properties: map[string]string{},
		DomainID:   0,
		Tag:        "",
	}

	err = taggedSuite.CheckRemoteParticipant(empty)
	if !errors.Is(err, ErrNotAllowedBySecurity) {
		t.Fatalf("tag-scoped rule must deny empty tag, got %v", err)
	}

	supplied := PeerDescriptor{
		Identity:   "tag-cell",
		Class:      PeerUnsecured,
		Properties: map[string]string{},
		DomainID:   0,
		Tag:        "cell",
	}

	err = taggedSuite.CheckRemoteParticipant(supplied)
	if err != nil {
		t.Fatalf("tag-scoped rule must allow supplied tag: %v", err)
	}
}

// TestAuditTransformKindInterfaceacc8 pins the transform-kind proxy check.
func TestAuditTransformKindInterfaceacc8(t *testing.T) {
	t.Parallel()

	if transformKindSatisfiesGovernanceacc8(nil, nil) {
		t.Fatalf("nil rule must not satisfy")
	}

	open := &access.DomainRule{
		DiscoveryProtection:  access.ProtectionNone,
		LivelinessProtection: access.ProtectionNone,
		RTPSProtection:       access.ProtectionNone,
	}
	if !transformKindSatisfiesGovernanceacc8(open, nil) {
		t.Fatalf("all-NONE rule must satisfy without transform")
	}

	rule := &access.DomainRule{DiscoveryProtection: access.ProtectionEncrypt}
	if transformKindSatisfiesGovernanceacc8(rule, nil) {
		t.Fatalf("ENCRYPT without AES-GCM allow-list must fail closed")
	}

	withGCM := &access.DomainRule{
		DiscoveryProtection:  access.ProtectionEncrypt,
		LivelinessProtection: access.ProtectionNone,
		RTPSProtection:       access.ProtectionNone,
		AllowedAlgorithms:    []string{"ECDHE-CEUM+P256", "AES256+GCM"},
	}
	if !transformKindSatisfiesGovernanceacc8(withGCM, nil) {
		t.Fatalf("ENCRYPT with AES-GCM allow-list must satisfy as proxy")
	}

	signed := &access.DomainRule{
		DiscoveryProtection:  access.ProtectionNone,
		LivelinessProtection: access.ProtectionNone,
		RTPSProtection:       access.ProtectionNone,
	}
	topic := &access.TopicRule{
		MetadataProtection: access.ProtectionNone,
		DataProtection:     access.ProtectionSign,
	}
	if transformKindSatisfiesGovernanceacc8(signed, topic) {
		t.Fatalf("SIGN data without AES-GCM allow-list must fail closed")
	}

	origin := &access.DomainRule{
		DiscoveryProtection:  access.ProtectionSignWithOrigin,
		LivelinessProtection: access.ProtectionNone,
		RTPSProtection:       access.ProtectionNone,
		AllowedAlgorithms:    []string{"AES256+GCM"},
	}
	if transformKindSatisfiesGovernanceacc8(origin, nil) {
		t.Fatalf("origin auth without AAD wiring must fail closed")
	}
}
