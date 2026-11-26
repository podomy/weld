// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

package security

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"errors"
	"math/big"
	"os"
	"testing"
	"time"

	"github.com/podomy/weld/internal/transports/dds/security/access"
	"github.com/podomy/weld/internal/transports/dds/security/auth"
)

// stubHandshake implements HandshakeReporter for wiring tests.
type stubHandshake struct {
	authenticated map[string]bool
}

func (s *stubHandshake) Authenticated(remote string) bool {
	return s.authenticated[remote]
}

// TestSecGapsHandshakeWiringacc9 pins Manager.Authenticated gating.
func TestSecGapsHandshakeWiringacc9(t *testing.T) {
	t.Parallel()

	suite := auditDomainSuite(t)

	allowStub := &stubHandshake{authenticated: map[string]bool{"audit-peer": true}}
	suite.SetHandshakeManager(allowStub)

	err := suite.CheckRemoteEndpoint(auditPeerDescriptor(0), "rt/chatter", false)
	if err != nil {
		t.Fatalf("handshake-authenticated peer must allow: %v", err)
	}

	denyStub := &stubHandshake{authenticated: map[string]bool{}}
	suite.SetHandshakeManager(denyStub)

	err = suite.CheckRemoteEndpoint(auditPeerDescriptor(0), "rt/chatter", false)
	if !errors.Is(err, ErrNotAllowedBySecurity) {
		t.Fatalf("handshake-unauthenticated peer must deny, got %v", err)
	}

	suite.SetHandshakeManager(nil)

	err = suite.CheckRemoteEndpoint(auditPeerDescriptor(0), "rt/chatter", false)
	if err != nil {
		t.Fatalf("nil handshake must preserve validation-only allow: %v", err)
	}
}

// gapsLeafFull mints one leaf with CN plus organization for DN tests.
func gapsLeafFull(t *testing.T, ca *x509.Certificate, caKey *ecdsa.PrivateKey, cn, org string, serial int64) (*x509.Certificate, []byte) {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate leaf: %v", err)
	}

	template := &x509.Certificate{
		SerialNumber: big.NewInt(serial),
		Subject:      pkix.Name{CommonName: cn, Organization: []string{org}},
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

	return cert, der
}

// TestSecGapsFullDNBindingacc9 pins colliding CNs distinguishing grants.
//
// Same CN with different organizations must resolve to different grants
// via the full DN; CN-only matching would conflate them.
func TestSecGapsFullDNBindingacc9(t *testing.T) {
	t.Parallel()

	ca, caKey, caPEM := auditCA(t, "gaps-dn-ca")
	certOne, _ := gapsLeafFull(t, ca, caKey, "same-cn", "org-one", 201)
	certTwo, _ := gapsLeafFull(t, ca, caKey, "same-cn", "org-two", 202)

	if certOne.Subject.CommonName != certTwo.Subject.CommonName {
		t.Fatalf("test certificates must share CN, got %q vs %q", certOne.Subject.CommonName, certTwo.Subject.CommonName)
	}

	fullOne := auth.CertificateSubjectFullauth7(certOne)
	fullTwo := auth.CertificateSubjectFullauth7(certTwo)
	if fullOne == fullTwo {
		t.Fatalf("full DNs must differ, both %q", fullOne)
	}

	before, after := auditValidity()
	perms := []byte(`<permissions>` +
		`<grant name="one"><subject_name>` + fullOne + `</subject_name>` +
		`<validity><not_before>` + before + `</not_before><not_after>` + after + `</not_after></validity>` +
		`<allow_rule><domains><id>0</id></domains><topics><topic>rt/one</topic></topics></allow_rule>` +
		`</grant>` +
		`<grant name="two"><subject_name>` + fullTwo + `</subject_name>` +
		`<validity><not_before>` + before + `</not_before><not_after>` + after + `</not_after></validity>` +
		`<allow_rule><domains><id>0</id></domains><topics><topic>rt/two</topic></topics></allow_rule>` +
		`</grant></permissions>`)

	doc, err := access.ParsePermissions(perms)
	if err != nil {
		t.Fatalf("parse perms: %v", err)
	}

	grantOne := access.GrantForSubject(doc, fullOne, time.Now())
	if grantOne == nil {
		t.Fatalf("grant one must resolve by full DN")
	}

	if ok, _ := access.AllowsPublishacc8(grantOne, 0, "rt/one", nil, nil); !ok {
		t.Fatalf("grant one must allow publish rt/one")
	}

	if ok, _ := access.AllowsSubscribeacc8(grantOne, 0, "rt/one", nil, nil); !ok {
		t.Fatalf("grant one must allow subscribe rt/one")
	}

	if ok, _ := access.AllowsPublishacc8(grantOne, 0, "rt/two", nil, nil); ok {
		t.Fatalf("grant one must not allow publish rt/two")
	}

	if ok, _ := access.AllowsSubscribeacc8(grantOne, 0, "rt/two", nil, nil); ok {
		t.Fatalf("grant one must not allow subscribe rt/two")
	}

	_ = caPEM
}

// TestSecGapsValidateOpsacc9 pins Validate plus topic plus instance ops.
func TestSecGapsValidateOpsacc9(t *testing.T) {
	t.Parallel()

	suite := auditDomainSuite(t)

	if err := suite.ValidateLocalPermissions("local", 0); err != nil {
		t.Fatalf("local permissions must validate: %v", err)
	}

	if err := suite.ValidateRemotePermissions("audit-peer", 0); err != nil {
		t.Fatalf("remote permissions must validate: %v", err)
	}

	if err := suite.ValidateRemotePermissions("unknown-peer", 0); !errors.Is(err, ErrNotAllowedBySecurity) {
		t.Fatalf("unknown remote must deny, got %v", err)
	}

	if err := suite.CheckCreateTopic("local", "rt/chatter", 0); err != nil {
		t.Fatalf("create topic must allow: %v", err)
	}

	if err := suite.CheckCreateTopic("local", "rt/other", 0); !errors.Is(err, ErrNotAllowedBySecurity) {
		t.Fatalf("uncovered topic must deny, got %v", err)
	}

	if err := suite.CheckLocalDataWriterRegisterInstance("local", "rt/chatter", 0); err != nil {
		t.Fatalf("register instance must allow: %v", err)
	}

	if err := suite.CheckRemoteTopic(auditPeerDescriptor(0), "rt/chatter"); err != nil {
		t.Fatalf("remote topic must allow: %v", err)
	}
}

// TestSecGapsScopedMatchacc9 pins partitions plus tags in match paths.
func TestSecGapsScopedMatchacc9(t *testing.T) {
	t.Parallel()

	ca, caKey, caPEM := auditCA(t, "gaps-scope-ca")
	_, _, localPEM := auditLeaf(t, ca, caKey, "local", 210)

	before, after := auditValidity()
	tagged := []byte(`<permissions><grant name="local-tagged"><subject_name>CN=local</subject_name>` +
		`<validity><not_before>` + before + `</not_before><not_after>` + after + `</not_after></validity>` +
		`<allow_rule><domains><id>0</id></domains><topics><topic>rt/chatter</topic></topics>` +
		`<data_tags><tag>secret</tag></data_tags></allow_rule>` +
		`</grant></permissions>`)

	govSigned, err := access.SignEnvelope(auditGovernance(), ca, caKey)
	if err != nil {
		t.Fatalf("sign gov: %v", err)
	}

	permsSigned, err := access.SignEnvelope(tagged, ca, caKey)
	if err != nil {
		t.Fatalf("sign perms: %v", err)
	}

	suite := auditSuiteSigned(t, localPEM, govSigned, permsSigned, &auditSuiteOptions{permissionsCA: caPEM})

	err = suite.CheckLocalDataWriterMatchInDomainacc8Scoped("local", "peer", "rt/chatter", 0, nil, nil, "")
	if !errors.Is(err, ErrNotAllowedBySecurity) {
		t.Fatalf("nil scope must deny tag-gated match, got %v", err)
	}

	err = suite.CheckLocalDataWriterMatchInDomainacc8Scoped("local", "peer", "rt/chatter", 0, nil, []string{"secret"}, "")
	if err != nil {
		t.Fatalf("supplied scope must allow match: %v", err)
	}

	err = suite.CheckLocalDataReaderMatchInDomainacc8Scoped("local", "peer", "rt/chatter", 0, nil, nil, "")
	if !errors.Is(err, ErrNotAllowedBySecurity) {
		t.Fatalf("nil scope must deny tag-gated reader match, got %v", err)
	}

	err = suite.CheckLocalDataReaderMatchInDomainacc8Scoped("local", "peer", "rt/chatter", 0, nil, []string{"secret"}, "")
	if err != nil {
		t.Fatalf("supplied scope must allow reader match: %v", err)
	}
}

// TestSecGapsTypeConsistencyacc9 pins type checks in match paths.
func TestSecGapsTypeConsistencyacc9(t *testing.T) {
	t.Parallel()

	doc, err := access.ParsePermissions([]byte(`<permissions>` +
		`<grant name="g"><subject_name>CN=x</subject_name>` +
		`<validity><not_before>2020-01-01T00:00:00Z</not_before>` +
		`<not_after>2040-01-01T00:00:00Z</not_after></validity>` +
		`<allow_rule><topics><topic>t</topic></topics></allow_rule>` +
		`</grant></permissions>`))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	grant := access.GrantForSubject(doc, "CN=x", time.Now())
	if grant == nil {
		t.Fatalf("grant must resolve")
	}

	if err := access.CheckTypeConsistencyacc9("T", "T", true); err != nil {
		t.Fatalf("identical types must assign: %v", err)
	}

	if err := access.CheckTypeConsistencyacc9("T1", "T2", false); err == nil {
		t.Fatalf("mismatched types must deny")
	}
}

// TestSecGapsFileChangeReloadacc9 pins mtime-triggered reload.
func TestSecGapsFileChangeReloadacc9(t *testing.T) {
	t.Parallel()

	ca, caKey, caPEM := auditCA(t, "gaps-reload-ca")
	_, _, localPEM := auditLeaf(t, ca, caKey, "local", 220)
	_, _, peerPEM := auditLeaf(t, ca, caKey, "peer", 221)

	dir := t.TempDir()
	caPath := writeGapsFile(t, dir, "ca.pem", caPEM)
	certPath := writeGapsFile(t, dir, "cert.pem", localPEM)
	keyPath := writeGapsFile(t, dir, "key.pem", localPEM)

	govSigned, err := access.SignEnvelope(auditGovernance(), ca, caKey)
	if err != nil {
		t.Fatalf("sign gov: %v", err)
	}

	govPath := writeGapsFile(t, dir, "governance.xml", govSigned)

	before, after := auditValidity()
	allowPerms := []byte(`<permissions><grant name="peer"><subject_name>CN=peer</subject_name>` +
		`<validity><not_before>` + before + `</not_before><not_after>` + after + `</not_after></validity>` +
		`<allow_rule><domains><id>0</id></domains><topics><topic>rt/chatter</topic></topics></allow_rule>` +
		`</grant></permissions>`)
	allowSigned, err := access.SignEnvelope(allowPerms, ca, caKey)
	if err != nil {
		t.Fatalf("sign allow: %v", err)
	}

	permsPath := writeGapsFile(t, dir, "permissions.xml", allowSigned)

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

	if err := suite.AuthenticatePeer("audit-peer", peerPEM); err != nil {
		t.Fatalf("authenticate: %v", err)
	}

	if err := suite.CheckRemoteEndpoint(auditPeerDescriptor(0), "rt/chatter", false); err != nil {
		t.Fatalf("allow must pass: %v", err)
	}

	denyPerms := []byte(`<permissions><grant name="peer"><subject_name>CN=peer</subject_name>` +
		`<validity><not_before>` + before + `</not_before><not_after>` + after + `</not_after></validity>` +
		`<allow_rule><domains><id>0</id></domains><topics><topic>rt/other</topic></topics></allow_rule>` +
		`</grant></permissions>`)
	denySigned, err := access.SignEnvelope(denyPerms, ca, caKey)
	if err != nil {
		t.Fatalf("sign deny: %v", err)
	}

	future := time.Now().Add(2 * time.Second)
	if err := os.Chtimes(permsPath, future, future); err != nil {
		t.Fatalf("chtimes: %v", err)
	}

	if err := os.WriteFile(permsPath, denySigned, 0o600); err != nil {
		t.Fatalf("rotate: %v", err)
	}

	if err := os.Chtimes(permsPath, future.Add(time.Second), future.Add(time.Second)); err != nil {
		t.Fatalf("chtimes2: %v", err)
	}

	err = suite.CheckRemoteEndpoint(auditPeerDescriptor(0), "rt/chatter", false)
	if !errors.Is(err, ErrNotAllowedBySecurity) {
		t.Fatalf("mtime-rotated files must flip to deny without explicit reload, got %v", err)
	}
}

// TestSecGapsRekeyOnEnableKeyRevisionacc9 pins rekey signalling.
func TestSecGapsRekeyOnEnableKeyRevisionacc9(t *testing.T) {
	t.Parallel()

	rule := `<domain_rule><domains><id>7</id></domains>` +
		`<allow_unauthenticated_participants>false</allow_unauthenticated_participants>` +
		`<enable_join_access_control>false</enable_join_access_control>` +
		`<enable_key_revision>true</enable_key_revision>` +
		`<discovery_protection_kind>SIGN</discovery_protection_kind>` +
		`<liveliness_protection_kind>SIGN</liveliness_protection_kind>` +
		`<rtps_protection_kind>SIGN</rtps_protection_kind>` +
		`<allowed_crypto_algorithms><algorithm>ECDHE-CEUM+P256</algorithm><algorithm>AES256+GCM</algorithm></allowed_crypto_algorithms>` +
		`<topic_access_rules><topic_rule>` +
		`<topic_expression>rt/chatter</topic_expression>` +
		`<enable_discovery_protection>false</enable_discovery_protection>` +
		`<enable_liveliness_protection>false</enable_liveliness_protection>` +
		`<enable_read_access_control>false</enable_read_access_control>` +
		`<enable_write_access_control>false</enable_write_access_control>` +
		`<metadata_protection_kind>NONE</metadata_protection_kind>` +
		`<data_protection_kind>NONE</data_protection_kind>` +
		`</topic_rule></topic_access_rules></domain_rule>`

	doc, err := access.ParseGovernance([]byte(`<dds><domain_access_rules>` + rule + `</domain_access_rules></dds>`))
	if err != nil {
		t.Fatalf("parse gov: %v", err)
	}

	if !RekeyOnEnableKeyRevision(doc, 7, "") {
		t.Fatalf("enable_key_revision must request rekey")
	}

	if RekeyOnEnableKeyRevision(doc, 8, "") {
		t.Fatalf("other domain must not request rekey")
	}

	if RekeyOnEnableKeyRevision(nil, 7, "") {
		t.Fatalf("nil governance must not request rekey")
	}
}

func writeGapsFile(t *testing.T, dir, name string, content []byte) string {
	t.Helper()

	path := dir + "/" + name
	if err := os.WriteFile(path, content, 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}

	return path
}
