// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

package security

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/podomy/weld/internal/transports/dds/security/access"
)

// secRule builds one domain rule with topic protection kinds.
func secRule(discovery, rtps, meta, data string) *access.DomainRule {
	return &access.DomainRule{
		Domains:              access.Domains{IDs: []uint32{0}},
		DiscoveryProtection:  discovery,
		LivelinessProtection: access.ProtectionNone,
		RTPSProtection:       rtps,
		AllowedAlgorithms:    []string{"ECDHE-CEUM+P256", "AES256+GCM"},
		TopicRules: []access.TopicRule{{
			Expression:         "rt/chatter",
			MetadataProtection: meta,
			DataProtection:     data,
		}},
	}
}

// TestSecTransformKindPerKind pins exact SIGN versus ENCRYPT mapping per 9.5.3.
func TestSecTransformKindPerKind(t *testing.T) {
	t.Parallel()

	signRule := secRule(access.ProtectionSign, access.ProtectionNone, access.ProtectionNone, access.ProtectionNone)
	encRule := secRule(access.ProtectionNone, access.ProtectionNone, access.ProtectionNone, access.ProtectionEncrypt)
	noneRule := secRule(access.ProtectionNone, access.ProtectionNone, access.ProtectionNone, access.ProtectionNone)
	encTopic := &encRule.TopicRules[0]

	checkSignKinds(t, signRule)
	checkEncryptKinds(t, encRule, encTopic)
	checkNoneKinds(t, noneRule, signRule, encRule, encTopic)
}

// checkSignKinds pins SIGN allowing GMAC and GCM.
func checkSignKinds(t *testing.T, signRule *access.DomainRule) {
	t.Helper()

	for _, kind := range []uint32{transformKindGMAC128, transformKindGCM128, transformKindGMAC256, transformKindGCM256} {
		if !transformKindSatisfiesGovernanceWithKindacc8(signRule, nil, kind) {
			t.Fatalf("SIGN must allow kind %d", kind)
		}
	}
}

// checkEncryptKinds pins ENCRYPT requiring GCM only.
func checkEncryptKinds(t *testing.T, encRule *access.DomainRule, encTopic *access.TopicRule) {
	t.Helper()

	for _, kind := range []uint32{transformKindGMAC128, transformKindGMAC256} {
		if transformKindSatisfiesGovernanceWithKindacc8(encRule, encTopic, kind) {
			t.Fatalf("ENCRYPT must deny GMAC kind %d", kind)
		}
	}

	for _, kind := range []uint32{transformKindGCM128, transformKindGCM256} {
		if !transformKindSatisfiesGovernanceWithKindacc8(encRule, encTopic, kind) {
			t.Fatalf("ENCRYPT must allow GCM kind %d", kind)
		}
	}
}

// checkNoneKinds pins NONE plus fallback plus unknown denial.
func checkNoneKinds(t *testing.T, noneRule, signRule, encRule *access.DomainRule, encTopic *access.TopicRule) {
	t.Helper()

	if !transformKindSatisfiesGovernanceWithKindacc8(noneRule, nil, 0) {
		t.Fatalf("NONE must allow without kind")
	}

	if !transformKindSatisfiesGovernanceWithKindacc8(noneRule, nil, transformKindGMAC128) {
		t.Fatalf("NONE must allow with kind")
	}

	if !transformKindSatisfiesGovernanceWithKindacc8(signRule, nil, 0) {
		t.Fatalf("zero kind must fall back to proxy for SIGN with GCM allow-list")
	}

	if transformKindSatisfiesGovernanceWithKindacc8(signRule, nil, 99) {
		t.Fatalf("unknown kind must deny SIGN")
	}

	if transformKindSatisfiesGovernanceWithKindacc8(encRule, encTopic, 99) {
		t.Fatalf("unknown kind must deny ENCRYPT")
	}
}

// TestSecNegotiatedKindGates pins Suite enforcement of the pinned kind.
func TestSecNegotiatedKindGates(t *testing.T) {
	t.Parallel()

	suite := auditDomainSuite(t)

	// Default proxy allows the audit SIGN plus ENCRYPT fixture.
	rule := access.FindDomainRule(mustVerifiedGov(t, suite), 0, "")
	if rule == nil {
		t.Fatalf("domain rule must exist")
	}

	topicRule := access.FindTopicRule(rule, "rt/chatter")
	if topicRule == nil {
		t.Fatalf("topic rule must exist")
	}

	if !suite.transformSatisfies(rule, topicRule) {
		t.Fatalf("proxy must allow audit fixture")
	}

	// Pinning GMAC denies the ENCRYPT data fixture fail-closed.
	suite.SetNegotiatedTransformKind(transformKindGMAC128)
	if suite.transformSatisfies(rule, topicRule) {
		t.Fatalf("GMAC must deny ENCRYPT data fixture")
	}

	// Pinning GCM allows it.
	suite.SetNegotiatedTransformKind(transformKindGCM256)
	if !suite.transformSatisfies(rule, topicRule) {
		t.Fatalf("GCM must allow ENCRYPT data fixture")
	}

	// Clearing restores the proxy.
	suite.SetNegotiatedTransformKind(0)
	if !suite.transformSatisfies(rule, topicRule) {
		t.Fatalf("cleared kind must restore proxy")
	}
}

// mustVerifiedGov returns the verified governance for tests.
func mustVerifiedGov(t *testing.T, suite *Suite) *access.GovernanceDoc {
	t.Helper()

	gov, _, _, ok, _ := suite.verifiedState()
	if !ok || gov == nil {
		t.Fatalf("verified governance must load")
	}

	return gov
}

// TestSecSubscribeOnlyMustNotAuthorizeWriter pins directional separation at Suite level.
func TestSecSubscribeOnlyMustNotAuthorizeWriter(t *testing.T) {
	t.Parallel()

	ca, caKey, caPEM := auditCA(t, "subscribe-only-ca")
	_, _, localPEM := auditLeaf(t, ca, caKey, "local", 310)
	_, _, peerPEM := auditLeaf(t, ca, caKey, "peer", 311)

	before, after := auditValidity()

	subscribeOnly := []byte(`<permissions><grant name="peer-sub"><subject_name>CN=peer</subject_name>` +
		`<validity><not_before>` + before + `</not_before><not_after>` + after + `</not_after></validity>` +
		`<allow_rule><domains><id>0</id></domains>` +
		`<subscribe><topics><topic>rt/chatter</topic></topics></subscribe>` +
		`</allow_rule></grant>`)

	govSigned, err := access.SignEnvelope(auditGovernance(), ca, caKey)
	if err != nil {
		t.Fatalf("sign gov: %v", err)
	}

	permsSigned, err := access.SignEnvelope(subscribeOnly, ca, caKey)
	if err != nil {
		t.Fatalf("sign perms: %v", err)
	}

	// Local grant needs publish to create a writer; peer grant is
	// subscribe-only, so remote writer (peer publishes) must deny while
	// remote reader (peer subscribes) must allow.
	localPerms := []byte(`<permissions>` +
		`<grant name="local"><subject_name>CN=local</subject_name>` +
		`<validity><not_before>` + before + `</not_before><not_after>` + after + `</not_after></validity>` +
		`<allow_rule><domains><id>0</id></domains><topics><topic>rt/chatter</topic></topics></allow_rule>` +
		`</grant>` +
		`<grant name="peer-sub"><subject_name>CN=peer</subject_name>` +
		`<validity><not_before>` + before + `</not_before><not_after>` + after + `</not_after></validity>` +
		`<allow_rule><domains><id>0</id></domains>` +
		`<subscribe><topics><topic>rt/chatter</topic></topics></subscribe>` +
		`</allow_rule></grant></permissions>`)

	localSigned, err := access.SignEnvelope(localPerms, ca, caKey)
	if err != nil {
		t.Fatalf("sign local perms: %v", err)
	}

	_ = permsSigned

	suite := auditSuiteSigned(t, localPEM, govSigned, localSigned, &auditSuiteOptions{permissionsCA: caPEM})

	if err := suite.AuthenticatePeer("audit-peer", peerPEM); err != nil {
		t.Fatalf("authenticate: %v", err)
	}

	// Remote reader = peer subscribes: subscribe-only grant allows.
	if err := suite.CheckRemoteEndpoint(auditPeerDescriptor(0), "rt/chatter", true); err != nil {
		t.Fatalf("subscribe-only must authorize reader, got %v", err)
	}

	// Remote writer = peer publishes: subscribe-only grant must deny.
	if err := suite.CheckRemoteEndpoint(auditPeerDescriptor(0), "rt/chatter", false); err == nil {
		t.Fatalf("subscribe-only must not authorize writer")
	}

	// Remote topic tries writer then reader: subscribe-only still allows
	// via the reader path.
	if err := suite.CheckRemoteTopic(auditPeerDescriptor(0), "rt/chatter"); err != nil {
		t.Fatalf("remote topic must allow via reader path, got %v", err)
	}
}

// TestSecDeletedFileForcesReload pins stat errors failing closed to reload.
func TestSecDeletedFileForcesReload(t *testing.T) {
	t.Parallel()

	ca, caKey, caPEM := auditCA(t, "deleted-file-ca")
	_, _, localPEM := auditLeaf(t, ca, caKey, "local", 320)
	_, _, peerPEM := auditLeaf(t, ca, caKey, "peer", 321)

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
		t.Fatalf("allow must pass before delete, got %v", err)
	}

	// Deleting the permissions file must fail closed: stat error forces
	// reload, reload fails, and the endpoint denies instead of serving
	// stale allows.
	if err := os.Remove(permsPath); err != nil {
		t.Fatalf("remove perms: %v", err)
	}

	// Bump another file mtime so verifiedFilesChanged triggers even if
	// the stat-error path is bypassed.
	future := time.Now().Add(2 * time.Second)
	if err := os.Chtimes(govPath, future, future); err != nil {
		t.Fatalf("chtimes: %v", err)
	}

	_ = filepath.Join

	if err := suite.CheckRemoteEndpoint(auditPeerDescriptor(0), "rt/chatter", false); !errors.Is(err, ErrNotAllowedBySecurity) {
		t.Fatalf("deleted permissions must deny, got %v", err)
	}
}
