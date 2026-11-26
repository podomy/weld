// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

package auth

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"errors"
	"math/big"
	"testing"
	"time"

	"github.com/podomy/weld/internal/transports/dds/security/tokens"
)

// mustMarshalECauth7 marshals one EC key for identity installation.
func mustMarshalECauth7(t *testing.T, key *ecdsa.PrivateKey) []byte {
	t.Helper()

	der, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatalf("marshal EC: %v", err)
	}

	return der
}

// TestSelfRequestCIDauth7 pins c.id self suppression before state change.
func TestSelfRequestCIDauth7(t *testing.T) {
	t.Parallel()

	authB, err := NewAuthenticator(KAgreeP256)
	if err != nil {
		t.Fatalf("auth B: %v", err)
	}

	managerB, err := NewManager("participant-B", authB)
	if err != nil {
		t.Fatalf("manager B: %v", err)
	}

	keyB, certB := testIdentity(t, "B-self")
	derB := mustMarshalECauth7(t, keyB)

	if err := managerB.SetIdentityMaterial(certB, []byte("perms-B"), []byte("pdata-B"), derB); err != nil {
		t.Fatalf("identity B: %v", err)
	}

	challenge, err := NewChallenge()
	if err != nil {
		t.Fatalf("challenge: %v", err)
	}

	selfReq := tokens.HandshakeRequestToken{
		Challenge1:      challenge,
		DH1:             []byte{0x04, 0x05, 0x06},
		KeyAgreeAlgo:    KAgreeP256,
		SignAlgo:        tokens.DSignAlgoECDSA,
		IdentityCert:    append([]byte(nil), certB...),
		Permissions:     []byte("perms-B"),
		ParticipantData: []byte("pdata-B"),
	}

	if _, _, err := managerB.HandleRequest("participant-A", selfReq, 99, time.Now()); !errors.Is(err, ErrSelfauth7) {
		t.Fatalf("self c.id must drop, got %v", err)
	}

	if got := managerB.Validation("participant-A"); got != ValidationFailed {
		t.Fatalf("self must not authenticate, got %d", int(got))
	}
}

// TestSelfRequestPDataauth7 pins pdata GUID self suppression.
func TestSelfRequestPDataauth7(t *testing.T) {
	t.Parallel()

	authB, err := NewAuthenticator(KAgreeP256)
	if err != nil {
		t.Fatalf("auth B: %v", err)
	}

	managerB, err := NewManager("participant-B", authB)
	if err != nil {
		t.Fatalf("manager B: %v", err)
	}

	_, certA := testIdentity(t, "A-other")
	keyB, certB := testIdentity(t, "B-self")
	derB := mustMarshalECauth7(t, keyB)

	if err := managerB.SetIdentityMaterial(certB, []byte("perms-B"), []byte("pdata-B"), derB); err != nil {
		t.Fatalf("identity B: %v", err)
	}

	parsedB, err := ParseCertificatePEM(certB)
	if err != nil {
		t.Fatalf("parse B: %v", err)
	}

	prefixB, err := AdjustedGUIDPrefix(parsedB)
	if err != nil {
		t.Fatalf("prefix B: %v", err)
	}

	challenge, err := NewChallenge()
	if err != nil {
		t.Fatalf("challenge: %v", err)
	}

	selfReq := tokens.HandshakeRequestToken{
		Challenge1:      challenge,
		DH1:             []byte{0x04, 0x05, 0x06},
		KeyAgreeAlgo:    KAgreeP256,
		SignAlgo:        tokens.DSignAlgoECDSA,
		IdentityCert:    append([]byte(nil), certA...),
		Permissions:     []byte("perms-A"),
		ParticipantData: tokens.EncodeParticipantData(prefixB),
	}

	if _, _, err := managerB.HandleRequest("participant-A", selfReq, 100, time.Now()); !errors.Is(err, ErrSelfauth7) {
		t.Fatalf("self pdata must drop, got %v", err)
	}
}

// TestSelfReplyCIDauth7 pins reply self suppression before secret derivation.
func TestSelfReplyCIDauth7(t *testing.T) {
	t.Parallel()
	managerA, managerB := newSelfReplyManagers(t)
	ca, caKey, pool := gapsCA(t, "self-reply-ca")
	certA := installSelfReplyIdentities(t, managerA, managerB, ca, caKey, pool)
	checkSelfReplyDrops(t, managerA, managerB, certA)
}

// newSelfReplyManagers builds two P256 managers.
func newSelfReplyManagers(t *testing.T) (*Manager, *Manager) {
	t.Helper()
	authA, err := NewAuthenticator(KAgreeP256)
	if err != nil {
		t.Fatalf("auth A: %v", err)
	}
	authB, err := NewAuthenticator(KAgreeP256)
	if err != nil {
		t.Fatalf("auth B: %v", err)
	}
	managerA, err := NewManager("participant-A", authA)
	if err != nil {
		t.Fatalf("manager A: %v", err)
	}
	managerB, err := NewManager("participant-B", authB)
	if err != nil {
		t.Fatalf("manager B: %v", err)
	}
	return managerA, managerB
}

// installSelfReplyIdentities installs A-self plus B-other leaves.
func installSelfReplyIdentities(t *testing.T, managerA, managerB *Manager, ca *x509.Certificate, caKey *ecdsa.PrivateKey, pool *x509.CertPool) []byte {
	t.Helper()
	certA, keyA := gapsLeaf(t, ca, caKey, "A-self", 40)
	certB, keyB := gapsLeaf(t, ca, caKey, "B-other", 41)
	derA, err := x509.MarshalECPrivateKey(keyA)
	if err != nil {
		t.Fatalf("marshal A: %v", err)
	}
	derB, err := x509.MarshalECPrivateKey(keyB)
	if err != nil {
		t.Fatalf("marshal B: %v", err)
	}
	if err := managerA.SetIdentityMaterial(certA, []byte("perms-A"), gapsPData(t, certA), derA); err != nil {
		t.Fatalf("identity A: %v", err)
	}
	if err := managerB.SetIdentityMaterial(certB, []byte("perms-B"), gapsPData(t, certB), derB); err != nil {
		t.Fatalf("identity B: %v", err)
	}
	managerA.SetTrustPool(pool)
	managerB.SetTrustPool(pool)
	return certA
}

// checkSelfReplyDrops drives handshake and verifies self reply drops.
func checkSelfReplyDrops(t *testing.T, managerA, managerB *Manager, certA []byte) {
	t.Helper()
	now := time.Now()
	req, reqID, err := managerA.StartInitiator("participant-B", now)
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	parsedReq := roundTripRequest(t, req)
	reply, replyID, err := managerB.HandleRequest("participant-A", parsedReq, reqID, now)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	selfReply := reply
	selfReply.IdentityCert = append([]byte(nil), certA...)
	if _, _, err := managerA.HandleReply("participant-B", selfReply, replyID, now); !errors.Is(err, ErrSelfauth7) {
		t.Fatalf("self reply must drop, got %v", err)
	}
	if got := managerA.Validation("participant-B"); got == ValidationOK || got == ValidationOKFinalMessage {
		t.Fatalf("self reply must not authenticate, got %d", int(got))
	}
}

// TestDistinctLoopbackStaysGreenauth7 pins distinct IDs still complete.
func TestDistinctLoopbackStaysGreenauth7(t *testing.T) {
	t.Parallel()

	managerA, managerB := signedLoopbackManagers(t)
	now := time.Now()
	reqID, replyID, final := signedLoopbackRequest(t, managerA, managerB, now)
	signedLoopbackFinal(t, managerA, managerB, reqID+replyID+1, final)
}

// TestPeerPDataMismatchauth7 pins pdata prefix binding against the peer cert.
func TestPeerPDataMismatchauth7(t *testing.T) {
	t.Parallel()

	_, certA := testIdentity(t, "pdata-A-auth7")
	_, certB := testIdentity(t, "pdata-B-auth7")

	certAParsed, err := ParseCertificatePEM(certA)
	if err != nil {
		t.Fatalf("parse A: %v", err)
	}

	prefixA, err := AdjustedGUIDPrefix(certAParsed)
	if err != nil {
		t.Fatalf("prefix A: %v", err)
	}

	goodPData := tokens.EncodeParticipantData(prefixA)

	if err := verifyPeerPDataauth7(certA, goodPData); err != nil {
		t.Fatalf("matching pdata must pass, got %v", err)
	}

	certBParsed, err := ParseCertificatePEM(certB)
	if err != nil {
		t.Fatalf("parse B: %v", err)
	}

	prefixB, err := AdjustedGUIDPrefix(certBParsed)
	if err != nil {
		t.Fatalf("prefix B: %v", err)
	}

	if prefixA == prefixB {
		t.Fatalf("distinct subjects must give distinct prefixes")
	}

	badPData := tokens.EncodeParticipantData(prefixB)

	if err := verifyPeerPDataauth7(certA, badPData); err == nil {
		t.Fatalf("mismatched pdata must fail")
	}

	if err := verifyPeerPDataauth7(certA, []byte("opaque-legacy")); err == nil {
		t.Fatalf("undecodable pdata with identity must fail closed")
	}

	if err := verifyPeerPDataauth7(certA, nil); err == nil {
		t.Fatalf("empty pdata with identity must fail closed")
	}

	if err := verifyPeerPDataauth7(nil, nil); err != nil {
		t.Fatalf("empty inputs must skip, got %v", err)
	}

	if err := verifyPeerPDataauth7(nil, []byte("opaque")); err != nil {
		t.Fatalf("pdata without cert has no binding to check, got %v", err)
	}
}

// TestPeerPDataEmptyWithIdentityFailsauth7 pins binding required per 10.5.
func TestPeerPDataEmptyWithIdentityFailsauth7(t *testing.T) {
	t.Parallel()

	_, cert := testIdentity(t, "pdata-empty-auth7")

	if err := verifyPeerPDataauth7(cert, nil); err == nil {
		t.Fatalf("empty pdata with identity must fail")
	}

	if err := verifyPeerPDataauth7(cert, []byte{}); err == nil {
		t.Fatalf("empty pdata with identity must fail")
	}
}

// TestPeerBindingRejectsChangeauth7 pins session binding to the first cert.
func TestPeerBindingRejectsChangeauth7(t *testing.T) {
	t.Parallel()

	if err := checkPeerBindingauth7(nil, []byte{0x01}); err != nil {
		t.Fatalf("empty stored must skip, got %v", err)
	}

	if err := checkPeerBindingauth7([]byte{0x01}, nil); err == nil {
		t.Fatalf("dropped cert must fail closed")
	}

	if err := checkPeerBindingauth7([]byte{0x01, 0x02}, []byte{0x01, 0x02}); err != nil {
		t.Fatalf("same cert must pass, got %v", err)
	}

	if err := checkPeerBindingauth7([]byte{0x01, 0x02}, []byte{0x03, 0x04}); err == nil {
		t.Fatalf("changed cert must fail")
	}
}

// TestPeerBindingSwapAttackauth7 pins cert swap across requests fails.
func TestPeerBindingSwapAttackauth7(t *testing.T) {
	t.Parallel()

	first := []byte{0x0a, 0x0b}
	second := []byte{0x0c, 0x0d}

	if err := checkPeerBindingauth7(nil, first); err != nil {
		t.Fatalf("first sight must pass, got %v", err)
	}

	if err := checkPeerBindingauth7(first, second); err == nil {
		t.Fatalf("swap to different cert must fail")
	}

	if err := checkPeerBindingauth7(first, nil); err == nil {
		t.Fatalf("swap to empty cert must fail")
	}

	if err := checkPeerBindingauth7(first, first); err != nil {
		t.Fatalf("same cert must pass, got %v", err)
	}
}

// TestRequestBindingIntegrationauth7 pins a changed cert cannot ride a session.
func TestRequestBindingIntegrationauth7(t *testing.T) {
	t.Parallel()

	authB, err := NewAuthenticator(KAgreeP256)
	if err != nil {
		t.Fatalf("auth B: %v", err)
	}

	managerB, err := NewManager("participant-B", authB)
	if err != nil {
		t.Fatalf("manager B: %v", err)
	}

	ca, caKey, pool := gapsCA(t, "bind-ca-auth7")
	managerB.SetTrustPool(pool)

	certB, keyB := gapsLeaf(t, ca, caKey, "bind-B-auth7", 20)
	derB, err := x509.MarshalECPrivateKey(keyB)
	if err != nil {
		t.Fatalf("marshal B: %v", err)
	}

	pdataB := gapsPData(t, certB)

	if err := managerB.SetIdentityMaterial(certB, []byte("perms-B"), pdataB, derB); err != nil {
		t.Fatalf("identity B: %v", err)
	}

	certA1, _ := gapsLeaf(t, ca, caKey, "bind-A1-auth7", 21)
	certA2, _ := gapsLeaf(t, ca, caKey, "bind-A2-auth7", 22)

	pdataA1 := gapsPData(t, certA1)
	pdataA2 := gapsPData(t, certA2)

	authA1, err := NewAuthenticator(KAgreeP256)
	if err != nil {
		t.Fatalf("auth A1: %v", err)
	}

	dh1, err := authA1.PublicKey()
	if err != nil {
		t.Fatalf("dh1: %v", err)
	}

	challenge1, err := NewChallenge()
	if err != nil {
		t.Fatalf("challenge: %v", err)
	}

	first := tokens.HandshakeRequestToken{
		Challenge1:      challenge1,
		DH1:             append([]byte(nil), dh1...),
		KeyAgreeAlgo:    KAgreeP256,
		SignAlgo:        tokens.DSignAlgoECDSA,
		IdentityCert:    append([]byte(nil), certA1...),
		Permissions:     []byte("perms-A1"),
		ParticipantData: pdataA1,
	}

	if _, _, err := managerB.HandleRequest("participant-A", first, 201, time.Now()); err != nil {
		t.Fatalf("first request must pass binding, got %v", err)
	}

	challenge2, err := NewChallenge()
	if err != nil {
		t.Fatalf("challenge: %v", err)
	}

	second := tokens.HandshakeRequestToken{
		Challenge1:      challenge2,
		DH1:             append([]byte(nil), dh1...),
		KeyAgreeAlgo:    KAgreeP256,
		SignAlgo:        tokens.DSignAlgoECDSA,
		IdentityCert:    append([]byte(nil), certA2...),
		Permissions:     []byte("perms-A2"),
		ParticipantData: pdataA2,
	}

	if _, _, err := managerB.HandleRequest("participant-A", second, 202, time.Now()); err == nil {
		t.Fatalf("changed cert must not ride the session")
	}

	if got := managerB.Validation("participant-A"); got != ValidationFailed {
		t.Fatalf("binding mismatch must fail the session, got %d", int(got))
	}
}

// TestRequestBindingDropToEmptyFailsauth7 pins empty cert after pinned fails.
func TestRequestBindingDropToEmptyFailsauth7(t *testing.T) {
	t.Parallel()

	authB, err := NewAuthenticator(KAgreeP256)
	if err != nil {
		t.Fatalf("auth B: %v", err)
	}

	managerB, err := NewManager("participant-B", authB)
	if err != nil {
		t.Fatalf("manager B: %v", err)
	}

	ca, caKey, pool := gapsCA(t, "bind-drop-ca-auth7")
	managerB.SetTrustPool(pool)

	certB, keyB := gapsLeaf(t, ca, caKey, "bind-B-drop", 30)
	derB, err := x509.MarshalECPrivateKey(keyB)
	if err != nil {
		t.Fatalf("marshal B: %v", err)
	}

	if err := managerB.SetIdentityMaterial(certB, []byte("perms-B"), gapsPData(t, certB), derB); err != nil {
		t.Fatalf("identity B: %v", err)
	}

	certA, _ := gapsLeaf(t, ca, caKey, "bind-A-drop", 31)

	authA, err := NewAuthenticator(KAgreeP256)
	if err != nil {
		t.Fatalf("auth A: %v", err)
	}

	dh1, err := authA.PublicKey()
	if err != nil {
		t.Fatalf("dh1: %v", err)
	}

	challenge1, err := NewChallenge()
	if err != nil {
		t.Fatalf("challenge: %v", err)
	}

	first := tokens.HandshakeRequestToken{
		Challenge1:      challenge1,
		DH1:             append([]byte(nil), dh1...),
		KeyAgreeAlgo:    KAgreeP256,
		SignAlgo:        tokens.DSignAlgoECDSA,
		IdentityCert:    append([]byte(nil), certA...),
		Permissions:     []byte("perms-A"),
		ParticipantData: gapsPData(t, certA),
	}

	if _, _, err := managerB.HandleRequest("participant-A", first, 301, time.Now()); err != nil {
		t.Fatalf("first request must pass, got %v", err)
	}

	challenge2, err := NewChallenge()
	if err != nil {
		t.Fatalf("challenge: %v", err)
	}

	dropped := tokens.HandshakeRequestToken{
		Challenge1:      challenge2,
		DH1:             append([]byte(nil), dh1...),
		KeyAgreeAlgo:    KAgreeP256,
		SignAlgo:        tokens.DSignAlgoECDSA,
		IdentityCert:    nil,
		Permissions:     []byte("perms-A"),
		ParticipantData: gapsPData(t, certA),
	}

	if _, _, err := managerB.HandleRequest("participant-A", dropped, 302, time.Now()); err == nil {
		t.Fatalf("dropped cert after pin must fail")
	}
}

// testIdentityOrgauth7 mints one EC identity with CN plus organization.
func testIdentityOrgauth7(t *testing.T, cn, org string) (*x509.Certificate, []byte) {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}

	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: cn, Organization: []string{org}},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
	}

	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create certificate: %v", err)
	}

	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("parse certificate: %v", err)
	}

	return cert, der
}

// TestFullDNBindingauth7 pins same CN different DN distinguishability.
//
// CN-only matching would conflate these two certificates; only the full
// DN distinguishes them, so all grant lookups use
// CertificateSubjectFullauth7.
func TestFullDNBindingauth7(t *testing.T) {
	t.Parallel()

	certA, _ := testIdentityOrgauth7(t, "same-cn-auth7", "org-one")
	certB, _ := testIdentityOrgauth7(t, "same-cn-auth7", "org-two")

	if certA.Subject.CommonName != certB.Subject.CommonName {
		t.Fatalf("test certificates must share CN, got %q vs %q", certA.Subject.CommonName, certB.Subject.CommonName)
	}

	fullA := CertificateSubjectFullauth7(certA)
	fullB := CertificateSubjectFullauth7(certB)

	if fullA == "" || fullB == "" {
		t.Fatalf("full DN must not be empty")
	}

	if fullA == fullB {
		t.Fatalf("full DN must distinguish orgs, both %q", fullA)
	}

	if CertificateSubjectFullauth7(nil) != "" {
		t.Fatalf("nil cert must give empty full DN")
	}
}
