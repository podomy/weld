// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

package auth

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"math/big"
	"testing"
	"time"

	"github.com/podomy/weld/internal/transports/dds/security/tokens"
)

// testIdentity mints one EC identity with a self-signed certificate.
func testIdentity(t *testing.T, name string) (*ecdsa.PrivateKey, []byte) {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}

	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: name},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
	}

	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create certificate: %v", err)
	}

	return key, der
}

// TestSignHandshakeECDSA pins EC signing plus verified verification.
func TestSignHandshakeECDSA(t *testing.T) {
	t.Parallel()

	key, der := testIdentity(t, "signer")

	content := bytes.Repeat([]byte{0xab}, 64)

	sig, err := SignHandshakeData(key, content)
	if err != nil {
		t.Fatalf("sign: %v", err)
	}

	if len(sig) == 0 {
		t.Fatalf("signature must not be empty")
	}

	if err := VerifySignature(der, content, sig); err != nil {
		t.Fatalf("verify: %v", err)
	}

	other := bytes.Repeat([]byte{0xac}, 64)

	if err := VerifySignature(der, other, sig); err == nil {
		t.Fatalf("forged content must fail")
	}
}

// TestSignHandshakeRejects pins fail-closed signing.
func TestSignHandshakeRejects(t *testing.T) {
	t.Parallel()

	key, _ := testIdentity(t, "signer")

	if _, err := SignHandshakeData(nil, []byte{0x01}); err == nil {
		t.Fatalf("nil key must fail")
	}

	if _, err := SignHandshakeData(key, nil); err == nil {
		t.Fatalf("empty content must fail")
	}

	rsaPEM := []byte("-----BEGIN RSA PRIVATE KEY-----\n-----END RSA PRIVATE KEY-----\n")

	if _, err := ParseECPrivateKey(rsaPEM); err == nil {
		t.Fatalf("non EC key must fail")
	}
}

// TestHandshakeSignedLoopback drives a signed handshake between managers.
func TestHandshakeSignedLoopback(t *testing.T) {
	t.Parallel()

	managerA, managerB := signedLoopbackManagers(t)

	now := time.Now()
	reqID, replyID, final := signedLoopbackRequest(t, managerA, managerB, now)
	signedLoopbackFinal(t, managerA, managerB, reqID+replyID+1, final)
}

// signedLoopbackManagers builds two managers with CA-chained EC identities.
//
// Each side carries valid participant data bound to its certificate per
// 10.5 and trusts the shared CA pool, so the fail-closed pdata and
// trust-pool checks pass for the loopback pair.
func signedLoopbackManagers(t *testing.T) (*Manager, *Manager) {
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

	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate CA: %v", err)
	}

	caTemplate := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "loopback-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
	}

	caDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatalf("create CA: %v", err)
	}

	ca, err := x509.ParseCertificate(caDER)
	if err != nil {
		t.Fatalf("parse CA: %v", err)
	}

	pool := x509.NewCertPool()
	pool.AddCert(ca)

	installLoopbackLeaf(t, managerA, ca, caKey, "A", "perms-A")
	installLoopbackLeaf(t, managerB, ca, caKey, "B", "perms-B")

	managerA.SetTrustPool(pool)
	managerB.SetTrustPool(pool)

	return managerA, managerB
}

// installLoopbackLeaf mints one CA-chained leaf with valid pdata.
func installLoopbackLeaf(t *testing.T, manager *Manager, ca *x509.Certificate, caKey *ecdsa.PrivateKey, name, perms string) {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate leaf: %v", err)
	}

	template := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		Subject:      pkix.Name{CommonName: name},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
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

	prefix, err := AdjustedGUIDPrefix(cert)
	if err != nil {
		t.Fatalf("prefix: %v", err)
	}

	pdata := tokens.EncodeParticipantData(prefix)

	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatalf("marshal %s: %v", name, err)
	}

	if err := manager.SetIdentityMaterial(der, []byte(perms), pdata, keyDER); err != nil {
		t.Fatalf("identity %s: %v", name, err)
	}
}

// signedLoopbackRequest drives the request plus reply half.
func signedLoopbackRequest(t *testing.T, managerA, managerB *Manager, now time.Time) (uint64, uint64, tokens.HandshakeFinalToken) {
	t.Helper()

	req, reqID, err := managerA.StartInitiator("participant-B", now)
	if err != nil {
		t.Fatalf("start: %v", err)
	}

	if req.KeyAgreeAlgo != tokens.KAgreeECDH || req.SignAlgo != tokens.DSignAlgoECDSA {
		t.Fatalf("spec algos %q/%q", req.KeyAgreeAlgo, req.SignAlgo)
	}

	parsedReq := roundTripRequest(t, req)

	reply, replyID, err := managerB.HandleRequest("participant-A", parsedReq, reqID, now)
	if err != nil {
		t.Fatalf("request: %v", err)
	}

	if len(reply.Signature) == 0 {
		t.Fatalf("reply must be signed")
	}

	parsedReply := roundTripReply(t, reply)

	final, _, err := managerA.HandleReply("participant-B", parsedReply, replyID, now)
	if err != nil {
		t.Fatalf("reply: %v", err)
	}

	if len(final.Signature) == 0 {
		t.Fatalf("final must be signed")
	}

	return reqID, replyID, final
}

// roundTripRequest encodes and decodes one request token.
func roundTripRequest(t *testing.T, req tokens.HandshakeRequestToken) tokens.HandshakeRequestToken {
	t.Helper()

	back := roundTripHolder(t, mustBuildRequest(t, req))

	parsed, err := tokens.ParseHandshakeRequest(back)
	if err != nil {
		t.Fatalf("parse req: %v", err)
	}

	return parsed
}

// mustBuildRequest builds one request DataHolder.
func mustBuildRequest(t *testing.T, req tokens.HandshakeRequestToken) tokens.DataHolder {
	t.Helper()

	holder, err := tokens.BuildHandshakeRequest(req)
	if err != nil {
		t.Fatalf("build req: %v", err)
	}

	return holder
}

// roundTripHolder encodes and decodes one DataHolder.
func roundTripHolder(t *testing.T, holder tokens.DataHolder) tokens.DataHolder {
	t.Helper()

	raw, err := tokens.EncodeDataHolder(holder)
	if err != nil {
		t.Fatalf("encode holder: %v", err)
	}

	back, err := tokens.DecodeDataHolder(raw, false)
	if err != nil {
		t.Fatalf("decode holder: %v", err)
	}

	return back
}

// roundTripReply encodes and decodes one reply token.
func roundTripReply(t *testing.T, reply tokens.HandshakeReplyToken) tokens.HandshakeReplyToken {
	t.Helper()

	holder, err := tokens.BuildHandshakeReply(reply)
	if err != nil {
		t.Fatalf("build reply: %v", err)
	}

	back := roundTripHolder(t, holder)

	parsed, err := tokens.ParseHandshakeReply(back)
	if err != nil {
		t.Fatalf("parse reply: %v", err)
	}

	return parsed
}

// signedLoopbackFinal drives the final half and checks key agreement.
func signedLoopbackFinal(t *testing.T, managerA, managerB *Manager, finalID uint64, final tokens.HandshakeFinalToken) {
	t.Helper()

	holder, err := tokens.BuildHandshakeFinal(final)
	if err != nil {
		t.Fatalf("build final: %v", err)
	}

	raw, err := tokens.EncodeDataHolder(holder)
	if err != nil {
		t.Fatalf("encode final: %v", err)
	}

	back, err := tokens.DecodeDataHolder(raw, false)
	if err != nil {
		t.Fatalf("decode final: %v", err)
	}

	parsed, err := tokens.ParseHandshakeFinal(back)
	if err != nil {
		t.Fatalf("parse final: %v", err)
	}

	if err := managerB.HandleFinal("participant-A", parsed, finalID); err != nil {
		t.Fatalf("final: %v", err)
	}

	secretA, err := managerA.SharedSecret("participant-B")
	if err != nil {
		t.Fatalf("secret A: %v", err)
	}

	secretB, err := managerB.SharedSecret("participant-A")
	if err != nil {
		t.Fatalf("secret B: %v", err)
	}

	if !bytes.Equal(secretA, secretB) {
		t.Fatalf("secrets differ")
	}

	cert, err := managerA.PeerCertificate("participant-B")
	if err != nil {
		t.Fatalf("peer cert: %v", err)
	}

	if len(cert) == 0 {
		t.Fatalf("peer cert must not be empty")
	}
}

// TestHandshakeTamperedSignatureFails pins fail-closed signature checks.
func TestHandshakeTamperedSignatureFails(t *testing.T) {
	t.Parallel()

	managerA, managerB := signedLoopbackManagers(t)

	now := time.Now()

	req, _, err := managerA.StartInitiator("B", now)
	if err != nil {
		t.Fatalf("start: %v", err)
	}

	reply, _, err := managerB.HandleRequest("A", req, 1, now)
	if err != nil {
		t.Fatalf("request: %v", err)
	}

	reply.Signature = bytes.Repeat([]byte{0xde}, 64)

	if _, _, err := managerA.HandleReply("B", reply, 2, now); err == nil {
		t.Fatalf("tampered signature must fail")
	}
}

// TestNewAuthenticatorRejectsEmpty pins fail-closed construction.
func TestNewAuthenticatorRejectsEmpty(t *testing.T) {
	t.Parallel()

	if _, err := NewAuthenticator(""); err == nil {
		t.Fatal("empty agreement must fail, not default")
	}
}

// TestHandshakeUnsignedFailsClosed pins unsigned handshakes never authenticate.
//
// Unsigned emission requires explicit loopback opt-in; the reply without
// identity material still fails closed and never authenticates.
func TestHandshakeUnsignedFailsClosed(t *testing.T) {
	t.Parallel()

	authA, err := NewAuthenticator(KAgreeP256)
	if err != nil {
		t.Fatalf("auth A: %v", err)
	}

	authB, err := NewAuthenticator(KAgreeP256)
	if err != nil {
		t.Fatalf("auth B: %v", err)
	}

	managerA, err := NewManager("A", authA)
	if err != nil {
		t.Fatalf("manager A: %v", err)
	}

	managerB, err := NewManager("B", authB)
	if err != nil {
		t.Fatalf("manager B: %v", err)
	}

	managerA.SetAllowUnauthenticated(true)
	managerB.SetAllowUnauthenticated(true)

	now := time.Now()

	req, _, err := managerA.StartInitiator("B", now)
	if err != nil {
		t.Fatalf("start: %v", err)
	}

	if _, _, err := managerB.HandleRequest("A", req, 1, now); err == nil {
		t.Fatalf("unsigned reply must fail closed")
	}

	if managerB.Authenticated("A") {
		t.Fatalf("unsigned peer must not authenticate")
	}
}

// TestHandleReplyRejectsKAgreeMismatch pins echo binding on the reply.
func TestHandleReplyRejectsKAgreeMismatch(t *testing.T) {
	t.Parallel()

	mismatched := func(t *testing.T, algo string) {
		t.Helper()

		managerA, managerB := signedLoopbackManagers(t)
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

		reply.KeyAgreeAlgo = algo

		if _, _, err := managerA.HandleReply("participant-B", reply, replyID, now); err == nil {
			t.Fatalf("kagree echo %q must fail", algo)
		}
	}

	mismatched(t, KAgreeP384)
	mismatched(t, "BOGUS-ALGO")
}

// TestHandleRequestRejectsForeignCurve pins capability matching on receipt.
func TestHandleRequestRejectsForeignCurve(t *testing.T) {
	t.Parallel()

	authB, err := NewAuthenticator(KAgreeP256)
	if err != nil {
		t.Fatalf("auth B: %v", err)
	}

	managerB, err := NewManager("participant-B", authB)
	if err != nil {
		t.Fatalf("manager B: %v", err)
	}

	challenge, err := NewChallenge()
	if err != nil {
		t.Fatalf("challenge: %v", err)
	}

	req := tokens.HandshakeRequestToken{
		Challenge1:   challenge,
		DH1:          []byte{0x04, 0x05, 0x06},
		KeyAgreeAlgo: KAgreeP384,
		SignAlgo:     tokens.DSignAlgoECDSA,
	}

	if _, _, err := managerB.HandleRequest("participant-A", req, 1, time.Now()); err == nil {
		t.Fatal("P-384 offer against P-256 keys must fail")
	}
}
