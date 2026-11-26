// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

package auth

import (
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

// gapsCA builds one CA for trust-pool verification tests.
func gapsCA(t *testing.T, cn string) (*x509.Certificate, *ecdsa.PrivateKey, *x509.CertPool) {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate CA: %v", err)
	}

	template := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: cn},
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

	pool := x509.NewCertPool()
	pool.AddCert(cert)

	return cert, key, pool
}

// gapsLeaf builds one leaf under the CA.
func gapsLeaf(t *testing.T, ca *x509.Certificate, caKey *ecdsa.PrivateKey, cn string, serial int64) ([]byte, *ecdsa.PrivateKey) {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate leaf: %v", err)
	}

	template := &x509.Certificate{
		SerialNumber: big.NewInt(serial),
		Subject:      pkix.Name{CommonName: cn},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
	}

	der, err := x509.CreateCertificate(rand.Reader, template, ca, &key.PublicKey, caKey)
	if err != nil {
		t.Fatalf("create leaf: %v", err)
	}

	return der, key
}

// gapsManagers builds two managers with CA-chained identities.
//
// Participant data carries the adjusted GUID prefix of each leaf per
// 10.5 so pdata binding passes fail-closed checks.
func gapsManagers(t *testing.T, ca *x509.Certificate, caKey *ecdsa.PrivateKey) (*Manager, *Manager) {
	t.Helper()

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

	certA, keyA := gapsLeaf(t, ca, caKey, "A", 10)
	certB, keyB := gapsLeaf(t, ca, caKey, "B", 11)

	derA, err := x509.MarshalECPrivateKey(keyA)
	if err != nil {
		t.Fatalf("marshal A: %v", err)
	}

	derB, err := x509.MarshalECPrivateKey(keyB)
	if err != nil {
		t.Fatalf("marshal B: %v", err)
	}

	pdataA := gapsPData(t, certA)
	pdataB := gapsPData(t, certB)

	if err := managerA.SetIdentityMaterial(certA, []byte("perms-A"), pdataA, derA); err != nil {
		t.Fatalf("identity A: %v", err)
	}

	if err := managerB.SetIdentityMaterial(certB, []byte("perms-B"), pdataB, derB); err != nil {
		t.Fatalf("identity B: %v", err)
	}

	return managerA, managerB
}

// gapsPData encodes valid participant data for one leaf certificate.
func gapsPData(t *testing.T, certDER []byte) []byte {
	t.Helper()

	cert, err := ParseCertificatePEM(certDER)
	if err != nil {
		t.Fatalf("parse leaf for pdata: %v", err)
	}

	prefix, err := AdjustedGUIDPrefix(cert)
	if err != nil {
		t.Fatalf("prefix for pdata: %v", err)
	}

	return tokens.EncodeParticipantData(prefix)
}

// TestGapsTrustPoolVerifiesPeerMaterial pins 9.4.2.8 chain plus perm checks.
func TestGapsTrustPoolVerifiesPeerMaterial(t *testing.T) {
	t.Parallel()

	ca, caKey, pool := gapsCA(t, "gaps-ca")
	managerA, managerB := gapsManagers(t, ca, caKey)
	managerA.SetTrustPool(pool)
	managerB.SetTrustPool(pool)

	now := time.Now()
	gapsChainedRequestPasses(t, managerA, managerB, now)
	gapsEmptyPermFails(t, ca, caKey, pool, managerB, now)
	gapsUnchainedFails(t, pool, managerB, now)
}

// gapsChainedRequestPasses pins a chained request passing with material.
func gapsChainedRequestPasses(t *testing.T, managerA, managerB *Manager, now time.Time) {
	t.Helper()

	req, reqID, err := managerA.StartInitiator("B", now)
	if err != nil {
		t.Fatalf("start: %v", err)
	}

	if _, _, err := managerB.HandleRequest("A", req, reqID, now); err != nil {
		t.Fatalf("chained request with perm plus pdata must pass, got %v", err)
	}
}

// gapsEmptyPermFails pins empty c.perm failing with a trust pool.
func gapsEmptyPermFails(t *testing.T, ca *x509.Certificate, caKey *ecdsa.PrivateKey, pool *x509.CertPool, managerB *Manager, now time.Time) {
	t.Helper()

	authC, err := NewAuthenticator(KAgreeP256)
	if err != nil {
		t.Fatalf("auth C: %v", err)
	}

	managerC, err := NewManager("C", authC)
	if err != nil {
		t.Fatalf("manager C: %v", err)
	}

	certC, keyC := gapsLeaf(t, ca, caKey, "C", 12)
	derC, err := x509.MarshalECPrivateKey(keyC)
	if err != nil {
		t.Fatalf("marshal C: %v", err)
	}

	if err := managerC.SetIdentityMaterial(certC, nil, []byte("pdata-C"), derC); err != nil {
		t.Fatalf("identity C: %v", err)
	}

	managerC.SetTrustPool(pool)
	badReq, _, err := managerC.StartInitiator("B", now)
	if err != nil {
		t.Fatalf("start C: %v", err)
	}

	if _, _, err := managerB.HandleRequest("C", badReq, 5001, now); err == nil {
		t.Fatalf("empty c.perm must fail with trust pool")
	}
}

// gapsUnchainedFails pins an unchained certificate failing with a pool.
func gapsUnchainedFails(t *testing.T, pool *x509.CertPool, managerB *Manager, now time.Time) {
	t.Helper()

	otherCA, otherKey, _ := gapsCA(t, "other-ca")
	otherCert, otherKeyLeaf := gapsLeaf(t, otherCA, otherKey, "evil", 99)
	otherDER, err := x509.MarshalECPrivateKey(otherKeyLeaf)
	if err != nil {
		t.Fatalf("marshal evil: %v", err)
	}

	authE, err := NewAuthenticator(KAgreeP256)
	if err != nil {
		t.Fatalf("auth E: %v", err)
	}

	managerE, err := NewManager("E", authE)
	if err != nil {
		t.Fatalf("manager E: %v", err)
	}

	if err := managerE.SetIdentityMaterial(otherCert, []byte("perms"), []byte("pdata"), otherDER); err != nil {
		t.Fatalf("identity evil: %v", err)
	}

	managerE.SetTrustPool(pool)

	evilReq, _, err := managerE.StartInitiator("B", now)
	if err != nil {
		t.Fatalf("start evil: %v", err)
	}

	if _, _, err := managerB.HandleRequest("E", evilReq, 5002, now); err == nil {
		t.Fatalf("unchained peer must fail with trust pool")
	}
}

// TestGapsFullHandshakeAuthenticates pins Authenticated after final.
func TestGapsFullHandshakeAuthenticates(t *testing.T) {
	t.Parallel()

	ca, caKey, pool := gapsCA(t, "gaps-full-ca")
	managerA, managerB := gapsManagers(t, ca, caKey)
	managerA.SetTrustPool(pool)
	managerB.SetTrustPool(pool)

	now := time.Now()
	req, reqID, err := managerA.StartInitiator("B", now)
	if err != nil {
		t.Fatalf("start: %v", err)
	}

	reply, replyID, err := managerB.HandleRequest("A", req, reqID, now)
	if err != nil {
		t.Fatalf("request: %v", err)
	}

	final, _, err := managerA.HandleReply("B", reply, replyID, now)
	if err != nil {
		t.Fatalf("reply: %v", err)
	}

	if err := managerB.HandleFinal("A", final, 99); err != nil {
		t.Fatalf("final: %v", err)
	}

	if !managerA.Authenticated("B") {
		t.Fatalf("initiator must authenticate after final")
	}

	if !managerB.Authenticated("A") {
		t.Fatalf("replier must authenticate after final")
	}

	if managerA.Authenticated("unknown") {
		t.Fatalf("unknown peer must not authenticate")
	}

	_ = tokens.KAgreeECDH
}

// TestGapsNilPoolWithCertFailsauth7 pins nil trust pool failing closed.
//
// Nil pool with identity material fails closed. Nil pool with empty
// material fails closed by default and allows only with explicit
// unauthenticated loopback opt-in.
func TestGapsNilPoolWithCertFailsauth7(t *testing.T) {
	t.Parallel()

	authX, err := NewAuthenticator(KAgreeP256)
	if err != nil {
		t.Fatalf("auth: %v", err)
	}

	manager, err := NewManager("X", authX)
	if err != nil {
		t.Fatalf("manager: %v", err)
	}

	_, certDER := testIdentity(t, "nil-pool-cert")

	if err := verifyRemoteIdentityauth7(manager, certDER, []byte("perms"), []byte("pdata")); err == nil {
		t.Fatalf("nil pool with cert must fail closed")
	}

	if err := verifyRemoteIdentityauth7(manager, nil, nil, nil); err == nil {
		t.Fatalf("nil pool with empty material must fail closed by default")
	}

	manager.SetAllowUnauthenticated(true)

	if err := verifyRemoteIdentityauth7(manager, nil, nil, nil); err != nil {
		t.Fatalf("nil pool with empty material must allow explicit unauthenticated loopback, got %v", err)
	}

	if err := verifyRemoteIdentityauth7(manager, certDER, []byte("perms"), []byte("pdata")); err == nil {
		t.Fatalf("explicit loopback must not admit identity material without trust")
	}
}

// TestGapsEmptyPDataFailsHandshakeauth7 pins pdata required with identity.
func TestGapsEmptyPDataFailsHandshakeauth7(t *testing.T) {
	t.Parallel()

	ca, caKey, pool := gapsCA(t, "empty-pdata-ca")
	managerA, managerB := gapsManagers(t, ca, caKey)
	managerA.SetTrustPool(pool)
	managerB.SetTrustPool(pool)

	now := time.Now()
	req, _, err := managerA.StartInitiator("B", now)
	if err != nil {
		t.Fatalf("start: %v", err)
	}

	emptyReq := req
	emptyReq.ParticipantData = nil

	if _, _, err := managerB.HandleRequest("A", emptyReq, 9001, now); err == nil {
		t.Fatalf("empty pdata with identity must fail")
	}

	undecodable := req
	undecodable.ParticipantData = []byte("opaque-legacy")

	if _, _, err := managerB.HandleRequest("A", undecodable, 9002, now); err == nil {
		t.Fatalf("undecodable pdata with identity must fail")
	}
}

// TestGapsUnsignedInitiatorCannotAuthauth7 pins unsigned emission alone never authenticates.
//
// Unsigned emission requires explicit loopback opt-in; even then the
// responder reply without identity material fails closed and neither
// side authenticates.
func TestGapsUnsignedInitiatorCannotAuthauth7(t *testing.T) {
	t.Parallel()

	ca, caKey, pool := gapsCA(t, "unsigned-init-ca")
	_, signedB := gapsManagers(t, ca, caKey)

	authU, err := NewAuthenticator(KAgreeP256)
	if err != nil {
		t.Fatalf("auth U: %v", err)
	}

	unsigned, err := NewManager("U", authU)
	if err != nil {
		t.Fatalf("manager U: %v", err)
	}

	unsigned.SetAllowUnauthenticated(true)

	signedB.SetTrustPool(pool)

	now := time.Now()
	req, reqID, err := unsigned.StartInitiator("B", now)
	if err != nil {
		t.Fatalf("explicit loopback unsigned start must emit, got %v", err)
	}

	if _, _, err := signedB.HandleRequest("U", req, reqID, now); err == nil {
		t.Fatalf("unsigned initiator must fail at responder reply")
	}

	if signedB.Authenticated("U") {
		t.Fatalf("unsigned peer must not authenticate")
	}

	if unsigned.Authenticated("B") {
		t.Fatalf("unsigned initiator must not authenticate")
	}
}
