// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

package auth

import (
	"crypto/x509"
	"testing"
	"time"

	"github.com/podomy/weld/internal/transports/dds/security/tokens"
)

// TestSecInitiatorDefaultDeny pins unsigned emission denied by default.
func TestSecInitiatorDefaultDeny(t *testing.T) {
	t.Parallel()

	authU, err := NewAuthenticator(KAgreeP256)
	if err != nil {
		t.Fatalf("auth: %v", err)
	}

	manager, err := NewManager("U", authU)
	if err != nil {
		t.Fatalf("manager: %v", err)
	}

	if _, _, err := manager.StartInitiator("B", time.Now()); err == nil {
		t.Fatalf("unsigned initiator without opt-in must fail closed")
	}
}

// TestSecInitiatorOptInEmits pins explicit loopback emitting without auth.
func TestSecInitiatorOptInEmits(t *testing.T) {
	t.Parallel()

	authU, err := NewAuthenticator(KAgreeP256)
	if err != nil {
		t.Fatalf("auth: %v", err)
	}

	manager, err := NewManager("U", authU)
	if err != nil {
		t.Fatalf("manager: %v", err)
	}

	manager.SetAllowUnauthenticated(true)

	req, _, err := manager.StartInitiator("B", time.Now())
	if err != nil {
		t.Fatalf("opt-in unsigned start must emit, got %v", err)
	}

	if len(req.IdentityCert) != 0 {
		t.Fatalf("unsigned request must carry no c.id")
	}
}

// TestSecCertSwapFails pins first-seen cert equality against swap.
func TestSecCertSwapFails(t *testing.T) {
	t.Parallel()

	ca, caKey, pool := gapsCA(t, "swap-ca")
	managerA, managerB := gapsManagers(t, ca, caKey)
	managerA.SetTrustPool(pool)
	managerB.SetTrustPool(pool)

	now := time.Now()
	req, reqID, err := managerA.StartInitiator("B", now)
	if err != nil {
		t.Fatalf("start: %v", err)
	}

	if _, _, err := managerB.HandleRequest("A", req, reqID, now); err != nil {
		t.Fatalf("first cert must pass, got %v", err)
	}

	// Second request from a different certificate under the same remote
	// label must fail closed as a swap, not ride the session.
	authE, err := NewAuthenticator(KAgreeP256)
	if err != nil {
		t.Fatalf("auth E: %v", err)
	}

	managerE, err := NewManager("E", authE)
	if err != nil {
		t.Fatalf("manager E: %v", err)
	}

	evilCert, evilKey := gapsLeaf(t, ca, caKey, "evil-swap", 98)
	evilDER, err := x509.MarshalECPrivateKey(evilKey)
	if err != nil {
		t.Fatalf("marshal evil: %v", err)
	}

	evilPData := gapsPData(t, evilCert)
	if err := managerE.SetIdentityMaterial(evilCert, []byte("perms-evil"), evilPData, evilDER); err != nil {
		t.Fatalf("identity evil: %v", err)
	}

	managerE.SetTrustPool(pool)

	evilReq, _, err := managerE.StartInitiator("B", now)
	if err != nil {
		t.Fatalf("start evil: %v", err)
	}

	if _, _, err := managerB.HandleRequest("A", evilReq, reqID+1, now); err == nil {
		t.Fatalf("cert swap for session must fail closed")
	}
}

// TestSecCertDropFails pins a dropped certificate failing closed.
func TestSecCertDropFails(t *testing.T) {
	t.Parallel()

	ca, caKey, pool := gapsCA(t, "drop-ca")
	managerA, managerB := gapsManagers(t, ca, caKey)
	managerA.SetTrustPool(pool)
	managerB.SetTrustPool(pool)

	now := time.Now()
	req, reqID, err := managerA.StartInitiator("B", now)
	if err != nil {
		t.Fatalf("start: %v", err)
	}

	if _, _, err := managerB.HandleRequest("A", req, reqID, now); err != nil {
		t.Fatalf("first cert must pass, got %v", err)
	}

	dropped := req
	dropped.IdentityCert = nil

	if _, _, err := managerB.HandleRequest("A", dropped, reqID+1, now); err == nil {
		t.Fatalf("dropped certificate must fail closed")
	}
}

// TestSecSignVerifyFinalFailClosed pins unsigned reply and final never completing.
func TestSecSignVerifyFinalFailClosed(t *testing.T) {
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

	req, reqID, err := managerA.StartInitiator("B", now)
	if err != nil {
		t.Fatalf("opt-in start must emit, got %v", err)
	}

	// Replier without identity must fail closed instead of emitting an
	// unsigned reply that could complete.
	if _, _, err := managerB.HandleRequest("A", req, reqID, now); err == nil {
		t.Fatalf("reply without identity material must fail closed")
	}

	// Initiator without identity must fail closed on a reply even if one
	// were presented.
	fakeReply := tokens.HandshakeReplyToken{
		Challenge1Echo:  req.Challenge1,
		KeyAgreeAlgo:    req.KeyAgreeAlgo,
		SignAlgo:        tokens.DSignAlgoECDSA,
		IdentityCert:    req.IdentityCert,
		Permissions:     req.Permissions,
		ParticipantData: req.ParticipantData,
		DH2:             req.DH1,
	}

	if _, _, err := managerA.HandleReply("B", fakeReply, reqID+1, now); err == nil {
		t.Fatalf("verify without local identity must fail closed")
	}
}
