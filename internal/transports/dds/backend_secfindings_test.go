// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

package dds

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"errors"
	"math/big"
	"testing"
	"time"

	"github.com/podomy/weld/internal/transports/dds/rtps"
	ddssecurity "github.com/podomy/weld/internal/transports/dds/security"
	"github.com/podomy/weld/internal/transports/dds/security/access"
	"github.com/podomy/weld/internal/transports/dds/security/crypto"
)

// TestSecTripleAuthBeforeDecrypt pins 9.4.2.4: unauthenticated senders drop
// before any trial-open decrypt side effects.
func TestSecTripleAuthBeforeDecrypt(t *testing.T) {
	t.Parallel()
	secured := &Backend{secSuite: enabledTestSuite(t)}
	var prefix [12]byte
	prefix[0] = 0x9A
	header := rtps.DefaultHeader(prefix)
	if secured.authenticateSender(header) {
		t.Fatalf("unauthenticated sender allowed, want deny")
	}
	key := bytes.Repeat([]byte{0x0c}, 32)
	salt := bytes.Repeat([]byte{0x0d}, 32)
	inner := rtps.RawSubmessage{ID: rtps.SubmessageData, Flags: rtps.FlagLittleEndian, Body: []byte{0x01, 0x02}}
	framed := secFrameSubmessage(inner)
	sessionKey := mustSECSessionKey(t, key, salt, 1)
	var origin [16]byte
	copy(origin[:12], prefix[:])
	wrapped, err := secWrapSubmessageWithOrigin(secKindAES256GMAC, 1, 1, 1, sessionKey, origin, framed)
	if err != nil {
		t.Fatalf("wrap: %v", err)
	}
	first, second, third, err := secSplitTriple(wrapped)
	if err != nil {
		t.Fatalf("split: %v", err)
	}
	toRaw := func(framed []byte) rtps.RawSubmessage {
		raw, err := framedBytesToRaw(framed)
		if err != nil {
			t.Fatalf("framed to raw: %v", err)
		}
		return raw
	}
	subs := []rtps.RawSubmessage{toRaw(first), toRaw(second), toRaw(third)}
	before := secured.demuxErrorCount()
	ch := make(chan dataItem, 4)
	secured.enqueueUserSubs(context.Background(), header, subs, ch)
	if len(ch) != 0 {
		t.Fatalf("unauthenticated triple queued, want drop before decrypt")
	}
	if secured.demuxErrorCount() <= before {
		t.Fatalf("unauthenticated triple did not count demux error")
	}
}

// TestHeartbeatDiscoveryEgressDeny pins 10.5.3.1 for heartbeat plus SEDP plus SPDP.
func TestHeartbeatDiscoveryEgressDeny(t *testing.T) {
	t.Parallel()
	plain := &Backend{}
	if err := plain.checkDiscoveryEgressAllowed(0); err != nil {
		t.Fatalf("tolerance discovery denied: %v", err)
	}
	if err := plain.checkHeartbeatEgressAllowed(0); err != nil {
		t.Fatalf("tolerance heartbeat denied: %v", err)
	}
	secured := &Backend{secSuite: enabledTestSuite(t)}
	if err := secured.checkDiscoveryEgressAllowed(0); err == nil {
		t.Fatalf("secured discovery without governance allowed, want deny")
	}
	if err := secured.checkHeartbeatEgressAllowed(0); err == nil {
		t.Fatalf("secured heartbeat without governance allowed, want deny")
	} else if !errors.Is(err, ErrSecuredTransformUnavailable) && !errors.Is(err, ddssecurity.ErrNotAllowedBySecurity) {
		t.Fatalf("secured heartbeat err = %v, want secured-transform denial", err)
	}
	sender := &sedpSender{backend: secured}
	if err := sender.SendSEDP(context.Background(), []byte{0x01}); err == nil {
		t.Fatalf("secured SEDP plain allowed, want deny")
	}
	spdp := &spdpSender{backend: secured}
	if err := spdp.Send(context.Background(), []byte{0x01}); err == nil {
		t.Fatalf("secured SPDP plain allowed, want deny")
	}
}

// TestAuthorizeRemotePresenceUsesValidator pins 7.4 presence gating.
func TestAuthorizeRemotePresenceUsesValidator(t *testing.T) {
	t.Parallel()
	if err := ddssecurity.ValidateDiscoveryTokens(true, true); err != nil {
		t.Fatalf("both tokens denied: %v", err)
	}
	if err := ddssecurity.ValidateDiscoveryTokens(true, false); err == nil {
		t.Fatalf("identity-only allowed, want deny")
	}
	secured := &Backend{secSuite: enabledTestSuite(t)}
	ann := Announcement{
		GUID:     GUID{Prefix: GUIDPrefix{0x03}},
		DomainID: 0,
		Alive:    true,
		Lease:    DefaultLeaseDuration,
		Security: SecurityInfo{IdentityTokenPresent: true, PermissionsTokenPresent: false},
	}
	if err := secured.authorizeRemoteParticipant(ann); err == nil {
		t.Fatalf("identity-only admission allowed, want deny")
	}
}

// TestSetNegotiatedTransformKindWiring pins the suite setter path per 9.5.3.
func TestSetNegotiatedTransformKindWiring(t *testing.T) {
	t.Parallel()
	plain := &Backend{}
	plain.SetNegotiatedTransformKind(2)
	secured := &Backend{secSuite: enabledTestSuite(t)}
	secured.SetNegotiatedTransformKind(2)
	secured.SetNegotiatedTransformKind(0)
}

// TestRekeyOnEnableKeyRevisionTrigger pins the publish-path trigger per 9.5.1.9.
func TestRekeyOnEnableKeyRevisionTrigger(t *testing.T) {
	t.Parallel()
	gov := &access.GovernanceDoc{Rules: []access.DomainRule{{Domains: access.Domains{IDs: []uint32{0}}, EnableKeyRevision: true}}}
	if !rekeyRequiredForDomain(gov, 0, "") {
		t.Fatalf("rekey flag not observed, want true")
	}
	if rekeyRequiredForDomain(nil, 0, "") {
		t.Fatalf("nil governance rekey allowed, want false")
	}
	plain := &access.GovernanceDoc{}
	if rekeyRequiredForDomain(plain, 0, "") {
		t.Fatalf("flag-off rekey required, want false")
	}
}

// TestLookupEgressExactFirst pins exact-match-first resolution per 9.5.3.
func TestLookupEgressExactFirst(t *testing.T) {
	t.Parallel()
	store := crypto.NewKeyStore()
	var origin [16]byte
	origin[0] = 0x41
	salt := bytes.Repeat([]byte{0x0d}, 32)
	k1 := bytes.Repeat([]byte{0x11}, 32)
	k2 := bytes.Repeat([]byte{0x22}, 32)
	if err := store.Store(crypto.MasterRecord{Sender: origin, KeyID: 1, Revision: 1, Kind: crypto.TransformAES256GCM, Salt: salt, SenderKey: k1, Session: 1}); err != nil {
		t.Fatalf("store k1: %v", err)
	}
	if err := store.Store(crypto.MasterRecord{Sender: origin, KeyID: 2, Revision: 1, Kind: crypto.TransformAES256GCM, Salt: salt, SenderKey: k2, Session: 1}); err != nil {
		t.Fatalf("store k2: %v", err)
	}
	backend := &Backend{cryptoStore: store}
	if _, err := backend.lookupEgressMasterExact(origin, 2, 1); err != nil {
		t.Fatalf("exact lookup denied: %v", err)
	}
	if _, _, _, err := backend.lookupEgressMaster(origin); err == nil {
		t.Fatalf("ambiguous keys first-hit allowed, want fail")
	}
	single := crypto.NewKeyStore()
	if err := single.Store(crypto.MasterRecord{Sender: origin, KeyID: 1, Revision: 1, Kind: crypto.TransformAES256GCM, Salt: salt, SenderKey: k1, Session: 1}); err != nil {
		t.Fatalf("store single: %v", err)
	}
	if err := single.Store(crypto.MasterRecord{Sender: origin, KeyID: 1, Revision: 2, Kind: crypto.TransformAES256GCM, Salt: salt, SenderKey: k2, Session: 1}); err != nil {
		t.Fatalf("store rev2: %v", err)
	}
	singleBackend := &Backend{cryptoStore: single}
	rec, keyID, rev, err := singleBackend.lookupEgressMaster(origin)
	if err != nil {
		t.Fatalf("single-key lookup denied: %v", err)
	}
	if keyID != 1 || rev != 2 {
		t.Fatalf("single-key lookup = %d/%d, want 1/2", keyID, rev)
	}
	_ = rec
}

// TestIngressRevisionSweepOpensOld pins wire revision sweep per 9.5.3.
func TestIngressRevisionSweepOpensOld(t *testing.T) {
	t.Parallel()
	store := crypto.NewKeyStore()
	var origin [16]byte
	origin[0] = 0x43
	salt := bytes.Repeat([]byte{0x0d}, 32)
	oldKey := bytes.Repeat([]byte{0x31}, 32)
	newKey := bytes.Repeat([]byte{0x32}, 32)
	if err := store.Store(crypto.MasterRecord{Sender: origin, KeyID: 7, Revision: 1, Kind: crypto.TransformAES256GCM, Salt: salt, SenderKey: oldKey, Session: 1}); err != nil {
		t.Fatalf("store rev1: %v", err)
	}
	if err := store.Store(crypto.MasterRecord{Sender: origin, KeyID: 7, Revision: 2, Kind: crypto.TransformAES256GCM, Salt: salt, SenderKey: newKey, Session: 1}); err != nil {
		t.Fatalf("store rev2: %v", err)
	}
	backend := &Backend{cryptoStore: store}
	inner := rtps.RawSubmessage{ID: rtps.SubmessageData, Flags: rtps.FlagLittleEndian, Body: []byte{0x09, 0x08}}
	framed := secFrameSubmessage(inner)
	oldSession := mustSECSessionKey(t, oldKey, salt, 1)
	wrapped, err := secWrapSubmessageWithOrigin(secKindAES256GCM, 7, 1, 3, oldSession, origin, framed)
	if err != nil {
		t.Fatalf("wrap old: %v", err)
	}
	first, second, third, err := secSplitTriple(wrapped)
	if err != nil {
		t.Fatalf("split: %v", err)
	}
	opened, err := backend.openSecureTripleWithOrigin(first, second, third, origin)
	if err != nil {
		t.Fatalf("old revision open denied with latest stored: %v", err)
	}
	if !bytes.Equal(opened, framed) {
		t.Fatalf("old revision roundtrip mismatch")
	}
}

// TestAnnounceInitialSendsTopics pins TopicsSnapshot wiring per 8.5.4.
func TestAnnounceInitialSendsTopics(t *testing.T) {
	t.Parallel()
	idx := mustSEDPIndexWithPrefix(t, 0x61)
	ep, err := idx.DeclareWriter("/topics_wired", "std_msgs__msg__String", "RIHS01_wired", nil)
	if err != nil {
		t.Fatalf("declare writer: %v", err)
	}
	_ = ep
	topics := idx.TopicsSnapshot()
	if len(topics) == 0 {
		t.Fatalf("topics snapshot empty, want one")
	}
	backend := newTestBackend(t, DefaultPeerConfig())
	recorder := &recordingSEDPSender{}
	if err := backend.announceInitial(context.Background(), recorder, idx, idx.LocalEndpoints(), topics, []int{1234}); err != nil {
		t.Fatalf("announce initial: %v", err)
	}
	if recorder.calls == 0 {
		t.Fatalf("no SEDP sends recorded")
	}
	if recorder.calls < 2 {
		t.Fatalf("topics snapshot not sent: calls=%d, want >=2 (endpoints plus topics)", recorder.calls)
	}
}

// recordingSEDPSender counts SEDP sends for announce tests.
type recordingSEDPSender struct {
	calls int
}

// SendSEDP records one send.
func (r *recordingSEDPSender) SendSEDP(_ context.Context, _ []byte) error {
	r.calls++
	return nil
}

// TestMarshalAliveSecureEmitsTokens pins 7.4 secure announce per 9.3.2.
//
//nolint:cyclop // secure plus tolerance plus unnegotiated branches are each asserted
func TestMarshalAliveSecureEmitsTokens(t *testing.T) {
	t.Parallel()
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	tmpl := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "secure-peer"}}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &priv.PublicKey, priv)
	if err != nil {
		t.Fatalf("create cert: %v", err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("parse cert: %v", err)
	}
	allowed := []string{"ECDSA+P256+SHA256", "ECDHE-CEUM+P256"}
	var guid GUID
	guid.Prefix[0] = 0x77
	secure, err := MarshalAlivePacketSecure(guid, 0, 1, time.Second, allowed, cert, "TestCA")
	if err != nil {
		t.Fatalf("secure announce: %v", err)
	}
	ann, err := ParseAnnouncement(secure)
	if err != nil {
		t.Fatalf("parse secure announce: %v", err)
	}
	if !ann.Security.IdentityTokenPresent || !ann.Security.PermissionsTokenPresent {
		t.Fatalf("secure announce tokens present=%v/%v, want both", ann.Security.IdentityTokenPresent, ann.Security.PermissionsTokenPresent)
	}
	plain, err := MarshalAlivePacket(guid, 0, 1, time.Second)
	if err != nil {
		t.Fatalf("plain announce: %v", err)
	}
	plainAnn, err := ParseAnnouncement(plain)
	if err != nil {
		t.Fatalf("parse plain announce: %v", err)
	}
	if plainAnn.Security.IdentityTokenPresent || plainAnn.Security.PermissionsTokenPresent {
		t.Fatalf("tolerance path emits tokens, want byte-identical plain")
	}
	if _, _, err := Spdp2NegotiatedSecureAlgos(nil, cert); err == nil {
		t.Fatalf("empty allow-list negotiated, want fail")
	}
}
