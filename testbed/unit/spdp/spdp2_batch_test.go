// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

package spdp_test

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"errors"
	"fmt"
	"math/big"
	"testing"
	"time"

	"github.com/podomy/weld/internal/transports/dds"
)

// spdp2TestSender records payloads for assert tests.
type spdp2TestSender struct {
	payloads [][]byte
}

// spdp2Send records one payload copy.
func (s *spdp2TestSender) Send(ctx context.Context, data []byte) error {
	err := ctx.Err()
	if err != nil {
		return fmt.Errorf("spdp: test send context: %w", err)
	}

	cp := append([]byte(nil), data...)
	s.payloads = append(s.payloads, cp)

	return nil
}

// checkSameDomainAccepted pins same domain plus tag admission.
func checkSameDomainAccepted(t *testing.T, table *dds.RemoteTable, base time.Time) {
	t.Helper()

	local := testGUID(0x41)
	good, err := dds.Spdp2MarshalAliveWithManual(local, 7, 1, expectedLease, 0, "alpha")
	if err != nil {
		t.Fatalf("marshal same domain: %v", err)
	}
	ann := mustParse(t, good)
	if ann.DomainID != 7 {
		t.Fatalf("domain = %d, want 7", ann.DomainID)
	}
	if ann.DomainTag != "alpha" {
		t.Fatalf("tag = %q, want alpha", ann.DomainTag)
	}

	err = table.HandlePacket(good, base)
	if err != nil {
		t.Fatalf("handle same domain: %v", err)
	}

	if table.Len() != 1 {
		t.Fatalf("len = %d, want 1", table.Len())
	}
}

// checkForeignDomainRejected pins numeric gate rejection.
func checkForeignDomainRejected(t *testing.T, table *dds.RemoteTable, base time.Time) {
	t.Helper()

	foreignGUID := testGUID(0x42)
	foreign := mustMarshalAlive(t, foreignGUID, 8, 1, expectedLease)
	err := table.HandlePacket(foreign, base)
	if !errors.Is(err, dds.ErrSpdp2DomainMismatch) {
		t.Fatalf("foreign domain = %v, want ErrSpdp2DomainMismatch", err)
	}

	if table.Len() != 1 {
		t.Fatalf("len = %d, want 1 after foreign reject", table.Len())
	}
}

// checkTagGate pins tag admission and rejection.
func checkTagGate(t *testing.T, base time.Time) {
	t.Helper()

	tagged, err := dds.Spdp2MarshalAliveWithManual(testGUID(0x43), 7, 1, expectedLease, 0, "alpha")
	if err != nil {
		t.Fatalf("marshal tagged: %v", err)
	}

	tagAnn := mustParse(t, tagged)
	if tagAnn.DomainTag != "alpha" {
		t.Fatalf("tag = %q, want alpha", tagAnn.DomainTag)
	}

	table2 := dds.NewRemoteTable(nil)
	table2.SetSpdp2LocalDomain(7, "alpha")
	err = table2.HandlePacket(tagged, base)
	if err != nil {
		t.Fatalf("handle matching tag: %v", err)
	}

	wrongTag, err := dds.Spdp2MarshalAliveWithManual(testGUID(0x44), 7, 1, expectedLease, 0, "beta")
	if err != nil {
		t.Fatalf("marshal wrong tag: %v", err)
	}

	err = table2.HandlePacket(wrongTag, base)
	if !errors.Is(err, dds.ErrSpdp2DomainMismatch) {
		t.Fatalf("wrong tag = %v, want ErrSpdp2DomainMismatch", err)
	}

	if table2.Len() != 1 {
		t.Fatalf("len = %d, want 1 after tag reject", table2.Len())
	}
}

// TestSpdp2DomainGateRejectsForeign checks numeric and tag gating.
func TestSpdp2DomainGateRejectsForeign(t *testing.T) {
	t.Parallel()

	base := time.Unix(10000, 0).UTC()
	table := dds.NewRemoteTable(nil)
	table.SetSpdp2LocalDomain(7, "alpha")

	gotDomain, gotTag, gotSet := table.Spdp2LocalDomain()
	if !gotSet || gotDomain != 7 || gotTag != "alpha" {
		t.Fatalf("gate = %d %q %v, want 7 alpha true", gotDomain, gotTag, gotSet)
	}

	checkSameDomainAccepted(t, table, base)
	checkForeignDomainRejected(t, table, base)
	checkTagGate(t, base)
}

// checkBuiltinStored pins stored builtin set and QoS.
func checkBuiltinStored(t *testing.T, base time.Time) {
	t.Helper()

	table := dds.NewRemoteTable(nil)
	guid := testGUID(0x51)
	raw := mustMarshalAlive(t, guid, 0, 1, expectedLease)
	err := table.HandlePacket(raw, base)
	if err != nil {
		t.Fatalf("handle: %v", err)
	}

	info, ok := table.Get(guid.Prefix)
	if !ok {
		t.Fatalf("missing after add")
	}

	if info.AvailableBuiltinEndpoints != expectedBuiltinSet {
		t.Fatalf("builtin set = %#x, want %#x", info.AvailableBuiltinEndpoints, expectedBuiltinSet)
	}

	if info.BuiltinEndpointQos != 0 {
		t.Fatalf("builtin qos = %#x, want 0", info.BuiltinEndpointQos)
	}
}

// checkBuiltinSelected pins intersection and QoS selection.
func checkBuiltinSelected(t *testing.T) {
	t.Helper()

	if got := dds.Spdp2BuiltinIntersection(0x3F, 0x06); got != 0x06 {
		t.Fatalf("intersection = %#x, want 0x06", got)
	}

	if got := dds.Spdp2BuiltinIntersection(0x3F, 0x00); got != 0x00 {
		t.Fatalf("empty intersection = %#x, want 0", got)
	}

	if dds.Spdp2IsBestEffortParticipantMessage(1) != true {
		t.Fatalf("qos 1 should be best-effort")
	}

	if dds.Spdp2IsBestEffortParticipantMessage(0) != false {
		t.Fatalf("qos 0 should be reliable")
	}
}

// TestSpdp2BuiltinIntersectionStoresAndSelects checks endpoint storage.
func TestSpdp2BuiltinIntersectionStoresAndSelects(t *testing.T) {
	t.Parallel()

	base := time.Unix(11000, 0).UTC()
	checkBuiltinStored(t, base)
	checkBuiltinSelected(t)
}

// manualAssertAnnounced asserts manual liveliness and checks sequences.
func manualAssertAnnounced(t *testing.T, part *dds.LocalParticipant, sender *spdp2TestSender) {
	t.Helper()

	ctx := context.Background()
	err := part.Announce(ctx, sender)
	if err != nil {
		t.Fatalf("announce: %v", err)
	}

	if len(sender.payloads) != 1 {
		t.Fatalf("count = %d, want 1", len(sender.payloads))
	}

	first := mustParse(t, sender.payloads[0])
	if first.ManualLivelinessCount != 0 {
		t.Fatalf("first manual = %d, want 0", first.ManualLivelinessCount)
	}

	err = part.AssertSpdp2ManualLiveliness(ctx, sender)
	if err != nil {
		t.Fatalf("assert: %v", err)
	}

	if part.Spdp2ManualCount() != 1 {
		t.Fatalf("manual = %d, want 1", part.Spdp2ManualCount())
	}

	if len(sender.payloads) != 2 {
		t.Fatalf("count = %d, want 2", len(sender.payloads))
	}

	second := mustParse(t, sender.payloads[1])
	if second.ManualLivelinessCount != 1 {
		t.Fatalf("second manual = %d, want 1", second.ManualLivelinessCount)
	}

	if second.Sequence != first.Sequence+1 {
		t.Fatalf("seq %d not after %d", second.Sequence, first.Sequence)
	}
}

// checkSameSeqManualRefresh pins same sequence manual refresh.
func checkSameSeqManualRefresh(t *testing.T) {
	t.Helper()

	table := dds.NewRemoteTable(nil)
	base := time.Unix(12000, 0).UTC()
	dup1, err := dds.Spdp2MarshalAliveWithManual(testGUID(0x52), 0, 5, expectedLease, 0, "")
	if err != nil {
		t.Fatalf("marshal dup1: %v", err)
	}

	err = table.HandlePacket(dup1, base)
	if err != nil {
		t.Fatalf("handle dup1: %v", err)
	}

	before, ok := table.Get(testGUID(0x52).Prefix)
	if !ok {
		t.Fatalf("missing after dup1")
	}

	dup2, err := dds.Spdp2MarshalAliveWithManual(testGUID(0x52), 0, 5, expectedLease, 1, "")
	if err != nil {
		t.Fatalf("marshal dup2: %v", err)
	}

	err = table.HandlePacket(dup2, base.Add(time.Second))
	if err != nil {
		t.Fatalf("same-seq manual increment should refresh, got %v", err)
	}

	after, ok := table.Get(testGUID(0x52).Prefix)
	if !ok {
		t.Fatalf("missing after same-seq refresh")
	}

	if after.LastSeq != 5 {
		t.Fatalf("seq = %d, want 5", after.LastSeq)
	}

	if after.ManualLivelinessCount != 1 {
		t.Fatalf("manual = %d, want 1", after.ManualLivelinessCount)
	}

	if !after.LeaseExpiry.After(before.LeaseExpiry) {
		t.Fatalf("lease not refreshed")
	}

	if table.Generation() != 1 {
		t.Fatalf("gen = %d, want 1 after same-seq refresh", table.Generation())
	}
}

// TestSpdp2ManualLivelinessAssertAndRefresh checks manual counting.
func TestSpdp2ManualLivelinessAssertAndRefresh(t *testing.T) {
	t.Setenv("ROS_DOMAIN_ID", "")

	part, err := dds.NewLocalParticipant(0, expectedLease)
	if err != nil {
		t.Fatalf("new participant: %v", err)
	}

	if part.Spdp2ManualCount() != 0 {
		t.Fatalf("initial manual = %d, want 0", part.Spdp2ManualCount())
	}

	sender := &spdp2TestSender{}
	manualAssertAnnounced(t, part, sender)
	checkSameSeqManualRefresh(t)
}

// checkFifthDuplicateRefresh pins fifth add plus duplicate refresh.
func checkFifthDuplicateRefresh(t *testing.T, table *dds.RemoteTable, base time.Time, guid dds.GUID) {
	t.Helper()

	fifth, err := dds.Spdp2MarshalAliveWithManual(guid, 0, 5, expectedLease, 0, "")
	if err != nil {
		t.Fatalf("marshal fifth: %v", err)
	}

	err = table.HandlePacket(fifth, base)
	if err != nil {
		t.Fatalf("handle 5: %v", err)
	}

	before, _ := table.Get(guid.Prefix)

	err = table.HandlePacket(fifth, base.Add(time.Second))
	if err != nil {
		t.Fatalf("duplicate should refresh, got %v", err)
	}

	after, _ := table.Get(guid.Prefix)
	if !after.LeaseExpiry.After(before.LeaseExpiry) {
		t.Fatalf("duplicate did not refresh lease")
	}

	if table.Generation() != 1 {
		t.Fatalf("gen = %d, want 1 after duplicate", table.Generation())
	}
}

// checkOlderStaleNewerAdvance pins stale reject plus newer advance.
func checkOlderStaleNewerAdvance(t *testing.T, table *dds.RemoteTable, base time.Time, guid dds.GUID) {
	t.Helper()

	older, err := dds.Spdp2MarshalAliveWithManual(guid, 0, 3, expectedLease, 0, "")
	if err != nil {
		t.Fatalf("marshal older: %v", err)
	}

	err = table.HandlePacket(older, base.Add(2*time.Second))
	if !errors.Is(err, dds.ErrStaleSPDP) {
		t.Fatalf("older = %v, want ErrStaleSPDP", err)
	}

	if table.Generation() != 1 {
		t.Fatalf("gen changed on stale")
	}

	newer, err := dds.Spdp2MarshalAliveWithManual(guid, 0, 6, expectedLease, 0, "")
	if err != nil {
		t.Fatalf("marshal newer: %v", err)
	}

	err = table.HandlePacket(newer, base.Add(3*time.Second))
	if err != nil {
		t.Fatalf("handle newer: %v", err)
	}

	if table.Generation() != 2 {
		t.Fatalf("gen = %d, want 2 after newer", table.Generation())
	}
}

// checkDisposeTombstoneDiscipline pins dispose plus tombstone stale.
func checkDisposeTombstoneDiscipline(t *testing.T, table *dds.RemoteTable, base time.Time, guid dds.GUID) {
	t.Helper()

	bye := mustMarshalDispose(t, guid, 7)
	err := table.HandlePacket(bye, base.Add(4*time.Second))
	if err != nil {
		t.Fatalf("handle dispose: %v", err)
	}

	if table.Len() != 0 {
		t.Fatalf("len = %d, want 0", table.Len())
	}

	unknownGUID := testGUID(0x54)
	unknownBye := mustMarshalDispose(t, unknownGUID, 1)
	err = table.HandlePacket(unknownBye, base.Add(5*time.Second))
	if err != nil {
		t.Fatalf("missing dispose should succeed, got %v", err)
	}

	if _, seen := table.TombstoneSeq(unknownGUID.Prefix); seen {
		t.Fatalf("missing dispose created tombstone")
	}

	staleAlive, err := dds.Spdp2MarshalAliveWithManual(guid, 0, 6, expectedLease, 0, "")
	if err != nil {
		t.Fatalf("marshal stale alive: %v", err)
	}

	err = table.HandlePacket(staleAlive, base.Add(6*time.Second))
	if !errors.Is(err, dds.ErrStaleSPDP) {
		t.Fatalf("tombstone stale = %v, want ErrStaleSPDP", err)
	}
}

// TestSpdp2StaleDuplicateNewerDiscipline checks high-water behavior.
func TestSpdp2StaleDuplicateNewerDiscipline(t *testing.T) {
	t.Parallel()

	table := dds.NewRemoteTable(nil)
	base := time.Unix(13000, 0).UTC()
	guid := testGUID(0x53)

	checkFifthDuplicateRefresh(t, table, base, guid)
	checkOlderStaleNewerAdvance(t, table, base, guid)
	checkDisposeTombstoneDiscipline(t, table, base, guid)
}

// spdp2SelfSignedCert builds one leaf cert with the given key.
func spdp2SelfSignedCert(t *testing.T, key any, pub any) *x509.Certificate {
	t.Helper()

	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "spdp2-test"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
	}

	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, pub, key)
	if err != nil {
		t.Fatalf("create cert: %v", err)
	}

	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("parse cert: %v", err)
	}

	return cert
}

// checkSecureECAlgos pins EC P-256 and P-384 algorithm names.
func checkSecureECAlgos(t *testing.T) {
	t.Helper()

	ecKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate ec256: %v", err)
	}

	ecCert := spdp2SelfSignedCert(t, ecKey, &ecKey.PublicKey)
	if got := dds.Spdp2CertAlgoForCert(ecCert); got != "EC-prime256v1" {
		t.Fatalf("ec256 algo = %q, want EC-prime256v1", got)
	}

	ec384Key, err := ecdsa.GenerateKey(elliptic.P384(), rand.Reader)
	if err != nil {
		t.Fatalf("generate ec384: %v", err)
	}

	ec384Cert := spdp2SelfSignedCert(t, ec384Key, &ec384Key.PublicKey)
	if got := dds.Spdp2CertAlgoForCert(ec384Cert); got != "EC-prime384v1" {
		t.Fatalf("ec384 algo = %q, want EC-prime384v1", got)
	}
}

// checkSecureRSAAlgo pins RSA-2048 algorithm name.
func checkSecureRSAAlgo(t *testing.T) {
	t.Helper()

	rsaKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate rsa: %v", err)
	}

	rsaCert := spdp2SelfSignedCert(t, rsaKey, &rsaKey.PublicKey)
	if got := dds.Spdp2CertAlgoForCert(rsaCert); got != "RSA-2048" {
		t.Fatalf("rsa algo = %q, want RSA-2048", got)
	}
}

// TestSpdp2SecureHonestyNoMirrorAndAlgo checks secure honesty.
func TestSpdp2SecureHonestyNoMirrorAndAlgo(t *testing.T) {
	t.Parallel()

	if got := dds.Spdp2SecureParticipantMask(); got != 1<<31 {
		t.Fatalf("secure mask = %#x, want valid-only", got)
	}

	checkSecureECAlgos(t)
	checkSecureRSAAlgo(t)
}
