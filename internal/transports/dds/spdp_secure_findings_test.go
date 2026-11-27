// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

package dds

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/x509"
	"errors"
	"testing"
	"time"

	"github.com/podomy/weld/internal/transports/dds/rtps"
)

// spdpTestP256Cert returns a stub P-256 identity certificate for announce tests.
func spdpTestP256Cert() *x509.Certificate {
	return &x509.Certificate{PublicKey: &ecdsa.PublicKey{Curve: elliptic.P256()}}
}

// spdpTestAllowList returns the P-256 governance allow-list for announce tests.
func spdpTestAllowList() []string {
	return []string{"ECDSA+P256+SHA256", "ECDHE-CEUM+P256"}
}

// TestSpdp2SecureAnnounceCarriesTokens pins secure alive content per 7.4.
func TestSpdp2SecureAnnounceCarriesTokens(t *testing.T) {
	t.Parallel()

	local, err := NewLocalParticipant(42, 10*time.Second)
	if err != nil {
		t.Fatalf("new participant: %v", err)
	}
	local.SetSpdp2SecurityMaterial(spdpTestAllowList(), spdpTestP256Cert(), "CN=TestCA")

	sender := &fakeSender{}
	if err := local.Announce(context.Background(), sender); err != nil {
		t.Fatalf("secure announce: %v", err)
	}
	if sender.count() != 1 {
		t.Fatalf("payloads = %d, want 1", sender.count())
	}

	ann, err := ParseAnnouncement(sender.payloads[0])
	if err != nil {
		t.Fatalf("parse secure announce: %v", err)
	}
	if !ann.Security.IdentityTokenPresent || !ann.Security.PermissionsTokenPresent {
		t.Fatalf("security = %+v, want identity plus permissions tokens", ann.Security)
	}
	if ann.AvailableBuiltinEndpoints != Spdp2SecureParticipantMask() {
		t.Fatalf("builtin set = %#x, want honest secure mask %#x", ann.AvailableBuiltinEndpoints, Spdp2SecureParticipantMask())
	}
	if Spdp2SecureParticipantMask() != 1<<31 {
		t.Fatalf("secure mask = %#x, want valid-only 1<<31", Spdp2SecureParticipantMask())
	}
	if ann.BuiltinEndpointQos != 0 {
		t.Fatalf("builtin qos = %#x, want 0", ann.BuiltinEndpointQos)
	}
}

// TestSpdp2SecureAnnounceHonestMaskNeverMirrors pins no peer mirroring.
func TestSpdp2SecureAnnounceHonestMaskNeverMirrors(t *testing.T) {
	t.Parallel()

	table := NewRemoteTable(nil)
	hostile, err := MarshalAlivePacket(testGUID(0x71), 42, 1, 10*time.Second)
	if err != nil {
		t.Fatalf("hostile packet: %v", err)
	}
	if err := table.HandlePacket(hostile, time.Now()); err != nil {
		t.Fatalf("handle hostile: %v", err)
	}

	local, err := NewLocalParticipant(42, 10*time.Second)
	if err != nil {
		t.Fatalf("new participant: %v", err)
	}
	local.SetSpdp2SecurityMaterial(spdpTestAllowList(), spdpTestP256Cert(), "CN=TestCA")

	sender := &fakeSender{}
	if err := local.Announce(context.Background(), sender); err != nil {
		t.Fatalf("secure announce: %v", err)
	}
	ann, err := ParseAnnouncement(sender.payloads[0])
	if err != nil {
		t.Fatalf("parse secure announce: %v", err)
	}
	if ann.AvailableBuiltinEndpoints != 1<<31 {
		t.Fatalf("builtin set = %#x, want 1<<31 regardless of peer state", ann.AvailableBuiltinEndpoints)
	}
}

// TestSpdp2SecureAnnounceUnnegotiatedFailsClosed pins fail-closed negotiation.
func TestSpdp2SecureAnnounceUnnegotiatedFailsClosed(t *testing.T) {
	t.Parallel()

	local, err := NewLocalParticipant(42, 10*time.Second)
	if err != nil {
		t.Fatalf("new participant: %v", err)
	}
	local.SetSpdp2SecurityMaterial(nil, spdpTestP256Cert(), "CN=TestCA")

	sender := &fakeSender{}
	if err := local.Announce(context.Background(), sender); !errors.Is(err, ErrSpdp2UnnegotiatedAlgo) {
		t.Fatalf("unnegotiated announce err = %v, want ErrSpdp2UnnegotiatedAlgo", err)
	}
	if sender.count() != 0 {
		t.Fatalf("payloads = %d, want 0 after refuse", sender.count())
	}
}

// TestSpdp2ToleranceAnnounceByteIdentical pins the unchanged baseline.
func TestSpdp2ToleranceAnnounceByteIdentical(t *testing.T) {
	t.Parallel()

	local, err := NewLocalParticipant(42, 10*time.Second)
	if err != nil {
		t.Fatalf("new participant: %v", err)
	}

	sender := &fakeSender{}
	if err := local.Announce(context.Background(), sender); err != nil {
		t.Fatalf("tolerance announce: %v", err)
	}
	guid := local.GUID()
	want, err := MarshalAlivePacket(guid, local.DomainID(), 1, local.LeaseDuration())
	if err != nil {
		t.Fatalf("marshal alive: %v", err)
	}
	if !bytes.Equal(sender.payloads[0], want) {
		t.Fatalf("tolerance announce differs from MarshalAlivePacket")
	}
	ann, err := ParseAnnouncement(sender.payloads[0])
	if err != nil {
		t.Fatalf("parse tolerance announce: %v", err)
	}
	if ann.Security.IdentityTokenPresent || ann.Security.PermissionsTokenPresent {
		t.Fatalf("tolerance security = %+v, want no tokens", ann.Security)
	}
	if ann.AvailableBuiltinEndpoints != 0x3F {
		t.Fatalf("tolerance builtin set = %#x, want 0x3F", ann.AvailableBuiltinEndpoints)
	}
}

// TestSpdp2SecureAnnounceCarriesManualAndTag pins detail propagation.
func TestSpdp2SecureAnnounceCarriesManualAndTag(t *testing.T) {
	t.Parallel()

	local, err := NewLocalParticipant(42, 10*time.Second)
	if err != nil {
		t.Fatalf("new participant: %v", err)
	}
	local.SetSpdp2DomainTag("prod")
	local.SetSpdp2SecurityMaterial(spdpTestAllowList(), spdpTestP256Cert(), "CN=TestCA")

	sender := &fakeSender{}
	if err := local.AssertSpdp2ManualLiveliness(context.Background(), sender); err != nil {
		t.Fatalf("assert manual: %v", err)
	}
	ann, err := ParseAnnouncement(sender.payloads[0])
	if err != nil {
		t.Fatalf("parse secure manual announce: %v", err)
	}
	if ann.ManualLivelinessCount != 1 {
		t.Fatalf("manual = %d, want 1", ann.ManualLivelinessCount)
	}
	if ann.DomainTag != "prod" {
		t.Fatalf("tag = %q, want prod", ann.DomainTag)
	}
	if !ann.Security.IdentityTokenPresent {
		t.Fatalf("security = %+v, want identity token", ann.Security)
	}
}

// TestSpdpTableSweepConcurrentSafe pins table-only Sweep under concurrency.
func TestSpdpTableSweepConcurrentSafe(t *testing.T) {
	t.Parallel()

	table := NewRemoteTable(nil)
	base := time.Now()
	alive, err := MarshalAlivePacket(testGUID(0x72), 42, 1, 10*time.Second)
	if err != nil {
		t.Fatalf("marshal alive: %v", err)
	}
	if err := table.HandlePacket(alive, base); err != nil {
		t.Fatalf("handle: %v", err)
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := range 50 {
			_ = table.Sweep(base.Add(time.Duration(i) * time.Second))
		}
	}()
	for i := range 50 {
		_ = table.Sweep(base.Add(time.Duration(i) * time.Second))
		_, _ = table.TombstoneSeq(testGUID(0x72).Prefix)
		_ = table.List()
	}
	<-done
	if got := table.Len(); got != 0 && got != 1 {
		t.Fatalf("len = %d, want 0 or 1 after concurrent sweep", got)
	}
}

// TestSpdpSecurePacketDirectPinsTokens verifies MarshalAlivePacketSecure output.
func TestSpdpSecurePacketDirectPinsTokens(t *testing.T) {
	t.Parallel()

	pkt, err := MarshalAlivePacketSecure(testGUID(0x73), 42, 1, 10*time.Second, spdpTestAllowList(), spdpTestP256Cert(), "CN=TestCA")
	if err != nil {
		t.Fatalf("secure packet: %v", err)
	}
	ann, err := ParseAnnouncement(pkt)
	if err != nil {
		t.Fatalf("parse secure packet: %v", err)
	}
	if !ann.Security.IdentityTokenPresent || !ann.Security.PermissionsTokenPresent {
		t.Fatalf("security = %+v, want both tokens", ann.Security)
	}
	if ann.AvailableBuiltinEndpoints != 1<<31 {
		t.Fatalf("builtin set = %#x, want 1<<31", ann.AvailableBuiltinEndpoints)
	}
	var drops rtps.Counter
	_, raws, err := rtps.DecodeMessage(pkt, &drops)
	if err != nil {
		t.Fatalf("decode secure packet: %v", err)
	}
	if len(raws) != 1 {
		t.Fatalf("submessages = %d, want 1", len(raws))
	}
}
