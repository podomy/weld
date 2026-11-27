// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

package spdp_test

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/x509"
	"errors"
	"testing"

	"github.com/podomy/weld/internal/transports/dds"
)

// secureFindingsCert returns a stub P-256 identity for announce tests.
func secureFindingsCert() *x509.Certificate {
	return &x509.Certificate{PublicKey: &ecdsa.PublicKey{Curve: elliptic.P256()}}
}

// secureFindingsAllow returns the P-256 governance allow-list.
func secureFindingsAllow() []string {
	return []string{"ECDSA+P256+SHA256", "ECDHE-CEUM+P256"}
}

// TestSpdpSecureAnnounceTokensAndHonestMask pins the secure announce shape end to end.
func TestSpdpSecureAnnounceTokensAndHonestMask(t *testing.T) {
	t.Parallel()

	part, err := dds.NewLocalParticipant(0, expectedLease)
	if err != nil {
		t.Fatalf("new participant: %v", err)
	}
	part.SetSpdp2SecurityMaterial(secureFindingsAllow(), secureFindingsCert(), "CN=TestCA")

	sender := &conformanceSender{}
	if err := part.Announce(context.Background(), sender); err != nil {
		t.Fatalf("secure announce: %v", err)
	}
	ann, err := dds.ParseAnnouncement(sender.last)
	if err != nil {
		t.Fatalf("parse secure announce: %v", err)
	}
	if !ann.Security.IdentityTokenPresent || !ann.Security.PermissionsTokenPresent {
		t.Fatalf("security = %+v, want PID 0x1001 plus 0x1002 tokens", ann.Security)
	}
	if got := dds.Spdp2SecureParticipantMask(); got != 1<<31 {
		t.Fatalf("secure mask = %#x, want valid-only", got)
	}
	if ann.AvailableBuiltinEndpoints != 1<<31 {
		t.Fatalf("builtin set = %#x, want honest secure mask", ann.AvailableBuiltinEndpoints)
	}
	if ann.BuiltinEndpointQos != 0 {
		t.Fatalf("builtin qos = %#x, want 0", ann.BuiltinEndpointQos)
	}
}

// TestSpdpToleranceAnnounceByteIdentical pins the unchanged baseline wire shape.
func TestSpdpToleranceAnnounceByteIdentical(t *testing.T) {
	t.Parallel()

	part, err := dds.NewLocalParticipant(0, expectedLease)
	if err != nil {
		t.Fatalf("new participant: %v", err)
	}
	sender := &conformanceSender{}
	if err := part.Announce(context.Background(), sender); err != nil {
		t.Fatalf("tolerance announce: %v", err)
	}
	want, err := dds.MarshalAlivePacket(part.GUID(), part.DomainID(), 1, part.LeaseDuration())
	if err != nil {
		t.Fatalf("marshal alive: %v", err)
	}
	if !bytes.Equal(sender.last, want) {
		t.Fatalf("tolerance announce differs from MarshalAlivePacket")
	}
	ann, err := dds.ParseAnnouncement(sender.last)
	if err != nil {
		t.Fatalf("parse tolerance announce: %v", err)
	}
	if ann.Security.IdentityTokenPresent || ann.Security.PermissionsTokenPresent {
		t.Fatalf("tolerance security = %+v, want no tokens", ann.Security)
	}
	if ann.AvailableBuiltinEndpoints != expectedBuiltinSet {
		t.Fatalf("builtin set = %#x, want %#x", ann.AvailableBuiltinEndpoints, expectedBuiltinSet)
	}
}

// TestSpdpSecureAnnounceUnnegotiatedFailsClosed pins fail-closed negotiation end to end.
func TestSpdpSecureAnnounceUnnegotiatedFailsClosed(t *testing.T) {
	t.Parallel()

	part, err := dds.NewLocalParticipant(0, expectedLease)
	if err != nil {
		t.Fatalf("new participant: %v", err)
	}
	part.SetSpdp2SecurityMaterial(nil, secureFindingsCert(), "CN=TestCA")

	sender := &conformanceSender{}
	if err := part.Announce(context.Background(), sender); !errors.Is(err, dds.ErrSpdp2UnnegotiatedAlgo) {
		t.Fatalf("unnegotiated announce err = %v, want ErrSpdp2UnnegotiatedAlgo", err)
	}
	if sender.last != nil {
		t.Fatalf("payload sent after refuse, want none")
	}
}
