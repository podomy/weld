// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

package dds_test

import (
	"errors"
	"os"
	"strconv"
	"testing"

	"github.com/podomy/weld/internal/transports"
	"github.com/podomy/weld/internal/transports/dds"
)

// clearEnv unsets the domain variable for the duration of a test.
func clearEnv(t *testing.T) {
	t.Helper()

	old, had := os.LookupEnv(transports.DomainEnvVar)
	if had {
		t.Setenv(transports.DomainEnvVar, old)
	}

	err := os.Unsetenv(transports.DomainEnvVar)
	if err != nil {
		t.Fatalf("unset env: %v", err)
	}
}

// TestDefaultPeerConfig checks the localhost defaults.
func TestDefaultPeerConfig(t *testing.T) {
	t.Parallel()

	cfg := dds.DefaultPeerConfig()

	if cfg.DomainID != 0 {
		t.Fatalf("domain = %d, want 0", cfg.DomainID)
	}

	if cfg.PeerAddress != dds.DefaultPeerAddress {
		t.Fatalf("peer = %q, want %q", cfg.PeerAddress, dds.DefaultPeerAddress)
	}

	if cfg.Range != transports.DiscoveryRangeSystemDefault {
		t.Fatalf("range = %v, want system_default", cfg.Range)
	}
}

// TestEffectivePeerTrimsSpaces checks surrounding spaces are removed.
func TestEffectivePeerTrimsSpaces(t *testing.T) {
	t.Parallel()

	cfg := dds.PeerConfig{DomainID: 0, PeerAddress: "  127.0.0.1  "}

	if got := cfg.EffectivePeer(); got != "127.0.0.1" {
		t.Fatalf("peer = %q, want 127.0.0.1", got)
	}

	empty := dds.PeerConfig{PeerAddress: "   "}
	if got := empty.EffectivePeer(); got != dds.DefaultPeerAddress {
		t.Fatalf("empty peer = %q, want %q", got, dds.DefaultPeerAddress)
	}
}

// TestNormalizePeerStoresTrimmedPeer checks the stored config is trimmed.
func TestNormalizePeerStoresTrimmedPeer(t *testing.T) {
	clearEnv(t)

	cfg := dds.PeerConfig{DomainID: 0, PeerAddress: "  127.0.0.1  "}

	normed, err := dds.NormalizePeerConfig(cfg)
	if err != nil {
		t.Fatalf("new peer: %v", err)
	}

	if got := normed.PeerAddress; got != "127.0.0.1" {
		t.Fatalf("stored peer = %q, want 127.0.0.1", got)
	}
}

// TestPeerAddressAcceptsRemote checks remote addresses are allowed.
//
// PeerAddress defaults to loopback but remote addresses are valid for
// tests that run the peer on another host.
func TestPeerAddressAcceptsRemote(t *testing.T) {
	clearEnv(t)

	cfg := dds.PeerConfig{DomainID: 0, PeerAddress: "192.168.1.10"}

	normed, err := dds.NormalizePeerConfig(cfg)
	if err != nil {
		t.Fatalf("new peer with remote address: %v", err)
	}

	if got := normed.PeerAddress; got != "192.168.1.10" {
		t.Fatalf("peer = %q", got)
	}
}

// TestNormalizePeerDomainEnvOverride checks ROS_DOMAIN_ID wins.
func TestNormalizePeerDomainEnvOverride(t *testing.T) {
	t.Setenv(transports.DomainEnvVar, "42")

	cfg := dds.PeerConfig{DomainID: 7, PeerAddress: "127.0.0.1"}

	normed, err := dds.NormalizePeerConfig(cfg)
	if err != nil {
		t.Fatalf("new peer: %v", err)
	}

	if got := normed.DomainID; got != 42 {
		t.Fatalf("domain = %d, want 42", got)
	}
}

// TestNormalizePeerDomainEnvWhitespace checks spaced env resolves.
func TestNormalizePeerDomainEnvWhitespace(t *testing.T) {
	t.Setenv(transports.DomainEnvVar, "  42  ")

	cfg := dds.PeerConfig{DomainID: 0, PeerAddress: "127.0.0.1"}

	normed, err := dds.NormalizePeerConfig(cfg)
	if err != nil {
		t.Fatalf("new peer: %v", err)
	}

	if got := normed.DomainID; got != 42 {
		t.Fatalf("domain = %d, want 42", got)
	}
}

// TestPeerRejectsCallerAboveMax checks caller range without env.
func TestPeerRejectsCallerAboveMax(t *testing.T) {
	clearEnv(t)

	cfg := dds.PeerConfig{DomainID: 999, PeerAddress: "127.0.0.1"}

	_, err := dds.NormalizePeerConfig(cfg)
	if err == nil {
		t.Fatalf("new peer with domain 999 succeeded, want error")
	}
}

// TestPeerBadEnvChainsToNumError checks env parse errors chain.
func TestPeerBadEnvChainsToNumError(t *testing.T) {
	t.Setenv(transports.DomainEnvVar, "nope")

	cfg := dds.PeerConfig{DomainID: 0, PeerAddress: "127.0.0.1"}

	_, err := dds.NormalizePeerConfig(cfg)
	if err == nil {
		t.Fatalf("new peer succeeded, want error")
	}

	if numErr, ok := errors.AsType[*strconv.NumError](err); !ok {
		t.Fatalf("error %v does not chain to NumError (got %v)", err, numErr)
	}
}

// TestPeerConfigValidation checks empty peer is rejected.
func TestPeerConfigValidation(t *testing.T) {
	t.Parallel()

	bad := dds.PeerConfig{DomainID: 0, PeerAddress: "   "}

	_, err := dds.NormalizePeerConfig(bad)
	if err == nil {
		t.Fatalf("bad config succeeded, want error")
	}
}

// TestPeerConfigDomainValidation checks domain range is rejected.
func TestPeerConfigDomainValidation(t *testing.T) {
	t.Parallel()

	bad := dds.PeerConfig{DomainID: 999, PeerAddress: "127.0.0.1"}

	err := bad.Validate()
	if err == nil {
		t.Fatalf("validate domain 999 succeeded, want error")
	}

	_, err = dds.NormalizePeerConfig(bad)
	if err == nil {
		t.Fatalf("new peer with domain 999 succeeded, want error")
	}
}

// TestPeerRangeValidation checks unknown ranges are rejected.
func TestPeerRangeValidation(t *testing.T) {
	bad := dds.PeerConfig{DomainID: 0, PeerAddress: "127.0.0.1", Range: transports.DiscoveryRange(99)}

	err := bad.Validate()
	if err == nil {
		t.Fatalf("validate unknown range succeeded, want error")
	}

	clearEnv(t)

	_, err = dds.NormalizePeerConfig(bad)
	if err == nil {
		t.Fatalf("new peer with unknown range succeeded, want error")
	}
}

// TestPeerNotSetNormalized checks NotSet becomes SystemDefault.
func TestPeerNotSetNormalized(t *testing.T) {
	clearEnv(t)

	cfg := dds.PeerConfig{DomainID: 0, PeerAddress: "127.0.0.1", Range: transports.DiscoveryRangeNotSet}

	normed, err := dds.NormalizePeerConfig(cfg)
	if err != nil {
		t.Fatalf("new peer: %v", err)
	}

	if got := normed.Range; got != transports.DiscoveryRangeSystemDefault {
		t.Fatalf("range = %v, want system_default", got)
	}
}

// TestPeerResolveDiscoveryRange checks range wiring passes through.
func TestPeerResolveDiscoveryRange(t *testing.T) {
	t.Parallel()

	cfg := dds.PeerConfig{PeerAddress: "127.0.0.1", Range: transports.DiscoveryRangeSubnet}

	if got := cfg.ResolveDiscoveryRange(); got != transports.DiscoveryRangeSubnet {
		t.Fatalf("range = %v, want subnet", got)
	}
}
