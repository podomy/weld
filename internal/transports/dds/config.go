// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

// Package dds implements the DDS/RTPS wire transport.
//
// The transport speaks RTPS directly over UDP with no cgo. PeerConfig
// carries the join parameters; topic and type naming live in naming.go.
package dds

import (
	"errors"
	"fmt"
	"strings"

	"github.com/podomy/weld/internal/transports"
)

type PeerConfig struct {
	// DomainID is the DDS domain to join.
	DomainID uint32
	// PeerAddress is the address of the DDS peer.
	PeerAddress string
	// Range is the discovery range for JoinDiscovery.
	Range transports.DiscoveryRange
}

// DefaultPeerConfig returns a PeerConfig with localhost defaults.
func DefaultPeerConfig() PeerConfig {
	return PeerConfig{
		DomainID:    0,
		PeerAddress: DefaultPeerAddress,
		Range:       transports.DiscoveryRangeSystemDefault,
	}
}

// Validate checks the peer configuration for usable values.
//
// It rejects a domain above transports.MaxDomainID, an empty peer, and
// an unknown discovery range. DiscoveryRangeNotSet is accepted here and
// normalized to SystemDefault by NormalizePeerConfig so the zero value
// config still joins with a concrete range.
func (c PeerConfig) Validate() error {
	if c.DomainID > transports.MaxDomainID {
		return fmt.Errorf("dds: domain id %d above maximum %d", c.DomainID, transports.MaxDomainID)
	}

	if strings.TrimSpace(c.PeerAddress) == "" {
		return errors.New("dds: peer address is empty") //nolint:wrapcheck // new error, nothing to wrap
	}

	switch c.Range {
	case transports.DiscoveryRangeNotSet,
		transports.DiscoveryRangeOff,
		transports.DiscoveryRangeLocalhost,
		transports.DiscoveryRangeSubnet,
		transports.DiscoveryRangeSystemDefault:
	default:
		return fmt.Errorf("dds: unknown discovery range %v", c.Range)
	}

	return nil
}

// EffectivePeer returns the configured peer or the localhost default.
//
// The result is trimmed of surrounding whitespace so config with
// accidental spaces still dials the intended address.
func (c PeerConfig) EffectivePeer() string {
	trimmed := strings.TrimSpace(c.PeerAddress)
	if trimmed == "" {
		return DefaultPeerAddress
	}

	return trimmed
}

// ResolveDiscoveryRange returns the configured discovery range.
//
// It mirrors transports.ResolveDiscoveryRange so callers have one domain
// and one range resolve path. No environment variable is consulted. NotSet
// passes through unchanged here; NormalizePeerConfig normalizes it
// to SystemDefault for the stored config.
func (c PeerConfig) ResolveDiscoveryRange() transports.DiscoveryRange {
	return transports.ResolveDiscoveryRange(c.Range)
}

// NormalizePeerConfig validates the configuration and returns it normalized.
//
// It resolves the domain through transports.ResolveDomainID so ROS_DOMAIN_ID
// overrides the caller value, trims the peer, and normalizes
// DiscoveryRangeNotSet to DiscoveryRangeSystemDefault. A caller DomainID
// above transports.MaxDomainID is rejected even when the environment is
// unset.
func NormalizePeerConfig(cfg PeerConfig) (PeerConfig, error) {
	err := cfg.Validate()
	if err != nil {
		return PeerConfig{}, fmt.Errorf("dds: invalid peer config: %w", err)
	}

	resolved, err := transports.ResolveDomainID(cfg.DomainID)
	if err != nil {
		return PeerConfig{}, fmt.Errorf("dds: resolve domain: %w", err)
	}

	cfg.DomainID = resolved
	cfg.PeerAddress = cfg.EffectivePeer()
	if cfg.Range == transports.DiscoveryRangeNotSet {
		cfg.Range = transports.DiscoveryRangeSystemDefault
	}

	return cfg, nil
}
