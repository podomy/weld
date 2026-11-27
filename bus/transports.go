// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

package bus

import (
	"fmt"

	"github.com/podomy/weld/internal/transports"
)

// Route maps a topic prefix to a named backend.
type Route = transports.Route

// Router routes requests across named backends by topic prefix.
type Router = transports.Router

// Domain handling constants re-exported for callers.
const (
	// DomainEnvVar names the environment variable carrying the DDS domain id.
	DomainEnvVar = transports.DomainEnvVar
	// MaxDomainID is the largest accepted domain id on the ROS wire.
	MaxDomainID = transports.MaxDomainID
)

// ResolveDiscoveryRange returns the configured range unchanged.
func ResolveDiscoveryRange(configured DiscoveryRange) DiscoveryRange {
	return transports.ResolveDiscoveryRange(configured)
}

// ValidateRouterConfig checks structural router config without backend existence.
func ValidateRouterConfig(cfg RouterConfig) error {
	err := cfg.Validate()
	if err != nil {
		return fmt.Errorf("bus: validate router config: %w", err)
	}

	return nil
}

// ResolveDomainConfig returns the actual domain id for the receiver.
func ResolveDomainConfig(cfg DomainConfig) (uint32, error) {
	id, err := cfg.Resolve()
	if err != nil {
		return 0, fmt.Errorf("bus: resolve domain config: %w", err)
	}

	return id, nil
}
