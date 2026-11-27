// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

package node

import (
	"github.com/podomy/weld/internal/core"
)

// Endpoint is a created topic endpoint with its resolved QoS. Best available
// resolves once at creation from the peers visible then; later discovery
// never updates the stored profile. Do not copy an endpoint after first use;
// use a pointer.
type Endpoint = core.Endpoint

// EndpointKind names the direction of an endpoint for best available resolution.
type EndpointKind = core.EndpointKind

const (
	// EndpointPublisher resolves best available as an offer.
	EndpointPublisher = core.EndpointPublisher
	// EndpointSubscription resolves best available as a request.
	EndpointSubscription = core.EndpointSubscription
)

// Peer is one endpoint visible at best available resolution time.
type Peer = core.Peer

// CreateEndpoint resolves requested (including best available and system
// defaults) once from the currently visible peers and stores the result.
// The visible slice is copied, so later caller mutation cannot rewrite history.
// It aliases core.CreateEndpoint directly so the noCopy endpoint value is
// never copied through a wrapper.
var CreateEndpoint = core.CreateEndpoint

// EndpointsCanDeliver reports whether two endpoints can flow. Incompatible
// pairs stay silent; the loud event carries topic plus endpoint pair plus
// policy kind. Nil endpoints never deliver.
func EndpointsCanDeliver(publisher, subscriber *Endpoint) bool {
	return core.EndpointsCanDeliver(publisher, subscriber)
}

// CheckEndpointPair checks publisher offered ActualQoS against subscriber
// requested ActualQoS. A nil endpoint yields an incompatible report so
// callers stay silent instead of flowing.
func CheckEndpointPair(publisher, subscriber *Endpoint) CompatibilityReport {
	return core.CheckEndpointPair(publisher, subscriber)
}

// EndpointsRetainForLateJoiner reports whether publisher history must be kept
// for subscriber as late joiner. Nil endpoints never retain.
func EndpointsRetainForLateJoiner(publisher, subscriber *Endpoint) bool {
	return core.EndpointsRetainForLateJoiner(publisher, subscriber)
}
