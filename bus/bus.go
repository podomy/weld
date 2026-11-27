// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

// Package bus is the public embedding surface for Weld.
//
// Concord and other Go programs import this package to use Weld as a
// library. It re-exports the backend contract and the DDS plus MQTT
// constructors. All matching, routing, and state machines stay inside
// internal core. The backends are translators at the border.
package bus

import (
	"context"
	"errors"
	"fmt"

	"github.com/podomy/weld/internal/transports"
	"github.com/podomy/weld/internal/transports/dds"
	"github.com/podomy/weld/internal/transports/mqtt"
)

// Backend carries serialized messages across one transport.
//
// Lifecycle is JoinDiscovery then PublishBytes and TakeBytes then Close.
// PublishBytes, TakeBytes, and ReportLiveliness before JoinDiscovery
// report ErrNotJoined. Close is idempotent. Implementations are safe
// for concurrent use.
type Backend = transports.Backend

// OutgoingMessage is one serialized message handed to a backend.
type OutgoingMessage = transports.OutgoingMessage

// IncomingMessage is one serialized message taken from a backend.
type IncomingMessage = transports.IncomingMessage

// DiscoveryInfo describes a local participant joining discovery.
type DiscoveryInfo = transports.DiscoveryInfo

// LivelinessReport carries one aliveness assertion for a local endpoint.
type LivelinessReport = transports.LivelinessReport

// DomainConfig carries the caller supplied domain id.
type DomainConfig = transports.DomainConfig

// DiscoveryRange selects which network segment discovery may use.
type DiscoveryRange = transports.DiscoveryRange

// Leg names the traffic leg a sample belongs to.
type Leg = transports.Leg

// RouterConfig selects a backend per topic by longest prefix match.
type RouterConfig = transports.RouterConfig

// DDSConfig configures one DDS backend.
type DDSConfig = dds.SidecarConfig

// MQTTConfig configures one MQTT backend.
type MQTTConfig = mqtt.MqttConfig

// Discovery ranges re-exported so callers need not import internal packages.
const (
	// DiscoveryRangeNotSet means no explicit range was configured.
	DiscoveryRangeNotSet = transports.DiscoveryRangeNotSet
	// DiscoveryRangeOff disables discovery multicast and peer contact.
	DiscoveryRangeOff = transports.DiscoveryRangeOff
	// DiscoveryRangeLocalhost restricts discovery to the local host.
	DiscoveryRangeLocalhost = transports.DiscoveryRangeLocalhost
	// DiscoveryRangeSubnet restricts discovery to the local subnet.
	DiscoveryRangeSubnet = transports.DiscoveryRangeSubnet
	// DiscoveryRangeSystemDefault defers to the middleware default range.
	DiscoveryRangeSystemDefault = transports.DiscoveryRangeSystemDefault
)

// Traffic legs re-exported for publish and take paths.
const (
	// LegData carries plain pubsub samples.
	LegData = transports.LegData
	// LegRequest carries service plus action requests.
	LegRequest = transports.LegRequest
	// LegReply carries service plus action replies.
	LegReply = transports.LegReply
)

// Sentinel errors re-exported for errors.Is checks.
var (
	// ErrBackendClosed reports use after Close.
	ErrBackendClosed = transports.ErrBackendClosed
	// ErrNotJoined reports Publish, Take, or Report before JoinDiscovery.
	ErrNotJoined = transports.ErrNotJoined
	// ErrStaleGeneration reports a reused or backward liveliness generation.
	ErrStaleGeneration = transports.ErrStaleGeneration
)

// DefaultDDSConfig returns a DDS config with localhost defaults.
func DefaultDDSConfig() DDSConfig {
	return dds.DefaultSidecarConfig()
}

// DefaultMQTTConfig returns an MQTT config with in-memory defaults.
func DefaultMQTTConfig() MQTTConfig {
	return MQTTConfig{
		TopicPrefix:   mqtt.DefaultTopicPrefix,
		KeepAliveSecs: mqtt.DefaultKeepAliveSecs,
		Version:       mqtt.Version5,
	}
}

// NewDDS validates the config and returns a DDS backend.
//
// JoinDiscovery dials the transport. The returned backend owns its
// sockets. Close releases them.
func NewDDS(cfg DDSConfig) (Backend, error) {
	backend, err := dds.NewBackend(cfg)
	if err != nil {
		return nil, fmt.Errorf("bus: new dds backend: %w", err)
	}

	return backend, nil
}

// NewMQTT validates the config and returns an MQTT backend.
//
// An empty BrokerURL selects in-memory mode with no broker dial.
// A set BrokerURL selects the configured broker session.
func NewMQTT(cfg MQTTConfig) (Backend, error) {
	backend, err := mqtt.New(cfg)
	if err != nil {
		return nil, fmt.Errorf("bus: new mqtt backend: %w", err)
	}

	return backend, nil
}

// NewRouter builds a topic prefix router over named backends.
func NewRouter(backends map[string]Backend, cfg RouterConfig) (Backend, error) {
	router, err := transports.NewRouter(backends, cfg)
	if err != nil {
		return nil, fmt.Errorf("bus: new router: %w", err)
	}

	return router, nil
}

// OpenDDS creates a DDS backend and joins discovery in one call.
func OpenDDS(ctx context.Context, cfg DDSConfig, info DiscoveryInfo) (Backend, error) {
	backend, err := NewDDS(cfg)
	if err != nil {
		return nil, err
	}

	err = backend.JoinDiscovery(ctx, info)
	if err != nil {
		closeErr := backend.Close()
		if closeErr != nil {
			joined := errors.Join(err, closeErr)
			return nil, fmt.Errorf("bus: open dds join discovery: %w", joined)
		}

		return nil, fmt.Errorf("bus: open dds join discovery: %w", err)
	}

	return backend, nil
}

// OpenMQTT creates an MQTT backend and joins discovery in one call.
func OpenMQTT(ctx context.Context, cfg MQTTConfig, info DiscoveryInfo) (Backend, error) {
	backend, err := NewMQTT(cfg)
	if err != nil {
		return nil, err
	}

	err = backend.JoinDiscovery(ctx, info)
	if err != nil {
		closeErr := backend.Close()
		if closeErr != nil {
			joined := errors.Join(err, closeErr)
			return nil, fmt.Errorf("bus: open mqtt join discovery: %w", joined)
		}

		return nil, fmt.Errorf("bus: open mqtt join discovery: %w", err)
	}

	return backend, nil
}

// ResolveDomain resolves the caller domain id through ROS_DOMAIN_ID.
func ResolveDomain(caller uint32) (uint32, error) {
	id, err := transports.ResolveDomainID(caller)
	if err != nil {
		return 0, fmt.Errorf("bus: resolve domain: %w", err)
	}

	return id, nil
}

// ParseDiscoveryRange parses a discovery range name.
func ParseDiscoveryRange(s string) (DiscoveryRange, error) {
	r, err := transports.ParseDiscoveryRange(s)
	if err != nil {
		return DiscoveryRangeNotSet, fmt.Errorf("bus: parse discovery range: %w", err)
	}

	return r, nil
}
