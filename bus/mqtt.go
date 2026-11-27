// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

package bus

import (
	"context"
	"errors"
	"fmt"

	"github.com/podomy/weld/internal/transports/mqtt"
)

// Conn hides the real MQTT broker behind a small testable surface.
//
// Publish copies its payload and properties; the caller keeps its slice.
// Receive blocks until the next queued sample arrives or ctx ends.
type Conn = mqtt.Conn

// ConnMessage is one wire sample exchanged with the broker.
type ConnMessage = mqtt.ConnMessage

// Properties carries MQTT v5 user properties for one sample.
//
// TypeHash holds the RIHS01 hash when known and empty when unknown.
// Extra carries caller defined pairs. Nil means none.
type Properties = mqtt.Properties

// Kind names the ROS traffic class carried on one MQTT topic.
type Kind = mqtt.Kind

// MQTT traffic classes for data, request, reply, and discovery paths.
const (
	// KindData carries plain pubsub samples on the rt leg.
	KindData = mqtt.KindData
	// KindRequest carries service or action requests on the rq leg.
	KindRequest = mqtt.KindRequest
	// KindReply carries service or action replies on the rr leg.
	KindReply = mqtt.KindReply
	// KindDiscovery carries retained endpoint announcements on the sdp leg.
	KindDiscovery = mqtt.KindDiscovery
)

// ServiceRole selects the service direction for one call.
type ServiceRole = mqtt.ServiceRole

// Service roles for publish and take paths.
const (
	// ServiceRoleServer takes requests and publishes replies.
	ServiceRoleServer = mqtt.ServiceRoleServer
	// ServiceRoleClient publishes requests and takes replies.
	ServiceRoleClient = mqtt.ServiceRoleClient
)

// ServiceMessage is one taken service sample.
type ServiceMessage = mqtt.ServiceMessage

// Version selects the MQTT protocol version on the wire.
type Version = mqtt.Version

// MQTT protocol versions.
const (
	// Version5 speaks MQTT v5.0 with user properties.
	Version5 = mqtt.Version5
	// Version311 speaks MQTT 3.1.1 without properties.
	Version311 = mqtt.Version311
)

// mqttBackend asserts a bus backend to the MQTT implementation.
//
// It reports an error when the backend is nil or was built by another
// transport so wrappers fail loudly instead of panicking.
func mqttBackend(backend Backend) (*mqtt.Backend, error) {
	if backend == nil {
		return nil, errors.New("bus: mqtt backend is nil") //nolint:wrapcheck // new error, nothing to wrap
	}

	mb, ok := backend.(*mqtt.Backend)
	if !ok {
		return nil, errors.New("bus: backend is not an mqtt backend") //nolint:wrapcheck // new error, nothing to wrap
	}

	return mb, nil
}

// NewWithConn validates the config and returns an MQTT backend over conn.
//
// The caller owns conn before the call and the backend owns it after.
// Nil conn is rejected. Tests inject the in-memory channel or a scripted
// stub while broker dialers inject a live session.
func NewWithConn(cfg MQTTConfig, conn Conn) (Backend, error) {
	backend, err := mqtt.NewWithConn(cfg, conn)
	if err != nil {
		return nil, fmt.Errorf("bus: new mqtt backend with conn: %w", err)
	}

	return backend, nil
}

// EncodeTopic maps one ROS topic to its MQTT wire form.
//
// The form is prefix/domain/kind/stripped where stripped is the ROS name
// without its leading slash. The root ROS topic slash encodes without a
// trailing stripped level.
func EncodeTopic(prefix string, domain uint32, kind Kind, rosTopic string) (string, error) {
	topic, err := mqtt.EncodeTopic(prefix, domain, kind, rosTopic)
	if err != nil {
		return "", fmt.Errorf("bus: encode mqtt topic: %w", err)
	}

	return topic, nil
}

// DecodeTopic parses one MQTT wire topic back to domain, kind, and ROS name.
//
// The prefix argument is normalized the same way as EncodeTopic so empty
// means the default weld root. The result round trips through EncodeTopic.
func DecodeTopic(prefix string, mqttTopic string) (uint32, Kind, string, error) {
	domain, kind, rosTopic, err := mqtt.DecodeTopic(prefix, mqttTopic)
	if err != nil {
		return 0, "", "", fmt.Errorf("bus: decode mqtt topic: %w", err)
	}

	return domain, kind, rosTopic, nil
}

// EncodeDiscoveryTopic maps one endpoint to its retained discovery topic.
//
// The form is prefix/domain/sdp/<participant>/<kind>/<stripped> where kind
// is rt, rq, or rr and stripped is the ROS name without its leading slash.
func EncodeDiscoveryTopic(prefix string, domain uint32, participant string, kind Kind, rosTopic string) (string, error) {
	topic, err := mqtt.EncodeDiscoveryTopic(prefix, domain, participant, kind, rosTopic)
	if err != nil {
		return "", fmt.Errorf("bus: encode mqtt discovery topic: %w", err)
	}

	return topic, nil
}

// DecodeDiscoveryTopic parses one retained discovery topic.
func DecodeDiscoveryTopic(prefix string, mqttTopic string) (uint32, string, Kind, string, error) {
	domain, participant, kind, rosTopic, err := mqtt.DecodeDiscoveryTopic(prefix, mqttTopic)
	if err != nil {
		return 0, "", "", "", fmt.Errorf("bus: decode mqtt discovery topic: %w", err)
	}

	return domain, participant, kind, rosTopic, nil
}

// NormalizePrefix trims surrounding space and slashes from a prefix.
//
// Empty input returns the default weld root. The result never has leading
// or trailing slashes. It keeps inner slashes so hierarchical roots stay
// valid.
func NormalizePrefix(prefix string) string {
	return mqtt.NormalizePrefix(prefix)
}

// PublishService publishes one service request or reply sample.
//
// The client role publishes requests and the server role publishes replies.
// The payload is opaque service bytes. Requires a prior JoinDiscovery and
// an MQTT backend; other backends report an error.
func PublishService(ctx context.Context, backend Backend, service string, typeName string, role ServiceRole, payload []byte, typeHash string) error {
	mb, err := mqttBackend(backend)
	if err != nil {
		return err
	}

	err = mb.PublishService(ctx, service, typeName, role, payload, typeHash)
	if err != nil {
		return fmt.Errorf("bus: publish mqtt service: %w", err)
	}

	return nil
}

// TakeService takes one service sample for one role.
//
// The server role takes requests and the client role takes replies.
// Blocks until a matching sample arrives; ctx cancellation reports ctx.Err.
// Requires an MQTT backend; other backends report an error.
func TakeService(ctx context.Context, backend Backend, role ServiceRole) (ServiceMessage, error) {
	mb, err := mqttBackend(backend)
	if err != nil {
		return ServiceMessage{}, err
	}

	msg, err := mb.TakeService(ctx, role)
	if err != nil {
		return ServiceMessage{}, fmt.Errorf("bus: take mqtt service: %w", err)
	}

	return msg, nil
}

// AnnounceEndpoint publishes one retained endpoint announcement.
//
// Kind selects the rt, rq, or rr leg the endpoint serves. Requires a prior
// JoinDiscovery and an MQTT backend; other backends report an error.
func AnnounceEndpoint(ctx context.Context, backend Backend, participant string, kind Kind, rosTopic string, typeName string, typeHash string) error {
	mb, err := mqttBackend(backend)
	if err != nil {
		return err
	}

	err = mb.AnnounceEndpoint(ctx, participant, kind, rosTopic, typeName, typeHash)
	if err != nil {
		return fmt.Errorf("bus: announce mqtt endpoint: %w", err)
	}

	return nil
}

// RetractEndpoint publishes one retained dispose tombstone.
//
// Requires a prior JoinDiscovery and an MQTT backend; other backends
// report an error.
func RetractEndpoint(ctx context.Context, backend Backend, participant string, kind Kind, rosTopic string) error {
	mb, err := mqttBackend(backend)
	if err != nil {
		return err
	}

	err = mb.RetractEndpoint(ctx, participant, kind, rosTopic)
	if err != nil {
		return fmt.Errorf("bus: retract mqtt endpoint: %w", err)
	}

	return nil
}

// HasRemoteEndpoint reports whether a live announcement names the topic.
//
// Only the data leg is consulted. Non-MQTT backends report false.
func HasRemoteEndpoint(backend Backend, rosTopic string) bool {
	mb, err := mqttBackend(backend)
	if err != nil {
		return false
	}

	return mb.HasRemoteEndpoint(rosTopic)
}

// DiscoveryGeneration returns the local effective-install count.
//
// Non-MQTT backends report zero.
func DiscoveryGeneration(backend Backend) uint64 {
	mb, err := mqttBackend(backend)
	if err != nil {
		return 0
	}

	return mb.DiscoveryGeneration()
}

// Dropped returns the count of take-path losses.
//
// Inbox and payload bounds are enforced inside the backends and surfaced
// here: MQTT bounds the take inbox and cross-leg buffers with evict-oldest
// plus drops, and both MQTT and DDS cap one user payload at 32 MiB with
// over cap publishes rejected. Every eviction or malformed take-path drop
// increments this counter. There is no public tuning knob for these bounds
// in v1. Non-MQTT backends report zero.
func Dropped(backend Backend) uint64 {
	mb, err := mqttBackend(backend)
	if err != nil {
		return 0
	}

	return mb.Dropped()
}

// Reconnects returns the count of successful redials after drops.
//
// In-memory connections never redial and report zero. Non-MQTT backends
// report zero.
func Reconnects(backend Backend) uint64 {
	mb, err := mqttBackend(backend)
	if err != nil {
		return 0
	}

	return mb.Reconnects()
}

// LeaveDiscovery unsubscribes the join filters from a live broker.
//
// In-memory connections need no unsubscription and report success.
// Requires an MQTT backend; other backends report an error.
func LeaveDiscovery(ctx context.Context, backend Backend) error {
	mb, err := mqttBackend(backend)
	if err != nil {
		return err
	}

	err = mb.LeaveDiscovery(ctx)
	if err != nil {
		return fmt.Errorf("bus: leave mqtt discovery: %w", err)
	}

	return nil
}
