// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

// Package transports defines the backend interface all network buses implement.
//
// internal/core never imports this package. Core owns matching, QoS,
// routing, timeouts, retries, and state machines. Transports are thin
// translators at the border: inbound they turn wire bytes into core
// messages, outbound they turn core messages into wire bytes.
//
// New buses arrive as pull requests implementing Backend with interop
// tests, never as plugins. There is no plugin registry and no dynamic
// loading, so version skew has nowhere to hide.
package transports

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
)

// DomainEnvVar names the environment variable carrying the DDS domain id.
const DomainEnvVar = "ROS_DOMAIN_ID"

// MaxDomainID is the largest accepted domain id on the ROS wire.
//
// ROS 2 restricts ROS_DOMAIN_ID to 0 through 101 inclusive. Values above
// this maximum are rejected as out of range.
const MaxDomainID uint32 = 101

// ErrBackendClosed reports use after Close.
var ErrBackendClosed = errors.New("transports: backend closed")

// ErrNotJoined reports Publish, Take, or Report before JoinDiscovery.
var ErrNotJoined = errors.New("transports: discovery not joined")

// ErrStaleGeneration reports a reused or backward liveliness generation.
var ErrStaleGeneration = errors.New("transports: stale generation")

// DomainConfig carries the configured domain id for a participant.
//
// The actual domain id is resolved from the environment at startup. When
// ROS_DOMAIN_ID is unset or empty the caller value in DomainID is kept.
// Otherwise the environment value must be an integral decimal in range
// 0 through MaxDomainID.
type DomainConfig struct {
	// DomainID is the caller supplied domain id used when the environment
	// carries no override.
	DomainID uint32
}

// Resolve returns the actual domain id for the receiver.
//
// It reads ROS_DOMAIN_ID and keeps the configured value when the variable
// is unset or empty. Both the caller value and the environment value must
// be at most MaxDomainID.
func (c DomainConfig) Resolve() (uint32, error) {
	id, err := ResolveDomainID(c.DomainID)
	if err != nil {
		return 0, fmt.Errorf("transports: resolve domain: %w", err)
	}

	return id, nil
}

// ResolveDomainID resolves the actual domain id for a caller value.
//
// When ROS_DOMAIN_ID is unset or empty it returns caller unchanged.
// Otherwise it trims surrounding whitespace, strips one optional leading
// plus, and parses the rest as base 10 decimal. A spaced plus such as
// "  +42  " parses as 42. Zero padded decimal such as 042 parses as 42.
// Doubled plus ("++42"), hex ("0x2A"), underscores ("4_2"), signs ("-1"),
// floats ("3.5"), exponents ("1e2"), and trailing text ("12x") are
// rejected. The caller value itself must also be at most MaxDomainID,
// so a misconfigured caller fails even when the environment carries no
// override.
func ResolveDomainID(caller uint32) (uint32, error) {
	if caller > MaxDomainID {
		return 0, fmt.Errorf("transports: caller domain id %d above maximum %d", caller, MaxDomainID)
	}

	raw, ok := os.LookupEnv(DomainEnvVar)
	if !ok {
		return caller, nil
	}

	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return caller, nil
	}

	digits := strings.TrimPrefix(trimmed, "+")
	if digits == "" {
		return 0, fmt.Errorf("transports: parse %s %q: missing digits", DomainEnvVar, raw)
	}

	parsed, err := strconv.ParseUint(digits, 10, 32)
	if err != nil {
		return 0, fmt.Errorf("transports: parse %s %q: %w", DomainEnvVar, raw, err)
	}

	id := uint32(parsed)
	if id > MaxDomainID {
		return 0, fmt.Errorf("transports: domain id %d above maximum %d", id, MaxDomainID)
	}

	return id, nil
}

// DiscoveryRange selects which network segment discovery may use.
type DiscoveryRange int

const (
	// DiscoveryRangeNotSet means no explicit range was configured.
	// Transports pass it through unchanged. Sidecar construction
	// normalizes it to DiscoveryRangeSystemDefault so a zero value
	// config still joins with a concrete range.
	DiscoveryRangeNotSet DiscoveryRange = iota
	// DiscoveryRangeOff disables discovery multicast and peer contact.
	DiscoveryRangeOff
	// DiscoveryRangeLocalhost restricts discovery to the local host.
	DiscoveryRangeLocalhost
	// DiscoveryRangeSubnet restricts discovery to the local subnet.
	DiscoveryRangeSubnet
	// DiscoveryRangeSystemDefault defers to the middleware default range.
	DiscoveryRangeSystemDefault
)

// String returns the lowercase name of the discovery range.
//
// Unknown values format as DiscoveryRange(id) so logs keep the raw
// number instead of collapsing distinct misconfigurations to one label.
func (r DiscoveryRange) String() string {
	switch r {
	case DiscoveryRangeOff:
		return "off"
	case DiscoveryRangeLocalhost:
		return "localhost"
	case DiscoveryRangeSubnet:
		return "subnet"
	case DiscoveryRangeSystemDefault:
		return "system_default"
	case DiscoveryRangeNotSet:
		return "not_set"
	default:
		return fmt.Sprintf("DiscoveryRange(%d)", int(r))
	}
}

// ParseDiscoveryRange parses a discovery range name.
//
// Accepted values are off, localhost, subnet, system_default, and not_set.
// Matching is case insensitive with surrounding whitespace ignored.
// An empty string maps to DiscoveryRangeNotSet.
// Callers must check the error. Unknown names return an error and
// DiscoveryRangeNotSet. Ignoring the error silently treats a typo as
// unset, which changes the discovery segment.
func ParseDiscoveryRange(s string) (DiscoveryRange, error) {
	switch strings.TrimSpace(strings.ToLower(s)) {
	case "":
		return DiscoveryRangeNotSet, nil
	case "not_set":
		return DiscoveryRangeNotSet, nil
	case "off":
		return DiscoveryRangeOff, nil
	case "localhost":
		return DiscoveryRangeLocalhost, nil
	case "subnet":
		return DiscoveryRangeSubnet, nil
	case "system_default":
		return DiscoveryRangeSystemDefault, nil
	default:
		return DiscoveryRangeNotSet, fmt.Errorf("transports: unknown discovery range %q", s)
	}
}

// ResolveDiscoveryRange returns the configured range unchanged.
//
// Unlike ResolveDomainID it reads no environment variable. There is no
// single stable env contract for discovery range across vendors, so the
// range stays explicit config. NotSet passes through unchanged here.
// Sidecar construction normalizes NotSet to SystemDefault so the join
// path always carries a concrete range. Callers that need a concrete
// range without a Sidecar must map NotSet explicitly.
func ResolveDiscoveryRange(configured DiscoveryRange) DiscoveryRange {
	return configured
}

// DiscoveryInfo describes a local participant joining discovery.
type DiscoveryInfo struct {
	// ParticipantName is the human readable participant name. It uses the
	// same namespace as LivelinessReport.ParticipantName so join and
	// liveliness observations correlate by exact string match.
	ParticipantName string
	// DomainID is the resolved domain id the participant joins.
	DomainID uint32
	// Range is the configured discovery range.
	Range DiscoveryRange
}

// LivelinessReport carries an aliveness assertion for a local endpoint.
//
// ParticipantName uses the same namespace as
// DiscoveryInfo.ParticipantName. Generation is owned by the caller: each
// ReportLiveliness for one participant must carry a Generation greater
// than the previous one. The backend accepts Generation greater than the
// last observed value and advances its watermark. A repeated Generation
// with the same Alive value is idempotent and a no-op. A Generation less
// than or equal to the watermark with any other content is stale and
// reports ErrStaleGeneration: same Generation with a different Alive
// value is a conflicting reuse, and a smaller Generation is backward.
// Alive false means dispose: the endpoint is gone and the backend
// unregisters it. Repeated dispose reports with the same Generation are
// idempotent. The caller must serialize increments per participant; the
// backend only checks ordering, it never assigns generations.
type LivelinessReport struct {
	// ParticipantName names the participant asserting liveliness.
	ParticipantName string
	// DomainID is the domain the assertion belongs to.
	DomainID uint32
	// Alive is true for a live assertion and false for a gone assertion.
	// False means dispose and unregisters the endpoint.
	Alive bool
	// Generation is a monotonic sequence for ordering assertions. The
	// caller owns the increment per participant.
	Generation uint64
}

// OutgoingMessage is one serialized message handed to a backend for send.
//
// Ownership is copy once. The caller passes Payload as uniquely owned
// and must not mutate it after PublishBytes returns. The callee copies
// the bytes when it retains them beyond the call. This keeps one copy
// on the send path. A nil Payload is valid and means an empty sample;
// backends treat nil and zero length identically and must not retain
// the caller slice in either case.
//
// v1 carries TopicName, TypeName, and Payload only. Type hash, QoS offer,
// GID, and sequence number are intentionally omitted. Interop in v1 is
// best effort on these three fields only: the backend performs no cross
// peer QoS check and no type hash verification. A compatible v2 extension
// adds them without breaking v1 callers, either as new optional fields
// on these structs (TypeHash, QoSOffer, GID, SequenceNumber) or as new
// Backend methods (for example PublishWithParams). V1 code keeps
// compiling and v1 peers keep interoperating; only peers that opt into
// the new fields pay for the checks.
type OutgoingMessage struct {
	// TopicName is the fully qualified ROS name such as /chatter.
	TopicName string
	// TypeName is the ROS IDL name such as std_msgs__msg__String.
	TypeName string
	// Payload holds the XCDR1 CDR bytes excluding any transport header.
	// See the ownership note on the parent type. Nil means empty.
	Payload []byte
}

// IncomingMessage is one serialized message taken from a backend.
//
// Ownership is copy once. TakeBytes returns Payload as a newly owned
// slice the caller may read or mutate. The backend must not retain or
// mutate the returned slice. The callee copies when it retains wire
// bytes beyond the call. A nil Payload is valid and means an empty
// sample, equivalent to zero length.
//
// v1 carries TopicName, TypeName, and Payload only. The same reserved
// extension note as OutgoingMessage applies: type hash, QoS offer, GID,
// and sequence number arrive later without breaking topics.
type IncomingMessage struct {
	// TopicName is the fully qualified ROS name the bytes arrived on.
	TopicName string
	// TypeName is the ROS IDL name of the payload.
	TypeName string
	// Payload holds the XCDR1 CDR bytes received from the wire.
	// See the ownership note on the parent type. Nil means empty.
	Payload []byte
}

// Backend is the small contract every transport bus implements.
//
// Lifecycle order is JoinDiscovery then PublishBytes and TakeBytes then
// Close. For sidecar based buses the process level Connect dials first,
// then JoinDiscovery, then data path, then Close. PublishBytes, TakeBytes,
// and ReportLiveliness before a successful JoinDiscovery report
// ErrNotJoined in a real backend. The fake used in tests enforces the
// same order.
//
// Context controls blocking. TakeBytes blocks until a message arrives or
// ctx ends and returns ctx.Err on cancellation. PublishBytes,
// JoinDiscovery, and ReportLiveliness honor deadlines the same way and
// return ctx.Err when ctx ends before completion.
//
// Concurrency is required. Implementations must be safe for concurrent
// use by multiple goroutines, including concurrent PublishBytes,
// TakeBytes, JoinDiscovery, ReportLiveliness, and Close. Callers may not
// serialize for the backend.
//
// Discovery in v1 flows one way from core to the bus through
// JoinDiscovery and ReportLiveliness. There are no inbound discovery
// events in v1. SPDP participant announcement and SEDP endpoint exchange
// stay inside the middleware. A future version adds inbound events
// either by polling (for example TakeDiscoveryEvent) or by callback,
// with a DiscoveryEvent struct carrying participant, endpoint, alive,
// and generation fields sourced the same caller owned way as
// LivelinessReport.Generation. Until then core learns about remote
// endpoints only through its own matching, never through backend
// callbacks.
//
// Teardown runs through Close. Close is idempotent and safe to call more
// than once. After the first Close the other methods report
// ErrBackendClosed. Implementations copy retained payloads and never
// hold caller slices.
type Backend interface {
	// PublishBytes publishes one serialized message.
	// The caller must not mutate msg.Payload after the call.
	// It blocks only until ctx ends or the send is accepted and
	// returns ctx.Err on cancellation. It requires a prior
	// JoinDiscovery and returns ErrNotJoined otherwise. After Close
	// it returns ErrBackendClosed. Nil payload means empty sample.
	PublishBytes(ctx context.Context, msg OutgoingMessage) error
	// TakeBytes receives one serialized message.
	// The returned Payload is newly owned by the caller.
	// It blocks until a message arrives or ctx ends and returns
	// ctx.Err on cancellation. It requires a prior JoinDiscovery
	// and returns ErrNotJoined otherwise. After Close it returns
	// ErrBackendClosed.
	TakeBytes(ctx context.Context) (IncomingMessage, error)
	// JoinDiscovery joins discovery with the given participant info.
	// It announces the local participant on the SPDP path.
	// It honors ctx deadlines and returns ctx.Err on cancellation.
	// After Close it returns ErrBackendClosed.
	JoinDiscovery(ctx context.Context, info DiscoveryInfo) error
	// ReportLiveliness reports local endpoint liveliness.
	// It asserts endpoints on the SEDP path. It honors ctx deadlines
	// and returns ctx.Err on cancellation. It requires a prior
	// JoinDiscovery and returns ErrNotJoined otherwise. Stale or
	// backward generations return ErrStaleGeneration. Repeated
	// reports with the same Generation and Alive value are
	// idempotent. After Close it returns ErrBackendClosed.
	ReportLiveliness(ctx context.Context, report LivelinessReport) error
	// Close releases the backend handle and its middleware resources.
	// It is idempotent. The first call tears down, later calls return nil.
	Close() error
}
