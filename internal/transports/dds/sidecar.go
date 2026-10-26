// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

// Package dds holds the CycloneDDS sidecar contract.
//
// CycloneDDS runs as a sidecar process over localhost, not linked into
// Weld. This package contains the contract only: configuration, topic and
// type naming rules, and a stub Sidecar whose Connect and backend methods
// return an explicit not implemented error until the backend lands, except
// Close which performs idempotent teardown. There is no cgo here and no C
// toolchain is required to build Weld.
package dds

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"

	"github.com/podomy/weld/internal/transports"
)

// Topic prefix constants for the DDS sidecar mapping.
const (
	// TopicPrefix carries normal topics such as rt/chatter.
	TopicPrefix = "rt"
	// RequestPrefix carries service requests such as rq/add_two_intsRequest.
	RequestPrefix = "rq"
	// ReplyPrefix carries service replies such as rr/add_two_intsReply.
	ReplyPrefix = "rr"
	// DefaultPeerAddress is the default sidecar peer address.
	DefaultPeerAddress = "127.0.0.1"
)

// DDS type name constants.
const (
	// DDSMarker separates the namespace from the type name.
	DDSMarker = "::dds_::"
	// TypeSuffix is the trailing underscore on every DDS type name.
	TypeSuffix = "_"
	// RequestSuffix is the DDS topic suffix for service requests.
	RequestSuffix = "Request"
	// ReplySuffix is the DDS topic suffix for service replies.
	// Topics use Reply while IDL types use Response. See ReplyTopic.
	ReplySuffix = "Reply"
	// ServiceRequestSuffix is the DDS type suffix for service requests.
	ServiceRequestSuffix = "_Request_"
	// ServiceResponseSuffix is the DDS type suffix for service responses.
	ServiceResponseSuffix = "_Response_"
)

// TopicKind names the reverse mapped kind of a DDS topic.
type TopicKind string

// Topic kinds returned by ParseTopicName.
const (
	// TopicKindTopic is a normal topic.
	TopicKindTopic TopicKind = "topic"
	// TopicKindRequest is a service request topic.
	TopicKindRequest TopicKind = "request"
	// TopicKindReply is a service reply topic.
	TopicKindReply TopicKind = "reply"
)

// ErrSidecarNotImplemented reports that the sidecar backend is not yet present.
var ErrSidecarNotImplemented = errors.New("dds: sidecar backend not implemented")

// SidecarConfig configures the CycloneDDS sidecar peer.
//
// The sidecar speaks to Weld over a peer address that defaults to
// localhost. PeerAddress accepts any address: the loopback default covers
// production and remote addresses cover tests that run the sidecar on
// another host. Range is the discovery range wired into
// DiscoveryInfo.Range on JoinDiscovery. Range is explicit config only. No
// environment variable is read for it because vendors share no single
// stable env contract for discovery range.
type SidecarConfig struct {
	// DomainID is the DDS domain the sidecar joins.
	DomainID uint32
	// PeerAddress is the address of the sidecar process.
	PeerAddress string
	// Range is the discovery range for JoinDiscovery.
	Range transports.DiscoveryRange
}

// DefaultSidecarConfig returns a SidecarConfig with localhost defaults.
func DefaultSidecarConfig() SidecarConfig {
	return SidecarConfig{
		DomainID:    0,
		PeerAddress: DefaultPeerAddress,
		Range:       transports.DiscoveryRangeSystemDefault,
	}
}

// Validate checks the sidecar configuration for usable values.
//
// It rejects a domain above transports.MaxDomainID, an empty peer, and
// an unknown discovery range. DiscoveryRangeNotSet is accepted here and
// normalized to SystemDefault by NewSidecar so the zero value config
// still joins with a concrete range.
func (c SidecarConfig) Validate() error {
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
func (c SidecarConfig) EffectivePeer() string {
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
// passes through unchanged here; NewSidecar normalizes it to
// SystemDefault for the stored config.
func (c SidecarConfig) ResolveDiscoveryRange() transports.DiscoveryRange {
	return transports.ResolveDiscoveryRange(c.Range)
}

// Sidecar is a stub for the CycloneDDS sidecar process handle.
//
// Lifecycle order is Connect then JoinDiscovery then PublishBytes and
// TakeBytes then Close. PublishBytes, TakeBytes, and ReportLiveliness
// before a successful JoinDiscovery report an error in a real backend.
// The stub exists so callers can be written against the contract before
// the backend lands. Connect and all Backend data path methods return an
// explicit not implemented error until the backend is available. Close
// performs idempotent teardown and returns nil. The stub is safe for
// concurrent use.
type Sidecar struct {
	mu     sync.Mutex
	config SidecarConfig
	closed bool
}

// NewSidecar validates the configuration and returns a stub Sidecar.
//
// It validates the peer, the domain range, and the discovery range,
// resolves the domain through transports.ResolveDomainID so ROS_DOMAIN_ID
// overrides the caller value, trims the peer, and normalizes
// DiscoveryRangeNotSet to DiscoveryRangeSystemDefault. A caller DomainID
// above transports.MaxDomainID is rejected even when the environment is
// unset.
func NewSidecar(cfg SidecarConfig) (*Sidecar, error) {
	err := cfg.Validate()
	if err != nil {
		return nil, fmt.Errorf("dds: invalid sidecar config: %w", err)
	}

	resolved, err := transports.ResolveDomainID(cfg.DomainID)
	if err != nil {
		return nil, fmt.Errorf("dds: resolve domain: %w", err)
	}

	cfg.DomainID = resolved
	cfg.PeerAddress = cfg.EffectivePeer()
	if cfg.Range == transports.DiscoveryRangeNotSet {
		cfg.Range = transports.DiscoveryRangeSystemDefault
	}

	return &Sidecar{config: cfg}, nil
}

// Config returns a copy of the sidecar configuration.
func (s *Sidecar) Config() SidecarConfig {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.config
}

// Closed reports whether Close has been called.
func (s *Sidecar) Closed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.closed
}

// Connect dials the sidecar process.
//
// Lifecycle is Connect first, then JoinDiscovery, then PublishBytes and
// TakeBytes, then Close. The stub always returns ErrSidecarNotImplemented
// until the backend lands. The stub returns immediately without blocking,
// so ctx cancellation has no observable effect on the stub; this is a
// stub exception. The real backend must honor ctx cancellation and return
// ctx.Err when ctx ends before dialing completes.
func (s *Sidecar) Connect(ctx context.Context) error {
	_ = ctx

	return fmt.Errorf("dds: connect domain %d peer %s: %w", s.config.DomainID, s.config.EffectivePeer(), ErrSidecarNotImplemented)
}

// Close releases the sidecar handle.
//
// It is idempotent per the transports.Backend contract. The first call
// marks the handle closed and later calls return nil. It never returns
// ErrSidecarNotImplemented.
func (s *Sidecar) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.closed = true

	return nil
}

// PublishBytes publishes one serialized message through the sidecar.
//
// It always returns ErrSidecarNotImplemented until the backend lands.
// The stub returns immediately without blocking, so ctx cancellation has
// no observable effect yet; this is a stub exception. The real backend
// must honor ctx deadlines and return ctx.Err on cancellation, copy
// retained payloads per the transports ownership rules, require a prior
// JoinDiscovery (ErrNotJoined otherwise), and report ErrBackendClosed
// after Close. Nil payload means an empty sample.
func (s *Sidecar) PublishBytes(ctx context.Context, msg transports.OutgoingMessage) error {
	_ = ctx
	_ = msg

	return fmt.Errorf("dds: publish domain %d peer %s: %w", s.config.DomainID, s.config.EffectivePeer(), ErrSidecarNotImplemented)
}

// TakeBytes receives one serialized message through the sidecar.
//
// It always returns ErrSidecarNotImplemented until the backend lands.
// The stub returns immediately without blocking, so ctx cancellation has
// no observable effect yet; this is a stub exception. The real backend
// must block until a message arrives or ctx ends, return ctx.Err on
// cancellation, require a prior JoinDiscovery, and report
// ErrBackendClosed after Close.
func (s *Sidecar) TakeBytes(ctx context.Context) (transports.IncomingMessage, error) {
	_ = ctx

	return transports.IncomingMessage{}, fmt.Errorf("dds: take domain %d peer %s: %w", s.config.DomainID, s.config.EffectivePeer(), ErrSidecarNotImplemented)
}

// JoinDiscovery joins discovery with the given participant info.
//
// It always returns ErrSidecarNotImplemented until the backend lands.
// The stub returns immediately without blocking, so ctx cancellation has
// no observable effect yet; this is a stub exception. The real backend
// announces on the SPDP path, honors ctx deadlines, and reports
// ErrBackendClosed after Close.
func (s *Sidecar) JoinDiscovery(ctx context.Context, info transports.DiscoveryInfo) error {
	_ = ctx
	_ = info

	return fmt.Errorf("dds: join discovery domain %d peer %s: %w", s.config.DomainID, s.config.EffectivePeer(), ErrSidecarNotImplemented)
}

// ReportLiveliness reports local endpoint liveliness through the sidecar.
//
// It always returns ErrSidecarNotImplemented until the backend lands.
// The stub returns immediately without blocking, so ctx cancellation has
// no observable effect yet; this is a stub exception. The real backend
// asserts on the SEDP path, honors ctx deadlines, enforces the
// Generation ordering in transports.LivelinessReport (stale returns an
// error), requires a prior JoinDiscovery, and reports ErrBackendClosed
// after Close.
func (s *Sidecar) ReportLiveliness(ctx context.Context, report transports.LivelinessReport) error {
	_ = ctx
	_ = report

	return fmt.Errorf("dds: report liveliness domain %d peer %s: %w", s.config.DomainID, s.config.EffectivePeer(), ErrSidecarNotImplemented)
}

// trimSlashes removes leading and trailing slashes only.
func trimSlashes(name string) string {
	return strings.Trim(name, "/")
}

// cleanTopicField trims spaces and slashes and rejects empty segments.
//
// ROS names are normalized by trimming surrounding whitespace and leading
// and trailing slashes, then rejected when the remainder is empty, holds
// a double slash, ends with a slash, or holds inner whitespace. It
// returns the stripped name with no leading or trailing slashes and
// single slashes inside. Normalization is limited to that trimming;
// case, dots, and other characters pass through unchanged for later
// interop checks.
func cleanTopicField(raw, what string) (string, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return "", fmt.Errorf("dds: %s name is empty", what)
	}

	if strings.Contains(trimmed, "//") {
		return "", fmt.Errorf("dds: %s name %q has an empty segment", what, raw)
	}

	if strings.HasSuffix(trimmed, "/") {
		return "", fmt.Errorf("dds: %s name %q has a trailing slash", what, raw)
	}

	if strings.ContainsAny(trimmed, " \t\n\r") {
		return "", fmt.Errorf("dds: %s name %q has whitespace", what, raw)
	}

	stripped := trimSlashes(trimmed)
	if stripped == "" {
		return "", fmt.Errorf("dds: %s name %q is empty", what, raw)
	}

	if slices.Contains(strings.Split(stripped, "/"), "") {
		return "", fmt.Errorf("dds: %s name %q has an empty segment", what, raw)
	}

	return stripped, nil
}

// TopicName maps a ROS topic name to its DDS topic name.
//
// For example /chatter maps to rt/chatter. When avoidConventions is true
// the bare stripped name is returned with no rt prefix, per the
// avoid_ros_namespace_conventions rule. The bare suffix behavior for
// services is an open question pending a CycloneDDS interop test: normal
// topics that end in Request or Reply keep their names here, but the
// bare reverse parser cannot tell them apart from service topics.
func TopicName(rosTopic string, avoidConventions bool) (string, error) {
	stripped, err := cleanTopicField(rosTopic, "topic")
	if err != nil {
		return "", err
	}

	if avoidConventions {
		return stripped, nil
	}

	return TopicPrefix + "/" + stripped, nil
}

// RequestTopic maps a ROS service name to its DDS request topic.
//
// For example /add_two_ints maps to rq/add_two_intsRequest. When
// avoidConventions is true the bare stripped name plus Request is
// returned with no rq prefix. The avoid conventions form is an open
// question pending a CycloneDDS interop test.
func RequestTopic(serviceName string, avoidConventions bool) (string, error) {
	stripped, err := cleanTopicField(serviceName, "service")
	if err != nil {
		return "", err
	}

	if avoidConventions {
		return stripped + RequestSuffix, nil
	}

	return RequestPrefix + "/" + stripped + RequestSuffix, nil
}

// ReplyTopic maps a ROS service name to its DDS reply topic.
//
// For example /add_two_ints maps to rr/add_two_intsReply. When
// avoidConventions is true the bare stripped name plus Reply is
// returned with no rr prefix. The avoid conventions form is an open
// question pending a CycloneDDS interop test.
//
// Note the topic Reply versus type Response distinction: DDS topics use
// the Reply suffix while IDL response types use the Response suffix in
// ServiceResponseTypeName. Do not mix them.
func ReplyTopic(serviceName string, avoidConventions bool) (string, error) {
	stripped, err := cleanTopicField(serviceName, "service")
	if err != nil {
		return "", err
	}

	if avoidConventions {
		return stripped + ReplySuffix, nil
	}

	return ReplyPrefix + "/" + stripped + ReplySuffix, nil
}

// parseBareTopicName maps a prefix free DDS name back to ROS form.
//
// Bare names carry no rt, rq, or rr prefix. A trailing Request marks a
// request and a trailing Reply marks a reply, otherwise the name is a
// normal topic. The exact names Request and Reply alone fall through to
// normal topics (/Request, /Reply) because they have no base. A plain
// topic that ends in Request or Reply misclassifies here. That ambiguity
// is an open question pending a CycloneDDS interop test.
func parseBareTopicName(trimmed, raw string) (string, TopicKind, error) {
	if strings.HasSuffix(trimmed, RequestSuffix) && trimmed != RequestSuffix {
		base := strings.TrimSuffix(trimmed, RequestSuffix)
		if base == "" || strings.HasSuffix(base, "/") {
			return "", "", fmt.Errorf("dds: dds topic name %q has an empty base", raw)
		}

		return "/" + base, TopicKindRequest, nil
	}

	if strings.HasSuffix(trimmed, ReplySuffix) && trimmed != ReplySuffix {
		base := strings.TrimSuffix(trimmed, ReplySuffix)
		if base == "" || strings.HasSuffix(base, "/") {
			return "", "", fmt.Errorf("dds: dds topic name %q has an empty base", raw)
		}

		return "/" + base, TopicKindReply, nil
	}

	return "/" + trimmed, TopicKindTopic, nil
}

// validateDDSTopicName rejects malformed DDS topic names.
func validateDDSTopicName(trimmed, raw string) error {
	if trimmed == "" {
		return errors.New("dds: dds topic name is empty") //nolint:wrapcheck // new error, nothing to wrap
	}

	if strings.HasPrefix(trimmed, "/") {
		return fmt.Errorf("dds: dds topic name %q must not start with slash", raw)
	}

	if strings.HasSuffix(trimmed, "/") {
		return fmt.Errorf("dds: dds topic name %q has a trailing slash", raw)
	}

	if strings.Contains(trimmed, "//") {
		return fmt.Errorf("dds: dds topic name %q has an empty segment", raw)
	}

	if strings.ContainsAny(trimmed, " \t\n\r") {
		return fmt.Errorf("dds: dds topic name %q has whitespace", raw)
	}

	if slices.Contains(strings.Split(trimmed, "/"), "") {
		return fmt.Errorf("dds: dds topic name %q has an empty segment", raw)
	}

	return nil
}

// parseRequestRemainder maps a request remainder to its ROS name.
func parseRequestRemainder(remainder, raw string) (string, TopicKind, bool, error) {
	if !strings.HasSuffix(remainder, RequestSuffix) {
		return "", "", false, fmt.Errorf("dds: request topic %q misses %q suffix", raw, RequestSuffix)
	}

	base := strings.TrimSuffix(remainder, RequestSuffix)
	if base == "" {
		return "", "", false, fmt.Errorf("dds: request topic %q has an empty base", raw)
	}

	return "/" + base, TopicKindRequest, true, nil
}

// parseReplyRemainder maps a reply remainder to its ROS name.
func parseReplyRemainder(remainder, raw string) (string, TopicKind, bool, error) {
	if !strings.HasSuffix(remainder, ReplySuffix) {
		return "", "", false, fmt.Errorf("dds: reply topic %q misses %q suffix", raw, ReplySuffix)
	}

	base := strings.TrimSuffix(remainder, ReplySuffix)
	if base == "" {
		return "", "", false, fmt.Errorf("dds: reply topic %q has an empty base", raw)
	}

	return "/" + base, TopicKindReply, true, nil
}

// parsePrefixedTopicName maps prefixed DDS names to ROS form.
// It reports handled false for bare names without a known prefix.
func parsePrefixedTopicName(trimmed, raw string) (string, TopicKind, bool, error) {
	prefix, remainder, found := strings.Cut(trimmed, "/")
	if !found {
		return "", "", false, nil
	}

	switch prefix {
	case TopicPrefix:
		return "/" + remainder, TopicKindTopic, true, nil
	case RequestPrefix:
		return parseRequestRemainder(remainder, raw)
	case ReplyPrefix:
		return parseReplyRemainder(remainder, raw)
	default:
		return "", "", false, nil
	}
}

// ParseTopicName maps a DDS topic name back to its ROS name and kind.
//
// It accepts rt, rq, and rr prefixed names plus bare names with no
// prefix. Prefixed requests require the Request suffix and prefixed
// replies require the Reply suffix. Bare names use the same suffix rule
// in parseBareTopicName. Empty segments, leading slashes, trailing
// slashes, and inner whitespace are rejected.
//
// Prefixes are case sensitive lowercase. Uppercase forms such as
// RT/chatter are not treated as prefixed names; they fall through to the
// bare path and map to /RT/chatter as a normal topic. A single segment
// rt, rq, or rr alone is also bare: rt maps to /rt as a normal topic,
// never to an empty prefixed name. The exact bare names Request and Reply
// map to /Request and /Reply as normal topics.
func ParseTopicName(ddsTopic string) (string, TopicKind, error) {
	trimmed := strings.TrimSpace(ddsTopic)
	if err := validateDDSTopicName(trimmed, ddsTopic); err != nil {
		return "", "", err
	}

	if ros, kind, handled, err := parsePrefixedTopicName(trimmed, ddsTopic); err != nil || handled {
		return ros, kind, err
	}

	return parseBareTopicName(trimmed, ddsTopic)
}

// splitIDL splits an IDL name on double underscores and validates parts.
func splitIDL(idlName string) ([]string, error) {
	parts := strings.Split(idlName, "__")
	if len(parts) < 3 {
		return nil, fmt.Errorf("dds: idl name %q needs at least pkg__group__Name", idlName)
	}

	for _, part := range parts {
		if strings.TrimSpace(part) == "" {
			return nil, fmt.Errorf("dds: idl name %q has an empty segment", idlName)
		}
	}

	return parts, nil
}

// MessageTypeName maps an IDL message name to its DDS type name.
//
// It maps double underscores to :: and forms <ns>::dds_::<Name>_.
// For example std_msgs__msg__String maps to
// std_msgs::msg::dds_::String_.
func MessageTypeName(idlName string) (string, error) {
	parts, err := splitIDL(idlName)
	if err != nil {
		return "", fmt.Errorf("dds: message type name: %w", err)
	}

	ns := strings.Join(parts[:len(parts)-1], "::")
	name := parts[len(parts)-1]

	return ns + DDSMarker + name + TypeSuffix, nil
}

// ServiceRequestTypeName maps an IDL service base name to its request type.
//
// The input is the base name without any Request suffix, for example
// my_pkg__srv__AddTwoInts. The output appends the Request suffix before
// the trailing underscore, for example
// my_pkg::srv::dds_::AddTwoInts_Request_.
func ServiceRequestTypeName(idlBase string) (string, error) {
	parts, err := splitIDL(idlBase)
	if err != nil {
		return "", fmt.Errorf("dds: service request type name: %w", err)
	}

	ns := strings.Join(parts[:len(parts)-1], "::")
	name := parts[len(parts)-1]

	return ns + DDSMarker + name + ServiceRequestSuffix, nil
}

// ServiceResponseTypeName maps an IDL service base name to its response type.
//
// The input is the base name without any Response suffix, for example
// my_pkg__srv__AddTwoInts. The output appends the Response suffix before
// the trailing underscore, for example
// my_pkg::srv::dds_::AddTwoInts_Response_. Note the type Response versus
// topic Reply distinction: DDS reply topics use ReplyTopic with Reply
// while response types use Response here.
func ServiceResponseTypeName(idlBase string) (string, error) {
	parts, err := splitIDL(idlBase)
	if err != nil {
		return "", fmt.Errorf("dds: service response type name: %w", err)
	}

	ns := strings.Join(parts[:len(parts)-1], "::")
	name := parts[len(parts)-1]

	return ns + DDSMarker + name + ServiceResponseSuffix, nil
}

// isIdentifier reports whether s is a C style identifier.
func isIdentifier(s string) bool {
	if s == "" {
		return false
	}

	for i, r := range s {
		if r == '_' || (r >= 'A' && r <= 'Z') || (r >= 'a' && r <= 'z') {
			continue
		}

		if i > 0 && r >= '0' && r <= '9' {
			continue
		}

		return false
	}

	return true
}

// DemangleTypeName reverses a DDS type name back to IDL form.
//
// It requires exactly one ::dds_:: marker, using the last index after
// asserting the single marker count. It strips the trailing underscore
// and maps :: back to __. The left namespace splits on :: with empty
// segments rejected. The right side must be an identifier, with an
// optional Request or Response part still a plain identifier. Service
// suffixes are preserved, so
// my_pkg::srv::dds_::AddTwoInts_Request_ demangles to
// my_pkg__srv__AddTwoInts_Request.
func DemangleTypeName(ddsName string) (string, error) {
	if strings.Count(ddsName, DDSMarker) != 1 {
		return "", fmt.Errorf("dds: type name %q must contain one %q marker", ddsName, DDSMarker)
	}

	idx := strings.LastIndex(ddsName, DDSMarker)
	left := ddsName[:idx]
	right := ddsName[idx+len(DDSMarker):]

	if !strings.HasSuffix(right, TypeSuffix) {
		return "", fmt.Errorf("dds: type name %q misses trailing underscore", ddsName)
	}

	right = strings.TrimSuffix(right, TypeSuffix)

	if strings.TrimSpace(left) == "" || strings.TrimSpace(right) == "" {
		return "", fmt.Errorf("dds: type name %q has an empty segment", ddsName)
	}

	if strings.Contains(right, ":") {
		return "", fmt.Errorf("dds: type name %q has unexpected scope in %q", ddsName, right)
	}

	if !isIdentifier(right) {
		return "", fmt.Errorf("dds: type name %q has invalid identifier %q", ddsName, right)
	}

	scope := strings.Split(left, "::")
	for _, part := range scope {
		if part == "" {
			return "", fmt.Errorf("dds: type name %q has an empty namespace segment", ddsName)
		}

		if !isIdentifier(part) {
			return "", fmt.Errorf("dds: type name %q has invalid namespace %q", ddsName, part)
		}
	}

	ns := strings.Join(scope, "__")

	return ns + "__" + right, nil
}

// Compile time check that the stub exposes the Backend contract. The
// methods stay stubbed with ErrSidecarNotImplemented until the backend
// lands as a separate type or this stub grows a real implementation.
// Close already performs idempotent teardown and returns nil.
var _ transports.Backend = (*Sidecar)(nil)
