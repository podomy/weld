// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

// Package mqtt holds the MQTT transport skeleton.
//
// MQTT lands after v1 behind the same Backend interface as DDS. Core
// never imports this package. This file carries the stable mapping
// decisions plus a stub Backend whose network path reports
// ErrMQTTNotImplemented. The stub keeps full lifecycle semantics so
// callers can be written against the contract before the broker
// dial lands: JoinDiscovery before data path, generation watermarks
// on liveliness, idempotent Close.
package mqtt

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"

	"github.com/podomy/weld/internal/transports"
)

// Broker defaults and MQTT topic constants.
const (
	// DefaultBrokerAddress is the loopback broker used by default.
	DefaultBrokerAddress = "tcp://127.0.0.1:1883"
	// domainPrefixHead is the fixed head of every domain prefix.
	domainPrefixHead = "weld/d"
	// livelinessSegment carries participant liveliness under a domain.
	livelinessSegment = "liveliness"
	// willOfflinePayload marks a participant as gone in its will.
	willOfflinePayload = "offline"
)

// ErrMQTTNotImplemented reports that the MQTT network path is not yet wired.
var ErrMQTTNotImplemented = errors.New("mqtt: backend not implemented")

// QoS is one MQTT delivery level.
type QoS byte

// MQTT delivery levels.
const (
	// QoSAtMostOnce delivers best effort without acknowledgement.
	QoSAtMostOnce QoS = 0
	// QoSAtLeastOnce delivers at least once with acknowledgement.
	QoSAtLeastOnce QoS = 1
	// QoSExactlyOnce delivers exactly once with a four way handshake.
	QoSExactlyOnce QoS = 2
)

// String returns the short name of the QoS level.
func (q QoS) String() string {
	switch q {
	case QoSAtMostOnce:
		return "at_most_once"
	case QoSAtLeastOnce:
		return "at_least_once"
	case QoSExactlyOnce:
		return "exactly_once"
	default:
		return fmt.Sprintf("QoS(%d)", byte(q))
	}
}

// ValidQoS reports whether q is 0, 1, or 2.
func ValidQoS(q QoS) bool {
	switch q {
	case QoSAtMostOnce, QoSAtLeastOnce, QoSExactlyOnce:
		return true
	default:
		return false
	}
}

// MQTTQoSForReliability maps ROS reliability to an MQTT QoS level.
//
// Reliable maps to at least once and best effort maps to at most
// once. Exactly once is never selected by default because ROS has
// no exactly once policy. It stays reserved for explicit config.
func MQTTQoSForReliability(reliable bool) QoS {
	if reliable {
		return QoSAtLeastOnce
	}

	return QoSAtMostOnce
}

// ReliabilityForMQTTQoS maps an MQTT QoS level back to ROS reliability.
//
// At most once maps to best effort, at least once and exactly once
// map to reliable. Values above 2 report an error.
func ReliabilityForMQTTQoS(q QoS) (bool, error) {
	switch q {
	case QoSAtMostOnce:
		return false, nil
	case QoSAtLeastOnce, QoSExactlyOnce:
		return true, nil
	default:
		return false, fmt.Errorf("mqtt: unknown QoS %d", byte(q))
	}
}

// Durability selects MQTT retention for a topic.
type Durability int

const (
	// DurabilityVolatile keeps no history for late joiners.
	DurabilityVolatile Durability = iota
	// DurabilityTransientLocal retains the last sample for late joiners.
	DurabilityTransientLocal
)

// String returns the short name of the durability.
func (d Durability) String() string {
	switch d {
	case DurabilityVolatile:
		return "volatile"
	case DurabilityTransientLocal:
		return "transient_local"
	default:
		return fmt.Sprintf("Durability(%d)", int(d))
	}
}

// Retained reports whether the durability retains the last sample.
//
// Transient local maps to MQTT retained true so the broker replays
// the last sample to late subscribers. Volatile maps to retained
// false. Unknown values map to false so a typo never retains.
func (d Durability) Retained() bool {
	switch d {
	case DurabilityTransientLocal:
		return true
	case DurabilityVolatile:
		return false
	default:
		return false
	}
}

// DomainPrefix returns the MQTT topic prefix isolating one domain.
//
// Domain 0 maps to weld/d0/ and domain 42 maps to weld/d42/.
// Every data and liveliness topic starts with this prefix, so two
// domains never share a topic string. The caller value must be at
// most transports.MaxDomainID.
func DomainPrefix(domain uint32) (string, error) {
	if domain > transports.MaxDomainID {
		return "", fmt.Errorf("mqtt: domain id %d above maximum %d", domain, transports.MaxDomainID)
	}

	return domainPrefixHead + strconv.FormatUint(uint64(domain), 10) + "/", nil
}

// stripROSTopic removes one leading slash from a ROS topic name.
func stripROSTopic(rosTopic string) (string, error) {
	trimmed := strings.TrimSpace(rosTopic)
	if trimmed == "" {
		return "", errors.New("mqtt: ros topic is empty") //nolint:wrapcheck // new error, nothing to wrap
	}

	if strings.Contains(trimmed, "+") || strings.Contains(trimmed, "#") {
		return "", fmt.Errorf("mqtt: ros topic %q carries MQTT wildcard", rosTopic)
	}

	stripped := strings.TrimPrefix(trimmed, "/")
	if stripped == "" {
		return "", fmt.Errorf("mqtt: ros topic %q has no name", rosTopic)
	}

	if strings.Contains(stripped, " ") {
		return "", fmt.Errorf("mqtt: ros topic %q carries space", rosTopic)
	}

	return stripped, nil
}

// checkTypeName validates an IDL type name for topic mapping.
func checkTypeName(typeName string) (string, error) {
	trimmed := strings.TrimSpace(typeName)
	if trimmed == "" {
		return "", errors.New("mqtt: type name is empty") //nolint:wrapcheck // new error, nothing to wrap
	}

	if strings.ContainsAny(trimmed, "/+# ") {
		return "", fmt.Errorf("mqtt: type name %q carries separator or wildcard", typeName)
	}

	return trimmed, nil
}

// MapTopic maps a ROS topic name into an MQTT data topic.
//
// Domain 0 with /chatter maps to weld/d0/chatter. The domain
// prefix isolates domains: the same ROS name on two domains maps
// to two disjoint MQTT topics. Bare names without a leading
// slash map the same way so avoid namespace conventions peers
// stay compatible.
func MapTopic(domain uint32, rosTopic string) (string, error) {
	prefix, err := DomainPrefix(domain)
	if err != nil {
		return "", err
	}

	stripped, err := stripROSTopic(rosTopic)
	if err != nil {
		return "", err
	}

	return prefix + stripped, nil
}

// MapTopicWithType maps a ROS topic plus IDL type into one MQTT topic.
//
// Domain 0 with /chatter and std_msgs__msg__String maps to
// weld/d0/chatter/std_msgs__msg__String. The type suffix keeps
// two IDL types on one ROS name on disjoint MQTT topics, which
// mirrors type isolation on the Zenoh path. Both parts reject
// MQTT wildcards.
func MapTopicWithType(domain uint32, rosTopic, typeName string) (string, error) {
	base, err := MapTopic(domain, rosTopic)
	if err != nil {
		return "", err
	}

	checked, err := checkTypeName(typeName)
	if err != nil {
		return "", err
	}

	return base + "/" + checked, nil
}

// ParseTopic reverses MapTopic into domain plus ROS topic name.
func ParseTopic(mqttTopic string) (uint32, string, error) {
	domain, remainder, err := splitDomainPrefix(mqttTopic)
	if err != nil {
		return 0, "", err
	}

	if remainder == "" {
		return 0, "", fmt.Errorf("mqtt: topic %q has no name after domain", mqttTopic)
	}

	if strings.Contains(remainder, "+") || strings.Contains(remainder, "#") {
		return 0, "", fmt.Errorf("mqtt: topic %q carries wildcard", mqttTopic)
	}

	return domain, "/" + remainder, nil
}

// ParseTopicWithType reverses MapTopicWithType into domain plus names.
func ParseTopicWithType(mqttTopic string) (uint32, string, string, error) {
	domain, rosTopic, err := ParseTopic(mqttTopic)
	if err != nil {
		return 0, "", "", err
	}

	stripped := strings.TrimPrefix(rosTopic, "/")
	head, tail, ok := splitLastSegment(stripped)
	if !ok {
		return 0, "", "", fmt.Errorf("mqtt: topic %q carries no type suffix", mqttTopic)
	}

	if _, err := checkTypeName(tail); err != nil {
		return 0, "", "", err
	}

	return domain, "/" + head, tail, nil
}

// splitDomainPrefix splits weld/d<id>/<rest> into id plus rest.
func splitDomainPrefix(mqttTopic string) (uint32, string, error) {
	if !strings.HasPrefix(mqttTopic, domainPrefixHead) {
		return 0, "", fmt.Errorf("mqtt: topic %q misses domain prefix", mqttTopic)
	}

	rest := strings.TrimPrefix(mqttTopic, domainPrefixHead)
	digits, remainder, ok := strings.Cut(rest, "/")
	if !ok {
		return 0, "", fmt.Errorf("mqtt: topic %q misses domain separator", mqttTopic)
	}

	if digits == "" {
		return 0, "", fmt.Errorf("mqtt: topic %q misses domain id", mqttTopic)
	}

	parsed, err := strconv.ParseUint(digits, 10, 32)
	if err != nil {
		return 0, "", fmt.Errorf("mqtt: topic %q has bad domain: %w", mqttTopic, err)
	}

	domain := uint32(parsed)
	if domain > transports.MaxDomainID {
		return 0, "", fmt.Errorf("mqtt: topic %q domain %d above maximum %d", mqttTopic, domain, transports.MaxDomainID)
	}

	return domain, remainder, nil
}

// splitLastSegment splits s at its last slash.
func splitLastSegment(s string) (string, string, bool) {
	slash := strings.LastIndex(s, "/")
	if slash < 0 {
		return "", "", false
	}

	head := s[:slash]
	tail := s[slash+1:]
	if head == "" || tail == "" {
		return "", "", false
	}

	return head, tail, true
}

// LivelinessTopic maps a participant into its MQTT liveliness topic.
//
// Domain 0 with talker maps to weld/d0/liveliness/talker. The
// domain prefix isolates liveliness the same way data is
// isolated. Participant names reject slashes and wildcards.
func LivelinessTopic(domain uint32, participantName string) (string, error) {
	prefix, err := DomainPrefix(domain)
	if err != nil {
		return "", err
	}

	trimmed := strings.TrimSpace(participantName)
	if trimmed == "" {
		return "", errors.New("mqtt: participant name is empty") //nolint:wrapcheck // new error, nothing to wrap
	}

	if strings.ContainsAny(trimmed, "/+# ") {
		return "", fmt.Errorf("mqtt: participant %q carries separator or wildcard", participantName)
	}

	return prefix + livelinessSegment + "/" + trimmed, nil
}

// WillMessage is one MQTT last will for a participant.
//
// The broker publishes the will when the client disconnects
// without a clean close, so peers observe the same gone signal
// they would see from a DDS dispose or a Zenoh liveliness drop.
// Payload is owned by the holder and copied on build.
type WillMessage struct {
	// Topic is the MQTT topic the will is published on.
	Topic string
	// Payload is the will body, offline by default.
	Payload []byte
	// QoS is the MQTT delivery level for the will.
	QoS QoS
	// Retained selects broker retention for late joiners.
	Retained bool
}

// BuildLivelinessWill builds the last will for one participant.
//
// The will targets the participant liveliness topic with QoS at
// least once, retained true, and an offline payload. Retained
// true matters: a late joiner subscribing after the drop still
// reads the last will. The payload is copied so the caller may
// reuse its input.
func BuildLivelinessWill(domain uint32, participantName string) (WillMessage, error) {
	topic, err := LivelinessTopic(domain, participantName)
	if err != nil {
		return WillMessage{}, err
	}

	return WillMessage{
		Topic:    topic,
		Payload:  append([]byte(nil), willOfflinePayload...),
		QoS:      QoSAtLeastOnce,
		Retained: true,
	}, nil
}

// Config configures the MQTT backend skeleton.
//
// BrokerAddress names the MQTT broker, DomainID selects the
// domain prefix isolating all topics, and ClientID names the
// local MQTT client. An empty ClientID lets the broker assign
// one. Range is explicit config only with no environment lookup,
// matching the DDS sidecar rule.
type Config struct {
	// DomainID is the domain the backend joins.
	DomainID uint32
	// BrokerAddress is the address of the MQTT broker.
	BrokerAddress string
	// ClientID names the local MQTT client.
	ClientID string
	// Range is the discovery range for JoinDiscovery.
	Range transports.DiscoveryRange
}

// DefaultConfig returns a Config with loopback defaults.
func DefaultConfig() Config {
	return Config{
		DomainID:      0,
		BrokerAddress: DefaultBrokerAddress,
		Range:         transports.DiscoveryRangeSystemDefault,
	}
}

// Validate checks the config for usable values.
func (c Config) Validate() error {
	if c.DomainID > transports.MaxDomainID {
		return fmt.Errorf("mqtt: domain id %d above maximum %d", c.DomainID, transports.MaxDomainID)
	}

	if strings.TrimSpace(c.BrokerAddress) == "" {
		return errors.New("mqtt: broker address is empty") //nolint:wrapcheck // new error, nothing to wrap
	}

	switch c.Range {
	case transports.DiscoveryRangeNotSet,
		transports.DiscoveryRangeOff,
		transports.DiscoveryRangeLocalhost,
		transports.DiscoveryRangeSubnet,
		transports.DiscoveryRangeSystemDefault:
	default:
		return fmt.Errorf("mqtt: unknown discovery range %v", c.Range)
	}

	return nil
}

// EffectiveBroker returns the configured broker trimmed of spaces.
func (c Config) EffectiveBroker() string {
	trimmed := strings.TrimSpace(c.BrokerAddress)
	if trimmed == "" {
		return DefaultBrokerAddress
	}

	return trimmed
}

// Backend is the MQTT lifecycle and mapping skeleton.
//
// Lifecycle follows the Backend contract: JoinDiscovery then the
// data path then Close. JoinDiscovery marks the backend joined
// without dialing. PublishBytes and TakeBytes keep lifecycle
// errors but report ErrMQTTNotImplemented for the wire path until
// the broker dial lands. ReportLiveliness keeps the generation
// watermark so ordering tests pin behavior early. The struct is
// safe for concurrent use.
type Backend struct {
	mu sync.RWMutex
	// cfg is the normalized configuration.
	cfg Config
	// domainID is the resolved domain from DomainConfig.
	domainID uint32
	// joined is true after a successful JoinDiscovery.
	joined bool
	// closed is true after the first Close.
	closed bool
	// lastGen holds the liveliness watermark per participant.
	lastGen map[string]uint64
	// lastAlive holds the last aliveness per participant.
	lastAlive map[string]bool
}

// NewBackend validates the config and returns a skeleton Backend.
func NewBackend(cfg Config) (*Backend, error) {
	err := cfg.Validate()
	if err != nil {
		return nil, fmt.Errorf("mqtt: invalid backend config: %w", err)
	}

	resolved, err := transports.ResolveDomainID(cfg.DomainID)
	if err != nil {
		return nil, fmt.Errorf("mqtt: resolve domain: %w", err)
	}

	cfg.DomainID = resolved
	cfg.BrokerAddress = cfg.EffectiveBroker()
	if cfg.Range == transports.DiscoveryRangeNotSet {
		cfg.Range = transports.DiscoveryRangeSystemDefault
	}

	return &Backend{
		cfg:       cfg,
		domainID:  resolved,
		lastGen:   make(map[string]uint64),
		lastAlive: make(map[string]bool),
	}, nil
}

// Config returns a copy of the backend configuration.
func (b *Backend) Config() Config {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return b.cfg
}

// DomainID returns the resolved domain id.
func (b *Backend) DomainID() uint32 {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return b.domainID
}

// Joined reports whether JoinDiscovery has succeeded.
func (b *Backend) Joined() bool {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return b.joined
}

// Closed reports whether Close has been called.
func (b *Backend) Closed() bool {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return b.closed
}

// JoinDiscovery marks the backend joined without dialing a broker.
//
// It honors ctx cancellation, reports ErrBackendClosed after
// Close, and is idempotent: a second join returns nil without
// changing state.
func (b *Backend) JoinDiscovery(ctx context.Context, _ transports.DiscoveryInfo) error {
	err := ctx.Err()
	if err != nil {
		return fmt.Errorf("mqtt: join discovery: %w", err)
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	if b.closed {
		return fmt.Errorf("mqtt: join discovery after close: %w", transports.ErrBackendClosed)
	}

	b.joined = true

	return nil
}

// PublishBytes keeps lifecycle checks and reports not implemented.
//
// It honors ctx cancellation, requires a prior JoinDiscovery,
// reports ErrBackendClosed after Close, copies the payload to
// honor copy once, and then reports ErrMQTTNotImplemented
// because the broker dial has not landed.
func (b *Backend) PublishBytes(ctx context.Context, msg transports.OutgoingMessage) error {
	err := ctx.Err()
	if err != nil {
		return fmt.Errorf("mqtt: publish bytes: %w", err)
	}

	b.mu.RLock()
	closed := b.closed
	joined := b.joined
	b.mu.RUnlock()

	if closed {
		return fmt.Errorf("mqtt: publish bytes after close: %w", transports.ErrBackendClosed)
	}

	if !joined {
		return fmt.Errorf("mqtt: publish bytes before join: %w", transports.ErrNotJoined)
	}

	if len(msg.Payload) != 0 {
		_ = append([]byte(nil), msg.Payload...)
	}

	return fmt.Errorf("mqtt: publish bytes on %q: %w", msg.TopicName, ErrMQTTNotImplemented)
}

// TakeBytes keeps lifecycle checks and reports not implemented.
//
// It honors ctx cancellation, requires a prior JoinDiscovery,
// reports ErrBackendClosed after Close, and then reports
// ErrMQTTNotImplemented because the broker dial has not landed.
func (b *Backend) TakeBytes(ctx context.Context) (transports.IncomingMessage, error) {
	err := ctx.Err()
	if err != nil {
		return transports.IncomingMessage{}, fmt.Errorf("mqtt: take bytes: %w", err)
	}

	b.mu.RLock()
	closed := b.closed
	joined := b.joined
	b.mu.RUnlock()

	if closed {
		return transports.IncomingMessage{}, fmt.Errorf("mqtt: take bytes after close: %w", transports.ErrBackendClosed)
	}

	if !joined {
		return transports.IncomingMessage{}, fmt.Errorf("mqtt: take bytes before join: %w", transports.ErrNotJoined)
	}

	return transports.IncomingMessage{}, fmt.Errorf("mqtt: take bytes: %w", ErrMQTTNotImplemented)
}

// ReportLiveliness records the generation watermark for one participant.
//
// The first report for a name is accepted. A greater generation
// advances the watermark. A repeated generation with the same
// alive value is idempotent. A repeated generation with a
// different alive value and any smaller generation report
// ErrStaleGeneration. It requires a prior JoinDiscovery and
// reports ErrBackendClosed after Close.
func (b *Backend) ReportLiveliness(ctx context.Context, report transports.LivelinessReport) error {
	err := ctx.Err()
	if err != nil {
		return fmt.Errorf("mqtt: report liveliness: %w", err)
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	if b.closed {
		return fmt.Errorf("mqtt: report liveliness after close: %w", transports.ErrBackendClosed)
	}

	if !b.joined {
		return fmt.Errorf("mqtt: report liveliness before join: %w", transports.ErrNotJoined)
	}

	last, seen := b.lastGen[report.ParticipantName]
	if !seen {
		b.lastGen[report.ParticipantName] = report.Generation
		b.lastAlive[report.ParticipantName] = report.Alive

		return nil
	}

	if report.Generation > last {
		b.lastGen[report.ParticipantName] = report.Generation
		b.lastAlive[report.ParticipantName] = report.Alive

		return nil
	}

	if report.Generation == last && report.Alive == b.lastAlive[report.ParticipantName] {
		return nil
	}

	return fmt.Errorf(
		"mqtt: report liveliness generation %d for %q: %w",
		report.Generation,
		report.ParticipantName,
		transports.ErrStaleGeneration,
	)
}

// Close releases the backend handle.
//
// It is idempotent. The first call marks the backend closed,
// later calls return nil. No broker connection exists yet so
// there is nothing to shut beyond the flag.
func (b *Backend) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.closed = true

	return nil
}
