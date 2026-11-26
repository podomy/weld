// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

package mqtt

import (
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/podomy/weld/internal/transports"
)

// DefaultTopicPrefix is the MQTT root used when no prefix is configured.
const DefaultTopicPrefix = "weld"

// Version selects the MQTT protocol version on the wire.
//
// Version5 is the default and the only version carrying user
// properties such as the type hash. Version311 speaks the older
// framing for brokers and embedded stacks without v5: no properties,
// so takes report an empty type hash and matching degrades to the
// hashless path. The zero value is Version5.
type Version int

const (
	// Version5 speaks MQTT v5.0 with user properties.
	Version5 Version = iota
	// Version311 speaks MQTT 3.1.1 without properties.
	Version311
)

// String returns the dotted version name for logs.
func (v Version) String() string {
	switch v {
	case Version311:
		return "3.1.1"
	case Version5:
		return "5.0"
	default:
		return "unknown"
	}
}

// maxQoS is the largest valid MQTT quality of service level.
//
// QoS 0 is at most once, QoS 1 is at least once with PUBACK, QoS 2 is
// exactly once through the PUBREC plus PUBREL plus PUBCOMP handshake
// proven in proof/qos2.tla. Subscribing, publishing, and the will all accept
// 0 through 2.
const maxQoS = 2

// MqttTLS carries TLS settings for mqtts sessions.
//
// Empty means plain TCP. CertFile plus KeyFile must be set together;
// a lone file is rejected. Keys never appear in errors.
type MqttTLS struct {
	// CAFile is an optional PEM bundle of trusted broker CAs.
	// Empty uses the host trust store.
	CAFile string
	// CertFile holds the optional client certificate.
	CertFile string
	// KeyFile holds the optional client private key.
	KeyFile string
	// ServerName overrides the TLS server name. Empty uses the broker host.
	ServerName string
}

// MqttWill carries the last will for ungraceful disconnects.
//
// The broker publishes the will when the session drops without a
// DISCONNECT, which keeps liveliness honest across kills. Empty Topic
// disables the will.
//
// The will carries no generation, so it cannot be ordered against
// live discovery generations: a generation-less will never overwrites
// a generation-carrying record in the endpoint cache. Liveliness from
// the will works only when the will topic is a discovery topic with a
// valid endpoint-record payload, and then only as first sighting.
// Prefer explicit RetractEndpoint for clean shutdowns. A retained
// will with an empty payload deletes retained state per spec instead
// of leaving a tombstone, so do not configure one.
type MqttWill struct {
	// Topic is the will topic. It must not carry wildcards.
	Topic string
	// Payload is the will body. Nil means empty.
	Payload []byte
	// QoS is the will quality of service, 0 through 2.
	QoS byte
	// Retained selects a retained will.
	Retained bool
}

// DefaultKeepAliveSecs paces PINGREQ when the link is idle.
const DefaultKeepAliveSecs = 30

// MqttConfig carries the settings for one MQTT backend.
type MqttConfig struct {
	// BrokerURL names the broker to dial. Empty means in-memory only.
	BrokerURL string
	// ClientID is the MQTT client identifier. Empty means auto assigned.
	ClientID string
	// TopicPrefix is the MQTT root for all weld topics. Empty means weld.
	TopicPrefix string
	// QoS is the MQTT quality of service level, 0 through 2.
	QoS byte
	// Retained selects retained publishes when true.
	Retained bool
	// DomainID is the caller supplied DDS domain id before env override.
	// ROS_DOMAIN_ID overrides it at resolve time; Validate itself
	// never reads the environment.
	DomainID uint32
	// Username is the MQTT username. Empty means anonymous.
	Username string
	// Password is the MQTT password. It never appears in errors.
	Password string
	// TLS carries TLS settings. Empty means plain TCP, and TLS is
	// only used when the broker URL scheme selects it (tls, mqtts,
	// or ssl); otherwise the settings are ignored.
	TLS MqttTLS
	// Will carries the last will. Empty topic disables it, and then
	// payload, QoS, and retained must also be empty; anything else is
	// rejected at validation.
	Will MqttWill
	// KeepAliveSecs paces keepalive. Zero means the default of 30.
	KeepAliveSecs uint16
	// Version selects the wire protocol. Zero means Version5.
	Version Version
}

// NormalizePrefix trims surrounding space and slashes from a prefix.
//
// Empty input returns DefaultTopicPrefix. The result never has leading or
// trailing slashes. It keeps inner slashes so hierarchical roots stay valid.
func NormalizePrefix(prefix string) string {
	trimmed := strings.TrimSpace(prefix)
	trimmed = strings.Trim(trimmed, "/")
	if trimmed == "" {
		return DefaultTopicPrefix
	}

	return trimmed
}

// NormalizedPrefix returns the effective topic prefix for the config.
func (c MqttConfig) NormalizedPrefix() string {
	return NormalizePrefix(c.TopicPrefix)
}

// ResolveDomain returns the actual domain id for the config.
//
// It delegates to transports.ResolveDomainID so ROS_DOMAIN_ID overrides the
// caller value the same way as the core transports.
func (c MqttConfig) ResolveDomain() (uint32, error) {
	id, err := transports.ResolveDomainID(c.DomainID)
	if err != nil {
		return 0, fmt.Errorf("mqtt: resolve domain: %w", err)
	}

	return id, nil
}

// Validate checks the config without dialing a broker and without
// reading the environment (ROS_DOMAIN_ID applies later, at resolve).
//
// Whitespace is rejected in broker URLs, client IDs, usernames, and
// will topics as weld policy: MQTT itself allows spaces in several of
// these, but ROS names never contain them and the topic map splits on
// plain levels, so weld refuses them early instead of failing at the
// codec or the broker. Length and UTF-8 shape are enforced at encode
// time, not here; the codec is the second layer (Dial always validates
// first).
func (c MqttConfig) Validate() error {
	if c.QoS > maxQoS {
		return fmt.Errorf("mqtt: qos %d above maximum %d", c.QoS, maxQoS)
	}

	switch c.Version {
	case Version5, Version311:
	default:
		return fmt.Errorf("mqtt: version %d unknown, want 5.0 or 3.1.1", int(c.Version))
	}

	if c.DomainID > transports.MaxDomainID {
		return fmt.Errorf("mqtt: domain id %d above maximum %d", c.DomainID, transports.MaxDomainID)
	}

	err := checkNoWhitespace("broker url", c.BrokerURL)
	if err != nil {
		return err
	}

	err = checkNoWhitespace("client id", c.ClientID)
	if err != nil {
		return err
	}

	err = checkPrefix(NormalizePrefix(c.TopicPrefix))
	if err != nil {
		return err
	}

	err = checkCredentials(c.Username, c.Password)
	if err != nil {
		return err
	}

	err = checkTLS(c.TLS)
	if err != nil {
		return err
	}

	err = checkWill(c.Will)
	if err != nil {
		return err
	}

	return nil
}

// checkCredentials validates the username plus password pair.
func checkCredentials(username, password string) error {
	err := checkNoWhitespace("username", username)
	if err != nil {
		return err
	}

	if password != "" && username == "" {
		return errors.New("mqtt: password without username") //nolint:wrapcheck // new error, nothing to wrap
	}

	return nil
}

// checkTLS validates the TLS file pair.
func checkTLS(tls MqttTLS) error {
	if (tls.CertFile == "") != (tls.KeyFile == "") {
		return errors.New("mqtt: tls cert and key must be set together") //nolint:wrapcheck // new error, nothing to wrap
	}

	return nil
}

// checkWill validates one last will without dialing.
//
// Empty topic disables the will (payload, QoS, and retained must then
// be empty too). A set topic must not carry wildcards, whitespace,
// invalid UTF-8, NUL, or a `$` prefix (broker namespace), and QoS
// stays within maxQoS.
func checkWill(will MqttWill) error {
	if will.Topic == "" {
		if len(will.Payload) > 0 || will.QoS != 0 || will.Retained {
			return errors.New("mqtt: will payload without topic") //nolint:wrapcheck // new error, nothing to wrap
		}

		return nil
	}

	if strings.Contains(will.Topic, "+") {
		return errors.New("mqtt: will topic carries plus wildcard") //nolint:wrapcheck // new error, nothing to wrap
	}

	if strings.Contains(will.Topic, "#") {
		return errors.New("mqtt: will topic carries hash wildcard") //nolint:wrapcheck // new error, nothing to wrap
	}

	if strings.ContainsAny(will.Topic, " \t\n\r") {
		return errors.New("mqtt: will topic carries whitespace") //nolint:wrapcheck // new error, nothing to wrap
	}

	if !utf8.ValidString(will.Topic) {
		return errors.New("mqtt: will topic carries invalid utf-8") //nolint:wrapcheck // new error, nothing to wrap
	}

	if strings.ContainsRune(will.Topic, 0) {
		return errors.New("mqtt: will topic carries null character") //nolint:wrapcheck // new error, nothing to wrap
	}

	if strings.HasPrefix(will.Topic, "$") {
		return errors.New("mqtt: will topic collides with broker $ namespace") //nolint:wrapcheck // new error, nothing to wrap
	}

	if will.QoS > maxQoS {
		return fmt.Errorf("mqtt: will qos %d above maximum %d", will.QoS, maxQoS)
	}

	return nil
}

// checkNoWhitespace rejects embedded whitespace in a free form field.
//
// Empty input is valid so in-memory mode needs no broker url or client id.
func checkNoWhitespace(field string, value string) error {
	if value == "" {
		return nil
	}

	if strings.ContainsAny(value, " \t\n\r") {
		return fmt.Errorf("mqtt: %s carries whitespace", field)
	}

	return nil
}

// checkPrefix rejects wildcards, whitespace, empty levels, empty
// input, and `$` prefixes in a topic prefix.
//
// The input must already be normalized. Wildcards plus and hash are MQTT
// subscription syntax and never valid in a concrete publish prefix.
// Whitespace is legal MQTT but banned here because ROS names never
// contain it and the weld topic map splits on plain levels. `$` is
// banned because the broker reserves it.
func checkPrefix(prefix string) error {
	if prefix == "" {
		return errors.New("mqtt: topic prefix is empty") //nolint:wrapcheck // new error, nothing to wrap
	}

	if strings.Contains(prefix, "+") {
		return errors.New("mqtt: topic prefix carries plus wildcard") //nolint:wrapcheck // new error, nothing to wrap
	}

	if strings.Contains(prefix, "#") {
		return errors.New("mqtt: topic prefix carries hash wildcard") //nolint:wrapcheck // new error, nothing to wrap
	}

	if strings.ContainsAny(prefix, " \t\n\r") {
		return errors.New("mqtt: topic prefix carries whitespace") //nolint:wrapcheck // new error, nothing to wrap
	}

	if strings.Contains(prefix, "//") {
		return errors.New("mqtt: topic prefix carries empty level") //nolint:wrapcheck // new error, nothing to wrap
	}

	if strings.HasPrefix(prefix, "$") {
		return errors.New("mqtt: topic prefix collides with broker $ namespace") //nolint:wrapcheck // new error, nothing to wrap
	}

	return nil
}
