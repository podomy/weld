// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

package mqtt

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/podomy/weld/internal/transports"
)

// Kind names the ROS traffic class carried on one MQTT topic.
type Kind string

// MQTT traffic classes for data, request, reply, and discovery paths.
const (
	// KindData carries plain pubsub samples on the rt leg.
	KindData Kind = "rt"
	// KindRequest carries service or action requests on the rq leg.
	KindRequest Kind = "rq"
	// KindReply carries service or action replies on the rr leg.
	KindReply Kind = "rr"
	// KindDiscovery carries retained endpoint announcements on the sdp leg.
	KindDiscovery Kind = "sdp"
)

// isValidKind reports whether k is one of the known traffic classes.
func isValidKind(k Kind) bool {
	return k == KindData || k == KindRequest || k == KindReply || k == KindDiscovery
}

// isDataKind reports whether k carries samples for the TakeBytes path.
func isDataKind(k Kind) bool {
	return k == KindData || k == KindRequest || k == KindReply
}

// EncodeTopic maps one ROS topic to its MQTT wire form.
//
// The form is prefix/domain/kind/stripped where stripped is the ROS name
// without its leading slash. The root ROS topic slash encodes without a
// trailing stripped level. The function rejects MQTT wildcards plus and
// hash and any whitespace in either input.
func EncodeTopic(prefix string, domain uint32, kind Kind, rosTopic string) (string, error) {
	normalized := NormalizePrefix(prefix)
	err := checkPrefix(normalized)
	if err != nil {
		return "", err
	}

	if domain > transports.MaxDomainID {
		return "", fmt.Errorf("mqtt: domain id %d above maximum %d", domain, transports.MaxDomainID)
	}

	if !isDataKind(kind) {
		return "", fmt.Errorf("mqtt: unknown data kind %q", string(kind))
	}

	stripped, err := stripROSTopic(rosTopic)
	if err != nil {
		return "", err
	}

	domainText := strconv.FormatUint(uint64(domain), 10)

	if stripped == "" {
		return normalized + "/" + domainText + "/" + string(kind), nil
	}

	return normalized + "/" + domainText + "/" + string(kind) + "/" + stripped, nil
}

// DecodeTopic parses one MQTT wire topic back to domain, kind, and ROS name.
//
// The prefix argument is normalized the same way as EncodeTopic so empty
// means the default weld root. The result round trips through EncodeTopic.
// Wildcards, whitespace, bad domains, unknown kinds, and prefix mismatches
// all return errors.
func DecodeTopic(prefix string, mqttTopic string) (uint32, Kind, string, error) {
	rest, err := splitTopicPrefix(prefix, mqttTopic, 2)
	if err != nil {
		return 0, "", "", err
	}

	domain, err := parseDomainPart(rest[0])
	if err != nil {
		return 0, "", "", err
	}

	kind := Kind(rest[1])
	if !isValidKind(kind) {
		return 0, "", "", fmt.Errorf("mqtt: unknown kind %q", string(kind))
	}

	rosTopic, err := joinROSTopic(mqttTopic, rest[2:])
	if err != nil {
		return 0, "", "", err
	}

	return domain, kind, rosTopic, nil
}

// splitTopicPrefix validates one wire topic and strips its prefix.
//
// It rejects wildcards plus whitespace, requires at least want levels
// past the normalized prefix, and returns the remainder levels.
func splitTopicPrefix(prefix, mqttTopic string, want int) ([]string, error) {
	normalized := NormalizePrefix(prefix)
	err := checkPrefix(normalized)
	if err != nil {
		return nil, err
	}

	if strings.Contains(mqttTopic, "+") {
		return nil, fmt.Errorf("mqtt: topic %q carries plus wildcard", mqttTopic)
	}

	if strings.Contains(mqttTopic, "#") {
		return nil, fmt.Errorf("mqtt: topic %q carries hash wildcard", mqttTopic)
	}

	if !utf8.ValidString(mqttTopic) {
		return nil, fmt.Errorf("mqtt: topic %q carries invalid utf-8", mqttTopic)
	}

	if strings.ContainsRune(mqttTopic, 0) {
		return nil, fmt.Errorf("mqtt: topic %q carries null character", mqttTopic)
	}

	if strings.ContainsAny(mqttTopic, " \t\n\r") {
		return nil, fmt.Errorf("mqtt: topic %q carries whitespace", mqttTopic)
	}

	prefixParts := strings.Split(normalized, "/")
	topicParts := strings.Split(mqttTopic, "/")

	if len(topicParts) < len(prefixParts)+want {
		return nil, fmt.Errorf("mqtt: topic %q is shorter than prefix plus %d levels", mqttTopic, want)
	}

	for i, part := range prefixParts {
		if topicParts[i] != part {
			return nil, fmt.Errorf("mqtt: topic %q misses prefix %q", mqttTopic, normalized)
		}
	}

	return topicParts[len(prefixParts):], nil
}

// joinROSTopic validates trailing levels and joins one ROS name.
//
// Empty input maps to the root name slash. Empty levels are rejected
// before joining so no double slash survives.
func joinROSTopic(mqttTopic string, rest []string) (string, error) {
	for _, level := range rest {
		if level == "" {
			return "", fmt.Errorf("mqtt: topic %q carries empty level", mqttTopic)
		}

		if !utf8.ValidString(level) {
			return "", fmt.Errorf("mqtt: topic %q carries invalid utf-8", mqttTopic)
		}

		if strings.ContainsRune(level, 0) {
			return "", fmt.Errorf("mqtt: topic %q carries null character", mqttTopic)
		}

		if strings.ContainsAny(level, " \t\n\r") {
			return "", fmt.Errorf("mqtt: topic %q carries whitespace", mqttTopic)
		}
	}

	if len(rest) == 0 {
		return "/", nil
	}

	joined := strings.Join(rest, "/")
	err := checkStrippedPath(joined)
	if err != nil {
		return "", err
	}

	return "/" + joined, nil
}

// stripROSTopic validates a ROS name and returns it without leading slash.
//
// The root name slash maps to empty. All other names must start with slash,
// carry no whitespace or wildcards, and contain no empty levels.
func stripROSTopic(rosTopic string) (string, error) {
	if rosTopic == "" {
		return "", errors.New("mqtt: ros topic is empty") //nolint:wrapcheck // new error, nothing to wrap
	}

	if !utf8.ValidString(rosTopic) {
		return "", errors.New("mqtt: ros topic carries invalid utf-8") //nolint:wrapcheck // new error, nothing to wrap
	}

	if strings.ContainsRune(rosTopic, 0) {
		return "", errors.New("mqtt: ros topic carries null character") //nolint:wrapcheck // new error, nothing to wrap
	}

	if strings.Contains(rosTopic, "+") {
		return "", fmt.Errorf("mqtt: ros topic %q carries plus wildcard", rosTopic)
	}

	if strings.Contains(rosTopic, "#") {
		return "", fmt.Errorf("mqtt: ros topic %q carries hash wildcard", rosTopic)
	}

	if strings.ContainsAny(rosTopic, " \t\n\r") {
		return "", fmt.Errorf("mqtt: ros topic %q carries whitespace", rosTopic)
	}

	if rosTopic == "/" {
		return "", nil
	}

	if !strings.HasPrefix(rosTopic, "/") {
		return "", fmt.Errorf("mqtt: ros topic %q misses leading slash", rosTopic)
	}

	stripped := strings.TrimPrefix(rosTopic, "/")
	err := checkStrippedPath(stripped)
	if err != nil {
		return "", err
	}

	return stripped, nil
}

// checkStrippedPath rejects empty levels in a slash separated path.
func checkStrippedPath(stripped string) error {
	if stripped == "" {
		return errors.New("mqtt: ros path is empty") //nolint:wrapcheck // new error, nothing to wrap
	}

	if strings.Contains(stripped, "//") {
		return fmt.Errorf("mqtt: ros path %q carries empty level", stripped)
	}

	if strings.HasSuffix(stripped, "/") {
		return fmt.Errorf("mqtt: ros path %q carries trailing slash", stripped)
	}

	return nil
}

// checkParticipant validates one discovery participant name.
//
// Participants are single MQTT levels: no slashes, wildcards, or
// whitespace. Empty names are rejected so announcements always name
// their owner.
func checkParticipant(participant string) error {
	if participant == "" {
		return errors.New("mqtt: participant name is empty") //nolint:wrapcheck // new error, nothing to wrap
	}

	if strings.ContainsAny(participant, "/ \t\n\r") {
		return fmt.Errorf("mqtt: participant %q carries separator", participant)
	}

	if strings.Contains(participant, "+") {
		return fmt.Errorf("mqtt: participant %q carries plus wildcard", participant)
	}

	if strings.Contains(participant, "#") {
		return fmt.Errorf("mqtt: participant %q carries hash wildcard", participant)
	}

	if !utf8.ValidString(participant) {
		return fmt.Errorf("mqtt: participant %q carries invalid utf-8", participant)
	}

	if strings.ContainsRune(participant, 0) {
		return fmt.Errorf("mqtt: participant %q carries null character", participant)
	}

	return nil
}

// EncodeDiscoveryTopic maps one endpoint to its retained discovery topic.
//
// The form is prefix/domain/sdp/<participant>/<kind>/<stripped> where
// kind is rt, rq, or rr and stripped is the ROS name without its
// leading slash. Discovery topics never collide with data topics
// because the sdp leg sits between the domain and the participant.
func EncodeDiscoveryTopic(prefix string, domain uint32, participant string, kind Kind, rosTopic string) (string, error) {
	normalized := NormalizePrefix(prefix)
	err := checkPrefix(normalized)
	if err != nil {
		return "", err
	}

	if domain > transports.MaxDomainID {
		return "", fmt.Errorf("mqtt: domain id %d above maximum %d", domain, transports.MaxDomainID)
	}

	err = checkParticipant(participant)
	if err != nil {
		return "", err
	}

	if !isDataKind(kind) {
		return "", fmt.Errorf("mqtt: unknown discovery kind %q", string(kind))
	}

	stripped, err := stripROSTopic(rosTopic)
	if err != nil {
		return "", err
	}

	domainText := strconv.FormatUint(uint64(domain), 10)
	base := normalized + "/" + domainText + "/" + string(KindDiscovery) + "/" + participant + "/" + string(kind)

	if stripped == "" {
		return base, nil
	}

	return base + "/" + stripped, nil
}

// DecodeDiscoveryTopic parses one retained discovery topic.
func DecodeDiscoveryTopic(prefix string, mqttTopic string) (uint32, string, Kind, string, error) {
	rest, err := splitTopicPrefix(prefix, mqttTopic, 4)
	if err != nil {
		return 0, "", "", "", err
	}

	domain, err := parseDomainPart(rest[0])
	if err != nil {
		return 0, "", "", "", err
	}

	if rest[1] != string(KindDiscovery) {
		return 0, "", "", "", fmt.Errorf("mqtt: topic %q is not a discovery topic", mqttTopic)
	}

	participant := rest[2]
	err = checkParticipant(participant)
	if err != nil {
		return 0, "", "", "", err
	}

	kind := Kind(rest[3])
	if !isDataKind(kind) {
		return 0, "", "", "", fmt.Errorf("mqtt: unknown discovery kind %q", string(kind))
	}

	rosTopic, err := joinROSTopic(mqttTopic, rest[4:])
	if err != nil {
		return 0, "", "", "", err
	}

	return domain, participant, kind, rosTopic, nil
}

// formatDomain renders one decimal domain level.
func formatDomain(domain uint32) string {
	return strconv.FormatUint(uint64(domain), 10)
}

// parseDomainPart parses one decimal domain level.
func parseDomainPart(text string) (uint32, error) {
	parsed, err := strconv.ParseUint(text, 10, 32)
	if err != nil {
		return 0, fmt.Errorf("mqtt: parse domain %q: %w", text, err)
	}

	domain := uint32(parsed)
	if domain > transports.MaxDomainID {
		return 0, fmt.Errorf("mqtt: domain id %d above maximum %d", domain, transports.MaxDomainID)
	}

	return domain, nil
}
