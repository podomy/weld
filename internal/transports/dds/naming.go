// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

// Package dds implements the DDS/RTPS topic and type naming.
package dds

import (
	"errors"
	"fmt"
	"slices"
	"strings"
)

// Topic prefix constants for the DDS topic mapping.
const (
	// TopicPrefix carries normal topics such as rt/chatter.
	TopicPrefix = "rt"
	// RequestPrefix carries service requests such as rq/add_two_intsRequest.
	RequestPrefix = "rq"
	// ReplyPrefix carries service replies such as rr/add_two_intsReply.
	ReplyPrefix = "rr"
	// DefaultPeerAddress is the default peer address.
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
