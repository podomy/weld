// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

package bus

import (
	"fmt"

	"github.com/podomy/weld/internal/transports/dds"
)

// TopicKind names the reverse mapped kind of a DDS topic.
type TopicKind = dds.TopicKind

// Topic kinds returned by ParseTopicName.
const (
	// TopicKindTopic is a normal topic.
	TopicKindTopic = dds.TopicKindTopic
	// TopicKindRequest is a service request topic.
	TopicKindRequest = dds.TopicKindRequest
	// TopicKindReply is a service reply topic.
	TopicKindReply = dds.TopicKindReply
)

// Topic prefix constants for the DDS sidecar mapping.
const (
	// TopicPrefix carries normal topics such as rt/chatter.
	TopicPrefix = dds.TopicPrefix
	// RequestPrefix carries service requests such as rq/add_two_intsRequest.
	RequestPrefix = dds.RequestPrefix
	// ReplyPrefix carries service replies such as rr/add_two_intsReply.
	ReplyPrefix = dds.ReplyPrefix
)

// DDS type name markers.
const (
	// DDSMarker separates the namespace from the type name.
	DDSMarker = dds.DDSMarker
	// TypeSuffix is the trailing underscore on every DDS type name.
	TypeSuffix = dds.TypeSuffix
)

// Clock abstracts time for discovery leases and announcement ticks.
type Clock = dds.Clock

// SystemClock returns the standard library clock.
func SystemClock() Clock {
	return dds.SystemClock()
}

// NewBackendWithClock validates the config and returns a backend with a clock.
func NewBackendWithClock(cfg DDSConfig, clock Clock) (Backend, error) {
	backend, err := dds.NewBackendWithClock(cfg, clock)
	if err != nil {
		return nil, fmt.Errorf("bus: new dds backend with clock: %w", err)
	}

	return backend, nil
}

// DefaultSidecarConfig returns a DDS config with localhost defaults.
func DefaultSidecarConfig() DDSConfig {
	return dds.DefaultSidecarConfig()
}

// EffectivePeer returns the configured peer or the localhost default.
func EffectivePeer(cfg DDSConfig) string {
	return cfg.EffectivePeer()
}

// TopicName maps a ROS topic name to its DDS topic name.
//
// For example /chatter maps to rt/chatter. When avoidConventions is true
// the bare stripped name is returned with no rt prefix, per the
// avoid_ros_namespace_conventions rule.
func TopicName(rosTopic string, avoidConventions bool) (string, error) {
	name, err := dds.TopicName(rosTopic, avoidConventions)
	if err != nil {
		return "", fmt.Errorf("bus: dds topic name: %w", err)
	}

	return name, nil
}

// RequestTopic maps a ROS service name to its DDS request topic.
//
// For example /add_two_ints maps to rq/add_two_intsRequest. When
// avoidConventions is true the bare stripped name plus Request is
// returned with no rq prefix.
func RequestTopic(serviceName string, avoidConventions bool) (string, error) {
	name, err := dds.RequestTopic(serviceName, avoidConventions)
	if err != nil {
		return "", fmt.Errorf("bus: dds request topic: %w", err)
	}

	return name, nil
}

// ReplyTopic maps a ROS service name to its DDS reply topic.
//
// For example /add_two_ints maps to rr/add_two_intsReply. When
// avoidConventions is true the bare stripped name plus Reply is
// returned with no rr prefix. DDS topics use Reply while IDL response
// types use Response in ServiceResponseTypeName.
func ReplyTopic(serviceName string, avoidConventions bool) (string, error) {
	name, err := dds.ReplyTopic(serviceName, avoidConventions)
	if err != nil {
		return "", fmt.Errorf("bus: dds reply topic: %w", err)
	}

	return name, nil
}

// ParseTopicName maps a DDS topic name back to its ROS name and kind.
//
// It accepts rt, rq, and rr prefixed names plus bare names with no
// prefix. Prefixed requests require the Request suffix and prefixed
// replies require the Reply suffix.
func ParseTopicName(ddsTopic string) (string, TopicKind, error) {
	ros, kind, err := dds.ParseTopicName(ddsTopic)
	if err != nil {
		return "", "", fmt.Errorf("bus: parse dds topic name: %w", err)
	}

	return ros, kind, nil
}

// MessageTypeName maps an IDL message name to its DDS type name.
//
// It maps double underscores to double colons and forms
// namespace::dds_::Name_. For example std_msgs__msg__String maps to
// std_msgs::msg::dds_::String_.
func MessageTypeName(idlName string) (string, error) {
	name, err := dds.MessageTypeName(idlName)
	if err != nil {
		return "", fmt.Errorf("bus: dds message type name: %w", err)
	}

	return name, nil
}

// ServiceRequestTypeName maps an IDL service base name to its request type.
//
// The input is the base name without any Request suffix, for example
// my_pkg__srv__AddTwoInts. The output appends the Request suffix before
// the trailing underscore.
func ServiceRequestTypeName(idlBase string) (string, error) {
	name, err := dds.ServiceRequestTypeName(idlBase)
	if err != nil {
		return "", fmt.Errorf("bus: dds service request type: %w", err)
	}

	return name, nil
}

// ServiceResponseTypeName maps an IDL service base name to its response type.
//
// The input is the base name without any Response suffix, for example
// my_pkg__srv__AddTwoInts. The output appends the Response suffix before
// the trailing underscore. Note the type Response versus topic Reply
// distinction: DDS reply topics use ReplyTopic with Reply while response
// types use Response here.
func ServiceResponseTypeName(idlBase string) (string, error) {
	name, err := dds.ServiceResponseTypeName(idlBase)
	if err != nil {
		return "", fmt.Errorf("bus: dds service response type: %w", err)
	}

	return name, nil
}

// DemangleTypeName reverses a DDS type name back to IDL form.
//
// It requires exactly one ::dds_:: marker. It strips the trailing
// underscore and maps double colons back to double underscores.
func DemangleTypeName(ddsName string) (string, error) {
	name, err := dds.DemangleTypeName(ddsName)
	if err != nil {
		return "", fmt.Errorf("bus: demangle dds type name: %w", err)
	}

	return name, nil
}
