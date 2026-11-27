// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

package bus

import (
	"fmt"

	"github.com/podomy/weld/internal/transports/dds"
	"github.com/podomy/weld/internal/types"
)

// DDSServiceRole selects the DDS service direction for one call.
//
// It aliases the DDS transport role. The preexisting bus ServiceRole
// aliases the MQTT transport role and is unchanged.
type DDSServiceRole = dds.ServiceRole

// DDS service roles for publish and subscribe paths.
const (
	// DDSServerRole subscribes to requests and publishes replies.
	DDSServerRole = dds.ServiceRoleServer
	// DDSClientRole publishes requests and subscribes to replies.
	DDSClientRole = dds.ServiceRoleClient
)

// DDSMetrics counts dropped inputs for DDS service envelope parsing.
type DDSMetrics = types.Metrics

// ParamRequestTopic maps a service name to its DDS request topic.
func ParamRequestTopic(service string) (string, error) {
	name, err := dds.ParamRequestTopic(service)
	if err != nil {
		return "", fmt.Errorf("bus: dds param request topic: %w", err)
	}

	return name, nil
}

// ParamReplyTopic maps a service name to its DDS reply topic.
func ParamReplyTopic(service string) (string, error) {
	name, err := dds.ParamReplyTopic(service)
	if err != nil {
		return "", fmt.Errorf("bus: dds param reply topic: %w", err)
	}

	return name, nil
}

// MarshalServiceRequest wraps payload with header plus GUID plus sequence.
func MarshalServiceRequest(payload []byte, guid [8]byte, seq uint64, littleEndian bool) ([]byte, error) {
	out, err := dds.MarshalServiceRequest(payload, guid, seq, littleEndian)
	if err != nil {
		return nil, fmt.Errorf("bus: marshal dds service request: %w", err)
	}

	return out, nil
}

// MarshalServiceReply wraps a reply payload the same way as a request.
func MarshalServiceReply(payload []byte, guid [8]byte, seq uint64, littleEndian bool) ([]byte, error) {
	out, err := dds.MarshalServiceReply(payload, guid, seq, littleEndian)
	if err != nil {
		return nil, fmt.Errorf("bus: marshal dds service reply: %w", err)
	}

	return out, nil
}

// UnmarshalServiceRequest validates one request packet and splits the parts.
func UnmarshalServiceRequest(data []byte, m *DDSMetrics) ([]byte, [8]byte, uint64, bool, error) {
	payload, guid, seq, littleEndian, err := dds.UnmarshalServiceRequest(data, m)
	if err != nil {
		return nil, [8]byte{}, 0, false, fmt.Errorf("bus: unmarshal dds service request: %w", err)
	}

	return payload, guid, seq, littleEndian, nil
}

// UnmarshalServiceReply validates one reply packet and splits the parts.
func UnmarshalServiceReply(data []byte, m *DDSMetrics) ([]byte, [8]byte, uint64, bool, error) {
	payload, guid, seq, littleEndian, err := dds.UnmarshalServiceReply(data, m)
	if err != nil {
		return nil, [8]byte{}, 0, false, fmt.Errorf("bus: unmarshal dds service reply: %w", err)
	}

	return payload, guid, seq, littleEndian, nil
}
