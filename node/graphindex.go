// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

package node

import (
	"fmt"

	"github.com/podomy/weld/internal/core"
)

// Graph is the partition local discovery index. Writers maps endpoint GID to
// entity info for publishers, readers for subscriptions, services for service
// endpoints. Generation counts applied mutations. Do not copy after first use.
type Graph = core.Graph

// GID identifies one endpoint or participant. It holds the 16 byte RMW GID.
// The zero value means unset.
type GID = core.GID

// EntityInfo is one writer or reader record in the index: demangled topic
// name and type, owning participant link, and translated RMW profile.
type EntityInfo = core.EntityInfo

// ServiceInfo is one service endpoint record in the index: demangled service
// name and type plus owning participant link.
type ServiceInfo = core.ServiceInfo

// NewGraph returns an empty partition local index.
func NewGraph() *Graph {
	return core.NewGraph()
}

// ParticipantInfo is one participant record in the index. See core.ParticipantInfo.
type ParticipantInfo = core.ParticipantInfo

// GIDFromBytes copies exactly 16 bytes into a GID.
func GIDFromBytes(b []byte) (GID, error) {
	gid, err := core.GIDFromBytes(b)
	if err != nil {
		return GID{}, fmt.Errorf("node: gid from bytes: %w", err)
	}
	return gid, nil
}

// ParseGID parses 32 hex digits into a GID.
func ParseGID(s string) (GID, error) {
	gid, err := core.ParseGID(s)
	if err != nil {
		return GID{}, fmt.Errorf("node: parse gid: %w", err)
	}
	return gid, nil
}

// DemangleTopicName converts a DDS topic name to its ROS form.
func DemangleTopicName(ddsTopic string) (string, error) {
	name, err := core.DemangleTopicName(ddsTopic)
	if err != nil {
		return "", fmt.Errorf("node: demangle topic name: %w", err)
	}
	return name, nil
}

// DemangleTypeName converts a DDS type name to its IDL form.
func DemangleTypeName(ddsName string) (string, error) {
	name, err := core.DemangleTypeName(ddsName)
	if err != nil {
		return "", fmt.Errorf("node: demangle type name: %w", err)
	}
	return name, nil
}
