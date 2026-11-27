// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

// Package graph is the public read snapshot view for tools.
package graph

import (
	"fmt"

	"github.com/podomy/weld/internal/core"
	igraph "github.com/podomy/weld/internal/graph"
)

// Index is the local read view backing tool snapshots.
type Index = igraph.Index

// Snapshot is a partition local copy of the graph index for tools.
type Snapshot = igraph.Snapshot

// GID is the 16 byte global id for one endpoint or participant.
type GID = igraph.GID

// NodeRef names one ROS node instance inside a participant.
type NodeRef = igraph.NodeRef

// EntityInfo is one writer or reader record.
type EntityInfo = igraph.EntityInfo

// ServiceInfo is one service record.
type ServiceInfo = igraph.ServiceInfo

// ParticipantInfo is one participant record.
type ParticipantInfo = igraph.ParticipantInfo

// TopicEndpointInfo is entity info by topic for tools.
type TopicEndpointInfo = igraph.TopicEndpointInfo

// ServiceEndpointInfo is entity info by service for tools.
type ServiceEndpointInfo = igraph.ServiceEndpointInfo

// NameAndType pairs one topic or service name with its merged type list.
type NameAndType = igraph.NameAndType

// NodeCount pairs one node with its endpoint count.
type NodeCount = igraph.NodeCount

// QoSProfile is the endpoint QoS carried in the index for tools.
type QoSProfile = igraph.QoSProfile

// CoreGraph is the write path index from internal core.
//
// It is identical to node.Graph. External callers name the type through
// this alias or node.Graph and obtain a value from node.NewGraph, so no
// internal import is required to call IndexFromCore or SnapshotFromCore.
type CoreGraph = core.Graph

// CoreGID is the 16 byte core identity for one endpoint or participant.
//
// It is identical to node.GID. The view GID is a byte copy of this value.
type CoreGID = core.GID

// CoreEntityInfo is one core writer or reader record.
type CoreEntityInfo = core.EntityInfo

// CoreServiceInfo is one core service record.
type CoreServiceInfo = core.ServiceInfo

// CoreParticipantInfo is one core participant record.
type CoreParticipantInfo = core.ParticipantInfo

// CoreQoSProfile is the core RMW QoS profile.
type CoreQoSProfile = core.QoSProfile

// Sentinel node names for foreign endpoints without participant records.
const (
	// UnknownNodeName marks endpoints with no participant record.
	UnknownNodeName = igraph.UnknownNodeName
	// UnknownNodeNamespace marks endpoints with no participant record.
	UnknownNodeNamespace = igraph.UnknownNodeNamespace
)

// NewIndex returns an empty index with unpresized tables.
func NewIndex() *Index {
	return igraph.NewIndex()
}

// NewIndexWithCapacity returns an empty index with presized tables.
//
// There is no upper bound on capacities; the caller owns sizing for the
// expected topology. The lower bound is validated inside: zero or
// negative capacities fall back to unpresized tables.
func NewIndexWithCapacity(writers, readers, services, participants int) *Index {
	return igraph.NewIndexWithCapacity(writers, readers, services, participants)
}

// IndexFromCore builds a view index from the core write path index.
func IndexFromCore(g *CoreGraph) (*Index, error) {
	idx, err := igraph.IndexFromCore(g)
	if err != nil {
		return nil, fmt.Errorf("graph: index from core: %w", err)
	}

	return idx, nil
}

// SnapshotFromCore builds a tool snapshot directly from the core index.
func SnapshotFromCore(g *CoreGraph) (Snapshot, error) {
	snap, err := igraph.SnapshotFromCore(g)
	if err != nil {
		return Snapshot{}, fmt.Errorf("graph: snapshot from core: %w", err)
	}

	return snap, nil
}

// GIDFromCore converts a core GID to the view GID.
func GIDFromCore(gid CoreGID) GID {
	return igraph.GIDFromCore(gid)
}

// GIDFromString parses 32 hex digits into a GID.
func GIDFromString(s string) (GID, error) {
	gid, err := igraph.GIDFromString(s)
	if err != nil {
		return GID{}, fmt.Errorf("graph: gid from string: %w", err)
	}

	return gid, nil
}

// QoSFromCore converts a core RMW profile to the display mirror.
func QoSFromCore(q CoreQoSProfile) QoSProfile {
	return igraph.QoSFromCore(q)
}

// EntityFromCore converts one core entity record to the view record.
func EntityFromCore(gid CoreGID, info CoreEntityInfo) EntityInfo {
	return igraph.EntityFromCore(gid, info)
}

// ServiceFromCore converts one core service record to the view record.
func ServiceFromCore(gid CoreGID, info CoreServiceInfo) ServiceInfo {
	return igraph.ServiceFromCore(gid, info)
}

// ParticipantFromCore converts one core participant record to the view record.
func ParticipantFromCore(gid CoreGID, info CoreParticipantInfo) ParticipantInfo {
	return igraph.ParticipantFromCore(gid, info)
}

// DefaultQoS returns the standard tool default profile.
func DefaultQoS() QoSProfile {
	return igraph.DefaultQoS()
}

// ServicesDefaultQoS returns the standard service default profile.
func ServicesDefaultQoS() QoSProfile {
	return igraph.ServicesDefaultQoS()
}

// IsHiddenNode reports whether the node name uses the hidden prefix.
func IsHiddenNode(name string) bool {
	return igraph.IsHiddenNode(name)
}

// IsHiddenService reports whether the service name uses the hidden prefix.
func IsHiddenService(name string) bool {
	return igraph.IsHiddenService(name)
}

// IsHiddenTopic reports whether the topic name uses the hidden prefix.
func IsHiddenTopic(name string) bool {
	return igraph.IsHiddenTopic(name)
}
