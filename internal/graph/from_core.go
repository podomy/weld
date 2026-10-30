// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package graph

import (
	"errors"
	"fmt"

	"github.com/podomy/weld/internal/core"
)

// GIDFromCore converts a core GID to the view GID.
func GIDFromCore(gid core.GID) GID {
	var out GID
	copy(out[:], gid[:])
	return out
}

// QoSFromCore converts a core RMW profile to the display mirror.
func QoSFromCore(q core.QoSProfile) QoSProfile {
	return QoSProfile{
		History:     q.History.String(),
		Depth:       q.Depth,
		Reliability: q.Reliability.String(),
		Durability:  q.Durability.String(),
		Liveliness:  q.Liveliness.String(),
	}
}

// EntityFromCore converts one core entity record to the view record.
func EntityFromCore(gid core.GID, info core.EntityInfo) EntityInfo {
	return EntityInfo{
		GID:           GIDFromCore(gid),
		TopicName:     info.TopicName,
		TopicType:     info.TopicType,
		TopicTypeHash: info.TypeHash,
		Participant:   GIDFromCore(info.ParticipantGID),
		QoS:           QoSFromCore(info.QoS),
	}
}

// ServiceFromCore converts one core service record to the view record.
func ServiceFromCore(gid core.GID, info core.ServiceInfo) ServiceInfo {
	return ServiceInfo{
		GID:             GIDFromCore(gid),
		ServiceName:     info.ServiceName,
		ServiceType:     info.ServiceType,
		ServiceTypeHash: info.ServiceTypeHash,
		Participant:     GIDFromCore(info.ParticipantGID),
		QoS:             ServicesDefaultQoS(),
	}
}

// ParticipantFromCore converts one core participant record to the view record.
func ParticipantFromCore(gid core.GID, info core.ParticipantInfo) ParticipantInfo {
	return ParticipantInfo{
		GID:     GIDFromCore(gid),
		Enclave: info.Enclave,
		Nodes: []NodeRef{
			{Name: info.NodeName, Namespace: info.NodeNamespace, Enclave: info.Enclave},
		},
	}
}

// IndexFromCore builds a view index from the core write path index.
func IndexFromCore(g *core.Graph) (*Index, error) {
	if g == nil {
		return nil, errors.New("graph: core graph is nil") //nolint:wrapcheck // new error, nothing to wrap
	}
	idx := NewIndex()
	for gid, info := range g.Writers() {
		converted := EntityFromCore(gid, info)
		err := idx.UpsertWriter(converted)
		if err != nil {
			return nil, fmt.Errorf("graph: convert writer %s: %w", gid.String(), err)
		}
	}
	for gid, info := range g.Readers() {
		converted := EntityFromCore(gid, info)
		err := idx.UpsertReader(converted)
		if err != nil {
			return nil, fmt.Errorf("graph: convert reader %s: %w", gid.String(), err)
		}
	}
	for gid, info := range g.Services() {
		converted := ServiceFromCore(gid, info)
		err := idx.UpsertService(converted)
		if err != nil {
			return nil, fmt.Errorf("graph: convert service %s: %w", gid.String(), err)
		}
	}
	for gid, info := range g.Participants() {
		converted := ParticipantFromCore(gid, info)
		err := idx.UpsertParticipant(converted)
		if err != nil {
			return nil, fmt.Errorf("graph: convert participant %s: %w", gid.String(), err)
		}
	}
	idx.mu.Lock()
	idx.generation = g.Generation()
	idx.mu.Unlock()
	return idx, nil
}

// SnapshotFromCore builds a tool snapshot directly from the core index.
func SnapshotFromCore(g *core.Graph) (Snapshot, error) {
	idx, err := IndexFromCore(g)
	if err != nil {
		return Snapshot{}, fmt.Errorf("graph: snapshot from core: %w", err)
	}
	return idx.Snapshot(), nil
}
