// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

package graph_test

import (
	"testing"

	"github.com/podomy/weld/graph"
)

// testGID returns a deterministic GID with the last byte set to seed.
func testGID(seed byte) graph.GID {
	var out graph.GID
	out[0] = 0xAA
	out[15] = seed

	return out
}

// mustUpsertParticipant inserts a participant or fails the test.
func mustUpsertParticipant(t *testing.T, idx *graph.Index, info graph.ParticipantInfo) {
	t.Helper()

	err := idx.UpsertParticipant(info)
	if err != nil {
		t.Fatalf("upsert participant: %v", err)
	}
}

// mustUpsertWriter inserts a writer or fails the test.
func mustUpsertWriter(t *testing.T, idx *graph.Index, info graph.EntityInfo) {
	t.Helper()

	err := idx.UpsertWriter(info)
	if err != nil {
		t.Fatalf("upsert writer: %v", err)
	}
}

// mustUpsertReader inserts a reader or fails the test.
func mustUpsertReader(t *testing.T, idx *graph.Index, info graph.EntityInfo) {
	t.Helper()

	err := idx.UpsertReader(info)
	if err != nil {
		t.Fatalf("upsert reader: %v", err)
	}
}

// TestIndexUpsertSnapshot checks upsert plus snapshot counts.
func TestIndexUpsertSnapshot(t *testing.T) {
	idx := graph.NewIndex()
	partA := testGID(0x01)
	partB := testGID(0x02)
	mustUpsertParticipant(t, idx, graph.ParticipantInfo{
		GID:     partA,
		Enclave: "/",
		Nodes:   []graph.NodeRef{{Name: "talker", Namespace: "/", Enclave: "/"}},
	})
	mustUpsertParticipant(t, idx, graph.ParticipantInfo{
		GID:     partB,
		Enclave: "/",
		Nodes:   []graph.NodeRef{{Name: "listener", Namespace: "/ns", Enclave: "/"}},
	})
	mustUpsertWriter(t, idx, graph.EntityInfo{
		GID:         testGID(0x10),
		TopicName:   "/chatter",
		TopicType:   "std_msgs/msg/String",
		Participant: partA,
		QoS:         graph.QoSProfile{History: "keep_last", Depth: 10, Reliability: "reliable", Durability: "volatile", Liveliness: "automatic"},
	})
	mustUpsertReader(t, idx, graph.EntityInfo{
		GID:         testGID(0x11),
		TopicName:   "/chatter",
		TopicType:   "std_msgs/msg/String",
		Participant: partB,
		QoS:         graph.QoSProfile{History: "keep_last", Depth: 10, Reliability: "reliable", Durability: "volatile", Liveliness: "automatic"},
	})
	snap := idx.Snapshot()
	if got := snap.PublisherCount("/chatter"); got != 1 {
		t.Fatalf("publisher count = %d, want 1", got)
	}
	if got := snap.SubscriptionCount("/chatter"); got != 1 {
		t.Fatalf("subscription count = %d, want 1", got)
	}
	topics := snap.TopicNamesAndTypes(false)
	if len(topics) != 1 || topics[0].Name != "/chatter" {
		t.Fatalf("topics = %+v, want one /chatter", topics)
	}
	names := snap.NodeNames(false)
	if len(names) != 2 {
		t.Fatalf("node names len = %d, want 2", len(names))
	}
}
