// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

package rmwbridge_test

import (
	"testing"

	"github.com/podomy/weld/internal/rmwbridge"
)

// checkEventNodeNames pins local node names resolving with namespaces.
//
// Local weld nodes carry real names from the bridge registry keyed by
// name; namespaces survive the round trip instead of collapsing to
// empty.
func checkEventNodeNames(t *testing.T) {
	t.Helper()
	bridge := rmwbridge.NewBridge()
	defer func() {
		closeBridgeChecked(t, bridge)
	}()
	node, err := bridge.CreateNode("talker")
	requireNoError(t, err, "create node")
	if node.Name != "talker" {
		t.Fatalf("node name = %q, want talker", node.Name)
	}
	if node.Namespace != "/" {
		t.Fatalf("node namespace = %q, want /", node.Namespace)
	}
	_, err = bridge.CreateNodeWithNamespace("listener", "/ns")
	requireNoError(t, err, "create node with namespace")
	names := bridge.NodeNames()
	byName := make(map[string]string, len(names))
	for _, info := range names {
		byName[info.Name] = info.Namespace
	}
	if byName["talker"] != "/" {
		t.Fatalf("talker namespace = %q, want /", byName["talker"])
	}
	if byName["listener"] != "/ns" {
		t.Fatalf("listener namespace = %q, want /ns", byName["listener"])
	}
}

// checkEventQoSPassthrough pins stored QoS matching the announcement.
//
// The announcer is weld itself here: ActualQoS returns exactly what the
// endpoint was created with instead of a zero-filled default.
func checkEventQoSPassthrough(t *testing.T) {
	t.Helper()
	bridge := rmwbridge.NewBridge()
	defer func() {
		closeBridgeChecked(t, bridge)
	}()
	node, err := bridge.CreateNode("n")
	requireNoError(t, err, "create node")
	qos := rmwbridge.QoS{Depth: "7"}
	pub, err := bridge.CreatePublisher(node, "/chatter", qos)
	requireNoError(t, err, "create publisher")
	defer func() {
		requireNoError(t, pub.Close(), "publisher close")
	}()
	if got := pub.ActualQoS(); got.Depth != "7" {
		t.Fatalf("pub actual qos = %q, want 7", got.Depth)
	}
	sub, err := bridge.CreateSubscription(node, "/chatter", qos)
	requireNoError(t, err, "create subscription")
	defer func() {
		requireNoError(t, sub.Close(), "subscription close")
	}()
	if got := sub.ActualQoS(); got.Depth != "7" {
		t.Fatalf("sub actual qos = %q, want 7", got.Depth)
	}
}

// checkEventUnknownRemote pins fail-closed reads for unknown remotes.
//
// Unknown topics report zero counts and empty snapshots: entries are
// skipped, never zero-filled.
func checkEventUnknownRemote(t *testing.T) {
	t.Helper()
	bridge := rmwbridge.NewBridge()
	defer func() {
		closeBridgeChecked(t, bridge)
	}()
	if got := bridge.CountPublishers("/missing"); got != 0 {
		t.Fatalf("publishers missing = %d, want 0", got)
	}
	if got := bridge.CountSubscribers("/missing"); got != 0 {
		t.Fatalf("subscribers missing = %d, want 0", got)
	}
	if got := bridge.TopicNamesAndTypes(); len(got) != 0 {
		t.Fatalf("topics empty bridge = %d, want 0", len(got))
	}
	if got := bridge.ServiceNamesAndTypes(); len(got) != 0 {
		t.Fatalf("services empty bridge = %d, want 0", len(got))
	}
}

// TestBridgeEventSources covers node names, QoS passthrough, and
// fail-closed unknown remote reads backing the event and info paths.
func TestBridgeEventSources(t *testing.T) {
	t.Run("node_names", checkEventNodeNames)
	t.Run("qos_passthrough", checkEventQoSPassthrough)
	t.Run("unknown_remote", checkEventUnknownRemote)
}
