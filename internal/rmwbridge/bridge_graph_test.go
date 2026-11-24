// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

package rmwbridge_test

import (
	"context"
	"testing"
	"time"

	"github.com/podomy/weld/internal/rmwbridge"
)

// setupGraphBridge joins discovery and creates two nodes.
func setupGraphBridge(t *testing.T, bridge *rmwbridge.Bridge) *rmwbridge.Node {
	t.Helper()
	ctx := context.Background()
	err := bridge.JoinDiscovery(ctx, rmwbridge.DiscoveryInfo{ParticipantName: "p1", DomainID: 42})
	requireNoError(t, err, "join discovery")
	if got := bridge.DomainID(); got != 42 {
		t.Fatalf("domain = %d, want 42", got)
	}
	n1, err := bridge.CreateNode("n1")
	requireNoError(t, err, "create node")
	_, err = bridge.CreateNodeWithNamespace("n2", "/ns")
	requireNoError(t, err, "create node with namespace")
	nodes := bridge.NodeNames()
	if len(nodes) != 2 {
		t.Fatalf("nodes = %d, want 2", len(nodes))
	}
	return n1
}

// setupGraphPubSub creates typed pub sub plus a raw publisher.
func setupGraphPubSub(t *testing.T, bridge *rmwbridge.Bridge, n1 *rmwbridge.Node) (*rmwbridge.Publisher, *rmwbridge.Subscription) {
	t.Helper()
	qos := rmwbridge.DefaultQoS()
	pub, err := bridge.CreatePublisherWithType(n1, "/chatter", "std_msgs__msg__String", qos)
	requireNoError(t, err, "create publisher with type")
	sub, err := bridge.CreateSubscriptionWithType(n1, "/chatter", "std_msgs__msg__String", qos)
	requireNoError(t, err, "create subscription with type")
	_, err = bridge.CreatePublisher(n1, "/raw", qos)
	requireNoError(t, err, "create publisher")
	return pub, sub
}

// checkGraphTopics pins topic names and types snapshot.
func checkGraphTopics(t *testing.T, bridge *rmwbridge.Bridge) {
	t.Helper()
	topics := bridge.TopicNamesAndTypes()
	byTopic := make(map[string][]string, len(topics))
	for _, info := range topics {
		byTopic[info.Name] = info.Types
	}
	gotTypes, ok := byTopic["/chatter"]
	if !ok {
		t.Fatalf("topics missing /chatter")
	}
	if len(gotTypes) != 1 || gotTypes[0] != "std_msgs__msg__String" {
		t.Fatalf("chatter types = %v, want [std_msgs__msg__String]", gotTypes)
	}
	rawTypes, ok := byTopic["/raw"]
	if !ok {
		t.Fatalf("topics missing /raw")
	}
	if len(rawTypes) != 0 {
		t.Fatalf("raw types = %v, want empty", rawTypes)
	}
}

// checkGraphCounts pins publisher and subscriber counts.
func checkGraphCounts(t *testing.T, bridge *rmwbridge.Bridge) {
	t.Helper()
	if got := bridge.CountPublishers("/chatter"); got != 1 {
		t.Fatalf("publishers = %d, want 1", got)
	}
	if got := bridge.CountSubscribers("/chatter"); got != 1 {
		t.Fatalf("subscribers = %d, want 1", got)
	}
	if got := bridge.CountPublishers("/missing"); got != 0 {
		t.Fatalf("missing publishers = %d, want 0", got)
	}
}

// checkGraphServices pins service names, types, and counts.
func checkGraphServices(t *testing.T, bridge *rmwbridge.Bridge) {
	t.Helper()
	_, err := bridge.CreateServiceWithType("add_two_ints", "example_interfaces__srv__AddTwoInts")
	requireNoError(t, err, "create service with type")
	_, err = bridge.CreateClientWithType("add_two_ints", "example_interfaces__srv__AddTwoInts")
	requireNoError(t, err, "create client with type")
	services := bridge.ServiceNamesAndTypes()
	if len(services) != 1 {
		t.Fatalf("services = %d, want 1", len(services))
	}
	if services[0].Name != "add_two_ints" {
		t.Fatalf("service name = %q, want add_two_ints", services[0].Name)
	}
	if len(services[0].Types) != 1 || services[0].Types[0] != "example_interfaces__srv__AddTwoInts" {
		t.Fatalf("service types = %v, want [example_interfaces__srv__AddTwoInts]", services[0].Types)
	}
	if got := bridge.CountServices("add_two_ints"); got != 1 {
		t.Fatalf("service count = %d, want 1", got)
	}
	if got := bridge.CountClients("add_two_ints"); got != 1 {
		t.Fatalf("client count = %d, want 1", got)
	}
	if got := bridge.CountServices("missing"); got != 0 {
		t.Fatalf("missing service count = %d, want 0", got)
	}
}

// TestGraphSnapshot covers node list, topic and service names with types,
// and publisher, subscriber, service, and client counts.
func TestGraphSnapshot(t *testing.T) {
	t.Parallel()

	bridge := rmwbridge.NewBridge()
	defer func() {
		closeBridgeChecked(t, bridge)
	}()

	n1 := setupGraphBridge(t, bridge)
	pub, sub := setupGraphPubSub(t, bridge, n1)
	defer func() {
		err := sub.Close()
		requireNoError(t, err, "subscription close")
	}()

	checkGraphTopics(t, bridge)
	checkGraphCounts(t, bridge)
	checkGraphServices(t, bridge)

	err := pub.Close()
	requireNoError(t, err, "publisher close")
	if got := bridge.CountPublishers("/chatter"); got != 0 {
		t.Fatalf("publishers after close = %d, want 0", got)
	}
}

// TestGraphWaitTrigger covers async trigger delivery through a wait set.
func TestGraphWaitTrigger(t *testing.T) {
	t.Parallel()

	guard := rmwbridge.NewGuard()
	ws := rmwbridge.NewWaitSet()
	ws.AddGuard(guard)

	go func() {
		time.Sleep(10 * time.Millisecond)
		guard.Trigger()
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err := ws.Wait(ctx)
	requireNoError(t, err, "wait after async trigger")
	if !guard.IsTriggered() {
		t.Fatalf("guard not triggered after wait")
	}
}

// TestGraphGuardReset covers guard trigger, check, and reset cycle.
func TestGraphGuardReset(t *testing.T) {
	t.Parallel()

	guard := rmwbridge.NewGuard()
	if guard.IsTriggered() {
		t.Fatalf("new guard is triggered")
	}
	guard.Trigger()
	guard.Trigger()
	if !guard.IsTriggered() {
		t.Fatalf("triggered guard reports false")
	}
	guard.Reset()
	if guard.IsTriggered() {
		t.Fatalf("reset guard still triggered")
	}

	ws := rmwbridge.NewWaitSet()
	ws.AddGuard(guard)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	err := ws.Wait(ctx)
	requireError(t, err, "wait after reset")
}
