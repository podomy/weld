// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

package rmwbridge_test

import (
	"testing"

	"github.com/podomy/weld/internal/rmwbridge"
)

// checkNodeCreateDuplicate pins create and duplicate handling.
func checkNodeCreateDuplicate(t *testing.T, bridge *rmwbridge.Bridge) *rmwbridge.Node {
	t.Helper()
	node, err := bridge.CreateNode("n1")
	requireNoError(t, err, "create node")
	if node.Name != "n1" {
		t.Fatalf("node name = %q, want n1", node.Name)
	}
	if node.Namespace != "/" {
		t.Fatalf("node namespace = %q, want /", node.Namespace)
	}
	_, err = bridge.CreateNode("n1")
	requireErrorIs(t, err, rmwbridge.ErrNodeExists, "duplicate node")
	_, err = bridge.CreateNode("")
	requireError(t, err, "empty node name")
	return node
}

// checkNodeDomainDestroy pins cross-domain create and destroy paths.
func checkNodeDomainDestroy(t *testing.T, bridge *rmwbridge.Bridge) {
	t.Helper()
	_, err := bridge.CreateNodeInDomain(1, "n1")
	requireNoError(t, err, "same name in other domain")
	err = bridge.DestroyNode("n1")
	requireNoError(t, err, "destroy node")
	err = bridge.DestroyNode("n1")
	requireErrorIs(t, err, rmwbridge.ErrNodeNotFound, "second destroy")
	err = bridge.DestroyNode("missing")
	requireErrorIs(t, err, rmwbridge.ErrNodeNotFound, "destroy missing")
	err = bridge.DestroyNode("")
	requireError(t, err, "destroy empty name")
}

// checkNodeRecreateNil pins recreate and nil handle paths.
func checkNodeRecreateNil(t *testing.T, bridge *rmwbridge.Bridge) {
	t.Helper()
	node2, err := bridge.CreateNode("n1")
	requireNoError(t, err, "recreate after destroy")
	if node2.Name != "n1" {
		t.Fatalf("recreated node name = %q, want n1", node2.Name)
	}
	_, err = bridge.CreatePublisher(nil, "/t", rmwbridge.DefaultQoS())
	requireError(t, err, "create publisher with nil node")
	ghost := &rmwbridge.Node{Name: "ghost", Namespace: "/", DomainID: 0}
	_ = ghost
	_, err = bridge.CreateSubscription(nil, "/t", rmwbridge.DefaultQoS())
	requireError(t, err, "create subscription with nil node")
}

// TestConformanceNode covers node lifecycle.
func TestConformanceNode(t *testing.T) {
	t.Run("node lifecycle", func(t *testing.T) {
		t.Parallel()
		bridge := newConformanceBridge(t)
		joinConformanceBridge(t, bridge, "p1", 0)
		checkNodeCreateDuplicate(t, bridge)
		checkNodeDomainDestroy(t, bridge)
		checkNodeRecreateNil(t, bridge)
	})
}
