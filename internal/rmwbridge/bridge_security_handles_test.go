// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

package rmwbridge_test

import (
	"context"
	"testing"
	"time"

	"github.com/podomy/weld/internal/rmwbridge"
)

// securityTimeout bounds every blocking call in these tests.
const securityTimeout = 200 * time.Millisecond

// newSecurityBridge returns a joined bridge for security tests.
func newSecurityBridge(t *testing.T) *rmwbridge.Bridge {
	t.Helper()
	b := rmwbridge.NewBridge()
	ctx, cancel := context.WithTimeout(context.Background(), securityTimeout)
	defer cancel()
	err := b.JoinDiscovery(ctx, rmwbridge.DiscoveryInfo{ParticipantName: "sec", DomainID: 0})
	if err != nil {
		t.Fatalf("join discovery: %v", err)
	}
	return b
}

// mustNode creates a uniquely named node for the given bridge.
func mustNode(t *testing.T, b *rmwbridge.Bridge, name string) *rmwbridge.Node {
	t.Helper()
	n, err := b.CreateNode(name)
	if err != nil {
		t.Fatalf("create node %q: %v", name, err)
	}
	return n
}

// checkNilPublisherSub pins nil node handle failures.
func checkNilPublisherSub(t *testing.T, b *rmwbridge.Bridge, qos rmwbridge.QoS) {
	t.Helper()
	_, err := b.CreatePublisher(nil, "/t", qos)
	requireError(t, err, "CreatePublisher(nil)")
	_, err = b.CreateSubscription(nil, "/t", qos)
	requireError(t, err, "CreateSubscription(nil)")
}

// checkNilGuard pins nil guard ignore and trigger cycle.
func checkNilGuard(t *testing.T) {
	t.Helper()
	ws := rmwbridge.NewWaitSet()
	ws.AddGuard(nil)
	g := rmwbridge.NewGuard()
	ws.AddGuard(g)
	g.Trigger()
	if !g.IsTriggered() {
		t.Fatalf("guard not triggered after Trigger")
	}
	ctx, cancel := context.WithTimeout(context.Background(), securityTimeout)
	defer cancel()
	err := ws.Wait(ctx)
	requireNoError(t, err, "wait with triggered guard")
	g.Reset()
	if g.IsTriggered() {
		t.Fatalf("guard still triggered after Reset")
	}
}

// checkNilPayloadRoundtrip pins nil payload empty sample roundtrip.
func checkNilPayloadRoundtrip(t *testing.T, b *rmwbridge.Bridge, qos rmwbridge.QoS) {
	t.Helper()
	n := mustNode(t, b, "nilhandles")
	pub, err := b.CreatePublisher(n, "/nil", qos)
	requireNoError(t, err, "create publisher")
	sub, err := b.CreateSubscription(n, "/nil", qos)
	requireNoError(t, err, "create subscription")
	ctx2, cancel2 := context.WithTimeout(context.Background(), securityTimeout)
	defer cancel2()
	err = pub.Publish(ctx2, nil)
	requireNoError(t, err, "publish nil payload")
	got, err := sub.Take(ctx2)
	requireNoError(t, err, "take nil payload")
	if len(got) != 0 {
		t.Fatalf("nil payload roundtrip len = %d, want 0", len(got))
	}
}

// checkUnknownNodeDecode pins unknown node and nil decode failures.
func checkUnknownNodeDecode(t *testing.T, b *rmwbridge.Bridge, qos rmwbridge.QoS) {
	t.Helper()
	ghost := &rmwbridge.Node{Name: "ghost", Namespace: "/", DomainID: 0}
	_, err := b.CreatePublisher(ghost, "/t", qos)
	requireErrorIs(t, err, rmwbridge.ErrNodeNotFound, "publisher with unknown node")
	_, err = b.CreateSubscription(ghost, "/t", qos)
	requireErrorIs(t, err, rmwbridge.ErrNodeNotFound, "subscription with unknown node")
	_, err = rmwbridge.DecodeCDR(nil)
	requireError(t, err, "DecodeCDR(nil)")
}

// TestSecurityNilHandles verifies nil handles return errors and never panic.
func TestSecurityNilHandles(t *testing.T) {
	t.Parallel()
	b := newSecurityBridge(t)
	defer func() {
		closeSecurityBridge(t, b)
	}()
	qos := rmwbridge.DefaultQoS()
	checkNilPublisherSub(t, b, qos)
	checkNilGuard(t)
	checkNilPayloadRoundtrip(t, b, qos)
	checkUnknownNodeDecode(t, b, qos)
}

// checkEmptyNodeNames pins empty node name failures.
func checkEmptyNodeNames(t *testing.T, b *rmwbridge.Bridge) {
	t.Helper()
	_, err := b.CreateNode("")
	requireError(t, err, "CreateNode empty")
	_, err = b.CreateNodeInDomain(0, "")
	requireError(t, err, "CreateNodeInDomain empty")
	err = b.DestroyNode("")
	requireError(t, err, "DestroyNode empty")
}

// checkEmptyTopicNames pins empty topic failures.
func checkEmptyTopicNames(t *testing.T, b *rmwbridge.Bridge, n *rmwbridge.Node, qos rmwbridge.QoS) {
	t.Helper()
	_, err := b.CreatePublisher(n, "", qos)
	requireError(t, err, "CreatePublisher empty topic")
	_, err = b.CreateSubscription(n, "", qos)
	requireError(t, err, "CreateSubscription empty topic")
}

// checkEmptyServiceClient pins empty service and client failures.
func checkEmptyServiceClient(t *testing.T, b *rmwbridge.Bridge) {
	t.Helper()
	_, err := b.CreateService("")
	requireError(t, err, "CreateService empty")
	err = b.DestroyService("")
	requireError(t, err, "DestroyService empty")
	_, err = b.CreateClient("")
	requireError(t, err, "CreateClient empty")
	err = b.DestroyClient("")
	requireError(t, err, "DestroyClient empty")
	err = b.RegisterType("", "")
	requireError(t, err, "RegisterType empty")
	_, err = b.LookupType("", "")
	requireError(t, err, "LookupType empty")
}

// checkTooManyQoS pins three QoS argument rejection.
func checkTooManyQoS(t *testing.T, b *rmwbridge.Bridge, qos rmwbridge.QoS) {
	t.Helper()
	_, err := b.CreateService("qos-too-many", qos, qos, qos)
	requireError(t, err, "CreateService with 3 QoS")
	_, err = b.CreateClient("qos-too-many", qos, qos, qos)
	requireError(t, err, "CreateClient with 3 QoS")
}

// TestSecurityEmptyNames verifies every name-taking API rejects "".
func TestSecurityEmptyNames(t *testing.T) {
	t.Parallel()
	b := newSecurityBridge(t)
	defer func() {
		closeSecurityBridge(t, b)
	}()
	qos := rmwbridge.DefaultQoS()
	n := mustNode(t, b, "emptynames")
	checkEmptyNodeNames(t, b)
	checkEmptyTopicNames(t, b, n, qos)
	checkEmptyServiceClient(t, b)
	checkTooManyQoS(t, b, qos)
}
