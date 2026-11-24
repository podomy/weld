// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

package rmwbridge_test

import (
	"context"
	"testing"
	"time"

	"github.com/podomy/weld/internal/rmwbridge"
)

// closeBridgeChecked closes a bridge and fails on error.
func closeBridgeChecked(t *testing.T, bridge *rmwbridge.Bridge) {
	t.Helper()
	err := bridge.Close()
	if err != nil {
		t.Fatalf("close: %v", err)
	}
}

// mustDomainNode creates a node in an explicit domain.
func mustDomainNode(t *testing.T, bridge *rmwbridge.Bridge, domain uint32, name string) *rmwbridge.Node {
	t.Helper()
	node, err := bridge.CreateNodeInDomain(domain, name)
	requireNoError(t, err, "create node")
	return node
}

// checkDatapathMatchedCounts pins per-domain matched counts.
func checkDatapathMatchedCounts(t *testing.T, pub0 *rmwbridge.Publisher, pub1 *rmwbridge.Publisher, sub0 *rmwbridge.Subscription, sub1 *rmwbridge.Subscription) {
	t.Helper()
	if got := pub0.CountMatchedSubscriptions(); got != 1 {
		t.Fatalf("pub0 matched = %d, want 1", got)
	}
	if got := pub1.CountMatchedSubscriptions(); got != 1 {
		t.Fatalf("pub1 matched = %d, want 1", got)
	}
	if got := sub0.CountMatchedPublishers(); got != 1 {
		t.Fatalf("sub0 matched = %d, want 1", got)
	}
	if got := sub1.CountMatchedPublishers(); got != 1 {
		t.Fatalf("sub1 matched = %d, want 1", got)
	}
}

// checkDomainPublishIsolation pins same-domain delivery.
func checkDomainPublishIsolation(t *testing.T, pub0 *rmwbridge.Publisher, sub0 *rmwbridge.Subscription, ctx context.Context) {
	t.Helper()
	err := pub0.Publish(ctx, []byte("d0"))
	requireNoError(t, err, "pub0 publish")
	got, err := sub0.Take(ctx)
	requireNoError(t, err, "sub0 take")
	if string(got) != "d0" {
		t.Fatalf("sub0 payload = %q, want d0", string(got))
	}
}

// checkDomainCrossTake pins cross-domain take timeout.
func checkDomainCrossTake(t *testing.T, sub *rmwbridge.Subscription) {
	t.Helper()
	short, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, err := sub.Take(short)
	requireError(t, err, "cross domain take")
}

// TestDomainIsolation pins per-domain registries for pub/sub datapath.
func TestDomainIsolation(t *testing.T) {
	t.Parallel()
	bridge := rmwbridge.NewBridge()
	defer func() {
		closeBridgeChecked(t, bridge)
	}()
	qos := rmwbridge.DefaultQoS()
	n0 := mustDomainNode(t, bridge, 0, "n0")
	n1 := mustDomainNode(t, bridge, 1, "n1")
	if n0.DomainID != 0 || n1.DomainID != 1 {
		t.Fatalf("node domains = %d, %d, want 0, 1", n0.DomainID, n1.DomainID)
	}
	pub0, err := bridge.CreatePublisher(n0, "/chatter", qos)
	requireNoError(t, err, "create pub0")
	sub0, err := bridge.CreateSubscription(n0, "/chatter", qos)
	requireNoError(t, err, "create sub0")
	pub1, err := bridge.CreatePublisher(n1, "/chatter", qos)
	requireNoError(t, err, "create pub1")
	sub1, err := bridge.CreateSubscription(n1, "/chatter", qos)
	requireNoError(t, err, "create sub1")
	checkDatapathMatchedCounts(t, pub0, pub1, sub0, sub1)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	checkDomainPublishIsolation(t, pub0, sub0, ctx)
	checkDomainCrossTake(t, sub1)
	err = pub1.Publish(ctx, []byte("d1"))
	requireNoError(t, err, "pub1 publish")
	got, err := sub1.Take(ctx)
	requireNoError(t, err, "sub1 take")
	if string(got) != "d1" {
		t.Fatalf("sub1 payload = %q, want d1", string(got))
	}
	checkDomainCrossTake(t, sub0)
}

// TestDomainDuplicateNodeName pins same name in different domains.
func TestDomainDuplicateNodeName(t *testing.T) {
	t.Parallel()
	bridge := rmwbridge.NewBridge()
	defer func() {
		closeBridgeChecked(t, bridge)
	}()
	_, err := bridge.CreateNodeInDomain(0, "dup")
	requireNoError(t, err, "create dup domain 0")
	_, err = bridge.CreateNodeInDomain(1, "dup")
	requireNoError(t, err, "same name in domain 1")
	_, err = bridge.CreateNodeInDomain(0, "dup")
	requireError(t, err, "duplicate in same domain")
}

// checkMatchedInitialCounts pins counts with one subscription.
func checkMatchedInitialCounts(t *testing.T, pub *rmwbridge.Publisher, sub1 *rmwbridge.Subscription) {
	t.Helper()
	if got := pub.CountMatchedSubscriptions(); got != 1 {
		t.Fatalf("pub matched = %d, want 1", got)
	}
	if got := sub1.CountMatchedPublishers(); got != 1 {
		t.Fatalf("sub matched = %d, want 1", got)
	}
}

// checkMatchedAfterSecondSub pins counts after second sub and close.
func checkMatchedAfterSecondSub(t *testing.T, bridge *rmwbridge.Bridge, node *rmwbridge.Node, qos rmwbridge.QoS, pub *rmwbridge.Publisher, sub1 *rmwbridge.Subscription) {
	t.Helper()
	sub2, err := bridge.CreateSubscription(node, "/chatter", qos)
	requireNoError(t, err, "create sub2")
	if got := pub.CountMatchedSubscriptions(); got != 2 {
		t.Fatalf("pub matched after second sub = %d, want 2", got)
	}
	err = sub2.Close()
	requireNoError(t, err, "close sub2")
	if got := pub.CountMatchedSubscriptions(); got != 1 {
		t.Fatalf("pub matched after close = %d, want 1", got)
	}
	err = pub.Close()
	requireNoError(t, err, "close pub")
	if got := sub1.CountMatchedPublishers(); got != 0 {
		t.Fatalf("sub matched after pub close = %d, want 0", got)
	}
}

// checkMatchedPublishAfterClose pins publish after close failure.
func checkMatchedPublishAfterClose(t *testing.T, pub *rmwbridge.Publisher) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err := pub.Publish(ctx, []byte("x"))
	requireError(t, err, "publish after close")
}

// TestMatchedCounts pins publisher and subscription matched tracking.
func TestMatchedCounts(t *testing.T) {
	t.Parallel()
	bridge := rmwbridge.NewBridge()
	defer func() {
		closeBridgeChecked(t, bridge)
	}()
	node, err := bridge.CreateNode("n1")
	requireNoError(t, err, "create node")
	qos := rmwbridge.QoS{Depth: "7"}
	pub, err := bridge.CreatePublisher(node, "/chatter", qos)
	requireNoError(t, err, "create publisher")
	if got := pub.ActualQoS(); got.Depth != "7" {
		t.Fatalf("pub actual qos = %q, want 7", got.Depth)
	}
	sub1, err := bridge.CreateSubscription(node, "/chatter", qos)
	requireNoError(t, err, "create sub1")
	if got := sub1.ActualQoS(); got.Depth != "7" {
		t.Fatalf("sub actual qos = %q, want 7", got.Depth)
	}
	checkMatchedInitialCounts(t, pub, sub1)
	checkMatchedAfterSecondSub(t, bridge, node, qos, pub, sub1)
	if got := pub.CountMatchedSubscriptions(); got != 0 {
		t.Fatalf("closed pub matched = %d, want 0", got)
	}
	checkMatchedPublishAfterClose(t, pub)
}

// TestPublisherGID pins distinct GIDs with domain in first bytes.
func TestPublisherGID(t *testing.T) {
	t.Parallel()
	bridge := rmwbridge.NewBridge()
	defer func() {
		closeBridgeChecked(t, bridge)
	}()
	n0, err := bridge.CreateNodeInDomain(3, "n0")
	requireNoError(t, err, "create node")
	qos := rmwbridge.DefaultQoS()
	pubA, err := bridge.CreatePublisher(n0, "/a", qos)
	requireNoError(t, err, "create pubA")
	pubB, err := bridge.CreatePublisher(n0, "/a", qos)
	requireNoError(t, err, "create pubB")
	gA := pubA.GID()
	gB := pubB.GID()
	if gA == gB {
		t.Fatalf("publisher GIDs collide")
	}
	if gA[0] != 3 || gB[0] != 3 {
		t.Fatalf("GID domain byte = %d, %d, want 3", gA[0], gB[0])
	}
}
