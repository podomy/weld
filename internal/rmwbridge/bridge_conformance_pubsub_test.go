// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

package rmwbridge_test

import (
	"context"
	"testing"
	"time"

	"github.com/podomy/weld/internal/rmwbridge"
)

// publishOrderedSamples publishes count ordered one-byte samples.
func publishOrderedSamples(t *testing.T, pub *rmwbridge.Publisher, ctx context.Context, count int) {
	t.Helper()
	for i := range count {
		payload := []byte{byte('a' + i)}
		err := pub.Publish(ctx, payload)
		requireNoError(t, err, "publish")
	}
}

// takeOrderedSamples takes count ordered samples and pins payloads.
func takeOrderedSamples(t *testing.T, sub *rmwbridge.Subscription, ctx context.Context, count int) {
	t.Helper()
	for i := range count {
		got, err := sub.Take(ctx)
		requireNoError(t, err, "take")
		want := byte('a' + i)
		if len(got) != 1 || got[0] != want {
			t.Fatalf("payload %d = %v, want [%v]", i, got, want)
		}
	}
}

// checkPubSubCloseAfter pins close idempotence and use after close.
func checkPubSubCloseAfter(t *testing.T, pub *rmwbridge.Publisher, sub *rmwbridge.Subscription, ctx context.Context) {
	t.Helper()
	err := sub.Close()
	requireNoError(t, err, "subscription close")
	err = sub.Close()
	requireNoError(t, err, "second subscription close")
	_, err = sub.Take(ctx)
	requireErrorIs(t, err, rmwbridge.ErrSubscriptionClosed, "take after close")
	err = pub.Close()
	requireNoError(t, err, "publisher close")
	err = pub.Publish(ctx, []byte("late"))
	requireErrorIs(t, err, rmwbridge.ErrPublisherClosed, "publish after close")
}

// checkPubSubRoundtrip pins ordered publish and take plus close.
func checkPubSubRoundtrip(t *testing.T) {
	t.Helper()
	bridge := newConformanceBridge(t)
	node := mustConformanceNode(t, bridge, "talker")
	qos := rmwbridge.QoS{Depth: "7"}
	pub, err := bridge.CreatePublisher(node, "/chatter", qos)
	requireNoError(t, err, "create publisher")
	sub, err := bridge.CreateSubscription(node, "/chatter", qos)
	requireNoError(t, err, "create subscription")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	count := 8
	publishOrderedSamples(t, pub, ctx, count)
	takeOrderedSamples(t, sub, ctx, count)
	checkPubSubCloseAfter(t, pub, sub, ctx)
}

// mustDomainPair creates one publisher and one subscription for a node.
func mustDomainPair(t *testing.T, bridge *rmwbridge.Bridge, node *rmwbridge.Node, topic string, qos rmwbridge.QoS) (*rmwbridge.Publisher, *rmwbridge.Subscription) {
	t.Helper()
	pub, err := bridge.CreatePublisher(node, topic, qos)
	requireNoError(t, err, "create publisher")
	sub, err := bridge.CreateSubscription(node, topic, qos)
	requireNoError(t, err, "create subscription")
	return pub, sub
}

// checkDomainMatchedCounts pins per-domain matched counts.
func checkDomainMatchedCounts(t *testing.T, pub0 *rmwbridge.Publisher, sub1 *rmwbridge.Subscription) {
	t.Helper()
	if got := pub0.CountMatchedSubscriptions(); got != 1 {
		t.Fatalf("pub0 matched = %d, want 1", got)
	}
	if got := sub1.CountMatchedPublishers(); got != 1 {
		t.Fatalf("sub1 matched = %d, want 1", got)
	}
}

// checkDomainPublishTake pins same-domain delivery and cross-domain block.
func checkDomainPublishTake(t *testing.T, pub0 *rmwbridge.Publisher, sub0 *rmwbridge.Subscription, pub1 *rmwbridge.Publisher, sub1 *rmwbridge.Subscription) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err := pub0.Publish(ctx, []byte("d0"))
	requireNoError(t, err, "pub0 publish")
	got, err := sub0.Take(ctx)
	requireNoError(t, err, "sub0 take")
	if string(got) != "d0" {
		t.Fatalf("sub0 payload = %q, want d0", string(got))
	}
	short, cancelShort := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancelShort()
	_, err = sub1.Take(short)
	requireError(t, err, "cross domain take")
	err = pub1.Publish(ctx, []byte("d1"))
	requireNoError(t, err, "pub1 publish")
	got, err = sub1.Take(ctx)
	requireNoError(t, err, "sub1 take")
	if string(got) != "d1" {
		t.Fatalf("sub1 payload = %q, want d1", string(got))
	}
	short2, cancelShort2 := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancelShort2()
	_, err = sub0.Take(short2)
	requireError(t, err, "reverse cross domain take")
}

// checkPubSubDomainIsolation pins per-domain registries for datapath.
func checkPubSubDomainIsolation(t *testing.T) {
	t.Helper()
	bridge := newConformanceBridge(t)
	qos := rmwbridge.DefaultQoS()
	n0, err := bridge.CreateNodeInDomain(0, "n0")
	requireNoError(t, err, "create node domain 0")
	n1, err := bridge.CreateNodeInDomain(1, "n1")
	requireNoError(t, err, "create node domain 1")
	pub0, sub0 := mustDomainPair(t, bridge, n0, "/chatter", qos)
	pub1, sub1 := mustDomainPair(t, bridge, n1, "/chatter", qos)
	checkDomainMatchedCounts(t, pub0, sub1)
	checkDomainPublishTake(t, pub0, sub0, pub1, sub1)
}

// checkMatchedInitial pins matched counts with one subscription.
func checkMatchedInitial(t *testing.T, pub *rmwbridge.Publisher, sub1 *rmwbridge.Subscription) {
	t.Helper()
	if got := pub.CountMatchedSubscriptions(); got != 1 {
		t.Fatalf("pub matched = %d, want 1", got)
	}
	if got := sub1.CountMatchedPublishers(); got != 1 {
		t.Fatalf("sub matched = %d, want 1", got)
	}
}

// checkMatchedSecondSub pins matched counts after second sub and closes.
func checkMatchedSecondSub(t *testing.T, bridge *rmwbridge.Bridge, node *rmwbridge.Node, qos rmwbridge.QoS, pub *rmwbridge.Publisher, sub1 *rmwbridge.Subscription) {
	t.Helper()
	sub2, err := bridge.CreateSubscription(node, "/chatter", qos)
	requireNoError(t, err, "create sub2")
	if got := pub.CountMatchedSubscriptions(); got != 2 {
		t.Fatalf("pub matched after second sub = %d, want 2", got)
	}
	err = sub2.Close()
	requireNoError(t, err, "close sub2")
	if got := pub.CountMatchedSubscriptions(); got != 1 {
		t.Fatalf("pub matched after sub close = %d, want 1", got)
	}
	err = pub.Close()
	requireNoError(t, err, "close pub")
	if got := sub1.CountMatchedPublishers(); got != 0 {
		t.Fatalf("sub matched after pub close = %d, want 0", got)
	}
	if got := pub.CountMatchedSubscriptions(); got != 0 {
		t.Fatalf("closed pub matched = %d, want 0", got)
	}
}

// checkPubSubMatchedCounts pins publisher and subscription matched tracking.
func checkPubSubMatchedCounts(t *testing.T) {
	t.Helper()
	bridge := newConformanceBridge(t)
	node := mustConformanceNode(t, bridge, "n1")
	qos := rmwbridge.QoS{Depth: "7"}
	pub, err := bridge.CreatePublisher(node, "/chatter", qos)
	requireNoError(t, err, "create publisher")
	if got := pub.CountMatchedSubscriptions(); got != 0 {
		t.Fatalf("matched before sub = %d, want 0", got)
	}
	sub1, err := bridge.CreateSubscription(node, "/chatter", qos)
	requireNoError(t, err, "create sub1")
	checkMatchedInitial(t, pub, sub1)
	checkMatchedSecondSub(t, bridge, node, qos, pub, sub1)
}

// checkPubSubQoS pins stored and actual QoS passthrough.
func checkPubSubQoS(t *testing.T, bridge *rmwbridge.Bridge, node *rmwbridge.Node, qos rmwbridge.QoS) (*rmwbridge.Publisher, *rmwbridge.Publisher) {
	t.Helper()
	pub, err := bridge.CreatePublisher(node, "/chatter", qos)
	requireNoError(t, err, "create publisher")
	if got := pub.ActualQoS(); got.Depth != "7" {
		t.Fatalf("pub actual qos = %q, want 7", got.Depth)
	}
	sub, err := bridge.CreateSubscription(node, "/chatter", qos)
	requireNoError(t, err, "create subscription")
	if got := sub.ActualQoS(); got.Depth != "7" {
		t.Fatalf("sub actual qos = %q, want 7", got.Depth)
	}
	if pub.QoS.Depth != "7" || sub.QoS.Depth != "7" {
		t.Fatalf("stored qos changed")
	}
	def := rmwbridge.DefaultQoS()
	if def.Depth != "10" {
		t.Fatalf("default qos = %q, want 10", def.Depth)
	}
	pubB, err := bridge.CreatePublisher(node, "/chatter", qos)
	requireNoError(t, err, "create second publisher")
	return pub, pubB
}

// checkGIDUniqueness pins distinct GIDs with domain in first bytes.
func checkGIDUniqueness(t *testing.T, bridge *rmwbridge.Bridge, node *rmwbridge.Node, qos rmwbridge.QoS, pub *rmwbridge.Publisher, pubB *rmwbridge.Publisher) {
	t.Helper()
	gA := pub.GID()
	gB := pubB.GID()
	if gA == gB {
		t.Fatalf("publisher GIDs collide")
	}
	if gA[0] != 3 || gB[0] != 3 {
		t.Fatalf("GID domain byte = %d, %d, want 3", gA[0], gB[0])
	}
	seen := map[[16]byte]struct{}{gA: {}, gB: {}}
	for i := range 8 {
		p, err := bridge.CreatePublisher(node, "/chatter", qos)
		requireNoError(t, err, "create publisher")
		g := p.GID()
		if _, dup := seen[g]; dup {
			t.Fatalf("duplicate GID at %d", i)
		}
		seen[g] = struct{}{}
	}
}

// checkPubSubQosGid pins QoS passthrough and GID uniqueness.
func checkPubSubQosGid(t *testing.T) {
	t.Helper()
	bridge := newConformanceBridge(t)
	node, err := bridge.CreateNodeInDomain(3, "n0")
	requireNoError(t, err, "create node")
	qos := rmwbridge.QoS{Depth: "7"}
	pub, pubB := checkPubSubQoS(t, bridge, node, qos)
	checkGIDUniqueness(t, bridge, node, qos, pub, pubB)
}

// TestConformancePubSub covers publish subscribe paths.
func TestConformancePubSub(t *testing.T) {
	t.Run("pubsub roundtrip ordered", func(t *testing.T) {
		t.Parallel()
		checkPubSubRoundtrip(t)
	})

	t.Run("pubsub domain isolation", func(t *testing.T) {
		t.Parallel()
		checkPubSubDomainIsolation(t)
	})

	t.Run("pubsub matched counts", func(t *testing.T) {
		t.Parallel()
		checkPubSubMatchedCounts(t)
	})

	t.Run("pubsub qos gid", func(t *testing.T) {
		t.Parallel()
		checkPubSubQosGid(t)
	})
}
