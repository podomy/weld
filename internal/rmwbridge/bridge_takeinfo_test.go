// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

package rmwbridge_test

import (
	"context"
	"testing"
	"time"

	"github.com/podomy/weld/internal/rmwbridge"
)

// takeInfoCtx returns a bounded context for take-info tests.
func takeInfoCtx(t *testing.T) (context.Context, context.CancelFunc) {
	t.Helper()
	return context.WithTimeout(context.Background(), 2*time.Second)
}

// mustTakeInfoNode creates a node for take-info tests.
func mustTakeInfoNode(t *testing.T, bridge *rmwbridge.Bridge) *rmwbridge.Node {
	t.Helper()
	node, err := bridge.CreateNode("takeinfo")
	requireNoError(t, err, "create node")
	return node
}

// mustTakeInfoEndpoints creates one publisher plus one subscription.
func mustTakeInfoEndpoints(t *testing.T, bridge *rmwbridge.Bridge, node *rmwbridge.Node) (*rmwbridge.Publisher, *rmwbridge.Subscription) {
	t.Helper()
	pub, err := bridge.CreatePublisher(node, "/takeinfo", rmwbridge.DefaultQoS())
	requireNoError(t, err, "create publisher")
	sub, err := bridge.CreateSubscription(node, "/takeinfo", rmwbridge.DefaultQoS())
	requireNoError(t, err, "create subscription")
	return pub, sub
}

// checkTakeInfoSeqGID takes one sample and pins its sequence plus GID.
func checkTakeInfoSeqGID(t *testing.T, ctx context.Context, sub *rmwbridge.Subscription, want []byte, wantSeq uint64, wantGID [16]byte) {
	t.Helper()
	got, gid, seq, err := sub.TakeWithInfo(ctx)
	requireNoError(t, err, "take with info")
	if string(got) != string(want) {
		t.Fatalf("payload = %q, want %q", string(got), string(want))
	}
	if seq != wantSeq {
		t.Fatalf("sequence = %d, want %d", seq, wantSeq)
	}
	if gid != wantGID {
		t.Fatalf("gid = %x, want %x", gid, wantGID)
	}
	if gid == ([16]byte{}) {
		t.Fatalf("gid is zero, want writer identity")
	}
}

// TestTakeInfoSequence pins 1,2,3 receive sequences across takes.
//
// The sequence increments on every delivered take, including Takes
// that discard the attribution. Intra-process and network sequence
// numbers are equal because the bridge is a single transport.
func TestTakeInfoSequence(t *testing.T) {
	t.Parallel()
	bridge := rmwbridge.NewBridge()
	defer func() {
		closeBridgeChecked(t, bridge)
	}()
	node := mustTakeInfoNode(t, bridge)
	pub, sub := mustTakeInfoEndpoints(t, bridge, node)
	wantGID := pub.GID()
	ctx, cancel := takeInfoCtx(t)
	defer cancel()
	for i, payload := range []string{"one", "two", "three"} {
		err := pub.Publish(ctx, []byte(payload))
		requireNoError(t, err, "publish")
		checkTakeInfoSeqGID(t, ctx, sub, []byte(payload), uint64(i+1), wantGID)
	}
}

// TestTakeInfoCountsPlainTake pins that a plain Take advances the sequence.
//
// A Take without attribution still delivers, so the next TakeWithInfo
// must report the following sequence with the same writer GID.
func TestTakeInfoCountsPlainTake(t *testing.T) {
	t.Parallel()
	bridge := rmwbridge.NewBridge()
	defer func() {
		closeBridgeChecked(t, bridge)
	}()
	node := mustTakeInfoNode(t, bridge)
	pub, sub := mustTakeInfoEndpoints(t, bridge, node)
	wantGID := pub.GID()
	ctx, cancel := takeInfoCtx(t)
	defer cancel()
	err := pub.Publish(ctx, []byte("first"))
	requireNoError(t, err, "publish first")
	got, err := sub.Take(ctx)
	requireNoError(t, err, "plain take")
	if string(got) != "first" {
		t.Fatalf("payload = %q, want first", string(got))
	}
	err = pub.Publish(ctx, []byte("second"))
	requireNoError(t, err, "publish second")
	checkTakeInfoSeqGID(t, ctx, sub, []byte("second"), 2, wantGID)
}

// TestTakeInfoPublisherGID pins the writer GID of a delivered take.
//
// Publishing from a known writer and taking must report a nonzero GID
// equal to the writer identity, with the domain in the first bytes.
func TestTakeInfoPublisherGID(t *testing.T) {
	t.Parallel()
	bridge := rmwbridge.NewBridge()
	defer func() {
		closeBridgeChecked(t, bridge)
	}()
	node, err := bridge.CreateNodeInDomain(5, "gidnode")
	requireNoError(t, err, "create node")
	pub, err := bridge.CreatePublisher(node, "/gid", rmwbridge.DefaultQoS())
	requireNoError(t, err, "create publisher")
	sub, err := bridge.CreateSubscription(node, "/gid", rmwbridge.DefaultQoS())
	requireNoError(t, err, "create subscription")
	wantGID := pub.GID()
	if wantGID == ([16]byte{}) {
		t.Fatalf("publisher GID is zero")
	}
	ctx, cancel := takeInfoCtx(t)
	defer cancel()
	err = pub.Publish(ctx, []byte("ping"))
	requireNoError(t, err, "publish")
	_, gid, seq, err := sub.TakeWithInfo(ctx)
	requireNoError(t, err, "take with info")
	if gid != wantGID {
		t.Fatalf("gid = %x, want %x", gid, wantGID)
	}
	if seq != 1 {
		t.Fatalf("sequence = %d, want 1", seq)
	}
	if gid[0] != 5 {
		t.Fatalf("gid domain byte = %d, want 5", gid[0])
	}
	lastGID, lastSeq := sub.LastTakeInfo()
	if lastGID != wantGID || lastSeq != 1 {
		t.Fatalf("last take info = %x/%d, want %x/1", lastGID, lastSeq, wantGID)
	}
}
