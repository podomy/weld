// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

package rmwbridge_test

import (
	"context"
	"testing"
	"time"

	"github.com/podomy/weld/internal/rmwbridge"
)

// TestSecurityDoubleClose verifies Close is idempotent on all handles.
func TestSecurityDoubleClose(t *testing.T) {
	t.Parallel()
	b := newSecurityBridge(t)
	n := mustNode(t, b, "doubleclose")
	qos := rmwbridge.DefaultQoS()
	pub, err := b.CreatePublisher(n, "/dc", qos)
	requireNoError(t, err, "create publisher")
	sub, err := b.CreateSubscription(n, "/dc", qos)
	requireNoError(t, err, "create subscription")
	err = pub.Close()
	requireNoError(t, err, "first pub close")
	err = pub.Close()
	requireNoError(t, err, "second pub close")
	err = sub.Close()
	requireNoError(t, err, "first sub close")
	err = sub.Close()
	requireNoError(t, err, "second sub close")
	err = b.Close()
	requireNoError(t, err, "first bridge close")
	err = b.Close()
	requireNoError(t, err, "second bridge close")
	err = pub.Close()
	requireNoError(t, err, "pub close after bridge close")
	err = sub.Close()
	requireNoError(t, err, "sub close after bridge close")
}

// setupClosedBridge creates handles then closes the bridge.
func setupClosedBridge(t *testing.T) (*rmwbridge.Bridge, *rmwbridge.Node, rmwbridge.QoS, *rmwbridge.Publisher, *rmwbridge.Subscription, *rmwbridge.Service, *rmwbridge.Client) {
	t.Helper()
	b := newSecurityBridge(t)
	n := mustNode(t, b, "closed")
	qos := rmwbridge.DefaultQoS()
	pub, err := b.CreatePublisher(n, "/closed", qos)
	requireNoError(t, err, "create publisher")
	sub, err := b.CreateSubscription(n, "/closed", qos)
	requireNoError(t, err, "create subscription")
	svc, err := b.CreateService("closedsvc")
	requireNoError(t, err, "create service")
	client, err := b.CreateClient("closedsvc")
	requireNoError(t, err, "create client")
	err = b.RegisterType("closed_type", "")
	requireNoError(t, err, "register type")
	err = b.Close()
	requireNoError(t, err, "close")
	return b, n, qos, pub, sub, svc, client
}

// checkClosedBridgeDiscovery pins discovery and datapath after close.
func checkClosedBridgeDiscovery(t *testing.T, b *rmwbridge.Bridge, ctx context.Context) {
	t.Helper()
	err := b.JoinDiscovery(ctx, rmwbridge.DiscoveryInfo{})
	requireErrorIs(t, err, rmwbridge.ErrBridgeClosed, "JoinDiscovery after close")
	err = b.PublishBytes(ctx, rmwbridge.OutgoingMessage{})
	requireErrorIs(t, err, rmwbridge.ErrBridgeClosed, "PublishBytes after close")
	_, err = b.TakeBytes(ctx)
	requireErrorIs(t, err, rmwbridge.ErrBridgeClosed, "TakeBytes after close")
	err = b.ReportLiveliness(ctx, rmwbridge.LivelinessReport{})
	requireErrorIs(t, err, rmwbridge.ErrBridgeClosed, "ReportLiveliness after close")
}

// checkClosedBridgeNodes pins node APIs after close.
func checkClosedBridgeNodes(t *testing.T, b *rmwbridge.Bridge) {
	t.Helper()
	_, err := b.CreateNode("after")
	requireErrorIs(t, err, rmwbridge.ErrBridgeClosed, "CreateNode after close")
	_, err = b.CreateNodeInDomain(0, "after")
	requireErrorIs(t, err, rmwbridge.ErrBridgeClosed, "CreateNodeInDomain after close")
	err = b.DestroyNode("closed")
	requireErrorIs(t, err, rmwbridge.ErrBridgeClosed, "DestroyNode after close")
}

// checkClosedBridgePubSub pins pub sub APIs after close.
func checkClosedBridgePubSub(t *testing.T, b *rmwbridge.Bridge, n *rmwbridge.Node, qos rmwbridge.QoS) {
	t.Helper()
	_, err := b.CreatePublisher(n, "/x", qos)
	requireErrorIs(t, err, rmwbridge.ErrBridgeClosed, "CreatePublisher after close")
	_, err = b.CreateSubscription(n, "/x", qos)
	requireErrorIs(t, err, rmwbridge.ErrBridgeClosed, "CreateSubscription after close")
}

// checkClosedBridgeServices pins service and client APIs after close.
func checkClosedBridgeServices(t *testing.T, b *rmwbridge.Bridge) {
	t.Helper()
	_, err := b.CreateService("x")
	requireErrorIs(t, err, rmwbridge.ErrBridgeClosed, "CreateService after close")
	err = b.DestroyService("closedsvc")
	requireErrorIs(t, err, rmwbridge.ErrBridgeClosed, "DestroyService after close")
	_, err = b.CreateClient("x")
	requireErrorIs(t, err, rmwbridge.ErrBridgeClosed, "CreateClient after close")
	err = b.DestroyClient("closedsvc")
	requireErrorIs(t, err, rmwbridge.ErrBridgeClosed, "DestroyClient after close")
	err = b.RegisterType("t", "")
	requireErrorIs(t, err, rmwbridge.ErrBridgeClosed, "RegisterType after close")
	_, err = b.LookupType("closed_type", "")
	requireErrorIs(t, err, rmwbridge.ErrBridgeClosed, "LookupType after close")
}

// checkClosedBridgeHandles pins handle methods after bridge close.
func checkClosedBridgeHandles(t *testing.T, ctx context.Context, pub *rmwbridge.Publisher, sub *rmwbridge.Subscription, svc *rmwbridge.Service, client *rmwbridge.Client) {
	t.Helper()
	err := pub.Publish(ctx, []byte("x"))
	requireErrorIs(t, err, rmwbridge.ErrBridgeClosed, "Publisher.Publish after close")
	_, err = sub.Take(ctx)
	requireErrorIs(t, err, rmwbridge.ErrBridgeClosed, "Subscription.Take after close")
	_, err = client.Call(ctx, []byte("x"))
	requireErrorIs(t, err, rmwbridge.ErrBridgeClosed, "Client.Call after close")
	_, err = svc.Call(ctx, []byte("x"))
	requireErrorIs(t, err, rmwbridge.ErrBridgeClosed, "Service.Call after close")
	_, _, err = svc.Serve(ctx)
	requireErrorIs(t, err, rmwbridge.ErrBridgeClosed, "Service.Serve after close")
}

// checkClosedBridgeSentinels pins zero counts and panic-free getters.
func checkClosedBridgeSentinels(t *testing.T, b *rmwbridge.Bridge, pub *rmwbridge.Publisher, sub *rmwbridge.Subscription, svc *rmwbridge.Service, client *rmwbridge.Client) {
	t.Helper()
	if got := pub.CountMatchedSubscriptions(); got != 0 {
		t.Fatalf("pub matched after close = %d, want 0", got)
	}
	if got := sub.CountMatchedPublishers(); got != 0 {
		t.Fatalf("sub matched after close = %d, want 0", got)
	}
	if svc.Exists() {
		t.Fatalf("service Exists after close = true, want false")
	}
	if client.Exists() {
		t.Fatalf("client Exists after close = true, want false")
	}
	_ = pub.ActualQoS()
	_ = sub.ActualQoS()
	_ = pub.GID()
	_ = svc.RequestActualQoS()
	_ = client.RequestActualQoS()
	g := rmwbridge.NewGuard()
	g.Trigger()
	if !g.IsTriggered() {
		t.Fatalf("guard trigger after bridge close failed")
	}
	ws := rmwbridge.NewWaitSet()
	ws.AddGuard(g)
	short, stop := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer stop()
	err := ws.Wait(short)
	requireNoError(t, err, "wait with triggered guard after close")
	err = pub.Close()
	requireNoError(t, err, "pub close after bridge close")
	err = sub.Close()
	requireNoError(t, err, "sub close after bridge close")
	err = b.Close()
	requireNoError(t, err, "second bridge close")
}

// TestSecurityClosedBridge verifies use after Close on every method.
func TestSecurityClosedBridge(t *testing.T) {
	t.Parallel()
	b, n, qos, pub, sub, svc, client := setupClosedBridge(t)
	ctx, cancel := context.WithTimeout(context.Background(), securityTimeout)
	defer cancel()
	checkClosedBridgeDiscovery(t, b, ctx)
	checkClosedBridgeNodes(t, b)
	checkClosedBridgePubSub(t, b, n, qos)
	checkClosedBridgeServices(t, b)
	checkClosedBridgeHandles(t, ctx, pub, sub, svc, client)
	checkClosedBridgeSentinels(t, b, pub, sub, svc, client)
}
