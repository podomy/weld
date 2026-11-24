// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

package rmwbridge_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/podomy/weld/internal/rmwbridge"
)

// checkExtendedNodeCreateDestroy pins node lifecycle on a fresh bridge.
func checkExtendedNodeCreateDestroy(t *testing.T) {
	t.Helper()
	bridge := rmwbridge.NewBridge()
	defer func() {
		closeBridgeChecked(t, bridge)
	}()
	node, err := bridge.CreateNode("n1")
	requireNoError(t, err, "create node")
	if node.Name != "n1" {
		t.Fatalf("node name = %q, want n1", node.Name)
	}
	_, err = bridge.CreateNode("n1")
	requireError(t, err, "duplicate create node")
	err = bridge.DestroyNode("n1")
	requireNoError(t, err, "destroy node")
	err = bridge.DestroyNode("n1")
	requireError(t, err, "second destroy")
	_, err = bridge.CreateNode("")
	requireError(t, err, "empty node name")
}

// checkExtendedPubSubQoS pins stored QoS passthrough.
func checkExtendedPubSubQoS(t *testing.T, pub *rmwbridge.Publisher, sub *rmwbridge.Subscription) {
	t.Helper()
	if pub.QoS.Depth != "7" {
		t.Fatalf("publisher qos depth = %q, want 7", pub.QoS.Depth)
	}
	if sub.QoS.Depth != "7" {
		t.Fatalf("subscription qos depth = %q, want 7", sub.QoS.Depth)
	}
}

// checkExtendedPubSubRoundtrip pins one publish and take roundtrip.
func checkExtendedPubSubRoundtrip(t *testing.T, pub *rmwbridge.Publisher, sub *rmwbridge.Subscription) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err := pub.Publish(ctx, []byte("hello"))
	requireNoError(t, err, "publish")
	got, err := sub.Take(ctx)
	requireNoError(t, err, "take")
	if string(got) != "hello" {
		t.Fatalf("payload = %q, want hello", string(got))
	}
	err = sub.Close()
	requireNoError(t, err, "subscription close")
}

// checkExtendedPubSub pins pub sub roundtrip on a fresh bridge.
func checkExtendedPubSub(t *testing.T) {
	t.Helper()
	bridge := rmwbridge.NewBridge()
	defer func() {
		closeBridgeChecked(t, bridge)
	}()
	node, err := bridge.CreateNode("talker_node")
	requireNoError(t, err, "create node")
	qos := rmwbridge.QoS{Depth: "7"}
	pub, err := bridge.CreatePublisher(node, "/chatter", qos)
	requireNoError(t, err, "create publisher")
	sub, err := bridge.CreateSubscription(node, "/chatter", qos)
	requireNoError(t, err, "create subscription")
	checkExtendedPubSubQoS(t, pub, sub)
	checkExtendedPubSubRoundtrip(t, pub, sub)
}

// serveExtendedReply serves one request for the extended service test.
func serveExtendedReply(svc *rmwbridge.Service, serverErr chan error) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	req, respond, err := svc.Serve(ctx)
	if err != nil {
		serverErr <- err
		return
	}
	if string(req) != "request" {
		serverErr <- errors.New("bad request payload")
		return
	}
	err = respond([]byte("reply:" + string(req)))
	if err != nil {
		serverErr <- fmt.Errorf("respond: %w", err)
		return
	}
	serverErr <- nil
}

// checkExtendedServiceCall pins service call on a fresh bridge.
func checkExtendedServiceCall(t *testing.T) {
	t.Helper()
	bridge := rmwbridge.NewBridge()
	defer func() {
		closeBridgeChecked(t, bridge)
	}()
	svc, err := bridge.CreateService("add_two_ints")
	requireNoError(t, err, "create service")
	serverErr := make(chan error, 1)
	go serveExtendedReply(svc, serverErr)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	resp, err := svc.Call(ctx, []byte("request"))
	requireNoError(t, err, "service call")
	if string(resp) != "reply:request" {
		t.Fatalf("reply = %q, want reply:request", string(resp))
	}
	err = <-serverErr
	requireNoError(t, err, "serve")
}

// checkExtendedGuardTrigger pins guard trigger and wait.
func checkExtendedGuardTrigger(t *testing.T) {
	t.Helper()
	guard := rmwbridge.NewGuard()
	if guard.IsTriggered() {
		t.Fatalf("new guard is triggered")
	}
	guard.Trigger()
	if !guard.IsTriggered() {
		t.Fatalf("triggered guard reports false")
	}
	ws := rmwbridge.NewWaitSet()
	ws.AddGuard(guard)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err := ws.Wait(ctx)
	requireNoError(t, err, "wait after trigger")
	guard.Reset()
	if guard.IsTriggered() {
		t.Fatalf("reset guard still triggered")
	}
}

// checkExtendedWaitTimeout pins wait timeout on an untriggered guard.
func checkExtendedWaitTimeout(t *testing.T) {
	t.Helper()
	ws := rmwbridge.NewWaitSet()
	guard := rmwbridge.NewGuard()
	ws.AddGuard(guard)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	start := time.Now()
	err := ws.Wait(ctx)
	requireTimeout(t, err, "wait without trigger")
	if time.Since(start) > 2*time.Second {
		t.Fatalf("wait took too long")
	}
}

// TestBridgeExtended covers the in-memory backend registries.
func TestBridgeExtended(t *testing.T) {
	t.Parallel()

	t.Run("node create destroy", func(t *testing.T) {
		t.Parallel()
		checkExtendedNodeCreateDestroy(t)
	})

	t.Run("pub sub roundtrip", func(t *testing.T) {
		t.Parallel()
		checkExtendedPubSub(t)
	})

	t.Run("service call", func(t *testing.T) {
		t.Parallel()
		checkExtendedServiceCall(t)
	})

	t.Run("guard trigger", func(t *testing.T) {
		t.Parallel()
		checkExtendedGuardTrigger(t)
	})

	t.Run("wait timeout", func(t *testing.T) {
		t.Parallel()
		checkExtendedWaitTimeout(t)
	})
}
