// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

package node_test

import (
	"context"
	"testing"

	"github.com/podomy/weld/node"
)

// requireNoError fails the test when err is not nil.
func requireNoError(t *testing.T, err error, msg string) {
	t.Helper()
	if err != nil {
		t.Fatalf("%s: %v", msg, err)
	}
}

// requireTrue fails the test when cond is false.
func requireTrue(t *testing.T, cond bool, msg string) {
	t.Helper()
	if !cond {
		t.Fatalf("%s", msg)
	}
}

// TestCreateEndpoint checks endpoint creation plus delivery gating through
// the node surface.
func TestCreateEndpoint(t *testing.T) {
	t.Parallel()

	publisher, err := node.CreateEndpoint("/chatter", "pub", node.EndpointPublisher, node.DefaultQoS(), nil, false)
	requireNoError(t, err, "create publisher")
	if publisher.Topic() != "/chatter" {
		t.Fatalf("publisher topic = %q, want %q", publisher.Topic(), "/chatter")
	}
	subscriber, err := node.CreateEndpoint("/chatter", "sub", node.EndpointSubscription, node.DefaultQoS(), nil, false)
	requireNoError(t, err, "create subscriber")
	requireTrue(t, node.EndpointsCanDeliver(&publisher, &subscriber), "default pair cannot deliver")
	report := node.CheckEndpointPair(&publisher, &subscriber)
	requireTrue(t, report.Compatible, "default pair incompatible: "+report.Reason())
}

// TestServiceRoundTrip checks one client call flowing through a server take
// plus reply back to the client through the node surface.
func TestServiceRoundTrip(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	client := node.NewServiceClient(nil, false, nil)
	server := node.NewServiceServer(nil, false, nil)
	var guid [8]byte
	guid[0] = 7
	timeout := node.UnspecifiedDuration()

	err := client.BeginCall(ctx, "/add_two_ints", guid, 1, timeout)
	requireNoError(t, err, "begin call")
	err = server.EnqueueRequest(ctx, "/add_two_ints", guid, 1, timeout)
	requireNoError(t, err, "enqueue request")
	request, ok := server.TakeRequest()
	requireTrue(t, ok, "take request missed")
	want := node.ServiceCallKey{Service: "/add_two_ints", Client: guid, Sequence: 1}
	if request.Key() != want {
		t.Fatalf("request key = %+v, want %+v", request.Key(), want)
	}
	err = server.Reply("/add_two_ints", guid, 1)
	requireNoError(t, err, "reply")
	err = client.CompleteCall("/add_two_ints", guid, 1)
	requireNoError(t, err, "complete call")
	if client.PendingCount() != 0 {
		t.Fatalf("pending = %d, want 0", client.PendingCount())
	}
}

// TestConfigStoreDeclareGet checks parameter declare plus typed read back
// through the node surface.
func TestConfigStoreDeclareGet(t *testing.T) {
	t.Parallel()

	store, err := node.NewConfigStore(node.ConfigStoreOptions{})
	requireNoError(t, err, "new config store")
	err = store.Declare("speed", node.NewIntegerParam(3), node.ParamDescriptor{})
	requireNoError(t, err, "declare")
	got, err := store.Get("speed")
	requireNoError(t, err, "get")
	if got.Type != node.ParamTypeInteger || got.Integer != 3 {
		t.Fatalf("value = %+v, want integer 3", got)
	}
}
