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

// serveRoundtripReply serves one request for the roundtrip test.
func serveRoundtripReply(svc *rmwbridge.Service, serverErr chan error) {
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

// checkServiceRoundtripCall pins client call and destroy paths.
func checkServiceRoundtripCall(t *testing.T, bridge *rmwbridge.Bridge, client *rmwbridge.Client, serverErr chan error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	resp, err := client.Call(ctx, []byte("request"))
	requireNoError(t, err, "client call")
	if string(resp) != "reply:request" {
		t.Fatalf("reply = %q, want reply:request", string(resp))
	}
	err = <-serverErr
	requireNoError(t, err, "serve")
	err = bridge.DestroyClient("add_two_ints")
	requireNoError(t, err, "destroy client")
	err = bridge.DestroyService("add_two_ints")
	requireNoError(t, err, "destroy service")
}

// TestServiceClientRoundtrip covers client Call against server Serve.
func TestServiceClientRoundtrip(t *testing.T) {
	t.Parallel()
	bridge := rmwbridge.NewBridge()
	defer func() {
		closeBridgeChecked(t, bridge)
	}()
	svc, err := bridge.CreateService("add_two_ints")
	requireNoError(t, err, "create service")
	client, err := bridge.CreateClient("add_two_ints")
	requireNoError(t, err, "create client")
	if !client.Exists() {
		t.Fatalf("client exists = false, want true")
	}
	serverErr := make(chan error, 1)
	go serveRoundtripReply(svc, serverErr)
	checkServiceRoundtripCall(t, bridge, client, serverErr)
}

// checkSvcAvailabilityBefore pins client exists false before server.
func checkSvcAvailabilityBefore(t *testing.T, client *rmwbridge.Client) {
	t.Helper()
	if client.Exists() {
		t.Fatalf("client exists before server, want false")
	}
}

// checkSvcAvailabilityAfter pins exists after create.
func checkSvcAvailabilityAfter(t *testing.T, client *rmwbridge.Client, svc *rmwbridge.Service) {
	t.Helper()
	if !svc.Exists() {
		t.Fatalf("service exists = false, want true")
	}
	if !client.Exists() {
		t.Fatalf("client exists after server create = false, want true")
	}
}

// checkSvcAvailabilityDestroy pins destroy and second destroy.
func checkSvcAvailabilityDestroy(t *testing.T, bridge *rmwbridge.Bridge, svc *rmwbridge.Service, client *rmwbridge.Client) {
	t.Helper()
	err := bridge.DestroyService("late_service")
	requireNoError(t, err, "destroy service")
	if svc.Exists() {
		t.Fatalf("service exists after destroy, want false")
	}
	if client.Exists() {
		t.Fatalf("client exists after server destroy, want false")
	}
	err = bridge.DestroyClient("late_service")
	requireNoError(t, err, "destroy client")
	err = bridge.DestroyClient("late_service")
	requireError(t, err, "second destroy client")
}

// TestServiceAvailability covers Exists before create, after create, and
// after destroy on both server and client handles.
func TestServiceAvailability(t *testing.T) {
	t.Parallel()
	bridge := rmwbridge.NewBridge()
	defer func() {
		closeBridgeChecked(t, bridge)
	}()
	client, err := bridge.CreateClient("late_service")
	requireNoError(t, err, "create client")
	checkSvcAvailabilityBefore(t, client)
	svc, err := bridge.CreateService("late_service")
	requireNoError(t, err, "create service")
	checkSvcAvailabilityAfter(t, client, svc)
	_, err = bridge.CreateClient("late_service")
	requireError(t, err, "duplicate create client")
	checkSvcAvailabilityDestroy(t, bridge, svc, client)
}

// TestServiceDeadlineCancel covers context timeout passthrough on Call
// with no responder and on an already canceled context.
func TestServiceDeadlineCancel(t *testing.T) {
	t.Parallel()
	bridge := rmwbridge.NewBridge()
	defer func() {
		closeBridgeChecked(t, bridge)
	}()
	svc, err := bridge.CreateService("slow_service")
	requireNoError(t, err, "create service")
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, err = svc.Call(ctx, []byte("request"))
	requireTimeout(t, err, "call without responder")
	canceled, stop := context.WithCancel(context.Background())
	stop()
	_, err = svc.Call(canceled, []byte("request"))
	requireErrorIs(t, err, context.Canceled, "canceled call")
}

// requireQoSDepth fails when a QoS depth differs from want.
func requireQoSDepth(t *testing.T, got string, want string, op string) {
	t.Helper()
	if got != want {
		t.Fatalf("%s depth = %q, want %s", op, got, want)
	}
}

// checkDualQoSDefaults pins default request and reply depths.
func checkDualQoSDefaults(t *testing.T, svc *rmwbridge.Service) {
	t.Helper()
	requireQoSDepth(t, svc.RequestActualQoS().Depth, "10", "default request")
	requireQoSDepth(t, svc.ReplyActualQoS().Depth, "10", "default reply")
}

// checkDualQoSSplit pins split request and reply depths.
func checkDualQoSSplit(t *testing.T, svc2 *rmwbridge.Service) {
	t.Helper()
	requireQoSDepth(t, svc2.RequestActualQoS().Depth, "5", "request")
	requireQoSDepth(t, svc2.ReplyActualQoS().Depth, "7", "reply")
	requireQoSDepth(t, svc2.RequestSubscriptionActualQoS().Depth, "5", "request subscription")
	requireQoSDepth(t, svc2.ResponsePublisherActualQoS().Depth, "7", "response publisher")
}

// checkDualQoSClient pins client request and reply depths.
func checkDualQoSClient(t *testing.T, client *rmwbridge.Client) {
	t.Helper()
	requireQoSDepth(t, client.RequestActualQoS().Depth, "3", "client request")
	requireQoSDepth(t, client.ReplyActualQoS().Depth, "3", "client reply")
	requireQoSDepth(t, client.RequestPublisherActualQoS().Depth, "3", "client request publisher")
	requireQoSDepth(t, client.ResponseSubscriptionActualQoS().Depth, "3", "client response subscription")
}

// TestServiceDualQoS covers fanout of one input and storage of two inputs.
func TestServiceDualQoS(t *testing.T) {
	t.Parallel()
	bridge := rmwbridge.NewBridge()
	defer func() {
		closeBridgeChecked(t, bridge)
	}()
	svc, err := bridge.CreateService("qos_default")
	requireNoError(t, err, "create service")
	checkDualQoSDefaults(t, svc)
	svc2, err := bridge.CreateService("qos_split", rmwbridge.QoS{Depth: "5"}, rmwbridge.QoS{Depth: "7"})
	requireNoError(t, err, "create service with qos")
	checkDualQoSSplit(t, svc2)
	client, err := bridge.CreateClient("qos_client", rmwbridge.QoS{Depth: "3"})
	requireNoError(t, err, "create client")
	checkDualQoSClient(t, client)
}
