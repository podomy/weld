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

// serveOneReply serves one request and replies with a prefix.
func serveOneReply(svc *rmwbridge.Service, serverErr chan error) {
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

// checkServiceCallReply pins client Call against server Serve.
func checkServiceCallReply(t *testing.T) {
	t.Helper()
	bridge := newConformanceBridge(t)
	svc, err := bridge.CreateService("add_two_ints")
	requireNoError(t, err, "create service")
	client, err := bridge.CreateClient("add_two_ints")
	requireNoError(t, err, "create client")
	if client.Exists() == false {
		t.Fatalf("client exists = false, want true")
	}
	if svc.Exists() == false {
		t.Fatalf("service exists = false, want true")
	}
	serverErr := make(chan error, 1)
	go serveOneReply(svc, serverErr)
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

// checkServiceAvailabilityBefore pins call before server exists.
func checkServiceAvailabilityBefore(t *testing.T, client *rmwbridge.Client) {
	t.Helper()
	if client.Exists() {
		t.Fatalf("client exists before server, want false")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, err := client.Call(ctx, []byte("ping"))
	requireErrorIs(t, err, rmwbridge.ErrServiceNotAvailable, "call without server")
}

// checkServiceAvailabilityDuplicates pins shared queue handles on
// duplicate create: a second server or client for a name succeeds and
// shares the same queue instead of reporting ErrServiceExists or
// ErrClientExists. The shared handles stay live until the destroy step
// below closes every handle for the name.
func checkServiceAvailabilityDuplicates(t *testing.T, bridge *rmwbridge.Bridge) {
	t.Helper()
	extra, err := bridge.CreateService("late_service")
	requireNoError(t, err, "second service shares queue")
	if !extra.Exists() {
		t.Fatalf("extra service exists = false, want true")
	}
	extraClient, err := bridge.CreateClient("late_service")
	requireNoError(t, err, "second client shares service")
	if !extraClient.Exists() {
		t.Fatalf("extra client exists = false, want true")
	}
}

// checkServiceAvailabilityDestroy pins destroy and second destroy paths.
func checkServiceAvailabilityDestroy(t *testing.T, bridge *rmwbridge.Bridge, svc *rmwbridge.Service, client *rmwbridge.Client) {
	t.Helper()
	err := bridge.DestroyService("late_service")
	requireNoError(t, err, "destroy service")
	if svc.Exists() {
		t.Fatalf("service exists after destroy, want false")
	}
	if client.Exists() {
		t.Fatalf("client exists after server destroy, want false")
	}
	err = bridge.DestroyService("late_service")
	requireErrorIs(t, err, rmwbridge.ErrServiceNotFound, "second destroy service")
	err = bridge.DestroyClient("late_service")
	requireNoError(t, err, "destroy client")
	err = bridge.DestroyClient("late_service")
	requireErrorIs(t, err, rmwbridge.ErrClientNotFound, "second destroy client")
}

// checkServiceAvailability pins Exists before and after lifecycle.
func checkServiceAvailability(t *testing.T) {
	t.Helper()
	bridge := newConformanceBridge(t)
	client, err := bridge.CreateClient("late_service")
	requireNoError(t, err, "create client")
	checkServiceAvailabilityBefore(t, client)
	svc, err := bridge.CreateService("late_service")
	requireNoError(t, err, "create service")
	if svc.Exists() == false {
		t.Fatalf("service exists = false, want true")
	}
	if client.Exists() == false {
		t.Fatalf("client exists after server create = false, want true")
	}
	checkServiceAvailabilityDuplicates(t, bridge)
	checkServiceAvailabilityDestroy(t, bridge, svc, client)
}

// checkServiceDeadlineCalls pins timeout passthrough on calls.
func checkServiceDeadlineCalls(t *testing.T, svc *rmwbridge.Service, client *rmwbridge.Client) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, err := client.Call(ctx, []byte("request"))
	requireTimeout(t, err, "call without responder")
	ctx2, cancel2 := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel2()
	_, err = svc.Call(ctx2, []byte("request"))
	requireTimeout(t, err, "service call without responder")
	canceled, stop := context.WithCancel(context.Background())
	stop()
	_, err = client.Call(canceled, []byte("request"))
	requireErrorIs(t, err, context.Canceled, "canceled call")
}

// checkServiceDeadlineServe pins timeout on Serve with no request.
func checkServiceDeadlineServe(t *testing.T, bridge *rmwbridge.Bridge) {
	t.Helper()
	serveSvc, err := bridge.CreateService("idle_service")
	requireNoError(t, err, "create idle service")
	serveCtx, cancelServe := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancelServe()
	_, _, err = serveSvc.Serve(serveCtx)
	requireTimeout(t, err, "serve without request")
}

// checkServiceDeadlineCancel pins context timeout on Call and Serve.
func checkServiceDeadlineCancel(t *testing.T) {
	t.Helper()
	bridge := newConformanceBridge(t)
	svc, err := bridge.CreateService("slow_service")
	requireNoError(t, err, "create service")
	client, err := bridge.CreateClient("slow_service")
	requireNoError(t, err, "create client")
	checkServiceDeadlineCalls(t, svc, client)
	checkServiceDeadlineServe(t, bridge)
}

// TestConformanceService covers service call paths.
func TestConformanceService(t *testing.T) {
	t.Run("service call reply", func(t *testing.T) {
		t.Parallel()
		checkServiceCallReply(t)
	})

	t.Run("service availability", func(t *testing.T) {
		t.Parallel()
		checkServiceAvailability(t)
	})

	t.Run("service deadline cancel", func(t *testing.T) {
		t.Parallel()
		checkServiceDeadlineCancel(t)
	})
}
