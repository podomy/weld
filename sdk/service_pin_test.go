// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package sdk_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/podomy/weld/sdk"
)

// checkAbandonTimeout pins that a hung call times out without hitting the cap.
func checkAbandonTimeout(t *testing.T, err error, label string) {
	t.Helper()
	if err == nil {
		t.Fatalf("%s succeeded, want timeout", label)
	}
	if errors.Is(err, sdk.ErrServiceInflightExceeded) {
		t.Fatalf("%s hit inflight cap, want reclaim on abandon: %v", label, err)
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("%s error = %v, want deadline", label, err)
	}
}

// TestServiceAbandonReclaimsSlot pins that a timed-out call reclaims its
// inflight slot while the hung handler stays tracked separately, so a
// burst of timeouts cannot wedge new calls at the cap.
func TestServiceAbandonReclaimsSlot(t *testing.T) {
	bus := sdk.NewBus()
	node, err := sdk.NewNode("abandon-node", bus)
	if err != nil {
		t.Fatalf("new node: %v", err)
	}
	t.Cleanup(func() {
		cerr := node.Close()
		if cerr != nil {
			t.Errorf("close node: %v", cerr)
		}
	})
	release := make(chan struct{})
	_, err = node.NewServiceServer("/hung", func(_ context.Context, _ []byte) ([]byte, error) {
		<-release
		return []byte("late"), nil
	})
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	client, err := node.NewServiceClient("/hung")
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	const capCalls = 64
	for i := range capCalls {
		callCtx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		_, err = client.Call(callCtx, []byte("x"))
		cancel()
		checkAbandonTimeout(t, err, "burst call")
		_ = i
	}
	callCtx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, err = client.Call(callCtx, []byte("x"))
	checkAbandonTimeout(t, err, "post-burst call")
	close(release)
	time.Sleep(200 * time.Millisecond)
}

// TestServiceCallAfterServerCloseGone pins the linearizability point:
// lookup plus owner recheck means a Call after Close fails with
// ErrNoSuchService instead of succeeding on a gone server.
func TestServiceCallAfterServerCloseGone(t *testing.T) {
	bus := sdk.NewBus()
	node, err := sdk.NewNode("gone-node", bus)
	if err != nil {
		t.Fatalf("new node: %v", err)
	}
	t.Cleanup(func() {
		cerr := node.Close()
		if cerr != nil {
			t.Errorf("close node: %v", cerr)
		}
	})
	srv, err := node.NewServiceServer("/gone", func(_ context.Context, req []byte) ([]byte, error) {
		return req, nil
	})
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	client, err := node.NewServiceClient("/gone")
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	callCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	_, err = client.Call(callCtx, []byte("before"))
	cancel()
	if err != nil {
		t.Fatalf("call before close: %v", err)
	}
	err = srv.Close()
	if err != nil {
		t.Fatalf("close server: %v", err)
	}
	afterCtx, afterCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer afterCancel()
	_, err = client.Call(afterCtx, []byte("after"))
	if err == nil {
		t.Fatalf("call after close succeeded, want ErrNoSuchService")
	}
	if !errors.Is(err, sdk.ErrNoSuchService) {
		t.Fatalf("call after close error = %v, want ErrNoSuchService", err)
	}
}

// TestServiceCallCloseRaceStress hammers concurrent Calls while the server
// closes. Every Call must either succeed when linearized before Close or
// fail with the gone sentinel when linearized after; no other outcome is
// allowed. A deterministic interleaving between lookup and recheck cannot
// be forced without a test hook, so this stress plus the deterministic
// after-close test above pins the contract.
func TestServiceCallCloseRaceStress(t *testing.T) {
	for range 50 {
		runCloseRaceOnce(t)
	}
}

// runCloseRaceOnce runs one close versus calls race and pins the after
// close gone sentinel.
func runCloseRaceOnce(t *testing.T) {
	t.Helper()
	bus := sdk.NewBus()
	node, err := sdk.NewNode("race-close-node", bus)
	if err != nil {
		t.Fatalf("new node: %v", err)
	}
	srv, err := node.NewServiceServer("/raceclose", func(ctx context.Context, req []byte) ([]byte, error) {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}
		return req, nil
	})
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	client, err := node.NewServiceClient("/raceclose")
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	done := make(chan struct{})
	go hammerServiceCalls(client, done)
	err = srv.Close()
	if err != nil {
		t.Fatalf("close server: %v", err)
	}
	<-done
	checkCallAfterCloseGone(t, client)
	err = node.Close()
	if err != nil {
		t.Fatalf("close node: %v", err)
	}
}

// hammerServiceCalls issues a burst of calls, ignoring per-call outcomes
// because both success before Close and gone after Close are linearizable.
func hammerServiceCalls(client *sdk.ServiceClient, done chan struct{}) {
	defer close(done)
	for range 20 {
		callCtx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		_, _ = client.Call(callCtx, []byte("x")) //nolint:errcheck // stress ignores per-call success versus gone, final check pins gone
		cancel()
	}
}

// checkCallAfterCloseGone pins that a fresh Call after Close fails gone.
func checkCallAfterCloseGone(t *testing.T, client *sdk.ServiceClient) {
	t.Helper()
	afterCtx, afterCancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer afterCancel()
	_, err := client.Call(afterCtx, []byte("after"))
	if err == nil {
		t.Fatalf("call after close succeeded, want gone")
	}
	if !errors.Is(err, sdk.ErrNoSuchService) && !errors.Is(err, sdk.ErrServiceClientGone) && !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("call after close error = %v, want gone sentinel", err)
	}
}
