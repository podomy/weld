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
