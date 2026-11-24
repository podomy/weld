// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

package rmwbridge_test

import (
	"context"
	"errors"
	"testing"

	"github.com/podomy/weld/internal/rmwbridge"
)

// closeSecurityBridge closes a security test bridge and fails on error.
func closeSecurityBridge(t *testing.T, b *rmwbridge.Bridge) {
	t.Helper()
	err := b.Close()
	if err != nil {
		t.Fatalf("close: %v", err)
	}
}

// TestSecurityGenerationRollback verifies stale generations are rejected.
func TestSecurityGenerationRollback(t *testing.T) {
	t.Parallel()
	b := newSecurityBridge(t)
	defer func() {
		closeSecurityBridge(t, b)
	}()
	ctx, cancel := context.WithTimeout(context.Background(), securityTimeout)
	defer cancel()
	rep := func(gen uint64, alive bool) error {
		return b.ReportLiveliness(ctx, rmwbridge.LivelinessReport{
			ParticipantName: "p1",
			Generation:      gen,
			Alive:           alive,
		})
	}
	if err := rep(5, true); err != nil {
		t.Fatalf("gen 5: %v", err)
	}
	// Idempotent repeat with same generation and aliveness.
	if err := rep(5, true); err != nil {
		t.Fatalf("repeat gen 5 alive=true: %v, want nil", err)
	}
	// Same generation with flipped aliveness is stale.
	if err := rep(5, false); !errors.Is(err, rmwbridge.ErrStaleGeneration) {
		t.Fatalf("gen 5 alive flip = %v, want ErrStaleGeneration", err)
	}
	// Backward step is stale.
	if err := rep(4, true); !errors.Is(err, rmwbridge.ErrStaleGeneration) {
		t.Fatalf("gen rollback 5->4 = %v, want ErrStaleGeneration", err)
	}
	// Zero after progress is stale.
	if err := rep(0, true); !errors.Is(err, rmwbridge.ErrStaleGeneration) {
		t.Fatalf("gen rollback 5->0 = %v, want ErrStaleGeneration", err)
	}
	// Forward step succeeds.
	if err := rep(6, false); err != nil {
		t.Fatalf("gen 6: %v", err)
	}
	// New participant starts fresh at any generation.
	if err := b.ReportLiveliness(ctx, rmwbridge.LivelinessReport{
		ParticipantName: "p2",
		Generation:      0,
	}); err != nil {
		t.Fatalf("new participant gen 0: %v", err)
	}
}
