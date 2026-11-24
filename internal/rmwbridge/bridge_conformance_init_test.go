// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

package rmwbridge_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/podomy/weld/internal/rmwbridge"
	"github.com/podomy/weld/internal/types"
)

// newConformanceBridge returns a bridge that is closed on cleanup.
func newConformanceBridge(t *testing.T) *rmwbridge.Bridge {
	t.Helper()
	bridge := rmwbridge.NewBridge()
	t.Cleanup(func() {
		err := bridge.Close()
		if err != nil {
			t.Fatalf("close: %v", err)
		}
	})
	return bridge
}

// joinConformanceBridge joins discovery with a timeout context.
func joinConformanceBridge(t *testing.T, bridge *rmwbridge.Bridge, name string, domain uint32) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err := bridge.JoinDiscovery(ctx, rmwbridge.DiscoveryInfo{ParticipantName: name, DomainID: domain})
	if err != nil {
		t.Fatalf("join discovery: %v", err)
	}
}

// mustConformanceNode creates a node or fails the test.
func mustConformanceNode(t *testing.T, bridge *rmwbridge.Bridge, name string) *rmwbridge.Node {
	t.Helper()
	node, err := bridge.CreateNode(name)
	if err != nil {
		t.Fatalf("create node %q: %v", name, err)
	}
	return node
}

// conformanceHash builds a valid RIHS01 hash string for a one-field type.
func conformanceHash(t *testing.T, typeName string) string {
	t.Helper()
	desc := types.TypeDescription{
		Description: types.IndividualTypeDescription{
			TypeName: typeName,
			Fields: []types.Field{
				{Name: "data", Type: types.FieldType{TypeID: types.FieldTypeString}},
			},
		},
		Referenced: []types.IndividualTypeDescription{},
	}
	hash, err := types.ComputeTypeHash(desc)
	if err != nil {
		t.Fatalf("compute hash for %q: %v", typeName, err)
	}
	return hash.String()
}

// requireNoError fails when err is not nil.
func requireNoError(t *testing.T, err error, op string) {
	t.Helper()
	if err != nil {
		t.Fatalf("%s: %v", op, err)
	}
}

// requireError fails when err is nil.
func requireError(t *testing.T, err error, op string) {
	t.Helper()
	if err == nil {
		t.Fatalf("%s succeeded, want error", op)
	}
}

// requireErrorIs fails unless err wraps target.
func requireErrorIs(t *testing.T, err error, target error, op string) {
	t.Helper()
	if err == nil {
		t.Fatalf("%s succeeded, want %v", op, target)
	}
	if errors.Is(err, target) == false {
		t.Fatalf("%s = %v, want %v", op, err, target)
	}
}

// requireTimeout fails unless err is deadline exceeded or canceled.
func requireTimeout(t *testing.T, err error, op string) {
	t.Helper()
	if err == nil {
		t.Fatalf("%s succeeded, want context timeout", op)
	}
	if errors.Is(err, context.DeadlineExceeded) == false && errors.Is(err, context.Canceled) == false {
		t.Fatalf("%s = %v, want context timeout", op, err)
	}
}

// checkInitIdentity pins identifier and version strings.
func checkInitIdentity(t *testing.T) {
	t.Helper()
	if got := rmwbridge.Identifier(); got != "rmw_weld" {
		t.Fatalf("identifier = %q, want rmw_weld", got)
	}
	if got := rmwbridge.Version(); got == "" {
		t.Fatalf("version is empty")
	}
}

// checkInitBeforeJoin pins publish, report, and take before join.
func checkInitBeforeJoin(t *testing.T, bridge *rmwbridge.Bridge, ctx context.Context) {
	t.Helper()
	err := bridge.PublishBytes(ctx, rmwbridge.OutgoingMessage{TopicName: "/t", TypeName: "x"})
	requireErrorIs(t, err, rmwbridge.ErrNotJoined, "publish before join")
	err = bridge.ReportLiveliness(ctx, rmwbridge.LivelinessReport{ParticipantName: "p", DomainID: 0, Alive: true, Generation: 1})
	requireErrorIs(t, err, rmwbridge.ErrNotJoined, "report before join")
	_, err = bridge.TakeBytes(ctx)
	requireErrorIs(t, err, rmwbridge.ErrNotJoined, "take before join")
}

// checkInitJoinDomains pins initial join and double join domain update.
func checkInitJoinDomains(t *testing.T, bridge *rmwbridge.Bridge, ctx context.Context) {
	t.Helper()
	err := bridge.JoinDiscovery(ctx, rmwbridge.DiscoveryInfo{ParticipantName: "p1", DomainID: 3})
	requireNoError(t, err, "join discovery")
	if got := bridge.DomainID(); got != 3 {
		t.Fatalf("domain = %d, want 3", got)
	}
	err = bridge.JoinDiscovery(ctx, rmwbridge.DiscoveryInfo{ParticipantName: "p1", DomainID: 7})
	requireNoError(t, err, "double join")
	if got := bridge.DomainID(); got != 7 {
		t.Fatalf("domain after double join = %d, want 7", got)
	}
}

// checkInitCanceledJoin pins join with a canceled context.
func checkInitCanceledJoin(t *testing.T, bridge *rmwbridge.Bridge) {
	t.Helper()
	canceled, stop := context.WithCancel(context.Background())
	stop()
	err := bridge.JoinDiscovery(canceled, rmwbridge.DiscoveryInfo{ParticipantName: "p1", DomainID: 7})
	requireError(t, err, "join with canceled context")
}

// checkInitCloseAfterClose pins idempotent close and use after close.
func checkInitCloseAfterClose(t *testing.T, bridge *rmwbridge.Bridge, ctx context.Context) {
	t.Helper()
	err := bridge.Close()
	requireNoError(t, err, "close")
	err = bridge.Close()
	requireNoError(t, err, "second close")
	err = bridge.JoinDiscovery(ctx, rmwbridge.DiscoveryInfo{ParticipantName: "p1", DomainID: 7})
	requireErrorIs(t, err, rmwbridge.ErrBridgeClosed, "join after close")
	_, err = bridge.CreateNode("late")
	requireErrorIs(t, err, rmwbridge.ErrBridgeClosed, "create node after close")
}

// TestConformanceInit covers bridge init and discovery join.
func TestConformanceInit(t *testing.T) {
	t.Run("init join", func(t *testing.T) {
		t.Parallel()
		checkInitIdentity(t)
		bridge := newConformanceBridge(t)
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		checkInitBeforeJoin(t, bridge, ctx)
		checkInitJoinDomains(t, bridge, ctx)
		checkInitCanceledJoin(t, bridge)
		checkInitCloseAfterClose(t, bridge, ctx)
	})
}
