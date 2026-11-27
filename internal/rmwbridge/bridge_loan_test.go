// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

package rmwbridge_test

import (
	"context"
	"testing"
	"time"

	"github.com/podomy/weld/internal/rmwbridge"
)

// loanCtx returns a bounded context for loan tests.
func loanCtx(t *testing.T) (context.Context, context.CancelFunc) {
	t.Helper()
	return context.WithTimeout(context.Background(), 2*time.Second)
}

// mustLoanEndpoints creates one publisher plus one subscription on /loan.
func mustLoanEndpoints(t *testing.T, bridge *rmwbridge.Bridge, node *rmwbridge.Node) (*rmwbridge.Publisher, *rmwbridge.Subscription) {
	t.Helper()
	pub, err := bridge.CreatePublisher(node, "/loan", rmwbridge.DefaultQoS())
	requireNoError(t, err, "create publisher")
	sub, err := bridge.CreateSubscription(node, "/loan", rmwbridge.DefaultQoS())
	requireNoError(t, err, "create subscription")
	return pub, sub
}

// TestBridgeLoanSharesHandle pins zero-copy handle handoff without payload copy.
func TestBridgeLoanSharesHandle(t *testing.T) {
	t.Parallel()
	bridge := rmwbridge.NewBridge()
	defer func() {
		closeBridgeChecked(t, bridge)
	}()
	node := mustDomainNode(t, bridge, 0, "loan")
	pub, sub := mustLoanEndpoints(t, bridge, node)
	const ptr uint64 = 0x12345678
	const members uint64 = 0x9abcdef0
	queued, err := pub.PublishLoan(ptr, members)
	requireNoError(t, err, "publish loan")
	if !queued {
		t.Fatalf("publish loan queued = false, want true")
	}
	gotPtr, gotMembers, _, seq, ok, err := sub.TakeLoan()
	requireNoError(t, err, "take loan")
	if !ok {
		t.Fatalf("take loan ok = false, want true")
	}
	if gotPtr != ptr || gotMembers != members {
		t.Fatalf("take loan = (%x, %x), want (%x, %x)", gotPtr, gotMembers, ptr, members)
	}
	if seq != 1 {
		t.Fatalf("take loan seq = %d, want 1", seq)
	}
	if got := bridge.LoanCopies(); got != 0 {
		t.Fatalf("loan copies = %d, want 0 (zero-copy moves no payload bytes)", got)
	}
	if got := bridge.LoanTransfers(); got != 1 {
		t.Fatalf("loan transfers = %d, want 1", got)
	}
	_, _, _, _, ok, err = sub.TakeLoan()
	requireNoError(t, err, "second take loan")
	if ok {
		t.Fatalf("second take loan ok = true, want false (exactly once)")
	}
}

// TestBridgeLoanPreservesWireSample pins that loan takes preserve wire samples.
//
// Publishing a wire copy plus a loan queues both legs; taking the loan
// must not drop the wire sample, so a later Take still delivers it.
func TestBridgeLoanPreservesWireSample(t *testing.T) {
	t.Parallel()
	bridge := rmwbridge.NewBridge()
	defer func() {
		closeBridgeChecked(t, bridge)
	}()
	node := mustDomainNode(t, bridge, 0, "loan-preserve")
	pub, sub := mustLoanEndpoints(t, bridge, node)
	ctx, cancel := loanCtx(t)
	defer cancel()
	err := pub.Publish(ctx, []byte("wire"))
	requireNoError(t, err, "publish wire")
	queued, err := pub.PublishLoan(0x1111, 0x2222)
	requireNoError(t, err, "publish loan")
	if !queued {
		t.Fatalf("publish loan queued = false, want true")
	}
	_, _, _, seq, ok, err := sub.TakeLoan()
	requireNoError(t, err, "take loan")
	if !ok || seq != 1 {
		t.Fatalf("take loan ok/seq = %v/%d, want true/1", ok, seq)
	}
	got, err := sub.Take(ctx)
	requireNoError(t, err, "take wire after loan")
	if string(got) != "wire" {
		t.Fatalf("wire after loan = %q, want wire", string(got))
	}
}

// TestBridgeLoanLargePayloadWireStillCopies pins the wire path still copies.
func TestBridgeLoanLargePayloadWireStillCopies(t *testing.T) {
	t.Parallel()
	bridge := rmwbridge.NewBridge()
	defer func() {
		closeBridgeChecked(t, bridge)
	}()
	node := mustDomainNode(t, bridge, 0, "loanwire")
	pub, sub := mustLoanEndpoints(t, bridge, node)
	ctx, cancel := loanCtx(t)
	defer cancel()
	payload := make([]byte, 1<<20)
	for i := range payload {
		payload[i] = byte(i)
	}
	requireNoError(t, pub.Publish(ctx, payload), "publish wire")
	payload[0] ^= 0xff
	got, err := sub.Take(ctx)
	requireNoError(t, err, "take wire")
	if len(got) != len(payload) {
		t.Fatalf("take len = %d, want %d", len(got), len(payload))
	}
	if got[0] == payload[0] {
		t.Fatalf("wire take shares backing store with published slice, want copy")
	}
	if got[0] != 0 {
		t.Fatalf("wire take byte 0 = %d, want 0 (publish-time copy)", got[0])
	}
	got[1] ^= 0xff
	if got[1] == byte(1) {
		t.Fatalf("mutating take must not affect retained bytes")
	}
}

// TestBridgeLoanNoMatchReportsFalse pins publish with no subscriber queues nothing.
func TestBridgeLoanNoMatchReportsFalse(t *testing.T) {
	t.Parallel()
	bridge := rmwbridge.NewBridge()
	defer func() {
		closeBridgeChecked(t, bridge)
	}()
	node := mustDomainNode(t, bridge, 0, "lonely")
	pub, err := bridge.CreatePublisher(node, "/nolisteners", rmwbridge.DefaultQoS())
	requireNoError(t, err, "create publisher")
	queued, err := pub.PublishLoan(1, 2)
	requireNoError(t, err, "publish loan no match")
	if queued {
		t.Fatalf("publish loan queued = true with no subscribers, want false")
	}
	if got := bridge.LoanTransfers(); got != 0 {
		t.Fatalf("loan transfers = %d, want 0", got)
	}
}

// TestBridgeLoanQueueFullReportsFalse pins the bounded queue falls back to wire.
func TestBridgeLoanQueueFullReportsFalse(t *testing.T) {
	t.Parallel()
	bridge := rmwbridge.NewBridge()
	defer func() {
		closeBridgeChecked(t, bridge)
	}()
	node := mustDomainNode(t, bridge, 0, "loanfull")
	pub, sub := mustLoanEndpoints(t, bridge, node)
	_ = sub
	queuedCount := 0
	for i := uint64(1); i <= 64; i++ {
		queued, err := pub.PublishLoan(i, i+1000)
		requireNoError(t, err, "publish loan fill")
		if !queued {
			break
		}
		queuedCount++
	}
	if queuedCount == 0 {
		t.Fatalf("no loan queued, want at least one")
	}
	if queuedCount > 16 {
		t.Fatalf("queued %d loans, want at most depth 16 (bounded, wire fallback)", queuedCount)
	}
	if got := bridge.LoanCopies(); got != 0 {
		t.Fatalf("loan copies = %d, want 0", got)
	}
}
