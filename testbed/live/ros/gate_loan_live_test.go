// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

// Package ros_test holds the loaned delivery live gates.
//
// These gates prove cross-vendor honesty (a Cyclone peer asking for loans
// gets a truthful false while the exchange still flows via copies) and
// in-process zero-copy (a Weld-to-Weld loaned exchange with no copies).
// They use exclusive domains from the parallel pool plus per-run topics so
// concurrent runs never mix.
package ros_test

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"github.com/podomy/weld/internal/transports/dds"
	"github.com/podomy/weld/internal/types"
	"github.com/podomy/weld/testbed/live/helpers"
)

// Loan live gate bounds.
const (
	// loanLiveTimeout bounds each loan gate context.
	loanLiveTimeout = 2 * time.Minute
	// loanLiveTakeTimeout bounds one TakeBytes wait for the ROS talker.
	loanLiveTakeTimeout = 20 * time.Second
	// loanLiveSettle waits for discovery before starting the talker.
	loanLiveSettle = 2 * time.Second
	// loanLiveType is the IDL name for the forward String exchange.
	loanLiveType = "std_msgs__msg__String"
)

// loanLiveTable builds an all scalar primitive table that may loan.
func loanLiveTable() *types.MessageMembers {
	members := []types.MessageMember{
		types.NewScalarMember("a", types.RosTypeInt32, 0),
		types.NewScalarMember("b", types.RosTypeFloat64, 8),
	}
	initFn := func(base any, init types.MessageInit) error { return nil }
	finiFn := func(base any) error { return nil }
	return types.NewMessageMembers("loan__msg", "LiveEligible", 16, members, initFn, finiFn)
}

// decodeLoanLiveString decodes one XCDR1 String payload or fails the test.
func decodeLoanLiveString(t *testing.T, wire []byte) string {
	t.Helper()
	dec, err := types.NewDecoder(wire)
	if err != nil {
		t.Fatalf("decode loan string header: %v", err)
	}
	got, err := dec.ReadString()
	if err != nil {
		t.Fatalf("decode loan string: %v", err)
	}
	if err := dec.Finish(); err != nil {
		t.Fatalf("decode loan string finish: %v", err)
	}
	return got
}

// TestLoanCrossVendorHonesty proves a Cyclone loan request gets a truthful
// false while the exchange still flows via copies.
//
// It subscribes on a per-run topic, starts a ROS talker on that same
// topic, and takes until the live ID arrives. A timeout fails loudly:
// the negotiation answer is already pinned false above, so a missing
// payload is a copy-path failure, never a silent skip.
func TestLoanCrossVendorHonesty(t *testing.T) {
	t.Parallel()
	domain := helpers.MustAllocateDomain(t)
	ctx, cancel := context.WithTimeout(context.Background(), loanLiveTimeout)
	defer cancel()
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)

	local := loanLiveTable()
	cyclone := dds.LoanPeerOffer{WantsLoan: true, InProcess: false, Vendor: "cyclonedds"}
	ok, reason := dds.NegotiateLoan(local, cyclone)
	if ok {
		t.Fatalf("cyclone loan offer negotiated true, want false with reason")
	}
	if reason == "" {
		t.Fatal("cyclone refusal carries empty reason")
	}
	if !strings.Contains(reason, "cop") {
		t.Fatalf("cyclone refusal %q must say the exchange copies", reason)
	}

	live := helpers.LiveID("loan")
	topic := helpers.LiveTopic(live)
	payload := "hello loan " + live
	backend := helpers.MustOpenLiveBackend(t, ctx, uint32(domain), "loan-honesty", "loan") //nolint:gosec // G115: allocated domain fits uint32
	err := helpers.SubscribeLiveReader(ctx, backend, topic, loanLiveType, "", "loan")
	if err != nil {
		t.Fatalf("subscribe %q: %v", topic, err)
	}
	helpers.Sleep(ctx, loanLiveSettle)
	pubArg := "{data: " + payload + "}"
	err = helpers.StartPeerProc(ctx, helpers.Container, domain, live, helpers.RosBin, "topic", "pub", topic, helpers.RosType, pubArg, "--rate", "2")
	if err != nil {
		t.Fatalf("start ROS talker: %v", err)
	}
	pidPath := helpers.HostPidPath(t, live)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.WithoutCancel(ctx), loanLiveSettle)
		defer cleanupCancel()
		helpers.StopPeerProc(cleanupCtx, helpers.Container, domain, pidPath, topic)
	})
	takeCtx, takeCancel := context.WithTimeout(ctx, loanLiveTakeTimeout)
	defer takeCancel()
	skips := 0
	for {
		msg, terr := backend.TakeBytes(takeCtx)
		if terr != nil {
			t.Fatalf("weld take timed out waiting for ROS talker %q after %d skips: %v", payload, skips, terr)
		}
		if msg.TopicName != topic {
			skips++
			continue
		}
		if decodeLoanLiveString(t, msg.Payload) != payload {
			skips++
			continue
		}
		t.Logf("cross-vendor copy path ok on %q skips %d with loan refused: %q", topic, skips, reason)
		return
	}
}

// TestLoanInProcessZeroCopy proves a Weld-to-Weld loaned exchange moves
// payload bytes with zero copies on the eligible path.
//
// Borrow fills one pooled buffer, Send transfers the handle, and Recv
// observes identical bytes while the channel copy counter stays zero.
// The negotiation for the same pair answers true, pinning the eligible
// path to the loan instead of a copy.
//
//nolint:cyclop // live gate sequences borrow send recv and counters in one run, splitting would obscure intent
func TestLoanInProcessZeroCopy(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), loanLiveTimeout)
	defer cancel()
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)

	local := loanLiveTable()
	weld := dds.LoanPeerOffer{WantsLoan: true, InProcess: true, Vendor: dds.LoanVendorWeld}
	ok, reason := dds.NegotiateLoan(local, weld)
	if !ok {
		t.Fatalf("in-process weld offer negotiated false: %q", reason)
	}
	pool, err := dds.NewLoanPool(4, 64)
	if err != nil {
		t.Fatalf("new pool: %v", err)
	}
	ch, err := dds.NewLoanChannel(4)
	if err != nil {
		t.Fatalf("new channel: %v", err)
	}
	want := []byte{0xDE, 0xAD, 0xBE, 0xEF, 0x01, 0x02, 0x03, 0x04}
	loan, err := pool.Borrow(local, len(want))
	if err != nil {
		t.Fatalf("borrow: %v", err)
	}
	view, err := loan.Bytes()
	if err != nil {
		t.Fatalf("bytes: %v", err)
	}
	copy(view, want)
	if err := ch.Send(loan); err != nil {
		t.Fatalf("send: %v", err)
	}
	got, err := ch.Recv(ctx)
	if err != nil {
		t.Fatalf("recv: %v", err)
	}
	payload, err := got.Bytes()
	if err != nil {
		t.Fatalf("bytes: %v", err)
	}
	if !bytes.Equal(payload, want) {
		t.Fatalf("payload = %v, want %v", payload, want)
	}
	if ch.Transfers() != 1 {
		t.Fatalf("transfers = %d, want 1", ch.Transfers())
	}
	if ch.Copies() != 0 {
		t.Fatalf("copies = %d, want 0 on zero-copy path", ch.Copies())
	}
	if err := got.Return(); err != nil {
		t.Fatalf("return: %v", err)
	}
	if pool.Returns() != 1 {
		t.Fatalf("pool returns = %d, want 1", pool.Returns())
	}
}
