// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package dds

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/podomy/weld/internal/transports"
	"github.com/podomy/weld/internal/types"
)

// defaultLoanTestTimeout bounds channel receives in loan tests.
const defaultLoanTestTimeout = 5 * time.Second

// newLoanedTestReceiver wraps a fresh DataReceiver with pool storage.
func newLoanedTestReceiver(t *testing.T, pool *LoanPool) *LoanedReceiver {
	t.Helper()
	recv, err := NewLoanedReceiver(NewDataReceiver(), pool)
	if err != nil {
		t.Fatalf("new loaned receiver: %v", err)
	}
	return recv
}

// loanStubInit is a no-op table init for loan fixtures.
func loanStubInit(base any, init types.MessageInit) error {
	return nil
}

// loanStubFini is a no-op table fini for loan fixtures.
func loanStubFini(base any) error {
	return nil
}

// loanEligibleTable builds an all scalar primitive table that may loan.
func loanEligibleTable() *types.MessageMembers {
	members := []types.MessageMember{
		types.NewScalarMember("a", types.RosTypeInt32, 0),
		types.NewScalarMember("b", types.RosTypeFloat64, 8),
	}
	return types.NewMessageMembers("loan__msg", "Eligible", 16, members, loanStubInit, loanStubFini)
}

// loanStringTable builds a table with one string member that must copy.
func loanStringTable() *types.MessageMembers {
	members := []types.MessageMember{
		types.NewStringMember("name", types.RosTypeString, 0, 0),
		types.NewScalarMember("b", types.RosTypeFloat64, 8),
	}
	return types.NewMessageMembers("loan__msg", "WithString", 16, members, loanStubInit, loanStubFini)
}

// loanSequenceTable builds a table with one sequence member that must copy.
func loanSequenceTable() *types.MessageMembers {
	members := []types.MessageMember{
		types.NewSequenceMember("dims", types.RosTypeDouble, 3, 0, "Dims"),
	}
	return types.NewMessageMembers("loan__msg", "WithSeq", 8, members, loanStubInit, loanStubFini)
}

// loanFixedArrayTable builds a table with one fixed array that must copy
// on the span path even though it holds no pointers.
func loanFixedArrayTable() *types.MessageMembers {
	members := []types.MessageMember{
		types.NewFixedArrayMember("cov", types.RosTypeDouble, 4, 0, "Cov"),
	}
	return types.NewMessageMembers("loan__msg", "WithFixed", 32, members, loanStubInit, loanStubFini)
}

// loanBoolVectorTable builds a table with one bool sequence that must copy.
func loanBoolVectorTable() *types.MessageMembers {
	members := []types.MessageMember{
		types.NewSequenceMember("flags", types.RosTypeBoolean, 0, 0, "Flags"),
	}
	return types.NewMessageMembers("loan__msg", "WithBoolVec", 8, members, loanStubInit, loanStubFini)
}

// loanNestedTable builds a nested all primitive table that may loan.
func loanNestedTable() *types.MessageMembers {
	inner := loanEligibleTable()
	members := []types.MessageMember{
		{Name: "sub", TypeID: types.RosTypeMessage, Members: inner, Offset: 0},
		types.NewScalarMember("c", types.RosTypeInt32, 8),
	}
	return types.NewMessageMembers("loan__msg", "Nested", 24, members, loanStubInit, loanStubFini)
}

// TestLoanPoolBounds checks the cap: borrows past capacity fall back to
// allocate and never block.
//
//nolint:gocognit,cyclop // test pins borrow, fallback, recycle, and hit breadth in one run, splitting would obscure intent
func TestLoanPoolBounds(t *testing.T) {
	t.Parallel()
	pool, err := NewLoanPool(2, 64)
	if err != nil {
		t.Fatalf("new pool: %v", err)
	}
	table := loanEligibleTable()
	first, err := pool.Borrow(table, 8)
	if err != nil {
		t.Fatalf("borrow one: %v", err)
	}
	second, err := pool.Borrow(table, 8)
	if err != nil {
		t.Fatalf("borrow two: %v", err)
	}
	third, err := pool.Borrow(table, 8)
	if err != nil {
		t.Fatalf("borrow past cap must fall back, got error: %v", err)
	}
	if !third.Pooled() {
		t.Fatal("fallback borrow at pool size must stay recyclable")
	}
	oversize, err := pool.Borrow(table, 128)
	if err != nil {
		t.Fatalf("oversize borrow: %v", err)
	}
	if oversize.Pooled() {
		t.Fatal("oversize fallback must not claim pooled storage")
	}
	if pool.Hits() != 0 {
		t.Fatalf("hits = %d, want 0 on empty pool", pool.Hits())
	}
	if pool.Fallbacks() != 4 {
		t.Fatalf("fallbacks = %d, want 4", pool.Fallbacks())
	}
	for _, loan := range []*Loan{first, second, third, oversize} {
		err = loan.Return()
		if err != nil {
			t.Fatalf("return: %v", err)
		}
	}
	if pool.Free() != 2 {
		t.Fatalf("free = %d, want capacity 2 after returns", pool.Free())
	}
	if pool.Returns() != 3 {
		t.Fatalf("returns = %d, want 3 pooled returns", pool.Returns())
	}
	fourth, err := pool.Borrow(table, 8)
	if err != nil {
		t.Fatalf("borrow after return: %v", err)
	}
	if !fourth.Pooled() {
		t.Fatal("borrow after return must hit recycled storage")
	}
	if pool.Hits() != 1 {
		t.Fatalf("hits = %d, want 1", pool.Hits())
	}
	err = fourth.Return()
	if err != nil {
		t.Fatalf("return fourth: %v", err)
	}
}

// TestLoanPoolInvalid checks constructor and borrow argument validation.
func TestLoanPoolInvalid(t *testing.T) {
	t.Parallel()
	if _, err := NewLoanPool(0, 64); err == nil {
		t.Fatal("zero capacity accepted")
	}
	if _, err := NewLoanPool(2, 0); err == nil {
		t.Fatal("zero buffer size accepted")
	}
	if _, err := NewLoanPool(2, maxPayloadLen+1); err == nil {
		t.Fatal("oversize buffer accepted")
	}
	pool, err := NewLoanPool(2, 64)
	if err != nil {
		t.Fatalf("new pool: %v", err)
	}
	if _, err := pool.Borrow(nil, 8); !errors.Is(err, ErrLoanNotEligible) {
		t.Fatalf("nil table borrow = %v, want ErrLoanNotEligible", err)
	}
	if _, err := pool.Borrow(loanEligibleTable(), -1); err == nil {
		t.Fatal("negative size accepted")
	}
	if _, err := pool.Borrow(loanEligibleTable(), maxPayloadLen+1); err == nil {
		t.Fatal("oversize borrow accepted")
	}
	var nilPool *LoanPool
	if _, err := nilPool.Borrow(loanEligibleTable(), 8); err == nil {
		t.Fatal("nil pool borrow succeeded")
	}
}

// TestLoanLifecycleDoubleReturn checks return consumes the handle: bytes
// fail after return and a second return is a loud error.
func TestLoanLifecycleDoubleReturn(t *testing.T) {
	t.Parallel()
	pool, err := NewLoanPool(2, 64)
	if err != nil {
		t.Fatalf("new pool: %v", err)
	}
	loan, err := pool.Borrow(loanEligibleTable(), 8)
	if err != nil {
		t.Fatalf("borrow: %v", err)
	}
	view, err := loan.Bytes()
	if err != nil {
		t.Fatalf("bytes before return: %v", err)
	}
	if len(view) != 8 {
		t.Fatalf("len = %d, want 8", len(view))
	}
	err = loan.Return()
	if err != nil {
		t.Fatalf("first return: %v", err)
	}
	if loan.Valid() {
		t.Fatal("handle still valid after return")
	}
	if _, err := loan.Bytes(); !errors.Is(err, ErrLoanReturned) {
		t.Fatalf("bytes after return = %v, want ErrLoanReturned", err)
	}
	if err := loan.Return(); !errors.Is(err, ErrLoanReturned) {
		t.Fatalf("double return = %v, want ErrLoanReturned", err)
	}
	if _, err := (*Loan)(nil).Bytes(); !errors.Is(err, ErrLoanReturned) {
		t.Fatalf("nil bytes = %v, want ErrLoanReturned", err)
	}
}

// TestLoanNoAliasingAfterReturn checks return zeroes pooled storage so a
// stale alias never observes live data and the next borrower sees zeros.
//
//nolint:cyclop // test pins borrow, poison, return, and reborrow checks in one run, splitting would obscure intent
func TestLoanNoAliasingAfterReturn(t *testing.T) {
	t.Parallel()
	pool, err := NewLoanPool(1, 8)
	if err != nil {
		t.Fatalf("new pool: %v", err)
	}
	table := loanEligibleTable()
	loan, err := pool.Borrow(table, 8)
	if err != nil {
		t.Fatalf("borrow: %v", err)
	}
	view, err := loan.Bytes()
	if err != nil {
		t.Fatalf("bytes: %v", err)
	}
	for i := range view {
		view[i] = 0xAA
	}
	stale := view
	err = loan.Return()
	if err != nil {
		t.Fatalf("return: %v", err)
	}
	for i, b := range stale {
		if b != 0 {
			t.Fatalf("stale alias byte %d = 0x%02x after return, want zeroed", i, b)
		}
	}
	next, err := pool.Borrow(table, 8)
	if err != nil {
		t.Fatalf("reborrow: %v", err)
	}
	defer func() {
		if err := next.Return(); err != nil {
			t.Errorf("return recycled: %v", err)
		}
	}()
	clean, err := next.Bytes()
	if err != nil {
		t.Fatalf("bytes: %v", err)
	}
	for i, b := range clean {
		if b != 0 {
			t.Fatalf("recycled byte %d = 0x%02x, want zeroed", i, b)
		}
	}
}

// TestLoanEligibilityMatrix checks self-contained memcpy-safe types pass
// while string, sequence, fixed array, and bool vector tables refuse.
func TestLoanEligibilityMatrix(t *testing.T) {
	t.Parallel()
	if ok, _ := CanLoanType(loanEligibleTable()); !ok {
		t.Fatal("all scalar table must be eligible")
	}
	if ok, _ := CanLoanType(loanNestedTable()); !ok {
		t.Fatal("nested scalar table must be eligible")
	}
	for name, table := range map[string]*types.MessageMembers{
		"string":     loanStringTable(),
		"sequence":   loanSequenceTable(),
		"fixedarray": loanFixedArrayTable(),
		"boolvector": loanBoolVectorTable(),
	} {
		if ok, reason := CanLoanType(table); ok {
			t.Fatalf("%s table reported eligible", name)
		} else if reason == "" {
			t.Fatalf("%s refusal carries empty reason", name)
		}
		if ok, _ := NegotiateLoan(table, LoanPeerOffer{WantsLoan: true, InProcess: true, Vendor: LoanVendorWeld}); ok {
			t.Fatalf("%s table negotiated true", name)
		}
	}
	if ok, _ := CanLoanType(nil); ok {
		t.Fatal("nil table reported eligible")
	}
}

// TestLoanBorrowRefusesNonEligible checks borrow fails loudly before
// touching storage for every non-eligible shape.
func TestLoanBorrowRefusesNonEligible(t *testing.T) {
	t.Parallel()
	pool, err := NewLoanPool(4, 64)
	if err != nil {
		t.Fatalf("new pool: %v", err)
	}
	tables := map[string]*types.MessageMembers{
		"string":     loanStringTable(),
		"sequence":   loanSequenceTable(),
		"fixedarray": loanFixedArrayTable(),
		"boolvector": loanBoolVectorTable(),
	}
	for name, table := range tables {
		if _, err := pool.Borrow(table, 8); !errors.Is(err, ErrLoanNotEligible) {
			t.Fatalf("%s borrow = %v, want ErrLoanNotEligible", name, err)
		}
	}
	if pool.Borrows() != 0 {
		t.Fatalf("borrows = %d after refusals, want 0", pool.Borrows())
	}
}

// TestLoanNegotiationTruthTable pins the anti-cheat core: only the
// in-process eligible pair answers true.
func TestLoanNegotiationTruthTable(t *testing.T) {
	t.Parallel()
	eligible := loanEligibleTable()
	ineligible := loanStringTable()
	cyclone := LoanPeerOffer{WantsLoan: true, InProcess: false, Vendor: "cyclonedds"}
	fastrtps := LoanPeerOffer{WantsLoan: true, InProcess: false, Vendor: "fastrtps"}
	remoteWeld := LoanPeerOffer{WantsLoan: true, InProcess: false, Vendor: LoanVendorWeld}
	localWeld := LoanPeerOffer{WantsLoan: true, InProcess: true, Vendor: LoanVendorWeld}
	localBare := LoanPeerOffer{WantsLoan: true, InProcess: true}
	foreignLocal := LoanPeerOffer{WantsLoan: true, InProcess: true, Vendor: "cyclonedds"}
	noOffer := LoanPeerOffer{WantsLoan: false, InProcess: true, Vendor: LoanVendorWeld}
	cases := []struct {
		name  string
		local *types.MessageMembers
		peer  LoanPeerOffer
		want  bool
	}{
		{"eligible-inprocess-weld", eligible, localWeld, true},
		{"eligible-inprocess-bare", eligible, localBare, true},
		{"eligible-cyclone", eligible, cyclone, false},
		{"eligible-fastrtps", eligible, fastrtps, false},
		{"eligible-remote-weld", eligible, remoteWeld, false},
		{"eligible-foreign-local", eligible, foreignLocal, false},
		{"eligible-no-offer", eligible, noOffer, false},
		{"ineligible-inprocess-weld", ineligible, localWeld, false},
		{"ineligible-cyclone", ineligible, cyclone, false},
		{"nil-local", nil, localWeld, false},
		{"nil-cyclone", nil, cyclone, false},
	}
	for _, tc := range cases {
		got, reason := NegotiateLoan(tc.local, tc.peer)
		if got != tc.want {
			t.Fatalf("%s = %v, want %v (reason %q)", tc.name, got, tc.want, reason)
		}
		if reason == "" {
			t.Fatalf("%s carries empty reason", tc.name)
		}
	}
}

// loanFrame publishes one payload and returns the recorded frame.
func loanFrame(t *testing.T, payload []byte) []byte {
	t.Helper()
	sink := &recordingWriter{}
	pub := mustNewPublisher(t, testWriterGUID(), sink)
	mustPublish(t, pub, transports.OutgoingMessage{TopicName: "/chatter", TypeName: "loan__msg__Eligible", Payload: payload})
	return sink.lastFrame(t)
}

// TestLoanedTakeRoundTrip checks the loaned take serves payload bytes,
// tracks sequence like Take, and counts loans plus copies.
func TestLoanedTakeRoundTrip(t *testing.T) {
	t.Parallel()
	pool, err := NewLoanPool(4, 64)
	if err != nil {
		t.Fatalf("new pool: %v", err)
	}
	recv := newLoanedTestReceiver(t, pool)
	table := loanEligibleTable()
	want := []byte{1, 2, 3, 4, 5, 6, 7, 8}
	loan, err := recv.TakeLoaned(loanFrame(t, want), table)
	if err != nil {
		t.Fatalf("take loaned: %v", err)
	}
	got, err := loan.Bytes()
	if err != nil {
		t.Fatalf("bytes: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("payload = %v, want %v", got, want)
	}
	if loan.TopicName != "/chatter" {
		t.Fatalf("topic = %q, want /chatter", loan.TopicName)
	}
	if loan.TypeName != "loan__msg__Eligible" {
		t.Fatalf("type = %q", loan.TypeName)
	}
	if recv.Loans() != 1 {
		t.Fatalf("loans = %d, want 1", recv.Loans())
	}
	if recv.Copies() != 1 {
		t.Fatalf("copies = %d, want 1 frame to loan copy", recv.Copies())
	}
	err = loan.Return()
	if err != nil {
		t.Fatalf("return: %v", err)
	}
}

// TestLoanedTakeRefusesNonEligible checks the loaned take refuses before
// sequence tracking so a refused sample never shifts ordering.
func TestLoanedTakeRefusesNonEligible(t *testing.T) {
	t.Parallel()
	pool, err := NewLoanPool(4, 64)
	if err != nil {
		t.Fatalf("new pool: %v", err)
	}
	recv := newLoanedTestReceiver(t, pool)
	frame := loanFrame(t, []byte{9, 9, 9, 9})
	if _, err := recv.TakeLoaned(frame, loanStringTable()); !errors.Is(err, ErrLoanNotEligible) {
		t.Fatalf("take loaned string = %v, want ErrLoanNotEligible", err)
	}
	if recv.Loans() != 0 {
		t.Fatalf("loans = %d, want 0 after refusal", recv.Loans())
	}
}

// TestLoanedTakeEmptyPayload checks empty samples serve an empty loan with
// no copy counted.
func TestLoanedTakeEmptyPayload(t *testing.T) {
	t.Parallel()
	pool, err := NewLoanPool(4, 64)
	if err != nil {
		t.Fatalf("new pool: %v", err)
	}
	recv := newLoanedTestReceiver(t, pool)
	loan, err := recv.TakeLoaned(loanFrame(t, nil), loanEligibleTable())
	if err != nil {
		t.Fatalf("take loaned empty: %v", err)
	}
	defer func() {
		if err := loan.Return(); err != nil {
			t.Errorf("return empty loan: %v", err)
		}
	}()
	got, err := loan.Bytes()
	if err != nil {
		t.Fatalf("bytes: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("empty payload len = %d, want 0", len(got))
	}
	if recv.Copies() != 0 {
		t.Fatalf("copies = %d, want 0 for empty sample", recv.Copies())
	}
}

// TestLoanedTakeRejectsBadFrame checks corrupt frames count drops like Take.
func TestLoanedTakeRejectsBadFrame(t *testing.T) {
	t.Parallel()
	pool, err := NewLoanPool(4, 64)
	if err != nil {
		t.Fatalf("new pool: %v", err)
	}
	recv := newLoanedTestReceiver(t, pool)
	before := recv.inner.DroppedCount()
	if _, err := recv.TakeLoaned([]byte{0x00, 0x01}, loanEligibleTable()); err == nil {
		t.Fatal("truncated frame accepted")
	}
	if recv.inner.DroppedCount() != before+1 {
		t.Fatalf("drops = %d, want %d", recv.inner.DroppedCount(), before+1)
	}
}

// TestLoanChannelZeroCopy checks handles pass without copies and the send
// consumes the sender handle.
//
//nolint:cyclop // test pins borrow, send, recv, and return breadth in one run, splitting would obscure intent
func TestLoanChannelZeroCopy(t *testing.T) {
	t.Parallel()
	pool, err := NewLoanPool(4, 64)
	if err != nil {
		t.Fatalf("new pool: %v", err)
	}
	ch, err := NewLoanChannel(4)
	if err != nil {
		t.Fatalf("new channel: %v", err)
	}
	if _, err := NewLoanChannel(0); err == nil {
		t.Fatal("zero depth accepted")
	}
	table := loanEligibleTable()
	loan, err := pool.Borrow(table, 4)
	if err != nil {
		t.Fatalf("borrow: %v", err)
	}
	view, err := loan.Bytes()
	if err != nil {
		t.Fatalf("bytes: %v", err)
	}
	copy(view, []byte{0xDE, 0xAD, 0xBE, 0xEF})
	err = ch.Send(loan)
	if err != nil {
		t.Fatalf("send: %v", err)
	}
	if loan.Valid() {
		t.Fatal("sender handle still valid after send")
	}
	if err := ch.Send(loan); !errors.Is(err, ErrLoanReturned) {
		t.Fatalf("second send = %v, want ErrLoanReturned", err)
	}
	if ch.Transfers() != 1 {
		t.Fatalf("transfers = %d, want 1", ch.Transfers())
	}
	ctx, cancel := context.WithTimeout(context.Background(), defaultLoanTestTimeout)
	defer cancel()
	got, err := ch.Recv(ctx)
	if err != nil {
		t.Fatalf("recv: %v", err)
	}
	payload, err := got.Bytes()
	if err != nil {
		t.Fatalf("bytes: %v", err)
	}
	if !bytes.Equal(payload, []byte{0xDE, 0xAD, 0xBE, 0xEF}) {
		t.Fatalf("payload = %v", payload)
	}
	if ch.Copies() != 0 {
		t.Fatalf("copies = %d, want 0 on zero-copy path", ch.Copies())
	}
	err = got.Return()
	if err != nil {
		t.Fatalf("return: %v", err)
	}
	if err := got.Return(); !errors.Is(err, ErrLoanReturned) {
		t.Fatalf("double return = %v, want ErrLoanReturned", err)
	}
}

// TestLoanChannelRespectsContext checks Recv honors cancellation and nil
// inputs fail loudly.
func TestLoanChannelRespectsContext(t *testing.T) {
	t.Parallel()
	ch, err := NewLoanChannel(1)
	if err != nil {
		t.Fatalf("new channel: %v", err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := ch.Recv(canceled); !errors.Is(err, context.Canceled) {
		t.Fatalf("recv canceled = %v, want context.Canceled", err)
	}
	if err := ch.Send(nil); !errors.Is(err, ErrLoanReturned) {
		t.Fatalf("send nil = %v, want ErrLoanReturned", err)
	}
	var nilCh *LoanChannel
	if err := nilCh.Send(nil); err == nil {
		t.Fatal("nil channel send succeeded")
	}
	if _, err := nilCh.Recv(context.Background()); err == nil {
		t.Fatal("nil channel recv succeeded")
	}
}

// TestLoanConcurrentRace hammers pool, receiver, and channel together for
// the race detector.
//
//nolint:gocognit,cyclop // test pins pool, receiver, and channel concurrency breadth in one run, splitting would obscure intent
func TestLoanConcurrentRace(t *testing.T) {
	t.Parallel()
	pool, err := NewLoanPool(8, 64)
	if err != nil {
		t.Fatalf("new pool: %v", err)
	}
	recv := newLoanedTestReceiver(t, pool)
	ch, err := NewLoanChannel(16)
	if err != nil {
		t.Fatalf("new channel: %v", err)
	}
	table := loanEligibleTable()
	frame := loanFrame(t, []byte{1, 2, 3, 4})
	ctx, cancel := context.WithTimeout(context.Background(), defaultLoanTestTimeout)
	defer cancel()
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			loan, err := pool.Borrow(table, 4)
			if err != nil {
				t.Errorf("borrow: %v", err)
				return
			}
			view, err := loan.Bytes()
			if err != nil {
				t.Errorf("bytes: %v", err)
				return
			}
			view[0] = 0xAA
			err = loan.Return()
			if err != nil {
				t.Errorf("return: %v", err)
			}
		})
	}
	for range 4 {
		wg.Go(func() {
			loan, err := recv.TakeLoaned(frame, table)
			if err != nil {
				return
			}
			if err := loan.Return(); err != nil {
				t.Errorf("return take loan: %v", err)
			}
		})
	}
	for range 4 {
		wg.Go(func() {
			loan, err := pool.Borrow(table, 4)
			if err != nil {
				t.Errorf("channel borrow: %v", err)
				return
			}
			err = ch.Send(loan)
			if err != nil {
				t.Errorf("channel send: %v", err)
				return
			}
		})
	}
	wg.Go(func() {
		for range 4 {
			loan, err := ch.Recv(ctx)
			if err != nil {
				t.Errorf("channel recv: %v", err)
				return
			}
			if err := loan.Return(); err != nil {
				t.Errorf("return channel loan: %v", err)
			}
		}
	})
	wg.Wait()
}

// TestLoanNewReceiverInvalid checks constructor validation.
func TestLoanNewReceiverInvalid(t *testing.T) {
	t.Parallel()
	pool, err := NewLoanPool(2, 64)
	if err != nil {
		t.Fatalf("new pool: %v", err)
	}
	if _, err := NewLoanedReceiver(nil, pool); err == nil {
		t.Fatal("nil receiver accepted")
	}
	if _, err := NewLoanedReceiver(NewDataReceiver(), nil); err == nil {
		t.Fatal("nil pool accepted")
	}
	var nilRecv *LoanedReceiver
	if _, err := nilRecv.TakeLoaned([]byte{}, loanEligibleTable()); err == nil {
		t.Fatal("nil receiver take succeeded")
	}
	ctx, cancel := context.WithTimeout(context.Background(), defaultLoanTestTimeout)
	defer cancel()
	if _, err := nilRecv.TakeLoanedFrom(ctx, nil, loanEligibleTable()); err == nil {
		t.Fatal("nil receiver take from succeeded")
	}
}
