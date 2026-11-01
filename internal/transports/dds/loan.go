// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package dds

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/podomy/weld/internal/types"
)

// Loaned zero-copy delivery within one process with truthful negotiation.
//
// Scope is in-process only. No shared-memory transport exists, so
// cross-process and cross-vendor exchanges always copy; the negotiation
// below answers false there with a reason instead of claiming a loan.
// Within one process the pool below reuses sample buffers and the channel
// passes loaned handles without copying. Take and TakeFrom signatures are
// unchanged; the loaned take lives on LoanedReceiver.
//
// Lifetime contract: Borrow and TakeLoaned hand out a loaned handle that
// owns its bytes. The caller reads through Bytes and must call Return
// exactly once when done. Return consumes the handle: Bytes fails after
// return, a second Return is a loud error, and pooled buffers are zeroed
// on return so a stale alias never observes live data. A Borrow that finds
// the pool empty allocates instead of blocking, so the data path never
// stalls on pool exhaustion.

var (
	// ErrLoanNotEligible reports a loan request for a type that must copy.
	ErrLoanNotEligible = errors.New("dds: loan not eligible")
	// ErrLoanReturned reports use after return or a second return.
	ErrLoanReturned = errors.New("dds: loan already returned")
	// ErrLoanPoolExhausted is unused as a failure; borrow falls back to
	// allocate instead. It stays defined so callers can probe fallback
	// accounting without string matching.
	ErrLoanPoolExhausted = errors.New("dds: loan pool exhausted")
	// ErrLoanInvalid reports invalid loan pool, receiver, or channel arguments.
	//
	// It keeps loan-domain argument validation distinct from the datapath
	// ErrInvalidFrame sentinel so errors.Is distinguishes loan setup bugs
	// from malformed DATA frames.
	ErrLoanInvalid = errors.New("dds: invalid loan argument")
)

// LoanVendorWeld identifies the in-process Weld peer in loan offers.
const LoanVendorWeld = "weld"

// loanPoolMaxEntries caps the number of recycled buffers in one pool.
const loanPoolMaxEntries = 1024

// loanChannelMaxDepth caps LoanChannel buffering so a misconfigured depth
// cannot exhaust memory. Oversize depths refuse loudly with ErrLoanInvalid.
const loanChannelMaxDepth = 1024

// LoanPool is a bounded pool of reusable sample buffers.
//
// Capacity bounds recycled entries; BufferSize is the uniform backing size.
// Borrow never blocks: an empty pool or an oversize request allocates.
// Only self-contained memcpy-safe types borrow; anything else is refused
// loudly at request time so a loan never aliases a type that must copy.
// It is safe for concurrent use.
type LoanPool struct {
	// mu guards free.
	mu sync.Mutex
	// free holds recycled full-size backing stores.
	free [][]byte
	// capacity bounds recycled entries.
	capacity int
	// bufSize is the uniform backing size.
	bufSize int
	// borrows counts Borrow calls.
	borrows atomic.Uint64
	// hits counts borrows served from recycled entries.
	hits atomic.Uint64
	// fallbacks counts borrows that allocated.
	fallbacks atomic.Uint64
	// returns counts successful Return calls.
	returns atomic.Uint64
}

// NewLoanPool returns an empty pool with the given recycled entry cap and
// uniform backing size. Both must be positive; bufSize must fit maxPayloadLen.
func NewLoanPool(capacity, bufSize int) (*LoanPool, error) {
	if capacity <= 0 {
		return nil, fmt.Errorf("dds: new loan pool capacity %d must be positive: %w", capacity, ErrLoanInvalid)
	}
	if capacity > loanPoolMaxEntries {
		return nil, fmt.Errorf("dds: new loan pool capacity %d above cap %d: %w", capacity, loanPoolMaxEntries, ErrLoanInvalid)
	}
	if bufSize <= 0 {
		return nil, fmt.Errorf("dds: new loan pool buffer size %d must be positive: %w", bufSize, ErrLoanInvalid)
	}
	if bufSize > maxPayloadLen {
		return nil, fmt.Errorf("dds: new loan pool buffer size %d above cap %d: %w", bufSize, maxPayloadLen, ErrLoanInvalid)
	}
	return &LoanPool{capacity: capacity, bufSize: bufSize}, nil
}

// Capacity returns the recycled entry cap.
func (p *LoanPool) Capacity() int {
	if p == nil {
		return 0
	}
	return p.capacity
}

// BufferSize returns the uniform backing size.
func (p *LoanPool) BufferSize() int {
	if p == nil {
		return 0
	}
	return p.bufSize
}

// Borrows returns the count of Borrow calls.
func (p *LoanPool) Borrows() uint64 {
	if p == nil {
		return 0
	}
	return p.borrows.Load()
}

// Hits returns the count of borrows served from recycled entries.
func (p *LoanPool) Hits() uint64 {
	if p == nil {
		return 0
	}
	return p.hits.Load()
}

// Fallbacks returns the count of borrows that allocated.
func (p *LoanPool) Fallbacks() uint64 {
	if p == nil {
		return 0
	}
	return p.fallbacks.Load()
}

// Returns returns the count of successful Return calls.
func (p *LoanPool) Returns() uint64 {
	if p == nil {
		return 0
	}
	return p.returns.Load()
}

// Free returns the count of currently recycled entries.
func (p *LoanPool) Free() int {
	if p == nil {
		return 0
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.free)
}

// loanEligible reports whether the table may loan with a reason string.
func loanEligible(m *types.MessageMembers) (bool, string) {
	if m == nil {
		return false, "nil type table: cannot loan without introspection"
	}
	ok, err := types.CanLoan(m)
	if err != nil {
		return false, fmt.Sprintf("can-loan check for %s/%s failed: %v", m.Namespace, m.Name, err)
	}
	if !ok {
		return false, fmt.Sprintf("type %s/%s is not self-contained: loan refuses, exchange copies", m.Namespace, m.Name)
	}
	safe, err := types.IsMemcpySafe(m)
	if err != nil {
		return false, fmt.Sprintf("memcpy check for %s/%s failed: %v", m.Namespace, m.Name, err)
	}
	if !safe {
		return false, fmt.Sprintf("type %s/%s is not memcpy-safe: loan refuses, exchange copies", m.Namespace, m.Name)
	}
	return true, fmt.Sprintf("type %s/%s is self-contained and memcpy-safe", m.Namespace, m.Name)
}

// CanLoanType reports local-only loan eligibility with a reason string.
func CanLoanType(m *types.MessageMembers) (bool, string) {
	return loanEligible(m)
}

// Borrow hands out a loaned buffer of size bytes for the given type.
//
// Non-eligible types are refused loudly with ErrLoanNotEligible before any
// storage is touched. An empty pool allocates instead of blocking and an
// oversize request allocates an exact-size buffer that is never recycled,
// so the pool stays uniform. The returned handle must be Returned exactly
// once.
func (p *LoanPool) Borrow(m *types.MessageMembers, size int) (*Loan, error) {
	if p == nil {
		return nil, fmt.Errorf("dds: loan borrow on nil pool: %w", ErrLoanInvalid)
	}
	ok, reason := loanEligible(m)
	if !ok {
		return nil, fmt.Errorf("dds: loan borrow refused: %s: %w", reason, ErrLoanNotEligible)
	}
	if size < 0 {
		return nil, fmt.Errorf("dds: loan borrow size %d must not be negative: %w", size, ErrLoanInvalid)
	}
	if size > maxPayloadLen {
		return nil, fmt.Errorf("dds: loan borrow size %d above cap %d: %w", size, maxPayloadLen, ErrLoanInvalid)
	}
	p.borrows.Add(1)
	if size > p.bufSize {
		p.fallbacks.Add(1)
		return &Loan{pool: p, buf: make([]byte, size), pooled: false, valid: true}, nil
	}
	p.mu.Lock()
	if len(p.free) > 0 {
		full := p.free[len(p.free)-1]
		p.free = p.free[:len(p.free)-1]
		p.mu.Unlock()
		p.hits.Add(1)
		return &Loan{pool: p, full: full, buf: full[:size], pooled: true, valid: true}, nil
	}
	p.mu.Unlock()
	p.fallbacks.Add(1)
	full := make([]byte, p.bufSize)
	return &Loan{pool: p, full: full, buf: full[:size], pooled: true, valid: true}, nil
}

// recycle stores one backing store or drops it when the pool is full.
// The caller must hold no locks; it zeroes the store so a stale alias
// never observes live data after return.
func (p *LoanPool) recycle(full []byte) {
	for i := range full {
		full[i] = 0
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(full) != p.bufSize {
		return
	}
	if len(p.free) >= p.capacity {
		return
	}
	p.free = append(p.free, full)
}

// Loan is one borrowed sample buffer with explicit return.
//
// The zero value is invalid: Bytes fails and Return reports ErrLoanReturned.
// A Loan must not be copied after first use; pass the pointer. Return
// consumes the handle, so use after return is a loud error, never silent
// aliasing. TopicName and TypeName are set by TakeLoaned and stay empty for
// raw Borrow handles. It is safe for concurrent use through its mutex:
// Bytes, Len, Pooled, Valid, and Return self-lock, and LoanChannel.Send
// locks the handle while consuming it.
type Loan struct {
	// mu guards valid plus buf and full across Bytes, Return, and Send.
	mu sync.Mutex
	// pool recycles pooled backing stores on Return.
	pool *LoanPool
	// full is the pooled backing store, nil for exact-size fallbacks.
	full []byte
	// buf is the exposed view.
	buf []byte
	// pooled reports whether full may recycle on Return.
	pooled bool
	// valid is consumed by Return and Send.
	valid bool
	// TopicName is the ROS topic set by TakeLoaned.
	TopicName string
	// TypeName is the IDL type set by TakeLoaned.
	TypeName string
}

// Bytes exposes the loaned sample bytes. It fails after Return.
func (l *Loan) Bytes() ([]byte, error) {
	if l == nil {
		return nil, fmt.Errorf("dds: loan bytes on nil handle: %w", ErrLoanReturned)
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if !l.valid {
		return nil, fmt.Errorf("dds: loan bytes after return: %w", ErrLoanReturned)
	}
	return l.buf, nil
}

// Len returns the exposed byte count, or -1 after return.
func (l *Loan) Len() int {
	if l == nil {
		return -1
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if !l.valid {
		return -1
	}
	return len(l.buf)
}

// Pooled reports whether Return may recycle the backing store.
func (l *Loan) Pooled() bool {
	if l == nil {
		return false
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.pooled
}

// Valid reports whether the handle still owns its bytes.
func (l *Loan) Valid() bool {
	if l == nil {
		return false
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.valid
}

// Return releases the handle exactly once.
//
// Pooled backing stores are zeroed and recycled when the pool has room;
// anything else is dropped for the collector. Every successful return
// with a pool counts, including exact-size fallbacks that never recycle,
// so Returns tracks completed lifetimes rather than recycles alone.
// A second Return is a loud error, not silent corruption.
func (l *Loan) Return() error {
	if l == nil {
		return fmt.Errorf("dds: loan return on nil handle: %w", ErrLoanReturned)
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if !l.valid {
		return fmt.Errorf("dds: loan double return: %w", ErrLoanReturned)
	}
	l.valid = false
	full := l.full
	pool := l.pool
	pooled := l.pooled
	l.buf = nil
	l.full = nil
	if pool != nil {
		if pooled && full != nil {
			pool.recycle(full)
		}
		pool.returns.Add(1)
		return nil
	}
	return nil
}

// LoanPeerOffer carries one peer loan request for negotiation.
type LoanPeerOffer struct {
	// WantsLoan reports whether the peer asked for loans.
	WantsLoan bool
	// InProcess reports whether peer and local share one address space.
	InProcess bool
	// Vendor names the peer stack when known, empty when unknown.
	// Only a positive foreign ID vetoes; an empty vendor on the in-process
	// path is treated as local because path identity, not the name,
	// governs safety.
	Vendor string
}

// NegotiateLoan answers whether a (local type, peer offer) pair may loan.
//
// It is the anti-cheat core: true only for self-contained memcpy-safe
// local types over the in-process path with a peer that asked for loans
// and is not positively identified as foreign. Every other pair answers
// false with a reason naming the veto, so cross-vendor and non-eligible
// exchanges always copy and never claim a loan. It is pure with no I/O.
func NegotiateLoan(local *types.MessageMembers, peer LoanPeerOffer) (bool, string) {
	if local == nil {
		return false, "nil local type table: cannot loan without introspection"
	}
	if !peer.WantsLoan {
		return false, "peer did not offer loan: exchange copies"
	}
	if !peer.InProcess {
		return false, "cross-process path copies: loan requires one shared process"
	}
	if peer.Vendor != "" && peer.Vendor != LoanVendorWeld {
		return false, fmt.Sprintf("cross-vendor peer %q has no shared-memory transport: exchange copies", peer.Vendor)
	}
	ok, reason := loanEligible(local)
	if !ok {
		return false, reason
	}
	return true, fmt.Sprintf("in-process loan for %s: %s", LoanVendorWeld, reason)
}

// LoanedReceiver serves loaned takes over one DataReceiver without
// changing Take or TakeFrom signatures.
//
// The pool supplies sample storage; Copies counts payload copies from
// frame into loan and Loans counts served samples. Foreign plain-CDR
// samples are never served here: TakeLoanedFrom rejects them loudly so
// callers fall back to the copying TakeFrom path. It is safe for
// concurrent use.
type LoanedReceiver struct {
	// inner tracks best-effort sequence for loaned takes.
	inner *DataReceiver
	// pool supplies loaned sample storage.
	pool *LoanPool
	// loans counts served loaned samples.
	loans atomic.Uint64
	// copies counts payload copies from frame into loan.
	copies atomic.Uint64
}

// NewLoanedReceiver wraps inner with pool storage for loaned takes.
func NewLoanedReceiver(inner *DataReceiver, pool *LoanPool) (*LoanedReceiver, error) {
	if inner == nil {
		return nil, fmt.Errorf("dds: new loaned receiver nil receiver: %w", ErrLoanInvalid)
	}
	if pool == nil {
		return nil, fmt.Errorf("dds: new loaned receiver nil pool: %w", ErrLoanInvalid)
	}
	return &LoanedReceiver{inner: inner, pool: pool}, nil
}

// Loans returns the count of served loaned samples.
func (r *LoanedReceiver) Loans() uint64 {
	if r == nil {
		return 0
	}
	return r.loans.Load()
}

// Copies returns the count of payload copies from frame into loan.
func (r *LoanedReceiver) Copies() uint64 {
	if r == nil {
		return 0
	}
	return r.copies.Load()
}

// Pool returns the backing loan pool.
func (r *LoanedReceiver) Pool() *LoanPool {
	if r == nil {
		return nil
	}
	return r.pool
}

// TakeLoaned validates one DATA frame and serves its payload as a loan.
//
// Validation, topic demangling, and best-effort ordering match Take
// exactly; only the copy target changes from a fresh allocation to a
// pooled loan. Non-eligible types are refused loudly with
// ErrLoanNotEligible before sequence is touched. The caller owns the
// returned handle and must Return it exactly once.
//
// Lifetime contract: the loan aliases pooled pool storage, not the input
// frame, so the caller may reuse the frame at once. The loan bytes stay
// valid until Return; after Return Bytes fails and a second Return is a
// loud error.
func (r *LoanedReceiver) TakeLoaned(frame []byte, m *types.MessageMembers) (*Loan, error) {
	if r == nil {
		return nil, fmt.Errorf("dds: loaned take on nil receiver: %w", ErrLoanInvalid)
	}
	if r.inner == nil || r.pool == nil {
		return nil, fmt.Errorf("dds: loaned take on incomplete receiver: %w", ErrLoanInvalid)
	}
	ok, reason := loanEligible(m)
	if !ok {
		return nil, fmt.Errorf("dds: loaned take refused: %s: %w", reason, ErrLoanNotEligible)
	}
	decoded, err := decodeFrame(frame)
	if err != nil {
		r.inner.countDrop()
		return nil, fmt.Errorf("dds: loaned take decode: %w", err)
	}
	payloadView, err := validateEnvelopePayload(decoded.envelope, decoded.userLen)
	if err != nil {
		r.inner.countDrop()
		return nil, fmt.Errorf("dds: loaned take envelope: %w", err)
	}
	rosTopic, _, err := ParseTopicName(decoded.ddsTopic)
	if err != nil {
		r.inner.countDrop()
		return nil, fmt.Errorf("dds: loaned take topic %q: %w", decoded.ddsTopic, err)
	}
	r.inner.mu.Lock()
	defer r.inner.mu.Unlock()
	err = r.inner.trackSequenceLocked(decoded.guid, decoded.seq)
	if err != nil {
		return nil, fmt.Errorf("dds: loaned take sequence: %w", err)
	}
	loan, err := r.pool.Borrow(m, int(decoded.userLen))
	if err != nil {
		return nil, fmt.Errorf("dds: loaned take borrow: %w", err)
	}
	loan.TopicName = rosTopic
	loan.TypeName = decoded.typeName
	if decoded.userLen > 0 {
		copy(loan.buf, payloadView[:decoded.userLen])
		r.copies.Add(1)
	}
	r.loans.Add(1)
	return loan, nil
}

// TakeLoanedFrom reads one RTPS DATA payload and serves it as a loan.
//
// It mirrors TakeFrom through the Weld DATA frame path with the same
// ordering and copy-once ownership, except the copy lands in a pooled
// loan. GUID mismatch and foreign plain-CDR samples are rejected loudly
// so cross-vendor data always flows through the copying TakeFrom path and
// never claims a loan. The caller must Return the handle exactly once.
func (r *LoanedReceiver) TakeLoanedFrom(ctx context.Context, s FrameSource, m *types.MessageMembers) (*Loan, error) {
	if r == nil {
		return nil, fmt.Errorf("dds: loaned take from on nil receiver: %w", ErrLoanInvalid)
	}
	if r.inner == nil || r.pool == nil {
		return nil, fmt.Errorf("dds: loaned take from on incomplete receiver: %w", ErrLoanInvalid)
	}
	if s == nil {
		return nil, fmt.Errorf("dds: loaned take from nil source: %w", ErrLoanInvalid)
	}
	ok, reason := loanEligible(m)
	if !ok {
		return nil, fmt.Errorf("dds: loaned take from refused: %s: %w", reason, ErrLoanNotEligible)
	}
	err := ctx.Err()
	if err != nil {
		return nil, err //nolint:wrapcheck // ctx error passes through unchanged
	}
	guid, _, payload, _, _, err := readFrameWithQoS(ctx, s)
	if err != nil {
		return nil, fmt.Errorf("dds: loaned take from read: %w", err)
	}
	frame, err := ParseRTPSPayload(payload)
	if err != nil {
		r.inner.countDrop()
		return nil, fmt.Errorf("dds: loaned take from payload is not a Weld frame, use TakeFrom copies: %w", err)
	}
	decoded, err := decodeFrame(frame)
	if err != nil {
		r.inner.countDrop()
		return nil, fmt.Errorf("dds: loaned take from decode: %w", err)
	}
	if decoded.guid != guid {
		r.inner.countDrop()
		return nil, fmt.Errorf("dds: loaned take from guid mismatch: %w", ErrLoanInvalid)
	}
	return r.TakeLoaned(frame, m)
}

// LoanChannel passes loaned handles between one in-process publisher and
// subscriber with zero copies.
//
// Send consumes the sender handle and Recv hands ownership to the caller,
// who must Return exactly once. Transfers counts handoffs; Copies stays
// zero by construction because the payload bytes are never duplicated on
// this path. A nonzero Copies would prove a copy slipped in, so gates
// assert Copies equals zero. It is safe for concurrent use.
type LoanChannel struct {
	// ch carries handles from Send to Recv.
	ch chan *Loan
	// transfers counts consumed sends.
	transfers atomic.Uint64
	// copies counts payload copies, always zero on this path.
	copies atomic.Uint64
}

// NewLoanChannel returns a channel buffering depth handles.
//
// Depth must be positive and at most loanChannelMaxDepth; oversize depths
// refuse loudly with ErrLoanInvalid instead of allocating without bound.
func NewLoanChannel(depth int) (*LoanChannel, error) {
	if depth <= 0 {
		return nil, fmt.Errorf("dds: new loan channel depth %d must be positive: %w", depth, ErrLoanInvalid)
	}
	if depth > loanChannelMaxDepth {
		return nil, fmt.Errorf("dds: new loan channel depth %d above cap %d: %w", depth, loanChannelMaxDepth, ErrLoanInvalid)
	}
	return &LoanChannel{ch: make(chan *Loan, depth)}, nil
}

// Send transfers one loan without copying. It consumes the sender handle: the
// sender must not use it after Send returns nil and must drop its alias.
// The sender handle is invalidated like Return, with buf and full cleared
// under the handle mutex so a stale alias observes nil, never live data.
//
// Send blocks when the channel buffer is full until Recv drains one
// slot; there is no non-blocking TrySend because no caller needs one.
// If a non-blocking handoff is ever required, add TrySend as a separate
// method rather than changing Send semantics.
func (c *LoanChannel) Send(loan *Loan) error {
	if c == nil {
		return fmt.Errorf("dds: loan channel send on nil channel: %w", ErrLoanInvalid)
	}
	if loan == nil {
		return fmt.Errorf("dds: loan channel send nil handle: %w", ErrLoanReturned)
	}
	loan.mu.Lock()
	defer loan.mu.Unlock()
	if !loan.valid {
		return fmt.Errorf("dds: loan channel send after return: %w", ErrLoanReturned)
	}
	loan.valid = false
	payload := loan.buf
	full := loan.full
	pooled := loan.pooled
	pool := loan.pool
	topic := loan.TopicName
	typeName := loan.TypeName
	loan.buf = nil
	loan.full = nil
	carried := &Loan{pool: pool, full: full, buf: payload, pooled: pooled, valid: true, TopicName: topic, TypeName: typeName}
	c.ch <- carried
	c.transfers.Add(1)
	return nil
}

// Recv waits for one loaned handle or context end. The caller owns the
// returned handle and must Return it exactly once.
func (c *LoanChannel) Recv(ctx context.Context) (*Loan, error) {
	if c == nil {
		return nil, fmt.Errorf("dds: loan channel recv on nil channel: %w", ErrLoanInvalid)
	}
	if ctx == nil {
		return nil, fmt.Errorf("dds: loan channel recv nil context: %w", ErrLoanInvalid)
	}
	select {
	case <-ctx.Done():
		err := ctx.Err()
		if err != nil {
			return nil, fmt.Errorf("dds: loan channel recv: %w", err)
		}
		return nil, fmt.Errorf("dds: loan channel recv context done: %w", context.Canceled)
	case loan := <-c.ch:
		return loan, nil
	}
}

// Transfers returns the count of zero-copy handoffs.
func (c *LoanChannel) Transfers() uint64 {
	if c == nil {
		return 0
	}
	return c.transfers.Load()
}

// Copies returns the count of payload copies on this path, always zero.
func (c *LoanChannel) Copies() uint64 {
	if c == nil {
		return 0
	}
	return c.copies.Load()
}

// Len returns the queued handle count.
func (c *LoanChannel) Len() int {
	if c == nil {
		return 0
	}
	return len(c.ch)
}
