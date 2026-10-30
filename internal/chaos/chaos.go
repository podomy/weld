// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

// Package chaos provides a deterministic chaos harness for wireless profiles.
//
// The harness implements the transports.Backend contract with programmed
// loss, delay, reorder, duplication, partition split plus heal, discovery
// flap, teardown mid run, clock jumps, a large message matrix, and late
// joiners. Time is virtual through FakeClock so every timing and partition
// behavior runs deterministically in tests with no real sleeps on the data
// path. Loss follows a two state bursty link per docs/wireless.md sections
// 3 and 8: a good state with loss near zero for seconds and a bad state
// with heavy loss for a dwell such as 300 ms, repeated. Uniform random
// drops alone are not offered because they understate correlated outage.
package chaos

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/podomy/weld/internal/transports"
)

// DiscoveryFlapNanos is the debounced discovery flap duration in nanoseconds.
//
// A flap of 200 ms models a sub second WiFi fade with no attachment change.
// It suspends liveliness watermark updates without ticking the discovery
// generation, per docs/wireless.md section 7. Genuine roams use Split plus
// Heal, which do tick the generation as remove plus add.
const DiscoveryFlapNanos int64 = 200_000_000

// permilleScale is the denominator for programmed loss rates.
const permilleScale = 1000

// defaultHistoryCapacity bounds late joiner retention when unset.
const defaultHistoryCapacity = 100

// FakeClock is a deterministic virtual clock in nanoseconds.
//
// It uses an atomic counter so PublishBytes and TakeBytes can read time
// without locking the backend mutex. Advance and Set jump forward or
// backward; backward jumps model sim time bag loops and never deadlock the
// backend, they only move deliverability thresholds. The zero value reads
// zero; prefer NewFakeClock for an explicit start.
type FakeClock struct {
	now atomic.Int64
}

// NewFakeClock returns a clock starting at startNanos.
func NewFakeClock(startNanos int64) *FakeClock {
	clock := &FakeClock{}
	clock.now.Store(startNanos)

	return clock
}

// Now returns the current virtual time in nanoseconds.
func (c *FakeClock) Now() int64 {
	return c.now.Load()
}

// Advance moves the clock by deltaNanos, which may be negative for jumps.
func (c *FakeClock) Advance(deltaNanos int64) {
	c.now.Add(deltaNanos)
}

// Set jumps the clock to an absolute time in nanoseconds.
func (c *FakeClock) Set(nanos int64) {
	c.now.Store(nanos)
}

// BurstyLink is a two state wireless link with programmed dwell.
//
// The cycle repeats goodNanos in the good state then badNanos in the bad
// state. Each state drops deterministically at its permille rate keyed by
// publish sequence and seed, so the same script always drops the same
// messages regardless of goroutine order. The zero value never drops.
type BurstyLink struct {
	goodNanos int64
	badNanos  int64
	lossGood  int
	lossBad   int
	seed      uint64
}

// NewBurstyLink validates dwells plus loss rates and returns the link.
//
// Good plus bad dwells must both be positive, or both zero for a lossless
// link. Rates are permille from 0 through 1000 inclusive.
func NewBurstyLink(goodNanos, badNanos int64, lossGoodPermille, lossBadPermille int, seed uint64) (BurstyLink, error) {
	if goodNanos == 0 && badNanos == 0 {
		if lossGoodPermille != 0 || lossBadPermille != 0 {
			return BurstyLink{}, errors.New("chaos: lossless link must carry zero loss rates") //nolint:wrapcheck // new error, nothing to wrap
		}

		return BurstyLink{seed: seed}, nil
	}

	if goodNanos <= 0 {
		return BurstyLink{}, errors.New("chaos: good dwell must be positive") //nolint:wrapcheck // new error, nothing to wrap
	}

	if badNanos <= 0 {
		return BurstyLink{}, errors.New("chaos: bad dwell must be positive") //nolint:wrapcheck // new error, nothing to wrap
	}

	if lossGoodPermille < 0 || lossGoodPermille > permilleScale {
		return BurstyLink{}, errors.New("chaos: good loss must be 0 through 1000 permille") //nolint:wrapcheck // new error, nothing to wrap
	}

	if lossBadPermille < 0 || lossBadPermille > permilleScale {
		return BurstyLink{}, errors.New("chaos: bad loss must be 0 through 1000 permille") //nolint:wrapcheck // new error, nothing to wrap
	}

	return BurstyLink{
		goodNanos: goodNanos,
		badNanos:  badNanos,
		lossGood:  lossGoodPermille,
		lossBad:   lossBadPermille,
		seed:      seed,
	}, nil
}

// CycleNanos returns the repeat period, or zero for a lossless link.
func (l BurstyLink) CycleNanos() int64 {
	return l.goodNanos + l.badNanos
}

// GoodDwell returns the good state dwell in nanoseconds.
func (l BurstyLink) GoodDwell() int64 {
	return l.goodNanos
}

// BadDwell returns the bad state dwell in nanoseconds.
func (l BurstyLink) BadDwell() int64 {
	return l.badNanos
}

// BadAt reports whether nowNanos falls in the bad state.
func (l BurstyLink) BadAt(nowNanos int64) bool {
	cycle := l.CycleNanos()
	if cycle <= 0 {
		return false
	}

	pos := nowNanos % cycle
	if pos < 0 {
		pos += cycle
	}

	return pos >= l.goodNanos
}

// DropAt reports whether the sequence drops at nowNanos.
//
// The decision hashes sequence with the seed and compares against the
// active state permille, so it is stable across runs and goroutines.
func (l BurstyLink) DropAt(nowNanos int64, seq uint64) bool {
	if l.CycleNanos() <= 0 {
		return false
	}

	rate := l.lossGood
	if l.BadAt(nowNanos) {
		rate = l.lossBad
	}

	if rate <= 0 {
		return false
	}

	if rate >= permilleScale {
		return true
	}

	return int(splitmix64(seq^l.seed)%permilleScale) < rate
}

// splitmix64 spreads one 64 bit input deterministically.
func splitmix64(x uint64) uint64 {
	x += 0x9E3779B97F4A7C15
	x = (x ^ (x >> 30)) * 0xBF58476D1CE4E5B9
	x = (x ^ (x >> 27)) * 0x94D049BB133111EB

	return x ^ (x >> 31)
}

// ChaosConfig programs one ChaosBackend.
type ChaosConfig struct {
	// StartNanos is the initial virtual time.
	StartNanos int64
	// GoodNanos and BadNanos are the bursty link dwells. Both zero means
	// a lossless link; otherwise both must be positive.
	GoodNanos int64
	// BadNanos is the bad state dwell. See GoodNanos.
	BadNanos int64
	// LossGoodPermille is the good state drop rate, 0 through 1000.
	LossGoodPermille int
	// LossBadPermille is the bad state drop rate, 0 through 1000.
	LossBadPermille int
	// DelayNanos holds each message until publish time plus delay.
	DelayNanos int64
	// DuplicateEvery duplicates every Nth publish when positive.
	DuplicateEvery int
	// ReorderWindow buffers this many publishes and releases them in
	// reverse order when at least 2. Values below 2 disable reorder.
	ReorderWindow int
	// HistoryCapacity bounds late joiner retention. Non positive means
	// the default of 100.
	HistoryCapacity int
	// Seed keys the deterministic drop hash.
	Seed uint64
}

// Validate checks the config without building a backend.
func (c ChaosConfig) Validate() error {
	_, err := NewBurstyLink(c.GoodNanos, c.BadNanos, c.LossGoodPermille, c.LossBadPermille, c.Seed)
	if err != nil {
		return err
	}

	if c.DelayNanos < 0 {
		return errors.New("chaos: delay must not be negative") //nolint:wrapcheck // new error, nothing to wrap
	}

	if c.DuplicateEvery < 0 {
		return errors.New("chaos: duplicate interval must not be negative") //nolint:wrapcheck // new error, nothing to wrap
	}

	if c.ReorderWindow < 0 {
		return errors.New("chaos: reorder window must not be negative") //nolint:wrapcheck // new error, nothing to wrap
	}

	if c.HistoryCapacity < 0 {
		return errors.New("chaos: history capacity must not be negative") //nolint:wrapcheck // new error, nothing to wrap
	}

	return nil
}

// historyCapacityOrDefault resolves the retention bound.
func (c ChaosConfig) historyCapacityOrDefault() int {
	if c.HistoryCapacity <= 0 {
		return defaultHistoryCapacity
	}

	return c.HistoryCapacity
}

// BackendStats is a point in time copy of backend counters.
type BackendStats struct {
	// Published counts PublishBytes calls accepted after join.
	Published uint64
	// Dropped counts publishes lost to the bursty link.
	Dropped uint64
	// Delivered counts messages handed out by TakeBytes.
	Delivered uint64
	// Duplicated counts extra copies enqueued by duplication.
	Duplicated uint64
	// Parked counts messages held by the current partition.
	Parked int
	// Queued counts messages awaiting TakeBytes.
	Queued int
	// HistoryLost counts retention evictions for late joiners.
	HistoryLost uint64
	// DiscoveryGeneration counts Split plus Heal events.
	DiscoveryGeneration uint64
	// Partitioned reports whether the link is currently split.
	Partitioned bool
}

// pendingMessage is one queued sample with its deliverability threshold.
type pendingMessage struct {
	msg       transports.IncomingMessage
	deliverAt int64
}

// ChaosBackend is a deterministic fake transports.Backend.
//
// PublishBytes applies partition parking, bursty link loss, bounded
// history, reorder buffering, fixed delay, and duplication in that order.
// TakeBytes returns the head message once its deliver time is reached and
// the link is not partitioned; during a partition it blocks until Heal,
// Close, or context cancel. ReportLiveliness enforces the same generation
// ordering as the transports fake, except reports inside a flap window are
// ignored without touching the watermark or the discovery generation.
// Close is idempotent and unblocks takers. All methods are safe for
// concurrent use. Do not copy a backend after first use.
type ChaosBackend struct {
	mu             sync.Mutex
	cond           *sync.Cond
	clock          *FakeClock
	link           BurstyLink
	delayNanos     int64
	duplicateEvery int
	reorderWindow  int
	historyCap     int
	history        []transports.IncomingMessage
	historyLost    uint64
	queue          []pendingMessage
	reorderBuf     []pendingMessage
	parked         []pendingMessage
	partitioned    bool
	flapUntil      int64
	joined         bool
	closed         bool
	lastGen        map[string]uint64
	lastAlive      map[string]bool
	discoveryGen   uint64
	seq            uint64
	published      uint64
	dropped        uint64
	delivered      uint64
	duplicated     uint64
}

// NewChaosBackend validates the config and returns a backend.
func NewChaosBackend(cfg ChaosConfig) (*ChaosBackend, error) {
	err := cfg.Validate()
	if err != nil {
		return nil, err
	}

	link, err := NewBurstyLink(cfg.GoodNanos, cfg.BadNanos, cfg.LossGoodPermille, cfg.LossBadPermille, cfg.Seed)
	if err != nil {
		return nil, err
	}

	backend := &ChaosBackend{
		clock:          NewFakeClock(cfg.StartNanos),
		link:           link,
		delayNanos:     cfg.DelayNanos,
		duplicateEvery: cfg.DuplicateEvery,
		reorderWindow:  cfg.ReorderWindow,
		historyCap:     cfg.historyCapacityOrDefault(),
		lastGen:        make(map[string]uint64),
		lastAlive:      make(map[string]bool),
	}
	backend.cond = sync.NewCond(&backend.mu)

	return backend, nil
}

// Now returns the current virtual time in nanoseconds.
func (b *ChaosBackend) Now() int64 {
	return b.clock.Now()
}

// Advance moves virtual time forward or backward and wakes takers.
func (b *ChaosBackend) Advance(deltaNanos int64) {
	b.clock.Advance(deltaNanos)
	b.mu.Lock()
	defer b.mu.Unlock()
	b.cond.Broadcast()
}

// SetClock jumps to an absolute virtual time and wakes takers.
func (b *ChaosBackend) SetClock(nanos int64) {
	b.clock.Set(nanos)
	b.mu.Lock()
	defer b.mu.Unlock()
	b.cond.Broadcast()
}

// Split enters the partitioned state and ticks the discovery generation.
//
// Publishes during the split park locally instead of dropping, so Heal can
// deliver them in order. Takes block until Heal, Close, or cancel.
func (b *ChaosBackend) Split() {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.partitioned {
		return
	}

	b.partitioned = true
	b.discoveryGen++
	b.drainReorderLocked(&b.parked)
	b.cond.Broadcast()
}

// Heal leaves the partitioned state, releases parked messages in order,
// and ticks the discovery generation.
func (b *ChaosBackend) Heal() {
	b.mu.Lock()
	defer b.mu.Unlock()

	if !b.partitioned {
		return
	}

	b.partitioned = false
	b.queue = append(b.queue, b.parked...)
	b.parked = nil
	b.discoveryGen++
	b.cond.Broadcast()
}

// Partitioned reports whether the backend is currently split.
func (b *ChaosBackend) Partitioned() bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.partitioned
}

// Flap suspends liveliness watermark updates for flapNanos virtual time.
//
// Short flaps model debounced WiFi fades: they never tick the discovery
// generation. Non positive input is a no-op.
func (b *ChaosBackend) Flap(flapNanos int64) {
	if flapNanos <= 0 {
		return
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	b.flapUntil = b.clock.Now() + flapNanos
}

// FlapActive reports whether the flap window still covers virtual now.
func (b *ChaosBackend) FlapActive() bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.clock.Now() < b.flapUntil
}

// DiscoveryGeneration returns the Split plus Heal event count.
func (b *ChaosBackend) DiscoveryGeneration() uint64 {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.discoveryGen
}

// Stats returns a copy of the backend counters.
func (b *ChaosBackend) Stats() BackendStats {
	b.mu.Lock()
	defer b.mu.Unlock()

	return BackendStats{
		Published:           b.published,
		Dropped:             b.dropped,
		Delivered:           b.delivered,
		Duplicated:          b.duplicated,
		Parked:              len(b.parked),
		Queued:              len(b.queue),
		HistoryLost:         b.historyLost,
		DiscoveryGeneration: b.discoveryGen,
		Partitioned:         b.partitioned,
	}
}

// History returns retained publishes oldest first for late joiners.
//
// Each payload is a fresh copy the caller owns. Retention is bounded: once
// full, each new publish evicts the oldest and counts HistoryLost.
func (b *ChaosBackend) History() []transports.IncomingMessage {
	b.mu.Lock()
	defer b.mu.Unlock()

	out := make([]transports.IncomingMessage, len(b.history))
	for i, msg := range b.history {
		out[i] = transports.IncomingMessage{
			TopicName: msg.TopicName,
			TypeName:  msg.TypeName,
			Payload:   copyPayload(msg.Payload),
		}
	}

	return out
}

// HistoryLost returns the retention eviction count.
func (b *ChaosBackend) HistoryLost() uint64 {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.historyLost
}

// Flush releases buffered reorder messages and wakes takers.
func (b *ChaosBackend) Flush() {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.partitioned {
		b.drainReorderLocked(&b.parked)
	} else {
		b.drainReorderLocked(&b.queue)
	}

	b.cond.Broadcast()
}

// JoinDiscovery marks the backend joined. It is idempotent.
func (b *ChaosBackend) JoinDiscovery(ctx context.Context, _ transports.DiscoveryInfo) error {
	err := ctx.Err()
	if err != nil {
		return fmt.Errorf("chaos: join discovery: %w", err)
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	if b.closed {
		return fmt.Errorf("chaos: join discovery after close: %w", transports.ErrBackendClosed)
	}

	b.joined = true
	b.cond.Broadcast()

	return nil
}

// PublishBytes programs one publish through parking, loss, and reorder.
//
// Loss and parking are silent by design like a real radio: the call still
// returns nil while Stats records the drop or the park. Payload is copied
// on entry so the caller may reuse its slice.
func (b *ChaosBackend) PublishBytes(ctx context.Context, msg transports.OutgoingMessage) error {
	err := ctx.Err()
	if err != nil {
		return fmt.Errorf("chaos: publish context: %w", err)
	}

	now := b.clock.Now()

	b.mu.Lock()
	defer b.mu.Unlock()

	if b.closed {
		return fmt.Errorf("chaos: publish after close: %w", transports.ErrBackendClosed)
	}

	if !b.joined {
		return fmt.Errorf("chaos: publish before join: %w", transports.ErrNotJoined)
	}

	b.seq++
	seq := b.seq
	b.published++

	incoming := transports.IncomingMessage{
		TopicName: msg.TopicName,
		TypeName:  msg.TypeName,
		Payload:   copyPayload(msg.Payload),
	}

	if b.partitioned {
		b.parked = append(b.parked, pendingMessage{msg: copyIncoming(incoming), deliverAt: now + b.delayNanos})
		b.appendHistoryLocked(incoming)
		b.cond.Broadcast()

		return nil
	}

	if b.link.DropAt(now, seq) {
		b.dropped++

		return nil
	}

	b.appendHistoryLocked(incoming)
	b.bufferOrEnqueueLocked(pendingMessage{msg: copyIncoming(incoming), deliverAt: now + b.delayNanos}, seq)
	b.cond.Broadcast()

	return nil
}

// TakeBytes returns the head message once it is deliverable.
//
// It blocks while the queue is empty, the head is delayed past virtual
// now, or the backend is partitioned. It returns ctx.Err on cancel and
// ErrBackendClosed after Close. The returned payload is newly owned.
func (b *ChaosBackend) TakeBytes(ctx context.Context) (transports.IncomingMessage, error) {
	err := ctx.Err()
	if err != nil {
		return transports.IncomingMessage{}, fmt.Errorf("chaos: take context: %w", err)
	}

	stop := context.AfterFunc(ctx, func() {
		b.cond.Broadcast()
	})
	defer stop()

	b.mu.Lock()
	defer b.mu.Unlock()

	for {
		err = ctx.Err()
		if err != nil {
			return transports.IncomingMessage{}, fmt.Errorf("chaos: take context: %w", err)
		}

		if b.closed {
			return transports.IncomingMessage{}, fmt.Errorf("chaos: take after close: %w", transports.ErrBackendClosed)
		}

		if !b.joined {
			return transports.IncomingMessage{}, fmt.Errorf("chaos: take before join: %w", transports.ErrNotJoined)
		}

		if !b.partitioned {
			msg, ok := b.takeHeadLocked(b.clock.Now())
			if ok {
				return msg, nil
			}
		}

		b.cond.Wait()
	}
}

// ReportLiveliness enforces caller owned generation ordering.
//
// Reports inside a flap window are ignored without touching the watermark,
// modeling a debounced fade. Otherwise greater generation wins, same
// generation with same aliveness is an idempotent no-op, and any other
// reuse is stale.
func (b *ChaosBackend) ReportLiveliness(ctx context.Context, report transports.LivelinessReport) error {
	err := ctx.Err()
	if err != nil {
		return fmt.Errorf("chaos: report context: %w", err)
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	if b.closed {
		return fmt.Errorf("chaos: report after close: %w", transports.ErrBackendClosed)
	}

	if !b.joined {
		return fmt.Errorf("chaos: report before join: %w", transports.ErrNotJoined)
	}

	if b.clock.Now() < b.flapUntil {
		return nil
	}

	last, seen := b.lastGen[report.ParticipantName]
	if !seen {
		b.lastGen[report.ParticipantName] = report.Generation
		b.lastAlive[report.ParticipantName] = report.Alive

		return nil
	}

	if report.Generation > last {
		b.lastGen[report.ParticipantName] = report.Generation
		b.lastAlive[report.ParticipantName] = report.Alive

		return nil
	}

	if report.Generation == last && report.Alive == b.lastAlive[report.ParticipantName] {
		return nil
	}

	return fmt.Errorf("chaos: stale generation %d for %q: %w", report.Generation, report.ParticipantName, transports.ErrStaleGeneration)
}

// Close tears down the backend. It is idempotent and unblocks takers.
func (b *ChaosBackend) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.closed = true
	b.cond.Broadcast()

	return nil
}

// takeHeadLocked removes the head message when its time has come.
//
// The caller must hold b.mu. Ownership of the stored payload transfers to
// the caller; history already holds a separate copy.
func (b *ChaosBackend) takeHeadLocked(now int64) (transports.IncomingMessage, bool) {
	if len(b.queue) == 0 {
		return transports.IncomingMessage{}, false
	}

	head := b.queue[0]
	if head.deliverAt > now {
		return transports.IncomingMessage{}, false
	}

	b.queue = append(b.queue[:0], b.queue[1:]...)
	b.delivered++

	return head.msg, true
}

// appendHistoryLocked retains one publish with drop oldest accounting.
//
// The caller must hold b.mu. The input payload is copied so later takes
// cannot alias history.
func (b *ChaosBackend) appendHistoryLocked(msg transports.IncomingMessage) {
	stored := copyIncoming(msg)

	if len(b.history) >= b.historyCap {
		b.history = append(b.history[1:], stored)
		b.historyLost++

		return
	}

	b.history = append(b.history, stored)
}

// bufferOrEnqueueLocked applies duplication plus reorder buffering.
//
// The caller must hold b.mu. With a reorder window below 2 the message
// queues directly; otherwise it joins the buffer and the buffer drains in
// reverse once full.
func (b *ChaosBackend) bufferOrEnqueueLocked(item pendingMessage, seq uint64) {
	if b.reorderWindow < 2 {
		b.queue = append(b.queue, item)
		b.duplicateLocked(item, seq)

		return
	}

	b.reorderBuf = append(b.reorderBuf, item)
	b.duplicateLockedTo(&b.reorderBuf, item, seq)

	if len(b.reorderBuf) >= b.reorderWindow {
		b.drainReorderLocked(&b.queue)
	}
}

// duplicateLocked appends a second copy to the live queue when due.
//
// The caller must hold b.mu.
func (b *ChaosBackend) duplicateLocked(item pendingMessage, seq uint64) {
	if !b.dueForDuplicate(seq) {
		return
	}

	b.queue = append(b.queue, pendingMessage{msg: copyIncoming(item.msg), deliverAt: item.deliverAt})
	b.duplicated++
}

// duplicateLockedTo appends a second copy to the given buffer when due.
//
// The caller must hold b.mu.
func (b *ChaosBackend) duplicateLockedTo(target *[]pendingMessage, item pendingMessage, seq uint64) {
	if !b.dueForDuplicate(seq) {
		return
	}

	*target = append(*target, pendingMessage{msg: copyIncoming(item.msg), deliverAt: item.deliverAt})
	b.duplicated++
}

// dueForDuplicate reports whether seq earns a duplicate copy.
func (b *ChaosBackend) dueForDuplicate(seq uint64) bool {
	if b.duplicateEvery <= 0 {
		return false
	}

	return seq%uint64(b.duplicateEvery) == 0
}

// drainReorderLocked releases buffered messages in reverse order.
//
// The caller must hold b.mu. Reversal is the deterministic reorder: a
// window of two swaps each pair.
func (b *ChaosBackend) drainReorderLocked(target *[]pendingMessage) {
	for _, item := range slices.Backward(b.reorderBuf) {
		*target = append(*target, item)
	}

	b.reorderBuf = nil
}

// copyPayload copies one payload preserving nil.
func copyPayload(payload []byte) []byte {
	if payload == nil {
		return nil
	}

	out := make([]byte, len(payload))
	copy(out, payload)

	return out
}

// copyIncoming copies one message including its payload.
func copyIncoming(msg transports.IncomingMessage) transports.IncomingMessage {
	return transports.IncomingMessage{
		TopicName: msg.TopicName,
		TypeName:  msg.TypeName,
		Payload:   copyPayload(msg.Payload),
	}
}

// LargePayloadSizes returns the message size matrix in bytes.
//
// The axis runs from empty control samples through MTU scale frames near
// 1500 bytes to large bulk frames, per docs/wireless.md section 4: small
// and often stays fresh on lossy links while bulk sheds first.
func LargePayloadSizes() []int {
	return []int{0, 64, 512, 1500, 8000, 32768, 65536}
}

// MakePayload builds a deterministic payload of size bytes.
//
// Content cycles from fill so each size is recognizable without random
// input. Non positive size yields nil, which the backend reads as empty.
func MakePayload(size int, fill byte) []byte {
	if size <= 0 {
		return nil
	}

	out := make([]byte, size)
	for i := range out {
		out[i] = fill + byte(i%251)
	}

	return out
}
