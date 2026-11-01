// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package core

import "sync"

// HistoryCache is a bounded oldest first retention buffer with a lost counter.
//
// Push drops the oldest sample once the cache is full and counts every drop
// in Lost, so overflow is never silent. Wire Lost into the loud channel
// (logs and metrics) per drop; each eviction increments Lost by one.
// Snapshot returns the retained samples oldest first for late joiners. Full
// is an advisory saturation probe for backpressure; it races with concurrent
// Push, so callers must still handle the Push eviction report. Core scopes
// to counts only; transports enforce byte caps alongside counts (see
// transport byte cap tests). The zero value is unusable because capacity is
// unset; build caches with NewHistoryCache and do not copy a cache after
// first use.
type HistoryCache[T any] struct {
	mu       sync.Mutex
	buf      []T
	capacity uint64
	lost     uint64
}

// NewHistoryCache returns an empty cache holding at most capacity samples.
//
// Capacity must come from RetentionCapacity. Zero panics because zero means
// not set: RetentionCapacity errors on keep last depth zero and on
// unresolved history, so zero must never silently normalize to one. This
// documents the conflict explicitly instead of hiding an unresolved profile
// behind a capacity of one. Capacities past MaxHistoryCacheEntries clamp to
// the ceiling so direct constructions cannot force unbounded allocation;
// overflow past the ceiling evicts oldest with loud Lost counting in Push.
// Panic on zero capacity is by design: it fails loudly at construction
// instead of hiding a misconfigured depth.
func NewHistoryCache[T any](capacity uint64) *HistoryCache[T] {
	if capacity == 0 {
		panic("core: NewHistoryCache: capacity 0 means not set; call RetentionCapacity first")
	}
	if capacity > MaxHistoryCacheEntries {
		capacity = MaxHistoryCacheEntries
	}
	return &HistoryCache[T]{capacity: capacity}
}

// Push appends v and reports whether an oldest sample was evicted. A zero
// value cache with zero capacity drops safely without an index panic; build
// caches with NewHistoryCache.
//
// The return is advisory backpressure feedback: true means one sample was
// dropped and counted in Lost, so the caller must slow the producer instead
// of outrunning the consumer per the DeliveryBehavior contract. Wire Lost
// into the loud channel per drop. The evicted head slot is zeroed before the
// reslice so pointer elements can be collected.
func (c *HistoryCache[T]) Push(v T) bool {
	if c == nil {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.capacity == 0 {
		return false
	}
	if uint64(len(c.buf)) >= c.capacity {
		var zero T
		c.buf[0] = zero
		c.buf = append(c.buf[1:], v)
		c.lost++
		return true
	}
	c.buf = append(c.buf, v)
	return false
}

// Len returns the number of retained samples. A nil cache reports zero.
func (c *HistoryCache[T]) Len() int {
	if c == nil {
		return 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.buf)
}

// Lost returns the monotonic count of samples dropped on overflow.
// A nil cache reports zero.
func (c *HistoryCache[T]) Lost() uint64 {
	if c == nil {
		return 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.lost
}

// Full reports whether the cache holds capacity samples and the next Push
// will evict. A nil or zero capacity cache reports false.
//
// Full is advisory: it observes saturation at one instant and races with
// concurrent Push, so a false result does not guarantee the next Push is
// eviction free. Callers treat full as the backpressure hint and still honor
// the Push eviction report to slow producers.
func (c *HistoryCache[T]) Full() bool {
	if c == nil {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.capacity == 0 {
		return false
	}
	return uint64(len(c.buf)) >= c.capacity
}

// Snapshot returns a copy of the retained samples oldest first for late
// joiners. The copy is safe to hold after further pushes. A nil cache yields
// an empty non nil slice.
//
// The copy is shallow: for pointer or slice elements the pointees are
// shared with the cache. An empty cache yields an empty non nil slice, never
// nil, so callers can distinguish allocation failure from empty retention by
// length alone. Mutating the returned slice never affects the cache.
func (c *HistoryCache[T]) Snapshot() []T {
	if c == nil {
		return []T{}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]T, len(c.buf))
	copy(out, c.buf)
	return out
}

// Clear drops all retained samples while preserving the Lost count. A nil
// cache is a no op.
//
// Clear keeps the monotonic overflow signal intact so a jump in retained
// state never rewrites drop history. After Clear, Len is zero, Full is
// false, and Snapshot is empty non nil. Capacity is unchanged.
func (c *HistoryCache[T]) Clear() {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	var zero T
	for i := range c.buf {
		c.buf[i] = zero
	}
	c.buf = c.buf[:0]
}
