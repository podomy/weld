// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package core

import (
	"errors"
	"fmt"
	"sync"
)

// PooledGroup is a bounded worker pool replacing reentrant callback groups.
//
// The group owns exactly Workers goroutines started at construction and no
// other goroutine or timer per task: Submit and Publish never start a
// goroutine and never arm a timer, so a soak with parallel goals observes
// concurrency at or below the declared bound. Tasks are callbacks bound to
// named subscription lanes. Each lane carries its own bounded queue with
// drop oldest and an independent lost counter, so one flooding lane cannot
// rewrite another lane drop history.
//
// Fairness is round robin over lanes in attach order. The shared cursor
// advances past the lane that supplied the last task, so a lane that is
// always ready takes at most one turn per cycle and every other ready lane
// takes its turn. Dedicated workers per group give group isolation: one busy
// group never borrows another group workers, and a blocked handler occupies
// only its own worker while the remaining workers keep draining.
//
// Copy once at ingress is the default. Publish stores one value header per
// lane and shares the pointee by reference; the group never copies pointed
// to data per worker. Callers needing shared delivery pass a pointer or a
// slice backed value and treat the delivered value as read only. Serial
// ordering is out of scope here: callers needing order use a serial group.
// AttachExclusive always fails loudly so an exclusive style declaration is
// never silently dropped.
//
// The zero value is unusable: cond and workers are unset. Build groups with
// NewPooledGroup and do not copy a group after first use; the struct embeds
// noCopy and vet copylocks flags copies. Use a pointer.
type PooledGroup[T any] struct {
	noCopy  noCopy
	mu      sync.Mutex
	cond    *sync.Cond
	lanes   []*pooledLane[T]
	byName  map[string]*pooledLane[T]
	cursor  int
	workers int
	depth   int
	closed  bool
	wg      sync.WaitGroup
}

// pooledLane is one named subscription queue inside a pooled group.
//
// The queue holds at most the group depth values oldest first. Push beyond
// depth drops the oldest value and counts one loss. The lost counter is
// independent per lane. Lanes are owned by the group mutex; handlers run
// outside the mutex.
type pooledLane[T any] struct {
	name   string
	handle func(T)
	queue  []T
	lost   uint64
}

// PooledSubscription is a handle to one lane of a pooled group.
//
// The handle stays valid across later attaches: lanes are heap allocated and
// never move, so holding the handle needs no relookup. All methods are safe
// for concurrent use. A nil subscription is a no op sink: Submit drops
// without counting, Lost and Len report zero, and Name reports empty.
type PooledSubscription[T any] struct {
	group *PooledGroup[T]
	lane  *pooledLane[T]
}

// NewPooledGroup returns a pooled group with workers dedicated threads and
// depth queue slots per subscription lane.
//
// Workers must be positive and depth must be positive: zero workers cannot
// drain and zero depth cannot retain. The call starts exactly workers
// goroutines and no per lane thread, so concurrency stays at or below the
// bound for the group lifetime.
func NewPooledGroup[T any](workers, depth int) (*PooledGroup[T], error) {
	if workers <= 0 {
		return nil, errors.New("core: new pooled group: workers must be positive") //nolint:wrapcheck // new error, nothing to wrap
	}
	if depth <= 0 {
		return nil, errors.New("core: new pooled group: depth must be positive") //nolint:wrapcheck // new error, nothing to wrap
	}
	g := &PooledGroup[T]{
		lanes:   nil,
		byName:  make(map[string]*pooledLane[T]),
		workers: workers,
		depth:   depth,
	}
	g.cond = sync.NewCond(&g.mu)
	g.wg.Add(workers)
	for range workers {
		go g.loop()
	}
	return g, nil
}

// Attach registers one subscription lane with the given handler.
//
// Names must be non empty and unique within the group, and handle must be
// non nil. Depth is the group depth: every lane holds at most Depth values.
// Attaching after Close fails. The returned handle shares the lane with the
// group workers; handlers run concurrently up to the worker bound with no
// ordering guarantee between lanes or within one lane under contention.
func (g *PooledGroup[T]) Attach(name string, handle func(T)) (*PooledSubscription[T], error) {
	if g == nil {
		return nil, errors.New("core: pooled group attach: nil group") //nolint:wrapcheck // new error, nothing to wrap
	}
	if name == "" {
		return nil, errors.New("core: pooled group attach: subscription name is empty") //nolint:wrapcheck // new error, nothing to wrap
	}
	if handle == nil {
		return nil, fmt.Errorf("core: pooled group attach %q: handler is nil", name)
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.closed {
		return nil, fmt.Errorf("core: pooled group attach %q: group is closed", name)
	}
	if _, dup := g.byName[name]; dup {
		return nil, fmt.Errorf("core: pooled group attach %q: subscription name already attached", name)
	}
	lane := &pooledLane[T]{name: name, handle: handle}
	g.lanes = append(g.lanes, lane)
	g.byName[name] = lane
	g.cond.Broadcast()
	return &PooledSubscription[T]{group: g, lane: lane}, nil
}

// AttachExclusive always fails and never registers a lane.
//
// Pooled execution is concurrent by design and cannot honor exclusive serial
// ordering, so accepting the declaration would drop it silently. Callers
// needing exclusive ordering use a serial group instead. The error names the
// subscription so the refusal is loud in logs and tests.
func (g *PooledGroup[T]) AttachExclusive(name string, _ func(T)) error {
	if g == nil {
		return errors.New("core: pooled group attach exclusive: nil group") //nolint:wrapcheck // new error, nothing to wrap
	}
	return fmt.Errorf("core: pooled group attach exclusive %q: exclusive ordering needs a serial group, pooled execution cannot honor it", name)
}

// Workers returns the declared worker bound.
//
// The bound is fixed at construction. A nil group reports zero.
func (g *PooledGroup[T]) Workers() int {
	if g == nil {
		return 0
	}
	return g.workers
}

// Depth returns the per subscription queue depth.
//
// Every lane holds at most this many values before drop oldest applies. A
// nil group reports zero. Use separate groups for separate depths: a logger
// on /odom at 50 Hz uses depth 100 while a controller uses depth 10, each
// with its own counters.
func (g *PooledGroup[T]) Depth() int {
	if g == nil {
		return 0
	}
	return g.depth
}

// SubscriptionCount returns the number of attached lanes.
//
// A nil or closed group reports its current count without blocking.
func (g *PooledGroup[T]) SubscriptionCount() int {
	if g == nil {
		return 0
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	return len(g.lanes)
}

// Publish fans one value out to every attached lane.
//
// The value header is stored once per lane queue and the pointee is shared
// by reference: the group never copies pointed to data per worker or per
// lane. The return counts lanes that evicted their oldest value on this
// call; callers treat a positive count as backpressure and slow the producer
// instead of outrunning the consumers. A nil or closed group drops the value
// and reports zero evictions.
func (g *PooledGroup[T]) Publish(v T) int {
	if g == nil {
		return 0
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.closed {
		return 0
	}
	evictions := 0
	for _, lane := range g.lanes {
		evicted := pushLocked(g.depth, lane, v)
		if evicted {
			evictions++
		}
	}
	if len(g.lanes) > 0 {
		g.cond.Broadcast()
	}
	return evictions
}

// Close discards pending tasks and joins the workers cleanly.
//
// Queued values are dropped without running their handlers; in flight
// handlers run to completion before Close returns. Close is idempotent and
// safe for concurrent use. Callers must let blocking handlers return before
// Close, otherwise Close waits for them. After Close, Attach fails and
// Submit plus Publish drop without counting. A nil group is a no op.
func (g *PooledGroup[T]) Close() {
	if g == nil {
		return
	}
	g.mu.Lock()
	if g.closed {
		g.mu.Unlock()
		return
	}
	g.closed = true
	for _, lane := range g.lanes {
		var zero T
		for i := range lane.queue {
			lane.queue[i] = zero
		}
		lane.queue = lane.queue[:0]
	}
	g.cond.Broadcast()
	g.mu.Unlock()
	g.wg.Wait()
}

// Name returns the subscription lane name.
//
// A nil subscription or a nil lane reports empty.
func (s *PooledSubscription[T]) Name() string {
	if s == nil || s.lane == nil {
		return ""
	}
	return s.lane.name
}

// Submit enqueues one value on this subscription lane without blocking.
//
// When the lane queue is full the oldest value is dropped and counted in
// Lost, then the new value is appended, so the survivors are always the
// newest. The return reports whether this call evicted one value; callers
// treat true as backpressure and slow the producer. Submit on a nil
// subscription or on a closed group drops the value without counting and
// reports false.
func (s *PooledSubscription[T]) Submit(v T) bool {
	if s == nil || s.group == nil || s.lane == nil {
		return false
	}
	g := s.group
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.closed {
		return false
	}
	evicted := pushLocked(g.depth, s.lane, v)
	g.cond.Signal()
	return evicted
}

// Lost returns the monotonic count of values dropped on overflow for this lane.
//
// Each lane counts independently, so one flooding lane never rewrites
// another lane history. A nil subscription reports zero.
func (s *PooledSubscription[T]) Lost() uint64 {
	if s == nil || s.group == nil || s.lane == nil {
		return 0
	}
	s.group.mu.Lock()
	defer s.group.mu.Unlock()
	return s.lane.lost
}

// Len returns the number of values currently queued for this lane.
//
// Len is advisory: workers may claim values concurrently, so a positive
// result does not guarantee a later Submit is eviction free. A nil
// subscription reports zero.
func (s *PooledSubscription[T]) Len() int {
	if s == nil || s.group == nil || s.lane == nil {
		return 0
	}
	s.group.mu.Lock()
	defer s.group.mu.Unlock()
	return len(s.lane.queue)
}

// pushLocked appends v to the lane queue and reports whether the oldest value
// was evicted.
//
// The caller must hold the group mutex. On overflow the head slot is zeroed
// before the reslice so pointer elements can be collected, the oldest value
// is dropped, the lost counter advances by one, and the survivors stay
// oldest first with the new value at the tail.
func pushLocked[T any](depth int, lane *pooledLane[T], v T) bool {
	if len(lane.queue) >= depth {
		var zero T
		lane.queue[0] = zero
		lane.queue = append(lane.queue[1:], v)
		lane.lost++
		return true
	}
	lane.queue = append(lane.queue, v)
	return false
}

// loop runs tasks until Close.
//
// Each iteration claims one task fairly and runs it outside the mutex so a
// slow handler occupies only its own worker. The worker exits when the group
// is closed; pending queues were already discarded by Close.
func (g *PooledGroup[T]) loop() {
	defer g.wg.Done()
	for {
		task, ok := g.next()
		if !ok {
			return
		}
		task()
	}
}

// next waits for and claims the next task in round robin lane order.
//
// It returns false once the group is closed. The claim runs under the group
// mutex and the handler runs outside it after return.
func (g *PooledGroup[T]) next() (func(), bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	for {
		if g.closed {
			return nil, false
		}
		task, ok := g.claimLocked()
		if ok {
			return task, true
		}
		g.cond.Wait()
	}
}

// claimLocked pops the oldest value from the next ready lane after the cursor.
//
// The caller must hold the group mutex. The scan starts at the cursor and
// wraps, so a lane that is always ready takes at most one turn per cycle and
// every other ready lane takes its turn. On success the cursor advances past
// the supplying lane. The returned closure runs the lane handler on the
// claimed value outside the mutex.
func (g *PooledGroup[T]) claimLocked() (func(), bool) {
	n := len(g.lanes)
	if n == 0 {
		return nil, false
	}
	for i := range n {
		idx := (g.cursor + i) % n
		lane := g.lanes[idx]
		if len(lane.queue) == 0 {
			continue
		}
		v := lane.queue[0]
		var zero T
		lane.queue[0] = zero
		lane.queue = lane.queue[1:]
		handle := lane.handle
		g.cursor = (idx + 1) % n
		return func() { handle(v) }, true
	}
	return nil, false
}
