// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package sdk

import (
	"cmp"
	"errors"
	"fmt"
	"slices"
	"sync"
)

// SerialBondTimeoutNanos is the manager bond timeout of 4.0 s.
const SerialBondTimeoutNanos int64 = 4000000000

// SerialEntity is one unit of ordered work plus lifecycle identity.
//
// Priority orders delivery with lower values first; equal values follow
// attach order. Lifecycle callbacks may be nil for no operation. Take
// performs at most one unit of work and reports whether work was done;
// nil Take means no polled work.
type SerialEntity struct {
	// Name identifies the entity within the group.
	Name string
	// Priority orders delivery with lower values first.
	Priority int
	// Configure brings the entity to the configured state.
	Configure func() error
	// Activate brings the entity to the active state.
	Activate func() error
	// Deactivate returns the entity to the inactive state.
	Deactivate func() error
	// Cleanup returns the entity to the unconfigured state.
	Cleanup func() error
	// Shutdown performs terminal teardown.
	Shutdown func() error
	// Take performs at most one unit of work.
	Take func() bool
}

// serialEntry is one attached entity with insertion order and bond state.
type serialEntry struct {
	// entity is immutable after insertion.
	entity SerialEntity
	// seq breaks priority ties by attach order.
	seq uint64
	// lastHeartbeatNanos is the last recorded heartbeat time.
	lastHeartbeatNanos int64
	// hasHeartbeat is true after the first recorded heartbeat.
	hasHeartbeat bool
}

// SerialGroup delivers attached entities one at a time in priority plus
// insertion order with lifecycle supervision and bond liveliness.
//
// REVIEW: background Spin and preshutdown hooks are deferred to user
// review; callers drive delivery with SpinOnce. Close rejects new work
// and is idempotent, matching Node.Close.
type SerialGroup struct {
	// mu guards entries plus lifecycle flags.
	mu sync.Mutex
	// runMu serializes concurrent SpinOnce callers.
	runMu sync.Mutex
	// name identifies the group.
	name string
	// entries holds attached entities in attach order.
	entries []*serialEntry
	// nextSeq numbers insertion order.
	nextSeq uint64
	// configured reports whether Configure completed.
	configured bool
	// active reports whether the group delivers.
	active bool
	// closed rejects new work after Close.
	closed bool
}

// NewSerialGroup returns an empty serial supervision group.
func NewSerialGroup(name string) (*SerialGroup, error) {
	if name == "" {
		return nil, errors.New("sdk: serial group name is empty") //nolint:wrapcheck // new error, nothing to wrap
	}
	return &SerialGroup{name: name}, nil
}

// Name returns the group name. A nil group reports empty.
func (g *SerialGroup) Name() string {
	if g == nil {
		return ""
	}
	return g.name
}

// Len returns the attached entity count. A nil group reports zero.
func (g *SerialGroup) Len() int {
	if g == nil {
		return 0
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	return len(g.entries)
}

// Order returns entity names in scan order. A nil group returns nil.
func (g *SerialGroup) Order() []string {
	if g == nil {
		return nil
	}
	g.mu.Lock()
	out := make([]*serialEntry, len(g.entries))
	copy(out, g.entries)
	g.mu.Unlock()
	sortSerialEntries(out)
	names := make([]string, len(out))
	for i := range out {
		names[i] = out[i].entity.Name
	}
	return names
}

// IsConfigured reports whether Configure completed. A nil group is false.
func (g *SerialGroup) IsConfigured() bool {
	if g == nil {
		return false
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.configured
}

// IsActive reports whether the group delivers. A nil group is false.
func (g *SerialGroup) IsActive() bool {
	if g == nil {
		return false
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.active
}

// IsClosed reports whether Close ran. A nil group reports closed.
func (g *SerialGroup) IsClosed() bool {
	if g == nil {
		return true
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.closed
}

// Attach adds an entity. When the group is already configured the entity
// Configure runs before insertion, and when already active Activate runs
// after Configure; a failing callback prevents insertion.
func (g *SerialGroup) Attach(e SerialEntity) error {
	if g == nil {
		return errors.New("sdk: serial group attach on nil group") //nolint:wrapcheck // new error, nothing to wrap
	}
	if e.Name == "" {
		return errors.New("sdk: serial group attach with empty entity name") //nolint:wrapcheck // new error, nothing to wrap
	}
	g.mu.Lock()
	if g.closed {
		g.mu.Unlock()
		return errors.New("sdk: serial group attach on closed group") //nolint:wrapcheck // new error, nothing to wrap
	}
	if findSerialIndex(g.entries, e.Name) >= 0 {
		g.mu.Unlock()
		return fmt.Errorf("sdk: serial group attach duplicate entity %q", e.Name)
	}
	configured := g.configured
	active := g.active
	g.mu.Unlock()
	if configured {
		err := callSerialCallback(e.Configure)
		if err != nil {
			return fmt.Errorf("sdk: serial group attach %q configure: %w", e.Name, err)
		}
	}
	if active {
		err := callSerialCallback(e.Activate)
		if err != nil {
			return fmt.Errorf("sdk: serial group attach %q activate: %w", e.Name, err)
		}
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.closed {
		return errors.New("sdk: serial group attach on closed group") //nolint:wrapcheck // new error, nothing to wrap
	}
	if findSerialIndex(g.entries, e.Name) >= 0 {
		return fmt.Errorf("sdk: serial group attach duplicate entity %q", e.Name)
	}
	g.entries = append(g.entries, &serialEntry{entity: e, seq: g.nextSeq})
	g.nextSeq++
	return nil
}

// Detach removes an entity by name. When active, Deactivate runs first.
func (g *SerialGroup) Detach(name string) error {
	if g == nil {
		return errors.New("sdk: serial group detach on nil group") //nolint:wrapcheck // new error, nothing to wrap
	}
	if name == "" {
		return errors.New("sdk: serial group detach with empty entity name") //nolint:wrapcheck // new error, nothing to wrap
	}
	g.mu.Lock()
	idx := findSerialIndex(g.entries, name)
	if idx < 0 {
		g.mu.Unlock()
		return fmt.Errorf("sdk: serial group detach unknown entity %q", name)
	}
	active := g.active
	entity := g.entries[idx].entity
	g.mu.Unlock()
	if active {
		err := callSerialCallback(entity.Deactivate)
		if err != nil {
			return fmt.Errorf("sdk: serial group detach %q deactivate: %w", name, err)
		}
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	idx = findSerialIndex(g.entries, name)
	if idx < 0 {
		return fmt.Errorf("sdk: serial group detach unknown entity %q", name)
	}
	copy(g.entries[idx:], g.entries[idx+1:])
	g.entries[len(g.entries)-1] = nil
	g.entries = g.entries[:len(g.entries)-1]
	return nil
}

// Configure runs entity Configure forward in attach order.
func (g *SerialGroup) Configure() error {
	if g == nil {
		return errors.New("sdk: serial group configure on nil group") //nolint:wrapcheck // new error, nothing to wrap
	}
	g.mu.Lock()
	if g.closed {
		g.mu.Unlock()
		return errors.New("sdk: serial group configure on closed group") //nolint:wrapcheck // new error, nothing to wrap
	}
	if g.configured {
		g.mu.Unlock()
		return nil
	}
	entities := copySerialEntities(g.entries)
	g.mu.Unlock()
	for i := range entities {
		err := callSerialCallback(entities[i].Configure)
		if err != nil {
			return fmt.Errorf("sdk: serial group configure entity %q: %w", entities[i].Name, err)
		}
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.closed {
		return errors.New("sdk: serial group configure on closed group") //nolint:wrapcheck // new error, nothing to wrap
	}
	g.configured = true
	return nil
}

// Activate runs entity Activate forward in attach order.
func (g *SerialGroup) Activate() error {
	if g == nil {
		return errors.New("sdk: serial group activate on nil group") //nolint:wrapcheck // new error, nothing to wrap
	}
	g.mu.Lock()
	if g.closed {
		g.mu.Unlock()
		return errors.New("sdk: serial group activate on closed group") //nolint:wrapcheck // new error, nothing to wrap
	}
	if g.active {
		g.mu.Unlock()
		return nil
	}
	if !g.configured {
		g.mu.Unlock()
		return errors.New("sdk: serial group activate before configure") //nolint:wrapcheck // new error, nothing to wrap
	}
	entities := copySerialEntities(g.entries)
	g.mu.Unlock()
	for i := range entities {
		err := callSerialCallback(entities[i].Activate)
		if err != nil {
			return fmt.Errorf("sdk: serial group activate entity %q: %w", entities[i].Name, err)
		}
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.closed {
		return errors.New("sdk: serial group activate on closed group") //nolint:wrapcheck // new error, nothing to wrap
	}
	g.active = true
	return nil
}

// Deactivate runs entity Deactivate in reverse attach order.
func (g *SerialGroup) Deactivate() error {
	if g == nil {
		return errors.New("sdk: serial group deactivate on nil group") //nolint:wrapcheck // new error, nothing to wrap
	}
	g.mu.Lock()
	if !g.active {
		g.mu.Unlock()
		return nil
	}
	entities := copySerialEntities(g.entries)
	g.mu.Unlock()
	for i := len(entities) - 1; i >= 0; i-- {
		err := callSerialCallback(entities[i].Deactivate)
		if err != nil {
			return fmt.Errorf("sdk: serial group deactivate entity %q: %w", entities[i].Name, err)
		}
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	g.active = false
	return nil
}

// Cleanup runs entity Cleanup in reverse attach order.
func (g *SerialGroup) Cleanup() error {
	if g == nil {
		return errors.New("sdk: serial group cleanup on nil group") //nolint:wrapcheck // new error, nothing to wrap
	}
	g.mu.Lock()
	if !g.configured {
		g.mu.Unlock()
		return nil
	}
	if g.active {
		g.mu.Unlock()
		return errors.New("sdk: serial group cleanup while active; deactivate first") //nolint:wrapcheck // new error, nothing to wrap
	}
	entities := copySerialEntities(g.entries)
	g.mu.Unlock()
	for i := len(entities) - 1; i >= 0; i-- {
		err := callSerialCallback(entities[i].Cleanup)
		if err != nil {
			return fmt.Errorf("sdk: serial group cleanup entity %q: %w", entities[i].Name, err)
		}
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	g.configured = false
	return nil
}

// Shutdown runs entity Shutdown in reverse attach order.
func (g *SerialGroup) Shutdown() error {
	if g == nil {
		return errors.New("sdk: serial group shutdown on nil group") //nolint:wrapcheck // new error, nothing to wrap
	}
	g.mu.Lock()
	entities := copySerialEntities(g.entries)
	g.mu.Unlock()
	for i := len(entities) - 1; i >= 0; i-- {
		err := callSerialCallback(entities[i].Shutdown)
		if err != nil {
			return fmt.Errorf("sdk: serial group shutdown entity %q: %w", entities[i].Name, err)
		}
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	g.active = false
	g.configured = false
	return nil
}

// SpinOnce runs at most one ready callback in scan order and reports
// whether work was done. Inactive or closed groups report false.
func (g *SerialGroup) SpinOnce() bool {
	if g == nil {
		return false
	}
	g.runMu.Lock()
	defer g.runMu.Unlock()
	takes := g.snapshotTakes()
	if takes == nil {
		return false
	}
	for _, take := range takes {
		if take == nil {
			continue
		}
		if take() {
			return true
		}
	}
	return false
}

// Close rejects new work and is idempotent.
func (g *SerialGroup) Close() error {
	if g == nil {
		return errors.New("sdk: serial group close on nil group") //nolint:wrapcheck // new error, nothing to wrap
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	g.closed = true
	return nil
}

// RecordHeartbeat records one bond heartbeat for an entity at nowNanos.
func (g *SerialGroup) RecordHeartbeat(entity string, nowNanos int64) error {
	if g == nil {
		return errors.New("sdk: serial group heartbeat on nil group") //nolint:wrapcheck // new error, nothing to wrap
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.closed {
		return errors.New("sdk: serial group heartbeat on closed group") //nolint:wrapcheck // new error, nothing to wrap
	}
	idx := findSerialIndex(g.entries, entity)
	if idx < 0 {
		return fmt.Errorf("sdk: serial group heartbeat unknown entity %q", entity)
	}
	g.entries[idx].lastHeartbeatNanos = nowNanos
	g.entries[idx].hasHeartbeat = true
	return nil
}

// BondAlive reports manager bond liveliness at nowNanos against the 4 s timeout.
func (g *SerialGroup) BondAlive(entity string, nowNanos int64) (bool, error) {
	if g == nil {
		return false, errors.New("sdk: serial group bond check on nil group") //nolint:wrapcheck // new error, nothing to wrap
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	idx := findSerialIndex(g.entries, entity)
	if idx < 0 {
		return false, fmt.Errorf("sdk: serial group bond check unknown entity %q", entity)
	}
	entry := g.entries[idx]
	return bondAliveAt(entry.hasHeartbeat, entry.lastHeartbeatNanos, nowNanos, SerialBondTimeoutNanos), nil
}

// CheckBonds returns names of entities whose bond is dead at nowNanos.
func (g *SerialGroup) CheckBonds(nowNanos int64) []string {
	if g == nil {
		return nil
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	var dead []string
	for _, entry := range g.entries {
		if !bondAliveAt(entry.hasHeartbeat, entry.lastHeartbeatNanos, nowNanos, SerialBondTimeoutNanos) {
			dead = append(dead, entry.entity.Name)
		}
	}
	return dead
}

// callSerialCallback runs fn unless nil. A nil callback is a no op success.
func callSerialCallback(fn func() error) error {
	if fn == nil {
		return nil
	}
	err := fn()
	if err != nil {
		return fmt.Errorf("sdk: serial callback: %w", err)
	}
	return nil
}

// findSerialIndex returns the index of name or minus one when absent.
func findSerialIndex(entries []*serialEntry, name string) int {
	for i := range entries {
		if entries[i].entity.Name == name {
			return i
		}
	}
	return -1
}

// copySerialEntities copies entity values in attach order.
func copySerialEntities(entries []*serialEntry) []SerialEntity {
	out := make([]SerialEntity, len(entries))
	for i := range entries {
		out[i] = entries[i].entity
	}
	return out
}

// sortSerialEntries orders entries by priority then insertion sequence.
func sortSerialEntries(out []*serialEntry) {
	slices.SortFunc(out, func(a, b *serialEntry) int {
		if c := cmp.Compare(a.entity.Priority, b.entity.Priority); c != 0 {
			return c
		}
		return cmp.Compare(a.seq, b.seq)
	})
}

// snapshotTakes copies Take callbacks in scan order. It returns nil when
// the group is closed or inactive so SpinOnce skips pending work.
func (g *SerialGroup) snapshotTakes() []func() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.closed || !g.active {
		return nil
	}
	out := make([]*serialEntry, len(g.entries))
	copy(out, g.entries)
	sortSerialEntries(out)
	takes := make([]func() bool, 0, len(out))
	for _, entry := range out {
		takes = append(takes, entry.entity.Take)
	}
	return takes
}

// bondAliveAt reports liveliness for one bond record. No heartbeat yet
// means not alive. A backward timestamp reports alive without rebasing.
func bondAliveAt(has bool, last, now, timeoutNanos int64) bool {
	if !has {
		return false
	}
	if now < last {
		return true
	}
	return now-last <= timeoutNanos
}

// PooledGroup is a bounded worker pool replacing reentrant callback
// groups. Tasks are plain callbacks; handlers run concurrently up to the
// worker bound with no ordering guarantee.
//
// REVIEW: a single shared queue with drop oldest overflow stands in for
// per-lane queues with fairness; one flooding producer shares one lost
// counter. Lane isolation is deferred to user review.
type PooledGroup struct {
	// mu guards queue plus lifecycle flags.
	mu sync.Mutex
	// cond wakes workers when tasks arrive or Close runs.
	cond *sync.Cond
	// queue holds pending tasks oldest first.
	queue []func()
	// workers is the fixed worker bound.
	workers int
	// depth bounds the queue; overflow drops oldest and counts lost.
	depth int
	// lost counts overflow drops.
	lost uint64
	// closed marks teardown; Close is idempotent.
	closed bool
	// wg joins workers on Close.
	wg sync.WaitGroup
}

// NewPooledGroup returns a pool with workers threads and depth slots.
func NewPooledGroup(workers, depth int) (*PooledGroup, error) {
	if workers <= 0 {
		return nil, errors.New("sdk: new pooled group: workers must be positive") //nolint:wrapcheck // new error, nothing to wrap
	}
	if depth <= 0 {
		return nil, errors.New("sdk: new pooled group: depth must be positive") //nolint:wrapcheck // new error, nothing to wrap
	}
	g := &PooledGroup{workers: workers, depth: depth}
	g.cond = sync.NewCond(&g.mu)
	g.wg.Add(workers)
	for range workers {
		go g.loop()
	}
	return g, nil
}

// Workers returns the declared worker bound. A nil group reports zero.
func (g *PooledGroup) Workers() int {
	if g == nil {
		return 0
	}
	return g.workers
}

// Depth returns the queue bound. A nil group reports zero.
func (g *PooledGroup) Depth() int {
	if g == nil {
		return 0
	}
	return g.depth
}

// Lost returns the monotonic count of dropped tasks. A nil group is zero.
func (g *PooledGroup) Lost() uint64 {
	if g == nil {
		return 0
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.lost
}

// Pending returns the queued task count. A nil group reports zero.
func (g *PooledGroup) Pending() int {
	if g == nil {
		return 0
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	return len(g.queue)
}

// Submit enqueues one task without blocking. A full queue drops the
// oldest task and counts one lost; the return reports the eviction.
func (g *PooledGroup) Submit(task func()) (bool, error) {
	if g == nil {
		return false, errors.New("sdk: pooled group submit on nil group") //nolint:wrapcheck // new error, nothing to wrap
	}
	if task == nil {
		return false, errors.New("sdk: pooled group submit with nil task") //nolint:wrapcheck // new error, nothing to wrap
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.closed {
		return false, errors.New("sdk: pooled group submit on closed group") //nolint:wrapcheck // new error, nothing to wrap
	}
	evicted := false
	if len(g.queue) >= g.depth {
		g.queue[0] = nil
		g.queue = append(g.queue[1:], task)
		g.lost++
		evicted = true
	} else {
		g.queue = append(g.queue, task)
	}
	g.cond.Signal()
	return evicted, nil
}

// Close discards pending tasks and joins workers cleanly.
func (g *PooledGroup) Close() {
	if g == nil {
		return
	}
	g.mu.Lock()
	if g.closed {
		g.mu.Unlock()
		return
	}
	g.closed = true
	for i := range g.queue {
		g.queue[i] = nil
	}
	g.queue = g.queue[:0]
	g.cond.Broadcast()
	g.mu.Unlock()
	g.wg.Wait()
}

// loop runs tasks until Close.
func (g *PooledGroup) loop() {
	defer g.wg.Done()
	for {
		task, ok := g.next()
		if !ok {
			return
		}
		task()
	}
}

// next waits for and claims the next task. It reports false on Close.
func (g *PooledGroup) next() (func(), bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	for {
		if g.closed {
			return nil, false
		}
		if len(g.queue) > 0 {
			task := g.queue[0]
			g.queue[0] = nil
			g.queue = g.queue[1:]
			return task, true
		}
		g.cond.Wait()
	}
}
