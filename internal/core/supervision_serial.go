// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package core

import (
	"cmp"
	"errors"
	"fmt"
	"runtime"
	"slices"
	"sync"
)

// Serial supervision invariants: one callback at a time in a stated order,
// subscriber owned bounded channels, forward bringup with reverse teardown,
// and bond liveness with explicit timeouts.
//
// Ordering: each SpinOnce scans entities sorted by priority ascending then
// insertion sequence ascending and runs the first ready Take. Lower priority
// values run first. Equal priority follows attach order. A constantly ready
// entity with a lower value starves higher values by design, which removes
// ambiguity about who starves whom. No two callbacks of one group overlap;
// concurrent SpinOnce callers serialize on an execution mutex.
//
// Delivery: each subscriber owns a bounded SerialChannel with depth from QoS
// and drop oldest overflow with a lost counter. AttachSerialChannel binds a
// channel to the group so its handler runs in serial order.
//
// Lifecycle: Configure plus Activate run forward in attach order. Deactivate
// plus Cleanup plus Shutdown run in reverse attach order. Late attaches while
// configured run Configure immediately, and while active run Activate
// immediately. After Deactivate, SpinOnce reports no work without calling
// Take, so new work is rejected. RegisterPreshutdown callbacks run in reverse
// registration order before entity Shutdown.
//
// Bond: heartbeats use caller supplied nanoseconds so tests stay
// deterministic. Heartbeat period is 0.25 s, check polling is 0.2 s, manager
// timeout is 4.0 s, service timeout is 5.0 s. An entity with no heartbeat yet
// is not alive. A backward timestamp reports alive without rebasing.
//
// Teardown: Destroy sets the destroyed flag, closes the done channel, skips
// pending Takes, and joins spinners. Destroy is idempotent. Lifecycle and
// attach calls serialize on the bringup goroutine; SpinOnce, Spin, Destroy,
// and bond probes are safe for concurrent use. Do not call Destroy from
// inside a Take callback; call it from another goroutine while Spin runs.

// Serial bond liveliness bounds in nanoseconds.
const (
	// SerialBondHeartbeatPeriodNanos is the bond heartbeat period of 0.25 s.
	SerialBondHeartbeatPeriodNanos int64 = 250000000
	// SerialBondCheckPeriodNanos is the bond check polling period of 0.2 s.
	SerialBondCheckPeriodNanos int64 = 200000000
	// SerialBondManagerTimeoutNanos is the manager bond timeout of 4.0 s.
	SerialBondManagerTimeoutNanos int64 = 4000000000
	// SerialBondServiceTimeoutNanos is the service bond timeout of 5.0 s.
	SerialBondServiceTimeoutNanos int64 = 5000000000
)

// SerialEntity is one unit of ordered work plus lifecycle and bond identity.
//
// Name is unique within the group. Priority orders delivery: lower values
// run first, equal values follow attach order. Lifecycle callbacks may be nil
// for no operation. Take performs at most one unit of work and reports
// whether work was done; nil Take means no polled work.
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
	// Take performs at most one unit of work and reports whether it did.
	Take func() bool
}

// serialEntry is one attached entity with its insertion sequence and bond state.
type serialEntry struct {
	// entity is immutable after insertion except through replacement on reattach.
	entity SerialEntity
	// seq is the insertion order used to break priority ties.
	seq uint64
	// lastHeartbeatNanos is the last recorded heartbeat time.
	lastHeartbeatNanos int64
	// hasHeartbeat is true after the first recorded heartbeat.
	hasHeartbeat bool
}

// SerialGroup delivers attached entities one at a time in priority plus
// insertion order with lifecycle supervision and bond liveliness.
//
// The zero value is unusable: build groups with NewSerialGroup. Do not copy
// a group after first use; use a pointer.
type SerialGroup struct {
	noCopy      noCopy
	mu          sync.Mutex
	runMu       sync.Mutex
	name        string
	entries     []*serialEntry
	nextSeq     uint64
	configured  bool
	active      bool
	destroyed   bool
	done        chan struct{}
	wg          sync.WaitGroup
	preshutdown []func()
}

// NewSerialGroup returns an empty serial supervision group.
//
// An empty name is an error. The group starts unconfigured and inactive.
func NewSerialGroup(name string) (*SerialGroup, error) {
	if name == "" {
		return nil, errors.New("core: serial group name is empty") //nolint:wrapcheck // new error, nothing to wrap
	}
	return &SerialGroup{name: name, done: make(chan struct{})}, nil
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

// Order returns entity names in scan order: priority ascending then insertion
// sequence ascending. A nil group returns nil. The result is a fresh slice.
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

// IsActive reports whether the group is active and delivering. A nil group is false.
func (g *SerialGroup) IsActive() bool {
	if g == nil {
		return false
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.active
}

// IsDestroyed reports whether Destroy ran. A nil group reports destroyed so
// callers skip work.
func (g *SerialGroup) IsDestroyed() bool {
	if g == nil {
		return true
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.destroyed
}

// Attach adds an entity with the next insertion sequence.
//
// Empty names and duplicate names are errors. Attaching to a destroyed group
// is an error. When the group is already configured the entity Configure runs
// before insertion, and when already active the entity Activate runs after
// Configure; a failing callback prevents insertion and the caller owns any
// partial Configure side effect. Lifecycle callbacks run without holding the
// group lock; serialize Attach with other bringup calls on one goroutine.
func (g *SerialGroup) Attach(e SerialEntity) error {
	if g == nil {
		return errors.New("core: serial group attach on nil group") //nolint:wrapcheck // new error, nothing to wrap
	}
	if e.Name == "" {
		return errors.New("core: serial group attach with empty entity name") //nolint:wrapcheck // new error, nothing to wrap
	}
	g.mu.Lock()
	if g.destroyed {
		g.mu.Unlock()
		return errors.New("core: serial group attach on destroyed group") //nolint:wrapcheck // new error, nothing to wrap
	}
	if findSerialIndex(g.entries, e.Name) >= 0 {
		g.mu.Unlock()
		return fmt.Errorf("core: serial group attach duplicate entity %q", e.Name)
	}
	configured := g.configured
	active := g.active
	g.mu.Unlock()
	if configured {
		err := callSerialCallback(e.Configure)
		if err != nil {
			return fmt.Errorf("core: serial group attach %q configure: %w", e.Name, err)
		}
	}
	if active {
		err := callSerialCallback(e.Activate)
		if err != nil {
			return fmt.Errorf("core: serial group attach %q activate: %w", e.Name, err)
		}
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.destroyed {
		return errors.New("core: serial group attach on destroyed group") //nolint:wrapcheck // new error, nothing to wrap
	}
	if findSerialIndex(g.entries, e.Name) >= 0 {
		return fmt.Errorf("core: serial group attach duplicate entity %q", e.Name)
	}
	g.entries = append(g.entries, &serialEntry{entity: e, seq: g.nextSeq})
	g.nextSeq++
	return nil
}

// Detach removes an entity by name.
//
// Empty and unknown names are errors. When the group is active the entity
// Deactivate runs before removal; a failing Deactivate keeps the entity
// attached for retry. Serialize Detach with other bringup calls.
func (g *SerialGroup) Detach(name string) error {
	if g == nil {
		return errors.New("core: serial group detach on nil group") //nolint:wrapcheck // new error, nothing to wrap
	}
	if name == "" {
		return errors.New("core: serial group detach with empty entity name") //nolint:wrapcheck // new error, nothing to wrap
	}
	g.mu.Lock()
	idx := findSerialIndex(g.entries, name)
	if idx < 0 {
		g.mu.Unlock()
		return fmt.Errorf("core: serial group detach unknown entity %q", name)
	}
	active := g.active
	entity := g.entries[idx].entity
	g.mu.Unlock()
	if active {
		err := callSerialCallback(entity.Deactivate)
		if err != nil {
			return fmt.Errorf("core: serial group detach %q deactivate: %w", name, err)
		}
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	idx = findSerialIndex(g.entries, name)
	if idx < 0 {
		return fmt.Errorf("core: serial group detach unknown entity %q", name)
	}
	copy(g.entries[idx:], g.entries[idx+1:])
	g.entries[len(g.entries)-1] = nil
	g.entries = g.entries[:len(g.entries)-1]
	return nil
}

// RegisterPreshutdown records a callback run before entity Shutdown.
//
// A nil callback or a destroyed group is an error. Callbacks run in reverse
// registration order inside Shutdown.
func (g *SerialGroup) RegisterPreshutdown(fn func()) error {
	if g == nil {
		return errors.New("core: serial group preshutdown on nil group") //nolint:wrapcheck // new error, nothing to wrap
	}
	if fn == nil {
		return errors.New("core: serial group preshutdown callback is nil") //nolint:wrapcheck // new error, nothing to wrap
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.destroyed {
		return errors.New("core: serial group preshutdown on destroyed group") //nolint:wrapcheck // new error, nothing to wrap
	}
	g.preshutdown = append(g.preshutdown, fn)
	return nil
}

// Configure runs entity Configure forward in attach order.
//
// Success marks the group configured. The call is idempotent once
// configured. A destroyed group and the first failing callback are errors;
// the group stays unconfigured on failure.
func (g *SerialGroup) Configure() error {
	if g == nil {
		return errors.New("core: serial group configure on nil group") //nolint:wrapcheck // new error, nothing to wrap
	}
	g.mu.Lock()
	if g.destroyed {
		g.mu.Unlock()
		return errors.New("core: serial group configure on destroyed group") //nolint:wrapcheck // new error, nothing to wrap
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
			return fmt.Errorf("core: serial group configure entity %q: %w", entities[i].Name, err)
		}
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.destroyed {
		return errors.New("core: serial group configure on destroyed group") //nolint:wrapcheck // new error, nothing to wrap
	}
	g.configured = true
	return nil
}

// Activate runs entity Activate forward in attach order.
//
// The group must be configured first. Success marks the group active and
// SpinOnce starts delivering. The call is idempotent once active. A
// destroyed group and the first failing callback are errors.
func (g *SerialGroup) Activate() error {
	if g == nil {
		return errors.New("core: serial group activate on nil group") //nolint:wrapcheck // new error, nothing to wrap
	}
	g.mu.Lock()
	if g.destroyed {
		g.mu.Unlock()
		return errors.New("core: serial group activate on destroyed group") //nolint:wrapcheck // new error, nothing to wrap
	}
	if g.active {
		g.mu.Unlock()
		return nil
	}
	if !g.configured {
		g.mu.Unlock()
		return errors.New("core: serial group activate before configure") //nolint:wrapcheck // new error, nothing to wrap
	}
	entities := copySerialEntities(g.entries)
	g.mu.Unlock()
	for i := range entities {
		err := callSerialCallback(entities[i].Activate)
		if err != nil {
			return fmt.Errorf("core: serial group activate entity %q: %w", entities[i].Name, err)
		}
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.destroyed {
		return errors.New("core: serial group activate on destroyed group") //nolint:wrapcheck // new error, nothing to wrap
	}
	g.active = true
	return nil
}

// Deactivate runs entity Deactivate in reverse attach order.
//
// Success marks the group inactive and SpinOnce stops calling Take, which
// rejects new work. The call is idempotent once inactive. The first failing
// callback is an error and the group stays active for retry.
func (g *SerialGroup) Deactivate() error {
	if g == nil {
		return errors.New("core: serial group deactivate on nil group") //nolint:wrapcheck // new error, nothing to wrap
	}
	g.mu.Lock()
	if !g.active {
		g.mu.Unlock()
		return nil
	}
	entities := copySerialEntities(g.entries)
	g.mu.Unlock()
	for i := range slices.Backward(entities) {
		err := callSerialCallback(entities[i].Deactivate)
		if err != nil {
			return fmt.Errorf("core: serial group deactivate entity %q: %w", entities[i].Name, err)
		}
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	g.active = false
	return nil
}

// Cleanup runs entity Cleanup in reverse attach order.
//
// The group must be inactive; deactivating first is required. Success clears
// the configured flag. The call is idempotent once unconfigured. The first
// failing callback is an error.
func (g *SerialGroup) Cleanup() error {
	if g == nil {
		return errors.New("core: serial group cleanup on nil group") //nolint:wrapcheck // new error, nothing to wrap
	}
	g.mu.Lock()
	if !g.configured {
		g.mu.Unlock()
		return nil
	}
	if g.active {
		g.mu.Unlock()
		return errors.New("core: serial group cleanup while active; deactivate first") //nolint:wrapcheck // new error, nothing to wrap
	}
	entities := copySerialEntities(g.entries)
	g.mu.Unlock()
	for i := range slices.Backward(entities) {
		err := callSerialCallback(entities[i].Cleanup)
		if err != nil {
			return fmt.Errorf("core: serial group cleanup entity %q: %w", entities[i].Name, err)
		}
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	g.configured = false
	return nil
}

// Shutdown runs preshutdown callbacks then entity Shutdown in reverse order.
//
// Preshutdown callbacks run in reverse registration order before entities,
// which run in reverse attach order. Shutdown clears active and configured so
// the group can be brought up again. The first failing callback is an error.
func (g *SerialGroup) Shutdown() error {
	if g == nil {
		return errors.New("core: serial group shutdown on nil group") //nolint:wrapcheck // new error, nothing to wrap
	}
	g.mu.Lock()
	entities := copySerialEntities(g.entries)
	preshutdown := make([]func(), len(g.preshutdown))
	copy(preshutdown, g.preshutdown)
	g.mu.Unlock()
	for i := range slices.Backward(preshutdown) {
		if preshutdown[i] != nil {
			preshutdown[i]()
		}
	}
	for i := range slices.Backward(entities) {
		err := callSerialCallback(entities[i].Shutdown)
		if err != nil {
			return fmt.Errorf("core: serial group shutdown entity %q: %w", entities[i].Name, err)
		}
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	g.active = false
	g.configured = false
	return nil
}

// SpinOnce runs at most one ready callback in scan order and reports whether
// work was done.
//
// SpinOnce does nothing and reports false when the group is not active or is
// destroyed, without calling any Take. Ready entities of equal priority run
// in insertion order. Concurrent SpinOnce callers serialize so callbacks
// never overlap.
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

// Spin delivers callbacks until stop closes or Destroy runs.
//
// Spin is the detached thread entry point: run it with go Spin(stop) where
// ROS uses a detached executor. A nil stop means run until Destroy. Idle
// spins yield instead of blocking so Destroy and stop stay responsive. Do not
// call Destroy from inside a Take callback; call it from the goroutine that
// owns teardown while Spin runs elsewhere.
func (g *SerialGroup) Spin(stop <-chan struct{}) {
	if g == nil {
		return
	}
	g.wg.Add(1)
	defer g.wg.Done()
	for {
		select {
		case <-g.done:
			return
		case <-stop:
			return
		default:
		}
		if g.IsDestroyed() {
			return
		}
		if g.SpinOnce() {
			continue
		}
		select {
		case <-g.done:
			return
		case <-stop:
			return
		default:
			runtime.Gosched()
		}
	}
}

// Destroy stops delivery, skips pending callbacks, and joins spinners.
//
// Destroy is idempotent and safe for concurrent use with Spin and SpinOnce.
// After Destroy, SpinOnce reports false without calling Take. A nil group is
// a no op.
func (g *SerialGroup) Destroy() {
	if g == nil {
		return
	}
	g.mu.Lock()
	if !g.destroyed {
		g.destroyed = true
		close(g.done)
	}
	g.mu.Unlock()
	g.wg.Wait()
}

// RecordHeartbeat records one bond heartbeat for an entity at nowNanos.
//
// Unknown entities and destroyed groups are errors.
func (g *SerialGroup) RecordHeartbeat(entity string, nowNanos int64) error {
	if g == nil {
		return errors.New("core: serial group heartbeat on nil group") //nolint:wrapcheck // new error, nothing to wrap
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.destroyed {
		return errors.New("core: serial group heartbeat on destroyed group") //nolint:wrapcheck // new error, nothing to wrap
	}
	idx := findSerialIndex(g.entries, entity)
	if idx < 0 {
		return fmt.Errorf("core: serial group heartbeat unknown entity %q", entity)
	}
	g.entries[idx].lastHeartbeatNanos = nowNanos
	g.entries[idx].hasHeartbeat = true
	return nil
}

// BondAlive reports manager bond liveness at nowNanos against the 4.0 s timeout.
//
// An entity with no heartbeat yet is not alive. A backward timestamp reports
// alive without rebasing. Unknown entities are errors.
func (g *SerialGroup) BondAlive(entity string, nowNanos int64) (bool, error) {
	if g == nil {
		return false, errors.New("core: serial group bond check on nil group") //nolint:wrapcheck // new error, nothing to wrap
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	idx := findSerialIndex(g.entries, entity)
	if idx < 0 {
		return false, fmt.Errorf("core: serial group bond check unknown entity %q", entity)
	}
	entry := g.entries[idx]
	return bondAliveAt(entry.hasHeartbeat, entry.lastHeartbeatNanos, nowNanos, SerialBondManagerTimeoutNanos), nil
}

// BondServiceAlive reports service bond liveness at nowNanos against the
// 5.0 s service timeout. Rules match BondAlive with the longer timeout.
func (g *SerialGroup) BondServiceAlive(entity string, nowNanos int64) (bool, error) {
	if g == nil {
		return false, errors.New("core: serial group service bond check on nil group") //nolint:wrapcheck // new error, nothing to wrap
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	idx := findSerialIndex(g.entries, entity)
	if idx < 0 {
		return false, fmt.Errorf("core: serial group service bond check unknown entity %q", entity)
	}
	entry := g.entries[idx]
	return bondAliveAt(entry.hasHeartbeat, entry.lastHeartbeatNanos, nowNanos, SerialBondServiceTimeoutNanos), nil
}

// CheckBonds returns names of entities whose manager bond is dead at nowNanos.
//
// Entities with no heartbeat yet count as dead. A nil group returns nil. Use
// it with SerialBondCheckPeriodNanos polling and respawn on loss.
func (g *SerialGroup) CheckBonds(nowNanos int64) []string {
	if g == nil {
		return nil
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	var dead []string
	for _, entry := range g.entries {
		if !bondAliveAt(entry.hasHeartbeat, entry.lastHeartbeatNanos, nowNanos, SerialBondManagerTimeoutNanos) {
			dead = append(dead, entry.entity.Name)
		}
	}
	return dead
}

// CheckServiceBonds returns names of entities whose service bond is dead at
// nowNanos against the 5.0 s timeout. Rules match CheckBonds otherwise.
func (g *SerialGroup) CheckServiceBonds(nowNanos int64) []string {
	if g == nil {
		return nil
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	var dead []string
	for _, entry := range g.entries {
		if !bondAliveAt(entry.hasHeartbeat, entry.lastHeartbeatNanos, nowNanos, SerialBondServiceTimeoutNanos) {
			dead = append(dead, entry.entity.Name)
		}
	}
	return dead
}

// BondLastSeen returns the last heartbeat time and whether one was recorded.
// Unknown entities are errors.
func (g *SerialGroup) BondLastSeen(entity string) (int64, bool, error) {
	if g == nil {
		return 0, false, errors.New("core: serial group bond last seen on nil group") //nolint:wrapcheck // new error, nothing to wrap
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	idx := findSerialIndex(g.entries, entity)
	if idx < 0 {
		return 0, false, fmt.Errorf("core: serial group bond last seen unknown entity %q", entity)
	}
	return g.entries[idx].lastHeartbeatNanos, g.entries[idx].hasHeartbeat, nil
}

// SerialChannel is a subscriber owned bounded queue with drop oldest overflow.
//
// Depth comes from QoS through NewSerialChannelFromQoS or explicitly through
// NewSerialChannel. TryPublish never blocks: a full channel drops the oldest
// sample, counts one lost, and stores the new one. Take pops the oldest
// without blocking. Close rejects further publishes while Take drains what
// remains. Do not copy a channel after first use; use a pointer.
type SerialChannel[T any] struct {
	noCopy   noCopy
	mu       sync.Mutex
	buf      []T
	capacity int
	lost     uint64
	closed   bool
}

// NewSerialChannel returns an empty subscriber channel holding depth samples.
//
// Depth must be positive; derive it from RetentionCapacity for QoS profiles.
func NewSerialChannel[T any](depth int) (*SerialChannel[T], error) {
	if depth <= 0 {
		return nil, fmt.Errorf("core: serial channel depth %d must be positive; derive depth from RetentionCapacity", depth)
	}
	return &SerialChannel[T]{capacity: depth}, nil
}

// NewSerialChannelFromQoS returns a subscriber channel with depth from the
// profile retention bound: keep last depth or the keep all bound.
//
// Unresolved profiles are errors through RetentionCapacity.
func NewSerialChannelFromQoS[T any](p QoSProfile) (*SerialChannel[T], error) {
	capacity, err := RetentionCapacity(p)
	if err != nil {
		return nil, fmt.Errorf("core: serial channel from QoS: %w", err)
	}
	const maxInt = uint64(int(^uint(0) >> 1))
	if capacity > maxInt {
		return nil, fmt.Errorf("core: serial channel from QoS: capacity %d exceeds int range", capacity)
	}
	channel, err := NewSerialChannel[T](int(capacity))
	if err != nil {
		return nil, fmt.Errorf("core: serial channel from QoS: %w", err)
	}
	return channel, nil
}

// Depth returns the channel bound. A nil channel reports zero.
func (c *SerialChannel[T]) Depth() int {
	if c == nil {
		return 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.capacity
}

// Len returns the queued sample count. A nil channel reports zero.
func (c *SerialChannel[T]) Len() int {
	if c == nil {
		return 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.buf)
}

// Lost returns the monotonic count of samples dropped on overflow. A nil
// channel reports zero.
func (c *SerialChannel[T]) Lost() uint64 {
	if c == nil {
		return 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.lost
}

// Closed reports whether Close ran. A nil channel reports closed so callers
// treat it as unusable.
func (c *SerialChannel[T]) Closed() bool {
	if c == nil {
		return true
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closed
}

// TryPublish enqueues v without blocking and reports whether an oldest sample
// was evicted. A full channel drops the oldest, counts one lost, and stores
// v. A nil or closed channel drops v and reports no eviction.
func (c *SerialChannel[T]) TryPublish(v T) bool {
	if c == nil {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return false
	}
	if len(c.buf) >= c.capacity {
		var zero T
		c.buf[0] = zero
		c.buf = append(c.buf[1:], v)
		c.lost++
		return true
	}
	c.buf = append(c.buf, v)
	return false
}

// Take dequeues the oldest sample without blocking. It reports false when
// empty. A nil channel reports false.
func (c *SerialChannel[T]) Take() (T, bool) {
	var zero T
	if c == nil {
		return zero, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.buf) == 0 {
		return zero, false
	}
	v := c.buf[0]
	c.buf[0] = zero
	c.buf = c.buf[1:]
	return v, true
}

// Close rejects further publishes while Take drains what remains. A nil
// channel is a no op.
func (c *SerialChannel[T]) Close() {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closed = true
}

// AttachSerialChannel binds a subscriber channel to the group so its handler
// runs in serial order.
//
// The handler runs inside SpinOnce, hence one at a time with the group scan
// order. A nil group, nil channel, nil handler, or empty name is an error.
func AttachSerialChannel[T any](g *SerialGroup, name string, priority int, ch *SerialChannel[T], handle func(T)) error {
	if g == nil {
		return errors.New("core: serial attach channel on nil group") //nolint:wrapcheck // new error, nothing to wrap
	}
	if name == "" {
		return errors.New("core: serial attach channel with empty entity name") //nolint:wrapcheck // new error, nothing to wrap
	}
	if ch == nil {
		return errors.New("core: serial attach channel on nil channel") //nolint:wrapcheck // new error, nothing to wrap
	}
	if handle == nil {
		return errors.New("core: serial attach channel with nil handler") //nolint:wrapcheck // new error, nothing to wrap
	}
	entity := SerialEntity{
		Name:     name,
		Priority: priority,
		Take: func() bool {
			v, ok := ch.Take()
			if !ok {
				return false
			}
			handle(v)
			return true
		},
	}
	err := g.Attach(entity)
	if err != nil {
		return fmt.Errorf("core: serial attach channel %q: %w", name, err)
	}
	return nil
}

// callSerialCallback runs fn unless nil. A nil callback is a no op success.
func callSerialCallback(fn func() error) error {
	if fn == nil {
		return nil
	}
	err := fn()
	if err != nil {
		return fmt.Errorf("core: serial callback: %w", err)
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

// copySerialEntities copies entity values in attach order. Callers hold no
// lock on return; the caller must hold the group lock while calling.
func copySerialEntities(entries []*serialEntry) []SerialEntity {
	out := make([]SerialEntity, len(entries))
	for i := range entries {
		out[i] = entries[i].entity
	}
	return out
}

// sortSerialEntries orders entries by priority ascending then insertion
// sequence ascending.
func sortSerialEntries(out []*serialEntry) {
	slices.SortFunc(out, func(a, b *serialEntry) int {
		if c := cmp.Compare(a.entity.Priority, b.entity.Priority); c != 0 {
			return c
		}
		return cmp.Compare(a.seq, b.seq)
	})
}

// snapshotTakes copies Take callbacks in scan order. It returns nil when the
// group is destroyed or inactive so SpinOnce skips pending work.
func (g *SerialGroup) snapshotTakes() []func() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.destroyed || !g.active {
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

// bondAliveAt reports liveness for one bond record. No heartbeat yet means
// not alive. A backward timestamp reports alive without rebasing.
func bondAliveAt(has bool, last, now, timeoutNanos int64) bool {
	if !has {
		return false
	}
	if now < last {
		return true
	}
	return now-last <= timeoutNanos
}
