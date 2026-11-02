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
// review; callers drive delivery with SpinOnce.
//
// Close versus Node.Close: Node.Close tears down tracked publishers,
// subscriptions, servers, and clients, while SerialGroup.Close only marks
// the group closed and rejects new work. It never runs Deactivate,
// Cleanup, or Shutdown; the caller must drive the lifecycle explicitly
// before Close. Close stays idempotent and a second Close is a quiet
// success. Empty query results are always non-nil slices.
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
	// detaching marks names with a Detach in flight so concurrent Detach
	// calls run Deactivate exactly once. The first Detach marks under mu,
	// runs Deactivate outside the lock, then removes under mu. A second
	// Detach for the same name sees the mark and fails without running
	// the callback again.
	detaching map[string]bool
	// configuring reports a Configure in progress. Attach treats it like
	// configured for needing Configure so a late Attach during Configure
	// never lands unconfigured.
	configuring bool
	// activating reports an Activate in progress. Attach treats it like
	// active for needing Activate.
	activating bool
}

// NewSerialGroup returns an empty serial supervision group.
func NewSerialGroup(name string) (*SerialGroup, error) {
	if name == "" {
		return nil, errors.New("sdk: serial group name is empty") //nolint:wrapcheck // new error, nothing to wrap
	}
	return &SerialGroup{name: name, detaching: make(map[string]bool)}, nil
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

// Order returns entity names in scan order. A nil group returns a
// non-nil empty slice. The result is always non-nil.
func (g *SerialGroup) Order() []string {
	if g == nil {
		return []string{}
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
//
// Attach is generation-checked: it snapshots configured, active,
// configuring, and activating, runs the required callbacks outside the
// lock, then rechecks the generation under the lock before inserting.
// Configuring counts like configured and activating counts like active,
// so a late Attach during Configure or Activate runs the missed callbacks
// before inserting and never lands unconfigured or inactive. When the
// generation moves forward (false to true), Attach runs the missed
// callbacks before inserting. When it moves backward (true to false via
// Cleanup, Deactivate, or Shutdown), Attach fails loudly without
// inserting because it cannot un-run callbacks; the caller retries after
// the lifecycle settles.
func (g *SerialGroup) Attach(e SerialEntity) error {
	if g == nil {
		return errors.New("sdk: serial group attach on nil group") //nolint:wrapcheck // new error, nothing to wrap
	}
	if e.Name == "" {
		return fmt.Errorf("sdk: serial group %q attach with empty entity name", g.name)
	}
	snap, err := g.snapshotAttach(e.Name)
	if err != nil {
		return err
	}
	ranConfigure := false
	ranActivate := false
	for {
		err = runAttachCallbacks(g.name, e, snap.configured, snap.active, &ranConfigure, &ranActivate)
		if err != nil {
			return err
		}
		done, next, err := g.tryAttachInsert(e, snap, ranConfigure, ranActivate)
		if err != nil {
			return err
		}
		if done {
			return nil
		}
		snap = next
	}
}

// serialAttachSnap pins one generation of lifecycle flags for Attach.
type serialAttachSnap struct {
	// configured counts configured or configuring as needing Configure.
	configured bool
	// active counts active or activating as needing Activate.
	active bool
	// configuring reports Configure in progress.
	configuring bool
	// activating reports Activate in progress.
	activating bool
	// cfgFlag is the committed configured flag.
	cfgFlag bool
	// actFlag is the committed active flag.
	actFlag bool
}

// snapshotAttach snapshots one generation while checking closed and
// duplicate. The caller must not hold mu.
func (g *SerialGroup) snapshotAttach(name string) (serialAttachSnap, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.closed {
		return serialAttachSnap{}, fmt.Errorf("sdk: serial group %q attach on closed group", g.name)
	}
	if findSerialIndex(g.entries, name) >= 0 {
		return serialAttachSnap{}, fmt.Errorf("sdk: serial group %q attach duplicate entity %q", g.name, name)
	}
	if g.detaching[name] {
		return serialAttachSnap{}, fmt.Errorf("sdk: serial group %q attach duplicate entity %q", g.name, name)
	}
	return serialAttachSnap{
		configured:  g.configured || g.configuring,
		active:      g.active || g.activating,
		configuring: g.configuring,
		activating:  g.activating,
		cfgFlag:     g.configured,
		actFlag:     g.active,
	}, nil
}

// runAttachCallbacks runs the missed Configure and Activate for one
// generation outside the group lock.
func runAttachCallbacks(group string, e SerialEntity, needConfigured, needActive bool, ranConfigure, ranActivate *bool) error {
	if needConfigured && !*ranConfigure {
		err := callSerialCallback(e.Configure)
		if err != nil {
			return fmt.Errorf("sdk: serial group %q attach %q configure: %w", group, e.Name, err)
		}
		*ranConfigure = true
	}
	if needActive && !*ranActivate {
		err := callSerialCallback(e.Activate)
		if err != nil {
			return fmt.Errorf("sdk: serial group %q attach %q activate: %w", group, e.Name, err)
		}
		*ranActivate = true
	}
	return nil
}

// tryAttachInsert inserts when the generation is stable, else returns the
// next generation for another round or a retry error when the generation
// moved backward after callbacks ran.
func (g *SerialGroup) tryAttachInsert(e SerialEntity, snap serialAttachSnap, ranConfigure, ranActivate bool) (bool, serialAttachSnap, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	err := checkAttachInsertLocked(g, e.Name)
	if err != nil {
		return false, serialAttachSnap{}, err
	}
	cur := serialAttachSnap{
		configured:  g.configured || g.configuring,
		active:      g.active || g.activating,
		configuring: g.configuring,
		activating:  g.activating,
		cfgFlag:     g.configured,
		actFlag:     g.active,
	}
	if cur == snap {
		g.entries = append(g.entries, &serialEntry{entity: e, seq: g.nextSeq})
		g.nextSeq++
		return true, serialAttachSnap{}, nil
	}
	err = checkAttachBackward(g, e.Name, ranConfigure, ranActivate)
	if err != nil {
		return false, serialAttachSnap{}, err
	}
	return false, cur, nil
}

// checkAttachInsertLocked rejects closed and duplicate names. The caller
// must hold mu.
func checkAttachInsertLocked(g *SerialGroup, name string) error {
	if g.closed {
		return fmt.Errorf("sdk: serial group %q attach on closed group", g.name)
	}
	if findSerialIndex(g.entries, name) >= 0 {
		return fmt.Errorf("sdk: serial group %q attach duplicate entity %q", g.name, name)
	}
	if g.detaching[name] {
		return fmt.Errorf("sdk: serial group %q attach duplicate entity %q", g.name, name)
	}
	return nil
}

// checkAttachBackward fails when callbacks already ran but the generation
// moved backward. The caller must hold mu.
func checkAttachBackward(g *SerialGroup, name string, ranConfigure, ranActivate bool) error {
	if ranConfigure && !g.configured && !g.configuring {
		return fmt.Errorf("sdk: serial group %q attach %q: concurrent lifecycle change; retry", g.name, name)
	}
	if ranActivate && !g.active && !g.activating {
		return fmt.Errorf("sdk: serial group %q attach %q: concurrent lifecycle change; retry", g.name, name)
	}
	return nil
}

// Detach removes an entity by name. When active, Deactivate runs first,
// exactly once: the first Detach marks detaching under the lock, runs
// Deactivate outside the lock, then removes under the lock. A concurrent
// Detach for the same name sees the mark and fails without running
// Deactivate again. A failing Deactivate leaves the entry present for
// retry and clears the mark.
func (g *SerialGroup) Detach(name string) error {
	if g == nil {
		return errors.New("sdk: serial group detach on nil group") //nolint:wrapcheck // new error, nothing to wrap
	}
	if name == "" {
		return fmt.Errorf("sdk: serial group %q detach with empty entity name", g.name)
	}
	g.mu.Lock()
	idx := findSerialIndex(g.entries, name)
	if idx < 0 {
		g.mu.Unlock()
		return fmt.Errorf("sdk: serial group %q detach unknown entity %q", g.name, name)
	}
	if g.detaching[name] {
		g.mu.Unlock()
		return fmt.Errorf("sdk: serial group %q detach %q already detaching", g.name, name)
	}
	g.detaching[name] = true
	active := g.active
	entity := g.entries[idx].entity
	g.mu.Unlock()
	if active {
		err := callSerialCallback(entity.Deactivate)
		if err != nil {
			g.mu.Lock()
			delete(g.detaching, name)
			g.mu.Unlock()
			return fmt.Errorf("sdk: serial group %q detach %q deactivate: %w", g.name, name, err)
		}
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	delete(g.detaching, name)
	idx = findSerialIndex(g.entries, name)
	if idx < 0 {
		return fmt.Errorf("sdk: serial group %q detach unknown entity %q", g.name, name)
	}
	copy(g.entries[idx:], g.entries[idx+1:])
	g.entries[len(g.entries)-1] = nil
	g.entries = g.entries[:len(g.entries)-1]
	return nil
}

// Configure runs entity Configure forward in attach order.
//
// Fail-fast partial contract: on the first failing entity Configure
// stops and returns without setting configured, leaving a half-driven
// group. Entities before the failure already ran Configure while later
// entities did not. IsConfigured stays false and the group is not usable
// until the caller retries Configure to completion before Activate.
// Retrying reruns from the first entity in order, so Configure callbacks
// should tolerate a second run after a partial failure. Callback order
// stays forward in attach order.
func (g *SerialGroup) Configure() error {
	if g == nil {
		return errors.New("sdk: serial group configure on nil group") //nolint:wrapcheck // new error, nothing to wrap
	}
	g.mu.Lock()
	if g.closed {
		g.mu.Unlock()
		return fmt.Errorf("sdk: serial group %q configure on closed group", g.name)
	}
	if g.configured {
		g.mu.Unlock()
		return nil
	}
	if g.configuring {
		g.mu.Unlock()
		return fmt.Errorf("sdk: serial group %q configure already in progress", g.name)
	}
	g.configuring = true
	entities := copySerialEntities(g.entries)
	g.mu.Unlock()
	for i := range entities {
		err := callSerialCallback(entities[i].Configure)
		if err != nil {
			g.mu.Lock()
			g.configuring = false
			g.mu.Unlock()
			return fmt.Errorf("sdk: serial group %q configure entity %q: %w", g.name, entities[i].Name, err)
		}
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.closed {
		g.configuring = false
		return fmt.Errorf("sdk: serial group %q configure on closed group", g.name)
	}
	g.configured = true
	g.configuring = false
	return nil
}

// Activate runs entity Activate forward in attach order.
//
// Fail-fast partial contract: on the first failing entity Activate stops
// and returns without setting active, leaving a half-driven group.
// Entities before the failure already ran Activate while later entities
// did not. IsActive stays false and SpinOnce stays idle until the caller
// retries Activate to completion. Retrying reruns from the first entity
// in order. Drive order is Configure before Activate. Callback order
// stays forward in attach order.
func (g *SerialGroup) Activate() error {
	if g == nil {
		return errors.New("sdk: serial group activate on nil group") //nolint:wrapcheck // new error, nothing to wrap
	}
	g.mu.Lock()
	if g.closed {
		g.mu.Unlock()
		return fmt.Errorf("sdk: serial group %q activate on closed group", g.name)
	}
	if g.active {
		g.mu.Unlock()
		return nil
	}
	if !g.configured {
		g.mu.Unlock()
		return fmt.Errorf("sdk: serial group %q activate before configure", g.name)
	}
	if g.activating {
		g.mu.Unlock()
		return fmt.Errorf("sdk: serial group %q activate already in progress", g.name)
	}
	g.activating = true
	entities := copySerialEntities(g.entries)
	g.mu.Unlock()
	for i := range entities {
		err := callSerialCallback(entities[i].Activate)
		if err != nil {
			g.mu.Lock()
			g.activating = false
			g.mu.Unlock()
			return fmt.Errorf("sdk: serial group %q activate entity %q: %w", g.name, entities[i].Name, err)
		}
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.closed {
		g.activating = false
		return fmt.Errorf("sdk: serial group %q activate on closed group", g.name)
	}
	g.active = true
	g.activating = false
	return nil
}

// Deactivate runs entity Deactivate in reverse attach order.
//
// Fail-fast partial contract: on the first failing entity Deactivate
// stops and returns without clearing active, leaving a half-driven group.
// Entities after the failure in reverse order already ran Deactivate
// while earlier entities did not. IsActive stays true until the caller
// retries Deactivate to completion before Cleanup. Detach in flight for
// one name is skipped here because that Detach owns its Deactivate
// exactly once. Callback order stays reverse in attach order.
func (g *SerialGroup) Deactivate() error {
	if g == nil {
		return errors.New("sdk: serial group deactivate on nil group") //nolint:wrapcheck // new error, nothing to wrap
	}
	g.mu.Lock()
	if !g.active {
		g.mu.Unlock()
		return nil
	}
	entities := copySerialEntitiesExcluding(g.entries, g.detaching)
	g.mu.Unlock()
	for _, entity := range slices.Backward(entities) {
		err := callSerialCallback(entity.Deactivate)
		if err != nil {
			return fmt.Errorf("sdk: serial group %q deactivate entity %q: %w", g.name, entity.Name, err)
		}
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	g.active = false
	return nil
}

// Cleanup runs entity Cleanup in reverse attach order.
//
// Fail-fast partial contract: on the first failing entity Cleanup stops
// and returns without clearing configured, leaving a half-driven group.
// Entities after the failure in reverse order already ran Cleanup while
// earlier entities did not. IsConfigured stays true until the caller
// retries Cleanup to completion. Drive order is Deactivate before
// Cleanup. Callback order stays reverse in attach order.
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
		return fmt.Errorf("sdk: serial group %q cleanup while active; deactivate first", g.name)
	}
	entities := copySerialEntities(g.entries)
	g.mu.Unlock()
	for _, entity := range slices.Backward(entities) {
		err := callSerialCallback(entity.Cleanup)
		if err != nil {
			return fmt.Errorf("sdk: serial group %q cleanup entity %q: %w", g.name, entity.Name, err)
		}
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	g.configured = false
	return nil
}

// Shutdown runs entity Shutdown in reverse attach order.
//
// Fail-fast partial contract: on the first failing entity Shutdown stops
// and returns without clearing active and configured, leaving a
// half-driven group. Entities after the failure in reverse order already
// ran Shutdown while earlier entities did not. The caller retries
// Shutdown to completion before Close. Shutdown is terminal teardown and
// runs even when inactive. Callback order stays reverse in attach order.
func (g *SerialGroup) Shutdown() error {
	if g == nil {
		return errors.New("sdk: serial group shutdown on nil group") //nolint:wrapcheck // new error, nothing to wrap
	}
	g.mu.Lock()
	entities := copySerialEntities(g.entries)
	g.mu.Unlock()
	for _, entity := range slices.Backward(entities) {
		err := callSerialCallback(entity.Shutdown)
		if err != nil {
			return fmt.Errorf("sdk: serial group %q shutdown entity %q: %w", g.name, entity.Name, err)
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
	if len(takes) == 0 {
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
//
// Close only marks the group closed and rejects new work; it never runs
// Deactivate, Cleanup, or Shutdown. Drive the lifecycle explicitly before
// Close. This diverges from Node.Close, which tears down tracked entities.
// A second Close is a quiet success.
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
		return fmt.Errorf("sdk: serial group %q heartbeat on closed group", g.name)
	}
	idx := findSerialIndex(g.entries, entity)
	if idx < 0 {
		return fmt.Errorf("sdk: serial group %q heartbeat unknown entity %q", g.name, entity)
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
		return false, fmt.Errorf("sdk: serial group %q bond check unknown entity %q", g.name, entity)
	}
	entry := g.entries[idx]
	return bondAliveAt(entry.hasHeartbeat, entry.lastHeartbeatNanos, nowNanos, SerialBondTimeoutNanos), nil
}

// CheckBonds returns names of entities whose bond is dead at nowNanos.
//
// The result is always non-nil; a nil group yields an empty slice and a
// healthy group yields an empty slice, never nil.
func (g *SerialGroup) CheckBonds(nowNanos int64) []string {
	if g == nil {
		return []string{}
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	dead := make([]string, 0)
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

// copySerialEntitiesExcluding copies entity values in attach order while
// skipping names marked detaching. The caller must hold mu. It keeps
// Detach as the single owner of its Deactivate so a concurrent group
// Deactivate never runs the same callback twice.
func copySerialEntitiesExcluding(entries []*serialEntry, detaching map[string]bool) []SerialEntity {
	out := make([]SerialEntity, 0, len(entries))
	for i := range entries {
		if detaching != nil && detaching[entries[i].entity.Name] {
			continue
		}
		out = append(out, entries[i].entity)
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

// snapshotTakes copies Take callbacks in scan order. It returns an empty
// non-nil slice when the group is closed or inactive so SpinOnce skips
// pending work without distinguishing nil from empty.
func (g *SerialGroup) snapshotTakes() []func() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.closed || !g.active {
		return []func() bool{}
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
		return false, fmt.Errorf("sdk: pooled group workers %d depth %d submit with nil task", g.workers, g.depth)
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.closed {
		return false, fmt.Errorf("sdk: pooled group workers %d depth %d submit on closed group", g.workers, g.depth)
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

// Close discards pending tasks and joins workers cleanly. Concurrent
// Close calls all join workers; a second Close waits instead of returning
// early. Close must not be called from a pool worker; self-close deadlocks
// by design because the calling worker waits for itself. The caller owns
// this contract.
func (g *PooledGroup) Close() error {
	if g == nil {
		return errors.New("sdk: pooled group close on nil group") //nolint:wrapcheck // new error, nothing to wrap
	}
	g.mu.Lock()
	if g.closed {
		g.mu.Unlock()
		g.wg.Wait()
		return nil
	}
	g.closed = true
	for i := range g.queue {
		g.queue[i] = nil
	}
	g.queue = g.queue[:0]
	g.cond.Broadcast()
	g.mu.Unlock()
	g.wg.Wait()
	return nil
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
