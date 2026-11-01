// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package sdk

import (
	"errors"
	"fmt"
	"maps"
	"math"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
)

// configWatchBuffer sizes each watcher channel. Broadcast never blocks: a
// full channel evicts its oldest event before the new one is queued.
// Evictions and drops count loudly in BroadcastDrops.
const configWatchBuffer = 16

// configHistoryLimit bounds retained history to the last entries.
// Overflow drops the oldest entries and counts each drop in HistoryDrops.
const configHistoryLimit = 10000

// ParamType names the kind of a configuration value.
type ParamType int

const (
	// ParamTypeNotSet means unconstrained in descriptors; values never carry it.
	ParamTypeNotSet ParamType = iota
	// ParamTypeBool holds a boolean.
	ParamTypeBool
	// ParamTypeInteger holds an int64.
	ParamTypeInteger
	// ParamTypeDouble holds a float64.
	ParamTypeDouble
	// ParamTypeString holds a string.
	ParamTypeString
)

// String renders the type in lowercase form.
func (t ParamType) String() string {
	switch t {
	case ParamTypeNotSet:
		return "not_set"
	case ParamTypeBool:
		return "bool"
	case ParamTypeInteger:
		return "integer"
	case ParamTypeDouble:
		return "double"
	case ParamTypeString:
		return "string"
	default:
		return "invalid"
	}
}

// Value is one typed configuration value.
//
// REVIEW: only bool, integer, double, and string ship in v1; byte and
// array kinds are deferred to user review.
type Value struct {
	// Type names the active field.
	Type ParamType
	// Bool holds the value when Type is ParamTypeBool.
	Bool bool
	// Integer holds the value when Type is ParamTypeInteger.
	Integer int64
	// Double holds the value when Type is ParamTypeDouble.
	Double float64
	// String holds the value when Type is ParamTypeString.
	String string
}

// NewBoolValue returns a boolean value.
func NewBoolValue(v bool) Value {
	return Value{Type: ParamTypeBool, Bool: v}
}

// NewIntegerValue returns an integer value.
func NewIntegerValue(v int64) Value {
	return Value{Type: ParamTypeInteger, Integer: v}
}

// NewDoubleValue returns a double value.
func NewDoubleValue(v float64) Value {
	return Value{Type: ParamTypeDouble, Double: v}
}

// NewStringValue returns a string value.
func NewStringValue(v string) Value {
	return Value{Type: ParamTypeString, String: v}
}

// Equal reports whether two values carry the same type and content.
func (v Value) Equal(o Value) bool {
	if v.Type != o.Type {
		return false
	}
	switch v.Type {
	case ParamTypeNotSet:
		return true
	case ParamTypeBool:
		return v.Bool == o.Bool
	case ParamTypeInteger:
		return v.Integer == o.Integer
	case ParamTypeDouble:
		return v.Double == o.Double
	case ParamTypeString:
		return v.String == o.String
	default:
		return false
	}
}

// Descriptor constrains one parameter: its type, documentation, and
// optional numeric range.
type Descriptor struct {
	// Type is the required value type, or NotSet for unconstrained.
	Type ParamType
	// Description documents the parameter for describe output.
	Description string
	// ReadOnly rejects Set after Declare while still allowing Declare.
	ReadOnly bool
	// HasIntegerRange enables IntegerMin and IntegerMax for integers.
	HasIntegerRange bool
	// IntegerMin is the inclusive lower bound for integers.
	IntegerMin int64
	// IntegerMax is the inclusive upper bound for integers.
	IntegerMax int64
	// HasDoubleRange enables DoubleMin and DoubleMax for doubles.
	HasDoubleRange bool
	// DoubleMin is the inclusive lower bound for doubles.
	DoubleMin float64
	// DoubleMax is the inclusive upper bound for doubles.
	DoubleMax float64
}

// isValidParamType reports whether t is a concrete descriptor or value type.
func isValidParamType(t ParamType) bool {
	switch t {
	case ParamTypeNotSet,
		ParamTypeBool,
		ParamTypeInteger,
		ParamTypeDouble,
		ParamTypeString:
		return true
	default:
		return false
	}
}

// validateValue checks type and range without touching read-only state.
// A NotSet value validates only against a NotSet descriptor. Invalid enum
// members fail loudly. NaN fails an enabled double range explicitly.
//
//nolint:gocognit,cyclop // validation sequences enum, NotSet, type, and range checks in one pass, splitting would obscure intent
func (d Descriptor) validateValue(v Value) error {
	if !isValidParamType(v.Type) {
		return fmt.Errorf("sdk: config: value type %d is invalid", int(v.Type))
	}
	if !isValidParamType(d.Type) {
		return fmt.Errorf("sdk: config: descriptor type %d is invalid", int(d.Type))
	}
	if v.Type == ParamTypeNotSet {
		if d.Type == ParamTypeNotSet {
			return nil
		}
		return errors.New("sdk: config: value type not_set is not concrete") //nolint:wrapcheck // new error, nothing to wrap
	}
	if d.Type != ParamTypeNotSet && v.Type != d.Type {
		return fmt.Errorf("sdk: config: type mismatch: have %s, want %s", v.Type, d.Type)
	}
	if v.Type == ParamTypeInteger && d.HasIntegerRange {
		if v.Integer < d.IntegerMin || v.Integer > d.IntegerMax {
			return fmt.Errorf("sdk: config: integer %d out of range [%d, %d]", v.Integer, d.IntegerMin, d.IntegerMax)
		}
	}
	if v.Type == ParamTypeDouble && d.HasDoubleRange {
		if math.IsNaN(v.Double) {
			return fmt.Errorf("sdk: config: double NaN is not in range [%v, %v]", d.DoubleMin, d.DoubleMax)
		}
		if v.Double < d.DoubleMin || v.Double > d.DoubleMax {
			return fmt.Errorf("sdk: config: double %v out of range [%v, %v]", v.Double, d.DoubleMin, d.DoubleMax)
		}
	}
	return nil
}

// Schema maps parameter names to descriptors for pre-launch validation.
type Schema map[string]Descriptor

// Validate checks raw values against the schema without touching a store.
func (s Schema) Validate(values map[string]Value) error {
	var problems []string
	names := make([]string, 0, len(values))
	for name := range values {
		names = append(names, name)
	}
	sort.Strings(names)
	schemaNames := make([]string, 0, len(s))
	for name := range s {
		schemaNames = append(schemaNames, name)
	}
	sort.Strings(schemaNames)
	for _, name := range schemaNames {
		v, ok := values[name]
		if !ok {
			problems = append(problems, "missing parameter "+name)
			continue
		}
		err := s[name].validateValue(v)
		if err != nil {
			problems = append(problems, name+": "+err.Error())
		}
	}
	for _, name := range names {
		if _, ok := s[name]; !ok {
			problems = append(problems, "unknown parameter "+name)
		}
	}
	if len(problems) == 0 {
		return nil
	}
	return errors.New("sdk: config: schema: " + strings.Join(problems, "; ")) //nolint:wrapcheck // joined new error, nothing to wrap
}

// ValidateName checks dotted parameter name shape: non-empty, at most 255
// bytes, dot-separated segments of [A-Za-z_][A-Za-z0-9_]*.
func ValidateName(name string) error {
	if name == "" {
		return errors.New("sdk: config: parameter name is empty") //nolint:wrapcheck // new error, nothing to wrap
	}
	if len(name) > 255 {
		return fmt.Errorf("sdk: config: parameter name %q exceeds 255 bytes", name)
	}
	if strings.HasPrefix(name, ".") || strings.HasSuffix(name, ".") || strings.Contains(name, "..") {
		return fmt.Errorf("sdk: config: parameter name %q has empty segment", name)
	}
	for segment := range strings.SplitSeq(name, ".") {
		err := validateNameSegment(segment)
		if err != nil {
			return fmt.Errorf("sdk: config: parameter name %q: %w", name, err)
		}
	}
	return nil
}

// validateNameSegment checks one dot-separated segment.
func validateNameSegment(segment string) error {
	if segment == "" {
		return errors.New("sdk: config: empty name segment") //nolint:wrapcheck // new error, nothing to wrap
	}
	for i := range len(segment) {
		c := segment[i]
		ok := c == '_' || (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (i > 0 && c >= '0' && c <= '9')
		if !ok {
			return fmt.Errorf("sdk: config: bad character %q in segment %q", c, segment)
		}
	}
	return nil
}

// Change is one parameter delta inside an event.
type Change struct {
	// Name is the parameter name.
	Name string
	// OldValue is the previous value. Valid only when HadOld is true.
	OldValue Value
	// NewValue is the applied value.
	NewValue Value
	// HadOld is false for newly declared entries.
	HadOld bool
}

// Event is one versioned batch: exactly one per successful mutation batch.
type Event struct {
	// Version is the store version after the batch.
	Version uint64
	// Source names the origin: declare, set, startup, or override.
	Source string
	// Changes lists applied deltas sorted by name for determinism.
	Changes []Change
}

// HistoryEntry records one parameter delta at one version.
type HistoryEntry struct {
	// Version is the store version after the batch.
	Version uint64
	// Name is the parameter name.
	Name string
	// OldValue is the previous value. Valid only when HadOld is true.
	OldValue Value
	// NewValue is the applied value.
	NewValue Value
	// HadOld is false for newly declared entries.
	HadOld bool
	// Source names the origin batch.
	Source string
}

// ValidatorFunc validates a pending set and may reject it. Rejection
// aborts the whole batch before the version moves.
type ValidatorFunc func(oldValue, newValue Value) error

// ApplyHookFunc runs after a validated batch commits. Hooks never reject.
type ApplyHookFunc func(oldValue, newValue Value)

// hookCall pairs one hook with its old and new values.
type hookCall struct {
	// fn runs after commit.
	fn ApplyHookFunc
	// oldValue is the previous value.
	oldValue Value
	// newValue is the applied value.
	newValue Value
}

// storeEntry is one stored parameter.
type storeEntry struct {
	// value is the current value.
	value Value
	// desc constrains future writes.
	desc Descriptor
}

// Store is a versioned parameter store with schema, validation, atomic
// batches, watches, and history. Build it with NewStore and share by
// pointer; do not copy after first use.
// History is bounded to the last configHistoryLimit entries; overflow drops
// the oldest and counts loudly in HistoryDrops. Broadcast never holds the
// write lock while sending; each watcher is checked plus sent under a read
// lock with eviction counted in BroadcastDrops.
//
// REVIEW: remap tables and undeclared-set flags are deferred; Declare
// pins names, Set rejects unknown names, and ApplyStartup auto-declares
// from files, CLI, and code in fixed precedence.
type Store struct {
	// mu guards all fields below.
	mu sync.RWMutex
	// params holds stored entries by name.
	params map[string]*storeEntry
	// version counts committed batches.
	version uint64
	// history records every delta oldest first.
	history []HistoryEntry
	// historyDrop counts history bound drops.
	historyDrop uint64
	// watchers holds active subscriptions.
	watchers map[uint64]chan Event
	// nextWatch numbers watcher ids.
	nextWatch uint64
	// validators runs in order outside the write lock with optimistic generation.
	validators map[string][]ValidatorFunc
	// hooks runs after commit in order.
	hooks map[string][]ApplyHookFunc
	// drops counts broadcast evictions plus drops loudly.
	drops atomic.Uint64
}

// BroadcastDrops returns the monotonic count of watcher broadcast evictions
// plus drops.
func (s *Store) BroadcastDrops() uint64 {
	if s == nil {
		return 0
	}
	return s.drops.Load()
}

// HistoryDrops returns the monotonic count of history entries dropped by the
// configHistoryLimit bound.
func (s *Store) HistoryDrops() uint64 {
	if s == nil {
		return 0
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.historyDrop
}

// pendingValidator pairs one validator with its snapshot inputs.
type pendingValidator struct {
	// name is the parameter under validation.
	name string
	// oldValue is the snapshot before the batch.
	oldValue Value
	// newValue is the snapshot after the batch.
	newValue Value
	// fn validates the transition.
	fn ValidatorFunc
}

// runPendingValidators runs collected validators outside the write lock.
func runPendingValidators(pending []pendingValidator) error {
	for _, p := range pending {
		err := p.fn(p.oldValue, p.newValue)
		if err != nil {
			return fmt.Errorf("sdk: set %q rejected by validator: %w", p.name, err)
		}
	}
	return nil
}

// Dump returns a copy of every stored value for canonical dump output.
// Values format stays compatible (v1 values only).
func (s *Store) Dump() map[string]Value {
	if s == nil {
		return map[string]Value{}
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make(map[string]Value, len(s.params))
	for k, e := range s.params {
		out[k] = e.value
	}
	return out
}

// DumpEntry pairs one stored value with its descriptor for round trips.
type DumpEntry struct {
	// Value is the stored parameter value.
	Value Value
	// Descriptor is the stored parameter descriptor.
	Descriptor Descriptor
}

// DumpDescriptors returns a copy of every stored descriptor for dump round
// trips.
func (s *Store) DumpDescriptors() map[string]Descriptor {
	if s == nil {
		return map[string]Descriptor{}
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make(map[string]Descriptor, len(s.params))
	for k, e := range s.params {
		out[k] = e.desc
	}
	return out
}

// DumpAll returns values plus descriptors so a dump round trip preserves
// read-only flags, ranges, and descriptions.
func (s *Store) DumpAll() map[string]DumpEntry {
	if s == nil {
		return map[string]DumpEntry{}
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make(map[string]DumpEntry, len(s.params))
	for k, e := range s.params {
		out[k] = DumpEntry{Value: e.value, Descriptor: e.desc}
	}
	return out
}

// NewStore returns an empty store.
func NewStore() *Store {
	return &Store{
		params:     make(map[string]*storeEntry),
		watchers:   make(map[uint64]chan Event),
		validators: make(map[string][]ValidatorFunc),
		hooks:      make(map[string][]ApplyHookFunc),
	}
}

// Declare adds a parameter with its descriptor. Re-declaring fails.
// A NotSet value validates only against a NotSet descriptor.
func (s *Store) Declare(name string, value Value, desc Descriptor) error {
	if s == nil {
		return errors.New("sdk: declare on nil store") //nolint:wrapcheck // new error, nothing to wrap
	}
	err := ValidateName(name)
	if err != nil {
		return err
	}
	s.mu.Lock()
	if _, ok := s.params[name]; ok {
		s.mu.Unlock()
		return fmt.Errorf("sdk: declare %q: already declared", name)
	}
	if desc.Type == ParamTypeNotSet && value.Type != ParamTypeNotSet {
		desc.Type = value.Type
	}
	err = desc.validateValue(value)
	if err != nil {
		s.mu.Unlock()
		return fmt.Errorf("sdk: declare %q: %w", name, err)
	}
	event := Event{Version: s.version + 1, Source: "declare"}
	event.Changes = []Change{{Name: name, NewValue: value, HadOld: false}}
	s.params[name] = &storeEntry{value: value, desc: desc}
	s.version = event.Version
	s.history = append(s.history, HistoryEntry{
		Version: event.Version, Name: name, NewValue: value, HadOld: false, Source: "declare",
	})
	s.trimHistoryLocked()
	s.mu.Unlock()
	s.broadcast(event)
	return nil
}

// Get returns a copy of one parameter.
func (s *Store) Get(name string) (Value, error) {
	if s == nil {
		return Value{}, errors.New("sdk: get on nil store") //nolint:wrapcheck // new error, nothing to wrap
	}
	err := ValidateName(name)
	if err != nil {
		return Value{}, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	entry, ok := s.params[name]
	if !ok {
		return Value{}, fmt.Errorf("sdk: get %q: not declared", name)
	}
	return entry.value, nil
}

// Describe returns a copy of one parameter descriptor.
func (s *Store) Describe(name string) (Descriptor, error) {
	if s == nil {
		return Descriptor{}, errors.New("sdk: describe on nil store") //nolint:wrapcheck // new error, nothing to wrap
	}
	err := ValidateName(name)
	if err != nil {
		return Descriptor{}, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	entry, ok := s.params[name]
	if !ok {
		return Descriptor{}, fmt.Errorf("sdk: describe %q: not declared", name)
	}
	return entry.desc, nil
}

// List returns stored names with the given dot prefix, sorted. An empty
// prefix lists everything.
func (s *Store) List(prefix string) []string {
	if s == nil {
		return []string{}
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []string
	for name := range s.params {
		if prefix == "" || name == prefix || strings.HasPrefix(name, prefix+".") {
			out = append(out, name)
		}
	}
	sort.Strings(out)
	if out == nil {
		return []string{}
	}
	return out
}

// Version returns the current store version.
func (s *Store) Version() uint64 {
	if s == nil {
		return 0
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.version
}

// History returns a copy of every entry oldest first.
func (s *Store) History() []HistoryEntry {
	if s == nil {
		return []HistoryEntry{}
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]HistoryEntry, len(s.history))
	copy(out, s.history)
	return out
}

// HistorySince returns entries with version above since, oldest first.
func (s *Store) HistorySince(since uint64) []HistoryEntry {
	if s == nil {
		return []HistoryEntry{}
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []HistoryEntry
	for _, h := range s.history {
		if h.Version > since {
			out = append(out, h)
		}
	}
	if out == nil {
		return []HistoryEntry{}
	}
	return out
}

// Watch subscribes to future events. The returned version is the current
// store version; HistorySince replays anything missed. The channel is
// buffered and never blocks the store: a full channel evicts its oldest
// event and counts loudly in BroadcastDrops. Consumers must tolerate
// non-monotonic event order across concurrent batches.
func (s *Store) Watch() (uint64, <-chan Event, func()) {
	if s == nil {
		ch := make(chan Event)
		close(ch)
		return 0, ch, func() {}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	id := s.nextWatch
	s.nextWatch++
	ch := make(chan Event, configWatchBuffer)
	s.watchers[id] = ch
	cancel := func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		if w, ok := s.watchers[id]; ok {
			delete(s.watchers, id)
			close(w)
		}
	}
	return s.version, ch, cancel
}

// AddValidator registers an on-set validator for one parameter. Validators
// run outside the write lock against a read snapshot with optimistic
// generation; the first error aborts before the version moves. A concurrent
// mutation aborts loudly. Validators must still avoid deep reentrancy: they
// must not call back into the store.
func (s *Store) AddValidator(name string, fn ValidatorFunc) error {
	if s == nil {
		return errors.New("sdk: add validator on nil store") //nolint:wrapcheck // new error, nothing to wrap
	}
	err := ValidateName(name)
	if err != nil {
		return err
	}
	if fn == nil {
		return fmt.Errorf("sdk: validator for %q is nil", name)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.validators[name] = append(s.validators[name], fn)
	return nil
}

// AddApplyHook registers a post-set hook for one parameter.
func (s *Store) AddApplyHook(name string, fn ApplyHookFunc) error {
	if s == nil {
		return errors.New("sdk: add apply hook on nil store") //nolint:wrapcheck // new error, nothing to wrap
	}
	err := ValidateName(name)
	if err != nil {
		return err
	}
	if fn == nil {
		return fmt.Errorf("sdk: apply hook for %q is nil", name)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.hooks[name] = append(s.hooks[name], fn)
	return nil
}

// Set writes one parameter with source "set".
func (s *Store) Set(name string, value Value) error {
	if s == nil {
		return errors.New("sdk: set on nil store") //nolint:wrapcheck // new error, nothing to wrap
	}
	_, err := s.SetWithSource(map[string]Value{name: value}, "set", false)
	if err != nil {
		return err
	}
	return nil
}

// SetAll writes every entry or none with source "set" in one event.
func (s *Store) SetAll(updates map[string]Value) (Event, error) {
	if s == nil {
		return Event{}, errors.New("sdk: set all on nil store") //nolint:wrapcheck // new error, nothing to wrap
	}
	event, err := s.SetWithSource(updates, "set", false)
	if err != nil {
		return Event{}, err
	}
	return event, nil
}

// ApplyStartup merges files, CLI, and code with fixed precedence
// files < CLI < code in one event with source "startup". New names
// auto-declare here. An empty merge emits no version bump.
func (s *Store) ApplyStartup(files, cli, code map[string]Value) (Event, error) {
	if s == nil {
		return Event{}, errors.New("sdk: apply startup on nil store") //nolint:wrapcheck // new error, nothing to wrap
	}
	merged := make(map[string]Value)
	maps.Copy(merged, files)
	maps.Copy(merged, cli)
	maps.Copy(merged, code)
	if len(merged) == 0 {
		s.mu.RLock()
		version := s.version
		s.mu.RUnlock()
		return Event{Version: version, Source: "startup", Changes: []Change{}}, nil
	}
	event, err := s.SetWithSource(merged, "startup", true)
	if err != nil {
		return Event{}, err
	}
	return event, nil
}

// SetWithSource writes every entry or none with the given source label in
// one versioned event. Unknown names auto-declare only when allowed.
// Validators run outside the write lock with optimistic generation.
func (s *Store) SetWithSource(updates map[string]Value, source string, autoDeclare bool) (Event, error) {
	if s == nil {
		return Event{}, errors.New("sdk: set with source on nil store") //nolint:wrapcheck // new error, nothing to wrap
	}
	if source == "" {
		return Event{}, errors.New("sdk: config: source is empty") //nolint:wrapcheck // new error, nothing to wrap
	}
	s.mu.Lock()
	event, hooks, pending, err := s.prepareBatchLocked(updates, source, autoDeclare)
	if err != nil {
		s.mu.Unlock()
		return Event{}, err
	}
	gen := s.version
	s.mu.Unlock()
	err = runPendingValidators(pending)
	if err != nil {
		return Event{}, err
	}
	s.mu.Lock()
	if s.version != gen {
		s.mu.Unlock()
		return Event{}, fmt.Errorf("sdk: config: %s: concurrent mutation aborted batch", source)
	}
	if len(event.Changes) == 0 {
		version := s.version
		s.mu.Unlock()
		return Event{Version: version, Source: source, Changes: []Change{}}, nil
	}
	s.commitPreparedLocked(event)
	s.mu.Unlock()
	s.broadcast(event)
	s.runHooks(hooks)
	return event, nil
}

// prepareBatchLocked validates every update without mutating state and
// collects validators to run outside the write lock with optimistic
// generation.
//
//nolint:gocognit // batch validation sequences sort, type, read-only, and validator collection in one pass, splitting would obscure intent
func (s *Store) prepareBatchLocked(updates map[string]Value, source string, autoDeclare bool) (Event, []hookCall, []pendingValidator, error) {
	ordered := make([]string, 0, len(updates))
	for name := range updates {
		ordered = append(ordered, name)
	}
	sort.Strings(ordered)
	next := s.version + 1
	var changes []Change
	var hooks []hookCall
	var pending []pendingValidator
	for _, name := range ordered {
		newValue := updates[name]
		err := ValidateName(name)
		if err != nil {
			return Event{}, nil, nil, err
		}
		entry, ok := s.params[name]
		if !ok {
			if newValue.Type == ParamTypeNotSet {
				return Event{}, nil, nil, fmt.Errorf("sdk: set %q: value type not_set is not concrete", name)
			}
			if !autoDeclare {
				return Event{}, nil, nil, fmt.Errorf("sdk: set %q: not declared", name)
			}
			changes = append(changes, Change{Name: name, NewValue: newValue, HadOld: false})
			for _, fn := range s.hooks[name] {
				hooks = append(hooks, hookCall{fn: fn, oldValue: Value{}, newValue: newValue})
			}
			continue
		}
		if entry.desc.ReadOnly {
			return Event{}, nil, nil, fmt.Errorf("sdk: set %q: parameter is read only", name)
		}
		err = entry.desc.validateValue(newValue)
		if err != nil {
			return Event{}, nil, nil, fmt.Errorf("sdk: set %q: %w", name, err)
		}
		old := entry.value
		for _, fn := range s.validators[name] {
			pending = append(pending, pendingValidator{name: name, oldValue: old, newValue: newValue, fn: fn})
		}
		changes = append(changes, Change{Name: name, OldValue: old, NewValue: newValue, HadOld: true})
		for _, fn := range s.hooks[name] {
			hooks = append(hooks, hookCall{fn: fn, oldValue: old, newValue: newValue})
		}
	}
	event := Event{Version: next, Source: source, Changes: changes}
	return event, hooks, pending, nil
}

// commitPreparedLocked applies a validated event: one version bump, one
// history entry per change, and value swaps. History is trimmed to the last
// configHistoryLimit entries with oldest drops counted loudly.
func (s *Store) commitPreparedLocked(event Event) {
	s.version = event.Version
	for _, c := range event.Changes {
		entry, ok := s.params[c.Name]
		if !ok {
			s.params[c.Name] = &storeEntry{value: c.NewValue, desc: Descriptor{Type: c.NewValue.Type}}
		} else {
			entry.value = c.NewValue
		}
		s.history = append(s.history, HistoryEntry{
			Version:  event.Version,
			Name:     c.Name,
			OldValue: c.OldValue,
			NewValue: c.NewValue,
			HadOld:   c.HadOld,
			Source:   event.Source,
		})
	}
	s.trimHistoryLocked()
}

// trimHistoryLocked drops oldest history beyond configHistoryLimit and counts
// loudly. The caller holds the write lock.
func (s *Store) trimHistoryLocked() {
	if len(s.history) <= configHistoryLimit {
		return
	}
	overflow := len(s.history) - configHistoryLimit
	for i := range overflow {
		s.history[i] = HistoryEntry{}
	}
	s.history = append([]HistoryEntry(nil), s.history[overflow:]...)
	s.historyDrop += uint64(overflow) //nolint:gosec // overflow is a non-negative length difference fitting uint64
}

// broadcast delivers the event to every watcher without holding the write
// lock. Each watcher is checked plus sent under a read lock with a
// per-watcher nonblocking send; a full channel evicts its oldest event and
// counts loudly in BroadcastDrops. Never sends on a closed channel because
// cancel deletes plus closes under the write lock, which waits for the read
// lock.
func (s *Store) broadcast(event Event) {
	s.mu.RLock()
	ids := make([]uint64, 0, len(s.watchers))
	for id := range s.watchers {
		ids = append(ids, id)
	}
	s.mu.RUnlock()
	for _, id := range ids {
		s.mu.RLock()
		ch, ok := s.watchers[id]
		if !ok {
			s.mu.RUnlock()
			continue
		}
		select {
		case ch <- event:
			s.mu.RUnlock()
		default:
			select {
			case <-ch:
				s.drops.Add(1)
			default:
			}
			select {
			case ch <- event:
				s.mu.RUnlock()
			default:
				s.drops.Add(1)
				s.mu.RUnlock()
			}
		}
	}
}

// LoadDump restores values plus descriptors atomically with the given source.
// New names declare with the supplied descriptors after validation; existing
// names keep their descriptors and validate against them. Validators run
// outside the write lock with optimistic generation.
//
//nolint:gocognit,cyclop // load sequences validation, optimistic prepare, commit, and descriptor pinning in one flow, splitting would obscure intent
func (s *Store) LoadDump(dump map[string]DumpEntry, source string) (Event, error) {
	if s == nil {
		return Event{}, errors.New("sdk: load dump on nil store") //nolint:wrapcheck // new error, nothing to wrap
	}
	if source == "" {
		return Event{}, errors.New("sdk: config: source is empty") //nolint:wrapcheck // new error, nothing to wrap
	}
	values := make(map[string]Value, len(dump))
	for name, entry := range dump {
		err := ValidateName(name)
		if err != nil {
			return Event{}, err
		}
		if _, ok := s.params[name]; !ok {
			err = entry.Descriptor.validateValue(entry.Value)
			if err != nil {
				return Event{}, fmt.Errorf("sdk: load dump %q: %w", name, err)
			}
		}
		values[name] = entry.Value
	}
	s.mu.Lock()
	event, hooks, pending, err := s.prepareBatchLocked(values, source, true)
	if err != nil {
		s.mu.Unlock()
		return Event{}, err
	}
	gen := s.version
	s.mu.Unlock()
	err = runPendingValidators(pending)
	if err != nil {
		return Event{}, err
	}
	s.mu.Lock()
	if s.version != gen {
		s.mu.Unlock()
		return Event{}, fmt.Errorf("sdk: config: %s: concurrent mutation aborted batch", source)
	}
	if len(event.Changes) == 0 {
		version := s.version
		s.mu.Unlock()
		return Event{Version: version, Source: source, Changes: []Change{}}, nil
	}
	s.commitPreparedLocked(event)
	for _, c := range event.Changes {
		if c.HadOld {
			continue
		}
		if entry, ok := dump[c.Name]; ok {
			if stored, exists := s.params[c.Name]; exists {
				stored.desc = entry.Descriptor
			}
		}
	}
	s.mu.Unlock()
	s.broadcast(event)
	s.runHooks(hooks)
	return event, nil
}

// runHooks executes apply hooks after commit in batch order.
func (s *Store) runHooks(hooks []hookCall) {
	for _, h := range hooks {
		h.fn(h.oldValue, h.newValue)
	}
}
