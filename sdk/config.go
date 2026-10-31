// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package sdk

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
)

// configWatchBuffer sizes each watcher channel. Broadcast never blocks: a
// full channel evicts its oldest event before the new one is queued.
const configWatchBuffer = 16

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

// validateValue checks type and range without touching read-only state.
func (d Descriptor) validateValue(v Value) error {
	if v.Type == ParamTypeNotSet {
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
	for _, segment := range strings.Split(name, ".") {
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
	for i := 0; i < len(segment); i++ {
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
	// watchers holds active subscriptions.
	watchers map[uint64]chan Event
	// nextWatch numbers watcher ids.
	nextWatch uint64
	// validators runs in order during validation.
	validators map[string][]ValidatorFunc
	// hooks runs after commit in order.
	hooks map[string][]ApplyHookFunc
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
	if desc.Type == ParamTypeNotSet {
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
	channels := s.watcherChannelsLocked()
	s.mu.Unlock()
	s.broadcast(channels, event)
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
// buffered and never blocks the store.
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

// AddValidator registers an on-set validator for one parameter.
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
	for k, v := range files {
		merged[k] = v
	}
	for k, v := range cli {
		merged[k] = v
	}
	for k, v := range code {
		merged[k] = v
	}
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
func (s *Store) SetWithSource(updates map[string]Value, source string, autoDeclare bool) (Event, error) {
	if s == nil {
		return Event{}, errors.New("sdk: set with source on nil store") //nolint:wrapcheck // new error, nothing to wrap
	}
	if source == "" {
		return Event{}, errors.New("sdk: config: source is empty") //nolint:wrapcheck // new error, nothing to wrap
	}
	s.mu.Lock()
	event, hooks, err := s.prepareBatchLocked(updates, source, autoDeclare)
	if err != nil {
		s.mu.Unlock()
		return Event{}, err
	}
	if len(event.Changes) == 0 {
		version := s.version
		s.mu.Unlock()
		return Event{Version: version, Source: source, Changes: []Change{}}, nil
	}
	s.commitPreparedLocked(event)
	channels := s.watcherChannelsLocked()
	s.mu.Unlock()
	s.broadcast(channels, event)
	s.runHooks(hooks)
	return event, nil
}

// prepareBatchLocked validates every update without mutating state.
func (s *Store) prepareBatchLocked(updates map[string]Value, source string, autoDeclare bool) (Event, []hookCall, error) {
	ordered := make([]string, 0, len(updates))
	for name := range updates {
		ordered = append(ordered, name)
	}
	sort.Strings(ordered)
	next := s.version + 1
	var changes []Change
	var hooks []hookCall
	for _, name := range ordered {
		newValue := updates[name]
		err := ValidateName(name)
		if err != nil {
			return Event{}, nil, err
		}
		if newValue.Type == ParamTypeNotSet {
			return Event{}, nil, fmt.Errorf("sdk: set %q: value type not_set is not concrete", name)
		}
		entry, ok := s.params[name]
		if !ok {
			if !autoDeclare {
				return Event{}, nil, fmt.Errorf("sdk: set %q: not declared", name)
			}
			changes = append(changes, Change{Name: name, NewValue: newValue, HadOld: false})
			for _, fn := range s.hooks[name] {
				hooks = append(hooks, hookCall{fn: fn, oldValue: Value{}, newValue: newValue})
			}
			continue
		}
		if entry.desc.ReadOnly {
			return Event{}, nil, fmt.Errorf("sdk: set %q: parameter is read only", name)
		}
		err = entry.desc.validateValue(newValue)
		if err != nil {
			return Event{}, nil, fmt.Errorf("sdk: set %q: %w", name, err)
		}
		old := entry.value
		for _, fn := range s.validators[name] {
			err = fn(old, newValue)
			if err != nil {
				return Event{}, nil, fmt.Errorf("sdk: set %q rejected by validator: %w", name, err)
			}
		}
		changes = append(changes, Change{Name: name, OldValue: old, NewValue: newValue, HadOld: true})
		for _, fn := range s.hooks[name] {
			hooks = append(hooks, hookCall{fn: fn, oldValue: old, newValue: newValue})
		}
	}
	event := Event{Version: next, Source: source, Changes: changes}
	return event, hooks, nil
}

// commitPreparedLocked applies a validated event: one version bump, one
// history entry per change, and value swaps.
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
}

// watcherChannelsLocked snapshots watcher channels. The caller holds the lock.
func (s *Store) watcherChannelsLocked() []chan Event {
	out := make([]chan Event, 0, len(s.watchers))
	for _, w := range s.watchers {
		out = append(out, w)
	}
	return out
}

// broadcast delivers the event to every channel without blocking.
func (s *Store) broadcast(channels []chan Event, event Event) {
	for _, ch := range channels {
		select {
		case ch <- event:
		default:
			select {
			case <-ch:
			default:
			}
			select {
			case ch <- event:
			default:
			}
		}
	}
}

// runHooks executes apply hooks after commit in batch order.
func (s *Store) runHooks(hooks []hookCall) {
	for _, h := range hooks {
		h.fn(h.oldValue, h.newValue)
	}
}
