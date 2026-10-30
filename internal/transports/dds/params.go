// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

// Package dds holds the CycloneDDS sidecar contract.
//
// This file maps the standard ROS parameter services to a config store.
// The standard services stay on the wire for ros2cli and are served from
// the store. Transports translate only: this file turns CDR bytes into
// store calls and store results into CDR bytes, with no QoS or matching
// decisions. Matching and QoS stay in core.
//
// The MemoryStore here is the transport side view of the core config
// store. Core owns the canonical store; this view exists so the wire
// mapping and the CLI have a faithful stand in until the core import
// lands. Behavior matches the core contract: one owner per parameter,
// atomic updates, versioned events, and validation before apply.
package dds

import (
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"

	"github.com/podomy/weld/internal/types"
)

// Parameter type codes from rcl_interfaces/msg/ParameterType.
const (
	// ParamNotSet marks an unset parameter.
	ParamNotSet uint8 = 0
	// ParamBool marks a bool parameter.
	ParamBool uint8 = 1
	// ParamInteger marks an int64 parameter.
	ParamInteger uint8 = 2
	// ParamDouble marks a float64 parameter.
	ParamDouble uint8 = 3
	// ParamString marks a string parameter.
	ParamString uint8 = 4
	// ParamByteArray marks a byte array parameter.
	ParamByteArray uint8 = 5
	// ParamBoolArray marks a bool array parameter.
	ParamBoolArray uint8 = 6
	// ParamIntegerArray marks an int64 array parameter.
	ParamIntegerArray uint8 = 7
	// ParamDoubleArray marks a float64 array parameter.
	ParamDoubleArray uint8 = 8
	// ParamStringArray marks a string array parameter.
	ParamStringArray uint8 = 9
)

// Parameter service suffixes for one node.
const (
	// ParamDescribeSuffix is the describe service suffix.
	ParamDescribeSuffix = "describe_parameters"
	// ParamGetSuffix is the get service suffix.
	ParamGetSuffix = "get_parameters"
	// ParamGetTypesSuffix is the get types service suffix.
	ParamGetTypesSuffix = "get_parameter_types"
	// ParamListSuffix is the list service suffix.
	ParamListSuffix = "list_parameters"
	// ParamSetSuffix is the set service suffix.
	ParamSetSuffix = "set_parameters"
	// ParamSetAtomicSuffix is the atomic set service suffix.
	ParamSetAtomicSuffix = "set_parameters_atomically"
)

// ParamEventsTopic is the global parameter event topic.
const ParamEventsTopic = "/parameter_events"

// ParamDepthRecursive selects recursive listing in ListParameters.
const ParamDepthRecursive uint64 = 0

// ParamValue is the variant value of one parameter.
type ParamValue struct {
	// Type selects which field below carries valid data.
	Type uint8
	// Bool holds the bool value when Type is ParamBool.
	Bool bool
	// Integer holds the int64 value when Type is ParamInteger.
	Integer int64
	// Double holds the float64 value when Type is ParamDouble.
	Double float64
	// String holds the string value when Type is ParamString.
	String string
	// ByteArray holds bytes when Type is ParamByteArray.
	ByteArray []byte
	// BoolArray holds bools when Type is ParamBoolArray.
	BoolArray []bool
	// IntegerArray holds int64 values when Type is ParamIntegerArray.
	IntegerArray []int64
	// DoubleArray holds float64 values when Type is ParamDoubleArray.
	DoubleArray []float64
	// StringArray holds strings when Type is ParamStringArray.
	StringArray []string
}

// Parameter pairs a name with its value.
type Parameter struct {
	// Name is the parameter name such as use_sim_time or ctrl.max_vel.
	Name string
	// Value is the typed value.
	Value ParamValue
}

// FloatingPointRange bounds one double parameter.
type FloatingPointRange struct {
	// From is the inclusive lower bound.
	From float64
	// To is the inclusive upper bound.
	To float64
	// Step is the valid step, zero means continuous.
	Step float64
}

// IntegerRange bounds one integer parameter.
type IntegerRange struct {
	// From is the inclusive lower bound.
	From int64
	// To is the inclusive upper bound.
	To int64
	// Step is the valid step, zero means continuous.
	Step uint64
}

// ParamDescriptor describes one parameter with constraints.
type ParamDescriptor struct {
	// Name is the parameter name.
	Name string
	// Type is the expected type code.
	Type uint8
	// Description is human readable help.
	Description string
	// AdditionalConstraints holds plain text constraints.
	AdditionalConstraints string
	// ReadOnly blocks changes after initialization when true.
	ReadOnly bool
	// DynamicTyping allows type changes when true.
	DynamicTyping bool
	// FloatingPointRange holds at most one double range.
	FloatingPointRange []FloatingPointRange
	// IntegerRange holds at most one integer range.
	IntegerRange []IntegerRange
}

// SetParametersResult reports one set outcome.
type SetParametersResult struct {
	// Successful is true when the set applied.
	Successful bool
	// Reason explains failure, undefined on success.
	Reason string
}

// ListParametersResult holds one list outcome.
type ListParametersResult struct {
	// Names holds matching parameter names in sorted order.
	Names []string
	// Prefixes holds matching child prefixes in sorted order.
	Prefixes []string
}

// ParameterEvent reports one atomic parameter change.
type ParameterEvent struct {
	// StampSec is event time seconds.
	StampSec int32
	// StampNanosec is event time nanoseconds.
	StampNanosec uint32
	// Node is the fully qualified node name.
	Node string
	// NewParams holds newly declared parameters.
	NewParams []Parameter
	// ChangedParams holds changed parameters.
	ChangedParams []Parameter
	// DeletedParams holds deleted parameters as NOT_SET values.
	DeletedParams []Parameter
}

// ParamEntry holds one stored value plus its descriptor.
type ParamEntry struct {
	// Value is the current value.
	Value ParamValue
	// Descriptor is the stored descriptor.
	Descriptor ParamDescriptor
}

// ParamStore is the store contract used by wire translation.
//
// The transport calls these methods after decoding wire bytes and
// encodes the results back to wire bytes. Core provides the canonical
// implementation; MemoryStore is the transport side stand in.
type ParamStore interface {
	// NodeName returns the fully qualified node name owning the store.
	NodeName() string
	// Declare adds one parameter, failing when already present.
	Declare(param Parameter, desc ParamDescriptor) error
	// Get returns one value, reporting false when absent.
	Get(name string) (ParamValue, bool)
	// GetMany returns values in request order, NOT_SET for missing.
	GetMany(names []string) []ParamValue
	// GetTypes returns type codes in request order, NOT_SET for missing.
	GetTypes(names []string) []uint8
	// SetMany applies each parameter independently in order.
	SetMany(params []Parameter) []SetParametersResult
	// SetAtomically applies all parameters or none.
	SetAtomically(params []Parameter) SetParametersResult
	// Describe returns descriptors in request order.
	Describe(names []string) []ParamDescriptor
	// List returns names and prefixes matching the filter.
	List(prefixes []string, depth uint64) ListParametersResult
	// Events returns a copy of recorded events in order.
	Events() []ParameterEvent
}

// MemoryStore is an in memory ParamStore for wire mapping and CLI.
//
// It is safe for concurrent use. Events accumulate in order; callers
// own returned slices. Validation runs before apply so failed sets
// leave the store unchanged.
type MemoryStore struct {
	mu     sync.RWMutex
	node   string
	params map[string]ParamEntry
	events []ParameterEvent
	clock  func() (int32, uint32)
}

// NewMemoryStore returns an empty store for the given node name.
func NewMemoryStore(node string) *MemoryStore {
	trimmed := strings.TrimSpace(node)
	if trimmed == "" {
		trimmed = "/weld"
	}
	return &MemoryStore{
		node:   trimmed,
		params: make(map[string]ParamEntry),
	}
}

// NodeName returns the owning node name.
func (s *MemoryStore) NodeName() string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.node
}

// validateParamName rejects empty names and names with spaces or slashes.
func validateParamName(name string) error {
	if name == "" {
		return errors.New("dds: parameter name is empty") //nolint:wrapcheck // new error, nothing to wrap
	}
	if strings.ContainsAny(name, " \t\n\r/") {
		return fmt.Errorf("dds: parameter name %q has spaces or slashes", name)
	}
	if strings.HasPrefix(name, ".") || strings.HasSuffix(name, ".") {
		return fmt.Errorf("dds: parameter name %q has leading or trailing dot", name)
	}
	if strings.Contains(name, "..") {
		return fmt.Errorf("dds: parameter name %q has an empty segment", name)
	}
	return nil
}

// validateParamType reports whether the type code is known.
func validateParamType(t uint8) error {
	switch t {
	case ParamNotSet,
		ParamBool,
		ParamInteger,
		ParamDouble,
		ParamString,
		ParamByteArray,
		ParamBoolArray,
		ParamIntegerArray,
		ParamDoubleArray,
		ParamStringArray:
		return nil
	default:
		return fmt.Errorf("dds: parameter type %d is unknown", t)
	}
}

// validateRangeCounts rejects over bound or mixed range descriptors.
func validateRangeCounts(desc ParamDescriptor) error {
	if len(desc.FloatingPointRange) > 1 {
		return fmt.Errorf("dds: floating range length %d above 1", len(desc.FloatingPointRange))
	}
	if len(desc.IntegerRange) > 1 {
		return fmt.Errorf("dds: integer range length %d above 1", len(desc.IntegerRange))
	}
	if len(desc.FloatingPointRange) > 0 && len(desc.IntegerRange) > 0 {
		return errors.New("dds: floating and integer ranges are mutually exclusive") //nolint:wrapcheck // new error, nothing to wrap
	}
	return nil
}

// checkDoubleRange validates a double value against its floating range.
func checkDoubleRange(value ParamValue, desc ParamDescriptor) error {
	if value.Type != ParamDouble || len(desc.FloatingPointRange) != 1 {
		return nil
	}
	r := desc.FloatingPointRange[0]
	low := r.From
	high := r.To
	if low > high {
		low, high = high, low
	}
	if value.Double < low || value.Double > high {
		return fmt.Errorf("dds: double %v outside range [%v, %v]", value.Double, r.From, r.To)
	}
	return nil
}

// checkIntegerStep validates the step offset from the lower bound.
func checkIntegerStep(value int64, r IntegerRange, low int64) error {
	if r.Step == 0 || value == r.From || value == r.To {
		return nil
	}
	diff := value - low
	if diff < 0 {
		diff = -diff
	}
	if r.Step > math.MaxInt64 {
		return fmt.Errorf("dds: integer %d misses step %d from %d", value, r.Step, r.From)
	}
	step := int64(r.Step)
	if step < 0 {
		step = -step
	}
	if step != 0 && diff%step != 0 {
		return fmt.Errorf("dds: integer %d misses step %d from %d", value, r.Step, r.From)
	}
	return nil
}

// checkIntegerRange validates an integer value against its integer range.
func checkIntegerRange(value ParamValue, desc ParamDescriptor) error {
	if value.Type != ParamInteger || len(desc.IntegerRange) != 1 {
		return nil
	}
	r := desc.IntegerRange[0]
	low := r.From
	high := r.To
	if low > high {
		low, high = high, low
	}
	if value.Integer < low || value.Integer > high {
		return fmt.Errorf("dds: integer %d outside range [%d, %d]", value.Integer, r.From, r.To)
	}
	return checkIntegerStep(value.Integer, r, low)
}

// checkRanges validates value against descriptor ranges.
//
// It enforces from and to bounds. Step zero means continuous. A nonzero
// integer step requires the value to equal from, to, or a step multiple
// from the lower bound. Double steps use only bounds because floating
// point modulo is not stable on the wire.
func checkRanges(value ParamValue, desc ParamDescriptor) error {
	err := validateRangeCounts(desc)
	if err != nil {
		return err
	}
	err = checkDoubleRange(value, desc)
	if err != nil {
		return err
	}
	return checkIntegerRange(value, desc)
}

// normalizeDescriptor fills name and type from the value when unset.
func normalizeDescriptor(name string, value ParamValue, desc ParamDescriptor) ParamDescriptor {
	out := desc
	out.Name = name
	if out.Type == ParamNotSet {
		out.Type = value.Type
	}
	return out
}

// stampNow returns event time, defaulting to zero without a clock.
func (s *MemoryStore) stampNow() (int32, uint32) {
	if s.clock != nil {
		sec, nsec := s.clock()
		return sec, nsec
	}
	return 0, 0
}

// Declare adds one parameter, failing when already present.
func (s *MemoryStore) Declare(param Parameter, desc ParamDescriptor) error {
	err := validateParamName(param.Name)
	if err != nil {
		return err
	}
	err = validateParamType(param.Value.Type)
	if err != nil {
		return err
	}
	if desc.Type != ParamNotSet && desc.Type != param.Value.Type {
		return fmt.Errorf("dds: declare %q descriptor type %d mismatches value type %d", param.Name, desc.Type, param.Value.Type)
	}
	err = checkRanges(param.Value, desc)
	if err != nil {
		return fmt.Errorf("dds: declare %q: %w", param.Name, err)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.params[param.Name]; ok {
		return fmt.Errorf("dds: declare %q: already declared", param.Name)
	}
	entry := ParamEntry{
		Value:      param.Value,
		Descriptor: normalizeDescriptor(param.Name, param.Value, desc),
	}
	s.params[param.Name] = entry
	sec, nsec := s.stampNow()
	s.events = append(s.events, ParameterEvent{
		StampSec:     sec,
		StampNanosec: nsec,
		Node:         s.node,
		NewParams:    []Parameter{param},
	})
	return nil
}

// Get returns one value, reporting false when absent.
func (s *MemoryStore) Get(name string) (ParamValue, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	entry, ok := s.params[name]
	if !ok {
		return ParamValue{Type: ParamNotSet}, false
	}
	return entry.Value, true
}

// GetMany returns values in request order, NOT_SET for missing.
func (s *MemoryStore) GetMany(names []string) []ParamValue {
	s.mu.RLock()
	defer s.mu.RUnlock()

	out := make([]ParamValue, 0, len(names))
	for _, n := range names {
		entry, ok := s.params[n]
		if !ok {
			out = append(out, ParamValue{Type: ParamNotSet})
			continue
		}
		out = append(out, entry.Value)
	}
	return out
}

// GetTypes returns type codes in request order, NOT_SET for missing.
func (s *MemoryStore) GetTypes(names []string) []uint8 {
	values := s.GetMany(names)
	out := make([]uint8, 0, len(values))
	for _, v := range values {
		out = append(out, v.Type)
	}
	return out
}

// validateSet checks one set without applying it.
func validateSet(existing *ParamEntry, param Parameter) error {
	err := validateParamName(param.Name)
	if err != nil {
		return err
	}
	err = validateParamType(param.Value.Type)
	if err != nil {
		return err
	}
	if existing == nil {
		return nil
	}
	if existing.Descriptor.ReadOnly {
		return fmt.Errorf("dds: parameter %q is read only", param.Name)
	}
	if param.Value.Type == ParamNotSet {
		return nil
	}
	if existing.Value.Type != ParamNotSet && param.Value.Type != existing.Value.Type && !existing.Descriptor.DynamicTyping {
		return fmt.Errorf("dds: parameter %q type %d mismatches declared %d without dynamic typing", param.Name, param.Value.Type, existing.Value.Type)
	}
	err = checkRanges(param.Value, existing.Descriptor)
	if err != nil {
		return err
	}
	return nil
}

// applySetLocked applies one validated set, caller holds write lock.
func (s *MemoryStore) applySetLocked(param Parameter, newParams, changedParams, deletedParams *[]Parameter) {
	if param.Value.Type == ParamNotSet {
		if _, ok := s.params[param.Name]; ok {
			delete(s.params, param.Name)
			*deletedParams = append(*deletedParams, param)
		}
		return
	}
	entry, ok := s.params[param.Name]
	if !ok {
		desc := ParamDescriptor{Name: param.Name, Type: param.Value.Type, DynamicTyping: true}
		s.params[param.Name] = ParamEntry{Value: param.Value, Descriptor: desc}
		*newParams = append(*newParams, param)
		return
	}
	entry.Value = param.Value
	if entry.Descriptor.Type == ParamNotSet {
		entry.Descriptor.Type = param.Value.Type
	}
	s.params[param.Name] = entry
	*changedParams = append(*changedParams, param)
}

// SetMany applies each parameter independently in order.
func (s *MemoryStore) SetMany(params []Parameter) []SetParametersResult {
	s.mu.Lock()
	defer s.mu.Unlock()

	out := make([]SetParametersResult, 0, len(params))
	var newParams []Parameter
	var changedParams []Parameter
	var deletedParams []Parameter
	for _, p := range params {
		var existing *ParamEntry
		if e, ok := s.params[p.Name]; ok {
			cp := e
			existing = &cp
		}
		err := validateSet(existing, p)
		if err != nil {
			out = append(out, SetParametersResult{Successful: false, Reason: err.Error()})
			continue
		}
		s.applySetLocked(p, &newParams, &changedParams, &deletedParams)
		out = append(out, SetParametersResult{Successful: true})
	}
	if len(newParams)+len(changedParams)+len(deletedParams) > 0 {
		sec, nsec := s.stampNow()
		s.events = append(s.events, ParameterEvent{
			StampSec:      sec,
			StampNanosec:  nsec,
			Node:          s.node,
			NewParams:     newParams,
			ChangedParams: changedParams,
			DeletedParams: deletedParams,
		})
	}
	return out
}

// SetAtomically applies all parameters or none.
func (s *MemoryStore) SetAtomically(params []Parameter) SetParametersResult {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, p := range params {
		var existing *ParamEntry
		if e, ok := s.params[p.Name]; ok {
			cp := e
			existing = &cp
		}
		err := validateSet(existing, p)
		if err != nil {
			return SetParametersResult{Successful: false, Reason: err.Error()}
		}
	}
	var newParams []Parameter
	var changedParams []Parameter
	var deletedParams []Parameter
	for _, p := range params {
		s.applySetLocked(p, &newParams, &changedParams, &deletedParams)
	}
	if len(params) > 0 {
		sec, nsec := s.stampNow()
		s.events = append(s.events, ParameterEvent{
			StampSec:      sec,
			StampNanosec:  nsec,
			Node:          s.node,
			NewParams:     newParams,
			ChangedParams: changedParams,
			DeletedParams: deletedParams,
		})
	}
	return SetParametersResult{Successful: true}
}

// Describe returns descriptors in request order.
func (s *MemoryStore) Describe(names []string) []ParamDescriptor {
	s.mu.RLock()
	defer s.mu.RUnlock()

	out := make([]ParamDescriptor, 0, len(names))
	for _, n := range names {
		entry, ok := s.params[n]
		if !ok {
			out = append(out, ParamDescriptor{Name: n, Type: ParamNotSet})
			continue
		}
		out = append(out, entry.Descriptor)
	}
	return out
}

// matchPrefix reports whether name matches one list prefix.
func matchPrefix(name, prefix string) bool {
	if prefix == "" {
		return true
	}
	if name == prefix {
		return true
	}
	if strings.HasPrefix(name, prefix+".") {
		return true
	}
	if strings.HasPrefix(name, prefix+"/") {
		return true
	}
	return false
}

// depthOf counts separators after the prefix.
func depthOf(name, prefix string) uint64 {
	rest := name
	if prefix != "" {
		if name == prefix {
			return 0
		}
		trimmed := strings.TrimPrefix(name, prefix+".")
		if trimmed == name {
			trimmed = strings.TrimPrefix(name, prefix+"/")
		}
		rest = trimmed
	}
	if rest == "" {
		return 0
	}
	count := uint64(1)
	for _, r := range rest {
		if r == '.' || r == '/' {
			count++
		}
	}
	return count
}

// topLevelPrefix returns the segment before the first dot or slash.
func topLevelPrefix(name string) string {
	if idx := strings.IndexAny(name, "./"); idx >= 0 {
		return name[:idx]
	}
	return name
}

// matchListPrefix reports whether name matches one list prefix.
func matchListPrefix(name string, prefixes []string) (bool, string) {
	if len(prefixes) == 0 {
		return true, ""
	}
	for _, p := range prefixes {
		if matchPrefix(name, p) {
			return true, p
		}
	}
	return false, ""
}

// addListPrefixes records child prefixes for one matched name.
func addListPrefixes(prefixSet map[string]struct{}, name string, prefixes []string) {
	if len(prefixes) == 0 {
		prefixSet[topLevelPrefix(name)] = struct{}{}
		return
	}
	for _, p := range prefixes {
		if !matchPrefix(name, p) {
			continue
		}
		rest := strings.TrimPrefix(name, p)
		rest = strings.TrimPrefix(rest, ".")
		rest = strings.TrimPrefix(rest, "/")
		idx := strings.IndexAny(rest, "./")
		if idx >= 0 {
			child := p + "." + rest[:idx]
			prefixSet[child] = struct{}{}
		}
	}
}

// List returns names and prefixes matching the filter.
func (s *MemoryStore) List(prefixes []string, depth uint64) ListParametersResult {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var names []string
	prefixSet := make(map[string]struct{})
	for name := range s.params {
		matched, matchedPrefix := matchListPrefix(name, prefixes)
		if !matched {
			continue
		}
		if depth != ParamDepthRecursive && depthOf(name, matchedPrefix) > depth {
			continue
		}
		names = append(names, name)
		addListPrefixes(prefixSet, name, prefixes)
	}
	sort.Strings(names)
	var outPrefixes []string
	for p := range prefixSet {
		outPrefixes = append(outPrefixes, p)
	}
	sort.Strings(outPrefixes)
	return ListParametersResult{Names: names, Prefixes: outPrefixes}
}

// Events returns a copy of recorded events in order.
func (s *MemoryStore) Events() []ParameterEvent {
	s.mu.RLock()
	defer s.mu.RUnlock()

	out := make([]ParameterEvent, len(s.events))
	for i, e := range s.events {
		e.NewParams = append([]Parameter(nil), e.NewParams...)
		e.ChangedParams = append([]Parameter(nil), e.ChangedParams...)
		e.DeletedParams = append([]Parameter(nil), e.DeletedParams...)
		out[i] = e
	}
	return out
}

// ParamServices holds the six service names for one node.
type ParamServices struct {
	// Describe serves descriptors.
	Describe string
	// Get serves values.
	Get string
	// GetTypes serves type codes.
	GetTypes string
	// List serves name listings.
	List string
	// Set serves independent sets.
	Set string
	// SetAtomically serves atomic sets.
	SetAtomically string
}

// ParamServiceNames returns the six service names for a node.
func ParamServiceNames(node string) (ParamServices, error) {
	trimmed := strings.TrimSpace(node)
	if trimmed == "" {
		return ParamServices{}, errors.New("dds: node name is empty") //nolint:wrapcheck // new error, nothing to wrap
	}
	if trimmed[0] != '/' {
		return ParamServices{}, fmt.Errorf("dds: node name %q must start with slash", node)
	}
	base := strings.TrimSuffix(trimmed, "/")
	if base == "" {
		base = "/"
	}
	join := func(suffix string) string {
		if base == "/" {
			return base + suffix
		}
		return base + "/" + suffix
	}
	return ParamServices{
		Describe:      join(ParamDescribeSuffix),
		Get:           join(ParamGetSuffix),
		GetTypes:      join(ParamGetTypesSuffix),
		List:          join(ParamListSuffix),
		Set:           join(ParamSetSuffix),
		SetAtomically: join(ParamSetAtomicSuffix),
	}, nil
}

// ParamRequestTopic maps a service name to its DDS request topic.
func ParamRequestTopic(service string) (string, error) {
	topic, err := RequestTopic(service, false)
	if err != nil {
		return "", fmt.Errorf("dds: param request topic %q: %w", service, err)
	}
	return topic, nil
}

// ParamReplyTopic maps a service name to its DDS reply topic.
func ParamReplyTopic(service string) (string, error) {
	topic, err := ReplyTopic(service, false)
	if err != nil {
		return "", fmt.Errorf("dds: param reply topic %q: %w", service, err)
	}
	return topic, nil
}

// encodeStringSlice writes a string sequence.
func encodeStringSlice(enc *types.Encoder, values []string) error {
	if values == nil {
		values = []string{}
	}
	err := enc.WriteSequenceLength(len(values))
	if err != nil {
		return fmt.Errorf("dds: encode string count %d: %w", len(values), err)
	}
	for _, v := range values {
		err = enc.WriteString(v)
		if err != nil {
			return fmt.Errorf("dds: encode string %q: %w", v, err)
		}
	}
	return nil
}

// decodeStringSlice reads a string sequence.
func decodeStringSlice(dec *types.Decoder) ([]string, error) {
	n, err := dec.ReadSequenceLength()
	if err != nil {
		return nil, fmt.Errorf("dds: decode string count: %w", err)
	}
	out := make([]string, 0, n)
	for range n {
		v, err := dec.ReadString()
		if err != nil {
			return nil, fmt.Errorf("dds: decode string: %w", err)
		}
		out = append(out, v)
	}
	return out, nil
}

// encodeParamBoolArray writes a bool sequence for one ParameterValue.
func encodeParamBoolArray(enc *types.Encoder, values []bool) error {
	if values == nil {
		values = []bool{}
	}
	err := enc.WriteSequenceLength(len(values))
	if err != nil {
		return fmt.Errorf("dds: encode bool array count: %w", err)
	}
	for _, b := range values {
		enc.WriteBool(b)
	}
	return nil
}

// encodeParamIntArray writes an int64 sequence for one ParameterValue.
func encodeParamIntArray(enc *types.Encoder, values []int64) error {
	if values == nil {
		values = []int64{}
	}
	err := enc.WriteSequenceLength(len(values))
	if err != nil {
		return fmt.Errorf("dds: encode integer array count: %w", err)
	}
	for _, v := range values {
		enc.WriteInt64(v)
	}
	return nil
}

// encodeParamDoubleArray writes a float64 sequence for one ParameterValue.
func encodeParamDoubleArray(enc *types.Encoder, values []float64) error {
	if values == nil {
		values = []float64{}
	}
	err := enc.WriteSequenceLength(len(values))
	if err != nil {
		return fmt.Errorf("dds: encode double array count: %w", err)
	}
	for _, v := range values {
		enc.WriteFloat64(v)
	}
	return nil
}

// encodeParamStringArray writes a string sequence for one ParameterValue.
func encodeParamStringArray(enc *types.Encoder, values []string) error {
	if values == nil {
		values = []string{}
	}
	err := enc.WriteSequenceLength(len(values))
	if err != nil {
		return fmt.Errorf("dds: encode string array count: %w", err)
	}
	for _, v := range values {
		err = enc.WriteString(v)
		if err != nil {
			return fmt.Errorf("dds: encode string array item: %w", err)
		}
	}
	return nil
}

// encodeParamValue writes one ParameterValue in IDL field order.
func encodeParamValue(enc *types.Encoder, value ParamValue) error {
	err := validateParamType(value.Type)
	if err != nil {
		return err
	}
	enc.WriteUint8(value.Type)
	enc.WriteBool(value.Bool)
	enc.WriteInt64(value.Integer)
	enc.WriteFloat64(value.Double)
	err = enc.WriteString(value.String)
	if err != nil {
		return fmt.Errorf("dds: encode param string: %w", err)
	}
	byteArray := value.ByteArray
	if byteArray == nil {
		byteArray = []byte{}
	}
	err = enc.WriteOctetSequence(byteArray)
	if err != nil {
		return fmt.Errorf("dds: encode param bytes: %w", err)
	}
	err = encodeParamBoolArray(enc, value.BoolArray)
	if err != nil {
		return err
	}
	err = encodeParamIntArray(enc, value.IntegerArray)
	if err != nil {
		return err
	}
	err = encodeParamDoubleArray(enc, value.DoubleArray)
	if err != nil {
		return err
	}
	return encodeParamStringArray(enc, value.StringArray)
}

// decodeParamBoolArray reads a bool sequence for one ParameterValue.
func decodeParamBoolArray(dec *types.Decoder) ([]bool, error) {
	count, err := dec.ReadSequenceLength()
	if err != nil {
		return nil, fmt.Errorf("dds: decode bool array count: %w", err)
	}
	out := make([]bool, 0, count)
	for range count {
		v, err := dec.ReadBool()
		if err != nil {
			return nil, fmt.Errorf("dds: decode bool array item: %w", err)
		}
		out = append(out, v)
	}
	return out, nil
}

// decodeParamIntArray reads an int64 sequence for one ParameterValue.
func decodeParamIntArray(dec *types.Decoder) ([]int64, error) {
	count, err := dec.ReadSequenceLength()
	if err != nil {
		return nil, fmt.Errorf("dds: decode integer array count: %w", err)
	}
	out := make([]int64, 0, count)
	for range count {
		v, err := dec.ReadInt64()
		if err != nil {
			return nil, fmt.Errorf("dds: decode integer array item: %w", err)
		}
		out = append(out, v)
	}
	return out, nil
}

// decodeParamDoubleArray reads a float64 sequence for one ParameterValue.
func decodeParamDoubleArray(dec *types.Decoder) ([]float64, error) {
	count, err := dec.ReadSequenceLength()
	if err != nil {
		return nil, fmt.Errorf("dds: decode double array count: %w", err)
	}
	out := make([]float64, 0, count)
	for range count {
		v, err := dec.ReadFloat64()
		if err != nil {
			return nil, fmt.Errorf("dds: decode double array item: %w", err)
		}
		out = append(out, v)
	}
	return out, nil
}

// decodeParamStringArray reads a string sequence for one ParameterValue.
func decodeParamStringArray(dec *types.Decoder) ([]string, error) {
	count, err := dec.ReadSequenceLength()
	if err != nil {
		return nil, fmt.Errorf("dds: decode string array count: %w", err)
	}
	out := make([]string, 0, count)
	for range count {
		v, err := dec.ReadString()
		if err != nil {
			return nil, fmt.Errorf("dds: decode string array item: %w", err)
		}
		out = append(out, v)
	}
	return out, nil
}

// decodeParamScalars reads the scalar prefix of one ParameterValue.
func decodeParamScalars(dec *types.Decoder, out *ParamValue) error {
	b, err := dec.ReadBool()
	if err != nil {
		return fmt.Errorf("dds: decode param bool: %w", err)
	}
	out.Bool = b
	integer, err := dec.ReadInt64()
	if err != nil {
		return fmt.Errorf("dds: decode param integer: %w", err)
	}
	out.Integer = integer
	double, err := dec.ReadFloat64()
	if err != nil {
		return fmt.Errorf("dds: decode param double: %w", err)
	}
	out.Double = double
	str, err := dec.ReadString()
	if err != nil {
		return fmt.Errorf("dds: decode param string: %w", err)
	}
	out.String = str
	byteArray, err := dec.ReadOctetSequence()
	if err != nil {
		return fmt.Errorf("dds: decode param bytes: %w", err)
	}
	out.ByteArray = byteArray
	return nil
}

// decodeParamValue reads one ParameterValue in IDL field order.
func decodeParamValue(dec *types.Decoder) (ParamValue, error) {
	var out ParamValue
	typeByte, err := dec.ReadUint8()
	if err != nil {
		return out, fmt.Errorf("dds: decode param type: %w", err)
	}
	err = validateParamType(typeByte)
	if err != nil {
		return out, err
	}
	out.Type = typeByte
	err = decodeParamScalars(dec, &out)
	if err != nil {
		return out, err
	}
	boolArray, err := decodeParamBoolArray(dec)
	if err != nil {
		return out, err
	}
	out.BoolArray = boolArray
	intArray, err := decodeParamIntArray(dec)
	if err != nil {
		return out, err
	}
	out.IntegerArray = intArray
	doubleArray, err := decodeParamDoubleArray(dec)
	if err != nil {
		return out, err
	}
	out.DoubleArray = doubleArray
	stringArray, err := decodeParamStringArray(dec)
	if err != nil {
		return out, err
	}
	out.StringArray = stringArray
	return out, nil
}

// encodeParameter writes one Parameter message.
func encodeParameter(enc *types.Encoder, param Parameter) error {
	err := validateParamName(param.Name)
	if err != nil {
		return err
	}
	err = enc.WriteString(param.Name)
	if err != nil {
		return fmt.Errorf("dds: encode parameter name: %w", err)
	}
	err = encodeParamValue(enc, param.Value)
	if err != nil {
		return fmt.Errorf("dds: encode parameter %q value: %w", param.Name, err)
	}
	return nil
}

// decodeParameter reads one Parameter message.
func decodeParameter(dec *types.Decoder) (Parameter, error) {
	var out Parameter
	name, err := dec.ReadString()
	if err != nil {
		return out, fmt.Errorf("dds: decode parameter name: %w", err)
	}
	err = validateParamName(name)
	if err != nil {
		return out, err
	}
	out.Name = name
	value, err := decodeParamValue(dec)
	if err != nil {
		return out, fmt.Errorf("dds: decode parameter %q value: %w", name, err)
	}
	out.Value = value
	return out, nil
}

// encodeParameterSeq writes a Parameter sequence.
func encodeParameterSeq(enc *types.Encoder, params []Parameter) error {
	if params == nil {
		params = []Parameter{}
	}
	err := enc.WriteSequenceLength(len(params))
	if err != nil {
		return fmt.Errorf("dds: encode parameter count %d: %w", len(params), err)
	}
	for _, p := range params {
		err = encodeParameter(enc, p)
		if err != nil {
			return err
		}
	}
	return nil
}

// decodeParameterSeq reads a Parameter sequence.
func decodeParameterSeq(dec *types.Decoder) ([]Parameter, error) {
	n, err := dec.ReadSequenceLength()
	if err != nil {
		return nil, fmt.Errorf("dds: decode parameter count: %w", err)
	}
	out := make([]Parameter, 0, n)
	for range n {
		p, err := decodeParameter(dec)
		if err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, nil
}

// validateEncodeDescriptorRanges rejects over bound or mixed descriptor ranges.
func validateEncodeDescriptorRanges(desc ParamDescriptor) error {
	if len(desc.FloatingPointRange) > 1 || len(desc.IntegerRange) > 1 {
		return errors.New("dds: descriptor ranges above bound 1") //nolint:wrapcheck // new error, nothing to wrap
	}
	if len(desc.FloatingPointRange) > 0 && len(desc.IntegerRange) > 0 {
		return errors.New("dds: descriptor ranges are mutually exclusive") //nolint:wrapcheck // new error, nothing to wrap
	}
	return nil
}

// encodeDescriptorFloatRanges writes floating ranges in IDL order.
func encodeDescriptorFloatRanges(enc *types.Encoder, ranges []FloatingPointRange) error {
	err := enc.WriteSequenceLength(len(ranges))
	if err != nil {
		return fmt.Errorf("dds: encode floating range count: %w", err)
	}
	for _, r := range ranges {
		enc.WriteFloat64(r.From)
		enc.WriteFloat64(r.To)
		enc.WriteFloat64(r.Step)
	}
	return nil
}

// encodeDescriptorIntRanges writes integer ranges in IDL order.
func encodeDescriptorIntRanges(enc *types.Encoder, ranges []IntegerRange) error {
	err := enc.WriteSequenceLength(len(ranges))
	if err != nil {
		return fmt.Errorf("dds: encode integer range count: %w", err)
	}
	for _, r := range ranges {
		enc.WriteInt64(r.From)
		enc.WriteInt64(r.To)
		enc.WriteUint64(r.Step)
	}
	return nil
}

// encodeDescriptor writes one ParameterDescriptor in IDL order.
func encodeDescriptor(enc *types.Encoder, desc ParamDescriptor) error {
	err := validateParamName(desc.Name)
	if err != nil {
		return err
	}
	err = validateParamType(desc.Type)
	if err != nil {
		return err
	}
	err = validateEncodeDescriptorRanges(desc)
	if err != nil {
		return err
	}
	err = enc.WriteString(desc.Name)
	if err != nil {
		return fmt.Errorf("dds: encode descriptor name: %w", err)
	}
	enc.WriteUint8(desc.Type)
	err = enc.WriteString(desc.Description)
	if err != nil {
		return fmt.Errorf("dds: encode descriptor description: %w", err)
	}
	err = enc.WriteString(desc.AdditionalConstraints)
	if err != nil {
		return fmt.Errorf("dds: encode descriptor constraints: %w", err)
	}
	enc.WriteBool(desc.ReadOnly)
	enc.WriteBool(desc.DynamicTyping)
	err = encodeDescriptorFloatRanges(enc, desc.FloatingPointRange)
	if err != nil {
		return err
	}
	return encodeDescriptorIntRanges(enc, desc.IntegerRange)
}

// decodeRangeCount reads a range count with bound 1 in IDL order.
func decodeRangeCount(dec *types.Decoder, countMsg, boundMsg string) (int, error) {
	count, err := dec.ReadSequenceLength()
	if err != nil {
		return 0, fmt.Errorf("%s: %w", countMsg, err)
	}
	if count > 1 {
		return 0, fmt.Errorf("%s %d above bound 1", boundMsg, count)
	}
	return count, nil
}

// readFloatingPointRange reads one floating range triple in IDL order.
func readFloatingPointRange(dec *types.Decoder) (FloatingPointRange, error) {
	var r FloatingPointRange
	from, err := dec.ReadFloat64()
	if err != nil {
		return r, fmt.Errorf("dds: decode floating from: %w", err)
	}
	to, err := dec.ReadFloat64()
	if err != nil {
		return r, fmt.Errorf("dds: decode floating to: %w", err)
	}
	step, err := dec.ReadFloat64()
	if err != nil {
		return r, fmt.Errorf("dds: decode floating step: %w", err)
	}
	r = FloatingPointRange{From: from, To: to, Step: step}
	return r, nil
}

// readIntegerRange reads one integer range triple in IDL order.
func readIntegerRange(dec *types.Decoder) (IntegerRange, error) {
	var r IntegerRange
	from, err := dec.ReadInt64()
	if err != nil {
		return r, fmt.Errorf("dds: decode integer from: %w", err)
	}
	to, err := dec.ReadInt64()
	if err != nil {
		return r, fmt.Errorf("dds: decode integer to: %w", err)
	}
	step, err := dec.ReadUint64()
	if err != nil {
		return r, fmt.Errorf("dds: decode integer step: %w", err)
	}
	r = IntegerRange{From: from, To: to, Step: step}
	return r, nil
}

// decodeDescriptorFloatRanges reads floating ranges in IDL order.
func decodeDescriptorFloatRanges(dec *types.Decoder) ([]FloatingPointRange, error) {
	count, err := decodeRangeCount(dec, "dds: decode floating range count", "dds: floating range count")
	if err != nil {
		return nil, err
	}
	out := make([]FloatingPointRange, 0, count)
	for range count {
		r, err := readFloatingPointRange(dec)
		if err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, nil
}

// decodeDescriptorIntRanges reads integer ranges in IDL order.
func decodeDescriptorIntRanges(dec *types.Decoder) ([]IntegerRange, error) {
	count, err := decodeRangeCount(dec, "dds: decode integer range count", "dds: integer range count")
	if err != nil {
		return nil, err
	}
	out := make([]IntegerRange, 0, count)
	for range count {
		r, err := readIntegerRange(dec)
		if err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, nil
}

// decodeDescriptorHeader reads the scalar prefix of one ParameterDescriptor.
func decodeDescriptorHeader(dec *types.Decoder, out *ParamDescriptor) error {
	name, err := dec.ReadString()
	if err != nil {
		return fmt.Errorf("dds: decode descriptor name: %w", err)
	}
	err = validateParamName(name)
	if err != nil {
		return err
	}
	out.Name = name
	typeByte, err := dec.ReadUint8()
	if err != nil {
		return fmt.Errorf("dds: decode descriptor type: %w", err)
	}
	err = validateParamType(typeByte)
	if err != nil {
		return err
	}
	out.Type = typeByte
	description, err := dec.ReadString()
	if err != nil {
		return fmt.Errorf("dds: decode descriptor description: %w", err)
	}
	out.Description = description
	constraints, err := dec.ReadString()
	if err != nil {
		return fmt.Errorf("dds: decode descriptor constraints: %w", err)
	}
	out.AdditionalConstraints = constraints
	readOnly, err := dec.ReadBool()
	if err != nil {
		return fmt.Errorf("dds: decode descriptor read only: %w", err)
	}
	out.ReadOnly = readOnly
	dynamic, err := dec.ReadBool()
	if err != nil {
		return fmt.Errorf("dds: decode descriptor dynamic typing: %w", err)
	}
	out.DynamicTyping = dynamic
	return nil
}

// decodeDescriptor reads one ParameterDescriptor in IDL order.
func decodeDescriptor(dec *types.Decoder) (ParamDescriptor, error) {
	var out ParamDescriptor
	err := decodeDescriptorHeader(dec, &out)
	if err != nil {
		return out, err
	}
	floatRanges, err := decodeDescriptorFloatRanges(dec)
	if err != nil {
		return out, err
	}
	out.FloatingPointRange = floatRanges
	intRanges, err := decodeDescriptorIntRanges(dec)
	if err != nil {
		return out, err
	}
	out.IntegerRange = intRanges
	if len(out.FloatingPointRange) > 0 && len(out.IntegerRange) > 0 {
		return out, errors.New("dds: descriptor ranges are mutually exclusive") //nolint:wrapcheck // new error, nothing to wrap
	}
	return out, nil
}

// encodeResult writes one SetParametersResult.
func encodeResult(enc *types.Encoder, result SetParametersResult) error {
	enc.WriteBool(result.Successful)
	err := enc.WriteString(result.Reason)
	if err != nil {
		return fmt.Errorf("dds: encode result reason: %w", err)
	}
	return nil
}

// decodeResult reads one SetParametersResult.
func decodeResult(dec *types.Decoder) (SetParametersResult, error) {
	var out SetParametersResult
	ok, err := dec.ReadBool()
	if err != nil {
		return out, fmt.Errorf("dds: decode result ok: %w", err)
	}
	out.Successful = ok
	reason, err := dec.ReadString()
	if err != nil {
		return out, fmt.Errorf("dds: decode result reason: %w", err)
	}
	out.Reason = reason
	return out, nil
}

// newServiceEncoder returns an encoder with the service header written.
func newServiceEncoder(guid [8]byte, seq uint64, littleEndian bool) (*types.Encoder, error) {
	enc := types.NewEncoder(littleEndian)
	err := enc.WriteServiceHeader(guid, seq)
	if err != nil {
		return nil, fmt.Errorf("dds: write service header: %w", err)
	}
	return enc, nil
}

// newServiceDecoder returns a decoder positioned after the service header.
func newServiceDecoder(wire []byte) (*types.Decoder, [8]byte, uint64, error) {
	var guid [8]byte
	dec, err := types.NewDecoder(wire)
	if err != nil {
		return nil, guid, 0, fmt.Errorf("dds: decode service header: %w", err)
	}
	gotGUID, seq, err := dec.ReadServiceHeader()
	if err != nil {
		return nil, guid, 0, fmt.Errorf("dds: read service header: %w", err)
	}
	return dec, gotGUID, seq, nil
}

// finishService checks strict consumption of one service sample.
//
// Encoder.Bytes pads the wire to 4 bytes with zeros. A final octet or bool
// sequence can leave 1 to 3 zero pad bytes after the last field. The pad is
// drained here and must be zero; any nonzero trailing byte fails.
func finishService(dec *types.Decoder) error {
	for dec.Remaining() > 0 {
		pad, err := dec.ReadUint8()
		if err != nil {
			return fmt.Errorf("dds: trailing service bytes: %w", err)
		}
		if pad != 0 {
			return fmt.Errorf("dds: trailing service byte 0x%02x", pad)
		}
	}
	err := dec.Finish()
	if err != nil {
		return fmt.Errorf("dds: trailing service bytes: %w", err)
	}
	return nil
}

// encodeServiceItems encodes one service response carrying a sequence.
// countMsg and itemMsg name the failing field for wrapped errors.
func encodeServiceItems[T any](guid [8]byte, seq uint64, littleEndian bool, items []T, countMsg, itemMsg string, encodeItem func(*types.Encoder, T) error) ([]byte, error) {
	enc, err := newServiceEncoder(guid, seq, littleEndian)
	if err != nil {
		return nil, err
	}
	if items == nil {
		items = []T{}
	}
	err = enc.WriteSequenceLength(len(items))
	if err != nil {
		return nil, fmt.Errorf("%s: %w", countMsg, err)
	}
	for _, item := range items {
		err = encodeItem(enc, item)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", itemMsg, err)
		}
	}
	return enc.Bytes(), nil
}

// decodeServiceItems decodes one service response carrying a sequence.
// countMsg names the count field for wrapped errors.
func decodeServiceItems[T any](wire []byte, countMsg string, decodeItem func(*types.Decoder) (T, error)) ([]T, [8]byte, uint64, error) {
	var guid [8]byte
	dec, gotGUID, seq, err := newServiceDecoder(wire)
	if err != nil {
		return nil, guid, 0, err
	}
	n, err := dec.ReadSequenceLength()
	if err != nil {
		return nil, guid, 0, fmt.Errorf("%s: %w", countMsg, err)
	}
	out := make([]T, 0, n)
	for range n {
		item, err := decodeItem(dec)
		if err != nil {
			return nil, guid, 0, err
		}
		out = append(out, item)
	}
	err = finishService(dec)
	if err != nil {
		return nil, guid, 0, err
	}
	return out, gotGUID, seq, nil
}

// EncodeGetParametersRequest encodes a GetParameters request.
func EncodeGetParametersRequest(names []string, guid [8]byte, seq uint64, littleEndian bool) ([]byte, error) {
	enc, err := newServiceEncoder(guid, seq, littleEndian)
	if err != nil {
		return nil, err
	}
	err = encodeStringSlice(enc, names)
	if err != nil {
		return nil, fmt.Errorf("dds: encode get request: %w", err)
	}
	return enc.Bytes(), nil
}

// DecodeGetParametersRequest decodes a GetParameters request.
func DecodeGetParametersRequest(wire []byte) ([]string, [8]byte, uint64, error) {
	var guid [8]byte
	dec, gotGUID, seq, err := newServiceDecoder(wire)
	if err != nil {
		return nil, guid, 0, err
	}
	names, err := decodeStringSlice(dec)
	if err != nil {
		return nil, guid, 0, fmt.Errorf("dds: decode get request: %w", err)
	}
	err = finishService(dec)
	if err != nil {
		return nil, guid, 0, err
	}
	return names, gotGUID, seq, nil
}

// EncodeGetParametersResponse encodes a GetParameters response.
func EncodeGetParametersResponse(values []ParamValue, guid [8]byte, seq uint64, littleEndian bool) ([]byte, error) {
	return encodeServiceItems(guid, seq, littleEndian, values, "dds: encode get response count", "dds: encode get response value", encodeParamValue)
}

// DecodeGetParametersResponse decodes a GetParameters response.
func DecodeGetParametersResponse(wire []byte) ([]ParamValue, [8]byte, uint64, error) {
	var guid [8]byte
	dec, gotGUID, seq, err := newServiceDecoder(wire)
	if err != nil {
		return nil, guid, 0, err
	}
	n, err := dec.ReadSequenceLength()
	if err != nil {
		return nil, guid, 0, fmt.Errorf("dds: decode get response count: %w", err)
	}
	out := make([]ParamValue, 0, n)
	for range n {
		v, err := decodeParamValue(dec)
		if err != nil {
			return nil, guid, 0, fmt.Errorf("dds: decode get response value: %w", err)
		}
		out = append(out, v)
	}
	err = finishService(dec)
	if err != nil {
		return nil, guid, 0, err
	}
	return out, gotGUID, seq, nil
}

// EncodeGetParameterTypesRequest encodes a GetParameterTypes request.
func EncodeGetParameterTypesRequest(names []string, guid [8]byte, seq uint64, littleEndian bool) ([]byte, error) {
	enc, err := newServiceEncoder(guid, seq, littleEndian)
	if err != nil {
		return nil, err
	}
	err = encodeStringSlice(enc, names)
	if err != nil {
		return nil, fmt.Errorf("dds: encode get types request: %w", err)
	}
	return enc.Bytes(), nil
}

// DecodeGetParameterTypesRequest decodes a GetParameterTypes request.
func DecodeGetParameterTypesRequest(wire []byte) ([]string, [8]byte, uint64, error) {
	return DecodeGetParametersRequest(wire)
}

// EncodeGetParameterTypesResponse encodes a GetParameterTypes response.
func EncodeGetParameterTypesResponse(types []uint8, guid [8]byte, seq uint64, littleEndian bool) ([]byte, error) {
	enc, err := newServiceEncoder(guid, seq, littleEndian)
	if err != nil {
		return nil, err
	}
	if types == nil {
		types = []uint8{}
	}
	for _, t := range types {
		err = validateParamType(t)
		if err != nil {
			return nil, err
		}
	}
	err = enc.WriteOctetSequence(types)
	if err != nil {
		return nil, fmt.Errorf("dds: encode get types response: %w", err)
	}
	return enc.Bytes(), nil
}

// DecodeGetParameterTypesResponse decodes a GetParameterTypes response.
func DecodeGetParameterTypesResponse(wire []byte) ([]uint8, [8]byte, uint64, error) {
	var guid [8]byte
	dec, gotGUID, seq, err := newServiceDecoder(wire)
	if err != nil {
		return nil, guid, 0, err
	}
	values, err := dec.ReadOctetSequence()
	if err != nil {
		return nil, guid, 0, fmt.Errorf("dds: decode get types response: %w", err)
	}
	for _, t := range values {
		err = validateParamType(t)
		if err != nil {
			return nil, guid, 0, err
		}
	}
	err = finishService(dec)
	if err != nil {
		return nil, guid, 0, err
	}
	return values, gotGUID, seq, nil
}

// EncodeSetParametersRequest encodes a SetParameters request.
func EncodeSetParametersRequest(params []Parameter, guid [8]byte, seq uint64, littleEndian bool) ([]byte, error) {
	enc, err := newServiceEncoder(guid, seq, littleEndian)
	if err != nil {
		return nil, err
	}
	err = encodeParameterSeq(enc, params)
	if err != nil {
		return nil, fmt.Errorf("dds: encode set request: %w", err)
	}
	return enc.Bytes(), nil
}

// DecodeSetParametersRequest decodes a SetParameters request.
func DecodeSetParametersRequest(wire []byte) ([]Parameter, [8]byte, uint64, error) {
	var guid [8]byte
	dec, gotGUID, seq, err := newServiceDecoder(wire)
	if err != nil {
		return nil, guid, 0, err
	}
	params, err := decodeParameterSeq(dec)
	if err != nil {
		return nil, guid, 0, fmt.Errorf("dds: decode set request: %w", err)
	}
	err = finishService(dec)
	if err != nil {
		return nil, guid, 0, err
	}
	return params, gotGUID, seq, nil
}

// EncodeSetParametersResponse encodes a SetParameters response.
func EncodeSetParametersResponse(results []SetParametersResult, guid [8]byte, seq uint64, littleEndian bool) ([]byte, error) {
	enc, err := newServiceEncoder(guid, seq, littleEndian)
	if err != nil {
		return nil, err
	}
	if results == nil {
		results = []SetParametersResult{}
	}
	err = enc.WriteSequenceLength(len(results))
	if err != nil {
		return nil, fmt.Errorf("dds: encode set response count: %w", err)
	}
	for _, r := range results {
		err = encodeResult(enc, r)
		if err != nil {
			return nil, err
		}
	}
	return enc.Bytes(), nil
}

// DecodeSetParametersResponse decodes a SetParameters response.
func DecodeSetParametersResponse(wire []byte) ([]SetParametersResult, [8]byte, uint64, error) {
	return decodeServiceItems(wire, "dds: decode set response count", decodeResult)
}

// EncodeSetAtomicallyRequest encodes a SetParametersAtomically request.
func EncodeSetAtomicallyRequest(params []Parameter, guid [8]byte, seq uint64, littleEndian bool) ([]byte, error) {
	return EncodeSetParametersRequest(params, guid, seq, littleEndian)
}

// DecodeSetAtomicallyRequest decodes a SetParametersAtomically request.
func DecodeSetAtomicallyRequest(wire []byte) ([]Parameter, [8]byte, uint64, error) {
	return DecodeSetParametersRequest(wire)
}

// EncodeSetAtomicallyResponse encodes a SetParametersAtomically response.
func EncodeSetAtomicallyResponse(result SetParametersResult, guid [8]byte, seq uint64, littleEndian bool) ([]byte, error) {
	enc, err := newServiceEncoder(guid, seq, littleEndian)
	if err != nil {
		return nil, err
	}
	err = encodeResult(enc, result)
	if err != nil {
		return nil, fmt.Errorf("dds: encode set atomically response: %w", err)
	}
	return enc.Bytes(), nil
}

// DecodeSetAtomicallyResponse decodes a SetParametersAtomically response.
func DecodeSetAtomicallyResponse(wire []byte) (SetParametersResult, [8]byte, uint64, error) {
	var empty SetParametersResult
	var guid [8]byte
	dec, gotGUID, seq, err := newServiceDecoder(wire)
	if err != nil {
		return empty, guid, 0, err
	}
	result, err := decodeResult(dec)
	if err != nil {
		return empty, guid, 0, err
	}
	err = finishService(dec)
	if err != nil {
		return empty, guid, 0, err
	}
	return result, gotGUID, seq, nil
}

// EncodeListParametersRequest encodes a ListParameters request.
func EncodeListParametersRequest(prefixes []string, depth uint64, guid [8]byte, seq uint64, littleEndian bool) ([]byte, error) {
	enc, err := newServiceEncoder(guid, seq, littleEndian)
	if err != nil {
		return nil, err
	}
	err = encodeStringSlice(enc, prefixes)
	if err != nil {
		return nil, fmt.Errorf("dds: encode list request prefixes: %w", err)
	}
	enc.WriteUint64(depth)
	return enc.Bytes(), nil
}

// DecodeListParametersRequest decodes a ListParameters request.
func DecodeListParametersRequest(wire []byte) ([]string, uint64, [8]byte, uint64, error) {
	var guid [8]byte
	dec, gotGUID, seq, err := newServiceDecoder(wire)
	if err != nil {
		return nil, 0, guid, 0, err
	}
	prefixes, err := decodeStringSlice(dec)
	if err != nil {
		return nil, 0, guid, 0, fmt.Errorf("dds: decode list request prefixes: %w", err)
	}
	depth, err := dec.ReadUint64()
	if err != nil {
		return nil, 0, guid, 0, fmt.Errorf("dds: decode list request depth: %w", err)
	}
	err = finishService(dec)
	if err != nil {
		return nil, 0, guid, 0, err
	}
	return prefixes, depth, gotGUID, seq, nil
}

// EncodeListParametersResponse encodes a ListParameters response.
func EncodeListParametersResponse(result ListParametersResult, guid [8]byte, seq uint64, littleEndian bool) ([]byte, error) {
	enc, err := newServiceEncoder(guid, seq, littleEndian)
	if err != nil {
		return nil, err
	}
	names := result.Names
	if names == nil {
		names = []string{}
	}
	prefixes := result.Prefixes
	if prefixes == nil {
		prefixes = []string{}
	}
	err = encodeStringSlice(enc, names)
	if err != nil {
		return nil, fmt.Errorf("dds: encode list response names: %w", err)
	}
	err = encodeStringSlice(enc, prefixes)
	if err != nil {
		return nil, fmt.Errorf("dds: encode list response prefixes: %w", err)
	}
	return enc.Bytes(), nil
}

// DecodeListParametersResponse decodes a ListParameters response.
func DecodeListParametersResponse(wire []byte) (ListParametersResult, [8]byte, uint64, error) {
	var empty ListParametersResult
	var guid [8]byte
	dec, gotGUID, seq, err := newServiceDecoder(wire)
	if err != nil {
		return empty, guid, 0, err
	}
	names, err := decodeStringSlice(dec)
	if err != nil {
		return empty, guid, 0, fmt.Errorf("dds: decode list response names: %w", err)
	}
	prefixes, err := decodeStringSlice(dec)
	if err != nil {
		return empty, guid, 0, fmt.Errorf("dds: decode list response prefixes: %w", err)
	}
	err = finishService(dec)
	if err != nil {
		return empty, guid, 0, err
	}
	return ListParametersResult{Names: names, Prefixes: prefixes}, gotGUID, seq, nil
}

// EncodeDescribeParametersRequest encodes a DescribeParameters request.
func EncodeDescribeParametersRequest(names []string, guid [8]byte, seq uint64, littleEndian bool) ([]byte, error) {
	enc, err := newServiceEncoder(guid, seq, littleEndian)
	if err != nil {
		return nil, err
	}
	err = encodeStringSlice(enc, names)
	if err != nil {
		return nil, fmt.Errorf("dds: encode describe request: %w", err)
	}
	return enc.Bytes(), nil
}

// DecodeDescribeParametersRequest decodes a DescribeParameters request.
func DecodeDescribeParametersRequest(wire []byte) ([]string, [8]byte, uint64, error) {
	return DecodeGetParametersRequest(wire)
}

// EncodeDescribeParametersResponse encodes a DescribeParameters response.
func EncodeDescribeParametersResponse(descs []ParamDescriptor, guid [8]byte, seq uint64, littleEndian bool) ([]byte, error) {
	return encodeServiceItems(guid, seq, littleEndian, descs, "dds: encode describe response count", "dds: encode describe response item", encodeDescriptor)
}

// DecodeDescribeParametersResponse decodes a DescribeParameters response.
func DecodeDescribeParametersResponse(wire []byte) ([]ParamDescriptor, [8]byte, uint64, error) {
	return decodeServiceItems(wire, "dds: decode describe response count", decodeDescriptor)
}

// validateParameterEventNode rejects empty, relative, or whitespace node names.
func validateParameterEventNode(node string) error {
	if node == "" {
		return errors.New("dds: event node is empty") //nolint:wrapcheck // new error, nothing to wrap
	}
	if node[0] != '/' {
		return fmt.Errorf("dds: event node %q must start with slash", node)
	}
	if node == "/" {
		return nil
	}
	trimmed := strings.TrimPrefix(node, "/")
	if strings.ContainsAny(trimmed, " \t\n\r") {
		return fmt.Errorf("dds: event node %q has whitespace", node)
	}
	if trimmed == "" {
		return fmt.Errorf("dds: event node %q is empty", node)
	}
	return nil
}

// encodeParameterEventParams writes the three parameter sequences of one event.
func encodeParameterEventParams(enc *types.Encoder, event ParameterEvent) error {
	newParams := event.NewParams
	if newParams == nil {
		newParams = []Parameter{}
	}
	changed := event.ChangedParams
	if changed == nil {
		changed = []Parameter{}
	}
	deleted := event.DeletedParams
	if deleted == nil {
		deleted = []Parameter{}
	}
	err := encodeParameterSeq(enc, newParams)
	if err != nil {
		return fmt.Errorf("dds: encode event new: %w", err)
	}
	err = encodeParameterSeq(enc, changed)
	if err != nil {
		return fmt.Errorf("dds: encode event changed: %w", err)
	}
	err = encodeParameterSeq(enc, deleted)
	if err != nil {
		return fmt.Errorf("dds: encode event deleted: %w", err)
	}
	return nil
}

// EncodeParameterEvent encodes a ParameterEvent topic sample.
func EncodeParameterEvent(event ParameterEvent, littleEndian bool) ([]byte, error) {
	err := validateParameterEventNode(event.Node)
	if err != nil {
		return nil, err
	}
	enc := types.NewEncoder(littleEndian)
	enc.WriteInt32(event.StampSec)
	enc.WriteUint32(event.StampNanosec)
	err = enc.WriteString(event.Node)
	if err != nil {
		return nil, fmt.Errorf("dds: encode event node: %w", err)
	}
	err = encodeParameterEventParams(enc, event)
	if err != nil {
		return nil, err
	}
	return enc.Bytes(), nil
}

// DecodeParameterEvent decodes a ParameterEvent topic sample.
func DecodeParameterEvent(payload []byte) (ParameterEvent, error) {
	var out ParameterEvent
	dec, err := types.NewDecoder(payload)
	if err != nil {
		return out, fmt.Errorf("dds: decode event header: %w", err)
	}
	sec, err := dec.ReadInt32()
	if err != nil {
		return out, fmt.Errorf("dds: decode event sec: %w", err)
	}
	out.StampSec = sec
	nsec, err := dec.ReadUint32()
	if err != nil {
		return out, fmt.Errorf("dds: decode event nsec: %w", err)
	}
	out.StampNanosec = nsec
	node, err := dec.ReadString()
	if err != nil {
		return out, fmt.Errorf("dds: decode event node: %w", err)
	}
	out.Node = node
	newParams, err := decodeParameterSeq(dec)
	if err != nil {
		return out, fmt.Errorf("dds: decode event new: %w", err)
	}
	out.NewParams = newParams
	changed, err := decodeParameterSeq(dec)
	if err != nil {
		return out, fmt.Errorf("dds: decode event changed: %w", err)
	}
	out.ChangedParams = changed
	deleted, err := decodeParameterSeq(dec)
	if err != nil {
		return out, fmt.Errorf("dds: decode event deleted: %w", err)
	}
	out.DeletedParams = deleted
	for dec.Remaining() > 0 {
		pad, err := dec.ReadUint8()
		if err != nil {
			return out, fmt.Errorf("dds: trailing event bytes: %w", err)
		}
		if pad != 0 {
			return out, fmt.Errorf("dds: trailing event byte 0x%02x", pad)
		}
	}
	err = dec.Finish()
	if err != nil {
		return out, fmt.Errorf("dds: trailing event bytes: %w", err)
	}
	return out, nil
}

// ServeGetParameters serves one GetParameters request from the store.
func ServeGetParameters(store ParamStore, wireReq []byte, littleEndian bool) ([]byte, error) {
	names, guid, seq, err := DecodeGetParametersRequest(wireReq)
	if err != nil {
		return nil, err
	}
	values := store.GetMany(names)
	resp, err := EncodeGetParametersResponse(values, guid, seq, littleEndian)
	if err != nil {
		return nil, fmt.Errorf("dds: serve get: %w", err)
	}
	return resp, nil
}

// ServeGetParameterTypes serves one GetParameterTypes request from the store.
func ServeGetParameterTypes(store ParamStore, wireReq []byte, littleEndian bool) ([]byte, error) {
	names, guid, seq, err := DecodeGetParameterTypesRequest(wireReq)
	if err != nil {
		return nil, err
	}
	typeCodes := store.GetTypes(names)
	resp, err := EncodeGetParameterTypesResponse(typeCodes, guid, seq, littleEndian)
	if err != nil {
		return nil, fmt.Errorf("dds: serve get types: %w", err)
	}
	return resp, nil
}

// ServeSetParameters serves one SetParameters request from the store.
func ServeSetParameters(store ParamStore, wireReq []byte, littleEndian bool) ([]byte, error) {
	params, guid, seq, err := DecodeSetParametersRequest(wireReq)
	if err != nil {
		return nil, err
	}
	results := store.SetMany(params)
	resp, err := EncodeSetParametersResponse(results, guid, seq, littleEndian)
	if err != nil {
		return nil, fmt.Errorf("dds: serve set: %w", err)
	}
	return resp, nil
}

// ServeSetAtomically serves one SetParametersAtomically request from the store.
func ServeSetAtomically(store ParamStore, wireReq []byte, littleEndian bool) ([]byte, error) {
	params, guid, seq, err := DecodeSetAtomicallyRequest(wireReq)
	if err != nil {
		return nil, err
	}
	result := store.SetAtomically(params)
	resp, err := EncodeSetAtomicallyResponse(result, guid, seq, littleEndian)
	if err != nil {
		return nil, fmt.Errorf("dds: serve set atomically: %w", err)
	}
	return resp, nil
}

// ServeListParameters serves one ListParameters request from the store.
func ServeListParameters(store ParamStore, wireReq []byte, littleEndian bool) ([]byte, error) {
	prefixes, depth, guid, seq, err := DecodeListParametersRequest(wireReq)
	if err != nil {
		return nil, err
	}
	result := store.List(prefixes, depth)
	resp, err := EncodeListParametersResponse(result, guid, seq, littleEndian)
	if err != nil {
		return nil, fmt.Errorf("dds: serve list: %w", err)
	}
	return resp, nil
}

// ServeDescribeParameters serves one DescribeParameters request from the store.
func ServeDescribeParameters(store ParamStore, wireReq []byte, littleEndian bool) ([]byte, error) {
	names, guid, seq, err := DecodeDescribeParametersRequest(wireReq)
	if err != nil {
		return nil, err
	}
	descs := store.Describe(names)
	resp, err := EncodeDescribeParametersResponse(descs, guid, seq, littleEndian)
	if err != nil {
		return nil, fmt.Errorf("dds: serve describe: %w", err)
	}
	return resp, nil
}
