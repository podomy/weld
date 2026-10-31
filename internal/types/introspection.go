// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

// Package types holds introspection tables mirroring
// rosidl_typesupport_introspection_c without cgo.
package types

import (
	"errors"
	"fmt"
	"reflect"
	"sync"
)

// IntrospectionIdentifier is the typesupport identifier for C introspection.
const IntrospectionIdentifier = "rosidl_typesupport_introspection_c"

// RosTypeID mirrors rosidl_typesupport_introspection_c field type numbers.
// These numbers are distinct from FieldType numeric IDs used in descriptions.
type RosTypeID uint8

// Introspection type numbers 1 through 18.
const (
	RosTypeFloat   RosTypeID = 1
	RosTypeDouble  RosTypeID = 2
	RosTypeLongDbl RosTypeID = 3
	RosTypeChar    RosTypeID = 4
	RosTypeWChar   RosTypeID = 5
	RosTypeBoolean RosTypeID = 6
	RosTypeOctet   RosTypeID = 7
	RosTypeUint8   RosTypeID = 8
	RosTypeInt8    RosTypeID = 9
	RosTypeUint16  RosTypeID = 10
	RosTypeInt16   RosTypeID = 11
	RosTypeUint32  RosTypeID = 12
	RosTypeInt32   RosTypeID = 13
	RosTypeUint64  RosTypeID = 14
	RosTypeInt64   RosTypeID = 15
	RosTypeString  RosTypeID = 16
	RosTypeWString RosTypeID = 17
	RosTypeMessage RosTypeID = 18
	RosTypeFloat32 RosTypeID = RosTypeFloat
	RosTypeFloat64 RosTypeID = RosTypeDouble
	RosTypeBool    RosTypeID = RosTypeBoolean
	RosTypeByte    RosTypeID = RosTypeOctet
)

// rosTypeIDNames maps canonical type ids to symbolic names.
// Aliases share numeric values with their canonical entry so a single
// lookup covers them. Unknown values fall back to ROS_TYPE_UNKNOWN.
var rosTypeIDNames = map[RosTypeID]string{
	RosTypeFloat:   "ROS_TYPE_FLOAT",
	RosTypeDouble:  "ROS_TYPE_DOUBLE",
	RosTypeLongDbl: "ROS_TYPE_LONG_DOUBLE",
	RosTypeChar:    "ROS_TYPE_CHAR",
	RosTypeWChar:   "ROS_TYPE_WCHAR",
	RosTypeBoolean: "ROS_TYPE_BOOLEAN",
	RosTypeOctet:   "ROS_TYPE_OCTET",
	RosTypeUint8:   "ROS_TYPE_UINT8",
	RosTypeInt8:    "ROS_TYPE_INT8",
	RosTypeUint16:  "ROS_TYPE_UINT16",
	RosTypeInt16:   "ROS_TYPE_INT16",
	RosTypeUint32:  "ROS_TYPE_UINT32",
	RosTypeInt32:   "ROS_TYPE_INT32",
	RosTypeUint64:  "ROS_TYPE_UINT64",
	RosTypeInt64:   "ROS_TYPE_INT64",
	RosTypeString:  "ROS_TYPE_STRING",
	RosTypeWString: "ROS_TYPE_WSTRING",
	RosTypeMessage: "ROS_TYPE_MESSAGE",
}

// String returns the symbolic name of the type id.
func (t RosTypeID) String() string {
	if name, ok := rosTypeIDNames[t]; ok {
		return name
	}
	return "ROS_TYPE_UNKNOWN"
}

// MessageInit selects how a message is initialized.
type MessageInit int

// Initialization modes mirroring rosidl_runtime_c message initialization.
const (
	MessageInitAll          MessageInit = 0
	MessageInitSkip         MessageInit = 1
	MessageInitZero         MessageInit = 2
	MessageInitDefaultsOnly MessageInit = 3
)

// String returns the symbolic name of the init mode.
func (m MessageInit) String() string {
	switch m {
	case MessageInitAll:
		return "ALL"
	case MessageInitSkip:
		return "SKIP"
	case MessageInitZero:
		return "ZERO"
	case MessageInitDefaultsOnly:
		return "DEFAULTS_ONLY"
	default:
		return "UNKNOWN"
	}
}

// Validate checks that the init mode is one of the four known values.
func (m MessageInit) Validate() error {
	switch m {
	case MessageInitAll, MessageInitSkip, MessageInitZero, MessageInitDefaultsOnly:
		return nil
	default:
		return fmt.Errorf("types: unknown message init mode %d", int(m))
	}
}

// SizeFunc returns the element count of an array or sequence member.
type SizeFunc func(base any) (uint, error)

// GetConstFunc returns a copy of the element at index.
// The Go port returns a copy, not a C style const pointer.
type GetConstFunc func(base any, index uint) (any, error)

// GetFunc returns a copy of the element at index.
// The Go port returns a copy; unlike C it does not expose a mutable
// pointer. Mutate through Assign or Fetch instead.
type GetFunc func(base any, index uint) (any, error)

// FetchFunc copies the element at index into out.
type FetchFunc func(base any, index uint, out any) error

// AssignFunc copies val into the element at index.
type AssignFunc func(base any, index uint, val any) error

// ResizeFunc resizes a sequence member with fini plus init semantics.
type ResizeFunc func(base any, size uint) error

// MessageInitFunc initializes a message instance per init mode.
type MessageInitFunc func(base any, init MessageInit) error

// MessageFiniFunc finalizes a message instance.
type MessageFiniFunc func(base any) error

// MessageMember describes one field of an interface type.
//
// Offset holds a Go struct layout offset from unsafe.Offsetof for
// reflection access only. It is not a C ABI offset and varies by
// architecture and Go layout rules.
//
// FieldName wires array accessors to a Go struct field by exact name.
// A typo fails at accessor call time, so constructors require a
// non-empty FieldName for every array member and Validate enforces it.
//
// NestedNamespace and NestedName identify the nested table used by
// lazy patching when Members is nil. At least one source must be set
// for every nested member: either Members or both reference strings.
type MessageMember struct {
	Name             string
	TypeID           RosTypeID
	StringUpperBound uint
	Members          *MessageMembers
	IsKey            bool
	IsArray          bool
	ArraySize        uint
	IsUpperBound     bool
	Offset           uint32
	DefaultValue     any
	SizeFunc         SizeFunc
	GetConstFunc     GetConstFunc
	GetFunc          GetFunc
	FetchFunc        FetchFunc
	AssignFunc       AssignFunc
	ResizeFunc       ResizeFunc
	IsRosidlBuffer   bool
	FieldName        string
	NestedNamespace  string
	NestedName       string
}

// IsSequence reports true for bounded or unbounded sequences.
func (m *MessageMember) IsSequence() bool {
	return m.IsArray && m.IsUpperBound
}

// IsFixedArray reports true for inline fixed size arrays.
func (m *MessageMember) IsFixedArray() bool {
	return m.IsArray && !m.IsUpperBound
}

// IsStringField reports true for scalar string or wstring fields only.
// Array and sequence string members return false even though their
// element type is a string; use ValueKindFor to distinguish bounded
// strings from string sequences. The name is kept for compatibility;
// IsScalarStringField is an explicit alias with the same semantics.
func (m *MessageMember) IsStringField() bool {
	if m.IsArray {
		return false
	}
	return m.TypeID == RosTypeString || m.TypeID == RosTypeWString
}

// IsScalarStringField is an explicit alias for IsStringField.
// It reports true only for non-array string fields.
func (m *MessageMember) IsScalarStringField() bool {
	return m.IsStringField()
}

// IsNested reports true for embedded message fields.
func (m *MessageMember) IsNested() bool {
	return m.TypeID == RosTypeMessage
}

// IsBoolVector reports true for boolean sequences only.
// Fixed boolean arrays are not bool vectors; the DDS bool vector
// special case applies to sequences (IsUpperBound true).
func (m *MessageMember) IsBoolVector() bool {
	return m.TypeID == RosTypeBoolean && m.IsSequence()
}

// IsBuffer reports true for uint8 sequences flagged as rosidl buffers.
func (m *MessageMember) IsBuffer() bool {
	if !m.IsRosidlBuffer {
		return false
	}
	if !m.IsSequence() {
		return false
	}
	return m.TypeID == RosTypeUint8 || m.TypeID == RosTypeOctet || m.TypeID == RosTypeByte
}

// NestedMembers returns the nested table, patching it lazily on first call.
// The patch is guarded by a global mutex so concurrent callers race safely.
// Reads and writes of Members hold nestedPatchMu through nestedMembersLocked.
func (m *MessageMember) NestedMembers() (*MessageMembers, error) {
	nestedPatchMu.Lock()
	defer nestedPatchMu.Unlock()
	return m.nestedMembersLocked()
}

// nestedMembersLocked implements NestedMembers with nestedPatchMu held.
// Callers must hold nestedPatchMu; EnsureNestedPatched relies on this
// to keep reads of Members race free while patching. It resolves through
// the registry directly (not the cloning Lookup) so lazy patching stores
// the shared table and pointer identity is preserved for patch callers.
func (m *MessageMember) nestedMembersLocked() (*MessageMembers, error) {
	if !m.IsNested() {
		return nil, fmt.Errorf("types: member %q is not nested", m.Name)
	}
	if m.Members != nil {
		return m.Members, nil
	}
	if m.NestedNamespace == "" || m.NestedName == "" {
		return nil, fmt.Errorf("types: member %q has no nested reference", m.Name)
	}
	introspectionMu.RLock()
	resolved, ok := messageRegistry[registryKey(m.NestedNamespace, m.NestedName)]
	introspectionMu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("types: resolve nested %s/%s: message %s/%s not registered", m.NestedNamespace, m.NestedName, m.NestedNamespace, m.NestedName)
	}
	m.Members = resolved
	return m.Members, nil
}

// Size calls the size function for array members.
func (m *MessageMember) Size(base any) (uint, error) {
	if m.SizeFunc == nil {
		return 0, fmt.Errorf("types: member %q has no size function", m.Name)
	}
	n, err := m.SizeFunc(base)
	if err != nil {
		return 0, fmt.Errorf("types: member %q size: %w", m.Name, err)
	}
	return n, nil
}

// GetConst calls the const accessor for array members.
func (m *MessageMember) GetConst(base any, index uint) (any, error) {
	if m.GetConstFunc == nil {
		return nil, fmt.Errorf("types: member %q has no get const function", m.Name)
	}
	v, err := m.GetConstFunc(base, index)
	if err != nil {
		return nil, fmt.Errorf("types: member %q get const %d: %w", m.Name, index, err)
	}
	return v, nil
}

// GetCopy returns a copy of the element at index.
// It is an explicit alias for GetConst that names the copy semantics.
// Kept by design alongside GetConst/Get: the three accessors all return
// copies (Go has no C style const pointer), and the alias preserves
// call-site intent without changing behavior.
func (m *MessageMember) GetCopy(base any, index uint) (any, error) {
	v, err := m.GetConst(base, index)
	if err != nil {
		return nil, fmt.Errorf("types: member %q get copy %d: %w", m.Name, index, err)
	}
	return v, nil
}

// Get calls the accessor for array members.
// The returned value is a copy; use Assign to mutate in place. Kept by
// design alongside GetConst/GetCopy: all three return copies because the Go
// port exposes no mutable C style pointer, and the names preserve call-site
// intent without changing behavior.
func (m *MessageMember) Get(base any, index uint) (any, error) {
	if m.GetFunc == nil {
		return nil, fmt.Errorf("types: member %q has no get function", m.Name)
	}
	v, err := m.GetFunc(base, index)
	if err != nil {
		return nil, fmt.Errorf("types: member %q get %d: %w", m.Name, index, err)
	}
	return v, nil
}

// Fetch copies one array element into out.
func (m *MessageMember) Fetch(base any, index uint, out any) error {
	if m.FetchFunc == nil {
		return fmt.Errorf("types: member %q has no fetch function", m.Name)
	}
	err := m.FetchFunc(base, index, out)
	if err != nil {
		return fmt.Errorf("types: member %q fetch %d: %w", m.Name, index, err)
	}
	return nil
}

// Assign copies val into one array element.
func (m *MessageMember) Assign(base any, index uint, val any) error {
	if m.AssignFunc == nil {
		return fmt.Errorf("types: member %q has no assign function", m.Name)
	}
	err := m.AssignFunc(base, index, val)
	if err != nil {
		return fmt.Errorf("types: member %q assign %d: %w", m.Name, index, err)
	}
	return nil
}

// Resize resizes a sequence member with fini plus init semantics.
func (m *MessageMember) Resize(base any, size uint) error {
	if m.ResizeFunc == nil {
		return fmt.Errorf("types: member %q has no resize function", m.Name)
	}
	err := m.ResizeFunc(base, size)
	if err != nil {
		return fmt.Errorf("types: member %q resize %d: %w", m.Name, size, err)
	}
	return nil
}

// Validate checks names, types, bounds, keys, buffers, and function wiring.
func (m *MessageMember) Validate() error {
	if err := m.validateIdentity(); err != nil {
		return err
	}
	if err := m.validateBufferKind(); err != nil {
		return err
	}
	if err := m.validateKeyRule(); err != nil {
		return err
	}
	if err := m.validateNestedReference(); err != nil {
		return err
	}
	return m.validateArrayWiring()
}

// validateIdentity checks the member name, default, type id, and string bound.
func (m *MessageMember) validateIdentity() error {
	if m.Name == "" {
		return errors.New("types: member has empty name") //nolint:wrapcheck // new error, nothing to wrap
	}
	if m.DefaultValue != nil {
		return fmt.Errorf("types: member %q default value must be nil", m.Name)
	}
	if m.TypeID == RosTypeID(0) {
		return fmt.Errorf("types: member %q has unset type id", m.Name)
	}
	if m.TypeID != RosTypeString && m.TypeID != RosTypeWString && m.StringUpperBound != 0 {
		return fmt.Errorf("types: member %q string upper bound on non-string", m.Name)
	}
	return nil
}

// validateBufferKind checks that rosidl buffers are uint8 or octet sequences.
func (m *MessageMember) validateBufferKind() error {
	if !m.IsRosidlBuffer {
		return nil
	}
	if !m.IsSequence() {
		return fmt.Errorf("types: member %q buffer must be a sequence", m.Name)
	}
	if m.TypeID != RosTypeUint8 && m.TypeID != RosTypeOctet && m.TypeID != RosTypeByte {
		return fmt.Errorf("types: member %q buffer must be uint8 or octet", m.Name)
	}
	return nil
}

// validateKeyRule rejects sequences used as keys.
func (m *MessageMember) validateKeyRule() error {
	if m.IsKey && m.IsSequence() {
		return fmt.Errorf("types: member %q key must not be a sequence", m.Name)
	}
	return nil
}

// validateNestedReference dispatches nested checks by the member type id.
func (m *MessageMember) validateNestedReference() error {
	if m.TypeID == RosTypeMessage {
		return m.validateNestedTarget()
	}
	return m.validateNonNestedTarget()
}

// validateNestedTarget requires a table or a reference for nested members.
func (m *MessageMember) validateNestedTarget() error {
	if m.Members == nil && (m.NestedNamespace == "" || m.NestedName == "") {
		return fmt.Errorf("types: member %q nested needs members or namespace plus name", m.Name)
	}
	return nil
}

// validateNonNestedTarget rejects nested tables and references on plain members.
func (m *MessageMember) validateNonNestedTarget() error {
	if m.Members != nil {
		return fmt.Errorf("types: member %q non-message carries nested table", m.Name)
	}
	if m.NestedNamespace != "" || m.NestedName != "" {
		return fmt.Errorf("types: member %q non-message carries nested reference", m.Name)
	}
	return nil
}

// validateArrayWiring dispatches array checks by the array and bound flags.
func (m *MessageMember) validateArrayWiring() error {
	if !m.IsArray {
		return m.validateScalarWiring()
	}
	if m.FieldName == "" {
		return fmt.Errorf("types: member %q array misses field wiring", m.Name)
	}
	if m.IsUpperBound {
		return m.validateSequenceWiring()
	}
	return m.validateFixedArrayWiring()
}

// validateScalarWiring rejects array state carried on scalar members.
func (m *MessageMember) validateScalarWiring() error {
	if m.ResizeFunc != nil {
		return fmt.Errorf("types: member %q is not a sequence but has resize", m.Name)
	}
	if m.ArraySize != 0 || m.IsUpperBound {
		return fmt.Errorf("types: member %q is not an array but carries array size", m.Name)
	}
	if m.SizeFunc != nil || m.GetConstFunc != nil || m.GetFunc != nil || m.FetchFunc != nil || m.AssignFunc != nil {
		return fmt.Errorf("types: member %q is scalar but carries array functions", m.Name)
	}
	return nil
}

// validateSequenceWiring requires resize plus accessors for sequences.
func (m *MessageMember) validateSequenceWiring() error {
	if m.ResizeFunc == nil {
		return fmt.Errorf("types: member %q is a sequence but resize is nil", m.Name)
	}
	if m.SizeFunc == nil || m.GetConstFunc == nil || m.GetFunc == nil || m.FetchFunc == nil || m.AssignFunc == nil {
		return fmt.Errorf("types: member %q is an array but misses array functions", m.Name)
	}
	return nil
}

// validateFixedArrayWiring requires size plus accessors without resize.
func (m *MessageMember) validateFixedArrayWiring() error {
	if m.ResizeFunc != nil {
		return fmt.Errorf("types: member %q is a fixed array but resize is not nil", m.Name)
	}
	if m.ArraySize == 0 {
		return fmt.Errorf("types: member %q is a fixed array with zero size", m.Name)
	}
	if m.SizeFunc == nil || m.GetConstFunc == nil || m.GetFunc == nil || m.FetchFunc == nil || m.AssignFunc == nil {
		return fmt.Errorf("types: member %q is an array but misses array functions", m.Name)
	}
	return nil
}

// MessageMembers describes all fields of one interface type.
type MessageMembers struct {
	Namespace   string
	Name        string
	MemberCount uint32
	SizeOf      uint
	HasAnyKey   bool
	Members     []MessageMember
	InitFunc    MessageInitFunc
	FiniFunc    MessageFiniFunc
}

// MemberByName returns the member with the given field name.
func (m *MessageMembers) MemberByName(name string) (*MessageMember, error) {
	for i := range m.Members {
		if m.Members[i].Name == name {
			return &m.Members[i], nil
		}
	}
	return nil, fmt.Errorf("types: member %q not found in %s/%s", name, m.Namespace, m.Name)
}

// Init runs the init function for the message instance.
func (m *MessageMembers) Init(base any, init MessageInit) error {
	err := init.Validate()
	if err != nil {
		return fmt.Errorf("types: init %s/%s: %w", m.Namespace, m.Name, err)
	}
	if m.InitFunc == nil {
		return fmt.Errorf("types: message %s/%s has no init function", m.Namespace, m.Name)
	}
	if base == nil {
		return fmt.Errorf("types: init %s/%s nil base", m.Namespace, m.Name)
	}
	err = m.InitFunc(base, init)
	if err != nil {
		return fmt.Errorf("types: init %s/%s mode %s: %w", m.Namespace, m.Name, init.String(), err)
	}
	return nil
}

// Fini runs the fini function for the message instance.
func (m *MessageMembers) Fini(base any) error {
	if m.FiniFunc == nil {
		return fmt.Errorf("types: message %s/%s has no fini function", m.Namespace, m.Name)
	}
	if base == nil {
		return fmt.Errorf("types: fini %s/%s nil base", m.Namespace, m.Name)
	}
	err := m.FiniFunc(base)
	if err != nil {
		return fmt.Errorf("types: fini %s/%s: %w", m.Namespace, m.Name, err)
	}
	return nil
}

// EnsureNestedPatched patches every nested member pointer lazily.
// It recurses into nested tables with visited and in-stack guards.
// The visited set holds only clean tables patched without error;
// the in-stack set detects cycles. Reads and writes of member tables
// hold nestedPatchMu for the whole walk so concurrent patching is safe.
func (m *MessageMembers) EnsureNestedPatched() error {
	nestedPatchMu.Lock()
	defer nestedPatchMu.Unlock()
	visited := map[string]bool{}
	inStack := map[string]bool{}
	return m.ensureNestedPatchedVisitedLocked(visited, inStack)
}

// ensureNestedPatchedVisitedLocked implements recursion with visited and stack guards.
// Callers must hold nestedPatchMu. A table is marked visited only after
// its subtree patches cleanly; cycles in the stack report an error.
func (m *MessageMembers) ensureNestedPatchedVisitedLocked(visited, inStack map[string]bool) error {
	key := registryKey(m.Namespace, m.Name)
	if inStack[key] {
		return fmt.Errorf("types: cycle detected at %s", key)
	}
	if visited[key] {
		return nil
	}
	inStack[key] = true
	defer delete(inStack, key)
	for i := range m.Members {
		if !m.Members[i].IsNested() {
			continue
		}
		if m.Members[i].Members == nil {
			_, err := m.Members[i].nestedMembersLocked()
			if err != nil {
				return fmt.Errorf("types: patch nested %s/%s.%s: %w", m.Namespace, m.Name, m.Members[i].Name, err)
			}
		}
		nested := m.Members[i].Members
		if nested == nil {
			continue
		}
		err := nested.ensureNestedPatchedVisitedLocked(visited, inStack)
		if err != nil {
			return fmt.Errorf("types: patch nested %s/%s.%s: %w", m.Namespace, m.Name, m.Members[i].Name, err)
		}
	}
	visited[key] = true
	return nil
}

// Validate checks counts, key presence, offsets, and member rules.
func (m *MessageMembers) Validate() error {
	if m.Namespace == "" || m.Name == "" {
		return errors.New("types: message has empty namespace or name") //nolint:wrapcheck // new error, nothing to wrap
	}
	if uint32(len(m.Members)) != m.MemberCount { //nolint:gosec // G115: member count is non-negative and checked against MemberCount
		return fmt.Errorf("types: message %s/%s member count %d != %d", m.Namespace, m.Name, m.MemberCount, len(m.Members))
	}
	if m.SizeOf == 0 {
		return fmt.Errorf("types: message %s/%s has zero size", m.Namespace, m.Name)
	}
	if err := m.validateMembers(); err != nil {
		return err
	}
	if err := m.validateOffsets(); err != nil {
		return err
	}
	if err := m.validateKeys(); err != nil {
		return err
	}
	if m.InitFunc == nil || m.FiniFunc == nil {
		return fmt.Errorf("types: message %s/%s misses init or fini", m.Namespace, m.Name)
	}
	return nil
}

// validateMembers validates every member rule in order.
func (m *MessageMembers) validateMembers() error {
	for i := range m.Members {
		err := m.Members[i].Validate()
		if err != nil {
			return fmt.Errorf("types: validate %s/%s: %w", m.Namespace, m.Name, err)
		}
	}
	return nil
}

// validateOffsets requires strictly increasing member offsets.
func (m *MessageMembers) validateOffsets() error {
	for i := range m.Members {
		if i == 0 {
			continue
		}
		if m.Members[i].Offset <= m.Members[i-1].Offset {
			return fmt.Errorf("types: message %s/%s offsets not strictly increasing", m.Namespace, m.Name)
		}
	}
	return nil
}

// validateKeys requires key presence to match the header flag.
func (m *MessageMembers) validateKeys() error {
	seenKey := false
	for i := range m.Members {
		if m.Members[i].IsKey {
			seenKey = true
			break
		}
	}
	if m.HasAnyKey != seenKey {
		return fmt.Errorf("types: message %s/%s key presence mismatch", m.Namespace, m.Name)
	}
	return nil
}

// ServiceMembers carries request, response, and event tables for a service.
type ServiceMembers struct {
	Namespace string
	Name      string
	Request   *MessageMembers
	Response  *MessageMembers
	Event     *MessageMembers
}

// EnsurePatched patches nested pointers in all three tables.
func (s *ServiceMembers) EnsurePatched() error {
	for _, table := range []*MessageMembers{s.Request, s.Response, s.Event} {
		if table == nil {
			return fmt.Errorf("types: service %s/%s has nil table", s.Namespace, s.Name)
		}
		err := table.EnsureNestedPatched()
		if err != nil {
			return fmt.Errorf("types: patch service %s/%s: %w", s.Namespace, s.Name, err)
		}
	}
	return nil
}

// Validate checks that all three tables are present and valid.
func (s *ServiceMembers) Validate() error {
	if s.Namespace == "" || s.Name == "" {
		return errors.New("types: service has empty namespace or name") //nolint:wrapcheck // new error, nothing to wrap
	}
	if s.Request == nil || s.Response == nil || s.Event == nil {
		return fmt.Errorf("types: service %s/%s misses request, response, or event", s.Namespace, s.Name)
	}
	for _, table := range []*MessageMembers{s.Request, s.Response, s.Event} {
		err := table.Validate()
		if err != nil {
			return fmt.Errorf("types: validate service %s/%s: %w", s.Namespace, s.Name, err)
		}
	}
	return nil
}

var (
	introspectionMu sync.RWMutex
	nestedPatchMu   sync.Mutex
	messageRegistry = map[string]*MessageMembers{}
)

// registryKey joins namespace and name for the lookup map.
func registryKey(namespace, name string) string {
	return namespace + "/" + name
}

// RegisterMessageMembers records a message table for lazy nested patching.
// Re-registering the same pointer is idempotent. Registering a different
// table under an already used namespace plus name fails instead of
// overwriting, so conflicting codegen tables surface loudly.
func RegisterMessageMembers(m *MessageMembers) error {
	if m == nil {
		return errors.New("types: cannot register nil message members") //nolint:wrapcheck // new error, nothing to wrap
	}
	if m.Namespace == "" || m.Name == "" {
		return errors.New("types: cannot register message with empty namespace or name") //nolint:wrapcheck // new error, nothing to wrap
	}
	introspectionMu.Lock()
	defer introspectionMu.Unlock()
	key := registryKey(m.Namespace, m.Name)
	if existing, ok := messageRegistry[key]; ok && existing != m {
		return fmt.Errorf("types: message %s already registered", key)
	}
	messageRegistry[key] = m
	return nil
}

// LookupMessageMembers returns a registered message table by namespace and name.
//
// The returned table is a clone like Registry.LookupType: the struct and
// its Members slice are copied so callers cannot mutate the registry
// table without holding a lock. Nested table pointers remain shared;
// callers are read-only and must not mutate through them. Lazy patching
// uses the direct registry pointer internally (see nestedMembersLocked)
// so patch identity is preserved while direct Lookup callers stay
// isolated.
func LookupMessageMembers(namespace, name string) (*MessageMembers, error) {
	introspectionMu.RLock()
	defer introspectionMu.RUnlock()
	m, ok := messageRegistry[registryKey(namespace, name)]
	if !ok {
		return nil, fmt.Errorf("types: message %s/%s not registered", namespace, name)
	}
	return cloneMessageMembers(m), nil
}

// cloneMessageMembers copies the table struct plus its Members slice.
// Member funcs and nested table pointers are shared; the copy isolates
// top-level mutation (for example setting Members on the clone) from the
// registry entry.
func cloneMessageMembers(m *MessageMembers) *MessageMembers {
	if m == nil {
		return nil
	}
	out := *m
	out.Members = append([]MessageMember(nil), m.Members...)
	return &out
}

// ValueKind names the DDS value type family derived from introspection.
type ValueKind int

// Value type families used for sertype construction.
const (
	ValuePrimitive ValueKind = iota
	ValueBoundedString
	ValueStringSequence
	ValueSpanSequence
	ValueBoolVector
)

// String returns the symbolic name of the value kind.
func (k ValueKind) String() string {
	switch k {
	case ValuePrimitive:
		return "primitive"
	case ValueBoundedString:
		return "bounded_string"
	case ValueStringSequence:
		return "string_sequence"
	case ValueSpanSequence:
		return "span_sequence"
	case ValueBoolVector:
		return "bool_vector"
	default:
		return "unknown"
	}
}

// ValueKindFor maps one member to its DDS value type family.
// Scalar strings map to bounded strings. Any array of strings, fixed
// or sequence, maps to string sequence. Boolean sequences map to bool
// vectors. Other arrays map to span sequences. Scalars map to primitives.
func ValueKindFor(m MessageMember) ValueKind {
	if m.IsBoolVector() {
		return ValueBoolVector
	}
	if m.TypeID == RosTypeString || m.TypeID == RosTypeWString {
		if m.IsArray {
			return ValueStringSequence
		}
		return ValueBoundedString
	}
	if m.IsArray {
		return ValueSpanSequence
	}
	return ValuePrimitive
}

// IsSelfContained reports false when any string, sequence, or bool vector is present.
// It returns an error for missing nested tables, failed lookups, and cycles.
// The walk holds nestedPatchMu for its duration to match the
// EnsureNestedPatched write path, so concurrent patching cannot race the
// reads of Members.
func IsSelfContained(m *MessageMembers) (bool, error) {
	if m == nil {
		return false, errors.New("types: nil message table") //nolint:wrapcheck // new error, nothing to wrap
	}
	nestedPatchMu.Lock()
	defer nestedPatchMu.Unlock()
	visited := map[string]bool{}
	inStack := map[string]bool{}
	ok, err := selfContainedInner(m, visited, inStack)
	if err != nil {
		return false, err
	}
	return ok, nil
}

// selfContainedInner walks nested tables with visited and stack guards.
// The visited set holds only tables proven self contained; the in-stack
// set detects cycles on the current path. A scalar nested member with
// no table is malformed and reports an error, while a nested sequence
// with no table is conservatively treated as not self contained because
// sequences already force a false result and the missing table adds no
// new diagnostic signal.
func selfContainedInner(m *MessageMembers, visited, inStack map[string]bool) (bool, error) {
	key := registryKey(m.Namespace, m.Name)
	if inStack[key] {
		return false, fmt.Errorf("types: cycle detected at %s", key)
	}
	if visited[key] {
		return true, nil
	}
	inStack[key] = true
	defer delete(inStack, key)
	for i := range m.Members {
		ok, err := selfContainedMember(&m.Members[i], visited, inStack)
		if err != nil {
			return false, err
		}
		if !ok {
			return false, nil
		}
	}
	visited[key] = true
	return true, nil
}

// selfContainedMember reports whether one member keeps the message self contained.
func selfContainedMember(member *MessageMember, visited, inStack map[string]bool) (bool, error) {
	switch ValueKindFor(*member) {
	case ValueBoundedString, ValueStringSequence, ValueSpanSequence, ValueBoolVector:
		return false, nil
	case ValuePrimitive:
	default:
	}
	if !member.IsNested() {
		return true, nil
	}
	return selfContainedNested(member, visited, inStack)
}

// selfContainedNested resolves and checks one nested member table.
func selfContainedNested(member *MessageMember, visited, inStack map[string]bool) (bool, error) {
	nested := member.Members
	if nested == nil && member.NestedNamespace != "" {
		resolved, err := LookupMessageMembers(member.NestedNamespace, member.NestedName)
		if err != nil {
			return false, fmt.Errorf("types: self contained lookup %s: %w", member.Name, err)
		}
		nested = resolved
	}
	if nested == nil {
		if member.IsArray {
			return false, nil
		}
		return false, fmt.Errorf("types: nested member %q has no table", member.Name)
	}
	ok, err := selfContainedInner(nested, visited, inStack)
	if err != nil {
		return false, err
	}
	return ok, nil
}

// CanLoan reports whether loaning is allowed for the message type.
// It returns an error for missing tables and cycles.
func CanLoan(m *MessageMembers) (bool, error) {
	ok, err := IsSelfContained(m)
	if err != nil {
		return false, err
	}
	return ok, nil
}

// IsMemcpySafe reports whether trivial memcpy is safe for the message type.
// It returns an error for missing tables and cycles.
func IsMemcpySafe(m *MessageMembers) (bool, error) {
	ok, err := IsSelfContained(m)
	if err != nil {
		return false, err
	}
	return ok, nil
}

// structField returns the reflect value of a named struct field.
func structField(base any, field string) (reflect.Value, error) {
	v := reflect.ValueOf(base)
	if v.Kind() != reflect.Pointer || v.IsNil() {
		return reflect.Value{}, errors.New("types: base must be a non nil pointer") //nolint:wrapcheck // new error, nothing to wrap
	}
	elem := v.Elem()
	if elem.Kind() != reflect.Struct {
		return reflect.Value{}, errors.New("types: base must point to a struct") //nolint:wrapcheck // new error, nothing to wrap
	}
	f := elem.FieldByName(field)
	if !f.IsValid() {
		return reflect.Value{}, fmt.Errorf("types: field %q not found", field)
	}
	return f, nil
}

// NewScalarMember builds a scalar member with no array functions.
func NewScalarMember(name string, typeID RosTypeID, offset uint32) MessageMember {
	return MessageMember{
		Name:           name,
		TypeID:         typeID,
		Offset:         offset,
		DefaultValue:   nil,
		IsRosidlBuffer: false,
	}
}

// NewStringMember builds a string member with an optional upper bound.
func NewStringMember(name string, typeID RosTypeID, upperBound uint, offset uint32) MessageMember {
	return MessageMember{
		Name:             name,
		TypeID:           typeID,
		StringUpperBound: upperBound,
		Offset:           offset,
		DefaultValue:     nil,
		IsRosidlBuffer:   false,
	}
}

// NewFixedArrayMember builds an inline fixed array with size and element accessors.
// Field must name the Go struct field exactly; Validate rejects empty wiring.
func NewFixedArrayMember(name string, typeID RosTypeID, arraySize uint, offset uint32, field string) MessageMember {
	return MessageMember{
		Name:           name,
		TypeID:         typeID,
		IsArray:        true,
		ArraySize:      arraySize,
		IsUpperBound:   false,
		Offset:         offset,
		DefaultValue:   nil,
		SizeFunc:       arraySizeFunc(field),
		GetConstFunc:   arrayGetConstFunc(field),
		GetFunc:        arrayGetFunc(field),
		FetchFunc:      arrayFetchFunc(field),
		AssignFunc:     arrayAssignFunc(field),
		ResizeFunc:     nil,
		IsRosidlBuffer: false,
		FieldName:      field,
	}
}

// NewSequenceMember builds a bounded or unbounded sequence with fini plus init resize.
// Field must name the Go struct field exactly; Validate rejects empty wiring.
func NewSequenceMember(name string, typeID RosTypeID, maxSize uint, offset uint32, field string) MessageMember {
	return MessageMember{
		Name:           name,
		TypeID:         typeID,
		IsArray:        true,
		ArraySize:      maxSize,
		IsUpperBound:   true,
		Offset:         offset,
		DefaultValue:   nil,
		SizeFunc:       arraySizeFunc(field),
		GetConstFunc:   arrayGetConstFunc(field),
		GetFunc:        arrayGetFunc(field),
		FetchFunc:      arrayFetchFunc(field),
		AssignFunc:     arrayAssignFunc(field),
		ResizeFunc:     sequenceResizeFunc(field, maxSize),
		IsRosidlBuffer: false,
		FieldName:      field,
	}
}

// NewBufferMember builds a uint8 sequence flagged as an rosidl buffer.
func NewBufferMember(name string, maxSize uint, offset uint32, field string) MessageMember {
	m := NewSequenceMember(name, RosTypeUint8, maxSize, offset, field)
	m.IsRosidlBuffer = true
	return m
}

// NewNestedMember builds a nested message member with lazy patching.
func NewNestedMember(name string, offset uint32, field, nestedNamespace, nestedName string) MessageMember {
	return MessageMember{
		Name:            name,
		TypeID:          RosTypeMessage,
		Offset:          offset,
		DefaultValue:    nil,
		Members:         nil,
		FieldName:       field,
		NestedNamespace: nestedNamespace,
		NestedName:      nestedName,
	}
}

// NewNestedSequenceMember builds a sequence of nested messages with lazy patching.
func NewNestedSequenceMember(name string, maxSize uint, offset uint32, field, nestedNamespace, nestedName string) MessageMember {
	return MessageMember{
		Name:            name,
		TypeID:          RosTypeMessage,
		IsArray:         true,
		ArraySize:       maxSize,
		IsUpperBound:    true,
		Offset:          offset,
		DefaultValue:    nil,
		SizeFunc:        arraySizeFunc(field),
		GetConstFunc:    arrayGetConstFunc(field),
		GetFunc:         arrayGetFunc(field),
		FetchFunc:       arrayFetchFunc(field),
		AssignFunc:      arrayAssignFunc(field),
		ResizeFunc:      sequenceResizeFunc(field, maxSize),
		FieldName:       field,
		NestedNamespace: nestedNamespace,
		NestedName:      nestedName,
	}
}

// NewMessageMembers builds a container with member count filled in.
func NewMessageMembers(namespace, name string, sizeOf uint, members []MessageMember, initFn MessageInitFunc, finiFn MessageFiniFunc) *MessageMembers {
	hasKey := false
	for i := range members {
		if members[i].IsKey {
			hasKey = true
			break
		}
	}
	return &MessageMembers{
		Namespace:   namespace,
		Name:        name,
		MemberCount: uint32(len(members)), //nolint:gosec // G115: member count is non-negative and fits uint32
		SizeOf:      sizeOf,
		HasAnyKey:   hasKey,
		Members:     members,
		InitFunc:    initFn,
		FiniFunc:    finiFn,
	}
}

// checkWrapperShape validates a Data plus Size plus Capacity wrapper once.
func checkWrapperShape(parent reflect.Value, field string) error {
	dataField := parent.FieldByName("Data")
	sizeField := parent.FieldByName("Size")
	capField := parent.FieldByName("Capacity")
	if !dataField.IsValid() || !sizeField.IsValid() || !capField.IsValid() {
		return fmt.Errorf("types: field %q wrapper misses Data Size Capacity", field)
	}
	if dataField.Kind() != reflect.Slice && dataField.Kind() != reflect.Array {
		return fmt.Errorf("types: field %q data is not slice or array", field)
	}
	if sizeField.Kind() != reflect.Uint || capField.Kind() != reflect.Uint {
		return fmt.Errorf("types: field %q size and capacity must be uint", field)
	}
	return nil
}

// arrayDataSlice resolves the element slice for array accessors.
// It supports direct slice or array fields and Sequence struct wrappers
// with Data plus Size plus Capacity triples.
func arrayDataSlice(base any, field string) (reflect.Value, reflect.Value, bool, error) {
	f, err := structField(base, field)
	if err != nil {
		return reflect.Value{}, reflect.Value{}, false, fmt.Errorf("types: array field %q: %w", field, err)
	}
	kind := f.Kind()
	if kind == reflect.Slice || kind == reflect.Array {
		return f, reflect.Value{}, false, nil
	}
	if kind == reflect.Struct {
		data := f.FieldByName("Data")
		if !data.IsValid() {
			return reflect.Value{}, reflect.Value{}, false, fmt.Errorf("types: field %q is not an array", field)
		}
		if data.Kind() != reflect.Slice && data.Kind() != reflect.Array {
			return reflect.Value{}, reflect.Value{}, false, fmt.Errorf("types: field %q is not an array", field)
		}
		err = checkWrapperShape(f, field)
		if err != nil {
			return reflect.Value{}, reflect.Value{}, false, err
		}
		return data, f, true, nil
	}
	return reflect.Value{}, reflect.Value{}, false, fmt.Errorf("types: field %q is not an array", field)
}

// syncSequenceTriple refreshes Size and Capacity from the Data slice.
// It returns an error for malformed wrappers instead of a silent no-op.
func syncSequenceTriple(parent reflect.Value) error {
	dataField := parent.FieldByName("Data")
	sizeField := parent.FieldByName("Size")
	capField := parent.FieldByName("Capacity")
	if !dataField.IsValid() || !sizeField.IsValid() || !capField.IsValid() {
		return errors.New("types: wrapper misses Data Size Capacity") //nolint:wrapcheck // new error, nothing to wrap
	}
	if sizeField.Kind() != reflect.Uint || capField.Kind() != reflect.Uint {
		return errors.New("types: wrapper size and capacity must be uint") //nolint:wrapcheck // new error, nothing to wrap
	}
	if !sizeField.CanSet() || !capField.CanSet() {
		return errors.New("types: wrapper size and capacity cannot be set") //nolint:wrapcheck // new error, nothing to wrap
	}
	sizeField.SetUint(uint64(dataField.Len())) //nolint:gosec // G115: slice length is non-negative
	capField.SetUint(uint64(dataField.Cap()))  //nolint:gosec // G115: slice capacity is non-negative
	return nil
}

// getElem resolves one array element for the get accessors.
// It shares the bounds check between arrayGetConstFunc and arrayGetFunc
// so the two paths cannot drift apart.
func getElem(base any, field string, index uint, what string) (any, error) {
	f, _, _, err := arrayDataSlice(base, field)
	if err != nil {
		return nil, fmt.Errorf("types: %s field %q: %w", what, field, err)
	}
	if int(index) >= f.Len() {
		return nil, fmt.Errorf("types: index %d out of range %d", index, f.Len())
	}
	return f.Index(int(index)).Interface(), nil
}

// arraySizeFunc returns a SizeFunc reading slice or array length by field name.
func arraySizeFunc(field string) SizeFunc {
	return func(base any) (uint, error) {
		f, _, _, err := arrayDataSlice(base, field)
		if err != nil {
			return 0, fmt.Errorf("types: size field %q: %w", field, err)
		}
		return uint(f.Len()), nil
	}
}

// arrayGetConstFunc returns a GetConstFunc copying the indexed element.
func arrayGetConstFunc(field string) GetConstFunc {
	return func(base any, index uint) (any, error) {
		v, err := getElem(base, field, index, "get const")
		if err != nil {
			return nil, err
		}
		return v, nil
	}
}

// arrayGetFunc returns a GetFunc exposing a copy of the indexed element.
func arrayGetFunc(field string) GetFunc {
	return func(base any, index uint) (any, error) {
		v, err := getElem(base, field, index, "get")
		if err != nil {
			return nil, err
		}
		return v, nil
	}
}

// arrayFetchFunc returns a FetchFunc copying one element into out.
func arrayFetchFunc(field string) FetchFunc {
	return func(base any, index uint, out any) error {
		f, _, _, err := arrayDataSlice(base, field)
		if err != nil {
			return fmt.Errorf("types: fetch field %q: %w", field, err)
		}
		if int(index) >= f.Len() {
			return fmt.Errorf("types: index %d out of range %d", index, f.Len())
		}
		ov := reflect.ValueOf(out)
		if ov.Kind() != reflect.Pointer || ov.IsNil() {
			return errors.New("types: fetch out must be a non nil pointer")
		}
		src := f.Index(int(index))
		dst := ov.Elem()
		if !src.Type().AssignableTo(dst.Type()) {
			return fmt.Errorf("types: cannot fetch %s into %s", src.Type().String(), dst.Type().String())
		}
		dst.Set(src)
		return nil
	}
}

// arrayAssignFunc returns an AssignFunc copying val into one element.
// Only AssignableTo values are accepted; implicit numeric conversion is
// rejected so callers convert explicitly and truncation stays visible.
func arrayAssignFunc(field string) AssignFunc {
	return func(base any, index uint, val any) error {
		f, _, _, err := arrayDataSlice(base, field)
		if err != nil {
			return fmt.Errorf("types: assign field %q: %w", field, err)
		}
		if int(index) >= f.Len() {
			return fmt.Errorf("types: index %d out of range %d", index, f.Len())
		}
		vv := reflect.ValueOf(val)
		dst := f.Index(int(index))
		if !vv.IsValid() {
			return errors.New("types: assign value is invalid")
		}
		if !vv.Type().AssignableTo(dst.Type()) {
			return fmt.Errorf("types: cannot assign %s to %s", vv.Type().String(), dst.Type().String())
		}
		dst.Set(vv)
		return nil
	}
}

// sequenceResizeFunc returns a ResizeFunc with shallow preserve semantics.
// It copies the assignable prefix and zeroes the remainder without running
// nested fini for the dropped tail or init for the grown head. Callers that
// store nested resources must finalize the tail through the table Fini path.
// Growth uses reflect.MakeSlice with len equal cap, so spare capacity is
// dropped and the new cap equals the requested size.
func sequenceResizeFunc(field string, maxSize uint) ResizeFunc {
	return func(base any, size uint) error {
		if err := checkResizeBound(size, maxSize); err != nil {
			return err
		}
		data, parent, isWrapper, err := arrayDataSlice(base, field)
		if err != nil {
			return fmt.Errorf("types: resize field %q: %w", field, err)
		}
		if err := checkResizeKind(data, field); err != nil {
			return err
		}
		next := buildResizedSlice(data, size)
		if isWrapper {
			return storeWrapperSlice(parent, field, next)
		}
		return storeDirectSlice(base, field, next)
	}
}

// checkResizeBound rejects sizes above the sequence max.
// Unbounded sequences with maxSize zero are still capped by
// maxSequenceElements before reflect allocation so a huge size returns an
// error instead of panicking or OOM on MakeSlice.
func checkResizeBound(size, maxSize uint) error {
	if maxSize != 0 && size > maxSize {
		return fmt.Errorf("types: resize %d above max %d", size, maxSize)
	}
	if uint64(size) > uint64(maxSequenceElements) {
		return fmt.Errorf("types: resize %d exceeds cap %d", size, maxSequenceElements)
	}
	return nil
}

// checkResizeKind rejects fixed arrays and non slice storage.
func checkResizeKind(data reflect.Value, field string) error {
	if data.Kind() == reflect.Array {
		return fmt.Errorf("types: field %q is a fixed array", field)
	}
	if data.Kind() != reflect.Slice {
		return fmt.Errorf("types: field %q is not a sequence slice", field)
	}
	return nil
}

// buildResizedSlice copies the assignable prefix and zeroes the remainder.
func buildResizedSlice(data reflect.Value, size uint) reflect.Value {
	elem := data.Type().Elem()
	next := reflect.MakeSlice(data.Type(), int(size), int(size))
	limit := min(data.Len(), int(size))
	for i := range limit {
		src := data.Index(i)
		dst := next.Index(i)
		if src.Type().AssignableTo(dst.Type()) {
			dst.Set(src)
		} else {
			dst.Set(reflect.Zero(elem))
		}
	}
	return next
}

// storeWrapperSlice stores the slice in a Data plus Size plus Capacity wrapper.
func storeWrapperSlice(parent reflect.Value, field string, next reflect.Value) error {
	dataField := parent.FieldByName("Data")
	if !dataField.IsValid() || !dataField.CanSet() {
		return fmt.Errorf("types: field %q data cannot be set", field)
	}
	dataField.Set(next)
	err := syncSequenceTriple(parent)
	if err != nil {
		return fmt.Errorf("types: resize sync %q: %w", field, err)
	}
	return nil
}

// storeDirectSlice stores the slice in a plain slice struct field.
func storeDirectSlice(base any, field string, next reflect.Value) error {
	f, err := structField(base, field)
	if err != nil {
		return fmt.Errorf("types: resize field %q: %w", field, err)
	}
	if !f.CanSet() {
		return fmt.Errorf("types: field %q cannot be set", field)
	}
	f.Set(next)
	return nil
}
