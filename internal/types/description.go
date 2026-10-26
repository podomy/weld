// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

// Shared type description structs for the schema registry.
package types

import (
	"errors"
	"fmt"
	"sort"
)

// MaxTypeNameLength caps every type name at 255 bytes.
//
// The cap measures bytes with len, not runes. Names must be ASCII, so bytes
// and runes agree for valid names. Slash separated package plus kind plus
// base name such as std_msgs/msg/String is conventional but not enforced.
const MaxTypeNameLength = 255

// FieldTypeID is the numeric ROS IDL field type identifier.
type FieldTypeID uint8

// Base field type identifiers.
const (
	FieldTypeNotSet         FieldTypeID = 0
	FieldTypeNestedType     FieldTypeID = 1
	FieldTypeInt8           FieldTypeID = 2
	FieldTypeUInt8          FieldTypeID = 3
	FieldTypeInt16          FieldTypeID = 4
	FieldTypeUInt16         FieldTypeID = 5
	FieldTypeInt32          FieldTypeID = 6
	FieldTypeUInt32         FieldTypeID = 7
	FieldTypeInt64          FieldTypeID = 8
	FieldTypeUInt64         FieldTypeID = 9
	FieldTypeFloat          FieldTypeID = 10
	FieldTypeDouble         FieldTypeID = 11
	FieldTypeLongDouble     FieldTypeID = 12
	FieldTypeChar           FieldTypeID = 13
	FieldTypeWChar          FieldTypeID = 14
	FieldTypeBoolean        FieldTypeID = 15
	FieldTypeByte           FieldTypeID = 16
	FieldTypeString         FieldTypeID = 17
	FieldTypeWString        FieldTypeID = 18
	FieldTypeFixedString    FieldTypeID = 19
	FieldTypeFixedWString   FieldTypeID = 20
	FieldTypeBoundedString  FieldTypeID = 21
	FieldTypeBoundedWString FieldTypeID = 22
)

// Array and sequence offsets applied to base identifiers.
const (
	FieldTypeArrayOffset             FieldTypeID = 48
	FieldTypeBoundedSequenceOffset   FieldTypeID = 96
	FieldTypeUnboundedSequenceOffset FieldTypeID = 144
)

// ArrayOf returns the fixed array identifier for a base type.
//
// Base must be FieldTypeNestedType to FieldTypeBoundedWString. It reports
// false for 0 and other out of range bases, fixing the old ArrayOf(0)
// equals 48 gap value.
func ArrayOf(base FieldTypeID) (FieldTypeID, bool) {
	if base < FieldTypeNestedType || base > FieldTypeBoundedWString {
		return 0, false
	}
	return base + FieldTypeArrayOffset, true
}

// BoundedSequenceOf returns the bounded sequence identifier for a base type.
//
// Base must be FieldTypeNestedType to FieldTypeBoundedWString. It reports
// false for out of range bases.
func BoundedSequenceOf(base FieldTypeID) (FieldTypeID, bool) {
	if base < FieldTypeNestedType || base > FieldTypeBoundedWString {
		return 0, false
	}
	return base + FieldTypeBoundedSequenceOffset, true
}

// UnboundedSequenceOf returns the unbounded sequence identifier for a base type.
//
// Base must be FieldTypeNestedType to FieldTypeBoundedWString. It reports
// false for out of range bases.
func UnboundedSequenceOf(base FieldTypeID) (FieldTypeID, bool) {
	if base < FieldTypeNestedType || base > FieldTypeBoundedWString {
		return 0, false
	}
	return base + FieldTypeUnboundedSequenceOffset, true
}

// Base strips array and sequence offsets to recover the scalar identifier.
//
// Ranges derive from the offset constants plus FieldTypeNestedType to
// FieldTypeBoundedWString: array 49 to 70, bounded sequence 97 to 118,
// unbounded sequence 145 to 166. Gap values return unchanged so Validate
// can reject them.
func (id FieldTypeID) Base() FieldTypeID {
	if id >= FieldTypeUnboundedSequenceOffset+FieldTypeNestedType && id <= FieldTypeUnboundedSequenceOffset+FieldTypeBoundedWString {
		return id - FieldTypeUnboundedSequenceOffset
	}
	if id >= FieldTypeBoundedSequenceOffset+FieldTypeNestedType && id <= FieldTypeBoundedSequenceOffset+FieldTypeBoundedWString {
		return id - FieldTypeBoundedSequenceOffset
	}
	if id >= FieldTypeArrayOffset+FieldTypeNestedType && id <= FieldTypeArrayOffset+FieldTypeBoundedWString {
		return id - FieldTypeArrayOffset
	}
	return id
}

// IsArray reports whether the identifier names a fixed array kind.
//
// The range derives from FieldTypeArrayOffset plus FieldTypeNestedType to
// FieldTypeBoundedWString.
func (id FieldTypeID) IsArray() bool {
	return id >= FieldTypeArrayOffset+FieldTypeNestedType && id <= FieldTypeArrayOffset+FieldTypeBoundedWString
}

// IsBoundedSequence reports whether the identifier names a bounded sequence kind.
//
// The range derives from FieldTypeBoundedSequenceOffset plus
// FieldTypeNestedType to FieldTypeBoundedWString.
func (id FieldTypeID) IsBoundedSequence() bool {
	return id >= FieldTypeBoundedSequenceOffset+FieldTypeNestedType && id <= FieldTypeBoundedSequenceOffset+FieldTypeBoundedWString
}

// IsUnboundedSequence reports whether the identifier names an unbounded sequence kind.
//
// The range derives from FieldTypeUnboundedSequenceOffset plus
// FieldTypeNestedType to FieldTypeBoundedWString.
func (id FieldTypeID) IsUnboundedSequence() bool {
	return id >= FieldTypeUnboundedSequenceOffset+FieldTypeNestedType && id <= FieldTypeUnboundedSequenceOffset+FieldTypeBoundedWString
}

// IsNested reports whether the base identifier is the nested type kind.
func (id FieldTypeID) IsNested() bool {
	return id.Base() == FieldTypeNestedType
}

// IsString reports whether the base identifier is a string kind.
//
// String kinds are FieldTypeString to FieldTypeBoundedWString, covering
// plain, wide, fixed, and bounded variants. Array and sequence wrappers
// keep the same base, so arrays of strings also report true.
func (id FieldTypeID) IsString() bool {
	base := id.Base()
	return base >= FieldTypeString && base <= FieldTypeBoundedWString
}

// FieldType carries type id plus capacity plus string capacity plus nested name.
type FieldType struct {
	TypeID         FieldTypeID `json:"type_id"`
	Capacity       uint64      `json:"capacity"`
	StringCapacity uint64      `json:"string_capacity"`
	NestedTypeName string      `json:"nested_type_name"`
}

// Field is one named field with a type and an IDL default value.
type Field struct {
	Name         string    `json:"name"`
	Type         FieldType `json:"type"`
	DefaultValue string    `json:"default_value"`
}

// IndividualTypeDescription is one type with its fields in IDL order.
type IndividualTypeDescription struct {
	TypeName string  `json:"type_name"`
	Fields   []Field `json:"fields"`
}

// TypeDescription is one description plus its referenced descriptions.
type TypeDescription struct {
	Description IndividualTypeDescription   `json:"type_description"`
	Referenced  []IndividualTypeDescription `json:"referenced_type_descriptions"`
}

// SourceEncoding names the file kind that produced a type source.
type SourceEncoding string

// Supported source encodings.
const (
	EncodingIDL      SourceEncoding = "idl"
	EncodingMsg      SourceEncoding = "msg"
	EncodingSrv      SourceEncoding = "srv"
	EncodingAction   SourceEncoding = "action"
	EncodingDynamic  SourceEncoding = "dynamic"
	EncodingImplicit SourceEncoding = "implicit"
)

// IsValid reports whether the encoding is one of the supported kinds.
func (e SourceEncoding) IsValid() bool {
	switch e {
	case EncodingIDL, EncodingMsg, EncodingSrv, EncodingAction, EncodingDynamic, EncodingImplicit:
		return true
	default:
		return false
	}
}

// TypeSource is one type name plus encoding plus raw file contents.
type TypeSource struct {
	TypeName        string         `json:"type_name"`
	Encoding        SourceEncoding `json:"encoding"`
	RawFileContents string         `json:"raw_file_contents"`
}

// ValidateTypeName checks that a type name is non empty plus ASCII plus within cap.
//
// The length check uses len, so it measures bytes. Non ASCII bytes are
// rejected, so valid names have equal byte and rune length.
func ValidateTypeName(name string) error {
	if name == "" {
		return errors.New("types: empty type name") //nolint:wrapcheck // new error, nothing to wrap
	}
	if len(name) > MaxTypeNameLength {
		return fmt.Errorf("types: type name exceeds %d bytes: %d", MaxTypeNameLength, len(name))
	}
	for i := range len(name) {
		if name[i] >= 128 {
			return fmt.Errorf("types: type name %q is not ASCII", name)
		}
	}
	return nil
}

// Validate checks field type bounds plus capacity plus string capacity plus nested name rules.
//
// Valid identifiers are FieldTypeNestedType to FieldTypeBoundedWString plus
// array 49 to 70 plus bounded sequence 97 to 118 plus unbounded sequence
// 145 to 166. Zero means not set and is rejected. Gaps 23 to 48, 71 to 96,
// 119 to 144, and 167 to 255 are rejected. Scalar kinds
// FieldTypeNestedType to FieldTypeBoundedWString must carry Capacity 0.
// Array and bounded sequence kinds must carry Capacity greater than 0.
// Unbounded sequence kinds must carry Capacity 0. Only string bases
// FieldTypeString to FieldTypeBoundedWString may carry StringCapacity;
// all other bases must carry StringCapacity 0. Nested kinds with base
// FieldTypeNestedType (identifiers 1, 49, 97, 145) require a nested type
// name; all other kinds forbid one.
func (t FieldType) Validate() error {
	if err := t.validateID(); err != nil {
		return err
	}
	if err := t.validateNestedName(); err != nil {
		return err
	}
	if err := t.validateStringCapacity(); err != nil {
		return err
	}
	return t.validateCapacity()
}

// validateID checks that the type id names a known scalar, array, or sequence kind.
func (t FieldType) validateID() error {
	id := t.TypeID
	valid := (id >= FieldTypeNestedType && id <= FieldTypeBoundedWString) ||
		(id >= FieldTypeArrayOffset+FieldTypeNestedType && id <= FieldTypeArrayOffset+FieldTypeBoundedWString) ||
		(id >= FieldTypeBoundedSequenceOffset+FieldTypeNestedType && id <= FieldTypeBoundedSequenceOffset+FieldTypeBoundedWString) ||
		(id >= FieldTypeUnboundedSequenceOffset+FieldTypeNestedType && id <= FieldTypeUnboundedSequenceOffset+FieldTypeBoundedWString)
	if valid {
		return nil
	}
	if id == FieldTypeNotSet {
		return errors.New("types: field type id 0 is not set") //nolint:wrapcheck // new error, nothing to wrap
	}
	return fmt.Errorf("types: invalid field type id %d", uint8(id))
}

// validateNestedName checks nested name presence against the base kind.
func (t FieldType) validateNestedName() error {
	id := t.TypeID
	if !id.IsNested() {
		return t.validateNonNestedName()
	}
	if t.NestedTypeName == "" {
		return fmt.Errorf("types: nested type id %d misses nested name", uint8(id))
	}
	err := ValidateTypeName(t.NestedTypeName)
	if err != nil {
		return fmt.Errorf("types: nested type name: %w", err)
	}
	return nil
}

// validateNonNestedName rejects nested names on non nested kinds.
func (t FieldType) validateNonNestedName() error {
	if t.NestedTypeName != "" {
		return fmt.Errorf("types: non nested type id %d carries nested name %q", uint8(t.TypeID), t.NestedTypeName)
	}
	return nil
}

// validateStringCapacity rejects string capacity on non string kinds.
func (t FieldType) validateStringCapacity() error {
	if t.TypeID.IsString() || t.StringCapacity == 0 {
		return nil
	}
	return fmt.Errorf("types: non string type id %d carries string capacity %d", uint8(t.TypeID), t.StringCapacity)
}

// validateCapacity checks capacity against array, sequence, and scalar rules.
func (t FieldType) validateCapacity() error {
	id := t.TypeID
	if id.IsArray() || id.IsBoundedSequence() {
		if t.Capacity == 0 {
			return fmt.Errorf("types: array or bounded sequence type id %d misses capacity", uint8(id))
		}
		return nil
	}
	if id.IsUnboundedSequence() {
		if t.Capacity != 0 {
			return fmt.Errorf("types: unbounded sequence type id %d carries capacity %d", uint8(id), t.Capacity)
		}
		return nil
	}
	if t.Capacity != 0 {
		return fmt.Errorf("types: scalar type id %d carries capacity %d", uint8(id), t.Capacity)
	}
	return nil
}

// Validate checks the field name and its nested type.
func (f Field) Validate() error {
	if f.Name == "" {
		return errors.New("types: empty field name") //nolint:wrapcheck // new error, nothing to wrap
	}
	err := f.Type.Validate()
	if err != nil {
		return fmt.Errorf("types: validate field %q: %w", f.Name, err)
	}
	return nil
}

// Validate checks the type name plus every field plus duplicate field names.
func (d IndividualTypeDescription) Validate() error {
	err := ValidateTypeName(d.TypeName)
	if err != nil {
		return fmt.Errorf("types: validate individual %q: %w", d.TypeName, err)
	}
	seen := make(map[string]bool, len(d.Fields))
	for i := range d.Fields {
		err = d.Fields[i].Validate()
		if err != nil {
			return fmt.Errorf("types: validate individual %q field %q: %w", d.TypeName, d.Fields[i].Name, err)
		}
		if seen[d.Fields[i].Name] {
			return fmt.Errorf("types: individual %q duplicate field %q", d.TypeName, d.Fields[i].Name)
		}
		seen[d.Fields[i].Name] = true
	}
	return nil
}

// Validate checks the root plus every referenced plus uniqueness plus ordering.
//
// Referenced names must be unique, must not contain the root, and must be
// sorted by type name for determinism. Use CanonicalJSON or RegisterFull to
// sort a copy; Validate rejects unsorted input.
func (d TypeDescription) Validate() error {
	err := d.Description.Validate()
	if err != nil {
		return fmt.Errorf("types: validate description %q: %w", d.Description.TypeName, err)
	}
	seen := make(map[string]bool, len(d.Referenced)+1)
	seen[d.Description.TypeName] = true
	for i := range d.Referenced {
		err = d.Referenced[i].Validate()
		if err != nil {
			return fmt.Errorf("types: validate description %q referenced %q: %w", d.Description.TypeName, d.Referenced[i].TypeName, err)
		}
		if d.Referenced[i].TypeName == d.Description.TypeName {
			return fmt.Errorf("types: description %q lists root in referenced", d.Description.TypeName)
		}
		if seen[d.Referenced[i].TypeName] {
			return fmt.Errorf("types: description %q duplicate referenced %q", d.Description.TypeName, d.Referenced[i].TypeName)
		}
		seen[d.Referenced[i].TypeName] = true
		if i > 0 && d.Referenced[i-1].TypeName >= d.Referenced[i].TypeName {
			return fmt.Errorf("types: description %q referenced not sorted: %q before %q", d.Description.TypeName, d.Referenced[i-1].TypeName, d.Referenced[i].TypeName)
		}
	}
	return nil
}

// Validate checks the source type name plus encoding plus raw contents.
//
// Empty RawFileContents is allowed only for dynamic and implicit encodings,
// which synthesize sources at runtime. File based encodings idl, msg, srv,
// and action require non empty contents.
func (s TypeSource) Validate() error {
	err := ValidateTypeName(s.TypeName)
	if err != nil {
		return fmt.Errorf("types: validate source %q: %w", s.TypeName, err)
	}
	if !s.Encoding.IsValid() {
		return fmt.Errorf("types: invalid source encoding %q for %q", string(s.Encoding), s.TypeName)
	}
	if s.RawFileContents == "" && s.Encoding != EncodingDynamic && s.Encoding != EncodingImplicit {
		return fmt.Errorf("types: source %q encoding %q misses raw contents", s.TypeName, string(s.Encoding))
	}
	return nil
}

// sortIndividuals orders descriptions by type name for determinism.
//
// Referenced names are unique after Validate, so the stable order is fully
// deterministic and independent of registration or discovery order. Sorting
// callers may run this without holding the registry lock once the slice
// holds clones.
func sortIndividuals(refs []IndividualTypeDescription) {
	sort.SliceStable(refs, func(i, j int) bool {
		return refs[i].TypeName < refs[j].TypeName
	})
}

// cloneIndividual returns a deep copy of one individual description.
//
// Nil Fields normalize to an empty slice so callers never observe a nil
// versus empty distinction. CanonicalJSON encodes both forms identically,
// and the normalized copy keeps registry lookups mutation safe.
func cloneIndividual(d IndividualTypeDescription) IndividualTypeDescription {
	out := IndividualTypeDescription{
		TypeName: d.TypeName,
	}
	if d.Fields != nil {
		out.Fields = make([]Field, len(d.Fields))
		copy(out.Fields, d.Fields)
	} else {
		out.Fields = []Field{}
	}
	return out
}

// equalIndividual reports whether two descriptions carry identical fields.
func equalIndividual(a, b IndividualTypeDescription) bool {
	if a.TypeName != b.TypeName {
		return false
	}
	if len(a.Fields) != len(b.Fields) {
		return false
	}
	for i := range a.Fields {
		if a.Fields[i] != b.Fields[i] {
			return false
		}
	}
	return true
}
