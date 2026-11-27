// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

// Package msg provides the public type registry surface for Weld.
//
// The package re-exports the internal schema types with zero logic
// duplication. All behavior lives in internal/types. This package only
// holds type aliases and thin constructor wrappers so external callers
// never import an internal path.
package msg

import (
	"github.com/podomy/weld/internal/types"
)

// Registry stores individuals plus sources plus full descriptions by name and hash.
type Registry = types.Registry

// IndividualTypeDescription is one type with its fields in IDL order.
type IndividualTypeDescription = types.IndividualTypeDescription

// TypeDescription is one description plus its referenced descriptions.
type TypeDescription = types.TypeDescription

// TypeSource is one type name plus encoding plus raw file contents.
type TypeSource = types.TypeSource

// TypeHash is one version byte plus 32 bytes of SHA-256.
type TypeHash = types.TypeHash

// EquivalenceHash is the wire 14 byte XTypes equivalence hash.
type EquivalenceHash = types.EquivalenceHash

// Equivalencext6aHash is the 14 byte MD5 based type equivalence hash.
type Equivalencext6aHash = types.Equivalencext6aHash

// Field is one named field with a type and an IDL default value.
type Field = types.Field

// FieldType carries type id plus capacity plus string capacity plus nested name.
type FieldType = types.FieldType

// FieldTypeID is the numeric ROS IDL field type identifier.
type FieldTypeID = types.FieldTypeID

// SourceEncoding names the file kind that produced a type source.
type SourceEncoding = types.SourceEncoding

// MessageMembers describes all fields of one interface type.
type MessageMembers = types.MessageMembers

// MessageMember describes one field of an interface type.
type MessageMember = types.MessageMember

// ServiceMembers carries request, response, and event tables for a service.
type ServiceMembers = types.ServiceMembers

// FieldTypeNestedType marks a nested message field.
const FieldTypeNestedType = types.FieldTypeNestedType

// FieldTypeNotSet marks an unset field type identifier.
const FieldTypeNotSet = types.FieldTypeNotSet

// FieldTypeInt8 marks an 8 bit signed integer field.
const FieldTypeInt8 = types.FieldTypeInt8

// FieldTypeUInt8 marks an 8 bit unsigned integer field.
const FieldTypeUInt8 = types.FieldTypeUInt8

// FieldTypeInt16 marks a 16 bit signed integer field.
const FieldTypeInt16 = types.FieldTypeInt16

// FieldTypeUInt16 marks a 16 bit unsigned integer field.
const FieldTypeUInt16 = types.FieldTypeUInt16

// FieldTypeInt32 marks a 32 bit signed integer field.
const FieldTypeInt32 = types.FieldTypeInt32

// FieldTypeUInt32 marks a 32 bit unsigned integer field.
const FieldTypeUInt32 = types.FieldTypeUInt32

// FieldTypeInt64 marks a 64 bit signed integer field.
const FieldTypeInt64 = types.FieldTypeInt64

// FieldTypeUInt64 marks a 64 bit unsigned integer field.
const FieldTypeUInt64 = types.FieldTypeUInt64

// FieldTypeFloat marks a 32 bit float field.
const FieldTypeFloat = types.FieldTypeFloat

// FieldTypeDouble marks a 64 bit float field.
const FieldTypeDouble = types.FieldTypeDouble

// FieldTypeLongDouble marks a 128 bit float field.
const FieldTypeLongDouble = types.FieldTypeLongDouble

// FieldTypeChar marks a char field.
const FieldTypeChar = types.FieldTypeChar

// FieldTypeWChar marks a wide char field.
const FieldTypeWChar = types.FieldTypeWChar

// FieldTypeBoolean marks a boolean field.
const FieldTypeBoolean = types.FieldTypeBoolean

// FieldTypeByte marks a byte field.
const FieldTypeByte = types.FieldTypeByte

// FieldTypeString marks an unbounded string field.
const FieldTypeString = types.FieldTypeString

// FieldTypeWString marks an unbounded wide string field.
const FieldTypeWString = types.FieldTypeWString

// FieldTypeFixedString marks a fixed capacity string field.
const FieldTypeFixedString = types.FieldTypeFixedString

// FieldTypeFixedWString marks a fixed capacity wide string field.
const FieldTypeFixedWString = types.FieldTypeFixedWString

// FieldTypeBoundedString marks a bounded string field.
const FieldTypeBoundedString = types.FieldTypeBoundedString

// FieldTypeBoundedWString marks a bounded wide string field.
const FieldTypeBoundedWString = types.FieldTypeBoundedWString

// FieldTypeArrayOffset is added to a base id for fixed arrays.
const FieldTypeArrayOffset = types.FieldTypeArrayOffset

// FieldTypeBoundedSequenceOffset is added to a base id for bounded sequences.
const FieldTypeBoundedSequenceOffset = types.FieldTypeBoundedSequenceOffset

// FieldTypeUnboundedSequenceOffset is added to a base id for unbounded sequences.
const FieldTypeUnboundedSequenceOffset = types.FieldTypeUnboundedSequenceOffset

// EncodingIDL marks a type source produced from a ROS IDL file.
const EncodingIDL = types.EncodingIDL

// EncodingMsg marks a type source produced from a ROS msg file.
const EncodingMsg = types.EncodingMsg

// EncodingSrv marks a type source produced from a ROS srv file.
const EncodingSrv = types.EncodingSrv

// EncodingAction marks a type source produced from a ROS action file.
const EncodingAction = types.EncodingAction

// EncodingDynamic marks a type source synthesized at runtime.
const EncodingDynamic = types.EncodingDynamic

// EncodingImplicit marks a type source synthesized implicitly.
const EncodingImplicit = types.EncodingImplicit

// ArrayOf returns the fixed array identifier for a base type.
func ArrayOf(base FieldTypeID) (FieldTypeID, bool) {
	return types.ArrayOf(base)
}

// BoundedSequenceOf returns the bounded sequence identifier for a base type.
func BoundedSequenceOf(base FieldTypeID) (FieldTypeID, bool) {
	return types.BoundedSequenceOf(base)
}

// UnboundedSequenceOf returns the unbounded sequence identifier for a base type.
func UnboundedSequenceOf(base FieldTypeID) (FieldTypeID, bool) {
	return types.UnboundedSequenceOf(base)
}

// NewRegistry returns an empty schema registry.
func NewRegistry() *Registry {
	return types.NewRegistry()
}
