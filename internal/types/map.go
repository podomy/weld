// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1
package types

import (
	"fmt"
)

// MapBound gathers the upper bound source for a map field.
//
// MaxSize 0 means unbounded. Map bounds count entries, not bytes.
// See DDS-XTypes 1.4 section 7.2.3.2.2.1 and rules 14 to 16.
type MapBound struct {
	// MaxSize caps the entry count, 0 means unbounded.
	MaxSize uint
}

// CheckMapLength enforces the bound at encode time.
//
// An unbounded map accepts any count that fits uint32. A bounded map
// rejects counts above the max. Negative counts are rejected. Counts
// above the 32MiB sequence cap are rejected, matching the C
// weld_cdr_map_length_check cap and ReadMapLength. Callers must also
// check Remaining against entry width before allocating, mirroring the
// sequence CheckSequenceAllocation guard.
func CheckMapLength(n int, bound MapBound) error {
	if n < 0 {
		return fmt.Errorf("types: negative map length %d: %w", n, ErrInvalid)
	}
	if uint64(n) > uint64(maxSequenceElements) {
		return fmt.Errorf("types: map length %d exceeds cap %d: %w", n, maxSequenceElements, ErrInvalid)
	}
	if uint64(n) > 0xFFFFFFFF {
		return fmt.Errorf("types: map length %d overflows uint32: %w", n, ErrInvalid)
	}
	if bound.MaxSize != 0 && uint64(n) > uint64(bound.MaxSize) {
		return fmt.Errorf("types: map length %d exceeds bound %d: %w", n, bound.MaxSize, ErrInvalid)
	}
	return nil
}

// WriteMapLength writes a map entry count prefix with bound checks.
//
// The count is a uint32 with 4 byte alignment, shared by both XCDR
// versions. Under XCDR2 the caller wraps the map in a DHEADER first.
// Entries follow in key order with no per entry framing.
func (e *Encoder) WriteMapLength(n int, bound MapBound) error {
	err := CheckMapLength(n, bound)
	if err != nil {
		return err
	}
	putErr := e.putUint32(uint32(n)) //nolint:gosec // G115: range checked by CheckMapLength
	if putErr != nil {
		return fmt.Errorf("types: encode map length: %w", putErr)
	}
	return nil
}

// ReadMapLength reads a map entry count prefix with bound checks.
//
// Over cap counts and counts above the bound count a drop and return
// an error without allocating, mirroring ReadSequenceLength. Callers
// must also call CheckSequenceAllocation before allocating entries.
func (d *Decoder) ReadMapLength(bound MapBound) (int, error) {
	length, err := d.readUint32()
	if err != nil {
		return 0, err
	}
	if uint64(length) > uint64(maxSequenceElements) {
		d.drops++
		return 0, fmt.Errorf("types: map length %d exceeds cap %d: %w", length, maxSequenceElements, ErrInvalid)
	}
	if bound.MaxSize != 0 && uint64(length) > uint64(bound.MaxSize) {
		d.drops++
		return 0, fmt.Errorf("types: map length %d exceeds bound %d: %w", length, bound.MaxSize, ErrInvalid)
	}
	return int(length), nil
}

// IsPortablext6aMapKey reports whether a description id is a portable map key.
//
// Portable keys are scalar signed and unsigned integer bases plus narrow
// and wide string bases, per DDS-XTypes 1.4 section 7.2.3.2.2.1. Array and
// sequence wrappers are rejected even when their base is portable: a key
// must be one scalar value.
func IsPortablext6aMapKey(id FieldTypeID) bool {
	if id.IsArray() || id.IsBoundedSequence() || id.IsUnboundedSequence() {
		return false
	}
	switch id.Base() {
	case FieldTypeInt8, FieldTypeUInt8, FieldTypeInt16, FieldTypeUInt16,
		FieldTypeInt32, FieldTypeUInt32, FieldTypeInt64, FieldTypeUInt64,
		FieldTypeString, FieldTypeWString, FieldTypeFixedString, FieldTypeFixedWString,
		FieldTypeBoundedString, FieldTypeBoundedWString:
		return true
	case FieldTypeNotSet, FieldTypeNestedType, FieldTypeFloat, FieldTypeDouble,
		FieldTypeLongDouble, FieldTypeChar, FieldTypeWChar, FieldTypeBoolean,
		FieldTypeByte, FieldTypeArrayOffset, FieldTypeBoundedSequenceOffset,
		FieldTypeUnboundedSequenceOffset:
		return false
	default:
		return false
	}
}

// IsPortablext6aRosMapKey reports whether an introspection kind is portable.
//
// The set mirrors IsPortablext6aMapKey: integer kinds plus narrow and
// wide strings. Bool, char, wchar, floats, and messages are rejected.
func IsPortablext6aRosMapKey(kind RosTypeID) bool {
	switch kind {
	case RosTypeInt8, RosTypeUint8, RosTypeOctet, RosTypeInt16, RosTypeUint16,
		RosTypeInt32, RosTypeUint32, RosTypeInt64, RosTypeUint64,
		RosTypeString, RosTypeWString:
		return true
	case RosTypeFloat, RosTypeDouble, RosTypeLongDbl, RosTypeChar, RosTypeWChar,
		RosTypeBoolean, RosTypeMessage:
		return false
	default:
		return false
	}
}

// CheckSortedxt6aMapKeysNumeric verifies strictly increasing numeric key order.
//
// Keys are CDR-encoded key bytes decoded with the given endian and kind,
// then compared via Comparext6bMapKey (numeric value order). This unifies
// encode (CheckMapKeysxt6bOrdered) and decode on numeric order so LE and BE
// agree. Empty and single entry inputs pass; duplicates and decreasing
// keys fail. Unsupported kinds fail.
func CheckSortedxt6aMapKeysNumeric(kind RosTypeID, keys [][]byte, littleEndian bool) error {
	decoded := make([]any, 0, len(keys))
	for i := range keys {
		v, err := decodext6bMapKeyBytes(kind, keys[i], littleEndian)
		if err != nil {
			return err
		}
		decoded = append(decoded, v)
	}
	return CheckMapKeysxt6bOrdered(kind, decoded)
}

// decodext6bMapKeyBytes decodes one CDR-encoded map key for order checks.
func decodext6bMapKeyBytes(kind RosTypeID, raw []byte, littleEndian bool) (any, error) {
	dec, err := newMapKeyDecoder(raw, littleEndian)
	if err != nil {
		return nil, err
	}
	if isxt6bSignedKeyKind(kind) {
		return decodeSignedMapKey(dec, kind)
	}
	if isxt6bUnsignedKeyKind(kind) {
		return decodeUnsignedMapKey(dec, kind)
	}
	if isxt6bStringKeyKind(kind) {
		return decodeStringMapKey(dec, raw, littleEndian)
	}
	return nil, fmt.Errorf("types: unsupported map key kind %s: %w", kind.String(), ErrInvalid)
}

// newMapKeyDecoder builds a decoder for one map key wire value.
func newMapKeyDecoder(raw []byte, littleEndian bool) (*Decoder, error) {
	hdr := EncodeHeader(littleEndian)
	wire := append([]byte(nil), hdr[:]...)
	wire = append(wire, raw...)
	dec, err := NewDecoder(wire)
	if err != nil {
		return nil, err
	}
	return dec, nil
}

// decodeSignedMapKey decodes signed integer map keys.
func decodeSignedMapKey(dec *Decoder, kind RosTypeID) (any, error) {
	if kind == RosTypeInt8 || kind == RosTypeChar {
		return readMapKey(dec, (*Decoder).ReadInt8)
	}
	if kind == RosTypeInt16 {
		return readMapKey(dec, (*Decoder).ReadInt16)
	}
	if kind == RosTypeInt32 {
		return readMapKey(dec, (*Decoder).ReadInt32)
	}
	if kind == RosTypeInt64 {
		return readMapKey(dec, (*Decoder).ReadInt64)
	}
	return nil, fmt.Errorf("types: unsupported signed map key kind %s: %w", kind.String(), ErrInvalid)
}

// decodeUnsignedMapKey decodes unsigned integer map keys.
func decodeUnsignedMapKey(dec *Decoder, kind RosTypeID) (any, error) {
	if kind == RosTypeUint8 || kind == RosTypeOctet {
		return readMapKey(dec, (*Decoder).ReadUint8)
	}
	if kind == RosTypeUint16 {
		return readMapKey(dec, (*Decoder).ReadUint16)
	}
	if kind == RosTypeUint32 {
		return readMapKey(dec, (*Decoder).ReadUint32)
	}
	if kind == RosTypeUint64 {
		return readMapKey(dec, (*Decoder).ReadUint64)
	}
	return nil, fmt.Errorf("types: unsupported unsigned map key kind %s: %w", kind.String(), ErrInvalid)
}

// readMapKey reads one key with error passthrough.
func readMapKey[T any](dec *Decoder, read func(*Decoder) (T, error)) (any, error) {
	v, err := read(dec)
	if err != nil {
		return nil, err
	}
	return v, nil
}

// decodeStringMapKey decodes narrow keys with wstring fallback.
func decodeStringMapKey(dec *Decoder, raw []byte, littleEndian bool) (any, error) {
	v, err := dec.ReadString()
	if err == nil {
		return v, nil
	}
	return decodeWStringMapKeyFallback(raw, littleEndian, err)
}

// decodeWStringMapKeyFallback retries a failed narrow read as wstring.
func decodeWStringMapKeyFallback(raw []byte, littleEndian bool, firstErr error) (any, error) {
	hdr := EncodeHeader(littleEndian)
	wire := append([]byte(nil), hdr[:]...)
	wire = append(wire, raw...)
	dec2, err := NewDecoder(wire)
	if err != nil {
		return nil, firstErr
	}
	ws, err := dec2.ReadWString()
	if err != nil {
		return nil, firstErr
	}
	return ws, nil
}

// CheckMapEntriesxt6bOrdered enforces numeric order on decoded map keys.
//
// Call after reading map entries: it rejects unsorted or duplicate keys
// via Comparext6bMapKey so decode fails closed on out-of-order wires.
func CheckMapEntriesxt6bOrdered(kind RosTypeID, keys []any) error {
	return CheckMapKeysxt6bOrdered(kind, keys)
}

// ReadMapKeysOrdered reads n map keys by kind and enforces numeric order.
//
// This is the decode-side enforcement for the encode-side
// CheckMapKeysxt6bOrdered/SortMapKeysxt6b rule per DDS-XTypes 7.2.3.2.2.1
// rules 14 to 16: keys arrive in strictly increasing numeric value order,
// not byte order, so LE and BE agree. It reads each key with the matching
// typed Read, collects values as any, then rejects duplicates and
// decreasing keys via CheckMapEntriesxt6bOrdered. Call after
// ReadMapLength plus CheckSequenceAllocation so a corrupt count cannot
// force a huge allocation before the order check.
func (d *Decoder) ReadMapKeysOrdered(kind RosTypeID, n int) ([]any, error) {
	if n < 0 {
		d.drops++
		return nil, fmt.Errorf("types: map keys negative count %d: %w", n, ErrInvalid)
	}
	if uint64(n) > uint64(maxSequenceElements) {
		d.drops++
		return nil, fmt.Errorf("types: map keys count %d exceeds cap %d: %w", n, maxSequenceElements, ErrInvalid)
	}
	keys := make([]any, 0, n)
	for range n {
		key, err := readMapKeyByKind(d, kind)
		if err != nil {
			return nil, err
		}
		keys = append(keys, key)
	}
	err := CheckMapEntriesxt6bOrdered(kind, keys)
	if err != nil {
		d.drops++
		return nil, fmt.Errorf("types: map keys out of order: %w", err)
	}
	return keys, nil
}

// readMapKeyByKind reads one map key value in stream order by kind.
func readMapKeyByKind(d *Decoder, kind RosTypeID) (any, error) {
	if isxt6bSignedKeyKind(kind) {
		return readSignedMapKeyByKind(d, kind)
	}
	if isxt6bUnsignedKeyKind(kind) {
		return readUnsignedMapKeyByKind(d, kind)
	}
	if isxt6bStringKeyKind(kind) {
		return readStringMapKeyByKind(d, kind)
	}
	d.drops++
	return nil, fmt.Errorf("types: unsupported map key kind %s: %w", kind.String(), ErrInvalid)
}

// readSignedMapKeyByKind reads one signed integer map key.
func readSignedMapKeyByKind(d *Decoder, kind RosTypeID) (any, error) {
	if kind == RosTypeInt8 || kind == RosTypeChar {
		return d.ReadInt8()
	}
	if kind == RosTypeInt16 {
		return d.ReadInt16()
	}
	if kind == RosTypeInt32 {
		return d.ReadInt32()
	}
	if kind == RosTypeInt64 {
		return d.ReadInt64()
	}
	d.drops++
	return nil, fmt.Errorf("types: unsupported signed map key kind %s: %w", kind.String(), ErrInvalid)
}

// readUnsignedMapKeyByKind reads one unsigned integer map key.
func readUnsignedMapKeyByKind(d *Decoder, kind RosTypeID) (any, error) {
	if kind == RosTypeUint8 || kind == RosTypeOctet {
		return d.ReadUint8()
	}
	if kind == RosTypeUint16 {
		return d.ReadUint16()
	}
	if kind == RosTypeUint32 {
		return d.ReadUint32()
	}
	if kind == RosTypeUint64 {
		return d.ReadUint64()
	}
	d.drops++
	return nil, fmt.Errorf("types: unsupported unsigned map key kind %s: %w", kind.String(), ErrInvalid)
}

// readStringMapKeyByKind reads one narrow or wide string map key.
func readStringMapKeyByKind(d *Decoder, kind RosTypeID) (any, error) {
	if kind == RosTypeWString {
		return d.ReadWString()
	}
	return d.ReadString()
}
