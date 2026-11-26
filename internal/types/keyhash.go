// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1
package types

import (
	"crypto/md5" //nolint:gosec // G501: MD5 is mandated by DDS-XTypes 7.6.8.1 for KeyHash
	"fmt"
)

// KeyHash computation per DDS-XTypes 1.4 section 7.6.8.1.
//
// The key hash of a keyed type is derived from the CDR serialization of
// its key fields only. This file holds the pure hash step: callers
// serialize the key holder first, then hash the bytes here. Nothing here
// touches take paths or the transport; discovery integration owns the
// wiring and uses these helpers later.
const (
	// KeyHashSize is the 16 byte RTPS KeyHash width.
	KeyHashSize = 16
)

// KeyHashMD5 returns the MD5 digest of serialized key bytes.
//
// The digest is used when the serialized key exceeds 16 bytes, per
// RTPS 2.5 clause 9.6.4.8. Short keys use ComputeKeyHash instead.
func KeyHashMD5(key []byte) [KeyHashSize]byte {
	sum := md5.Sum(key) //nolint:gosec // G501: MD5 is mandated by DDS-XTypes 7.6.8.1 for KeyHash
	var out [KeyHashSize]byte
	copy(out[:], sum[:])
	return out
}

// ComputeKeyHash returns the RTPS KeyHash for serialized key bytes.
//
// Keys of 16 bytes or fewer are copied with zero padding. Longer keys
// hash with MD5. The input is the serialized key holder described in
// 7.6.8.2; this helper never serializes by itself.
func ComputeKeyHash(key []byte) [KeyHashSize]byte {
	var out [KeyHashSize]byte
	if len(key) > KeyHashSize {
		return KeyHashMD5(key)
	}
	copy(out[:], key)
	return out
}

// Projectxt6aKeyHolder concatenates holder chunks in key path order.
//
// Chunks maps a key field path such as "inner.code" to its already
// serialized bytes. Paths lists the key fields in IDL declaration order.
// The output is pure bytes ready for ComputeKeyHash. For framed holders
// built from live values use SerializeKeyHolder; this helper stays for
// pre-serialized chunk assembly.
func Projectxt6aKeyHolder(chunks map[string][]byte, paths []string) ([]byte, error) {
	if len(paths) == 0 {
		return nil, fmt.Errorf("types: key holder has no key paths: %w", ErrInvalid)
	}
	var out []byte
	for i := range paths {
		chunk, ok := chunks[paths[i]]
		if !ok {
			return nil, fmt.Errorf("types: key holder misses path %q: %w", paths[i], ErrInvalid)
		}
		out = append(out, chunk...)
	}
	return out, nil
}

// KeyField is one key field value for holder serialization.
//
// Kind selects the CDR encoding; Value holds the Go value (uint32,
// int32, string, bool, etc.). Paths order is the IDL declaration order
// of the holder per 7.6.8.2.
type KeyField struct {
	Name  string
	Kind  RosTypeID
	Value any
}

// SerializeKeyHolder serializes key fields in declaration order.
//
// The output is the key holder CDR without the encapsulation header:
// each key field is encoded with its CDR alignment relative to the
// holder start, matching 7.6.8.2 steps. The caller hashes the output
// with ComputeKeyHash (short keys padded, long keys MD5). Little endian
// selects the holder byte order.
func SerializeKeyHolder(fields []KeyField, littleEndian bool) ([]byte, error) {
	if len(fields) == 0 {
		return nil, fmt.Errorf("types: key holder has no key fields: %w", ErrInvalid)
	}
	enc := NewEncoder(littleEndian)
	// Strip the header at the end: holder bytes exclude encapsulation.
	for i := range fields {
		err := writeKeyField(enc, fields[i])
		if err != nil {
			return nil, err
		}
	}
	if len(enc.buf) < HeaderSize {
		return nil, fmt.Errorf("types: key holder wire too short: %w", ErrTruncated)
	}
	out := append([]byte(nil), enc.buf[HeaderSize:]...)
	return out, nil
}

// writeKeyField encodes one key field value by kind.
//
// Float, double, long-double, and nested message keys are supported per
// DDS-XTypes 7.6.8.2 key holder CDR: floats use 4 byte CDR, doubles 8
// byte, long-doubles 16 byte opaque, and messages flatten their nested
// key fields in declaration order on the same encoder so alignment is
// preserved.
func writeKeyField(enc *Encoder, f KeyField) error {
	if isKeyIntegerKind(f.Kind) {
		return writeKeyIntegerField(enc, f)
	}
	return writeKeyNonIntegerField(enc, f)
}

// isKeyIntegerKind reports 32 and 16 and 64 bit integer key kinds.
func isKeyIntegerKind(kind RosTypeID) bool {
	return kind == RosTypeUint32 || kind == RosTypeOctet || kind == RosTypeUint8 ||
		kind == RosTypeInt32 || kind == RosTypeChar || kind == RosTypeInt8 ||
		kind == RosTypeUint16 || kind == RosTypeWChar || kind == RosTypeInt16 ||
		kind == RosTypeUint64 || kind == RosTypeInt64
}

// writeKeyIntegerField encodes integer-like key fields by kind.
func writeKeyIntegerField(enc *Encoder, f KeyField) error {
	if f.Kind == RosTypeUint32 || f.Kind == RosTypeOctet || f.Kind == RosTypeUint8 {
		return writeKeyUint32(enc, f)
	}
	if f.Kind == RosTypeInt32 || f.Kind == RosTypeChar || f.Kind == RosTypeInt8 {
		return writeKeyInt32(enc, f)
	}
	if f.Kind == RosTypeUint16 || f.Kind == RosTypeWChar {
		return writeKeyUint16(enc, f)
	}
	if f.Kind == RosTypeInt16 {
		return writeKeyInt16(enc, f)
	}
	if f.Kind == RosTypeUint64 || f.Kind == RosTypeInt64 {
		return writeKeyUint64(enc, f)
	}
	return fmt.Errorf("types: key field %q kind %s not integer: %w", f.Name, f.Kind.String(), ErrInvalid)
}

// writeKeyNonIntegerField encodes non-integer key fields by kind.
func writeKeyNonIntegerField(enc *Encoder, f KeyField) error {
	if f.Kind == RosTypeBoolean {
		return writeKeyBool(enc, f)
	}
	if f.Kind == RosTypeString {
		return writeKeyString(enc, f)
	}
	if f.Kind == RosTypeWString {
		return writeKeyWString(enc, f)
	}
	if f.Kind == RosTypeFloat {
		return writeKeyFloat(enc, f)
	}
	if f.Kind == RosTypeDouble {
		return writeKeyDouble(enc, f)
	}
	if f.Kind == RosTypeLongDbl {
		return writeKeyLongDouble(enc, f)
	}
	if f.Kind == RosTypeMessage {
		return writeKeyMessage(enc, f)
	}
	return fmt.Errorf("types: key field %q kind %s unsupported: %w", f.Name, f.Kind.String(), ErrInvalid)
}

// writeKeyUint32 encodes uint32-like key fields.
func writeKeyUint32(enc *Encoder, f KeyField) error {
	v, ok := toUint32(f.Value)
	if !ok {
		return fmt.Errorf("types: key field %q wants uint32 value: %w", f.Name, ErrInvalid)
	}
	enc.WriteUint32(v)
	return nil
}

// writeKeyInt32 encodes int32-like key fields.
func writeKeyInt32(enc *Encoder, f KeyField) error {
	v, ok := toInt32(f.Value)
	if !ok {
		return fmt.Errorf("types: key field %q wants int32 value: %w", f.Name, ErrInvalid)
	}
	enc.WriteInt32(v)
	return nil
}

// writeKeyUint16 encodes uint16-like key fields.
func writeKeyUint16(enc *Encoder, f KeyField) error {
	v, ok := toUint32(f.Value)
	if !ok {
		return fmt.Errorf("types: key field %q wants uint16 value: %w", f.Name, ErrInvalid)
	}
	if v > 0xFFFF {
		return fmt.Errorf("types: key field %q uint16 %d overflows: %w", f.Name, v, ErrInvalid)
	}
	enc.WriteUint16(uint16(v))
	return nil
}

// writeKeyInt16 encodes int16 key fields.
func writeKeyInt16(enc *Encoder, f KeyField) error {
	v, ok := toInt32(f.Value)
	if !ok {
		return fmt.Errorf("types: key field %q wants int16 value: %w", f.Name, ErrInvalid)
	}
	if v < -32768 || v > 32767 {
		return fmt.Errorf("types: key field %q int16 %d overflows: %w", f.Name, v, ErrInvalid)
	}
	enc.WriteInt16(int16(v))
	return nil
}

// writeKeyUint64 encodes 64 bit key fields.
func writeKeyUint64(enc *Encoder, f KeyField) error {
	v, ok := toUint64(f.Value)
	if !ok {
		return fmt.Errorf("types: key field %q wants 64 bit value: %w", f.Name, ErrInvalid)
	}
	if f.Kind == RosTypeInt64 {
		enc.WriteInt64(int64(v)) //nolint:gosec // G115: two's complement reinterpret is IDL defined
	} else {
		enc.WriteUint64(v)
	}
	return nil
}

// writeKeyBool encodes bool key fields.
func writeKeyBool(enc *Encoder, f KeyField) error {
	v, ok := f.Value.(bool)
	if !ok {
		return fmt.Errorf("types: key field %q wants bool value: %w", f.Name, ErrInvalid)
	}
	enc.WriteBool(v)
	return nil
}

// writeKeyString encodes string key fields.
func writeKeyString(enc *Encoder, f KeyField) error {
	v, ok := f.Value.(string)
	if !ok {
		return fmt.Errorf("types: key field %q wants string value: %w", f.Name, ErrInvalid)
	}
	err := enc.WriteString(v)
	if err != nil {
		return fmt.Errorf("types: key field %q string: %w", f.Name, err)
	}
	return nil
}

// writeKeyWString encodes wstring key fields.
func writeKeyWString(enc *Encoder, f KeyField) error {
	v, ok := f.Value.(string)
	if !ok {
		return fmt.Errorf("types: key field %q wants wstring value: %w", f.Name, ErrInvalid)
	}
	err := enc.WriteWString(v)
	if err != nil {
		return fmt.Errorf("types: key field %q wstring: %w", f.Name, err)
	}
	return nil
}

// writeKeyFloat encodes float32 key fields with 4 byte CDR alignment.
func writeKeyFloat(enc *Encoder, f KeyField) error {
	v, ok := toFloat32(f.Value)
	if !ok {
		return fmt.Errorf("types: key field %q wants float32 value: %w", f.Name, ErrInvalid)
	}
	enc.WriteFloat32(v)
	return nil
}

// writeKeyDouble encodes float64 key fields with 8 byte CDR alignment.
func writeKeyDouble(enc *Encoder, f KeyField) error {
	v, ok := toFloat64(f.Value)
	if !ok {
		return fmt.Errorf("types: key field %q wants float64 value: %w", f.Name, ErrInvalid)
	}
	enc.WriteFloat64(v)
	return nil
}

// writeKeyLongDouble encodes 16 byte long-double key fields.
func writeKeyLongDouble(enc *Encoder, f KeyField) error {
	v, ok := toLongDouble(f.Value)
	if !ok {
		return fmt.Errorf("types: key field %q wants long-double value: %w", f.Name, ErrInvalid)
	}
	err := enc.WriteLongDouble(v)
	if err != nil {
		return fmt.Errorf("types: key field %q long-double: %w", f.Name, err)
	}
	return nil
}

// writeKeyMessage encodes nested message key fields by flattening.
//
// Value must be []KeyField holding the nested keys in declaration order.
// The nested fields are encoded inline on the same encoder per 7.6.8.2,
// so inter-field pad matches SerializeKeyHolder.
func writeKeyMessage(enc *Encoder, f KeyField) error {
	nested, ok := f.Value.([]KeyField)
	if !ok {
		return fmt.Errorf("types: key field %q wants []KeyField value: %w", f.Name, ErrInvalid)
	}
	if len(nested) == 0 {
		return fmt.Errorf("types: key field %q nested holder empty: %w", f.Name, ErrInvalid)
	}
	for i := range nested {
		err := writeKeyField(enc, nested[i])
		if err != nil {
			return fmt.Errorf("types: key field %q nested %q: %w", f.Name, nested[i].Name, err)
		}
	}
	return nil
}

// toFloat32 converts Go values to float32 for key encoding.
func toFloat32(v any) (float32, bool) {
	switch x := v.(type) {
	case float32:
		return x, true
	case float64:
		return float32(x), true
	default:
		return 0, false
	}
}

// toFloat64 converts Go values to float64 for key encoding.
func toFloat64(v any) (float64, bool) {
	switch x := v.(type) {
	case float64:
		return x, true
	case float32:
		return float64(x), true
	default:
		return 0, false
	}
}

// toLongDouble converts Go values to LongDouble for key encoding.
func toLongDouble(v any) (LongDouble, bool) {
	switch x := v.(type) {
	case LongDouble:
		return x, true
	case [16]byte:
		return LongDouble(x), true
	default:
		return LongDouble{}, false
	}
}

// toUint32 converts integer Go values to uint32 for key encoding.
func toUint32(v any) (uint32, bool) {
	switch x := v.(type) {
	case uint32:
		return x, true
	case uint8:
		return uint32(x), true
	case uint16:
		return uint32(x), true
	case uint64:
		if x > 0xFFFFFFFF {
			return 0, false
		}
		return uint32(x), true
	case int:
		if x < 0 || uint64(x) > 0xFFFFFFFF {
			return 0, false
		}
		return uint32(x), true
	case int32:
		if x < 0 {
			return 0, false
		}
		return uint32(x), true
	default:
		return 0, false
	}
}

// toInt32 converts integer Go values to int32 for key encoding.
func toInt32(v any) (int32, bool) {
	switch x := v.(type) {
	case int32:
		return x, true
	case int8:
		return int32(x), true
	case int16:
		return int32(x), true
	case int:
		if int64(x) < -2147483648 || int64(x) > 2147483647 {
			return 0, false
		}
		return int32(x), true
	case uint8:
		return int32(x), true
	default:
		return 0, false
	}
}

// toUint64 converts integer Go values to uint64 for key encoding.
func toUint64(v any) (uint64, bool) {
	switch x := v.(type) {
	case uint64:
		return x, true
	case int64:
		return uint64(x), true //nolint:gosec // G115: two's complement reinterpret is IDL defined
	case uint32:
		return uint64(x), true
	case int32:
		return uint64(uint32(x)), true //nolint:gosec // G115: two's complement reinterpret is IDL defined
	case int:
		return uint64(x), true //nolint:gosec // G115: int to uint64 reinterpret for key hashing
	default:
		return 0, false
	}
}

// EncodeKeyFieldChunks builds holder chunks from live values.
//
// Each chunk is the CDR slice of one KeyField from a single shared
// encoder, so leading alignment pad for fields after the first is kept in
// the following chunk. Concatenating the chunks in declaration order via
// Projectxt6aKeyHolder equals SerializeKeyHolder output, including layouts
// that need pad such as u8 followed by u32 or u64.
func EncodeKeyFieldChunks(fields []KeyField, littleEndian bool) (map[string][]byte, error) {
	if len(fields) == 0 {
		return nil, fmt.Errorf("types: key holder has no key fields: %w", ErrInvalid)
	}
	enc := NewEncoder(littleEndian)
	out := make(map[string][]byte, len(fields))
	for i := range fields {
		start := len(enc.buf)
		err := writeKeyField(enc, fields[i])
		if err != nil {
			return nil, err
		}
		out[fields[i].Name] = append([]byte(nil), enc.buf[start:]...)
	}
	return out, nil
}
