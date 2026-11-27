// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1
package types

// Package types carries the wire codec for Weld.
//
// This file implements XCDR1 data encoding per introspection plus XCDR2
// delimited encoding for appendable and mutable kinds. XCDR1 stays the
// only representation on final DDS paths so existing ROS 2 wires are
// byte identical. XCDR2 is emitted only through the explicit
// extensibility aware constructors. Alignment is relative to the user
// payload start, outside the encapsulation header and service prefix,
// matching the de facto ROS 2 wire layout. No transport code,
// no cgo, and no shared global state live here.

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"strings"
	"unicode/utf16"
)

// Limits guard decoders against corrupt length prefixes.
//
// The 32MiB values bound a single sample to keep worst case allocation
// small when a corrupt prefix claims a huge length. The size matches the
// common DDS RTPS maximum message size used to bound one sample on the
// wire, so a single string or sequence cannot force more than 32MiB.
const (
	// maxStringBytes caps one string payload including NUL.
	maxStringBytes = 32 << 20
	// maxSequenceElements caps one sequence element count regardless of width.
	maxSequenceElements = 32 << 20
	// maxWStringUnits caps one wstring length in UTF-16 code units including NUL.
	maxWStringUnits = maxStringBytes / 2
)

// ErrTruncated reports short input that needs more bytes.
//
// Over cap lengths return ErrInvalid instead, so callers can tell a
// truncated sample from a malformed one. Both count a drop and never panic.
var ErrTruncated = errors.New("cdr: truncated")

// ErrInvalid reports malformed input that fails validation.
//
// Over cap prefixes, missing NUL, bad bool, and trailing bytes map here.
// Both sentinels count a drop and let TakeFrom continue with the next sample.
var ErrInvalid = errors.New("cdr: invalid")

// Wire value constants avoid magic literals at encode and decode sites.
const (
	// boolFalseByte is the IDL bool false encoding.
	boolFalseByte = 0x00
	// boolTrueByte is the IDL bool true encoding.
	boolTrueByte = 0x01
	// nulByte is the string NUL terminator and the zero pad byte.
	nulByte = 0x00
)

// Encoder builds one XCDR sample including its 4 byte header.
//
// The header is written at construction so later alignment is already
// rebased 4 bytes forward. Call Bytes to get the padded wire form.
// Metrics for opaque parsing live in envelope Metrics, which is separate
// because Metrics is caller owned and may be nil while Encoder state is
// builder owned. Decoder carries its own private drop counter for
// streaming reads, so the two counters have different lifetimes.
// Final encoders use XCDR1; appendable and mutable encoders built by
// NewEncoderWithExt use XCDR2 with 4 byte max alignment.
type Encoder struct {
	buf          []byte
	order        binary.ByteOrder
	le           bool
	payloadStart int
	// noDummy skips the empty dummy byte in Bytes. WriteServiceHeader
	// sets it because ROS sends no dummy for empty service structs.
	noDummy bool
	// ext is the extensibility kind selected at construction.
	ext Extensibility
	// ver is the XCDR version selected at construction.
	ver EncodingVersion
}

// NewEncoder returns an Encoder with the 4 byte header already written.
//
// le selects little endian (0x01) when true and big endian (0x00)
// when false. Header bytes are built by EncodeHeader so header encoding
// lives in one place. The encoder is final XCDR1 so existing ROS wires
// stay byte identical; use NewEncoderWithExt for appendable/mutable V2.
func NewEncoder(littleEndian bool) *Encoder {
	e := &Encoder{le: littleEndian, payloadStart: HeaderSize, ext: ExtFinal, ver: XCDRVersion1}
	if littleEndian {
		e.order = binary.LittleEndian
	} else {
		e.order = binary.BigEndian
	}
	h := EncodeHeader(littleEndian)
	e.buf = append(e.buf, h[:]...)
	return e
}

// NewEncoderWithExt returns an Encoder for one extensibility kind.
//
// Final kinds use XCDR1; appendable and mutable kinds use XCDR2 per
// Table 32. The header is written by EncodeEncapsulationxt6b so V2 IDs
// appear on the wire for appendable/mutable. Alignment caps at 4 under
// V2 via Alignxt6bVersioned; final V1 paths are byte identical.
func NewEncoderWithExt(ext Extensibility, littleEndian bool) *Encoder {
	ver := XCDRVersion1
	if ext != ExtFinal {
		ver = XCDRVersion2
	}
	e := &Encoder{le: littleEndian, payloadStart: HeaderSize, ext: ext, ver: ver}
	if littleEndian {
		e.order = binary.LittleEndian
	} else {
		e.order = binary.BigEndian
	}
	h := EncodeEncapsulationxt6b(ext, littleEndian)
	e.buf = append(e.buf, h[:]...)
	return e
}

// NewEncoderWithExtVersioned returns an Encoder for an explicit kind plus version.
//
// This wires the V1 PLAIN_CDR (appendable) and V1 PL_CDR (mutable) emit
// paths per Table 57, which NewEncoderWithExt never selects because it
// defaults non-final kinds to V2. V1 appendable yields 0x0000/0x0001 and
// V1 mutable yields 0x0002/0x0003 via EncodeEncapsulationxt6bWithVersion;
// V2 yields CDR2/D_CDR2/PL_CDR2 per Table 32. Use this for explicit V1
// appendable/mutable emits; all other code keeps NewEncoderWithExt.
func NewEncoderWithExtVersioned(ext Extensibility, ver EncodingVersion, littleEndian bool) *Encoder {
	e := &Encoder{le: littleEndian, payloadStart: HeaderSize, ext: ext, ver: ver}
	if littleEndian {
		e.order = binary.LittleEndian
	} else {
		e.order = binary.BigEndian
	}
	h := EncodeEncapsulationxt6bWithVersion(ext, ver, littleEndian)
	e.buf = append(e.buf, h[:]...)
	return e
}

// Extensibility reports the kind selected at construction.
func (e *Encoder) Extensibility() Extensibility {
	return e.ext
}

// Version reports the XCDR version selected at construction.
func (e *Encoder) Version() EncodingVersion {
	return e.ver
}

// LittleEndian reports the negotiated byte order for this encoder.
func (e *Encoder) LittleEndian() bool {
	return e.le
}

// Len returns the current buffer size including the header.
//
// The final dummy byte and 4 byte pad applied by Bytes are not included,
// so Len is stable across Bytes calls.
func (e *Encoder) Len() int {
	return len(e.buf)
}

// Bytes returns the wire form padded to a 4 byte boundary.
//
// Bytes is a pure finalize step. It does not mutate the encoder, so it
// may be called multiple times and Len stays unchanged. An empty user
// payload, meaning no bytes after the header or after the service prefix,
// encodes as one dummy zero byte via needsDummyByte before padding,
// except for service encoders where WriteServiceHeader disables the dummy
// to match ROS. Padding bytes are zero.
func (e *Encoder) Bytes() []byte {
	userLen := max(len(e.buf)-e.payloadStart, 0)
	needDummy := needsDummyByte(userLen) && !e.noDummy
	total := len(e.buf)
	if needDummy {
		total++
	}
	pad := padTo4(total)
	out := make([]byte, 0, total+pad)
	out = append(out, e.buf...)
	if needDummy {
		out = append(out, nulByte)
	}
	for range pad {
		out = append(out, nulByte)
	}
	return out
}

// Align pads with zeroes until the buffer offset is a multiple of n.
//
// Only 1, 2, 4, or 8 are valid. Any other n is a caller bug and returns
// an error without mutating the buffer. All current callers pass constants,
// so the error never triggers in practice. This mirrors Decoder.Align
// which returns an error for invalid n without counting a drop.
//
// Alignment is relative to the user payload start, not the buffer
// start: the encapsulation header (and service prefix when present) is
// outside the CDR alignment domain, matching the de facto ROS 2 wire
// layout where a leading double needs no pad.
func (e *Encoder) Align(n int) error {
	if n != 1 && n != 2 && n != 4 && n != 8 {
		return fmt.Errorf("cdr: invalid alignment %d", n)
	}
	pad := alignPad(len(e.buf)-e.payloadStart, n)
	for range pad {
		e.buf = append(e.buf, nulByte)
	}
	return nil
}

// alignWidth aligns with the XCDR version cap.
//
// V1 aligns to the full width; V2 caps at 4 per 7.4.2. Final V1 paths
// call Align directly so they stay byte identical; this helper is for
// width sensitive 8 and 16 byte primitives on versioned encoders.
func (e *Encoder) alignWidth(width int) error {
	need := width
	if e.ver == XCDRVersion2 && need > MaxAlignxt6bV2 {
		need = MaxAlignxt6bV2
	}
	return e.Align(need)
}

// putByte appends one raw byte with 1 byte alignment.
func (e *Encoder) putByte(v byte) {
	e.buf = append(e.buf, v)
}

// putUint16 appends a 2 byte value with 2 byte alignment.
func (e *Encoder) putUint16(v uint16) error {
	err := e.Align(2)
	if err != nil {
		return err
	}
	var tmp [2]byte
	e.order.PutUint16(tmp[:], v)
	e.buf = append(e.buf, tmp[:]...)
	return nil
}

// putUint32 appends a 4 byte value with 4 byte alignment.
func (e *Encoder) putUint32(v uint32) error {
	err := e.Align(4)
	if err != nil {
		return err
	}
	var tmp [4]byte
	e.order.PutUint32(tmp[:], v)
	e.buf = append(e.buf, tmp[:]...)
	return nil
}

// putUint64 appends an 8 byte value with version capped alignment.
//
// V1 aligns to 8; V2 caps at 4 per 7.4.2. Final V1 encoders keep the
// legacy 8 byte alignment byte identical.
func (e *Encoder) putUint64(v uint64) error {
	err := e.alignWidth(8)
	if err != nil {
		return err
	}
	var tmp [8]byte
	e.order.PutUint64(tmp[:], v)
	e.buf = append(e.buf, tmp[:]...)
	return nil
}

// WriteUint8 writes an octet or uint8 with 1 byte alignment.
func (e *Encoder) WriteUint8(v uint8) {
	e.putByte(v)
}

// WriteInt8 writes a signed 8 bit value with 1 byte alignment.
func (e *Encoder) WriteInt8(v int8) {
	e.putByte(byte(v)) //nolint:gosec // G115: intentional two's-complement reinterpret for XCDR1 encoding
}

// WriteChar writes an IDL char (signed 8 bit) with 1 byte alignment.
func (e *Encoder) WriteChar(v int8) {
	e.putByte(byte(v)) //nolint:gosec // G115: intentional two's-complement reinterpret for XCDR1 encoding
}

// WriteBool writes an IDL bool as one byte, 0 or 1.
func (e *Encoder) WriteBool(v bool) {
	if v {
		e.putByte(boolTrueByte)
		return
	}
	e.putByte(boolFalseByte)
}

// WriteWchar writes an IDL wchar as 2 bytes with 2 byte alignment.
func (e *Encoder) WriteWchar(v uint16) {
	err := e.putUint16(v)
	if err != nil {
		panic("types: wchar align failed")
	}
}

// WriteInt16 writes a 2 byte signed value with 2 byte alignment.
func (e *Encoder) WriteInt16(v int16) {
	err := e.putUint16(uint16(v)) //nolint:gosec // G115: intentional two's-complement reinterpret for XCDR1 encoding
	if err != nil {
		panic("types: int16 align failed")
	}
}

// WriteUint16 writes a 2 byte unsigned value with 2 byte alignment.
func (e *Encoder) WriteUint16(v uint16) {
	err := e.putUint16(v)
	if err != nil {
		panic("types: uint16 align failed")
	}
}

// WriteInt32 writes a 4 byte signed value with 4 byte alignment.
func (e *Encoder) WriteInt32(v int32) {
	err := e.putUint32(uint32(v)) //nolint:gosec // G115: intentional two's-complement reinterpret for XCDR1 encoding
	if err != nil {
		panic("types: int32 align failed")
	}
}

// WriteUint32 writes a 4 byte unsigned value with 4 byte alignment.
func (e *Encoder) WriteUint32(v uint32) {
	err := e.putUint32(v)
	if err != nil {
		panic("types: uint32 align failed")
	}
}

// WriteInt64 writes an 8 byte signed value with 8 byte alignment.
func (e *Encoder) WriteInt64(v int64) {
	err := e.putUint64(uint64(v)) //nolint:gosec // G115: intentional two's-complement reinterpret for XCDR1 encoding
	if err != nil {
		panic("types: int64 align failed")
	}
}

// WriteUint64 writes an 8 byte unsigned value with 8 byte alignment.
func (e *Encoder) WriteUint64(v uint64) {
	err := e.putUint64(v)
	if err != nil {
		panic("types: uint64 align failed")
	}
}

// WriteFloat32 writes a 4 byte float with 4 byte alignment.
func (e *Encoder) WriteFloat32(v float32) {
	err := e.putUint32(math.Float32bits(v))
	if err != nil {
		panic("types: float32 align failed")
	}
}

// WriteFloat64 writes an 8 byte float with 8 byte alignment.
func (e *Encoder) WriteFloat64(v float64) {
	err := e.putUint64(math.Float64bits(v))
	if err != nil {
		panic("types: float64 align failed")
	}
}

// countRunes returns the rune count of s without allocating a slice.
func countRunes(s string) int {
	n := 0
	for range s {
		n++
	}
	return n
}

// truncatext6bStringToOctets limits s to at most bound octets on a rune boundary.
func truncatext6bStringToOctets(s string, bound int) string {
	if len(s) <= bound {
		return s
	}
	used := 0
	end := 0
	for i, r := range s {
		size := len(string(r))
		_ = i
		if used+size > bound {
			break
		}
		used += size
		end += size
	}
	return s[:end]
}

// truncatext6bWStringToUnits limits s to at most bound UTF-16 units on a rune boundary.
func truncatext6bWStringToUnits(s string, bound int) string {
	used := 0
	end := 0
	for i, r := range s {
		units := 1
		if r > 0xFFFF {
			units = 2
		}
		if used+units > bound {
			break
		}
		used += units
		end = i + len(string(r))
	}
	return s[:end]
}

// unitsOfxt6bWString counts UTF-16 code units in s excluding NUL.
func unitsOfxt6bWString(s string) int {
	n := 0
	for _, r := range s {
		if r > 0xFFFF {
			n += 2
		} else {
			n++
		}
	}
	return n
}

// encodeString appends length plus NUL for s.
//
// Pad ownership: the encoder inserts pad only per next-member alignment.
// It writes the length prefix with Align(4), then the bytes plus NUL with
// no trailing pad. The next member's Align absorbs any gap, and Bytes
// pads a terminal string to 4. The decoder never skips string pad and
// checks the NUL only.
func (e *Encoder) encodeString(s string) error {
	err := e.Align(4)
	if err != nil {
		return err
	}
	total := len(s) + 1
	var lenBytes [4]byte
	e.order.PutUint32(lenBytes[:], uint32(total)) //nolint:gosec // G115: total is a non-negative length guarded by callers
	e.buf = append(e.buf, lenBytes[:]...)
	e.buf = append(e.buf, s...)
	e.buf = append(e.buf, nulByte)
	return nil
}

// WriteString writes an IDL string as length plus NUL.
//
// The length prefix counts the trailing NUL. Payload bytes are the UTF-8
// bytes plus one NUL with no trailing pad. See encodeString for pad
// ownership.
//
// This is the unbounded write: it does not enforce maxStringBytes. A
// string longer than the cap encodes without error but the decoder will
// reject it with a drop. It does guard uint32 overflow: when len plus NUL
// exceeds MaxUint32 nothing is written and an error is returned. Use
// WriteStringChecked to enforce the decoder cap at encode time.
//
// Tolerant policy: interior NUL bytes are preserved as payload and only
// the final byte is required to be NUL on decode. Invalid UTF-8 bytes
// are preserved as is. No validation or normalization is applied here.
func (e *Encoder) WriteString(s string) error {
	if uint64(len(s))+1 > uint64(math.MaxUint32) {
		return errors.New("cdr: string length overflows uint32") //nolint:wrapcheck // new error, nothing to wrap
	}
	err := e.encodeString(s)
	if err != nil {
		return fmt.Errorf("cdr: encode string: %w", err)
	}
	return nil
}

// WriteStringChecked writes a string and enforces the decoder cap.
//
// It returns an error without writing when the length including NUL
// exceeds maxStringBytes or overflows uint32. Otherwise it encodes the
// same wire form as WriteString.
func (e *Encoder) WriteStringChecked(s string) error {
	if uint64(len(s))+1 > uint64(math.MaxUint32) {
		return errors.New("cdr: string length overflows uint32") //nolint:wrapcheck // new error, nothing to wrap
	}
	if len(s)+1 > maxStringBytes {
		return fmt.Errorf("cdr: string length %d exceeds cap %d", len(s)+1, maxStringBytes)
	}
	err := e.encodeString(s)
	if err != nil {
		return fmt.Errorf("cdr: encode string: %w", err)
	}
	return nil
}

// WriteBoundedString writes a string checked against a bound.
//
// bound is the maximum octet count excluding NUL per DDS-XTypes
// 7.2.3.1.2. Zero means unbounded. A negative bound returns an error
// and leaves the buffer unchanged. Overlong input returns an error
// without writing; this strict path never truncates. For the explicit
// truncate policy use WriteBoundedStringTruncate. Even bounded writes
// may exceed the decoder cap when the bound is large; the decoder still
// rejects over cap input. Overflow past MaxUint32 returns an error
// without writing.
func (e *Encoder) WriteBoundedString(s string, bound int) (bool, error) {
	if bound < 0 {
		return false, fmt.Errorf("cdr: negative string bound %d", bound)
	}
	if bound == 0 {
		err := e.WriteString(s)
		if err != nil {
			return false, err
		}
		return false, nil
	}
	if len(s) > bound {
		return false, fmt.Errorf("cdr: string octets %d exceeds bound %d", len(s), bound)
	}
	if uint64(len(s))+1 > uint64(math.MaxUint32) {
		return false, errors.New("cdr: string length overflows uint32") //nolint:wrapcheck // new error, nothing to wrap
	}
	err := e.encodeString(s)
	if err != nil {
		return false, fmt.Errorf("cdr: encode string: %w", err)
	}
	return false, nil
}

// WriteBoundedStringTruncate writes a string with truncate policy.
//
// bound is the maximum octet count excluding NUL. Zero means unbounded.
// Overlong input is truncated on a rune boundary so multi byte UTF-8 is
// never split, the truncated form is encoded, and the call reports
// truncated true with nil error. This is the explicit non-strict helper;
// strict paths use WriteBoundedString and fail instead of truncating.
func (e *Encoder) WriteBoundedStringTruncate(s string, bound int) (bool, error) {
	if bound < 0 {
		return false, fmt.Errorf("cdr: negative string bound %d", bound)
	}
	if bound == 0 {
		err := e.WriteString(s)
		if err != nil {
			return false, err
		}
		return false, nil
	}
	if len(s) > bound {
		truncated := truncatext6bStringToOctets(s, bound)
		if uint64(len(truncated))+1 > uint64(math.MaxUint32) {
			return false, errors.New("cdr: string length overflows uint32") //nolint:wrapcheck // new error, nothing to wrap
		}
		err := e.encodeString(truncated)
		if err != nil {
			return false, fmt.Errorf("cdr: encode string: %w", err)
		}
		return true, nil
	}
	if uint64(len(s))+1 > uint64(math.MaxUint32) {
		return false, errors.New("cdr: string length overflows uint32") //nolint:wrapcheck // new error, nothing to wrap
	}
	err := e.encodeString(s)
	if err != nil {
		return false, fmt.Errorf("cdr: encode string: %w", err)
	}
	return false, nil
}

// WriteStringxt6bBoundedStrict writes a string and rejects over bound input.
//
// bound counts octets excluding NUL per DDS-XTypes 7.2.3.1.2. Zero means
// unbounded. Overlong input returns an error without writing.
func (e *Encoder) WriteStringxt6bBoundedStrict(s string, bound int) error {
	if bound < 0 {
		return fmt.Errorf("cdr: negative string bound %d", bound)
	}
	if bound > 0 && len(s) > bound {
		return fmt.Errorf("cdr: string octets %d exceeds bound %d", len(s), bound)
	}
	if uint64(len(s))+1 > uint64(math.MaxUint32) {
		return errors.New("cdr: string length overflows uint32") //nolint:wrapcheck // new error, nothing to wrap
	}
	err := e.encodeString(s)
	if err != nil {
		return fmt.Errorf("cdr: encode string: %w", err)
	}
	return nil
}

// encodeWString appends length plus UTF-16 units plus NUL for s.
//
// The length prefix counts UTF-16 code units including the trailing NUL.
// No trailing pad is written. Pad ownership matches encodeString: the
// next member's Align absorbs any gap and Bytes pads a terminal wstring.
func (e *Encoder) encodeWString(s string) error {
	units := utf16.Encode([]rune(s))
	err := e.Align(4)
	if err != nil {
		return err
	}
	total := len(units) + 1
	var lenBytes [4]byte
	e.order.PutUint32(lenBytes[:], uint32(total)) //nolint:gosec // G115: total is a non-negative length guarded by callers
	e.buf = append(e.buf, lenBytes[:]...)
	for _, u := range units {
		var tmp [2]byte
		e.order.PutUint16(tmp[:], u)
		e.buf = append(e.buf, tmp[:]...)
	}
	e.buf = append(e.buf, nulByte, nulByte)
	return nil
}

// WriteWString writes an IDL wstring as length plus units plus NUL.
//
// The input Go string is encoded to UTF-16. The length prefix counts
// UTF-16 code units including the trailing NUL. No trailing pad is
// written.
//
// This is the unbounded write: it does not enforce maxWStringUnits. An
// over cap value encodes without error but the decoder rejects it with a
// drop. It does guard uint32 overflow on the unit count. Use
// WriteWStringChecked to enforce the cap at encode time.
//
// Tolerant policy: interior zero units are preserved and only the final
// unit is required to be zero on decode. Unpaired surrogates on decode
// map to U+FFFD via utf16.Decode.
func (e *Encoder) WriteWString(s string) error {
	units := utf16.Encode([]rune(s))
	if uint64(len(units))+1 > uint64(math.MaxUint32) {
		return errors.New("cdr: wstring length overflows uint32") //nolint:wrapcheck // new error, nothing to wrap
	}
	err := e.encodeWString(s)
	if err != nil {
		return fmt.Errorf("cdr: encode wstring: %w", err)
	}
	return nil
}

// WriteWStringChecked writes a wstring and enforces the decoder cap.
//
// It returns an error without writing when the unit count including NUL
// exceeds maxWStringUnits or overflows uint32.
func (e *Encoder) WriteWStringChecked(s string) error {
	units := utf16.Encode([]rune(s))
	if uint64(len(units))+1 > uint64(math.MaxUint32) {
		return errors.New("cdr: wstring length overflows uint32") //nolint:wrapcheck // new error, nothing to wrap
	}
	if len(units)+1 > maxWStringUnits {
		return fmt.Errorf("cdr: wstring length %d exceeds cap %d", len(units)+1, maxWStringUnits)
	}
	err := e.encodeWString(s)
	if err != nil {
		return fmt.Errorf("cdr: encode wstring: %w", err)
	}
	return nil
}

// WriteBoundedWString writes a wstring checked against a bound.
//
// bound is the maximum UTF-16 code unit count excluding NUL per
// DDS-XTypes 7.2.3.1.2. Zero means unbounded. A negative bound returns
// an error and leaves the buffer unchanged. Overlong input returns an
// error without writing; this strict path never truncates. For the
// explicit truncate policy use WriteBoundedWStringTruncate. Even bounded
// writes may exceed the decoder cap when the bound is large. Overflow
// past MaxUint32 returns an error without writing.
func (e *Encoder) WriteBoundedWString(s string, bound int) (bool, error) {
	if bound < 0 {
		return false, fmt.Errorf("cdr: negative wstring bound %d", bound)
	}
	if bound == 0 {
		err := e.WriteWString(s)
		if err != nil {
			return false, err
		}
		return false, nil
	}
	if unitsOfxt6bWString(s) > bound {
		return false, fmt.Errorf("cdr: wstring units %d exceeds bound %d", unitsOfxt6bWString(s), bound)
	}
	err := e.WriteWString(s)
	if err != nil {
		return false, err
	}
	return false, nil
}

// WriteBoundedWStringTruncate writes a wstring with truncate policy.
//
// bound is the maximum UTF-16 code unit count excluding NUL. Overlong
// input is truncated on a rune boundary to fit bound units, encoded,
// and reported with truncated true. Truncation never splits a surrogate
// pair. This is the explicit non-strict helper; strict paths use
// WriteBoundedWString and fail instead of truncating.
func (e *Encoder) WriteBoundedWStringTruncate(s string, bound int) (bool, error) {
	if bound < 0 {
		return false, fmt.Errorf("cdr: negative wstring bound %d", bound)
	}
	if bound == 0 {
		err := e.WriteWString(s)
		if err != nil {
			return false, err
		}
		return false, nil
	}
	if unitsOfxt6bWString(s) > bound {
		truncated := truncatext6bWStringToUnits(s, bound)
		units := utf16.Encode([]rune(truncated))
		if uint64(len(units))+1 > uint64(math.MaxUint32) {
			return false, errors.New("cdr: wstring length overflows uint32") //nolint:wrapcheck // new error, nothing to wrap
		}
		err := e.encodeWString(truncated)
		if err != nil {
			return false, fmt.Errorf("cdr: encode wstring: %w", err)
		}
		return true, nil
	}
	err := e.WriteWString(s)
	if err != nil {
		return false, err
	}
	return false, nil
}

// WriteWStringxt6bBoundedStrict writes a wstring and rejects over bound input.
//
// bound counts UTF-16 code units excluding NUL per DDS-XTypes 7.2.3.1.2.
// Zero means unbounded. Overlong input returns an error without writing.
func (e *Encoder) WriteWStringxt6bBoundedStrict(s string, bound int) error {
	if bound < 0 {
		return fmt.Errorf("cdr: negative wstring bound %d", bound)
	}
	if bound > 0 && unitsOfxt6bWString(s) > bound {
		return fmt.Errorf("cdr: wstring units %d exceeds bound %d", unitsOfxt6bWString(s), bound)
	}
	err := e.WriteWString(s)
	if err != nil {
		return err
	}
	return nil
}

// WriteSequenceLength writes an unbounded sequence length prefix.
//
// Callers write each element with the matching Write method right after
// this call. The prefix is a uint32 with 4 byte alignment.
//
// This is the unbounded write: it does not enforce maxSequenceElements.
// An over cap length encodes without error but the decoder rejects it
// with a drop. Negative n returns an error and writes nothing. Lengths
// past MaxUint32 return an overflow error without writing. Use
// WriteSequenceLengthChecked to enforce the cap at encode time.
func (e *Encoder) WriteSequenceLength(n int) error {
	if n < 0 {
		return fmt.Errorf("cdr: negative sequence length %d", n)
	}
	if uint64(n) > uint64(math.MaxUint32) {
		return fmt.Errorf("cdr: sequence length %d overflows uint32", n)
	}
	err := e.putUint32(uint32(n))
	if err != nil {
		return fmt.Errorf("cdr: encode sequence length: %w", err)
	}
	return nil
}

// WriteSequenceLengthChecked writes a sequence length with cap enforcement.
//
// It returns an error without writing when n is negative, overflows
// uint32, or exceeds maxSequenceElements.
func (e *Encoder) WriteSequenceLengthChecked(n int) error {
	if n < 0 {
		return fmt.Errorf("cdr: negative sequence length %d", n)
	}
	if uint64(n) > uint64(math.MaxUint32) {
		return fmt.Errorf("cdr: sequence length %d overflows uint32", n)
	}
	if n > maxSequenceElements {
		return fmt.Errorf("cdr: sequence length %d exceeds cap %d", n, maxSequenceElements)
	}
	err := e.putUint32(uint32(n))
	if err != nil {
		return fmt.Errorf("cdr: encode sequence length: %w", err)
	}
	return nil
}

// WriteBoundedSequenceLength writes a sequence length checked by bound.
//
// bound 0 means unbounded and follows WriteSequenceLength without a bound
// check. A negative bound returns an error and leaves the buffer
// unchanged. A negative n returns an error and writes nothing. When a
// positive bound is set and n exceeds it, an error is returned and
// nothing is written. Overflow past MaxUint32 returns an error without
// writing. This is the bounded sequence helper for IDL bounded sequences.
func (e *Encoder) WriteBoundedSequenceLength(n, bound int) error {
	if bound < 0 {
		return fmt.Errorf("cdr: negative sequence bound %d", bound)
	}
	if n < 0 {
		return fmt.Errorf("cdr: negative sequence length %d", n)
	}
	if uint64(n) > uint64(math.MaxUint32) {
		return fmt.Errorf("cdr: sequence length %d overflows uint32", n)
	}
	if bound > 0 && n > bound {
		return fmt.Errorf("cdr: sequence length %d exceeds bound %d", n, bound)
	}
	err := e.putUint32(uint32(n))
	if err != nil {
		return fmt.Errorf("cdr: encode sequence length: %w", err)
	}
	return nil
}

// WriteOctetSequence writes a uint8 sequence with a length prefix.
//
// The bytes are appended in bulk. No per element padding is needed.
// This is the unbounded write: it does not enforce maxSequenceElements.
// Over cap input encodes but the decoder rejects it with a drop.
// Overflow past MaxUint32 returns an error without writing. Use
// WriteOctetSequenceChecked to enforce the cap at encode time.
func (e *Encoder) WriteOctetSequence(v []byte) error {
	if v == nil {
		v = []byte{}
	}
	if uint64(len(v)) > uint64(math.MaxUint32) {
		return fmt.Errorf("cdr: octet sequence length %d overflows uint32", len(v))
	}
	err := e.WriteSequenceLength(len(v))
	if err != nil {
		return err
	}
	e.buf = append(e.buf, v...)
	return nil
}

// WriteOctetSequenceChecked writes an octet sequence with cap enforcement.
//
// It returns an error without writing when the length overflows uint32
// or exceeds maxSequenceElements.
func (e *Encoder) WriteOctetSequenceChecked(v []byte) error {
	if v == nil {
		v = []byte{}
	}
	if uint64(len(v)) > uint64(math.MaxUint32) {
		return fmt.Errorf("cdr: octet sequence length %d overflows uint32", len(v))
	}
	if len(v) > maxSequenceElements {
		return fmt.Errorf("cdr: octet sequence length %d exceeds cap %d", len(v), maxSequenceElements)
	}
	err := e.WriteSequenceLength(len(v))
	if err != nil {
		return err
	}
	e.buf = append(e.buf, v...)
	return nil
}

// WriteServiceHeader appends the 8 byte GUID plus 8 byte sequence.
//
// Call this once immediately after NewEncoder and before user fields,
// when the buffer holds only the 4 byte header. The 8+8 service header
// is packed outside the CDR alignment domain on purpose: only Align(4)
// is applied so GUID at 4..12 and sequence at 12..20 stay contiguous with
// no Align(8) pad. User payload is rebased at offset 20. The GUID is the
// opaque 8 byte rmw request id echoed verbatim in the reply and never
// byte swapped.
//
// A service encoder never emits the empty dummy byte: ROS sends no dummy
// for empty service structs, so Bytes skips the dummy once the service
// header is present.
//
// Misuse: calling twice or after user fields returns an error and leaves
// the buffer unchanged. The guard checks payloadStart against HeaderSize
// and buffer length against HeaderSize.
func (e *Encoder) WriteServiceHeader(guid [8]byte, seq uint64) error {
	if e.payloadStart != HeaderSize {
		return errors.New("cdr: service header already written") //nolint:wrapcheck // new error, nothing to wrap
	}
	if len(e.buf) != HeaderSize {
		return errors.New("cdr: service header requires empty encoder") //nolint:wrapcheck // new error, nothing to wrap
	}
	e.buf = append(e.buf, guid[:]...)
	err := e.Align(4)
	if err != nil {
		return fmt.Errorf("cdr: service header align: %w", err)
	}
	tmp := encodeServiceSequence(seq, e.le)
	e.buf = append(e.buf, tmp[:]...)
	e.payloadStart = ServiceEnvelopeSize
	e.noDummy = true
	return nil
}

// Decoder reads one XCDR sample including its 4 byte header.
//
// All reads bounds check the buffer, count failures in drops, and
// return errors instead of panicking. The private counter is separate
// from envelope Metrics because Decoder owns streaming field reads while
// Metrics is caller owned for opaque prefix parsing. Final XCDR1 wires
// keep 8 byte alignment; XCDR2 wires parsed from V2 headers cap at 4.
type Decoder struct {
	data  []byte
	off   int
	order binary.ByteOrder
	le    bool
	// payloadStart is the user payload offset that alignment is relative
	// to. It is HeaderSize normally and ServiceEnvelopeSize after a
	// service header, mirroring Encoder so both sides share one origin.
	payloadStart int
	// drops counts truncated, empty, and malformed inputs seen by this decoder.
	drops uint64
	// ext is the extensibility kind from the encapsulation header.
	ext Extensibility
	// ver is the XCDR version from the encapsulation header.
	ver EncodingVersion
}

// NewDecoder validates the header and returns a decoder at offset 4.
//
// Header validation accepts all Table 57 XCDR IDs via
// ParseEncapsulationxt6b so V1 and V2 wires decode. The envelope error
// text is mapped to the cdr prefix here so validation logic has a
// single source. It rejects empty buffers, buffers shorter than 4 bytes,
// and unknown IDs. Reserved bytes 2 and 3 are ignored. Failures count
// one drop. The version and extensibility are retained so 8 byte reads
// cap at 4 under V2 while V1 stays byte identical.
func NewDecoder(data []byte) (*Decoder, error) {
	if len(data) < HeaderSize {
		d := &Decoder{drops: 1, payloadStart: HeaderSize, ext: ExtFinal, ver: XCDRVersion1}
		return d, fmt.Errorf("cdr: buffer too short for header: %d bytes: %w", len(data), ErrTruncated)
	}
	var hdr [4]byte
	copy(hdr[:], data[:HeaderSize])
	parsed, err := ParseEncapsulationxt6b(hdr)
	if err != nil {
		d := &Decoder{drops: 1, payloadStart: HeaderSize, ext: ExtFinal, ver: XCDRVersion1}
		msg := strings.TrimPrefix(err.Error(), "envelope: ")
		return d, fmt.Errorf("cdr: %s: %w", msg, ErrInvalid)
	}
	d := &Decoder{data: data, off: HeaderSize, le: parsed.LittleEndian, payloadStart: HeaderSize, ext: parsed.Extensibility, ver: parsed.Version}
	if parsed.LittleEndian {
		d.order = binary.LittleEndian
	} else {
		d.order = binary.BigEndian
	}
	return d, nil
}

// Extensibility reports the kind from the encapsulation header.
func (d *Decoder) Extensibility() Extensibility {
	return d.ext
}

// Version reports the XCDR version from the encapsulation header.
func (d *Decoder) Version() EncodingVersion {
	return d.ver
}

// alignWidth aligns with the XCDR version cap.
//
// V1 aligns to the full width; V2 caps at 4 per 7.4.2.
func (d *Decoder) alignWidth(width int) error {
	need := width
	if d.ver == XCDRVersion2 && need > MaxAlignxt6bV2 {
		need = MaxAlignxt6bV2
	}
	return d.Align(need)
}

// LittleEndian reports the negotiated byte order for this decoder.
func (d *Decoder) LittleEndian() bool {
	return d.le
}

// Offset returns the current read offset including the header.
func (d *Decoder) Offset() int {
	return d.off
}

// Remaining returns the number of unread bytes.
func (d *Decoder) Remaining() int {
	return len(d.data) - d.off
}

// Dropped returns the count of failed reads so far.
func (d *Decoder) Dropped() uint64 {
	return d.drops
}

// Align advances the offset to a multiple of n.
//
// It returns an error and counts a drop when the padding runs past the
// end of the buffer. An invalid n is a caller bug and returns an error
// without counting a drop. Alignment is relative to the user payload
// start, matching Encoder.Align.
func (d *Decoder) Align(n int) error {
	if n != 1 && n != 2 && n != 4 && n != 8 {
		return fmt.Errorf("cdr: invalid alignment %d: %w", n, ErrInvalid)
	}
	pad := alignPad(d.off-d.payloadStart, n)
	if d.off+pad > len(d.data) {
		d.drops++
		return fmt.Errorf("cdr: truncated align pad need %d have %d: %w", pad, len(d.data)-d.off, ErrTruncated)
	}
	d.off += pad
	return nil
}

// need checks that n bytes are available from the current offset.
//
// The n less than 0 branch is defensive and unreachable from typed reads
// because all lengths come from uint32 prefixes converted to non negative
// ints. It is kept so a future caller passing a negative size fails
// closed with a drop instead of panicking on slicing.
func (d *Decoder) need(n int) error {
	if n < 0 {
		d.drops++
		return fmt.Errorf("cdr: negative read size %d: %w", n, ErrInvalid)
	}
	if d.off+n > len(d.data) {
		d.drops++
		return fmt.Errorf("cdr: truncated need %d have %d: %w", n, len(d.data)-d.off, ErrTruncated)
	}
	return nil
}

// readByte reads one raw byte.
func (d *Decoder) readByte() (byte, error) {
	err := d.need(1)
	if err != nil {
		return 0, err
	}
	v := d.data[d.off]
	d.off++
	return v, nil
}

// readUint16 reads a 2 byte value with 2 byte alignment.
func (d *Decoder) readUint16() (uint16, error) {
	err := d.Align(2)
	if err != nil {
		return 0, err
	}
	err = d.need(2)
	if err != nil {
		return 0, err
	}
	v := d.order.Uint16(d.data[d.off : d.off+2])
	d.off += 2
	return v, nil
}

// readUint32 reads a 4 byte value with 4 byte alignment.
func (d *Decoder) readUint32() (uint32, error) {
	err := d.Align(4)
	if err != nil {
		return 0, err
	}
	err = d.need(4)
	if err != nil {
		return 0, err
	}
	v := d.order.Uint32(d.data[d.off : d.off+4])
	d.off += 4
	return v, nil
}

// readUint64 reads an 8 byte value with version capped alignment.
//
// V1 aligns to 8; V2 caps at 4 per 7.4.2. Final V1 wires stay identical.
func (d *Decoder) readUint64() (uint64, error) {
	err := d.alignWidth(8)
	if err != nil {
		return 0, err
	}
	err = d.need(8)
	if err != nil {
		return 0, err
	}
	v := d.order.Uint64(d.data[d.off : d.off+8])
	d.off += 8
	return v, nil
}

// ReadUint8 reads an octet or uint8.
func (d *Decoder) ReadUint8() (uint8, error) {
	v, err := d.readByte()
	if err != nil {
		return 0, err
	}
	return v, nil
}

// ReadInt8 reads a signed 8 bit value.
func (d *Decoder) ReadInt8() (int8, error) {
	v, err := d.readByte()
	if err != nil {
		return 0, err
	}
	return int8(v), nil //nolint:gosec // G115: intentional two's-complement reinterpret for XCDR1 encoding
}

// ReadChar reads an IDL char as signed 8 bit.
func (d *Decoder) ReadChar() (int8, error) {
	v, err := d.readByte()
	if err != nil {
		return 0, err
	}
	return int8(v), nil //nolint:gosec // G115: intentional two's-complement reinterpret for XCDR1 encoding
}

// ReadBool reads an IDL bool byte and requires 0 or 1.
func (d *Decoder) ReadBool() (bool, error) {
	v, err := d.readByte()
	if err != nil {
		return false, err
	}
	if v == boolFalseByte {
		return false, nil
	}
	if v == boolTrueByte {
		return true, nil
	}
	d.drops++
	return false, fmt.Errorf("cdr: invalid bool byte 0x%02x: %w", v, ErrInvalid)
}

// ReadWchar reads an IDL wchar as 2 bytes.
func (d *Decoder) ReadWchar() (uint16, error) {
	v, err := d.readUint16()
	if err != nil {
		return 0, err
	}
	return v, nil
}

// ReadInt16 reads a 2 byte signed value.
func (d *Decoder) ReadInt16() (int16, error) {
	v, err := d.readUint16()
	if err != nil {
		return 0, err
	}
	return int16(v), nil //nolint:gosec // G115: intentional two's-complement reinterpret for XCDR1 encoding
}

// ReadUint16 reads a 2 byte unsigned value.
func (d *Decoder) ReadUint16() (uint16, error) {
	v, err := d.readUint16()
	if err != nil {
		return 0, err
	}
	return v, nil
}

// ReadInt32 reads a 4 byte signed value.
func (d *Decoder) ReadInt32() (int32, error) {
	v, err := d.readUint32()
	if err != nil {
		return 0, err
	}
	return int32(v), nil //nolint:gosec // G115: intentional two's-complement reinterpret for XCDR1 encoding
}

// ReadUint32 reads a 4 byte unsigned value.
func (d *Decoder) ReadUint32() (uint32, error) {
	v, err := d.readUint32()
	if err != nil {
		return 0, err
	}
	return v, nil
}

// ReadInt64 reads an 8 byte signed value.
func (d *Decoder) ReadInt64() (int64, error) {
	v, err := d.readUint64()
	if err != nil {
		return 0, err
	}
	return int64(v), nil //nolint:gosec // G115: intentional two's-complement reinterpret for XCDR1 encoding
}

// ReadUint64 reads an 8 byte unsigned value.
func (d *Decoder) ReadUint64() (uint64, error) {
	v, err := d.readUint64()
	if err != nil {
		return 0, err
	}
	return v, nil
}

// ReadFloat32 reads a 4 byte float.
func (d *Decoder) ReadFloat32() (float32, error) {
	v, err := d.readUint32()
	if err != nil {
		return 0, err
	}
	return math.Float32frombits(v), nil
}

// ReadFloat64 reads an 8 byte float.
func (d *Decoder) ReadFloat64() (float64, error) {
	v, err := d.readUint64()
	if err != nil {
		return 0, err
	}
	return math.Float64frombits(v), nil
}

// ReadString reads an IDL string with length plus NUL.
//
// The length prefix includes the NUL. The trailing NUL is checked and
// stripped. No string pad is skipped: the next member's Align absorbs any
// gap. See encodeString for pad ownership.
//
// Tolerant policy: interior NUL bytes are preserved as payload and do
// not terminate the string. Invalid UTF-8 bytes are preserved as is in
// the returned Go string.
func (d *Decoder) ReadString() (string, error) {
	return d.readStringWithBound(0)
}

// ReadBoundedString reads a string checked against a bound.
//
// bound is the maximum octet count excluding NUL. Zero means unbounded.
// A negative bound counts a drop and returns an error. Overlong input
// counts a drop and returns an error.
func (d *Decoder) ReadBoundedString(bound int) (string, error) {
	return d.readStringWithBound(bound)
}

// readStringWithBound decodes one length prefixed NUL terminated string.
func (d *Decoder) readStringWithBound(bound int) (string, error) {
	if bound < 0 {
		d.drops++
		return "", fmt.Errorf("cdr: negative string bound %d: %w", bound, ErrInvalid)
	}
	length, err := d.readUint32()
	if err != nil {
		return "", err
	}
	if length == 0 {
		d.drops++
		return "", fmt.Errorf("cdr: string length zero misses NUL: %w", ErrInvalid)
	}
	if uint64(length) > uint64(maxStringBytes) {
		d.drops++
		return "", fmt.Errorf("cdr: string length %d exceeds cap %d: %w", length, maxStringBytes, ErrInvalid)
	}
	n := int(length)
	err = d.need(n)
	if err != nil {
		return "", err
	}
	raw := d.data[d.off : d.off+n]
	d.off += n
	if raw[n-1] != nulByte {
		d.drops++
		return "", fmt.Errorf("cdr: string misses NUL terminator: %w", ErrInvalid)
	}
	s := string(raw[:n-1])
	// Bound enforcement goes through the 7.2.2.8 try-construct policy
	// with DISCARD so struct decode and this helper share one rule:
	// over-bound input discards the sample fail-closed.
	_, discard, err := ApplyTryConstructString(TryConstructDiscard, s, bound)
	if err != nil {
		d.drops++
		return "", fmt.Errorf("cdr: string bound: %w", err)
	}
	if discard {
		d.drops++
		return "", fmt.Errorf("cdr: string octets %d exceeds bound %d: %w", len(s), bound, ErrInvalid)
	}
	return s, nil
}

// ReadWString reads an IDL wstring and decodes UTF-16 to a Go string.
//
// Unpaired surrogates decode to U+FFFD via utf16.Decode. Interior zero
// units are preserved; only the final unit must be zero.
func (d *Decoder) ReadWString() (string, error) {
	return d.readWStringWithBound(0)
}

// ReadBoundedWString reads a wstring checked against a bound.
//
// bound is the maximum UTF-16 code unit count excluding NUL. Zero means
// unbounded. A negative bound counts a drop and returns an error.
func (d *Decoder) ReadBoundedWString(bound int) (string, error) {
	return d.readWStringWithBound(bound)
}

// readWStringWithBound decodes one length prefixed NUL terminated wstring.
func (d *Decoder) readWStringWithBound(bound int) (string, error) {
	if bound < 0 {
		d.drops++
		return "", fmt.Errorf("cdr: negative wstring bound %d: %w", bound, ErrInvalid)
	}
	length, err := d.readUint32()
	if err != nil {
		return "", err
	}
	if length == 0 {
		d.drops++
		return "", fmt.Errorf("cdr: wstring length zero misses NUL: %w", ErrInvalid)
	}
	if uint64(length) > uint64(maxWStringUnits) {
		d.drops++
		return "", fmt.Errorf("cdr: wstring length %d exceeds cap %d: %w", length, maxWStringUnits, ErrInvalid)
	}
	n := int(length) * 2
	err = d.need(n)
	if err != nil {
		return "", err
	}
	units := make([]uint16, length)
	for i := range units {
		units[i] = d.order.Uint16(d.data[d.off+i*2 : d.off+i*2+2])
	}
	d.off += n
	if units[len(units)-1] != 0 {
		d.drops++
		return "", fmt.Errorf("cdr: wstring misses NUL terminator: %w", ErrInvalid)
	}
	s := string(utf16.Decode(units[:len(units)-1]))
	if bound > 0 && int(length)-1 > bound {
		d.drops++
		return "", fmt.Errorf("cdr: wstring units %d exceeds bound %d: %w", int(length)-1, bound, ErrInvalid)
	}
	return s, nil
}

// ReadSequenceLength reads an unbounded sequence length prefix.
//
// The caller then reads that many elements. Oversized or truncated
// lengths count a drop and return an error without allocating.
func (d *Decoder) ReadSequenceLength() (int, error) {
	length, err := d.readUint32()
	if err != nil {
		return 0, err
	}
	if uint64(length) > uint64(maxSequenceElements) {
		d.drops++
		return 0, fmt.Errorf("cdr: sequence length %d exceeds cap %d: %w", length, maxSequenceElements, ErrInvalid)
	}
	return int(length), nil
}

// ReadBoundedSequenceLength reads a sequence length checked by bound.
//
// bound 0 means unbounded and follows ReadSequenceLength. A negative
// bound counts a drop and returns an error. Overlong counts go through
// the 7.2.2.8 try-construct policy with DISCARD so struct decode and
// this helper share one rule: over-bound input discards the sample
// fail-closed without allocating.
func (d *Decoder) ReadBoundedSequenceLength(bound int) (int, error) {
	if bound < 0 {
		d.drops++
		return 0, fmt.Errorf("cdr: negative sequence bound %d: %w", bound, ErrInvalid)
	}
	n, err := d.ReadSequenceLength()
	if err != nil {
		return 0, err
	}
	_, discard, err := ApplyTryConstructSequence(TryConstructDiscard, n, bound)
	if err != nil {
		d.drops++
		return 0, fmt.Errorf("cdr: sequence bound: %w", err)
	}
	if discard {
		d.drops++
		return 0, fmt.Errorf("cdr: sequence length %d exceeds bound %d: %w", n, bound, ErrInvalid)
	}
	return n, nil
}

// CheckSequenceAllocation verifies that n elements of elemSize bytes fit
// before the caller allocates with make or reflect.MakeSlice.
//
// It enforces two layers: the 32M element count cap plus remaining wire
// bytes for wide element types. A negative n or non positive elemSize is
// a caller bug and returns an error without counting a drop. A count above
// the cap counts a drop with ErrInvalid. A byte total above Remaining
// counts a drop with ErrTruncated. Callers must invoke this after
// ReadSequenceLength and before allocation so a corrupt prefix with a
// large count but short wire fails without OOM.
func (d *Decoder) CheckSequenceAllocation(n, elemSize int) error {
	if n < 0 {
		return fmt.Errorf("cdr: negative sequence allocation %d: %w", n, ErrInvalid)
	}
	if elemSize <= 0 {
		return fmt.Errorf("cdr: invalid element size %d: %w", elemSize, ErrInvalid)
	}
	if uint64(n) > uint64(maxSequenceElements) {
		d.drops++
		return fmt.Errorf("cdr: sequence length %d exceeds cap %d: %w", n, maxSequenceElements, ErrInvalid)
	}
	total := uint64(n) * uint64(elemSize)
	if total > uint64(d.Remaining()) { //nolint:gosec // G115: Remaining is len minus off and non-negative
		d.drops++
		return fmt.Errorf("cdr: sequence needs %d bytes have %d: %w", total, d.Remaining(), ErrTruncated)
	}
	return nil
}

// ReadOctetSequence reads a uint8 sequence with a length prefix.
func (d *Decoder) ReadOctetSequence() ([]byte, error) {
	length, err := d.ReadSequenceLength()
	if err != nil {
		return nil, err
	}
	err = d.need(length)
	if err != nil {
		return nil, err
	}
	out := make([]byte, length)
	copy(out, d.data[d.off:d.off+length])
	d.off += length
	return out, nil
}

// ReadServiceHeader reads the 8 byte GUID plus 8 byte sequence.
//
// Call this once right after NewDecoder for service samples, before user
// fields, when Offset equals HeaderSize. The 8+8 header is packed outside
// the CDR alignment domain: GUID at 4..12 and sequence at 12..20 stay
// contiguous with no Align(8) pad, so user payload is rebased at offset
// 20. The pad here uses the relative origin (off minus payloadStart)
// shared with Encoder.Align; HeaderSize is 4, a multiple of 4, so the
// relative and absolute forms agree and the wire stays identical. The
// sequence uses the negotiated byte order via
// decodeServiceSequence. The GUID is the opaque 8 byte rmw request id
// echoed verbatim in the reply.
//
// Calling when Offset is not HeaderSize counts a drop and returns an
// error without advancing.
func (d *Decoder) ReadServiceHeader() ([8]byte, uint64, error) {
	var guid [8]byte
	if d.off != HeaderSize {
		d.drops++
		return guid, 0, fmt.Errorf("cdr: service header requires offset %d, have %d: %w", HeaderSize, d.off, ErrInvalid)
	}
	err := d.need(ServiceHeaderSize)
	if err != nil {
		return guid, 0, err
	}
	copy(guid[:], d.data[d.off:d.off+ServiceGUIDSize])
	d.off += ServiceGUIDSize
	pad := alignPad(d.off-d.payloadStart, 4)
	if d.off+pad+ServiceSequenceSize > len(d.data) {
		d.drops++
		return guid, 0, fmt.Errorf("cdr: truncated service sequence: %w", ErrTruncated)
	}
	d.off += pad
	seq := decodeServiceSequence(d.data[d.off:d.off+ServiceSequenceSize], d.le)
	d.off += ServiceSequenceSize
	d.payloadStart = ServiceEnvelopeSize
	return guid, seq, nil
}

// Finish reports trailing bytes as an extra field drop.
//
// Call after the expected fields are read. Exact consumption returns nil.
// XCDR1 final zero pad to a 4 byte boundary is accepted: Encoder.Bytes
// pads the wire, so a message ending with a 1 byte bool like PointCloud2
// is_dense leaves up to 3 zero bytes that are not trailing garbage.
// Those bytes must be all zero and must equal the pad needed from the
// consumed offset relative to the user payload start. A plain empty
// message with no fields read carries one dummy zero byte plus pad from
// Encoder.Bytes and MarshalMessage; Finish consumes that dummy plus pad
// as success so plain empty payloads round trip. Service empty messages
// carry no dummy by ROS convention (WriteServiceHeader sets noDummy and
// MarshalServiceMessage emits no dummy), so the dummy path is gated on
// the plain origin and the service path stays byte-identical. Any other
// remainder counts a drop and returns an error. Opaque UnmarshalMessage
// views require this step for strict validation because they do not check
// trailing garbage on their own.
func (d *Decoder) Finish() error {
	remaining := len(d.data) - d.off
	if remaining == 0 {
		return nil
	}
	if d.tryConsumeEmptyDummy(remaining) {
		return nil
	}
	expected := alignPad(d.off-d.payloadStart, 4)
	if remaining == expected && isZeroBytes(d.data[d.off:]) {
		d.off = len(d.data)
		return nil
	}
	d.drops++
	return fmt.Errorf("cdr: %d trailing bytes: %w", remaining, ErrInvalid)
}

// tryConsumeEmptyDummy consumes the plain empty dummy plus pad on success.
// It applies only when no user bytes were read on the plain origin; the
// service origin stays byte-identical with no dummy. It reports whether it
// consumed.
func (d *Decoder) tryConsumeEmptyDummy(remaining int) bool {
	if d.off != d.payloadStart || d.payloadStart != HeaderSize {
		return false
	}
	want := 1 + alignPad(d.off+1, 4)
	if remaining != want {
		return false
	}
	if !isZeroBytes(d.data[d.off:]) {
		return false
	}
	d.off = len(d.data)
	return true
}

// isZeroBytes reports whether all bytes are zero.
func isZeroBytes(b []byte) bool {
	for _, v := range b {
		if v != nulByte {
			return false
		}
	}
	return true
}

// int64Fromxt6bAny converts signed integer values for key comparison.
func int64Fromxt6bAny(v any) (int64, error) {
	switch x := v.(type) {
	case int:
		return int64(x), nil
	case int8:
		return int64(x), nil
	case int16:
		return int64(x), nil
	case int32:
		return int64(x), nil
	case int64:
		return x, nil
	default:
		return 0, fmt.Errorf("types: map key %T not signed integer: %w", v, ErrInvalid)
	}
}

// uint64Fromxt6bAny converts unsigned integer values for key comparison.
func uint64Fromxt6bAny(v any) (uint64, error) {
	switch x := v.(type) {
	case uint:
		return uint64(x), nil
	case uint8:
		return uint64(x), nil
	case uint16:
		return uint64(x), nil
	case uint32:
		return uint64(x), nil
	case uint64:
		return x, nil
	default:
		return 0, fmt.Errorf("types: map key %T not unsigned integer: %w", v, ErrInvalid)
	}
}

// stringFromxt6bAny converts string values for key comparison.
func stringFromxt6bAny(v any) (string, error) {
	s, ok := v.(string)
	if !ok {
		return "", fmt.Errorf("types: map key %T not string: %w", v, ErrInvalid)
	}
	return s, nil
}

// comparext6bSignedKeys compares signed keys and returns -1, 0, or 1.
func comparext6bSignedKeys(a, b any) (int, error) {
	av, err := int64Fromxt6bAny(a)
	if err != nil {
		return 0, err
	}
	bv, err := int64Fromxt6bAny(b)
	if err != nil {
		return 0, err
	}
	if av < bv {
		return -1, nil
	}
	if av > bv {
		return 1, nil
	}
	return 0, nil
}

// comparext6bUnsignedKeys compares unsigned keys and returns -1, 0, or 1.
func comparext6bUnsignedKeys(a, b any) (int, error) {
	av, err := uint64Fromxt6bAny(a)
	if err != nil {
		return 0, err
	}
	bv, err := uint64Fromxt6bAny(b)
	if err != nil {
		return 0, err
	}
	if av < bv {
		return -1, nil
	}
	if av > bv {
		return 1, nil
	}
	return 0, nil
}

// comparext6bStringKeys compares string keys by bytes and returns -1, 0, or 1.
func comparext6bStringKeys(a, b any) (int, error) {
	av, err := stringFromxt6bAny(a)
	if err != nil {
		return 0, err
	}
	bv, err := stringFromxt6bAny(b)
	if err != nil {
		return 0, err
	}
	if av < bv {
		return -1, nil
	}
	if av > bv {
		return 1, nil
	}
	return 0, nil
}

// isxt6bSignedKeyKind reports signed integer key kinds.
func isxt6bSignedKeyKind(kind RosTypeID) bool {
	return kind == RosTypeInt8 || kind == RosTypeChar || kind == RosTypeInt16 || kind == RosTypeInt32 || kind == RosTypeInt64
}

// isxt6bUnsignedKeyKind reports unsigned integer key kinds.
func isxt6bUnsignedKeyKind(kind RosTypeID) bool {
	return kind == RosTypeUint8 || kind == RosTypeOctet || kind == RosTypeUint16 || kind == RosTypeUint32 || kind == RosTypeUint64
}

// isxt6bStringKeyKind reports narrow and wide string key kinds.
func isxt6bStringKeyKind(kind RosTypeID) bool {
	return kind == RosTypeString || kind == RosTypeWString
}

// Comparext6bMapKey compares two map keys of one IDL kind.
//
// Supported kinds are signed and unsigned integers plus narrow and wide
// strings per DDS-XTypes 7.2.3.2.2. Narrow and wide strings both use Go
// string values and compare by bytes. Returns -1, 0, or 1. Other kinds
// and mismatched Go values return an error.
func Comparext6bMapKey(kind RosTypeID, a, b any) (int, error) {
	if isxt6bSignedKeyKind(kind) {
		v, err := comparext6bSignedKeys(a, b)
		if err != nil {
			return 0, err
		}
		return v, nil
	}
	if isxt6bUnsignedKeyKind(kind) {
		v, err := comparext6bUnsignedKeys(a, b)
		if err != nil {
			return 0, err
		}
		return v, nil
	}
	if isxt6bStringKeyKind(kind) {
		v, err := comparext6bStringKeys(a, b)
		if err != nil {
			return 0, err
		}
		return v, nil
	}
	return 0, fmt.Errorf("types: unsupported map key kind %s: %w", kind.String(), ErrInvalid)
}

// CheckMapKeysxt6bOrdered verifies strictly increasing keys for encode.
//
// Each consecutive pair must compare less than zero. Equal keys report
// duplicates and greater keys report decreasing order. Empty and single
// element slices pass.
func CheckMapKeysxt6bOrdered(kind RosTypeID, keys []any) error {
	for i := 1; i < len(keys); i++ {
		cmp, err := Comparext6bMapKey(kind, keys[i-1], keys[i])
		if err != nil {
			return err
		}
		if cmp == 0 {
			return fmt.Errorf("types: map keys duplicate at %d: %w", i, ErrInvalid)
		}
		if cmp > 0 {
			return fmt.Errorf("types: map keys not ordered at %d: %w", i, ErrInvalid)
		}
	}
	return nil
}

// MaxAlignxt6bV1 is the V1 max alignment.
const MaxAlignxt6bV1 = 8

// MaxAlignxt6bV2 is the V2 max alignment.
const MaxAlignxt6bV2 = 4

// MaxAlignxt6bForVersion returns the max alignment for a version.
func MaxAlignxt6bForVersion(v EncodingVersion) int {
	if v == XCDRVersion2 {
		return MaxAlignxt6bV2
	}
	return MaxAlignxt6bV1
}

// XCDR2Alignxt6b maps a width to its V2 alignment.
func XCDR2Alignxt6b(width int) int {
	if width == 8 || width == 16 {
		return 4
	}
	return width
}

// Alignxt6bVersioned aligns with a version cap and rebased origin.
func (e *Encoder) Alignxt6bVersioned(n int, v EncodingVersion) error {
	err := v.Validate()
	if err != nil {
		return err
	}
	need := n
	lim := MaxAlignxt6bForVersion(v)
	if need > lim {
		need = lim
	}
	err = e.Align(need)
	if err != nil {
		return err
	}
	return nil
}

// Alignxt6bVersioned aligns the decoder with a version cap and rebased origin.
func (d *Decoder) Alignxt6bVersioned(n int, v EncodingVersion) error {
	err := v.Validate()
	if err != nil {
		return err
	}
	need := n
	lim := MaxAlignxt6bForVersion(v)
	if need > lim {
		need = lim
	}
	err = d.Align(need)
	if err != nil {
		return err
	}
	return nil
}

// CheckSupportxt6bForType reports long double as supported.
//
// The 16 byte long double is supported as opaque 16 bytes with V1
// alignment 8 and V2 alignment 4 via Alignxt6bVersioned. No ROS 2 usage
// exists but the codec round trips the bytes.
func CheckSupportxt6bForType(kind RosTypeID) error {
	switch kind {
	case RosTypeLongDbl, RosTypeFloat, RosTypeDouble, RosTypeString, RosTypeWString, RosTypeMessage,
		RosTypeBoolean, RosTypeChar, RosTypeWChar, RosTypeOctet, RosTypeUint8, RosTypeInt8,
		RosTypeUint16, RosTypeInt16, RosTypeUint32, RosTypeInt32, RosTypeUint64, RosTypeInt64:
		return nil
	default:
		return fmt.Errorf("types: unsupported kind %s: %w", kind.String(), ErrInvalid)
	}
}

// WriteStructxt6bDelimited frames a struct with a DHEADER delimiter.
func (e *Encoder) WriteStructxt6bDelimited(body func() error) error {
	at, err := e.WriteDHeaderPlaceholder()
	if err != nil {
		return err
	}
	if body != nil {
		err = body()
		if err != nil {
			return fmt.Errorf("types: delimited body: %w", err)
		}
	}
	err = e.FillDHeader(at)
	if err != nil {
		return err
	}
	return nil
}

// ReadStructxt6bDelimited reads a DHEADER then runs the body reader.
//
// The DHEADER is read via ReadDHeaderWithBound with an unbounded bound so
// every delimited struct decode enforces the appendable bound helper.
func (d *Decoder) ReadStructxt6bDelimited(body func(size uint32) error) error {
	return d.ReadStructxt6bDelimitedWithBound(0, body)
}

// ReadStructxt6bDelimitedWithBound reads a DHEADER with an appendable bound.
//
// Bound 0 means unbounded. Over-bound sizes fail via CheckAppendableBound
// so the caller drops the sample. Unknown trailing members must be skipped
// via SkipUnknownxt6bMember by the body reader.
func (d *Decoder) ReadStructxt6bDelimitedWithBound(bound uint32, body func(size uint32) error) error {
	size, err := d.ReadDHeaderWithBound(bound)
	if err != nil {
		return err
	}
	if body == nil {
		err = d.SkipBytes(uint64(size))
		if err != nil {
			return err
		}
		return nil
	}
	err = body(size)
	if err != nil {
		return fmt.Errorf("types: delimited body: %w", err)
	}
	return nil
}

// SkipUnknownxt6bMember skips or fails per MustUnderstand.
func SkipUnknownxt6bMember(d *Decoder, mustUnderstand bool, memberID uint32, size uint64) error {
	if mustUnderstand {
		err := CheckMustUnderstand(true, memberID)
		if err != nil {
			return err
		}
		return nil
	}
	err := d.SkipBytes(size)
	if err != nil {
		return err
	}
	return nil
}

// ReadMapLengthxt6bChecked reads map length then checks allocation.
func (d *Decoder) ReadMapLengthxt6bChecked(bound MapBound, elemSize int) (int, error) {
	n, err := d.ReadMapLength(bound)
	if err != nil {
		return 0, err
	}
	err = d.CheckSequenceAllocation(n, elemSize)
	if err != nil {
		return 0, err
	}
	return n, nil
}

// ReadSequenceLengthxt6bChecked reads sequence length then checks allocation.
func (d *Decoder) ReadSequenceLengthxt6bChecked(elemSize int) (int, error) {
	n, err := d.ReadSequenceLength()
	if err != nil {
		return 0, err
	}
	err = d.CheckSequenceAllocation(n, elemSize)
	if err != nil {
		return 0, err
	}
	return n, nil
}

// ReadBoundedSequenceLengthWithTryConstruct reads a bounded length with policy.
//
// The raw length is read via ReadSequenceLength, then ApplyTryConstructSequence
// enforces the reader bound: DISCARD reports discard true, USE_DEFAULT
// returns 0, TRIM returns the bound. Over-bound without policy would fail
// in ReadBoundedSequenceLength; this helper applies the 7.2.2.8 policy
// instead so struct/sequence decode never bypasses try-construct.
func (d *Decoder) ReadBoundedSequenceLengthWithTryConstruct(kind TryConstructKind, bound int) (int, bool, error) {
	n, err := d.ReadSequenceLength()
	if err != nil {
		return 0, false, err
	}
	applied, discard, err := ApplyTryConstructSequence(kind, n, bound)
	if err != nil {
		d.drops++
		return 0, false, err
	}
	if discard {
		return 0, true, nil
	}
	err = d.CheckSequenceAllocation(applied, 1)
	if err != nil {
		return 0, false, err
	}
	return applied, false, nil
}

// ReadBoundedStringWithTryConstruct reads a bounded string with policy.
//
// The raw string is read unbounded, then ApplyTryConstructString enforces
// the reader bound. DISCARD reports discard, USE_DEFAULT returns empty,
// TRIM returns the octet-truncated form.
func (d *Decoder) ReadBoundedStringWithTryConstruct(kind TryConstructKind, bound int) (string, bool, error) {
	s, err := d.ReadString()
	if err != nil {
		return "", false, err
	}
	return ApplyTryConstructString(kind, s, bound)
}

// LongDouble is the 16 byte IDL long double as opaque bytes.
//
// Go has no float128; the codec preserves the 16 octets with XCDR
// alignment 8 under V1 and 4 under V2 per 7.4.3.5.3 rule 22.
type LongDouble [16]byte

// WriteLongDouble writes a 16 byte long double with version capped alignment.
func (e *Encoder) WriteLongDouble(v LongDouble) error {
	err := e.alignWidth(8)
	if err != nil {
		return err
	}
	e.buf = append(e.buf, v[:]...)
	return nil
}

// ReadLongDouble reads a 16 byte long double with version capped alignment.
func (d *Decoder) ReadLongDouble() (LongDouble, error) {
	var out LongDouble
	err := d.alignWidth(8)
	if err != nil {
		return out, err
	}
	err = d.need(16)
	if err != nil {
		return out, err
	}
	copy(out[:], d.data[d.off:d.off+16])
	d.off += 16
	return out, nil
}

// validateFixedDecimal checks decimal fixed-point syntax.
//
// Accepted forms are optional minus, digits, and optional dot plus
// digits, per 7.2.3.4. Empty, plus sign, exponent, and interior spaces
// are rejected. At least one digit is required on either side.
func validateFixedDecimal(s string) error {
	if s == "" {
		return fmt.Errorf("cdr: fixed decimal empty: %w", ErrInvalid)
	}
	i := 0
	if s[0] == '-' {
		i = 1
		if len(s) == 1 {
			return fmt.Errorf("cdr: fixed decimal %q misses digits: %w", s, ErrInvalid)
		}
	}
	seenDigit := false
	seenDot := false
	for ; i < len(s); i++ {
		c := s[i]
		if c == '.' {
			if seenDot {
				return fmt.Errorf("cdr: fixed decimal %q has two dots: %w", s, ErrInvalid)
			}
			seenDot = true
			continue
		}
		if c < '0' || c > '9' {
			return fmt.Errorf("cdr: fixed decimal %q bad byte %#02x: %w", s, c, ErrInvalid)
		}
		seenDigit = true
	}
	if !seenDigit {
		return fmt.Errorf("cdr: fixed decimal %q misses digits: %w", s, ErrInvalid)
	}
	return nil
}

// WriteFixedDecimal writes a decimal fixed-point value as pure BCD octets.
//
// The wire form follows DDS-XTypes 7.2.3.4: each octet holds two decimal
// digits with the low nibble of the last octet carrying the sign (0xC
// positive, 0xD negative). No digit count or scale prefix is transmitted;
// the IDL fixed<digits,scale> declaration supplies them and the reader
// passes them to ReadFixedDecimal. Example "-12.34" with digits=4 scale=2
// encodes as octets 0x01 0x23 0x4D with no length prefix. Validation
// rejects malformed decimals without writing. Octets need no alignment.
//
// This is the single-infer API: digits and scale are inferred from the
// string (digits count excluding sign and dot, scale count after dot).
// To enforce a declaration, use WriteFixedDecimalChecked which rejects
// inferred values that differ from the declared digits and scale.
func (e *Encoder) WriteFixedDecimal(s string) error {
	err := validateFixedDecimal(s)
	if err != nil {
		return err
	}
	negative := len(s) > 0 && s[0] == '-'
	body := s
	if negative {
		body = body[1:]
	}
	scale := 0
	if idx := indexByte(body, '.'); idx >= 0 {
		scale = len(body) - idx - 1
		body = body[:idx] + body[idx+1:]
	}
	nDigits := len(body)
	if nDigits == 0 {
		return fmt.Errorf("cdr: fixed decimal %q misses digits: %w", s, ErrInvalid)
	}
	if scale > nDigits {
		return fmt.Errorf("cdr: fixed scale %d above digits %d: %w", scale, nDigits, ErrInvalid)
	}
	octets := encodeFixedBCD(body, negative)
	e.buf = append(e.buf, octets...)
	return nil
}

// WriteFixedDecimalChecked writes a fixed-point value checked against its declaration.
//
// digits counts decimal digits excluding sign and dot; scale counts
// fractional digits, both from the IDL fixed<digits,scale> declaration per
// DDS-XTypes 7.2.3.4. The inferred digits and scale from s must exactly
// match the declaration; otherwise the write fails without mutating the
// buffer. This mirrors ReadFixedDecimal(digits, scale) so writer and
// reader share one declaration. Use WriteFixedDecimal for the
// single-infer form when the declaration is not available.
func (e *Encoder) WriteFixedDecimalChecked(s string, digits, scale int) error {
	if digits <= 0 {
		return fmt.Errorf("cdr: fixed digits %d must be positive: %w", digits, ErrInvalid)
	}
	if scale < 0 || scale > digits {
		return fmt.Errorf("cdr: fixed scale %d outside 0 to digits %d: %w", scale, digits, ErrInvalid)
	}
	err := validateFixedDecimal(s)
	if err != nil {
		return err
	}
	negative := len(s) > 0 && s[0] == '-'
	body := s
	if negative {
		body = body[1:]
	}
	inferScale := 0
	if idx := indexByte(body, '.'); idx >= 0 {
		inferScale = len(body) - idx - 1
		body = body[:idx] + body[idx+1:]
	}
	inferDigits := len(body)
	if inferDigits == 0 {
		return fmt.Errorf("cdr: fixed decimal %q misses digits: %w", s, ErrInvalid)
	}
	if inferDigits != digits || inferScale != scale {
		return fmt.Errorf("cdr: fixed %q digits %d scale %d vs declared %d %d: %w", s, inferDigits, inferScale, digits, scale, ErrInvalid)
	}
	octets := encodeFixedBCD(body, negative)
	e.buf = append(e.buf, octets...)
	return nil
}

// indexByte returns the index of c in s, or -1 if absent.
func indexByte(s string, c byte) int {
	for i := range len(s) {
		if s[i] == c {
			return i
		}
	}
	return -1
}

// encodeFixedBCD packs decimal digits plus sign into BCD octets.
func encodeFixedBCD(digits string, negative bool) []byte {
	padded := digits
	if (len(digits)+1)%2 == 1 {
		padded = "0" + digits
	}
	return encodeFixedNibbles(padded, negative)
}

// encodeFixedNibbles packs padded digits plus sign nibble into octets.
func encodeFixedNibbles(padded string, negative bool) []byte {
	nibbles := make([]byte, 0, len(padded)+1)
	for i := range len(padded) {
		nibbles = append(nibbles, padded[i]-'0')
	}
	if negative {
		nibbles = append(nibbles, 0x0D)
	} else {
		nibbles = append(nibbles, 0x0C)
	}
	out := make([]byte, 0, len(nibbles)/2)
	for i := 0; i < len(nibbles); i += 2 {
		out = append(out, (nibbles[i]<<4)|nibbles[i+1])
	}
	return out
}

// ReadFixedDecimal reads a decimal fixed-point value with explicit type params.
//
// digits counts decimal digits excluding sign and dot; scale counts
// fractional digits, both from the IDL fixed<digits,scale> declaration per
// DDS-XTypes 7.2.3.4. The wire carries only BCD octets; the dot is placed
// from scale. Bad digits/scale params and overlong wires count a drop and
// fail without allocating. Example octets 0x01 0x23 0x4D with digits=4
// scale=2 decodes as "-12.34".
func (d *Decoder) ReadFixedDecimal(digits, scale int) (string, error) {
	if digits <= 0 {
		d.drops++
		return "", fmt.Errorf("cdr: fixed digits %d must be positive: %w", digits, ErrInvalid)
	}
	if scale < 0 || scale > digits {
		d.drops++
		return "", fmt.Errorf("cdr: fixed scale %d outside 0 to digits %d: %w", scale, digits, ErrInvalid)
	}
	if uint64(digits) > uint64(maxStringBytes) {
		d.drops++
		return "", fmt.Errorf("cdr: fixed digits %d exceeds cap %d: %w", digits, maxStringBytes, ErrInvalid)
	}
	nOctets := (digits + 2) / 2
	err := d.need(nOctets)
	if err != nil {
		return "", err
	}
	raw := d.data[d.off : d.off+nOctets]
	d.off += nOctets
	s, err := decodeFixedBCD(raw, digits, scale)
	if err != nil {
		d.drops++
		return "", err
	}
	err = validateFixedDecimal(s)
	if err != nil {
		d.drops++
		return "", fmt.Errorf("cdr: fixed decimal: %w", err)
	}
	return s, nil
}

// decodeFixedBCD unpacks BCD octets into a decimal string.
func decodeFixedBCD(raw []byte, nDigits, scale int) (string, error) {
	nibbles := unpackBCDNibbles(raw)
	if len(nibbles) == 0 {
		return "", fmt.Errorf("cdr: fixed decimal empty BCD: %w", ErrInvalid)
	}
	digits, negative, err := splitBCDSign(nibbles)
	if err != nil {
		return "", err
	}
	digits, err = stripBCDPad(digits, nDigits)
	if err != nil {
		return "", err
	}
	if err := validateBCDDigits(digits); err != nil {
		return "", err
	}
	return formatFixedDigits(digits, nDigits, scale, negative), nil
}

// unpackBCDNibbles expands BCD octets into high plus low nibbles.
func unpackBCDNibbles(raw []byte) []byte {
	nibbles := make([]byte, 0, len(raw)*2)
	for _, b := range raw {
		nibbles = append(nibbles, (b>>4)&0x0F, b&0x0F)
	}
	return nibbles
}

// splitBCDSign separates the trailing sign nibble from digit nibbles.
func splitBCDSign(nibbles []byte) ([]byte, bool, error) {
	sign := nibbles[len(nibbles)-1]
	switch sign {
	case 0x0C:
		return nibbles[:len(nibbles)-1], false, nil
	case 0x0D:
		return nibbles[:len(nibbles)-1], true, nil
	default:
		return nil, false, fmt.Errorf("cdr: fixed sign nibble %#x invalid: %w", sign, ErrInvalid)
	}
}

// stripBCDPad removes the leading pad nibble and checks digit count.
func stripBCDPad(digits []byte, nDigits int) ([]byte, error) {
	if len(digits) > nDigits {
		if digits[0] != 0 {
			return nil, fmt.Errorf("cdr: fixed pad nibble %#x nonzero: %w", digits[0], ErrInvalid)
		}
		digits = digits[len(digits)-nDigits:]
	}
	if len(digits) != nDigits {
		return nil, fmt.Errorf("cdr: fixed digits %d vs %d: %w", len(digits), nDigits, ErrInvalid)
	}
	return digits, nil
}

// validateBCDDigits rejects nibbles above 9.
func validateBCDDigits(digits []byte) error {
	for _, nb := range digits {
		if nb > 9 {
			return fmt.Errorf("cdr: fixed digit nibble %#x invalid: %w", nb, ErrInvalid)
		}
	}
	return nil
}

// formatFixedDigits renders digits with sign and decimal point.
func formatFixedDigits(digits []byte, nDigits, scale int, negative bool) string {
	var out []byte
	if negative {
		out = append(out, '-')
	}
	if scale == 0 {
		for _, nb := range digits {
			out = append(out, '0'+nb)
		}
		return string(out)
	}
	intLen := nDigits - scale
	if intLen <= 0 {
		out = append(out, '0')
	} else {
		for _, nb := range digits[:intLen] {
			out = append(out, '0'+nb)
		}
	}
	out = append(out, '.')
	if intLen <= 0 {
		for range -intLen {
			out = append(out, '0')
		}
		for _, nb := range digits {
			out = append(out, '0'+nb)
		}
	} else {
		for _, nb := range digits[intLen:] {
			out = append(out, '0'+nb)
		}
	}
	return string(out)
}

// WriteEnum writes an enumerated value with the width from Enumxt6aWidth.
//
// Bounds 1-8 use 1 byte, 9-16 use 2 bytes, 17-32 use 4 bytes per
// Table 30. Over range bounds and out of width values fail without
// writing. V2 caps 8 byte alignment at 4; enum widths never exceed 4.
func (e *Encoder) WriteEnum(bitBound uint32, value int32) error {
	width, err := Enumxt6aWidth(bitBound)
	if err != nil {
		return err
	}
	switch width {
	case 1:
		return e.writeEnum1(value)
	case 2:
		return e.writeEnum2(value)
	case 4:
		return e.writeEnum4(value)
	default:
		return fmt.Errorf("cdr: enum width %d unknown: %w", width, ErrInvalid)
	}
}

// writeEnum1 writes one byte enum value with range checks.
func (e *Encoder) writeEnum1(value int32) error {
	if err := checkEnum1Byte(value); err != nil {
		return err
	}
	e.putByte(byte(value)) //nolint:gosec // G115: range checked above
	return nil
}

// writeEnum2 writes two byte enum value with range checks.
func (e *Encoder) writeEnum2(value int32) error {
	if err := checkEnum2Byte(value); err != nil {
		return err
	}
	err := e.putUint16(uint16(value)) //nolint:gosec // G115: range checked above
	if err != nil {
		return fmt.Errorf("cdr: encode enum: %w", err)
	}
	return nil
}

// writeEnum4 writes four byte enum value.
func (e *Encoder) writeEnum4(value int32) error {
	err := e.putUint32(uint32(value)) //nolint:gosec // G115: truncation is IDL defined
	if err != nil {
		return fmt.Errorf("cdr: encode enum: %w", err)
	}
	return nil
}

// checkEnum1Byte validates one byte enum range.
//
// The read path uses ReadInt8, so only the signed -128 to 127 range
// round-trips per Table 30. Unsigned 128 to 255 would decode as negative,
// so they fail fail-closed instead of silently aliasing.
func checkEnum1Byte(value int32) error {
	if value >= -128 && value <= 127 {
		return nil
	}
	return fmt.Errorf("cdr: enum value %d outside 1 byte signed range: %w", value, ErrInvalid)
}

// checkEnum2Byte validates two byte enum range.
//
// The read path uses ReadInt16, so only -32768 to 32767 round-trips.
// Unsigned 32768 to 65535 would decode as negative, so they fail
// fail-closed per Table 30.
func checkEnum2Byte(value int32) error {
	if value >= -32768 && value <= 32767 {
		return nil
	}
	return fmt.Errorf("cdr: enum value %d outside 2 byte signed range: %w", value, ErrInvalid)
}

// ReadEnum reads an enumerated value with the width from Enumxt6aWidth.
func (d *Decoder) ReadEnum(bitBound uint32) (int32, error) {
	width, err := Enumxt6aWidth(bitBound)
	if err != nil {
		d.drops++
		return 0, fmt.Errorf("cdr: read enum: %w", err)
	}
	switch width {
	case 1:
		v, err := d.ReadInt8()
		if err != nil {
			return 0, err
		}
		return int32(v), nil
	case 2:
		v, err := d.ReadInt16()
		if err != nil {
			return 0, err
		}
		return int32(v), nil
	case 4:
		v, err := d.ReadInt32()
		if err != nil {
			return 0, err
		}
		return v, nil
	default:
		d.drops++
		return 0, fmt.Errorf("cdr: enum width %d unknown: %w", width, ErrInvalid)
	}
}

// WriteBitmask writes a bitmask value with the width from Bitmaskxt6aWidth.
//
// Bounds 1-8 use 1 byte, 9-16 use 2 bytes, 17-32 use 4 bytes, and
// 33-64 use 8 bytes per Table 31. V2 caps 8 byte alignment at 4.
func (e *Encoder) WriteBitmask(bitBound uint32, value uint64) error {
	width, err := Bitmaskxt6aWidth(bitBound)
	if err != nil {
		return err
	}
	switch width {
	case 1:
		return e.writeBitmask1(value)
	case 2:
		return e.writeBitmask2(value)
	case 4:
		return e.writeBitmask4(value)
	case 8:
		return e.writeBitmask8(value)
	default:
		return fmt.Errorf("cdr: bitmask width %d unknown: %w", width, ErrInvalid)
	}
}

// writeBitmask1 writes a 1 byte bitmask value.
func (e *Encoder) writeBitmask1(value uint64) error {
	if value > 0xFF {
		return fmt.Errorf("cdr: bitmask value %#x outside 1 byte: %w", value, ErrInvalid)
	}
	e.putByte(byte(value))
	return nil
}

// writeBitmask2 writes a 2 byte bitmask value.
func (e *Encoder) writeBitmask2(value uint64) error {
	if value > 0xFFFF {
		return fmt.Errorf("cdr: bitmask value %#x outside 2 bytes: %w", value, ErrInvalid)
	}
	err := e.putUint16(uint16(value))
	if err != nil {
		return fmt.Errorf("cdr: encode bitmask: %w", err)
	}
	return nil
}

// writeBitmask4 writes a 4 byte bitmask value.
func (e *Encoder) writeBitmask4(value uint64) error {
	if value > 0xFFFFFFFF {
		return fmt.Errorf("cdr: bitmask value %#x outside 4 bytes: %w", value, ErrInvalid)
	}
	err := e.putUint32(uint32(value))
	if err != nil {
		return fmt.Errorf("cdr: encode bitmask: %w", err)
	}
	return nil
}

// writeBitmask8 writes an 8 byte bitmask value.
func (e *Encoder) writeBitmask8(value uint64) error {
	err := e.putUint64(value)
	if err != nil {
		return fmt.Errorf("cdr: encode bitmask: %w", err)
	}
	return nil
}

// ReadBitmask reads a bitmask value with the width from Bitmaskxt6aWidth.
func (d *Decoder) ReadBitmask(bitBound uint32) (uint64, error) {
	width, err := Bitmaskxt6aWidth(bitBound)
	if err != nil {
		d.drops++
		return 0, fmt.Errorf("cdr: read bitmask: %w", err)
	}
	switch width {
	case 1:
		v, err := d.ReadUint8()
		if err != nil {
			return 0, err
		}
		return uint64(v), nil
	case 2:
		v, err := d.ReadUint16()
		if err != nil {
			return 0, err
		}
		return uint64(v), nil
	case 4:
		v, err := d.ReadUint32()
		if err != nil {
			return 0, err
		}
		return uint64(v), nil
	case 8:
		v, err := d.ReadUint64()
		if err != nil {
			return 0, err
		}
		return v, nil
	default:
		d.drops++
		return 0, fmt.Errorf("cdr: bitmask width %d unknown: %w", width, ErrInvalid)
	}
}

// SortMapKeysxt6b returns the keys sorted by CDR comparison.
//
// The input slice is not mutated; a sorted copy is returned. Keys must
// all share one portable kind per Comparext6bMapKey. Duplicates fail.
func SortMapKeysxt6b(kind RosTypeID, keys []any) ([]any, error) {
	out := append([]any(nil), keys...)
	for i := 1; i < len(out); i++ {
		for j := i; j > 0; j-- {
			cmp, err := Comparext6bMapKey(kind, out[j-1], out[j])
			if err != nil {
				return nil, err
			}
			if cmp == 0 {
				return nil, fmt.Errorf("types: map keys duplicate at %d: %w", j, ErrInvalid)
			}
			if cmp < 0 {
				break
			}
			out[j-1], out[j] = out[j], out[j-1]
		}
	}
	return out, nil
}

// MarshalTypeObjectSeqCDR encodes a TypeObjectSeq as sequence plus bodies.
//
// Each object already carries its own DHEADER per 7.3.3.5; the sequence
// adds only the uint32 element count. Little endian selects the header
// and the DHEADER byte order for the count. The bodies are appended
// verbatim so hash inputs match serialized objects byte for byte.
func MarshalTypeObjectSeqCDR(objects [][]byte, littleEndian bool) ([]byte, error) {
	enc := NewEncoder(littleEndian)
	if uint64(len(objects)) > uint64(math.MaxUint32) {
		return nil, fmt.Errorf("cdr: type object seq length %d overflows uint32: %w", len(objects), ErrInvalid)
	}
	err := enc.WriteSequenceLength(len(objects))
	if err != nil {
		return nil, err
	}
	raw := enc.Bytes()
	// Strip the 4 byte pad tail: sequence bodies follow without pad.
	// Bytes pads only when the payload is empty or needs 4 byte rounding;
	// here the payload holds the length prefix, so trim trailing pad that
	// belongs to finalize, then re-append bodies plus final pad.
	trimmed := raw[:HeaderSize+4]
	out := append([]byte(nil), trimmed...)
	for _, obj := range objects {
		// Each DHEADER needs 4 byte alignment; pad between objects.
		pad := alignPad(len(out)-HeaderSize, 4)
		for range pad {
			out = append(out, nulByte)
		}
		out = append(out, obj...)
	}
	pad := padTo4(len(out))
	for range pad {
		out = append(out, nulByte)
	}
	return out, nil
}

// ParseTypeObjectSeqCDR splits a TypeObjectSeq into its element bodies.
//
// The sequence length prefix is validated; each element is delimited by
// its own DHEADER so boundaries are recovered without extra framing.
// Inter-element pad for 4 byte DHEADER alignment is skipped by the
// decoder. Trailing pad zeros are accepted via Finish.
func ParseTypeObjectSeqCDR(wire []byte) ([][]byte, error) {
	dec, err := NewDecoder(wire)
	if err != nil {
		return nil, err
	}
	n, err := dec.ReadSequenceLength()
	if err != nil {
		return nil, err
	}
	out := make([][]byte, 0, n)
	for range n {
		offBefore := dec.off
		size, err := dec.ReadDHeader()
		if err != nil {
			return nil, err
		}
		// ReadDHeader aligned then consumed 4 bytes; element starts there.
		start := dec.off - 4
		_ = offBefore
		// DHEADER covers discriminator plus body; total element is 4 plus size.
		total := 4 + int(size)
		if start+total > len(dec.data) {
			return nil, fmt.Errorf("cdr: type object element exceeds buffer: %w", ErrTruncated)
		}
		elem := make([]byte, total)
		copy(elem, dec.data[start:start+total])
		// Advance past body already consumed by ReadDHeader plus content.
		// ReadDHeader consumed 4 bytes; skip the size bytes.
		err = dec.SkipBytes(uint64(size))
		if err != nil {
			return nil, err
		}
		out = append(out, elem)
	}
	err = dec.Finish()
	if err != nil {
		// Allow trailing pad zeros only; Finish already enforces this.
		return nil, err
	}
	return out, nil
}
