// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later
package types

// Package types carries the wire codec for Weld.
//
// This file implements plain XCDR1 data encoding per introspection.
// XCDR1 is the only representation used on DDS paths. XCDR2 is never
// emitted. Alignment is relative to the user payload start, outside the
// encapsulation header and service prefix, matching the de facto ROS 2
// wire layout. No transport code,
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

// Encoder builds one XCDR1 sample including its 4 byte header.
//
// The header is written at construction so later alignment is already
// rebased 4 bytes forward. Call Bytes to get the padded wire form.
// Metrics for opaque parsing live in envelope Metrics, which is separate
// because Metrics is caller owned and may be nil while Encoder state is
// builder owned. Decoder carries its own private drop counter for
// streaming reads, so the two counters have different lifetimes.
type Encoder struct {
	buf          []byte
	order        binary.ByteOrder
	le           bool
	payloadStart int
	// noDummy skips the empty dummy byte in Bytes. WriteServiceHeader
	// sets it because ROS sends no dummy for empty service structs.
	noDummy bool
}

// NewEncoder returns an Encoder with the 4 byte header already written.
//
// le selects little endian (0x01) when true and big endian (0x00)
// when false. Header bytes are built by EncodeHeader so header encoding
// lives in one place.
func NewEncoder(littleEndian bool) *Encoder {
	e := &Encoder{le: littleEndian, payloadStart: HeaderSize}
	if littleEndian {
		e.order = binary.LittleEndian
	} else {
		e.order = binary.BigEndian
	}
	h := EncodeHeader(littleEndian)
	e.buf = append(e.buf, h[:]...)
	return e
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
// Precondition: callers pass 1, 2, 4, or 8. All current callers satisfy
// this, so the silent ignore below never triggers in practice. Any other
// n is a caller bug and leaves the buffer unchanged without reporting an
// error. The signature stays void to avoid rippling through every write
// path; changing it would force error returns on all callers. This differs
// from Decoder.Align which returns an error for invalid n without
// counting a drop.
//
// Alignment is relative to the user payload start, not the buffer
// start: the encapsulation header (and service prefix when present) is
// outside the CDR alignment domain, matching the de facto ROS 2 wire
// layout where a leading double needs no pad.
func (e *Encoder) Align(n int) {
	if n != 1 && n != 2 && n != 4 && n != 8 {
		return
	}
	pad := alignPad(len(e.buf)-e.payloadStart, n)
	for range pad {
		e.buf = append(e.buf, nulByte)
	}
}

// putByte appends one raw byte with 1 byte alignment.
func (e *Encoder) putByte(v byte) {
	e.buf = append(e.buf, v)
}

// putUint16 appends a 2 byte value with 2 byte alignment.
func (e *Encoder) putUint16(v uint16) {
	e.Align(2)
	var tmp [2]byte
	e.order.PutUint16(tmp[:], v)
	e.buf = append(e.buf, tmp[:]...)
}

// putUint32 appends a 4 byte value with 4 byte alignment.
func (e *Encoder) putUint32(v uint32) {
	e.Align(4)
	var tmp [4]byte
	e.order.PutUint32(tmp[:], v)
	e.buf = append(e.buf, tmp[:]...)
}

// putUint64 appends an 8 byte value with 8 byte alignment.
func (e *Encoder) putUint64(v uint64) {
	e.Align(8)
	var tmp [8]byte
	e.order.PutUint64(tmp[:], v)
	e.buf = append(e.buf, tmp[:]...)
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
	e.putUint16(v)
}

// WriteInt16 writes a 2 byte signed value with 2 byte alignment.
func (e *Encoder) WriteInt16(v int16) {
	e.putUint16(uint16(v)) //nolint:gosec // G115: intentional two's-complement reinterpret for XCDR1 encoding
}

// WriteUint16 writes a 2 byte unsigned value with 2 byte alignment.
func (e *Encoder) WriteUint16(v uint16) {
	e.putUint16(v)
}

// WriteInt32 writes a 4 byte signed value with 4 byte alignment.
func (e *Encoder) WriteInt32(v int32) {
	e.putUint32(uint32(v)) //nolint:gosec // G115: intentional two's-complement reinterpret for XCDR1 encoding
}

// WriteUint32 writes a 4 byte unsigned value with 4 byte alignment.
func (e *Encoder) WriteUint32(v uint32) {
	e.putUint32(v)
}

// WriteInt64 writes an 8 byte signed value with 8 byte alignment.
func (e *Encoder) WriteInt64(v int64) {
	e.putUint64(uint64(v)) //nolint:gosec // G115: intentional two's-complement reinterpret for XCDR1 encoding
}

// WriteUint64 writes an 8 byte unsigned value with 8 byte alignment.
func (e *Encoder) WriteUint64(v uint64) {
	e.putUint64(v)
}

// WriteFloat32 writes a 4 byte float with 4 byte alignment.
func (e *Encoder) WriteFloat32(v float32) {
	e.putUint32(math.Float32bits(v))
}

// WriteFloat64 writes an 8 byte float with 8 byte alignment.
func (e *Encoder) WriteFloat64(v float64) {
	e.putUint64(math.Float64bits(v))
}

// countRunes returns the rune count of s without allocating a slice.
func countRunes(s string) int {
	n := 0
	for range s {
		n++
	}
	return n
}

// truncateRunes limits s to at most bound runes on a rune boundary.
func truncateRunes(s string, bound int) string {
	runes := []rune(s)
	if len(runes) <= bound {
		return s
	}
	return string(runes[:bound])
}

// encodeString appends length plus NUL for s.
//
// Pad ownership: the encoder inserts pad only per next-member alignment.
// It writes the length prefix with Align(4), then the bytes plus NUL with
// no trailing pad. The next member's Align absorbs any gap, and Bytes
// pads a terminal string to 4. The decoder never skips string pad and
// checks the NUL only.
func (e *Encoder) encodeString(s string) {
	e.Align(4)
	total := len(s) + 1
	var lenBytes [4]byte
	e.order.PutUint32(lenBytes[:], uint32(total)) //nolint:gosec // G115: total is a non-negative length guarded by callers
	e.buf = append(e.buf, lenBytes[:]...)
	e.buf = append(e.buf, s...)
	e.buf = append(e.buf, nulByte)
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
	e.encodeString(s)
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
	e.encodeString(s)
	return nil
}

// WriteBoundedString writes a string checked against a bound.
//
// bound is the maximum rune count excluding NUL. Zero means unbounded.
// A negative bound returns an error and leaves the buffer unchanged.
// When the rune count exceeds a positive bound the input is truncated on
// a rune boundary so multi byte UTF-8 is never split, the truncated form
// is encoded, and the call reports truncated true with nil error.
// Even bounded writes may exceed the decoder cap when the bound is large;
// the decoder still rejects over cap input. Overflow past MaxUint32
// returns an error without writing.
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
	if countRunes(s) > bound {
		truncated := truncateRunes(s, bound)
		if uint64(len(truncated))+1 > uint64(math.MaxUint32) {
			return false, errors.New("cdr: string length overflows uint32") //nolint:wrapcheck // new error, nothing to wrap
		}
		e.encodeString(truncated)
		return true, nil
	}
	if uint64(len(s))+1 > uint64(math.MaxUint32) {
		return false, errors.New("cdr: string length overflows uint32") //nolint:wrapcheck // new error, nothing to wrap
	}
	e.encodeString(s)
	return false, nil
}

// encodeWString appends length plus UTF-16 units plus NUL for s.
//
// The length prefix counts UTF-16 code units including the trailing NUL.
// No trailing pad is written. Pad ownership matches encodeString: the
// next member's Align absorbs any gap and Bytes pads a terminal wstring.
func (e *Encoder) encodeWString(s string) {
	units := utf16.Encode([]rune(s))
	e.Align(4)
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
	e.encodeWString(s)
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
	e.encodeWString(s)
	return nil
}

// WriteBoundedWString writes a wstring checked against a bound.
//
// bound is the maximum rune count excluding NUL, not the wire unit
// count. An astral rune such as U+1F600 counts as one rune but encodes
// as two UTF-16 units, so bound 1 accepts one such rune. Zero means
// unbounded. A negative bound returns an error and leaves the buffer
// unchanged. Overlong input is truncated on a rune boundary, encoded,
// and reported with truncated true. Truncation never splits a surrogate
// pair because it cuts on runes before UTF-16 encoding. Even bounded
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
	if countRunes(s) > bound {
		truncated := truncateRunes(s, bound)
		units := utf16.Encode([]rune(truncated))
		if uint64(len(units))+1 > uint64(math.MaxUint32) {
			return false, errors.New("cdr: wstring length overflows uint32") //nolint:wrapcheck // new error, nothing to wrap
		}
		e.encodeWString(truncated)
		return true, nil
	}
	err := e.WriteWString(s)
	if err != nil {
		return false, err
	}
	return false, nil
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
	e.putUint32(uint32(n))
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
	e.putUint32(uint32(n))
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
	e.putUint32(uint32(n))
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
	e.Align(4)
	tmp := encodeServiceSequence(seq, e.le)
	e.buf = append(e.buf, tmp[:]...)
	e.payloadStart = ServiceEnvelopeSize
	e.noDummy = true
	return nil
}

// Decoder reads one XCDR1 sample including its 4 byte header.
//
// All reads bounds check the buffer, count failures in drops, and
// return errors instead of panicking. The private counter is separate
// from envelope Metrics because Decoder owns streaming field reads while
// Metrics is caller owned for opaque prefix parsing.
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
}

// NewDecoder validates the header and returns a decoder at offset 4.
//
// Header validation is shared with ParseHeader in one place. The envelope
// error text is mapped to the cdr prefix here so validation logic has a
// single source. It rejects empty buffers, buffers shorter than 4 bytes,
// a nonzero first byte, and a second byte above 1. Reserved bytes 2 and 3
// are ignored. Failures count one drop.
func NewDecoder(data []byte) (*Decoder, error) {
	le, err := ParseHeader(data)
	if err != nil {
		d := &Decoder{drops: 1, payloadStart: HeaderSize}
		msg := strings.TrimPrefix(err.Error(), "envelope: ")
		if len(data) < HeaderSize {
			return d, fmt.Errorf("cdr: %s: %w", msg, ErrTruncated)
		}
		return d, fmt.Errorf("cdr: %s: %w", msg, ErrInvalid)
	}
	d := &Decoder{data: data, off: HeaderSize, le: le, payloadStart: HeaderSize}
	if le {
		d.order = binary.LittleEndian
	} else {
		d.order = binary.BigEndian
	}
	return d, nil
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

// readUint64 reads an 8 byte value with 8 byte alignment.
func (d *Decoder) readUint64() (uint64, error) {
	err := d.Align(8)
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
// bound is the maximum rune count excluding NUL. Zero means unbounded.
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
	if bound > 0 && countRunes(s) > bound {
		d.drops++
		return "", fmt.Errorf("cdr: string length %d exceeds bound %d: %w", countRunes(s), bound, ErrInvalid)
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

// ReadBoundedWString reads a wstring checked against a rune bound.
//
// bound is the maximum rune count excluding NUL, not the wire unit
// count. An astral rune counts as one rune but two units. Zero means
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
	if bound > 0 && countRunes(s) > bound {
		d.drops++
		return "", fmt.Errorf("cdr: wstring length %d exceeds bound %d: %w", countRunes(s), bound, ErrInvalid)
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
// bound counts a drop and returns an error. An overlong prefix counts a
// drop and returns an error without allocating. This is the bounded
// sequence helper for IDL bounded sequences.
func (d *Decoder) ReadBoundedSequenceLength(bound int) (int, error) {
	if bound < 0 {
		d.drops++
		return 0, fmt.Errorf("cdr: negative sequence bound %d: %w", bound, ErrInvalid)
	}
	n, err := d.ReadSequenceLength()
	if err != nil {
		return 0, err
	}
	if bound > 0 && n > bound {
		d.drops++
		return 0, fmt.Errorf("cdr: sequence length %d exceeds bound %d: %w", n, bound, ErrInvalid)
	}
	return n, nil
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
