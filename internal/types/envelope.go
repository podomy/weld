// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later
package types

// Envelope handling for XCDR1 samples.
//
// The base envelope is a 4 byte header followed by payload rebased 4
// bytes forward. Header bytes are 0x00, endian flag (0x01 LE, 0x00 BE),
// 0x00, 0x00. Services add an 8 byte GUID plus an 8 byte sequence in
// negotiated endianness after the header. Serialized data is padded to
// 4 bytes with a zeroed tail. Header plus length are validated before
// parsing. Truncated, empty, and extra field input drops with a metric
// counter and never panics.

import (
	"encoding/binary"
	"errors"
	"fmt"
)

// Envelope sizes on the wire.
const (
	// HeaderSize is the base 4 byte XCDR1 encapsulation header.
	HeaderSize = 4
	// ServiceGUIDSize is the service request GUID prefix.
	ServiceGUIDSize = 8
	// ServiceSequenceSize is the service sequence number width.
	ServiceSequenceSize = 8
	// ServiceHeaderSize is GUID plus sequence after the base header.
	ServiceHeaderSize = ServiceGUIDSize + ServiceSequenceSize
	// ServiceEnvelopeSize is the full service prefix including header.
	ServiceEnvelopeSize = HeaderSize + ServiceHeaderSize
)

// Metrics counts dropped inputs for envelope parsing.
//
// Drops increments for truncated, empty, and malformed buffers. The
// struct is plain data owned by the caller. There is no global state.
// Metrics is separate from Decoder drops because Metrics covers opaque
// envelope parsing with a caller owned counter that may be nil, while
// Decoder carries a streaming read counter for typed CDR fields.
// Different lifetimes and ownership motivate separate counters.
type Metrics struct {
	// Drops counts inputs dropped instead of parsed.
	Drops uint64
}

// RecordDrop adds one drop to the counter.
func (m *Metrics) RecordDrop() {
	if m == nil {
		return
	}
	m.Drops++
}

// Count returns the drop total, treating nil as zero.
func (m *Metrics) Count() uint64 {
	if m == nil {
		return 0
	}
	return m.Drops
}

// EncodeHeader builds the 4 byte XCDR1 header for an endianness.
func EncodeHeader(littleEndian bool) [4]byte {
	var h [4]byte
	h[0] = 0x00
	if littleEndian {
		h[1] = 0x01
	} else {
		h[1] = 0x00
	}
	h[2] = 0x00
	h[3] = 0x00
	return h
}

// ParseHeader validates 4 header bytes and returns the endian flag.
//
// It rejects a nonzero first byte and a second byte above 1. The third
// and fourth bytes are reserved and ignored, so nonzero reserved bytes
// are accepted for forward compatibility.
func ParseHeader(data []byte) (bool, error) {
	if len(data) < HeaderSize {
		return false, fmt.Errorf("envelope: buffer too short for header: %d bytes", len(data))
	}
	if data[0] != 0x00 {
		return false, fmt.Errorf("envelope: bad header first byte 0x%02x", data[0])
	}
	if data[1] > 0x01 {
		return false, fmt.Errorf("envelope: bad header endian flag 0x%02x", data[1])
	}
	return data[1] == 0x01, nil
}

// padTo4 returns the zero pad needed to reach a 4 byte boundary.
//
// padTo4(n) equals alignPad(n, 4). The two helpers share the same modular
// rule; padTo4 is kept for call sites that always target 4 byte wire pad
// while alignPad takes an explicit alignment for CDR Align steps.
func padTo4(n int) int {
	return (4 - (n % 4)) % 4
}

// alignPad returns the pad needed to advance offset off to a multiple of n.
func alignPad(off, n int) int {
	return (n - (off % n)) % n
}

// needsDummyByte is the single empty rule shared by all marshal paths.
//
// It reports true iff the user payload length is 0, independent of any
// header or service prefix. Both MarshalMessage and MarshalServiceMessage
// and Encoder.Bytes use this helper so empty handling cannot diverge.
func needsDummyByte(userLen int) bool {
	return userLen == 0
}

// encodeServiceSequence encodes a service sequence number in an endianness.
func encodeServiceSequence(seq uint64, littleEndian bool) [8]byte {
	var tmp [8]byte
	if littleEndian {
		binary.LittleEndian.PutUint64(tmp[:], seq)
	} else {
		binary.BigEndian.PutUint64(tmp[:], seq)
	}
	return tmp
}

// decodeServiceSequence decodes a service sequence number in an endianness.
func decodeServiceSequence(b []byte, littleEndian bool) uint64 {
	if littleEndian {
		return binary.LittleEndian.Uint64(b)
	}
	return binary.BigEndian.Uint64(b)
}

// MarshalMessage wraps a raw payload with header plus zeroed pad.
//
// An empty or nil payload encodes as one dummy zero byte. Output size
// includes the header and is always a multiple of 4. The dummy rule is
// needsDummyByte so Encoder.Bytes agrees on empty input.
func MarshalMessage(payload []byte, littleEndian bool) []byte {
	h := EncodeHeader(littleEndian)
	body := payload
	if needsDummyByte(len(body)) {
		body = []byte{0x00}
	}
	out := make([]byte, 0, HeaderSize+len(body)+4)
	out = append(out, h[:]...)
	out = append(out, body...)
	pad := padTo4(len(out))
	for range pad {
		out = append(out, 0x00)
	}
	return out
}

// UnmarshalMessage validates the header plus length and returns payload.
//
// This is an unvalidated prefix view, not a strict parse. Only the
// header is validated. The returned slice aliases data after the header,
// including any zero pad tail and any trailing garbage. It does not
// validate CDR field alignment, lengths, NUL terminators, or pad bytes.
// For strict validation run a Decoder over the original wire, read the
// expected typed fields, and call Decoder.Finish to detect trailing
// garbage as a drop.
func UnmarshalMessage(data []byte, m *Metrics) ([]byte, bool, error) {
	if len(data) == 0 {
		m.RecordDrop()
		return nil, false, errors.New("envelope: empty buffer") //nolint:wrapcheck // new error, nothing to wrap
	}
	le, err := ParseHeader(data)
	if err != nil {
		m.RecordDrop()
		return nil, false, err
	}
	return data[HeaderSize:], le, nil
}

// MarshalServiceMessage wraps a payload with header plus GUID and sequence.
//
// Layout is a packed 4 byte header plus 8 byte GUID plus 8 byte sequence
// in the negotiated order plus payload. The 8+8 service header is packed
// outside the CDR alignment domain on purpose: no Align(8) pad is added
// between GUID and sequence even though the GUID ends at offset 12.
// User payload is therefore rebased at offset 20. This matches
// Encoder.WriteServiceHeader and Decoder.ReadServiceHeader and is pinned
// by golden byte tests. Interop example LE with guid 01..08 and seq
// 0x0102030405060708 is 00 01 00 00 01 02 03 04 05 06 07 08
// 08 07 06 05 04 03 02 01. BE replaces the last 8 with 01 02 03 04
// 05 06 07 08. Empty payload encodes as one dummy byte.
//
// The GUID is opaque service identity bytes copied verbatim and never
// byte swapped. This 8 byte service GUID is distinct from the 16 byte DDS
// participant GUID used on discovery; do not mix them.
func MarshalServiceMessage(payload []byte, guid [8]byte, seq uint64, littleEndian bool) []byte {
	h := EncodeHeader(littleEndian)
	body := payload
	if needsDummyByte(len(body)) {
		body = []byte{0x00}
	}
	out := make([]byte, 0, ServiceEnvelopeSize+len(body)+4)
	out = append(out, h[:]...)
	out = append(out, guid[:]...)
	tmp := encodeServiceSequence(seq, littleEndian)
	out = append(out, tmp[:]...)
	out = append(out, body...)
	pad := padTo4(len(out))
	for range pad {
		out = append(out, 0x00)
	}
	return out
}

// UnmarshalServiceMessage validates a service sample and splits the parts.
//
// This is an unvalidated prefix view. Only the header and the 20 byte
// prefix length are validated. The GUID and sequence use the negotiated
// order from the packed 8+8 layout with user payload rebased at 20. The
// returned payload aliases data after the prefix, including pad tail and
// any trailing garbage. It does not validate CDR fields. The GUID is
// opaque and never swapped; it is the 8 byte service GUID, distinct from
// the 16 byte DDS participant GUID. For strict validation run a Decoder
// over the original wire, call Decoder.ReadServiceHeader, read typed
// fields, and call Decoder.Finish. Short, empty, and bad header input
// counts a drop in m when m is non nil and returns an error. It never
// panics.
func UnmarshalServiceMessage(data []byte, m *Metrics) ([]byte, [8]byte, uint64, bool, error) {
	var guid [8]byte
	if len(data) == 0 {
		m.RecordDrop()
		return nil, guid, 0, false, errors.New("envelope: empty service buffer") //nolint:wrapcheck // new error, nothing to wrap
	}
	le, err := ParseHeader(data)
	if err != nil {
		m.RecordDrop()
		return nil, guid, 0, false, err
	}
	if len(data) < ServiceEnvelopeSize {
		m.RecordDrop()
		return nil, guid, 0, false, fmt.Errorf("envelope: truncated service header need %d have %d", ServiceEnvelopeSize, len(data))
	}
	copy(guid[:], data[HeaderSize:HeaderSize+ServiceGUIDSize])
	seq := decodeServiceSequence(data[HeaderSize+ServiceGUIDSize:ServiceEnvelopeSize], le)
	return data[ServiceEnvelopeSize:], guid, seq, le, nil
}
