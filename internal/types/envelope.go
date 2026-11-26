// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1
package types

// Envelope handling for XCDR1 and XCDR2 samples.
//
// The base envelope is a 4 byte Table 57 header followed by payload
// rebased 4 bytes forward. Final V1 headers are 0x00, endian flag (0x01
// LE, 0x00 BE), 0x00, 0x00; appendable/mutable V2 headers use the
// delimited IDs via EncodeHeaderForExt. Services add an 8 byte GUID plus
// an 8 byte sequence in negotiated endianness after the header.
// Serialized data is padded to 4 bytes with a zeroed tail. Header plus
// length are validated before parsing. Truncated, empty, and extra field
// input drops with a metric counter and never panics.

import (
	"encoding/binary"
	"errors"
	"fmt"
)

// Envelope sizes on the wire.
const (
	// HeaderSize is the base 4 byte XCDR1 encapsulation header.
	HeaderSize = 4
	// ServiceGUIDSize is the service request GUID width.
	//
	// The service GUID is the 8 byte rmw request id (cdds_request_header_t
	// guid) echoed verbatim in the reply. Wire layout is a 4 byte CDR
	// header plus 8 byte GUID plus 8 byte sequence plus XCDR1 user fields,
	// 20 bytes before payload with no dummy byte for empty structs.
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

// EncodeHeader builds the 4 byte XCDR header for final V1.
//
// It delegates to EncodeEncapsulationxt6b with final kind so header
// encoding lives in one place. Final V1 yields 00 01 or 00 00 which
// matches the legacy ROS wire. Option bytes start zero. Use
// EncodeHeaderForExt for appendable/mutable V2 IDs; this wrapper stays
// byte identical for existing final paths.
func EncodeHeader(littleEndian bool) [4]byte {
	return EncodeEncapsulationxt6b(ExtFinal, littleEndian)
}

// EncodeHeaderForExt builds the 4 byte header for one extensibility kind.
//
// Final yields V1 IDs; appendable and mutable yield V2 delimited IDs per
// Table 32. Parsing via ParseEncapsulationxt6b already accepts all
// Table 57 IDs. Option bytes start zero. For explicit V1 appendable
// (PLAIN_CDR) and V1 mutable (PL_CDR) emits use
// EncodeHeaderForExtWithVersion.
func EncodeHeaderForExt(ext Extensibility, littleEndian bool) [4]byte {
	return EncodeEncapsulationxt6b(ext, littleEndian)
}

// EncodeHeaderForExtWithVersion builds the header for a kind plus version.
//
// V1 appendable yields PLAIN_CDR (0x0000/0x0001) and V1 mutable yields
// PL_CDR (0x0002/0x0003); V2 yields CDR2/D_CDR2/PL_CDR2 per Table 32.
func EncodeHeaderForExtWithVersion(ext Extensibility, ver EncodingVersion, littleEndian bool) [4]byte {
	return EncodeEncapsulationxt6bWithVersion(ext, ver, littleEndian)
}

// ParseHeader validates 4 header bytes and returns the endian flag.
//
// It delegates to ParseEncapsulationxt6b so all Table 57 XCDR IDs are
// accepted, not only 00 00 and 00 01. The legacy ROS IDs remain valid.
// Reserved option bytes are ignored. XML and unknown IDs are rejected.
func ParseHeader(data []byte) (bool, error) {
	if len(data) < HeaderSize {
		return false, fmt.Errorf("envelope: buffer too short for header: %d bytes", len(data))
	}
	var hdr [4]byte
	copy(hdr[:], data[:HeaderSize])
	parsed, err := ParseEncapsulationxt6b(hdr)
	if err != nil {
		return false, err
	}
	return parsed.LittleEndian, nil
}

// padTo4 returns the zero pad needed to reach a 4 byte boundary.
//
// padTo4(n) equals alignPad(n, 4). It is kept as a thin wrapper so all
// 4 byte wire pad shares the single modular rule in alignPad and the two
// helpers cannot diverge; behavior is identical.
func padTo4(n int) int {
	return alignPad(n, 4)
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
// needsDummyByte so Encoder.Bytes agrees on empty input. This is the
// final V1 path and stays byte identical; use MarshalMessageWithExt for
// appendable/mutable V2 IDs.
func MarshalMessage(payload []byte, littleEndian bool) []byte {
	return MarshalMessageWithExt(payload, ExtFinal, littleEndian)
}

// MarshalMessageWithExt wraps a payload with the header for one kind.
//
// Final emits V1 IDs; appendable and mutable emit V2 delimited IDs per
// Table 32. Empty handling and pad match MarshalMessage so final wires
// are identical. For explicit V1 appendable/mutable emits use
// MarshalMessageWithExtVersioned.
func MarshalMessageWithExt(payload []byte, ext Extensibility, littleEndian bool) []byte {
	h := EncodeHeaderForExt(ext, littleEndian)
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

// MarshalMessageWithExtVersioned wraps a payload with an explicit version.
//
// V1 appendable emits PLAIN_CDR and V1 mutable emits PL_CDR per Table 57;
// V2 emits the delimited IDs per Table 32.
func MarshalMessageWithExtVersioned(payload []byte, ext Extensibility, ver EncodingVersion, littleEndian bool) []byte {
	h := EncodeHeaderForExtWithVersion(ext, ver, littleEndian)
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
// by golden byte tests plus live ROS captures. Empty payload stays empty:
// ROS sends no dummy byte for empty service structs. This is the final
// V1 path; use MarshalServiceMessageWithExt for V2 kinds.
//
// The GUID is the opaque 8 byte rmw request id echoed verbatim in the
// reply. It is copied verbatim and never byte swapped.
func MarshalServiceMessage(payload []byte, guid [8]byte, seq uint64, littleEndian bool) []byte {
	return MarshalServiceMessageWithExt(payload, guid, seq, ExtFinal, littleEndian)
}

// MarshalServiceMessageWithExt wraps a service payload for one kind.
//
// Final emits V1 IDs; appendable and mutable emit V2 IDs per Table 32.
// Layout and empty handling match MarshalServiceMessage so final wires
// are identical. For explicit V1 appendable/mutable emits use
// MarshalServiceMessageWithExtVersioned.
func MarshalServiceMessageWithExt(payload []byte, guid [8]byte, seq uint64, ext Extensibility, littleEndian bool) []byte {
	h := EncodeHeaderForExt(ext, littleEndian)
	body := payload
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

// MarshalServiceMessageWithExtVersioned wraps a service payload with version.
//
// V1 appendable emits PLAIN_CDR and V1 mutable emits PL_CDR per Table 57.
func MarshalServiceMessageWithExtVersioned(payload []byte, guid [8]byte, seq uint64, ext Extensibility, ver EncodingVersion, littleEndian bool) []byte {
	h := EncodeHeaderForExtWithVersion(ext, ver, littleEndian)
	body := payload
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

// RepresentationIDxt6b names Table 57 encapsulation IDs.
type RepresentationIDxt6b uint16

// Table 57 representation IDs.
const (
	// Repxt6bCDRBE is XCDR final or appendable big endian V1.
	Repxt6bCDRBE RepresentationIDxt6b = 0x0000
	// Repxt6bCDRLE is XCDR final or appendable little endian V1.
	Repxt6bCDRLE RepresentationIDxt6b = 0x0001
	// Repxt6bPLCDRBE is XCDR mutable big endian V1.
	Repxt6bPLCDRBE RepresentationIDxt6b = 0x0002
	// Repxt6bPLCDRLE is XCDR mutable little endian V1.
	Repxt6bPLCDRLE RepresentationIDxt6b = 0x0003
	// Repxt6bCDR2BE is XCDR final big endian V2.
	Repxt6bCDR2BE RepresentationIDxt6b = 0x0006
	// Repxt6bCDR2LE is XCDR final little endian V2.
	Repxt6bCDR2LE RepresentationIDxt6b = 0x0007
	// Repxt6bDCDR2BE is XCDR appendable big endian V2.
	Repxt6bDCDR2BE RepresentationIDxt6b = 0x0008
	// Repxt6bDCDR2LE is XCDR appendable little endian V2.
	Repxt6bDCDR2LE RepresentationIDxt6b = 0x0009
	// Repxt6bPLCDR2BE is XCDR mutable big endian V2.
	Repxt6bPLCDR2BE RepresentationIDxt6b = 0x000A
	// Repxt6bPLCDR2LE is XCDR mutable little endian V2.
	Repxt6bPLCDR2LE RepresentationIDxt6b = 0x000B
)

// String returns the symbolic name of the representation ID.
func (r RepresentationIDxt6b) String() string {
	switch r {
	case Repxt6bCDRBE:
		return "CDR_BE"
	case Repxt6bCDRLE:
		return "CDR_LE"
	case Repxt6bPLCDRBE:
		return "PL_CDR_BE"
	case Repxt6bPLCDRLE:
		return "PL_CDR_LE"
	case Repxt6bCDR2BE:
		return "CDR2_BE"
	case Repxt6bCDR2LE:
		return "CDR2_LE"
	case Repxt6bDCDR2BE:
		return "D_CDR2_BE"
	case Repxt6bDCDR2LE:
		return "D_CDR2_LE"
	case Repxt6bPLCDR2BE:
		return "PL_CDR2_BE"
	case Repxt6bPLCDR2LE:
		return "PL_CDR2_LE"
	default:
		return "UNKNOWN"
	}
}

// ParsedEncapsulationxt6b is one decoded 4 byte header.
type ParsedEncapsulationxt6b struct {
	// Representation is the Table 57 ID from the first two bytes.
	Representation RepresentationIDxt6b
	// Extensibility is the kind implied by the representation.
	Extensibility Extensibility
	// Version is the XCDR version implied by the representation.
	Version EncodingVersion
	// LittleEndian is the negotiated byte order.
	LittleEndian bool
}

// representationxt6bLayout is the decoded meaning of one ID.
type representationxt6bLayout struct {
	ext Extensibility
	ver EncodingVersion
	le  bool
}

// lookupxt6bRepresentation maps a Table 57 ID to kind plus version.
func lookupxt6bRepresentation(id RepresentationIDxt6b) (representationxt6bLayout, bool) {
	switch id {
	case Repxt6bCDRBE:
		return representationxt6bLayout{ExtFinal, XCDRVersion1, false}, true
	case Repxt6bCDRLE:
		return representationxt6bLayout{ExtFinal, XCDRVersion1, true}, true
	case Repxt6bPLCDRBE:
		return representationxt6bLayout{ExtMutable, XCDRVersion1, false}, true
	case Repxt6bPLCDRLE:
		return representationxt6bLayout{ExtMutable, XCDRVersion1, true}, true
	case Repxt6bCDR2BE:
		return representationxt6bLayout{ExtFinal, XCDRVersion2, false}, true
	case Repxt6bCDR2LE:
		return representationxt6bLayout{ExtFinal, XCDRVersion2, true}, true
	case Repxt6bDCDR2BE:
		return representationxt6bLayout{ExtAppendable, XCDRVersion2, false}, true
	case Repxt6bDCDR2LE:
		return representationxt6bLayout{ExtAppendable, XCDRVersion2, true}, true
	case Repxt6bPLCDR2BE:
		return representationxt6bLayout{ExtMutable, XCDRVersion2, false}, true
	case Repxt6bPLCDR2LE:
		return representationxt6bLayout{ExtMutable, XCDRVersion2, true}, true
	default:
		return representationxt6bLayout{}, false
	}
}

// ParseEncapsulationxt6b decodes a 4 byte header into kind plus version.
//
// All Table 57 XCDR IDs are accepted. XML and unknown IDs are rejected.
// Reserved option bytes are ignored for forward compatibility.
func ParseEncapsulationxt6b(header [4]byte) (ParsedEncapsulationxt6b, error) {
	var out ParsedEncapsulationxt6b
	if header[0] != 0x00 {
		return out, fmt.Errorf("envelope: bad encapsulation first byte 0x%02x: %w", header[0], ErrInvalid)
	}
	id := RepresentationIDxt6b(uint16(header[0])<<8 | uint16(header[1]))
	out.Representation = id
	layout, ok := lookupxt6bRepresentation(id)
	if !ok {
		return out, fmt.Errorf("envelope: unknown representation id 0x%04x: %w", uint16(id), ErrInvalid)
	}
	out.Extensibility = layout.ext
	out.Version = layout.ver
	out.LittleEndian = layout.le
	return out, nil
}

// RepresentationForxt6b selects the emitted ID for a kind plus version.
//
// Final and appendable use plain CDR under V1 and the delimited form
// under V2. Mutable uses the parameter list form under both versions.
// Unknown kinds and versions are rejected.
func RepresentationForxt6b(ext Extensibility, ver EncodingVersion, littleEndian bool) (RepresentationIDxt6b, error) {
	err := ver.Validate()
	if err != nil {
		return 0, err
	}
	if ver == XCDRVersion2 {
		return representationxt6bV2(ext, littleEndian)
	}
	return representationxt6bV1(ext, littleEndian)
}

// representationxt6bV1 selects the V1 ID for a kind.
func representationxt6bV1(ext Extensibility, littleEndian bool) (RepresentationIDxt6b, error) {
	switch ext {
	case ExtFinal, ExtAppendable:
		if littleEndian {
			return Repxt6bCDRLE, nil
		}
		return Repxt6bCDRBE, nil
	case ExtMutable:
		if littleEndian {
			return Repxt6bPLCDRLE, nil
		}
		return Repxt6bPLCDRBE, nil
	default:
		return 0, fmt.Errorf("envelope: unknown extensibility %d", int(ext))
	}
}

// representationxt6bV2 selects the V2 ID for a kind.
func representationxt6bV2(ext Extensibility, littleEndian bool) (RepresentationIDxt6b, error) {
	switch ext {
	case ExtFinal:
		if littleEndian {
			return Repxt6bCDR2LE, nil
		}
		return Repxt6bCDR2BE, nil
	case ExtAppendable:
		if littleEndian {
			return Repxt6bDCDR2LE, nil
		}
		return Repxt6bDCDR2BE, nil
	case ExtMutable:
		if littleEndian {
			return Repxt6bPLCDR2LE, nil
		}
		return Repxt6bPLCDR2BE, nil
	default:
		return 0, fmt.Errorf("envelope: unknown extensibility %d", int(ext))
	}
}

// EncodeEncapsulationxt6b builds the 4 byte header for an emitted sample.
//
// Final kinds keep the V1 IDs so existing DDS paths stay byte identical.
// Appendable and mutable kinds emit the V2 delimited IDs per Table 32.
// Option bytes start zero. For explicit V1 appendable (PLAIN_CDR) and V1
// mutable (PL_CDR) emits use EncodeEncapsulationxt6bWithVersion.
func EncodeEncapsulationxt6b(ext Extensibility, littleEndian bool) [4]byte {
	ver := XCDRVersion1
	if ext != ExtFinal {
		ver = XCDRVersion2
	}
	return EncodeEncapsulationxt6bWithVersion(ext, ver, littleEndian)
}

// EncodeEncapsulationxt6bWithVersion builds the header for an explicit version.
//
// V1 appendable emits PLAIN_CDR (0x0000/0x0001) and V1 mutable emits PL_CDR
// (0x0002/0x0003) per Table 57; V2 emits CDR2/D_CDR2/PL_CDR2 per Table 32.
// Unknown kinds fall back to plain CDR in the requested endian.
func EncodeEncapsulationxt6bWithVersion(ext Extensibility, ver EncodingVersion, littleEndian bool) [4]byte {
	id, err := RepresentationForxt6b(ext, ver, littleEndian)
	if err != nil {
		id = Repxt6bCDRLE
		if !littleEndian {
			id = Repxt6bCDRBE
		}
	}
	var h [4]byte
	h[0] = byte(uint16(id) >> 8)
	h[1] = byte(uint16(id) & 0xFF)
	return h
}
