// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

package rtps

import (
	"context"
	"crypto/md5" //nolint:gosec // G501: MD5 is the spec-mandated Checksum128_t digest per 9.4.2.15, not a security control
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"hash/crc64"
	"io"
	"log"
	"net"
	"sync"
	"time"
)

// TCP framing and HeaderExtension constants for RTPS over TCP.
const (
	// SubmessageHeaderExtension is the RTPS_HE submessage id per 9.4.5.1.
	SubmessageHeaderExtension uint8 = 0x00
	// TCPMaxFrameSize bounds one length-prefixed RTPS message on a stream.
	TCPMaxFrameSize = MaxDatagramSize
	// tcpFramePrefixSize is the 4 byte big-endian length prefix per frame.
	tcpFramePrefixSize = 4
	// tcpDefaultIOTimeout bounds one framed send or receive.
	tcpDefaultIOTimeout = 5 * time.Second
	// tcpRetryAttempts caps DialTCPWithRetry dials.
	tcpRetryAttempts = 8
	// tcpRetryBase is the first reconnect wait.
	tcpRetryBase = 50 * time.Millisecond
	// tcpRetryMax caps the reconnect wait.
	tcpRetryMax = 2 * time.Second
)

// HeaderExtension flag bits per 9.4.5.2.
const (
	// tcpExtFlagLength marks messageLength present.
	tcpExtFlagLength uint8 = 0x02
	// tcpExtFlagTimestamp marks rtpsSendTimestamp present.
	tcpExtFlagTimestamp uint8 = 0x04
	// tcpExtFlagUExtension marks uExtension4 present.
	tcpExtFlagUExtension uint8 = 0x08
	// tcpExtFlagWExtension marks wExtension8 present.
	tcpExtFlagWExtension uint8 = 0x10
	// tcpExtFlagChecksumC2 is the low checksum flag.
	tcpExtFlagChecksumC2 uint8 = 0x20
	// tcpExtFlagChecksumC1 is the high checksum flag.
	tcpExtFlagChecksumC1 uint8 = 0x40
	// tcpExtFlagParameters marks the parameter list present.
	tcpExtFlagParameters uint8 = 0x80
)

// HeaderExtension field sizes on the wire.
const (
	// tcpMessageLengthLen is the messageLength field size.
	tcpMessageLengthLen = 4
	// tcpTimestampLen is the timestamp field size.
	tcpTimestampLen = 8
	// tcpUExtensionLen is the uExtension4 field size.
	tcpUExtensionLen = 4
	// tcpWExtensionLen is the wExtension8 field size.
	tcpWExtensionLen = 8
	// tcpChecksum32Len is the CRC-32C field size.
	tcpChecksum32Len = 4
	// tcpChecksum64Len is the CRC-64/XZ field size.
	tcpChecksum64Len = 8
	// tcpChecksum128Len is the MD5 field size.
	tcpChecksum128Len = 16
)

// CRC tables for the HeaderExtension checksum per 9.4.2.15.
var (
	// tcpCRC32CTable holds the Castagnoli table for Checksum32_t.
	tcpCRC32CTable = crc32.MakeTable(crc32.Castagnoli)
	// tcpCRC64XZTable holds the ECMA table for Checksum64_t.
	tcpCRC64XZTable = crc64.MakeTable(crc64.ECMA)
)

// TCPChecksumKind selects the HeaderExtension checksum algorithm.
type TCPChecksumKind int

const (
	// TCPChecksumNone omits the checksum field.
	TCPChecksumNone TCPChecksumKind = iota
	// TCPChecksumCRC32 selects CRC-32C per 9.4.2.15.
	TCPChecksumCRC32
	// TCPChecksumCRC64 selects CRC-64/XZ per 9.4.2.15.
	TCPChecksumCRC64
	// TCPChecksumMD5 selects the MD5 digest per 9.4.2.15.
	TCPChecksumMD5
)

// TCPDropCounter counts dropped TCP inputs for observability.
//
// Drops increments once per rejected frame, truncated stream, failed
// checksum, or malformed extension. It is safe for concurrent use.
// A nil counter records nothing.
type TCPDropCounter struct {
	// mu guards drops for concurrent use.
	mu sync.Mutex
	// drops counts rejected inputs.
	drops uint64
}

// RecordDrop adds one drop, treating nil as a no-op.
func (c *TCPDropCounter) RecordDrop() {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.drops++
}

// Count returns the drop total, treating nil as zero.
func (c *TCPDropCounter) Count() uint64 {
	if c == nil {
		return 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.drops
}

// dropf records one drop and logs it loudly.
func (c *TCPDropCounter) dropf(format string, args ...any) {
	c.RecordDrop()
	log.Printf("rtps: tcp "+format, args...)
}

// HeaderExtension carries the RTPS HeaderExtension submessage fields per
// 8.3.7 and 9.4.5.2. Only flagged fields appear on the wire. Parameters
// pass through opaquely; no parameter ids are defined by this version of
// the protocol.
type HeaderExtension struct {
	// LittleEndian selects the byte order for multi-byte fields.
	LittleEndian bool
	// HasLength marks messageLength present.
	HasLength bool
	// MessageLength is the full message length from the RTPS header.
	MessageLength uint32
	// HasTimestamp marks the send timestamp present.
	HasTimestamp bool
	// Seconds is the send timestamp seconds.
	Seconds uint32
	// Fraction is the send timestamp fraction.
	Fraction uint32
	// HasUExtension marks uExtension4 present.
	HasUExtension bool
	// UExtension is the 4 byte future-use field, wire order fixed per 9.4.2.17.
	UExtension uint32
	// HasWExtension marks wExtension8 present.
	HasWExtension bool
	// WExtension is the 8 byte future-use field, wire order fixed per 9.4.2.18.
	WExtension uint64
	// Checksum selects the checksum algorithm, or none.
	Checksum TCPChecksumKind
	// ChecksumValue holds the checksum bytes: 4, 8, or 16 per kind.
	ChecksumValue []byte
	// Parameters carries the passthrough parameter list.
	Parameters []Parameter
}

// flags renders the submessage flags for the present fields.
func (e HeaderExtension) flags() uint8 {
	var flags uint8
	if e.LittleEndian {
		flags |= FlagLittleEndian
	}
	if e.HasLength {
		flags |= tcpExtFlagLength
	}
	if e.HasTimestamp {
		flags |= tcpExtFlagTimestamp
	}
	if e.HasUExtension {
		flags |= tcpExtFlagUExtension
	}
	if e.HasWExtension {
		flags |= tcpExtFlagWExtension
	}
	if e.Checksum == TCPChecksumCRC32 || e.Checksum == TCPChecksumMD5 {
		flags |= tcpExtFlagChecksumC2
	}
	if e.Checksum == TCPChecksumCRC64 || e.Checksum == TCPChecksumMD5 {
		flags |= tcpExtFlagChecksumC1
	}
	if len(e.Parameters) > 0 {
		flags |= tcpExtFlagParameters
	}
	return flags
}

// checksumKindLen returns the wire length for a kind, or -1 when unknown.
func checksumKindLen(kind TCPChecksumKind) int {
	switch kind {
	case TCPChecksumNone:
		return 0
	case TCPChecksumCRC32:
		return tcpChecksum32Len
	case TCPChecksumCRC64:
		return tcpChecksum64Len
	case TCPChecksumMD5:
		return tcpChecksum128Len
	default:
		return -1
	}
}

// checksumKindFromFlags reads the checksum kind from extension flags.
func checksumKindFromFlags(flags uint8) TCPChecksumKind {
	c1 := flags&tcpExtFlagChecksumC1 != 0
	c2 := flags&tcpExtFlagChecksumC2 != 0
	switch {
	case c1 && c2:
		return TCPChecksumMD5
	case c1:
		return TCPChecksumCRC64
	case c2:
		return TCPChecksumCRC32
	default:
		return TCPChecksumNone
	}
}

// TCPComputeChecksum returns the wire bytes for kind over msg. CRC outputs
// use big-endian order per 9.4.2.15; MD5 is raw digest bytes.
func TCPComputeChecksum(kind TCPChecksumKind, msg []byte) ([]byte, error) {
	switch kind {
	case TCPChecksumCRC32:
		sum := crc32.Checksum(msg, tcpCRC32CTable)
		out := make([]byte, tcpChecksum32Len)
		binary.BigEndian.PutUint32(out, sum)
		return out, nil
	case TCPChecksumCRC64:
		sum := crc64.Checksum(msg, tcpCRC64XZTable)
		out := make([]byte, tcpChecksum64Len)
		binary.BigEndian.PutUint64(out, sum)
		return out, nil
	case TCPChecksumMD5:
		sum := md5.Sum(msg) //nolint:gosec // G501: MD5 is the spec-mandated Checksum128_t digest, not a security control
		out := make([]byte, tcpChecksum128Len)
		copy(out, sum[:])
		return out, nil
	case TCPChecksumNone:
		return nil, errors.New("rtps: tcp checksum kind none (9.4.2.15)") //nolint:wrapcheck // new error, nothing to wrap
	default:
		return nil, fmt.Errorf("rtps: tcp checksum kind %d unknown (9.4.2.15)", int(kind))
	}
}

// appendTCPWireU32 appends v in the extension byte order.
func appendTCPWireU32(out []byte, v uint32, littleEndian bool) []byte {
	var slot [4]byte
	putUint32(slot[:], v, littleEndian)
	return append(out, slot[:]...)
}

// appendTCPWireU32BE appends v in fixed big-endian order for octet arrays.
func appendTCPWireU32BE(out []byte, v uint32) []byte {
	var slot [4]byte
	binary.BigEndian.PutUint32(slot[:], v)
	return append(out, slot[:]...)
}

// appendTCPWireU64BE appends v in fixed big-endian order for octet arrays.
func appendTCPWireU64BE(out []byte, v uint64) []byte {
	var slot [8]byte
	binary.BigEndian.PutUint64(slot[:], v)
	return append(out, slot[:]...)
}

// encodeBody renders the extension content in flag order.
func (e HeaderExtension) encodeBody() ([]byte, error) {
	want := checksumKindLen(e.Checksum)
	if want < 0 {
		return nil, fmt.Errorf("rtps: tcp extension checksum kind %d unknown (8.3.7.3)", int(e.Checksum))
	}
	if e.Checksum == TCPChecksumNone && len(e.ChecksumValue) != 0 {
		return nil, errors.New("rtps: tcp extension checksum value without kind (8.3.7.3)") //nolint:wrapcheck // new error, nothing to wrap
	}
	if e.Checksum != TCPChecksumNone && len(e.ChecksumValue) != want {
		return nil, fmt.Errorf("rtps: tcp extension checksum need %d have %d (8.3.7.3)", want, len(e.ChecksumValue))
	}
	out := make([]byte, 0, tcpTimestampLen+len(e.ChecksumValue))
	if e.HasLength {
		out = appendTCPWireU32(out, e.MessageLength, e.LittleEndian)
	}
	if e.HasTimestamp {
		out = appendTCPWireU32(out, e.Seconds, e.LittleEndian)
		out = appendTCPWireU32(out, e.Fraction, e.LittleEndian)
	}
	if e.HasUExtension {
		out = appendTCPWireU32BE(out, e.UExtension)
	}
	if e.HasWExtension {
		out = appendTCPWireU64BE(out, e.WExtension)
	}
	out = append(out, e.ChecksumValue...)
	params, err := encodeExtensionParams(e.Parameters, e.LittleEndian)
	if err != nil {
		return nil, err
	}
	out = append(out, params...)
	if len(out) > maxSubmessageBody {
		return nil, fmt.Errorf("rtps: tcp extension body %d above %d (8.3.7.3)", len(out), maxSubmessageBody)
	}
	return out, nil
}

// encodeExtensionParams renders params without a trailing sentinel. The
// sentinel is not required in the HeaderExtension per 9.4.2.11.
func encodeExtensionParams(params []Parameter, littleEndian bool) ([]byte, error) {
	out := make([]byte, 0, len(params)*(parameterHeaderSize+4))
	for i := range params {
		if len(params[i].Value) > maxSubmessageBody {
			return nil, fmt.Errorf("rtps: tcp extension parameter %#04x value %d above %d (9.4.2.11)", params[i].ID, len(params[i].Value), maxSubmessageBody)
		}
		out = appendParamEntry(out, params[i].ID, params[i].Value, littleEndian)
	}
	return out, nil
}

// EncodeHeaderExtension frames e as one RTPS_HE submessage.
func EncodeHeaderExtension(e HeaderExtension) ([]byte, error) {
	body, err := e.encodeBody()
	if err != nil {
		return nil, err
	}
	framed, err := WrapSubmessage(SubmessageHeaderExtension, e.flags(), body)
	if err != nil {
		return nil, fmt.Errorf("rtps: tcp encode extension: %w", err)
	}
	return framed, nil
}

// checkExtensionParamKnown rejects unknown must-understand parameters. Per
// Table 9.6 an unrecognized id with the 0x4000 bit rejects the message;
// other unknown ids pass through opaquely.
func checkExtensionParamKnown(id uint16) error {
	if id&0x4000 != 0 {
		return fmt.Errorf("rtps: tcp extension param %#04x must understand (9.4.2.11)", id)
	}
	return nil
}

// decodeExtensionParams parses extension parameters with passthrough. A
// trailing sentinel is accepted; trailing bytes after it are malformed.
func decodeExtensionParams(body []byte, littleEndian bool, drops *TCPDropCounter) ([]Parameter, error) {
	var params []Parameter
	off := 0
	for off < len(body) {
		id, length, ok := paramHeaderAt(body, off, littleEndian)
		if !ok {
			drops.dropf("tcp extension param header need %d have %d", parameterHeaderSize, len(body)-off)
			return nil, fmt.Errorf("rtps: tcp extension param header need %d have %d (9.4.2.11)", parameterHeaderSize, len(body)-off)
		}
		off += parameterHeaderSize
		if id == ParamSentinel {
			err := checkSentinelLength(length)
			if err != nil {
				drops.dropf("tcp extension sentinel carries bytes")
				return nil, fmt.Errorf("rtps: tcp extension sentinel (9.4.2.11): %w", err)
			}
			if off != len(body) {
				drops.dropf("tcp extension trailing %d after sentinel", len(body)-off)
				return nil, fmt.Errorf("rtps: tcp extension trailing %d after sentinel (9.4.2.11)", len(body)-off)
			}
			return params, nil
		}
		err := checkExtensionParamKnown(id)
		if err != nil {
			drops.dropf("tcp extension param %#04x must understand", id)
			return nil, err
		}
		value, next, err := copyParameterValue(body, off, length, id)
		if err != nil {
			drops.dropf("tcp extension param %#04x truncated", id)
			return nil, fmt.Errorf("rtps: tcp extension params (9.4.2.11): %w", err)
		}
		params = append(params, Parameter{ID: id, Value: value})
		off = next
	}
	return params, nil
}

// extensionFixedLen returns the flagged fixed field size.
func extensionFixedLen(flags uint8) int {
	fixedLen := 0
	if flags&tcpExtFlagLength != 0 {
		fixedLen += tcpMessageLengthLen
	}
	if flags&tcpExtFlagTimestamp != 0 {
		fixedLen += tcpTimestampLen
	}
	if flags&tcpExtFlagUExtension != 0 {
		fixedLen += tcpUExtensionLen
	}
	if flags&tcpExtFlagWExtension != 0 {
		fixedLen += tcpWExtensionLen
	}
	return fixedLen
}

// decodeExtensionFixed parses the flagged fixed fields, returning the
// offset of the checksum or parameter section. The caller guarantees the
// body holds every flagged field; see extensionFixedLen.
func decodeExtensionFixed(body []byte, flags uint8, out *HeaderExtension) int {
	off := 0
	if flags&tcpExtFlagLength != 0 {
		out.HasLength = true
		out.MessageLength = uint32Value(body[off:off+tcpMessageLengthLen], out.LittleEndian)
		off += tcpMessageLengthLen
	}
	if flags&tcpExtFlagTimestamp != 0 {
		out.HasTimestamp = true
		out.Seconds = uint32Value(body[off:off+4], out.LittleEndian)
		out.Fraction = uint32Value(body[off+4:off+8], out.LittleEndian)
		off += tcpTimestampLen
	}
	if flags&tcpExtFlagUExtension != 0 {
		out.HasUExtension = true
		out.UExtension = binary.BigEndian.Uint32(body[off : off+tcpUExtensionLen])
		off += tcpUExtensionLen
	}
	if flags&tcpExtFlagWExtension != 0 {
		out.HasWExtension = true
		out.WExtension = binary.BigEndian.Uint64(body[off : off+tcpWExtensionLen])
		off += tcpWExtensionLen
	}
	return off
}

// copyExtensionChecksum copies the checksum field at off.
func copyExtensionChecksum(body []byte, off, cksLen int, out *HeaderExtension) int {
	out.ChecksumValue = append([]byte(nil), body[off:off+cksLen]...)
	return off + cksLen
}

// DecodeHeaderExtension parses one extension body with validity checks per
// 8.3.7.3: flagged fields must fit, and trailing bytes need the parameter
// flag. Checksum verification needs the full frame; see DecodeTCPMessage.
func DecodeHeaderExtension(body []byte, flags uint8, drops *TCPDropCounter) (HeaderExtension, error) {
	var out HeaderExtension
	out.LittleEndian = LittleEndian(flags)
	out.Checksum = checksumKindFromFlags(flags)
	wantLen := extensionFixedLen(flags) + checksumKindLen(out.Checksum)
	if len(body) < wantLen {
		drops.dropf("tcp extension need %d have %d", wantLen, len(body))
		return out, fmt.Errorf("rtps: tcp extension need %d have %d (8.3.7.3)", wantLen, len(body))
	}
	off := decodeExtensionFixed(body, flags, &out)
	cksLen := checksumKindLen(out.Checksum)
	if cksLen > 0 {
		off = copyExtensionChecksum(body, off, cksLen, &out)
	}
	if flags&tcpExtFlagParameters != 0 {
		params, err := decodeExtensionParams(body[off:], out.LittleEndian, drops)
		if err != nil {
			return HeaderExtension{}, fmt.Errorf("rtps: tcp extension (8.3.7.3): %w", err)
		}
		out.Parameters = params
	} else if off != len(body) {
		drops.dropf("tcp extension trailing %d without params flag", len(body)-off)
		return HeaderExtension{}, fmt.Errorf("rtps: tcp extension trailing %d without params flag (8.3.7.3)", len(body)-off)
	}
	return out, nil
}

// checkTCPMessageSize rejects messages above the stream bound.
func checkTCPMessageSize(n int) error {
	if n > TCPMaxFrameSize {
		return fmt.Errorf("rtps: tcp message %d above max %d", n, TCPMaxFrameSize)
	}
	return nil
}

// assembleTCPMessage concatenates header, framed extension, and submessages.
func assembleTCPMessage(h Header, extBytes []byte, subs []RawSubmessage) ([]byte, error) {
	raw := h.Encode()
	out := make([]byte, 0, len(raw)+len(extBytes)+len(subs)*8)
	out = append(out, raw[:]...)
	out = append(out, extBytes...)
	for i := range subs {
		framed, err := WrapSubmessage(subs[i].ID, subs[i].Flags, subs[i].Body)
		if err != nil {
			return nil, fmt.Errorf("rtps: tcp encode submessage %d: %w", i, err)
		}
		out = append(out, framed...)
	}
	err := checkTCPMessageSize(len(out))
	if err != nil {
		return nil, err
	}
	return out, nil
}

// extensionChecksumBodyOffset returns the checksum offset within the body.
func extensionChecksumBodyOffset(e HeaderExtension) int {
	off := 0
	if e.HasLength {
		off += tcpMessageLengthLen
	}
	if e.HasTimestamp {
		off += tcpTimestampLen
	}
	if e.HasUExtension {
		off += tcpUExtensionLen
	}
	if e.HasWExtension {
		off += tcpWExtensionLen
	}
	return off
}

// encodeTCPMessageWithExtension runs the two-pass length plus checksum fill.
// The checksum covers the full message with its own field zeroed per 8.3.7.
func encodeTCPMessageWithExtension(h Header, e HeaderExtension, subs []RawSubmessage) ([]byte, error) {
	cksLen := checksumKindLen(e.Checksum)
	if cksLen < 0 {
		return nil, fmt.Errorf("rtps: tcp extension checksum kind %d unknown (8.3.7.3)", int(e.Checksum))
	}
	zeroed := e
	zeroed.ChecksumValue = make([]byte, cksLen)
	extBytes, err := EncodeHeaderExtension(zeroed)
	if err != nil {
		return nil, err
	}
	msg, err := assembleTCPMessage(h, extBytes, subs)
	if err != nil {
		return nil, err
	}
	if e.HasLength {
		zeroed.MessageLength = uint32(len(msg)) //nolint:gosec // G115: length bounded by checkTCPMessageSize
		extBytes, err = EncodeHeaderExtension(zeroed)
		if err != nil {
			return nil, err
		}
		msg, err = assembleTCPMessage(h, extBytes, subs)
		if err != nil {
			return nil, err
		}
	}
	if cksLen > 0 {
		sum, err := TCPComputeChecksum(e.Checksum, msg)
		if err != nil {
			return nil, err
		}
		off := HeaderSize + SubmessageHeaderSize + extensionChecksumBodyOffset(zeroed)
		copy(msg[off:off+cksLen], sum)
	}
	return msg, nil
}

// EncodeTCPMessage frames header plus optional extension plus submessages.
// A nil extension omits RTPS_HE entirely. MessageLength, when flagged, is
// filled with the full message length; the checksum, when selected, is
// computed over the message with its own field zeroed.
func EncodeTCPMessage(h Header, ext *HeaderExtension, subs []RawSubmessage) ([]byte, error) {
	if ext == nil {
		msg, err := EncodeMessage(h, subs)
		if err != nil {
			return nil, fmt.Errorf("rtps: tcp encode message: %w", err)
		}
		err = checkTCPMessageSize(len(msg))
		if err != nil {
			return nil, err
		}
		return msg, nil
	}
	msg, err := encodeTCPMessageWithExtension(h, *ext, subs)
	if err != nil {
		return nil, err
	}
	return msg, nil
}

// checkTCPExtensionLength validates messageLength against the frame. On TCP
// the frame length is the message length, so a flagged length must match
// exactly; a short length is invalid per 8.3.7.3.
func checkTCPExtensionLength(ext HeaderExtension, bodyLen, frameLen int, drops *TCPDropCounter) error {
	if !ext.HasLength {
		return nil
	}
	if uint64(ext.MessageLength) < uint64(HeaderSize+SubmessageHeaderSize+bodyLen) { //nolint:gosec // G115: bodyLen bounded by TCPMaxFrameSize
		drops.dropf("tcp extension length below header")
		return fmt.Errorf("rtps: tcp extension length %d below header plus extension (8.3.7.3)", ext.MessageLength)
	}
	if uint64(ext.MessageLength) != uint64(frameLen) { //nolint:gosec // G115: frameLen bounded by TCPMaxFrameSize
		drops.dropf("tcp extension length mismatch")
		return fmt.Errorf("rtps: tcp extension length %d differs from frame %d (8.3.7.3)", ext.MessageLength, frameLen)
	}
	return nil
}

// equalTCPChecksum compares digests without leaking length info.
func equalTCPChecksum(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	match := true
	for i := range a {
		if a[i] != b[i] {
			match = false
		}
	}
	return match
}

// verifyTCPChecksum checks the extension checksum with its field zeroed.
// The frame stays in sync, so a mismatch drops the message but keeps the
// connection open for the next frame. Offsets bound-check against the
// frame before any probe or frame slice so a crafted flagged length can
// never panic.
func verifyTCPChecksum(frame []byte, ext HeaderExtension, extOff int, drops *TCPDropCounter) error {
	cksLen := checksumKindLen(ext.Checksum)
	if cksLen <= 0 {
		return fmt.Errorf("rtps: tcp extension checksum kind %d unknown (8.3.7.3)", int(ext.Checksum))
	}
	cksOff := extOff + SubmessageHeaderSize + extensionChecksumBodyOffset(ext)
	if cksOff < 0 || cksOff+cksLen > len(frame) {
		drops.dropf("tcp extension checksum overruns frame")
		return fmt.Errorf("rtps: tcp extension checksum overruns %d (8.3.7.3)", len(frame))
	}
	probe := append([]byte(nil), frame...)
	for i := range cksLen {
		probe[cksOff+i] = 0
	}
	sum, err := TCPComputeChecksum(ext.Checksum, probe)
	if err != nil {
		return err
	}
	if !equalTCPChecksum(sum, frame[cksOff:cksOff+cksLen]) {
		drops.dropf("tcp extension checksum mismatch")
		return errors.New("rtps: tcp extension checksum mismatch (8.3.7.3)") //nolint:wrapcheck // new error, nothing to wrap
	}
	return nil
}

// decodeTCPFirstExtension interprets a leading RTPS_HE submessage per
// DDSI-RTPS 2.5 9.4.5.1.3. A non-extension first submessage means no
// extension. A zero octetsToNextHeader extends to the frame end per
// 9.4.5.1.3. An RTPS_HE in any later position is rejected fail-closed;
// see checkTCPLaterExtensions.
func decodeTCPFirstExtension(frame []byte, off int, drops *TCPDropCounter) (*HeaderExtension, int, error) {
	sh, err := DecodeSubmessageHeader(frame[off:])
	if err != nil {
		drops.dropf("tcp extension header truncated")
		return nil, 0, fmt.Errorf("rtps: tcp decode extension header (8.3.7.3): %w", err)
	}
	if sh.ID != SubmessageHeaderExtension {
		return nil, off, nil
	}
	bodyLen := int(sh.Length)
	if bodyLen == 0 {
		bodyLen = len(frame) - off - SubmessageHeaderSize
	}
	if bodyLen < 0 || off+SubmessageHeaderSize+bodyLen > len(frame) {
		drops.dropf("tcp extension overruns frame")
		return nil, 0, fmt.Errorf("rtps: tcp extension length %d overruns %d left (8.3.7.3)", sh.Length, len(frame)-off)
	}
	body := frame[off+SubmessageHeaderSize : off+SubmessageHeaderSize+bodyLen]
	ext, err := DecodeHeaderExtension(body, sh.Flags, drops)
	if err != nil {
		return nil, 0, fmt.Errorf("rtps: tcp decode extension (8.3.7.3): %w", err)
	}
	err = checkTCPExtensionLength(ext, bodyLen, len(frame), drops)
	if err != nil {
		return nil, 0, err
	}
	if ext.Checksum != TCPChecksumNone {
		err = verifyTCPChecksum(frame, ext, off, drops)
		if err != nil {
			return nil, 0, err
		}
	}
	return &ext, off + SubmessageHeaderSize + bodyLen, nil
}

// checkTCPLaterExtensions validates every later RTPS_HE in the frame per
// DDSI-RTPS 2.5 9.4.5.1. Each later extension must be well-formed and is
// then rejected as mispositioned fail-closed: the HeaderExtension must
// be first per 8.3.7.3.
func checkTCPLaterExtensions(subs []RawSubmessage, drops *TCPDropCounter) error {
	for i := range subs {
		if subs[i].ID != SubmessageHeaderExtension {
			continue
		}
		var tmp TCPDropCounter
		_, err := DecodeHeaderExtension(subs[i].Body, subs[i].Flags, &tmp)
		if err != nil {
			drops.dropf("tcp later extension invalid at %d", i)
			return fmt.Errorf("rtps: tcp later extension invalid at %d (8.3.7.3): %w", i, err)
		}
		drops.dropf("tcp later extension not first at %d", i)
		return fmt.Errorf("rtps: tcp later extension not first at %d (8.3.7.3)", i)
	}
	return nil
}

// collectTCPSubmessages parses raw submessages from off with drop accounting.
func collectTCPSubmessages(frame []byte, off int, drops *TCPDropCounter) ([]RawSubmessage, error) {
	var subs []RawSubmessage
	for off < len(frame) {
		if len(subs) >= maxSubmessagesPerDatagram {
			drops.dropf("tcp message submessages above cap")
			return nil, fmt.Errorf("rtps: tcp decode message submessages above %d (8.3.4.1)", maxSubmessagesPerDatagram)
		}
		sub, next, err := decodeOne(frame, off)
		if err != nil {
			drops.dropf("tcp message submessage truncated")
			return nil, fmt.Errorf("rtps: tcp decode message (8.3.4.1): %w", err)
		}
		subs = append(subs, sub)
		off = next
	}
	return subs, nil
}

// DecodeTCPMessage parses one reassembled TCP frame into its header, its
// leading extension when present, and the remaining submessages per
// 9.4.5.1. Messages without an extension decode with a nil extension.
// A later RTPS_HE is rejected fail-closed per 8.3.7.3. Any failure
// records one drop and returns an error; decode never panics.
func DecodeTCPMessage(frame []byte, drops *TCPDropCounter) (Header, *HeaderExtension, []RawSubmessage, error) {
	var zero Header
	hdr, err := DecodeHeader(frame)
	if err != nil {
		drops.dropf("tcp message header rejected")
		return zero, nil, nil, fmt.Errorf("rtps: tcp decode message (8.3.4.1): %w", err)
	}
	off := HeaderSize
	if off == len(frame) {
		return hdr, nil, nil, nil
	}
	ext, next, err := decodeTCPFirstExtension(frame, off, drops)
	if err != nil {
		return zero, nil, nil, err
	}
	rest, err := collectTCPSubmessages(frame, next, drops)
	if err != nil {
		return zero, nil, nil, err
	}
	err = checkTCPLaterExtensions(rest, drops)
	if err != nil {
		return zero, nil, nil, err
	}
	return hdr, ext, rest, nil
}

// writeTCPFull writes buf until complete, looping short writes.
func writeTCPFull(conn *net.TCPConn, buf []byte) error {
	for len(buf) > 0 {
		sent, err := conn.Write(buf)
		if err != nil {
			return fmt.Errorf("rtps: tcp write: %w", err)
		}
		if sent <= 0 {
			return errors.New("rtps: tcp write zero") //nolint:wrapcheck // new error, nothing to wrap
		}
		buf = buf[sent:]
	}
	return nil
}

// WriteTCPFrame sends one length-prefixed message over conn. Empty and
// oversize payloads drop loudly without touching the stream.
func WriteTCPFrame(conn *net.TCPConn, msg []byte, timeout time.Duration, drops *TCPDropCounter) error {
	if len(msg) == 0 {
		drops.dropf("tcp send empty payload")
		return errors.New("rtps: tcp send drops empty payload") //nolint:wrapcheck // new error, nothing to wrap
	}
	if len(msg) > TCPMaxFrameSize {
		drops.dropf("tcp send oversize")
		return fmt.Errorf("rtps: tcp send %d above max %d", len(msg), TCPMaxFrameSize)
	}
	if timeout > 0 {
		err := conn.SetWriteDeadline(time.Now().Add(timeout))
		if err != nil {
			return fmt.Errorf("rtps: tcp send deadline: %w", err)
		}
	}
	var prefix [tcpFramePrefixSize]byte
	binary.BigEndian.PutUint32(prefix[:], uint32(len(msg))) //nolint:gosec // G115: length bounded by TCPMaxFrameSize
	err := writeTCPFull(conn, prefix[:])
	if err != nil {
		return fmt.Errorf("rtps: tcp send prefix: %w", err)
	}
	err = writeTCPFull(conn, msg)
	if err != nil {
		return fmt.Errorf("rtps: tcp send body: %w", err)
	}
	return nil
}

// checkTCPFrameSize rejects empty and oversize frame lengths before any
// body allocation, bounding receive memory.
func checkTCPFrameSize(size uint32, drops *TCPDropCounter) error {
	if size == 0 {
		drops.dropf("tcp receive empty frame")
		return errors.New("rtps: tcp receive drops empty frame") //nolint:wrapcheck // new error, nothing to wrap
	}
	if size > TCPMaxFrameSize {
		drops.dropf("tcp receive oversize frame")
		return fmt.Errorf("rtps: tcp receive %d above max %d", size, TCPMaxFrameSize)
	}
	return nil
}

// ReadTCPFrame reads one length-prefixed message, assembling partial reads
// until the frame is complete. Truncated streams and oversize lengths drop
// loudly; the stream is then out of sync and the caller must close it.
func ReadTCPFrame(conn *net.TCPConn, timeout time.Duration, drops *TCPDropCounter) ([]byte, error) {
	if timeout > 0 {
		err := conn.SetReadDeadline(time.Now().Add(timeout))
		if err != nil {
			return nil, fmt.Errorf("rtps: tcp receive deadline: %w", err)
		}
	}
	var prefix [tcpFramePrefixSize]byte
	_, err := io.ReadFull(conn, prefix[:])
	if err != nil {
		drops.dropf("tcp receive prefix truncated")
		return nil, fmt.Errorf("rtps: tcp receive prefix: %w", err)
	}
	size := binary.BigEndian.Uint32(prefix[:])
	err = checkTCPFrameSize(size, drops)
	if err != nil {
		return nil, err
	}
	body := make([]byte, size)
	_, err = io.ReadFull(conn, body)
	if err != nil {
		drops.dropf("tcp receive body truncated")
		return nil, fmt.Errorf("rtps: tcp receive body: %w", err)
	}
	return body, nil
}

// IsTCPTimeout reports whether err is a network timeout.
func IsTCPTimeout(err error) bool {
	return isTimeout(err)
}

// TCPConnConfig bounds per-connection IO. Non-positive values take defaults.
type TCPConnConfig struct {
	// ReadTimeout bounds one framed receive.
	ReadTimeout time.Duration
	// WriteTimeout bounds one framed send.
	WriteTimeout time.Duration
}

// DefaultTCPConnConfig returns the standard IO bounds.
func DefaultTCPConnConfig() TCPConnConfig {
	return TCPConnConfig{ReadTimeout: tcpDefaultIOTimeout, WriteTimeout: tcpDefaultIOTimeout}
}

// withDefaults fills non-positive timeouts with defaults.
func (c TCPConnConfig) withDefaults() TCPConnConfig {
	out := c
	if out.ReadTimeout <= 0 {
		out.ReadTimeout = tcpDefaultIOTimeout
	}
	if out.WriteTimeout <= 0 {
		out.WriteTimeout = tcpDefaultIOTimeout
	}
	return out
}

// TCPConnection is one handshake-less RTPS stream. RTPS over TCP adds no
// handshake beyond the RTPS messages themselves, so a dialed socket is
// immediately ready. Sends serialize on a mutex so concurrent callers
// cannot interleave frames; one reader at a time is expected and guarded
// the same way. Any framing failure fails the connection so a fresh dial
// resynchronizes the stream; message-level failures keep it open.
type TCPConnection struct {
	// guard owns the closed flag for concurrent use.
	guard closeGuard
	// conn carries framed RTPS messages.
	conn *net.TCPConn
	// cfg bounds per-call IO.
	cfg TCPConnConfig
	// sendMu serializes concurrent sends.
	sendMu sync.Mutex
	// recvMu serializes concurrent receives.
	recvMu sync.Mutex
	// drops counts framing and decode failures.
	drops TCPDropCounter
}

// DialTCP opens one RTPS stream to addr with no extra handshake. The
// context only gates the dial; a canceled context fails the dial.
func DialTCP(ctx context.Context, addr string, cfg TCPConnConfig) (*TCPConnection, error) {
	err := checkContext(ctx, "tcp dial")
	if err != nil {
		return nil, err
	}
	dialer := net.Dialer{}
	raw, err := dialer.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("rtps: tcp dial %s: %w", addr, err)
	}
	tcp, ok := raw.(*net.TCPConn)
	if !ok {
		cerr := raw.Close()
		if cerr != nil {
			return nil, fmt.Errorf("rtps: tcp dial %s non-tcp close: %w", addr, cerr)
		}
		return nil, fmt.Errorf("rtps: tcp dial %s non-tcp conn", addr)
	}
	return &TCPConnection{conn: tcp, cfg: cfg.withDefaults()}, nil
}

// Send writes one framed message, serializing concurrent callers. Empty and
// oversize payloads fail loudly. Use after Close reports ErrClosed.
func (c *TCPConnection) Send(ctx context.Context, msg []byte) error {
	err := checkContext(ctx, "tcp send")
	if err != nil {
		return err
	}
	if c.isClosed() {
		return fmt.Errorf("rtps: tcp send: %w", ErrClosed)
	}
	c.sendMu.Lock()
	defer c.sendMu.Unlock()
	if c.isClosed() {
		return fmt.Errorf("rtps: tcp send: %w", ErrClosed)
	}
	err = WriteTCPFrame(c.conn, msg, c.cfg.WriteTimeout, &c.drops)
	if err != nil {
		if c.isClosed() {
			return fmt.Errorf("rtps: tcp send: %w", ErrClosed)
		}
		cerr := ctx.Err()
		if cerr != nil {
			return fmt.Errorf("rtps: tcp send: %w", cerr)
		}
		return err
	}
	return nil
}

// mapReceiveError fails the connection on stream desync and maps cancel.
// Timeouts keep the connection open for retry; every other IO failure
// closes it so the next call reports ErrClosed and the peer redials.
func (c *TCPConnection) mapReceiveError(ctx context.Context, err error) error {
	if c.isClosed() {
		return fmt.Errorf("rtps: tcp receive: %w", ErrClosed)
	}
	cerr := ctx.Err()
	if cerr != nil {
		return fmt.Errorf("rtps: tcp receive: %w", cerr)
	}
	if IsTCPTimeout(err) {
		return err
	}
	if c.guard.tryClose() {
		closeErr := c.conn.Close()
		if closeErr != nil {
			return fmt.Errorf("rtps: tcp receive: %w", errors.Join(err, closeErr))
		}
	}
	return err
}

// Receive reads one framed message. It blocks until a frame arrives, the
// read timeout fires, the context ends, or Close runs. Framing failures
// close the connection; timeouts do not.
func (c *TCPConnection) Receive(ctx context.Context) ([]byte, error) {
	err := checkContext(ctx, "tcp receive")
	if err != nil {
		return nil, err
	}
	if c.isClosed() {
		return nil, fmt.Errorf("rtps: tcp receive: %w", ErrClosed)
	}
	c.recvMu.Lock()
	defer c.recvMu.Unlock()
	if c.isClosed() {
		return nil, fmt.Errorf("rtps: tcp receive: %w", ErrClosed)
	}
	frame, err := ReadTCPFrame(c.conn, c.cfg.ReadTimeout, &c.drops)
	if err != nil {
		return nil, c.mapReceiveError(ctx, err)
	}
	return frame, nil
}

// ReceiveMessage reads one frame and decodes its header, extension, and
// submessages. Message-level failures such as a bad checksum drop loudly
// but keep the connection open, since the frame was fully consumed.
func (c *TCPConnection) ReceiveMessage(ctx context.Context) (Header, *HeaderExtension, []RawSubmessage, error) {
	var zero Header
	frame, err := c.Receive(ctx)
	if err != nil {
		return zero, nil, nil, err
	}
	hdr, ext, subs, err := DecodeTCPMessage(frame, &c.drops)
	if err != nil {
		return zero, nil, nil, fmt.Errorf("rtps: tcp receive message (8.3.4.1): %w", err)
	}
	return hdr, ext, subs, nil
}

// Drops returns the framing and decode drop total.
func (c *TCPConnection) Drops() uint64 {
	return c.drops.Count()
}

// RemoteAddr returns the peer address.
func (c *TCPConnection) RemoteAddr() net.Addr {
	return c.conn.RemoteAddr()
}

// IsClosed reports whether Close has run.
func (c *TCPConnection) IsClosed() bool {
	return c.guard.isClosed()
}

// isClosed reports whether Close has run.
func (c *TCPConnection) isClosed() bool {
	return c.IsClosed()
}

// Close shuts the connection and unblocks in-flight IO. It is idempotent
// and safe for concurrent use; later data path calls report ErrClosed.
func (c *TCPConnection) Close() error {
	if !c.guard.tryClose() {
		return nil
	}
	err := c.conn.Close()
	if err != nil {
		return fmt.Errorf("rtps: tcp close: %w", err)
	}
	return nil
}

// TCPRetryPolicy bounds reconnect dials.
type TCPRetryPolicy struct {
	// Attempts caps dial tries.
	Attempts int
	// Initial is the first reconnect wait.
	Initial time.Duration
	// Max caps the reconnect wait.
	Max time.Duration
}

// DefaultTCPRetryPolicy returns eight dials from 50ms capped at 2s.
func DefaultTCPRetryPolicy() TCPRetryPolicy {
	return TCPRetryPolicy{Attempts: tcpRetryAttempts, Initial: tcpRetryBase, Max: tcpRetryMax}
}

// withDefaults fills non-positive fields with defaults.
func (p TCPRetryPolicy) withDefaults() TCPRetryPolicy {
	out := p
	if out.Attempts <= 0 {
		out.Attempts = tcpRetryAttempts
	}
	if out.Initial <= 0 {
		out.Initial = tcpRetryBase
	}
	if out.Max <= 0 {
		out.Max = tcpRetryMax
	}
	return out
}

// tcpRetryWait returns Initial doubled per attempt, capped at Max.
func tcpRetryWait(attempt int, policy TCPRetryPolicy) time.Duration {
	wait := policy.Initial
	for range attempt {
		wait *= 2
		if wait >= policy.Max {
			return policy.Max
		}
	}
	if wait > policy.Max {
		return policy.Max
	}
	return wait
}

// DialTCPWithRetry dials with exponential backoff until a dial succeeds,
// attempts run out, or the context ends. Each failure logs loudly.
func DialTCPWithRetry(ctx context.Context, addr string, cfg TCPConnConfig, policy TCPRetryPolicy) (*TCPConnection, error) {
	policy = policy.withDefaults()
	var err error
	for attempt := range policy.Attempts {
		var conn *TCPConnection
		conn, err = DialTCP(ctx, addr, cfg)
		if err == nil {
			return conn, nil
		}
		log.Printf("rtps: tcp dial %s attempt %d: %v", addr, attempt+1, err)
		if attempt+1 >= policy.Attempts {
			break
		}
		wait := tcpRetryWait(attempt, policy)
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			cerr := ctx.Err()
			return nil, fmt.Errorf("rtps: tcp dial %s retry: %w", addr, cerr)
		case <-timer.C:
		}
	}
	return nil, fmt.Errorf("rtps: tcp dial %s after %d attempts: %w", addr, policy.Attempts, err)
}

// TCPListener binds one TCP socket for RTPS streams.
type TCPListener struct {
	// guard owns the closed flag for concurrent use.
	guard closeGuard
	// ln accepts RTPS streams.
	ln *net.TCPListener
	// cfg bounds accepted streams.
	cfg TCPConnConfig
	// addr is the bound address.
	addr net.Addr
}

// ListenTCP binds addr for RTPS streams. A zero port binds an ephemeral
// port for tests; pass 127.0.0.1 explicitly for loopback.
func ListenTCP(ctx context.Context, addr string, cfg TCPConnConfig) (*TCPListener, error) {
	err := checkContext(ctx, "tcp listen")
	if err != nil {
		return nil, err
	}
	resolved, err := net.ResolveTCPAddr("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("rtps: tcp listen %s: %w", addr, err)
	}
	ln, err := net.ListenTCP("tcp", resolved)
	if err != nil {
		return nil, fmt.Errorf("rtps: tcp listen %s: %w", addr, err)
	}
	return &TCPListener{ln: ln, cfg: cfg.withDefaults(), addr: ln.Addr()}, nil
}

// acceptOnce sets one short deadline and accepts one stream. It reports
// whether the caller should retry the accept.
func (l *TCPListener) acceptOnce(ctx context.Context) (*net.TCPConn, bool, error) {
	err := l.ln.SetDeadline(time.Now().Add(socketPollInterval))
	if err != nil {
		if l.isClosed() {
			return nil, false, fmt.Errorf("rtps: tcp accept: %w", ErrClosed)
		}
		return nil, false, fmt.Errorf("rtps: tcp accept deadline: %w", err)
	}
	raw, err := l.ln.AcceptTCP()
	if err != nil {
		if l.isClosed() {
			return nil, false, fmt.Errorf("rtps: tcp accept: %w", ErrClosed)
		}
		cerr := ctx.Err()
		if cerr != nil {
			return nil, false, fmt.Errorf("rtps: tcp accept: %w", cerr)
		}
		if IsTCPTimeout(err) {
			return nil, true, nil
		}
		return nil, false, fmt.Errorf("rtps: tcp accept: %w", err)
	}
	return raw, false, nil
}

// Accept waits for one RTPS stream. It polls with a short deadline so
// context cancel and Close unblock the wait promptly.
func (l *TCPListener) Accept(ctx context.Context) (*TCPConnection, error) {
	err := checkContext(ctx, "tcp accept")
	if err != nil {
		return nil, err
	}
	if l.isClosed() {
		return nil, fmt.Errorf("rtps: tcp accept: %w", ErrClosed)
	}
	for {
		raw, retry, err := l.acceptOnce(ctx)
		if err != nil {
			return nil, err
		}
		if retry {
			continue
		}
		return &TCPConnection{conn: raw, cfg: l.cfg}, nil
	}
}

// Addr returns the bound address.
func (l *TCPListener) Addr() net.Addr {
	return l.addr
}

// Close shuts the listener and unblocks Accept. It is idempotent and safe
// for concurrent use; later calls report ErrClosed.
func (l *TCPListener) Close() error {
	if !l.guard.tryClose() {
		return nil
	}
	err := l.ln.Close()
	if err != nil {
		return fmt.Errorf("rtps: tcp listener close: %w", err)
	}
	return nil
}

// isClosed reports whether Close has run.
func (l *TCPListener) isClosed() bool {
	return l.guard.isClosed()
}

// TCPMetatrafficUnicastPort maps a domain plus participant to the TCP
// discovery port. DDSI-RTPS 2.5 defines only the UDP/IP PSM, so TCP reuses
// the 9.6 port expressions on streams.
func TCPMetatrafficUnicastPort(domain, participant uint32) (int, error) {
	port, err := MetatrafficUnicastPort(domain, participant)
	if err != nil {
		return 0, fmt.Errorf("rtps: tcp metatraffic port: %w", err)
	}
	return port, nil
}

// TCPUserUnicastPort maps a domain plus participant to the TCP user port.
// DDSI-RTPS 2.5 defines only the UDP/IP PSM, so TCP reuses the 9.6 port
// expressions on streams.
func TCPUserUnicastPort(domain, participant uint32) (int, error) {
	port, err := UsertrafficUnicastPort(domain, participant)
	if err != nil {
		return 0, fmt.Errorf("rtps: tcp user port: %w", err)
	}
	return port, nil
}
