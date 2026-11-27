// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

// Package rtps implements RTPS 2.x message framing for first contact
// discovery and unreliable user data.
//
// Scope is deliberately narrow: localhost UDP, best effort delivery,
// generic byte payloads. There is no reliability engine, no discovery
// state machine, and no CDR parsing here; serialized payloads stay opaque
// so any XCDR1 type rides the same path. Wire layout matches CycloneDDS
// peers: RTPS magic, version 2.x, vendor id, 12 byte GUID prefix, and
// submessage headers with per submessage endianness.
//
// Every decode path validates length before slicing. Truncated input
// records one drop in the caller supplied Counter and returns an error.
// Decode never panics, including on random bytes.
//
// The package is pure Go with no cgo. Non test code imports only the
// standard library plus internal/transports for the shared domain limit.
// All network use stays in udp.go on the standard library net package.
package rtps

import (
	"encoding/binary"
	"errors"
	"fmt"
)

// Framing constants for the RTPS message header.
const (
	// Magic spells the RTPS protocol identifier in ASCII.
	Magic = "RTPS"
	// HeaderSize is the fixed 20 byte RTPS message header.
	HeaderSize = 20
	// SubmessageHeaderSize is the fixed 4 byte submessage header.
	SubmessageHeaderSize = 4
	// ProtocolMajor is the only accepted RTPS major version.
	ProtocolMajor = 2
	// ProtocolMinor is the sent RTPS minor version, 2.5 per DDSI-RTPS 2.5.
	// Peers may send any 2.x minor; decode accepts every minor under major 2.
	ProtocolMinor = 5
	// FlagLittleEndian is the submessage flags bit selecting little
	// endian for the length and content that follow it.
	FlagLittleEndian = 0x01
	// MaxDatagramSize is the largest UDP payload sent or received.
	MaxDatagramSize = 65507
)

// Framing offsets and limits for header and submessage bodies.
const (
	// maxSubmessageBody caps one submessage body to uint16 range.
	maxSubmessageBody = 0xFFFF
	// maxSubmessagesPerDatagram caps decoded submessages for bounded work.
	maxSubmessagesPerDatagram = 4096
	// vendorIDFirst is the first CycloneDDS vendor id byte.
	vendorIDFirst = 0x01
	// vendorIDSecond is the second CycloneDDS vendor id byte.
	vendorIDSecond = 0x10
	// headerMagicLen is the RTPS magic length.
	headerMagicLen = 4
	// headerMajorOff is the major version offset.
	headerMajorOff = 4
	// headerMinorOff is the minor version offset.
	headerMinorOff = 5
	// headerVendorOff is the vendor id offset.
	headerVendorOff = 6
	// headerVendorLen is the vendor id length.
	headerVendorLen = 2
	// headerPrefixOff is the GUID prefix offset.
	headerPrefixOff = 8
	// headerPrefixLen is the GUID prefix length.
	headerPrefixLen = 12
	// subIDOff is the submessage id offset.
	subIDOff = 0
	// subFlagsOff is the submessage flags offset.
	subFlagsOff = 1
	// subLenOff is the submessage length offset.
	subLenOff = 2
	// subLenSize is the submessage length size.
	subLenSize = 2
	// seqHighOff is the sequence high word offset.
	seqHighOff = 0
	// seqLowOff is the sequence low word offset.
	seqLowOff = 4
	// seqWordSize is one sequence word size.
	seqWordSize = 4
)

// DefaultVendorID returns the CycloneDDS vendor id.
//
// Vendor ids are opaque two byte tags copied verbatim, never byte swapped.
// Decode accepts any vendor; this value is only the default for sends.
func DefaultVendorID() [2]byte {
	return [2]byte{vendorIDFirst, vendorIDSecond}
}

// Header is the 20 byte RTPS message header: magic, version 2.x, vendor
// id, and 12 byte GUID prefix. The prefix is opaque bytes identifying the
// sending participant; combined with an entity id it names an endpoint.
type Header struct {
	// Major is the RTPS major version, always 2 on this wire.
	Major uint8
	// Minor is the RTPS minor version of the sender.
	Minor uint8
	// Vendor is the opaque sender vendor id, never byte swapped.
	Vendor [2]byte
	// Prefix is the opaque 12 byte sender GUID prefix.
	Prefix [12]byte
}

// DefaultHeader returns a Header for prefix with RTPS 2.5 and the
// CycloneDDS vendor id.
func DefaultHeader(prefix [12]byte) Header {
	return Header{
		Major:  ProtocolMajor,
		Minor:  ProtocolMinor,
		Vendor: DefaultVendorID(),
		Prefix: prefix,
	}
}

// Encode renders the 20 byte header on the wire.
func (h Header) Encode() [HeaderSize]byte {
	var out [HeaderSize]byte
	copy(out[0:headerMagicLen], Magic)
	out[headerMajorOff] = h.Major
	out[headerMinorOff] = h.Minor
	copy(out[headerVendorOff:headerVendorOff+headerVendorLen], h.Vendor[:])
	copy(out[headerPrefixOff:headerPrefixOff+headerPrefixLen], h.Prefix[:])
	return out
}

// DecodeHeader parses the first 20 bytes of buf.
//
// It rejects short buffers, wrong magic, and major versions other than 2.
// Any 2.x minor is accepted for forward compatibility with peers.
func DecodeHeader(buf []byte) (Header, error) {
	var out Header
	if len(buf) < HeaderSize {
		return out, fmt.Errorf("rtps: header need %d have %d (8.3.3)", HeaderSize, len(buf))
	}
	if string(buf[0:headerMagicLen]) != Magic {
		return out, fmt.Errorf("rtps: bad magic %#02x %#02x %#02x %#02x (8.3.3)", buf[0], buf[1], buf[2], buf[3])
	}
	if buf[headerMajorOff] != ProtocolMajor {
		return out, fmt.Errorf("rtps: major version %d, want %d (8.3.3)", buf[headerMajorOff], ProtocolMajor)
	}
	out.Major = buf[headerMajorOff]
	out.Minor = buf[headerMinorOff]
	copy(out.Vendor[:], buf[headerVendorOff:headerVendorOff+headerVendorLen])
	copy(out.Prefix[:], buf[headerPrefixOff:headerPrefixOff+headerPrefixLen])
	return out, nil
}

// SubmessageHeader is the 4 byte header before every submessage: id,
// flags, and length in octets to the next header. Length uses the
// endianness selected by the flags little endian bit, matching common
// vendor behavior on this wire.
type SubmessageHeader struct {
	// ID selects the submessage kind, for example DATA 0x15.
	ID uint8
	// Flags carries the endianness bit plus kind specific bits.
	Flags uint8
	// Length is the body size in octets, excluding this header.
	Length uint16
}

// LittleEndian reports whether flags selects little endian order.
func LittleEndian(flags uint8) bool {
	return flags&FlagLittleEndian != 0
}

// Encode renders the 4 byte header with length in flag order.
func (s SubmessageHeader) Encode() [SubmessageHeaderSize]byte {
	var out [SubmessageHeaderSize]byte
	out[subIDOff] = s.ID
	out[subFlagsOff] = s.Flags
	putUint16(out[subLenOff:subLenOff+subLenSize], s.Length, LittleEndian(s.Flags))
	return out
}

// DecodeSubmessageHeader parses 4 bytes, reading length in flag order.
func DecodeSubmessageHeader(buf []byte) (SubmessageHeader, error) {
	var out SubmessageHeader
	if len(buf) < SubmessageHeaderSize {
		return out, fmt.Errorf("rtps: submessage header need %d have %d (8.3.4)", SubmessageHeaderSize, len(buf))
	}
	out.ID = buf[subIDOff]
	out.Flags = buf[subFlagsOff]
	out.Length = uint16Value(buf[subLenOff:subLenOff+subLenSize], LittleEndian(buf[subFlagsOff]))
	return out, nil
}

// Counter counts dropped inputs. Drops increments once per truncated or
// malformed datagram. The struct is plain caller owned data with no global
// state. A nil Counter is valid and simply records nothing.
type Counter struct {
	// Drops counts inputs dropped instead of parsed.
	Drops uint64
}

// RecordDrop adds one drop, treating nil as a no-op.
func (c *Counter) RecordDrop() {
	if c == nil {
		return
	}
	c.Drops++
}

// Count returns the drop total, treating nil as zero.
func (c *Counter) Count() uint64 {
	if c == nil {
		return 0
	}
	return c.Drops
}

// RawSubmessage is one framed submessage with an opaque body. Bodies from
// DecodeMessage are copies owned by the struct; the input buffer may be
// reused after decode returns.
type RawSubmessage struct {
	// ID selects the submessage kind.
	ID uint8
	// Flags carries the endianness bit plus kind specific bits.
	Flags uint8
	// Body holds the submessage content without its header.
	Body []byte
}

// WrapSubmessage frames body with id and flags, encoding the length in
// flag order. Bodies above 64K are rejected loudly instead of truncated.
func WrapSubmessage(id, flags uint8, body []byte) ([]byte, error) {
	if len(body) > maxSubmessageBody {
		return nil, fmt.Errorf("rtps: submessage %#02x body %d above %d (8.3.4)", id, len(body), maxSubmessageBody)
	}
	hdr := SubmessageHeader{ID: id, Flags: flags, Length: uint16(len(body))} //nolint:gosec // G115: length guarded by the maxSubmessageBody check above
	raw := hdr.Encode()
	out := make([]byte, 0, SubmessageHeaderSize+len(body))
	out = append(out, raw[:]...)
	out = append(out, body...)
	return out, nil
}

// EncodeMessage frames header plus submessages into one datagram.
func EncodeMessage(h Header, subs []RawSubmessage) ([]byte, error) {
	raw := h.Encode()
	out := make([]byte, 0, HeaderSize+len(subs)*(SubmessageHeaderSize+8))
	out = append(out, raw[:]...)
	for i := range subs {
		framed, err := WrapSubmessage(subs[i].ID, subs[i].Flags, subs[i].Body)
		if err != nil {
			return nil, fmt.Errorf("rtps: encode submessage %d: %w", i, err)
		}
		out = append(out, framed...)
	}
	return out, nil
}

// verifyUDPChecksum checks the HeaderExtension checksum on the UDP path
// per DDSI-RTPS 2.5 8.3.7.3. The checksum covers the full datagram with
// its own field zeroed; a mismatch drops fail-closed.
func verifyUDPChecksum(buf []byte, ext HeaderExtension, extOff int, drops *Counter) error {
	cksLen := checksumKindLen(ext.Checksum)
	if cksLen <= 0 {
		drops.RecordDrop()
		return fmt.Errorf("rtps: header extension checksum kind %d unknown (8.3.7.3)", int(ext.Checksum))
	}
	cksOff := extOff + SubmessageHeaderSize + extensionChecksumBodyOffset(ext)
	if cksOff < 0 || cksOff+cksLen > len(buf) {
		drops.RecordDrop()
		return fmt.Errorf("rtps: header extension checksum overruns %d (8.3.7.3)", len(buf))
	}
	probe := make([]byte, len(buf))
	copy(probe, buf)
	for i := range cksLen {
		probe[cksOff+i] = 0
	}
	sum, err := TCPComputeChecksum(ext.Checksum, probe)
	if err != nil {
		drops.RecordDrop()
		return err
	}
	if !equalTCPChecksum(sum, buf[cksOff:cksOff+cksLen]) {
		drops.RecordDrop()
		return errors.New("rtps: header extension checksum mismatch (8.3.7.3)") //nolint:wrapcheck // new error, nothing to wrap
	}
	return nil
}

// ValidateHeaderExtensionPlacement validates every HeaderExtension in the
// datagram per DDSI-RTPS 2.5 8.3.7.3 on the UDP path. Each present
// extension must be well-formed; the first must be at index 0 and any
// later HeaderExtension drops loudly as mispositioned, even when
// well-formed. Short bodies, malformed parameters, length mismatches,
// and checksum mismatches drop loudly. A valid first extension is
// accepted per 2.5 and stays as raw for the dispatcher.
//
// Standalone datagram walks that do not go through DecodeMessage (such as
// the SEDP submessage split) must call this with the full datagram plus
// the walked submessages so mispositioned or checksum-invalid extensions
// drop fail-closed instead of skipping as opaque bytes.
func ValidateHeaderExtensionPlacement(buf []byte, subs []RawSubmessage, drops *Counter) error {
	off := HeaderSize
	for i := range subs {
		if subs[i].ID != SubmessageHeaderExtension {
			off += SubmessageHeaderSize + len(subs[i].Body)
			continue
		}
		if err := validateHeaderExtensionAt(buf, subs[i], i, off, drops); err != nil {
			return err
		}
		if i != 0 {
			drops.RecordDrop()
			return fmt.Errorf("rtps: header extension not first at %d (8.3.7.3)", i)
		}
		off += SubmessageHeaderSize + len(subs[i].Body)
	}
	return nil
}

// validateHeaderExtensionAt validates one header extension body.
func validateHeaderExtensionAt(buf []byte, sub RawSubmessage, index, off int, drops *Counter) error {
	var tmp TCPDropCounter
	ext, err := DecodeHeaderExtension(sub.Body, sub.Flags, &tmp)
	if err != nil {
		drops.RecordDrop()
		return fmt.Errorf("rtps: header extension invalid at %d (8.3.7.3): %w", index, err)
	}
	if ext.HasLength && uint64(ext.MessageLength) < uint64(HeaderSize+SubmessageHeaderSize+len(sub.Body)) {
		drops.RecordDrop()
		return fmt.Errorf("rtps: header extension length %d too small (8.3.7.3)", ext.MessageLength)
	}
	if ext.HasLength {
		err = checkUDPMessageLength(ext.MessageLength, len(buf), drops)
		if err != nil {
			return err
		}
	}
	if ext.Checksum != TCPChecksumNone {
		err = verifyUDPChecksum(buf, ext, off, drops)
		if err != nil {
			return err
		}
	}
	return nil
}

// checkUDPMessageLength validates MessageLength against the UDP datagram
// length per DDSI-RTPS 2.5 8.3.7.3. On UDP the datagram length is
// determinable as len(buf), so an exact match is required: the
// MessageLength field names the full RTPS message length from the RTPS
// header, which on UDP is exactly the datagram length. A short length
// drops as too small; any mismatch drops, which also rejects a wildly
// excessive length (for example above datagram plus one max submessage)
// that would otherwise claim bytes past the datagram.
func checkUDPMessageLength(messageLength uint32, datagramLen int, drops *Counter) error {
	if datagramLen < 0 {
		drops.RecordDrop()
		return fmt.Errorf("rtps: header extension datagram length %d invalid (8.3.7.3)", datagramLen)
	}
	msgLen64 := uint64(messageLength)
	dgramLen64 := uint64(datagramLen)
	if msgLen64 != dgramLen64 {
		drops.RecordDrop()
		if msgLen64 > dgramLen64+uint64(maxSubmessageBody) {
			return fmt.Errorf("rtps: header extension length %d wildly exceeds datagram %d (8.3.7.3)", messageLength, datagramLen)
		}
		return fmt.Errorf("rtps: header extension length %d differs from datagram %d (8.3.7.3)", messageLength, datagramLen)
	}
	return nil
}

// DecodeMessage parses one datagram into its header and submessages.
//
// A datagram is atomic: any truncation drops the whole message with one
// counter increment and an error. Unknown submessage ids are preserved as
// raw bodies for the dispatcher; see DecodeSubmessage. A HeaderExtension
// must be first per 8.3.7.3 and is validated loudly; mispositioned, short,
// or checksum mismatched extensions drop instead of skipping, while a
// valid first extension is accepted per 2.5. Bodies are copied,
// so buf may be reused after return. Submessage count is capped at
// maxSubmessagesPerDatagram so a crafted header chain cannot force
// unbounded allocation; over-cap datagrams drop loudly.
func DecodeMessage(buf []byte, drops *Counter) (Header, []RawSubmessage, error) {
	var zero Header
	hdr, err := DecodeHeader(buf)
	if err != nil {
		drops.RecordDrop()
		return zero, nil, fmt.Errorf("rtps: decode message: %w (8.3.4.1)", err)
	}
	var subs []RawSubmessage
	off := HeaderSize
	for off < len(buf) {
		if len(subs) >= maxSubmessagesPerDatagram {
			drops.RecordDrop()
			return zero, nil, fmt.Errorf("rtps: decode message submessages above %d (8.3.4.1)", maxSubmessagesPerDatagram)
		}
		sub, next, err := decodeOne(buf, off)
		if err != nil {
			drops.RecordDrop()
			return zero, nil, fmt.Errorf("rtps: decode message: %w (8.3.4.1)", err)
		}
		subs = append(subs, sub)
		off = next
	}
	err = ValidateHeaderExtensionPlacement(buf, subs, drops)
	if err != nil {
		return zero, nil, err
	}
	return hdr, subs, nil
}

// decodeOne parses the submessage at off and returns it with the offset of
// the next header. The body is copied following the copy once at ingress
// rule, so later buffer reuse cannot corrupt parsed data. A zero
// octetsToNextHeader extends to the end of the datagram per 9.4.5.1.3;
// on UDP the datagram length is the message length, so a zero length
// consumes the datagram tail and must be the last submessage.
func decodeOne(buf []byte, off int) (RawSubmessage, int, error) {
	var sub RawSubmessage
	if len(buf)-off < SubmessageHeaderSize {
		return sub, 0, fmt.Errorf("rtps: submessage header need %d have %d (8.3.4)", SubmessageHeaderSize, len(buf)-off)
	}
	id := buf[off+subIDOff]
	flags := buf[off+subFlagsOff]
	length := uint16Value(buf[off+subLenOff:off+subLenOff+subLenSize], LittleEndian(flags))
	end := off + SubmessageHeaderSize + int(length)
	bodyLen := int(length)
	if length == 0 {
		end = len(buf)
		bodyLen = end - off - SubmessageHeaderSize
	}
	if end > len(buf) {
		return sub, 0, fmt.Errorf("rtps: submessage %#02x length %d overruns %d left (8.3.4)", id, length, len(buf)-off)
	}
	body := make([]byte, bodyLen)
	copy(body, buf[off+SubmessageHeaderSize:end])
	sub = RawSubmessage{ID: id, Flags: flags, Body: body}
	return sub, end, nil
}

// byteOrder selects the wire order for a submessage or parameter.
func byteOrder(littleEndian bool) binary.ByteOrder {
	if littleEndian {
		return binary.LittleEndian
	}
	return binary.BigEndian
}

// putUint16 stores v in buf in the given order. Callers guarantee 2 bytes.
func putUint16(buf []byte, v uint16, littleEndian bool) {
	byteOrder(littleEndian).PutUint16(buf, v)
}

// uint16Value loads 2 bytes in the given order. Callers guarantee length.
func uint16Value(buf []byte, littleEndian bool) uint16 {
	return byteOrder(littleEndian).Uint16(buf)
}

// putUint32 stores v in buf in the given order. Callers guarantee 4 bytes.
func putUint32(buf []byte, v uint32, littleEndian bool) {
	byteOrder(littleEndian).PutUint32(buf, v)
}

// uint32Value loads 4 bytes in the given order. Callers guarantee length.
func uint32Value(buf []byte, littleEndian bool) uint32 {
	return byteOrder(littleEndian).Uint32(buf)
}

// putSeqNumber stores a sequence number as high word then low word, each
// in the given order. RTPS SequenceNumber is a high long followed by a
// low unsigned long; writing it as one 64 bit integer swaps the words.
// Callers guarantee 8 bytes.
func putSeqNumber(buf []byte, v uint64, littleEndian bool) {
	putUint32(buf[seqHighOff:seqHighOff+seqWordSize], uint32(v>>32), littleEndian)
	putUint32(buf[seqLowOff:seqLowOff+seqWordSize], uint32(v), littleEndian) //nolint:gosec // G115: low word truncation is the layout
}

// seqNumberValue loads a high word then low word sequence number in the
// given order. Callers guarantee length.
func seqNumberValue(buf []byte, littleEndian bool) uint64 {
	return uint64(uint32Value(buf[seqHighOff:seqHighOff+seqWordSize], littleEndian))<<32 | uint64(uint32Value(buf[seqLowOff:seqLowOff+seqWordSize], littleEndian))
}
