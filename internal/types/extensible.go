// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1
package types

import (
	"encoding/binary"
	"fmt"
	"reflect"
)

// Extensibility names the XTypes extensibility kind of an aggregated type.
//
// The kind controls the wire framing: final types carry no delimiter,
// appendable types carry a DHEADER delimiter under XCDR2, and mutable
// types carry a DHEADER plus per member headers under both versions.
// See DDS-XTypes 1.4 section 7.2.4 and Table 11.
type Extensibility int

// Extensibility kinds.
const (
	// ExtFinal marks a type that cannot gain or lose members.
	ExtFinal Extensibility = iota
	// ExtAppendable marks a type that can gain members at the end.
	ExtAppendable
	// ExtMutable marks a type that can gain, lose, or reorder members.
	ExtMutable
)

// String returns the symbolic name of the extensibility kind.
func (e Extensibility) String() string {
	switch e {
	case ExtFinal:
		return "final"
	case ExtAppendable:
		return "appendable"
	case ExtMutable:
		return "mutable"
	default:
		return "unknown"
	}
}

// EncodingVersion names the XCDR wire version in use on a stream.
type EncodingVersion int

// Encoding versions.
const (
	// XCDRVersion1 selects the version 1 rules (PLAIN_CDR and PL_CDR).
	XCDRVersion1 EncodingVersion = 1
	// XCDRVersion2 selects the version 2 rules (PLAIN_CDR2 and PL_CDR2).
	XCDRVersion2 EncodingVersion = 2
)

// Validate checks that the version is 1 or 2.
func (v EncodingVersion) Validate() error {
	switch v {
	case XCDRVersion1, XCDRVersion2:
		return nil
	default:
		return fmt.Errorf("types: unknown encoding version %d", int(v))
	}
}

// NeedsDHeader reports whether the kind carries a DHEADER delimiter.
//
// Final types never carry one. Appendable types carry one only under
// XCDR2. Mutable types carry one under both versions. See Table 11.
func (e Extensibility) NeedsDHeader(v EncodingVersion) bool {
	switch e {
	case ExtMutable:
		return true
	case ExtAppendable:
		return v == XCDRVersion2
	case ExtFinal:
		return false
	default:
		return false
	}
}

// XCDR1 parameter list framing constants per 7.4.1.2.
const (
	// PIDListEnd terminates a version 1 mutable parameter list.
	PIDListEnd = 0x3F02
	// PIDExtended introduces the 32 bit member id and length form.
	PIDExtended = 0x3F01
	// pidExtendedLength is the fixed 16 bit length after PID_EXTENDED.
	pidExtendedLength = 8
	// flagMustUnderstandV1 is the second bit of the 16 bit parameter id.
	flagMustUnderstandV1 = 0x4000
	// flagImplExtensionV1 is the top bit of the 16 bit parameter id.
	flagImplExtensionV1 = 0x8000
	// pidMemberMask keeps the 14 bit member id of a short parameter id.
	pidMemberMask = 0x3FFF
	// ememberMustUnderstand is bit 31 of the extended member header.
	ememberMustUnderstand = 0x80000000
	// ememberIDMask keeps the 28 bit member id of the extended header.
	ememberIDMask = 0x0FFFFFFF
)

// EncodePIDShort builds the 16 bit parameter id for the short PL form.
//
// The must understand bit is set exactly when the member requires it,
// per 7.4.1.2.1. Member ids above the 14 bit range need EncodePIDExtended.
func EncodePIDShort(memberID uint32, mustUnderstand bool) (uint16, error) {
	if memberID > pidMemberMask {
		return 0, fmt.Errorf("types: pid member id %d exceeds 14 bits", memberID)
	}
	v := uint16(memberID)
	if mustUnderstand {
		v |= flagMustUnderstandV1
	}
	return v, nil
}

// DecodePIDShort splits a 16 bit parameter id into member id and flags.
func DecodePIDShort(pid uint16) (memberID uint32, mustUnderstand bool, implExtension bool) {
	mustUnderstand = pid&flagMustUnderstandV1 != 0
	implExtension = pid&flagImplExtensionV1 != 0
	memberID = uint32(pid & pidMemberMask)
	return memberID, mustUnderstand, implExtension
}

// EncodePIDExtended builds the 32 bit extended member header word.
//
// The top bit carries must understand and the low 28 bits carry the
// member id, per 7.4.1.2. Only FLAG_1 and FLAG_2 are used; the two
// reserved flag bits stay zero.
func EncodePIDExtended(memberID uint32, mustUnderstand bool) (uint32, error) {
	if memberID > ememberIDMask {
		return 0, fmt.Errorf("types: extended member id %d exceeds 28 bits", memberID)
	}
	v := memberID
	if mustUnderstand {
		v |= ememberMustUnderstand
	}
	return v, nil
}

// DecodePIDExtended splits the 32 bit extended member header word.
func DecodePIDExtended(v uint32) (memberID uint32, mustUnderstand bool) {
	mustUnderstand = v&ememberMustUnderstand != 0
	memberID = v & ememberIDMask
	return memberID, mustUnderstand
}

// WriteDHeaderPlaceholder appends a zero uint32 and returns its offset.
//
// The caller serializes the delimited content next, then calls
// FillDHeader to patch the size. Alignment matches a plain uint32
// write so both XCDR versions share the helper.
func (e *Encoder) WriteDHeaderPlaceholder() (int, error) {
	err := e.Align(4)
	if err != nil {
		return 0, err
	}
	at := len(e.buf)
	var zero [4]byte
	e.buf = append(e.buf, zero[:]...)
	return at, nil
}

// FillDHeader patches the placeholder at offset with the content size.
//
// The size counts bytes after the header, per DHEADER(O) = O.ssize.
// It returns an error without mutating when the offset is invalid or
// the content overflows uint32.
func (e *Encoder) FillDHeader(at int) error {
	if at < 0 || at+4 > len(e.buf) {
		return fmt.Errorf("types: dheader offset %d out of range %d", at, len(e.buf))
	}
	size := len(e.buf) - (at + 4)
	if size < 0 || uint64(size) > 0xFFFFFFFF {
		return fmt.Errorf("types: dheader content %d overflows uint32", size)
	}
	if e.le {
		binary.LittleEndian.PutUint32(e.buf[at:at+4], uint32(size))
	} else {
		binary.BigEndian.PutUint32(e.buf[at:at+4], uint32(size))
	}
	return nil
}

// ReadDHeader reads a DHEADER delimiter and returns the content size.
//
// The caller reads exactly size bytes of content next, which lets it
// skip unknown trailing members of appendable types. Overlong sizes
// that run past the buffer count a drop and return an error.
func (d *Decoder) ReadDHeader() (uint32, error) {
	size, err := d.readUint32()
	if err != nil {
		return 0, err
	}
	if uint64(d.off)+uint64(size) > uint64(len(d.data)) { //nolint:gosec // G115: offsets are non-negative positions
		d.drops++
		return 0, fmt.Errorf("types: dheader size %d exceeds remaining %d: %w", size, len(d.data)-d.off, ErrTruncated)
	}
	return size, nil
}

// EMHeader length code values per 7.4.3.4.2.
const (
	// lcLen1 marks an 8 byte header member of 1 byte.
	lcLen1 = 0
	// lcLen2 marks an 8 byte header member of 2 bytes.
	lcLen2 = 1
	// lcLen4 marks an 8 byte header member of 4 bytes.
	lcLen4 = 2
	// lcLen8 marks an 8 byte header member of 8 bytes.
	lcLen8 = 3
	// lcNextInt marks a 8 byte header with NEXTINT holding the size.
	lcNextInt = 4
	// lcReuseNextInt reuses NEXTINT as the leading member length.
	lcReuseNextInt = 5
	// lcReuse4x reuses NEXTINT as a uint32 count scaled by 4.
	lcReuse4x = 6
	// lcReuse8x reuses NEXTINT as a uint32 count scaled by 8.
	lcReuse8x = 7
)

// LengthCodeForSize maps a member size to its EMHEADER length code.
//
// Sizes 1, 2, 4, and 8 use codes 0 to 3 with a 4 byte header. Any other
// size uses code 4 with an 8 byte header where NEXTINT holds the size.
func LengthCodeForSize(size uint32) uint8 {
	switch size {
	case 1:
		return lcLen1
	case 2:
		return lcLen2
	case 4:
		return lcLen4
	case 8:
		return lcLen8
	default:
		return lcNextInt
	}
}

// EncodeEMHeader1 builds the first EMHEADER word from flag, code, and id.
//
// EMHEADER1 = (M_FLAG<<31) + (LC<<28) + member id, per 7.4.3.4.2.
// Member ids above 28 bits are rejected.
func EncodeEMHeader1(mustUnderstand bool, lc uint8, memberID uint32) (uint32, error) {
	if lc > 7 {
		return 0, fmt.Errorf("types: length code %d exceeds 7", lc)
	}
	if memberID > ememberIDMask {
		return 0, fmt.Errorf("types: emheader member id %d exceeds 28 bits", memberID)
	}
	v := (uint32(lc) << 28) | memberID
	if mustUnderstand {
		v |= ememberMustUnderstand
	}
	return v, nil
}

// DecodeEMHeader1 splits an EMHEADER1 word into flag, code, and id.
func DecodeEMHeader1(v uint32) (mustUnderstand bool, lc uint8, memberID uint32) {
	mustUnderstand = v&ememberMustUnderstand != 0
	lc = uint8((v >> 28) & 0x07)
	memberID = v & ememberIDMask
	return mustUnderstand, lc, memberID
}

// MemberSizeFromLC resolves the member size from a length code.
//
// Codes 0 to 3 map to 1, 2, 4, and 8. Code 4 takes NEXTINT as the size.
// Codes 5 to 7 reuse NEXTINT as the leading member bytes per rule 22:
// 5 repeats NEXTINT as the size, 6 scales a uint32 count by 4, and 7
// scales it by 8. The caller rewinds 4 bytes so NEXTINT is also the
// first member value; see ReadEMHeader.
func MemberSizeFromLC(lc uint8, nextInt uint32) (uint64, error) {
	switch lc {
	case lcLen1:
		return 1, nil
	case lcLen2:
		return 2, nil
	case lcLen4:
		return 4, nil
	case lcLen8:
		return 8, nil
	case lcNextInt:
		return uint64(nextInt), nil
	case lcReuseNextInt:
		return uint64(nextInt), nil
	case lcReuse4x:
		return uint64(nextInt) * 4, nil
	case lcReuse8x:
		return uint64(nextInt) * 8, nil
	default:
		return 0, fmt.Errorf("types: length code %d out of range", lc)
	}
}

// WriteEMHeader writes a mutable member header for the content size.
//
// Short codes 0 to 3 emit 4 bytes. Larger sizes emit 8 bytes with
// NEXTINT holding the exact size. The content follows immediately.
func (e *Encoder) WriteEMHeader(memberID uint32, mustUnderstand bool, size uint32) error {
	lc := LengthCodeForSize(size)
	first, err := EncodeEMHeader1(mustUnderstand, lc, memberID)
	if err != nil {
		return err
	}
	putErr := e.putUint32(first)
	if putErr != nil {
		return fmt.Errorf("types: encode emheader: %w", putErr)
	}
	return e.writeEMHeaderNext(lc, size)
}

// writeEMHeaderNext appends NEXTINT for long mutable members.
//
// Short codes need no second word and return nil immediately.
func (e *Encoder) writeEMHeaderNext(lc uint8, size uint32) error {
	if lc < lcNextInt {
		return nil
	}
	err := e.putUint32(size)
	if err != nil {
		return fmt.Errorf("types: encode emheader nextint: %w", err)
	}
	return nil
}

// WriteEMHeaderWithLC writes a mutable member header with an explicit LC.
//
// Codes 0 to 4 behave like WriteEMHeader. Codes 5 to 7 write the given
// NEXTINT word per rule 22; the caller must ensure the member value
// starts with the same leading length so the 4 reuse bytes line up.
// Introspection tables do not carry the reuse type info, so this stays
// a codec primitive: full wiring must pick LC 5 to 7 by member type.
func (e *Encoder) WriteEMHeaderWithLC(memberID uint32, mustUnderstand bool, lc uint8, nextInt uint32) error {
	first, err := EncodeEMHeader1(mustUnderstand, lc, memberID)
	if err != nil {
		return err
	}
	putErr := e.putUint32(first)
	if putErr != nil {
		return fmt.Errorf("types: encode emheader: %w", putErr)
	}
	if lc < lcNextInt {
		return nil
	}
	writeErr := e.putUint32(nextInt)
	if writeErr != nil {
		return fmt.Errorf("types: encode emheader nextint: %w", writeErr)
	}
	return nil
}

// EMHeader is the decoded mutable member header.
type EMHeader struct {
	// MemberID is the 28 bit member identifier.
	MemberID uint32
	// MustUnderstand flags members the reader must recognize.
	MustUnderstand bool
	// Size is the content byte count that follows the header.
	Size uint64
	// HeaderLen is the header width, 4 or 8 bytes.
	HeaderLen int
}

// ReadEMHeader reads one mutable member header from the decoder.
//
// It returns the member id, the must understand flag, and the content
// size. Codes 5 to 7 reuse NEXTINT as the leading member bytes per
// rule 22: the offset rewinds 4 bytes so NEXTINT is also the first
// content value, and the size is NEXTINT, 4*NEXTINT, or 8*NEXTINT.
func (d *Decoder) ReadEMHeader() (EMHeader, error) {
	var out EMHeader
	first, err := d.readUint32()
	if err != nil {
		return out, err
	}
	must, lc, id := DecodeEMHeader1(first)
	out.MustUnderstand = must
	out.MemberID = id
	size, hlen, err := d.emHeaderSize(lc)
	if err != nil {
		return out, err
	}
	out.Size = size
	out.HeaderLen = hlen
	if uint64(d.off)+out.Size > uint64(len(d.data)) { //nolint:gosec // G115: offsets are non-negative positions
		d.drops++
		return out, fmt.Errorf("types: emheader size %d exceeds remaining %d: %w", out.Size, len(d.data)-d.off, ErrTruncated)
	}
	return out, nil
}

// emHeaderSize resolves the content size for a length code.
//
// Codes 0 to 3 map inline with a 4 byte header. Code 4 reads NEXTINT
// for an 8 byte header. Codes 5 to 7 read NEXTINT then rewind 4 bytes
// per rule 22 so NEXTINT doubles as the leading member value.
func (d *Decoder) emHeaderSize(lc uint8) (uint64, int, error) {
	if lc < lcNextInt {
		size, err := MemberSizeFromLC(lc, 0)
		if err != nil {
			d.drops++
			return 0, 0, fmt.Errorf("types: read emheader: %w", err)
		}
		return size, 4, nil
	}
	next, err := d.readUint32()
	if err != nil {
		return 0, 0, err
	}
	if lc >= lcReuseNextInt {
		d.off -= 4
	}
	size, err := MemberSizeFromLC(lc, next)
	if err != nil {
		d.drops++
		return 0, 0, fmt.Errorf("types: read emheader: %w", err)
	}
	return size, 8, nil
}

// pidHeader is the decoded short or extended parameter header.
type pidHeader struct {
	memberID       uint32
	mustUnderstand bool
	size           uint32
	isSentinel     bool
}

// WritePIDMember writes one XCDR1 mutable member with its PID header.
//
// Small ids and sizes use the short 4 byte header. Larger values use
// the PID_EXTENDED 12 byte form. The content bytes follow the header
// with no extra pad; the next header aligns to 4 on write. The outer
// header flag bits are exactly the encoded must-understand bit: short
// headers carry FLAG_M (0x4000) when set, extended headers carry
// 0x7F01 (FLAG_M plus PID_EXTENDED with its own MUST_UNDERSTAND per
// Table 31), and no other outer flag bits are used.
func (e *Encoder) WritePIDMember(memberID uint32, mustUnderstand bool, content []byte) error {
	if len(content) > 0xFFFFFFFF {
		return fmt.Errorf("types: pid content %d overflows uint32", len(content))
	}
	size := uint32(len(content)) //nolint:gosec // G115: length guarded above
	err := e.Align(4)
	if err != nil {
		return err
	}
	if memberID <= pidMemberMask && size <= 0xFFFF {
		pid, pidErr := EncodePIDShort(memberID, mustUnderstand)
		if pidErr != nil {
			return pidErr
		}
		var hdr [4]byte
		e.order.PutUint16(hdr[0:2], pid)
		e.order.PutUint16(hdr[2:4], uint16(size))
		e.buf = append(e.buf, hdr[:]...)
	} else {
		ext, extErr := EncodePIDExtended(memberID, mustUnderstand)
		if extErr != nil {
			return extErr
		}
		var hdr [12]byte
		e.order.PutUint16(hdr[0:2], flagMustUnderstandV1|PIDExtended)
		e.order.PutUint16(hdr[2:4], pidExtendedLength)
		e.order.PutUint32(hdr[4:8], ext)
		e.order.PutUint32(hdr[8:12], size)
		e.buf = append(e.buf, hdr[:]...)
	}
	e.buf = append(e.buf, content...)
	return nil
}

// ReadPIDMember reads one XCDR1 mutable member header plus its size.
//
// It returns the header with the sentinel flag set for PID_LIST_END.
// PID_EXTENDED switches to the 12 byte long form. Unknown member ids
// are returned normally so the caller can skip size bytes.
func (d *Decoder) ReadPIDMember() (pidHeader, error) {
	var out pidHeader
	err := d.Align(4)
	if err != nil {
		return out, err
	}
	var pid uint16
	var length uint16
	rawPID, err := d.readRawUint16()
	if err != nil {
		return out, err
	}
	pid = rawPID
	length, err = d.readRawUint16()
	if err != nil {
		return out, err
	}
	id, must, _ := DecodePIDShort(pid)
	if id == PIDListEnd && length == 0 {
		out.isSentinel = true
		return out, nil
	}
	if id == PIDExtended {
		return d.readPIDExtended(length)
	}
	out.memberID = id
	out.mustUnderstand = must
	out.size = uint32(length)
	err = d.checkPIDSize(out.size)
	if err != nil {
		return out, err
	}
	return out, nil
}

// readPIDExtended reads the 12 byte long parameter header form.
func (d *Decoder) readPIDExtended(slength uint16) (pidHeader, error) {
	var out pidHeader
	if slength != pidExtendedLength {
		d.drops++
		return out, fmt.Errorf("types: extended pid length %d, want %d: %w", slength, pidExtendedLength, ErrInvalid)
	}
	ext, err := d.readRawUint32()
	if err != nil {
		return out, err
	}
	llength, err := d.readRawUint32()
	if err != nil {
		return out, err
	}
	id, must := DecodePIDExtended(ext)
	out.memberID = id
	out.mustUnderstand = must
	out.size = llength
	err = d.checkPIDSize(out.size)
	if err != nil {
		return out, err
	}
	return out, nil
}

// checkPIDSize verifies that size content bytes are still available.
func (d *Decoder) checkPIDSize(size uint32) error {
	if uint64(d.off)+uint64(size) > uint64(len(d.data)) { //nolint:gosec // G115: offsets are non-negative positions
		d.drops++
		return fmt.Errorf("types: pid size %d exceeds remaining %d: %w", size, len(d.data)-d.off, ErrTruncated)
	}
	return nil
}

// readRawUint16 reads 2 bytes in stream order without alignment.
func (d *Decoder) readRawUint16() (uint16, error) {
	err := d.need(2)
	if err != nil {
		return 0, err
	}
	v := d.order.Uint16(d.data[d.off : d.off+2])
	d.off += 2
	return v, nil
}

// readRawUint32 reads 4 bytes in stream order without alignment.
func (d *Decoder) readRawUint32() (uint32, error) {
	err := d.need(4)
	if err != nil {
		return 0, err
	}
	v := d.order.Uint32(d.data[d.off : d.off+4])
	d.off += 4
	return v, nil
}

// WritePIDListEnd writes the terminating sentinel of a mutable list.
//
// The sentinel is PID_LIST_END with zero length at 4 byte alignment,
// per rules 23 and 28 of 7.4.3.5.3. Table 31 marks PID_LIST_END with
// FLAG_MUST_UNDERSTAND set, so the emitted wire id is 0x7F02. Readers
// mask the flag bits and accept both 0x3F02 and 0x7F02.
func (e *Encoder) WritePIDListEnd() error {
	err := e.Align(4)
	if err != nil {
		return err
	}
	var hdr [4]byte
	e.order.PutUint16(hdr[0:2], PIDListEnd|flagMustUnderstandV1)
	e.order.PutUint16(hdr[2:4], 0)
	e.buf = append(e.buf, hdr[:]...)
	return nil
}

// SkipPIDMemberContent reads one V1 mutable member and skips unknown content.
//
// This is the V1 counterpart of SkipUnknownxt6bMember for XCDR2 EMHEADER:
// the PID header is read via ReadPIDMember, the sentinel reports done
// true with no skip, known member ids report done false with the header
// so the caller reads size bytes, and unknown ids are skipped or fail per
// the must-understand flag via CheckMustUnderstand per 7.4.1.2. It gives
// the V1 PID path a production must-understand caller in decode.
func (d *Decoder) SkipPIDMemberContent(known map[uint32]bool) (hdr pidHeader, done bool, err error) {
	hdr, err = d.ReadPIDMember()
	if err != nil {
		return hdr, false, err
	}
	if hdr.isSentinel {
		return hdr, true, nil
	}
	if known != nil && known[hdr.memberID] {
		return hdr, false, nil
	}
	err = CheckMustUnderstand(hdr.mustUnderstand, hdr.memberID)
	if err != nil {
		return hdr, false, err
	}
	err = d.SkipBytes(uint64(hdr.size))
	if err != nil {
		return hdr, false, err
	}
	return hdr, false, nil
}

// SkipBytes advances the decoder past size content bytes.
func (d *Decoder) SkipBytes(size uint64) error {
	if size > uint64(len(d.data)-d.off) { //nolint:gosec // G115: remaining length is non-negative
		d.drops++
		return fmt.Errorf("types: skip %d exceeds remaining %d: %w", size, len(d.data)-d.off, ErrTruncated)
	}
	d.off += int(size) //nolint:gosec // G115: size fits the remaining buffer
	return nil
}

// CheckMustUnderstand enforces the must understand flag on skip.
//
// Unknown members flagged must understand abort the sample with an
// invalid error. Unknown members without the flag may be skipped by
// the caller. Known members never reach this helper.
func CheckMustUnderstand(mustUnderstand bool, memberID uint32) error {
	if mustUnderstand {
		return fmt.Errorf("types: unknown must-understand member %d: %w", memberID, ErrInvalid)
	}
	return nil
}

// TryConstructKind names the 7.2.2.8 try-construct policy for one member.
//
// DISCARD drops the whole sample when construction fails. USE_DEFAULT
// substitutes the type default and keeps the sample. TRIM shortens a
// bounded string, sequence, or map to the reader bound and keeps the
// sample; it applies only to bound shrinking and fails otherwise.
type TryConstructKind int

// Try-construct policies per 7.2.2.8.
const (
	// TryConstructDiscard drops the sample on construction failure.
	TryConstructDiscard TryConstructKind = iota
	// TryConstructUseDefault substitutes the default on failure.
	TryConstructUseDefault
	// TryConstructTrim shortens bounded values to the reader bound.
	TryConstructTrim
)

// String returns the symbolic name of the try-construct kind.
func (k TryConstructKind) String() string {
	switch k {
	case TryConstructDiscard:
		return "DISCARD"
	case TryConstructUseDefault:
		return "USE_DEFAULT"
	case TryConstructTrim:
		return "TRIM"
	default:
		return "UNKNOWN"
	}
}

// ApplyTryConstructString enforces try-construct for a bounded string.
//
// bound counts octets excluding NUL with 0 meaning unbounded. In-bounds
// input returns itself with discard false. Over-bound input follows the
// policy: DISCARD returns discard true, USE_DEFAULT returns empty with
// discard false, and TRIM returns the rune safe truncation. Unknown
// kinds fail.
func ApplyTryConstructString(kind TryConstructKind, s string, bound int) (string, bool, error) {
	if bound < 0 {
		return "", false, fmt.Errorf("types: negative string bound %d: %w", bound, ErrInvalid)
	}
	if bound == 0 || len(s) <= bound {
		return s, false, nil
	}
	switch kind {
	case TryConstructDiscard:
		return "", true, nil
	case TryConstructUseDefault:
		return "", false, nil
	case TryConstructTrim:
		return truncatext6bStringToOctets(s, bound), false, nil
	default:
		return "", false, fmt.Errorf("types: unknown try-construct %d: %w", int(kind), ErrInvalid)
	}
}

// ApplyTryConstructSequence enforces try-construct for a bounded sequence length.
//
// bound 0 means unbounded. In-bounds counts return n with discard
// false. Over-bound counts follow the policy: DISCARD returns discard
// true, USE_DEFAULT returns 0 with discard false, and TRIM returns bound
// with discard false. Negative counts and unknown kinds fail.
func ApplyTryConstructSequence(kind TryConstructKind, n, bound int) (int, bool, error) {
	if n < 0 {
		return 0, false, fmt.Errorf("types: negative sequence length %d: %w", n, ErrInvalid)
	}
	if bound < 0 {
		return 0, false, fmt.Errorf("types: negative sequence bound %d: %w", bound, ErrInvalid)
	}
	if bound == 0 || n <= bound {
		return n, false, nil
	}
	switch kind {
	case TryConstructDiscard:
		return 0, true, nil
	case TryConstructUseDefault:
		return 0, false, nil
	case TryConstructTrim:
		return bound, false, nil
	default:
		return 0, false, fmt.Errorf("types: unknown try-construct %d: %w", int(kind), ErrInvalid)
	}
}

// CheckAppendableBound enforces an appendable size bound on decode.
//
// bound 0 means unbounded. contentSize above a positive bound fails with
// ErrInvalid so the caller drops the sample. Negative sizes fail.
func CheckAppendableBound(contentSize uint32, bound uint32) error {
	if bound == 0 {
		return nil
	}
	if uint64(contentSize) > uint64(bound) {
		return fmt.Errorf("types: appendable content %d exceeds bound %d: %w", contentSize, bound, ErrInvalid)
	}
	return nil
}

// ReadDHeaderWithBound reads a DHEADER and enforces an appendable bound.
//
// The size is checked by CheckAppendableBound before the caller consumes
// content. Overlong sizes count a drop via ReadDHeader or the bound
// check and return an error without allocating.
func (d *Decoder) ReadDHeaderWithBound(bound uint32) (uint32, error) {
	size, err := d.ReadDHeader()
	if err != nil {
		return 0, err
	}
	err = CheckAppendableBound(size, bound)
	if err != nil {
		d.drops++
		return 0, err
	}
	return size, nil
}

// WriteStructFramed frames one struct body per extensibility and version.
//
// Final types carry no delimiter: the body runs directly. Appendable
// types carry a DHEADER delimiter only under XCDR2 (NeedsDHeader); under
// V1 they are plain. Mutable types carry a DHEADER delimiter only under
// XCDR2; under V1 the caller writes PID members plus a sentinel with no
// outer delimiter. For V2 mutable the caller writes EMHeader members
// inside the delimited body. This is the engine framing dispatcher so
// production encoders never hard-code appendable.
func (e *Encoder) WriteStructFramed(ext Extensibility, ver EncodingVersion, body func() error) error {
	if err := ver.Validate(); err != nil {
		return err
	}
	if !ext.NeedsDHeader(ver) {
		if body == nil {
			return nil
		}
		return body()
	}
	return e.WriteStructxt6bDelimited(body)
}

// ReadStructFramed reads one struct body per extensibility and version.
//
// Undelimited kinds (final, V1 appendable, V1 mutable) run the body with
// size 0 and no DHEADER. Delimited kinds (V2 appendable, any mutable V2)
// read a DHEADER with an optional appendable bound then run the body with
// the content size. Unknown trailing members are skipped by the body via
// SkipUnknownxt6bMember (V2) or SkipPIDMemberContent (V1).
func (d *Decoder) ReadStructFramed(ext Extensibility, ver EncodingVersion, bound uint32, body func(size uint32) error) error {
	if err := ver.Validate(); err != nil {
		return err
	}
	if !ext.NeedsDHeader(ver) {
		if body == nil {
			return nil
		}
		err := body(0)
		if err != nil {
			return fmt.Errorf("types: framed body: %w", err)
		}
		return nil
	}
	return d.ReadStructxt6bDelimitedWithBound(bound, body)
}

// TypeConsistencyxt6aKind selects the overall assignability mode.
type TypeConsistencyxt6aKind int

// Type consistency kinds per 7.2.5.4.2.
const (
	Consistencyxt6aDisallowCoercion TypeConsistencyxt6aKind = iota
	Consistencyxt6aAllowCoercion
)

// String returns the symbolic name of the consistency kind.
func (k TypeConsistencyxt6aKind) String() string {
	switch k {
	case Consistencyxt6aDisallowCoercion:
		return "DISALLOW_TYPE_COERCION"
	case Consistencyxt6aAllowCoercion:
		return "ALLOW_TYPE_COERCION"
	default:
		return "UNKNOWN"
	}
}

// TypeConsistencyxt6aPolicy carries the reader side enforcement options.
//
// The fields mirror TypeConsistencyEnforcementQosPolicy per 7.6.3.4.1.
// They feed Evaluatext6aConsistency and the assignability tables in
// introspection.go and union.go.
type TypeConsistencyxt6aPolicy struct {
	Kind                   TypeConsistencyxt6aKind
	ForceTypeValidation    bool
	PreventTypeWidening    bool
	IgnoreSequenceBounds   bool
	IgnoreStringBounds     bool
	IgnoreMemberNames      bool
	IgnoreLiteralNames     bool
	IgnoreUnits            bool
	IgnoreUnspecifiedUnits bool
	IgnoreRanges           bool
}

// Defaultxt6aConsistency returns the spec default policy.
//
// The default kind is ALLOW_TYPE_COERCION with all other flags false,
// per 7.6.3.4.1.
func Defaultxt6aConsistency() TypeConsistencyxt6aPolicy {
	return TypeConsistencyxt6aPolicy{Kind: Consistencyxt6aAllowCoercion}
}

// Evaluatext6aConsistency applies the 7.6.3.4.2 matching steps.
//
// Step 1 uses type information when both sides provide it: with coercion
// disallowed the types must be minimally equivalent; with coercion
// allowed the reader type must be assignable from the writer type.
// Step 2 falls back to type name equality when either side lacks type
// information, and fails when force validation is set. Both sides reach
// the same verdict from the same inputs.
func Evaluatext6aConsistency(policy TypeConsistencyxt6aPolicy, minimalEquivalent, assignable, infoAvailable bool, readerName, writerName string) error {
	if infoAvailable {
		return evaluatext6aWithInfo(policy, minimalEquivalent, assignable)
	}
	if policy.ForceTypeValidation {
		return fmt.Errorf("types: consistency needs type info: %w", ErrInvalid)
	}
	if readerName != writerName {
		return fmt.Errorf("types: consistency type names %q vs %q: %w", readerName, writerName, ErrInvalid)
	}
	return nil
}

// evaluatext6aWithInfo runs step 1 when type info is available.
func evaluatext6aWithInfo(policy TypeConsistencyxt6aPolicy, minimalEquivalent, assignable bool) error {
	if policy.Kind == Consistencyxt6aDisallowCoercion {
		if !minimalEquivalent {
			return fmt.Errorf("types: consistency disallows coercion without minimal equivalence: %w", ErrInvalid)
		}
		return nil
	}
	if !assignable {
		return fmt.Errorf("types: consistency reader type not assignable: %w", ErrInvalid)
	}
	return nil
}

// MinimalEquivalentxt6a reports bidirectional assignability per 7.2.5.
//
// EXACT_TYPE matching (coercion disallowed) needs the reader and writer
// types to assign both ways through the 7.2.5 tables; one-way
// assignability is the ALLOW_TYPE_COERCION (MINIMAL) case decided by
// CheckTypeConsistencyxt6a.
func MinimalEquivalentxt6a(reader, writer TypeRefxt6a, policy TypeConsistencyxt6aPolicy) bool {
	if IsAssignablext6aFrom(reader, writer, policy) != nil {
		return false
	}

	return IsAssignablext6aFrom(writer, reader, policy) == nil
}

// CheckTypeConsistencyxt6a evaluates 7.6.3.4.2 step 1 from descriptors.
//
// It derives minimal equivalence (bidirectional assignability) plus
// reader-from-writer assignability through the 7.2.5 tables, then
// applies the policy kind via Evaluatext6aConsistency. Both sides reach
// the same verdict from the same inputs.
func CheckTypeConsistencyxt6a(policy TypeConsistencyxt6aPolicy, reader, writer TypeRefxt6a, readerName, writerName string) error {
	if reflect.DeepEqual(reader, writer) {
		return nil
	}

	assignable := IsAssignablext6aFrom(reader, writer, policy) == nil
	minimal := assignable && IsAssignablext6aFrom(writer, reader, policy) == nil

	return Evaluatext6aConsistency(policy, minimal, assignable, true, readerName, writerName)
}
