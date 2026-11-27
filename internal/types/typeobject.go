// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1
package types

import (
	"crypto/md5" //nolint:gosec // G501: MD5 is mandated by DDS-XTypes 7.3.3.5 for minimal name hash
	"fmt"
)

// Spec TypeObject and TypeIdentifier per DDS-XTypes 1.4 Annex B.
//
// Encoding uses XCDR2 with DHEADER per 7.3.3.5 and 7.3.3.6. Discovery
// matching consumes these blobs; assignability lives in introspection.

// EquivalenceKind values for TypeObject discriminators per Annex B.
//
// EK_MINIMAL selects the minimal variant and EK_COMPLETE selects the
// complete variant. EK_BOTH belongs to collection headers and is never
// a valid TypeObject or TypeIdentifier discriminator here.
const (
	EKMinimal  uint8 = 0xF1
	EKComplete uint8 = 0xF2
	EKBoth     uint8 = 0xF3
)

// TypeKind subset values for TypeIdentifier discriminators.
//
// The numbers follow Annex B Table 29 for the kinds implemented here.
// Only the subset needed for struct member types plus string bounds
// plus strongly connected hashes is covered; other kinds fall under the
// extended arm and round trip verbatim.
const (
	TKNone       uint8 = 0x00
	TKBoolean    uint8 = 0x01
	TKByte       uint8 = 0x02
	TKInt8       uint8 = 0x03
	TKUInt8      uint8 = 0x04
	TKInt16      uint8 = 0x05
	TKUInt16     uint8 = 0x06
	TKInt32      uint8 = 0x07
	TKUInt32     uint8 = 0x08
	TKInt64      uint8 = 0x09
	TKUInt64     uint8 = 0x0A
	TKFloat32    uint8 = 0x0B
	TKFloat64    uint8 = 0x0C
	TKFloat128   uint8 = 0x0D
	TKChar8      uint8 = 0x10
	TKChar16     uint8 = 0x11
	TKString8    uint8 = 0x70
	TKString16   uint8 = 0x71
	TIStrongComp uint8 = 0xB0
)

// TypeIdentifier is one spec TypeIdentifier union value.
//
// Primitives carry no body. String kinds carry a uint32 bound with 0
// meaning unbounded per Table 29. Hash kinds carry the 14 byte
// EquivalenceHash of the referenced TypeObject. Unknown discriminators
// are preserved in Extended for forward compatibility.
type TypeIdentifier struct {
	Kind  uint8
	Bound uint32
	Hash  [14]byte
	// Extended carries the raw body for unknown discriminators.
	Extended []byte
}

// IsHashKind reports whether the kind carries an equivalence hash.
func (id TypeIdentifier) IsHashKind() bool {
	return id.Kind == EKMinimal || id.Kind == EKComplete || id.Kind == TIStrongComp
}

// isPrimitiveTypeKind reports bare primitive kinds with no body.
func isPrimitiveTypeKind(kind uint8) bool {
	switch kind {
	case TKNone, TKBoolean, TKByte, TKInt8, TKUInt8, TKInt16, TKUInt16,
		TKInt32, TKUInt32, TKInt64, TKUInt64, TKFloat32, TKFloat64,
		TKFloat128, TKChar8, TKChar16:
		return true
	default:
		return false
	}
}

// writeTypeIdentifierBody encodes the body after the discriminator.
func writeTypeIdentifierBody(enc *Encoder, id TypeIdentifier) {
	switch {
	case id.Kind == TKString8 || id.Kind == TKString16:
		enc.WriteUint32(id.Bound)
	case id.IsHashKind():
		for _, b := range id.Hash {
			enc.WriteUint8(b)
		}
	case isPrimitiveTypeKind(id.Kind):
		// No body.
	default:
		for _, b := range id.Extended {
			enc.WriteUint8(b)
		}
	}
}

// MarshalTypeIdentifier encodes one identifier with CDR alignment.
//
// Layout is discriminator octet plus body with 4 byte alignment for the
// bound or raw 14 hash bytes. No DHEADER is used; the identifier rides
// inside TypeLookup sequences with their own length prefixes.
func MarshalTypeIdentifier(id TypeIdentifier, littleEndian bool) ([]byte, error) {
	enc := NewEncoder(littleEndian)
	enc.WriteUint8(id.Kind)
	writeTypeIdentifierBody(enc, id)
	// Use the raw buffer without finalize pad: identifiers are embedded.
	if len(enc.buf) < HeaderSize {
		return nil, fmt.Errorf("types: type identifier wire too short: %w", ErrTruncated)
	}
	out := append([]byte(nil), enc.buf[HeaderSize:]...)
	return out, nil
}

// readTypeIdentifierBody decodes the body after the discriminator.
func readTypeIdentifierBody(dec *Decoder, out *TypeIdentifier) error {
	kind := out.Kind
	if kind == EKBoth {
		return fmt.Errorf("types: EK_BOTH is not a type identifier discriminator: %w", ErrInvalid)
	}
	switch {
	case kind == TKString8 || kind == TKString16:
		bound, err := dec.ReadUint32()
		if err != nil {
			return err
		}
		out.Bound = bound
		return nil
	case TypeIdentifier{Kind: kind}.IsHashKind():
		for i := range out.Hash {
			b, err := dec.ReadUint8()
			if err != nil {
				return err
			}
			out.Hash[i] = b
		}
		return nil
	case isPrimitiveTypeKind(kind):
		return nil
	default:
		rest := dec.data[dec.off:]
		out.Extended = append([]byte(nil), rest...)
		return nil
	}
}

// UnmarshalTypeIdentifier decodes one identifier from CDR bytes.
func UnmarshalTypeIdentifier(data []byte, littleEndian bool) (TypeIdentifier, error) {
	var out TypeIdentifier
	// Prepend a header so the versioned decoder can parse.
	hdr := EncodeHeader(littleEndian)
	wire := append([]byte(nil), hdr[:]...)
	wire = append(wire, data...)
	dec, err := NewDecoder(wire)
	if err != nil {
		return out, err
	}
	kind, err := dec.ReadUint8()
	if err != nil {
		return out, err
	}
	out.Kind = kind
	err = readTypeIdentifierBody(dec, &out)
	if err != nil {
		return out, err
	}
	return out, nil
}

// MinimalMember is one member of a spec minimal struct.
type MinimalMember struct {
	MemberID       uint32
	Kind           uint8
	NameHash       [4]byte
	Capacity       uint64
	StringCapacity uint64
	NestedTypeName string
	Units          string
}

// CompleteMember is one member of a spec complete struct.
type CompleteMember struct {
	MemberID       uint32
	Kind           uint8
	Name           string
	Optional       bool
	MustUnderstand bool
	Capacity       uint64
	StringCapacity uint64
	NestedTypeName string
	Units          string
}

// MinimalTypeObject is one spec minimal struct body.
type MinimalTypeObject struct {
	TypeName string
	Ext      Extensibility
	Members  []MinimalMember
}

// CompleteTypeObject is one spec complete struct body.
type CompleteTypeObject struct {
	TypeName string
	Ext      Extensibility
	Members  []CompleteMember
}

// ComputeMinimalNameHash returns the minimal member name hash per 7.3.3.5.
//
// It is the first 4 bytes of MD5 over the member name. The raw-prefix form
// (first 4 name bytes) is rejected: it collides on long names sharing a
// prefix and disagrees across implementations.
func ComputeMinimalNameHash(name string) [4]byte {
	sum := md5.Sum([]byte(name)) //nolint:gosec // G501: MD5 is mandated by DDS-XTypes 7.3.3.5
	var out [4]byte
	copy(out[:], sum[:4])
	return out
}

// TypeObject is one spec TypeObject union value.
//
// Exactly one arm is non-nil. The discriminator is EK_MINIMAL or
// EK_COMPLETE per 7.3.3.5/7.3.3.6. Encoding uses XCDR2 with a DHEADER
// per Annex B: DHEADER over discriminator plus body.
type TypeObject struct {
	Minimal  *MinimalTypeObject
	Complete *CompleteTypeObject
}

// Discriminator returns EK_MINIMAL or EK_COMPLETE for the set arm.
func (o TypeObject) Discriminator() (uint8, error) {
	if o.Minimal != nil && o.Complete == nil {
		return EKMinimal, nil
	}
	if o.Complete != nil && o.Minimal == nil {
		return EKComplete, nil
	}
	return 0, fmt.Errorf("types: type object needs exactly one arm: %w", ErrInvalid)
}

// writeMinimalTypeObjectBody encodes the minimal arm after the discriminator.
func writeMinimalTypeObjectBody(enc *Encoder, obj *MinimalTypeObject) error {
	err := enc.WriteString(obj.TypeName)
	if err != nil {
		return err
	}
	enc.WriteUint8(uint8(obj.Ext)) //nolint:gosec // G115: Ext is 0-2 per Validate
	err = enc.WriteSequenceLength(len(obj.Members))
	if err != nil {
		return err
	}
	for _, m := range obj.Members {
		enc.WriteUint32(m.MemberID)
		enc.WriteUint8(m.Kind)
		for _, b := range m.NameHash {
			enc.WriteUint8(b)
		}
		enc.WriteUint64(m.Capacity)
		enc.WriteUint64(m.StringCapacity)
		err = enc.WriteString(m.NestedTypeName)
		if err != nil {
			return err
		}
		err = enc.WriteString(m.Units)
		if err != nil {
			return err
		}
	}
	return nil
}

// writeCompleteTypeObjectBody encodes the complete arm after the discriminator.
func writeCompleteTypeObjectBody(enc *Encoder, obj *CompleteTypeObject) error {
	err := enc.WriteString(obj.TypeName)
	if err != nil {
		return err
	}
	enc.WriteUint8(uint8(obj.Ext)) //nolint:gosec // G115: Ext is 0-2 per Validate
	err = enc.WriteSequenceLength(len(obj.Members))
	if err != nil {
		return err
	}
	for _, m := range obj.Members {
		enc.WriteUint32(m.MemberID)
		enc.WriteUint8(m.Kind)
		var flags byte
		if m.Optional {
			flags |= 0x01
		}
		if m.MustUnderstand {
			flags |= 0x02
		}
		enc.WriteUint8(flags)
		err = enc.WriteString(m.Name)
		if err != nil {
			return err
		}
		enc.WriteUint64(m.Capacity)
		enc.WriteUint64(m.StringCapacity)
		err = enc.WriteString(m.NestedTypeName)
		if err != nil {
			return err
		}
		err = enc.WriteString(m.Units)
		if err != nil {
			return err
		}
	}
	return nil
}

// MarshalTypeObject encodes one TypeObject with XCDR2 DHEADER framing.
//
// The output holds DHEADER plus discriminator plus body without any
// outer encapsulation header, matching the hash input of 7.3.4.5 and
// the vendor streams pinned there. Little endian selects the DHEADER
// byte order. The outer is always delimited (appendable framing) so
// TypeObjectSeq can split elements via their DHEADER sizes; this is
// correct for the TypeObject type itself and does not depend on the
// described type's extensibility (which is carried explicitly in the
// body Ext byte plus framing-independent detail). User-data structs use
// WriteStructFramed for per-kind final/plain, appendable/delimited, and
// mutable/PL framing.
func MarshalTypeObject(o TypeObject, littleEndian bool) ([]byte, error) {
	disc, err := o.Discriminator()
	if err != nil {
		return nil, err
	}
	enc := NewEncoderWithExt(ExtAppendable, littleEndian)
	err = enc.WriteStructxt6bDelimited(func() error {
		enc.WriteUint8(disc)
		if o.Minimal != nil {
			return writeMinimalTypeObjectBody(enc, o.Minimal)
		}
		return writeCompleteTypeObjectBody(enc, o.Complete)
	})
	if err != nil {
		return nil, err
	}
	// Strip the 4 byte encapsulation header: the DHEADER opens the object.
	if len(enc.buf) < HeaderSize {
		return nil, fmt.Errorf("types: type object wire too short: %w", ErrTruncated)
	}
	out := append([]byte(nil), enc.buf[HeaderSize:]...)
	return out, nil
}

// UnmarshalTypeObject decodes one DHEADER framed TypeObject.
//
// The blob carries its own DHEADER without an outer encapsulation header.
// Endianness is detected fail-closed: both orders are tried, and exactly
// one must fully consume the blob. Trying the hint first preserves the
// fast path, but a BE blob misread as LE (or trailing garbage accepted
// as LE) no longer silently wins. Ambiguous blobs where both orders parse
// fail instead of guessing.
func UnmarshalTypeObject(data []byte, littleEndian bool) (TypeObject, error) {
	first, errFirst := unmarshalTypeObjectWithLE(data, littleEndian)
	second, errSecond := unmarshalTypeObjectWithLE(data, !littleEndian)
	if errFirst == nil && errSecond == nil {
		if first.DiscriminatorEqual(second) {
			return first, nil
		}
		return first, fmt.Errorf("types: type object endian ambiguous: %w", ErrInvalid)
	}
	if errFirst == nil {
		return first, nil
	}
	if errSecond == nil {
		return second, nil
	}
	return first, errFirst
}

// DiscriminatorEqual reports whether two TypeObjects carry the same arm
// with equal bodies. It backs the fail-closed endian detection in
// UnmarshalTypeObject: when both orders parse, equal results keep the
// hint while differing results fail as ambiguous.
func (o TypeObject) DiscriminatorEqual(other TypeObject) bool {
	if o.Minimal != nil && other.Minimal != nil {
		return minimalEqual(*o.Minimal, *other.Minimal)
	}
	if o.Complete != nil && other.Complete != nil {
		return completeEqual(*o.Complete, *other.Complete)
	}
	return false
}

// minimalEqual compares two minimal bodies field by field.
func minimalEqual(a, b MinimalTypeObject) bool {
	if a.TypeName != b.TypeName || a.Ext != b.Ext || len(a.Members) != len(b.Members) {
		return false
	}
	for i := range a.Members {
		if a.Members[i] != b.Members[i] {
			return false
		}
	}
	return true
}

// completeEqual compares two complete bodies field by field.
func completeEqual(a, b CompleteTypeObject) bool {
	if a.TypeName != b.TypeName || a.Ext != b.Ext || len(a.Members) != len(b.Members) {
		return false
	}
	for i := range a.Members {
		if a.Members[i] != b.Members[i] {
			return false
		}
	}
	return true
}

// unmarshalTypeObjectWithLE decodes with an explicit byte order.
func unmarshalTypeObjectWithLE(data []byte, littleEndian bool) (TypeObject, error) {
	var out TypeObject
	hdr := EncodeEncapsulationxt6b(ExtAppendable, littleEndian)
	wire := append([]byte(nil), hdr[:]...)
	wire = append(wire, data...)
	dec, err := NewDecoder(wire)
	if err != nil {
		return out, err
	}
	var disc uint8
	var consumed uint32
	err = dec.ReadStructxt6bDelimited(func(size uint32) error {
		bodyStart := dec.off
		v, err := dec.ReadUint8()
		if err != nil {
			return err
		}
		disc = v
		switch disc {
		case EKMinimal:
			obj, err := readMinimalBody(dec)
			if err != nil {
				return err
			}
			out.Minimal = &obj
		case EKComplete:
			obj, err := readCompleteBody(dec)
			if err != nil {
				return err
			}
			out.Complete = &obj
		default:
			return fmt.Errorf("types: unknown equivalence kind %#02x: %w", disc, ErrInvalid)
		}
		consumed = uint32(dec.off - bodyStart) //nolint:gosec // G115: offsets are non-negative and bounded by wire length
		if consumed != size {
			return fmt.Errorf("types: type object body %d vs dheader %d: %w", consumed, size, ErrInvalid)
		}
		return nil
	})
	if err != nil {
		return out, err
	}
	if dec.off != len(dec.data) {
		return out, fmt.Errorf("types: type object has %d trailing bytes: %w", len(dec.data)-dec.off, ErrInvalid)
	}
	return out, nil
}

// readMinimalBody reads a minimal struct body after the discriminator.
func readMinimalBody(dec *Decoder) (MinimalTypeObject, error) {
	var out MinimalTypeObject
	typeName, ext, n, seen, err := readTypeObjectHeader(dec, "minimal")
	if err != nil {
		return out, err
	}
	out.TypeName = typeName
	out.Ext = ext
	out.Members = make([]MinimalMember, 0, n)
	for range n {
		m, err := readMinimalMember(dec, seen)
		if err != nil {
			return out, err
		}
		out.Members = append(out.Members, m)
	}
	return out, nil
}

// readTypeObjectHeader reads the shared TypeName plus Ext plus member count.
func readTypeObjectHeader(dec *Decoder, what string) (string, Extensibility, int, map[uint32]bool, error) {
	name, err := dec.ReadString()
	if err != nil {
		return "", ExtFinal, 0, nil, err
	}
	ext, err := readTypeObjectExt(dec, what)
	if err != nil {
		return "", ExtFinal, 0, nil, err
	}
	n, err := dec.ReadSequenceLength()
	if err != nil {
		return "", ExtFinal, 0, nil, err
	}
	err = dec.CheckSequenceAllocation(n, 8)
	if err != nil {
		return "", ExtFinal, 0, nil, err
	}
	seen := make(map[uint32]bool, n)
	return name, ext, n, seen, nil
}

// readTypeObjectExt reads and validates the extensibility byte.
func readTypeObjectExt(dec *Decoder, what string) (Extensibility, error) {
	ext, err := dec.ReadUint8()
	if err != nil {
		return ExtFinal, err
	}
	if ext != uint8(ExtFinal) && ext != uint8(ExtAppendable) && ext != uint8(ExtMutable) {
		return ExtFinal, fmt.Errorf("types: %s extensibility %d invalid: %w", what, ext, ErrInvalid)
	}
	return Extensibility(ext), nil
}

// readMinimalMember reads one minimal member with duplicate checks.
func readMinimalMember(dec *Decoder, seen map[uint32]bool) (MinimalMember, error) {
	var m MinimalMember
	id, err := dec.ReadUint32()
	if err != nil {
		return m, err
	}
	if seen[id] {
		return m, fmt.Errorf("types: duplicate minimal member id %d: %w", id, ErrInvalid)
	}
	seen[id] = true
	m.MemberID = id
	kind, err := dec.ReadUint8()
	if err != nil {
		return m, err
	}
	m.Kind = kind
	for i := range m.NameHash {
		b, err := dec.ReadUint8()
		if err != nil {
			return m, err
		}
		m.NameHash[i] = b
	}
	capacity, err := dec.ReadUint64()
	if err != nil {
		return m, err
	}
	m.Capacity = capacity
	scap, err := dec.ReadUint64()
	if err != nil {
		return m, err
	}
	m.StringCapacity = scap
	nested, err := dec.ReadString()
	if err != nil {
		return m, err
	}
	m.NestedTypeName = nested
	units, err := dec.ReadString()
	if err != nil {
		return m, err
	}
	m.Units = units
	return m, nil
}

// readCompleteBody reads a complete struct body after the discriminator.
func readCompleteBody(dec *Decoder) (CompleteTypeObject, error) {
	var out CompleteTypeObject
	typeName, ext, n, seen, err := readTypeObjectHeader(dec, "complete")
	if err != nil {
		return out, err
	}
	out.TypeName = typeName
	out.Ext = ext
	out.Members = make([]CompleteMember, 0, n)
	for range n {
		m, err := readCompleteMember(dec, seen)
		if err != nil {
			return out, err
		}
		out.Members = append(out.Members, m)
	}
	return out, nil
}

// readCompleteMember reads one complete member with duplicate id checks.
func readCompleteMember(dec *Decoder, seen map[uint32]bool) (CompleteMember, error) {
	var m CompleteMember
	id, err := dec.ReadUint32()
	if err != nil {
		return m, err
	}
	if seen[id] {
		return m, fmt.Errorf("types: duplicate complete member id %d: %w", id, ErrInvalid)
	}
	seen[id] = true
	m.MemberID = id
	kind, err := dec.ReadUint8()
	if err != nil {
		return m, err
	}
	m.Kind = kind
	flags, err := dec.ReadUint8()
	if err != nil {
		return m, err
	}
	if flags&0xFC != 0 {
		return m, fmt.Errorf("types: member flags %#02x reserved: %w", flags, ErrInvalid)
	}
	m.Optional = flags&0x01 != 0
	m.MustUnderstand = flags&0x02 != 0
	nm, err := dec.ReadString()
	if err != nil {
		return m, err
	}
	m.Name = nm
	capacity, err := dec.ReadUint64()
	if err != nil {
		return m, err
	}
	m.Capacity = capacity
	scap, err := dec.ReadUint64()
	if err != nil {
		return m, err
	}
	m.StringCapacity = scap
	nested, err := dec.ReadString()
	if err != nil {
		return m, err
	}
	m.NestedTypeName = nested
	units, err := dec.ReadString()
	if err != nil {
		return m, err
	}
	m.Units = units
	return m, nil
}
