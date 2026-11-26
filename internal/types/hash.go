// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

// RIHS01 type hash with canonical JSON encoding.
package types

import (
	"crypto/md5" //nolint:gosec // G501: MD5 is mandated by DDS-XTypes 7.3.3.10 for EquivalenceHash
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"unicode/utf16"
)

// Type hash versions and string form.
const (
	HashVersionUnset uint8 = 0
	HashVersion1     uint8 = 1
	HashPrefix             = "RIHS01_"
	HashStringLength       = 71
)

// C table layout and ASCII bounds for canonical JSON.
const (
	hashRows        = 4
	hashCols        = 8
	asciiUpperA     = 'A'
	asciiUpperZ     = 'Z'
	asciiControlEnd = 0x20
	asciiDEL        = 0x7f
	asciiMax        = 0x80
	bmpMax          = 0xffff
)

// UnsetTypeHash is the zero hash with version 0 and empty string form.
var UnsetTypeHash = TypeHash{Version: HashVersionUnset}

// TypeHash is one version byte plus 32 bytes of SHA-256.
type TypeHash struct {
	Version uint8
	Digest  [32]byte
}

// ParseTypeHash parses a RIHS01 string or empty unset encoding.
func ParseTypeHash(s string) (TypeHash, error) {
	if s == "" {
		return UnsetTypeHash, nil
	}
	if len(s) != HashStringLength {
		return UnsetTypeHash, fmt.Errorf("types: bad hash length %d, want %d", len(s), HashStringLength)
	}
	if !strings.HasPrefix(s, HashPrefix) {
		return UnsetTypeHash, fmt.Errorf("types: bad hash prefix %q", s)
	}
	hexPart := s[len(HashPrefix):]
	for i := range hexPart {
		c := hexPart[i]
		if c >= asciiUpperA && c <= asciiUpperZ {
			return UnsetTypeHash, errors.New("types: hash hex must be lowercase") //nolint:wrapcheck // new error, nothing to wrap
		}
	}
	raw, err := hex.DecodeString(hexPart)
	if err != nil {
		return UnsetTypeHash, fmt.Errorf("types: decode hash hex: %w", err)
	}
	if len(raw) != 32 {
		return UnsetTypeHash, fmt.Errorf("types: decoded hash length %d, want 32", len(raw))
	}
	var h TypeHash
	h.Version = HashVersion1
	copy(h.Digest[:], raw)
	return h, nil
}

// String returns RIHS01 plus 64 lowercase hex digits, or empty for unset.
func (h TypeHash) String() string {
	if h.Version == HashVersionUnset {
		return ""
	}
	return HashPrefix + hex.EncodeToString(h.Digest[:])
}

// IsUnset reports whether the hash is the unset version 0 value.
func (h TypeHash) IsUnset() bool {
	return h.Version == HashVersionUnset
}

// Equal reports whether two hashes share version and digest bytes.
func (h TypeHash) Equal(o TypeHash) bool {
	return h.Version == o.Version && h.Digest == o.Digest
}

// VersionedBytes returns version byte followed by the 32 digest bytes.
func (h TypeHash) VersionedBytes() [33]byte {
	var out [33]byte
	out[0] = h.Version
	copy(out[1:], h.Digest[:])
	return out
}

// ByteRows splits the digest into hashRows rows of hashCols bytes for static tables.
func (h TypeHash) ByteRows() [hashRows][hashCols]byte {
	var rows [hashRows][hashCols]byte
	for r := range hashRows {
		copy(rows[r][:], h.Digest[r*hashCols:(r+1)*hashCols])
	}
	return rows
}

// CTable emits the digest as a C static table with hashCols bytes per row over hashRows rows.
func (h TypeHash) CTable() string {
	rows := h.ByteRows()
	var b strings.Builder
	b.WriteString("{\n") //nolint:errcheck // strings.Builder writes never fail
	for r := range hashRows {
		b.WriteString("    ") //nolint:errcheck // strings.Builder writes never fail
		for c := range hashCols {
			writeHexByte(&b, rows[r][c])
			if c != hashCols-1 {
				b.WriteString(", ") //nolint:errcheck // strings.Builder writes never fail
			}
		}
		if r != hashRows-1 {
			b.WriteString(",") //nolint:errcheck // strings.Builder writes never fail
		}
		b.WriteString("\n") //nolint:errcheck // strings.Builder writes never fail
	}
	b.WriteString("}") //nolint:errcheck // strings.Builder writes never fail
	return b.String()
}

// writeHexByte writes one byte as 0x plus two lowercase hex digits.
func writeHexByte(b *strings.Builder, v byte) {
	const digits = "0123456789abcdef"
	b.WriteString("0x")         //nolint:errcheck // strings.Builder writes never fail
	b.WriteByte(digits[v>>4])   //nolint:errcheck // strings.Builder writes never fail
	b.WriteByte(digits[v&0x0f]) //nolint:errcheck // strings.Builder writes never fail
}

// CanonicalJSON returns the hashable JSON with separators comma space and colon space.
//
// Referenced descriptions are sorted by type name on a copy, so caller order
// never affects the hash. Nil and empty referenced plus fields encode
// identically. The sorted copy is validated before encoding.
func CanonicalJSON(desc TypeDescription) (string, error) {
	sorted := TypeDescription{
		Description: cloneIndividual(desc.Description),
		Referenced:  make([]IndividualTypeDescription, len(desc.Referenced)),
	}
	for i := range desc.Referenced {
		sorted.Referenced[i] = cloneIndividual(desc.Referenced[i])
	}
	sortIndividuals(sorted.Referenced)
	err := sorted.Validate()
	if err != nil {
		return "", fmt.Errorf("types: canonical json: %w", err)
	}
	var b strings.Builder
	writeTypeDescription(&b, sorted)
	return b.String(), nil
}

// ComputeTypeHash hashes the canonical JSON with defaults already omitted.
func ComputeTypeHash(desc TypeDescription) (TypeHash, error) {
	s, err := CanonicalJSON(desc)
	if err != nil {
		return UnsetTypeHash, fmt.Errorf("types: compute hash: %w", err)
	}
	sum := sha256.Sum256([]byte(s))
	return TypeHash{Version: HashVersion1, Digest: sum}, nil
}

// writeTypeDescription writes a full description in fixed key order.
func writeTypeDescription(b *strings.Builder, d TypeDescription) {
	b.WriteString(`{"type_description": `) //nolint:errcheck // strings.Builder writes never fail
	writeIndividual(b, d.Description)
	b.WriteString(`, "referenced_type_descriptions": [`) //nolint:errcheck // strings.Builder writes never fail
	refs := d.Referenced
	if refs == nil {
		refs = []IndividualTypeDescription{}
	}
	for i := range refs {
		if i > 0 {
			b.WriteString(", ") //nolint:errcheck // strings.Builder writes never fail
		}
		writeIndividual(b, refs[i])
	}
	b.WriteString(`]}`) //nolint:errcheck // strings.Builder writes never fail
}

// writeIndividual writes one individual description in fixed key order.
func writeIndividual(b *strings.Builder, d IndividualTypeDescription) {
	b.WriteString(`{"type_name": `) //nolint:errcheck // strings.Builder writes never fail
	writeJSONString(b, d.TypeName)
	b.WriteString(`, "fields": [`) //nolint:errcheck // strings.Builder writes never fail
	fields := d.Fields
	if fields == nil {
		fields = []Field{}
	}
	for i := range fields {
		if i > 0 {
			b.WriteString(", ") //nolint:errcheck // strings.Builder writes never fail
		}
		writeField(b, fields[i])
	}
	b.WriteString(`]}`) //nolint:errcheck // strings.Builder writes never fail
}

// writeField writes name and type only so default values never affect the hash.
func writeField(b *strings.Builder, f Field) {
	b.WriteString(`{"name": `) //nolint:errcheck // strings.Builder writes never fail
	writeJSONString(b, f.Name)
	b.WriteString(`, "type": `) //nolint:errcheck // strings.Builder writes never fail
	writeFieldType(b, f.Type)
	b.WriteString(`}`) //nolint:errcheck // strings.Builder writes never fail
}

// writeFieldType writes type id plus capacity plus string capacity plus nested name.
func writeFieldType(b *strings.Builder, t FieldType) {
	b.WriteString(`{"type_id": `)                           //nolint:errcheck // strings.Builder writes never fail
	b.WriteString(strconv.FormatUint(uint64(t.TypeID), 10)) //nolint:errcheck // strings.Builder writes never fail
	b.WriteString(`, "capacity": `)                         //nolint:errcheck // strings.Builder writes never fail
	b.WriteString(strconv.FormatUint(t.Capacity, 10))       //nolint:errcheck // strings.Builder writes never fail
	b.WriteString(`, "string_capacity": `)                  //nolint:errcheck // strings.Builder writes never fail
	b.WriteString(strconv.FormatUint(t.StringCapacity, 10)) //nolint:errcheck // strings.Builder writes never fail
	b.WriteString(`, "nested_type_name": `)                 //nolint:errcheck // strings.Builder writes never fail
	writeJSONString(b, t.NestedTypeName)
	b.WriteString(`}`) //nolint:errcheck // strings.Builder writes never fail
}

// writeJSONString writes a double quoted string with ASCII escaping.
func writeJSONString(b *strings.Builder, s string) {
	b.WriteByte('"') //nolint:errcheck // strings.Builder writes never fail
	for _, r := range s {
		writeJSONStringCh(b, r)
	}
	b.WriteByte('"') //nolint:errcheck // strings.Builder writes never fail
}

// writeJSONStringCh writes one rune with JSON short escapes for known controls.
func writeJSONStringCh(b *strings.Builder, r rune) {
	switch r {
	case '"':
		b.WriteString("\\\"") //nolint:errcheck // strings.Builder writes never fail
	case '\\':
		b.WriteString("\\\\") //nolint:errcheck // strings.Builder writes never fail
	case '\b':
		b.WriteString("\\b") //nolint:errcheck // strings.Builder writes never fail
	case '\f':
		b.WriteString("\\f") //nolint:errcheck // strings.Builder writes never fail
	case '\n':
		b.WriteString("\\n") //nolint:errcheck // strings.Builder writes never fail
	case '\r':
		b.WriteString("\\r") //nolint:errcheck // strings.Builder writes never fail
	case '\t':
		b.WriteString("\\t") //nolint:errcheck // strings.Builder writes never fail
	default:
		writeJSONStringEsc(b, r)
	}
}

// writeJSONStringEsc writes a non short escape rune as ASCII or \uXXXX.
func writeJSONStringEsc(b *strings.Builder, r rune) {
	switch {
	case r < asciiControlEnd || r == asciiDEL:
		b.WriteString("\\u")                //nolint:errcheck // strings.Builder writes never fail
		b.WriteString(lowerHex4(uint16(r))) //nolint:errcheck,gosec // Builder writes never fail; G115: case guards r to uint16 range
	case r < asciiMax:
		b.WriteByte(byte(r)) //nolint:errcheck // strings.Builder writes never fail
	case r <= bmpMax:
		b.WriteString("\\u")                //nolint:errcheck // strings.Builder writes never fail
		b.WriteString(lowerHex4(uint16(r))) //nolint:errcheck // strings.Builder writes never fail
	default:
		r1, r2 := utf16.EncodeRune(r)
		b.WriteString("\\u")                 //nolint:errcheck // strings.Builder writes never fail
		b.WriteString(lowerHex4(uint16(r1))) //nolint:errcheck,gosec // Builder writes never fail; G115: EncodeRune units fit uint16
		b.WriteString("\\u")                 //nolint:errcheck // strings.Builder writes never fail
		b.WriteString(lowerHex4(uint16(r2))) //nolint:errcheck,gosec // Builder writes never fail; G115: EncodeRune units fit uint16
	}
}

// lowerHex4 formats a 16 bit value as 4 lowercase hex digits.
func lowerHex4(v uint16) string {
	const digits = "0123456789abcdef"
	var out [4]byte
	out[0] = digits[(v>>12)&0xf]
	out[1] = digits[(v>>8)&0xf]
	out[2] = digits[(v>>4)&0xf]
	out[3] = digits[v&0xf]
	return string(out[:])
}

// Equivalencext6aHash is the 14 byte MD5 based type equivalence hash.
//
// It sits alongside the RIHS01 SHA-256 hash without replacing it. The
// model computes MD5 over the same canonical JSON used for RIHS01 and
// keeps the first 14 bytes. The wire form in EquivalenceHash computes
// over the serialized TypeObjectSeq CDR per 7.3.3.10; both share the
// 14 byte width but use distinct string prefixes (EQM14_ model versus
// EQ14_ wire) so a string never parses as the wrong kind.
type Equivalencext6aHash struct {
	Digest [14]byte
}

// equivalencext6aPrefix prefixes the model string form.
const equivalencext6aPrefix = "EQM14_"

// equivalencext6aHexLen is 14 bytes as 28 lowercase hex digits.
const equivalencext6aHexLen = 28

// ComputeEquivalencext6aHash returns the 14 byte equivalence hash.
func ComputeEquivalencext6aHash(desc TypeDescription) (Equivalencext6aHash, error) {
	s, err := CanonicalJSON(desc)
	if err != nil {
		return Equivalencext6aHash{}, fmt.Errorf("types: equivalence hash: %w", err)
	}
	sum := md5.Sum([]byte(s)) //nolint:gosec // G501: MD5 is mandated by DDS-XTypes 7.3.3.10
	var out Equivalencext6aHash
	copy(out.Digest[:], sum[:14])
	return out, nil
}

// String returns EQM14_ plus 28 lowercase hex digits.
func (h Equivalencext6aHash) String() string {
	return equivalencext6aPrefix + hex.EncodeToString(h.Digest[:])
}

// Equal reports whether two equivalence hashes share digest bytes.
func (h Equivalencext6aHash) Equal(o Equivalencext6aHash) bool {
	return h.Digest == o.Digest
}

// ParseEquivalencext6aHash parses the EQM14_ string form.
func ParseEquivalencext6aHash(s string) (Equivalencext6aHash, error) {
	var out Equivalencext6aHash
	if len(s) != len(equivalencext6aPrefix)+equivalencext6aHexLen {
		return out, fmt.Errorf("types: bad equivalence hash length %d", len(s))
	}
	if !strings.HasPrefix(s, equivalencext6aPrefix) {
		return out, fmt.Errorf("types: bad equivalence hash prefix %q", s)
	}
	raw, err := hex.DecodeString(s[len(equivalencext6aPrefix):])
	if err != nil {
		return out, fmt.Errorf("types: decode equivalence hash hex: %w", err)
	}
	copy(out.Digest[:], raw)
	return out, nil
}

// EquivalenceHash is the wire 14 byte XTypes equivalence hash.
//
// It is the first 14 bytes of MD5 over the CDR serialized TypeObjectSeq
// per DDS-XTypes 7.3.3.10. RIHS01 stays the ROS interop hash; this hash
// sits alongside for spec discovery and registry indexing. The string
// form uses EQ14_ while the model hash uses EQM14_; the bytes differ
// because the inputs differ (CDR versus canonical JSON) and the prefixes
// keep the domains distinct.
type EquivalenceHash struct {
	Digest [14]byte
}

// equivalencePrefix prefixes the wire string form.
const equivalencePrefix = "EQ14_"

// String returns EQ14_ plus 28 lowercase hex digits.
func (h EquivalenceHash) String() string {
	return equivalencePrefix + hex.EncodeToString(h.Digest[:])
}

// Equal reports whether two wire hashes share digest bytes.
func (h EquivalenceHash) Equal(o EquivalenceHash) bool {
	return h.Digest == o.Digest
}

// ParseEquivalenceHash parses the EQ14_ string form of the wire hash.
//
// It rejects the EQM14_ model prefix so model strings never parse as wire.
func ParseEquivalenceHash(s string) (EquivalenceHash, error) {
	var out EquivalenceHash
	if len(s) != len(equivalencePrefix)+equivalencext6aHexLen {
		return out, fmt.Errorf("types: bad equivalence hash length %d", len(s))
	}
	if !strings.HasPrefix(s, equivalencePrefix) {
		return out, fmt.Errorf("types: bad equivalence hash prefix %q", s)
	}
	raw, err := hex.DecodeString(s[len(equivalencePrefix):])
	if err != nil {
		return out, fmt.Errorf("types: decode equivalence hash hex: %w", err)
	}
	copy(out.Digest[:], raw)
	return out, nil
}

// ComputeEquivalenceHash returns the wire hash over a TypeObjectSeq CDR.
//
// seqBytes is the output of MarshalTypeObjectSeqCDR: sequence length
// plus DHEADER framed TypeObjects. The hash is MD5 truncated to 14
// bytes per 7.3.3.10.
func ComputeEquivalenceHash(seqBytes []byte) EquivalenceHash {
	sum := md5.Sum(seqBytes) //nolint:gosec // G501: MD5 is mandated by DDS-XTypes 7.3.3.10
	var out EquivalenceHash
	copy(out.Digest[:], sum[:14])
	return out
}

// ComputeWireEquivalenceHash builds the TypeObjectSeq CDR then hashes it.
//
// objects holds DHEADER framed TypeObjects from MarshalTypeObject.
// littleEndian selects the sequence count byte order. The returned hash
// pins the wire form; the model Equivalencext6aHash over canonical JSON
// is kept for indexing and differs.
func ComputeWireEquivalenceHash(objects [][]byte, littleEndian bool) (EquivalenceHash, error) {
	seq, err := MarshalTypeObjectSeqCDR(objects, littleEndian)
	if err != nil {
		return EquivalenceHash{}, fmt.Errorf("types: wire equivalence hash: %w", err)
	}
	return ComputeEquivalenceHash(seq), nil
}
