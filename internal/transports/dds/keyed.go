// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package dds

// Keyed topic support for CycloneDDS interop.
//
// Weld treats all topics as unkeyed on the publish and take paths. Peers
// may still send keyed DATA: the DATA submessage carries the Key flag
// instead of the Data flag, inline QoS carries the 16 byte KeyHash PID
// 0x0070 plus the 4 byte StatusInfo PID 0x0071, and SEDP announces the
// endpoint with a with-key entity kind (writer 0x02, reader 0x07) instead
// of the keyless kinds (writer 0x03, reader 0x04) used in sedp.go. Native
// keyed types cannot talk to ROS types, and standard ROS types are
// normally unkeyed, so keyed handling must stay additive: unkeyed samples
// flow exactly as before, keyed content decodes through helpers here, and
// anything unparseable reports a typed error the caller maps to a drop.
//
// This file holds only pure helpers plus endpoint record support. Wiring
// into datapath.go TakeFrom, the SEDP upsert and matching paths, and the
// backend publish path is listed in the INTEGRATION trailer of the task
// report and stays out of this file so no existing file changes.
import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/podomy/weld/internal/transports/dds/rtps"
	"github.com/podomy/weld/internal/types"
)

// Keyed RTPS entity kinds.
//
// The kind byte is the last octet of the 4 byte entity id. Keyless Weld
// endpoints use 0x03 for writers and 0x04 for readers (see sedp.go); keyed
// endpoints use 0x02 and 0x07 for the same roles per the RTPS spec.
const (
	// keyedUserWriterKind marks a user writer with key.
	keyedUserWriterKind byte = 0x02
	// keyedUserReaderKind marks a user reader with key.
	keyedUserReaderKind byte = 0x07
)

// Keyed inline QoS parameter ids.
//
// These RTPS PIDs ride in DATA inline QoS for keyed samples. They are
// defined here because rtps/pid.go covers discovery PIDs only and that
// file stays untouched. Values match the RTPS spec: 0x0070 KeyHash and
// 0x0071 StatusInfo.
const (
	// keyedPIDKeyHash carries the 16 byte instance KeyHash.
	keyedPIDKeyHash uint16 = 0x0070
	// keyedPIDStatusInfo carries the 4 byte instance StatusInfo.
	keyedPIDStatusInfo uint16 = 0x0071
)

// Keyed content widths.
const (
	// keyedHashSize is the KeyHash width in bytes.
	keyedHashSize = 16
	// keyedStatusSize is the StatusInfo width in bytes.
	keyedStatusSize = 4
)

// StatusInfo flag bits for keyed instance state.
const (
	// StatusInfoDisposedFlag marks a disposed instance.
	StatusInfoDisposedFlag uint32 = 0x01
	// StatusInfoUnregisteredFlag marks an unregistered instance.
	StatusInfoUnregisteredFlag uint32 = 0x02
)

var (
	// ErrKeyedDecodeFail reports undecodable keyed content.
	//
	// Truncated payloads, unparseable KeyHash or StatusInfo params, and
	// unsupported key member types all wrap this sentinel so the caller
	// can map it to DropReasonKeyedDecodeFail. The cause underneath keeps
	// errors.Is working for types.ErrTruncated and types.ErrInvalid.
	ErrKeyedDecodeFail = errors.New("dds: keyed decode fail")
	// ErrKeyedMismatch reports a loud keyed versus unkeyed incompatibility.
	//
	// A keyed endpoint never silently matches an unkeyed one, even when
	// topic and type agree. This sentinel is a matching decision, not a
	// drop, so it stays separate from ErrKeyedDecodeFail.
	ErrKeyedMismatch = errors.New("dds: keyed mismatch")
)

// DropReasonKeyedDecodeFail counts drops from undecodable keyed content.
//
// It follows the DropReason pattern in datapath.go and is defined here so
// datapath.go stays untouched. It is distinct from DropReasonUnknownWriter
// and DropReasonProvisionalDecodeFail so dashboards tell keyed failures
// apart from discovery failures.
const DropReasonKeyedDecodeFail DropReason = "keyed-decode-fail"

// keyedFail wraps a keyed decode cause with the drop sentinel.
func keyedFail(op string, err error) error {
	return fmt.Errorf("dds: keyed %s: %w", op, errors.Join(ErrKeyedDecodeFail, err))
}

// EndpointKeyed reports whether an endpoint uses a with-key entity kind.
//
// Writer kind 0x02 and reader kind 0x07 count as keyed; keyless kinds
// 0x03 and 0x04 and any other kind count as unkeyed. Builtin entities
// (kind byte with 0xC0 bits set, such as the SEDP 0xC2 writers in
// sedp.go) are explicitly excluded before the kind comparison per the
// RTPS EntityKind table, so builtin discovery traffic never reads as
// keyed even though the check reads only the kind byte.
func EndpointKeyed(ep SEDPEndpoint) bool {
	kind := ep.ID.Entity[3]
	if kind&0xC0 == 0xC0 {
		return false
	}
	return kind == keyedUserWriterKind || kind == keyedUserReaderKind
}

// KeyedUserEntity builds a with-key user entity id above the builtin range.
//
// The counter layout mirrors sedpEntityFor: the 24 bit counter renders
// big endian and the kind goes last. Writers take kind 0x02 and readers
// take kind 0x07. SEDP declaration takes this entity through the future
// keyed declare path; this helper only mints the id.
func KeyedUserEntity(counter uint32, isReader bool) SEDPEntityID {
	kind := keyedUserWriterKind
	if isReader {
		kind = keyedUserReaderKind
	}
	var raw [4]byte
	binary.BigEndian.PutUint32(raw[:], counter)
	return SEDPEntityID{raw[1], raw[2], raw[3], kind}
}

// CheckKeyedMatch decides whether a local reader matches a remote writer.
//
// Roles are enforced loudly: a non-reader local or a reader remote is a
// caller bug and reports ErrKeyedMismatch instead of matching. Keyed
// versus unkeyed endpoints never match, loudly, even when hashes agree.
// Type hashes follow the hashless peer wildcard from the interop notes:
// an empty hash on either side matches anything, while two set hashes
// must be equal and differ silently with no match. Topic, kind, and
// domain filtering stay with SEDPEndpointsMatch; this check covers only
// roles plus keyed compatibility plus hash.
func CheckKeyedMatch(local, remote SEDPEndpoint) (bool, error) {
	if !local.IsReader {
		return false, fmt.Errorf("dds: keyed match local %s is not a reader: %w", local.ID.String(), ErrKeyedMismatch)
	}
	if remote.IsReader {
		return false, fmt.Errorf("dds: keyed match remote %s is not a writer: %w", remote.ID.String(), ErrKeyedMismatch)
	}
	localKeyed := EndpointKeyed(local)
	remoteKeyed := EndpointKeyed(remote)
	if localKeyed != remoteKeyed {
		return false, fmt.Errorf("dds: keyed match local keyed %v vs remote keyed %v: %w", localKeyed, remoteKeyed, ErrKeyedMismatch)
	}
	if local.TypeHash == "" || remote.TypeHash == "" {
		return true, nil
	}
	if local.TypeHash != remote.TypeHash {
		return false, nil
	}
	return true, nil
}

// ExtractKeyBytes returns the ordered key bytes for one XCDR1 sample.
//
// The payload is one full XCDR1 sample including the 4 byte encapsulation
// header in either byte order; topic DATA only, never a service envelope.
// The descriptor supplies the field layout. Key bytes concatenate the
// canonical CDR of every IsKey leaf member in field order: scalar and
// fixed array members contribute when flagged, and nested message members
// recurse so nested IsKey leaves contribute whether or not the nested
// member itself is flagged. The key encoding is canonical: alignment is
// relative to the key buffer start in the payload byte order, so key bytes
// never depend on surrounding non-key fields. Types with no keys return
// an empty slice and nil error. Truncated payloads, unresolvable nested
// tables, sequence members flagged as keys, and member types without a
// CDR codec all return an error wrapping ErrKeyedDecodeFail. The walk
// bounds checks every read through types.Decoder and never panics.
func ExtractKeyBytes(payload []byte, desc *types.MessageMembers) ([]byte, error) {
	if desc == nil {
		return nil, keyedFail("extract nil descriptor", errors.New("dds: keyed nil descriptor"))
	}
	dec, err := types.NewDecoder(payload)
	if err != nil {
		return nil, keyedFail("extract header", err)
	}
	enc := types.NewEncoder(dec.LittleEndian())
	err = extractWalkMembers(dec, enc, desc)
	if err != nil {
		return nil, err
	}
	if enc.Len() == types.HeaderSize {
		return []byte{}, nil
	}
	out := enc.Bytes()
	key := make([]byte, enc.Len()-types.HeaderSize)
	copy(key, out[types.HeaderSize:enc.Len()])
	return key, nil
}

// extractWalkMembers walks one message table, copying keys and skipping rest.
func extractWalkMembers(dec *types.Decoder, enc *types.Encoder, desc *types.MessageMembers) error {
	if desc == nil {
		return keyedFail("walk nil table", errors.New("dds: keyed nil table"))
	}
	for i := range desc.Members {
		m := &desc.Members[i]
		err := extractWalkMember(dec, enc, m)
		if err != nil {
			return err
		}
	}
	return nil
}

// extractWalkMember copies or skips one member by kind and key flag.
func extractWalkMember(dec *types.Decoder, enc *types.Encoder, m *types.MessageMember) error {
	if m.IsNested() {
		return extractWalkNested(dec, enc, m)
	}
	if m.IsSequence() {
		if m.IsKey {
			return keyedFail("walk key sequence "+m.Name, fmt.Errorf("dds: keyed member %q must not be a sequence: %w", m.Name, types.ErrInvalid))
		}
		return skipKeyedMember(dec, m)
	}
	if m.IsKey {
		return copyKeyedMember(dec, enc, m)
	}
	return skipKeyedMember(dec, m)
}

// extractWalkNested recurses into nested members, copying key leaves.
//
// Nested sequences flagged as keys are invalid. Unkeyed nested sequences
// are skipped element by element. Fixed arrays recurse per element; a
// plain nested member recurses once. Non-key leaves are skipped inside
// the recursion, so a nested table without keys contributes nothing.
func extractWalkNested(dec *types.Decoder, enc *types.Encoder, m *types.MessageMember) error {
	if m.IsSequence() {
		if m.IsKey {
			return keyedFail("walk key sequence "+m.Name, fmt.Errorf("dds: keyed member %q must not be a sequence: %w", m.Name, types.ErrInvalid))
		}
		return skipKeyedMember(dec, m)
	}
	nested, err := resolveKeyedNested(m)
	if err != nil {
		return err
	}
	if m.IsFixedArray() {
		for i := range m.ArraySize {
			err = extractWalkMembers(dec, enc, nested)
			if err != nil {
				return fmt.Errorf("dds: keyed nested array %q element %d: %w", m.Name, i, err)
			}
		}
		return nil
	}
	err = extractWalkMembers(dec, enc, nested)
	if err != nil {
		return fmt.Errorf("dds: keyed nested %q: %w", m.Name, err)
	}
	return nil
}

// resolveKeyedNested resolves the table for one nested member.
func resolveKeyedNested(m *types.MessageMember) (*types.MessageMembers, error) {
	nested, err := m.NestedMembers()
	if err != nil {
		return nil, keyedFail("resolve nested "+m.Name, err)
	}
	if nested == nil {
		return nil, keyedFail("resolve nested "+m.Name, errors.New("dds: keyed nil nested table"))
	}
	return nested, nil
}

// copyKeyedMember copies one key member from the decoder to the key encoder.
func copyKeyedMember(dec *types.Decoder, enc *types.Encoder, m *types.MessageMember) error {
	if m.IsFixedArray() {
		for i := range m.ArraySize {
			err := transferKeyedScalar(dec, enc, m, true)
			if err != nil {
				return fmt.Errorf("dds: keyed array %q element %d: %w", m.Name, i, err)
			}
		}
		return nil
	}
	err := transferKeyedScalar(dec, enc, m, true)
	if err != nil {
		return fmt.Errorf("dds: keyed copy %q: %w", m.Name, err)
	}
	return nil
}

// skipKeyedMember reads and discards one non-key member.
func skipKeyedMember(dec *types.Decoder, m *types.MessageMember) error {
	if m.IsFixedArray() {
		for i := range m.ArraySize {
			err := transferKeyedScalar(dec, nil, m, false)
			if err != nil {
				return fmt.Errorf("dds: keyed skip array %q element %d: %w", m.Name, i, err)
			}
		}
		return nil
	}
	if m.IsSequence() {
		return skipKeyedSequence(dec, m)
	}
	err := transferKeyedScalar(dec, nil, m, false)
	if err != nil {
		return fmt.Errorf("dds: keyed skip %q: %w", m.Name, err)
	}
	return nil
}

// skipKeyedSequence reads a length prefix then skips that many elements.
func skipKeyedSequence(dec *types.Decoder, m *types.MessageMember) error {
	n, err := dec.ReadBoundedSequenceLength(int(m.ArraySize))
	if err != nil {
		return keyedFail("skip sequence "+m.Name, err)
	}
	if m.IsNested() {
		nested, err := resolveKeyedNested(m)
		if err != nil {
			return err
		}
		for i := range n {
			err = extractSkipNested(dec, nested)
			if err != nil {
				return fmt.Errorf("dds: keyed skip sequence %q element %d: %w", m.Name, i, err)
			}
		}
		return nil
	}
	for i := range n {
		err = transferKeyedScalar(dec, nil, m, false)
		if err != nil {
			return fmt.Errorf("dds: keyed skip sequence %q element %d: %w", m.Name, i, err)
		}
	}
	return nil
}

// extractSkipNested skips every field of one nested table.
func extractSkipNested(dec *types.Decoder, nested *types.MessageMembers) error {
	for i := range nested.Members {
		err := skipKeyedMember(dec, &nested.Members[i])
		if err != nil {
			return err
		}
	}
	return nil
}

// keyedScalarFuncs maps scalar type ids to their transfer functions.
//
// Array indexing replaces a type switch: the exhaustive linter requires
// every enum member in a switch, while the complexity budget forbids a
// full switch, so a table with a bounds plus nil check reports both
// unknown ids and long doubles as typed failures. Message dispatches
// through transferNested before the table; long double has no slot
// because cdr.go carries no long double codec.
var keyedScalarFuncs = [19]func(*types.Decoder, *types.Encoder, bool, *types.MessageMember) error{
	types.RosTypeFloat:   transferFloat32,
	types.RosTypeDouble:  transferFloat64,
	types.RosTypeChar:    transferChar,
	types.RosTypeWChar:   transferWchar,
	types.RosTypeBoolean: transferBool,
	types.RosTypeOctet:   transferUint8,
	types.RosTypeUint8:   transferUint8,
	types.RosTypeInt8:    transferInt8,
	types.RosTypeUint16:  transferUint16,
	types.RosTypeInt16:   transferInt16,
	types.RosTypeUint32:  transferUint32,
	types.RosTypeInt32:   transferInt32,
	types.RosTypeUint64:  transferUint64,
	types.RosTypeInt64:   transferInt64,
	types.RosTypeString:  transferString,
	types.RosTypeWString: transferWString,
}

// transferKeyedScalar reads one scalar element, copying it when capture is set.
//
// A nil encoder means skip: the value is read and discarded. String bounds
// validate on read; writes preserve the exact wire bytes unconditionally.
// Messages recurse, long doubles fail without a codec, and unknown type
// ids fail invalid. Every failure wraps ErrKeyedDecodeFail.
func transferKeyedScalar(dec *types.Decoder, enc *types.Encoder, m *types.MessageMember, capture bool) error {
	if m.TypeID == types.RosTypeMessage {
		return transferNested(dec, enc, m, capture)
	}
	if m.TypeID == types.RosTypeLongDbl {
		return keyedFail("member "+m.Name, fmt.Errorf("dds: keyed member %q is long double without a codec: %w", m.Name, types.ErrInvalid))
	}
	id := uint8(m.TypeID)
	if int(id) < len(keyedScalarFuncs) && keyedScalarFuncs[id] != nil {
		return keyedScalarFuncs[id](dec, enc, capture, m)
	}
	return keyedFail("member "+m.Name, fmt.Errorf("dds: keyed member %q has unknown type %d: %w", m.Name, id, types.ErrInvalid))
}

// transferNested copies or skips one nested element for array and sequence slots.
func transferNested(dec *types.Decoder, enc *types.Encoder, m *types.MessageMember, capture bool) error {
	nested, err := resolveKeyedNested(m)
	if err != nil {
		return err
	}
	if !capture {
		return extractSkipNested(dec, nested)
	}
	return extractWalkMembers(dec, enc, nested)
}

// keyedReadError wraps one scalar read failure with the drop sentinel.
func keyedReadError(name, op string, err error) error {
	return keyedFail(op+" "+name, err)
}

// transferFloat32 moves one float32 element.
func transferFloat32(dec *types.Decoder, enc *types.Encoder, capture bool, m *types.MessageMember) error {
	v, err := dec.ReadFloat32()
	if err != nil {
		return keyedReadError(m.Name, "read float32", err)
	}
	if capture {
		enc.WriteFloat32(v)
	}
	return nil
}

// transferFloat64 moves one float64 element.
func transferFloat64(dec *types.Decoder, enc *types.Encoder, capture bool, m *types.MessageMember) error {
	v, err := dec.ReadFloat64()
	if err != nil {
		return keyedReadError(m.Name, "read float64", err)
	}
	if capture {
		enc.WriteFloat64(v)
	}
	return nil
}

// transferChar moves one char element.
func transferChar(dec *types.Decoder, enc *types.Encoder, capture bool, m *types.MessageMember) error {
	v, err := dec.ReadChar()
	if err != nil {
		return keyedReadError(m.Name, "read char", err)
	}
	if capture {
		enc.WriteChar(v)
	}
	return nil
}

// transferWchar moves one wchar element.
func transferWchar(dec *types.Decoder, enc *types.Encoder, capture bool, m *types.MessageMember) error {
	v, err := dec.ReadWchar()
	if err != nil {
		return keyedReadError(m.Name, "read wchar", err)
	}
	if capture {
		enc.WriteWchar(v)
	}
	return nil
}

// transferBool moves one bool element.
func transferBool(dec *types.Decoder, enc *types.Encoder, capture bool, m *types.MessageMember) error {
	v, err := dec.ReadBool()
	if err != nil {
		return keyedReadError(m.Name, "read bool", err)
	}
	if capture {
		enc.WriteBool(v)
	}
	return nil
}

// transferUint8 moves one octet element.
func transferUint8(dec *types.Decoder, enc *types.Encoder, capture bool, m *types.MessageMember) error {
	v, err := dec.ReadUint8()
	if err != nil {
		return keyedReadError(m.Name, "read uint8", err)
	}
	if capture {
		enc.WriteUint8(v)
	}
	return nil
}

// transferInt8 moves one int8 element.
func transferInt8(dec *types.Decoder, enc *types.Encoder, capture bool, m *types.MessageMember) error {
	v, err := dec.ReadInt8()
	if err != nil {
		return keyedReadError(m.Name, "read int8", err)
	}
	if capture {
		enc.WriteInt8(v)
	}
	return nil
}

// transferUint16 moves one uint16 element.
func transferUint16(dec *types.Decoder, enc *types.Encoder, capture bool, m *types.MessageMember) error {
	v, err := dec.ReadUint16()
	if err != nil {
		return keyedReadError(m.Name, "read uint16", err)
	}
	if capture {
		enc.WriteUint16(v)
	}
	return nil
}

// transferInt16 moves one int16 element.
func transferInt16(dec *types.Decoder, enc *types.Encoder, capture bool, m *types.MessageMember) error {
	v, err := dec.ReadInt16()
	if err != nil {
		return keyedReadError(m.Name, "read int16", err)
	}
	if capture {
		enc.WriteInt16(v)
	}
	return nil
}

// transferUint32 moves one uint32 element.
func transferUint32(dec *types.Decoder, enc *types.Encoder, capture bool, m *types.MessageMember) error {
	v, err := dec.ReadUint32()
	if err != nil {
		return keyedReadError(m.Name, "read uint32", err)
	}
	if capture {
		enc.WriteUint32(v)
	}
	return nil
}

// transferInt32 moves one int32 element.
func transferInt32(dec *types.Decoder, enc *types.Encoder, capture bool, m *types.MessageMember) error {
	v, err := dec.ReadInt32()
	if err != nil {
		return keyedReadError(m.Name, "read int32", err)
	}
	if capture {
		enc.WriteInt32(v)
	}
	return nil
}

// transferUint64 moves one uint64 element.
func transferUint64(dec *types.Decoder, enc *types.Encoder, capture bool, m *types.MessageMember) error {
	v, err := dec.ReadUint64()
	if err != nil {
		return keyedReadError(m.Name, "read uint64", err)
	}
	if capture {
		enc.WriteUint64(v)
	}
	return nil
}

// transferInt64 moves one int64 element.
func transferInt64(dec *types.Decoder, enc *types.Encoder, capture bool, m *types.MessageMember) error {
	v, err := dec.ReadInt64()
	if err != nil {
		return keyedReadError(m.Name, "read int64", err)
	}
	if capture {
		enc.WriteInt64(v)
	}
	return nil
}

// transferString moves one string element with bound validation on read.
func transferString(dec *types.Decoder, enc *types.Encoder, capture bool, m *types.MessageMember) error {
	v, err := dec.ReadBoundedString(int(m.StringUpperBound))
	if err != nil {
		return keyedReadError(m.Name, "read string", err)
	}
	if capture {
		err = enc.WriteString(v)
		if err != nil {
			return keyedReadError(m.Name, "write string", err)
		}
	}
	return nil
}

// transferWString moves one wstring element with bound validation on read.
func transferWString(dec *types.Decoder, enc *types.Encoder, capture bool, m *types.MessageMember) error {
	v, err := dec.ReadBoundedWString(int(m.StringUpperBound))
	if err != nil {
		return keyedReadError(m.Name, "read wstring", err)
	}
	if capture {
		err = enc.WriteWString(v)
		if err != nil {
			return keyedReadError(m.Name, "write wstring", err)
		}
	}
	return nil
}

// InstanceHandle derives the 16 byte instance handle for one key.
//
// The handle is the first 16 bytes of SHA-256 over the IDL type name,
// one 0x00 separator, then the ordered key bytes from ExtractKeyBytes.
// The separator keeps type names and key bytes unambiguous. The function
// is pure: equal inputs always yield equal handles.
func InstanceHandle(typeIDL string, keyBytes []byte) [16]byte {
	input := make([]byte, 0, len(typeIDL)+1+len(keyBytes))
	input = append(input, typeIDL...)
	input = append(input, 0x00)
	input = append(input, keyBytes...)
	sum := sha256.Sum256(input)
	var out [16]byte
	copy(out[:], sum[:16])
	return out
}

// InstanceHandleForPayload extracts the key and derives its handle in one step.
func InstanceHandleForPayload(typeIDL string, payload []byte, desc *types.MessageMembers) ([16]byte, error) {
	var empty [16]byte
	key, err := ExtractKeyBytes(payload, desc)
	if err != nil {
		return empty, err
	}
	return InstanceHandle(typeIDL, key), nil
}

// ParseKeyHashValue parses one 16 byte KeyHash inline QoS value.
func ParseKeyHashValue(value []byte) ([16]byte, error) {
	var out [16]byte
	if len(value) != keyedHashSize {
		return out, keyedFail("parse keyhash", fmt.Errorf("dds: keyed keyhash length %d, want %d: %w", len(value), keyedHashSize, types.ErrInvalid))
	}
	copy(out[:], value)
	return out, nil
}

// EncodeKeyHashParam renders one KeyHash inline QoS param for emission.
func EncodeKeyHashParam(hash [16]byte) rtps.Param {
	return rtps.Param{ID: keyedPIDKeyHash, Value: append([]byte(nil), hash[:]...)}
}

// LookupKeyHashParam returns the first KeyHash from inline QoS params.
//
// The first 0x0070 param wins, matching the SEDP first-wins convention.
// Absent KeyHash reports found false with nil error. A present but
// malformed value reports a typed decode failure.
func LookupKeyHashParam(params []rtps.Param) ([16]byte, bool, error) {
	var empty [16]byte
	for i := range params {
		if params[i].ID != keyedPIDKeyHash {
			continue
		}
		hash, err := ParseKeyHashValue(params[i].Value)
		if err != nil {
			return empty, false, err
		}
		return hash, true, nil
	}
	return empty, false, nil
}

// ParseStatusInfoValue parses one 4 byte StatusInfo inline QoS value.
func ParseStatusInfoValue(value []byte, littleEndian bool) (uint32, error) {
	if len(value) != keyedStatusSize {
		return 0, keyedFail("parse statusinfo", fmt.Errorf("dds: keyed statusinfo length %d, want %d: %w", len(value), keyedStatusSize, types.ErrInvalid))
	}
	if littleEndian {
		return binary.LittleEndian.Uint32(value), nil
	}
	return binary.BigEndian.Uint32(value), nil
}

// EncodeStatusInfoParam renders one StatusInfo inline QoS param for emission.
func EncodeStatusInfoParam(status uint32, littleEndian bool) rtps.Param {
	var value [keyedStatusSize]byte
	if littleEndian {
		binary.LittleEndian.PutUint32(value[:], status)
	} else {
		binary.BigEndian.PutUint32(value[:], status)
	}
	return rtps.Param{ID: keyedPIDStatusInfo, Value: value[:]}
}

// LookupStatusInfoParam returns the first StatusInfo from inline QoS params.
//
// The first 0x0071 param wins. Absent StatusInfo reports found false with
// nil error. A present but malformed value reports a typed decode failure.
func LookupStatusInfoParam(params []rtps.Param, littleEndian bool) (uint32, bool, error) {
	for i := range params {
		if params[i].ID != keyedPIDStatusInfo {
			continue
		}
		status, err := ParseStatusInfoValue(params[i].Value, littleEndian)
		if err != nil {
			return 0, false, err
		}
		return status, true, nil
	}
	return 0, false, nil
}
