// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package dds

import (
	"bytes"
	"encoding/hex"
	"errors"
	"testing"

	"github.com/podomy/weld/internal/transports/dds/rtps"
	"github.com/podomy/weld/internal/types"
)

// keyedFlatPayload builds the flat keyed sample: seq u32, label string, count i32.
func keyedFlatPayload(t *testing.T) []byte {
	t.Helper()
	enc := types.NewEncoder(true)
	enc.WriteUint32(0x01020304)
	err := enc.WriteString("ab")
	if err != nil {
		t.Fatalf("encode label: %v", err)
	}
	enc.WriteInt32(0x05060708)
	return enc.Bytes()
}

// keyedNestedPayload builds the nested keyed sample: tag u8, code i16, value i32, tail u16.
func keyedNestedPayload(t *testing.T) []byte {
	t.Helper()
	enc := types.NewEncoder(true)
	enc.WriteUint8(0xAA)
	enc.WriteInt16(0x0302)
	enc.WriteInt32(0x0708090A)
	enc.WriteUint16(0x0504)
	return enc.Bytes()
}

// TestKeyedExtractFlatGolden pins canonical key bytes for two scalar keys.
func TestKeyedExtractFlatGolden(t *testing.T) {
	payload := keyedFlatPayload(t)
	key, err := ExtractKeyBytes(payload, types.KeyedTestMembers())
	if err != nil {
		t.Fatalf("extract flat: %v", err)
	}
	want := "0403020108070605"
	if got := hex.EncodeToString(key); got != want {
		t.Fatalf("flat key = %s, want %s", got, want)
	}
}

// TestKeyedExtractPaddingGolden pins alignment pad inside the key buffer.
func TestKeyedExtractPaddingGolden(t *testing.T) {
	members := []types.MessageMember{
		types.NewScalarMember("a", types.RosTypeUint8, 0),
		types.NewScalarMember("b", types.RosTypeUint32, 4),
	}
	members[0].IsKey = true
	members[1].IsKey = true
	desc := types.NewMessageMembers("weld_test", "Pad", 8, members, func(_ any, _ types.MessageInit) error { return nil }, func(_ any) error { return nil })
	enc := types.NewEncoder(true)
	enc.WriteUint8(0xAA)
	enc.WriteUint32(0x01020304)
	key, err := ExtractKeyBytes(enc.Bytes(), desc)
	if err != nil {
		t.Fatalf("extract padded: %v", err)
	}
	want := "aa00000004030201"
	if got := hex.EncodeToString(key); got != want {
		t.Fatalf("padded key = %s, want %s", got, want)
	}
}

// TestKeyedExtractNestedGolden pins leaf scoped key collection through a struct.
func TestKeyedExtractNestedGolden(t *testing.T) {
	payload := keyedNestedPayload(t)
	key, err := ExtractKeyBytes(payload, types.KeyedTestNestedMembers())
	if err != nil {
		t.Fatalf("extract nested: %v", err)
	}
	want := "aa0002030405"
	if got := hex.EncodeToString(key); got != want {
		t.Fatalf("nested key = %s, want %s", got, want)
	}
}

// TestKeyedExtractBigEndian pins the same logical keys in big endian order.
func TestKeyedExtractBigEndian(t *testing.T) {
	enc := types.NewEncoder(false)
	enc.WriteUint32(0x01020304)
	err := enc.WriteString("ab")
	if err != nil {
		t.Fatalf("encode label: %v", err)
	}
	enc.WriteInt32(0x05060708)
	key, err := ExtractKeyBytes(enc.Bytes(), types.KeyedTestMembers())
	if err != nil {
		t.Fatalf("extract big endian: %v", err)
	}
	want := "0102030405060708"
	if got := hex.EncodeToString(key); got != want {
		t.Fatalf("big endian key = %s, want %s", got, want)
	}
}

// TestKeyedExtractEmptyKeyType pins empty extraction for unkeyed types.
func TestKeyedExtractEmptyKeyType(t *testing.T) {
	members := []types.MessageMember{
		types.NewStringMember("data", types.RosTypeString, 0, 0),
	}
	desc := types.NewMessageMembers("std_msgs", "String", 8, members, func(_ any, _ types.MessageInit) error { return nil }, func(_ any) error { return nil })
	enc := types.NewEncoder(true)
	err := enc.WriteString("hello")
	if err != nil {
		t.Fatalf("encode string: %v", err)
	}
	key, err := ExtractKeyBytes(enc.Bytes(), desc)
	if err != nil {
		t.Fatalf("extract unkeyed: %v", err)
	}
	if len(key) != 0 {
		t.Fatalf("unkeyed key length = %d, want 0", len(key))
	}
}

// TestKeyedExtractTruncated pins typed errors for short payloads, never a panic.
func TestKeyedExtractTruncated(t *testing.T) {
	full := keyedFlatPayload(t)
	cuts := []struct {
		name string
		n    int
	}{
		{name: "empty", n: 0},
		{name: "short-header", n: 2},
		{name: "mid-seq", n: 6},
		{name: "mid-label", n: 10},
		{name: "mid-count", n: len(full) - 1},
	}
	for _, tc := range cuts {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ExtractKeyBytes(full[:tc.n], types.KeyedTestMembers())
			if err == nil {
				t.Fatalf("cut %d succeeded, want truncated failure", tc.n)
			}
			if !errors.Is(err, ErrKeyedDecodeFail) {
				t.Fatalf("cut %d error misses keyed sentinel: %v", tc.n, err)
			}
			if !errors.Is(err, types.ErrTruncated) {
				t.Fatalf("cut %d error misses truncated cause: %v", tc.n, err)
			}
		})
	}
}

// TestKeyedExtractBadHeader pins the invalid cause for a corrupt header.
func TestKeyedExtractBadHeader(t *testing.T) {
	payload := keyedFlatPayload(t)
	payload[1] = 0x02
	_, err := ExtractKeyBytes(payload, types.KeyedTestMembers())
	if err == nil {
		t.Fatalf("bad header succeeded, want invalid failure")
	}
	if !errors.Is(err, ErrKeyedDecodeFail) {
		t.Fatalf("bad header error misses keyed sentinel: %v", err)
	}
	if !errors.Is(err, types.ErrInvalid) {
		t.Fatalf("bad header error misses invalid cause: %v", err)
	}
}

// TestKeyedExtractBadDescriptor pins failures for nil and unresolvable descriptors.
func TestKeyedExtractBadDescriptor(t *testing.T) {
	payload := keyedFlatPayload(t)
	_, err := ExtractKeyBytes(payload, nil)
	if err == nil {
		t.Fatalf("nil descriptor succeeded, want failure")
	}
	if !errors.Is(err, ErrKeyedDecodeFail) {
		t.Fatalf("nil descriptor error misses keyed sentinel: %v", err)
	}
	members := []types.MessageMember{
		types.NewNestedMember("inner", 0, "Inner", "weld_test", "Missing"),
	}
	desc := types.NewMessageMembers("weld_test", "Broken", 8, members, func(_ any, _ types.MessageInit) error { return nil }, func(_ any) error { return nil })
	_, err = ExtractKeyBytes(payload, desc)
	if err == nil {
		t.Fatalf("missing nested table succeeded, want failure")
	}
	if !errors.Is(err, ErrKeyedDecodeFail) {
		t.Fatalf("missing nested error misses keyed sentinel: %v", err)
	}
}

// TestKeyedExtractSequenceKeyFails pins the loud reject for sequence keys.
func TestKeyedExtractSequenceKeyFails(t *testing.T) {
	members := []types.MessageMember{
		types.NewSequenceMember("items", types.RosTypeInt32, 8, 0, "Items"),
	}
	members[0].IsKey = true
	desc := types.NewMessageMembers("weld_test", "SeqKey", 8, members, func(_ any, _ types.MessageInit) error { return nil }, func(_ any) error { return nil })
	enc := types.NewEncoder(true)
	err := enc.WriteSequenceLength(1)
	if err != nil {
		t.Fatalf("encode length: %v", err)
	}
	enc.WriteInt32(7)
	_, err = ExtractKeyBytes(enc.Bytes(), desc)
	if err == nil {
		t.Fatalf("sequence key succeeded, want failure")
	}
	if !errors.Is(err, ErrKeyedDecodeFail) {
		t.Fatalf("sequence key error misses keyed sentinel: %v", err)
	}
	if errors.Is(err, types.ErrTruncated) {
		t.Fatalf("sequence key error must not look truncated: %v", err)
	}
}

// TestKeyedExtractLongDoubleKeyFails pins the unsupported codec reject.
func TestKeyedExtractLongDoubleKeyFails(t *testing.T) {
	members := []types.MessageMember{
		types.NewScalarMember("wide", types.RosTypeLongDbl, 0),
	}
	members[0].IsKey = true
	desc := types.NewMessageMembers("weld_test", "WideKey", 16, members, func(_ any, _ types.MessageInit) error { return nil }, func(_ any) error { return nil })
	enc := types.NewEncoder(true)
	enc.WriteFloat64(1.5)
	enc.WriteFloat64(2.5)
	_, err := ExtractKeyBytes(enc.Bytes(), desc)
	if err == nil {
		t.Fatalf("long double key succeeded, want failure")
	}
	if !errors.Is(err, ErrKeyedDecodeFail) {
		t.Fatalf("long double key error misses keyed sentinel: %v", err)
	}
}

// TestKeyedInstanceHandleGolden pins the SHA-256 handle for flat and empty keys.
func TestKeyedInstanceHandleGolden(t *testing.T) {
	key, err := ExtractKeyBytes(keyedFlatPayload(t), types.KeyedTestMembers())
	if err != nil {
		t.Fatalf("extract flat: %v", err)
	}
	got := InstanceHandle("weld_test__msg__Keyed", key)
	want := "aa800d2de33ad88e3afc08b6ef406686"
	if hex.EncodeToString(got[:]) != want {
		t.Fatalf("keyed handle = %s, want %s", hex.EncodeToString(got[:]), want)
	}
	empty := InstanceHandle("std_msgs__msg__String", nil)
	wantEmpty := "5e2429d5b77f69e8d2f9da1a6e302165"
	if hex.EncodeToString(empty[:]) != wantEmpty {
		t.Fatalf("empty handle = %s, want %s", hex.EncodeToString(empty[:]), wantEmpty)
	}
	if got == empty {
		t.Fatalf("keyed and empty handles must differ")
	}
	again := InstanceHandle("weld_test__msg__Keyed", key)
	if got != again {
		t.Fatalf("handle is not pure across calls")
	}
}

// TestKeyedInstanceHandleForPayload pins the extract plus handle one step path.
func TestKeyedInstanceHandleForPayload(t *testing.T) {
	got, err := InstanceHandleForPayload("weld_test__msg__Keyed", keyedFlatPayload(t), types.KeyedTestMembers())
	if err != nil {
		t.Fatalf("handle for payload: %v", err)
	}
	want := "aa800d2de33ad88e3afc08b6ef406686"
	if hex.EncodeToString(got[:]) != want {
		t.Fatalf("payload handle = %s, want %s", hex.EncodeToString(got[:]), want)
	}
	_, err = InstanceHandleForPayload("weld_test__msg__Keyed", []byte{0x00}, types.KeyedTestMembers())
	if !errors.Is(err, ErrKeyedDecodeFail) {
		t.Fatalf("short payload handle error = %v, want keyed sentinel", err)
	}
}

// keyedTestReader builds a local reader endpoint with the given keyed flag and hash.
func keyedTestReader(keyed bool, hash string) SEDPEndpoint {
	entity := SEDPEntityID{0x00, 0x10, 0x00, 0x04}
	if keyed {
		entity = KeyedUserEntity(0x001000, true)
	}
	return SEDPEndpoint{
		ID:       SEDPGUID{Entity: entity},
		ROSTopic: "/keyed",
		DDSTopic: "rt/keyed",
		TypeDDS:  "weld_test::msg::dds_::Keyed_",
		TypeIDL:  "weld_test__msg__Keyed",
		TypeHash: hash,
		IsReader: true,
		DomainID: 42,
	}
}

// keyedTestWriter builds a remote writer endpoint with the given keyed flag and hash.
func keyedTestWriter(keyed bool, hash string) SEDPEndpoint {
	entity := SEDPEntityID{0x00, 0x10, 0x00, 0x03}
	if keyed {
		entity = KeyedUserEntity(0x002000, false)
	}
	return SEDPEndpoint{
		ID:       SEDPGUID{Entity: entity},
		ROSTopic: "/keyed",
		DDSTopic: "rt/keyed",
		TypeDDS:  "weld_test::msg::dds_::Keyed_",
		TypeIDL:  "weld_test__msg__Keyed",
		TypeHash: hash,
		IsReader: false,
		DomainID: 42,
	}
}

// keyedMatchCase is one row of the keyed matching matrix.
type keyedMatchCase struct {
	name      string
	local     SEDPEndpoint
	remote    SEDPEndpoint
	wantMatch bool
	wantLoud  bool
}

// runKeyedMatchCase checks one matching row for match, silence, or loud reject.
func runKeyedMatchCase(t *testing.T, tc keyedMatchCase) {
	t.Helper()
	match, err := CheckKeyedMatch(tc.local, tc.remote)
	if tc.wantLoud {
		if !errors.Is(err, ErrKeyedMismatch) {
			t.Fatalf("case %s error = %v, want loud keyed mismatch", tc.name, err)
		}
		if match {
			t.Fatalf("case %s matched on loud incompatible", tc.name)
		}
		return
	}
	if err != nil {
		t.Fatalf("case %s error = %v, want nil", tc.name, err)
	}
	if match != tc.wantMatch {
		t.Fatalf("case %s match = %v, want %v", tc.name, match, tc.wantMatch)
	}
}

// TestKeyedMatchMatrix pins match, silent mismatch, and loud incompatible.
func TestKeyedMatchMatrix(t *testing.T) {
	const hashA = "RIHS01_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	const hashB = "RIHS01_bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	cases := []keyedMatchCase{
		{name: "unkeyed-pair", local: keyedTestReader(false, hashA), remote: keyedTestWriter(false, hashA), wantMatch: true},
		{name: "keyed-pair", local: keyedTestReader(true, hashA), remote: keyedTestWriter(true, hashA), wantMatch: true},
		{name: "hash-wildcard-local", local: keyedTestReader(true, ""), remote: keyedTestWriter(true, hashA), wantMatch: true},
		{name: "hash-wildcard-remote", local: keyedTestReader(false, hashA), remote: keyedTestWriter(false, ""), wantMatch: true},
		{name: "hash-mismatch-silent", local: keyedTestReader(false, hashA), remote: keyedTestWriter(false, hashB)},
		{name: "keyed-vs-unkeyed-loud", local: keyedTestReader(true, hashA), remote: keyedTestWriter(false, hashA), wantLoud: true},
		{name: "unkeyed-vs-keyed-loud", local: keyedTestReader(false, hashA), remote: keyedTestWriter(true, hashA), wantLoud: true},
		{name: "keyed-vs-unkeyed-loud-hashless", local: keyedTestReader(true, ""), remote: keyedTestWriter(false, ""), wantLoud: true},
		{name: "local-not-reader-loud", local: keyedTestWriter(false, hashA), remote: keyedTestWriter(false, hashA), wantLoud: true},
		{name: "remote-not-writer-loud", local: keyedTestReader(false, hashA), remote: keyedTestReader(false, hashA), wantLoud: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			runKeyedMatchCase(t, tc)
		})
	}
}

// TestKeyedUserEntityKinds pins with-key entity kinds against keyless ones.
func TestKeyedUserEntityKinds(t *testing.T) {
	writer := KeyedUserEntity(0x001000, false)
	if writer[3] != 0x02 {
		t.Fatalf("keyed writer kind = %#02x, want 0x02", writer[3])
	}
	reader := KeyedUserEntity(0x001000, true)
	if reader[3] != 0x07 {
		t.Fatalf("keyed reader kind = %#02x, want 0x07", reader[3])
	}
	if !bytes.Equal(writer[:3], []byte{0x00, 0x10, 0x00}) {
		t.Fatalf("keyed writer counter = % x, want 00 10 00", writer[:3])
	}
	if !EndpointKeyed(SEDPEndpoint{ID: SEDPGUID{Entity: writer}}) {
		t.Fatalf("keyed writer endpoint reads unkeyed")
	}
	if !EndpointKeyed(SEDPEndpoint{ID: SEDPGUID{Entity: reader}}) {
		t.Fatalf("keyed reader endpoint reads unkeyed")
	}
	keyless := SEDPEndpoint{ID: SEDPGUID{Entity: SEDPEntityID{0x00, 0x10, 0x00, 0x03}}}
	if EndpointKeyed(keyless) {
		t.Fatalf("keyless writer endpoint reads keyed")
	}
}

// TestKeyedHashParamRoundTrip pins KeyHash encode, lookup, and rejects.
func TestKeyedHashParamRoundTrip(t *testing.T) {
	var want [16]byte
	for i := range want {
		want[i] = byte(i + 1)
	}
	params := []rtps.Param{EncodeKeyHashParam(want)}
	got, found, err := LookupKeyHashParam(params)
	if err != nil {
		t.Fatalf("lookup keyhash: %v", err)
	}
	if !found {
		t.Fatalf("lookup keyhash missed present param")
	}
	if got != want {
		t.Fatalf("keyhash = %x, want %x", got, want)
	}
	_, found, err = LookupKeyHashParam(nil)
	if err != nil {
		t.Fatalf("lookup absent keyhash: %v", err)
	}
	if found {
		t.Fatalf("lookup absent keyhash reported found")
	}
	bad := []rtps.Param{{ID: 0x0070, Value: []byte{0x01, 0x02}}}
	_, _, err = LookupKeyHashParam(bad)
	if !errors.Is(err, ErrKeyedDecodeFail) {
		t.Fatalf("short keyhash error = %v, want keyed sentinel", err)
	}
}

// TestKeyedStatusInfoRoundTrip pins StatusInfo encode and lookup in both orders.
func TestKeyedStatusInfoRoundTrip(t *testing.T) {
	for _, le := range []bool{true, false} {
		params := []rtps.Param{EncodeStatusInfoParam(StatusInfoDisposedFlag, le)}
		got, found, err := LookupStatusInfoParam(params, le)
		if err != nil {
			t.Fatalf("lookup status le=%v: %v", le, err)
		}
		if !found {
			t.Fatalf("lookup status le=%v missed present param", le)
		}
		if got != StatusInfoDisposedFlag {
			t.Fatalf("status le=%v = %#x, want disposed flag", le, got)
		}
	}
	_, found, err := LookupStatusInfoParam(nil, true)
	if err != nil {
		t.Fatalf("lookup absent status: %v", err)
	}
	if found {
		t.Fatalf("lookup absent status reported found")
	}
	bad := []rtps.Param{{ID: 0x0071, Value: []byte{0x01}}}
	_, _, err = LookupStatusInfoParam(bad, true)
	if !errors.Is(err, ErrKeyedDecodeFail) {
		t.Fatalf("short status error = %v, want keyed sentinel", err)
	}
}

// TestKeyedDropReasonDistinct pins the new reason apart from existing reasons.
func TestKeyedDropReasonDistinct(t *testing.T) {
	if DropReasonKeyedDecodeFail == DropReasonUnknownWriter {
		t.Fatalf("keyed drop reason collides with unknown writer")
	}
	if DropReasonKeyedDecodeFail == DropReasonProvisionalDecodeFail {
		t.Fatalf("keyed drop reason collides with provisional decode fail")
	}
	if DropReasonKeyedDecodeFail == "" {
		t.Fatalf("keyed drop reason is empty")
	}
}

// TestKeyedDataFlagRoundTrip pins the RTPS Key flag through a DATA sample.
func TestKeyedDataFlagRoundTrip(t *testing.T) {
	key, err := ExtractKeyBytes(keyedFlatPayload(t), types.KeyedTestMembers())
	if err != nil {
		t.Fatalf("extract flat: %v", err)
	}
	data := rtps.Data{
		SequenceNumber: 1,
		Payload:        key,
		Key:            true,
	}
	raw, err := data.Submessage(true)
	if err != nil {
		t.Fatalf("keyed submessage: %v", err)
	}
	if raw.Flags&rtps.DataFlagKeyPresent == 0 {
		t.Fatalf("keyed submessage flags %#02x miss key bit", raw.Flags)
	}
	var drops rtps.Counter
	decoded, err := rtps.DecodeData(raw.Body, raw.Flags, &drops)
	if err != nil {
		t.Fatalf("decode keyed data: %v", err)
	}
	if !decoded.Key {
		t.Fatalf("decoded keyed data reports Key false")
	}
	if !bytes.Equal(decoded.Payload, key) {
		t.Fatalf("decoded key payload = %x, want %x", decoded.Payload, key)
	}
}

// TestKeyedDescriptorsValidate pins the synthetic tables as well formed.
func TestKeyedDescriptorsValidate(t *testing.T) {
	for name, desc := range map[string]*types.MessageMembers{
		"flat":   types.KeyedTestMembers(),
		"inner":  types.KeyedTestInnerMembers(),
		"nested": types.KeyedTestNestedMembers(),
	} {
		if err := desc.Validate(); err != nil {
			t.Fatalf("descriptor %s invalid: %v", name, err)
		}
	}
}
