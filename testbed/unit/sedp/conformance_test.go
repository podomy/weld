// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

//nolint:cyclop // sedp package holds many sequential conformance assertions, high average reflects breadth
package unit_sedp

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/podomy/weld/internal/transports/dds"
	"github.com/podomy/weld/internal/transports/dds/rtps"
)

// Expected interop identity for rt/chatter on domain 42.
const (
	sedpConformTypeHash = "RIHS01_df668c740482bbd48fb39d76a70dfd4bd59db1288021743503259e948f6b1a18"
	sedpConformDDSTopic = "rt/chatter"
	sedpConformROSTopic = "/chatter"
	sedpConformTypeDDS  = "std_msgs::msg::dds_::String_"
	sedpConformTypeIDL  = "std_msgs__msg__String"
	sedpConformDomain   = uint32(42)
)

// Wire constants mirrored for assertions without importing private symbols.
const (
	sedpConformHeaderLen     = 20
	sedpConformSubHeaderLen  = 4
	sedpConformDataFixedLen  = 20
	sedpConformSeqOff        = 12
	sedpConformWriterOff     = 8
	sedpConformReaderOff     = 4
	sedpConformHeartbeatLen  = 28
	sedpConformSubDATA       = 0x15
	sedpConformSubHeartbeat  = 0x07
	sedpConformFlagLE        = 0x01
	sedpConformFlagInlineQoS = 0x02
	sedpConformFlagData      = 0x04
)

// Builtin writer entities on the wire.
var (
	sedpConformPubsWriter        = [4]byte{0x00, 0x00, 0x02, 0xC2}
	sedpConformPubsWriterCyclone = [4]byte{0x00, 0x00, 0x03, 0xC2}
	sedpConformSubsWriter        = [4]byte{0x00, 0x00, 0x04, 0xC2}
)

// sedpConformPrefix returns a deterministic 12 byte prefix for tests.
func sedpConformPrefix(base byte) dds.SEDPGUIDPrefix {
	var prefix dds.SEDPGUIDPrefix
	for i := range prefix {
		prefix[i] = base + byte(i)
	}
	return prefix
}

// sedpConformDeclareWriter declares a writer for the canonical chatter identity.
//
//nolint:unparam // test helper keeps explicit type hash param for clarity across hash cases
func sedpConformDeclareWriter(t *testing.T, idx *dds.SEDPIndex, typeHash string) dds.SEDPEndpoint {
	t.Helper()
	ep, err := idx.DeclareWriter(sedpConformROSTopic, sedpConformTypeIDL, typeHash, nil)
	if err != nil {
		t.Fatalf("declare writer: %v", err)
	}
	return ep
}

// sedpConformDeclareReader declares a reader for the canonical chatter identity.
func sedpConformDeclareReader(t *testing.T, idx *dds.SEDPIndex, typeHash string) dds.SEDPEndpoint {
	t.Helper()
	ep, err := idx.DeclareReader(sedpConformROSTopic, sedpConformTypeIDL, typeHash, nil)
	if err != nil {
		t.Fatalf("declare reader: %v", err)
	}
	return ep
}

// sedpConformNewIndex returns an index with a fixed prefix and domain.
func sedpConformNewIndex(t *testing.T, base byte, domain uint32) *dds.SEDPIndex {
	t.Helper()
	prefix := sedpConformPrefix(base)
	idx, err := dds.NewSEDPIndex(prefix, domain)
	if err != nil {
		t.Fatalf("new index: %v", err)
	}
	return idx
}

// sedpConformMustSerialize serializes an endpoint and fails on error.
func sedpConformMustSerialize(t *testing.T, ep dds.SEDPEndpoint) []byte {
	t.Helper()
	raw, err := dds.SerializeSEDPDATA(ep)
	if err != nil {
		t.Fatalf("serialize: %v", err)
	}
	return raw
}

// sedpConformMustParse parses a sample and fails on error.
func sedpConformMustParse(t *testing.T, raw []byte) dds.SEDPEndpoint {
	t.Helper()
	ep, err := dds.ParseSEDPDATA(raw)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	return ep
}

// sedpConformSub is one framed submessage for assertions.
type sedpConformSub struct {
	id    byte
	flags byte
	body  []byte
}

// sedpConformSplit splits an RTPS datagram into submessages.
func sedpConformSplit(t *testing.T, raw []byte) []sedpConformSub {
	t.Helper()
	if len(raw) < sedpConformHeaderLen {
		t.Fatalf("datagram too short: %d", len(raw))
	}
	if string(raw[0:4]) != "RTPS" {
		t.Fatalf("bad magic %q", raw[0:4])
	}
	if raw[4] != 2 {
		t.Fatalf("major = %d, want 2", raw[4])
	}
	out := []sedpConformSub{}
	off := sedpConformHeaderLen
	for off < len(raw) {
		if len(raw)-off < sedpConformSubHeaderLen {
			t.Fatalf("submessage header truncated at %d", off)
		}
		id := raw[off]
		flags := raw[off+1]
		var length int
		if flags&sedpConformFlagLE != 0 {
			length = int(binary.LittleEndian.Uint16(raw[off+2 : off+4]))
		} else {
			length = int(binary.BigEndian.Uint16(raw[off+2 : off+4]))
		}
		end := off + sedpConformSubHeaderLen + length
		if end > len(raw) {
			t.Fatalf("submessage %#02x length %d overruns", id, length)
		}
		body := append([]byte(nil), raw[off+sedpConformSubHeaderLen:end]...)
		out = append(out, sedpConformSub{id: id, flags: flags, body: body})
		off = end
	}
	return out
}

// sedpConformFindDATA returns the first DATA submessage.
func sedpConformFindDATA(t *testing.T, subs []sedpConformSub) sedpConformSub {
	t.Helper()
	for _, sub := range subs {
		if sub.id == sedpConformSubDATA {
			return sub
		}
	}
	t.Fatalf("no DATA submessage found")
	return sedpConformSub{}
}

// sedpConformWriterOf returns the writer entity of a DATA body.
func sedpConformWriterOf(t *testing.T, body []byte) [4]byte {
	t.Helper()
	if len(body) < sedpConformDataFixedLen {
		t.Fatalf("DATA body too short: %d", len(body))
	}
	var writer [4]byte
	copy(writer[:], body[sedpConformWriterOff:sedpConformWriterOff+4])
	return writer
}

// sedpConformSeqOf returns high and low words of the DATA sequence.
func sedpConformSeqOf(t *testing.T, body []byte) (uint32, uint32) {
	t.Helper()
	if len(body) < sedpConformDataFixedLen {
		t.Fatalf("DATA body too short for seq: %d", len(body))
	}
	high := binary.LittleEndian.Uint32(body[sedpConformSeqOff : sedpConformSeqOff+4])
	low := binary.LittleEndian.Uint32(body[sedpConformSeqOff+4 : sedpConformSeqOff+8])
	return high, low
}

// sedpConformInlineParams decodes inline QoS params from a DATA body.
func sedpConformInlineParams(t *testing.T, body []byte, flags byte) []rtps.Param {
	t.Helper()
	if len(body) < sedpConformDataFixedLen {
		t.Fatalf("DATA body too short for params: %d", len(body))
	}
	little := flags&sedpConformFlagLE != 0
	if !little {
		t.Fatalf("expected little endian DATA flags %#02x", flags)
	}
	rest := body[sedpConformDataFixedLen:]
	params, _, err := rtps.DecodeParamsEndian(rest, little)
	if err != nil {
		t.Fatalf("decode inline params: %v", err)
	}
	return params
}

// sedpConformFindParam returns the first value for an id.
func sedpConformFindParam(t *testing.T, params []rtps.Param, id uint16) []byte {
	t.Helper()
	for _, param := range params {
		if param.ID == id {
			return param.Value
		}
	}
	t.Fatalf("param %#04x not found", id)
	return nil
}

// sedpConformHasParam reports whether an id is present.
func sedpConformHasParam(params []rtps.Param, id uint16) bool {
	for _, param := range params {
		if param.ID == id {
			return true
		}
	}
	return false
}

// sedpConformCheckPayloadCopy asserts a PL_CDR payload copy follows inline params.
func sedpConformCheckPayloadCopy(t *testing.T, body []byte, flags byte) []byte {
	t.Helper()
	if len(body) < sedpConformDataFixedLen {
		t.Fatalf("DATA body too short for payload: %d", len(body))
	}
	little := flags&sedpConformFlagLE != 0
	if !little {
		t.Fatalf("expected little endian DATA flags %#02x", flags)
	}
	rest := body[sedpConformDataFixedLen:]
	_, consumed, err := rtps.DecodeParamsEndian(rest, little)
	if err != nil {
		t.Fatalf("decode inline params for payload split: %v", err)
	}
	payload := rest[consumed:]
	if len(payload) < 4 {
		t.Fatalf("payload copy too short: %d, want at least 4", len(payload))
	}
	if payload[0] != 0x00 || payload[1] != 0x03 || payload[2] != 0x00 || payload[3] != 0x00 {
		t.Fatalf("payload encap = % X, want 00 03 00 00 for PL_CDR LE", payload[0:4])
	}
	return payload
}

// sedpConformEncodeStringPad renders a CDR string with pad included in length.
func sedpConformEncodeStringPad(s string) []byte {
	out := make([]byte, 0, 4+len(s)+4)
	out = binary.LittleEndian.AppendUint32(out, uint32(len(s)+1)) //nolint:gosec // G115: test strings are short, length fits uint32
	out = append(out, s...)
	out = append(out, 0x00)
	for len(out)%4 != 0 {
		out = append(out, 0x00)
	}
	binary.LittleEndian.PutUint32(out[0:4], uint32(len(out)-4)) //nolint:gosec // G115: test buffer length is small, fits uint32
	return out
}

// sedpConformEncodeStringMinimal renders a CDR string with length excluding pad.
func sedpConformEncodeStringMinimal(s string) []byte {
	out := make([]byte, 0, 4+len(s)+1)
	out = binary.LittleEndian.AppendUint32(out, uint32(len(s)+1)) //nolint:gosec // G115: test strings are short, length fits uint32
	out = append(out, s...)
	out = append(out, 0x00)
	return out
}

// sedpConformEncodeSequencePad renders an octet sequence with pad in count.
func sedpConformEncodeSequencePad(s string) []byte {
	out := make([]byte, 0, 4+len(s)+4)
	out = binary.LittleEndian.AppendUint32(out, uint32(len(s))) //nolint:gosec // G115: test strings are short, length fits uint32
	out = append(out, s...)
	for len(out)%4 != 0 {
		out = append(out, 0x00)
	}
	binary.LittleEndian.PutUint32(out[0:4], uint32(len(out)-4)) //nolint:gosec // G115: test buffer length is small, fits uint32
	return out
}

// sedpConformEncodeSequenceMinimal renders an octet sequence with minimal count.
func sedpConformEncodeSequenceMinimal(s string) []byte {
	out := make([]byte, 0, 4+len(s))
	out = binary.LittleEndian.AppendUint32(out, uint32(len(s))) //nolint:gosec // G115: test strings are short, length fits uint32
	out = append(out, s...)
	return out
}

// sedpConformGUIDValue renders a 16 byte endpoint GUID value.
func sedpConformGUIDValue(id dds.SEDPGUID) []byte {
	out := make([]byte, 0, 16)
	out = append(out, id.Prefix[:]...)
	out = append(out, id.Entity[:]...)
	return out
}

// sedpConformBuildBody builds a DATA body with the given writer, seq, and params.
func sedpConformBuildBody(writer [4]byte, seq uint64, params []rtps.Param) []byte {
	encoded := rtps.EncodeParams(params)
	body := make([]byte, 0, sedpConformDataFixedLen+len(encoded))
	body = append(body, 0x00, 0x00)
	body = binary.LittleEndian.AppendUint16(body, 16)
	body = append(body, 0x00, 0x00, 0x00, 0x00)
	body = append(body, writer[:]...)
	body = binary.LittleEndian.AppendUint32(body, uint32(seq>>32))
	body = binary.LittleEndian.AppendUint32(body, uint32(seq)) //nolint:gosec // G115: test sequence numbers are small, fit uint32
	body = append(body, encoded...)
	return body
}

// sedpConformBuildMessage wraps one DATA body in an RTPS message.
//
//nolint:unparam // test helper keeps explicit flags param for clarity across flag cases
func sedpConformBuildMessage(prefix dds.SEDPGUIDPrefix, flags byte, body []byte) []byte {
	out := make([]byte, 0, sedpConformHeaderLen+sedpConformSubHeaderLen+len(body))
	out = append(out, 'R', 'T', 'P', 'S')
	out = append(out, 0x02, 0x01, 0x01, 0x10)
	out = append(out, prefix[:]...)
	out = append(out, sedpConformSubDATA, flags)
	out = binary.LittleEndian.AppendUint16(out, uint16(len(body))) //nolint:gosec // G115: test body length is small, fits uint16
	out = append(out, body...)
	return out
}

// sedpConformIdentityParams returns topic, type, and GUID params for an endpoint.
func sedpConformIdentityParams(ep dds.SEDPEndpoint, topicValue, typeValue []byte, guidID uint16) []rtps.Param {
	return []rtps.Param{
		{ID: rtps.PIDTopicName, Value: topicValue},
		{ID: rtps.PIDTypeName, Value: typeValue},
		{ID: guidID, Value: sedpConformGUIDValue(ep.ID)},
	}
}

// sedpConformCheckStringValue asserts a CDR string value carries pad length, NUL, and pad.
func sedpConformCheckStringValue(t *testing.T, value []byte, want string) {
	t.Helper()
	if len(value) < 4 {
		t.Fatalf("string value too short: %d", len(value))
	}
	length := binary.LittleEndian.Uint32(value[0:4])
	if length%4 != 0 {
		t.Fatalf("string length %d is not pad-in-length (mod 4 != 0)", length)
	}
	if int(length) != len(value)-4 {
		t.Fatalf("string length %d does not match value %d", length, len(value)-4)
	}
	raw := value[4:]
	nul := bytes.IndexByte(raw, 0x00)
	if nul < 0 {
		t.Fatalf("string value misses NUL terminator")
	}
	got := string(raw[:nul])
	if got != want {
		t.Fatalf("string = %q, want %q", got, want)
	}
	for _, b := range raw[nul+1 : length] {
		if b != 0x00 {
			t.Fatalf("trailing pad byte %#02x is not zero", b)
		}
	}
}

// sedpConformCheckSequenceValue asserts an octet sequence carries pad count and trims zeros.
func sedpConformCheckSequenceValue(t *testing.T, value []byte) {
	t.Helper()
	if len(value) < 4 {
		t.Fatalf("sequence value too short: %d", len(value))
	}
	count := binary.LittleEndian.Uint32(value[0:4])
	if count%4 != 0 {
		t.Fatalf("sequence count %d is not pad-in-length (mod 4 != 0)", count)
	}
	if int(count) != len(value)-4 {
		t.Fatalf("sequence count %d does not match value %d", count, len(value)-4)
	}
}

// TestSEDPConformStringPadInLength checks CDR string pad length, NUL, and tolerant decode.
func TestSEDPConformStringPadInLength(t *testing.T) {
	t.Parallel()
	idx := sedpConformNewIndex(t, 0x10, sedpConformDomain)
	ep := sedpConformDeclareWriter(t, idx, sedpConformTypeHash)
	raw := sedpConformMustSerialize(t, ep)
	subs := sedpConformSplit(t, raw)
	data := sedpConformFindDATA(t, subs)
	params := sedpConformInlineParams(t, data.body, data.flags)
	topicValue := sedpConformFindParam(t, params, rtps.PIDTopicName)
	sedpConformCheckStringValue(t, topicValue, sedpConformDDSTopic)
	typeValue := sedpConformFindParam(t, params, rtps.PIDTypeName)
	sedpConformCheckStringValue(t, typeValue, sedpConformTypeDDS)

	padParams := sedpConformIdentityParams(ep, sedpConformEncodeStringPad(sedpConformDDSTopic), sedpConformEncodeStringPad(sedpConformTypeDDS), rtps.PIDEndpointGUIDWire)
	padBody := sedpConformBuildBody(sedpConformPubsWriter, 1, padParams)
	padRaw := sedpConformBuildMessage(ep.ID.Prefix, sedpConformFlagLE|sedpConformFlagInlineQoS|sedpConformFlagData, padBody)
	padDecoded := sedpConformMustParse(t, padRaw)
	if padDecoded.DDSTopic != sedpConformDDSTopic {
		t.Fatalf("pad topic = %q, want %q", padDecoded.DDSTopic, sedpConformDDSTopic)
	}

	minParams := sedpConformIdentityParams(ep, sedpConformEncodeStringMinimal(sedpConformDDSTopic), sedpConformEncodeStringMinimal(sedpConformTypeDDS), rtps.PIDEndpointGUIDWire)
	minBody := sedpConformBuildBody(sedpConformPubsWriter, 1, minParams)
	minRaw := sedpConformBuildMessage(ep.ID.Prefix, sedpConformFlagLE|sedpConformFlagInlineQoS|sedpConformFlagData, minBody)
	minDecoded := sedpConformMustParse(t, minRaw)
	if minDecoded.DDSTopic != sedpConformDDSTopic {
		t.Fatalf("minimal topic = %q, want %q", minDecoded.DDSTopic, sedpConformDDSTopic)
	}
	if minDecoded.TypeDDS != sedpConformTypeDDS {
		t.Fatalf("minimal type = %q, want %q", minDecoded.TypeDDS, sedpConformTypeDDS)
	}
	if padDecoded.DDSTopic != minDecoded.DDSTopic {
		t.Fatalf("pad %q vs minimal %q mismatch", padDecoded.DDSTopic, minDecoded.DDSTopic)
	}
}

// TestSEDPConformOctetSequencePadAndTrim checks sequence pad length and trailing zero trim.
func TestSEDPConformOctetSequencePadAndTrim(t *testing.T) {
	t.Parallel()
	idx := sedpConformNewIndex(t, 0x11, sedpConformDomain)
	ep := sedpConformDeclareWriter(t, idx, sedpConformTypeHash)
	raw := sedpConformMustSerialize(t, ep)
	subs := sedpConformSplit(t, raw)
	data := sedpConformFindDATA(t, subs)
	params := sedpConformInlineParams(t, data.body, data.flags)
	userValue := sedpConformFindParam(t, params, rtps.PIDUserData)
	sedpConformCheckSequenceValue(t, userValue)
	count := binary.LittleEndian.Uint32(userValue[0:4])
	trimmed := strings.TrimRight(string(userValue[4:4+count]), "\x00")
	if !strings.Contains(trimmed, "typehash="+sedpConformTypeHash) {
		t.Fatalf("userdata %q misses type hash", trimmed)
	}

	userdata := "typehash=" + sedpConformTypeHash + ";dds_topic=" + sedpConformDDSTopic + ";domain=42"
	padParams := []rtps.Param{
		{ID: rtps.PIDTopicName, Value: sedpConformEncodeStringPad(sedpConformDDSTopic)},
		{ID: rtps.PIDTypeName, Value: sedpConformEncodeStringPad(sedpConformTypeDDS)},
		{ID: rtps.PIDEndpointGUIDWire, Value: sedpConformGUIDValue(ep.ID)},
		{ID: rtps.PIDUserData, Value: sedpConformEncodeSequencePad(userdata)},
	}
	padBody := sedpConformBuildBody(sedpConformPubsWriter, 1, padParams)
	padRaw := sedpConformBuildMessage(ep.ID.Prefix, sedpConformFlagLE|sedpConformFlagInlineQoS|sedpConformFlagData, padBody)
	padDecoded := sedpConformMustParse(t, padRaw)
	if padDecoded.TypeHash != sedpConformTypeHash {
		t.Fatalf("pad hash = %q, want %q", padDecoded.TypeHash, sedpConformTypeHash)
	}

	minParams := []rtps.Param{
		{ID: rtps.PIDTopicName, Value: sedpConformEncodeStringPad(sedpConformDDSTopic)},
		{ID: rtps.PIDTypeName, Value: sedpConformEncodeStringPad(sedpConformTypeDDS)},
		{ID: rtps.PIDEndpointGUIDWire, Value: sedpConformGUIDValue(ep.ID)},
		{ID: rtps.PIDUserData, Value: sedpConformEncodeSequenceMinimal(userdata)},
	}
	minBody := sedpConformBuildBody(sedpConformPubsWriter, 1, minParams)
	minRaw := sedpConformBuildMessage(ep.ID.Prefix, sedpConformFlagLE|sedpConformFlagInlineQoS|sedpConformFlagData, minBody)
	minDecoded := sedpConformMustParse(t, minRaw)
	if minDecoded.TypeHash != sedpConformTypeHash {
		t.Fatalf("minimal hash = %q, want %q", minDecoded.TypeHash, sedpConformTypeHash)
	}
	if padDecoded.TypeHash != minDecoded.TypeHash {
		t.Fatalf("pad hash %q vs minimal %q mismatch", padDecoded.TypeHash, minDecoded.TypeHash)
	}
}

// TestSEDPConformDataPresentAlways checks flags 0x07 with and without QoS.
//
// Endpoint DATA always carries a PL_CDR payload copy of the params so
// CycloneDDS accepts the sample; inline-only endpoint DATA is dropped
// with missing payload. Both forms parse back with QoS intact.
func TestSEDPConformDataPresentAlways(t *testing.T) {
	t.Parallel()
	idx := sedpConformNewIndex(t, 0x12, sedpConformDomain)
	ep := sedpConformDeclareWriter(t, idx, sedpConformTypeHash)
	rawEmpty := sedpConformMustSerialize(t, ep)
	subsEmpty := sedpConformSplit(t, rawEmpty)
	dataEmpty := sedpConformFindDATA(t, subsEmpty)
	wantEmpty := byte(sedpConformFlagLE | sedpConformFlagInlineQoS | sedpConformFlagData)
	if dataEmpty.flags != wantEmpty {
		t.Fatalf("empty QoS flags = %#02x, want %#02x", dataEmpty.flags, wantEmpty)
	}
	payloadEmpty := sedpConformCheckPayloadCopy(t, dataEmpty.body, dataEmpty.flags)
	if len(payloadEmpty) <= 4 {
		t.Fatalf("empty QoS payload len = %d, want list copy beyond encap", len(payloadEmpty))
	}

	withQoS := ep
	withQoS.QoS = []byte{0x01, 0x02, 0x03}
	rawFull, err := dds.SerializeSEDPDATA(withQoS)
	if err != nil {
		t.Fatalf("serialize with QoS: %v", err)
	}
	subsFull := sedpConformSplit(t, rawFull)
	dataFull := sedpConformFindDATA(t, subsFull)
	wantFull := byte(sedpConformFlagLE | sedpConformFlagInlineQoS | sedpConformFlagData)
	if dataFull.flags != wantFull {
		t.Fatalf("full QoS flags = %#02x, want %#02x", dataFull.flags, wantFull)
	}
	payloadFull := sedpConformCheckPayloadCopy(t, dataFull.body, dataFull.flags)
	if !bytes.HasSuffix(payloadFull, withQoS.QoS) {
		t.Fatalf("full QoS payload misses opaque suffix %x in %x", withQoS.QoS, payloadFull)
	}
	decoded, err := dds.ParseSEDPDATA(rawFull)
	if err != nil {
		t.Fatalf("parse with QoS: %v", err)
	}
	if !bytes.Equal(decoded.QoS, withQoS.QoS) {
		t.Fatalf("QoS = %x, want %x", decoded.QoS, withQoS.QoS)
	}
	decodedEmpty, err := dds.ParseSEDPDATA(rawEmpty)
	if err != nil {
		t.Fatalf("parse empty: %v", err)
	}
	if len(decodedEmpty.QoS) != 0 {
		t.Fatalf("empty QoS parsed as %x, want empty", decodedEmpty.QoS)
	}
}

// TestSEDPConformSequenceHighThenLow checks the sequence is high word then low word.
func TestSEDPConformSequenceHighThenLow(t *testing.T) {
	t.Parallel()
	idx := sedpConformNewIndex(t, 0x13, sedpConformDomain)
	ep := sedpConformDeclareWriter(t, idx, sedpConformTypeHash)
	first := sedpConformMustSerialize(t, ep)
	second := sedpConformMustSerialize(t, ep)
	firstData := sedpConformFindDATA(t, sedpConformSplit(t, first))
	secondData := sedpConformFindDATA(t, sedpConformSplit(t, second))
	firstHigh, firstLow := sedpConformSeqOf(t, firstData.body)
	secondHigh, secondLow := sedpConformSeqOf(t, secondData.body)
	firstSeq := uint64(firstHigh)<<32 | uint64(firstLow)
	secondSeq := uint64(secondHigh)<<32 | uint64(secondLow)
	if firstSeq == 0 {
		t.Fatalf("first sequence is zero")
	}
	if secondSeq <= firstSeq {
		t.Fatalf("second sequence %d not above first %d", secondSeq, firstSeq)
	}
	if firstHigh != 0 {
		t.Fatalf("first high = %d, want 0 for small sequence", firstHigh)
	}
	if firstLow == 0 {
		t.Fatalf("first low is zero, want nonzero sequence")
	}
	rawLow := make([]byte, 4)
	binary.LittleEndian.PutUint32(rawLow, firstLow)
	if !bytes.Equal(firstData.body[sedpConformSeqOff+4:sedpConformSeqOff+8], rawLow) {
		t.Fatalf("low word bytes mismatch")
	}
}

// sedpConformHeartbeats returns heartbeat submessages from a split datagram.
func sedpConformHeartbeats(t *testing.T, subs []sedpConformSub) []sedpConformSub {
	t.Helper()
	out := []sedpConformSub{}
	for _, sub := range subs {
		if sub.id == sedpConformSubHeartbeat {
			out = append(out, sub)
		}
	}
	return out
}

// sedpConformCheckHeartbeat validates one heartbeat body against a writer and sequence.
func sedpConformCheckHeartbeat(t *testing.T, hb sedpConformSub, writer [4]byte, seq uint64) int32 {
	t.Helper()
	if len(hb.body) != sedpConformHeartbeatLen {
		t.Fatalf("heartbeat len = %d, want %d", len(hb.body), sedpConformHeartbeatLen)
	}
	var gotWriter [4]byte
	copy(gotWriter[:], hb.body[4:8])
	if gotWriter != writer {
		t.Fatalf("heartbeat writer = %x, want %x", gotWriter, writer)
	}
	firstHigh := binary.LittleEndian.Uint32(hb.body[8:12])
	firstLow := binary.LittleEndian.Uint32(hb.body[12:16])
	lastHigh := binary.LittleEndian.Uint32(hb.body[16:20])
	lastLow := binary.LittleEndian.Uint32(hb.body[20:24])
	first := uint64(firstHigh)<<32 | uint64(firstLow)
	last := uint64(lastHigh)<<32 | uint64(lastLow)
	if first != 1 {
		t.Fatalf("heartbeat first = %d, want 1", first)
	}
	if last != seq {
		t.Fatalf("heartbeat last = %d, want DATA seq %d", last, seq)
	}
	count := int32(binary.LittleEndian.Uint32(hb.body[24:28])) //nolint:gosec // G115: test heartbeat count is non-negative small value
	if count == 0 {
		t.Fatalf("heartbeat count is zero")
	}
	return count
}

// TestSEDPConformHeartbeatsAppended checks heartbeats follow DATA with fresh counts.
func TestSEDPConformHeartbeatsAppended(t *testing.T) {
	t.Parallel()
	idx := sedpConformNewIndex(t, 0x14, sedpConformDomain)
	writer := sedpConformDeclareWriter(t, idx, sedpConformTypeHash)
	first := sedpConformMustSerialize(t, writer)
	second := sedpConformMustSerialize(t, writer)
	firstSubs := sedpConformSplit(t, first)
	secondSubs := sedpConformSplit(t, second)
	firstHBs := sedpConformHeartbeats(t, firstSubs)
	secondHBs := sedpConformHeartbeats(t, secondSubs)
	if len(firstHBs) != 1 {
		t.Fatalf("writer heartbeats = %d, want 1 (Cyclone DATA writer only)", len(firstHBs))
	}
	if len(secondHBs) != 1 {
		t.Fatalf("second heartbeats = %d, want 1", len(secondHBs))
	}
	firstData := sedpConformFindDATA(t, firstSubs)
	firstHigh, firstLow := sedpConformSeqOf(t, firstData.body)
	firstSeq := uint64(firstHigh)<<32 | uint64(firstLow)
	secondData := sedpConformFindDATA(t, secondSubs)
	secondHigh, secondLow := sedpConformSeqOf(t, secondData.body)
	secondSeq := uint64(secondHigh)<<32 | uint64(secondLow)
	count1 := sedpConformCheckHeartbeat(t, firstHBs[0], sedpConformPubsWriterCyclone, firstSeq)
	count2 := sedpConformCheckHeartbeat(t, secondHBs[0], sedpConformPubsWriterCyclone, secondSeq)
	if count2 <= count1 {
		t.Fatalf("cyclone count %d not above %d", count2, count1)
	}

	reader := sedpConformDeclareReader(t, idx, sedpConformTypeHash)
	rawReader := sedpConformMustSerialize(t, reader)
	readerSubs := sedpConformSplit(t, rawReader)
	readerHBs := sedpConformHeartbeats(t, readerSubs)
	if len(readerHBs) != 1 {
		t.Fatalf("reader heartbeats = %d, want 1", len(readerHBs))
	}
	readerData := sedpConformFindDATA(t, readerSubs)
	readerHigh, readerLow := sedpConformSeqOf(t, readerData.body)
	readerSeq := uint64(readerHigh)<<32 | uint64(readerLow)
	sedpConformCheckHeartbeat(t, readerHBs[0], sedpConformSubsWriter, readerSeq)
}

// TestSEDPConformReliableHistoryProtocolVendor checks reliable QoS, history, version, and vendor.
//
//nolint:gocognit,cyclop // conformance test asserts reliable QoS, history and vendor in one run
func TestSEDPConformReliableHistoryProtocolVendor(t *testing.T) {
	t.Parallel()
	idx := sedpConformNewIndex(t, 0x15, sedpConformDomain)
	ep := sedpConformDeclareWriter(t, idx, sedpConformTypeHash)
	raw := sedpConformMustSerialize(t, ep)
	if raw[4] != 2 {
		t.Fatalf("RTPS major = %d, want 2", raw[4])
	}
	if raw[5] != 1 {
		t.Fatalf("RTPS minor = %d, want 1 for protocol 2.1", raw[5])
	}
	if raw[6] != 0x01 || raw[7] != 0x10 {
		t.Fatalf("RTPS vendor = %02x %02x, want 01 10", raw[6], raw[7])
	}
	subs := sedpConformSplit(t, raw)
	data := sedpConformFindDATA(t, subs)
	params := sedpConformInlineParams(t, data.body, data.flags)

	reliability := sedpConformFindParam(t, params, rtps.PIDReliability)
	if len(reliability) < 12 {
		t.Fatalf("reliability len = %d, want at least 12", len(reliability))
	}
	kind := binary.LittleEndian.Uint32(reliability[0:4])
	sec := binary.LittleEndian.Uint32(reliability[4:8])
	frac := binary.LittleEndian.Uint32(reliability[8:12])
	if kind != 2 {
		t.Fatalf("reliability kind = %d, want 2 for reliable", kind)
	}
	if sec != 0x7FFFFFFF {
		t.Fatalf("reliability sec = %#08x, want infinite 0x7FFFFFFF", sec)
	}
	if frac != 0xFFFFFFFF {
		t.Fatalf("reliability frac = %#08x, want infinite 0xFFFFFFFF", frac)
	}

	history := sedpConformFindParam(t, params, rtps.PIDHistory)
	if len(history) < 8 {
		t.Fatalf("history len = %d, want at least 8", len(history))
	}
	historyKind := binary.LittleEndian.Uint32(history[0:4])
	depth := binary.LittleEndian.Uint32(history[4:8])
	if historyKind != 0 {
		t.Fatalf("history kind = %d, want 0 for KEEP_LAST", historyKind)
	}
	if depth != 100 {
		t.Fatalf("history depth = %d, want 100", depth)
	}

	protocol := sedpConformFindParam(t, params, rtps.PIDProtocolVersion)
	if len(protocol) < 2 {
		t.Fatalf("protocol len = %d, want at least 2", len(protocol))
	}
	if protocol[0] != 2 || protocol[1] != 1 {
		t.Fatalf("protocol = %02x %02x, want 02 01 for 2.1", protocol[0], protocol[1])
	}

	vendor := sedpConformFindParam(t, params, rtps.PIDVendorID)
	if len(vendor) < 2 {
		t.Fatalf("vendor len = %d, want at least 2", len(vendor))
	}
	if vendor[0] != 0x01 || vendor[1] != 0x10 {
		t.Fatalf("vendor = %02x %02x, want 01 10", vendor[0], vendor[1])
	}
}

// TestSEDPConformTypeHashChatterDomain42 checks the canonical String identity on domain 42.
//
//nolint:cyclop // conformance test asserts canonical identity across many fields in one run
func TestSEDPConformTypeHashChatterDomain42(t *testing.T) {
	t.Parallel()
	if dds.StringTypeHash != sedpConformTypeHash {
		t.Fatalf("StringTypeHash = %q, want %q", dds.StringTypeHash, sedpConformTypeHash)
	}
	idx := sedpConformNewIndex(t, 0x16, sedpConformDomain)
	writer := sedpConformDeclareWriter(t, idx, sedpConformTypeHash)
	if writer.DDSTopic != sedpConformDDSTopic {
		t.Fatalf("dds topic = %q, want %q", writer.DDSTopic, sedpConformDDSTopic)
	}
	if writer.ROSTopic != sedpConformROSTopic {
		t.Fatalf("ros topic = %q, want %q", writer.ROSTopic, sedpConformROSTopic)
	}
	if writer.TypeDDS != sedpConformTypeDDS {
		t.Fatalf("dds type = %q, want %q", writer.TypeDDS, sedpConformTypeDDS)
	}
	if writer.TypeIDL != sedpConformTypeIDL {
		t.Fatalf("idl = %q, want %q", writer.TypeIDL, sedpConformTypeIDL)
	}
	if writer.TypeHash != sedpConformTypeHash {
		t.Fatalf("hash = %q, want %q", writer.TypeHash, sedpConformTypeHash)
	}
	if writer.DomainID != sedpConformDomain {
		t.Fatalf("domain = %d, want %d", writer.DomainID, sedpConformDomain)
	}
	raw := sedpConformMustSerialize(t, writer)
	decoded := sedpConformMustParse(t, raw)
	if decoded.DDSTopic != sedpConformDDSTopic {
		t.Fatalf("round trip topic = %q, want %q", decoded.DDSTopic, sedpConformDDSTopic)
	}
	if decoded.TypeDDS != sedpConformTypeDDS {
		t.Fatalf("round trip type = %q, want %q", decoded.TypeDDS, sedpConformTypeDDS)
	}
	if decoded.TypeHash != sedpConformTypeHash {
		t.Fatalf("round trip hash = %q, want %q", decoded.TypeHash, sedpConformTypeHash)
	}
	if decoded.DomainID != sedpConformDomain {
		t.Fatalf("round trip domain = %d, want %d", decoded.DomainID, sedpConformDomain)
	}
	if !decoded.ID.Equal(writer.ID) {
		t.Fatalf("round trip GUID mismatch")
	}
	reader := sedpConformDeclareReader(t, idx, sedpConformTypeHash)
	rawReader := sedpConformMustSerialize(t, reader)
	decodedReader := sedpConformMustParse(t, rawReader)
	if decodedReader.TypeHash != sedpConformTypeHash {
		t.Fatalf("reader hash = %q, want %q", decodedReader.TypeHash, sedpConformTypeHash)
	}
}

// TestSEDPConformEndpointGUIDWire checks the endpoint GUID uses PID 0x005A.
func TestSEDPConformEndpointGUIDWire(t *testing.T) {
	t.Parallel()
	idx := sedpConformNewIndex(t, 0x17, sedpConformDomain)
	ep := sedpConformDeclareWriter(t, idx, sedpConformTypeHash)
	raw := sedpConformMustSerialize(t, ep)
	subs := sedpConformSplit(t, raw)
	data := sedpConformFindDATA(t, subs)
	params := sedpConformInlineParams(t, data.body, data.flags)
	if !sedpConformHasParam(params, 0x005A) {
		t.Fatalf("inline QoS misses endpoint GUID wire PID 0x005A")
	}
	guidValue := sedpConformFindParam(t, params, 0x005A)
	if len(guidValue) != 16 {
		t.Fatalf("guid len = %d, want 16", len(guidValue))
	}
	if !bytes.Equal(guidValue[0:12], ep.ID.Prefix[:]) {
		t.Fatalf("guid prefix mismatch")
	}
	if !bytes.Equal(guidValue[12:16], ep.ID.Entity[:]) {
		t.Fatalf("guid entity mismatch")
	}

	legacyParams := sedpConformIdentityParams(ep, sedpConformEncodeStringPad(sedpConformDDSTopic), sedpConformEncodeStringPad(sedpConformTypeDDS), rtps.PIDEndpointGUID)
	legacyBody := sedpConformBuildBody(sedpConformPubsWriter, 9, legacyParams)
	legacyRaw := sedpConformBuildMessage(ep.ID.Prefix, sedpConformFlagLE|sedpConformFlagInlineQoS|sedpConformFlagData, legacyBody)
	legacyDecoded := sedpConformMustParse(t, legacyRaw)
	if !legacyDecoded.ID.Equal(ep.ID) {
		t.Fatalf("legacy GUID PID 0x005B GUID mismatch")
	}
}

// TestSEDPConformBuiltinWriters checks pubs, Cyclone pubs, and subs writer entities.
func TestSEDPConformBuiltinWriters(t *testing.T) {
	t.Parallel()
	idx := sedpConformNewIndex(t, 0x18, sedpConformDomain)
	writer := sedpConformDeclareWriter(t, idx, sedpConformTypeHash)
	reader := sedpConformDeclareReader(t, idx, sedpConformTypeHash)
	rawWriter := sedpConformMustSerialize(t, writer)
	dataWriter := sedpConformFindDATA(t, sedpConformSplit(t, rawWriter))
	gotPubs := sedpConformWriterOf(t, dataWriter.body)
	if gotPubs != sedpConformPubsWriterCyclone {
		t.Fatalf("pubs writer = %x, want %x", gotPubs, sedpConformPubsWriterCyclone)
	}
	rawReader := sedpConformMustSerialize(t, reader)
	dataReader := sedpConformFindDATA(t, sedpConformSplit(t, rawReader))
	gotSubs := sedpConformWriterOf(t, dataReader.body)
	if gotSubs != sedpConformSubsWriter {
		t.Fatalf("subs writer = %x, want %x", gotSubs, sedpConformSubsWriter)
	}

	cycloneParams := sedpConformIdentityParams(writer, sedpConformEncodeStringPad(sedpConformDDSTopic), sedpConformEncodeStringPad(sedpConformTypeDDS), rtps.PIDEndpointGUIDWire)
	cycloneBody := sedpConformBuildBody(sedpConformPubsWriterCyclone, 7, cycloneParams)
	cycloneRaw := sedpConformBuildMessage(writer.ID.Prefix, sedpConformFlagLE|sedpConformFlagInlineQoS|sedpConformFlagData, cycloneBody)
	cycloneDecoded := sedpConformMustParse(t, cycloneRaw)
	if cycloneDecoded.IsReader {
		t.Fatalf("Cyclone pubs writer parsed as reader")
	}
	if !cycloneDecoded.ID.Equal(writer.ID) {
		t.Fatalf("Cyclone GUID mismatch")
	}

	subsParams := sedpConformIdentityParams(reader, sedpConformEncodeStringPad(sedpConformDDSTopic), sedpConformEncodeStringPad(sedpConformTypeDDS), rtps.PIDEndpointGUIDWire)
	subsBody := sedpConformBuildBody(sedpConformSubsWriter, 8, subsParams)
	subsRaw := sedpConformBuildMessage(reader.ID.Prefix, sedpConformFlagLE|sedpConformFlagInlineQoS|sedpConformFlagData, subsBody)
	subsDecoded := sedpConformMustParse(t, subsRaw)
	if !subsDecoded.IsReader {
		t.Fatalf("subscriptions writer parsed as writer")
	}
}

// sedpConformQueueReceiver replays queued samples through the SEDP receiver.
type sedpConformQueueReceiver struct {
	queue [][]byte
}

// ReceiveSEDP returns the next queued sample or an empty poll slice.
func (r *sedpConformQueueReceiver) ReceiveSEDP(ctx context.Context) ([]byte, error) {
	err := ctx.Err()
	if err != nil {
		return nil, fmt.Errorf("conform receiver context done: %w", err)
	}
	if len(r.queue) == 0 {
		return []byte{}, nil
	}
	next := r.queue[0]
	r.queue = r.queue[1:]
	return append([]byte(nil), next...), nil
}

// sedpConformWaitFor polls cond until true or the timeout expires.
//
// It yields the scheduler instead of sleeping so unit builds stay free of
// timer-based flakes.
func sedpConformWaitFor(t *testing.T, timeout time.Duration, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		ok := cond()
		if ok {
			return
		}
		runtime.Gosched()
	}
	ok := cond()
	if !ok {
		t.Fatalf("timeout waiting: %s", msg)
	}
}

// TestSEDPConformDomainStamping checks ListenLoop stamps domain zero to the index domain.
//
//nolint:cyclop // domain stamping test asserts zero plus explicit domains across two indexes
func TestSEDPConformDomainStamping(t *testing.T) {
	t.Parallel()
	peer := sedpConformNewIndex(t, 0x19, 0)
	writerZero := sedpConformDeclareWriter(t, peer, sedpConformTypeHash)
	rawZero := sedpConformMustSerialize(t, writerZero)
	parsedZero, err := dds.ParseSEDPDATA(rawZero)
	if err != nil {
		t.Fatalf("parse zero domain: %v", err)
	}
	if parsedZero.DomainID != 0 {
		t.Fatalf("zero sample domain = %d, want 0 before stamping", parsedZero.DomainID)
	}

	local := sedpConformNewIndex(t, 0x1A, sedpConformDomain)
	rx := &sedpConformQueueReceiver{queue: [][]byte{rawZero}}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errCh := make(chan error, 1)
	go func() {
		errCh <- local.ListenLoop(ctx, rx)
	}()
	sedpConformWaitFor(t, 2*time.Second, func() bool {
		return local.Generation() == 1
	}, "listen stamps zero domain")
	remotes := local.RemoteEndpoints()
	if len(remotes) != 1 {
		t.Fatalf("remotes = %d, want 1", len(remotes))
	}
	if remotes[0].DomainID != sedpConformDomain {
		t.Fatalf("stamped domain = %d, want %d", remotes[0].DomainID, sedpConformDomain)
	}
	cancel()
	err = <-errCh
	if err != nil {
		ctxErr := context.Canceled
		if !errors.Is(err, ctxErr) {
			t.Fatalf("listen loop err = %v, want context.Canceled", err)
		}
	}

	explicitPeer := sedpConformNewIndex(t, 0x1B, sedpConformDomain)
	explicitWriter := sedpConformDeclareWriter(t, explicitPeer, sedpConformTypeHash)
	rawExplicit := sedpConformMustSerialize(t, explicitWriter)
	parsedExplicit, err := dds.ParseSEDPDATA(rawExplicit)
	if err != nil {
		t.Fatalf("parse explicit: %v", err)
	}
	if parsedExplicit.DomainID != sedpConformDomain {
		t.Fatalf("explicit domain = %d, want %d", parsedExplicit.DomainID, sedpConformDomain)
	}
	localSecond := sedpConformNewIndex(t, 0x1C, sedpConformDomain)
	rxSecond := &sedpConformQueueReceiver{queue: [][]byte{rawExplicit}}
	ctxSecond, cancelSecond := context.WithCancel(context.Background())
	defer cancelSecond()
	errChSecond := make(chan error, 1)
	go func() {
		errChSecond <- localSecond.ListenLoop(ctxSecond, rxSecond)
	}()
	sedpConformWaitFor(t, 2*time.Second, func() bool {
		return localSecond.Generation() == 1
	}, "listen keeps explicit domain")
	remotesSecond := localSecond.RemoteEndpoints()
	if len(remotesSecond) != 1 {
		t.Fatalf("second remotes = %d, want 1", len(remotesSecond))
	}
	if remotesSecond[0].DomainID != sedpConformDomain {
		t.Fatalf("explicit kept domain = %d, want %d", remotesSecond[0].DomainID, sedpConformDomain)
	}
	cancelSecond()
	err = <-errChSecond
	if err != nil {
		ctxErr := context.Canceled
		if !errors.Is(err, ctxErr) {
			t.Fatalf("second listen err = %v, want context.Canceled", err)
		}
	}
}

// TestSEDPConformUserMatchingExact checks matching needs exact topic, type, and hash.
func TestSEDPConformUserMatchingExact(t *testing.T) {
	t.Parallel()
	reader := dds.SEDPEndpoint{
		ROSTopic: sedpConformROSTopic,
		DDSTopic: sedpConformDDSTopic,
		Kind:     dds.TopicKindTopic,
		TypeDDS:  sedpConformTypeDDS,
		TypeIDL:  sedpConformTypeIDL,
		TypeHash: sedpConformTypeHash,
		DomainID: sedpConformDomain,
		IsReader: true,
	}
	writer := dds.SEDPEndpoint{
		ROSTopic: sedpConformROSTopic,
		DDSTopic: sedpConformDDSTopic,
		Kind:     dds.TopicKindTopic,
		TypeDDS:  sedpConformTypeDDS,
		TypeIDL:  sedpConformTypeIDL,
		TypeHash: sedpConformTypeHash,
		DomainID: sedpConformDomain,
		IsReader: false,
	}
	withHash := func(ep dds.SEDPEndpoint, hash string) dds.SEDPEndpoint {
		ep.TypeHash = hash
		return ep
	}
	withROS := func(ep dds.SEDPEndpoint, topic string) dds.SEDPEndpoint {
		ep.ROSTopic = topic
		return ep
	}
	withDDS := func(ep dds.SEDPEndpoint, topic string) dds.SEDPEndpoint {
		ep.DDSTopic = topic
		return ep
	}
	withType := func(ep dds.SEDPEndpoint, dtype string) dds.SEDPEndpoint {
		ep.TypeDDS = dtype
		return ep
	}
	withDomain := func(ep dds.SEDPEndpoint, domain uint32) dds.SEDPEndpoint {
		ep.DomainID = domain
		return ep
	}
	cases := []struct {
		name   string
		reader dds.SEDPEndpoint
		writer dds.SEDPEndpoint
		want   bool
	}{
		{name: "match", reader: reader, writer: writer, want: true},
		{name: "hash mismatch", reader: reader, writer: withHash(writer, "RIHS01_other"), want: false},
		{name: "empty hash matches empty", reader: withHash(reader, ""), writer: withHash(writer, ""), want: true},
		{name: "empty versus set hash", reader: withHash(reader, ""), writer: writer, want: true},
		{name: "ros topic mismatch", reader: reader, writer: withROS(writer, "/other"), want: false},
		{name: "dds topic mismatch", reader: reader, writer: withDDS(writer, "rt/other"), want: false},
		{name: "type mismatch", reader: reader, writer: withType(writer, "std_msgs::msg::dds_::Bool_"), want: false},
		{name: "domain mismatch", reader: reader, writer: withDomain(writer, 0), want: false},
		{name: "remote reader no match", reader: reader, writer: func() dds.SEDPEndpoint {
			out := writer
			out.IsReader = true
			return out
		}(), want: false},
		{name: "local writer no match", reader: func() dds.SEDPEndpoint {
			out := reader
			out.IsReader = false
			return out
		}(), writer: writer, want: false},
	}
	for _, tc := range cases {
		got := dds.SEDPEndpointsMatch(tc.reader, tc.writer)
		if got != tc.want {
			t.Errorf("%s: match = %v, want %v", tc.name, got, tc.want)
		}
	}

	local := sedpConformNewIndex(t, 0x1D, sedpConformDomain)
	localReader, err := local.DeclareReader(sedpConformROSTopic, sedpConformTypeIDL, sedpConformTypeHash, nil)
	if err != nil {
		t.Fatalf("declare local reader: %v", err)
	}
	peer := sedpConformNewIndex(t, 0x1E, sedpConformDomain)
	good, err := peer.DeclareWriter(sedpConformROSTopic, sedpConformTypeIDL, sedpConformTypeHash, nil)
	if err != nil {
		t.Fatalf("declare good writer: %v", err)
	}
	badHash := good
	badHash.TypeHash = "RIHS01_other"
	badHash.ID.Entity[2]++
	err = local.UpsertRemote(good)
	if err != nil {
		t.Fatalf("upsert good: %v", err)
	}
	err = local.UpsertRemote(badHash)
	if err != nil {
		t.Fatalf("upsert bad hash: %v", err)
	}
	matched, err := local.MatchingWriters(localReader.ID)
	if err != nil {
		t.Fatalf("matching writers: %v", err)
	}
	if len(matched) != 1 {
		t.Fatalf("matched = %d, want 1 exact match", len(matched))
	}
	if !matched[0].ID.Equal(good.ID) {
		t.Fatalf("matched wrong writer")
	}
}

// TestSEDPConformBuiltinQosRoundTrip checks 0x0077 passthrough.
func TestSEDPConformBuiltinQosRoundTrip(t *testing.T) {
	t.Parallel()
	idx := sedpConformNewIndex(t, 0x20, sedpConformDomain)
	ep, err := idx.DeclareWriter(sedpConformROSTopic, sedpConformTypeIDL, sedpConformTypeHash, nil)
	if err != nil {
		t.Fatalf("declare writer: %v", err)
	}
	ep.BuiltinEndpointQos = 0x3F
	raw := sedpConformMustSerialize(t, ep)
	decoded := sedpConformMustParse(t, raw)
	if decoded.BuiltinEndpointQos != 0x3F {
		t.Fatalf("builtin QoS = %#x, want 0x3F", decoded.BuiltinEndpointQos)
	}
	subs := sedpConformSplit(t, raw)
	data := sedpConformFindDATA(t, subs)
	params := sedpConformInlineParams(t, data.body, data.flags)
	qosValue := sedpConformFindParam(t, params, 0x0077)
	if len(qosValue) != 4 {
		t.Fatalf("builtin QoS len = %d, want 4", len(qosValue))
	}
}

// TestSEDPConformTypeObjectStub checks XTypes TypeObject passthrough.
func TestSEDPConformTypeObjectStub(t *testing.T) {
	t.Parallel()
	idx := sedpConformNewIndex(t, 0x21, sedpConformDomain)
	ep, err := idx.DeclareWriter(sedpConformROSTopic, sedpConformTypeIDL, sedpConformTypeHash, nil)
	if err != nil {
		t.Fatalf("declare writer: %v", err)
	}
	ep.TypeObject = []byte{0x01, 0x02, 0x03, 0x04}
	raw := sedpConformMustSerialize(t, ep)
	decoded := sedpConformMustParse(t, raw)
	if !bytes.Equal(decoded.TypeObject, ep.TypeObject) {
		t.Fatalf("type object = % X, want % X", decoded.TypeObject, ep.TypeObject)
	}
	if decoded.DDSTopic != sedpConformDDSTopic {
		t.Fatalf("topic = %q", decoded.DDSTopic)
	}
}

// TestSEDPConformVendorTolerated checks vendor 0x8003 and 0x800c do not break parsing.
func TestSEDPConformVendorTolerated(t *testing.T) {
	t.Parallel()
	idx := sedpConformNewIndex(t, 0x22, sedpConformDomain)
	ep, err := idx.DeclareWriter(sedpConformROSTopic, sedpConformTypeIDL, sedpConformTypeHash, nil)
	if err != nil {
		t.Fatalf("declare writer: %v", err)
	}
	params := []rtps.Param{
		{ID: rtps.PIDTopicName, Value: sedpConformEncodeStringPad(sedpConformDDSTopic)},
		{ID: rtps.PIDTypeName, Value: sedpConformEncodeStringPad(sedpConformTypeDDS)},
		{ID: rtps.PIDEndpointGUIDWire, Value: sedpConformGUIDValue(ep.ID)},
		{ID: 0x8003, Value: []byte{0x00, 0x00, 0x00, 0x00}},
		{ID: 0x800c, Value: []byte{0x01, 0x00, 0x00, 0x00}},
	}
	body := sedpConformBuildBody(sedpConformPubsWriter, 1, params)
	raw := sedpConformBuildMessage(ep.ID.Prefix, sedpConformFlagLE|sedpConformFlagInlineQoS|sedpConformFlagData, body)
	decoded := sedpConformMustParse(t, raw)
	if decoded.DDSTopic != sedpConformDDSTopic {
		t.Fatalf("topic = %q, want %q", decoded.DDSTopic, sedpConformDDSTopic)
	}
	back, _, err := rtps.DecodeParamsEndian(rtps.EncodeParams(params), true)
	if err != nil {
		t.Fatalf("vendor round trip: %v", err)
	}
	if len(back) != len(params) {
		t.Fatalf("vendor params = %d, want %d", len(back), len(params))
	}
}

// TestSEDPConformPadSkipped checks PIDPad never surfaces.
func TestSEDPConformPadSkipped(t *testing.T) {
	t.Parallel()
	blob := []byte{
		0x00, 0x00, 0x00, 0x00,
		0x05, 0x00, 0x02, 0x00, 0x41, 0x42, 0x00, 0x00,
		0x01, 0x00, 0x00, 0x00,
	}
	back, _, err := rtps.DecodeParamsEndian(blob, true)
	if err != nil {
		t.Fatalf("decode pad: %v", err)
	}
	if len(back) != 1 || back[0].ID != rtps.PIDTopicName {
		t.Fatalf("pad decode = %+v, want single topic", back)
	}
}
