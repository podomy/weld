// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

package rtps_test

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/podomy/weld/internal/transports/dds/rtps"
)

// testData returns a DATA sample with an XCDR1 little endian payload.
func testData() rtps.Data {
	return rtps.Data{
		ReaderID:       rtps.EntityUnknown,
		WriterID:       rtps.EntitySPDPWriter,
		SequenceNumber: 1,
		Payload:        []byte{0x00, 0x01, 0x00, 0x00, 0x41},
	}
}

// TestDataGoldenLE pins the exact DATA submessage bytes in little endian.
func TestDataGoldenLE(t *testing.T) {
	t.Parallel()

	sub, err := testData().Submessage(true)
	if err != nil {
		t.Fatalf("submessage: %v", err)
	}
	raw, err := rtps.WrapSubmessage(sub.ID, sub.Flags, sub.Body)
	if err != nil {
		t.Fatalf("wrap: %v", err)
	}
	want := []byte{
		0x15, 0x05, 0x19, 0x00,
		0x00, 0x00, 0x10, 0x00,
		0x00, 0x00, 0x00, 0x00,
		0x00, 0x01, 0x00, 0xC2,
		0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00,
		0x00, 0x01, 0x00, 0x00, 0x41,
	}
	if !bytes.Equal(raw, want) {
		t.Fatalf("DATA LE = % X, want % X", raw, want)
	}
}

// TestDataGoldenBE pins the exact DATA submessage bytes in big endian.
// Payload stays opaque caller bytes, here an XCDR1 big endian sample.
func TestDataGoldenBE(t *testing.T) {
	t.Parallel()

	data := testData()
	data.Payload = []byte{0x00, 0x00, 0x00, 0x00, 0x41}
	sub, err := data.Submessage(false)
	if err != nil {
		t.Fatalf("submessage: %v", err)
	}
	raw, err := rtps.WrapSubmessage(sub.ID, sub.Flags, sub.Body)
	if err != nil {
		t.Fatalf("wrap: %v", err)
	}
	want := []byte{
		0x15, 0x04, 0x00, 0x19,
		0x00, 0x00, 0x00, 0x10,
		0x00, 0x00, 0x00, 0x00,
		0x00, 0x01, 0x00, 0xC2,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01,
		0x00, 0x00, 0x00, 0x00, 0x41,
	}
	if !bytes.Equal(raw, want) {
		t.Fatalf("DATA BE = % X, want % X", raw, want)
	}
}

// checkDataRoundTrip encodes and decodes one Data value and compares.
func checkDataRoundTrip(t *testing.T, want rtps.Data, littleEndian bool) {
	t.Helper()

	framed := encodeDataFramed(t, want, littleEndian)
	got := decodeDataOnly(t, framed)
	compareDataFields(t, got, want)
	compareDataQoS(t, got, want)
}

// encodeDataFramed renders want as one framed submessage on a header.
func encodeDataFramed(t *testing.T, want rtps.Data, littleEndian bool) []byte {
	t.Helper()

	sub, err := want.Submessage(littleEndian)
	if err != nil {
		t.Fatalf("submessage: %v", err)
	}
	framed, err := rtps.WrapSubmessage(sub.ID, sub.Flags, sub.Body)
	if err != nil {
		t.Fatalf("wrap: %v", err)
	}
	return appendMessage(t, framed)
}

// decodeDataOnly decodes one framed message and returns its DATA part.
func decodeDataOnly(t *testing.T, framed []byte) rtps.Data {
	t.Helper()

	var drops rtps.Counter
	_, raws, err := rtps.DecodeMessage(framed, &drops)
	if err != nil {
		t.Fatalf("decode message: %v", err)
	}
	if len(raws) != 1 {
		t.Fatalf("submessages = %d, want 1", len(raws))
	}
	decoded, err := rtps.DecodeSubmessage(raws[0], &drops)
	if err != nil {
		t.Fatalf("decode submessage: %v", err)
	}
	if decoded.Data == nil {
		t.Fatalf("data is nil")
	}
	if drops.Count() != 0 {
		t.Fatalf("drops = %d, want 0", drops.Count())
	}
	return *decoded.Data
}

// compareDataFields checks routing metadata and payload bytes.
func compareDataFields(t *testing.T, got, want rtps.Data) {
	t.Helper()

	if got.ReaderID != want.ReaderID || got.WriterID != want.WriterID {
		t.Fatalf("ids = % X % X", got.ReaderID, got.WriterID)
	}
	if got.SequenceNumber != want.SequenceNumber {
		t.Fatalf("sequence = %d, want %d", got.SequenceNumber, want.SequenceNumber)
	}
	if got.Key != want.Key {
		t.Fatalf("key = %v, want %v", got.Key, want.Key)
	}
	if !bytes.Equal(got.Payload, want.Payload) {
		t.Fatalf("payload = % X, want % X", got.Payload, want.Payload)
	}
}

// compareDataQoS checks inline QoS parameters entry by entry.
func compareDataQoS(t *testing.T, got, want rtps.Data) {
	t.Helper()

	if len(got.InlineQoS) != len(want.InlineQoS) {
		t.Fatalf("qos len = %d, want %d", len(got.InlineQoS), len(want.InlineQoS))
	}
	for i := range want.InlineQoS {
		if got.InlineQoS[i].ID != want.InlineQoS[i].ID {
			t.Fatalf("qos %d id = %#04x, want %#04x", i, got.InlineQoS[i].ID, want.InlineQoS[i].ID)
		}
		if !bytes.Equal(got.InlineQoS[i].Value, want.InlineQoS[i].Value) {
			t.Fatalf("qos %d value = % X", i, got.InlineQoS[i].Value)
		}
	}
}

// appendMessage prefixes a header before one framed submessage.
func appendMessage(t *testing.T, framed []byte) []byte {
	t.Helper()

	raw := rtps.DefaultHeader(testPrefix()).Encode()
	out := make([]byte, 0, len(raw)+len(framed))
	out = append(out, raw[:]...)
	out = append(out, framed...)
	return out
}

// TestDataRoundTrip checks DATA in both endians with QoS and key forms.
func TestDataRoundTrip(t *testing.T) {
	t.Parallel()

	plain := testData()
	keyed := testData()
	keyed.Key = true
	keyed.Payload = []byte{0x00, 0x00, 0x00, 0x00, 0xDE, 0xAD}
	withQoS := testData()
	withQoS.SequenceNumber = 0x0102030405060708
	withQoS.InlineQoS = []rtps.Parameter{
		{ID: 0x0050, Value: []byte{0xAA, 0xBB, 0xCC}},
		{ID: 0x0051, Value: []byte{0x01}},
	}

	for _, littleEndian := range []bool{true, false} {
		checkDataRoundTrip(t, plain, littleEndian)
		checkDataRoundTrip(t, keyed, littleEndian)
		checkDataRoundTrip(t, withQoS, littleEndian)
	}
}

// TestDataInlineQoSPadding checks odd values pad to 4 bytes with zeros.
func TestDataInlineQoSPadding(t *testing.T) {
	t.Parallel()

	data := testData()
	data.InlineQoS = []rtps.Parameter{{ID: 0x0050, Value: []byte{0xAA, 0xBB, 0xCC}}}
	body, err := data.Body(true)
	if err != nil {
		t.Fatalf("body: %v", err)
	}
	// Fixed 20 plus param 2+2+3+1 pad plus sentinel 4.
	if len(body) != 20+8+4+len(data.Payload) {
		t.Fatalf("body len = %d", len(body))
	}
	if body[20+4+3] != 0x00 {
		t.Fatalf("pad byte = %#02x, want zero", body[20+4+3])
	}
}

// TestDataDecodeErrors checks contradictory flags and truncated content.
func TestDataDecodeErrors(t *testing.T) {
	t.Parallel()

	var drops rtps.Counter
	if _, err := rtps.DecodeData(make([]byte, 19), 0x05, &drops); err == nil {
		t.Fatalf("short data succeeded, want error")
	}
	body, err := testData().Body(true)
	if err != nil {
		t.Fatalf("body: %v", err)
	}
	if _, err := rtps.DecodeData(body, 0x05|rtps.DataFlagKeyPresent, &drops); err == nil {
		t.Fatalf("data plus key succeeded, want error")
	}

	badInline, err := badInlineBody(t)
	if err != nil {
		t.Fatalf("bad inline body: %v", err)
	}
	if _, err := rtps.DecodeData(badInline, 0x05|rtps.DataFlagInlineQoS, &drops); err == nil {
		t.Fatalf("unterminated inline qos succeeded, want error")
	}
	if drops.Count() != 3 {
		t.Fatalf("drops = %d, want 3", drops.Count())
	}
}

// badInlineBody returns a DATA body whose trailing bytes cannot parse as
// a parameter list, for the unterminated inline QoS case.
func badInlineBody(t *testing.T) ([]byte, error) {
	t.Helper()

	data := testData()
	body, err := data.Body(true)
	if err != nil {
		return nil, fmt.Errorf("rtps test: bad inline body: %w", err)
	}
	// Inline QoS flag set but body carries no parameter list.
	return body, nil
}

// TestInfoTS checks valid and invalidated timestamps in both endians.
func TestInfoTS(t *testing.T) {
	t.Parallel()

	stamp := rtps.InfoTS{Valid: true, Seconds: 0x11223344, Fraction: 0xAABBCCDD}
	le := stamp.Encode(true)
	if want := []byte{0x44, 0x33, 0x22, 0x11, 0xDD, 0xCC, 0xBB, 0xAA}; !bytes.Equal(le, want) {
		t.Fatalf("info_ts LE = % X, want % X", le, want)
	}
	be := stamp.Encode(false)
	if want := []byte{0x11, 0x22, 0x33, 0x44, 0xAA, 0xBB, 0xCC, 0xDD}; !bytes.Equal(be, want) {
		t.Fatalf("info_ts BE = % X, want % X", be, want)
	}

	var drops rtps.Counter
	back, err := rtps.DecodeInfoTS(le, stamp.Flags(true), &drops)
	if err != nil {
		t.Fatalf("decode LE: %v", err)
	}
	if back != stamp {
		t.Fatalf("round trip = %+v, want %+v", back, stamp)
	}
	back, err = rtps.DecodeInfoTS(be, stamp.Flags(false), &drops)
	if err != nil {
		t.Fatalf("decode BE: %v", err)
	}
	if back != stamp {
		t.Fatalf("round trip = %+v, want %+v", back, stamp)
	}

	empty := rtps.InfoTS{}
	if got := empty.Encode(true); got != nil {
		t.Fatalf("invalid encode = % X, want nil", got)
	}
	back, err = rtps.DecodeInfoTS([]byte{0xFF}, empty.Flags(true), &drops)
	if err != nil {
		t.Fatalf("decode invalid: %v", err)
	}
	if back.Valid {
		t.Fatalf("invalid decodes as valid")
	}

	if _, err := rtps.DecodeInfoTS([]byte{0x01, 0x02}, 0x01, &drops); err == nil {
		t.Fatalf("short info_ts succeeded, want error")
	}
	if drops.Count() != 1 {
		t.Fatalf("drops = %d, want 1", drops.Count())
	}
}

// TestHeartbeat checks golden bytes, round trip, and tolerant trailing.
func TestHeartbeat(t *testing.T) {
	t.Parallel()

	beat := rtps.Heartbeat{
		ReaderID:      rtps.EntityUnknown,
		WriterID:      rtps.EntitySPDPWriter,
		FirstSequence: 1,
		LastSequence:  5,
		Count:         9,
	}
	got := beat.Encode(true)
	want := []byte{
		0x00, 0x00, 0x00, 0x00,
		0x00, 0x01, 0x00, 0xC2,
		0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x05, 0x00, 0x00, 0x00,
		0x09, 0x00, 0x00, 0x00,
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("heartbeat = % X, want % X", got, want)
	}

	var drops rtps.Counter
	back, err := rtps.DecodeHeartbeat(got, beat.Flags(true, true, false), &drops)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if back != beat {
		t.Fatalf("round trip = %+v, want %+v", back, beat)
	}
	back, err = rtps.DecodeHeartbeat(beat.Encode(false), beat.Flags(false, false, true), &drops)
	if err != nil {
		t.Fatalf("decode BE: %v", err)
	}
	if back != beat {
		t.Fatalf("round trip BE = %+v, want %+v", back, beat)
	}
	if _, err := rtps.DecodeHeartbeat(got[:27], 0x01, &drops); err == nil {
		t.Fatalf("short heartbeat succeeded, want error")
	}
	if _, err := rtps.DecodeHeartbeat(append(got, 0xFF, 0xFF), 0x01, &drops); err == nil {
		t.Fatalf("trailing heartbeat succeeded, want error")
	}
	if drops.Count() != 2 {
		t.Fatalf("drops = %d, want 2", drops.Count())
	}
}

// TestHeartbeatTrailingExact pins the fixed 28 byte width per 8.3.8.6.
func TestHeartbeatTrailingExact(t *testing.T) {
	t.Parallel()

	beat := rtps.Heartbeat{
		ReaderID:      rtps.EntityUnknown,
		WriterID:      rtps.EntitySPDPWriter,
		FirstSequence: 1,
		LastSequence:  5,
		Count:         9,
	}
	got := beat.Encode(true)
	var drops rtps.Counter
	if _, err := rtps.DecodeHeartbeat(append(got, 0x00), 0x01, &drops); err == nil {
		t.Fatalf("trailing heartbeat succeeded, want error")
	}
	if drops.Count() != 1 {
		t.Fatalf("drops = %d, want 1", drops.Count())
	}
}

// TestAckNackEncode checks receipt bytes and bitmap validation.
func TestAckNackEncode(t *testing.T) {
	t.Parallel()

	receipt := rtps.AckNack{
		ReaderID: rtps.EntityUnknown,
		WriterID: rtps.EntitySPDPWriter,
		Base:     1,
		NumBits:  0,
		Count:    7,
	}
	got, err := receipt.Encode(true)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	want := []byte{
		0x00, 0x00, 0x00, 0x00,
		0x00, 0x01, 0x00, 0xC2,
		0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00,
		0x07, 0x00, 0x00, 0x00,
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("acknack = % X, want % X", got, want)
	}

	bad := receipt
	bad.NumBits = 8
	if _, err := bad.Encode(true); err == nil {
		t.Fatalf("bitmap mismatch succeeded, want error")
	}

	withBits := receipt
	withBits.NumBits = 8
	withBits.Bitmap = []uint32{0xA5000000}
	withBits.Count = -3
	be, err := withBits.Encode(false)
	if err != nil {
		t.Fatalf("encode bits BE: %v", err)
	}
	if len(be) != 28 {
		t.Fatalf("bits len = %d, want 28", len(be))
	}
	// Reader 4 plus writer 4 plus base 8 plus numbits 4 puts the word at 20.
	if be[20] != 0xA5 {
		t.Fatalf("bitmap byte = %#02x, want 0xa5", be[20])
	}
}

// TestGapDecode checks gap start parsing with the list skipped.
func TestGapDecode(t *testing.T) {
	t.Parallel()

	body := []byte{
		0x00, 0x00, 0x00, 0x00,
		0x00, 0x01, 0x00, 0xC2,
		0x00, 0x00, 0x00, 0x00, 0x0A, 0x00, 0x00, 0x00,
		// Skipped sequence set: base 8 plus numbits 4 plus one word.
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0B,
		0x01, 0x00, 0x00, 0x00,
		0xFF, 0xFF, 0xFF, 0xFF,
	}
	var drops rtps.Counter
	gap, err := rtps.DecodeGap(body, 0x01, &drops)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if gap.GapStart != 10 {
		t.Fatalf("gap start = %d, want 10", gap.GapStart)
	}
	if gap.WriterID != rtps.EntitySPDPWriter {
		t.Fatalf("writer = % X", gap.WriterID)
	}
	if _, err := rtps.DecodeGap(body[:15], 0x01, &drops); err == nil {
		t.Fatalf("short gap succeeded, want error")
	}
	if drops.Count() != 1 {
		t.Fatalf("drops = %d, want 1", drops.Count())
	}
}

// TestPadEncode checks zero alignment bytes and range validation.
func TestPadEncode(t *testing.T) {
	t.Parallel()

	got, err := rtps.EncodePad(8)
	if err != nil {
		t.Fatalf("encode pad: %v", err)
	}
	if !bytes.Equal(got, make([]byte, 8)) {
		t.Fatalf("pad = % X, want zeros", got)
	}
	if _, err := rtps.EncodePad(-1); err == nil {
		t.Fatalf("negative pad succeeded, want error")
	}
	if _, err := rtps.EncodePad(0x10000); err == nil {
		t.Fatalf("oversize pad succeeded, want error")
	}
}

// TestDecodeSubmessageDispatcher checks typed kinds plus skipped content.
func TestDecodeSubmessageDispatcher(t *testing.T) {
	t.Parallel()

	msg := buildDispatchMessage(t)

	var drops rtps.Counter
	_, raws, err := rtps.DecodeMessage(msg, &drops)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(raws) != 5 {
		t.Fatalf("submessages = %d, want 5", len(raws))
	}
	decoded := make([]rtps.Submessage, 0, len(raws))
	for _, raw := range raws {
		sub, err := rtps.DecodeSubmessage(raw, &drops)
		if err != nil {
			t.Fatalf("dispatch %#02x: %v", raw.ID, err)
		}
		decoded = append(decoded, sub)
	}
	checkDispatchFields(t, decoded)
	if drops.Count() != 0 {
		t.Fatalf("drops = %d, want 0", drops.Count())
	}
}

// buildDispatchMessage frames DATA plus unknown plus INFO_TS plus PAD
// plus ACKNACK in one datagram.
func buildDispatchMessage(t *testing.T) []byte {
	t.Helper()

	dataSub, err := testData().Submessage(true)
	if err != nil {
		t.Fatalf("data: %v", err)
	}
	stamp := rtps.InfoTS{Valid: true, Seconds: 3, Fraction: 4}
	pad, err := rtps.EncodePad(4)
	if err != nil {
		t.Fatalf("pad: %v", err)
	}
	receipt := rtps.AckNack{WriterID: rtps.EntitySPDPWriter, Base: 2, Count: 1}
	receiptBody, err := receipt.Encode(true)
	if err != nil {
		t.Fatalf("acknack: %v", err)
	}
	msg, err := rtps.EncodeMessage(rtps.DefaultHeader(testPrefix()), []rtps.RawSubmessage{
		dataSub,
		{ID: 0xFF, Flags: 0x01, Body: []byte{0xDE, 0xAD, 0xBE, 0xEF}},
		{ID: rtps.SubmessageInfoTS, Flags: stamp.Flags(true), Body: stamp.Encode(true)},
		{ID: rtps.SubmessagePad, Flags: 0x01, Body: pad},
		{ID: rtps.SubmessageAckNack, Flags: 0x05, Body: receiptBody},
	})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	return msg
}

// checkDispatchFields verifies each dispatched submessage by position.
func checkDispatchFields(t *testing.T, decoded []rtps.Submessage) {
	t.Helper()

	if decoded[0].Data == nil || decoded[0].Data.SequenceNumber != 1 {
		t.Fatalf("data missing or wrong sequence")
	}
	if want := []byte{0xDE, 0xAD, 0xBE, 0xEF}; !bytes.Equal(decoded[1].Raw, want) {
		t.Fatalf("unknown raw = % X, want % X", decoded[1].Raw, want)
	}
	if decoded[2].InfoTS == nil || !decoded[2].InfoTS.Valid {
		t.Fatalf("info_ts missing")
	}
	if decoded[3].ID != rtps.SubmessagePad || decoded[3].Raw != nil {
		t.Fatalf("pad not skipped cleanly")
	}
	if decoded[4].Raw == nil {
		t.Fatalf("acknack raw missing")
	}
}

// TestUnknownSubmessageSkipSweep decodes every id and requires no panic.
func TestUnknownSubmessageSkipSweep(t *testing.T) {
	t.Parallel()

	known := map[uint8]bool{
		rtps.SubmessageHeaderExtension: true,
		rtps.SubmessagePad:             true,
		rtps.SubmessageAckNack:         true,
		rtps.SubmessageHeartbeat:       true,
		rtps.SubmessageGap:             true,
		rtps.SubmessageInfoTS:          true,
		rtps.SubmessageInfoSrc:         true,
		rtps.SubmessageInfoReplyIP4:    true,
		rtps.SubmessageInfoDst:         true,
		rtps.SubmessageInfoReply:       true,
		rtps.SubmessageData:            true,
		rtps.SubmessageDataFrag:        true,
		rtps.SubmessageNackFrag:        true,
		rtps.SubmessageHeartbeatFrag:   true,
	}
	for id := range 256 {
		raw := rtps.RawSubmessage{ID: uint8(id), Flags: 0x01, Body: []byte{0x01, 0x02, 0x03, 0x04}}
		var drops rtps.Counter
		got, err := rtps.DecodeSubmessage(raw, &drops)
		if known[uint8(id)] {
			continue
		}
		if err != nil {
			t.Fatalf("unknown id %#02x: %v, want skip", id, err)
		}
		if !bytes.Equal(got.Raw, raw.Body) {
			t.Fatalf("unknown id %#02x raw lost", id)
		}
		if drops.Count() != 0 {
			t.Fatalf("unknown id %#02x drops = %d", id, drops.Count())
		}
	}
}

// TestDataBodyOversize rejects payloads above 64K.
func TestDataBodyOversize(t *testing.T) {
	t.Parallel()

	data := testData()
	data.Payload = make([]byte, 70000)
	if _, err := data.Body(true); err == nil {
		t.Fatal("oversize data body succeeded, want error")
	}
	if _, err := data.Submessage(true); err == nil {
		t.Fatal("oversize data submessage succeeded, want error")
	}
}

// TestDataInlineQoSOversize rejects inline values above 64K.
func TestDataInlineQoSOversize(t *testing.T) {
	t.Parallel()

	data := testData()
	data.InlineQoS = []rtps.Parameter{{ID: 0x0050, Value: make([]byte, 70000)}}
	if _, err := data.Body(true); err == nil {
		t.Fatal("oversize inline qos succeeded, want error")
	}
}

// TestAckNackOversize rejects bodies above 64K.
func TestAckNackOversize(t *testing.T) {
	t.Parallel()

	big := rtps.AckNack{
		ReaderID: rtps.EntityUnknown,
		WriterID: rtps.EntitySPDPWriter,
		Base:     1,
		NumBits:  600000,
		Bitmap:   make([]uint32, 18750),
		Count:    1,
	}
	if _, err := big.Encode(true); err == nil {
		t.Fatal("oversize acknack succeeded, want error")
	}
}

// TestAckNackRtps1BitsAbove256Invalid pins loud decode reject per 8.3.5.5.
func TestAckNackRtps1BitsAbove256Invalid(t *testing.T) {
	t.Parallel()

	body := make([]byte, 0, 24+9*4)
	body = append(body, 0x00, 0x00, 0x00, 0x00)
	body = append(body, 0x00, 0x01, 0x00, 0xC2)
	var base [8]byte
	binary.LittleEndian.PutUint32(base[0:4], 0)
	binary.LittleEndian.PutUint32(base[4:8], 1)
	body = append(body, base[:]...)
	var bits [4]byte
	binary.LittleEndian.PutUint32(bits[:], 257)
	body = append(body, bits[:]...)
	for range 9 {
		var w [4]byte
		binary.LittleEndian.PutUint32(w[:], 0xFFFFFFFF)
		body = append(body, w[:]...)
	}
	var count [4]byte
	binary.LittleEndian.PutUint32(count[:], 1)
	body = append(body, count[:]...)
	var drops rtps.Counter
	if _, err := rtps.DecodeAckNack(body, 0x01, &drops); err == nil {
		t.Fatal("acknack 257 bits succeeded, want invalid")
	}
	if drops.Count() != 1 {
		t.Fatalf("drops = %d, want 1", drops.Count())
	}
}

// TestNackFragRtps1BitsAbove256Invalid pins loud decode reject per 8.3.5.7.
func TestNackFragRtps1BitsAbove256Invalid(t *testing.T) {
	t.Parallel()

	body := make([]byte, 0, 28+9*4)
	body = append(body, 0x00, 0x00, 0x00, 0x00)
	body = append(body, 0x00, 0x01, 0x00, 0xC2)
	var sn [8]byte
	binary.LittleEndian.PutUint32(sn[0:4], 0)
	binary.LittleEndian.PutUint32(sn[4:8], 9)
	body = append(body, sn[:]...)
	var base [4]byte
	binary.LittleEndian.PutUint32(base[:], 1)
	body = append(body, base[:]...)
	var bits [4]byte
	binary.LittleEndian.PutUint32(bits[:], 257)
	body = append(body, bits[:]...)
	for range 9 {
		var w [4]byte
		binary.LittleEndian.PutUint32(w[:], 0xFFFFFFFF)
		body = append(body, w[:]...)
	}
	var count [4]byte
	binary.LittleEndian.PutUint32(count[:], 1)
	body = append(body, count[:]...)
	var drops rtps.Counter
	if _, err := rtps.DecodeNackFrag(body, 0x01, &drops); err == nil {
		t.Fatal("nackfrag 257 bits succeeded, want invalid")
	}
	if drops.Count() != 1 {
		t.Fatalf("drops = %d, want 1", drops.Count())
	}
}

// TestDecodeSubmessageErrors checks truncated typed kinds drop loudly.
func TestDecodeSubmessageErrors(t *testing.T) {
	t.Parallel()

	cases := []rtps.RawSubmessage{
		{ID: rtps.SubmessageData, Flags: 0x05, Body: []byte{0x01}},
		{ID: rtps.SubmessageDataFrag, Flags: 0x01, Body: []byte{0x01}},
		{ID: rtps.SubmessageInfoTS, Flags: 0x01, Body: []byte{0x01}},
		{ID: rtps.SubmessageHeartbeat, Flags: 0x01, Body: []byte{0x01}},
		{ID: rtps.SubmessageGap, Flags: 0x01, Body: []byte{0x01}},
	}
	for _, raw := range cases {
		var drops rtps.Counter
		if _, err := rtps.DecodeSubmessage(raw, &drops); err == nil {
			t.Fatalf("id %#02x succeeded, want error", raw.ID)
		} else if drops.Count() != 1 {
			t.Fatalf("id %#02x drops = %d, want 1", raw.ID, drops.Count())
		}
	}
}

// TestDecodeDataInlineQoSBad checks unterminated inline QoS via dispatcher.
func TestDecodeDataInlineQoSBad(t *testing.T) {
	t.Parallel()

	body, err := testData().Body(true)
	if err != nil {
		t.Fatalf("body: %v", err)
	}
	raw := rtps.RawSubmessage{ID: rtps.SubmessageData, Flags: 0x07, Body: body}
	var drops rtps.Counter
	if _, err := rtps.DecodeSubmessage(raw, &drops); err == nil {
		t.Fatal("bad inline qos via dispatcher succeeded, want error")
	}
}

// TestDecodeSubmessageHeartbeatGap checks typed heartbeat and gap dispatch.
func TestDecodeSubmessageHeartbeatGap(t *testing.T) {
	t.Parallel()

	beat := rtps.Heartbeat{
		ReaderID:      rtps.EntityUnknown,
		WriterID:      rtps.EntitySPDPWriter,
		FirstSequence: 1,
		LastSequence:  5,
		Count:         9,
	}
	beatBody := beat.Encode(true)
	var drops rtps.Counter
	beatRaw := rtps.RawSubmessage{ID: rtps.SubmessageHeartbeat, Flags: beat.Flags(true, true, false), Body: beatBody}
	decoded, err := rtps.DecodeSubmessage(beatRaw, &drops)
	if err != nil {
		t.Fatalf("heartbeat dispatch: %v", err)
	}
	if decoded.Heartbeat == nil || decoded.Heartbeat.LastSequence != 5 {
		t.Fatalf("heartbeat = %+v, want last 5", decoded.Heartbeat)
	}
	gapBody := []byte{
		0x00, 0x00, 0x00, 0x00,
		0x00, 0x01, 0x00, 0xC2,
		0x00, 0x00, 0x00, 0x00, 0x0A, 0x00, 0x00, 0x00,
	}
	gapRaw := rtps.RawSubmessage{ID: rtps.SubmessageGap, Flags: 0x01, Body: gapBody}
	decoded, err = rtps.DecodeSubmessage(gapRaw, &drops)
	if err != nil {
		t.Fatalf("gap dispatch: %v", err)
	}
	if decoded.Gap == nil || decoded.Gap.GapStart != 10 {
		t.Fatalf("gap = %+v, want start 10", decoded.Gap)
	}
	if drops.Count() != 0 {
		t.Fatalf("drops = %d, want 0", drops.Count())
	}
}

// gapBaseBody builds a GAP body with the given start, base, and bits.
func gapBaseBody(start, base uint64, numBits uint32) []byte {
	body := []byte{
		0x00, 0x00, 0x00, 0x00,
		0x00, 0x01, 0x00, 0xC2,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00,
		0xFF, 0xFF, 0xFF, 0xFF,
	}
	putGapSeq(body[8:16], start)
	putGapSeq(body[16:24], base)
	putGapNumBits(body[24:28], numBits)
	return body
}

// putGapSeq stores one sequence as high word then low word little endian.
func putGapSeq(dst []byte, v uint64) {
	binary.LittleEndian.PutUint32(dst[0:4], uint32(v>>32))
	binary.LittleEndian.PutUint32(dst[4:8], uint32(v)) //nolint:gosec // G115: low word truncation is the RTPS layout
}

// putGapNumBits stores one bit count little endian.
func putGapNumBits(dst []byte, v uint32) {
	binary.LittleEndian.PutUint32(dst, v)
}

// TestGapBaseAtOrBelowStartRejects pins loud reject for unsorted lists.
func TestGapBaseAtOrBelowStartRejects(t *testing.T) {
	t.Parallel()

	for _, base := range []uint64{10, 5, 0} {
		body := gapBaseBody(10, base, 1)
		var drops rtps.Counter
		if _, err := rtps.DecodeGap(body, 0x01, &drops); err == nil {
			t.Fatalf("base %d with start 10 succeeded, want error", base)
		}
		if drops.Count() != 1 {
			t.Fatalf("base %d drops = %d, want 1", base, drops.Count())
		}
		var subDrops rtps.Counter
		raw := rtps.RawSubmessage{ID: rtps.SubmessageGap, Flags: 0x01, Body: body}
		if _, err := rtps.DecodeSubmessage(raw, &subDrops); err == nil {
			t.Fatalf("dispatcher base %d succeeded, want error", base)
		}
		if subDrops.Count() != 1 {
			t.Fatalf("dispatcher base %d drops = %d, want 1", base, subDrops.Count())
		}
	}

	good := gapBaseBody(10, 11, 1)
	var drops rtps.Counter
	gap, err := rtps.DecodeGap(good, 0x01, &drops)
	if err != nil {
		t.Fatalf("base 11 with start 10: %v", err)
	}
	if gap.GapStart != 10 || gap.GapListBase != 11 {
		t.Fatalf("gap = %+v, want start 10 base 11", gap)
	}
	if drops.Count() != 0 {
		t.Fatalf("drops = %d, want 0", drops.Count())
	}
}

// TestDataSeqZeroCountsDrop pins seq 0 invalid per 8.3.8.2, matching DataFrag.
func TestDataSeqZeroCountsDrop(t *testing.T) {
	t.Parallel()

	zero := testData()
	zero.SequenceNumber = 0
	body, err := zero.Body(true)
	if err != nil {
		t.Fatalf("body: %v", err)
	}
	var drops rtps.Counter
	if _, err := rtps.DecodeData(body, 0x05, &drops); err == nil {
		t.Fatalf("decode seq 0 succeeded, want invalid")
	}
	if drops.Count() != 1 {
		t.Fatalf("drops = %d, want 1", drops.Count())
	}
	raw := rtps.RawSubmessage{ID: rtps.SubmessageData, Flags: 0x05, Body: body}
	var subDrops rtps.Counter
	if _, err := rtps.DecodeSubmessage(raw, &subDrops); err == nil {
		t.Fatalf("dispatcher seq 0 succeeded, want invalid")
	}
	if subDrops.Count() != 1 {
		t.Fatalf("dispatcher drops = %d, want 1", subDrops.Count())
	}
}

// validDataFragBody returns a minimal valid DATA_FRAG body little endian.
func validDataFragBody() []byte {
	return []byte{
		0x00, 0x00, 0x1C, 0x00,
		0x00, 0x00, 0x00, 0x00,
		0x00, 0x01, 0x00, 0xC2,
		0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00,
		0x01, 0x00, 0x00, 0x00,
		0x01, 0x00,
		0x05, 0x00,
		0x05, 0x00, 0x00, 0x00,
		0x00, 0x01, 0x00, 0x00, 0x41,
	}
}

// TestDataSeqRtps1UnknownInvalid pins UNKNOWN seq invalid like DataFrag.
func TestDataSeqRtps1UnknownInvalid(t *testing.T) {
	t.Parallel()

	unknown := testData()
	unknown.SequenceNumber = 0xFFFFFFFF00000000
	body, err := unknown.Body(true)
	if err != nil {
		t.Fatalf("body: %v", err)
	}
	var drops rtps.Counter
	if _, err := rtps.DecodeData(body, 0x05, &drops); err == nil {
		t.Fatalf("decode unknown succeeded, want invalid")
	}
	if drops.Count() != 1 {
		t.Fatalf("drops = %d, want 1", drops.Count())
	}
}

// knownTypedCase names one dispatcher case with its expected typed field.
type knownTypedCase struct {
	name string
	raw  rtps.RawSubmessage
	want string
}

// knownTypedCases builds valid bodies for every known typed ID.
func knownTypedCases(t *testing.T) []knownTypedCase {
	t.Helper()
	dataSub, err := testData().Submessage(true)
	if err != nil {
		t.Fatalf("data submessage: %v", err)
	}
	stamp := rtps.InfoTS{Valid: true, Seconds: 3, Fraction: 4}
	beat := rtps.Heartbeat{
		ReaderID:      rtps.EntityUnknown,
		WriterID:      rtps.EntitySPDPWriter,
		FirstSequence: 1,
		LastSequence:  5,
		Count:         9,
	}
	gapBody := []byte{
		0x00, 0x00, 0x00, 0x00,
		0x00, 0x01, 0x00, 0xC2,
		0x00, 0x00, 0x00, 0x00, 0x0A, 0x00, 0x00, 0x00,
	}
	return []knownTypedCase{
		{name: "data", raw: rtps.RawSubmessage{ID: rtps.SubmessageData, Flags: dataSub.Flags, Body: dataSub.Body}, want: "data"},
		{name: "datafrag", raw: rtps.RawSubmessage{ID: rtps.SubmessageDataFrag, Flags: 0x01, Body: validDataFragBody()}, want: "datafrag"},
		{name: "infots", raw: rtps.RawSubmessage{ID: rtps.SubmessageInfoTS, Flags: stamp.Flags(true), Body: stamp.Encode(true)}, want: "infots"},
		{name: "heartbeat", raw: rtps.RawSubmessage{ID: rtps.SubmessageHeartbeat, Flags: beat.Flags(true, true, false), Body: beat.Encode(true)}, want: "heartbeat"},
		{name: "gap", raw: rtps.RawSubmessage{ID: rtps.SubmessageGap, Flags: 0x01, Body: gapBody}, want: "gap"},
	}
}

// checkKnownTyped asserts one dispatcher result carries the typed field.
func checkKnownTyped(t *testing.T, tc knownTypedCase, got rtps.Submessage) {
	t.Helper()
	if tc.want == "data" || tc.want == "datafrag" {
		checkKnownDataTyped(t, tc, got)
		return
	}
	checkKnownControlTyped(t, tc, got)
}

// checkKnownDataTyped asserts data and datafrag typed fields.
func checkKnownDataTyped(t *testing.T, tc knownTypedCase, got rtps.Submessage) {
	t.Helper()
	if tc.want == "data" {
		if got.Data == nil {
			t.Fatalf("data typed is nil, want non-nil")
		}
		if got.Raw != nil {
			t.Fatalf("data raw is set, want typed only")
		}
		return
	}
	if got.DataFrag == nil {
		t.Fatalf("datafrag typed is nil, want non-nil")
	}
	if got.Raw != nil {
		t.Fatalf("datafrag raw is set, want typed only")
	}
	if got.DataFrag.SequenceNumber != 1 {
		t.Fatalf("datafrag seq = %d, want 1", got.DataFrag.SequenceNumber)
	}
}

// checkKnownControlTyped asserts infots, heartbeat, and gap typed fields.
func checkKnownControlTyped(t *testing.T, tc knownTypedCase, got rtps.Submessage) {
	t.Helper()
	switch tc.want {
	case "infots":
		if got.InfoTS == nil {
			t.Fatalf("infots typed is nil, want non-nil")
		}
	case "heartbeat":
		if got.Heartbeat == nil {
			t.Fatalf("heartbeat typed is nil, want non-nil")
		}
	case "gap":
		if got.Gap == nil {
			t.Fatalf("gap typed is nil, want non-nil")
		}
	}
}

// TestDecodeSubmessageKnownTyped asserts typed pointers for known IDs.
func TestDecodeSubmessageKnownTyped(t *testing.T) {
	t.Parallel()

	for _, tc := range knownTypedCases(t) {
		var drops rtps.Counter
		got, err := rtps.DecodeSubmessage(tc.raw, &drops)
		if err != nil {
			t.Fatalf("%s dispatch: %v", tc.name, err)
		}
		checkKnownTyped(t, tc, got)
		if drops.Count() != 0 {
			t.Fatalf("%s drops = %d, want 0", tc.name, drops.Count())
		}
	}
}

// TestInfoSrcRoundTrip pins minimal INFO_SRC encode plus decode per 8.3.8.10.
func TestInfoSrcRoundTrip(t *testing.T) {
	t.Parallel()

	src := rtps.InfoSrc{Major: 2, Minor: 5, Vendor: [2]byte{0x01, 0x10}}
	body := src.Encode()
	if len(body) != 5 {
		t.Fatalf("info_src len = %d, want 5", len(body))
	}
	var drops rtps.Counter
	back, err := rtps.DecodeInfoSrc(body, 0x01, &drops)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if back != src {
		t.Fatalf("round trip = %+v, want %+v", back, src)
	}
	if drops.Count() != 0 {
		t.Fatalf("drops = %d, want 0", drops.Count())
	}
}

// TestInfoSrcInvalid pins fail-closed validation for malformed bodies.
func TestInfoSrcInvalid(t *testing.T) {
	t.Parallel()

	cases := [][]byte{
		{0x00, 0x02},
		{0x00, 0x02, 0x05},
		{0x00, 0x02, 0x05, 0x10},
		{0x01, 0x02, 0x05, 0x01, 0x10},
		{0x00, 0x01, 0x05, 0x01, 0x10},
	}
	for i, body := range cases {
		var drops rtps.Counter
		if _, err := rtps.DecodeInfoSrc(body, 0x01, &drops); err == nil {
			t.Fatalf("case %d succeeded, want error", i)
		}
		if drops.Count() != 1 {
			t.Fatalf("case %d drops = %d, want 1", i, drops.Count())
		}
	}
}

// TestInfoTSExactSize pins the fixed 8 byte valid width per 8.3.8.9.
func TestInfoTSExactSize(t *testing.T) {
	t.Parallel()

	stamp := rtps.InfoTS{Valid: true, Seconds: 1, Fraction: 2}
	body := append(stamp.Encode(true), 0x00)
	var drops rtps.Counter
	if _, err := rtps.DecodeInfoTS(body, stamp.Flags(true), &drops); err == nil {
		t.Fatalf("trailing info_ts succeeded, want error")
	}
	if drops.Count() != 1 {
		t.Fatalf("drops = %d, want 1", drops.Count())
	}
}

// TestInfoSrcExactSize pins the fixed 5 byte width per 8.3.8.10.
func TestInfoSrcExactSize(t *testing.T) {
	t.Parallel()

	src := rtps.InfoSrc{Major: 2, Minor: 1, Vendor: [2]byte{0x01, 0x01}}
	body := append(src.Encode(), 0xFF, 0xFF)
	var drops rtps.Counter
	if _, err := rtps.DecodeInfoSrc(body, 0x01, &drops); err == nil {
		t.Fatalf("trailing info_src succeeded, want error")
	}
	if drops.Count() != 1 {
		t.Fatalf("drops = %d, want 1", drops.Count())
	}
	if _, err := rtps.DecodeInfoSrc(src.Encode()[:4], 0x01, &drops); err == nil {
		t.Fatalf("short info_src succeeded, want error")
	}
	if drops.Count() != 2 {
		t.Fatalf("drops = %d, want 2", drops.Count())
	}
}

// TestDecodeSubmessageInfoSrc pins typed dispatch plus error paths.
func TestDecodeSubmessageInfoSrc(t *testing.T) {
	t.Parallel()

	src := rtps.InfoSrc{Major: 2, Minor: 5, Vendor: [2]byte{0x01, 0x10}}
	raw := rtps.RawSubmessage{ID: rtps.SubmessageInfoSrc, Flags: 0x01, Body: src.Encode()}
	var drops rtps.Counter
	got, err := rtps.DecodeSubmessage(raw, &drops)
	if err != nil {
		t.Fatalf("dispatch: %v", err)
	}
	if got.InfoSrc == nil || *got.InfoSrc != src {
		t.Fatalf("info_src = %+v, want %+v", got.InfoSrc, src)
	}
	bad := rtps.RawSubmessage{ID: rtps.SubmessageInfoSrc, Flags: 0x01, Body: []byte{0x01}}
	var badDrops rtps.Counter
	if _, err := rtps.DecodeSubmessage(bad, &badDrops); err == nil {
		t.Fatalf("short info_src succeeded, want error")
	}
	if badDrops.Count() != 1 {
		t.Fatalf("bad drops = %d, want 1", badDrops.Count())
	}
}

// TestIsKnownSubmessageID pins known versus unknown classification per 8.3.4.
func TestIsKnownSubmessageID(t *testing.T) {
	t.Parallel()

	known := []uint8{
		rtps.SubmessageHeaderExtension,
		rtps.SubmessagePad,
		rtps.SubmessageAckNack,
		rtps.SubmessageHeartbeat,
		rtps.SubmessageGap,
		rtps.SubmessageInfoTS,
		rtps.SubmessageInfoSrc,
		rtps.SubmessageInfoReplyIP4,
		rtps.SubmessageInfoDst,
		rtps.SubmessageInfoReply,
		rtps.SubmessageData,
		rtps.SubmessageDataFrag,
		rtps.SubmessageNackFrag,
		rtps.SubmessageHeartbeatFrag,
	}
	for _, id := range known {
		if !rtps.IsKnownSubmessageID(id) {
			t.Fatalf("id %#02x not known, want known", id)
		}
	}
	for _, id := range []uint8{0x02, 0xFF} {
		if rtps.IsKnownSubmessageID(id) {
			t.Fatalf("id %#02x known, want unknown", id)
		}
	}
}

// putGapSeqLE stores one sequence as high word then low word little endian.
func putGapSeqLE(dst []byte, v uint64) {
	binary.LittleEndian.PutUint32(dst[0:4], uint32(v>>32))
	binary.LittleEndian.PutUint32(dst[4:8], uint32(v)) //nolint:gosec // G115: low word truncation is the RTPS layout
}

// gapEmptyBody builds an exact empty gap list body without bitmap words.
func gapEmptyBody(start, base uint64) []byte {
	body := make([]byte, 0, 28)
	body = append(body, 0x00, 0x00, 0x00, 0x00)
	body = append(body, 0x00, 0x01, 0x00, 0xC2)
	var startRaw [8]byte
	putGapSeqLE(startRaw[:], start)
	body = append(body, startRaw[:]...)
	var baseRaw [8]byte
	putGapSeqLE(baseRaw[:], base)
	body = append(body, baseRaw[:]...)
	var bits [4]byte
	binary.LittleEndian.PutUint32(bits[:], 0)
	body = append(body, bits[:]...)
	return body
}

// TestGapEmptyBaseZeroAccepts pins empty set with base 0 per 8.3.5.5.
func TestGapEmptyBaseZeroAccepts(t *testing.T) {
	t.Parallel()

	body := gapEmptyBody(10, 0)
	var drops rtps.Counter
	gap, err := rtps.DecodeGap(body, 0x01, &drops)
	if err != nil {
		t.Fatalf("decode empty base 0: %v", err)
	}
	if gap.GapListBase != 0 || gap.NumBits != 0 {
		t.Fatalf("gap = base %d bits %d, want 0 0", gap.GapListBase, gap.NumBits)
	}
	if drops.Count() != 0 {
		t.Fatalf("drops = %d, want 0", drops.Count())
	}
}

// TestGapBaseOverflowRejects pins fail-closed on base plus bits wrap.
func TestGapBaseOverflowRejects(t *testing.T) {
	t.Parallel()

	base := ^uint64(0) - 5
	body := gapBaseBody(1, base, 10)
	var drops rtps.Counter
	if _, err := rtps.DecodeGap(body, 0x01, &drops); err == nil {
		t.Fatalf("gap overflow base %d succeeded, want error", base)
	}
	if drops.Count() != 1 {
		t.Fatalf("drops = %d, want 1", drops.Count())
	}
}

// ackNackEmptyBody builds an exact empty receipt body.
func ackNackEmptyBody(base uint64) []byte {
	body := make([]byte, 0, 24)
	body = append(body, 0x00, 0x00, 0x00, 0x00)
	body = append(body, 0x00, 0x01, 0x00, 0xC2)
	var baseRaw [8]byte
	putGapSeqLE(baseRaw[:], base)
	body = append(body, baseRaw[:]...)
	var bits [4]byte
	binary.LittleEndian.PutUint32(bits[:], 0)
	body = append(body, bits[:]...)
	var count [4]byte
	binary.LittleEndian.PutUint32(count[:], 1)
	body = append(body, count[:]...)
	return body
}

// TestAckNackEmptyBaseZeroAccepts pins empty receipt with base 0.
func TestAckNackEmptyBaseZeroAccepts(t *testing.T) {
	t.Parallel()

	var drops rtps.Counter
	got, err := rtps.DecodeAckNack(ackNackEmptyBody(0), 0x01, &drops)
	if err != nil {
		t.Fatalf("decode empty base 0: %v", err)
	}
	if got.Base != 0 || got.NumBits != 0 {
		t.Fatalf("acknack = base %d bits %d, want 0 0", got.Base, got.NumBits)
	}
	if drops.Count() != 0 {
		t.Fatalf("drops = %d, want 0", drops.Count())
	}
}

// ackNackOverflowBody builds a receipt with bitmap before count.
func ackNackOverflowBody(base uint64, numBits uint32) []byte {
	body := make([]byte, 0, 28)
	body = append(body, 0x00, 0x00, 0x00, 0x00)
	body = append(body, 0x00, 0x01, 0x00, 0xC2)
	var baseRaw [8]byte
	putGapSeqLE(baseRaw[:], base)
	body = append(body, baseRaw[:]...)
	var bits [4]byte
	binary.LittleEndian.PutUint32(bits[:], numBits)
	body = append(body, bits[:]...)
	var word [4]byte
	binary.LittleEndian.PutUint32(word[:], 0xFFFFFFFF)
	body = append(body, word[:]...)
	var count [4]byte
	binary.LittleEndian.PutUint32(count[:], 1)
	body = append(body, count[:]...)
	return body
}

// TestAckNackBaseOverflowRejects pins fail-closed on base plus bits wrap.
func TestAckNackBaseOverflowRejects(t *testing.T) {
	t.Parallel()

	base := ^uint64(0) - 3
	body := ackNackOverflowBody(base, 10)
	var drops rtps.Counter
	if _, err := rtps.DecodeAckNack(body, 0x01, &drops); err == nil {
		t.Fatalf("acknack overflow base %d succeeded, want error", base)
	}
	if drops.Count() != 1 {
		t.Fatalf("drops = %d, want 1", drops.Count())
	}
}

// TestAckNackTrailingRejects pins exact layout with no trailing garbage.
func TestAckNackTrailingRejects(t *testing.T) {
	t.Parallel()

	receipt := rtps.AckNack{
		ReaderID: rtps.EntityUnknown,
		WriterID: rtps.EntitySPDPWriter,
		Base:     1,
		NumBits:  0,
		Count:    7,
	}
	body, err := receipt.Encode(true)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	body = append(body, 0x00)
	var drops rtps.Counter
	if _, err := rtps.DecodeAckNack(body, 0x01, &drops); err == nil {
		t.Fatalf("trailing byte succeeded, want error")
	}
	if drops.Count() != 1 {
		t.Fatalf("drops = %d, want 1", drops.Count())
	}
}

// gapFilteredBody builds a gap list plus 8 byte ChangeCount filteredCount.
func gapFilteredBody(start, base uint64, numBits uint32, filtered uint64) []byte {
	body := gapBaseBody(start, base, numBits)
	var raw [8]byte
	binary.LittleEndian.PutUint32(raw[0:4], uint32(filtered>>32))
	binary.LittleEndian.PutUint32(raw[4:8], uint32(filtered)) //nolint:gosec // G115: low word truncation is the RTPS layout
	body = append(body, raw[:]...)
	return body
}

// TestGapFilteredCountWidth pins ChangeCount_t as 8 bytes per 8.3.8.4.
func TestGapFilteredCountWidth(t *testing.T) {
	t.Parallel()

	const want uint64 = 0x0102030405060708
	body := gapFilteredBody(10, 11, 1, want)
	var drops rtps.Counter
	gap, err := rtps.DecodeGap(body, 0x01|0x04, &drops)
	if err != nil {
		t.Fatalf("decode filtered: %v", err)
	}
	if !gap.HasFiltered {
		t.Fatalf("hasFiltered = false, want true")
	}
	if gap.FilteredCount != want {
		t.Fatalf("filtered = %#x, want %#x", gap.FilteredCount, want)
	}
	if drops.Count() != 0 {
		t.Fatalf("drops = %d, want 0", drops.Count())
	}
	short := body[:len(body)-1]
	var shortDrops rtps.Counter
	if _, err := rtps.DecodeGap(short, 0x01|0x04, &shortDrops); err == nil {
		t.Fatalf("short filtered succeeded, want error")
	}
	if shortDrops.Count() != 1 {
		t.Fatalf("short drops = %d, want 1", shortDrops.Count())
	}
}

// TestGapTrailingRejects pins exact length past bitmap and G/F per 8.3.8.4 like AckNack.
func TestGapTrailingRejects(t *testing.T) {
	t.Parallel()

	base := gapBaseBody(10, 11, 1)
	var drops rtps.Counter
	if _, err := rtps.DecodeGap(append(base, 0x00), 0x01, &drops); err == nil {
		t.Fatalf("gap trailing after bitmap succeeded, want error")
	}
	if drops.Count() != 1 {
		t.Fatalf("drops = %d, want 1", drops.Count())
	}

	withGroup := append(gapBaseBody(10, 11, 1), make([]byte, 16)...)
	// Fill group with valid positive sequences: start 1, end 2.
	binary.LittleEndian.PutUint32(withGroup[32:36], 0)
	binary.LittleEndian.PutUint32(withGroup[36:40], 1)
	binary.LittleEndian.PutUint32(withGroup[40:44], 0)
	binary.LittleEndian.PutUint32(withGroup[44:48], 2)
	var gDrops rtps.Counter
	if _, err := rtps.DecodeGap(append(withGroup, 0xFF), 0x01|0x02, &gDrops); err == nil {
		t.Fatalf("gap trailing after group succeeded, want error")
	}
	if gDrops.Count() != 1 {
		t.Fatalf("group drops = %d, want 1", gDrops.Count())
	}

	filtered := gapFilteredBody(10, 11, 1, 7)
	var fDrops rtps.Counter
	if _, err := rtps.DecodeGap(append(filtered, 0x00, 0x01), 0x01|0x04, &fDrops); err == nil {
		t.Fatalf("gap trailing after filtered succeeded, want error")
	}
	if fDrops.Count() != 1 {
		t.Fatalf("filtered drops = %d, want 1", fDrops.Count())
	}
}

// TestGapEmptyExtraWordsRejects pins numBits 0 with extra words fail-closed per 8.3.5.5.
func TestGapEmptyExtraWordsRejects(t *testing.T) {
	t.Parallel()

	empty := gapEmptyBody(10, 0)
	var drops rtps.Counter
	if _, err := rtps.DecodeGap(empty, 0x01, &drops); err != nil {
		t.Fatalf("empty base 0 with numBits 0: %v", err)
	}
	// Extra word without G/F flags must reject as trailing.
	extra := append(append([]byte(nil), empty...), 0x00, 0x00, 0x00, 0x00)
	var extraDrops rtps.Counter
	if _, err := rtps.DecodeGap(extra, 0x01, &extraDrops); err == nil {
		t.Fatalf("numBits 0 with extra word succeeded, want error")
	}
	if extraDrops.Count() != 1 {
		t.Fatalf("drops = %d, want 1", extraDrops.Count())
	}
	// Only G/F fields may follow per flags; bare extra words never pass.
	raw := rtps.RawSubmessage{ID: rtps.SubmessageGap, Flags: 0x01, Body: extra}
	var subDrops rtps.Counter
	if _, err := rtps.DecodeSubmessage(raw, &subDrops); err == nil {
		t.Fatalf("dispatcher extra words succeeded, want error")
	}
}

// TestGapFlagValues pins G 0x02 and F 0x04 per DDSI-RTPS 2.5 8.3.8.4 table 24.
func TestGapFlagValues(t *testing.T) {
	t.Parallel()

	if rtps.GapFlagGroupInfo != 0x02 {
		t.Fatalf("GapFlagGroupInfo = %#02x, want 0x02", rtps.GapFlagGroupInfo)
	}
	if rtps.GapFlagFilteredCount != 0x04 {
		t.Fatalf("GapFlagFilteredCount = %#02x, want 0x04", rtps.GapFlagFilteredCount)
	}
}

// nackFragBody builds a minimal NACK_FRAG body with one bitmap word.
func nackFragBody(writerSN uint64, base, numBits uint32) []byte {
	body := make([]byte, 0, 32)
	body = append(body, 0x00, 0x00, 0x00, 0x00)
	body = append(body, 0x00, 0x01, 0x00, 0xC2)
	var sn [8]byte
	binary.LittleEndian.PutUint32(sn[0:4], uint32(writerSN>>32))
	binary.LittleEndian.PutUint32(sn[4:8], uint32(writerSN)) //nolint:gosec // G115: low word truncation is the RTPS layout
	body = append(body, sn[:]...)
	var b [4]byte
	binary.LittleEndian.PutUint32(b[:], base)
	body = append(body, b[:]...)
	var n [4]byte
	binary.LittleEndian.PutUint32(n[:], numBits)
	body = append(body, n[:]...)
	var w [4]byte
	binary.LittleEndian.PutUint32(w[:], 0xFFFFFFFF)
	body = append(body, w[:]...)
	var c [4]byte
	binary.LittleEndian.PutUint32(c[:], 1)
	body = append(body, c[:]...)
	return body
}

// TestNackFragTrailingRejects pins exact length like AckNack per 8.3.8.12.
func TestNackFragTrailingRejects(t *testing.T) {
	t.Parallel()

	body := nackFragBody(9, 1, 1)
	var drops rtps.Counter
	if _, err := rtps.DecodeNackFrag(append(body, 0x00), 0x01, &drops); err == nil {
		t.Fatalf("nackfrag trailing succeeded, want error")
	}
	if drops.Count() != 1 {
		t.Fatalf("drops = %d, want 1", drops.Count())
	}
	// Exact body must accept.
	var goodDrops rtps.Counter
	got, err := rtps.DecodeNackFrag(body, 0x01, &goodDrops)
	if err != nil {
		t.Fatalf("valid nackfrag: %v", err)
	}
	if got.Base != 1 || got.NumBits != 1 || got.WriterSN != 9 {
		t.Fatalf("nackfrag = %+v, want base 1 bits 1 sn 9", got)
	}
}

// TestNackFragOverflowRejects pins base plus bits above uint32 per 8.3.8.12 and 8.3.5.7.
func TestNackFragOverflowRejects(t *testing.T) {
	t.Parallel()

	// Base at max uint32 with bits 2 exceeds the 32-bit fragment space.
	body := nackFragBody(5, 0xFFFFFFFF, 2)
	var drops rtps.Counter
	if _, err := rtps.DecodeNackFrag(body, 0x01, &drops); err == nil {
		t.Fatalf("nackfrag base max plus bits succeeded, want error")
	}
	if drops.Count() != 1 {
		t.Fatalf("drops = %d, want 1", drops.Count())
	}
	// The uint64 no-wrap guard mirrors AckNack/Gap: near-max base plus bits drops.
	wide := nackFragBody(5, 0xFFFFFF00, 255)
	var wideDrops rtps.Counter
	if _, err := rtps.DecodeNackFrag(wide, 0x01, &wideDrops); err == nil {
		t.Fatalf("nackfrag wide range succeeded, want error")
	}
	if wideDrops.Count() != 1 {
		t.Fatalf("wide drops = %d, want 1", wideDrops.Count())
	}
}

// heartbeatFragBody builds a 24-byte HEARTBEAT_FRAG body per 8.3.8.7.
func heartbeatFragBody(writerSN uint64, lastFrag uint32) []byte {
	body := make([]byte, 0, 24)
	body = append(body, 0x00, 0x00, 0x00, 0x00)
	body = append(body, 0x00, 0x01, 0x00, 0xC2)
	var sn [8]byte
	binary.LittleEndian.PutUint32(sn[0:4], uint32(writerSN>>32))
	binary.LittleEndian.PutUint32(sn[4:8], uint32(writerSN)) //nolint:gosec // G115: low word truncation is the RTPS layout
	body = append(body, sn[:]...)
	var last [4]byte
	binary.LittleEndian.PutUint32(last[:], lastFrag)
	body = append(body, last[:]...)
	var count [4]byte
	binary.LittleEndian.PutUint32(count[:], 7)
	body = append(body, count[:]...)
	return body
}

// TestHeartbeatFragRoundTrip pins exact 24-byte layout per 8.3.8.7.
func TestHeartbeatFragRoundTrip(t *testing.T) {
	t.Parallel()

	body := heartbeatFragBody(3, 5)
	if len(body) != 24 {
		t.Fatalf("heartbeatfrag len = %d, want 24", len(body))
	}
	var drops rtps.Counter
	got, err := rtps.DecodeHeartbeatFrag(body, 0x01, &drops)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.WriterSN != 3 || got.LastFragmentNum != 5 || got.Count != 7 {
		t.Fatalf("heartbeatfrag = %+v, want sn 3 last 5 count 7", got)
	}
	if drops.Count() != 0 {
		t.Fatalf("drops = %d, want 0", drops.Count())
	}
	raw := rtps.RawSubmessage{ID: rtps.SubmessageHeartbeatFrag, Flags: 0x01, Body: body}
	decoded, err := rtps.DecodeSubmessage(raw, &drops)
	if err != nil {
		t.Fatalf("dispatch: %v", err)
	}
	if decoded.HeartbeatFrag == nil || decoded.HeartbeatFrag.LastFragmentNum != 5 {
		t.Fatalf("typed = %+v, want last 5", decoded.HeartbeatFrag)
	}
}

// TestHeartbeatFragInvalid pins short, non-positive, zero-last, and trailing per 8.3.8.7.
func TestHeartbeatFragInvalid(t *testing.T) {
	t.Parallel()

	cases := map[string][]byte{
		"short":    heartbeatFragBody(3, 5)[:23],
		"writer 0": heartbeatFragBody(0, 5),
		"last 0":   heartbeatFragBody(3, 0),
		"trailing": append(heartbeatFragBody(3, 5), 0x00),
	}
	for name, body := range cases {
		var drops rtps.Counter
		if _, err := rtps.DecodeHeartbeatFrag(body, 0x01, &drops); err == nil {
			t.Fatalf("%s succeeded, want error", name)
		}
		if drops.Count() != 1 {
			t.Fatalf("%s drops = %d, want 1", name, drops.Count())
		}
	}
}

// TestHeartbeatBounds pins first 0 and last below first minus one per 8.3.8.6.3.
func TestHeartbeatBounds(t *testing.T) {
	t.Parallel()

	beat := rtps.Heartbeat{ReaderID: rtps.EntityUnknown, WriterID: rtps.EntitySPDPWriter, FirstSequence: 1, LastSequence: 5, Count: 9}
	if _, err := rtps.DecodeHeartbeat(append([]byte(nil), beat.Encode(true)...), beat.Flags(true, false, false), &rtps.Counter{}); err != nil {
		t.Fatalf("valid heartbeat: %v", err)
	}
	firstZero := rtps.Heartbeat{ReaderID: rtps.EntityUnknown, WriterID: rtps.EntitySPDPWriter, FirstSequence: 0, LastSequence: 5, Count: 1}
	var d0 rtps.Counter
	if _, err := rtps.DecodeHeartbeat(firstZero.Encode(true), 0x01, &d0); err == nil {
		t.Fatalf("first 0 succeeded, want error")
	}
	below := rtps.Heartbeat{ReaderID: rtps.EntityUnknown, WriterID: rtps.EntitySPDPWriter, FirstSequence: 5, LastSequence: 3, Count: 1}
	var d1 rtps.Counter
	if _, err := rtps.DecodeHeartbeat(below.Encode(true), 0x01, &d1); err == nil {
		t.Fatalf("last below first minus one succeeded, want error")
	}
	// Empty range with first equal to last plus one is valid per 8.3.8.6.3.
	empty := rtps.Heartbeat{ReaderID: rtps.EntityUnknown, WriterID: rtps.EntitySPDPWriter, FirstSequence: 6, LastSequence: 5, Count: 1}
	var d2 rtps.Counter
	if _, err := rtps.DecodeHeartbeat(empty.Encode(true), 0x01, &d2); err != nil {
		t.Fatalf("first last-plus-one: %v", err)
	}
	if d0.Count()+d1.Count() != 2 || d2.Count() != 0 {
		t.Fatalf("drops = %d %d %d", d0.Count(), d1.Count(), d2.Count())
	}
}

// TestDataFragNegatives pins zero and UNKNOWN sequences plus geometry per 8.3.8.3.
func TestDataFragNegatives(t *testing.T) {
	t.Parallel()

	zero := validDataFragBody()
	// Zero the sequence low word: bytes 16..20 hold low word LE (01 00 00 00 -> 00 00 00 00).
	zeroSeq := append([]byte(nil), zero...)
	zeroSeq[16] = 0x00
	zeroSeq[17] = 0x00
	zeroSeq[18] = 0x00
	zeroSeq[19] = 0x00
	var d0 rtps.Counter
	if _, err := rtps.DecodeDataFrag(zeroSeq, 0x01, &d0); err == nil {
		t.Fatalf("datafrag seq 0 succeeded, want error")
	}
	// UNKNOWN sequence is negative signed: high FFFFFFFF plus low 0 per 8.3.5.4.
	unknown := append([]byte(nil), zero...)
	unknown[12] = 0xFF
	unknown[13] = 0xFF
	unknown[14] = 0xFF
	unknown[15] = 0xFF
	unknown[16] = 0x00
	unknown[17] = 0x00
	unknown[18] = 0x00
	unknown[19] = 0x00
	var d1 rtps.Counter
	if _, err := rtps.DecodeDataFrag(unknown, 0x01, &d1); err == nil {
		t.Fatalf("datafrag unknown succeeded, want error")
	}
	// Starting number 0 fails per 8.3.8.3.
	startZero := append([]byte(nil), zero...)
	startZero[20] = 0x00
	startZero[21] = 0x00
	startZero[22] = 0x00
	startZero[23] = 0x00
	var d2 rtps.Counter
	if _, err := rtps.DecodeDataFrag(startZero, 0x01, &d2); err == nil {
		t.Fatalf("datafrag start 0 succeeded, want error")
	}
	if d0.Count()+d1.Count()+d2.Count() != 3 {
		t.Fatalf("drops = %d %d %d", d0.Count(), d1.Count(), d2.Count())
	}
}

// TestDataNeitherFlag pins bare control tolerance and trailing reject per 8.3.8.2.
func TestDataNeitherFlag(t *testing.T) {
	t.Parallel()

	fixed := testData()
	body, err := fixed.Body(true)
	if err != nil {
		t.Fatalf("body: %v", err)
	}
	bare := body[:20]
	var drops rtps.Counter
	got, err := rtps.DecodeData(bare, 0x01, &drops)
	if err != nil {
		t.Fatalf("bare control DATA without D nor K: %v", err)
	}
	if got.Payload != nil {
		t.Fatalf("bare payload = % X, want nil", got.Payload)
	}
	trailing := append(append([]byte(nil), bare...), 0xAA)
	var tDrops rtps.Counter
	if _, err := rtps.DecodeData(trailing, 0x01, &tDrops); err == nil {
		t.Fatalf("payload without D nor K succeeded, want error")
	}
	if drops.Count()+tDrops.Count() != 1 {
		t.Fatalf("drops = %d %d", drops.Count(), tDrops.Count())
	}
}

// TestDecodeParametersOversize pins maxParamsPerList cap via DATA inline QoS per 8.3.5.9.
func TestDecodeParametersOversize(t *testing.T) {
	t.Parallel()

	data := testData()
	for i := range 1025 {
		data.InlineQoS = append(data.InlineQoS, rtps.Parameter{ID: uint16(0x1000 + i), Value: []byte{0x01}})
	}
	body, err := data.Body(true)
	if err != nil {
		t.Fatalf("body 1025 params: %v", err)
	}
	flags := uint8(0x01 | rtps.DataFlagDataPresent | rtps.DataFlagInlineQoS)
	var drops rtps.Counter
	if _, err := rtps.DecodeData(body, flags, &drops); err == nil {
		t.Fatalf("1025 params succeeded, want error")
	}
	if drops.Count() != 1 {
		t.Fatalf("drops = %d, want 1", drops.Count())
	}
}

// TestInfoDstWidths pins 0 or 12 byte widths per 8.3.8.13.
func TestInfoDstWidths(t *testing.T) {
	t.Parallel()

	var drops rtps.Counter
	absent, err := rtps.DecodeInfoDst(nil, 0x01, &drops)
	if err != nil || absent.HasPrefix {
		t.Fatalf("empty = %+v %v, want absent", absent, err)
	}
	prefix := [12]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}
	good, err := rtps.DecodeInfoDst(prefix[:], 0x01, &drops)
	if err != nil || !good.HasPrefix || good.Prefix != prefix {
		t.Fatalf("12-byte = %+v %v", good, err)
	}
	for _, n := range []int{1, 5, 11, 13} {
		var badDrops rtps.Counter
		if _, err := rtps.DecodeInfoDst(make([]byte, n), 0x01, &badDrops); err == nil {
			t.Fatalf("len %d succeeded, want error", n)
		}
		if badDrops.Count() != 1 {
			t.Fatalf("len %d drops = %d, want 1", n, badDrops.Count())
		}
	}
	if drops.Count() != 0 {
		t.Fatalf("drops = %d, want 0", drops.Count())
	}
}

// TestInfoReplyIP4Widths pins 8 and 16 byte widths plus flags per 8.3.8.11.
func TestInfoReplyIP4Widths(t *testing.T) {
	t.Parallel()

	uni := []byte{0x01, 0x00, 0x00, 0x00, 0xF9, 0x45, 0x00, 0x00}
	var drops rtps.Counter
	got, err := rtps.DecodeInfoReplyIP4(uni, 0x01, &drops)
	if err != nil || got.HasMulticast {
		t.Fatalf("unicast = %+v %v", got, err)
	}
	multi := append(append([]byte(nil), uni...), 0x02, 0x00, 0x00, 0x00, 0xEF, 0xFF, 0x00, 0x01)
	got, err = rtps.DecodeInfoReplyIP4(multi, 0x01|0x02, &drops)
	if err != nil || !got.HasMulticast {
		t.Fatalf("multicast = %+v %v", got, err)
	}
	var badDrops rtps.Counter
	if _, err := rtps.DecodeInfoReplyIP4(uni, 0x01|0x02, &badDrops); err == nil {
		t.Fatalf("8 bytes with multicast flag succeeded, want error")
	}
	if _, err := rtps.DecodeInfoReplyIP4(multi, 0x01, &badDrops); err == nil {
		t.Fatalf("16 bytes without multicast flag succeeded, want error")
	}
	if _, err := rtps.DecodeInfoReplyIP4(uni, 0x01|0x08, &badDrops); err == nil {
		t.Fatalf("reserved flags succeeded, want error")
	}
}

// TestInfoReplyWidths pins locator multiple plus flags per 8.3.8.11.
func TestInfoReplyWidths(t *testing.T) {
	t.Parallel()

	loc := make([]byte, 24)
	var drops rtps.Counter
	got, err := rtps.DecodeInfoReply(loc, 0x01, &drops)
	if err != nil || len(got.Locators) != 1 {
		t.Fatalf("one locator = %+v %v", got, err)
	}
	empty, err := rtps.DecodeInfoReply(nil, 0x01, &drops)
	if err != nil || len(empty.Locators) != 0 {
		t.Fatalf("empty = %+v %v", empty, err)
	}
	var badDrops rtps.Counter
	if _, err := rtps.DecodeInfoReply(make([]byte, 25), 0x01, &badDrops); err == nil {
		t.Fatalf("25 bytes succeeded, want error")
	}
	if _, err := rtps.DecodeInfoReply(loc, 0x01|0x08, &badDrops); err == nil {
		t.Fatalf("reserved flags succeeded, want error")
	}
}

// TestClassifyUnicastInfoDst pins absent, match, mismatch, and malformed per 8.3.8.13.
func TestClassifyUnicastInfoDst(t *testing.T) {
	t.Parallel()

	local := testPrefix()
	plain, err := rtps.EncodeMessage(rtps.DefaultHeader(testPrefix()), checksumTestSubs())
	if err != nil {
		t.Fatalf("encode plain: %v", err)
	}
	if got := rtps.ClassifyUnicastInfoDst(plain, local); got != rtps.InfoDstAbsent {
		t.Fatalf("plain = %d, want absent", got)
	}
	zeroRaw := rtps.RawSubmessage{ID: rtps.SubmessageInfoDst, Flags: 0x01, Body: nil}
	zeroMsg, err := rtps.EncodeMessage(rtps.DefaultHeader(testPrefix()), []rtps.RawSubmessage{zeroRaw})
	if err != nil {
		t.Fatalf("encode zero: %v", err)
	}
	if got := rtps.ClassifyUnicastInfoDst(zeroMsg, local); got != rtps.InfoDstAbsent {
		t.Fatalf("zero-length = %d, want absent", got)
	}
	matchRaw := rtps.RawSubmessage{ID: rtps.SubmessageInfoDst, Flags: 0x01, Body: append([]byte(nil), local[:]...)}
	matchMsg, err := rtps.EncodeMessage(rtps.DefaultHeader(testPrefix()), []rtps.RawSubmessage{matchRaw})
	if err != nil {
		t.Fatalf("encode match: %v", err)
	}
	if got := rtps.ClassifyUnicastInfoDst(matchMsg, local); got != rtps.InfoDstAbsent {
		t.Fatalf("match = %d, want absent", got)
	}
	var other [12]byte
	other[0] = 0xFF
	mismatchRaw := rtps.RawSubmessage{ID: rtps.SubmessageInfoDst, Flags: 0x01, Body: append([]byte(nil), other[:]...)}
	mismatchMsg, err := rtps.EncodeMessage(rtps.DefaultHeader(testPrefix()), []rtps.RawSubmessage{mismatchRaw})
	if err != nil {
		t.Fatalf("encode mismatch: %v", err)
	}
	if got := rtps.ClassifyUnicastInfoDst(mismatchMsg, local); got != rtps.InfoDstMismatch {
		t.Fatalf("mismatch = %d, want mismatch", got)
	}
	malformedRaw := rtps.RawSubmessage{ID: rtps.SubmessageInfoDst, Flags: 0x01, Body: []byte{0x01, 0x02, 0x03}}
	malformedMsg, err := rtps.EncodeMessage(rtps.DefaultHeader(testPrefix()), []rtps.RawSubmessage{malformedRaw})
	if err != nil {
		t.Fatalf("encode malformed: %v", err)
	}
	if got := rtps.ClassifyUnicastInfoDst(malformedMsg, local); got != rtps.InfoDstMalformed {
		t.Fatalf("malformed = %d, want malformed", got)
	}
	if got := rtps.ClassifyUnicastInfoDst([]byte{0x00, 0x01}, local); got != rtps.InfoDstMalformed {
		t.Fatalf("undecodable = %d, want malformed", got)
	}
}

// TestKnownInvalidCallerDropsRest documents rule 6 per 8.3.4.1.
func TestKnownInvalidCallerDropsRest(t *testing.T) {
	t.Parallel()

	// A known but invalid submessage fails loudly; the caller must drop the
	// rest of the message per DDSI-RTPS 2.5 8.3.4.1 rule 6 instead of
	// continuing to later submessages.
	raw := rtps.RawSubmessage{ID: rtps.SubmessageHeartbeat, Flags: 0x01, Body: []byte{0x01}}
	var drops rtps.Counter
	if _, err := rtps.DecodeSubmessage(raw, &drops); err == nil {
		t.Fatalf("known-invalid heartbeat succeeded, want error")
	}
	if drops.Count() != 1 {
		t.Fatalf("drops = %d, want 1", drops.Count())
	}
}

// TestInfoSrcVendorFull pins the full 2-byte vendor id per 8.3.8.10.
func TestInfoSrcVendorFull(t *testing.T) {
	t.Parallel()

	// Both vendor bytes ride verbatim; a single-byte truncation would lose 0xCD.
	// TODO: backend_test.go also decodes INFO_SRC and is owned elsewhere; keep its
	// expectations on [2]byte vendor in sync without editing it here.
	src := rtps.InfoSrc{Major: 2, Minor: 5, Vendor: [2]byte{0xAB, 0xCD}}
	body := src.Encode()
	if len(body) != 5 || body[3] != 0xAB || body[4] != 0xCD {
		t.Fatalf("info_src bytes = % X, want vendor AB CD", body)
	}
	var drops rtps.Counter
	back, err := rtps.DecodeInfoSrc(body, 0x01, &drops)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if back.Vendor != src.Vendor {
		t.Fatalf("vendor = % X, want AB CD", back.Vendor)
	}
}

// splitTestDatagram walks one framed datagram into raw submessages for
// placement tests, mirroring the wire header layout.
func splitTestDatagram(t *testing.T, msg []byte) []rtps.RawSubmessage {
	t.Helper()

	if len(msg) < 20 {
		t.Fatalf("datagram len = %d, want at least 20", len(msg))
	}
	var out []rtps.RawSubmessage
	off := 20
	for off < len(msg) {
		if len(msg)-off < 4 {
			t.Fatalf("submessage header truncated at %d", off)
		}
		id := msg[off]
		flags := msg[off+1]
		var ln int
		if flags&0x01 != 0 {
			ln = int(binary.LittleEndian.Uint16(msg[off+2 : off+4]))
		} else {
			ln = int(binary.BigEndian.Uint16(msg[off+2 : off+4]))
		}
		if off+4+ln > len(msg) {
			t.Fatalf("submessage %#02x overruns datagram", id)
		}
		body := append([]byte(nil), msg[off+4:off+4+ln]...)
		out = append(out, rtps.RawSubmessage{ID: id, Flags: flags, Body: body})
		off += 4 + ln
	}
	return out
}

// TestDecodeSubmessageHeaderExtension pins typed HE dispatch per 8.3.7.3.
func TestDecodeSubmessageHeaderExtension(t *testing.T) {
	t.Parallel()

	if !rtps.IsKnownSubmessageID(rtps.SubmessageHeaderExtension) {
		t.Fatalf("HE %#02x not known, want known per 8.3.4", rtps.SubmessageHeaderExtension)
	}
	framed, err := rtps.EncodeHeaderExtension(rtps.HeaderExtension{LittleEndian: true})
	if err != nil {
		t.Fatalf("encode HE: %v", err)
	}
	raw := rtps.RawSubmessage{ID: rtps.SubmessageHeaderExtension, Flags: 0x01, Body: framed[4:]}
	var drops rtps.Counter
	got, err := rtps.DecodeSubmessage(raw, &drops)
	if err != nil {
		t.Fatalf("dispatch valid HE: %v", err)
	}
	if got.HeaderExtension == nil {
		t.Fatalf("header extension missing after typed decode")
	}
	if drops.Count() != 0 {
		t.Fatalf("drops = %d, want 0", drops.Count())
	}

	// Malformed HE fails so callers invalidate the rest per 8.3.4.1 rule 6.
	bad := rtps.RawSubmessage{ID: rtps.SubmessageHeaderExtension, Flags: 0x03, Body: []byte{0x01}}
	var badDrops rtps.Counter
	if _, err := rtps.DecodeSubmessage(bad, &badDrops); err == nil {
		t.Fatalf("malformed HE succeeded, want error")
	}
	if badDrops.Count() != 1 {
		t.Fatalf("bad drops = %d, want 1", badDrops.Count())
	}

	// Caller walk breaks on the known-invalid HE instead of continuing to
	// the trailing DATA.
	trailing, err := testData().Submessage(true)
	if err != nil {
		t.Fatalf("data submessage: %v", err)
	}
	walk := []rtps.RawSubmessage{bad, trailing}
	broke := false
	for _, r := range walk {
		if _, err := rtps.DecodeSubmessage(r, &badDrops); err != nil {
			broke = true
			break
		}
	}
	if !broke {
		t.Fatalf("walk continued past malformed HE, want rule-6 break")
	}
}

// TestValidateHeaderExtensionPlacement pins first-position plus checksum per 8.3.7.3.
func TestValidateHeaderExtensionPlacement(t *testing.T) {
	t.Parallel()

	hdr := rtps.DefaultHeader(testPrefix())
	ext := rtps.HeaderExtension{LittleEndian: true, Checksum: rtps.TCPChecksumCRC32}
	msg, err := rtps.EncodeTCPMessage(hdr, &ext, checksumTestSubs())
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	raws := splitTestDatagram(t, msg)
	var drops rtps.Counter
	if err := rtps.ValidateHeaderExtensionPlacement(msg, raws, &drops); err != nil {
		t.Fatalf("valid first HE: %v", err)
	}
	if drops.Count() != 0 {
		t.Fatalf("drops = %d, want 0", drops.Count())
	}
}

// TestValidateHeaderExtensionChecksum pins checksum fail-closed per 8.3.7.3.
func TestValidateHeaderExtensionChecksum(t *testing.T) {
	t.Parallel()

	hdr := rtps.DefaultHeader(testPrefix())
	ext := rtps.HeaderExtension{LittleEndian: true, Checksum: rtps.TCPChecksumCRC32}
	msg, err := rtps.EncodeTCPMessage(hdr, &ext, checksumTestSubs())
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	raws := splitTestDatagram(t, msg)

	bad := append([]byte(nil), msg...)
	bad[len(bad)-1] ^= 0xFF
	var tamperDrops rtps.Counter
	if err := rtps.ValidateHeaderExtensionPlacement(bad, raws, &tamperDrops); err == nil {
		t.Fatalf("tampered checksum succeeded, want error")
	}
	if tamperDrops.Count() != 1 {
		t.Fatalf("tamper drops = %d, want 1", tamperDrops.Count())
	}
}

// TestValidateHeaderExtensionMispositioned pins not-first fail-closed per 8.3.7.3.
func TestValidateHeaderExtensionMispositioned(t *testing.T) {
	t.Parallel()

	hdr := rtps.DefaultHeader(testPrefix())
	// Well-formed HE in a later position drops as mispositioned.
	empty, err := rtps.EncodeHeaderExtension(rtps.HeaderExtension{LittleEndian: true})
	if err != nil {
		t.Fatalf("encode empty HE: %v", err)
	}
	stamp := rtps.InfoTS{Valid: true, Seconds: 1, Fraction: 2}
	misplaced := []rtps.RawSubmessage{
		{ID: rtps.SubmessageInfoTS, Flags: stamp.Flags(true), Body: stamp.Encode(true)},
		{ID: rtps.SubmessageHeaderExtension, Flags: 0x01, Body: empty[4:]},
	}
	var wire []byte
	raw := hdr.Encode()
	wire = append(wire, raw[:]...)
	for _, s := range misplaced {
		f, err := rtps.WrapSubmessage(s.ID, s.Flags, s.Body)
		if err != nil {
			t.Fatalf("wrap: %v", err)
		}
		wire = append(wire, f...)
	}
	var misDrops rtps.Counter
	if err := rtps.ValidateHeaderExtensionPlacement(wire, misplaced, &misDrops); err == nil {
		t.Fatalf("mispositioned HE succeeded, want error")
	}
	if misDrops.Count() != 1 {
		t.Fatalf("misplaced drops = %d, want 1", misDrops.Count())
	}

	// Malformed HE body drops fail-closed.
	malformed := []rtps.RawSubmessage{
		{ID: rtps.SubmessageHeaderExtension, Flags: 0x03, Body: []byte{0x01}},
	}
	var malDrops rtps.Counter
	if err := rtps.ValidateHeaderExtensionPlacement(wire, malformed, &malDrops); err == nil {
		t.Fatalf("malformed HE succeeded, want error")
	}
	if malDrops.Count() != 1 {
		t.Fatalf("malformed drops = %d, want 1", malDrops.Count())
	}
}
