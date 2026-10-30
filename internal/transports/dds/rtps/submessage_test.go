// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package rtps_test

import (
	"bytes"
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
	back, err := rtps.DecodeHeartbeat(append(got, 0xFF, 0xFF), beat.Flags(true, true, false), &drops)
	if err != nil {
		t.Fatalf("decode with trailing: %v", err)
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
		rtps.SubmessagePad:       true,
		rtps.SubmessageAckNack:   true,
		rtps.SubmessageHeartbeat: true,
		rtps.SubmessageGap:       true,
		rtps.SubmessageInfoTS:    true,
		rtps.SubmessageData:      true,
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

// TestDecodeSubmessageErrors checks truncated typed kinds drop loudly.
func TestDecodeSubmessageErrors(t *testing.T) {
	t.Parallel()

	cases := []rtps.RawSubmessage{
		{ID: rtps.SubmessageData, Flags: 0x05, Body: []byte{0x01}},
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
