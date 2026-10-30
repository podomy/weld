// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package rtps_test

import (
	"bytes"
	"math/rand"
	"testing"

	"github.com/podomy/weld/internal/transports/dds/rtps"
)

// testPrefix returns a fixed GUID prefix for golden tests.
func testPrefix() [12]byte {
	return [12]byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C}
}

// TestHeaderGoldenBytes pins the exact 20 byte header on the wire.
func TestHeaderGoldenBytes(t *testing.T) {
	t.Parallel()

	got := rtps.DefaultHeader(testPrefix()).Encode()
	want := []byte{
		0x52, 0x54, 0x50, 0x53,
		0x02, 0x01, 0x01, 0x10,
		0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C,
	}
	if !bytes.Equal(got[:], want) {
		t.Fatalf("header = % X, want % X", got, want)
	}

	back, err := rtps.DecodeHeader(got[:])
	if err != nil {
		t.Fatalf("decode header: %v", err)
	}
	if back.Major != 2 || back.Minor != 1 {
		t.Fatalf("version = %d.%d, want 2.1", back.Major, back.Minor)
	}
	if back.Vendor != rtps.DefaultVendorID() {
		t.Fatalf("vendor = % X", back.Vendor)
	}
	if back.Prefix != testPrefix() {
		t.Fatalf("prefix = % X", back.Prefix)
	}
}

// TestHeaderDecodeErrors checks magic, version, and length validation.
func TestHeaderDecodeErrors(t *testing.T) {
	t.Parallel()

	full := rtps.DefaultHeader(testPrefix()).Encode()

	badMagic := full
	badMagic[0] = 'X'
	if _, err := rtps.DecodeHeader(badMagic[:]); err == nil {
		t.Fatalf("bad magic succeeded, want error")
	}

	badMajor := full
	badMajor[4] = 3
	if _, err := rtps.DecodeHeader(badMajor[:]); err == nil {
		t.Fatalf("major 3 succeeded, want error")
	}

	if _, err := rtps.DecodeHeader(full[:19]); err == nil {
		t.Fatalf("short header succeeded, want error")
	}

	for _, minor := range []uint8{1, 3, 5} {
		other := full
		other[5] = minor
		back, err := rtps.DecodeHeader(other[:])
		if err != nil {
			t.Fatalf("minor %d: %v", minor, err)
		}
		if back.Minor != minor {
			t.Fatalf("minor = %d, want %d", back.Minor, minor)
		}
	}
}

// TestSubmessageHeaderEndian checks length encoding in both orders.
func TestSubmessageHeaderEndian(t *testing.T) {
	t.Parallel()

	checkSubmessageHeaderOrder(t, true)
	checkSubmessageHeaderOrder(t, false)

	if _, err := rtps.DecodeSubmessageHeader([]byte{0x15, 0x01, 0x02}); err == nil {
		t.Fatalf("short submessage header succeeded, want error")
	}
}

// checkSubmessageHeaderOrder round trips one length in one order.
func checkSubmessageHeaderOrder(t *testing.T, littleEndian bool) {
	t.Helper()

	var flags uint8
	if littleEndian {
		flags = 0x01
	}
	hdr := rtps.SubmessageHeader{ID: 0x15, Flags: flags, Length: 0x1234}
	raw := hdr.Encode()
	back, err := rtps.DecodeSubmessageHeader(raw[:])
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if back != hdr {
		t.Fatalf("round trip = %+v, want %+v", back, hdr)
	}
	checkSubmessageHeaderBytes(t, raw, littleEndian)
}

// checkSubmessageHeaderBytes pins the encoded length byte order.
func checkSubmessageHeaderBytes(t *testing.T, raw [4]byte, littleEndian bool) {
	t.Helper()

	if littleEndian && (raw[2] != 0x34 || raw[3] != 0x12) {
		t.Fatalf("LE length = %02x %02x, want 34 12", raw[2], raw[3])
	}
	if !littleEndian && (raw[2] != 0x12 || raw[3] != 0x34) {
		t.Fatalf("BE length = %02x %02x, want 12 34", raw[2], raw[3])
	}
}

// buildTestMessage returns a header plus INFO_TS plus DATA datagram.
func buildTestMessage(t *testing.T) []byte {
	t.Helper()

	stamp := rtps.InfoTS{Valid: true, Seconds: 42, Fraction: 7}
	data := rtps.Data{
		ReaderID:       rtps.EntityUnknown,
		WriterID:       rtps.EntitySPDPWriter,
		SequenceNumber: 1,
		Payload:        []byte{0x00, 0x01, 0x00, 0x00, 0x41},
	}
	sub, err := data.Submessage(true)
	if err != nil {
		t.Fatalf("data submessage: %v", err)
	}
	msg, err := rtps.EncodeMessage(rtps.DefaultHeader(testPrefix()), []rtps.RawSubmessage{
		{ID: rtps.SubmessageInfoTS, Flags: stamp.Flags(true), Body: stamp.Encode(true)},
		sub,
	})
	if err != nil {
		t.Fatalf("encode message: %v", err)
	}
	return msg
}

// TestEncodeDecodeMessageRoundTrip checks framing preserves submessages.
func TestEncodeDecodeMessageRoundTrip(t *testing.T) {
	t.Parallel()

	msg := buildTestMessage(t)
	var drops rtps.Counter
	hdr, subs, err := rtps.DecodeMessage(msg, &drops)
	if err != nil {
		t.Fatalf("decode message: %v", err)
	}
	if hdr.Prefix != testPrefix() {
		t.Fatalf("prefix = % X", hdr.Prefix)
	}
	if len(subs) != 2 {
		t.Fatalf("submessages = %d, want 2", len(subs))
	}
	if subs[0].ID != rtps.SubmessageInfoTS || subs[1].ID != rtps.SubmessageData {
		t.Fatalf("ids = %#02x %#02x", subs[0].ID, subs[1].ID)
	}
	if drops.Count() != 0 {
		t.Fatalf("drops = %d, want 0", drops.Count())
	}
}

// TestDecodeMessageTruncatedFuzz drops every non boundary truncation.
func TestDecodeMessageTruncatedFuzz(t *testing.T) {
	t.Parallel()

	msg := buildTestMessage(t)
	infoEnd := rtps.HeaderSize + rtps.SubmessageHeaderSize + 8
	valid := map[int]bool{rtps.HeaderSize: true, infoEnd: true}

	var drops rtps.Counter
	for i := range msg {
		_, _, err := rtps.DecodeMessage(msg[:i], &drops)
		if valid[i] {
			if err != nil {
				t.Fatalf("truncation %d: %v, want valid prefix", i, err)
			}
			continue
		}
		if err == nil {
			t.Fatalf("truncation %d succeeded, want error", i)
		}
	}
	want := uint64(len(msg) - len(valid)) //nolint:gosec // G115: message is longer than its two valid prefixes
	if drops.Count() != want {
		t.Fatalf("drops = %d, want %d", drops.Count(), want)
	}

	if _, _, err := rtps.DecodeMessage(msg[:10], nil); err == nil {
		t.Fatalf("nil counter truncation succeeded, want error")
	}
}

// TestDecodeMessageRandomFuzz feeds random bytes and requires no panic.
func TestDecodeMessageRandomFuzz(t *testing.T) {
	t.Parallel()

	rng := rand.New(rand.NewSource(1)) //nolint:gosec // G404: fixed seed keeps the fuzz reproducible, not security use
	var drops rtps.Counter
	var errs int
	for range 2000 {
		buf := make([]byte, rng.Intn(65))
		if _, err := rng.Read(buf); err != nil {
			t.Fatalf("rand read: %v", err)
		}
		if _, _, err := rtps.DecodeMessage(buf, &drops); err != nil {
			errs++
		}
	}
	if errs == 0 {
		t.Fatalf("random fuzz hit no errors, want some")
	}
	if drops.Count() != uint64(errs) {
		t.Fatalf("drops = %d, errs = %d", drops.Count(), errs)
	}
}

// TestWrapSubmessageTooLarge rejects bodies above 64K loudly.
func TestWrapSubmessageTooLarge(t *testing.T) {
	t.Parallel()

	if _, err := rtps.WrapSubmessage(rtps.SubmessageData, 0x01, make([]byte, 0x10000)); err == nil {
		t.Fatalf("oversize wrap succeeded, want error")
	}
}

// TestCounterNilSafe checks nil counters never panic and read zero.
func TestCounterNilSafe(t *testing.T) {
	t.Parallel()

	var nilCounter *rtps.Counter
	nilCounter.RecordDrop()
	if nilCounter.Count() != 0 {
		t.Fatalf("nil count = %d, want 0", nilCounter.Count())
	}

	var live rtps.Counter
	live.RecordDrop()
	live.RecordDrop()
	if live.Count() != 2 {
		t.Fatalf("count = %d, want 2", live.Count())
	}
}

// TestEncodeMessageOversize rejects a submessage body above 64K.
func TestEncodeMessageOversize(t *testing.T) {
	t.Parallel()

	big := make([]byte, 0x10000)
	msg, err := rtps.EncodeMessage(rtps.DefaultHeader(testPrefix()), []rtps.RawSubmessage{
		{ID: rtps.SubmessageData, Flags: 0x05, Body: big},
	})
	if err == nil {
		t.Fatalf("oversize encode succeeded with %d bytes, want error", len(msg))
	}
}
