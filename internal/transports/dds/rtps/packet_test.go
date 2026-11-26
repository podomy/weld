// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

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
		0x02, 0x05, 0x01, 0x10,
		0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C,
	}
	if !bytes.Equal(got[:], want) {
		t.Fatalf("header = % X, want % X", got, want)
	}

	back, err := rtps.DecodeHeader(got[:])
	if err != nil {
		t.Fatalf("decode header: %v", err)
	}
	if back.Major != 2 || back.Minor != 5 {
		t.Fatalf("version = %d.%d, want 2.5", back.Major, back.Minor)
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

// checksumTestSubs returns one INFO_TS submessage for checksum datagrams.
func checksumTestSubs() []rtps.RawSubmessage {
	stamp := rtps.InfoTS{Valid: true, Seconds: 42, Fraction: 7}
	return []rtps.RawSubmessage{
		{ID: rtps.SubmessageInfoTS, Flags: stamp.Flags(true), Body: stamp.Encode(true)},
	}
}

// TestHeaderExtensionChecksumValid pins UDP checksum accept per 8.3.7.
func TestHeaderExtensionChecksumValid(t *testing.T) {
	t.Parallel()

	hdr := rtps.DefaultHeader(testPrefix())
	ext := rtps.HeaderExtension{LittleEndian: true, Checksum: rtps.TCPChecksumCRC32}
	msg, err := rtps.EncodeTCPMessage(hdr, &ext, checksumTestSubs())
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	var drops rtps.Counter
	_, subs, err := rtps.DecodeMessage(msg, &drops)
	if err != nil {
		t.Fatalf("decode valid checksum: %v", err)
	}
	if len(subs) != 2 {
		t.Fatalf("submessages = %d, want 2", len(subs))
	}
	if subs[0].ID != rtps.SubmessageHeaderExtension {
		t.Fatalf("first id = %#02x, want header extension", subs[0].ID)
	}
	if drops.Count() != 0 {
		t.Fatalf("drops = %d, want 0", drops.Count())
	}
}

// TestHeaderExtensionChecksumTampered pins fail-closed drop on mismatch.
func TestHeaderExtensionChecksumTampered(t *testing.T) {
	t.Parallel()

	hdr := rtps.DefaultHeader(testPrefix())
	ext := rtps.HeaderExtension{LittleEndian: true, Checksum: rtps.TCPChecksumCRC32}
	msg, err := rtps.EncodeTCPMessage(hdr, &ext, checksumTestSubs())
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	bad := append([]byte(nil), msg...)
	bad[len(bad)-1] ^= 0xFF
	var drops rtps.Counter
	if _, _, err := rtps.DecodeMessage(bad, &drops); err == nil {
		t.Fatalf("tampered checksum succeeded, want error")
	}
	if drops.Count() != 1 {
		t.Fatalf("drops = %d, want 1", drops.Count())
	}
}

// heFramed encodes one HeaderExtension body without length auto-fill per
// 8.3.7.3, so wrong lengths can be pinned fail-closed.
func heFramed(t *testing.T, ext rtps.HeaderExtension) []byte {
	t.Helper()

	framed, err := rtps.EncodeHeaderExtension(ext)
	if err != nil {
		t.Fatalf("encode extension: %v", err)
	}
	return framed
}

// heDatagram prefixes a header before one framed extension plus subs.
func heDatagram(t *testing.T, framed []byte, subs []rtps.RawSubmessage) []byte {
	t.Helper()

	raw := rtps.DefaultHeader(testPrefix()).Encode()
	out := make([]byte, 0, len(raw)+len(framed)+len(subs)*8)
	out = append(out, raw[:]...)
	out = append(out, framed...)
	for _, s := range subs {
		f, err := rtps.WrapSubmessage(s.ID, s.Flags, s.Body)
		if err != nil {
			t.Fatalf("wrap sub: %v", err)
		}
		out = append(out, f...)
	}
	return out
}

// TestHeaderExtensionNotFirstRejects pins fail-closed on a valid HE at index 1 per 8.3.7.3.
func TestHeaderExtensionNotFirstRejects(t *testing.T) {
	t.Parallel()

	empty, err := rtps.EncodeHeaderExtension(rtps.HeaderExtension{LittleEndian: true})
	if err != nil {
		t.Fatalf("encode empty HE: %v", err)
	}
	msg := heDatagram(t, mustWrapInfoTS(t), []rtps.RawSubmessage{
		{ID: 0x00, Flags: 0x01, Body: empty[4:]},
	})
	var drops rtps.Counter
	if _, _, err := rtps.DecodeMessage(msg, &drops); err == nil {
		t.Fatalf("HE second succeeded, want not-first error")
	}
	if drops.Count() != 1 {
		t.Fatalf("drops = %d, want 1", drops.Count())
	}
}

// mustWrapInfoTS returns one framed INFO_TS for HE position tests.
func mustWrapInfoTS(t *testing.T) []byte {
	t.Helper()

	stamp := rtps.InfoTS{Valid: true, Seconds: 1, Fraction: 2}
	framed, err := rtps.WrapSubmessage(rtps.SubmessageInfoTS, stamp.Flags(true), stamp.Encode(true))
	if err != nil {
		t.Fatalf("wrap info_ts: %v", err)
	}
	return framed
}

// TestHeaderExtensionLaterInvalidRejects pins every later HE validated fail-closed per 8.3.7.3.
func TestHeaderExtensionLaterInvalidRejects(t *testing.T) {
	t.Parallel()

	// Body claims length present but carries one byte: short fail-closed.
	msg := heDatagram(t, mustWrapInfoTS(t), []rtps.RawSubmessage{
		{ID: 0x00, Flags: 0x03, Body: []byte{0x01}},
	})
	var drops rtps.Counter
	if _, _, err := rtps.DecodeMessage(msg, &drops); err == nil {
		t.Fatalf("invalid later HE succeeded, want error")
	}
	if drops.Count() != 1 {
		t.Fatalf("drops = %d, want 1", drops.Count())
	}
}

// TestHeaderExtensionShortRejects pins first HE short body fail-closed per 8.3.7.3.
func TestHeaderExtensionShortRejects(t *testing.T) {
	t.Parallel()

	// Truncated HE: flags length present but body one byte.
	raw := rtps.DefaultHeader(testPrefix()).Encode()
	bad, err := rtps.WrapSubmessage(0x00, 0x03, []byte{0x01})
	if err != nil {
		t.Fatalf("wrap: %v", err)
	}
	full := append(raw[:], bad...)
	var drops rtps.Counter
	if _, _, err := rtps.DecodeMessage(full, &drops); err == nil {
		t.Fatalf("short HE succeeded, want error")
	}
	if drops.Count() != 1 {
		t.Fatalf("drops = %d, want 1", drops.Count())
	}
}

// TestHeaderExtensionParamRejects pins malformed HE params fail-closed per 8.3.7.3 and 9.4.2.11.
func TestHeaderExtensionParamRejects(t *testing.T) {
	t.Parallel()

	raw := rtps.DefaultHeader(testPrefix()).Encode()
	// Params flag set but body holds only two bytes: truncated param header.
	bad, err := rtps.WrapSubmessage(0x00, 0x81, []byte{0x01, 0x00})
	if err != nil {
		t.Fatalf("wrap: %v", err)
	}
	full := append(raw[:], bad...)
	var drops rtps.Counter
	if _, _, err := rtps.DecodeMessage(full, &drops); err == nil {
		t.Fatalf("bad HE params succeeded, want error")
	}
	if drops.Count() != 1 {
		t.Fatalf("drops = %d, want 1", drops.Count())
	}
}

// TestHeaderExtensionLengthMismatch pins MessageLength equality per 8.3.7.3 on UDP.
func TestHeaderExtensionLengthMismatch(t *testing.T) {
	t.Parallel()

	hdr := rtps.DefaultHeader(testPrefix())
	good, err := rtps.EncodeTCPMessage(hdr, &rtps.HeaderExtension{LittleEndian: true, HasLength: true}, checksumTestSubs())
	if err != nil {
		t.Fatalf("encode good: %v", err)
	}
	var drops rtps.Counter
	if _, _, err := rtps.DecodeMessage(good, &drops); err != nil {
		t.Fatalf("valid length: %v", err)
	}
	// Mismatch by one: differs fail-closed.
	off := len(good)
	badLen := uint32(off + 1) //nolint:gosec // G115: test datagram length fits uint32
	badExt := rtps.HeaderExtension{LittleEndian: true, HasLength: true, MessageLength: badLen}
	bad := heDatagram(t, heFramed(t, badExt), checksumTestSubs())
	var badDrops rtps.Counter
	if _, _, err := rtps.DecodeMessage(bad, &badDrops); err == nil {
		t.Fatalf("length mismatch succeeded, want error")
	}
	if badDrops.Count() != 1 {
		t.Fatalf("drops = %d, want 1", badDrops.Count())
	}
}

// TestHeaderExtensionLengthWildlyExcessive pins wildly excessive reject per 8.3.7.3.
func TestHeaderExtensionLengthWildlyExcessive(t *testing.T) {
	t.Parallel()

	tail := checksumTestSubs()
	// Placeholder to learn datagram length without length field.
	probe := heDatagram(t, heFramed(t, rtps.HeaderExtension{LittleEndian: true}), tail)
	huge := uint32(len(probe) + 0xFFFF + 1) //nolint:gosec // G115: test datagram length fits uint32
	wild := rtps.HeaderExtension{LittleEndian: true, HasLength: true, MessageLength: huge}
	msg := heDatagram(t, heFramed(t, wild), tail)
	var drops rtps.Counter
	if _, _, err := rtps.DecodeMessage(msg, &drops); err == nil {
		t.Fatalf("wildly excessive length succeeded, want error")
	}
	if drops.Count() != 1 {
		t.Fatalf("drops = %d, want 1", drops.Count())
	}
}

// TestHeaderExtensionLengthTooSmall pins lower-bound reject per 8.3.7.3.
func TestHeaderExtensionLengthTooSmall(t *testing.T) {
	t.Parallel()

	small := rtps.HeaderExtension{LittleEndian: true, HasLength: true, MessageLength: 10}
	msg := heDatagram(t, heFramed(t, small), checksumTestSubs())
	var drops rtps.Counter
	if _, _, err := rtps.DecodeMessage(msg, &drops); err == nil {
		t.Fatalf("too small length succeeded, want error")
	}
	if drops.Count() != 1 {
		t.Fatalf("drops = %d, want 1", drops.Count())
	}
}

// TestUDPOctetsToNextHeaderExtendsToEnd pins zero length consumes tail per 9.4.5.1.3.
func TestUDPOctetsToNextHeaderExtendsToEnd(t *testing.T) {
	t.Parallel()

	stamp := rtps.InfoTS{Valid: true, Seconds: 0x11223344, Fraction: 0xAABBCCDD}
	body := stamp.Encode(true)
	framed, err := rtps.WrapSubmessage(rtps.SubmessageInfoTS, 0x01, body)
	if err != nil {
		t.Fatalf("wrap: %v", err)
	}
	// Rewrite length to zero: extends to end per 9.4.5.1.3.
	framed[2] = 0x00
	framed[3] = 0x00
	raw := rtps.DefaultHeader(testPrefix()).Encode()
	msg := append(raw[:], framed...)
	var drops rtps.Counter
	_, subs, err := rtps.DecodeMessage(msg, &drops)
	if err != nil {
		t.Fatalf("zero length: %v", err)
	}
	if len(subs) != 1 {
		t.Fatalf("submessages = %d, want 1", len(subs))
	}
	if len(subs[0].Body) != 8 {
		t.Fatalf("tail body len = %d, want 8", len(subs[0].Body))
	}
	decoded, err := rtps.DecodeSubmessage(subs[0], &drops)
	if err != nil {
		t.Fatalf("decode tail: %v", err)
	}
	if decoded.InfoTS == nil || decoded.InfoTS.Seconds != 0x11223344 {
		t.Fatalf("tail info_ts = %+v, want seconds 0x11223344", decoded.InfoTS)
	}
	if drops.Count() != 0 {
		t.Fatalf("drops = %d, want 0", drops.Count())
	}
}
