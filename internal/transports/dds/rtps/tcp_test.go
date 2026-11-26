// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

package rtps_test

import (
	"testing"

	"github.com/podomy/weld/internal/transports/dds/rtps"
)

// tcpTestPrefix returns a fixed GUID prefix for TCP framing tests.
func tcpTestPrefix() [12]byte {
	return [12]byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C}
}

// tcpInfoTS returns one INFO_TS submessage for TCP frames.
func tcpInfoTS() rtps.RawSubmessage {
	stamp := rtps.InfoTS{Valid: true, Seconds: 1, Fraction: 2}
	return rtps.RawSubmessage{ID: rtps.SubmessageInfoTS, Flags: stamp.Flags(true), Body: stamp.Encode(true)}
}

// TestTCPFirstExtensionValid pins a leading HE decodes per 9.4.5.1.3 and 8.3.7.3.
func TestTCPFirstExtensionValid(t *testing.T) {
	t.Parallel()

	hdr := rtps.DefaultHeader(tcpTestPrefix())
	ext := rtps.HeaderExtension{LittleEndian: true, HasLength: true}
	frame, err := rtps.EncodeTCPMessage(hdr, &ext, []rtps.RawSubmessage{tcpInfoTS()})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	var drops rtps.TCPDropCounter
	gotHdr, gotExt, subs, err := rtps.DecodeTCPMessage(frame, &drops)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if gotHdr.Prefix != tcpTestPrefix() {
		t.Fatalf("prefix mismatch")
	}
	if gotExt == nil || !gotExt.HasLength || gotExt.MessageLength != uint32(len(frame)) { //nolint:gosec // G115: test frame length fits uint32
		t.Fatalf("ext = %+v len %d, want length %d", gotExt, len(frame), len(frame))
	}
	if len(subs) != 1 || subs[0].ID != rtps.SubmessageInfoTS {
		t.Fatalf("subs = %d, want 1 INFO_TS", len(subs))
	}
	if drops.Count() != 0 {
		t.Fatalf("drops = %d, want 0", drops.Count())
	}
}

// TestTCPLaterExtensionNotFirstRejects pins a valid later HE rejected mispositioned per 8.3.7.3.
func TestTCPLaterExtensionNotFirstRejects(t *testing.T) {
	t.Parallel()

	empty, err := rtps.EncodeHeaderExtension(rtps.HeaderExtension{LittleEndian: true})
	if err != nil {
		t.Fatalf("encode HE: %v", err)
	}
	// INFO_TS first, valid empty HE second: well-formed but mispositioned.
	msg, err := rtps.EncodeMessage(rtps.DefaultHeader(tcpTestPrefix()), []rtps.RawSubmessage{
		tcpInfoTS(),
		{ID: 0x00, Flags: 0x01, Body: empty[4:]},
	})
	if err != nil {
		t.Fatalf("encode message: %v", err)
	}
	var drops rtps.TCPDropCounter
	if _, _, _, err := rtps.DecodeTCPMessage(msg, &drops); err == nil {
		t.Fatalf("later valid HE succeeded, want not-first error")
	}
	if drops.Count() != 1 {
		t.Fatalf("drops = %d, want 1", drops.Count())
	}
}

// TestTCPLaterExtensionInvalidRejects pins every later HE validated, not passed through per 8.3.7.3.
func TestTCPLaterExtensionInvalidRejects(t *testing.T) {
	t.Parallel()

	// Later HE claims length present but carries one byte: malformed.
	msg, err := rtps.EncodeMessage(rtps.DefaultHeader(tcpTestPrefix()), []rtps.RawSubmessage{
		tcpInfoTS(),
		{ID: 0x00, Flags: 0x03, Body: []byte{0x01}},
	})
	if err != nil {
		t.Fatalf("encode message: %v", err)
	}
	var drops rtps.TCPDropCounter
	if _, _, _, err := rtps.DecodeTCPMessage(msg, &drops); err == nil {
		t.Fatalf("later invalid HE succeeded, want error")
	}
	if drops.Count() != 1 {
		t.Fatalf("drops = %d, want 1", drops.Count())
	}
}

// TestTCPMessageLengthMismatch pins exact equality per 8.3.7.3 on TCP.
func TestTCPMessageLengthMismatch(t *testing.T) {
	t.Parallel()

	hdr := rtps.DefaultHeader(tcpTestPrefix())
	good, err := rtps.EncodeTCPMessage(hdr, &rtps.HeaderExtension{LittleEndian: true, HasLength: true}, []rtps.RawSubmessage{tcpInfoTS()})
	if err != nil {
		t.Fatalf("encode good: %v", err)
	}
	// Tamper the length field (offset 20+4) by one: differs fail-closed.
	bad := append([]byte(nil), good...)
	off := rtps.HeaderSize + 4
	bad[off]++
	var drops rtps.TCPDropCounter
	if _, _, _, err := rtps.DecodeTCPMessage(bad, &drops); err == nil {
		t.Fatalf("length mismatch succeeded, want error")
	}
	if drops.Count() != 1 {
		t.Fatalf("drops = %d, want 1", drops.Count())
	}
}

// TestTCPZeroLengthExtends pins zero octetsToNextHeader extends to end per 9.4.5.1.3.
func TestTCPZeroLengthExtends(t *testing.T) {
	t.Parallel()

	hdr := rtps.DefaultHeader(tcpTestPrefix())
	base, err := rtps.EncodeTCPMessage(hdr, nil, []rtps.RawSubmessage{tcpInfoTS()})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	// Rewrite the INFO_TS length to zero; body is the tail.
	zeroed := append([]byte(nil), base...)
	zeroed[rtps.HeaderSize+2] = 0x00
	zeroed[rtps.HeaderSize+3] = 0x00
	var drops rtps.TCPDropCounter
	_, _, subs, err := rtps.DecodeTCPMessage(zeroed, &drops)
	if err != nil {
		t.Fatalf("zero length: %v", err)
	}
	if len(subs) != 1 || len(subs[0].Body) != 8 {
		t.Fatalf("subs = %d body %d, want 1 and 8", len(subs), len(subs[0].Body))
	}
}
