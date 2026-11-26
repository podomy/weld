// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1
package types

import (
	"testing"
)

// checkEnvelopeV2Header verifies V2 emission plus Table 57 parsing.
func checkEnvelopeV2Header(t *testing.T, le bool) {
	t.Helper()
	app := MarshalMessageWithExt([]byte{0x01}, ExtAppendable, le)
	if app[0] != 0x00 {
		t.Fatalf("le=%v first byte %#02x", le, app[0])
	}
	want := Repxt6bDCDR2LE
	if !le {
		want = Repxt6bDCDR2BE
	}
	got := RepresentationIDxt6b(uint16(app[0])<<8 | uint16(app[1]))
	if got != want {
		t.Fatalf("le=%v id %s want %s", le, got.String(), want.String())
	}
	mut := MarshalMessageWithExt([]byte{0x01}, ExtMutable, le)
	wantMut := Repxt6bPLCDR2LE
	if !le {
		wantMut = Repxt6bPLCDR2BE
	}
	gotMut := RepresentationIDxt6b(uint16(mut[0])<<8 | uint16(mut[1]))
	if gotMut != wantMut {
		t.Fatalf("le=%v mutable id %s want %s", le, gotMut.String(), wantMut.String())
	}
	final := MarshalMessage([]byte{0x01}, le)
	gotFin := RepresentationIDxt6b(uint16(final[0])<<8 | uint16(final[1]))
	wantFin := Repxt6bCDRLE
	if !le {
		wantFin = Repxt6bCDRBE
	}
	if gotFin != wantFin {
		t.Fatalf("le=%v final id %s want %s", le, gotFin.String(), wantFin.String())
	}
}

// TestEnvelopeV2 pins V2 emission with V1 final stability.
func TestEnvelopeV2(t *testing.T) {
	for _, le := range []bool{true, false} {
		checkEnvelopeV2Header(t, le)
		checkEnvelopeV1Versioned(t, le)
	}
}

// checkEnvelopeV1Versioned pins explicit V1 appendable/mutable emits.
//
// NewEncoderWithExt always selects V2 for non-final kinds, so the V1
// PLAIN_CDR (0x0000/0x0001) and PL_CDR (0x0002/0x0003) branches in
// RepresentationForxt6b would be dead without the versioned constructor.
// This wires them per Table 57.
func checkEnvelopeV1Versioned(t *testing.T, le bool) {
	t.Helper()
	app := NewEncoderWithExtVersioned(ExtAppendable, XCDRVersion1, le)
	gotApp := RepresentationIDxt6b(uint16(app.buf[0])<<8 | uint16(app.buf[1]))
	wantApp := Repxt6bCDRLE
	if !le {
		wantApp = Repxt6bCDRBE
	}
	if gotApp != wantApp {
		t.Fatalf("le=%v V1 appendable %s want %s", le, gotApp.String(), wantApp.String())
	}
	mut := NewEncoderWithExtVersioned(ExtMutable, XCDRVersion1, le)
	gotMut := RepresentationIDxt6b(uint16(mut.buf[0])<<8 | uint16(mut.buf[1]))
	wantMut := Repxt6bPLCDRLE
	if !le {
		wantMut = Repxt6bPLCDRBE
	}
	if gotMut != wantMut {
		t.Fatalf("le=%v V1 mutable %s want %s", le, gotMut.String(), wantMut.String())
	}
	msg := MarshalMessageWithExtVersioned([]byte{0x01}, ExtMutable, XCDRVersion1, le)
	gotMsg := RepresentationIDxt6b(uint16(msg[0])<<8 | uint16(msg[1]))
	if gotMsg != wantMut {
		t.Fatalf("le=%v V1 mutable message %s want %s", le, gotMsg.String(), wantMut.String())
	}
}
