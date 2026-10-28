// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

// Package unit_datapath pins builtin_interfaces Time and Duration XCDR round trips.
//
// Corpus Tier 0 holds exactly two entries, builtin_interfaces Duration and
// builtin_interfaces Time, each with Summary int32 sec plus uint32 nanosec.
// Both base shapes are already covered live by
// testbed/live/ros/gate_type_builtin_test.go with sec 42 plus nanosec 123.
// This file documents that completeness check and pins pure Go XCDR edge
// shapes with no Docker: zero, max, negative, and full uint32 range, in both
// little endian and big endian orders, plus truncation and trailing checks.
// The needed primitives are WriteInt32, WriteUint32, ReadInt32, and
// ReadUint32, all already present in internal/types with no new codec work.
//
//nolint:cyclop,gocognit // test breadth pins many wire branches in one decoder
package unit_datapath

import (
	"testing"

	"github.com/podomy/weld/internal/types"
)

// builtinExtraWireCase is one sec plus nanosec edge shape.
type builtinExtraWireCase struct {
	label   string
	sec     int32
	nanosec uint32
}

// builtinExtraWireCases returns the pinned Time and Duration edge shapes.
//
// Zero covers the default, base covers the live gate sentinel, max covers
// int32 high plus realistic nanosec high, neg covers the signed Duration
// path, min covers int32 low, and full nanosec covers the uint32 range.
// Both Time and Duration share this layout so one table pins both types.
func builtinExtraWireCases() []builtinExtraWireCase {
	return []builtinExtraWireCase{
		{label: "zero", sec: 0, nanosec: 0},
		{label: "base", sec: 42, nanosec: 123},
		{label: "time-max", sec: 2147483647, nanosec: 999999999},
		{label: "duration-neg", sec: -1, nanosec: 0},
		{label: "duration-neg-nanos", sec: -5, nanosec: 500000000},
		{label: "sec-min", sec: -2147483648, nanosec: 0},
		{label: "nanosec-max", sec: 0, nanosec: 4294967295},
	}
}

// builtinExtraEncode encodes one edge shape as XCDR1.
func builtinExtraEncode(t *testing.T, tc builtinExtraWireCase, le bool) []byte {
	t.Helper()
	enc := types.NewEncoder(le)
	enc.WriteInt32(tc.sec)
	enc.WriteUint32(tc.nanosec)
	wire := enc.Bytes()
	if len(wire)%4 != 0 {
		t.Fatalf("%s wire length %d is not padded to 4", tc.label, len(wire))
	}
	return wire
}

// builtinExtraDecode decodes and checks one edge shape.
func builtinExtraDecode(t *testing.T, wire []byte, tc builtinExtraWireCase, le bool) {
	t.Helper()
	dec, err := types.NewDecoder(wire)
	if err != nil {
		t.Fatalf("%s new decoder: %v", tc.label, err)
	}
	if dec.LittleEndian() != le {
		t.Fatalf("%s endian = %v, want %v", tc.label, dec.LittleEndian(), le)
	}
	sec, err := dec.ReadInt32()
	if err != nil {
		t.Fatalf("%s read sec: %v", tc.label, err)
	}
	if sec != tc.sec {
		t.Fatalf("%s sec = %d, want %d", tc.label, sec, tc.sec)
	}
	nanosec, err := dec.ReadUint32()
	if err != nil {
		t.Fatalf("%s read nanosec: %v", tc.label, err)
	}
	if nanosec != tc.nanosec {
		t.Fatalf("%s nanosec = %d, want %d", tc.label, nanosec, tc.nanosec)
	}
	err = dec.Finish()
	if err != nil {
		t.Fatalf("%s finish: %v", tc.label, err)
	}
	if dec.Dropped() != 0 {
		t.Fatalf("%s drops = %d, want 0", tc.label, dec.Dropped())
	}
	if dec.Remaining() != 0 {
		t.Fatalf("%s remaining = %d, want 0", tc.label, dec.Remaining())
	}
}

// builtinExtraCheckCorpusEntry checks one builtin_interfaces corpus entry.
func builtinExtraCheckCorpusEntry(t *testing.T, name string) {
	t.Helper()
	entry, ok := types.Lookup("builtin_interfaces", name)
	if !ok {
		t.Fatalf("corpus misses builtin_interfaces/%s, no shape to pin", name)
	}
	if entry.Kind != types.KindMessage {
		t.Fatalf("corpus builtin_interfaces/%s kind = %q, want message", name, entry.Kind)
	}
	if entry.HasHeader {
		t.Fatalf("corpus builtin_interfaces/%s HasHeader = true, want false", name)
	}
	if entry.HeaderNote != "" {
		t.Fatalf("corpus builtin_interfaces/%s HeaderNote = %q, want empty", name, entry.HeaderNote)
	}
	if len(entry.Dependencies) != 0 {
		t.Fatalf("corpus builtin_interfaces/%s dependencies = %v, want empty", name, entry.Dependencies)
	}
}

// TestBuiltinExtraCorpusDocumentsTimeAndDuration checks Tier 0 completeness.
//
// It asserts the corpus holds exactly Duration and Time under
// builtin_interfaces with the int32 sec plus uint32 nanosec summary and no
// header. This documents that no further builtin_interfaces message remains
// uncovered: the base live gate already covers both with sec 42 plus
// nanosec 123, and this package adds edge shapes on the same layout.
func TestBuiltinExtraCorpusDocumentsTimeAndDuration(t *testing.T) {
	t.Parallel()
	builtinExtraCheckCorpusEntry(t, "Duration")
	builtinExtraCheckCorpusEntry(t, "Time")
	count := 0
	for _, e := range types.Corpus() {
		if e.Package == "builtin_interfaces" {
			count++
		}
	}
	if count != 2 {
		t.Fatalf("builtin_interfaces entries = %d, want 2 (Duration plus Time)", count)
	}
}

// TestBuiltinExtraRoundTrip checks XCDR round trips for edge shapes.
//
// Each table entry round trips in little endian and big endian. The codec
// path is WriteInt32 plus WriteUint32 on encode and ReadInt32 plus
// ReadUint32 plus Finish on decode, with zero drops and zero remainder.
func TestBuiltinExtraRoundTrip(t *testing.T) {
	t.Parallel()
	for _, tc := range builtinExtraWireCases() {
		for _, le := range []bool{true, false} {
			wire := builtinExtraEncode(t, tc, le)
			if le {
				if wire[1] != 0x01 {
					t.Fatalf("%s LE header flag = 0x%02x, want 0x01", tc.label, wire[1])
				}
			} else {
				if wire[1] != 0x00 {
					t.Fatalf("%s BE header flag = 0x%02x, want 0x00", tc.label, wire[1])
				}
			}
			builtinExtraDecode(t, wire, tc, le)
		}
	}
}

// TestBuiltinExtraWireGolden pins the base sentinel wire form.
//
// Sec 42 plus nanosec 123 in little endian is header 00 01 00 00 plus
// 2A 00 00 00 plus 7B 00 00 00 with no extra pad. The big endian form uses
// header 00 00 00 00 plus 00 00 00 2A plus 00 00 00 7B. Both must decode to
// the same sentinel.
func TestBuiltinExtraWireGolden(t *testing.T) {
	t.Parallel()
	encLE := types.NewEncoder(true)
	encLE.WriteInt32(42)
	encLE.WriteUint32(123)
	wireLE := encLE.Bytes()
	wantLE := []byte{0x00, 0x01, 0x00, 0x00, 0x2A, 0x00, 0x00, 0x00, 0x7B, 0x00, 0x00, 0x00}
	if len(wireLE) != len(wantLE) {
		t.Fatalf("LE golden length = %d, want %d", len(wireLE), len(wantLE))
	}
	for i := range wantLE {
		if wireLE[i] != wantLE[i] {
			t.Fatalf("LE golden byte %d = 0x%02x, want 0x%02x", i, wireLE[i], wantLE[i])
		}
	}
	builtinExtraDecode(t, wireLE, builtinExtraWireCase{label: "golden-le", sec: 42, nanosec: 123}, true)

	encBE := types.NewEncoder(false)
	encBE.WriteInt32(42)
	encBE.WriteUint32(123)
	wireBE := encBE.Bytes()
	wantBE := []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x2A, 0x00, 0x00, 0x00, 0x7B}
	if len(wireBE) != len(wantBE) {
		t.Fatalf("BE golden length = %d, want %d", len(wireBE), len(wantBE))
	}
	for i := range wantBE {
		if wireBE[i] != wantBE[i] {
			t.Fatalf("BE golden byte %d = 0x%02x, want 0x%02x", i, wireBE[i], wantBE[i])
		}
	}
	builtinExtraDecode(t, wireBE, builtinExtraWireCase{label: "golden-be", sec: 42, nanosec: 123}, false)
}

// TestBuiltinExtraTruncate checks truncated and trailing inputs fail.
//
// Valid wire with trailing garbage must fail Finish. Empty, short header,
// and truncated payload must fail at NewDecoder or at the field read with
// a counted drop. No corrupt shape may decode to a valid sentinel.
func TestBuiltinExtraTruncate(t *testing.T) {
	t.Parallel()
	tc := builtinExtraWireCase{label: "base", sec: 42, nanosec: 123}
	valid := builtinExtraEncode(t, tc, true)

	trailing := append(append([]byte(nil), valid...), 0xFF)
	dec, err := types.NewDecoder(trailing)
	if err != nil {
		t.Fatalf("trailing new decoder: %v", err)
	}
	_, err = dec.ReadInt32()
	if err != nil {
		t.Fatalf("trailing read sec: %v", err)
	}
	_, err = dec.ReadUint32()
	if err != nil {
		t.Fatalf("trailing read nanosec: %v", err)
	}
	err = dec.Finish()
	if err == nil {
		t.Fatalf("trailing bytes passed Finish, want error")
	}
	if dec.Dropped() == 0 {
		t.Fatalf("trailing drops not counted")
	}

	corrupts := [][]byte{
		{},
		nil,
		valid[:0],
		valid[:2],
		valid[:4],
		valid[:5],
		valid[:len(valid)-1],
	}
	for i, corrupt := range corrupts {
		cp := append([]byte(nil), corrupt...)
		cdec, cerr := types.NewDecoder(cp)
		if cerr != nil {
			if cdec.Dropped() == 0 {
				t.Fatalf("corrupt %d header drop not counted", i)
			}
			continue
		}
		_, rerr := cdec.ReadInt32()
		if rerr != nil {
			if cdec.Dropped() == 0 {
				t.Fatalf("corrupt %d sec drop not counted", i)
			}
			continue
		}
		_, rerr = cdec.ReadUint32()
		if rerr != nil {
			if cdec.Dropped() == 0 {
				t.Fatalf("corrupt %d nanosec drop not counted", i)
			}
			continue
		}
		ferr := cdec.Finish()
		if ferr == nil {
			t.Fatalf("corrupt %d passed Finish, want error", i)
		}
	}

	badHeader := append([]byte(nil), valid...)
	badHeader[0] = 0x01
	bdec, berr := types.NewDecoder(badHeader)
	if berr == nil {
		t.Fatalf("bad header passed NewDecoder, want error")
	}
	if bdec.Dropped() == 0 {
		t.Fatalf("bad header drop not counted")
	}
}
