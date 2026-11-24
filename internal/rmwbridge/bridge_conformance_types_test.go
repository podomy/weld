// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

package rmwbridge_test

import (
	"bytes"
	"testing"

	"github.com/podomy/weld/internal/rmwbridge"
	"github.com/podomy/weld/internal/types"
)

// checkTypesRegisterLookup pins register, re-register, and lookup.
func checkTypesRegisterLookup(t *testing.T, bridge *rmwbridge.Bridge, name string, hash string) {
	t.Helper()
	err := bridge.RegisterType(name, hash)
	requireNoError(t, err, "register type")
	err = bridge.RegisterType(name, hash)
	requireNoError(t, err, "re-register identical")
	got, err := bridge.LookupType(name, hash)
	requireNoError(t, err, "lookup type")
	if got.TypeName != name {
		t.Fatalf("type name = %q, want %q", got.TypeName, name)
	}
	if got.TypeHash != hash {
		t.Fatalf("type hash = %q, want %q", got.TypeHash, hash)
	}
	got, err = bridge.LookupType(name, "")
	requireNoError(t, err, "lookup without hash")
	if got.TypeHash != hash {
		t.Fatalf("unpinned lookup hash = %q, want %q", got.TypeHash, hash)
	}
}

// checkTypesMismatch pins missing, conflict, and bad hash paths.
func checkTypesMismatch(t *testing.T, bridge *rmwbridge.Bridge, name string, hash string) {
	t.Helper()
	_, err := bridge.LookupType("weld_test/msg/Missing", "")
	requireErrorIs(t, err, rmwbridge.ErrTypeNotFound, "lookup missing type")
	other := conformanceHash(t, "weld_test/msg/ConformOther")
	if other == hash {
		t.Fatalf("test hashes collide")
	}
	err = bridge.RegisterType(name, other)
	requireErrorIs(t, err, rmwbridge.ErrTypeHashMismatch, "conflicting re-register")
	_, err = bridge.LookupType(name, other)
	requireErrorIs(t, err, rmwbridge.ErrTypeHashMismatch, "lookup mismatch")
	err = bridge.RegisterType("weld_test/msg/BadHash", "not-a-hash")
	requireError(t, err, "register bad hash")
	err = bridge.RegisterType("", "")
	requireError(t, err, "register empty name")
	err = bridge.RegisterType("weld_test/msg/Unknown", "")
	requireNoError(t, err, "register unknown hash")
}

// checkTypesRegister pins type registration paths.
func checkTypesRegister(t *testing.T) {
	t.Helper()
	bridge := newConformanceBridge(t)
	name := "weld_test/msg/Conform"
	hash := conformanceHash(t, name)
	checkTypesRegisterLookup(t, bridge, name, hash)
	checkTypesMismatch(t, bridge, name, hash)
}

// checkCDREncodeCopy pins encode copy semantics.
func checkCDREncodeCopy(t *testing.T) []byte {
	t.Helper()
	wire := types.MarshalMessage([]byte("hello"), true)
	enc := rmwbridge.EncodeCDR(wire)
	if bytes.Equal(enc, wire) == false {
		t.Fatalf("encoded mismatch")
	}
	wire[4] ^= 0xff
	if bytes.Equal(enc, wire) {
		t.Fatalf("encode shares backing array with input")
	}
	wire[4] ^= 0xff
	return enc
}

// checkCDRDecodeCopy pins decode copy semantics.
func checkCDRDecodeCopy(t *testing.T, enc []byte) {
	t.Helper()
	dec, err := rmwbridge.DecodeCDR(enc)
	requireNoError(t, err, "decode cdr")
	if bytes.Equal(dec, enc) == false {
		t.Fatalf("decoded mismatch")
	}
	enc[4] ^= 0xff
	if bytes.Equal(dec, enc) {
		t.Fatalf("decode shares backing array with input")
	}
}

// checkCDRBadWire pins short and malformed wire failures.
func checkCDRBadWire(t *testing.T, bridge *rmwbridge.Bridge) {
	t.Helper()
	dec2, err := bridge.DecodeCDR(types.MarshalMessage([]byte("hi"), true))
	requireNoError(t, err, "bridge decode")
	if len(dec2) == 0 {
		t.Fatalf("bridge decode returned empty")
	}
	_, err = rmwbridge.DecodeCDR(nil)
	requireError(t, err, "decode nil wire")
	_, err = rmwbridge.DecodeCDR([]byte{0x00, 0x01, 0x00})
	requireError(t, err, "decode short wire")
	_, err = rmwbridge.DecodeCDR([]byte{0x01, 0x01, 0x00, 0x00, 0x00})
	requireError(t, err, "decode bad first byte")
	_, err = rmwbridge.DecodeCDR([]byte{0x00, 0x02, 0x00, 0x00, 0x00})
	requireError(t, err, "decode bad endian flag")
	_, err = bridge.DecodeCDR([]byte{0x00})
	requireError(t, err, "bridge decode short wire")
}

// checkCDREncodeDecode pins CDR copy and error paths.
func checkCDREncodeDecode(t *testing.T) {
	t.Helper()
	enc := checkCDREncodeCopy(t)
	checkCDRDecodeCopy(t, enc)
	bridge := newConformanceBridge(t)
	checkCDRBadWire(t, bridge)
}

// TestConformanceTypes covers type registration and CDR handling.
func TestConformanceTypes(t *testing.T) {
	t.Run("typesupport register lookup", func(t *testing.T) {
		t.Parallel()
		checkTypesRegister(t)
	})

	t.Run("cdr encode decode", func(t *testing.T) {
		t.Parallel()
		checkCDREncodeDecode(t)
	})
}
