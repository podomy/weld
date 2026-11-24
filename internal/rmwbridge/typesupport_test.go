// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1
package rmwbridge_test

import (
	"bytes"
	"testing"

	"github.com/podomy/weld/internal/rmwbridge"
	"github.com/podomy/weld/internal/types"
)

// roundtripTypeDesc builds the roundtrip test type description.
func roundtripTypeDesc() types.TypeDescription {
	return types.TypeDescription{
		Description: types.IndividualTypeDescription{
			TypeName: "weld_test/msg/Roundtrip",
			Fields: []types.Field{
				{Name: "data", Type: types.FieldType{TypeID: types.FieldTypeString}},
			},
		},
		Referenced: []types.IndividualTypeDescription{},
	}
}

// checkRegistryCDRCopy pins CDR encode and decode copy semantics.
func checkRegistryCDRCopy(t *testing.T) {
	t.Helper()
	wire := types.MarshalMessage([]byte("hello"), true)
	enc := rmwbridge.EncodeCDR(wire)
	if !bytes.Equal(enc, wire) {
		t.Fatalf("encoded = %x, want %x", enc, wire)
	}
	wire[4] ^= 0xff
	if bytes.Equal(enc, wire) {
		t.Fatalf("encode shares backing array with input")
	}
	wire[4] ^= 0xff
	dec, err := rmwbridge.DecodeCDR(enc)
	requireNoError(t, err, "decode cdr")
	if !bytes.Equal(dec, enc) {
		t.Fatalf("decoded = %x, want %x", dec, enc)
	}
	enc[4] ^= 0xff
	if bytes.Equal(dec, enc) {
		t.Fatalf("decode shares backing array with input")
	}
}

// TestTypeSupportRegistryRoundtrip pins register, lookup, and CDR copy.
func TestTypeSupportRegistryRoundtrip(t *testing.T) {
	t.Parallel()

	bridge := rmwbridge.NewBridge()
	defer func() {
		closeBridgeChecked(t, bridge)
	}()

	desc := roundtripTypeDesc()
	hash, err := types.ComputeTypeHash(desc)
	requireNoError(t, err, "compute hash")
	hashStr := hash.String()
	checkTypesRegisterLookup(t, bridge, desc.Description.TypeName, hashStr)
	checkRegistryCDRCopy(t)
}

// checkMismatchLookupRegister pins missing lookup and unknown hash register.
func checkMismatchLookupRegister(t *testing.T, bridge *rmwbridge.Bridge) {
	t.Helper()
	_, err := bridge.LookupType("weld_test/msg/Missing", "")
	requireErrorIs(t, err, rmwbridge.ErrTypeNotFound, "lookup missing type")
	err = bridge.RegisterType("weld_test/msg/Mismatch", "")
	requireNoError(t, err, "register unknown hash")
}

// checkMismatchConflict pins conflicting re-register and bad hashes.
func checkMismatchConflict(t *testing.T, bridge *rmwbridge.Bridge) {
	t.Helper()
	desc := types.TypeDescription{
		Description: types.IndividualTypeDescription{
			TypeName: "weld_test/msg/Other",
			Fields: []types.Field{
				{Name: "flag", Type: types.FieldType{TypeID: types.FieldTypeBoolean}},
			},
		},
		Referenced: []types.IndividualTypeDescription{},
	}
	otherHash, err := types.ComputeTypeHash(desc)
	requireNoError(t, err, "compute other hash")
	err = bridge.RegisterType("weld_test/msg/Mismatch", otherHash.String())
	requireErrorIs(t, err, rmwbridge.ErrTypeHashMismatch, "conflicting re-register")
	err = bridge.RegisterType("weld_test/msg/BadHash", "not-a-hash")
	requireError(t, err, "register bad hash")
	err = bridge.RegisterType("", "")
	requireError(t, err, "register empty name")
}

// checkMismatchDecode pins short and nil wire decode failures.
func checkMismatchDecode(t *testing.T) {
	t.Helper()
	_, err := rmwbridge.DecodeCDR([]byte{0x00, 0x02, 0x00})
	requireError(t, err, "decode short wire")
	_, err = rmwbridge.DecodeCDR(nil)
	requireError(t, err, "decode nil wire")
}

// TestTypeSupportMismatch pins hash mismatch and unknown type errors.
func TestTypeSupportMismatch(t *testing.T) {
	t.Parallel()

	bridge := rmwbridge.NewBridge()
	defer func() {
		closeBridgeChecked(t, bridge)
	}()

	checkMismatchLookupRegister(t, bridge)
	checkMismatchConflict(t, bridge)
	checkMismatchDecode(t)
}
