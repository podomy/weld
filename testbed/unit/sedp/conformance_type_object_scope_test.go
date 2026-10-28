// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

// Full XTypes TypeObject exchange with dependency resolution and
// validation is v2 and out of scope for v1. V1 keeps raw TypeObject
// bytes as opaque passthrough and matches on USER_DATA type hash only.
// See the passthrough record in internal/transports/dds/sedp.go:265.
package unit_sedp

import (
	"bytes"
	"testing"

	"github.com/podomy/weld/internal/transports/dds"
	"github.com/podomy/weld/internal/transports/dds/rtps"
)

// typeObjectCase pairs one wired IDL with its pinned hash and topic.
type typeObjectCase struct {
	// label names the subtest.
	label string
	// idl is the double underscore Weld type name.
	idl string
	// hash is the expected RIHS01 type hash.
	hash string
	// rosTopic is the topic used for declare and match checks.
	rosTopic string
}

// typeObjectCases returns the 8 wired IDLs: String plus the 7 matrix types.
func typeObjectCases() []typeObjectCase {
	return []typeObjectCase{
		{label: "string", idl: "std_msgs__msg__String", hash: sedpConformTypeHash, rosTopic: "/chatter"},
		{label: "std-int32", idl: "std_msgs__msg__Int32", hash: matrixHashInt32, rosTopic: "/matrix/int32"},
		{label: "std-float64", idl: "std_msgs__msg__Float64", hash: matrixHashFloat64, rosTopic: "/matrix/float64"},
		{label: "std-bool", idl: "std_msgs__msg__Bool", hash: matrixHashBool, rosTopic: "/matrix/bool"},
		{label: "geometry-vector3", idl: "geometry_msgs__msg__Vector3", hash: matrixHashVector3, rosTopic: "/matrix/vector3"},
		{label: "geometry-pointstamped", idl: "geometry_msgs__msg__PointStamped", hash: matrixHashPointStamped, rosTopic: "/matrix/pointstamped"},
		{label: "sensor-temperature", idl: "sensor_msgs__msg__Temperature", hash: matrixHashTemperature, rosTopic: "/matrix/temperature"},
		{label: "sensor-fluidpressure", idl: "sensor_msgs__msg__FluidPressure", hash: matrixHashFluidPressure, rosTopic: "/matrix/fluidpressure"},
	}
}

// typeObjectVendorValue returns one distinct 4 byte value per vendor PID.
func typeObjectVendorValue(id uint16) []byte {
	switch id {
	case 0x8003:
		return []byte{0x01, 0x02, 0x03, 0x04}
	case 0x8007:
		return []byte{0x05, 0x06, 0x07, 0x08}
	case 0x8019:
		return []byte{0x09, 0x0A, 0x0B, 0x0C}
	case 0x800c:
		return []byte{0x0D, 0x0E, 0x0F, 0x10}
	default:
		return []byte{0x00, 0x00, 0x00, 0x00}
	}
}

// typeObjectFindVendor returns the value for one vendor PID or fails.
func typeObjectFindVendor(t *testing.T, params []rtps.Param, id uint16) []byte {
	t.Helper()
	for _, param := range params {
		if param.ID == id {
			return param.Value
		}
	}
	t.Fatalf("vendor PID %#04x missing", id)
	return nil
}

// typeObjectCheckHashRoundTrip checks USER_DATA hash round trip for one IDL case.
func typeObjectCheckHashRoundTrip(t *testing.T, tc typeObjectCase, index int) {
	t.Helper()
	if tc.hash == "" {
		t.Fatalf("%s: pinned hash is empty", tc.label)
	}
	idx := sedpConformNewIndex(t, byte(0x60+index), sedpConformDomain) //nolint:gosec // G115: test index is small, sum fits byte
	writer, err := idx.DeclareWriter(tc.rosTopic, tc.idl, tc.hash, nil)
	if err != nil {
		t.Fatalf("declare writer: %v", err)
	}
	if writer.TypeHash != tc.hash {
		t.Fatalf("%s: writer hash = %q, want %q", tc.label, writer.TypeHash, tc.hash)
	}
	raw, err := dds.SerializeSEDPDATA(writer)
	if err != nil {
		t.Fatalf("%s: serialize: %v", tc.label, err)
	}
	if !bytes.Contains(raw, []byte(tc.hash)) {
		t.Fatalf("%s: serialized DATA misses hash %q", tc.label, tc.hash)
	}
	decoded, err := dds.ParseSEDPDATA(raw)
	if err != nil {
		t.Fatalf("%s: parse: %v", tc.label, err)
	}
	if decoded.TypeHash != tc.hash {
		t.Fatalf("%s: round trip hash = %q, want %q", tc.label, decoded.TypeHash, tc.hash)
	}
	if decoded.TypeIDL != tc.idl {
		t.Fatalf("%s: round trip IDL = %q, want %q", tc.label, decoded.TypeIDL, tc.idl)
	}
}

// TestTypeObjectUserDataHashRoundTrip checks USER_DATA hash round trip for 8 IDLs.
func TestTypeObjectUserDataHashRoundTrip(t *testing.T) {
	t.Parallel()
	for i, tc := range typeObjectCases() {
		t.Run(tc.label, func(t *testing.T) {
			t.Parallel()
			typeObjectCheckHashRoundTrip(t, tc, i)
		})
	}
}

// typeObjectPinVendorIDs pins the four vendor PID constants used in the test.
func typeObjectPinVendorIDs(t *testing.T) {
	t.Helper()
	if rtps.PIDVendorAdlinkWriterDataLifecycle != 0x8003 {
		t.Fatalf("PID 0x8003 = %#04x", rtps.PIDVendorAdlinkWriterDataLifecycle)
	}
	if rtps.PIDVendorAdlinkParticipantVersionInfo != 0x8007 {
		t.Fatalf("PID 0x8007 = %#04x", rtps.PIDVendorAdlinkParticipantVersionInfo)
	}
	if rtps.PIDVendorCycloneReceiveBufferSize != 0x8019 {
		t.Fatalf("PID 0x8019 = %#04x", rtps.PIDVendorCycloneReceiveBufferSize)
	}
	if rtps.PIDVendorAdlinkEntityFactory != 0x800c {
		t.Fatalf("PID 0x800c = %#04x", rtps.PIDVendorAdlinkEntityFactory)
	}
}

// typeObjectBuildVendorParams returns base plus vendor params for the endpoint.
func typeObjectBuildVendorParams(t *testing.T, ep dds.SEDPEndpoint, vendorIDs []uint16) []rtps.Param {
	t.Helper()
	params := []rtps.Param{
		{ID: rtps.PIDTopicName, Value: sedpConformEncodeStringPad(sedpConformDDSTopic)},
		{ID: rtps.PIDTypeName, Value: sedpConformEncodeStringPad(sedpConformTypeDDS)},
		{ID: rtps.PIDEndpointGUIDWire, Value: sedpConformGUIDValue(ep.ID)},
	}
	for _, id := range vendorIDs {
		params = append(params, rtps.Param{ID: id, Value: typeObjectVendorValue(id)})
	}
	return params
}

// typeObjectCheckVendorDecoded checks decoded vendor params carry all IDs and values.
func typeObjectCheckVendorDecoded(t *testing.T, decoded, params []rtps.Param, vendorIDs []uint16, encoded []byte, consumed int) {
	t.Helper()
	if consumed != len(encoded) {
		t.Fatalf("vendor consumed = %d, want %d", consumed, len(encoded))
	}
	if len(decoded) != len(params) {
		t.Fatalf("vendor decoded = %d, want %d", len(decoded), len(params))
	}
	for _, id := range vendorIDs {
		got := typeObjectFindVendor(t, decoded, id)
		want := typeObjectVendorValue(id)
		if !bytes.Equal(got, want) {
			t.Fatalf("vendor %#04x = % X, want % X", id, got, want)
		}
	}
}

// typeObjectCheckVendorReEncoded checks the second decode matches the first.
func typeObjectCheckVendorReEncoded(t *testing.T, decoded []rtps.Param, reEncoded, encoded []byte) {
	t.Helper()
	if !bytes.Equal(reEncoded, encoded) {
		t.Fatalf("vendor re-encode mismatch: % X vs % X", reEncoded, encoded)
	}
	second, secondConsumed, err := rtps.DecodeParamsEndian(reEncoded, true)
	if err != nil {
		t.Fatalf("decode re-encoded: %v", err)
	}
	if secondConsumed != len(reEncoded) {
		t.Fatalf("second consumed = %d, want %d", secondConsumed, len(reEncoded))
	}
	if len(second) != len(decoded) {
		t.Fatalf("second decoded = %d, want %d", len(second), len(decoded))
	}
	for i := range decoded {
		if decoded[i].ID != second[i].ID {
			t.Fatalf("second id [%d] = %#04x, want %#04x", i, second[i].ID, decoded[i].ID)
		}
		if !bytes.Equal(decoded[i].Value, second[i].Value) {
			t.Fatalf("second value [%d] mismatch", i)
		}
	}
}

// typeObjectCheckVendorCodec checks vendor params decode plus re-encode stability.
func typeObjectCheckVendorCodec(t *testing.T, params []rtps.Param, vendorIDs []uint16) []rtps.Param {
	t.Helper()
	encoded := rtps.EncodeParams(params)
	decoded, consumed, err := rtps.DecodeParamsEndian(encoded, true)
	if err != nil {
		t.Fatalf("decode vendor params: %v", err)
	}
	typeObjectCheckVendorDecoded(t, decoded, params, vendorIDs, encoded, consumed)
	reEncoded := rtps.EncodeParams(decoded)
	typeObjectCheckVendorReEncoded(t, decoded, reEncoded, encoded)
	return decoded
}

// typeObjectCheckVendorParsed checks the vendor params survive a SEDP parse.
func typeObjectCheckVendorParsed(t *testing.T, ep dds.SEDPEndpoint, params []rtps.Param) {
	t.Helper()
	body := sedpConformBuildBody(sedpConformPubsWriter, 1, params)
	raw := sedpConformBuildMessage(ep.ID.Prefix, sedpConformFlagLE|sedpConformFlagInlineQoS|sedpConformFlagData, body)
	parsed, err := dds.ParseSEDPDATA(raw)
	if err != nil {
		t.Fatalf("parse with vendor PIDs: %v", err)
	}
	if parsed.DDSTopic != sedpConformDDSTopic {
		t.Fatalf("vendor topic = %q, want %q", parsed.DDSTopic, sedpConformDDSTopic)
	}
	if parsed.TypeDDS != sedpConformTypeDDS {
		t.Fatalf("vendor type = %q, want %q", parsed.TypeDDS, sedpConformTypeDDS)
	}
}

// TestTypeObjectVendorPIDsPreserved checks vendor PIDs decode and re-encode stably.
func TestTypeObjectVendorPIDsPreserved(t *testing.T) {
	t.Parallel()
	typeObjectPinVendorIDs(t)
	vendorIDs := []uint16{0x8003, 0x8007, 0x8019, 0x800c}
	idx := sedpConformNewIndex(t, 0x70, sedpConformDomain)
	ep := sedpConformDeclareWriter(t, idx, sedpConformTypeHash)
	params := typeObjectBuildVendorParams(t, ep, vendorIDs)
	typeObjectCheckVendorCodec(t, params, vendorIDs)
	typeObjectCheckVendorParsed(t, ep, params)
}

// TestTypeObjectInlineAbsentTolerated checks missing TypeObject PID parses as nil.
func TestTypeObjectInlineAbsentTolerated(t *testing.T) {
	t.Parallel()
	if rtps.PIDTypeObject != 0x0072 {
		t.Fatalf("PIDTypeObject = %#04x, want 0x0072", rtps.PIDTypeObject)
	}
	idx := sedpConformNewIndex(t, 0x71, sedpConformDomain)
	ep, err := idx.DeclareWriter(sedpConformROSTopic, sedpConformTypeIDL, sedpConformTypeHash, nil)
	if err != nil {
		t.Fatalf("declare writer: %v", err)
	}
	if len(ep.TypeObject) != 0 {
		t.Fatalf("fresh TypeObject len = %d, want 0", len(ep.TypeObject))
	}
	raw, err := dds.SerializeSEDPDATA(ep)
	if err != nil {
		t.Fatalf("serialize without TypeObject: %v", err)
	}
	subs := sedpConformSplit(t, raw)
	data := sedpConformFindDATA(t, subs)
	params := sedpConformInlineParams(t, data.body, data.flags)
	if sedpConformHasParam(params, rtps.PIDTypeObject) {
		t.Fatalf("inline QoS carries unexpected TypeObject PID 0x0072")
	}
	decoded, err := dds.ParseSEDPDATA(raw)
	if err != nil {
		t.Fatalf("parse without TypeObject: %v", err)
	}
	if len(decoded.TypeObject) != 0 {
		t.Fatalf("decoded TypeObject len = %d, want 0 for absent PID", len(decoded.TypeObject))
	}
	if decoded.TypeHash != sedpConformTypeHash {
		t.Fatalf("absent hash = %q, want %q", decoded.TypeHash, sedpConformTypeHash)
	}
	manualParams := []rtps.Param{
		{ID: rtps.PIDTopicName, Value: sedpConformEncodeStringPad(sedpConformDDSTopic)},
		{ID: rtps.PIDTypeName, Value: sedpConformEncodeStringPad(sedpConformTypeDDS)},
		{ID: rtps.PIDEndpointGUIDWire, Value: sedpConformGUIDValue(ep.ID)},
	}
	manualBody := sedpConformBuildBody(sedpConformPubsWriter, 2, manualParams)
	manualRaw := sedpConformBuildMessage(ep.ID.Prefix, sedpConformFlagLE|sedpConformFlagInlineQoS|sedpConformFlagData, manualBody)
	manualDecoded, err := dds.ParseSEDPDATA(manualRaw)
	if err != nil {
		t.Fatalf("parse manual without TypeObject: %v", err)
	}
	if len(manualDecoded.TypeObject) != 0 {
		t.Fatalf("manual TypeObject len = %d, want 0", len(manualDecoded.TypeObject))
	}
	if manualDecoded.DDSTopic != sedpConformDDSTopic {
		t.Fatalf("manual topic = %q, want %q", manualDecoded.DDSTopic, sedpConformDDSTopic)
	}
}
