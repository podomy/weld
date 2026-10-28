// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

//nolint:cyclop,gocognit // test matrix breadth over seven ROS types
package unit_sedp

import (
	"bytes"
	"strings"
	"testing"

	"github.com/podomy/weld/internal/transports/dds"
)

// Pinned RIHS01 type hashes from testbed/live/ros/gate_type_matrix_test.go:69-75.
//
// These are the weld-jazzy Jazzy values reported by ros2 topic info -v on
// domain 42. The backend table in internal/transports/dds/backend.go
// (sedpTypeHashForIDL) carries the same strings as the static fallback for
// the Publish path and for probes that fail. The function is unexported,
// so this test pins the same strings here and exercises the public
// declare plus serialize plus parse plus match path that the backend uses
// after the mapping. A change on either side shows up as a mismatch here.
const (
	matrixHashInt32         = "RIHS01_b6578ded3c58c626cfe8d1a6fb6e04f706f97e9f03d2727c9ff4e74b1cef0deb"
	matrixHashFloat64       = "RIHS01_705ba9c3d1a09df43737eb67095534de36fd426c0587779bda2bc51fe790182a"
	matrixHashBool          = "RIHS01_feb91e995ff9ebd09c0cb3d2aed18b11077585839fb5db80193b62d74528f6c9"
	matrixHashVector3       = "RIHS01_cc12fe83e4c02719f1ce8070bfd14aecd40f75a96696a67a2a1f37f7dbb0765d"
	matrixHashPointStamped  = "RIHS01_4c0296af86e01e562e9e0405d138a01537247580076c58ea38d7923ac1045897"
	matrixHashTemperature   = "RIHS01_72514a14126ab9f8a9abec974c78e5610a367b59db5da355ff1fb982d5bad4b8"
	matrixHashFluidPressure = "RIHS01_22dfb2b145a0bd5a31a1ac3882a1b32148b51d9b2f3bab250290d66f3595bc32"
)

// matrixHashCase pairs one IDL name with its pinned hash and a topic.
type matrixHashCase struct {
	// label names the case in subtest output.
	label string
	// idl is the double underscore Weld type name.
	idl string
	// hash is the expected RIHS01 type hash.
	hash string
	// rosTopic is the reader and writer topic for matching checks.
	rosTopic string
}

// matrixHashCases returns the seven live matrix types in corpus order.
func matrixHashCases() []matrixHashCase {
	return []matrixHashCase{
		{label: "std-int32", idl: "std_msgs__msg__Int32", hash: matrixHashInt32, rosTopic: "/matrix/int32"},
		{label: "std-float64", idl: "std_msgs__msg__Float64", hash: matrixHashFloat64, rosTopic: "/matrix/float64"},
		{label: "std-bool", idl: "std_msgs__msg__Bool", hash: matrixHashBool, rosTopic: "/matrix/bool"},
		{label: "geometry-vector3", idl: "geometry_msgs__msg__Vector3", hash: matrixHashVector3, rosTopic: "/matrix/vector3"},
		{label: "geometry-pointstamped", idl: "geometry_msgs__msg__PointStamped", hash: matrixHashPointStamped, rosTopic: "/matrix/pointstamped"},
		{label: "sensor-temperature", idl: "sensor_msgs__msg__Temperature", hash: matrixHashTemperature, rosTopic: "/matrix/temperature"},
		{label: "sensor-fluidpressure", idl: "sensor_msgs__msg__FluidPressure", hash: matrixHashFluidPressure, rosTopic: "/matrix/fluidpressure"},
	}
}

// isMatrixHashValid reports whether s has RIHS01 plus 64 lowercase hex digits.
func isMatrixHashValid(s string) bool {
	const wantLen = 7 + 64
	if len(s) != wantLen {
		return false
	}
	if !strings.HasPrefix(s, "RIHS01_") {
		return false
	}
	for _, c := range s[7:] {
		isHex := (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')
		if !isHex {
			return false
		}
	}
	return true
}

// matrixHashPrefix returns a deterministic SEDP prefix for one subtest.
func matrixHashPrefix(base byte) dds.SEDPGUIDPrefix {
	return sedpConformPrefix(base)
}

// TestMatrixHashPins checks the seven live matrix hashes at unit level.
//
// For each IDL it asserts the pinned hash is a non-empty valid RIHS01
// string (no empty fallback), the declare path preserves it, the
// serialized SEDP DATA user data carries it, the parse round trip keeps
// it, and endpoint matching requires exact hash equality.
func TestMatrixHashPins(t *testing.T) {
	t.Parallel()
	for i, tc := range matrixHashCases() {
		t.Run(tc.label, func(t *testing.T) {
			t.Parallel()
			if tc.hash == "" {
				t.Fatalf("%s: pinned hash is empty, want RIHS01 value", tc.label)
			}
			if !isMatrixHashValid(tc.hash) {
				t.Fatalf("%s: pinned hash %q is not RIHS01 plus 64 hex", tc.label, tc.hash)
			}
			ddsType, err := dds.MessageTypeName(tc.idl)
			if err != nil {
				t.Fatalf("%s: message type name: %v", tc.label, err)
			}
			if ddsType == "" {
				t.Fatalf("%s: DDS type is empty for IDL %q", tc.label, tc.idl)
			}
			prefix := matrixHashPrefix(byte(0x40 + i)) //nolint:gosec // G115: test index is small, sum fits byte
			idx, err := dds.NewSEDPIndex(prefix, 42)
			if err != nil {
				t.Fatalf("%s: new index: %v", tc.label, err)
			}
			writer, err := idx.DeclareWriter(tc.rosTopic, tc.idl, tc.hash, nil)
			if err != nil {
				t.Fatalf("%s: declare writer: %v", tc.label, err)
			}
			if writer.TypeHash != tc.hash {
				t.Fatalf("%s: writer hash = %q, want %q", tc.label, writer.TypeHash, tc.hash)
			}
			if writer.TypeIDL != tc.idl {
				t.Fatalf("%s: writer IDL = %q, want %q", tc.label, writer.TypeIDL, tc.idl)
			}
			reader, err := idx.DeclareReader(tc.rosTopic, tc.idl, tc.hash, nil)
			if err != nil {
				t.Fatalf("%s: declare reader: %v", tc.label, err)
			}
			if reader.TypeHash != tc.hash {
				t.Fatalf("%s: reader hash = %q, want %q", tc.label, reader.TypeHash, tc.hash)
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
			if !dds.SEDPEndpointsMatch(reader, writer) {
				t.Fatalf("%s: exact hash endpoints do not match", tc.label)
			}
			emptyWriter := writer
			emptyWriter.TypeHash = ""
			// Empty hash wildcards for Humble pre-hash peers.
			if !dds.SEDPEndpointsMatch(reader, emptyWriter) {
				t.Fatalf("%s: set hash reader did not match empty hash writer", tc.label)
			}
			otherWriter := writer
			otherWriter.TypeHash = "RIHS01_0000000000000000000000000000000000000000000000000000000000000000"
			if otherWriter.TypeHash == tc.hash {
				t.Fatalf("%s: other hash fixture collides with pinned hash", tc.label)
			}
			if dds.SEDPEndpointsMatch(reader, otherWriter) {
				t.Fatalf("%s: set hash reader matched other hash writer", tc.label)
			}
			emptyReader := reader
			emptyReader.TypeHash = ""
			// Empty hash wildcards for Humble pre-hash peers.
			if !dds.SEDPEndpointsMatch(emptyReader, writer) {
				t.Fatalf("%s: empty hash reader did not match set hash writer", tc.label)
			}
		})
	}
}
