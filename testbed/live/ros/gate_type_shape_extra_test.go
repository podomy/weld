// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

//nolint:dupl // live gate decoders repeat similar wire checks across types
package ros_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/podomy/weld/internal/types"
	"github.com/podomy/weld/testbed/live/helpers"
)

// Extra shape static type hashes from ros2 topic info -v on weld-jazzy.
//
// These are the RIHS01 hashes reported for each ROS type on domain 42.
// Live probing via ros2 topic info -v is preferred at runtime, these values
// are the fallback when the probe cannot run. Values were captured from
// weld-jazzy Jazzy.
const (
	shapeExtraHashPlane        = "RIHS01_dfbfe8314689c850615d4a727af017e9aa86c10e369a606c8c851ef8f16c58c8"
	shapeExtraHashMeshTriangle = "RIHS01_618e5c073eeb729e433ef6226e72c01d995c459fb7d76348c9700409a5020bd0"
	shapeExtraHashMesh         = "RIHS01_f2150b82d8ee7e8bc3f396a2b158aefb4b9a5510a474be271ba1268aebb55289"
)

// Extra shape fixed sentinels for types without a string field.
//
// Plane, MeshTriangle, and Mesh carry no Header and no frame_id, so the
// per-run live ID cannot ride in the payload. Isolation uses per-run topic
// plus domain 42, matching the Vector3 matrix case. Values stay fixed so
// Weld take and ros2 echo checks are deterministic. Mesh stays small with
// one triangle plus three vertices.
const (
	shapeExtraPlaneA float64 = 1.0
	shapeExtraPlaneB float64 = 2.0
	shapeExtraPlaneC float64 = 3.0
	shapeExtraPlaneD float64 = 4.0
	shapeExtraTri0   uint32  = 0
	shapeExtraTri1   uint32  = 1
	shapeExtraTri2   uint32  = 2
	shapeExtraVert0X float64 = 1.0
	shapeExtraVert0Y float64 = 2.0
	shapeExtraVert0Z float64 = 3.0
	shapeExtraVert1X float64 = 4.0
	shapeExtraVert1Y float64 = 5.0
	shapeExtraVert1Z float64 = 6.0
	shapeExtraVert2X float64 = 7.0
	shapeExtraVert2Y float64 = 8.0
	shapeExtraVert2Z float64 = 9.0
)

// yamlShapeExtraPlane returns ROS YAML for the Plane sentinel.
func yamlShapeExtraPlane(live string) string {
	return "{coef: [1.0, 2.0, 3.0, 4.0]}"
}

// encodeShapeExtraPlane encodes the fixed Plane sentinel as XCDR1.
func encodeShapeExtraPlane(live string) ([]byte, error) {
	enc := types.NewEncoder(true)
	enc.WriteFloat64(shapeExtraPlaneA)
	enc.WriteFloat64(shapeExtraPlaneB)
	enc.WriteFloat64(shapeExtraPlaneC)
	enc.WriteFloat64(shapeExtraPlaneD)
	return enc.Bytes(), nil
}

// decodeShapeExtraPlane decodes and checks the Plane sentinel.
func decodeShapeExtraPlane(payload []byte, live string) error {
	dec, err := types.NewDecoder(payload)
	if err != nil {
		return fmt.Errorf("decode plane header: %w", err)
	}
	a, err := dec.ReadFloat64()
	if err != nil {
		return fmt.Errorf("decode plane a: %w", err)
	}
	b, err := dec.ReadFloat64()
	if err != nil {
		return fmt.Errorf("decode plane b: %w", err)
	}
	c, err := dec.ReadFloat64()
	if err != nil {
		return fmt.Errorf("decode plane c: %w", err)
	}
	d, err := dec.ReadFloat64()
	if err != nil {
		return fmt.Errorf("decode plane d: %w", err)
	}
	if a != shapeExtraPlaneA || b != shapeExtraPlaneB || c != shapeExtraPlaneC || d != shapeExtraPlaneD {
		return fmt.Errorf("plane (%v, %v, %v, %v) wants (1.0, 2.0, 3.0, 4.0)", a, b, c, d)
	}
	err = dec.Finish()
	if err != nil {
		return fmt.Errorf("finish plane: %w", err)
	}
	return nil
}

// wantShapeExtraPlane returns the echo substring for the Plane sentinel.
func wantShapeExtraPlane(live string) string {
	return "4.0"
}

// yamlShapeExtraMeshTriangle returns ROS YAML for the MeshTriangle sentinel.
func yamlShapeExtraMeshTriangle(live string) string {
	return "{vertex_indices: [0, 1, 2]}"
}

// encodeShapeExtraMeshTriangle encodes the fixed MeshTriangle sentinel as XCDR1.
func encodeShapeExtraMeshTriangle(live string) ([]byte, error) {
	enc := types.NewEncoder(true)
	enc.WriteUint32(shapeExtraTri0)
	enc.WriteUint32(shapeExtraTri1)
	enc.WriteUint32(shapeExtraTri2)
	return enc.Bytes(), nil
}

// decodeShapeExtraMeshTriangle decodes and checks the MeshTriangle sentinel.
func decodeShapeExtraMeshTriangle(payload []byte, live string) error {
	dec, err := types.NewDecoder(payload)
	if err != nil {
		return fmt.Errorf("decode meshtriangle header: %w", err)
	}
	v0, err := dec.ReadUint32()
	if err != nil {
		return fmt.Errorf("decode meshtriangle 0: %w", err)
	}
	v1, err := dec.ReadUint32()
	if err != nil {
		return fmt.Errorf("decode meshtriangle 1: %w", err)
	}
	v2, err := dec.ReadUint32()
	if err != nil {
		return fmt.Errorf("decode meshtriangle 2: %w", err)
	}
	if v0 != shapeExtraTri0 || v1 != shapeExtraTri1 || v2 != shapeExtraTri2 {
		return fmt.Errorf("meshtriangle (%v, %v, %v) wants (0, 1, 2)", v0, v1, v2)
	}
	err = dec.Finish()
	if err != nil {
		return fmt.Errorf("finish meshtriangle: %w", err)
	}
	return nil
}

// wantShapeExtraMeshTriangle returns the echo substring for the MeshTriangle sentinel.
func wantShapeExtraMeshTriangle(live string) string {
	return "vertex_indices"
}

// yamlShapeExtraMesh returns ROS YAML for the small Mesh sentinel.
func yamlShapeExtraMesh(live string) string {
	return "{triangles: [{vertex_indices: [0, 1, 2]}], vertices: [{x: 1.0, y: 2.0, z: 3.0}, {x: 4.0, y: 5.0, z: 6.0}, {x: 7.0, y: 8.0, z: 9.0}]}"
}

// encodeShapeExtraMeshTriangles writes the single triangle indices.
func encodeShapeExtraMeshTriangles(enc *types.Encoder) {
	enc.WriteUint32(shapeExtraTri0)
	enc.WriteUint32(shapeExtraTri1)
	enc.WriteUint32(shapeExtraTri2)
}

// encodeShapeExtraMeshVertices writes the three fixed vertices.
func encodeShapeExtraMeshVertices(enc *types.Encoder) {
	enc.WriteFloat64(shapeExtraVert0X)
	enc.WriteFloat64(shapeExtraVert0Y)
	enc.WriteFloat64(shapeExtraVert0Z)
	enc.WriteFloat64(shapeExtraVert1X)
	enc.WriteFloat64(shapeExtraVert1Y)
	enc.WriteFloat64(shapeExtraVert1Z)
	enc.WriteFloat64(shapeExtraVert2X)
	enc.WriteFloat64(shapeExtraVert2Y)
	enc.WriteFloat64(shapeExtraVert2Z)
}

// encodeShapeExtraMesh encodes the small Mesh sentinel as XCDR1.
func encodeShapeExtraMesh(live string) ([]byte, error) {
	enc := types.NewEncoder(true)
	err := enc.WriteSequenceLength(1)
	if err != nil {
		return nil, fmt.Errorf("encode mesh triangles len: %w", err)
	}
	encodeShapeExtraMeshTriangles(enc)
	err = enc.WriteSequenceLength(3)
	if err != nil {
		return nil, fmt.Errorf("encode mesh vertices len: %w", err)
	}
	encodeShapeExtraMeshVertices(enc)
	return enc.Bytes(), nil
}

// decodeShapeExtraMeshTriangles reads and checks the single triangle.
func decodeShapeExtraMeshTriangles(dec *types.Decoder) error {
	n, err := dec.ReadSequenceLength()
	if err != nil {
		return fmt.Errorf("decode mesh triangles len: %w", err)
	}
	if n != 1 {
		return fmt.Errorf("mesh triangles len %d wants 1", n)
	}
	v0, err := dec.ReadUint32()
	if err != nil {
		return fmt.Errorf("decode mesh triangle 0: %w", err)
	}
	v1, err := dec.ReadUint32()
	if err != nil {
		return fmt.Errorf("decode mesh triangle 1: %w", err)
	}
	v2, err := dec.ReadUint32()
	if err != nil {
		return fmt.Errorf("decode mesh triangle 2: %w", err)
	}
	if v0 != shapeExtraTri0 || v1 != shapeExtraTri1 || v2 != shapeExtraTri2 {
		return fmt.Errorf("mesh triangle (%v, %v, %v) wants (0, 1, 2)", v0, v1, v2)
	}
	return nil
}

// decodeShapeExtraMeshVertex reads one vertex triple with checks.
func decodeShapeExtraMeshVertex(dec *types.Decoder, label string, wantX, wantY, wantZ float64, wantText string) error {
	x, y, z, err := helpers.ReadFloat64Triple(dec, "decode mesh "+label+" x", "decode mesh "+label+" y", "decode mesh "+label+" z")
	if err != nil {
		return fmt.Errorf("read mesh %s: %w", label, err)
	}
	if x != wantX || y != wantY || z != wantZ {
		return fmt.Errorf("mesh %s (%v, %v, %v) wants %s", label, x, y, z, wantText)
	}
	return nil
}

// decodeShapeExtraMeshVertices reads and checks the three vertices.
func decodeShapeExtraMeshVertices(dec *types.Decoder) error {
	n, err := dec.ReadSequenceLength()
	if err != nil {
		return fmt.Errorf("decode mesh vertices len: %w", err)
	}
	if n != 3 {
		return fmt.Errorf("mesh vertices len %d wants 3", n)
	}
	err = decodeShapeExtraMeshVertex(dec, "vertex 0", shapeExtraVert0X, shapeExtraVert0Y, shapeExtraVert0Z, "(1.0, 2.0, 3.0)")
	if err != nil {
		return err
	}
	err = decodeShapeExtraMeshVertex(dec, "vertex 1", shapeExtraVert1X, shapeExtraVert1Y, shapeExtraVert1Z, "(4.0, 5.0, 6.0)")
	if err != nil {
		return err
	}
	err = decodeShapeExtraMeshVertex(dec, "vertex 2", shapeExtraVert2X, shapeExtraVert2Y, shapeExtraVert2Z, "(7.0, 8.0, 9.0)")
	if err != nil {
		return err
	}
	return nil
}

// decodeShapeExtraMesh decodes and checks the small Mesh sentinel.
func decodeShapeExtraMesh(payload []byte, live string) error {
	dec, err := types.NewDecoder(payload)
	if err != nil {
		return fmt.Errorf("decode mesh header: %w", err)
	}
	err = decodeShapeExtraMeshTriangles(dec)
	if err != nil {
		return err
	}
	err = decodeShapeExtraMeshVertices(dec)
	if err != nil {
		return err
	}
	err = dec.Finish()
	if err != nil {
		return fmt.Errorf("finish mesh: %w", err)
	}
	return nil
}

// wantShapeExtraMesh returns the echo substring for the Mesh sentinel.
func wantShapeExtraMesh(live string) string {
	return "x: 7.0"
}

// shapeExtraCases returns the extra shape live matrix in fixed order.
func shapeExtraCases() []typeMatrixCase {
	return []typeMatrixCase{
		{
			Label:      "shape-plane",
			RosType:    "shape_msgs/msg/Plane",
			IDLName:    "shape_msgs__msg__Plane",
			staticHash: shapeExtraHashPlane,
			yaml:       yamlShapeExtraPlane,
			encode:     encodeShapeExtraPlane,
			decode:     decodeShapeExtraPlane,
			want:       wantShapeExtraPlane,
		},
		{
			Label:      "shape-meshtriangle",
			RosType:    "shape_msgs/msg/MeshTriangle",
			IDLName:    "shape_msgs__msg__MeshTriangle",
			staticHash: shapeExtraHashMeshTriangle,
			yaml:       yamlShapeExtraMeshTriangle,
			encode:     encodeShapeExtraMeshTriangle,
			decode:     decodeShapeExtraMeshTriangle,
			want:       wantShapeExtraMeshTriangle,
		},
		{
			Label:      "shape-mesh",
			RosType:    "shape_msgs/msg/Mesh",
			IDLName:    "shape_msgs__msg__Mesh",
			staticHash: shapeExtraHashMesh,
			yaml:       yamlShapeExtraMesh,
			encode:     encodeShapeExtraMesh,
			decode:     decodeShapeExtraMesh,
			want:       wantShapeExtraMesh,
		},
	}
}

// runShapeExtraLabel runs forward plus reverse for one extra shape label.
//
// Domain is the allocated exclusive domain shared by both directions.
// It looks up the case, skips when the peer lacks the type, then runs
// forward with one live ID and reverse with another so the two directions
// never share a topic. Forward and reverse reuse the shared matrix runners
// so domain 42 plus docker argv plus hash probing stay identical.
func runShapeExtraLabel(t *testing.T, ctx context.Context, domain int, label string) {
	t.Helper()
	runSharedMatrixLabel(t, ctx, domain, label, shapeExtraCases())
}

// TestROSTypeShapePlane checks Plane forward plus reverse.
//
// Plane carries no string field, so the fixed coef sentinel uses per-run
// topic plus domain isolation.
func TestROSTypeShapePlane(t *testing.T) {
	t.Parallel()
	domain := helpers.MustAllocateDomain(t)
	ctx, cancel := context.WithTimeout(context.Background(), typeMatrixTestTimeout)
	defer cancel()
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)
	runShapeExtraLabel(t, ctx, domain, "shape-plane")
}

// TestROSTypeShapeMeshTriangle checks MeshTriangle forward plus reverse.
//
// MeshTriangle carries no string field, so the fixed indices sentinel uses
// per-run topic plus domain isolation.
func TestROSTypeShapeMeshTriangle(t *testing.T) {
	t.Parallel()
	domain := helpers.MustAllocateDomain(t)
	ctx, cancel := context.WithTimeout(context.Background(), typeMatrixTestTimeout)
	defer cancel()
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)
	runShapeExtraLabel(t, ctx, domain, "shape-meshtriangle")
}

// TestROSTypeShapeMesh checks Mesh forward plus reverse.
//
// Mesh carries no string field, so the fixed one triangle plus three
// vertices sentinel uses per-run topic plus domain isolation.
func TestROSTypeShapeMesh(t *testing.T) {
	t.Parallel()
	domain := helpers.MustAllocateDomain(t)
	ctx, cancel := context.WithTimeout(context.Background(), typeMatrixTestTimeout)
	defer cancel()
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)
	runShapeExtraLabel(t, ctx, domain, "shape-mesh")
}
