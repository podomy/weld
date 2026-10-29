// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package ros_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/podomy/weld/internal/types"
	"github.com/podomy/weld/testbed/live/helpers"
)

// Static Marker type hash from ros2 topic info -v on weld-jazzy.
//
// This is the RIHS01 hash reported for visualization_msgs/msg/Marker on
// domain 42. Live probing via ros2 topic info -v is preferred at runtime;
// this value is the fallback when the probe cannot run. Value was captured
// from weld-jazzy Jazzy.
const vizMarkerHash = "RIHS01_45b13ccf791f225962bf74e746f9644518855d783a6f42ba0cc14fde2b4f3ce0"

// Small Marker sentinels for the live gate.
//
// The Marker keeps every sequence empty and every nested string empty so
// the sample stays small. Header frame_id carries the per-run live ID so
// stale runs never match. Pose holds fixed 1.0 plus 2.0 plus 3.0 with
// identity orientation, scale stays 1.0 on all axes, color stays opaque
// red, and lifetime stays zero which means forever.
const (
	vizMarkerNS             = "weld"
	vizMarkerID     int32   = 7
	vizMarkerType   int32   = 1
	vizMarkerAction int32   = 0
	vizMarkerPosX   float64 = 1.0
	vizMarkerPosY   float64 = 2.0
	vizMarkerPosZ   float64 = 3.0
	vizMarkerOriW   float64 = 1.0
	vizMarkerScale  float64 = 1.0
	vizMarkerRed    float32 = 1.0
	vizMarkerAlpha  float32 = 1.0
	vizMarkerZero   float32 = 0.0
)

// yamlVizMarker returns ROS YAML for the small Marker.
//
// Only the header plus ns plus id plus type plus action plus pose plus
// scale plus color plus lifetime are set. The peer fills the remaining
// fields with empty defaults, which matches the empty sequences and empty
// strings used by the Weld codec below.
func yamlVizMarker(live string) string {
	return fmt.Sprintf("{header: {stamp: {sec: 0, nanosec: 0}, frame_id: '%s'}, ns: 'weld', id: 7, type: 1, action: 0, pose: {position: {x: 1.0, y: 2.0, z: 3.0}, orientation: {x: 0.0, y: 0.0, z: 0.0, w: 1.0}}, scale: {x: 1.0, y: 1.0, z: 1.0}, color: {r: 1.0, g: 0.0, b: 0.0, a: 1.0}, lifetime: {sec: 0, nanosec: 0}}", live)
}

// encodeVizMarker encodes the small Marker as XCDR1 bytes.
//
// Field order follows visualization_msgs/msg/Marker on Jazzy: header,
// ns, id, type, action, pose, scale, color, lifetime, frame_locked,
// points, colors, texture_resource, texture header plus format plus data,
// uv_coordinates, text, mesh_resource, mesh_file filename plus data, and
// mesh_use_embedded_materials. Empty sequences and strings keep the sample
// small. Only string and sequence writes can fail.
// encodeVizMarkerPose writes header plus ns plus id plus pose.
func encodeVizMarkerPose(enc *types.Encoder, live string) error {
	err := encodeMatrixHeader(enc, live)
	if err != nil {
		return fmt.Errorf("encode marker header: %w", err)
	}
	err = enc.WriteString(vizMarkerNS)
	if err != nil {
		return fmt.Errorf("encode marker ns: %w", err)
	}
	enc.WriteInt32(vizMarkerID)
	enc.WriteInt32(vizMarkerType)
	enc.WriteInt32(vizMarkerAction)
	enc.WriteFloat64(vizMarkerPosX)
	enc.WriteFloat64(vizMarkerPosY)
	enc.WriteFloat64(vizMarkerPosZ)
	enc.WriteFloat64(0.0)
	enc.WriteFloat64(0.0)
	enc.WriteFloat64(0.0)
	enc.WriteFloat64(vizMarkerOriW)
	return nil
}

// encodeVizMarkerDisplay writes scale plus color plus lifetime.
func encodeVizMarkerDisplay(enc *types.Encoder) {
	enc.WriteFloat64(vizMarkerScale)
	enc.WriteFloat64(vizMarkerScale)
	enc.WriteFloat64(vizMarkerScale)
	enc.WriteFloat32(vizMarkerRed)
	enc.WriteFloat32(vizMarkerZero)
	enc.WriteFloat32(vizMarkerZero)
	enc.WriteFloat32(vizMarkerAlpha)
	enc.WriteInt32(0)
	enc.WriteUint32(0)
	enc.WriteBool(false)
}

// encodeVizMarkerPoints writes points plus colors plus texture resource.
func encodeVizMarkerPoints(enc *types.Encoder) error {
	err := enc.WriteSequenceLength(0)
	if err != nil {
		return fmt.Errorf("encode marker points: %w", err)
	}
	err = enc.WriteSequenceLength(0)
	if err != nil {
		return fmt.Errorf("encode marker colors: %w", err)
	}
	err = enc.WriteString("")
	if err != nil {
		return fmt.Errorf("encode marker texture resource: %w", err)
	}
	return nil
}

// encodeVizMarkerTexture writes texture header plus format plus data.
func encodeVizMarkerTexture(enc *types.Encoder) error {
	enc.WriteInt32(0)
	enc.WriteUint32(0)
	err := enc.WriteString("")
	if err != nil {
		return fmt.Errorf("encode marker texture frame: %w", err)
	}
	err = enc.WriteString("")
	if err != nil {
		return fmt.Errorf("encode marker texture format: %w", err)
	}
	err = enc.WriteOctetSequence([]byte{})
	if err != nil {
		return fmt.Errorf("encode marker texture data: %w", err)
	}
	return nil
}

// encodeVizMarkerMesh writes uv plus text plus mesh plus embedded flag.
func encodeVizMarkerMesh(enc *types.Encoder) error {
	err := enc.WriteSequenceLength(0)
	if err != nil {
		return fmt.Errorf("encode marker uv: %w", err)
	}
	err = enc.WriteString("")
	if err != nil {
		return fmt.Errorf("encode marker text: %w", err)
	}
	err = enc.WriteString("")
	if err != nil {
		return fmt.Errorf("encode marker mesh resource: %w", err)
	}
	err = enc.WriteString("")
	if err != nil {
		return fmt.Errorf("encode marker mesh filename: %w", err)
	}
	err = enc.WriteOctetSequence([]byte{})
	if err != nil {
		return fmt.Errorf("encode marker mesh data: %w", err)
	}
	return nil
}

func encodeVizMarker(live string) ([]byte, error) {
	enc := types.NewEncoder(true)
	err := encodeVizMarkerPose(enc, live)
	if err != nil {
		return nil, err
	}
	encodeVizMarkerDisplay(enc)
	err = encodeVizMarkerPoints(enc)
	if err != nil {
		return nil, err
	}
	err = encodeVizMarkerTexture(enc)
	if err != nil {
		return nil, err
	}
	err = encodeVizMarkerMesh(enc)
	if err != nil {
		return nil, err
	}
	enc.WriteBool(false)
	return enc.Bytes(), nil
}

// decodeVizMarker decodes and checks the small Marker.
//
// It verifies the live frame_id plus every fixed sentinel and requires
// empty sequences and empty strings elsewhere. A trailing run of zero pad
// bytes is accepted, matching the Bool matrix case, because Bytes pads the
// wire form to a 4 byte boundary.
// decodeVizIdentity checks frame plus ns plus id plus type plus action.
func decodeVizIdentity(dec *types.Decoder, live string) error {
	frame, err := decodeMatrixHeader(dec)
	if err != nil {
		return fmt.Errorf("decode marker frame: %w", err)
	}
	if frame != live {
		return fmt.Errorf("marker frame %q wants %q", frame, live)
	}
	ns, err := dec.ReadString()
	if err != nil {
		return fmt.Errorf("decode marker ns: %w", err)
	}
	if ns != vizMarkerNS {
		return fmt.Errorf("marker ns %q wants %q", ns, vizMarkerNS)
	}
	id, err := dec.ReadInt32()
	if err != nil {
		return fmt.Errorf("decode marker id: %w", err)
	}
	if id != vizMarkerID {
		return fmt.Errorf("marker id %d wants %d", id, vizMarkerID)
	}
	mtype, err := dec.ReadInt32()
	if err != nil {
		return fmt.Errorf("decode marker type: %w", err)
	}
	if mtype != vizMarkerType {
		return fmt.Errorf("marker type %d wants %d", mtype, vizMarkerType)
	}
	action, err := dec.ReadInt32()
	if err != nil {
		return fmt.Errorf("decode marker action: %w", err)
	}
	if action != vizMarkerAction {
		return fmt.Errorf("marker action %d wants %d", action, vizMarkerAction)
	}
	return nil
}

// readVizPos reads marker position xyz with checks.
func readVizPos(dec *types.Decoder) (float64, float64, float64, error) {
	px, py, pz, err := helpers.ReadFloat64Triple(dec, "decode marker pos x", "decode marker pos y", "decode marker pos z")
	if err != nil {
		return 0, 0, 0, fmt.Errorf("read marker pos: %w", err)
	}
	return px, py, pz, nil
}

// readVizOri reads marker orientation xyzw with checks.
func readVizOri(dec *types.Decoder) (float64, float64, float64, float64, error) {
	ox, oy, oz, ow, err := helpers.ReadFloat64Quad(dec, "decode marker ori x", "decode marker ori y", "decode marker ori z", "decode marker ori w")
	if err != nil {
		return 0, 0, 0, 0, fmt.Errorf("read marker ori: %w", err)
	}
	return ox, oy, oz, ow, nil
}

// decodeVizPose checks position plus orientation.
func decodeVizPose(dec *types.Decoder) error {
	px, py, pz, err := readVizPos(dec)
	if err != nil {
		return err
	}
	if px != vizMarkerPosX || py != vizMarkerPosY || pz != vizMarkerPosZ {
		return fmt.Errorf("marker pos (%v, %v, %v) wants (1.0, 2.0, 3.0)", px, py, pz)
	}
	ox, oy, oz, ow, err := readVizOri(dec)
	if err != nil {
		return err
	}
	if ox != 0.0 || oy != 0.0 || oz != 0.0 || ow != vizMarkerOriW {
		return fmt.Errorf("marker ori (%v, %v, %v, %v) wants (0, 0, 0, 1)", ox, oy, oz, ow)
	}
	return nil
}

// readVizScale reads marker scale xyz with checks.
func readVizScale(dec *types.Decoder) error {
	sx, err := dec.ReadFloat64()
	if err != nil {
		return fmt.Errorf("decode marker scale x: %w", err)
	}
	sy, err := dec.ReadFloat64()
	if err != nil {
		return fmt.Errorf("decode marker scale y: %w", err)
	}
	sz, err := dec.ReadFloat64()
	if err != nil {
		return fmt.Errorf("decode marker scale z: %w", err)
	}
	if sx != vizMarkerScale || sy != vizMarkerScale || sz != vizMarkerScale {
		return fmt.Errorf("marker scale (%v, %v, %v) wants (1.0, 1.0, 1.0)", sx, sy, sz)
	}
	return nil
}

// decodeVizScaleColor checks scale plus color plus lifetime plus lock.
func decodeVizScaleColor(dec *types.Decoder) error {
	err := readVizScale(dec)
	if err != nil {
		return err
	}
	err = readVizColor(dec)
	if err != nil {
		return err
	}
	err = readVizLifetime(dec)
	if err != nil {
		return err
	}
	return nil
}

// readVizColor reads marker color rgba with checks.
func readVizColor(dec *types.Decoder) error {
	cr, err := dec.ReadFloat32()
	if err != nil {
		return fmt.Errorf("decode marker color r: %w", err)
	}
	cg, err := dec.ReadFloat32()
	if err != nil {
		return fmt.Errorf("decode marker color g: %w", err)
	}
	cb, err := dec.ReadFloat32()
	if err != nil {
		return fmt.Errorf("decode marker color b: %w", err)
	}
	ca, err := dec.ReadFloat32()
	if err != nil {
		return fmt.Errorf("decode marker color a: %w", err)
	}
	if cr != vizMarkerRed || cg != vizMarkerZero || cb != vizMarkerZero || ca != vizMarkerAlpha {
		return fmt.Errorf("marker color (%v, %v, %v, %v) wants (1, 0, 0, 1)", cr, cg, cb, ca)
	}
	return nil
}

// readVizLifetime reads marker lifetime plus lock with checks.
func readVizLifetime(dec *types.Decoder) error {
	lsec, err := dec.ReadInt32()
	if err != nil {
		return fmt.Errorf("decode marker lifetime sec: %w", err)
	}
	lnsec, err := dec.ReadUint32()
	if err != nil {
		return fmt.Errorf("decode marker lifetime nanosec: %w", err)
	}
	if lsec != 0 || lnsec != 0 {
		return fmt.Errorf("marker lifetime (%d, %d) wants (0, 0)", lsec, lnsec)
	}
	locked, err := dec.ReadBool()
	if err != nil {
		return fmt.Errorf("decode marker frame locked: %w", err)
	}
	if locked {
		return errors.New("marker frame locked true wants false") //nolint:wrapcheck // new sentinel error, nothing to wrap
	}
	return nil
}

// decodeVizPointsTexture checks points plus colors plus texture.
func decodeVizPointsTexture(dec *types.Decoder) error {
	npts, err := dec.ReadSequenceLength()
	if err != nil {
		return fmt.Errorf("decode marker points: %w", err)
	}
	if npts != 0 {
		return fmt.Errorf("marker points len %d wants 0", npts)
	}
	ncol, err := dec.ReadSequenceLength()
	if err != nil {
		return fmt.Errorf("decode marker colors: %w", err)
	}
	if ncol != 0 {
		return fmt.Errorf("marker colors len %d wants 0", ncol)
	}
	texRes, err := dec.ReadString()
	if err != nil {
		return fmt.Errorf("decode marker texture resource: %w", err)
	}
	if texRes != "" {
		return fmt.Errorf("marker texture resource %q wants empty", texRes)
	}
	err = readVizTextureStamp(dec)
	if err != nil {
		return err
	}
	return nil
}

// readVizTextureStamp checks texture stamp plus frame plus format plus data.
func readVizTextureStamp(dec *types.Decoder) error {
	tsec, err := dec.ReadInt32()
	if err != nil {
		return fmt.Errorf("decode marker texture sec: %w", err)
	}
	tnsec, err := dec.ReadUint32()
	if err != nil {
		return fmt.Errorf("decode marker texture nanosec: %w", err)
	}
	if tsec != 0 || tnsec != 0 {
		return fmt.Errorf("marker texture stamp (%d, %d) wants (0, 0)", tsec, tnsec)
	}
	tframe, err := dec.ReadString()
	if err != nil {
		return fmt.Errorf("decode marker texture frame: %w", err)
	}
	if tframe != "" {
		return fmt.Errorf("marker texture frame %q wants empty", tframe)
	}
	tfmt, err := dec.ReadString()
	if err != nil {
		return fmt.Errorf("decode marker texture format: %w", err)
	}
	if tfmt != "" {
		return fmt.Errorf("marker texture format %q wants empty", tfmt)
	}
	tdata, err := dec.ReadOctetSequence()
	if err != nil {
		return fmt.Errorf("decode marker texture data: %w", err)
	}
	if len(tdata) != 0 {
		return fmt.Errorf("marker texture data len %d wants 0", len(tdata))
	}
	return nil
}

// readVizMesh checks mesh resource plus filename plus data plus materials.
func readVizMesh(dec *types.Decoder) error {
	meshRes, err := dec.ReadString()
	if err != nil {
		return fmt.Errorf("decode marker mesh resource: %w", err)
	}
	if meshRes != "" {
		return fmt.Errorf("marker mesh resource %q wants empty", meshRes)
	}
	meshName, err := dec.ReadString()
	if err != nil {
		return fmt.Errorf("decode marker mesh filename: %w", err)
	}
	if meshName != "" {
		return fmt.Errorf("marker mesh filename %q wants empty", meshName)
	}
	meshData, err := dec.ReadOctetSequence()
	if err != nil {
		return fmt.Errorf("decode marker mesh data: %w", err)
	}
	if len(meshData) != 0 {
		return fmt.Errorf("marker mesh data len %d wants 0", len(meshData))
	}
	memb, err := dec.ReadBool()
	if err != nil {
		return fmt.Errorf("decode marker mesh materials: %w", err)
	}
	if memb {
		return errors.New("marker mesh materials true wants false") //nolint:wrapcheck // new sentinel error, nothing to wrap
	}
	return nil
}

// decodeVizMeshTail checks uv plus text plus mesh plus pad.
// decodeVizMeshTail checks uv plus text plus mesh plus pad.
func decodeVizMeshTail(dec *types.Decoder) error {
	nuv, err := dec.ReadSequenceLength()
	if err != nil {
		return fmt.Errorf("decode marker uv: %w", err)
	}
	if nuv != 0 {
		return fmt.Errorf("marker uv len %d wants 0", nuv)
	}
	text, err := dec.ReadString()
	if err != nil {
		return fmt.Errorf("decode marker text: %w", err)
	}
	if text != "" {
		return fmt.Errorf("marker text %q wants empty", text)
	}
	err = readVizMesh(dec)
	if err != nil {
		return err
	}
	for dec.Remaining() > 0 {
		pad, err := dec.ReadUint8()
		if err != nil {
			return fmt.Errorf("decode marker pad: %w", err)
		}
		if pad != 0 {
			return fmt.Errorf("marker pad 0x%02x wants zero", pad)
		}
	}
	return nil
}

// decodeVizMarker decodes and checks the small Marker.
func decodeVizMarker(payload []byte, live string) error {
	dec, err := types.NewDecoder(payload)
	if err != nil {
		return fmt.Errorf("decode marker header: %w", err)
	}
	err = decodeVizIdentity(dec, live)
	if err != nil {
		return err
	}
	err = decodeVizPose(dec)
	if err != nil {
		return err
	}
	err = decodeVizScaleColor(dec)
	if err != nil {
		return err
	}
	err = decodeVizPointsTexture(dec)
	if err != nil {
		return err
	}
	err = decodeVizMeshTail(dec)
	if err != nil {
		return err
	}
	return nil
}

// wantVizMarker returns the live ID expected in echo output.
//
// Header frame_id carries the per-run live ID, so the echo stream must
// hold it once the Weld pub arrives.
func wantVizMarker(live string) string {
	return live
}

// TestROSTypeVisualizationMarker checks Marker forward plus reverse.
//
// Forward starts ros2 topic pub for visualization_msgs Marker and decodes
// via NewDecoder on the Weld take path. Reverse publishes the small Marker
// via Weld and watches ros2 topic echo. Header frame_id carries the
// per-run live ID so stale runs never match. It skips when the peer lacks
// the type and never starts the compose stack.
func TestROSTypeVisualizationMarker(t *testing.T) {
	t.Parallel()
	domain := helpers.MustAllocateDomain(t)
	ctx, cancel := context.WithTimeout(context.Background(), typeMatrixTestTimeout)
	defer cancel()
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)
	skipIfNoTypeInterface(t, ctx, domain, "visualization_msgs/msg/Marker")
	tc := typeMatrixCase{
		Label:      "viz-marker",
		RosType:    "visualization_msgs/msg/Marker",
		IDLName:    "visualization_msgs__msg__Marker",
		staticHash: vizMarkerHash,
		yaml:       yamlVizMarker,
		encode:     encodeVizMarker,
		decode:     decodeVizMarker,
		want:       wantVizMarker,
	}
	liveFwd := helpers.LiveID("ros-fwd-viz-marker")
	topicFwd := helpers.LiveTopic(liveFwd)
	runMatrixForward(t, ctx, domain, tc, liveFwd, topicFwd)
	liveRev := helpers.LiveID("ros-rev-viz-marker")
	topicRev := helpers.LiveTopic(liveRev)
	runMatrixReverse(t, ctx, domain, tc, liveRev, topicRev)
}
