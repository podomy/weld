// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

//nolint:cyclop,dupl,gocognit,nestif // live gate nests marker checks to keep wire assertions together
package ros_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/podomy/weld/internal/types"
	"github.com/podomy/weld/testbed/live/helpers"
)

// Viz extra coverage for remaining visualization_msgs.
//
// Live small samples keep every nested array empty except MarkerArray,
// which needs one Marker to carry the per-run live ID in its header.
// This keeps DDS samples small and domain 42 quiet. Full nested paths
// such as Control with Marker, InteractiveMarker with menu plus control
// plus Marker, Init with InteractiveMarker, Update with poses plus erases,
// and ImageMarker with points are covered by pure-Go local codec round
// trips below without a ROS peer. The split is documented here so a
// reader knows live proves interop and local proves nesting.

// Static type hashes from ros2 topic info -v on weld-jazzy.
//
// These are the RIHS01 hashes reported for each ROS type on domain 42.
// Live probing via ros2 topic info -v is preferred at runtime, these values
// are the fallback when the probe cannot run. Values were captured from
// weld-jazzy Jazzy.
const (
	vizExtraHashMarkerArray           = "RIHS01_86cb8800b6fb05b5eff1abd7a56f62a5641d3ae9a1c29e78e67e704f1d067dcf"
	vizExtraHashMenuEntry             = "RIHS01_22170c387c70fd4236232ec902de8604e72ff027342c7c0f28ad9f68c64c51d6"
	vizExtraHashImageMarker           = "RIHS01_603152491ef2331c200a5305230d31f6e8704875944b388da0f547c415d11836"
	vizExtraHashControl               = "RIHS01_60e2fa36344f5f4791b24a809542a18bffd555f563550d4b22b3bbfc31ec0ed5"
	vizExtraHashInteractiveMarker     = "RIHS01_3d5b51448b51d73b0f395b94d259edd3a5d269ae9f7d9fd5cceb9ae4b72be346"
	vizExtraHashInteractiveMarkerPose = "RIHS01_c60e9a4407d5f709a63e0fe9caea324aee08fe717cd090209ebe35012ce7cb66"
	vizExtraHashFeedback              = "RIHS01_6cc48741df9f05d19ba7d9ea3101e9fcd1309c9d6bda3c55668ba607492f725e"
	vizExtraHashInit                  = "RIHS01_23fda1b3373b154d9d6408dd7b9f8129b2a2b76b905ee421e8c4109d8bf71f78"
	vizExtraHashUpdate                = "RIHS01_0a8b000c4fd4d50876ac716a7de018911599a2f015795388e956a2ca8b0c54f0"
)

// Viz extra fixed sentinels.
//
// Header types carry the per-run live ID in frame_id. MenuEntry carries it
// in title. Control carries it in name. Init and Update carry it in
// server_id. Remaining fields stay fixed so Weld take and ros2 echo checks
// are deterministic. Float values use exact binary fractions.
const (
	vizExtraMenuID           uint32  = 7
	vizExtraMenuParent       uint32  = 0
	vizExtraMenuCommandType  uint8   = 0
	vizExtraImageNS          string  = "weld"
	vizExtraImageID          int32   = 7
	vizExtraImageType        int32   = 0
	vizExtraImageAction      int32   = 0
	vizExtraImagePosX        float64 = 1.0
	vizExtraImagePosY        float64 = 2.0
	vizExtraImagePosZ        float64 = 0.0
	vizExtraImageScale       float32 = 1.0
	vizExtraImageRed         float32 = 1.0
	vizExtraImageZero        float32 = 0.0
	vizExtraImageAlpha       float32 = 1.0
	vizExtraImageFilled      uint8   = 0
	vizExtraControlOriMode   uint8   = 0
	vizExtraControlInterMode uint8   = 1
	vizExtraMarkerName       string  = "weld"
	vizExtraMarkerDesc       string  = ""
	vizExtraMarkerScale      float32 = 1.0
	vizExtraPoseName         string  = "weld"
	vizExtraFeedbackClient   string  = "weld"
	vizExtraFeedbackMarker   string  = "weld"
	vizExtraFeedbackControl  string  = "weld"
	vizExtraFeedbackEvent    uint8   = 1
	vizExtraFeedbackMenuID   uint32  = 0
	vizExtraInitSeq          uint64  = 0
	vizExtraUpdateSeq        uint64  = 0
	vizExtraUpdateType       uint8   = 1
)

// yamlVizExtraMarkerArray returns ROS YAML for MarkerArray with one Marker.
//
// The single Marker reuses the small Marker sentinels with header frame_id
// carrying the per-run live ID. Remaining Marker fields use empty defaults.
func yamlVizExtraMarkerArray(live string) string {
	return fmt.Sprintf("{markers: [{header: {stamp: {sec: 0, nanosec: 0}, frame_id: '%s'}, ns: 'weld', id: 7, type: 1, action: 0, pose: {position: {x: 1.0, y: 2.0, z: 3.0}, orientation: {x: 0.0, y: 0.0, z: 0.0, w: 1.0}}, scale: {x: 1.0, y: 1.0, z: 1.0}, color: {r: 1.0, g: 0.0, b: 0.0, a: 1.0}, lifetime: {sec: 0, nanosec: 0}}]}", live)
}

// yamlVizExtraMenuEntry returns ROS YAML for MenuEntry with live title.
func yamlVizExtraMenuEntry(live string) string {
	return fmt.Sprintf("{id: 7, parent_id: 0, title: '%s', command: '', command_type: 0}", live)
}

// yamlVizExtraImageMarker returns ROS YAML for small ImageMarker.
func yamlVizExtraImageMarker(live string) string {
	return fmt.Sprintf("{header: {stamp: {sec: 0, nanosec: 0}, frame_id: '%s'}, ns: 'weld', id: 7, type: 0, action: 0, position: {x: 1.0, y: 2.0, z: 0.0}, scale: 1.0, outline_color: {r: 1.0, g: 0.0, b: 0.0, a: 1.0}, filled: 0, fill_color: {r: 0.0, g: 0.0, b: 0.0, a: 0.0}, lifetime: {sec: 0, nanosec: 0}, points: [], outline_colors: []}", live)
}

// yamlVizExtraControl returns ROS YAML for small InteractiveMarkerControl.
func yamlVizExtraControl(live string) string {
	return fmt.Sprintf("{name: '%s', orientation: {x: 0.0, y: 0.0, z: 0.0, w: 1.0}, orientation_mode: 0, interaction_mode: 1, always_visible: false, markers: [], independent_marker_orientation: false, description: ''}", live)
}

// yamlVizExtraInteractiveMarker returns ROS YAML for small InteractiveMarker.
func yamlVizExtraInteractiveMarker(live string) string {
	return fmt.Sprintf("{header: {stamp: {sec: 0, nanosec: 0}, frame_id: '%s'}, pose: {position: {x: 1.0, y: 2.0, z: 3.0}, orientation: {x: 0.0, y: 0.0, z: 0.0, w: 1.0}}, name: 'weld', description: '', scale: 1.0, menu_entries: [], controls: []}", live)
}

// yamlVizExtraPose returns ROS YAML for InteractiveMarkerPose.
func yamlVizExtraPose(live string) string {
	return fmt.Sprintf("{header: {stamp: {sec: 0, nanosec: 0}, frame_id: '%s'}, pose: {position: {x: 1.0, y: 2.0, z: 3.0}, orientation: {x: 0.0, y: 0.0, z: 0.0, w: 1.0}}, name: 'weld'}", live)
}

// yamlVizExtraFeedback returns ROS YAML for InteractiveMarkerFeedback.
func yamlVizExtraFeedback(live string) string {
	return fmt.Sprintf("{header: {stamp: {sec: 0, nanosec: 0}, frame_id: '%s'}, client_id: 'weld', marker_name: 'weld', control_name: 'weld', event_type: 1, pose: {position: {x: 1.0, y: 2.0, z: 3.0}, orientation: {x: 0.0, y: 0.0, z: 0.0, w: 1.0}}, menu_entry_id: 0, mouse_point: {x: 0.0, y: 0.0, z: 0.0}, mouse_point_valid: false}", live)
}

// yamlVizExtraInit returns ROS YAML for InteractiveMarkerInit with no markers.
func yamlVizExtraInit(live string) string {
	return fmt.Sprintf("{server_id: '%s', seq_num: 0, markers: []}", live)
}

// yamlVizExtraUpdate returns ROS YAML for InteractiveMarkerUpdate with no payload.
func yamlVizExtraUpdate(live string) string {
	return fmt.Sprintf("{server_id: '%s', seq_num: 0, type: 1, markers: [], poses: [], erases: []}", live)
}

// encodeVizExtraMarkerInto writes one small Marker into enc.
//
// It reuses the Marker field helpers from the Marker gate so the nested
// path matches the standalone Marker wire form exactly. Only string and
// sequence writes can fail.
func encodeVizExtraMarkerInto(enc *types.Encoder, live string) error {
	err := encodeVizMarkerPose(enc, live)
	if err != nil {
		return fmt.Errorf("encode nested marker pose: %w", err)
	}
	encodeVizMarkerDisplay(enc)
	err = encodeVizMarkerPoints(enc)
	if err != nil {
		return fmt.Errorf("encode nested marker points: %w", err)
	}
	err = encodeVizMarkerTexture(enc)
	if err != nil {
		return fmt.Errorf("encode nested marker texture: %w", err)
	}
	err = encodeVizMarkerMesh(enc)
	if err != nil {
		return fmt.Errorf("encode nested marker mesh: %w", err)
	}
	enc.WriteBool(false)
	return nil
}

// decodeVizExtraMarkerTailNoPad checks uv plus text plus mesh without pad.
//
// It matches decodeVizMeshTail except it stops after the embedded materials
// flag so the caller owns trailing pad or following fields. This is needed
// for nested Markers where outer fields or outer pad follow the Marker.
func decodeVizExtraMarkerTailNoPad(dec *types.Decoder) error {
	nuv, err := dec.ReadSequenceLength()
	if err != nil {
		return fmt.Errorf("decode nested marker uv: %w", err)
	}
	if nuv != 0 {
		return fmt.Errorf("nested marker uv len %d wants 0", nuv)
	}
	text, err := dec.ReadString()
	if err != nil {
		return fmt.Errorf("decode nested marker text: %w", err)
	}
	if text != "" {
		return fmt.Errorf("nested marker text %q wants empty", text)
	}
	err = readVizMesh(dec)
	if err != nil {
		return err
	}
	return nil
}

// decodeVizExtraMarkerInner decodes one nested Marker without outer pad.
//
// It verifies the live frame_id plus every fixed sentinel and requires
// empty sequences and empty strings elsewhere. The caller owns trailing
// pad handling because a nested Marker may be followed by outer fields.
func decodeVizExtraMarkerInner(dec *types.Decoder, live string) error {
	err := decodeVizIdentity(dec, live)
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
	err = decodeVizExtraMarkerTailNoPad(dec)
	if err != nil {
		return err
	}
	return nil
}

// encodeVizExtraMarkerArray encodes MarkerArray with one small Marker.
func encodeVizExtraMarkerArray(live string) ([]byte, error) {
	enc := types.NewEncoder(true)
	err := enc.WriteSequenceLength(1)
	if err != nil {
		return nil, fmt.Errorf("encode marker array len: %w", err)
	}
	err = encodeVizExtraMarkerInto(enc, live)
	if err != nil {
		return nil, err
	}
	return enc.Bytes(), nil
}

// decodeVizExtraMarkerArray decodes MarkerArray with one small Marker.
func decodeVizExtraMarkerArray(payload []byte, live string) error {
	dec, err := types.NewDecoder(payload)
	if err != nil {
		return fmt.Errorf("decode marker array header: %w", err)
	}
	n, err := dec.ReadSequenceLength()
	if err != nil {
		return fmt.Errorf("decode marker array len: %w", err)
	}
	if n != 1 {
		return fmt.Errorf("marker array len %d wants 1", n)
	}
	err = decodeVizExtraMarkerInner(dec, live)
	if err != nil {
		return err
	}
	for dec.Remaining() > 0 {
		pad, err := dec.ReadUint8()
		if err != nil {
			return fmt.Errorf("decode marker array pad: %w", err)
		}
		if pad != 0 {
			return fmt.Errorf("marker array pad 0x%02x wants zero", pad)
		}
	}
	return nil
}

// wantVizExtraMarkerArray returns the live ID expected in echo output.
func wantVizExtraMarkerArray(live string) string {
	return live
}

// encodeVizExtraMenuEntry encodes MenuEntry with live title.
func encodeVizExtraMenuEntry(live string) ([]byte, error) {
	enc := types.NewEncoder(true)
	enc.WriteUint32(vizExtraMenuID)
	enc.WriteUint32(vizExtraMenuParent)
	err := enc.WriteString(live)
	if err != nil {
		return nil, fmt.Errorf("encode menu title: %w", err)
	}
	err = enc.WriteString("")
	if err != nil {
		return nil, fmt.Errorf("encode menu command: %w", err)
	}
	enc.WriteUint8(vizExtraMenuCommandType)
	return enc.Bytes(), nil
}

// decodeVizExtraMenuEntry decodes MenuEntry with live title.
func decodeVizExtraMenuEntry(payload []byte, live string) error {
	dec, err := types.NewDecoder(payload)
	if err != nil {
		return fmt.Errorf("decode menu header: %w", err)
	}
	id, err := dec.ReadUint32()
	if err != nil {
		return fmt.Errorf("decode menu id: %w", err)
	}
	if id != vizExtraMenuID {
		return fmt.Errorf("menu id %d wants 7", id)
	}
	parent, err := dec.ReadUint32()
	if err != nil {
		return fmt.Errorf("decode menu parent: %w", err)
	}
	if parent != vizExtraMenuParent {
		return fmt.Errorf("menu parent %d wants 0", parent)
	}
	title, err := dec.ReadString()
	if err != nil {
		return fmt.Errorf("decode menu title: %w", err)
	}
	if title != live {
		return fmt.Errorf("menu title %q wants %q", title, live)
	}
	command, err := dec.ReadString()
	if err != nil {
		return fmt.Errorf("decode menu command: %w", err)
	}
	if command != "" {
		return fmt.Errorf("menu command %q wants empty", command)
	}
	ctype, err := dec.ReadUint8()
	if err != nil {
		return fmt.Errorf("decode menu command type: %w", err)
	}
	if ctype != vizExtraMenuCommandType {
		return fmt.Errorf("menu command type %d wants 0", ctype)
	}
	for dec.Remaining() > 0 {
		pad, err := dec.ReadUint8()
		if err != nil {
			return fmt.Errorf("decode menu pad: %w", err)
		}
		if pad != 0 {
			return fmt.Errorf("menu pad 0x%02x wants zero", pad)
		}
	}
	return nil
}

// wantVizExtraMenuEntry returns the live ID expected in echo output.
func wantVizExtraMenuEntry(live string) string {
	return live
}

// encodeVizExtraImagePose writes header plus ns plus id plus position.
func encodeVizExtraImagePose(enc *types.Encoder, live string) error {
	err := encodeMatrixHeader(enc, live)
	if err != nil {
		return fmt.Errorf("encode image marker header: %w", err)
	}
	err = enc.WriteString(vizExtraImageNS)
	if err != nil {
		return fmt.Errorf("encode image marker ns: %w", err)
	}
	enc.WriteInt32(vizExtraImageID)
	enc.WriteInt32(vizExtraImageType)
	enc.WriteInt32(vizExtraImageAction)
	enc.WriteFloat64(vizExtraImagePosX)
	enc.WriteFloat64(vizExtraImagePosY)
	enc.WriteFloat64(vizExtraImagePosZ)
	return nil
}

// encodeVizExtraImageColors writes scale plus outline plus filled plus fill.
func encodeVizExtraImageColors(enc *types.Encoder) {
	enc.WriteFloat32(vizExtraImageScale)
	enc.WriteFloat32(vizExtraImageRed)
	enc.WriteFloat32(vizExtraImageZero)
	enc.WriteFloat32(vizExtraImageZero)
	enc.WriteFloat32(vizExtraImageAlpha)
	enc.WriteUint8(vizExtraImageFilled)
	enc.WriteFloat32(vizExtraImageZero)
	enc.WriteFloat32(vizExtraImageZero)
	enc.WriteFloat32(vizExtraImageZero)
	enc.WriteFloat32(vizExtraImageZero)
	enc.WriteInt32(0)
	enc.WriteUint32(0)
}

// encodeVizExtraImageMarker encodes small ImageMarker with empty sequences.
func encodeVizExtraImageMarker(live string) ([]byte, error) {
	enc := types.NewEncoder(true)
	err := encodeVizExtraImagePose(enc, live)
	if err != nil {
		return nil, err
	}
	encodeVizExtraImageColors(enc)
	err = enc.WriteSequenceLength(0)
	if err != nil {
		return nil, fmt.Errorf("encode image points len: %w", err)
	}
	err = enc.WriteSequenceLength(0)
	if err != nil {
		return nil, fmt.Errorf("encode image outline colors len: %w", err)
	}
	return enc.Bytes(), nil
}

// decodeVizExtraImagePose checks header plus ns plus id plus position.
func decodeVizExtraImagePose(dec *types.Decoder, live string) error {
	frame, err := decodeMatrixHeader(dec)
	if err != nil {
		return fmt.Errorf("decode image frame: %w", err)
	}
	if frame != live {
		return fmt.Errorf("image frame %q wants %q", frame, live)
	}
	ns, err := dec.ReadString()
	if err != nil {
		return fmt.Errorf("decode image ns: %w", err)
	}
	if ns != vizExtraImageNS {
		return fmt.Errorf("image ns %q wants %q", ns, vizExtraImageNS)
	}
	id, err := dec.ReadInt32()
	if err != nil {
		return fmt.Errorf("decode image id: %w", err)
	}
	if id != vizExtraImageID {
		return fmt.Errorf("image id %d wants 7", id)
	}
	mtype, err := dec.ReadInt32()
	if err != nil {
		return fmt.Errorf("decode image type: %w", err)
	}
	if mtype != vizExtraImageType {
		return fmt.Errorf("image type %d wants 0", mtype)
	}
	action, err := dec.ReadInt32()
	if err != nil {
		return fmt.Errorf("decode image action: %w", err)
	}
	if action != vizExtraImageAction {
		return fmt.Errorf("image action %d wants 0", action)
	}
	px, err := dec.ReadFloat64()
	if err != nil {
		return fmt.Errorf("decode image pos x: %w", err)
	}
	py, err := dec.ReadFloat64()
	if err != nil {
		return fmt.Errorf("decode image pos y: %w", err)
	}
	pz, err := dec.ReadFloat64()
	if err != nil {
		return fmt.Errorf("decode image pos z: %w", err)
	}
	if px != vizExtraImagePosX || py != vizExtraImagePosY || pz != vizExtraImagePosZ {
		return fmt.Errorf("image pos (%v, %v, %v) wants (1.0, 2.0, 0.0)", px, py, pz)
	}
	return nil
}

// decodeVizExtraImageColors checks scale plus outline plus filled plus fill.
func decodeVizExtraImageColors(dec *types.Decoder) error {
	scale, err := dec.ReadFloat32()
	if err != nil {
		return fmt.Errorf("decode image scale: %w", err)
	}
	if scale != vizExtraImageScale {
		return fmt.Errorf("image scale %v wants 1.0", scale)
	}
	r, err := dec.ReadFloat32()
	if err != nil {
		return fmt.Errorf("decode image outline r: %w", err)
	}
	g, err := dec.ReadFloat32()
	if err != nil {
		return fmt.Errorf("decode image outline g: %w", err)
	}
	b, err := dec.ReadFloat32()
	if err != nil {
		return fmt.Errorf("decode image outline b: %w", err)
	}
	a, err := dec.ReadFloat32()
	if err != nil {
		return fmt.Errorf("decode image outline a: %w", err)
	}
	if r != vizExtraImageRed || g != vizExtraImageZero || b != vizExtraImageZero || a != vizExtraImageAlpha {
		return fmt.Errorf("image outline (%v, %v, %v, %v) wants (1, 0, 0, 1)", r, g, b, a)
	}
	filled, err := dec.ReadUint8()
	if err != nil {
		return fmt.Errorf("decode image filled: %w", err)
	}
	if filled != vizExtraImageFilled {
		return fmt.Errorf("image filled %d wants 0", filled)
	}
	fr, err := dec.ReadFloat32()
	if err != nil {
		return fmt.Errorf("decode image fill r: %w", err)
	}
	fg, err := dec.ReadFloat32()
	if err != nil {
		return fmt.Errorf("decode image fill g: %w", err)
	}
	fb, err := dec.ReadFloat32()
	if err != nil {
		return fmt.Errorf("decode image fill b: %w", err)
	}
	fa, err := dec.ReadFloat32()
	if err != nil {
		return fmt.Errorf("decode image fill a: %w", err)
	}
	if fr != vizExtraImageZero || fg != vizExtraImageZero || fb != vizExtraImageZero || fa != vizExtraImageZero {
		return fmt.Errorf("image fill (%v, %v, %v, %v) wants zeros", fr, fg, fb, fa)
	}
	lsec, err := dec.ReadInt32()
	if err != nil {
		return fmt.Errorf("decode image lifetime sec: %w", err)
	}
	lnsec, err := dec.ReadUint32()
	if err != nil {
		return fmt.Errorf("decode image lifetime nanosec: %w", err)
	}
	if lsec != 0 || lnsec != 0 {
		return fmt.Errorf("image lifetime (%d, %d) wants (0, 0)", lsec, lnsec)
	}
	return nil
}

// decodeVizExtraImageMarker decodes small ImageMarker with empty sequences.
func decodeVizExtraImageMarker(payload []byte, live string) error {
	dec, err := types.NewDecoder(payload)
	if err != nil {
		return fmt.Errorf("decode image header: %w", err)
	}
	err = decodeVizExtraImagePose(dec, live)
	if err != nil {
		return err
	}
	err = decodeVizExtraImageColors(dec)
	if err != nil {
		return err
	}
	npts, err := dec.ReadSequenceLength()
	if err != nil {
		return fmt.Errorf("decode image points len: %w", err)
	}
	if npts != 0 {
		return fmt.Errorf("image points len %d wants 0", npts)
	}
	ncol, err := dec.ReadSequenceLength()
	if err != nil {
		return fmt.Errorf("decode image outline colors len: %w", err)
	}
	if ncol != 0 {
		return fmt.Errorf("image outline colors len %d wants 0", ncol)
	}
	err = dec.Finish()
	if err != nil {
		return fmt.Errorf("finish image: %w", err)
	}
	return nil
}

// wantVizExtraImageMarker returns the live ID expected in echo output.
func wantVizExtraImageMarker(live string) string {
	return live
}

// encodeVizExtraMenuEntryInto writes one MenuEntry with given title.
func encodeVizExtraMenuEntryInto(enc *types.Encoder, title string) error {
	enc.WriteUint32(vizExtraMenuID)
	enc.WriteUint32(vizExtraMenuParent)
	err := enc.WriteString(title)
	if err != nil {
		return fmt.Errorf("encode nested menu title: %w", err)
	}
	err = enc.WriteString("")
	if err != nil {
		return fmt.Errorf("encode nested menu command: %w", err)
	}
	enc.WriteUint8(vizExtraMenuCommandType)
	return nil
}

// decodeVizExtraMenuEntryFrom checks one MenuEntry with given title.
func decodeVizExtraMenuEntryFrom(dec *types.Decoder, title string) error {
	id, err := dec.ReadUint32()
	if err != nil {
		return fmt.Errorf("decode nested menu id: %w", err)
	}
	if id != vizExtraMenuID {
		return fmt.Errorf("nested menu id %d wants 7", id)
	}
	parent, err := dec.ReadUint32()
	if err != nil {
		return fmt.Errorf("decode nested menu parent: %w", err)
	}
	if parent != vizExtraMenuParent {
		return fmt.Errorf("nested menu parent %d wants 0", parent)
	}
	got, err := dec.ReadString()
	if err != nil {
		return fmt.Errorf("decode nested menu title: %w", err)
	}
	if got != title {
		return fmt.Errorf("nested menu title %q wants %q", got, title)
	}
	command, err := dec.ReadString()
	if err != nil {
		return fmt.Errorf("decode nested menu command: %w", err)
	}
	if command != "" {
		return fmt.Errorf("nested menu command %q wants empty", command)
	}
	ctype, err := dec.ReadUint8()
	if err != nil {
		return fmt.Errorf("decode nested menu type: %w", err)
	}
	if ctype != vizExtraMenuCommandType {
		return fmt.Errorf("nested menu type %d wants 0", ctype)
	}
	return nil
}

// encodeVizExtraControlInto writes one control with optional Marker.
//
// Name carries the live ID when used at top level. WithMarker selects
// whether markers holds one small Marker or stays empty.
func encodeVizExtraControlInto(enc *types.Encoder, name string, withMarker bool, markerLive string) error {
	err := enc.WriteString(name)
	if err != nil {
		return fmt.Errorf("encode control name: %w", err)
	}
	enc.WriteFloat64(0.0)
	enc.WriteFloat64(0.0)
	enc.WriteFloat64(0.0)
	enc.WriteFloat64(1.0)
	enc.WriteUint8(vizExtraControlOriMode)
	enc.WriteUint8(vizExtraControlInterMode)
	enc.WriteBool(false)
	if withMarker {
		err = enc.WriteSequenceLength(1)
		if err != nil {
			return fmt.Errorf("encode control markers len: %w", err)
		}
		err = encodeVizExtraMarkerInto(enc, markerLive)
		if err != nil {
			return err
		}
	} else {
		err = enc.WriteSequenceLength(0)
		if err != nil {
			return fmt.Errorf("encode control markers len: %w", err)
		}
	}
	enc.WriteBool(false)
	err = enc.WriteString("")
	if err != nil {
		return fmt.Errorf("encode control description: %w", err)
	}
	return nil
}

// decodeVizExtraControlFrom checks one control with optional Marker.
func decodeVizExtraControlFrom(dec *types.Decoder, name string, withMarker bool, markerLive string) error {
	got, err := dec.ReadString()
	if err != nil {
		return fmt.Errorf("decode control name: %w", err)
	}
	if got != name {
		return fmt.Errorf("control name %q wants %q", got, name)
	}
	ox, err := dec.ReadFloat64()
	if err != nil {
		return fmt.Errorf("decode control ori x: %w", err)
	}
	oy, err := dec.ReadFloat64()
	if err != nil {
		return fmt.Errorf("decode control ori y: %w", err)
	}
	oz, err := dec.ReadFloat64()
	if err != nil {
		return fmt.Errorf("decode control ori z: %w", err)
	}
	ow, err := dec.ReadFloat64()
	if err != nil {
		return fmt.Errorf("decode control ori w: %w", err)
	}
	if ox != 0.0 || oy != 0.0 || oz != 0.0 || ow != 1.0 {
		return fmt.Errorf("control ori (%v, %v, %v, %v) wants (0, 0, 0, 1)", ox, oy, oz, ow)
	}
	omode, err := dec.ReadUint8()
	if err != nil {
		return fmt.Errorf("decode control ori mode: %w", err)
	}
	if omode != vizExtraControlOriMode {
		return fmt.Errorf("control ori mode %d wants 0", omode)
	}
	imode, err := dec.ReadUint8()
	if err != nil {
		return fmt.Errorf("decode control interaction mode: %w", err)
	}
	if imode != vizExtraControlInterMode {
		return fmt.Errorf("control interaction mode %d wants 1", imode)
	}
	vis, err := dec.ReadBool()
	if err != nil {
		return fmt.Errorf("decode control visible: %w", err)
	}
	if vis {
		return errors.New("control visible true wants false") //nolint:wrapcheck // new sentinel error, nothing to wrap
	}
	n, err := dec.ReadSequenceLength()
	if err != nil {
		return fmt.Errorf("decode control markers len: %w", err)
	}
	if withMarker {
		if n != 1 {
			return fmt.Errorf("control markers len %d wants 1", n)
		}
		err = decodeVizExtraMarkerInner(dec, markerLive)
		if err != nil {
			return err
		}
	} else if n != 0 {
		return fmt.Errorf("control markers len %d wants 0", n)
	}
	indep, err := dec.ReadBool()
	if err != nil {
		return fmt.Errorf("decode control independent: %w", err)
	}
	if indep {
		return errors.New("control independent true wants false") //nolint:wrapcheck // new sentinel error, nothing to wrap
	}
	desc, err := dec.ReadString()
	if err != nil {
		return fmt.Errorf("decode control description: %w", err)
	}
	if desc != "" {
		return fmt.Errorf("control description %q wants empty", desc)
	}
	return nil
}

// encodeVizExtraControl encodes small Control with live name and no markers.
func encodeVizExtraControl(live string) ([]byte, error) {
	enc := types.NewEncoder(true)
	err := encodeVizExtraControlInto(enc, live, false, live)
	if err != nil {
		return nil, err
	}
	return enc.Bytes(), nil
}

// decodeVizExtraControl decodes small Control with live name and no markers.
func decodeVizExtraControl(payload []byte, live string) error {
	dec, err := types.NewDecoder(payload)
	if err != nil {
		return fmt.Errorf("decode control header: %w", err)
	}
	err = decodeVizExtraControlFrom(dec, live, false, live)
	if err != nil {
		return err
	}
	err = dec.Finish()
	if err != nil {
		return fmt.Errorf("finish control: %w", err)
	}
	return nil
}

// wantVizExtraControl returns the live ID expected in echo output.
func wantVizExtraControl(live string) string {
	return live
}

// encodeVizExtraInteractiveMarkerInto writes one InteractiveMarker.
//
// Header carries the live ID. WithNested selects whether menu_entries and
// controls each hold one element or stay empty. The nested path uses live
// for menu title, control name, and nested Marker header.
func encodeVizExtraInteractiveMarkerInto(enc *types.Encoder, live string, withNested bool) error {
	err := encodeMatrixHeader(enc, live)
	if err != nil {
		return fmt.Errorf("encode imarker header: %w", err)
	}
	enc.WriteFloat64(1.0)
	enc.WriteFloat64(2.0)
	enc.WriteFloat64(3.0)
	enc.WriteFloat64(0.0)
	enc.WriteFloat64(0.0)
	enc.WriteFloat64(0.0)
	enc.WriteFloat64(1.0)
	err = enc.WriteString(vizExtraMarkerName)
	if err != nil {
		return fmt.Errorf("encode imarker name: %w", err)
	}
	err = enc.WriteString(vizExtraMarkerDesc)
	if err != nil {
		return fmt.Errorf("encode imarker description: %w", err)
	}
	enc.WriteFloat32(vizExtraMarkerScale)
	if withNested {
		err = enc.WriteSequenceLength(1)
		if err != nil {
			return fmt.Errorf("encode imarker menu len: %w", err)
		}
		err = encodeVizExtraMenuEntryInto(enc, live)
		if err != nil {
			return err
		}
		err = enc.WriteSequenceLength(1)
		if err != nil {
			return fmt.Errorf("encode imarker controls len: %w", err)
		}
		err = encodeVizExtraControlInto(enc, live, true, live)
		if err != nil {
			return err
		}
	} else {
		err = enc.WriteSequenceLength(0)
		if err != nil {
			return fmt.Errorf("encode imarker menu len: %w", err)
		}
		err = enc.WriteSequenceLength(0)
		if err != nil {
			return fmt.Errorf("encode imarker controls len: %w", err)
		}
	}
	return nil
}

// decodeVizExtraInteractiveMarkerFrom checks one InteractiveMarker.
func decodeVizExtraInteractiveMarkerFrom(dec *types.Decoder, live string, withNested bool) error {
	frame, err := decodeMatrixHeader(dec)
	if err != nil {
		return fmt.Errorf("decode imarker frame: %w", err)
	}
	if frame != live {
		return fmt.Errorf("imarker frame %q wants %q", frame, live)
	}
	px, py, pz, err := helpers.ReadFloat64Triple(dec, "decode imarker pos x", "decode imarker pos y", "decode imarker pos z")
	if err != nil {
		return fmt.Errorf("read imarker pos: %w", err)
	}
	if px != 1.0 || py != 2.0 || pz != 3.0 {
		return fmt.Errorf("imarker pos (%v, %v, %v) wants (1.0, 2.0, 3.0)", px, py, pz)
	}
	ox, oy, oz, ow, err := helpers.ReadFloat64Quad(dec, "decode imarker ori x", "decode imarker ori y", "decode imarker ori z", "decode imarker ori w")
	if err != nil {
		return fmt.Errorf("read imarker ori: %w", err)
	}
	if ox != 0.0 || oy != 0.0 || oz != 0.0 || ow != 1.0 {
		return fmt.Errorf("imarker ori (%v, %v, %v, %v) wants (0, 0, 0, 1)", ox, oy, oz, ow)
	}
	name, err := dec.ReadString()
	if err != nil {
		return fmt.Errorf("decode imarker name: %w", err)
	}
	if name != vizExtraMarkerName {
		return fmt.Errorf("imarker name %q wants %q", name, vizExtraMarkerName)
	}
	desc, err := dec.ReadString()
	if err != nil {
		return fmt.Errorf("decode imarker description: %w", err)
	}
	if desc != vizExtraMarkerDesc {
		return fmt.Errorf("imarker description %q wants empty", desc)
	}
	scale, err := dec.ReadFloat32()
	if err != nil {
		return fmt.Errorf("decode imarker scale: %w", err)
	}
	if scale != vizExtraMarkerScale {
		return fmt.Errorf("imarker scale %v wants 1.0", scale)
	}
	nmenu, err := dec.ReadSequenceLength()
	if err != nil {
		return fmt.Errorf("decode imarker menu len: %w", err)
	}
	nctrl, err := dec.ReadSequenceLength()
	if err != nil {
		// Read controls len after menu entries. When nested, menu holds
		// one entry that must be consumed before controls len is valid,
		// so this path only runs for the empty live case. Nested decode
		// uses the dedicated nested helper below.
		return fmt.Errorf("decode imarker controls len: %w", err)
	}
	if withNested {
		// This branch is unused because nested decode needs the menu
		// entry bytes between the two lengths. Kept for symmetry and
		// guarded so a future caller cannot silently use it.
		return errors.New("nested imarker needs entry aware decode") //nolint:wrapcheck // new sentinel error, nothing to wrap
	}
	if nmenu != 0 {
		return fmt.Errorf("imarker menu len %d wants 0", nmenu)
	}
	if nctrl != 0 {
		return fmt.Errorf("imarker controls len %d wants 0", nctrl)
	}
	return nil
}

// decodeVizExtraInteractiveMarkerNested checks one InteractiveMarker with nesting.
//
// It reads menu length, then the single MenuEntry, then controls length,
// then the single Control with Marker. Split from the empty case because
// sequence elements sit between the two length prefixes.
func decodeVizExtraInteractiveMarkerNested(dec *types.Decoder, live string) error {
	frame, err := decodeMatrixHeader(dec)
	if err != nil {
		return fmt.Errorf("decode nested imarker frame: %w", err)
	}
	if frame != live {
		return fmt.Errorf("nested imarker frame %q wants %q", frame, live)
	}
	px, py, pz, err := helpers.ReadFloat64Triple(dec, "decode nested imarker pos x", "decode nested imarker pos y", "decode nested imarker pos z")
	if err != nil {
		return fmt.Errorf("read nested imarker pos: %w", err)
	}
	if px != 1.0 || py != 2.0 || pz != 3.0 {
		return fmt.Errorf("nested imarker pos (%v, %v, %v) wants (1.0, 2.0, 3.0)", px, py, pz)
	}
	ox, oy, oz, ow, err := helpers.ReadFloat64Quad(dec, "decode nested imarker ori x", "decode nested imarker ori y", "decode nested imarker ori z", "decode nested imarker ori w")
	if err != nil {
		return fmt.Errorf("read nested imarker ori: %w", err)
	}
	if ox != 0.0 || oy != 0.0 || oz != 0.0 || ow != 1.0 {
		return fmt.Errorf("nested imarker ori (%v, %v, %v, %v) wants (0, 0, 0, 1)", ox, oy, oz, ow)
	}
	name, err := dec.ReadString()
	if err != nil {
		return fmt.Errorf("decode nested imarker name: %w", err)
	}
	if name != vizExtraMarkerName {
		return fmt.Errorf("nested imarker name %q wants %q", name, vizExtraMarkerName)
	}
	desc, err := dec.ReadString()
	if err != nil {
		return fmt.Errorf("decode nested imarker description: %w", err)
	}
	if desc != vizExtraMarkerDesc {
		return fmt.Errorf("nested imarker description %q wants empty", desc)
	}
	scale, err := dec.ReadFloat32()
	if err != nil {
		return fmt.Errorf("decode nested imarker scale: %w", err)
	}
	if scale != vizExtraMarkerScale {
		return fmt.Errorf("nested imarker scale %v wants 1.0", scale)
	}
	nmenu, err := dec.ReadSequenceLength()
	if err != nil {
		return fmt.Errorf("decode nested imarker menu len: %w", err)
	}
	if nmenu != 1 {
		return fmt.Errorf("nested imarker menu len %d wants 1", nmenu)
	}
	err = decodeVizExtraMenuEntryFrom(dec, live)
	if err != nil {
		return err
	}
	nctrl, err := dec.ReadSequenceLength()
	if err != nil {
		return fmt.Errorf("decode nested imarker controls len: %w", err)
	}
	if nctrl != 1 {
		return fmt.Errorf("nested imarker controls len %d wants 1", nctrl)
	}
	err = decodeVizExtraControlFrom(dec, live, true, live)
	if err != nil {
		return err
	}
	return nil
}

// encodeVizExtraInteractiveMarker encodes small InteractiveMarker.
func encodeVizExtraInteractiveMarker(live string) ([]byte, error) {
	enc := types.NewEncoder(true)
	err := encodeVizExtraInteractiveMarkerInto(enc, live, false)
	if err != nil {
		return nil, err
	}
	return enc.Bytes(), nil
}

// decodeVizExtraInteractiveMarker decodes small InteractiveMarker.
func decodeVizExtraInteractiveMarker(payload []byte, live string) error {
	dec, err := types.NewDecoder(payload)
	if err != nil {
		return fmt.Errorf("decode imarker header: %w", err)
	}
	err = decodeVizExtraInteractiveMarkerFrom(dec, live, false)
	if err != nil {
		return err
	}
	err = dec.Finish()
	if err != nil {
		return fmt.Errorf("finish imarker: %w", err)
	}
	return nil
}

// wantVizExtraInteractiveMarker returns the live ID expected in echo output.
func wantVizExtraInteractiveMarker(live string) string {
	return live
}

// encodeVizExtraPose encodes InteractiveMarkerPose.
func encodeVizExtraPose(live string) ([]byte, error) {
	enc := types.NewEncoder(true)
	err := encodeMatrixHeader(enc, live)
	if err != nil {
		return nil, fmt.Errorf("encode ipose header: %w", err)
	}
	enc.WriteFloat64(1.0)
	enc.WriteFloat64(2.0)
	enc.WriteFloat64(3.0)
	enc.WriteFloat64(0.0)
	enc.WriteFloat64(0.0)
	enc.WriteFloat64(0.0)
	enc.WriteFloat64(1.0)
	err = enc.WriteString(vizExtraPoseName)
	if err != nil {
		return nil, fmt.Errorf("encode ipose name: %w", err)
	}
	return enc.Bytes(), nil
}

// decodeVizExtraPoseCommon checks pose plus name for InteractiveMarkerPose.
func decodeVizExtraPoseCommon(dec *types.Decoder) error {
	px, py, pz, err := helpers.ReadFloat64Triple(dec, "decode ipose pos x", "decode ipose pos y", "decode ipose pos z")
	if err != nil {
		return fmt.Errorf("read ipose pos: %w", err)
	}
	if px != 1.0 || py != 2.0 || pz != 3.0 {
		return fmt.Errorf("ipose pos (%v, %v, %v) wants (1.0, 2.0, 3.0)", px, py, pz)
	}
	ox, oy, oz, ow, err := helpers.ReadFloat64Quad(dec, "decode ipose ori x", "decode ipose ori y", "decode ipose ori z", "decode ipose ori w")
	if err != nil {
		return fmt.Errorf("read ipose ori: %w", err)
	}
	if ox != 0.0 || oy != 0.0 || oz != 0.0 || ow != 1.0 {
		return fmt.Errorf("ipose ori (%v, %v, %v, %v) wants (0, 0, 0, 1)", ox, oy, oz, ow)
	}
	name, err := dec.ReadString()
	if err != nil {
		return fmt.Errorf("decode ipose name: %w", err)
	}
	if name != vizExtraPoseName {
		return fmt.Errorf("ipose name %q wants %q", name, vizExtraPoseName)
	}
	return nil
}

// decodeVizExtraPose decodes InteractiveMarkerPose.
func decodeVizExtraPose(payload []byte, live string) error {
	dec, err := types.NewDecoder(payload)
	if err != nil {
		return fmt.Errorf("decode ipose header: %w", err)
	}
	frame, err := decodeMatrixHeader(dec)
	if err != nil {
		return fmt.Errorf("decode ipose frame: %w", err)
	}
	if frame != live {
		return fmt.Errorf("ipose frame %q wants %q", frame, live)
	}
	err = decodeVizExtraPoseCommon(dec)
	if err != nil {
		return err
	}
	err = dec.Finish()
	if err != nil {
		return fmt.Errorf("finish ipose: %w", err)
	}
	return nil
}

// decodeVizExtraPoseFrom checks one Pose element inside Update.
func decodeVizExtraPoseFrom(dec *types.Decoder, live string) error {
	frame, err := decodeMatrixHeader(dec)
	if err != nil {
		return fmt.Errorf("decode nested pose frame: %w", err)
	}
	if frame != live {
		return fmt.Errorf("nested pose frame %q wants %q", frame, live)
	}
	err = decodeVizExtraPoseCommon(dec)
	if err != nil {
		return err
	}
	return nil
}

// wantVizExtraPose returns the live ID expected in echo output.
func wantVizExtraPose(live string) string {
	return live
}

// encodeVizExtraFeedbackPose writes pose plus menu id plus mouse point.
func encodeVizExtraFeedbackPose(enc *types.Encoder) {
	enc.WriteFloat64(1.0)
	enc.WriteFloat64(2.0)
	enc.WriteFloat64(3.0)
	enc.WriteFloat64(0.0)
	enc.WriteFloat64(0.0)
	enc.WriteFloat64(0.0)
	enc.WriteFloat64(1.0)
	enc.WriteUint32(vizExtraFeedbackMenuID)
	enc.WriteFloat64(0.0)
	enc.WriteFloat64(0.0)
	enc.WriteFloat64(0.0)
	enc.WriteBool(false)
}

// encodeVizExtraFeedback encodes InteractiveMarkerFeedback.
func encodeVizExtraFeedback(live string) ([]byte, error) {
	enc := types.NewEncoder(true)
	err := encodeMatrixHeader(enc, live)
	if err != nil {
		return nil, fmt.Errorf("encode feedback header: %w", err)
	}
	err = enc.WriteString(vizExtraFeedbackClient)
	if err != nil {
		return nil, fmt.Errorf("encode feedback client: %w", err)
	}
	err = enc.WriteString(vizExtraFeedbackMarker)
	if err != nil {
		return nil, fmt.Errorf("encode feedback marker: %w", err)
	}
	err = enc.WriteString(vizExtraFeedbackControl)
	if err != nil {
		return nil, fmt.Errorf("encode feedback control: %w", err)
	}
	enc.WriteUint8(vizExtraFeedbackEvent)
	encodeVizExtraFeedbackPose(enc)
	return enc.Bytes(), nil
}

// decodeVizExtraFeedbackNames checks client plus marker plus control.
func decodeVizExtraFeedbackNames(dec *types.Decoder) error {
	client, err := dec.ReadString()
	if err != nil {
		return fmt.Errorf("decode feedback client: %w", err)
	}
	if client != vizExtraFeedbackClient {
		return fmt.Errorf("feedback client %q wants %q", client, vizExtraFeedbackClient)
	}
	marker, err := dec.ReadString()
	if err != nil {
		return fmt.Errorf("decode feedback marker: %w", err)
	}
	if marker != vizExtraFeedbackMarker {
		return fmt.Errorf("feedback marker %q wants %q", marker, vizExtraFeedbackMarker)
	}
	control, err := dec.ReadString()
	if err != nil {
		return fmt.Errorf("decode feedback control: %w", err)
	}
	if control != vizExtraFeedbackControl {
		return fmt.Errorf("feedback control %q wants %q", control, vizExtraFeedbackControl)
	}
	event, err := dec.ReadUint8()
	if err != nil {
		return fmt.Errorf("decode feedback event: %w", err)
	}
	if event != vizExtraFeedbackEvent {
		return fmt.Errorf("feedback event %d wants 1", event)
	}
	return nil
}

// decodeVizExtraFeedbackPose checks pose plus menu id plus mouse point.
func decodeVizExtraFeedbackPose(dec *types.Decoder) error {
	px, py, pz, err := helpers.ReadFloat64Triple(dec, "decode feedback pos x", "decode feedback pos y", "decode feedback pos z")
	if err != nil {
		return fmt.Errorf("read feedback pos: %w", err)
	}
	if px != 1.0 || py != 2.0 || pz != 3.0 {
		return fmt.Errorf("feedback pos (%v, %v, %v) wants (1.0, 2.0, 3.0)", px, py, pz)
	}
	ox, oy, oz, ow, err := helpers.ReadFloat64Quad(dec, "decode feedback ori x", "decode feedback ori y", "decode feedback ori z", "decode feedback ori w")
	if err != nil {
		return fmt.Errorf("read feedback ori: %w", err)
	}
	if ox != 0.0 || oy != 0.0 || oz != 0.0 || ow != 1.0 {
		return fmt.Errorf("feedback ori (%v, %v, %v, %v) wants (0, 0, 0, 1)", ox, oy, oz, ow)
	}
	menu, err := dec.ReadUint32()
	if err != nil {
		return fmt.Errorf("decode feedback menu id: %w", err)
	}
	if menu != vizExtraFeedbackMenuID {
		return fmt.Errorf("feedback menu id %d wants 0", menu)
	}
	mx, my, mz, err := helpers.ReadFloat64Triple(dec, "decode feedback mouse x", "decode feedback mouse y", "decode feedback mouse z")
	if err != nil {
		return fmt.Errorf("read feedback mouse: %w", err)
	}
	if mx != 0.0 || my != 0.0 || mz != 0.0 {
		return fmt.Errorf("feedback mouse (%v, %v, %v) wants zeros", mx, my, mz)
	}
	valid, err := dec.ReadBool()
	if err != nil {
		return fmt.Errorf("decode feedback mouse valid: %w", err)
	}
	if valid {
		return errors.New("feedback mouse valid true wants false") //nolint:wrapcheck // new sentinel error, nothing to wrap
	}
	return nil
}

// decodeVizExtraFeedback decodes InteractiveMarkerFeedback.
func decodeVizExtraFeedback(payload []byte, live string) error {
	dec, err := types.NewDecoder(payload)
	if err != nil {
		return fmt.Errorf("decode feedback header: %w", err)
	}
	frame, err := decodeMatrixHeader(dec)
	if err != nil {
		return fmt.Errorf("decode feedback frame: %w", err)
	}
	if frame != live {
		return fmt.Errorf("feedback frame %q wants %q", frame, live)
	}
	err = decodeVizExtraFeedbackNames(dec)
	if err != nil {
		return err
	}
	err = decodeVizExtraFeedbackPose(dec)
	if err != nil {
		return err
	}
	for dec.Remaining() > 0 {
		pad, err := dec.ReadUint8()
		if err != nil {
			return fmt.Errorf("decode feedback pad: %w", err)
		}
		if pad != 0 {
			return fmt.Errorf("feedback pad 0x%02x wants zero", pad)
		}
	}
	return nil
}

// wantVizExtraFeedback returns the live ID expected in echo output.
func wantVizExtraFeedback(live string) string {
	return live
}

// encodeVizExtraInit encodes InteractiveMarkerInit with no markers.
func encodeVizExtraInit(live string) ([]byte, error) {
	enc := types.NewEncoder(true)
	err := enc.WriteString(live)
	if err != nil {
		return nil, fmt.Errorf("encode init server: %w", err)
	}
	enc.WriteUint64(vizExtraInitSeq)
	err = enc.WriteSequenceLength(0)
	if err != nil {
		return nil, fmt.Errorf("encode init markers len: %w", err)
	}
	return enc.Bytes(), nil
}

// decodeVizExtraInit decodes InteractiveMarkerInit with no markers.
func decodeVizExtraInit(payload []byte, live string) error {
	dec, err := types.NewDecoder(payload)
	if err != nil {
		return fmt.Errorf("decode init header: %w", err)
	}
	server, err := dec.ReadString()
	if err != nil {
		return fmt.Errorf("decode init server: %w", err)
	}
	if server != live {
		return fmt.Errorf("init server %q wants %q", server, live)
	}
	seq, err := dec.ReadUint64()
	if err != nil {
		return fmt.Errorf("decode init seq: %w", err)
	}
	if seq != vizExtraInitSeq {
		return fmt.Errorf("init seq %d wants 0", seq)
	}
	n, err := dec.ReadSequenceLength()
	if err != nil {
		return fmt.Errorf("decode init markers len: %w", err)
	}
	if n != 0 {
		return fmt.Errorf("init markers len %d wants 0", n)
	}
	err = dec.Finish()
	if err != nil {
		return fmt.Errorf("finish init: %w", err)
	}
	return nil
}

// wantVizExtraInit returns the live ID expected in echo output.
func wantVizExtraInit(live string) string {
	return live
}

// encodeVizExtraUpdate encodes InteractiveMarkerUpdate with empty payload.
func encodeVizExtraUpdate(live string) ([]byte, error) {
	enc := types.NewEncoder(true)
	err := enc.WriteString(live)
	if err != nil {
		return nil, fmt.Errorf("encode update server: %w", err)
	}
	enc.WriteUint64(vizExtraUpdateSeq)
	enc.WriteUint8(vizExtraUpdateType)
	err = enc.WriteSequenceLength(0)
	if err != nil {
		return nil, fmt.Errorf("encode update markers len: %w", err)
	}
	err = enc.WriteSequenceLength(0)
	if err != nil {
		return nil, fmt.Errorf("encode update poses len: %w", err)
	}
	err = enc.WriteSequenceLength(0)
	if err != nil {
		return nil, fmt.Errorf("encode update erases len: %w", err)
	}
	return enc.Bytes(), nil
}

// decodeVizExtraUpdate decodes InteractiveMarkerUpdate with empty payload.
func decodeVizExtraUpdate(payload []byte, live string) error {
	dec, err := types.NewDecoder(payload)
	if err != nil {
		return fmt.Errorf("decode update header: %w", err)
	}
	server, err := dec.ReadString()
	if err != nil {
		return fmt.Errorf("decode update server: %w", err)
	}
	if server != live {
		return fmt.Errorf("update server %q wants %q", server, live)
	}
	seq, err := dec.ReadUint64()
	if err != nil {
		return fmt.Errorf("decode update seq: %w", err)
	}
	if seq != vizExtraUpdateSeq {
		return fmt.Errorf("update seq %d wants 0", seq)
	}
	utype, err := dec.ReadUint8()
	if err != nil {
		return fmt.Errorf("decode update type: %w", err)
	}
	if utype != vizExtraUpdateType {
		return fmt.Errorf("update type %d wants 1", utype)
	}
	nm, err := dec.ReadSequenceLength()
	if err != nil {
		return fmt.Errorf("decode update markers len: %w", err)
	}
	if nm != 0 {
		return fmt.Errorf("update markers len %d wants 0", nm)
	}
	np, err := dec.ReadSequenceLength()
	if err != nil {
		return fmt.Errorf("decode update poses len: %w", err)
	}
	if np != 0 {
		return fmt.Errorf("update poses len %d wants 0", np)
	}
	ne, err := dec.ReadSequenceLength()
	if err != nil {
		return fmt.Errorf("decode update erases len: %w", err)
	}
	if ne != 0 {
		return fmt.Errorf("update erases len %d wants 0", ne)
	}
	err = dec.Finish()
	if err != nil {
		return fmt.Errorf("finish update: %w", err)
	}
	return nil
}

// wantVizExtraUpdate returns the live ID expected in echo output.
func wantVizExtraUpdate(live string) string {
	return live
}

// vizExtraCases returns the viz extra live matrix in fixed order.
func vizExtraCases() []typeMatrixCase {
	return []typeMatrixCase{
		{
			Label:      "viz-markerarray",
			RosType:    "visualization_msgs/msg/MarkerArray",
			IDLName:    "visualization_msgs__msg__MarkerArray",
			staticHash: vizExtraHashMarkerArray,
			yaml:       yamlVizExtraMarkerArray,
			encode:     encodeVizExtraMarkerArray,
			decode:     decodeVizExtraMarkerArray,
			want:       wantVizExtraMarkerArray,
		},
		{
			Label:      "viz-menuentry",
			RosType:    "visualization_msgs/msg/MenuEntry",
			IDLName:    "visualization_msgs__msg__MenuEntry",
			staticHash: vizExtraHashMenuEntry,
			yaml:       yamlVizExtraMenuEntry,
			encode:     encodeVizExtraMenuEntry,
			decode:     decodeVizExtraMenuEntry,
			want:       wantVizExtraMenuEntry,
		},
		{
			Label:      "viz-imagemarker",
			RosType:    "visualization_msgs/msg/ImageMarker",
			IDLName:    "visualization_msgs__msg__ImageMarker",
			staticHash: vizExtraHashImageMarker,
			yaml:       yamlVizExtraImageMarker,
			encode:     encodeVizExtraImageMarker,
			decode:     decodeVizExtraImageMarker,
			want:       wantVizExtraImageMarker,
		},
		{
			Label:      "viz-imcontrol",
			RosType:    "visualization_msgs/msg/InteractiveMarkerControl",
			IDLName:    "visualization_msgs__msg__InteractiveMarkerControl",
			staticHash: vizExtraHashControl,
			yaml:       yamlVizExtraControl,
			encode:     encodeVizExtraControl,
			decode:     decodeVizExtraControl,
			want:       wantVizExtraControl,
		},
		{
			Label:      "viz-imarker",
			RosType:    "visualization_msgs/msg/InteractiveMarker",
			IDLName:    "visualization_msgs__msg__InteractiveMarker",
			staticHash: vizExtraHashInteractiveMarker,
			yaml:       yamlVizExtraInteractiveMarker,
			encode:     encodeVizExtraInteractiveMarker,
			decode:     decodeVizExtraInteractiveMarker,
			want:       wantVizExtraInteractiveMarker,
		},
		{
			Label:      "viz-ipose",
			RosType:    "visualization_msgs/msg/InteractiveMarkerPose",
			IDLName:    "visualization_msgs__msg__InteractiveMarkerPose",
			staticHash: vizExtraHashInteractiveMarkerPose,
			yaml:       yamlVizExtraPose,
			encode:     encodeVizExtraPose,
			decode:     decodeVizExtraPose,
			want:       wantVizExtraPose,
		},
		{
			Label:      "viz-ifeedback",
			RosType:    "visualization_msgs/msg/InteractiveMarkerFeedback",
			IDLName:    "visualization_msgs__msg__InteractiveMarkerFeedback",
			staticHash: vizExtraHashFeedback,
			yaml:       yamlVizExtraFeedback,
			encode:     encodeVizExtraFeedback,
			decode:     decodeVizExtraFeedback,
			want:       wantVizExtraFeedback,
		},
		{
			Label:      "viz-iinit",
			RosType:    "visualization_msgs/msg/InteractiveMarkerInit",
			IDLName:    "visualization_msgs__msg__InteractiveMarkerInit",
			staticHash: vizExtraHashInit,
			yaml:       yamlVizExtraInit,
			encode:     encodeVizExtraInit,
			decode:     decodeVizExtraInit,
			want:       wantVizExtraInit,
		},
		{
			Label:      "viz-iupdate",
			RosType:    "visualization_msgs/msg/InteractiveMarkerUpdate",
			IDLName:    "visualization_msgs__msg__InteractiveMarkerUpdate",
			staticHash: vizExtraHashUpdate,
			yaml:       yamlVizExtraUpdate,
			encode:     encodeVizExtraUpdate,
			decode:     decodeVizExtraUpdate,
			want:       wantVizExtraUpdate,
		},
	}
}

// runVizExtraLabel runs forward plus reverse for one viz extra label.
//
// Domain is the allocated exclusive domain shared by both directions.
// It looks up the case, skips when the peer lacks the type, then runs
// forward with one live ID and reverse with another so the two directions
// never share a topic. Forward and reverse reuse the shared matrix runners
// so domain 42 plus docker argv plus hash probing stay identical.
func runVizExtraLabel(t *testing.T, ctx context.Context, domain int, label string) {
	t.Helper()
	runSharedMatrixLabel(t, ctx, domain, label, vizExtraCases())
}

// TestROSTypeVizMarkerArray checks MarkerArray forward plus reverse.
//
// MarkerArray holds one small Marker so the nested header frame_id carries
// the per-run live ID. The nested Marker reuses the standalone Marker wire
// form with empty sequences elsewhere.
func TestROSTypeVizMarkerArray(t *testing.T) {
	t.Parallel()
	domain := helpers.MustAllocateDomain(t)
	ctx, cancel := context.WithTimeout(context.Background(), typeMatrixTestTimeout)
	defer cancel()
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)
	runVizExtraLabel(t, ctx, domain, "viz-markerarray")
}

// TestROSTypeVizMenuEntry checks MenuEntry forward plus reverse.
//
// MenuEntry carries no header, so title carries the per-run live ID and
// per-run topic plus domain isolation keeps runs apart.
func TestROSTypeVizMenuEntry(t *testing.T) {
	t.Parallel()
	domain := helpers.MustAllocateDomain(t)
	ctx, cancel := context.WithTimeout(context.Background(), typeMatrixTestTimeout)
	defer cancel()
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)
	runVizExtraLabel(t, ctx, domain, "viz-menuentry")
}

// TestROSTypeVizImageMarker checks ImageMarker forward plus reverse.
//
// Header frame_id carries the per-run live ID. Points and outline colors
// stay empty to keep the live sample small. The nested point plus color
// path is covered by the local codec test below.
func TestROSTypeVizImageMarker(t *testing.T) {
	t.Parallel()
	domain := helpers.MustAllocateDomain(t)
	ctx, cancel := context.WithTimeout(context.Background(), typeMatrixTestTimeout)
	defer cancel()
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)
	runVizExtraLabel(t, ctx, domain, "viz-imagemarker")
}

// TestROSTypeVizInteractiveMarkerControl checks Control forward plus reverse.
//
// Control carries no header, so name carries the per-run live ID. Markers
// stay empty to keep the live sample small. The Marker nested path is
// covered by the local codec test below.
func TestROSTypeVizInteractiveMarkerControl(t *testing.T) {
	t.Parallel()
	domain := helpers.MustAllocateDomain(t)
	ctx, cancel := context.WithTimeout(context.Background(), typeMatrixTestTimeout)
	defer cancel()
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)
	runVizExtraLabel(t, ctx, domain, "viz-imcontrol")
}

// TestROSTypeVizInteractiveMarker checks InteractiveMarker forward plus reverse.
//
// Header frame_id carries the per-run live ID. Menu entries and controls
// stay empty to keep the live sample small. The nested menu plus control
// plus Marker path is covered by the local codec test below.
func TestROSTypeVizInteractiveMarker(t *testing.T) {
	t.Parallel()
	domain := helpers.MustAllocateDomain(t)
	ctx, cancel := context.WithTimeout(context.Background(), typeMatrixTestTimeout)
	defer cancel()
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)
	runVizExtraLabel(t, ctx, domain, "viz-imarker")
}

// TestROSTypeVizInteractiveMarkerPose checks Pose forward plus reverse.
//
// Header frame_id carries the per-run live ID so stale runs never match.
func TestROSTypeVizInteractiveMarkerPose(t *testing.T) {
	t.Parallel()
	domain := helpers.MustAllocateDomain(t)
	ctx, cancel := context.WithTimeout(context.Background(), typeMatrixTestTimeout)
	defer cancel()
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)
	runVizExtraLabel(t, ctx, domain, "viz-ipose")
}

// TestROSTypeVizInteractiveMarkerFeedback checks Feedback forward plus reverse.
//
// Header frame_id carries the per-run live ID. Client plus marker plus
// control names stay fixed, event stays pose update, and mouse stays zero
// with valid false.
func TestROSTypeVizInteractiveMarkerFeedback(t *testing.T) {
	t.Parallel()
	domain := helpers.MustAllocateDomain(t)
	ctx, cancel := context.WithTimeout(context.Background(), typeMatrixTestTimeout)
	defer cancel()
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)
	runVizExtraLabel(t, ctx, domain, "viz-ifeedback")
}

// TestROSTypeVizInteractiveMarkerInit checks Init forward plus reverse.
//
// Server ID carries the per-run live ID. Markers stay empty to keep the
// live sample small. The InteractiveMarker nested path is covered by the
// local codec test below.
func TestROSTypeVizInteractiveMarkerInit(t *testing.T) {
	t.Parallel()
	domain := helpers.MustAllocateDomain(t)
	ctx, cancel := context.WithTimeout(context.Background(), typeMatrixTestTimeout)
	defer cancel()
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)
	runVizExtraLabel(t, ctx, domain, "viz-iinit")
}

// TestROSTypeVizInteractiveMarkerUpdate checks Update forward plus reverse.
//
// Server ID carries the per-run live ID. Markers plus poses plus erases
// stay empty to keep the live sample small. Nested paths are covered by
// the local codec test below.
func TestROSTypeVizInteractiveMarkerUpdate(t *testing.T) {
	t.Parallel()
	domain := helpers.MustAllocateDomain(t)
	ctx, cancel := context.WithTimeout(context.Background(), typeMatrixTestTimeout)
	defer cancel()
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)
	runVizExtraLabel(t, ctx, domain, "viz-iupdate")
}

// encodeVizExtraImageFull encodes ImageMarker with one point plus one color.
//
// It is pure-Go local coverage for the Point plus ColorRGBA sequence path
// that live keeps empty to stay small. No ROS peer is needed.
func encodeVizExtraImageFull(live string) ([]byte, error) {
	enc := types.NewEncoder(true)
	err := encodeVizExtraImagePose(enc, live)
	if err != nil {
		return nil, err
	}
	encodeVizExtraImageColors(enc)
	err = enc.WriteSequenceLength(1)
	if err != nil {
		return nil, fmt.Errorf("encode full image points len: %w", err)
	}
	enc.WriteFloat64(vizExtraImagePosX)
	enc.WriteFloat64(vizExtraImagePosY)
	enc.WriteFloat64(vizExtraImagePosZ)
	err = enc.WriteSequenceLength(1)
	if err != nil {
		return nil, fmt.Errorf("encode full image colors len: %w", err)
	}
	enc.WriteFloat32(vizExtraImageRed)
	enc.WriteFloat32(vizExtraImageZero)
	enc.WriteFloat32(vizExtraImageZero)
	enc.WriteFloat32(vizExtraImageAlpha)
	return enc.Bytes(), nil
}

// decodeVizExtraImageFull decodes ImageMarker with one point plus one color.
func decodeVizExtraImageFull(payload []byte, live string) error {
	dec, err := types.NewDecoder(payload)
	if err != nil {
		return fmt.Errorf("decode full image header: %w", err)
	}
	err = decodeVizExtraImagePose(dec, live)
	if err != nil {
		return err
	}
	err = decodeVizExtraImageColors(dec)
	if err != nil {
		return err
	}
	npts, err := dec.ReadSequenceLength()
	if err != nil {
		return fmt.Errorf("decode full image points len: %w", err)
	}
	if npts != 1 {
		return fmt.Errorf("full image points len %d wants 1", npts)
	}
	px, py, pz, err := helpers.ReadFloat64Triple(dec, "decode full image pt x", "decode full image pt y", "decode full image pt z")
	if err != nil {
		return fmt.Errorf("read full image point: %w", err)
	}
	if px != vizExtraImagePosX || py != vizExtraImagePosY || pz != vizExtraImagePosZ {
		return fmt.Errorf("full image point (%v, %v, %v) wants (1.0, 2.0, 0.0)", px, py, pz)
	}
	ncol, err := dec.ReadSequenceLength()
	if err != nil {
		return fmt.Errorf("decode full image colors len: %w", err)
	}
	if ncol != 1 {
		return fmt.Errorf("full image colors len %d wants 1", ncol)
	}
	r, err := dec.ReadFloat32()
	if err != nil {
		return fmt.Errorf("decode full image color r: %w", err)
	}
	g, err := dec.ReadFloat32()
	if err != nil {
		return fmt.Errorf("decode full image color g: %w", err)
	}
	b, err := dec.ReadFloat32()
	if err != nil {
		return fmt.Errorf("decode full image color b: %w", err)
	}
	a, err := dec.ReadFloat32()
	if err != nil {
		return fmt.Errorf("decode full image color a: %w", err)
	}
	if r != vizExtraImageRed || g != vizExtraImageZero || b != vizExtraImageZero || a != vizExtraImageAlpha {
		return fmt.Errorf("full image color (%v, %v, %v, %v) wants (1, 0, 0, 1)", r, g, b, a)
	}
	err = dec.Finish()
	if err != nil {
		return fmt.Errorf("finish full image: %w", err)
	}
	return nil
}

// TestROSVizExtraImageMarkerLocal checks ImageMarker point sequence locally.
//
// Live keeps points empty to stay small. This pure-Go test proves the
// Point plus ColorRGBA sequence path round trips without a ROS peer.
func TestROSVizExtraImageMarkerLocal(t *testing.T) {
	t.Parallel()
	live := "local-image-full"
	payload, err := encodeVizExtraImageFull(live)
	if err != nil {
		t.Fatalf("encode full image: %v", err)
	}
	err = decodeVizExtraImageFull(payload, live)
	if err != nil {
		t.Fatalf("decode full image: %v", err)
	}
}

// TestROSVizExtraControlNestedLocal checks Control with one Marker locally.
//
// Live keeps markers empty to stay small. This pure-Go test proves the
// Marker sequence path inside Control round trips without a ROS peer.
func TestROSVizExtraControlNestedLocal(t *testing.T) {
	t.Parallel()
	live := "local-control-full"
	enc := types.NewEncoder(true)
	err := encodeVizExtraControlInto(enc, live, true, live)
	if err != nil {
		t.Fatalf("encode full control: %v", err)
	}
	payload := enc.Bytes()
	dec, err := types.NewDecoder(payload)
	if err != nil {
		t.Fatalf("decode full control header: %v", err)
	}
	err = decodeVizExtraControlFrom(dec, live, true, live)
	if err != nil {
		t.Fatalf("decode full control: %v", err)
	}
	err = dec.Finish()
	if err != nil {
		t.Fatalf("finish full control: %v", err)
	}
}

// TestROSVizExtraInteractiveMarkerNestedLocal checks nested InteractiveMarker locally.
//
// Live keeps menu plus controls empty to stay small. This pure-Go test
// proves MenuEntry plus Control plus Marker nesting round trips without
// a ROS peer.
func TestROSVizExtraInteractiveMarkerNestedLocal(t *testing.T) {
	t.Parallel()
	live := "local-imarker-full"
	enc := types.NewEncoder(true)
	err := encodeVizExtraInteractiveMarkerInto(enc, live, true)
	if err != nil {
		t.Fatalf("encode nested imarker: %v", err)
	}
	payload := enc.Bytes()
	dec, err := types.NewDecoder(payload)
	if err != nil {
		t.Fatalf("decode nested imarker header: %v", err)
	}
	err = decodeVizExtraInteractiveMarkerNested(dec, live)
	if err != nil {
		t.Fatalf("decode nested imarker: %v", err)
	}
	err = dec.Finish()
	if err != nil {
		t.Fatalf("finish nested imarker: %v", err)
	}
}

// TestROSVizExtraInitNestedLocal checks Init with one InteractiveMarker locally.
//
// Live keeps markers empty to stay small. This pure-Go test proves the
// InteractiveMarker sequence path inside Init round trips without a peer.
func TestROSVizExtraInitNestedLocal(t *testing.T) {
	t.Parallel()
	live := "local-init-full"
	enc := types.NewEncoder(true)
	err := enc.WriteString(live)
	if err != nil {
		t.Fatalf("encode nested init server: %v", err)
	}
	enc.WriteUint64(vizExtraInitSeq)
	err = enc.WriteSequenceLength(1)
	if err != nil {
		t.Fatalf("encode nested init len: %v", err)
	}
	err = encodeVizExtraInteractiveMarkerInto(enc, live, false)
	if err != nil {
		t.Fatalf("encode nested init marker: %v", err)
	}
	payload := enc.Bytes()
	dec, err := types.NewDecoder(payload)
	if err != nil {
		t.Fatalf("decode nested init header: %v", err)
	}
	server, err := dec.ReadString()
	if err != nil {
		t.Fatalf("decode nested init server: %v", err)
	}
	if server != live {
		t.Fatalf("nested init server %q wants %q", server, live)
	}
	seq, err := dec.ReadUint64()
	if err != nil {
		t.Fatalf("decode nested init seq: %v", err)
	}
	if seq != vizExtraInitSeq {
		t.Fatalf("nested init seq %d wants 0", seq)
	}
	n, err := dec.ReadSequenceLength()
	if err != nil {
		t.Fatalf("decode nested init len: %v", err)
	}
	if n != 1 {
		t.Fatalf("nested init len %d wants 1", n)
	}
	err = decodeVizExtraInteractiveMarkerFrom(dec, live, false)
	if err != nil {
		t.Fatalf("decode nested init marker: %v", err)
	}
	err = dec.Finish()
	if err != nil {
		t.Fatalf("finish nested init: %v", err)
	}
}

// TestROSVizExtraUpdateNestedLocal checks Update with marker plus pose plus erase.
//
// Live keeps all three sequences empty to stay small. This pure-Go test
// proves marker plus pose plus string sequence paths round trip without
// a peer.
func TestROSVizExtraUpdateNestedLocal(t *testing.T) {
	t.Parallel()
	live := "local-update-full"
	enc := types.NewEncoder(true)
	err := enc.WriteString(live)
	if err != nil {
		t.Fatalf("encode nested update server: %v", err)
	}
	enc.WriteUint64(vizExtraUpdateSeq)
	enc.WriteUint8(vizExtraUpdateType)
	err = enc.WriteSequenceLength(1)
	if err != nil {
		t.Fatalf("encode nested update markers len: %v", err)
	}
	err = encodeVizExtraInteractiveMarkerInto(enc, live, false)
	if err != nil {
		t.Fatalf("encode nested update marker: %v", err)
	}
	err = enc.WriteSequenceLength(1)
	if err != nil {
		t.Fatalf("encode nested update poses len: %v", err)
	}
	err = encodeMatrixHeader(enc, live)
	if err != nil {
		t.Fatalf("encode nested update pose header: %v", err)
	}
	enc.WriteFloat64(1.0)
	enc.WriteFloat64(2.0)
	enc.WriteFloat64(3.0)
	enc.WriteFloat64(0.0)
	enc.WriteFloat64(0.0)
	enc.WriteFloat64(0.0)
	enc.WriteFloat64(1.0)
	err = enc.WriteString(vizExtraPoseName)
	if err != nil {
		t.Fatalf("encode nested update pose name: %v", err)
	}
	err = enc.WriteSequenceLength(1)
	if err != nil {
		t.Fatalf("encode nested update erases len: %v", err)
	}
	err = enc.WriteString(live)
	if err != nil {
		t.Fatalf("encode nested update erase: %v", err)
	}
	payload := enc.Bytes()
	dec, err := types.NewDecoder(payload)
	if err != nil {
		t.Fatalf("decode nested update header: %v", err)
	}
	server, err := dec.ReadString()
	if err != nil {
		t.Fatalf("decode nested update server: %v", err)
	}
	if server != live {
		t.Fatalf("nested update server %q wants %q", server, live)
	}
	seq, err := dec.ReadUint64()
	if err != nil {
		t.Fatalf("decode nested update seq: %v", err)
	}
	if seq != vizExtraUpdateSeq {
		t.Fatalf("nested update seq %d wants 0", seq)
	}
	utype, err := dec.ReadUint8()
	if err != nil {
		t.Fatalf("decode nested update type: %v", err)
	}
	if utype != vizExtraUpdateType {
		t.Fatalf("nested update type %d wants 1", utype)
	}
	nm, err := dec.ReadSequenceLength()
	if err != nil {
		t.Fatalf("decode nested update markers len: %v", err)
	}
	if nm != 1 {
		t.Fatalf("nested update markers len %d wants 1", nm)
	}
	err = decodeVizExtraInteractiveMarkerFrom(dec, live, false)
	if err != nil {
		t.Fatalf("decode nested update marker: %v", err)
	}
	np, err := dec.ReadSequenceLength()
	if err != nil {
		t.Fatalf("decode nested update poses len: %v", err)
	}
	if np != 1 {
		t.Fatalf("nested update poses len %d wants 1", np)
	}
	err = decodeVizExtraPoseFrom(dec, live)
	if err != nil {
		t.Fatalf("decode nested update pose: %v", err)
	}
	ne, err := dec.ReadSequenceLength()
	if err != nil {
		t.Fatalf("decode nested update erases len: %v", err)
	}
	if ne != 1 {
		t.Fatalf("nested update erases len %d wants 1", ne)
	}
	erase, err := dec.ReadString()
	if err != nil {
		t.Fatalf("decode nested update erase: %v", err)
	}
	if erase != live {
		t.Fatalf("nested update erase %q wants %q", erase, live)
	}
	err = dec.Finish()
	if err != nil {
		t.Fatalf("finish nested update: %v", err)
	}
}
