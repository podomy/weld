// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

//nolint:cyclop,dupl,gocognit // sensor rest matrix breadth over remaining sensor types
package ros_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/podomy/weld/internal/transports"
	"github.com/podomy/weld/internal/transports/dds"
	"github.com/podomy/weld/internal/types"
	"github.com/podomy/weld/testbed/live/helpers"
)

// Static sensor rest type hashes from ros2 topic info -v on weld-jazzy.
//
// These are the RIHS01 hashes reported for each ROS type on domain 42.
// Live probing via ros2 topic info -v is preferred at runtime, these values
// are the fallback when the probe cannot run. Values were captured from
// weld-jazzy Jazzy.
const (
	sensorRestHashCameraInfo       = "RIHS01_b3dfd68ff46c9d56c80fd3bd4ed22c7a4ddce8c8348f2f59c299e73118e7e275"
	sensorRestHashCompressedImage  = "RIHS01_15640771531571185e2efc8a100baf923961a4d15d5569652e6cb6691e8e371a"
	sensorRestHashPointCloud       = "RIHS01_614593df71d3c2b9bd4604a71b750fd218f0d65c045ea988b713719455a65b3b"
	sensorRestHashPointField       = "RIHS01_5c6a4750728c2bcfbbf7037225b20b02d4429634732146b742dee1726637ef01"
	sensorRestHashRange            = "RIHS01_b42b62562e93cbfe9d42b82fe5994dfa3d63d7d5c90a317981703f7388adff3a"
	sensorRestHashROI              = "RIHS01_ad16bcba5f9131dcdba6fbded19f726f5440e3c513b4fb586dd3027eeed8abb1"
	sensorRestHashHumidity         = "RIHS01_8687c99b4fb393cb2e545e407b5ea7fd0b5d8960bcd849a0f86c544740138839"
	sensorRestHashTimeRef          = "RIHS01_dd66e84cf40bbb5d5a40472e6ecf2675a031334d4c426abdb2ad41801a8efc99"
	sensorRestHashMagField         = "RIHS01_e80f32f56a20486c9923008fc1a1db07bbb273cbbf6a5b3bfa00835ee00e4dff"
	sensorRestHashJoy              = "RIHS01_0d356c79cad3401e35ffeb75a96a96e08be3ef896b8b83841d73e890989372c5"
	sensorRestHashJointState       = "RIHS01_a13ee3a330e346c9d87b5aa18d24e11690752bd33a0350f11c5882bc9179260e"
	sensorRestHashBattery          = "RIHS01_4bee5dfce981c98faa6828b868307a0a73f992ed0789f374ee96c8f840e69741"
	sensorRestHashIlluminance      = "RIHS01_b954b25f452fcf81a91c9c2a7e3b3fd85c4c873d452aecb3cfd8fd1da732a22d"
	sensorRestHashMultiEcho        = "RIHS01_ba5eac341cd5bbb2701527aa4568e8baec172b69cadb9a1945d6f149d087ee48"
	sensorRestHashMultiDOF         = "RIHS01_4d4ded702cfba7ff3ec783835c1a1425f75939a430355d1fee4b3bbc40b"
	sensorRestHashLaserEcho        = "RIHS01_0fbc05a0db7d37fe52c0f0375356db55da0046f7ef5bd27ca6b34bd0582bc952"
	sensorRestHashNavSatStatus     = "RIHS01_d1ed3befa628e09571bd273b888ba1c1fd187c9a5e0006b385d7e5e9095a3204"
	sensorRestHashJoyFeedback      = "RIHS01_231dd362f71d6fc08272770d07120ad5fe5874ce2dbac70109b28986834290cd"
	sensorRestHashJoyFeedbackArray = "RIHS01_3287c32e1b688cae0455e465443df3cca7dae66ee4ebf85c4658d585037bcaa"
	sensorRestHashChannelFloat     = "RIHS01_92665437ddf39346f4ba39ee32c648390605b633cc077d40f4bd7d58af6cd"
	sensorRestHashPointCloud2      = "RIHS01_9198cabf7da3796ae6fe19c4cb3bdd3525492988c70522628af5daa124bae2b5"
)

// Fixed sensor rest sentinels shared by YAML, encode, and decode.
//
// Types with a Header carry the per-run live ID in frame_id so stale runs
// never match. Types with a string field but no Header carry the live ID
// in that string. Types with neither use per-run topic plus domain
// isolation. Float values use exact binary fractions so local codec checks
// stay stable.
const (
	sensorRestROIX        uint32  = 1
	sensorRestROIY        uint32  = 2
	sensorRestROIH        uint32  = 2
	sensorRestROIW        uint32  = 3
	sensorRestFieldOff    uint32  = 0
	sensorRestFieldDT     uint8   = 7
	sensorRestFieldCnt    uint32  = 1
	sensorRestEcho0       float32 = 1.0
	sensorRestEcho1       float32 = 2.0
	sensorRestStatus      int8    = 1
	sensorRestService     uint16  = 1
	sensorRestFbType      uint8   = 1
	sensorRestFbID        uint8   = 2
	sensorRestFbInt       float32 = 0.5
	sensorRestAxis0       float32 = 1.0
	sensorRestAxis1       float32 = -1.0
	sensorRestBtn0        int32   = 1
	sensorRestBtn1        int32   = 0
	sensorRestFormat              = "jpeg"
	sensorRestRadType     uint8   = 0
	sensorRestFOV         float32 = 0.5
	sensorRestMinRange    float32 = 0.5
	sensorRestMaxRange    float32 = 10.0
	sensorRestRange       float32 = 1.0
	sensorRestHumidity    float64 = 0.5
	sensorRestIllum       float64 = 100.0
	sensorRestMagX        float64 = 1.0
	sensorRestMagY        float64 = 2.0
	sensorRestMagZ        float64 = 3.0
	sensorRestJointPos    float64 = 1.0
	sensorRestJointVel    float64 = 2.0
	sensorRestJointEff    float64 = 3.0
	sensorRestBattVolt    float32 = 12.0
	sensorRestBattTemp    float32 = 25.0
	sensorRestBattCurr    float32 = -1.0
	sensorRestBattChg     float32 = 2.0
	sensorRestBattCap     float32 = 3.0
	sensorRestBattDes     float32 = 3.0
	sensorRestBattPct     float32 = 0.5
	sensorRestBattStat    uint8   = 2
	sensorRestBattHeal    uint8   = 1
	sensorRestBattTech    uint8   = 2
	sensorRestCell0       float32 = 4.0
	sensorRestCell1       float32 = 4.0
	sensorRestCellTemp0   float32 = 4.0
	sensorRestCellTemp1   float32 = 4.0
	sensorRestLocation            = "slot1"
	sensorRestSerial              = "SN001"
	sensorRestCamH        uint32  = 2
	sensorRestCamW        uint32  = 2
	sensorRestCamDist             = "plumb_bob"
	sensorRestPtX         float32 = 1.0
	sensorRestPtY         float32 = 2.0
	sensorRestPtZ         float32 = 3.0
	sensorRestChanVal     float32 = 1.0
	sensorRestPC2H        uint32  = 1
	sensorRestPC2W        uint32  = 1
	sensorRestPC2Step     uint32  = 4
	sensorRestPC2Row      uint32  = 4
	sensorRestJointName           = "joint1"
	sensorRestChannelName         = "ch1"
)

// sensorRestPC2Data holds one float32 1.0 in little endian bytes.
var sensorRestPC2Data = []byte{0x00, 0x00, 0x80, 0x3F}

// sensorRestCompData holds four compressed bytes.
var sensorRestCompData = []byte{0x01, 0x02, 0x03, 0x04}

// checkSensorRestPad accepts only zero pad bytes for bool tail types.
func checkSensorRestPad(dec *types.Decoder, label string) error {
	for dec.Remaining() > 0 {
		pad, err := dec.ReadUint8()
		if err != nil {
			return fmt.Errorf("decode %s pad: %w", label, err)
		}
		if pad != 0 {
			return fmt.Errorf("%s pad 0x%02x wants zero", label, pad)
		}
	}
	return nil
}

// yamlSensorRestROI returns ROS YAML for the fixed RegionOfInterest.
func yamlSensorRestROI(live string) string {
	return "{x_offset: 1, y_offset: 2, height: 2, width: 3, do_rectify: false}"
}

// encodeSensorRestROI encodes the fixed RegionOfInterest as XCDR1.
func encodeSensorRestROI(live string) ([]byte, error) {
	enc := types.NewEncoder(true)
	enc.WriteUint32(sensorRestROIX)
	enc.WriteUint32(sensorRestROIY)
	enc.WriteUint32(sensorRestROIH)
	enc.WriteUint32(sensorRestROIW)
	enc.WriteBool(false)
	return enc.Bytes(), nil
}

// decodeSensorRestROI decodes and checks the fixed RegionOfInterest.
func decodeSensorRestROI(payload []byte, live string) error {
	dec, err := types.NewDecoder(payload)
	if err != nil {
		return fmt.Errorf("decode roi header: %w", err)
	}
	x, err := dec.ReadUint32()
	if err != nil {
		return fmt.Errorf("decode roi x: %w", err)
	}
	y, err := dec.ReadUint32()
	if err != nil {
		return fmt.Errorf("decode roi y: %w", err)
	}
	h, err := dec.ReadUint32()
	if err != nil {
		return fmt.Errorf("decode roi height: %w", err)
	}
	w, err := dec.ReadUint32()
	if err != nil {
		return fmt.Errorf("decode roi width: %w", err)
	}
	if x != sensorRestROIX || y != sensorRestROIY || h != sensorRestROIH || w != sensorRestROIW {
		return fmt.Errorf("roi (%d, %d, %d, %d) wants (1, 2, 2, 3)", x, y, h, w)
	}
	rect, err := dec.ReadBool()
	if err != nil {
		return fmt.Errorf("decode roi rectify: %w", err)
	}
	if rect {
		return errors.New("roi rectify true wants false") //nolint:wrapcheck // new sentinel error, nothing to wrap
	}
	err = checkSensorRestPad(dec, "roi")
	if err != nil {
		return err
	}
	return nil
}

// wantSensorRestROI returns the echo substring for the ROI sentinel.
func wantSensorRestROI(live string) string {
	return "x_offset: 1"
}

// yamlSensorRestPointField returns ROS YAML with live name.
func yamlSensorRestPointField(live string) string {
	return fmt.Sprintf("{name: '%s', offset: 0, datatype: 7, count: 1}", live)
}

// encodeSensorRestPointField encodes PointField with live name.
func encodeSensorRestPointField(live string) ([]byte, error) {
	enc := types.NewEncoder(true)
	err := enc.WriteString(live)
	if err != nil {
		return nil, fmt.Errorf("encode pointfield name: %w", err)
	}
	enc.WriteUint32(sensorRestFieldOff)
	enc.WriteUint8(sensorRestFieldDT)
	enc.WriteUint32(sensorRestFieldCnt)
	return enc.Bytes(), nil
}

// decodeSensorRestPointField decodes and checks live name plus sentinels.
func decodeSensorRestPointField(payload []byte, live string) error {
	dec, err := types.NewDecoder(payload)
	if err != nil {
		return fmt.Errorf("decode pointfield header: %w", err)
	}
	name, err := dec.ReadString()
	if err != nil {
		return fmt.Errorf("decode pointfield name: %w", err)
	}
	if name != live {
		return fmt.Errorf("pointfield name %q wants %q", name, live)
	}
	off, err := dec.ReadUint32()
	if err != nil {
		return fmt.Errorf("decode pointfield offset: %w", err)
	}
	if off != sensorRestFieldOff {
		return fmt.Errorf("pointfield offset %d wants 0", off)
	}
	dt, err := dec.ReadUint8()
	if err != nil {
		return fmt.Errorf("decode pointfield datatype: %w", err)
	}
	if dt != sensorRestFieldDT {
		return fmt.Errorf("pointfield datatype %d wants 7", dt)
	}
	cnt, err := dec.ReadUint32()
	if err != nil {
		return fmt.Errorf("decode pointfield count: %w", err)
	}
	if cnt != sensorRestFieldCnt {
		return fmt.Errorf("pointfield count %d wants 1", cnt)
	}
	err = dec.Finish()
	if err != nil {
		return fmt.Errorf("finish pointfield: %w", err)
	}
	return nil
}

// wantSensorRestPointField returns the live ID expected in echo output.
func wantSensorRestPointField(live string) string {
	return live
}

// yamlSensorRestLaserEcho returns ROS YAML for the fixed LaserEcho.
func yamlSensorRestLaserEcho(live string) string {
	return "{echoes: [1.0, 2.0]}"
}

// encodeSensorRestLaserEcho encodes two fixed echoes as XCDR1.
func encodeSensorRestLaserEcho(live string) ([]byte, error) {
	enc := types.NewEncoder(true)
	err := enc.WriteSequenceLength(2)
	if err != nil {
		return nil, fmt.Errorf("encode laserecho len: %w", err)
	}
	enc.WriteFloat32(sensorRestEcho0)
	enc.WriteFloat32(sensorRestEcho1)
	return enc.Bytes(), nil
}

// decodeSensorRestLaserEcho decodes and checks the two fixed echoes.
func decodeSensorRestLaserEcho(payload []byte, live string) error {
	dec, err := types.NewDecoder(payload)
	if err != nil {
		return fmt.Errorf("decode laserecho header: %w", err)
	}
	n, err := dec.ReadSequenceLength()
	if err != nil {
		return fmt.Errorf("decode laserecho len: %w", err)
	}
	if n != 2 {
		return fmt.Errorf("laserecho len %d wants 2", n)
	}
	e0, err := dec.ReadFloat32()
	if err != nil {
		return fmt.Errorf("decode laserecho 0: %w", err)
	}
	e1, err := dec.ReadFloat32()
	if err != nil {
		return fmt.Errorf("decode laserecho 1: %w", err)
	}
	if e0 != sensorRestEcho0 || e1 != sensorRestEcho1 {
		return fmt.Errorf("laserecho (%v, %v) wants (1.0, 2.0)", e0, e1)
	}
	err = dec.Finish()
	if err != nil {
		return fmt.Errorf("finish laserecho: %w", err)
	}
	return nil
}

// wantSensorRestLaserEcho returns the echo substring for the echo sentinel.
func wantSensorRestLaserEcho(live string) string {
	return "1.0"
}

// yamlSensorRestNavSatStatus returns ROS YAML for the fixed NavSatStatus.
func yamlSensorRestNavSatStatus(live string) string {
	return "{status: 1, service: 1}"
}

// encodeSensorRestNavSatStatus encodes the fixed NavSatStatus as XCDR1.
func encodeSensorRestNavSatStatus(live string) ([]byte, error) {
	enc := types.NewEncoder(true)
	enc.WriteInt8(sensorRestStatus)
	enc.WriteUint16(sensorRestService)
	return enc.Bytes(), nil
}

// decodeSensorRestNavSatStatus decodes and checks the fixed NavSatStatus.
func decodeSensorRestNavSatStatus(payload []byte, live string) error {
	dec, err := types.NewDecoder(payload)
	if err != nil {
		return fmt.Errorf("decode navsatstatus header: %w", err)
	}
	status, err := dec.ReadInt8()
	if err != nil {
		return fmt.Errorf("decode navsatstatus status: %w", err)
	}
	service, err := dec.ReadUint16()
	if err != nil {
		return fmt.Errorf("decode navsatstatus service: %w", err)
	}
	if status != sensorRestStatus || service != sensorRestService {
		return fmt.Errorf("navsatstatus (%d, %d) wants (1, 1)", status, service)
	}
	err = checkSensorRestPad(dec, "navsatstatus")
	if err != nil {
		return err
	}
	return nil
}

// wantSensorRestNavSatStatus returns the echo substring for the status.
func wantSensorRestNavSatStatus(live string) string {
	return "status: 1"
}

// yamlSensorRestJoyFeedback returns ROS YAML for the fixed JoyFeedback.
func yamlSensorRestJoyFeedback(live string) string {
	return "{type: 1, id: 2, intensity: 0.5}"
}

// encodeSensorRestJoyFeedbackBody writes type plus id plus intensity.
func encodeSensorRestJoyFeedbackBody(enc *types.Encoder) {
	enc.WriteUint8(sensorRestFbType)
	enc.WriteUint8(sensorRestFbID)
	enc.WriteFloat32(sensorRestFbInt)
}

// encodeSensorRestJoyFeedback encodes the fixed JoyFeedback as XCDR1.
func encodeSensorRestJoyFeedback(live string) ([]byte, error) {
	enc := types.NewEncoder(true)
	encodeSensorRestJoyFeedbackBody(enc)
	return enc.Bytes(), nil
}

// decodeSensorRestJoyFeedbackBody reads type plus id plus intensity.
func decodeSensorRestJoyFeedbackBody(dec *types.Decoder) error {
	typ, err := dec.ReadUint8()
	if err != nil {
		return fmt.Errorf("decode joyfeedback type: %w", err)
	}
	id, err := dec.ReadUint8()
	if err != nil {
		return fmt.Errorf("decode joyfeedback id: %w", err)
	}
	inten, err := dec.ReadFloat32()
	if err != nil {
		return fmt.Errorf("decode joyfeedback intensity: %w", err)
	}
	if typ != sensorRestFbType || id != sensorRestFbID || inten != sensorRestFbInt {
		return fmt.Errorf("joyfeedback (%d, %d, %v) wants (1, 2, 0.5)", typ, id, inten)
	}
	return nil
}

// decodeSensorRestJoyFeedback decodes and checks the fixed JoyFeedback.
func decodeSensorRestJoyFeedback(payload []byte, live string) error {
	dec, err := types.NewDecoder(payload)
	if err != nil {
		return fmt.Errorf("decode joyfeedback header: %w", err)
	}
	err = decodeSensorRestJoyFeedbackBody(dec)
	if err != nil {
		return err
	}
	err = dec.Finish()
	if err != nil {
		return fmt.Errorf("finish joyfeedback: %w", err)
	}
	return nil
}

// wantSensorRestJoyFeedback returns the echo substring for JoyFeedback.
func wantSensorRestJoyFeedback(live string) string {
	return "type: 1"
}

// yamlSensorRestJoyFeedbackArray returns ROS YAML for one JoyFeedback.
func yamlSensorRestJoyFeedbackArray(live string) string {
	return "{array: [{type: 1, id: 2, intensity: 0.5}]}"
}

// encodeSensorRestJoyFeedbackArray encodes one fixed JoyFeedback in a seq.
func encodeSensorRestJoyFeedbackArray(live string) ([]byte, error) {
	enc := types.NewEncoder(true)
	err := enc.WriteSequenceLength(1)
	if err != nil {
		return nil, fmt.Errorf("encode joyfeedbackarray len: %w", err)
	}
	encodeSensorRestJoyFeedbackBody(enc)
	return enc.Bytes(), nil
}

// decodeSensorRestJoyFeedbackArray decodes and checks one JoyFeedback.
func decodeSensorRestJoyFeedbackArray(payload []byte, live string) error {
	dec, err := types.NewDecoder(payload)
	if err != nil {
		return fmt.Errorf("decode joyfeedbackarray header: %w", err)
	}
	n, err := dec.ReadSequenceLength()
	if err != nil {
		return fmt.Errorf("decode joyfeedbackarray len: %w", err)
	}
	if n != 1 {
		return fmt.Errorf("joyfeedbackarray len %d wants 1", n)
	}
	err = decodeSensorRestJoyFeedbackBody(dec)
	if err != nil {
		return err
	}
	err = dec.Finish()
	if err != nil {
		return fmt.Errorf("finish joyfeedbackarray: %w", err)
	}
	return nil
}

// wantSensorRestJoyFeedbackArray returns the echo substring for the array.
func wantSensorRestJoyFeedbackArray(live string) string {
	return "type: 1"
}

// yamlSensorRestJoy returns ROS YAML with live frame_id.
func yamlSensorRestJoy(live string) string {
	return fmt.Sprintf("{header: {stamp: {sec: 0, nanosec: 0}, frame_id: '%s'}, axes: [1.0, -1.0], buttons: [1, 0]}", live)
}

// encodeSensorRestJoy encodes Joy with live frame_id and fixed seqs.
func encodeSensorRestJoy(live string) ([]byte, error) {
	enc := types.NewEncoder(true)
	err := encodeMatrixHeader(enc, live)
	if err != nil {
		return nil, fmt.Errorf("encode joy header: %w", err)
	}
	err = enc.WriteSequenceLength(2)
	if err != nil {
		return nil, fmt.Errorf("encode joy axes len: %w", err)
	}
	enc.WriteFloat32(sensorRestAxis0)
	enc.WriteFloat32(sensorRestAxis1)
	err = enc.WriteSequenceLength(2)
	if err != nil {
		return nil, fmt.Errorf("encode joy buttons len: %w", err)
	}
	enc.WriteInt32(sensorRestBtn0)
	enc.WriteInt32(sensorRestBtn1)
	return enc.Bytes(), nil
}

// decodeSensorRestJoyAxes checks the two fixed axes.
func decodeSensorRestJoyAxes(dec *types.Decoder) error {
	n, err := dec.ReadSequenceLength()
	if err != nil {
		return fmt.Errorf("decode joy axes len: %w", err)
	}
	if n != 2 {
		return fmt.Errorf("joy axes len %d wants 2", n)
	}
	a0, err := dec.ReadFloat32()
	if err != nil {
		return fmt.Errorf("decode joy axis 0: %w", err)
	}
	a1, err := dec.ReadFloat32()
	if err != nil {
		return fmt.Errorf("decode joy axis 1: %w", err)
	}
	if a0 != sensorRestAxis0 || a1 != sensorRestAxis1 {
		return fmt.Errorf("joy axes (%v, %v) wants (1.0, -1.0)", a0, a1)
	}
	return nil
}

// decodeSensorRestJoyButtons checks the two fixed buttons.
func decodeSensorRestJoyButtons(dec *types.Decoder) error {
	n, err := dec.ReadSequenceLength()
	if err != nil {
		return fmt.Errorf("decode joy buttons len: %w", err)
	}
	if n != 2 {
		return fmt.Errorf("joy buttons len %d wants 2", n)
	}
	b0, err := dec.ReadInt32()
	if err != nil {
		return fmt.Errorf("decode joy button 0: %w", err)
	}
	b1, err := dec.ReadInt32()
	if err != nil {
		return fmt.Errorf("decode joy button 1: %w", err)
	}
	if b0 != sensorRestBtn0 || b1 != sensorRestBtn1 {
		return fmt.Errorf("joy buttons (%d, %d) wants (1, 0)", b0, b1)
	}
	return nil
}

// decodeSensorRestJoy decodes and checks live frame_id plus fixed seqs.
func decodeSensorRestJoy(payload []byte, live string) error {
	dec, err := types.NewDecoder(payload)
	if err != nil {
		return fmt.Errorf("decode joy header: %w", err)
	}
	frame, err := decodeMatrixHeader(dec)
	if err != nil {
		return fmt.Errorf("decode joy frame: %w", err)
	}
	if frame != live {
		return fmt.Errorf("joy frame %q wants %q", frame, live)
	}
	err = decodeSensorRestJoyAxes(dec)
	if err != nil {
		return err
	}
	err = decodeSensorRestJoyButtons(dec)
	if err != nil {
		return err
	}
	err = dec.Finish()
	if err != nil {
		return fmt.Errorf("finish joy: %w", err)
	}
	return nil
}

// wantSensorRestJoy returns the live ID expected in echo output.
func wantSensorRestJoy(live string) string {
	return live
}

// yamlSensorRestCompressedImage returns ROS YAML with live frame_id.
func yamlSensorRestCompressedImage(live string) string {
	return fmt.Sprintf("{header: {stamp: {sec: 0, nanosec: 0}, frame_id: '%s'}, format: 'jpeg', data: [1, 2, 3, 4]}", live)
}

// encodeSensorRestCompressedImage encodes CompressedImage with live frame.
func encodeSensorRestCompressedImage(live string) ([]byte, error) {
	enc := types.NewEncoder(true)
	err := encodeMatrixHeader(enc, live)
	if err != nil {
		return nil, fmt.Errorf("encode compressed header: %w", err)
	}
	err = enc.WriteString(sensorRestFormat)
	if err != nil {
		return nil, fmt.Errorf("encode compressed format: %w", err)
	}
	err = enc.WriteOctetSequence(sensorRestCompData)
	if err != nil {
		return nil, fmt.Errorf("encode compressed data: %w", err)
	}
	return enc.Bytes(), nil
}

// decodeSensorRestCompressedImage decodes and checks live frame plus data.
func decodeSensorRestCompressedImage(payload []byte, live string) error {
	dec, err := types.NewDecoder(payload)
	if err != nil {
		return fmt.Errorf("decode compressed header: %w", err)
	}
	frame, err := decodeMatrixHeader(dec)
	if err != nil {
		return fmt.Errorf("decode compressed frame: %w", err)
	}
	if frame != live {
		return fmt.Errorf("compressed frame %q wants %q", frame, live)
	}
	format, err := dec.ReadString()
	if err != nil {
		return fmt.Errorf("decode compressed format: %w", err)
	}
	if format != sensorRestFormat {
		return fmt.Errorf("compressed format %q wants %q", format, sensorRestFormat)
	}
	data, err := dec.ReadOctetSequence()
	if err != nil {
		return fmt.Errorf("decode compressed data: %w", err)
	}
	if len(data) != len(sensorRestCompData) {
		return fmt.Errorf("compressed data len %d wants %d", len(data), len(sensorRestCompData))
	}
	for i := range data {
		if data[i] != sensorRestCompData[i] {
			return fmt.Errorf("compressed data byte %d 0x%02x wants 0x%02x", i, data[i], sensorRestCompData[i])
		}
	}
	err = dec.Finish()
	if err != nil {
		return fmt.Errorf("finish compressed: %w", err)
	}
	return nil
}

// wantSensorRestCompressedImage returns the live ID expected in echo.
func wantSensorRestCompressedImage(live string) string {
	return live
}

// yamlSensorRestRange returns ROS YAML with live frame_id.
func yamlSensorRestRange(live string) string {
	return fmt.Sprintf("{header: {stamp: {sec: 0, nanosec: 0}, frame_id: '%s'}, radiation_type: 0, field_of_view: 0.5, min_range: 0.5, max_range: 10.0, range: 1.0, variance: 0.0}", live)
}

// encodeSensorRestRange encodes Range with live frame_id.
func encodeSensorRestRange(live string) ([]byte, error) {
	enc := types.NewEncoder(true)
	err := encodeMatrixHeader(enc, live)
	if err != nil {
		return nil, fmt.Errorf("encode range header: %w", err)
	}
	enc.WriteUint8(sensorRestRadType)
	enc.WriteFloat32(sensorRestFOV)
	enc.WriteFloat32(sensorRestMinRange)
	enc.WriteFloat32(sensorRestMaxRange)
	enc.WriteFloat32(sensorRestRange)
	enc.WriteFloat32(0.0)
	return enc.Bytes(), nil
}

// decodeSensorRestRangeFloats checks fov plus min plus max plus range.
func decodeSensorRestRangeFloats(dec *types.Decoder) error {
	fov, err := dec.ReadFloat32()
	if err != nil {
		return fmt.Errorf("decode range fov: %w", err)
	}
	minR, err := dec.ReadFloat32()
	if err != nil {
		return fmt.Errorf("decode range min: %w", err)
	}
	maxR, err := dec.ReadFloat32()
	if err != nil {
		return fmt.Errorf("decode range max: %w", err)
	}
	rng, err := dec.ReadFloat32()
	if err != nil {
		return fmt.Errorf("decode range range: %w", err)
	}
	if fov != sensorRestFOV || minR != sensorRestMinRange || maxR != sensorRestMaxRange || rng != sensorRestRange {
		return fmt.Errorf("range floats (%v, %v, %v, %v) wants (0.5, 0.5, 10.0, 1.0)", fov, minR, maxR, rng)
	}
	variance, err := dec.ReadFloat32()
	if err != nil {
		return fmt.Errorf("decode range variance: %w", err)
	}
	if variance != 0.0 {
		return fmt.Errorf("range variance %v wants 0.0", variance)
	}
	return nil
}

// decodeSensorRestRange decodes and checks live frame_id plus sentinels.
func decodeSensorRestRange(payload []byte, live string) error {
	dec, err := types.NewDecoder(payload)
	if err != nil {
		return fmt.Errorf("decode range header: %w", err)
	}
	frame, err := decodeMatrixHeader(dec)
	if err != nil {
		return fmt.Errorf("decode range frame: %w", err)
	}
	if frame != live {
		return fmt.Errorf("range frame %q wants %q", frame, live)
	}
	rad, err := dec.ReadUint8()
	if err != nil {
		return fmt.Errorf("decode range radiation: %w", err)
	}
	if rad != sensorRestRadType {
		return fmt.Errorf("range radiation %d wants 0", rad)
	}
	err = decodeSensorRestRangeFloats(dec)
	if err != nil {
		return err
	}
	err = dec.Finish()
	if err != nil {
		return fmt.Errorf("finish range: %w", err)
	}
	return nil
}

// wantSensorRestRange returns the live ID expected in echo output.
func wantSensorRestRange(live string) string {
	return live
}

// yamlSensorRestHumidity returns ROS YAML with live frame_id.
func yamlSensorRestHumidity(live string) string {
	return fmt.Sprintf("{header: {stamp: {sec: 0, nanosec: 0}, frame_id: '%s'}, relative_humidity: 0.5, variance: 0.0}", live)
}

// encodeSensorRestHumidity encodes RelativeHumidity with live frame_id.
func encodeSensorRestHumidity(live string) ([]byte, error) {
	enc := types.NewEncoder(true)
	err := encodeMatrixHeader(enc, live)
	if err != nil {
		return nil, fmt.Errorf("encode humidity header: %w", err)
	}
	enc.WriteFloat64(sensorRestHumidity)
	enc.WriteFloat64(0.0)
	return enc.Bytes(), nil
}

// decodeSensorRestHumidity decodes and checks live frame_id plus values.
func decodeSensorRestHumidity(payload []byte, live string) error {
	err := decodeMatrixPair(payload, live, "humidity", sensorRestHumidity, "0.5, 0.0")
	if err != nil {
		return fmt.Errorf("decode humidity pair: %w", err)
	}
	return nil
}

// wantSensorRestHumidity returns the live ID expected in echo output.
func wantSensorRestHumidity(live string) string {
	return live
}

// yamlSensorRestIlluminance returns ROS YAML with live frame_id.
func yamlSensorRestIlluminance(live string) string {
	return fmt.Sprintf("{header: {stamp: {sec: 0, nanosec: 0}, frame_id: '%s'}, illuminance: 100.0, variance: 0.0}", live)
}

// encodeSensorRestIlluminance encodes Illuminance with live frame_id.
func encodeSensorRestIlluminance(live string) ([]byte, error) {
	enc := types.NewEncoder(true)
	err := encodeMatrixHeader(enc, live)
	if err != nil {
		return nil, fmt.Errorf("encode illuminance header: %w", err)
	}
	enc.WriteFloat64(sensorRestIllum)
	enc.WriteFloat64(0.0)
	return enc.Bytes(), nil
}

// decodeSensorRestIlluminance decodes and checks live frame plus values.
func decodeSensorRestIlluminance(payload []byte, live string) error {
	err := decodeMatrixPair(payload, live, "illuminance", sensorRestIllum, "100.0, 0.0")
	if err != nil {
		return fmt.Errorf("decode illuminance pair: %w", err)
	}
	return nil
}

// wantSensorRestIlluminance returns the live ID expected in echo output.
func wantSensorRestIlluminance(live string) string {
	return live
}

// yamlSensorRestTimeRef returns ROS YAML with live frame_id and source.
func yamlSensorRestTimeRef(live string) string {
	return fmt.Sprintf("{header: {stamp: {sec: 0, nanosec: 0}, frame_id: '%s'}, time_ref: {sec: 42, nanosec: 123}, source: '%s-src'}", live, live)
}

// encodeSensorRestTimeRef encodes TimeReference with live frame and source.
func encodeSensorRestTimeRef(live string) ([]byte, error) {
	enc := types.NewEncoder(true)
	err := encodeMatrixHeader(enc, live)
	if err != nil {
		return nil, fmt.Errorf("encode timeref header: %w", err)
	}
	enc.WriteInt32(builtinSec)
	enc.WriteUint32(builtinNanosec)
	err = enc.WriteString(live + "-src")
	if err != nil {
		return nil, fmt.Errorf("encode timeref source: %w", err)
	}
	return enc.Bytes(), nil
}

// decodeSensorRestTimeRef decodes and checks live frame plus time plus source.
func decodeSensorRestTimeRef(payload []byte, live string) error {
	dec, err := types.NewDecoder(payload)
	if err != nil {
		return fmt.Errorf("decode timeref header: %w", err)
	}
	frame, err := decodeMatrixHeader(dec)
	if err != nil {
		return fmt.Errorf("decode timeref frame: %w", err)
	}
	if frame != live {
		return fmt.Errorf("timeref frame %q wants %q", frame, live)
	}
	sec, err := dec.ReadInt32()
	if err != nil {
		return fmt.Errorf("decode timeref sec: %w", err)
	}
	nsec, err := dec.ReadUint32()
	if err != nil {
		return fmt.Errorf("decode timeref nanosec: %w", err)
	}
	if sec != builtinSec || nsec != builtinNanosec {
		return fmt.Errorf("timeref time (%d, %d) wants (42, 123)", sec, nsec)
	}
	source, err := dec.ReadString()
	if err != nil {
		return fmt.Errorf("decode timeref source: %w", err)
	}
	if source != live+"-src" {
		return fmt.Errorf("timeref source %q wants %q", source, live+"-src")
	}
	err = dec.Finish()
	if err != nil {
		return fmt.Errorf("finish timeref: %w", err)
	}
	return nil
}

// wantSensorRestTimeRef returns the live ID expected in echo output.
func wantSensorRestTimeRef(live string) string {
	return live
}

// yamlSensorRestMagField returns ROS YAML with live frame_id.
func yamlSensorRestMagField(live string) string {
	return fmt.Sprintf("{header: {stamp: {sec: 0, nanosec: 0}, frame_id: '%s'}, magnetic_field: {x: 1.0, y: 2.0, z: 3.0}, magnetic_field_covariance: [0, 0, 0, 0, 0, 0, 0, 0, 0]}", live)
}

// encodeSensorRestMagField encodes MagneticField with live frame_id.
func encodeSensorRestMagField(live string) ([]byte, error) {
	enc := types.NewEncoder(true)
	err := encodeMatrixHeader(enc, live)
	if err != nil {
		return nil, fmt.Errorf("encode mag header: %w", err)
	}
	enc.WriteFloat64(sensorRestMagX)
	enc.WriteFloat64(sensorRestMagY)
	enc.WriteFloat64(sensorRestMagZ)
	for range 9 {
		enc.WriteFloat64(0.0)
	}
	return enc.Bytes(), nil
}

// decodeSensorRestMagVector checks the three fixed field components.
func decodeSensorRestMagVector(dec *types.Decoder) error {
	x, err := dec.ReadFloat64()
	if err != nil {
		return fmt.Errorf("decode mag x: %w", err)
	}
	y, err := dec.ReadFloat64()
	if err != nil {
		return fmt.Errorf("decode mag y: %w", err)
	}
	z, err := dec.ReadFloat64()
	if err != nil {
		return fmt.Errorf("decode mag z: %w", err)
	}
	if x != sensorRestMagX || y != sensorRestMagY || z != sensorRestMagZ {
		return fmt.Errorf("mag (%v, %v, %v) wants (1.0, 2.0, 3.0)", x, y, z)
	}
	return nil
}

// decodeSensorRestMagCov checks nine zero covariance entries.
func decodeSensorRestMagCov(dec *types.Decoder) error {
	for i := range 9 {
		v, err := dec.ReadFloat64()
		if err != nil {
			return fmt.Errorf("decode mag cov %d: %w", i, err)
		}
		if v != 0.0 {
			return fmt.Errorf("mag cov %d %v wants 0.0", i, v)
		}
	}
	return nil
}

// decodeSensorRestMagField decodes and checks live frame plus sentinels.
func decodeSensorRestMagField(payload []byte, live string) error {
	dec, err := types.NewDecoder(payload)
	if err != nil {
		return fmt.Errorf("decode mag header: %w", err)
	}
	frame, err := decodeMatrixHeader(dec)
	if err != nil {
		return fmt.Errorf("decode mag frame: %w", err)
	}
	if frame != live {
		return fmt.Errorf("mag frame %q wants %q", frame, live)
	}
	err = decodeSensorRestMagVector(dec)
	if err != nil {
		return err
	}
	err = decodeSensorRestMagCov(dec)
	if err != nil {
		return err
	}
	err = dec.Finish()
	if err != nil {
		return fmt.Errorf("finish mag: %w", err)
	}
	return nil
}

// wantSensorRestMagField returns the live ID expected in echo output.
func wantSensorRestMagField(live string) string {
	return live
}

// yamlSensorRestJointState returns ROS YAML with live frame_id.
func yamlSensorRestJointState(live string) string {
	return fmt.Sprintf("{header: {stamp: {sec: 0, nanosec: 0}, frame_id: '%s'}, name: ['joint1'], position: [1.0], velocity: [2.0], effort: [3.0]}", live)
}

// encodeSensorRestJointState encodes JointState with one fixed joint.
func encodeSensorRestJointState(live string) ([]byte, error) {
	enc := types.NewEncoder(true)
	err := encodeMatrixHeader(enc, live)
	if err != nil {
		return nil, fmt.Errorf("encode jointstate header: %w", err)
	}
	err = enc.WriteSequenceLength(1)
	if err != nil {
		return nil, fmt.Errorf("encode jointstate names len: %w", err)
	}
	err = enc.WriteString(sensorRestJointName)
	if err != nil {
		return nil, fmt.Errorf("encode jointstate name: %w", err)
	}
	err = enc.WriteSequenceLength(1)
	if err != nil {
		return nil, fmt.Errorf("encode jointstate position len: %w", err)
	}
	enc.WriteFloat64(sensorRestJointPos)
	err = enc.WriteSequenceLength(1)
	if err != nil {
		return nil, fmt.Errorf("encode jointstate velocity len: %w", err)
	}
	enc.WriteFloat64(sensorRestJointVel)
	err = enc.WriteSequenceLength(1)
	if err != nil {
		return nil, fmt.Errorf("encode jointstate effort len: %w", err)
	}
	enc.WriteFloat64(sensorRestJointEff)
	return enc.Bytes(), nil
}

// decodeSensorRestJointName checks the single fixed joint name.
func decodeSensorRestJointName(dec *types.Decoder) error {
	n, err := dec.ReadSequenceLength()
	if err != nil {
		return fmt.Errorf("decode jointstate names len: %w", err)
	}
	if n != 1 {
		return fmt.Errorf("jointstate names len %d wants 1", n)
	}
	name, err := dec.ReadString()
	if err != nil {
		return fmt.Errorf("decode jointstate name: %w", err)
	}
	if name != sensorRestJointName {
		return fmt.Errorf("jointstate name %q wants %q", name, sensorRestJointName)
	}
	return nil
}

// decodeSensorRestJointSeq checks one float64 sequence with fixed value.
func decodeSensorRestJointSeq(dec *types.Decoder, label string, want float64, wantText string) error {
	n, err := dec.ReadSequenceLength()
	if err != nil {
		return fmt.Errorf("decode jointstate %s len: %w", label, err)
	}
	if n != 1 {
		return fmt.Errorf("jointstate %s len %d wants 1", label, n)
	}
	got, err := dec.ReadFloat64()
	if err != nil {
		return fmt.Errorf("decode jointstate %s value: %w", label, err)
	}
	if got != want {
		return fmt.Errorf("jointstate %s %v wants %s", label, got, wantText)
	}
	return nil
}

// decodeSensorRestJointState decodes and checks live frame plus one joint.
func decodeSensorRestJointState(payload []byte, live string) error {
	dec, err := types.NewDecoder(payload)
	if err != nil {
		return fmt.Errorf("decode jointstate header: %w", err)
	}
	frame, err := decodeMatrixHeader(dec)
	if err != nil {
		return fmt.Errorf("decode jointstate frame: %w", err)
	}
	if frame != live {
		return fmt.Errorf("jointstate frame %q wants %q", frame, live)
	}
	err = decodeSensorRestJointName(dec)
	if err != nil {
		return err
	}
	err = decodeSensorRestJointSeq(dec, "position", sensorRestJointPos, "1.0")
	if err != nil {
		return err
	}
	err = decodeSensorRestJointSeq(dec, "velocity", sensorRestJointVel, "2.0")
	if err != nil {
		return err
	}
	err = decodeSensorRestJointSeq(dec, "effort", sensorRestJointEff, "3.0")
	if err != nil {
		return err
	}
	err = dec.Finish()
	if err != nil {
		return fmt.Errorf("finish jointstate: %w", err)
	}
	return nil
}

// wantSensorRestJointState returns the live ID expected in echo output.
func wantSensorRestJointState(live string) string {
	return live
}

// yamlSensorRestMultiDOF returns ROS YAML with live frame_id.
func yamlSensorRestMultiDOF(live string) string {
	return fmt.Sprintf("{header: {stamp: {sec: 0, nanosec: 0}, frame_id: '%s'}, joint_names: ['joint1'], transforms: [{translation: {x: 1.0, y: 2.0, z: 3.0}, rotation: {x: 0.0, y: 0.0, z: 0.0, w: 1.0}}], twist: [{linear: {x: 0.5, y: -0.5, z: 1.0}, angular: {x: 0.1, y: 0.2, z: 0.3}}], wrench: [{force: {x: 1.0, y: 2.0, z: 3.0}, torque: {x: 4.0, y: 5.0, z: 6.0}}]}", live)
}

// encodeSensorRestMultiDOFTransform writes one fixed transform.
func encodeSensorRestMultiDOFTransform(enc *types.Encoder) {
	enc.WriteFloat64(1.0)
	enc.WriteFloat64(2.0)
	enc.WriteFloat64(3.0)
	enc.WriteFloat64(0.0)
	enc.WriteFloat64(0.0)
	enc.WriteFloat64(0.0)
	enc.WriteFloat64(1.0)
}

// encodeSensorRestMultiDOFTwist writes one fixed twist.
func encodeSensorRestMultiDOFTwist(enc *types.Encoder) {
	enc.WriteFloat64(0.5)
	enc.WriteFloat64(-0.5)
	enc.WriteFloat64(1.0)
	enc.WriteFloat64(0.1)
	enc.WriteFloat64(0.2)
	enc.WriteFloat64(0.3)
}

// encodeSensorRestMultiDOFWrench writes one fixed wrench.
func encodeSensorRestMultiDOFWrench(enc *types.Encoder) {
	enc.WriteFloat64(1.0)
	enc.WriteFloat64(2.0)
	enc.WriteFloat64(3.0)
	enc.WriteFloat64(4.0)
	enc.WriteFloat64(5.0)
	enc.WriteFloat64(6.0)
}

// encodeSensorRestMultiDOF encodes MultiDOFJointState with one joint.
func encodeSensorRestMultiDOF(live string) ([]byte, error) {
	enc := types.NewEncoder(true)
	err := encodeMatrixHeader(enc, live)
	if err != nil {
		return nil, fmt.Errorf("encode multidof header: %w", err)
	}
	err = enc.WriteSequenceLength(1)
	if err != nil {
		return nil, fmt.Errorf("encode multidof names len: %w", err)
	}
	err = enc.WriteString(sensorRestJointName)
	if err != nil {
		return nil, fmt.Errorf("encode multidof name: %w", err)
	}
	err = enc.WriteSequenceLength(1)
	if err != nil {
		return nil, fmt.Errorf("encode multidof transforms len: %w", err)
	}
	encodeSensorRestMultiDOFTransform(enc)
	err = enc.WriteSequenceLength(1)
	if err != nil {
		return nil, fmt.Errorf("encode multidof twists len: %w", err)
	}
	encodeSensorRestMultiDOFTwist(enc)
	err = enc.WriteSequenceLength(1)
	if err != nil {
		return nil, fmt.Errorf("encode multidof wrenches len: %w", err)
	}
	encodeSensorRestMultiDOFWrench(enc)
	return enc.Bytes(), nil
}

// decodeSensorRestMultiDOFNames checks the single fixed joint name.
func decodeSensorRestMultiDOFNames(dec *types.Decoder) error {
	n, err := dec.ReadSequenceLength()
	if err != nil {
		return fmt.Errorf("decode multidof names len: %w", err)
	}
	if n != 1 {
		return fmt.Errorf("multidof names len %d wants 1", n)
	}
	name, err := dec.ReadString()
	if err != nil {
		return fmt.Errorf("decode multidof name: %w", err)
	}
	if name != sensorRestJointName {
		return fmt.Errorf("multidof name %q wants %q", name, sensorRestJointName)
	}
	return nil
}

// decodeSensorRestMultiDOFTransform checks one fixed transform.
func decodeSensorRestMultiDOFTransform(dec *types.Decoder) error {
	n, err := dec.ReadSequenceLength()
	if err != nil {
		return fmt.Errorf("decode multidof transforms len: %w", err)
	}
	if n != 1 {
		return fmt.Errorf("multidof transforms len %d wants 1", n)
	}
	for i, want := range []float64{1.0, 2.0, 3.0, 0.0, 0.0, 0.0, 1.0} {
		got, err := dec.ReadFloat64()
		if err != nil {
			return fmt.Errorf("decode multidof transform %d: %w", i, err)
		}
		if got != want {
			return fmt.Errorf("multidof transform %d %v wants %v", i, got, want)
		}
	}
	return nil
}

// decodeSensorRestMultiDOFTwist checks one fixed twist.
func decodeSensorRestMultiDOFTwist(dec *types.Decoder) error {
	n, err := dec.ReadSequenceLength()
	if err != nil {
		return fmt.Errorf("decode multidof twists len: %w", err)
	}
	if n != 1 {
		return fmt.Errorf("multidof twists len %d wants 1", n)
	}
	for i, want := range []float64{0.5, -0.5, 1.0, 0.1, 0.2, 0.3} {
		got, err := dec.ReadFloat64()
		if err != nil {
			return fmt.Errorf("decode multidof twist %d: %w", i, err)
		}
		if got != want {
			return fmt.Errorf("multidof twist %d %v wants %v", i, got, want)
		}
	}
	return nil
}

// decodeSensorRestMultiDOFWrench checks one fixed wrench.
func decodeSensorRestMultiDOFWrench(dec *types.Decoder) error {
	n, err := dec.ReadSequenceLength()
	if err != nil {
		return fmt.Errorf("decode multidof wrenches len: %w", err)
	}
	if n != 1 {
		return fmt.Errorf("multidof wrenches len %d wants 1", n)
	}
	for i, want := range []float64{1.0, 2.0, 3.0, 4.0, 5.0, 6.0} {
		got, err := dec.ReadFloat64()
		if err != nil {
			return fmt.Errorf("decode multidof wrench %d: %w", i, err)
		}
		if got != want {
			return fmt.Errorf("multidof wrench %d %v wants %v", i, got, want)
		}
	}
	return nil
}

// decodeSensorRestMultiDOF decodes and checks live frame plus one joint.
func decodeSensorRestMultiDOF(payload []byte, live string) error {
	dec, err := types.NewDecoder(payload)
	if err != nil {
		return fmt.Errorf("decode multidof header: %w", err)
	}
	frame, err := decodeMatrixHeader(dec)
	if err != nil {
		return fmt.Errorf("decode multidof frame: %w", err)
	}
	if frame != live {
		return fmt.Errorf("multidof frame %q wants %q", frame, live)
	}
	err = decodeSensorRestMultiDOFNames(dec)
	if err != nil {
		return err
	}
	err = decodeSensorRestMultiDOFTransform(dec)
	if err != nil {
		return err
	}
	err = decodeSensorRestMultiDOFTwist(dec)
	if err != nil {
		return err
	}
	err = decodeSensorRestMultiDOFWrench(dec)
	if err != nil {
		return err
	}
	err = dec.Finish()
	if err != nil {
		return fmt.Errorf("finish multidof: %w", err)
	}
	return nil
}

// wantSensorRestMultiDOF returns the live ID expected in echo output.
func wantSensorRestMultiDOF(live string) string {
	return live
}

// yamlSensorRestBattery returns ROS YAML with live frame_id.
func yamlSensorRestBattery(live string) string {
	return fmt.Sprintf("{header: {stamp: {sec: 0, nanosec: 0}, frame_id: '%s'}, voltage: 12.0, temperature: 25.0, current: -1.0, charge: 2.0, capacity: 3.0, design_capacity: 3.0, percentage: 0.5, power_supply_status: 2, power_supply_health: 1, power_supply_technology: 2, present: true, cell_voltage: [4.0, 4.0], cell_temperature: [4.0, 4.0], location: 'slot1', serial_number: 'SN001'}", live)
}

// encodeSensorRestBatteryFloats writes the seven fixed float32 readings.
func encodeSensorRestBatteryFloats(enc *types.Encoder) {
	enc.WriteFloat32(sensorRestBattVolt)
	enc.WriteFloat32(sensorRestBattTemp)
	enc.WriteFloat32(sensorRestBattCurr)
	enc.WriteFloat32(sensorRestBattChg)
	enc.WriteFloat32(sensorRestBattCap)
	enc.WriteFloat32(sensorRestBattDes)
	enc.WriteFloat32(sensorRestBattPct)
}

// encodeSensorRestBattery encodes BatteryState with live frame_id.
func encodeSensorRestBattery(live string) ([]byte, error) {
	enc := types.NewEncoder(true)
	err := encodeMatrixHeader(enc, live)
	if err != nil {
		return nil, fmt.Errorf("encode battery header: %w", err)
	}
	encodeSensorRestBatteryFloats(enc)
	enc.WriteUint8(sensorRestBattStat)
	enc.WriteUint8(sensorRestBattHeal)
	enc.WriteUint8(sensorRestBattTech)
	enc.WriteBool(true)
	err = enc.WriteSequenceLength(2)
	if err != nil {
		return nil, fmt.Errorf("encode battery cells len: %w", err)
	}
	enc.WriteFloat32(sensorRestCell0)
	enc.WriteFloat32(sensorRestCell1)
	err = enc.WriteSequenceLength(2)
	if err != nil {
		return nil, fmt.Errorf("encode battery cell temps len: %w", err)
	}
	enc.WriteFloat32(sensorRestCellTemp0)
	enc.WriteFloat32(sensorRestCellTemp1)
	err = enc.WriteString(sensorRestLocation)
	if err != nil {
		return nil, fmt.Errorf("encode battery location: %w", err)
	}
	err = enc.WriteString(sensorRestSerial)
	if err != nil {
		return nil, fmt.Errorf("encode battery serial: %w", err)
	}
	return enc.Bytes(), nil
}

// decodeSensorRestBatteryFloats checks the seven fixed float32 readings.
func decodeSensorRestBatteryFloats(dec *types.Decoder) error {
	for i, want := range []float32{sensorRestBattVolt, sensorRestBattTemp, sensorRestBattCurr, sensorRestBattChg, sensorRestBattCap, sensorRestBattDes, sensorRestBattPct} {
		got, err := dec.ReadFloat32()
		if err != nil {
			return fmt.Errorf("decode battery float %d: %w", i, err)
		}
		if got != want {
			return fmt.Errorf("battery float %d %v wants %v", i, got, want)
		}
	}
	return nil
}

// decodeSensorRestBatteryEnums checks status plus health plus tech plus present.
func decodeSensorRestBatteryEnums(dec *types.Decoder) error {
	stat, err := dec.ReadUint8()
	if err != nil {
		return fmt.Errorf("decode battery status: %w", err)
	}
	heal, err := dec.ReadUint8()
	if err != nil {
		return fmt.Errorf("decode battery health: %w", err)
	}
	tech, err := dec.ReadUint8()
	if err != nil {
		return fmt.Errorf("decode battery tech: %w", err)
	}
	if stat != sensorRestBattStat || heal != sensorRestBattHeal || tech != sensorRestBattTech {
		return fmt.Errorf("battery enums (%d, %d, %d) wants (2, 1, 2)", stat, heal, tech)
	}
	present, err := dec.ReadBool()
	if err != nil {
		return fmt.Errorf("decode battery present: %w", err)
	}
	if !present {
		return errors.New("battery present false wants true") //nolint:wrapcheck // new sentinel error, nothing to wrap
	}
	return nil
}

// decodeSensorRestBatteryTail checks cells plus cell temps plus location plus serial.
func decodeSensorRestBatteryTail(dec *types.Decoder) error {
	n, err := dec.ReadSequenceLength()
	if err != nil {
		return fmt.Errorf("decode battery cells len: %w", err)
	}
	if n != 2 {
		return fmt.Errorf("battery cells len %d wants 2", n)
	}
	c0, err := dec.ReadFloat32()
	if err != nil {
		return fmt.Errorf("decode battery cell 0: %w", err)
	}
	c1, err := dec.ReadFloat32()
	if err != nil {
		return fmt.Errorf("decode battery cell 1: %w", err)
	}
	if c0 != sensorRestCell0 || c1 != sensorRestCell1 {
		return fmt.Errorf("battery cells (%v, %v) wants (4.0, 4.0)", c0, c1)
	}
	t, err := dec.ReadSequenceLength()
	if err != nil {
		return fmt.Errorf("decode battery cell temps len: %w", err)
	}
	if t != 2 {
		return fmt.Errorf("battery cell temps len %d wants 2", t)
	}
	t0, err := dec.ReadFloat32()
	if err != nil {
		return fmt.Errorf("decode battery cell temp 0: %w", err)
	}
	t1, err := dec.ReadFloat32()
	if err != nil {
		return fmt.Errorf("decode battery cell temp 1: %w", err)
	}
	if t0 != sensorRestCellTemp0 || t1 != sensorRestCellTemp1 {
		return fmt.Errorf("battery cell temps (%v, %v) wants (4.0, 4.0)", t0, t1)
	}
	loc, err := dec.ReadString()
	if err != nil {
		return fmt.Errorf("decode battery location: %w", err)
	}
	if loc != sensorRestLocation {
		return fmt.Errorf("battery location %q wants %q", loc, sensorRestLocation)
	}
	serial, err := dec.ReadString()
	if err != nil {
		return fmt.Errorf("decode battery serial: %w", err)
	}
	if serial != sensorRestSerial {
		return fmt.Errorf("battery serial %q wants %q", serial, sensorRestSerial)
	}
	return nil
}

// decodeSensorRestBattery decodes and checks live frame plus sentinels.
func decodeSensorRestBattery(payload []byte, live string) error {
	dec, err := types.NewDecoder(payload)
	if err != nil {
		return fmt.Errorf("decode battery header: %w", err)
	}
	frame, err := decodeMatrixHeader(dec)
	if err != nil {
		return fmt.Errorf("decode battery frame: %w", err)
	}
	if frame != live {
		return fmt.Errorf("battery frame %q wants %q", frame, live)
	}
	err = decodeSensorRestBatteryFloats(dec)
	if err != nil {
		return err
	}
	err = decodeSensorRestBatteryEnums(dec)
	if err != nil {
		return err
	}
	err = decodeSensorRestBatteryTail(dec)
	if err != nil {
		return err
	}
	err = dec.Finish()
	if err != nil {
		return fmt.Errorf("finish battery: %w", err)
	}
	return nil
}

// wantSensorRestBattery returns the live ID expected in echo output.
func wantSensorRestBattery(live string) string {
	return live
}

// yamlSensorRestCameraInfo returns ROS YAML with live frame_id.
func yamlSensorRestCameraInfo(live string) string {
	return fmt.Sprintf("{header: {stamp: {sec: 0, nanosec: 0}, frame_id: '%s'}, height: 2, width: 2, distortion_model: 'plumb_bob', d: [0.0], k: [0, 0, 0, 0, 0, 0, 0, 0, 0], r: [0, 0, 0, 0, 0, 0, 0, 0, 0], p: [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0], binning_x: 0, binning_y: 0, roi: {x_offset: 0, y_offset: 0, height: 0, width: 0, do_rectify: false}}", live)
}

// encodeSensorRestCameraInfoBody writes CameraInfo fields after the header.
func encodeSensorRestCameraInfoBody(enc *types.Encoder) error {
	enc.WriteUint32(sensorRestCamH)
	enc.WriteUint32(sensorRestCamW)
	err := enc.WriteString(sensorRestCamDist)
	if err != nil {
		return fmt.Errorf("encode camerainfo distortion: %w", err)
	}
	err = enc.WriteSequenceLength(1)
	if err != nil {
		return fmt.Errorf("encode camerainfo d len: %w", err)
	}
	enc.WriteFloat64(0.0)
	for range 9 {
		enc.WriteFloat64(0.0)
	}
	for range 9 {
		enc.WriteFloat64(0.0)
	}
	for range 12 {
		enc.WriteFloat64(0.0)
	}
	enc.WriteUint32(0)
	enc.WriteUint32(0)
	enc.WriteUint32(0)
	enc.WriteUint32(0)
	enc.WriteUint32(0)
	enc.WriteUint32(0)
	enc.WriteBool(false)
	return nil
}

// encodeSensorRestCameraInfo encodes CameraInfo with live frame_id.
func encodeSensorRestCameraInfo(live string) ([]byte, error) {
	enc := types.NewEncoder(true)
	err := encodeMatrixHeader(enc, live)
	if err != nil {
		return nil, fmt.Errorf("encode camerainfo header: %w", err)
	}
	err = encodeSensorRestCameraInfoBody(enc)
	if err != nil {
		return nil, err
	}
	return enc.Bytes(), nil
}

// decodeSensorRestCameraInfoDims checks height plus width plus distortion.
func decodeSensorRestCameraInfoDims(dec *types.Decoder) error {
	h, err := dec.ReadUint32()
	if err != nil {
		return fmt.Errorf("decode camerainfo height: %w", err)
	}
	w, err := dec.ReadUint32()
	if err != nil {
		return fmt.Errorf("decode camerainfo width: %w", err)
	}
	if h != sensorRestCamH || w != sensorRestCamW {
		return fmt.Errorf("camerainfo size (%d, %d) wants (2, 2)", h, w)
	}
	dist, err := dec.ReadString()
	if err != nil {
		return fmt.Errorf("decode camerainfo distortion: %w", err)
	}
	if dist != sensorRestCamDist {
		return fmt.Errorf("camerainfo distortion %q wants %q", dist, sensorRestCamDist)
	}
	return nil
}

// decodeSensorRestCameraInfoMatrices checks d plus k plus r plus p.
func decodeSensorRestCameraInfoMatrices(dec *types.Decoder) error {
	n, err := dec.ReadSequenceLength()
	if err != nil {
		return fmt.Errorf("decode camerainfo d len: %w", err)
	}
	if n != 1 {
		return fmt.Errorf("camerainfo d len %d wants 1", n)
	}
	d0, err := dec.ReadFloat64()
	if err != nil {
		return fmt.Errorf("decode camerainfo d 0: %w", err)
	}
	if d0 != 0.0 {
		return fmt.Errorf("camerainfo d 0 %v wants 0.0", d0)
	}
	for i := range 9 {
		v, err := dec.ReadFloat64()
		if err != nil {
			return fmt.Errorf("decode camerainfo k %d: %w", i, err)
		}
		if v != 0.0 {
			return fmt.Errorf("camerainfo k %d %v wants 0.0", i, v)
		}
	}
	for i := range 9 {
		v, err := dec.ReadFloat64()
		if err != nil {
			return fmt.Errorf("decode camerainfo r %d: %w", i, err)
		}
		if v != 0.0 {
			return fmt.Errorf("camerainfo r %d %v wants 0.0", i, v)
		}
	}
	for i := range 12 {
		v, err := dec.ReadFloat64()
		if err != nil {
			return fmt.Errorf("decode camerainfo p %d: %w", i, err)
		}
		if v != 0.0 {
			return fmt.Errorf("camerainfo p %d %v wants 0.0", i, v)
		}
	}
	return nil
}

// decodeSensorRestCameraInfoBinningROI checks binning plus roi.
func decodeSensorRestCameraInfoBinningROI(dec *types.Decoder) error {
	bx, err := dec.ReadUint32()
	if err != nil {
		return fmt.Errorf("decode camerainfo binning_x: %w", err)
	}
	by, err := dec.ReadUint32()
	if err != nil {
		return fmt.Errorf("decode camerainfo binning_y: %w", err)
	}
	if bx != 0 || by != 0 {
		return fmt.Errorf("camerainfo binning (%d, %d) wants (0, 0)", bx, by)
	}
	for i, label := range []string{"roi x", "roi y", "roi height", "roi width"} {
		v, err := dec.ReadUint32()
		if err != nil {
			return fmt.Errorf("decode camerainfo %s: %w", label, err)
		}
		if v != 0 {
			return fmt.Errorf("camerainfo %s %d wants 0", label, v)
		}
		_ = i
	}
	rect, err := dec.ReadBool()
	if err != nil {
		return fmt.Errorf("decode camerainfo rectify: %w", err)
	}
	if rect {
		return errors.New("camerainfo rectify true wants false") //nolint:wrapcheck // new sentinel error, nothing to wrap
	}
	return nil
}

// decodeSensorRestCameraInfo decodes and checks live frame plus matrices.
func decodeSensorRestCameraInfo(payload []byte, live string) error {
	dec, err := types.NewDecoder(payload)
	if err != nil {
		return fmt.Errorf("decode camerainfo header: %w", err)
	}
	frame, err := decodeMatrixHeader(dec)
	if err != nil {
		return fmt.Errorf("decode camerainfo frame: %w", err)
	}
	if frame != live {
		return fmt.Errorf("camerainfo frame %q wants %q", frame, live)
	}
	err = decodeSensorRestCameraInfoDims(dec)
	if err != nil {
		return err
	}
	err = decodeSensorRestCameraInfoMatrices(dec)
	if err != nil {
		return err
	}
	err = decodeSensorRestCameraInfoBinningROI(dec)
	if err != nil {
		return err
	}
	err = checkSensorRestPad(dec, "camerainfo")
	if err != nil {
		return err
	}
	return nil
}

// wantSensorRestCameraInfo returns the live ID expected in echo output.
func wantSensorRestCameraInfo(live string) string {
	return live
}

// yamlSensorRestPointCloud returns ROS YAML with live frame_id.
func yamlSensorRestPointCloud(live string) string {
	return fmt.Sprintf("{header: {stamp: {sec: 0, nanosec: 0}, frame_id: '%s'}, points: [{x: 1.0, y: 2.0, z: 3.0}], channels: [{name: 'ch1', values: [1.0]}]}", live)
}

// encodeSensorRestPointCloud encodes PointCloud with one point and channel.
func encodeSensorRestPointCloud(live string) ([]byte, error) {
	enc := types.NewEncoder(true)
	err := encodeMatrixHeader(enc, live)
	if err != nil {
		return nil, fmt.Errorf("encode pointcloud header: %w", err)
	}
	err = enc.WriteSequenceLength(1)
	if err != nil {
		return nil, fmt.Errorf("encode pointcloud points len: %w", err)
	}
	enc.WriteFloat32(sensorRestPtX)
	enc.WriteFloat32(sensorRestPtY)
	enc.WriteFloat32(sensorRestPtZ)
	err = enc.WriteSequenceLength(1)
	if err != nil {
		return nil, fmt.Errorf("encode pointcloud channels len: %w", err)
	}
	err = enc.WriteString(sensorRestChannelName)
	if err != nil {
		return nil, fmt.Errorf("encode pointcloud channel name: %w", err)
	}
	err = enc.WriteSequenceLength(1)
	if err != nil {
		return nil, fmt.Errorf("encode pointcloud values len: %w", err)
	}
	enc.WriteFloat32(sensorRestChanVal)
	return enc.Bytes(), nil
}

// decodeSensorRestPointCloudPoints checks the single fixed point.
func decodeSensorRestPointCloudPoints(dec *types.Decoder) error {
	n, err := dec.ReadSequenceLength()
	if err != nil {
		return fmt.Errorf("decode pointcloud points len: %w", err)
	}
	if n != 1 {
		return fmt.Errorf("pointcloud points len %d wants 1", n)
	}
	x, err := dec.ReadFloat32()
	if err != nil {
		return fmt.Errorf("decode pointcloud x: %w", err)
	}
	y, err := dec.ReadFloat32()
	if err != nil {
		return fmt.Errorf("decode pointcloud y: %w", err)
	}
	z, err := dec.ReadFloat32()
	if err != nil {
		return fmt.Errorf("decode pointcloud z: %w", err)
	}
	if x != sensorRestPtX || y != sensorRestPtY || z != sensorRestPtZ {
		return fmt.Errorf("pointcloud point (%v, %v, %v) wants (1.0, 2.0, 3.0)", x, y, z)
	}
	return nil
}

// decodeSensorRestPointCloudChannels checks the single fixed channel.
func decodeSensorRestPointCloudChannels(dec *types.Decoder) error {
	n, err := dec.ReadSequenceLength()
	if err != nil {
		return fmt.Errorf("decode pointcloud channels len: %w", err)
	}
	if n != 1 {
		return fmt.Errorf("pointcloud channels len %d wants 1", n)
	}
	name, err := dec.ReadString()
	if err != nil {
		return fmt.Errorf("decode pointcloud channel name: %w", err)
	}
	if name != sensorRestChannelName {
		return fmt.Errorf("pointcloud channel %q wants %q", name, sensorRestChannelName)
	}
	m, err := dec.ReadSequenceLength()
	if err != nil {
		return fmt.Errorf("decode pointcloud values len: %w", err)
	}
	if m != 1 {
		return fmt.Errorf("pointcloud values len %d wants 1", m)
	}
	v, err := dec.ReadFloat32()
	if err != nil {
		return fmt.Errorf("decode pointcloud value: %w", err)
	}
	if v != sensorRestChanVal {
		return fmt.Errorf("pointcloud value %v wants 1.0", v)
	}
	return nil
}

// decodeSensorRestPointCloud decodes and checks live frame plus point.
func decodeSensorRestPointCloud(payload []byte, live string) error {
	dec, err := types.NewDecoder(payload)
	if err != nil {
		return fmt.Errorf("decode pointcloud header: %w", err)
	}
	frame, err := decodeMatrixHeader(dec)
	if err != nil {
		return fmt.Errorf("decode pointcloud frame: %w", err)
	}
	if frame != live {
		return fmt.Errorf("pointcloud frame %q wants %q", frame, live)
	}
	err = decodeSensorRestPointCloudPoints(dec)
	if err != nil {
		return err
	}
	err = decodeSensorRestPointCloudChannels(dec)
	if err != nil {
		return err
	}
	err = dec.Finish()
	if err != nil {
		return fmt.Errorf("finish pointcloud: %w", err)
	}
	return nil
}

// wantSensorRestPointCloud returns the live ID expected in echo output.
func wantSensorRestPointCloud(live string) string {
	return live
}

// yamlSensorRestChannelFloat returns ROS YAML with live name.
func yamlSensorRestChannelFloat(live string) string {
	return fmt.Sprintf("{name: '%s', values: [1.0, 2.0]}", live)
}

// encodeSensorRestChannelFloat encodes ChannelFloat32 with live name.
func encodeSensorRestChannelFloat(live string) ([]byte, error) {
	enc := types.NewEncoder(true)
	err := enc.WriteString(live)
	if err != nil {
		return nil, fmt.Errorf("encode channelfloat name: %w", err)
	}
	err = enc.WriteSequenceLength(2)
	if err != nil {
		return nil, fmt.Errorf("encode channelfloat values len: %w", err)
	}
	enc.WriteFloat32(sensorRestEcho0)
	enc.WriteFloat32(sensorRestEcho1)
	return enc.Bytes(), nil
}

// decodeSensorRestChannelFloat decodes and checks live name plus values.
func decodeSensorRestChannelFloat(payload []byte, live string) error {
	dec, err := types.NewDecoder(payload)
	if err != nil {
		return fmt.Errorf("decode channelfloat header: %w", err)
	}
	name, err := dec.ReadString()
	if err != nil {
		return fmt.Errorf("decode channelfloat name: %w", err)
	}
	if name != live {
		return fmt.Errorf("channelfloat name %q wants %q", name, live)
	}
	n, err := dec.ReadSequenceLength()
	if err != nil {
		return fmt.Errorf("decode channelfloat values len: %w", err)
	}
	if n != 2 {
		return fmt.Errorf("channelfloat values len %d wants 2", n)
	}
	v0, err := dec.ReadFloat32()
	if err != nil {
		return fmt.Errorf("decode channelfloat 0: %w", err)
	}
	v1, err := dec.ReadFloat32()
	if err != nil {
		return fmt.Errorf("decode channelfloat 1: %w", err)
	}
	if v0 != sensorRestEcho0 || v1 != sensorRestEcho1 {
		return fmt.Errorf("channelfloat (%v, %v) wants (1.0, 2.0)", v0, v1)
	}
	err = dec.Finish()
	if err != nil {
		return fmt.Errorf("finish channelfloat: %w", err)
	}
	return nil
}

// wantSensorRestChannelFloat returns the live ID expected in echo output.
func wantSensorRestChannelFloat(live string) string {
	return live
}

// yamlSensorRestPointCloud2 returns ROS YAML with live frame_id.
func yamlSensorRestPointCloud2(live string) string {
	return fmt.Sprintf("{header: {stamp: {sec: 0, nanosec: 0}, frame_id: '%s'}, height: 1, width: 1, fields: [{name: 'x', offset: 0, datatype: 7, count: 1}], is_bigendian: false, point_step: 4, row_step: 4, data: [0, 0, 128, 63], is_dense: true}", live)
}

// encodeSensorRestPointCloud2 encodes PointCloud2 with one x field.
func encodeSensorRestPointCloud2(live string) ([]byte, error) {
	enc := types.NewEncoder(true)
	err := encodeMatrixHeader(enc, live)
	if err != nil {
		return nil, fmt.Errorf("encode pc2 header: %w", err)
	}
	enc.WriteUint32(sensorRestPC2H)
	enc.WriteUint32(sensorRestPC2W)
	err = enc.WriteSequenceLength(1)
	if err != nil {
		return nil, fmt.Errorf("encode pc2 fields len: %w", err)
	}
	err = enc.WriteString("x")
	if err != nil {
		return nil, fmt.Errorf("encode pc2 field name: %w", err)
	}
	enc.WriteUint32(0)
	enc.WriteUint8(7)
	enc.WriteUint32(1)
	enc.WriteBool(false)
	enc.WriteUint32(sensorRestPC2Step)
	enc.WriteUint32(sensorRestPC2Row)
	err = enc.WriteOctetSequence(sensorRestPC2Data)
	if err != nil {
		return nil, fmt.Errorf("encode pc2 data: %w", err)
	}
	enc.WriteBool(true)
	return enc.Bytes(), nil
}

// decodeSensorRestPointCloud2Dims checks height plus width plus fields.
func decodeSensorRestPointCloud2Dims(dec *types.Decoder) error {
	h, err := dec.ReadUint32()
	if err != nil {
		return fmt.Errorf("decode pc2 height: %w", err)
	}
	w, err := dec.ReadUint32()
	if err != nil {
		return fmt.Errorf("decode pc2 width: %w", err)
	}
	if h != sensorRestPC2H || w != sensorRestPC2W {
		return fmt.Errorf("pc2 size (%d, %d) wants (1, 1)", h, w)
	}
	n, err := dec.ReadSequenceLength()
	if err != nil {
		return fmt.Errorf("decode pc2 fields len: %w", err)
	}
	if n != 1 {
		return fmt.Errorf("pc2 fields len %d wants 1", n)
	}
	name, err := dec.ReadString()
	if err != nil {
		return fmt.Errorf("decode pc2 field name: %w", err)
	}
	if name != "x" {
		return fmt.Errorf("pc2 field name %q wants %q", name, "x")
	}
	off, err := dec.ReadUint32()
	if err != nil {
		return fmt.Errorf("decode pc2 offset: %w", err)
	}
	if off != 0 {
		return fmt.Errorf("pc2 offset %d wants 0", off)
	}
	dt, err := dec.ReadUint8()
	if err != nil {
		return fmt.Errorf("decode pc2 datatype: %w", err)
	}
	if dt != 7 {
		return fmt.Errorf("pc2 datatype %d wants 7", dt)
	}
	cnt, err := dec.ReadUint32()
	if err != nil {
		return fmt.Errorf("decode pc2 count: %w", err)
	}
	if cnt != 1 {
		return fmt.Errorf("pc2 count %d wants 1", cnt)
	}
	return nil
}

// decodeSensorRestPointCloud2Steps checks endian plus steps plus data.
func decodeSensorRestPointCloud2Steps(dec *types.Decoder) error {
	big, err := dec.ReadBool()
	if err != nil {
		return fmt.Errorf("decode pc2 bigendian: %w", err)
	}
	if big {
		return errors.New("pc2 bigendian true wants false") //nolint:wrapcheck // new sentinel error, nothing to wrap
	}
	step, err := dec.ReadUint32()
	if err != nil {
		return fmt.Errorf("decode pc2 step: %w", err)
	}
	row, err := dec.ReadUint32()
	if err != nil {
		return fmt.Errorf("decode pc2 row: %w", err)
	}
	if step != sensorRestPC2Step || row != sensorRestPC2Row {
		return fmt.Errorf("pc2 steps (%d, %d) wants (4, 4)", step, row)
	}
	data, err := dec.ReadOctetSequence()
	if err != nil {
		return fmt.Errorf("decode pc2 data: %w", err)
	}
	if len(data) != len(sensorRestPC2Data) {
		return fmt.Errorf("pc2 data len %d wants %d", len(data), len(sensorRestPC2Data))
	}
	for i := range data {
		if data[i] != sensorRestPC2Data[i] {
			return fmt.Errorf("pc2 data byte %d 0x%02x wants 0x%02x", i, data[i], sensorRestPC2Data[i])
		}
	}
	dense, err := dec.ReadBool()
	if err != nil {
		return fmt.Errorf("decode pc2 dense: %w", err)
	}
	if !dense {
		return errors.New("pc2 dense false wants true") //nolint:wrapcheck // new sentinel error, nothing to wrap
	}
	return nil
}

// decodeSensorRestPointCloud2 decodes and checks live frame plus field.
func decodeSensorRestPointCloud2(payload []byte, live string) error {
	dec, err := types.NewDecoder(payload)
	if err != nil {
		return fmt.Errorf("decode pc2 header: %w", err)
	}
	frame, err := decodeMatrixHeader(dec)
	if err != nil {
		return fmt.Errorf("decode pc2 frame: %w", err)
	}
	if frame != live {
		return fmt.Errorf("pc2 frame %q wants %q", frame, live)
	}
	err = decodeSensorRestPointCloud2Dims(dec)
	if err != nil {
		return err
	}
	err = decodeSensorRestPointCloud2Steps(dec)
	if err != nil {
		return err
	}
	err = checkSensorRestPad(dec, "pc2")
	if err != nil {
		return err
	}
	return nil
}

// wantSensorRestPointCloud2 returns the live ID expected in echo output.
func wantSensorRestPointCloud2(live string) string {
	return live
}

// yamlSensorRestMultiEcho returns ROS YAML with live frame_id.
func yamlSensorRestMultiEcho(live string) string {
	return fmt.Sprintf("{header: {stamp: {sec: 0, nanosec: 0}, frame_id: '%s'}, angle_min: -1.0, angle_max: 1.0, angle_increment: 0.5, time_increment: 0.0, scan_time: 0.5, range_min: 0.5, range_max: 10.0, ranges: [{echoes: [1.0, 2.0]}], intensities: [{echoes: [10.0, 20.0]}]}", live)
}

// encodeSensorRestMultiEchoSeq writes one LaserEcho with two values.
func encodeSensorRestMultiEchoSeq(enc *types.Encoder, v0, v1 float32, label string) error {
	err := enc.WriteSequenceLength(1)
	if err != nil {
		return fmt.Errorf("encode multiecho %s len: %w", label, err)
	}
	err = enc.WriteSequenceLength(2)
	if err != nil {
		return fmt.Errorf("encode multiecho %s echoes len: %w", label, err)
	}
	enc.WriteFloat32(v0)
	enc.WriteFloat32(v1)
	return nil
}

// encodeSensorRestMultiEcho encodes MultiEchoLaserScan with one echo each.
func encodeSensorRestMultiEcho(live string) ([]byte, error) {
	enc := types.NewEncoder(true)
	err := encodeMatrixHeader(enc, live)
	if err != nil {
		return nil, fmt.Errorf("encode multiecho header: %w", err)
	}
	enc.WriteFloat32(-1.0)
	enc.WriteFloat32(1.0)
	enc.WriteFloat32(0.5)
	enc.WriteFloat32(0.0)
	enc.WriteFloat32(0.5)
	enc.WriteFloat32(0.5)
	enc.WriteFloat32(10.0)
	err = encodeSensorRestMultiEchoSeq(enc, 1.0, 2.0, "ranges")
	if err != nil {
		return nil, err
	}
	err = encodeSensorRestMultiEchoSeq(enc, 10.0, 20.0, "intensities")
	if err != nil {
		return nil, err
	}
	return enc.Bytes(), nil
}

// decodeSensorRestMultiEchoAngles checks the seven fixed float32 fields.
func decodeSensorRestMultiEchoAngles(dec *types.Decoder) error {
	for i, want := range []float32{-1.0, 1.0, 0.5, 0.0, 0.5, 0.5, 10.0} {
		got, err := dec.ReadFloat32()
		if err != nil {
			return fmt.Errorf("decode multiecho angle %d: %w", i, err)
		}
		if got != want {
			return fmt.Errorf("multiecho angle %d %v wants %v", i, got, want)
		}
	}
	return nil
}

// decodeSensorRestMultiEchoSeq checks one LaserEcho with two values.
func decodeSensorRestMultiEchoSeq(dec *types.Decoder, label string, v0, v1 float32, wantText string) error {
	n, err := dec.ReadSequenceLength()
	if err != nil {
		return fmt.Errorf("decode multiecho %s len: %w", label, err)
	}
	if n != 1 {
		return fmt.Errorf("multiecho %s len %d wants 1", label, n)
	}
	m, err := dec.ReadSequenceLength()
	if err != nil {
		return fmt.Errorf("decode multiecho %s echoes len: %w", label, err)
	}
	if m != 2 {
		return fmt.Errorf("multiecho %s echoes len %d wants 2", label, m)
	}
	g0, err := dec.ReadFloat32()
	if err != nil {
		return fmt.Errorf("decode multiecho %s 0: %w", label, err)
	}
	g1, err := dec.ReadFloat32()
	if err != nil {
		return fmt.Errorf("decode multiecho %s 1: %w", label, err)
	}
	if g0 != v0 || g1 != v1 {
		return fmt.Errorf("multiecho %s (%v, %v) wants %s", label, g0, g1, wantText)
	}
	return nil
}

// decodeSensorRestMultiEcho decodes and checks live frame plus echoes.
func decodeSensorRestMultiEcho(payload []byte, live string) error {
	dec, err := types.NewDecoder(payload)
	if err != nil {
		return fmt.Errorf("decode multiecho header: %w", err)
	}
	frame, err := decodeMatrixHeader(dec)
	if err != nil {
		return fmt.Errorf("decode multiecho frame: %w", err)
	}
	if frame != live {
		return fmt.Errorf("multiecho frame %q wants %q", frame, live)
	}
	err = decodeSensorRestMultiEchoAngles(dec)
	if err != nil {
		return err
	}
	err = decodeSensorRestMultiEchoSeq(dec, "ranges", 1.0, 2.0, "(1.0, 2.0)")
	if err != nil {
		return err
	}
	err = decodeSensorRestMultiEchoSeq(dec, "intensities", 10.0, 20.0, "(10.0, 20.0)")
	if err != nil {
		return err
	}
	err = dec.Finish()
	if err != nil {
		return fmt.Errorf("finish multiecho: %w", err)
	}
	return nil
}

// wantSensorRestMultiEcho returns the live ID expected in echo output.
func wantSensorRestMultiEcho(live string) string {
	return live
}

// sensorRestCases returns the rest sensor live matrix in corpus order.
func sensorRestCases() []typeMatrixCase {
	return []typeMatrixCase{
		{
			Label:      "sensor-camerainfo",
			RosType:    "sensor_msgs/msg/CameraInfo",
			IDLName:    "sensor_msgs__msg__CameraInfo",
			staticHash: sensorRestHashCameraInfo,
			yaml:       yamlSensorRestCameraInfo,
			encode:     encodeSensorRestCameraInfo,
			decode:     decodeSensorRestCameraInfo,
			want:       wantSensorRestCameraInfo,
		},
		{
			Label:      "sensor-compressedimage",
			RosType:    "sensor_msgs/msg/CompressedImage",
			IDLName:    "sensor_msgs__msg__CompressedImage",
			staticHash: sensorRestHashCompressedImage,
			yaml:       yamlSensorRestCompressedImage,
			encode:     encodeSensorRestCompressedImage,
			decode:     decodeSensorRestCompressedImage,
			want:       wantSensorRestCompressedImage,
		},
		{
			Label:      "sensor-pointcloud",
			RosType:    "sensor_msgs/msg/PointCloud",
			IDLName:    "sensor_msgs__msg__PointCloud",
			staticHash: sensorRestHashPointCloud,
			yaml:       yamlSensorRestPointCloud,
			encode:     encodeSensorRestPointCloud,
			decode:     decodeSensorRestPointCloud,
			want:       wantSensorRestPointCloud,
		},
		{
			Label:      "sensor-pointfield",
			RosType:    "sensor_msgs/msg/PointField",
			IDLName:    "sensor_msgs__msg__PointField",
			staticHash: sensorRestHashPointField,
			yaml:       yamlSensorRestPointField,
			encode:     encodeSensorRestPointField,
			decode:     decodeSensorRestPointField,
			want:       wantSensorRestPointField,
		},
		{
			Label:      "sensor-range",
			RosType:    "sensor_msgs/msg/Range",
			IDLName:    "sensor_msgs__msg__Range",
			staticHash: sensorRestHashRange,
			yaml:       yamlSensorRestRange,
			encode:     encodeSensorRestRange,
			decode:     decodeSensorRestRange,
			want:       wantSensorRestRange,
		},
		{
			Label:      "sensor-roi",
			RosType:    "sensor_msgs/msg/RegionOfInterest",
			IDLName:    "sensor_msgs__msg__RegionOfInterest",
			staticHash: sensorRestHashROI,
			yaml:       yamlSensorRestROI,
			encode:     encodeSensorRestROI,
			decode:     decodeSensorRestROI,
			want:       wantSensorRestROI,
		},
		{
			Label:      "sensor-humidity",
			RosType:    "sensor_msgs/msg/RelativeHumidity",
			IDLName:    "sensor_msgs__msg__RelativeHumidity",
			staticHash: sensorRestHashHumidity,
			yaml:       yamlSensorRestHumidity,
			encode:     encodeSensorRestHumidity,
			decode:     decodeSensorRestHumidity,
			want:       wantSensorRestHumidity,
		},
		{
			Label:      "sensor-timeref",
			RosType:    "sensor_msgs/msg/TimeReference",
			IDLName:    "sensor_msgs__msg__TimeReference",
			staticHash: sensorRestHashTimeRef,
			yaml:       yamlSensorRestTimeRef,
			encode:     encodeSensorRestTimeRef,
			decode:     decodeSensorRestTimeRef,
			want:       wantSensorRestTimeRef,
		},
		{
			Label:      "sensor-magfield",
			RosType:    "sensor_msgs/msg/MagneticField",
			IDLName:    "sensor_msgs__msg__MagneticField",
			staticHash: sensorRestHashMagField,
			yaml:       yamlSensorRestMagField,
			encode:     encodeSensorRestMagField,
			decode:     decodeSensorRestMagField,
			want:       wantSensorRestMagField,
		},
		{
			Label:      "sensor-joy",
			RosType:    "sensor_msgs/msg/Joy",
			IDLName:    "sensor_msgs__msg__Joy",
			staticHash: sensorRestHashJoy,
			yaml:       yamlSensorRestJoy,
			encode:     encodeSensorRestJoy,
			decode:     decodeSensorRestJoy,
			want:       wantSensorRestJoy,
		},
		{
			Label:      "sensor-jointstate",
			RosType:    "sensor_msgs/msg/JointState",
			IDLName:    "sensor_msgs__msg__JointState",
			staticHash: sensorRestHashJointState,
			yaml:       yamlSensorRestJointState,
			encode:     encodeSensorRestJointState,
			decode:     decodeSensorRestJointState,
			want:       wantSensorRestJointState,
		},
		{
			Label:      "sensor-batterystate",
			RosType:    "sensor_msgs/msg/BatteryState",
			IDLName:    "sensor_msgs__msg__BatteryState",
			staticHash: sensorRestHashBattery,
			yaml:       yamlSensorRestBattery,
			encode:     encodeSensorRestBattery,
			decode:     decodeSensorRestBattery,
			want:       wantSensorRestBattery,
		},
		{
			Label:      "sensor-illuminance",
			RosType:    "sensor_msgs/msg/Illuminance",
			IDLName:    "sensor_msgs__msg__Illuminance",
			staticHash: sensorRestHashIlluminance,
			yaml:       yamlSensorRestIlluminance,
			encode:     encodeSensorRestIlluminance,
			decode:     decodeSensorRestIlluminance,
			want:       wantSensorRestIlluminance,
		},
		{
			Label:      "sensor-multiecho",
			RosType:    "sensor_msgs/msg/MultiEchoLaserScan",
			IDLName:    "sensor_msgs__msg__MultiEchoLaserScan",
			staticHash: sensorRestHashMultiEcho,
			yaml:       yamlSensorRestMultiEcho,
			encode:     encodeSensorRestMultiEcho,
			decode:     decodeSensorRestMultiEcho,
			want:       wantSensorRestMultiEcho,
		},
		{
			Label:      "sensor-multidof",
			RosType:    "sensor_msgs/msg/MultiDOFJointState",
			IDLName:    "sensor_msgs__msg__MultiDOFJointState",
			staticHash: sensorRestHashMultiDOF,
			yaml:       yamlSensorRestMultiDOF,
			encode:     encodeSensorRestMultiDOF,
			decode:     decodeSensorRestMultiDOF,
			want:       wantSensorRestMultiDOF,
		},
		{
			Label:      "sensor-laserecho",
			RosType:    "sensor_msgs/msg/LaserEcho",
			IDLName:    "sensor_msgs__msg__LaserEcho",
			staticHash: sensorRestHashLaserEcho,
			yaml:       yamlSensorRestLaserEcho,
			encode:     encodeSensorRestLaserEcho,
			decode:     decodeSensorRestLaserEcho,
			want:       wantSensorRestLaserEcho,
		},
		{
			Label:      "sensor-navsatstatus",
			RosType:    "sensor_msgs/msg/NavSatStatus",
			IDLName:    "sensor_msgs__msg__NavSatStatus",
			staticHash: sensorRestHashNavSatStatus,
			yaml:       yamlSensorRestNavSatStatus,
			encode:     encodeSensorRestNavSatStatus,
			decode:     decodeSensorRestNavSatStatus,
			want:       wantSensorRestNavSatStatus,
		},
		{
			Label:      "sensor-joyfeedback",
			RosType:    "sensor_msgs/msg/JoyFeedback",
			IDLName:    "sensor_msgs__msg__JoyFeedback",
			staticHash: sensorRestHashJoyFeedback,
			yaml:       yamlSensorRestJoyFeedback,
			encode:     encodeSensorRestJoyFeedback,
			decode:     decodeSensorRestJoyFeedback,
			want:       wantSensorRestJoyFeedback,
		},
		{
			Label:      "sensor-joyfeedbackarray",
			RosType:    "sensor_msgs/msg/JoyFeedbackArray",
			IDLName:    "sensor_msgs__msg__JoyFeedbackArray",
			staticHash: sensorRestHashJoyFeedbackArray,
			yaml:       yamlSensorRestJoyFeedbackArray,
			encode:     encodeSensorRestJoyFeedbackArray,
			decode:     decodeSensorRestJoyFeedbackArray,
			want:       wantSensorRestJoyFeedbackArray,
		},
		{
			Label:      "sensor-channelfloat",
			RosType:    "sensor_msgs/msg/ChannelFloat32",
			IDLName:    "sensor_msgs__msg__ChannelFloat32",
			staticHash: sensorRestHashChannelFloat,
			yaml:       yamlSensorRestChannelFloat,
			encode:     encodeSensorRestChannelFloat,
			decode:     decodeSensorRestChannelFloat,
			want:       wantSensorRestChannelFloat,
		},
		{
			Label:      "sensor-pointcloud2",
			RosType:    "sensor_msgs/msg/PointCloud2",
			IDLName:    "sensor_msgs__msg__PointCloud2",
			staticHash: sensorRestHashPointCloud2,
			yaml:       yamlSensorRestPointCloud2,
			encode:     encodeSensorRestPointCloud2,
			decode:     decodeSensorRestPointCloud2,
			want:       wantSensorRestPointCloud2,
		},
	}
}

// runSensorRestLabel runs forward plus reverse for one rest sensor label.
//
// Domain is the allocated exclusive domain shared by both directions.
// It looks up the case, skips when the peer lacks the type, then runs
// forward with one live ID and reverse with another so the two directions
// never share a topic. Forward and reverse reuse the shared matrix runners
// so domain 42 plus docker argv plus hash probing stay identical.
func runSensorRestLabel(t *testing.T, ctx context.Context, domain int, label string) {
	t.Helper()
	runSharedMatrixLabel(t, ctx, domain, label, sensorRestCases())
}

// TestROSTypeSensorCameraInfo checks CameraInfo forward plus reverse.
func TestROSTypeSensorCameraInfo(t *testing.T) {
	t.Parallel()
	domain := helpers.MustAllocateDomain(t)
	ctx, cancel := context.WithTimeout(context.Background(), typeMatrixTestTimeout)
	defer cancel()
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)
	runSensorRestLabel(t, ctx, domain, "sensor-camerainfo")
}

// TestROSTypeSensorCompressedImage checks CompressedImage forward plus reverse.
func TestROSTypeSensorCompressedImage(t *testing.T) {
	t.Parallel()
	domain := helpers.MustAllocateDomain(t)
	ctx, cancel := context.WithTimeout(context.Background(), typeMatrixTestTimeout)
	defer cancel()
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)
	runSensorRestLabel(t, ctx, domain, "sensor-compressedimage")
}

// TestROSTypeSensorPointCloud checks PointCloud forward plus reverse.
func TestROSTypeSensorPointCloud(t *testing.T) {
	t.Parallel()
	domain := helpers.MustAllocateDomain(t)
	ctx, cancel := context.WithTimeout(context.Background(), typeMatrixTestTimeout)
	defer cancel()
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)
	runSensorRestLabel(t, ctx, domain, "sensor-pointcloud")
}

// TestROSTypeSensorPointField checks PointField forward plus reverse.
func TestROSTypeSensorPointField(t *testing.T) {
	t.Parallel()
	domain := helpers.MustAllocateDomain(t)
	ctx, cancel := context.WithTimeout(context.Background(), typeMatrixTestTimeout)
	defer cancel()
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)
	runSensorRestLabel(t, ctx, domain, "sensor-pointfield")
}

// TestROSTypeSensorRange checks Range forward plus reverse.
func TestROSTypeSensorRange(t *testing.T) {
	t.Parallel()
	domain := helpers.MustAllocateDomain(t)
	ctx, cancel := context.WithTimeout(context.Background(), typeMatrixTestTimeout)
	defer cancel()
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)
	runSensorRestLabel(t, ctx, domain, "sensor-range")
}

// TestROSTypeSensorROI checks RegionOfInterest forward plus reverse.
func TestROSTypeSensorROI(t *testing.T) {
	t.Parallel()
	domain := helpers.MustAllocateDomain(t)
	ctx, cancel := context.WithTimeout(context.Background(), typeMatrixTestTimeout)
	defer cancel()
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)
	runSensorRestLabel(t, ctx, domain, "sensor-roi")
}

// TestROSTypeSensorHumidity checks RelativeHumidity forward plus reverse.
func TestROSTypeSensorHumidity(t *testing.T) {
	t.Parallel()
	domain := helpers.MustAllocateDomain(t)
	ctx, cancel := context.WithTimeout(context.Background(), typeMatrixTestTimeout)
	defer cancel()
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)
	runSensorRestLabel(t, ctx, domain, "sensor-humidity")
}

// TestROSTypeSensorTimeRef checks TimeReference forward plus reverse.
func TestROSTypeSensorTimeRef(t *testing.T) {
	t.Parallel()
	domain := helpers.MustAllocateDomain(t)
	ctx, cancel := context.WithTimeout(context.Background(), typeMatrixTestTimeout)
	defer cancel()
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)
	runSensorRestLabel(t, ctx, domain, "sensor-timeref")
}

// TestROSTypeSensorMagField checks MagneticField forward plus reverse.
func TestROSTypeSensorMagField(t *testing.T) {
	t.Parallel()
	domain := helpers.MustAllocateDomain(t)
	ctx, cancel := context.WithTimeout(context.Background(), typeMatrixTestTimeout)
	defer cancel()
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)
	runSensorRestLabel(t, ctx, domain, "sensor-magfield")
}

// TestROSTypeSensorJoy checks Joy forward plus reverse.
func TestROSTypeSensorJoy(t *testing.T) {
	t.Parallel()
	domain := helpers.MustAllocateDomain(t)
	ctx, cancel := context.WithTimeout(context.Background(), typeMatrixTestTimeout)
	defer cancel()
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)
	runSensorRestLabel(t, ctx, domain, "sensor-joy")
}

// TestROSTypeSensorJointState checks JointState forward plus reverse.
func TestROSTypeSensorJointState(t *testing.T) {
	t.Parallel()
	domain := helpers.MustAllocateDomain(t)
	ctx, cancel := context.WithTimeout(context.Background(), typeMatrixTestTimeout)
	defer cancel()
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)
	runSensorRestLabel(t, ctx, domain, "sensor-jointstate")
}

// TestROSTypeSensorBatteryState checks BatteryState forward plus reverse.
func TestROSTypeSensorBatteryState(t *testing.T) {
	t.Parallel()
	domain := helpers.MustAllocateDomain(t)
	ctx, cancel := context.WithTimeout(context.Background(), typeMatrixTestTimeout)
	defer cancel()
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)
	runSensorRestLabel(t, ctx, domain, "sensor-batterystate")
}

// TestROSTypeSensorIlluminance checks Illuminance forward plus reverse.
func TestROSTypeSensorIlluminance(t *testing.T) {
	t.Parallel()
	domain := helpers.MustAllocateDomain(t)
	ctx, cancel := context.WithTimeout(context.Background(), typeMatrixTestTimeout)
	defer cancel()
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)
	runSensorRestLabel(t, ctx, domain, "sensor-illuminance")
}

// TestROSTypeSensorMultiEcho checks MultiEchoLaserScan forward plus reverse.
func TestROSTypeSensorMultiEcho(t *testing.T) {
	t.Parallel()
	domain := helpers.MustAllocateDomain(t)
	ctx, cancel := context.WithTimeout(context.Background(), typeMatrixTestTimeout)
	defer cancel()
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)
	runSensorRestLabel(t, ctx, domain, "sensor-multiecho")
}

// TestROSTypeSensorMultiDOF checks MultiDOFJointState forward plus reverse.
func TestROSTypeSensorMultiDOF(t *testing.T) {
	t.Parallel()
	domain := helpers.MustAllocateDomain(t)
	ctx, cancel := context.WithTimeout(context.Background(), typeMatrixTestTimeout)
	defer cancel()
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)
	runSensorRestLabel(t, ctx, domain, "sensor-multidof")
}

// TestROSTypeSensorLaserEcho checks LaserEcho forward plus reverse.
func TestROSTypeSensorLaserEcho(t *testing.T) {
	t.Parallel()
	domain := helpers.MustAllocateDomain(t)
	ctx, cancel := context.WithTimeout(context.Background(), typeMatrixTestTimeout)
	defer cancel()
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)
	runSensorRestLabel(t, ctx, domain, "sensor-laserecho")
}

// TestROSTypeSensorNavSatStatus checks NavSatStatus forward plus reverse.
func TestROSTypeSensorNavSatStatus(t *testing.T) {
	t.Parallel()
	domain := helpers.MustAllocateDomain(t)
	ctx, cancel := context.WithTimeout(context.Background(), typeMatrixTestTimeout)
	defer cancel()
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)
	runSensorRestLabel(t, ctx, domain, "sensor-navsatstatus")
}

// TestROSTypeSensorJoyFeedback checks JoyFeedback forward plus reverse.
func TestROSTypeSensorJoyFeedback(t *testing.T) {
	t.Parallel()
	domain := helpers.MustAllocateDomain(t)
	ctx, cancel := context.WithTimeout(context.Background(), typeMatrixTestTimeout)
	defer cancel()
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)
	runSensorRestLabel(t, ctx, domain, "sensor-joyfeedback")
}

// TestROSTypeSensorJoyFeedbackArray checks JoyFeedbackArray forward plus reverse.
func TestROSTypeSensorJoyFeedbackArray(t *testing.T) {
	t.Parallel()
	domain := helpers.MustAllocateDomain(t)
	ctx, cancel := context.WithTimeout(context.Background(), typeMatrixTestTimeout)
	defer cancel()
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)
	runSensorRestLabel(t, ctx, domain, "sensor-joyfeedbackarray")
}

// TestROSTypeSensorChannelFloat checks ChannelFloat32 forward plus reverse.
func TestROSTypeSensorChannelFloat(t *testing.T) {
	t.Parallel()
	domain := helpers.MustAllocateDomain(t)
	ctx, cancel := context.WithTimeout(context.Background(), typeMatrixTestTimeout)
	defer cancel()
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)
	runSensorRestLabel(t, ctx, domain, "sensor-channelfloat")
}

// TestROSTypeSensorPointCloud2 checks PointCloud2 forward plus reverse.
func TestROSTypeSensorPointCloud2(t *testing.T) {
	t.Parallel()
	domain := helpers.MustAllocateDomain(t)
	ctx, cancel := context.WithTimeout(context.Background(), typeMatrixTestTimeout)
	defer cancel()
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)
	runSensorRestLabel(t, ctx, domain, "sensor-pointcloud2")
}

// sensorRestLargeAllowEntry pins one temporary payload miss exception.
//
// An entry allows a skip only for an exact RosType plus exact RIHS01 Hash
// while time is before Expiry. The list stays empty so every miss fails.
type sensorRestLargeAllowEntry struct {
	RosType string
	Hash    string
	Expiry  time.Time
}

// sensorRestLargeAllowlist holds temporary exceptions. Empty is strict.
var sensorRestLargeAllowlist []sensorRestLargeAllowEntry

// sensorRestLargeAllowlisted reports whether a miss may skip.
//
// It requires exact type plus exact hash plus unexpired time. Empty list
// means false so callers fail.
func sensorRestLargeAllowlisted(rosType, hash string, now time.Time) bool {
	for _, e := range sensorRestLargeAllowlist {
		if e.RosType == rosType && e.Hash == hash && now.Before(e.Expiry) {
			return true
		}
	}
	return false
}

// publishSensorRestLargeStrict publishes until ros2 echo holds the live ID.
//
// It fails on any publish error including no route. It returns false only
// when the poll wait ends without the echo, and the caller then fails.
// No payload miss skips. Only peer absent plus type absent skip.
func publishSensorRestLargeStrict(ctx context.Context, t *testing.T, backend *dds.Backend, msg transports.OutgoingMessage, echoPath, live string) bool {
	t.Helper()
	deadline := time.Now().Add(largePayloadPoll)
	for {
		err := helpers.PublishLiveWithRetry(ctx, backend, msg, largePayloadPubRetry, largePayloadPubGap, "large")
		if err != nil {
			t.Fatalf("weld pub rest large image: %v", err)
		}
		if helpers.WaitForFileContains(ctx, echoPath, live, largePayloadRepub) {
			return true
		}
		cerr := ctx.Err()
		if cerr != nil {
			return false
		}
		if time.Now().After(deadline) {
			return false
		}
	}
}

// TestROSTypeSensorImageLarge proves a 100KiB Image flows via Weld.
//
// It reuses the large payload proof pattern on one allocated exclusive domain: a
// 320 by 320 mono8 Image with deterministic bytes and a per-run live ID
// in header frame_id. The local XCDR1 codec round trips full bytes, then
// the fragmented sample publishes through Weld and ros2 topic echo in the
// already running weld-jazzy peer proves the live ID plus geometry plus
// all data bytes. The sample stays under the 32MiB maxPayloadLen cap while
// exceeding one DATA MTU so the wire fragments. It uses os/exec docker
// argv only with context timeouts and skips cleanly when no peer runs or
// when the Image type is absent.
func TestROSTypeSensorImageLarge(t *testing.T) {
	t.Parallel()
	domain := helpers.MustAllocateDomain(t)
	ctx, cancel := context.WithTimeout(context.Background(), largePayloadOverall)
	defer cancel()
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)
	skipIfNoImageType(t, ctx, domain)
	runID := helpers.LiveID("ros-rest-image-large")
	topic := helpers.LiveTopic(runID)
	wantData := buildLargeImageData()
	wire := encodeLargeImage(t, runID, wantData)
	err := decodeLargeImage(wire, runID, wantData)
	if err != nil {
		t.Fatalf("local rest large image codec: %v", err)
	}
	t.Logf("local rest large image codec ok len %d id %s", len(wire), runID)
	backend := helpers.MustOpenLiveBackend(t, ctx, uint32(domain), "ros-rest-image-large-"+runID, "large") //nolint:gosec // G115: pooled domain 70 through 101 fits uint32
	echoPath := filepath.Join(t.TempDir(), "rosecho-rest-large-image-"+runID+".log")
	startLargeEcho(t, ctx, uint32(domain), topic, echoPath) //nolint:gosec // G115: pooled domain 70 through 101 fits uint32
	helpers.Sleep(ctx, largePayloadSettle)
	cerr := ctx.Err()
	if cerr != nil {
		t.Fatalf("context ended before rest large pub: %v", cerr)
	}
	msg := transports.OutgoingMessage{
		TopicName: topic,
		TypeName:  largePayloadIDL,
		Payload:   wire,
	}
	if !publishSensorRestLargeStrict(ctx, t, backend, msg, echoPath, runID) {
		data, rerr := os.ReadFile(echoPath) //nolint:gosec // live harness reads controlled temp file path
		if rerr != nil {
			t.Fatalf("ros echo did not see rest large image %q: %v", runID, rerr)
		}
		if sensorRestLargeAllowlisted(largePayloadRosType, sensorRest2HashImage, time.Now()) {
			t.Skipf("ros echo did not see rest large image %q, allowlisted", runID)
		}
		t.Fatalf("ros echo did not see rest large image %q, pub ok len %d\necho output:\n%s", runID, len(wire), string(data))
	}
	helpers.Sleep(ctx, 2*time.Second)
	verifyLargeEcho(t, echoPath, runID, wantData)
	t.Logf("rest large image fragmented ok len %d id %s", len(wire), runID)
}

// Sensor SetCameraInfo service pins below are pure Go with no Docker.
//
// SetCameraInfo travels on rq request DDS topics and rr reply DDS topics,
// never on the rt topic path, so topic gate coverage is N/A. The test pins
// the corpus kind plus summary, the rq and rr topic kind mapping, and XCDR
// request and response round trips through the service envelope.

// sensorRestSvcGUID returns a fixed 8 byte service GUID for tests.
func sensorRestSvcGUID() [8]byte {
	var guid [8]byte
	for i := range guid {
		guid[i] = byte(i + 1)
	}
	return guid
}

// sensorRestSvcSeq is the fixed service sequence number for tests.
const sensorRestSvcSeq = uint64(7)

// sensorRestSvcFrame is the fixed frame_id for service body payloads.
const sensorRestSvcFrame = "weld-sensor-svc"

// sensorRestCheckCorpus returns the corpus entry for a sensor service.
func sensorRestCheckCorpus(t *testing.T, name string) types.CorpusEntry {
	t.Helper()
	entry, ok := types.Lookup("sensor_msgs", name)
	if !ok {
		t.Fatalf("corpus misses sensor_msgs/%s, no shape to pin", name)
	}
	if entry.Kind != types.KindService {
		t.Fatalf("corpus sensor_msgs/%s kind %q is not service", name, entry.Kind)
	}
	return entry
}

// sensorRestRequireSummary fails when the summary misses a fragment.
func sensorRestRequireSummary(t *testing.T, entry types.CorpusEntry, fragments ...string) {
	t.Helper()
	summary := strings.ToLower(entry.Summary)
	for _, fragment := range fragments {
		if !strings.Contains(summary, strings.ToLower(fragment)) {
			t.Fatalf("corpus %s/%s Summary %q misses %q", entry.Package, entry.Name, entry.Summary, fragment)
		}
	}
}

// sensorRestNewServiceEncoder returns an encoder with the service header.
func sensorRestNewServiceEncoder(t *testing.T, le bool, guid [8]byte, seq uint64) *types.Encoder {
	t.Helper()
	enc := types.NewEncoder(le)
	err := enc.WriteServiceHeader(guid, seq)
	if err != nil {
		t.Fatalf("write service header: %v", err)
	}
	return enc
}

// sensorRestNewServiceDecoder returns a decoder after the service header.
func sensorRestNewServiceDecoder(t *testing.T, wire []byte, guid [8]byte, seq uint64) *types.Decoder {
	t.Helper()
	dec, err := types.NewDecoder(wire)
	if err != nil {
		t.Fatalf("new decoder: %v", err)
	}
	gotGUID, gotSeq, err := dec.ReadServiceHeader()
	if err != nil {
		t.Fatalf("read service header: %v", err)
	}
	if gotGUID != guid {
		t.Fatalf("service guid = %v, want %v", gotGUID, guid)
	}
	if gotSeq != seq {
		t.Fatalf("service seq = %d, want %d", gotSeq, seq)
	}
	if dec.Offset() != types.ServiceEnvelopeSize {
		t.Fatalf("service payload offset = %d, want %d", dec.Offset(), types.ServiceEnvelopeSize)
	}
	return dec
}

// sensorRestSvcFinish checks strict consumption with no drops.
func sensorRestSvcFinish(t *testing.T, dec *types.Decoder, label string) {
	t.Helper()
	err := dec.Finish()
	if err != nil {
		t.Fatalf("finish %s: %v", label, err)
	}
	if dec.Dropped() != 0 {
		t.Fatalf("decoder %s drops = %d, want 0", label, dec.Dropped())
	}
}

// sensorRestCheckServiceKind pins that a service maps to rq and rr.
func sensorRestCheckServiceKind(t *testing.T, service string) {
	t.Helper()
	requestTopic, err := dds.RequestTopic(service, false)
	if err != nil {
		t.Fatalf("request topic %q: %v", service, err)
	}
	replyTopic, err := dds.ReplyTopic(service, false)
	if err != nil {
		t.Fatalf("reply topic %q: %v", service, err)
	}
	_, requestKind, err := dds.ParseTopicName(requestTopic)
	if err != nil {
		t.Fatalf("parse request topic %q: %v", requestTopic, err)
	}
	if requestKind != dds.TopicKindRequest {
		t.Fatalf("request topic %q kind = %q, want %q", requestTopic, requestKind, dds.TopicKindRequest)
	}
	_, replyKind, err := dds.ParseTopicName(replyTopic)
	if err != nil {
		t.Fatalf("parse reply topic %q: %v", replyTopic, err)
	}
	if replyKind != dds.TopicKindReply {
		t.Fatalf("reply topic %q kind = %q, want %q", replyTopic, replyKind, dds.TopicKindReply)
	}
	if requestKind == dds.TopicKindTopic || replyKind == dds.TopicKindTopic {
		t.Fatalf("service topics classified as plain topic, want request and reply outside topic gate")
	}
}

// sensorRestEncodeCameraInfoBody writes a CameraInfo body for services.
func sensorRestEncodeCameraInfoBody(enc *types.Encoder, frame string) error {
	err := encodeMatrixHeader(enc, frame)
	if err != nil {
		return fmt.Errorf("encode svc camerainfo header: %w", err)
	}
	err = encodeSensorRestCameraInfoBody(enc)
	if err != nil {
		return fmt.Errorf("encode svc camerainfo body: %w", err)
	}
	return nil
}

// sensorRestDecodeCameraInfoBody reads a CameraInfo body for services.
func sensorRestDecodeCameraInfoBody(dec *types.Decoder, frame, label string) error {
	gotFrame, err := decodeMatrixHeader(dec)
	if err != nil {
		return fmt.Errorf("decode %s frame: %w", label, err)
	}
	if gotFrame != frame {
		return fmt.Errorf("%s frame %q wants %q", label, gotFrame, frame)
	}
	err = decodeSensorRestCameraInfoDims(dec)
	if err != nil {
		return fmt.Errorf("decode %s dims: %w", label, err)
	}
	err = decodeSensorRestCameraInfoMatrices(dec)
	if err != nil {
		return fmt.Errorf("decode %s matrices: %w", label, err)
	}
	err = decodeSensorRestCameraInfoBinningROI(dec)
	if err != nil {
		return fmt.Errorf("decode %s binning roi: %w", label, err)
	}
	err = checkSensorRestPad(dec, label)
	if err != nil {
		return fmt.Errorf("decode %s pad: %w", label, err)
	}
	return nil
}

// TestROSPureGoSensorSetCameraInfo pins SetCameraInfo shapes with pure Go.
//
// SetCameraInfo travels on rq and rr DDS topics, so topic gate coverage is
// N/A. The test pins the corpus kind plus summary, the rq kind as
// TopicKindRequest and the rr kind as TopicKindReply, the
// ServiceEnvelopeSize rebase for both endian orders, and XCDR request and
// response round trips through the service envelope with no container.
func TestROSPureGoSensorSetCameraInfo(t *testing.T) {
	t.Parallel()
	entry := sensorRestCheckCorpus(t, "SetCameraInfo")
	sensorRestRequireSummary(t, entry, "CameraInfo camera_info", "bool success", "string status_message")
	camEntry, ok := types.Lookup("sensor_msgs", "CameraInfo")
	if !ok {
		t.Fatalf("corpus misses sensor_msgs/CameraInfo, no shape to pin")
	}
	if camEntry.Kind != types.KindMessage {
		t.Fatalf("corpus sensor_msgs/CameraInfo kind %q is not message", camEntry.Kind)
	}
	if !camEntry.HasHeader {
		t.Fatalf("corpus sensor_msgs/CameraInfo HasHeader false, want true")
	}
	sensorRestCheckServiceKind(t, "/set_camera_info")
	guid := sensorRestSvcGUID()
	seq := sensorRestSvcSeq
	t.Run("request", func(t *testing.T) {
		t.Parallel()
		for _, le := range []bool{true, false} {
			enc := sensorRestNewServiceEncoder(t, le, guid, seq)
			err := sensorRestEncodeCameraInfoBody(enc, sensorRestSvcFrame)
			if err != nil {
				t.Fatalf("encode setcamerainfo request: %v", err)
			}
			wire := enc.Bytes()
			if len(wire)%4 != 0 {
				t.Fatalf("setcamerainfo request wire length %d is not padded to 4", len(wire))
			}
			dec := sensorRestNewServiceDecoder(t, wire, guid, seq)
			err = sensorRestDecodeCameraInfoBody(dec, sensorRestSvcFrame, "setcamerainfo request")
			if err != nil {
				t.Fatalf("decode setcamerainfo request: %v", err)
			}
			if dec.Dropped() != 0 {
				t.Fatalf("decoder setcamerainfo request drops = %d, want 0", dec.Dropped())
			}
		}
	})
	t.Run("response", func(t *testing.T) {
		t.Parallel()
		for _, le := range []bool{true, false} {
			enc := sensorRestNewServiceEncoder(t, le, guid, seq+1)
			enc.WriteBool(true)
			err := enc.WriteString("ok")
			if err != nil {
				t.Fatalf("encode setcamerainfo response message: %v", err)
			}
			wire := enc.Bytes()
			if len(wire)%4 != 0 {
				t.Fatalf("setcamerainfo response wire length %d is not padded to 4", len(wire))
			}
			dec := sensorRestNewServiceDecoder(t, wire, guid, seq+1)
			success, err := dec.ReadBool()
			if err != nil {
				t.Fatalf("read setcamerainfo success: %v", err)
			}
			if !success {
				t.Fatalf("read setcamerainfo success = false, want true")
			}
			msg, err := dec.ReadString()
			if err != nil {
				t.Fatalf("read setcamerainfo message: %v", err)
			}
			if msg != "ok" {
				t.Fatalf("read setcamerainfo message = %q, want %q", msg, "ok")
			}
			sensorRestSvcFinish(t, dec, "setcamerainfo response")
		}
	})
	t.Run("service_envelope_rebase", func(t *testing.T) {
		t.Parallel()
		for _, le := range []bool{true, false} {
			enc := sensorRestNewServiceEncoder(t, le, guid, seq+2)
			if enc.Len() != types.ServiceEnvelopeSize {
				t.Fatalf("le=%v encoder len = %d, want %d", le, enc.Len(), types.ServiceEnvelopeSize)
			}
			wire := enc.Bytes()
			dec, err := types.NewDecoder(wire)
			if err != nil {
				t.Fatalf("le=%v new decoder: %v", le, err)
			}
			gotGUID, gotSeq, err := dec.ReadServiceHeader()
			if err != nil {
				t.Fatalf("le=%v read service header: %v", le, err)
			}
			if gotGUID != guid {
				t.Fatalf("le=%v guid = %v, want %v", le, gotGUID, guid)
			}
			if gotSeq != seq+2 {
				t.Fatalf("le=%v seq = %d, want %d", le, gotSeq, seq+2)
			}
			if dec.Offset() != types.ServiceEnvelopeSize {
				t.Fatalf("le=%v offset = %d, want %d", le, dec.Offset(), types.ServiceEnvelopeSize)
			}
		}
	})
}
