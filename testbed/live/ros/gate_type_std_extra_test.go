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

// Fixed sentinels for extra std_msgs primitives.
//
// Types without a string field use a fixed sentinel and rely on per-run
// topic plus domain isolation so concurrent runs never mix. Header carries
// the per-run live ID in frame_id.
const (
	extraUInt8Sentinel   uint8   = 42
	extraInt8Sentinel    int8    = -42
	extraUInt16Sentinel  uint16  = 4242
	extraUInt32Sentinel  uint32  = 305419896
	extraFloat32Sentinel float32 = 42.5
	extraCharSentinel    int8    = 90
	extraByteSentinel    uint8   = 66
)

// Static type hashes from ros2 topic info -v on weld-jazzy.
//
// These are the RIHS01 hashes reported for each ROS type on domain 42.
// Live probing via ros2 topic info -v is preferred at runtime; these values
// are the fallback when the probe cannot run. Values were captured from
// weld-jazzy Jazzy.
const (
	extraHashUInt8   = "RIHS01_6138bd83d8c3569cb80a667db03cfc1629f529fee79d944c39c34e352e72f010"
	extraHashInt8    = "RIHS01_26525065a403d972cb672f0777e333f0c799ad444ae5fcd79e43d1e73bd0f440"
	extraHashUInt16  = "RIHS01_08a406e4b022bc22e907f985d6a9e9dd1d4fbecae573549cf49350113e7757b1"
	extraHashUInt32  = "RIHS01_a5c874829b752bc5fa190024b0ad76f578cc278271e855c7d02a818b3516fb4a"
	extraHashFloat32 = "RIHS01_7170d3d8f841f7be3172ce5f4f59f3a4d7f63b0447e8b33327601ad64d83d6e2"
	extraHashChar    = "RIHS01_3ad2d04dd29ba19d04b16659afa3ccaedd691914b02a64e82e252f2fa6a586a9"
	extraHashByte    = "RIHS01_41e1a3345f73fe93ede006da826a6ee274af23dd4653976ff249b0f44e3e798f"
	extraHashEmpty   = "RIHS01_20b625256f32d5dbc0d04fee44f43c41e51c70d3502f84b4a08e7a9c26a96312"
	extraHashHeader  = "RIHS01_f49fb3ae2cf070f793645ff749683ac6b06203e41c891e17701b1cb597ce6a01"
)

// typeExtraCases returns the extra std_msgs matrix in corpus order.
//
// Order is numeric primitives first, then Char plus Byte, then Empty plus
// Header. String plus Int32 plus Float64 plus Bool are already covered by the
// base matrix, so this set starts at UInt8.
func typeExtraCases() []typeMatrixCase {
	return []typeMatrixCase{
		{
			Label:      "std-uint8",
			RosType:    "std_msgs/msg/UInt8",
			IDLName:    "std_msgs__msg__UInt8",
			staticHash: extraHashUInt8,
			yaml:       yamlExtraUInt8,
			encode:     encodeExtraUInt8,
			decode:     decodeExtraUInt8,
			want:       wantExtraUInt8,
		},
		{
			Label:      "std-int8",
			RosType:    "std_msgs/msg/Int8",
			IDLName:    "std_msgs__msg__Int8",
			staticHash: extraHashInt8,
			yaml:       yamlExtraInt8,
			encode:     encodeExtraInt8,
			decode:     decodeExtraInt8,
			want:       wantExtraInt8,
		},
		{
			Label:      "std-uint16",
			RosType:    "std_msgs/msg/UInt16",
			IDLName:    "std_msgs__msg__UInt16",
			staticHash: extraHashUInt16,
			yaml:       yamlExtraUInt16,
			encode:     encodeExtraUInt16,
			decode:     decodeExtraUInt16,
			want:       wantExtraUInt16,
		},
		{
			Label:      "std-uint32",
			RosType:    "std_msgs/msg/UInt32",
			IDLName:    "std_msgs__msg__UInt32",
			staticHash: extraHashUInt32,
			yaml:       yamlExtraUInt32,
			encode:     encodeExtraUInt32,
			decode:     decodeExtraUInt32,
			want:       wantExtraUInt32,
		},
		{
			Label:      "std-float32",
			RosType:    "std_msgs/msg/Float32",
			IDLName:    "std_msgs__msg__Float32",
			staticHash: extraHashFloat32,
			yaml:       yamlExtraFloat32,
			encode:     encodeExtraFloat32,
			decode:     decodeExtraFloat32,
			want:       wantExtraFloat32,
		},
		{
			Label:      "std-char",
			RosType:    "std_msgs/msg/Char",
			IDLName:    "std_msgs__msg__Char",
			staticHash: extraHashChar,
			yaml:       yamlExtraChar,
			encode:     encodeExtraChar,
			decode:     decodeExtraChar,
			want:       wantExtraChar,
		},
		{
			Label:      "std-byte",
			RosType:    "std_msgs/msg/Byte",
			IDLName:    "std_msgs__msg__Byte",
			staticHash: extraHashByte,
			yaml:       yamlExtraByte,
			encode:     encodeExtraByte,
			decode:     decodeExtraByte,
			want:       wantExtraByte,
		},
		{
			Label:      "std-empty",
			RosType:    "std_msgs/msg/Empty",
			IDLName:    "std_msgs__msg__Empty",
			staticHash: extraHashEmpty,
			yaml:       yamlExtraEmpty,
			encode:     encodeExtraEmpty,
			decode:     decodeExtraEmpty,
			want:       wantExtraEmpty,
		},
		{
			Label:      "std-header",
			RosType:    "std_msgs/msg/Header",
			IDLName:    "std_msgs__msg__Header",
			staticHash: extraHashHeader,
			yaml:       yamlExtraHeader,
			encode:     encodeExtraHeader,
			decode:     decodeExtraHeader,
			want:       wantExtraHeader,
		},
	}
}

// yamlExtraUInt8 returns ROS YAML for the UInt8 sentinel.
func yamlExtraUInt8(live string) string {
	return "{data: 42}"
}

// encodeExtraUInt8 encodes the fixed UInt8 sentinel as XCDR1.
func encodeExtraUInt8(live string) ([]byte, error) {
	enc := types.NewEncoder(true)
	enc.WriteUint8(extraUInt8Sentinel)
	return enc.Bytes(), nil
}

// checkExtraPad accepts only zero pad bytes for single octet types.
func checkExtraPad(dec *types.Decoder, label string) error {
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

// decodeExtraUInt8 decodes and checks the UInt8 sentinel.
//
// A single octet leaves 3 zero pad bytes from Bytes, so Finish would report
// trailing bytes. The loop below accepts only zero pad here.
func decodeExtraUInt8(payload []byte, live string) error {
	dec, err := types.NewDecoder(payload)
	if err != nil {
		return fmt.Errorf("decode uint8 header: %w", err)
	}
	got, err := dec.ReadUint8()
	if err != nil {
		return fmt.Errorf("decode uint8 data: %w", err)
	}
	if got != extraUInt8Sentinel {
		return fmt.Errorf("uint8 data %d wants %d", got, extraUInt8Sentinel)
	}
	err = checkExtraPad(dec, "uint8")
	if err != nil {
		return err
	}
	return nil
}

// wantExtraUInt8 returns the echo substring for the UInt8 sentinel.
func wantExtraUInt8(live string) string {
	return "42"
}

// yamlExtraInt8 returns ROS YAML for the Int8 sentinel.
func yamlExtraInt8(live string) string {
	return "{data: -42}"
}

// encodeExtraInt8 encodes the fixed Int8 sentinel as XCDR1.
func encodeExtraInt8(live string) ([]byte, error) {
	enc := types.NewEncoder(true)
	enc.WriteInt8(extraInt8Sentinel)
	return enc.Bytes(), nil
}

// decodeExtraInt8 decodes and checks the Int8 sentinel.
//
// A single byte leaves 3 zero pad bytes from Bytes, so Finish would report
// trailing bytes. The loop below accepts only zero pad here.
func decodeExtraInt8(payload []byte, live string) error {
	dec, err := types.NewDecoder(payload)
	if err != nil {
		return fmt.Errorf("decode int8 header: %w", err)
	}
	got, err := dec.ReadInt8()
	if err != nil {
		return fmt.Errorf("decode int8 data: %w", err)
	}
	if got != extraInt8Sentinel {
		return fmt.Errorf("int8 data %d wants %d", got, extraInt8Sentinel)
	}
	err = checkExtraPad(dec, "int8")
	if err != nil {
		return err
	}
	return nil
}

// wantExtraInt8 returns the echo substring for the Int8 sentinel.
func wantExtraInt8(live string) string {
	return "-42"
}

// yamlExtraUInt16 returns ROS YAML for the UInt16 sentinel.
func yamlExtraUInt16(live string) string {
	return "{data: 4242}"
}

// encodeExtraUInt16 encodes the fixed UInt16 sentinel as XCDR1.
func encodeExtraUInt16(live string) ([]byte, error) {
	enc := types.NewEncoder(true)
	enc.WriteUint16(extraUInt16Sentinel)
	return enc.Bytes(), nil
}

// decodeExtraUInt16 decodes and checks the UInt16 sentinel.
//
// A single uint16 leaves 2 zero pad bytes from Bytes, so Finish would report
// trailing bytes. The loop below accepts only zero pad here.
func decodeExtraUInt16(payload []byte, live string) error {
	dec, err := types.NewDecoder(payload)
	if err != nil {
		return fmt.Errorf("decode uint16 header: %w", err)
	}
	got, err := dec.ReadUint16()
	if err != nil {
		return fmt.Errorf("decode uint16 data: %w", err)
	}
	if got != extraUInt16Sentinel {
		return fmt.Errorf("uint16 data %d wants %d", got, extraUInt16Sentinel)
	}
	err = checkExtraPad(dec, "uint16")
	if err != nil {
		return err
	}
	return nil
}

// wantExtraUInt16 returns the echo substring for the UInt16 sentinel.
func wantExtraUInt16(live string) string {
	return "4242"
}

// yamlExtraUInt32 returns ROS YAML for the UInt32 sentinel.
func yamlExtraUInt32(live string) string {
	return "{data: 305419896}"
}

// encodeExtraUInt32 encodes the fixed UInt32 sentinel as XCDR1.
func encodeExtraUInt32(live string) ([]byte, error) {
	enc := types.NewEncoder(true)
	enc.WriteUint32(extraUInt32Sentinel)
	return enc.Bytes(), nil
}

// decodeExtraUInt32 decodes and checks the UInt32 sentinel.
func decodeExtraUInt32(payload []byte, live string) error {
	dec, err := types.NewDecoder(payload)
	if err != nil {
		return fmt.Errorf("decode uint32 header: %w", err)
	}
	got, err := dec.ReadUint32()
	if err != nil {
		return fmt.Errorf("decode uint32 data: %w", err)
	}
	if got != extraUInt32Sentinel {
		return fmt.Errorf("uint32 data %d wants %d", got, extraUInt32Sentinel)
	}
	err = dec.Finish()
	if err != nil {
		return fmt.Errorf("finish uint32: %w", err)
	}
	return nil
}

// wantExtraUInt32 returns the echo substring for the UInt32 sentinel.
func wantExtraUInt32(live string) string {
	return "305419896"
}

// yamlExtraFloat32 returns ROS YAML for the Float32 sentinel.
func yamlExtraFloat32(live string) string {
	return "{data: 42.5}"
}

// encodeExtraFloat32 encodes the fixed Float32 sentinel as XCDR1.
func encodeExtraFloat32(live string) ([]byte, error) {
	enc := types.NewEncoder(true)
	enc.WriteFloat32(extraFloat32Sentinel)
	return enc.Bytes(), nil
}

// decodeExtraFloat32 decodes and checks the Float32 sentinel.
func decodeExtraFloat32(payload []byte, live string) error {
	dec, err := types.NewDecoder(payload)
	if err != nil {
		return fmt.Errorf("decode float32 header: %w", err)
	}
	got, err := dec.ReadFloat32()
	if err != nil {
		return fmt.Errorf("decode float32 data: %w", err)
	}
	if got != extraFloat32Sentinel {
		return fmt.Errorf("float32 data %v wants %v", got, extraFloat32Sentinel)
	}
	err = dec.Finish()
	if err != nil {
		return fmt.Errorf("finish float32: %w", err)
	}
	return nil
}

// wantExtraFloat32 returns the echo substring for the Float32 sentinel.
func wantExtraFloat32(live string) string {
	return "42.5"
}

// yamlExtraChar returns ROS YAML for the Char sentinel.
//
// Char uses a numeric YAML form because ros2 topic pub expects an integer
// for the IDL char field. 90 is ASCII Z.
func yamlExtraChar(live string) string {
	return "{data: 90}"
}

// encodeExtraChar encodes the fixed Char sentinel as XCDR1.
func encodeExtraChar(live string) ([]byte, error) {
	enc := types.NewEncoder(true)
	enc.WriteChar(extraCharSentinel)
	return enc.Bytes(), nil
}

// decodeExtraChar decodes and checks the Char sentinel.
//
// A single char leaves 3 zero pad bytes from Bytes, so Finish would report
// trailing bytes. The loop below accepts only zero pad here.
func decodeExtraChar(payload []byte, live string) error {
	dec, err := types.NewDecoder(payload)
	if err != nil {
		return fmt.Errorf("decode char header: %w", err)
	}
	got, err := dec.ReadChar()
	if err != nil {
		return fmt.Errorf("decode char data: %w", err)
	}
	if got != extraCharSentinel {
		return fmt.Errorf("char data %d wants %d", got, extraCharSentinel)
	}
	err = checkExtraPad(dec, "char")
	if err != nil {
		return err
	}
	return nil
}

// wantExtraChar returns the echo substring for the Char sentinel.
func wantExtraChar(live string) string {
	return "90"
}

// yamlExtraByte returns ROS YAML for the Byte sentinel.
//
// Byte uses the bracket list form because ros2 topic pub treats a bare
// integer as a length and a list as byte content. 66 is ASCII B.
func yamlExtraByte(live string) string {
	return "{data: [66]}"
}

// encodeExtraByte encodes the fixed Byte sentinel as XCDR1.
func encodeExtraByte(live string) ([]byte, error) {
	enc := types.NewEncoder(true)
	enc.WriteUint8(extraByteSentinel)
	return enc.Bytes(), nil
}

// decodeExtraByte decodes and checks the Byte sentinel.
//
// A single octet leaves 3 zero pad bytes from Bytes, so Finish would report
// trailing bytes. The loop below accepts only zero pad here.
func decodeExtraByte(payload []byte, live string) error {
	dec, err := types.NewDecoder(payload)
	if err != nil {
		return fmt.Errorf("decode byte header: %w", err)
	}
	got, err := dec.ReadUint8()
	if err != nil {
		return fmt.Errorf("decode byte data: %w", err)
	}
	if got != extraByteSentinel {
		return fmt.Errorf("byte data %d wants %d", got, extraByteSentinel)
	}
	err = checkExtraPad(dec, "byte")
	if err != nil {
		return err
	}
	return nil
}

// wantExtraByte returns the echo substring for the Byte sentinel.
func wantExtraByte(live string) string {
	return "data: B"
}

// yamlExtraEmpty returns ROS YAML for Empty.
func yamlExtraEmpty(live string) string {
	return "{}"
}

// encodeExtraEmpty encodes Empty as XCDR1.
//
// Empty has no fields, so the encoder writes only the header and Bytes
// adds the single dummy zero byte plus pad.
func encodeExtraEmpty(live string) ([]byte, error) {
	enc := types.NewEncoder(true)
	return enc.Bytes(), nil
}

// decodeExtraEmpty decodes and checks Empty wire form.
//
// The payload must hold one zero dummy byte plus zero pad. Any non-zero
// byte reports an error so a corrupt sample never passes.
func decodeExtraEmpty(payload []byte, live string) error {
	dec, err := types.NewDecoder(payload)
	if err != nil {
		return fmt.Errorf("decode empty header: %w", err)
	}
	dummy, err := dec.ReadUint8()
	if err != nil {
		return fmt.Errorf("decode empty dummy: %w", err)
	}
	if dummy != 0 {
		return fmt.Errorf("empty dummy 0x%02x wants zero", dummy)
	}
	for dec.Remaining() > 0 {
		pad, err := dec.ReadUint8()
		if err != nil {
			return fmt.Errorf("decode empty pad: %w", err)
		}
		if pad != 0 {
			return fmt.Errorf("empty pad 0x%02x wants zero", pad)
		}
	}
	return nil
}

// wantExtraEmpty returns the echo substring for Empty.
func wantExtraEmpty(live string) string {
	return "{}"
}

// yamlExtraHeader returns ROS YAML with live frame_id.
func yamlExtraHeader(live string) string {
	return fmt.Sprintf("{stamp: {sec: 0, nanosec: 0}, frame_id: '%s'}", live)
}

// encodeExtraHeader encodes Header with live frame_id.
func encodeExtraHeader(live string) ([]byte, error) {
	enc := types.NewEncoder(true)
	err := encodeMatrixHeader(enc, live)
	if err != nil {
		return nil, fmt.Errorf("encode header header: %w", err)
	}
	return enc.Bytes(), nil
}

// decodeExtraHeader decodes and checks live frame_id.
func decodeExtraHeader(payload []byte, live string) error {
	dec, err := types.NewDecoder(payload)
	if err != nil {
		return fmt.Errorf("decode header header: %w", err)
	}
	frame, err := decodeMatrixHeader(dec)
	if err != nil {
		return fmt.Errorf("decode header frame: %w", err)
	}
	if frame != live {
		return fmt.Errorf("header frame %q wants %q", frame, live)
	}
	err = dec.Finish()
	if err != nil {
		return fmt.Errorf("finish header: %w", err)
	}
	return nil
}

// wantExtraHeader returns the live ID expected in echo output.
func wantExtraHeader(live string) string {
	return live
}

// runExtraLabel runs forward plus reverse for one extra matrix label.
//
// Domain is the allocated exclusive domain shared by both directions.
// It looks up the case in the extra set, skips when the peer lacks the
// type, then runs forward with one live ID and reverse with another so the
// two directions never share a topic. It reuses the base matrix forward
// and reverse paths so hash resolution and backend handling stay identical.
func runExtraLabel(t *testing.T, ctx context.Context, domain int, label string) {
	t.Helper()
	runSharedMatrixLabel(t, ctx, domain, label, typeExtraCases())
}

// TestROSTypeStdUInt8 checks UInt8 forward plus reverse.
//
// Forward starts ros2 topic pub for std_msgs UInt8 and decodes via
// NewDecoder on the Weld take path. Reverse publishes the fixed sentinel
// via Weld and watches ros2 topic echo. The sentinel uses per-run topic
// plus domain isolation because UInt8 carries no string field.
func TestROSTypeStdUInt8(t *testing.T) {
	t.Parallel()
	domain := helpers.MustAllocateDomain(t)
	ctx, cancel := context.WithTimeout(context.Background(), typeMatrixTestTimeout)
	defer cancel()
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)
	runExtraLabel(t, ctx, domain, "std-uint8")
}

// TestROSTypeStdInt8 checks Int8 forward plus reverse.
//
// It mirrors the UInt8 case with the -42 sentinel and the same topic plus
// domain isolation.
func TestROSTypeStdInt8(t *testing.T) {
	t.Parallel()
	domain := helpers.MustAllocateDomain(t)
	ctx, cancel := context.WithTimeout(context.Background(), typeMatrixTestTimeout)
	defer cancel()
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)
	runExtraLabel(t, ctx, domain, "std-int8")
}

// TestROSTypeStdUInt16 checks UInt16 forward plus reverse.
//
// It mirrors the UInt8 case with the 4242 sentinel and the same topic plus
// domain isolation.
func TestROSTypeStdUInt16(t *testing.T) {
	t.Parallel()
	domain := helpers.MustAllocateDomain(t)
	ctx, cancel := context.WithTimeout(context.Background(), typeMatrixTestTimeout)
	defer cancel()
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)
	runExtraLabel(t, ctx, domain, "std-uint16")
}

// TestROSTypeStdUInt32 checks UInt32 forward plus reverse.
//
// It mirrors the UInt8 case with the 305419896 sentinel and the same topic
// plus domain isolation.
func TestROSTypeStdUInt32(t *testing.T) {
	t.Parallel()
	domain := helpers.MustAllocateDomain(t)
	ctx, cancel := context.WithTimeout(context.Background(), typeMatrixTestTimeout)
	defer cancel()
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)
	runExtraLabel(t, ctx, domain, "std-uint32")
}

// TestROSTypeStdFloat32 checks Float32 forward plus reverse.
//
// It mirrors the UInt8 case with the 42.5 sentinel and the same topic plus
// domain isolation.
func TestROSTypeStdFloat32(t *testing.T) {
	t.Parallel()
	domain := helpers.MustAllocateDomain(t)
	ctx, cancel := context.WithTimeout(context.Background(), typeMatrixTestTimeout)
	defer cancel()
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)
	runExtraLabel(t, ctx, domain, "std-float32")
}

// TestROSTypeStdChar checks Char forward plus reverse.
//
// Char YAML uses the numeric form because ros2 topic pub expects an integer
// for the IDL char field. The 90 sentinel uses per-run topic plus domain
// isolation.
func TestROSTypeStdChar(t *testing.T) {
	t.Parallel()
	domain := helpers.MustAllocateDomain(t)
	ctx, cancel := context.WithTimeout(context.Background(), typeMatrixTestTimeout)
	defer cancel()
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)
	runExtraLabel(t, ctx, domain, "std-char")
}

// TestROSTypeStdByte checks Byte forward plus reverse.
//
// Byte YAML uses the bracket list form because ros2 topic pub treats a bare
// integer as a length. The 66 sentinel uses per-run topic plus domain
// isolation.
func TestROSTypeStdByte(t *testing.T) {
	t.Parallel()
	domain := helpers.MustAllocateDomain(t)
	ctx, cancel := context.WithTimeout(context.Background(), typeMatrixTestTimeout)
	defer cancel()
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)
	runExtraLabel(t, ctx, domain, "std-byte")
}

// TestROSTypeStdEmpty checks Empty forward plus reverse.
//
// Empty has no fields, so the local codec check verifies the dummy zero
// byte and the echo check looks for {}. Per-run topics still isolate runs.
func TestROSTypeStdEmpty(t *testing.T) {
	t.Parallel()
	domain := helpers.MustAllocateDomain(t)
	ctx, cancel := context.WithTimeout(context.Background(), typeMatrixTestTimeout)
	defer cancel()
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)
	runExtraLabel(t, ctx, domain, "std-empty")
}

// TestROSTypeStdHeader checks Header forward plus reverse.
//
// Header frame_id carries the per-run live ID so stale runs never match.
// Stamp values stay fixed.
func TestROSTypeStdHeader(t *testing.T) {
	t.Parallel()
	domain := helpers.MustAllocateDomain(t)
	ctx, cancel := context.WithTimeout(context.Background(), typeMatrixTestTimeout)
	defer cancel()
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)
	runExtraLabel(t, ctx, domain, "std-header")
}
