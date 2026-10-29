// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

//nolint:cyclop,dupl // test matrix breadth over thirteen std types
package ros_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/podomy/weld/internal/types"
	"github.com/podomy/weld/testbed/live/helpers"
)

// Fixed sentinels for missing std_msgs primitives.
const (
	stdMultiInt16Sentinel  int16  = -1234
	stdMultiInt64Sentinel  int64  = 1234567890123
	stdMultiUInt64Sentinel uint64 = 1234567890123456789
)

// Fixed layout fields for missing std_msgs multi arrays.
const (
	stdMultiDimSize    uint32 = 2
	stdMultiDimStride  uint32 = 2
	stdMultiDataOffset uint32 = 0
	stdMultiDimSize4   uint32 = 4
	stdMultiDimStride4 uint32 = 4
)

// Fixed sentinels for float multi arrays.
const (
	stdMultiF32A float32 = 1.5
	stdMultiF32B float32 = -2.5
	stdMultiF64A float64 = 42.5
	stdMultiF64B float64 = -42.5
)

// Fixed sentinels for integer multi array elements.
const (
	stdMultiI16A int16  = 1234
	stdMultiI16B int16  = -1234
	stdMultiI32A int32  = 305419896
	stdMultiI32B int32  = -123456
	stdMultiI64A int64  = 1234567890123
	stdMultiI64B int64  = -1234567890123
	stdMultiI8A  int8   = 42
	stdMultiI8B  int8   = -42
	stdMultiI8C  int8   = 7
	stdMultiI8D  int8   = -7
	stdMultiU16A uint16 = 4242
	stdMultiU16B uint16 = 42
	stdMultiU32A uint32 = 305419896
	stdMultiU32B uint32 = 42
	stdMultiU64A uint64 = 1234567890123456789
	stdMultiU64B uint64 = 43
	stdMultiU8A  uint8  = 66
	stdMultiU8B  uint8  = 67
	stdMultiU8C  uint8  = 68
	stdMultiU8D  uint8  = 69
)

// Static type hashes from ros2 topic info -v on weld-jazzy.
const (
	stdMultiHashInt16        = "RIHS01_1dcc3464e47c288a55f943a389d337cdb06804de3f5cd7a266b0de718eee17e5"
	stdMultiHashInt64        = "RIHS01_8cd1048c2f186b6bd9a92472dc1ce51723c0833a221e2b7aecfff111774f4b49"
	stdMultiHashUInt64       = "RIHS01_fbdc52018fc13755dce18024d1a671c856aa8b4aaf63adfb095b608f98e8c943"
	stdMultiHashFloat32Multi = "RIHS01_0599f6f85b4bfca379873a0b4375a0aca022156bd2d7021275d116ed1fa8bfe0"
	stdMultiHashFloat64Multi = "RIHS01_1025ddc6b9552d191f89ef1a8d2f60f3d373e28b283d8891ddcc974e8c55397f"
	stdMultiHashInt16Multi   = "RIHS01_b58810e8e5b90fb19a5062469eb8409f5ab11a446d60de7157a1457e52a076ce"
	stdMultiHashInt32Multi   = "RIHS01_84a7346323525d1b4dfca899df3820f245e54009dac5a6b69217d14fdefd1701"
	stdMultiHashInt64Multi   = "RIHS01_e60f9fe34d697f0939ad49d33158693c1277fbac0e2f04b7c2995dc21c89b422"
	stdMultiHashInt8Multi    = "RIHS01_f21998d4b492abd63330765d75d5831238d400740386f651f13a872a4d2188db"
	stdMultiHashUInt16Multi  = "RIHS01_94fe73428ec63baecc774f8fb82406123e9291cf728f1b7c91caf5335129492b"
	stdMultiHashUInt32Multi  = "RIHS01_6c2577c7ad3cbdcc2164a41c12f1d5ad314ea320f3fb1ee47e78019fe16bb5b0"
	stdMultiHashUInt64Multi  = "RIHS01_fc1c685c2f76bdc6983da025cb25d2db5fb5157b059e300f6d957d86f981b366"
	stdMultiHashUInt8Multi   = "RIHS01_5687e861b8d307a5e48b7515467ae7a5fc2daf805bd0ce6d8e9e604bade9f385"
)

// typeStdMultiCases returns the missing std_msgs matrix in corpus order.
func typeStdMultiCases() []typeMatrixCase {
	return []typeMatrixCase{
		{
			Label:      "std-int16",
			RosType:    "std_msgs/msg/Int16",
			IDLName:    "std_msgs__msg__Int16",
			staticHash: stdMultiHashInt16,
			yaml:       yamlStdMultiInt16,
			encode:     encodeStdMultiInt16,
			decode:     decodeStdMultiInt16,
			want:       wantStdMultiInt16,
		},
		{
			Label:      "std-int64",
			RosType:    "std_msgs/msg/Int64",
			IDLName:    "std_msgs__msg__Int64",
			staticHash: stdMultiHashInt64,
			yaml:       yamlStdMultiInt64,
			encode:     encodeStdMultiInt64,
			decode:     decodeStdMultiInt64,
			want:       wantStdMultiInt64,
		},
		{
			Label:      "std-uint64",
			RosType:    "std_msgs/msg/UInt64",
			IDLName:    "std_msgs__msg__UInt64",
			staticHash: stdMultiHashUInt64,
			yaml:       yamlStdMultiUInt64,
			encode:     encodeStdMultiUInt64,
			decode:     decodeStdMultiUInt64,
			want:       wantStdMultiUInt64,
		},
		{
			Label:      "std-float32multiarray",
			RosType:    "std_msgs/msg/Float32MultiArray",
			IDLName:    "std_msgs__msg__Float32MultiArray",
			staticHash: stdMultiHashFloat32Multi,
			yaml:       yamlStdMultiFloat32Multi,
			encode:     encodeStdMultiFloat32Multi,
			decode:     decodeStdMultiFloat32Multi,
			want:       wantStdMultiLabel,
		},
		{
			Label:      "std-float64multiarray",
			RosType:    "std_msgs/msg/Float64MultiArray",
			IDLName:    "std_msgs__msg__Float64MultiArray",
			staticHash: stdMultiHashFloat64Multi,
			yaml:       yamlStdMultiFloat64Multi,
			encode:     encodeStdMultiFloat64Multi,
			decode:     decodeStdMultiFloat64Multi,
			want:       wantStdMultiLabel,
		},
		{
			Label:      "std-int16multiarray",
			RosType:    "std_msgs/msg/Int16MultiArray",
			IDLName:    "std_msgs__msg__Int16MultiArray",
			staticHash: stdMultiHashInt16Multi,
			yaml:       yamlStdMultiInt16Multi,
			encode:     encodeStdMultiInt16Multi,
			decode:     decodeStdMultiInt16Multi,
			want:       wantStdMultiLabel,
		},
		{
			Label:      "std-int32multiarray",
			RosType:    "std_msgs/msg/Int32MultiArray",
			IDLName:    "std_msgs__msg__Int32MultiArray",
			staticHash: stdMultiHashInt32Multi,
			yaml:       yamlStdMultiInt32Multi,
			encode:     encodeStdMultiInt32Multi,
			decode:     decodeStdMultiInt32Multi,
			want:       wantStdMultiLabel,
		},
		{
			Label:      "std-int64multiarray",
			RosType:    "std_msgs/msg/Int64MultiArray",
			IDLName:    "std_msgs__msg__Int64MultiArray",
			staticHash: stdMultiHashInt64Multi,
			yaml:       yamlStdMultiInt64Multi,
			encode:     encodeStdMultiInt64Multi,
			decode:     decodeStdMultiInt64Multi,
			want:       wantStdMultiLabel,
		},
		{
			Label:      "std-int8multiarray",
			RosType:    "std_msgs/msg/Int8MultiArray",
			IDLName:    "std_msgs__msg__Int8MultiArray",
			staticHash: stdMultiHashInt8Multi,
			yaml:       yamlStdMultiInt8Multi,
			encode:     encodeStdMultiInt8Multi,
			decode:     decodeStdMultiInt8Multi,
			want:       wantStdMultiLabel,
		},
		{
			Label:      "std-uint16multiarray",
			RosType:    "std_msgs/msg/UInt16MultiArray",
			IDLName:    "std_msgs__msg__UInt16MultiArray",
			staticHash: stdMultiHashUInt16Multi,
			yaml:       yamlStdMultiUInt16Multi,
			encode:     encodeStdMultiUInt16Multi,
			decode:     decodeStdMultiUInt16Multi,
			want:       wantStdMultiLabel,
		},
		{
			Label:      "std-uint32multiarray",
			RosType:    "std_msgs/msg/UInt32MultiArray",
			IDLName:    "std_msgs__msg__UInt32MultiArray",
			staticHash: stdMultiHashUInt32Multi,
			yaml:       yamlStdMultiUInt32Multi,
			encode:     encodeStdMultiUInt32Multi,
			decode:     decodeStdMultiUInt32Multi,
			want:       wantStdMultiLabel,
		},
		{
			Label:      "std-uint64multiarray",
			RosType:    "std_msgs/msg/UInt64MultiArray",
			IDLName:    "std_msgs__msg__UInt64MultiArray",
			staticHash: stdMultiHashUInt64Multi,
			yaml:       yamlStdMultiUInt64Multi,
			encode:     encodeStdMultiUInt64Multi,
			decode:     decodeStdMultiUInt64Multi,
			want:       wantStdMultiLabel,
		},
		{
			Label:      "std-uint8multiarray",
			RosType:    "std_msgs/msg/UInt8MultiArray",
			IDLName:    "std_msgs__msg__UInt8MultiArray",
			staticHash: stdMultiHashUInt8Multi,
			yaml:       yamlStdMultiUInt8Multi,
			encode:     encodeStdMultiUInt8Multi,
			decode:     decodeStdMultiUInt8Multi,
			want:       wantStdMultiLabel,
		},
	}
}

// checkStdMultiPad accepts only zero pad bytes.
func checkStdMultiPad(dec *types.Decoder, label string) error {
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

// encodeStdMultiLayout writes dim sequence of one plus label plus sizes plus offset.
func encodeStdMultiLayout(enc *types.Encoder, live string, size, stride uint32) error {
	err := enc.WriteSequenceLength(1)
	if err != nil {
		return fmt.Errorf("encode layout dim len: %w", err)
	}
	err = enc.WriteString(live)
	if err != nil {
		return fmt.Errorf("encode layout label: %w", err)
	}
	enc.WriteUint32(size)
	enc.WriteUint32(stride)
	enc.WriteUint32(stdMultiDataOffset)
	return nil
}

// decodeStdMultiLayout reads dim sequence of one plus label plus sizes plus offset.
func decodeStdMultiLayout(dec *types.Decoder, live, label string, size, stride uint32) error {
	n, err := dec.ReadSequenceLength()
	if err != nil {
		return fmt.Errorf("decode %s dim len: %w", label, err)
	}
	if n != 1 {
		return fmt.Errorf("%s dim len %d wants 1", label, n)
	}
	gotLabel, err := dec.ReadString()
	if err != nil {
		return fmt.Errorf("decode %s label: %w", label, err)
	}
	if gotLabel != live {
		return fmt.Errorf("%s label %q wants %q", label, gotLabel, live)
	}
	gotSize, err := dec.ReadUint32()
	if err != nil {
		return fmt.Errorf("decode %s size: %w", label, err)
	}
	if gotSize != size {
		return fmt.Errorf("%s size %d wants %d", label, gotSize, size)
	}
	gotStride, err := dec.ReadUint32()
	if err != nil {
		return fmt.Errorf("decode %s stride: %w", label, err)
	}
	if gotStride != stride {
		return fmt.Errorf("%s stride %d wants %d", label, gotStride, stride)
	}
	gotOffset, err := dec.ReadUint32()
	if err != nil {
		return fmt.Errorf("decode %s offset: %w", label, err)
	}
	if gotOffset != stdMultiDataOffset {
		return fmt.Errorf("%s offset %d wants %d", label, gotOffset, stdMultiDataOffset)
	}
	return nil
}

// wantStdMultiLabel returns the live ID expected in echo output.
func wantStdMultiLabel(live string) string {
	return live
}

// yamlStdMultiInt16 returns ROS YAML for the Int16 sentinel.
func yamlStdMultiInt16(live string) string {
	return "{data: -1234}"
}

// encodeStdMultiInt16 encodes the Int16 sentinel as XCDR1.
func encodeStdMultiInt16(live string) ([]byte, error) {
	enc := types.NewEncoder(true)
	enc.WriteInt16(stdMultiInt16Sentinel)
	return enc.Bytes(), nil
}

// decodeStdMultiInt16 decodes and checks the Int16 sentinel.
func decodeStdMultiInt16(payload []byte, live string) error {
	dec, err := types.NewDecoder(payload)
	if err != nil {
		return fmt.Errorf("decode int16 header: %w", err)
	}
	got, err := dec.ReadInt16()
	if err != nil {
		return fmt.Errorf("decode int16 data: %w", err)
	}
	if got != stdMultiInt16Sentinel {
		return fmt.Errorf("int16 data %d wants %d", got, stdMultiInt16Sentinel)
	}
	err = checkStdMultiPad(dec, "int16")
	if err != nil {
		return err
	}
	return nil
}

// wantStdMultiInt16 returns the echo substring for the Int16 sentinel.
func wantStdMultiInt16(live string) string {
	return "-1234"
}

// yamlStdMultiInt64 returns ROS YAML for the Int64 sentinel.
func yamlStdMultiInt64(live string) string {
	return "{data: 1234567890123}"
}

// encodeStdMultiInt64 encodes the Int64 sentinel as XCDR1.
func encodeStdMultiInt64(live string) ([]byte, error) {
	enc := types.NewEncoder(true)
	enc.WriteInt64(stdMultiInt64Sentinel)
	return enc.Bytes(), nil
}

// decodeStdMultiInt64 decodes and checks the Int64 sentinel.
func decodeStdMultiInt64(payload []byte, live string) error {
	dec, err := types.NewDecoder(payload)
	if err != nil {
		return fmt.Errorf("decode int64 header: %w", err)
	}
	got, err := dec.ReadInt64()
	if err != nil {
		return fmt.Errorf("decode int64 data: %w", err)
	}
	if got != stdMultiInt64Sentinel {
		return fmt.Errorf("int64 data %d wants %d", got, stdMultiInt64Sentinel)
	}
	err = dec.Finish()
	if err != nil {
		return fmt.Errorf("finish int64: %w", err)
	}
	return nil
}

// wantStdMultiInt64 returns the echo substring for the Int64 sentinel.
func wantStdMultiInt64(live string) string {
	return "1234567890123"
}

// yamlStdMultiUInt64 returns ROS YAML for the UInt64 sentinel.
func yamlStdMultiUInt64(live string) string {
	return "{data: 1234567890123456789}"
}

// encodeStdMultiUInt64 encodes the UInt64 sentinel as XCDR1.
func encodeStdMultiUInt64(live string) ([]byte, error) {
	enc := types.NewEncoder(true)
	enc.WriteUint64(stdMultiUInt64Sentinel)
	return enc.Bytes(), nil
}

// decodeStdMultiUInt64 decodes and checks the UInt64 sentinel.
func decodeStdMultiUInt64(payload []byte, live string) error {
	dec, err := types.NewDecoder(payload)
	if err != nil {
		return fmt.Errorf("decode uint64 header: %w", err)
	}
	got, err := dec.ReadUint64()
	if err != nil {
		return fmt.Errorf("decode uint64 data: %w", err)
	}
	if got != stdMultiUInt64Sentinel {
		return fmt.Errorf("uint64 data %d wants %d", got, stdMultiUInt64Sentinel)
	}
	err = dec.Finish()
	if err != nil {
		return fmt.Errorf("finish uint64: %w", err)
	}
	return nil
}

// wantStdMultiUInt64 returns the echo substring for the UInt64 sentinel.
func wantStdMultiUInt64(live string) string {
	return "1234567890123456789"
}

// yamlStdMultiFloat32Multi returns ROS YAML for Float32MultiArray.
func yamlStdMultiFloat32Multi(live string) string {
	return fmt.Sprintf("{layout: {dim: [{label: '%s', size: 2, stride: 2}], data_offset: 0}, data: [1.5, -2.5]}", live)
}

// encodeStdMultiFloat32Multi encodes Float32MultiArray with live label.
func encodeStdMultiFloat32Multi(live string) ([]byte, error) {
	enc := types.NewEncoder(true)
	err := encodeStdMultiLayout(enc, live, stdMultiDimSize, stdMultiDimStride)
	if err != nil {
		return nil, err
	}
	err = enc.WriteSequenceLength(2)
	if err != nil {
		return nil, fmt.Errorf("encode float32 array len: %w", err)
	}
	enc.WriteFloat32(stdMultiF32A)
	enc.WriteFloat32(stdMultiF32B)
	return enc.Bytes(), nil
}

// decodeStdMultiFloat32Multi decodes and checks Float32MultiArray.
func decodeStdMultiFloat32Multi(payload []byte, live string) error {
	dec, err := types.NewDecoder(payload)
	if err != nil {
		return fmt.Errorf("decode float32 array header: %w", err)
	}
	err = decodeStdMultiLayout(dec, live, "float32 array", stdMultiDimSize, stdMultiDimStride)
	if err != nil {
		return err
	}
	n, err := dec.ReadSequenceLength()
	if err != nil {
		return fmt.Errorf("decode float32 array len: %w", err)
	}
	if n != 2 {
		return fmt.Errorf("float32 array len %d wants 2", n)
	}
	a, err := dec.ReadFloat32()
	if err != nil {
		return fmt.Errorf("decode float32 array a: %w", err)
	}
	b, err := dec.ReadFloat32()
	if err != nil {
		return fmt.Errorf("decode float32 array b: %w", err)
	}
	if a != stdMultiF32A || b != stdMultiF32B {
		return fmt.Errorf("float32 array (%v, %v) wants (1.5, -2.5)", a, b)
	}
	err = dec.Finish()
	if err != nil {
		return fmt.Errorf("finish float32 array: %w", err)
	}
	return nil
}

// yamlStdMultiFloat64Multi returns ROS YAML for Float64MultiArray.
func yamlStdMultiFloat64Multi(live string) string {
	return fmt.Sprintf("{layout: {dim: [{label: '%s', size: 2, stride: 2}], data_offset: 0}, data: [42.5, -42.5]}", live)
}

// encodeStdMultiFloat64Multi encodes Float64MultiArray with live label.
func encodeStdMultiFloat64Multi(live string) ([]byte, error) {
	enc := types.NewEncoder(true)
	err := encodeStdMultiLayout(enc, live, stdMultiDimSize, stdMultiDimStride)
	if err != nil {
		return nil, err
	}
	err = enc.WriteSequenceLength(2)
	if err != nil {
		return nil, fmt.Errorf("encode float64 array len: %w", err)
	}
	enc.WriteFloat64(stdMultiF64A)
	enc.WriteFloat64(stdMultiF64B)
	return enc.Bytes(), nil
}

// decodeStdMultiFloat64Multi decodes and checks Float64MultiArray.
func decodeStdMultiFloat64Multi(payload []byte, live string) error {
	dec, err := types.NewDecoder(payload)
	if err != nil {
		return fmt.Errorf("decode float64 array header: %w", err)
	}
	err = decodeStdMultiLayout(dec, live, "float64 array", stdMultiDimSize, stdMultiDimStride)
	if err != nil {
		return err
	}
	n, err := dec.ReadSequenceLength()
	if err != nil {
		return fmt.Errorf("decode float64 array len: %w", err)
	}
	if n != 2 {
		return fmt.Errorf("float64 array len %d wants 2", n)
	}
	a, err := dec.ReadFloat64()
	if err != nil {
		return fmt.Errorf("decode float64 array a: %w", err)
	}
	b, err := dec.ReadFloat64()
	if err != nil {
		return fmt.Errorf("decode float64 array b: %w", err)
	}
	if a != stdMultiF64A || b != stdMultiF64B {
		return fmt.Errorf("float64 array (%v, %v) wants (42.5, -42.5)", a, b)
	}
	err = dec.Finish()
	if err != nil {
		return fmt.Errorf("finish float64 array: %w", err)
	}
	return nil
}

// yamlStdMultiInt16Multi returns ROS YAML for Int16MultiArray.
func yamlStdMultiInt16Multi(live string) string {
	return fmt.Sprintf("{layout: {dim: [{label: '%s', size: 2, stride: 2}], data_offset: 0}, data: [1234, -1234]}", live)
}

// encodeStdMultiInt16Multi encodes Int16MultiArray with live label.
func encodeStdMultiInt16Multi(live string) ([]byte, error) {
	enc := types.NewEncoder(true)
	err := encodeStdMultiLayout(enc, live, stdMultiDimSize, stdMultiDimStride)
	if err != nil {
		return nil, err
	}
	err = enc.WriteSequenceLength(2)
	if err != nil {
		return nil, fmt.Errorf("encode int16 array len: %w", err)
	}
	enc.WriteInt16(stdMultiI16A)
	enc.WriteInt16(stdMultiI16B)
	return enc.Bytes(), nil
}

// decodeStdMultiInt16Multi decodes and checks Int16MultiArray.
func decodeStdMultiInt16Multi(payload []byte, live string) error {
	dec, err := types.NewDecoder(payload)
	if err != nil {
		return fmt.Errorf("decode int16 array header: %w", err)
	}
	err = decodeStdMultiLayout(dec, live, "int16 array", stdMultiDimSize, stdMultiDimStride)
	if err != nil {
		return err
	}
	n, err := dec.ReadSequenceLength()
	if err != nil {
		return fmt.Errorf("decode int16 array len: %w", err)
	}
	if n != 2 {
		return fmt.Errorf("int16 array len %d wants 2", n)
	}
	a, err := dec.ReadInt16()
	if err != nil {
		return fmt.Errorf("decode int16 array a: %w", err)
	}
	b, err := dec.ReadInt16()
	if err != nil {
		return fmt.Errorf("decode int16 array b: %w", err)
	}
	if a != stdMultiI16A || b != stdMultiI16B {
		return fmt.Errorf("int16 array (%d, %d) wants (1234, -1234)", a, b)
	}
	err = dec.Finish()
	if err != nil {
		return fmt.Errorf("finish int16 array: %w", err)
	}
	return nil
}

// yamlStdMultiInt32Multi returns ROS YAML for Int32MultiArray.
func yamlStdMultiInt32Multi(live string) string {
	return fmt.Sprintf("{layout: {dim: [{label: '%s', size: 2, stride: 2}], data_offset: 0}, data: [305419896, -123456]}", live)
}

// encodeStdMultiInt32Multi encodes Int32MultiArray with live label.
func encodeStdMultiInt32Multi(live string) ([]byte, error) {
	enc := types.NewEncoder(true)
	err := encodeStdMultiLayout(enc, live, stdMultiDimSize, stdMultiDimStride)
	if err != nil {
		return nil, err
	}
	err = enc.WriteSequenceLength(2)
	if err != nil {
		return nil, fmt.Errorf("encode int32 array len: %w", err)
	}
	enc.WriteInt32(stdMultiI32A)
	enc.WriteInt32(stdMultiI32B)
	return enc.Bytes(), nil
}

// decodeStdMultiInt32Multi decodes and checks Int32MultiArray.
func decodeStdMultiInt32Multi(payload []byte, live string) error {
	dec, err := types.NewDecoder(payload)
	if err != nil {
		return fmt.Errorf("decode int32 array header: %w", err)
	}
	err = decodeStdMultiLayout(dec, live, "int32 array", stdMultiDimSize, stdMultiDimStride)
	if err != nil {
		return err
	}
	n, err := dec.ReadSequenceLength()
	if err != nil {
		return fmt.Errorf("decode int32 array len: %w", err)
	}
	if n != 2 {
		return fmt.Errorf("int32 array len %d wants 2", n)
	}
	a, err := dec.ReadInt32()
	if err != nil {
		return fmt.Errorf("decode int32 array a: %w", err)
	}
	b, err := dec.ReadInt32()
	if err != nil {
		return fmt.Errorf("decode int32 array b: %w", err)
	}
	if a != stdMultiI32A || b != stdMultiI32B {
		return fmt.Errorf("int32 array (%d, %d) wants (305419896, -123456)", a, b)
	}
	err = dec.Finish()
	if err != nil {
		return fmt.Errorf("finish int32 array: %w", err)
	}
	return nil
}

// yamlStdMultiInt64Multi returns ROS YAML for Int64MultiArray.
func yamlStdMultiInt64Multi(live string) string {
	return fmt.Sprintf("{layout: {dim: [{label: '%s', size: 2, stride: 2}], data_offset: 0}, data: [1234567890123, -1234567890123]}", live)
}

// encodeStdMultiInt64Multi encodes Int64MultiArray with live label.
func encodeStdMultiInt64Multi(live string) ([]byte, error) {
	enc := types.NewEncoder(true)
	err := encodeStdMultiLayout(enc, live, stdMultiDimSize, stdMultiDimStride)
	if err != nil {
		return nil, err
	}
	err = enc.WriteSequenceLength(2)
	if err != nil {
		return nil, fmt.Errorf("encode int64 array len: %w", err)
	}
	enc.WriteInt64(stdMultiI64A)
	enc.WriteInt64(stdMultiI64B)
	return enc.Bytes(), nil
}

// decodeStdMultiInt64Multi decodes and checks Int64MultiArray.
func decodeStdMultiInt64Multi(payload []byte, live string) error {
	dec, err := types.NewDecoder(payload)
	if err != nil {
		return fmt.Errorf("decode int64 array header: %w", err)
	}
	err = decodeStdMultiLayout(dec, live, "int64 array", stdMultiDimSize, stdMultiDimStride)
	if err != nil {
		return err
	}
	n, err := dec.ReadSequenceLength()
	if err != nil {
		return fmt.Errorf("decode int64 array len: %w", err)
	}
	if n != 2 {
		return fmt.Errorf("int64 array len %d wants 2", n)
	}
	a, err := dec.ReadInt64()
	if err != nil {
		return fmt.Errorf("decode int64 array a: %w", err)
	}
	b, err := dec.ReadInt64()
	if err != nil {
		return fmt.Errorf("decode int64 array b: %w", err)
	}
	if a != stdMultiI64A || b != stdMultiI64B {
		return fmt.Errorf("int64 array (%d, %d) wants (1234567890123, -1234567890123)", a, b)
	}
	err = dec.Finish()
	if err != nil {
		return fmt.Errorf("finish int64 array: %w", err)
	}
	return nil
}

// yamlStdMultiInt8Multi returns ROS YAML for Int8MultiArray.
func yamlStdMultiInt8Multi(live string) string {
	return fmt.Sprintf("{layout: {dim: [{label: '%s', size: 4, stride: 4}], data_offset: 0}, data: [42, -42, 7, -7]}", live)
}

// encodeStdMultiInt8Multi encodes Int8MultiArray with live label.
func encodeStdMultiInt8Multi(live string) ([]byte, error) {
	enc := types.NewEncoder(true)
	err := encodeStdMultiLayout(enc, live, stdMultiDimSize4, stdMultiDimStride4)
	if err != nil {
		return nil, err
	}
	err = enc.WriteSequenceLength(4)
	if err != nil {
		return nil, fmt.Errorf("encode int8 array len: %w", err)
	}
	enc.WriteInt8(stdMultiI8A)
	enc.WriteInt8(stdMultiI8B)
	enc.WriteInt8(stdMultiI8C)
	enc.WriteInt8(stdMultiI8D)
	return enc.Bytes(), nil
}

// decodeStdMultiInt8Multi decodes and checks Int8MultiArray.
func decodeStdMultiInt8Multi(payload []byte, live string) error {
	dec, err := types.NewDecoder(payload)
	if err != nil {
		return fmt.Errorf("decode int8 array header: %w", err)
	}
	err = decodeStdMultiLayout(dec, live, "int8 array", stdMultiDimSize4, stdMultiDimStride4)
	if err != nil {
		return err
	}
	n, err := dec.ReadSequenceLength()
	if err != nil {
		return fmt.Errorf("decode int8 array len: %w", err)
	}
	if n != 4 {
		return fmt.Errorf("int8 array len %d wants 4", n)
	}
	a, err := dec.ReadInt8()
	if err != nil {
		return fmt.Errorf("decode int8 array a: %w", err)
	}
	b, err := dec.ReadInt8()
	if err != nil {
		return fmt.Errorf("decode int8 array b: %w", err)
	}
	c, err := dec.ReadInt8()
	if err != nil {
		return fmt.Errorf("decode int8 array c: %w", err)
	}
	d, err := dec.ReadInt8()
	if err != nil {
		return fmt.Errorf("decode int8 array d: %w", err)
	}
	if a != stdMultiI8A || b != stdMultiI8B || c != stdMultiI8C || d != stdMultiI8D {
		return fmt.Errorf("int8 array (%d, %d, %d, %d) wants (42, -42, 7, -7)", a, b, c, d)
	}
	err = dec.Finish()
	if err != nil {
		return fmt.Errorf("finish int8 array: %w", err)
	}
	return nil
}

// yamlStdMultiUInt16Multi returns ROS YAML for UInt16MultiArray.
func yamlStdMultiUInt16Multi(live string) string {
	return fmt.Sprintf("{layout: {dim: [{label: '%s', size: 2, stride: 2}], data_offset: 0}, data: [4242, 42]}", live)
}

// encodeStdMultiUInt16Multi encodes UInt16MultiArray with live label.
func encodeStdMultiUInt16Multi(live string) ([]byte, error) {
	enc := types.NewEncoder(true)
	err := encodeStdMultiLayout(enc, live, stdMultiDimSize, stdMultiDimStride)
	if err != nil {
		return nil, err
	}
	err = enc.WriteSequenceLength(2)
	if err != nil {
		return nil, fmt.Errorf("encode uint16 array len: %w", err)
	}
	enc.WriteUint16(stdMultiU16A)
	enc.WriteUint16(stdMultiU16B)
	return enc.Bytes(), nil
}

// decodeStdMultiUInt16Multi decodes and checks UInt16MultiArray.
func decodeStdMultiUInt16Multi(payload []byte, live string) error {
	dec, err := types.NewDecoder(payload)
	if err != nil {
		return fmt.Errorf("decode uint16 array header: %w", err)
	}
	err = decodeStdMultiLayout(dec, live, "uint16 array", stdMultiDimSize, stdMultiDimStride)
	if err != nil {
		return err
	}
	n, err := dec.ReadSequenceLength()
	if err != nil {
		return fmt.Errorf("decode uint16 array len: %w", err)
	}
	if n != 2 {
		return fmt.Errorf("uint16 array len %d wants 2", n)
	}
	a, err := dec.ReadUint16()
	if err != nil {
		return fmt.Errorf("decode uint16 array a: %w", err)
	}
	b, err := dec.ReadUint16()
	if err != nil {
		return fmt.Errorf("decode uint16 array b: %w", err)
	}
	if a != stdMultiU16A || b != stdMultiU16B {
		return fmt.Errorf("uint16 array (%d, %d) wants (4242, 42)", a, b)
	}
	err = dec.Finish()
	if err != nil {
		return fmt.Errorf("finish uint16 array: %w", err)
	}
	return nil
}

// yamlStdMultiUInt32Multi returns ROS YAML for UInt32MultiArray.
func yamlStdMultiUInt32Multi(live string) string {
	return fmt.Sprintf("{layout: {dim: [{label: '%s', size: 2, stride: 2}], data_offset: 0}, data: [305419896, 42]}", live)
}

// encodeStdMultiUInt32Multi encodes UInt32MultiArray with live label.
func encodeStdMultiUInt32Multi(live string) ([]byte, error) {
	enc := types.NewEncoder(true)
	err := encodeStdMultiLayout(enc, live, stdMultiDimSize, stdMultiDimStride)
	if err != nil {
		return nil, err
	}
	err = enc.WriteSequenceLength(2)
	if err != nil {
		return nil, fmt.Errorf("encode uint32 array len: %w", err)
	}
	enc.WriteUint32(stdMultiU32A)
	enc.WriteUint32(stdMultiU32B)
	return enc.Bytes(), nil
}

// decodeStdMultiUInt32Multi decodes and checks UInt32MultiArray.
func decodeStdMultiUInt32Multi(payload []byte, live string) error {
	dec, err := types.NewDecoder(payload)
	if err != nil {
		return fmt.Errorf("decode uint32 array header: %w", err)
	}
	err = decodeStdMultiLayout(dec, live, "uint32 array", stdMultiDimSize, stdMultiDimStride)
	if err != nil {
		return err
	}
	n, err := dec.ReadSequenceLength()
	if err != nil {
		return fmt.Errorf("decode uint32 array len: %w", err)
	}
	if n != 2 {
		return fmt.Errorf("uint32 array len %d wants 2", n)
	}
	a, err := dec.ReadUint32()
	if err != nil {
		return fmt.Errorf("decode uint32 array a: %w", err)
	}
	b, err := dec.ReadUint32()
	if err != nil {
		return fmt.Errorf("decode uint32 array b: %w", err)
	}
	if a != stdMultiU32A || b != stdMultiU32B {
		return fmt.Errorf("uint32 array (%d, %d) wants (305419896, 42)", a, b)
	}
	err = dec.Finish()
	if err != nil {
		return fmt.Errorf("finish uint32 array: %w", err)
	}
	return nil
}

// yamlStdMultiUInt64Multi returns ROS YAML for UInt64MultiArray.
func yamlStdMultiUInt64Multi(live string) string {
	return fmt.Sprintf("{layout: {dim: [{label: '%s', size: 2, stride: 2}], data_offset: 0}, data: [1234567890123456789, 43]}", live)
}

// encodeStdMultiUInt64Multi encodes UInt64MultiArray with live label.
func encodeStdMultiUInt64Multi(live string) ([]byte, error) {
	enc := types.NewEncoder(true)
	err := encodeStdMultiLayout(enc, live, stdMultiDimSize, stdMultiDimStride)
	if err != nil {
		return nil, err
	}
	err = enc.WriteSequenceLength(2)
	if err != nil {
		return nil, fmt.Errorf("encode uint64 array len: %w", err)
	}
	enc.WriteUint64(stdMultiU64A)
	enc.WriteUint64(stdMultiU64B)
	return enc.Bytes(), nil
}

// decodeStdMultiUInt64Multi decodes and checks UInt64MultiArray.
func decodeStdMultiUInt64Multi(payload []byte, live string) error {
	dec, err := types.NewDecoder(payload)
	if err != nil {
		return fmt.Errorf("decode uint64 array header: %w", err)
	}
	err = decodeStdMultiLayout(dec, live, "uint64 array", stdMultiDimSize, stdMultiDimStride)
	if err != nil {
		return err
	}
	n, err := dec.ReadSequenceLength()
	if err != nil {
		return fmt.Errorf("decode uint64 array len: %w", err)
	}
	if n != 2 {
		return fmt.Errorf("uint64 array len %d wants 2", n)
	}
	a, err := dec.ReadUint64()
	if err != nil {
		return fmt.Errorf("decode uint64 array a: %w", err)
	}
	b, err := dec.ReadUint64()
	if err != nil {
		return fmt.Errorf("decode uint64 array b: %w", err)
	}
	if a != stdMultiU64A || b != stdMultiU64B {
		return fmt.Errorf("uint64 array (%d, %d) wants (1234567890123456789, 43)", a, b)
	}
	err = dec.Finish()
	if err != nil {
		return fmt.Errorf("finish uint64 array: %w", err)
	}
	return nil
}

// yamlStdMultiUInt8Multi returns ROS YAML for UInt8MultiArray.
func yamlStdMultiUInt8Multi(live string) string {
	return fmt.Sprintf("{layout: {dim: [{label: '%s', size: 4, stride: 4}], data_offset: 0}, data: [66, 67, 68, 69]}", live)
}

// encodeStdMultiUInt8Multi encodes UInt8MultiArray with live label.
func encodeStdMultiUInt8Multi(live string) ([]byte, error) {
	enc := types.NewEncoder(true)
	err := encodeStdMultiLayout(enc, live, stdMultiDimSize4, stdMultiDimStride4)
	if err != nil {
		return nil, err
	}
	err = enc.WriteOctetSequence([]byte{stdMultiU8A, stdMultiU8B, stdMultiU8C, stdMultiU8D})
	if err != nil {
		return nil, fmt.Errorf("encode uint8 array data: %w", err)
	}
	return enc.Bytes(), nil
}

// decodeStdMultiUInt8Multi decodes and checks UInt8MultiArray.
func decodeStdMultiUInt8Multi(payload []byte, live string) error {
	dec, err := types.NewDecoder(payload)
	if err != nil {
		return fmt.Errorf("decode uint8 array header: %w", err)
	}
	err = decodeStdMultiLayout(dec, live, "uint8 array", stdMultiDimSize4, stdMultiDimStride4)
	if err != nil {
		return err
	}
	data, err := dec.ReadOctetSequence()
	if err != nil {
		return fmt.Errorf("decode uint8 array data: %w", err)
	}
	want := []byte{stdMultiU8A, stdMultiU8B, stdMultiU8C, stdMultiU8D}
	if len(data) != len(want) {
		return fmt.Errorf("uint8 array len %d wants %d", len(data), len(want))
	}
	for i := range data {
		if data[i] != want[i] {
			return fmt.Errorf("uint8 array byte %d 0x%02x wants 0x%02x", i, data[i], want[i])
		}
	}
	err = dec.Finish()
	if err != nil {
		return fmt.Errorf("finish uint8 array: %w", err)
	}
	return nil
}

// runStdMultiLabel runs forward plus reverse for one std multi label.
//
// Domain is the allocated exclusive domain shared by both directions.
func runStdMultiLabel(t *testing.T, ctx context.Context, domain int, label string) {
	t.Helper()
	runSharedMatrixLabel(t, ctx, domain, label, typeStdMultiCases())
}

// TestROSTypeStdInt16 checks Int16 forward plus reverse.
func TestROSTypeStdInt16(t *testing.T) {
	t.Parallel()
	domain := helpers.MustAllocateDomain(t)
	ctx, cancel := context.WithTimeout(context.Background(), typeMatrixTestTimeout)
	defer cancel()
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)
	runStdMultiLabel(t, ctx, domain, "std-int16")
}

// TestROSTypeStdInt64 checks Int64 forward plus reverse.
func TestROSTypeStdInt64(t *testing.T) {
	t.Parallel()
	domain := helpers.MustAllocateDomain(t)
	ctx, cancel := context.WithTimeout(context.Background(), typeMatrixTestTimeout)
	defer cancel()
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)
	runStdMultiLabel(t, ctx, domain, "std-int64")
}

// TestROSTypeStdUInt64 checks UInt64 forward plus reverse.
func TestROSTypeStdUInt64(t *testing.T) {
	t.Parallel()
	domain := helpers.MustAllocateDomain(t)
	ctx, cancel := context.WithTimeout(context.Background(), typeMatrixTestTimeout)
	defer cancel()
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)
	runStdMultiLabel(t, ctx, domain, "std-uint64")
}

// TestROSTypeStdFloat32MultiArray checks Float32MultiArray forward plus reverse.
func TestROSTypeStdFloat32MultiArray(t *testing.T) {
	t.Parallel()
	domain := helpers.MustAllocateDomain(t)
	ctx, cancel := context.WithTimeout(context.Background(), typeMatrixTestTimeout)
	defer cancel()
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)
	runStdMultiLabel(t, ctx, domain, "std-float32multiarray")
}

// TestROSTypeStdFloat64MultiArray checks Float64MultiArray forward plus reverse.
func TestROSTypeStdFloat64MultiArray(t *testing.T) {
	t.Parallel()
	domain := helpers.MustAllocateDomain(t)
	ctx, cancel := context.WithTimeout(context.Background(), typeMatrixTestTimeout)
	defer cancel()
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)
	runStdMultiLabel(t, ctx, domain, "std-float64multiarray")
}

// TestROSTypeStdInt16MultiArray checks Int16MultiArray forward plus reverse.
func TestROSTypeStdInt16MultiArray(t *testing.T) {
	t.Parallel()
	domain := helpers.MustAllocateDomain(t)
	ctx, cancel := context.WithTimeout(context.Background(), typeMatrixTestTimeout)
	defer cancel()
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)
	runStdMultiLabel(t, ctx, domain, "std-int16multiarray")
}

// TestROSTypeStdInt32MultiArray checks Int32MultiArray forward plus reverse.
func TestROSTypeStdInt32MultiArray(t *testing.T) {
	t.Parallel()
	domain := helpers.MustAllocateDomain(t)
	ctx, cancel := context.WithTimeout(context.Background(), typeMatrixTestTimeout)
	defer cancel()
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)
	runStdMultiLabel(t, ctx, domain, "std-int32multiarray")
}

// TestROSTypeStdInt64MultiArray checks Int64MultiArray forward plus reverse.
func TestROSTypeStdInt64MultiArray(t *testing.T) {
	t.Parallel()
	domain := helpers.MustAllocateDomain(t)
	ctx, cancel := context.WithTimeout(context.Background(), typeMatrixTestTimeout)
	defer cancel()
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)
	runStdMultiLabel(t, ctx, domain, "std-int64multiarray")
}

// TestROSTypeStdInt8MultiArray checks Int8MultiArray forward plus reverse.
func TestROSTypeStdInt8MultiArray(t *testing.T) {
	t.Parallel()
	domain := helpers.MustAllocateDomain(t)
	ctx, cancel := context.WithTimeout(context.Background(), typeMatrixTestTimeout)
	defer cancel()
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)
	runStdMultiLabel(t, ctx, domain, "std-int8multiarray")
}

// TestROSTypeStdUInt16MultiArray checks UInt16MultiArray forward plus reverse.
func TestROSTypeStdUInt16MultiArray(t *testing.T) {
	t.Parallel()
	domain := helpers.MustAllocateDomain(t)
	ctx, cancel := context.WithTimeout(context.Background(), typeMatrixTestTimeout)
	defer cancel()
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)
	runStdMultiLabel(t, ctx, domain, "std-uint16multiarray")
}

// TestROSTypeStdUInt32MultiArray checks UInt32MultiArray forward plus reverse.
func TestROSTypeStdUInt32MultiArray(t *testing.T) {
	t.Parallel()
	domain := helpers.MustAllocateDomain(t)
	ctx, cancel := context.WithTimeout(context.Background(), typeMatrixTestTimeout)
	defer cancel()
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)
	runStdMultiLabel(t, ctx, domain, "std-uint32multiarray")
}

// TestROSTypeStdUInt64MultiArray checks UInt64MultiArray forward plus reverse.
func TestROSTypeStdUInt64MultiArray(t *testing.T) {
	t.Parallel()
	domain := helpers.MustAllocateDomain(t)
	ctx, cancel := context.WithTimeout(context.Background(), typeMatrixTestTimeout)
	defer cancel()
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)
	runStdMultiLabel(t, ctx, domain, "std-uint64multiarray")
}

// TestROSTypeStdUInt8MultiArray checks UInt8MultiArray forward plus reverse.
func TestROSTypeStdUInt8MultiArray(t *testing.T) {
	t.Parallel()
	domain := helpers.MustAllocateDomain(t)
	ctx, cancel := context.WithTimeout(context.Background(), typeMatrixTestTimeout)
	defer cancel()
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)
	runStdMultiLabel(t, ctx, domain, "std-uint8multiarray")
}
