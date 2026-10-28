// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package unit_datapath

import (
	"testing"

	"github.com/podomy/weld/internal/types"
)

// Fixed sentinels for TestStdMulti round trips.
const (
	testStdMultiInt16  int16  = -1234
	testStdMultiInt64  int64  = 1234567890123
	testStdMultiUInt64 uint64 = 1234567890123456789
	testStdMultiDim    uint32 = 2
	testStdMultiStride uint32 = 2
	testStdMultiOffset uint32 = 0
	testStdMultiDim4   uint32 = 4
	testStdMultiLabel  string = "std-multi-live-id"
)

// stdMultiNewDecoder builds a Decoder for payload or fails the test.
func stdMultiNewDecoder(t *testing.T, label string, payload []byte) *types.Decoder {
	t.Helper()
	dec, err := types.NewDecoder(payload)
	if err != nil {
		t.Fatalf("decode %s header: %v", label, err)
	}
	return dec
}

// stdMultiFinish checks Finish plus zero drops for label.
func stdMultiFinish(t *testing.T, dec *types.Decoder, label string) {
	t.Helper()
	err := dec.Finish()
	if err != nil {
		t.Fatalf("finish %s: %v", label, err)
	}
	if dec.Dropped() != 0 {
		t.Fatalf("decoder %s drops = %d want 0", label, dec.Dropped())
	}
}

// stdMultiEncodeLayout writes MultiArrayLayout with one dim plus offset.
func stdMultiEncodeLayout(t *testing.T, enc *types.Encoder, label, live string, size, stride uint32) {
	t.Helper()
	err := enc.WriteSequenceLength(1)
	if err != nil {
		t.Fatalf("encode %s dim len: %v", label, err)
	}
	err = enc.WriteString(live)
	if err != nil {
		t.Fatalf("encode %s label: %v", label, err)
	}
	enc.WriteUint32(size)
	enc.WriteUint32(stride)
	enc.WriteUint32(testStdMultiOffset)
}

// stdMultiCheckUint32 reads one uint32 layout field and checks want.
func stdMultiCheckUint32(t *testing.T, dec *types.Decoder, label, field string, want uint32) {
	t.Helper()
	got, err := dec.ReadUint32()
	if err != nil {
		t.Fatalf("decode %s %s: %v", label, field, err)
	}
	if got != want {
		t.Fatalf("decode %s %s = %d want %d", label, field, got, want)
	}
}

// stdMultiDecodeLayout reads MultiArrayLayout dim plus offset and checks values.
func stdMultiDecodeLayout(t *testing.T, dec *types.Decoder, label, live string, size, stride uint32) {
	t.Helper()
	n, err := dec.ReadSequenceLength()
	if err != nil {
		t.Fatalf("decode %s dim len: %v", label, err)
	}
	if n != 1 {
		t.Fatalf("decode %s dim len = %d want 1", label, n)
	}
	gotLabel, err := dec.ReadString()
	if err != nil {
		t.Fatalf("decode %s label: %v", label, err)
	}
	if gotLabel != live {
		t.Fatalf("decode %s label = %q want %q", label, gotLabel, live)
	}
	stdMultiCheckUint32(t, dec, label, "size", size)
	stdMultiCheckUint32(t, dec, label, "stride", stride)
	stdMultiCheckUint32(t, dec, label, "offset", testStdMultiOffset)
}

// stdMultiRoundTripPair checks one two-element multi array round trip.
func stdMultiRoundTripPair[T comparable](t *testing.T, live, label string, size, stride uint32, a, b T, write func(enc *types.Encoder, a, b T), read func(dec *types.Decoder) (T, error)) {
	t.Helper()
	enc := types.NewEncoder(true)
	stdMultiEncodeLayout(t, enc, label, live, size, stride)
	err := enc.WriteSequenceLength(2)
	if err != nil {
		t.Fatalf("encode %s len: %v", label, err)
	}
	write(enc, a, b)
	dec := stdMultiNewDecoder(t, label, enc.Bytes())
	stdMultiDecodeLayout(t, dec, label, live, size, stride)
	n, err := dec.ReadSequenceLength()
	if err != nil {
		t.Fatalf("decode %s len: %v", label, err)
	}
	if n != 2 {
		t.Fatalf("%s len = %d want 2", label, n)
	}
	gotA, err := read(dec)
	if err != nil {
		t.Fatalf("decode %s a: %v", label, err)
	}
	gotB, err := read(dec)
	if err != nil {
		t.Fatalf("decode %s b: %v", label, err)
	}
	if gotA != a || gotB != b {
		t.Fatalf("%s = (%v, %v) want (%v, %v)", label, gotA, gotB, a, b)
	}
	stdMultiFinish(t, dec, label)
}

// stdMultiRoundTripInt8 checks the four-element int8 round trip.
func stdMultiRoundTripInt8(t *testing.T, live string) {
	t.Helper()
	label := "int8"
	enc := types.NewEncoder(true)
	stdMultiEncodeLayout(t, enc, label, live, testStdMultiDim4, testStdMultiDim4)
	err := enc.WriteSequenceLength(4)
	if err != nil {
		t.Fatalf("encode int8 len: %v", err)
	}
	enc.WriteInt8(42)
	enc.WriteInt8(-42)
	enc.WriteInt8(7)
	enc.WriteInt8(-7)
	dec := stdMultiNewDecoder(t, label, enc.Bytes())
	stdMultiDecodeLayout(t, dec, label, live, testStdMultiDim4, testStdMultiDim4)
	n, err := dec.ReadSequenceLength()
	if err != nil {
		t.Fatalf("decode int8 len: %v", err)
	}
	if n != 4 {
		t.Fatalf("int8 len = %d want 4", n)
	}
	want := []int8{42, -42, 7, -7}
	for i, w := range want {
		got, err := dec.ReadInt8()
		if err != nil {
			t.Fatalf("decode int8 %d: %v", i, err)
		}
		if got != w {
			t.Fatalf("int8 %d = %d want %d", i, got, w)
		}
	}
	stdMultiFinish(t, dec, label)
}

// stdMultiRoundTripUint8 checks the four-element uint8 round trip.
func stdMultiRoundTripUint8(t *testing.T, live string) {
	t.Helper()
	label := "uint8"
	enc := types.NewEncoder(true)
	stdMultiEncodeLayout(t, enc, label, live, testStdMultiDim4, testStdMultiDim4)
	err := enc.WriteOctetSequence([]byte{66, 67, 68, 69})
	if err != nil {
		t.Fatalf("encode uint8 data: %v", err)
	}
	dec := stdMultiNewDecoder(t, label, enc.Bytes())
	stdMultiDecodeLayout(t, dec, label, live, testStdMultiDim4, testStdMultiDim4)
	data, err := dec.ReadOctetSequence()
	if err != nil {
		t.Fatalf("decode uint8 data: %v", err)
	}
	want := []byte{66, 67, 68, 69}
	if len(data) != len(want) {
		t.Fatalf("uint8 len = %d want %d", len(data), len(want))
	}
	for i := range data {
		if data[i] != want[i] {
			t.Fatalf("uint8 %d = 0x%02x want 0x%02x", i, data[i], want[i])
		}
	}
	stdMultiFinish(t, dec, label)
}

// TestStdMultiPrimitives checks Int16 plus Int64 plus UInt64 XCDR round trips.
func TestStdMultiPrimitives(t *testing.T) {
	t.Parallel()
	enc := types.NewEncoder(true)
	enc.WriteInt16(testStdMultiInt16)
	dec := stdMultiNewDecoder(t, "int16", enc.Bytes())
	got16, err := dec.ReadInt16()
	if err != nil {
		t.Fatalf("decode int16 data: %v", err)
	}
	if got16 != testStdMultiInt16 {
		t.Fatalf("int16 = %d want %d", got16, testStdMultiInt16)
	}
	for dec.Remaining() > 0 {
		pad, err := dec.ReadUint8()
		if err != nil {
			t.Fatalf("decode int16 pad: %v", err)
		}
		if pad != 0 {
			t.Fatalf("int16 pad 0x%02x wants zero", pad)
		}
	}
	if dec.Dropped() != 0 {
		t.Fatalf("int16 drops = %d want 0", dec.Dropped())
	}
	enc64 := types.NewEncoder(true)
	enc64.WriteInt64(testStdMultiInt64)
	dec64 := stdMultiNewDecoder(t, "int64", enc64.Bytes())
	got64, err := dec64.ReadInt64()
	if err != nil {
		t.Fatalf("decode int64 data: %v", err)
	}
	if got64 != testStdMultiInt64 {
		t.Fatalf("int64 = %d want %d", got64, testStdMultiInt64)
	}
	stdMultiFinish(t, dec64, "int64")
	encU64 := types.NewEncoder(true)
	encU64.WriteUint64(testStdMultiUInt64)
	decU64 := stdMultiNewDecoder(t, "uint64", encU64.Bytes())
	gotU64, err := decU64.ReadUint64()
	if err != nil {
		t.Fatalf("decode uint64 data: %v", err)
	}
	if gotU64 != testStdMultiUInt64 {
		t.Fatalf("uint64 = %d want %d", gotU64, testStdMultiUInt64)
	}
	stdMultiFinish(t, decU64, "uint64")
}

// stdMultiArrayCase names one multi array case plus its round trip func.
type stdMultiArrayCase struct {
	label string
	run   func(t *testing.T, live string)
}

// stdMultiArrayCases returns the ten multi array cases in corpus order.
func stdMultiArrayCases() []stdMultiArrayCase {
	return []stdMultiArrayCase{
		{
			label: "float32",
			run: func(t *testing.T, live string) {
				t.Helper()
				stdMultiRoundTripPair(t, live, "float32", testStdMultiDim, testStdMultiStride, float32(1.5), float32(-2.5),
					func(enc *types.Encoder, a, b float32) {
						enc.WriteFloat32(a)
						enc.WriteFloat32(b)
					}, (*types.Decoder).ReadFloat32)
			},
		},
		{
			label: "float64",
			run: func(t *testing.T, live string) {
				t.Helper()
				stdMultiRoundTripPair(t, live, "float64", testStdMultiDim, testStdMultiStride, 42.5, -42.5,
					func(enc *types.Encoder, a, b float64) {
						enc.WriteFloat64(a)
						enc.WriteFloat64(b)
					}, (*types.Decoder).ReadFloat64)
			},
		},
		{
			label: "int16",
			run: func(t *testing.T, live string) {
				t.Helper()
				stdMultiRoundTripPair(t, live, "int16", testStdMultiDim, testStdMultiStride, int16(1234), int16(-1234),
					func(enc *types.Encoder, a, b int16) {
						enc.WriteInt16(a)
						enc.WriteInt16(b)
					}, (*types.Decoder).ReadInt16)
			},
		},
		{
			label: "int32",
			run: func(t *testing.T, live string) {
				t.Helper()
				stdMultiRoundTripPair(t, live, "int32", testStdMultiDim, testStdMultiStride, int32(305419896), int32(-123456),
					func(enc *types.Encoder, a, b int32) {
						enc.WriteInt32(a)
						enc.WriteInt32(b)
					}, (*types.Decoder).ReadInt32)
			},
		},
		{
			label: "int64",
			run: func(t *testing.T, live string) {
				t.Helper()
				stdMultiRoundTripPair(t, live, "int64", testStdMultiDim, testStdMultiStride, int64(1234567890123), int64(-1234567890123),
					func(enc *types.Encoder, a, b int64) {
						enc.WriteInt64(a)
						enc.WriteInt64(b)
					}, (*types.Decoder).ReadInt64)
			},
		},
		{
			label: "int8",
			run: func(t *testing.T, live string) {
				t.Helper()
				stdMultiRoundTripInt8(t, live)
			},
		},
		{
			label: "uint16",
			run: func(t *testing.T, live string) {
				t.Helper()
				stdMultiRoundTripPair(t, live, "uint16", testStdMultiDim, testStdMultiStride, uint16(4242), uint16(42),
					func(enc *types.Encoder, a, b uint16) {
						enc.WriteUint16(a)
						enc.WriteUint16(b)
					}, (*types.Decoder).ReadUint16)
			},
		},
		{
			label: "uint32",
			run: func(t *testing.T, live string) {
				t.Helper()
				stdMultiRoundTripPair(t, live, "uint32", testStdMultiDim, testStdMultiStride, uint32(305419896), uint32(42),
					func(enc *types.Encoder, a, b uint32) {
						enc.WriteUint32(a)
						enc.WriteUint32(b)
					}, (*types.Decoder).ReadUint32)
			},
		},
		{
			label: "uint64",
			run: func(t *testing.T, live string) {
				t.Helper()
				stdMultiRoundTripPair(t, live, "uint64", testStdMultiDim, testStdMultiStride, uint64(1234567890123456789), uint64(43),
					func(enc *types.Encoder, a, b uint64) {
						enc.WriteUint64(a)
						enc.WriteUint64(b)
					}, (*types.Decoder).ReadUint64)
			},
		},
		{
			label: "uint8",
			run: func(t *testing.T, live string) {
				t.Helper()
				stdMultiRoundTripUint8(t, live)
			},
		},
	}
}

// TestStdMultiArrays checks ten multi array XCDR layouts plus data sequences.
func TestStdMultiArrays(t *testing.T) {
	t.Parallel()
	live := testStdMultiLabel
	for _, tc := range stdMultiArrayCases() {
		t.Run(tc.label, func(t *testing.T) {
			t.Parallel()
			tc.run(t, live)
		})
	}
}
