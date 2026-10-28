// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

// Package unit_datapath holds DATA path conformance tests.
//
// Tests run pure Go with no Docker.
package unit_datapath

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/podomy/weld/internal/transports/dds"
	"github.com/podomy/weld/internal/types"
)

// plainCDRCase describes one generic plain CDR type for fallback pins.
type plainCDRCase struct {
	label    string
	rosTopic string
	idl      string
}

// plainCDRMatrixCases returns the seven generic types beyond String.
func plainCDRMatrixCases() []plainCDRCase {
	return []plainCDRCase{
		{label: "int32", rosTopic: "/plain_int32", idl: "std_msgs__msg__Int32"},
		{label: "float64", rosTopic: "/plain_float64", idl: "std_msgs__msg__Float64"},
		{label: "bool", rosTopic: "/plain_bool", idl: "std_msgs__msg__Bool"},
		{label: "vector3", rosTopic: "/plain_vector3", idl: "geometry_msgs__msg__Vector3"},
		{label: "pointstamped", rosTopic: "/plain_pointstamped", idl: "geometry_msgs__msg__PointStamped"},
		{label: "temperature", rosTopic: "/plain_temperature", idl: "sensor_msgs__msg__Temperature"},
		{label: "fluidpressure", rosTopic: "/plain_fluidpressure", idl: "sensor_msgs__msg__FluidPressure"},
	}
}

// plainCDRFrameID is the fixed frame_id for header types.
const plainCDRFrameID = "plain-frame"

// plainCDREncode builds one valid XCDR1 sample for tc.
func plainCDREncode(t *testing.T, tc plainCDRCase) []byte {
	t.Helper()
	switch tc.idl {
	case "std_msgs__msg__Int32":
		enc := types.NewEncoder(true)
		enc.WriteInt32(305419896)
		return enc.Bytes()
	case "std_msgs__msg__Float64":
		enc := types.NewEncoder(true)
		enc.WriteFloat64(42.5)
		return enc.Bytes()
	case "std_msgs__msg__Bool":
		enc := types.NewEncoder(true)
		enc.WriteBool(true)
		return enc.Bytes()
	case "geometry_msgs__msg__Vector3":
		enc := types.NewEncoder(true)
		enc.WriteFloat64(1.5)
		enc.WriteFloat64(-2.5)
		enc.WriteFloat64(42.5)
		return enc.Bytes()
	case "geometry_msgs__msg__PointStamped":
		enc := types.NewEncoder(true)
		enc.WriteInt32(0)
		enc.WriteUint32(0)
		err := enc.WriteString(plainCDRFrameID)
		if err != nil {
			t.Fatalf("encode %s frame: %v", tc.label, err)
		}
		enc.WriteFloat64(1.0)
		enc.WriteFloat64(2.0)
		enc.WriteFloat64(3.0)
		return enc.Bytes()
	case "sensor_msgs__msg__Temperature":
		enc := types.NewEncoder(true)
		enc.WriteInt32(0)
		enc.WriteUint32(0)
		err := enc.WriteString(plainCDRFrameID)
		if err != nil {
			t.Fatalf("encode %s frame: %v", tc.label, err)
		}
		enc.WriteFloat64(42.5)
		enc.WriteFloat64(0.0)
		return enc.Bytes()
	case "sensor_msgs__msg__FluidPressure":
		enc := types.NewEncoder(true)
		enc.WriteInt32(0)
		enc.WriteUint32(0)
		err := enc.WriteString(plainCDRFrameID)
		if err != nil {
			t.Fatalf("encode %s frame: %v", tc.label, err)
		}
		enc.WriteFloat64(101325.0)
		enc.WriteFloat64(0.0)
		return enc.Bytes()
	default:
		t.Fatalf("unknown idl %q", tc.idl)
		return nil
	}
}

// plainCDRCheckHeader reads Header stamp plus frame_id and checks the frame.
func plainCDRCheckHeader(t *testing.T, dec *types.Decoder, label string) {
	t.Helper()
	sec, err := dec.ReadInt32()
	if err != nil {
		t.Fatalf("decode %s sec: %v", label, err)
	}
	if sec != 0 {
		t.Fatalf("decode %s sec = %d want 0", label, sec)
	}
	nsec, err := dec.ReadUint32()
	if err != nil {
		t.Fatalf("decode %s nanosec: %v", label, err)
	}
	if nsec != 0 {
		t.Fatalf("decode %s nanosec = %d want 0", label, nsec)
	}
	frame, err := dec.ReadString()
	if err != nil {
		t.Fatalf("decode %s frame: %v", label, err)
	}
	if frame != plainCDRFrameID {
		t.Fatalf("decode %s frame = %q want %q", label, frame, plainCDRFrameID)
	}
}

// plainCDRNewDecoder builds a Decoder for payload or fails the test.
func plainCDRNewDecoder(t *testing.T, label string, payload []byte) *types.Decoder {
	t.Helper()
	dec, err := types.NewDecoder(payload)
	if err != nil {
		t.Fatalf("decode %s header: %v", label, err)
	}
	return dec
}

// plainCDRFinish checks Finish plus zero drops for label.
func plainCDRFinish(t *testing.T, dec *types.Decoder, label string) {
	t.Helper()
	err := dec.Finish()
	if err != nil {
		t.Fatalf("finish %s: %v", label, err)
	}
	if dec.Dropped() != 0 {
		t.Fatalf("decoder %s drops = %d want 0", label, dec.Dropped())
	}
}

// plainCDRCheckInt32 checks the pinned Int32 sample value.
func plainCDRCheckInt32(t *testing.T, dec *types.Decoder, label string) {
	t.Helper()
	got, err := dec.ReadInt32()
	if err != nil {
		t.Fatalf("decode %s data: %v", label, err)
	}
	if got != 305419896 {
		t.Fatalf("decode %s data = %d want 305419896", label, got)
	}
}

// plainCDRCheckFloat checks one pinned Float64 sample value.
func plainCDRCheckFloat(t *testing.T, dec *types.Decoder, label string, want float64) {
	t.Helper()
	got, err := dec.ReadFloat64()
	if err != nil {
		t.Fatalf("decode %s data: %v", label, err)
	}
	if got != want {
		t.Fatalf("decode %s data = %v want %v", label, got, want)
	}
}

// plainCDRCheckBool checks the pinned Bool true value plus zero padding.
func plainCDRCheckBool(t *testing.T, dec *types.Decoder, label string) {
	t.Helper()
	got, err := dec.ReadBool()
	if err != nil {
		t.Fatalf("decode %s data: %v", label, err)
	}
	if !got {
		t.Fatalf("decode %s data false want true", label)
	}
	for dec.Remaining() > 0 {
		pad, err := dec.ReadUint8()
		if err != nil {
			t.Fatalf("decode %s pad: %v", label, err)
		}
		if pad != 0 {
			t.Fatalf("decode %s pad 0x%02x want zero", label, pad)
		}
	}
	if dec.Dropped() != 0 {
		t.Fatalf("decoder %s drops = %d want 0", label, dec.Dropped())
	}
}

// plainCDRCheckTriple checks three pinned Float64 values.
func plainCDRCheckTriple(t *testing.T, dec *types.Decoder, label string, wantX, wantY, wantZ float64) {
	t.Helper()
	x, err := dec.ReadFloat64()
	if err != nil {
		t.Fatalf("decode %s x: %v", label, err)
	}
	y, err := dec.ReadFloat64()
	if err != nil {
		t.Fatalf("decode %s y: %v", label, err)
	}
	z, err := dec.ReadFloat64()
	if err != nil {
		t.Fatalf("decode %s z: %v", label, err)
	}
	if x != wantX || y != wantY || z != wantZ {
		t.Fatalf("decode %s (%v, %v, %v) want (%v, %v, %v)", label, x, y, z, wantX, wantY, wantZ)
	}
}

// plainCDRCheckPair checks a pinned value plus variance pair.
func plainCDRCheckPair(t *testing.T, dec *types.Decoder, label string, want, wantVariance float64) {
	t.Helper()
	got, err := dec.ReadFloat64()
	if err != nil {
		t.Fatalf("decode %s value: %v", label, err)
	}
	variance, err := dec.ReadFloat64()
	if err != nil {
		t.Fatalf("decode %s variance: %v", label, err)
	}
	if got != want || variance != wantVariance {
		t.Fatalf("decode %s (%v, %v) want (%v, %v)", label, got, variance, want, wantVariance)
	}
}

// plainCDRCheck validates payload bytes for tc via Decoder.
func plainCDRCheck(t *testing.T, tc plainCDRCase, payload []byte) {
	t.Helper()
	switch tc.idl {
	case "std_msgs__msg__Int32":
		dec := plainCDRNewDecoder(t, tc.label, payload)
		plainCDRCheckInt32(t, dec, tc.label)
		plainCDRFinish(t, dec, tc.label)
	case "std_msgs__msg__Float64":
		dec := plainCDRNewDecoder(t, tc.label, payload)
		plainCDRCheckFloat(t, dec, tc.label, 42.5)
		plainCDRFinish(t, dec, tc.label)
	case "std_msgs__msg__Bool":
		dec := plainCDRNewDecoder(t, tc.label, payload)
		plainCDRCheckBool(t, dec, tc.label)
	case "geometry_msgs__msg__Vector3":
		dec := plainCDRNewDecoder(t, tc.label, payload)
		plainCDRCheckTriple(t, dec, tc.label, 1.5, -2.5, 42.5)
		plainCDRFinish(t, dec, tc.label)
	case "geometry_msgs__msg__PointStamped":
		dec := plainCDRNewDecoder(t, tc.label, payload)
		plainCDRCheckHeader(t, dec, tc.label)
		plainCDRCheckTriple(t, dec, tc.label, 1.0, 2.0, 3.0)
		plainCDRFinish(t, dec, tc.label)
	case "sensor_msgs__msg__Temperature":
		dec := plainCDRNewDecoder(t, tc.label, payload)
		plainCDRCheckHeader(t, dec, tc.label)
		plainCDRCheckPair(t, dec, tc.label, 42.5, 0.0)
		plainCDRFinish(t, dec, tc.label)
	case "sensor_msgs__msg__FluidPressure":
		dec := plainCDRNewDecoder(t, tc.label, payload)
		plainCDRCheckHeader(t, dec, tc.label)
		plainCDRCheckPair(t, dec, tc.label, 101325.0, 0.0)
		plainCDRFinish(t, dec, tc.label)
	default:
		t.Fatalf("unknown idl %q", tc.idl)
	}
}

// plainCDRReceiver installs one remote writer and returns receiver plus GUID.
func plainCDRReceiver(t *testing.T, rosTopic, idl string) (*dds.DataReceiver, dds.WriterGUID) {
	t.Helper()
	var prefix dds.SEDPGUIDPrefix
	for i := range prefix {
		prefix[i] = byte(0x50 + i)
	}
	idx, err := dds.NewSEDPIndex(prefix, dataPathTestDomain)
	if err != nil {
		t.Fatalf("new sedp index: %v", err)
	}
	ddsTopic, err := dds.TopicName(rosTopic, false)
	if err != nil {
		t.Fatalf("topic name %q: %v", rosTopic, err)
	}
	ddsType, err := dds.MessageTypeName(idl)
	if err != nil {
		t.Fatalf("message type %q: %v", idl, err)
	}
	id := dds.SEDPGUID{Prefix: prefix, Entity: dds.SEDPEntityID{0x00, 0x00, 0x02, 0x03}}
	ep := dds.SEDPEndpoint{
		ID:       id,
		ROSTopic: rosTopic,
		DDSTopic: ddsTopic,
		TypeDDS:  ddsType,
		TypeIDL:  idl,
		IsReader: false,
		DomainID: dataPathTestDomain,
	}
	err = idx.UpsertRemote(ep)
	if err != nil {
		t.Fatalf("upsert remote %s: %v", rosTopic, err)
	}
	recv := dds.NewDataReceiver()
	recv.SetSEDPIndex(idx)
	var guid dds.WriterGUID
	copy(guid[:12], prefix[:])
	copy(guid[12:16], id.Entity[:])
	return recv, guid
}

// TestPlainCDRMatrixRoundTrip checks seven generic types via plain CDR fallback.
func TestPlainCDRMatrixRoundTrip(t *testing.T) {
	t.Parallel()
	for _, tc := range plainCDRMatrixCases() {
		if tc.rosTopic == "/chatter" {
			t.Fatalf("case %s uses hardcoded chatter topic", tc.label)
		}
		wire := plainCDREncode(t, tc)
		plainCDRCheck(t, tc, wire)
		recv, guid := plainCDRReceiver(t, tc.rosTopic, tc.idl)
		src := &dataPathSliceFrameSource{}
		src.queue(guid, wire)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		msg, err := recv.TakeFrom(ctx, src)
		cancel()
		if err != nil {
			t.Fatalf("take from %s: %v", tc.label, err)
		}
		if msg.TopicName != tc.rosTopic {
			t.Fatalf("take %s topic = %q want %q", tc.label, msg.TopicName, tc.rosTopic)
		}
		if msg.TypeName != tc.idl {
			t.Fatalf("take %s type = %q want %q", tc.label, msg.TypeName, tc.idl)
		}
		if !bytes.Equal(msg.Payload, wire) {
			t.Fatalf("take %s payload mismatch", tc.label)
		}
		if recv.DroppedCount() != 0 {
			t.Fatalf("drops after valid %s = %d want 0", tc.label, recv.DroppedCount())
		}
		plainCDRCheck(t, tc, msg.Payload)
	}
}

// plainCDRCorrupts returns corrupt shapes that must fail validation for tc.
func plainCDRCorrupts(tc plainCDRCase, valid []byte) [][]byte {
	out := [][]byte{{}}
	badHeader := append([]byte(nil), valid...)
	if len(badHeader) > 0 {
		badHeader[0] = 0x01
		out = append(out, badHeader)
	}
	if len(valid) >= 4 {
		headerOnly := append([]byte(nil), valid[:4]...)
		out = append(out, headerOnly)
	}
	trailing := append(append([]byte(nil), valid...), 0xFF)
	out = append(out, trailing)
	if tc.idl == "std_msgs__msg__Bool" {
		invalidBool := append([]byte(nil), valid...)
		if len(invalidBool) > 4 {
			invalidBool[4] = 0x02
			out = append(out, invalidBool)
		}
	} else if len(valid) > 1 {
		trunc := append([]byte(nil), valid[:len(valid)-1]...)
		out = append(out, trunc)
	}
	return out
}

// TestPlainCDRMatrixTruncate checks corrupt truncations count drops per type.
func TestPlainCDRMatrixTruncate(t *testing.T) {
	t.Parallel()
	for _, tc := range plainCDRMatrixCases() {
		valid := plainCDREncode(t, tc)
		for i, corrupt := range plainCDRCorrupts(tc, valid) {
			recv, guid := plainCDRReceiver(t, tc.rosTopic, tc.idl)
			cp := append([]byte(nil), corrupt...)
			src := &dataPathSliceFrameSource{}
			src.queue(guid, cp)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			_, err := recv.TakeFrom(ctx, src)
			cancel()
			if err == nil {
				t.Fatalf("case %s corrupt %d succeeded, want error", tc.label, i)
			}
			if recv.DroppedCount() != 1 {
				t.Fatalf("case %s corrupt %d drops = %d want 1", tc.label, i, recv.DroppedCount())
			}
		}
	}
}

// TestPlainCDRMatrixUnknownWriter checks unknown writers drop without chatter mapping.
func TestPlainCDRMatrixUnknownWriter(t *testing.T) {
	t.Parallel()
	for _, tc := range plainCDRMatrixCases() {
		if tc.rosTopic == "/chatter" {
			t.Fatalf("case %s uses hardcoded chatter topic", tc.label)
		}
		valid := plainCDREncode(t, tc)
		recv, _ := plainCDRReceiver(t, tc.rosTopic, tc.idl)
		var unknown dds.WriterGUID
		for i := range unknown {
			unknown[i] = 0xFF
		}
		src := &dataPathSliceFrameSource{}
		src.queue(unknown, valid)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_, err := recv.TakeFrom(ctx, src)
		cancel()
		if err == nil {
			t.Fatalf("case %s unknown writer succeeded, want error", tc.label)
		}
		if recv.DroppedCount() != 1 {
			t.Fatalf("case %s unknown drops = %d want 1", tc.label, recv.DroppedCount())
		}
		bare := dds.NewDataReceiver()
		src2 := &dataPathSliceFrameSource{}
		src2.queue(unknown, valid)
		ctx2, cancel2 := context.WithTimeout(context.Background(), 5*time.Second)
		_, err = bare.TakeFrom(ctx2, src2)
		cancel2()
		if err == nil {
			t.Fatalf("case %s bare receiver succeeded, want error", tc.label)
		}
		if bare.DroppedCount() != 1 {
			t.Fatalf("case %s bare drops = %d want 1", tc.label, bare.DroppedCount())
		}
	}
}
