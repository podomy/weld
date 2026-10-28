// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

// Package unit_services pins ten ROS service XCDR encodings with pure Go.
//
// This file covers std_srvs Empty, SetBool, and Trigger plus nav_msgs
// GetMap, GetPlan, LoadMap, and SetMap plus sensor_msgs SetCameraInfo plus
// diagnostic_msgs SelfTest and AddDiagnostics. The std_srvs shapes were
// verified against testbed/unit/datapath conformance_std_srvs_test.go and
// conformance_std_srvs_extra_test.go which already pin Trigger, Empty, and
// SetBool; this file re-pins those three for single package completeness
// and adds the seven missing service shapes. No live DDS traffic is used.
// Services travel on rq request and rr reply DDS topics, never on the rt
// topic path, so topic gate checks are N/A and this file pins the request
// kind as request and the reply kind as reply.
//
// Complex nested types use representative sketches built only from
// internal/types primitives. OccupancyGrid is a frame id string plus width
// plus height plus resolution plus octet sequence data. PoseStamped is a
// frame id string plus seven float64 values. Path is a frame id string
// plus a pose count plus seven float64 values per pose. CameraInfo is a
// frame id string plus height plus width plus distortion model string.
// DiagnosticStatus is a level byte plus name string plus message string.
// The sketches exercise the same codec paths as the IDL shapes: strings,
// bools, uint8, uint32, float32, float64, and sequences, with strict
// Finish and zero drops.
package unit_services

import (
	"strings"
	"testing"

	"github.com/podomy/weld/internal/transports/dds"
	"github.com/podomy/weld/internal/types"
)

// servicesTestGUID returns a fixed 8 byte service GUID for tests.
func servicesTestGUID() [8]byte {
	var guid [8]byte
	for i := range guid {
		guid[i] = byte(i + 1)
	}
	return guid
}

// servicesTestSeq is the fixed base service sequence number for tests.
const servicesTestSeq = uint64(101)

// servicesPinned lists all ten pinned services as package and name pairs.
func servicesPinned() [][2]string {
	return [][2]string{
		{"std_srvs", "Empty"},
		{"std_srvs", "SetBool"},
		{"std_srvs", "Trigger"},
		{"nav_msgs", "GetMap"},
		{"nav_msgs", "GetPlan"},
		{"nav_msgs", "LoadMap"},
		{"nav_msgs", "SetMap"},
		{"sensor_msgs", "SetCameraInfo"},
		{"diagnostic_msgs", "SelfTest"},
		{"diagnostic_msgs", "AddDiagnostics"},
	}
}

// servicesCheckCorpus requires a service entry in the corpus.
func servicesCheckCorpus(t *testing.T, pkg, name string) types.CorpusEntry {
	t.Helper()
	entry, ok := types.Lookup(pkg, name)
	if !ok {
		t.Fatalf("corpus misses %s/%s, no shape to pin", pkg, name)
	}
	if entry.Kind != types.KindService {
		t.Fatalf("corpus %s/%s kind = %q, want service", pkg, name, entry.Kind)
	}
	return entry
}

// servicesNewEncoder returns an encoder with the service header written.
func servicesNewEncoder(t *testing.T, le bool, guid [8]byte, seq uint64) *types.Encoder {
	t.Helper()
	enc := types.NewEncoder(le)
	err := enc.WriteServiceHeader(guid, seq)
	if err != nil {
		t.Fatalf("write service header: %v", err)
	}
	if enc.Len() != types.ServiceEnvelopeSize {
		t.Fatalf("encoder len after header = %d, want %d", enc.Len(), types.ServiceEnvelopeSize)
	}
	return enc
}

// servicesNewDecoder returns a decoder positioned after the service header.
func servicesNewDecoder(t *testing.T, wire []byte, guid [8]byte, seq uint64) *types.Decoder {
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

// servicesFinish checks strict consumption with no drops.
func servicesFinish(t *testing.T, dec *types.Decoder, label string) {
	t.Helper()
	err := dec.Finish()
	if err != nil {
		t.Fatalf("finish %s: %v", label, err)
	}
	if dec.Dropped() != 0 {
		t.Fatalf("decoder %s drops = %d, want 0", label, dec.Dropped())
	}
	if dec.Remaining() != 0 {
		t.Fatalf("decoder %s remaining = %d, want 0", label, dec.Remaining())
	}
}

// servicesCheckServiceKind pins that a service maps to rq and rr topics.
func servicesCheckServiceKind(t *testing.T, service string) {
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

// servicesEncodeEmpty encodes a body with no user fields.
func servicesEncodeEmpty(t *testing.T, le bool, guid [8]byte, seq uint64) []byte {
	t.Helper()
	enc := servicesNewEncoder(t, le, guid, seq)
	wire := enc.Bytes()
	if len(wire)%4 != 0 {
		t.Fatalf("empty service wire length %d is not padded to 4", len(wire))
	}
	return wire
}

// servicesDecodeEmpty decodes a body with no user fields.
func servicesDecodeEmpty(t *testing.T, wire []byte, guid [8]byte, seq uint64, label string) {
	t.Helper()
	dec := servicesNewDecoder(t, wire, guid, seq)
	for dec.Remaining() > 0 {
		pad, err := dec.ReadUint8()
		if err != nil {
			t.Fatalf("read %s pad: %v", label, err)
		}
		if pad != 0x00 {
			t.Fatalf("read %s pad = 0x%02x, want zero", label, pad)
		}
	}
	servicesFinish(t, dec, label)
}

// servicesCheckEmpty round trips an empty body in both orders.
func servicesCheckEmpty(t *testing.T, guid [8]byte, seq uint64, label string) {
	t.Helper()
	for _, le := range []bool{true, false} {
		wire := servicesEncodeEmpty(t, le, guid, seq)
		if le {
			if wire[1] != 0x01 {
				t.Fatalf("%s LE header flag = 0x%02x, want 0x01", label, wire[1])
			}
		} else {
			if wire[1] != 0x00 {
				t.Fatalf("%s BE header flag = 0x%02x, want 0x00", label, wire[1])
			}
		}
		servicesDecodeEmpty(t, wire, guid, seq, label)
	}
}

// servicesEncodeBoolRequest encodes one bool request field.
func servicesEncodeBoolRequest(t *testing.T, le bool, guid [8]byte, seq uint64, data bool) []byte {
	t.Helper()
	enc := servicesNewEncoder(t, le, guid, seq)
	enc.WriteBool(data)
	wire := enc.Bytes()
	if len(wire)%4 != 0 {
		t.Fatalf("bool request wire length %d is not padded to 4", len(wire))
	}
	return wire
}

// servicesDecodeBoolRequest decodes one bool request field.
func servicesDecodeBoolRequest(t *testing.T, wire []byte, guid [8]byte, seq uint64, want bool, label string) {
	t.Helper()
	dec := servicesNewDecoder(t, wire, guid, seq)
	got, err := dec.ReadBool()
	if err != nil {
		t.Fatalf("read %s data: %v", label, err)
	}
	if got != want {
		t.Fatalf("read %s data = %v, want %v", label, got, want)
	}
	for dec.Remaining() > 0 {
		pad, err := dec.ReadUint8()
		if err != nil {
			t.Fatalf("read %s pad: %v", label, err)
		}
		if pad != 0x00 {
			t.Fatalf("read %s pad = 0x%02x, want zero", label, pad)
		}
	}
	if dec.Dropped() != 0 {
		t.Fatalf("decoder %s drops = %d, want 0", label, dec.Dropped())
	}
	servicesFinish(t, dec, label)
}

// servicesEncodeBoolStringResponse encodes a bool plus string response.
func servicesEncodeBoolStringResponse(t *testing.T, le bool, guid [8]byte, seq uint64, success bool, message string) []byte {
	t.Helper()
	enc := servicesNewEncoder(t, le, guid, seq)
	enc.WriteBool(success)
	err := enc.WriteString(message)
	if err != nil {
		t.Fatalf("encode response message: %v", err)
	}
	wire := enc.Bytes()
	if len(wire)%4 != 0 {
		t.Fatalf("bool string response wire length %d is not padded to 4", len(wire))
	}
	return wire
}

// servicesDecodeBoolStringResponse decodes a bool plus string response.
func servicesDecodeBoolStringResponse(t *testing.T, wire []byte, guid [8]byte, seq uint64, wantSuccess bool, wantMessage, label string) {
	t.Helper()
	dec := servicesNewDecoder(t, wire, guid, seq)
	gotSuccess, err := dec.ReadBool()
	if err != nil {
		t.Fatalf("read %s success: %v", label, err)
	}
	if gotSuccess != wantSuccess {
		t.Fatalf("read %s success = %v, want %v", label, gotSuccess, wantSuccess)
	}
	gotMessage, err := dec.ReadString()
	if err != nil {
		t.Fatalf("read %s message: %v", label, err)
	}
	if gotMessage != wantMessage {
		t.Fatalf("read %s message = %q, want %q", label, gotMessage, wantMessage)
	}
	servicesFinish(t, dec, label)
}

// servicesEncodeStringRequest encodes one string request field.
func servicesEncodeStringRequest(t *testing.T, le bool, guid [8]byte, seq uint64, value string) []byte {
	t.Helper()
	enc := servicesNewEncoder(t, le, guid, seq)
	err := enc.WriteString(value)
	if err != nil {
		t.Fatalf("encode string request: %v", err)
	}
	wire := enc.Bytes()
	if len(wire)%4 != 0 {
		t.Fatalf("string request wire length %d is not padded to 4", len(wire))
	}
	return wire
}

// servicesDecodeStringRequest decodes one string request field.
func servicesDecodeStringRequest(t *testing.T, wire []byte, guid [8]byte, seq uint64, want, label string) {
	t.Helper()
	dec := servicesNewDecoder(t, wire, guid, seq)
	got, err := dec.ReadString()
	if err != nil {
		t.Fatalf("read %s value: %v", label, err)
	}
	if got != want {
		t.Fatalf("read %s value = %q, want %q", label, got, want)
	}
	servicesFinish(t, dec, label)
}

// occupancySketch is a representative OccupancyGrid payload.
type occupancySketch struct {
	frameID    string
	width      uint32
	height     uint32
	resolution float32
	data       []byte
}

// servicesOccupancyBase returns a fixed occupancy sketch for tests.
func servicesOccupancyBase() occupancySketch {
	return occupancySketch{
		frameID:    "map",
		width:      4,
		height:     2,
		resolution: 0.05,
		data:       []byte{0, 100, 100, 0, 0, 50, 50, 0},
	}
}

// servicesEncodeOccupancy appends an occupancy sketch to the encoder.
func servicesEncodeOccupancy(t *testing.T, enc *types.Encoder, occ occupancySketch, label string) {
	t.Helper()
	err := enc.WriteString(occ.frameID)
	if err != nil {
		t.Fatalf("encode %s frame id: %v", label, err)
	}
	enc.WriteUint32(occ.width)
	enc.WriteUint32(occ.height)
	enc.WriteFloat32(occ.resolution)
	err = enc.WriteOctetSequence(occ.data)
	if err != nil {
		t.Fatalf("encode %s data: %v", label, err)
	}
}

// servicesDecodeOccupancyHeader reads frame plus width plus height.
func servicesDecodeOccupancyHeader(t *testing.T, dec *types.Decoder, want occupancySketch, label, field string) {
	t.Helper()
	gotFrame, err := dec.ReadString()
	if err != nil {
		t.Fatalf("read %s %s frame id: %v", label, field, err)
	}
	if gotFrame != want.frameID {
		t.Fatalf("read %s %s frame id = %q, want %q", label, field, gotFrame, want.frameID)
	}
	gotWidth, err := dec.ReadUint32()
	if err != nil {
		t.Fatalf("read %s %s width: %v", label, field, err)
	}
	if gotWidth != want.width {
		t.Fatalf("read %s %s width = %d, want %d", label, field, gotWidth, want.width)
	}
	gotHeight, err := dec.ReadUint32()
	if err != nil {
		t.Fatalf("read %s %s height: %v", label, field, err)
	}
	if gotHeight != want.height {
		t.Fatalf("read %s %s height = %d, want %d", label, field, gotHeight, want.height)
	}
}

// servicesDecodeOccupancyPayload reads resolution plus octet data.
func servicesDecodeOccupancyPayload(t *testing.T, dec *types.Decoder, want occupancySketch, label, field string) {
	t.Helper()
	gotRes, err := dec.ReadFloat32()
	if err != nil {
		t.Fatalf("read %s %s resolution: %v", label, field, err)
	}
	if gotRes != want.resolution {
		t.Fatalf("read %s %s resolution = %v, want %v", label, field, gotRes, want.resolution)
	}
	gotData, err := dec.ReadOctetSequence()
	if err != nil {
		t.Fatalf("read %s %s data: %v", label, field, err)
	}
	if len(gotData) != len(want.data) {
		t.Fatalf("read %s %s data length = %d, want %d", label, field, len(gotData), len(want.data))
	}
	for i := range want.data {
		if gotData[i] != want.data[i] {
			t.Fatalf("read %s %s data byte %d = %d, want %d", label, field, i, gotData[i], want.data[i])
		}
	}
}

// servicesDecodeOccupancy reads an occupancy sketch from the decoder.
func servicesDecodeOccupancy(t *testing.T, dec *types.Decoder, want occupancySketch, label, field string) {
	t.Helper()
	servicesDecodeOccupancyHeader(t, dec, want, label, field)
	servicesDecodeOccupancyPayload(t, dec, want, label, field)
}

// poseSketch is a representative PoseStamped payload.
type poseSketch struct {
	frameID string
	xyz     [3]float64
	quat    [4]float64
}

// servicesPoseBase returns a fixed pose sketch for tests.
func servicesPoseBase(frame string, x float64) poseSketch {
	return poseSketch{
		frameID: frame,
		xyz:     [3]float64{x, 2.0, 0.0},
		quat:    [4]float64{0.0, 0.0, 0.0, 1.0},
	}
}

// servicesEncodePose appends a pose sketch to the encoder.
func servicesEncodePose(t *testing.T, enc *types.Encoder, pose poseSketch, label string) {
	t.Helper()
	err := enc.WriteString(pose.frameID)
	if err != nil {
		t.Fatalf("encode %s frame id: %v", label, err)
	}
	for i := range pose.xyz {
		enc.WriteFloat64(pose.xyz[i])
	}
	for i := range pose.quat {
		enc.WriteFloat64(pose.quat[i])
	}
}

// servicesDecodePose reads a pose sketch from the decoder.
func servicesDecodePose(t *testing.T, dec *types.Decoder, want poseSketch, label, field string) {
	t.Helper()
	gotFrame, err := dec.ReadString()
	if err != nil {
		t.Fatalf("read %s %s frame id: %v", label, field, err)
	}
	if gotFrame != want.frameID {
		t.Fatalf("read %s %s frame id = %q, want %q", label, field, gotFrame, want.frameID)
	}
	for i := range want.xyz {
		got, err := dec.ReadFloat64()
		if err != nil {
			t.Fatalf("read %s %s xyz %d: %v", label, field, i, err)
		}
		if got != want.xyz[i] {
			t.Fatalf("read %s %s xyz %d = %v, want %v", label, field, i, got, want.xyz[i])
		}
	}
	for i := range want.quat {
		got, err := dec.ReadFloat64()
		if err != nil {
			t.Fatalf("read %s %s quat %d: %v", label, field, i, err)
		}
		if got != want.quat[i] {
			t.Fatalf("read %s %s quat %d = %v, want %v", label, field, i, got, want.quat[i])
		}
	}
}

// servicesCheckBoolRequest round trips a bool request in both orders.
func servicesCheckBoolRequest(t *testing.T, guid [8]byte, seq uint64, label string, data bool) {
	t.Helper()
	for _, le := range []bool{true, false} {
		wire := servicesEncodeBoolRequest(t, le, guid, seq, data)
		servicesDecodeBoolRequest(t, wire, guid, seq, data, label)
	}
}

// servicesCheckBoolStringResponse round trips a bool plus string response.
func servicesCheckBoolStringResponse(t *testing.T, guid [8]byte, seq uint64, label string, success bool, message string) {
	t.Helper()
	for _, le := range []bool{true, false} {
		wire := servicesEncodeBoolStringResponse(t, le, guid, seq, success, message)
		servicesDecodeBoolStringResponse(t, wire, guid, seq, success, message, label)
	}
}

// servicesCheckStringRequest round trips a string request in both orders.
func servicesCheckStringRequest(t *testing.T, guid [8]byte, seq uint64, label, value string) {
	t.Helper()
	for _, le := range []bool{true, false} {
		wire := servicesEncodeStringRequest(t, le, guid, seq, value)
		servicesDecodeStringRequest(t, wire, guid, seq, value, label)
	}
}

// servicesCheckWirePadded requires wire length padded to 4.
func servicesCheckWirePadded(t *testing.T, wire []byte, label string) {
	t.Helper()
	if len(wire)%4 != 0 {
		t.Fatalf("%s wire length %d is not padded to 4", label, len(wire))
	}
}

// servicesDrainPadZero drains Remaining bytes and checks all zeros.
func servicesDrainPadZero(t *testing.T, dec *types.Decoder, label string) {
	t.Helper()
	for dec.Remaining() > 0 {
		pad, err := dec.ReadUint8()
		if err != nil {
			t.Fatalf("read %s pad: %v", label, err)
		}
		if pad != 0x00 {
			t.Fatalf("read %s pad = 0x%02x, want zero", label, pad)
		}
	}
}

// servicesRoundTripOccupancy round trips one occupancy map response.
func servicesRoundTripOccupancy(t *testing.T, guid [8]byte, seq uint64, label string, occ occupancySketch) {
	t.Helper()
	for _, le := range []bool{true, false} {
		enc := servicesNewEncoder(t, le, guid, seq)
		servicesEncodeOccupancy(t, enc, occ, label)
		wire := enc.Bytes()
		servicesCheckWirePadded(t, wire, label)
		dec := servicesNewDecoder(t, wire, guid, seq)
		servicesDecodeOccupancy(t, dec, occ, label, "map")
		servicesFinish(t, dec, label)
	}
}

// servicesCheckGetPlanRequest round trips a GetPlan request.
func servicesCheckGetPlanRequest(t *testing.T, guid [8]byte, seq uint64, start, goal poseSketch, tolerance float32) {
	t.Helper()
	const label = "getplan request"
	for _, le := range []bool{true, false} {
		enc := servicesNewEncoder(t, le, guid, seq)
		servicesEncodePose(t, enc, start, label)
		servicesEncodePose(t, enc, goal, label)
		enc.WriteFloat32(tolerance)
		wire := enc.Bytes()
		servicesCheckWirePadded(t, wire, label)
		dec := servicesNewDecoder(t, wire, guid, seq)
		servicesDecodePose(t, dec, start, label, "start")
		servicesDecodePose(t, dec, goal, label, "goal")
		servicesDecodeTolerance(t, dec, tolerance, label)
		servicesFinish(t, dec, label)
	}
}

// servicesDecodeTolerance reads and checks the GetPlan tolerance.
func servicesDecodeTolerance(t *testing.T, dec *types.Decoder, want float32, label string) {
	t.Helper()
	got, err := dec.ReadFloat32()
	if err != nil {
		t.Fatalf("read %s tolerance: %v", label, err)
	}
	if got != want {
		t.Fatalf("read %s tolerance = %v, want %v", label, got, want)
	}
}

// servicesEncodePlanPoses writes frame plus count plus float64 poses.
func servicesEncodePlanPoses(t *testing.T, enc *types.Encoder, frame string, poses [][7]float64, label string) {
	t.Helper()
	err := enc.WriteString(frame)
	if err != nil {
		t.Fatalf("encode %s frame: %v", label, err)
	}
	err = enc.WriteSequenceLength(len(poses))
	if err != nil {
		t.Fatalf("encode %s count: %v", label, err)
	}
	for _, pose := range poses {
		for _, v := range pose {
			enc.WriteFloat64(v)
		}
	}
}

// servicesDecodePlanHeader reads frame plus count and checks them.
func servicesDecodePlanHeader(t *testing.T, dec *types.Decoder, wantFrame string, wantCount int, label string) {
	t.Helper()
	gotFrame, err := dec.ReadString()
	if err != nil {
		t.Fatalf("read %s frame: %v", label, err)
	}
	if gotFrame != wantFrame {
		t.Fatalf("read %s frame = %q, want %q", label, gotFrame, wantFrame)
	}
	gotCount, err := dec.ReadSequenceLength()
	if err != nil {
		t.Fatalf("read %s count: %v", label, err)
	}
	if gotCount != wantCount {
		t.Fatalf("read %s count = %d, want %d", label, gotCount, wantCount)
	}
}

// servicesDecodePlanPoses reads and checks every pose field.
func servicesDecodePlanPoses(t *testing.T, dec *types.Decoder, want [][7]float64, label string) {
	t.Helper()
	for i, wantPose := range want {
		for j, wantV := range wantPose {
			got, err := dec.ReadFloat64()
			if err != nil {
				t.Fatalf("read %s pose %d field %d: %v", label, i, j, err)
			}
			if got != wantV {
				t.Fatalf("read %s pose %d field %d = %v, want %v", label, i, j, got, wantV)
			}
		}
	}
}

// servicesCheckGetPlanResponse round trips a GetPlan response.
func servicesCheckGetPlanResponse(t *testing.T, guid [8]byte, seq uint64, frame string, poses [][7]float64) {
	t.Helper()
	const label = "getplan response"
	for _, le := range []bool{true, false} {
		enc := servicesNewEncoder(t, le, guid, seq)
		servicesEncodePlanPoses(t, enc, frame, poses, label)
		wire := enc.Bytes()
		servicesCheckWirePadded(t, wire, label)
		dec := servicesNewDecoder(t, wire, guid, seq)
		servicesDecodePlanHeader(t, dec, frame, len(poses), label)
		servicesDecodePlanPoses(t, dec, poses, label)
		servicesFinish(t, dec, label)
	}
}

// servicesCheckLoadMapResponse round trips a LoadMap response.
func servicesCheckLoadMapResponse(t *testing.T, guid [8]byte, seq uint64, occ occupancySketch, result uint8) {
	t.Helper()
	const label = "loadmap response"
	for _, le := range []bool{true, false} {
		enc := servicesNewEncoder(t, le, guid, seq)
		servicesEncodeOccupancy(t, enc, occ, label)
		enc.WriteUint8(result)
		wire := enc.Bytes()
		servicesCheckWirePadded(t, wire, label)
		dec := servicesNewDecoder(t, wire, guid, seq)
		servicesDecodeOccupancy(t, dec, occ, label, "map")
		servicesDecodeLoadResult(t, dec, result, label)
		servicesDrainPadZero(t, dec, label)
		servicesFinish(t, dec, label)
	}
}

// servicesDecodeLoadResult reads and checks the LoadMap result byte.
func servicesDecodeLoadResult(t *testing.T, dec *types.Decoder, want uint8, label string) {
	t.Helper()
	got, err := dec.ReadUint8()
	if err != nil {
		t.Fatalf("read %s result: %v", label, err)
	}
	if got != want {
		t.Fatalf("read %s result = %d, want %d", label, got, want)
	}
}

// servicesEncodeCovariance writes 36 covariance entries.
func servicesEncodeCovariance(enc *types.Encoder, covariance [36]float64) {
	for _, v := range covariance {
		enc.WriteFloat64(v)
	}
}

// servicesDecodeCovariance reads and checks 36 covariance entries.
func servicesDecodeCovariance(t *testing.T, dec *types.Decoder, want [36]float64, label string) {
	t.Helper()
	for i, wantV := range want {
		got, err := dec.ReadFloat64()
		if err != nil {
			t.Fatalf("read %s covariance %d: %v", label, i, err)
		}
		if got != wantV {
			t.Fatalf("read %s covariance %d = %v, want %v", label, i, got, wantV)
		}
	}
}

// servicesCheckSetMapRequest round trips a SetMap request.
func servicesCheckSetMapRequest(t *testing.T, guid [8]byte, seq uint64, occ occupancySketch, initial poseSketch, covariance [36]float64) {
	t.Helper()
	const label = "setmap request"
	for _, le := range []bool{true, false} {
		enc := servicesNewEncoder(t, le, guid, seq)
		servicesEncodeOccupancy(t, enc, occ, label)
		servicesEncodePose(t, enc, initial, label)
		servicesEncodeCovariance(enc, covariance)
		wire := enc.Bytes()
		servicesCheckWirePadded(t, wire, label)
		dec := servicesNewDecoder(t, wire, guid, seq)
		servicesDecodeOccupancy(t, dec, occ, label, "map")
		servicesDecodePose(t, dec, initial, label, "initial pose")
		servicesDecodeCovariance(t, dec, covariance, label)
		servicesFinish(t, dec, label)
	}
}

// servicesEncodeCameraInfo writes frame plus size plus distortion model.
func servicesEncodeCameraInfo(t *testing.T, enc *types.Encoder, frameID string, height, width uint32, distortion, label string) {
	t.Helper()
	err := enc.WriteString(frameID)
	if err != nil {
		t.Fatalf("encode %s frame: %v", label, err)
	}
	enc.WriteUint32(height)
	enc.WriteUint32(width)
	err = enc.WriteString(distortion)
	if err != nil {
		t.Fatalf("encode %s distortion: %v", label, err)
	}
}

// servicesDecodeCameraDims reads and checks height plus width.
func servicesDecodeCameraDims(t *testing.T, dec *types.Decoder, wantHeight, wantWidth uint32, label string) {
	t.Helper()
	gotHeight, err := dec.ReadUint32()
	if err != nil {
		t.Fatalf("read %s height: %v", label, err)
	}
	if gotHeight != wantHeight {
		t.Fatalf("read %s height = %d, want %d", label, gotHeight, wantHeight)
	}
	gotWidth, err := dec.ReadUint32()
	if err != nil {
		t.Fatalf("read %s width: %v", label, err)
	}
	if gotWidth != wantWidth {
		t.Fatalf("read %s width = %d, want %d", label, gotWidth, wantWidth)
	}
}

// servicesDecodeCameraInfo reads and checks a SetCameraInfo request body.
func servicesDecodeCameraInfo(t *testing.T, dec *types.Decoder, wantFrame string, wantHeight, wantWidth uint32, wantDistortion, label string) {
	t.Helper()
	gotFrame, err := dec.ReadString()
	if err != nil {
		t.Fatalf("read %s frame: %v", label, err)
	}
	if gotFrame != wantFrame {
		t.Fatalf("read %s frame = %q, want %q", label, gotFrame, wantFrame)
	}
	servicesDecodeCameraDims(t, dec, wantHeight, wantWidth, label)
	gotDistortion, err := dec.ReadString()
	if err != nil {
		t.Fatalf("read %s distortion: %v", label, err)
	}
	if gotDistortion != wantDistortion {
		t.Fatalf("read %s distortion = %q, want %q", label, gotDistortion, wantDistortion)
	}
}

// servicesCheckCameraInfoRequest round trips a SetCameraInfo request.
func servicesCheckCameraInfoRequest(t *testing.T, guid [8]byte, seq uint64, frameID string, height, width uint32, distortion string) {
	t.Helper()
	const label = "setcamera request"
	for _, le := range []bool{true, false} {
		enc := servicesNewEncoder(t, le, guid, seq)
		servicesEncodeCameraInfo(t, enc, frameID, height, width, distortion, label)
		wire := enc.Bytes()
		servicesCheckWirePadded(t, wire, label)
		dec := servicesNewDecoder(t, wire, guid, seq)
		servicesDecodeCameraInfo(t, dec, frameID, height, width, distortion, label)
		servicesFinish(t, dec, label)
	}
}

// servicesEncodeSelfTest writes id plus passed plus one status.
func servicesEncodeSelfTest(t *testing.T, enc *types.Encoder, id string, passed bool, level uint8, name, message, label string) {
	t.Helper()
	err := enc.WriteString(id)
	if err != nil {
		t.Fatalf("encode %s id: %v", label, err)
	}
	enc.WriteBool(passed)
	err = enc.WriteSequenceLength(1)
	if err != nil {
		t.Fatalf("encode %s count: %v", label, err)
	}
	enc.WriteUint8(level)
	err = enc.WriteString(name)
	if err != nil {
		t.Fatalf("encode %s name: %v", label, err)
	}
	err = enc.WriteString(message)
	if err != nil {
		t.Fatalf("encode %s status message: %v", label, err)
	}
}

// servicesDecodeSelfTestHead reads id plus passed plus count.
func servicesDecodeSelfTestHead(t *testing.T, dec *types.Decoder, wantID string, wantPassed bool, label string) {
	t.Helper()
	gotID, err := dec.ReadString()
	if err != nil {
		t.Fatalf("read %s id: %v", label, err)
	}
	if gotID != wantID {
		t.Fatalf("read %s id = %q, want %q", label, gotID, wantID)
	}
	gotPassed, err := dec.ReadBool()
	if err != nil {
		t.Fatalf("read %s passed: %v", label, err)
	}
	if gotPassed != wantPassed {
		t.Fatalf("read %s passed = %v, want %v", label, gotPassed, wantPassed)
	}
	gotCount, err := dec.ReadSequenceLength()
	if err != nil {
		t.Fatalf("read %s count: %v", label, err)
	}
	if gotCount != 1 {
		t.Fatalf("read %s count = %d, want 1", label, gotCount)
	}
}

// servicesDecodeSelfTestStatus reads level plus name plus message.
func servicesDecodeSelfTestStatus(t *testing.T, dec *types.Decoder, wantLevel uint8, wantName, wantMessage, label string) {
	t.Helper()
	gotLevel, err := dec.ReadUint8()
	if err != nil {
		t.Fatalf("read %s level: %v", label, err)
	}
	if gotLevel != wantLevel {
		t.Fatalf("read %s level = %d, want %d", label, gotLevel, wantLevel)
	}
	gotName, err := dec.ReadString()
	if err != nil {
		t.Fatalf("read %s name: %v", label, err)
	}
	if gotName != wantName {
		t.Fatalf("read %s name = %q, want %q", label, gotName, wantName)
	}
	gotMessage, err := dec.ReadString()
	if err != nil {
		t.Fatalf("read %s status message: %v", label, err)
	}
	if gotMessage != wantMessage {
		t.Fatalf("read %s status message = %q, want %q", label, gotMessage, wantMessage)
	}
}

// servicesCheckSelfTestResponse round trips a SelfTest response.
func servicesCheckSelfTestResponse(t *testing.T, guid [8]byte, seq uint64, id string, passed bool, level uint8, name, message string) {
	t.Helper()
	const label = "selftest response"
	for _, le := range []bool{true, false} {
		enc := servicesNewEncoder(t, le, guid, seq)
		servicesEncodeSelfTest(t, enc, id, passed, level, name, message, label)
		wire := enc.Bytes()
		servicesCheckWirePadded(t, wire, label)
		dec := servicesNewDecoder(t, wire, guid, seq)
		servicesDecodeSelfTestHead(t, dec, id, passed, label)
		servicesDecodeSelfTestStatus(t, dec, level, name, message, label)
		servicesFinish(t, dec, label)
	}
}

// TestServicesCorpusPinsAllTen requires all ten service entries.
func TestServicesCorpusPinsAllTen(t *testing.T) {
	t.Parallel()
	for _, pair := range servicesPinned() {
		entry := servicesCheckCorpus(t, pair[0], pair[1])
		if strings.TrimSpace(entry.Summary) == "" {
			t.Fatalf("corpus %s/%s summary is empty, no shape to pin", pair[0], pair[1])
		}
	}
	if len(servicesPinned()) != 10 {
		t.Fatalf("pinned services = %d, want 10", len(servicesPinned()))
	}
}

// TestServicesStdSrvsEmpty pins std_srvs Empty request and response.
func TestServicesStdSrvsEmpty(t *testing.T) {
	t.Parallel()
	servicesCheckCorpus(t, "std_srvs", "Empty")
	guid := servicesTestGUID()
	t.Run("request", func(t *testing.T) {
		t.Parallel()
		servicesCheckEmpty(t, guid, servicesTestSeq, "empty request")
	})
	t.Run("response", func(t *testing.T) {
		t.Parallel()
		servicesCheckEmpty(t, guid, servicesTestSeq+1, "empty response")
	})
}

// TestServicesStdSrvsTrigger pins std_srvs Trigger request and response.
func TestServicesStdSrvsTrigger(t *testing.T) {
	t.Parallel()
	servicesCheckCorpus(t, "std_srvs", "Trigger")
	guid := servicesTestGUID()
	t.Run("request", func(t *testing.T) {
		t.Parallel()
		servicesCheckEmpty(t, guid, servicesTestSeq+2, "trigger request")
	})
	t.Run("response", func(t *testing.T) {
		t.Parallel()
		servicesCheckBoolStringResponse(t, guid, servicesTestSeq+3, "trigger response", true, "trigger ok")
	})
}

// TestServicesStdSrvsSetBool pins std_srvs SetBool request and response.
func TestServicesStdSrvsSetBool(t *testing.T) {
	t.Parallel()
	servicesCheckCorpus(t, "std_srvs", "SetBool")
	guid := servicesTestGUID()
	t.Run("request", func(t *testing.T) {
		t.Parallel()
		servicesCheckBoolRequest(t, guid, servicesTestSeq+4, "setbool request", true)
	})
	t.Run("response", func(t *testing.T) {
		t.Parallel()
		servicesCheckBoolStringResponse(t, guid, servicesTestSeq+5, "setbool response", false, "setbool ok")
	})
}

// TestServicesNavGetMap pins nav_msgs GetMap request and response.
func TestServicesNavGetMap(t *testing.T) {
	t.Parallel()
	servicesCheckCorpus(t, "nav_msgs", "GetMap")
	guid := servicesTestGUID()
	occ := servicesOccupancyBase()
	t.Run("request", func(t *testing.T) {
		t.Parallel()
		servicesCheckEmpty(t, guid, servicesTestSeq+10, "getmap request")
	})
	t.Run("response", func(t *testing.T) {
		t.Parallel()
		servicesRoundTripOccupancy(t, guid, servicesTestSeq+11, "getmap response", occ)
	})
}

// TestServicesNavGetPlan pins nav_msgs GetPlan request and response.
func TestServicesNavGetPlan(t *testing.T) {
	t.Parallel()
	servicesCheckCorpus(t, "nav_msgs", "GetPlan")
	guid := servicesTestGUID()
	start := servicesPoseBase("map", 1.0)
	goal := servicesPoseBase("map", 9.0)
	tolerance := float32(0.5)
	planFrame := "map"
	planPoses := [][7]float64{
		{1.0, 2.0, 0.0, 0.0, 0.0, 0.0, 1.0},
		{9.0, 2.0, 0.0, 0.0, 0.0, 0.0, 1.0},
	}
	t.Run("request", func(t *testing.T) {
		t.Parallel()
		servicesCheckGetPlanRequest(t, guid, servicesTestSeq+12, start, goal, tolerance)
	})
	t.Run("response", func(t *testing.T) {
		t.Parallel()
		servicesCheckGetPlanResponse(t, guid, servicesTestSeq+13, planFrame, planPoses)
	})
}

// TestServicesNavLoadMap pins nav_msgs LoadMap request and response.
func TestServicesNavLoadMap(t *testing.T) {
	t.Parallel()
	servicesCheckCorpus(t, "nav_msgs", "LoadMap")
	guid := servicesTestGUID()
	occ := servicesOccupancyBase()
	mapURL := "file:///tmp/test_map.yaml"
	result := uint8(0)
	t.Run("request", func(t *testing.T) {
		t.Parallel()
		servicesCheckStringRequest(t, guid, servicesTestSeq+14, "loadmap request", mapURL)
	})
	t.Run("response", func(t *testing.T) {
		t.Parallel()
		servicesCheckLoadMapResponse(t, guid, servicesTestSeq+15, occ, result)
	})
}

// TestServicesNavSetMap pins nav_msgs SetMap request and response.
func TestServicesNavSetMap(t *testing.T) {
	t.Parallel()
	servicesCheckCorpus(t, "nav_msgs", "SetMap")
	guid := servicesTestGUID()
	occ := servicesOccupancyBase()
	initial := servicesPoseBase("map", 3.0)
	var covariance [36]float64
	covariance[0] = 0.25
	covariance[7] = 0.25
	covariance[35] = 0.0688
	t.Run("request", func(t *testing.T) {
		t.Parallel()
		servicesCheckSetMapRequest(t, guid, servicesTestSeq+16, occ, initial, covariance)
	})
	t.Run("response", func(t *testing.T) {
		t.Parallel()
		servicesCheckBoolRequest(t, guid, servicesTestSeq+17, "setmap response", true)
	})
}

// TestServicesSensorSetCameraInfo pins sensor_msgs SetCameraInfo.
func TestServicesSensorSetCameraInfo(t *testing.T) {
	t.Parallel()
	servicesCheckCorpus(t, "sensor_msgs", "SetCameraInfo")
	guid := servicesTestGUID()
	frameID := "camera"
	height := uint32(480)
	width := uint32(640)
	distortion := "plumb_bob"
	t.Run("request", func(t *testing.T) {
		t.Parallel()
		servicesCheckCameraInfoRequest(t, guid, servicesTestSeq+18, frameID, height, width, distortion)
	})
	t.Run("response", func(t *testing.T) {
		t.Parallel()
		servicesCheckBoolStringResponse(t, guid, servicesTestSeq+19, "setcamera response", true, "calibration ok")
	})
}

// TestServicesDiagSelfTest pins diagnostic_msgs SelfTest request and response.
func TestServicesDiagSelfTest(t *testing.T) {
	t.Parallel()
	servicesCheckCorpus(t, "diagnostic_msgs", "SelfTest")
	guid := servicesTestGUID()
	id := "weld_self_test"
	passed := true
	level := uint8(0)
	statusName := "weld check"
	statusMessage := "ok"
	t.Run("request", func(t *testing.T) {
		t.Parallel()
		servicesCheckEmpty(t, guid, servicesTestSeq+20, "selftest request")
	})
	t.Run("response", func(t *testing.T) {
		t.Parallel()
		servicesCheckSelfTestResponse(t, guid, servicesTestSeq+21, id, passed, level, statusName, statusMessage)
	})
}

// TestServicesDiagAddDiagnostics pins diagnostic_msgs AddDiagnostics.
func TestServicesDiagAddDiagnostics(t *testing.T) {
	t.Parallel()
	servicesCheckCorpus(t, "diagnostic_msgs", "AddDiagnostics")
	guid := servicesTestGUID()
	t.Run("request", func(t *testing.T) {
		t.Parallel()
		servicesCheckStringRequest(t, guid, servicesTestSeq+22, "adddiagnostics request", "weld_ns")
	})
	t.Run("response", func(t *testing.T) {
		t.Parallel()
		servicesCheckBoolStringResponse(t, guid, servicesTestSeq+23, "adddiagnostics response", true, "added")
	})
}

// TestServicesEnvelopeRebase pins the 20 byte service prefix rebase.
func TestServicesEnvelopeRebase(t *testing.T) {
	t.Parallel()
	guid := servicesTestGUID()
	for _, le := range []bool{true, false} {
		enc := servicesNewEncoder(t, le, guid, servicesTestSeq+30)
		if enc.Len() != types.ServiceEnvelopeSize {
			t.Fatalf("le=%v encoder len = %d, want %d", le, enc.Len(), types.ServiceEnvelopeSize)
		}
		if types.ServiceEnvelopeSize != types.HeaderSize+types.ServiceHeaderSize {
			t.Fatalf("envelope size = %d, want header %d plus service header %d", types.ServiceEnvelopeSize, types.HeaderSize, types.ServiceHeaderSize)
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
		if gotSeq != servicesTestSeq+30 {
			t.Fatalf("le=%v seq = %d, want %d", le, gotSeq, servicesTestSeq+30)
		}
		if dec.Offset() != types.ServiceEnvelopeSize {
			t.Fatalf("le=%v offset = %d, want %d", le, dec.Offset(), types.ServiceEnvelopeSize)
		}
		servicesDecodeEmpty(t, wire, guid, servicesTestSeq+30, "rebase empty")
	}
}

// TestServicesTopicKinds pins rq and rr mapping for all ten services.
func TestServicesTopicKinds(t *testing.T) {
	t.Parallel()
	services := []string{
		"/empty",
		"/set_bool",
		"/trigger",
		"/get_map",
		"/get_plan",
		"/load_map",
		"/set_map",
		"/set_camera_info",
		"/self_test",
		"/add_diagnostics",
	}
	if len(services) != 10 {
		t.Fatalf("topic services = %d, want 10", len(services))
	}
	for _, service := range services {
		servicesCheckServiceKind(t, service)
	}
}
