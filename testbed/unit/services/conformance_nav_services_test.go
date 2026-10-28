// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package unit_services

import (
	"strings"
	"testing"

	"github.com/podomy/weld/internal/types"
)

// Nav service unit sentinels for pure Go XCDR checks with no Docker.
//
// Resolution and cell sizes stay at exact binary fractions so LE and BE
// codec checks are stable. Width 3 plus height 1 with data [0, 100, -1]
// covers free plus occupied plus unknown in row major order. Origin stays
// at fixed 1.0 plus 2.0 plus 3.0 with identity orientation. Tolerance is a
// fixed 0.5 float32. Map URL is a fixed package URL string.
const (
	navSvcResolution float32 = 0.5
	navSvcTolerance  float32 = 0.5
	navSvcWidth      uint32  = 3
	navSvcHeight     uint32  = 1
	navSvcData0      int8    = 0
	navSvcData1      int8    = 100
	navSvcData2      int8    = -1
	navSvcSeq        uint64  = 7
	navSvcFrame      string  = "weld-nav-svc"
	navSvcMapURL     string  = "package://weld_maps/floor1.yaml"
	navSvcCovLen     int     = 36
)

// navUnitGUID returns a fixed 8 byte service GUID for tests.
func navUnitGUID() [8]byte {
	var guid [8]byte
	for i := range guid {
		guid[i] = byte(i + 1)
	}
	return guid
}

// navUnitCheckCorpus returns the corpus entry for a nav service or fails.
func navUnitCheckCorpus(t *testing.T, name string) types.CorpusEntry {
	t.Helper()
	entry, ok := types.Lookup("nav_msgs", name)
	if !ok {
		t.Fatalf("corpus misses nav_msgs/%s, no shape to pin", name)
	}
	if entry.Kind != types.KindService {
		t.Fatalf("corpus nav_msgs/%s kind %q is not service", name, entry.Kind)
	}
	return entry
}

// navUnitRequireSummary fails when the summary misses a required fragment.
func navUnitRequireSummary(t *testing.T, entry types.CorpusEntry, fragments ...string) {
	t.Helper()
	summary := strings.ToLower(entry.Summary)
	for _, fragment := range fragments {
		lower := strings.ToLower(fragment)
		if !strings.Contains(summary, lower) {
			t.Fatalf("corpus %s/%s Summary %q misses %q", entry.Package, entry.Name, entry.Summary, fragment)
		}
	}
}

// navUnitNewEncoder returns an encoder with the service header written.
func navUnitNewEncoder(t *testing.T, le bool, guid [8]byte, seq uint64) *types.Encoder {
	t.Helper()
	enc := types.NewEncoder(le)
	err := enc.WriteServiceHeader(guid, seq)
	if err != nil {
		t.Fatalf("write service header: %v", err)
	}
	return enc
}

// navUnitFinish checks strict consumption with zero drops.
func navUnitFinish(t *testing.T, dec *types.Decoder, label string) {
	t.Helper()
	err := dec.Finish()
	if err != nil {
		t.Fatalf("finish %s: %v", label, err)
	}
	if dec.Dropped() != 0 {
		t.Fatalf("decoder %s drops = %d, want 0", label, dec.Dropped())
	}
}

// navUnitEncodeHeader writes Header stamp plus frame_id.
func navUnitEncodeHeader(t *testing.T, enc *types.Encoder, frame string) {
	t.Helper()
	enc.WriteInt32(0)
	enc.WriteUint32(0)
	err := enc.WriteString(frame)
	if err != nil {
		t.Fatalf("encode header frame: %v", err)
	}
}

// navUnitDecodeHeader reads Header stamp plus frame_id and checks the frame.
func navUnitDecodeHeader(t *testing.T, dec *types.Decoder, label, want string) {
	t.Helper()
	sec, err := dec.ReadInt32()
	if err != nil {
		t.Fatalf("decode %s header sec: %v", label, err)
	}
	nsec, err := dec.ReadUint32()
	if err != nil {
		t.Fatalf("decode %s header nanosec: %v", label, err)
	}
	if sec != 0 || nsec != 0 {
		t.Fatalf("%s header stamp (%d, %d) wants (0, 0)", label, sec, nsec)
	}
	frame, err := dec.ReadString()
	if err != nil {
		t.Fatalf("decode %s header frame: %v", label, err)
	}
	if frame != want {
		t.Fatalf("%s frame %q wants %q", label, frame, want)
	}
}

// navUnitEncodePose writes Point plus Quaternion in Pose order.
func navUnitEncodePose(enc *types.Encoder) {
	enc.WriteFloat64(1.0)
	enc.WriteFloat64(2.0)
	enc.WriteFloat64(3.0)
	enc.WriteFloat64(0.0)
	enc.WriteFloat64(0.0)
	enc.WriteFloat64(0.0)
	enc.WriteFloat64(1.0)
}

// navUnitDecodePosition reads Point xyz and checks sentinels.
func navUnitDecodePosition(t *testing.T, dec *types.Decoder, label string) {
	t.Helper()
	want := [3]float64{1.0, 2.0, 3.0}
	for i := range want {
		got, err := dec.ReadFloat64()
		if err != nil {
			t.Fatalf("decode %s position %d: %v", label, i, err)
		}
		if got != want[i] {
			t.Fatalf("%s position %d = %v, want %v", label, i, got, want[i])
		}
	}
}

// navUnitDecodeOrientation reads Quaternion xyzw and checks sentinels.
func navUnitDecodeOrientation(t *testing.T, dec *types.Decoder, label string) {
	t.Helper()
	want := [4]float64{0.0, 0.0, 0.0, 1.0}
	for i := range want {
		got, err := dec.ReadFloat64()
		if err != nil {
			t.Fatalf("decode %s orientation %d: %v", label, i, err)
		}
		if got != want[i] {
			t.Fatalf("%s orientation %d = %v, want %v", label, i, got, want[i])
		}
	}
}

// navUnitDecodePose reads Point plus Quaternion and checks sentinels.
func navUnitDecodePose(t *testing.T, dec *types.Decoder, label string) {
	t.Helper()
	navUnitDecodePosition(t, dec, label)
	navUnitDecodeOrientation(t, dec, label)
}

// navUnitEncodeCovariance writes 36 zero covariance entries.
func navUnitEncodeCovariance(enc *types.Encoder) {
	for range navSvcCovLen {
		enc.WriteFloat64(0.0)
	}
}

// navUnitDecodeCovariance reads 36 entries and checks all zeros.
func navUnitDecodeCovariance(t *testing.T, dec *types.Decoder, label string) {
	t.Helper()
	for i := range navSvcCovLen {
		v, err := dec.ReadFloat64()
		if err != nil {
			t.Fatalf("decode %s covariance %d: %v", label, i, err)
		}
		if v != 0.0 {
			t.Fatalf("%s covariance %d = %v wants 0.0", label, i, v)
		}
	}
}

// navUnitEncodeInfo writes MapMetaData info with fixed sentinels.
func navUnitEncodeInfo(enc *types.Encoder) {
	enc.WriteInt32(0)
	enc.WriteUint32(0)
	enc.WriteFloat32(navSvcResolution)
	enc.WriteUint32(navSvcWidth)
	enc.WriteUint32(navSvcHeight)
	navUnitEncodePose(enc)
}

// navUnitDecodeInfo checks stamp plus resolution plus size plus origin.
func navUnitDecodeInfo(t *testing.T, dec *types.Decoder, label string) {
	t.Helper()
	sec, err := dec.ReadInt32()
	if err != nil {
		t.Fatalf("decode %s stamp sec: %v", label, err)
	}
	nsec, err := dec.ReadUint32()
	if err != nil {
		t.Fatalf("decode %s stamp nanosec: %v", label, err)
	}
	if sec != 0 || nsec != 0 {
		t.Fatalf("%s stamp (%d, %d) wants (0, 0)", label, sec, nsec)
	}
	res, err := dec.ReadFloat32()
	if err != nil {
		t.Fatalf("decode %s resolution: %v", label, err)
	}
	width, err := dec.ReadUint32()
	if err != nil {
		t.Fatalf("decode %s width: %v", label, err)
	}
	height, err := dec.ReadUint32()
	if err != nil {
		t.Fatalf("decode %s height: %v", label, err)
	}
	if res != navSvcResolution || width != navSvcWidth || height != navSvcHeight {
		t.Fatalf("%s info (%v, %v, %v) wants (0.5, 3, 1)", label, res, width, height)
	}
	navUnitDecodePose(t, dec, label+" origin")
}

// navUnitEncodeGridData writes the fixed [0, 100, -1] data sequence.
func navUnitEncodeGridData(t *testing.T, enc *types.Encoder) {
	t.Helper()
	err := enc.WriteSequenceLength(3)
	if err != nil {
		t.Fatalf("encode grid data len: %v", err)
	}
	enc.WriteInt8(navSvcData0)
	enc.WriteInt8(navSvcData1)
	enc.WriteInt8(navSvcData2)
}

// navUnitDecodeGridData checks the fixed data sequence with no pad.
func navUnitDecodeGridData(t *testing.T, dec *types.Decoder, label string) {
	t.Helper()
	n, err := dec.ReadSequenceLength()
	if err != nil {
		t.Fatalf("decode %s data len: %v", label, err)
	}
	if n != 3 {
		t.Fatalf("%s data len %d wants 3", label, n)
	}
	v0, err := dec.ReadInt8()
	if err != nil {
		t.Fatalf("decode %s data 0: %v", label, err)
	}
	v1, err := dec.ReadInt8()
	if err != nil {
		t.Fatalf("decode %s data 1: %v", label, err)
	}
	v2, err := dec.ReadInt8()
	if err != nil {
		t.Fatalf("decode %s data 2: %v", label, err)
	}
	if v0 != navSvcData0 || v1 != navSvcData1 || v2 != navSvcData2 {
		t.Fatalf("%s data (%v, %v, %v) wants (0, 100, -1)", label, v0, v1, v2)
	}
}

// navUnitEncodeGridBody writes header plus info plus data.
func navUnitEncodeGridBody(t *testing.T, enc *types.Encoder, frame string) {
	t.Helper()
	navUnitEncodeHeader(t, enc, frame)
	navUnitEncodeInfo(enc)
	navUnitEncodeGridData(t, enc)
}

// navUnitDecodeGridBody reads header plus info plus data.
func navUnitDecodeGridBody(t *testing.T, dec *types.Decoder, frame, label string) {
	t.Helper()
	navUnitDecodeHeader(t, dec, label+" grid", frame)
	navUnitDecodeInfo(t, dec, label+" grid")
	navUnitDecodeGridData(t, dec, label+" grid")
}

// navUnitEncodePoseStamped writes header plus pose.
func navUnitEncodePoseStamped(t *testing.T, enc *types.Encoder, frame string) {
	t.Helper()
	navUnitEncodeHeader(t, enc, frame)
	navUnitEncodePose(enc)
}

// navUnitDecodePoseStamped reads header plus pose.
func navUnitDecodePoseStamped(t *testing.T, dec *types.Decoder, frame, label string) {
	t.Helper()
	navUnitDecodeHeader(t, dec, label, frame)
	navUnitDecodePose(t, dec, label)
}

// navUnitCheckPadZero drains Remaining bytes and checks all zeros.
func navUnitCheckPadZero(t *testing.T, dec *types.Decoder, label string) {
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
	if dec.Dropped() != 0 {
		t.Fatalf("decoder %s drops = %d, want 0", label, dec.Dropped())
	}
}

// TestNavServiceCorpus pins nav service corpus kinds plus summaries.
func TestNavServiceCorpus(t *testing.T) {
	t.Parallel()
	getMap := navUnitCheckCorpus(t, "GetMap")
	navUnitRequireSummary(t, getMap, "request empty", "OccupancyGrid map")
	getPlan := navUnitCheckCorpus(t, "GetPlan")
	navUnitRequireSummary(t, getPlan, "PoseStamped start", "float32 tolerance", "Path plan")
	loadMap := navUnitCheckCorpus(t, "LoadMap")
	navUnitRequireSummary(t, loadMap, "string map_url", "OccupancyGrid map", "uint8 result")
	setMap := navUnitCheckCorpus(t, "SetMap")
	navUnitRequireSummary(t, setMap, "OccupancyGrid map", "PoseWithCovarianceStamped", "bool success")
}

// TestNavServiceTopics pins nav services to rq plus rr, never to rt.
func TestNavServiceTopics(t *testing.T) {
	t.Parallel()
	servicesCheckServiceKind(t, "/get_map")
	servicesCheckServiceKind(t, "/get_plan")
	servicesCheckServiceKind(t, "/load_map")
	servicesCheckServiceKind(t, "/set_map")
}

// TestNavGetMap pins GetMap empty request plus OccupancyGrid response.
func TestNavGetMap(t *testing.T) {
	t.Parallel()
	guid := navUnitGUID()
	for _, le := range []bool{true, false} {
		encReq := navUnitNewEncoder(t, le, guid, navSvcSeq)
		wireReq := encReq.Bytes()
		if len(wireReq)%4 != 0 {
			t.Fatalf("getmap request wire length %d is not padded to 4", len(wireReq))
		}
		decReq := servicesNewDecoder(t, wireReq, guid, navSvcSeq)
		navUnitCheckPadZero(t, decReq, "getmap request")
		navUnitFinish(t, decReq, "getmap request")

		encResp := navUnitNewEncoder(t, le, guid, navSvcSeq+1)
		navUnitEncodeGridBody(t, encResp, navSvcFrame)
		wireResp := encResp.Bytes()
		if len(wireResp)%4 != 0 {
			t.Fatalf("getmap response wire length %d is not padded to 4", len(wireResp))
		}
		decResp := servicesNewDecoder(t, wireResp, guid, navSvcSeq+1)
		navUnitDecodeGridBody(t, decResp, navSvcFrame, "getmap response")
		navUnitCheckPadZero(t, decResp, "getmap response")
		navUnitFinish(t, decResp, "getmap response")
	}
}

// TestNavGetPlan pins GetPlan start plus goal plus tolerance and Path plan.
func TestNavGetPlan(t *testing.T) {
	t.Parallel()
	guid := navUnitGUID()
	for _, le := range []bool{true, false} {
		encReq := navUnitNewEncoder(t, le, guid, navSvcSeq+4)
		navUnitEncodePoseStamped(t, encReq, "start")
		navUnitEncodePoseStamped(t, encReq, "goal")
		encReq.WriteFloat32(navSvcTolerance)
		wireReq := encReq.Bytes()
		decReq := servicesNewDecoder(t, wireReq, guid, navSvcSeq+4)
		navUnitDecodePoseStamped(t, decReq, "start", "getplan request start")
		navUnitDecodePoseStamped(t, decReq, "goal", "getplan request goal")
		tol, err := decReq.ReadFloat32()
		if err != nil {
			t.Fatalf("read getplan request tolerance: %v", err)
		}
		if tol != navSvcTolerance {
			t.Fatalf("read getplan request tolerance = %v, want %v", tol, navSvcTolerance)
		}
		navUnitFinish(t, decReq, "getplan request")

		encResp := navUnitNewEncoder(t, le, guid, navSvcSeq+5)
		navUnitEncodeHeader(t, encResp, "plan")
		err = encResp.WriteSequenceLength(1)
		if err != nil {
			t.Fatalf("encode getplan poses len: %v", err)
		}
		navUnitEncodePoseStamped(t, encResp, "plan-pose")
		wireResp := encResp.Bytes()
		decResp := servicesNewDecoder(t, wireResp, guid, navSvcSeq+5)
		navUnitDecodeHeader(t, decResp, "getplan response plan", "plan")
		n, err := decResp.ReadSequenceLength()
		if err != nil {
			t.Fatalf("read getplan response poses len: %v", err)
		}
		if n != 1 {
			t.Fatalf("read getplan response poses len = %d, want 1", n)
		}
		navUnitDecodePoseStamped(t, decResp, "plan-pose", "getplan response pose")
		navUnitFinish(t, decResp, "getplan response")
	}
}

// TestNavLoadMap pins LoadMap map_url request plus grid plus result response.
func TestNavLoadMap(t *testing.T) {
	t.Parallel()
	guid := navUnitGUID()
	for _, le := range []bool{true, false} {
		encReq := navUnitNewEncoder(t, le, guid, navSvcSeq+2)
		err := encReq.WriteString(navSvcMapURL)
		if err != nil {
			t.Fatalf("encode loadmap url: %v", err)
		}
		wireReq := encReq.Bytes()
		decReq := servicesNewDecoder(t, wireReq, guid, navSvcSeq+2)
		got, err := decReq.ReadString()
		if err != nil {
			t.Fatalf("read loadmap request url: %v", err)
		}
		if got != navSvcMapURL {
			t.Fatalf("read loadmap request url = %q, want %q", got, navSvcMapURL)
		}
		navUnitFinish(t, decReq, "loadmap request")

		encResp := navUnitNewEncoder(t, le, guid, navSvcSeq+3)
		navUnitEncodeGridBody(t, encResp, navSvcFrame)
		encResp.WriteUint8(0)
		wireResp := encResp.Bytes()
		decResp := servicesNewDecoder(t, wireResp, guid, navSvcSeq+3)
		navUnitDecodeGridBody(t, decResp, navSvcFrame, "loadmap response")
		result, err := decResp.ReadUint8()
		if err != nil {
			t.Fatalf("read loadmap response result: %v", err)
		}
		if result != 0 {
			t.Fatalf("read loadmap response result = %d, want 0", result)
		}
		navUnitCheckPadZero(t, decResp, "loadmap response")
		navUnitFinish(t, decResp, "loadmap response")
	}
}

// TestNavSetMap pins SetMap grid plus init pose request and bool response.
func TestNavSetMap(t *testing.T) {
	t.Parallel()
	guid := navUnitGUID()
	for _, le := range []bool{true, false} {
		encReq := navUnitNewEncoder(t, le, guid, navSvcSeq+6)
		navUnitEncodeGridBody(t, encReq, navSvcFrame)
		navUnitEncodeHeader(t, encReq, "init")
		navUnitEncodePose(encReq)
		navUnitEncodeCovariance(encReq)
		wireReq := encReq.Bytes()
		decReq := servicesNewDecoder(t, wireReq, guid, navSvcSeq+6)
		navUnitDecodeGridBody(t, decReq, navSvcFrame, "setmap request")
		navUnitDecodeHeader(t, decReq, "setmap request init", "init")
		navUnitDecodePose(t, decReq, "setmap request init pose")
		navUnitDecodeCovariance(t, decReq, "setmap request init covariance")
		if decReq.Dropped() != 0 {
			t.Fatalf("decoder setmap request drops = %d, want 0", decReq.Dropped())
		}
		navUnitFinish(t, decReq, "setmap request")

		encResp := navUnitNewEncoder(t, le, guid, navSvcSeq+7)
		encResp.WriteBool(true)
		wireResp := encResp.Bytes()
		if len(wireResp)%4 != 0 {
			t.Fatalf("setmap response wire length %d is not padded to 4", len(wireResp))
		}
		decResp := servicesNewDecoder(t, wireResp, guid, navSvcSeq+7)
		got, err := decResp.ReadBool()
		if err != nil {
			t.Fatalf("read setmap response success: %v", err)
		}
		if got != true {
			t.Fatalf("read setmap response success = %v, want true", got)
		}
		navUnitCheckPadZero(t, decResp, "setmap response")
		navUnitFinish(t, decResp, "setmap response")
	}
}
