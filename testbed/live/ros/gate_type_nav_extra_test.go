// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

//nolint:dupl,gocognit // test breadth repeats similar wire checks across types
package ros_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/podomy/weld/internal/transports/dds"
	"github.com/podomy/weld/internal/types"
	"github.com/podomy/weld/testbed/live/helpers"
)

// Extra nav static type hashes from ros2 topic info -v on weld-jazzy.
//
// These are the RIHS01 hashes reported for each ROS type on domain 42.
// Live probing via ros2 topic info -v is preferred at runtime, these values
// are the fallback when the probe cannot run. Values were captured from
// weld-jazzy Jazzy. Odometry and Path already live in gate_type_nav_test.go,
// so this file covers OccupancyGrid, MapMetaData, and GridCells only.
// GetMap, SetMap, LoadMap, and GetPlan are services and have no topic gate:
// requests travel on rq request DDS topics and replies travel on rr reply
// DDS topics, never on the rt topic path. Service shapes are pinned with
// pure Go XCDR round trips below and no Docker.
const (
	navExtraHashOccupancyGrid = "RIHS01_8d348150c12913a31ee0ec170fbf25089e4745d17035792a1ba94d6f0bc0cfc7"
	navExtraHashMapMetaData   = "RIHS01_2772d4b2000ef2b35dbaeb80fd3946c1369f817fb4f75677d916d27c17d763c8"
	navExtraHashGridCells     = "RIHS01_bb99c2f5d0a04750745a81ec6a8147aa373cce5bd17c8cd6507f2413354a6933"
)

// Extra nav fixed sentinels shared by YAML, encode, and decode.
//
// OccupancyGrid and GridCells carry a Header, so the per-run live ID rides
// in frame_id and stale runs never match. MapMetaData carries no Header and
// no string field, so isolation uses per-run topic plus domain 42, matching
// the Vector3 matrix case. Resolution and cell sizes stay at exact binary
// fractions so local codec checks are stable. Width 3 plus height 1 with
// data [0, 100, -1] covers free plus occupied plus unknown in row major
// order. Origin stays at fixed 1.0 plus 2.0 plus 3.0 with identity
// orientation.
const (
	navExtraResolution float32 = 0.5
	navExtraCellWidth  float32 = 0.5
	navExtraCellHeight float32 = 0.5
	navExtraWidth      uint32  = 3
	navExtraHeight     uint32  = 1
	navExtraData0      int8    = 0
	navExtraData1      int8    = 100
	navExtraData2      int8    = -1
)

// yamlNavExtraMapMetaData returns ROS YAML for the fixed MapMetaData.
func yamlNavExtraMapMetaData(live string) string {
	return "{map_load_time: {sec: 0, nanosec: 0}, resolution: 0.5, width: 3, height: 1, origin: {position: {x: 1.0, y: 2.0, z: 3.0}, orientation: {x: 0.0, y: 0.0, z: 0.0, w: 1.0}}}"
}

// yamlNavExtraGridCells returns ROS YAML with live frame_id.
func yamlNavExtraGridCells(live string) string {
	return fmt.Sprintf("{header: {stamp: {sec: 0, nanosec: 0}, frame_id: '%s'}, cell_width: 0.5, cell_height: 0.5, cells: [{x: 1.0, y: 2.0, z: 3.0}]}", live)
}

// yamlNavExtraOccupancyGrid returns ROS YAML with live frame_id.
func yamlNavExtraOccupancyGrid(live string) string {
	return fmt.Sprintf("{header: {stamp: {sec: 0, nanosec: 0}, frame_id: '%s'}, info: {map_load_time: {sec: 0, nanosec: 0}, resolution: 0.5, width: 3, height: 1, origin: {position: {x: 1.0, y: 2.0, z: 3.0}, orientation: {x: 0.0, y: 0.0, z: 0.0, w: 1.0}}}, data: [0, 100, -1]}", live)
}

// encodeNavExtraInfo writes MapMetaData info with fixed sentinels.
func encodeNavExtraInfo(enc *types.Encoder) {
	enc.WriteInt32(0)
	enc.WriteUint32(0)
	enc.WriteFloat32(navExtraResolution)
	enc.WriteUint32(navExtraWidth)
	enc.WriteUint32(navExtraHeight)
	encodeNavPose(enc)
}

// decodeNavExtraStamp checks map_load_time sec plus nanosec.
func decodeNavExtraStamp(dec *types.Decoder, label string) error {
	sec, err := dec.ReadInt32()
	if err != nil {
		return fmt.Errorf("decode %s stamp sec: %w", label, err)
	}
	nsec, err := dec.ReadUint32()
	if err != nil {
		return fmt.Errorf("decode %s stamp nanosec: %w", label, err)
	}
	if sec != 0 || nsec != 0 {
		return fmt.Errorf("%s stamp (%d, %d) wants (0, 0)", label, sec, nsec)
	}
	return nil
}

// decodeNavExtraRes checks resolution plus width plus height.
func decodeNavExtraRes(dec *types.Decoder, label string) error {
	res, err := dec.ReadFloat32()
	if err != nil {
		return fmt.Errorf("decode %s resolution: %w", label, err)
	}
	width, err := dec.ReadUint32()
	if err != nil {
		return fmt.Errorf("decode %s width: %w", label, err)
	}
	height, err := dec.ReadUint32()
	if err != nil {
		return fmt.Errorf("decode %s height: %w", label, err)
	}
	if res != navExtraResolution || width != navExtraWidth || height != navExtraHeight {
		return fmt.Errorf("%s info (%v, %v, %v) wants (0.5, 3, 1)", label, res, width, height)
	}
	return nil
}

// decodeNavExtraInfo checks stamp plus resolution plus size plus origin.
func decodeNavExtraInfo(dec *types.Decoder, label string) error {
	err := decodeNavExtraStamp(dec, label)
	if err != nil {
		return err
	}
	err = decodeNavExtraRes(dec, label)
	if err != nil {
		return err
	}
	err = decodeNavPose(dec, label+" origin")
	if err != nil {
		return err
	}
	return nil
}

// encodeNavExtraMapMetaData encodes fixed MapMetaData as XCDR1.
func encodeNavExtraMapMetaData(live string) ([]byte, error) {
	enc := types.NewEncoder(true)
	encodeNavExtraInfo(enc)
	return enc.Bytes(), nil
}

// decodeNavExtraMapMetaData decodes and checks fixed MapMetaData sentinels.
func decodeNavExtraMapMetaData(payload []byte, live string) error {
	dec, err := types.NewDecoder(payload)
	if err != nil {
		return fmt.Errorf("decode mapmetadata header: %w", err)
	}
	err = decodeNavExtraInfo(dec, "mapmetadata")
	if err != nil {
		return err
	}
	err = dec.Finish()
	if err != nil {
		return fmt.Errorf("finish mapmetadata: %w", err)
	}
	return nil
}

// wantNavExtraMapMetaData returns the fixed echo substring.
func wantNavExtraMapMetaData(live string) string {
	return "resolution: 0.5"
}

// encodeNavExtraGridCells encodes GridCells with live frame_id as XCDR1.
func encodeNavExtraGridCells(live string) ([]byte, error) {
	enc := types.NewEncoder(true)
	err := encodeMatrixHeader(enc, live)
	if err != nil {
		return nil, fmt.Errorf("encode gridcells header: %w", err)
	}
	enc.WriteFloat32(navExtraCellWidth)
	enc.WriteFloat32(navExtraCellHeight)
	err = enc.WriteSequenceLength(1)
	if err != nil {
		return nil, fmt.Errorf("encode gridcells cells len: %w", err)
	}
	enc.WriteFloat64(matrixPointX)
	enc.WriteFloat64(matrixPointY)
	enc.WriteFloat64(matrixPointZ)
	return enc.Bytes(), nil
}

// decodeNavExtraCellSize checks cell_width plus cell_height.
func decodeNavExtraCellSize(dec *types.Decoder) error {
	width, err := dec.ReadFloat32()
	if err != nil {
		return fmt.Errorf("decode gridcells cell_width: %w", err)
	}
	height, err := dec.ReadFloat32()
	if err != nil {
		return fmt.Errorf("decode gridcells cell_height: %w", err)
	}
	if width != navExtraCellWidth || height != navExtraCellHeight {
		return fmt.Errorf("gridcells cell size (%v, %v) wants (0.5, 0.5)", width, height)
	}
	return nil
}

// decodeNavExtraCellPoint checks the single cell point.
func decodeNavExtraCellPoint(dec *types.Decoder) error {
	n, err := dec.ReadSequenceLength()
	if err != nil {
		return fmt.Errorf("decode gridcells cells len: %w", err)
	}
	if n != 1 {
		return fmt.Errorf("gridcells cells len %d wants 1", n)
	}
	x, err := dec.ReadFloat64()
	if err != nil {
		return fmt.Errorf("decode gridcells cell x: %w", err)
	}
	y, err := dec.ReadFloat64()
	if err != nil {
		return fmt.Errorf("decode gridcells cell y: %w", err)
	}
	z, err := dec.ReadFloat64()
	if err != nil {
		return fmt.Errorf("decode gridcells cell z: %w", err)
	}
	if x != matrixPointX || y != matrixPointY || z != matrixPointZ {
		return fmt.Errorf("gridcells cell (%v, %v, %v) wants (1.0, 2.0, 3.0)", x, y, z)
	}
	return nil
}

// decodeNavExtraGridCells decodes and checks live frame_id plus sentinels.
func decodeNavExtraGridCells(payload []byte, live string) error {
	dec, err := types.NewDecoder(payload)
	if err != nil {
		return fmt.Errorf("decode gridcells header: %w", err)
	}
	frame, err := decodeMatrixHeader(dec)
	if err != nil {
		return fmt.Errorf("decode gridcells frame: %w", err)
	}
	if frame != live {
		return fmt.Errorf("gridcells frame %q wants %q", frame, live)
	}
	err = decodeNavExtraCellSize(dec)
	if err != nil {
		return err
	}
	err = decodeNavExtraCellPoint(dec)
	if err != nil {
		return err
	}
	err = dec.Finish()
	if err != nil {
		return fmt.Errorf("finish gridcells: %w", err)
	}
	return nil
}

// wantNavExtraGridCells returns the live ID expected in echo output.
func wantNavExtraGridCells(live string) string {
	return live
}

// encodeNavExtraGridData writes the fixed [0, 100, -1] data sequence.
func encodeNavExtraGridData(enc *types.Encoder) error {
	err := enc.WriteSequenceLength(3)
	if err != nil {
		return fmt.Errorf("encode occupancygrid data len: %w", err)
	}
	enc.WriteInt8(navExtraData0)
	enc.WriteInt8(navExtraData1)
	enc.WriteInt8(navExtraData2)
	return nil
}

// decodeNavExtraGridData checks the fixed data sequence plus zero pad.
func decodeNavExtraGridData(dec *types.Decoder) error {
	err := decodeNavExtraGridDataBody(dec)
	if err != nil {
		return err
	}
	for dec.Remaining() > 0 {
		pad, err := dec.ReadUint8()
		if err != nil {
			return fmt.Errorf("decode occupancygrid pad: %w", err)
		}
		if pad != 0 {
			return fmt.Errorf("occupancygrid pad 0x%02x wants zero", pad)
		}
	}
	return nil
}

// decodeNavExtraGridDataBody checks the fixed data sequence with no pad.
func decodeNavExtraGridDataBody(dec *types.Decoder) error {
	n, err := dec.ReadSequenceLength()
	if err != nil {
		return fmt.Errorf("decode occupancygrid data len: %w", err)
	}
	if n != 3 {
		return fmt.Errorf("occupancygrid data len %d wants 3", n)
	}
	v0, err := dec.ReadInt8()
	if err != nil {
		return fmt.Errorf("decode occupancygrid data 0: %w", err)
	}
	v1, err := dec.ReadInt8()
	if err != nil {
		return fmt.Errorf("decode occupancygrid data 1: %w", err)
	}
	v2, err := dec.ReadInt8()
	if err != nil {
		return fmt.Errorf("decode occupancygrid data 2: %w", err)
	}
	if v0 != navExtraData0 || v1 != navExtraData1 || v2 != navExtraData2 {
		return fmt.Errorf("occupancygrid data (%v, %v, %v) wants (0, 100, -1)", v0, v1, v2)
	}
	return nil
}

// encodeNavExtraOccupancyGrid encodes OccupancyGrid with live frame_id.
func encodeNavExtraOccupancyGrid(live string) ([]byte, error) {
	enc := types.NewEncoder(true)
	err := encodeMatrixHeader(enc, live)
	if err != nil {
		return nil, fmt.Errorf("encode occupancygrid header: %w", err)
	}
	encodeNavExtraInfo(enc)
	err = encodeNavExtraGridData(enc)
	if err != nil {
		return nil, err
	}
	return enc.Bytes(), nil
}

// decodeNavExtraOccupancyGrid decodes and checks live frame_id plus info plus data.
func decodeNavExtraOccupancyGrid(payload []byte, live string) error {
	dec, err := types.NewDecoder(payload)
	if err != nil {
		return fmt.Errorf("decode occupancygrid header: %w", err)
	}
	frame, err := decodeMatrixHeader(dec)
	if err != nil {
		return fmt.Errorf("decode occupancygrid frame: %w", err)
	}
	if frame != live {
		return fmt.Errorf("occupancygrid frame %q wants %q", frame, live)
	}
	err = decodeNavExtraInfo(dec, "occupancygrid")
	if err != nil {
		return err
	}
	err = decodeNavExtraGridData(dec)
	if err != nil {
		return err
	}
	return nil
}

// wantNavExtraOccupancyGrid returns the live ID expected in echo output.
func wantNavExtraOccupancyGrid(live string) string {
	return live
}

// navExtraCases returns the extra nav live matrix in fixed order.
func navExtraCases() []typeMatrixCase {
	return []typeMatrixCase{
		{
			Label:      "nav-occupancygrid",
			RosType:    "nav_msgs/msg/OccupancyGrid",
			IDLName:    "nav_msgs__msg__OccupancyGrid",
			staticHash: navExtraHashOccupancyGrid,
			yaml:       yamlNavExtraOccupancyGrid,
			encode:     encodeNavExtraOccupancyGrid,
			decode:     decodeNavExtraOccupancyGrid,
			want:       wantNavExtraOccupancyGrid,
		},
		{
			Label:      "nav-mapmetadata",
			RosType:    "nav_msgs/msg/MapMetaData",
			IDLName:    "nav_msgs__msg__MapMetaData",
			staticHash: navExtraHashMapMetaData,
			yaml:       yamlNavExtraMapMetaData,
			encode:     encodeNavExtraMapMetaData,
			decode:     decodeNavExtraMapMetaData,
			want:       wantNavExtraMapMetaData,
		},
		{
			Label:      "nav-gridcells",
			RosType:    "nav_msgs/msg/GridCells",
			IDLName:    "nav_msgs__msg__GridCells",
			staticHash: navExtraHashGridCells,
			yaml:       yamlNavExtraGridCells,
			encode:     encodeNavExtraGridCells,
			decode:     decodeNavExtraGridCells,
			want:       wantNavExtraGridCells,
		},
	}
}

// runNavExtraLabel runs forward plus reverse for one extra nav label.
//
// Domain is the allocated exclusive domain shared by both directions.
// It looks up the case, skips when the peer lacks the type, then runs
// forward with one live ID and reverse with another so the two directions
// never share a topic. Forward and reverse reuse the shared matrix runners
// so domain 42 plus docker argv plus hash probing stay identical.
func runNavExtraLabel(t *testing.T, ctx context.Context, domain int, label string) {
	t.Helper()
	runSharedMatrixLabel(t, ctx, domain, label, navExtraCases())
}

// TestROSTypeNavOccupancyGrid checks OccupancyGrid forward plus reverse.
//
// Forward starts ros2 topic pub for nav_msgs OccupancyGrid and decodes via
// NewDecoder on the Weld take path. Reverse publishes the fixed 3 by 1 map
// via Weld and watches ros2 topic echo. Header frame_id carries the
// per-run live ID so stale runs never match.
func TestROSTypeNavOccupancyGrid(t *testing.T) {
	t.Parallel()
	domain := helpers.MustAllocateDomain(t)
	ctx, cancel := context.WithTimeout(context.Background(), typeMatrixTestTimeout)
	defer cancel()
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)
	runNavExtraLabel(t, ctx, domain, "nav-occupancygrid")
}

// TestROSTypeNavMapMetaData checks MapMetaData forward plus reverse.
//
// Forward starts ros2 topic pub for nav_msgs MapMetaData and decodes via
// NewDecoder on the Weld take path. Reverse publishes the fixed info via
// Weld and watches ros2 topic echo. The type carries no string field, so
// per-run topic plus domain isolation keeps runs apart, matching the
// Vector3 matrix case.
func TestROSTypeNavMapMetaData(t *testing.T) {
	t.Parallel()
	domain := helpers.MustAllocateDomain(t)
	ctx, cancel := context.WithTimeout(context.Background(), typeMatrixTestTimeout)
	defer cancel()
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)
	runNavExtraLabel(t, ctx, domain, "nav-mapmetadata")
}

// TestROSTypeNavGridCells checks GridCells forward plus reverse.
//
// Forward starts ros2 topic pub for nav_msgs GridCells and decodes via
// NewDecoder on the Weld take path. Reverse publishes one cell via Weld
// and watches ros2 topic echo. Header frame_id carries the per-run live
// ID so stale runs never match.
func TestROSTypeNavGridCells(t *testing.T) {
	t.Parallel()
	domain := helpers.MustAllocateDomain(t)
	ctx, cancel := context.WithTimeout(context.Background(), typeMatrixTestTimeout)
	defer cancel()
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)
	runNavExtraLabel(t, ctx, domain, "nav-gridcells")
}

// Nav service pins below are pure Go with no Docker.
//
// GetMap, SetMap, LoadMap, and GetPlan are services, so topic gate coverage
// is N/A: requests travel on rq request DDS topics and replies travel on rr
// reply DDS topics, never on the rt topic path. The tests pin the corpus
// kind plus summary, the rq and rr topic kind mapping, and XCDR request and
// response round trips through the service envelope with no container.

// navExtraSvcGUID returns a fixed 8 byte service GUID for tests.
func navExtraSvcGUID() [8]byte {
	var guid [8]byte
	for i := range guid {
		guid[i] = byte(i + 1)
	}
	return guid
}

// navExtraSvcSeq is the fixed service sequence number for tests.
const navExtraSvcSeq = uint64(7)

// navExtraSvcFrame is the fixed frame_id for service body payloads.
const navExtraSvcFrame = "weld-nav-svc"

// navExtraSvcTolerance is the fixed GetPlan tolerance.
const navExtraSvcTolerance float32 = 0.5

// navExtraSvcMapURL is the fixed LoadMap request URL.
const navExtraSvcMapURL = "package://weld_maps/floor1.yaml"

// navExtraCheckCorpus returns the corpus entry for a nav service or fails.
func navExtraCheckCorpus(t *testing.T, name string) types.CorpusEntry {
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

// navExtraRequireSummary fails when the summary misses a required fragment.
func navExtraRequireSummary(t *testing.T, entry types.CorpusEntry, fragments ...string) {
	t.Helper()
	summary := strings.ToLower(entry.Summary)
	for _, fragment := range fragments {
		if !strings.Contains(summary, strings.ToLower(fragment)) {
			t.Fatalf("corpus %s/%s Summary %q misses %q", entry.Package, entry.Name, entry.Summary, fragment)
		}
	}
}

// navExtraNewServiceEncoder returns an encoder with the service header written.
func navExtraNewServiceEncoder(t *testing.T, le bool, guid [8]byte, seq uint64) *types.Encoder {
	t.Helper()
	enc := types.NewEncoder(le)
	err := enc.WriteServiceHeader(guid, seq)
	if err != nil {
		t.Fatalf("write service header: %v", err)
	}
	return enc
}

// navExtraNewServiceDecoder returns a decoder positioned after the service header.
func navExtraNewServiceDecoder(t *testing.T, wire []byte, guid [8]byte, seq uint64) *types.Decoder {
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

// navExtraSvcFinish checks strict consumption with no drops.
func navExtraSvcFinish(t *testing.T, dec *types.Decoder, label string) {
	t.Helper()
	err := dec.Finish()
	if err != nil {
		t.Fatalf("finish %s: %v", label, err)
	}
	if dec.Dropped() != 0 {
		t.Fatalf("decoder %s drops = %d, want 0", label, dec.Dropped())
	}
}

// navExtraCheckServiceKind pins that a service maps to rq and rr, not to rt.
func navExtraCheckServiceKind(t *testing.T, service string) {
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

// navExtraEncodeOccupancyGridBody writes header plus info plus data.
func navExtraEncodeOccupancyGridBody(enc *types.Encoder, frame string) error {
	err := encodeMatrixHeader(enc, frame)
	if err != nil {
		return fmt.Errorf("encode service grid header: %w", err)
	}
	encodeNavExtraInfo(enc)
	err = encodeNavExtraGridData(enc)
	if err != nil {
		return err
	}
	return nil
}

// navExtraDecodeOccupancyGridBody reads header plus info plus data.
func navExtraDecodeOccupancyGridBody(dec *types.Decoder, frame, label string) error {
	gotFrame, err := decodeMatrixHeader(dec)
	if err != nil {
		return fmt.Errorf("decode %s grid frame: %w", label, err)
	}
	if gotFrame != frame {
		return fmt.Errorf("%s grid frame %q wants %q", label, gotFrame, frame)
	}
	err = decodeNavExtraInfo(dec, label+" grid")
	if err != nil {
		return err
	}
	err = decodeNavExtraGridDataBody(dec)
	if err != nil {
		return err
	}
	return nil
}

// navExtraEncodePoseStampedBody writes header plus pose.
func navExtraEncodePoseStampedBody(enc *types.Encoder, frame string) error {
	err := encodeMatrixHeader(enc, frame)
	if err != nil {
		return fmt.Errorf("encode service posestamped header: %w", err)
	}
	encodeNavPose(enc)
	return nil
}

// navExtraDecodePoseStampedBody reads header plus pose.
func navExtraDecodePoseStampedBody(dec *types.Decoder, frame, label string) error {
	gotFrame, err := decodeMatrixHeader(dec)
	if err != nil {
		return fmt.Errorf("decode %s frame: %w", label, err)
	}
	if gotFrame != frame {
		return fmt.Errorf("%s frame %q wants %q", label, gotFrame, frame)
	}
	err = decodeNavPose(dec, label)
	if err != nil {
		return err
	}
	return nil
}

// navExtraEncodeCovariance writes 36 zero covariance entries.
func navExtraEncodeCovariance(enc *types.Encoder) {
	for range navCovLen {
		enc.WriteFloat64(0.0)
	}
}

// navExtraCheckGetMapCorpus pins the GetMap corpus shape.
func navExtraCheckGetMapCorpus(t *testing.T) {
	t.Helper()
	entry := navExtraCheckCorpus(t, "GetMap")
	navExtraRequireSummary(t, entry, "request empty", "OccupancyGrid map")
}

// navExtraCheckGetPlanCorpus pins the GetPlan corpus shape.
func navExtraCheckGetPlanCorpus(t *testing.T) {
	t.Helper()
	entry := navExtraCheckCorpus(t, "GetPlan")
	navExtraRequireSummary(t, entry, "PoseStamped start", "float32 tolerance", "Path plan")
}

// navExtraCheckLoadMapCorpus pins the LoadMap corpus shape.
func navExtraCheckLoadMapCorpus(t *testing.T) {
	t.Helper()
	entry := navExtraCheckCorpus(t, "LoadMap")
	navExtraRequireSummary(t, entry, "string map_url", "OccupancyGrid map", "uint8 result")
}

// navExtraCheckSetMapCorpus pins the SetMap corpus shape.
func navExtraCheckSetMapCorpus(t *testing.T) {
	t.Helper()
	entry := navExtraCheckCorpus(t, "SetMap")
	navExtraRequireSummary(t, entry, "OccupancyGrid map", "PoseWithCovarianceStamped", "bool success")
}

// navExtraEncodeGetMapResponse encodes a GetMap response body.
func navExtraEncodeGetMapResponse(t *testing.T, le bool, guid [8]byte, seq uint64) []byte {
	t.Helper()
	enc := navExtraNewServiceEncoder(t, le, guid, seq)
	err := navExtraEncodeOccupancyGridBody(enc, navExtraSvcFrame)
	if err != nil {
		t.Fatalf("encode getmap response: %v", err)
	}
	wire := enc.Bytes()
	if len(wire)%4 != 0 {
		t.Fatalf("getmap response wire length %d is not padded to 4", len(wire))
	}
	return wire
}

// navExtraDecodeGetMapResponse decodes a GetMap response body.
func navExtraDecodeGetMapResponse(t *testing.T, wire []byte, guid [8]byte, seq uint64, label string) {
	t.Helper()
	dec := navExtraNewServiceDecoder(t, wire, guid, seq)
	err := navExtraDecodeOccupancyGridBody(dec, navExtraSvcFrame, label)
	if err != nil {
		t.Fatalf("decode %s: %v", label, err)
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
	navExtraSvcFinish(t, dec, label)
}

// navExtraEncodeEmptyRequest encodes a service request with no user fields.
func navExtraEncodeEmptyRequest(t *testing.T, le bool, guid [8]byte, seq uint64) []byte {
	t.Helper()
	enc := navExtraNewServiceEncoder(t, le, guid, seq)
	wire := enc.Bytes()
	if len(wire)%4 != 0 {
		t.Fatalf("empty request wire length %d is not padded to 4", len(wire))
	}
	return wire
}

// navExtraDecodeEmptyRequest decodes a service request with no user fields.
func navExtraDecodeEmptyRequest(t *testing.T, wire []byte, guid [8]byte, seq uint64, label string) {
	t.Helper()
	dec := navExtraNewServiceDecoder(t, wire, guid, seq)
	for dec.Remaining() > 0 {
		pad, err := dec.ReadUint8()
		if err != nil {
			t.Fatalf("read %s pad: %v", label, err)
		}
		if pad != 0x00 {
			t.Fatalf("read %s pad = 0x%02x, want zero", label, pad)
		}
	}
	navExtraSvcFinish(t, dec, label)
}

// navExtraEncodeLoadMapRequest encodes a LoadMap request body.
func navExtraEncodeLoadMapRequest(t *testing.T, le bool, guid [8]byte, seq uint64) []byte {
	t.Helper()
	enc := navExtraNewServiceEncoder(t, le, guid, seq)
	err := enc.WriteString(navExtraSvcMapURL)
	if err != nil {
		t.Fatalf("encode loadmap url: %v", err)
	}
	return enc.Bytes()
}

// navExtraDecodeLoadMapRequest decodes a LoadMap request body.
func navExtraDecodeLoadMapRequest(t *testing.T, wire []byte, guid [8]byte, seq uint64, label string) {
	t.Helper()
	dec := navExtraNewServiceDecoder(t, wire, guid, seq)
	got, err := dec.ReadString()
	if err != nil {
		t.Fatalf("read %s url: %v", label, err)
	}
	if got != navExtraSvcMapURL {
		t.Fatalf("read %s url = %q, want %q", label, got, navExtraSvcMapURL)
	}
	navExtraSvcFinish(t, dec, label)
}

// navExtraEncodeLoadMapResponse encodes a LoadMap response body.
func navExtraEncodeLoadMapResponse(t *testing.T, le bool, guid [8]byte, seq uint64) []byte {
	t.Helper()
	enc := navExtraNewServiceEncoder(t, le, guid, seq)
	err := navExtraEncodeOccupancyGridBody(enc, navExtraSvcFrame)
	if err != nil {
		t.Fatalf("encode loadmap grid: %v", err)
	}
	enc.WriteUint8(0)
	return enc.Bytes()
}

// navExtraDecodeLoadMapResponse decodes a LoadMap response body.
func navExtraDecodeLoadMapResponse(t *testing.T, wire []byte, guid [8]byte, seq uint64, label string) {
	t.Helper()
	dec := navExtraNewServiceDecoder(t, wire, guid, seq)
	err := navExtraDecodeOccupancyGridBody(dec, navExtraSvcFrame, label)
	if err != nil {
		t.Fatalf("decode %s grid: %v", label, err)
	}
	result, err := dec.ReadUint8()
	if err != nil {
		t.Fatalf("read %s result: %v", label, err)
	}
	if result != 0 {
		t.Fatalf("read %s result = %d, want 0", label, result)
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
	navExtraSvcFinish(t, dec, label)
}

// navExtraEncodeGetPlanRequest encodes a GetPlan request body.
func navExtraEncodeGetPlanRequest(t *testing.T, le bool, guid [8]byte, seq uint64) []byte {
	t.Helper()
	enc := navExtraNewServiceEncoder(t, le, guid, seq)
	err := navExtraEncodePoseStampedBody(enc, "start")
	if err != nil {
		t.Fatalf("encode getplan start: %v", err)
	}
	err = navExtraEncodePoseStampedBody(enc, "goal")
	if err != nil {
		t.Fatalf("encode getplan goal: %v", err)
	}
	enc.WriteFloat32(navExtraSvcTolerance)
	return enc.Bytes()
}

// navExtraDecodeGetPlanRequest decodes a GetPlan request body.
func navExtraDecodeGetPlanRequest(t *testing.T, wire []byte, guid [8]byte, seq uint64, label string) {
	t.Helper()
	dec := navExtraNewServiceDecoder(t, wire, guid, seq)
	err := navExtraDecodePoseStampedBody(dec, "start", label+" start")
	if err != nil {
		t.Fatalf("decode %s start: %v", label, err)
	}
	err = navExtraDecodePoseStampedBody(dec, "goal", label+" goal")
	if err != nil {
		t.Fatalf("decode %s goal: %v", label, err)
	}
	tol, err := dec.ReadFloat32()
	if err != nil {
		t.Fatalf("read %s tolerance: %v", label, err)
	}
	if tol != navExtraSvcTolerance {
		t.Fatalf("read %s tolerance = %v, want %v", label, tol, navExtraSvcTolerance)
	}
	navExtraSvcFinish(t, dec, label)
}

// navExtraEncodeGetPlanResponse encodes a GetPlan response body.
func navExtraEncodeGetPlanResponse(t *testing.T, le bool, guid [8]byte, seq uint64) []byte {
	t.Helper()
	enc := navExtraNewServiceEncoder(t, le, guid, seq)
	err := encodeMatrixHeader(enc, "plan")
	if err != nil {
		t.Fatalf("encode getplan plan header: %v", err)
	}
	err = enc.WriteSequenceLength(1)
	if err != nil {
		t.Fatalf("encode getplan poses len: %v", err)
	}
	err = navExtraEncodePoseStampedBody(enc, "plan-pose")
	if err != nil {
		t.Fatalf("encode getplan pose: %v", err)
	}
	return enc.Bytes()
}

// navExtraDecodeGetPlanResponse decodes a GetPlan response body.
func navExtraDecodeGetPlanResponse(t *testing.T, wire []byte, guid [8]byte, seq uint64, label string) {
	t.Helper()
	dec := navExtraNewServiceDecoder(t, wire, guid, seq)
	frame, err := decodeMatrixHeader(dec)
	if err != nil {
		t.Fatalf("decode %s plan frame: %v", label, err)
	}
	if frame != "plan" {
		t.Fatalf("%s plan frame %q wants %q", label, frame, "plan")
	}
	n, err := dec.ReadSequenceLength()
	if err != nil {
		t.Fatalf("read %s poses len: %v", label, err)
	}
	if n != 1 {
		t.Fatalf("read %s poses len = %d, want 1", label, n)
	}
	err = navExtraDecodePoseStampedBody(dec, "plan-pose", label+" pose")
	if err != nil {
		t.Fatalf("decode %s pose: %v", label, err)
	}
	navExtraSvcFinish(t, dec, label)
}

// navExtraEncodeSetMapRequest encodes a SetMap request body.
func navExtraEncodeSetMapRequest(t *testing.T, le bool, guid [8]byte, seq uint64) []byte {
	t.Helper()
	enc := navExtraNewServiceEncoder(t, le, guid, seq)
	err := navExtraEncodeOccupancyGridBody(enc, navExtraSvcFrame)
	if err != nil {
		t.Fatalf("encode setmap grid: %v", err)
	}
	err = encodeMatrixHeader(enc, "init")
	if err != nil {
		t.Fatalf("encode setmap init header: %v", err)
	}
	encodeNavPose(enc)
	navExtraEncodeCovariance(enc)
	return enc.Bytes()
}

// navExtraDecodeSetMapRequest decodes a SetMap request body.
func navExtraDecodeSetMapRequest(t *testing.T, wire []byte, guid [8]byte, seq uint64, label string) {
	t.Helper()
	dec := navExtraNewServiceDecoder(t, wire, guid, seq)
	err := navExtraDecodeOccupancyGridBody(dec, navExtraSvcFrame, label)
	if err != nil {
		t.Fatalf("decode %s grid: %v", label, err)
	}
	frame, err := decodeMatrixHeader(dec)
	if err != nil {
		t.Fatalf("decode %s init frame: %v", label, err)
	}
	if frame != "init" {
		t.Fatalf("%s init frame %q wants %q", label, frame, "init")
	}
	err = decodeNavPose(dec, label+" init pose")
	if err != nil {
		t.Fatalf("decode %s init pose: %v", label, err)
	}
	err = decodeNavCovariance(dec, label+" init covariance")
	if err != nil {
		t.Fatalf("decode %s init covariance: %v", label, err)
	}
	if dec.Dropped() != 0 {
		t.Fatalf("decoder %s drops = %d, want 0", label, dec.Dropped())
	}
	navExtraSvcFinish(t, dec, label)
}

// navExtraEncodeBoolResponse encodes a service response carrying one bool.
func navExtraEncodeBoolResponse(t *testing.T, le bool, guid [8]byte, seq uint64, value bool) []byte {
	t.Helper()
	enc := navExtraNewServiceEncoder(t, le, guid, seq)
	enc.WriteBool(value)
	wire := enc.Bytes()
	if len(wire)%4 != 0 {
		t.Fatalf("bool response wire length %d is not padded to 4", len(wire))
	}
	return wire
}

// navExtraDecodeBoolResponse decodes a service response carrying one bool.
func navExtraDecodeBoolResponse(t *testing.T, wire []byte, guid [8]byte, seq uint64, want bool, label string) {
	t.Helper()
	dec := navExtraNewServiceDecoder(t, wire, guid, seq)
	got, err := dec.ReadBool()
	if err != nil {
		t.Fatalf("read %s success: %v", label, err)
	}
	if got != want {
		t.Fatalf("read %s success = %v, want %v", label, got, want)
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
	navExtraSvcFinish(t, dec, label)
}

// TestROSNavExtraServices pins nav_msgs service shapes with pure Go and no Docker.
//
// GetMap, SetMap, LoadMap, and GetPlan travel on rq and rr DDS topics, so
// topic gate coverage is N/A. The test pins the corpus kind plus summary,
// the rq kind as TopicKindRequest and the rr kind as TopicKindReply, the
// ServiceEnvelopeSize rebase for both endian orders, and XCDR request and
// response round trips through the service envelope.
func TestROSNavExtraServices(t *testing.T) {
	t.Parallel()
	navExtraCheckGetMapCorpus(t)
	navExtraCheckGetPlanCorpus(t)
	navExtraCheckLoadMapCorpus(t)
	navExtraCheckSetMapCorpus(t)

	navExtraCheckServiceKind(t, "/get_map")
	navExtraCheckServiceKind(t, "/get_plan")
	navExtraCheckServiceKind(t, "/load_map")
	navExtraCheckServiceKind(t, "/set_map")

	guid := navExtraSvcGUID()
	seq := navExtraSvcSeq

	t.Run("getmap_request_empty", func(t *testing.T) {
		t.Parallel()
		for _, le := range []bool{true, false} {
			wire := navExtraEncodeEmptyRequest(t, le, guid, seq)
			navExtraDecodeEmptyRequest(t, wire, guid, seq, "getmap request")
		}
	})
	t.Run("getmap_response", func(t *testing.T) {
		t.Parallel()
		for _, le := range []bool{true, false} {
			wire := navExtraEncodeGetMapResponse(t, le, guid, seq+1)
			navExtraDecodeGetMapResponse(t, wire, guid, seq+1, "getmap response")
		}
	})
	t.Run("loadmap_request", func(t *testing.T) {
		t.Parallel()
		for _, le := range []bool{true, false} {
			wire := navExtraEncodeLoadMapRequest(t, le, guid, seq+2)
			navExtraDecodeLoadMapRequest(t, wire, guid, seq+2, "loadmap request")
		}
	})
	t.Run("loadmap_response", func(t *testing.T) {
		t.Parallel()
		for _, le := range []bool{true, false} {
			wire := navExtraEncodeLoadMapResponse(t, le, guid, seq+3)
			navExtraDecodeLoadMapResponse(t, wire, guid, seq+3, "loadmap response")
		}
	})
	t.Run("getplan_request", func(t *testing.T) {
		t.Parallel()
		for _, le := range []bool{true, false} {
			wire := navExtraEncodeGetPlanRequest(t, le, guid, seq+4)
			navExtraDecodeGetPlanRequest(t, wire, guid, seq+4, "getplan request")
		}
	})
	t.Run("getplan_response", func(t *testing.T) {
		t.Parallel()
		for _, le := range []bool{true, false} {
			wire := navExtraEncodeGetPlanResponse(t, le, guid, seq+5)
			navExtraDecodeGetPlanResponse(t, wire, guid, seq+5, "getplan response")
		}
	})
	t.Run("setmap_request", func(t *testing.T) {
		t.Parallel()
		for _, le := range []bool{true, false} {
			wire := navExtraEncodeSetMapRequest(t, le, guid, seq+6)
			navExtraDecodeSetMapRequest(t, wire, guid, seq+6, "setmap request")
		}
	})
	t.Run("setmap_response", func(t *testing.T) {
		t.Parallel()
		for _, le := range []bool{true, false} {
			wire := navExtraEncodeBoolResponse(t, le, guid, seq+7, true)
			navExtraDecodeBoolResponse(t, wire, guid, seq+7, true, "setmap response")
		}
	})
}
