// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

//nolint:dupl // live gate decoders repeat similar wire checks across types
package ros_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/podomy/weld/internal/transports"
	"github.com/podomy/weld/internal/transports/dds"
	"github.com/podomy/weld/internal/types"
	"github.com/podomy/weld/testbed/live/helpers"
)

// Static sensor rest2 type hash from the Jazzy type description JSON.
//
// The RIHS01 hash is the first entry of type_hashes in
// /opt/ros/jazzy/share/sensor_msgs/msg/Image.json on weld-jazzy.
// Live probing via ros2 topic info -v is preferred at runtime, this value
// is the fallback when the probe cannot run.
const sensorRest2HashImage = "RIHS01_d31d41a9a4c4bc8eae9be757b0beed306564f7526c88ea6a4588fb9582527d47"

// Fixed sensor rest2 Image sentinels shared by YAML, encode, and decode.
//
// The Header carries the per-run live ID in frame_id so stale runs never
// match. Geometry stays at 2 by 2 mono8 with four fixed bytes so Weld take
// and ros2 echo checks stay deterministic.
const (
	sensorRest2ImageHeight uint32 = 2
	sensorRest2ImageWidth  uint32 = 2
	sensorRest2ImageStep   uint32 = 2
	sensorRest2ImageFormat        = "mono8"
)

// sensorRest2ImageData holds four fixed mono8 bytes in row order.
var sensorRest2ImageData = []byte{0x01, 0x02, 0x03, 0x04}

// yamlSensorRest2Image returns ROS YAML with live frame_id.
func yamlSensorRest2Image(live string) string {
	return fmt.Sprintf("{header: {stamp: {sec: 0, nanosec: 0}, frame_id: '%s'}, height: 2, width: 2, encoding: 'mono8', is_bigendian: false, step: 2, data: [1, 2, 3, 4]}", live)
}

// encodeSensorRest2Image encodes the small Image with live frame_id.
func encodeSensorRest2Image(live string) ([]byte, error) {
	enc := types.NewEncoder(true)
	err := encodeMatrixHeader(enc, live)
	if err != nil {
		return nil, fmt.Errorf("encode image header: %w", err)
	}
	enc.WriteUint32(sensorRest2ImageHeight)
	enc.WriteUint32(sensorRest2ImageWidth)
	err = enc.WriteString(sensorRest2ImageFormat)
	if err != nil {
		return nil, fmt.Errorf("encode image encoding: %w", err)
	}
	enc.WriteUint8(0)
	enc.WriteUint32(sensorRest2ImageStep)
	err = enc.WriteOctetSequence(sensorRest2ImageData)
	if err != nil {
		return nil, fmt.Errorf("encode image data: %w", err)
	}
	return enc.Bytes(), nil
}

// decodeSensorRest2ImageDims checks height plus width plus encoding plus step.
func decodeSensorRest2ImageDims(dec *types.Decoder) error {
	height, err := dec.ReadUint32()
	if err != nil {
		return fmt.Errorf("decode image height: %w", err)
	}
	width, err := dec.ReadUint32()
	if err != nil {
		return fmt.Errorf("decode image width: %w", err)
	}
	if height != sensorRest2ImageHeight || width != sensorRest2ImageWidth {
		return fmt.Errorf("image size (%d, %d) wants (2, 2)", height, width)
	}
	encoding, err := dec.ReadString()
	if err != nil {
		return fmt.Errorf("decode image encoding: %w", err)
	}
	if encoding != sensorRest2ImageFormat {
		return fmt.Errorf("image encoding %q wants %q", encoding, sensorRest2ImageFormat)
	}
	big, err := dec.ReadUint8()
	if err != nil {
		return fmt.Errorf("decode image bigendian: %w", err)
	}
	if big != 0 {
		return fmt.Errorf("image bigendian %d wants 0", big)
	}
	step, err := dec.ReadUint32()
	if err != nil {
		return fmt.Errorf("decode image step: %w", err)
	}
	if step != sensorRest2ImageStep {
		return fmt.Errorf("image step %d wants 2", step)
	}
	return nil
}

// decodeSensorRest2ImageData checks the four fixed data bytes plus finish.
func decodeSensorRest2ImageData(dec *types.Decoder) error {
	data, err := dec.ReadOctetSequence()
	if err != nil {
		return fmt.Errorf("decode image data: %w", err)
	}
	if len(data) != len(sensorRest2ImageData) {
		return fmt.Errorf("image data len %d wants %d", len(data), len(sensorRest2ImageData))
	}
	for i := range data {
		if data[i] != sensorRest2ImageData[i] {
			return fmt.Errorf("image data byte %d 0x%02x wants 0x%02x", i, data[i], sensorRest2ImageData[i])
		}
	}
	err = dec.Finish()
	if err != nil {
		return fmt.Errorf("finish image: %w", err)
	}
	return nil
}

// decodeSensorRest2Image decodes and checks live frame_id plus sentinels.
func decodeSensorRest2Image(payload []byte, live string) error {
	dec, err := types.NewDecoder(payload)
	if err != nil {
		return fmt.Errorf("decode image header: %w", err)
	}
	frame, err := decodeMatrixHeader(dec)
	if err != nil {
		return fmt.Errorf("decode image frame: %w", err)
	}
	if frame != live {
		return fmt.Errorf("image frame %q wants %q", frame, live)
	}
	err = decodeSensorRest2ImageDims(dec)
	if err != nil {
		return err
	}
	err = decodeSensorRest2ImageData(dec)
	if err != nil {
		return err
	}
	return nil
}

// wantSensorRest2Image returns the live ID expected in echo output.
func wantSensorRest2Image(live string) string {
	return live
}

// sensorRest2Cases returns the rest2 sensor live matrix in fixed order.
func sensorRest2Cases() []typeMatrixCase {
	return []typeMatrixCase{
		{
			Label:      "sensor-image",
			RosType:    "sensor_msgs/msg/Image",
			IDLName:    "sensor_msgs__msg__Image",
			staticHash: sensorRest2HashImage,
			yaml:       yamlSensorRest2Image,
			encode:     encodeSensorRest2Image,
			decode:     decodeSensorRest2Image,
			want:       wantSensorRest2Image,
		},
	}
}

// runSensorRest2Label runs forward plus reverse for one rest2 sensor label.
//
// It looks up the case, skips when the peer lacks the type, then runs
// forward with one live ID and reverse with another so the two directions
// never share a topic. Forward and reverse reuse the shared matrix runners
// so domain 42 plus docker argv plus hash probing stay identical.
func runSensorRest2Label(t *testing.T, ctx context.Context, domain int, label string) {
	t.Helper()
	runSharedMatrixLabel(t, ctx, domain, label, sensorRest2Cases())
}

// TestROSTypeSensorImage checks Image forward plus reverse.
//
// Header frame_id carries the per-run live ID so stale runs never match.
// Geometry stays at 2 by 2 mono8 with four fixed bytes.
func TestROSTypeSensorImage(t *testing.T) {
	t.Parallel()
	domain := helpers.MustAllocateDomain(t)
	ctx, cancel := context.WithTimeout(context.Background(), typeMatrixTestTimeout)
	defer cancel()
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)
	runSensorRest2Label(t, ctx, domain, "sensor-image")
}

// Large sensor rest2 PointCloud2 constants for timeouts plus geometry.
//
// Each test allocates one exclusive domain from the parallel pool,
// distro 43 and 44, and liveliness 65 through 69. The sample holds 100KiB
// of data so one sample exceeds a single DATA MTU and fragments on the
// wire while staying far under the 32MiB maxPayloadLen cap that bounds
// DATA_FRAG reassembly. Timeouts reuse the generous large payload values
// so fragment reassembly plus SEDP matching complete before the poll ends.
const (
	sensorRest2PC2Height    uint32 = 64
	sensorRest2PC2Width     uint32 = 100
	sensorRest2PC2PointStep uint32 = 16
	sensorRest2PC2RowStep   uint32 = 1600
	sensorRest2PC2DataLen          = 102400
	sensorRest2PC2RosType          = "sensor_msgs/msg/PointCloud2"
	sensorRest2PC2IDL              = "sensor_msgs__msg__PointCloud2"
)

// sensorRest2PC2Datatype is the FLOAT32 PointField datatype.
const sensorRest2PC2Datatype uint8 = 7

// buildSensorRest2PC2Data returns deterministic bytes of length DataLen.
func buildSensorRest2PC2Data() []byte {
	data := make([]byte, sensorRest2PC2DataLen)
	for i := range data {
		data[i] = byte(i % 256)
	}
	return data
}

// encodeSensorRest2PC2Field writes one float32 PointField entry.
func encodeSensorRest2PC2Field(enc *types.Encoder, name string, offset uint32) error {
	err := enc.WriteString(name)
	if err != nil {
		return fmt.Errorf("encode pc2large field name: %w", err)
	}
	enc.WriteUint32(offset)
	enc.WriteUint8(sensorRest2PC2Datatype)
	enc.WriteUint32(1)
	return nil
}

// encodeSensorRest2PC2 encodes one large PointCloud2 with live frame_id.
func encodeSensorRest2PC2(t *testing.T, live string, data []byte) []byte {
	t.Helper()
	enc := types.NewEncoder(true)
	err := encodeMatrixHeader(enc, live)
	if err != nil {
		t.Fatalf("encode pc2large header: %v", err)
	}
	enc.WriteUint32(sensorRest2PC2Height)
	enc.WriteUint32(sensorRest2PC2Width)
	err = enc.WriteSequenceLength(3)
	if err != nil {
		t.Fatalf("encode pc2large fields len: %v", err)
	}
	names := []string{"x", "y", "z"}
	for i, name := range names {
		err = encodeSensorRest2PC2Field(enc, name, uint32(i*4))
		if err != nil {
			t.Fatalf("encode pc2large field %q: %v", name, err)
		}
	}
	enc.WriteBool(false)
	enc.WriteUint32(sensorRest2PC2PointStep)
	enc.WriteUint32(sensorRest2PC2RowStep)
	err = enc.WriteOctetSequence(data)
	if err != nil {
		t.Fatalf("encode pc2large data: %v", err)
	}
	enc.WriteBool(true)
	wire := enc.Bytes()
	if len(wire) == 0 {
		t.Fatalf("encoded pc2large is empty")
	}
	return wire
}

// decodeSensorRest2PC2Field checks one float32 PointField entry.
func decodeSensorRest2PC2Field(dec *types.Decoder, name string, offset uint32) error {
	got, err := dec.ReadString()
	if err != nil {
		return fmt.Errorf("decode pc2large field name: %w", err)
	}
	if got != name {
		return fmt.Errorf("pc2large field name %q wants %q", got, name)
	}
	off, err := dec.ReadUint32()
	if err != nil {
		return fmt.Errorf("decode pc2large offset: %w", err)
	}
	if off != offset {
		return fmt.Errorf("pc2large offset %d wants %d", off, offset)
	}
	dt, err := dec.ReadUint8()
	if err != nil {
		return fmt.Errorf("decode pc2large datatype: %w", err)
	}
	if dt != sensorRest2PC2Datatype {
		return fmt.Errorf("pc2large datatype %d wants 7", dt)
	}
	cnt, err := dec.ReadUint32()
	if err != nil {
		return fmt.Errorf("decode pc2large count: %w", err)
	}
	if cnt != 1 {
		return fmt.Errorf("pc2large count %d wants 1", cnt)
	}
	return nil
}

// decodeSensorRest2PC2Geometry checks height plus width plus fields length.
func decodeSensorRest2PC2Geometry(dec *types.Decoder) error {
	height, err := dec.ReadUint32()
	if err != nil {
		return fmt.Errorf("decode pc2large height: %w", err)
	}
	width, err := dec.ReadUint32()
	if err != nil {
		return fmt.Errorf("decode pc2large width: %w", err)
	}
	if height != sensorRest2PC2Height || width != sensorRest2PC2Width {
		return fmt.Errorf("pc2large size (%d, %d) wants (64, 100)", height, width)
	}
	n, err := dec.ReadSequenceLength()
	if err != nil {
		return fmt.Errorf("decode pc2large fields len: %w", err)
	}
	if n != 3 {
		return fmt.Errorf("pc2large fields len %d wants 3", n)
	}
	return nil
}

// decodeSensorRest2PC2Fields checks the three float32 field entries.
func decodeSensorRest2PC2Fields(dec *types.Decoder) error {
	names := []string{"x", "y", "z"}
	for i, name := range names {
		err := decodeSensorRest2PC2Field(dec, name, uint32(i*4))
		if err != nil {
			return err
		}
	}
	return nil
}

// decodeSensorRest2PC2Steps checks endian plus step plus row step.
func decodeSensorRest2PC2Steps(dec *types.Decoder) error {
	big, err := dec.ReadBool()
	if err != nil {
		return fmt.Errorf("decode pc2large bigendian: %w", err)
	}
	if big {
		return errors.New("pc2large bigendian true wants false") //nolint:wrapcheck // new sentinel error, nothing to wrap
	}
	step, err := dec.ReadUint32()
	if err != nil {
		return fmt.Errorf("decode pc2large step: %w", err)
	}
	row, err := dec.ReadUint32()
	if err != nil {
		return fmt.Errorf("decode pc2large row: %w", err)
	}
	if step != sensorRest2PC2PointStep || row != sensorRest2PC2RowStep {
		return fmt.Errorf("pc2large steps (%d, %d) wants (16, 1600)", step, row)
	}
	return nil
}

// decodeSensorRest2PC2Dims checks height plus width plus fields plus steps.
func decodeSensorRest2PC2Dims(dec *types.Decoder) error {
	err := decodeSensorRest2PC2Geometry(dec)
	if err != nil {
		return err
	}
	err = decodeSensorRest2PC2Fields(dec)
	if err != nil {
		return err
	}
	err = decodeSensorRest2PC2Steps(dec)
	if err != nil {
		return err
	}
	return nil
}

// decodeSensorRest2PC2Data checks full data bytes plus dense plus pad.
func decodeSensorRest2PC2Data(dec *types.Decoder, want []byte) error {
	got, err := dec.ReadOctetSequence()
	if err != nil {
		return fmt.Errorf("decode pc2large data: %w", err)
	}
	if len(got) != len(want) {
		return fmt.Errorf("pc2large data len %d wants %d", len(got), len(want))
	}
	for i := range got {
		if got[i] != want[i] {
			return fmt.Errorf("pc2large data byte %d 0x%02x wants 0x%02x", i, got[i], want[i])
		}
	}
	dense, err := dec.ReadBool()
	if err != nil {
		return fmt.Errorf("decode pc2large dense: %w", err)
	}
	if !dense {
		return errors.New("pc2large dense false wants true") //nolint:wrapcheck // new sentinel error, nothing to wrap
	}
	err = checkSensorRestPad(dec, "pc2large")
	if err != nil {
		return err
	}
	return nil
}

// decodeSensorRest2PC2 checks one large PointCloud2 wire sample.
func decodeSensorRest2PC2(payload []byte, live string, want []byte) error {
	dec, err := types.NewDecoder(payload)
	if err != nil {
		return fmt.Errorf("decode pc2large header: %w", err)
	}
	frame, err := decodeMatrixHeader(dec)
	if err != nil {
		return fmt.Errorf("decode pc2large frame: %w", err)
	}
	if frame != live {
		return fmt.Errorf("pc2large frame %q wants %q", frame, live)
	}
	err = decodeSensorRest2PC2Dims(dec)
	if err != nil {
		return err
	}
	err = decodeSensorRest2PC2Data(dec, want)
	if err != nil {
		return err
	}
	return nil
}

// skipIfNoPC2Type skips when the peer lacks sensor_msgs PointCloud2.
//
// Domain is the allocated exclusive domain plumbed to DockerArgs, so
// parallel workers never share a domain.
func skipIfNoPC2Type(t *testing.T, ctx context.Context, domain int) {
	t.Helper()
	probeCtx, cancel := context.WithTimeout(ctx, largePayloadProbe)
	defer cancel()
	args := helpers.DockerArgs(helpers.Container, domain, helpers.RosBin, "interface", "show", sensorRest2PC2RosType)
	cmd := exec.CommandContext(probeCtx, "docker", args...) //nolint:gosec // live harness runs docker with controlled args and no shell
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Skipf("ros2 interface show %s absent, skipping large pc2: %v output %s", sensorRest2PC2RosType, err, string(out))
	}
}

// startSensorRest2PC2Echo starts ros2 topic echo for PointCloud2 into a file.
func startSensorRest2PC2Echo(t *testing.T, ctx context.Context, domain uint32, topic, logPath string) {
	t.Helper()
	echoFile, err := os.Create(logPath) //nolint:gosec // live harness uses controlled temp file path
	if err != nil {
		t.Fatalf("create ros echo log: %v", err)
	}
	t.Cleanup(func() {
		cerr := echoFile.Close()
		if cerr != nil {
			t.Logf("close ros echo log: %v", cerr)
		}
	})
	args := helpers.DockerArgs(helpers.Container, int(domain), helpers.RosBin, "topic", "echo", topic, sensorRest2PC2RosType)
	echoCmd := exec.CommandContext(ctx, "docker", args...) //nolint:gosec // live harness runs docker with fixed test args
	echoCmd.Stdout = echoFile
	echoCmd.Stderr = echoFile
	err = echoCmd.Start()
	if err != nil {
		t.Fatalf("start ros2 topic echo: %v", err)
	}
	t.Cleanup(func() {
		helpers.StopCmd(echoCmd)
		cleanupCtx, cleanupCancel := context.WithTimeout(context.WithoutCancel(ctx), largePayloadClean)
		defer cleanupCancel()
		helpers.KillContainerProc(cleanupCtx, helpers.Container, int(domain), topic)
	})
}

// sensorRest2LargeAllowEntry pins one temporary payload miss exception.
//
// An entry allows a skip only for an exact RosType plus exact RIHS01 Hash
// while time is before Expiry. The list stays empty so every miss fails.
type sensorRest2LargeAllowEntry struct {
	RosType string
	Hash    string
	Expiry  time.Time
}

// sensorRest2LargeAllowlist holds temporary exceptions. Empty is strict.
var sensorRest2LargeAllowlist []sensorRest2LargeAllowEntry

// sensorRest2LargeAllowlisted reports whether a miss may skip.
//
// It requires exact type plus exact hash plus unexpired time. Empty list
// means false so callers fail.
func sensorRest2LargeAllowlisted(rosType, hash string, now time.Time) bool {
	for _, e := range sensorRest2LargeAllowlist {
		if e.RosType == rosType && e.Hash == hash && now.Before(e.Expiry) {
			return true
		}
	}
	return false
}

// publishSensorRest2PC2UntilEcho publishes until ros2 echo holds the live ID.
func publishSensorRest2PC2UntilEcho(ctx context.Context, t *testing.T, backend *dds.Backend, msg transports.OutgoingMessage, echoPath, live string) bool {
	t.Helper()
	deadline := time.Now().Add(largePayloadPoll)
	for {
		err := helpers.PublishLiveWithRetry(ctx, backend, msg, largePayloadPubRetry, largePayloadPubGap, "large")
		if err != nil {
			t.Fatalf("weld pub large pc2: %v", err)
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

// verifySensorRest2PC2Echo asserts one echo doc holds live frame plus bytes.
func verifySensorRest2PC2Echo(t *testing.T, echoPath, live string, want []byte) {
	t.Helper()
	data, err := os.ReadFile(echoPath) //nolint:gosec // live harness reads controlled temp file path
	if err != nil {
		t.Fatalf("read ros echo log: %v", err)
	}
	text := string(data)
	if !strings.Contains(text, live) {
		t.Fatalf("ros echo misses live ID %q\necho output:\n%s", live, text)
	}
	docs := strings.Split(text, "---")
	found := ""
	for _, doc := range docs {
		if strings.Contains(doc, live) {
			found = doc
			break
		}
	}
	if found == "" {
		t.Fatalf("ros echo has live ID %q but no doc holds it\necho output:\n%s", live, text)
	}
	for _, marker := range []string{"height: 64", "width: 100", "point_step: 16", "row_step: 1600", "frame_id"} {
		if !strings.Contains(found, marker) {
			t.Fatalf("ros echo doc misses %q for %q\ndoc:\n%s", marker, live, found)
		}
	}
	err = parseLargeEchoData(found, want)
	if err != nil {
		t.Fatalf("ros echo data check: %v\ndoc tail:\n%s", err, found)
	}
}

// TestROSTypeSensorPointCloud2Large proves a 100KiB PointCloud2 flows via Weld.
//
// It follows the large payload proof pattern on one allocated exclusive domain: a 64 by
// 100 cloud with three float32 fields and deterministic bytes plus a
// per-run live ID in header frame_id. The local XCDR1 codec round trips
// full bytes, then the fragmented sample publishes through Weld and ros2
// topic echo in the already running weld-jazzy peer proves the live ID
// plus geometry plus all data bytes. The sample is 100KiB so it exceeds
// one DATA MTU and fragments while staying far under the 32MiB
// maxPayloadLen cap. It uses os/exec docker argv only with context
// timeouts and skips cleanly when no peer runs or when the type is absent.
func TestROSTypeSensorPointCloud2Large(t *testing.T) {
	t.Parallel()
	domain := helpers.MustAllocateDomain(t)
	ctx, cancel := context.WithTimeout(context.Background(), largePayloadOverall)
	defer cancel()
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)
	skipIfNoPC2Type(t, ctx, domain)
	runID := helpers.LiveID("ros-rest2-pc2-large")
	topic := helpers.LiveTopic(runID)
	wantData := buildSensorRest2PC2Data()
	wire := encodeSensorRest2PC2(t, runID, wantData)
	err := decodeSensorRest2PC2(wire, runID, wantData)
	if err != nil {
		t.Fatalf("local rest2 large pc2 codec: %v", err)
	}
	t.Logf("local rest2 large pc2 codec ok len %d id %s", len(wire), runID)
	backend := helpers.MustOpenLiveBackend(t, ctx, uint32(domain), "ros-rest2-pc2-large-"+runID, "large") //nolint:gosec // G115: pooled domain 70 through 101 fits uint32
	echoPath := filepath.Join(t.TempDir(), "rosecho-rest2-pc2-large-"+runID+".log")
	startSensorRest2PC2Echo(t, ctx, uint32(domain), topic, echoPath) //nolint:gosec // G115: pooled domain 70 through 101 fits uint32
	helpers.Sleep(ctx, largePayloadSettle)
	cerr := ctx.Err()
	if cerr != nil {
		t.Fatalf("context ended before rest2 large pc2 pub: %v", cerr)
	}
	msg := transports.OutgoingMessage{
		TopicName: topic,
		TypeName:  sensorRest2PC2IDL,
		Payload:   wire,
	}
	if !publishSensorRest2PC2UntilEcho(ctx, t, backend, msg, echoPath, runID) {
		data, rerr := os.ReadFile(echoPath) //nolint:gosec // live harness reads controlled temp file path
		if rerr != nil {
			t.Fatalf("ros echo did not see rest2 large pc2 %q: %v", runID, rerr)
		}
		if sensorRest2LargeAllowlisted(sensorRest2PC2RosType, sensorRestHashPointCloud2, time.Now()) {
			t.Skipf("allowlisted live gap %s hash %s expires %s: ros echo did not see rest2 large pc2 %q", "sensor-pc2-large", sensorRestHashPointCloud2, time.Date(2027, time.March, 31, 0, 0, 0, 0, time.UTC).Format("2006-01-02"), runID)
		}
		t.Fatalf("ros echo did not see rest2 large pc2 %q, pub ok len %d\necho output:\n%s", runID, len(wire), string(data))
	}
	helpers.Sleep(ctx, 2*time.Second)
	verifySensorRest2PC2Echo(t, echoPath, runID, wantData)
	t.Logf("rest2 large pc2 fragmented ok len %d id %s", len(wire), runID)
}
