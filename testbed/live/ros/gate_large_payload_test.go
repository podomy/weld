// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

//nolint:dupl // live gate decoders repeat similar wire checks across types
package ros_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/podomy/weld/internal/transports"
	"github.com/podomy/weld/internal/transports/dds"
	"github.com/podomy/weld/internal/types"
	"github.com/podomy/weld/testbed/live/helpers"
)

// Large payload Image constants for timeouts plus geometry.
//
// Each test allocates one exclusive domain from the parallel pool, so
// the allocated domain isolates runs. The Image is about
// 100KiB so one sample exceeds a single DATA MTU and fragments on the
// wire while staying far under the 32MiB maxPayloadLen cap that bounds
// DATA_FRAG reassembly. Timeouts are generous so fragment reassembly
// plus SEDP matching complete before the poll ends.
const (
	largePayloadOverall  time.Duration = 4 * time.Minute
	largePayloadSettle   time.Duration = 6 * time.Second
	largePayloadPoll     time.Duration = 90 * time.Second
	largePayloadRepub    time.Duration = 2 * time.Second
	largePayloadClean    time.Duration = 10 * time.Second
	largePayloadProbe    time.Duration = 5 * time.Second
	largePayloadPubRetry time.Duration = 15 * time.Second
	largePayloadPubGap   time.Duration = 200 * time.Millisecond
	largePayloadHeight   uint32        = 320
	largePayloadWidth    uint32        = 320
	largePayloadStep     uint32        = 320
	largePayloadEncoding               = "mono8"
	largePayloadRosType                = "sensor_msgs/msg/Image"
	largePayloadIDL                    = "sensor_msgs__msg__Image"
)

// largePayloadDataLen is height times width for mono8.
const largePayloadDataLen = 320 * 320

// largeEchoTruncateLen matches ros2 topic echo DEFAULT_TRUNCATE_LENGTH.
// Echo shows the first 128 array items plus a '...' marker when data is larger.
const largeEchoTruncateLen = 128

// isLargeEchoTruncationToken reports whether s is the echo truncation marker.
// It accepts ... with or without single or double quotes and surrounding space.
func isLargeEchoTruncationToken(s string) bool {
	trimmed := strings.TrimSpace(s)
	trimmed = strings.Trim(trimmed, "'\"")
	trimmed = strings.TrimSpace(trimmed)
	return trimmed == "..."
}

// isLargeBlockTruncation reports whether a trimmed line is the echo truncation marker.
// It accepts a bare ... line plus the block form - '...' used for truncated arrays.
func isLargeBlockTruncation(trimmed string) bool {
	if trimmed == "..." {
		return true
	}
	if !strings.HasPrefix(trimmed, "-") {
		return false
	}
	rest := strings.TrimSpace(strings.TrimPrefix(trimmed, "-"))
	return isLargeEchoTruncationToken(rest)
}

// largeEchoFieldUint parses one numeric header field from an echo doc.
// It scans lines for the "key: value" form and returns the first integer value.
func largeEchoFieldUint(doc, key string) (int, error) {
	for line := range strings.SplitSeq(doc, "\n") {
		trimmed := strings.TrimSpace(line)
		if !strings.HasPrefix(trimmed, key+":") {
			continue
		}
		rest := strings.TrimSpace(strings.TrimPrefix(trimmed, key+":"))
		if rest == "" {
			continue
		}
		field := strings.Fields(rest)[0]
		field = strings.Trim(field, "'\"")
		v, err := strconv.Atoi(field)
		if err != nil {
			return 0, fmt.Errorf("large image echo header %s parse %q: %w", key, field, err)
		}
		return v, nil
	}
	return 0, fmt.Errorf("large image echo header misses %s", key)
}

// checkLargeImageHeaderCount validates header total equals want length.
// Height times width and height times step must both match len want for mono8.
func checkLargeImageHeaderCount(doc string, wantLen int) error {
	height, err := largeEchoFieldUint(doc, "height")
	if err != nil {
		return err
	}
	width, err := largeEchoFieldUint(doc, "width")
	if err != nil {
		return err
	}
	step, err := largeEchoFieldUint(doc, "step")
	if err != nil {
		return err
	}
	if height*width != wantLen {
		return fmt.Errorf("large image echo header total %d wants %d", height*width, wantLen)
	}
	if height*step != wantLen {
		return fmt.Errorf("large image echo header step total %d wants %d", height*step, wantLen)
	}
	return nil
}

// checkLargePC2HeaderCount validates header total equals want length.
// Row step times height and width times point step must both match len want.
func checkLargePC2HeaderCount(doc string, wantLen int) error {
	height, err := largeEchoFieldUint(doc, "height")
	if err != nil {
		return err
	}
	width, err := largeEchoFieldUint(doc, "width")
	if err != nil {
		return err
	}
	pointStep, err := largeEchoFieldUint(doc, "point_step")
	if err != nil {
		return err
	}
	rowStep, err := largeEchoFieldUint(doc, "row_step")
	if err != nil {
		return err
	}
	if rowStep*height != wantLen {
		return fmt.Errorf("large pc2 echo header row total %d wants %d", rowStep*height, wantLen)
	}
	if width*pointStep != wantLen {
		return fmt.Errorf("large pc2 echo header point total %d wants %d", width*pointStep, wantLen)
	}
	return nil
}

// skipIfNoImageType skips when the peer lacks sensor_msgs Image.
//
// Domain is the allocated exclusive domain plumbed to DockerArgs, so
// parallel workers never share a domain.
func skipIfNoImageType(t *testing.T, ctx context.Context, domain int) {
	t.Helper()
	probeCtx, cancel := context.WithTimeout(ctx, largePayloadProbe)
	defer cancel()
	args := helpers.DockerArgs(helpers.Container, domain, helpers.RosBin, "interface", "show", largePayloadRosType)
	cmd := exec.CommandContext(probeCtx, "docker", args...) //nolint:gosec // live harness runs docker with controlled args and no shell
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Skipf("ros2 interface show %s absent, skipping large payload: %v output %s", largePayloadRosType, err, string(out))
	}
}

// buildLargeImageData returns deterministic mono8 bytes of length height times width.
func buildLargeImageData() []byte {
	data := make([]byte, largePayloadDataLen)
	for i := range data {
		data[i] = byte(i % 256)
	}
	return data
}

// encodeLargeImage encodes one Image with live frame_id plus fixed geometry.
func encodeLargeImage(t *testing.T, live string, data []byte) []byte {
	t.Helper()
	enc := types.NewEncoder(true)
	enc.WriteInt32(0)
	enc.WriteUint32(0)
	err := enc.WriteString(live)
	if err != nil {
		t.Fatalf("encode large image frame: %v", err)
	}
	enc.WriteUint32(largePayloadHeight)
	enc.WriteUint32(largePayloadWidth)
	err = enc.WriteString(largePayloadEncoding)
	if err != nil {
		t.Fatalf("encode large image encoding: %v", err)
	}
	enc.WriteUint8(0)
	enc.WriteUint32(largePayloadStep)
	err = enc.WriteOctetSequence(data)
	if err != nil {
		t.Fatalf("encode large image data: %v", err)
	}
	wire := enc.Bytes()
	if len(wire) == 0 {
		t.Fatalf("encoded large image is empty")
	}
	return wire
}

// decodeLargeHeader checks sec plus nanosec plus frame_id.
func decodeLargeHeader(dec *types.Decoder, live string) error {
	sec, err := dec.ReadInt32()
	if err != nil {
		return fmt.Errorf("large image decode sec: %w", err)
	}
	if sec != 0 {
		return fmt.Errorf("large image sec %d wants 0", sec)
	}
	nsec, err := dec.ReadUint32()
	if err != nil {
		return fmt.Errorf("large image decode nanosec: %w", err)
	}
	if nsec != 0 {
		return fmt.Errorf("large image nanosec %d wants 0", nsec)
	}
	frame, err := dec.ReadString()
	if err != nil {
		return fmt.Errorf("large image decode frame: %w", err)
	}
	if frame != live {
		return fmt.Errorf("large image frame %q wants %q", frame, live)
	}
	return nil
}

// decodeLargeDims checks height plus width plus encoding plus endian plus step.
func decodeLargeDims(dec *types.Decoder) error {
	height, err := dec.ReadUint32()
	if err != nil {
		return fmt.Errorf("large image decode height: %w", err)
	}
	if height != largePayloadHeight {
		return fmt.Errorf("large image height %d wants %d", height, largePayloadHeight)
	}
	width, err := dec.ReadUint32()
	if err != nil {
		return fmt.Errorf("large image decode width: %w", err)
	}
	if width != largePayloadWidth {
		return fmt.Errorf("large image width %d wants %d", width, largePayloadWidth)
	}
	encoding, err := dec.ReadString()
	if err != nil {
		return fmt.Errorf("large image decode encoding: %w", err)
	}
	if encoding != largePayloadEncoding {
		return fmt.Errorf("large image encoding %q wants %q", encoding, largePayloadEncoding)
	}
	bigEndian, err := dec.ReadUint8()
	if err != nil {
		return fmt.Errorf("large image decode bigendian: %w", err)
	}
	if bigEndian != 0 {
		return fmt.Errorf("large image bigendian %d wants 0", bigEndian)
	}
	step, err := dec.ReadUint32()
	if err != nil {
		return fmt.Errorf("large image decode step: %w", err)
	}
	if step != largePayloadStep {
		return fmt.Errorf("large image step %d wants %d", step, largePayloadStep)
	}
	return nil
}

// decodeLargeImage checks one Image wire sample against live plus want data.
func decodeLargeImage(payload []byte, live string, want []byte) error {
	dec, err := types.NewDecoder(payload)
	if err != nil {
		return fmt.Errorf("large image decode header: %w", err)
	}
	err = decodeLargeHeader(dec, live)
	if err != nil {
		return err
	}
	err = decodeLargeDims(dec)
	if err != nil {
		return err
	}
	got, err := dec.ReadOctetSequence()
	if err != nil {
		return fmt.Errorf("large image decode data: %w", err)
	}
	if len(got) != len(want) {
		return fmt.Errorf("large image data len %d wants %d", len(got), len(want))
	}
	if !bytes.Equal(got, want) {
		return errors.New("large image data bytes differ") //nolint:wrapcheck // new sentinel error, nothing to wrap
	}
	err = dec.Finish()
	if err != nil {
		return fmt.Errorf("large image finish: %w", err)
	}
	return nil
}

// startLargeEcho starts ros2 topic echo for Image and logs to a file.
func startLargeEcho(t *testing.T, ctx context.Context, domain uint32, topic, logPath string) {
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
	args := helpers.DockerArgs(helpers.Container, int(domain), helpers.RosBin, "topic", "echo", topic, largePayloadRosType)
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

// publishLargeUntilEcho publishes until ros2 echo holds the live ID.
func publishLargeUntilEcho(ctx context.Context, t *testing.T, backend *dds.Backend, msg transports.OutgoingMessage, echoPath, live string) bool {
	t.Helper()
	deadline := time.Now().Add(largePayloadPoll)
	for {
		err := helpers.PublishLiveWithRetry(ctx, backend, msg, largePayloadPubRetry, largePayloadPubGap, "large")
		if err != nil {
			t.Fatalf("weld pub large image: %v", err)
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

// parseLargeFlowData checks flow style bracket data against want bytes.
// A trailing '...' marker means echo truncated the display after the prefix.
// Truncated docs pass when visible items prefix match want and want is longer.
// Missing payload or length mismatch still fails.
//
//nolint:cyclop,gocognit // live flow check asserts header, geometry, and byte windows in one pass
func parseLargeFlowData(rest string, want []byte) error {
	end := strings.Index(rest, "]")
	if end < 0 {
		return errors.New("large image echo flow data misses closing bracket") //nolint:wrapcheck // new sentinel error, nothing to wrap
	}
	inner := rest[1:end]
	parts := strings.Split(inner, ",")
	values := make([]int, 0, len(parts))
	truncated := false
	for _, part := range parts {
		trimmed := strings.TrimSpace(part)
		if trimmed == "" {
			continue
		}
		if isLargeEchoTruncationToken(trimmed) {
			truncated = true
			break
		}
		v, err := strconv.ParseUint(strings.Trim(trimmed, "'\""), 10, 8)
		if err != nil {
			return fmt.Errorf("large image echo flow parse index %d: %w", len(values), err)
		}
		values = append(values, int(v))
	}
	if truncated {
		if len(values) == 0 {
			return errors.New("large image echo flow data holds no values before truncation") //nolint:wrapcheck // new sentinel error, nothing to wrap
		}
		if len(values) > len(want) {
			return fmt.Errorf("large image echo flow truncated count %d exceeds want %d", len(values), len(want))
		}
		for i, v := range values {
			if v != int(want[i]) {
				return fmt.Errorf("large image echo flow byte %d is %d wants %d", i, v, want[i])
			}
		}
		return nil
	}
	if len(values) != len(want) {
		return fmt.Errorf("large image echo flow data count %d wants %d", len(values), len(want))
	}
	for i, v := range values {
		if v != int(want[i]) {
			return fmt.Errorf("large image echo flow byte %d is %d wants %d", i, v, want[i])
		}
	}
	return nil
}

// parseBlockLineValue parses one dash line into a byte value.
//
// It returns parsed true when the line holds a value, parsed false when the
// line is blank or a separator, and done true when the block ends. A parse
// failure returns an error for the caller to report.
func parseBlockLineValue(trimmed string, valuesLen int) (int, bool, bool, error) {
	if trimmed == "" {
		return 0, false, false, nil
	}
	if trimmed == "---" {
		return 0, false, true, nil
	}
	if !strings.HasPrefix(trimmed, "-") {
		if valuesLen > 0 {
			return 0, false, true, nil
		}
		return 0, false, false, nil
	}
	num := strings.TrimSpace(strings.TrimPrefix(trimmed, "-"))
	if num == "" {
		return 0, false, false, nil
	}
	fields := strings.Fields(num)
	v, err := strconv.Atoi(fields[0])
	if err != nil {
		return 0, false, false, fmt.Errorf("large image echo block parse %q: %w", trimmed, err)
	}
	if v < 0 || v > 255 {
		return 0, false, false, fmt.Errorf("large image echo block byte %d out of range %d", valuesLen, v)
	}
	return v, true, false, nil
}

// collectLargeBlockValues scans block dash lines into ints plus truncated flag.
// A trailing - '...' line sets truncated true and ends the scan without error.
func collectLargeBlockValues(rest string, wantLen int) ([]int, bool, error) {
	lines := strings.Split(rest, "\n")
	values := make([]int, 0, wantLen)
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if isLargeBlockTruncation(trimmed) {
			return values, true, nil
		}
		v, parsed, done, err := parseBlockLineValue(trimmed, len(values))
		if err != nil {
			return nil, false, err
		}
		if done {
			break
		}
		if !parsed {
			continue
		}
		values = append(values, v)
	}
	return values, false, nil
}

// checkLargeBlockValues checks collected ints against want bytes.
// Truncated docs pass when visible items prefix match want and want is longer.
// Missing payload or length mismatch still fails.
func checkLargeBlockValues(values []int, truncated bool, want []byte) error {
	if truncated {
		if len(values) == 0 {
			return errors.New("large image echo block data holds no values before truncation") //nolint:wrapcheck // new sentinel error, nothing to wrap
		}
		if len(values) > len(want) {
			return fmt.Errorf("large image echo block truncated count %d exceeds want %d", len(values), len(want))
		}
		for i, v := range values {
			if v != int(want[i]) {
				return fmt.Errorf("large image echo block byte %d is %d wants %d", i, v, want[i])
			}
		}
		return nil
	}
	if len(values) != len(want) {
		return fmt.Errorf("large image echo block data count %d wants %d", len(values), len(want))
	}
	for i, v := range values {
		if v != int(want[i]) {
			return fmt.Errorf("large image echo block byte %d is %d wants %d", i, v, want[i])
		}
	}
	return nil
}

// parseLargeBlockData checks block style dash data against want bytes.
func parseLargeBlockData(rest string, want []byte) error {
	values, truncated, err := collectLargeBlockValues(rest, len(want))
	if err != nil {
		return err
	}
	err = checkLargeBlockValues(values, truncated, want)
	if err != nil {
		return err
	}
	return nil
}

// parseLargeEchoData counts and spot checks Image data values in one echo doc.
// Echo truncates large arrays after the prefix and ends the list with '...'.
// Truncated docs pass when the visible prefix matches want and the header total
// already proves full length. Missing payload still fails.
func parseLargeEchoData(doc string, want []byte) error {
	_, rest, found := strings.Cut(doc, "data:")
	if !found {
		return errors.New("large image echo misses data marker") //nolint:wrapcheck // new sentinel error, nothing to wrap
	}
	rest = strings.TrimSpace(rest)
	if rest == "" {
		return errors.New("large image echo data holds no payload") //nolint:wrapcheck // new sentinel error, nothing to wrap
	}
	if strings.HasPrefix(rest, "[") {
		err := parseLargeFlowData(rest, want)
		if err != nil {
			return err
		}
		return nil
	}
	if !strings.Contains(rest, "-") && strings.Contains(rest, "...") {
		if len(want) <= largeEchoTruncateLen {
			return fmt.Errorf("large image echo string truncation unexpected for want %d", len(want))
		}
		return nil
	}
	err := parseLargeBlockData(rest, want)
	if err != nil {
		return err
	}
	return nil
}

// verifyLargeEcho asserts one echo doc holds live frame_id plus full bytes.
// Echo truncates the data display, so the check validates the visible prefix
// plus first and last visible items plus the header total against want length.
// A trailing '...' marker is accepted. Missing payload or length mismatch fails.
func verifyLargeEcho(t *testing.T, echoPath, live string, want []byte) {
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
	if !strings.Contains(found, strconv.Itoa(int(largePayloadHeight))) {
		t.Fatalf("ros echo doc misses height %d for %q\ndoc:\n%s", largePayloadHeight, live, found)
	}
	if !strings.Contains(found, strconv.Itoa(int(largePayloadWidth))) {
		t.Fatalf("ros echo doc misses width %d for %q\ndoc:\n%s", largePayloadWidth, live, found)
	}
	if !strings.Contains(found, largePayloadEncoding) {
		t.Fatalf("ros echo doc misses encoding %q for %q\ndoc:\n%s", largePayloadEncoding, live, found)
	}
	if !strings.Contains(found, "frame_id") {
		t.Fatalf("ros echo doc misses header frame_id for %q\ndoc:\n%s", live, found)
	}
	err = checkLargeImageHeaderCount(found, len(want))
	if err != nil {
		t.Fatalf("ros echo header count: %v\ndoc:\n%s", err, found)
	}
	err = parseLargeEchoData(found, want)
	if err != nil {
		t.Fatalf("ros echo data check: %v\ndoc tail:\n%s", err, found)
	}
}

// TestROSLargePayloadImageFragmented proves a 100KiB Image flows via Weld.
//
// It builds a 320 by 320 mono8 Image with deterministic bytes and a
// per-run live ID in header frame_id, checks the local XCDR1 codec
// round trips full bytes, then publishes the fragmented XCDR1 sample
// through Weld on one allocated exclusive domain and proves ros2 topic echo in the
// already running weld-jazzy peer sees the live ID plus full geometry
// plus all data bytes. The sample stays under the 32MiB maxPayloadLen
// cap that bounds DATA_FRAG reassembly while exceeding one DATA MTU so
// the wire fragments and reassembles. It uses os/exec docker argv only
// with context timeouts generous for reassembly and skips cleanly when
// no peer runs or when the Image type is absent. Only one run per
// domain and topic at a time is supported.
func TestROSLargePayloadImageFragmented(t *testing.T) {
	t.Parallel()
	domain := helpers.MustAllocateDomain(t)
	ctx, cancel := context.WithTimeout(context.Background(), largePayloadOverall)
	defer cancel()
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)
	skipIfNoImageType(t, ctx, domain)
	runID := helpers.LiveID("ros-large-image")
	topic := helpers.LiveTopic(runID)
	wantData := buildLargeImageData()
	wire := encodeLargeImage(t, runID, wantData)
	err := decodeLargeImage(wire, runID, wantData)
	if err != nil {
		t.Fatalf("local large image codec: %v", err)
	}
	t.Logf("local large image codec ok len %d id %s", len(wire), runID)
	backend := helpers.MustOpenLiveBackend(t, ctx, uint32(domain), "ros-large-image-"+runID, "large") //nolint:gosec // G115: pooled domain 70 through 101 fits uint32
	echoPath := filepath.Join(t.TempDir(), "rosecho-large-image-"+runID+".log")
	startLargeEcho(t, ctx, uint32(domain), topic, echoPath) //nolint:gosec // G115: pooled domain 70 through 101 fits uint32
	helpers.Sleep(ctx, largePayloadSettle)
	cerr := ctx.Err()
	if cerr != nil {
		t.Fatalf("context ended before large pub: %v", cerr)
	}
	msg := transports.OutgoingMessage{
		TopicName: topic,
		TypeName:  largePayloadIDL,
		Payload:   wire,
	}
	if !publishLargeUntilEcho(ctx, t, backend, msg, echoPath, runID) {
		data, rerr := os.ReadFile(echoPath) //nolint:gosec // live harness reads controlled temp file path
		if rerr != nil {
			t.Fatalf("ros echo did not see large image %q: %v", runID, rerr)
		}
		t.Fatalf("ros echo did not see large image %q, pub ok len %d\necho output:\n%s", runID, len(wire), string(data))
	}
	helpers.Sleep(ctx, 2*time.Second)
	verifyLargeEcho(t, echoPath, runID, wantData)
	t.Logf("large image fragmented ok len %d id %s", len(wire), runID)
}

// Large payload PointCloud2 constants for geometry.
//
// The cloud holds 25600
// points of one float32 x field so data is 102400 bytes, about 100KiB.
// One sample exceeds a single DATA MTU and fragments on the wire while
// staying far under the 32MiB maxPayloadLen cap that bounds DATA_FRAG
// reassembly.
const (
	largePayloadPC2RosType  = "sensor_msgs/msg/PointCloud2"
	largePayloadPC2IDL      = "sensor_msgs__msg__PointCloud2"
	largePayloadPC2Height   = uint32(1)
	largePayloadPC2Width    = uint32(25600)
	largePayloadPC2Step     = uint32(4)
	largePayloadPC2RowStep  = uint32(102400)
	largePayloadPC2Datatype = uint8(7)
)

// largePayloadPC2DataLen is width times point step for one float32 field.
const largePayloadPC2DataLen = 25600 * 4

// skipIfNoPointCloud2Type skips when the peer lacks PointCloud2.
//
// Domain is the allocated exclusive domain plumbed to DockerArgs, so
// parallel workers never share a domain.
func skipIfNoPointCloud2Type(t *testing.T, ctx context.Context, domain int) {
	t.Helper()
	probeCtx, cancel := context.WithTimeout(ctx, largePayloadProbe)
	defer cancel()
	args := helpers.DockerArgs(helpers.Container, domain, helpers.RosBin, "interface", "show", largePayloadPC2RosType)
	cmd := exec.CommandContext(probeCtx, "docker", args...) //nolint:gosec // live harness runs docker with controlled args and no shell
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Skipf("ros2 interface show %s absent, skipping large payload: %v output %s", largePayloadPC2RosType, err, string(out))
	}
}

// buildLargePC2Data returns deterministic bytes of length data len.
func buildLargePC2Data() []byte {
	data := make([]byte, largePayloadPC2DataLen)
	for i := range data {
		data[i] = byte(i % 256)
	}
	return data
}

// encodeLargePC2 encodes one PointCloud2 with live frame_id plus fixed shape.
func encodeLargePC2(t *testing.T, live string, data []byte) []byte {
	t.Helper()
	enc := types.NewEncoder(true)
	enc.WriteInt32(0)
	enc.WriteUint32(0)
	err := enc.WriteString(live)
	if err != nil {
		t.Fatalf("encode large pc2 frame: %v", err)
	}
	enc.WriteUint32(largePayloadPC2Height)
	enc.WriteUint32(largePayloadPC2Width)
	err = enc.WriteSequenceLength(1)
	if err != nil {
		t.Fatalf("encode large pc2 fields len: %v", err)
	}
	err = enc.WriteString("x")
	if err != nil {
		t.Fatalf("encode large pc2 field name: %v", err)
	}
	enc.WriteUint32(0)
	enc.WriteUint8(largePayloadPC2Datatype)
	enc.WriteUint32(1)
	enc.WriteBool(false)
	enc.WriteUint32(largePayloadPC2Step)
	enc.WriteUint32(largePayloadPC2RowStep)
	err = enc.WriteOctetSequence(data)
	if err != nil {
		t.Fatalf("encode large pc2 data: %v", err)
	}
	enc.WriteBool(true)
	wire := enc.Bytes()
	if len(wire) == 0 {
		t.Fatalf("encoded large pc2 is empty")
	}
	return wire
}

// decodeLargePC2Header checks sec plus nanosec plus frame_id.
func decodeLargePC2Header(dec *types.Decoder, live string) error {
	sec, err := dec.ReadInt32()
	if err != nil {
		return fmt.Errorf("large pc2 decode sec: %w", err)
	}
	if sec != 0 {
		return fmt.Errorf("large pc2 sec %d wants 0", sec)
	}
	nsec, err := dec.ReadUint32()
	if err != nil {
		return fmt.Errorf("large pc2 decode nanosec: %w", err)
	}
	if nsec != 0 {
		return fmt.Errorf("large pc2 nanosec %d wants 0", nsec)
	}
	frame, err := dec.ReadString()
	if err != nil {
		return fmt.Errorf("large pc2 decode frame: %w", err)
	}
	if frame != live {
		return fmt.Errorf("large pc2 frame %q wants %q", frame, live)
	}
	return nil
}

// decodeLargePC2Geometry checks height plus width plus fields length.
func decodeLargePC2Geometry(dec *types.Decoder) error {
	height, err := dec.ReadUint32()
	if err != nil {
		return fmt.Errorf("large pc2 decode height: %w", err)
	}
	if height != largePayloadPC2Height {
		return fmt.Errorf("large pc2 height %d wants %d", height, largePayloadPC2Height)
	}
	width, err := dec.ReadUint32()
	if err != nil {
		return fmt.Errorf("large pc2 decode width: %w", err)
	}
	if width != largePayloadPC2Width {
		return fmt.Errorf("large pc2 width %d wants %d", width, largePayloadPC2Width)
	}
	n, err := dec.ReadSequenceLength()
	if err != nil {
		return fmt.Errorf("large pc2 decode fields len: %w", err)
	}
	if n != 1 {
		return fmt.Errorf("large pc2 fields len %d wants 1", n)
	}
	return nil
}

// decodeLargePC2Field checks the single x field descriptor.
func decodeLargePC2Field(dec *types.Decoder) error {
	name, err := dec.ReadString()
	if err != nil {
		return fmt.Errorf("large pc2 decode field name: %w", err)
	}
	if name != "x" {
		return fmt.Errorf("large pc2 field name %q wants %q", name, "x")
	}
	off, err := dec.ReadUint32()
	if err != nil {
		return fmt.Errorf("large pc2 decode offset: %w", err)
	}
	if off != 0 {
		return fmt.Errorf("large pc2 offset %d wants 0", off)
	}
	dt, err := dec.ReadUint8()
	if err != nil {
		return fmt.Errorf("large pc2 decode datatype: %w", err)
	}
	if dt != largePayloadPC2Datatype {
		return fmt.Errorf("large pc2 datatype %d wants %d", dt, largePayloadPC2Datatype)
	}
	cnt, err := dec.ReadUint32()
	if err != nil {
		return fmt.Errorf("large pc2 decode count: %w", err)
	}
	if cnt != 1 {
		return fmt.Errorf("large pc2 count %d wants 1", cnt)
	}
	return nil
}

// decodeLargePC2Dims checks height plus width plus the single x field.
func decodeLargePC2Dims(dec *types.Decoder) error {
	err := decodeLargePC2Geometry(dec)
	if err != nil {
		return err
	}
	err = decodeLargePC2Field(dec)
	if err != nil {
		return err
	}
	return nil
}

// decodeLargePC2Steps checks endian plus steps plus data plus dense.
func decodeLargePC2Steps(dec *types.Decoder, want []byte) error {
	big, err := dec.ReadBool()
	if err != nil {
		return fmt.Errorf("large pc2 decode bigendian: %w", err)
	}
	if big {
		return errors.New("large pc2 bigendian true wants false") //nolint:wrapcheck // new sentinel error, nothing to wrap
	}
	step, err := dec.ReadUint32()
	if err != nil {
		return fmt.Errorf("large pc2 decode step: %w", err)
	}
	if step != largePayloadPC2Step {
		return fmt.Errorf("large pc2 step %d wants %d", step, largePayloadPC2Step)
	}
	row, err := dec.ReadUint32()
	if err != nil {
		return fmt.Errorf("large pc2 decode row: %w", err)
	}
	if row != largePayloadPC2RowStep {
		return fmt.Errorf("large pc2 row %d wants %d", row, largePayloadPC2RowStep)
	}
	got, err := dec.ReadOctetSequence()
	if err != nil {
		return fmt.Errorf("large pc2 decode data: %w", err)
	}
	if len(got) != len(want) {
		return fmt.Errorf("large pc2 data len %d wants %d", len(got), len(want))
	}
	if !bytes.Equal(got, want) {
		return errors.New("large pc2 data bytes differ") //nolint:wrapcheck // new sentinel error, nothing to wrap
	}
	dense, err := dec.ReadBool()
	if err != nil {
		return fmt.Errorf("large pc2 decode dense: %w", err)
	}
	if !dense {
		return errors.New("large pc2 dense false wants true") //nolint:wrapcheck // new sentinel error, nothing to wrap
	}
	return nil
}

// decodeLargePC2 checks one PointCloud2 wire sample against live plus want.
func decodeLargePC2(payload []byte, live string, want []byte) error {
	dec, err := types.NewDecoder(payload)
	if err != nil {
		return fmt.Errorf("large pc2 decode header: %w", err)
	}
	err = decodeLargePC2Header(dec, live)
	if err != nil {
		return err
	}
	err = decodeLargePC2Dims(dec)
	if err != nil {
		return err
	}
	err = decodeLargePC2Steps(dec, want)
	if err != nil {
		return err
	}
	err = dec.Finish()
	if err != nil {
		return fmt.Errorf("large pc2 finish: %w", err)
	}
	return nil
}

// startLargePC2Echo starts ros2 topic echo for PointCloud2 and logs to a file.
func startLargePC2Echo(t *testing.T, ctx context.Context, domain uint32, topic, logPath string) {
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
	args := helpers.DockerArgs(helpers.Container, int(domain), helpers.RosBin, "topic", "echo", topic, largePayloadPC2RosType)
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

// publishLargePC2UntilEcho publishes until ros2 echo holds the live ID.
func publishLargePC2UntilEcho(ctx context.Context, t *testing.T, backend *dds.Backend, msg transports.OutgoingMessage, echoPath, live string) bool {
	t.Helper()
	deadline := time.Now().Add(largePayloadPoll)
	for {
		err := helpers.PublishLiveWithRetry(ctx, backend, msg, largePayloadPubRetry, largePayloadPubGap, "large-pc2")
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

// findLargePC2Doc returns the echo doc holding the live ID.
func findLargePC2Doc(t *testing.T, text, live string) string {
	t.Helper()
	if !strings.Contains(text, live) {
		t.Fatalf("ros echo misses live ID %q\necho output:\n%s", live, text)
	}
	for doc := range strings.SplitSeq(text, "---") {
		if strings.Contains(doc, live) {
			return doc
		}
	}
	t.Fatalf("ros echo has live ID %q but no doc holds it\necho output:\n%s", live, text)
	return ""
}

// checkLargePC2Geometry asserts header geometry strings in one echo doc.
func checkLargePC2Geometry(t *testing.T, found, live string) {
	t.Helper()
	for _, want := range []string{"frame_id", "height: 1", "width: 25600", "point_step: 4", "row_step: 102400"} {
		if !strings.Contains(found, want) {
			t.Fatalf("ros echo doc misses %q for %q\ndoc:\n%s", want, live, found)
		}
	}
}

// verifyLargePC2Echo asserts one echo doc holds live frame_id plus full bytes.
// Echo truncates the data display, so the check validates the visible prefix
// plus first and last visible items plus the header total against want length.
// A trailing '...' marker is accepted. Missing payload or length mismatch fails.
func verifyLargePC2Echo(t *testing.T, echoPath, live string, want []byte) {
	t.Helper()
	data, err := os.ReadFile(echoPath) //nolint:gosec // live harness reads controlled temp file path
	if err != nil {
		t.Fatalf("read ros echo log: %v", err)
	}
	text := string(data)
	found := findLargePC2Doc(t, text, live)
	checkLargePC2Geometry(t, found, live)
	err = checkLargePC2HeaderCount(found, len(want))
	if err != nil {
		t.Fatalf("ros echo header count: %v\ndoc:\n%s", err, found)
	}
	err = parseLargeEchoData(found, want)
	if err != nil {
		t.Fatalf("ros echo data check: %v\ndoc tail:\n%s", err, found)
	}
}

// TestROSLargePayloadPointCloud2Fragmented proves a 100KiB PointCloud2 flows via Weld.
//
// It builds a 1 by 25600 cloud with one float32 x field and deterministic
// bytes plus a per-run live ID in header frame_id, checks the local XCDR1
// codec round trips full bytes, then publishes the fragmented XCDR1 sample
// through Weld on one allocated exclusive domain and proves ros2 topic echo in the
// already running weld-jazzy peer sees the live ID plus full geometry plus
// all data bytes. The sample stays under the 32MiB maxPayloadLen cap that
// bounds DATA_FRAG reassembly while exceeding one DATA MTU so the wire
// fragments and reassembles. It uses os/exec docker argv only with context
// timeouts generous for reassembly and skips cleanly when no peer runs or
// when the PointCloud2 type is absent. Only one run per domain and topic
// at a time is supported.
func TestROSLargePayloadPointCloud2Fragmented(t *testing.T) {
	t.Parallel()
	domain := helpers.MustAllocateDomain(t)
	ctx, cancel := context.WithTimeout(context.Background(), largePayloadOverall)
	defer cancel()
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)
	skipIfNoPointCloud2Type(t, ctx, domain)
	runID := helpers.LiveID("ros-large-pc2")
	topic := helpers.LiveTopic(runID)
	wantData := buildLargePC2Data()
	wire := encodeLargePC2(t, runID, wantData)
	err := decodeLargePC2(wire, runID, wantData)
	if err != nil {
		t.Fatalf("local large pc2 codec: %v", err)
	}
	t.Logf("local large pc2 codec ok len %d id %s", len(wire), runID)
	backend := helpers.MustOpenLiveBackend(t, ctx, uint32(domain), "ros-large-pc2-"+runID, "large-pc2") //nolint:gosec // G115: pooled domain 70 through 101 fits uint32
	echoPath := filepath.Join(t.TempDir(), "rosecho-large-pc2-"+runID+".log")
	startLargePC2Echo(t, ctx, uint32(domain), topic, echoPath) //nolint:gosec // G115: pooled domain 70 through 101 fits uint32
	helpers.Sleep(ctx, largePayloadSettle)
	cerr := ctx.Err()
	if cerr != nil {
		t.Fatalf("context ended before large pc2 pub: %v", cerr)
	}
	msg := transports.OutgoingMessage{
		TopicName: topic,
		TypeName:  largePayloadPC2IDL,
		Payload:   wire,
	}
	if !publishLargePC2UntilEcho(ctx, t, backend, msg, echoPath, runID) {
		data, rerr := os.ReadFile(echoPath) //nolint:gosec // live harness reads controlled temp file path
		if rerr != nil {
			t.Fatalf("ros echo did not see large pc2 %q: %v", runID, rerr)
		}
		t.Fatalf("ros echo did not see large pc2 %q, pub ok len %d\necho output:\n%s", runID, len(wire), string(data))
	}
	helpers.Sleep(ctx, 2*time.Second)
	verifyLargePC2Echo(t, echoPath, runID, wantData)
	t.Logf("large pc2 fragmented ok len %d id %s", len(wire), runID)
}
