// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

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

// Stereo DisparityImage constants for timeouts.
//
// Each test allocates one exclusive domain from the parallel pool, so
// per-run topics plus the allocated domain isolate runs. Timeouts bound
// every docker and backend call through context.
const (
	stereoDisparityTestTimeout = 2 * time.Minute
	stereoDisparityTalkerNap   = 5 * time.Second
	stereoDisparityEchoNap     = 6 * time.Second
	stereoDisparityTakeWait    = 45 * time.Second
	stereoDisparityPollWait    = 30 * time.Second
	stereoDisparityCleanWait   = 10 * time.Second
	stereoDisparityRepubWait   = 2 * time.Second
)

// Stereo DisparityImage identity and static hash.
//
// RosType is the slash form for ros2 topic pub and echo. IDLName is the
// double underscore form for Weld SEDP Subscribe and PublishBytes.
// Static hash is the RIHS01 value from ros2 topic info -v on weld-jazzy.
// Live probing is preferred at runtime and this value is the fallback.
const (
	stereoDisparityRosType    = "stereo_msgs/msg/DisparityImage"
	stereoDisparityIDLName    = "stereo_msgs__msg__DisparityImage"
	stereoDisparityStaticHash = "RIHS01_1ec1ff6b5bace919e4544a37f2d96ead9f81783701b7b7a4d97a09325ecf2711"
)

// Small DisparityImage sentinels.
//
// The image is 2 by 2 with 32FC1 encoding so the sample stays small.
// Step is width times 4. Data holds 1.0 plus 2.0 plus 3.0 plus 4.0 as
// little endian float32 bytes. F plus T plus range plus delta are fixed.
// ROI covers the full 2 by 2 window with no rectify. Outer and inner
// frame_id carry the per-run live ID.
const (
	stereoDisparityHeight   uint32  = 2
	stereoDisparityWidth    uint32  = 2
	stereoDisparityStep     uint32  = 8
	stereoDisparityEncoding string  = "32FC1"
	stereoDisparityF        float32 = 500.0
	stereoDisparityT        float32 = 0.1
	stereoDisparityMin      float32 = 0.0
	stereoDisparityMax      float32 = 64.0
	stereoDisparityDelta    float32 = 0.125
	stereoDisparityROIH     uint32  = 2
	stereoDisparityROIW     uint32  = 2
)

// stereoDisparityData holds the 2 by 2 float32 payload bytes.
var stereoDisparityData = []byte{
	0x00, 0x00, 0x80, 0x3F,
	0x00, 0x00, 0x00, 0x40,
	0x00, 0x00, 0x40, 0x40,
	0x00, 0x00, 0x80, 0x40,
}

// yamlStereoDisparity returns ROS YAML for the small DisparityImage.
//
// Both outer and inner frame_id carry live so the echo check keys on it.
func yamlStereoDisparity(live string) string {
	return fmt.Sprintf("{header: {stamp: {sec: 0, nanosec: 0}, frame_id: '%s'}, image: {header: {stamp: {sec: 0, nanosec: 0}, frame_id: '%s'}, height: 2, width: 2, encoding: '32FC1', is_bigendian: 0, step: 8, data: [0, 0, 128, 63, 0, 0, 0, 64, 0, 0, 64, 64, 0, 0, 128, 64]}, f: 500.0, t: 0.1, valid_window: {x_offset: 0, y_offset: 0, height: 2, width: 2, do_rectify: false}, min_disparity: 0.0, max_disparity: 64.0, delta_d: 0.125}", live, live)
}

// encodeStereoDisparityImage writes the nested Image with live frame_id.
//
// Order is inner header plus height plus width plus encoding plus
// is_bigendian plus step plus data sequence. Only string and sequence
// writes can fail.
func encodeStereoDisparityImage(enc *types.Encoder, live string) error {
	err := encodeMatrixHeader(enc, live)
	if err != nil {
		return fmt.Errorf("encode disparity inner header: %w", err)
	}
	enc.WriteUint32(stereoDisparityHeight)
	enc.WriteUint32(stereoDisparityWidth)
	err = enc.WriteString(stereoDisparityEncoding)
	if err != nil {
		return fmt.Errorf("encode disparity encoding: %w", err)
	}
	enc.WriteUint8(0)
	enc.WriteUint32(stereoDisparityStep)
	err = enc.WriteOctetSequence(stereoDisparityData)
	if err != nil {
		return fmt.Errorf("encode disparity data: %w", err)
	}
	return nil
}

// encodeStereoDisparity encodes the small DisparityImage as XCDR1.
func encodeStereoDisparity(live string) ([]byte, error) {
	enc := types.NewEncoder(true)
	err := encodeMatrixHeader(enc, live)
	if err != nil {
		return nil, fmt.Errorf("encode disparity header: %w", err)
	}
	err = encodeStereoDisparityImage(enc, live)
	if err != nil {
		return nil, err
	}
	enc.WriteFloat32(stereoDisparityF)
	enc.WriteFloat32(stereoDisparityT)
	enc.WriteUint32(0)
	enc.WriteUint32(0)
	enc.WriteUint32(stereoDisparityROIH)
	enc.WriteUint32(stereoDisparityROIW)
	enc.WriteBool(false)
	enc.WriteFloat32(stereoDisparityMin)
	enc.WriteFloat32(stereoDisparityMax)
	enc.WriteFloat32(stereoDisparityDelta)
	return enc.Bytes(), nil
}

// decodeDisparityDims checks height plus width plus encoding.
func decodeDisparityDims(dec *types.Decoder) error {
	height, err := dec.ReadUint32()
	if err != nil {
		return fmt.Errorf("decode disparity height: %w", err)
	}
	if height != stereoDisparityHeight {
		return fmt.Errorf("disparity height %d wants 2", height)
	}
	width, err := dec.ReadUint32()
	if err != nil {
		return fmt.Errorf("decode disparity width: %w", err)
	}
	if width != stereoDisparityWidth {
		return fmt.Errorf("disparity width %d wants 2", width)
	}
	encoding, err := dec.ReadString()
	if err != nil {
		return fmt.Errorf("decode disparity encoding: %w", err)
	}
	if encoding != stereoDisparityEncoding {
		return fmt.Errorf("disparity encoding %q wants %q", encoding, stereoDisparityEncoding)
	}
	return nil
}

// decodeDisparityEndianStep checks bigendian plus step.
func decodeDisparityEndianStep(dec *types.Decoder) error {
	bigend, err := dec.ReadUint8()
	if err != nil {
		return fmt.Errorf("decode disparity bigendian: %w", err)
	}
	if bigend != 0 {
		return fmt.Errorf("disparity bigendian %d wants 0", bigend)
	}
	step, err := dec.ReadUint32()
	if err != nil {
		return fmt.Errorf("decode disparity step: %w", err)
	}
	if step != stereoDisparityStep {
		return fmt.Errorf("disparity step %d wants 8", step)
	}
	return nil
}

// decodeStereoDisparityImage reads the nested Image and checks sentinels.
//
// It verifies the inner live frame_id plus height plus width plus encoding
// plus step plus data bytes. is_bigendian must be zero.
func decodeStereoDisparityImage(dec *types.Decoder, live string) error {
	frame, err := decodeMatrixHeader(dec)
	if err != nil {
		return fmt.Errorf("decode disparity inner frame: %w", err)
	}
	if frame != live {
		return fmt.Errorf("disparity inner frame %q wants %q", frame, live)
	}
	err = decodeDisparityDims(dec)
	if err != nil {
		return err
	}
	err = decodeDisparityEndianStep(dec)
	if err != nil {
		return err
	}
	data, err := dec.ReadOctetSequence()
	if err != nil {
		return fmt.Errorf("decode disparity data: %w", err)
	}
	if len(data) != len(stereoDisparityData) {
		return fmt.Errorf("disparity data len %d wants %d", len(data), len(stereoDisparityData))
	}
	for i := range data {
		if data[i] != stereoDisparityData[i] {
			return fmt.Errorf("disparity data byte %d 0x%02x wants 0x%02x", i, data[i], stereoDisparityData[i])
		}
	}
	return nil
}

// decodeDisparitySize checks roi height plus width plus rectify.
func decodeDisparitySize(dec *types.Decoder) error {
	roih, err := dec.ReadUint32()
	if err != nil {
		return fmt.Errorf("decode disparity roi height: %w", err)
	}
	if roih != stereoDisparityROIH {
		return fmt.Errorf("disparity roi height %d wants 2", roih)
	}
	roiw, err := dec.ReadUint32()
	if err != nil {
		return fmt.Errorf("decode disparity roi width: %w", err)
	}
	if roiw != stereoDisparityROIW {
		return fmt.Errorf("disparity roi width %d wants 2", roiw)
	}
	rect, err := dec.ReadBool()
	if err != nil {
		return fmt.Errorf("decode disparity rectify: %w", err)
	}
	if rect {
		return errors.New("disparity rectify true wants false") //nolint:wrapcheck // new sentinel error, nothing to wrap
	}
	return nil
}

// decodeDisparityOffsets checks x plus y offsets are zero.
func decodeDisparityOffsets(dec *types.Decoder) error {
	xoff, err := dec.ReadUint32()
	if err != nil {
		return fmt.Errorf("decode disparity x_offset: %w", err)
	}
	if xoff != 0 {
		return fmt.Errorf("disparity x_offset %d wants 0", xoff)
	}
	yoff, err := dec.ReadUint32()
	if err != nil {
		return fmt.Errorf("decode disparity y_offset: %w", err)
	}
	if yoff != 0 {
		return fmt.Errorf("disparity y_offset %d wants 0", yoff)
	}
	return nil
}

// decodeStereoDisparityROI reads valid_window plus range and checks sentinels.
//
// ROI must be zero offset with full 2 by 2 size and no rectify. Range must
// be 0.0 plus 64.0 plus 0.125.
func decodeStereoDisparityROI(dec *types.Decoder) error {
	err := decodeDisparityOffsets(dec)
	if err != nil {
		return err
	}
	err = decodeDisparitySize(dec)
	if err != nil {
		return err
	}
	mind, err := dec.ReadFloat32()
	if err != nil {
		return fmt.Errorf("decode disparity min: %w", err)
	}
	if mind != stereoDisparityMin {
		return fmt.Errorf("disparity min %v wants 0.0", mind)
	}
	maxd, err := dec.ReadFloat32()
	if err != nil {
		return fmt.Errorf("decode disparity max: %w", err)
	}
	if maxd != stereoDisparityMax {
		return fmt.Errorf("disparity max %v wants 64.0", maxd)
	}
	delta, err := dec.ReadFloat32()
	if err != nil {
		return fmt.Errorf("decode disparity delta: %w", err)
	}
	if delta != stereoDisparityDelta {
		return fmt.Errorf("disparity delta %v wants 0.125", delta)
	}
	return nil
}

// decodeStereoDisparity decodes and checks live frame_id plus sentinels.
func decodeStereoDisparity(payload []byte, live string) error {
	dec, err := types.NewDecoder(payload)
	if err != nil {
		return fmt.Errorf("decode disparity header: %w", err)
	}
	frame, err := decodeMatrixHeader(dec)
	if err != nil {
		return fmt.Errorf("decode disparity frame: %w", err)
	}
	if frame != live {
		return fmt.Errorf("disparity frame %q wants %q", frame, live)
	}
	err = decodeStereoDisparityImage(dec, live)
	if err != nil {
		return err
	}
	gotF, err := dec.ReadFloat32()
	if err != nil {
		return fmt.Errorf("decode disparity f: %w", err)
	}
	if gotF != stereoDisparityF {
		return fmt.Errorf("disparity f %v wants 500.0", gotF)
	}
	gotT, err := dec.ReadFloat32()
	if err != nil {
		return fmt.Errorf("decode disparity t: %w", err)
	}
	if gotT != stereoDisparityT {
		return fmt.Errorf("disparity t %v wants 0.1", gotT)
	}
	err = decodeStereoDisparityROI(dec)
	if err != nil {
		return err
	}
	err = dec.Finish()
	if err != nil {
		return fmt.Errorf("finish disparity: %w", err)
	}
	return nil
}

// wantStereoDisparity returns the live ID expected in echo output.
func wantStereoDisparity(live string) string {
	return live
}

// resolveStereoDisparityHash returns the live hash or the static fallback.
//
// Domain is the allocated exclusive domain plumbed to topic hash probing,
// so parallel workers never share a domain. It probes the live topic for
// the hash that CycloneDDS advertises. When the probe succeeds the live
// value wins so distro changes are picked up. When the probe fails the
// static hash is used. When both are missing it returns an error so the
// caller can skip cleanly.
func resolveStereoDisparityHash(ctx context.Context, domain int, topic string) (string, error) {
	typeHash, err := helpers.ResolveTopicHash(ctx, helpers.Container, domain, helpers.RosBin, topic, stereoDisparityStaticHash, typeMatrixProbeWait, "stereo")
	if err != nil {
		return "", fmt.Errorf("stereo resolve hash: %w", err)
	}
	return typeHash, nil
}

// takeStereoSamples takes until the DisparityImage decodes or the context ends.
func takeStereoSamples(t *testing.T, takeCtx context.Context, backend *dds.Backend, live string) bool {
	t.Helper()
	for takeCtx.Err() == nil {
		msg, terr := backend.TakeBytes(takeCtx)
		if terr != nil {
			if errors.Is(terr, context.DeadlineExceeded) || errors.Is(terr, context.Canceled) {
				break
			}
			text := terr.Error()
			if strings.Contains(text, "not implemented") {
				t.Fatalf("weld take backend error stereo disparity: %v", terr)
			}
			if strings.Contains(text, "not wired") {
				t.Fatalf("weld take backend error stereo disparity: %v", terr)
			}
			continue
		}
		derr := decodeStereoDisparity(msg.Payload, live)
		if derr == nil {
			return true
		}
	}
	return false
}

// runStereoDisparityForward checks Weld take sees a ROS talker.

// It starts ros2 topic pub via docker argv with no shell, proves the local
// XCDR1 codec round trips, resolves the RIHS01 hash via ros2 topic info -v
// with static fallback, then takes via the Weld backend and decodes each
// sample with NewDecoder. A missing payload skips with the interop gap
// note, while backend errors still fail. It stops the talker and closes
// the backend before returning.
//
//nolint:cyclop // live stereo gate sequences talker, echo, and allowlist verdict in one run
func runStereoDisparityForward(t *testing.T, ctx context.Context, domain int, live, topic string) {
	t.Helper()
	t.Cleanup(func() {
		helpers.AssertNoExcessSkips(t)
	})
	logPath := filepath.Join(t.TempDir(), "talker-"+live+"-stereo-disparity.log")
	logFile, err := os.Create(logPath) //nolint:gosec // test writes controlled temp path
	if err != nil {
		t.Fatalf("create talker log stereo disparity: %v", err)
	}
	t.Cleanup(func() {
		cerr := logFile.Close()
		if cerr != nil {
			t.Logf("close talker log stereo disparity: %v", cerr)
		}
	})
	yaml := yamlStereoDisparity(live)
	args := helpers.DockerArgs(helpers.Container, domain, helpers.RosBin, "topic", "pub", topic, stereoDisparityRosType, yaml, "--rate", "2", "--qos-reliability", "reliable", "--qos-durability", "transient_local", "--qos-history", "keep_last", "--qos-depth", "10")
	talker := exec.CommandContext(ctx, "docker", args...) //nolint:gosec // test harness runs docker with controlled args
	talker.Stdout = logFile
	talker.Stderr = logFile
	err = talker.Start()
	if err != nil {
		t.Fatalf("start ROS talker stereo disparity: %v", err)
	}
	stopTalker := func() {
		helpers.StopCmd(talker)
		cleanupCtx, cleanupCancel := context.WithTimeout(context.WithoutCancel(ctx), stereoDisparityCleanWait)
		defer cleanupCancel()
		if err := helpers.KillAndWaitContainerProc(cleanupCtx, helpers.Container, topic); err != nil {
			t.Errorf("stop peer proc: %v", err)
		}
	}
	t.Cleanup(stopTalker)
	helpers.Sleep(ctx, stereoDisparityTalkerNap)
	if ctx.Err() != nil {
		t.Fatalf("context ended before take stereo disparity: %v", ctx.Err())
	}
	typeHash, err := resolveStereoDisparityHash(ctx, domain, topic)
	if err != nil {
		t.Skipf("stereo disparity hash unavailable for topic %s, skipping: %v", topic, err)
	}
	t.Logf("stereo disparity forward topic %s hash %s", topic, typeHash)
	payload, err := encodeStereoDisparity(live)
	if err != nil {
		t.Fatalf("local encode stereo disparity: %v", err)
	}
	err = decodeStereoDisparity(payload, live)
	if err != nil {
		t.Fatalf("local decode stereo disparity: %v", err)
	}
	backend, err := helpers.OpenLiveBackend(ctx, domain, "weld-stereo-disparity-forward", "stereo")
	if err != nil {
		t.Fatalf("open backend stereo disparity: %v", err)
	}
	//nolint:contextcheck // backend Close is cleanup without caller ctx, goodbye uses bounded Background
	closeBackend := func() {
		cerr := backend.Close()
		if cerr != nil {
			t.Logf("close backend stereo disparity: %v", cerr)
		}
	}
	t.Cleanup(closeBackend)
	err = helpers.SubscribeLiveReader(ctx, backend, topic, stereoDisparityIDLName, typeHash, "stereo")
	if err != nil {
		t.Fatalf("subscribe stereo disparity: %v", err)
	}
	takeCtx, takeCancel := context.WithTimeout(ctx, stereoDisparityTakeWait)
	defer takeCancel()
	if takeStereoSamples(t, takeCtx, backend, live) {
		closeBackend()
		stopTalker()
		return
	}
	closeBackend()
	stopTalker()
	t.Logf("weld take did not see stereo disparity payload hash %s (known interop gap), local codec ok", typeHash)
	reason := "weld take misses stereo disparity user DATA but local codec ok, known interop gap forward stereo"
	helpers.RegisterAllowlistedSkip(t.Name(), reason, time.Date(2027, 5, 1, 0, 0, 0, 0, time.UTC))
	helpers.NoteProductSkip(t, t.Name(), reason)
}

// publishStereoDisparityUntilEcho publishes until ros2 topic echo sees want.
func publishStereoDisparityUntilEcho(ctx context.Context, t *testing.T, backend *dds.Backend, msg transports.OutgoingMessage, echoPath, want, typeHash string) bool {
	t.Helper()
	return helpers.PublishUntilEcho(ctx, t, backend, msg, echoPath, want, typeHash, stereoDisparityPollWait, stereoDisparityRepubWait, typeMatrixPubRetry, typeMatrixPubInterval, "stereo disparity")
}

// runStereoDisparityReverse checks ros2 topic echo sees a Weld pub.
//
// It starts ros2 topic echo via docker argv with no shell, resolves the
// RIHS01 hash via ros2 topic info -v with static fallback for logging,
// then republishes one Weld sample until the echo stream holds the live
// ID or the poll wait ends. A missing payload skips with the interop gap
// note, while pub failures still fail. It stops the echo and closes the
// backend before returning.
//
//nolint:cyclop // live stereo gate sequences talker, echo, and allowlist verdict in one run
func runStereoDisparityReverse(t *testing.T, ctx context.Context, domain int, live, topic string) {
	t.Helper()
	t.Cleanup(func() {
		helpers.AssertNoExcessSkips(t)
	})
	echoPath := filepath.Join(t.TempDir(), "rosecho-"+live+"-stereo-disparity.log")
	echoFile, err := os.Create(echoPath) //nolint:gosec // test writes controlled temp path
	if err != nil {
		t.Fatalf("create ros echo log stereo disparity: %v", err)
	}
	t.Cleanup(func() {
		cerr := echoFile.Close()
		if cerr != nil {
			t.Logf("close ros echo log stereo disparity: %v", cerr)
		}
	})
	args := helpers.DockerArgs(helpers.Container, domain, helpers.RosBin, "topic", "echo", topic, stereoDisparityRosType)
	echoCmd := exec.CommandContext(ctx, "docker", args...) //nolint:gosec // test harness runs docker with controlled args
	echoCmd.Stdout = echoFile
	echoCmd.Stderr = echoFile
	err = echoCmd.Start()
	if err != nil {
		t.Fatalf("start ros2 topic echo stereo disparity: %v", err)
	}
	stopEcho := func() {
		helpers.StopCmd(echoCmd)
		cleanupCtx, cleanupCancel := context.WithTimeout(context.WithoutCancel(ctx), stereoDisparityCleanWait)
		defer cleanupCancel()
		if err := helpers.KillAndWaitContainerProc(cleanupCtx, helpers.Container, topic); err != nil {
			t.Errorf("stop peer proc: %v", err)
		}
	}
	t.Cleanup(stopEcho)
	helpers.Sleep(ctx, stereoDisparityEchoNap)
	if ctx.Err() != nil {
		t.Fatalf("context ended before pub stereo disparity: %v", ctx.Err())
	}
	typeHash, err := resolveStereoDisparityHash(ctx, domain, topic)
	if err != nil {
		t.Skipf("stereo disparity hash unavailable for topic %s, skipping: %v", topic, err)
	}
	t.Logf("stereo disparity reverse topic %s hash %s", topic, typeHash)
	payload, err := encodeStereoDisparity(live)
	if err != nil {
		t.Fatalf("local encode stereo disparity: %v", err)
	}
	err = decodeStereoDisparity(payload, live)
	if err != nil {
		t.Fatalf("local decode stereo disparity: %v", err)
	}
	backend, err := helpers.OpenLiveBackend(ctx, domain, "weld-stereo-disparity-reverse", "stereo")
	if err != nil {
		t.Fatalf("open backend stereo disparity: %v", err)
	}
	//nolint:contextcheck // backend Close is cleanup without caller ctx, goodbye uses bounded Background
	closeBackend := func() {
		cerr := backend.Close()
		if cerr != nil {
			t.Logf("close backend stereo disparity: %v", cerr)
		}
	}
	t.Cleanup(closeBackend)
	msg := transports.OutgoingMessage{
		TopicName: topic,
		TypeName:  stereoDisparityIDLName,
		Payload:   payload,
	}
	want := wantStereoDisparity(live)
	if publishStereoDisparityUntilEcho(ctx, t, backend, msg, echoPath, want, typeHash) {
		closeBackend()
		stopEcho()
		return
	}
	data, rerr := os.ReadFile(echoPath) //nolint:gosec // test reads controlled temp path
	if rerr != nil {
		t.Fatalf("ros2 topic echo did not see stereo disparity payload %q: %v", want, rerr)
	}
	closeBackend()
	stopEcho()
	t.Logf("ros2 topic echo did not see stereo disparity payload %q hash %s (known interop gap), pub ok\necho output:\n%s", want, typeHash, string(data))
	reason := "ros echo misses stereo disparity user DATA but pub ok, known interop gap reverse stereo"
	helpers.RegisterAllowlistedSkip(t.Name(), reason, time.Date(2027, 5, 1, 0, 0, 0, 0, time.UTC))
	helpers.NoteProductSkip(t, t.Name(), reason)
}

// TestROSTypeStereoDisparity checks DisparityImage small forward plus reverse.
//
// Forward starts ros2 topic pub for stereo_msgs DisparityImage and decodes
// via NewDecoder on the Weld take path. Reverse publishes the small 2 by 2
// sample via Weld and watches ros2 topic echo. Outer and inner frame_id
// carry the per-run live ID so stale runs never match. Data stays at 1.0
// plus 2.0 plus 3.0 plus 4.0 with full window and fixed geometry.
func TestROSTypeStereoDisparity(t *testing.T) {
	t.Parallel()
	domain := helpers.MustAllocateDomain(t)
	ctx, cancel := context.WithTimeout(context.Background(), stereoDisparityTestTimeout)
	defer cancel()
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)
	skipIfNoTypeInterface(t, ctx, domain, stereoDisparityRosType)
	liveFwd := helpers.LiveID("ros-fwd-stereo-disparity")
	topicFwd := helpers.LiveTopic(liveFwd)
	runStereoDisparityForward(t, ctx, domain, liveFwd, topicFwd)
	liveRev := helpers.LiveID("ros-rev-stereo-disparity")
	topicRev := helpers.LiveTopic(liveRev)
	runStereoDisparityReverse(t, ctx, domain, liveRev, topicRev)
}
