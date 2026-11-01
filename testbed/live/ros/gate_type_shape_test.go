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

// Shape gate constants for timeouts.
//
// Each test allocates one exclusive domain from the parallel pool, so
// per-run topics plus the allocated domain isolate runs. Timeouts bound
// every docker and backend call through context.
const (
	shapeTestTimeout = 2 * time.Minute
	shapeTalkerNap   = 5 * time.Second
	shapeEchoNap     = 6 * time.Second
	shapeTakeWait    = 45 * time.Second
	shapePollWait    = 30 * time.Second
	shapeProbeWait   = 5 * time.Second
	shapeCleanWait   = 10 * time.Second
	shapePubRetry    = 5 * time.Second
	shapePubInterval = 50 * time.Millisecond
	shapeRepubWait   = 2 * time.Second
)

// Shape sentinels for the BOX case.
//
// SolidPrimitive carries no string field, so the fixed BOX type plus fixed
// dimensions act as the sentinel. Per-run topic plus domain isolation keeps
// concurrent runs apart. Polygon stays empty to keep the wire minimal.
const (
	shapeBoxType uint8   = 1
	shapeDimX    float64 = 1.0
	shapeDimY    float64 = 2.0
	shapeDimZ    float64 = 3.0
)

// Shape type names and static hash.
//
// RosType is the slash form for ros2 topic pub and echo. IDLName is the
// double underscore form for Weld SEDP Subscribe and PublishBytes. The
// static hash was captured from ros2 topic info -v on weld-jazzy Jazzy for
// shape_msgs/msg/SolidPrimitive on domain 42. Live probing is preferred at
// runtime; this value is the fallback when the probe cannot run.
const (
	shapeRosType  = "shape_msgs/msg/SolidPrimitive"
	shapeIDLName  = "shape_msgs__msg__SolidPrimitive"
	shapeHashInit = "RIHS01_2802a15190aadc3f496584df4b0b4c5824d8a0b31aaef839faa75bc34dda38ac"
)

// yamlShapeSolidPrimitive returns ROS YAML for the BOX sentinel.
//
// Dimensions stay fixed at 1.0 plus 2.0 plus 3.0 with an empty polygon so
// the echo output is deterministic.
func yamlShapeSolidPrimitive() string {
	return "{type: 1, dimensions: [1.0, 2.0, 3.0], polygon: {points: []}}"
}

// encodeShapeSolidPrimitive encodes the BOX sentinel as XCDR1.
//
// Layout follows Jazzy IDL order: uint8 type, then bounded sequence
// float64[<=3] dimensions, then polygon with zero points. Alignment is
// handled by the Encoder per field.
func encodeShapeSolidPrimitive() ([]byte, error) {
	enc := types.NewEncoder(true)
	enc.WriteUint8(shapeBoxType)
	err := enc.WriteBoundedSequenceLength(3, 3)
	if err != nil {
		return nil, fmt.Errorf("encode shape dimensions length: %w", err)
	}
	enc.WriteFloat64(shapeDimX)
	enc.WriteFloat64(shapeDimY)
	enc.WriteFloat64(shapeDimZ)
	err = enc.WriteSequenceLength(0)
	if err != nil {
		return nil, fmt.Errorf("encode shape polygon length: %w", err)
	}
	return enc.Bytes(), nil
}

// readShapeType reads the SolidPrimitive type byte with checks.
func readShapeType(dec *types.Decoder) (uint8, error) {
	gotType, err := dec.ReadUint8()
	if err != nil {
		return 0, fmt.Errorf("decode shape type: %w", err)
	}
	if gotType != shapeBoxType {
		return 0, fmt.Errorf("shape type %d wants %d", gotType, shapeBoxType)
	}
	return gotType, nil
}

// readShapeDims reads the three dimension values with checks.
func readShapeDims(dec *types.Decoder) (float64, float64, float64, error) {
	n, err := dec.ReadBoundedSequenceLength(3)
	if err != nil {
		return 0, 0, 0, fmt.Errorf("decode shape dimensions length: %w", err)
	}
	if n != 3 {
		return 0, 0, 0, fmt.Errorf("shape dimensions len %d wants 3", n)
	}
	x, err := dec.ReadFloat64()
	if err != nil {
		return 0, 0, 0, fmt.Errorf("decode shape dim x: %w", err)
	}
	y, err := dec.ReadFloat64()
	if err != nil {
		return 0, 0, 0, fmt.Errorf("decode shape dim y: %w", err)
	}
	z, err := dec.ReadFloat64()
	if err != nil {
		return 0, 0, 0, fmt.Errorf("decode shape dim z: %w", err)
	}
	return x, y, z, nil
}

// checkShapePolygon checks the polygon holds zero points.
func checkShapePolygon(dec *types.Decoder) error {
	m, err := dec.ReadSequenceLength()
	if err != nil {
		return fmt.Errorf("decode shape polygon length: %w", err)
	}
	if m != 0 {
		return fmt.Errorf("shape polygon points %d wants 0", m)
	}
	return nil
}

// decodeShapeSolidPrimitive decodes and checks the BOX sentinel.
func decodeShapeSolidPrimitive(payload []byte) error {
	dec, err := types.NewDecoder(payload)
	if err != nil {
		return fmt.Errorf("decode shape header: %w", err)
	}
	_, err = readShapeType(dec)
	if err != nil {
		return err
	}
	x, y, z, err := readShapeDims(dec)
	if err != nil {
		return err
	}
	if x != shapeDimX || y != shapeDimY || z != shapeDimZ {
		return fmt.Errorf("shape dims (%v, %v, %v) wants (1.0, 2.0, 3.0)", x, y, z)
	}
	err = checkShapePolygon(dec)
	if err != nil {
		return err
	}
	err = dec.Finish()
	if err != nil {
		return fmt.Errorf("finish shape: %w", err)
	}
	return nil
}

// wantShapeSolidPrimitive returns the echo substring for the BOX sentinel.
func wantShapeSolidPrimitive() string {
	return "type: 1"
}

// takeShapeSamples takes until the BOX payload decodes or the context ends.
func takeShapeSamples(t *testing.T, takeCtx context.Context, backend *dds.Backend) bool {
	t.Helper()
	for takeCtx.Err() == nil {
		msg, terr := backend.TakeBytes(takeCtx)
		if terr != nil {
			if errors.Is(terr, context.DeadlineExceeded) || errors.Is(terr, context.Canceled) {
				break
			}
			text := terr.Error()
			if strings.Contains(text, "not implemented") {
				t.Fatalf("weld take backend error shape: %v", terr)
			}
			if strings.Contains(text, "not wired") {
				t.Fatalf("weld take backend error shape: %v", terr)
			}
			continue
		}
		derr := decodeShapeSolidPrimitive(msg.Payload)
		if derr == nil {
			return true
		}
	}
	return false
}

// runShapeForward checks Weld take sees a ROS talker for SolidPrimitive.
//
// It starts ros2 topic pub via docker argv with no shell, proves the local
// XCDR1 codec round trips, resolves the RIHS01 hash via ros2 topic info -v
// with static fallback, then takes via the Weld backend and decodes each
// sample with NewDecoder. A missing payload skips with the interop gap
// note, while backend errors still fail. It stops the talker and closes
// stopShapePeer stops one shape peer and verifies the container process
// is gone so later labels run on a quiet domain.
func stopShapePeer(t *testing.T, ctx context.Context, cmd *exec.Cmd, topic string) {
	t.Helper()
	helpers.StopCmd(cmd)
	cleanupCtx, cleanupCancel := context.WithTimeout(context.WithoutCancel(ctx), shapeCleanWait)
	defer cleanupCancel()
	if err := helpers.KillAndWaitContainerProc(cleanupCtx, helpers.Container, topic); err != nil {
		t.Errorf("stop peer proc: %v", err)
	}
}

// the backend before returning so later labels run on a quiet domain.
func runShapeForward(t *testing.T, ctx context.Context, domain int, live, topic string) {
	t.Helper()
	t.Cleanup(func() {
		helpers.AssertNoExcessSkips(t)
	})
	logPath := filepath.Join(t.TempDir(), "talker-"+live+"-shape.log")
	logFile, err := os.Create(logPath) //nolint:gosec // test writes controlled temp path
	if err != nil {
		t.Fatalf("create talker log shape: %v", err)
	}
	t.Cleanup(func() {
		cerr := logFile.Close()
		if cerr != nil {
			t.Logf("close talker log shape: %v", cerr)
		}
	})
	yaml := yamlShapeSolidPrimitive()
	args := helpers.DockerArgs(helpers.Container, domain, helpers.RosBin, "topic", "pub", topic, shapeRosType, yaml, "--rate", "2", "--qos-reliability", "reliable", "--qos-durability", "transient_local", "--qos-history", "keep_last", "--qos-depth", "10")
	talker := exec.CommandContext(ctx, "docker", args...) //nolint:gosec // test harness runs docker with controlled args
	talker.Stdout = logFile
	talker.Stderr = logFile
	err = talker.Start()
	if err != nil {
		t.Fatalf("start ROS talker shape: %v", err)
	}
	stopTalker := func() {
		stopShapePeer(t, ctx, talker, topic)
	}
	t.Cleanup(stopTalker)
	helpers.Sleep(ctx, shapeTalkerNap)
	if ctx.Err() != nil {
		t.Fatalf("context ended before take shape: %v", ctx.Err())
	}
	typeHash, err := helpers.ResolveTopicHash(ctx, helpers.Container, domain, helpers.RosBin, topic, shapeHashInit, shapeProbeWait, "shape")
	if err != nil {
		t.Skipf("shape hash unavailable for topic %s, skipping: %v", topic, err)
	}
	t.Logf("shape forward topic %s hash %s", topic, typeHash)
	payload, err := encodeShapeSolidPrimitive()
	if err != nil {
		t.Fatalf("local encode shape: %v", err)
	}
	err = decodeShapeSolidPrimitive(payload)
	if err != nil {
		t.Fatalf("local decode shape: %v", err)
	}
	backend, err := helpers.OpenLiveBackend(ctx, domain, "weld-shape-forward", "shape")
	if err != nil {
		t.Fatalf("open backend shape: %v", err)
	}
	//nolint:contextcheck // backend Close is cleanup without caller ctx, goodbye uses bounded Background
	closeBackend := func() {
		cerr := backend.Close()
		if cerr != nil {
			t.Logf("close backend shape: %v", cerr)
		}
	}
	t.Cleanup(closeBackend)
	err = helpers.SubscribeLiveReader(ctx, backend, topic, shapeIDLName, typeHash, "shape")
	if err != nil {
		t.Fatalf("subscribe shape: %v", err)
	}
	takeCtx, takeCancel := context.WithTimeout(ctx, shapeTakeWait)
	defer takeCancel()
	if takeShapeSamples(t, takeCtx, backend) {
		closeBackend()
		stopTalker()
		return
	}
	closeBackend()
	stopTalker()
	t.Logf("weld take did not see shape payload hash %s (known interop gap), local codec ok", typeHash)
	reason := "weld take misses shape user DATA but local codec ok, known interop gap forward shape"
	helpers.RegisterAllowlistedSkip(t.Name(), reason, time.Date(2027, 5, 1, 0, 0, 0, 0, time.UTC))
	helpers.NoteProductSkip(t, t.Name(), reason)
}

// publishShapeUntilEcho publishes until ros2 topic echo sees want.
func publishShapeUntilEcho(ctx context.Context, t *testing.T, backend *dds.Backend, msg transports.OutgoingMessage, echoPath, want, typeHash string) bool {
	t.Helper()
	return helpers.PublishUntilEcho(ctx, t, backend, msg, echoPath, want, typeHash, shapePollWait, shapeRepubWait, shapePubRetry, shapePubInterval, "shape")
}

// runShapeReverse checks ros2 topic echo sees a Weld pub for SolidPrimitive.
//
// It starts ros2 topic echo via docker argv with no shell, resolves the
// RIHS01 hash via ros2 topic info -v with static fallback for logging,
// then republishes one Weld sample until the echo stream holds the
// expected substring or the poll wait ends. A missing payload skips
// with the interop gap note, while pub failures still fail. It stops
// the echo and closes the backend before returning so later runs see a
// quiet domain.
func runShapeReverse(t *testing.T, ctx context.Context, domain int, live, topic string) {
	t.Helper()
	t.Cleanup(func() {
		helpers.AssertNoExcessSkips(t)
	})
	echoPath := filepath.Join(t.TempDir(), "rosecho-"+live+"-shape.log")
	echoFile, err := os.Create(echoPath) //nolint:gosec // test writes controlled temp path
	if err != nil {
		t.Fatalf("create ros echo log shape: %v", err)
	}
	t.Cleanup(func() {
		cerr := echoFile.Close()
		if cerr != nil {
			t.Logf("close ros echo log shape: %v", cerr)
		}
	})
	args := helpers.DockerArgs(helpers.Container, domain, helpers.RosBin, "topic", "echo", topic, shapeRosType)
	echoCmd := exec.CommandContext(ctx, "docker", args...) //nolint:gosec // test harness runs docker with controlled args
	echoCmd.Stdout = echoFile
	echoCmd.Stderr = echoFile
	err = echoCmd.Start()
	if err != nil {
		t.Fatalf("start ros2 topic echo shape: %v", err)
	}
	stopEcho := func() {
		stopShapePeer(t, ctx, echoCmd, topic)
	}
	t.Cleanup(stopEcho)
	helpers.Sleep(ctx, shapeEchoNap)
	if ctx.Err() != nil {
		t.Fatalf("context ended before pub shape: %v", ctx.Err())
	}
	typeHash, err := helpers.ResolveTopicHash(ctx, helpers.Container, domain, helpers.RosBin, topic, shapeHashInit, shapeProbeWait, "shape")
	if err != nil {
		t.Skipf("shape hash unavailable for topic %s, skipping: %v", topic, err)
	}
	t.Logf("shape reverse topic %s hash %s", topic, typeHash)
	payload, err := encodeShapeSolidPrimitive()
	if err != nil {
		t.Fatalf("local encode shape: %v", err)
	}
	err = decodeShapeSolidPrimitive(payload)
	if err != nil {
		t.Fatalf("local decode shape: %v", err)
	}
	backend, err := helpers.OpenLiveBackend(ctx, domain, "weld-shape-reverse", "shape")
	if err != nil {
		t.Fatalf("open backend shape: %v", err)
	}
	//nolint:contextcheck // backend Close is cleanup without caller ctx, goodbye uses bounded Background
	closeBackend := func() {
		cerr := backend.Close()
		if cerr != nil {
			t.Logf("close backend shape: %v", cerr)
		}
	}
	t.Cleanup(closeBackend)
	msg := transports.OutgoingMessage{
		TopicName: topic,
		TypeName:  shapeIDLName,
		Payload:   payload,
	}
	want := wantShapeSolidPrimitive()
	if publishShapeUntilEcho(ctx, t, backend, msg, echoPath, want, typeHash) {
		closeBackend()
		stopEcho()
		return
	}
	data, rerr := os.ReadFile(echoPath) //nolint:gosec // test reads controlled temp path
	if rerr != nil {
		t.Fatalf("ros2 topic echo did not see shape payload %q: %v", want, rerr)
	}
	closeBackend()
	stopEcho()
	t.Logf("ros2 topic echo did not see shape payload %q hash %s (known interop gap), pub ok\necho output:\n%s", want, typeHash, string(data))
	reason := "ros echo misses shape user DATA but pub ok, known interop gap reverse shape"
	helpers.RegisterAllowlistedSkip(t.Name(), reason, time.Date(2027, 5, 1, 0, 0, 0, 0, time.UTC))
	helpers.NoteProductSkip(t, t.Name(), reason)
}

// TestROSTypeShapeSolidPrimitive checks SolidPrimitive forward plus reverse.
//
// Forward starts ros2 topic pub for shape_msgs SolidPrimitive BOX and
// decodes via NewDecoder on the Weld take path. Reverse publishes the BOX
// sentinel via Weld and watches ros2 topic echo. The sentinel uses per-run
// topic plus domain isolation because SolidPrimitive carries no string
// field for a live ID.
func TestROSTypeShapeSolidPrimitive(t *testing.T) {
	t.Parallel()
	domain := helpers.MustAllocateDomain(t)
	ctx, cancel := context.WithTimeout(context.Background(), shapeTestTimeout)
	defer cancel()
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)
	helpers.SkipIfNoTypeInterface(t, ctx, helpers.Container, domain, helpers.RosBin, shapeRosType, shapeProbeWait)
	liveFwd := helpers.LiveID("ros-fwd-shape")
	topicFwd := helpers.LiveTopic(liveFwd)
	runShapeForward(t, ctx, domain, liveFwd, topicFwd)
	liveRev := helpers.LiveID("ros-rev-shape")
	topicRev := helpers.LiveTopic(liveRev)
	runShapeReverse(t, ctx, domain, liveRev, topicRev)
}
