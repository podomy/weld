// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package ros_test

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/podomy/weld/testbed/live/helpers"
)

// ROS distro matrix constants.
//
// Jazzy on domain 42 is the canonical gate peer. Humble on domain 43 and
// Rolling on domain 44 extend the same String round trip with no cross
// talk because each distro has its own domain. RMW is always
// rmw_cyclonedds_cpp. FastDDS stays out of the gates because v1 scope is
// Cyclone only: FastDDS discovery and type handling are outside the v1
// backend claim, so gating on it would test unimplemented behavior.
const (
	matrixEchoWait    = 10 * time.Second
	matrixPollWait    = 20 * time.Second
	matrixForwardNap  = 5 * time.Second
	matrixReverseNap  = 6 * time.Second
	matrixTestTimeout = 2 * time.Minute
	matrixCleanWait   = 10 * time.Second
	matrixTypeProbe   = 5 * time.Second
)

// matrixRunForward checks weld echo sees a ROS talker for one distro peer.
//
// It starts ros2 topic pub detached via docker exec -d writing to
// /testbed/logs/<run-id>.out inside the peer, which is the host file
// testbed/logs/<run-id>.out through the compose bind mount. It runs weld
// echo and asserts the echo output holds this run live ID. Container,
// setup, and domain all come from peer so Humble and Rolling reuse this
// path with no duplication. Cleanup kills the pidfile PID plus one scoped
// pkill for the run ID. It skips cleanly when the peer container is absent
// as verified by docker inspect, and it skips when the peer lacks the
// std_msgs String interface. A healthy echo that misses the live payload
// fails through the shared allowlist path, never skips.
func matrixRunForward(t *testing.T, ctx context.Context, bin string, peer helpers.DistroPeer) {
	t.Helper()
	helpers.SkipIfNoDistroPeer(t, ctx, peer)
	helpers.SkipIfNoDistroTypeInterface(t, ctx, peer, helpers.RosType, matrixTypeProbe)
	exists, err := helpers.DistroSetupExists(ctx, peer)
	if err != nil {
		t.Fatalf("setup probe %s: %v", peer.SetupBash, err)
	}
	if !exists {
		t.Fatalf("setup %s missing in %s, distro gate requires setup", peer.SetupBash, peer.Container)
	}
	live := helpers.LiveID("ros-" + peer.Distro)
	topic := helpers.LiveTopic(live)
	payload := "hello forward " + live
	pidPath := helpers.HostPidPath(t, live)
	pubArg := fmt.Sprintf("{data: %s}", payload)
	err = helpers.StartDistroPeerProc(ctx, peer, live, peer.RosBin, "topic", "pub", topic, helpers.RosType, pubArg, "--rate", "2", "--qos-reliability", "reliable", "--qos-durability", "transient_local", "--qos-history", "keep_last", "--qos-depth", "10")
	if err != nil {
		t.Fatalf("start ROS talker %s: %v", peer.Container, err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.WithoutCancel(ctx), matrixCleanWait)
		defer cleanupCancel()
		helpers.StopPeerProcAndWait(cleanupCtx, peer.Container, peer.Domain, pidPath, live)
	})
	helpers.Sleep(ctx, matrixForwardNap)
	if ctx.Err() != nil {
		t.Fatalf("context ended before echo: %v", ctx.Err())
	}
	stdout, stderr, err := helpers.RunWeldEcho(ctx, bin, topic, peer.Domain, matrixEchoWait)
	if err != nil {
		t.Fatalf("weld echo %s: %v\nstdout:\n%s\nstderr:\n%s", peer.Distro, err, stdout, stderr)
	}
	combined := stdout + "\n" + stderr
	if strings.Contains(combined, "not implemented") {
		t.Fatalf("weld echo backend error:\n%s", combined)
	}
	if strings.Contains(combined, "not wired") {
		t.Fatalf("weld echo backend error:\n%s", combined)
	}
	if !strings.Contains(stdout, live) {
		detail := fmt.Sprintf("distro %s stdout:\n%s\nstderr:\n%s", peer.Distro, stdout, stderr)
		failOnMissingPayload(t, peer.Distro, "forward", payload, detail)
	}
}

// matrixRunReverse checks ros2 topic echo sees weld pub for one distro peer.
//
// It starts ros2 topic echo detached via docker exec -d writing to
// /testbed/logs/<run-id>.out inside the peer, which is the host file
// testbed/logs/<run-id>.out through the compose bind mount. It publishes
// one weld message that waits for routes internally, then polls the host
// file directly with no docker exec cat per poll. Container, setup, and
// domain all come from peer so Humble and Rolling reuse this path with no
// duplication. Cleanup kills the pidfile PID plus one scoped pkill for the
// topic. It skips cleanly when the peer container is absent as verified by
// docker inspect, and it skips when the peer lacks the std_msgs String
// interface. A healthy pub that the peer echo misses fails through the
// shared allowlist path, never skips.
func matrixRunReverse(t *testing.T, ctx context.Context, bin string, peer helpers.DistroPeer) {
	t.Helper()
	helpers.SkipIfNoDistroPeer(t, ctx, peer)
	helpers.SkipIfNoDistroTypeInterface(t, ctx, peer, helpers.RosType, matrixTypeProbe)
	exists, err := helpers.DistroSetupExists(ctx, peer)
	if err != nil {
		t.Fatalf("setup probe %s: %v", peer.SetupBash, err)
	}
	if !exists {
		t.Fatalf("setup %s missing in %s, distro gate requires setup", peer.SetupBash, peer.Container)
	}
	live := helpers.LiveID("ros-" + peer.Distro)
	topic := helpers.LiveTopic(live)
	payload := "hello reverse " + live
	echoPath := helpers.HostOutPath(t, live)
	pidPath := helpers.HostPidPath(t, live)
	err = helpers.StartDistroPeerProc(ctx, peer, live, peer.RosBin, "topic", "echo", topic, helpers.RosType)
	if err != nil {
		t.Fatalf("start ros2 topic echo %s: %v", peer.Container, err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.WithoutCancel(ctx), matrixCleanWait)
		defer cleanupCancel()
		helpers.StopPeerProcAndWait(cleanupCtx, peer.Container, peer.Domain, pidPath, topic)
	})
	helpers.Sleep(ctx, matrixReverseNap)
	if ctx.Err() != nil {
		t.Fatalf("context ended before pub: %v", ctx.Err())
	}
	found, stdout, stderr, err := helpers.PublishUntilSeen(ctx, bin, topic, peer.Domain, payload, echoPath, matrixPollWait, 3)
	if err != nil {
		t.Fatalf("weld pub %s: %v\nstdout:\n%s\nstderr:\n%s", peer.Distro, err, stdout, stderr)
	}
	if !strings.Contains(stdout, "sent:") {
		t.Fatalf("weld pub missing confirmation:\nstdout:\n%s\nstderr:\n%s", stdout, stderr)
	}
	if !found {
		data, rerr := os.ReadFile(echoPath) //nolint:gosec // test reads/writes controlled temp or repo file path
		if rerr != nil {
			t.Fatalf("ros2 topic echo did not see live payload %q on %s: %v", payload, peer.Distro, rerr)
		}
		failOnMissingPayload(t, peer.Distro, "reverse", payload, "distro "+peer.Distro+" echo output:\n"+string(data))
	}
}

// TestROSDistroMatrixForward checks weld echo sees each distro talker.
//
// It drives the shared forward helper over Jazzy domain 42, Humble domain
// 43, and Rolling domain 44. Each entry skips cleanly when its container
// is absent as verified by docker inspect, so a partial fleet still gives
// a clean result for the peers that are present.
func TestROSDistroMatrixForward(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), matrixTestTimeout)
	defer cancel()
	bin := helpers.BuildWeld(t, ctx)
	for _, peer := range helpers.DistroPeers() {
		t.Run(peer.Distro+"/forward", func(t *testing.T) {
			matrixRunForward(t, ctx, bin, peer)
		})
	}
}

// TestROSDistroMatrixReverse checks each distro echo sees weld pub.
//
// It drives the shared reverse helper over Jazzy domain 42, Humble domain
// 43, and Rolling domain 44. Each entry skips cleanly when its container
// is absent as verified by docker inspect, so a partial fleet still gives
// a clean result for the peers that are present.
func TestROSDistroMatrixReverse(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), matrixTestTimeout)
	defer cancel()
	bin := helpers.BuildWeld(t, ctx)
	for _, peer := range helpers.DistroPeers() {
		t.Run(peer.Distro+"/reverse", func(t *testing.T) {
			matrixRunReverse(t, ctx, bin, peer)
		})
	}
}

// TestROSDistroMatrixPubTimeout pins the shared pub timeout value.
//
// RunWeldPub uses helpers.PubTimeout so the matrix reverse path keeps the
// same generous outer timeout while weld waits for SPDP routes internally.
// The test pins the shared value so a timeout drift surfaces here.
func TestROSDistroMatrixPubTimeout(t *testing.T) {
	t.Parallel()
	if helpers.PubTimeout != 30*time.Second {
		t.Fatalf("helpers pub timeout = %v, want 30s", helpers.PubTimeout)
	}
}
