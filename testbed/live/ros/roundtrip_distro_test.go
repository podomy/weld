// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

package ros_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/podomy/weld/internal/transports"
	"github.com/podomy/weld/testbed/live/helpers"
)

// ROS distro matrix constants.
//
// Jazzy on domain 42 is the canonical gate peer. Humble on domain 43 and
// Rolling on domain 44 extend the same String round trip with no cross
// talk because each distro has its own domain. RMW is always
// rmw_cyclonedds_cpp: CycloneDDS is the only supported DDS.
const (
	matrixEchoWait    = 10 * time.Second
	matrixPollWait    = 20 * time.Second
	matrixForwardNap  = 5 * time.Second
	matrixReverseNap  = 6 * time.Second
	matrixTestTimeout = 2 * time.Minute
	matrixCleanWait   = 10 * time.Second
	matrixTypeProbe   = 5 * time.Second
	matrixPubRetry    = 5 * time.Second
	matrixPubInterval = 50 * time.Millisecond
	matrixRepubWait   = 2 * time.Second
)

// matrixRunForward checks Weld backend take sees a ROS talker for one distro peer.
//
// It starts ros2 topic pub detached via StartDistroPeerProc writing to
// /testbed/logs/<run-id>.out inside the peer, which is the host file
// testbed/logs/<run-id>.out through the compose bind mount. It subscribes
// via the Weld backend and takes until one sample decodes to the live
// payload. Container, setup, and domain all come from peer so Humble and
// Rolling reuse this path with no duplication. Cleanup kills the pidfile
// PID plus one scoped pkill for the run ID. It skips cleanly when the peer
// container is absent as verified by docker inspect, and it skips when the
// peer lacks the std_msgs String interface. A healthy take that misses the
// live payload fails through the shared allowlist path, never skips.
//
//nolint:gocognit,cyclop // live forward gate sequences talker, take, and payload verdict in one run, splitting would obscure intent
func matrixRunForward(t *testing.T, ctx context.Context, peer helpers.DistroPeer) {
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
		t.Fatalf("context ended before take: %v", ctx.Err())
	}
	backend := helpers.MustOpenLiveBackend(t, ctx, uint32(peer.Domain), "ros-"+peer.Distro+"-fwd-"+live, peer.Distro+"-fwd") //nolint:gosec // G115: distro domains 42 through 44 fit uint32
	typeHash, err := helpers.ResolveTopicHash(ctx, peer.Container, peer.Domain, peer.RosBin, topic, helpers.ForwardStringHash, matrixTypeProbe, peer.Distro+"-fwd")
	if err != nil {
		t.Fatalf("matrix hash unavailable for topic %s: %v", topic, err)
	}
	err = helpers.SubscribeLiveReader(ctx, backend, topic, helpers.ForwardStringIDL, typeHash, peer.Distro+"-fwd")
	if err != nil {
		t.Fatalf("matrix subscribe %s: %v", topic, err)
	}
	takeCtx, takeCancel := context.WithTimeout(ctx, matrixEchoWait)
	defer takeCancel()
	for takeCtx.Err() == nil {
		msg, terr := backend.TakeBytes(takeCtx)
		if terr != nil {
			if keyedLiveTakeFailure(t, terr) {
				break
			}
			continue
		}
		if msg.TopicName != "" && msg.TopicName != topic {
			continue
		}
		if keyedLiveDecodeString(msg.Payload, payload) != nil {
			continue
		}
		return
	}
	detail := fmt.Sprintf("distro %s topic %s hash %s", peer.Distro, topic, typeHash)
	failOnMissingPayload(t, peer.Distro, "forward", payload, detail)
}

// matrixRunReverse checks ros2 topic echo sees Weld backend pub for one distro peer.
//
// It starts ros2 topic echo detached via StartDistroPeerProc writing to
// /testbed/logs/<run-id>.out inside the peer, which is the host file
// testbed/logs/<run-id>.out through the compose bind mount. It encodes the
// live payload as XCDR1 String and publishes via the Weld backend with
// publish until echo retry, then polls the host file directly with no
// docker exec cat per poll. Container, setup, and domain all come from peer
// so Humble and Rolling reuse this path with no duplication. Cleanup kills
// the pidfile PID plus one scoped pkill for the topic. It skips cleanly
// when the peer container is absent as verified by docker inspect, and it
// skips when the peer lacks the std_msgs String interface. A healthy pub
// that the peer echo misses fails through the shared allowlist path, never
// skips.
func matrixRunReverse(t *testing.T, ctx context.Context, peer helpers.DistroPeer) {
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
	backend := helpers.MustOpenLiveBackend(t, ctx, uint32(peer.Domain), "ros-"+peer.Distro+"-rev-"+live, peer.Distro+"-rev") //nolint:gosec // G115: distro domains 42 through 44 fit uint32
	typeHash, err := helpers.ResolveTopicHash(ctx, peer.Container, peer.Domain, peer.RosBin, topic, helpers.ForwardStringHash, matrixTypeProbe, peer.Distro+"-rev")
	if err != nil {
		t.Fatalf("matrix hash unavailable for topic %s: %v", topic, err)
	}
	payloadBytes := keyedLiveEncodeString(t, payload)
	if err := keyedLiveDecodeString(payloadBytes, payload); err != nil {
		t.Fatalf("matrix local codec: %v", err)
	}
	msg := transports.OutgoingMessage{
		TopicName: topic,
		TypeName:  helpers.ForwardStringIDL,
		Payload:   payloadBytes,
		TypeHash:  typeHash,
	}
	if !helpers.PublishUntilEcho(ctx, t, backend, msg, echoPath, payload, typeHash, matrixPollWait, matrixRepubWait, matrixPubRetry, matrixPubInterval, peer.Distro+"-rev") {
		data, rerr := os.ReadFile(echoPath) //nolint:gosec // test reads/writes controlled temp or repo file path
		if rerr != nil {
			t.Fatalf("ros2 topic echo did not see live payload %q on %s: %v", payload, peer.Distro, rerr)
		}
		failOnMissingPayload(t, peer.Distro, "reverse", payload, "distro "+peer.Distro+" echo output:\n"+string(data))
	}
}

// TestROSDistroMatrixForward checks Weld backend take sees each distro talker.
//
// It drives the shared forward helper over Jazzy domain 42, Humble domain
// 43, and Rolling domain 44. Each entry skips cleanly when its container
// is absent as verified by docker inspect, so a partial fleet still gives
// a clean result for the peers that are present.
func TestROSDistroMatrixForward(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), matrixTestTimeout)
	defer cancel()
	for _, peer := range helpers.DistroPeers() {
		t.Run(peer.Distro+"/forward", func(t *testing.T) {
			matrixRunForward(t, ctx, peer)
		})
	}
}

// TestROSDistroMatrixReverse checks each distro echo sees Weld backend pub.
//
// It drives the shared reverse helper over Jazzy domain 42, Humble domain
// 43, and Rolling domain 44. Each entry skips cleanly when its container
// is absent as verified by docker inspect, so a partial fleet still gives
// a clean result for the peers that are present.
func TestROSDistroMatrixReverse(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), matrixTestTimeout)
	defer cancel()
	for _, peer := range helpers.DistroPeers() {
		t.Run(peer.Distro+"/reverse", func(t *testing.T) {
			matrixRunReverse(t, ctx, peer)
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
