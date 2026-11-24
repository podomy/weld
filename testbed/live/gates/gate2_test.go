// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

package gates_test

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/podomy/weld/testbed/live/helpers"
)

// TestGate2ForwardEchoSeesTalker checks ROS echo receives a ROS talker.
//
// It starts ros2 topic pub detached via docker exec -d writing to
// /testbed/logs/<run-id>.out inside the peer, which is the host file
// testbed/logs/<run-id>.out through the compose bind mount. It runs stock
// ros2 topic echo and asserts the echo output holds this run live ID. Topic,
// payload, and log name all carry the per-run ID so concurrent or stale runs
// never mix. Cleanup kills the pidfile PID plus one scoped pkill for the run
// ID. It skips cleanly when no daemon or peer container is present and never
// starts the compose stack. It uses an allocated exclusive domain with
// explicit domain plumbing and no process env, so it is safe for future
// t.Parallel workers.
func TestGate2ForwardEchoSeesTalker(t *testing.T) {
	domain := helpers.MustAllocateDomain(t)
	t.Cleanup(func() {
		helpers.AssertNoExcessSkips(t)
	})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)
	id := helpers.LiveID("live")
	topic := helpers.LiveTopic(id)
	payload := "hello forward " + id
	pidPath := helpers.HostPidPath(t, id)
	pubArg := "{data: " + payload + "}"
	err := helpers.StartPeerProc(ctx, helpers.Container, domain, id, helpers.RosBin, "topic", "pub", topic, helpers.RosType, pubArg, "--rate", "2")
	if err != nil {
		t.Fatalf("start ROS talker: %v", err)
	}
	t.Cleanup(func() {
		helpers.StopPeerProcAndWait(context.WithoutCancel(ctx), helpers.Container, domain, pidPath, id)
	})
	helpers.WaitForPeerReady(ctx, pidPath, helpers.TalkerSettle)
	if ctx.Err() != nil {
		t.Fatalf("context ended before echo: %v", ctx.Err())
	}
	stdout, stderr, err := helpers.RunRosEcho(ctx, helpers.Container, topic, domain, helpers.EchoTimeout)
	if err != nil {
		t.Fatalf("ros echo: %v\nstdout:\n%s\nstderr:\n%s", err, stdout, stderr)
	}
	if !strings.Contains(stdout, id) {
		t.Fatalf("ros echo did not receive live payload %q:\nstdout:\n%s\nstderr:\n%s", payload, stdout, stderr)
	}
}

// TestGate2ReverseEchoSeesPub checks ros2 topic echo sees ros2 pub.
//
// It starts ros2 topic echo detached via docker exec -d writing to
// /testbed/logs/<run-id>.out inside the peer, which is the host file
// testbed/logs/<run-id>.out through the compose bind mount. It publishes
// via stock ros2 topic pub then polls the host file directly with no docker
// exec cat per poll. Topic, payload, and log name all carry the per-run ID,
// and cleanup kills the pidfile PID plus one scoped pkill for the topic. It
// skips cleanly when no daemon or peer container is present. It uses an
// allocated exclusive domain with explicit domain plumbing and no process
// env, so it is safe for future t.Parallel workers.
func TestGate2ReverseEchoSeesPub(t *testing.T) {
	domain := helpers.MustAllocateDomain(t)
	t.Cleanup(func() {
		helpers.AssertNoExcessSkips(t)
	})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)
	id := helpers.LiveID("live")
	topic := helpers.LiveTopic(id)
	payload := "hello reverse " + id
	echoPath := helpers.HostOutPath(t, id)
	pidPath := helpers.HostPidPath(t, id)
	err := helpers.StartPeerProc(ctx, helpers.Container, domain, id, helpers.RosBin, "topic", "echo", topic, helpers.RosType)
	if err != nil {
		t.Fatalf("start ros2 topic echo: %v", err)
	}
	t.Cleanup(func() {
		helpers.StopPeerProcAndWait(context.WithoutCancel(ctx), helpers.Container, domain, pidPath, topic)
	})
	helpers.WaitForPeerReady(ctx, pidPath, helpers.EchoSettle)
	if ctx.Err() != nil {
		t.Fatalf("context ended before pub: %v", ctx.Err())
	}
	found, stdout, stderr, err := helpers.RosPubUntilSeen(ctx, helpers.Container, topic, domain, payload, echoPath, helpers.FilePollTimeout, 3)
	if err != nil {
		t.Fatalf("ros pub: %v\nstdout:\n%s\nstderr:\n%s", err, stdout, stderr)
	}
	if !found {
		data, rerr := os.ReadFile(echoPath) //nolint:gosec // test reads/writes controlled temp or repo file path
		if rerr != nil {
			t.Fatalf("ros2 topic echo did not see live payload %q: %v", payload, rerr)
		}
		t.Fatalf("ros2 topic echo did not see live payload %q:\necho output:\n%s", payload, string(data))
	}
}
