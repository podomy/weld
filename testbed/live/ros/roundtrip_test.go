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

// ROS round trip constants for /chatter.
//
// Each test allocates one exclusive domain from the parallel pool, so
// per-run topics plus the allocated domain isolate runs. The container
// name matches the testbed services.
const (
	rosTopic    = "/chatter"
	rosType     = "std_msgs/msg/String"
	echoWait    = 10 * time.Second
	pollWait    = 20 * time.Second
	forwardNap  = 5 * time.Second
	reverseNap  = 6 * time.Second
	testTimeout = 2 * time.Minute
	cleanWait   = 10 * time.Second
	typeProbe   = 5 * time.Second
)

// rosPayloadGap is one explicit allowlist entry for a known payload gap.
//
// Distro names the ROS distro the gap applies to. Direction names the
// gapping direction as forward or reverse. Reason records why the
// entry exists. Expires is the fail closed date: before expiry a missing
// payload with a healthy peer fails with an ALLOWLISTED prefix so it can
// be counted separately, after expiry the same miss fails with an
// ALLOWLIST EXPIRED prefix. Both paths fail, never skip.
type rosPayloadGap struct {
	distro    string
	direction string
	reason    string
	expires   time.Time
}

// rosPayloadGapAllowlist holds the genuine Jazzy reverse gap.
//
// Forward now passes with identical hashes, so Jazzy forward has no entry
// and fails as an unexpected miss. The Jazzy reverse path stays gapping:
// the ros2 topic echo peer stays healthy while weld pub user DATA does not
// arrive. Humble and Rolling have no entry and fail as unexpected misses.
// Keep this list empty unless a live run proves a gap.
var rosPayloadGapAllowlist = []rosPayloadGap{
	{
		distro:    "jazzy",
		direction: "reverse",
		reason:    "ros2 topic echo stays healthy but weld pub user DATA does not arrive on domain 42",
		expires:   time.Date(2027, time.March, 31, 0, 0, 0, 0, time.UTC),
	},
}

// rosPayloadGapEntry returns the allowlist entry for distro and direction when present.
func rosPayloadGapEntry(distro, direction string) (rosPayloadGap, bool) {
	for _, entry := range rosPayloadGapAllowlist {
		if entry.distro == distro && entry.direction == direction {
			return entry, true
		}
	}
	return rosPayloadGap{}, false
}

// failOnMissingPayload fails when a healthy peer misses the live payload.
//
// It never skips. An allowlisted distro and direction fails with an
// ALLOWLISTED prefix so it can be counted separately from unexpected
// misses. An expired entry fails closed with an ALLOWLIST EXPIRED prefix.
// A distro and direction pair with no entry fails as an unexpected miss.
// The caller passes direction as forward or reverse and detail as the peer
// output for triage.
func failOnMissingPayload(t *testing.T, distro, direction, payload, detail string) {
	t.Helper()
	now := time.Now()
	if entry, ok := rosPayloadGapEntry(distro, direction); ok {
		if now.After(entry.expires) {
			t.Fatalf("ALLOWLIST EXPIRED distro %s direction %s: %s (expired %s): missing live payload %q\n%s",
				distro, direction, entry.reason, entry.expires.Format("2006-01-02"), payload, detail)
		}
		t.Fatalf("ALLOWLISTED known payload gap distro %s direction %s: %s (expires %s): missing live payload %q\n%s",
			distro, direction, entry.reason, entry.expires.Format("2006-01-02"), payload, detail)
	}
	t.Fatalf("missing live payload %q distro %s direction %s with healthy peer:\n%s", payload, distro, direction, detail)
}

// TestROSForwardWeldEchoSeesROSTalker checks weld echo receives a ROS talker.
//
// It starts ros2 topic pub detached via docker exec -d writing to
// /testbed/logs/<run-id>.out inside the peer, which is the host file
// testbed/logs/<run-id>.out through the compose bind mount. It runs weld
// echo and asserts the echo output holds this run live ID. Topic, payload,
// and log name all carry the per-run ID so stale runs never mix. Cleanup
// kills the pidfile PID plus one scoped pkill for the run ID. It skips
// cleanly when no daemon or peer container is present and never runs in
// parallel because the allocated domain is exclusive.
func TestROSForwardWeldEchoSeesROSTalker(t *testing.T) {
	t.Parallel()
	domain := helpers.MustAllocateDomain(t)
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)
	helpers.SkipIfNoTypeInterface(t, ctx, helpers.Container, domain, helpers.RosBin, rosType, typeProbe)
	bin := helpers.BuildWeld(t, ctx)
	live := helpers.LiveID("ros")
	topic := helpers.LiveTopic(live)
	payload := "hello forward " + live
	pidPath := helpers.HostPidPath(t, live)
	pubArg := fmt.Sprintf("{data: %s}", payload)
	err := helpers.StartPeerProc(ctx, helpers.Container, domain, live, helpers.RosBin, "topic", "pub", topic, rosType, pubArg, "--rate", "2")
	if err != nil {
		t.Fatalf("start ROS talker: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.WithoutCancel(ctx), cleanWait)
		defer cleanupCancel()
		helpers.StopPeerProc(cleanupCtx, helpers.Container, domain, pidPath, live)
	})
	helpers.Sleep(ctx, forwardNap)
	if ctx.Err() != nil {
		t.Fatalf("context ended before echo: %v", ctx.Err())
	}
	stdout, stderr, err := helpers.RunWeldEcho(ctx, bin, topic, domain, echoWait)
	if err != nil {
		t.Fatalf("weld echo: %v\nstdout:\n%s\nstderr:\n%s", err, stdout, stderr)
	}
	combined := stdout + "\n" + stderr
	if strings.Contains(combined, "not implemented") {
		t.Fatalf("weld echo backend error:\n%s", combined)
	}
	if strings.Contains(combined, "not wired") {
		t.Fatalf("weld echo backend error:\n%s", combined)
	}
	if !strings.Contains(stdout, live) {
		detail := fmt.Sprintf("stdout:\n%s\nstderr:\n%s", stdout, stderr)
		failOnMissingPayload(t, "jazzy", "forward", payload, detail)
	}
}

// TestROSReverseROSEchoSeesWeldPub checks ros2 topic echo receives weld pub.
//
// It starts ros2 topic echo detached via docker exec -d writing to
// /testbed/logs/<run-id>.out inside the peer, which is the host file
// testbed/logs/<run-id>.out through the compose bind mount. It publishes
// one weld message that waits for routes internally, then polls the host
// file directly with no docker exec cat per poll. Topic, payload, and log
// name all carry the per-run ID, and cleanup kills the pidfile PID plus
// one scoped pkill for the topic. It skips cleanly when no daemon or peer
// container is present and never runs in parallel.
func TestROSReverseROSEchoSeesWeldPub(t *testing.T) {
	t.Parallel()
	domain := helpers.MustAllocateDomain(t)
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)
	helpers.SkipIfNoTypeInterface(t, ctx, helpers.Container, domain, helpers.RosBin, rosType, typeProbe)
	bin := helpers.BuildWeld(t, ctx)
	live := helpers.LiveID("ros")
	topic := helpers.LiveTopic(live)
	payload := "hello reverse " + live
	echoPath := helpers.HostOutPath(t, live)
	pidPath := helpers.HostPidPath(t, live)
	err := helpers.StartPeerProc(ctx, helpers.Container, domain, live, helpers.RosBin, "topic", "echo", topic, rosType)
	if err != nil {
		t.Fatalf("start ros2 topic echo: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.WithoutCancel(ctx), cleanWait)
		defer cleanupCancel()
		helpers.StopPeerProc(cleanupCtx, helpers.Container, domain, pidPath, topic)
	})
	helpers.Sleep(ctx, reverseNap)
	if ctx.Err() != nil {
		t.Fatalf("context ended before pub: %v", ctx.Err())
	}
	found, stdout, stderr, err := helpers.PublishUntilSeen(ctx, bin, topic, domain, payload, echoPath, pollWait, 3)
	if err != nil {
		t.Fatalf("weld pub: %v\nstdout:\n%s\nstderr:\n%s", err, stdout, stderr)
	}
	if !strings.Contains(stdout, "sent:") {
		t.Fatalf("weld pub missing confirmation:\nstdout:\n%s\nstderr:\n%s", stdout, stderr)
	}
	if !found {
		data, rerr := os.ReadFile(echoPath) //nolint:gosec // test reads/writes controlled temp or repo file path
		if rerr != nil {
			t.Fatalf("ros2 topic echo did not see live payload %q: %v", payload, rerr)
		}
		failOnMissingPayload(t, "jazzy", "reverse", payload, "echo output:\n"+string(data))
	}
}
