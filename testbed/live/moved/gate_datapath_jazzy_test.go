// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package moved

import (
	"context"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/podomy/weld/testbed/live/helpers"
)

// TestGateDataPathJazzyDomain42Talker observes the live talker on one allocated domain.
//
// It starts a real ROS talker on the allocated exclusive domain, then
// runs ros2 topic echo for that talker. It uses docker argv with
// structured environment and no shell through the shared helpers. It
// skips cleanly when no daemon or peer is present and never starts the
// compose stack. It uses an allocated exclusive domain with explicit
// domain plumbing, so it is safe for future t.Parallel workers. The echo
// uses --timeout instead of --once so the probe always exits: --once
// waits forever on a quiet topic and leaks a subscriber that later tests
// observe as a phantom route.
func TestGateDataPathJazzyDomain42Talker(t *testing.T) {
	domain := helpers.MustAllocateDomain(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)
	runID := helpers.LiveID("datapath-jazzy")
	topic := "/chatter"
	payload := "hello datapath " + runID
	pubArg := "{data: " + payload + "}"
	err := helpers.StartPeerProc(ctx, helpers.Container, domain, runID, helpers.RosBin, "topic", "pub", topic, helpers.RosType, pubArg, "--rate", "2")
	if err != nil {
		t.Fatalf("start ROS talker on domain %d: %v", domain, err)
	}
	pidPath := helpers.HostPidPath(t, runID)
	t.Cleanup(func() {
		helpers.StopPeerProcAndWait(context.WithoutCancel(ctx), helpers.Container, domain, pidPath, runID)
	})
	helpers.WaitForPeerReady(ctx, pidPath, helpers.TalkerSettle)
	if ctx.Err() != nil {
		t.Fatalf("context ended before echo: %v", ctx.Err())
	}
	echoCtx, echoCancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer echoCancel()
	args := helpers.DockerArgs(helpers.Container, domain, helpers.RosBin, "topic", "echo", topic, helpers.RosType, "--timeout", "5")
	echoCmd := exec.CommandContext(echoCtx, "docker", args...) //nolint:gosec // test harness runs docker with fixed test args and no shell
	echoOut, err := echoCmd.CombinedOutput()
	if err != nil {
		t.Fatalf("live domain %d talker started but echo failed: %v output %s", domain, err, string(echoOut))
	}
	if !strings.Contains(string(echoOut), "data:") {
		t.Fatalf("live domain %d talker started but echo output misses data field: %s", domain, string(echoOut))
	}
	t.Logf("live domain %d talker observed: %s", domain, strings.TrimSpace(string(echoOut)))
}
