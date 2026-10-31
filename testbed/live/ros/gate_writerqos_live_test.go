// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

// Writer QoS live gate: per-profile translation proven against CycloneDDS.
//
// Unit tests pin the exact SEDP bytes and the equivalence matrix; this gate
// proves the live directions still behave. Each subtest allocates one
// exclusive domain from the parallel pool with per-run topics and payloads,
// skips cleanly only when no peer is present or the ros2 QoS flags are
// unsupported, and fails loudly otherwise: a missing payload on a
// compatible pair is a flow failure, and delivery on an incompatible pair
// is a silence failure. The incompatible case pins the loud refusal
// through CheckWireCompatibility first, so live silence always pairs with
// a named policy in the error text.
//
// Rationale for the offer directions: weld pub announces the facade offer
// (reliable plus transient local) because per-profile writer plumbing is
// not yet wired into the backend publish path. A reliable offer satisfies
// a best effort request under the same RxO rule a best effort offer does,
// so the reverse live leg proves the direction flows while the inline
// CheckWireCompatibility leg proves the translated best effort offer
// itself is compatible. The volatile leg uses a topic exchange with the
// same durability wire value (kind 0) that service endpoints announce.
package ros_test

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/podomy/weld/internal/core"
	"github.com/podomy/weld/internal/transports/dds"
	"github.com/podomy/weld/testbed/live/helpers"
)

// Writer QoS live gate constants for Jazzy canonical.
//
// Each test allocates one exclusive domain from the parallel pool. The
// container name and Cyclone RMW come from helpers so the gate stays on
// weld-jazzy with RMW_IMPLEMENTATION=rmw_cyclonedds_cpp. Only one run per
// domain and topic at a time is supported, so tests never run in parallel
// and every payload carries a per-run live ID.
const (
	wqEchoWait     = 10 * time.Second
	wqPollWait     = 20 * time.Second
	wqForwardNap   = 5 * time.Second
	wqReverseNap   = 6 * time.Second
	wqTestTimeout  = 4 * time.Minute
	wqProbeWait    = 15 * time.Second
	wqCleanWait    = 10 * time.Second
	wqMismatchWait = 8 * time.Second
)

// wqReliability sets reliability from its flag name.
func wqReliability(name string, out *core.QoSProfile) error {
	switch name {
	case "reliable":
		out.Reliability = core.ReliabilityReliable
	case "best_effort":
		out.Reliability = core.ReliabilityBestEffort
	default:
		return fmt.Errorf("writerqos: unknown reliability %q", name)
	}

	return nil
}

// wqDurability sets durability from its flag name.
func wqDurability(name string, out *core.QoSProfile) error {
	switch name {
	case "transient_local":
		out.Durability = core.DurabilityTransientLocal
	case "volatile":
		out.Durability = core.DurabilityVolatile
	default:
		return fmt.Errorf("writerqos: unknown durability %q", name)
	}

	return nil
}

// wqProfile builds an explicit core profile with fixed history and liveliness.
//
// History stays keep last with depth 10 and liveliness stays automatic so
// only reliability and durability drive the verdicts.
func wqProfile(reliability, durability string) (core.QoSProfile, error) {
	var out core.QoSProfile

	err := wqReliability(reliability, &out)
	if err != nil {
		return core.QoSProfile{}, err
	}

	err = wqDurability(durability, &out)
	if err != nil {
		return core.QoSProfile{}, err
	}

	out.History = core.HistoryKeepLast
	out.Depth = 10
	out.Liveliness = core.LivelinessAutomatic

	return out, nil
}

// wqForwardOffered translates one offered profile to wire QoS, failing loudly.
//
// The loud refusal carries topic, endpoint pair, and policy kind, so an
// untranslatable offer never passes silently.
func wqForwardOffered(t *testing.T, topic, publisher, subscriber string, offered core.QoSProfile) dds.WireQoS {
	t.Helper()

	wire, err := dds.ForwardQoS(topic, publisher, subscriber, offered)
	if err != nil {
		t.Fatalf("ForwardQoS offered: %v", err)
	}

	return wire
}

// wqCheckWireCompatible asserts a wire gate report has no errors.
func wqCheckWireCompatible(t *testing.T, name string, report core.CompatibilityReport) {
	t.Helper()

	helpers.CheckQoSCompatible(t, name, report)
}

// wqCheckWireIncompatible asserts a wire gate report names the policy loudly.
func wqCheckWireIncompatible(t *testing.T, name string, report core.CompatibilityReport, offered, requested core.QoSProfile, publisher, subscriber string, wantPolicy core.PolicyKind, domain int, runID string) {
	t.Helper()

	helpers.CheckQoSIncompatible(t, name, report, offered, requested, publisher, subscriber, wantPolicy, "writerqos", domain, runID)
}

// wqRequireQoSFlags skips the test when the peer lacks the ros2 QoS flags.
func wqRequireQoSFlags(t *testing.T, ctx context.Context, domain int) {
	t.Helper()

	probeCtx, cancel := context.WithTimeout(ctx, wqProbeWait)
	defer cancel()

	argv := helpers.DockerArgs(helpers.Container, domain, helpers.RosBin, "topic", "pub", "--help")
	cmd := exec.CommandContext(probeCtx, "docker", argv...) //nolint:gosec // live harness probes ros2 help with fixed argv and no shell
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Skipf("writerqos pub help probe failed, skipping: %v", err)
	}

	if !strings.Contains(string(out), "--qos-reliability") || !strings.Contains(string(out), "--qos-durability") {
		t.Skipf("ros2 topic pub lacks QoS flags, skipping writerqos gate")
	}
}

// wqSetup allocates one exclusive domain, skips without a peer or flags,
// and builds weld once per test binary process.
func wqSetup(t *testing.T) (context.Context, context.CancelFunc, string, int) {
	t.Helper()

	domain := helpers.MustAllocateDomain(t)
	ctx, cancel := context.WithTimeout(context.Background(), wqTestTimeout)
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)
	wqRequireQoSFlags(t, ctx, domain)
	bin := helpers.BuildWeld(t, ctx)

	return ctx, cancel, bin, domain
}

// wqRunRosPub starts a ROS talker with explicit QoS flags and a per-run payload.
func wqRunRosPub(t *testing.T, ctx context.Context, domain int, pubRel, pubDur, payloadPrefix string) (string, string) {
	t.Helper()

	live := helpers.LiveID("wq-" + payloadPrefix)
	topic := helpers.LiveTopic(live)
	payload := "hello wq " + live
	logPath := filepath.Join(t.TempDir(), "wq-talker-"+live+".log")
	logFile, err := os.Create(logPath) //nolint:gosec // test reads and writes controlled temp file path
	if err != nil {
		t.Fatalf("create talker log: %v", err)
	}

	t.Cleanup(func() {
		cerr := logFile.Close()
		if cerr != nil {
			t.Logf("close talker log: %v", cerr)
		}
	})

	pubArg := fmt.Sprintf("{data: %s}", payload)
	argv := []string{
		helpers.RosBin, "topic", "pub", topic, helpers.RosType, pubArg, "--rate", "2",
		"--qos-reliability", pubRel, "--qos-durability", pubDur, "--qos-history", "keep_last", "--qos-depth", "10",
	}
	args := helpers.DockerArgs(helpers.Container, domain, argv...)
	talker := exec.CommandContext(ctx, "docker", args...) //nolint:gosec // live harness runs docker with controlled test args
	talker.Stdout = logFile
	talker.Stderr = logFile

	err = talker.Start()
	if err != nil {
		t.Fatalf("start ROS talker: %v", err)
	}

	t.Cleanup(func() {
		helpers.StopCmd(talker)
		cleanupCtx, cleanupCancel := context.WithTimeout(context.WithoutCancel(ctx), wqCleanWait)
		defer cleanupCancel()
		helpers.KillContainerProc(cleanupCtx, helpers.Container, domain, live)
	})

	return topic, payload
}

// wqRunRosEcho starts a ROS echo reader with explicit QoS flags into a log file.
func wqRunRosEcho(t *testing.T, ctx context.Context, domain int, topic, echoRel, echoDur string) string {
	t.Helper()

	echoPath := filepath.Join(t.TempDir(), "wq-rosecho-"+strings.ReplaceAll(topic, "/", "_")+".log")
	echoFile, err := os.Create(echoPath) //nolint:gosec // test reads and writes controlled temp file path
	if err != nil {
		t.Fatalf("create ros echo log: %v", err)
	}

	t.Cleanup(func() {
		cerr := echoFile.Close()
		if cerr != nil {
			t.Logf("close ros echo log: %v", cerr)
		}
	})

	argv := []string{
		helpers.RosBin, "topic", "echo", topic, helpers.RosType,
		"--qos-reliability", echoRel, "--qos-durability", echoDur, "--qos-history", "keep_last", "--qos-depth", "10",
	}
	args := helpers.DockerArgs(helpers.Container, domain, argv...)
	echoCmd := exec.CommandContext(ctx, "docker", args...) //nolint:gosec // live harness runs docker with controlled test args
	echoCmd.Stdout = echoFile
	echoCmd.Stderr = echoFile

	err = echoCmd.Start()
	if err != nil {
		t.Fatalf("start ros2 topic echo: %v", err)
	}

	t.Cleanup(func() {
		helpers.StopCmd(echoCmd)
		cleanupCtx, cleanupCancel := context.WithTimeout(context.WithoutCancel(ctx), wqCleanWait)
		defer cleanupCancel()
		helpers.KillContainerProc(cleanupCtx, helpers.Container, domain, topic)
	})

	return echoPath
}

// TestWriterQoSLiveBestEffortOfferFlows proves the best effort offer direction flows.
//
// The inline leg translates a best effort volatile offer and a best effort
// volatile request to wire QoS and requires the compatibility gate to pass.
// The live leg runs weld pub to a best effort Cyclone echo and requires
// delivery: the facade announces reliable, which satisfies a best effort
// request under the same RxO rule a best effort offer does, so the
// direction flows while the inline leg pins the translated best effort
// offer itself.
func TestWriterQoSLiveBestEffortOfferFlows(t *testing.T) {
	ctx, cancel, bin, domain := wqSetup(t)
	defer cancel()

	offered, err := wqProfile("best_effort", "volatile")
	if err != nil {
		t.Fatalf("best effort offer profile: %v", err)
	}

	requested, err := wqProfile("best_effort", "volatile")
	if err != nil {
		t.Fatalf("best effort request profile: %v", err)
	}

	topic := helpers.LiveTopic(helpers.LiveID("wq-best-effort"))
	report := dds.CheckWireCompatibility(
		wqForwardOffered(t, topic, "weld-pub", "ros-echo", offered),
		wqForwardOffered(t, topic, "weld-pub", "ros-echo", requested),
	)
	wqCheckWireCompatible(t, "best-effort-offer-flows", report)

	live := helpers.LiveID("wq-best-effort")
	liveTopic := helpers.LiveTopic(live)
	payload := "hello wq best effort " + live
	echoPath := wqRunRosEcho(t, ctx, domain, liveTopic, "best_effort", "volatile")
	helpers.Sleep(ctx, wqReverseNap)

	if ctx.Err() != nil {
		t.Fatalf("context ended before pub: %v", ctx.Err())
	}

	found, stdout, stderr, err := helpers.PublishUntilSeen(ctx, bin, liveTopic, domain, payload, echoPath, wqPollWait, 3)
	if err != nil {
		t.Fatalf("weld pub: %v\nstdout:\n%s\nstderr:\n%s", err, stdout, stderr)
	}

	if !strings.Contains(stdout, "sent:") {
		t.Fatalf("weld pub missing confirmation:\nstdout:\n%s\nstderr:\n%s", stdout, stderr)
	}

	if !found {
		data, rerr := os.ReadFile(echoPath) //nolint:gosec // test reads controlled temp file path
		if rerr != nil {
			t.Fatalf("ros echo missed live payload %q: %v", payload, rerr)
		}

		t.Fatalf("ros echo missed live payload %q, want flow for compatible pair\necho output:\n%s", payload, string(data))
	}
}

// TestWriterQoSLiveVolatileServiceEquivalent proves the volatile exchange matches.
//
// The inline leg translates the resolved services default offer and a
// volatile request to wire QoS, requires volatile durability on the wire,
// and requires the compatibility gate to pass. The live leg runs a ROS
// volatile talker to weld echo with volatile durability and requires
// delivery with the same wire durability value service endpoints announce.
func TestWriterQoSLiveVolatileServiceEquivalent(t *testing.T) {
	ctx, cancel, bin, domain := wqSetup(t)
	defer cancel()

	offered := core.ResolveSystemDefaults(core.ServicesDefaultQoS())
	requested, err := wqProfile("reliable", "volatile")
	if err != nil {
		t.Fatalf("volatile request profile: %v", err)
	}

	topic := helpers.LiveTopic(helpers.LiveID("wq-volatile"))
	wireOffered := wqForwardOffered(t, topic, "ros-pub", "weld-echo", offered)
	if wireOffered.DurabilityKind != 0 {
		t.Fatalf("services default wire durability = %d, want volatile 0", wireOffered.DurabilityKind)
	}

	report := dds.CheckWireCompatibility(wireOffered, wqForwardOffered(t, topic, "ros-pub", "weld-echo", requested))
	wqCheckWireCompatible(t, "volatile-service-equivalent", report)

	liveTopic, payload := wqRunRosPub(t, ctx, domain, "reliable", "volatile", "volatile")
	helpers.Sleep(ctx, wqForwardNap)

	if ctx.Err() != nil {
		t.Fatalf("context ended before echo: %v", ctx.Err())
	}

	stdout, stderr, err := helpers.RunWeldEcho(ctx, bin, liveTopic, domain, wqEchoWait)
	if err != nil {
		t.Fatalf("weld echo: %v\nstdout:\n%s\nstderr:\n%s", err, stdout, stderr)
	}

	combined := stdout + "\n" + stderr
	helpers.AssertNoBackendError(t, "volatile-service-equivalent", combined)

	if !strings.Contains(stdout, payload) {
		t.Fatalf("weld echo missed live payload %q, want flow for compatible pair\nstdout:\n%s\nstderr:\n%s", payload, stdout, stderr)
	}
}

// TestWriterQoSLiveIncompatibleRefusedLoudly proves the blocking direction is refused loudly.
//
// The inline leg translates a best effort offer and a reliable request to
// wire QoS and requires the compatibility gate to fail with the
// reliability policy named. The live leg runs a ROS best effort talker to
// weld echo with a reliable requester and requires silence on the live ID:
// delivery would mean the incompatible pair flowed.
func TestWriterQoSLiveIncompatibleRefusedLoudly(t *testing.T) {
	ctx, cancel, bin, domain := wqSetup(t)
	defer cancel()

	offered, err := wqProfile("best_effort", "transient_local")
	if err != nil {
		t.Fatalf("best effort offer profile: %v", err)
	}

	requested := core.FacadeAdvertisedQoS()
	mismatchLive := helpers.LiveID("wq-mismatch")
	topic := helpers.LiveTopic(mismatchLive)
	report := dds.CheckWireCompatibility(
		wqForwardOffered(t, topic, "ros-pub", "weld-echo", offered),
		wqForwardOffered(t, topic, "ros-pub", "weld-echo", requested),
	)
	wqCheckWireIncompatible(t, "reliable-requester-vs-best-effort-offer", report, offered, requested, "ros-pub", "weld-echo", core.PolicyReliability, domain, mismatchLive)

	subCtx, subCancel := context.WithTimeout(ctx, wqMismatchWait+2*wqEchoWait)
	defer subCancel()

	liveTopic, payload := wqRunRosPub(t, subCtx, domain, "best_effort", "transient_local", "mismatch")
	helpers.Sleep(subCtx, wqForwardNap)

	if subCtx.Err() != nil {
		t.Fatalf("context ended before echo: %v", subCtx.Err())
	}

	stdout, stderr, err := helpers.RunWeldEcho(subCtx, bin, liveTopic, domain, wqEchoWait)
	if err != nil {
		t.Fatalf("weld echo: %v\nstdout:\n%s\nstderr:\n%s", err, stdout, stderr)
	}

	combined := stdout + "\n" + stderr
	helpers.AssertNoBackendError(t, "reliable-requester-vs-best-effort-offer", combined)

	if strings.Contains(stdout, payload) {
		t.Fatalf("incompatible pair delivered payload %q, want no flow (gate reports incompatible with reliability named)", payload)
	}

	t.Logf("incompatible pair stayed silent on live ID as required, reliability refusal is loud above")
}
