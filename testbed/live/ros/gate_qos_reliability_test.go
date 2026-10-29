// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

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
	"github.com/podomy/weld/testbed/live/helpers"
)

// Reliability gate constants for Jazzy canonical.
//
// Each subtest allocates one exclusive domain from the parallel pool. The container name and
// Cyclone RMW come from helpers so the gate stays on weld-jazzy with
// RMW_IMPLEMENTATION=rmw_cyclonedds_cpp. Only one run per domain and topic
// at a time is supported, so tests never run in parallel and every payload
// carries a per-run live ID. Humble and Rolling already cover default QoS
// in the distro matrix, so this file pins only the reliability axis on
// Jazzy with explicit --qos-reliability flags.
const (
	relEchoWait     = 10 * time.Second
	relPollWait     = 20 * time.Second
	relForwardNap   = 5 * time.Second
	relReverseNap   = 6 * time.Second
	relTestTimeout  = 4 * time.Minute
	relProbeWait    = 15 * time.Second
	relCleanWait    = 10 * time.Second
	relMismatchWait = 8 * time.Second
)

// relLiveCase describes one live reliability combination.
//
// Direction is forward for ROS pub to weld echo, reverse for weld pub to
// ROS echo, or ros_to_ros for ROS pub to ROS echo. PubRel is the writer
// reliability and echoRel is the reader reliability. Compatible cases must
// deliver the live ID. Incompatible cases must not deliver and must carry
// an explicit core reliability error.
type relLiveCase struct {
	// name scopes the subtest and the live ID prefix.
	name string
	// direction is forward, reverse, or ros_to_ros.
	direction string
	// pubRel is the writer reliability flag value.
	pubRel string
	// echoRel is the reader reliability flag value.
	echoRel string
	// compatible reports whether the pair must flow.
	compatible bool
}

// relCompatibleCases lists reliability pairs that must deliver live.
//
// The forward reliable case is ROS reliable pub to the facade reliable
// reader. The ros_to_ros best_effort case is pure ROS to prove the
// best_effort pair flows without weld in the path. The reverse case is the
// facade reliable writer to a ROS best_effort reader, which is compatible
// per ROS RxO rules.
func relCompatibleCases() []relLiveCase {
	return []relLiveCase{
		{name: "reliable_to_reliable_forward", direction: "forward", pubRel: "reliable", echoRel: "reliable", compatible: true},
		{name: "besteffort_to_besteffort_rostoros", direction: "ros_to_ros", pubRel: "best_effort", echoRel: "best_effort", compatible: true},
		{name: "reliable_to_besteffort_reverse", direction: "reverse", pubRel: "reliable", echoRel: "best_effort", compatible: true},
	}
}

// relIncompatibleCases lists reliability pairs that must mismatch loudly.
//
// A best_effort writer can never satisfy a reliable reader, so ROS
// best_effort pub to the facade reliable reader must fail loudly with a
// reliability policy error and must never deliver silently.
func relIncompatibleCases() []relLiveCase {
	return []relLiveCase{
		{name: "besteffort_to_reliable_mismatch", direction: "forward", pubRel: "best_effort", echoRel: "reliable", compatible: false},
	}
}

// relProfile builds a core profile with fixed non reliability fields.
//
// Durability is passed by the caller so forward cases can use
// transient_local to match the facade and reverse and ros_to_ros cases can
// use volatile. History stays keep_last with depth 10 and liveliness stays
// automatic so only reliability and the chosen durability drive matching.
func relProfile(reliability, durability string) (core.QoSProfile, error) {
	var out core.QoSProfile
	switch reliability {
	case "reliable":
		out.Reliability = core.ReliabilityReliable
	case "best_effort":
		out.Reliability = core.ReliabilityBestEffort
	default:
		return core.QoSProfile{}, fmt.Errorf("reliability: unknown reliability %q", reliability)
	}
	switch durability {
	case "transient_local":
		out.Durability = core.DurabilityTransientLocal
	case "volatile":
		out.Durability = core.DurabilityVolatile
	default:
		return core.QoSProfile{}, fmt.Errorf("reliability: unknown durability %q", durability)
	}
	out.History = core.HistoryKeepLast
	out.Depth = 10
	out.Liveliness = core.LivelinessAutomatic
	return out, nil
}

// relProbeHelp runs one ros2 help argv and returns combined output.
func relProbeHelp(ctx context.Context, argv []string) (string, error) {
	probeCtx, cancel := context.WithTimeout(ctx, relProbeWait)
	defer cancel()
	cmd := exec.CommandContext(probeCtx, "docker", argv...) //nolint:gosec // live harness probes fixed ros2 help argv with no shell
	out, err := cmd.CombinedOutput()
	if err != nil {
		return string(out), fmt.Errorf("reliability: pub help probe: %w", err)
	}
	return string(out), nil
}

// relSupported probes ros2 topic pub help for the reliability flag.
func relSupported(ctx context.Context, domain int) (bool, error) {
	argv := helpers.DockerArgs(helpers.Container, domain, helpers.RosBin, "topic", "pub", "--help")
	out, err := relProbeHelp(ctx, argv)
	if err != nil {
		return false, err
	}
	if !strings.Contains(out, "--qos-reliability") {
		return false, nil
	}
	return true, nil
}

// relRequireFlag skips the test when the peer lacks the reliability flag.
func relRequireFlag(t *testing.T, ctx context.Context, domain int) {
	t.Helper()
	ok, err := relSupported(ctx, domain)
	if err != nil {
		t.Skipf("reliability flag probe failed, skipping: %v", err)
	}
	if !ok {
		t.Skipf("ros2 topic pub lacks --qos-reliability, skipping reliability gate")
	}
}

// relCheckCompatible asserts a compatible report has no errors.
func relCheckCompatible(t *testing.T, name string, report core.CompatibilityReport) {
	t.Helper()
	helpers.CheckQoSCompatible(t, name, report)
}

// relCheckIncompatible asserts an incompatible report names reliability loudly.
func relCheckIncompatible(t *testing.T, name string, report core.CompatibilityReport, offered, requested core.QoSProfile, publisher, subscriber string) {
	t.Helper()
	helpers.CheckQoSIncompatible(t, name, report, offered, requested, publisher, subscriber, core.PolicyReliability, "reliability")
}

// relCoreOfferedRequested maps a live case to offered and requested profiles.
//
// Forward uses the ROS pub profile as offered and the facade as requested.
// Reverse uses the facade as offered and the ROS echo profile as requested.
// Ros_to_ros uses two neutral volatile profiles so only reliability drives
// the verdict. Durability is chosen to keep every compatible case free of
// durability errors.
func relCoreOfferedRequested(c relLiveCase) (core.QoSProfile, core.QoSProfile, string, string, error) {
	facade := core.FacadeAdvertisedQoS()
	switch c.direction {
	case "forward":
		rosProfile, err := relProfile(c.pubRel, "transient_local")
		if err != nil {
			return core.QoSProfile{}, core.QoSProfile{}, "", "", err
		}
		return rosProfile, facade, "ros-pub", "weld-echo", nil
	case "reverse":
		rosProfile, err := relProfile(c.echoRel, "volatile")
		if err != nil {
			return core.QoSProfile{}, core.QoSProfile{}, "", "", err
		}
		return facade, rosProfile, "weld-pub", "ros-echo", nil
	case "ros_to_ros":
		offered, err := relProfile(c.pubRel, "volatile")
		if err != nil {
			return core.QoSProfile{}, core.QoSProfile{}, "", "", err
		}
		requested, err := relProfile(c.echoRel, "volatile")
		if err != nil {
			return core.QoSProfile{}, core.QoSProfile{}, "", "", err
		}
		return offered, requested, "ros-pub", "ros-echo", nil
	default:
		return core.QoSProfile{}, core.QoSProfile{}, "", "", fmt.Errorf("reliability: unknown direction %q", c.direction)
	}
}

// relCheckCore asserts core compatibility matches the case expectation first.
func relCheckCore(t *testing.T, c relLiveCase) {
	t.Helper()
	offered, requested, publisher, subscriber, err := relCoreOfferedRequested(c)
	if err != nil {
		t.Fatalf("case %s: profiles: %v", c.name, err)
	}
	report := core.CheckCompatibility(offered, requested)
	if c.compatible {
		relCheckCompatible(t, c.name, report)
		return
	}
	relCheckIncompatible(t, c.name, report, offered, requested, publisher, subscriber)
}

// relAssertCompatibleFlow fails when a compatible echo misses the payload.
//
// A clean weld echo with no backend error plus a missing live ID is a flow
// failure, not a skip. Only no-peer and unsupported flag cases skip.
func relAssertCompatibleFlow(t *testing.T, c relLiveCase, stdout, stderr, live, payload string) {
	t.Helper()
	if !strings.Contains(stdout, live) {
		t.Fatalf("case %s: weld echo missed live payload %q, want flow for compatible pair\nstdout:\n%s\nstderr:\n%s", c.name, payload, stdout, stderr)
	}
}

// relAssertIncompatibleSilent fails when an incompatible pair delivers.
//
// Callers pin core.CheckCompatibility incompatible first via relCheckCore,
// so live delivery must be absent. Silence with a loud core error is the
// required result.
func relAssertIncompatibleSilent(t *testing.T, c relLiveCase, stdout, live, payload string) {
	t.Helper()
	if strings.Contains(stdout, live) {
		t.Fatalf("case %s: incompatible pair delivered payload %q, want no flow (core.CheckCompatibility reports incompatible)", c.name, payload)
	}
	t.Logf("case %s: incompatible pair stayed silent on live ID as required, core error is loud above", c.name)
}

// relAssertReverseDelivered fails when ROS echo misses the payload.
//
// A clean weld pub with sent confirmation plus a missing live ID is a flow
// failure, not a skip. Only no-peer and unsupported flag cases skip.
func relAssertReverseDelivered(t *testing.T, c relLiveCase, echoOutput, live, payload string) {
	t.Helper()
	if !strings.Contains(echoOutput, live) {
		t.Fatalf("case %s: ros echo missed live payload %q, want flow for compatible pair\necho output:\n%s", c.name, payload, echoOutput)
	}
}

// relRunForward runs ROS pub with a reliability flag and checks weld echo.
//
// Domain is the allocated exclusive domain plumbed to DockerArgs and weld
// --domain flags, so parallel workers never share a domain.
//
// It starts ROS pub with the reliability flag, waits for settle, then checks
// weld echo output for the live ID. Compatible cases must deliver and a
// missing payload with a clean echo fails. Incompatible cases must stay
// silent on the live ID and delivery fails; core.CheckCompatibility pins the
// mismatch loudly first via relCheckCore.
func relRunForward(t *testing.T, ctx context.Context, bin string, domain int, c relLiveCase) {
	t.Helper()
	live := helpers.LiveID("rel-" + c.name)
	topic := helpers.LiveTopic(live)
	payload := "hello rel " + live
	logPath := filepath.Join(t.TempDir(), "rel-talker-"+live+".log")
	logFile, err := os.Create(logPath) //nolint:gosec // test reads and writes controlled temp file path
	if err != nil {
		t.Fatalf("case %s: create talker log: %v", c.name, err)
	}
	t.Cleanup(func() {
		cerr := logFile.Close()
		if cerr != nil {
			t.Logf("close talker log: %v", cerr)
		}
	})
	pubArg := fmt.Sprintf("{data: %s}", payload)
	argv := []string{helpers.RosBin, "topic", "pub", topic, helpers.RosType, pubArg, "--rate", "2", "--qos-reliability", c.pubRel, "--qos-durability", "transient_local", "--qos-history", "keep_last", "--qos-depth", "10"}
	args := helpers.DockerArgs(helpers.Container, domain, argv...)
	talker := exec.CommandContext(ctx, "docker", args...) //nolint:gosec // live harness runs docker with controlled test args
	talker.Stdout = logFile
	talker.Stderr = logFile
	err = talker.Start()
	if err != nil {
		t.Fatalf("case %s: start ROS talker: %v", c.name, err)
	}
	t.Cleanup(func() {
		helpers.StopCmd(talker)
		cleanupCtx, cleanupCancel := context.WithTimeout(context.WithoutCancel(ctx), relCleanWait)
		defer cleanupCancel()
		helpers.KillContainerProc(cleanupCtx, helpers.Container, domain, live)
	})
	helpers.Sleep(ctx, relForwardNap)
	if ctx.Err() != nil {
		t.Fatalf("case %s: context ended before echo: %v", c.name, ctx.Err())
	}
	stdout, stderr, err := helpers.RunWeldEcho(ctx, bin, topic, domain, relEchoWait)
	if err != nil {
		t.Fatalf("case %s: weld echo: %v\nstdout:\n%s\nstderr:\n%s", c.name, err, stdout, stderr)
	}
	combined := stdout + "\n" + stderr
	if strings.Contains(combined, "not implemented") {
		t.Fatalf("case %s: weld echo backend error:\n%s", c.name, combined)
	}
	if strings.Contains(combined, "not wired") {
		t.Fatalf("case %s: weld echo backend error:\n%s", c.name, combined)
	}
	if c.compatible {
		relAssertCompatibleFlow(t, c, stdout, stderr, live, payload)
		return
	}
	relAssertIncompatibleSilent(t, c, stdout, live, payload)
}

// relRunReverse runs ROS echo with a reliability flag and checks weld pub.
//
// Domain is the allocated exclusive domain plumbed to DockerArgs and weld
// --domain flags, so parallel workers never share a domain.
//
// It starts ROS echo with the reliability flag, waits for settle, then
// checks weld pub delivery. Compatible cases must deliver and a missing
// payload fails.
func relRunReverse(t *testing.T, ctx context.Context, bin string, domain int, c relLiveCase) {
	t.Helper()
	live := helpers.LiveID("rel-" + c.name)
	topic := helpers.LiveTopic(live)
	payload := "hello rel reverse " + live
	echoPath := filepath.Join(t.TempDir(), "rel-rosecho-"+live+".log")
	echoFile, err := os.Create(echoPath) //nolint:gosec // test reads and writes controlled temp file path
	if err != nil {
		t.Fatalf("case %s: create ros echo log: %v", c.name, err)
	}
	t.Cleanup(func() {
		cerr := echoFile.Close()
		if cerr != nil {
			t.Logf("close ros echo log: %v", cerr)
		}
	})
	argv := []string{helpers.RosBin, "topic", "echo", topic, helpers.RosType, "--qos-reliability", c.echoRel, "--qos-durability", "volatile", "--qos-history", "keep_last", "--qos-depth", "10"}
	args := helpers.DockerArgs(helpers.Container, domain, argv...)
	echoCmd := exec.CommandContext(ctx, "docker", args...) //nolint:gosec // live harness runs docker with fixed test args
	echoCmd.Stdout = echoFile
	echoCmd.Stderr = echoFile
	err = echoCmd.Start()
	if err != nil {
		t.Fatalf("case %s: start ros2 topic echo: %v", c.name, err)
	}
	t.Cleanup(func() {
		helpers.StopCmd(echoCmd)
		cleanupCtx, cleanupCancel := context.WithTimeout(context.WithoutCancel(ctx), relCleanWait)
		defer cleanupCancel()
		helpers.KillContainerProc(cleanupCtx, helpers.Container, domain, topic)
	})
	helpers.Sleep(ctx, relReverseNap)
	if ctx.Err() != nil {
		t.Fatalf("case %s: context ended before pub: %v", c.name, ctx.Err())
	}
	found, stdout, stderr, err := helpers.PublishUntilSeen(ctx, bin, topic, domain, payload, echoPath, relPollWait, 3)
	if err != nil {
		t.Fatalf("case %s: weld pub: %v\nstdout:\n%s\nstderr:\n%s", c.name, err, stdout, stderr)
	}
	if !strings.Contains(stdout, "sent:") {
		t.Fatalf("case %s: weld pub missing confirmation:\nstdout:\n%s\nstderr:\n%s", c.name, stdout, stderr)
	}
	if !found {
		data, rerr := os.ReadFile(echoPath) //nolint:gosec // test reads controlled temp file path
		if rerr != nil {
			t.Fatalf("case %s: ros echo missed live payload %q: %v", c.name, payload, rerr)
		}
		relAssertReverseDelivered(t, c, string(data), live, payload)
	}
}

// relRunRosToRos runs ROS pub to ROS echo with matched reliability flags.
//
// Domain is the allocated exclusive domain plumbed to DockerArgs, so
// parallel workers never share a domain.
//
// Both sides run in the peer on the allocated exclusive domain with per-run topics and payloads.
// This proves the best_effort pair flows without weld in the path, which
// the weld facade cannot prove because the facade always offers reliable.
// A miss here fails because a compatible pair must flow.
func relRunRosToRos(t *testing.T, ctx context.Context, domain int, c relLiveCase) {
	t.Helper()
	live := helpers.LiveID("rel-" + c.name)
	topic := helpers.LiveTopic(live)
	payload := "hello rel rostoros " + live
	echoPath := filepath.Join(t.TempDir(), "rel-rostoros-echo-"+live+".log")
	echoFile, err := os.Create(echoPath) //nolint:gosec // test reads and writes controlled temp file path
	if err != nil {
		t.Fatalf("case %s: create ros echo log: %v", c.name, err)
	}
	t.Cleanup(func() {
		cerr := echoFile.Close()
		if cerr != nil {
			t.Logf("close ros echo log: %v", cerr)
		}
	})
	echoArgv := []string{helpers.RosBin, "topic", "echo", topic, helpers.RosType, "--qos-reliability", c.echoRel, "--qos-durability", "volatile", "--qos-history", "keep_last", "--qos-depth", "10"}
	echoArgs := helpers.DockerArgs(helpers.Container, domain, echoArgv...)
	echoCmd := exec.CommandContext(ctx, "docker", echoArgs...) //nolint:gosec // live harness runs docker with fixed test args
	echoCmd.Stdout = echoFile
	echoCmd.Stderr = echoFile
	err = echoCmd.Start()
	if err != nil {
		t.Fatalf("case %s: start ros2 topic echo: %v", c.name, err)
	}
	t.Cleanup(func() {
		helpers.StopCmd(echoCmd)
		cleanupCtx, cleanupCancel := context.WithTimeout(context.WithoutCancel(ctx), relCleanWait)
		defer cleanupCancel()
		helpers.KillContainerProc(cleanupCtx, helpers.Container, domain, topic)
	})
	helpers.Sleep(ctx, relReverseNap)
	if ctx.Err() != nil {
		t.Fatalf("case %s: context ended before pub: %v", c.name, ctx.Err())
	}
	pubLogPath := filepath.Join(t.TempDir(), "rel-rostoros-pub-"+live+".log")
	pubLog, err := os.Create(pubLogPath) //nolint:gosec // test reads and writes controlled temp file path
	if err != nil {
		t.Fatalf("case %s: create ros pub log: %v", c.name, err)
	}
	t.Cleanup(func() {
		cerr := pubLog.Close()
		if cerr != nil {
			t.Logf("close ros pub log: %v", cerr)
		}
	})
	pubArg := fmt.Sprintf("{data: %s}", payload)
	pubArgv := []string{helpers.RosBin, "topic", "pub", topic, helpers.RosType, pubArg, "--rate", "2", "--qos-reliability", c.pubRel, "--qos-durability", "volatile", "--qos-history", "keep_last", "--qos-depth", "10"}
	pubArgs := helpers.DockerArgs(helpers.Container, domain, pubArgv...)
	pubCmd := exec.CommandContext(ctx, "docker", pubArgs...) //nolint:gosec // live harness runs docker with fixed test args
	pubCmd.Stdout = pubLog
	pubCmd.Stderr = pubLog
	err = pubCmd.Start()
	if err != nil {
		t.Fatalf("case %s: start ros2 topic pub: %v", c.name, err)
	}
	t.Cleanup(func() {
		helpers.StopCmd(pubCmd)
		cleanupCtx, cleanupCancel := context.WithTimeout(context.WithoutCancel(ctx), relCleanWait)
		defer cleanupCancel()
		helpers.KillContainerProc(cleanupCtx, helpers.Container, domain, live)
	})
	found := helpers.WaitForFileContains(ctx, echoPath, live, relPollWait)
	if !found {
		data, rerr := os.ReadFile(echoPath) //nolint:gosec // test reads controlled temp file path
		if rerr != nil {
			t.Fatalf("case %s: ros echo missed live payload %q: %v", c.name, payload, rerr)
		}
		pubData, perr := os.ReadFile(pubLogPath) //nolint:gosec // test reads controlled temp file path
		if perr != nil {
			t.Fatalf("case %s: ros echo missed live payload %q, pub log unreadable: %v\necho output:\n%s", c.name, payload, perr, string(data))
		}
		t.Fatalf("case %s: ros echo missed live payload %q, want flow for compatible pair\necho output:\n%s\npub output:\n%s", c.name, payload, string(data), string(pubData))
	}
}

// relRunCase checks core loudly then runs the live direction.
func relRunCase(t *testing.T, ctx context.Context, bin string, domain int, c relLiveCase) {
	t.Helper()
	relCheckCore(t, c)
	switch c.direction {
	case "forward":
		relRunForward(t, ctx, bin, domain, c)
	case "reverse":
		relRunReverse(t, ctx, bin, domain, c)
	case "ros_to_ros":
		relRunRosToRos(t, ctx, domain, c)
	default:
		t.Fatalf("case %s: unknown direction %q", c.name, c.direction)
	}
}

// TestROSQoSReliabilityCore pins the four way reliability truth table.
//
// Reliable offered always satisfies both readers. Best_effort offered
// satisfies only a best_effort reader and mismatches a reliable reader with
// an explicit reliability error. The table uses neutral volatile profiles so
// only reliability drives the verdict, then the live gates prove the same
// verdicts on the allocated exclusive domain.
func TestROSQoSReliabilityCore(t *testing.T) {
	t.Parallel()
	rel, err := relProfile("reliable", "volatile")
	if err != nil {
		t.Fatalf("reliable profile: %v", err)
	}
	best, err := relProfile("best_effort", "volatile")
	if err != nil {
		t.Fatalf("best effort profile: %v", err)
	}
	report := core.CheckCompatibility(rel, rel)
	relCheckCompatible(t, "reliable_to_reliable", report)
	report = core.CheckCompatibility(best, best)
	relCheckCompatible(t, "besteffort_to_besteffort", report)
	report = core.CheckCompatibility(best, rel)
	relCheckIncompatible(t, "besteffort_to_reliable", report, best, rel, "ros-pub", "weld-echo")
	report = core.CheckCompatibility(rel, best)
	relCheckCompatible(t, "reliable_to_besteffort", report)
}

// TestROSQoSReliabilityCompatibleDelivers checks compatible pairs deliver live.
//
// It parametrizes ros2 topic pub and echo with --qos-reliability on the
// allocated exclusive domain with the Cyclone RMW in weld-jazzy. Each subtest uses its own live ID
// and topic with context timeouts and docker argv only. It skips cleanly
// only when no peer is present or when the flag is unsupported. A missing
// payload with a clean echo is a failure. The ros_to_ros best_effort pair
// also fails on a miss.
// relGateSetup runs the outer reliability probe and returns the subtest context.
//
// It allocates one exclusive domain for the reliability flag probe, skips
// cleanly without a peer or flag, builds weld once, then releases the outer
// domain so parallel workers reuse the 32-domain pool. Subtests allocate
// fresh exclusive domains for their live directions. The caller defers the
// returned cancel.
func relGateSetup(t *testing.T) (context.Context, context.CancelFunc, string) {
	t.Helper()
	domain := helpers.MustAllocateDomain(t)
	ctx, cancel := context.WithTimeout(context.Background(), relTestTimeout)
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)
	relRequireFlag(t, ctx, domain)
	bin := helpers.BuildWeld(t, ctx)
	// The outer domain covered only the reliability probe. Free it now so
	// parallel workers reuse the 32-domain pool instead of pinning two
	// domains per worker for the whole test.
	helpers.ReleaseDomain(domain)
	return ctx, cancel, bin
}

func TestROSQoSReliabilityCompatibleDelivers(t *testing.T) {
	t.Parallel()
	ctx, cancel, bin := relGateSetup(t)
	defer cancel()
	// Reliability compatible cases span three entries: forward reliable to
	// reliable, ros_to_ros best effort to best effort without weld, and
	// reverse reliable to best effort. Each needs a fresh domain so parallel
	// workers never share discovery.
	t.Log("reliability gate runs single-axis compatible cases on fresh domains")
	for _, c := range relCompatibleCases() {
		t.Run(c.name, func(t *testing.T) {
			subdomain := helpers.MustAllocateDomain(t)
			relRunCase(t, ctx, bin, subdomain, c)
		})
	}
}

// TestROSQoSReliabilityIncompatibleLoud checks the mismatch pair fails loudly.
//
// Best_effort offered against a reliable request must return an explicit
// reliability error through core.CheckCompatibility and must never deliver
// silently live. The subtest logs the mismatch with topic, endpoint pair,
// and policy kind so silence is a failure. It skips cleanly only when no
// peer is present or when the reliability flag is unsupported.
func TestROSQoSReliabilityIncompatibleLoud(t *testing.T) {
	t.Parallel()
	ctx, cancel, bin := relGateSetup(t)
	defer cancel()
	// Reliability incompatible cases pin the single best-effort offer against
	// the facade reliable request. It needs a fresh domain so parallel
	// workers stay isolated from compatible subtests.
	t.Log("reliability gate runs single-axis mismatch cases on fresh domains")
	for _, c := range relIncompatibleCases() {
		t.Run(c.name, func(t *testing.T) {
			subdomain := helpers.MustAllocateDomain(t)
			subCtx, subCancel := context.WithTimeout(ctx, relMismatchWait+2*relEchoWait)
			defer subCancel()
			relRunCase(t, subCtx, bin, subdomain, c)
		})
	}
}
