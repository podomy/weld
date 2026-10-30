// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package ros_test

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/podomy/weld/internal/core"
	"github.com/podomy/weld/testbed/live/helpers"
)

// QoS matrix constants for Jazzy canonical.
//
// Each subtest allocates one exclusive domain from the parallel pool, so
// per-run topics plus the allocated domain isolate runs. The container
// name and Cyclone RMW come from helpers so the gate stays on weld-jazzy
// with RMW_IMPLEMENTATION=rmw_cyclonedds_cpp.
const (
	qosEchoWait     = 10 * time.Second
	qosPollWait     = 20 * time.Second
	qosForwardNap   = 5 * time.Second
	qosReverseNap   = 6 * time.Second
	qosTestTimeout  = 4 * time.Minute
	qosProbeWait    = 15 * time.Second
	qosCleanWait    = 10 * time.Second
	qosMismatchWait = 8 * time.Second
)

// qosCase describes one live QoS combination.
//
// Reliability, durability, history, and depth map directly to the
// ros2 topic pub and echo flags of the same names. Direction is either
// forward for ROS pub to weld echo or reverse for weld pub to ROS echo.
// Compatible cases must deliver the live ID. Incompatible cases must not
// deliver and must carry an explicit core mismatch with the named policy.
type qosCase struct {
	// name scopes the subtest and the live ID prefix.
	name string
	// direction is forward or reverse.
	direction string
	// reliability is reliable or best_effort.
	reliability string
	// durability is transient_local or volatile.
	durability string
	// history is keep_last or keep_all.
	history string
	// depth is the queue size string passed to --qos-depth.
	depth string
	// compatible reports whether the pair must flow.
	compatible bool
	// wantPolicy names the expected mismatch policy for incompatible cases.
	wantPolicy core.PolicyKind
}

// qosCompatibleCases lists pairs that must deliver.
//
// Forward entries offer reliable plus transient local so they satisfy the
// facade request under docs/qos. Reverse entries request volatile so
// they satisfy both the facade offer of transient local and the wire offer
// of volatile. History and depth vary to prove they never affect matching.
func qosCompatibleCases() []qosCase {
	return []qosCase{
		{name: "forward_reliable_transient_keep10", direction: "forward", reliability: "reliable", durability: "transient_local", history: "keep_last", depth: "10", compatible: true},
		{name: "forward_reliable_transient_depth1", direction: "forward", reliability: "reliable", durability: "transient_local", history: "keep_last", depth: "1", compatible: true},
		{name: "forward_reliable_transient_keepall", direction: "forward", reliability: "reliable", durability: "transient_local", history: "keep_all", depth: "10", compatible: true},
		{name: "reverse_reliable_volatile_keep10", direction: "reverse", reliability: "reliable", durability: "volatile", history: "keep_last", depth: "10", compatible: true},
		{name: "reverse_besteffort_volatile_keep10", direction: "reverse", reliability: "best_effort", durability: "volatile", history: "keep_last", depth: "10", compatible: true},
		{name: "reverse_reliable_volatile_depth1", direction: "reverse", reliability: "reliable", durability: "volatile", history: "keep_last", depth: "1", compatible: true},
	}
}

// qosIncompatibleCases lists forward pairs that must mismatch loudly.
//
// Best effort offered against the facade reliable request is incompatible
// under both the facade profile and the wire reader, so live delivery must
// not happen. Durability, history, and depth still vary to prove the matrix
// passes all four flags while reliability drives the mismatch.
func qosIncompatibleCases() []qosCase {
	return []qosCase{
		{name: "forward_besteffort_transient_keep10", direction: "forward", reliability: "best_effort", durability: "transient_local", history: "keep_last", depth: "10", compatible: false, wantPolicy: core.PolicyReliability},
		{name: "forward_besteffort_volatile_keep10", direction: "forward", reliability: "best_effort", durability: "volatile", history: "keep_last", depth: "10", compatible: false, wantPolicy: core.PolicyReliability},
		{name: "forward_besteffort_transient_keepall", direction: "forward", reliability: "best_effort", durability: "transient_local", history: "keep_all", depth: "10", compatible: false, wantPolicy: core.PolicyReliability},
	}
}

// qosProbeHelp runs one ros2 help argv and returns combined output.
func qosProbeHelp(ctx context.Context, argv []string) (string, error) {
	probeCtx, cancel := context.WithTimeout(ctx, qosProbeWait)
	defer cancel()
	cmd := exec.CommandContext(probeCtx, "docker", argv...) //nolint:gosec // live harness probes fixed ros2 help argv with no shell
	out, err := cmd.CombinedOutput()
	if err != nil {
		return string(out), fmt.Errorf("helpers: qos help probe: %w", err)
	}
	return string(out), nil
}

// qosMatrixSupported probes pub and echo help for the four QoS flags.
func qosMatrixSupported(ctx context.Context, domain int) (bool, error) {
	pubArgv := helpers.DockerArgs(helpers.Container, domain, helpers.RosBin, "topic", "pub", "--help")
	pubOut, err := qosProbeHelp(ctx, pubArgv)
	if err != nil {
		return false, err
	}
	echoArgv := helpers.DockerArgs(helpers.Container, domain, helpers.RosBin, "topic", "echo", "--help")
	echoOut, err := qosProbeHelp(ctx, echoArgv)
	if err != nil {
		return false, err
	}
	want := []string{"--qos-reliability", "--qos-durability", "--qos-history", "--qos-depth"}
	for _, flag := range want {
		if !strings.Contains(pubOut, flag) {
			return false, nil
		}
		if !strings.Contains(echoOut, flag) {
			return false, nil
		}
	}
	return true, nil
}

// qosRequireFlags skips the test when the peer lacks the QoS flags.
func qosRequireFlags(t *testing.T, ctx context.Context, domain int) {
	t.Helper()
	ok, err := qosMatrixSupported(ctx, domain)
	if err != nil {
		t.Skipf("qos flag probe failed, skipping matrix: %v", err)
	}
	if !ok {
		t.Skipf("ros2 topic pub or echo lacks QoS flags, skipping matrix")
	}
}

// qosProfileFromStrings builds a core profile from ROS flag values.
func qosProfileFromStrings(reliability, durability, history, depth string) (core.QoSProfile, error) {
	var out core.QoSProfile
	switch reliability {
	case "reliable":
		out.Reliability = core.ReliabilityReliable
	case "best_effort":
		out.Reliability = core.ReliabilityBestEffort
	default:
		return core.QoSProfile{}, fmt.Errorf("qos: unknown reliability %q", reliability)
	}
	switch durability {
	case "transient_local":
		out.Durability = core.DurabilityTransientLocal
	case "volatile":
		out.Durability = core.DurabilityVolatile
	default:
		return core.QoSProfile{}, fmt.Errorf("qos: unknown durability %q", durability)
	}
	switch history {
	case "keep_last":
		out.History = core.HistoryKeepLast
	case "keep_all":
		out.History = core.HistoryKeepAll
	default:
		return core.QoSProfile{}, fmt.Errorf("qos: unknown history %q", history)
	}
	parsed, err := strconv.ParseUint(depth, 10, 64)
	if err != nil {
		return core.QoSProfile{}, fmt.Errorf("qos: parse depth %q: %w", depth, err)
	}
	out.Depth = parsed
	out.Liveliness = core.LivelinessAutomatic
	return out, nil
}

// qosResolveProfiles maps direction to offered plus requested plus names.
func qosResolveProfiles(c qosCase, rosProfile, facade core.QoSProfile) (core.QoSProfile, core.QoSProfile, string, string) {
	if c.direction == "forward" {
		return rosProfile, facade, "ros-pub", "weld-echo"
	}
	return facade, rosProfile, "weld-pub", "ros-echo"
}

// qosCheckCompatible asserts a compatible report has no errors.
func qosCheckCompatible(t *testing.T, c qosCase, report core.CompatibilityReport) {
	t.Helper()
	helpers.CheckQoSCompatible(t, c.name, report)
}

// qosCheckIncompatible asserts an incompatible report names the policy loudly.
func qosCheckIncompatible(t *testing.T, c qosCase, report core.CompatibilityReport, offered, requested core.QoSProfile, publisher, subscriber string) {
	t.Helper()
	helpers.CheckQoSIncompatible(t, c.name, report, offered, requested, publisher, subscriber, c.wantPolicy, "qos")
}

// qosCheckCore asserts core compatibility matches the case expectation.
func qosCheckCore(t *testing.T, c qosCase) {
	t.Helper()
	rosProfile, err := qosProfileFromStrings(c.reliability, c.durability, c.history, c.depth)
	if err != nil {
		t.Fatalf("case %s: profile: %v", c.name, err)
	}
	facade := core.FacadeAdvertisedQoS()
	offered, requested, publisher, subscriber := qosResolveProfiles(c, rosProfile, facade)
	report := core.CheckCompatibility(offered, requested)
	if c.compatible {
		qosCheckCompatible(t, c, report)
		return
	}
	qosCheckIncompatible(t, c, report, offered, requested, publisher, subscriber)
}

// qosArgv builds the four QoS flags for ros2 pub and echo.
func qosArgv(c qosCase) []string {
	return []string{"--qos-reliability", c.reliability, "--qos-durability", c.durability, "--qos-history", c.history, "--qos-depth", c.depth}
}

// qosRunForward runs ROS pub with QoS flags and checks weld echo output.
func qosRunForward(t *testing.T, ctx context.Context, bin string, domain int, c qosCase) {
	t.Helper()
	live := helpers.LiveID("qos-" + c.name)
	topic := helpers.LiveTopic(live)
	payload := "hello qos " + live
	logPath := filepath.Join(t.TempDir(), "qos-talker-"+live+".log")
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
	argv := []string{helpers.RosBin, "topic", "pub", topic, helpers.RosType, pubArg, "--rate", "2"}
	argv = append(argv, qosArgv(c)...)
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
		cleanupCtx, cleanupCancel := context.WithTimeout(context.WithoutCancel(ctx), qosCleanWait)
		defer cleanupCancel()
		helpers.KillContainerProc(cleanupCtx, helpers.Container, domain, live)
	})
	helpers.Sleep(ctx, qosForwardNap)
	if ctx.Err() != nil {
		t.Fatalf("case %s: context ended before echo: %v", c.name, ctx.Err())
	}
	stdout, stderr, err := helpers.RunWeldEcho(ctx, bin, topic, domain, qosEchoWait)
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
		if !strings.Contains(stdout, live) {
			t.Fatalf("case %s: weld echo missed live payload %q\nstdout:\n%s\nstderr:\n%s", c.name, payload, stdout, stderr)
		}
		return
	}
	if strings.Contains(stdout, live) {
		t.Fatalf("case %s: incompatible pair delivered payload %q, want no flow", c.name, payload)
	}
	t.Logf("case %s: incompatible pair stayed silent on live ID as required, core error is loud above", c.name)
}

// qosRunReverse runs ROS echo with QoS flags and checks weld pub delivery.
func qosRunReverse(t *testing.T, ctx context.Context, bin string, domain int, c qosCase) {
	t.Helper()
	live := helpers.LiveID("qos-" + c.name)
	topic := helpers.LiveTopic(live)
	payload := "hello qos reverse " + live
	echoPath := filepath.Join(t.TempDir(), "qos-rosecho-"+live+".log")
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
	argv := []string{helpers.RosBin, "topic", "echo", topic, helpers.RosType}
	argv = append(argv, qosArgv(c)...)
	args := helpers.DockerArgs(helpers.Container, domain, argv...)
	echoCmd := exec.CommandContext(ctx, "docker", args...) //nolint:gosec // live harness runs docker with controlled test args
	echoCmd.Stdout = echoFile
	echoCmd.Stderr = echoFile
	err = echoCmd.Start()
	if err != nil {
		t.Fatalf("case %s: start ros2 topic echo: %v", c.name, err)
	}
	t.Cleanup(func() {
		helpers.StopCmd(echoCmd)
		cleanupCtx, cleanupCancel := context.WithTimeout(context.WithoutCancel(ctx), qosCleanWait)
		defer cleanupCancel()
		helpers.KillContainerProc(cleanupCtx, helpers.Container, domain, topic)
	})
	helpers.Sleep(ctx, qosReverseNap)
	if ctx.Err() != nil {
		t.Fatalf("case %s: context ended before pub: %v", c.name, ctx.Err())
	}
	found, stdout, stderr, err := helpers.PublishUntilSeen(ctx, bin, topic, domain, payload, echoPath, qosPollWait, 3)
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
		t.Fatalf("case %s: ros echo missed live payload %q\necho output:\n%s", c.name, payload, string(data))
	}
}

// qosRunCase checks core loudly then runs the live direction.
func qosRunCase(t *testing.T, ctx context.Context, bin string, domain int, c qosCase) {
	t.Helper()
	qosCheckCore(t, c)
	if c.direction == "forward" {
		qosRunForward(t, ctx, bin, domain, c)
		return
	}
	qosRunReverse(t, ctx, bin, domain, c)
}

// TestROSQoSFacadeProfile pins the single advertised facade profile.
func TestROSQoSFacadeProfile(t *testing.T) {
	t.Parallel()
	got := core.FacadeAdvertisedQoS()
	if got.Reliability != core.ReliabilityReliable {
		t.Fatalf("facade reliability = %s, want reliable", got.Reliability.String())
	}
	if got.Durability != core.DurabilityTransientLocal {
		t.Fatalf("facade durability = %s, want transient_local", got.Durability.String())
	}
	if got.History != core.HistoryKeepLast {
		t.Fatalf("facade history = %s, want keep_last", got.History.String())
	}
	if got.Depth != core.FacadeHistoryDepth {
		t.Fatalf("facade depth = %d, want %d", got.Depth, core.FacadeHistoryDepth)
	}
	if got.Liveliness != core.LivelinessAutomatic {
		t.Fatalf("facade liveliness = %s, want automatic", got.Liveliness.String())
	}
}

// qosCheckCompatibleTable pins all compatible cases stay compatible.
func qosCheckCompatibleTable(t *testing.T, facade core.QoSProfile) {
	t.Helper()
	for _, c := range qosCompatibleCases() {
		rosProfile, err := qosProfileFromStrings(c.reliability, c.durability, c.history, c.depth)
		if err != nil {
			t.Fatalf("case %s: profile: %v", c.name, err)
		}
		offered, requested, _, _ := qosResolveProfiles(c, rosProfile, facade)
		report := core.CheckCompatibility(offered, requested)
		if !report.Compatible {
			t.Fatalf("case %s: want compatible, got %s", c.name, report.ErrorText())
		}
	}
}

// qosCheckIncompatibleTable pins all incompatible cases stay incompatible.
func qosCheckIncompatibleTable(t *testing.T, facade core.QoSProfile) {
	t.Helper()
	for _, c := range qosIncompatibleCases() {
		rosProfile, err := qosProfileFromStrings(c.reliability, c.durability, c.history, c.depth)
		if err != nil {
			t.Fatalf("case %s: profile: %v", c.name, err)
		}
		report := core.CheckCompatibility(rosProfile, facade)
		if report.Compatible {
			t.Fatalf("case %s: want incompatible, got compatible", c.name)
		}
		if len(report.Errors) == 0 {
			t.Fatalf("case %s: incompatible with no explicit errors", c.name)
		}
	}
}

// qosCheckDepthVariations pins depth plus history variations stay compatible.
func qosCheckDepthVariations(t *testing.T, facade core.QoSProfile) {
	t.Helper()
	deep, err := qosProfileFromStrings("reliable", "transient_local", "keep_last", "1")
	if err != nil {
		t.Fatalf("depth profile: %v", err)
	}
	report := core.CheckCompatibility(deep, facade)
	if !report.Compatible {
		t.Fatalf("depth variation must stay compatible, got %s", report.ErrorText())
	}
	keepAll, err := qosProfileFromStrings("reliable", "transient_local", "keep_all", "10")
	if err != nil {
		t.Fatalf("keep all profile: %v", err)
	}
	report = core.CheckCompatibility(keepAll, facade)
	if !report.Compatible {
		t.Fatalf("history variation must stay compatible, got %s", report.ErrorText())
	}
}

// TestROSQoSMatrixCore pins the RxO truth table for the matrix values.
func TestROSQoSMatrixCore(t *testing.T) {
	t.Parallel()
	facade := core.FacadeAdvertisedQoS()
	qosCheckCompatibleTable(t, facade)
	qosCheckIncompatibleTable(t, facade)
	qosCheckDepthVariations(t, facade)
}

// TestROSQoSCompatibleDelivers checks compatible QoS pairs deliver live.
//
// It parametrizes ros2 topic pub and echo with --qos-reliability,
// --qos-durability, --qos-history, and --qos-depth on the allocated exclusive domain with the
// Cyclone RMW in weld-jazzy. Each subtest uses its own live ID and topic
// with context timeouts and docker argv only. It skips cleanly only when no
// peer is present or when the flags are unsupported. A missing payload with
// ok echo output fails.
func TestROSQoSCompatibleDelivers(t *testing.T) {
	t.Parallel()
	domain := helpers.MustAllocateDomain(t)
	ctx, cancel := context.WithTimeout(context.Background(), qosTestTimeout)
	defer cancel()
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)
	qosRequireFlags(t, ctx, domain)
	bin := helpers.BuildWeld(t, ctx)
	// The outer domain served only the flag probe. Release it before the
	// subtests so concurrent workers share the 32-domain pool instead of
	// each holding two domains for the whole test. Matrix subtests then
	// run the four-flag live direction on fresh exclusive domains.
	helpers.ReleaseDomain(domain)
	// Matrix compatible cases span six entries: three forward reliable
	// transient local with varying history and depth, plus three reverse
	// with volatile and mixed reliability. Each needs a fresh domain so
	// parallel workers never share discovery.
	t.Logf("qos matrix runs four-flag compatible cases on fresh domains")
	for _, c := range qosCompatibleCases() {
		t.Run(c.name, func(t *testing.T) {
			subdomain := helpers.MustAllocateDomain(t)
			qosRunCase(t, ctx, bin, subdomain, c)
		})
	}
}

// TestROSQoSIncompatibleLoud checks incompatible QoS pairs fail loudly.
//
// Best effort offered against the facade reliable request must return an
// explicit reliability error through core and must never deliver silently
// live. Each subtest logs the mismatch with topic, endpoint pair, and
// policy kind so silence is a failure. It skips cleanly when no peer is
// present or when the QoS flags are unsupported.
func TestROSQoSIncompatibleLoud(t *testing.T) {
	t.Parallel()
	domain := helpers.MustAllocateDomain(t)
	ctx, cancel := context.WithTimeout(context.Background(), qosTestTimeout)
	defer cancel()
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)
	qosRequireFlags(t, ctx, domain)
	bin := helpers.BuildWeld(t, ctx)
	// The outer domain served only the flag probe. Release it before the
	// subtests so concurrent workers share the 32-domain pool instead of
	// each holding two domains for the whole test. Matrix mismatch subtests
	// then check the loud reliability error on fresh exclusive domains.
	helpers.ReleaseDomain(domain)
	// Matrix incompatible cases pin three forward best-effort offers against
	// the facade reliable request with varying durability, history, and
	// depth. Each needs a fresh domain so parallel workers stay isolated.
	t.Logf("qos matrix runs four-flag mismatch cases on fresh domains")
	for _, c := range qosIncompatibleCases() {
		t.Run(c.name, func(t *testing.T) {
			subdomain := helpers.MustAllocateDomain(t)
			subCtx, subCancel := context.WithTimeout(ctx, qosMismatchWait+2*qosEchoWait)
			defer subCancel()
			qosRunCase(t, subCtx, bin, subdomain, c)
		})
	}
}
