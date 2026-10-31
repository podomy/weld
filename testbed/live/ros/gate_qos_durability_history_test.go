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

// QoS durability and history constants for Jazzy canonical.
//
// Each subtest allocates one exclusive domain from the parallel pool. The container name and
// Cyclone RMW come from helpers so the gate stays on weld-jazzy with
// RMW_IMPLEMENTATION=rmw_cyclonedds_cpp. Only one run per domain and topic
// at a time is supported, so tests never run in parallel and every payload
// carries a per-run live ID. Reliability is pinned to reliable so the
// durability and history matrix drives the result.
const (
	qosDHEchoWait    = 10 * time.Second
	qosDHPollWait    = 20 * time.Second
	qosDHForwardNap  = 5 * time.Second
	qosDHReverseNap  = 6 * time.Second
	qosDHTestTimeout = 6 * time.Minute
	qosDHProbeWait   = 15 * time.Second
	qosDHCleanWait   = 10 * time.Second
	qosDHMismatchDur = 8 * time.Second
)

// qosDHCase describes one live durability plus history plus depth combination.
//
// Reliability is fixed to reliable. Direction is forward for ROS pub to weld
// echo or reverse for weld pub to ROS echo. Durability varies between
// transient_local and volatile. History varies between keep_last and
// keep_all. Depth varies across 1, 10, and 100 for keep_last. Compatible
// cases must deliver the live ID. Incompatible cases must not deliver and
// must carry an explicit durability mismatch.
type qosDHCase struct {
	// name scopes the subtest and the live ID prefix.
	name string
	// direction is forward or reverse.
	direction string
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

// qosDHCompatibleCases lists durability and history pairs that must deliver.
//
// Forward entries offer transient_local so they satisfy the facade request
// for transient_local. Reverse entries request transient_local or volatile
// so the facade offer of transient_local satisfies both. History and depth
// vary to prove they never affect matching. Reliability stays reliable.
func qosDHCompatibleCases() []qosDHCase {
	return []qosDHCase{
		{name: "forward_transient_keeplast_1", direction: "forward", durability: "transient_local", history: "keep_last", depth: "1", compatible: true},
		{name: "forward_transient_keeplast_10", direction: "forward", durability: "transient_local", history: "keep_last", depth: "10", compatible: true},
		{name: "forward_transient_keeplast_100", direction: "forward", durability: "transient_local", history: "keep_last", depth: "100", compatible: true},
		{name: "forward_transient_keepall", direction: "forward", durability: "transient_local", history: "keep_all", depth: "10", compatible: true},
		{name: "reverse_transient_keeplast_1", direction: "reverse", durability: "transient_local", history: "keep_last", depth: "1", compatible: true},
		{name: "reverse_transient_keeplast_10", direction: "reverse", durability: "transient_local", history: "keep_last", depth: "10", compatible: true},
		{name: "reverse_transient_keeplast_100", direction: "reverse", durability: "transient_local", history: "keep_last", depth: "100", compatible: true},
		{name: "reverse_transient_keepall", direction: "reverse", durability: "transient_local", history: "keep_all", depth: "10", compatible: true},
		{name: "reverse_volatile_keeplast_1", direction: "reverse", durability: "volatile", history: "keep_last", depth: "1", compatible: true},
		{name: "reverse_volatile_keeplast_10", direction: "reverse", durability: "volatile", history: "keep_last", depth: "10", compatible: true},
		{name: "reverse_volatile_keeplast_100", direction: "reverse", durability: "volatile", history: "keep_last", depth: "100", compatible: true},
		{name: "reverse_volatile_keepall", direction: "reverse", durability: "volatile", history: "keep_all", depth: "10", compatible: true},
	}
}

// qosDHIncompatibleCases lists forward volatile pairs that must mismatch.
//
// A volatile offer cannot satisfy the facade transient_local request, so
// forward volatile never flows regardless of history or depth. History and
// depth vary to prove they do not rescue the durability mismatch. Reliability
// stays reliable so durability alone drives the result.
func qosDHIncompatibleCases() []qosDHCase {
	return []qosDHCase{
		{name: "forward_volatile_keeplast_1", direction: "forward", durability: "volatile", history: "keep_last", depth: "1", compatible: false, wantPolicy: core.PolicyDurability},
		{name: "forward_volatile_keeplast_10", direction: "forward", durability: "volatile", history: "keep_last", depth: "10", compatible: false, wantPolicy: core.PolicyDurability},
		{name: "forward_volatile_keeplast_100", direction: "forward", durability: "volatile", history: "keep_last", depth: "100", compatible: false, wantPolicy: core.PolicyDurability},
		{name: "forward_volatile_keepall", direction: "forward", durability: "volatile", history: "keep_all", depth: "10", compatible: false, wantPolicy: core.PolicyDurability},
	}
}

// qosDHProbeHelp runs one ros2 help argv and returns combined output.
func qosDHProbeHelp(ctx context.Context, argv []string) (string, error) {
	probeCtx, cancel := context.WithTimeout(ctx, qosDHProbeWait)
	defer cancel()
	cmd := exec.CommandContext(probeCtx, "docker", argv...) //nolint:gosec // live harness probes fixed ros2 help argv with no shell
	out, err := cmd.CombinedOutput()
	if err != nil {
		return string(out), fmt.Errorf("helpers: qos durability history help probe: %w", err)
	}
	return string(out), nil
}

// qosDHFlagsSupported probes pub and echo help for durability history depth.
//
// It returns flagsOk true when both sides list --qos-durability,
// --qos-history, and --qos-depth, and keepAllOk true when both sides also
// mention keep_all. Callers skip cleanly when flagsOk is false and skip only
// keep_all cases when keepAllOk is false.
func qosDHFlagsSupported(ctx context.Context, domain int) (bool, bool, error) {
	pubArgv := helpers.DockerArgs(helpers.Container, domain, helpers.RosBin, "topic", "pub", "--help")
	pubOut, err := qosDHProbeHelp(ctx, pubArgv)
	if err != nil {
		return false, false, err
	}
	echoArgv := helpers.DockerArgs(helpers.Container, domain, helpers.RosBin, "topic", "echo", "--help")
	echoOut, err := qosDHProbeHelp(ctx, echoArgv)
	if err != nil {
		return false, false, err
	}
	want := []string{"--qos-durability", "--qos-history", "--qos-depth"}
	for _, flag := range want {
		if !strings.Contains(pubOut, flag) {
			return false, false, nil
		}
		if !strings.Contains(echoOut, flag) {
			return false, false, nil
		}
	}
	keepAllOk := strings.Contains(pubOut, "keep_all") && strings.Contains(echoOut, "keep_all")
	return true, keepAllOk, nil
}

// qosDHRequireFlags skips the test when the peer lacks the QoS flags.
//
// It returns keepAllOk so callers can skip only keep_all cases with a reason
// when the value is unsupported while still running keep_last depths.
func qosDHRequireFlags(t *testing.T, ctx context.Context, domain int) bool {
	t.Helper()
	flagsOk, keepAllOk, err := qosDHFlagsSupported(ctx, domain)
	if err != nil {
		t.Skipf("qos durability history flag probe failed, skipping matrix: %v", err)
	}
	if !flagsOk {
		t.Skipf("ros2 topic pub or echo lacks --qos-durability, --qos-history, or --qos-depth, skipping matrix")
	}
	if !keepAllOk {
		t.Logf("ros2 topic pub or echo help lacks keep_all, keep_all cases will skip with reason")
	}
	return keepAllOk
}

// qosDHProfileFromStrings builds a core profile with pinned reliable.
//
// Durability, history, and depth come from the case table. Reliability is
// fixed to reliable so durability alone decides compatibility.
func qosDHProfileFromStrings(durability, history, depth string) (core.QoSProfile, error) {
	var out core.QoSProfile
	out.Reliability = core.ReliabilityReliable
	switch durability {
	case "transient_local":
		out.Durability = core.DurabilityTransientLocal
	case "volatile":
		out.Durability = core.DurabilityVolatile
	default:
		return core.QoSProfile{}, fmt.Errorf("qos durability history: unknown durability %q", durability)
	}
	switch history {
	case "keep_last":
		out.History = core.HistoryKeepLast
	case "keep_all":
		out.History = core.HistoryKeepAll
	default:
		return core.QoSProfile{}, fmt.Errorf("qos durability history: unknown history %q", history)
	}
	parsed, err := strconv.ParseUint(depth, 10, 64)
	if err != nil {
		return core.QoSProfile{}, fmt.Errorf("qos durability history: parse depth %q: %w", depth, err)
	}
	out.Depth = parsed
	out.Liveliness = core.LivelinessAutomatic
	return out, nil
}

// qosDHResolveProfiles maps direction to offered plus requested plus names.
func qosDHResolveProfiles(c qosDHCase, rosProfile, facade core.QoSProfile) (core.QoSProfile, core.QoSProfile, string, string) {
	if c.direction == "forward" {
		return rosProfile, facade, "ros-pub", "weld-echo"
	}
	return facade, rosProfile, "weld-pub", "ros-echo"
}

// qosDHCheckCompatible asserts a compatible report has no errors.
func qosDHCheckCompatible(t *testing.T, c qosDHCase, report core.CompatibilityReport) {
	t.Helper()
	helpers.CheckQoSCompatible(t, c.name, report)
}

// qosDHCheckIncompatible asserts an incompatible report names durability loudly.
func qosDHCheckIncompatible(t *testing.T, c qosDHCase, report core.CompatibilityReport, offered, requested core.QoSProfile, publisher, subscriber string) {
	t.Helper()
	helpers.CheckQoSIncompatible(t, c.name, report, offered, requested, publisher, subscriber, c.wantPolicy, "qos durability history", 0, "")
}

// qosDHCheckCore asserts core compatibility matches the case expectation.
func qosDHCheckCore(t *testing.T, c qosDHCase) {
	t.Helper()
	rosProfile, err := qosDHProfileFromStrings(c.durability, c.history, c.depth)
	if err != nil {
		t.Fatalf("case %s: profile: %v", c.name, err)
	}
	facade := core.FacadeAdvertisedQoS()
	offered, requested, publisher, subscriber := qosDHResolveProfiles(c, rosProfile, facade)
	report := core.CheckCompatibility(offered, requested)
	if c.compatible {
		qosDHCheckCompatible(t, c, report)
		return
	}
	qosDHCheckIncompatible(t, c, report, offered, requested, publisher, subscriber)
}

// qosDHAssertCompatibleFlow fails when a compatible echo misses the payload.
//
// A clean weld echo with no backend error plus a missing live ID is a flow
// failure, not a skip. Only no-peer and unsupported flag cases skip.
func qosDHAssertCompatibleFlow(t *testing.T, c qosDHCase, stdout, stderr, live, payload string) {
	t.Helper()
	if !strings.Contains(stdout, live) {
		t.Fatalf("case %s: weld echo missed live payload %q, want flow for compatible pair\nstdout:\n%s\nstderr:\n%s", c.name, payload, stdout, stderr)
	}
}

// qosDHAssertIncompatibleSilent fails when an incompatible pair delivers.
//
// Callers pin core.CheckCompatibility incompatible first via qosDHCheckCore,
// so live delivery must be absent. Silence with a loud core error is the
// required result.
func qosDHAssertIncompatibleSilent(t *testing.T, c qosDHCase, stdout, live, payload string) {
	t.Helper()
	if strings.Contains(stdout, live) {
		t.Fatalf("case %s: incompatible pair delivered payload %q, want no flow (core.CheckCompatibility reports incompatible)", c.name, payload)
	}
	t.Logf("case %s: incompatible pair stayed silent on live ID as required, core error is loud above", c.name)
}

// qosDHAssertReverseDelivered fails when ROS echo misses the payload.
//
// A clean weld pub with sent confirmation plus a missing live ID is a flow
// failure, not a skip. Only no-peer and unsupported flag cases skip.
func qosDHAssertReverseDelivered(t *testing.T, c qosDHCase, echoOutput, live, payload string) {
	t.Helper()
	if !strings.Contains(echoOutput, live) {
		t.Fatalf("case %s: ros echo missed live payload %q, want flow for compatible pair\necho output:\n%s", c.name, payload, echoOutput)
	}
}

// qosDHArgv builds the QoS flags for ros2 pub and echo.
//
// Reliability is pinned to reliable so the durability and history matrix
// stays isolated. Durability, history, and depth come from the case.
func qosDHArgv(c qosDHCase) []string {
	return []string{"--qos-reliability", "reliable", "--qos-durability", c.durability, "--qos-history", c.history, "--qos-depth", c.depth}
}

// qosDHRunForward runs ROS pub with durability history depth flags.
//
// Domain is the allocated exclusive domain plumbed to DockerArgs and weld
// --domain flags, so parallel workers never share a domain.
//
// It starts ros2 topic pub with the probed flags, waits for settle, then
// checks weld echo output for the live ID. Compatible cases must deliver and
// a missing payload with a clean echo fails. Incompatible cases must stay
// silent on the live ID and delivery fails; core.CheckCompatibility pins the
// mismatch loudly first via qosDHCheckCore.
func qosDHRunForward(t *testing.T, ctx context.Context, bin string, domain int, c qosDHCase) {
	t.Helper()
	live := helpers.LiveID("qos-dh-" + c.name)
	topic := helpers.LiveTopic(live)
	payload := "hello qos dh " + live
	logPath := filepath.Join(t.TempDir(), "qos-dh-talker-"+live+".log")
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
	argv = append(argv, qosDHArgv(c)...)
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
		cleanupCtx, cleanupCancel := context.WithTimeout(context.WithoutCancel(ctx), qosDHCleanWait)
		defer cleanupCancel()
		helpers.KillContainerProc(cleanupCtx, helpers.Container, domain, live)
	})
	helpers.Sleep(ctx, qosDHForwardNap)
	err = ctx.Err()
	if err != nil {
		t.Fatalf("case %s: context ended before echo: %v", c.name, err)
	}
	stdout, stderr, err := helpers.RunWeldEchoWithDurability(ctx, bin, topic, domain, qosDHEchoWait, "transient_local")
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
		qosDHAssertCompatibleFlow(t, c, stdout, stderr, live, payload)
		return
	}
	qosDHAssertIncompatibleSilent(t, c, stdout, live, payload)
}

// qosDHRunReverse runs ROS echo with durability history depth flags.
//
// Domain is the allocated exclusive domain plumbed to DockerArgs and weld
// --domain flags, so parallel workers never share a domain.
//
// It starts ros2 topic echo with the probed flags, waits for settle, then
// publishes one weld message and polls the echo log for the live ID.
// Compatible cases must deliver and a missing payload fails. This path is
// only used for compatible cases because facade transient_local satisfies
// every reverse request.
func qosDHRunReverse(t *testing.T, ctx context.Context, bin string, domain int, c qosDHCase) {
	t.Helper()
	live := helpers.LiveID("qos-dh-" + c.name)
	topic := helpers.LiveTopic(live)
	payload := "hello qos dh reverse " + live
	echoPath := filepath.Join(t.TempDir(), "qos-dh-rosecho-"+live+".log")
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
	argv = append(argv, qosDHArgv(c)...)
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
		cleanupCtx, cleanupCancel := context.WithTimeout(context.WithoutCancel(ctx), qosDHCleanWait)
		defer cleanupCancel()
		helpers.KillContainerProc(cleanupCtx, helpers.Container, domain, topic)
	})
	helpers.Sleep(ctx, qosDHReverseNap)
	err = ctx.Err()
	if err != nil {
		t.Fatalf("case %s: context ended before pub: %v", c.name, err)
	}
	found, stdout, stderr, err := helpers.PublishUntilSeen(ctx, bin, topic, domain, payload, echoPath, qosDHPollWait, 3)
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
		qosDHAssertReverseDelivered(t, c, string(data), live, payload)
	}
}

// qosDHRunCase checks core loudly then runs the live direction.
//
// It skips keep_all live cases with a reason when the peer help probe shows
// keep_all unsupported. Core truth for keep_all is still pinned by the
// core-only test which needs no peer flags.
func qosDHRunCase(t *testing.T, ctx context.Context, bin string, domain int, c qosDHCase, keepAllOk bool) {
	t.Helper()
	qosDHCheckCore(t, c)
	if c.history == "keep_all" && !keepAllOk {
		t.Skipf("case %s: peer lacks keep_all value, skipping live flow with reason", c.name)
	}
	if c.direction == "forward" {
		qosDHRunForward(t, ctx, bin, domain, c)
		return
	}
	qosDHRunReverse(t, ctx, bin, domain, c)
}

// checkDHCompatibleTable pins all compatible cases stay compatible.
func checkDHCompatibleTable(t *testing.T, facade core.QoSProfile) {
	t.Helper()
	for _, c := range qosDHCompatibleCases() {
		rosProfile, err := qosDHProfileFromStrings(c.durability, c.history, c.depth)
		if err != nil {
			t.Fatalf("case %s: profile: %v", c.name, err)
		}
		offered, requested, _, _ := qosDHResolveProfiles(c, rosProfile, facade)
		report := core.CheckCompatibility(offered, requested)
		if !report.Compatible {
			t.Fatalf("case %s: want compatible, got %s", c.name, report.ErrorText())
		}
	}
}

// checkOneDHIncompatible pins one incompatible case stays incompatible.
func checkOneDHIncompatible(t *testing.T, c qosDHCase, facade core.QoSProfile) {
	t.Helper()
	rosProfile, err := qosDHProfileFromStrings(c.durability, c.history, c.depth)
	if err != nil {
		t.Fatalf("case %s: profile: %v", c.name, err)
	}
	offered, requested, publisher, subscriber := qosDHResolveProfiles(c, rosProfile, facade)
	report := core.CheckCompatibility(offered, requested)
	if report.Compatible {
		t.Fatalf("case %s: want incompatible, got compatible", c.name)
	}
	if len(report.Errors) == 0 {
		t.Fatalf("case %s: incompatible with no explicit errors", c.name)
	}
	found := false
	for _, issue := range report.Errors {
		if issue.Kind == c.wantPolicy {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("case %s: errors %s miss policy %s", c.name, report.ErrorText(), string(c.wantPolicy))
	}
	events := core.CompatibilityEvents("/chatter", publisher, subscriber, offered, requested)
	if len(events) == 0 {
		t.Fatalf("case %s: no loud events for incompatible pair", c.name)
	}
}

// checkDHIncompatibleTable pins all incompatible cases stay incompatible.
func checkDHIncompatibleTable(t *testing.T, facade core.QoSProfile) {
	t.Helper()
	for _, c := range qosDHIncompatibleCases() {
		checkOneDHIncompatible(t, c, facade)
	}
}

// TestROSQoSDurabilityHistoryCore pins the durability history truth table.
func TestROSQoSDurabilityHistoryCore(t *testing.T) {
	t.Parallel()
	facade := core.FacadeAdvertisedQoS()
	checkDHCompatibleTable(t, facade)
	checkDHIncompatibleTable(t, facade)
}

// TestROSQoSDurabilityHistoryCompatibleDelivers checks compatible pairs flow.
//
// It parametrizes ros2 topic pub and echo with --qos-durability,
// --qos-history, and --qos-depth on the allocated exclusive domain with the Cyclone RMW in
// weld-jazzy. Each subtest uses its own live ID and topic with context
// timeouts and docker argv only. It skips cleanly only when no peer is
// present or when the flags are unsupported. A missing payload with a clean
// echo is a failure. Keep_all cases skip with reason when the peer help
// lacks the keep_all value.
func TestROSQoSDurabilityHistoryCompatibleDelivers(t *testing.T) {
	t.Parallel()
	domain := helpers.MustAllocateDomain(t)
	ctx, cancel := context.WithTimeout(context.Background(), qosDHTestTimeout)
	defer cancel()
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)
	keepAllOk := qosDHRequireFlags(t, ctx, domain)
	bin := helpers.BuildWeld(t, ctx)
	// The outer domain served only the flag probe. Release it before the
	// subtests so concurrent workers share the 32-domain pool instead of
	// each holding two domains for the whole test.
	helpers.ReleaseDomain(domain)
	for _, c := range qosDHCompatibleCases() {
		t.Run(c.name, func(t *testing.T) {
			subdomain := helpers.MustAllocateDomain(t)
			qosDHRunCase(t, ctx, bin, subdomain, c, keepAllOk)
		})
	}
}

// TestROSQoSDurabilityHistoryIncompatibleLoud checks volatile forward fails.
//
// Volatile offered against the facade transient_local request must return an
// explicit durability error through core.CheckCompatibility and must never
// deliver silently live. Each subtest logs the mismatch with topic, endpoint
// pair, and policy kind so silence is a failure. It skips cleanly only when
// no peer is present or when the QoS flags are unsupported. Keep_all cases
// skip with reason when the peer help lacks the keep_all value.
func TestROSQoSDurabilityHistoryIncompatibleLoud(t *testing.T) {
	t.Parallel()
	domain := helpers.MustAllocateDomain(t)
	ctx, cancel := context.WithTimeout(context.Background(), qosDHTestTimeout)
	defer cancel()
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)
	keepAllOk := qosDHRequireFlags(t, ctx, domain)
	bin := helpers.BuildWeld(t, ctx)
	// The outer domain served only the flag probe. Release it before the
	// subtests so concurrent workers share the 32-domain pool instead of
	// each holding two domains for the whole test.
	helpers.ReleaseDomain(domain)
	for _, c := range qosDHIncompatibleCases() {
		t.Run(c.name, func(t *testing.T) {
			subdomain := helpers.MustAllocateDomain(t)
			subCtx, subCancel := context.WithTimeout(ctx, qosDHMismatchDur+2*qosDHEchoWait)
			defer subCancel()
			qosDHRunCase(t, subCtx, bin, subdomain, c, keepAllOk)
		})
	}
}
