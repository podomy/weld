// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package ros_test

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/podomy/weld/internal/core"
	"github.com/podomy/weld/testbed/live/helpers"
)

// Full QoS matrix constants for Jazzy canonical.
//
// Each subtest allocates one exclusive domain from the parallel pool. The container name and
// Cyclone RMW come from helpers so the gate stays on weld-jazzy with
// RMW_IMPLEMENTATION=rmw_cyclonedds_cpp. Only one run per domain and topic
// at a time is supported, so tests never run in parallel and every payload
// carries a per-run live ID. Humble and Rolling already cover default QoS
// in the distro matrix, so this file pins only the full flag matrix on
// Jazzy with explicit --qos-reliability, --qos-durability, --qos-history,
// and --qos-depth flags.
const (
	qosFullEchoWait     = 10 * time.Second
	qosFullPollWait     = 20 * time.Second
	qosFullForwardNap   = 5 * time.Second
	qosFullReverseNap   = 6 * time.Second
	qosFullTestTimeout  = 30 * time.Minute
	qosFullProbeWait    = 15 * time.Second
	qosFullCleanWait    = 10 * time.Second
	qosFullMismatchWait = 8 * time.Second
)

// qosFullCase describes one live QoS combination.
//
// Reliability, durability, history, and depth map directly to the ros2 topic
// pub and echo flags of the same names. Direction is either forward for ROS
// pub to weld echo or reverse for weld pub to ROS echo.
type qosFullCase struct {
	// name scopes the subtest and the live ID prefix.
	name string
	// direction is forward or reverse.
	direction string
	// reliability is the --qos-reliability flag value.
	reliability string
	// durability is the --qos-durability flag value.
	durability string
	// history is the --qos-history flag value.
	history string
	// depth is the queue size string passed to --qos-depth.
	depth string
}

// qosFullAllReliabilityValues returns every reliability value probed in help.
//
// It mirrors ros2 topic pub --help on Jazzy: system_default, reliable,
// best_effort, unknown, and best_available.
func qosFullAllReliabilityValues() []string {
	return []string{"system_default", "reliable", "best_effort", "unknown", "best_available"}
}

// qosFullAllDurabilityValues returns every durability value probed in help.
//
// It mirrors ros2 topic pub --help on Jazzy: system_default,
// transient_local, volatile, unknown, and best_available.
func qosFullAllDurabilityValues() []string {
	return []string{"system_default", "transient_local", "volatile", "unknown", "best_available"}
}

// qosFullAllHistoryValues returns every history value probed in help.
//
// It mirrors ros2 topic pub --help on Jazzy: system_default, keep_last,
// keep_all, and unknown. History has no best_available in the CLI, but the
// list includes best_available as a candidate so the probe can report it as
// unsupported with a reason.
func qosFullAllHistoryValues() []string {
	return []string{"system_default", "keep_last", "keep_all", "unknown", "best_available"}
}

// qosFullAllDepths returns representative depths for the unbounded depth flag.
//
// Depth is an arbitrary queue size N, so the matrix samples 1, 10, and 100
// to prove depth never affects matching.
func qosFullAllDepths() []string {
	return []string{"1", "10", "100"}
}

// qosFullLiveReliabilities returns concrete reliabilities pinned live.
//
// Markers need endpoint resolution and stay core only, so live flow pins
// only reliable and best_effort.
func qosFullLiveReliabilities() []string {
	return []string{"reliable", "best_effort"}
}

// qosFullLiveDurabilities returns concrete durabilities pinned live.
//
// Markers need endpoint resolution and stay core only, so live flow pins
// only transient_local and volatile.
func qosFullLiveDurabilities() []string {
	return []string{"transient_local", "volatile"}
}

// qosFullLiveHistories returns concrete histories pinned live.
//
// Markers stay core only, so live flow pins only keep_last and keep_all.
func qosFullLiveHistories() []string {
	return []string{"keep_last", "keep_all"}
}

// qosFullGenerateFullCases returns the full cross product for core truth.
//
// It crosses every reliability by every durability by every history by
// every sampled depth in both directions. History best_available has no
// core mapping and is reported as unsupported by callers with a reason.
func qosFullGenerateFullCases() []qosFullCase {
	var out []qosFullCase
	directions := []string{"forward", "reverse"}
	rels := qosFullAllReliabilityValues()
	durs := qosFullAllDurabilityValues()
	hists := qosFullAllHistoryValues()
	depths := qosFullAllDepths()
	for _, direction := range directions {
		for _, rel := range rels {
			for _, dur := range durs {
				for _, hist := range hists {
					for _, depth := range depths {
						safe := strings.ReplaceAll(rel, "-", "_")
						safeDur := strings.ReplaceAll(dur, "-", "_")
						safeHist := strings.ReplaceAll(hist, "-", "_")
						name := fmt.Sprintf("%s_%s_%s_%s_d%s", direction, safe, safeDur, safeHist, depth)
						out = append(out, qosFullCase{
							name:        name,
							direction:   direction,
							reliability: rel,
							durability:  dur,
							history:     hist,
							depth:       depth,
						})
					}
				}
			}
		}
	}
	return out
}

// qosFullLiveCases returns every concrete combination pinned live.
//
// It crosses reliable and best_effort by transient_local and volatile by
// keep_last and keep_all by depths 1, 10, and 100 in both directions.
// Markers are excluded here and stay core only with a skip reason live.
func qosFullLiveCases() []qosFullCase {
	var out []qosFullCase
	directions := []string{"forward", "reverse"}
	for _, direction := range directions {
		for _, rel := range qosFullLiveReliabilities() {
			for _, dur := range qosFullLiveDurabilities() {
				for _, hist := range qosFullLiveHistories() {
					for _, depth := range qosFullAllDepths() {
						name := fmt.Sprintf("%s_%s_%s_%s_d%s", direction, rel, dur, hist, depth)
						out = append(out, qosFullCase{
							name:        name,
							direction:   direction,
							reliability: rel,
							durability:  dur,
							history:     hist,
							depth:       depth,
						})
					}
				}
			}
		}
	}
	return out
}

// qosFullIsConcreteValue reports whether a flag value is concrete for live.
//
// Concrete means the CLI value maps to a fixed core policy without endpoint
// resolution. Markers return false so callers skip live with a reason.
func qosFullIsConcreteValue(value string) bool {
	switch value {
	case "reliable", "best_effort", "transient_local", "volatile", "keep_last", "keep_all":
		return true
	default:
		return false
	}
}

// qosFullIsConcreteCase reports whether a case uses only concrete values.
//
// Live flow pins only concrete cases. Any marker makes the case core only
// with a skip reason live.
func qosFullIsConcreteCase(c qosFullCase) bool {
	if !qosFullIsConcreteValue(c.reliability) {
		return false
	}
	if !qosFullIsConcreteValue(c.durability) {
		return false
	}
	if !qosFullIsConcreteValue(c.history) {
		return false
	}
	return true
}

// qosFullProbeHelp runs one ros2 help argv and returns combined output.
func qosFullProbeHelp(ctx context.Context, argv []string) (string, error) {
	probeCtx, cancel := context.WithTimeout(ctx, qosFullProbeWait)
	defer cancel()
	cmd := exec.CommandContext(probeCtx, "docker", argv...) //nolint:gosec // live harness probes fixed ros2 help argv with no shell
	out, err := cmd.CombinedOutput()
	if err != nil {
		return string(out), fmt.Errorf("qos full: help probe: %w", err)
	}
	return string(out), nil
}

// qosFullExtractValues parses allowed values for one flag from help output.
//
// It finds the flag token, then reads the brace list that follows it, and
// returns the comma separated entries trimmed of spaces. It returns nil
// when the flag or brace list is absent.
func qosFullExtractValues(help, flag string) []string {
	idx := strings.Index(help, flag)
	if idx < 0 {
		return nil
	}
	rest := help[idx:]
	open := strings.Index(rest, "{")
	if open < 0 {
		return nil
	}
	after := rest[open:]
	closeIdx := strings.Index(after, "}")
	if closeIdx < 0 {
		return nil
	}
	inner := after[1:closeIdx]
	parts := strings.Split(inner, ",")
	var out []string
	for _, part := range parts {
		trimmed := strings.TrimSpace(part)
		if trimmed == "" {
			continue
		}
		out = append(out, trimmed)
	}
	return out
}

// qosFullContainsValue reports whether a probed list holds a flag value.
func qosFullContainsValue(values []string, want string) bool {
	return slices.Contains(values, want)
}

// qosFullProbeQoS probes pub and echo help for QoS flags and values.
//
// It returns the probed reliability, durability, and history value lists
// plus depthOk true when both sides list --qos-depth. Callers skip live
// cases whose values are absent from these lists with a reason.
func qosFullProbeQoS(ctx context.Context, domain int) ([]string, []string, []string, bool, error) {
	pubArgv := helpers.DockerArgs(helpers.Container, domain, helpers.RosBin, "topic", "pub", "--help")
	pubOut, err := qosFullProbeHelp(ctx, pubArgv)
	if err != nil {
		return nil, nil, nil, false, err
	}
	echoArgv := helpers.DockerArgs(helpers.Container, domain, helpers.RosBin, "topic", "echo", "--help")
	echoOut, err := qosFullProbeHelp(ctx, echoArgv)
	if err != nil {
		return nil, nil, nil, false, err
	}
	for _, flag := range []string{"--qos-reliability", "--qos-durability", "--qos-history", "--qos-depth"} {
		if !strings.Contains(pubOut, flag) {
			return nil, nil, nil, false, fmt.Errorf("qos full: pub help lacks %s", flag)
		}
		if !strings.Contains(echoOut, flag) {
			return nil, nil, nil, false, fmt.Errorf("qos full: echo help lacks %s", flag)
		}
	}
	pubRel := qosFullExtractValues(pubOut, "--qos-reliability")
	pubDur := qosFullExtractValues(pubOut, "--qos-durability")
	pubHist := qosFullExtractValues(pubOut, "--qos-history")
	echoRel := qosFullExtractValues(echoOut, "--qos-reliability")
	echoDur := qosFullExtractValues(echoOut, "--qos-durability")
	echoHist := qosFullExtractValues(echoOut, "--qos-history")
	relVals := pubRel
	if len(echoRel) < len(relVals) {
		relVals = echoRel
	}
	durVals := pubDur
	if len(echoDur) < len(durVals) {
		durVals = echoDur
	}
	histVals := pubHist
	if len(echoHist) < len(histVals) {
		histVals = echoHist
	}
	_ = echoRel
	_ = echoDur
	_ = echoHist
	depthOk := strings.Contains(pubOut, "--qos-depth") && strings.Contains(echoOut, "--qos-depth")
	return relVals, durVals, histVals, depthOk, nil
}

// qosFullRequireProbe skips the test when the peer lacks QoS flags.
//
// It returns the probed value lists and depth support for per-case skip
// decisions. Probe failures skip with the probe error as the reason.
func qosFullRequireProbe(t *testing.T, ctx context.Context, domain int) ([]string, []string, []string, bool) {
	t.Helper()
	relVals, durVals, histVals, depthOk, err := qosFullProbeQoS(ctx, domain)
	if err != nil {
		t.Skipf("qos full flag probe failed, skipping matrix: %v", err)
	}
	if !depthOk {
		t.Skipf("ros2 topic pub or echo lacks --qos-depth, skipping full matrix")
	}
	t.Logf("qos full probed reliability=%v durability=%v history=%v", relVals, durVals, histVals)
	if !qosFullContainsValue(histVals, "best_available") {
		t.Logf("peer help lacks history best_available, those candidates skip with reason")
	}
	return relVals, durVals, histVals, depthOk
}

// qosFullCaseSupported reports whether a case is supported by probed values.
//
// It returns false with a reason when any of the case values is absent from
// the probed help lists, so callers skip with that reason.
func qosFullCaseSupported(c qosFullCase, relVals, durVals, histVals []string) (bool, string) {
	if !qosFullContainsValue(relVals, c.reliability) {
		return false, fmt.Sprintf("reliability %q absent from peer help", c.reliability)
	}
	if !qosFullContainsValue(durVals, c.durability) {
		return false, fmt.Sprintf("durability %q absent from peer help", c.durability)
	}
	if !qosFullContainsValue(histVals, c.history) {
		return false, fmt.Sprintf("history %q absent from peer help", c.history)
	}
	return true, ""
}

// qosFullParseReliability maps a CLI reliability value to core policy.
func qosFullParseReliability(value string) (core.ReliabilityPolicy, error) {
	switch value {
	case "reliable":
		return core.ReliabilityReliable, nil
	case "best_effort":
		return core.ReliabilityBestEffort, nil
	case "system_default":
		return core.ReliabilitySystemDefault, nil
	case "unknown":
		return core.ReliabilityUnknown, nil
	case "best_available":
		return core.ReliabilityBestAvailable, nil
	default:
		return 0, fmt.Errorf("qos full: unknown reliability %q", value)
	}
}

// qosFullParseDurability maps a CLI durability value to core policy.
func qosFullParseDurability(value string) (core.DurabilityPolicy, error) {
	switch value {
	case "transient_local":
		return core.DurabilityTransientLocal, nil
	case "volatile":
		return core.DurabilityVolatile, nil
	case "system_default":
		return core.DurabilitySystemDefault, nil
	case "unknown":
		return core.DurabilityUnknown, nil
	case "best_available":
		return core.DurabilityBestAvailable, nil
	default:
		return 0, fmt.Errorf("qos full: unknown durability %q", value)
	}
}

// qosFullParseHistory maps a CLI history value to core policy.
func qosFullParseHistory(value string) (core.HistoryPolicy, error) {
	switch value {
	case "keep_last":
		return core.HistoryKeepLast, nil
	case "keep_all":
		return core.HistoryKeepAll, nil
	case "system_default":
		return core.HistorySystemDefault, nil
	case "unknown":
		return core.HistoryUnknown, nil
	default:
		return 0, fmt.Errorf("qos full: unsupported history %q", value)
	}
}

// qosFullParseDepth maps a CLI depth string to a queue size.
func qosFullParseDepth(depth string) (uint64, error) {
	parsed, err := strconv.ParseUint(depth, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("qos full: parse depth %q: %w", depth, err)
	}
	return parsed, nil
}

// qosFullProfileFromStrings builds a core profile from ROS flag values.
//
// It maps every probed CLI value including system_default, unknown, and
// best_available to the matching core marker. History best_available has no
// core policy and returns an error so callers skip with a reason.
func qosFullProfileFromStrings(reliability, durability, history, depth string) (core.QoSProfile, error) {
	var out core.QoSProfile
	rel, err := qosFullParseReliability(reliability)
	if err != nil {
		return core.QoSProfile{}, err
	}
	out.Reliability = rel
	dur, err := qosFullParseDurability(durability)
	if err != nil {
		return core.QoSProfile{}, err
	}
	out.Durability = dur
	hist, err := qosFullParseHistory(history)
	if err != nil {
		return core.QoSProfile{}, err
	}
	out.History = hist
	parsed, err := qosFullParseDepth(depth)
	if err != nil {
		return core.QoSProfile{}, err
	}
	out.Depth = parsed
	out.Liveliness = core.LivelinessAutomatic
	return out, nil
}

// qosFullBuildReport maps a case to offered plus requested plus the report.
//
// Forward uses the ROS profile as offered and the facade as requested with
// ros-pub and weld-echo endpoints. Reverse swaps them with weld-pub and
// ros-echo endpoints. Mapping errors return loudly so callers skip with a
// reason instead of flowing silently.
func qosFullBuildReport(c qosFullCase, facade core.QoSProfile) (core.QoSProfile, core.QoSProfile, string, string, core.CompatibilityReport, error) {
	rosProfile, err := qosFullProfileFromStrings(c.reliability, c.durability, c.history, c.depth)
	if err != nil {
		return core.QoSProfile{}, core.QoSProfile{}, "", "", core.CompatibilityReport{}, err
	}
	offered, requested, publisher, subscriber := helpers.ResolveQoSDirection(c.direction, rosProfile, facade)
	report := core.CheckCompatibility(offered, requested)
	return offered, requested, publisher, subscriber, report, nil
}

// qosFullWantCompatible reports the expected RxO verdict for a case.
//
// Forward is compatible only when the ROS offer avoids both concrete
// mismatches: best_effort against the facade reliable request and volatile
// against the facade transient_local request. Markers never trigger errors,
// so they stay compatible. Reverse always flows because the facade offers
// reliable plus transient_local to every ROS request.
func qosFullWantCompatible(c qosFullCase) bool {
	if c.direction == "reverse" {
		return true
	}
	if c.reliability == "best_effort" {
		return false
	}
	if c.durability == "volatile" {
		return false
	}
	return true
}

// qosFullWantPolicies returns the expected mismatch policies for a case.
//
// Forward best_effort expects a reliability error and forward volatile
// expects a durability error. A forward best_effort plus volatile pair
// expects both. Reverse and compatible cases expect none.
func qosFullWantPolicies(c qosFullCase) []core.PolicyKind {
	if c.direction == "reverse" {
		return nil
	}
	var out []core.PolicyKind
	if c.reliability == "best_effort" {
		out = append(out, core.PolicyReliability)
	}
	if c.durability == "volatile" {
		out = append(out, core.PolicyDurability)
	}
	return out
}

// qosFullAssertCore asserts core compatibility matches the RxO expectation.
//
// Compatible cases must have no errors. Incompatible cases must carry every
// expected policy error plus at least one loud CompatibilityEvents entry so
// silence is a failure. Events are logged with topic, endpoint pair, and
// policy kind.
func qosFullAssertCore(t *testing.T, c qosFullCase, offered, requested core.QoSProfile, publisher, subscriber string, report core.CompatibilityReport) {
	t.Helper()
	want := qosFullWantCompatible(c)
	if report.Compatible != want {
		t.Fatalf("case %s: compatible = %v, want %v errors %s", c.name, report.Compatible, want, report.ErrorText())
	}
	if want {
		helpers.CheckQoSCompatible(t, c.name, report)
		return
	}
	wantPolicies := qosFullWantPolicies(c)
	if len(report.Errors) == 0 {
		t.Fatalf("case %s: incompatible with no errors, want %v", c.name, wantPolicies)
	}
	for _, kind := range wantPolicies {
		found := false
		for _, issue := range report.Errors {
			if issue.Kind == kind {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("case %s: errors %s miss policy %s", c.name, report.ErrorText(), string(kind))
		}
	}
	events := core.CompatibilityEvents("/chatter", publisher, subscriber, offered, requested)
	if len(events) == 0 {
		t.Fatalf("case %s: no loud events for incompatible pair", c.name)
	}
	for _, ev := range events {
		t.Logf("qos full mismatch topic=%s publisher=%s subscriber=%s policy=%s offered=%s requested=%s: %s", ev.Topic, ev.Publisher, ev.Subscriber, string(ev.Policy), ev.Offered, ev.Requested, ev.Detail)
	}
}

// qosFullArgv builds the four QoS flags for ros2 pub and echo.
func qosFullArgv(c qosFullCase) []string {
	return []string{"--qos-reliability", c.reliability, "--qos-durability", c.durability, "--qos-history", c.history, "--qos-depth", c.depth}
}

// qosFullRunForward runs ROS pub with QoS flags and checks weld echo output.
//
// Compatible cases must deliver the live ID; a miss with ok echo output
// fails. Incompatible cases must stay silent on the live ID because the loud
// core error above already proves the mismatch.
func qosFullRunForward(t *testing.T, ctx context.Context, bin string, domain int, c qosFullCase, compatible bool) {
	t.Helper()
	live := helpers.LiveID("qos-full-" + c.name)
	topic := helpers.LiveTopic(live)
	payload := "hello qos full " + live
	logPath := filepath.Join(t.TempDir(), "qos-full-talker-"+live+".log")
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
	argv = append(argv, qosFullArgv(c)...)
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
		cleanupCtx, cleanupCancel := context.WithTimeout(context.WithoutCancel(ctx), qosFullCleanWait)
		defer cleanupCancel()
		if err := helpers.KillAndWaitContainerProc(cleanupCtx, helpers.Container, live); err != nil {
			t.Errorf("stop peer proc: %v", err)
		}
	})
	helpers.Sleep(ctx, qosFullForwardNap)
	err = ctx.Err()
	if err != nil {
		t.Fatalf("case %s: context ended before echo: %v", c.name, err)
	}
	stdout, stderr, err := helpers.RunWeldEchoWithDurability(ctx, bin, topic, domain, qosFullEchoWait, "transient_local")
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
	if compatible {
		if !strings.Contains(stdout, live) {
			t.Fatalf("case %s: weld echo missed live payload %q\nstdout:\n%s\nstderr:\n%s", c.name, payload, stdout, stderr)
		}
		return
	}
	requireTalkerAliveForSilence(t, ctx, domain, topic, live, c.name, talker, "qos-full")
	if strings.Contains(stdout, live) {
		t.Fatalf("case %s: incompatible pair delivered payload %q, want no flow", c.name, payload)
	}
	t.Logf("case %s: incompatible pair stayed silent on live ID as required, core error is loud above", c.name)
}

// qosFullRunReverse runs ROS echo with QoS flags and checks weld pub delivery.
//
// Compatible cases must deliver the live ID; a miss with ok pub output
// fails. Reverse concrete cases are always compatible because the facade
// offers reliable plus transient_local, so this path never expects silence.
func qosFullRunReverse(t *testing.T, ctx context.Context, bin string, domain int, c qosFullCase, compatible bool) {
	t.Helper()
	live := helpers.LiveID("qos-full-" + c.name)
	topic := helpers.LiveTopic(live)
	payload := "hello qos full reverse " + live
	echoPath := filepath.Join(t.TempDir(), "qos-full-rosecho-"+live+".log")
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
	argv = append(argv, qosFullArgv(c)...)
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
		cleanupCtx, cleanupCancel := context.WithTimeout(context.WithoutCancel(ctx), qosFullCleanWait)
		defer cleanupCancel()
		if err := helpers.KillAndWaitContainerProc(cleanupCtx, helpers.Container, topic); err != nil {
			t.Errorf("stop peer proc: %v", err)
		}
	})
	helpers.Sleep(ctx, qosFullReverseNap)
	err = ctx.Err()
	if err != nil {
		t.Fatalf("case %s: context ended before pub: %v", c.name, err)
	}
	found, stdout, stderr, err := helpers.PublishUntilSeen(ctx, bin, topic, domain, payload, echoPath, qosFullPollWait, 3)
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
		if !compatible {
			t.Fatalf("case %s: incompatible reverse missed payload but reverse must flow, want loud failure not silence\necho output:\n%s", c.name, string(data))
		}
		t.Fatalf("case %s: ros echo missed live payload %q\necho output:\n%s", c.name, payload, string(data))
	}
	if !compatible {
		t.Fatalf("case %s: incompatible reverse delivered payload %q, want loud mismatch", c.name, payload)
	}
}

// qosFullRunLiveCase checks core loudly then runs the live direction.
//
// Marker cases skip live with a reason because their values need endpoint
// resolution. Unsupported probed values skip live with the probe reason.
// Core truth for skipped cases stays pinned by TestROSQoSFullCore.
func qosFullRunLiveCase(t *testing.T, ctx context.Context, bin string, domain int, c qosFullCase, relVals, durVals, histVals []string) {
	t.Helper()
	facade := core.FacadeAdvertisedQoS()
	offered, requested, publisher, subscriber, report, err := qosFullBuildReport(c, facade)
	if err != nil {
		t.Skipf("case %s: profile mapping unsupported, skipping live with reason: %v", c.name, err)
	}
	qosFullAssertCore(t, c, offered, requested, publisher, subscriber, report)
	ok, reason := qosFullCaseSupported(c, relVals, durVals, histVals)
	if !ok {
		t.Skipf("case %s: peer lacks flag value, skipping live with reason: %s", c.name, reason)
	}
	if !qosFullIsConcreteCase(c) {
		t.Skipf("case %s: marker value needs resolution, skipping live with reason (core truth pinned above)", c.name)
	}
	compatible := report.Compatible
	if c.direction == "forward" {
		qosFullRunForward(t, ctx, bin, domain, c, compatible)
		return
	}
	qosFullRunReverse(t, ctx, bin, domain, c, compatible)
}

// qosFullCheckHistoryDepthInvariant pins history and depth as non matching.
//
// For each reliability by durability by direction group, every history and
// depth variation must yield the same Compatible verdict. History, depth,
// lifespan, and the avoidance flag never affect matching.
func qosFullCheckHistoryDepthInvariant(t *testing.T, facade core.QoSProfile) {
	t.Helper()
	type groupKey struct {
		reliability string
		durability  string
		direction   string
	}
	seen := make(map[groupKey]bool)
	want := make(map[groupKey]bool)
	for _, c := range qosFullGenerateFullCases() {
		if c.history == "best_available" {
			continue
		}
		_, _, _, _, report, err := qosFullBuildReport(c, facade)
		if err != nil {
			t.Fatalf("case %s: profile: %v", c.name, err)
		}
		key := groupKey{reliability: c.reliability, durability: c.durability, direction: c.direction}
		first, ok := seen[key]
		if !ok {
			seen[key] = true
			want[key] = report.Compatible
			_ = first
			continue
		}
		if want[key] != report.Compatible {
			t.Fatalf("group %v: history %s depth %s changed verdict to %v, want %v (history and depth must not affect matching)", key, c.history, c.depth, report.Compatible, want[key])
		}
	}
}

// TestROSQoSFullCore pins the full RxO truth table for all probed values.
//
// It crosses every probed reliability by durability by history by sampled
// depth in both directions against the facade. Forward is compatible only
// for reliable plus transient_local offers. Reverse always flows. History
// best_available has no core mapping and skips with a reason. History and
// depth invariance is pinned separately so they never affect matching.
func TestROSQoSFullCore(t *testing.T) {
	t.Parallel()
	facade := core.FacadeAdvertisedQoS()
	for _, c := range qosFullGenerateFullCases() {
		t.Run(c.name, func(t *testing.T) {
			offered, requested, publisher, subscriber, report, err := qosFullBuildReport(c, facade)
			if err != nil {
				t.Skipf("case %s: profile mapping unsupported, skipping with reason: %v", c.name, err)
			}
			qosFullAssertCore(t, c, offered, requested, publisher, subscriber, report)
		})
	}
	qosFullCheckHistoryDepthInvariant(t, facade)
}

// TestROSQoSFullCompatibleDelivers checks compatible concrete pairs deliver live.
//
// It parametrizes ros2 topic pub and echo with --qos-reliability,
// --qos-durability, --qos-history, and --qos-depth on the allocated exclusive domain with the
// Cyclone RMW in weld-jazzy. Each subtest uses its own live ID and topic
// with context timeouts and docker argv only. Marker and unsupported values
// skip with a reason while core truth stays pinned above. A missing payload
// with ok echo output fails. It skips cleanly only when no peer is present
// or when the flags are unsupported.
func TestROSQoSFullCompatibleDelivers(t *testing.T) {
	t.Parallel()
	domain, token := helpers.MustAllocateDomainLease(t)
	ctx, cancel := context.WithTimeout(context.Background(), qosFullTestTimeout)
	defer cancel()
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)
	relVals, durVals, histVals, _ := qosFullRequireProbe(t, ctx, domain)
	bin := helpers.BuildWeld(t, ctx)
	// The outer domain served only the flag probe. Release it before the
	// subtests so concurrent workers share the 32-domain pool instead of
	// each holding two domains for the whole test.
	helpers.ReleaseDomainLease(domain, token)
	var selected []qosFullCase
	for _, c := range qosFullLiveCases() {
		facade := core.FacadeAdvertisedQoS()
		_, _, _, _, report, err := qosFullBuildReport(c, facade)
		if err != nil {
			continue
		}
		if !report.Compatible {
			continue
		}
		selected = append(selected, c)
	}
	if len(selected) == 0 {
		t.Fatalf("qos full compatible selected 0 cases, want >0")
	}
	for _, c := range selected {
		t.Run(c.name, func(t *testing.T) {
			subdomain := helpers.MustAllocateDomain(t)
			qosFullRunLiveCase(t, ctx, bin, subdomain, c, relVals, durVals, histVals)
		})
	}
}

// TestROSQoSFullIncompatibleLoud checks incompatible concrete pairs fail loudly.
//
// Best_effort offered against the facade reliable request and volatile
// offered against the facade transient_local request must each return an
// explicit error through core with loud CompatibilityEvents and must never
// deliver silently live. Each subtest logs the mismatch with topic, endpoint
// pair, and policy kind so silence is a failure. Marker and unsupported
// values skip with a reason while core truth stays pinned above. It skips
// cleanly when no peer is present or when the QoS flags are unsupported.
func TestROSQoSFullIncompatibleLoud(t *testing.T) {
	t.Parallel()
	domain, token := helpers.MustAllocateDomainLease(t)
	ctx, cancel := context.WithTimeout(context.Background(), qosFullTestTimeout)
	defer cancel()
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)
	relVals, durVals, histVals, _ := qosFullRequireProbe(t, ctx, domain)
	bin := helpers.BuildWeld(t, ctx)
	// The outer domain served only the flag probe. Release it before the
	// subtests so concurrent workers share the 32-domain pool instead of
	// each holding two domains for the whole test.
	helpers.ReleaseDomainLease(domain, token)
	var selected []qosFullCase
	for _, c := range qosFullLiveCases() {
		facade := core.FacadeAdvertisedQoS()
		_, _, _, _, report, err := qosFullBuildReport(c, facade)
		if err != nil {
			continue
		}
		if report.Compatible {
			continue
		}
		selected = append(selected, c)
	}
	if len(selected) == 0 {
		t.Fatalf("qos full incompatible selected 0 cases, want >0")
	}
	for _, c := range selected {
		t.Run(c.name, func(t *testing.T) {
			subdomain := helpers.MustAllocateDomain(t)
			subCtx, subCancel := context.WithTimeout(ctx, qosFullMismatchWait+2*qosFullEchoWait)
			defer subCancel()
			qosFullRunLiveCase(t, subCtx, bin, subdomain, c, relVals, durVals, histVals)
		})
	}
}
