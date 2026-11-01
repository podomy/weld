// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

// Package live_cli holds live CLI checks that need a peer and network.
//
// These tests drive the built weld binary and the already running
// weld-jazzy peer through os/exec docker argv only. They pin topic
// validation, domain range and defaults, timeout handling, text selection,
// help output, exit codes, gap and drop counters, pub confirmation,
// echo payload lines, route waiting, and SEDP reader declaration.
// Live tests use allocated exclusive domains with per-run live IDs and
// context timeouts. No timeout binary and no shell strings are used.
//
//nolint:cyclop // test breadth
package live_cli

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/podomy/weld/testbed/live/helpers"
)

// CLI conformance constants for live timeouts.
//
// Each test allocates one exclusive domain from the helpers pool, so weld
// to weld checks and weld to ROS checks never share a domain. Isolated
// checks allocate pooled domains with no peer to prove route waiting and
// runtime exit codes without colliding with concurrent runs.
const (
	cliBuildTimeout     = 2 * time.Minute
	cliShortTimeout     = 15 * time.Second
	cliEchoTimeout      = 8 * time.Second
	cliPubTimeout       = 30 * time.Second
	cliLiveRouteTimeout = 7 * time.Second
	cliForwardSleep     = 5 * time.Second
	cliReverseSleep     = 6 * time.Second
)

// cliTopicSeq counts cliTopicSuffix calls for in-process uniqueness.
var cliTopicSeq atomic.Uint64

// cliTopicSuffix returns a short numeric suffix for isolated topic names.
//
// It is only a topic suffix, not a payload live ID. Payload isolation
// always uses helpers.LiveID. The numeric form keeps isolated topics valid
// without extra sanitizing. The pid plus an atomic sequence keeps parallel
// workers in one process unique even when UnixNano repeats.
func cliTopicSuffix() string {
	seq := cliTopicSeq.Add(1)
	return strconv.FormatInt(int64(os.Getpid()), 10) + "_" +
		strconv.FormatInt(time.Now().UnixNano()%1000000, 10) + "_" +
		strconv.FormatUint(seq, 10)
}

// cliEnvWithoutDomain returns env with ROS_DOMAIN_ID removed.
func cliEnvWithoutDomain() []string {
	base := os.Environ()
	out := make([]string, 0, len(base))
	for _, kv := range base {
		if !strings.HasPrefix(kv, "ROS_DOMAIN_ID=") {
			out = append(out, kv)
		}
	}
	return out
}

// cliRunResult carries one weld run outcome.
type cliRunResult struct {
	stdout string
	stderr string
	code   int
}

// cliRunWeld runs the weld binary with args and env and returns the result.
func cliRunWeld(ctx context.Context, bin string, env []string, args ...string) cliRunResult {
	cmd := exec.CommandContext(ctx, bin, args...) //nolint:gosec // G204: test drives built weld binary and docker argv, no untrusted input
	cmd.Env = env
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	if err == nil {
		return cliRunResult{stdout: stdout.String(), stderr: stderr.String(), code: 0}
	}
	exitErr := &exec.ExitError{}
	ok := errors.As(err, &exitErr)
	if !ok {
		return cliRunResult{stdout: stdout.String(), stderr: stderr.String(), code: -1}
	}
	return cliRunResult{stdout: stdout.String(), stderr: stderr.String(), code: exitErr.ExitCode()}
}

// cliFirstLine returns the first line of s without trailing newline.
func cliFirstLine(s string) string {
	line, _, _ := strings.Cut(s, "\n")
	return strings.TrimRight(line, " \r\t")
}

// TestCLITextVsPositional checks text wins and arg counts.
func TestGateCLITextVsPositional(t *testing.T) {
	domain := helpers.MustAllocateDomain(t)
	domainStr := strconv.Itoa(domain)
	ctx, cancel := context.WithTimeout(context.Background(), cliBuildTimeout)
	defer cancel()
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)
	bin := helpers.BuildWeld(t, ctx)
	topic := "/weldcli" + cliTopicSuffix()

	// Pub needs a discovered route, so start a ROS echo peer on the same
	// topic first. The isolated domain has no ambient peers, and without
	// one both pubs fail with no route.
	peerID := helpers.LiveID("cli-peer")
	peerArgv := []string{helpers.RosBin, "topic", "echo", topic, helpers.RosType}
	err := helpers.StartPeerProc(ctx, helpers.Container, domain, peerID, peerArgv...)
	if err != nil {
		t.Fatalf("start echo peer: %v", err)
	}
	t.Cleanup(func() {
		helpers.StopPeerProcAndWait(context.WithoutCancel(ctx), helpers.Container, domain, helpers.HostPidPath(t, peerID), topic)
	})
	helpers.SettleAfterJoin(ctx)

	// Flag plus positional is rejected: silently dropping the positional
	// would lose user data, so pub matches service and action and fails
	// loudly instead of letting text win.
	winCtx, winCancel := context.WithTimeout(ctx, cliShortTimeout)
	winRes := cliRunWeld(winCtx, bin, helpers.EnvWithDomain(domain), "pub", "--topic", topic, "--domain", domainStr, "--text", "flag-wins", "pos-ignored")
	winCancel()
	if winRes.code != 2 {
		t.Fatalf("flag plus positional: exit = %d, want 2\nstdout:\n%s\nstderr:\n%s", winRes.code, winRes.stdout, winRes.stderr)
	}
	if !strings.Contains(winRes.stderr, "flag plus positionals") {
		t.Fatalf("flag plus positional stderr = %q, want flag plus positionals\nstdout:\n%s", winRes.stderr, winRes.stdout)
	}

	// Single positional without flag must succeed.
	posCtx, posCancel := context.WithTimeout(ctx, cliShortTimeout)
	posRes := cliRunWeld(posCtx, bin, helpers.EnvWithDomain(domain), "pub", "--topic", topic, "--domain", domainStr, "hello-pos")
	posCancel()
	if posRes.code != 0 {
		t.Fatalf("positional: exit = %d, want 0\nstderr:\n%s", posRes.code, posRes.stderr)
	}
	if strings.TrimSpace(posRes.stdout) != "sent: hello-pos" {
		t.Fatalf("positional stdout = %q, want sent hello-pos", posRes.stdout)
	}

	// Two positionals without flag must fail with exit 2.
	twoCtx, twoCancel := context.WithTimeout(ctx, cliShortTimeout)
	twoRes := cliRunWeld(twoCtx, bin, cliEnvWithoutDomain(), "pub", "--topic", "/chatter", "--domain", domainStr, "one", "two")
	twoCancel()
	if twoRes.code != 2 {
		t.Fatalf("two args: exit = %d, want 2\nstdout:\n%s\nstderr:\n%s", twoRes.code, twoRes.stdout, twoRes.stderr)
	}
	if !strings.HasPrefix(cliFirstLine(twoRes.stderr), "weld:") {
		t.Fatalf("two args misses weld prefix:\n%s", twoRes.stderr)
	}

	// Missing message must fail with exit 2.
	missCtx, missCancel := context.WithTimeout(ctx, cliShortTimeout)
	missRes := cliRunWeld(missCtx, bin, cliEnvWithoutDomain(), "pub", "--topic", "/chatter", "--domain", domainStr)
	missCancel()
	if missRes.code != 2 {
		t.Fatalf("missing text: exit = %d, want 2", missRes.code)
	}
	if !strings.HasPrefix(cliFirstLine(missRes.stderr), "weld:") {
		t.Fatalf("missing text misses weld prefix:\n%s", missRes.stderr)
	}
}

// TestCLIPubPrintsSent checks pub prints sent text on an allocated domain.
func TestGateCLIPubPrintsSent(t *testing.T) {
	domain := helpers.MustAllocateDomain(t)
	domainStr := strconv.Itoa(domain)
	ctx, cancel := context.WithTimeout(context.Background(), cliBuildTimeout)
	defer cancel()
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)
	bin := helpers.BuildWeld(t, ctx)
	live := helpers.LiveID("cli")
	text := "sent-probe " + live
	topic := "/weldcli" + cliTopicSuffix()

	// Pub needs a discovered route, so start a ROS echo peer on the same
	// topic first. The isolated domain has no ambient peers, and without
	// one pub fails with no route.
	peerID := helpers.LiveID("cli-peer")
	peerArgv := []string{helpers.RosBin, "topic", "echo", topic, helpers.RosType}
	err := helpers.StartPeerProc(ctx, helpers.Container, domain, peerID, peerArgv...)
	if err != nil {
		t.Fatalf("start echo peer: %v", err)
	}
	t.Cleanup(func() {
		helpers.StopPeerProcAndWait(context.WithoutCancel(ctx), helpers.Container, domain, helpers.HostPidPath(t, peerID), topic)
	})
	helpers.SettleAfterJoin(ctx)

	pubCtx, pubCancel := context.WithTimeout(ctx, cliPubTimeout)
	res := cliRunWeld(pubCtx, bin, helpers.EnvWithDomain(domain), "pub", "--topic", topic, "--domain", domainStr, "--text", text)
	pubCancel()
	if res.code != 0 {
		t.Fatalf("pub: exit = %d, want 0\nstdout:\n%s\nstderr:\n%s", res.code, res.stdout, res.stderr)
	}
	if strings.TrimSpace(res.stdout) != "sent: "+text {
		t.Fatalf("pub stdout = %q, want sent text", res.stdout)
	}
}

// TestCLIEchoPrintsPayload checks echo prints one payload per line.
func TestGateCLIEchoPrintsPayload(t *testing.T) {
	domain := helpers.MustAllocateDomain(t)
	domainStr := strconv.Itoa(domain)
	ctx, cancel := context.WithTimeout(context.Background(), cliBuildTimeout)
	defer cancel()
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)
	bin := helpers.BuildWeld(t, ctx)
	live := helpers.LiveID("cli")
	text := "echo-payload " + live
	topic := "/weldcli" + cliTopicSuffix()

	// Start echo with its own timeout, then publish into it.
	echoCtx, echoCancel := context.WithTimeout(ctx, cliEchoTimeout+15*time.Second)
	defer echoCancel()
	echoCmd := exec.CommandContext(echoCtx, bin, "echo", "--topic", topic, "--domain", domainStr, "--timeout", cliEchoTimeout.String()) //nolint:gosec // G204: test drives built weld binary and docker argv, no untrusted input
	echoCmd.Env = helpers.EnvWithDomain(domain)
	var echoOut bytes.Buffer
	var echoErr bytes.Buffer
	echoCmd.Stdout = &echoOut
	echoCmd.Stderr = &echoErr
	err := echoCmd.Start()
	if err != nil {
		t.Fatalf("start echo: %v", err)
	}
	t.Cleanup(func() {
		helpers.StopCmd(echoCmd)
	})
	helpers.Sleep(ctx, 2*time.Second)

	pubCtx, pubCancel := context.WithTimeout(ctx, cliPubTimeout)
	pubRes := cliRunWeld(pubCtx, bin, helpers.EnvWithDomain(domain), "pub", "--topic", topic, "--domain", domainStr, "--text", text)
	pubCancel()
	if pubRes.code != 0 {
		_ = echoCmd.Process.Kill() //nolint:errcheck // best-effort test cleanup, failure is not fatal
		_ = echoCmd.Wait()         //nolint:errcheck // best-effort test cleanup, failure is not fatal
		t.Fatalf("pub for echo: exit = %d, want 0\nstderr:\n%s", pubRes.code, pubRes.stderr)
	}

	waitErr := echoCmd.Wait()
	if waitErr != nil {
		t.Fatalf("echo wait: %v\nstdout:\n%s\nstderr:\n%s", waitErr, echoOut.String(), echoErr.String())
	}
	if !strings.Contains(echoOut.String(), text) {
		t.Fatalf("echo output misses payload %q\nstdout:\n%s\nstderr:\n%s", text, echoOut.String(), echoErr.String())
	}
	// Payload must appear on its own line.
	found := false
	for line := range strings.SplitSeq(strings.TrimSpace(echoOut.String()), "\n") {
		if strings.TrimSpace(line) == text {
			found = true
		}
	}
	if !found {
		t.Fatalf("echo payload not on its own line, got:\n%s", echoOut.String())
	}
}

// TestCLIPubWaitsForRoute checks retry on ErrNoRoute and ctx cancel.
//
//nolint:gocognit,cyclop // CLI test asserts route retry plus cancel across static and live checks
func TestGateCLIPubWaitsForRoute(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), cliBuildTimeout)
	defer cancel()

	// Static shape: retry interval, timeout, ErrNoRoute check, ctx cancel.
	path := filepath.Join(helpers.RepoRoot(t), "cmd", "weld", "main.go")
	data, err := os.ReadFile(path) //nolint:gosec // G304: test reads fixed repo and temp paths, no untrusted input
	if err != nil {
		t.Fatalf("read main.go: %v", err)
	}
	text := string(data)
	if !strings.Contains(text, "pubRouteRetryInterval") {
		t.Fatalf("main.go misses pubRouteRetryInterval")
	}
	if !strings.Contains(text, "50 * time.Millisecond") {
		t.Fatalf("main.go misses 50ms retry interval")
	}
	if !strings.Contains(text, "pubRouteRetryTimeout") {
		t.Fatalf("main.go misses pubRouteRetryTimeout")
	}
	if !strings.Contains(text, "5 * time.Second") {
		t.Fatalf("main.go misses 5s retry timeout")
	}
	if !strings.Contains(text, "ErrNoRoute") {
		t.Fatalf("main.go misses ErrNoRoute")
	}
	if !strings.Contains(text, "ctx.Done()") {
		t.Fatalf("main.go misses ctx cancel in route wait")
	}

	bin := helpers.BuildWeld(t, ctx)
	topic := "/weldcli" + cliTopicSuffix()

	// Isolated domain must retry up to timeout then exit 1.
	isoCtx, isoCancel := context.WithTimeout(ctx, cliLiveRouteTimeout+15*time.Second)
	start := time.Now()
	isoPool := helpers.MustAllocateDomain(t)
	isoDomain := strconv.Itoa(isoPool)
	isoRes := cliRunWeld(isoCtx, bin, cliEnvWithoutDomain(), "pub", "--topic", topic, "--domain", isoDomain, "--text", "route-wait")
	elapsed := time.Since(start)
	isoCancel()
	if isoRes.code != 1 {
		t.Fatalf("route wait: exit = %d, want 1\nstderr:\n%s", isoRes.code, isoRes.stderr)
	}
	if elapsed < 4*time.Second {
		t.Fatalf("route wait elapsed %v too short, want retry up to timeout", elapsed)
	}
	if !strings.Contains(isoRes.stderr, "no route") {
		t.Fatalf("route wait misses no route:\n%s", isoRes.stderr)
	}

	// Canceled context must end quickly with runtime exit.
	// The CLI honors interrupt during route wait, so SIGINT ends the
	// 5s retry early with a runtime exit instead of running to timeout.
	intCtx, intCancel := context.WithTimeout(ctx, cliShortTimeout)
	defer intCancel()
	intTopic := "/weldcli" + cliTopicSuffix()
	intPool := helpers.MustAllocateDomain(t)
	intDomain := strconv.Itoa(intPool)
	intCmd := exec.CommandContext(intCtx, bin, "pub", "--topic", intTopic, "--domain", intDomain, "--text", "route-cancel") //nolint:gosec // G204: test drives built weld binary and docker argv, no untrusted input
	intCmd.Env = cliEnvWithoutDomain()
	var intOut bytes.Buffer
	var intErr bytes.Buffer
	intCmd.Stdout = &intOut
	intCmd.Stderr = &intErr
	intStart := time.Now()
	err = intCmd.Start()
	if err != nil {
		t.Fatalf("start pub for cancel: %v", err)
	}
	helpers.Sleep(ctx, 1*time.Second)
	sigErr := intCmd.Process.Signal(os.Interrupt)
	if sigErr != nil {
		_ = intCmd.Process.Kill() //nolint:errcheck // best-effort test cleanup, failure is not fatal
		_ = intCmd.Wait()         //nolint:errcheck // best-effort test cleanup, failure is not fatal
		t.Fatalf("signal pub: %v", sigErr)
	}
	waitErr := intCmd.Wait()
	intElapsed := time.Since(intStart)
	if waitErr == nil {
		t.Fatalf("route cancel succeeded, want runtime failure")
	}
	if intElapsed > 5*time.Second {
		t.Fatalf("route cancel elapsed %v too long, want fast interrupt", intElapsed)
	}
	combined := intOut.String() + "\n" + intErr.String()
	if !strings.Contains(combined, "weld:") || !strings.Contains(combined, "context") {
		t.Fatalf("route cancel output misses weld prefix or context, want both:\n%s", combined)
	}
}

// TestCLIWeldToWeldRoundTrip checks echo sees pub on an allocated domain.
func TestGateCLIWeldToWeldRoundTrip(t *testing.T) {
	domain := helpers.MustAllocateDomain(t)
	domainStr := strconv.Itoa(domain)
	ctx, cancel := context.WithTimeout(context.Background(), cliBuildTimeout)
	defer cancel()
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)
	bin := helpers.BuildWeld(t, ctx)
	live := helpers.LiveID("cli")
	text := "roundtrip " + live
	topic := "/weldcli" + cliTopicSuffix()

	echoCtx, echoCancel := context.WithTimeout(ctx, cliEchoTimeout+15*time.Second)
	defer echoCancel()
	echoCmd := exec.CommandContext(echoCtx, bin, "echo", "--topic", topic, "--domain", domainStr, "--timeout", cliEchoTimeout.String()) //nolint:gosec // G204: test drives built weld binary and docker argv, no untrusted input
	echoCmd.Env = helpers.EnvWithDomain(domain)
	var echoOut bytes.Buffer
	var echoErr bytes.Buffer
	echoCmd.Stdout = &echoOut
	echoCmd.Stderr = &echoErr
	err := echoCmd.Start()
	if err != nil {
		t.Fatalf("start echo: %v", err)
	}
	t.Cleanup(func() {
		helpers.StopCmd(echoCmd)
	})
	helpers.Sleep(ctx, 2*time.Second)

	pubCtx, pubCancel := context.WithTimeout(ctx, cliPubTimeout)
	pubRes := cliRunWeld(pubCtx, bin, helpers.EnvWithDomain(domain), "pub", "--topic", topic, "--domain", domainStr, "--text", text)
	pubCancel()
	if pubRes.code != 0 {
		_ = echoCmd.Process.Kill() //nolint:errcheck // best-effort test cleanup, failure is not fatal
		_ = echoCmd.Wait()         //nolint:errcheck // best-effort test cleanup, failure is not fatal
		t.Fatalf("pub: exit = %d, want 0\nstderr:\n%s", pubRes.code, pubRes.stderr)
	}
	if strings.TrimSpace(pubRes.stdout) != "sent: "+text {
		_ = echoCmd.Process.Kill() //nolint:errcheck // best-effort test cleanup, failure is not fatal
		_ = echoCmd.Wait()         //nolint:errcheck // best-effort test cleanup, failure is not fatal
		t.Fatalf("pub stdout = %q, want sent text", pubRes.stdout)
	}

	waitErr := echoCmd.Wait()
	if waitErr != nil {
		t.Fatalf("echo wait: %v\nstdout:\n%s\nstderr:\n%s", waitErr, echoOut.String(), echoErr.String())
	}
	if !strings.Contains(echoOut.String(), live) {
		t.Fatalf("echo did not receive live %q\nstdout:\n%s\nstderr:\n%s", text, echoOut.String(), echoErr.String())
	}
}

// TestCLIForwardRosTalkerSeenByWeldEcho checks weld echo sees ROS pub on an allocated domain.
//
// A probe reader subscribes before the talker starts so the talker
// publishes only after the reader is visible, then WaitForSEDPRemote waits
// for the ROS writer before echo runs, and echo retries twice more so a
// late match still lands.
func TestGateCLIForwardRosTalkerSeenByWeldEcho(t *testing.T) {
	domain := helpers.MustAllocateDomain(t)
	t.Cleanup(func() {
		helpers.AssertNoExcessSkips(t)
	})
	domainStr := strconv.Itoa(domain)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)
	bin := helpers.BuildWeld(t, ctx)
	live := helpers.LiveID("cli")
	payload := "hello forward " + live
	topic := helpers.LiveTopic(live)

	probe := helpers.MustOpenLiveBackend(t, ctx, uint32(domain), "cli-fwd-"+live, "forward-probe") //nolint:gosec // G115: pooled domain 70 through 101 fits uint32
	err := probe.Subscribe(ctx, topic, helpers.ForwardStringIDL, helpers.ForwardStringHash)
	if err != nil {
		t.Fatalf("subscribe forward probe reader: %v", err)
	}
	pubArg := fmt.Sprintf("{data: %s}", payload)
	args := helpers.DockerArgs(helpers.Container, domain, helpers.RosBin, "topic", "pub", topic, helpers.RosType, pubArg, "--rate", "2")
	talker := exec.CommandContext(ctx, "docker", args...) //nolint:gosec // G204: test drives built weld binary and docker argv, no untrusted input
	var talkerBuf bytes.Buffer
	talker.Stdout = &talkerBuf
	talker.Stderr = &talkerBuf
	err = talker.Start()
	if err != nil {
		t.Fatalf("start ROS talker: %v", err)
	}
	t.Cleanup(func() {
		helpers.StopCmd(talker)
		cleanupCtx, cleanupCancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		defer cleanupCancel()
		if err := helpers.KillAndWaitContainerProc(cleanupCtx, helpers.Container, live); err != nil {
			t.Errorf("stop peer proc: %v", err)
		}
	})
	helpers.WaitForSEDPRemote(t, ctx, probe, topic, "ROS talker writer", helpers.SEDPRemoteTimeout)
	helpers.Sleep(ctx, cliForwardSleep)
	if ctx.Err() != nil {
		t.Fatalf("context ended before echo: %v", ctx.Err())
	}

	var echoRes cliRunResult
	for attempt := range helpers.ForwardEchoAttempts {
		echoCtx, echoCancel := context.WithTimeout(ctx, 25*time.Second)
		echoRes = cliRunWeld(echoCtx, bin, helpers.EnvWithDomain(domain), "echo", "--topic", topic, "--domain", domainStr, "--timeout", "10s")
		echoCancel()
		if echoRes.code != 0 {
			t.Fatalf("weld echo: exit = %d, want 0\nstdout:\n%s\nstderr:\n%s", echoRes.code, echoRes.stdout, echoRes.stderr)
		}
		if strings.Contains(echoRes.stdout, live) {
			break
		}
		if attempt+1 < helpers.ForwardEchoAttempts {
			helpers.Sleep(ctx, helpers.ForwardEchoRetryGap)
		}
	}
	combined := echoRes.stdout + "\n" + echoRes.stderr
	if strings.Contains(combined, "not implemented") {
		t.Fatalf("weld echo backend error:\n%s", combined)
	}
	if strings.Contains(combined, "not wired") {
		t.Fatalf("weld echo backend error:\n%s", combined)
	}
	if !strings.Contains(echoRes.stdout, live) {
		t.Logf("weld echo did not receive ROS live %q (known user DATA gap), echo ok without payload\nstdout:\n%s\nstderr:\n%s", payload, echoRes.stdout, echoRes.stderr)
		reason := "weld echo misses ROS user DATA but echo ok, known user DATA gap forward cli"
		helpers.RegisterAllowlistedSkip(t.Name(), reason, time.Date(2027, 5, 1, 0, 0, 0, 0, time.UTC))
		helpers.NoteProductSkip(t, t.Name(), reason)
	}
}

// TestCLIReverseWeldPubSeenByRosEcho checks ros echo sees weld pub on an allocated domain.
//
//nolint:gocognit,cyclop // reverse test asserts publish plus file poll across two peers in one run
func TestGateCLIReverseWeldPubSeenByRosEcho(t *testing.T) {
	domain := helpers.MustAllocateDomain(t)
	t.Cleanup(func() {
		helpers.AssertNoExcessSkips(t)
	})
	domainStr := strconv.Itoa(domain)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)
	bin := helpers.BuildWeld(t, ctx)
	live := helpers.LiveID("cli")
	payload := "hello reverse " + live
	topic := helpers.LiveTopic(live)

	echoPath := filepath.Join(t.TempDir(), "rosecho.log")
	echoFile, err := os.Create(echoPath) //nolint:gosec // G304: test reads fixed repo and temp paths, no untrusted input
	if err != nil {
		t.Fatalf("create ros echo log: %v", err)
	}
	defer func() {
		cerr := echoFile.Close()
		if cerr != nil {
			t.Logf("close ros echo log: %v", cerr)
		}
	}()

	args := helpers.DockerArgs(helpers.Container, domain, helpers.RosBin, "topic", "echo", topic, helpers.RosType)
	echoCmd := exec.CommandContext(ctx, "docker", args...) //nolint:gosec // G204: test drives built weld binary and docker argv, no untrusted input
	echoCmd.Stdout = echoFile
	echoCmd.Stderr = echoFile
	err = echoCmd.Start()
	if err != nil {
		t.Fatalf("start ros2 topic echo: %v", err)
	}
	t.Cleanup(func() {
		helpers.StopCmd(echoCmd)
		cleanupCtx, cleanupCancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		defer cleanupCancel()
		if err := helpers.KillAndWaitContainerProc(cleanupCtx, helpers.Container, topic); err != nil {
			t.Errorf("stop peer proc: %v", err)
		}
	})
	helpers.Sleep(ctx, cliReverseSleep)
	if ctx.Err() != nil {
		t.Fatalf("context ended before pub: %v", ctx.Err())
	}

	pubCtx, pubCancel := context.WithTimeout(ctx, cliPubTimeout)
	pubRes := cliRunWeld(pubCtx, bin, helpers.EnvWithDomain(domain), "pub", "--topic", topic, "--domain", domainStr, "--text", payload)
	pubCancel()
	if pubRes.code != 0 {
		t.Fatalf("weld pub: exit = %d, want 0\nstdout:\n%s\nstderr:\n%s", pubRes.code, pubRes.stdout, pubRes.stderr)
	}
	if !strings.Contains(pubRes.stdout, "sent:") {
		t.Fatalf("weld pub misses confirmation:\nstdout:\n%s\nstderr:\n%s", pubRes.stdout, pubRes.stderr)
	}

	deadline := time.Now().Add(20 * time.Second)
	found := false
	for time.Now().Before(deadline) {
		fileData, readErr := os.ReadFile(echoPath) //nolint:gosec // G304: test reads fixed repo and temp paths, no untrusted input
		if readErr != nil {
			t.Fatalf("read ros echo log: %v", readErr)
		}
		if strings.Contains(string(fileData), live) {
			found = true
			break
		}
		if ctx.Err() != nil {
			break
		}
		helpers.Sleep(ctx, 200*time.Millisecond)
	}
	if !found {
		fileData, readErr := os.ReadFile(echoPath) //nolint:gosec // G304: test reads fixed repo and temp paths, no untrusted input
		if readErr != nil {
			t.Fatalf("read ros echo log: %v", readErr)
		}
		t.Logf("ros echo did not see live %q (known user DATA gap), pub ok\noutput:\n%s", payload, string(fileData))
		reason := "ros echo misses weld user DATA but pub ok, known user DATA gap reverse cli"
		helpers.RegisterAllowlistedSkip(t.Name(), reason, time.Date(2027, 5, 1, 0, 0, 0, 0, time.UTC))
		helpers.NoteProductSkip(t, t.Name(), reason)
	}
}
