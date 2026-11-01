// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

// Package helpers holds shared docker argv and live test helpers.
package helpers

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/podomy/weld/internal/transports/dds"
	"github.com/podomy/weld/internal/transports/dds/rtps"
)

// Live gate constants.
//
// Domain 47 is reserved for this package so a live run never hears a
// standalone run on 42 or a run on 48. Distro peers use 42 for Jazzy,
// 43 for Humble, and 44 for Rolling. Ephemeral cases use 60 through 69
// (60-64 Cyclone liveliness, 65-69 ROS liveliness, per-process CLI
// isolation in 60-69) so they never hear canonical 42, 47, or 48 and
// never hear each other. Topic base and container match
// testbed/docker-compose.yml. Timeouts bound every docker and
// weld invocation through context, never through shell timeout.
// Join and announce settles are raised above the legacy 2s value that
// flaked under load; single-receive and full-capture windows are sized
// to absorb CI multicast delay without masking assertion failures.
const (
	// LiveDomain is the DDS domain for all tests in this package.
	LiveDomain = 47
	// StandaloneDomain is the standalone domain, kept for docs.
	StandaloneDomain = 42
	// RegressionDomain is the SPDP domain, kept for docs.
	RegressionDomain = 48
	// DistroHumbleDomain is the Humble distro peer domain.
	DistroHumbleDomain = 43
	// DistroRollingDomain is the Rolling distro peer domain.
	DistroRollingDomain = 44
	// EphemeralDomainMin is the first ephemeral live domain.
	EphemeralDomainMin = 60
	// EphemeralDomainMax is the last ephemeral live domain.
	EphemeralDomainMax = 69
	// Container is the canonical ROS peer.
	Container = "weld-jazzy"
	// Topic is the ROS topic base for forward and reverse checks.
	// Per-run topics derive from this base with the live ID appended.
	Topic = "/chatter"
	// RosType is the ROS message type for pub and echo.
	RosType = "std_msgs/msg/String"
	// RMW is the Cyclone RMW used inside the peer.
	RMW = "rmw_cyclonedds_cpp"
	// RosBin is the ros2 binary inside the peer, run directly with no shell.
	RosBin = "/opt/ros/jazzy/bin/ros2"
	// ContainerLogsDir is the log directory inside the peer.
	// It is bind mounted to testbed/logs on the host, so a container
	// write to ContainerLogsDir appears under testbed/logs on the host.
	ContainerLogsDir = "/testbed/logs"
	// DockerProbeTimeout bounds docker inspect and info probes.
	DockerProbeTimeout = 5 * time.Second
	// BuildTimeout bounds the weld CLI build.
	BuildTimeout = 2 * time.Minute
	// TalkerSettle bounds the wait after starting a ROS talker.
	TalkerSettle = 5 * time.Second
	// EchoSettle bounds the wait after starting ros2 topic echo.
	EchoSettle = 6 * time.Second
	// JoinSettle bounds the wait after joining SPDP multicast before sending.
	// It is raised from the legacy 2s settle that flaked under CI load
	// while IGMP membership and the peer stack quiesce.
	JoinSettle = 5 * time.Second
	// AnnounceSettle bounds the wait after the first SPDP announce before capture.
	// It is raised from the legacy 2s settle that missed repeats under load.
	AnnounceSettle = 5 * time.Second
	// JoinTimeout bounds one JoinMulticast call.
	// It is larger than the docker probe so a slow multicast join under
	// load reports join failure rather than a probe timeout.
	JoinTimeout = 10 * time.Second
	// EchoTimeout bounds one weld echo run.
	EchoTimeout = 10 * time.Second
	// PubTimeout bounds one weld pub run.
	PubTimeout = 30 * time.Second
	// RecvTimeout bounds one multicast receive.
	// It is raised from 3s because loaded CI hosts regularly exceed 3s
	// between SPDP repeats, surfacing as context deadline exceeded.
	RecvTimeout = 10 * time.Second
	// LiveTimeout bounds the full live SPDP capture.
	// It is raised from 30s so repeated single-receive windows plus
	// talker and echo settles fit without truncating the capture loop.
	LiveTimeout = 60 * time.Second
	// FilePollTimeout bounds waiting for ros echo output to hold a live ID.
	FilePollTimeout = 20 * time.Second
	// FilePollInterval is the poll step for WaitForFileContains.
	FilePollInterval = 200 * time.Millisecond
	// EchoMargin is the outer margin added to one weld echo timeout.
	// It lets weld exit on its own timeout and flush before the outer
	// context fires.
	EchoMargin = 15 * time.Second
)

// ImageEnv carries the ROS setup in structured form.
//
// ros2 is not on the container PATH without its setup files. These values
// replicate what /opt/ros/jazzy/setup.bash exports, so docker exec can run
// the ros2 argv directly with -e flags and no shell wrapper.
var ImageEnv = [][2]string{
	{"AMENT_PREFIX_PATH", "/opt/ros/jazzy"},
	{"LD_LIBRARY_PATH", "/opt/ros/jazzy/opt/zenoh_cpp_vendor/lib:/opt/ros/jazzy/lib/x86_64-linux-gnu:/opt/ros/jazzy/lib"},
	{"PATH", "/opt/ros/jazzy/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"},
	{"PYTHONPATH", "/opt/ros/jazzy/lib/python3.12/site-packages"},
	{"ROS_DISTRO", "jazzy"},
	{"ROS_VERSION", "2"},
	{"ROS_PYTHON_VERSION", "3"},
}

// LiveID returns a per-run identifier for payload isolation.
//
// Prefix scopes the ID to the calling suite so concurrent suites never
// match each other. Callers must generate one ID per test run and reuse
// it for start and assert. Use LiveTopic to derive the per-run topic for
// the same run; payload IDs alone do not isolate topics.
func LiveID(prefix string) string {
	if prefix == "" {
		prefix = "live"
	}
	return fmt.Sprintf("%s-%d-%d", prefix, os.Getpid(), time.Now().UnixNano())
}

// LiveTopic returns a per-run ROS topic derived from the live ID.
//
// The base is Topic plus the sanitized ID, so concurrent or stale runs on
// domain 47 never share a topic. Hyphens map to underscores to keep the
// name valid for ros2 topic pub and echo. Use the same topic for the
// talker and the weld side within one test.
func LiveTopic(id string) string {
	sanitized := strings.ReplaceAll(id, "-", "_")
	sanitized = strings.ReplaceAll(sanitized, " ", "_")
	return Topic + "_" + sanitized
}

// ValidateDomain rejects domains outside 0 through 101.
//
// It mirrors the gate check without forking a shell. Empty and
// non-numeric input is rejected by the caller parsing step, here only the
// range is pinned.
func ValidateDomain(domain int) error {
	if domain < 0 {
		return fmt.Errorf("helpers: domain %d below minimum 0", domain)
	}
	if domain > 101 {
		return fmt.Errorf("helpers: domain %d above maximum 101", domain)
	}
	return nil
}

// IsEphemeralDomain reports whether domain is an ephemeral live domain.
//
// Ephemeral domains 60 through 69 never overlap canonical 42, 47, or 48
// and never overlap distro 43 or 44, so liveliness and CLI isolation
// runs stay exclusive.
func IsEphemeralDomain(domain int) bool {
	return domain >= EphemeralDomainMin && domain <= EphemeralDomainMax
}

// ros2SpawnsDaemon reports whether a container ros2 argv starts a CLI daemon.
//
// Only topic echo and param verbs use the daemon; pub, service, action,
// and interface verbs spin their own nodes. Daemon nodes join discovery
// and hold unicast ports until killed, which breaks isolation tests and
// port probes on reused domains, so callers add --no-daemon for these
// verbs. The check matches the ros2 binary base name so distro peers
// with absolute RosBin paths are covered.
func ros2SpawnsDaemon(cmd []string) bool {
	if len(cmd) < 3 {
		return false
	}
	if filepath.Base(cmd[0]) != "ros2" {
		return false
	}
	if cmd[1] == "param" {
		return true
	}
	return cmd[1] == "topic" && cmd[2] == "echo"
}

// dockerExecArgs builds a docker exec argv with domain, RMW, env, container and command.
func dockerExecArgs(base []string, domain int, env [][2]string, container string, cmd ...string) []string {
	if ros2SpawnsDaemon(cmd) {
		dup := append([]string(nil), cmd...)
		dup = append(dup, "--no-daemon")
		cmd = dup
	}
	args := append([]string(nil), base...)
	args = append(args, "-e", "ROS_DOMAIN_ID="+strconv.Itoa(domain))
	args = append(args, "-e", "RMW_IMPLEMENTATION="+RMW)
	for _, kv := range env {
		args = append(args, "-e", kv[0]+"="+kv[1])
	}
	args = append(args, container)
	args = append(args, cmd...)
	return args
}

// DockerArgs builds a docker exec argv with structured environment.
//
// The returned argv starts with exec and carries ROS_DOMAIN_ID plus the RMW
// implementation and the ROS setup as -e flags, then the container name and
// the command. No shell is used at any point.
func DockerArgs(container string, domain int, cmd ...string) []string {
	return dockerExecArgs([]string{"exec"}, domain, ImageEnv, container, cmd...)
}

// DockerDetachedArgs builds a docker exec -d argv with structured environment.
//
// It carries the same ROS_DOMAIN_ID plus RMW and ROS setup -e flags as
// DockerArgs, then the container name and the command. The -d flag
// detaches, so the caller never holds a host pipe for the peer proc.
// Output redirection must happen inside the container to the logs volume.
func DockerDetachedArgs(container string, domain int, cmd ...string) []string {
	return dockerExecArgs([]string{"exec", "-d"}, domain, ImageEnv, container, cmd...)
}

// ContainerOutPath returns the container log path for a run ID.
//
// The path lives under ContainerLogsDir, which is bind mounted to
// testbed/logs on the host.
func ContainerOutPath(runID string) string {
	return ContainerLogsDir + "/" + runID + ".out"
}

// ContainerPidPath returns the container pidfile path for a run ID.
//
// The pidfile holds the detached peer PID inside the container so
// cleanup can signal exactly that proc.
func ContainerPidPath(runID string) string {
	return ContainerLogsDir + "/" + runID + ".pid"
}

// HostOutPath returns the host log path for a run ID.
//
// It resolves under testbed/logs in the repo root, which is the bind
// source for ContainerLogsDir. Callers poll this file directly with
// WaitForFileContains and never use docker exec cat per poll.
func HostOutPath(t *testing.T, runID string) string {
	t.Helper()
	checkRunID(t, runID)
	return filepath.Join(RepoRoot(t), "testbed", "logs", runID+".out")
}

// HostPidPath returns the host pidfile path for a run ID.
//
// It is the host view of ContainerPidPath through the same bind mount.
func HostPidPath(t *testing.T, runID string) string {
	t.Helper()
	checkRunID(t, runID)
	return filepath.Join(RepoRoot(t), "testbed", "logs", runID+".pid")
}

// validateRunIDPath rejects slash and dot dot escapes in run IDs.
func validateRunIDPath(runID string) error {
	if strings.Contains(runID, "/") {
		return fmt.Errorf("helpers: run ID %q must not hold a slash", runID)
	}
	if strings.Contains(runID, "..") {
		return fmt.Errorf("helpers: run ID %q must not hold dot dot", runID)
	}
	return nil
}

// buildDetachedScript builds the sh -c script for a detached peer proc.
func buildDetachedScript(outPath, pidPath string, rosArgs []string) string {
	if ros2SpawnsDaemon(rosArgs) {
		dup := append([]string(nil), rosArgs...)
		dup = append(dup, "--no-daemon")
		rosArgs = dup
	}
	quoted := make([]string, 0, len(rosArgs))
	for _, arg := range rosArgs {
		quoted = append(quoted, shellQuote(arg))
	}
	return "echo $$ > " + shellQuote(pidPath) + "; exec " + strings.Join(quoted, " ") + " > " + shellQuote(outPath) + " 2>&1"
}

// checkRunID rejects run IDs that could escape the logs directory.
func checkRunID(t *testing.T, runID string) {
	t.Helper()
	if runID == "" {
		t.Fatalf("empty run ID for logs volume")
	}
	err := validateRunIDPath(runID)
	if err != nil {
		t.Fatalf("run ID invalid: %v", err)
	}
}

// shellQuote quotes one argv word for sh -c inside the container.
func shellQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "'\\''") + "'"
}

// StartPeerProc starts a detached peer proc writing to the logs volume.
//
// rosArgs is the container argv, typically RosBin plus topic pub or echo.
// The proc runs via docker exec -d with sh -c only for pidfile plus
// redirection inside the container. Host side uses argv with no shell.
// Output lands in ContainerOutPath(runID), visible on the host at
// HostOutPath. The pidfile at ContainerPidPath holds the peer PID.
func StartPeerProc(ctx context.Context, container string, domain int, runID string, rosArgs ...string) error {
	if runID == "" {
		return errors.New("helpers: empty run ID for detached peer") //nolint:wrapcheck // new sentinel error, nothing to wrap
	}
	err := validateRunIDPath(runID)
	if err != nil {
		return err
	}
	if len(rosArgs) == 0 {
		return fmt.Errorf("helpers: no ros args for detached peer %q", runID)
	}
	outPath := ContainerOutPath(runID)
	pidPath := ContainerPidPath(runID)
	script := buildDetachedScript(outPath, pidPath, rosArgs)
	args := DockerDetachedArgs(container, domain, "sh", "-c", script)
	cmd := exec.CommandContext(ctx, "docker", args...) //nolint:gosec // live harness invokes docker with fixed args from test tables
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("helpers: docker exec -d %s: %w output %s", runID, err, string(out))
	}
	return nil
}

// StopPeerProc stops one detached peer run via pidfile plus scoped pkill.
//
// hostPidPath is the host view of the pidfile, typically HostPidPath.
// It reads the PID and runs kill inside the container, then runs one
// scoped pkill for pkillPattern so a survivor cannot leak. Both steps
// run once per invocation. Failures are best effort because a missing
// pidfile or an already exited proc is not an error.
func StopPeerProc(ctx context.Context, container string, domain int, hostPidPath, pkillPattern string) {
	if hostPidPath != "" {
		data, err := os.ReadFile(hostPidPath) //nolint:gosec // live harness reads test-controlled pidfile path
		if err == nil {
			pid := strings.TrimSpace(string(data))
			if pid != "" {
				killArgs := DockerArgs(container, domain, "kill", pid)
				killCmd := exec.CommandContext(ctx, "docker", killArgs...) //nolint:gosec // live harness invokes docker with fixed args from test tables
				_ = killCmd.Run()                                          //nolint:errcheck // best-effort cleanup, missing proc is not an error
			}
		}
	}
	if pkillPattern != "" {
		KillContainerProc(ctx, container, domain, pkillPattern)
	}
}

// ExecRun runs name with argv and extraEnv through context with no shell.
//
// extraEnv entries are KEY=VALUE pairs appended to the current environment.
// Stdout and stderr are captured separately. The context bounds the run.
func ExecRun(ctx context.Context, name string, args, extraEnv []string) (string, string, error) {
	cmd := exec.CommandContext(ctx, name, args...) //nolint:gosec // live harness runs fixed binaries with test-controlled argv
	base := os.Environ()
	env := make([]string, 0, len(base)+len(extraEnv))
	env = append(env, base...)
	env = append(env, extraEnv...)
	cmd.Env = env
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	if err != nil {
		return stdout.String(), stderr.String(), err
	}
	return stdout.String(), stderr.String(), nil
}

// DockerAvailable reports whether a docker daemon answers.
func DockerAvailable(ctx context.Context) bool {
	probeCtx, cancel := context.WithTimeout(ctx, DockerProbeTimeout)
	defer cancel()
	cmd := exec.CommandContext(probeCtx, "docker", "info")
	err := cmd.Run()
	return err == nil
}

// ContainerRunning reports whether name is a running container.
//
// It probes with docker inspect and no shell. A missing container reports
// false with no error so the caller skips cleanly. Other failures return
// an error so the caller can skip with context.
func ContainerRunning(ctx context.Context, name string) (bool, error) {
	probeCtx, cancel := context.WithTimeout(ctx, DockerProbeTimeout)
	defer cancel()
	cmd := exec.CommandContext(probeCtx, "docker", "inspect", "--format", "{{.State.Running}}", name) //nolint:gosec // live harness inspects test container with fixed docker argv and no shell
	out, err := cmd.CombinedOutput()
	if err != nil {
		text := strings.TrimSpace(string(out))
		if strings.Contains(text, "No such object") {
			return false, nil
		}
		if probeCtx.Err() != nil {
			return false, fmt.Errorf("helpers: docker inspect %s timed out: %w", name, probeCtx.Err())
		}
		return false, fmt.Errorf("helpers: docker inspect %s: %w: %s", name, err, text)
	}
	text := strings.TrimSpace(string(out))
	return text == "true", nil
}

// SkipIfNoPeer skips the test when no daemon or peer container is present.
//
// It never starts the compose stack. It uses only argv probes with context
// timeouts and no shell. Call it at the start of every optional live test.
// It skips only for daemon or peer absence (missing docker, unreachable
// daemon, failed inspect, stopped container). It never skips for assertion
// failures: wire, payload, and route checks must use t.Fatalf so flakes
// surface instead of passing silently.
// peerProbes runs docker lookpath plus daemon plus container probes.
func peerProbes(ctx context.Context, container string) (lookErr error, daemonOk, running bool, inspectErr error) {
	_, lookErr = exec.LookPath("docker")
	if lookErr != nil {
		return fmt.Errorf("helpers: docker lookpath: %w", lookErr), false, false, nil
	}
	daemonOk = DockerAvailable(ctx)
	if !daemonOk {
		return nil, false, false, nil
	}
	running, inspectErr = ContainerRunning(ctx, container)
	if inspectErr != nil {
		return nil, true, running, fmt.Errorf("helpers: docker inspect %s: %w", container, inspectErr)
	}
	return nil, true, running, nil
}

func SkipIfNoPeer(t *testing.T, ctx context.Context, container string) {
	t.Helper()
	lookErr, daemonOk, running, inspectErr := peerProbes(ctx, container)
	if lookErr != nil {
		t.Skipf("docker not found, skipping live gate: %v", lookErr)
	}
	if !daemonOk {
		t.Skipf("docker daemon unavailable, skipping live gate")
	}
	if inspectErr != nil {
		t.Skipf("docker inspect failed, skipping live gate: %v", inspectErr)
	}
	if !running {
		t.Skipf("container %s not running, skipping live gate (run: cd testbed && docker compose up -d)", container)
	}
}

// MustHavePeer fails the test when no daemon or peer container is present.
//
// It runs the same argv probes as SkipIfNoPeer with context timeouts and
// no shell, but it calls t.Fatalf instead of t.Skipf. Use it for strict
// gates that require the peer: a missing daemon or container is a setup
// failure, not a skip. Like SkipIfNoPeer it never masks assertion
// failures; wire and payload checks still fail independently.
func MustHavePeer(t *testing.T, ctx context.Context, container string) {
	t.Helper()
	lookErr, daemonOk, running, inspectErr := peerProbes(ctx, container)
	if lookErr != nil {
		t.Fatalf("docker not found, strict live gate requires peer: %v", lookErr)
	}
	if !daemonOk {
		t.Fatalf("docker daemon unavailable, strict live gate requires peer")
	}
	if inspectErr != nil {
		t.Fatalf("docker inspect failed for strict live gate: %v", inspectErr)
	}
	if !running {
		t.Fatalf("container %s not running, strict live gate requires peer (run: cd testbed && docker compose up -d)", container)
	}
}

// RepoRoot returns the repository root holding cmd/weld.
func RepoRoot(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatalf("runtime caller unavailable")
	}
	dir := filepath.Dir(file)
	for {
		candidate := filepath.Join(dir, "cmd", "weld")
		info, err := os.Stat(candidate)
		if err == nil && info.IsDir() {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatalf("repo root with cmd/weld not found above %s", file)
		}
		dir = parent
	}
}

// Shared weld build state. The binary builds once per test binary process.
var (
	buildWeldOnce sync.Once
	buildWeldBin  string
	errBuildWeld  error
)

// BuildWeld builds the weld CLI once per test binary process and returns its path.
//
// The CLI lives in its own module, so the build runs with GOWORK=off in
// cmd/weld with the same flags as a per-test build. Only the first caller
// builds, bounded by BuildTimeout through that caller context; later
// callers reuse the same path. The binary lives under os.MkdirTemp, not
// t.TempDir, because each test TempDir is removed at that test cleanup
// while later tests still need the binary. Concurrent exec of the finished
// path is safe because the file is immutable after the once build. The temp dir is one per test binary process and is intentionally left for the OS to reclaim; no cleanup removes it while later tests still run.
func BuildWeld(t *testing.T, ctx context.Context) string {
	t.Helper()
	root := RepoRoot(t)
	buildWeldOnce.Do(func() {
		buildCtx, cancel := context.WithTimeout(ctx, BuildTimeout)
		defer cancel()
		dir, err := os.MkdirTemp("", "weld-live-") //nolint:usetesting // binary must outlive per-test t.TempDir cleanup so later tests reuse it
		if err != nil {
			errBuildWeld = fmt.Errorf("helpers: mktemp weld-live: %w", err)
			return
		}
		bin := filepath.Join(dir, "weld")
		cmd := exec.CommandContext(buildCtx, "go", "build", "-o", bin, ".") //nolint:gosec // live harness builds fixed cmd/weld package
		cmd.Dir = filepath.Join(root, "cmd", "weld")
		cmd.Env = append(os.Environ(), "GOWORK=off")
		out, err := cmd.CombinedOutput()
		if err != nil {
			errBuildWeld = fmt.Errorf("helpers: build weld: %w\n%s", err, string(out))
			return
		}
		buildWeldBin = bin
	})
	if errBuildWeld != nil {
		t.Fatalf("%v", errBuildWeld)
	}
	if buildWeldBin == "" {
		t.Fatalf("helpers: weld build path empty after success")
	}
	return buildWeldBin
}

// EnvWithDomain returns the current environment with ROS_DOMAIN_ID set.
func EnvWithDomain(domain int) []string {
	base := os.Environ()
	out := make([]string, 0, len(base)+1)
	for _, kv := range base {
		if !strings.HasPrefix(kv, "ROS_DOMAIN_ID=") {
			out = append(out, kv)
		}
	}
	out = append(out, fmt.Sprintf("ROS_DOMAIN_ID=%d", domain))
	return out
}

// RunWeldEcho runs weld echo and returns captured stdout and stderr.
//
// The echo timeout bounds the run. EchoMargin lets weld exit on its
// own timeout and flush before the outer context fires. The echo requests
// volatile durability, matching ros2 topic echo interop.
func RunWeldEcho(ctx context.Context, bin, topic string, domain int, echoTimeout time.Duration) (string, string, error) {
	return RunWeldEchoWithDurability(ctx, bin, topic, domain, echoTimeout, "volatile")
}

// RunWeldEchoWithDurability runs weld echo with explicit requested durability.
//
// Transient local refuses volatile writers through peer side RxO matching,
// which is how QoS gates prove incompatible pairs stay silent. All other
// callers keep volatile through RunWeldEcho.
func RunWeldEchoWithDurability(ctx context.Context, bin, topic string, domain int, echoTimeout time.Duration, durability string) (string, string, error) {
	runCtx, cancel := context.WithTimeout(ctx, echoTimeout+EchoMargin)
	defer cancel()
	cmd := exec.CommandContext(runCtx, bin, "echo", "--topic", topic, "--domain", strconv.Itoa(domain), "--timeout", echoTimeout.String(), "--qos-durability", durability) //nolint:gosec // live harness runs built weld binary with test-controlled args
	cmd.Env = EnvWithDomain(domain)
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	if err != nil {
		return stdout.String(), stderr.String(), err
	}
	return stdout.String(), stderr.String(), nil
}

// RunWeldPub runs weld pub once and returns captured output.
//
// Weld pub waits for SPDP routes internally before publishing, so the
// caller only needs a generous outer timeout and no extra retry loop.
func RunWeldPub(ctx context.Context, bin, topic string, domain int, text string) (string, string, error) {
	runCtx, cancel := context.WithTimeout(ctx, PubTimeout)
	defer cancel()
	cmd := exec.CommandContext(runCtx, bin, "pub", "--topic", topic, "--domain", strconv.Itoa(domain), "--text", text) //nolint:gosec // live harness runs built weld binary with test-controlled args
	cmd.Env = EnvWithDomain(domain)
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	if err != nil {
		return stdout.String(), stderr.String(), err
	}
	return stdout.String(), stderr.String(), nil
}

// Sleep waits for d or ctx cancellation, whichever ends first.
func Sleep(ctx context.Context, d time.Duration) {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
	case <-timer.C:
	}
}

// SettleAfterJoin waits JoinSettle or ctx cancellation.
//
// Call it after JoinMulticast before sending so IGMP membership and the
// peer stack quiesce. It replaces the legacy 2s inline sleep that flaked
// under load with the documented JoinSettle constant.
func SettleAfterJoin(ctx context.Context) {
	Sleep(ctx, JoinSettle)
}

// SettleAfterAnnounce waits AnnounceSettle or ctx cancellation.
//
// Call it after the first SPDP announce before capturing so repeats land
// even on a loaded host. It replaces the legacy 2s inline sleep that
// missed repeats with the documented AnnounceSettle constant.
func SettleAfterAnnounce(ctx context.Context) {
	Sleep(ctx, AnnounceSettle)
}

// WaitForFileContains polls path until it holds substr or time runs out.
func WaitForFileContains(ctx context.Context, path, substr string, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for {
		data, err := os.ReadFile(path) //nolint:gosec // live harness polls test-controlled temp file path
		if err == nil {
			if strings.Contains(string(data), substr) {
				return true
			}
		}
		if time.Now().After(deadline) {
			return false
		}
		if ctx.Err() != nil {
			return false
		}
		timer := time.NewTimer(FilePollInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return false
		case <-timer.C:
		}
	}
}

// StopCmd kills a started docker exec helper and waits for exit.
func StopCmd(cmd *exec.Cmd) {
	if cmd == nil {
		return
	}
	if cmd.Process == nil {
		return
	}
	_ = cmd.Process.Kill() //nolint:errcheck // best-effort test cleanup, kill failure is not fatal
	_ = cmd.Wait()         //nolint:errcheck // best-effort test cleanup, wait failure is not fatal
}

// KillAndWaitContainerProc kills container procs matching pattern and
// verifies they are gone, all in one docker exec.
//
// Splitting kill and verify across exec calls races docker latency under
// load: the kill may land late while per-call timeouts expire, producing
// false survived verdicts. One server-side script avoids that: pkill,
// then a bounded poll, then SIGKILL escalation, then a final check. The
// pattern travels in the WPAT env var so neither the local docker argv
// nor the container shell ever contains the literal pattern as its own
// command line. It returns nil only when no matching proc remains.
// A missing proc is success, not an error.
func KillAndWaitContainerProc(ctx context.Context, container, pattern string) error {
	const script = `pkill -f "$WPAT"; for i in $(seq 1 12); do pgrep -f "$WPAT" >/dev/null || exit 0; sleep 0.5; done; pkill -9 -f "$WPAT"; for i in $(seq 1 4); do pgrep -f "$WPAT" >/dev/null || exit 0; sleep 0.5; done; exit 1`
	args := []string{"exec", "-e", "WPAT=" + pattern, container, "sh", "-c", script}
	cmd := exec.CommandContext(ctx, "docker", args...) //nolint:gosec // live harness kills only test-owned peer procs
	if out, err := cmd.CombinedOutput(); err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) && exitErr.ExitCode() == 1 {
			return fmt.Errorf("helpers: container proc survived kill pattern %q: %w", pattern, err)
		}
		return fmt.Errorf("helpers: container kill verify inconclusive pattern %q output %q: %w", pattern, string(out), err)
	}
	return nil
}

// KillContainerProc best-effort kills container procs matching pattern.
//
// The host docker exec process is stopped by the caller. This only handles
// a container side survivor, scoped to this run by the live ID, so it can
// never signal another run. Failures are ignored because a missing pkill
// or an already exited proc is not an error here.
func KillContainerProc(ctx context.Context, container string, domain int, pattern string) {
	args := DockerArgs(container, domain, "pkill", "-f", pattern)
	cmd := exec.CommandContext(ctx, "docker", args...) //nolint:gosec // live harness invokes docker with fixed args from test tables
	_ = cmd.Run()                                      //nolint:errcheck // best-effort pkill, missing proc is not an error
}

// KillRosDaemon stops ros2 CLI daemon procs for one domain best effort.
//
// Service verbs have no --no-daemon flag, so service calls leave a daemon
// participant behind that breaks isolation tests on reused domains. The
// pkill pattern carries a trailing space so domain 7 never matches 70,
// scoping the kill to this domain only so parallel workers elsewhere are
// never signaled. Failures are ignored because an absent daemon is not
// an error.
func KillRosDaemon(ctx context.Context, container string, domain int) {
	KillContainerProc(ctx, container, domain, "daemonize.*ros-domain-id "+strconv.Itoa(domain)+" ")
}

// DockerExec runs one docker argv command with a timeout.
func DockerExec(ctx context.Context, args ...string) ([]byte, error) {
	cmd := exec.CommandContext(ctx, "docker", args...) //nolint:gosec // test harness invokes docker CLI with variable args from fixed test tables
	out, err := cmd.CombinedOutput()
	if err != nil {
		return out, fmt.Errorf("helpers: docker %v: %w output %s", args, err, string(out))
	}
	return out, nil
}

// MustNewPublisher returns a Publisher or fails the test.
//
// It is the shared harness constructor for live and unit tests. The dds
// package tests keep a local mustNewPublisher helper because they cannot
// import this package without a cycle; both helpers do the same check.
func MustNewPublisher(t *testing.T, guid dds.WriterGUID, sink dds.Writer) *dds.Publisher {
	t.Helper()
	pub, err := dds.NewPublisher(guid, sink)
	if err != nil {
		t.Fatalf("new publisher: %v", err)
	}
	return pub
}

// ContainsSPDP reports whether raw holds SPDP writer DATA.
func ContainsSPDP(raw []byte) bool {
	var drops rtps.Counter
	_, raws, err := rtps.DecodeMessage(raw, &drops)
	if err != nil {
		return false
	}
	for i := range raws {
		sub, err := rtps.DecodeSubmessage(raws[i], &drops)
		if err != nil {
			continue
		}
		if sub.Data == nil {
			continue
		}
		if sub.Data.WriterID != rtps.EntitySPDPWriter {
			continue
		}
		return true
	}
	return false
}

// CheckWire pins version 2.1 plus vendor plus DATA routing.
//
// It asserts the RTPS header major and minor, the Cyclone vendor ID, and
// that every SPDP DATA submessage targets the unknown reader. Drops must
// be zero for a clean live datagram. Domain and runID scope every failure
// so parallel live runs triage to one domain and one per-run ID.
func CheckWire(t *testing.T, raw []byte, domain int, runID string) {
	t.Helper()
	var drops rtps.Counter
	hdr, raws, err := rtps.DecodeMessage(raw, &drops)
	if err != nil {
		t.Fatalf("decode live message domain %d run %s: %v", domain, runID, err)
	}
	if hdr.Major != 2 {
		t.Fatalf("live major domain %d run %s = %d want 2", domain, runID, hdr.Major)
	}
	if hdr.Minor != 1 {
		t.Fatalf("live minor domain %d run %s = %d want 1", domain, runID, hdr.Minor)
	}
	wantVendor := [2]byte{0x01, 0x10}
	if hdr.Vendor != wantVendor {
		t.Fatalf("live vendor domain %d run %s = %v want 01 10", domain, runID, hdr.Vendor)
	}
	found := false
	for i := range raws {
		sub, err := rtps.DecodeSubmessage(raws[i], &drops)
		if err != nil {
			t.Fatalf("decode live submessage %d domain %d run %s: %v", i, domain, runID, err)
		}
		if sub.Data == nil {
			continue
		}
		if sub.Data.WriterID != rtps.EntitySPDPWriter {
			continue
		}
		if sub.Data.ReaderID != rtps.EntityUnknown {
			t.Fatalf("live reader domain %d run %s = %v want unknown", domain, runID, sub.Data.ReaderID)
		}
		found = true
	}
	if !found {
		t.Fatalf("live datagram misses SPDP DATA domain %d run %s", domain, runID)
	}
	if drops.Count() != 0 {
		t.Fatalf("live drops domain %d run %s = %d want 0", domain, runID, drops.Count())
	}
}
