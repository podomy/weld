// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package helpers

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/podomy/weld/internal/transports"
	"github.com/podomy/weld/internal/transports/dds"
	"github.com/podomy/weld/internal/types"
)

// ReadFloat64Triple reads three float64 values with labeled errors.
//
// The labels scope each read error so pose, twist, and marker callers keep
// distinct messages while sharing one read loop with no duplication.
func ReadFloat64Triple(dec *types.Decoder, labelX, labelY, labelZ string) (float64, float64, float64, error) {
	x, err := dec.ReadFloat64()
	if err != nil {
		return 0, 0, 0, fmt.Errorf("%s: %w", labelX, err)
	}
	y, err := dec.ReadFloat64()
	if err != nil {
		return 0, 0, 0, fmt.Errorf("%s: %w", labelY, err)
	}
	z, err := dec.ReadFloat64()
	if err != nil {
		return 0, 0, 0, fmt.Errorf("%s: %w", labelZ, err)
	}
	return x, y, z, nil
}

// ReadFloat64Quad reads four float64 values with labeled errors.
//
// The labels scope each read error so pose, quaternion, and marker callers
// keep distinct messages while sharing one read loop with no duplication.
func ReadFloat64Quad(dec *types.Decoder, labelX, labelY, labelZ, labelW string) (float64, float64, float64, float64, error) {
	x, err := dec.ReadFloat64()
	if err != nil {
		return 0, 0, 0, 0, fmt.Errorf("%s: %w", labelX, err)
	}
	y, err := dec.ReadFloat64()
	if err != nil {
		return 0, 0, 0, 0, fmt.Errorf("%s: %w", labelY, err)
	}
	z, err := dec.ReadFloat64()
	if err != nil {
		return 0, 0, 0, 0, fmt.Errorf("%s: %w", labelZ, err)
	}
	w, err := dec.ReadFloat64()
	if err != nil {
		return 0, 0, 0, 0, fmt.Errorf("%s: %w", labelW, err)
	}
	return x, y, z, w, nil
}

// ProbeDockerHelp runs one docker help argv and returns combined output.
//
// The wait bounds the probe through context. The label scopes the wrapped
// error so matrix, shape, and QoS callers keep distinct messages while
// sharing one probe path with no shell.
func ProbeDockerHelp(ctx context.Context, argv []string, wait time.Duration, label string) (string, error) {
	probeCtx, cancel := context.WithTimeout(ctx, wait)
	defer cancel()
	cmd := exec.CommandContext(probeCtx, "docker", argv...) //nolint:gosec // live harness probes fixed ros2 help argv with no shell
	out, err := cmd.CombinedOutput()
	if err != nil {
		return string(out), fmt.Errorf("helpers: %s help probe: %w", label, err)
	}
	return string(out), nil
}

// OpenLiveBackend opens a Weld backend and joins discovery.
//
// It dials the live DDS backend on the given domain and joins with the
// given participant name. The domain must sit in 0 through 101. The label
// scopes wrapped errors so matrix, shape, and liveliness callers keep
// distinct messages. The caller owns Close. A join failure closes the
// backend before returning so no half joined backend leaks.
func OpenLiveBackend(ctx context.Context, domain int, participant, label string) (*dds.Backend, error) {
	if domain < 0 || domain > 101 {
		return nil, fmt.Errorf("helpers: %s domain %d out of range 0 through 101", label, domain)
	}
	cfg := dds.SidecarConfig{
		DomainID:    uint32(domain),
		PeerAddress: dds.DefaultPeerAddress,
		Range:       transports.DiscoveryRangeSystemDefault,
	}
	backend, err := dds.NewBackend(cfg)
	if err != nil {
		return nil, fmt.Errorf("helpers: %s open backend: %w", label, err)
	}
	info := transports.DiscoveryInfo{
		ParticipantName: participant,
		DomainID:        uint32(domain),
		Range:           transports.DiscoveryRangeSystemDefault,
	}
	err = backend.JoinDiscovery(ctx, info)
	if err != nil {
		cerr := backend.Close() //nolint:contextcheck // backend Close is cleanup without caller ctx, goodbye uses bounded Background
		if cerr != nil {
			return nil, fmt.Errorf("helpers: %s join discovery: %w", label, errors.Join(err, cerr))
		}
		return nil, fmt.Errorf("helpers: %s join discovery: %w", label, err)
	}
	return backend, nil
}

// MustOpenLiveBackend opens a backend or fails the test.
//
// It wraps OpenLiveBackend with t.Fatalf and registers backend Close in
// t.Cleanup. The domain is uint32 to match the liveliness gates that use
// ephemeral domain constants. Close runs without caller context, so the
// cleanup carries a contextcheck exemption with bounded Background goodbye.
func MustOpenLiveBackend(t *testing.T, ctx context.Context, domain uint32, participant, label string) *dds.Backend {
	t.Helper()
	backend, err := OpenLiveBackend(ctx, int(domain), participant, label)
	if err != nil {
		t.Fatalf("new backend domain %d: %v", domain, err)
	}
	t.Cleanup(func() { //nolint:contextcheck // backend Close is cleanup without caller ctx, goodbye uses bounded Background
		cerr := backend.Close()
		if cerr != nil {
			t.Logf("backend close: %v", cerr)
		}
	})
	return backend
}

// PublishLiveWithRetry publishes once, retrying ErrNoRoute briefly.
//
// SPDP exchange needs time after JoinDiscovery, so the first publish can
// see empty routes. Only ErrNoRoute retries. Other errors return at once.
// The wait is bounded by retryTimeout with retryInterval sleeps. The label
// scopes wrapped errors so callers keep distinct messages.
func PublishLiveWithRetry(ctx context.Context, backend *dds.Backend, msg transports.OutgoingMessage, retryTimeout, retryInterval time.Duration, label string) error {
	start := time.Now()
	for {
		err := backend.PublishBytes(ctx, msg)
		if err == nil {
			return nil
		}
		if !errors.Is(err, dds.ErrNoRoute) {
			return fmt.Errorf("helpers: %s publish bytes: %w", label, err)
		}
		if time.Since(start) >= retryTimeout {
			return fmt.Errorf("helpers: %s publish bytes: %w", label, err)
		}
		cerr := ctx.Err()
		if cerr != nil {
			return fmt.Errorf("helpers: %s publish context: %w", label, cerr)
		}
		timer := time.NewTimer(retryInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			cerr = ctx.Err()
			if cerr != nil {
				return fmt.Errorf("helpers: %s publish wait: %w", label, cerr)
			}
			return fmt.Errorf("helpers: %s publish bytes: %w", label, err)
		case <-timer.C:
		}
	}
}

// IsValidRIHSHash reports whether s is an RIHS01 hash string.
//
// The form is RIHS01 plus underscore plus 64 lowercase hex digits.
func IsValidRIHSHash(s string) bool {
	const wantLen = 7 + 64
	if len(s) != wantLen {
		return false
	}
	if !strings.HasPrefix(s, "RIHS01_") {
		return false
	}
	for _, c := range s[7:] {
		isHex := (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')
		if !isHex {
			return false
		}
	}
	return true
}

// ParseTopicHash extracts the topic type hash from ros2 topic info -v output.
//
// It scans for a line holding Topic type hash and returns the trimmed
// value. A missing line reports an error so the caller can fall back to
// the static hash. The label scopes the error message.
func ParseTopicHash(out, label string) (string, error) {
	for line := range strings.SplitSeq(out, "\n") {
		trimmed := strings.TrimSpace(line)
		const prefix = "Topic type hash:"
		if !strings.HasPrefix(trimmed, prefix) {
			continue
		}
		hash := strings.TrimSpace(strings.TrimPrefix(trimmed, prefix))
		if !IsValidRIHSHash(hash) {
			return "", fmt.Errorf("helpers: %s parse hash %q invalid", label, hash)
		}
		return hash, nil
	}
	return "", errors.New("helpers: parse finds no Topic type hash in output") //nolint:wrapcheck // new error, nothing to wrap
}

// ProbeTopicHash queries the live hash via ros2 topic info -v.
//
// It runs docker argv with no shell on the given domain with a probe
// timeout. A probe failure returns an error for fallback handling.
func ProbeTopicHash(ctx context.Context, container string, domain int, rosBin, topic string, probeWait time.Duration, label string) (string, error) {
	probeCtx, cancel := context.WithTimeout(ctx, probeWait)
	defer cancel()
	args := DockerArgs(container, domain, rosBin, "topic", "info", "-v", topic)
	cmd := exec.CommandContext(probeCtx, "docker", args...) //nolint:gosec // test harness runs docker with controlled args
	out, err := cmd.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("helpers: %s probe topic info %s: %w output %s", label, topic, err, string(out))
	}
	hash, err := ParseTopicHash(string(out), label)
	if err != nil {
		return "", fmt.Errorf("helpers: %s probe parse %s: %w output %s", label, topic, err, string(out))
	}
	return hash, nil
}

// ResolveTopicHash returns the live hash or the static fallback.
//
// It probes the live topic for the hash that CycloneDDS advertises. When
// the probe succeeds the live value wins so distro changes are picked up.
// When the probe fails the static hash is used. When both are missing it
// returns an error so the caller can skip cleanly.
func ResolveTopicHash(ctx context.Context, container string, domain int, rosBin, topic, staticHash string, probeWait time.Duration, label string) (string, error) {
	live, err := ProbeTopicHash(ctx, container, domain, rosBin, topic, probeWait, label)
	if err == nil {
		return live, nil
	}
	if staticHash != "" && IsValidRIHSHash(staticHash) {
		return staticHash, nil
	}
	return "", fmt.Errorf("helpers: %s hash unavailable for topic %s: %w", label, topic, err)
}

// Topic discovery bounds for direct take tests.
//
// Direct take tests subscribe then take at once, so the first take can run
// before unicast SEDP delivers the ROS writer. Multicast receive is
// send-only, so writer discovery arrives only over unicast after SPDP
// routes settle. The waiter below polls for the topic with this interval
// up to this timeout, mirroring publishWithRouteWait and waitForEchoDiscovery.
const (
	// topicDiscoveryInterval spaces polls while waiting for a remote writer.
	topicDiscoveryInterval = 50 * time.Millisecond
	// topicDiscoveryTimeout bounds the wait for a remote writer.
	topicDiscoveryTimeout = 5 * time.Second
)

// WaitForTopicDiscovery polls until a remote endpoint for topic appears.
//
// It returns true when HasRemoteEndpoint reports the topic before the
// timeout, and false on timeout or context end. The wait is best effort:
// callers still take after a timeout so a missing peer surfaces as the
// known interop gap rather than a subscribe failure. It mirrors
// publishWithRouteWait polling with a ctx-aware timer.
func WaitForTopicDiscovery(ctx context.Context, backend *dds.Backend, topic string) bool {
	if backend == nil {
		return false
	}
	if backend.HasRemoteEndpoint(topic) {
		return true
	}
	start := time.Now()
	for {
		if time.Since(start) >= topicDiscoveryTimeout {
			return backend.HasRemoteEndpoint(topic)
		}
		err := ctx.Err()
		if err != nil {
			return false
		}
		timer := time.NewTimer(topicDiscoveryInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return false
		case <-timer.C:
		}
		if backend.HasRemoteEndpoint(topic) {
			return true
		}
	}
}

// SubscribeLiveReader declares a local reader for topic and IDL type.
//
// The caller supplies the RIHS01 type hash from live probe or static
// fallback. CycloneDDS matching requires hash equality, so an empty hash
// never matches a ROS writer that advertises the real hash. After the
// subscribe it waits best effort for the remote writer with
// WaitForTopicDiscovery so direct take tests do not take before unicast
// SEDP arrives. The wait never fails the subscribe: a timeout still
// returns nil and the take path reports the gap.
func SubscribeLiveReader(ctx context.Context, backend *dds.Backend, topic, idlName, typeHash, label string) error {
	err := backend.Subscribe(ctx, topic, idlName, typeHash)
	if err != nil {
		return fmt.Errorf("helpers: %s subscribe %s: %w", label, topic, err)
	}
	WaitForTopicDiscovery(ctx, backend, topic)
	return nil
}

// SkipIfNoTypeInterface skips when the peer lacks the ROS type.
//
// It runs ros2 interface show via docker argv with no shell. A non-zero
// exit means the container does not carry the package, so the caller skips
// cleanly instead of failing the matrix.
func SkipIfNoTypeInterface(t *testing.T, ctx context.Context, container string, domain int, rosBin, rosType string, probeWait time.Duration) {
	t.Helper()
	probeCtx, cancel := context.WithTimeout(ctx, probeWait)
	defer cancel()
	args := DockerArgs(container, domain, rosBin, "interface", "show", rosType)
	cmd := exec.CommandContext(probeCtx, "docker", args...) //nolint:gosec // test harness runs docker with controlled args
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Skipf("ros2 interface show %s absent, skipping: %v output %s", rosType, err, string(out))
	}
}

// CreateLiveLogFile creates a log file and registers cleanup close.
//
// The path is typically under t.TempDir. The cleanup logs but never fails
// the test on close because close errors after a pass must not flip the
// result.
func CreateLiveLogFile(t *testing.T, path string) *os.File {
	t.Helper()
	logFile, err := os.Create(path) //nolint:gosec // live harness writes controlled temp file path
	if err != nil {
		t.Fatalf("create log %s: %v", path, err)
	}
	t.Cleanup(func() {
		cerr := logFile.Close()
		if cerr != nil {
			t.Logf("close log %s: %v", path, cerr)
		}
	})
	return logFile
}

// StartDockerLogged starts a docker argv proc with output to the log file.
//
// Stdout and stderr both append to logFile. The caller owns StopCmd for
// the returned command. A start failure fails the test with the label.
func StartDockerLogged(t *testing.T, ctx context.Context, args []string, logFile *os.File, label string) *exec.Cmd {
	t.Helper()
	cmd := exec.CommandContext(ctx, "docker", args...) //nolint:gosec // live harness runs docker with controlled test args
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	err := cmd.Start()
	if err != nil {
		t.Fatalf("%s start: %v", label, err)
	}
	return cmd
}

// AssertNoBackendError fails when combined output holds a backend gap.
//
// Weld echo prints not implemented or not wired only when the backend path
// is unwired. Live gates must fail loudly on those strings so a silent
// backend gap never passes as a skip.
func AssertNoBackendError(t *testing.T, name, combined string) {
	t.Helper()
	if strings.Contains(combined, "not implemented") {
		t.Fatalf("case %s: weld echo backend error:\n%s", name, combined)
	}
	if strings.Contains(combined, "not wired") {
		t.Fatalf("case %s: weld echo backend error:\n%s", name, combined)
	}
}

// PublishUntilEcho publishes until ros2 topic echo sees want.
//
// CycloneDDS discovers the Weld writer on its own schedule, so a single
// early publish can leave before matching completes and never arrive. It
// publishes with PublishLiveWithRetry, polls the echo file for one
// republish interval, and repeats until the overall poll wait ends. It
// reports true when the echo holds want. A publish without routes fails
// loudly as product news, while other publish failures still fail.
// The label scopes fatal messages so matrix, shape, and stereo
// callers keep distinct output while sharing one retry loop.
func PublishUntilEcho(ctx context.Context, t *testing.T, backend *dds.Backend, msg transports.OutgoingMessage, echoPath, want, typeHash string, pollWait, repubWait, retryTimeout, retryInterval time.Duration, label string) bool {
	t.Helper()
	deadline := time.Now().Add(pollWait)
	for {
		err := PublishLiveWithRetry(ctx, backend, msg, retryTimeout, retryInterval, label)
		if err != nil {
			if strings.Contains(err.Error(), "no route") {
				t.Fatalf("weld pub %s hash %s has no route, want route: %v", label, typeHash, err)
			}
			t.Fatalf("weld pub %s: %v", label, err)
		}
		if WaitForFileContains(ctx, echoPath, want, repubWait) {
			return true
		}
		if ctx.Err() != nil {
			return false
		}
		if time.Now().After(deadline) {
			return false
		}
	}
}

// Peer teardown bound for event-driven waits.
//
// PeerStopTimeout caps StopPeerProcAndWait. It matches the legacy 10s
// cleanup context literal used at every gates call site, so worst case
// teardown timing is identical while the common case returns as soon as
// the peer proc exits. TalkerSettle and EchoSettle in helpers.go stay as
// the upper bounds for WaitForPeerReady callers: call sites pass those
// constants explicitly, so assertion timing semantics are unchanged.
// Polling reuses FilePollInterval, DockerArgs, StopPeerProc, and
// KillContainerProc instead of new machinery. StopCmd is not reused here
// because it serves host-side StartDockerLogged procs while gates use
// detached StartPeerProc procs that need container-side kill plus pkill.
const PeerStopTimeout = 10 * time.Second

// WaitForPeerReady polls until the detached peer proc writes its pidfile.
//
// It returns true when hostPidPath holds a non-blank PID before the
// timeout, and false on timeout or context end. The timeout is an upper
// bound only: callers pass TalkerSettle for talkers and EchoSettle for
// echo readers, so worst case timing matches the previous Sleep while the
// common case returns as soon as the container sh script records the PID.
// A false return never fails the caller: the capture or echo path still
// runs and reports the gap, so assertion timing semantics are unchanged.
func WaitForPeerReady(ctx context.Context, hostPidPath string, timeout time.Duration) bool {
	if hostPidPath == "" {
		return false
	}
	if timeout <= 0 {
		timeout = TalkerSettle
	}
	if peerPidReady(hostPidPath) {
		return true
	}
	deadline := time.Now().Add(timeout)
	for {
		if time.Now().After(deadline) {
			return peerPidReady(hostPidPath)
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
		if peerPidReady(hostPidPath) {
			return true
		}
	}
}

// peerPidReady reports whether path holds a non-blank PID.
func peerPidReady(path string) bool {
	data, err := os.ReadFile(path) //nolint:gosec // live harness polls test-controlled pidfile path
	if err != nil {
		return false
	}
	return strings.TrimSpace(string(data)) != ""
}

// StopPeerProcAndWait stops one detached peer run and waits for exit.
//
// It calls StopPeerProc (pidfile kill plus scoped pkill) then polls
// kill -0 for the pidfile PID until the proc exits or PeerStopTimeout
// expires. The kill -0 probe sends no signal and only tests PID presence
// with the same kill binary StopPeerProc already uses. Missing or empty
// pidfiles return at once. After the wait it rechecks the pidfile: a
// late pidfile write can appear after the first kill missed, so a present
// PID triggers one second kill plus scoped pkill. It is best effort
// cleanup and never fails the test: a surviving proc is left for the
// scoped pkill already sent.
func StopPeerProcAndWait(ctx context.Context, container string, domain int, hostPidPath, pkillPattern string) {
	stopCtx, cancel := context.WithTimeout(ctx, PeerStopTimeout)
	defer cancel()
	StopPeerProc(stopCtx, container, domain, hostPidPath, pkillPattern)
	waitForPidExit(stopCtx, container, domain, hostPidPath)
	recheckPeerProcMiss(stopCtx, container, domain, hostPidPath, pkillPattern)
}

// recheckPeerProcMiss retries kill plus pkill when a late pidfile appears.
//
// The first StopPeerProc can miss when the container sh script writes the
// pidfile after the read. After the wait this re-reads the pidfile and,
// when a PID is present and still live, issues one second pidfile kill
// plus scoped pkill. Missing or exited procs are a no-op.
func recheckPeerProcMiss(ctx context.Context, container string, domain int, hostPidPath, pkillPattern string) {
	if hostPidPath == "" {
		return
	}
	pid, err := readPeerPid(hostPidPath)
	if err != nil {
		return
	}
	if pid == "" {
		return
	}
	if peerPidGone(ctx, container, domain, pid) {
		return
	}
	killArgs := DockerArgs(container, domain, "kill", pid)
	killCmd := exec.CommandContext(ctx, "docker", killArgs...) //nolint:gosec // live harness invokes docker with fixed args from test tables
	_ = killCmd.Run()                                          //nolint:errcheck // best-effort cleanup, missing proc is not an error
	if pkillPattern != "" {
		KillContainerProc(ctx, container, domain, pkillPattern)
	}
}

// waitForPidExit polls kill -0 until the pidfile PID exits or ctx ends.
func waitForPidExit(ctx context.Context, container string, domain int, hostPidPath string) {
	if hostPidPath == "" {
		return
	}
	for {
		pid, err := readPeerPid(hostPidPath)
		if err != nil {
			return
		}
		if pid == "" {
			return
		}
		if peerPidGone(ctx, container, domain, pid) {
			return
		}
		if ctx.Err() != nil {
			return
		}
		timer := time.NewTimer(FilePollInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

// readPeerPid returns the trimmed pidfile PID.
func readPeerPid(path string) (string, error) {
	data, err := os.ReadFile(path) //nolint:gosec // live harness reads test-controlled pidfile path
	if err != nil {
		return "", fmt.Errorf("helpers: read peer pidfile: %w", err)
	}
	return strings.TrimSpace(string(data)), nil
}

// peerPidGone probes one container PID with kill -0.
//
// It builds argv with DockerArgs like StopPeerProc and runs with no shell.
// A non-zero exit means the PID is gone. Probe failures count as gone
// because teardown is best effort and must never block the cleanup budget.
func peerPidGone(ctx context.Context, container string, domain int, pid string) bool {
	if pid == "" {
		return true
	}
	args := DockerArgs(container, domain, "kill", "-0", pid)
	cmd := exec.CommandContext(ctx, "docker", args...) //nolint:gosec // live harness invokes docker with fixed args from test tables
	err := cmd.Run()
	return err != nil
}
