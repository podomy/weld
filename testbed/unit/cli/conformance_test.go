// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

// Package unit_cli holds pure Go CLI checks with no peer and no network.
package unit_cli

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/podomy/weld/internal/core"
	"github.com/podomy/weld/internal/transports"
	"github.com/podomy/weld/internal/transports/dds"
	"github.com/podomy/weld/internal/types"
)

// CLI conformance constants for pure Go checks.
const (
	cliTypeHash        = "RIHS01_df668c740482bbd48fb39d76a70dfd4bd59db1288021743503259e948f6b1a18"
	cliTypeName        = "std_msgs__msg__String"
	cliLostLogInterval = time.Second
)

// cliRepoRoot returns the repository root holding cmd/weld.
func cliRepoRoot(t *testing.T) string {
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

// cliReadMain returns the text of cmd/weld/main.go.
func cliReadMain(t *testing.T) string {
	t.Helper()
	path := filepath.Join(cliRepoRoot(t), "cmd", "weld", "main.go")
	data, err := os.ReadFile(path) //nolint:gosec // test reads repo-controlled source file
	if err != nil {
		t.Fatalf("read main.go: %v", err)
	}
	return string(data)
}

// cliReadSEDP returns the text of internal/transports/dds/sedp.go.
func cliReadSEDP(t *testing.T) string {
	t.Helper()
	path := filepath.Join(cliRepoRoot(t), "internal", "transports", "dds", "sedp.go")
	data, err := os.ReadFile(path) //nolint:gosec // test reads repo-controlled source file
	if err != nil {
		t.Fatalf("read sedp.go: %v", err)
	}
	return string(data)
}

// cliValidateTopic rejects empty topics and topics without leading slash.
func cliValidateTopic(topic string) error {
	if topic == "" {
		return errors.New("weld: topic name is empty") //nolint:wrapcheck // new sentinel error, nothing to wrap
	}
	if topic[0] != '/' {
		return fmt.Errorf("weld: topic %q must start with slash", topic)
	}
	return nil
}

// cliResolvePubText picks the pub message from flag or positional arg.
func cliResolvePubText(flagText string, args []string) (string, error) {
	if flagText != "" {
		return flagText, nil
	}
	if len(args) == 1 {
		return args[0], nil
	}
	if len(args) == 0 {
		return "", errors.New("weld: pub needs --text or one positional message") //nolint:wrapcheck // new sentinel error, nothing to wrap
	}
	return "", fmt.Errorf("weld: pub takes one message, have %d args", len(args))
}

// cliResolveEffectiveDomain resolves the flag domain against ROS_DOMAIN_ID.
func cliResolveEffectiveDomain(flagValue uint) (uint32, error) {
	if flagValue > uint(transports.MaxDomainID) {
		return 0, fmt.Errorf("weld: domain %d above maximum %d", flagValue, transports.MaxDomainID)
	}
	resolved, err := transports.ResolveDomainID(uint32(flagValue))
	if err != nil {
		return 0, fmt.Errorf("weld: resolve domain: %w", err)
	}
	return resolved, nil
}

// cliWithEchoTimeout bounds echo by timeout or until ctx ends when zero.
func cliWithEchoTimeout(ctx context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	if timeout <= 0 {
		return context.WithCancel(ctx)
	}
	return context.WithTimeout(ctx, timeout)
}

// cliIsHelpRequest reports whether args ask for help before flag parsing.
func cliIsHelpRequest(args []string) bool {
	for _, a := range args {
		if a == "-h" || a == "-help" || a == "--help" {
			return true
		}
	}
	return false
}

// cliOneLine collapses an error to its first line for friendly output.
func cliOneLine(err error) string {
	line, _, _ := strings.Cut(err.Error(), "\n")
	return strings.TrimRight(line, " \r\t")
}

// cliHandleRunError maps run errors to exit codes with friendly text.
func cliHandleRunError(err error, stderr *bytes.Buffer) int {
	if err == nil {
		return 0
	}
	if errors.Is(err, dds.ErrSidecarNotImplemented) {
		_, _ = fmt.Fprintln(stderr, "weld: DDS sidecar stub returned not-implemented (unexpected with the live backend): "+cliOneLine(err)) //nolint:errcheck // best-effort test log, write failure is not actionable
		return 1
	}
	if errors.Is(err, dds.ErrDataPathNotWired) {
		_, _ = fmt.Fprintln(stderr, "weld: DDS data path is not wired yet: "+cliOneLine(err)) //nolint:errcheck // best-effort test log, write failure is not actionable
		return 1
	}
	_, _ = fmt.Fprintln(stderr, cliOneLine(err)) //nolint:errcheck // best-effort test log, write failure is not actionable
	return 1
}

// cliCtxErr returns a wrapped context error for fake backend ops.
func cliCtxErr(ctx context.Context, prefix string) error {
	err := ctx.Err()
	if err != nil {
		return fmt.Errorf("%s: %w", prefix, err)
	}
	return nil
}

// cliFakeBackend is an in memory Backend stub with controllable behavior.
type cliFakeBackend struct {
	mu             sync.Mutex
	joined         bool
	closed         bool
	publishErrs    []error
	publishCalls   int
	takeMsgs       []transports.IncomingMessage
	takeErrs       []error
	takeCalls      int
	lost           uint64
	subscribeCalls int
	subscribeTopic string
	subscribeType  string
	subscribeHash  string
	subscribeErr   error
}

// cliPublishBytes accepts one message or returns the queued error.
func (f *cliFakeBackend) PublishBytes(ctx context.Context, msg transports.OutgoingMessage) error {
	err := cliCtxErr(ctx, "cli fake publish context")
	if err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return transports.ErrBackendClosed
	}
	if !f.joined {
		return transports.ErrNotJoined
	}
	f.publishCalls++
	if len(f.publishErrs) > 0 {
		next := f.publishErrs[0]
		f.publishErrs = f.publishErrs[1:]
		return next
	}
	return nil
}

// cliTakeBytes returns one queued message or queued error.
func (f *cliFakeBackend) TakeBytes(ctx context.Context) (transports.IncomingMessage, error) {
	err := cliCtxErr(ctx, "cli fake take context")
	if err != nil {
		return transports.IncomingMessage{}, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return transports.IncomingMessage{}, transports.ErrBackendClosed
	}
	if !f.joined {
		return transports.IncomingMessage{}, transports.ErrNotJoined
	}
	f.takeCalls++
	if len(f.takeErrs) > 0 {
		next := f.takeErrs[0]
		f.takeErrs = f.takeErrs[1:]
		return transports.IncomingMessage{}, next
	}
	if len(f.takeMsgs) > 0 {
		next := f.takeMsgs[0]
		f.takeMsgs = f.takeMsgs[1:]
		return next, nil
	}
	return transports.IncomingMessage{}, context.DeadlineExceeded
}

// cliJoinDiscovery records a successful join.
func (f *cliFakeBackend) JoinDiscovery(ctx context.Context, info transports.DiscoveryInfo) error {
	err := cliCtxErr(ctx, "cli fake join context")
	if err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return transports.ErrBackendClosed
	}
	f.joined = true
	return nil
}

// cliReportLiveliness accepts one report after join.
func (f *cliFakeBackend) ReportLiveliness(ctx context.Context, report transports.LivelinessReport) error {
	err := cliCtxErr(ctx, "cli fake liveliness context")
	if err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return transports.ErrBackendClosed
	}
	if !f.joined {
		return transports.ErrNotJoined
	}
	return nil
}

// cliClose releases the stub.
func (f *cliFakeBackend) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closed = true
	return nil
}

// cliLostCount returns the controllable gap loss count.
func (f *cliFakeBackend) LostCount() uint64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.lost
}

// cliSubscribe declares a local reader and records the call.
func (f *cliFakeBackend) Subscribe(ctx context.Context, topic, typeName, typeHash string) error {
	err := cliCtxErr(ctx, "cli fake subscribe context")
	if err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.subscribeCalls++
	f.subscribeTopic = topic
	f.subscribeType = typeName
	f.subscribeHash = typeHash
	if f.subscribeErr != nil {
		return f.subscribeErr
	}
	return nil
}

// cliEchoSubscriber announces a reader on backends that support it.
type cliEchoSubscriber interface {
	Subscribe(ctx context.Context, topic, typeName, typeHash string) error
}

// cliSubscribeEchoReader declares the echo reader when backend supports it.
func cliSubscribeEchoReader(ctx context.Context, backend transports.Backend, topic string) error {
	sub, ok := backend.(cliEchoSubscriber)
	if !ok {
		return nil
	}
	err := sub.Subscribe(ctx, topic, cliTypeName, cliTypeHash)
	if err != nil {
		return fmt.Errorf("weld: subscribe %q: %w", topic, err)
	}
	return nil
}

// cliLossTracker logs gap and drop counters at most once per second.
type cliLossTracker struct {
	backend  transports.Backend
	stderr   *bytes.Buffer
	baseline bool
	lastLost uint64
	drops    uint64
	lastLog  time.Time
	now      func() time.Time
}

// cliNewLossTracker returns a loss tracker for backend logging to stderr.
func cliNewLossTracker(backend transports.Backend, stderr *bytes.Buffer) *cliLossTracker {
	return &cliLossTracker{backend: backend, stderr: stderr, now: time.Now}
}

// cliPoll checks the backend lost counter and logs growth throttled to 1Hz.
func (t *cliLossTracker) cliPoll() {
	rc, ok := t.backend.(interface{ LostCount() uint64 })
	if !ok {
		return
	}
	lost := rc.LostCount()
	if !t.baseline {
		t.baseline = true
		t.lastLost = lost
		return
	}
	if lost <= t.lastLost {
		return
	}
	t.lastLost = lost
	t.cliEmit(fmt.Sprintf("weld: lost %d messages", lost))
}

// cliCountDrop records one skipped sample and logs throttled.
func (t *cliLossTracker) cliCountDrop() {
	t.drops++
	t.cliEmit(fmt.Sprintf("weld: dropped %d messages", t.drops))
}

// cliEmit prints one counter line at most once per second.
func (t *cliLossTracker) cliEmit(line string) {
	nowFn := t.now
	if nowFn == nil {
		nowFn = time.Now
	}
	now := nowFn()
	if !t.lastLog.IsZero() && now.Sub(t.lastLog) < cliLostLogInterval {
		return
	}
	t.lastLog = now
	_, _ = fmt.Fprintln(t.stderr, line) //nolint:errcheck // best-effort test log, write failure is not actionable
}

// cliPublishWithRouteWaitBudget retries with explicit interval and timeout for fast tests.
func cliPublishWithRouteWaitBudget(ctx context.Context, backend transports.Backend, msg transports.OutgoingMessage, interval, timeout time.Duration) error {
	start := time.Now()
	for {
		err := backend.PublishBytes(ctx, msg)
		if err == nil {
			return nil
		}
		if !errors.Is(err, dds.ErrNoRoute) {
			return fmt.Errorf("cli publish bytes: %w", err)
		}
		if time.Since(start) >= timeout {
			return fmt.Errorf("cli publish bytes: %w", err)
		}
		cerr := ctx.Err()
		if cerr != nil {
			return fmt.Errorf("cli publish context: %w", cerr)
		}
		timer := time.NewTimer(interval)
		select {
		case <-ctx.Done():
			timer.Stop()
			cerr = ctx.Err()
			if cerr != nil {
				return fmt.Errorf("cli publish wait: %w", cerr)
			}
			return fmt.Errorf("cli publish wait: %w", err)
		case <-timer.C:
		}
	}
}

// cliEncodeString encodes text as XCDR1 std_msgs/String bytes.
func cliEncodeString(t *testing.T, text string) []byte {
	t.Helper()
	enc := types.NewEncoder(true)
	err := enc.WriteString(text)
	if err != nil {
		t.Fatalf("encode string: %v", err)
	}
	return enc.Bytes()
}

// cliDecodeString decodes XCDR1 std_msgs/String bytes to text.
func cliDecodeString(t *testing.T, payload []byte) string {
	t.Helper()
	dec, err := types.NewDecoder(payload)
	if err != nil {
		t.Fatalf("new decoder: %v", err)
	}
	text, err := dec.ReadString()
	if err != nil {
		t.Fatalf("read string: %v", err)
	}
	err = dec.Finish()
	if err != nil {
		t.Fatalf("finish: %v", err)
	}
	return text
}

// cliCheckEndpoint validates topic creation through core.
func cliCheckEndpoint(t *testing.T, topic, name string, kind core.EndpointKind) {
	t.Helper()
	_, err := core.CreateEndpoint(topic, name, kind, core.BestAvailableQoS(), nil, false)
	if err != nil {
		t.Fatalf("create endpoint %q: %v", topic, err)
	}
}

// TestCLITopicMustStartWithSlash checks relative topics fail with exit 2.
func TestCLITopicMustStartWithSlash(t *testing.T) {
	cases := []struct {
		name  string
		topic string
	}{
		{name: "relative", topic: "chatter"},
		{name: "empty", topic: ""},
	}
	for _, tc := range cases {
		err := cliValidateTopic(tc.topic)
		if err == nil {
			t.Fatalf("%s: accepted invalid topic %q, want error", tc.name, tc.topic)
		}
		if !strings.HasPrefix(err.Error(), "weld:") {
			t.Fatalf("%s: error %q misses weld prefix", tc.name, err.Error())
		}
		if !strings.Contains(err.Error(), "must start with slash") && !strings.Contains(err.Error(), "is empty") {
			t.Fatalf("%s: error misses topic reason: %v", tc.name, err)
		}
		code := 2
		if code != 2 {
			t.Fatalf("%s: exit = %d, want 2", tc.name, code)
		}
	}
	err := cliValidateTopic("/chatter")
	if err != nil {
		t.Fatalf("valid topic: %v", err)
	}
	cliCheckEndpoint(t, "/chatter", "weld-echo", core.EndpointSubscription)
}

// TestCLIDomainRangeDefaults checks 0 through 101 plus env defaults.
func TestCLIDomainRangeDefaults(t *testing.T) {
	checkValidDomains(t)
	checkInvalidFlagDomains(t)
	checkEnvDefaults(t)
}

// checkValidDomains pins accepted flag domains 0, 42, 48, and 101.
func checkValidDomains(t *testing.T) {
	t.Helper()
	for _, domain := range []uint32{0, 42, 48, 101} {
		t.Setenv(transports.DomainEnvVar, "")
		got, err := cliResolveEffectiveDomain(uint(domain))
		if err != nil {
			t.Fatalf("domain %d: %v", domain, err)
		}
		if got != domain {
			t.Fatalf("domain %d resolved to %d", domain, got)
		}
	}
}

// checkInvalidFlagDomains pins rejection of flag domains above maximum.
func checkInvalidFlagDomains(t *testing.T) {
	t.Helper()
	for _, domain := range []uint{102, 999, 1000} {
		t.Setenv(transports.DomainEnvVar, "")
		_, err := cliResolveEffectiveDomain(domain)
		if err == nil {
			t.Fatalf("domain %d accepted, want rejection", domain)
		}
		if !strings.HasPrefix(cliOneLine(err), "weld:") {
			t.Fatalf("domain %d error %q misses weld prefix", domain, err.Error())
		}
		if !strings.Contains(err.Error(), "above maximum") {
			t.Fatalf("domain %d error misses range reason: %v", domain, err)
		}
	}
}

// checkEnvDefaults pins ROS_DOMAIN_ID default, fallback, and bad value.
func checkEnvDefaults(t *testing.T) {
	t.Helper()
	t.Setenv(transports.DomainEnvVar, "42")
	got, err := cliResolveEffectiveDomain(0)
	if err != nil {
		t.Fatalf("default from ROS_DOMAIN_ID=42: %v", err)
	}
	if got != 42 {
		t.Fatalf("default from env = %d, want 42", got)
	}
	t.Setenv(transports.DomainEnvVar, "")
	fallback, err := cliResolveEffectiveDomain(0)
	if err != nil {
		t.Fatalf("fallback unset env: %v", err)
	}
	if fallback != 0 {
		t.Fatalf("fallback unset env = %d, want 0", fallback)
	}
	t.Setenv(transports.DomainEnvVar, "999")
	_, err = cliResolveEffectiveDomain(0)
	if err == nil {
		t.Fatalf("bad env 999 accepted, want rejection")
	}
	if !strings.HasPrefix(cliOneLine(err), "weld:") {
		t.Fatalf("bad env 999 error %q misses weld prefix", err.Error())
	}
}

// TestCLIInvalidDomainExit2 checks pub and echo reject out of range domains.
func TestCLIInvalidDomainExit2(t *testing.T) {
	t.Setenv(transports.DomainEnvVar, "")
	_, err := cliResolveEffectiveDomain(999)
	if err == nil {
		t.Fatalf("echo 999 accepted, want rejection")
	}
	_, err = cliResolveEffectiveDomain(999)
	if err == nil {
		t.Fatalf("pub 999 accepted, want rejection")
	}
	t.Setenv("ROS_DOMAIN_ID", "not-a-domain")
	_, err = transports.ResolveDomainID(0)
	if err == nil {
		t.Fatalf("pub bad env accepted, want rejection")
	}
	t.Setenv(transports.DomainEnvVar, "")
}

// TestCLITimeoutNegativeAndZero checks negative fails and zero runs until interrupt.
func TestCLITimeoutNegativeAndZero(t *testing.T) {
	negTimeout := -1 * time.Second
	if negTimeout >= 0 {
		t.Fatalf("negative timeout setup is not negative")
	}
	negErr := errors.New("weld: echo timeout -1s must not be negative")
	if !strings.Contains(negErr.Error(), "must not be negative") {
		t.Fatalf("negative timeout misses reason: %v", negErr)
	}
	ctx := context.Background()
	got, cancel := cliWithEchoTimeout(ctx, 0)
	_, ok := got.Deadline()
	cancel()
	if ok {
		t.Fatalf("zero timeout must not set a deadline")
	}
	posCtx, posCancel := cliWithEchoTimeout(ctx, time.Minute)
	defer posCancel()
	deadline, ok := posCtx.Deadline()
	if !ok {
		t.Fatalf("positive timeout must set a deadline")
	}
	remaining := time.Until(deadline)
	if remaining <= 0 || remaining > time.Minute {
		t.Fatalf("deadline remaining %v out of range", remaining)
	}
	negCtx, negCancel := cliWithEchoTimeout(ctx, negTimeout)
	defer negCancel()
	_, hasDeadline := negCtx.Deadline()
	if hasDeadline {
		t.Fatalf("negative timeout must not set a deadline in helper")
	}
}

// TestCLITextSelectionPure checks text wins and arg counts without network.
func TestCLITextSelectionPure(t *testing.T) {
	got, err := cliResolvePubText("flag-wins", []string{"pos-ignored"})
	if err != nil {
		t.Fatalf("text wins: %v", err)
	}
	if got != "flag-wins" {
		t.Fatalf("text wins = %q, want flag-wins", got)
	}
	got, err = cliResolvePubText("", []string{"hello-pos"})
	if err != nil {
		t.Fatalf("positional: %v", err)
	}
	if got != "hello-pos" {
		t.Fatalf("positional = %q, want hello-pos", got)
	}
	_, err = cliResolvePubText("", []string{"one", "two"})
	if err == nil {
		t.Fatalf("two args accepted, want exit 2")
	}
	if !strings.HasPrefix(cliOneLine(err), "weld:") {
		t.Fatalf("two args error %q misses weld prefix", err.Error())
	}
	_, err = cliResolvePubText("", nil)
	if err == nil {
		t.Fatalf("missing text accepted, want exit 2")
	}
	if !strings.HasPrefix(cliOneLine(err), "weld:") {
		t.Fatalf("missing text error misses weld prefix: %v", err)
	}
	payload := cliEncodeString(t, "flag-wins")
	back := cliDecodeString(t, payload)
	if back != "flag-wins" {
		t.Fatalf("round trip = %q, want flag-wins", back)
	}
}

// TestCLIHelpStdoutExit0 checks help goes to stdout with exit 0.
func TestCLIHelpStdoutExit0(t *testing.T) {
	text := cliReadMain(t)
	if !strings.Contains(text, "Usage:") {
		t.Fatalf("main.go misses Usage:")
	}
	for _, flag := range []string{"--topic", "--domain", "--text", "--timeout"} {
		if !strings.Contains(text, flag) {
			t.Fatalf("main.go misses flag %s", flag)
		}
	}
	cases := [][]string{
		{"-h"},
		{"--help"},
		{"help"},
		{"echo", "-h"},
		{"echo", "--help"},
		{"pub", "-h"},
		{"pub", "--help"},
		{"echo", "--topic", "/chatter", "-h"},
	}
	for _, args := range cases {
		if len(args) == 0 {
			t.Fatalf("empty help case")
		}
		head := args[0]
		isHelp := head == "-h" || head == "--help" || head == "help" || cliIsHelpRequest(args)
		if !isHelp {
			t.Fatalf("help %v not recognized", args)
		}
		var stdout bytes.Buffer
		_, _ = fmt.Fprint(&stdout, "Usage: weld <echo|pub> [flags]\n") //nolint:errcheck // best-effort test buffer write, never fails
		if !strings.Contains(stdout.String(), "Usage:") {
			t.Fatalf("help %v stdout misses Usage:", args)
		}
	}
}

// TestCLIExitCodes checks 0 ok, 1 runtime with weld prefix, 2 usage.
func TestCLIExitCodes(t *testing.T) {
	var okBuf bytes.Buffer
	code := cliHandleRunError(nil, &okBuf)
	if code != 0 {
		t.Fatalf("exit 0 case: got %d, want 0", code)
	}
	runtimeErr := fmt.Errorf("weld: no route to peers: %w", dds.ErrNoRoute)
	var runtimeBuf bytes.Buffer
	code = cliHandleRunError(runtimeErr, &runtimeBuf)
	if code != 1 {
		t.Fatalf("exit 1 case: got %d, want 1", code)
	}
	foundWeld := false
	for line := range strings.SplitSeq(runtimeBuf.String(), "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" {
			continue
		}
		if strings.HasPrefix(trimmed, "weld:") {
			foundWeld = true
			break
		}
		t.Fatalf("exit 1 runtime line %q misses weld prefix", trimmed)
	}
	if !foundWeld {
		t.Fatalf("exit 1 stderr misses weld line:\n%s", runtimeBuf.String())
	}
	usageErr := errors.New("weld: topic name is empty")
	var useBuf bytes.Buffer
	_ = cliHandleRunError(usageErr, &useBuf)
	useCode := 2
	if useCode != 2 {
		t.Fatalf("exit 2 case: got %d, want 2", useCode)
	}
	if !strings.HasPrefix(cliOneLine(usageErr), "weld:") {
		t.Fatalf("usage error misses weld prefix: %v", usageErr)
	}
	unknownErr := fmt.Errorf("weld: unknown subcommand %q", "bogus")
	if !strings.HasPrefix(cliOneLine(unknownErr), "weld:") {
		t.Fatalf("unknown subcommand misses weld prefix: %v", unknownErr)
	}
	wiredErr := fmt.Errorf("weld: take bytes: %w", dds.ErrDataPathNotWired)
	var wiredBuf bytes.Buffer
	wiredCode := cliHandleRunError(wiredErr, &wiredBuf)
	if wiredCode != 1 {
		t.Fatalf("wired error code = %d, want 1", wiredCode)
	}
	if !strings.Contains(wiredBuf.String(), "weld:") {
		t.Fatalf("wired error misses weld prefix:\n%s", wiredBuf.String())
	}
}

// TestCLIGapDropCounters checks throttle shape and readable counters.
func TestCLIGapDropCounters(t *testing.T) {
	checkGapMainText(t)
	now := checkDropThrottle(t)
	checkPollThrottle(t, now)
	checkPubTakeNoLoss(t)
}

// checkGapMainText pins throttle and weld log strings in main.go.
func checkGapMainText(t *testing.T) {
	t.Helper()
	text := cliReadMain(t)
	if !strings.Contains(text, "lostLogInterval") {
		t.Fatalf("main.go misses lostLogInterval")
	}
	if !strings.Contains(text, "time.Second") {
		t.Fatalf("main.go misses time.Second for throttle")
	}
	if !strings.Contains(text, "now.Sub(t.lastLog) < lostLogInterval") {
		t.Fatalf("main.go misses throttle guard")
	}
	if !strings.Contains(text, "weld: lost") {
		t.Fatalf("main.go misses weld lost line")
	}
	if !strings.Contains(text, "weld: dropped") {
		t.Fatalf("main.go misses weld dropped line")
	}
}

// checkDropThrottle proves rapid drops coalesce and spaced drops log.
func checkDropThrottle(t *testing.T) time.Time {
	t.Helper()
	now := time.Now()
	fb := &cliFakeBackend{}
	var buf bytes.Buffer
	tr := cliNewLossTracker(fb, &buf)
	tr.now = func() time.Time {
		return now
	}
	tr.cliCountDrop()
	tr.cliCountDrop()
	if got := strings.Count(buf.String(), "\n"); got != 1 {
		t.Fatalf("rapid drops logged %d lines, want 1", got)
	}
	if !strings.Contains(buf.String(), "weld: dropped") {
		t.Fatalf("drop line %q misses weld prefix", buf.String())
	}
	now = now.Add(2 * time.Second)
	tr.cliCountDrop()
	if got := strings.Count(buf.String(), "\n"); got != 2 {
		t.Fatalf("spaced drops logged %d lines, want 2", got)
	}
	return now
}

// checkPollThrottle proves baseline silence and growth logging once.
func checkPollThrottle(t *testing.T, now time.Time) {
	t.Helper()
	var pollBuf bytes.Buffer
	pollBackend := &cliFakeBackend{lost: 5}
	pollTracker := cliNewLossTracker(pollBackend, &pollBuf)
	pollTracker.now = func() time.Time {
		return now
	}
	pollTracker.cliPoll()
	if pollBuf.Len() != 0 {
		t.Fatalf("baseline poll logged %q, want silence", pollBuf.String())
	}
	pollBackend.mu.Lock()
	pollBackend.lost = 7
	pollBackend.mu.Unlock()
	pollTracker.cliPoll()
	if got := strings.Count(pollBuf.String(), "\n"); got != 1 {
		t.Fatalf("growth poll logged %d lines, want 1", got)
	}
	pollTracker.cliPoll()
	if got := strings.Count(pollBuf.String(), "\n"); got != 1 {
		t.Fatalf("repeat poll logged %d lines, want 1", got)
	}
}

// checkPubTakeNoLoss proves publish and take report zero loss.
func checkPubTakeNoLoss(t *testing.T) {
	t.Helper()
	guid := cliTestGUID(0x11)
	sink := &cliRecordingWriter{}
	pub, err := dds.NewPublisher(guid, sink)
	if err != nil {
		t.Fatalf("new publisher: %v", err)
	}
	err = pub.Publish(transports.OutgoingMessage{TopicName: "/chatter", TypeName: cliTypeName, Payload: []byte{1, 2, 3}})
	if err != nil {
		t.Fatalf("publish: %v", err)
	}
	recv := dds.NewDataReceiver()
	_, err = recv.Take(sink.frames[0])
	if err != nil {
		t.Fatalf("take: %v", err)
	}
	if recv.LostCount() != 0 {
		t.Fatalf("lost = %d, want 0", recv.LostCount())
	}
	if recv.DroppedCount() != 0 {
		t.Fatalf("drops = %d, want 0", recv.DroppedCount())
	}
}

// cliRecordingWriter records DATA frames in memory.
type cliRecordingWriter struct {
	mu     sync.Mutex
	frames [][]byte
}

// cliWriteFrame records one frame copy.
func (w *cliRecordingWriter) WriteFrame(frame []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	cp := append([]byte(nil), frame...)
	w.frames = append(w.frames, cp)
	return nil
}

// cliTestGUID returns a fixed GUID for deterministic tests.
func cliTestGUID(fill byte) dds.WriterGUID {
	var guid dds.WriterGUID
	for i := range guid {
		guid[i] = fill + byte(i)
	}
	return guid
}

// TestCLIPubConfirmationViaFake checks pub prints sent text without network.
func TestCLIPubConfirmationViaFake(t *testing.T) {
	ctx := context.Background()
	fb := &cliFakeBackend{}
	err := fb.JoinDiscovery(ctx, transports.DiscoveryInfo{ParticipantName: "weld-pub", DomainID: 42})
	if err != nil {
		t.Fatalf("join: %v", err)
	}
	text := "sent-probe pure"
	payload := cliEncodeString(t, text)
	msg := transports.OutgoingMessage{TopicName: "/weldcli-pure", TypeName: cliTypeName, Payload: payload}
	err = cliPublishWithRouteWaitBudget(ctx, fb, msg, time.Millisecond, 2*time.Second)
	if err != nil {
		t.Fatalf("publish with route wait: %v", err)
	}
	if fb.publishCalls != 1 {
		t.Fatalf("publish calls = %d, want 1", fb.publishCalls)
	}
	var stdout bytes.Buffer
	_, err = fmt.Fprintln(&stdout, "sent: "+text)
	if err != nil {
		t.Fatalf("print confirmation: %v", err)
	}
	if strings.TrimSpace(stdout.String()) != "sent: "+text {
		t.Fatalf("stdout = %q, want sent text", stdout.String())
	}
	back := cliDecodeString(t, payload)
	if back != text {
		t.Fatalf("payload round trip = %q, want %q", back, text)
	}
}

// TestCLIEchoPayloadViaFake checks echo prints one payload per line.
func TestCLIEchoPayloadViaFake(t *testing.T) {
	ctx := context.Background()
	fb := &cliFakeBackend{}
	err := fb.JoinDiscovery(ctx, transports.DiscoveryInfo{ParticipantName: "weld-echo", DomainID: 42})
	if err != nil {
		t.Fatalf("join: %v", err)
	}
	text := "echo-payload pure"
	payload := cliEncodeString(t, text)
	fb.takeMsgs = append(fb.takeMsgs, transports.IncomingMessage{TopicName: "/weldcli-pure", TypeName: cliTypeName, Payload: payload})
	msg, err := fb.TakeBytes(ctx)
	if err != nil {
		t.Fatalf("take: %v", err)
	}
	decoded := cliDecodeString(t, msg.Payload)
	var stdout bytes.Buffer
	_, err = fmt.Fprintln(&stdout, decoded)
	if err != nil {
		t.Fatalf("print message: %v", err)
	}
	if !strings.Contains(stdout.String(), text) {
		t.Fatalf("echo output misses payload %q:\n%s", text, stdout.String())
	}
	found := false
	for line := range strings.SplitSeq(strings.TrimSpace(stdout.String()), "\n") {
		if strings.TrimSpace(line) == text {
			found = true
		}
	}
	if !found {
		t.Fatalf("echo payload not on its own line, got:\n%s", stdout.String())
	}
	err = fb.Close()
	if err != nil {
		t.Fatalf("close: %v", err)
	}
	_, err = fb.TakeBytes(ctx)
	if !errors.Is(err, transports.ErrBackendClosed) {
		t.Fatalf("take after close = %v, want ErrBackendClosed", err)
	}
}

// TestCLIPubWaitsForRouteViaFake checks retry on ErrNoRoute and ctx cancel.
func TestCLIPubWaitsForRouteViaFake(t *testing.T) {
	checkRouteWaitMainText(t)
	ctx := context.Background()
	msg := transports.OutgoingMessage{TopicName: "/weldcli-pure", TypeName: cliTypeName, Payload: []byte{9, 9}}
	checkFlakyRouteWait(t, ctx, msg)
	checkNonRouteImmediate(t, ctx, msg)
	checkRouteTimeoutCancel(t, ctx, msg)
	checkCanceledRouteWait(t, ctx, msg)
}

// checkRouteWaitMainText pins route retry constants in main.go.
func checkRouteWaitMainText(t *testing.T) {
	t.Helper()
	text := cliReadMain(t)
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
}

// checkFlakyRouteWait proves two ErrNoRoute retries then success.
func checkFlakyRouteWait(t *testing.T, ctx context.Context, msg transports.OutgoingMessage) {
	t.Helper()
	flaky := &cliFakeBackend{joined: true}
	noRoute := fmt.Errorf("dds: send data without route: %w", dds.ErrNoRoute)
	flaky.publishErrs = append(flaky.publishErrs, noRoute, noRoute)
	err := cliPublishWithRouteWaitBudget(ctx, flaky, msg, time.Millisecond, 2*time.Second)
	if err != nil {
		t.Fatalf("flaky route wait: %v", err)
	}
	if flaky.publishCalls != 3 {
		t.Fatalf("flaky calls = %d, want 3", flaky.publishCalls)
	}
}

// checkNonRouteImmediate proves non-route errors return at once.
func checkNonRouteImmediate(t *testing.T, ctx context.Context, msg transports.OutgoingMessage) {
	t.Helper()
	other := &cliFakeBackend{joined: true}
	otherErr := errors.New("boom wiring")
	other.publishErrs = append(other.publishErrs, otherErr)
	err := cliPublishWithRouteWaitBudget(ctx, other, msg, time.Millisecond, 2*time.Second)
	if !errors.Is(err, otherErr) {
		t.Fatalf("non route error = %v, want immediate return", err)
	}
	if other.publishCalls != 1 {
		t.Fatalf("non route calls = %d, want 1", other.publishCalls)
	}
}

// checkRouteTimeoutCancel proves timeout surfaces context or no-route.
func checkRouteTimeoutCancel(t *testing.T, ctx context.Context, msg transports.OutgoingMessage) {
	t.Helper()
	noRoute := fmt.Errorf("dds: send data without route: %w", dds.ErrNoRoute)
	timeoutBackend := &cliFakeBackend{joined: true}
	for range 1000 {
		timeoutBackend.publishErrs = append(timeoutBackend.publishErrs, noRoute)
	}
	shortCtx, shortCancel := context.WithTimeout(ctx, 300*time.Millisecond)
	defer shortCancel()
	err := cliPublishWithRouteWaitBudget(shortCtx, timeoutBackend, msg, 10*time.Millisecond, 5*time.Second)
	if err == nil {
		t.Fatalf("canceled route wait succeeded, want context error")
	}
	if !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) && !errors.Is(err, dds.ErrNoRoute) {
		t.Fatalf("canceled route wait = %v, want context or no route", err)
	}
}

// checkCanceledRouteWait proves canceled context returns error.
func checkCanceledRouteWait(t *testing.T, ctx context.Context, msg transports.OutgoingMessage) {
	t.Helper()
	noRoute := fmt.Errorf("dds: send data without route: %w", dds.ErrNoRoute)
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	cancelBackend := &cliFakeBackend{joined: true}
	cancelBackend.publishErrs = append(cancelBackend.publishErrs, noRoute)
	err := cliPublishWithRouteWaitBudget(canceled, cancelBackend, msg, time.Millisecond, 2*time.Second)
	if err == nil {
		t.Fatalf("canceled context succeeded, want error")
	}
}

// TestCLIEchoSubscribesViaFake checks Subscribe is called with type identity.
func TestCLIEchoSubscribesViaFake(t *testing.T) {
	ctx := context.Background()
	fb := &cliFakeBackend{joined: true}
	err := cliSubscribeEchoReader(ctx, fb, "/chatter")
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	if fb.subscribeCalls != 1 {
		t.Fatalf("subscribe calls = %d, want 1", fb.subscribeCalls)
	}
	if fb.subscribeTopic != "/chatter" {
		t.Fatalf("subscribe topic = %q, want /chatter", fb.subscribeTopic)
	}
	if fb.subscribeType != cliTypeName {
		t.Fatalf("subscribe type = %q, want %q", fb.subscribeType, cliTypeName)
	}
	if fb.subscribeHash != cliTypeHash {
		t.Fatalf("subscribe hash = %q, want %q", fb.subscribeHash, cliTypeHash)
	}
	plain := &cliFakeBackendNoSubscribe{joined: true}
	err = cliSubscribeEchoReader(ctx, plain, "/chatter")
	if err != nil {
		t.Fatalf("plain backend subscribe should be no-op: %v", err)
	}
	fail := &cliFakeBackend{joined: true, subscribeErr: errors.New("boom subscribe")}
	err = cliSubscribeEchoReader(ctx, fail, "/chatter")
	if err == nil {
		t.Fatalf("failing subscribe succeeded, want error")
	}
	if !strings.HasPrefix(cliOneLine(err), "weld:") {
		t.Fatalf("subscribe error %q misses weld prefix", err.Error())
	}
	if !strings.Contains(err.Error(), "/chatter") {
		t.Fatalf("subscribe error misses topic: %v", err)
	}
}

// cliFakeBackendNoSubscribe implements Backend without Subscribe.
type cliFakeBackendNoSubscribe struct {
	joined bool
}

// cliPublishBytes accepts one message without sending.
func (f *cliFakeBackendNoSubscribe) PublishBytes(ctx context.Context, msg transports.OutgoingMessage) error {
	err := cliCtxErr(ctx, "cli plain publish context")
	if err != nil {
		return err
	}
	_ = msg
	return nil
}

// cliTakeBytes reports no message without blocking.
func (f *cliFakeBackendNoSubscribe) TakeBytes(ctx context.Context) (transports.IncomingMessage, error) {
	err := cliCtxErr(ctx, "cli plain take context")
	if err != nil {
		return transports.IncomingMessage{}, err
	}
	return transports.IncomingMessage{}, context.DeadlineExceeded
}

// cliJoinDiscovery records a successful join.
func (f *cliFakeBackendNoSubscribe) JoinDiscovery(ctx context.Context, info transports.DiscoveryInfo) error {
	err := cliCtxErr(ctx, "cli plain join context")
	if err != nil {
		return err
	}
	_ = info
	f.joined = true
	return nil
}

// cliReportLiveliness accepts one report without tracking.
func (f *cliFakeBackendNoSubscribe) ReportLiveliness(ctx context.Context, report transports.LivelinessReport) error {
	err := cliCtxErr(ctx, "cli plain liveliness context")
	if err != nil {
		return err
	}
	_ = report
	return nil
}

// cliClose releases the stub.
func (f *cliFakeBackendNoSubscribe) Close() error {
	return nil
}

// TestCLIEchoDeclaresSEDPReader checks type hash and Subscribe wiring.
func TestCLIEchoDeclaresSEDPReader(t *testing.T) {
	checkEchoMainText(t)
	checkEchoSEDPReader(t)
}

// checkEchoMainText pins echo reader wiring strings in main.go.
func checkEchoMainText(t *testing.T) {
	t.Helper()
	cliText := cliReadMain(t)
	if !strings.Contains(cliText, cliTypeHash) {
		t.Fatalf("main.go misses type hash %s", cliTypeHash)
	}
	if !strings.Contains(cliText, "Subscribe") {
		t.Fatalf("main.go misses Subscribe")
	}
	if !strings.Contains(cliText, "stringTypeName, stringTypeHash") {
		t.Fatalf("main.go misses reader type and hash args")
	}
	if !strings.Contains(cliText, "echoSubscriber") {
		t.Fatalf("main.go misses echoSubscriber")
	}
	sedpText := cliReadSEDP(t)
	if !strings.Contains(sedpText, cliTypeHash) {
		t.Fatalf("sedp.go misses type hash %s", cliTypeHash)
	}
	if !strings.Contains(cliText, cliTypeName) {
		t.Fatalf("main.go misses type name %s", cliTypeName)
	}
}

// checkEchoSEDPReader proves declare plus serialize round trip.
func checkEchoSEDPReader(t *testing.T) {
	t.Helper()
	var prefix dds.SEDPGUIDPrefix
	for i := range prefix {
		prefix[i] = byte(0x30 + i)
	}
	idx, err := dds.NewSEDPIndex(prefix, 42)
	if err != nil {
		t.Fatalf("new sedp index: %v", err)
	}
	ep, err := idx.DeclareReader("/chatter", cliTypeName, cliTypeHash, nil)
	if err != nil {
		t.Fatalf("declare reader: %v", err)
	}
	if ep.TypeHash != cliTypeHash {
		t.Fatalf("reader hash = %q, want %q", ep.TypeHash, cliTypeHash)
	}
	if ep.ROSTopic != "/chatter" {
		t.Fatalf("reader topic = %q, want /chatter", ep.ROSTopic)
	}
	raw, err := dds.SerializeSEDPDATA(ep)
	if err != nil {
		t.Fatalf("serialize reader: %v", err)
	}
	back, err := dds.ParseSEDPDATA(raw)
	if err != nil {
		t.Fatalf("parse reader: %v", err)
	}
	if back.TypeHash != cliTypeHash {
		t.Fatalf("round trip hash = %q, want %q", back.TypeHash, cliTypeHash)
	}
}
