// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package bench

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/podomy/weld/internal/transports"
	"github.com/podomy/weld/internal/transports/dds"
	"github.com/podomy/weld/internal/types"
	"github.com/podomy/weld/testbed/bench/report"
	"github.com/podomy/weld/testbed/live/helpers"
)

// Bench IDL and wire constants.
const (
	// benchTypeIDL is the ROS IDL name carried on every bench topic.
	benchTypeIDL = "std_msgs__msg__String"
	// benchQoSDepth is the keep last depth pinned on every endpoint.
	benchQoSDepth = 100
	// benchHeaderBytes is the 16 byte send_ns plus seq header carried hex
	// encoded in the String payload.
	benchHeaderBytes = 16

	// benchHeaderChars is the 32 ASCII hex chars of the header at the start
	// of every String payload.
	benchHeaderChars = 32

	// benchFillBase is the printable ASCII base of the deterministic fill.
	benchFillBase = 33

	// benchFillSpan is the fill alphabet span: chars 33 through 126.
	benchFillSpan = 94

	// benchMinSize is the smallest encodable payload: 32 header chars plus
	// one fill char. rclpy truncates String at the first NUL byte, so the
	// binary header plus byte fill layout cannot cross the ROS wire; the
	// header travels hex encoded (same 16 bytes) and the fill uses NUL
	// free printable ASCII. The 16 header bytes still decode to send_ns
	// plus seq, and .data stays exactly size chars.
	benchMinSize = benchHeaderChars + 1
	// benchCPUHz is the recorded USER_HZ assumption for /proc/self/stat.
	benchCPUHz = 100.0
	// benchDiscoveryTimeout bounds one join to first remote endpoint wait.
	benchDiscoveryTimeout = 30 * time.Second
	// benchDiscoveryPoll spaces discovery polls.
	benchDiscoveryPoll = 50 * time.Millisecond
	// benchSettle is the post discovery settle sleep on probe legs.
	benchSettle = 2 * time.Second
	// benchPubRetry bounds one publish with retry for route settle.
	benchPubRetry = 10 * time.Second
	// benchPubRetryGap spaces publish route retries.
	benchPubRetryGap = 50 * time.Millisecond
	// benchTakeMargin pads the take budget past the paced send duration.
	benchTakeMargin = 30 * time.Second
	// benchProbeMargin pads the probe timeout past its send duration.
	benchProbeMargin = 60 * time.Second
	// benchDefaultTimeout bounds one entrypoint in default (smoke) mode.
	benchDefaultTimeout = 6 * time.Minute
	// benchFullTimeout bounds one entrypoint in full-matrix mode, where a
	// single leg runs every tier plus warmup.
	benchFullTimeout = 50 * time.Minute
	// benchSaturationTake caps one uncapped saturation run.
	benchSaturationTake = 180 * time.Second
	// benchMaxInflight bounds concurrent publishes in one series. Publishing
	// holds the backend lock across find plus align plus send, so pipelined
	// dispatch keeps the paced rates while a single shared publisher
	// serializes samples in order.
	benchMaxInflight = 64
)

// tier pins one payload size plus count, warmup, and paced rate. A zero
// rateHz means uncapped (open loop); saturation tiers use it with a fixed
// count and the saturation take budget instead of a paced duration.
type tier struct {
	name   string
	size   int
	count  int
	warmup int
	rateHz float64
}

// p0Tier is the single smoke tier every leg runs in default mode: 256 B
// payloads, N=300 at 100 Hz with a 100 message warmup.
func p0Tier() tier {
	return tier{name: "p0", size: 256, count: 300, warmup: 100, rateHz: 100}
}

// benchFull reports whether WELD_BENCH_FULL=1 selects the full matrix
// (every tier plus fanout, fanin, and saturation) instead of the P0 smoke.
func benchFull() bool {
	return os.Getenv("WELD_BENCH_FULL") == "1"
}

// fullTiers holds the P0 through P6 table used in full mode. p0 matches
// p0Tier exactly. p3 is a small-message control below the fragmentation
// boundary (65515), p4 the first fragmented tier just above it, p6 a mid
// large tier at 256 KiB, and p5 the large multi-fragment tier at 1 MiB.
func fullTiers() []tier {
	return []tier{
		{name: "p0", size: 256, count: 300, warmup: 100, rateHz: 100},
		{name: "p1", size: 1024, count: 3000, warmup: 500, rateHz: 100},
		{name: "p2", size: 4096, count: 3000, warmup: 500, rateHz: 100},
		{name: "p3", size: 64, count: 1000, warmup: 100, rateHz: 100},
		{name: "p4", size: 65536, count: 2500, warmup: 250, rateHz: 50},
		{name: "p6", size: 262144, count: 1000, warmup: 100, rateHz: 50},
		{name: "p5", size: 1048576, count: 500, warmup: 100, rateHz: 10},
	}
}

// saturationTier is the uncapped headroom run: fixed P1 payloads blasted
// as fast as the stack allows until count is reached. Loss alongside the
// rate is the verdict, never the rate alone. Headroom rows record
// rate plus loss without failing: the tier is loss-tolerant by design.
func saturationTier() tier {
	return tier{name: "sat", size: 1024, count: 20000, warmup: 0, rateHz: 0}
}

// sweepRates holds the offered-load steps for the load-latency sweep at
// fixed 1024 B payloads. Rates climb until the stack chokes; the choke
// point where loss appears or p99 climbs steeply is the verdict. The top
// steps run past what any stack clears cleanly today, with the uncapped
// saturation blast as the final word.
func sweepRates() []float64 {
	return []float64{50, 100, 200, 400, 800, 1600, 5000, 10000, 30000, 60000, 100000}
}

// sweepSize pins the sweep payload to P1 bytes so rate isolates load,
// not payload shape.
const sweepSize = 1024

// sweepCount bounds one sweep step: enough samples for stable p50/p99.
// Lossless steps finish in paced time (about five minutes for the whole
// sweep); lossy steps each burn their full take budget, so a choking
// sweep runs longer. The entrypoint budget bounds one entrypoint, not the
// pathological all-lossy sum; a timeout there fails loudly like any other
// loud condition, never silently.
const sweepCount = 1000

// sweepWarmup drains JIT and discovery effects on the in-process leg
// before measurement; container legs stay warmup free with match wait
// plus settle like every other paced tier.
const sweepWarmup = 100

// isHeadroomTier reports whether a tier records headroom without failing
// on loss. The uncapped saturation blast and every load-latency sweep
// step qualify: loss is the choke signal there, not a failure. Every
// other paced tier fails loudly on gaps.
func isHeadroomTier(tr tier) bool {
	return tr.name == "sat" || strings.HasPrefix(tr.name, "sw")
}

// sweepTier builds one sweep tier at the given offered rate. Names carry
// the rate so rows and chart points stay distinguishable.
func sweepTier(rateHz float64) tier {
	return tier{name: "sw" + strconv.Itoa(int(rateHz)), size: sweepSize, count: sweepCount, warmup: sweepWarmup, rateHz: rateHz}
}

// selectTiers returns the P0 smoke tier, or the full matrix in full mode.
func selectTiers(full bool) []tier {
	if !full {
		return []tier{p0Tier()}
	}
	return fullTiers()
}

// benchShared collects every result in one test binary run so a whole
// `go test` invocation leaves exactly one self-contained HTML report
// no matter how many entrypoints ran.
var benchShared = struct {
	sync.Mutex
	htmlPa  string
	results []report.Result
}{}

// benchRun carries one entrypoint's live ID for per-run topics plus notes.
// Result persistence is shared across entrypoints through benchShared;
// the run only owns its backends.
type benchRun struct {
	t      *testing.T
	liveID string
	openMu sync.Mutex
	open   []*dds.Backend
}

// newBenchRun registers the shared HTML report path and returns the run
// handle. The path is testbed/logs/bench-<timestamp>-<pid>.html, created
// once per test binary run. The live ID stays per entrypoint for topic
// isolation.
func newBenchRun(t *testing.T, prefix string) *benchRun {
	t.Helper()
	liveID := helpers.LiveID(prefix)
	benchShared.Lock()
	defer benchShared.Unlock()
	if benchShared.htmlPa == "" {
		root := helpers.RepoRoot(t)
		logsDir := filepath.Join(root, "testbed", "logs")
		err := os.MkdirAll(logsDir, 0o750)
		if err != nil {
			t.Fatalf("create bench logs dir: %v", err)
		}
		stamp := time.Now().UTC().Format("20060102-150405")
		base := "bench-" + stamp + "-" + strconv.Itoa(os.Getpid())
		benchShared.htmlPa = filepath.Join(logsDir, base+".html")
		t.Logf("bench html: %s", benchShared.htmlPa)
	}
	return &benchRun{t: t, liveID: liveID}
}

// benchRenderShared writes the single HTML report for the whole binary run.
// TestMain calls it after m.Run so partial runs still leave a readable
// report; empty runs render nothing.
func benchRenderShared() {
	benchShared.Lock()
	results := make([]report.Result, len(benchShared.results))
	copy(results, benchShared.results)
	htmlPath := benchShared.htmlPa
	benchShared.Unlock()
	if len(results) == 0 {
		_, _ = fmt.Println("bench: no results, skipping report render") //nolint:errcheck // report teardown log is best effort
		return
	}
	html := report.HTML(results, "Weld bench")
	err := os.WriteFile(htmlPath, []byte(html), 0o644) //nolint:gosec // bench harness writes its own report path
	if err != nil {
		_, _ = fmt.Printf("bench: write html %s: %v\n", htmlPath, err) //nolint:errcheck // report teardown log is best effort
		return
	}
	_, _ = fmt.Printf("bench html: %s (%d results)\n", htmlPath, len(results)) //nolint:errcheck // report teardown log is best effort
}

// leaseDomain allocates one pooled domain and returns its release func.
// Scenarios hold the lease only for their duration so a reused domain
// never carries another scenario's traffic; entrypoints run serially (see
// TestMain) so concurrent holds never overlap, and the deferred release
// runs at scenario end paired with scenario end backend close.
//
// It skips (never fails) when Docker or any fleet container is absent:
// pooled allocation purges residue through the whole fleet, so a missing
// peer means the environment cannot provide a clean domain. Genuine
// allocation errors after the environment check still fail loudly.
func leaseDomain(t *testing.T, ctx context.Context) (int, func()) {
	t.Helper()
	if !helpers.DockerAvailable(ctx) {
		t.Skip("bench: docker unavailable, skipping")
	}
	for _, name := range benchFleetContainers() {
		ok, err := helpers.ContainerRunning(ctx, name)
		if err != nil || !ok {
			t.Skipf("bench: fleet container %s unavailable, skipping", name)
		}
	}
	domain, token, err := helpers.AllocateDomainLease()
	if err != nil {
		t.Fatalf("allocate bench domain: %v", err)
	}
	return domain, func() {
		helpers.ReleaseDomainLease(domain, token)
	}
}

// benchFleetContainers lists every fleet peer purged on pooled allocation.
// weld joins so leaseDomain skips when the containerized Weld
// endpoint is absent, exactly like the ROS peers.
func benchFleetContainers() []string {
	seen := map[string]bool{helpers.Container: true, weldContainer: true}
	out := []string{helpers.Container, weldContainer}
	for _, peer := range helpers.DistroPeers() {
		if peer.Container == "" || seen[peer.Container] {
			continue
		}
		seen[peer.Container] = true
		out = append(out, peer.Container)
	}
	return out
}

// record appends one result to the shared in-memory set rendered once as
// HTML by TestMain. Rows are recorded before the loss assert so the HTML
// keeps the evidence for a loud failure.
func (r *benchRun) record(res report.Result) {
	r.t.Helper()
	benchShared.Lock()
	benchShared.results = append(benchShared.results, res)
	benchShared.Unlock()
	r.t.Logf("bench %s %s size=%d sent=%d recv=%d loss=%.2f%% p50=%.0fns",
		res.Scenario, res.Leg, res.PayloadBytes, res.Sent, res.Recv, res.LossPct, res.LatNs.P50)
}

// checkDomainEnv fails loudly on a conflicting host ROS_DOMAIN_ID.
// The pool never touches process state, so parallel workers cannot set or
// unset the variable per scenario; an explicit conflict check plus the
// DomainID assert in openBenchBackend keeps the domain pinned.
func checkDomainEnv(t *testing.T, domain int) {
	t.Helper()
	raw, ok := os.LookupEnv(transports.DomainEnvVar)
	if !ok {
		return
	}
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return
	}
	if trimmed != strconv.Itoa(domain) {
		t.Fatalf("host %s=%q conflicts with bench domain %d; unset it or match it",
			transports.DomainEnvVar, raw, domain)
	}
}

// openBackend joins a Weld backend on the pooled domain and asserts
// the resolved domain equals the intended one, catching ROS_DOMAIN_ID
// overrides from the host environment. The backend stays open until
// closeScenarioBackends: scenario runners close every backend before
// releasing the domain, so a released-then-reused domain never carries
// lingering writers or readers into the next scenario.
func (r *benchRun) openBackend(t *testing.T, ctx context.Context, domain int, participant string) *dds.Backend {
	t.Helper()
	checkDomainEnv(t, domain)
	err := helpers.ValidateDomain(domain)
	if err != nil {
		t.Fatalf("bench domain invalid: %v", err)
	}
	cfg := dds.SidecarConfig{
		DomainID:    uint32(domain), //nolint:gosec // G115: ValidateDomain pins 0 through 101
		PeerAddress: dds.DefaultPeerAddress,
		Range:       transports.DiscoveryRangeSystemDefault,
	}
	backend, err := dds.NewBackend(cfg)
	if err != nil {
		t.Fatalf("new bench backend domain %d: %v", domain, err)
	}
	info := transports.DiscoveryInfo{
		ParticipantName: participant,
		DomainID:        uint32(domain), //nolint:gosec // G115: ValidateDomain pins 0 through 101
		Range:           transports.DiscoveryRangeSystemDefault,
	}
	err = backend.JoinDiscovery(ctx, info)
	if err != nil {
		_ = backend.Close() //nolint:errcheck,contextcheck // join failed, best effort close before fatal; backend Close is cleanup without caller ctx
		t.Fatalf("bench join domain %d: %v", domain, err)
	}
	if backend.DomainID() != uint32(domain) { //nolint:gosec // G115: ValidateDomain pins 0 through 101
		_ = backend.Close() //nolint:errcheck,contextcheck // domain mismatch, best effort close before fatal; backend Close is cleanup without caller ctx
		t.Fatalf("bench domain id = %d want %d (check %s override)",
			backend.DomainID(), domain, transports.DomainEnvVar)
	}
	r.openMu.Lock()
	r.open = append(r.open, backend)
	r.openMu.Unlock()
	return backend
}

// closeScenarioBackends closes every backend opened since the last call.
// Scenario runners call it before releasing the domain, so writers stop
// announcing and sending maintenance traffic before the domain is reused.
func (r *benchRun) closeScenarioBackends() {
	r.openMu.Lock()
	pending := r.open
	r.open = nil
	r.openMu.Unlock()
	for _, backend := range pending {
		cerr := backend.Close()
		if cerr != nil {
			r.t.Logf("bench backend close: %v", cerr)
		}
	}
}

// subscribeBenchReader declares a transient local reader for bench traffic.
func subscribeBenchReader(t *testing.T, ctx context.Context, backend *dds.Backend, topic, label string) {
	t.Helper()
	err := backend.SubscribeWithDurability(ctx, topic, benchTypeIDL, dds.StringTypeHash, false)
	if err != nil {
		t.Fatalf("bench subscribe %s %s: %v", label, topic, err)
	}
}

// waitDiscoveryMs polls until the backend sees a remote endpoint for topic
// and returns the elapsed milliseconds. A timeout fails loudly: discovery
// failure is product news, never a skip.
func waitDiscoveryMs(t *testing.T, ctx context.Context, backend *dds.Backend, topic, label string) float64 {
	t.Helper()
	start := time.Now()
	for {
		if backend.HasRemoteEndpoint(topic) {
			return float64(time.Since(start)) / float64(time.Millisecond)
		}
		if ctx.Err() != nil {
			t.Fatalf("bench discovery %s topic %s ended: %v", label, topic, ctx.Err())
		}
		if time.Since(start) >= benchDiscoveryTimeout {
			t.Fatalf("bench discovery %s topic %s timed out after %s",
				label, topic, benchDiscoveryTimeout)
		}
		helpers.Sleep(ctx, benchDiscoveryPoll)
	}
}

// encodeBenchPayload builds the XCDR String sample for one sequence number.
// The String holds exactly size chars: the 32 ASCII hex chars of the 16
// header bytes (8 byte little endian send_ns plus 8 byte little endian
// seq, seq starting at 1 like the probe) followed by deterministic fill
// chr(33 + ((seq+i)%94)) for fill position i. Sizes below 33 fail loudly.
func encodeBenchPayload(seq, size int, sendNs int64) ([]byte, error) {
	if size < benchMinSize {
		return nil, fmt.Errorf("bench: payload size %d below minimum %d", size, benchMinSize)
	}
	if seq < 1 {
		return nil, fmt.Errorf("bench: seq %d below 1", seq)
	}
	var hdr [benchHeaderBytes]byte
	binary.LittleEndian.PutUint64(hdr[0:8], uint64(sendNs)) //nolint:gosec // G115: bench stamp is a non-negative UnixNano
	binary.LittleEndian.PutUint64(hdr[8:16], uint64(seq))
	text := make([]byte, 0, size)
	text = append(text, hex.EncodeToString(hdr[:])...)
	for i := benchHeaderChars; i < size; i++ {
		text = append(text, byte(benchFillBase+(seq+i-benchHeaderChars)%benchFillSpan))
	}
	enc := types.NewEncoder(true)
	err := enc.WriteString(string(text))
	if err != nil {
		return nil, fmt.Errorf("bench: encode payload seq %d: %w", seq, err)
	}
	return enc.Bytes(), nil
}

// parseBenchPayload decodes one XCDR String sample into send_ns and seq and
// verifies the fill pattern. Undecodable or corrupt samples fail loudly:
// on a per-run topic any foreign payload is a leak, never skippable.
func parseBenchPayload(wire []byte) (int64, int, error) {
	dec, err := types.NewDecoder(wire)
	if err != nil {
		return 0, 0, fmt.Errorf("bench: decode header: %w", err)
	}
	text, err := dec.ReadString()
	if err != nil {
		return 0, 0, fmt.Errorf("bench: decode string: %w", err)
	}
	err = dec.Finish()
	if err != nil {
		return 0, 0, fmt.Errorf("bench: decode finish: %w", err)
	}
	if len(text) < benchMinSize {
		return 0, 0, fmt.Errorf("bench: payload %d chars below minimum %d", len(text), benchMinSize)
	}
	hdr, err := hex.DecodeString(text[:benchHeaderChars])
	if err != nil {
		return 0, 0, fmt.Errorf("bench: decode hex header: %w", err)
	}
	sendNs := int64(binary.LittleEndian.Uint64(hdr[0:8])) //nolint:gosec // G115: bench controlled LE header round trip
	seq := int(binary.LittleEndian.Uint64(hdr[8:16]))     //nolint:gosec // G115: bench seq space fits int
	if seq < 1 {
		return 0, 0, fmt.Errorf("bench: seq %d below 1", seq)
	}
	for i := benchHeaderChars; i < len(text); i++ {
		want := byte(benchFillBase + (seq+i-benchHeaderChars)%benchFillSpan)
		if text[i] != want {
			return 0, 0, fmt.Errorf("bench: fill mismatch at char %d seq %d", i, seq)
		}
	}
	return sendNs, seq, nil
}

// computeGaps returns the sorted missing sequence numbers in [1, count]
// not present in seen. Seqs start at 1 like the probe.
func computeGaps(seen map[int]bool, count int) []int {
	var gaps []int
	for seq := 1; seq <= count; seq++ {
		if !seen[seq] {
			gaps = append(gaps, seq)
		}
	}
	return gaps
}

// lossPct returns the loss percent over the expected count.
func lossPct(sent, recv int) float64 {
	if sent <= 0 {
		return 0
	}
	return 100 * float64(sent-recv) / float64(sent)
}

// uniqueRecv reports unique deliveries as sent minus missing seqs, so
// duplicate redeliveries (counted separately in Dups) never inflate Recv
// past Sent or drive LossPct negative. Gaps name exactly the missing seqs
// in the per-run [1..count] space, so the difference is the unique set.
func uniqueRecv(sent int, gaps []int) int {
	if n := sent - len(gaps); n >= 0 {
		return n
	}
	return 0
}

// cpuSample holds one /proc/self/stat CPU reading plus wall time.
type cpuSample struct {
	jiffies uint64
	at      time.Time
}

// readJiffies returns utime plus stime from /proc/self/stat in jiffies.
// HZ is recorded as 100, the Linux USER_HZ on all supported platforms.
func readJiffies() (uint64, error) {
	data, err := os.ReadFile("/proc/self/stat") // bench harness reads its own proc stat
	if err != nil {
		return 0, fmt.Errorf("bench: read proc stat: %w", err)
	}
	rest, found := cutAfterLastParen(string(data))
	if !found {
		return 0, errors.New("bench: proc stat misses paren") //nolint:wrapcheck // new error, nothing to wrap
	}
	fields := strings.Fields(rest)
	if len(fields) < 13 {
		return 0, fmt.Errorf("bench: proc stat has %d fields, want 13", len(fields))
	}
	utime, err := strconv.ParseUint(fields[11], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("bench: parse utime: %w", err)
	}
	stime, err := strconv.ParseUint(fields[12], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("bench: parse stime: %w", err)
	}
	return utime + stime, nil
}

// cutAfterLastParen splits /proc/self/stat after the comm paren.
func cutAfterLastParen(s string) (string, bool) {
	idx := strings.LastIndex(s, ")")
	if idx < 0 {
		return "", false
	}
	return s[idx+1:], true
}

// readRSSMiB returns /proc/self/status VmRSS in MiB.
func readRSSMiB() (float64, error) {
	data, err := os.ReadFile("/proc/self/status") // bench harness reads its own proc status
	if err != nil {
		return 0, fmt.Errorf("bench: read proc status: %w", err)
	}
	for line := range strings.Lines(string(data)) {
		fields := strings.Fields(line)
		if len(fields) != 3 || fields[0] != "VmRSS:" || fields[2] != "kB" {
			continue
		}
		kb, err := strconv.ParseFloat(fields[1], 64)
		if err != nil {
			return 0, fmt.Errorf("bench: parse VmRSS: %w", err)
		}
		return kb / 1024, nil
	}
	return 0, errors.New("bench: proc status misses VmRSS") //nolint:wrapcheck // new error, nothing to wrap
}

// sampleCPU takes one CPU plus wall reading.
func sampleCPU() (cpuSample, error) {
	jiffies, err := readJiffies()
	if err != nil {
		return cpuSample{}, err
	}
	return cpuSample{jiffies: jiffies, at: time.Now()}, nil
}

// cpuSeconds returns consumed CPU seconds from jiffies deltas.
func cpuSeconds(before, after cpuSample) float64 {
	if after.at.Before(before.at) {
		return 0
	}
	return float64(after.jiffies-before.jiffies) / benchCPUHz
}

// heapAllocMiB returns current heap allocation in MiB.
func heapAllocMiB() float64 {
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)
	return float64(mem.HeapAlloc) / (1 << 20)
}

// pubOutcome carries one publish series result from a child goroutine.
type pubOutcome struct {
	sent    int
	firstNs int64
	lastNs  int64
	err     error
}

// pubState folds concurrent publish completions under one mutex.
type pubState struct {
	mu       sync.Mutex
	sent     int
	firstNs  int64
	lastNs   int64
	firstErr error
}

// markDispatched records one dispatch stamp; only the dispatcher calls it.
func (s *pubState) markDispatched(sendNs int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.firstNs == 0 {
		s.firstNs = sendNs
	}
	s.lastNs = sendNs
}

// markDone records one publish completion from a worker goroutine.
func (s *pubState) markDone(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err != nil {
		if s.firstErr == nil {
			s.firstErr = err
		}
		return
	}
	s.sent++
}

// outcome returns the folded outcome, preferring the dispatch error.
func (s *pubState) outcome(dispatchErr error) pubOutcome {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := pubOutcome{sent: s.sent, firstNs: s.firstNs, lastNs: s.lastNs}
	if dispatchErr != nil {
		out.err = dispatchErr
		return out
	}
	out.err = s.firstErr
	return out
}

// pacedDispatcher paces dispatches at rateHz; a zero rateHz never waits.
type pacedDispatcher struct {
	ticker *time.Ticker
	ch     <-chan time.Time
}

// newPacedDispatcher builds a dispatcher for rateHz.
func newPacedDispatcher(rateHz float64) *pacedDispatcher {
	if rateHz <= 0 {
		return &pacedDispatcher{}
	}
	ticker := time.NewTicker(time.Duration(float64(time.Second) / rateHz))
	return &pacedDispatcher{ticker: ticker, ch: ticker.C}
}

// stop releases the ticker.
func (d *pacedDispatcher) stop() {
	if d.ticker != nil {
		d.ticker.Stop()
	}
}

// wait blocks for the next slot or context end.
func (d *pacedDispatcher) wait(ctx context.Context) bool {
	if d.ch == nil {
		return ctx.Err() == nil
	}
	select {
	case <-ctx.Done():
		return false
	case <-d.ch:
		return true
	}
}

// acquireSem takes one inflight slot or reports context end.
func acquireSem(ctx context.Context, sem chan struct{}) bool {
	select {
	case <-ctx.Done():
		return false
	case sem <- struct{}{}:
		return true
	}
}

// publishOne runs one publish on a worker goroutine.
func publishOne(ctx context.Context, pub *dds.Backend, msg transports.OutgoingMessage, sem chan struct{}, wg *sync.WaitGroup, st *pubState) {
	defer wg.Done()
	defer func() { <-sem }()
	err := helpers.PublishLiveWithRetry(ctx, pub, msg, benchPubRetry, benchPubRetryGap, "bench")
	if err != nil {
		err = fmt.Errorf("bench: publish: %w", err)
	}
	st.markDone(err)
}

// publishSeries dispatches count samples paced at rateHz and waits for all
// publishes to complete; callers run it on a child goroutine while the test
// goroutine takes. Up to benchMaxInflight publishes run at once for
// throughput headroom at high rates; serial dispatch would also keep up
// since a matched steady-state publish completes in microseconds. The
// backend still pauses 200ms per publish while no matching reader is
// observed (single-shot backstop), so dispatch stamps send_ns and latency
// covers any such wait. Seq numbers start at seqBase, letting callers hold
// disjoint seq spaces across publishers.
func publishSeries(ctx context.Context, pub *dds.Backend, topic string, count, seqBase, size int, rateHz float64) pubOutcome {
	disp := newPacedDispatcher(rateHz)
	defer disp.stop()
	sem := make(chan struct{}, benchMaxInflight)
	var wg sync.WaitGroup
	st := &pubState{}
	var dispatchErr error
	for k := range count {
		if ctx.Err() != nil {
			dispatchErr = fmt.Errorf("bench: publish context ended: %w", ctx.Err())
			break
		}
		seq := seqBase + k
		sendNs := time.Now().UnixNano()
		wire, err := encodeBenchPayload(seq, size, sendNs)
		if err != nil {
			dispatchErr = err
			break
		}
		msg := transports.OutgoingMessage{
			TopicName: topic,
			TypeName:  benchTypeIDL,
			Payload:   wire,
			TypeHash:  dds.StringTypeHash,
		}
		if !acquireSem(ctx, sem) {
			dispatchErr = fmt.Errorf("bench: publish slot wait ended: %w", ctx.Err())
			break
		}
		st.markDispatched(sendNs)
		wg.Add(1)
		go publishOne(ctx, pub, msg, sem, &wg, st)
		if !disp.wait(ctx) {
			dispatchErr = fmt.Errorf("bench: publish pacing ended: %w", ctx.Err())
			break
		}
	}
	wg.Wait()
	return st.outcome(dispatchErr)
}

// takeResult carries one take series collected on the test goroutine.
type takeResult struct {
	latNs   []int64
	seen    map[int]bool
	dups    int
	skipped int
	firstNs int64
	lastNs  int64
	err     error
}

// noteTakeErr folds one TakeBytes error: context end stops quietly, stale
// sequences count a dup, undecodable or misrouted frames skip with a
// count, anything else stops loudly. Skipping mirrors the agent and the
// probe: a first DATA that beats SEDP has no decodable frame yet, and a
// single skipped sample must not fail the run when the gap it would leave
// still fails loudly at the end if never repaired.
func noteTakeErr(res *takeResult, ctx context.Context, err error) bool {
	if ctx.Err() != nil {
		return false
	}
	if errors.Is(err, dds.ErrStaleSequence) {
		res.dups++
		return true
	}
	if errors.Is(err, dds.ErrProvisionalDecodeFail) ||
		errors.Is(err, dds.ErrUnknownWriter) ||
		errors.Is(err, dds.ErrTruncated) ||
		errors.Is(err, dds.ErrInvalidFrame) {
		res.skipped++
		return true
	}
	res.err = fmt.Errorf("bench: take: %w", err)
	return false
}

// observeTake folds one taken payload into the result: out of window seqs
// are ignored, dups counted, first arrivals appended with latency. The
// window is [1, want]: seqs start at 1 like the probe.
func observeTake(res *takeResult, msg transports.IncomingMessage, want int) {
	sendNs, seq, err := parseBenchPayload(msg.Payload)
	if err != nil {
		res.err = err
		return
	}
	if seq < 1 || seq > want {
		return
	}
	recvNs := time.Now().UnixNano()
	if res.seen[seq] {
		res.dups++
		return
	}
	res.seen[seq] = true
	res.latNs = append(res.latNs, recvNs-sendNs)
	if len(res.seen) == 1 {
		res.firstNs = recvNs
	}
	res.lastNs = recvNs
}

// collectTakes takes until want unique seqs arrive in [1, want] or ctx
// ends. Undecodable or misrouted frames on the per-run topic skip with a
// count, mirroring the agent and probe: a first DATA that beats SEDP has
// no decodable frame yet, and a single skip must not fail the run when
// the gap it would leave still fails loudly at the end if never repaired.
// Out of window seqs are ignored: stale replays from another
// series never pollute the measured set. Product stale sequence errors from
// RTPS redelivery or dual path racing count as dups and continue: the bench
// accounts payload level unique delivery, and loss still fails loudly.
func collectTakes(ctx context.Context, sub *dds.Backend, want int) takeResult {
	res := takeResult{seen: make(map[int]bool)}
	for len(res.seen) < want {
		msg, err := sub.TakeBytes(ctx)
		if err != nil {
			if !noteTakeErr(&res, ctx, err) {
				return res
			}
			continue
		}
		observeTake(&res, msg, want)
		if res.err != nil {
			return res
		}
	}
	return res
}

// skippedNote renders the skipped-frame count for take-loop notes so a
// run that skipped undecodable frames stays transparent about it.
func skippedNote(n int) string {
	if n == 0 {
		return ""
	}
	return ";skipped_undecodable=" + strconv.Itoa(n)
}

// takeBudget returns the take bound for a paced series.
func takeBudget(count int, rateHz float64) time.Duration {
	if rateHz <= 0 {
		return benchSaturationTake
	}
	return time.Duration(float64(count)/rateHz*float64(time.Second)) + benchTakeMargin
}

// announceWriter publishes one out-of-window sample so the local writer is
// declared and SEDP visible before the discovery wait. Backend writers
// declare lazily on first publish, so waiting for a remote endpoint without
// a prior publish would time out. The announce seq sits outside every
// measured window, so take loops skip it by construction.
func announceWriter(t *testing.T, ctx context.Context, pub *dds.Backend, topic string, size, seq int) {
	t.Helper()
	wire, err := encodeBenchPayload(seq, size, time.Now().UnixNano())
	if err != nil {
		t.Fatalf("bench announce encode: %v", err)
	}
	msg := transports.OutgoingMessage{
		TopicName: topic,
		TypeName:  benchTypeIDL,
		Payload:   wire,
		TypeHash:  dds.StringTypeHash,
	}
	err = helpers.PublishLiveWithRetry(ctx, pub, msg, benchPubRetry, benchPubRetryGap, "bench-announce")
	if err != nil {
		t.Fatalf("bench announce publish: %v", err)
	}
}

// probeDoc mirrors the probe stdout JSON document. The qos object shape is
// probe owned, so it stays raw; QoS is pinned by argv instead.
type probeDoc struct {
	Mode        string          `json:"mode"`
	Topic       string          `json:"topic"`
	QoS         json.RawMessage `json:"qos"`
	Count       int             `json:"count"`
	Sent        int             `json:"sent"`
	Received    int             `json:"received"`
	Gaps        []int           `json:"gaps"`
	Dups        int             `json:"dups"`
	SendFirstNs int64           `json:"send_first_ns"`
	SendLastNs  int64           `json:"send_last_ns"`
	RecvFirstNs int64           `json:"recv_first_ns"`
	RecvLastNs  int64           `json:"recv_last_ns"`
	LatNs       []float64       `json:"lat_ns"`
	Note        string          `json:"note"`
}

// probeQoSArgs pins the symmetric bench QoS on every probe argv.
func probeQoSArgs() []string {
	return []string{
		"--qos-reliability", "reliable",
		"--qos-durability", "transient_local",
		"--qos-history", "keep_last",
		"--qos-depth", strconv.Itoa(benchQoSDepth),
	}
}

// probeArgs builds the probe argv for one pub or sub mode. Timeout is
// whole seconds.
func probeArgs(mode, topic string, count int, rateHz float64, size int, timeout time.Duration, runID string) []string {
	args := []string{
		"--mode", mode,
		"--topic", topic,
	}
	args = append(args,
		"--count", strconv.Itoa(count),
		"--rate", strconv.FormatFloat(rateHz, 'f', -1, 64),
		"--size", strconv.Itoa(size),
	)
	args = append(args, probeQoSArgs()...)
	secs := max(int64(timeout.Seconds()), 1)
	args = append(args, "--timeout", strconv.FormatInt(secs, 10))
	if runID != "" {
		args = append(args, "--run-id", runID)
	}
	return args
}

// probeMatchWaitArgs builds the --wait-for-match argv with the discovery
// timeout. Gating the first publish or take on a matched peer keeps the
// first sample off the provisional path: steady state is unchanged.
func probeMatchWaitArgs() []string {
	return []string{"--wait-for-match", strconv.FormatFloat(benchDiscoveryTimeout.Seconds(), 'f', -1, 64)}
}

// loadProbeScript reads the probe program delivered to the peer over stdin.
// The script lives at testbed/bench/probes/bench_probe.py in the repo.
func loadProbeScript(t *testing.T) []byte {
	t.Helper()
	path := filepath.Join(helpers.RepoRoot(t), "testbed", "bench", "probes", "bench_probe.py")
	data, err := os.ReadFile(path) //nolint:gosec // bench harness reads its own probe script
	if err != nil {
		t.Fatalf("load probe script %s: %v", path, err)
	}
	if len(data) == 0 {
		t.Fatalf("probe script %s is empty", path)
	}
	return data
}

// tryExecProbe delivers the probe script over docker exec stdin and parses
// the single stdout JSON document, returning an error instead of failing.
// Goroutines use it so probe failures arrive on channels for the test
// goroutine to fail loudly; execProbe wraps it for direct calls.
func tryExecProbe(t *testing.T, ctx context.Context, dockerBase []string, script []byte, args []string, label string) (probeDoc, error) {
	t.Helper()
	dockerArgs, err := interactiveDockerArgsOrErr(t, dockerBase)
	if err != nil {
		return probeDoc{}, err
	}
	full := make([]string, 0, len(dockerArgs)+len(args)+2)
	full = append(full, dockerArgs...)
	full = append(full, "python3", "-")
	full = append(full, args...)
	timeout := ctx
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		timeout, cancel = context.WithTimeout(ctx, benchDefaultTimeout)
		defer cancel()
	}
	cmd := exec.CommandContext(timeout, "docker", full...) //nolint:gosec // bench harness drives the peer probe with fixed docker argv
	cmd.Stdin = bytes.NewReader(script)
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err = cmd.Run()
	doc, perr := parseProbeDoc(stdout.Bytes())
	if err != nil {
		var exitErr *exec.ExitError
		code := -1
		if errors.As(err, &exitErr) {
			code = exitErr.ExitCode()
		}
		if code == 2 && perr == nil && doc.Received == 0 {
			return probeDoc{}, fmt.Errorf("bench: probe discovery failure %s: %w stdout %q stderr %q",
				label, err, stdout.String(), stderr.String())
		}
		return probeDoc{}, fmt.Errorf("bench: probe failed %s exit %d (bad args produce exit 2 with no JSON): %w stdout %q stderr %q",
			label, code, err, stdout.String(), stderr.String())
	}
	if perr != nil {
		return probeDoc{}, fmt.Errorf("bench: probe output invalid %s: %w stdout %q stderr %q",
			label, perr, stdout.String(), stderr.String())
	}
	if stderr.Len() > 0 {
		t.Logf("probe stderr %s: %s", label, stderr.String())
	}
	return doc, nil
}

// interactiveDockerArgsOrErr inserts -i after exec, returning an error
// instead of failing so goroutines can forward it on channels.
func interactiveDockerArgsOrErr(t *testing.T, base []string) ([]string, error) {
	t.Helper()
	if len(base) == 0 || base[0] != "exec" {
		return nil, fmt.Errorf("bench: docker base argv misses exec: %v", base)
	}
	args := make([]string, 0, len(base)+1)
	args = append(args, "exec", "-i")
	args = append(args, base[1:]...)
	return args, nil
}

// execProbe delivers the probe script over docker exec stdin and parses the
// single stdout JSON document. Exit 2 with zero received is a discovery
// failure and fails loudly; any other nonzero exit fails loudly with the
// probe output. Extra stdout documents fail loudly too.
func execProbe(t *testing.T, ctx context.Context, dockerBase []string, script []byte, args []string, label string) probeDoc {
	t.Helper()
	doc, err := tryExecProbe(t, ctx, dockerBase, script, args, label)
	if err != nil {
		t.Fatalf("%v", err)
	}
	return doc
}

// weldContainer hosts the containerized Weld endpoint: the bench
// agent binary runs here via docker exec, one short-lived proc per leg.
const weldContainer = "weld"

// benchDockerPrefix binds the Jazzy peer container plus domain with no
// container command; execProbe appends the probe argv behind it.
func benchDockerPrefix(domain int) []string {
	return helpers.DockerArgs(helpers.Container, domain)
}

// weldDockerPrefix binds the weld container plus domain with no
// container command; execWeldAgent appends the agent argv behind it.
func weldDockerPrefix(domain int) []string {
	return helpers.DockerArgs(weldContainer, domain)
}

// tryExecWeldAgent runs /bench-agent in weld and parses the single
// stdout JSON document, returning an error instead of failing.
// Goroutines use it so agent failures arrive on channels for the test
// goroutine to fail loudly; execWeldAgent wraps it for direct calls.
// The agent mirrors the probe contract exactly (same argv flags, same
// single-doc stdout shape, same exit codes), so the doc reuses probeDoc
// and exit 2 with zero received is a discovery failure.
func tryExecWeldAgent(t *testing.T, ctx context.Context, dockerPrefix, args []string, label string) (probeDoc, error) {
	t.Helper()
	full := make([]string, 0, len(dockerPrefix)+len(args)+1)
	full = append(full, dockerPrefix...)
	full = append(full, "/bench-agent")
	full = append(full, args...)
	timeout := ctx
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		timeout, cancel = context.WithTimeout(ctx, benchDefaultTimeout)
		defer cancel()
	}
	cmd := exec.CommandContext(timeout, "docker", full...) //nolint:gosec // bench harness drives the agent with fixed docker argv
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	doc, perr := parseProbeDoc(stdout.Bytes())
	if err != nil {
		var exitErr *exec.ExitError
		code := -1
		if errors.As(err, &exitErr) {
			code = exitErr.ExitCode()
		}
		if code == 2 && perr == nil && doc.Received == 0 {
			return probeDoc{}, fmt.Errorf("bench: weld agent discovery failure %s: %w stdout %q stderr %q",
				label, err, stdout.String(), stderr.String())
		}
		return probeDoc{}, fmt.Errorf("bench: weld agent failed %s exit %d (bad args produce exit 2 with no JSON): %w stdout %q stderr %q",
			label, code, err, stdout.String(), stderr.String())
	}
	if perr != nil {
		return probeDoc{}, fmt.Errorf("bench: weld agent output invalid %s: %w stdout %q stderr %q",
			label, perr, stdout.String(), stderr.String())
	}
	if stderr.Len() > 0 {
		t.Logf("weld agent stderr %s: %s", label, stderr.String())
	}
	return doc, nil
}

// execWeldAgent runs /bench-agent in weld and parses the single
// stdout JSON document. Exit 2 with zero received is a discovery failure
// and fails loudly; any other nonzero exit fails loudly with the agent
// output. Extra stdout documents fail loudly too.
func execWeldAgent(t *testing.T, ctx context.Context, dockerPrefix, args []string, label string) probeDoc {
	t.Helper()
	doc, err := tryExecWeldAgent(t, ctx, dockerPrefix, args, label)
	if err != nil {
		t.Fatalf("%v", err)
	}
	return doc
}

// parseProbeDoc decodes exactly one JSON document from probe stdout.
func parseProbeDoc(stdout []byte) (probeDoc, error) {
	var doc probeDoc
	trimmed := bytes.TrimSpace(stdout)
	if len(trimmed) == 0 {
		return doc, errors.New("bench: probe stdout is empty") //nolint:wrapcheck // new error, nothing to wrap
	}
	dec := json.NewDecoder(bytes.NewReader(trimmed))
	err := dec.Decode(&doc)
	if err != nil {
		return doc, fmt.Errorf("bench: decode probe doc: %w", err)
	}
	var extra any
	err = dec.Decode(&extra)
	if err != io.EOF {
		if err == nil {
			return doc, errors.New("bench: probe stdout holds more than one JSON doc") //nolint:wrapcheck // new error, nothing to wrap
		}
		return doc, fmt.Errorf("bench: probe stdout trailing check: %w", err)
	}
	return doc, nil
}

// probeTimeout returns the probe --timeout for a paced series.
func probeTimeout(count int, rateHz float64) time.Duration {
	if rateHz <= 0 {
		return benchSaturationTake
	}
	return time.Duration(float64(count)/rateHz*float64(time.Second)) + benchProbeMargin
}

// checkProbeDoc fails loudly when the probe doc disagrees with the request.
// Sent versus received comparisons stay per leg: a sub probe never sends,
// so an in-doc overreceive check would false fire on every sub run.
func checkProbeDoc(t *testing.T, doc probeDoc, mode, topic string, count int, label string) {
	t.Helper()
	if doc.Mode != mode {
		t.Fatalf("probe mode %s = %q want %q", label, doc.Mode, mode)
	}
	if doc.Topic != topic {
		t.Fatalf("probe topic %s = %q want %q", label, doc.Topic, topic)
	}
	if doc.Count != count {
		t.Logf("probe %s count = %d want %d", label, doc.Count, count)
	}
}

// elapsedNs returns recvLast minus sendFirst guarding missing stamps.
func elapsedNs(sendFirst, recvLast int64, fallback time.Duration) int64 {
	if sendFirst > 0 && recvLast > sendFirst {
		return recvLast - sendFirst
	}
	return fallback.Nanoseconds()
}

// throughput derives messages per second and MiB per second.
func throughput(recv, payloadBytes int, elapsed time.Duration) (float64, float64) {
	secs := elapsed.Seconds()
	if secs <= 0 || recv <= 0 {
		return 0, 0
	}
	msgs := float64(recv) / secs
	mibs := float64(recv*payloadBytes) / secs / (1 << 20)
	return msgs, mibs
}

// summarizeLat fills a report Acc from int64 nanosecond samples.
func summarizeLat(acc *report.Acc, latNs []int64) {
	for _, v := range latNs {
		acc.Add(float64(v))
	}
}

// summarizeProbeLat fills a report Acc from probe float samples.
func summarizeProbeLat(acc *report.Acc, latNs []float64) {
	for _, v := range latNs {
		acc.Add(v)
	}
}

// assertThroughput fails loudly on any measured loss.
// assertThroughput fails loudly on any missing seq. Duplicates do not fail:
// reliable redelivery on dual-path multicast is lawful and counted
// separately in Dups. A zero Sent (discovery-only rows) skips.
func assertThroughput(t *testing.T, res report.Result) {
	t.Helper()
	if res.Sent == 0 {
		return
	}
	if len(res.Gaps) > 0 {
		t.Fatalf("bench loss %s %s: sent %d unique %d dups %d loss %.2f%% gaps %v notes %q",
			res.Scenario, res.Leg, res.Sent, res.Recv, res.Dups, res.LossPct, res.Gaps, res.Notes)
	}
}

// baseNotes formats the shared Notes kv prefix.
func baseNotes(liveID string, domain int, topic, extra string) string {
	notes := "run_id=" + liveID + ";domain=" + strconv.Itoa(domain) + ";topic=" + topic
	if extra != "" {
		notes += ";" + extra
	}
	return notes
}

// runWeldThroughput runs one L1 tier with an in-process publisher and
// subscriber: join both, subscribe transient local, wait discovery, drain
// warmup, then publish paced on a child goroutine while the test goroutine
// takes. The result row is recorded before the loss assert so the HTML
// keeps the evidence for a loud failure.
func runWeldThroughput(t *testing.T, ctx context.Context, run *benchRun, domain int, topic string, tr tier, rateHz float64) {
	t.Helper()
	if tr.size < benchMinSize {
		t.Fatalf("tier %s size %d below header %d", tr.name, tr.size, benchMinSize)
	}
	pub := run.openBackend(t, ctx, domain, "bench-pub-"+run.liveID)
	sub := run.openBackend(t, ctx, domain, "bench-sub-"+run.liveID)
	subscribeBenchReader(t, ctx, sub, topic, "l1")
	announceWriter(t, ctx, pub, topic, tr.size, tr.warmup+tr.count+1)
	discoveryMs := waitDiscoveryMs(t, ctx, sub, topic, "l1-sub")
	helpers.Sleep(ctx, benchSettle)
	if ctx.Err() != nil {
		t.Fatalf("bench l1 %s context ended after discovery: %v", tr.name, ctx.Err())
	}
	if tr.warmup > 0 {
		warmCtx, warmCancel := context.WithTimeout(ctx, takeBudget(tr.warmup, rateHz))
		defer warmCancel()
		warmCh := make(chan pubOutcome, 1)
		go func() {
			warmCh <- publishSeries(warmCtx, pub, topic, tr.warmup, 1, tr.size, rateHz)
		}()
		warmTake := collectTakes(warmCtx, sub, tr.warmup)
		warmPub := <-warmCh
		if warmPub.err != nil {
			t.Fatalf("bench l1 %s warmup publish: %v", tr.name, warmPub.err)
		}
		if warmTake.err != nil {
			t.Fatalf("bench l1 %s warmup take: %v", tr.name, warmTake.err)
		}
	}
	cpuBefore, err := sampleCPU()
	if err != nil {
		t.Fatalf("bench l1 %s cpu before: %v", tr.name, err)
	}
	heapBefore := heapAllocMiB()
	takeCtx, takeCancel := context.WithTimeout(ctx, takeBudget(tr.count, rateHz))
	defer takeCancel()
	pubCh := make(chan pubOutcome, 1)
	go func() {
		pubCh <- publishSeries(takeCtx, pub, topic, tr.count, 1, tr.size, rateHz)
	}()
	got := collectTakes(takeCtx, sub, tr.count)
	pubOut := <-pubCh
	if pubOut.err != nil {
		t.Fatalf("bench l1 %s publish: %v", tr.name, pubOut.err)
	}
	if got.err != nil {
		t.Fatalf("bench l1 %s take: %v (got %d/%d)", tr.name, got.err, len(got.seen), tr.count)
	}
	cpuAfter, err := sampleCPU()
	if err != nil {
		t.Fatalf("bench l1 %s cpu after: %v", tr.name, err)
	}
	rssMiB, err := readRSSMiB()
	if err != nil {
		t.Fatalf("bench l1 %s rss: %v", tr.name, err)
	}
	gaps := computeGaps(got.seen, tr.count)
	slices.Sort(got.latNs)
	var acc report.Acc
	summarizeLat(&acc, got.latNs)
	elapsed := time.Duration(elapsedNs(pubOut.firstNs, got.lastNs, takeBudget(tr.count, rateHz)))
	thrMsgs, thrMiB := throughput(len(got.seen), tr.size, elapsed)
	notes := baseNotes(run.liveID, domain, topic,
		"heap_delta_mib="+strconv.FormatFloat(heapAllocMiB()-heapBefore, 'f', 3, 64)+skippedNote(got.skipped))
	res := report.Result{
		Scenario:     tr.name,
		Leg:          "L1",
		PayloadBytes: tr.size,
		RateHz:       rateHz,
		WarmupN:      tr.warmup,
		MeasuredN:    tr.count,
		Sent:         pubOut.sent,
		Recv:         len(got.seen),
		Dups:         got.dups,
		Gaps:         gaps,
		LossPct:      lossPct(pubOut.sent, len(got.seen)),
		LatNs:        acc.Summary(),
		ThrMsgs:      thrMsgs,
		ThrMiBs:      thrMiB,
		CPUS:         cpuSeconds(cpuBefore, cpuAfter),
		RSSMiB:       rssMiB,
		DiscoveryMs:  discoveryMs,
		Notes:        notes,
	}
	run.record(res)
	if isHeadroomTier(tr) {
		return
	}
	assertThroughput(t, res)
}

// runWeldContainerThroughput runs one L1c tier with two agent nodes in
// weld: an agent sub starts first, then an agent pub with a bounded
// match wait. Both rows derive from the agent docs; discovery is agent
// managed and recorded as zero with a note. Warmup free like the probe
// legs: a transient local late joiner replays history, so the match wait
// plus settle replaces warmup.
func runWeldContainerThroughput(t *testing.T, ctx context.Context, run *benchRun, domain int, agentPrefix []string, topic string, tr tier, rateHz float64) {
	t.Helper()
	subArgs := probeArgs("sub", topic, tr.count, rateHz, tr.size, probeTimeout(tr.count, rateHz), run.liveID)
	subArgs = append(subArgs, probeMatchWaitArgs()...)
	subCtx, subCancel := context.WithTimeout(ctx, probeTimeout(tr.count, rateHz)+benchTakeMargin)
	defer subCancel()
	type docOut struct {
		doc probeDoc
		err error
	}
	subCh := make(chan docOut, 1)
	go func() {
		doc, err := tryExecWeldAgent(t, subCtx, agentPrefix, subArgs, "l1c-sub")
		subCh <- docOut{doc: doc, err: err}
	}()
	helpers.Sleep(ctx, benchSettle)
	if ctx.Err() != nil {
		t.Fatalf("bench l1c %s context ended before pub: %v", tr.name, ctx.Err())
	}
	pubArgs := probeArgs("pub", topic, tr.count, rateHz, tr.size, probeTimeout(tr.count, rateHz), run.liveID)
	pubArgs = append(pubArgs, probeMatchWaitArgs()...)
	pubDoc := execWeldAgent(t, subCtx, agentPrefix, pubArgs, "l1c-pub")
	var subDoc probeDoc
	select {
	case out := <-subCh:
		if out.err != nil {
			t.Fatalf("bench l1c %s sub agent: %v", tr.name, out.err)
		}
		subDoc = out.doc
	case <-subCtx.Done():
		t.Fatalf("bench l1c %s sub wait ended: %v", tr.name, subCtx.Err())
	}
	checkProbeDoc(t, pubDoc, "pub", topic, tr.count, "l1c-pub")
	checkProbeDoc(t, subDoc, "sub", topic, tr.count, "l1c-sub")
	var acc report.Acc
	summarizeProbeLat(&acc, subDoc.LatNs)
	elapsed := time.Duration(elapsedNs(subDoc.SendFirstNs, subDoc.RecvLastNs, probeTimeout(tr.count, rateHz)))
	thrMsgs, thrMiB := throughput(uniqueRecv(pubDoc.Sent, subDoc.Gaps), tr.size, elapsed)
	notes := baseNotes(run.liveID, domain, topic,
		"discovery=agent-managed;agent_pub_note="+pubDoc.Note+";agent_sub_note="+subDoc.Note)
	res := report.Result{
		Scenario:     tr.name,
		Leg:          "L1c",
		PayloadBytes: tr.size,
		RateHz:       rateHz,
		WarmupN:      0,
		MeasuredN:    tr.count,
		Sent:         pubDoc.Sent,
		Recv:         uniqueRecv(pubDoc.Sent, subDoc.Gaps),
		Dups:         subDoc.Dups,
		Gaps:         subDoc.Gaps,
		LossPct:      lossPct(pubDoc.Sent, uniqueRecv(pubDoc.Sent, subDoc.Gaps)),
		LatNs:        acc.Summary(),
		ThrMsgs:      thrMsgs,
		ThrMiBs:      thrMiB,
		DiscoveryMs:  0,
		Notes:        notes,
	}
	run.record(res)
	if isHeadroomTier(tr) {
		return
	}
	assertThroughput(t, res)
}

// runForward runs one L3 tier: an agent pub in weld plus a probe
// sub in the ROS peer. The probe sub starts first; the agent pub waits
// for the match bounded, since probe sub startup under load is not
// guaranteed. Warmup free like before: transient local history replay
// would pollute probe counts, so the match wait plus settle replaces
// warmup. Discovery is container managed and recorded as zero with a
// note; like L2 no host CPU or RSS is reported.
func runForward(t *testing.T, ctx context.Context, run *benchRun, domain int, dockerPrefix []string, topic string, tr tier, rateHz float64) {
	t.Helper()
	script := loadProbeScript(t)
	agentPrefix := weldDockerPrefix(domain)
	subArgs := probeArgs("sub", topic, tr.count, rateHz, tr.size, probeTimeout(tr.count, rateHz), run.liveID)
	subCtx, subCancel := context.WithTimeout(ctx, probeTimeout(tr.count, rateHz)+benchTakeMargin)
	defer subCancel()
	type docOut struct {
		doc probeDoc
		err error
	}
	docCh := make(chan docOut, 1)
	go func() {
		doc, err := tryExecProbe(t, subCtx, dockerPrefix, script, subArgs, "l3-sub")
		docCh <- docOut{doc: doc, err: err}
	}()
	helpers.Sleep(ctx, benchSettle)
	if ctx.Err() != nil {
		t.Fatalf("bench l3 %s context ended before publish: %v", tr.name, ctx.Err())
	}
	pubArgs := probeArgs("pub", topic, tr.count, rateHz, tr.size, probeTimeout(tr.count, rateHz), run.liveID)
	pubArgs = append(pubArgs, probeMatchWaitArgs()...)
	pubDoc := execWeldAgent(t, subCtx, agentPrefix, pubArgs, "l3-pub")
	var subDoc probeDoc
	select {
	case out := <-docCh:
		if out.err != nil {
			t.Fatalf("bench l3 %s probe: %v", tr.name, out.err)
		}
		subDoc = out.doc
	case <-subCtx.Done():
		t.Fatalf("bench l3 %s probe wait ended: %v", tr.name, subCtx.Err())
	}
	checkProbeDoc(t, pubDoc, "pub", topic, tr.count, "l3-pub")
	checkProbeDoc(t, subDoc, "sub", topic, tr.count, "l3")
	var acc report.Acc
	summarizeProbeLat(&acc, subDoc.LatNs)
	elapsed := time.Duration(elapsedNs(subDoc.SendFirstNs, subDoc.RecvLastNs, probeTimeout(tr.count, rateHz)))
	thrMsgs, thrMiB := throughput(uniqueRecv(pubDoc.Sent, subDoc.Gaps), tr.size, elapsed)
	notes := baseNotes(run.liveID, domain, topic,
		"discovery=agent-managed;agent_pub_note="+pubDoc.Note+";probe_note="+subDoc.Note)
	res := report.Result{
		Scenario:     tr.name,
		Leg:          "L3",
		PayloadBytes: tr.size,
		RateHz:       rateHz,
		WarmupN:      0,
		MeasuredN:    tr.count,
		Sent:         pubDoc.Sent,
		Recv:         uniqueRecv(pubDoc.Sent, subDoc.Gaps),
		Dups:         subDoc.Dups,
		Gaps:         subDoc.Gaps,
		LossPct:      lossPct(pubDoc.Sent, uniqueRecv(pubDoc.Sent, subDoc.Gaps)),
		LatNs:        acc.Summary(),
		ThrMsgs:      thrMsgs,
		ThrMiBs:      thrMiB,
		DiscoveryMs:  0,
		Notes:        notes,
	}
	run.record(res)
	if isHeadroomTier(tr) {
		return
	}
	assertThroughput(t, res)
}

// runReverse runs one L4 tier: a probe pub in the ROS peer plus an agent
// sub in weld. The agent sub starts first; the probe pub waits for
// the match bounded so early samples are never lost before discovery.
// Warmup free like forward; discovery is container managed and recorded
// as zero with a note.
func runReverse(t *testing.T, ctx context.Context, run *benchRun, domain int, dockerPrefix []string, topic string, tr tier, rateHz float64) {
	t.Helper()
	script := loadProbeScript(t)
	agentPrefix := weldDockerPrefix(domain)
	subArgs := probeArgs("sub", topic, tr.count, rateHz, tr.size, probeTimeout(tr.count, rateHz), run.liveID)
	subArgs = append(subArgs, probeMatchWaitArgs()...)
	subCtx, subCancel := context.WithTimeout(ctx, probeTimeout(tr.count, rateHz)+benchTakeMargin)
	defer subCancel()
	type docOut struct {
		doc probeDoc
		err error
	}
	subCh := make(chan docOut, 1)
	go func() {
		doc, err := tryExecWeldAgent(t, subCtx, agentPrefix, subArgs, "l4-sub")
		subCh <- docOut{doc: doc, err: err}
	}()
	helpers.Sleep(ctx, benchSettle)
	if ctx.Err() != nil {
		t.Fatalf("bench l4 %s context ended before pub: %v", tr.name, ctx.Err())
	}
	pubArgs := probeArgs("pub", topic, tr.count, rateHz, tr.size, probeTimeout(tr.count, rateHz), run.liveID)
	pubArgs = append(pubArgs, probeMatchWaitArgs()...)
	pubDoc := execProbe(t, subCtx, dockerPrefix, script, pubArgs, "l4-pub")
	var subDoc probeDoc
	select {
	case out := <-subCh:
		if out.err != nil {
			t.Fatalf("bench l4 %s sub agent: %v", tr.name, out.err)
		}
		subDoc = out.doc
	case <-subCtx.Done():
		t.Fatalf("bench l4 %s sub wait ended: %v", tr.name, subCtx.Err())
	}
	checkProbeDoc(t, pubDoc, "pub", topic, tr.count, "l4")
	checkProbeDoc(t, subDoc, "sub", topic, tr.count, "l4-sub")
	var acc report.Acc
	summarizeProbeLat(&acc, subDoc.LatNs)
	elapsed := time.Duration(elapsedNs(subDoc.SendFirstNs, subDoc.RecvLastNs, probeTimeout(tr.count, rateHz)))
	thrMsgs, thrMiB := throughput(uniqueRecv(pubDoc.Sent, subDoc.Gaps), tr.size, elapsed)
	notes := baseNotes(run.liveID, domain, topic,
		"discovery=agent-managed;probe_note="+pubDoc.Note+";agent_sub_note="+subDoc.Note)
	res := report.Result{
		Scenario:     tr.name,
		Leg:          "L4",
		PayloadBytes: tr.size,
		RateHz:       rateHz,
		WarmupN:      0,
		MeasuredN:    tr.count,
		Sent:         pubDoc.Sent,
		Recv:         uniqueRecv(pubDoc.Sent, subDoc.Gaps),
		Dups:         subDoc.Dups,
		Gaps:         subDoc.Gaps,
		LossPct:      lossPct(pubDoc.Sent, uniqueRecv(pubDoc.Sent, subDoc.Gaps)),
		LatNs:        acc.Summary(),
		ThrMsgs:      thrMsgs,
		ThrMiBs:      thrMiB,
		DiscoveryMs:  0,
		Notes:        notes,
	}
	run.record(res)
	if isHeadroomTier(tr) {
		return
	}
	assertThroughput(t, res)
}

// runRosRos runs one L2 tier with two probe nodes in one peer container:
// a sub probe starts first, then a pub probe. Both rows derive from the
// probe docs; discovery is ros managed and recorded as zero with a note.
func runRosRos(t *testing.T, ctx context.Context, run *benchRun, domain int, dockerPrefix []string, topic string, tr tier, rateHz float64) {
	t.Helper()
	script := loadProbeScript(t)
	subArgs := probeArgs("sub", topic, tr.count, rateHz, tr.size, probeTimeout(tr.count, rateHz), run.liveID)
	subCtx, subCancel := context.WithTimeout(ctx, probeTimeout(tr.count, rateHz)+benchTakeMargin)
	defer subCancel()
	type docOut struct {
		doc probeDoc
		err error
	}
	subCh := make(chan docOut, 1)
	go func() {
		doc, err := tryExecProbe(t, subCtx, dockerPrefix, script, subArgs, "l2-sub")
		subCh <- docOut{doc: doc, err: err}
	}()
	helpers.Sleep(ctx, benchSettle)
	if ctx.Err() != nil {
		t.Fatalf("bench l2 %s context ended before pub: %v", tr.name, ctx.Err())
	}
	pubArgs := probeArgs("pub", topic, tr.count, rateHz, tr.size, probeTimeout(tr.count, rateHz), run.liveID)
	pubDoc := execProbe(t, subCtx, dockerPrefix, script, pubArgs, "l2-pub")
	var subDoc probeDoc
	select {
	case out := <-subCh:
		if out.err != nil {
			t.Fatalf("bench l2 %s sub probe: %v", tr.name, out.err)
		}
		subDoc = out.doc
	case <-subCtx.Done():
		t.Fatalf("bench l2 %s sub wait ended: %v", tr.name, subCtx.Err())
	}
	checkProbeDoc(t, pubDoc, "pub", topic, tr.count, "l2-pub")
	checkProbeDoc(t, subDoc, "sub", topic, tr.count, "l2-sub")
	var acc report.Acc
	summarizeProbeLat(&acc, subDoc.LatNs)
	elapsed := time.Duration(elapsedNs(subDoc.SendFirstNs, subDoc.RecvLastNs, probeTimeout(tr.count, rateHz)))
	thrMsgs, thrMiB := throughput(uniqueRecv(pubDoc.Sent, subDoc.Gaps), tr.size, elapsed)
	notes := baseNotes(run.liveID, domain, topic,
		"discovery=ros-managed;pub_note="+pubDoc.Note+";sub_note="+subDoc.Note)
	res := report.Result{
		Scenario:     tr.name,
		Leg:          "L2",
		PayloadBytes: tr.size,
		RateHz:       rateHz,
		WarmupN:      0,
		MeasuredN:    tr.count,
		Sent:         pubDoc.Sent,
		Recv:         uniqueRecv(pubDoc.Sent, subDoc.Gaps),
		Dups:         subDoc.Dups,
		Gaps:         subDoc.Gaps,
		LossPct:      lossPct(pubDoc.Sent, uniqueRecv(pubDoc.Sent, subDoc.Gaps)),
		LatNs:        acc.Summary(),
		ThrMsgs:      thrMsgs,
		ThrMiBs:      thrMiB,
		DiscoveryMs:  0,
		Notes:        notes,
	}
	run.record(res)
	if isHeadroomTier(tr) {
		return
	}
	assertThroughput(t, res)
}

// Scale dimensions for the full matrix. Fanout fans one publisher out to
// benchFanoutSubs subscribers; fanin fans benchFaninPubs publishers with
// disjoint seq spaces into one subscriber. Count and size stay P0-like so
// scale isolates endpoint count, not payload.
const (
	benchFanoutSubs = 5
	benchFaninPubs  = 5
	benchScaleCount = 300
	benchScaleSize  = 256
	benchScaleRate  = 100.0
)

// recordFanoutSub checks one fanout subscriber doc and records its row.
// kind names the stack ("agent" or "probe") in the note so rows from
// different transports stay distinguishable. Subscribers are expected to
// see the full seq space; gaps fail loudly.
func recordFanoutSub(t *testing.T, run *benchRun, domain int, topic, scenario, leg, subLabel, kind string, idx, nsubs, sent int, pubNote string, sub probeDoc) {
	t.Helper()
	checkProbeDoc(t, sub, "sub", topic, benchScaleCount, subLabel)
	var acc report.Acc
	summarizeProbeLat(&acc, sub.LatNs)
	elapsed := time.Duration(elapsedNs(sub.SendFirstNs, sub.RecvLastNs, probeTimeout(benchScaleCount, benchScaleRate)))
	thrMsgs, thrMiB := throughput(uniqueRecv(sent, sub.Gaps), benchScaleSize, elapsed)
	notes := baseNotes(run.liveID, domain, topic,
		fmt.Sprintf("fanout sub=%d/%d;%s_pub_note=%s;%s_sub_note=%s", idx+1, nsubs, kind, pubNote, kind, sub.Note))
	res := report.Result{
		Scenario:     scenario,
		Leg:          leg,
		PayloadBytes: benchScaleSize,
		RateHz:       benchScaleRate,
		WarmupN:      0,
		MeasuredN:    benchScaleCount,
		Sent:         sent,
		Recv:         uniqueRecv(sent, sub.Gaps),
		Dups:         sub.Dups,
		Gaps:         sub.Gaps,
		LossPct:      lossPct(sent, uniqueRecv(sent, sub.Gaps)),
		LatNs:        acc.Summary(),
		ThrMsgs:      thrMsgs,
		ThrMiBs:      thrMiB,
		DiscoveryMs:  0,
		Notes:        notes,
	}
	run.record(res)
	assertThroughput(t, res)
}

// runFanoutInproc runs L1 fanout: one in-process publisher to
// benchFanoutSubs in-process subscribers on one topic. Each subscriber
// gets its own row so per-subscriber tail latency is visible. CPU and
// memory are process-wide and shared, so per-row cost fields stay zero
// with a note; see the L1 rows for process costs.
func runFanoutInproc(t *testing.T, ctx context.Context, run *benchRun, domain int, topic string) {
	t.Helper()
	const count = benchScaleCount
	pub := run.openBackend(t, ctx, domain, "bench-pub-"+run.liveID)
	subs := make([]*dds.Backend, 0, benchFanoutSubs)
	for i := range benchFanoutSubs {
		sub := run.openBackend(t, ctx, domain, fmt.Sprintf("bench-sub-%d-%s", i, run.liveID))
		subscribeBenchReader(t, ctx, sub, topic, "l1-fanout")
		subs = append(subs, sub)
	}
	announceWriter(t, ctx, pub, topic, benchScaleSize, count+1)
	for i, sub := range subs {
		waitDiscoveryMs(t, ctx, sub, topic, fmt.Sprintf("l1-fanout-sub%d", i))
	}
	helpers.Sleep(ctx, benchSettle)
	if ctx.Err() != nil {
		t.Fatalf("bench l1 fanout context ended before publish: %v", ctx.Err())
	}
	takeCtx, takeCancel := context.WithTimeout(ctx, takeBudget(count, benchScaleRate))
	defer takeCancel()
	type takeRes struct {
		idx int
		got takeResult
	}
	takeCh := make(chan takeRes, len(subs))
	for i, sub := range subs {
		go func(i int, sub *dds.Backend) {
			takeCh <- takeRes{idx: i, got: collectTakes(takeCtx, sub, count)}
		}(i, sub)
	}
	pubCh := make(chan pubOutcome, 1)
	go func() {
		pubCh <- publishSeries(takeCtx, pub, topic, count, 1, benchScaleSize, benchScaleRate)
	}()
	pubOut := <-pubCh
	if pubOut.err != nil {
		t.Fatalf("bench l1 fanout publish: %v", pubOut.err)
	}
	for range subs {
		one := <-takeCh
		if one.got.err != nil {
			t.Fatalf("bench l1 fanout take sub %d: %v", one.idx, one.got.err)
		}
		gaps := computeGaps(one.got.seen, count)
		slices.Sort(one.got.latNs)
		var acc report.Acc
		summarizeLat(&acc, one.got.latNs)
		elapsed := time.Duration(elapsedNs(pubOut.firstNs, one.got.lastNs, takeBudget(count, benchScaleRate)))
		thrMsgs, thrMiB := throughput(len(one.got.seen), benchScaleSize, elapsed)
		notes := baseNotes(run.liveID, domain, topic,
			fmt.Sprintf("fanout sub=%d/%d shared-run-no-per-row-cpu", one.idx+1, len(subs))+skippedNote(one.got.skipped))
		res := report.Result{
			Scenario:     "fanout",
			Leg:          "L1",
			PayloadBytes: benchScaleSize,
			RateHz:       benchScaleRate,
			WarmupN:      0,
			MeasuredN:    count,
			Sent:         pubOut.sent,
			Recv:         len(one.got.seen),
			Dups:         one.got.dups,
			Gaps:         gaps,
			LossPct:      lossPct(pubOut.sent, len(one.got.seen)),
			LatNs:        acc.Summary(),
			ThrMsgs:      thrMsgs,
			ThrMiBs:      thrMiB,
			CPUS:         0,
			RSSMiB:       0,
			DiscoveryMs:  0,
			Notes:        notes,
		}
		run.record(res)
		assertThroughput(t, res)
	}
}

// runFaninInproc runs L1 fanin: benchFaninPubs in-process publishers with
// disjoint seq spaces tiling [1..total] into one in-process subscriber.
// Missing seqs are real loss: every publisher is paced identically and the
// sub starts first.
func runFaninInproc(t *testing.T, ctx context.Context, run *benchRun, domain int, topic string) {
	t.Helper()
	const pubs = benchFaninPubs
	const perPub = benchScaleCount / benchFaninPubs
	const total = pubs * perPub
	sub := run.openBackend(t, ctx, domain, "bench-sub-"+run.liveID)
	subscribeBenchReader(t, ctx, sub, topic, "l1-fanin")
	publishers := make([]*dds.Backend, 0, pubs)
	for i := range pubs {
		publishers = append(publishers, run.openBackend(t, ctx, domain, fmt.Sprintf("bench-pub-%d-%s", i, run.liveID)))
	}
	announceWriter(t, ctx, publishers[0], topic, benchScaleSize, total+1)
	waitDiscoveryMs(t, ctx, sub, topic, "l1-fanin-sub")
	helpers.Sleep(ctx, benchSettle)
	if ctx.Err() != nil {
		t.Fatalf("bench l1 fanin context ended before publish: %v", ctx.Err())
	}
	takeCtx, takeCancel := context.WithTimeout(ctx, takeBudget(total, benchScaleRate))
	defer takeCancel()
	type pubRes struct {
		sent int
		err  error
	}
	pubCh := make(chan pubRes, pubs)
	startNs := time.Now().UnixNano()
	for i, pub := range publishers {
		go func(i int, pub *dds.Backend) {
			out := publishSeries(takeCtx, pub, topic, perPub, i*perPub+1, benchScaleSize, benchScaleRate)
			pubCh <- pubRes{sent: out.sent, err: out.err}
		}(i, pub)
	}
	got := collectTakes(takeCtx, sub, total)
	sent := 0
	for range publishers {
		one := <-pubCh
		if one.err != nil {
			t.Fatalf("bench l1 fanin publish: %v", one.err)
		}
		sent += one.sent
	}
	if got.err != nil {
		t.Fatalf("bench l1 fanin take: %v (got %d/%d)", got.err, len(got.seen), total)
	}
	gaps := computeGaps(got.seen, total)
	slices.Sort(got.latNs)
	var acc report.Acc
	summarizeLat(&acc, got.latNs)
	elapsed := time.Duration(elapsedNs(startNs, got.lastNs, takeBudget(total, benchScaleRate)))
	thrMsgs, thrMiB := throughput(len(got.seen), benchScaleSize, elapsed)
	notes := baseNotes(run.liveID, domain, topic,
		fmt.Sprintf("fanin pubs=%d perpub=%d", pubs, perPub)+skippedNote(got.skipped))
	res := report.Result{
		Scenario:     "fanin",
		Leg:          "L1",
		PayloadBytes: benchScaleSize,
		RateHz:       benchScaleRate,
		WarmupN:      0,
		MeasuredN:    total,
		Sent:         sent,
		Recv:         len(got.seen),
		Dups:         got.dups,
		Gaps:         gaps,
		LossPct:      lossPct(sent, len(got.seen)),
		LatNs:        acc.Summary(),
		ThrMsgs:      thrMsgs,
		ThrMiBs:      thrMiB,
		CPUS:         0,
		RSSMiB:       0,
		DiscoveryMs:  0,
		Notes:        notes,
	}
	run.record(res)
	assertThroughput(t, res)
}

// runFanoutAgent runs L1c fanout: one agent pub plus benchFanoutSubs agent
// subs in the weld container, all on one per-run topic. Each subscriber
// gets its own row.
func runFanoutAgent(t *testing.T, ctx context.Context, run *benchRun, domain int, topic string) {
	t.Helper()
	agentPrefix := weldDockerPrefix(domain)
	scriptArgs := func(mode string, count int) []string {
		return probeArgs(mode, topic, count, benchScaleRate, benchScaleSize, probeTimeout(count, benchScaleRate), run.liveID)
	}
	type subDoc struct {
		doc probeDoc
		err error
	}
	subCh := make(chan subDoc, benchFanoutSubs)
	for range benchFanoutSubs {
		args := scriptArgs("sub", benchScaleCount)
		args = append(args, probeMatchWaitArgs()...)
		go func(args []string) {
			doc, err := tryExecWeldAgent(t, ctx, agentPrefix, args, "l1c-fanout-sub")
			subCh <- subDoc{doc: doc, err: err}
		}(args)
	}
	helpers.Sleep(ctx, benchSettle)
	if ctx.Err() != nil {
		t.Fatalf("bench l1c fanout context ended before publish: %v", ctx.Err())
	}
	pubArgs := scriptArgs("pub", benchScaleCount)
	pubArgs = append(pubArgs, probeMatchWaitArgs()...)
	pubDoc := execWeldAgent(t, ctx, agentPrefix, pubArgs, "l1c-fanout-pub")
	checkProbeDoc(t, pubDoc, "pub", topic, benchScaleCount, "l1c-fanout-pub")
	for i := range benchFanoutSubs {
		var out subDoc
		select {
		case out = <-subCh:
			if out.err != nil {
				t.Fatalf("bench l1c fanout sub agent: %v", out.err)
			}
		case <-ctx.Done():
			t.Fatalf("bench l1c fanout sub wait ended: %v", ctx.Err())
		}
		recordFanoutSub(t, run, domain, topic, "fanout", "L1c", "l1c-fanout-sub", "agent", i, benchFanoutSubs, pubDoc.Sent, pubDoc.Note, out.doc)
	}
}

// runFaninAgent runs L1c fanin: benchFaninPubs agent publishers with
// disjoint seq spaces tiling [1..total] into one agent subscriber.
func runFaninAgent(t *testing.T, ctx context.Context, run *benchRun, domain int, topic string) {
	t.Helper()
	agentPrefix := weldDockerPrefix(domain)
	const pubs = benchFaninPubs
	const perPub = benchScaleCount / benchFaninPubs
	const total = pubs * perPub
	subArgs := probeArgs("sub", topic, total, benchScaleRate, benchScaleSize, probeTimeout(total, benchScaleRate), run.liveID)
	subArgs = append(subArgs, probeMatchWaitArgs()...)
	subCtx, subCancel := context.WithTimeout(ctx, probeTimeout(total, benchScaleRate)+benchTakeMargin)
	defer subCancel()
	type subDoc struct {
		doc probeDoc
		err error
	}
	subCh := make(chan subDoc, 1)
	go func() {
		doc, err := tryExecWeldAgent(t, subCtx, agentPrefix, subArgs, "l1c-fanin-sub")
		subCh <- subDoc{doc: doc, err: err}
	}()
	helpers.Sleep(ctx, benchSettle)
	if ctx.Err() != nil {
		t.Fatalf("bench l1c fanin context ended before publish: %v", ctx.Err())
	}
	type pubDoc struct {
		doc probeDoc
		err error
	}
	pubCh := make(chan pubDoc, pubs)
	for i := range pubs {
		base := i*perPub + 1
		args := probeArgs("pub", topic, perPub, benchScaleRate, benchScaleSize, probeTimeout(perPub, benchScaleRate), run.liveID)
		args = append(args, "--seq-base", strconv.Itoa(base))
		args = append(args, probeMatchWaitArgs()...)
		go func(args []string) {
			doc, err := tryExecWeldAgent(t, subCtx, agentPrefix, args, "l1c-fanin-pub")
			pubCh <- pubDoc{doc: doc, err: err}
		}(args)
	}
	sent := 0
	for range pubs {
		var out pubDoc
		select {
		case out = <-pubCh:
			if out.err != nil {
				t.Fatalf("bench l1c fanin pub: %v", out.err)
			}
		case <-subCtx.Done():
			t.Fatalf("bench l1c fanin pub wait ended: %v", subCtx.Err())
		}
		checkProbeDoc(t, out.doc, "pub", topic, perPub, "l1c-fanin-pub")
		sent += out.doc.Sent
	}
	var sub probeDoc
	select {
	case out := <-subCh:
		if out.err != nil {
			t.Fatalf("bench l1c fanin sub agent: %v", out.err)
		}
		sub = out.doc
	case <-subCtx.Done():
		t.Fatalf("bench l1c fanin sub wait ended: %v", subCtx.Err())
	}
	checkProbeDoc(t, sub, "sub", topic, total, "l1c-fanin-sub")
	var acc report.Acc
	summarizeProbeLat(&acc, sub.LatNs)
	elapsed := time.Duration(elapsedNs(sub.SendFirstNs, sub.RecvLastNs, probeTimeout(total, benchScaleRate)))
	thrMsgs, thrMiB := throughput(uniqueRecv(sent, sub.Gaps), benchScaleSize, elapsed)
	notes := baseNotes(run.liveID, domain, topic,
		fmt.Sprintf("fanin pubs=%d perpub=%d;agent_sub_note=%s", pubs, perPub, sub.Note))
	res := report.Result{
		Scenario:     "fanin",
		Leg:          "L1c",
		PayloadBytes: benchScaleSize,
		RateHz:       benchScaleRate,
		WarmupN:      0,
		MeasuredN:    total,
		Sent:         sent,
		Recv:         uniqueRecv(sent, sub.Gaps),
		Dups:         sub.Dups,
		Gaps:         sub.Gaps,
		LossPct:      lossPct(sent, uniqueRecv(sent, sub.Gaps)),
		LatNs:        acc.Summary(),
		ThrMsgs:      thrMsgs,
		ThrMiBs:      thrMiB,
		DiscoveryMs:  0,
		Notes:        notes,
	}
	run.record(res)
	assertThroughput(t, res)
}

// runFanoutProbe runs L2 fanout: one probe pub plus benchFanoutSubs probe
// subs in the ROS peer, all on one per-run topic.
func runFanoutProbe(t *testing.T, ctx context.Context, run *benchRun, domain int, topic string) {
	t.Helper()
	dockerPrefix := benchDockerPrefix(domain)
	script := loadProbeScript(t)
	type subDoc struct {
		doc probeDoc
		err error
	}
	subCh := make(chan subDoc, benchFanoutSubs)
	for range benchFanoutSubs {
		subArgs := probeArgs("sub", topic, benchScaleCount, benchScaleRate, benchScaleSize, probeTimeout(benchScaleCount, benchScaleRate), run.liveID)
		go func(args []string) {
			doc, err := tryExecProbe(t, ctx, dockerPrefix, script, args, "l2-fanout-sub")
			subCh <- subDoc{doc: doc, err: err}
		}(subArgs)
	}
	helpers.Sleep(ctx, benchSettle)
	if ctx.Err() != nil {
		t.Fatalf("bench l2 fanout context ended before publish: %v", ctx.Err())
	}
	pubArgs := probeArgs("pub", topic, benchScaleCount, benchScaleRate, benchScaleSize, probeTimeout(benchScaleCount, benchScaleRate), run.liveID)
	pubDoc := execProbe(t, ctx, dockerPrefix, script, pubArgs, "l2-fanout-pub")
	checkProbeDoc(t, pubDoc, "pub", topic, benchScaleCount, "l2-fanout-pub")
	for i := range benchFanoutSubs {
		var out subDoc
		select {
		case out = <-subCh:
			if out.err != nil {
				t.Fatalf("bench l2 fanout sub probe: %v", out.err)
			}
		case <-ctx.Done():
			t.Fatalf("bench l2 fanout sub wait ended: %v", ctx.Err())
		}
		recordFanoutSub(t, run, domain, topic, "fanout", "L2", "l2-fanout-sub", "probe", i, benchFanoutSubs, pubDoc.Sent, pubDoc.Note, out.doc)
	}
}

// runFaninProbe runs L2 fanin: benchFaninPubs probe publishers with disjoint
// seq spaces tiling [1..total] into one probe subscriber.
func runFaninProbe(t *testing.T, ctx context.Context, run *benchRun, domain int, topic string) {
	t.Helper()
	dockerPrefix := benchDockerPrefix(domain)
	script := loadProbeScript(t)
	const pubs = benchFaninPubs
	const perPub = benchScaleCount / benchFaninPubs
	const total = pubs * perPub
	subArgs := probeArgs("sub", topic, total, benchScaleRate, benchScaleSize, probeTimeout(total, benchScaleRate), run.liveID)
	subCtx, subCancel := context.WithTimeout(ctx, probeTimeout(total, benchScaleRate)+benchTakeMargin)
	defer subCancel()
	type subDoc struct {
		doc probeDoc
		err error
	}
	subCh := make(chan subDoc, 1)
	go func() {
		doc, err := tryExecProbe(t, subCtx, dockerPrefix, script, subArgs, "l2-fanin-sub")
		subCh <- subDoc{doc: doc, err: err}
	}()
	helpers.Sleep(ctx, benchSettle)
	if ctx.Err() != nil {
		t.Fatalf("bench l2 fanin context ended before publish: %v", ctx.Err())
	}
	type pubDocOut struct {
		doc probeDoc
		err error
	}
	pubCh := make(chan pubDocOut, pubs)
	for i := range pubs {
		args := probeArgs("pub", topic, perPub, benchScaleRate, benchScaleSize, probeTimeout(perPub, benchScaleRate), run.liveID)
		args = append(args, "--seq-base", strconv.Itoa(i*perPub+1))
		go func(args []string) {
			doc, err := tryExecProbe(t, subCtx, dockerPrefix, script, args, "l2-fanin-pub")
			pubCh <- pubDocOut{doc: doc, err: err}
		}(args)
	}
	sent := 0
	for range pubs {
		var out pubDocOut
		select {
		case out = <-pubCh:
			if out.err != nil {
				t.Fatalf("bench l2 fanin pub: %v", out.err)
			}
		case <-subCtx.Done():
			t.Fatalf("bench l2 fanin pub wait ended: %v", subCtx.Err())
		}
		checkProbeDoc(t, out.doc, "pub", topic, perPub, "l2-fanin-pub")
		sent += out.doc.Sent
	}
	var sub probeDoc
	select {
	case out := <-subCh:
		if out.err != nil {
			t.Fatalf("bench l2 fanin sub probe: %v", out.err)
		}
		sub = out.doc
	case <-subCtx.Done():
		t.Fatalf("bench l2 fanin sub wait ended: %v", subCtx.Err())
	}
	checkProbeDoc(t, sub, "sub", topic, total, "l2-fanin-sub")
	var acc report.Acc
	summarizeProbeLat(&acc, sub.LatNs)
	elapsed := time.Duration(elapsedNs(sub.SendFirstNs, sub.RecvLastNs, probeTimeout(total, benchScaleRate)))
	thrMsgs, thrMiB := throughput(uniqueRecv(sent, sub.Gaps), benchScaleSize, elapsed)
	notes := baseNotes(run.liveID, domain, topic,
		fmt.Sprintf("fanin pubs=%d perpub=%d;probe_sub_note=%s", pubs, perPub, sub.Note))
	res := report.Result{
		Scenario:     "fanin",
		Leg:          "L2",
		PayloadBytes: benchScaleSize,
		RateHz:       benchScaleRate,
		WarmupN:      0,
		MeasuredN:    total,
		Sent:         sent,
		Recv:         uniqueRecv(sent, sub.Gaps),
		Dups:         sub.Dups,
		Gaps:         sub.Gaps,
		LossPct:      lossPct(sent, uniqueRecv(sent, sub.Gaps)),
		LatNs:        acc.Summary(),
		ThrMsgs:      thrMsgs,
		ThrMiBs:      thrMiB,
		DiscoveryMs:  0,
		Notes:        notes,
	}
	run.record(res)
	assertThroughput(t, res)
}
