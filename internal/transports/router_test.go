// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

package transports_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/podomy/weld/internal/transports"
)

// errRouterTakeEmpty marks a fake with no queued message.
var errRouterTakeEmpty = errors.New("router test: no message")

// routerFakeBackend is a tracking Backend for router tests.
type routerFakeBackend struct {
	mu          sync.Mutex
	published   []transports.OutgoingMessage
	takeMsgs    []transports.IncomingMessage
	takeErr     error
	joinCalls   int
	joinErr     error
	reportCalls int
	publishErr  error
	joined      bool
	closed      bool
	closeCalls  int
	// blockUntil, when non-nil, gates TakeBytes until closed,
	// simulating a quiet backend that ignores cancellation while
	// holding a popped sample.
	blockUntil chan struct{}
	// gated counts entries into the blockUntil gate.
	gated int
}

// PublishBytes records the message for later inspection.
func (f *routerFakeBackend) PublishBytes(ctx context.Context, msg transports.OutgoingMessage) error {
	err := ctx.Err()
	if err != nil {
		return fmt.Errorf("router test: context done: %w", err)
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	if f.closed {
		return transports.ErrBackendClosed
	}

	if !f.joined {
		return transports.ErrNotJoined
	}

	if f.publishErr != nil {
		return f.publishErr
	}

	cp := msg
	if msg.Payload != nil {
		cp.Payload = append([]byte(nil), msg.Payload...)
	}

	f.published = append(f.published, cp)

	return nil
}

// TakeBytes returns the queued message or the canned error.
func (f *routerFakeBackend) TakeBytes(ctx context.Context) (transports.IncomingMessage, error) {
	err := ctx.Err()
	if err != nil {
		return transports.IncomingMessage{}, fmt.Errorf("router test: context done: %w", err)
	}

	if f.blockUntil != nil {
		f.mu.Lock()
		f.gated++
		f.mu.Unlock()

		<-f.blockUntil
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	if f.closed {
		return transports.IncomingMessage{}, transports.ErrBackendClosed
	}

	if !f.joined {
		return transports.IncomingMessage{}, transports.ErrNotJoined
	}

	if f.takeErr != nil {
		return transports.IncomingMessage{}, f.takeErr
	}

	if len(f.takeMsgs) == 0 {
		return transports.IncomingMessage{}, fmt.Errorf("router take: %w", errRouterTakeEmpty)
	}

	out := f.takeMsgs[0]
	if out.Payload != nil {
		out.Payload = append([]byte(nil), out.Payload...)
	}

	return out, nil
}

// JoinDiscovery marks the fake joined and counts the call.
// A failing join leaves the fake unjoined, mirroring real backends.
func (f *routerFakeBackend) JoinDiscovery(ctx context.Context, _ transports.DiscoveryInfo) error {
	err := ctx.Err()
	if err != nil {
		return fmt.Errorf("router test: context done: %w", err)
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	if f.closed {
		return transports.ErrBackendClosed
	}

	f.joinCalls++

	if f.joinErr != nil {
		return f.joinErr
	}

	f.joined = true

	return nil
}

// ReportLiveliness counts the call without contacting a bus.
func (f *routerFakeBackend) ReportLiveliness(ctx context.Context, _ transports.LivelinessReport) error {
	err := ctx.Err()
	if err != nil {
		return fmt.Errorf("router test: context done: %w", err)
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	if f.closed {
		return transports.ErrBackendClosed
	}

	if !f.joined {
		return transports.ErrNotJoined
	}

	f.reportCalls++

	return nil
}

// Close marks the fake closed and counts the call.
func (f *routerFakeBackend) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.closed {
		return nil
	}

	f.closeCalls++
	f.closed = true

	return nil
}

// publishedCount returns the number of recorded publishes.
func (f *routerFakeBackend) publishedCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()

	return len(f.published)
}

// newJoinedRouter builds a two backend router that already joined.
func newJoinedRouter(t *testing.T, backends map[string]transports.Backend, cfg transports.RouterConfig) *transports.Router {
	t.Helper()

	router, err := transports.NewRouter(backends, cfg)
	if err != nil {
		t.Fatalf("new router: %v", err)
	}

	err = router.JoinDiscovery(context.Background(), transports.DiscoveryInfo{ParticipantName: "p"})
	if err != nil {
		t.Fatalf("join: %v", err)
	}

	return router
}

// newTwoBackendRouter builds a joined router over fresh dds plus mqtt fakes.
func newTwoBackendRouter(t *testing.T, cfg transports.RouterConfig) (*routerFakeBackend, *routerFakeBackend, *transports.Router) {
	t.Helper()

	dds := &routerFakeBackend{}
	mqtt := &routerFakeBackend{}
	backends := map[string]transports.Backend{"dds": dds, "mqtt": mqtt}

	return dds, mqtt, newJoinedRouter(t, backends, cfg)
}

// publishOne publishes one topic through the router or fails the test.
func publishOne(t *testing.T, router *transports.Router, topic, what string) {
	t.Helper()

	err := router.PublishBytes(context.Background(), transports.OutgoingMessage{TopicName: topic})
	if err != nil {
		t.Fatalf("publish %s: %v", what, err)
	}
}

// assertPublishedCounts fails unless the backends saw want counts.
func assertPublishedCounts(t *testing.T, dds, mqtt *routerFakeBackend, wantDDS, wantMQTT int, what string) {
	t.Helper()

	if got := dds.publishedCount(); got != wantDDS {
		t.Fatalf("%s dds publishes = %d, want %d", what, got, wantDDS)
	}

	if got := mqtt.publishedCount(); got != wantMQTT {
		t.Fatalf("%s mqtt publishes = %d, want %d", what, got, wantMQTT)
	}
}

// TestRouterDefaultRouting checks unmatched topics use Default.
func TestRouterDefaultRouting(t *testing.T) {
	t.Parallel()

	dds, mqtt, router := newTwoBackendRouter(t, transports.RouterConfig{
		Default: "dds",
		Routes:  []transports.Route{{Prefix: "/mqtt", BackendName: "mqtt"}},
	})

	publishOne(t, router, "/dds/topic", "default")
	assertPublishedCounts(t, dds, mqtt, 1, 0, "default")
}

// TestRouterLongestPrefixWins checks the longest matching prefix routes.
func TestRouterLongestPrefixWins(t *testing.T) {
	t.Parallel()

	dds := &routerFakeBackend{}
	mqtt := &routerFakeBackend{}

	backends := map[string]transports.Backend{"dds": dds, "mqtt": mqtt}
	cfg := transports.RouterConfig{
		Default: "dds",
		Routes: []transports.Route{
			{Prefix: "/a", BackendName: "dds"},
			{Prefix: "/a/b", BackendName: "mqtt"},
		},
	}

	router := newJoinedRouter(t, backends, cfg)

	publishOne(t, router, "/a/b/c", "deep")
	publishOne(t, router, "/a/d", "shallow")
	assertPublishedCounts(t, dds, mqtt, 1, 1, "prefix")
}

// TestRouterEmptyPrefixMatchesAll checks empty prefix catches every topic.
func TestRouterEmptyPrefixMatchesAll(t *testing.T) {
	t.Parallel()

	dds, mqtt, router := newTwoBackendRouter(t, transports.RouterConfig{
		Default: "dds",
		Routes:  []transports.Route{{Prefix: "", BackendName: "mqtt"}},
	})

	publishOne(t, router, "/anything", "empty")
	assertPublishedCounts(t, dds, mqtt, 0, 1, "empty")
}

// TestRouterUnknownBackendRejected checks bad names fail construction.
func TestRouterUnknownBackendRejected(t *testing.T) {
	t.Parallel()

	dds := &routerFakeBackend{}
	backends := map[string]transports.Backend{"dds": dds}

	_, err := transports.NewRouter(backends, transports.RouterConfig{Default: "missing"})
	if err == nil {
		t.Fatalf("unknown default succeeded, want error")
	}

	_, err = transports.NewRouter(backends, transports.RouterConfig{
		Default: "dds",
		Routes:  []transports.Route{{Prefix: "/x", BackendName: "missing"}},
	})
	if err == nil {
		t.Fatalf("unknown route backend succeeded, want error")
	}

	_, err = transports.NewRouter(backends, transports.RouterConfig{})
	if err == nil {
		t.Fatalf("empty default succeeded, want error")
	}
}

// TestRouterJoinFansOut checks JoinDiscovery reaches every backend.
func TestRouterJoinFansOut(t *testing.T) {
	t.Parallel()

	dds := &routerFakeBackend{}
	mqtt := &routerFakeBackend{}

	backends := map[string]transports.Backend{"dds": dds, "mqtt": mqtt}
	cfg := transports.RouterConfig{Default: "dds"}

	router, err := transports.NewRouter(backends, cfg)
	if err != nil {
		t.Fatalf("new router: %v", err)
	}

	err = router.JoinDiscovery(context.Background(), transports.DiscoveryInfo{ParticipantName: "p"})
	if err != nil {
		t.Fatalf("join: %v", err)
	}

	if dds.joinCalls != 1 {
		t.Fatalf("dds joins = %d, want 1", dds.joinCalls)
	}

	if mqtt.joinCalls != 1 {
		t.Fatalf("mqtt joins = %d, want 1", mqtt.joinCalls)
	}

	failing := &routerFakeBackend{joinErr: errors.New("boom")}
	ok := &routerFakeBackend{}

	bad, err := transports.NewRouter(
		map[string]transports.Backend{"bad": failing, "ok": ok},
		transports.RouterConfig{Default: "ok", Routes: []transports.Route{{Prefix: "/b", BackendName: "bad"}}},
	)
	if err != nil {
		t.Fatalf("new router: %v", err)
	}

	err = bad.JoinDiscovery(context.Background(), transports.DiscoveryInfo{ParticipantName: "p"})
	if err == nil {
		t.Fatalf("failing join succeeded, want error")
	}
}

// TestRouterTakePollsInOrder checks Take tries backends in order.
func TestRouterTakePollsInOrder(t *testing.T) {
	t.Parallel()

	first := &routerFakeBackend{takeErr: errRouterTakeEmpty}
	second := &routerFakeBackend{
		takeMsgs: []transports.IncomingMessage{{TopicName: "/b", Payload: []byte{1}}},
	}

	backends := map[string]transports.Backend{"dds": first, "mqtt": second}
	cfg := transports.RouterConfig{
		Default: "dds",
		Routes:  []transports.Route{{Prefix: "/m", BackendName: "mqtt"}},
	}

	router := newJoinedRouter(t, backends, cfg)

	msg, err := router.TakeBytes(context.Background())
	if err != nil {
		t.Fatalf("take: %v", err)
	}

	if msg.TopicName != "/b" {
		t.Fatalf("topic = %q, want %q", msg.TopicName, "/b")
	}
}

// TestRouterReportFansOut checks liveliness reaches every backend.
func TestRouterReportFansOut(t *testing.T) {
	t.Parallel()

	dds := &routerFakeBackend{}
	mqtt := &routerFakeBackend{}

	backends := map[string]transports.Backend{"dds": dds, "mqtt": mqtt}
	cfg := transports.RouterConfig{Default: "dds"}

	router := newJoinedRouter(t, backends, cfg)

	err := router.ReportLiveliness(context.Background(), transports.LivelinessReport{ParticipantName: "p", Alive: true})
	if err != nil {
		t.Fatalf("report: %v", err)
	}

	if dds.reportCalls != 1 {
		t.Fatalf("dds reports = %d, want 1", dds.reportCalls)
	}

	if mqtt.reportCalls != 1 {
		t.Fatalf("mqtt reports = %d, want 1", mqtt.reportCalls)
	}
}

// TestRouterCloseIdempotent checks double Close closes backends once.
func TestRouterCloseIdempotent(t *testing.T) {
	t.Parallel()

	dds := &routerFakeBackend{}
	mqtt := &routerFakeBackend{}

	backends := map[string]transports.Backend{"dds": dds, "mqtt": mqtt}
	cfg := transports.RouterConfig{Default: "dds"}

	router := newJoinedRouter(t, backends, cfg)

	err := router.Close()
	if err != nil {
		t.Fatalf("close: %v", err)
	}

	err = router.Close()
	if err != nil {
		t.Fatalf("second close: %v", err)
	}

	if dds.closeCalls != 1 {
		t.Fatalf("dds closes = %d, want 1", dds.closeCalls)
	}

	if mqtt.closeCalls != 1 {
		t.Fatalf("mqtt closes = %d, want 1", mqtt.closeCalls)
	}

	err = router.PublishBytes(context.Background(), transports.OutgoingMessage{TopicName: "/a"})
	if !errors.Is(err, transports.ErrBackendClosed) {
		t.Fatalf("publish after close = %v, want ErrBackendClosed", err)
	}
}

// TestRouterContextCancel checks canceled context returns context error.
func TestRouterContextCancel(t *testing.T) {
	t.Parallel()

	dds := &routerFakeBackend{}
	mqtt := &routerFakeBackend{}

	backends := map[string]transports.Backend{"dds": dds, "mqtt": mqtt}
	cfg := transports.RouterConfig{Default: "dds"}

	router := newJoinedRouter(t, backends, cfg)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := router.PublishBytes(ctx, transports.OutgoingMessage{TopicName: "/a"})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("publish canceled = %v, want context.Canceled", err)
	}

	_, err = router.TakeBytes(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("take canceled = %v, want context.Canceled", err)
	}

	err = router.JoinDiscovery(ctx, transports.DiscoveryInfo{ParticipantName: "p"})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("join canceled = %v, want context.Canceled", err)
	}

	err = router.ReportLiveliness(ctx, transports.LivelinessReport{ParticipantName: "p"})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("report canceled = %v, want context.Canceled", err)
	}
}

// waitRouterGated fails unless the fake entered its take gate.
func waitRouterGated(t *testing.T, f *routerFakeBackend, what string) {
	t.Helper()

	deadline := time.Now().Add(10 * time.Second)

	for {
		f.mu.Lock()
		gated := f.gated
		f.mu.Unlock()

		if gated > 0 {
			return
		}

		if time.Now().After(deadline) {
			t.Fatalf("%s never entered take gate", what)
		}

		time.Sleep(10 * time.Millisecond)
	}
}

// TestRouterTakeDoesNotWaitForQuietBackend proves one productive
// backend wins immediately while another blocks until cancel.
func TestRouterTakeDoesNotWaitForQuietBackend(t *testing.T) {
	t.Parallel()

	quiet := &routerFakeBackend{blockUntil: make(chan struct{})}

	t.Cleanup(func() {
		close(quiet.blockUntil)
	})

	quick := &routerFakeBackend{
		takeMsgs: []transports.IncomingMessage{{TopicName: "/q", Payload: []byte{0x07}}},
	}

	backends := map[string]transports.Backend{"quiet": quiet, "quick": quick}
	cfg := transports.RouterConfig{Default: "quiet"}

	router := newJoinedRouter(t, backends, cfg)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	msg, err := router.TakeBytes(ctx)
	if err != nil {
		t.Fatalf("take with quiet backend: %v", err)
	}

	if msg.TopicName != "/q" {
		t.Fatalf("topic = %q, want %q", msg.TopicName, "/q")
	}
}

// TestRouterAbortBuffersArrivedSamples proves a sample popped before
// cancellation is buffered instead of dropped.
func TestRouterAbortBuffersArrivedSamples(t *testing.T) {
	t.Parallel()

	slow := &routerFakeBackend{
		takeMsgs:   []transports.IncomingMessage{{TopicName: "/s", Payload: []byte{0x09}}},
		blockUntil: make(chan struct{}),
	}

	backends := map[string]transports.Backend{"slow": slow}
	cfg := transports.RouterConfig{Default: "slow"}

	router := newJoinedRouter(t, backends, cfg)

	ctx, cancel := context.WithCancel(context.Background())

	type takeResult struct {
		msg transports.IncomingMessage
		err error
	}

	first := make(chan takeResult, 1)

	go func() {
		msg, err := router.TakeBytes(ctx)
		first <- takeResult{msg: msg, err: err}
	}()

	waitRouterGated(t, slow, "slow backend")
	cancel()

	select {
	case res := <-first:
		if res.err == nil {
			t.Fatalf("aborted take succeeded, want context error")
		}
	case <-time.After(10 * time.Second):
		t.Fatalf("aborted take never returned")
	}

	close(slow.blockUntil)

	takeCtx, takeCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer takeCancel()

	msg, err := router.TakeBytes(takeCtx)
	if err != nil {
		t.Fatalf("take after abort: %v", err)
	}

	if msg.TopicName != "/s" {
		t.Fatalf("topic = %q, want buffered %q", msg.TopicName, "/s")
	}
}

// TestRouterAbortReportsContextError proves cancellation wins over a
// backend error that arrived first.
func TestRouterAbortReportsContextError(t *testing.T) {
	t.Parallel()

	failing := &routerFakeBackend{takeErr: errRouterTakeEmpty}
	blocked := &routerFakeBackend{blockUntil: make(chan struct{})}

	t.Cleanup(func() {
		close(blocked.blockUntil)
	})

	backends := map[string]transports.Backend{"failing": failing, "blocked": blocked}
	cfg := transports.RouterConfig{Default: "failing"}

	router := newJoinedRouter(t, backends, cfg)

	ctx, cancel := context.WithCancel(context.Background())

	type takeResult struct {
		msg transports.IncomingMessage
		err error
	}

	first := make(chan takeResult, 1)

	go func() {
		msg, err := router.TakeBytes(ctx)
		first <- takeResult{msg: msg, err: err}
	}()

	waitRouterGated(t, blocked, "blocked backend")
	cancel()

	select {
	case res := <-first:
		if res.err == nil {
			t.Fatalf("aborted take succeeded, want context error")
		}

		if !errors.Is(res.err, context.Canceled) {
			t.Fatalf("aborted take err = %v, want context canceled", res.err)
		}
	case <-time.After(10 * time.Second):
		t.Fatalf("aborted take never returned")
	}
}

// TestRouterJoinAfterCloseFails proves a closed router stays closed.
func TestRouterJoinAfterCloseFails(t *testing.T) {
	t.Parallel()

	backends := map[string]transports.Backend{"a": &routerFakeBackend{}}
	cfg := transports.RouterConfig{Default: "a"}

	router := newJoinedRouter(t, backends, cfg)

	if err := router.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	err := router.JoinDiscovery(context.Background(), transports.DiscoveryInfo{ParticipantName: "p"})
	if !errors.Is(err, transports.ErrBackendClosed) {
		t.Fatalf("join after close err = %v, want %v", err, transports.ErrBackendClosed)
	}

	if _, err := router.TakeBytes(context.Background()); !errors.Is(err, transports.ErrBackendClosed) {
		t.Fatalf("take after close err = %v, want %v", err, transports.ErrBackendClosed)
	}
}

// TestRouterPartialJoinStaysUnjoined proves a failed fan-out join does
// not mark the router joined: later takes fail loudly instead of
// running on a subset of backends.
func TestRouterPartialJoinStaysUnjoined(t *testing.T) {
	t.Parallel()

	ok := &routerFakeBackend{}
	failing := &routerFakeBackend{joinErr: errors.New("boom")}

	backends := map[string]transports.Backend{"ok": ok, "failing": failing}
	cfg := transports.RouterConfig{Default: "ok"}

	router, err := transports.NewRouter(backends, cfg)
	if err != nil {
		t.Fatalf("new router: %v", err)
	}

	err = router.JoinDiscovery(context.Background(), transports.DiscoveryInfo{ParticipantName: "p"})
	if err == nil {
		t.Fatalf("partial join succeeded, want error")
	}

	if _, err := router.TakeBytes(context.Background()); !errors.Is(err, transports.ErrNotJoined) {
		t.Fatalf("take after failed join err = %v, want %v", err, transports.ErrNotJoined)
	}
}

// TestRouterUnjoinedTakeFails proves takes need a successful join first.
func TestRouterUnjoinedTakeFails(t *testing.T) {
	t.Parallel()

	backends := map[string]transports.Backend{"a": &routerFakeBackend{}}
	cfg := transports.RouterConfig{Default: "a"}

	router, err := transports.NewRouter(backends, cfg)
	if err != nil {
		t.Fatalf("new router: %v", err)
	}

	if _, err := router.TakeBytes(context.Background()); !errors.Is(err, transports.ErrNotJoined) {
		t.Fatalf("take before join err = %v, want %v", err, transports.ErrNotJoined)
	}
}

// TestRouterZeroValueFails proves the zero value is unusable: Join,
// takes, publish, report, and Close all fail loudly instead of
// silently succeeding on nothing.
func TestRouterZeroValueFails(t *testing.T) {
	t.Parallel()

	var router transports.Router

	if err := router.JoinDiscovery(context.Background(), transports.DiscoveryInfo{}); err == nil {
		t.Fatalf("zero router join succeeded, want error")
	}

	if _, err := router.TakeBytes(context.Background()); err == nil {
		t.Fatalf("zero router take succeeded, want error")
	}

	msg := transports.OutgoingMessage{TopicName: "/a", TypeName: "t", Payload: []byte{0x01}}
	if err := router.PublishBytes(context.Background(), msg); err == nil {
		t.Fatalf("zero router publish succeeded, want error")
	}

	if err := router.ReportLiveliness(context.Background(), transports.LivelinessReport{}); err == nil {
		t.Fatalf("zero router report succeeded, want error")
	}

	if err := router.Close(); err == nil {
		t.Fatalf("zero router close succeeded, want error")
	}
}
