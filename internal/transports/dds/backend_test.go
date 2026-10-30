// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package dds

import (
	"bytes"
	"context"
	"errors"
	"net"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/podomy/weld/internal/transports"
	"github.com/podomy/weld/internal/transports/dds/rtps"
)

// clearBackendEnv unsets the domain variable for deterministic tests.
func clearBackendEnv(t *testing.T) {
	t.Helper()

	old, had := os.LookupEnv(transports.DomainEnvVar)
	if had {
		t.Setenv(transports.DomainEnvVar, old)
	}

	err := os.Unsetenv(transports.DomainEnvVar)
	if err != nil {
		t.Fatalf("unset env: %v", err)
	}
}

// newTestBackend returns a backend with short intervals for loop tests.
func newTestBackend(t *testing.T, cfg SidecarConfig) *Backend {
	t.Helper()

	clearBackendEnv(t)

	backend, err := NewBackend(cfg)
	if err != nil {
		t.Fatalf("new backend: %v", err)
	}

	backend.spdpInterval = 5 * time.Millisecond
	backend.sedpInterval = 5 * time.Millisecond

	return backend
}

// fakeClock is a controllable Clock for lease tests.
type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

// Now returns the stubbed time.
func (f *fakeClock) Now() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.now
}

// After never fires so loops stay idle until Close.
func (f *fakeClock) After(_ time.Duration) <-chan time.Time {
	return make(chan time.Time)
}

// setNow sets the stubbed time.
func (f *fakeClock) setNow(t time.Time) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.now = t
}

// countingPeer counts writes and closes for dial tests.
type countingPeer struct {
	mu     sync.Mutex
	writes int
	closes int
}

// Write counts one write.
func (p *countingPeer) Write(b []byte) (int, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.writes++

	return len(b), nil
}

// Close counts one close.
func (p *countingPeer) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.closes++

	return nil
}

// TestBackendImplementsContract checks Backend satisfies transports.Backend.
func TestBackendImplementsContract(t *testing.T) {
	t.Parallel()

	var _ transports.Backend = (*Backend)(nil)
}

// TestNewBackendRejectsBadConfig checks validation follows NewSidecar rules.
func TestNewBackendRejectsBadConfig(t *testing.T) {
	clearBackendEnv(t)

	cases := []SidecarConfig{
		{DomainID: 999, PeerAddress: "127.0.0.1"},
		{DomainID: 0, PeerAddress: "   "},
		{DomainID: 0, PeerAddress: "127.0.0.1", Range: transports.DiscoveryRange(99)},
	}

	for _, cfg := range cases {
		_, err := NewBackend(cfg)
		if err == nil {
			t.Fatalf("NewBackend(%+v) succeeded, want error", cfg)
		}
	}

	_, err := NewBackendWithClock(SidecarConfig{PeerAddress: "127.0.0.1"}, nil)
	if err == nil {
		t.Fatalf("NewBackendWithClock with nil clock succeeded, want error")
	}
}

// TestNewBackendNormalizes checks peer trim and range normalization.
func TestNewBackendNormalizes(t *testing.T) {
	clearBackendEnv(t)

	backend, err := NewBackend(SidecarConfig{
		DomainID:    0,
		PeerAddress: "  127.0.0.1  ",
		Range:       transports.DiscoveryRangeNotSet,
	})
	if err != nil {
		t.Fatalf("new backend: %v", err)
	}

	cfg := backend.Config()
	if cfg.PeerAddress != "127.0.0.1" {
		t.Fatalf("peer = %q, want 127.0.0.1", cfg.PeerAddress)
	}

	if cfg.Range != transports.DiscoveryRangeSystemDefault {
		t.Fatalf("range = %v, want system_default", cfg.Range)
	}

	if got := backend.DomainID(); got != 0 {
		t.Fatalf("domain = %d, want 0", got)
	}

	if backend.Joined() {
		t.Fatalf("joined is true before JoinDiscovery")
	}

	if backend.Closed() {
		t.Fatalf("closed is true before Close")
	}

	err = backend.Close()
	if err != nil {
		t.Fatalf("close: %v", err)
	}
}

// TestNewBackendResolvesDomain checks DomainConfig resolution through env.
func TestNewBackendResolvesDomain(t *testing.T) {
	t.Setenv(transports.DomainEnvVar, "42")

	backend, err := NewBackend(SidecarConfig{DomainID: 7, PeerAddress: "127.0.0.1"})
	if err != nil {
		t.Fatalf("new backend: %v", err)
	}

	if got := backend.DomainID(); got != 42 {
		t.Fatalf("domain = %d, want 42", got)
	}

	if got := backend.Config().DomainID; got != 42 {
		t.Fatalf("config domain = %d, want 42", got)
	}

	err = backend.Close()
	if err != nil {
		t.Fatalf("close: %v", err)
	}
}

// TestBackendRequiresJoin checks data path before join reports ErrNotJoined.
func TestBackendRequiresJoin(t *testing.T) {
	clearBackendEnv(t)

	backend, err := NewBackend(DefaultSidecarConfig())
	if err != nil {
		t.Fatalf("new backend: %v", err)
	}

	defer func() {
		err := backend.Close()
		if err != nil {
			t.Errorf("close: %v", err)
		}
	}()

	err = backend.PublishBytes(context.Background(), transports.OutgoingMessage{TopicName: "/chatter"})
	if !errors.Is(
		err,
		transports.ErrNotJoined,
	) {
		t.Fatalf("publish before join = %v, want ErrNotJoined", err)
	}

	_, err = backend.TakeBytes(context.Background())
	if !errors.Is(err, transports.ErrNotJoined) {
		t.Fatalf("take before join = %v, want ErrNotJoined", err)
	}

	err = backend.ReportLiveliness(
		context.Background(),
		transports.LivelinessReport{ParticipantName: "p", Alive: true},
	)
	if !errors.Is(err, transports.ErrNotJoined) {
		t.Fatalf("report before join = %v, want ErrNotJoined", err)
	}
}

// TestBackendJoinCanceledContext checks canceled join honors ctx.
func TestBackendJoinCanceledContext(t *testing.T) {
	clearBackendEnv(t)

	backend, err := NewBackend(DefaultSidecarConfig())
	if err != nil {
		t.Fatalf("new backend: %v", err)
	}

	defer func() {
		err := backend.Close()
		if err != nil {
			t.Errorf("close: %v", err)
		}
	}()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err = backend.JoinDiscovery(ctx, transports.DiscoveryInfo{ParticipantName: "p"})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("join canceled = %v, want context.Canceled", err)
	}

	if backend.Joined() {
		t.Fatalf("joined is true after canceled join")
	}
}

// TestBackendDataPathNotWired checks the unwired path reports explicitly.
func TestBackendDataPathNotWired(t *testing.T) {
	backend := newTestBackend(t, DefaultSidecarConfig())

	defer func() {
		err := backend.Close()
		if err != nil {
			t.Errorf("close: %v", err)
		}
	}()

	backend.dial = func(_ context.Context, _ string) (UDPPeer, error) {
		return &countingPeer{}, nil
	}

	err := backend.JoinDiscovery(context.Background(), transports.DiscoveryInfo{ParticipantName: "p"})
	if err != nil {
		t.Fatalf("join: %v", err)
	}

	err = backend.PublishBytes(context.Background(), transports.OutgoingMessage{TopicName: "/chatter"})
	if !errors.Is(err, ErrDataPathNotWired) {
		t.Fatalf("publish = %v, want ErrDataPathNotWired", err)
	}

	if errors.Is(err, transports.ErrNotJoined) {
		t.Fatalf("publish = %v, must not be ErrNotJoined after join", err)
	}

	_, err = backend.TakeBytes(context.Background())
	if !errors.Is(err, ErrDataPathNotWired) {
		t.Fatalf("take = %v, want ErrDataPathNotWired", err)
	}
}

// TestBackendCloseIdempotent checks Close returns nil twice.
func TestBackendCloseIdempotent(t *testing.T) {
	clearBackendEnv(t)

	backend, err := NewBackend(DefaultSidecarConfig())
	if err != nil {
		t.Fatalf("new backend: %v", err)
	}

	if backend.Closed() {
		t.Fatalf("closed is true before Close")
	}

	err = backend.Close()
	if err != nil {
		t.Fatalf("close: %v", err)
	}

	if !backend.Closed() {
		t.Fatalf("closed is false after Close")
	}

	err = backend.Close()
	if err != nil {
		t.Fatalf("second close: %v", err)
	}
}

// TestBackendPostCloseErrors checks all methods fail after Close.
func TestBackendPostCloseErrors(t *testing.T) {
	backend := newTestBackend(t, DefaultSidecarConfig())

	err := backend.JoinDiscovery(context.Background(), transports.DiscoveryInfo{ParticipantName: "p"})
	if err != nil {
		t.Fatalf("join: %v", err)
	}

	err = backend.Close()
	if err != nil {
		t.Fatalf("close: %v", err)
	}

	err = backend.Close()
	if err != nil {
		t.Fatalf("second close: %v", err)
	}

	err = backend.JoinDiscovery(context.Background(), transports.DiscoveryInfo{ParticipantName: "p"})
	if !errors.Is(err, transports.ErrBackendClosed) {
		t.Fatalf("join after close = %v, want ErrBackendClosed", err)
	}

	err = backend.PublishBytes(context.Background(), transports.OutgoingMessage{TopicName: "/chatter"})
	if !errors.Is(err, transports.ErrBackendClosed) {
		t.Fatalf("publish after close = %v, want ErrBackendClosed", err)
	}

	_, err = backend.TakeBytes(context.Background())
	if !errors.Is(err, transports.ErrBackendClosed) {
		t.Fatalf("take after close = %v, want ErrBackendClosed", err)
	}

	err = backend.ReportLiveliness(
		context.Background(),
		transports.LivelinessReport{ParticipantName: "p", Alive: true},
	)
	if !errors.Is(err, transports.ErrBackendClosed) {
		t.Fatalf("report after close = %v, want ErrBackendClosed", err)
	}
}

// TestBackendReportGenerationMatrix checks stale and idempotent cases.
func TestBackendReportGenerationMatrix(t *testing.T) {
	backend := newTestBackend(t, DefaultSidecarConfig())

	defer func() {
		err := backend.Close()
		if err != nil {
			t.Errorf("close: %v", err)
		}
	}()

	err := backend.JoinDiscovery(context.Background(), transports.DiscoveryInfo{ParticipantName: "p"})
	if err != nil {
		t.Fatalf("join: %v", err)
	}

	err = backend.ReportLiveliness(
		context.Background(),
		transports.LivelinessReport{ParticipantName: "talker", Generation: 7, Alive: true},
	)
	if err != nil {
		t.Fatalf("gen 7 live: %v", err)
	}

	err = backend.ReportLiveliness(
		context.Background(),
		transports.LivelinessReport{ParticipantName: "talker", Generation: 7, Alive: true},
	)
	if err != nil {
		t.Fatalf("repeat same gen same alive is not idempotent: %v", err)
	}

	err = backend.ReportLiveliness(
		context.Background(),
		transports.LivelinessReport{ParticipantName: "talker", Generation: 7, Alive: false},
	)
	if !errors.Is(err, transports.ErrStaleGeneration) {
		t.Fatalf("same gen different alive = %v, want ErrStaleGeneration", err)
	}

	err = backend.ReportLiveliness(
		context.Background(),
		transports.LivelinessReport{ParticipantName: "talker", Generation: 6, Alive: true},
	)
	if !errors.Is(err, transports.ErrStaleGeneration) {
		t.Fatalf("backward gen = %v, want ErrStaleGeneration", err)
	}

	err = backend.ReportLiveliness(
		context.Background(),
		transports.LivelinessReport{ParticipantName: "talker", Generation: 8, Alive: false},
	)
	if err != nil {
		t.Fatalf("gen 8 dispose: %v", err)
	}

	err = backend.ReportLiveliness(
		context.Background(),
		transports.LivelinessReport{ParticipantName: "talker", Generation: 8, Alive: false},
	)
	if err != nil {
		t.Fatalf("repeat dispose is not idempotent: %v", err)
	}

	err = backend.ReportLiveliness(
		context.Background(),
		transports.LivelinessReport{ParticipantName: "other", Generation: 1, Alive: true},
	)
	if err != nil {
		t.Fatalf("other participant gen 1: %v", err)
	}
}

// TestBackendLeaseClock checks ReportLiveliness stamps the injected clock.
func TestBackendLeaseClock(t *testing.T) {
	clearBackendEnv(t)

	stub := &fakeClock{}
	stub.setNow(time.Date(2026, time.January, 2, 3, 4, 5, 0, time.UTC))

	backend, err := NewBackendWithClock(DefaultSidecarConfig(), stub)
	if err != nil {
		t.Fatalf("new backend: %v", err)
	}

	defer func() {
		err := backend.Close()
		if err != nil {
			t.Errorf("close: %v", err)
		}
	}()

	err = backend.JoinDiscovery(context.Background(), transports.DiscoveryInfo{ParticipantName: "p"})
	if err != nil {
		t.Fatalf("join: %v", err)
	}

	err = backend.ReportLiveliness(
		context.Background(),
		transports.LivelinessReport{ParticipantName: "talker", Generation: 1, Alive: true},
	)
	if err != nil {
		t.Fatalf("report: %v", err)
	}

	backend.mu.Lock()
	lease, ok := backend.leases["talker"]
	backend.mu.Unlock()

	if !ok {
		t.Fatalf("lease missing for talker")
	}

	if !lease.Equal(stub.Now()) {
		t.Fatalf("lease = %v, want %v", lease, stub.Now())
	}
}

// TestBackendDialError checks a failing dial surfaces without joining.
func TestBackendDialError(t *testing.T) {
	clearBackendEnv(t)

	backend, err := NewBackend(DefaultSidecarConfig())
	if err != nil {
		t.Fatalf("new backend: %v", err)
	}

	defer func() {
		err := backend.Close()
		if err != nil {
			t.Errorf("close: %v", err)
		}
	}()

	backend.dial = func(_ context.Context, _ string) (UDPPeer, error) {
		return nil, errors.New("dds: dial boom")
	}

	err = backend.JoinDiscovery(context.Background(), transports.DiscoveryInfo{ParticipantName: "p"})
	if err == nil {
		t.Fatalf("join with failing dial succeeded, want error")
	}

	if backend.Joined() {
		t.Fatalf("joined is true after failed dial")
	}
}

// TestBackendJoinDialsPeer checks JoinDiscovery dials through UDPPeer.
func TestBackendJoinDialsPeer(t *testing.T) {
	clearBackendEnv(t)

	backend, err := NewBackend(DefaultSidecarConfig())
	if err != nil {
		t.Fatalf("new backend: %v", err)
	}

	defer func() {
		err := backend.Close()
		if err != nil {
			t.Errorf("close: %v", err)
		}
	}()

	peer := &countingPeer{}

	backend.dial = func(_ context.Context, addr string) (UDPPeer, error) {
		if addr == "" {
			return nil, errors.New("dds: empty dial address")
		}

		return peer, nil
	}

	err = backend.JoinDiscovery(context.Background(), transports.DiscoveryInfo{ParticipantName: "p"})
	if err != nil {
		t.Fatalf("join: %v", err)
	}

	backend.mu.Lock()
	stored := backend.peer
	backend.mu.Unlock()

	if stored != peer {
		t.Fatalf("stored peer does not match dialed peer")
	}
}

// waitForAnnounce polls until both loops have ticked or the timeout ends.
func waitForAnnounce(t *testing.T, backend *Backend) {
	t.Helper()

	deadline := time.Now().Add(2 * time.Second)

	for time.Now().Before(deadline) {
		backend.mu.Lock()
		spdp := backend.spdpCount
		sedp := backend.sedpCount
		gen := backend.discoveryGen
		backend.mu.Unlock()

		if spdp > 0 && sedp > 0 && gen > 0 {
			return
		}

		time.Sleep(5 * time.Millisecond)
	}

	t.Fatalf("announcement loops did not tick")
}

// TestBackendAnnouncementLoopsTick checks SPDP and SEDP loops advance.
func TestBackendAnnouncementLoopsTick(t *testing.T) {
	backend := newTestBackend(t, DefaultSidecarConfig())

	defer func() {
		err := backend.Close()
		if err != nil {
			t.Errorf("close: %v", err)
		}
	}()

	err := backend.JoinDiscovery(context.Background(), transports.DiscoveryInfo{ParticipantName: "p"})
	if err != nil {
		t.Fatalf("join: %v", err)
	}

	waitForAnnounce(t, backend)

	if got := backend.DiscoveryGeneration(); got == 0 {
		t.Fatalf("discovery generation = 0, want positive")
	}
}

// TestBackendCloseStopsLoops checks cancel stops announcement ticks.
func TestBackendCloseStopsLoops(t *testing.T) {
	backend := newTestBackend(t, DefaultSidecarConfig())

	err := backend.JoinDiscovery(context.Background(), transports.DiscoveryInfo{ParticipantName: "p"})
	if err != nil {
		t.Fatalf("join: %v", err)
	}

	waitForAnnounce(t, backend)

	err = backend.Close()
	if err != nil {
		t.Fatalf("close: %v", err)
	}

	backend.mu.Lock()
	spdp := backend.spdpCount
	sedp := backend.sedpCount
	gen := backend.discoveryGen
	backend.mu.Unlock()

	// Close waits for loops through runWG, so counters stay stable
	// without a wall sleep. A second locked read proves no tick.
	backend.mu.Lock()
	defer backend.mu.Unlock()

	if backend.spdpCount != spdp || backend.sedpCount != sedp || backend.discoveryGen != gen {
		t.Fatalf(
			"loops ticked after close: spdp %d to %d, sedp %d to %d, gen %d to %d",
			spdp,
			backend.spdpCount,
			sedp,
			backend.sedpCount,
			gen,
			backend.discoveryGen,
		)
	}
}

// TestBackendContextCancelStopsJoin checks canceled ctx never starts loops.
func TestBackendContextCancelStopsJoin(t *testing.T) {
	backend := newTestBackend(t, DefaultSidecarConfig())

	defer func() {
		err := backend.Close()
		if err != nil {
			t.Errorf("close: %v", err)
		}
	}()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := backend.JoinDiscovery(ctx, transports.DiscoveryInfo{ParticipantName: "p"})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("join canceled = %v, want context.Canceled", err)
	}

	backend.mu.Lock()
	defer backend.mu.Unlock()

	if backend.spdpCount != 0 || backend.sedpCount != 0 {
		t.Fatalf("loops ticked after canceled join")
	}
}

// TestBackendConcurrentJoinCloseRace checks concurrent join and close stay safe.
func TestBackendConcurrentJoinCloseRace(t *testing.T) {
	clearBackendEnv(t)

	backend, err := NewBackend(DefaultSidecarConfig())
	if err != nil {
		t.Fatalf("new backend: %v", err)
	}

	backend.spdpInterval = 5 * time.Millisecond
	backend.sedpInterval = 5 * time.Millisecond

	var wg sync.WaitGroup

	for range 8 {
		wg.Go(func() {
			err := backend.JoinDiscovery(context.Background(), transports.DiscoveryInfo{ParticipantName: "p"})
			if err != nil && !errors.Is(err, transports.ErrBackendClosed) {
				t.Errorf("join: %v", err)
			}
		})
	}

	wg.Go(func() {
		err := backend.Close()
		if err != nil {
			t.Errorf("close: %v", err)
		}
	})

	wg.Wait()

	err = backend.Close()
	if err != nil {
		t.Fatalf("final close: %v", err)
	}

	if !backend.Closed() {
		t.Fatalf("closed is false after concurrent join and close")
	}

	err = backend.PublishBytes(context.Background(), transports.OutgoingMessage{TopicName: "/chatter"})
	if !errors.Is(err, transports.ErrBackendClosed) {
		t.Fatalf("publish after concurrent close = %v, want ErrBackendClosed", err)
	}
}

// checkAllowedErr fails unless err is nil or matches an allowed sentinel.
func checkAllowedErr(t *testing.T, err error, op string, allowed ...error) {
	t.Helper()

	if err == nil {
		return
	}

	for _, want := range allowed {
		if errors.Is(err, want) {
			return
		}
	}

	t.Errorf("%s: %v", op, err)
}

// TestBackendConcurrentUse checks concurrent data path and close stay safe.
func TestBackendConcurrentUse(t *testing.T) {
	backend := newTestBackend(t, DefaultSidecarConfig())

	err := backend.JoinDiscovery(context.Background(), transports.DiscoveryInfo{ParticipantName: "p"})
	if err != nil {
		t.Fatalf("join: %v", err)
	}

	var wg sync.WaitGroup

	for range 8 {
		wg.Go(func() {
			err := backend.PublishBytes(context.Background(), transports.OutgoingMessage{TopicName: "/chatter"})
			checkAllowedErr(t, err, "publish", transports.ErrBackendClosed, ErrDataPathNotWired, ErrNoRoute)

			_, err = backend.TakeBytes(context.Background())
			checkAllowedErr(t, err, "take", transports.ErrBackendClosed, ErrDataPathNotWired)

			err = backend.ReportLiveliness(
				context.Background(),
				transports.LivelinessReport{ParticipantName: "p", Generation: 1, Alive: true},
			)
			checkAllowedErr(t, err, "report", transports.ErrBackendClosed, transports.ErrStaleGeneration)
		})
	}

	wg.Go(func() {
		err := backend.Close()
		if err != nil {
			t.Errorf("close: %v", err)
		}
	})

	wg.Wait()

	err = backend.Close()
	if err != nil {
		t.Fatalf("final close: %v", err)
	}
}

// waitForRoutes polls until both backends see each other or the timeout ends.
func waitForRoutes(t *testing.T, first, second *Backend) {
	t.Helper()

	deadline := time.Now().Add(5 * time.Second)

	for time.Now().Before(deadline) {
		first.mu.Lock()
		firstRoutes := len(first.routes)
		first.mu.Unlock()

		second.mu.Lock()
		secondRoutes := len(second.routes)
		second.mu.Unlock()

		if firstRoutes > 0 && secondRoutes > 0 {
			return
		}

		time.Sleep(20 * time.Millisecond)
	}

	t.Fatalf("live peers did not discover each other")
}

// newLiveBackend returns a joined backend on an isolated domain.
func newLiveBackend(t *testing.T, domain uint32, name string) *Backend {
	t.Helper()

	clearBackendEnv(t)

	backend, err := NewBackend(SidecarConfig{
		DomainID:    domain,
		PeerAddress: DefaultPeerAddress,
		Range:       transports.DiscoveryRangeSystemDefault,
	})
	if err != nil {
		t.Fatalf("new backend: %v", err)
	}

	backend.spdpInterval = 10 * time.Millisecond
	backend.sedpInterval = 10 * time.Millisecond

	err = backend.JoinDiscovery(context.Background(), transports.DiscoveryInfo{ParticipantName: name})
	if err != nil {
		cerr := backend.Close()
		if cerr != nil {
			t.Fatalf("close after failed join %v: %v", err, cerr)
		}

		t.Fatalf("join: %v", err)
	}

	return backend
}

// TestBackendLivePairStringRoundTrip checks two backends discover and relay one String.
func TestBackendLivePairStringRoundTrip(t *testing.T) {
	if testing.Short() {
		t.Skip("short mode skips live UDP discovery")
	}

	const domain = 51

	first := newLiveBackend(t, domain, "pair-a")
	defer func() {
		err := first.Close()
		if err != nil {
			t.Errorf("close first: %v", err)
		}
	}()

	second := newLiveBackend(t, domain, "pair-b")
	defer func() {
		err := second.Close()
		if err != nil {
			t.Errorf("close second: %v", err)
		}
	}()

	waitForRoutes(t, first, second)

	payload := []byte("hello")

	err := first.PublishBytes(context.Background(), transports.OutgoingMessage{
		TopicName: "/chatter",
		TypeName:  "std_msgs__msg__String",
		Payload:   payload,
	})
	if err != nil {
		t.Fatalf("publish: %v", err)
	}

	takeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	msg, err := second.TakeBytes(takeCtx)
	if err != nil {
		t.Fatalf("take: %v", err)
	}

	if msg.TopicName != "/chatter" {
		t.Fatalf("topic = %q, want /chatter", msg.TopicName)
	}

	if msg.TypeName != "std_msgs__msg__String" {
		t.Fatalf("type = %q, want std_msgs__msg__String", msg.TypeName)
	}

	if string(msg.Payload) != "hello" {
		t.Fatalf("payload = %q, want hello", msg.Payload)
	}
}

// TestBackendPublishRouteUnknown checks publish without peers names topic and domain.
func TestBackendPublishRouteUnknown(t *testing.T) {
	if testing.Short() {
		t.Skip("short mode skips live UDP discovery")
	}

	const domain = 52

	backend := newLiveBackend(t, domain, "lonely")
	defer func() {
		err := backend.Close()
		if err != nil {
			t.Errorf("close: %v", err)
		}
	}()

	err := backend.PublishBytes(context.Background(), transports.OutgoingMessage{
		TopicName: "/chatter",
		TypeName:  "std_msgs__msg__String",
		Payload:   []byte("hello"),
	})
	if !errors.Is(err, ErrNoRoute) {
		t.Fatalf("publish = %v, want ErrNoRoute", err)
	}

	if !strings.Contains(err.Error(), "/chatter") || !strings.Contains(err.Error(), "52") {
		t.Fatalf("publish error %q must name topic and domain", err.Error())
	}
}

// TestBackendTakeCanceled checks canceled take honors ctx.
func TestBackendTakeCanceled(t *testing.T) {
	if testing.Short() {
		t.Skip("short mode skips live UDP discovery")
	}

	const domain = 53

	backend := newLiveBackend(t, domain, "cancel-take")
	defer func() {
		err := backend.Close()
		if err != nil {
			t.Errorf("close: %v", err)
		}
	}()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := backend.TakeBytes(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("take canceled = %v, want context.Canceled", err)
	}
}

// TestBackendPublishCanceled checks canceled publish honors ctx.
func TestBackendPublishCanceled(t *testing.T) {
	if testing.Short() {
		t.Skip("short mode skips live UDP discovery")
	}

	const domain = 54

	backend := newLiveBackend(t, domain, "cancel-publish")
	defer func() {
		err := backend.Close()
		if err != nil {
			t.Errorf("close: %v", err)
		}
	}()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := backend.PublishBytes(ctx, transports.OutgoingMessage{
		TopicName: "/chatter",
		TypeName:  "std_msgs__msg__String",
		Payload:   []byte("hello"),
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("publish canceled = %v, want context.Canceled", err)
	}
}

// TestBackendUnicastOnlyDiscovery checks two backends discover with a
// blocked multicast port via directed unicast SPDP.
func TestBackendUnicastOnlyDiscovery(t *testing.T) {
	if testing.Short() {
		t.Skip("short mode skips live UDP discovery")
	}

	const domain uint32 = 59

	metaPort, err := rtps.MetatrafficMulticastPort(domain)
	if err != nil {
		t.Fatalf("meta port: %v", err)
	}

	blocker, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: metaPort})
	if err != nil {
		t.Skipf("pre-bind multicast port %d: %v", metaPort, err)
	}

	defer func() {
		err := blocker.Close()
		if err != nil {
			t.Errorf("close blocker: %v", err)
		}
	}()

	first := newLiveBackend(t, domain, "unicast-a")
	defer func() {
		err := first.Close()
		if err != nil {
			t.Errorf("close first: %v", err)
		}
	}()

	second := newLiveBackend(t, domain, "unicast-b")
	defer func() {
		err := second.Close()
		if err != nil {
			t.Errorf("close second: %v", err)
		}
	}()

	waitForRoutes(t, first, second)

	err = first.PublishBytes(context.Background(), transports.OutgoingMessage{
		TopicName: "/chatter",
		TypeName:  "std_msgs__msg__String",
		Payload:   []byte("hello"),
	})
	if err != nil {
		t.Fatalf("publish: %v", err)
	}

	takeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	msg, err := second.TakeBytes(takeCtx)
	if err != nil {
		t.Fatalf("take: %v", err)
	}

	if msg.TopicName != "/chatter" {
		t.Fatalf("topic = %q, want /chatter", msg.TopicName)
	}

	if string(msg.Payload) != "hello" {
		t.Fatalf("payload = %q, want hello", string(msg.Payload))
	}
}

// buildDemuxMessage builds one RTPS message with the given writer and payload for demux tests.
func buildDemuxMessage(t *testing.T, writer rtps.EntityID, payload []byte) []byte {
	t.Helper()

	var prefix [12]byte
	for i := range prefix {
		prefix[i] = byte(i + 1)
	}

	header := rtps.DefaultHeader(prefix)

	data := rtps.Data{
		ReaderID:       rtps.EntityUnknown,
		WriterID:       writer,
		SequenceNumber: 1,
		Payload:        payload,
	}

	raw, err := data.Submessage(true)
	if err != nil {
		t.Fatalf("submessage: %v", err)
	}

	msg, err := rtps.EncodeMessage(header, []rtps.RawSubmessage{raw})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	return msg
}

// TestBackendDemuxBuiltinRouting checks SPDP versus SEDP versus user routing by writer entity.
func TestBackendDemuxBuiltinRouting(t *testing.T) {
	t.Parallel()

	backend := &Backend{
		routes: make(map[GUIDPrefix][]int),
		spdpCh: make(chan []byte, demuxQueueDepth),
		sedpCh: make(chan []byte, demuxQueueDepth),
		dataCh: make(chan dataItem, demuxQueueDepth),
	}

	ctx := t.Context()

	spdpMsg := buildDemuxMessage(t, rtps.EntitySPDPWriter, []byte{1, 2, 3})
	backend.dispatchRTPS(ctx, spdpMsg, backend.spdpCh, backend.sedpCh, backend.dataCh)

	if len(backend.spdpCh) != 1 {
		t.Fatalf("spdp queue = %d, want 1", len(backend.spdpCh))
	}

	if len(backend.sedpCh) != 0 {
		t.Fatalf("sedp queue = %d, want 0", len(backend.sedpCh))
	}

	if len(backend.dataCh) != 0 {
		t.Fatalf("data queue = %d, want 0", len(backend.dataCh))
	}

	<-backend.spdpCh

	sedpWriters := []rtps.EntityID{
		{0x00, 0x00, sedpWriterEntityB14, 0xC2},
		{0x00, 0x00, sedpWriterEntityB14Cyclone, 0xC2},
		{0x00, 0x00, sedpWriterEntityB14Alt, 0xC2},
	}

	for _, writer := range sedpWriters {
		msg := buildDemuxMessage(t, writer, []byte{4, 5, 6})
		backend.dispatchRTPS(ctx, msg, backend.spdpCh, backend.sedpCh, backend.dataCh)

		if len(backend.sedpCh) != 1 {
			t.Fatalf("sedp queue for %v = %d, want 1", writer, len(backend.sedpCh))
		}

		<-backend.sedpCh

		if len(backend.spdpCh) != 0 {
			t.Fatalf("spdp queue after sedp = %d, want 0", len(backend.spdpCh))
		}

		if len(backend.dataCh) != 0 {
			t.Fatalf("data queue after sedp = %d, want 0", len(backend.dataCh))
		}
	}

	userWriter := rtps.EntityID{writerEntityUserB12, writerEntityUserB13, writerEntityUserB14, writerEntityUserB15}
	userMsg := buildDemuxMessage(t, userWriter, []byte{7, 8, 9})
	backend.dispatchRTPS(ctx, userMsg, backend.spdpCh, backend.sedpCh, backend.dataCh)

	if len(backend.dataCh) != 1 {
		t.Fatalf("data queue = %d, want 1", len(backend.dataCh))
	}

	if len(backend.spdpCh) != 0 || len(backend.sedpCh) != 0 {
		t.Fatalf("builtin queues after user = %d %d, want 0 0", len(backend.spdpCh), len(backend.sedpCh))
	}
}

// TestBackendDemuxMultiSub checks one message with two user subs queues twice.
func TestBackendDemuxMultiSub(t *testing.T) {
	t.Parallel()

	backend := &Backend{
		routes: make(map[GUIDPrefix][]int),
		spdpCh: make(chan []byte, demuxQueueDepth),
		sedpCh: make(chan []byte, demuxQueueDepth),
		dataCh: make(chan dataItem, demuxQueueDepth),
	}

	var prefix [12]byte
	for i := range prefix {
		prefix[i] = byte(i + 9)
	}

	header := rtps.DefaultHeader(prefix)
	userWriter := rtps.EntityID{writerEntityUserB12, writerEntityUserB13, writerEntityUserB14, writerEntityUserB15}

	first := rtps.Data{
		ReaderID:       rtps.EntityUnknown,
		WriterID:       userWriter,
		SequenceNumber: 1,
		Payload:        []byte{0x0A},
	}

	second := rtps.Data{
		ReaderID:       rtps.EntityUnknown,
		WriterID:       userWriter,
		SequenceNumber: 2,
		Payload:        []byte{0x0B},
	}

	rawFirst, err := first.Submessage(true)
	if err != nil {
		t.Fatalf("first submessage: %v", err)
	}

	rawSecond, err := second.Submessage(true)
	if err != nil {
		t.Fatalf("second submessage: %v", err)
	}

	msg, err := rtps.EncodeMessage(header, []rtps.RawSubmessage{rawFirst, rawSecond})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	ctx := t.Context()
	backend.dispatchRTPS(ctx, msg, backend.spdpCh, backend.sedpCh, backend.dataCh)

	if len(backend.dataCh) != 2 {
		t.Fatalf("data queue = %d, want 2", len(backend.dataCh))
	}

	firstItem := <-backend.dataCh
	secondItem := <-backend.dataCh

	if len(firstItem.payload) == 0 || len(secondItem.payload) == 0 {
		t.Fatalf("empty payload in multi-sub")
	}

	if bytes.Equal(firstItem.payload, secondItem.payload) {
		t.Fatalf("multi-sub payloads equal, want distinct")
	}
}

// TestBackendDemuxLegacyPrefixes checks legacy SPDP and SEDP constants route best effort.
func TestBackendDemuxLegacyPrefixes(t *testing.T) {
	t.Parallel()

	if legacySPDPrefix != "SPDP" {
		t.Fatalf("legacy SPDP prefix = %q, want SPDP", legacySPDPrefix)
	}

	if legacySEDPrefix != "SEDP" {
		t.Fatalf("legacy SEDP prefix = %q, want SEDP", legacySEDPrefix)
	}

	backend := &Backend{
		routes: make(map[GUIDPrefix][]int),
		spdpCh: make(chan []byte, demuxQueueDepth),
		sedpCh: make(chan []byte, demuxQueueDepth),
		dataCh: make(chan dataItem, demuxQueueDepth),
	}

	ctx := t.Context()

	spdpLegacy := append([]byte(legacySPDPrefix), 0x00, 0x01)
	backend.enqueueSPDP(ctx, backend.spdpCh, spdpLegacy)

	if len(backend.spdpCh) != 1 {
		t.Fatalf("legacy spdp queue = %d, want 1", len(backend.spdpCh))
	}

	got := <-backend.spdpCh
	if !bytes.HasPrefix(got, []byte(legacySPDPrefix)) {
		t.Fatalf("legacy spdp bytes lost prefix")
	}

	sedpLegacy := append([]byte(legacySEDPrefix), 0x02, 0x03)
	backend.enqueueSEDP(ctx, backend.sedpCh, sedpLegacy)

	if len(backend.sedpCh) != 1 {
		t.Fatalf("legacy sedp queue = %d, want 1", len(backend.sedpCh))
	}

	got = <-backend.sedpCh
	if !bytes.HasPrefix(got, []byte(legacySEDPrefix)) {
		t.Fatalf("legacy sedp bytes lost prefix")
	}

	before := backend.demuxErrorCount()
	backend.dispatchRTPS(ctx, []byte{0x00, 0x01, 0x02}, backend.spdpCh, backend.sedpCh, backend.dataCh)

	if backend.demuxErrorCount() != before+1 {
		t.Fatalf("demux errors = %d, want %d", backend.demuxErrorCount(), before+1)
	}
}

// mustMarshalAliveBackend builds one SPDP alive packet or fails the test.
func mustMarshalAliveBackend(t *testing.T, guid GUID, domain uint32, seq uint64) []byte {
	t.Helper()

	packet, err := MarshalAlivePacket(guid, domain, seq, DefaultLeaseDuration)
	if err != nil {
		t.Fatalf("alive packet: %v", err)
	}

	return packet
}

// assertRoutePresent fails unless the prefix has a recorded route.
func assertRoutePresent(t *testing.T, backend *Backend, prefix GUIDPrefix) {
	t.Helper()

	backend.mu.RLock()
	_, ok := backend.routes[prefix]
	backend.mu.RUnlock()

	if !ok {
		t.Fatalf("route missing for alive")
	}
}

// assertRouteAbsent fails when the prefix still has a recorded route.
func assertRouteAbsent(t *testing.T, backend *Backend, prefix GUIDPrefix) {
	t.Helper()

	backend.mu.RLock()
	_, ok := backend.routes[prefix]
	backend.mu.RUnlock()

	if ok {
		t.Fatalf("route remains after dispose")
	}
}

// checkSnoopFiltering checks domain mismatch plus self filtering return no keep.
func checkSnoopFiltering(t *testing.T, backend *Backend, domain uint32, local *LocalParticipant, packet []byte) {
	t.Helper()

	_, keep := backend.snoopAnnouncement(packet, domain+1, local)
	if keep {
		t.Fatalf("domain mismatch keep is true, want false")
	}

	selfPacket := mustMarshalAliveBackend(t, local.GUID(), domain, 2)

	_, keep = backend.snoopAnnouncement(selfPacket, domain, local)
	if keep {
		t.Fatalf("self keep is true, want false")
	}
}

// checkSnoopDispose checks a dispose packet deletes the route.
func checkSnoopDispose(t *testing.T, backend *Backend, guid GUID, domain uint32, local *LocalParticipant) {
	t.Helper()

	dispose, err := MarshalDisposePacket(guid, domain, 2)
	if err != nil {
		t.Fatalf("dispose packet: %v", err)
	}

	_, keep := backend.snoopAnnouncement(dispose, domain, local)
	if !keep {
		t.Fatalf("dispose keep is false, want true")
	}

	assertRouteAbsent(t, backend, guid.Prefix)
}

// checkParsedPortsMatch checks parsed SPDP ports equal want order.
func checkParsedPortsMatch(t *testing.T, parsed *Announcement, wantPorts []uint16) {
	t.Helper()

	if len(parsed.Ports) != len(wantPorts) {
		t.Fatalf("parsed ports = %v, want %v", parsed.Ports, wantPorts)
	}

	for i := range wantPorts {
		if parsed.Ports[i] != wantPorts[i] {
			t.Fatalf("parsed port %d = %d, want %d", i, parsed.Ports[i], wantPorts[i])
		}
	}
}

// checkStoredPortsMatch checks stored route ports equal want order.
func checkStoredPortsMatch(t *testing.T, backend *Backend, prefix GUIDPrefix, wantPorts []uint16) {
	t.Helper()

	backend.mu.RLock()
	stored := append([]int(nil), backend.routes[prefix]...)
	backend.mu.RUnlock()

	if len(stored) != len(wantPorts) {
		t.Fatalf("stored ports = %v, want %v", stored, wantPorts)
	}

	for i := range wantPorts {
		if stored[i] != int(wantPorts[i]) {
			t.Fatalf("stored port %d = %d, want %d", i, stored[i], wantPorts[i])
		}
	}
}

// checkSnapshotContainsPorts checks snapshot holds each want port.
func checkSnapshotContainsPorts(t *testing.T, snap []int, wantPorts []uint16) {
	t.Helper()

	for _, want := range wantPorts {
		if !slices.Contains(snap, int(want)) {
			t.Fatalf("snapshot %v misses port %d", snap, want)
		}
	}
}

// TestBackendSnoopAnnouncementRoutes checks route record plus domain plus self plus dispose.
func TestBackendSnoopAnnouncementRoutes(t *testing.T) {
	clearBackendEnv(t)

	backend, err := NewBackend(DefaultSidecarConfig())
	if err != nil {
		t.Fatalf("new backend: %v", err)
	}

	defer func() {
		err := backend.Close()
		if err != nil {
			t.Errorf("close: %v", err)
		}
	}()

	const domain uint32 = 77

	local, err := NewLocalParticipant(domain, DefaultLeaseDuration)
	if err != nil {
		t.Fatalf("local: %v", err)
	}

	var guid GUID
	guid.Prefix[0] = 0xAA
	guid.Prefix[1] = 0xBB
	guid.EntityID = [4]byte{0x00, 0x00, 0x01, 0xC1}

	packet := mustMarshalAliveBackend(t, guid, domain, 1)

	parsed, keep := backend.snoopAnnouncement(packet, domain, local)
	if !keep {
		t.Fatalf("alive keep is false, want true")
	}

	if parsed == nil {
		t.Fatalf("alive parsed is nil")
	}

	metaPort, err := rtps.MetatrafficUnicastPort(domain, 0)
	if err != nil {
		t.Fatalf("meta port: %v", err)
	}

	userPort, err := rtps.UsertrafficUnicastPort(domain, 0)
	if err != nil {
		t.Fatalf("user port: %v", err)
	}

	wantPorts := []uint16{uint16(metaPort), uint16(userPort)} //nolint:gosec // G115: standard ports fit uint16.
	checkParsedPortsMatch(t, parsed, wantPorts)
	assertRoutePresent(t, backend, guid.Prefix)
	checkStoredPortsMatch(t, backend, guid.Prefix, wantPorts)
	checkSnapshotContainsPorts(t, backend.snapshotRoutes(), wantPorts)
	checkSnoopFiltering(t, backend, domain, local, packet)
	checkSnoopDispose(t, backend, guid, domain, local)
}

// TestBackendGenerationBumps checks discovery plus SPDP plus SEDP counters.
func TestBackendGenerationBumps(t *testing.T) {
	backend := newTestBackend(t, DefaultSidecarConfig())

	defer func() {
		err := backend.Close()
		if err != nil {
			t.Errorf("close: %v", err)
		}
	}()

	if got := backend.DiscoveryGeneration(); got != 0 {
		t.Fatalf("initial generation = %d, want 0", got)
	}

	backend.bumpDiscovery()

	if got := backend.DiscoveryGeneration(); got != 1 {
		t.Fatalf("after bumpDiscovery = %d, want 1", got)
	}

	backend.bumpSPDP()

	if got := backend.DiscoveryGeneration(); got != 2 {
		t.Fatalf("after bumpSPDP generation = %d, want 2", got)
	}

	backend.mu.RLock()
	spdp := backend.spdpCount
	sedpBefore := backend.sedpCount
	backend.mu.RUnlock()

	if spdp != 1 {
		t.Fatalf("spdp count = %d, want 1", spdp)
	}

	backend.bumpSEDP()

	backend.mu.RLock()
	sedp := backend.sedpCount
	gen := backend.discoveryGen
	backend.mu.RUnlock()

	if sedp != sedpBefore+1 {
		t.Fatalf("sedp count = %d, want %d", sedp, sedpBefore+1)
	}

	if gen != 2 {
		t.Fatalf("generation after sedp = %d, want 2", gen)
	}
}

// TestBackendRouteErrorTable checks topic plus domain appear and ErrNoRoute wraps.
func TestBackendRouteErrorTable(t *testing.T) {
	t.Parallel()

	cases := []struct {
		topic  string
		domain uint32
	}{
		{"/chatter", 0},
		{"/ns/topic", 42},
		{"/a/b/c", 77},
	}

	for _, tc := range cases {
		err := routeError(tc.topic, tc.domain, ErrNoRoute)
		if !errors.Is(err, ErrNoRoute) {
			t.Fatalf("route error %q misses ErrNoRoute", tc.topic)
		}

		if !strings.Contains(err.Error(), tc.topic) {
			t.Fatalf("route error %q misses topic", err.Error())
		}

		domainStr := strconv.Itoa(int(tc.domain))
		if !strings.Contains(err.Error(), domainStr) {
			t.Fatalf("route error %q misses domain %d", err.Error(), tc.domain)
		}
	}
}

// TestBackendCancelPaths checks canceled contexts report context errors without transport.
func TestBackendCancelPaths(t *testing.T) {
	backend := newTestBackend(t, DefaultSidecarConfig())

	defer func() {
		err := backend.Close()
		if err != nil {
			t.Errorf("close: %v", err)
		}
	}()

	canceled, cancel := context.WithCancel(context.Background())
	cancel()

	receiver := &spdpReceiver{backend: backend}
	_, err := receiver.Receive(canceled)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("spdp receive canceled = %v, want context.Canceled", err)
	}

	sedpRecv := &sedpReceiver{backend: backend}
	_, err = sedpRecv.ReceiveSEDP(canceled)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("sedp receive canceled = %v, want context.Canceled", err)
	}

	source := &frameSource{backend: backend}
	_, _, _, err = source.ReadFrame(canceled)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("frame source canceled = %v, want context.Canceled", err)
	}

	sink := &frameSink{backend: backend}
	err = sink.WriteFrame(canceled, WriterGUID{}, []byte{1, 2, 3})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("frame sink canceled = %v, want context.Canceled", err)
	}
}

// TestBackendSnapshotPublishStruct checks live flag and struct copy behavior.
func TestBackendSnapshotPublishStruct(t *testing.T) {
	backend := newTestBackend(t, DefaultSidecarConfig())

	defer func() {
		err := backend.Close()
		if err != nil {
			t.Errorf("close: %v", err)
		}
	}()

	snap := backend.snapshotPublish()

	if snap.live {
		t.Fatalf("stub snapshot live is true, want false")
	}

	if snap.publisher != nil || snap.sedp != nil {
		t.Fatalf("stub snapshot has publisher or sedp")
	}

	if snap.routeCount != 0 {
		t.Fatalf("stub route count = %d, want 0", snap.routeCount)
	}

	if snap.domain != backend.DomainID() {
		t.Fatalf("snapshot domain = %d, want %d", snap.domain, backend.DomainID())
	}
}

// TestBackendSedpIntervalIndependence checks SPDP and SEDP intervals stay separate.
func TestBackendSedpIntervalIndependence(t *testing.T) {
	backend := newTestBackend(t, DefaultSidecarConfig())

	defer func() {
		err := backend.Close()
		if err != nil {
			t.Errorf("close: %v", err)
		}
	}()

	backend.mu.Lock()
	backend.spdpInterval = 5 * time.Millisecond
	backend.sedpInterval = 50 * time.Millisecond
	backend.mu.Unlock()

	spdp, sedp := backend.snapshotIntervals()

	if spdp != 5*time.Millisecond {
		t.Fatalf("spdp interval = %v, want 5ms", spdp)
	}

	if sedp != 50*time.Millisecond {
		t.Fatalf("sedp interval = %v, want 50ms", sedp)
	}

	backend.mu.Lock()
	backend.sedpInterval = 7 * time.Millisecond
	backend.mu.Unlock()

	spdp, sedp = backend.snapshotIntervals()

	if spdp != 5*time.Millisecond {
		t.Fatalf("spdp moved after sedp change = %v", spdp)
	}

	if sedp != 7*time.Millisecond {
		t.Fatalf("sedp = %v, want 7ms", sedp)
	}
}

// TestBackendCloseNilsTransport checks Close clears unicast plus multicast under lock.
func TestBackendCloseNilsTransport(t *testing.T) {
	backend := newTestBackend(t, DefaultSidecarConfig())

	backend.mu.Lock()
	backend.uni = nil
	backend.mcast = nil
	backend.joined = true
	backend.done = make(chan struct{})
	backend.mu.Unlock()

	err := backend.Close()
	if err != nil {
		t.Fatalf("close: %v", err)
	}

	backend.mu.RLock()
	uni := backend.uni
	mcast := backend.mcast
	backend.mu.RUnlock()

	if uni != nil || mcast != nil {
		t.Fatalf("close left uni %v mcast %v, want nil nil", uni, mcast)
	}

	if !backend.Closed() {
		t.Fatalf("closed is false after Close")
	}
}

// TestBackendSendGoodbyeBestEffortNoLive checks goodbye is a no-op without live transport.
func TestBackendSendGoodbyeBestEffortNoLive(t *testing.T) {
	clearBackendEnv(t)

	backend, err := NewBackend(DefaultSidecarConfig())
	if err != nil {
		t.Fatalf("new backend: %v", err)
	}

	backend.sendGoodbyeBestEffort(nil)

	local, err := NewLocalParticipant(0, DefaultLeaseDuration)
	if err != nil {
		t.Fatalf("new local participant: %v", err)
	}

	backend.sendGoodbyeBestEffort(local)

	err = backend.Close()
	if err != nil {
		t.Fatalf("close: %v", err)
	}
}

// TestBackendHasRemoteEndpoint checks the topic probe for SEDP discovery.
func TestBackendHasRemoteEndpoint(t *testing.T) {
	t.Parallel()

	backend := &Backend{}
	if backend.HasRemoteEndpoint("/chatter") {
		t.Fatalf("empty backend reports endpoint, want false")
	}

	backend.sedp = mustSEDPIndexWithPrefix(t, 0x30)
	if backend.HasRemoteEndpoint("/chatter") {
		t.Fatalf("index without remotes reports endpoint, want false")
	}

	peer := mustSEDPIndexWithPrefix(t, 0x40)
	writer := mustSEDPWriter(t, peer, "/chatter", "RIHS01_abc")
	err := backend.sedp.UpsertRemote(writer)
	if err != nil {
		t.Fatalf("upsert remote: %v", err)
	}

	if !backend.HasRemoteEndpoint("/chatter") {
		t.Fatalf("missing /chatter endpoint, want true")
	}

	if backend.HasRemoteEndpoint("/other") {
		t.Fatalf("unexpected /other endpoint, want false")
	}
}

// TestBackendSedpBuiltinReaderMapping checks writer to reader pairing.
func TestBackendSedpBuiltinReaderMapping(t *testing.T) {
	t.Parallel()

	cases := []struct {
		writer rtps.EntityID
		reader rtps.EntityID
	}{
		{
			writer: rtps.EntityID{0x00, 0x00, sedpWriterEntityB14, 0xC2},
			reader: rtps.EntityID{0x00, 0x00, sedpWriterEntityB14, 0xC7},
		},
		{
			writer: rtps.EntityID{0x00, 0x00, sedpWriterEntityB14Cyclone, 0xC2},
			reader: rtps.EntityID{0x00, 0x00, sedpWriterEntityB14Cyclone, 0xC7},
		},
		{
			writer: rtps.EntityID{0x00, 0x00, sedpWriterEntityB14Alt, 0xC2},
			reader: rtps.EntityID{0x00, 0x00, sedpWriterEntityB14Alt, 0xC7},
		},
	}

	for _, tc := range cases {
		got, ok := sedpBuiltinReaderForWriter(tc.writer)
		if !ok {
			t.Fatalf("reader for %v not found, want %v", tc.writer, tc.reader)
		}

		if got != tc.reader {
			t.Fatalf("reader for %v = %v, want %v", tc.writer, got, tc.reader)
		}
	}

	user := rtps.EntityID{writerEntityUserB12, writerEntityUserB13, writerEntityUserB14, writerEntityUserB15}
	if _, ok := sedpBuiltinReaderForWriter(user); ok {
		t.Fatalf("user writer maps to reader, want false")
	}
}

// TestBackendBuildAckFinalFlag checks pure ACKs set Final and NACKs do not.
func TestBackendBuildAckFinalFlag(t *testing.T) {
	t.Parallel()

	local, err := NewLocalParticipant(0, DefaultLeaseDuration)
	if err != nil {
		t.Fatalf("local participant: %v", err)
	}

	backend := &Backend{local: local}

	var remote WriterGUID
	lg := local.GUID()
	copy(remote[:12], lg.Prefix[:])
	copy(remote[12:16], []byte{0x00, 0x00, 0x02, 0x03})

	reader := rtps.EntityID{0x00, 0x00, 0x00, 0x01}

	pure, err := backend.buildAckSubmessage(remote, reader, 10, 0, nil, true)
	if err != nil {
		t.Fatalf("build pure ack: %v", err)
	}

	if pure.Flags&rtps.AckNackFlagFinal == 0 {
		t.Fatalf("pure ack flags %#02x missing Final", pure.Flags)
	}

	nack, err := backend.buildAckSubmessage(remote, reader, 10, 32, []uint32{0xFFFFFFFF}, false)
	if err != nil {
		t.Fatalf("build nack: %v", err)
	}

	if nack.Flags&rtps.AckNackFlagFinal != 0 {
		t.Fatalf("nack flags %#02x has Final, want without", nack.Flags)
	}
}

// TestBackendAckBaseZeroNoop checks base 0 decodes and repairs as no-op.
func TestBackendAckBaseZeroNoop(t *testing.T) {
	t.Parallel()

	ack := rtps.AckNack{
		ReaderID: rtps.EntityID{0x00, 0x00, 0x00, 0x01},
		WriterID: rtps.EntityID{writerEntityUserB12, writerEntityUserB13, writerEntityUserB14, writerEntityUserB15},
		Base:     0,
		NumBits:  0,
		Bitmap:   nil,
		Count:    1,
	}

	body, err := ack.Encode(true)
	if err != nil {
		t.Fatalf("encode base 0 ack: %v", err)
	}

	req, err := decodeUserAckNack(body, rtps.FlagLittleEndian)
	if err != nil {
		t.Fatalf("decode base 0 ack: %v", err)
	}

	if req.base != 0 {
		t.Fatalf("decoded base = %d, want 0", req.base)
	}

	local, err := NewLocalParticipant(0, DefaultLeaseDuration)
	if err != nil {
		t.Fatalf("local participant: %v", err)
	}

	backend := &Backend{
		local:         local,
		repairHistory: newUserHistory(),
		routes:        make(map[GUIDPrefix][]int),
	}

	err = backend.repairFromAck(t.Context(), ack.WriterID, 0)
	if err != nil {
		t.Fatalf("repair base 0: %v", err)
	}
}
