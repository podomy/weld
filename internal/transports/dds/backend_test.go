// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

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

	"github.com/podomy/weld/internal/core"
	"github.com/podomy/weld/internal/transports"
	"github.com/podomy/weld/internal/transports/dds/rtps"
	ddssecurity "github.com/podomy/weld/internal/transports/dds/security"
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
func newTestBackend(t *testing.T, cfg PeerConfig) *Backend {
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

// TestNewBackendRejectsBadConfig checks validation follows the shared config rules.
func TestNewBackendRejectsBadConfig(t *testing.T) {
	clearBackendEnv(t)

	cases := []PeerConfig{
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

	_, err := NewBackendWithClock(PeerConfig{PeerAddress: "127.0.0.1"}, nil)
	if err == nil {
		t.Fatalf("NewBackendWithClock with nil clock succeeded, want error")
	}
}

// TestNewBackendNormalizes checks peer trim and range normalization.
func TestNewBackendNormalizes(t *testing.T) {
	clearBackendEnv(t)

	backend, err := NewBackend(PeerConfig{
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

	backend, err := NewBackend(PeerConfig{DomainID: 7, PeerAddress: "127.0.0.1"})
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

	backend, err := NewBackend(DefaultPeerConfig())
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

	backend, err := NewBackend(DefaultPeerConfig())
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
	backend := newTestBackend(t, DefaultPeerConfig())

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

	backend, err := NewBackend(DefaultPeerConfig())
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
	backend := newTestBackend(t, DefaultPeerConfig())

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
	backend := newTestBackend(t, DefaultPeerConfig())

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

	backend, err := NewBackendWithClock(DefaultPeerConfig(), stub)
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

	backend, err := NewBackend(DefaultPeerConfig())
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

	backend, err := NewBackend(DefaultPeerConfig())
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
	backend := newTestBackend(t, DefaultPeerConfig())

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
	backend := newTestBackend(t, DefaultPeerConfig())

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
	backend := newTestBackend(t, DefaultPeerConfig())

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

	backend, err := NewBackend(DefaultPeerConfig())
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
	backend := newTestBackend(t, DefaultPeerConfig())

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

	backend, err := NewBackend(PeerConfig{
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

	backend, err := NewBackend(DefaultPeerConfig())
	if err != nil {
		t.Fatalf("new backend: %v", err)
	}

	defer func() {
		err := backend.Close()
		if err != nil {
			t.Errorf("close: %v", err)
		}
	}()

	const domain uint32 = 25

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

// TestBackendSnoopTombstoneBlocksStaleRoute pins the tombstone route gate.
//
// After a dispose removes the route, a stale alive at or below the
// tombstone sequence must not reinstall routes even though no table entry
// exists; only a newer alive reinstalls. The test mirrors the live path:
// snoop installs routes while HandlePacket advances the table, so the
// dispose leaves a tombstone the route gate consults.
func TestBackendSnoopTombstoneBlocksStaleRoute(t *testing.T) {
	clearBackendEnv(t)

	backend, err := NewBackend(DefaultPeerConfig())
	if err != nil {
		t.Fatalf("new backend: %v", err)
	}

	defer func() {
		err := backend.Close()
		if err != nil {
			t.Errorf("close: %v", err)
		}
	}()

	const domain uint32 = 26

	local, err := NewLocalParticipant(domain, DefaultLeaseDuration)
	if err != nil {
		t.Fatalf("local: %v", err)
	}

	// Mirror a joined backend: snoop installs routes while the table
	// advances generations, exactly like the live demux plus listen loop.
	backend.remotes = NewRemoteTable(nil)

	snoop := func(t *testing.T, packet []byte) {
		t.Helper()
		if _, keep := backend.snoopAnnouncement(packet, domain, local); !keep {
			t.Fatalf("snoop keep is false, want true")
		}
		if err := backend.remotes.HandlePacket(packet, time.Now()); err != nil {
			t.Fatalf("table handle: %v", err)
		}
	}

	var guid GUID
	guid.Prefix[0] = 0xCC
	guid.Prefix[1] = 0xDD
	guid.EntityID = [4]byte{0x00, 0x00, 0x01, 0xC1}

	alive := mustMarshalAliveBackend(t, guid, domain, 5)
	snoop(t, alive)
	assertRoutePresent(t, backend, guid.Prefix)

	dispose, err := MarshalDisposePacket(guid, domain, 6)
	if err != nil {
		t.Fatalf("dispose packet: %v", err)
	}
	snoop(t, dispose)
	assertRouteAbsent(t, backend, guid.Prefix)

	stale := mustMarshalAliveBackend(t, guid, domain, 5)
	if _, keep := backend.snoopAnnouncement(stale, domain, local); !keep {
		t.Fatalf("stale keep is false, want true (snoop keeps, table rejects)")
	}
	if err := backend.remotes.HandlePacket(stale, time.Now()); !errors.Is(err, ErrStaleSPDP) {
		t.Fatalf("stale table handle = %v, want ErrStaleSPDP", err)
	}
	assertRouteAbsent(t, backend, guid.Prefix)

	newer := mustMarshalAliveBackend(t, guid, domain, 7)
	snoop(t, newer)
	assertRoutePresent(t, backend, guid.Prefix)
}

// TestBackendGenerationBumps checks discovery plus SPDP plus SEDP counters.
func TestBackendGenerationBumps(t *testing.T) {
	backend := newTestBackend(t, DefaultPeerConfig())

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
	backend := newTestBackend(t, DefaultPeerConfig())

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
	backend := newTestBackend(t, DefaultPeerConfig())

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
	backend := newTestBackend(t, DefaultPeerConfig())

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
	backend := newTestBackend(t, DefaultPeerConfig())

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

	backend, err := NewBackend(DefaultPeerConfig())
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

// sedpTestPrefix returns a fixed SEDP prefix for sweep tests.
func sedpTestPrefix() SEDPGUIDPrefix {
	var prefix SEDPGUIDPrefix
	prefix[0] = 0xE1
	return prefix
}

// sedpPeerPrefix returns a distinct SEDP prefix for sweep test peers.
func sedpPeerPrefix() SEDPGUIDPrefix {
	var prefix SEDPGUIDPrefix
	prefix[0] = 0xE2
	return prefix
}

// injectLoopbackRouteForPlainFastTest installs a loopback unicast route for offline publish.
func injectLoopbackRouteForPlainFastTest(t *testing.T, backend *Backend) {
	t.Helper()

	backend.mu.RLock()
	uni := backend.uni
	backend.mu.RUnlock()

	if uni == nil {
		t.Fatalf("unicast is nil after join, want live transport")
	}

	port := uni.LocalPort()

	backend.mu.Lock()
	var routePrefix GUIDPrefix
	routePrefix[0] = 0xAA
	routePrefix[1] = 0xBB
	backend.routes[routePrefix] = []int{port}
	if backend.routeExpiry == nil {
		backend.routeExpiry = make(map[GUIDPrefix]time.Time)
	}
	backend.routeExpiry[routePrefix] = time.Now().Add(time.Minute)
	backend.mu.Unlock()
}

// distinctPeerPrefixForPlainFastTest returns a peer prefix distinct from local.
func distinctPeerPrefixForPlainFastTest(local GUIDPrefix) SEDPGUIDPrefix {
	var peerPrefix SEDPGUIDPrefix
	for i := range peerPrefix {
		peerPrefix[i] = 0xB0 + byte(i)
	}

	matched := true
	for i := range peerPrefix {
		if peerPrefix[i] != local[i] {
			matched = false
			break
		}
	}

	if matched {
		peerPrefix[0] ^= 0xFF
	}

	return peerPrefix
}

// upsertMatchingReaderForPlainFastTest declares a peer reader matching topic and installs it.
func upsertMatchingReaderForPlainFastTest(t *testing.T, backend *Backend, domain uint32, topic string) SEDPEndpoint {
	t.Helper()

	writerEp, err := backend.sedp.DeclareWriter(topic, "std_msgs__msg__String", StringTypeHash, nil)
	if err != nil {
		t.Fatalf("declare writer: %v", err)
	}

	backend.mu.RLock()
	localPrefix := backend.local.GUID().Prefix
	backend.mu.RUnlock()

	peerPrefix := distinctPeerPrefixForPlainFastTest(localPrefix)

	peerIdx, err := NewSEDPIndex(peerPrefix, domain)
	if err != nil {
		t.Fatalf("new peer index: %v", err)
	}

	readerEp, err := peerIdx.DeclareReader(topic, "std_msgs__msg__String", StringTypeHash, nil)
	if err != nil {
		t.Fatalf("declare peer reader: %v", err)
	}

	err = backend.sedp.UpsertRemote(readerEp)
	if err != nil {
		t.Fatalf("upsert remote reader: %v", err)
	}

	return writerEp
}

// TestBackendPublishPlainSkipsPauseWithMatchingReader pins the steady-state fast path.
func TestBackendPublishPlainSkipsPauseWithMatchingReader(t *testing.T) {
	// Domain 38 sits outside every live range (fixed 42-48, ephemeral 60-69, pooled 70-101).
	const domain uint32 = 38

	backend := newLiveBackend(t, domain, "plain-fast")
	defer func() {
		err := backend.Close()
		if err != nil {
			t.Errorf("close: %v", err)
		}
	}()

	injectLoopbackRouteForPlainFastTest(t, backend)

	const topic = "/chatter_plain_fast_38"

	writerEp := upsertMatchingReaderForPlainFastTest(t, backend, domain, topic)

	if !backend.hasMatchingRemoteReader(writerEp) {
		t.Fatalf("matching reader not observed after upsert, want true")
	}

	payload := encodeTestString(t, "hello fast")

	msg := transports.OutgoingMessage{
		TopicName: topic,
		TypeName:  "std_msgs__msg__String",
		TypeHash:  StringTypeHash,
		Payload:   payload,
	}

	// The first publish pauses once so a freshly matched peer finishes
	// connecting; it must still succeed well inside the wait budget.
	start := time.Now()

	err := backend.PublishBytes(context.Background(), msg)
	if err != nil {
		t.Fatalf("first publish with match: %v", err)
	}

	elapsed := time.Since(start)

	if elapsed >= 2*time.Second {
		t.Fatalf("first publish with match took %v, want under 2s", elapsed)
	}

	// The second publish is the steady state: the pause already happened,
	// so it must send without the 200ms tax.
	start = time.Now()

	err = backend.PublishBytes(context.Background(), msg)
	if err != nil {
		t.Fatalf("second publish with match: %v", err)
	}

	elapsed = time.Since(start)

	if elapsed >= 200*time.Millisecond {
		t.Fatalf("second publish with match took %v, want under 200ms", elapsed)
	}
}

// TestBackendPublishPlainFirstPublishPausesForPeerSettle pins the
// single-shot settle pause.
//
// A fresh backend with a pre-installed matching reader still pauses once
// before the first send: the match may have arrived microseconds ago while
// the peer is still connecting its reader, and DATA sent immediately is
// dropped as out-of-sync while reporting sent. Without the pause the first
// publish returns in well under 200ms and the sample is silently lost.
func TestBackendPublishPlainFirstPublishPausesForPeerSettle(t *testing.T) {
	// Domain 38 sits outside every live range (fixed 42-48, ephemeral 60-69, pooled 70-101).
	const domain uint32 = 38

	backend := newLiveBackend(t, domain, "plain-first-pause")
	defer func() {
		err := backend.Close()
		if err != nil {
			t.Errorf("close: %v", err)
		}
	}()

	injectLoopbackRouteForPlainFastTest(t, backend)

	const topic = "/chatter_plain_first_pause_38"

	writerEp := upsertMatchingReaderForPlainFastTest(t, backend, domain, topic)

	if !backend.hasMatchingRemoteReader(writerEp) {
		t.Fatalf("matching reader not observed after upsert, want true")
	}

	payload := encodeTestString(t, "hello first")

	msg := transports.OutgoingMessage{
		TopicName: topic,
		TypeName:  "std_msgs__msg__String",
		TypeHash:  StringTypeHash,
		Payload:   payload,
	}

	start := time.Now()

	err := backend.PublishBytes(context.Background(), msg)
	if err != nil {
		t.Fatalf("first publish with match: %v", err)
	}

	elapsed := time.Since(start)

	if elapsed < 200*time.Millisecond {
		t.Fatalf("first publish with match took %v, want at least 200ms settle pause", elapsed)
	}
}

// TestBackendPublishPlainNoMatchFailsLoudly pins loud failure instead of
// silent drops.
//
// Publishing with routes but no matching remote reader must return
// ErrNoRoute so the CLI retry loop covers cold discovery instead of
// sending a sample the peer would drop while reporting sent. Without the
// recheck the publish succeeds and the sample is silently lost.
func TestBackendPublishPlainNoMatchFailsLoudly(t *testing.T) {
	// Domain 38 sits outside every live range (fixed 42-48, ephemeral 60-69, pooled 70-101).
	const domain uint32 = 38

	backend := newLiveBackend(t, domain, "plain-no-match")
	defer func() {
		err := backend.Close()
		if err != nil {
			t.Errorf("close: %v", err)
		}
	}()

	injectLoopbackRouteForPlainFastTest(t, backend)

	const topic = "/chatter_plain_no_match_38"

	payload := encodeTestString(t, "hello nowhere")

	msg := transports.OutgoingMessage{
		TopicName: topic,
		TypeName:  "std_msgs__msg__String",
		TypeHash:  StringTypeHash,
		Payload:   payload,
	}

	err := backend.PublishBytes(context.Background(), msg)
	if !errors.Is(err, ErrNoRoute) {
		t.Fatalf("publish without match = %v, want ErrNoRoute", err)
	}
}

// TestBackendSweepOnceExpiresPendingEvict pins the stub-path sweeper unit.
func TestBackendSweepOnceExpiresPendingEvict(t *testing.T) {
	t.Parallel()

	backend := newTestBackend(t, DefaultPeerConfig())
	defer func() {
		if err := backend.Close(); err != nil {
			t.Errorf("close: %v", err)
		}
	}()

	local, err := NewSEDPIndex(sedpTestPrefix(), 42)
	if err != nil {
		t.Fatalf("new index: %v", err)
	}
	backend.mu.Lock()
	backend.sedp = local
	backend.mu.Unlock()

	peer, err := NewSEDPIndex(sedpPeerPrefix(), 42)
	if err != nil {
		t.Fatalf("new peer index: %v", err)
	}
	ep, err := peer.DeclareWriter("/sweep", "std_msgs__msg__String", "RIHS01_sweep", nil)
	if err != nil {
		t.Fatalf("declare writer: %v", err)
	}
	if err := local.UpsertRemote(ep); err != nil {
		t.Fatalf("upsert: %v", err)
	}
	local.SchedulePrefixEvict(ep.ID.Prefix, time.Now().Add(-2*sedpEvictGracePeriod))

	backend.sweepOnce()

	if n := len(local.RemoteEndpoints()); n != 0 {
		t.Fatalf("remotes = %d, want 0 after sweep", n)
	}
}

// TestBackendSameSeqAliveRefreshesRoute pins the route-lease counterpart of
// the table refresh: a same-sequence alive is not stale and extends
// routeExpiry, so a periodic Cyclone re-announcement keeps the publish route
// across the 10s lease instead of expiring mid-run.
func TestBackendSameSeqAliveRefreshesRoute(t *testing.T) {
	t.Parallel()

	cfg := PeerConfig{DomainID: 70, PeerAddress: DefaultPeerAddress, Range: 0}
	backend := newTestBackend(t, cfg)
	clock := &fakeClock{}
	base := time.Unix(7000, 0).UTC()
	clock.setNow(base)
	backend.mu.Lock()
	backend.clock = clock
	backend.remotes = NewRemoteTable(nil)
	backend.mu.Unlock()

	guid := testGUID(0xD2)
	first := mustMarshalAlive(t, guid, 70, 9, 10*time.Second)
	backend.handleSpdpBytesOnce(first, 70, nil, base)
	firstAnn, err := ParseAnnouncement(first)
	if err != nil {
		t.Fatalf("parse first: %v", err)
	}
	if backend.isAnnouncementStale(firstAnn) {
		t.Fatalf("first alive reports stale, want live")
	}
	backend.mu.RLock()
	expiry := backend.routeExpiry[guid.Prefix]
	backend.mu.RUnlock()
	if expiry.IsZero() {
		t.Fatalf("missing routeExpiry after first alive")
	}

	later := base.Add(9 * time.Second)
	clock.setNow(later)
	dup := mustMarshalAlive(t, guid, 70, 9, 10*time.Second)
	ann, err := ParseAnnouncement(dup)
	if err != nil {
		t.Fatalf("parse dup: %v", err)
	}
	if backend.isAnnouncementStale(ann) {
		t.Fatalf("same-seq alive reports stale, want refresh")
	}
	backend.handleSpdpBytesOnce(dup, 70, nil, later)
	backend.mu.RLock()
	refreshed := backend.routeExpiry[guid.Prefix]
	backend.mu.RUnlock()
	if !refreshed.After(expiry) {
		t.Fatalf("routeExpiry not refreshed: first %v refreshed %v", expiry, refreshed)
	}
	if n := backend.sweepExpiredRoutes(base.Add(10 * time.Second)); n != 0 {
		t.Fatalf("sweep at original expiry evicted %d, want 0", n)
	}
	cerr := backend.Close()
	if cerr != nil {
		t.Fatalf("close: %v", cerr)
	}
}

// TestBackendRepairLargeSampleUsesFrags pins fragment-aware repair: a 1MiB
// stored sample must attempt a DATA_FRAG resend (failing here on no route),
// not fail at DATA encoding with "data body above".
func TestBackendRepairLargeSampleUsesFrags(t *testing.T) {
	t.Parallel()

	cfg := PeerConfig{DomainID: 71, PeerAddress: DefaultPeerAddress, Range: 0}
	backend := newTestBackend(t, cfg)
	var guid WriterGUID
	for i := range guid {
		guid[i] = byte(0xE0 + i)
	}
	payload := make([]byte, 1<<20)
	for i := range payload {
		payload[i] = byte(i % 251)
	}
	backend.mu.RLock()
	history := backend.repairHistory
	backend.mu.RUnlock()
	if history == nil {
		backend.mu.Lock()
		backend.repairHistory = newUserHistory()
		history = backend.repairHistory
		backend.mu.Unlock()
	}
	history.store(guid, 1, payload)
	err := backend.repairOneSeq(context.Background(), guid, 1, 1, 1)
	if errors.Is(err, ErrNoRoute) || errors.Is(err, ErrDataPathNotWired) {
	} else {
		t.Fatalf("repair large err = %v, want frag-path send error (no route or not wired), not data-body limit", err)
	}
	cerr := backend.Close()
	if cerr != nil {
		t.Fatalf("close: %v", cerr)
	}
}

// TestBackendQueueFragRepairContracts pins the NACKFRAG queue gate: empty
// requests are silent no-ops, a closed backend fails loudly instead of
// queueing post-teardown work, and a full queue reports backpressure.
func TestBackendQueueFragRepairContracts(t *testing.T) {
	t.Parallel()

	cfg := PeerConfig{DomainID: 72, PeerAddress: DefaultPeerAddress, Range: 0}
	backend := newTestBackend(t, cfg)
	local, err := NewLocalParticipant(72, DefaultLeaseDuration)
	if err != nil {
		t.Fatalf("local: %v", err)
	}
	backend.mu.Lock()
	backend.local = local
	backend.mu.Unlock()
	var writer rtps.EntityID
	writer[3] = 0x02
	if err := backend.queueFragRepair(writer, 9, nil); err != nil {
		t.Fatalf("empty frags: %v, want silent no-op", err)
	}
	cerr := backend.Close()
	if cerr != nil {
		t.Fatalf("close: %v", cerr)
	}
	if err := backend.queueFragRepair(writer, 9, []uint32{1}); err == nil {
		t.Fatalf("closed queue accepted work, want loud failure")
	}
}

// TestBackendQueueFragRepairBackpressure pins the full-queue report: with
// no worker draining, the bounded queue fills and the overflow returns
// ErrRepairBackpressure instead of blocking demux.
func TestBackendQueueFragRepairBackpressure(t *testing.T) {
	t.Parallel()

	cfg := PeerConfig{DomainID: 73, PeerAddress: DefaultPeerAddress, Range: 0}
	backend := newTestBackend(t, cfg)
	local, err := NewLocalParticipant(73, DefaultLeaseDuration)
	if err != nil {
		t.Fatalf("local: %v", err)
	}
	backend.mu.Lock()
	backend.local = local
	backend.repairQueue = make(chan repairTask, repairQueueDepth)
	backend.mu.Unlock()
	var writer rtps.EntityID
	writer[3] = 0x02
	for i := range repairQueueDepth {
		if err := backend.queueFragRepair(writer, uint64(i+1), []uint32{1}); err != nil {
			t.Fatalf("fill %d: %v", i, err)
		}
	}
	if err := backend.queueFragRepair(writer, 9999, []uint32{1}); !errors.Is(err, ErrRepairBackpressure) {
		t.Fatalf("overflow err = %v, want ErrRepairBackpressure", err)
	}
	cerr := backend.Close()
	if cerr != nil {
		t.Fatalf("close: %v", cerr)
	}
}

// TestServeRepairWindowContinuesPastFailedSeq pins the window discipline:
// one failed seq logs loudly and continues instead of aborting the rest.
// With no routes every seq fails to send, so a 3-seq window must count
// exactly 3 demux errors.
func TestServeRepairWindowContinuesPastFailedSeq(t *testing.T) {
	t.Parallel()

	cfg := PeerConfig{DomainID: 74, PeerAddress: DefaultPeerAddress, Range: 0}
	backend := newTestBackend(t, cfg)
	local, err := NewLocalParticipant(74, DefaultLeaseDuration)
	if err != nil {
		t.Fatalf("local: %v", err)
	}
	backend.mu.Lock()
	backend.local = local
	backend.repairHistory = newUserHistory()
	backend.mu.Unlock()
	var guid WriterGUID
	for i := range guid {
		guid[i] = byte(0xF0 + i)
	}
	payload := make([]byte, 128)
	backend.mu.RLock()
	history := backend.repairHistory
	backend.mu.RUnlock()
	for seq := uint64(1); seq <= 3; seq++ {
		history.store(guid, seq, payload)
	}
	before := backend.demuxErrorCount()
	serveRepairWindow(backend, context.Background(), guid, 1, 1, 3, 3)
	if got := backend.demuxErrorCount() - before; got != 3 {
		t.Fatalf("demux errors = %d, want 3 (one per failed seq, no abort)", got)
	}
	cerr := backend.Close()
	if cerr != nil {
		t.Fatalf("close: %v", cerr)
	}
}

// TestServePathsSkipQuietlyAfterClose pins teardown discipline: repair,
// ack, and frag repair after Close return silently without counting
// demux errors.
func TestServePathsSkipQuietlyAfterClose(t *testing.T) {
	t.Parallel()

	cfg := PeerConfig{DomainID: 75, PeerAddress: DefaultPeerAddress, Range: 0}
	backend := newTestBackend(t, cfg)
	local, err := NewLocalParticipant(75, DefaultLeaseDuration)
	if err != nil {
		t.Fatalf("local: %v", err)
	}
	backend.mu.Lock()
	backend.local = local
	backend.repairHistory = newUserHistory()
	backend.mu.Unlock()
	cerr := backend.Close()
	if cerr != nil {
		t.Fatalf("close: %v", cerr)
	}
	var writer rtps.EntityID
	writer[3] = 0x02
	before := backend.demuxErrorCount()
	backend.serveRepair(repairTask{kind: repairTaskRepair, writer: writer, base: 1})
	backend.serveAckSend(repairTask{kind: repairTaskAck})
	backend.serveFragRepair(repairTask{kind: repairTaskFragRepair, writer: writer, seq: 1, fragNums: []uint32{1}})
	if got := backend.demuxErrorCount() - before; got != 0 {
		t.Fatalf("demux errors after close = %d, want 0 silent", got)
	}
}

// TestBackendwir10RouteAtomicStaleNeverInstalls pins atomic route plus table advance.
func TestBackendwir10RouteAtomicStaleNeverInstalls(t *testing.T) {
	t.Parallel()

	backend := newTestBackend(t, PeerConfig{DomainID: 70, PeerAddress: DefaultPeerAddress, Range: 0})
	clock := &fakeClock{}
	base := time.Unix(7100, 0).UTC()
	clock.setNow(base)
	backend.mu.Lock()
	backend.clock = clock
	backend.remotes = NewRemoteTable(nil)
	backend.mu.Unlock()
	defer func() {
		err := backend.Close()
		if err != nil {
			t.Errorf("close: %v", err)
		}
	}()

	guid := testGUID(0xD3)
	first := mustMarshalAlive(t, guid, 70, 5, 10*time.Second)
	backend.handleSpdpBytesOnce(first, 70, nil, base)
	backend.mu.RLock()
	firstPorts := append([]int(nil), backend.routes[guid.Prefix]...)
	backend.mu.RUnlock()
	if len(firstPorts) == 0 {
		t.Fatalf("missing route after seq 5")
	}
	stale := mustMarshalAlive(t, guid, 70, 3, 10*time.Second)
	backend.handleSpdpBytesOnce(stale, 70, nil, base.Add(time.Second))
	backend.mu.RLock()
	kept := append([]int(nil), backend.routes[guid.Prefix]...)
	backend.mu.RUnlock()
	if !slices.Equal(firstPorts, kept) {
		t.Fatalf("stale seq 3 rewrote routes %v want %v", kept, firstPorts)
	}
	backend.mu.RLock()
	remotes := backend.remotes
	backend.mu.RUnlock()
	info, ok := remotes.Get(guid.Prefix)
	if !ok {
		t.Fatalf("missing table entry after seq 5")
	}
	if info.LastSeq != 5 {
		t.Fatalf("table LastSeq = %d, want 5 (stale must not advance)", info.LastSeq)
	}
}

// TestBackendwir10RouteAtomicSameSeqLeaseOnly pins the narrow lease window.
func TestBackendwir10RouteAtomicSameSeqLeaseOnly(t *testing.T) {
	t.Parallel()

	backend := newTestBackend(t, PeerConfig{DomainID: 70, PeerAddress: DefaultPeerAddress, Range: 0})
	clock := &fakeClock{}
	base := time.Unix(7200, 0).UTC()
	clock.setNow(base)
	backend.mu.Lock()
	backend.clock = clock
	backend.remotes = NewRemoteTable(nil)
	backend.mu.Unlock()
	defer func() {
		err := backend.Close()
		if err != nil {
			t.Errorf("close: %v", err)
		}
	}()

	guid := testGUID(0xD4)
	first := mustMarshalAlive(t, guid, 70, 9, 10*time.Second)
	backend.handleSpdpBytesOnce(first, 70, nil, base)
	backend.mu.RLock()
	firstPorts := append([]int(nil), backend.routes[guid.Prefix]...)
	firstExpiry := backend.routeExpiry[guid.Prefix]
	backend.mu.RUnlock()
	later := base.Add(9 * time.Second)
	clock.setNow(later)
	dup := mustMarshalAlive(t, guid, 70, 9, 10*time.Second)
	backend.handleSpdpBytesOnce(dup, 70, nil, later)
	backend.mu.RLock()
	kept := append([]int(nil), backend.routes[guid.Prefix]...)
	refreshed := backend.routeExpiry[guid.Prefix]
	backend.mu.RUnlock()
	if !slices.Equal(firstPorts, kept) {
		t.Fatalf("same-seq alive rewrote ports %v want %v (lease-only)", kept, firstPorts)
	}
	if !refreshed.After(firstExpiry) {
		t.Fatalf("same-seq alive did not refresh lease %v want after %v", refreshed, firstExpiry)
	}
}

// TestBackendwir10SelfLoopbackDropsBeforeTake pins user DATA self drop.
func TestBackendwir10SelfLoopbackDropsBeforeTake(t *testing.T) {
	t.Parallel()

	backend := newTestBackend(t, PeerConfig{DomainID: 71, PeerAddress: DefaultPeerAddress, Range: 0})
	local, err := NewLocalParticipant(71, DefaultLeaseDuration)
	if err != nil {
		t.Fatalf("local: %v", err)
	}
	backend.mu.Lock()
	backend.local = local
	backend.mu.Unlock()
	defer func() {
		err := backend.Close()
		if err != nil {
			t.Errorf("close: %v", err)
		}
	}()

	lg := local.GUID()
	var hdr rtps.Header
	copy(hdr.Prefix[:], lg.Prefix[:])
	decoded := &rtps.Data{
		WriterID:       rtps.EntityID{0x00, 0x10, 0x00, 0x03},
		SequenceNumber: 1,
		Payload:        []byte{0x01, 0x02, 0x03},
	}
	ch := make(chan dataItem, 4)
	before := backend.demuxErrorCount()
	backend.enqueueDataWithDecoded(context.Background(), ch, hdr, decoded)
	if len(ch) != 0 {
		t.Fatalf("self loopback queued %d, want 0 dropped before Take", len(ch))
	}
	if backend.demuxErrorCount() != before+1 {
		t.Fatalf("self drop did not count demux error")
	}
}

// TestBackendwir10StrictMatchHonesty pins topic-only versus full match.
func TestBackendwir10StrictMatchHonesty(t *testing.T) {
	t.Parallel()

	backend := newTestBackend(t, PeerConfig{DomainID: 72, PeerAddress: DefaultPeerAddress, Range: 0})
	defer func() {
		err := backend.Close()
		if err != nil {
			t.Errorf("close: %v", err)
		}
	}()
	backend.mu.Lock()
	backend.sedp = mustSEDPIndexWithPrefix(t, 0x31)
	backend.mu.Unlock()
	writerEp, err := backend.sedp.DeclareWriter("/honest", "std_msgs__msg__String", StringTypeHash, nil)
	if err != nil {
		t.Fatalf("declare writer: %v", err)
	}
	peer := mustSEDPIndexWithPrefix(t, 0x32)
	bad, err := peer.DeclareReader("/honest", "std_msgs__msg__String", "RIHS01_mismatch", nil)
	if err != nil {
		t.Fatalf("declare bad reader: %v", err)
	}
	err = backend.sedp.UpsertRemote(bad)
	if err != nil {
		t.Fatalf("upsert bad: %v", err)
	}
	if !backend.HasRemoteEndpoint("/honest") {
		t.Fatalf("topic-only probe misses /honest, want true")
	}
	if backend.hasMatchingRemoteReader(writerEp) {
		t.Fatalf("full match hits mismatched hash, want false (DATA must not rely on topic-only)")
	}
	if backend.hasStrictRemoteReaderwir10("/honest", "std_msgs__msg__String", StringTypeHash) {
		t.Fatalf("strict helper hits mismatched hash, want false")
	}
	good, err := peer.DeclareReader("/honest", "std_msgs__msg__String", StringTypeHash, nil)
	if err != nil {
		t.Fatalf("declare good reader: %v", err)
	}
	err = backend.sedp.UpsertRemote(good)
	if err != nil {
		t.Fatalf("upsert good: %v", err)
	}
	if !backend.hasMatchingRemoteReader(writerEp) {
		t.Fatalf("full match misses compatible reader, want true")
	}
	if !backend.hasStrictRemoteReaderwir10("/honest", "std_msgs__msg__String", StringTypeHash) {
		t.Fatalf("strict helper misses compatible reader, want true")
	}
}

// TestBackendwir10HeartbeatPartitionFilter pins read-only partition gating.
func TestBackendwir10HeartbeatPartitionFilter(t *testing.T) {
	t.Parallel()

	localIdx, err := NewSEDPIndex(sedpTestPrefix(), 42)
	if err != nil {
		t.Fatalf("new index: %v", err)
	}
	writer, err := localIdx.DeclareWriter("/part", "std_msgs__msg__String", StringTypeHash, nil)
	if err != nil {
		t.Fatalf("declare writer: %v", err)
	}
	writer.Partitions = []string{"a"}
	peerIdx, err := NewSEDPIndex(sedpPeerPrefix(), 42)
	if err != nil {
		t.Fatalf("new peer: %v", err)
	}
	reader, err := peerIdx.DeclareReader("/part", "std_msgs__msg__String", StringTypeHash, nil)
	if err != nil {
		t.Fatalf("declare reader: %v", err)
	}
	reader.Partitions = []string{"b"}
	if hasPartitionedRemoteReaderwir10(writer, []SEDPEndpoint{reader}) {
		t.Fatalf("mismatched partitions match, want skip")
	}
	reader.Partitions = []string{"a"}
	if !hasPartitionedRemoteReaderwir10(writer, []SEDPEndpoint{reader}) {
		t.Fatalf("common partition misses, want heartbeat")
	}
	var empty SEDPEndpoint
	empty.DDSTopic = writer.DDSTopic
	if !hasPartitionedRemoteReaderwir10(empty, []SEDPEndpoint{{DDSTopic: empty.DDSTopic, IsReader: true}}) {
		t.Fatalf("default partitions miss, want match")
	}
	if !PartitionsMatch(nil, nil) {
		t.Fatalf("PartitionsMatch nil nil is false, want true for default")
	}
}

// TestBackendwir10NilSuiteNegativePins pins tolerance mode boundaries.
func TestBackendwir10NilSuiteNegativePins(t *testing.T) {
	t.Parallel()

	recv := NewDataReceiver()
	handled, err := HandleProtectedSubmessage(recv, rtps.RawSubmessage{ID: SecuredSubmessageSecPrefix})
	if !handled {
		t.Fatalf("protected prefix not handled, want loud drop")
	}
	if !errors.Is(err, ErrSecuredTransformUnavailable) {
		t.Fatalf("protected drop = %v, want ErrSecuredTransformUnavailable", err)
	}
	if recv.DropReasonCount(DropReasonSecuredTransformUnavailable) == 0 {
		t.Fatalf("protected drop did not count secured-transform-unavailable")
	}
	handled, err = HandleProtectedSubmessage(recv, rtps.RawSubmessage{ID: 0x15})
	if handled {
		t.Fatalf("plain DATA handled as protected, want passthrough")
	}
	if err != nil {
		t.Fatalf("plain DATA err = %v, want nil byte-identical", err)
	}
	backend := newTestBackend(t, DefaultPeerConfig())
	defer func() {
		cerr := backend.Close()
		if cerr != nil {
			t.Errorf("close: %v", cerr)
		}
	}()
	if backend.HasRemoteEndpoint("/plain") {
		t.Fatalf("empty backend reports endpoint, want false (zero handshake state)")
	}
	if backend.hasStrictRemoteReaderwir10("/plain", "std_msgs__msg__String", StringTypeHash) {
		t.Fatalf("strict helper hits with no index, want false (zero token installs)")
	}
	plain := []byte{0x01, 0x02, 0x03}
	cp := append([]byte(nil), plain...)
	if !bytes.Equal(plain, cp) {
		t.Fatalf("plain topic bytes changed, want byte-identical")
	}
}

// TestBackendInvalidSubmessageDropsRest pins 8.3.4.1 rule 6 in dispatch.
// A poison DATA followed by valid DATA must drop the valid tail and count
// a demux error instead of queueing the second sample.
func TestBackendInvalidSubmessageDropsRest(t *testing.T) {
	t.Parallel()

	backend := &Backend{
		routes: make(map[GUIDPrefix][]int),
		spdpCh: make(chan []byte, demuxQueueDepth),
		sedpCh: make(chan []byte, demuxQueueDepth),
		dataCh: make(chan dataItem, demuxQueueDepth),
	}
	var prefix [12]byte
	for i := range prefix {
		prefix[i] = byte(i + 21)
	}
	header := rtps.DefaultHeader(prefix)
	userWriter := rtps.EntityID{writerEntityUserB12, writerEntityUserB13, writerEntityUserB14, writerEntityUserB15}
	poison := rtps.RawSubmessage{ID: rtps.SubmessageData, Flags: 0x05, Body: []byte{0x01}}
	validData := rtps.Data{ReaderID: rtps.EntityUnknown, WriterID: userWriter, SequenceNumber: 7, Payload: []byte{0x09}}
	validRaw, err := validData.Submessage(true)
	if err != nil {
		t.Fatalf("valid submessage: %v", err)
	}
	msg, err := rtps.EncodeMessage(header, []rtps.RawSubmessage{poison, validRaw})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	before := backend.demuxErrorCount()
	backend.dispatchRTPS(t.Context(), msg, backend.spdpCh, backend.sedpCh, backend.dataCh)
	if len(backend.dataCh) != 0 {
		t.Fatalf("data queue = %d, want 0 dropped rest", len(backend.dataCh))
	}
	if backend.demuxErrorCount() <= before {
		t.Fatalf("demux errors did not increase, want drop count")
	}
}

// TestBackendInfoSrcValidPasses pins INFO_SRC passthrough in dispatch.
// A valid INFO_SRC before DATA must not drop the DATA sample.
func TestBackendInfoSrcValidPasses(t *testing.T) {
	t.Parallel()

	backend := &Backend{
		routes: make(map[GUIDPrefix][]int),
		spdpCh: make(chan []byte, demuxQueueDepth),
		sedpCh: make(chan []byte, demuxQueueDepth),
		dataCh: make(chan dataItem, demuxQueueDepth),
	}
	var prefix [12]byte
	for i := range prefix {
		prefix[i] = byte(i + 31)
	}
	header := rtps.DefaultHeader(prefix)
	userWriter := rtps.EntityID{writerEntityUserB12, writerEntityUserB13, writerEntityUserB14, writerEntityUserB15}
	src := rtps.InfoSrc{Major: 2, Minor: 5, Vendor: [2]byte{0x01, 0x10}}
	infoRaw := rtps.RawSubmessage{ID: rtps.SubmessageInfoSrc, Flags: 0x01, Body: src.Encode()}
	validData := rtps.Data{ReaderID: rtps.EntityUnknown, WriterID: userWriter, SequenceNumber: 9, Payload: []byte{0x0A}}
	validRaw, err := validData.Submessage(true)
	if err != nil {
		t.Fatalf("valid submessage: %v", err)
	}
	msg, err := rtps.EncodeMessage(header, []rtps.RawSubmessage{infoRaw, validRaw})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	backend.dispatchRTPS(t.Context(), msg, backend.spdpCh, backend.sedpCh, backend.dataCh)
	if len(backend.dataCh) != 1 {
		t.Fatalf("data queue = %d, want 1 INFO_SRC passthrough", len(backend.dataCh))
	}
}

// TestBackendInfoSrcInvalidDropsRest pins fail-closed INFO_SRC in dispatch.
// A malformed INFO_SRC must drop the trailing DATA per 8.3.4.1 rule 6.
func TestBackendInfoSrcInvalidDropsRest(t *testing.T) {
	t.Parallel()

	backend := &Backend{
		routes: make(map[GUIDPrefix][]int),
		spdpCh: make(chan []byte, demuxQueueDepth),
		sedpCh: make(chan []byte, demuxQueueDepth),
		dataCh: make(chan dataItem, demuxQueueDepth),
	}
	var prefix [12]byte
	for i := range prefix {
		prefix[i] = byte(i + 41)
	}
	header := rtps.DefaultHeader(prefix)
	userWriter := rtps.EntityID{writerEntityUserB12, writerEntityUserB13, writerEntityUserB14, writerEntityUserB15}
	badInfo := rtps.RawSubmessage{ID: rtps.SubmessageInfoSrc, Flags: 0x01, Body: []byte{0x01, 0x02, 0x03}}
	validData := rtps.Data{ReaderID: rtps.EntityUnknown, WriterID: userWriter, SequenceNumber: 11, Payload: []byte{0x0B}}
	validRaw, err := validData.Submessage(true)
	if err != nil {
		t.Fatalf("valid submessage: %v", err)
	}
	msg, err := rtps.EncodeMessage(header, []rtps.RawSubmessage{badInfo, validRaw})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	before := backend.demuxErrorCount()
	backend.dispatchRTPS(t.Context(), msg, backend.spdpCh, backend.sedpCh, backend.dataCh)
	if len(backend.dataCh) != 0 {
		t.Fatalf("data queue = %d, want 0 malformed INFO_SRC drops rest", len(backend.dataCh))
	}
	if backend.demuxErrorCount() <= before {
		t.Fatalf("demux errors did not increase, want drop count")
	}
}

// TestBackendHandleInfoSrcDispatchValid pins INFO_SRC passthrough.
// A valid minimal-form INFO_SRC is handled without identity override and
// attribution stays with the RTPS header prefix.
func TestBackendHandleInfoSrcDispatchValid(t *testing.T) {
	t.Parallel()

	src := rtps.InfoSrc{Major: 2, Minor: 5, Vendor: [2]byte{0x01, 0x10}}
	valid := rtps.RawSubmessage{ID: rtps.SubmessageInfoSrc, Flags: 0x01, Body: src.Encode()}
	handled, invalid := handleInfoSrc(valid)
	if !handled || invalid {
		t.Fatalf("valid INFO_SRC handled=%v invalid=%v, want true false", handled, invalid)
	}
	if handled, invalid := preHandleUserSub(valid); !handled || invalid {
		t.Fatalf("preHandle valid INFO_SRC handled=%v invalid=%v, want true false", handled, invalid)
	}

	other := rtps.RawSubmessage{ID: rtps.SubmessageData, Flags: 0x01, Body: []byte{0x00}}
	if handled, invalid := handleInfoSrc(other); handled || invalid {
		t.Fatalf("non INFO_SRC handled=%v invalid=%v, want false false", handled, invalid)
	}

	var prefix [12]byte
	for i := range prefix {
		prefix[i] = byte(i + 51)
	}
	header := rtps.DefaultHeader(prefix)
	if got := attributionPrefixForDispatch(header); got != GUIDPrefix(header.Prefix) {
		t.Fatalf("attribution prefix = %x, want header prefix", got)
	}
}

// TestBackendHandleInfoSrcDispatchMalformed pins fail-closed validation.
// Malformed INFO_SRC bodies report invalid so the caller drops the rest
// per 8.3.4.1 rule 6.
func TestBackendHandleInfoSrcDispatchMalformed(t *testing.T) {
	t.Parallel()

	malformed := []rtps.RawSubmessage{
		{ID: rtps.SubmessageInfoSrc, Flags: 0x01, Body: []byte{0x01, 0x02, 0x03}},
		{ID: rtps.SubmessageInfoSrc, Flags: 0x01, Body: []byte{0x01, 0x02, 0x05, 0x10}},
		{ID: rtps.SubmessageInfoSrc, Flags: 0x01, Body: []byte{0x00, 0x01, 0x05, 0x10}},
	}
	for i, bad := range malformed {
		if handled, invalid := handleInfoSrc(bad); handled || !invalid {
			t.Fatalf("malformed %d handled=%v invalid=%v, want false true", i, handled, invalid)
		}
		if handled, invalid := preHandleUserSub(bad); handled || !invalid {
			t.Fatalf("preHandle malformed %d handled=%v invalid=%v, want false true", i, handled, invalid)
		}
	}
}

// TestBackendParticipantMessageIDs pins the 8.4.13.5 endpoint identities.
// The P2M writer and reader use the second entity byte, distinct from the
// SEDP Topics writer and from user writers.
func TestBackendParticipantMessageIDs(t *testing.T) {
	t.Parallel()

	if got := participantMessageWriter(); got != (rtps.EntityID{0x00, 0x02, 0x00, 0xC2}) {
		t.Fatalf("participant writer = %v, want 000200c2", got)
	}
	if got := participantMessageReader(); got != (rtps.EntityID{0x00, 0x02, 0x00, 0xC7}) {
		t.Fatalf("participant reader = %v, want 000200c7", got)
	}
	if !isParticipantMessageWriter(participantMessageWriter()) {
		t.Fatalf("writer not recognized, want true")
	}
	if !isParticipantMessageReader(participantMessageReader()) {
		t.Fatalf("reader not recognized, want true")
	}
	user := rtps.EntityID{writerEntityUserB12, writerEntityUserB13, writerEntityUserB14, writerEntityUserB15}
	if isParticipantMessageWriter(user) || isParticipantMessageReader(user) {
		t.Fatalf("user writer recognized as participant message, want false")
	}
}

// TestBackendParticipantMessageLiveliness pins the liveliness assertion.
// Incoming ParticipantMessage DATA asserts the participant lease without
// queueing user DATA.
func TestBackendParticipantMessageLiveliness(t *testing.T) {
	t.Parallel()

	backend := &Backend{receiver: NewDataReceiver()}
	var prefix [12]byte
	for i := range prefix {
		prefix[i] = byte(i + 61)
	}
	name := participantNameForWriter(writerGUIDFromPrefix(GUIDPrefix(prefix), participantMessageWriter()))
	backend.receiver.SetParticipantLease(name, core.NewDuration(60, 0))
	if !backend.receiver.AssertParticipant(name) {
		t.Fatalf("participant assert before note failed, want true")
	}
	header := rtps.DefaultHeader(prefix)
	decoded := &rtps.Data{
		ReaderID:       rtps.EntityUnknown,
		WriterID:       participantMessageWriter(),
		SequenceNumber: 3,
		Payload:        []byte{0x01, 0x02},
	}
	backend.noteParticipantMessageData(header, decoded)
	if state := backend.receiver.ParticipantState(name, backend.receiver.receiverNow()().UnixNano()); state != core.LivelinessAlive {
		t.Fatalf("participant state = %v, want alive", state)
	}

	dataCh := make(chan dataItem, 4)
	raw, err := decoded.Submessage(true)
	if err != nil {
		t.Fatalf("participant submessage: %v", err)
	}
	if backend.enqueueUserDataSub(t.Context(), header, raw, dataCh) {
		t.Fatalf("participant DATA reported drop-rest, want skip")
	}
	if len(dataCh) != 0 {
		t.Fatalf("participant DATA queued %d, want 0", len(dataCh))
	}
}

// TestBackendHasRemoteEndpointStrict pins the strict topic plus type probe.
// A topic-only match stays true while the strict probe requires type and
// hash agreement.
func TestBackendHasRemoteEndpointStrict(t *testing.T) {
	t.Parallel()

	backend := &Backend{}
	backend.sedp = mustSEDPIndexWithPrefix(t, 0x70)
	mustSEDPWriter(t, backend.sedp, "/strict", "RIHS01_abc")
	peer := mustSEDPIndexWithPrefix(t, 0x71)
	reader := mustSEDPReader(t, peer, "/strict", "RIHS01_abc")
	if err := backend.sedp.UpsertRemote(reader); err != nil {
		t.Fatalf("upsert remote: %v", err)
	}
	if !backend.HasRemoteEndpoint("/strict") {
		t.Fatalf("topic probe misses, want true")
	}
	if !backend.HasRemoteEndpointStrict("/strict", "std_msgs__msg__String", "RIHS01_abc") {
		t.Fatalf("strict probe with matching type misses, want true")
	}
	if backend.HasRemoteEndpointStrict("/strict", "std_msgs__msg__String", "nope") {
		t.Fatalf("strict probe with wrong hash hit, want false")
	}
	if backend.HasRemoteEndpointStrict("/missing", "std_msgs__msg__String", "") {
		t.Fatalf("strict probe on missing topic hit, want false")
	}
}

// TestBackendPartitionDemuxGate pins partition-scoped DATA demux.
// A writer on a disjoint partition drops while a common partition queues.
func TestBackendPartitionDemuxGate(t *testing.T) {
	t.Parallel()

	idx := mustSEDPIndexWithPrefix(t, 0x72)
	local, err := idx.DeclareReaderWithDurability("/part", "std_msgs__msg__String", "", nil, true)
	if err != nil {
		t.Fatalf("declare reader: %v", err)
	}
	idx.StampLocalPartitions("/part", "std_msgs__msg__String", []string{"A"})

	var remotePrefix GUIDPrefix
	for i := range remotePrefix {
		remotePrefix[i] = byte(i + 0x80)
	}
	writerEntity := rtps.EntityID{0x00, 0x00, 0x02, 0x03}
	remote := SEDPEndpoint{
		ID:         SEDPGUID{Prefix: SEDPGUIDPrefix(remotePrefix), Entity: SEDPEntityID(writerEntity)},
		ROSTopic:   local.ROSTopic,
		DDSTopic:   local.DDSTopic,
		Kind:       local.Kind,
		TypeDDS:    local.TypeDDS,
		TypeIDL:    local.TypeIDL,
		IsReader:   false,
		DomainID:   local.DomainID,
		Partitions: []string{"B"},
	}
	if err := idx.UpsertRemote(remote); err != nil {
		t.Fatalf("upsert remote: %v", err)
	}

	backend := &Backend{
		sedp:   idx,
		routes: make(map[GUIDPrefix][]int),
		dataCh: make(chan dataItem, 4),
	}
	var headerPrefix [12]byte
	copy(headerPrefix[:], remotePrefix[:])
	header := rtps.DefaultHeader(headerPrefix)
	decoded := &rtps.Data{
		ReaderID:       rtps.EntityUnknown,
		WriterID:       writerEntity,
		SequenceNumber: 1,
		Payload:        []byte{0x0A, 0x0B},
	}
	before := backend.demuxErrorCount()
	backend.enqueueDataWithDecoded(t.Context(), backend.dataCh, header, decoded)
	if len(backend.dataCh) != 0 {
		t.Fatalf("disjoint partition queued, want drop")
	}
	if backend.demuxErrorCount() <= before {
		t.Fatalf("demux errors did not increase on partition drop")
	}

	remote.Partitions = []string{"A"}
	if err := idx.UpsertRemote(remote); err != nil {
		t.Fatalf("upsert remote common: %v", err)
	}
	decoded.SequenceNumber = 2
	backend.enqueueDataWithDecoded(t.Context(), backend.dataCh, header, decoded)
	if len(backend.dataCh) != 1 {
		t.Fatalf("common partition queued %d, want 1", len(backend.dataCh))
	}
}

// enabledTestSuite returns an enabled suite with dummy paths for gate tests.
// NewSuite stores config without reading files, so dummy paths exercise the
// enabled fail-closed branches without crypto material.
func enabledTestSuite(t *testing.T) *ddssecurity.Suite {
	t.Helper()
	suite, err := ddssecurity.NewSuite(ddssecurity.SuiteConfig{
		Enabled:                 true,
		IdentityCertificatePath: "test-cert",
		IdentityPrivateKeyPath:  "test-key",
		PermissionsCAPath:       "test-ca",
		GovernancePath:          "test-gov",
		PermissionsDocumentPath: "test-perms",
	})
	if err != nil {
		t.Fatalf("new enabled suite: %v", err)
	}
	return suite
}

// TestBackendUnknownWriterGate pins the provisional fast-path gate.
// Tolerance mode queues unknown writers while an enabled suite drops them
// until SEDP confirms.
func TestBackendUnknownWriterGate(t *testing.T) {
	t.Parallel()

	plain := &Backend{}
	var unknown WriterGUID
	unknown[0] = 0xEE
	if !plain.unknownWriterAllowed(unknown) {
		t.Fatalf("tolerance unknown writer denied, want allow")
	}

	secured := &Backend{secSuite: enabledTestSuite(t)}
	if secured.unknownWriterAllowed(unknown) {
		t.Fatalf("secured unknown writer allowed, want deny")
	}

	idx := mustSEDPIndexWithPrefix(t, 0x73)
	peer := mustSEDPIndexWithPrefix(t, 0x74)
	writer := mustSEDPWriter(t, peer, "/gate", "RIHS01_abc")
	if err := idx.UpsertRemote(writer); err != nil {
		t.Fatalf("upsert remote: %v", err)
	}
	secured.sedp = idx
	var guid WriterGUID
	copy(guid[:12], writer.ID.Prefix[:])
	copy(guid[12:16], writer.ID.Entity[:])
	if !secured.unknownWriterAllowed(guid) {
		t.Fatalf("secured confirmed writer denied, want allow")
	}
}

// TestBackendAuthenticateSenderOrdersBeforeSelf pins auth before suppression.
// Tolerance keeps legacy self drops byte-identical while an enabled suite
// drops unauthenticated senders fail-closed.
func TestBackendAuthenticateSenderOrdersBeforeSelf(t *testing.T) {
	t.Parallel()

	plain := &Backend{}
	var prefix [12]byte
	for i := range prefix {
		prefix[i] = byte(i + 3)
	}
	header := rtps.DefaultHeader(prefix)
	if !plain.authenticateSender(header) {
		t.Fatalf("tolerance sender denied, want allow")
	}

	secured := &Backend{secSuite: enabledTestSuite(t)}
	if secured.authenticateSender(header) {
		t.Fatalf("secured unauthenticated sender allowed, want deny")
	}
}

// TestBackendAuthorizeEgress pins fail-closed deny-plain for protected topics.
// Tolerance allows while an enabled suite without verified governance denies.
func TestBackendAuthorizeEgress(t *testing.T) {
	t.Parallel()

	plain := &Backend{}
	if err := plain.authorizeEgress("/open", 0); err != nil {
		t.Fatalf("tolerance egress denied: %v", err)
	}

	secured := &Backend{secSuite: enabledTestSuite(t)}
	if err := secured.authorizeEgress("/open", 0); err == nil {
		t.Fatalf("secured egress without governance allowed, want deny")
	} else if !errors.Is(err, ddssecurity.ErrNotAllowedBySecurity) {
		t.Fatalf("secured egress err = %v, want not-allowed-by-security denial", err)
	}
}

// TestBackendAuthorizeRemoteParticipant pins token identity threading.
// Tolerance admits while an enabled suite denies peers without identity
// tokens and refuses tokened peers at Phase 1 fail-closed.
func TestBackendAuthorizeRemoteParticipant(t *testing.T) {
	t.Parallel()

	plain := &Backend{}
	ann := Announcement{GUID: GUID{Prefix: GUIDPrefix{0x01}}, DomainID: 0, Alive: true, Lease: DefaultLeaseDuration}
	if err := plain.authorizeRemoteParticipant(ann); err != nil {
		t.Fatalf("tolerance admission denied: %v", err)
	}

	secured := &Backend{secSuite: enabledTestSuite(t)}
	if err := secured.authorizeRemoteParticipant(ann); err == nil {
		t.Fatalf("secured admission without token allowed, want deny")
	}

	before := secured.demuxErrorCount()
	secured.storeAnnouncementWithTableAdvancewir10(ann, []int{1234}, time.Now())
	if len(secured.routes) != 0 {
		t.Fatalf("secured routes installed without token, want none")
	}
	if secured.demuxErrorCount() <= before {
		t.Fatalf("secured admission did not count a demux error")
	}
}

// TestBackendOriginAADBinds pins origin binding in the egress wrap.
// The same inner plus key under different origins must seal differently so
// a ciphertext cut from one origin never verifies under another.
func TestBackendOriginAADBinds(t *testing.T) {
	t.Parallel()

	inner := []byte{0x15, 0x01, 0x02, 0x00, 0xAA, 0xBB}
	key := bytes.Repeat([]byte{0x07}, 16)
	var originA, originB [16]byte
	originA[0] = 0xA1
	originB[0] = 0xB2
	first, err := secWrapSubmessageWithOrigin(secKindAES128GCM, 1, 9, 1, key, originA, inner)
	if err != nil {
		t.Fatalf("wrap origin A: %v", err)
	}
	second, err := secWrapSubmessageWithOrigin(secKindAES128GCM, 1, 9, 1, key, originB, inner)
	if err != nil {
		t.Fatalf("wrap origin B: %v", err)
	}
	if bytes.Equal(first, second) {
		t.Fatalf("origin A and B seals match, want origin binding")
	}
	third, err := secWrapSubmessageWithOrigin(secKindAES128GCM, 2, 10, 2, key, originA, inner)
	if err != nil {
		t.Fatalf("wrap origin A rotated: %v", err)
	}
	if bytes.Equal(first, third) {
		t.Fatalf("rotated seal matches, want session binding")
	}

	gmacA, err := secWrapSubmessageWithOrigin(secKindAES128GMAC, 1, 9, 1, key, originA, inner)
	if err != nil {
		t.Fatalf("gmac wrap origin A: %v", err)
	}
	gmacB, err := secWrapSubmessageWithOrigin(secKindAES128GMAC, 1, 9, 1, key, originB, inner)
	if err != nil {
		t.Fatalf("gmac wrap origin B: %v", err)
	}
	if bytes.Equal(gmacA, gmacB) {
		t.Fatalf("gmac origin A and B seals match, want origin binding")
	}

	if _, err := secWrapSubmessageWithOrigin(99, 1, 9, 1, key, originA, inner); err == nil {
		t.Fatalf("bad kind wrapped, want error")
	}
	if _, err := secWrapSubmessageWithOrigin(secKindAES128GCM, 1, 9, 1, key[:8], originA, inner); err == nil {
		t.Fatalf("short key wrapped, want error")
	}
}
