// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package transports_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/podomy/weld/internal/transports"
)

// fakeBackend is a test Backend with canned responses.
//
// It enforces lifecycle: JoinDiscovery before data path, ErrNotJoined
// otherwise, and ErrBackendClosed after Close. It tracks liveliness
// generations per participant and copies payloads to enforce copy once.
// It is safe for concurrent use.
type fakeBackend struct {
	mu        sync.Mutex
	incoming  transports.IncomingMessage
	stored    []byte
	closed    bool
	joined    bool
	lastGen   map[string]uint64
	lastAlive map[string]bool
}

// lastGenFor returns the watermark map, creating it on first use.
func (f *fakeBackend) gens() map[string]uint64 {
	if f.lastGen == nil {
		f.lastGen = make(map[string]uint64)
	}

	return f.lastGen
}

// alives returns the alive map, creating it on first use.
func (f *fakeBackend) alives() map[string]bool {
	if f.lastAlive == nil {
		f.lastAlive = make(map[string]bool)
	}

	return f.lastAlive
}

// PublishBytes records a publish call without sending anything.
func (f *fakeBackend) PublishBytes(ctx context.Context, msg transports.OutgoingMessage) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("transports_test: context done: %w", err)
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	if f.closed {
		return transports.ErrBackendClosed
	}

	if !f.joined {
		return transports.ErrNotJoined
	}

	if msg.Payload == nil {
		f.stored = nil
	} else {
		f.stored = append([]byte(nil), msg.Payload...)
	}

	return nil
}

// TakeBytes returns a copy of the canned incoming message.
func (f *fakeBackend) TakeBytes(ctx context.Context) (transports.IncomingMessage, error) {
	if err := ctx.Err(); err != nil {
		return transports.IncomingMessage{}, fmt.Errorf("transports_test: context done: %w", err)
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	if f.closed {
		return transports.IncomingMessage{}, transports.ErrBackendClosed
	}

	if !f.joined {
		return transports.IncomingMessage{}, transports.ErrNotJoined
	}

	out := f.incoming
	if out.Payload == nil {
		return out, nil
	}

	out.Payload = append([]byte(nil), out.Payload...)

	return out, nil
}

// JoinDiscovery accepts discovery info without contacting a bus.
func (f *fakeBackend) JoinDiscovery(ctx context.Context, _ transports.DiscoveryInfo) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("transports_test: context done: %w", err)
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	if f.closed {
		return transports.ErrBackendClosed
	}

	f.joined = true

	return nil
}

// ReportLiveliness accepts a liveliness report without contacting a bus.
func (f *fakeBackend) ReportLiveliness(ctx context.Context, report transports.LivelinessReport) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("transports_test: context done: %w", err)
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	if f.closed {
		return transports.ErrBackendClosed
	}

	if !f.joined {
		return transports.ErrNotJoined
	}

	gens := f.gens()
	alives := f.alives()

	last, seen := gens[report.ParticipantName]
	if !seen {
		gens[report.ParticipantName] = report.Generation
		alives[report.ParticipantName] = report.Alive

		return nil
	}

	if report.Generation > last {
		gens[report.ParticipantName] = report.Generation
		alives[report.ParticipantName] = report.Alive

		return nil
	}

	if report.Generation == last && report.Alive == alives[report.ParticipantName] {
		return nil
	}

	return transports.ErrStaleGeneration
}

// Close marks the fake closed. It is idempotent and returns nil.
func (f *fakeBackend) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.closed = true

	return nil
}

// clearEnv unsets the domain variable for the duration of a test.
func clearEnv(t *testing.T) {
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

// TestBackendInterface checks the fake satisfies the Backend contract.
func TestBackendInterface(t *testing.T) {
	t.Parallel()

	var _ transports.Backend = (*fakeBackend)(nil)

	fake := &fakeBackend{
		incoming: transports.IncomingMessage{TopicName: "/chatter"},
	}

	err := fake.JoinDiscovery(context.Background(), transports.DiscoveryInfo{ParticipantName: "p", DomainID: 0})
	if err != nil {
		t.Fatalf("join discovery: %v", err)
	}

	got, err := fake.TakeBytes(context.Background())
	if err != nil {
		t.Fatalf("take bytes: %v", err)
	}

	if got.TopicName != "/chatter" {
		t.Fatalf("topic = %q, want %q", got.TopicName, "/chatter")
	}

	err = fake.PublishBytes(context.Background(), transports.OutgoingMessage{TopicName: "/chatter"})
	if err != nil {
		t.Fatalf("publish bytes: %v", err)
	}

	err = fake.ReportLiveliness(context.Background(), transports.LivelinessReport{ParticipantName: "p", Alive: true})
	if err != nil {
		t.Fatalf("report liveliness: %v", err)
	}

	err = fake.Close()
	if err != nil {
		t.Fatalf("close: %v", err)
	}

	err = fake.Close()
	if err != nil {
		t.Fatalf("second close is not idempotent: %v", err)
	}

	fake.mu.Lock()
	closed := fake.closed
	fake.mu.Unlock()

	if !closed {
		t.Fatalf("closed flag is false after Close")
	}
}

// TestBackendPostCloseErrors checks all methods fail after Close.
func TestBackendPostCloseErrors(t *testing.T) {
	t.Parallel()

	fake := &fakeBackend{
		incoming: transports.IncomingMessage{TopicName: "/chatter"},
	}

	err := fake.JoinDiscovery(context.Background(), transports.DiscoveryInfo{ParticipantName: "p"})
	if err != nil {
		t.Fatalf("join: %v", err)
	}

	err = fake.Close()
	if err != nil {
		t.Fatalf("close: %v", err)
	}

	err = fake.Close()
	if err != nil {
		t.Fatalf("second close: %v", err)
	}

	err = fake.PublishBytes(context.Background(), transports.OutgoingMessage{TopicName: "/chatter"})
	if !errors.Is(err, transports.ErrBackendClosed) {
		t.Fatalf("publish after close = %v, want ErrBackendClosed", err)
	}

	_, err = fake.TakeBytes(context.Background())
	if !errors.Is(err, transports.ErrBackendClosed) {
		t.Fatalf("take after close = %v, want ErrBackendClosed", err)
	}

	err = fake.JoinDiscovery(context.Background(), transports.DiscoveryInfo{ParticipantName: "p"})
	if !errors.Is(err, transports.ErrBackendClosed) {
		t.Fatalf("join after close = %v, want ErrBackendClosed", err)
	}

	err = fake.ReportLiveliness(context.Background(), transports.LivelinessReport{ParticipantName: "p", Alive: true})
	if !errors.Is(err, transports.ErrBackendClosed) {
		t.Fatalf("report after close = %v, want ErrBackendClosed", err)
	}
}

// TestBackendRequiresJoin checks data path before Join returns ErrNotJoined.
func TestBackendRequiresJoin(t *testing.T) {
	t.Parallel()

	fake := &fakeBackend{
		incoming: transports.IncomingMessage{TopicName: "/chatter"},
	}

	err := fake.PublishBytes(context.Background(), transports.OutgoingMessage{TopicName: "/chatter"})
	if !errors.Is(err, transports.ErrNotJoined) {
		t.Fatalf("publish before join = %v, want ErrNotJoined", err)
	}

	_, err = fake.TakeBytes(context.Background())
	if !errors.Is(err, transports.ErrNotJoined) {
		t.Fatalf("take before join = %v, want ErrNotJoined", err)
	}

	err = fake.ReportLiveliness(context.Background(), transports.LivelinessReport{ParticipantName: "p"})
	if !errors.Is(err, transports.ErrNotJoined) {
		t.Fatalf("report before join = %v, want ErrNotJoined", err)
	}
}

// TestBackendCopyOncePublish checks mutating caller slice is safe.
func TestBackendCopyOncePublish(t *testing.T) {
	t.Parallel()

	fake := &fakeBackend{}

	err := fake.JoinDiscovery(context.Background(), transports.DiscoveryInfo{ParticipantName: "p"})
	if err != nil {
		t.Fatalf("join: %v", err)
	}

	payload := []byte{0, 1, 2}

	err = fake.PublishBytes(context.Background(), transports.OutgoingMessage{TopicName: "/chatter", Payload: payload})
	if err != nil {
		t.Fatalf("publish: %v", err)
	}

	payload[0] = 99

	fake.mu.Lock()
	stored := append([]byte(nil), fake.stored...)
	fake.mu.Unlock()

	if len(stored) != 3 || stored[0] != 0 || stored[1] != 1 || stored[2] != 2 {
		t.Fatalf("stored = %v, want [0 1 2]", stored)
	}
}

// TestBackendCopyOnceTake checks mutating returned slice is safe.
func TestBackendCopyOnceTake(t *testing.T) {
	t.Parallel()

	fake := &fakeBackend{
		incoming: transports.IncomingMessage{TopicName: "/chatter", Payload: []byte{3, 4}},
	}

	err := fake.JoinDiscovery(context.Background(), transports.DiscoveryInfo{ParticipantName: "p"})
	if err != nil {
		t.Fatalf("join: %v", err)
	}

	first, err := fake.TakeBytes(context.Background())
	if err != nil {
		t.Fatalf("take: %v", err)
	}

	first.Payload[0] = 99

	second, err := fake.TakeBytes(context.Background())
	if err != nil {
		t.Fatalf("second take: %v", err)
	}

	if len(second.Payload) != 2 || second.Payload[0] != 3 || second.Payload[1] != 4 {
		t.Fatalf("second payload = %v, want [3 4]", second.Payload)
	}
}

// TestBackendNilPayload checks nil payload is valid both directions.
func TestBackendNilPayload(t *testing.T) {
	t.Parallel()

	fake := &fakeBackend{
		incoming: transports.IncomingMessage{TopicName: "/chatter"},
	}

	err := fake.JoinDiscovery(context.Background(), transports.DiscoveryInfo{ParticipantName: "p"})
	if err != nil {
		t.Fatalf("join: %v", err)
	}

	err = fake.PublishBytes(context.Background(), transports.OutgoingMessage{TopicName: "/chatter"})
	if err != nil {
		t.Fatalf("publish nil: %v", err)
	}

	got, err := fake.TakeBytes(context.Background())
	if err != nil {
		t.Fatalf("take nil: %v", err)
	}

	if len(got.Payload) != 0 {
		t.Fatalf("payload len = %d, want 0", len(got.Payload))
	}
}

// TestBackendContextCancel checks canceled context returns ctx error.
func TestBackendContextCancel(t *testing.T) {
	t.Parallel()

	fake := &fakeBackend{
		incoming: transports.IncomingMessage{TopicName: "/chatter"},
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := fake.JoinDiscovery(ctx, transports.DiscoveryInfo{ParticipantName: "p"})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("join canceled = %v, want context.Canceled", err)
	}

	err = fake.JoinDiscovery(context.Background(), transports.DiscoveryInfo{ParticipantName: "p"})
	if err != nil {
		t.Fatalf("join: %v", err)
	}

	err = fake.PublishBytes(ctx, transports.OutgoingMessage{TopicName: "/chatter"})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("publish canceled = %v, want context.Canceled", err)
	}

	_, err = fake.TakeBytes(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("take canceled = %v, want context.Canceled", err)
	}

	err = fake.ReportLiveliness(ctx, transports.LivelinessReport{ParticipantName: "p"})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("report canceled = %v, want context.Canceled", err)
	}
}

// TestReportLivelinessGenerationMatrix checks stale and idempotent cases.
func TestReportLivelinessGenerationMatrix(t *testing.T) {
	t.Parallel()

	fake := &fakeBackend{}

	err := fake.JoinDiscovery(context.Background(), transports.DiscoveryInfo{ParticipantName: "p"})
	if err != nil {
		t.Fatalf("join: %v", err)
	}

	err = fake.ReportLiveliness(context.Background(), transports.LivelinessReport{ParticipantName: "talker", Generation: 7, Alive: true})
	if err != nil {
		t.Fatalf("gen 7 live: %v", err)
	}

	err = fake.ReportLiveliness(context.Background(), transports.LivelinessReport{ParticipantName: "talker", Generation: 7, Alive: true})
	if err != nil {
		t.Fatalf("repeat same gen same alive is not idempotent: %v", err)
	}

	err = fake.ReportLiveliness(context.Background(), transports.LivelinessReport{ParticipantName: "talker", Generation: 7, Alive: false})
	if !errors.Is(err, transports.ErrStaleGeneration) {
		t.Fatalf("same gen different alive = %v, want ErrStaleGeneration", err)
	}

	err = fake.ReportLiveliness(context.Background(), transports.LivelinessReport{ParticipantName: "talker", Generation: 6, Alive: true})
	if !errors.Is(err, transports.ErrStaleGeneration) {
		t.Fatalf("backward gen = %v, want ErrStaleGeneration", err)
	}

	err = fake.ReportLiveliness(context.Background(), transports.LivelinessReport{ParticipantName: "talker", Generation: 8, Alive: false})
	if err != nil {
		t.Fatalf("gen 8 dispose: %v", err)
	}

	err = fake.ReportLiveliness(context.Background(), transports.LivelinessReport{ParticipantName: "talker", Generation: 8, Alive: false})
	if err != nil {
		t.Fatalf("repeat dispose is not idempotent: %v", err)
	}

	err = fake.ReportLiveliness(context.Background(), transports.LivelinessReport{ParticipantName: "other", Generation: 1, Alive: true})
	if err != nil {
		t.Fatalf("other participant gen 1: %v", err)
	}
}

// assertAllowedBackendErr fails only on unexpected backend errors.
// Concurrent Close races the data path, so nil or ErrBackendClosed are
// both allowed. Any other error is a test failure.
func assertAllowedBackendErr(t *testing.T, err error, op string) {
	t.Helper()

	if err != nil && !errors.Is(err, transports.ErrBackendClosed) {
		t.Errorf("%s: %v", op, err)
	}
}

// TestBackendConcurrentUse checks concurrent publish, take, and close.
func TestBackendConcurrentUse(t *testing.T) {
	t.Parallel()

	fake := &fakeBackend{
		incoming: transports.IncomingMessage{TopicName: "/chatter", Payload: []byte{1}},
	}

	err := fake.JoinDiscovery(context.Background(), transports.DiscoveryInfo{ParticipantName: "p"})
	if err != nil {
		t.Fatalf("join: %v", err)
	}

	var wg sync.WaitGroup

	for range 8 {
		wg.Go(func() {
			assertAllowedBackendErr(t, fake.PublishBytes(context.Background(), transports.OutgoingMessage{TopicName: "/chatter", Payload: []byte{1, 2}}), "publish")

			_, err := fake.TakeBytes(context.Background())
			assertAllowedBackendErr(t, err, "take")

			assertAllowedBackendErr(t, fake.ReportLiveliness(context.Background(), transports.LivelinessReport{ParticipantName: "p", Alive: true}), "report")
		})
	}

	wg.Go(func() {
		if err := fake.Close(); err != nil {
			t.Errorf("close: %v", err)
		}
	})

	wg.Wait()

	err = fake.Close()
	if err != nil {
		t.Fatalf("final close: %v", err)
	}

	err = fake.PublishBytes(context.Background(), transports.OutgoingMessage{TopicName: "/chatter"})
	if !errors.Is(err, transports.ErrBackendClosed) {
		t.Fatalf("publish after concurrent close = %v, want ErrBackendClosed", err)
	}
}

// TestResolveDomainIDKeepsCallerWhenUnset checks unset keeps caller value.
func TestResolveDomainIDKeepsCallerWhenUnset(t *testing.T) {
	clearEnv(t)

	got, err := transports.ResolveDomainID(7)
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}

	if got != 7 {
		t.Fatalf("domain = %d, want 7", got)
	}
}

// TestResolveDomainIDKeepsCallerWhenEmpty checks empty keeps caller value.
func TestResolveDomainIDKeepsCallerWhenEmpty(t *testing.T) {
	t.Setenv(transports.DomainEnvVar, "")

	got, err := transports.ResolveDomainID(3)
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}

	if got != 3 {
		t.Fatalf("domain = %d, want 3", got)
	}
}

// TestResolveDomainIDKeepsCallerWhenWhitespace checks blank keeps caller value.
func TestResolveDomainIDKeepsCallerWhenWhitespace(t *testing.T) {
	t.Setenv(transports.DomainEnvVar, "   ")

	got, err := transports.ResolveDomainID(5)
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}

	if got != 5 {
		t.Fatalf("domain = %d, want 5", got)
	}
}

// TestResolveDomainIDParsesValid checks valid values parse exactly.
func TestResolveDomainIDParsesValid(t *testing.T) {
	cases := []struct {
		raw  string
		want uint32
	}{
		{raw: "0", want: 0},
		{raw: "42", want: 42},
		{raw: "101", want: 101},
		{raw: "  +42  ", want: 42},
		{raw: "+42", want: 42},
		{raw: "042", want: 42},
	}

	for _, tc := range cases {
		t.Setenv(transports.DomainEnvVar, tc.raw)

		got, err := transports.ResolveDomainID(0)
		if err != nil {
			t.Fatalf("resolve %q: %v", tc.raw, err)
		}

		if got != tc.want {
			t.Fatalf("resolve %q = %d, want %d", tc.raw, got, tc.want)
		}
	}
}

// TestResolveDomainIDEnvOverridesCaller checks env wins over caller.
func TestResolveDomainIDEnvOverridesCaller(t *testing.T) {
	t.Setenv(transports.DomainEnvVar, "42")

	got, err := transports.ResolveDomainID(7)
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}

	if got != 42 {
		t.Fatalf("domain = %d, want 42", got)
	}
}

// TestResolveDomainIDTrimsWhitespace checks surrounding spaces are ignored.
func TestResolveDomainIDTrimsWhitespace(t *testing.T) {
	t.Setenv(transports.DomainEnvVar, "  42  ")

	got, err := transports.ResolveDomainID(0)
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}

	if got != 42 {
		t.Fatalf("domain = %d, want 42", got)
	}
}

// TestResolveDomainIDPlusAndPaddedForms checks plus and zero padded forms.
func TestResolveDomainIDPlusAndPaddedForms(t *testing.T) {
	t.Setenv(transports.DomainEnvVar, "+42")

	got, err := transports.ResolveDomainID(0)
	if err != nil {
		t.Fatalf("resolve plus 42: %v", err)
	}

	if got != 42 {
		t.Fatalf("domain = %d, want 42", got)
	}

	t.Setenv(transports.DomainEnvVar, "042")

	got, err = transports.ResolveDomainID(0)
	if err != nil {
		t.Fatalf("resolve 042: %v", err)
	}

	if got != 42 {
		t.Fatalf("domain = %d, want 42", got)
	}
}

// TestResolveDomainIDRejectsNonIntegral checks garbage input is rejected.
func TestResolveDomainIDRejectsNonIntegral(t *testing.T) {
	for _, raw := range []string{"abc", "3.5", "1e2", "12x", "-1", "++42", "0x2A", "4_2", "+", "  ++42  "} {
		t.Setenv(transports.DomainEnvVar, raw)

		_, err := transports.ResolveDomainID(0)
		if err == nil {
			t.Fatalf("resolve %q succeeded, want error", raw)
		}
	}
}

// TestResolveDomainIDRejectsOverflow checks unrepresentable input fails.
func TestResolveDomainIDRejectsOverflow(t *testing.T) {
	for _, raw := range []string{"4294967296", "99999999999999999999999"} {
		t.Setenv(transports.DomainEnvVar, raw)

		_, err := transports.ResolveDomainID(0)
		if err == nil {
			t.Fatalf("resolve %q succeeded, want overflow error", raw)
		}
	}
}

// TestResolveDomainIDRejectsOutOfRange checks values above max are rejected.
func TestResolveDomainIDRejectsOutOfRange(t *testing.T) {
	for _, raw := range []string{"102", "232", "99999"} {
		t.Setenv(transports.DomainEnvVar, raw)

		_, err := transports.ResolveDomainID(0)
		if err == nil {
			t.Fatalf("resolve %q succeeded, want range error", raw)
		}
	}
}

// TestResolveDomainIDRejectsCallerAboveMax checks caller range is enforced.
func TestResolveDomainIDRejectsCallerAboveMax(t *testing.T) {
	clearEnv(t)

	_, err := transports.ResolveDomainID(999)
	if err == nil {
		t.Fatalf("resolve caller 999 succeeded, want error")
	}

	t.Setenv(transports.DomainEnvVar, "42")

	_, err = transports.ResolveDomainID(999)
	if err == nil {
		t.Fatalf("resolve caller 999 with env override succeeded, want error")
	}
}

// TestDomainConfigResolve checks DomainConfig keeps caller when unset.
func TestDomainConfigResolve(t *testing.T) {
	clearEnv(t)

	cfg := transports.DomainConfig{DomainID: 9}

	got, err := cfg.Resolve()
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}

	if got != 9 {
		t.Fatalf("domain = %d, want 9", got)
	}
}

// TestDomainConfigResolveEnvOverride checks env wins through DomainConfig.
func TestDomainConfigResolveEnvOverride(t *testing.T) {
	t.Setenv(transports.DomainEnvVar, "42")

	cfg := transports.DomainConfig{DomainID: 9}

	got, err := cfg.Resolve()
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}

	if got != 42 {
		t.Fatalf("domain = %d, want 42", got)
	}
}

// TestDomainConfigResolveCallerAboveMax checks caller range via DomainConfig.
func TestDomainConfigResolveCallerAboveMax(t *testing.T) {
	clearEnv(t)

	cfg := transports.DomainConfig{DomainID: 999}

	_, err := cfg.Resolve()
	if err == nil {
		t.Fatalf("resolve succeeded, want caller range error")
	}
}

// TestDomainConfigResolveErrorWraps checks errors are non nil and wrapped.
func TestDomainConfigResolveErrorWraps(t *testing.T) {
	t.Setenv(transports.DomainEnvVar, "nope")

	cfg := transports.DomainConfig{}

	_, err := cfg.Resolve()
	if err == nil {
		t.Fatalf("resolve succeeded, want error")
	}

	if !strings.Contains(err.Error(), transports.DomainEnvVar) {
		t.Fatalf("error %q misses env name %q", err.Error(), transports.DomainEnvVar)
	}
}

// TestDomainConfigResolveErrorChain checks the parse error chain via errors.As.
func TestDomainConfigResolveErrorChain(t *testing.T) {
	t.Setenv(transports.DomainEnvVar, "nope")

	cfg := transports.DomainConfig{}

	_, err := cfg.Resolve()
	if err == nil {
		t.Fatalf("resolve succeeded, want error")
	}

	if numErr, ok := errors.AsType[*strconv.NumError](err); !ok {
		t.Fatalf("error %v does not chain to NumError (got %v)", err, numErr)
	}
}

// TestDiscoveryRangeString checks the enum names.
func TestDiscoveryRangeString(t *testing.T) {
	t.Parallel()

	cases := map[transports.DiscoveryRange]string{
		transports.DiscoveryRangeNotSet:        "not_set",
		transports.DiscoveryRangeOff:           "off",
		transports.DiscoveryRangeLocalhost:     "localhost",
		transports.DiscoveryRangeSubnet:        "subnet",
		transports.DiscoveryRangeSystemDefault: "system_default",
	}

	for r, want := range cases {
		if got := r.String(); got != want {
			t.Fatalf("range %d = %q, want %q", int(r), got, want)
		}
	}
}

// TestDiscoveryRangeStringUnknown checks unknown values keep their number.
func TestDiscoveryRangeStringUnknown(t *testing.T) {
	t.Parallel()

	got := transports.DiscoveryRange(99).String()
	want := "DiscoveryRange(99)"

	if got != want {
		t.Fatalf("range 99 = %q, want %q", got, want)
	}
}

// TestParseDiscoveryRange checks range names parse and garbage fails.
func TestParseDiscoveryRange(t *testing.T) {
	t.Parallel()

	got, err := transports.ParseDiscoveryRange("localhost")
	if err != nil {
		t.Fatalf("parse localhost: %v", err)
	}

	if got != transports.DiscoveryRangeLocalhost {
		t.Fatalf("range = %v, want localhost", got)
	}

	_, err = transports.ParseDiscoveryRange("everywhere")
	if err == nil {
		t.Fatalf("parse everywhere succeeded, want error")
	}
}

// TestParseDiscoveryRangeVariants checks case, spaces, and empty forms.
func TestParseDiscoveryRangeVariants(t *testing.T) {
	t.Parallel()

	cases := []struct {
		raw  string
		want transports.DiscoveryRange
	}{
		{raw: "", want: transports.DiscoveryRangeNotSet},
		{raw: "not_set", want: transports.DiscoveryRangeNotSet},
		{raw: "NOT_SET", want: transports.DiscoveryRangeNotSet},
		{raw: "off", want: transports.DiscoveryRangeOff},
		{raw: "OFF", want: transports.DiscoveryRangeOff},
		{raw: "localhost", want: transports.DiscoveryRangeLocalhost},
		{raw: "LOCALHOST", want: transports.DiscoveryRangeLocalhost},
		{raw: "  localhost  ", want: transports.DiscoveryRangeLocalhost},
		{raw: "subnet", want: transports.DiscoveryRangeSubnet},
		{raw: "Subnet", want: transports.DiscoveryRangeSubnet},
		{raw: "system_default", want: transports.DiscoveryRangeSystemDefault},
		{raw: "SYSTEM_DEFAULT", want: transports.DiscoveryRangeSystemDefault},
	}

	for _, tc := range cases {
		got, err := transports.ParseDiscoveryRange(tc.raw)
		if err != nil {
			t.Fatalf("parse %q: %v", tc.raw, err)
		}

		if got != tc.want {
			t.Fatalf("parse %q = %v, want %v", tc.raw, got, tc.want)
		}
	}
}

// TestParseDiscoveryRangeRoundTrip checks String output parses back.
func TestParseDiscoveryRangeRoundTrip(t *testing.T) {
	t.Parallel()

	ranges := []transports.DiscoveryRange{
		transports.DiscoveryRangeNotSet,
		transports.DiscoveryRangeOff,
		transports.DiscoveryRangeLocalhost,
		transports.DiscoveryRangeSubnet,
		transports.DiscoveryRangeSystemDefault,
	}

	for _, r := range ranges {
		back, err := transports.ParseDiscoveryRange(r.String())
		if err != nil {
			t.Fatalf("round trip %v: %v", r, err)
		}

		if back != r {
			t.Fatalf("round trip %v = %v", r, back)
		}
	}
}

// TestResolveDiscoveryRange checks the value passes through unchanged.
func TestResolveDiscoveryRange(t *testing.T) {
	t.Parallel()

	got := transports.ResolveDiscoveryRange(transports.DiscoveryRangeSubnet)
	if got != transports.DiscoveryRangeSubnet {
		t.Fatalf("range = %v, want subnet", got)
	}

	got = transports.ResolveDiscoveryRange(transports.DiscoveryRangeNotSet)
	if got != transports.DiscoveryRangeNotSet {
		t.Fatalf("not set passthrough = %v, want not_set", got)
	}
}

// TestDiscoveryInfoConstruction checks the join info carries its fields.
func TestDiscoveryInfoConstruction(t *testing.T) {
	t.Parallel()

	info := transports.DiscoveryInfo{
		ParticipantName: "talker",
		DomainID:        42,
		Range:           transports.DiscoveryRangeLocalhost,
	}

	if info.ParticipantName != "talker" {
		t.Fatalf("participant = %q", info.ParticipantName)
	}

	if info.DomainID != 42 {
		t.Fatalf("domain = %d", info.DomainID)
	}

	if info.Range != transports.DiscoveryRangeLocalhost {
		t.Fatalf("range = %v", info.Range)
	}
}

// TestLivelinessReportConstruction checks report fields and dispose meaning.
func TestLivelinessReportConstruction(t *testing.T) {
	t.Parallel()

	live := transports.LivelinessReport{ParticipantName: "talker", DomainID: 3, Alive: true, Generation: 7}
	if !live.Alive {
		t.Fatalf("live report Alive is false")
	}

	if live.Generation != 7 {
		t.Fatalf("generation = %d", live.Generation)
	}

	gone := transports.LivelinessReport{ParticipantName: "talker", DomainID: 3, Alive: false, Generation: 8}
	if gone.Alive {
		t.Fatalf("dispose report Alive is true")
	}

	if gone.Generation != live.Generation+1 {
		t.Fatalf("dispose generation = %d, want %d", gone.Generation, live.Generation+1)
	}
}

// TestMessageConstruction checks topic, type, and payload plumbing.
func TestMessageConstruction(t *testing.T) {
	t.Parallel()

	out := transports.OutgoingMessage{TopicName: "/chatter", TypeName: "std_msgs__msg__String", Payload: []byte{0, 1, 2}}
	if out.TopicName != "/chatter" {
		t.Fatalf("topic = %q", out.TopicName)
	}

	if out.TypeName != "std_msgs__msg__String" {
		t.Fatalf("type = %q", out.TypeName)
	}

	if len(out.Payload) != 3 {
		t.Fatalf("payload len = %d", len(out.Payload))
	}

	in := transports.IncomingMessage{TopicName: "/chatter", TypeName: "std_msgs__msg__String", Payload: []byte{3, 4}}
	if in.TopicName != "/chatter" {
		t.Fatalf("topic = %q", in.TopicName)
	}

	if len(in.Payload) != 2 {
		t.Fatalf("payload len = %d", len(in.Payload))
	}
}
