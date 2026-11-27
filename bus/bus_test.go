// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

package bus_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/podomy/weld/bus"
)

// joinInfo returns a discovery participant for tests.
func joinInfo() bus.DiscoveryInfo {
	return bus.DiscoveryInfo{
		ParticipantName: "test-participant",
		DomainID:        0,
		Range:           bus.DiscoveryRangeLocalhost,
	}
}

// openTestMQTT returns a joined in-memory backend or fails the test.
func openTestMQTT(t *testing.T) bus.Backend {
	t.Helper()

	ctx := context.Background()
	backend, err := bus.OpenMQTT(ctx, bus.DefaultMQTTConfig(), joinInfo())
	if err != nil {
		t.Fatalf("open mqtt: %v", err)
	}

	return backend
}

// closeTestBackend closes a backend or fails the test.
func closeTestBackend(t *testing.T, backend bus.Backend) {
	t.Helper()

	err := backend.Close()
	if err != nil {
		t.Fatalf("close backend: %v", err)
	}
}

// publishTestSample publishes one fixed sample.
func publishTestSample(t *testing.T, backend bus.Backend) bus.OutgoingMessage {
	t.Helper()

	out := bus.OutgoingMessage{
		TopicName: "/chatter",
		TypeName:  "std_msgs__msg__String",
		Payload:   []byte{1, 2, 3},
		TypeHash:  "RIHS01_abc",
		Leg:       bus.LegData,
	}

	err := backend.PublishBytes(context.Background(), out)
	if err != nil {
		t.Fatalf("publish: %v", err)
	}

	return out
}

// takeTestSample takes one sample with a bound.
func takeTestSample(t *testing.T, backend bus.Backend) bus.IncomingMessage {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	in, err := backend.TakeBytes(ctx)
	if err != nil {
		t.Fatalf("take: %v", err)
	}

	return in
}

// checkTestSample compares topic and type names.
func checkTestSample(t *testing.T, in bus.IncomingMessage, out bus.OutgoingMessage) {
	t.Helper()

	if in.TopicName != out.TopicName {
		t.Fatalf("topic = %q, want %q", in.TopicName, out.TopicName)
	}

	if in.TypeName != out.TypeName {
		t.Fatalf("type = %q, want %q", in.TypeName, out.TypeName)
	}

	if in.TypeHash != out.TypeHash {
		t.Fatalf("hash = %q, want %q", in.TypeHash, out.TypeHash)
	}
}

// TestNewDDSValidates checks DDS construction without dialing.
func TestNewDDSValidates(t *testing.T) {
	cfg := bus.DefaultDDSConfig()
	backend, err := bus.NewDDS(cfg)
	if err != nil {
		t.Fatalf("new dds: %v", err)
	}

	closeTestBackend(t, backend)
}

// TestNewDDSRejectsBadDomain checks domain validation.
func TestNewDDSRejectsBadDomain(t *testing.T) {
	cfg := bus.DefaultDDSConfig()
	cfg.DomainID = 999

	_, err := bus.NewDDS(cfg)
	if err == nil {
		t.Fatal("new dds with bad domain succeeded")
	}
}

// TestMQTTRoundTrip checks publish and take over the in-memory backend.
func TestMQTTRoundTrip(t *testing.T) {
	backend := openTestMQTT(t)
	defer closeTestBackend(t, backend)

	out := publishTestSample(t, backend)
	in := takeTestSample(t, backend)

	checkTestSample(t, in, out)

	if len(in.Payload) != len(out.Payload) {
		t.Fatalf("payload len = %d, want %d", len(in.Payload), len(out.Payload))
	}
}

// TestMQTTLivelinessOrdering checks generation watermarks.
func TestMQTTLivelinessOrdering(t *testing.T) {
	backend := openTestMQTT(t)
	defer closeTestBackend(t, backend)

	ctx := context.Background()
	report := bus.LivelinessReport{
		ParticipantName: "p1",
		DomainID:        0,
		Alive:           true,
		Generation:      1,
	}

	err := backend.ReportLiveliness(ctx, report)
	if err != nil {
		t.Fatalf("report: %v", err)
	}

	report.Alive = false

	err = backend.ReportLiveliness(ctx, report)
	if err == nil {
		t.Fatal("stale generation succeeded")
	}

	if !errors.Is(err, bus.ErrStaleGeneration) {
		t.Fatalf("stale err = %v, want ErrStaleGeneration", err)
	}
}

// TestRouterRequiresJoin checks the join gate propagates.
func TestRouterRequiresJoin(t *testing.T) {
	first, err := bus.NewMQTT(bus.DefaultMQTTConfig())
	if err != nil {
		t.Fatalf("new mqtt: %v", err)
	}

	defer closeTestBackend(t, first)

	backends := map[string]bus.Backend{"mqtt": first}
	router, err := bus.NewRouter(backends, bus.RouterConfig{Default: "mqtt"})
	if err != nil {
		t.Fatalf("new router: %v", err)
	}

	defer closeTestBackend(t, router)

	_, err = router.TakeBytes(context.Background())
	if err == nil {
		t.Fatal("take before join succeeded")
	}

	if !errors.Is(err, bus.ErrNotJoined) {
		t.Fatalf("take err = %v, want ErrNotJoined", err)
	}
}

// TestResolveDomain checks domain resolution bounds.
func TestResolveDomain(t *testing.T) {
	id, err := bus.ResolveDomain(0)
	if err != nil {
		t.Fatalf("resolve domain: %v", err)
	}

	if id > 101 {
		t.Fatalf("domain = %d above maximum", id)
	}
}

// TestResolveDomainRejectsBadCaller checks caller validation.
func TestResolveDomainRejectsBadCaller(t *testing.T) {
	_, err := bus.ResolveDomain(999)
	if err == nil {
		t.Fatal("resolve bad domain succeeded")
	}
}

// TestParseDiscoveryRange checks range parsing.
func TestParseDiscoveryRange(t *testing.T) {
	r, err := bus.ParseDiscoveryRange("localhost")
	if err != nil {
		t.Fatalf("parse range: %v", err)
	}

	if r != bus.DiscoveryRangeLocalhost {
		t.Fatalf("range = %v, want localhost", r)
	}
}
