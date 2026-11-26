// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

package mqtt_test

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/podomy/weld/internal/transports"
	"github.com/podomy/weld/internal/transports/mqtt"
)

// testConfig returns a minimal valid config for contract tests.
func testConfig() mqtt.MqttConfig {
	return mqtt.MqttConfig{
		TopicPrefix: "weld",
		QoS:         0,
		DomainID:    0,
	}
}

// newJoinedBackend builds a joined backend or fails the test.
func newJoinedBackend(t *testing.T) *mqtt.Backend {
	t.Helper()

	backend, err := mqtt.New(testConfig())
	if err != nil {
		t.Fatalf("new backend: %v", err)
	}

	err = backend.JoinDiscovery(context.Background(), transports.DiscoveryInfo{ParticipantName: "p", DomainID: 0})
	if err != nil {
		t.Fatalf("join discovery: %v", err)
	}

	return backend
}

// TestBackendImplementsContract checks the backend satisfies the interface.
func TestBackendImplementsContract(t *testing.T) {
	t.Parallel()

	var _ transports.Backend = (*mqtt.Backend)(nil)
}

// TestBackendRequiresJoin checks data calls before join report ErrNotJoined.
func TestBackendRequiresJoin(t *testing.T) {
	t.Parallel()

	backend, err := mqtt.New(testConfig())
	if err != nil {
		t.Fatalf("new backend: %v", err)
	}

	defer func() {
		cerr := backend.Close()
		if cerr != nil {
			t.Errorf("close: %v", cerr)
		}
	}()

	err = backend.PublishBytes(context.Background(), transports.OutgoingMessage{TopicName: "/chatter"})
	if !errors.Is(err, transports.ErrNotJoined) {
		t.Fatalf("publish before join = %v, want ErrNotJoined", err)
	}

	_, err = backend.TakeBytes(context.Background())
	if !errors.Is(err, transports.ErrNotJoined) {
		t.Fatalf("take before join = %v, want ErrNotJoined", err)
	}

	err = backend.ReportLiveliness(context.Background(), transports.LivelinessReport{ParticipantName: "p"})
	if !errors.Is(err, transports.ErrNotJoined) {
		t.Fatalf("report before join = %v, want ErrNotJoined", err)
	}
}

// TestBackendPostCloseErrors checks all calls fail after Close.
func TestBackendPostCloseErrors(t *testing.T) {
	t.Parallel()

	backend := newJoinedBackend(t)

	err := backend.Close()
	if err != nil {
		t.Fatalf("close: %v", err)
	}

	err = backend.Close()
	if err != nil {
		t.Fatalf("second close is not idempotent: %v", err)
	}

	err = backend.PublishBytes(context.Background(), transports.OutgoingMessage{TopicName: "/chatter"})
	if !errors.Is(err, transports.ErrBackendClosed) {
		t.Fatalf("publish after close = %v, want ErrBackendClosed", err)
	}

	_, err = backend.TakeBytes(context.Background())
	if !errors.Is(err, transports.ErrBackendClosed) {
		t.Fatalf("take after close = %v, want ErrBackendClosed", err)
	}

	err = backend.JoinDiscovery(context.Background(), transports.DiscoveryInfo{ParticipantName: "p"})
	if !errors.Is(err, transports.ErrBackendClosed) {
		t.Fatalf("join after close = %v, want ErrBackendClosed", err)
	}

	err = backend.ReportLiveliness(context.Background(), transports.LivelinessReport{ParticipantName: "p", Alive: true})
	if !errors.Is(err, transports.ErrBackendClosed) {
		t.Fatalf("report after close = %v, want ErrBackendClosed", err)
	}
}

// TestReportLivelinessGenerationMatrix checks stale and idempotent cases.
func TestReportLivelinessGenerationMatrix(t *testing.T) {
	t.Parallel()

	backend := newJoinedBackend(t)

	defer func() {
		cerr := backend.Close()
		if cerr != nil {
			t.Errorf("close: %v", cerr)
		}
	}()

	err := backend.ReportLiveliness(context.Background(), transports.LivelinessReport{ParticipantName: "talker", Generation: 7, Alive: true})
	if err != nil {
		t.Fatalf("gen 7 live: %v", err)
	}

	err = backend.ReportLiveliness(context.Background(), transports.LivelinessReport{ParticipantName: "talker", Generation: 7, Alive: true})
	if err != nil {
		t.Fatalf("repeat same gen same alive is not idempotent: %v", err)
	}

	err = backend.ReportLiveliness(context.Background(), transports.LivelinessReport{ParticipantName: "talker", Generation: 7, Alive: false})
	if !errors.Is(err, transports.ErrStaleGeneration) {
		t.Fatalf("same gen different alive = %v, want ErrStaleGeneration", err)
	}

	err = backend.ReportLiveliness(context.Background(), transports.LivelinessReport{ParticipantName: "talker", Generation: 6, Alive: true})
	if !errors.Is(err, transports.ErrStaleGeneration) {
		t.Fatalf("backward gen = %v, want ErrStaleGeneration", err)
	}

	err = backend.ReportLiveliness(context.Background(), transports.LivelinessReport{ParticipantName: "talker", Generation: 8, Alive: false})
	if err != nil {
		t.Fatalf("gen 8 dispose: %v", err)
	}

	err = backend.ReportLiveliness(context.Background(), transports.LivelinessReport{ParticipantName: "talker", Generation: 8, Alive: false})
	if err != nil {
		t.Fatalf("repeat dispose is not idempotent: %v", err)
	}

	err = backend.ReportLiveliness(context.Background(), transports.LivelinessReport{ParticipantName: "other", Generation: 1, Alive: true})
	if err != nil {
		t.Fatalf("other participant gen 1: %v", err)
	}
}

// TestBackendCopyOncePublish checks mutating the caller slice stays safe.
func TestBackendCopyOncePublish(t *testing.T) {
	t.Parallel()

	backend := newJoinedBackend(t)

	defer func() {
		cerr := backend.Close()
		if cerr != nil {
			t.Errorf("close: %v", cerr)
		}
	}()

	payload := []byte{0, 1, 2}

	err := backend.PublishBytes(context.Background(), transports.OutgoingMessage{TopicName: "/chatter", TypeName: "std_msgs__msg__String", Payload: payload})
	if err != nil {
		t.Fatalf("publish: %v", err)
	}

	payload[0] = 99

	got, err := backend.TakeBytes(context.Background())
	if err != nil {
		t.Fatalf("take: %v", err)
	}

	if len(got.Payload) != 3 {
		t.Fatalf("payload len = %d, want 3", len(got.Payload))
	}

	if got.Payload[0] != 0 || got.Payload[1] != 1 || got.Payload[2] != 2 {
		t.Fatalf("payload = %v, want [0 1 2]", got.Payload)
	}

	if got.TopicName != "/chatter" {
		t.Fatalf("topic = %q, want %q", got.TopicName, "/chatter")
	}

	if got.TypeName != "std_msgs__msg__String" {
		t.Fatalf("type = %q, want %q", got.TypeName, "std_msgs__msg__String")
	}
}

// TestBackendCopyOnceTake checks mutating a take result stays safe.
func TestBackendCopyOnceTake(t *testing.T) {
	t.Parallel()

	backend := newJoinedBackend(t)

	defer func() {
		cerr := backend.Close()
		if cerr != nil {
			t.Errorf("close: %v", cerr)
		}
	}()

	err := backend.PublishBytes(context.Background(), transports.OutgoingMessage{TopicName: "/chatter", Payload: []byte{3, 4}})
	if err != nil {
		t.Fatalf("publish first: %v", err)
	}

	err = backend.PublishBytes(context.Background(), transports.OutgoingMessage{TopicName: "/chatter", Payload: []byte{3, 4}})
	if err != nil {
		t.Fatalf("publish second: %v", err)
	}

	first, err := backend.TakeBytes(context.Background())
	if err != nil {
		t.Fatalf("take first: %v", err)
	}

	first.Payload[0] = 99

	second, err := backend.TakeBytes(context.Background())
	if err != nil {
		t.Fatalf("take second: %v", err)
	}

	if len(second.Payload) != 2 || second.Payload[0] != 3 || second.Payload[1] != 4 {
		t.Fatalf("second payload = %v, want [3 4]", second.Payload)
	}
}

// TestEnvelopeBoundsRejectsOversize checks the envelope size caps.
func TestEnvelopeBoundsRejectsOversize(t *testing.T) {
	t.Parallel()

	backend := newJoinedBackend(t)
	closeBackendOnCleanup(t, backend)

	ctx := context.Background()

	long := make([]byte, 1025)
	for i := range long {
		long[i] = 'a'
	}

	err := backend.PublishBytes(ctx, transports.OutgoingMessage{TopicName: "/chatter", TypeName: string(long), Payload: []byte{0x01}})
	if err == nil {
		t.Fatalf("oversize type name accepted, want error")
	}

	big := make([]byte, 33<<20)
	err = backend.PublishBytes(ctx, transports.OutgoingMessage{TopicName: "/chatter", TypeName: "std_msgs__msg__String", Payload: big})
	if err == nil {
		t.Fatalf("oversize payload accepted, want error")
	}

	err = backend.PublishBytes(ctx, transports.OutgoingMessage{TopicName: "/chatter", TypeName: "std_msgs__msg__String", Payload: []byte{0x01}})
	if err != nil {
		t.Fatalf("small publish rejected: %v", err)
	}
}

// TestBackendNilPayload checks nil payload works in both directions.
func TestBackendNilPayload(t *testing.T) {
	t.Parallel()

	backend := newJoinedBackend(t)

	defer func() {
		cerr := backend.Close()
		if cerr != nil {
			t.Errorf("close: %v", cerr)
		}
	}()

	err := backend.PublishBytes(context.Background(), transports.OutgoingMessage{TopicName: "/chatter"})
	if err != nil {
		t.Fatalf("publish nil: %v", err)
	}

	got, err := backend.TakeBytes(context.Background())
	if err != nil {
		t.Fatalf("take nil: %v", err)
	}

	if len(got.Payload) != 0 {
		t.Fatalf("payload len = %d, want 0", len(got.Payload))
	}
}

// TestBackendContextCancel checks canceled contexts return ctx errors.
func TestBackendContextCancel(t *testing.T) {
	t.Parallel()

	backend, err := mqtt.New(testConfig())
	if err != nil {
		t.Fatalf("new backend: %v", err)
	}

	defer func() {
		cerr := backend.Close()
		if cerr != nil {
			t.Errorf("close: %v", cerr)
		}
	}()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err = backend.JoinDiscovery(ctx, transports.DiscoveryInfo{ParticipantName: "p"})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("join canceled = %v, want context.Canceled", err)
	}

	err = backend.JoinDiscovery(context.Background(), transports.DiscoveryInfo{ParticipantName: "p"})
	if err != nil {
		t.Fatalf("join: %v", err)
	}

	err = backend.PublishBytes(ctx, transports.OutgoingMessage{TopicName: "/chatter"})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("publish canceled = %v, want context.Canceled", err)
	}

	_, err = backend.TakeBytes(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("take canceled = %v, want context.Canceled", err)
	}

	err = backend.ReportLiveliness(ctx, transports.LivelinessReport{ParticipantName: "p"})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("report canceled = %v, want context.Canceled", err)
	}
}

// publishTakeOnce publishes, takes, and reports liveliness once.
func publishTakeOnce(t *testing.T, backend *mqtt.Backend) {
	t.Helper()

	err := backend.PublishBytes(context.Background(), transports.OutgoingMessage{TopicName: "/chatter", Payload: []byte{1, 2}})
	assertAllowedBackendErr(t, err, "publish")

	_, terr := backend.TakeBytes(context.Background())
	assertAllowedBackendErr(t, terr, "take")

	rerr := backend.ReportLiveliness(context.Background(), transports.LivelinessReport{ParticipantName: "p", Generation: 1, Alive: true})
	if rerr != nil && !errors.Is(rerr, transports.ErrBackendClosed) && !errors.Is(rerr, transports.ErrStaleGeneration) {
		t.Errorf("report: %v", rerr)
	}
}

// assertAllowedBackendErr fails only on unexpected backend errors.
func assertAllowedBackendErr(t *testing.T, err error, op string) {
	t.Helper()

	if err != nil && !errors.Is(err, transports.ErrBackendClosed) {
		t.Errorf("%s: %v", op, err)
	}
}

// TestBackendConcurrentUse checks concurrent publish, take, and close.
func TestBackendConcurrentUse(t *testing.T) {
	t.Parallel()

	backend := newJoinedBackend(t)

	var wg sync.WaitGroup

	for range 8 {
		wg.Go(func() {
			publishTakeOnce(t, backend)
		})
	}

	wg.Go(func() {
		cerr := backend.Close()
		if cerr != nil {
			t.Errorf("close: %v", cerr)
		}
	})

	wg.Wait()

	err := backend.Close()
	if err != nil {
		t.Fatalf("final close: %v", err)
	}

	err = backend.PublishBytes(context.Background(), transports.OutgoingMessage{TopicName: "/chatter"})
	if !errors.Is(err, transports.ErrBackendClosed) {
		t.Fatalf("publish after concurrent close = %v, want ErrBackendClosed", err)
	}
}

// TestTopicRoundTrip checks encode then decode returns the input.
func TestTopicRoundTrip(t *testing.T) {
	t.Parallel()

	kinds := []mqtt.Kind{mqtt.KindData, mqtt.KindRequest, mqtt.KindReply}
	rosTopics := []string{"/chatter", "/a/b", "/"}
	domains := []uint32{0, 42, 101}

	for _, domain := range domains {
		for _, kind := range kinds {
			for _, rosTopic := range rosTopics {
				checkTopicRoundTrip(t, domain, kind, rosTopic)
			}
		}
	}
}

// checkTopicRoundTrip checks one encode plus decode round trip.
func checkTopicRoundTrip(t *testing.T, domain uint32, kind mqtt.Kind, rosTopic string) {
	t.Helper()

	wire, err := mqtt.EncodeTopic("weld", domain, kind, rosTopic)
	if err != nil {
		t.Fatalf("encode %d %s %s: %v", domain, string(kind), rosTopic, err)
	}

	gotDomain, gotKind, gotROS, err := mqtt.DecodeTopic("weld", wire)
	if err != nil {
		t.Fatalf("decode %q: %v", wire, err)
	}

	if gotDomain != domain {
		t.Fatalf("domain = %d, want %d for %q", gotDomain, domain, wire)
	}

	if gotKind != kind {
		t.Fatalf("kind = %q, want %q for %q", string(gotKind), string(kind), wire)
	}

	if gotROS != rosTopic {
		t.Fatalf("ros = %q, want %q for %q", gotROS, rosTopic, wire)
	}
}

// TestTopicRejectsWildcards checks plus, hash, and whitespace are rejected.
func TestTopicRejectsWildcards(t *testing.T) {
	t.Parallel()

	badROS := []string{"/a+b", "/a#b", "/a b", "chatter", "", "/a//b", "/a/", "/a\xff", "/a\x00"}

	for _, rosTopic := range badROS {
		_, err := mqtt.EncodeTopic("weld", 0, mqtt.KindData, rosTopic)
		if err == nil {
			t.Fatalf("encode %q succeeded, want error", rosTopic)
		}
	}

	badWire := []string{"weld/0/rt/a+b", "weld/0/rt/a#b", "weld/0/rt/a b", "weld/0/xx/a", "other/0/rt/a", "weld/999/rt/a"}

	for _, wire := range badWire {
		_, _, _, err := mqtt.DecodeTopic("weld", wire)
		if err == nil {
			t.Fatalf("decode %q succeeded, want error", wire)
		}
	}
}

// TestConfigValidate checks prefix normalization and field limits.
func TestConfigValidate(t *testing.T) {
	t.Parallel()

	normalized := mqtt.NormalizePrefix("  /weld/  ")
	if normalized != "weld" {
		t.Fatalf("normalized = %q, want %q", normalized, "weld")
	}

	normalized = mqtt.NormalizePrefix("")
	if normalized != mqtt.DefaultTopicPrefix {
		t.Fatalf("empty normalized = %q, want %q", normalized, mqtt.DefaultTopicPrefix)
	}

	bad := testConfig()
	bad.QoS = 3
	checkConfigRejected(t, bad, "qos 3")

	bad = testConfig()
	bad.DomainID = 999
	checkConfigRejected(t, bad, "domain 999")

	bad = testConfig()
	bad.TopicPrefix = "weld/+"
	checkConfigRejected(t, bad, "plus prefix")

	bad = testConfig()
	bad.Will.Payload = []byte{0x01}
	checkConfigRejected(t, bad, "will payload without topic")

	bad = testConfig()
	bad.Will.Topic = "w/t"
	bad.Will.QoS = 3
	checkConfigRejected(t, bad, "will qos 3")

	bad = testConfig()
	bad.Will.Topic = "w/#"
	checkConfigRejected(t, bad, "will wildcard")

	bad = testConfig()
	bad.TopicPrefix = "$SYS"
	checkConfigRejected(t, bad, "$ prefix")

	bad = testConfig()
	bad.Will.Topic = "$SYS/up"
	checkConfigRejected(t, bad, "$ will topic")

	bad = testConfig()
	bad.TLS.CertFile = "client.pem"
	checkConfigRejected(t, bad, "lone tls cert")

	bad = testConfig()
	bad.Password = "secret"
	checkConfigRejected(t, bad, "password without username")

	bad = testConfig()
	bad.Version = mqtt.Version(99)
	checkConfigRejected(t, bad, "unknown version")

	if got := mqtt.Version311.String(); got != "3.1.1" {
		t.Fatalf("version string = %q, want 3.1.1", got)
	}

	if got := mqtt.Version5.String(); got != "5.0" {
		t.Fatalf("version string = %q, want 5.0", got)
	}

	good := testConfig()
	good.Username = "fleet"
	good.Will.Topic = "weld/0/sdp/fleet"
	checkConfigAccepted(t, good)

	id, err := good.ResolveDomain()
	if err != nil {
		t.Fatalf("resolve domain: %v", err)
	}

	if id != 0 {
		t.Fatalf("domain = %d, want 0", id)
	}
}

// checkConfigRejected fails unless Validate rejects the config.
func checkConfigRejected(t *testing.T, cfg mqtt.MqttConfig, what string) {
	t.Helper()

	if err := cfg.Validate(); err == nil {
		t.Fatalf("%s succeeded, want error", what)
	}
}

// checkConfigAccepted fails when Validate rejects the config.
func checkConfigAccepted(t *testing.T, cfg mqtt.MqttConfig) {
	t.Helper()

	if err := cfg.Validate(); err != nil {
		t.Fatalf("validate good: %v", err)
	}
}

// TestBackendRejectsZeroValue checks the zero value fails loudly.
func TestBackendRejectsZeroValue(t *testing.T) {
	t.Parallel()

	backend := &mqtt.Backend{}

	if err := backend.Close(); err == nil {
		t.Fatalf("zero backend close succeeded, want error")
	}

	if _, err := backend.TakeBytes(context.Background()); err == nil {
		t.Fatalf("zero backend take succeeded, want error")
	}

	if err := backend.JoinDiscovery(context.Background(), transports.DiscoveryInfo{}); err == nil {
		t.Fatalf("zero backend join succeeded, want error")
	}
}

// TestBackendJoinAfterCloseFails proves a closed backend stays closed.
func TestBackendJoinAfterCloseFails(t *testing.T) {
	t.Parallel()

	backend := newJoinedBackend(t)
	ctx := context.Background()

	if err := backend.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	err := backend.JoinDiscovery(ctx, transports.DiscoveryInfo{ParticipantName: "p"})
	if !errors.Is(err, transports.ErrBackendClosed) {
		t.Fatalf("join after close err = %v, want %v", err, transports.ErrBackendClosed)
	}

	if _, err := backend.TakeBytes(ctx); !errors.Is(err, transports.ErrBackendClosed) {
		t.Fatalf("take after close err = %v, want %v", err, transports.ErrBackendClosed)
	}
}

// TestBackendLeaveDiscoveryGuards proves Leave needs join and rejects close.
func TestBackendLeaveDiscoveryGuards(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	fresh, err := mqtt.New(mqtt.MqttConfig{TopicPrefix: "weld", DomainID: 0})
	if err != nil {
		t.Fatalf("new: %v", err)
	}

	if err := fresh.LeaveDiscovery(ctx); !errors.Is(err, transports.ErrNotJoined) {
		t.Fatalf("leave before join err = %v, want %v", err, transports.ErrNotJoined)
	}

	joined := newJoinedBackend(t)

	if err := joined.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	if err := joined.LeaveDiscovery(ctx); !errors.Is(err, transports.ErrBackendClosed) {
		t.Fatalf("leave after close err = %v, want %v", err, transports.ErrBackendClosed)
	}
}

// TestAnnounceRejectKeepsGeneration proves a rejected announce does not
// advance the per-writer discovery sequence.
func TestAnnounceRejectKeepsGeneration(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	backend, err := mqtt.New(mqtt.MqttConfig{TopicPrefix: "weld", DomainID: 0})
	if err != nil {
		t.Fatalf("new: %v", err)
	}

	before := backend.DiscoveryGeneration()

	err = backend.AnnounceEndpoint(ctx, "p", mqtt.KindData, "/chatter", "t", "")
	if !errors.Is(err, transports.ErrNotJoined) {
		t.Fatalf("announce before join err = %v, want %v", err, transports.ErrNotJoined)
	}

	if got := backend.DiscoveryGeneration(); got != before {
		t.Fatalf("generation moved on rejected announce: %d -> %d", before, got)
	}

	err = backend.RetractEndpoint(ctx, "p", mqtt.KindData, "/chatter")
	if !errors.Is(err, transports.ErrNotJoined) {
		t.Fatalf("retract before join err = %v, want %v", err, transports.ErrNotJoined)
	}

	if got := backend.DiscoveryGeneration(); got != before {
		t.Fatalf("generation moved on rejected retract: %d -> %d", before, got)
	}
}
