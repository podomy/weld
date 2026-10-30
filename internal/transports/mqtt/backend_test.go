// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package mqtt

import (
	"context"
	"errors"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/podomy/weld/internal/transports"
)

// clearMQTTEnv unsets the domain variable for deterministic tests.
func clearMQTTEnv(t *testing.T) {
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

// newTestBackend returns a backend with default config.
func newTestBackend(t *testing.T) *Backend {
	t.Helper()

	clearMQTTEnv(t)

	backend, err := NewBackend(DefaultConfig())
	if err != nil {
		t.Fatalf("new backend: %v", err)
	}

	return backend
}

// TestBackendImplementsContract checks Backend satisfies transports.Backend.
func TestBackendImplementsContract(t *testing.T) {
	t.Parallel()

	var _ transports.Backend = (*Backend)(nil)
}

// TestNewBackendRejectsBadConfig checks validation follows Config rules.
func TestNewBackendRejectsBadConfig(t *testing.T) {
	t.Parallel()

	cases := []Config{
		{DomainID: 999, BrokerAddress: "tcp://127.0.0.1:1883"},
		{DomainID: 0, BrokerAddress: "   "},
		{DomainID: 0, BrokerAddress: "tcp://127.0.0.1:1883", Range: transports.DiscoveryRange(99)},
	}

	for _, cfg := range cases {
		_, err := NewBackend(cfg)
		if err == nil {
			t.Fatalf("NewBackend(%+v) succeeded, want error", cfg)
		}
	}
}

// TestNewBackendNormalizes checks broker trim and range normalization.
func TestNewBackendNormalizes(t *testing.T) {
	clearMQTTEnv(t)

	backend, err := NewBackend(Config{
		DomainID:      0,
		BrokerAddress: "  tcp://127.0.0.1:1883  ",
		Range:         transports.DiscoveryRangeNotSet,
	})
	if err != nil {
		t.Fatalf("new backend: %v", err)
	}

	cfg := backend.Config()
	if cfg.BrokerAddress != "tcp://127.0.0.1:1883" {
		t.Fatalf("broker = %q, want trimmed", cfg.BrokerAddress)
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

// TestNewBackendResolvesDomain checks ROS_DOMAIN_ID overrides caller value.
func TestNewBackendResolvesDomain(t *testing.T) {
	t.Setenv(transports.DomainEnvVar, "42")

	backend, err := NewBackend(Config{DomainID: 7, BrokerAddress: DefaultBrokerAddress})
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
	backend := newTestBackend(t)

	defer func() {
		err := backend.Close()
		if err != nil {
			t.Errorf("close: %v", err)
		}
	}()

	err := backend.PublishBytes(context.Background(), transports.OutgoingMessage{TopicName: "/chatter"})
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

// TestBackendJoinCanceledContext checks canceled join honors ctx.
func TestBackendJoinCanceledContext(t *testing.T) {
	backend := newTestBackend(t)

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

	if backend.Joined() {
		t.Fatalf("joined is true after canceled join")
	}
}

// TestBackendDataPathNotImplemented checks stub network reports sentinel.
func TestBackendDataPathNotImplemented(t *testing.T) {
	backend := newTestBackend(t)

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

	err = backend.PublishBytes(context.Background(), transports.OutgoingMessage{TopicName: "/chatter"})
	if !errors.Is(err, ErrMQTTNotImplemented) {
		t.Fatalf("publish = %v, want ErrMQTTNotImplemented", err)
	}

	if errors.Is(err, transports.ErrNotJoined) {
		t.Fatalf("publish = %v, must not be ErrNotJoined after join", err)
	}

	_, err = backend.TakeBytes(context.Background())
	if !errors.Is(err, ErrMQTTNotImplemented) {
		t.Fatalf("take = %v, want ErrMQTTNotImplemented", err)
	}
}

// TestBackendCloseIdempotent checks Close returns nil twice.
func TestBackendCloseIdempotent(t *testing.T) {
	backend := newTestBackend(t)

	if backend.Closed() {
		t.Fatalf("closed is true before Close")
	}

	err := backend.Close()
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
	backend := newTestBackend(t)

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

	err = backend.ReportLiveliness(context.Background(), transports.LivelinessReport{ParticipantName: "p"})
	if !errors.Is(err, transports.ErrBackendClosed) {
		t.Fatalf("report after close = %v, want ErrBackendClosed", err)
	}
}

// TestBackendReportGenerationMatrix checks stale and idempotent cases.
func TestBackendReportGenerationMatrix(t *testing.T) {
	backend := newTestBackend(t)

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

	err = backend.ReportLiveliness(context.Background(), transports.LivelinessReport{ParticipantName: "talker", Generation: 7, Alive: true})
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

// TestBackendContextCancel checks canceled contexts report ctx errors.
func TestBackendContextCancel(t *testing.T) {
	backend := newTestBackend(t)

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

// checkAllowedMQTT closes over allowed errors for concurrent checks.
func checkAllowedMQTT(t *testing.T, err error, op string) {
	t.Helper()

	if err != nil && !errors.Is(err, transports.ErrBackendClosed) && !errors.Is(err, ErrMQTTNotImplemented) {
		t.Errorf("%s: %v", op, err)
	}
}

// TestBackendConcurrentUse checks concurrent data path and close stay safe.
func TestBackendConcurrentUse(t *testing.T) {
	backend := newTestBackend(t)

	err := backend.JoinDiscovery(context.Background(), transports.DiscoveryInfo{ParticipantName: "p"})
	if err != nil {
		t.Fatalf("join: %v", err)
	}

	var wg sync.WaitGroup

	for range 8 {
		wg.Go(func() {
			err := backend.PublishBytes(context.Background(), transports.OutgoingMessage{TopicName: "/chatter"})
			checkAllowedMQTT(t, err, "publish")

			_, err = backend.TakeBytes(context.Background())
			checkAllowedMQTT(t, err, "take")

			err = backend.ReportLiveliness(context.Background(), transports.LivelinessReport{ParticipantName: "p", Generation: 1, Alive: true})
			if err != nil && !errors.Is(err, transports.ErrBackendClosed) && !errors.Is(err, transports.ErrStaleGeneration) {
				t.Errorf("report: %v", err)
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
}

// TestDomainPrefixIsolation checks domains map to disjoint prefixes.
func TestDomainPrefixIsolation(t *testing.T) {
	t.Parallel()

	first, err := DomainPrefix(0)
	if err != nil {
		t.Fatalf("prefix 0: %v", err)
	}

	second, err := DomainPrefix(42)
	if err != nil {
		t.Fatalf("prefix 42: %v", err)
	}

	if first == second {
		t.Fatalf("prefixes equal %q", first)
	}

	if first != "weld/d0/" {
		t.Fatalf("prefix 0 = %q, want weld/d0/", first)
	}

	if second != "weld/d42/" {
		t.Fatalf("prefix 42 = %q, want weld/d42/", second)
	}

	if strings.HasPrefix(second, first) || strings.HasPrefix(first, second) {
		t.Fatalf("prefixes overlap %q %q", first, second)
	}

	_, err = DomainPrefix(transports.MaxDomainID + 1)
	if err == nil {
		t.Fatalf("domain above max succeeded, want error")
	}
}

// TestMapTopicRoundTrip checks ROS topics map and parse back.
func TestMapTopicRoundTrip(t *testing.T) {
	t.Parallel()

	cases := []struct {
		domain uint32
		ros    string
		want   string
	}{
		{domain: 0, ros: "/chatter", want: "weld/d0/chatter"},
		{domain: 42, ros: "/ns/topic", want: "weld/d42/ns/topic"},
		{domain: 7, ros: "chatter", want: "weld/d7/chatter"},
	}

	for _, tc := range cases {
		got, err := MapTopic(tc.domain, tc.ros)
		if err != nil {
			t.Fatalf("map %q: %v", tc.ros, err)
		}

		if got != tc.want {
			t.Fatalf("map %q domain %d = %q, want %q", tc.ros, tc.domain, got, tc.want)
		}

		domain, ros, err := ParseTopic(got)
		if err != nil {
			t.Fatalf("parse %q: %v", got, err)
		}

		if domain != tc.domain {
			t.Fatalf("parsed domain = %d, want %d", domain, tc.domain)
		}

		stripped := strings.TrimPrefix(tc.ros, "/")
		if ros != "/"+stripped {
			t.Fatalf("parsed ros = %q, want %q", ros, "/"+stripped)
		}
	}
}

// TestMapTopicRejectsBad checks empty and wildcard topics fail.
func TestMapTopicRejectsBad(t *testing.T) {
	t.Parallel()

	for _, ros := range []string{"", "   ", "/", "/a/+/c", "/a/#/c", "/a b"} {
		_, err := MapTopic(0, ros)
		if err == nil {
			t.Fatalf("map %q succeeded, want error", ros)
		}
	}

	_, err := MapTopic(transports.MaxDomainID+1, "/chatter")
	if err == nil {
		t.Fatalf("map with bad domain succeeded, want error")
	}
}

// TestMapTopicWithTypeRoundTrip checks topic plus type mapping.
func TestMapTopicWithTypeRoundTrip(t *testing.T) {
	t.Parallel()

	got, err := MapTopicWithType(0, "/chatter", "std_msgs__msg__String")
	if err != nil {
		t.Fatalf("map with type: %v", err)
	}

	want := "weld/d0/chatter/std_msgs__msg__String"
	if got != want {
		t.Fatalf("mapped = %q, want %q", got, want)
	}

	domain, ros, typeName, err := ParseTopicWithType(got)
	if err != nil {
		t.Fatalf("parse with type: %v", err)
	}

	if domain != 0 {
		t.Fatalf("domain = %d, want 0", domain)
	}

	if ros != "/chatter" {
		t.Fatalf("ros = %q, want /chatter", ros)
	}

	if typeName != "std_msgs__msg__String" {
		t.Fatalf("type = %q, want std_msgs__msg__String", typeName)
	}

	_, _, _, err = ParseTopicWithType("weld/d0/chatter")
	if err == nil {
		t.Fatalf("parse without type suffix succeeded, want error")
	}

	for _, bad := range []string{"", "a/b", "a//b"} {
		_, err := MapTopicWithType(0, "/chatter", bad)
		if bad == "" || strings.Contains(bad, "/") {
			if err == nil {
				t.Fatalf("map type %q succeeded, want error", bad)
			}
		}
	}
}

// TestParseTopicRejectsBad checks malformed MQTT topics fail.
func TestParseTopicRejectsBad(t *testing.T) {
	t.Parallel()

	for _, raw := range []string{"", "chatter", "weld/d/chatter", "weld/d999/chatter", "weld/d0/", "weld/d0/a+b"} {
		_, _, err := ParseTopic(raw)
		if err == nil {
			t.Fatalf("parse %q succeeded, want error", raw)
		}
	}
}

// TestQoSMappingTable pins the reliability to MQTT QoS table.
//
//nolint:cyclop // mapping test sequences forward, valid, and reverse checks in one run, splitting would obscure intent
func TestQoSMappingTable(t *testing.T) {
	t.Parallel()

	if got := MQTTQoSForReliability(true); got != QoSAtLeastOnce {
		t.Fatalf("reliable QoS = %v, want at_least_once", got)
	}

	if got := MQTTQoSForReliability(false); got != QoSAtMostOnce {
		t.Fatalf("best effort QoS = %v, want at_most_once", got)
	}

	if !ValidQoS(QoSAtMostOnce) || !ValidQoS(QoSAtLeastOnce) || !ValidQoS(QoSExactlyOnce) {
		t.Fatalf("valid QoS levels report invalid")
	}

	if ValidQoS(QoS(3)) {
		t.Fatalf("QoS 3 reports valid")
	}

	reliable, err := ReliabilityForMQTTQoS(QoSAtMostOnce)
	if err != nil {
		t.Fatalf("reverse QoS 0: %v", err)
	}

	if reliable {
		t.Fatalf("QoS 0 maps to reliable, want best effort")
	}

	reliable, err = ReliabilityForMQTTQoS(QoSAtLeastOnce)
	if err != nil {
		t.Fatalf("reverse QoS 1: %v", err)
	}

	if !reliable {
		t.Fatalf("QoS 1 maps to best effort, want reliable")
	}

	reliable, err = ReliabilityForMQTTQoS(QoSExactlyOnce)
	if err != nil {
		t.Fatalf("reverse QoS 2: %v", err)
	}

	if !reliable {
		t.Fatalf("QoS 2 maps to best effort, want reliable")
	}

	_, err = ReliabilityForMQTTQoS(QoS(9))
	if err == nil {
		t.Fatalf("reverse QoS 9 succeeded, want error")
	}
}

// TestDurabilityRetained checks transient local retains and volatile does not.
func TestDurabilityRetained(t *testing.T) {
	t.Parallel()

	if !DurabilityTransientLocal.Retained() {
		t.Fatalf("transient local retained is false, want true")
	}

	if DurabilityVolatile.Retained() {
		t.Fatalf("volatile retained is true, want false")
	}

	if Durability(99).Retained() {
		t.Fatalf("unknown durability retains, want false")
	}
}

// TestLivelinessTopicIsolation checks liveliness topics carry domain prefix.
func TestLivelinessTopicIsolation(t *testing.T) {
	t.Parallel()

	first, err := LivelinessTopic(0, "talker")
	if err != nil {
		t.Fatalf("liveliness 0: %v", err)
	}

	second, err := LivelinessTopic(1, "talker")
	if err != nil {
		t.Fatalf("liveliness 1: %v", err)
	}

	if first == second {
		t.Fatalf("liveliness topics equal %q across domains", first)
	}

	if first != "weld/d0/liveliness/talker" {
		t.Fatalf("liveliness 0 = %q, want weld/d0/liveliness/talker", first)
	}

	for _, bad := range []string{"", "  ", "a/b", "a+b", "a#b"} {
		_, err := LivelinessTopic(0, bad)
		if err == nil {
			t.Fatalf("liveliness %q succeeded, want error", bad)
		}
	}
}

// TestBuildLivelinessWill checks will topic, QoS, retention, and payload.
func TestBuildLivelinessWill(t *testing.T) {
	t.Parallel()

	will, err := BuildLivelinessWill(3, "talker")
	if err != nil {
		t.Fatalf("build will: %v", err)
	}

	if will.Topic != "weld/d3/liveliness/talker" {
		t.Fatalf("will topic = %q, want weld/d3/liveliness/talker", will.Topic)
	}

	if will.QoS != QoSAtLeastOnce {
		t.Fatalf("will QoS = %v, want at_least_once", will.QoS)
	}

	if !will.Retained {
		t.Fatalf("will retained is false, want true")
	}

	if string(will.Payload) != "offline" {
		t.Fatalf("will payload = %q, want offline", string(will.Payload))
	}

	_, err = BuildLivelinessWill(0, "")
	if err == nil {
		t.Fatalf("build will with empty participant succeeded, want error")
	}

	_, err = BuildLivelinessWill(transports.MaxDomainID+1, "talker")
	if err == nil {
		t.Fatalf("build will with bad domain succeeded, want error")
	}
}
