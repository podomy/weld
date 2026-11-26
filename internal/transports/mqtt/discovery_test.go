// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

package mqtt_test

import (
	"context"
	"testing"

	"github.com/podomy/weld/internal/transports"
	"github.com/podomy/weld/internal/transports/mqtt"
)

// TestDiscoveryTopicRoundTrip checks announce topics survive the trip.
func TestDiscoveryTopicRoundTrip(t *testing.T) {
	t.Parallel()

	rosTopics := []string{"/chatter", "/a/b", "/"}
	kinds := []mqtt.Kind{mqtt.KindData, mqtt.KindRequest, mqtt.KindReply}

	for _, kind := range kinds {
		for _, rosTopic := range rosTopics {
			checkDiscoveryRoundTrip(t, kind, rosTopic)
		}
	}
}

// checkDiscoveryRoundTrip checks one announce topic round trip.
func checkDiscoveryRoundTrip(t *testing.T, kind mqtt.Kind, rosTopic string) {
	t.Helper()

	wire, err := mqtt.EncodeDiscoveryTopic("weld", 42, "talker", kind, rosTopic)
	if err != nil {
		t.Fatalf("encode %s %s: %v", string(kind), rosTopic, err)
	}

	domain, participant, gotKind, gotROS, err := mqtt.DecodeDiscoveryTopic("weld", wire)
	if err != nil {
		t.Fatalf("decode %q: %v", wire, err)
	}

	if domain != 42 {
		t.Fatalf("domain = %d, want 42", domain)
	}

	if participant != "talker" {
		t.Fatalf("participant = %q, want talker", participant)
	}

	if gotKind != kind {
		t.Fatalf("kind = %q, want %q", string(gotKind), string(kind))
	}

	if gotROS != rosTopic {
		t.Fatalf("ros = %q, want %q", gotROS, rosTopic)
	}
}

// TestDiscoveryTopicRejectsBadInput checks malformed topics fail.
func TestDiscoveryTopicRejectsBadInput(t *testing.T) {
	t.Parallel()

	encodes := []struct {
		name        string
		participant string
		kind        mqtt.Kind
		ros         string
	}{
		{"slash participant", "a/b", mqtt.KindData, "/chatter"},
		{"sdp data kind", "talker", mqtt.KindDiscovery, "/chatter"},
	}

	for _, tc := range encodes {
		_, err := mqtt.EncodeDiscoveryTopic("weld", 0, tc.participant, tc.kind, tc.ros)
		if err == nil {
			t.Fatalf("%s succeeded, want error", tc.name)
		}
	}

	_, _, _, _, err := mqtt.DecodeDiscoveryTopic("weld", "weld/0/rt/chatter")
	if err == nil {
		t.Fatalf("data topic as discovery succeeded, want error")
	}
}

// newAnnounceBackend returns a joined backend for discovery tests.
func newAnnounceBackend(t *testing.T, ctx context.Context) *mqtt.Backend {
	t.Helper()

	backend, err := mqtt.New(mqtt.MqttConfig{TopicPrefix: "weld", QoS: 0, DomainID: 0})
	if err != nil {
		t.Fatalf("new backend: %v", err)
	}

	t.Cleanup(func() {
		if cerr := backend.Close(); cerr != nil {
			t.Logf("backend close: %v", cerr)
		}
	})

	err = backend.JoinDiscovery(ctx, transports.DiscoveryInfo{ParticipantName: "p", DomainID: 0})
	if err != nil {
		t.Fatalf("join: %v", err)
	}

	return backend
}

// announceChatter announces one endpoint or fails the test.
func announceChatter(t *testing.T, ctx context.Context, backend *mqtt.Backend, hash string) {
	t.Helper()

	err := backend.AnnounceEndpoint(ctx, "talker", mqtt.KindData, "/chatter", "std_msgs__msg__String", hash)
	if err != nil {
		t.Fatalf("announce: %v", err)
	}
}

// TestAnnounceFeedsHasRemoteEndpoint checks announce plus retract flow.
func TestAnnounceFeedsHasRemoteEndpoint(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	backend := newAnnounceBackend(t, ctx)

	if backend.HasRemoteEndpoint("/chatter") {
		t.Fatalf("endpoint present before announce")
	}

	announceChatter(t, ctx, backend, "RIHS01_abc")

	if !backend.HasRemoteEndpoint("/chatter") {
		t.Fatalf("endpoint missing after announce")
	}

	if backend.HasRemoteEndpoint("/other") {
		t.Fatalf("unexpected endpoint /other present")
	}

	if got := backend.DiscoveryGeneration(); got != 1 {
		t.Fatalf("generation = %d, want 1", got)
	}

	err := backend.RetractEndpoint(ctx, "talker", mqtt.KindData, "/chatter")
	if err != nil {
		t.Fatalf("retract: %v", err)
	}

	if backend.HasRemoteEndpoint("/chatter") {
		t.Fatalf("endpoint present after retract")
	}
}

// TestDiscoverySkipsTakePath checks announcements never surface as data.
func TestDiscoverySkipsTakePath(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	backend := newAnnounceBackend(t, ctx)

	announceChatter(t, ctx, backend, "")

	err := backend.PublishBytes(ctx, transports.OutgoingMessage{TopicName: "/chatter", TypeName: "std_msgs__msg__String", Payload: []byte{1}})
	if err != nil {
		t.Fatalf("publish: %v", err)
	}

	msg, err := backend.TakeBytes(ctx)
	if err != nil {
		t.Fatalf("take: %v", err)
	}

	if msg.TopicName != "/chatter" || len(msg.Payload) != 1 {
		t.Fatalf("take = %+v, want /chatter with 1 byte", msg)
	}
}
