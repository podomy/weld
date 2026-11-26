// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

package mqtt_test

import (
	"context"
	"testing"

	"github.com/podomy/weld/internal/transports"
	"github.com/podomy/weld/internal/transports/mqtt"
)

// newServiceBackend returns a joined backend for service tests.
func newServiceBackend(t *testing.T) *mqtt.Backend {
	t.Helper()

	backend, err := mqtt.New(mqtt.MqttConfig{TopicPrefix: "weld", QoS: 0, DomainID: 0})
	if err != nil {
		t.Fatalf("new backend: %v", err)
	}

	err = backend.JoinDiscovery(context.Background(), transports.DiscoveryInfo{ParticipantName: "svc", DomainID: 0})
	if err != nil {
		t.Fatalf("join: %v", err)
	}

	return backend
}

// closeBackendOnCleanup closes the backend and logs failures.
func closeBackendOnCleanup(t *testing.T, backend *mqtt.Backend) {
	t.Helper()

	t.Cleanup(func() {
		if cerr := backend.Close(); cerr != nil {
			t.Logf("backend close: %v", cerr)
		}
	})
}

// TestServiceRequestReplyRoundTrip checks client to server and back.
func TestServiceRequestReplyRoundTrip(t *testing.T) {
	t.Parallel()

	backend := newServiceBackend(t)
	closeBackendOnCleanup(t, backend)

	ctx := context.Background()
	request := []byte{0x01, 0x02}

	err := backend.PublishService(ctx, "/add_two_ints", "my_pkg__srv__AddTwoInts", mqtt.ServiceRoleClient, request, "RIHS01_abc")
	if err != nil {
		t.Fatalf("publish request: %v", err)
	}

	taken, err := backend.TakeService(ctx, mqtt.ServiceRoleServer)
	if err != nil {
		t.Fatalf("take request: %v", err)
	}

	assertServiceRequest(t, taken, "/add_two_ints", "RIHS01_abc")

	reply := []byte{0x03}

	err = backend.PublishService(ctx, "/add_two_ints", "my_pkg__srv__AddTwoInts", mqtt.ServiceRoleServer, reply, "")
	if err != nil {
		t.Fatalf("publish reply: %v", err)
	}

	got, err := backend.TakeService(ctx, mqtt.ServiceRoleClient)
	if err != nil {
		t.Fatalf("take reply: %v", err)
	}

	assertServiceReply(t, got)
}

// assertServiceRequest checks one taken request.
func assertServiceRequest(t *testing.T, taken mqtt.ServiceMessage, service, hash string) {
	t.Helper()

	if taken.Service != service {
		t.Fatalf("service = %q, want %s", taken.Service, service)
	}

	if !taken.IsRequest {
		t.Fatalf("request flag is false, want true")
	}

	if taken.TypeHash != hash {
		t.Fatalf("hash = %q, want %s", taken.TypeHash, hash)
	}
}

// assertServiceReply checks one taken reply.
func assertServiceReply(t *testing.T, got mqtt.ServiceMessage) {
	t.Helper()

	if got.IsRequest {
		t.Fatalf("reply flag is request, want reply")
	}

	if len(got.Payload) != 1 || got.Payload[0] != 0x03 {
		t.Fatalf("reply payload = %v, want [3]", got.Payload)
	}
}

// TestServiceIsolatedFromData checks legs arrive with their leg set.
func TestServiceIsolatedFromData(t *testing.T) {
	t.Parallel()

	backend := newServiceBackend(t)
	closeBackendOnCleanup(t, backend)

	ctx := context.Background()

	err := backend.PublishBytes(ctx, transports.OutgoingMessage{TopicName: "/chatter", TypeName: "std_msgs__msg__String", Payload: []byte{0x09}})
	if err != nil {
		t.Fatalf("publish data: %v", err)
	}

	err = backend.PublishService(ctx, "/svc", "my_pkg__srv__Trigger", mqtt.ServiceRoleClient, []byte{0x07}, "")
	if err != nil {
		t.Fatalf("publish request: %v", err)
	}

	data, err := backend.TakeBytes(ctx)
	if err != nil {
		t.Fatalf("take data: %v", err)
	}

	assertTakeLeg(t, data, "/chatter", transports.LegData)

	svc, err := backend.TakeBytes(ctx)
	if err != nil {
		t.Fatalf("take service: %v", err)
	}

	assertTakeLeg(t, svc, "/svc", transports.LegRequest)
}

// assertTakeLeg checks one taken message names its topic and leg.
func assertTakeLeg(t *testing.T, msg transports.IncomingMessage, topic string, leg transports.Leg) {
	t.Helper()

	if msg.TopicName != topic || msg.Leg != leg {
		t.Fatalf("take = %+v, want %s %s leg", msg, topic, string(leg))
	}
}

// TestServiceRejectsUnknownRole checks bad roles fail loudly.
func TestServiceRejectsUnknownRole(t *testing.T) {
	t.Parallel()

	backend := newServiceBackend(t)
	closeBackendOnCleanup(t, backend)

	ctx := context.Background()
	bad := mqtt.ServiceRole(99)

	if err := backend.PublishService(ctx, "/svc", "t", bad, []byte{0x01}, ""); err == nil {
		t.Fatalf("publish with bad role succeeded, want error")
	}

	if _, err := backend.TakeService(ctx, bad); err == nil {
		t.Fatalf("take with bad role succeeded, want error")
	}
}
