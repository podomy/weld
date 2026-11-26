// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

package transports_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/podomy/weld/internal/transports"
	"github.com/podomy/weld/internal/transports/mqtt"
)

// newJoinedMQTT returns a joined in-memory MQTT backend for forward tests.
func newJoinedMQTT(t *testing.T) *mqtt.Backend {
	t.Helper()

	backend, err := mqtt.New(mqtt.MqttConfig{TopicPrefix: "weld", QoS: 0, DomainID: 0})
	if err != nil {
		t.Fatalf("new mqtt backend: %v", err)
	}

	err = backend.JoinDiscovery(context.Background(), transports.DiscoveryInfo{ParticipantName: "fwd", DomainID: 0})
	if err != nil {
		t.Fatalf("join: %v", err)
	}

	return backend
}

// TestForwardOncePreservesLeg checks request legs bridge unchanged.
func TestForwardOncePreservesLeg(t *testing.T) {
	t.Parallel()

	src := newJoinedMQTT(t)
	defer func() {
		err := src.Close()
		if err != nil {
			t.Errorf("close src: %v", err)
		}
	}()

	dst := newJoinedMQTT(t)
	defer func() {
		err := dst.Close()
		if err != nil {
			t.Errorf("close dst: %v", err)
		}
	}()

	err := src.PublishBytes(context.Background(), transports.OutgoingMessage{
		TopicName: "/svc",
		TypeName:  "my_pkg__srv__Trigger",
		Payload:   []byte{0x07},
		TypeHash:  "RIHS01_leg",
		Leg:       transports.LegRequest,
	})
	if err != nil {
		t.Fatalf("publish to src: %v", err)
	}

	_, err = transports.ForwardOnce(context.Background(), src, dst)
	if err != nil {
		t.Fatalf("forward once: %v", err)
	}

	delivered, err := dst.TakeBytes(context.Background())
	if err != nil {
		t.Fatalf("take from dst: %v", err)
	}

	if delivered.TopicName != "/svc" || delivered.Leg != transports.LegRequest {
		t.Fatalf("dst = %+v, want /svc request leg", delivered)
	}

	if delivered.TypeHash != "RIHS01_leg" {
		t.Fatalf("dst hash = %q, want RIHS01_leg", delivered.TypeHash)
	}
}

// TestForwardOnceDeliversDDSIncomingToMQTT checks take from src publishes to dst.
func TestForwardOnceDeliversDDSIncomingToMQTT(t *testing.T) {
	t.Parallel()

	src := newJoinedMQTT(t)
	defer func() {
		err := src.Close()
		if err != nil {
			t.Errorf("close src: %v", err)
		}
	}()

	dst := newJoinedMQTT(t)
	defer func() {
		err := dst.Close()
		if err != nil {
			t.Errorf("close dst: %v", err)
		}
	}()

	payload := []byte{0, 1, 2, 3}

	err := src.PublishBytes(context.Background(), transports.OutgoingMessage{
		TopicName: "/chatter",
		TypeName:  "std_msgs__msg__String",
		Payload:   payload,
	})
	if err != nil {
		t.Fatalf("publish to src: %v", err)
	}

	got, err := transports.ForwardOnce(context.Background(), src, dst)
	if err != nil {
		t.Fatalf("forward once: %v", err)
	}

	if got.TopicName != "/chatter" {
		t.Fatalf("forwarded topic = %q, want /chatter", got.TopicName)
	}

	delivered, err := dst.TakeBytes(context.Background())
	if err != nil {
		t.Fatalf("take from dst: %v", err)
	}

	if delivered.TopicName != "/chatter" {
		t.Fatalf("dst topic = %q, want /chatter", delivered.TopicName)
	}

	if delivered.TypeName != "std_msgs__msg__String" {
		t.Fatalf("dst type = %q, want std_msgs__msg__String", delivered.TypeName)
	}

	if len(delivered.Payload) != len(payload) {
		t.Fatalf("dst payload len = %d, want %d", len(delivered.Payload), len(payload))
	}
}

// TestForwardOnceContextCancel checks a canceled context reports ctx error.
func TestForwardOnceContextCancel(t *testing.T) {
	t.Parallel()

	src := newJoinedMQTT(t)
	defer func() {
		err := src.Close()
		if err != nil {
			t.Errorf("close src: %v", err)
		}
	}()

	dst := newJoinedMQTT(t)
	defer func() {
		err := dst.Close()
		if err != nil {
			t.Errorf("close dst: %v", err)
		}
	}()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := transports.ForwardOnce(ctx, src, dst)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("forward canceled = %v, want context.Canceled", err)
	}
}

// TestForwardOnceNilBackend checks nil legs fail loudly.
func TestForwardOnceNilBackend(t *testing.T) {
	t.Parallel()

	dst := newJoinedMQTT(t)
	defer func() {
		err := dst.Close()
		if err != nil {
			t.Errorf("close dst: %v", err)
		}
	}()

	_, err := transports.ForwardOnce(context.Background(), nil, dst)
	if err == nil {
		t.Fatalf("nil source succeeded, want error")
	}

	src := newJoinedMQTT(t)
	defer func() {
		err := src.Close()
		if err != nil {
			t.Errorf("close src: %v", err)
		}
	}()

	_, err = transports.ForwardOnce(context.Background(), src, nil)
	if err == nil {
		t.Fatalf("nil destination succeeded, want error")
	}
}

// publishLoopSamples publishes total ordered samples to src.
func publishLoopSamples(t *testing.T, src *mqtt.Backend, total int) {
	t.Helper()

	for i := range total {
		err := src.PublishBytes(context.Background(), transports.OutgoingMessage{
			TopicName: "/loop",
			TypeName:  "t",
			Payload:   []byte{byte(i)},
		})
		if err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}
}

// takeLoopSamples takes total ordered samples from dst.
func takeLoopSamples(t *testing.T, dst *mqtt.Backend, total int) {
	t.Helper()

	for i := range total {
		msg, err := dst.TakeBytes(context.Background())
		if err != nil {
			t.Fatalf("take %d: %v", i, err)
		}

		if len(msg.Payload) != 1 || msg.Payload[0] != byte(i) {
			t.Fatalf("take %d payload = %v, want [%d]", i, msg.Payload, i)
		}
	}
}

// awaitLoopDone fails unless the loop exits with context canceled.
func awaitLoopDone(t *testing.T, done <-chan error) {
	t.Helper()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("loop err = %v, want context canceled", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatalf("loop never returned after cancel")
	}
}

// TestForwardLoopRelaysUntilCancel proves the loop forwards every
// sample in order and exits with the caller context error.
func TestForwardLoopRelaysUntilCancel(t *testing.T) {
	t.Parallel()

	src := newJoinedMQTT(t)

	defer func() {
		if err := src.Close(); err != nil {
			t.Errorf("close src: %v", err)
		}
	}()

	dst := newJoinedMQTT(t)

	defer func() {
		if err := dst.Close(); err != nil {
			t.Errorf("close dst: %v", err)
		}
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)

	go func() {
		done <- transports.ForwardLoop(ctx, src, dst)
	}()

	const total = 3

	publishLoopSamples(t, src, total)
	takeLoopSamples(t, dst, total)
	cancel()
	awaitLoopDone(t, done)
}
