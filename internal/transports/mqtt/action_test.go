// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

package mqtt_test

import (
	"context"
	"testing"

	"github.com/podomy/weld/internal/transports"
	"github.com/podomy/weld/internal/transports/mqtt"
)

// TestActionNames checks exchange names match the DDS backend shape.
func TestActionNames(t *testing.T) {
	t.Parallel()

	cases := []struct {
		action   string
		exchange string
		fn       func(string) (string, error)
		want     string
	}{
		{"/fibonacci", "send_goal", mqtt.SendGoalServiceName, "/fibonacci/_action/send_goal"},
		{"/fibonacci", "cancel_goal", mqtt.CancelGoalServiceName, "/fibonacci/_action/cancel_goal"},
		{"/fibonacci", "get_result", mqtt.GetResultServiceName, "/fibonacci/_action/get_result"},
		{"/fibonacci", "feedback", mqtt.FeedbackTopicName, "/fibonacci/_action/feedback"},
		{"/fibonacci", "status", mqtt.StatusTopicName, "/fibonacci/_action/status"},
	}

	for _, tc := range cases {
		got, err := tc.fn(tc.action)
		if err != nil {
			t.Fatalf("%s: %v", tc.exchange, err)
		}

		if got != tc.want {
			t.Fatalf("%s = %q, want %q", tc.exchange, got, tc.want)
		}
	}
}

// TestActionNamesRejectBadInput checks malformed bases fail.
func TestActionNamesRejectBadInput(t *testing.T) {
	t.Parallel()

	fns := []func(string) (string, error){
		mqtt.SendGoalServiceName,
		mqtt.CancelGoalServiceName,
		mqtt.GetResultServiceName,
		mqtt.FeedbackTopicName,
		mqtt.StatusTopicName,
	}

	bad := []string{"", "fibonacci", "/", "/a//b", "/a\tb", "/a/"}

	for _, fn := range fns {
		for _, name := range bad {
			_, err := fn(name)
			if err == nil {
				t.Fatalf("action %q succeeded, want error", name)
			}
		}
	}
}

// TestActionServiceRoundTrip checks a send goal flows over rq and rr.
func TestActionServiceRoundTrip(t *testing.T) {
	t.Parallel()

	backend := newActionBackend(t)
	ctx := context.Background()

	sendGoal, err := mqtt.SendGoalServiceName("/fibonacci")
	if err != nil {
		t.Fatalf("send goal name: %v", err)
	}

	err = backend.PublishService(ctx, sendGoal, "example_interfaces__action__Fibonacci_SendGoal", mqtt.ServiceRoleClient, []byte{0x0A}, "RIHS01_action")
	if err != nil {
		t.Fatalf("publish send goal: %v", err)
	}

	taken, err := backend.TakeService(ctx, mqtt.ServiceRoleServer)
	if err != nil {
		t.Fatalf("take send goal: %v", err)
	}

	if taken.Service != "/fibonacci/_action/send_goal" || !taken.IsRequest {
		t.Fatalf("send goal = %+v, want request", taken)
	}

	checkActionFeedback(t, ctx, backend)
}

// newActionBackend returns a joined backend for action tests.
func newActionBackend(t *testing.T) *mqtt.Backend {
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

	err = backend.JoinDiscovery(context.Background(), transports.DiscoveryInfo{ParticipantName: "action", DomainID: 0})
	if err != nil {
		t.Fatalf("join: %v", err)
	}

	return backend
}

// checkActionFeedback checks one feedback publish plus take.
func checkActionFeedback(t *testing.T, ctx context.Context, backend *mqtt.Backend) {
	t.Helper()

	feedback, err := mqtt.FeedbackTopicName("/fibonacci")
	if err != nil {
		t.Fatalf("feedback name: %v", err)
	}

	err = backend.PublishBytes(ctx, transports.OutgoingMessage{TopicName: feedback, TypeName: "example_interfaces__action__Fibonacci_FeedbackMessage", Payload: []byte{0x0B}, Leg: transports.LegData})
	if err != nil {
		t.Fatalf("publish feedback: %v", err)
	}

	msg, err := backend.TakeBytes(ctx)
	if err != nil {
		t.Fatalf("take feedback: %v", err)
	}

	if msg.TopicName != "/fibonacci/_action/feedback" || msg.Leg != transports.LegData {
		t.Fatalf("feedback = %+v, want data leg", msg)
	}
}
