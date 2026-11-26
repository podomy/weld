// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

package dds

import (
	"context"
	"testing"
	"time"
)

// TestListenerPollerBacksOff pins bounded retries on persistent errors.
func TestListenerPollerBacksOff(t *testing.T) {
	t.Parallel()

	recv := NewDataReceiver()

	fail := func(ctx context.Context) (SampledMessage, error) {
		select {
		case <-ctx.Done():
			return SampledMessage{}, ctx.Err()
		default:
		}

		return SampledMessage{}, context.DeadlineExceeded
	}

	ctx := t.Context()

	var poller ListenerPoller
	err := poller.Start(ctx, recv, "/backoff", fail)
	if err != nil {
		t.Fatalf("poller start: %v", err)
	}

	time.Sleep(60 * time.Millisecond)
	poller.Stop()

	stats := poller.Stats()
	if stats.Errors == 0 {
		t.Fatalf("failures = 0, want persistent error count")
	}

	if stats.Errors > 20 {
		t.Fatalf("failures = %d, want bounded backoff under 20 in 60ms", stats.Errors)
	}

	if stats.Running {
		t.Fatalf("poller still running after stop")
	}
}

// TestListenerPanicLogsIsolated pins that a failing listener cannot break others.
func TestListenerPanicLogsIsolated(t *testing.T) {
	t.Parallel()

	recv := NewDataReceiver()
	delivered := make(chan struct{}, 1)

	recv.AddListener("/panic", &DataListener{
		OnData: func(sample SampledMessage) {
			panic("edge boom")
		},
	})
	recv.AddListener("/panic", &DataListener{
		OnData: func(sample SampledMessage) {
			delivered <- struct{}{}
		},
	})

	recv.NotifyData("/panic", SampledMessage{})

	select {
	case <-delivered:
	case <-time.After(2 * time.Second):
		t.Fatalf("second listener missed after first panicked")
	}
}
