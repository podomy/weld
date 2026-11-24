// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

package rmwbridge_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/podomy/weld/internal/rmwbridge"
)

// reportLivelinessWithTimeout reports liveliness and wraps failures.
func reportLivelinessWithTimeout(t *testing.T, bridge *rmwbridge.Bridge, generation uint64, alive bool) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err := bridge.ReportLiveliness(ctx, rmwbridge.LivelinessReport{
		ParticipantName: "p1",
		DomainID:        0,
		Alive:           alive,
		Generation:      generation,
	})
	if err != nil {
		return fmt.Errorf("report liveliness gen %d: %w", generation, err)
	}
	return nil
}

// checkGuardWaitLifecycle pins guard wait, reset, and timeout paths.
func checkGuardWaitLifecycle(t *testing.T) {
	t.Helper()
	guard := rmwbridge.NewGuard()
	if guard.IsTriggered() {
		t.Fatalf("new guard is triggered")
	}
	guard.Trigger()
	guard.Trigger()
	if guard.IsTriggered() == false {
		t.Fatalf("triggered guard reports false")
	}
	ws := rmwbridge.NewWaitSet()
	ws.AddGuard(nil)
	ws.AddGuard(guard)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err := ws.Wait(ctx)
	requireNoError(t, err, "wait after trigger")
	guard.Reset()
	if guard.IsTriggered() {
		t.Fatalf("reset guard still triggered")
	}
	checkGuardWaitTimeout(t)
	checkGuardAsyncTrigger(t)
}

// checkGuardWaitTimeout pins wait timeout on reset and empty sets.
func checkGuardWaitTimeout(t *testing.T) {
	t.Helper()
	guard := rmwbridge.NewGuard()
	ws := rmwbridge.NewWaitSet()
	ws.AddGuard(guard)
	timeout, cancelTimeout := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancelTimeout()
	err := ws.Wait(timeout)
	requireTimeout(t, err, "wait after reset")
	empty := rmwbridge.NewWaitSet()
	timeout2, cancelTimeout2 := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancelTimeout2()
	err = empty.Wait(timeout2)
	requireTimeout(t, err, "empty wait")
}

// checkGuardAsyncTrigger pins async trigger delivery through a wait set.
func checkGuardAsyncTrigger(t *testing.T) {
	t.Helper()
	async := rmwbridge.NewGuard()
	ws2 := rmwbridge.NewWaitSet()
	ws2.AddGuard(async)
	go func() {
		time.Sleep(10 * time.Millisecond)
		async.Trigger()
	}()
	ctx2, cancel2 := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel2()
	err := ws2.Wait(ctx2)
	requireNoError(t, err, "wait after async trigger")
}

// checkLivelinessBeforeJoin pins report before join failure.
func checkLivelinessBeforeJoin(t *testing.T, bridge *rmwbridge.Bridge, ctx context.Context) {
	t.Helper()
	err := bridge.ReportLiveliness(ctx, rmwbridge.LivelinessReport{ParticipantName: "p", DomainID: 0, Alive: true, Generation: 1})
	requireErrorIs(t, err, rmwbridge.ErrNotJoined, "report before join")
}

// checkLivelinessGenerations pins generation watermark and stale handling.
func checkLivelinessGenerations(t *testing.T, bridge *rmwbridge.Bridge) {
	t.Helper()
	err := reportLivelinessWithTimeout(t, bridge, 1, true)
	requireNoError(t, err, "first report")
	err = reportLivelinessWithTimeout(t, bridge, 1, true)
	requireNoError(t, err, "idempotent repeat")
	err = reportLivelinessWithTimeout(t, bridge, 1, false)
	requireErrorIs(t, err, rmwbridge.ErrStaleGeneration, "flipped alive")
	err = reportLivelinessWithTimeout(t, bridge, 0, true)
	requireErrorIs(t, err, rmwbridge.ErrStaleGeneration, "backward")
	err = reportLivelinessWithTimeout(t, bridge, 2, true)
	requireNoError(t, err, "watermark advance")
}

// checkLivelinessWatermark pins stale after watermark and dispose advance.
func checkLivelinessWatermark(t *testing.T, bridge *rmwbridge.Bridge) {
	t.Helper()
	err := reportLivelinessWithTimeout(t, bridge, 2, true)
	requireNoError(t, err, "idempotent at new watermark")
	err = reportLivelinessWithTimeout(t, bridge, 1, true)
	requireErrorIs(t, err, rmwbridge.ErrStaleGeneration, "stale after watermark")
	err = reportLivelinessWithTimeout(t, bridge, 3, false)
	requireNoError(t, err, "advance with dispose")
	canceled, stop := context.WithCancel(context.Background())
	stop()
	err = bridge.ReportLiveliness(canceled, rmwbridge.LivelinessReport{ParticipantName: "p1", DomainID: 0, Alive: true, Generation: 4})
	requireError(t, err, "report with canceled context")
}

// TestConformanceLiveliness covers guard wait and liveliness.
func TestConformanceLiveliness(t *testing.T) {
	t.Run("guard wait", func(t *testing.T) {
		t.Parallel()
		checkGuardWaitLifecycle(t)
	})

	t.Run("liveliness", func(t *testing.T) {
		t.Parallel()
		bridge := newConformanceBridge(t)
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		checkLivelinessBeforeJoin(t, bridge, ctx)
		joinConformanceBridge(t, bridge, "p1", 0)
		checkLivelinessGenerations(t, bridge)
		checkLivelinessWatermark(t, bridge)
	})
}
