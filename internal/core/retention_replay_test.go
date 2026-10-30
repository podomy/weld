// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package core

import (
	"testing"
)

// TestRetentionReplay pins bounded retention replay for late joiners,
// per outage deadline alerts, and backward rebase without unmatch.
func TestRetentionReplay(t *testing.T) {
	t.Run("keep last depths", checkReplayKeepLastDepths)
	t.Run("keep all bound", checkReplayKeepAllBound)
	t.Run("late joiner exact", checkReplayLateJoinerExact)
	t.Run("drop oldest lost", checkReplayDropOldestLost)
	t.Run("deadline per outage", checkReplayDeadlinePerOutage)
	t.Run("deadline ratios", checkReplayDeadlineRatios)
	t.Run("backward rebase", checkReplayBackwardRebase)
	t.Run("facade bounds", checkReplayFacadeBounds)
	t.Run("clear preserves lost", checkReplayClearPreservesLost)
}

// checkReplayKeepLastDepths pins transient keep last 1, 10, 100 forward.
//
//nolint:gocognit // replay test sequences capacity, push, snapshot, and loss checks in one run, splitting would obscure intent
func checkReplayKeepLastDepths(t *testing.T) {
	t.Helper()
	for _, depth := range []uint64{1, 10, 100} {
		profile := QoSProfile{
			History:     HistoryKeepLast,
			Depth:       depth,
			Reliability: ReliabilityReliable,
			Durability:  DurabilityTransientLocal,
			Liveliness:  LivelinessAutomatic,
		}
		capacity, err := RetentionCapacity(profile)
		if err != nil {
			t.Fatalf("depth %d capacity: %v", depth, err)
		}
		if capacity != depth {
			t.Fatalf("depth %d capacity = %d, want %d", depth, capacity, depth)
		}
		cache := NewHistoryCache[int](capacity)
		total := int(depth) + 5 //nolint:gosec // test depth 1,10,100 fits int
		for i := range total {
			cache.Push(i)
		}
		if got := cache.Len(); got != int(depth) { //nolint:gosec // test depth 1,10,100 fits int
			t.Fatalf("depth %d len = %d, want %d", depth, got, depth)
		}
		if got, want := cache.Lost(), uint64(5); got != want {
			t.Fatalf("depth %d lost = %d, want %d", depth, got, want)
		}
		snap := cache.Snapshot()
		if len(snap) != int(depth) { //nolint:gosec // test depth 1,10,100 fits int
			t.Fatalf("depth %d snapshot len = %d, want %d", depth, len(snap), depth)
		}
		for i, v := range snap {
			want := total - int(depth) + i //nolint:gosec // test depth 1,10,100 fits int
			if v != want {
				t.Fatalf("depth %d snapshot[%d] = %d, want %d", depth, i, v, want)
			}
		}
	}
}

// checkReplayKeepAllBound pins keep all forwards with the explicit bound.
func checkReplayKeepAllBound(t *testing.T) {
	t.Helper()
	profile := QoSProfile{
		History:     HistoryKeepAll,
		Reliability: ReliabilityReliable,
		Durability:  DurabilityTransientLocal,
		Liveliness:  LivelinessAutomatic,
	}
	capacity, err := RetentionCapacity(profile)
	if err != nil {
		t.Fatalf("keep all capacity: %v", err)
	}
	if capacity != KeepAllRetentionBound {
		t.Fatalf("keep all capacity = %d, want %d", capacity, KeepAllRetentionBound)
	}
	cache := NewHistoryCache[int](capacity)
	for i := range int(capacity) { //nolint:gosec // keep-all bound 1000 fits int
		if evicted := cache.Push(i); evicted {
			t.Fatalf("push %d must not evict before bound", i)
		}
	}
	if evicted := cache.Push(int(capacity)); !evicted { //nolint:gosec // keep-all bound 1000 fits int
		t.Fatal("push past keep all bound must evict")
	}
	if got := cache.Lost(); got != 1 {
		t.Fatalf("keep all lost = %d, want 1", got)
	}
	if got := cache.Len(); got != int(capacity) { //nolint:gosec // keep-all bound 1000 fits int
		t.Fatalf("keep all len = %d, want %d", got, capacity)
	}
}

// checkReplayLateJoinerExact pins a late joiner reads exactly retained history.
func checkReplayLateJoinerExact(t *testing.T) {
	t.Helper()
	capacity, err := RetentionCapacity(ResolveSystemDefaults(DefaultQoS()))
	if err != nil {
		t.Fatalf("capacity: %v", err)
	}
	cache := NewHistoryCache[int](capacity)
	published := int(capacity) + 20 //nolint:gosec // default depth 10 plus 20 fits int
	for i := range published {
		cache.Push(i)
	}
	history := cache.Snapshot()
	if len(history) != int(capacity) { //nolint:gosec // default depth 10 fits int
		t.Fatalf("late joiner history len = %d, want %d", len(history), capacity)
	}
	for i, v := range history {
		want := published - int(capacity) + i //nolint:gosec // default depth 10 fits int
		if v != want {
			t.Fatalf("late joiner history[%d] = %d, want %d", i, v, want)
		}
	}
	if got, want := cache.Lost(), uint64(published)-capacity; got != want {
		t.Fatalf("lost = %d, want %d", got, want)
	}
}

// checkReplayDropOldestLost pins each overflow drops oldest and counts one.
func checkReplayDropOldestLost(t *testing.T) {
	t.Helper()
	cache := NewHistoryCache[int](3)
	for _, v := range []int{1, 2, 3} {
		if evicted := cache.Push(v); evicted {
			t.Fatalf("push %d must not evict", v)
		}
	}
	for i, v := range []int{4, 5, 6, 7} {
		if evicted := cache.Push(v); !evicted {
			t.Fatalf("push %d must evict", v)
		}
		if got, want := cache.Lost(), uint64(i+1); got != want {
			t.Fatalf("lost after %d = %d, want %d", v, got, want)
		}
	}
	snap := cache.Snapshot()
	for i, want := range []int{5, 6, 7} {
		if snap[i] != want {
			t.Fatalf("retained[%d] = %d, want %d", i, snap[i], want)
		}
	}
}

// checkReplayDeadlinePerOutage pins one alert per outage, not per sample.
func checkReplayDeadlinePerOutage(t *testing.T) {
	t.Helper()
	tracker := NewDeadlineTracker("/scan", "listener", NewDuration(0, 100000000), 0)
	if _, ok := tracker.Check(50000000); ok {
		t.Fatal("fresh window must not alert")
	}
	if _, ok := tracker.Check(100000000); !ok {
		t.Fatal("first lapse must alert")
	}
	if _, ok := tracker.Check(150000000); ok {
		t.Fatal("second check in same outage must stay quiet")
	}
	if _, ok := tracker.Check(500000000); ok {
		t.Fatal("third check in same outage must stay quiet")
	}
	tracker.MarkSeen(500000000)
	if _, ok := tracker.Check(550000000); ok {
		t.Fatal("fresh window after seen must not alert")
	}
	if _, ok := tracker.Check(600000000); !ok {
		t.Fatal("next outage must alert once")
	}
	if _, ok := tracker.Check(650000000); ok {
		t.Fatal("repeat in second outage must stay quiet")
	}
}

// checkReplayDeadlineRatios pins one alert per spell across Tbad over P.
//
//nolint:gocognit // deadline test sequences ratio sweep, outage, and alert checks in one run, splitting would obscure intent
func checkReplayDeadlineRatios(t *testing.T) {
	t.Helper()
	period := int64(100000000)
	step := int64(20000000)
	base := int64(1000000000)
	for _, ratio := range []float64{0.2, 1, 5, 20} {
		bad := int64(float64(period) * ratio)
		tracker := NewDeadlineTracker("/scan", "listener", NewDuration(0, 100000000), base)
		count := int(bad / step)
		count = max(count, 1)
		if bad < step {
			count = 1
		}
		alerts := 0
		for i := 1; i <= count; i++ {
			now := base + int64(i)*step
			now = min(now, base+bad)
			if _, ok := tracker.Check(now); ok {
				alerts++
			}
			if now == base+bad {
				break
			}
		}
		if ratio < 1 {
			if alerts != 0 {
				t.Fatalf("ratio %v below period must not alert, got %d", ratio, alerts)
			}
			continue
		}
		if alerts != 1 {
			t.Fatalf("ratio %v must yield one alert per spell, got %d", ratio, alerts)
		}
		tracker.MarkSeen(base + bad + period)
		if _, ok := tracker.Check(base + bad + period + period); !ok {
			t.Fatalf("ratio %v next spell must alert after clear", ratio)
		}
	}
}

// checkReplayBackwardRebase pins backward jumps rebase without unmatch.
func checkReplayBackwardRebase(t *testing.T) {
	t.Helper()
	tracker := NewDeadlineTracker("/scan", "listener", NewDuration(0, 100000000), 200000000)
	if _, ok := tracker.Check(100000000); ok {
		t.Fatal("backward jump must not alert")
	}
	if _, ok := tracker.Check(150000000); ok {
		t.Fatal("rebased window must not alert early")
	}
	if _, ok := tracker.Check(200000000); !ok {
		t.Fatal("rebased window must alert after a full deadline")
	}
	if _, ok := tracker.Check(250000000); ok {
		t.Fatal("repeat after rebase alert must stay quiet")
	}
	jump := ClockJump{FromNanos: 250000000, ToNanos: 100000000, Backward: true, SimActive: true}
	RebaseDeadlinesOnJump(jump, &tracker)
	if _, ok := tracker.Check(150000000); ok {
		t.Fatal("helper rebased window must not alert early")
	}
	if _, ok := tracker.Check(200000000); !ok {
		t.Fatal("helper rebased window must alert after a full deadline")
	}
}

// checkReplayFacadeBounds pins the wire bound semantics stay intact.
func checkReplayFacadeBounds(t *testing.T) {
	t.Helper()
	if FacadeHistoryDepth != 100 {
		t.Fatalf("facade depth = %d, want 100", FacadeHistoryDepth)
	}
	if KeepAllRetentionBound != 1000 {
		t.Fatalf("keep all bound = %d, want 1000", KeepAllRetentionBound)
	}
	got := FacadeAdvertisedQoS()
	if got.History != HistoryKeepLast || got.Depth != FacadeHistoryDepth {
		t.Fatalf("facade must advertise bounded keep last, got %+v", got)
	}
}

// checkReplayClearPreservesLost pins Clear keeps drop history across jumps.
func checkReplayClearPreservesLost(t *testing.T) {
	t.Helper()
	cache := NewHistoryCache[int](2)
	cache.Push(1)
	cache.Push(2)
	cache.Push(3)
	if got := cache.Lost(); got != 1 {
		t.Fatalf("lost before clear = %d, want 1", got)
	}
	cache.Clear()
	if got := cache.Len(); got != 0 {
		t.Fatalf("len after clear = %d, want 0", got)
	}
	if got := cache.Lost(); got != 1 {
		t.Fatalf("lost after clear = %d, want 1", got)
	}
	if snap := cache.Snapshot(); snap == nil || len(snap) != 0 {
		t.Fatalf("cleared snapshot must be empty non nil")
	}
	cache.Push(4)
	cache.Push(5)
	snap := cache.Snapshot()
	if len(snap) != 2 || snap[0] != 4 || snap[1] != 5 {
		t.Fatalf("snapshot after jump = %v, want [4 5]", snap)
	}
}
