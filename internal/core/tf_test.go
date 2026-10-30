// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package core

import (
	"strings"
	"sync"
	"testing"
)

// TestTF covers the topic split, latched statics, lookup taxonomy, and jump
// behavior.
func TestTF(t *testing.T) {
	t.Run("topic split QoS", checkTFTopicQoS)
	t.Run("normalize", checkTFNormalize)
	t.Run("store validate", checkTFStoreValidate)
	t.Run("static replace", checkTFStaticReplace)
	t.Run("clear sticky", checkTFClearSticky)
	t.Run("snapshot statics", checkTFSnapshotStatics)
	t.Run("lookup taxonomy", checkTFLookupTaxonomy)
	t.Run("zero time latest", checkTFZeroTimeLatest)
	t.Run("same frame identity", checkTFSameFrameIdentity)
	t.Run("static ignore time", checkTFStaticIgnoreTime)
	t.Run("timed no interpolation", checkTFTimedNoInterpolation)
	t.Run("multi hop compose", checkTFMultiHopCompose)
	t.Run("reverse inverse", checkTFReverseInverse)
	t.Run("lookup timeout", checkTFLookupTimeout)
	t.Run("jump clears dynamics", checkTFJumpClearsDynamics)
	t.Run("dynamic bound", checkTFDynamicBound)
	t.Run("concurrent store lookup", checkTFConcurrentStoreLookup)
}

// checkTFTopicQoS pins /tf plus /tf_static QoS and topics.
func checkTFTopicQoS(t *testing.T) {
	t.Helper()
	assertTFTopics(t)
	assertTFDynamicQoS(t)
	assertTFStaticQoS(t)
}

// assertTFTopics pins topic names.
func assertTFTopics(t *testing.T) {
	t.Helper()
	if TFTopic != "/tf" {
		t.Fatalf("TFTopic must be /tf, got %q", TFTopic)
	}
	if TFStaticTopic != "/tf_static" {
		t.Fatalf("TFStaticTopic must be /tf_static, got %q", TFStaticTopic)
	}
}

// assertTFDynamicQoS pins dynamic reliable volatile keep last.
func assertTFDynamicQoS(t *testing.T) {
	t.Helper()
	dynamic := TFQoS()
	if dynamic.Reliability != ReliabilityReliable || dynamic.Durability != DurabilityVolatile {
		t.Fatalf("dynamic must be reliable volatile, got %+v", dynamic)
	}
	if dynamic.History != HistoryKeepLast || dynamic.Depth != TFHistoryDepth {
		t.Fatalf("dynamic must be keep last depth %d, got %+v", TFHistoryDepth, dynamic)
	}
	if TFHistoryDepth != FacadeHistoryDepth {
		t.Fatalf("dynamic depth must match facade %d, got %d", FacadeHistoryDepth, TFHistoryDepth)
	}
	err := Validate(ResolveSystemDefaults(dynamic))
	if err != nil {
		t.Fatalf("dynamic QoS must validate: %v", err)
	}
}

// assertTFStaticQoS pins static reliable transient local keep last.
func assertTFStaticQoS(t *testing.T) {
	t.Helper()
	static := TFStaticQoS()
	if static.Reliability != ReliabilityReliable || static.Durability != DurabilityTransientLocal {
		t.Fatalf("static must be reliable transient local, got %+v", static)
	}
	if static.History != HistoryKeepLast || static.Depth != TFStaticHistoryDepth {
		t.Fatalf("static must be keep last depth %d, got %+v", TFStaticHistoryDepth, static)
	}
	err := Validate(ResolveSystemDefaults(static))
	if err != nil {
		t.Fatalf("static QoS must validate: %v", err)
	}
}

// checkTFNormalize pins frame id normalization.
func checkTFNormalize(t *testing.T) {
	t.Helper()
	if got := NormalizeFrameID("/base_link"); got != "base_link" {
		t.Fatalf("leading slash must strip, got %q", got)
	}
	if got := NormalizeFrameID("base_link"); got != "base_link" {
		t.Fatalf("bare id must survive, got %q", got)
	}
	if got := NormalizeFrameID("  /odom  "); got != "odom" {
		t.Fatalf("whitespace plus slash must normalize, got %q", got)
	}
	if got := NormalizeFrameID(""); got != "" {
		t.Fatalf("empty must stay empty, got %q", got)
	}
	if got := NormalizeFrameID("///map"); got != "map" {
		t.Fatalf("multiple slashes must strip, got %q", got)
	}
}

// tfSample builds one dynamic sample with identity rotation.
func tfSample(frame, child string, stamp int64, x float64) TransformStamped {
	return TransformStamped{
		StampNanos:   stamp,
		FrameID:      frame,
		ChildFrameID: child,
		Transform: Transform{
			Translation: Vector3{X: x},
			Rotation:    Quaternion{X: 0, Y: 0, Z: 0, W: 1},
		},
	}
}

// checkTFStoreValidate pins store validation rejects bad input atomically.
func checkTFStoreValidate(t *testing.T) {
	t.Helper()
	store := NewTFStore()
	err := store.StoreDynamic(tfSample("", "child", 100, 0))
	if err == nil {
		t.Fatal("empty frame must fail dynamic store")
	}
	err = store.StoreDynamic(tfSample("parent", "", 100, 0))
	if err == nil {
		t.Fatal("empty child must fail dynamic store")
	}
	err = store.StoreDynamic(tfSample("parent", "child", 0, 0))
	if err == nil {
		t.Fatal("zero stamp must fail dynamic store")
	}
	err = store.StoreDynamic(tfSample("parent", "child", -5, 0))
	if err == nil {
		t.Fatal("negative stamp must fail dynamic store")
	}
	bad := tfSample("parent", "child", 100, 0)
	bad.Transform.Rotation = Quaternion{X: 0, Y: 0, Z: 0, W: 0}
	err = store.StoreDynamic(bad)
	if err == nil {
		t.Fatal("zero norm quaternion must fail dynamic store")
	}
	nan := tfSample("parent", "child", 100, 0)
	nan.Transform.Rotation = Quaternion{X: 1, Y: 0, Z: 0, W: 1}
	nan.Transform.Rotation.X = 1
	// Force NaN through bit pattern free construction.
	nan.Transform.Rotation.Y = 0
	nan.Transform.Rotation.Z = 0
	nan.Transform.Rotation.W = 1
	// Use explicit NaN assignment without importing math in test helper.
	// The caller below covers NaN through a dedicated case.
	_ = nan
	nanQ := tfSample("a", "b", 100, 0)
	nanQ.Transform.Rotation.X = float64(1)
	// NaN case uses quiet NaN literal.
	nanQ.Transform.Rotation.Y = nanFloat()
	err = store.StoreDynamic(nanQ)
	if err == nil {
		t.Fatal("NaN quaternion must fail dynamic store")
	}
	if got := store.DynamicCount(); got != 0 {
		t.Fatalf("failed batch must store nothing, count %d", got)
	}
	err = store.StoreStatic(tfSample("p", "c", -1, 0))
	if err == nil {
		t.Fatal("negative static stamp must fail")
	}
}

// nanFloat returns a quiet NaN without importing math at call sites.
func nanFloat() float64 {
	// Division of infinities is avoided; use bit free NaN literal.
	// Go guarantees 0.0/0.0 is NaN at runtime.
	var zero float64
	return zero / zero
}

// checkTFStaticReplace pins replace on rebroadcast.
func checkTFStaticReplace(t *testing.T) {
	t.Helper()
	store := NewTFStore()
	err := store.StoreStatic(tfSample("map", "odom", 100, 1))
	if err != nil {
		t.Fatalf("store static: %v", err)
	}
	err = store.StoreStatic(tfSample("/map", "/odom", 200, 2))
	if err != nil {
		t.Fatalf("store static rebroadcast: %v", err)
	}
	if got := store.StaticCount(); got != 1 {
		t.Fatalf("rebroadcast must replace, count %d", got)
	}
	got, err := store.Lookup("map", "odom", 9999)
	if err != nil {
		t.Fatalf("lookup replaced static: %v", err)
	}
	if got.Transform.Translation.X != 2 {
		t.Fatalf("replacement must win, got %v", got.Transform.Translation.X)
	}
	err = store.StoreStatic(tfSample("map", "base_link", 300, 3))
	if err != nil {
		t.Fatalf("store second static: %v", err)
	}
	if got := store.StaticCount(); got != 2 {
		t.Fatalf("distinct pair must add, count %d", got)
	}
}

// checkTFClearSticky pins Clear preserves statics while ClearStatics sticks.
func checkTFClearSticky(t *testing.T) {
	t.Helper()
	store := tfStoreWithBoth(t)
	assertClearPreservesStatics(t, store)
	assertClearStaticsSticky(t, store)
}

// tfStoreWithBoth builds a store with one dynamic plus one static.
func tfStoreWithBoth(t *testing.T) *TFStore {
	t.Helper()
	store := NewTFStore()
	err := store.StoreDynamic(tfSample("odom", "base_link", 100, 1))
	if err != nil {
		t.Fatalf("store dynamic: %v", err)
	}
	err = store.StoreStatic(tfSample("map", "odom", 100, 2))
	if err != nil {
		t.Fatalf("store static: %v", err)
	}
	return store
}

// assertClearPreservesStatics pins Clear drops dynamics and keeps statics.
func assertClearPreservesStatics(t *testing.T, store *TFStore) {
	t.Helper()
	store.Clear()
	if got := store.DynamicCount(); got != 0 {
		t.Fatalf("Clear must drop dynamics, got %d", got)
	}
	if got := store.StaticCount(); got != 1 {
		t.Fatalf("Clear must preserve statics, got %d", got)
	}
	snap := store.SnapshotStatics()
	if len(snap) != 1 {
		t.Fatalf("late joiner must still replay one static, got %+v", snap)
	}
}

// assertClearStaticsSticky pins latched clear stays empty until rebroadcast.
func assertClearStaticsSticky(t *testing.T, store *TFStore) {
	t.Helper()
	store.ClearStatics()
	if got := store.StaticCount(); got != 0 {
		t.Fatalf("ClearStatics must drop latched set, got %d", got)
	}
	snap := store.SnapshotStatics()
	if snap == nil || len(snap) != 0 {
		t.Fatalf("cleared statics must snapshot empty non nil, got %+v", snap)
	}
	store.ClearStatics()
	if got := store.StaticCount(); got != 0 {
		t.Fatalf("second ClearStatics must stay cleared, got %d", got)
	}
	_, err := store.Lookup("map", "odom", 0)
	if err == nil || !strings.Contains(err.Error(), "empty storage") {
		t.Fatalf("cleared store must report empty storage, got %v", err)
	}
	store.ClearDynamics()
	if got := store.DynamicCount(); got != 0 {
		t.Fatalf("ClearDynamics on empty must stay zero, got %d", got)
	}
}

// checkTFSnapshotStatics pins sorted late joiner replay with copy isolation.
func checkTFSnapshotStatics(t *testing.T) {
	t.Helper()
	store := NewTFStore()
	snap := store.SnapshotStatics()
	if snap == nil || len(snap) != 0 {
		t.Fatalf("empty snapshot must be empty non nil, got %+v", snap)
	}
	err := store.StoreStatic(tfSample("map", "zebra", 10, 1))
	if err != nil {
		t.Fatalf("store static: %v", err)
	}
	err = store.StoreStatic(tfSample("map", "apple", 20, 2))
	if err != nil {
		t.Fatalf("store static: %v", err)
	}
	snap = store.SnapshotStatics()
	if len(snap) != 2 {
		t.Fatalf("snapshot must hold two, got %+v", snap)
	}
	if snap[0].ChildFrameID != "apple" || snap[1].ChildFrameID != "zebra" {
		t.Fatalf("snapshot must sort by child, got %+v", snap)
	}
	snap[0].Transform.Translation.X = 99
	again := store.SnapshotStatics()
	if again[0].Transform.Translation.X == 99 {
		t.Fatal("snapshot mutation must not alias the store")
	}
}

// checkTFLookupTaxonomy pins exact error strings on every path.
//
//nolint:gocognit,cyclop // taxonomy test sequences empty, unknown, extrapolation, and connectivity checks in one run, splitting would obscure intent
func checkTFLookupTaxonomy(t *testing.T) {
	t.Helper()
	store := NewTFStore()
	_, err := store.Lookup("a", "b", 0)
	if err == nil || err.Error() != "tf: empty storage" {
		t.Fatalf("empty must be exact, got %v", err)
	}
	err = store.StoreStatic(tfSample("map", "odom", 100, 1))
	if err != nil {
		t.Fatalf("store static: %v", err)
	}
	_, err = store.Lookup("map", "missing", 0)
	if err == nil || err.Error() != `tf: no such frame "missing"` {
		t.Fatalf("no such frame must be exact, got %v", err)
	}
	_, err = store.Lookup("ghost", "odom", 0)
	if err == nil || err.Error() != `tf: no such frame "ghost"` {
		t.Fatalf("unknown target must be exact, got %v", err)
	}
	err = store.StoreStatic(tfSample("other", "frame", 100, 1))
	if err != nil {
		t.Fatalf("store static: %v", err)
	}
	_, err = store.Lookup("map", "frame", 0)
	if err == nil || err.Error() != `tf: connectivity loss: no path from "map" to "frame"` {
		t.Fatalf("connectivity must be exact, got %v", err)
	}
	dyn := NewTFStore()
	err = dyn.StoreDynamic(tfSample("odom", "base_link", 200, 1))
	if err != nil {
		t.Fatalf("store dynamic: %v", err)
	}
	_, err = dyn.Lookup("odom", "base_link", 100)
	if err == nil || err.Error() != `tf: extrapolation into past: requested 100 earliest 200 for "odom" to "base_link"` {
		t.Fatalf("past must be exact, got %v", err)
	}
	_, err = dyn.Lookup("odom", "base_link", 300)
	if err == nil || err.Error() != `tf: extrapolation into future: requested 300 latest 200 for "odom" to "base_link"` {
		t.Fatalf("future must be exact, got %v", err)
	}
	_, err = dyn.Lookup("odom", "base_link", -1)
	if err == nil || !strings.Contains(err.Error(), "invalid stamp") {
		t.Fatalf("negative stamp must be invalid, got %v", err)
	}
}

// checkTFZeroTimeLatest pins zero means latest.
func checkTFZeroTimeLatest(t *testing.T) {
	t.Helper()
	store := NewTFStore()
	err := store.StoreDynamic(tfSample("odom", "base_link", 100, 1))
	if err != nil {
		t.Fatalf("store dynamic: %v", err)
	}
	err = store.StoreDynamic(tfSample("odom", "base_link", 200, 2))
	if err != nil {
		t.Fatalf("store dynamic: %v", err)
	}
	got, err := store.Lookup("odom", "base_link", 0)
	if err != nil {
		t.Fatalf("zero lookup: %v", err)
	}
	if got.StampNanos != 200 || got.Transform.Translation.X != 2 {
		t.Fatalf("zero must mean latest, got %+v", got)
	}
}

// checkTFSameFrameIdentity pins same frame holds only for valid nonempty.
func checkTFSameFrameIdentity(t *testing.T) {
	t.Helper()
	empty := NewTFStore()
	_, err := empty.Lookup("odom", "odom", 0)
	if err == nil || err.Error() != "tf: empty storage" {
		t.Fatalf("empty same frame must report empty, got %v", err)
	}
	store := NewTFStore()
	err = store.StoreStatic(tfSample("map", "odom", 100, 1))
	if err != nil {
		t.Fatalf("store static: %v", err)
	}
	_, err = store.Lookup("ghost", "ghost", 0)
	if err == nil || err.Error() != `tf: no such frame "ghost"` {
		t.Fatalf("unknown same frame must report no such frame, got %v", err)
	}
	got, err := store.Lookup("odom", "odom", 0)
	if err != nil {
		t.Fatalf("known same frame must succeed: %v", err)
	}
	if got.Transform.Translation.X != 0 || got.Transform.Rotation.W != 1 {
		t.Fatalf("same frame must be identity, got %+v", got)
	}
	got, err = store.Lookup("odom", "odom", 500)
	if err != nil {
		t.Fatalf("timed same frame must succeed: %v", err)
	}
	if got.StampNanos != 500 {
		t.Fatalf("same frame must stamp requested, got %+v", got)
	}
}

// checkTFStaticIgnoreTime pins statics ignore time but stamp output.
func checkTFStaticIgnoreTime(t *testing.T) {
	t.Helper()
	store := NewTFStore()
	err := store.StoreStatic(tfSample("map", "odom", 100, 7))
	if err != nil {
		t.Fatalf("store static: %v", err)
	}
	got, err := store.Lookup("map", "odom", 9999)
	if err != nil {
		t.Fatalf("static future lookup must succeed: %v", err)
	}
	if got.Transform.Translation.X != 7 {
		t.Fatalf("static value must survive, got %+v", got)
	}
	if got.StampNanos != 9999 {
		t.Fatalf("static must stamp requested time, got %+v", got)
	}
	got, err = store.Lookup("map", "odom", 50)
	if err != nil {
		t.Fatalf("static past lookup must succeed: %v", err)
	}
	if got.StampNanos != 50 {
		t.Fatalf("static past must stamp requested, got %+v", got)
	}
	got, err = store.Lookup("map", "odom", 0)
	if err != nil {
		t.Fatalf("static zero lookup must succeed: %v", err)
	}
	if got.StampNanos != 100 {
		t.Fatalf("static zero must keep stored stamp, got %+v", got)
	}
}

// checkTFTimedNoInterpolation pins newest at or before without blending.
func checkTFTimedNoInterpolation(t *testing.T) {
	t.Helper()
	store := NewTFStore()
	err := store.StoreDynamic(tfSample("odom", "base_link", 100, 1))
	if err != nil {
		t.Fatalf("store dynamic: %v", err)
	}
	err = store.StoreDynamic(tfSample("odom", "base_link", 200, 2))
	if err != nil {
		t.Fatalf("store dynamic: %v", err)
	}
	err = store.StoreDynamic(tfSample("odom", "base_link", 300, 3))
	if err != nil {
		t.Fatalf("store dynamic: %v", err)
	}
	got, err := store.Lookup("odom", "base_link", 250)
	if err != nil {
		t.Fatalf("timed lookup: %v", err)
	}
	if got.Transform.Translation.X != 2 || got.StampNanos != 250 {
		t.Fatalf("timed must use newest at or before with requested stamp, got %+v", got)
	}
}

// checkTFMultiHopCompose pins BFS composition across two edges.
func checkTFMultiHopCompose(t *testing.T) {
	t.Helper()
	store := NewTFStore()
	err := store.StoreStatic(tfSample("map", "odom", 10, 1))
	if err != nil {
		t.Fatalf("store static: %v", err)
	}
	err = store.StoreStatic(tfSample("odom", "base_link", 10, 2))
	if err != nil {
		t.Fatalf("store static: %v", err)
	}
	got, err := store.Lookup("map", "base_link", 0)
	if err != nil {
		t.Fatalf("multi hop lookup: %v", err)
	}
	if got.Transform.Translation.X != 3 {
		t.Fatalf("two unit translations must sum to 3, got %v", got.Transform.Translation.X)
	}
	if got.FrameID != "map" || got.ChildFrameID != "base_link" {
		t.Fatalf("output frames must be normalized requested, got %+v", got)
	}
}

// checkTFReverseInverse pins reverse traversal inverts translation.
func checkTFReverseInverse(t *testing.T) {
	t.Helper()
	store := NewTFStore()
	err := store.StoreDynamic(tfSample("odom", "base_link", 100, 5))
	if err != nil {
		t.Fatalf("store dynamic: %v", err)
	}
	fwd, err := store.Lookup("odom", "base_link", 0)
	if err != nil {
		t.Fatalf("forward lookup: %v", err)
	}
	rev, err := store.Lookup("base_link", "odom", 0)
	if err != nil {
		t.Fatalf("reverse lookup: %v", err)
	}
	if fwd.Transform.Translation.X != 5 {
		t.Fatalf("forward must be 5, got %v", fwd.Transform.Translation.X)
	}
	if rev.Transform.Translation.X != -5 {
		t.Fatalf("reverse must invert to -5, got %v", rev.Transform.Translation.X)
	}
	round := fwd.Transform.Multiply(rev.Transform)
	if round.Translation.X > 1e-9 || round.Translation.X < -1e-9 {
		t.Fatalf("forward plus reverse must cancel, got %v", round.Translation.X)
	}
}

// checkTFLookupTimeout pins polling with buffer clock accounting.
//
//nolint:gocognit,cyclop // timeout test sequences nil clock, negative, success, and expiry checks in one run, splitting would obscure intent
func checkTFLookupTimeout(t *testing.T) {
	t.Helper()
	store := NewTFStore()
	clock := NewClockState(ClockWall, 1000, 1000)
	_, err := store.LookupWithTimeout("a", "b", 0, 10, nil)
	if err == nil {
		t.Fatal("nil clock must fail")
	}
	_, err = store.LookupWithTimeout("a", "b", 0, -1, clock)
	if err == nil {
		t.Fatal("negative timeout must fail")
	}
	err = store.StoreStatic(tfSample("map", "odom", 10, 1))
	if err != nil {
		t.Fatalf("store static: %v", err)
	}
	got, err := store.LookupWithTimeout("map", "odom", 0, 0, clock)
	if err != nil {
		t.Fatalf("zero timeout with data must succeed: %v", err)
	}
	if got.Transform.Translation.X != 1 {
		t.Fatalf("timeout lookup value must be 1, got %+v", got)
	}
	missing := NewTFStore()
	_, err = missing.LookupWithTimeout("map", "odom", 0, 0, clock)
	if err == nil || !strings.Contains(err.Error(), "empty storage") {
		t.Fatalf("zero timeout miss must carry taxonomy, got %v", err)
	}
	concurrent := NewTFStore()
	done := make(chan TransformStamped, 1)
	failed := make(chan error, 1)
	go func() {
		got, err := concurrent.LookupWithTimeout("map", "odom", 0, 5000000000, clock)
		if err != nil {
			failed <- err
			return
		}
		done <- got
	}()
	clock.SleepWall(5000000)
	err = concurrent.StoreStatic(tfSample("map", "odom", 10, 9))
	if err != nil {
		t.Fatalf("concurrent store: %v", err)
	}
	select {
	case got := <-done:
		if got.Transform.Translation.X != 9 {
			t.Fatalf("poll must observe late static, got %+v", got)
		}
	case err := <-failed:
		t.Fatalf("poll must succeed, got %v", err)
	}
	expiring := NewTFStore()
	expiryClock := NewClockState(ClockWall, 1000, 1000)
	type expiryResult struct {
		err error
	}
	expiryCh := make(chan expiryResult, 1)
	go func() {
		_, err := expiring.LookupWithTimeout("map", "odom", 0, 50, expiryClock)
		expiryCh <- expiryResult{err: err}
	}()
	expiryClock.SleepWall(5000000)
	expiryClock.SetWallTime(2000)
	res := <-expiryCh
	if res.err == nil || !strings.Contains(res.err.Error(), "timed out after 50 ns") {
		t.Fatalf("expiry must wrap taxonomy with timeout, got %v", res.err)
	}
	if !strings.Contains(res.err.Error(), "empty storage") {
		t.Fatalf("expiry must preserve taxonomy, got %v", res.err)
	}
}

// checkTFJumpClearsDynamics pins backward plus activation clears.
func checkTFJumpClearsDynamics(t *testing.T) {
	t.Helper()
	store := NewTFStore()
	err := store.StoreDynamic(tfSample("odom", "base_link", 1000, 1))
	if err != nil {
		t.Fatalf("store dynamic: %v", err)
	}
	err = store.StoreStatic(tfSample("map", "odom", 10, 2))
	if err != nil {
		t.Fatalf("store static: %v", err)
	}
	store.HandleClockJump(ClockJump{FromNanos: 1000, ToNanos: 900, Backward: true, SimActive: true})
	if got := store.DynamicCount(); got != 0 {
		t.Fatalf("backward sim jump must clear dynamics, got %d", got)
	}
	if got := store.StaticCount(); got != 1 {
		t.Fatalf("backward jump must preserve statics, got %d", got)
	}
	err = store.StoreDynamic(tfSample("odom", "base_link", 1000, 1))
	if err != nil {
		t.Fatalf("store dynamic: %v", err)
	}
	store.HandleClockJump(ClockJump{FromNanos: 1000, ToNanos: 1100, SimActive: true})
	if got := store.DynamicCount(); got != 1 {
		t.Fatalf("forward jump must preserve dynamics, got %d", got)
	}
	store.HandleClockJump(ClockJump{FromNanos: 1100, ToNanos: 900, Backward: true, SimActive: false})
	if got := store.DynamicCount(); got != 1 {
		t.Fatalf("backward wall inactive jump must not clear wall dynamics, got %d", got)
	}
	store.HandleClockJump(ClockJump{FromNanos: 1100, ToNanos: 1100, ActivationChanged: true, SimActive: false})
	if got := store.DynamicCount(); got != 0 {
		t.Fatalf("activation change must clear dynamics, got %d", got)
	}
	if got := store.StaticCount(); got != 1 {
		t.Fatalf("activation must preserve statics, got %d", got)
	}
}

// checkTFDynamicBound pins per key history bound drops oldest.
func checkTFDynamicBound(t *testing.T) {
	t.Helper()
	store := NewTFStore()
	for i := int64(1); i <= int64(TFHistoryDepth)+1; i++ {
		err := store.StoreDynamic(tfSample("odom", "base_link", i*10, float64(i)))
		if err != nil {
			t.Fatalf("store %d: %v", i, err)
		}
	}
	if got := store.DynamicCount(); got != int(TFHistoryDepth) {
		t.Fatalf("count must stay at bound %d, got %d", TFHistoryDepth, got)
	}
	got, err := store.Lookup("odom", "base_link", 10)
	if err == nil {
		t.Fatalf("evicted oldest must extrapolate past, got %+v", got)
	}
	if !strings.Contains(err.Error(), "extrapolation into past") {
		t.Fatalf("evicted lookup must be past, got %v", err)
	}
	latest, err := store.Lookup("odom", "base_link", 0)
	if err != nil {
		t.Fatalf("latest lookup: %v", err)
	}
	wantStamp := (int64(TFHistoryDepth) + 1) * 10
	if latest.StampNanos != wantStamp {
		t.Fatalf("latest must be %d, got %+v", wantStamp, latest)
	}
}

// checkTFConcurrentStoreLookup pins concurrent access stays race free.
func checkTFConcurrentStoreLookup(t *testing.T) {
	t.Helper()
	store := NewTFStore()
	err := store.StoreStatic(tfSample("map", "odom", 10, 1))
	if err != nil {
		t.Fatalf("store static: %v", err)
	}
	var wg sync.WaitGroup
	for i := range 8 {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			err := store.StoreDynamic(tfSample("odom", "base_link", int64(100+n), float64(n)))
			if err != nil {
				t.Error(err)
			}
			_, err = store.Lookup("map", "odom", 0)
			if err != nil {
				t.Error(err)
			}
			_ = store.SnapshotStatics()
			_ = store.DynamicCount()
			_ = store.StaticCount()
		}(i)
	}
	wg.Wait()
}
