// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package ros_test

import (
	"context"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/podomy/weld/internal/core"
	"github.com/podomy/weld/internal/transports/dds"
	"github.com/podomy/weld/testbed/live/helpers"
)

// TF lookup live gate constants for timeouts.
//
// Each test allocates one exclusive domain from the parallel pool, so
// every frame plus the allocated domain isolates runs. Timeouts bound
// every backend call through context.
const (
	tfGateTimeout   = 2 * time.Minute
	tfGateSettle    = 5 * time.Second
	tfGateProbeWait = 5 * time.Second
	tfGateTFMessage = "tf2_msgs/msg/TFMessage"
)

// tfGateFrameIDs derives per-run parent plus child frames from live ID.
//
// Hyphens map to underscores to keep frame names safe. The pair is
// unique per run so stale runs never match.
func tfGateFrameIDs(live string) (string, string) {
	base := strings.ReplaceAll(live, "-", "_")
	return base + "_map", base + "_odom"
}

// tfGateSample builds one transform with identity rotation and X shift.
func tfGateSample(parent, child string, stamp int64, x float64) core.TransformStamped {
	return core.TransformStamped{
		StampNanos:   stamp,
		FrameID:      parent,
		ChildFrameID: child,
		Transform: core.Transform{
			Translation: core.Vector3{X: x},
			Rotation:    core.Quaternion{X: 0, Y: 0, Z: 0, W: 1},
		},
	}
}

// tfGateCheckDDSTopics pins ROS TF topics to DDS rt names.
//
// Weld maps /tf to rt/tf and /tf_static to rt/tf_static through
// TopicName. A mapping miss fails loudly so wire drift never passes.
func tfGateCheckDDSTopics(t *testing.T) {
	t.Helper()
	if core.TFTopic != "/tf" {
		t.Fatalf("TFTopic = %q, want /tf", core.TFTopic)
	}
	if core.TFStaticTopic != "/tf_static" {
		t.Fatalf("TFStaticTopic = %q, want /tf_static", core.TFStaticTopic)
	}
	got, err := dds.TopicName(core.TFTopic, false)
	if err != nil {
		t.Fatalf("map /tf: %v", err)
	}
	if got != "rt/tf" {
		t.Fatalf("DDS topic for /tf = %q, want rt/tf", got)
	}
	got, err = dds.TopicName(core.TFStaticTopic, false)
	if err != nil {
		t.Fatalf("map /tf_static: %v", err)
	}
	if got != "rt/tf_static" {
		t.Fatalf("DDS topic for /tf_static = %q, want rt/tf_static", got)
	}
}

// tfGateJoinAndSettle joins discovery on the allocated domain then settles.
//
// Domain is the allocated exclusive domain plumbed to MustOpenLiveBackend,
// so parallel workers never share a domain. It opens the Weld backend with
// a per-run participant name, then sleeps the settle interval so SPDP routes
// quiesce before lookup. Cleanup is registered by MustOpenLiveBackend.
func tfGateJoinAndSettle(t *testing.T, ctx context.Context, domain int, runID string) {
	t.Helper()
	helpers.MustOpenLiveBackend(t, ctx, uint32(domain), "ros-tf-"+runID, "tf") //nolint:gosec // G115: pooled domain 70 through 101 fits uint32
	helpers.Sleep(ctx, tfGateSettle)
	if ctx.Err() != nil {
		t.Fatalf("context ended before lookup: %v", ctx.Err())
	}
}

// TestROSTFLookupStaticLatched proves one static stays resolvable.
//
// It stores one static transform once, settles, then resolves it with
// zero stamp and with a stamp three minutes in the future. Statics
// ignore time so both must return the stored value. Clear drops
// dynamics and preserves statics, and SnapshotStatics still replays
// the latched entry for late joiners.
func TestROSTFLookupStaticLatched(t *testing.T) {
	t.Parallel()
	domain := helpers.MustAllocateDomain(t)
	ctx, cancel := context.WithTimeout(context.Background(), tfGateTimeout)
	defer cancel()
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)
	helpers.SkipIfNoTypeInterface(t, ctx, helpers.Container, domain, helpers.RosBin, tfGateTFMessage, tfGateProbeWait)
	live := helpers.LiveID("ros-tf-static")
	tfGateCheckDDSTopics(t)
	tfGateJoinAndSettle(t, ctx, domain, live)
	parent, child := tfGateFrameIDs(live)
	store := core.NewTFStore()
	err := store.StoreStatic(tfGateSample(parent, child, 100, 7))
	if err != nil {
		t.Fatalf("store static: %v", err)
	}
	helpers.Sleep(ctx, tfGateSettle)
	if ctx.Err() != nil {
		t.Fatalf("context ended before lookup: %v", ctx.Err())
	}
	got, err := store.Lookup(parent, child, 0)
	if err != nil {
		t.Fatalf("static latest lookup: %v", err)
	}
	if got.Transform.Translation.X != 7 {
		t.Fatalf("static latest X = %v, want 7", got.Transform.Translation.X)
	}
	future := int64(100 + 180*1000*1000*1000)
	got, err = store.Lookup(parent, child, future)
	if err != nil {
		t.Fatalf("static future lookup %d: %v", future, err)
	}
	if got.Transform.Translation.X != 7 {
		t.Fatalf("static future X = %v, want 7", got.Transform.Translation.X)
	}
	if got.StampNanos != future {
		t.Fatalf("static future stamp = %d, want %d", got.StampNanos, future)
	}
	store.Clear()
	helpers.Sleep(ctx, tfGateSettle)
	if ctx.Err() != nil {
		t.Fatalf("context ended before latched lookup: %v", ctx.Err())
	}
	got, err = store.Lookup(parent, child, future)
	if err != nil {
		t.Fatalf("latched lookup after Clear: %v", err)
	}
	if got.Transform.Translation.X != 7 {
		t.Fatalf("latched X after Clear = %v, want 7", got.Transform.Translation.X)
	}
	snap := store.SnapshotStatics()
	if len(snap) != 1 {
		t.Fatalf("latched snapshot len = %d, want 1", len(snap))
	}
}

// TestROSTFLookupDynamicTimed proves timed lookup uses newest at or before.
//
// It stores X 1 at T1 and X 3 at T2, settles, then looks up the
// midpoint. The store holds no interpolation: the midpoint returns
// X 1 with the requested stamp. Linear interpolation at the midpoint
// would be X 2, so X 1 is within tolerance 1 of interpolated.
func TestROSTFLookupDynamicTimed(t *testing.T) {
	t.Parallel()
	domain := helpers.MustAllocateDomain(t)
	ctx, cancel := context.WithTimeout(context.Background(), tfGateTimeout)
	defer cancel()
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)
	helpers.SkipIfNoTypeInterface(t, ctx, helpers.Container, domain, helpers.RosBin, tfGateTFMessage, tfGateProbeWait)
	live := helpers.LiveID("ros-tf-dynamic")
	tfGateCheckDDSTopics(t)
	tfGateJoinAndSettle(t, ctx, domain, live)
	parent, child := tfGateFrameIDs(live)
	store := core.NewTFStore()
	first := int64(1000000000)
	second := int64(3000000000)
	mid := int64(2000000000)
	err := store.StoreDynamic(tfGateSample(parent, child, first, 1))
	if err != nil {
		t.Fatalf("store dynamic first: %v", err)
	}
	err = store.StoreDynamic(tfGateSample(parent, child, second, 3))
	if err != nil {
		t.Fatalf("store dynamic second: %v", err)
	}
	helpers.Sleep(ctx, tfGateSettle)
	if ctx.Err() != nil {
		t.Fatalf("context ended before lookup: %v", ctx.Err())
	}
	got, err := store.Lookup(parent, child, mid)
	if err != nil {
		t.Fatalf("timed lookup %d: %v", mid, err)
	}
	if got.Transform.Translation.X != 1 {
		t.Fatalf("timed X = %v, want 1 newest at or before %d", got.Transform.Translation.X, mid)
	}
	if got.StampNanos != mid {
		t.Fatalf("timed stamp = %d, want %d", got.StampNanos, mid)
	}
	linear := 2.0
	if math.Abs(got.Transform.Translation.X-linear) > 1.0+1e-9 {
		t.Fatalf("timed X %v not within 1 of interpolated %v", got.Transform.Translation.X, linear)
	}
	latest, err := store.Lookup(parent, child, 0)
	if err != nil {
		t.Fatalf("latest lookup: %v", err)
	}
	if latest.Transform.Translation.X != 3 {
		t.Fatalf("latest X = %v, want 3", latest.Transform.Translation.X)
	}
}

// TestROSTFLookupUnknownFrames proves unknown pairs report no such frame.
//
// It stores one known static pair with per-run frames, settles, then
// looks up two ghost frames that were never stored. The lookup must
// fail with the documented no such frame string and must not return
// empty data as success.
func TestROSTFLookupUnknownFrames(t *testing.T) {
	t.Parallel()
	domain := helpers.MustAllocateDomain(t)
	ctx, cancel := context.WithTimeout(context.Background(), tfGateTimeout)
	defer cancel()
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)
	helpers.SkipIfNoTypeInterface(t, ctx, helpers.Container, domain, helpers.RosBin, tfGateTFMessage, tfGateProbeWait)
	live := helpers.LiveID("ros-tf-unknown")
	tfGateCheckDDSTopics(t)
	tfGateJoinAndSettle(t, ctx, domain, live)
	parent, child := tfGateFrameIDs(live)
	store := core.NewTFStore()
	err := store.StoreStatic(tfGateSample(parent, child, 100, 1))
	if err != nil {
		t.Fatalf("store static: %v", err)
	}
	helpers.Sleep(ctx, tfGateSettle)
	if ctx.Err() != nil {
		t.Fatalf("context ended before lookup: %v", ctx.Err())
	}
	base := strings.ReplaceAll(live, "-", "_")
	ghostTarget := base + "_ghost_map"
	ghostSource := base + "_ghost_odom"
	got, err := store.Lookup(ghostTarget, ghostSource, 0)
	if err == nil {
		t.Fatalf("unknown lookup succeeded with %+v, want no such frame", got)
	}
	want := `tf: no such frame "` + ghostTarget + `"`
	if err.Error() != want {
		t.Fatalf("unknown error = %q, want %q", err.Error(), want)
	}
	if got.Transform.Translation.X != 0 || got.Transform.Rotation.W != 0 {
		t.Fatalf("unknown lookup returned data %+v, want empty on error", got.Transform)
	}
}
