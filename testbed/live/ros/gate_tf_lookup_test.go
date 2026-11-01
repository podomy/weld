// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package ros_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/podomy/weld/internal/core"
	"github.com/podomy/weld/internal/transports"
	"github.com/podomy/weld/internal/transports/dds"
	"github.com/podomy/weld/internal/types"
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
// so parallel workers never share a domain. It opens Weld pub and sub
// backends with per-run participant names, then sleeps the settle interval
// so SPDP routes quiesce before publish. Cleanup is registered by
// MustOpenLiveBackend for both backends. It returns the pub and sub
// backends for real TF publish plus take.
func tfGateJoinAndSettle(t *testing.T, ctx context.Context, domain int, runID string) (*dds.Backend, *dds.Backend) {
	t.Helper()
	pub := helpers.MustOpenLiveBackend(t, ctx, uint32(domain), "ros-tf-pub-"+runID, "tf") //nolint:gosec // G115: pooled domain 70 through 101 fits uint32
	sub := helpers.MustOpenLiveBackend(t, ctx, uint32(domain), "ros-tf-sub-"+runID, "tf") //nolint:gosec // G115: pooled domain 70 through 101 fits uint32
	helpers.Sleep(ctx, tfGateSettle)
	if ctx.Err() != nil {
		t.Fatalf("context ended before publish: %v", ctx.Err())
	}
	return pub, sub
}

// tfEncodeTransform encodes one transform for DDS transport.
func tfEncodeTransform(t *testing.T, parent, child string, stamp int64, x float64) []byte {
	t.Helper()
	enc := types.NewEncoder(true)
	err := enc.WriteString(parent)
	if err != nil {
		t.Fatalf("encode tf parent: %v", err)
	}
	err = enc.WriteString(child)
	if err != nil {
		t.Fatalf("encode tf child: %v", err)
	}
	enc.WriteInt64(stamp)
	enc.WriteFloat64(x)
	return enc.Bytes()
}

// tfDecodeTransform decodes one DDS transform payload.
func tfDecodeTransform(t *testing.T, payload []byte) (string, string, int64, float64) {
	t.Helper()
	dec, err := types.NewDecoder(payload)
	if err != nil {
		t.Fatalf("new tf decoder: %v", err)
	}
	parent, err := dec.ReadString()
	if err != nil {
		t.Fatalf("decode tf parent: %v", err)
	}
	child, err := dec.ReadString()
	if err != nil {
		t.Fatalf("decode tf child: %v", err)
	}
	stamp, err := dec.ReadInt64()
	if err != nil {
		t.Fatalf("decode tf stamp: %v", err)
	}
	x, err := dec.ReadFloat64()
	if err != nil {
		t.Fatalf("decode tf x: %v", err)
	}
	err = dec.Finish()
	if err != nil {
		t.Fatalf("finish tf: %v", err)
	}
	return parent, child, stamp, x
}

// tfPublishTake publishes one transform via DDS and takes it back.
//
// It subscribes the sub backend, publishes via the pub backend with
// PublishLiveWithRetry, then takes until the payload arrives or the take
// wait ends. A publish without routes fails loudly as product news. A
// missing take fails loudly so a silent DDS path never passes as a TF
// lookup.
func tfPublishTake(t *testing.T, ctx context.Context, pub, sub *dds.Backend, topic, parent, child string, stamp int64, x float64) []byte {
	t.Helper()
	err := sub.Subscribe(ctx, topic, helpers.ForwardStringIDL, helpers.ForwardStringHash)
	if err != nil {
		t.Fatalf("subscribe tf %s: %v", topic, err)
	}
	helpers.WaitForTopicDiscovery(ctx, sub, topic)
	payload := tfEncodeTransform(t, parent, child, stamp, x)
	msg := transports.OutgoingMessage{
		TopicName: topic,
		TypeName:  helpers.ForwardStringIDL,
		Payload:   payload,
	}
	err = helpers.PublishLiveWithRetry(ctx, pub, msg, tfGateSettle, 50*time.Millisecond, "tf")
	if err != nil {
		t.Fatalf("weld pub tf %s: %v", topic, err)
	}
	takeCtx, takeCancel := context.WithTimeout(ctx, tfGateSettle)
	defer takeCancel()
	for takeCtx.Err() == nil {
		got, terr := sub.TakeBytes(takeCtx)
		if terr != nil {
			continue
		}
		if len(got.Payload) == 0 {
			continue
		}
		return got.Payload
	}
	t.Fatalf("weld take missed tf payload on %s", topic)
	return nil
}

// TestROSTFLookupStaticLatched proves one static stays resolvable via DDS.
//
// It publishes one static transform over DDS through the opened backend,
// takes it back, stores the taken sample, then resolves it with zero
// stamp and with a stamp three minutes in the future. Statics ignore
// time so both must return the stored value.
func TestROSTFLookupStaticLatched(t *testing.T) {
	t.Parallel()
	domain := helpers.MustAllocateDomain(t)
	ctx, cancel := context.WithTimeout(context.Background(), tfGateTimeout)
	defer cancel()
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)
	helpers.SkipIfNoTypeInterface(t, ctx, helpers.Container, domain, helpers.RosBin, tfGateTFMessage, tfGateProbeWait)
	live := helpers.LiveID("ros-tf-static")
	tfGateCheckDDSTopics(t)
	pub, sub := tfGateJoinAndSettle(t, ctx, domain, live)
	parent, child := tfGateFrameIDs(live)
	topic := helpers.LiveTopic(live)
	taken := tfPublishTake(t, ctx, pub, sub, topic, parent, child, 100, 7)
	gotParent, gotChild, gotStamp, gotX := tfDecodeTransform(t, taken)
	if gotParent != parent || gotChild != child {
		t.Fatalf("taken frames = %s/%s, want %s/%s", gotParent, gotChild, parent, child)
	}
	if gotStamp != 100 || gotX != 7 {
		t.Fatalf("taken sample = %d/%v, want 100/7", gotStamp, gotX)
	}
	store := core.NewTFStore()
	err := store.StoreStatic(tfGateSample(parent, child, gotStamp, gotX))
	if err != nil {
		t.Fatalf("store static taken: %v", err)
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
}

// TestROSTFLookupDynamicTimed proves timed lookup uses newest at or before via DDS.
//
// It publishes X 1 at T1 and X 3 at T2 over DDS through the opened
// backend, takes each back, stores the taken samples, then looks up the
// midpoint. The store holds no interpolation: the midpoint returns X 1
// with the requested stamp.
func TestROSTFLookupDynamicTimed(t *testing.T) {
	t.Parallel()
	domain := helpers.MustAllocateDomain(t)
	ctx, cancel := context.WithTimeout(context.Background(), tfGateTimeout)
	defer cancel()
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)
	helpers.SkipIfNoTypeInterface(t, ctx, helpers.Container, domain, helpers.RosBin, tfGateTFMessage, tfGateProbeWait)
	live := helpers.LiveID("ros-tf-dynamic")
	tfGateCheckDDSTopics(t)
	pub, sub := tfGateJoinAndSettle(t, ctx, domain, live)
	parent, child := tfGateFrameIDs(live)
	topic := helpers.LiveTopic(live)
	store := core.NewTFStore()
	first := int64(1000000000)
	second := int64(3000000000)
	mid := int64(2000000000)
	firstTaken := tfPublishTake(t, ctx, pub, sub, topic+"-first", parent, child, first, 1)
	_, _, firstStamp, firstX := tfDecodeTransform(t, firstTaken)
	err := store.StoreDynamic(tfGateSample(parent, child, firstStamp, firstX))
	if err != nil {
		t.Fatalf("store dynamic first taken: %v", err)
	}
	secondTaken := tfPublishTake(t, ctx, pub, sub, topic+"-second", parent, child, second, 3)
	_, _, secondStamp, secondX := tfDecodeTransform(t, secondTaken)
	err = store.StoreDynamic(tfGateSample(parent, child, secondStamp, secondX))
	if err != nil {
		t.Fatalf("store dynamic second taken: %v", err)
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
	latest, err := store.Lookup(parent, child, 0)
	if err != nil {
		t.Fatalf("latest lookup: %v", err)
	}
	if latest.Transform.Translation.X != 3 {
		t.Fatalf("latest X = %v, want 3", latest.Transform.Translation.X)
	}
}

// TestROSTFLookupUnknownFrames proves unknown pairs report no such frame via DDS.
//
// It publishes one known static pair over DDS through the opened backend,
// takes it back, stores the taken sample, then looks up two ghost frames
// that were never stored. The lookup must fail with the documented no
// such frame string and must not return empty data as success.
func TestROSTFLookupUnknownFrames(t *testing.T) {
	t.Parallel()
	domain := helpers.MustAllocateDomain(t)
	ctx, cancel := context.WithTimeout(context.Background(), tfGateTimeout)
	defer cancel()
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)
	helpers.SkipIfNoTypeInterface(t, ctx, helpers.Container, domain, helpers.RosBin, tfGateTFMessage, tfGateProbeWait)
	live := helpers.LiveID("ros-tf-unknown")
	tfGateCheckDDSTopics(t)
	pub, sub := tfGateJoinAndSettle(t, ctx, domain, live)
	parent, child := tfGateFrameIDs(live)
	topic := helpers.LiveTopic(live)
	taken := tfPublishTake(t, ctx, pub, sub, topic, parent, child, 100, 1)
	takenParent, takenChild, takenStamp, takenX := tfDecodeTransform(t, taken)
	store := core.NewTFStore()
	err := store.StoreStatic(tfGateSample(takenParent, takenChild, takenStamp, takenX))
	if err != nil {
		t.Fatalf("store static taken: %v", err)
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
