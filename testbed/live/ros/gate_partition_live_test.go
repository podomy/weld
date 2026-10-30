// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package ros_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/podomy/weld/internal/transports/dds"
	"github.com/podomy/weld/internal/transports/dds/rtps"
	"github.com/podomy/weld/testbed/live/helpers"
)

// DDS partition gate: mechanism and scope.
//
// The stock ros2 CLI in the testbed images selects reliability, durability,
// history, and depth only (see the pub and echo argv in
// gate_qos_reliability_test.go); it exposes no partition flag, and the
// CYCLONEDDS_URI XML config used by the testbed peers offers no per-topic
// partition control either. The partitioned peer is therefore a second Weld
// endpoint set driven through the new partition API in
// internal/transports/dds/partition.go, while a Cyclone peer covers the
// default-partition flow in both directions.
//
// The Weld-to-Weld leg exchanges real SEDP wire bytes through the exact
// backend codec: SEDPIndex DeclareWriter and DeclareReader mint the
// endpoints, the partition list rides the opaque QoS passthrough field as
// an rtps-encoded PID_PARTITION param (a test-only vehicle until sedp.go
// carries PID_PARTITION natively; see the INTEGRATION notes in
// partition.go), SerializeSEDPDATA frames the DATA, ParseSEDPDATAs parses
// it back, and FindPartitionParam recovers the lists. MatchPartitions and
// PartitionRouteKey then decide the outcome. DATA-path enforcement (the
// backend dropping samples for non-matching partitions) still awaits the
// backend route-key wiring, so this gate proves discovery-layer isolation
// plus the wire codec, not DATA drops.
//
// Each test takes one exclusive domain from the parallel pool, uses a
// per-run topic and live ID, skips only when no peer container is present,
// and fails loudly otherwise.

// TestGatePartitionDefaultFlowLive checks a Cyclone talker on the default
// partition reaches weld echo on a per-run topic.
func TestGatePartitionDefaultFlowLive(t *testing.T) {
	t.Parallel()
	domain := helpers.MustAllocateDomain(t)
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)
	helpers.SkipIfNoTypeInterface(t, ctx, helpers.Container, domain, helpers.RosBin, rosType, typeProbe)
	bin := helpers.BuildWeld(t, ctx)
	live := helpers.LiveID("part")
	topic := helpers.LiveTopic(live)
	payload := "hello partition default " + live
	pidPath := helpers.HostPidPath(t, live)
	pubArg := fmt.Sprintf("{data: %s}", payload)
	err := helpers.StartPeerProc(ctx, helpers.Container, domain, live, helpers.RosBin, "topic", "pub", topic, rosType, pubArg, "--rate", "2")
	if err != nil {
		t.Fatalf("start ROS talker: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.WithoutCancel(ctx), cleanWait)
		defer cleanupCancel()
		helpers.StopPeerProc(cleanupCtx, helpers.Container, domain, pidPath, live)
	})
	helpers.Sleep(ctx, forwardNap)
	if ctx.Err() != nil {
		t.Fatalf("context ended before echo: %v", ctx.Err())
	}
	stdout, stderr, err := helpers.RunWeldEcho(ctx, bin, topic, domain, echoWait)
	if err != nil {
		t.Fatalf("weld echo: %v\nstdout:\n%s\nstderr:\n%s", err, stdout, stderr)
	}
	if !strings.Contains(stdout, live) {
		t.Fatalf("weld echo missed default-partition payload %q:\nstdout:\n%s\nstderr:\n%s", payload, stdout, stderr)
	}
}

// TestGatePartitionReverseFlowLive checks weld pub on the default partition
// reaches ros2 topic echo on a per-run topic.
func TestGatePartitionReverseFlowLive(t *testing.T) {
	t.Parallel()
	domain := helpers.MustAllocateDomain(t)
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)
	helpers.SkipIfNoTypeInterface(t, ctx, helpers.Container, domain, helpers.RosBin, rosType, typeProbe)
	bin := helpers.BuildWeld(t, ctx)
	live := helpers.LiveID("part")
	topic := helpers.LiveTopic(live)
	payload := "hello partition reverse " + live
	echoPath := helpers.HostOutPath(t, live)
	pidPath := helpers.HostPidPath(t, live)
	err := helpers.StartPeerProc(ctx, helpers.Container, domain, live, helpers.RosBin, "topic", "echo", topic, rosType)
	if err != nil {
		t.Fatalf("start ros2 topic echo: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.WithoutCancel(ctx), cleanWait)
		defer cleanupCancel()
		helpers.StopPeerProc(cleanupCtx, helpers.Container, domain, pidPath, topic)
	})
	helpers.Sleep(ctx, reverseNap)
	if ctx.Err() != nil {
		t.Fatalf("context ended before pub: %v", ctx.Err())
	}
	found, stdout, stderr, err := helpers.PublishUntilSeen(ctx, bin, topic, domain, payload, echoPath, pollWait, 3)
	if err != nil {
		t.Fatalf("weld pub: %v\nstdout:\n%s\nstderr:\n%s", err, stdout, stderr)
	}
	if !strings.Contains(stdout, "sent:") {
		t.Fatalf("weld pub missing confirmation:\nstdout:\n%s\nstderr:\n%s", stdout, stderr)
	}
	if !found {
		t.Fatalf("ros2 topic echo missed default-partition payload %q", payload)
	}
}

// TestGatePartitionIsolationLive proves discovery-layer isolation between
// partitions on one per-run topic while a Cyclone talker publishes on it.
//
// A Cyclone talker on the default partition runs alongside so the exchange
// happens with live discovery traffic present. A Weld writer offering
// partition B must not match a Weld reader requesting partition A (keys
// differ, outcome names no_common_partition), while offering B to a reader
// requesting B matches with equal keys, and default meets default. Every
// mismatch fails loudly; only a missing peer skips.
func TestGatePartitionIsolationLive(t *testing.T) {
	t.Parallel()
	domain := helpers.MustAllocateDomain(t)
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)
	helpers.SkipIfNoTypeInterface(t, ctx, helpers.Container, domain, helpers.RosBin, rosType, typeProbe)
	live := helpers.LiveID("part")
	topic := helpers.LiveTopic(live)
	pidPath := helpers.HostPidPath(t, live)
	pubArg := fmt.Sprintf("{data: %s}", "partition beside "+live)
	err := helpers.StartPeerProc(ctx, helpers.Container, domain, live, helpers.RosBin, "topic", "pub", topic, rosType, pubArg, "--rate", "2")
	if err != nil {
		t.Fatalf("start ROS talker: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.WithoutCancel(ctx), cleanWait)
		defer cleanupCancel()
		helpers.StopPeerProc(cleanupCtx, helpers.Container, domain, pidPath, live)
	})

	var writerPrefix dds.SEDPGUIDPrefix
	writerPrefix[0] = 0xA1
	writerIdx, err := dds.NewSEDPIndex(writerPrefix, uint32(domain)) //nolint:gosec // G115: allocated domains fit uint32.
	if err != nil {
		t.Fatalf("new writer index: %v", err)
	}
	var readerPrefix dds.SEDPGUIDPrefix
	readerPrefix[0] = 0xB2
	readerIdx, err := dds.NewSEDPIndex(readerPrefix, uint32(domain)) //nolint:gosec // G115: allocated domains fit uint32.
	if err != nil {
		t.Fatalf("new reader index: %v", err)
	}

	offered := partitionLiveOffered(t, writerIdx, topic, []string{"B"})
	requestedA := partitionLiveRequested(t, readerIdx, topic, []string{"A"})
	requestedB := partitionLiveRequested(t, readerIdx, topic, []string{"B"})

	outcome := dds.MatchPartitions(offered, requestedA)
	if outcome.Matched {
		t.Fatalf("partition A reader matched partition B writer, want isolation: %+v", outcome)
	}
	if outcome.Reason != dds.PartitionMismatchNoCommonPartition {
		t.Fatalf("isolation reason = %q, want %q", outcome.Reason, dds.PartitionMismatchNoCommonPartition)
	}
	ddsTopic := offeredDDSTopic(t, writerIdx, topic)
	if dds.PartitionRouteKey(ddsTopic, offered) == dds.PartitionRouteKey(ddsTopic, requestedA) {
		t.Fatalf("partitions A and B share discovery route key")
	}

	flow := dds.MatchPartitions(offered, requestedB)
	if !flow.Matched {
		t.Fatalf("partition B reader missed partition B writer: %+v", flow)
	}
	if dds.PartitionRouteKey(ddsTopic, offered) != dds.PartitionRouteKey(ddsTopic, requestedB) {
		t.Fatalf("matching partitions differ in discovery route key")
	}

	cycloneOffered := []string(nil)
	if !dds.PartitionsMatch(nil, cycloneOffered) {
		t.Fatalf("default Cyclone offer missed default Weld request")
	}
}

// partitionLiveOffered declares a writer, smuggles partitions through the
// QoS passthrough as a PID_PARTITION param, and returns the wire-decoded
// offered list from real SEDP bytes.
func partitionLiveOffered(t *testing.T, idx *dds.SEDPIndex, rosTopic string, partitions []string) []string {
	t.Helper()
	ep, err := idx.DeclareWriter(rosTopic, "std_msgs__msg__String", dds.StringTypeHash, partitionLiveQoS(t, partitions))
	if err != nil {
		t.Fatalf("declare writer: %v", err)
	}
	return partitionLiveWirePartitions(t, ep)
}

// partitionLiveRequested declares a reader and returns the wire-decoded
// requested list from real SEDP bytes.
func partitionLiveRequested(t *testing.T, idx *dds.SEDPIndex, rosTopic string, partitions []string) []string {
	t.Helper()
	ep, err := idx.DeclareReader(rosTopic, "std_msgs__msg__String", dds.StringTypeHash, partitionLiveQoS(t, partitions))
	if err != nil {
		t.Fatalf("declare reader: %v", err)
	}
	return partitionLiveWirePartitions(t, ep)
}

// partitionLiveQoS encodes partitions as an rtps param list for the QoS
// passthrough field, the test-only vehicle documented in the file header.
func partitionLiveQoS(t *testing.T, partitions []string) []byte {
	t.Helper()
	param, err := dds.PartitionParam(partitions)
	if err != nil {
		t.Fatalf("partition param: %v", err)
	}
	return rtps.EncodeParams([]rtps.Param{param})
}

// partitionLiveWirePartitions serializes one endpoint to SEDP DATA bytes,
// parses them back, and recovers the partition list from the QoS bytes.
func partitionLiveWirePartitions(t *testing.T, ep dds.SEDPEndpoint) []string {
	t.Helper()
	raw, err := dds.SerializeSEDPDATA(ep)
	if err != nil {
		t.Fatalf("serialize SEDP: %v", err)
	}
	eps, err := dds.ParseSEDPDATAs(raw)
	if err != nil {
		t.Fatalf("parse SEDP: %v", err)
	}
	if len(eps) != 1 {
		t.Fatalf("parse SEDP gave %d endpoints, want 1", len(eps))
	}
	params, err := rtps.DecodeParams(eps[0].QoS)
	if err != nil {
		t.Fatalf("decode QoS params: %v", err)
	}
	names, err := dds.FindPartitionParam(params)
	if err != nil {
		t.Fatalf("find partition param: %v", err)
	}
	return names
}

// offeredDDSTopic returns the DDS topic for the route key from a declared
// writer on the same ROS topic.
func offeredDDSTopic(t *testing.T, idx *dds.SEDPIndex, rosTopic string) string {
	t.Helper()
	ep, err := idx.DeclareWriter(rosTopic, "std_msgs__msg__String", dds.StringTypeHash, nil)
	if err != nil {
		t.Fatalf("declare probe writer: %v", err)
	}
	return ep.DDSTopic
}
