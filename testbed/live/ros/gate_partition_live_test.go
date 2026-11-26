// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

//nolint:dupl // live gate take helpers repeat similar wire checks across QoS gates
package ros_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/podomy/weld/internal/transports"
	"github.com/podomy/weld/internal/transports/dds"
	"github.com/podomy/weld/internal/transports/dds/rtps"
	"github.com/podomy/weld/internal/types"
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
// PartitionRouteKey then decide the outcome. DATA-path enforcement rides
// the same route key in the backend (partitionAllowsDelivery on demux plus
// partitionGateReject and retention rekeying on take, pinned by unit
// tests); this gate proves discovery-layer isolation plus the wire codec
// because the live peers expose no partition control for DATA legs.
//
// Each test takes one exclusive domain from the parallel pool, uses a
// per-run topic and live ID, skips only when no peer container is present,
// and fails loudly otherwise.

// Partition live publish retry bounds for Weld backend reverse legs.
const (
	partitionLivePubRetry      = 5 * time.Second
	partitionLivePubInterval   = 50 * time.Millisecond
	partitionLiveRepubWait     = 2 * time.Second
	partitionLiveTakeWait      = 45 * time.Second
	partitionLiveHashProbeWait = 5 * time.Second
)

// partitionLiveEncodeString encodes one String payload as XCDR1.
func partitionLiveEncodeString(t *testing.T, text string) []byte {
	t.Helper()
	enc := types.NewEncoder(true)
	err := enc.WriteString(text)
	if err != nil {
		t.Fatalf("partition encode: %v", err)
	}
	return enc.Bytes()
}

// partitionLiveDecodeString decodes one String payload and checks the live payload.
func partitionLiveDecodeString(payload []byte, want string) error {
	dec, err := types.NewDecoder(payload)
	if err != nil {
		return fmt.Errorf("partition decode header: %w", err)
	}
	got, err := dec.ReadString()
	if err != nil {
		return fmt.Errorf("partition decode data: %w", err)
	}
	if got != want {
		return fmt.Errorf("partition data %q wants %q", got, want)
	}
	err = dec.Finish()
	if err != nil {
		return fmt.Errorf("partition finish: %w", err)
	}
	return nil
}

// partitionLiveTakeFailure classifies one TakeBytes error.
func partitionLiveTakeFailure(t *testing.T, terr error) bool {
	t.Helper()
	if errors.Is(terr, context.DeadlineExceeded) || errors.Is(terr, context.Canceled) {
		return true
	}
	text := terr.Error()
	if strings.Contains(text, "not implemented") {
		t.Fatalf("partition take backend error: %v", terr)
	}
	if strings.Contains(text, "not wired") {
		t.Fatalf("partition take backend error: %v", terr)
	}
	return false
}

// partitionLiveTakeUntilLive takes until one sample decodes to the live payload.
func partitionLiveTakeUntilLive(t *testing.T, ctx context.Context, backend *dds.Backend, topic, payload string, timeout time.Duration) {
	t.Helper()
	takeCtx, takeCancel := context.WithTimeout(ctx, timeout)
	defer takeCancel()
	for takeCtx.Err() == nil {
		msg, terr := backend.TakeBytes(takeCtx)
		if terr != nil {
			if partitionLiveTakeFailure(t, terr) {
				break
			}
			continue
		}
		if msg.TopicName != "" && msg.TopicName != topic {
			continue
		}
		if partitionLiveDecodeString(msg.Payload, payload) != nil {
			continue
		}
		return
	}
	t.Fatalf("weld take did not see partition payload %q on %s after %v", payload, topic, timeout)
}

// TestGatePartitionDefaultFlowLive checks a Cyclone talker on the default
// partition reaches Weld backend take on a per-run topic.
func TestGatePartitionDefaultFlowLive(t *testing.T) {
	t.Parallel()
	domain := helpers.MustAllocateDomain(t)
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)
	helpers.SkipIfNoTypeInterface(t, ctx, helpers.Container, domain, helpers.RosBin, rosType, typeProbe)
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
		helpers.StopPeerProcAndWait(cleanupCtx, helpers.Container, domain, pidPath, live)
	})
	helpers.Sleep(ctx, forwardNap)
	if ctx.Err() != nil {
		t.Fatalf("context ended before take: %v", ctx.Err())
	}
	typeHash, err := helpers.ResolveTopicHash(ctx, helpers.Container, domain, helpers.RosBin, topic, helpers.ForwardStringHash, partitionLiveHashProbeWait, "part")
	if err != nil {
		t.Fatalf("hash unavailable for topic %s: %v", topic, err)
	}
	t.Logf("partition forward topic %s hash %s", topic, typeHash)
	backend := helpers.MustOpenLiveBackend(t, ctx, uint32(domain), "part-forward-"+live, "part") //nolint:gosec // G115: pooled domain 70 through 101 fits uint32
	err = helpers.SubscribeLiveReader(ctx, backend, topic, helpers.ForwardStringIDL, typeHash, "part")
	if err != nil {
		t.Fatalf("subscribe %s: %v", topic, err)
	}
	partitionLiveTakeUntilLive(t, ctx, backend, topic, payload, partitionLiveTakeWait)
}

// TestGatePartitionReverseFlowLive checks Weld backend publish on the default partition
// reaches ros2 topic echo on a per-run topic.
func TestGatePartitionReverseFlowLive(t *testing.T) {
	t.Parallel()
	domain := helpers.MustAllocateDomain(t)
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)
	helpers.SkipIfNoTypeInterface(t, ctx, helpers.Container, domain, helpers.RosBin, rosType, typeProbe)
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
		helpers.StopPeerProcAndWait(cleanupCtx, helpers.Container, domain, pidPath, topic)
	})
	helpers.Sleep(ctx, reverseNap)
	if ctx.Err() != nil {
		t.Fatalf("context ended before pub: %v", ctx.Err())
	}
	typeHash, err := helpers.ResolveTopicHash(ctx, helpers.Container, domain, helpers.RosBin, topic, helpers.ForwardStringHash, partitionLiveHashProbeWait, "part")
	if err != nil {
		t.Fatalf("hash unavailable for topic %s: %v", topic, err)
	}
	t.Logf("partition reverse topic %s hash %s", topic, typeHash)
	encoded := partitionLiveEncodeString(t, payload)
	err = partitionLiveDecodeString(encoded, payload)
	if err != nil {
		t.Fatalf("local codec: %v", err)
	}
	backend := helpers.MustOpenLiveBackend(t, ctx, uint32(domain), "part-reverse-"+live, "part") //nolint:gosec // G115: pooled domain 70 through 101 fits uint32
	msg := transports.OutgoingMessage{
		TopicName: topic,
		TypeName:  helpers.ForwardStringIDL,
		Payload:   encoded,
	}
	if !helpers.PublishUntilEcho(ctx, t, backend, msg, echoPath, payload, typeHash, pollWait, partitionLiveRepubWait, partitionLivePubRetry, partitionLivePubInterval, "part") {
		data, rerr := os.ReadFile(echoPath) //nolint:gosec // test reads controlled logs path
		if rerr != nil {
			t.Fatalf("ros2 topic echo missed default-partition payload %q: %v", payload, rerr)
		}
		t.Fatalf("ros2 topic echo missed default-partition payload %q\necho output:\n%s", payload, string(data))
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
		helpers.StopPeerProcAndWait(cleanupCtx, helpers.Container, domain, pidPath, live)
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

	ddsTopic := offeredDDSTopic(t, writerIdx, topic)
	offeredKey := partitionLiveRouteKey(t, ddsTopic, offered)
	partitionLiveCheckIsolated(t, offered, requestedA, offeredKey, partitionLiveRouteKey(t, ddsTopic, requestedA))
	partitionLiveCheckFlows(t, offered, requestedB, offeredKey, partitionLiveRouteKey(t, ddsTopic, requestedB))

	cycloneOffered := []string(nil)
	if !dds.PartitionsMatch(nil, cycloneOffered) {
		t.Fatalf("default Cyclone offer missed default Weld request")
	}
}

// partitionLiveRouteKey returns the discovery route key or fails the test.
func partitionLiveRouteKey(t *testing.T, ddsTopic string, partitions []string) string {
	t.Helper()

	key, err := dds.PartitionRouteKey(ddsTopic, partitions)
	if err != nil {
		t.Fatalf("route key: %v", err)
	}

	return key
}

// partitionLiveCheckIsolated pins discovery isolation between partitions.
func partitionLiveCheckIsolated(t *testing.T, offered, requestedA []string, offeredKey, requestedAKey string) {
	t.Helper()

	outcome := dds.MatchPartitions(offered, requestedA)
	if outcome.Matched {
		t.Fatalf("partition A reader matched partition B writer, want isolation: %+v", outcome)
	}
	if outcome.Reason != dds.PartitionMismatchNoCommonPartition {
		t.Fatalf("isolation reason = %q, want %q", outcome.Reason, dds.PartitionMismatchNoCommonPartition)
	}
	if offeredKey == requestedAKey {
		t.Fatalf("partitions A and B share discovery route key")
	}
}

// partitionLiveCheckFlows pins discovery flow for matching partitions.
func partitionLiveCheckFlows(t *testing.T, offered, requestedB []string, offeredKey, requestedBKey string) {
	t.Helper()

	flow := dds.MatchPartitions(offered, requestedB)
	if !flow.Matched {
		t.Fatalf("partition B reader missed partition B writer: %+v", flow)
	}
	if offeredKey != requestedBKey {
		t.Fatalf("matching partitions differ in discovery route key")
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
	raw, err := rtps.EncodeParams([]rtps.Param{param})
	if err != nil {
		t.Fatalf("encode partition param: %v", err)
	}
	return raw
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
