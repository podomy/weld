// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package ros_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/podomy/weld/internal/transports/dds"
	"github.com/podomy/weld/testbed/live/helpers"
)

// ROS params live gate constants for timeouts.
//
// Each test allocates one exclusive domain from the parallel pool, so
// per-run node names plus the allocated domain isolate runs. Timeouts
// bound every docker call through context.
const (
	paramsTimeout   = 2 * time.Minute
	paramsSettle    = 6 * time.Second
	paramsCleanWait = 10 * time.Second
	paramsProbeWait = 5 * time.Second
	paramsPollWait  = 20 * time.Second
	paramsPollStep  = 500 * time.Millisecond
	paramsType      = "rcl_interfaces/srv/GetParameters"

	paramsGetIDLBase       = "rcl_interfaces__srv__GetParameters"
	paramsSetAtomicIDLBase = "rcl_interfaces__srv__SetParametersAtomically"

	paramsCallTimeout = 15 * time.Second
	paramsPubRetry    = 10 * time.Second
	paramsPubGap      = 200 * time.Millisecond
)

// paramsNodeName returns a per-run ROS node name for the live ID.
//
// Hyphens and spaces map to underscores to keep the name valid for ros2
// service names. The weld prefix scopes the node to this gate.
func paramsNodeName(live string) string {
	sanitized := strings.ReplaceAll(live, "-", "_")
	sanitized = strings.ReplaceAll(sanitized, " ", "_")
	return "weld_" + sanitized
}

// paramsNodeScript returns a python3 -c script for a parameter node.
//
// The node declares gate_int, gate_double, gate_string, and gate_bool
// plus gate_ranged with integer range 0 through 10. Marker scopes the
// string value to this run so read-back proves per-run isolation.
func paramsNodeScript(node, marker string) string {
	return "import rclpy; " +
		"from rclpy.node import Node; " +
		"from rcl_interfaces.msg import ParameterDescriptor, IntegerRange; " +
		"rclpy.init(); " +
		"node = Node('" + node + "'); " +
		"node.declare_parameter('gate_int', 7); " +
		"node.declare_parameter('gate_double', 3.25); " +
		"node.declare_parameter('gate_string', '" + marker + "'); " +
		"node.declare_parameter('gate_bool', True); " +
		"node.declare_parameter('gate_ranged', 5, " +
		"ParameterDescriptor(integer_range=[IntegerRange(from_value=0, to_value=10, step=0)])); " +
		"rclpy.spin(node)"
}

// runPeerRos runs one ros2 argv in the peer and returns combined output.
//
// Domain is the allocated exclusive domain plumbed to DockerArgs, so
// parallel workers never share a domain. It wraps docker failures with
// context so callers fail loudly with the argv and the peer output. It
// exists only for service list polling; parameter reads and writes go
// through the Weld backend DDS path.
func runPeerRos(ctx context.Context, domain int, rosArgs ...string) (string, error) {
	args := helpers.DockerArgs(helpers.Container, domain, rosArgs...)
	out, err := helpers.DockerExec(ctx, args...)
	if err != nil {
		return string(out), fmt.Errorf("peer ros %v: %w", rosArgs, err)
	}
	return string(out), nil
}

// startParamsNode starts a per-run rclpy parameter node in the peer.
//
// Domain is the allocated exclusive domain plumbed to StartPeerProc and
// StopPeerProc, so parallel workers never share a domain. It uses
// StartPeerProc with pidfile plus scoped pkill cleanup, then settles
// before any service call so discovery quiesces. A cancelled context fails
// loudly instead of probing a half started node.
func startParamsNode(t *testing.T, ctx context.Context, domain int, live, node, marker string) {
	t.Helper()
	script := paramsNodeScript(node, marker)
	err := helpers.StartPeerProc(ctx, helpers.Container, domain, live, "python3", "-c", script)
	if err != nil {
		t.Fatalf("start params node %q: %v", node, err)
	}
	pidPath := helpers.HostPidPath(t, live)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.WithoutCancel(ctx), paramsCleanWait)
		defer cleanupCancel()
		helpers.StopPeerProcAndWait(cleanupCtx, helpers.Container, domain, pidPath, live)
	})
	helpers.Sleep(ctx, paramsSettle)
	if err := ctx.Err(); err != nil {
		t.Fatalf("context ended before param services: %v", err)
	}
}

// paramsServices returns the six standard service names for a node.
//
// It uses dds.ParamServiceNames so the gate pins the same suffix mapping
// as the transport params table.
func paramsServices(t *testing.T, node string) dds.ParamServices {
	t.Helper()
	services, err := dds.ParamServiceNames("/" + node)
	if err != nil {
		t.Fatalf("param service names for %q: %v", node, err)
	}
	return services
}

// waitForParamServices polls ros2 service list until all six names appear.
//
// It fails loudly on timeout so a missing peer service never passes as a
// skip. Discovery needs time after settle, so polling absorbs slow starts
// without masking assertion failures.
func waitForParamServices(t *testing.T, ctx context.Context, domain int, services dds.ParamServices) {
	t.Helper()
	want := []string{services.Describe, services.Get, services.GetTypes, services.List, services.Set, services.SetAtomically}
	deadline := time.Now().Add(paramsPollWait)
	for {
		out, err := runPeerRos(ctx, domain, helpers.RosBin, "service", "list")
		if err != nil {
			t.Fatalf("service list: %v", err)
		}
		missing := false
		for _, name := range want {
			if !strings.Contains(out, name) {
				missing = true
				break
			}
		}
		if !missing {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("param services missing for node, want %v, got:\n%s", want, out)
		}
		if ctx.Err() != nil {
			t.Fatalf("context ended waiting for param services: %v", ctx.Err())
		}
		helpers.Sleep(ctx, paramsPollStep)
	}
}

// paramsGUID returns a fixed per-test 8 byte GUID from a base.
//
// It fills bytes sequentially from base so each test carries a distinct
// key while seq separates calls within a test.
func paramsGUID(base byte) [8]byte {
	var guid [8]byte
	for i := range guid {
		guid[i] = base + byte(i)
	}
	return guid
}

// weldGetParameters reads parameters through the Weld backend DDS path.
//
// It subscribes to the Get reply, derives the request writer GUID, then
// republishes the request until the matching GUID and seq reply arrives.
// Republishing absorbs slow ROS discovery. Undecodable or mismatched
// samples are skipped. A missing reply by the call deadline fails loudly.
//
//nolint:cyclop,gocognit // live get path asserts declare, encode, publish, take, and decode in one run
func weldGetParameters(t *testing.T, ctx context.Context, domain int, backend *dds.Backend, getService string, guid [8]byte, seq uint64, names []string) []dds.ParamValue {
	t.Helper()
	err := backend.SubscribeService(ctx, getService, paramsGetIDLBase, dds.ServiceRoleClient, "")
	if err != nil {
		t.Fatalf("subscribe param get reply %q: %v", getService, err)
	}
	wireReq, err := dds.EncodeGetParametersRequest(names, guid, seq, true)
	if err != nil {
		t.Fatalf("encode get parameters %q: %v", getService, err)
	}
	callCtx, callCancel := context.WithTimeout(ctx, paramsCallTimeout)
	defer callCancel()
	repub := time.NewTimer(0)
	defer repub.Stop()
	var pubs, noRoutes, takeTimeouts, takeErrs, wrongTopic, decodeFails, guidMismatch int
	for {
		select {
		case <-callCtx.Done():
			out, lerr := runPeerRos(ctx, domain, helpers.RosBin, "service", "list")
			if lerr != nil {
				t.Logf("weld get %q service list probe failed: %v", getService, lerr)
			} else {
				t.Logf("weld get %q service list on failure:\n%s", getService, out)
			}
			t.Logf("weld get %q stats pubs=%d noRoute=%d takeTimeout=%d takeErr=%d wrongTopic=%d decodeFail=%d guidMismatch=%d", getService, pubs, noRoutes, takeTimeouts, takeErrs, wrongTopic, decodeFails, guidMismatch)
			t.Fatalf("weld get parameters %q timed out after %v: %v", getService, paramsCallTimeout, callCtx.Err())
		case <-repub.C:
		}
		pubs++
		err = backend.PublishService(callCtx, getService, paramsGetIDLBase, dds.ServiceRoleClient, wireReq, "")
		if err != nil {
			if errors.Is(err, dds.ErrNoRoute) {
				noRoutes++
			} else {
				t.Fatalf("publish get parameters %q: %v", getService, err)
			}
		}
		repub.Reset(500 * time.Millisecond)
		takeCtx, takeCancel := context.WithTimeout(callCtx, 500*time.Millisecond)
		incoming, err := backend.TakeBytes(takeCtx)
		takeCancel()
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
				takeTimeouts++
				continue
			}
			if svcIsSkippableTake(err) {
				takeErrs++
				continue
			}
			t.Fatalf("take get parameters reply %q: %v", getService, err)
		}
		if incoming.TopicName != "" && incoming.TopicName != getService {
			wrongTopic++
			continue
		}
		values, gotGUID, gotSeq, err := dds.DecodeGetParametersResponse(incoming.Payload)
		if err != nil {
			decodeFails++
			continue
		}
		if gotGUID != guid || gotSeq != seq {
			guidMismatch++
			continue
		}
		return values
	}
}

// weldSetAtomically writes parameters through the Weld backend DDS path.
//
// It subscribes to the SetAtomically reply topic with the atomic
// response type, publishes one EncodeSetAtomicallyRequest to the
// SetAtomically request topic, then takes until the matching GUID and
// seq reply arrives and decodes it. Undecodable or mismatched samples
// are skipped. A missing reply by the call deadline fails loudly.
// weldSetAtomically writes parameters through the Weld backend DDS path.
//
// It subscribes to the SetAtomically reply, then republishes the request
// until the matching GUID and seq reply arrives. Republishing absorbs
// slow ROS discovery. Undecodable or mismatched samples are skipped. A
// missing reply by the call deadline fails loudly.
//
//nolint:cyclop,gocognit // live set path asserts declare, encode, publish, take, and decode in one run
func weldSetAtomically(t *testing.T, ctx context.Context, backend *dds.Backend, atomicService string, guid [8]byte, seq uint64, params []dds.Parameter) dds.SetParametersResult {
	t.Helper()
	err := backend.SubscribeService(ctx, atomicService, paramsSetAtomicIDLBase, dds.ServiceRoleClient, "")
	if err != nil {
		t.Fatalf("subscribe param atomic reply %q: %v", atomicService, err)
	}
	wireReq, err := dds.EncodeSetAtomicallyRequest(params, guid, seq, true)
	if err != nil {
		t.Fatalf("encode set atomically %q: %v", atomicService, err)
	}
	callCtx, callCancel := context.WithTimeout(ctx, paramsCallTimeout)
	defer callCancel()
	repub := time.NewTimer(0)
	defer repub.Stop()
	for {
		select {
		case <-callCtx.Done():
			t.Fatalf("weld set atomically %q timed out after %v: %v", atomicService, paramsCallTimeout, callCtx.Err())
		case <-repub.C:
		}
		err = backend.PublishService(callCtx, atomicService, paramsSetAtomicIDLBase, dds.ServiceRoleClient, wireReq, "")
		if err != nil && !errors.Is(err, dds.ErrNoRoute) {
			t.Fatalf("publish set atomically %q: %v", atomicService, err)
		}
		repub.Reset(500 * time.Millisecond)
		takeCtx, takeCancel := context.WithTimeout(callCtx, 500*time.Millisecond)
		incoming, err := backend.TakeBytes(takeCtx)
		takeCancel()
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
				continue
			}
			if svcIsSkippableTake(err) {
				continue
			}
			t.Fatalf("take set atomically reply %q: %v", atomicService, err)
		}
		if incoming.TopicName != "" && incoming.TopicName != atomicService {
			continue
		}
		result, gotGUID, gotSeq, err := dds.DecodeSetAtomicallyResponse(incoming.Payload)
		if err != nil {
			continue
		}
		if gotGUID != guid || gotSeq != seq {
			continue
		}
		return result
	}
}

// TestROSParamsDeclareReadBack checks declare plus read-back for four scalars.
//
// It starts a per-run rclpy node declaring int, double, string, and bool,
// waits for the six standard parameter services, then reads all four back
// through the Weld backend GetParameters DDS path. Missing services or
// wrong values fail loudly.
func TestROSParamsDeclareReadBack(t *testing.T) {
	t.Parallel()
	domain := helpers.MustAllocateDomain(t)
	ctx, cancel := context.WithTimeout(context.Background(), paramsTimeout)
	defer cancel()
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)
	helpers.SkipIfNoTypeInterface(t, ctx, helpers.Container, domain, helpers.RosBin, paramsType, paramsProbeWait)
	live := helpers.LiveID("params")
	node := paramsNodeName(live)
	startParamsNode(t, ctx, domain, live, node, live)
	services := paramsServices(t, node)
	waitForParamServices(t, ctx, domain, services)
	backend := helpers.MustOpenLiveBackend(t, ctx, uint32(domain), "params-readback-"+live, "params-readback") //nolint:gosec // G115: pooled domain 70 through 101 fits uint32
	guid := paramsGUID(1)
	values := weldGetParameters(t, ctx, domain, backend, services.Get, guid, 1, []string{"gate_int", "gate_double", "gate_string", "gate_bool"})
	if len(values) != 4 {
		t.Fatalf("get parameters count = %d, want 4", len(values))
	}
	if values[0].Type != dds.ParamInteger || values[0].Integer != 7 {
		t.Fatalf("gate_int = type %d value %d, want type %d value 7", values[0].Type, values[0].Integer, dds.ParamInteger)
	}
	if values[1].Type != dds.ParamDouble || values[1].Double != 3.25 {
		t.Fatalf("gate_double = type %d value %v, want type %d value 3.25", values[1].Type, values[1].Double, dds.ParamDouble)
	}
	if values[2].Type != dds.ParamString || values[2].String != live {
		t.Fatalf("gate_string = type %d value %q, want type %d value %q", values[2].Type, values[2].String, dds.ParamString, live)
	}
	if values[3].Type != dds.ParamBool || values[3].Bool != true {
		t.Fatalf("gate_bool = type %d value %v, want type %d value true", values[3].Type, values[3].Bool, dds.ParamBool)
	}
}

// TestROSParamsAtomicSetReadBack checks one atomic set plus re-read.
//
// It starts a per-run node, records gate_int as 7 through the Weld Get
// path, sets it to 99 through the Weld SetParametersAtomically path,
// then re-reads through the Weld Get path. A rejected set or a stale
// re-read fails loudly.
func TestROSParamsAtomicSetReadBack(t *testing.T) {
	t.Parallel()
	domain := helpers.MustAllocateDomain(t)
	ctx, cancel := context.WithTimeout(context.Background(), paramsTimeout)
	defer cancel()
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)
	helpers.SkipIfNoTypeInterface(t, ctx, helpers.Container, domain, helpers.RosBin, paramsType, paramsProbeWait)
	live := helpers.LiveID("params")
	node := paramsNodeName(live)
	startParamsNode(t, ctx, domain, live, node, live)
	services := paramsServices(t, node)
	waitForParamServices(t, ctx, domain, services)
	backend := helpers.MustOpenLiveBackend(t, ctx, uint32(domain), "params-atomic-"+live, "params-atomic") //nolint:gosec // G115: pooled domain 70 through 101 fits uint32
	guid := paramsGUID(11)
	before := weldGetParameters(t, ctx, domain, backend, services.Get, guid, 1, []string{"gate_int"})
	if len(before) != 1 {
		t.Fatalf("initial get count = %d, want 1", len(before))
	}
	if before[0].Type != dds.ParamInteger || before[0].Integer != 7 {
		t.Fatalf("initial gate_int = type %d value %d, want type %d value 7", before[0].Type, before[0].Integer, dds.ParamInteger)
	}
	setParams := []dds.Parameter{{Name: "gate_int", Value: dds.ParamValue{Type: dds.ParamInteger, Integer: 99}}}
	result := weldSetAtomically(t, ctx, backend, services.SetAtomically, guid, 2, setParams)
	if !result.Successful {
		t.Fatalf("atomic set of gate_int to 99 rejected: %q", result.Reason)
	}
	after := weldGetParameters(t, ctx, domain, backend, services.Get, guid, 3, []string{"gate_int"})
	if len(after) != 1 {
		t.Fatalf("re-read get count = %d, want 1", len(after))
	}
	if after[0].Type != dds.ParamInteger || after[0].Integer != 99 {
		t.Fatalf("re-read gate_int = type %d value %d, want type %d value 99", after[0].Type, after[0].Integer, dds.ParamInteger)
	}
}

// TestROSParamsValidationRejected checks out-of-range set is rejected.
//
// It starts a per-run node with gate_ranged in 0 through 10, reads 5
// through the Weld Get path, attempts 99 through the Weld
// SetParametersAtomically path, requires successful false with a
// non-empty reason, then re-reads through the Weld Get path to prove the
// value stayed at 5 and was not silently accepted.
func TestROSParamsValidationRejected(t *testing.T) {
	t.Parallel()
	domain := helpers.MustAllocateDomain(t)
	ctx, cancel := context.WithTimeout(context.Background(), paramsTimeout)
	defer cancel()
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)
	helpers.SkipIfNoTypeInterface(t, ctx, helpers.Container, domain, helpers.RosBin, paramsType, paramsProbeWait)
	live := helpers.LiveID("params")
	node := paramsNodeName(live)
	startParamsNode(t, ctx, domain, live, node, live)
	services := paramsServices(t, node)
	waitForParamServices(t, ctx, domain, services)
	backend := helpers.MustOpenLiveBackend(t, ctx, uint32(domain), "params-validation-"+live, "params-validation") //nolint:gosec // G115: pooled domain 70 through 101 fits uint32
	guid := paramsGUID(21)
	before := weldGetParameters(t, ctx, domain, backend, services.Get, guid, 1, []string{"gate_ranged"})
	if len(before) != 1 {
		t.Fatalf("initial get count = %d, want 1", len(before))
	}
	if before[0].Type != dds.ParamInteger || before[0].Integer != 5 {
		t.Fatalf("initial gate_ranged = type %d value %d, want type %d value 5", before[0].Type, before[0].Integer, dds.ParamInteger)
	}
	setParams := []dds.Parameter{{Name: "gate_ranged", Value: dds.ParamValue{Type: dds.ParamInteger, Integer: 99}}}
	result := weldSetAtomically(t, ctx, backend, services.SetAtomically, guid, 2, setParams)
	if result.Successful {
		t.Fatalf("out-of-range set of gate_ranged to 99 succeeded, want rejection")
	}
	if strings.TrimSpace(result.Reason) == "" {
		t.Fatalf("rejected set misses non-empty reason")
	}
	after := weldGetParameters(t, ctx, domain, backend, services.Get, guid, 3, []string{"gate_ranged"})
	if len(after) != 1 {
		t.Fatalf("re-read get count = %d, want 1", len(after))
	}
	if after[0].Type != dds.ParamInteger || after[0].Integer != 5 {
		t.Fatalf("gate_ranged after rejected set = type %d value %d, want type %d value 5", after[0].Type, after[0].Integer, dds.ParamInteger)
	}
}
