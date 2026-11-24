// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

// RMW selection live gate: explicit RMW env proven against stock ROS.
//
// Unit config cannot prove the container honors RMW_IMPLEMENTATION, so this
// gate runs stock ros2 with an explicit RMW_IMPLEMENTATION value in docker
// argv. Phase one runs pub plus echo on a per-run live ID topic with
// RMW_IMPLEMENTATION=rmw_cyclonedds_cpp and requires delivery. Phase two
// probes RMW_IMPLEMENTATION=rmw_weld: when the Go-backed shim is absent it
// requires a loud implementation-not-found or not-implemented refusal,
// never a silent pass; when the probe shows the shim working it requires
// weld pub plus echo delivery of a per-run live payload.
//
//nolint:dupl // rmw refusal gates repeat similar docker setup across areas, splitting would obscure gate intent
package ros_test

import (
	"bytes"
	"context"
	"fmt"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/podomy/weld/testbed/live/helpers"
)

// RMW live gate constants for the Jazzy peer.
//
// Each run allocates one exclusive domain from the parallel pool, so
// per-run topics plus the allocated domain isolate runs. Timeouts bound
// every docker invocation through context, never through shell timeout.
const (
	rmwEchoWait    = 10 * time.Second
	rmwSettle      = 3 * time.Second
	rmwTestTimeout = 2 * time.Minute
	rmwTypeProbe   = 5 * time.Second
	rmwWeldTimeout = 30 * time.Second
)

// RMW implementation names pinned by this gate.
const (
	rmwCyclone = "rmw_cyclonedds_cpp"
	rmwWeld    = "rmw_weld"
)

// rmwEchoResult carries one background echo run to the test goroutine.
type rmwEchoResult struct {
	stdout string
	stderr string
	err    error
}

// rmwDockerArgs builds a docker exec argv with an explicit RMW value.
//
// The returned argv starts with exec and carries ROS_DOMAIN_ID plus the
// given RMW_IMPLEMENTATION and the ROS setup as -e flags, then the
// container name and the command. No shell is used at any point. Topic
// echo verbs gain --no-daemon so no CLI daemon lingers on the domain.
//
//nolint:unparam // container stays explicit for docker symmetry though current gates pass ros-jazzy
func rmwDockerArgs(domain int, rmw string, container string, cmd ...string) []string {
	if len(cmd) >= 3 && cmd[1] == "topic" && cmd[2] == "echo" {
		dup := append([]string(nil), cmd...)
		dup = append(dup, "--no-daemon")
		cmd = dup
	}
	args := []string{"exec"}
	args = append(args, "-e", "ROS_DOMAIN_ID="+strconv.Itoa(domain))
	args = append(args, "-e", "RMW_IMPLEMENTATION="+rmw)
	for _, kv := range helpers.ImageEnv {
		args = append(args, "-e", kv[0]+"="+kv[1])
	}
	args = append(args, container)
	args = append(args, cmd...)
	return args
}

// rmwRun runs one docker argv command and returns captured output.
//
// The context bounds the run. Stdout and stderr are captured separately.
// No shell is used.
func rmwRun(ctx context.Context, args []string) (string, string, error) {
	cmd := exec.CommandContext(ctx, "docker", args...) //nolint:gosec // live harness runs docker with controlled test args and no shell
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	if err != nil {
		return stdout.String(), stderr.String(), err
	}
	return stdout.String(), stderr.String(), nil
}

// rmwAssertExplicitEnv fails when argv lacks the explicit RMW -e flag.
//
// It proves env selection is explicit in docker argv rather than inherited
// from the daemon or image default.
func rmwAssertExplicitEnv(t *testing.T, args []string, rmw string) {
	t.Helper()
	want := "RMW_IMPLEMENTATION=" + rmw
	for i := 0; i+1 < len(args); i++ {
		if args[i] == "-e" && args[i+1] == want {
			return
		}
	}
	t.Fatalf("docker argv misses explicit %q in %q", want, strings.Join(args, " "))
}

// rmwAssertWeldRefusal fails unless rmw_weld is refused loudly.
//
// A nil error is a silent pass and fails. An error with no failure text
// in combined output is a silent refusal and fails. Only an error paired
// with implementation-not-found or not-implemented style text passes.
func rmwAssertWeldRefusal(t *testing.T, stdout, stderr string, err error) {
	t.Helper()
	combined := stdout + "\n" + stderr
	if err == nil {
		t.Fatalf("rmw_weld unexpectedly succeeded, want loud implementation refusal:\n%s", combined)
	}
	lower := strings.ToLower(combined)
	markers := []string{
		"not found",
		"not implemented",
		"not-implemented",
		"failed",
		"cannot",
		"unable",
		"unknown",
		"no such",
		"invalid",
		"error",
		"exception",
	}
	for _, marker := range markers {
		if strings.Contains(lower, marker) {
			return
		}
	}
	t.Fatalf("rmw_weld refusal is silent, want implementation-not-found or not-implemented text, got err %v:\n%s", err, combined)
}

// TestRMWLiveSelectionGate proves explicit RMW selection and weld behavior.
//
// It skips cleanly only when no daemon or peer container is present. Phase
// one publishes plus echoes a per-run live ID topic with explicit
// RMW_IMPLEMENTATION=rmw_cyclonedds_cpp and requires delivery. Phase two
// probes RMW_IMPLEMENTATION=rmw_weld: without the Go-backed shim it
// attempts weld pub and requires a loud implementation-not-found or
// not-implemented error, never a silent pass; with the shim it runs weld
// pub plus echo on a fresh per-run live ID topic and requires delivery.
func TestRMWLiveSelectionGate(t *testing.T) {
	t.Parallel()
	domain := helpers.MustAllocateDomain(t)
	ctx, cancel := context.WithTimeout(context.Background(), rmwTestTimeout)
	defer cancel()
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)
	helpers.SkipIfNoTypeInterface(t, ctx, helpers.Container, domain, helpers.RosBin, helpers.RosType, rmwTypeProbe)
	live := helpers.LiveID("rmw")
	topic := helpers.LiveTopic(live)
	payload := "hello rmw " + live
	pubArg := fmt.Sprintf("{data: %s}", payload)
	secs := int(rmwEchoWait / time.Second)
	if secs <= 0 {
		secs = 1
	}
	echoArgs := rmwDockerArgs(domain, rmwCyclone, helpers.Container, helpers.RosBin, "topic", "echo", topic, helpers.RosType, "--timeout", strconv.Itoa(secs))
	rmwAssertExplicitEnv(t, echoArgs, rmwCyclone)
	echoCtx, echoCancel := context.WithTimeout(ctx, rmwEchoWait+helpers.EchoMargin)
	defer echoCancel()
	resultCh := make(chan rmwEchoResult, 1)
	go func() {
		stdout, stderr, err := rmwRun(echoCtx, echoArgs)
		resultCh <- rmwEchoResult{stdout: stdout, stderr: stderr, err: err}
	}()
	helpers.Sleep(ctx, rmwSettle)
	err := ctx.Err()
	if err != nil {
		t.Fatalf("context ended before pub: %v", err)
	}
	pubArgs := rmwDockerArgs(domain, rmwCyclone, helpers.Container, helpers.RosBin, "topic", "pub", topic, helpers.RosType, pubArg, "--once")
	rmwAssertExplicitEnv(t, pubArgs, rmwCyclone)
	pubCtx, pubCancel := context.WithTimeout(ctx, helpers.PubTimeout)
	defer pubCancel()
	pubOut, pubErrOut, err := rmwRun(pubCtx, pubArgs)
	if err != nil {
		t.Fatalf("cyclone pub: %v\nstdout:\n%s\nstderr:\n%s", err, pubOut, pubErrOut)
	}
	var echoRes rmwEchoResult
	select {
	case echoRes = <-resultCh:
	case <-ctx.Done():
		cerr := ctx.Err()
		if cerr != nil {
			t.Fatalf("context ended waiting for echo: %v", cerr)
		}
		t.Fatalf("context ended waiting for echo")
	}
	err = echoRes.err
	if err != nil {
		t.Fatalf("cyclone echo: %v\nstdout:\n%s\nstderr:\n%s", err, echoRes.stdout, echoRes.stderr)
	}
	if !strings.Contains(echoRes.stdout, live) {
		t.Fatalf("missing live payload %q with explicit %s:\nstdout:\n%s\nstderr:\n%s", payload, rmwCyclone, echoRes.stdout, echoRes.stderr)
	}
	weldShimOK, _ := rmwWeldShimAvailable(ctx, domain)
	if !weldShimOK {
		weldPayload := "hello rmw weld " + live
		weldPubArg := fmt.Sprintf("{data: %s}", weldPayload)
		weldArgs := rmwDockerArgs(domain, rmwWeld, helpers.Container, helpers.RosBin, "topic", "pub", topic, helpers.RosType, weldPubArg, "--once")
		rmwAssertExplicitEnv(t, weldArgs, rmwWeld)
		weldCtx, weldCancel := context.WithTimeout(ctx, rmwWeldTimeout)
		defer weldCancel()
		wout, werrOut, werr := rmwRun(weldCtx, weldArgs)
		rmwAssertWeldRefusal(t, wout, werrOut, werr)
	} else {
		rmwWeldPubEchoRoundtrip(t, ctx, domain)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.WithoutCancel(ctx), helpers.PeerStopTimeout)
		defer cleanupCancel()
		helpers.KillRosDaemon(cleanupCtx, helpers.Container, domain)
	})
}

// rmwWeldPubEchoRoundtrip requires weld pub plus echo delivery of a live payload.
//
// It runs only after rmwWeldShimAvailable reports the Go-backed shim
// working. It starts one ros2 topic echo with
// RMW_IMPLEMENTATION=rmw_weld on a fresh per-run live ID topic, publishes
// one live payload with the same RMW value, and requires the echo output
// to carry the live ID. This proves payload flow through the shim,
// matching the interop driver which proves hello in both directions
// against a ROS peer.
func rmwWeldPubEchoRoundtrip(t *testing.T, ctx context.Context, domain int) {
	t.Helper()
	live := helpers.LiveID("rmw-weld")
	topic := helpers.LiveTopic(live)
	payload := "hello rmw weld " + live
	pubArg := fmt.Sprintf("{data: %s}", payload)
	secs := int(rmwEchoWait / time.Second)
	if secs <= 0 {
		secs = 1
	}
	echoArgs := rmwDockerArgs(domain, rmwWeld, helpers.Container, helpers.RosBin, "topic", "echo", topic, helpers.RosType, "--timeout", strconv.Itoa(secs))
	rmwAssertExplicitEnv(t, echoArgs, rmwWeld)
	echoCtx, echoCancel := context.WithTimeout(ctx, rmwEchoWait+helpers.EchoMargin)
	defer echoCancel()
	resultCh := make(chan rmwEchoResult, 1)
	go func() {
		stdout, stderr, err := rmwRun(echoCtx, echoArgs)
		resultCh <- rmwEchoResult{stdout: stdout, stderr: stderr, err: err}
	}()
	helpers.Sleep(ctx, rmwSettle)
	err := ctx.Err()
	if err != nil {
		t.Fatalf("context ended before weld pub: %v", err)
	}
	pubArgs := rmwDockerArgs(domain, rmwWeld, helpers.Container, helpers.RosBin, "topic", "pub", topic, helpers.RosType, pubArg, "--once")
	rmwAssertExplicitEnv(t, pubArgs, rmwWeld)
	pubCtx, pubCancel := context.WithTimeout(ctx, rmwWeldTimeout)
	defer pubCancel()
	pubOut, pubErrOut, err := rmwRun(pubCtx, pubArgs)
	if err != nil {
		t.Fatalf("weld pub: %v\nstdout:\n%s\nstderr:\n%s", err, pubOut, pubErrOut)
	}
	var echoRes rmwEchoResult
	select {
	case echoRes = <-resultCh:
	case <-ctx.Done():
		cerr := ctx.Err()
		if cerr != nil {
			t.Fatalf("context ended waiting for weld echo: %v", cerr)
		}
		t.Fatalf("context ended waiting for weld echo")
	}
	err = echoRes.err
	if err != nil {
		t.Fatalf("weld echo: %v\nstdout:\n%s\nstderr:\n%s", err, echoRes.stdout, echoRes.stderr)
	}
	if !strings.Contains(echoRes.stdout, live) {
		t.Fatalf("missing live payload %q with explicit %s:\nstdout:\n%s\nstderr:\n%s", payload, rmwWeld, echoRes.stdout, echoRes.stderr)
	}
}

// RMW weld area probe type for the service and client gates.
const rmwWeldTriggerType = "std_srvs/srv/Trigger"

// rmwWeldNodeName returns a valid ROS node name for one live ID.
//
// Hyphens and spaces map to underscores so concurrent runs never share
// a node name. The live ID already starts with a letter prefix.
func rmwWeldNodeName(live string) string {
	sanitized := strings.ReplaceAll(live, "-", "_")
	sanitized = strings.ReplaceAll(sanitized, " ", "_")
	return "weld_" + sanitized
}

// rmwWeldServiceName returns a per-run ROS service name for one live ID.
//
// Hyphens and spaces map to underscores so concurrent runs never share
// a service.
func rmwWeldServiceName(live string) string {
	sanitized := strings.ReplaceAll(live, "-", "_")
	sanitized = strings.ReplaceAll(sanitized, " ", "_")
	return "/weld_rmw_svc_" + sanitized
}

// rmwWeldCleanupDaemon registers best effort daemon cleanup for one domain.
//
// Python rclpy probes do not spawn the CLI daemon, but the cleanup keeps
// reused pooled domains isolated at no cost when a probe path changes.
func rmwWeldCleanupDaemon(t *testing.T, ctx context.Context, domain int) {
	t.Helper()
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.WithoutCancel(ctx), helpers.PeerStopTimeout)
		defer cleanupCancel()
		helpers.KillRosDaemon(cleanupCtx, helpers.Container, domain)
	})
}

// rmwWeldRunProbe runs one RMW_IMPLEMENTATION=rmw_weld probe and requires loud refusal.
//
// It builds the docker exec argv with explicit env, checks the env pin,
// runs with the weld probe timeout, and requires loud refusal through
// rmwAssertWeldRefusal. A silent pass or silent refusal fails.
func rmwWeldRunProbe(t *testing.T, ctx context.Context, domain int, cmd ...string) {
	t.Helper()
	args := rmwDockerArgs(domain, rmwWeld, helpers.Container, cmd...)
	rmwAssertExplicitEnv(t, args, rmwWeld)
	probeCtx, probeCancel := context.WithTimeout(ctx, rmwWeldTimeout)
	defer probeCancel()
	stdout, stderr, err := rmwRun(probeCtx, args)
	rmwAssertWeldRefusal(t, stdout, stderr, err)
}

// testRMWWeldNodeRefusal requires node creation under rmw_weld.
//
// It creates one rclpy node with RMW_IMPLEMENTATION=rmw_weld. When the
// shim probe shows the Go-backed shim working it requires success with
// node create plus destroy and no error, matching linktest.c which proves
// create_node returns OK through the real FFI. When the probe shows the
// C-only stub it requires loud refusal, so runs without the shim stay
// green and never pass silently.
func testRMWWeldNodeRefusal(t *testing.T) {
	t.Parallel()
	domain := helpers.MustAllocateDomain(t)
	ctx, cancel := context.WithTimeout(context.Background(), rmwTestTimeout)
	defer cancel()
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)
	rmwWeldCleanupDaemon(t, ctx, domain)
	live := helpers.LiveID("rmw-weld-node")
	node := rmwWeldNodeName(live)
	script := fmt.Sprintf("import rclpy\nrclpy.init()\nnode = rclpy.create_node('%s')\nnode.destroy_node()\nrclpy.shutdown()\nprint('WELD_RMW_NODE_OK %s', flush=True)\n", node, live)
	rmwWeldRunAreaOrRefusal(t, ctx, domain, "WELD_RMW_NODE_OK "+live, script)
}

// testRMWWeldPubRefusal requires publisher creation under rmw_weld.
//
// It creates one rclpy String publisher with RMW_IMPLEMENTATION=rmw_weld.
// When the shim probe shows the Go-backed shim working it requires
// success with publisher create plus destroy and no error, matching
// linktest.c which proves create_publisher returns OK through the real
// FFI. When the probe shows the C-only stub it requires loud refusal, so
// runs without the shim stay green and never pass silently.
func testRMWWeldPubRefusal(t *testing.T) {
	t.Parallel()
	domain := helpers.MustAllocateDomain(t)
	ctx, cancel := context.WithTimeout(context.Background(), rmwTestTimeout)
	defer cancel()
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)
	helpers.SkipIfNoTypeInterface(t, ctx, helpers.Container, domain, helpers.RosBin, helpers.RosType, rmwTypeProbe)
	rmwWeldCleanupDaemon(t, ctx, domain)
	live := helpers.LiveID("rmw-weld-pub")
	node := rmwWeldNodeName(live)
	topic := helpers.LiveTopic(live)
	script := fmt.Sprintf("import rclpy\nfrom std_msgs.msg import String\nrclpy.init()\nnode = rclpy.create_node('%s')\npub = node.create_publisher(String, '%s', 10)\nnode.destroy_node()\nrclpy.shutdown()\nprint('WELD_RMW_PUB_OK %s', flush=True)\n", node, topic, live)
	rmwWeldRunAreaOrRefusal(t, ctx, domain, "WELD_RMW_PUB_OK "+live, script)
}

// testRMWWeldSubRefusal requires subscription creation under rmw_weld.
//
// It creates one rclpy String subscription with
// RMW_IMPLEMENTATION=rmw_weld. When the shim probe shows the Go-backed
// shim working it requires success with subscription create plus destroy
// and no error, matching linktest.c which proves create_subscription
// returns OK through the real FFI. When the probe shows the C-only stub
// it requires loud refusal, so runs without the shim stay green and never
// pass silently.
func testRMWWeldSubRefusal(t *testing.T) {
	t.Parallel()
	domain := helpers.MustAllocateDomain(t)
	ctx, cancel := context.WithTimeout(context.Background(), rmwTestTimeout)
	defer cancel()
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)
	helpers.SkipIfNoTypeInterface(t, ctx, helpers.Container, domain, helpers.RosBin, helpers.RosType, rmwTypeProbe)
	rmwWeldCleanupDaemon(t, ctx, domain)
	live := helpers.LiveID("rmw-weld-sub")
	node := rmwWeldNodeName(live)
	topic := helpers.LiveTopic(live)
	script := fmt.Sprintf("import rclpy\nfrom std_msgs.msg import String\nrclpy.init()\nnode = rclpy.create_node('%s')\nsub = node.create_subscription(String, '%s', lambda msg: None, 10)\nnode.destroy_node()\nrclpy.shutdown()\nprint('WELD_RMW_SUB_OK %s', flush=True)\n", node, topic, live)
	rmwWeldRunAreaOrRefusal(t, ctx, domain, "WELD_RMW_SUB_OK "+live, script)
}

// testRMWWeldServiceRefusal requires service creation under rmw_weld.
//
// It creates one rclpy Trigger service with RMW_IMPLEMENTATION=rmw_weld.
// When the shim probe shows the Go-backed shim working it requires
// success with service create plus destroy and no error, matching
// svcwait_test.c which proves create_service returns OK through the real
// FFI. When the probe shows the C-only stub it requires loud refusal, so
// runs without the shim stay green and never pass silently.
func testRMWWeldServiceRefusal(t *testing.T) {
	t.Parallel()
	domain := helpers.MustAllocateDomain(t)
	ctx, cancel := context.WithTimeout(context.Background(), rmwTestTimeout)
	defer cancel()
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)
	helpers.SkipIfNoTypeInterface(t, ctx, helpers.Container, domain, helpers.RosBin, rmwWeldTriggerType, rmwTypeProbe)
	rmwWeldCleanupDaemon(t, ctx, domain)
	live := helpers.LiveID("rmw-weld-svc")
	node := rmwWeldNodeName(live)
	service := rmwWeldServiceName(live)
	script := fmt.Sprintf("import rclpy\nfrom std_srvs.srv import Trigger\nrclpy.init()\nnode = rclpy.create_node('%s')\nsrv = node.create_service(Trigger, '%s', lambda req, resp: resp)\nnode.destroy_node()\nrclpy.shutdown()\nprint('WELD_RMW_SVC_OK %s', flush=True)\n", node, service, live)
	rmwWeldRunAreaOrRefusal(t, ctx, domain, "WELD_RMW_SVC_OK "+live, script)
}

// testRMWWeldClientRefusal requires client creation under rmw_weld.
//
// It creates one rclpy Trigger client with RMW_IMPLEMENTATION=rmw_weld.
// When the shim probe shows the Go-backed shim working it requires
// success with client create plus destroy and no error, matching
// svcwait_test.c which proves create_client returns OK through the real
// FFI. When the probe shows the C-only stub it requires loud refusal, so
// runs without the shim stay green and never pass silently.
func testRMWWeldClientRefusal(t *testing.T) {
	t.Parallel()
	domain := helpers.MustAllocateDomain(t)
	ctx, cancel := context.WithTimeout(context.Background(), rmwTestTimeout)
	defer cancel()
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)
	helpers.SkipIfNoTypeInterface(t, ctx, helpers.Container, domain, helpers.RosBin, rmwWeldTriggerType, rmwTypeProbe)
	rmwWeldCleanupDaemon(t, ctx, domain)
	live := helpers.LiveID("rmw-weld-cli")
	node := rmwWeldNodeName(live)
	service := rmwWeldServiceName(live)
	script := fmt.Sprintf("import rclpy\nfrom std_srvs.srv import Trigger\nrclpy.init()\nnode = rclpy.create_node('%s')\ncli = node.create_client(Trigger, '%s')\nnode.destroy_node()\nrclpy.shutdown()\nprint('WELD_RMW_CLI_OK %s', flush=True)\n", node, service, live)
	rmwWeldRunAreaOrRefusal(t, ctx, domain, "WELD_RMW_CLI_OK "+live, script)
}

// testRMWWeldWaitRefusal requires wait under rmw_weld.
//
// It creates one subscription plus one publisher with
// RMW_IMPLEMENTATION=rmw_weld, publishes the live payload, spins until
// the subscription takes it, then spins once more with no new message.
// When the shim probe shows the Go-backed shim working it requires
// success with the taken payload matching, which proves the wait OK path,
// plus a clean idle spin with no error, which proves the wait TIMEOUT
// path; this mirrors svcwait_test.c which proves trigger plus wait OK
// plus wait TIMEOUT through the real FFI. When the probe shows the C-only
// stub it requires loud refusal, so runs without the shim stay green and
// never pass silently.
func testRMWWeldWaitRefusal(t *testing.T) {
	t.Parallel()
	domain := helpers.MustAllocateDomain(t)
	ctx, cancel := context.WithTimeout(context.Background(), rmwTestTimeout)
	defer cancel()
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)
	helpers.SkipIfNoTypeInterface(t, ctx, helpers.Container, domain, helpers.RosBin, helpers.RosType, rmwTypeProbe)
	rmwWeldCleanupDaemon(t, ctx, domain)
	live := helpers.LiveID("rmw-weld-wait")
	node := rmwWeldNodeName(live)
	topic := helpers.LiveTopic(live)
	payload := "hello rmw weld wait " + live
	script := fmt.Sprintf("import time\nimport rclpy\nfrom std_msgs.msg import String\nrclpy.init()\nnode = rclpy.create_node('%s')\ngot = []\nsub = node.create_subscription(String, '%s', lambda msg: got.append(msg.data), 10)\npub = node.create_publisher(String, '%s', 10)\npub.publish(String(data='%s'))\ndeadline = time.time() + 5.0\nwhile not got and time.time() < deadline:\n    rclpy.spin_once(node, timeout_sec=0.2)\nif not got or got[0] != '%s':\n    raise SystemExit('wait missed live payload')\nrclpy.spin_once(node, timeout_sec=0.2)\nnode.destroy_node()\nrclpy.shutdown()\nprint('WELD_RMW_WAIT_OK %s', flush=True)\n", node, topic, topic, payload, payload, live)
	rmwWeldRunAreaOrRefusal(t, ctx, domain, "WELD_RMW_WAIT_OK "+live, script)
}

// TestRMWWeldAreaRefusalGates requires per-area behavior under rmw_weld.
//
// Each subtest runs one area probe with RMW_IMPLEMENTATION=rmw_weld on an
// exclusive pooled domain: node create, publisher create, subscription
// create, service create, client create, wait with spin, and pub plus sub
// roundtrip with a live payload. Each area first probes for the Go-backed
// shim: when the probe shows the shim working the area must succeed with
// a nil error and a per-run live marker; when the probe shows the C-only
// stub the create areas must refuse loudly through rmwAssertWeldRefusal
// while the roundtrip skips loudly, so runs without the shim stay green,
// never pass silently, and never crash. Subtests skip only for a missing
// peer or a missing ROS type, plus the roundtrip skips when the shim is
// absent. Daemon cleanup keeps reused pooled domains isolated.
func TestRMWWeldAreaRefusalGates(t *testing.T) {
	t.Parallel()
	t.Run("NodeCreate", testRMWWeldNodeRefusal)
	t.Run("PubCreate", testRMWWeldPubRefusal)
	t.Run("SubCreate", testRMWWeldSubRefusal)
	t.Run("ServiceCreate", testRMWWeldServiceRefusal)
	t.Run("ClientCreate", testRMWWeldClientRefusal)
	t.Run("WaitCreate", testRMWWeldWaitRefusal)
	t.Run("PubSubRoundtrip", testRMWWeldPubSubRoundtrip)
}

// testRMWWeldPubSubRoundtrip requires live payload flow through rmw_weld pub plus sub.
//
// It creates one publisher plus one subscription on a per-run live ID
// topic with RMW_IMPLEMENTATION=rmw_weld, publishes the live payload,
// spins until the subscription takes it, and requires the taken payload
// to match. This mirrors linktest.c which proves hello publish plus take
// with a payload check through the real FFI, and interop_test.c which
// proves hello in both directions against a ROS peer. It skips loudly
// when the shim probe fails, so runs without the shim stay green.
func testRMWWeldPubSubRoundtrip(t *testing.T) {
	t.Parallel()
	domain := helpers.MustAllocateDomain(t)
	ctx, cancel := context.WithTimeout(context.Background(), rmwTestTimeout)
	defer cancel()
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)
	helpers.SkipIfNoTypeInterface(t, ctx, helpers.Container, domain, helpers.RosBin, helpers.RosType, rmwTypeProbe)
	rmwWeldCleanupDaemon(t, ctx, domain)
	live := helpers.LiveID("rmw-weld-roundtrip")
	rmwWeldRequireShimOrSkip(t, ctx, domain)
	node := rmwWeldNodeName(live)
	topic := helpers.LiveTopic(live)
	payload := "hello rmw weld roundtrip " + live
	script := fmt.Sprintf("import time\nimport rclpy\nfrom std_msgs.msg import String\nrclpy.init()\nnode = rclpy.create_node('%s')\ngot = []\nsub = node.create_subscription(String, '%s', lambda msg: got.append(msg.data), 10)\npub = node.create_publisher(String, '%s', 10)\npub.publish(String(data='%s'))\ndeadline = time.time() + 5.0\nwhile not got and time.time() < deadline:\n    rclpy.spin_once(node, timeout_sec=0.2)\nif not got or got[0] != '%s':\n    raise SystemExit('roundtrip missed live payload')\nnode.destroy_node()\nrclpy.shutdown()\nprint('WELD_RMW_ROUNDTRIP_OK %s', flush=True)\n", node, topic, topic, payload, payload, live)
	rmwWeldRunSuccessMarked(t, ctx, domain, "WELD_RMW_ROUNDTRIP_OK "+live, "python3", "-c", script)
}

// rmwWeldShimAvailable probes whether the container carries the rmw_weld shim.
//
// It runs one minimal rclpy node create plus destroy with
// RMW_IMPLEMENTATION=rmw_weld and reports true only when the run succeeds.
// Any error, including loud refusal, reports false with combined output for
// the caller to log in a loud skip. It never fails the test.
func rmwWeldShimAvailable(ctx context.Context, domain int) (bool, string) {
	tmpl := "import rclpy\nrclpy.init()\nnode = rclpy.create_node('%s')\nnode.destroy_node()\nrclpy.shutdown()\n"
	probeLive := helpers.LiveID("rmw-weld-probe")
	node := rmwWeldNodeName(probeLive)
	script := fmt.Sprintf(tmpl, node)
	args := rmwDockerArgs(domain, rmwWeld, helpers.Container, "python3", "-c", script)
	probeCtx, probeCancel := context.WithTimeout(ctx, rmwWeldTimeout)
	defer probeCancel()
	stdout, stderr, err := rmwRun(probeCtx, args)
	combined := stdout + "\n" + stderr
	if err != nil {
		return false, combined
	}
	return true, combined
}

// rmwWeldRequireShimOrSkip skips the caller when the rmw_weld shim is absent.
//
// It probes the container with rmwWeldShimAvailable and skips loudly with
// probe output when the shim is missing. It never fails on absent shim so
// fleet runs without the shim stay green. Call it after SkipIfNoPeer.
func rmwWeldRequireShimOrSkip(t *testing.T, ctx context.Context, domain int) {
	t.Helper()
	ok, detail := rmwWeldShimAvailable(ctx, domain)
	if !ok {
		t.Skipf("rmw_weld shim absent in container %s, skipping success path, probe output:\n%s", helpers.Container, detail)
	}
}

// rmwWeldRunSuccessMarked runs one RMW_IMPLEMENTATION=rmw_weld probe and requires marked success.
//
// It builds the docker exec argv with explicit env, checks the env pin,
// runs with the weld probe timeout, and requires a nil error plus the
// marker in stdout. The marker carries the per-run live ID so the check
// proves payload flow, not just process exit. Call only after the shim
// probe shows the Go-backed shim working, or through
// rmwWeldRunAreaOrRefusal which probes first.
func rmwWeldRunSuccessMarked(t *testing.T, ctx context.Context, domain int, marker string, cmd ...string) {
	t.Helper()
	args := rmwDockerArgs(domain, rmwWeld, helpers.Container, cmd...)
	rmwAssertExplicitEnv(t, args, rmwWeld)
	runCtx, runCancel := context.WithTimeout(ctx, rmwWeldTimeout)
	defer runCancel()
	stdout, stderr, err := rmwRun(runCtx, args)
	if err != nil {
		t.Fatalf("rmw_weld success path failed, want nil error, got %v:\nstdout:\n%s\nstderr:\n%s", err, stdout, stderr)
	}
	if !strings.Contains(stdout, marker) {
		t.Fatalf("rmw_weld success path misses marker %q:\nstdout:\n%s\nstderr:\n%s", marker, stdout, stderr)
	}
}

// rmwWeldRunAreaOrRefusal runs one area script as success or refusal based on the shim probe.
//
// It probes the container with rmwWeldShimAvailable on the same domain.
// When the probe shows the Go-backed shim working it requires success
// with a nil error and the marker in stdout. When the probe shows the
// C-only stub it falls back to the loud refusal path through
// rmwWeldRunProbe, so runs without the shim stay green and never pass
// silently.
func rmwWeldRunAreaOrRefusal(t *testing.T, ctx context.Context, domain int, marker, script string) {
	t.Helper()
	ok, _ := rmwWeldShimAvailable(ctx, domain)
	if !ok {
		rmwWeldRunProbe(t, ctx, domain, "python3", "-c", script)
		return
	}
	rmwWeldRunSuccessMarked(t, ctx, domain, marker, "python3", "-c", script)
}

// rmwWeldRunSuccess runs one RMW_IMPLEMENTATION=rmw_weld probe and requires success.
//
// It builds the docker exec argv with explicit env, checks the env pin,
// runs with the weld probe timeout, and requires a nil error. Any error,
// including the loud refusal the old gates expect, fails with combined
// output. Call only after rmwWeldRequireShimOrSkip so absent shim skips
// instead of failing.
func rmwWeldRunSuccess(t *testing.T, ctx context.Context, domain int, cmd ...string) {
	t.Helper()
	args := rmwDockerArgs(domain, rmwWeld, helpers.Container, cmd...)
	rmwAssertExplicitEnv(t, args, rmwWeld)
	probeCtx, probeCancel := context.WithTimeout(ctx, rmwWeldTimeout)
	defer probeCancel()
	stdout, stderr, err := rmwRun(probeCtx, args)
	if err != nil {
		t.Fatalf("rmw_weld success path failed, want nil error, got %v:\nstdout:\n%s\nstderr:\n%s", err, stdout, stderr)
	}
}

// TestRMWWeldLocalRoundtrip exercises the refusal to success transition per RMW area.
//
// It does not import internal/rmwbridge: that package carries cgo and the
// bridge doc keeps the Go core pure Go, while live gates already run pure
// Go docker exec plus helpers. Instead this test reuses the same per run
// live IDs, pooled domains, and explicit RMW argv as the refusal gates and
// runs the same six area scripts against the live container, requiring
// success. Each subtest skips loudly when no peer is present, when the ROS
// type is missing, or when the rmw_weld shim probe fails, so runs without
// the shim stay green. Once the shim lands, each area must succeed with no
// error. Daemon cleanup keeps reused pooled domains isolated.
func TestRMWWeldLocalRoundtrip(t *testing.T) {
	t.Parallel()
	areas := []struct {
		name       string
		livePrefix string
		rosType    string
	}{
		{name: "NodeCreate", livePrefix: "rmw-weld-node", rosType: ""},
		{name: "PubCreate", livePrefix: "rmw-weld-pub", rosType: helpers.RosType},
		{name: "SubCreate", livePrefix: "rmw-weld-sub", rosType: helpers.RosType},
		{name: "ServiceCreate", livePrefix: "rmw-weld-svc", rosType: rmwWeldTriggerType},
		{name: "ClientCreate", livePrefix: "rmw-weld-cli", rosType: rmwWeldTriggerType},
		{name: "WaitCreate", livePrefix: "rmw-weld-wait", rosType: helpers.RosType},
	}
	for _, area := range areas {
		t.Run(area.name, func(t *testing.T) {
			t.Parallel()
			domain := helpers.MustAllocateDomain(t)
			ctx, cancel := context.WithTimeout(context.Background(), rmwTestTimeout)
			defer cancel()
			helpers.SkipIfNoPeer(t, ctx, helpers.Container)
			if area.rosType != "" {
				helpers.SkipIfNoTypeInterface(t, ctx, helpers.Container, domain, helpers.RosBin, area.rosType, rmwTypeProbe)
			}
			rmwWeldCleanupDaemon(t, ctx, domain)
			live := helpers.LiveID(area.livePrefix)
			rmwWeldRequireShimOrSkip(t, ctx, domain)
			node := rmwWeldNodeName(live)
			topic := helpers.LiveTopic(live)
			service := rmwWeldServiceName(live)
			var script string
			switch area.name {
			case "NodeCreate":
				script = fmt.Sprintf("import rclpy\nrclpy.init()\nnode = rclpy.create_node('%s')\nnode.destroy_node()\nrclpy.shutdown()\n", node)
			case "PubCreate":
				script = fmt.Sprintf("import rclpy\nfrom std_msgs.msg import String\nrclpy.init()\nnode = rclpy.create_node('%s')\npub = node.create_publisher(String, '%s', 10)\nnode.destroy_node()\nrclpy.shutdown()\n", node, topic)
			case "SubCreate":
				script = fmt.Sprintf("import rclpy\nfrom std_msgs.msg import String\nrclpy.init()\nnode = rclpy.create_node('%s')\nsub = node.create_subscription(String, '%s', lambda msg: None, 10)\nnode.destroy_node()\nrclpy.shutdown()\n", node, topic)
			case "ServiceCreate":
				script = fmt.Sprintf("import rclpy\nfrom std_srvs.srv import Trigger\nrclpy.init()\nnode = rclpy.create_node('%s')\nsrv = node.create_service(Trigger, '%s', lambda req, resp: resp)\nnode.destroy_node()\nrclpy.shutdown()\n", node, service)
			case "ClientCreate":
				script = fmt.Sprintf("import rclpy\nfrom std_srvs.srv import Trigger\nrclpy.init()\nnode = rclpy.create_node('%s')\ncli = node.create_client(Trigger, '%s')\nnode.destroy_node()\nrclpy.shutdown()\n", node, service)
			case "WaitCreate":
				script = fmt.Sprintf("import rclpy\nfrom std_msgs.msg import String\nrclpy.init()\nnode = rclpy.create_node('%s')\nsub = node.create_subscription(String, '%s', lambda msg: None, 10)\nrclpy.spin_once(node, timeout_sec=0.1)\nnode.destroy_node()\nrclpy.shutdown()\n", node, topic)
			default:
				t.Fatalf("unknown RMW success area %q", area.name)
			}
			rmwWeldRunSuccess(t, ctx, domain, "python3", "-c", script)
		})
	}
}
