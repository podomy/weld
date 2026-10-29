// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package ros_test

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/podomy/weld/internal/core"
	"github.com/podomy/weld/internal/transports/dds"
	"github.com/podomy/weld/internal/types"
	"github.com/podomy/weld/testbed/live/helpers"
)

// Service gate constants for std_srvs Trigger.
//
// Each test allocates one exclusive domain from the parallel pool, so
// per-run service names plus the allocated domain isolate runs. The single
// pinned type is std_srvs/srv/Trigger: std_srvs ships in every
// ros-base image so ros2 interface show succeeds on Humble, Jazzy, and
// Rolling, the empty request keeps encoding minimal to one dummy byte,
// and the bool plus string response carries an exact reply to assert.
// AddTwoInts is avoided because the demo_nodes_cpp server binary is not
// guaranteed in ros-base images and would add int64 codec surface.
// Corpus pins Trigger at internal/types/corpus.go and the DDS rq plus rr
// mapping is pinned via dds.NewServiceEndpoints per invariants.
// Service calls carry no QoS flags, so no ros2 topic QoS flags apply here.
const (
	svcTriggerRosType      = "std_srvs/srv/Trigger"
	svcTriggerIDLBase      = "std_srvs__srv__Trigger"
	svcTriggerIDLRequest   = "std_srvs__srv__Trigger_Request"
	svcTriggerIDLResponse  = "std_srvs__srv__Trigger_Response"
	svcTriggerTimeout      = 2 * time.Minute
	svcTriggerCallTimeout  = 15 * time.Second
	svcTriggerServeTimeout = 20 * time.Second
	svcTriggerShortTimeout = 2 * time.Second
	svcTriggerSettle       = 5 * time.Second
	svcTriggerClean        = 10 * time.Second
	svcTriggerProbe        = 5 * time.Second
	svcTriggerPubRetry     = 10 * time.Second
	svcTriggerPubGap       = 200 * time.Millisecond
)

// svcTriggerServiceName returns a per-run ROS service name for live ID.
//
// Hyphens and spaces map to underscores so concurrent runs never share
// a service. The caller reuses the same name for start and assert.
func svcTriggerServiceName(live string) string {
	sanitized := strings.ReplaceAll(live, "-", "_")
	sanitized = strings.ReplaceAll(sanitized, " ", "_")
	return "/weld_trigger_" + sanitized
}

// svcTriggerRequirePeer skips when no peer or no Trigger type is present.
//
// It allows only SkipIfNoPeer and SkipIfNoTypeInterface. A missing
// payload with a healthy peer must fail, never skip.
func svcTriggerRequirePeer(t *testing.T, ctx context.Context, domain int) {
	t.Helper()
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)
	helpers.SkipIfNoTypeInterface(t, ctx, helpers.Container, domain, helpers.RosBin, svcTriggerRosType, svcTriggerProbe)
}

// svcTriggerCheckMapping pins rq plus rr mapping and service QoS truth.
//
// It checks dds.NewServiceEndpoints for the per-run service plus the
// request and response DDS type names, and resolves service QoS via
// core.ResolveServiceQoS. Any mapping or QoS error fails loudly.
func svcTriggerCheckMapping(t *testing.T, service string) {
	t.Helper()
	eps, err := dds.NewServiceEndpoints(service, svcTriggerIDLBase, false)
	if err != nil {
		t.Fatalf("service endpoints %q: %v", service, err)
	}
	stripped := strings.TrimPrefix(service, "/")
	if eps.RequestTopic != "rq/"+stripped+"Request" {
		t.Fatalf("request topic = %q, want %q", eps.RequestTopic, "rq/"+stripped+"Request")
	}
	if eps.ReplyTopic != "rr/"+stripped+"Reply" {
		t.Fatalf("reply topic = %q, want %q", eps.ReplyTopic, "rr/"+stripped+"Reply")
	}
	reqType, err := dds.ServiceRequestTypeName(svcTriggerIDLBase)
	if err != nil {
		t.Fatalf("request type: %v", err)
	}
	if reqType != eps.RequestType {
		t.Fatalf("request type = %q, want %q", eps.RequestType, reqType)
	}
	rspType, err := dds.ServiceResponseTypeName(svcTriggerIDLBase)
	if err != nil {
		t.Fatalf("response type: %v", err)
	}
	if rspType != eps.ResponseType {
		t.Fatalf("response type = %q, want %q", eps.ResponseType, rspType)
	}
	_, _, err = core.ResolveServiceQoS(service, core.BestAvailableQoS())
	if err != nil {
		t.Fatalf("resolve service QoS %q: %v", service, err)
	}
}

// svcEncodeTriggerRequest encodes an empty Trigger request with header.
//
// The empty request carries only the 20 byte service prefix with no
// payload, matching the ROS wire captured from CycloneDDS peers. It
// returns the wire bytes for PublishService.
func svcEncodeTriggerRequest(guid [8]byte, seq uint64) ([]byte, error) {
	enc := types.NewEncoder(true)
	err := enc.WriteServiceHeader(guid, seq)
	if err != nil {
		return nil, fmt.Errorf("svc trigger encode request header: %w", err)
	}
	return enc.Bytes(), nil
}

// svcDecodeTriggerRequest decodes one Trigger request and returns the key.
//
// It checks the service header then requires empty payload with only zero
// pad before Finish, matching the ROS header-only wire. Nonzero trailing
// bytes fail loudly.
func svcDecodeTriggerRequest(wire []byte) ([8]byte, uint64, error) {
	var guid [8]byte
	dec, err := types.NewDecoder(wire)
	if err != nil {
		return guid, 0, fmt.Errorf("svc trigger decode request header: %w", err)
	}
	gotGUID, seq, err := dec.ReadServiceHeader()
	if err != nil {
		return guid, 0, fmt.Errorf("svc trigger read request header: %w", err)
	}
	for dec.Remaining() > 0 {
		pad, err := dec.ReadUint8()
		if err != nil {
			return guid, 0, fmt.Errorf("svc trigger read request pad: %w", err)
		}
		if pad != 0x00 {
			return guid, 0, fmt.Errorf("svc trigger request pad 0x%02x, want zero", pad)
		}
	}
	err = dec.Finish()
	if err != nil {
		return guid, 0, fmt.Errorf("svc trigger finish request: %w", err)
	}
	return gotGUID, seq, nil
}

// svcEncodeTriggerResponse encodes success plus message with header.
//
// It returns the wire bytes for PublishBytes on the reply path.
func svcEncodeTriggerResponse(success bool, message string, guid [8]byte, seq uint64) ([]byte, error) {
	enc := types.NewEncoder(true)
	err := enc.WriteServiceHeader(guid, seq)
	if err != nil {
		return nil, fmt.Errorf("svc trigger encode response header: %w", err)
	}
	enc.WriteBool(success)
	err = enc.WriteString(message)
	if err != nil {
		return nil, fmt.Errorf("svc trigger encode response message: %w", err)
	}
	return enc.Bytes(), nil
}

// svcDecodeTriggerResponse decodes one Trigger reply for the wanted key.
//
// It returns matched false when GUID or sequence differs so the caller
// skips stray replies. A matched reply with corrupt fields fails loudly.
func svcDecodeTriggerResponse(wire []byte, wantGUID [8]byte, wantSeq uint64) (bool, string, bool, error) {
	dec, err := types.NewDecoder(wire)
	if err != nil {
		return false, "", false, fmt.Errorf("svc trigger decode response header: %w", err)
	}
	gotGUID, seq, err := dec.ReadServiceHeader()
	if err != nil {
		return false, "", false, fmt.Errorf("svc trigger read response header: %w", err)
	}
	if gotGUID != wantGUID || seq != wantSeq {
		return false, "", false, nil
	}
	success, err := dec.ReadBool()
	if err != nil {
		return false, "", true, fmt.Errorf("svc trigger read success: %w", err)
	}
	message, err := dec.ReadString()
	if err != nil {
		return false, "", true, fmt.Errorf("svc trigger read message: %w", err)
	}
	err = dec.Finish()
	if err != nil {
		return false, "", true, fmt.Errorf("svc trigger finish response: %w", err)
	}
	return success, message, true, nil
}

// svcTriggerServerScript returns the python3 Trigger server program.
//
// The server answers every request with success true and the fixed
// reply message so Weld can assert an exact reply. Service and message
// are embedded as single quoted python strings.
func svcTriggerServerScript(service, replyMessage, node string) string {
	return "import rclpy\n" +
		"from rclpy.node import Node\n" +
		"from std_srvs.srv import Trigger\n" +
		"rclpy.init()\n" +
		"node = Node('" + node + "')\n" +
		"def cb(req, resp):\n" +
		"    resp.success = True\n" +
		"    resp.message = '" + replyMessage + "'\n" +
		"    return resp\n" +
		"srv = node.create_service(Trigger, '" + service + "', cb)\n" +
		"rclpy.spin(node)\n"
}

// svcStartTriggerServer starts a transient ROS Trigger server in the peer.
//
// It runs python3 with rclpy plus std_srvs only, so no demo_nodes binary
// is required. Output goes to the logs volume and cleanup kills the
// pidfile PID plus one scoped pkill for the run ID. The caller settles
// with helpers.Sleep before the first Weld take.
func svcStartTriggerServer(t *testing.T, ctx context.Context, domain int, service, live, replyMessage string) {
	t.Helper()
	node := "weld_trigger_" + strings.ReplaceAll(live, "-", "_")
	script := svcTriggerServerScript(service, replyMessage, node)
	pidPath := helpers.HostPidPath(t, live)
	err := helpers.StartPeerProc(ctx, helpers.Container, domain, live, "python3", "-c", script)
	if err != nil {
		t.Fatalf("start ROS trigger server %q: %v", service, err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.WithoutCancel(ctx), svcTriggerClean)
		defer cleanupCancel()
		helpers.StopPeerProc(cleanupCtx, helpers.Container, domain, pidPath, live)
	})
}

// svcRosServiceCall runs ros2 service call for Trigger and returns output.
//
// Domain is the allocated exclusive domain plumbed to DockerArgs, so
// parallel workers never share a domain. It uses docker argv with no shell
// and carries the domain plus RMW env from helpers.DockerArgs. Services
// carry no QoS flags.
func svcRosServiceCall(ctx context.Context, domain int, service string) (string, error) {
	args := helpers.DockerArgs(helpers.Container, domain, helpers.RosBin, "service", "call", service, svcTriggerRosType, "{}")
	cmd := exec.CommandContext(ctx, "docker", args...) //nolint:gosec // live harness runs docker with fixed test args
	out, err := cmd.CombinedOutput()
	if err != nil {
		return string(out), fmt.Errorf("svc ros service call %q: %w output %s", service, err, string(out))
	}
	return string(out), nil
}

// svcWaitForServiceVisible polls ros2 service list until service appears.
//
// Discovery on the allocated exclusive domain needs time after Weld announces its
// request reader plus reply writer. A fixed sleep alone flakes under
// fleet noise, so this polls every 500ms until the timeout or ctx end.
// It verifies gate 1 (graph lists the service) before ros2 service call
// checks gates 2 and 3. A missing service after the timeout fails loudly
// with the last list output.
func svcWaitForServiceVisible(ctx context.Context, t *testing.T, domain int, service string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var last string
	for {
		if cerr := ctx.Err(); cerr != nil {
			t.Fatalf("context ended waiting for service %q visible: %v last list:\n%s", service, cerr, last)
		}
		args := helpers.DockerArgs(helpers.Container, domain, helpers.RosBin, "service", "list")
		cmd := exec.CommandContext(ctx, "docker", args...) //nolint:gosec // live harness runs docker with fixed test args
		out, err := cmd.CombinedOutput()
		last = string(out)
		if err == nil && strings.Contains(last, service) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("service %q not visible after %v, last list err=%v:\n%s", service, timeout, err, last)
		}
		timer := time.NewTimer(500 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			t.Fatalf("context ended waiting for service %q visible: %v last list:\n%s", service, ctx.Err(), last)
		case <-timer.C:
		}
	}
}

// svcIsSkippableTake reports whether a Take error is a per-sample drop.
//
// Stale duplicates and truncated or invalid frames are skipped while
// waiting for the live reply. Any other error fails the call.
func svcIsSkippableTake(err error) bool {
	return errors.Is(err, dds.ErrStaleSequence) || errors.Is(err, dds.ErrTruncated) || errors.Is(err, dds.ErrInvalidFrame)
}

// svcWeldTriggerCall calls a ROS Trigger server via the Weld service path.
//
// It subscribes to the reply, publishes the empty request with the
// given GUID and sequence, then takes until the matching reply arrives
// or the deadline ends. GUID mismatch and skippable drops are skipped.
// A missing reply by the deadline fails loudly with the service name.
// svcWeldTriggerCall calls a ROS Trigger server via the Weld service path.
//
// It subscribes to the reply, then republishes the empty request until
// the matching reply arrives or the deadline ends. Republishing absorbs
// slow server discovery: the first publish can leave before the server's
// request reader is discovered, so a single publish plus long take
// misses. GUID mismatch and skippable drops are skipped. A missing reply
// by the deadline fails loudly with the service name.
//
//nolint:cyclop,gocognit // live call path asserts declare, encode, publish, take, and match in one run
func svcWeldTriggerCall(t *testing.T, ctx context.Context, backend *dds.Backend, service string, guid [8]byte, seq uint64, deadline time.Duration) (bool, string) {
	t.Helper()
	err := backend.SubscribeService(ctx, service, svcTriggerIDLBase, dds.ServiceRoleClient, "")
	if err != nil {
		t.Fatalf("subscribe trigger reply %q: %v", service, err)
	}
	client := core.NewServiceClient(nil, false, nil)
	callDur := core.DurationFromNanoseconds(int64(deadline))
	err = client.BeginCall(ctx, service, guid, seq, callDur)
	if err != nil {
		t.Fatalf("begin trigger call %q: %v", service, err)
	}
	wireReq, err := svcEncodeTriggerRequest(guid, seq)
	if err != nil {
		t.Fatalf("encode trigger request %q: %v", service, err)
	}
	callCtx, callCancel := context.WithTimeout(ctx, deadline)
	defer callCancel()
	repub := time.NewTimer(0)
	defer repub.Stop()
	for {
		select {
		case <-callCtx.Done():
			t.Fatalf("weld trigger call %q timed out after %v: %v", service, deadline, callCtx.Err())
		case <-repub.C:
		}
		err = backend.PublishService(callCtx, service, svcTriggerIDLBase, dds.ServiceRoleClient, wireReq, "")
		if err != nil && !errors.Is(err, dds.ErrNoRoute) {
			t.Fatalf("publish trigger request %q: %v", service, err)
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
			t.Fatalf("take trigger reply %q: %v", service, err)
		}
		if incoming.TopicName != "" && incoming.TopicName != service {
			continue
		}
		success, message, matched, err := svcDecodeTriggerResponse(incoming.Payload, guid, seq)
		if err != nil {
			t.Fatalf("decode trigger reply %q: %v", service, err)
		}
		if !matched {
			continue
		}
		err = client.CompleteCall(service, guid, seq)
		if err != nil {
			t.Fatalf("complete trigger call %q: %v", service, err)
		}
		return success, message
	}
}

// svcWeldTriggerServeOnce serves one Trigger request via Weld backend path.
//
// It takes until one decodable request arrives, tracks it through
// core.ServiceServer Enqueue plus Take plus Reply, then publishes the
// fixed success reply. Stray topics, undecodable samples, and skippable
// take errors (foreign frames the demux drops) are skipped. A Take
// timeout fails loudly through the wrapped context error.
//
//nolint:cyclop,gocognit // live serve path asserts take, decode, reply, and publish in one run
func svcWeldTriggerServeOnce(ctx context.Context, backend *dds.Backend, service, replyMessage string, takeTimeout time.Duration) error {
	server := core.NewServiceServer(nil, false, nil)
	takeCtx, takeCancel := context.WithTimeout(ctx, takeTimeout)
	defer takeCancel()
	for {
		incoming, err := backend.TakeBytes(takeCtx)
		if err != nil {
			if svcIsSkippableTake(err) {
				continue
			}
			return fmt.Errorf("svc trigger serve take %q: %w", service, err)
		}
		if incoming.TopicName != "" && incoming.TopicName != service {
			continue
		}
		guid, seq, err := svcDecodeTriggerRequest(incoming.Payload)
		if err != nil {
			continue
		}
		timeoutDur := core.DurationFromNanoseconds(int64(takeTimeout))
		err = server.EnqueueRequest(ctx, service, guid, seq, timeoutDur)
		if err != nil {
			return fmt.Errorf("svc trigger serve enqueue %q: %w", service, err)
		}
		req, ok := server.TakeRequest()
		if !ok {
			return fmt.Errorf("svc trigger serve take empty %q", service)
		}
		if req.Client != guid || req.Sequence != seq {
			return fmt.Errorf("svc trigger serve key mismatch %q", service)
		}
		wireResp, err := svcEncodeTriggerResponse(true, replyMessage, guid, seq)
		if err != nil {
			return fmt.Errorf("svc trigger serve encode %q: %w", service, err)
		}
		err = backend.PublishService(ctx, service, svcTriggerIDLBase, dds.ServiceRoleServer, wireResp, "")
		if err != nil {
			return fmt.Errorf("svc trigger serve publish %q: %w", service, err)
		}
		err = server.Reply(service, guid, seq)
		if err != nil {
			return fmt.Errorf("svc trigger serve reply %q: %w", service, err)
		}
		return nil
	}
}

// TestROSServiceTriggerForwardWeldCallsROSServer checks Weld calls ROS Trigger.
//
// It starts a transient python3 rclpy Trigger server for the per-run
// service, settles, then calls it from the Weld backend Subscribe plus
// Publish path with a context deadline and asserts the exact bool plus
// message reply. It skips only for no peer or missing Trigger type. A
// missing reply with a healthy peer fails loudly.
func TestROSServiceTriggerForwardWeldCallsROSServer(t *testing.T) {
	t.Parallel()
	domain := helpers.MustAllocateDomain(t)
	ctx, cancel := context.WithTimeout(context.Background(), svcTriggerTimeout)
	defer cancel()
	svcTriggerRequirePeer(t, ctx, domain)
	live := helpers.LiveID("svc-trigger-fwd")
	service := svcTriggerServiceName(live)
	replyMessage := "forward-ok-" + live
	svcTriggerCheckMapping(t, service)
	svcStartTriggerServer(t, ctx, domain, service, live, replyMessage)
	helpers.Sleep(ctx, svcTriggerSettle)
	if ctx.Err() != nil {
		t.Fatalf("context ended before trigger call: %v", ctx.Err())
	}
	backend := helpers.MustOpenLiveBackend(t, ctx, uint32(domain), "svc-trigger-fwd-"+live, "svc-forward") //nolint:gosec // G115: pooled domain 70 through 101 fits uint32
	var guid [8]byte
	for i := range guid {
		guid[i] = byte(i + 1)
	}
	success, message := svcWeldTriggerCall(t, ctx, backend, service, guid, 1, svcTriggerCallTimeout)
	if !success {
		t.Fatalf("trigger forward %q success = false, want true", service)
	}
	if message != replyMessage {
		t.Fatalf("trigger forward %q message = %q, want %q", service, message, replyMessage)
	}
}

// TestROSServiceTriggerReverseROSSCallsWeldServer checks ROS calls Weld Trigger.
//
// It subscribes Weld to the per-run Trigger request, declares the reply
// writer with synchronous announce, waits for the graph to list the
// service, serves one reply from a background goroutine through the Weld
// backend Take plus Publish path, then runs ros2 service call in the peer
// and asserts the reply holds success true plus the exact message. The
// list poll isolates gate 1 (graph) from the call wait (gates 2 and 3).
// It skips only for no peer or missing Trigger type. A missing reply
// fails loudly.
func TestROSServiceTriggerReverseROSSCallsWeldServer(t *testing.T) {
	t.Parallel()
	domain := helpers.MustAllocateDomain(t)
	ctx, cancel := context.WithTimeout(context.Background(), svcTriggerTimeout)
	defer cancel()
	// Service verbs have no --no-daemon flag, so the calls below leave a
	// daemon participant behind. Remove it at cleanup so reused domains
	// stay isolated for later tests.
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.WithoutCancel(ctx), svcTriggerClean)
		defer cleanupCancel()
		helpers.KillRosDaemon(cleanupCtx, helpers.Container, domain)
	})
	svcTriggerRequirePeer(t, ctx, domain)
	live := helpers.LiveID("svc-trigger-rev")
	service := svcTriggerServiceName(live)
	replyMessage := "reverse-ok-" + live
	svcTriggerCheckMapping(t, service)
	backend := helpers.MustOpenLiveBackend(t, ctx, uint32(domain), "svc-trigger-rev-"+live, "svc-reverse") //nolint:gosec // G115: pooled domain 70 through 101 fits uint32
	err := backend.SubscribeService(ctx, service, svcTriggerIDLBase, dds.ServiceRoleServer, "")
	if err != nil {
		t.Fatalf("subscribe trigger request %q: %v", service, err)
	}
	err = backend.DeclareServiceWriter(ctx, service, svcTriggerIDLBase, dds.ServiceRoleServer, "")
	if err != nil {
		t.Fatalf("declare trigger reply writer %q: %v", service, err)
	}
	helpers.Sleep(ctx, svcTriggerSettle)
	if ctx.Err() != nil {
		t.Fatalf("context ended before ros service call: %v", ctx.Err())
	}
	svcWaitForServiceVisible(ctx, t, domain, service, svcTriggerServeTimeout)
	serveErr := make(chan error, 1)
	go func() {
		serveErr <- svcWeldTriggerServeOnce(ctx, backend, service, replyMessage, svcTriggerServeTimeout)
	}()
	out, err := svcRosServiceCall(ctx, domain, service)
	if err != nil {
		t.Fatalf("ros service call %q: %v", service, err)
	}
	if !strings.Contains(out, "True") && !strings.Contains(out, "true") {
		t.Fatalf("ros service call %q misses success true:\n%s", service, out)
	}
	if !strings.Contains(out, replyMessage) {
		t.Fatalf("ros service call %q misses exact message %q:\n%s", service, replyMessage, out)
	}
	select {
	case err := <-serveErr:
		if err != nil {
			t.Fatalf("weld trigger serve %q: %v", service, err)
		}
	case <-ctx.Done():
		t.Fatalf("weld trigger serve %q did not finish: %v", service, ctx.Err())
	}
}

// TestROSServiceTriggerDeadlineExceedsCleanly checks a missing server times out.
//
// It begins one core client call with a short timeout and no server,
// expires it past the deadline, and proves exactly one timeout with no
// pending call and no hang. A second complete for the expired key must
// report no pending. It needs no peer and never skips.
//
//nolint:cyclop // deadline test asserts timeout across publish, take, and error shapes in one run
func TestROSServiceTriggerDeadlineExceedsCleanly(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), svcTriggerShortTimeout)
	defer cancel()
	service := "/weld_trigger_missing"
	var guid [8]byte
	for i := range guid {
		guid[i] = byte(i + 11)
	}
	const seq = uint64(7)
	client := core.NewServiceClient(nil, false, nil)
	callDur := core.DurationFromNanoseconds(int64(200 * time.Millisecond))
	err := client.BeginCall(ctx, service, guid, seq, callDur)
	if err != nil {
		t.Fatalf("begin missing call: %v", err)
	}
	if !client.HasPending(service, guid, seq) {
		t.Fatalf("missing call not pending after begin")
	}
	deadline := time.Now().Add(400 * time.Millisecond)
	timer := time.NewTimer(time.Until(deadline))
	select {
	case <-ctx.Done():
		timer.Stop()
		t.Fatalf("outer context ended before expiry: %v", ctx.Err())
	case <-timer.C:
	}
	expired := client.ExpireCalls(time.Now().UnixNano())
	if len(expired) != 1 {
		t.Fatalf("expired = %d, want 1", len(expired))
	}
	if expired[0].Service != service || expired[0].Sequence != seq {
		t.Fatalf("expired key = %+v, want service %q seq %d", expired[0], service, seq)
	}
	if client.TimeoutCount() != 1 {
		t.Fatalf("timeout count = %d, want 1", client.TimeoutCount())
	}
	if client.HasPending(service, guid, seq) {
		t.Fatalf("expired call still pending, want clean timeout")
	}
	err = client.CompleteCall(service, guid, seq)
	if err == nil {
		t.Fatalf("complete after expiry succeeded, want no pending")
	}
	if !errors.Is(err, core.ErrServiceNoPending) {
		t.Fatalf("complete after expiry = %v, want ErrServiceNoPending", err)
	}
}
