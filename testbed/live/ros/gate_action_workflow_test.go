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

	"github.com/podomy/weld/internal/core"
	"github.com/podomy/weld/internal/transports"
	"github.com/podomy/weld/internal/transports/dds"
	"github.com/podomy/weld/internal/types"
	"github.com/podomy/weld/testbed/live/helpers"
)

// Action workflow gate constants for timeouts.
//
// Each test allocates one exclusive domain from the parallel pool, so
// per-run action bases plus per-run envelopes plus the allocated domain
// isolate runs. Timeouts bound every backend call through context.
const (
	actionGateOverall     time.Duration = 2 * time.Minute
	actionGateTakeTimeout time.Duration = 20 * time.Second
	actionGateShortTake   time.Duration = 3 * time.Second
	actionGateSettle      time.Duration = 2 * time.Second
	actionGateRetry       time.Duration = 10 * time.Second
	actionGateRetryStep   time.Duration = 200 * time.Millisecond
	actionGateProbeWait   time.Duration = 5 * time.Second
	actionGateType        string        = "std_msgs__msg__String"
	actionGateRosType     string        = "std_msgs/msg/String"
)

// Topology note: no standard action server ships in the testbed images.
//
// The jazzy, humble, and rolling Dockerfiles install ros base plus the
// RMW only, and helpers only reference ros2 topic pub, topic echo,
// topic info, and interface show. No fibonacci or other action server
// binary exists to drive. These gates therefore drive goal plus cancel
// between TWO Weld backends on one allocated exclusive domain, with core.ActionServer
// as the workflow truth table on the server side. The client backend is
// the transient talker equivalent: it publishes each envelope with route
// retry after the server subscribes and settles. No ros2 data plane
// argv runs here, so no QoS flags apply; the only docker argv are the
// SkipIfNoPeer inspect probe and the SkipIfNoTypeInterface show probe,
// neither of which takes QoS flags. The single exchange shape is the
// send goal ROS service name with a String envelope, and the full eight
// topic fanout stays pinned in unit tests.

// actionBaseFor returns a per-run ROS action base name for run isolation.
func actionBaseFor(runID string) string {
	sanitized := strings.ReplaceAll(runID, "-", "_")
	sanitized = strings.ReplaceAll(sanitized, " ", "_")
	return "/weldact_" + sanitized
}

// encodeActionEnvelope encodes text as XCDR1 String bytes or fails the test.
func encodeActionEnvelope(t *testing.T, text string) []byte {
	t.Helper()
	enc := types.NewEncoder(true)
	err := enc.WriteString(text)
	if err != nil {
		t.Fatalf("encode action envelope: %v", err)
	}
	wire := enc.Bytes()
	if len(wire) == 0 {
		t.Fatalf("encoded action envelope is empty")
	}
	return wire
}

// decodeActionEnvelope decodes XCDR1 String bytes with a labeled error.
func decodeActionEnvelope(wire []byte) (string, error) {
	dec, err := types.NewDecoder(wire)
	if err != nil {
		return "", fmt.Errorf("action decode header: %w", err)
	}
	got, err := dec.ReadString()
	if err != nil {
		return "", fmt.Errorf("action decode string: %w", err)
	}
	err = dec.Finish()
	if err != nil {
		return "", fmt.Errorf("action decode finish: %w", err)
	}
	return got, nil
}

// openActionPair joins a client plus server backend on the allocated domain.
//
// Domain is the allocated exclusive domain plumbed to MustOpenLiveBackend,
// so parallel workers never share a domain.
func openActionPair(t *testing.T, ctx context.Context, domain uint32, runID string) (*dds.Backend, *dds.Backend) {
	t.Helper()
	client := helpers.MustOpenLiveBackend(t, ctx, domain, "act-client-"+runID, "action-client")
	server := helpers.MustOpenLiveBackend(t, ctx, domain, "act-server-"+runID, "action-server")
	return client, server
}

// newActionWorkflowServer returns an activated core action server.
func newActionWorkflowServer(t *testing.T, actionBase string, nowNanos int64) *core.ActionServer {
	t.Helper()
	srv, err := core.NewActionServer(actionBase, core.ClockWall, 0)
	if err != nil {
		t.Fatalf("new action server: %v", err)
	}
	srv.Activate(nowNanos)
	return srv
}

// sendGoalServiceTopic returns the send goal ROS service name for one base.
func sendGoalServiceTopic(t *testing.T, actionBase string) string {
	t.Helper()
	topic, err := dds.SendGoalServiceName(actionBase)
	if err != nil {
		t.Fatalf("send goal service name: %v", err)
	}
	return topic
}

// publishActionEnvelope publishes one envelope with route retry.
func publishActionEnvelope(t *testing.T, ctx context.Context, backend *dds.Backend, topic, text string) error {
	t.Helper()
	msg := transports.OutgoingMessage{
		TopicName: topic,
		TypeName:  actionGateType,
		Payload:   encodeActionEnvelope(t, text),
	}
	err := helpers.PublishLiveWithRetry(ctx, backend, msg, actionGateRetry, actionGateRetryStep, "action")
	if err != nil {
		return fmt.Errorf("action publish: %w", err)
	}
	return nil
}

// takeActionEnvelope takes until the per-run topic yields text or times out.
func takeActionEnvelope(ctx context.Context, backend *dds.Backend, topic string) (string, error) {
	takeCtx, takeCancel := context.WithTimeout(ctx, actionGateTakeTimeout)
	defer takeCancel()
	skips := 0
	for {
		msg, err := backend.TakeBytes(takeCtx)
		if err != nil {
			return "", fmt.Errorf("action take after %d skips: %w", skips, err)
		}
		if msg.TopicName != topic {
			skips++
			continue
		}
		got, err := decodeActionEnvelope(msg.Payload)
		if err != nil {
			skips++
			continue
		}
		return got, nil
	}
}

// settleActionServer subscribes the server then settles before any take.
func settleActionServer(t *testing.T, ctx context.Context, server *dds.Backend, topic string) {
	t.Helper()
	err := server.Subscribe(ctx, topic, actionGateType, "")
	if err != nil {
		t.Fatalf("subscribe action topic %q: %v", topic, err)
	}
	helpers.Sleep(ctx, actionGateSettle)
	if ctx.Err() != nil {
		t.Fatalf("context ended before action take: %v", ctx.Err())
	}
	helpers.WaitForTopicDiscovery(ctx, server, topic)
}

// checkActionGatePeer skips cleanly only for a missing peer or type.
//
// Domain is the allocated exclusive domain plumbed to the type probe, so
// parallel workers never share a domain.
func checkActionGatePeer(t *testing.T, ctx context.Context, domain int) {
	t.Helper()
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)
	helpers.SkipIfNoTypeInterface(t, ctx, helpers.Container, domain, helpers.RosBin, actionGateRosType, actionGateProbeWait)
}

// TestROSActionWorkflowGoalAccepts proves a live goal is accepted.
func TestROSActionWorkflowGoalAccepts(t *testing.T) {
	t.Parallel()
	domain := helpers.MustAllocateDomain(t)
	ctx, cancel := context.WithTimeout(context.Background(), actionGateOverall)
	defer cancel()
	checkActionGatePeer(t, ctx, domain)
	runID := helpers.LiveID("act")
	actionBase := actionBaseFor(runID)
	topic := sendGoalServiceTopic(t, actionBase)
	client, server := openActionPair(t, ctx, uint32(domain), runID) //nolint:gosec // G115: pooled domain 70 through 101 fits uint32
	settleActionServer(t, ctx, server, topic)
	goalID := "goal_" + strings.ReplaceAll(runID, "-", "_")
	sent := "goal:" + goalID + ":" + runID
	err := publishActionEnvelope(t, ctx, client, topic, sent)
	if err != nil {
		t.Fatalf("publish goal envelope: %v", err)
	}
	got, err := takeActionEnvelope(ctx, server, topic)
	if err != nil {
		t.Fatalf("take goal envelope: %v", err)
	}
	if !strings.Contains(got, runID) || !strings.Contains(got, goalID) {
		t.Fatalf("goal envelope %q misses run %q goal %q", got, runID, goalID)
	}
	now := time.Now().UnixNano()
	srv := newActionWorkflowServer(t, actionBase, now)
	err = srv.AcceptGoal(goalID, now, core.ClockWall, now)
	if err != nil {
		t.Fatalf("accept goal: %v", err)
	}
	status, ok := srv.GoalStatusOf(goalID, now)
	if !ok {
		t.Fatalf("accepted goal %q has no status", goalID)
	}
	if status != core.GoalExecuting {
		t.Fatalf("accepted goal status = %s, want executing", status.String())
	}
	take := srv.Take()
	if take.Kind != core.TakeExecute || take.GoalID != goalID {
		t.Fatalf("take = %s %q, want execute %q", take.Kind.String(), take.GoalID, goalID)
	}
}

// TestROSActionWorkflowCancelEndsCanceled proves cancel ends terminal.
//
//nolint:cyclop,gocognit // cancel test asserts publish, take, cancel, and reply absence in one run
func TestROSActionWorkflowCancelEndsCanceled(t *testing.T) {
	t.Parallel()
	domain := helpers.MustAllocateDomain(t)
	ctx, cancel := context.WithTimeout(context.Background(), actionGateOverall)
	defer cancel()
	checkActionGatePeer(t, ctx, domain)
	runID := helpers.LiveID("act")
	actionBase := actionBaseFor(runID)
	topic := sendGoalServiceTopic(t, actionBase)
	client, server := openActionPair(t, ctx, uint32(domain), runID) //nolint:gosec // G115: pooled domain 70 through 101 fits uint32
	settleActionServer(t, ctx, server, topic)
	goalID := "goal_" + strings.ReplaceAll(runID, "-", "_")
	err := publishActionEnvelope(t, ctx, client, topic, "goal:"+goalID+":"+runID)
	if err != nil {
		t.Fatalf("publish goal envelope: %v", err)
	}
	_, err = takeActionEnvelope(ctx, server, topic)
	if err != nil {
		t.Fatalf("take goal envelope: %v", err)
	}
	now := time.Now().UnixNano()
	srv := newActionWorkflowServer(t, actionBase, now)
	err = srv.AcceptGoal(goalID, now, core.ClockWall, now)
	if err != nil {
		t.Fatalf("accept goal: %v", err)
	}
	err = publishActionEnvelope(t, ctx, client, topic, "cancel:"+goalID+":"+runID)
	if err != nil {
		t.Fatalf("publish cancel envelope: %v", err)
	}
	got, err := takeActionEnvelope(ctx, server, topic)
	if err != nil {
		t.Fatalf("take cancel envelope timed out, cancel hung: %v", err)
	}
	if !strings.Contains(got, "cancel:") || !strings.Contains(got, goalID) {
		t.Fatalf("cancel envelope %q misses cancel marker or goal %q", got, goalID)
	}
	cancelNow := time.Now().UnixNano()
	err = srv.CancelGoal(goalID, cancelNow)
	if err != nil {
		t.Fatalf("cancel goal: %v", err)
	}
	take := srv.Take()
	if take.Kind != core.TakeCancel || take.GoalID != goalID {
		t.Fatalf("take = %s %q, want cancel %q", take.Kind.String(), take.GoalID, goalID)
	}
	doneNow := time.Now().UnixNano()
	err = srv.CompleteGoal(goalID, core.GoalCanceled, []byte("result:"+goalID), doneNow)
	if err != nil {
		t.Fatalf("complete canceled goal: %v", err)
	}
	status, ok := srv.GoalStatusOf(goalID, doneNow)
	if !ok {
		t.Fatalf("canceled goal %q has no status", goalID)
	}
	if status != core.GoalCanceled {
		t.Fatalf("canceled goal status = %s, want canceled", status.String())
	}
	payload, status, err := srv.GetResult(goalID, doneNow)
	if err != nil {
		t.Fatalf("get canceled result: %v", err)
	}
	if status != core.GoalCanceled {
		t.Fatalf("canceled result status = %s, want canceled", status.String())
	}
	if string(payload) != "result:"+goalID {
		t.Fatalf("canceled result payload = %q, want result for %q", string(payload), goalID)
	}
}

// TestROSActionWorkflowTimeoutStaysTerminal proves a short wait ends clean.
func TestROSActionWorkflowTimeoutStaysTerminal(t *testing.T) {
	t.Parallel()
	domain := helpers.MustAllocateDomain(t)
	ctx, cancel := context.WithTimeout(context.Background(), actionGateOverall)
	defer cancel()
	checkActionGatePeer(t, ctx, domain)
	runID := helpers.LiveID("act")
	actionBase := actionBaseFor(runID)
	topic := sendGoalServiceTopic(t, actionBase)
	client, server := openActionPair(t, ctx, uint32(domain), runID) //nolint:gosec // G115: pooled domain 70 through 101 fits uint32
	settleActionServer(t, ctx, server, topic)
	goalID := "goal_" + strings.ReplaceAll(runID, "-", "_")
	err := publishActionEnvelope(t, ctx, client, topic, "goal:"+goalID+":"+runID)
	if err != nil {
		t.Fatalf("publish goal envelope: %v", err)
	}
	_, err = takeActionEnvelope(ctx, server, topic)
	if err != nil {
		t.Fatalf("take goal envelope: %v", err)
	}
	now := time.Now().UnixNano()
	srv := newActionWorkflowServer(t, actionBase, now)
	err = srv.AcceptGoal(goalID, now, core.ClockWall, now)
	if err != nil {
		t.Fatalf("accept goal: %v", err)
	}
	shortCtx, shortCancel := context.WithTimeout(ctx, actionGateShortTake)
	defer shortCancel()
	_, err = client.TakeBytes(shortCtx)
	if err == nil {
		t.Fatalf("short result wait saw a sample, want a clean timeout")
	}
	if !errors.Is(err, context.DeadlineExceeded) && !errors.Is(shortCtx.Err(), context.DeadlineExceeded) {
		t.Fatalf("short result wait error = %v, want deadline exceeded", err)
	}
	doneNow := time.Now().UnixNano()
	err = srv.CompleteGoal(goalID, core.GoalSucceeded, []byte("result:"+goalID), doneNow)
	if err != nil {
		t.Fatalf("complete slow goal: %v", err)
	}
	payload, status, err := srv.GetResult(goalID, doneNow)
	if err != nil {
		t.Fatalf("get terminal result: %v", err)
	}
	if status != core.GoalSucceeded {
		t.Fatalf("terminal result status = %s, want succeeded", status.String())
	}
	if string(payload) != "result:"+goalID {
		t.Fatalf("terminal result payload = %q, want result for %q", string(payload), goalID)
	}
}
