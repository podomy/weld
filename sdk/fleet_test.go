// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package sdk_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/podomy/weld/sdk"
)

// TestMixedFleetInMemory verifies the minimal SDK surface over the
// in-memory bus with fake clocks only. No live ROS, no network, no Docker.
func TestMixedFleetInMemory(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	bus := sdk.NewBus()
	planner, err := sdk.NewNode("fleet_planner", bus)
	if err != nil {
		t.Fatalf("new planner node: %v", err)
	}
	tracker, err := sdk.NewNode("tracker", bus)
	if err != nil {
		t.Fatalf("new tracker node: %v", err)
	}
	t.Cleanup(func() {
		_ = planner.Close()
		_ = tracker.Close()
	})
	testConfig(t, ctx, planner)
	testClock(t, planner, tracker)
	testTopics(t, ctx, planner, tracker)
	testServices(t, ctx, planner, tracker)
	testSupervision(t)
	testTeardown(t, planner, tracker)
}

// testConfig covers schema, precedence, watch, and validation.
func testConfig(t *testing.T, ctx context.Context, planner *sdk.Node) {
	t.Helper()
	_ = ctx
	store := planner.Config()
	if store == nil {
		t.Fatalf("node config is nil")
	}
	desc := sdk.Descriptor{Type: sdk.ParamTypeDouble, Description: "max linear velocity in m/s"}
	err := store.Declare("max_vel_x", sdk.NewDoubleValue(1.0), desc)
	if err != nil {
		t.Fatalf("declare max_vel_x: %v", err)
	}
	err = store.AddValidator("max_vel_x", func(_, newValue sdk.Value) error {
		if newValue.Type == sdk.ParamTypeDouble && newValue.Double < 0 {
			return fmt.Errorf("max_vel_x must be non-negative")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("add validator: %v", err)
	}
	hookCalls := 0
	err = store.AddApplyHook("max_vel_x", func(_, _ sdk.Value) {
		hookCalls++
	})
	if err != nil {
		t.Fatalf("add apply hook: %v", err)
	}
	version, ch, hookCancel := store.Watch()
	defer hookCancel()
	files := map[string]sdk.Value{"max_vel_x": sdk.NewDoubleValue(1.0)}
	cli := map[string]sdk.Value{"max_vel_x": sdk.NewDoubleValue(2.0)}
	code := map[string]sdk.Value{"max_vel_x": sdk.NewDoubleValue(3.0)}
	event, err := store.ApplyStartup(files, cli, code)
	if err != nil {
		t.Fatalf("apply startup: %v", err)
	}
	if event.Source != "startup" {
		t.Fatalf("startup source = %q, want startup", event.Source)
	}
	if len(event.Changes) != 1 {
		t.Fatalf("startup changes = %d, want 1", len(event.Changes))
	}
	got, err := store.Get("max_vel_x")
	if err != nil {
		t.Fatalf("get max_vel_x: %v", err)
	}
	if !got.Equal(sdk.NewDoubleValue(3.0)) {
		t.Fatalf("precedence: got %v, want 3.0", got.Double)
	}
	select {
	case evt := <-ch:
		if evt.Version != version+1 {
			t.Fatalf("watch version = %d, want %d", evt.Version, version+1)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("watch timed out waiting for startup event")
	}
	if hookCalls != 1 {
		t.Fatalf("apply hooks = %d, want 1", hookCalls)
	}
	err = store.Set("max_vel_x", sdk.NewStringValue("fast"))
	if err == nil {
		t.Fatalf("string fast accepted for double max_vel_x")
	}
	err = store.Set("max_vel_x", sdk.NewDoubleValue(-1.0))
	if err == nil {
		t.Fatalf("negative max_vel_x accepted despite validator")
	}
	schema := sdk.Schema{"max_vel_x": desc}
	snapshot := map[string]sdk.Value{"max_vel_x": sdk.NewDoubleValue(3.0)}
	err = schema.Validate(snapshot)
	if err != nil {
		t.Fatalf("schema validate: %v", err)
	}
	bad := map[string]sdk.Value{"max_vel_x": sdk.NewStringValue("fast")}
	err = schema.Validate(bad)
	if err == nil {
		t.Fatalf("schema accepted string fast for double")
	}
	hist := store.HistorySince(version)
	if len(hist) == 0 {
		t.Fatalf("history since startup is empty")
	}
}

// testClock covers one domain per context, jumps, fallback, and agreement.
func testClock(t *testing.T, planner, tracker *sdk.Node) {
	t.Helper()
	fake := sdk.NewFakeClock(1000000000)
	clk := planner.Clock()
	jump := clk.SetUseSimTime(true)
	if !jump.ActivationChanged {
		t.Fatalf("sim activation did not report activation change")
	}
	var jumps []sdk.Jump
	id, err := clk.RegisterJumpCallback(func(j sdk.Jump) {
		jumps = append(jumps, j)
	})
	if err != nil {
		t.Fatalf("register jump callback: %v", err)
	}
	now, err := fake.Advance(500000000)
	if err != nil {
		t.Fatalf("fake advance: %v", err)
	}
	first := clk.SetSimTime(now)
	if first.Backward {
		t.Fatalf("forward sim step reported backward")
	}
	fake.Set(900000000)
	loop := clk.SetSimTime(fake.Now())
	if !loop.Backward {
		t.Fatalf("bag loop back to 900ms did not report backward")
	}
	if len(jumps) != 2 {
		t.Fatalf("jump callbacks = %d, want 2", len(jumps))
	}
	clk.UnregisterJumpCallback(id)
	clk.SetSimTime(fake.Now())
	if len(jumps) != 2 {
		t.Fatalf("callbacks fired after unregister")
	}
	err = clk.CheckPeerAgreement(sdk.DomainWall)
	if err == nil {
		t.Fatalf("sim versus wall agreement passed silently")
	}
	jump, err = clk.SetDomain(sdk.DomainWall)
	if err != nil {
		t.Fatalf("set domain wall: %v", err)
	}
	_ = jump
	err = clk.CheckPeerAgreement(sdk.DomainWall)
	if err != nil {
		t.Fatalf("wall versus wall agreement failed: %v", err)
	}
	tclk := tracker.Clock()
	tclk.SetUseSimTime(true)
	if !tclk.UsingFallback() {
		t.Fatalf("sim without sample did not report wall fallback")
	}
	_, badErr := tclk.SetDomain(sdk.Domain(99))
	if badErr == nil {
		t.Fatalf("invalid domain accepted silently")
	}
}

// testTopics covers byte publish and subscribe with order and mismatch.
func testTopics(t *testing.T, ctx context.Context, planner, tracker *sdk.Node) {
	t.Helper()
	pubQos := sdk.DefaultQoS()
	pubQos.Depth = 20
	pub, err := planner.NewPublisher("/cmd_vel", "geometry_msgs/msg/Twist", pubQos)
	if err != nil {
		t.Fatalf("new publisher: %v", err)
	}
	sub, err := tracker.NewSubscription("/cmd_vel", "geometry_msgs/msg/Twist", pubQos)
	if err != nil {
		t.Fatalf("new subscription: %v", err)
	}
	const count = 20
	for i := 0; i < count; i++ {
		payload := []byte(fmt.Sprintf("twist-%02d", i))
		err = pub.Publish(ctx, payload)
		if err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}
	for i := 0; i < count; i++ {
		callCtx, callCancel := context.WithTimeout(ctx, 5*time.Second)
		msg, err := sub.Next(callCtx)
		callCancel()
		if err != nil {
			t.Fatalf("next %d: %v", i, err)
		}
		want := fmt.Sprintf("twist-%02d", i)
		if string(msg) != want {
			t.Fatalf("message %d = %q, want %q", i, msg, want)
		}
	}
	if sub.Lost() != 0 {
		t.Fatalf("lost = %d, want 0", sub.Lost())
	}
	ok, detail := sdk.Compatible(sdk.SensorDataQoS(), sdk.DefaultQoS())
	if ok {
		t.Fatalf("best effort to reliable reported compatible")
	}
	if detail == "" {
		t.Fatalf("mismatch reported empty detail")
	}
	raw := []byte("mutable")
	err = pub.Publish(ctx, raw)
	if err != nil {
		t.Fatalf("publish mutable: %v", err)
	}
	raw[0] = 'X'
	callCtx, callCancel := context.WithTimeout(ctx, 5*time.Second)
	defer callCancel()
	msg, err := sub.Next(callCtx)
	if err != nil {
		t.Fatalf("next mutable: %v", err)
	}
	if string(msg) != "mutable" {
		t.Fatalf("ingress copy broken: got %q", msg)
	}
	canceled, cancelFunc := context.WithCancel(context.Background())
	cancelFunc()
	err = pub.Publish(canceled, []byte("late"))
	if err == nil {
		t.Fatalf("publish with canceled context succeeded")
	}
}

// testServices covers deadlines, gone servers, and duplicate names.
func testServices(t *testing.T, ctx context.Context, planner, tracker *sdk.Node) {
	t.Helper()
	server, err := tracker.NewServiceServer("/plan", func(_ context.Context, req []byte) ([]byte, error) {
		return append([]byte("plan-ok:"), req...), nil
	})
	if err != nil {
		t.Fatalf("new service server: %v", err)
	}
	client, err := planner.NewServiceClient("/plan")
	if err != nil {
		t.Fatalf("new service client: %v", err)
	}
	callCtx, callCancel := context.WithTimeout(ctx, 5*time.Second)
	reply, err := client.Call(callCtx, []byte("goal-a3f1"))
	callCancel()
	if err != nil {
		t.Fatalf("service call: %v", err)
	}
	if string(reply) != "plan-ok:goal-a3f1" {
		t.Fatalf("reply = %q, want plan-ok:goal-a3f1", reply)
	}
	slow, err := tracker.NewServiceServer("/slow", func(callCtx context.Context, _ []byte) ([]byte, error) {
		select {
		case <-callCtx.Done():
			cerr := callCtx.Err()
			if cerr != nil {
				return nil, cerr
			}
			return nil, fmt.Errorf("slow handler: context done")
		case <-time.After(5 * time.Second):
			return []byte("slow-ok"), nil
		}
	})
	if err != nil {
		t.Fatalf("new slow server: %v", err)
	}
	defer func() {
		_ = slow.Close()
	}()
	slowCtx, slowCancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer slowCancel()
	_, err = client.Call(slowCtx, []byte("x"))
	_ = err
	deadlineCtx, deadlineCancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer deadlineCancel()
	slowClient, err := planner.NewServiceClient("/slow")
	if err != nil {
		t.Fatalf("new slow client: %v", err)
	}
	_, err = slowClient.Call(deadlineCtx, []byte("x"))
	if err == nil {
		t.Fatalf("slow call beat its deadline")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("slow call error = %v, want deadline exceeded", err)
	}
	_, err = tracker.NewServiceServer("/plan", func(_ context.Context, req []byte) ([]byte, error) {
		return req, nil
	})
	if err == nil {
		t.Fatalf("duplicate service registration succeeded")
	}
	err = server.Close()
	if err != nil {
		t.Fatalf("close server: %v", err)
	}
	goneCtx, goneCancel := context.WithTimeout(ctx, 5*time.Second)
	defer goneCancel()
	_, err = client.Call(goneCtx, []byte("after-close"))
	if err == nil {
		t.Fatalf("call to gone server succeeded")
	}
	if !errors.Is(err, sdk.ErrNoSuchService) {
		t.Fatalf("gone server error = %v, want no such service", err)
	}
	canceled, cancelFunc := context.WithCancel(context.Background())
	cancelFunc()
	_, err = client.Call(canceled, []byte("canceled"))
	if err == nil {
		t.Fatalf("call with canceled context succeeded")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled call error = %v, want canceled", err)
	}
}

// testSupervision covers serial order, bond liveness, and pooled work.
func testSupervision(t *testing.T) {
	t.Helper()
	group, err := sdk.NewSerialGroup("bringup")
	if err != nil {
		t.Fatalf("new serial group: %v", err)
	}
	var orderMu sync.Mutex
	var order []string
	attach := func(name string, priority int) {
		t.Helper()
		done := false
		err := group.Attach(sdk.SerialEntity{
			Name:     name,
			Priority: priority,
			Take: func() bool {
				if done {
					return false
				}
				done = true
				orderMu.Lock()
				order = append(order, name)
				orderMu.Unlock()
				return true
			},
		})
		if err != nil {
			t.Fatalf("attach %s: %v", name, err)
		}
	}
	attach("planner_server", 2)
	attach("odometry_filter", 1)
	attach("motor_driver", 0)
	err = group.Configure()
	if err != nil {
		t.Fatalf("serial configure: %v", err)
	}
	err = group.Activate()
	if err != nil {
		t.Fatalf("serial activate: %v", err)
	}
	for i := 0; i < 3; i++ {
		if !group.SpinOnce() {
			t.Fatalf("spin %d did no work", i)
		}
	}
	if group.SpinOnce() {
		t.Fatalf("extra spin did work with nothing ready")
	}
	want := []string{"motor_driver", "odometry_filter", "planner_server"}
	if len(order) != len(want) {
		t.Fatalf("serial order = %v, want %v", order, want)
	}
	for i := range want {
		if order[i] != want[i] {
			t.Fatalf("serial order = %v, want %v", order, want)
		}
	}
	fake := sdk.NewFakeClock(0)
	now, err := fake.Advance(1000000000)
	if err != nil {
		t.Fatalf("fake advance: %v", err)
	}
	err = group.RecordHeartbeat("motor_driver", now)
	if err != nil {
		t.Fatalf("record heartbeat: %v", err)
	}
	alive, err := group.BondAlive("motor_driver", now)
	if err != nil {
		t.Fatalf("bond alive: %v", err)
	}
	if !alive {
		t.Fatalf("bond with fresh heartbeat reported dead")
	}
	later, err := fake.Advance(10 * 1000000000)
	if err != nil {
		t.Fatalf("fake advance late: %v", err)
	}
	dead := group.CheckBonds(later)
	if len(dead) == 0 {
		t.Fatalf("expired bonds reported none dead")
	}
	err = group.Deactivate()
	if err != nil {
		t.Fatalf("serial deactivate: %v", err)
	}
	if group.SpinOnce() {
		t.Fatalf("spin after deactivate did work")
	}
	err = group.Close()
	if err != nil {
		t.Fatalf("serial close: %v", err)
	}
	err = group.Close()
	if err != nil {
		t.Fatalf("serial second close: %v", err)
	}
	pooled, err := sdk.NewPooledGroup(2, 16)
	if err != nil {
		t.Fatalf("new pooled group: %v", err)
	}
	defer func() {
		_ = pooled.Close() //nolint:errcheck // test teardown is best effort
	}()
	const tasks = 8
	var wg sync.WaitGroup
	wg.Add(tasks)
	var poolMu sync.Mutex
	ran := 0
	for i := 0; i < tasks; i++ {
		_, err = pooled.Submit(func() {
			poolMu.Lock()
			ran++
			poolMu.Unlock()
			wg.Done()
		})
		if err != nil {
			t.Fatalf("pooled submit: %v", err)
		}
	}
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatalf("pooled tasks did not finish")
	}
	if ran != tasks {
		t.Fatalf("pooled ran = %d, want %d", ran, tasks)
	}
	if pooled.Lost() != 0 {
		t.Fatalf("pooled lost = %d, want 0", pooled.Lost())
	}
}

// testTeardown covers idempotent close and post-close errors.
func testTeardown(t *testing.T, planner, tracker *sdk.Node) {
	t.Helper()
	err := planner.Close()
	if err != nil {
		t.Fatalf("planner close: %v", err)
	}
	err = planner.Close()
	if err != nil {
		t.Fatalf("planner second close: %v", err)
	}
	if !planner.IsClosed() {
		t.Fatalf("planner not marked closed")
	}
	_, err = planner.NewPublisher("/late", "std_msgs/msg/String", sdk.DefaultQoS())
	if err == nil {
		t.Fatalf("publisher on closed node succeeded")
	}
	_, err = planner.NewServiceClient("/late")
	if err == nil {
		t.Fatalf("client on closed node succeeded")
	}
	select {
	case <-planner.Done():
	default:
		t.Fatalf("done not closed after Close")
	}
	err = tracker.Close()
	if err != nil {
		t.Fatalf("tracker close: %v", err)
	}
	err = tracker.Close()
	if err != nil {
		t.Fatalf("tracker second close: %v", err)
	}
}
