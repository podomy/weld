// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package core

import (
	"errors"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestSerialSupervision covers serial group delivery, lifecycle, bond, and
// subscriber channels: ordered one at a time, insertion order for equal
// priority, lifecycle forward plus reverse, destroy while spinning, and
// bounded channels with lost counters.
func TestSerialSupervision(t *testing.T) {
	t.Run("channel depth from QoS with drop oldest", checkSerialChannelQoS)
	t.Run("channel overflow counts lost", checkSerialChannelOverflow)
	t.Run("channel close drains then rejects", checkSerialChannelClose)
	t.Run("equal priority follows insertion order", checkSerialInsertionOrder)
	t.Run("priority orders delivery", checkSerialPriorityOrder)
	t.Run("callbacks never overlap", checkSerialOneAtATime)
	t.Run("lifecycle forward and reverse", checkSerialLifecycleOrder)
	t.Run("late attach auto activates", checkSerialLateAttach)
	t.Run("deactivation rejects work", checkSerialDeactivateRejects)
	t.Run("bond liveness and respawn", checkSerialBond)
	t.Run("destroy while spinning joins cleanly", checkSerialDestroyWhileSpinning)
	t.Run("detached spin delivers channel work", checkSerialDetachedSpin)
	t.Run("attach validation", checkSerialAttachValidation)
}

// serialOrderGuard records callback order with a mutex for race clean tests.
type serialOrderGuard struct {
	mu    sync.Mutex
	order []string
}

// serialRecord appends name to the recorded order.
func (g *serialOrderGuard) serialRecord(name string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.order = append(g.order, name)
}

// serialSnapshot returns a copy of the recorded order.
func (g *serialOrderGuard) serialSnapshot() []string {
	g.mu.Lock()
	defer g.mu.Unlock()
	out := make([]string, len(g.order))
	copy(out, g.order)
	return out
}

// newSerialTestGroup builds an active group with the named entities attached.
// Lifecycle callbacks are nil so Configure plus Activate succeed trivially.
func newSerialTestGroup(t *testing.T, names ...SerialEntity) *SerialGroup {
	t.Helper()
	group, err := NewSerialGroup("test")
	if err != nil {
		t.Fatalf("new serial group: %v", err)
	}
	for _, entity := range names {
		err = group.Attach(entity)
		if err != nil {
			t.Fatalf("attach %q: %v", entity.Name, err)
		}
	}
	err = group.Configure()
	if err != nil {
		t.Fatalf("configure: %v", err)
	}
	err = group.Activate()
	if err != nil {
		t.Fatalf("activate: %v", err)
	}
	return group
}

// checkSerialChannelQoS pins subscriber channel depth from QoS profiles.
func checkSerialChannelQoS(t *testing.T) {
	t.Helper()
	channel, err := NewSerialChannelFromQoS[int](DefaultQoS())
	if err != nil {
		t.Fatalf("channel from default QoS: %v", err)
	}
	if channel.Depth() != int(DefaultHistoryDepth) {
		t.Fatalf("default QoS channel depth got %d want %d", channel.Depth(), DefaultHistoryDepth)
	}
	rosout, err := NewSerialChannelFromQoS[int](RosoutQoS())
	if err != nil {
		t.Fatalf("channel from rosout QoS: %v", err)
	}
	if rosout.Depth() != 1000 {
		t.Fatalf("rosout channel depth got %d want 1000", rosout.Depth())
	}
	_, err = NewSerialChannelFromQoS[int](SystemDefaultQoS())
	if err == nil {
		t.Fatal("unresolved QoS must fail channel creation")
	}
	_, err = NewSerialChannel[int](0)
	if err == nil {
		t.Fatal("zero depth must fail channel creation")
	}
	_, err = NewSerialGroup("")
	if err == nil {
		t.Fatal("empty group name must fail creation")
	}
}

// checkSerialChannelOverflow pins drop oldest with a lost counter.
func checkSerialChannelOverflow(t *testing.T) {
	t.Helper()
	channel, err := NewSerialChannel[int](2)
	if err != nil {
		t.Fatalf("new channel: %v", err)
	}
	if channel.TryPublish(1) {
		t.Fatal("first publish must not evict")
	}
	if channel.TryPublish(2) {
		t.Fatal("second publish must not evict")
	}
	if !channel.TryPublish(3) {
		t.Fatal("third publish into depth 2 must evict oldest")
	}
	if channel.Lost() != 1 {
		t.Fatalf("lost got %d want 1", channel.Lost())
	}
	first, ok := channel.Take()
	if !ok || first != 2 {
		t.Fatalf("first take got %d,%v want 2,true", first, ok)
	}
	second, ok := channel.Take()
	if !ok || second != 3 {
		t.Fatalf("second take got %d,%v want 3,true", second, ok)
	}
	_, ok = channel.Take()
	if ok {
		t.Fatal("empty take must report false")
	}
	if channel.Len() != 0 {
		t.Fatalf("drained len got %d want 0", channel.Len())
	}
}

// checkSerialChannelClose pins close drains then rejects publishes.
func checkSerialChannelClose(t *testing.T) {
	t.Helper()
	channel, err := NewSerialChannel[int](4)
	if err != nil {
		t.Fatalf("new channel: %v", err)
	}
	if channel.Closed() {
		t.Fatal("fresh channel must not report closed")
	}
	_ = channel.TryPublish(7)
	_ = channel.TryPublish(8)
	channel.Close()
	if !channel.Closed() {
		t.Fatal("closed channel must report closed")
	}
	if channel.TryPublish(9) {
		t.Fatal("publish after close must not evict")
	}
	if channel.Lost() != 0 {
		t.Fatalf("close drop must not count lost, got %d", channel.Lost())
	}
	first, ok := channel.Take()
	if !ok || first != 7 {
		t.Fatalf("drain first got %d,%v want 7,true", first, ok)
	}
	second, ok := channel.Take()
	if !ok || second != 8 {
		t.Fatalf("drain second got %d,%v want 8,true", second, ok)
	}
	_, ok = channel.Take()
	if ok {
		t.Fatal("drained closed channel must report false")
	}
}

// checkSerialInsertionOrder pins equal priority runs in attach order.
func checkSerialInsertionOrder(t *testing.T) {
	t.Helper()
	guard := &serialOrderGuard{}
	mkTake := func(name string, guard *serialOrderGuard) func() bool {
		done := false
		return func() bool {
			if done {
				return false
			}
			done = true
			guard.serialRecord(name)
			return true
		}
	}
	group := newSerialTestGroup(t,
		SerialEntity{Name: "first", Priority: 0, Take: mkTake("first", guard)},
		SerialEntity{Name: "second", Priority: 0, Take: mkTake("second", guard)},
		SerialEntity{Name: "third", Priority: 0, Take: mkTake("third", guard)},
	)
	for i := range 3 {
		if !group.SpinOnce() {
			t.Fatalf("spin %d must find work", i)
		}
	}
	if group.SpinOnce() {
		t.Fatal("drained group must report no work")
	}
	got := guard.serialSnapshot()
	want := []string{"first", "second", "third"}
	assertSerialOrder(t, got, want)
	if got := group.Order(); len(got) != 3 || got[0] != "first" || got[2] != "third" {
		t.Fatalf("introspected order got %v want insertion order", got)
	}
	group.Destroy()
}

// checkSerialPriorityOrder pins lower priority values run first.
func checkSerialPriorityOrder(t *testing.T) {
	t.Helper()
	guard := &serialOrderGuard{}
	ready := map[string]*bool{"low": new(bool), "high": new(bool), "mid": new(bool)}
	mkTake := func(name string, flag *bool, guard *serialOrderGuard) func() bool {
		return func() bool {
			if !*flag {
				return false
			}
			*flag = false
			guard.serialRecord(name)
			return true
		}
	}
	*ready["low"] = true
	*ready["high"] = true
	*ready["mid"] = true
	group := newSerialTestGroup(t,
		SerialEntity{Name: "low", Priority: 5, Take: mkTake("low", ready["low"], guard)},
		SerialEntity{Name: "high", Priority: 0, Take: mkTake("high", ready["high"], guard)},
		SerialEntity{Name: "mid", Priority: 3, Take: mkTake("mid", ready["mid"], guard)},
	)
	for i := range 3 {
		if !group.SpinOnce() {
			t.Fatalf("spin %d must find work", i)
		}
	}
	assertSerialOrder(t, guard.serialSnapshot(), []string{"high", "mid", "low"})
	group.Destroy()
}

// checkSerialOneAtATime pins concurrent spinners never overlap callbacks.
func checkSerialOneAtATime(t *testing.T) {
	t.Helper()
	var inFlight atomic.Int32
	var maxSeen atomic.Int32
	observe := func(current int32) {
		for {
			seen := maxSeen.Load()
			if current <= seen {
				return
			}
			if maxSeen.CompareAndSwap(seen, current) {
				return
			}
		}
	}
	mkTake := func() func() bool {
		return func() bool {
			current := inFlight.Add(1)
			observe(current)
			runtime.Gosched()
			inFlight.Add(-1)
			return true
		}
	}
	group := newSerialTestGroup(t,
		SerialEntity{Name: "a", Take: mkTake()},
		SerialEntity{Name: "b", Take: mkTake()},
	)
	var wg sync.WaitGroup
	for range 4 {
		wg.Go(func() {
			for range 50 {
				group.SpinOnce()
			}
		})
	}
	wg.Wait()
	if got := maxSeen.Load(); got != 1 {
		t.Fatalf("max concurrent callbacks got %d want 1", got)
	}
	group.Destroy()
}

// checkSerialLifecycleOrder pins forward configure plus activate and reverse teardown.
//
//nolint:cyclop // lifecycle test sequences configure, activate, and teardown checks in one run, splitting would obscure intent
func checkSerialLifecycleOrder(t *testing.T) {
	t.Helper()
	guard := &serialOrderGuard{}
	mkEntity := func(name string, guard *serialOrderGuard) SerialEntity {
		return SerialEntity{
			Name:       name,
			Configure:  func() error { guard.serialRecord("configure " + name); return nil },
			Activate:   func() error { guard.serialRecord("activate " + name); return nil },
			Deactivate: func() error { guard.serialRecord("deactivate " + name); return nil },
			Cleanup:    func() error { guard.serialRecord("cleanup " + name); return nil },
			Shutdown:   func() error { guard.serialRecord("shutdown " + name); return nil },
		}
	}
	group, err := NewSerialGroup("bringup")
	if err != nil {
		t.Fatalf("new group: %v", err)
	}
	for _, name := range []string{"motor_driver", "odometry_filter", "planner_server"} {
		err = group.Attach(mkEntity(name, guard))
		if err != nil {
			t.Fatalf("attach %s: %v", name, err)
		}
	}
	err = group.Activate()
	if err == nil {
		t.Fatal("activate before configure must fail")
	}
	err = group.Configure()
	if err != nil {
		t.Fatalf("configure: %v", err)
	}
	err = group.Activate()
	if err != nil {
		t.Fatalf("activate: %v", err)
	}
	assertSerialOrder(t, guard.serialSnapshot(), []string{
		"configure motor_driver", "configure odometry_filter", "configure planner_server",
		"activate motor_driver", "activate odometry_filter", "activate planner_server",
	})
	err = group.Cleanup()
	if err == nil {
		t.Fatal("cleanup while active must fail")
	}
	err = group.Deactivate()
	if err != nil {
		t.Fatalf("deactivate: %v", err)
	}
	err = group.Cleanup()
	if err != nil {
		t.Fatalf("cleanup: %v", err)
	}
	err = group.RegisterPreshutdown(func() { guard.serialRecord("preshutdown") })
	if err != nil {
		t.Fatalf("register preshutdown: %v", err)
	}
	err = group.Shutdown()
	if err != nil {
		t.Fatalf("shutdown: %v", err)
	}
	got := guard.serialSnapshot()
	wantTail := []string{
		"deactivate planner_server", "deactivate odometry_filter", "deactivate motor_driver",
		"cleanup planner_server", "cleanup odometry_filter", "cleanup motor_driver",
		"preshutdown",
		"shutdown planner_server", "shutdown odometry_filter", "shutdown motor_driver",
	}
	if len(got) < len(wantTail) {
		t.Fatalf("lifecycle trace too short: %v", got)
	}
	assertSerialOrder(t, got[len(got)-len(wantTail):], wantTail)
	if group.IsActive() || group.IsConfigured() {
		t.Fatal("shutdown must clear active and configured")
	}
	group.Destroy()
}

// checkSerialLateAttach pins late entities auto configure plus activate.
func checkSerialLateAttach(t *testing.T) {
	t.Helper()
	guard := &serialOrderGuard{}
	group, err := NewSerialGroup("late")
	if err != nil {
		t.Fatalf("new group: %v", err)
	}
	err = group.Attach(SerialEntity{Name: "early"})
	if err != nil {
		t.Fatalf("attach early: %v", err)
	}
	err = group.Configure()
	if err != nil {
		t.Fatalf("configure: %v", err)
	}
	err = group.Activate()
	if err != nil {
		t.Fatalf("activate: %v", err)
	}
	lateTake := func() bool {
		guard.serialRecord("late")
		return true
	}
	err = group.Attach(SerialEntity{
		Name:      "late",
		Configure: func() error { guard.serialRecord("configure late"); return nil },
		Activate:  func() error { guard.serialRecord("activate late"); return nil },
		Take:      lateTake,
	})
	if err != nil {
		t.Fatalf("late attach: %v", err)
	}
	assertSerialOrder(t, guard.serialSnapshot(), []string{"configure late", "activate late"})
	if !group.SpinOnce() {
		t.Fatal("late entity work must run while active")
	}
	if got := guard.serialSnapshot(); got[len(got)-1] != "late" {
		t.Fatalf("late work missing, trace %v", got)
	}
	group.Destroy()
}

// checkSerialDeactivateRejects pins SpinOnce skips Take while inactive.
func checkSerialDeactivateRejects(t *testing.T) {
	t.Helper()
	var calls atomic.Int32
	group := newSerialTestGroup(t, SerialEntity{Name: "work", Take: func() bool {
		calls.Add(1)
		return true
	}})
	if !group.SpinOnce() {
		t.Fatal("active spin must find work")
	}
	if calls.Load() != 1 {
		t.Fatalf("take calls got %d want 1", calls.Load())
	}
	err := group.Deactivate()
	if err != nil {
		t.Fatalf("deactivate: %v", err)
	}
	if group.SpinOnce() {
		t.Fatal("inactive spin must report no work")
	}
	if calls.Load() != 1 {
		t.Fatalf("inactive spin must skip Take, calls got %d", calls.Load())
	}
	group.Destroy()
}

// checkSerialBond pins heartbeat liveness, timeouts, and respawn polling.
//
//nolint:gocognit,cyclop // bond test sequences heartbeat, timeout, and respawn checks in one run, splitting would obscure intent
func checkSerialBond(t *testing.T) {
	t.Helper()
	group, err := NewSerialGroup("bond")
	if err != nil {
		t.Fatalf("new group: %v", err)
	}
	for _, name := range []string{"nav", "planner"} {
		err = group.Attach(SerialEntity{Name: name})
		if err != nil {
			t.Fatalf("attach %s: %v", name, err)
		}
	}
	if dead := group.CheckBonds(0); len(dead) != 2 {
		t.Fatalf("no heartbeat means all dead, got %v", dead)
	}
	err = group.RecordHeartbeat("nav", 0)
	if err != nil {
		t.Fatalf("heartbeat: %v", err)
	}
	alive, err := group.BondAlive("nav", SerialBondManagerTimeoutNanos-1)
	if err != nil {
		t.Fatalf("bond alive: %v", err)
	}
	if !alive {
		t.Fatal("heartbeat must be alive just inside manager timeout")
	}
	alive, err = group.BondAlive("nav", SerialBondManagerTimeoutNanos+1)
	if err != nil {
		t.Fatalf("bond alive: %v", err)
	}
	if alive {
		t.Fatal("heartbeat must be dead just past manager timeout")
	}
	serviceAlive, err := group.BondServiceAlive("nav", SerialBondManagerTimeoutNanos+1)
	if err != nil {
		t.Fatalf("service bond alive: %v", err)
	}
	if !serviceAlive {
		t.Fatal("manager timeout must still be inside service timeout")
	}
	serviceAlive, err = group.BondServiceAlive("nav", SerialBondServiceTimeoutNanos+1)
	if err != nil {
		t.Fatalf("service bond alive: %v", err)
	}
	if serviceAlive {
		t.Fatal("heartbeat must be dead past service timeout")
	}
	dead := group.CheckBonds(SerialBondManagerTimeoutNanos + 1)
	assertSerialOrder(t, dead, []string{"nav", "planner"})
	serviceDead := group.CheckServiceBonds(SerialBondManagerTimeoutNanos + 1)
	assertSerialOrder(t, serviceDead, []string{"planner"})
	err = group.RecordHeartbeat("nav", SerialBondManagerTimeoutNanos+2)
	if err != nil {
		t.Fatalf("respawn heartbeat: %v", err)
	}
	alive, err = group.BondAlive("nav", SerialBondManagerTimeoutNanos+2)
	if err != nil {
		t.Fatalf("bond alive: %v", err)
	}
	if !alive {
		t.Fatal("fresh heartbeat must respawn liveness")
	}
	last, has, err := group.BondLastSeen("nav")
	if err != nil {
		t.Fatalf("last seen: %v", err)
	}
	if !has || last != SerialBondManagerTimeoutNanos+2 {
		t.Fatalf("last seen got %d,%v", last, has)
	}
	err = group.RecordHeartbeat("missing", 0)
	if err == nil {
		t.Fatal("heartbeat for unknown entity must fail")
	}
	_, err = group.BondAlive("missing", 0)
	if err == nil {
		t.Fatal("bond check for unknown entity must fail")
	}
	group.Destroy()
}

// checkSerialDestroyWhileSpinning pins destroy skips pending and joins cleanly.
func checkSerialDestroyWhileSpinning(t *testing.T) {
	t.Helper()
	var count atomic.Int64
	group := newSerialTestGroup(t, SerialEntity{Name: "busy", Take: func() bool {
		count.Add(1)
		return true
	}})
	stop := make(chan struct{})
	exited := make(chan struct{})
	go func() {
		group.Spin(stop)
		close(exited)
	}()
	waitSerialCount(t, &count, 50, 5*time.Second)
	group.Destroy()
	group.Destroy()
	select {
	case <-exited:
	case <-time.After(5 * time.Second):
		t.Fatal("spin did not exit after destroy")
	}
	select {
	case <-stop:
		t.Fatal("destroy must not close the caller stop channel")
	default:
	}
	close(stop)
	frozen := count.Load()
	if group.SpinOnce() {
		t.Fatal("spin once after destroy must report no work")
	}
	if count.Load() != frozen {
		t.Fatal("destroy must skip pending callbacks")
	}
}

// checkSerialDetachedSpin pins a detached Spin thread delivers channel work in order.
func checkSerialDetachedSpin(t *testing.T) {
	t.Helper()
	group, err := NewSerialGroup("detached")
	if err != nil {
		t.Fatalf("new group: %v", err)
	}
	channel, err := NewSerialChannelFromQoS[int](DefaultQoS())
	if err != nil {
		t.Fatalf("new channel: %v", err)
	}
	var handled atomic.Int64
	var sum atomic.Int64
	err = AttachSerialChannel(group, "controller", 0, channel, func(v int) {
		handled.Add(1)
		sum.Add(int64(v))
	})
	if err != nil {
		t.Fatalf("attach channel: %v", err)
	}
	err = group.Configure()
	if err != nil {
		t.Fatalf("configure: %v", err)
	}
	err = group.Activate()
	if err != nil {
		t.Fatalf("activate: %v", err)
	}
	stop := make(chan struct{})
	exited := make(chan struct{})
	go func() {
		group.Spin(stop)
		close(exited)
	}()
	for i := 1; i <= 10; i++ {
		_ = channel.TryPublish(i)
	}
	deadline := time.Now().Add(5 * time.Second)
	for handled.Load() < 10 {
		if time.Now().After(deadline) {
			t.Fatalf("detached spin handled %d of 10", handled.Load())
		}
		runtime.Gosched()
	}
	if sum.Load() != 55 {
		t.Fatalf("handled sum got %d want 55", sum.Load())
	}
	close(stop)
	select {
	case <-exited:
	case <-time.After(5 * time.Second):
		t.Fatal("detached spin did not exit on stop")
	}
	group.Destroy()
}

// checkSerialAttachValidation pins attach and detach error paths.
//
//nolint:cyclop // attach test sequences validation, duplicate, and detach checks in one run, splitting would obscure intent
func checkSerialAttachValidation(t *testing.T) {
	t.Helper()
	group, err := NewSerialGroup("validate")
	if err != nil {
		t.Fatalf("new group: %v", err)
	}
	err = group.Attach(SerialEntity{})
	if err == nil {
		t.Fatal("empty entity name must fail")
	}
	err = group.Attach(SerialEntity{Name: "a"})
	if err != nil {
		t.Fatalf("attach a: %v", err)
	}
	err = group.Attach(SerialEntity{Name: "a"})
	if err == nil {
		t.Fatal("duplicate attach must fail")
	}
	err = group.Detach("missing")
	if err == nil {
		t.Fatal("detach unknown must fail")
	}
	err = group.Detach("")
	if err == nil {
		t.Fatal("detach empty must fail")
	}
	err = group.Configure()
	if err != nil {
		t.Fatalf("configure: %v", err)
	}
	failing := errors.New("boom")
	err = group.Attach(SerialEntity{Name: "bad", Configure: func() error { return fmt.Errorf("configure: %w", failing) }})
	if err == nil {
		t.Fatal("failing late configure must fail attach")
	}
	err = group.Activate()
	if err != nil {
		t.Fatalf("activate: %v", err)
	}
	err = group.Attach(SerialEntity{Name: "late-bad", Activate: func() error { return fmt.Errorf("activate: %w", failing) }})
	if err == nil {
		t.Fatal("failing late activate must fail attach")
	}
	err = group.RegisterPreshutdown(nil)
	if err == nil {
		t.Fatal("nil preshutdown must fail")
	}
	group.Destroy()
	err = group.Attach(SerialEntity{Name: "after"})
	if err == nil {
		t.Fatal("attach after destroy must fail")
	}
}

// waitSerialCount waits until count reaches want or the deadline passes.
func waitSerialCount(t *testing.T, count *atomic.Int64, want int64, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for count.Load() < want {
		if time.Now().After(deadline) {
			t.Fatalf("spin count %d did not reach %d", count.Load(), want)
		}
		runtime.Gosched()
	}
}

// assertSerialOrder asserts two name slices match exactly in order.
func assertSerialOrder(t *testing.T, got, want []string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("order len got %v want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("order got %v want %v", got, want)
		}
	}
}
