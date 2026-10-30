// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package core

import (
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestPooledSupervision covers the pooled group contract: declared worker
// bound, fairness across lanes, isolation under a blocked handler, drop
// oldest with independent lost counters, copy once publish sharing, loud
// exclusive refusal, independent depths, and clean close.
func TestPooledSupervision(t *testing.T) {
	t.Run("validation", checkPooledValidation)
	t.Run("nil safe", checkPooledNilSafe)
	t.Run("bounded parallelism", checkPooledBoundedParallelism)
	t.Run("fairness", checkPooledFairness)
	t.Run("isolation under blocked handler", checkPooledIsolation)
	t.Run("drop oldest and independent counters", checkPooledDropOldest)
	t.Run("copy once sharing", checkPooledCopyOnce)
	t.Run("exclusive refused", checkPooledExclusiveRefused)
	t.Run("independent depths", checkPooledIndependentDepths)
	t.Run("close skips pending and joins", checkPooledClose)
}

// waitPooled polls cond until true or timeout, failing the test on timeout.
func waitPooled(t *testing.T, timeout time.Duration, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	if !cond() {
		t.Fatalf("pooled: timed out waiting: %s", msg)
	}
}

// mustAttachPooled attaches a lane or fails the test.
func mustAttachPooled[T any](t *testing.T, g *PooledGroup[T], name string, handle func(T)) *PooledSubscription[T] {
	t.Helper()
	sub, err := g.Attach(name, handle)
	if err != nil {
		t.Fatalf("pooled: attach %q failed: %v", name, err)
	}
	return sub
}

// checkPooledValidation pins constructor and attach rejection rules.
func checkPooledValidation(t *testing.T) {
	t.Helper()
	_, err := NewPooledGroup[int](0, 8)
	if err == nil {
		t.Fatalf("pooled: zero workers must fail")
	}
	_, err = NewPooledGroup[int](-1, 8)
	if err == nil {
		t.Fatalf("pooled: negative workers must fail")
	}
	_, err = NewPooledGroup[int](2, 0)
	if err == nil {
		t.Fatalf("pooled: zero depth must fail")
	}
	g, err := NewPooledGroup[int](2, 8)
	if err != nil {
		t.Fatalf("pooled: new group failed: %v", err)
	}
	defer g.Close()
	if g.Workers() != 2 || g.Depth() != 8 {
		t.Fatalf("pooled: bound mismatch workers=%d depth=%d", g.Workers(), g.Depth())
	}
	_, err = g.Attach("", func(int) {})
	if err == nil {
		t.Fatalf("pooled: empty name must fail")
	}
	_, err = g.Attach("a", nil)
	if err == nil {
		t.Fatalf("pooled: nil handler must fail")
	}
	mustAttachPooled(t, g, "a", func(int) {})
	_, err = g.Attach("a", func(int) {})
	if err == nil {
		t.Fatalf("pooled: duplicate name must fail")
	}
	if g.SubscriptionCount() != 1 {
		t.Fatalf("pooled: count must stay 1 after refused attaches, got %d", g.SubscriptionCount())
	}
}

// checkPooledNilSafe pins nil receiver behavior.
func checkPooledNilSafe(t *testing.T) {
	t.Helper()
	var g *PooledGroup[int]
	if g.Workers() != 0 || g.Depth() != 0 || g.SubscriptionCount() != 0 {
		t.Fatalf("pooled: nil group getters must report zero")
	}
	if g.Publish(1) != 0 {
		t.Fatalf("pooled: nil group publish must report zero evictions")
	}
	g.Close()
	_, err := g.Attach("a", func(int) {})
	if err == nil {
		t.Fatalf("pooled: nil group attach must fail")
	}
	err = g.AttachExclusive("a", func(int) {})
	if err == nil {
		t.Fatalf("pooled: nil group exclusive attach must fail")
	}
	var s *PooledSubscription[int]
	if s.Submit(1) {
		t.Fatalf("pooled: nil subscription submit must report no eviction")
	}
	if s.Lost() != 0 || s.Len() != 0 || s.Name() != "" {
		t.Fatalf("pooled: nil subscription getters must report zero")
	}
}

// checkPooledBoundedParallelism soaks parallel goals and pins max concurrency.
//
//nolint:gocognit,cyclop // parallelism test sequences soak, bound, and concurrency checks in one run, splitting would obscure intent
func checkPooledBoundedParallelism(t *testing.T) {
	t.Helper()
	const workers = 4
	const lanes = 3
	const perLane = 100
	g, err := NewPooledGroup[int](workers, 128)
	if err != nil {
		t.Fatalf("pooled: new group failed: %v", err)
	}
	defer g.Close()
	var current atomic.Int64
	var peak atomic.Int64
	var total atomic.Int64
	var wg sync.WaitGroup
	wg.Add(lanes * perLane)
	for i := range lanes {
		name := string(rune('a' + i))
		mustAttachPooled(t, g, name, func(int) {
			cur := current.Add(1)
			for {
				old := peak.Load()
				if cur <= old || peak.CompareAndSwap(old, cur) {
					break
				}
			}
			time.Sleep(time.Millisecond)
			current.Add(-1)
			total.Add(1)
			wg.Done()
		})
	}
	// One publish fans out to every lane, so perLane publishes yield
	// lanes*perLane executions. Depth 128 holds perLane values per lane.
	for i := range perLane {
		evicted := g.Publish(i)
		if evicted != 0 {
			t.Fatalf("pooled: soak publish evicted %d with depth 128", evicted)
		}
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		wg.Wait()
	}()
	select {
	case <-done:
	case <-time.After(15 * time.Second):
		t.Fatalf("pooled: soak did not finish")
	}
	if got := total.Load(); got != lanes*perLane {
		t.Fatalf("pooled: soak total=%d want %d", got, lanes*perLane)
	}
	if got := peak.Load(); got > workers {
		t.Fatalf("pooled: peak concurrency=%d above bound=%d", got, workers)
	}
	if got := peak.Load(); got < 2 {
		t.Fatalf("pooled: peak concurrency=%d shows no parallelism", got)
	}
}

// checkPooledFairness floods every lane and pins equal progress.
func checkPooledFairness(t *testing.T) {
	t.Helper()
	const lanes = 3
	const perLane = 50
	g, err := NewPooledGroup[int](2, 128)
	if err != nil {
		t.Fatalf("pooled: new group failed: %v", err)
	}
	defer g.Close()
	var counts [lanes]atomic.Int64
	var wg sync.WaitGroup
	wg.Add(lanes * perLane)
	names := [lanes]string{"alpha", "beta", "gamma"}
	for i := range lanes {
		mustAttachPooled(t, g, names[i], func(int) {
			counts[i].Add(1)
			wg.Done()
		})
	}
	// One publish fans out to every lane, so perLane publishes give each
	// lane exactly perLane executions with no overflow at depth 128.
	for i := range perLane {
		if evicted := g.Publish(i); evicted != 0 {
			t.Fatalf("pooled: fairness publish evicted %d", evicted)
		}
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		wg.Wait()
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatalf("pooled: fairness run did not finish")
	}
	for i := range lanes {
		if got := counts[i].Load(); got != perLane {
			t.Fatalf("pooled: lane %q progress=%d want %d: starvation", names[i], got, perLane)
		}
	}
}

// checkPooledIsolation pins that one blocked handler does not starve others.
func checkPooledIsolation(t *testing.T) {
	t.Helper()
	g, err := NewPooledGroup[int](2, 32)
	if err != nil {
		t.Fatalf("pooled: new isolation group failed: %v", err)
	}
	defer g.Close()
	started := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	slowLane := mustAttachPooled(t, g, "slow", func(int) {
		once.Do(func() { close(started) })
		<-release
	})
	var fast atomic.Int64
	var wg sync.WaitGroup
	wg.Add(40)
	fastLaneA := mustAttachPooled(t, g, "fast-a", func(int) {
		fast.Add(1)
		wg.Done()
	})
	fastLaneB := mustAttachPooled(t, g, "fast-b", func(int) {
		fast.Add(1)
		wg.Done()
	})
	if slowLane.Submit(0) {
		t.Fatalf("pooled: first slow submit must not evict")
	}
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatalf("pooled: slow handler did not start")
	}
	for range 20 {
		fastLaneA.Submit(1)
		fastLaneB.Submit(1)
	}
	fastDone := make(chan struct{})
	go func() {
		defer close(fastDone)
		wg.Wait()
	}()
	select {
	case <-fastDone:
	case <-time.After(5 * time.Second):
		t.Fatalf("pooled: fast lanes starved behind blocked handler, fast=%d", fast.Load())
	}
	close(release)
	if got := fast.Load(); got != 40 {
		t.Fatalf("pooled: fast total=%d want 40", got)
	}
}

// checkPooledDropOldest pins newest wins order and per lane counters.
//
//nolint:cyclop // drop test sequences publish, order, and counter checks in one run, splitting would obscure intent
func checkPooledDropOldest(t *testing.T) {
	t.Helper()
	g, err := NewPooledGroup[int](1, 3)
	if err != nil {
		t.Fatalf("pooled: new group failed: %v", err)
	}
	defer g.Close()
	started := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	var mu sync.Mutex
	var order []int
	lane := mustAttachPooled(t, g, "odom", func(v int) {
		first := false
		once.Do(func() {
			first = true
			close(started)
		})
		if first {
			<-release
		}
		mu.Lock()
		order = append(order, v)
		mu.Unlock()
	})
	other := mustAttachPooled(t, g, "quiet", func(int) {})
	if lane.Submit(0) {
		t.Fatalf("pooled: first submit must not evict")
	}
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatalf("pooled: blocking task did not start")
	}
	// Worker holds 0; queue takes 1, 2, 3 then 4 evicts 1.
	for _, tc := range []struct {
		v     int
		evict bool
	}{{1, false}, {2, false}, {3, false}, {4, true}} {
		evicted := lane.Submit(tc.v)
		if evicted != tc.evict {
			t.Fatalf("pooled: submit %d evicted=%v want %v", tc.v, evicted, tc.evict)
		}
	}
	if got := lane.Lost(); got != 1 {
		t.Fatalf("pooled: lane lost=%d want 1", got)
	}
	if got := other.Lost(); got != 0 {
		t.Fatalf("pooled: quiet lane lost=%d want 0: counters must be independent", got)
	}
	if got := lane.Len(); got != 3 {
		t.Fatalf("pooled: lane len=%d want 3", got)
	}
	close(release)
	waitPooled(t, 5*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(order) == 4
	}, "drop oldest drain")
	mu.Lock()
	defer mu.Unlock()
	want := []int{0, 2, 3, 4}
	if len(order) != len(want) {
		t.Fatalf("pooled: order=%v want %v", order, want)
	}
	for i := range want {
		if order[i] != want[i] {
			t.Fatalf("pooled: order=%v want %v: drop oldest must keep newest", order, want)
		}
	}
}

// checkPooledCopyOnce pins publish shares one pointer across lanes.
func checkPooledCopyOnce(t *testing.T) {
	t.Helper()
	type sample struct {
		id int
	}
	g, err := NewPooledGroup[*sample](2, 8)
	if err != nil {
		t.Fatalf("pooled: new group failed: %v", err)
	}
	defer g.Close()
	var wg sync.WaitGroup
	wg.Add(2)
	var mu sync.Mutex
	var seen []*sample
	collect := func(v *sample) {
		mu.Lock()
		seen = append(seen, v)
		mu.Unlock()
		wg.Done()
	}
	mustAttachPooled(t, g, "a", collect)
	mustAttachPooled(t, g, "b", collect)
	in := &sample{id: 7}
	if evictions := g.Publish(in); evictions != 0 {
		t.Fatalf("pooled: publish evictions=%d want 0", evictions)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		wg.Wait()
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatalf("pooled: publish fanout did not finish")
	}
	mu.Lock()
	defer mu.Unlock()
	if len(seen) != 2 {
		t.Fatalf("pooled: fanout seen=%d want 2", len(seen))
	}
	for i, got := range seen {
		if got != in {
			t.Fatalf("pooled: lane %d got %p want %p: ingress must share by reference", i, got, in)
		}
		if got.id != 7 {
			t.Fatalf("pooled: lane %d id=%d want 7", i, got.id)
		}
	}
}

// checkPooledExclusiveRefused pins loud refusal of exclusive declarations.
func checkPooledExclusiveRefused(t *testing.T) {
	t.Helper()
	g, err := NewPooledGroup[int](2, 8)
	if err != nil {
		t.Fatalf("pooled: new group failed: %v", err)
	}
	defer g.Close()
	mustAttachPooled(t, g, "a", func(int) {})
	err = g.AttachExclusive("ctrl", func(int) {})
	if err == nil {
		t.Fatalf("pooled: exclusive attach must fail, never drop silently")
	}
	if !strings.Contains(err.Error(), "serial") {
		t.Fatalf("pooled: exclusive error must name serial group, got %q", err.Error())
	}
	if got := g.SubscriptionCount(); got != 1 {
		t.Fatalf("pooled: refused exclusive must not register, count=%d", got)
	}
}

// checkPooledIndependentDepths pins the logger versus controller split.
//
//nolint:cyclop // depths test sequences logger, controller, and split checks in one run, splitting would obscure intent
func checkPooledIndependentDepths(t *testing.T) {
	t.Helper()
	loggerGroup, err := NewPooledGroup[int](2, 100)
	if err != nil {
		t.Fatalf("pooled: new logger group failed: %v", err)
	}
	defer loggerGroup.Close()
	ctrlGroup, err := NewPooledGroup[int](1, 10)
	if err != nil {
		t.Fatalf("pooled: new controller group failed: %v", err)
	}
	defer ctrlGroup.Close()
	if loggerGroup.Depth() != 100 || ctrlGroup.Depth() != 10 {
		t.Fatalf("pooled: depths logger=%d ctrl=%d want 100 and 10", loggerGroup.Depth(), ctrlGroup.Depth())
	}
	loggerLane := mustAttachPooled(t, loggerGroup, "odom", func(int) {})
	ctrlLane := mustAttachPooled(t, ctrlGroup, "cmd", func(int) {})
	// Fill is racy while workers drain; pin independence structurally: flood
	// the controller lane behind a blocked head and show the logger counter
	// stays zero.
	started := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	blocking, err := NewPooledGroup[int](1, 10)
	if err != nil {
		t.Fatalf("pooled: new blocking group failed: %v", err)
	}
	defer blocking.Close()
	head := mustAttachPooled(t, blocking, "cmd", func(int) {
		once.Do(func() { close(started) })
		<-release
	})
	if head.Submit(0) {
		t.Fatalf("pooled: head submit must not evict")
	}
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatalf("pooled: head did not start")
	}
	for range 12 {
		head.Submit(1)
	}
	if got := head.Lost(); got != 2 {
		t.Fatalf("pooled: controller style lost=%d want 2", got)
	}
	if got := loggerLane.Lost(); got != 0 {
		t.Fatalf("pooled: logger lost=%d want 0: counters must be independent", got)
	}
	if got := ctrlLane.Lost(); got != 0 {
		t.Fatalf("pooled: idle controller lost=%d want 0", got)
	}
	close(release)
}

// checkPooledClose pins pending skip, idempotent join, and post close drops.
func checkPooledClose(t *testing.T) {
	t.Helper()
	g, err := NewPooledGroup[int](1, 8)
	if err != nil {
		t.Fatalf("pooled: new group failed: %v", err)
	}
	started := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	var mu sync.Mutex
	ran := 0
	lane := mustAttachPooled(t, g, "a", func(int) {
		once.Do(func() { close(started) })
		<-release
		mu.Lock()
		ran++
		mu.Unlock()
	})
	if lane.Submit(0) {
		t.Fatalf("pooled: head submit must not evict")
	}
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatalf("pooled: head did not start")
	}
	lane.Submit(1)
	lane.Submit(2)
	lane.Submit(3)
	closed := make(chan struct{})
	go func() {
		defer close(closed)
		g.Close()
	}()
	time.Sleep(50 * time.Millisecond)
	close(release)
	select {
	case <-closed:
	case <-time.After(5 * time.Second):
		t.Fatalf("pooled: close did not join")
	}
	mu.Lock()
	gotRan := ran
	mu.Unlock()
	if gotRan != 1 {
		t.Fatalf("pooled: close ran=%d want 1: pending must be skipped", gotRan)
	}
	g.Close()
	if lane.Submit(99) {
		t.Fatalf("pooled: submit after close must report no eviction")
	}
	if got := lane.Lost(); got != 0 {
		t.Fatalf("pooled: submit after close must not count loss, lost=%d", got)
	}
	if evictions := g.Publish(99); evictions != 0 {
		t.Fatalf("pooled: publish after close must report zero evictions")
	}
	_, err = g.Attach("late", func(int) {})
	if err == nil {
		t.Fatalf("pooled: attach after close must fail")
	}
}
