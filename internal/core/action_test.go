// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package core

import (
	"errors"
	"strings"
	"testing"
)

// TestAction covers the transport free workflow: admission gating, cancel
// propagation, one active plus one pending preempt, deterministic back to
// back replacement, deactivation with terminal results, 5 Hz feedback
// buffering for late attach, atomic take plus dispatch, result caching
// independent of handle lifetime, result timeout from completion, and
// explicit clock mismatch.
func TestAction(t *testing.T) {
	t.Run("admission gating", checkActionAdmission)
	t.Run("cancel propagation", checkActionCancel)
	t.Run("preempt slot", checkActionPreempt)
	t.Run("back to back preempt", checkActionBackToBack)
	t.Run("deactivation", checkActionDeactivate)
	t.Run("feedback late attach", checkActionFeedback)
	t.Run("take dispatch atomic", checkActionTake)
	t.Run("result caching", checkActionResultCache)
	t.Run("result timeout from completion", checkActionResultTimeout)
	t.Run("clock mismatch", checkActionClockMismatch)
	t.Run("terminal states", checkActionTerminal)
	t.Run("bounds and defaults", checkActionBounds)
	t.Run("validation", checkActionValidation)
	t.Run("backward jump", checkActionBackwardJump)
	t.Run("feedback overflow", checkActionFeedbackOverflow)
}

// newActiveServer returns an activated server for tests.
func newActiveServer(t *testing.T, domain ClockDomain, resultTimeout int64) *ActionServer { //nolint:unparam // test helper keeps explicit resultTimeout param for clarity across retention cases
	t.Helper()
	srv, err := NewActionServer("/navigate_to_pose", domain, resultTimeout)
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	srv.Activate(0)
	return srv
}

// checkActionAdmission pins reject when inactive and accept when active.
func checkActionAdmission(t *testing.T) {
	t.Helper()
	srv, err := NewActionServer("/navigate_to_pose", ClockWall, 0)
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	err = srv.AcceptGoal("g1", 0, ClockWall, 0)
	if err == nil {
		t.Fatal("inactive server must reject goals")
	}
	srv.Activate(0)
	if !srv.IsActive() {
		t.Fatal("server must report active after activate")
	}
	err = srv.AcceptGoal("g1", 0, ClockWall, 0)
	if err != nil {
		t.Fatalf("active server must accept goals: %v", err)
	}
	err = srv.AcceptGoal("g1", 0, ClockWall, 0)
	if err == nil {
		t.Fatal("duplicate active id must fail admission")
	}
	err = srv.AcceptGoal("", 0, ClockWall, 0)
	if err == nil {
		t.Fatal("empty goal id must fail admission")
	}
}

// checkActionCancel pins cancel moves active to canceling and take reports it.
//
//nolint:cyclop // cancel test sequences admission, take, and result checks in one run, splitting would obscure intent
func checkActionCancel(t *testing.T) {
	t.Helper()
	srv := newActiveServer(t, ClockWall, 0)
	err := srv.AcceptGoal("g1", 0, ClockWall, 0)
	if err != nil {
		t.Fatalf("accept g1: %v", err)
	}
	err = srv.CancelGoal("g1", 100)
	if err != nil {
		t.Fatalf("cancel g1: %v", err)
	}
	status, ok := srv.GoalStatusOf("g1", 100)
	if !ok || status != GoalCanceling {
		t.Fatalf("cancel must move to canceling, got %s ok=%v", status, ok)
	}
	take := srv.Take()
	if take.Kind != TakeCancel || take.GoalID != "g1" {
		t.Fatalf("take must report cancel once, got %+v", take)
	}
	again := srv.Take()
	if again.Kind != TakeEmpty {
		t.Fatalf("second take after cancel dispatch must be empty, got %+v", again)
	}
	err = srv.CompleteGoal("g1", GoalCanceled, []byte("ok"), 200)
	if err != nil {
		t.Fatalf("complete canceled: %v", err)
	}
	payload, status, err := srv.GetResult("g1", 200)
	if err != nil {
		t.Fatalf("get result: %v", err)
	}
	if status != GoalCanceled || string(payload) != "ok" {
		t.Fatalf("canceled result must round trip, got %s %q", status, payload)
	}
	err = srv.CancelGoal("g1", 200)
	if err == nil {
		t.Fatal("cancel on terminal result must fail")
	}
	err = srv.CancelGoal("missing", 200)
	if err == nil {
		t.Fatal("cancel on unknown goal must fail")
	}
}

// checkActionPreempt pins one active plus one pending with preempt dispatch.
//
//nolint:gocognit,cyclop // preempt test sequences admission, take, and promotion checks in one run, splitting would obscure intent
func checkActionPreempt(t *testing.T) {
	t.Helper()
	srv := newActiveServer(t, ClockWall, 0)
	err := srv.AcceptGoal("a", 0, ClockWall, 0)
	if err != nil {
		t.Fatalf("accept a: %v", err)
	}
	err = srv.AcceptGoal("b", 0, ClockWall, 100)
	if err != nil {
		t.Fatalf("accept b: %v", err)
	}
	active, ok := srv.ActiveGoalID()
	if !ok || active != "a" {
		t.Fatalf("active must be a, got %q ok=%v", active, ok)
	}
	pending, ok := srv.PendingGoalID()
	if !ok || pending != "b" {
		t.Fatalf("pending must be b, got %q ok=%v", pending, ok)
	}
	take := srv.Take()
	if take.Kind != TakePreempt || take.GoalID != "a" {
		t.Fatalf("take must preempt active once, got %+v", take)
	}
	err = srv.CompleteGoal("a", GoalCanceled, []byte("preempted"), 200)
	if err != nil {
		t.Fatalf("complete a: %v", err)
	}
	active, ok = srv.ActiveGoalID()
	if !ok || active != "b" {
		t.Fatalf("pending b must promote to active, got %q ok=%v", active, ok)
	}
	pending, ok = srv.PendingGoalID()
	if ok {
		t.Fatalf("pending slot must clear after promotion, got %q", pending)
	}
	status, ok := srv.GoalStatusOf("b", 200)
	if !ok || status != GoalExecuting {
		t.Fatalf("promoted b must execute, got %s ok=%v", status, ok)
	}
	take = srv.Take()
	if take.Kind != TakeExecute || take.GoalID != "b" {
		t.Fatalf("take must execute promoted b, got %+v", take)
	}
}

// checkActionBackToBack pins deterministic replacement of the pending slot.
//
//nolint:gocognit,cyclop // back to back test sequences admission, replacement, and take checks in one run, splitting would obscure intent
func checkActionBackToBack(t *testing.T) {
	t.Helper()
	srv := newActiveServer(t, ClockWall, 0)
	err := srv.AcceptGoal("a", 0, ClockWall, 0)
	if err != nil {
		t.Fatalf("accept a: %v", err)
	}
	err = srv.AcceptGoal("b", 0, ClockWall, 10)
	if err != nil {
		t.Fatalf("accept b: %v", err)
	}
	err = srv.AcceptGoal("c", 0, ClockWall, 20)
	if err != nil {
		t.Fatalf("accept c: %v", err)
	}
	pending, ok := srv.PendingGoalID()
	if !ok || pending != "c" {
		t.Fatalf("back to back must keep latest pending c, got %q ok=%v", pending, ok)
	}
	active, ok := srv.ActiveGoalID()
	if !ok || active != "a" {
		t.Fatalf("active must stay a, got %q ok=%v", active, ok)
	}
	status, ok := srv.GoalStatusOf("b", 20)
	if !ok || status != GoalAborted {
		t.Fatalf("replaced b must abort terminal, got %s ok=%v", status, ok)
	}
	payload, status, err := srv.GetResult("b", 20)
	if err != nil {
		t.Fatalf("replaced b result must be cached: %v", err)
	}
	if status != GoalAborted {
		t.Fatalf("replaced b status must be aborted, got %s", status)
	}
	if len(payload) != 0 {
		t.Fatalf("replaced b payload must be empty, got %q", payload)
	}
	first := srv.Take()
	if first.Kind != TakePreempt || first.GoalID != "a" {
		t.Fatalf("first take must preempt a once, got %+v", first)
	}
	second := srv.Take()
	if second.Kind != TakeEmpty {
		t.Fatalf("second take must be empty, not a second preempt, got %+v", second)
	}
	if got := srv.EmptyTakes(); got == 0 {
		t.Fatal("empty takes must count the quiet take")
	}
}

// checkActionDeactivate pins teardown terminal results in flight work.
//
//nolint:cyclop // deactivate test sequences admission, teardown, and terminal checks in one run, splitting would obscure intent
func checkActionDeactivate(t *testing.T) {
	t.Helper()
	srv := newActiveServer(t, ClockWall, 0)
	err := srv.AcceptGoal("a", 0, ClockWall, 0)
	if err != nil {
		t.Fatalf("accept a: %v", err)
	}
	err = srv.AcceptGoal("b", 0, ClockWall, 10)
	if err != nil {
		t.Fatalf("accept b: %v", err)
	}
	srv.Deactivate(500000000)
	if srv.IsActive() {
		t.Fatal("server must report inactive after deactivate")
	}
	status, ok := srv.GoalStatusOf("a", 500000000)
	if !ok || status != GoalCanceled {
		t.Fatalf("deactivate must cancel active, got %s ok=%v", status, ok)
	}
	status, ok = srv.GoalStatusOf("b", 500000000)
	if !ok || status != GoalCanceled {
		t.Fatalf("deactivate must cancel pending, got %s ok=%v", status, ok)
	}
	if _, ok := srv.ActiveGoalID(); ok {
		t.Fatal("active slot must clear on deactivate")
	}
	if _, ok := srv.PendingGoalID(); ok {
		t.Fatal("pending slot must clear on deactivate")
	}
	err = srv.AcceptGoal("c", 0, ClockWall, 500000001)
	if err == nil {
		t.Fatal("deactivated server must reject new work")
	}
	err = srv.CancelGoal("a", 500000001)
	if err == nil {
		t.Fatal("cancel while inactive must fail")
	}
	take := srv.Take()
	if take.Kind != TakeEmpty {
		t.Fatalf("take after deactivate must be empty, got %+v", take)
	}
	if got := srv.EmptyTakes(); got == 0 {
		t.Fatal("empty take after deactivate must count")
	}
	srv.Deactivate(600000000)
	status, ok = srv.GoalStatusOf("a", 600000000)
	if !ok || !status.IsTerminal() {
		t.Fatalf("second deactivate must keep terminal results, got %s ok=%v", status, ok)
	}
}

// checkActionFeedback pins 5 Hz buffering with late attach before and after completion.
//
//nolint:cyclop // feedback test sequences publish, snapshot, and completion checks in one run, splitting would obscure intent
func checkActionFeedback(t *testing.T) {
	t.Helper()
	srv := newActiveServer(t, ClockWall, 0)
	err := srv.AcceptGoal("g1", 0, ClockWall, 0)
	if err != nil {
		t.Fatalf("accept g1: %v", err)
	}
	for i, at := range []int64{0, 200000000, 400000000} {
		err = srv.PublishFeedback("g1", []byte{byte(i)}, at)
		if err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}
	snap, err := srv.FeedbackSnapshot("g1", 400000000)
	if err != nil {
		t.Fatalf("snapshot before take: %v", err)
	}
	if len(snap) != 3 {
		t.Fatalf("feedback must buffer before handle attach, got %d", len(snap))
	}
	for i, sample := range snap {
		if sample.Payload[0] != byte(i) {
			t.Fatalf("feedback order must round trip at %d, got %+v", i, sample)
		}
	}
	snap[0].Payload[0] = 99
	again, err := srv.FeedbackSnapshot("g1", 400000000)
	if err != nil {
		t.Fatalf("second snapshot: %v", err)
	}
	if again[0].Payload[0] == 99 {
		t.Fatal("snapshot mutation must not rewrite the buffer")
	}
	err = srv.CompleteGoal("g1", GoalSucceeded, []byte("done"), 500000000)
	if err != nil {
		t.Fatalf("complete g1: %v", err)
	}
	late, err := srv.FeedbackSnapshot("g1", 500000000)
	if err != nil {
		t.Fatalf("late snapshot after completion: %v", err)
	}
	if len(late) != 3 {
		t.Fatalf("feedback must survive completion for late attach, got %d", len(late))
	}
	err = srv.PublishFeedback("g1", []byte{9}, 600000000)
	if err == nil {
		t.Fatal("feedback after completion must fail")
	}
}

// checkActionTake pins atomic take plus dispatch with an empty metric.
func checkActionTake(t *testing.T) {
	t.Helper()
	srv := newActiveServer(t, ClockWall, 0)
	take := srv.Take()
	if take.Kind != TakeEmpty {
		t.Fatalf("take with nothing ready must be empty, got %+v", take)
	}
	if got := srv.EmptyTakes(); got != 1 {
		t.Fatalf("empty takes must be 1, got %d", got)
	}
	err := srv.AcceptGoal("g1", 0, ClockWall, 10)
	if err != nil {
		t.Fatalf("accept g1: %v", err)
	}
	exec := srv.Take()
	if exec.Kind != TakeExecute || exec.GoalID != "g1" {
		t.Fatalf("take must execute once, got %+v", exec)
	}
	quiet := srv.Take()
	if quiet.Kind != TakeEmpty {
		t.Fatalf("second take must be empty, got %+v", quiet)
	}
	if got := srv.EmptyTakes(); got != 2 {
		t.Fatalf("empty takes must be 2, got %d", got)
	}
}

// checkActionResultCache pins results survive slot clearing and handle close.
func checkActionResultCache(t *testing.T) {
	t.Helper()
	srv := newActiveServer(t, ClockWall, 0)
	err := srv.AcceptGoal("g1", 0, ClockWall, 0)
	if err != nil {
		t.Fatalf("accept g1: %v", err)
	}
	err = srv.CompleteGoal("g1", GoalSucceeded, []byte("r1"), 100)
	if err != nil {
		t.Fatalf("complete g1: %v", err)
	}
	if _, ok := srv.ActiveGoalID(); ok {
		t.Fatal("active slot must clear after completion")
	}
	payload, status, err := srv.GetResult("g1", 100)
	if err != nil {
		t.Fatalf("cached result must survive handle close: %v", err)
	}
	if status != GoalSucceeded || string(payload) != "r1" {
		t.Fatalf("cached result must round trip, got %s %q", status, payload)
	}
	payload[0] = 'X'
	fresh, _, err := srv.GetResult("g1", 100)
	if err != nil {
		t.Fatalf("second get: %v", err)
	}
	if string(fresh) != "r1" {
		t.Fatalf("result mutation must not rewrite the cache, got %q", fresh)
	}
	err = srv.AcceptGoal("g2", 0, ClockWall, 100)
	if err != nil {
		t.Fatalf("accept g2 after completion: %v", err)
	}
	_, _, err = srv.GetResult("g2", 100)
	if err == nil {
		t.Fatal("in flight goal must report not complete")
	}
}

// checkActionResultTimeout pins expiry measured from completion, not acceptance.
func checkActionResultTimeout(t *testing.T) {
	t.Helper()
	srv, err := NewActionServer("/navigate_to_pose", ClockWall, 1000000000)
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	srv.Activate(0)
	err = srv.AcceptGoal("g1", 0, ClockWall, 0)
	if err != nil {
		t.Fatalf("accept g1: %v", err)
	}
	err = srv.CompleteGoal("g1", GoalSucceeded, []byte("r"), 10000000000)
	if err != nil {
		t.Fatalf("complete g1: %v", err)
	}
	_, _, err = srv.GetResult("g1", 10000000000+500000000)
	if err != nil {
		t.Fatalf("result inside retention from completion must flow: %v", err)
	}
	_, _, err = srv.GetResult("g1", 10000000000+1000000001)
	if err == nil {
		t.Fatal("result past retention from completion must time out")
	}
	if !strings.Contains(err.Error(), "timed out") {
		t.Fatalf("timeout error must say timed out, got %q", err.Error())
	}
	_, ok := srv.GoalStatusOf("g1", 10000000000+1000000001)
	if ok {
		t.Fatal("expired result must evict from status lookup")
	}
	err = srv.AcceptGoal("g1", 0, ClockWall, 10000000000+1000000001)
	if err != nil {
		t.Fatalf("expired id must be reusable: %v", err)
	}
}

// checkActionClockMismatch pins wall stamp against sim domain as explicit error.
func checkActionClockMismatch(t *testing.T) {
	t.Helper()
	srv := newActiveServer(t, ClockSim, 0)
	err := srv.AcceptGoal("g1", 1234, ClockWall, 1234)
	if err == nil {
		t.Fatal("wall stamp against sim domain must fail")
	}
	var mismatch *ClockMismatchError
	if !errors.As(err, &mismatch) {
		t.Fatalf("mismatch must be a ClockMismatchError, got %T %v", err, err)
	}
	if mismatch.StampNanos != 1234 {
		t.Fatalf("mismatch must carry goal stamp, got %d", mismatch.StampNanos)
	}
	if mismatch.ServerDomain != ClockSim || mismatch.GoalDomain != ClockWall {
		t.Fatalf("mismatch must carry both domains, got %+v", mismatch)
	}
	msg := mismatch.Error()
	if !strings.Contains(msg, "1234") || !strings.Contains(msg, "sim") || !strings.Contains(msg, "wall") {
		t.Fatalf("mismatch text must carry stamp and domains, got %q", msg)
	}
	err = ValidateGoalClock("/a", "g2", 50, ClockSim, ClockSim)
	if err != nil {
		t.Fatalf("matching domains must pass: %v", err)
	}
	err = srv.AcceptGoal("g2", 2000, ClockSim, 2000)
	if err != nil {
		t.Fatalf("matching sim goal must flow: %v", err)
	}
}

// checkActionTerminal pins terminal states guaranteed and guarded transitions.
//
//nolint:cyclop // terminal test sequences completion, promotion, and guard checks in one run, splitting would obscure intent
func checkActionTerminal(t *testing.T) {
	t.Helper()
	if GoalSucceeded.IsTerminal() != true || GoalCanceled.IsTerminal() != true || GoalAborted.IsTerminal() != true {
		t.Fatal("succeeded, canceled, and aborted must be terminal")
	}
	if GoalAccepted.IsTerminal() || GoalExecuting.IsTerminal() || GoalCanceling.IsTerminal() || GoalUnknown.IsTerminal() {
		t.Fatal("accepted, executing, canceling, and unknown must not be terminal")
	}
	srv := newActiveServer(t, ClockWall, 0)
	err := srv.AcceptGoal("g1", 0, ClockWall, 0)
	if err != nil {
		t.Fatalf("accept g1: %v", err)
	}
	err = srv.CompleteGoal("g1", GoalExecuting, []byte("x"), 10)
	if err == nil {
		t.Fatal("complete with non terminal status must fail")
	}
	err = srv.CompleteGoal("missing", GoalSucceeded, nil, 10)
	if err == nil {
		t.Fatal("complete on unknown goal must fail")
	}
	err = srv.AcceptGoal("g2", 0, ClockWall, 10)
	if err != nil {
		t.Fatalf("accept g2: %v", err)
	}
	err = srv.CompleteGoal("g2", GoalSucceeded, nil, 20)
	if err == nil {
		t.Fatal("complete on pending goal before promotion must fail")
	}
	err = srv.CompleteGoal("g1", GoalSucceeded, []byte("ok"), 20)
	if err != nil {
		t.Fatalf("complete g1: %v", err)
	}
	status, ok := srv.GoalStatusOf("g1", 20)
	if !ok || status != GoalSucceeded {
		t.Fatalf("terminal result must stick, got %s ok=%v", status, ok)
	}
	status, ok = srv.GoalStatusOf("g2", 20)
	if !ok || status != GoalExecuting {
		t.Fatalf("pending g2 must promote to executing, got %s ok=%v", status, ok)
	}
}

// checkActionBounds pins timing constants and getters.
func checkActionBounds(t *testing.T) {
	t.Helper()
	if ActionFeedbackIntervalNanos != 200000000 {
		t.Fatalf("feedback interval must be 200 ms for 5 Hz, got %d", ActionFeedbackIntervalNanos)
	}
	if ActionAdmissionBoundNanos != 500000000 {
		t.Fatalf("admission bound must be 500 ms, got %d", ActionAdmissionBoundNanos)
	}
	if ActionDeactivateBoundNanos != 500000000 {
		t.Fatalf("deactivate bound must be 500 ms, got %d", ActionDeactivateBoundNanos)
	}
	if DefaultActionResultTimeoutNanos != 60000000000 {
		t.Fatalf("default result timeout must be 60 s, got %d", DefaultActionResultTimeoutNanos)
	}
	if DefaultActionGoalTimeoutNanos != 60000000000 {
		t.Fatalf("default goal timeout must be 60 s, got %d", DefaultActionGoalTimeoutNanos)
	}
	srv := newActiveServer(t, ClockSteady, 0)
	if srv.ServerDomain() != ClockSteady {
		t.Fatalf("domain getter must round trip, got %s", srv.ServerDomain())
	}
	if srv.ResultTimeoutNanos() != DefaultActionResultTimeoutNanos {
		t.Fatalf("zero timeout must select default retention, got %d", srv.ResultTimeoutNanos())
	}
	if srv.Name() != "/navigate_to_pose" {
		t.Fatalf("name getter must round trip, got %q", srv.Name())
	}
	if got := ClockWall.String(); got != "wall" {
		t.Fatalf("wall string must be wall, got %q", got)
	}
	if got := TakeEmpty.String(); got != "empty" {
		t.Fatalf("empty take string must be empty, got %q", got)
	}
}

// checkActionValidation pins constructor and clock validation errors.
func checkActionValidation(t *testing.T) {
	t.Helper()
	_, err := NewActionServer("", ClockWall, 0)
	if err == nil {
		t.Fatal("empty server name must fail")
	}
	_, err = NewActionServer("/a", ClockDomain(99), 0)
	if err == nil {
		t.Fatal("invalid clock domain must fail")
	}
	_, err = NewActionServer("/a", ClockWall, -1)
	if err == nil {
		t.Fatal("negative result timeout must fail")
	}
	srv := newActiveServer(t, ClockWall, 0)
	err = srv.AcceptGoal("bad", 0, ClockDomain(99), 0)
	if err == nil {
		t.Fatal("invalid goal domain must fail")
	}
	err = srv.PublishFeedback("missing", []byte("x"), 0)
	if err == nil {
		t.Fatal("feedback for unknown goal must fail")
	}
	_, err = srv.FeedbackSnapshot("missing", 0)
	if err == nil {
		t.Fatal("snapshot for unknown goal must fail")
	}
	_, _, err = srv.GetResult("missing", 0)
	if err == nil {
		t.Fatal("result for unknown goal must fail")
	}
	_, ok := srv.GoalStatusOf("missing", 0)
	if ok {
		t.Fatal("status for unknown goal must report false")
	}
}

// checkActionBackwardJump pins backward jumps never expire results early.
func checkActionBackwardJump(t *testing.T) {
	t.Helper()
	srv, err := NewActionServer("/navigate_to_pose", ClockWall, 1000)
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	srv.Activate(100)
	err = srv.AcceptGoal("g1", 100, ClockWall, 100)
	if err != nil {
		t.Fatalf("accept g1: %v", err)
	}
	err = srv.CompleteGoal("g1", GoalSucceeded, []byte("r"), 10000)
	if err != nil {
		t.Fatalf("complete g1: %v", err)
	}
	_, _, err = srv.GetResult("g1", 5000)
	if err != nil {
		t.Fatalf("backward jump before completion must not expire: %v", err)
	}
	_, _, err = srv.GetResult("g1", 11001)
	if err == nil {
		t.Fatal("forward jump past retention must expire")
	}
}

// checkActionFeedbackOverflow pins bounded buffering drops oldest with a count.
func checkActionFeedbackOverflow(t *testing.T) {
	t.Helper()
	srv := newActiveServer(t, ClockWall, 0)
	err := srv.AcceptGoal("g1", 0, ClockWall, 0)
	if err != nil {
		t.Fatalf("accept g1: %v", err)
	}
	for i := range ActionFeedbackBufferCap + 1 {
		err = srv.PublishFeedback("g1", []byte{byte(i)}, int64(i))
		if err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}
	lost, ok := srv.FeedbackLost("g1", int64(ActionFeedbackBufferCap+1))
	if !ok || lost != 1 {
		t.Fatalf("one overflow must count one lost, got %d ok=%v", lost, ok)
	}
	snap, err := srv.FeedbackSnapshot("g1", int64(ActionFeedbackBufferCap+1))
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	if len(snap) != ActionFeedbackBufferCap {
		t.Fatalf("buffer must stay bounded at cap, got %d", len(snap))
	}
	if snap[0].Payload[0] != 1 {
		t.Fatalf("overflow must drop oldest, got first %d", snap[0].Payload[0])
	}
}
