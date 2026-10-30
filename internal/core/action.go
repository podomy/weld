// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package core

import (
	"errors"
	"fmt"
	"sync"
)

// Action workflow timing and buffering bounds.
//
// Feedback at 5 Hz means one sample every 200 ms. Admission and
// deactivation each complete synchronously well inside 500 ms, so
// transports can enforce the bound as a wall timeout around the call.
// Result retention defaults to 60 s from completion; the goal timeout
// default is the same interval but measured from acceptance on the
// client side.
const (
	// ActionFeedbackIntervalNanos is the expected feedback period at 5 Hz.
	ActionFeedbackIntervalNanos int64 = 200000000
	// ActionAdmissionBoundNanos bounds goal admission at 500 ms.
	ActionAdmissionBoundNanos int64 = 500000000
	// ActionDeactivateBoundNanos bounds teardown of in flight work at 500 ms.
	ActionDeactivateBoundNanos int64 = 500000000
	// DefaultActionGoalTimeoutNanos is the client side goal timeout when
	// the client sets none, measured from acceptance.
	DefaultActionGoalTimeoutNanos int64 = 60000000000
	// DefaultActionResultTimeoutNanos retains a terminal result for 60 s
	// measured from completion, not from acceptance.
	DefaultActionResultTimeoutNanos int64 = 60000000000
	// ActionFeedbackBufferCap bounds buffered feedback per goal for late
	// attach. At 5 Hz it holds about 6.4 s of history.
	ActionFeedbackBufferCap = 32
)

// ClockDomain names the time source of one action context.
//
// Each context has exactly one domain. Wall and sim must agree between
// client and server; a wall stamped goal against a sim domain errors
// loudly instead of freezing.
type ClockDomain int

// Clock domains.
const (
	// ClockWall is wall time.
	ClockWall ClockDomain = iota
	// ClockSim is simulated time from the clock topic.
	ClockSim
	// ClockSteady is monotonic time for local measurement.
	ClockSteady
)

// String renders the domain in lowercase form.
func (d ClockDomain) String() string {
	switch d {
	case ClockWall:
		return "wall"
	case ClockSim:
		return "sim"
	case ClockSteady:
		return "steady"
	default:
		return "invalid"
	}
}

// isValidClockDomain reports whether d is a concrete domain.
func isValidClockDomain(d ClockDomain) bool {
	return d == ClockWall || d == ClockSim || d == ClockSteady
}

// GoalStatus names one action goal state.
//
// Accepted is queued in the pending slot, Executing runs in the active
// slot, Canceling awaits confirmation after a cancel request. Succeeded,
// Canceled, and Aborted are terminal and never leave the terminal set.
type GoalStatus int

// Goal states.
const (
	// GoalUnknown marks an invalid status and never appears on a stored goal.
	GoalUnknown GoalStatus = iota
	// GoalAccepted is queued in the pending preempt slot.
	GoalAccepted
	// GoalExecuting runs in the active slot.
	GoalExecuting
	// GoalCanceling awaits confirmation after CancelGoal.
	GoalCanceling
	// GoalSucceeded is terminal success.
	GoalSucceeded
	// GoalCanceled is terminal cancel, including deactivate teardown.
	GoalCanceled
	// GoalAborted is terminal abort, including preempt replacement before start.
	GoalAborted
)

// String renders the status in lowercase form.
func (s GoalStatus) String() string {
	switch s { //nolint:exhaustive // default returns invalid for unknown statuses including GoalUnknown, which never appears on stored goals
	case GoalAccepted:
		return "accepted"
	case GoalExecuting:
		return "executing"
	case GoalCanceling:
		return "canceling"
	case GoalSucceeded:
		return "succeeded"
	case GoalCanceled:
		return "canceled"
	case GoalAborted:
		return "aborted"
	default:
		return "invalid"
	}
}

// IsTerminal reports whether s is a terminal state.
func (s GoalStatus) IsTerminal() bool {
	return s == GoalSucceeded || s == GoalCanceled || s == GoalAborted
}

// isTerminalResult reports whether s may be stored as a terminal result.
func isTerminalResult(s GoalStatus) bool {
	return s.IsTerminal()
}

// TakeKind names one unit of server side work.
type TakeKind int

// Take kinds.
const (
	// TakeEmpty means nothing is ready. It bumps the empty metric.
	TakeEmpty TakeKind = iota
	// TakeExecute means start the named active goal.
	TakeExecute
	// TakePreempt means preempt the named active goal for the pending goal.
	TakePreempt
	// TakeCancel means confirm cancel of the named active goal.
	TakeCancel
)

// String renders the take kind in lowercase form.
func (k TakeKind) String() string {
	switch k {
	case TakeEmpty:
		return "empty"
	case TakeExecute:
		return "execute"
	case TakePreempt:
		return "preempt"
	case TakeCancel:
		return "cancel"
	default:
		return "invalid"
	}
}

// GoalTake is one atomic take plus dispatch step.
//
// Empty carries no goal and only bumps the empty metric. Non empty
// carries the goal to act on. Take plus dispatch happen under one lock
// so a take with nothing ready never crashes and never double dispatches.
type GoalTake struct {
	// Kind selects empty, execute, preempt, or cancel.
	Kind TakeKind
	// GoalID names the active goal for non empty takes.
	GoalID string
}

// FeedbackSample is one buffered feedback entry.
type FeedbackSample struct {
	// AtNanos is the fake clock time of publication.
	AtNanos int64
	// Payload is a copy of the published bytes.
	Payload []byte
}

// ClockMismatchError is the explicit error for a wall stamp against a
// sim domain, or any domain disagreement.
//
// It carries the goal stamp plus both domains so operators learn the
// stamp and the active domain without guessing.
type ClockMismatchError struct {
	// Server names the action server.
	Server string
	// GoalID names the rejected goal.
	GoalID string
	// StampNanos is the goal stamp that mismatched.
	StampNanos int64
	// GoalDomain is the domain the goal was stamped in.
	GoalDomain ClockDomain
	// ServerDomain is the active clock domain of the server.
	ServerDomain ClockDomain
}

// Error renders the mismatch with stamp and both domains.
func (e *ClockMismatchError) Error() string {
	return fmt.Sprintf("core: action %q goal %q clock mismatch: stamp %d in %s against server domain %s",
		e.Server, e.GoalID, e.StampNanos, e.GoalDomain.String(), e.ServerDomain.String())
}

// ValidateGoalClock rejects a goal whose domain disagrees with the server.
//
// Any disagreement errors, so a wall stamped receding horizon goal against
// sim time surfaces here instead of freezing silently. Matching domains
// pass. The returned error is a ClockMismatchError carrying stamp and
// both domains.
func ValidateGoalClock(server, goalID string, stampNanos int64, goalDomain, serverDomain ClockDomain) error {
	if goalDomain == serverDomain {
		return nil
	}
	return &ClockMismatchError{
		Server:       server,
		GoalID:       goalID,
		StampNanos:   stampNanos,
		GoalDomain:   goalDomain,
		ServerDomain: serverDomain,
	}
}

// actionGoal is one in flight goal in the active or pending slot.
type actionGoal struct {
	id                string
	stampNanos        int64
	stampDomain       ClockDomain
	acceptedNanos     int64
	status            GoalStatus
	feedback          []FeedbackSample
	lost              uint64
	dispatched        bool
	preemptDispatched bool
	cancelDispatched  bool
}

// actionResult is one cached terminal result.
//
// It survives promotion, handle close, and slot clearing until the result
// timeout expires measured from completion. Feedback is retained so late
// attach still reads history after completion.
type actionResult struct {
	status         GoalStatus
	payload        []byte
	completedNanos int64
	feedback       []FeedbackSample
	lost           uint64
}

// ActionServer is one transport free action workflow.
//
// It holds one active plus one pending preempt slot, buffers feedback for
// late attach, caches results independent of handle lifetime, and
// guarantees terminal states. All times are caller supplied nanoseconds
// so tests run on a fake clock; no clock is read here. Safe for
// concurrent use through its mutex. Do not copy a server after first use.
type ActionServer struct {
	noCopy             noCopy
	mu                 sync.Mutex
	name               string
	serverDomain       ClockDomain
	resultTimeoutNanos int64
	active             bool
	activeGoal         *actionGoal
	pendingGoal        *actionGoal
	results            map[string]*actionResult
	emptyTakes         uint64
	lastActivateNanos  int64
}

// NewActionServer returns an inactive server with the given clock domain
// and result retention.
//
// An empty name is an error. An invalid domain is an error. A zero
// result timeout selects the default retention. A negative timeout is an
// error. The server starts inactive; call Activate before AcceptGoal.
func NewActionServer(name string, serverDomain ClockDomain, resultTimeoutNanos int64) (*ActionServer, error) {
	if name == "" {
		return nil, errors.New("core: action: server name is empty") //nolint:wrapcheck // new error, nothing to wrap
	}
	if !isValidClockDomain(serverDomain) {
		return nil, fmt.Errorf("core: action %q: invalid clock domain %d", name, int(serverDomain))
	}
	timeout := resultTimeoutNanos
	if timeout == 0 {
		timeout = DefaultActionResultTimeoutNanos
	}
	if timeout < 0 {
		return nil, fmt.Errorf("core: action %q: negative result timeout %d", name, resultTimeoutNanos)
	}
	return &ActionServer{
		name:               name,
		serverDomain:       serverDomain,
		resultTimeoutNanos: timeout,
		results:            make(map[string]*actionResult),
	}, nil
}

// Name returns the server action name.
func (s *ActionServer) Name() string {
	return s.name
}

// ServerDomain returns the active clock domain of the server.
func (s *ActionServer) ServerDomain() ClockDomain {
	return s.serverDomain
}

// ResultTimeoutNanos returns the result retention measured from completion.
func (s *ActionServer) ResultTimeoutNanos() int64 {
	return s.resultTimeoutNanos
}

// IsActive reports whether the server accepts goals.
func (s *ActionServer) IsActive() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.active
}

// Activate marks the server active so it admits goals.
//
// It is idempotent. The stamp orders activation against fake clock tests
// and carries no expiry.
func (s *ActionServer) Activate(nowNanos int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.active = true
	s.lastActivateNanos = nowNanos
}

// Deactivate terminal results in flight goals and rejects new work.
//
// Both active and pending goals move to Canceled with completion at
// nowNanos and their results stay cached until the result timeout. It is
// synchronous and completes well inside the 500 ms deactivate bound. It
// is idempotent; a second call is a no op.
func (s *ActionServer) Deactivate(nowNanos int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.activeGoal != nil {
		s.cacheLocked(s.activeGoal, GoalCanceled, nil, nowNanos)
		s.activeGoal = nil
	}
	if s.pendingGoal != nil {
		s.cacheLocked(s.pendingGoal, GoalCanceled, nil, nowNanos)
		s.pendingGoal = nil
	}
	s.active = false
}

// AcceptGoal admits one goal under lifecycle gating with deterministic preempt.
//
// It rejects when inactive. It rejects empty IDs, duplicates of in flight
// or retained goals, invalid domains, and clock domain mismatches with an
// explicit ClockMismatchError. With no active goal the new goal becomes
// active as Executing. With an active goal but no pending goal it becomes
// pending as Accepted. With both slots full the old pending goal is
// terminal resulted as Aborted and replaced by the new pending goal, so
// back to back arrivals preempt deterministically and never preempt both
// goals at once. The call is synchronous inside the 500 ms admission bound.
func (s *ActionServer) AcceptGoal(goalID string, stampNanos int64, goalDomain ClockDomain, nowNanos int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.active {
		return fmt.Errorf("core: action %q: reject goal %q while inactive", s.name, goalID)
	}
	if goalID == "" {
		return fmt.Errorf("core: action %q: goal id is empty", s.name)
	}
	if !isValidClockDomain(goalDomain) {
		return fmt.Errorf("core: action %q goal %q: invalid clock domain %d", s.name, goalID, int(goalDomain))
	}
	s.purgeExpiredLocked(nowNanos)
	if s.activeGoal != nil && s.activeGoal.id == goalID {
		return fmt.Errorf("core: action %q: duplicate goal %q is already active", s.name, goalID)
	}
	if s.pendingGoal != nil && s.pendingGoal.id == goalID {
		return fmt.Errorf("core: action %q: duplicate goal %q is already pending", s.name, goalID)
	}
	if _, ok := s.results[goalID]; ok {
		return fmt.Errorf("core: action %q: duplicate goal %q result is still retained", s.name, goalID)
	}
	err := ValidateGoalClock(s.name, goalID, stampNanos, goalDomain, s.serverDomain)
	if err != nil {
		return fmt.Errorf("core: action %q: %w", s.name, err)
	}
	fresh := &actionGoal{
		id:            goalID,
		stampNanos:    stampNanos,
		stampDomain:   goalDomain,
		acceptedNanos: nowNanos,
		status:        GoalAccepted,
	}
	if s.activeGoal == nil {
		fresh.status = GoalExecuting
		s.activeGoal = fresh
		return nil
	}
	if s.pendingGoal == nil {
		s.pendingGoal = fresh
		return nil
	}
	s.cacheLocked(s.pendingGoal, GoalAborted, nil, nowNanos)
	s.pendingGoal = fresh
	return nil
}

// CancelGoal propagates cancel for one goal.
//
// It requires the lifecycle to be active and accepts cancel only then. An
// active Executing goal moves to Canceling and a later Take reports
// Cancel once. Cancel on an already Canceling goal is idempotent. Cancel
// on a pending goal terminal results it immediately as Canceled since it
// never started. Cancel on a retained terminal result errors as already
// terminal. Unknown IDs error.
func (s *ActionServer) CancelGoal(goalID string, nowNanos int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.active {
		return fmt.Errorf("core: action %q: cancel goal %q while inactive", s.name, goalID)
	}
	if goalID == "" {
		return fmt.Errorf("core: action %q: cancel goal id is empty", s.name)
	}
	s.purgeExpiredLocked(nowNanos)
	if s.activeGoal != nil && s.activeGoal.id == goalID {
		if s.activeGoal.status == GoalCanceling {
			return nil
		}
		if s.activeGoal.status != GoalExecuting {
			return fmt.Errorf("core: action %q: cancel goal %q in status %s", s.name, goalID, s.activeGoal.status.String())
		}
		s.activeGoal.status = GoalCanceling
		s.activeGoal.cancelDispatched = false
		return nil
	}
	if s.pendingGoal != nil && s.pendingGoal.id == goalID {
		s.cacheLocked(s.pendingGoal, GoalCanceled, nil, nowNanos)
		s.pendingGoal = nil
		return nil
	}
	if res, ok := s.results[goalID]; ok {
		return fmt.Errorf("core: action %q: cancel goal %q already terminal as %s", s.name, goalID, res.status.String())
	}
	return fmt.Errorf("core: action %q: unknown goal %q for cancel", s.name, goalID)
}

// PublishFeedback buffers one feedback sample for the active goal.
//
// The payload is copied so caller mutation cannot rewrite history. When
// the buffer is full the oldest sample is dropped and counted in the
// lost counter, matching the bounded retention rule. Late attach reads
// the buffer through FeedbackSnapshot. Only the active goal may publish;
// pending or terminal goals error.
func (s *ActionServer) PublishFeedback(goalID string, payload []byte, nowNanos int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.activeGoal == nil || s.activeGoal.id != goalID {
		return fmt.Errorf("core: action %q: feedback for non active goal %q", s.name, goalID)
	}
	if s.activeGoal.status != GoalExecuting && s.activeGoal.status != GoalCanceling {
		return fmt.Errorf("core: action %q: feedback for goal %q in status %s", s.name, goalID, s.activeGoal.status.String())
	}
	cp := append([]byte(nil), payload...)
	if len(s.activeGoal.feedback) >= ActionFeedbackBufferCap {
		s.activeGoal.feedback[0] = FeedbackSample{}
		s.activeGoal.feedback = append(s.activeGoal.feedback[1:], FeedbackSample{AtNanos: nowNanos, Payload: cp})
		s.activeGoal.lost++
		return nil
	}
	s.activeGoal.feedback = append(s.activeGoal.feedback, FeedbackSample{AtNanos: nowNanos, Payload: cp})
	return nil
}

// FeedbackSnapshot returns a copy of buffered feedback for late attach.
//
// It serves in flight goals and retained terminal results, so a client
// that attaches after publication still reads history. Payloads are deep
// copied. Expired results error as timed out and are evicted. Unknown IDs
// error.
func (s *ActionServer) FeedbackSnapshot(goalID string, nowNanos int64) ([]FeedbackSample, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.activeGoal != nil && s.activeGoal.id == goalID {
		return copyFeedback(s.activeGoal.feedback), nil
	}
	if s.pendingGoal != nil && s.pendingGoal.id == goalID {
		return copyFeedback(s.pendingGoal.feedback), nil
	}
	res, ok := s.results[goalID]
	if !ok {
		return nil, fmt.Errorf("core: action %q: unknown goal %q for feedback", s.name, goalID)
	}
	if s.isExpiredLocked(res, nowNanos) {
		delete(s.results, goalID)
		return nil, fmt.Errorf("core: action %q: feedback for goal %q timed out", s.name, goalID)
	}
	return copyFeedback(res.feedback), nil
}

// FeedbackLost returns the drop count for one goal and whether it exists.
func (s *ActionServer) FeedbackLost(goalID string, nowNanos int64) (uint64, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.activeGoal != nil && s.activeGoal.id == goalID {
		return s.activeGoal.lost, true
	}
	if s.pendingGoal != nil && s.pendingGoal.id == goalID {
		return s.pendingGoal.lost, true
	}
	res, ok := s.results[goalID]
	if !ok {
		return 0, false
	}
	if s.isExpiredLocked(res, nowNanos) {
		delete(s.results, goalID)
		return 0, false
	}
	return res.lost, true
}

// CompleteGoal terminal results the active goal and promotes the pending goal.
//
// Only Succeeded, Canceled, and Aborted are accepted; any other status
// errors without mutating state. The result payload is copied and cached
// with completion at nowNanos, so the result survives slot clearing and
// handle close until the result timeout. Completion promotes a pending
// goal to active as Executing with fresh dispatch flags, giving
// deterministic back to back preempt. Completing a pending goal directly
// errors; cancel it or wait for promotion.
func (s *ActionServer) CompleteGoal(goalID string, status GoalStatus, result []byte, nowNanos int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !isTerminalResult(status) {
		return fmt.Errorf("core: action %q: complete goal %q with non terminal status %s", s.name, goalID, status.String())
	}
	if s.activeGoal == nil || s.activeGoal.id != goalID {
		if s.pendingGoal != nil && s.pendingGoal.id == goalID {
			return fmt.Errorf("core: action %q: pending goal %q has not started", s.name, goalID)
		}
		return fmt.Errorf("core: action %q: unknown active goal %q for complete", s.name, goalID)
	}
	s.cacheLocked(s.activeGoal, status, result, nowNanos)
	s.activeGoal = nil
	if s.pendingGoal != nil {
		s.pendingGoal.status = GoalExecuting
		s.pendingGoal.dispatched = false
		s.pendingGoal.preemptDispatched = false
		s.pendingGoal.cancelDispatched = false
		s.activeGoal = s.pendingGoal
		s.pendingGoal = nil
	}
	return nil
}

// GetResult returns the cached terminal result for one goal.
//
// The payload is copied. Expiry is measured from completion, never from
// acceptance: a goal accepted at T0 and completed at T1 expires at T1
// plus the result timeout. Expired entries are evicted and report a
// timeout. In flight goals report not complete. Unknown IDs error.
func (s *ActionServer) GetResult(goalID string, nowNanos int64) ([]byte, GoalStatus, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.activeGoal != nil && s.activeGoal.id == goalID {
		return nil, GoalUnknown, fmt.Errorf("core: action %q: goal %q is not complete", s.name, goalID)
	}
	if s.pendingGoal != nil && s.pendingGoal.id == goalID {
		return nil, GoalUnknown, fmt.Errorf("core: action %q: goal %q is not complete", s.name, goalID)
	}
	res, ok := s.results[goalID]
	if !ok {
		return nil, GoalUnknown, fmt.Errorf("core: action %q: unknown goal %q for result", s.name, goalID)
	}
	if s.isExpiredLocked(res, nowNanos) {
		delete(s.results, goalID)
		return nil, GoalUnknown, fmt.Errorf("core: action %q: result for goal %q timed out", s.name, goalID)
	}
	return append([]byte(nil), res.payload...), res.status, nil
}

// Take atomically takes plus dispatches one work item.
//
// Canceling work reports Cancel once, pending backed active work reports
// Preempt once, and undispatched active work reports Execute once. With
// nothing ready it returns TakeEmpty and counts one empty take, so take
// with nothing ready is an observable signal rather than a crash.
func (s *ActionServer) Take() GoalTake {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.activeGoal == nil {
		s.emptyTakes++
		return GoalTake{Kind: TakeEmpty}
	}
	active := s.activeGoal
	if active.status == GoalCanceling {
		if !active.cancelDispatched {
			active.cancelDispatched = true
			return GoalTake{Kind: TakeCancel, GoalID: active.id}
		}
		s.emptyTakes++
		return GoalTake{Kind: TakeEmpty}
	}
	if s.pendingGoal != nil && active.status == GoalExecuting {
		if !active.preemptDispatched {
			active.preemptDispatched = true
			return GoalTake{Kind: TakePreempt, GoalID: active.id}
		}
		s.emptyTakes++
		return GoalTake{Kind: TakeEmpty}
	}
	if active.status == GoalExecuting && !active.dispatched {
		active.dispatched = true
		return GoalTake{Kind: TakeExecute, GoalID: active.id}
	}
	s.emptyTakes++
	return GoalTake{Kind: TakeEmpty}
}

// EmptyTakes returns the count of empty takes for the take metric.
func (s *ActionServer) EmptyTakes() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.emptyTakes
}

// ActiveGoalID returns the active goal ID when one exists.
func (s *ActionServer) ActiveGoalID() (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.activeGoal == nil {
		return "", false
	}
	return s.activeGoal.id, true
}

// PendingGoalID returns the pending goal ID when one exists.
func (s *ActionServer) PendingGoalID() (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.pendingGoal == nil {
		return "", false
	}
	return s.pendingGoal.id, true
}

// GoalStatusOf returns the status of an in flight or retained goal.
//
// Retained results report their terminal status until the result timeout
// expires from completion; expired entries are evicted and report false.
// Unknown IDs report false.
func (s *ActionServer) GoalStatusOf(goalID string, nowNanos int64) (GoalStatus, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.activeGoal != nil && s.activeGoal.id == goalID {
		return s.activeGoal.status, true
	}
	if s.pendingGoal != nil && s.pendingGoal.id == goalID {
		return s.pendingGoal.status, true
	}
	res, ok := s.results[goalID]
	if !ok {
		return GoalUnknown, false
	}
	if s.isExpiredLocked(res, nowNanos) {
		delete(s.results, goalID)
		return GoalUnknown, false
	}
	return res.status, true
}

// cacheLocked stores one terminal result with completion at nowNanos.
//
// Callers must hold the mutex. Payload and feedback are copied so later
// mutation cannot rewrite the cache. The cache survives slot clearing and
// handle close independent of goal lifetime.
func (s *ActionServer) cacheLocked(goal *actionGoal, status GoalStatus, payload []byte, nowNanos int64) {
	cp := append([]byte(nil), payload...)
	s.results[goal.id] = &actionResult{
		status:         status,
		payload:        cp,
		completedNanos: nowNanos,
		feedback:       copyFeedback(goal.feedback),
		lost:           goal.lost,
	}
}

// isExpiredLocked reports whether a result exceeded retention from completion.
//
// A backward jump where now is before completion never expires, so the
// cache survives clock rewinds. Callers must hold the mutex.
func (s *ActionServer) isExpiredLocked(res *actionResult, nowNanos int64) bool {
	if nowNanos < res.completedNanos {
		return false
	}
	return nowNanos-res.completedNanos > s.resultTimeoutNanos
}

// purgeExpiredLocked evicts results whose retention lapsed at nowNanos.
//
// Callers must hold the mutex. It keeps duplicate admission exact: an
// expired ID is reusable after the purge.
func (s *ActionServer) purgeExpiredLocked(nowNanos int64) {
	for id, res := range s.results {
		if s.isExpiredLocked(res, nowNanos) {
			delete(s.results, id)
		}
	}
}

// copyFeedback deep copies one feedback buffer.
func copyFeedback(in []FeedbackSample) []FeedbackSample {
	out := make([]FeedbackSample, len(in))
	for i, sample := range in {
		out[i] = FeedbackSample{
			AtNanos: sample.AtNanos,
			Payload: append([]byte(nil), sample.Payload...),
		}
	}
	return out
}
