// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

package node

import (
	"fmt"

	"github.com/podomy/weld/internal/core"
)

// ActionServer is one transport free action workflow with one active plus one
// pending preempt slot, buffered feedback for late attach, and cached results.
// All times are caller supplied nanoseconds. Do not copy a server after first
// use. The Take, AcceptGoal, CancelGoal, PublishFeedback, CompleteGoal, and
// GetResult methods delegate directly to the core workflow.
type ActionServer = core.ActionServer

// GoalStatus names one action goal state. Terminal states never leave the
// terminal set.
type GoalStatus = core.GoalStatus

const (
	// GoalUnknown marks an invalid status and never appears on a stored goal.
	GoalUnknown = core.GoalUnknown
	// GoalAccepted is queued in the pending preempt slot.
	GoalAccepted = core.GoalAccepted
	// GoalExecuting runs in the active slot.
	GoalExecuting = core.GoalExecuting
	// GoalCanceling awaits confirmation after a cancel request.
	GoalCanceling = core.GoalCanceling
	// GoalSucceeded is terminal success.
	GoalSucceeded = core.GoalSucceeded
	// GoalCanceled is terminal cancel, including deactivate teardown.
	GoalCanceled = core.GoalCanceled
	// GoalAborted is terminal abort, including preempt replacement before start.
	GoalAborted = core.GoalAborted
)

// GoalTake is one atomic take plus dispatch step. Empty carries no goal and
// only bumps the empty metric.
type GoalTake = core.GoalTake

// TakeKind names one unit of server side work.
type TakeKind = core.TakeKind

const (
	// TakeEmpty means nothing is ready. It bumps the empty metric.
	TakeEmpty = core.TakeEmpty
	// TakeExecute means start the named active goal.
	TakeExecute = core.TakeExecute
	// TakePreempt means preempt the named active goal for the pending goal.
	TakePreempt = core.TakePreempt
	// TakeCancel means confirm cancel of the named active goal.
	TakeCancel = core.TakeCancel
)

// ClockDomain names the time source of one action context. Wall and sim must
// agree between client and server; a mismatch errors loudly.
type ClockDomain = core.ClockDomain

const (
	// ClockWall is wall time.
	ClockWall = core.ClockWall
	// ClockSim is simulated time from the clock topic.
	ClockSim = core.ClockSim
	// ClockSteady is monotonic time for local measurement.
	ClockSteady = core.ClockSteady
)

// NewActionServer returns an inactive server with the given clock domain and
// result retention. An empty name, an invalid domain, and a negative timeout
// are errors; a zero timeout selects the default retention. The server starts
// inactive; call Activate before AcceptGoal.
func NewActionServer(name string, serverDomain ClockDomain, resultTimeoutNanos int64) (*ActionServer, error) {
	server, err := core.NewActionServer(name, serverDomain, resultTimeoutNanos)
	if err != nil {
		return nil, fmt.Errorf("node: new action server: %w", err)
	}
	return server, nil
}

// FeedbackSample is one buffered feedback entry. See core.FeedbackSample.
type FeedbackSample = core.FeedbackSample

const (
	// ActionFeedbackIntervalNanos is the expected feedback period at 5 Hz.
	ActionFeedbackIntervalNanos = core.ActionFeedbackIntervalNanos
	// ActionAdmissionBoundNanos bounds goal admission at 500 ms.
	ActionAdmissionBoundNanos = core.ActionAdmissionBoundNanos
	// ActionDeactivateBoundNanos bounds teardown of in flight work at 500 ms.
	ActionDeactivateBoundNanos = core.ActionDeactivateBoundNanos
	// DefaultActionGoalTimeoutNanos is the client side goal timeout when the client sets none.
	DefaultActionGoalTimeoutNanos = core.DefaultActionGoalTimeoutNanos
	// DefaultActionResultTimeoutNanos retains a terminal result for 60 s by default.
	DefaultActionResultTimeoutNanos = core.DefaultActionResultTimeoutNanos
	// ActionFeedbackBufferCap bounds buffered feedback per goal for late attach.
	ActionFeedbackBufferCap = core.ActionFeedbackBufferCap
)
