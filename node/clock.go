// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

package node

import (
	"fmt"

	"github.com/podomy/weld/internal/core"
)

// ClockState is one context clock with wall, steady, and sim sources. The
// zero value is unusable because the callback map is nil; build with
// NewClockState and do not copy a state after first use.
type ClockState = core.ClockState

// ClockJump describes one observed time step. Backward is true when the new
// tick precedes the previous tick.
type ClockJump = core.ClockJump

// NewClockState returns a state with explicit wall plus steady fallbacks. An
// invalid domain fails loudly with a panic because the domain selects the
// active source and silent fallback to wall would hide misconfiguration.
func NewClockState(domain ClockDomain, wallNanos, steadyNanos int64) *ClockState {
	return core.NewClockState(domain, wallNanos, steadyNanos)
}

// JumpCallback observes one clock jump. See core.JumpCallback.
type JumpCallback = core.JumpCallback

// ClockMismatchError is the explicit error for a domain disagreement. See core.ClockMismatchError.
type ClockMismatchError = core.ClockMismatchError

// OverflowPolicy names the queue overflow behavior. See core.OverflowPolicy.
type OverflowPolicy = core.OverflowPolicy

const (
	// OverflowDropOldest drops the oldest sample on overflow and counts the drop.
	OverflowDropOldest = core.OverflowDropOldest
)

const (
	// ClockTopic carries sim time for nodes with sim time enabled.
	ClockTopic = core.ClockTopic
	// ClockHistoryDepth is the wire depth for the clock topic.
	ClockHistoryDepth = core.ClockHistoryDepth
	// ClockPublishPeriodNanos spaces /clock ticks at 40 Hz.
	ClockPublishPeriodNanos = core.ClockPublishPeriodNanos
)

// ValidateGoalClock rejects a goal whose domain disagrees with the server.
func ValidateGoalClock(server, goalID string, stampNanos int64, goalDomain, serverDomain ClockDomain) error {
	err := core.ValidateGoalClock(server, goalID, stampNanos, goalDomain, serverDomain)
	if err != nil {
		return fmt.Errorf("node: validate goal clock: %w", err)
	}
	return nil
}

// CheckUseSimTimeAgreement enforces agreement between two sim flags.
func CheckUseSimTimeAgreement(localUseSim, remoteUseSim bool) error {
	err := core.CheckUseSimTimeAgreement(localUseSim, remoteUseSim)
	if err != nil {
		return fmt.Errorf("node: check use sim time agreement: %w", err)
	}
	return nil
}

// RebaseDeadlinesOnJump rebases deadline trackers after a backward jump.
func RebaseDeadlinesOnJump(jump ClockJump, trackers ...*DeadlineTracker) []DeadlineAlert {
	return core.RebaseDeadlinesOnJump(jump, trackers...)
}

// ShouldPublishClock reports whether a /clock tick is due.
func ShouldPublishClock(lastPublishNanos, nowNanos int64) bool {
	return core.ShouldPublishClock(lastPublishNanos, nowNanos)
}
