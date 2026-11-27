// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

// Package wtime is the public surface for the generic clock domain.
//
// It wraps internal/time/clock so importers do not shadow stdlib time.
// The folder name wtime keeps `wtime` and `time` usable side by side.
// All types are aliases, so wtime values are clock values with no
// conversion. Constructors wrap errors with a wtime prefix.
package wtime

import (
	"fmt"

	clock "github.com/podomy/weld/internal/time"
)

// Domain is one clock domain with wall plus steady plus sim sources.
type Domain = clock.Domain

// Options declares one clock domain.
type Options = clock.Options

// TimerOptions declares one timer.
type TimerOptions = clock.TimerOptions

// Timer is one clock bound periodic timer.
type Timer = clock.Timer

// JumpEvent describes one detected time jump.
type JumpEvent = clock.JumpEvent

// JumpCallback observes time jumps in registration order.
type JumpCallback = clock.JumpCallback

// SourceKind names one clock timeline in a domain.
type SourceKind = clock.SourceKind

// OverloadPolicy names timer behavior when firings fall behind.
type OverloadPolicy = clock.OverloadPolicy

// FireContext carries one timer firing to the typed callback.
type FireContext = clock.FireContext

// CacheGuard tracks the domain generation for one cache.
type CacheGuard = clock.CacheGuard

// SimMismatchError is the loud error for use_sim_time disagreement.
type SimMismatchError = clock.SimMismatchError

// Clock timelines re-exported so callers need not import internal packages.
const (
	// KindUnspecified means no timeline was selected.
	KindUnspecified = clock.KindUnspecified
	// KindWall is wall time, the active timeline without sim time.
	KindWall = clock.KindWall
	// KindSteady is monotonic time, the default timer timeline.
	KindSteady = clock.KindSteady
	// KindSim is simulated time, active while UseSimTime holds.
	KindSim = clock.KindSim
)

// Timer overload policies re-exported as declared policy.
const (
	// OverloadInvalid means no policy was declared and is rejected.
	OverloadInvalid = clock.OverloadInvalid
	// OverloadCoalesce skips to now plus period and counts skips in Dropped.
	OverloadCoalesce = clock.OverloadCoalesce
	// OverloadCatchUp advances one period per Call and keeps Backlog visible.
	OverloadCatchUp = clock.OverloadCatchUp
)

// Default thresholds and fallback re-exported for explicit configuration.
const (
	// DefaultJumpForwardThresholdNanos fires forward jumps at 100 ms and above.
	DefaultJumpForwardThresholdNanos = clock.DefaultJumpForwardThresholdNanos
	// DefaultJumpBackwardThresholdNanos fires backward jumps at 1 ms and above.
	DefaultJumpBackwardThresholdNanos = clock.DefaultJumpBackwardThresholdNanos
	// DefaultFallbackKind serves reads while sim is requested but unavailable.
	DefaultFallbackKind = clock.DefaultFallbackKind
)

// Sentinel errors re-exported for errors.Is checks.
var (
	// ErrTimerCanceled reports a Call on a canceled timer.
	ErrTimerCanceled = clock.ErrTimerCanceled
	// ErrTimerNotReady reports a Call before the next call time.
	ErrTimerNotReady = clock.ErrTimerNotReady
	// ErrTimerClosed reports use of a Timer after Close.
	ErrTimerClosed = clock.ErrTimerClosed
	// ErrUnknownJumpCallback reports removal of an unknown callback id.
	ErrUnknownJumpCallback = clock.ErrUnknownJumpCallback
	// ErrNilJumpCallback reports registration of a nil jump callback.
	ErrNilJumpCallback = clock.ErrNilJumpCallback
	// ErrNilTimerCallback reports a nil timer fire callback.
	ErrNilTimerCallback = clock.ErrNilTimerCallback
	// ErrNilDomain reports a nil Domain where one is required.
	ErrNilDomain = clock.ErrNilDomain
	// ErrExternalSim reports SetSimTime on a domain with an external sim source.
	ErrExternalSim = clock.ErrExternalSim
)

// NewDomain builds one clock domain from opts.
func NewDomain(opts Options) (*Domain, error) {
	domain, err := clock.NewDomain(opts)
	if err != nil {
		return nil, fmt.Errorf("wtime: new domain: %w", err)
	}

	return domain, nil
}

// NewTimer builds a timer bound to opts.Kind reading the domain clock.
func NewTimer(d *Domain, opts TimerOptions, fire func(FireContext)) (*Timer, error) {
	timer, err := clock.NewTimer(d, opts, fire)
	if err != nil {
		return nil, fmt.Errorf("wtime: new timer: %w", err)
	}

	return timer, nil
}

// IsSimMismatch reports whether err carries a SimMismatchError.
func IsSimMismatch(err error) bool {
	return clock.IsSimMismatch(err)
}
