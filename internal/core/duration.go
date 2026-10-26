// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package core

import (
	"fmt"
	"math"
)

// nanosPerSecond converts between seconds and nanoseconds.
const nanosPerSecond = 1000000000

// Duration bound constants derive from the max int64 total nanoseconds.
//
// MaxInt64 is 9223372036854775807, which equals 9223372036 seconds times
// nanosPerSecond plus 854775807 nanoseconds. InfiniteDuration carries exactly
// that total. BestAvailableDuration carries one nanosecond less so the
// sentinel stays distinct from infinite.
const (
	// maxSeconds is the seconds part of max int64 nanoseconds.
	maxSeconds = int64(9223372036)
	// maxNanoseconds is the nanoseconds remainder of max int64 nanoseconds.
	maxNanoseconds = uint32(854775807)
	// bestAvailableNanoseconds marks the best available sentinel remainder.
	bestAvailableNanoseconds = uint32(854775806)
)

// Duration is the RMW style time base: seconds plus nanoseconds.
//
// The zero value means unspecified, which is the default for deadline,
// lifespan, and lease duration. Infinite is the largest total nanosecond
// count, near max int64. Infinite minus one is reserved as the best
// available sentinel; see BestAvailableDuration.
type Duration struct {
	// Seconds is whole seconds. Negative values are malformed and rejected
	// by Validate.
	Seconds int64
	// Nanoseconds is the sub second part, always below one second after
	// construction through NewDuration.
	Nanoseconds uint32
}

// UnspecifiedDuration returns the zero duration, meaning no constraint.
//
// An unspecified deadline means the implementation default (no freshness
// promise on offer, no freshness requirement on request). This is the normal
// value carried by every preset except rosout lifespan and best available.
func UnspecifiedDuration() Duration {
	return Duration{}
}

// InfiniteDuration returns a duration with total nanoseconds at max int64.
//
// Infinite is a concrete value, not a default: an infinite offered deadline
// is slower than any finite requested deadline and therefore incompatible.
// An infinite requested deadline or lease means no constraint and always
// flows; see checkDeadline and checkLease.
func InfiniteDuration() Duration {
	return Duration{Seconds: maxSeconds, Nanoseconds: maxNanoseconds}
}

// BestAvailableDuration returns the sentinel carried by the best available
// preset: infinite minus one nanosecond.
//
// The preset cannot carry unspecified (that means default, which resolves
// differently) and cannot carry infinite (transports may treat infinite as
// a concrete promise), so infinite minus one marks "resolve me at endpoint
// creation". Endpoint creation replaces the sentinel from visible peers and
// the sentinel never reaches entity creation or the wire.
func BestAvailableDuration() Duration {
	return Duration{Seconds: maxSeconds, Nanoseconds: bestAvailableNanoseconds}
}

// IsBestAvailableDuration reports whether d is the best available sentinel.
func IsBestAvailableDuration(d Duration) bool {
	return d == BestAvailableDuration()
}

// NewDuration builds a duration from seconds plus nanoseconds, carrying
// nanosecond overflow into seconds and saturating to infinite on overflow.
//
// Negative seconds pass through unchanged with no carry so Validate can
// reject them with a clear error instead of hiding malformed input.
func NewDuration(sec int64, nsec uint32) Duration {
	if sec < 0 {
		return Duration{Seconds: sec, Nanoseconds: nsec}
	}
	extra := int64(nsec / nanosPerSecond)
	rem := nsec % nanosPerSecond
	if extra > math.MaxInt64-sec {
		return InfiniteDuration()
	}
	total := sec + extra
	if total > maxSeconds || (total == maxSeconds && rem > maxNanoseconds) {
		return InfiniteDuration()
	}
	return Duration{Seconds: total, Nanoseconds: rem}
}

// DurationFromNanoseconds builds a duration from a total nanosecond count.
//
// Zero maps to unspecified. Negative input is malformed and yields
// Duration{Seconds: -1} so Validate rejects it loudly instead of collapsing
// to unspecified; zero and negative stay distinguishable after the call.
// Max int64 input yields exactly InfiniteDuration.
func DurationFromNanoseconds(total int64) Duration {
	if total == 0 {
		return UnspecifiedDuration()
	}
	if total < 0 {
		return Duration{Seconds: -1}
	}
	return Duration{Seconds: total / nanosPerSecond, Nanoseconds: uint32(total % nanosPerSecond)}
}

// TotalNanoseconds returns the duration as a saturating total nanosecond count.
//
// Totals above max int64 saturate to max int64; totals below min int64
// saturate to min int64 so a huge negative Seconds never overflows the
// multiply. Unspecified returns zero. Negative totals are malformed but
// still saturate instead of overflowing.
func (d Duration) TotalNanoseconds() int64 {
	if d.Seconds < math.MinInt64/nanosPerSecond {
		return math.MinInt64
	}
	if d.Seconds > maxSeconds {
		return math.MaxInt64
	}
	if d.Seconds == maxSeconds && d.Nanoseconds > maxNanoseconds {
		return math.MaxInt64
	}
	return d.Seconds*nanosPerSecond + int64(d.Nanoseconds)
}

// IsUnspecified reports whether d carries no constraint.
func (d Duration) IsUnspecified() bool {
	return d == Duration{}
}

// IsInfinite reports whether d totals exactly max int64 nanoseconds.
func (d Duration) IsInfinite() bool {
	return d.TotalNanoseconds() == math.MaxInt64
}

// Compare orders d against other by total nanoseconds, returning minus one,
// zero, or one.
//
// Comparison is saturating: totals above max int64 all saturate to max int64,
// so distinct very large durations can compare equal.
func (d Duration) Compare(other Duration) int {
	a := d.TotalNanoseconds()
	b := other.TotalNanoseconds()
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	default:
		return 0
	}
}

// MaxDuration returns the slower (larger total) of a and b.
//
// Equal totals return a verbatim, so saturating equals keep their input
// shape: Min or Max over huge totals still reports IsInfinite but does not
// normalize to InfiniteDuration. Callers needing the canonical form check
// IsInfinite first and substitute InfiniteDuration explicitly.
func MaxDuration(a, b Duration) Duration {
	if a.Compare(b) >= 0 {
		return a
	}
	return b
}

// MinDuration returns the faster (smaller total) of a and b.
//
// Equal totals return a verbatim, so saturating equals keep their input
// shape: Min or Max over huge totals still reports IsInfinite but does not
// normalize to InfiniteDuration. Callers needing the canonical form check
// IsInfinite first and substitute InfiniteDuration explicitly.
func MinDuration(a, b Duration) Duration {
	if a.Compare(b) <= 0 {
		return a
	}
	return b
}

// String renders d in lowercase form: unspecified, best_available, infinite,
// seconds, or invalid for negative input.
//
// Negative input is malformed and renders as invalid with separate seconds
// and nanoseconds fields so the decimal form never misreads a value such as
// minus one second plus half a second as minus one and a half seconds. The
// best available sentinel renders as best_available, never as a raw second
// count.
func (d Duration) String() string {
	switch {
	case d.IsUnspecified():
		return "unspecified"
	case IsBestAvailableDuration(d):
		return "best_available"
	case d.IsInfinite():
		return "infinite"
	case d.Seconds < 0:
		return fmt.Sprintf("invalid %ds %dns", d.Seconds, d.Nanoseconds)
	case d.Nanoseconds == 0:
		return fmt.Sprintf("%ds", d.Seconds)
	default:
		return fmt.Sprintf("%d.%09ds", d.Seconds, d.Nanoseconds)
	}
}
