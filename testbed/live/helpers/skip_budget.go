// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package helpers

import (
	"maps"
	"sync"
	"testing"
	"time"
)

// SkipAllowance is one allowlisted product skip entry.
//
// Test is the full test name. Reason records why the skip is allowed.
// Expires bounds the allowance; a zero or past time means expired.
type SkipAllowance struct {
	Reason  string
	Expires time.Time
}

var (
	// skipMu guards skipAllowances and skipCounts.
	skipMu sync.Mutex
	// skipAllowances holds product skips by test name.
	skipAllowances = make(map[string]SkipAllowance)
	// skipCounts holds product skip hits by test name per run.
	skipCounts = make(map[string]int)
)

// RegisterAllowlistedSkip adds one product skip to the run registry.
//
// Test is the full test name. Reason must be non-empty. Expires must be
// a future time; past or zero expiries fail closed at use time.
func RegisterAllowlistedSkip(testName, reason string, expires time.Time) {
	skipMu.Lock()
	skipAllowances[testName] = SkipAllowance{Reason: reason, Expires: expires}
	skipMu.Unlock()
}

// resetSkipBudget clears per-run product skip counts.
//
// It keeps the allowlist and exists for unit isolation only.
func resetSkipBudget() {
	skipMu.Lock()
	skipCounts = make(map[string]int)
	skipMu.Unlock()
}

// ResetSkipBudget clears per-run product skip counts.
//
// It keeps the allowlist. Use it for unit isolation of the budget itself.
func ResetSkipBudget() {
	resetSkipBudget()
}

// lookupAllowance returns the allowance for a test name.
//
// It reports false when the test has no registry entry.
func lookupAllowance(testName string) (SkipAllowance, bool) {
	skipMu.Lock()
	entry, ok := skipAllowances[testName]
	skipMu.Unlock()
	return entry, ok
}

// recordProductSkip counts one product skip hit for a test name.
func recordProductSkip(testName string) {
	skipMu.Lock()
	skipCounts[testName]++
	skipMu.Unlock()
}

// NoteProductSkip records one product skip and skips when allowlisted.
//
// It fails the test when the test name is not allowlisted, when the
// reason is empty, or when the entry is missing a reason or is expired.
// SkipIfNoPeer stays exempt and must never call this helper because a
// missing daemon or peer is environment, not product.
func NoteProductSkip(t *testing.T, testName, reason string) {
	t.Helper()
	recordProductSkip(testName)
	entry, ok := lookupAllowance(testName)
	if !ok {
		t.Fatalf("helpers: unallowlisted product skip %q reason %q", testName, reason)
	}
	if reason == "" {
		t.Fatalf("helpers: product skip %q needs a reason", testName)
	}
	if entry.Reason == "" {
		t.Fatalf("helpers: allowlist entry %q needs a reason", testName)
	}
	now := time.Now()
	if entry.Expires.IsZero() || !now.Before(entry.Expires) {
		t.Fatalf("helpers: allowlisted skip %q expired %v now %v reason %q", testName, entry.Expires, now, entry.Reason)
	}
	t.Skipf("helpers: allowlisted product skip %q reason %q", testName, reason)
}

// AssertNoExcessSkips fails when a recorded product skip is not allowed.
//
// It fails for an unallowlisted test name, for a missing registry reason,
// or for an expired entry. Call it at the end of a gate or in TestMain.
// SkipIfNoPeer skips are never recorded here and never fail this check.
func AssertNoExcessSkips(t *testing.T) {
	t.Helper()
	skipMu.Lock()
	pending := make(map[string]int, len(skipCounts))
	maps.Copy(pending, skipCounts)
	allow := make(map[string]SkipAllowance, len(skipAllowances))
	maps.Copy(allow, skipAllowances)
	skipMu.Unlock()
	now := time.Now()
	for name, n := range pending {
		entry, ok := allow[name]
		if !ok {
			t.Fatalf("helpers: unallowlisted product skip %q seen %d times", name, n)
		}
		if entry.Reason == "" {
			t.Fatalf("helpers: allowlist entry %q needs a reason", name)
		}
		if entry.Expires.IsZero() || !now.Before(entry.Expires) {
			t.Fatalf("helpers: allowlisted skip %q expired %v now %v seen %d times reason %q", name, entry.Expires, now, n, entry.Reason)
		}
	}
}
