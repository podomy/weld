// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

package logging

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

// TestAuditLogBounded pins oldest-first eviction past the cap.
func TestAuditLogBounded(t *testing.T) {
	t.Parallel()

	plugin := New()
	plugin.SetLogOptions(true)

	total := MaxLogEntries + 256

	for i := range total {
		plugin.Log("op", "id", true, fmt.Sprintf("reason-%d", i))
	}

	if got := plugin.Count(); got != MaxLogEntries {
		t.Fatalf("log count %d must equal the cap %d", got, MaxLogEntries)
	}

	entries := plugin.Entries()
	if len(entries) != MaxLogEntries {
		t.Fatalf("entries %d must equal the cap %d", len(entries), MaxLogEntries)
	}

	last := entries[len(entries)-1]
	if last.Reason != fmt.Sprintf("reason-%d", total-1) {
		t.Fatalf("newest event must survive, got %q", last.Reason)
	}

	first := entries[0]
	if first.Reason == "reason-0" {
		t.Fatalf("oldest event must be evicted")
	}

	allowed, denied := plugin.Summary()
	if allowed != MaxLogEntries || denied != 0 {
		t.Fatalf("summary allowed=%d denied=%d, want %d 0", allowed, denied, MaxLogEntries)
	}
}

// TestAuditLogTimestamps pins UTC timestamps on recorded events.
func TestAuditLogTimestamps(t *testing.T) {
	t.Parallel()

	plugin := New()
	plugin.SetLogOptions(true)

	before := time.Now().UTC().Add(-time.Minute)

	plugin.Log("check_remote_participant", "peer", false, "refused")

	entries := plugin.Entries()
	if len(entries) != 1 {
		t.Fatalf("entries = %d, want 1", len(entries))
	}

	at := entries[0].At
	if at.IsZero() {
		t.Fatalf("event timestamp must be set")
	}

	if at.Before(before) || at.After(time.Now().UTC().Add(time.Minute)) {
		t.Fatalf("event timestamp %v out of recording window", at)
	}

	formatted := Format(entries[0])
	if !strings.Contains(formatted, "at=") {
		t.Fatalf("formatted event misses timestamp: %q", formatted)
	}
}

// TestAuditLogcry9Sequence pins sequence continuity plus dropped counter.
func TestAuditLogcry9Sequence(t *testing.T) {
	t.Parallel()

	plugin := New()
	plugin.SetLogOptions(true)

	for i := range MaxLogEntries {
		plugin.Log("op", "id", true, fmt.Sprintf("reason-%d", i))
	}

	entries := plugin.Entries()
	if len(entries) != MaxLogEntries {
		t.Fatalf("entries %d must equal the cap %d", len(entries), MaxLogEntries)
	}

	checkLogSeqcry9(t, entries, 1)

	if got := plugin.Droppedcry9(); got != 0 {
		t.Fatalf("dropped before cap %d, want 0", got)
	}

	plugin.Log("op", "id", true, fmt.Sprintf("reason-%d", MaxLogEntries))

	entries = plugin.Entries()
	if len(entries) != MaxLogEntries {
		t.Fatalf("entries %d must stay at the cap %d", len(entries), MaxLogEntries)
	}

	if got := plugin.Droppedcry9(); got != 1 {
		t.Fatalf("dropped after one eviction %d, want 1", got)
	}

	checkLogSeqcry9(t, entries, 2)

	formatted := Format(entries[0])
	if !strings.Contains(formatted, "seq=") {
		t.Fatalf("formatted event misses sequence: %q", formatted)
	}
}

// checkLogSeqcry9 asserts contiguous sequences from wantFirst.
func checkLogSeqcry9(t *testing.T, entries []Event, wantFirst uint64) {
	t.Helper()

	for i, entry := range entries {
		want := wantFirst + uint64(i)
		if entry.Seq != want {
			t.Fatalf("seq index %d got %d, want %d", i, entry.Seq, want)
		}
	}
}
