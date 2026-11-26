// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

package logging

import (
	"testing"
)

// TestGapsSeqPlusTombstone pins sequences and overflow markers.
func TestGapsSeqPlusTombstone(t *testing.T) {
	t.Parallel()

	plugin := New()
	plugin.EnableLogging()

	if _, ok := plugin.Tombstone(); ok {
		t.Fatalf("tombstone must be absent before overflow")
	}

	for range MaxLogEntries + 5 {
		plugin.Log("op", "id", true, "reason")
	}

	entries := plugin.Entries()
	if len(entries) != MaxLogEntries {
		t.Fatalf("entries %d, want cap %d", len(entries), MaxLogEntries)
	}

	seen := make(map[uint64]bool)
	for _, event := range entries {
		if event.Seq == 0 {
			t.Fatalf("sequence must not be zero")
		}

		if seen[event.Seq] {
			t.Fatalf("duplicate seq %d", event.Seq)
		}

		seen[event.Seq] = true
	}

	if entries[len(entries)-1].Seq != uint64(MaxLogEntries+5) {
		t.Fatalf("last seq %d, want %d", entries[len(entries)-1].Seq, MaxLogEntries+5)
	}

	marker, ok := plugin.Tombstone()
	if !ok {
		t.Fatalf("tombstone must exist after eviction")
	}

	if marker.Op != "audit-overflow" {
		t.Fatalf("tombstone op %q, want audit-overflow", marker.Op)
	}

	if plugin.Droppedcry9() != 5 {
		t.Fatalf("dropped %d, want 5", plugin.Droppedcry9())
	}
}
