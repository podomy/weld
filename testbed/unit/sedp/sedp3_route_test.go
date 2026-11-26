// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

package unit_sedp

import (
	"testing"

	"github.com/podomy/weld/internal/transports/dds"
)

// TestRouteKeysedp3Isolation pins partition route key isolation.
//
// The same DDS topic on different partitions never shares a discovery key,
// while the same set in different orders shares one, so backend demux and
// retention keyed by PartitionRouteKey stay isolated across partitions.
func TestRouteKeysedp3Isolation(t *testing.T) {
	t.Parallel()

	def, err := dds.PartitionRouteKey("rt/chatter", nil)
	if err != nil {
		t.Fatalf("default key: %v", err)
	}

	explicit, err := dds.PartitionRouteKey("rt/chatter", []string{""})
	if err != nil {
		t.Fatalf("explicit default key: %v", err)
	}

	if def != explicit {
		t.Fatalf("default %q vs explicit %q differ, want equal", def, explicit)
	}

	a, err := dds.PartitionRouteKey("rt/chatter", []string{"A"})
	if err != nil {
		t.Fatalf("partition A key: %v", err)
	}

	if a == def {
		t.Fatalf("partition A shares default key %q", def)
	}

	b, err := dds.PartitionRouteKey("rt/chatter", []string{"B"})
	if err != nil {
		t.Fatalf("partition B key: %v", err)
	}

	if b == a {
		t.Fatalf("partitions A and B share key %q", a)
	}

	ab, err := dds.PartitionRouteKey("rt/chatter", []string{"A", "B"})
	if err != nil {
		t.Fatalf("partition AB key: %v", err)
	}

	ba, err := dds.PartitionRouteKey("rt/chatter", []string{"B", "A"})
	if err != nil {
		t.Fatalf("partition BA key: %v", err)
	}

	if ab != ba {
		t.Fatalf("order changes key: %q vs %q", ab, ba)
	}
}
