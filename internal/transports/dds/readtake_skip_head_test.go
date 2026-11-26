// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

package dds

import (
	"testing"

	"github.com/podomy/weld/internal/transports"
)

// TestDrainControlsSkipsNonMatchingHead checks head-of-line blocking removal.
//
// A non-matching control at the head must be skipped in place so a matching
// tail still drains instead of stalling behind the break.
func TestDrainControlsSkipsNonMatchingHead(t *testing.T) {
	t.Parallel()

	sampleKeyedDesc(t)
	recv := NewDataReceiver()

	first, err := recv.RegisterInstance("/ctl", "sample_test__msg__Keyed", encodeSampleKey(t, 1))
	if err != nil {
		t.Fatalf("register first: %v", err)
	}

	second, err := recv.RegisterInstance("/ctl", "sample_test__msg__Keyed", encodeSampleKey(t, 2))
	if err != nil {
		t.Fatalf("register second: %v", err)
	}

	err = recv.DisposeInstance(first)
	if err != nil {
		t.Fatalf("dispose: %v", err)
	}

	err = recv.UnregisterInstance(second)
	if err != nil {
		t.Fatalf("unregister: %v", err)
	}

	mustTakeStrings(t, recv, "/ctl", "std_msgs__msg__String", []string{"live"})

	got, err := recv.TakeNext(-1, SampleStateAny, ViewStateAny, InstanceStateNoWriters)
	if err != nil {
		t.Fatalf("take next: %v", err)
	}

	if len(got) != 1 {
		t.Fatalf("delivered = %d, want 1 matching tail", len(got))
	}

	if got[0].Info.InstanceState != InstanceStateNoWriters {
		t.Fatalf("state = %v, want no_writers", got[0].Info.InstanceState)
	}

	_ = transports.OutgoingMessage{}
}
