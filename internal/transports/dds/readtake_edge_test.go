// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

package dds

import (
	"testing"

	"github.com/podomy/weld/internal/transports"
)

// TestReadThenTakeKeepsStoredState pins non-destructive read marking.
//
// Read returns copies marked READ while stored history keeps its NOT_READ
// info, so a later take still reports the pre-take state and drains.
func TestReadThenTakeKeepsStoredState(t *testing.T) {
	t.Parallel()

	recv := NewDataReceiver()
	st := recv.readerStateFor()

	sample := SampledMessage{
		Msg: transports.IncomingMessage{TopicName: "/q", TypeName: "t", Payload: []byte{0x01}},
	}
	sample.Info.SampleState = SampleStateNotRead
	sample.Info.ViewState = ViewStateNew
	sample.Info.InstanceState = InstanceStateAlive

	st.mu.Lock()
	st.appendHistoryLocked(sample, false)
	st.mu.Unlock()

	got, err := recv.ReadNext(-1, SampleStateAny, ViewStateAny, InstanceStateAny)
	if err != nil {
		t.Fatalf("read next: %v", err)
	}

	if len(got) != 1 {
		t.Fatalf("read = %d, want 1", len(got))
	}

	if got[0].Info.SampleState != SampleStateRead {
		t.Fatalf("read copy state = %v, want read", got[0].Info.SampleState)
	}

	history := recv.SampleHistory()
	if len(history) != 1 {
		t.Fatalf("history = %d, want 1", len(history))
	}

	if history[0].Info.SampleState != SampleStateNotRead {
		t.Fatalf("stored state = %v, want not_read", history[0].Info.SampleState)
	}

	taken, err := recv.TakeNext(-1, SampleStateAny, ViewStateAny, InstanceStateAny)
	if err != nil {
		t.Fatalf("take next: %v", err)
	}

	if len(taken) != 1 {
		t.Fatalf("take = %d, want 1", len(taken))
	}

	if taken[0].Info.SampleState != SampleStateNotRead {
		t.Fatalf("take state = %v, want not_read", taken[0].Info.SampleState)
	}

	if len(recv.SampleHistory()) != 0 {
		t.Fatalf("history not drained after take")
	}
}
