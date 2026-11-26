// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

package dds

import (
	"testing"
	"time"
)

// TestRanksAcrossGenerations checks per-collection ranks per DDS 1.4 2.2.2.5.1.6.
func TestRanksAcrossGenerations(t *testing.T) {
	t.Parallel()

	recv := NewDataReceiver()
	var handle [16]byte
	handle[0] = 0xF0

	st := recv.readerStateFor()
	st.mu.Lock()
	st.instances[handle] = &instanceRecord{
		handle:        handle,
		typeIDL:       "std_msgs__msg__String",
		topic:         "/ranks",
		view:          ViewStateNotNew,
		state:         InstanceStateAlive,
		disposedCount: 1,
	}
	base := time.Unix(7000, 0).UTC()
	first := SampledMessage{
		Info: SampleInfo{
			SampleState:             SampleStateNotRead,
			ViewState:               ViewStateNew,
			InstanceState:           InstanceStateAlive,
			ValidData:               true,
			ReceptionTimestamp:      base,
			SourceTimestamp:         base,
			InstanceHandle:          handle,
			DisposedGenerationCount: 0,
			SampleSequence:          1,
		},
	}
	second := SampledMessage{
		Info: SampleInfo{
			SampleState:             SampleStateNotRead,
			ViewState:               ViewStateNotNew,
			InstanceState:           InstanceStateAlive,
			ValidData:               true,
			ReceptionTimestamp:      base.Add(time.Second),
			SourceTimestamp:         base.Add(time.Second),
			InstanceHandle:          handle,
			DisposedGenerationCount: 1,
			SampleSequence:          2,
		},
	}
	st.appendHistoryLocked(first, false)
	st.appendHistoryLocked(second, false)
	st.mu.Unlock()

	got, err := recv.TakeNext(-1, SampleStateAny, ViewStateAny, InstanceStateAny)
	if err != nil {
		t.Fatalf("take next: %v", err)
	}

	if len(got) != 2 {
		t.Fatalf("delivered = %d, want 2", len(got))
	}

	if got[0].Info.SampleRank != 1 || got[1].Info.SampleRank != 0 {
		t.Fatalf("sample ranks = %d %d, want 1 0", got[0].Info.SampleRank, got[1].Info.SampleRank)
	}

	if got[0].Info.GenerationRank != 1 || got[1].Info.GenerationRank != 0 {
		t.Fatalf("generation ranks = %d %d, want 1 0", got[0].Info.GenerationRank, got[1].Info.GenerationRank)
	}

	if got[0].Info.AbsoluteGenerationRank != 1 || got[1].Info.AbsoluteGenerationRank != 0 {
		t.Fatalf("absolute ranks = %d %d, want 1 0", got[0].Info.AbsoluteGenerationRank, got[1].Info.AbsoluteGenerationRank)
	}
}
