// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

package dds

import (
	"testing"
	"time"
)

// craftedSample builds one history sample for enforcement tests.
func craftedSample(handle [16]byte, writer WriterGUID, seq uint64, at time.Time) SampledMessage {
	return SampledMessage{
		Info: SampleInfo{
			SampleState:        SampleStateNotRead,
			ViewState:          ViewStateNew,
			InstanceState:      InstanceStateAlive,
			ValidData:          true,
			ReceptionTimestamp: at,
			SourceTimestamp:    at,
			InstanceHandle:     handle,
			PublicationHandle:  [16]byte(writer),
			SampleSequence:     seq,
		},
	}
}

// TestExclusiveOwnershipSuppressesNonOwner checks exclusive delivery arbitration.
func TestExclusiveOwnershipSuppressesNonOwner(t *testing.T) {
	t.Parallel()

	recv := NewDataReceiver()
	var handle [16]byte
	handle[0] = 0xE0

	var w1, w2 WriterGUID
	w1[0] = 0x01
	w2[0] = 0x02

	err := recv.SetOwnershipKind(handle, OwnershipExclusive)
	if err != nil {
		t.Fatalf("set kind: %v", err)
	}

	recv.SetOwnershipStrength(w1, 1)
	recv.SetOwnershipStrength(w2, 10)

	if _, ok := recv.OfferOwnership(handle, w1); !ok {
		t.Fatalf("first offer not deliverable")
	}

	owner, ok := recv.OfferOwnership(handle, w2)
	if !ok || owner != w2 {
		t.Fatalf("owner = %x %v, want second writer deliverable", owner, ok)
	}

	st := recv.readerStateFor()
	st.mu.Lock()
	base := time.Unix(1000, 0).UTC()
	st.appendHistoryLocked(craftedSample(handle, w1, 1, base), false)
	st.appendHistoryLocked(craftedSample(handle, w2, 2, base.Add(time.Second)), false)
	st.mu.Unlock()

	got, err := recv.TakeNext(-1, SampleStateAny, ViewStateAny, InstanceStateAny)
	if err != nil {
		t.Fatalf("take next: %v", err)
	}

	if len(got) != 1 {
		t.Fatalf("delivered = %d, want 1 owner sample", len(got))
	}

	if got[0].Info.PublicationHandle != [16]byte(w2) {
		t.Fatalf("delivered writer = %x, want owner", got[0].Info.PublicationHandle)
	}
}

// TestTimeBasedFilterDropsFromTakeNext checks minimum separation enforcement.
func TestTimeBasedFilterDropsFromTakeNext(t *testing.T) {
	t.Parallel()

	recv := NewDataReceiver()
	var handle [16]byte
	handle[0] = 0xE1

	if err := recv.SetTimeBasedFilter(int64(time.Second)); err != nil {
		t.Fatalf("set time filter: %v", err)
	}

	st := recv.readerStateFor()
	st.mu.Lock()
	base := time.Unix(2000, 0).UTC()
	st.appendHistoryLocked(craftedSample(handle, WriterGUID{}, 1, base), false)
	st.appendHistoryLocked(craftedSample(handle, WriterGUID{}, 2, base.Add(100*time.Millisecond)), false)
	st.mu.Unlock()

	got, err := recv.TakeNext(-1, SampleStateAny, ViewStateAny, InstanceStateAny)
	if err != nil {
		t.Fatalf("take next: %v", err)
	}

	if len(got) != 1 {
		t.Fatalf("delivered = %d, want 1 after time filter", len(got))
	}

	if got := recv.TimeFilteredCount(); got != 1 {
		t.Fatalf("filtered = %d, want 1", got)
	}
}

// TestPerInstanceLimitBoundsTakeNext checks per-instance retention bounds.
func TestPerInstanceLimitBoundsTakeNext(t *testing.T) {
	t.Parallel()

	recv := NewDataReceiver()
	var handle [16]byte
	handle[0] = 0xE2

	err := recv.SetResourceLimits(ResourceLimits{MaxSamples: -1, MaxInstances: -1, MaxSamplesPerInstance: 1})
	if err != nil {
		t.Fatalf("set limits: %v", err)
	}

	st := recv.readerStateFor()
	st.mu.Lock()
	base := time.Unix(3000, 0).UTC()
	st.appendHistoryLocked(craftedSample(handle, WriterGUID{}, 1, base), false)
	st.appendHistoryLocked(craftedSample(handle, WriterGUID{}, 2, base.Add(time.Second)), false)
	st.mu.Unlock()

	got, err := recv.TakeNext(-1, SampleStateAny, ViewStateAny, InstanceStateAny)
	if err != nil {
		t.Fatalf("take next: %v", err)
	}

	if len(got) != 1 {
		t.Fatalf("delivered = %d, want 1 bounded by per-instance limit", len(got))
	}
}

// TestOrderByReceptionApplies checks oldest-reception-first delivery.
func TestOrderByReceptionApplies(t *testing.T) {
	t.Parallel()

	recv := NewDataReceiver()
	var handle [16]byte
	handle[0] = 0xE3

	st := recv.readerStateFor()
	st.mu.Lock()
	newer := craftedSample(handle, WriterGUID{}, 2, time.Unix(4002, 0).UTC())
	older := craftedSample(handle, WriterGUID{}, 1, time.Unix(4001, 0).UTC())
	st.appendHistoryLocked(newer, false)
	st.appendHistoryLocked(older, false)
	st.mu.Unlock()

	got, err := recv.TakeNext(-1, SampleStateAny, ViewStateAny, InstanceStateAny)
	if err != nil {
		t.Fatalf("take next: %v", err)
	}

	if len(got) != 2 {
		t.Fatalf("delivered = %d, want 2", len(got))
	}

	if got[0].Info.SampleSequence != 1 || got[1].Info.SampleSequence != 2 {
		t.Fatalf("order = %d %d, want 1 2 oldest first", got[0].Info.SampleSequence, got[1].Info.SampleSequence)
	}
}
