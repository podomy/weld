// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

package dds

import (
	"bytes"
	"testing"
)

// TestOwnershipSharedOwnerDeterministic pins the shared owner rule.
//
// Shared delivery still flows to every writer, but the owner query
// reports the lowest GUID so repeated queries agree regardless of map
// iteration order.
func TestOwnershipSharedOwnerDeterministic(t *testing.T) {
	t.Parallel()

	recv := NewDataReceiver()
	first, second := ownershipWriters()
	var handle [16]byte
	handle[0] = 0x09

	err := recv.SetOwnershipKind(handle, OwnershipShared)
	if err != nil {
		t.Fatalf("set kind: %v", err)
	}

	_, _ = recv.OfferOwnership(handle, second)
	_, _ = recv.OfferOwnership(handle, first)

	want := first
	if bytes.Compare(second[:], first[:]) < 0 {
		want = second
	}

	for range 20 {
		got, ok := recv.OwnershipOwner(handle)
		if !ok {
			t.Fatalf("no shared owner recorded")
		}

		if got != want {
			t.Fatalf("shared owner = %x, want lowest %x", got, want)
		}
	}
}

// ownershipqos4Writers returns two ordered writer GUIDs for arbitration tests.
func ownershipqos4Writers() (WriterGUID, WriterGUID) {
	first := testWriterGUID()
	second := testWriterGUID()
	second[0]++
	return first, second
}

// TestOwnershipqos4AliveFilters pins dead writers lose arbitration.
func TestOwnershipqos4AliveFilters(t *testing.T) {
	t.Parallel()
	recv := NewDataReceiver()
	first, second := ownershipqos4Writers()
	var handle [16]byte
	handle[0] = 0xA1
	err := recv.SetOwnershipKind(handle, OwnershipExclusive)
	if err != nil {
		t.Fatalf("set kind: %v", err)
	}
	recv.SetOwnershipStrength(first, 10)
	recv.SetOwnershipStrength(second, 1)
	if _, ok := recv.OfferOwnership(handle, first); !ok {
		t.Fatalf("first offer must deliver when alone")
	}
	recv.SetOwnershipqos4Alive(first, false)
	owner, ok := recv.OfferOwnership(handle, second)
	if !ok {
		t.Fatalf("alive second must deliver when first is dead")
	}
	if owner != second {
		t.Fatalf("owner = %x, want second", owner)
	}
	if stored, ok := recv.OwnershipOwner(handle); !ok || stored != second {
		t.Fatalf("stored owner = %x, want second", stored)
	}
}

// TestOwnershipqos4DeadlineFilters pins deadline-missed writers lose arbitration.
func TestOwnershipqos4DeadlineFilters(t *testing.T) {
	t.Parallel()
	recv := NewDataReceiver()
	first, second := ownershipqos4Writers()
	var handle [16]byte
	handle[0] = 0xA2
	err := recv.SetOwnershipKind(handle, OwnershipExclusive)
	if err != nil {
		t.Fatalf("set kind: %v", err)
	}
	recv.SetOwnershipStrength(first, 20)
	recv.SetOwnershipStrength(second, 5)
	if _, ok := recv.OfferOwnership(handle, first); !ok {
		t.Fatalf("stronger first must deliver when alone")
	}
	recv.SetOwnershipqos4DeadlineMissed(first, true)
	owner, ok := recv.OfferOwnership(handle, second)
	if !ok {
		t.Fatalf("clean second must deliver when first missed deadline")
	}
	if owner != second {
		t.Fatalf("owner = %x, want second", owner)
	}
}

// TestOwnershipqos4NoEligibleReportsFalse pins no alive clean writer reports false.
func TestOwnershipqos4NoEligibleReportsFalse(t *testing.T) {
	t.Parallel()
	recv := NewDataReceiver()
	first, second := ownershipqos4Writers()
	var handle [16]byte
	handle[0] = 0xA3
	err := recv.SetOwnershipKind(handle, OwnershipExclusive)
	if err != nil {
		t.Fatalf("set kind: %v", err)
	}
	recv.SetOwnershipStrength(first, 10)
	recv.SetOwnershipStrength(second, 20)
	_, _ = recv.OfferOwnership(handle, first)
	_, _ = recv.OfferOwnership(handle, second)
	recv.SetOwnershipqos4Alive(first, false)
	recv.SetOwnershipqos4Alive(second, false)
	if _, ok := recv.OwnershipOwner(handle); ok {
		t.Fatalf("no eligible writer must report false")
	}
	if _, ok := recv.OfferOwnership(handle, first); ok {
		t.Fatalf("dead writer offer must not deliver")
	}
}
