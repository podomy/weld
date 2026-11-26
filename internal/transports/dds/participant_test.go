// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

package dds

import (
	"errors"
	"testing"

	"github.com/podomy/weld/internal/core"
)

// TestParseWireLivelinessKindNames checks all three wire kinds parse.
func TestParseWireLivelinessKindNames(t *testing.T) {
	t.Parallel()

	for kind, want := range map[uint32]string{
		0: "automatic",
		1: "manual_by_participant",
		2: "manual_by_topic",
	} {
		got, err := ParseWireLivelinessKind(kind)
		if err != nil {
			t.Fatalf("kind %d: %v", kind, err)
		}

		if got != want {
			t.Fatalf("kind %d = %q, want %q", kind, got, want)
		}
	}

	if _, err := ParseWireLivelinessKind(9); !errors.Is(err, ErrLivelinessExpired) {
		t.Fatalf("unknown kind = %v, want ErrLivelinessExpired", err)
	}

	if !IsManualByParticipant(1) {
		t.Fatalf("kind 1 is not manual by participant")
	}

	if IsManualByParticipant(0) || IsManualByParticipant(2) {
		t.Fatalf("automatic or by-topic reads as participant")
	}
}

// TestParticipantAssertPath checks manual assert plus lease stages.
func TestParticipantAssertPath(t *testing.T) {
	t.Parallel()

	recv := NewDataReceiver()
	recv.SetParticipantLease("p1", core.NewDuration(10, 0))

	if recv.AssertParticipantAt("ghost", 100) {
		t.Fatalf("assert on unknown participant succeeded")
	}

	if !recv.AssertParticipantAt("p1", 100) {
		t.Fatalf("assert on known participant failed")
	}

	if got := recv.ParticipantState("p1", 100+5_000_000_000); got != core.LivelinessAlive {
		t.Fatalf("state at 5s = %v, want alive", got)
	}

	if got := recv.ParticipantState("p1", 100+15_000_000_000); got != core.LivelinessStale {
		t.Fatalf("state at 15s = %v, want stale", got)
	}

	if got := recv.ParticipantState("p1", 100+25_000_000_000); got != core.LivelinessExpired {
		t.Fatalf("state at 25s = %v, want expired", got)
	}

	gen := recv.ParticipantGeneration("p1")
	if gen == 0 {
		t.Fatalf("generation stayed zero across a stage change")
	}

	if !recv.AssertParticipantAt("p1", 100+25_000_000_000) {
		t.Fatalf("recovery assert failed")
	}

	if got := recv.ParticipantState("p1", 100+25_000_000_000); got != core.LivelinessAlive {
		t.Fatalf("state after assert = %v, want alive", got)
	}

	if got := recv.ParticipantGeneration("p1"); got != gen+1 {
		t.Fatalf("generation = %d, want %d", got, gen+1)
	}
}

// TestParticipantUnknownPasses checks unregistered writers never drop.
func TestParticipantUnknownPasses(t *testing.T) {
	t.Parallel()

	recv := NewDataReceiver()

	if got := recv.ParticipantState("never-seen", 999); got != core.LivelinessAlive {
		t.Fatalf("unknown state = %v, want alive", got)
	}

	recv.SetParticipantLease("", core.NewDuration(1, 0))

	if recv.AssertParticipantAt("", 1) {
		t.Fatalf("empty participant assert succeeded")
	}
}

// TestGateParticipantDeliveryMatchesCore checks gating reuses core rules.
func TestGateParticipantDeliveryMatchesCore(t *testing.T) {
	t.Parallel()

	outcome := GateParticipantDelivery(core.LivelinessExpired, false)
	if outcome.Decision != core.GateDrop {
		t.Fatalf("expired decision = %v, want drop", outcome.Decision)
	}

	held := GateParticipantDelivery(core.LivelinessStale, false)
	if held.Decision != core.GateHold {
		t.Fatalf("stale decision = %v, want hold", held.Decision)
	}

	delivered := GateParticipantDelivery(core.LivelinessAlive, false)
	if delivered.Decision != core.GateDeliver {
		t.Fatalf("alive decision = %v, want deliver", delivered.Decision)
	}
}
