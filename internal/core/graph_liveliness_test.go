// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package core

import (
	"sync"
	"testing"
)

// TestGraphLivelinessLateJoinerBothWays proves retained announcements
// reach late joiners in both directions plus replay without bump.
func TestGraphLivelinessLateJoinerBothWays(t *testing.T) {
	t.Run("late reader sees early writer", checkGraphLateReader)
	t.Run("late writer sees early reader", checkGraphLateWriter)
	t.Run("late subscriber replays retained", checkGraphRetainedReplay)
	t.Run("dispose only on expiry or explicit", checkGraphDisposeExpiryExplicit)
	t.Run("no silent flap", checkGraphNoFlap)
}

// checkGraphLateReader pins an early SEDP writer retained for a late reader.
func checkGraphLateReader(t *testing.T) {
	t.Helper()
	g := NewGraph()
	writerGID := sampleGID(101)
	writerInfo := sampleEntityInfo()
	writerInfo.TopicName = "/late-fwd"
	err := g.UpsertWriter(writerGID, writerInfo)
	if err != nil {
		t.Fatalf("upsert early writer: %v", err)
	}
	if g.Generation() != 1 {
		t.Fatalf("generation after early writer = %d, want 1", g.Generation())
	}
	partGID := sampleGID(103)
	partInfo := sampleParticipantInfo()
	err = g.ReportParticipantAlive(partGID, partInfo, 1, 0, 10_000_000_000)
	if err != nil {
		t.Fatalf("report alive: %v", err)
	}
	readerGID := sampleGID(102)
	readerInfo := sampleEntityInfo()
	readerInfo.TopicName = "/late-fwd"
	err = g.UpsertReader(readerGID, readerInfo)
	if err != nil {
		t.Fatalf("upsert late reader: %v", err)
	}
	got, ok := g.Writer(writerGID)
	if !ok {
		t.Fatalf("late reader must still see early writer")
	}
	if got.TopicName != "/late-fwd" {
		t.Fatalf("retained writer topic = %q", got.TopicName)
	}
	if _, ok := g.Reader(readerGID); !ok {
		t.Fatalf("late reader must be present")
	}
	if _, ok := g.Participant(partGID); !ok {
		t.Fatalf("late reader must still see SPDP participant")
	}
	if len(g.Writers()) != 1 {
		t.Fatalf("writers retained = %d, want 1", len(g.Writers()))
	}
}

// checkGraphLateWriter pins an early reader retained for a late writer.
func checkGraphLateWriter(t *testing.T) {
	t.Helper()
	g := NewGraph()
	readerGID := sampleGID(111)
	readerInfo := sampleEntityInfo()
	readerInfo.TopicName = "/late-rev"
	err := g.UpsertReader(readerGID, readerInfo)
	if err != nil {
		t.Fatalf("upsert early reader: %v", err)
	}
	writerGID := sampleGID(112)
	writerInfo := sampleEntityInfo()
	writerInfo.TopicName = "/late-rev"
	err = g.UpsertWriter(writerGID, writerInfo)
	if err != nil {
		t.Fatalf("upsert late writer: %v", err)
	}
	if _, ok := g.Reader(readerGID); !ok {
		t.Fatalf("late writer must still see early reader")
	}
	if _, ok := g.Writer(writerGID); !ok {
		t.Fatalf("late writer must be present")
	}
	if len(g.Readers()) != 1 {
		t.Fatalf("readers retained = %d, want 1", len(g.Readers()))
	}
}

// checkGraphRetainedReplay pins replay fires hook without bumping.
func checkGraphRetainedReplay(t *testing.T) {
	t.Helper()
	g := NewGraph()
	var mu sync.Mutex
	var seen []uint64
	g.SetOnChange(func(gen uint64) {
		mu.Lock()
		defer mu.Unlock()
		seen = append(seen, gen)
	})
	err := g.UpsertWriter(sampleGID(121), sampleEntityInfo())
	if err != nil {
		t.Fatalf("upsert writer: %v", err)
	}
	if g.Generation() != 1 {
		t.Fatalf("generation = %d, want 1", g.Generation())
	}
	gen := g.ReplayRetained()
	if gen != 1 {
		t.Fatalf("replay generation = %d, want 1", gen)
	}
	if g.Generation() != 1 {
		t.Fatalf("replay must not bump, got %d", g.Generation())
	}
	mu.Lock()
	defer mu.Unlock()
	if len(seen) != 2 {
		t.Fatalf("hook fired %d times, want 2 (mutate plus replay)", len(seen))
	}
	if seen[0] != 1 || seen[1] != 1 {
		t.Fatalf("hook gens = %v, want [1 1]", seen)
	}
}

// checkGraphDisposeExpiryExplicit pins remove only on expiry or explicit.
//
//nolint:gocognit,cyclop // dispose test sequences early sweep, stale seq, refresh, and expiry checks in one run, splitting would obscure intent
func checkGraphDisposeExpiryExplicit(t *testing.T) {
	t.Helper()
	g := NewGraph()
	gid := sampleGID(131)
	info := sampleParticipantInfo()
	err := g.ReportParticipantAlive(gid, info, 1, 0, 10_000_000_000)
	if err != nil {
		t.Fatalf("report alive: %v", err)
	}
	base := g.Generation()
	if got := g.SweepExpired(5_000_000_000); got != 0 {
		t.Fatalf("early sweep removed %d, want 0", got)
	}
	if g.Generation() != base {
		t.Fatalf("early sweep must not bump, got %d want %d", g.Generation(), base)
	}
	err = g.ReportParticipantAlive(gid, info, 1, 1_000_000_000, 10_000_000_000)
	if err != nil {
		t.Fatalf("duplicate seq alive: %v", err)
	}
	if g.Generation() != base {
		t.Fatalf("stale seq must not bump, got %d want %d", g.Generation(), base)
	}
	if g.ReportParticipantDispose(gid, 1) {
		t.Fatalf("stale dispose must not remove")
	}
	if g.Generation() != base {
		t.Fatalf("stale dispose must not bump")
	}
	updated := info
	updated.Sequence = info.Sequence + 1
	err = g.ReportParticipantAlive(gid, updated, 2, 1_000_000_000, 10_000_000_000)
	if err != nil {
		t.Fatalf("refresh alive: %v", err)
	}
	if g.Generation() != base+1 {
		t.Fatalf("refresh must bump once, got %d want %d", g.Generation(), base+1)
	}
	if !g.ReportParticipantDispose(gid, 3) {
		t.Fatalf("explicit dispose must remove")
	}
	if _, ok := g.Participant(gid); ok {
		t.Fatalf("disposed participant must be absent")
	}
	if g.ReportParticipantDispose(gid, 4) {
		t.Fatalf("missing dispose must report false")
	}
	// Lease expiry path with two entries bumping once.
	a := sampleGID(132)
	b := sampleGID(133)
	err = g.ReportParticipantAlive(a, sampleParticipantInfo(), 1, 0, 10_000_000_000)
	if err != nil {
		t.Fatalf("report alive a: %v", err)
	}
	err = g.ReportParticipantAlive(b, sampleParticipantInfo(), 1, 0, 10_000_000_000)
	if err != nil {
		t.Fatalf("report alive b: %v", err)
	}
	beforeSweep := g.Generation()
	removed := g.SweepExpired(11_000_000_000)
	if removed != 2 {
		t.Fatalf("expiry sweep removed %d, want 2", removed)
	}
	if g.Generation() != beforeSweep+1 {
		t.Fatalf("sweep must bump once, got %d want %d", g.Generation(), beforeSweep+1)
	}
	// Endpoints stay retained through participant sweep.
	wgid := sampleGID(134)
	err = g.UpsertWriter(wgid, sampleEntityInfo())
	if err != nil {
		t.Fatalf("upsert writer: %v", err)
	}
	pgid := sampleGID(135)
	err = g.ReportParticipantAlive(pgid, sampleParticipantInfo(), 1, 0, 5_000_000_000)
	if err != nil {
		t.Fatalf("report alive pgid: %v", err)
	}
	removed = g.SweepExpired(6_000_000_000)
	if removed != 1 {
		t.Fatalf("participant sweep removed %d, want 1", removed)
	}
	if _, ok := g.Writer(wgid); !ok {
		t.Fatalf("writer must stay retained after participant sweep")
	}
	if !g.RemoveWriter(wgid) {
		t.Fatalf("explicit writer remove must succeed")
	}
	if g.RemoveWriter(wgid) {
		t.Fatalf("second writer remove must report false")
	}
}

// checkGraphNoFlap pins generation bumps only on real mutation.
func checkGraphNoFlap(t *testing.T) {
	t.Helper()
	g := NewGraph()
	err := g.UpsertWriter(sampleGID(141), sampleEntityInfo())
	if err != nil {
		t.Fatalf("upsert writer: %v", err)
	}
	base := g.Generation()
	err = g.UpsertWriter(sampleGID(141), sampleEntityInfo())
	if err != nil {
		t.Fatalf("identical upsert: %v", err)
	}
	if g.Generation() != base {
		t.Fatalf("identical upsert must not bump, got %d want %d", g.Generation(), base)
	}
	pid := sampleGID(142)
	info := sampleParticipantInfo()
	info.Sequence = 8
	err = g.UpsertParticipant(pid, info)
	if err != nil {
		t.Fatalf("upsert participant: %v", err)
	}
	afterAdd := g.Generation()
	stale := info
	stale.Sequence = 7
	stale.Enclave = "/other"
	err = g.UpsertParticipant(pid, stale)
	if err != nil {
		t.Fatalf("stale upsert: %v", err)
	}
	if g.Generation() != afterAdd {
		t.Fatalf("stale node sequence must not bump")
	}
	got, ok := g.Participant(pid)
	if !ok {
		t.Fatalf("participant must be present")
	}
	if got.Sequence != 8 {
		t.Fatalf("stale upsert must not downgrade, got %d", got.Sequence)
	}
	err = g.UpsertParticipant(pid, info)
	if err != nil {
		t.Fatalf("identical participant upsert: %v", err)
	}
	if g.Generation() != afterAdd {
		t.Fatalf("identical participant upsert must not bump")
	}
	beforeReplay := g.Generation()
	g.ReplayRetained()
	if g.Generation() != beforeReplay {
		t.Fatalf("replay must not bump")
	}
}
