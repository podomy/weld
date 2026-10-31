// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

//nolint:cyclop // chaos conformance holds sequential wireless matrix assertions, high average reflects breadth
package unit_chaos

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/podomy/weld/internal/chaos"
	"github.com/podomy/weld/internal/transports"
)

// burstGoodNanos is the good state dwell for the conformance burst matrix.
const burstGoodNanos = int64(5_000_000_000)

// burstBadNanos is the bad state dwell for the conformance burst matrix.
const burstBadNanos = int64(300_000_000)

// burstPeriodNanos is the 50 Hz publish period for the burst matrix.
const burstPeriodNanos = int64(20_000_000)

// burstSpells is the repeat count for the burst matrix.
const burstSpells = 10

// openBackend joins a backend or fails the test.
func openBackend(t *testing.T, cfg chaos.ChaosConfig) *chaos.ChaosBackend {
	t.Helper()

	backend, err := chaos.NewChaosBackend(cfg)
	if err != nil {
		t.Fatalf("new backend: %v", err)
	}

	err = backend.JoinDiscovery(context.Background(), transports.DiscoveryInfo{ParticipantName: "conformance"})
	if err != nil {
		t.Fatalf("join discovery: %v", err)
	}

	return backend
}

// sendIndex publishes the publish index as a 4 byte payload.
func sendIndex(t *testing.T, backend *chaos.ChaosBackend, idx int) {
	t.Helper()

	payload := []byte{byte(idx >> 24), byte(idx >> 16), byte(idx >> 8), byte(idx)} //nolint:gosec // G115: spell index stays below 2650 and fits in 4 bytes

	err := backend.PublishBytes(context.Background(), transports.OutgoingMessage{TopicName: "/burst", Payload: payload})
	if err != nil {
		t.Fatalf("publish index %d: %v", idx, err)
	}
}

// recvIndex takes one message and decodes its publish index.
func recvIndex(t *testing.T, backend *chaos.ChaosBackend) int {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	msg, err := backend.TakeBytes(ctx)
	if err != nil {
		t.Fatalf("take: %v", err)
	}

	if len(msg.Payload) != 4 {
		t.Fatalf("payload len = %d, want 4", len(msg.Payload))
	}

	return int(msg.Payload[0])<<24 | int(msg.Payload[1])<<16 | int(msg.Payload[2])<<8 | int(msg.Payload[3])
}

// TestChaosBurstLossAccounting pins the wireless section 8 burst example.
//
// Good 0 percent for 5 s plus bad 100 percent for 300 ms, 10 repeats, at
// 50 Hz publish: each spell loses exactly the 15 samples in the bad dwell
// as one contiguous gap, 150 total, with delivered indices ordered.
//
//nolint:gocognit // burst test asserts the full publish, gap, and order matrix in one run
func TestChaosBurstLossAccounting(t *testing.T) {
	t.Parallel()

	backend := openBackend(t, chaos.ChaosConfig{
		GoodNanos:       burstGoodNanos,
		BadNanos:        burstBadNanos,
		LossBadPermille: 1000,
		Seed:            42,
	})

	perSpell := int((burstGoodNanos + burstBadNanos) / burstPeriodNanos)
	total := perSpell * burstSpells
	badPerSpell := int(burstBadNanos / burstPeriodNanos)

	idx := 0

	for spell := range burstSpells {
		base := int64(spell) * (burstGoodNanos + burstBadNanos)

		for i := range perSpell {
			backend.SetClock(base + int64(i)*burstPeriodNanos)
			sendIndex(t, backend, idx)
			idx++
		}
	}

	stats := backend.Stats()
	if stats.Published != uint64(total) {
		t.Fatalf("published = %d, want %d", stats.Published, total)
	}

	if stats.Dropped != uint64(badPerSpell*burstSpells) {
		t.Fatalf("dropped = %d, want %d", stats.Dropped, badPerSpell*burstSpells)
	}

	seen := make(map[int]bool, total)

	var order []int

	for range total - badPerSpell*burstSpells {
		got := recvIndex(t, backend)
		seen[got] = true
		order = append(order, got)
	}

	for i := 1; i < len(order); i++ {
		if order[i] <= order[i-1] {
			t.Fatalf("delivery not ordered at %d: %d after %d", i, order[i], order[i-1])
		}
	}

	var missing []int

	for i := range total {
		if !seen[i] {
			missing = append(missing, i)
		}
	}

	if len(missing) != badPerSpell*burstSpells {
		t.Fatalf("missing = %d, want %d", len(missing), badPerSpell*burstSpells)
	}

	for spell := range burstSpells {
		start := spell*perSpell + perSpell - badPerSpell

		for k := range badPerSpell {
			want := start + k
			got := missing[spell*badPerSpell+k]

			if got != want {
				t.Fatalf("spell %d gap %d = %d, want %d", spell, k, got, want)
			}
		}
	}
}

// TestChaosPartitionQueryEachSide checks split, local query, and rejoin.
//
// Each side reports only its visible segment: history keeps local publishes
// during the split, takes block across the cut, and Heal converges to
// publish order with the generation showing remove plus add.
func TestChaosPartitionQueryEachSide(t *testing.T) {
	t.Parallel()

	backend := openBackend(t, chaos.ChaosConfig{})

	err := backend.PublishBytes(context.Background(), transports.OutgoingMessage{TopicName: "/part", Payload: []byte{1}})
	if err != nil {
		t.Fatalf("publish pre split: %v", err)
	}

	if got := len(backend.History()); got != 1 {
		t.Fatalf("history before split = %d, want 1", got)
	}

	backend.Split()

	if backend.DiscoveryGeneration() != 1 {
		t.Fatalf("generation after split = %d, want 1", backend.DiscoveryGeneration())
	}

	for _, payload := range [][]byte{{2}, {3}} {
		err := backend.PublishBytes(context.Background(), transports.OutgoingMessage{TopicName: "/part", Payload: payload})
		if err != nil {
			t.Fatalf("publish during split: %v", err)
		}
	}

	if got := len(backend.History()); got != 3 {
		t.Fatalf("partition local history = %d, want 3", got)
	}

	if stats := backend.Stats(); stats.Parked != 2 {
		t.Fatalf("parked = %d, want 2", stats.Parked)
	}

	backend.Heal()

	if backend.DiscoveryGeneration() != 2 {
		t.Fatalf("generation after heal = %d, want 2", backend.DiscoveryGeneration())
	}

	for _, want := range [][]byte{{1}, {2}, {3}} {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)

		msg, err := backend.TakeBytes(ctx)
		cancel()

		if err != nil {
			t.Fatalf("take after heal: %v", err)
		}

		if !bytes.Equal(msg.Payload, want) {
			t.Fatalf("take = %v, want %v", msg.Payload, want)
		}
	}
}

// TestChaosDiscoveryFlapDebounced checks the 200 ms flap ticks nothing.
//
// Reports inside the window are ignored without touching the watermark or
// the generation; after the window the watermark resumes where it stopped.
func TestChaosDiscoveryFlapDebounced(t *testing.T) {
	t.Parallel()

	backend := openBackend(t, chaos.ChaosConfig{})

	err := backend.ReportLiveliness(context.Background(), transports.LivelinessReport{ParticipantName: "flapper", Generation: 1, Alive: true})
	if err != nil {
		t.Fatalf("baseline report: %v", err)
	}

	backend.Flap(chaos.DiscoveryFlapNanos)

	err = backend.ReportLiveliness(context.Background(), transports.LivelinessReport{ParticipantName: "flapper", Generation: 2, Alive: true})
	if err != nil {
		t.Fatalf("report inside flap is not ignored: %v", err)
	}

	backend.Advance(chaos.DiscoveryFlapNanos - 1)

	if !backend.FlapActive() {
		t.Fatalf("flap ends %d ns early", 1)
	}

	backend.Advance(1)

	if backend.FlapActive() {
		t.Fatalf("flap still active after 200 ms")
	}

	if backend.DiscoveryGeneration() != 0 {
		t.Fatalf("generation after flap = %d, want 0", backend.DiscoveryGeneration())
	}

	err = backend.ReportLiveliness(context.Background(), transports.LivelinessReport{ParticipantName: "flapper", Generation: 2, Alive: true})
	if err != nil {
		t.Fatalf("watermark moved during flap, want ignored: %v", err)
	}

	err = backend.ReportLiveliness(context.Background(), transports.LivelinessReport{ParticipantName: "flapper", Generation: 2, Alive: false})
	if !errors.Is(err, transports.ErrStaleGeneration) {
		t.Fatalf("conflicting reuse = %v, want ErrStaleGeneration", err)
	}
}

// waitForChaosProgress polls until the taker delivers at least once.
func waitForChaosProgress(t *testing.T, backend *chaos.ChaosBackend) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for backend.Stats().Delivered == 0 {
		if time.Now().After(deadline) {
			t.Fatalf("no publisher progress before close: delivered = 0, want at least 1 of 5")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestChaosTeardownMidRun checks Close interrupts a draining taker.
func TestChaosTeardownMidRun(t *testing.T) {
	t.Parallel()

	backend := openBackend(t, chaos.ChaosConfig{})

	for range 5 {
		err := backend.PublishBytes(context.Background(), transports.OutgoingMessage{TopicName: "/drain", Payload: []byte{1}})
		if err != nil {
			t.Fatalf("publish: %v", err)
		}
	}

	collected := make(chan uint64, 1)
	failed := make(chan error, 1)

	var wg sync.WaitGroup

	wg.Go(func() {
		var count uint64

		for {
			_, err := backend.TakeBytes(context.Background())
			if err != nil {
				failed <- err
				collected <- count

				return
			}

			count++
		}
	})

	time.Sleep(50 * time.Millisecond)

	waitForChaosProgress(t, backend)

	err := backend.Close()
	if err != nil {
		t.Fatalf("close mid run: %v", err)
	}

	wg.Wait()

	err = <-failed
	if !errors.Is(err, transports.ErrBackendClosed) {
		t.Fatalf("drain error = %v, want ErrBackendClosed", err)
	}

	got := <-collected

	stats := backend.Stats()
	if stats.Delivered == 0 {
		t.Fatalf("delivered = 0, want at least 1 to prove mid-drain interrupt, got = %d", got)
	}
	if got == 0 {
		t.Fatalf("collected = 0, want at least 1 to prove mid-drain interrupt, delivered = %d", stats.Delivered)
	}
	if got != stats.Delivered {
		t.Fatalf("collected = %d, delivered = %d", got, stats.Delivered)
	}

	if stats.Published != 5 {
		t.Fatalf("published = %d, want 5", stats.Published)
	}

	err = backend.Close()
	if err != nil {
		t.Fatalf("second close is not idempotent: %v", err)
	}
}

// TestChaosLargeMatrixServesLateJoiners checks bulk plus control retention.
//
// Every matrix size round trips intact on Take and stays visible in the
// bounded history snapshot a late joiner reads after the burst.
func TestChaosLargeMatrixServesLateJoiners(t *testing.T) {
	t.Parallel()

	backend := openBackend(t, chaos.ChaosConfig{})

	sizes := chaos.LargePayloadSizes()

	for i, size := range sizes {
		payload := chaos.MakePayload(size, byte(i+1))

		err := backend.PublishBytes(context.Background(), transports.OutgoingMessage{TopicName: "/bulk", Payload: payload})
		if err != nil {
			t.Fatalf("publish size %d: %v", size, err)
		}
	}

	history := backend.History()

	if len(history) != len(sizes) {
		t.Fatalf("history = %d entries, want %d", len(history), len(sizes))
	}

	if backend.HistoryLost() != 0 {
		t.Fatalf("history lost = %d, want 0", backend.HistoryLost())
	}

	for i, size := range sizes {
		want := chaos.MakePayload(size, byte(i+1))

		if !bytes.Equal(history[i].Payload, want) {
			t.Fatalf("history %d len = %d, want %d", i, len(history[i].Payload), len(want))
		}

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)

		msg, err := backend.TakeBytes(ctx)
		cancel()

		if err != nil {
			t.Fatalf("take size %d: %v", size, err)
		}

		if !bytes.Equal(msg.Payload, want) {
			t.Fatalf("take %d len = %d, want %d", i, len(msg.Payload), len(want))
		}
	}
}

// TestChaosClockJumpMovesLinkState checks jumps rebase link decisions.
//
// A backward jump returns the link to good, a forward jump into the bad
// dwell drops deterministically, and delivery resumes after the jump.
func TestChaosClockJumpMovesLinkState(t *testing.T) {
	t.Parallel()

	backend := openBackend(t, chaos.ChaosConfig{
		GoodNanos:       burstGoodNanos,
		BadNanos:        burstBadNanos,
		LossBadPermille: 1000,
	})

	backend.SetClock(1_000_000_000)

	err := backend.PublishBytes(context.Background(), transports.OutgoingMessage{TopicName: "/jump", Payload: []byte{10}})
	if err != nil {
		t.Fatalf("publish in good: %v", err)
	}

	backend.SetClock(burstGoodNanos + 100_000_000)

	err = backend.PublishBytes(context.Background(), transports.OutgoingMessage{TopicName: "/jump", Payload: []byte{11}})
	if err != nil {
		t.Fatalf("publish in bad: %v", err)
	}

	backend.SetClock(1_000_000_000)

	err = backend.PublishBytes(context.Background(), transports.OutgoingMessage{TopicName: "/jump", Payload: []byte{12}})
	if err != nil {
		t.Fatalf("publish after backward jump: %v", err)
	}

	stats := backend.Stats()
	if stats.Published != 3 || stats.Dropped != 1 {
		t.Fatalf("stats = %+v, want 3 published 1 dropped", stats)
	}

	for _, want := range [][]byte{{10}, {12}} {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)

		msg, err := backend.TakeBytes(ctx)
		cancel()

		if err != nil {
			t.Fatalf("take: %v", err)
		}

		if !bytes.Equal(msg.Payload, want) {
			t.Fatalf("take = %v, want %v", msg.Payload, want)
		}
	}
}
