// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

package mqtt

import (
	"context"
	"encoding/json"
	"testing"
)

// installRecord installs one record under test without a broker.
func installRecord(cache *endpointCache, generation uint64, alive bool) {
	cache.install(endpointKey{participant: "talker", kind: KindData, rosTopic: "/chatter"}, endpointRecord{Type: "t", Alive: alive, Generation: generation})
}

// hasTestEndpoint reports the cached alive bit for the test endpoint.
func hasTestEndpoint(cache *endpointCache) bool {
	return cache.hasLive("/chatter", KindData)
}

// TestInstallDropsStaleRetract checks a delayed retract never hides a
// newer announce.
func TestInstallDropsStaleRetract(t *testing.T) {
	t.Parallel()

	cache := newEndpointCache()
	installRecord(cache, 5, true)

	if !hasTestEndpoint(cache) {
		t.Fatalf("endpoint missing after announce")
	}

	installRecord(cache, 3, false)

	if !hasTestEndpoint(cache) {
		t.Fatalf("stale retract hid newer announce")
	}

	if got := cache.installCount(); got != 1 {
		t.Fatalf("installs = %d, want 1 counting the dropped retract", got)
	}
}

// TestInstallAppliesFreshRetract checks a newer tombstone wins.
func TestInstallAppliesFreshRetract(t *testing.T) {
	t.Parallel()

	cache := newEndpointCache()
	installRecord(cache, 5, true)
	installRecord(cache, 7, false)

	if hasTestEndpoint(cache) {
		t.Fatalf("endpoint present after fresh retract")
	}

	if got := cache.installCount(); got != 2 {
		t.Fatalf("installs = %d, want 2", got)
	}
}

// TestInstallLegacyFirstWins documents mixed-version behavior: records
// without generations order by arrival, so legacy peers must upgrade
// together with the fleet.
func TestInstallLegacyFirstWins(t *testing.T) {
	t.Parallel()

	cache := newEndpointCache()
	installRecord(cache, 0, true)
	installRecord(cache, 0, false)

	if !hasTestEndpoint(cache) {
		t.Fatalf("legacy retract overwrote legacy announce; first writer must win")
	}
}

// newDiscoveryBackend returns a joined in-memory backend for tests.
func newDiscoveryBackend(t *testing.T) *Backend {
	t.Helper()

	backend := &Backend{
		joined:    true,
		prefix:    "weld",
		conn:      newMemoryConn(),
		version:   Version5,
		lastGen:   make(map[string]uint64),
		lastAlive: make(map[string]bool),
		discovery: newEndpointCache(),
		inbox:     make(chan ConnMessage, inboxCap),
		done:      make(chan struct{}),
	}

	t.Cleanup(func() {
		if err := backend.Close(); err != nil {
			t.Logf("backend close: %v", err)
		}
	})

	return backend
}

// staleRetractFeed delivers one crafted tombstone through the real
// noteDiscoveryMessage path.
func staleRetractFeed(t *testing.T, backend *Backend, topic string, generation uint64) {
	t.Helper()

	body, err := json.Marshal(endpointRecord{Alive: false, Generation: generation}) //nolint:errchkjson // test record holds a bool plus a counter, encoding never fails
	if err != nil {
		t.Fatalf("marshal tombstone: %v", err)
	}

	if !backend.noteDiscoveryMessage(ConnMessage{Topic: topic, Payload: body}) {
		t.Fatalf("tombstone topic not routed to discovery")
	}
}

// TestDelayedRetractNeverHidesAnnounce wires a stale tombstone through
// the receive path after a fresh announce.
func TestDelayedRetractNeverHidesAnnounce(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	backend := newDiscoveryBackend(t)

	topic, err := EncodeDiscoveryTopic("weld", 0, "talker", KindData, "/chatter")
	if err != nil {
		t.Fatalf("discovery topic: %v", err)
	}

	if err := backend.AnnounceEndpoint(ctx, "talker", KindData, "/chatter", "t", ""); err != nil {
		t.Fatalf("announce: %v", err)
	}

	staleRetractFeed(t, backend, topic, 0)

	if !backend.HasRemoteEndpoint("/chatter") {
		t.Fatalf("stale wire retract hid fresh announce")
	}

	fresh, err := json.Marshal(endpointRecord{Alive: false, Generation: 1 << 62}) //nolint:errchkjson // test record holds a bool plus a counter, encoding never fails
	if err != nil {
		t.Fatalf("marshal fresh tombstone: %v", err)
	}

	backend.noteDiscoveryMessage(ConnMessage{Topic: topic, Payload: fresh})

	if backend.HasRemoteEndpoint("/chatter") {
		t.Fatalf("fresh retract did not clear announce")
	}
}

// TestRejectedAnnounceKeepsDiscSeq proves a rejected announce or retract
// does not advance the per-writer discovery sequence: the bump happens
// only after the closed/joined gate passes.
func TestRejectedAnnounceKeepsDiscSeq(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	backend := &Backend{
		prefix:    "weld",
		conn:      newMemoryConn(),
		version:   Version5,
		lastGen:   make(map[string]uint64),
		lastAlive: make(map[string]bool),
		discovery: newEndpointCache(),
		inbox:     make(chan ConnMessage, inboxCap),
		done:      make(chan struct{}),
	}

	t.Cleanup(func() {
		if err := backend.Close(); err != nil {
			t.Logf("backend close: %v", err)
		}
	})

	if err := backend.AnnounceEndpoint(ctx, "p", KindData, "/chatter", "t", ""); err == nil {
		t.Fatalf("announce before join succeeded, want error")
	}

	backend.mu.Lock()
	seq := backend.discSeq
	backend.mu.Unlock()

	if seq != 0 {
		t.Fatalf("discSeq = %d after rejected announce, want 0", seq)
	}

	if err := backend.RetractEndpoint(ctx, "p", KindData, "/chatter"); err == nil {
		t.Fatalf("retract before join succeeded, want error")
	}

	backend.mu.Lock()
	seq = backend.discSeq
	backend.mu.Unlock()

	if seq != 0 {
		t.Fatalf("discSeq = %d after rejected retract, want 0", seq)
	}
}

// TestMalformedDiscoveryLegSkipsInbox proves a discovery-leg topic that
// fails discovery decode never reaches the take inbox: it is skipped
// like malformed JSON, not surfaced as data.
func TestMalformedDiscoveryLegSkipsInbox(t *testing.T) {
	t.Parallel()

	backend := newDiscoveryBackend(t)

	if backend.noteDiscoveryMessage(ConnMessage{Topic: "weld/0/sdp", Payload: []byte("{}")}) != true {
		t.Fatalf("bare sdp topic not skipped")
	}

	select {
	case wire := <-backend.inbox:
		t.Fatalf("bare sdp reached inbox: %+v", wire)
	default:
	}
}

// TestTakeDecodeErrorCountsDrop proves a corrupt inbox sample returns
// an error and counts one drop instead of vanishing silently.
func TestTakeDecodeErrorCountsDrop(t *testing.T) {
	t.Parallel()

	backend := newDiscoveryBackend(t)
	ctx := context.Background()

	backend.stageWire(ConnMessage{Topic: "weld/0/rt/chatter", Payload: []byte{0x01}})

	if _, err := backend.TakeBytes(ctx); err == nil {
		t.Fatalf("take of corrupt envelope succeeded, want error")
	}

	backend.mu.Lock()
	dropped := backend.dropped
	backend.mu.Unlock()

	if dropped != 1 {
		t.Fatalf("dropped = %d, want 1", dropped)
	}
}
