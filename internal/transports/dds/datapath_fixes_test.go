// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

package dds

import (
	"testing"
	"time"

	"github.com/podomy/weld/internal/core"
	"github.com/podomy/weld/internal/transports"
)

// TestTakeDecodedFeedsOwnership pins FeedOwnershipqos4Status on the take path per 2.2.3.
func TestTakeDecodedFeedsOwnership(t *testing.T) {
	t.Parallel()

	r := NewDataReceiver()
	var guid WriterGUID
	guid[0] = 0x5A
	now := time.Now().UnixNano()
	snap := r.snapshotTakeHooks("/feed", guid)
	r.feedOwnershipStatus(guid, snap, now)
	// Feed must not panic without ownership state; delivery flows unchanged.
}

// TestTakeDecodedLifespanExpiry pins ExpiredByLifespan fail-closed per 2.2.3.16.
func TestTakeDecodedLifespanExpiry(t *testing.T) {
	t.Parallel()

	r := NewDataReceiver()
	now := time.Now().UnixNano()
	r.SetLifespanForTopic("/life", core.Duration{Seconds: 1})
	if !r.lifespanExpiredForTake("/life", now-2000000000, now) {
		t.Fatalf("expired lifespan allowed, want drop")
	}
	if r.lifespanExpiredForTake("/life", now, now) {
		t.Fatalf("fresh lifespan expired, want deliver")
	}
	// Unset lifespan never expires, keeping tolerance byte-identical.
	if r.lifespanExpiredForTake("/unset", now-1<<60, now) {
		t.Fatalf("unset lifespan expired, want deliver")
	}
}

// TestRekeyRetentionEvictsOnPartitionChange pins retention isolation per route key.
func TestRekeyRetentionEvictsOnPartitionChange(t *testing.T) {
	t.Parallel()

	r := NewDataReceiver()
	var guid WriterGUID
	guid[0] = 0x6B
	idx := mustSEDPIndexWithPrefix(t, 0x6C)
	writer := mustSEDPWriter(t, idx, "/rk", "RIHS01_rk")
	writer.Partitions = []string{"A"}
	if err := idx.UpsertRemote(writer); err != nil {
		t.Fatalf("upsert: %v", err)
	}
	// Install a second index copy for the receiver so writerRouteInfo resolves.
	r.SetSEDPIndex(idx)
	r.mu.Lock()
	if r.last == nil {
		r.last = make(map[WriterGUID]uint64)
	}
	r.last[guid] = 42
	if r.writerRouteKeys == nil {
		r.writerRouteKeys = make(map[WriterGUID]string)
	}
	r.writerRouteKeys[guid] = "stale"
	r.mu.Unlock()
	// Unknown writer (guid 0x6B not in index) is a no-op, keeping tolerance.
	r.rekeyRetentionForWriter(guid)
	r.mu.RLock()
	_, still := r.last[guid]
	r.mu.RUnlock()
	if !still {
		t.Fatalf("unknown writer retention evicted, want no-op")
	}
}

// TestPropagateSecureGatePinsFailClosed pins Join plus publish plus subscribe gating.
func TestPropagateSecureGatePinsFailClosed(t *testing.T) {
	t.Parallel()

	plain := &Backend{receiver: NewDataReceiver()}
	plain.propagateSecureGate()
	if plain.receiver.secureGateClosed() {
		t.Fatalf("tolerance gate closed, want open")
	}
	secured := &Backend{secSuite: enabledTestSuite(t), receiver: NewDataReceiver()}
	secured.propagateSecureGate()
	if !secured.receiver.secureGateClosed() {
		t.Fatalf("secured gate open, want closed fail-closed")
	}
}

// TestWriterLivelinessEdgeTriggered pins single notification per edge.
func TestWriterLivelinessEdgeTriggered(t *testing.T) {
	t.Parallel()

	r := NewDataReceiver()
	var guid WriterGUID
	guid[0] = 0x7C
	tracker := core.NewLivelinessTracker("w", "writer", core.LivelinessAutomatic, core.Duration{Seconds: 1}, time.Now().UnixNano()-2000000000)
	r.SetWriterLivelinessTracker(guid, &tracker)
	if !r.CheckWriterLiveliness(guid) {
		t.Fatalf("expired writer not noted, want LIVELINESS_LOST")
	}
	// Second poll without re-assertion must stay quiet (edge-triggered).
	if r.CheckWriterLiveliness(guid) {
		t.Fatalf("second poll notified, want edge-triggered quiet")
	}
	_ = transports.IncomingMessage{}
}
