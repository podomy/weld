// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package cyclone

import (
	"context"
	"testing"
	"time"

	"github.com/podomy/weld/testbed/live/helpers"

	"github.com/podomy/weld/internal/transports/dds"
	"github.com/podomy/weld/internal/transports/dds/rtps"
)

// TestCycloneDiscoveryLeaseExpiryLive proves lease expiry removes a remote entry.
//
// It sends one short lease alive packet on one allocated domain over direct RTPS,
// parses it with dds.ParseAnnouncement, feeds a remote table, and requires
// Sweep to remove the entry once the lease ends. Prefix inequality proves
// the entry is non-local. All socket calls use context timeouts and the
// compose stack is never started.
func TestCycloneDiscoveryLeaseExpiryLive(t *testing.T) {
	domain := helpers.MustAllocateDomainU32(t)
	ctx, cancel := discoverySetup(t, domain)
	defer cancel()
	observer, announcer := newLiveParticipants(t, domain, discoveryShortLease)
	mc, sender := discoveryJoinAndSender(t, ctx, domain)
	packet := checkShortAlive(t, announcer, observer, domain)
	discoverySendTwice(t, sender, packet)
	receiveShortAlive(t, ctx, mc, announcer, observer, domain)
	checkShortSweep(t, packet)
}

// checkShortAlive marshals short lease packet and pins fields.
// checkShortAlive marshals short lease packet and pins fields on one allocated domain.
func checkShortAlive(t *testing.T, announcer, observer *dds.LocalParticipant, domain uint32) []byte {
	t.Helper()
	packet, err := dds.MarshalAlivePacket(announcer.GUID(), domain, 1, discoveryShortLease)
	if err != nil {
		t.Fatalf("marshal alive: %v", err)
	}
	ann, err := dds.ParseAnnouncement(packet)
	if err != nil {
		t.Fatalf("parse alive: %v", err)
	}
	if ann.DomainID != domain {
		t.Fatalf("domain = %d, want %d", ann.DomainID, domain)
	}
	if !ann.Alive {
		t.Fatalf("parsed as dispose")
	}
	if ann.Lease != discoveryShortLease {
		t.Fatalf("lease = %v, want %v", ann.Lease, discoveryShortLease)
	}
	if ann.GUID.Prefix == observer.GUID().Prefix {
		t.Fatalf("prefix matches observer, want non-local")
	}
	hdr, data := decodeGateSPDP(t, packet)
	checkGateWire(t, hdr, data)
	return packet
}

// isShortPacket reports whether raw is the expected short lease packet.
// isShortPacket reports whether raw is the expected short lease packet on one allocated domain.
func isShortPacket(raw []byte, announcer, observer *dds.LocalParticipant, domain uint32) bool {
	if !containsGateSPDP(raw) {
		return false
	}
	got, perr := dds.ParseAnnouncement(raw)
	if perr != nil {
		return false
	}
	if got.DomainID != domain {
		return false
	}
	if !got.Alive {
		return false
	}
	if got.GUID.Prefix != announcer.GUID().Prefix {
		return false
	}
	return got.GUID.Prefix != observer.GUID().Prefix
}

// receiveShortAlive receives until short lease packet arrives.
// receiveShortAlive receives until short lease packet arrives on one allocated domain.
func receiveShortAlive(t *testing.T, ctx context.Context, mc *rtps.MulticastConn, announcer, observer *dds.LocalParticipant, domain uint32) {
	t.Helper()
	for ctx.Err() == nil {
		recvCtx, recvCancel := context.WithTimeout(ctx, 3*time.Second)
		raw, rerr := mc.Receive(recvCtx)
		recvCancel()
		if rerr != nil {
			if ctx.Err() != nil {
				break
			}
			continue
		}
		if !isShortPacket(raw, announcer, observer, domain) {
			continue
		}
		return
	}
	t.Fatalf("no short lease SPDP on domain %d", domain)
}

// checkShortSweep proves sweep lifecycle for short lease.
func checkShortSweep(t *testing.T, packet []byte) {
	t.Helper()
	table := dds.NewRemoteTable(nil)
	base := time.Now().UTC()
	err := table.HandlePacket(packet, base)
	if err != nil {
		t.Fatalf("handle alive: %v", err)
	}
	if table.Len() != 1 {
		t.Fatalf("len = %d, want 1", table.Len())
	}
	if table.Generation() != 1 {
		t.Fatalf("gen = %d, want 1", table.Generation())
	}
	checkEarlySweep(t, table, base)
	checkExpirySweep(t, table, base)
}

// checkEarlySweep proves early sweep removes nothing.
func checkEarlySweep(t *testing.T, table *dds.RemoteTable, base time.Time) {
	t.Helper()
	removed := table.Sweep(base.Add(500 * time.Millisecond))
	if removed != 0 {
		t.Fatalf("early sweep removed = %d, want 0", removed)
	}
	if table.Len() != 1 {
		t.Fatalf("len after early sweep = %d, want 1", table.Len())
	}
}

// checkExpirySweep proves expiry sweep removes the entry.
func checkExpirySweep(t *testing.T, table *dds.RemoteTable, base time.Time) {
	t.Helper()
	expiry := base.Add(discoveryShortLease).Add(discoverySweepSlack)
	removed := table.Sweep(expiry)
	if removed != 1 {
		t.Fatalf("expiry sweep removed = %d, want 1", removed)
	}
	if table.Len() != 0 {
		t.Fatalf("len = %d, want 0 after expiry", table.Len())
	}
	if table.Generation() != 2 {
		t.Fatalf("gen = %d, want 2 after expiry", table.Generation())
	}
	removed = table.Sweep(expiry.Add(time.Second))
	if removed != 0 {
		t.Fatalf("second sweep removed = %d, want 0", removed)
	}
}
