// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package cyclone

import (
	"context"
	"testing"

	"github.com/podomy/weld/testbed/live/helpers"

	"github.com/podomy/weld/internal/transports/dds"
	"github.com/podomy/weld/internal/transports/dds/rtps"
)

// TestCycloneDiscoveryInitialLive proves initial SPDP discovery on an allocated domain.
//
// It joins one allocated domain SPDP multicast group with direct RTPS, sends one
// Weld alive packet, and requires one non-local announcement decoded with
// dds.ParseAnnouncement. GUID prefix inequality against a separate observer
// proves the peer is non-local. It uses per-run participant identities so a
// stale run never matches. It uses context timeouts on every socket call and
// never starts the compose stack.
func TestCycloneDiscoveryInitialLive(t *testing.T) {
	domain := helpers.MustAllocateDomainU32(t)
	ctx, cancel := discoverySetup(t, domain)
	defer cancel()
	observer, announcer := newLiveParticipants(t, domain, discoveryLease)
	checkAnnouncerDomain(t, announcer, domain)
	mc, sender := discoveryJoinAndSender(t, ctx, domain)
	checkInitialMulticast(t, mc, domain)
	packet := checkInitialSelf(t, announcer, domain)
	discoverySendTwice(t, sender, packet)
	ann := receiveInitialAnnouncement(t, ctx, mc, observer, announcer, domain)
	t.Logf("initial SPDP non-local prefix seq %d lease %v ports %v", ann.Sequence, ann.Lease, ann.Ports)
}

// checkAnnouncerDomain pins announcer domain identity.
func checkAnnouncerDomain(t *testing.T, announcer *dds.LocalParticipant, domain uint32) {
	t.Helper()
	if announcer.DomainID() != domain {
		t.Fatalf("announcer domain = %d, want %d", announcer.DomainID(), domain)
	}
}

// checkInitialMulticast pins multicast domain, port, and target.
func checkInitialMulticast(t *testing.T, mc *rtps.MulticastConn, domain uint32) {
	t.Helper()
	if mc.Domain() != domain {
		t.Fatalf("multicast domain = %d, want %d", mc.Domain(), domain)
	}
	wantPort, err := rtps.MetatrafficMulticastPort(domain)
	if err != nil {
		t.Fatalf("expected multicast port: %v", err)
	}
	if mc.Port() != wantPort {
		t.Fatalf("multicast port = %d, want %d", mc.Port(), wantPort)
	}
	target := mc.Target()
	if target == nil {
		t.Fatalf("nil multicast target")
	}
	if target.IP.String() != rtps.SPDPMulticastGroup {
		t.Fatalf("target group = %s, want %s", target.IP.String(), rtps.SPDPMulticastGroup)
	}
}

// checkInitialSelf marshals alive and pins self parse fields.
func checkInitialSelf(t *testing.T, announcer *dds.LocalParticipant, domain uint32) []byte {
	t.Helper()
	packet, err := dds.MarshalAlivePacket(announcer.GUID(), domain, 1, announcer.LeaseDuration())
	if err != nil {
		t.Fatalf("marshal alive: %v", err)
	}
	self, err := dds.ParseAnnouncement(packet)
	if err != nil {
		t.Fatalf("self parse: %v", err)
	}
	if self.DomainID != domain {
		t.Fatalf("self domain = %d, want %d", self.DomainID, domain)
	}
	if !self.Alive {
		t.Fatalf("self parsed as dispose")
	}
	if self.Lease <= 0 {
		t.Fatalf("self lease = %v, want positive", self.Lease)
	}
	if len(self.Ports) == 0 {
		t.Fatalf("self ports empty")
	}
	if self.GUID.Prefix != announcer.GUID().Prefix {
		t.Fatalf("self GUID mismatch")
	}
	hdrSelf, dataSelf := decodeGateSPDP(t, packet)
	checkGateWire(t, hdrSelf, dataSelf)
	return packet
}

// isInitialPacket reports whether raw is the expected announcer packet.
func isInitialPacket(raw []byte, observer, announcer *dds.LocalParticipant, domain uint32) (dds.Announcement, bool) {
	return isDiscoveryAnnouncerPacket(raw, observer, announcer, domain)
}

// validateInitialAnn pins live ports, sequence, and lease.
func validateInitialAnn(t *testing.T, ann dds.Announcement) {
	t.Helper()
	if len(ann.Ports) == 0 {
		t.Fatalf("live ports empty")
	}
	if ann.Sequence == 0 {
		t.Fatalf("live sequence is zero")
	}
	if ann.Lease <= 0 {
		t.Fatalf("live lease = %v, want positive", ann.Lease)
	}
}

// receiveInitialAnnouncement receives until announcer packet arrives.
// receiveInitialAnnouncement receives until announcer packet arrives on one allocated domain.
func receiveInitialAnnouncement(t *testing.T, ctx context.Context, mc *rtps.MulticastConn, observer, announcer *dds.LocalParticipant, domain uint32) dds.Announcement {
	t.Helper()
	for ctx.Err() == nil {
		recvCtx, recvCancel := context.WithTimeout(ctx, discoveryRecvTimeout)
		raw, rerr := mc.Receive(recvCtx)
		recvCancel()
		if rerr != nil {
			if ctx.Err() != nil {
				break
			}
			continue
		}
		ann, ok := isInitialPacket(raw, observer, announcer, domain)
		if !ok {
			continue
		}
		hdr, data := decodeGateSPDP(t, raw)
		checkGateWire(t, hdr, data)
		validateInitialAnn(t, ann)
		return ann
	}
	t.Fatalf("no non-local SPDP on domain %d", domain)
	return dds.Announcement{}
}
