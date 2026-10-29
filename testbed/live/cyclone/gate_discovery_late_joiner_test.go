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

// TestCycloneDiscoveryLateJoinerLive proves a late joiner still discovers a peer.
//
// The announcer sends first, then the late observer joins one allocated domain
// SPDP multicast group and still captures the next announcement. A remote
// table fed late with the same packet also adds the entry, which mirrors
// the unit late joiner path. GUID prefix inequality proves non-locality.
// All socket calls use context timeouts and the compose stack is never
// started.
func TestCycloneDiscoveryLateJoinerLive(t *testing.T) {
	domain := helpers.MustAllocateDomainU32(t)
	ctx, cancel := discoverySetup(t, domain)
	defer cancel()
	announcer := checkLateEarlySend(t, domain)
	observer, mc, sender := checkLateSetup(t, ctx, announcer, domain)
	latePacket := checkLateSelf(t, announcer, domain)
	discoverySendTwice(t, sender, latePacket)
	receiveLateJoiner(t, ctx, mc, observer, announcer, domain)
	checkLateTable(t, latePacket, observer, announcer)
}

// checkLateEarlySend sends one early packet before observer joins.
func checkLateEarlySend(t *testing.T, domain uint32) *dds.LocalParticipant {
	t.Helper()
	announcer, err := dds.NewLocalParticipant(domain, discoveryLease)
	if err != nil {
		t.Fatalf("new announcer: %v", err)
	}
	earlySender, err := rtps.DialMulticastSender(domain)
	if err != nil {
		t.Fatalf("dial early sender: %v", err)
	}
	t.Cleanup(func() {
		discoveryCloseSender(t, earlySender)
	})
	earlyPacket, err := dds.MarshalAlivePacket(announcer.GUID(), domain, 1, announcer.LeaseDuration())
	if err != nil {
		t.Fatalf("marshal early alive: %v", err)
	}
	err = earlySender.Send(earlyPacket)
	if err != nil {
		t.Fatalf("early multicast send: %v", err)
	}
	return announcer
}

// checkLateSetup creates late observer plus multicast and sender.
// checkLateSetup creates late observer plus multicast and sender on one allocated domain.
func checkLateSetup(t *testing.T, ctx context.Context, announcer *dds.LocalParticipant, domain uint32) (*dds.LocalParticipant, *rtps.MulticastConn, *rtps.MulticastSender) {
	t.Helper()
	observer, err := dds.NewLocalParticipant(domain, discoveryLease)
	if err != nil {
		t.Fatalf("new late observer: %v", err)
	}
	if observer.GUID().Prefix == announcer.GUID().Prefix {
		t.Fatalf("observer and announcer prefixes match, want distinct")
	}
	mc, sender := discoveryJoinAndSender(t, ctx, domain)
	return observer, mc, sender
}

// checkLateSelf marshals late packet and pins self fields.
// checkLateSelf marshals late packet and pins self fields on one allocated domain.
func checkLateSelf(t *testing.T, announcer *dds.LocalParticipant, domain uint32) []byte {
	t.Helper()
	latePacket, err := dds.MarshalAlivePacket(announcer.GUID(), domain, 2, announcer.LeaseDuration())
	if err != nil {
		t.Fatalf("marshal late alive: %v", err)
	}
	self, err := dds.ParseAnnouncement(latePacket)
	if err != nil {
		t.Fatalf("self parse: %v", err)
	}
	if self.GUID.Prefix != announcer.GUID().Prefix {
		t.Fatalf("self GUID mismatch")
	}
	if self.Sequence != 2 {
		t.Fatalf("self seq = %d, want 2", self.Sequence)
	}
	return latePacket
}

// isLatePacket reports whether raw is the expected late packet.
// isLatePacket reports whether raw is the expected late packet on one allocated domain.
func isLatePacket(raw []byte, observer, announcer *dds.LocalParticipant, domain uint32) (dds.Announcement, bool) {
	return isDiscoveryAnnouncerPacket(raw, observer, announcer, domain)
}

// receiveLateJoiner receives until late packet arrives.
// receiveLateJoiner receives until late packet arrives on one allocated domain.
func receiveLateJoiner(t *testing.T, ctx context.Context, mc *rtps.MulticastConn, observer, announcer *dds.LocalParticipant, domain uint32) {
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
		ann, ok := isLatePacket(raw, observer, announcer, domain)
		if !ok {
			continue
		}
		hdr, data := decodeGateSPDP(t, raw)
		checkGateWire(t, hdr, data)
		t.Logf("late joiner captured seq %d lease %v", ann.Sequence, ann.Lease)
		return
	}
	t.Fatalf("late joiner saw no SPDP on domain %d", domain)
}

// checkLateTable proves late packet lands in a fresh table.
func checkLateTable(t *testing.T, latePacket []byte, observer, announcer *dds.LocalParticipant) {
	t.Helper()
	table := dds.NewRemoteTable(nil)
	now := time.Now().UTC()
	err := table.HandlePacket(latePacket, now)
	if err != nil {
		t.Fatalf("late table handle: %v", err)
	}
	if table.Len() != 1 {
		t.Fatalf("late table len = %d, want 1", table.Len())
	}
	info, ok := table.Get(announcer.GUID().Prefix)
	if !ok {
		t.Fatalf("late table missing announcer")
	}
	if info.LastSeq != 2 {
		t.Fatalf("late table seq = %d, want 2", info.LastSeq)
	}
	if info.GUID.Prefix == observer.GUID().Prefix {
		t.Fatalf("late table prefix matches observer, want non-local")
	}
}
