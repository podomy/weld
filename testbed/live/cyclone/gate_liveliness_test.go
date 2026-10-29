// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

// Package cyclone holds Cyclone SPDP liveliness live cases.
//
// These tests use allocated exclusive domains so a live run never
// hears other live runs. They use direct RTPS
// ParseAnnouncement plus RemoteTable and never parse trace files.
package cyclone

import (
	"context"
	"errors"
	"os/exec"
	"testing"
	"time"

	"github.com/podomy/weld/internal/transports/dds"
	"github.com/podomy/weld/internal/transports/dds/rtps"
	"github.com/podomy/weld/testbed/live/helpers"
)

// Cyclone liveliness timeouts on allocated exclusive domains.
//
// Each case allocates its own exclusive domain so runs never mix with each
// other. Isolation uses two allocated domains to mirror the canonical split
// without colliding with it.
const (
	liveLeaseDuration  time.Duration = 10 * time.Second
	liveJoinTimeout    time.Duration = 3 * time.Second
	liveRecvTimeout    time.Duration = 6 * time.Second
	liveOverallTimeout time.Duration = 30 * time.Second
	liveSweepBefore                  = 5 * time.Second
	liveSweepAfter                   = 11 * time.Second
)

// liveCaptureSender is a test Sender that records one packet.
type liveCaptureSender struct {
	packet []byte
}

// Send records one SPDP packet for later parsing.
func (s *liveCaptureSender) Send(_ context.Context, data []byte) error {
	s.packet = append([]byte(nil), data...)
	return nil
}

// clearLiveDomain documents multi domain isolation without process env.
//
// Multi domain checks pass explicit domains to every socket and backend
// plus DockerArgs -e for peers, so no env clear is needed for parallel use.
func clearLiveDomain(t *testing.T) {
	t.Helper()
}

// newLiveParticipants returns an observer plus announcer pair.
func newLiveParticipants(t *testing.T, domain uint32, lease time.Duration) (*dds.LocalParticipant, *dds.LocalParticipant) {
	t.Helper()
	observer, err := dds.NewLocalParticipant(domain, lease)
	if err != nil {
		t.Fatalf("new observer: %v", err)
	}
	announcer, err := dds.NewLocalParticipant(domain, lease)
	if err != nil {
		t.Fatalf("new announcer: %v", err)
	}
	if observer.GUID().Prefix == announcer.GUID().Prefix {
		t.Fatalf("observer and announcer prefixes match, want distinct")
	}
	return observer, announcer
}

// joinLiveMulticast joins the SPDP multicast group for one domain.
func joinLiveMulticast(t *testing.T, ctx context.Context, domain uint32) *rtps.MulticastConn {
	t.Helper()
	joinCtx, joinCancel := context.WithTimeout(ctx, liveJoinTimeout)
	mc, err := rtps.JoinMulticast(joinCtx, domain)
	joinCancel()
	if err != nil {
		t.Fatalf("join multicast %d: %v", domain, err)
	}
	t.Cleanup(func() {
		cerr := mc.Close()
		if cerr != nil {
			t.Logf("multicast close: %v", cerr)
		}
	})
	want, err := rtps.MetatrafficMulticastPort(domain)
	if err != nil {
		t.Fatalf("expected multicast port: %v", err)
	}
	if mc.Port() != want {
		t.Fatalf("multicast port = %d, want %d", mc.Port(), want)
	}
	return mc
}

// dialLiveSender returns a send-only SPDP multicast sender.
func dialLiveSender(t *testing.T, domain uint32) *rtps.MulticastSender {
	t.Helper()
	sender, err := rtps.DialMulticastSender(domain)
	if err != nil {
		t.Fatalf("dial multicast sender %d: %v", domain, err)
	}
	t.Cleanup(func() {
		cerr := sender.Close()
		if cerr != nil {
			t.Logf("sender close: %v", cerr)
		}
	})
	return sender
}

// marshalLiveAlive builds one alive packet and checks the self parse.
func marshalLiveAlive(t *testing.T, announcer *dds.LocalParticipant, domain uint32, seq uint64) []byte {
	t.Helper()
	packet, err := dds.MarshalAlivePacket(announcer.GUID(), domain, seq, announcer.LeaseDuration())
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
	return packet
}

// sendLivePacket multicasts one packet and fails on send error.
func sendLivePacket(t *testing.T, sender *rtps.MulticastSender, packet []byte) {
	t.Helper()
	err := sender.Send(packet)
	if err != nil {
		t.Fatalf("multicast send: %v", err)
	}
}

// tryParseLivePacket checks one raw packet for the announcer.
func tryParseLivePacket(t *testing.T, raw []byte, announcer, observer dds.GUID, domain uint32) (dds.Announcement, bool) {
	t.Helper()
	if !containsGateSPDP(raw) {
		return dds.Announcement{}, false
	}
	ann, err := dds.ParseAnnouncement(raw)
	if err != nil {
		if errors.Is(err, dds.ErrInvalidSPDP) {
			t.Fatalf("deserialization failure on domain %d: %v", domain, err)
		}
		return dds.Announcement{}, false
	}
	if ann.DomainID != domain {
		return dds.Announcement{}, false
	}
	if ann.GUID.Prefix == observer.Prefix {
		return dds.Announcement{}, false
	}
	if ann.GUID.Prefix != announcer.Prefix {
		return dds.Announcement{}, false
	}
	if ann.Sequence == 0 {
		t.Fatalf("sequence is zero")
	}
	return ann, true
}

// handleLivePacket records one packet and checks wire format.
func handleLivePacket(t *testing.T, table *dds.RemoteTable, raw []byte) {
	t.Helper()
	hdr, data := decodeGateSPDP(t, raw)
	checkGateWire(t, hdr, data)
	err := table.HandlePacket(raw, time.Now())
	if err != nil {
		t.Fatalf("handle packet: %v", err)
	}
}

// receiveLiveAnnouncement receives until the announcer packet arrives.
func receiveLiveAnnouncement(t *testing.T, ctx context.Context, mc *rtps.MulticastConn, announcer, observer dds.GUID, domain uint32) dds.Announcement {
	t.Helper()
	table := dds.NewRemoteTable(nil)
	recvCtx, recvCancel := context.WithTimeout(ctx, liveRecvTimeout)
	defer recvCancel()
	for {
		raw, err := mc.Receive(recvCtx)
		if err != nil {
			if recvCtx.Err() != nil {
				t.Fatalf("did not capture announcement on domain %d: %v", domain, err)
			}
			continue
		}
		ann, ok := tryParseLivePacket(t, raw, announcer, observer, domain)
		if !ok {
			continue
		}
		handleLivePacket(t, table, raw)
		return ann
	}
}

// TestCycloneLivelinessLateJoinerBothWays proves late SPDP discovery.
//
// Direction A starts the multicast receiver before the announcer sends,
// mirroring a talker that starts after echo. Direction B closes the
// receiver, sends while nobody listens, then rejoins and resends,
// mirroring a pub that starts before echo where the late echo still
// sees the repeated announcement. Both use ParseAnnouncement plus
// RemoteTable on one allocated domain and skip without a daemon.
func TestCycloneLivelinessLateJoinerBothWays(t *testing.T) {
	domain := helpers.MustAllocateDomainU32(t)
	discoveryPinDomain(t, domain)
	ctx, cancel := context.WithTimeout(context.Background(), liveOverallTimeout)
	defer cancel()
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)
	runID := helpers.LiveID("cyclone-late")
	observer, announcer := newLiveParticipants(t, domain, liveLeaseDuration)
	mc := joinLiveMulticast(t, ctx, domain)
	sender := dialLiveSender(t, domain)
	packetA := marshalLiveAlive(t, announcer, domain, 1)
	sendLivePacket(t, sender, packetA)
	sendLivePacket(t, sender, packetA)
	foundA := receiveLiveAnnouncement(t, ctx, mc, announcer.GUID(), observer.GUID(), domain)
	t.Logf("late direction A alive seq %d lease %v id %s", foundA.Sequence, foundA.Lease, runID)
	err := mc.Close()
	if err != nil {
		t.Logf("multicast close: %v", err)
	}
	packetB := marshalLiveAlive(t, announcer, domain, 2)
	sendLivePacket(t, sender, packetB)
	mcLate := joinLiveMulticast(t, ctx, domain)
	sendLivePacket(t, sender, packetB)
	sendLivePacket(t, sender, packetB)
	foundB := receiveLiveAnnouncement(t, ctx, mcLate, announcer.GUID(), observer.GUID(), domain)
	if foundB.Sequence < foundA.Sequence {
		t.Fatalf("late sequence went backward: %d then %d", foundA.Sequence, foundB.Sequence)
	}
	t.Logf("late direction B alive seq %d lease %v id %s", foundB.Sequence, foundB.Lease, runID)
	probeCtx, probeCancel := context.WithTimeout(ctx, liveJoinTimeout)
	defer probeCancel()
	probe := exec.CommandContext(probeCtx, "docker", "inspect", "--format", "{{.State.Running}}", helpers.Container) //nolint:gosec // live harness inspects fixed test container with docker argv and no shell
	out, err := probe.CombinedOutput()
	if err != nil {
		t.Fatalf("docker inspect: %v output %s", err, string(out))
	}
	if string(out) == "" {
		t.Fatalf("empty docker inspect output")
	}
}

// TestCycloneLivelinessLeaseExpiryRemoves proves a 10s lease sweep.
//
// It handles one alive packet into a RemoteTable, proves a sweep before
// expiry removes nothing, then proves a sweep after the 10s lease
// removes the entry. It also captures one live multicast copy on one
// allocated domain so the lease path is exercised over the wire.
// It skips without a daemon and never starts the compose stack.
// checkLeaseTableSweep proves early sweep keeps and late sweep removes.
func checkLeaseTableSweep(t *testing.T, packet []byte) {
	t.Helper()
	table := dds.NewRemoteTable(nil)
	now := time.Now()
	err := table.HandlePacket(packet, now)
	if err != nil {
		t.Fatalf("handle alive: %v", err)
	}
	if table.Len() != 1 {
		t.Fatalf("table len = %d, want 1", table.Len())
	}
	removed := table.Sweep(now.Add(liveSweepBefore))
	if removed != 0 {
		t.Fatalf("early sweep removed = %d, want 0", removed)
	}
	if table.Len() != 1 {
		t.Fatalf("table len after early sweep = %d, want 1", table.Len())
	}
	removed = table.Sweep(now.Add(liveSweepAfter))
	if removed != 1 {
		t.Fatalf("expiry sweep removed = %d, want 1", removed)
	}
	if table.Len() != 0 {
		t.Fatalf("table len after expiry = %d, want 0", table.Len())
	}
}

// checkLeaseWireCopy proves one live multicast copy lands in a table.
// checkLeaseWireCopy proves one live multicast copy lands in a table on one allocated domain.
func checkLeaseWireCopy(t *testing.T, ctx context.Context, mc *rtps.MulticastConn, announcer *dds.LocalParticipant, runID string, domain uint32) {
	t.Helper()
	wireTable := dds.NewRemoteTable(nil)
	recvCtx, recvCancel := context.WithTimeout(ctx, liveRecvTimeout)
	defer recvCancel()
	wireFound := receiveLeaseWire(t, recvCtx, mc, announcer, wireTable, domain)
	if !wireFound {
		t.Fatalf("no live multicast copy on domain %d id %s", domain, runID)
	}
	if wireTable.Len() != 1 {
		t.Fatalf("wire table len = %d, want 1", wireTable.Len())
	}
	removed := wireTable.Sweep(time.Now().Add(liveSweepAfter))
	if removed != 1 {
		t.Fatalf("wire expiry sweep removed = %d, want 1", removed)
	}
}

// isLeaseWirePacket reports whether raw is the expected announcer packet.
// isLeaseWirePacket reports whether raw is the expected announcer packet on one allocated domain.
func isLeaseWirePacket(raw []byte, announcer *dds.LocalParticipant, domain uint32) bool {
	if !containsGateSPDP(raw) {
		return false
	}
	wann, perr := dds.ParseAnnouncement(raw)
	if perr != nil {
		return false
	}
	if wann.DomainID != domain {
		return false
	}
	return wann.GUID.Prefix == announcer.GUID().Prefix
}

// receiveLeaseWire receives until announcer prefix arrives.
// receiveLeaseWire receives until announcer prefix arrives on one allocated domain.
func receiveLeaseWire(t *testing.T, recvCtx context.Context, mc *rtps.MulticastConn, announcer *dds.LocalParticipant, wireTable *dds.RemoteTable, domain uint32) bool {
	t.Helper()
	for {
		raw, rerr := mc.Receive(recvCtx)
		if rerr != nil {
			if recvCtx.Err() != nil {
				return false
			}
			continue
		}
		if !isLeaseWirePacket(raw, announcer, domain) {
			continue
		}
		herr := wireTable.HandlePacket(raw, time.Now())
		if herr != nil {
			t.Fatalf("wire handle: %v", herr)
		}
		return true
	}
}

func TestCycloneLivelinessLeaseExpiryRemoves(t *testing.T) {
	domain := helpers.MustAllocateDomainU32(t)
	discoveryPinDomain(t, domain)
	ctx, cancel := context.WithTimeout(context.Background(), liveOverallTimeout)
	defer cancel()
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)
	runID := helpers.LiveID("cyclone-lease")
	_, announcer := newLiveParticipants(t, domain, liveLeaseDuration)
	if announcer.LeaseDuration() != liveLeaseDuration {
		t.Fatalf("lease = %v, want %v", announcer.LeaseDuration(), liveLeaseDuration)
	}
	packet := marshalLiveAlive(t, announcer, domain, 1)
	ann, err := dds.ParseAnnouncement(packet)
	if err != nil {
		t.Fatalf("parse alive: %v", err)
	}
	if ann.Lease != liveLeaseDuration {
		t.Fatalf("ann lease = %v, want %v", ann.Lease, liveLeaseDuration)
	}
	checkLeaseTableSweep(t, packet)
	mc := joinLiveMulticast(t, ctx, domain)
	sender := dialLiveSender(t, domain)
	sendLivePacket(t, sender, packet)
	sendLivePacket(t, sender, packet)
	checkLeaseWireCopy(t, ctx, mc, announcer, runID, domain)
	t.Logf("lease expiry removed after %v id %s", liveLeaseDuration, runID)
}

// TestCycloneLivelinessDisposeRemoves proves Goodbye and dispose removal.
//
// It handles one alive packet, then removes it with a dispose packet at
// a higher sequence through RemoteTable.HandlePacket. It also exercises
// LocalParticipant.Goodbye through a capture sender and parses the
// Goodbye output as a dispose. One allocated domain keeps the run
// isolated from other runs. It skips without a daemon.
// checkDisposeTable proves alive then dispose removes the entry.
// checkDisposeTable proves alive then dispose removes the entry on one allocated domain.
func checkDisposeTable(t *testing.T, announcer *dds.LocalParticipant, table *dds.RemoteTable, domain uint32) []byte {
	t.Helper()
	alive, err := dds.MarshalAlivePacket(announcer.GUID(), domain, 1, announcer.LeaseDuration())
	if err != nil {
		t.Fatalf("marshal alive: %v", err)
	}
	err = table.HandlePacket(alive, time.Now())
	if err != nil {
		t.Fatalf("handle alive: %v", err)
	}
	if table.Len() != 1 {
		t.Fatalf("table len = %d, want 1", table.Len())
	}
	dispose, err := dds.MarshalDisposePacket(announcer.GUID(), domain, 2)
	if err != nil {
		t.Fatalf("marshal dispose: %v", err)
	}
	checkDisposePacket(t, dispose, domain)
	err = table.HandlePacket(dispose, time.Now())
	if err != nil {
		t.Fatalf("handle dispose: %v", err)
	}
	if table.Len() != 0 {
		t.Fatalf("table len after dispose = %d, want 0", table.Len())
	}
	return alive
}

// checkDisposePacket validates one dispose packet fields.
// checkDisposePacket validates one dispose packet fields on one allocated domain.
func checkDisposePacket(t *testing.T, dispose []byte, domain uint32) {
	t.Helper()
	dann, err := dds.ParseAnnouncement(dispose)
	if err != nil {
		t.Fatalf("parse dispose: %v", err)
	}
	if dann.Alive {
		t.Fatalf("dispose parsed as alive")
	}
	if dann.DomainID != domain {
		t.Fatalf("dispose domain = %d, want %d", dann.DomainID, domain)
	}
	if dann.Sequence != 2 {
		t.Fatalf("dispose seq = %d, want 2", dann.Sequence)
	}
}

// checkStaleDispose proves old sequence dispose does not remove.
// checkStaleDispose proves old sequence dispose does not remove on one allocated domain.
func checkStaleDispose(t *testing.T, announcer *dds.LocalParticipant, table *dds.RemoteTable, alive []byte, domain uint32) {
	t.Helper()
	stale, err := dds.MarshalDisposePacket(announcer.GUID(), domain, 1)
	if err != nil {
		t.Fatalf("marshal stale dispose: %v", err)
	}
	err = table.HandlePacket(alive, time.Now())
	if err != nil {
		t.Fatalf("rehandle alive: %v", err)
	}
	err = table.HandlePacket(stale, time.Now())
	if err != nil {
		t.Fatalf("handle stale dispose: %v", err)
	}
	if table.Len() != 1 {
		t.Fatalf("table len after stale dispose = %d, want 1", table.Len())
	}
}

// checkGoodbye proves Goodbye emits a dispose packet.
func checkGoodbye(t *testing.T, ctx context.Context, announcer *dds.LocalParticipant) {
	t.Helper()
	capSender := &liveCaptureSender{}
	goodbyeCtx, goodbyeCancel := context.WithTimeout(ctx, liveJoinTimeout)
	defer goodbyeCancel()
	err := announcer.Goodbye(goodbyeCtx, capSender)
	if err != nil {
		t.Fatalf("goodbye: %v", err)
	}
	if len(capSender.packet) == 0 {
		t.Fatalf("goodbye produced no packet")
	}
	gann, err := dds.ParseAnnouncement(capSender.packet)
	if err != nil {
		t.Fatalf("parse goodbye: %v", err)
	}
	if gann.Alive {
		t.Fatalf("goodbye parsed as alive, want dispose")
	}
	if gann.GUID.Prefix != announcer.GUID().Prefix {
		t.Fatalf("goodbye GUID mismatch")
	}
}

// marshalDisposeWirePackets builds alive seq 10 plus dispose seq 11.
// marshalDisposeWirePackets builds alive seq 10 plus dispose seq 11 on one allocated domain.
func marshalDisposeWirePackets(t *testing.T, announcer *dds.LocalParticipant, domain uint32) ([]byte, []byte) {
	t.Helper()
	alivePacket, err := dds.MarshalAlivePacket(announcer.GUID(), domain, 10, announcer.LeaseDuration())
	if err != nil {
		t.Fatalf("marshal live alive: %v", err)
	}
	disposePacket, err := dds.MarshalDisposePacket(announcer.GUID(), domain, 11)
	if err != nil {
		t.Fatalf("marshal live dispose: %v", err)
	}
	return alivePacket, disposePacket
}

// sendDisposeWirePackets multicasts alive twice plus dispose twice.
func sendDisposeWirePackets(t *testing.T, sender *rtps.MulticastSender, alivePacket, disposePacket []byte) {
	t.Helper()
	sendLivePacket(t, sender, alivePacket)
	sendLivePacket(t, sender, alivePacket)
	sendLivePacket(t, sender, disposePacket)
	sendLivePacket(t, sender, disposePacket)
}

// handleDisposeWireAlive records one alive seq 10 and reports sawAlive.
func handleDisposeWireAlive(t *testing.T, table *dds.RemoteTable, raw []byte, ann dds.Announcement) bool {
	t.Helper()
	if ann.Sequence != 10 {
		return false
	}
	handleLivePacket(t, table, raw)
	return true
}

// handleDisposeWireDispose validates one dispose seq 11 and reports done.
// handleDisposeWireDispose validates one dispose seq 11 and reports done on one allocated domain.
func handleDisposeWireDispose(t *testing.T, table *dds.RemoteTable, raw []byte, ann dds.Announcement, sawAlive bool, domain uint32) bool {
	t.Helper()
	if ann.Sequence != 11 {
		t.Fatalf("live dispose seq = %d, want 11", ann.Sequence)
	}
	if ann.DomainID != domain {
		t.Fatalf("live dispose domain = %d, want %d", ann.DomainID, domain)
	}
	handleLivePacket(t, table, raw)
	if table.Len() != 0 {
		t.Fatalf("live table len after dispose = %d, want 0", table.Len())
	}
	if !sawAlive {
		t.Fatalf("saw live dispose before alive")
	}
	return true
}

// handleDisposeWirePacket processes one raw packet and reports progress.
//
// It returns the updated sawAlive plus done. Unknown packets leave sawAlive
// unchanged with done false. Alive seq 10 sets sawAlive. Dispose seq 11
// validates removal and reports done true.
// handleDisposeWirePacket processes one raw packet and reports progress on one allocated domain.
func handleDisposeWirePacket(t *testing.T, table *dds.RemoteTable, raw []byte, observer, announcer *dds.LocalParticipant, sawAlive bool, domain uint32) (bool, bool) {
	t.Helper()
	ann, ok := tryParseLivePacket(t, raw, announcer.GUID(), observer.GUID(), domain)
	if !ok {
		return sawAlive, false
	}
	if ann.Alive {
		if handleDisposeWireAlive(t, table, raw, ann) {
			return true, false
		}
		return sawAlive, false
	}
	return sawAlive, handleDisposeWireDispose(t, table, raw, ann, sawAlive, domain)
}

// checkDisposeWireLive proves dispose traverses multicast and parses live.
// checkDisposeWireLive proves dispose traverses multicast and parses live on one allocated domain.
func checkDisposeWireLive(t *testing.T, ctx context.Context, observer, announcer *dds.LocalParticipant, domain uint32) {
	t.Helper()
	mc := joinLiveMulticast(t, ctx, domain)
	sender := dialLiveSender(t, domain)
	alivePacket, disposePacket := marshalDisposeWirePackets(t, announcer, domain)
	sendDisposeWirePackets(t, sender, alivePacket, disposePacket) //nolint:contextcheck // multicast Send has no context param, socket uses timeouts
	table := dds.NewRemoteTable(nil)
	recvCtx, recvCancel := context.WithTimeout(ctx, liveRecvTimeout)
	defer recvCancel()
	sawAlive := false
	for recvCtx.Err() == nil {
		raw, err := mc.Receive(recvCtx)
		if err != nil {
			if recvCtx.Err() != nil {
				break
			}
			continue
		}
		alive, done := handleDisposeWirePacket(t, table, raw, observer, announcer, sawAlive, domain)
		sawAlive = alive
		if done {
			return
		}
	}
	t.Fatalf("did not observe live dispose on domain %d", domain)
}

func TestCycloneLivelinessDisposeRemoves(t *testing.T) {
	domain := helpers.MustAllocateDomainU32(t)
	discoveryPinDomain(t, domain)
	ctx, cancel := context.WithTimeout(context.Background(), liveOverallTimeout)
	defer cancel()
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)
	runID := helpers.LiveID("cyclone-dispose")
	observer, announcer := newLiveParticipants(t, domain, liveLeaseDuration)
	table := dds.NewRemoteTable(nil)
	alive := checkDisposeTable(t, announcer, table, domain)
	checkStaleDispose(t, announcer, table, alive, domain)
	checkGoodbye(t, ctx, announcer)
	checkDisposeWireLive(t, ctx, observer, announcer, domain)
	t.Logf("dispose removed prefix id %s", runID)
}

// TestCycloneLivelinessDomainIsolation proves one listener never sees foreign traffic.
//
// It mirrors the canonical split with two allocated domains:
// a listener must never observe SPDP announced on the foreign domain. It joins
// the listener domain, sends alive on foreign, asserts no listener receive
// carries the foreign prefix, then sends alive on listener and asserts the
// listener copy parses with the listener domain.
// It skips without a daemon and uses only docker argv for the peer check.
// checkNoLeak proves listener never sees foreign domain traffic on allocated domains.
func checkNoLeak(t *testing.T, mc *rtps.MulticastConn, foreign *dds.LocalParticipant, listenerDomain, foreignDomain uint32) {
	t.Helper()
	shortCtx, shortCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer shortCancel()
	if leakSeen(t, shortCtx, mc, foreign, foreignDomain) {
		t.Fatalf("domain %d listener saw domain %d SPDP", listenerDomain, foreignDomain)
	}
}

// leakSeen polls briefly for any foreign packet.
// leakSeen polls briefly for any foreign packet on one allocated domain.
func leakSeen(t *testing.T, shortCtx context.Context, mc *rtps.MulticastConn, foreign *dds.LocalParticipant, foreignDomain uint32) bool {
	t.Helper()
	for shortCtx.Err() == nil {
		recvCtx, recvCancel := context.WithTimeout(shortCtx, 500*time.Millisecond)
		raw, rerr := mc.Receive(recvCtx)
		recvCancel()
		if rerr != nil {
			continue
		}
		if checkLeakPacket(raw, foreign, foreignDomain) {
			return true
		}
	}
	return false
}

// checkLeakPacket reports whether raw is foreign domain traffic.
// checkLeakPacket reports whether raw is foreign domain traffic on allocated domains.
func checkLeakPacket(raw []byte, foreign *dds.LocalParticipant, foreignDomain uint32) bool {
	if !containsGateSPDP(raw) {
		return false
	}
	got, perr := dds.ParseAnnouncement(raw)
	if perr != nil {
		return false
	}
	if got.DomainID == foreignDomain {
		return true
	}
	return got.GUID.Prefix == foreign.GUID().Prefix
}

// checkOwnIsolated proves own domain traffic still arrives.
// checkOwnIsolated proves own domain traffic still arrives on one allocated domain.
func checkOwnIsolated(t *testing.T, ctx context.Context, mc *rtps.MulticastConn, listener, foreign *dds.LocalParticipant, listenerDomain uint32) {
	t.Helper()
	ownSender := dialLiveSender(t, listenerDomain)
	ownPacket := marshalLiveAlive(t, listener, listenerDomain, 1)
	sendLivePacket(t, ownSender, ownPacket) //nolint:contextcheck // multicast Send has no context param, socket uses timeouts
	sendLivePacket(t, ownSender, ownPacket) //nolint:contextcheck // multicast Send has no context param, socket uses timeouts
	own := receiveLiveAnnouncement(t, ctx, mc, listener.GUID(), foreign.GUID(), listenerDomain)
	if own.DomainID != listenerDomain {
		t.Fatalf("own domain = %d, want %d", own.DomainID, listenerDomain)
	}
}

func TestCycloneLivelinessDomainIsolation(t *testing.T) {
	listenerDomain := helpers.MustAllocateDomainU32(t)
	foreignDomain := helpers.MustAllocateDomainU32(t)
	clearLiveDomain(t)
	ctx, cancel := context.WithTimeout(context.Background(), liveOverallTimeout)
	defer cancel()
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)
	runID := helpers.LiveID("cyclone-iso")
	listener, err := dds.NewLocalParticipant(listenerDomain, liveLeaseDuration)
	if err != nil {
		t.Fatalf("new listener: %v", err)
	}
	foreign, err := dds.NewLocalParticipant(foreignDomain, liveLeaseDuration)
	if err != nil {
		t.Fatalf("new foreign: %v", err)
	}
	mc := joinLiveMulticast(t, ctx, listenerDomain)
	foreignSender := dialLiveSender(t, foreignDomain)
	foreignPacket, err := dds.MarshalAlivePacket(foreign.GUID(), foreignDomain, 1, foreign.LeaseDuration())
	if err != nil {
		t.Fatalf("marshal foreign alive: %v", err)
	}
	fann, err := dds.ParseAnnouncement(foreignPacket)
	if err != nil {
		t.Fatalf("parse foreign: %v", err)
	}
	if fann.DomainID != foreignDomain {
		t.Fatalf("foreign domain = %d, want %d", fann.DomainID, foreignDomain)
	}
	sendLivePacket(t, foreignSender, foreignPacket)
	sendLivePacket(t, foreignSender, foreignPacket)
	checkNoLeak(t, mc, foreign, listenerDomain, foreignDomain)
	checkOwnIsolated(t, ctx, mc, listener, foreign, listenerDomain)
	t.Logf("isolation held %d never saw %d id %s", listenerDomain, foreignDomain, runID)
}
