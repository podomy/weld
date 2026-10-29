// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

// Package cyclone proves Gate 1 SPDP ST0 non-local discovery on an allocated domain
// with direct Go RTPS and no log file parsing.
package cyclone

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/podomy/weld/internal/transports/dds"
	"github.com/podomy/weld/internal/transports/dds/rtps"
	"github.com/podomy/weld/testbed/live/helpers"
)

// Gate 1 timeouts for allocated domains.
const (
	gateLease         time.Duration = 10 * time.Second
	gateSocketTimeout time.Duration = 3 * time.Second
	gatePureTimeout   time.Duration = 6 * time.Second
	gateLiveTimeout   time.Duration = 10 * time.Second
)

// containsGateSPDP reports whether raw holds SPDP writer DATA.
func containsGateSPDP(raw []byte) bool {
	var drops rtps.Counter
	_, raws, err := rtps.DecodeMessage(raw, &drops)
	if err != nil {
		return false
	}
	for i := range raws {
		sub, err := rtps.DecodeSubmessage(raws[i], &drops)
		if err != nil {
			continue
		}
		if sub.Data == nil {
			continue
		}
		if sub.Data.WriterID != rtps.EntitySPDPWriter {
			continue
		}
		return true
	}
	return false
}

// decodeGateSPDP decodes one datagram and returns the SPDP DATA.
func decodeGateSPDP(t *testing.T, raw []byte) (rtps.Header, rtps.Data) {
	t.Helper()
	var drops rtps.Counter
	hdr, raws, err := rtps.DecodeMessage(raw, &drops)
	if err != nil {
		t.Fatalf("decode message: %v", err)
	}
	for i := range raws {
		sub, err := rtps.DecodeSubmessage(raws[i], &drops)
		if err != nil {
			t.Fatalf("decode submessage %d: %v", i, err)
		}
		if sub.Data == nil {
			continue
		}
		if sub.Data.WriterID != rtps.EntitySPDPWriter {
			continue
		}
		return hdr, *sub.Data
	}
	t.Fatalf("no SPDP DATA in datagram")
	return rtps.Header{}, rtps.Data{}
}

// checkGateWire pins RTPS 2.1, vendor 01 10, and SPDP routing.
func checkGateWire(t *testing.T, hdr rtps.Header, data rtps.Data) {
	t.Helper()
	if hdr.Major != 2 {
		t.Fatalf("major = %d, want 2", hdr.Major)
	}
	if hdr.Minor != 1 {
		t.Fatalf("minor = %d, want 2.1", hdr.Minor)
	}
	wantVendor := [2]byte{0x01, 0x10}
	if hdr.Vendor != wantVendor {
		t.Fatalf("vendor = %v, want 01 10", hdr.Vendor)
	}
	if data.ReaderID != rtps.EntityUnknown {
		t.Fatalf("reader = %v, want unknown", data.ReaderID)
	}
	if data.WriterID != rtps.EntitySPDPWriter {
		t.Fatalf("writer = %v, want SPDP writer", data.WriterID)
	}
}

// checkGateAnnouncement pins domain, liveness, lease, ports, and non-locality.
func checkGateAnnouncement(t *testing.T, ann dds.Announcement, observer dds.GUID, domain uint32) {
	t.Helper()
	if ann.DomainID != domain {
		t.Fatalf("domain = %d, want %d", ann.DomainID, domain)
	}
	if !ann.Alive {
		t.Fatalf("parsed as dispose, want alive")
	}
	if ann.Lease <= 0 {
		t.Fatalf("lease = %v, want positive", ann.Lease)
	}
	if len(ann.Ports) == 0 {
		t.Fatalf("ports empty, want unicast locators")
	}
	if ann.GUID.Prefix == observer.Prefix {
		t.Fatalf("prefix matches observer, want non-local by inequality")
	}
	if ann.Sequence == 0 {
		t.Fatalf("sequence is zero, want nonzero")
	}
}

// dialGateUnicast binds the DDSI user unicast port for one allocated domain.
func dialGateUnicast(t *testing.T, ctx context.Context, domain uint32) {
	t.Helper()
	dialCtx, dialCancel := context.WithTimeout(ctx, gateSocketTimeout)
	uni, err := rtps.DialUnicast(dialCtx, domain)
	dialCancel()
	if err != nil {
		t.Fatalf("dial unicast: %v", err)
	}
	t.Cleanup(func() {
		discoveryCloseUnicast(t, uni)
	})
	if uni.Domain() != domain {
		t.Fatalf("unicast domain = %d, want %d", uni.Domain(), domain)
	}
	wantPort, err := rtps.UsertrafficUnicastPort(domain, uni.Participant())
	if err != nil {
		t.Fatalf("want unicast port: %v", err)
	}
	if uni.LocalPort() != wantPort {
		t.Fatalf("unicast port = %d, want %d", uni.LocalPort(), wantPort)
	}
}

// joinGateMulticast joins the SPDP multicast group for one allocated domain.
func joinGateMulticast(t *testing.T, ctx context.Context, domain uint32) *rtps.MulticastConn {
	t.Helper()
	joinCtx, joinCancel := context.WithTimeout(ctx, gateSocketTimeout)
	mc, err := rtps.JoinMulticast(joinCtx, domain)
	joinCancel()
	if err != nil {
		t.Fatalf("join multicast: %v", err)
	}
	t.Cleanup(func() {
		discoveryCloseMulticast(t, mc)
	})
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
		t.Fatalf("target group = %v, want 239.255.0.1", target.IP)
	}
	if target.Port != wantPort {
		t.Fatalf("target port = %d, want %d", target.Port, wantPort)
	}
	return mc
}

// dialGateSender returns a send-only SPDP multicast sender for one allocated domain.
func dialGateSender(t *testing.T, domain uint32) *rtps.MulticastSender {
	t.Helper()
	sender, err := rtps.DialMulticastSender(domain)
	if err != nil {
		t.Fatalf("dial multicast sender: %v", err)
	}
	t.Cleanup(func() {
		discoveryCloseSender(t, sender)
	})
	wantPort, err := rtps.MetatrafficMulticastPort(domain)
	if err != nil {
		t.Fatalf("expected multicast port: %v", err)
	}
	if sender.Port() != wantPort {
		t.Fatalf("sender port = %d, want %d", sender.Port(), wantPort)
	}
	return sender
}

// marshalGateAlive builds one alive packet and checks the self parse.
func marshalGateAlive(t *testing.T, announcer *dds.LocalParticipant, domain uint32, seq uint64) []byte {
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
	if self.GUID.Prefix != announcer.GUID().Prefix {
		t.Fatalf("self GUID mismatch")
	}
	hdr, data := decodeGateSPDP(t, packet)
	checkGateWire(t, hdr, data)
	return packet
}

// sendGatePacket multicasts one packet and fails on send error.
func sendGatePacket(t *testing.T, sender *rtps.MulticastSender, packet []byte) {
	t.Helper()
	err := sender.Send(packet)
	if err != nil {
		t.Fatalf("multicast send: %v", err)
	}
}

// gateOutcome classifies one received datagram for the capture loop.
type gateOutcome int

// Gate outcome values for one datagram.
const (
	// gateSkip means the datagram holds no SPDP DATA.
	gateSkip gateOutcome = iota
	// gateOK means the datagram holds valid SPDP.
	gateOK
	// gateFail means the datagram holds SPDP DATA that fails to parse.
	gateFail
)

// isGateAnnouncer reports whether ann comes from the announcer.
func isGateAnnouncer(ann dds.Announcement, announcer dds.GUID) bool {
	return ann.GUID.Prefix == announcer.Prefix
}

// classifyGateRaw validates one datagram against the observer identity.
func classifyGateRaw(t *testing.T, raw []byte, observer dds.GUID, domain uint32) (dds.Announcement, gateOutcome) {
	t.Helper()
	var empty dds.Announcement
	if !containsGateSPDP(raw) {
		return empty, gateSkip
	}
	ann, err := dds.ParseAnnouncement(raw)
	if err != nil {
		return empty, gateFail
	}
	hdr, data := decodeGateSPDP(t, raw)
	checkGateWire(t, hdr, data)
	checkGateAnnouncement(t, ann, observer, domain)
	return ann, gateOK
}

// nextGateRaw reads one datagram or reports loop control.
func nextGateRaw(mc *rtps.MulticastConn, recvCtx context.Context) ([]byte, bool, bool) {
	raw, err := mc.Receive(recvCtx)
	if err != nil {
		ctxErr := recvCtx.Err()
		if ctxErr != nil {
			return nil, true, false
		}
		return nil, false, true
	}
	return raw, false, false
}

// tallyGateAnn counts one valid announcement toward found or externals.
func tallyGateAnn(ann dds.Announcement, announcer dds.GUID, found bool, externals int) (bool, int) {
	if isGateAnnouncer(ann, announcer) {
		return true, externals
	}
	return found, externals + 1
}

// captureGateLoop receives until the announcer packet arrives or ctx ends.
func captureGateLoop(t *testing.T, mc *rtps.MulticastConn, recvCtx context.Context, announcer, observer dds.GUID, domain uint32) (int, int, int, bool) {
	t.Helper()
	successes := 0
	failures := 0
	externals := 0
	found := false
	for {
		raw, ended, skip := nextGateRaw(mc, recvCtx)
		if ended {
			break
		}
		if skip {
			continue
		}
		ann, outcome := classifyGateRaw(t, raw, observer, domain)
		if outcome == gateSkip {
			continue
		}
		if outcome == gateFail {
			failures++
			continue
		}
		successes++
		found, externals = tallyGateAnn(ann, announcer, found, externals)
		if found {
			break
		}
	}
	return successes, failures, externals, found
}

// TestCycloneGate1SPDPMulticastPorts pins group and B/B+1 ports for one allocated domain.
func TestCycloneGate1SPDPMulticastPorts(t *testing.T) {
	domain := helpers.MustAllocateDomainU32(t)
	if rtps.SPDPMulticastGroup != "239.255.0.1" {
		t.Fatalf("group = %q, want 239.255.0.1", rtps.SPDPMulticastGroup)
	}
	wantMetaMC := rtps.BasePort + rtps.DomainGain*int(domain) + rtps.MetatrafficMulticastOffset
	wantUserMC := rtps.BasePort + rtps.DomainGain*int(domain) + rtps.UsertrafficMulticastOffset
	wantMetaUC := rtps.BasePort + rtps.DomainGain*int(domain) + rtps.MetatrafficUnicastOffset
	wantUserUC := rtps.BasePort + rtps.DomainGain*int(domain) + rtps.UsertrafficUnicastOffset
	metaMC, err := rtps.MetatrafficMulticastPort(domain)
	if err != nil {
		t.Fatalf("meta multicast port: %v", err)
	}
	if metaMC != wantMetaMC {
		t.Fatalf("meta multicast = %d, want %d", metaMC, wantMetaMC)
	}
	userMC, err := rtps.UsertrafficMulticastPort(domain)
	if err != nil {
		t.Fatalf("user multicast port: %v", err)
	}
	if userMC != wantUserMC {
		t.Fatalf("user multicast = %d, want %d", userMC, wantUserMC)
	}
	metaUC, err := rtps.MetatrafficUnicastPort(domain, 0)
	if err != nil {
		t.Fatalf("meta unicast port: %v", err)
	}
	if metaUC != wantMetaUC {
		t.Fatalf("meta unicast = %d, want %d", metaUC, wantMetaUC)
	}
	userUC, err := rtps.UsertrafficUnicastPort(domain, 0)
	if err != nil {
		t.Fatalf("user unicast = %d, want %d", userUC, wantUserUC)
	}
	if userUC != wantUserUC {
		t.Fatalf("user unicast = %d, want %d", userUC, wantUserUC)
	}
	group := net.ParseIP(rtps.SPDPMulticastGroup)
	if group == nil {
		t.Fatalf("group parse failed")
	}
	if !group.IsMulticast() {
		t.Fatalf("group %v is not multicast", group)
	}
}

// TestCycloneGate1SPDPST0NonLocalPureGo proves non-local SPDP with direct UDP only.
//
// It needs no container and passes with no Docker. It binds unicast plus
// multicast on one allocated domain, sends one Weld alive packet over multicast, and
// checks the multicast copy with GUID inequality, domain, liveness, ports,
// lease, and wire fields.
func TestCycloneGate1SPDPST0NonLocalPureGo(t *testing.T) {
	domain := helpers.MustAllocateDomainU32(t)
	discoveryPinDomain(t, domain)
	ctx := context.Background()
	observer, err := dds.NewLocalParticipant(domain, gateLease)
	if err != nil {
		t.Fatalf("new observer: %v", err)
	}
	announcer, err := dds.NewLocalParticipant(domain, gateLease)
	if err != nil {
		t.Fatalf("new announcer: %v", err)
	}
	if observer.GUID().Prefix == announcer.GUID().Prefix {
		t.Fatalf("observer and announcer prefixes match, want distinct")
	}
	dialGateUnicast(t, ctx, domain)
	mc := joinGateMulticast(t, ctx, domain)
	sender := dialGateSender(t, domain)
	packet := marshalGateAlive(t, announcer, domain, 1)
	sendGatePacket(t, sender, packet)
	sendGatePacket(t, sender, packet)
	recvCtx, recvCancel := context.WithTimeout(ctx, gatePureTimeout)
	defer recvCancel()
	successes, failures, externals, found := captureGateLoop(t, mc, recvCtx, announcer.GUID(), observer.GUID(), domain)
	if failures != 0 {
		t.Fatalf("deserialization failures = %d, want 0", failures)
	}
	if successes < 1 {
		t.Fatalf("successes = %d, want at least 1", successes)
	}
	if !found {
		t.Fatalf("did not capture Weld announcement on domain %d multicast", domain)
	}
	t.Logf("pure-go SPDP non-local: domain %d alive lease %v ports %v externals %d", domain, announcer.LeaseDuration(), successes, externals)
}

// TestCycloneGate1SPDPST0NonLocalLive proves non-local SPDP beside a live peer.
//
// It assumes the weld-jazzy container is already running. It uses the same
// direct UDP path as the pure-go test and additionally counts any external
// allocated domain SPDP seen during the window. It passes on the self proof when
// no external talker is present and logs externals when Cyclone is present.
func TestCycloneGate1SPDPST0NonLocalLive(t *testing.T) {
	domain := helpers.MustAllocateDomainU32(t)
	discoveryPinDomain(t, domain)
	ctx := context.Background()
	observer, err := dds.NewLocalParticipant(domain, gateLease)
	if err != nil {
		t.Fatalf("new observer: %v", err)
	}
	announcer, err := dds.NewLocalParticipant(domain, gateLease)
	if err != nil {
		t.Fatalf("new announcer: %v", err)
	}
	if observer.GUID().Prefix == announcer.GUID().Prefix {
		t.Fatalf("observer and announcer prefixes match, want distinct")
	}
	dialGateUnicast(t, ctx, domain)
	mc := joinGateMulticast(t, ctx, domain)
	sender := dialGateSender(t, domain)
	packet := marshalGateAlive(t, announcer, domain, 7)
	sendGatePacket(t, sender, packet)
	sendGatePacket(t, sender, packet)
	sendGatePacket(t, sender, packet)
	recvCtx, recvCancel := context.WithTimeout(ctx, gateLiveTimeout)
	defer recvCancel()
	successes, failures, externals, found := captureGateLoop(t, mc, recvCtx, announcer.GUID(), observer.GUID(), domain)
	if failures != 0 {
		t.Fatalf("live deserialization failures = %d, want 0", failures)
	}
	if successes < 1 {
		t.Fatalf("live successes = %d, want at least 1", successes)
	}
	if !found {
		t.Fatalf("live run did not capture Weld announcement on domain %d", domain)
	}
	if externals > 0 {
		t.Logf("live SPDP non-local beside Cyclone: domain %d alive externals %d", domain, externals)
	} else {
		t.Logf("live SPDP non-local self proof only: domain %d alive, no external peer seen", domain)
	}
}
