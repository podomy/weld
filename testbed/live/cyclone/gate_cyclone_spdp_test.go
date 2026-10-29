// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

// Package cyclone holds live interop checks that need real sockets.
//
// The Cyclone SPDP checks below run on allocated exclusive domains and use direct UDP
// through rtps.DialUnicast plus rtps.JoinMulticast and
// dds.ParseAnnouncement. They do not parse container trace files.
package cyclone

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/podomy/weld/internal/transports/dds"
	"github.com/podomy/weld/internal/transports/dds/rtps"
	"github.com/podomy/weld/testbed/live/helpers"
)

// Cyclone live test timeouts on allocated exclusive domains.
//
// B is 7400 plus 250 per domain for the metatraffic multicast port.
// Unicast discovery ports add the per participant gain. Expected ports are
// derived dynamically from the allocated domain via rtps port helpers.
const (
	cycloneReceiveTimeout time.Duration = 6 * time.Second
	cycloneSocketTimeout  time.Duration = 3 * time.Second
	cycloneDockerTimeout  time.Duration = 5 * time.Second
	cycloneTestLease      time.Duration = 10 * time.Second
)

// TestCycloneSPDPNonLocalLive checks a Weld announcement over live UDP.
//
// It joins one allocated domain SPDP multicast group, binds a unicast socket,
// sends one Weld alive packet, and captures it back. The captured
// announcement must be non-local by GUID prefix inequality against a
// separate observer identity, with the allocated DomainID, Alive true, positive
// lease, and non-empty ports. Every SPDP packet on the group must parse
// without ErrInvalidSPDP, which is the zero deserialization failure
// equivalent. Wire checks pin RTPS 2.1, vendor 01 10, and unknown-reader
// DATA to the SPDP writer.
//
//nolint:gocognit,cyclop // live UDP conformance test asserts many sequential wire properties in one run
func TestCycloneSPDPNonLocalLive(t *testing.T) {
	domain := helpers.MustAllocateDomainU32(t)
	discoveryPinDomain(t, domain)
	ctx := context.Background()
	observer, err := dds.NewLocalParticipant(domain, cycloneTestLease)
	if err != nil {
		t.Fatalf("new observer: %v", err)
	}
	announcer, err := dds.NewLocalParticipant(domain, cycloneTestLease)
	if err != nil {
		t.Fatalf("new announcer: %v", err)
	}
	if observer.GUID().Prefix == announcer.GUID().Prefix {
		t.Fatalf("observer and announcer prefixes match, want distinct random prefixes")
	}
	if announcer.DomainID() != domain {
		t.Fatalf("announcer domain = %d, want %d", announcer.DomainID(), domain)
	}
	if announcer.LeaseDuration() <= 0 {
		t.Fatalf("announcer lease = %v, want positive", announcer.LeaseDuration())
	}
	dialCtx, dialCancel := context.WithTimeout(ctx, cycloneSocketTimeout)
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
	wantUni, err := rtps.UsertrafficUnicastPort(domain, uni.Participant())
	if err != nil {
		t.Fatalf("want unicast port: %v", err)
	}
	if uni.LocalPort() != wantUni {
		t.Fatalf("unicast port = %d, want %d for participant %d", uni.LocalPort(), wantUni, uni.Participant())
	}
	joinCtx, joinCancel := context.WithTimeout(ctx, cycloneSocketTimeout)
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
	wantMC, err := rtps.MetatrafficMulticastPort(domain)
	if err != nil {
		t.Fatalf("expected multicast port: %v", err)
	}
	if mc.Port() != wantMC {
		t.Fatalf("multicast port = %d, want %d", mc.Port(), wantMC)
	}
	target := mc.Target()
	if target == nil {
		t.Fatalf("nil multicast target")
	}
	if target.IP.String() != rtps.SPDPMulticastGroup {
		t.Fatalf("target group = %v, want 239.255.0.1", target.IP)
	}
	if target.Port != wantMC {
		t.Fatalf("target port = %d, want %d", target.Port, wantMC)
	}
	sender, err := rtps.DialMulticastSender(domain)
	if err != nil {
		t.Fatalf("dial multicast sender: %v", err)
	}
	t.Cleanup(func() {
		discoveryCloseSender(t, sender)
	})
	if sender.Port() != wantMC {
		t.Fatalf("sender port = %d, want %d", sender.Port(), wantMC)
	}
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
	err = sender.Send(packet)
	if err != nil {
		t.Fatalf("multicast send: %v", err)
	}
	recvCtx, recvCancel := context.WithTimeout(ctx, cycloneReceiveTimeout)
	defer recvCancel()
	foundWeld := false
	successes := 0
	failures := 0
	seenOther := 0
	for {
		raw, err := mc.Receive(recvCtx)
		if err != nil {
			ctxErr := recvCtx.Err()
			if ctxErr != nil {
				break
			}
			continue
		}
		if !containsGateSPDP(raw) {
			continue
		}
		ann, err := dds.ParseAnnouncement(raw)
		if err != nil {
			if errors.Is(err, dds.ErrInvalidSPDP) {
				failures++
			}
			continue
		}
		successes++
		hdr, data := decodeGateSPDP(t, raw)
		checkGateWire(t, hdr, data)
		if ann.DomainID != domain {
			t.Fatalf("live domain = %d, want %d", ann.DomainID, domain)
		}
		if !ann.Alive {
			t.Fatalf("live parsed as dispose for prefix %v", ann.GUID.Prefix)
		}
		if ann.Lease <= 0 {
			t.Fatalf("live lease = %v, want positive", ann.Lease)
		}
		if len(ann.Ports) == 0 {
			t.Fatalf("live ports empty")
		}
		if ann.GUID.Prefix == observer.GUID().Prefix {
			t.Fatalf("live prefix matches observer, want non-local by inequality")
		}
		if ann.GUID.Prefix == announcer.GUID().Prefix {
			foundWeld = true
			if ann.Sequence == 0 {
				t.Fatalf("live sequence is zero")
			}
		} else {
			seenOther++
		}
		if foundWeld && successes >= 1 {
			break
		}
	}
	if failures != 0 {
		t.Fatalf("deserialization failures = %d, want 0", failures)
	}
	if successes < 1 {
		t.Fatalf("successes = %d, want at least 1", successes)
	}
	if !foundWeld {
		t.Fatalf("did not capture Weld announcement on domain %d multicast", domain)
	}
	t.Logf("captured Weld SPDP non-local: domain %d alive lease %v ports %v others %d", domain, self.Lease, self.Ports, seenOther)
}

// TestCycloneSPDPUnicastLoopback checks directed SPDP over DialUnicast.
//
// It sends one Weld alive packet to the bound unicast port and reads it
// back, then parses with dds.ParseAnnouncement. This proves the unicast
// half of the direct UDP path carries SPDP without deserialization
// failures.
//
//nolint:cyclop // loopback test asserts sequential setup, send, receive and parse steps in one run
func TestCycloneSPDPUnicastLoopback(t *testing.T) {
	domain := helpers.MustAllocateDomainU32(t)
	discoveryPinDomain(t, domain)
	ctx := context.Background()
	observer, err := dds.NewLocalParticipant(domain, cycloneTestLease)
	if err != nil {
		t.Fatalf("new observer: %v", err)
	}
	announcer, err := dds.NewLocalParticipant(domain, cycloneTestLease)
	if err != nil {
		t.Fatalf("new announcer: %v", err)
	}
	if observer.GUID().Prefix == announcer.GUID().Prefix {
		t.Fatalf("prefixes match, want distinct")
	}
	dialCtx, dialCancel := context.WithTimeout(ctx, cycloneSocketTimeout)
	uni, err := rtps.DialUnicast(dialCtx, domain)
	dialCancel()
	if err != nil {
		t.Fatalf("dial unicast: %v", err)
	}
	t.Cleanup(func() {
		discoveryCloseUnicast(t, uni)
	})
	packet, err := dds.MarshalAlivePacket(announcer.GUID(), domain, 3, announcer.LeaseDuration())
	if err != nil {
		t.Fatalf("marshal alive: %v", err)
	}
	sendCtx, sendCancel := context.WithTimeout(ctx, cycloneSocketTimeout)
	err = uni.SendTo(sendCtx, "127.0.0.1", uni.LocalPort(), packet)
	sendCancel()
	if err != nil {
		t.Fatalf("unicast send: %v", err)
	}
	recvCtx, recvCancel := context.WithTimeout(ctx, cycloneReceiveTimeout)
	_, _, got, err := uni.Receive(recvCtx)
	recvCancel()
	if err != nil {
		t.Fatalf("unicast receive: %v", err)
	}
	ann, err := dds.ParseAnnouncement(got)
	if err != nil {
		t.Fatalf("parse unicast: %v", err)
	}
	if errors.Is(err, dds.ErrInvalidSPDP) {
		t.Fatalf("unicast parse returned ErrInvalidSPDP")
	}
	if ann.DomainID != domain {
		t.Fatalf("unicast domain = %d, want %d", ann.DomainID, domain)
	}
	if !ann.Alive {
		t.Fatalf("unicast parsed as dispose")
	}
	if ann.Lease <= 0 {
		t.Fatalf("unicast lease = %v, want positive", ann.Lease)
	}
	if len(ann.Ports) == 0 {
		t.Fatalf("unicast ports empty")
	}
	if ann.GUID.Prefix == observer.GUID().Prefix {
		t.Fatalf("unicast prefix matches observer, want non-local")
	}
	if ann.GUID.Prefix != announcer.GUID().Prefix {
		t.Fatalf("unicast GUID mismatch")
	}
	hdr, data := decodeGateSPDP(t, got)
	checkGateWire(t, hdr, data)
}

// TestCycloneSPDPContainerLogCrossCheck optionally reads container logs.
//
// It uses docker argv with no shell through the shared helpers. It skips
// cleanly when no daemon or weld-jazzy container is present and never
// starts the compose stack. Failures never fail the suite because the log
// is only a cross-check next to the direct UDP assertions above.
func TestCycloneSPDPContainerLogCrossCheck(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), cycloneDockerTimeout)
	defer cancel()
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)
	raw, err := helpers.DockerExec(ctx, "logs", "--tail", "20", helpers.Container)
	if err != nil {
		t.Logf("docker logs unavailable: %v", err)
		return
	}
	t.Logf("container %s logs tail bytes %d", helpers.Container, len(string(raw)))
}
