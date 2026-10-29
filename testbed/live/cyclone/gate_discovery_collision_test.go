// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package cyclone

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/podomy/weld/internal/transports"
	"github.com/podomy/weld/internal/transports/dds"
	"github.com/podomy/weld/internal/transports/dds/rtps"
	"github.com/podomy/weld/testbed/live/helpers"
)

// TestCycloneDiscoveryCollisionLive proves two Weld backends share one domain safely.
//
// Two direct RTPS unicast dials on one allocated domain must land on successive
// participant indices with distinct ports and no EADDRINUSE. Two Weld
// backends joined on the same domain must also join without address reuse
// and relay one String sample. Prefix inequality on a separate observer
// pair proves per-run non-local identities. All calls use context timeouts
// and the compose stack is never started.
// collisionDomain is fixed instead of pool allocated. Successive slot
// probing needs a domain no other run ever joins, and pool domains can
// carry residue from killed runs that no test can release. Packages run
// serially, so one fixed domain outside the live pool stays exclusive.
const collisionDomain uint32 = 58

func TestCycloneDiscoveryCollisionLive(t *testing.T) {
	domain := collisionDomain
	ctx, cancel := discoverySetup(t, domain)
	defer cancel()
	observer, announcer := newLiveParticipants(t, domain, discoveryLease)
	checkCollisionPacket(t, announcer, observer, domain)
	first, second := checkSuccessiveUnicast(t, ctx, domain)
	checkUnicastAddrs(t, first)
	closeUnicastPair(t, first, second)
	runID := helpers.LiveID("collision")
	firstBackend, secondBackend := checkCollisionBackends(t, ctx, runID, domain)
	payload := []byte("hello collision " + runID)
	checkCollisionPublish(t, ctx, firstBackend, payload, domain)
	checkCollisionTake(t, ctx, secondBackend, payload, domain)
}

// checkCollisionPacket pins alive packet fields for collision domain.
// checkCollisionPacket pins alive packet fields for one allocated domain.
func checkCollisionPacket(t *testing.T, announcer, observer *dds.LocalParticipant, domain uint32) {
	t.Helper()
	packet, err := dds.MarshalAlivePacket(announcer.GUID(), domain, 1, announcer.LeaseDuration())
	if err != nil {
		t.Fatalf("marshal alive: %v", err)
	}
	parsed, err := dds.ParseAnnouncement(packet)
	if err != nil {
		t.Fatalf("parse alive: %v", err)
	}
	if parsed.DomainID != domain {
		t.Fatalf("parsed domain = %d, want %d", parsed.DomainID, domain)
	}
	if !parsed.Alive {
		t.Fatalf("parsed as dispose")
	}
	if parsed.GUID.Prefix == observer.GUID().Prefix {
		t.Fatalf("parsed prefix matches observer, want non-local")
	}
	if parsed.GUID.Prefix != announcer.GUID().Prefix {
		t.Fatalf("parsed GUID mismatch")
	}
}

// checkSuccessiveUnicast dials two unicasts and pins successive indices.
// checkSuccessiveUnicast dials two unicasts and pins successive indices on one allocated domain.
func checkSuccessiveUnicast(t *testing.T, ctx context.Context, domain uint32) (*rtps.UnicastConn, *rtps.UnicastConn) {
	t.Helper()
	dialCtx, dialCancel := context.WithTimeout(ctx, discoveryDialTimeout)
	first, err := rtps.DialUnicast(dialCtx, domain)
	dialCancel()
	if err != nil {
		t.Fatalf("dial first unicast: %v", err)
	}
	t.Cleanup(func() {
		discoveryCloseUnicast(t, first)
	})
	dialCtx2, dialCancel2 := context.WithTimeout(ctx, discoveryDialTimeout)
	second, err := rtps.DialUnicast(dialCtx2, domain)
	dialCancel2()
	if err != nil {
		t.Fatalf("dial second unicast: %v", err)
	}
	t.Cleanup(func() {
		discoveryCloseUnicast(t, second)
	})
	if first.Participant()+1 != second.Participant() {
		t.Fatalf("participants = %d %d, want successive", first.Participant(), second.Participant())
	}
	if first.LocalPort() == second.LocalPort() {
		t.Fatalf("unicast ports collide on %d", first.LocalPort())
	}
	checkUnicastPorts(t, first, second, domain)
	return first, second
}

// checkUnicastPorts pins expected user ports for both unicasts.
// checkUnicastPorts pins expected user ports for both unicasts on one allocated domain.
func checkUnicastPorts(t *testing.T, first, second *rtps.UnicastConn, domain uint32) {
	t.Helper()
	wantFirst, err := rtps.UsertrafficUnicastPort(domain, first.Participant())
	if err != nil {
		t.Fatalf("want first port: %v", err)
	}
	if first.LocalPort() != wantFirst {
		t.Fatalf("first port = %d, want %d", first.LocalPort(), wantFirst)
	}
	wantSecond, err := rtps.UsertrafficUnicastPort(domain, second.Participant())
	if err != nil {
		t.Fatalf("want second port: %v", err)
	}
	if second.LocalPort() != wantSecond {
		t.Fatalf("second port = %d, want %d", second.LocalPort(), wantSecond)
	}
}

// checkUnicastAddrs pins advertised IPv4 for first unicast.
func checkUnicastAddrs(t *testing.T, first *rtps.UnicastConn) {
	t.Helper()
	advertised := rtps.AdvertiseIPv4()
	addr := first.LocalAddr()
	if addr == nil {
		t.Fatalf("nil first local addr")
	}
	if !addr.IP.Equal(advertised) {
		t.Fatalf("first ip = %s, want advertised %s", addr.IP.String(), advertised.String())
	}
}

// closeUnicastPair closes both unicasts early.
func closeUnicastPair(t *testing.T, first, second *rtps.UnicastConn) {
	t.Helper()
	if err := first.Close(); err != nil {
		t.Fatalf("close first early: %v", err)
	}
	if err := second.Close(); err != nil {
		t.Fatalf("close second early: %v", err)
	}
}

// checkCollisionBackends joins two backends on collision domain.
// checkCollisionBackends joins two backends on one allocated domain.
func checkCollisionBackends(t *testing.T, ctx context.Context, runID string, domain uint32) (*dds.Backend, *dds.Backend) {
	t.Helper()
	firstBackend, err := dds.NewBackend(dds.SidecarConfig{
		DomainID:    domain,
		PeerAddress: dds.DefaultPeerAddress,
		Range:       transports.DiscoveryRangeSystemDefault,
	})
	if err != nil {
		t.Fatalf("new first backend: %v", err)
	}
	t.Cleanup(func() { //nolint:contextcheck // backend Close is cleanup without caller ctx, goodbye uses bounded Background
		cerr := firstBackend.Close()
		if cerr != nil {
			t.Logf("close first backend: %v", cerr)
		}
	})
	if err := firstBackend.JoinDiscovery(ctx, transports.DiscoveryInfo{ParticipantName: "collision-a-" + runID}); err != nil {
		t.Fatalf("join first backend: %v", err)
	}
	if !firstBackend.Joined() {
		t.Fatalf("first backend not joined")
	}
	secondBackend, err := dds.NewBackend(dds.SidecarConfig{
		DomainID:    domain,
		PeerAddress: dds.DefaultPeerAddress,
		Range:       transports.DiscoveryRangeSystemDefault,
	})
	if err != nil {
		t.Fatalf("new second backend: %v", err)
	}
	t.Cleanup(func() { //nolint:contextcheck // backend Close is cleanup without caller ctx, goodbye uses bounded Background
		cerr := secondBackend.Close()
		if cerr != nil {
			t.Logf("close second backend: %v", cerr)
		}
	})
	if err := secondBackend.JoinDiscovery(ctx, transports.DiscoveryInfo{ParticipantName: "collision-b-" + runID}); err != nil {
		t.Fatalf("join second backend: %v", err)
	}
	if !secondBackend.Joined() {
		t.Fatalf("second backend not joined")
	}
	if firstBackend.DomainID() != domain {
		t.Fatalf("first backend domain = %d, want %d", firstBackend.DomainID(), domain)
	}
	if secondBackend.DomainID() != domain {
		t.Fatalf("second backend domain = %d, want %d", secondBackend.DomainID(), domain)
	}
	return firstBackend, secondBackend
}

// checkCollisionPublish publishes until route appears.
// checkCollisionPublish publishes until route appears on one allocated domain.
func checkCollisionPublish(t *testing.T, ctx context.Context, firstBackend *dds.Backend, payload []byte, domain uint32) {
	t.Helper()
	published := false
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		pubCtx, pubCancel := context.WithTimeout(ctx, 500*time.Millisecond)
		err := firstBackend.PublishBytes(pubCtx, transports.OutgoingMessage{
			TopicName: "/chatter",
			TypeName:  "std_msgs__msg__String",
			Payload:   payload,
		})
		pubCancel()
		if err == nil {
			published = true
			break
		}
		if !errors.Is(err, dds.ErrNoRoute) {
			t.Fatalf("publish: %v", err)
		}
		if ctx.Err() != nil {
			break
		}
	}
	if !published {
		t.Fatalf("backends on domain %d did not gain a route", domain)
	}
}

// checkCollisionTake takes one sample and pins topic and payload.
// checkCollisionTake takes one sample and pins topic and payload on one allocated domain.
func checkCollisionTake(t *testing.T, ctx context.Context, secondBackend *dds.Backend, payload []byte, domain uint32) {
	t.Helper()
	takeCtx, takeCancel := context.WithTimeout(ctx, 5*time.Second)
	defer takeCancel()
	msg, err := secondBackend.TakeBytes(takeCtx)
	if err != nil {
		t.Fatalf("take: %v", err)
	}
	if msg.TopicName != "/chatter" {
		t.Fatalf("topic = %q, want /chatter", msg.TopicName)
	}
	if string(msg.Payload) != string(payload) {
		t.Fatalf("payload mismatch: got %q", string(msg.Payload))
	}
	t.Logf("collision backends relayed %d bytes on domain %d", len(msg.Payload), domain)
}
