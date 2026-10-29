// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package cyclone

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/podomy/weld/testbed/live/helpers"

	"github.com/podomy/weld/internal/transports/dds"
	"github.com/podomy/weld/internal/transports/dds/rtps"
)

// TestCycloneDiscoveryDomainsLive proves domain 0 and 101 work and 102 fails fast.
//
// It pins the B and B+1 multicast ports plus unicast ports for domains 0
// and 101 with direct RTPS, requires NewLocalParticipant to accept both,
// and requires multicast port mapping plus JoinMulticast plus DialUnicast
// plus MarshalAlivePacket for domain 102 to fail before any socket blocks.
// A live round trip on one allocated domain with prefix inequality proves the
// valid path still discovers. All calls use context timeouts and the
// compose stack is never started.
func TestCycloneDiscoveryDomainsLive(t *testing.T) {
	domain := helpers.MustAllocateDomainU32(t)
	ctx, cancel := context.WithTimeout(context.Background(), discoveryOverall)
	defer cancel()
	discoveryDockerCrossCheck(t, ctx)
	checkDomainPorts(t, 0, 7400, 7401, 7410, 7411)
	checkDomainPorts(t, 101, 32650, 32651, 32660, 32661)
	obs0 := checkValidParticipants(t)
	checkDomain102PortsFail(t)
	checkDomain102SocketsFail(t, ctx, obs0)
	receiveRangeLive(t, ctx, domain)
	t.Logf("domains 0 and 101 valid, 102 rejected fast, live %d discovered", domain)
}

// checkDomainPorts pins multicast and unicast ports for one domain.
func checkDomainPorts(t *testing.T, domain uint32, metaMC, userMC, metaUC, userUC int) {
	t.Helper()
	meta, err := rtps.MetatrafficMulticastPort(domain)
	if err != nil {
		t.Fatalf("meta multicast %d: %v", domain, err)
	}
	if meta != metaMC {
		t.Fatalf("meta multicast %d = %d, want %d", domain, meta, metaMC)
	}
	user, err := rtps.UsertrafficMulticastPort(domain)
	if err != nil {
		t.Fatalf("user multicast %d: %v", domain, err)
	}
	if user != userMC {
		t.Fatalf("user multicast %d = %d, want %d", domain, user, userMC)
	}
	metaUni, err := rtps.MetatrafficUnicastPort(domain, 0)
	if err != nil {
		t.Fatalf("meta unicast %d: %v", domain, err)
	}
	if metaUni != metaUC {
		t.Fatalf("meta unicast %d = %d, want %d", domain, metaUni, metaUC)
	}
	userUni, err := rtps.UsertrafficUnicastPort(domain, 0)
	if err != nil {
		t.Fatalf("user unicast %d: %v", domain, err)
	}
	if userUni != userUC {
		t.Fatalf("user unicast %d = %d, want %d", domain, userUni, userUC)
	}
}

// checkValidParticipants proves domains 0 and 101 accept participants.
func checkValidParticipants(t *testing.T) dds.GUID {
	t.Helper()
	obs0, err := dds.NewLocalParticipant(0, discoveryLease)
	if err != nil {
		t.Fatalf("new participant 0: %v", err)
	}
	if obs0.DomainID() != 0 {
		t.Fatalf("participant 0 domain = %d, want 0", obs0.DomainID())
	}
	obs101, err := dds.NewLocalParticipant(101, discoveryLease)
	if err != nil {
		t.Fatalf("new participant 101: %v", err)
	}
	if obs101.DomainID() != 101 {
		t.Fatalf("participant 101 domain = %d, want 101", obs101.DomainID())
	}
	if obs0.GUID().Prefix == obs101.GUID().Prefix {
		t.Fatalf("domain 0 and 101 prefixes match, want distinct")
	}
	return obs0.GUID()
}

// checkDomain102PortsFail proves port mapping rejects 102.
func checkDomain102PortsFail(t *testing.T) {
	t.Helper()
	if _, err := rtps.MetatrafficMulticastPort(102); err == nil {
		t.Fatalf("meta multicast 102 succeeded, want error")
	}
	if _, err := rtps.UsertrafficMulticastPort(102); err == nil {
		t.Fatalf("user multicast 102 succeeded, want error")
	}
	if _, err := rtps.MetatrafficUnicastPort(102, 0); err == nil {
		t.Fatalf("meta unicast 102 succeeded, want error")
	}
	if _, err := rtps.UsertrafficUnicastPort(102, 0); err == nil {
		t.Fatalf("user unicast 102 succeeded, want error")
	}
}

// checkDomain102SocketsFail proves sockets and marshal reject 102 fast.
func checkDomain102SocketsFail(t *testing.T, ctx context.Context, goodGUID dds.GUID) {
	t.Helper()
	failCtx, failCancel := context.WithTimeout(ctx, discoveryDialTimeout)
	_, err := rtps.JoinMulticast(failCtx, 102)
	joinCtxErr := failCtx.Err()
	failCancel()
	if err == nil {
		t.Fatalf("join multicast 102 succeeded, want error")
	}
	if joinCtxErr != nil {
		t.Fatalf("join 102 did not fail fast, context ended first")
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		t.Fatalf("join 102 returned context error, want domain error: %v", err)
	}
	failCtx2, failCancel2 := context.WithTimeout(ctx, discoveryDialTimeout)
	_, err = rtps.DialUnicast(failCtx2, 102)
	dialCtxErr := failCtx2.Err()
	failCancel2()
	if err == nil {
		t.Fatalf("dial unicast 102 succeeded, want error")
	}
	if dialCtxErr != nil {
		t.Fatalf("dial 102 did not fail fast, context ended first")
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		t.Fatalf("dial 102 returned context error, want domain error: %v", err)
	}
	checkDomain102Canceled(t, ctx)
	if _, err := dds.NewLocalParticipant(102, discoveryLease); err == nil {
		t.Fatalf("new participant 102 succeeded, want error")
	}
	if _, err := dds.MarshalAlivePacket(goodGUID, 102, 1, discoveryLease); err == nil {
		t.Fatalf("marshal alive 102 succeeded, want error")
	}
	if _, err := dds.MarshalDisposePacket(goodGUID, 102, 1); err == nil {
		t.Fatalf("marshal dispose 102 succeeded, want error")
	}
}

// checkDomain102Canceled proves a canceled context still reports domain 102
// as a domain error, so domain validation runs before any context wait.
func checkDomain102Canceled(t *testing.T, ctx context.Context) {
	t.Helper()
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	_, err := rtps.JoinMulticast(canceled, 102)
	if err == nil {
		t.Fatalf("join multicast 102 with canceled context succeeded, want error")
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("join 102 with canceled context returned context error, want domain error: %v", err)
	}
	_, err = rtps.DialUnicast(canceled, 102)
	if err == nil {
		t.Fatalf("dial unicast 102 with canceled context succeeded, want error")
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("dial 102 with canceled context returned context error, want domain error: %v", err)
	}
}

// receiveRangeLive proves live round trip on one allocated domain.
// receiveRangeLive proves live round trip on one allocated domain.
func receiveRangeLive(t *testing.T, ctx context.Context, domain uint32) {
	t.Helper()
	discoveryPinDomain(t, domain)
	liveObserver, liveAnnouncer := newLiveParticipants(t, domain, discoveryLease)
	mc, sender := discoveryJoinAndSender(t, ctx, domain)
	packet := marshalLiveAlive(t, liveAnnouncer, domain, 1)
	ann, err := dds.ParseAnnouncement(packet)
	if err != nil {
		t.Fatalf("parse live: %v", err)
	}
	if ann.DomainID != domain {
		t.Fatalf("live domain = %d, want %d", ann.DomainID, domain)
	}
	if ann.GUID.Prefix == liveObserver.GUID().Prefix {
		t.Fatalf("live prefix matches observer")
	}
	err = sender.Send(packet) //nolint:contextcheck // multicast Send has no context param, socket uses timeouts
	if err != nil {
		t.Fatalf("live send: %v", err)
	}
	receiveRangePacket(t, ctx, mc, liveObserver, liveAnnouncer, domain)
}

// isRangePacket reports whether raw is the expected live packet.
// isRangePacket reports whether raw is the expected live packet on one allocated domain.
func isRangePacket(raw []byte, liveObserver, liveAnnouncer *dds.LocalParticipant, domain uint32) bool {
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
	if got.GUID.Prefix != liveAnnouncer.GUID().Prefix {
		return false
	}
	return got.GUID.Prefix != liveObserver.GUID().Prefix
}

// receiveRangePacket receives until live announcer arrives.
// receiveRangePacket receives until live announcer arrives on one allocated domain.
func receiveRangePacket(t *testing.T, ctx context.Context, mc *rtps.MulticastConn, liveObserver, liveAnnouncer *dds.LocalParticipant, domain uint32) {
	t.Helper()
	liveDeadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(liveDeadline) {
		recvCtx, recvCancel := context.WithTimeout(ctx, 3*time.Second)
		raw, rerr := mc.Receive(recvCtx)
		recvCancel()
		if rerr != nil {
			if ctx.Err() != nil {
				break
			}
			continue
		}
		if !isRangePacket(raw, liveObserver, liveAnnouncer, domain) {
			continue
		}
		return
	}
	t.Fatalf("no live SPDP on domain %d", domain)
}
