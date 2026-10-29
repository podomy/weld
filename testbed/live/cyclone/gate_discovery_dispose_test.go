// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package cyclone

import (
	"context"
	"testing"
	"time"

	"github.com/podomy/weld/testbed/live/helpers"

	"github.com/podomy/weld/internal/transports/dds"
)

// TestCycloneDiscoveryDisposeRemoveLive proves dispose removes a remote entry.
//
// It sends one alive packet then one dispose packet on one allocated domain over
// direct RTPS, parses both with dds.ParseAnnouncement, feeds a remote
// table, and requires Len 1 after alive and Len 0 after dispose with a
// generation bump. Prefix inequality proves non-locality. All socket calls
// use context timeouts and the compose stack is never started.
func TestCycloneDiscoveryDisposeRemoveLive(t *testing.T) {
	domain := helpers.MustAllocateDomainU32(t)
	ctx, cancel := discoverySetup(t, domain)
	defer cancel()
	observer, announcer := newLiveParticipants(t, domain, discoveryLease)
	mc, sender := discoveryJoinAndSender(t, ctx, domain)
	alive := checkDisposeAlive(t, announcer, observer, domain)
	bye := checkDisposeBye(t, announcer, observer, domain)
	checkDisposeWire(t, alive)
	sendDisposeSequence(t, sender, alive, bye)
	receiveDisposeSequence(t, ctx, mc, announcer, domain)
	checkDisposeLifecycle(t, alive, bye)
}

// checkDisposeAlive marshals alive and pins fields.
// checkDisposeAlive marshals alive and pins fields on one allocated domain.
func checkDisposeAlive(t *testing.T, announcer, observer *dds.LocalParticipant, domain uint32) []byte {
	t.Helper()
	alive, err := dds.MarshalAlivePacket(announcer.GUID(), domain, 1, announcer.LeaseDuration())
	if err != nil {
		t.Fatalf("marshal alive: %v", err)
	}
	aliveAnn, err := dds.ParseAnnouncement(alive)
	if err != nil {
		t.Fatalf("parse alive: %v", err)
	}
	if !aliveAnn.Alive {
		t.Fatalf("alive parsed as dispose")
	}
	if aliveAnn.Sequence != 1 {
		t.Fatalf("alive seq = %d, want 1", aliveAnn.Sequence)
	}
	if aliveAnn.GUID.Prefix == observer.GUID().Prefix {
		t.Fatalf("alive prefix matches observer")
	}
	return alive
}

// checkDisposeBye marshals dispose and pins fields.
// checkDisposeBye marshals dispose and pins fields on one allocated domain.
func checkDisposeBye(t *testing.T, announcer, observer *dds.LocalParticipant, domain uint32) []byte {
	t.Helper()
	bye, err := dds.MarshalDisposePacket(announcer.GUID(), domain, 2)
	if err != nil {
		t.Fatalf("marshal dispose: %v", err)
	}
	byeAnn, err := dds.ParseAnnouncement(bye)
	if err != nil {
		t.Fatalf("parse dispose: %v", err)
	}
	if byeAnn.Alive {
		t.Fatalf("dispose parsed as alive")
	}
	if byeAnn.Sequence != 2 {
		t.Fatalf("dispose seq = %d, want 2", byeAnn.Sequence)
	}
	if byeAnn.GUID.Prefix != announcer.GUID().Prefix {
		t.Fatalf("dispose GUID mismatch")
	}
	if byeAnn.GUID.Prefix == observer.GUID().Prefix {
		t.Fatalf("dispose prefix matches observer")
	}
	return bye
}

// checkDisposeWire pins wire format for alive packet.
func checkDisposeWire(t *testing.T, alive []byte) {
	t.Helper()
	hdrAlive, dataAlive := decodeGateSPDP(t, alive)
	checkGateWire(t, hdrAlive, dataAlive)
}

// sendDisposeSequence sends alive once and dispose twice.
func sendDisposeSequence(t *testing.T, sender interface{ Send([]byte) error }, alive, bye []byte) {
	t.Helper()
	if err := sender.Send(alive); err != nil {
		t.Fatalf("send alive: %v", err)
	}
	if err := sender.Send(bye); err != nil {
		t.Fatalf("send dispose: %v", err)
	}
	if err := sender.Send(bye); err != nil {
		t.Fatalf("resend dispose: %v", err)
	}
}

// isDisposePacket classifies raw as alive or dispose for announcer.
// isDisposePacket classifies raw as alive or dispose for announcer on one allocated domain.
func isDisposePacket(raw []byte, announcer *dds.LocalParticipant, domain uint32) (bool, bool) {
	if !containsGateSPDP(raw) {
		return false, false
	}
	got, perr := dds.ParseAnnouncement(raw)
	if perr != nil {
		return false, false
	}
	if got.DomainID != domain {
		return false, false
	}
	if got.GUID.Prefix != announcer.GUID().Prefix {
		return false, false
	}
	return true, got.Alive
}

// updateDisposeSeen records one packet into seen flags.
// updateDisposeSeen records one packet into seen flags on one allocated domain.
func updateDisposeSeen(sawAlive, sawBye *bool, raw []byte, announcer *dds.LocalParticipant, domain uint32) bool {
	ok, alive := isDisposePacket(raw, announcer, domain)
	if !ok {
		return false
	}
	if alive {
		*sawAlive = true
	} else {
		*sawBye = true
	}
	return *sawAlive && *sawBye
}

// receiveOneDispose receives one packet or reports context end.
// receiveOneDispose receives one packet or reports context end on one allocated domain.
func receiveOneDispose(ctx context.Context, mc interface {
	Receive(context.Context) ([]byte, error)
}, sawAlive, sawBye *bool, announcer *dds.LocalParticipant, domain uint32,
) (bool, bool) {
	recvCtx, recvCancel := context.WithTimeout(ctx, discoveryRecvTimeout)
	raw, rerr := mc.Receive(recvCtx)
	recvCancel()
	if rerr != nil {
		return ctx.Err() != nil, false
	}
	if updateDisposeSeen(sawAlive, sawBye, raw, announcer, domain) {
		return false, true
	}
	return false, false
}

// receiveDisposeSequence receives until both alive and dispose arrive.
// receiveDisposeSequence receives until both alive and dispose arrive on one allocated domain.
func receiveDisposeSequence(t *testing.T, ctx context.Context, mc interface {
	Receive(context.Context) ([]byte, error)
}, announcer *dds.LocalParticipant, domain uint32,
) {
	t.Helper()
	sawAlive := false
	sawBye := false
	for ctx.Err() == nil {
		ended, done := receiveOneDispose(ctx, mc, &sawAlive, &sawBye, announcer, domain)
		if done {
			return
		}
		if ended {
			break
		}
	}
	if !sawAlive {
		t.Fatalf("no alive SPDP on domain %d", domain)
	}
	if !sawBye {
		t.Fatalf("no dispose SPDP on domain %d", domain)
	}
}

// checkDisposeLifecycle proves table len and generation lifecycle.
func checkDisposeLifecycle(t *testing.T, alive, bye []byte) {
	t.Helper()
	table := dds.NewRemoteTable(nil)
	baseNow := time.Now().UTC()
	if err := table.HandlePacket(alive, baseNow); err != nil {
		t.Fatalf("handle alive: %v", err)
	}
	if table.Len() != 1 {
		t.Fatalf("len = %d, want 1 after alive", table.Len())
	}
	if err := table.HandlePacket(bye, baseNow.Add(time.Second)); err != nil {
		t.Fatalf("handle dispose: %v", err)
	}
	if table.Len() != 0 {
		t.Fatalf("len = %d, want 0 after dispose", table.Len())
	}
	if table.Generation() != 2 {
		t.Fatalf("gen = %d, want 2", table.Generation())
	}
	if err := table.HandlePacket(bye, baseNow.Add(time.Second)); err != nil {
		t.Fatalf("handle missing dispose: %v", err)
	}
	if table.Generation() != 2 {
		t.Fatalf("gen changed on missing dispose")
	}
}
