// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package gates_test

import (
	"context"
	"testing"
	"time"

	"github.com/podomy/weld/internal/transports/dds"
	"github.com/podomy/weld/internal/transports/dds/rtps"
	"github.com/podomy/weld/testbed/live/helpers"
)

// TestGate1SPDPAcceptance is the SPDP acceptance gate.
//
// It joins one allocated exclusive domain SPDP multicast group, starts a
// Cyclone talker detached via docker exec -d writing to
// /testbed/logs/<run-id>.out in the already running peer, and requires
// one non-local SPDP entry decoded with dds.ParseAnnouncement. GUID
// prefix inequality proves the peer accepted our participant over
// unicast. The talker uses a per-run topic and payload ID so concurrent
// or stale runs never mix. Cleanup kills the pidfile PID plus one scoped
// pkill for the run ID. It skips cleanly when no daemon or peer is
// present and never starts the compose stack. It is gate 1 of the Go
// gates with direct RTPS and no trace files. It uses an allocated
// exclusive domain with explicit domain plumbing and no process env, so
// it is safe for future t.Parallel workers.
//
//nolint:gocognit,cyclop // test sequences many assertions, splitting would obscure intent
func TestGate1SPDPAcceptance(t *testing.T) {
	domain := helpers.MustAllocateDomain(t)
	ctx, cancel := context.WithTimeout(context.Background(), helpers.LiveTimeout)
	defer cancel()
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)
	err := helpers.ValidateDomain(domain)
	if err != nil {
		t.Fatalf("allocated domain invalid: %v", err)
	}
	local, err := dds.NewLocalParticipant(uint32(domain), 10*time.Second) //nolint:gosec // G115: pooled domain 70 through 101 fits uint32
	if err != nil {
		t.Fatalf("new local participant: %v", err)
	}
	localPrefix := local.GUID().Prefix
	joinCtx, joinCancel := context.WithTimeout(ctx, helpers.DockerProbeTimeout)
	mc, err := rtps.JoinMulticast(joinCtx, uint32(domain)) //nolint:gosec // G115: pooled domain 70 through 101 fits uint32
	joinCancel()
	if err != nil {
		t.Fatalf("join multicast: %v", err)
	}
	defer func() {
		cerr := mc.Close()
		if cerr != nil {
			t.Logf("multicast close: %v", cerr)
		}
	}()
	if mc.Domain() != uint32(domain) { //nolint:gosec // G115: pooled domain 70 through 101 fits uint32
		t.Fatalf("multicast domain = %d want %d", mc.Domain(), domain)
	}
	wantPort, err := rtps.MetatrafficMulticastPort(uint32(domain)) //nolint:gosec // G115: pooled domain 70 through 101 fits uint32
	if err != nil {
		t.Fatalf("expected multicast port: %v", err)
	}
	if mc.Port() != wantPort {
		t.Fatalf("multicast port = %d want %d", mc.Port(), wantPort)
	}
	target := mc.Target()
	if target == nil {
		t.Fatalf("nil multicast target")
	}
	if target.IP.String() != rtps.SPDPMulticastGroup {
		t.Fatalf("target group = %s want %s", target.IP.String(), rtps.SPDPMulticastGroup)
	}
	payloadID := helpers.LiveID("live")
	topic := helpers.LiveTopic(payloadID)
	pubArg := "{data: hello gate1 " + payloadID + "}"
	pidPath := helpers.HostPidPath(t, payloadID)
	err = helpers.StartPeerProc(ctx, helpers.Container, domain, payloadID, helpers.RosBin, "topic", "pub", topic, helpers.RosType, pubArg, "--rate", "2")
	if err != nil {
		t.Fatalf("start talker: %v", err)
	}
	t.Cleanup(func() {
		helpers.StopPeerProcAndWait(context.WithoutCancel(ctx), helpers.Container, domain, pidPath, payloadID)
	})
	helpers.WaitForPeerReady(ctx, pidPath, helpers.TalkerSettle)
	if ctx.Err() != nil {
		t.Fatalf("context ended before capture: %v", ctx.Err())
	}
	found := false
	successes := 0
	for ctx.Err() == nil {
		recvCtx, recvCancel := context.WithTimeout(ctx, helpers.RecvTimeout)
		raw, rerr := mc.Receive(recvCtx)
		recvCancel()
		if rerr != nil {
			if ctx.Err() != nil {
				break
			}
			continue
		}
		if !helpers.ContainsSPDP(raw) {
			continue
		}
		ann, perr := dds.ParseAnnouncement(raw)
		if perr != nil {
			continue
		}
		if ann.DomainID != uint32(domain) { //nolint:gosec // G115: pooled domain 70 through 101 fits uint32
			continue
		}
		if !ann.Alive {
			continue
		}
		if len(ann.Ports) == 0 {
			t.Fatalf("live ports empty")
		}
		if ann.GUID.Prefix == localPrefix {
			continue
		}
		helpers.CheckWire(t, raw)
		if ann.Sequence == 0 {
			t.Fatalf("live sequence is zero")
		}
		if ann.Lease <= 0 {
			t.Fatalf("live lease = %v want positive", ann.Lease)
		}
		successes++
		found = true
		t.Logf("live SPDP non-local prefix %x seq %d lease %v ports %v id %s", ann.GUID.Prefix, ann.Sequence, ann.Lease, ann.Ports, payloadID)
		break
	}
	if !found {
		t.Fatalf("no non-local SPDP on domain %d, successes = %d", domain, successes)
	}
	if successes < 1 {
		t.Fatalf("successes = %d want at least 1", successes)
	}
}
