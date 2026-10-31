// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

//nolint:cyclop // test breadth
package cyclone

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/podomy/weld/internal/transports/dds"
	"github.com/podomy/weld/internal/transports/dds/rtps"
	"github.com/podomy/weld/testbed/live/helpers"
)

// TestCycloneSPDPAllocatedDecodesNonLocal checks SPDP from the running peer.
//
// It joins one allocated exclusive domain SPDP multicast group, starts a Cyclone talker
// detached via docker exec -d writing to /testbed/logs/<run-id>.out in the
// already running peer, and requires one non-local SPDP entry decoded with
// dds.ParseAnnouncement. GUID prefix inequality proves the peer is
// non-local. The talker uses a per-run topic and payload ID so stale runs
// never mix. Cleanup kills the pidfile PID plus one scoped pkill for the
// run ID. It skips cleanly when no daemon or peer container is present and
// never starts the compose stack. It uses an allocated exclusive domain
// with explicit domain plumbing and no process env, so it is safe for
// future t.Parallel workers.
//
//nolint:gocognit,cyclop // live SPDP test sequences many assertions, splitting would obscure intent
func TestCycloneSPDPDomain48DecodesNonLocal(t *testing.T) {
	domain := helpers.MustAllocateDomainU32(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)
	local, err := dds.NewLocalParticipant(domain, 10*time.Second)
	if err != nil {
		t.Fatalf("new local participant: %v", err)
	}
	localPrefix := local.GUID().Prefix
	joinCtx, joinCancel := context.WithTimeout(ctx, gateSocketTimeout)
	mc, err := rtps.JoinMulticast(joinCtx, domain)
	joinCancel()
	if err != nil {
		t.Fatalf("join multicast: %v", err)
	}
	t.Cleanup(func() {
		cerr := mc.Close()
		if cerr != nil {
			t.Logf("multicast close: %v", cerr)
		}
	})
	if mc.Domain() != domain {
		t.Fatalf("multicast domain = %d want %d", mc.Domain(), domain)
	}
	wantPort, err := rtps.MetatrafficMulticastPort(domain)
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
	runID := helpers.LiveID("cyclone-live")
	topic := helpers.LiveTopic(runID)
	pubArg := fmt.Sprintf("{data: weld-live %s}", runID)
	pidPath := helpers.HostPidPath(t, runID)
	err = helpers.StartPeerProc(ctx, helpers.Container, int(domain), runID, helpers.RosBin, "topic", "pub", topic, helpers.RosType, pubArg, "--rate", "2")
	if err != nil {
		t.Fatalf("start talker: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		helpers.StopPeerProcAndWait(cleanupCtx, helpers.Container, int(domain), pidPath, runID)
	})
	found := false
	successes := 0
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
		if !containsGateSPDP(raw) {
			continue
		}
		ann, perr := dds.ParseAnnouncement(raw)
		if perr != nil {
			continue
		}
		if ann.DomainID != domain {
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
		hdr, data := decodeGateSPDP(t, raw)
		checkGateWire(t, hdr, data)
		if ann.Sequence == 0 {
			t.Fatalf("live sequence is zero")
		}
		if ann.Lease <= 0 {
			t.Fatalf("live lease = %v want positive", ann.Lease)
		}
		successes++
		found = true
		t.Logf("live SPDP non-local prefix %x seq %d lease %v ports %v id %s", ann.GUID.Prefix, ann.Sequence, ann.Lease, ann.Ports, runID)
		break
	}
	if !found {
		t.Fatalf("no non-local SPDP on domain %d, successes = %d", domain, successes)
	}
	if successes < 1 {
		t.Fatalf("successes = %d want at least 1", successes)
	}
}
