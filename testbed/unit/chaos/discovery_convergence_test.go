// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

//nolint:cyclop // discovery gate holds three convergence scripts, breadth raises the package average
package unit_chaos

import (
	"context"
	"testing"

	"github.com/podomy/weld/internal/chaos"
	"github.com/podomy/weld/internal/transports"
	"github.com/podomy/weld/internal/transports/dds"
)

// discoveryAnnounceNanos is one virtual SEDP announce tick.
const discoveryAnnounceNanos = int64(200_000_000)

// discoveryMaxAnnounces bounds bursty loss convergence to 4 virtual seconds.
const discoveryMaxAnnounces = 20

// discoveryHealBound bounds post heal convergence polls.
const discoveryHealBound = 10

// discoveryStormRepeats is the duplicate storm size.
const discoveryStormRepeats = 500

// discoveryPrefix builds a 12 byte participant prefix from one fill byte.
func discoveryPrefix(fill byte) dds.SEDPGUIDPrefix {
	var prefix dds.SEDPGUIDPrefix
	for i := range prefix {
		prefix[i] = fill + byte(i)
	}

	return prefix
}

// newDiscoveryIndex returns an SEDP index with a distinct prefix.
func newDiscoveryIndex(t *testing.T, fill byte) *dds.SEDPIndex {
	t.Helper()

	prefix := discoveryPrefix(fill)

	idx, err := dds.NewSEDPIndex(prefix, 0)
	if err != nil {
		t.Fatalf("new sedp index: %v", err)
	}

	return idx
}

// mustDeclareChatterWriter declares the /chatter writer under test.
func mustDeclareChatterWriter(t *testing.T, idx *dds.SEDPIndex) dds.SEDPEndpoint {
	t.Helper()

	ep, err := idx.DeclareWriter("/chatter", "std_msgs__msg__String", "RIHS01_convergence", nil)
	if err != nil {
		t.Fatalf("declare writer: %v", err)
	}

	return ep
}

// mustDeclareChatterReader declares the /chatter reader used for matching.
func mustDeclareChatterReader(t *testing.T, idx *dds.SEDPIndex) dds.SEDPEndpoint {
	t.Helper()

	ep, err := idx.DeclareReader("/chatter", "std_msgs__msg__String", "RIHS01_convergence", nil)
	if err != nil {
		t.Fatalf("declare reader: %v", err)
	}

	return ep
}

// mustSerializeSEDP serializes one endpoint with a fresh DATA sequence.
func mustSerializeSEDP(t *testing.T, ep dds.SEDPEndpoint) []byte {
	t.Helper()

	wire, err := dds.SerializeSEDPDATA(ep)
	if err != nil {
		t.Fatalf("serialize sedp: %v", err)
	}

	return wire
}

// installDiscoveryWire parses one wire datagram and upserts each endpoint.
func installDiscoveryWire(t *testing.T, dst *dds.SEDPIndex, wire []byte) {
	t.Helper()

	eps, err := dds.ParseSEDPDATAs(wire)
	if err != nil {
		t.Fatalf("parse sedp: %v", err)
	}

	for _, ep := range eps {
		err = dst.UpsertRemote(ep)
		if err != nil {
			t.Fatalf("upsert remote: %v", err)
		}
	}
}

// assertPartitionHolds advances virtual time while partitioned and fails if the writer leaks.
func assertPartitionHolds(t *testing.T, observer *dds.SEDPIndex, switchBackend *chaos.ChaosBackend) {
	t.Helper()

	for i := range 5 {
		switchBackend.Advance(discoveryAnnounceNanos)

		remotes := observer.RemoteEndpoints()
		if hasChatterTopic(remotes) {
			t.Fatalf("partition heal: writer visible at wait tick %d while partitioned", i)
		}
	}
}

// pollForChatterAfterHeal resends the writer until it appears or fails with the poll bound.
func pollForChatterAfterHeal(t *testing.T, observer *dds.SEDPIndex, writerEp dds.SEDPEndpoint) int {
	t.Helper()

	for i := range discoveryHealBound {
		wire := mustSerializeSEDP(t, writerEp)
		installDiscoveryWire(t, observer, wire)

		remotes := observer.RemoteEndpoints()
		if hasChatterTopic(remotes) {
			return i
		}
	}

	t.Fatalf("partition heal: writer not visible within %d polls after heal", discoveryHealBound)

	return -1
}

// hasChatterTopic reports whether remotes contain /chatter like HasRemoteEndpoint.
func hasChatterTopic(remotes []dds.SEDPEndpoint) bool {
	for _, ep := range remotes {
		if ep.ROSTopic == "/chatter" {
			return true
		}
	}

	return false
}

// openPartitionSwitch returns a joined chaos backend used as the partition switch.
func openPartitionSwitch(t *testing.T) *chaos.ChaosBackend {
	t.Helper()

	backend, err := chaos.NewChaosBackend(chaos.ChaosConfig{})
	if err != nil {
		t.Fatalf("new chaos backend: %v", err)
	}

	err = backend.JoinDiscovery(context.Background(), transports.DiscoveryInfo{ParticipantName: "discovery-gate"})
	if err != nil {
		t.Fatalf("join discovery: %v", err)
	}

	return backend
}

// TestDiscoveryConvergenceBurstyLoss pins that bursty SEDP loss still converges via periodic resend within the announce bound.
func TestDiscoveryConvergenceBurstyLoss(t *testing.T) {
	t.Parallel()

	clock := chaos.NewFakeClock(0)

	link, err := chaos.NewBurstyLink(1_000_000_000, 1_000_000_000, 500, 800, 3)
	if err != nil {
		t.Fatalf("new bursty link: %v", err)
	}

	writerIdx := newDiscoveryIndex(t, 0xA0)
	observerIdx := newDiscoveryIndex(t, 0xB0)

	writerEp := mustDeclareChatterWriter(t, writerIdx)
	readerEp := mustDeclareChatterReader(t, observerIdx)

	dropped := 0
	installedAt := -1

	for i := range discoveryMaxAnnounces {
		wire := mustSerializeSEDP(t, writerEp)

		seq := uint64(i + 1)
		now := clock.Now()

		if link.DropAt(now, seq) {
			dropped++
			clock.Advance(discoveryAnnounceNanos)

			continue
		}

		installDiscoveryWire(t, observerIdx, wire)

		remotes := observerIdx.RemoteEndpoints()
		if hasChatterTopic(remotes) {
			installedAt = i

			break
		}

		clock.Advance(discoveryAnnounceNanos)
	}

	if installedAt < 0 {
		t.Fatalf("bursty loss: writer not installed after %d announces at virtual %d ns", discoveryMaxAnnounces, clock.Now())
	}

	if dropped == 0 {
		t.Fatalf("bursty loss: dropped 0 of %d announces, adversity not exercised", discoveryMaxAnnounces)
	}

	writers, err := observerIdx.MatchingWriters(readerEp.ID)
	if err != nil {
		t.Fatalf("matching writers: %v", err)
	}

	if len(writers) != 1 {
		t.Fatalf("bursty loss: matching writers = %d at announce %d, want 1", len(writers), installedAt)
	}
}

// TestDiscoveryConvergencePartitionHeal pins that a healed partition re-announces the writer within the heal bound.
func TestDiscoveryConvergencePartitionHeal(t *testing.T) {
	t.Parallel()

	switchBackend := openPartitionSwitch(t)

	defer func() {
		err := switchBackend.Close()
		if err != nil {
			t.Errorf("close switch: %v", err)
		}
	}()

	writerIdx := newDiscoveryIndex(t, 0xC0)
	observerIdx := newDiscoveryIndex(t, 0xD0)

	writerEp := mustDeclareChatterWriter(t, writerIdx)
	readerEp := mustDeclareChatterReader(t, observerIdx)

	switchBackend.Split()

	if !switchBackend.Partitioned() {
		t.Fatalf("partition heal: switch not partitioned after Split")
	}

	if got := switchBackend.DiscoveryGeneration(); got != 1 {
		t.Fatalf("partition heal: generation after split = %d, want 1", got)
	}

	assertPartitionHolds(t, observerIdx, switchBackend)

	switchBackend.Heal()

	if switchBackend.Partitioned() {
		t.Fatalf("partition heal: switch still partitioned after Heal")
	}

	if got := switchBackend.DiscoveryGeneration(); got != 2 {
		t.Fatalf("partition heal: generation after heal = %d, want 2", got)
	}

	foundAt := pollForChatterAfterHeal(t, observerIdx, writerEp)

	writers, err := observerIdx.MatchingWriters(readerEp.ID)
	if err != nil {
		t.Fatalf("matching writers: %v", err)
	}

	if len(writers) != 1 {
		t.Fatalf("partition heal: matching writers = %d at poll %d, want 1", len(writers), foundAt)
	}
}

// TestDiscoveryConvergenceHeartbeatStorm pins that duplicate SEDP heartbeats do not grow state without bound and still converge.
func TestDiscoveryConvergenceHeartbeatStorm(t *testing.T) {
	t.Parallel()

	writerIdx := newDiscoveryIndex(t, 0xE0)
	observerIdx := newDiscoveryIndex(t, 0xF0)

	writerEp := mustDeclareChatterWriter(t, writerIdx)
	readerEp := mustDeclareChatterReader(t, observerIdx)

	wire := mustSerializeSEDP(t, writerEp)
	installDiscoveryWire(t, observerIdx, wire)

	remotes := observerIdx.RemoteEndpoints()
	if !hasChatterTopic(remotes) {
		t.Fatalf("heartbeat storm: writer missing before storm at generation %d", observerIdx.Generation())
	}

	baseGen := observerIdx.Generation()
	if baseGen != 1 {
		t.Fatalf("heartbeat storm: generation before storm = %d, want 1", baseGen)
	}

	for range discoveryStormRepeats {
		installDiscoveryWire(t, observerIdx, wire)
	}

	remotes = observerIdx.RemoteEndpoints()
	if len(remotes) != 1 {
		t.Fatalf("heartbeat storm: remotes = %d after %d duplicates, want 1", len(remotes), discoveryStormRepeats)
	}

	if got := observerIdx.Generation(); got != baseGen {
		t.Fatalf("heartbeat storm: generation = %d after %d duplicates, want %d", got, discoveryStormRepeats, baseGen)
	}

	writers, err := observerIdx.MatchingWriters(readerEp.ID)
	if err != nil {
		t.Fatalf("matching writers: %v", err)
	}

	if len(writers) != 1 {
		t.Fatalf("heartbeat storm: matching writers = %d after %d duplicates, want 1", len(writers), discoveryStormRepeats)
	}
}
