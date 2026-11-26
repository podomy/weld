// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

package main

import (
	"testing"
	"time"

	"github.com/podomy/weld/internal/transports/dds"
)

// checkEventDeadline covers per-type deadline init plus take plus validation.
//
// Init ok is the setter on a live handle; take returns real counts after
// a full period without delivery; unknown handles fail closed.
func checkEventDeadline(t *testing.T) {
	t.Helper()
	bh := weldTestBridge(t)
	defer func() {
		requireWeldCode(t, weldGoBridgeFree(bh), 0, "bridge_free")
	}()
	sub, code := weldGoSubCreate(bh, "n", "/dl")
	requireWeldCode(t, code, 0, "sub_create")
	defer func() {
		requireWeldCode(t, weldGoSubDestroy(sub), 0, "sub_destroy")
	}()
	pub, code := weldGoPubCreate(bh, "n", "/dl")
	requireWeldCode(t, code, 0, "pub_create")
	defer func() {
		requireWeldCode(t, weldGoPubDestroy(pub), 0, "pub_destroy")
	}()
	// Init ok on live handles.
	requireWeldCode(t, weldGoSubSetDeadline(sub, 0, 1), 0, "sub_set_deadline")
	requireWeldCode(t, weldGoPubSetDeadline(pub, 0, 1), 0, "pub_set_deadline")
	// Unknown handles fail closed.
	requireWeldCode(t, weldGoSubSetDeadline(1<<40, 0, 1), -2, "sub_set_deadline unknown")
	requireWeldCode(t, weldGoPubSetDeadline(1<<40, 0, 1), -2, "pub_set_deadline unknown")
	if _, code := weldGoSubDeadlineTotal(1 << 40); code != -2 {
		t.Fatalf("sub_deadline_total unknown = %d, want -2", code)
	}
	if _, code := weldGoPubDeadlineTotal(1 << 40); code != -2 {
		t.Fatalf("pub_deadline_total unknown = %d, want -2", code)
	}
	// A 1ns deadline misses immediately without delivery.
	time.Sleep(5 * time.Millisecond)
	total, code := weldGoSubDeadlineTotal(sub)
	requireWeldCode(t, code, 0, "sub_deadline_total code")
	if total == 0 {
		t.Fatalf("sub_deadline_total = 0, want nonzero after missed period")
	}
	total, code = weldGoPubDeadlineTotal(pub)
	requireWeldCode(t, code, 0, "pub_deadline_total code")
	if total == 0 {
		t.Fatalf("pub_deadline_total = 0, want nonzero after missed period")
	}
	// Infinite deadlines never miss.
	requireWeldCode(t, weldGoSubSetDeadline(sub, 0, 0), 0, "sub_set_deadline infinite")
	total, code = weldGoSubDeadlineTotal(sub)
	requireWeldCode(t, code, 0, "sub_deadline_total infinite code")
	if total != 0 {
		t.Fatalf("sub_deadline_total infinite = %d, want 0", total)
	}
}

// checkEventLiveliness covers liveliness init plus real matched counts.
//
// Init ok is endpoint creation; take returns real counts via the
// existing matched totals: alive writers track matched publishers.
func checkEventLiveliness(t *testing.T) {
	t.Helper()
	bh := weldTestBridge(t)
	defer func() {
		requireWeldCode(t, weldGoBridgeFree(bh), 0, "bridge_free")
	}()
	sub, code := weldGoSubCreate(bh, "n", "/live")
	requireWeldCode(t, code, 0, "sub_create")
	defer func() {
		requireWeldCode(t, weldGoSubDestroy(sub), 0, "sub_destroy")
	}()
	if n, ok := weldMatchedSubTotal(sub); !ok {
		t.Fatalf("matched sub unknown handle")
	} else if n != 0 {
		t.Fatalf("matched sub = %d, want 0 before publisher", n)
	}
	pub, code := weldGoPubCreate(bh, "n", "/live")
	requireWeldCode(t, code, 0, "pub_create")
	defer func() {
		requireWeldCode(t, weldGoPubDestroy(pub), 0, "pub_destroy")
	}()
	if n, ok := weldMatchedSubTotal(sub); !ok || n != 1 {
		t.Fatalf("matched sub = %d/%v, want 1/true after publisher", n, ok)
	}
	if n, ok := weldMatchedPubTotal(pub); !ok || n != 1 {
		t.Fatalf("matched pub = %d/%v, want 1/true after subscription", n, ok)
	}
	if _, ok := weldMatchedSubTotal(1 << 40); ok {
		t.Fatalf("matched sub unknown handle reports ok")
	}
}

// checkEventIncompat covers incompat init plus matcher decisions.
//
// Init ok is endpoint creation; take returns real counts: identical
// offers match while reliability mismatches refuse via the shared
// CheckWireCompatibility matcher. Malformed QoS counts fail-closed.
func checkEventIncompat(t *testing.T) {
	t.Helper()
	base := dds.WireQoS{
		ReliabilityKind: 2,
		DurabilityKind:  1,
		HistoryKind:     0,
		Depth:           100,
		Deadline:        dds.InfiniteWireDuration(),
		Lifespan:        dds.InfiniteWireDuration(),
		LivelinessKind:  0,
		LeaseDuration:   dds.InfiniteWireDuration(),
	}
	local := dds.SEDPEndpoint{
		ROSTopic: "/qos",
		DDSTopic: "rt/qos",
		Kind:     dds.TopicKindTopic,
		TypeDDS:  "weld_test::msg::dds_::S_",
		TypeIDL:  "weld_test__msg__S",
		DomainID: 0,
		IsReader: true,
		RxOWire:  &base,
	}
	same := base
	remoteSame := dds.SEDPEndpoint{
		ROSTopic: "/qos",
		DDSTopic: "rt/qos",
		Kind:     dds.TopicKindTopic,
		TypeDDS:  "weld_test::msg::dds_::S_",
		TypeIDL:  "weld_test__msg__S",
		DomainID: 0,
		RxOWire:  &same,
	}
	if weldQoSMismatched(local, remoteSame, false) {
		t.Fatalf("identical QoS reports mismatched")
	}
	diff := base
	diff.ReliabilityKind = 1
	remoteDiff := remoteSame
	remoteDiff.RxOWire = &diff
	if !weldQoSMismatched(local, remoteDiff, false) {
		t.Fatalf("reliability mismatch reports matched")
	}
	part := remoteSame
	part.Partitions = []string{"other"}
	if weldQoSMismatched(local, part, false) {
		t.Fatalf("partition mismatch counts as QoS incompatibility")
	}
	malformed := remoteSame
	malformed.RxOMalformed = true
	if !weldQoSMismatched(local, malformed, false) {
		t.Fatalf("malformed QoS reports matched")
	}
	nilQoS := remoteSame
	nilQoS.RxOWire = nil
	if weldQoSMismatched(local, nilQoS, false) {
		t.Fatalf("unknown QoS counts as incompatibility")
	}
	// Unknown handles fail closed with no counts.
	if _, code := weldGoSubIncompatTotal(1 << 40); code != -2 {
		t.Fatalf("sub_incompat unknown = %d, want -2", code)
	}
	if _, code := weldGoPubIncompatTotal(1 << 40); code != -2 {
		t.Fatalf("pub_incompat unknown = %d, want -2", code)
	}
	bh := weldTestBridge(t)
	defer func() {
		requireWeldCode(t, weldGoBridgeFree(bh), 0, "bridge_free")
	}()
	sub, code := weldGoSubCreate(bh, "n", "/qos")
	requireWeldCode(t, code, 0, "sub_create")
	defer func() {
		requireWeldCode(t, weldGoSubDestroy(sub), 0, "sub_destroy")
	}()
	total, code := weldGoSubIncompatTotal(sub)
	requireWeldCode(t, code, 0, "sub_incompat code")
	if total != 0 {
		t.Fatalf("sub_incompat lone = %d, want 0", total)
	}
}

// checkEventQoSMapValid pins the facade offer mapping.
func checkEventQoSMapValid(t *testing.T, wire dds.WireQoS) {
	t.Helper()
	ep := dds.SEDPEndpoint{RxOWire: &wire}
	qos, ok := weldQoSFromWire(ep)
	if !ok {
		t.Fatalf("qos map valid = false, want true")
	}
	if qos.reliability != 2 || qos.durability != 1 || qos.history != 0 || qos.depth != 100 || qos.liveliness != 0 {
		t.Fatalf("qos map = %+v, want facade 2/1/0/100/0", qos)
	}
	if qos.deadlineSec != 0 || qos.deadlineNs != 0 || qos.leaseSec != 0 || qos.leaseNs != 0 {
		t.Fatalf("qos infinite durations = %d/%d/%d/%d, want zeros", qos.deadlineSec, qos.deadlineNs, qos.leaseSec, qos.leaseNs)
	}
}

// checkEventQoSMapReject pins malformed plus empty offers failing closed.
func checkEventQoSMapReject(t *testing.T, wire dds.WireQoS) {
	t.Helper()
	malformed := dds.SEDPEndpoint{RxOWire: &wire, RxOMalformed: true}
	if _, ok := weldQoSFromWire(malformed); ok {
		t.Fatalf("qos map malformed = true, want false")
	}
	if _, ok := weldQoSFromWire(dds.SEDPEndpoint{}); ok {
		t.Fatalf("qos map empty = true, want false")
	}
}

// checkEventQoSMap covers the wire-to-RMW QoS mapping fail-closed rules.
func checkEventQoSMap(t *testing.T) {
	t.Helper()
	wire := dds.WireQoS{
		ReliabilityKind: 2,
		DurabilityKind:  1,
		HistoryKind:     0,
		Depth:           100,
		Deadline:        dds.InfiniteWireDuration(),
		Lifespan:        dds.InfiniteWireDuration(),
		LivelinessKind:  0,
		LeaseDuration:   dds.InfiniteWireDuration(),
	}
	checkEventQoSMapValid(t, wire)
	checkEventQoSMapReject(t, wire)
}

// checkRemoteQoSValidation covers fail-closed validation without a backend.
func checkRemoteQoSValidation(t *testing.T) {
	t.Helper()
	bh := weldTestBridge(t)
	defer func() {
		requireWeldCode(t, weldGoBridgeFree(bh), 0, "bridge_free")
	}()
	_, _, _, _, _, _, _, _, _, code := weldGoRemoteEpQoS(bh, "/t", 0, 0)
	requireWeldCode(t, code, -2, "remote_qos empty")
	_, _, _, _, _, _, _, _, _, code = weldGoRemoteEpQoS(1<<40, "/t", 0, 0)
	requireWeldCode(t, code, -2, "remote_qos unknown bridge")
	_, _, _, _, _, _, _, _, _, code = weldGoRemoteEpQoS(bh, "/t", 9, 0)
	requireWeldCode(t, code, -1, "remote_qos bad role")
	_, _, _, _, _, _, _, _, _, code = weldGoRemoteEpQoS(bh, "/t", 0, -1)
	requireWeldCode(t, code, -2, "remote_qos negative index")
}

// checkRemoteQoSAnnounced covers remote QoS matching the weld announcement.
//
// Bridge B publishes so its SEDP offer lands in bridge A remotes: A must
// report the facade reliable transient-local keep-last offer.
func checkRemoteQoSAnnounced(t *testing.T) {
	t.Helper()
	bhA, bhB := joinNetflowPair(t)
	defer func() {
		requireWeldCode(t, weldGoBridgeFree(bhA), 0, "bridge_free A")
	}()
	defer func() {
		requireWeldCode(t, weldGoBridgeFree(bhB), 0, "bridge_free B")
	}()
	subA, pubB := declareNetflowPair(t, bhA, bhB)
	defer func() {
		requireWeldCode(t, weldGoSubDestroy(subA), 0, "sub_destroy A")
	}()
	defer func() {
		requireWeldCode(t, weldGoPubDestroy(pubB), 0, "pub_destroy B")
	}()
	_, _ = waitNetflowWriterPeer(t, bhA, bhB, pubB)
	reliability, durability, history, depth, deadlineSec, deadlineNs, liveliness, leaseSec, leaseNs, code := weldGoRemoteEpQoS(bhA, "/weld_netflow_probe", 0, 0)
	requireWeldCode(t, code, 0, "remote_qos announced code")
	if reliability != 2 {
		t.Fatalf("remote reliability = %d, want 2", reliability)
	}
	if durability != 1 {
		t.Fatalf("remote durability = %d, want 1", durability)
	}
	if history != 0 {
		t.Fatalf("remote history = %d, want 0", history)
	}
	if depth != 100 {
		t.Fatalf("remote depth = %d, want 100", depth)
	}
	if deadlineSec != 0 || deadlineNs != 0 {
		t.Fatalf("remote deadline = %d/%d, want 0/0", deadlineSec, deadlineNs)
	}
	if liveliness != 0 {
		t.Fatalf("remote liveliness = %d, want 0", liveliness)
	}
	if leaseSec != 0 || leaseNs != 0 {
		t.Fatalf("remote lease = %d/%d, want 0/0", leaseSec, leaseNs)
	}
	_, _, _, _, _, _, _, _, _, code = weldGoRemoteEpQoS(bhA, "/weld_netflow_probe", 0, 1<<20)
	requireWeldCode(t, code, -2, "remote_qos past end")
}

// TestWeldEventSources exercises deadline, liveliness, incompatibility,
// and remote QoS getters through their C ABI paths.
func TestWeldEventSources(t *testing.T) {
	t.Run("deadline", checkEventDeadline)
	t.Run("liveliness", checkEventLiveliness)
	t.Run("incompat", checkEventIncompat)
	t.Run("qos_map", checkEventQoSMap)
	t.Run("remote_qos_validation", checkRemoteQoSValidation)
	t.Run("remote_qos_announced", checkRemoteQoSAnnounced)
}
