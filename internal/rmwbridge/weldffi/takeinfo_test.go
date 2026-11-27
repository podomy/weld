// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

package main

import (
	"testing"
)

// weldTakeInfoBridge creates a bridge with one pub plus one sub.
func weldTakeInfoBridge(t *testing.T) (int64, int64, int64) {
	t.Helper()
	bh := weldTestBridge(t)
	pub, code := weldGoPubCreate(bh, "n", "/takeinfo")
	requireWeldCode(t, code, 0, "pub_create")
	sub, code := weldGoSubCreate(bh, "n", "/takeinfo")
	requireWeldCode(t, code, 0, "sub_create")
	return bh, pub, sub
}

// weldTakeInfoGID returns the writer GID for a publisher handle.
func weldTakeInfoGID(t *testing.T, pub int64) [16]byte {
	t.Helper()
	weldMu.Lock()
	defer weldMu.Unlock()
	e, ok := weldPubs[pub]
	if !ok {
		t.Fatalf("publisher handle missing from registry")
	}
	return e.pub.GID()
}

// checkWeldTakeInfo takes one sample and pins its sequence plus GID.
func checkWeldTakeInfo(t *testing.T, sub int64, want string, wantSeq uint64, wantGID [16]byte) {
	t.Helper()
	data, n, code := weldGoSubTakePoll(sub, 256)
	requireWeldCode(t, code, 0, "take_poll")
	if string(data) != want || int(n) != len(want) {
		t.Fatalf("take_poll = %q n = %d, want %q n = %d", string(data), n, want, len(want))
	}
	gid, seq, icode := weldGoSubTakeInfo(sub)
	requireWeldCode(t, icode, 0, "take_info")
	if seq != wantSeq {
		t.Fatalf("sequence = %d, want %d", seq, wantSeq)
	}
	if gid != wantGID {
		t.Fatalf("gid = %x, want %x", gid, wantGID)
	}
	if gid == ([16]byte{}) {
		t.Fatalf("gid is zero, want writer identity")
	}
}

// checkWeldTakeInfoSequence pins 1,2,3 sequences plus GID across takes.
//
// Intra-process and network sequence numbers are equal because the
// bridge is a single transport, so the side channel reports one value
// for both. The GID equals the publishing writer identity.
func checkWeldTakeInfoSequence(t *testing.T) {
	t.Helper()
	bh, pub, sub := weldTakeInfoBridge(t)
	defer func() {
		requireWeldCode(t, weldGoBridgeFree(bh), 0, "bridge_free")
	}()
	wantGID := weldTakeInfoGID(t, pub)
	if wantGID == ([16]byte{}) {
		t.Fatalf("publisher GID is zero")
	}
	payloads := []string{"one", "two", "three"}
	for i, payload := range payloads {
		requireWeldCode(t, weldGoPubPublish(pub, []byte(payload)), 0, "publish")
		checkWeldTakeInfo(t, sub, payload, uint64(i+1), wantGID)
	}
	requireWeldCode(t, weldGoSubDestroy(sub), 0, "sub_destroy")
	requireWeldCode(t, weldGoPubDestroy(pub), 0, "pub_destroy")
}

// checkWeldTakeInfoStashRetry pins that a short retry keeps its sequence.
//
// The oversized take stashes with its dequeue-time attribution, so the
// retry delivers the same sequence and the next take advances by one.
func checkWeldTakeInfoStashRetry(t *testing.T) {
	t.Helper()
	bh, pub, sub := weldTakeInfoBridge(t)
	defer func() {
		requireWeldCode(t, weldGoBridgeFree(bh), 0, "bridge_free")
	}()
	wantGID := weldTakeInfoGID(t, pub)
	requireWeldCode(t, weldGoPubPublish(pub, []byte("0123456789")), 0, "publish 10 bytes")
	_, _, code := weldGoSubTakePoll(sub, 2)
	requireWeldCode(t, code, -4, "short take_poll")
	checkWeldTakeInfo(t, sub, "0123456789", 1, wantGID)
	requireWeldCode(t, weldGoPubPublish(pub, []byte("next")), 0, "publish next")
	checkWeldTakeInfo(t, sub, "next", 2, wantGID)
	requireWeldCode(t, weldGoSubDestroy(sub), 0, "sub_destroy")
	requireWeldCode(t, weldGoPubDestroy(pub), 0, "pub_destroy")
}

// checkWeldTakeInfoEndToEnd pins publish plus take GID attribution.
//
// Publishing from a known writer and taking must report a nonzero GID
// equal to the writer identity.
func checkWeldTakeInfoEndToEnd(t *testing.T) {
	t.Helper()
	bh, pub, sub := weldTakeInfoBridge(t)
	defer func() {
		requireWeldCode(t, weldGoBridgeFree(bh), 0, "bridge_free")
	}()
	wantGID := weldTakeInfoGID(t, pub)
	requireWeldCode(t, weldGoPubPublish(pub, []byte("ping")), 0, "publish")
	checkWeldTakeInfo(t, sub, "ping", 1, wantGID)
	requireWeldCode(t, weldGoSubDestroy(sub), 0, "sub_destroy")
	requireWeldCode(t, weldGoPubDestroy(pub), 0, "pub_destroy")
}

// checkWeldTakeSourcePinsPublisherStamp pins source stamp after take.
func checkWeldTakeSourcePinsPublisherStamp(t *testing.T) {
	t.Helper()
	bh, pub, sub := weldTakeInfoBridge(t)
	defer func() {
		requireWeldCode(t, weldGoBridgeFree(bh), 0, "bridge_free")
	}()
	requireWeldCode(t, weldGoPubPublish(pub, []byte("ping")), 0, "publish")
	data, _, code := weldGoSubTakePoll(sub, 256)
	requireWeldCode(t, code, 0, "take_poll")
	if string(data) != "ping" {
		t.Fatalf("take = %q, want ping", string(data))
	}
	ns, code := weldGoSubTakeSource(sub)
	requireWeldCode(t, code, 0, "take_source")
	if ns == 0 {
		t.Fatalf("source stamp is zero, want publisher time")
	}
	requireWeldCode(t, weldGoSubDestroy(sub), 0, "sub_destroy")
	requireWeldCode(t, weldGoPubDestroy(pub), 0, "pub_destroy")
}

// checkWeldTakeInfoNoTake pins the side channel with no delivered take.
func checkWeldTakeInfoNoTake(t *testing.T) {
	t.Helper()
	bh := weldTestBridge(t)
	defer func() {
		requireWeldCode(t, weldGoBridgeFree(bh), 0, "bridge_free")
	}()
	sub, code := weldGoSubCreate(bh, "n", "/t")
	requireWeldCode(t, code, 0, "sub_create")
	defer func() {
		requireWeldCode(t, weldGoSubDestroy(sub), 0, "sub_destroy")
	}()
	_, _, icode := weldGoSubTakeInfo(sub)
	requireWeldCode(t, icode, -2, "take_info without take")
	_, _, icode = weldGoSubTakeInfo(1 << 40)
	requireWeldCode(t, icode, -2, "take_info unknown handle")
}

// checkWeldPubGIDPinsTakeGID pins pub GID equals take GID in-process.
//
// The publisher GID export and the take-info side channel share the
// bridge counter authority, so rmw_compare_gids_equal holds between
// rmw_get_gid_for_publisher and the take GID.
func checkWeldPubGIDPinsTakeGID(t *testing.T) {
	t.Helper()
	bh, pub, sub := weldTakeInfoBridge(t)
	defer func() {
		requireWeldCode(t, weldGoBridgeFree(bh), 0, "bridge_free")
	}()
	pubGID, code := weldGoPubGetGID(pub)
	requireWeldCode(t, code, 0, "pub_get_gid")
	if pubGID == ([16]byte{}) {
		t.Fatalf("pub GID is zero, want writer identity")
	}
	requireWeldCode(t, weldGoPubPublish(pub, []byte("ping")), 0, "publish")
	checkWeldTakeInfo(t, sub, "ping", 1, pubGID)
	_, code = weldGoPubGetGID(1 << 40)
	requireWeldCode(t, code, -2, "pub_get_gid unknown")
	requireWeldCode(t, weldGoSubDestroy(sub), 0, "sub_destroy")
	requireWeldCode(t, weldGoPubDestroy(pub), 0, "pub_destroy")
}

// TestWeldTakeInfo exercises the take-info side channel.
func TestWeldTakeInfo(t *testing.T) {
	t.Run("sequence", checkWeldTakeInfoSequence)
	t.Run("stash_retry", checkWeldTakeInfoStashRetry)
	t.Run("end_to_end_gid", checkWeldTakeInfoEndToEnd)
	t.Run("no_take", checkWeldTakeInfoNoTake)
	t.Run("source_stamp", checkWeldTakeSourcePinsPublisherStamp)
	t.Run("pub_gid_pins_take_gid", checkWeldPubGIDPinsTakeGID)
}
