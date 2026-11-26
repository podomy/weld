// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

package main

import (
	"testing"

	"github.com/podomy/weld/internal/transports/dds"
	"github.com/podomy/weld/internal/types"
)

// weldTestGuid returns a fixed 8 byte guid for split-leg tests.
func weldTestGuid() [8]byte {
	return [8]byte{1, 2, 3, 4, 5, 6, 7, 8}
}

// weldTestBridge creates an unjoined bridge for tests.
func weldTestBridge(t *testing.T) int64 {
	t.Helper()
	bh, code := weldGoBridgeNewNoJoin()
	if code != 0 {
		t.Fatalf("bridge_new = %d, want 0", code)
	}
	return bh
}

// requireWeldCode fails when code is not the wanted value.
func requireWeldCode(t *testing.T, code int32, want int32, op string) {
	t.Helper()
	if code != want {
		t.Fatalf("%s = %d, want %d", op, code, want)
	}
}

// checkNormalizeDynamicType pins slash to IDL normalization.
func checkNormalizeDynamicType(t *testing.T) {
	t.Helper()
	got := weldNormalizeDynamicType("std_msgs/msg/String")
	if got != "std_msgs__msg__String" {
		t.Fatalf("normalize slash = %q, want IDL form", got)
	}
	got = weldNormalizeDynamicType("std_msgs__msg__String")
	if got != "std_msgs__msg__String" {
		t.Fatalf("normalize IDL = %q, want passthrough", got)
	}
	got = weldNormalizeDynamicType("")
	if got != "" {
		t.Fatalf("normalize empty = %q, want empty", got)
	}
}

// checkSubHasData pins readiness reporting around publish.
func checkSubHasData(t *testing.T) {
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
	if got := weldGoSubHasData(sub); got != 0 {
		t.Fatalf("has_data empty = %d, want 0", got)
	}
	pub, code := weldGoPubCreate(bh, "n", "/t")
	requireWeldCode(t, code, 0, "pub_create")
	defer func() {
		requireWeldCode(t, weldGoPubDestroy(pub), 0, "pub_destroy")
	}()
	requireWeldCode(t, weldGoPubPublish(pub, []byte{9, 8, 7}), 0, "publish")
	if got := weldGoSubHasData(sub); got <= 0 {
		t.Fatalf("has_data ready = %d, want positive", got)
	}
	requireWeldCode(t, weldGoSubHasData(-1), -2, "has_data bad handle")
	requireWeldCode(t, weldGoSubHasData(1<<40), -2, "has_data unknown handle")
}

// checkDynamicType pins dynamic type reporting and validation.
func checkDynamicType(t *testing.T) {
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
	requireWeldCode(t, weldGoSubSetDynamicType(sub, "std_msgs/msg/String", ""), 0, "set_dynamic slash")
	requireWeldCode(t, weldGoSubSetDynamicType(sub, "std_msgs__msg__String", "RIHS01_x"), 0, "set_dynamic idl")
	requireWeldCode(t, weldGoSubSetDynamicType(sub, "", ""), -3, "set_dynamic empty")
	requireWeldCode(t, weldGoSubSetDynamicType(1<<40, "std_msgs/msg/String", ""), -2, "set_dynamic unknown")
}

// checkServiceSetType pins service type reporting and validation.
func checkServiceSetType(t *testing.T) {
	t.Helper()
	bh := weldTestBridge(t)
	defer func() {
		requireWeldCode(t, weldGoBridgeFree(bh), 0, "bridge_free")
	}()
	svc, code := weldGoSvcCreate(bh, "/svc")
	requireWeldCode(t, code, 0, "svc_create")
	defer func() {
		requireWeldCode(t, weldGoSvcDestroy(svc), 0, "svc_destroy")
	}()
	requireWeldCode(t, weldGoSvcSetType(svc, "pkg__srv__Name", ""), 0, "set_type")
	requireWeldCode(t, weldGoSvcSetType(svc, "", ""), 0, "set_type clear")
	requireWeldCode(t, weldGoSvcSetType(1<<40, "pkg__srv__Name", ""), -2, "set_type unknown")
}

// checkServiceSplitRoundTrip pins the in-memory send plus serve plus poll path.
func checkServiceSplitRoundTrip(t *testing.T) {
	t.Helper()
	bh := weldTestBridge(t)
	defer func() {
		requireWeldCode(t, weldGoBridgeFree(bh), 0, "bridge_free")
	}()
	svc, code := weldGoSvcCreate(bh, "/svc")
	requireWeldCode(t, code, 0, "svc_create")
	defer func() {
		requireWeldCode(t, weldGoSvcDestroy(svc), 0, "svc_destroy")
	}()
	guid := weldTestGuid()
	req := []byte{4, 1, 2, 3}
	requireWeldCode(t, weldGoSvcSendNB(svc, 1, guid, req), 0, "send_nb")
	requireWeldCode(t, weldGoSvcSendNB(svc, 1, guid, req), -3, "send_nb duplicate")
	n, ncode := weldGoSvcHasRequest(svc)
	requireWeldCode(t, ncode, 0, "has_request code")
	if n != 1 {
		t.Fatalf("has_request = %d, want 1", n)
	}
	ready, readyCode := weldGoSvcHasResponse(svc, 1, guid, "pkg__srv__Name")
	requireWeldCode(t, readyCode, 0, "has_response pending code")
	if ready != 0 {
		t.Fatalf("has_response pending = %d, want 0", ready)
	}
	got, srvSeq, scode := weldGoSvcServeNB(svc, 64)
	requireWeldCode(t, scode, 0, "serve_nb code")
	if string(got) != string(req) {
		t.Fatalf("serve_nb payload = %v, want %v", got, req)
	}
	n, ncode = weldGoSvcHasRequest(svc)
	requireWeldCode(t, ncode, 0, "has_request after serve code")
	if n != 0 {
		t.Fatalf("has_request after serve = %d, want 0", n)
	}
	resp := []byte{8, 7, 6}
	requireWeldCode(t, weldGoSvcRespond(svc, srvSeq, resp), 0, "respond")
	ready, readyCode = weldGoSvcHasResponse(svc, 1, guid, "pkg__srv__Name")
	requireWeldCode(t, readyCode, 0, "has_response ready code")
	if ready != 1 {
		t.Fatalf("has_response ready = %d, want 1", ready)
	}
	back, count, pcode := weldGoSvcPollReply(svc, 1, guid, 64)
	requireWeldCode(t, pcode, 0, "poll_reply code")
	if string(back) != string(resp) {
		t.Fatalf("poll_reply payload = %v, want %v", back, resp)
	}
	if int(count) != len(resp) {
		t.Fatalf("poll_reply count = %d, want %d", count, len(resp))
	}
	_, _, pcode = weldGoSvcPollReply(svc, 1, guid, 64)
	requireWeldCode(t, pcode, -2, "poll_reply consumed")
	bad := guid
	bad[0] ^= 0xFF
	requireWeldCode(t, weldGoSvcSendNB(svc, 2, guid, req), 0, "send_nb second")
	requireWeldCode(t, weldGoSvcSendNB(svc, 3, bad, req), 0, "send_nb third")
	_, _, pcode = weldGoSvcPollReply(svc, 3, guid, 64)
	requireWeldCode(t, pcode, -2, "poll_reply guid mismatch")
	_, _, hcode := weldGoSvcPollReply(1<<40, 1, guid, 64)
	requireWeldCode(t, hcode, -2, "poll_reply unknown handle")
}

// checkServiceSplitShort pins short-buffer stash plus retry for poll_reply.
func checkServiceSplitShort(t *testing.T) {
	t.Helper()
	bh := weldTestBridge(t)
	defer func() {
		requireWeldCode(t, weldGoBridgeFree(bh), 0, "bridge_free")
	}()
	svc, code := weldGoSvcCreate(bh, "/svc")
	requireWeldCode(t, code, 0, "svc_create")
	defer func() {
		requireWeldCode(t, weldGoSvcDestroy(svc), 0, "svc_destroy")
	}()
	guid := weldTestGuid()
	requireWeldCode(t, weldGoSvcSendNB(svc, 1, guid, []byte{1}), 0, "send_nb")
	got, srvSeq, scode := weldGoSvcServeNB(svc, 64)
	requireWeldCode(t, scode, 0, "serve_nb code")
	if len(got) != 1 {
		t.Fatalf("serve_nb payload len = %d, want 1", len(got))
	}
	resp := []byte{1, 2, 3, 4}
	requireWeldCode(t, weldGoSvcRespond(svc, srvSeq, resp), 0, "respond")
	_, _, pcode := weldGoSvcPollReply(svc, 1, guid, 2)
	requireWeldCode(t, pcode, -4, "poll_reply short")
	back, count, pcode := weldGoSvcPollReply(svc, 1, guid, 64)
	requireWeldCode(t, pcode, 0, "poll_reply retry code")
	if string(back) != string(resp) || int(count) != len(resp) {
		t.Fatalf("poll_reply retry = %v/%d, want %v/%d", back, count, resp, len(resp))
	}
}

// checkServiceRTPSArgs pins RTPS leg validation without a backend.
func checkServiceRTPSArgs(t *testing.T) {
	t.Helper()
	bh := weldTestBridge(t)
	defer func() {
		requireWeldCode(t, weldGoBridgeFree(bh), 0, "bridge_free")
	}()
	svc, code := weldGoSvcCreate(bh, "/svc")
	requireWeldCode(t, code, 0, "svc_create")
	defer func() {
		requireWeldCode(t, weldGoSvcDestroy(svc), 0, "svc_destroy")
	}()
	guid := weldTestGuid()
	// No join means no RTPS backend, so the RTPS send fails loudly.
	requireWeldCode(t, weldGoSvcSendRTPS(svc, 1, guid, "pkg__srv__Name", []byte{1, 2}), -3, "send_rtps no backend")
	requireWeldCode(t, weldGoSvcSendRTPS(1<<40, 1, guid, "pkg__srv__Name", []byte{1}), -2, "send_rtps unknown")
	requireWeldCode(t, weldGoSvcSendRTPS(svc, 0, guid, "pkg__srv__Name", []byte{1}), -2, "send_rtps bad seq")
	requireWeldCode(t, weldGoSvcSendRTPS(svc, 1, guid, "", []byte{1}), -3, "send_rtps empty base")
	_, _, tcode := weldGoSvcTakeReply(svc, 1, guid, "pkg__srv__Name", 64)
	requireWeldCode(t, tcode, -2, "take_reply no waiter")
	_, _, tcode = weldGoSvcTakeReply(1<<40, 1, guid, "pkg__srv__Name", 64)
	requireWeldCode(t, tcode, -2, "take_reply unknown handle")
	_, hcode := weldGoSvcHasResponse(svc, 9, guid, "pkg__srv__Name")
	requireWeldCode(t, hcode, -2, "has_response no waiter")
	_, hcode = weldGoSvcHasRequest(1 << 40)
	requireWeldCode(t, hcode, -2, "has_request unknown handle")
}

// checkServiceReplyMatch pins guid plus seq matching on wire packets.
func checkServiceReplyMatch(t *testing.T) {
	t.Helper()
	guid := weldTestGuid()
	wire, err := dds.MarshalServiceRequest([]byte{5, 6}, guid, 7, true)
	if err != nil {
		t.Fatalf("marshal service request: %v", err)
	}
	inner, ok := weldMatchServiceReply(wire, guid, 7)
	if !ok {
		t.Fatalf("match own packet = false, want true")
	}
	if string(inner) != string(wire[types.ServiceEnvelopeSize:]) {
		t.Fatalf("match inner = %v, want envelope suffix", inner)
	}
	other := guid
	other[0] ^= 0xFF
	if _, ok := weldMatchServiceReply(wire, other, 7); ok {
		t.Fatalf("match wrong guid = true, want false")
	}
	if _, ok := weldMatchServiceReply(wire, guid, 8); ok {
		t.Fatalf("match wrong seq = true, want false")
	}
	if _, ok := weldMatchServiceReply([]byte{1, 2, 3}, guid, 7); ok {
		t.Fatalf("match truncated = true, want false")
	}
}

// checkContentFilter pins filter validation and storage.
func checkContentFilter(t *testing.T) {
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
	requireWeldCode(t, weldGoSubSetFilter(sub, "count < 10", nil), 0, "set_filter plain")
	requireWeldCode(t, weldGoSubSetFilter(sub, "count = %0", []string{"10"}), 0, "set_filter params")
	requireWeldCode(t, weldGoSubSetFilter(sub, "count =", nil), -3, "set_filter invalid")
	requireWeldCode(t, weldGoSubSetFilter(sub, "", nil), -1, "set_filter empty")
	requireWeldCode(t, weldGoSubSetFilter(1<<40, "count < 10", nil), -2, "set_filter unknown")
}

// checkRemoteEp pins remote endpoint queries on an empty backend view.
func checkRemoteEp(t *testing.T) {
	t.Helper()
	bh := weldTestBridge(t)
	defer func() {
		requireWeldCode(t, weldGoBridgeFree(bh), 0, "bridge_free")
	}()
	n, code := weldGoRemoteEpCount(bh, "/t", 0)
	requireWeldCode(t, code, 0, "remote_ep_count code")
	if n != 0 {
		t.Fatalf("remote_ep_count = %d, want 0", n)
	}
	_, _, code = weldGoRemoteEpAt(bh, "/t", 0, 0)
	requireWeldCode(t, code, -2, "remote_ep_at empty")
	_, code = weldGoRemoteEpCount(bh, "/t", 9)
	requireWeldCode(t, code, -1, "remote_ep_count bad role")
	_, code = weldGoRemoteEpCount(1<<40, "/t", 0)
	requireWeldCode(t, code, -2, "remote_ep_count unknown bridge")
	_, _, code = weldGoRemoteEpAt(bh, "/t", 1, -1)
	requireWeldCode(t, code, -2, "remote_ep_at negative index")
}

// checkLiveliness pins liveliness reporting and generation rules.
func checkLiveliness(t *testing.T) {
	t.Helper()
	bh := weldTestBridge(t)
	defer func() {
		requireWeldCode(t, weldGoBridgeFree(bh), 0, "bridge_free")
	}()
	// Unjoined bridges reject reports.
	requireWeldCode(t, weldGoReportLiveliness(bh, "p", 0, 1, 1), -3, "liveliness unjoined")
	requireWeldCode(t, weldGoJoin(bh, "p", 0), 0, "join")
	requireWeldCode(t, weldGoReportLiveliness(bh, "p", 0, 1, 1), 0, "liveliness first")
	requireWeldCode(t, weldGoReportLiveliness(bh, "p", 0, 1, 1), 0, "liveliness idempotent")
	requireWeldCode(t, weldGoReportLiveliness(bh, "p", 0, 1, 2), 0, "liveliness advance")
	requireWeldCode(t, weldGoReportLiveliness(bh, "p", 0, 1, 1), -3, "liveliness stale")
	requireWeldCode(t, weldGoReportLiveliness(bh, "p", 0, 2, 3), -1, "liveliness bad alive")
	requireWeldCode(t, weldGoReportLiveliness(bh, "p", -1, 1, 3), -1, "liveliness bad domain")
	requireWeldCode(t, weldGoReportLiveliness(bh, "", 0, 1, 3), -3, "liveliness empty participant")
	requireWeldCode(t, weldGoReportLiveliness(1<<40, "p", 0, 1, 1), -2, "liveliness unknown bridge")
}

// TestWeldMissingExports exercises every new export through its C ABI path.
func TestWeldMissingExports(t *testing.T) {
	t.Run("normalize", checkNormalizeDynamicType)
	t.Run("has_data", checkSubHasData)
	t.Run("dynamic_type", checkDynamicType)
	t.Run("service_type", checkServiceSetType)
	t.Run("split_roundtrip", checkServiceSplitRoundTrip)
	t.Run("split_short", checkServiceSplitShort)
	t.Run("rtps_args", checkServiceRTPSArgs)
	t.Run("reply_match", checkServiceReplyMatch)
	t.Run("content_filter", checkContentFilter)
	t.Run("remote_ep", checkRemoteEp)
	t.Run("liveliness", checkLiveliness)
}
