// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

package main

import (
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/podomy/weld/internal/transports/dds"
	"github.com/podomy/weld/internal/transports/dds/rtps"
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

// sharedServedReq is one dequeued request awaiting a response.
type sharedServedReq struct {
	handle int64
	seq    int64
	req    []byte
}

// sharedSplitState tracks FFI multiserver progress.
type sharedSplitState struct {
	mu     sync.Mutex
	served int
	total  int
}

// sharedSplitFinished reports whether every request was served.
func (s *sharedSplitState) sharedSplitFinished() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.served >= s.total
}

// sharedSplitNote records one dequeued request.
func (s *sharedSplitState) sharedSplitNote() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.served++
}

// serveSharedSplit drains one FFI service handle until total is served.
func serveSharedSplit(handle int64, state *sharedSplitState, servedCh chan sharedServedReq, serveErr chan error, done chan struct{}) {
	defer close(done)
	for {
		got, srvSeq, scode := weldGoSvcServeNB(handle, 256)
		if scode == 1 {
			if state.sharedSplitFinished() {
				return
			}
			time.Sleep(time.Millisecond)
			continue
		}
		if scode != 0 {
			serveErr <- fmt.Errorf("serve_nb code = %d, want 0", scode)
			return
		}
		state.sharedSplitNote()
		servedCh <- sharedServedReq{handle: handle, seq: srvSeq, req: got}
	}
}

// sendSharedBurst enqueues total requests, retrying a full queue.
//
// Sends are non-blocking, so a busy queue reports -3 until the servers
// drain it; the retry preserves send order.
func sendSharedBurst(t *testing.T, svc int64, guids [][8]byte, total int) {
	t.Helper()
	for i := range total {
		req := []byte{byte(i), byte(i >> 8)}
		sent := false
		for attempt := 0; attempt < 1000 && !sent; attempt++ {
			code := weldGoSvcSendNB(svc, int64(i+1), guids[i], req)
			if code == 0 {
				sent = true
				break
			}
			if code != -3 {
				t.Fatalf("send_nb = %d, want 0", code)
			}
			time.Sleep(time.Millisecond)
		}
		if !sent {
			t.Fatalf("send_nb %d never accepted: queue stayed full", i)
		}
	}
}

// drainSharedServed responds to every dequeued request and pins exactly-once.
func drainSharedServed(t *testing.T, svcA, svcB int64, total int, servedCh chan sharedServedReq, doneA, doneB chan struct{}, serveErr chan error) {
	t.Helper()
	seen := make(map[string]int)
	perHandle := make(map[int64]int)
	for range total {
		entry := <-servedCh
		seen[string(entry.req)]++
		perHandle[entry.handle]++
		resp := append([]byte("resp:"), entry.req...)
		requireWeldCode(t, weldGoSvcRespond(entry.handle, entry.seq, resp), 0, "respond")
	}
	<-doneA
	<-doneB
	close(serveErr)
	for err := range serveErr {
		t.Fatalf("server: %v", err)
	}
	if len(seen) != total {
		t.Fatalf("distinct served = %d, want %d", len(seen), total)
	}
	for payload, n := range seen {
		if n != 1 {
			t.Fatalf("payload %q served %d times, want exactly once", payload, n)
		}
	}
	if perHandle[svcA] == 0 || perHandle[svcB] == 0 {
		t.Fatalf("server split = %d/%d, want both positive", perHandle[svcA], perHandle[svcB])
	}
}

// checkSharedReplies collects every reply and pins the payloads.
func checkSharedReplies(t *testing.T, svc int64, guids [][8]byte, total int) {
	t.Helper()
	for i := range total {
		req := []byte{byte(i), byte(i >> 8)}
		want := append([]byte("resp:"), req...)
		back, _, pcode := weldGoSvcPollReply(svc, int64(i+1), guids[i], 256)
		requireWeldCode(t, pcode, 0, "poll_reply code")
		if string(back) != string(want) {
			t.Fatalf("poll_reply %d = %v, want %v", i, back, want)
		}
	}
}

// checkSharedSurvivor pins the queue staying live after one destroy.
func checkSharedSurvivor(t *testing.T, svcA, svcB int64) {
	t.Helper()
	requireWeldCode(t, weldGoSvcDestroy(svcA), 0, "svc_destroy first")
	guid := weldTestGuid()
	requireWeldCode(t, weldGoSvcSendNB(svcB, 100, guid, []byte{9}), 0, "send_nb survivor")
	got, srvSeq, scode := weldGoSvcServeNB(svcB, 256)
	requireWeldCode(t, scode, 0, "serve_nb survivor")
	if string(got) != string([]byte{9}) {
		t.Fatalf("serve_nb survivor = %v, want [9]", got)
	}
	requireWeldCode(t, weldGoSvcRespond(svcB, srvSeq, []byte{7}), 0, "respond survivor")
	back, _, pcode := weldGoSvcPollReply(svcB, 100, guid, 256)
	requireWeldCode(t, pcode, 0, "poll_reply survivor")
	if string(back) != string([]byte{7}) {
		t.Fatalf("poll_reply survivor = %v, want [7]", back)
	}
	requireWeldCode(t, weldGoSvcDestroy(svcB), 0, "svc_destroy second")
}

// checkServiceSharedQueueMultiServer pins N servers on one service name
// through the split-leg FFI: two service handles share one request
// queue, so concurrent servers divide requests work-queue style with
// each request served exactly once, no loss, and no double serve.
func checkServiceSharedQueueMultiServer(t *testing.T) {
	t.Helper()
	bh := weldTestBridge(t)
	defer func() {
		requireWeldCode(t, weldGoBridgeFree(bh), 0, "bridge_free")
	}()
	svcA, code := weldGoSvcCreate(bh, "/shared")
	requireWeldCode(t, code, 0, "svc_create first")
	svcB, code := weldGoSvcCreate(bh, "/shared")
	requireWeldCode(t, code, 0, "svc_create second shares queue")
	const total = 32
	state := &sharedSplitState{total: total}
	servedCh := make(chan sharedServedReq, total)
	serveErr := make(chan error, 2)
	doneA := make(chan struct{})
	doneB := make(chan struct{})
	go serveSharedSplit(svcA, state, servedCh, serveErr, doneA)
	go serveSharedSplit(svcB, state, servedCh, serveErr, doneB)
	guids := make([][8]byte, total)
	for i := range guids {
		guids[i] = weldTestGuid()
		guids[i][7] = byte(i + 1)
	}
	sendSharedBurst(t, svcA, guids, total)
	drainSharedServed(t, svcA, svcB, total, servedCh, doneA, doneB, serveErr)
	checkSharedReplies(t, svcA, guids, total)
	checkSharedSurvivor(t, svcA, svcB)
}

// checkNetflowLocal pins the local socket entry on a joined bridge.
//
// The IP must equal the advertised source the backend uses for SPDP
// locators and the port must be the nonzero bound unicast port. An
// in-memory-only bridge reports the internal code: no socket exists.
func checkNetflowLocal(t *testing.T) {
	t.Helper()
	bh := weldTestBridge(t)
	defer func() {
		requireWeldCode(t, weldGoBridgeFree(bh), 0, "bridge_free")
	}()
	_, _, code := weldGoNetflowLocal(bh)
	requireWeldCode(t, code, -3, "netflow_local in-memory")
	_, _, code = weldGoNetflowLocal(1 << 40)
	requireWeldCode(t, code, -2, "netflow_local unknown bridge")
	requireWeldCode(t, weldGoJoin(bh, "netflow-local", 76), 0, "join")
	ip, port, code := weldGoNetflowLocal(bh)
	requireWeldCode(t, code, 0, "netflow_local code")
	if parsed := net.ParseIP(ip); parsed == nil {
		t.Fatalf("netflow_local ip = %q, want a valid IP", ip)
	}
	if ip != rtps.AdvertiseIPv4().String() {
		t.Fatalf("netflow_local ip = %q, want advertised %q", ip, rtps.AdvertiseIPv4().String())
	}
	if port <= 0 || port > 65535 {
		t.Fatalf("netflow_local port = %d, want 1..65535", port)
	}
	ipAgain, portAgain, code := weldGoNetflowLocal(bh)
	requireWeldCode(t, code, 0, "netflow_local repeat code")
	if ipAgain != ip || portAgain != port {
		t.Fatalf("netflow_local repeat = %q:%d, want stable %q:%d", ipAgain, portAgain, ip, port)
	}
}

// checkNetflowRemoteArgs pins remote validation without a backend.
func checkNetflowRemoteArgs(t *testing.T) {
	t.Helper()
	bh := weldTestBridge(t)
	defer func() {
		requireWeldCode(t, weldGoBridgeFree(bh), 0, "bridge_free")
	}()
	_, code := weldGoNetflowRemoteCount(bh, "/t", 1)
	requireWeldCode(t, code, -3, "netflow_remote_count in-memory")
	_, _, code = weldGoNetflowRemoteAt(bh, "/t", 1, 0)
	requireWeldCode(t, code, -3, "netflow_remote_at in-memory")
	_, code = weldGoNetflowRemoteCount(1<<40, "/t", 1)
	requireWeldCode(t, code, -2, "netflow_remote_count unknown bridge")
	_, code = weldGoNetflowRemoteCount(bh, "/t", 9)
	requireWeldCode(t, code, -1, "netflow_remote_count bad role")
	requireWeldCode(t, weldGoJoin(bh, "netflow-lone", 78), 0, "join")
	n, code := weldGoNetflowRemoteCount(bh, "/t", 1)
	requireWeldCode(t, code, 0, "netflow_remote_count lone code")
	if n != 0 {
		t.Fatalf("netflow_remote_count lone = %d, want 0", n)
	}
	_, _, code = weldGoNetflowRemoteAt(bh, "/t", 1, 0)
	requireWeldCode(t, code, -2, "netflow_remote_at past end")
}

// checkNetflowRemoteDiscovery pins discovered peer ports between bridges.
//
// Bridge B publishes so its endpoints land in bridge A remotes: A must
// report B's bound unicast port at loopback for both roles. Entries
// with unknown ports never surface.
// joinNetflowPair creates and joins two bridges on one domain.
func joinNetflowPair(t *testing.T) (int64, int64) {
	t.Helper()
	bhA, code := weldGoBridgeNewNoJoin()
	requireWeldCode(t, code, 0, "bridge_new A")
	bhB, code := weldGoBridgeNewNoJoin()
	requireWeldCode(t, code, 0, "bridge_new B")
	requireWeldCode(t, weldGoJoin(bhA, "netflow-a", 77), 0, "join A")
	requireWeldCode(t, weldGoJoin(bhB, "netflow-b", 77), 0, "join B")
	return bhA, bhB
}

// declareNetflowPair declares a reader on A and a writer on B.
//
// Setting the publisher type also declares a reader for the topic, so
// both roles see the peer.
func declareNetflowPair(t *testing.T, bhA, bhB int64) (int64, int64) {
	t.Helper()
	subA, code := weldGoSubCreate(bhA, "n", "/weld_netflow_probe")
	requireWeldCode(t, code, 0, "sub_create A")
	requireWeldCode(t, weldGoSubSetDynamicType(subA, "std_msgs/msg/String", ""), 0, "set_dynamic A")
	pubB, code := weldGoPubCreate(bhB, "n", "/weld_netflow_probe")
	requireWeldCode(t, code, 0, "pub_create B")
	requireWeldCode(t, weldGoPubSetType(pubB, "std_msgs__msg__String", ""), 0, "set_type B")
	return subA, pubB
}

// waitNetflowWriterPeer republishes until A sees a writer peer.
//
// The writer declares on publish and publishes skip the wire until a
// remote reader is known, so republish until SEDP converges. It returns
// B's bound port with the writer peer count.
func waitNetflowWriterPeer(t *testing.T, bhA, bhB, pubB int64) (int, int64) {
	t.Helper()
	_, peerPort, code := weldGoNetflowLocal(bhB)
	requireWeldCode(t, code, 0, "netflow_local B")
	if peerPort <= 0 {
		t.Fatalf("netflow_local B port = %d, want nonzero", peerPort)
	}
	deadline := time.Now().Add(10 * time.Second)
	var n int64
	for {
		requireWeldCode(t, weldGoPubPublish(pubB, []byte{1}), 0, "publish B declares writer")
		n, code = weldGoNetflowRemoteCount(bhA, "/weld_netflow_probe", 0)
		requireWeldCode(t, code, 0, "netflow_remote_count code")
		if n > 0 || time.Now().After(deadline) {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if n <= 0 {
		t.Fatalf("netflow_remote_count writer peers = %d, want at least 1", n)
	}
	return peerPort, n
}

// checkNetflowEntries validates every remote entry and pins B's port.
func checkNetflowEntries(t *testing.T, bhA int64, role int32, n int64, peerPort int, what string) {
	t.Helper()
	found := false
	for i := range n {
		ip, port, code := weldGoNetflowRemoteAt(bhA, "/weld_netflow_probe", role, i)
		requireWeldCode(t, code, 0, "netflow_remote_at "+what+" code")
		if parsed := net.ParseIP(ip); parsed == nil {
			t.Fatalf("netflow_remote_at %s ip = %q, want a valid IP", what, ip)
		}
		if port <= 0 || port > 65535 {
			t.Fatalf("netflow_remote_at %s port = %d, want 1..65535", what, port)
		}
		// Remote flows address the loopback destination the
		// transport sends peer DATA to; the port must be the
		// discovered bound port of bridge B.
		if ip != weldNetflowLoopback {
			t.Fatalf("netflow_remote_at %s ip = %q, want loopback %q", what, ip, weldNetflowLoopback)
		}
		if port == peerPort {
			found = true
		}
	}
	if !found {
		t.Fatalf("netflow %s remotes miss B port %d", what, peerPort)
	}
	_, _, code := weldGoNetflowRemoteAt(bhA, "/weld_netflow_probe", role, n)
	requireWeldCode(t, code, -2, "netflow_remote_at "+what+" past end")
}

func checkNetflowRemoteDiscovery(t *testing.T) {
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
	peerPort, n := waitNetflowWriterPeer(t, bhA, bhB, pubB)
	checkNetflowEntries(t, bhA, 0, n, peerPort, "writer")
	nReaders, code := weldGoNetflowRemoteCount(bhA, "/weld_netflow_probe", 1)
	requireWeldCode(t, code, 0, "netflow_remote_count reader role code")
	if nReaders <= 0 {
		t.Fatalf("netflow_remote_count reader peers = %d, want at least 1", nReaders)
	}
	checkNetflowEntries(t, bhA, 1, nReaders, peerPort, "reader")
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
	t.Run("shared_queue", checkServiceSharedQueueMultiServer)
	t.Run("netflow_local", checkNetflowLocal)
	t.Run("netflow_remote_args", checkNetflowRemoteArgs)
	t.Run("netflow_remote_discovery", checkNetflowRemoteDiscovery)
	t.Run("content_filter", checkContentFilter)
	t.Run("remote_ep", checkRemoteEp)
	t.Run("liveliness", checkLiveliness)
}
