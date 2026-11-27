// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

// This file fills the remaining weak FFI symbols declared in
// rmw_weld/src/rmw_common.h: subscription readiness plus dynamic types,
// the split service legs (in-memory send/poll plus RTPS send/take with
// non-consuming probes), content filters, live remote endpoint queries,
// and liveliness reports.
//
// Contract notes for the C agent (rmw_weld split modules):
//   - In-memory legs reuse the existing bridge Service rendezvous, so a
//     send_nb request is servable by the existing serve_nb export and
//     answerable by the existing respond export with no C changes.
//   - The RTPS client leg is complete (send_rtps declares the client
//     writer plus reply reader, take_reply polls the reply topic, the
//     has_response probe peeks the wire). The RTPS server leg is future
//     work: serve_nb stays in-memory only, so has_request counts only
//     requests servable by serve_nb.
//   - In-memory takes bypass content filters: the filter needs a type
//     descriptor the FFI layer does not carry. Wire takes filter through
//     the backend once the subscription type is known.
//   - Liveliness has no C caller yet: rmw_node_assert_liveliness and
//     rmw_publisher_assert_liveliness are validation-only stubs. Wire
//     them to weld_ffi_report_liveliness with participant set to the
//     node name, domain to the context domain, alive to 1, and
//     generation to a process-wide monotonic counter (first call any
//     value, strictly increasing after; repeating a generation with the
//     same alive value is idempotent, any other reuse is stale).
package main

/*
#include <stdlib.h>
*/
import "C"

import (
	"context"
	"fmt"
	"strings"
	"time"
	"unsafe"

	"github.com/podomy/weld/internal/rmwbridge"
	"github.com/podomy/weld/internal/transports/dds"
	"github.com/podomy/weld/internal/types"
)

// weldSplitPollBound bounds the split-leg reply waits.
//
// It matches weldCallBound so take_response behaves like the old combined
// call_nb path: the server answers well before this limit in tests, and
// expiry reports empty so the C layer keeps the pending entry and may
// retry the take.
const weldSplitPollBound = 5 * time.Second

// Remote endpoint roles for weld_ffi_remote_ep_count/at.
//
// They match the WELD_FILTER_ROLE_* enum in rmw_graph.c: 0 writer, 1
// reader, 2 server, 3 client.
const (
	weldRoleWriter = 0
	weldRoleReader = 1
	weldRoleServer = 2
	weldRoleClient = 3
)

// weldSvcType records the IDL service base for one service handle.
type weldSvcType struct {
	bridgeID int64
	name     string
	typeName string
	typeHash string
}

// weldSubFilter records one compiled content filter for a subscription.
type weldSubFilter struct {
	expr   string
	params []string
	filter *dds.ContentFilter
}

// weldSplitWaiter parks one in-memory request sent by send_nb.
type weldSplitWaiter struct {
	guid  [8]byte
	reply chan []byte
}

// weldRTPSWaiter parks one RTPS request sent by send_rtps.
type weldRTPSWaiter struct {
	bridgeID int64
	service  string
	base     string
	guid     [8]byte
}

// weldServeRTPSReq holds one dequeued RTPS request for serve_rtps.
type weldServeRTPSReq struct {
	payload []byte
	guid    [8]byte
	seq     uint64
}

// Split-leg and annotation state. Guarded by weldMu like the rest of
// the handle registry. Backend calls never run under weldMu.
var (
	// weldSvcTypes maps service handle to its IDL service base.
	weldSvcTypes = make(map[int64]weldSvcType)
	// weldSubFilters maps subscription handle to its content filter.
	weldSubFilters = make(map[int64]weldSubFilter)
	// weldSplitNB parks in-memory reply waiters by service plus seq.
	weldSplitNB = make(map[weldSvcReplyKey]weldSplitWaiter)
	// weldSplitNBStash holds consumed in-memory replies that did not fit.
	weldSplitNBStash = make(map[weldSvcReplyKey][]byte)
	// weldSplitRTPS parks RTPS reply waiters by service plus seq.
	weldSplitRTPS = make(map[weldSvcReplyKey]weldRTPSWaiter)
	// weldSplitRTPSStash holds consumed RTPS replies that did not fit.
	weldSplitRTPSStash = make(map[weldSvcReplyKey][]byte)
	// weldServeRTPSStash holds dequeued RTPS requests that did not fit.
	weldServeRTPSStash = make(map[int64][]weldServeRTPSReq)
)

// weldNormalizeDynamicType converts a slash type name to IDL form.
//
// Dynamic subscriptions report slash names such as
// std_msgs/msg/String; the reader declare path stores IDL names such
// as std_msgs__msg__String. Names without a slash pass through so IDL
// input keeps working. It never panics.
func weldNormalizeDynamicType(name string) string {
	if strings.Contains(name, "/") {
		return strings.ReplaceAll(name, "/", "__")
	}
	return name
}

// weldReadGuid copies exactly 8 guid bytes from a C buffer.
//
// The guid rides as raw bytes and may contain NULs, so the length is
// fixed instead of NUL terminated. It returns ErrNull for a NULL
// pointer. It never panics.
func weldReadGuid(ptr *C.char) ([8]byte, C.int) {
	var guid [8]byte
	if ptr == nil {
		return guid, C.int(weldCodeErrNull)
	}
	raw := C.GoBytes(unsafe.Pointer(ptr), 8)
	copy(guid[:], raw)
	return guid, C.int(weldCodeOK)
}

// weldCheckSeq validates a C supplied service sequence id.
//
// Sequences start at 1 on the C side; anything lower is a protocol
// violation and reports the handle code. It never panics.
func weldCheckSeq(seq C.longlong) (int64, C.int) {
	if int64(seq) <= 0 {
		return 0, C.int(weldCodeErrHandle)
	}
	return int64(seq), C.int(weldCodeOK)
}

// weldWriteReplyToC copies a service reply into a C buffer.
//
// An oversized reply writes 0 to outN and reports short without
// consuming the parked waiter; the caller retries with a larger buffer.
// It never panics.
func weldWriteReplyToC(respBuf *C.char, respBufLen C.int, outN *C.int, resp []byte) C.int {
	if len(resp) > int(respBufLen) {
		*outN = C.int(0)
		return C.int(weldCodeErrShort)
	}
	code := weldCopyToC(respBuf, respBufLen, resp)
	if code != C.int(weldCodeOK) {
		*outN = C.int(0)
		return code
	}
	*outN = C.int(len(resp))
	return C.int(weldCodeOK)
}

// weldMatchServiceReply splits one wire packet and matches guid plus seq.
//
// The inner payload aliases the packet, so callers copy before the
// packet is reused. It never panics.
func weldMatchServiceReply(packet []byte, guid [8]byte, seq uint64) ([]byte, bool) {
	if len(packet) == 0 {
		return nil, false
	}
	var metrics types.Metrics
	inner, got, gotSeq, _, err := dds.UnmarshalServiceReply(packet, &metrics)
	if err != nil {
		return nil, false
	}
	if got != guid || gotSeq != seq {
		return nil, false
	}
	return append([]byte(nil), inner...), true
}

// weldTakeOverflowReply removes one matching reply from the wire stash.
//
// Stash order is arrival order, so the first guid plus seq match wins
// and anything else stays parked for its own consumer. It never panics.
func weldTakeOverflowReply(bridgeID int64, guid [8]byte, seq uint64) ([]byte, bool) {
	weldMu.Lock()
	defer weldMu.Unlock()
	queue := weldRTPSOverflow[bridgeID]
	for i, msg := range queue {
		inner, ok := weldMatchServiceReply(msg.Payload, guid, seq)
		if !ok {
			continue
		}
		queue = append(queue[:i], queue[i+1:]...)
		if len(queue) == 0 {
			delete(weldRTPSOverflow, bridgeID)
		} else {
			weldRTPSOverflow[bridgeID] = queue
		}
		return inner, true
	}
	return nil, false
}

// weldPeekOverflowReply reports a matching reply stashed on the wire.
//
// Unlike the take form it leaves the stash intact for the consuming
// take. It never panics.
func weldPeekOverflowReply(bridgeID int64, guid [8]byte, seq uint64) bool {
	weldMu.Lock()
	defer weldMu.Unlock()
	for _, msg := range weldRTPSOverflow[bridgeID] {
		_, ok := weldMatchServiceReply(msg.Payload, guid, seq)
		if ok {
			return true
		}
	}
	return false
}

// weldOverflowHasTopic reports a stashed wire sample for one topic.
//
// Samples with an empty topic match any subscription, mirroring the
// live take filters. It never panics.
func weldOverflowHasTopic(bridgeID int64, topic string) bool {
	weldMu.Lock()
	defer weldMu.Unlock()
	for _, msg := range weldRTPSOverflow[bridgeID] {
		if msg.TopicName == "" || msg.TopicName == topic {
			return true
		}
	}
	return false
}

// weldLoadSvcLocked returns the service entry for a handle.
//
// It reports false for an unknown handle. The caller must hold weldMu.
func weldLoadSvcLocked(key int64) (weldSvcEntry, bool) {
	e, ok := weldServices[key]
	return e, ok
}

// weldRemoteEpMatchesTopicRole reports whether an endpoint fills a topic role.
//
// Writer matches topic kind writers and reader matches topic kind
// readers. It never panics.
func weldRemoteEpMatchesTopicRole(ep dds.SEDPEndpoint, role int) bool {
	if ep.Kind != dds.TopicKindTopic {
		return false
	}
	if role == weldRoleWriter {
		return !ep.IsReader
	}
	return ep.IsReader
}

// weldRemoteEpMatchesServiceRole reports whether an endpoint fills a service role.
//
// Server matches a request reader or a reply writer, and client matches
// the reverse pair, mirroring the graph count helpers. It never panics.
func weldRemoteEpMatchesServiceRole(ep dds.SEDPEndpoint, role int) bool {
	if role == weldRoleServer {
		return (ep.Kind == dds.TopicKindRequest && ep.IsReader) ||
			(ep.Kind == dds.TopicKindReply && !ep.IsReader)
	}
	return (ep.Kind == dds.TopicKindRequest && !ep.IsReader) ||
		(ep.Kind == dds.TopicKindReply && ep.IsReader)
}

// weldRemoteEpMatchesRole reports whether an endpoint fills one role.
//
// Roles match the WELD_FILTER_ROLE_* enum: 0 writer, 1 reader, 2
// server, 3 client. Any other role never matches. It never panics.
func weldRemoteEpMatchesRole(ep dds.SEDPEndpoint, role int) bool {
	switch role {
	case weldRoleWriter, weldRoleReader:
		return weldRemoteEpMatchesTopicRole(ep, role)
	case weldRoleServer, weldRoleClient:
		return weldRemoteEpMatchesServiceRole(ep, role)
	default:
		return false
	}
}

// weldRemoteEpsFor collects live SEDP remotes for a topic plus role.
//
// An absent backend contributes nothing. The backend snapshot arrives
// sorted, so the filtered order stays stable absent discovery churn. It
// never panics.
func weldRemoteEpsFor(bridgeID int64, topic string, role int) []dds.SEDPEndpoint {
	weldMu.Lock()
	backend := weldRTPS[bridgeID]
	weldMu.Unlock()
	if backend == nil {
		return nil
	}
	var out []dds.SEDPEndpoint
	for _, ep := range backend.RemoteEndpoints() {
		if ep.ROSTopic != topic {
			continue
		}
		if !weldRemoteEpMatchesRole(ep, role) {
			continue
		}
		out = append(out, ep)
	}
	return out
}

// weldSplitKeysForBridgeLocked collects split-leg keys servable from one bridge.
//
// Entries whose service handle is already gone collect too so bridge
// teardown drops orphans. The caller must hold weldMu. It never panics.
func weldSplitKeysForBridgeLocked(bridgeID int64) []weldSvcReplyKey {
	seen := make(map[weldSvcReplyKey]struct{})
	for rkey := range weldSplitNB {
		seen[rkey] = struct{}{}
	}
	for rkey := range weldSplitNBStash {
		seen[rkey] = struct{}{}
	}
	for rkey := range weldSplitRTPS {
		seen[rkey] = struct{}{}
	}
	for rkey := range weldSplitRTPSStash {
		seen[rkey] = struct{}{}
	}
	var out []weldSvcReplyKey
	for rkey := range seen {
		e, ok := weldServices[rkey.svc]
		if !ok || e.bridgeID == bridgeID {
			out = append(out, rkey)
		}
	}
	return out
}

// weldSvcKeysLocked collects split-leg keys for one service handle.
//
// The caller must hold weldMu. It never panics.
func weldSvcKeysLocked(key int64) []weldSvcReplyKey {
	seen := make(map[weldSvcReplyKey]struct{})
	for rkey := range weldSplitNB {
		if rkey.svc == key {
			seen[rkey] = struct{}{}
		}
	}
	for rkey := range weldSplitNBStash {
		if rkey.svc == key {
			seen[rkey] = struct{}{}
		}
	}
	for rkey := range weldSplitRTPS {
		if rkey.svc == key {
			seen[rkey] = struct{}{}
		}
	}
	for rkey := range weldSplitRTPSStash {
		if rkey.svc == key {
			seen[rkey] = struct{}{}
		}
	}
	out := make([]weldSvcReplyKey, 0, len(seen))
	for rkey := range seen {
		out = append(out, rkey)
	}
	return out
}

// weldForgetSplitKey deletes waiter plus stash state for one sequence.
//
// The caller must hold weldMu. It never panics.
func weldForgetSplitKey(rkey weldSvcReplyKey) {
	delete(weldSplitNB, rkey)
	delete(weldSplitNBStash, rkey)
	delete(weldSplitRTPS, rkey)
	delete(weldSplitRTPSStash, rkey)
}

// weldDropSvcExtrasLocked removes split-leg and type state for one service.
//
// The caller must hold weldMu.
func weldDropSvcExtrasLocked(key int64) {
	delete(weldSvcTypes, key)
	delete(weldServeRTPSStash, key)
	for _, rkey := range weldSvcKeysLocked(key) {
		weldForgetSplitKey(rkey)
	}
}

// weldDropSubExtrasLocked removes filter state for one subscription.
//
// The caller must hold weldMu.
func weldDropSubExtrasLocked(key int64) {
	delete(weldSubFilters, key)
}

// weldDropBridgeExtrasLocked removes split-leg, type, and filter state for one bridge.
//
// Entries whose service or subscription handle is already gone are
// orphans and drop too. The caller must hold weldMu.
func weldDropBridgeExtrasLocked(bridgeID int64) {
	for id, e := range weldSvcTypes {
		if e.bridgeID == bridgeID {
			delete(weldSvcTypes, id)
		}
	}
	for id := range weldSubFilters {
		if b, ok := weldSubs[id]; !ok || b.bridgeID == bridgeID {
			delete(weldSubFilters, id)
		}
	}
	for _, rkey := range weldSplitKeysForBridgeLocked(bridgeID) {
		weldForgetSplitKey(rkey)
	}
	for id, e := range weldServices {
		if e.bridgeID == bridgeID {
			delete(weldServeRTPSStash, id)
		}
	}
	for id := range weldServeRTPSStash {
		if _, ok := weldServices[id]; !ok {
			delete(weldServeRTPSStash, id)
		}
	}
	weldDropEventExtrasLocked(bridgeID)
}

// weldReadFilterParams splits a NUL joined params blob into nparams strings.
//
// The C layer packs each parameter NUL terminated, so the blob is walked
// with advancing GoString reads instead of a fixed length. A zero count
// allows a NULL blob. It never panics on well-formed input.
func weldReadFilterParams(blob *C.char, nparams C.int) ([]string, C.int) {
	if nparams < 0 {
		return nil, C.int(weldCodeErrNull)
	}
	if nparams == 0 {
		return nil, C.int(weldCodeOK)
	}
	if blob == nil {
		return nil, C.int(weldCodeErrNull)
	}
	out := make([]string, 0, int(nparams))
	p := unsafe.Pointer(blob)
	for range int(nparams) {
		s := C.GoString((*C.char)(p))
		out = append(out, s)
		p = unsafe.Add(p, len(s)+1)
	}
	return out, C.int(weldCodeOK)
}

// weld_ffi_sub_has_data reports queued samples for a subscription.
//
// It returns the in-memory queue depth plus one for a stashed retry
// payload, or 1 when only a stashed wire sample waits. Wire samples not
// yet pulled by a take stay invisible until the first take parks them
// in the overflow. Unknown handles report -2. It never panics.
//
//export weld_ffi_sub_has_data
func weld_ffi_sub_has_data(handle C.longlong) (rc C.int) {
	defer func() {
		r := recover()
		if r != nil {
			rc = C.int(weldCodeErrInternal)
		}
	}()
	key := int64(handle)
	if key <= 0 {
		return C.int(weldCodeErrHandle)
	}
	weldMu.Lock()
	e, ok := weldSubs[key]
	if !ok {
		weldMu.Unlock()
		return C.int(weldCodeErrHandle)
	}
	n := e.sub.PendingMessages()
	if _, stashed := weldSubPending[key]; stashed {
		n++
	}
	bkey := e.bridgeID
	topic := e.sub.Topic
	weldMu.Unlock()
	if n > 0 {
		return C.int(n)
	}
	if weldOverflowHasTopic(bkey, topic) {
		return C.int(1)
	}
	return C.int(0)
}

// weld_ffi_sub_set_dynamic_type stores wire type info for a dynamic subscription.
//
// typeName arrives in slash form such as std_msgs/msg/String and is
// normalized to IDL form before storing; IDL input passes through. The
// stored type drives same-bridge reader declares and the bridge graph,
// mirroring weld_ffi_sub_set_type. It returns 0 on success, -1 for NULL
// pointers, -2 for an unknown subscription, and -3 for an empty name.
// It never panics.
//
//export weld_ffi_sub_set_dynamic_type
func weld_ffi_sub_set_dynamic_type(handle C.longlong, typeName *C.char, typeHash *C.char) (rc C.int) {
	defer func() {
		r := recover()
		if r != nil {
			rc = C.int(weldCodeErrInternal)
		}
	}()
	key := int64(handle)
	if key <= 0 {
		return C.int(weldCodeErrHandle)
	}
	tn, th, code := weldReadTypePair(typeName, typeHash)
	if code != C.int(weldCodeOK) {
		return code
	}
	if tn == "" {
		return C.int(weldCodeErrInternal)
	}
	normalized := weldNormalizeDynamicType(tn)
	weldMu.Lock()
	st, ok := weldSubTypes[key]
	if !ok {
		weldMu.Unlock()
		return C.int(weldCodeErrHandle)
	}
	st.typeName = normalized
	st.typeHash = th
	weldSubTypes[key] = st
	sub := weldSubs[key].sub
	bridgeID := st.bridgeID
	topic := st.topic
	weldMu.Unlock()
	sub.SetTypeName(normalized)
	weldRefreshRTPSReader(bridgeID, topic)
	return C.int(weldCodeOK)
}

// weld_ffi_service_set_type records the IDL service base for a service handle.
//
// The base feeds RTPS endpoint declares on the first RTPS send or take
// and the bridge graph; declares stay lazy so a process with only a
// server never announces client endpoints. An empty name clears the
// stored type. It returns 0 on success, -1 for NULL pointers, and -2
// for an unknown handle. It never panics.
//
//export weld_ffi_service_set_type
func weld_ffi_service_set_type(handle C.longlong, typeName *C.char, typeHash *C.char) (rc C.int) {
	defer func() {
		r := recover()
		if r != nil {
			rc = C.int(weldCodeErrInternal)
		}
	}()
	key := int64(handle)
	if key <= 0 {
		return C.int(weldCodeErrHandle)
	}
	tn, th, code := weldReadTypePair(typeName, typeHash)
	if code != C.int(weldCodeOK) {
		return code
	}
	weldMu.Lock()
	e, ok := weldLoadSvcLocked(key)
	if !ok {
		weldMu.Unlock()
		return C.int(weldCodeErrHandle)
	}
	weldSvcTypes[key] = weldSvcType{bridgeID: e.bridgeID, name: e.name, typeName: tn, typeHash: th}
	svc := e.svc
	weldMu.Unlock()
	svc.SetTypeName(tn)
	return C.int(weldCodeOK)
}

// weld_ffi_service_has_request counts requests servable by serve_nb.
//
// The count sums the in-memory rendezvous depth with dequeued requests
// stashed for short-buffer retry. RTPS requests are excluded here and
// counted by weld_ffi_service_has_request_rtps, so this count matches
// what the next serve_nb can dequeue. It returns 0 on success, -1 for
// a NULL out pointer, and -2 for an unknown handle. It never panics.
//
//export weld_ffi_service_has_request
func weld_ffi_service_has_request(handle C.longlong, outN *C.int) (rc C.int) {
	defer func() {
		r := recover()
		if r != nil {
			rc = C.int(weldCodeErrInternal)
		}
	}()
	key := int64(handle)
	if key <= 0 {
		return C.int(weldCodeErrHandle)
	}
	if outN == nil {
		return C.int(weldCodeErrNull)
	}
	weldMu.Lock()
	e, ok := weldLoadSvcLocked(key)
	if !ok {
		weldMu.Unlock()
		return C.int(weldCodeErrHandle)
	}
	n := e.svc.PendingRequests() + len(weldSvcStash[key])
	weldMu.Unlock()
	*outN = C.int(n)
	return C.int(weldCodeOK)
}

// weldWaitSplitReply waits bounded for one parked in-memory reply.
//
// It reports false on context expiry without consuming the waiter. It
// never panics.
func weldWaitSplitReply(reply chan []byte, ctx context.Context) ([]byte, bool) {
	select {
	case resp := <-reply:
		return resp, true
	case <-ctx.Done():
		return nil, false
	}
}

// weldPeekSplitNB checks one parked in-memory waiter without consuming.
//
// A ready reply parks in the short stash for the consuming poll, so the
// peek leaves C-visible state unchanged. It never panics.
func weldPeekSplitNB(rkey weldSvcReplyKey, reply chan []byte) C.int {
	select {
	case resp := <-reply:
		weldMu.Lock()
		if _, still := weldSplitNB[rkey]; still {
			weldSplitNBStash[rkey] = resp
		}
		weldMu.Unlock()
		return C.int(1)
	default:
		return C.int(0)
	}
}

// weldPollRTPSReply polls the wire once for one reply and stashes the rest.
//
// A match returns its payload with ready true. Anything else parks in
// the bridge overflow for its own consumer and reports ready false; a
// transport error reports err so the caller can tell a quiet wire from
// a broken one. It never panics.
func weldPollRTPSReply(ctx context.Context, backend *dds.Backend, bridgeID int64, guid [8]byte, seq uint64) ([]byte, bool, error) {
	msg, err := backend.TakeBytes(ctx)
	if err != nil {
		return nil, false, fmt.Errorf("weld: poll rtps reply: %w", err)
	}
	inner, ok := weldMatchServiceReply(msg.Payload, guid, seq)
	if ok {
		return inner, true, nil
	}
	weldStashRTPSOverflow(bridgeID, msg)
	return nil, false, nil
}

// weldCheckReplyArgs validates a reply buffer plus count pointer.
//
// A zero length allows a NULL buffer. It never panics.
func weldCheckReplyArgs(respBuf *C.char, respBufLen C.int, outN *C.int) C.int {
	if outN == nil {
		return C.int(weldCodeErrNull)
	}
	if respBufLen < 0 {
		return C.int(weldCodeErrNull)
	}
	if respBufLen > 0 && respBuf == nil {
		return C.int(weldCodeErrNull)
	}
	return C.int(weldCodeOK)
}

// weldLoadSplitNB loads a parked in-memory waiter plus its stash state.
//
// It validates the handle, the sequence, and the guid in one place so
// the peek and poll exports share one gate. It never panics.
func weldLoadSplitNB(key, sid int64, guid [8]byte) (chan []byte, []byte, bool, C.int) {
	rkey := weldSvcReplyKey{svc: key, seq: sid}
	weldMu.Lock()
	defer weldMu.Unlock()
	if _, ok := weldServices[key]; !ok {
		return nil, nil, false, C.int(weldCodeErrHandle)
	}
	waiter, parked := weldSplitNB[rkey]
	if !parked || waiter.guid != guid {
		return nil, nil, false, C.int(weldCodeErrHandle)
	}
	stashed, haveStash := weldSplitNBStash[rkey]
	return waiter.reply, stashed, haveStash, C.int(weldCodeOK)
}

// weld_ffi_service_has_response peeks for one reply without consuming.
//
// The in-memory waiter and the short-buffer stash report ready without
// touching the queue. The RTPS leg scans the reply stash plus the wire
// overflow and finishes with one short wire poll; anything consumed
// parks back for the consuming take, so C state never changes. It
// returns 0 on success with 1 for ready and 0 for pending in outN, -1
// for NULL pointers, -2 for an unknown handle or sequence, and -3 when
// no waiter exists and no backend can be polled. It never panics.
//
//export weld_ffi_service_has_response
func weld_ffi_service_has_response(handle C.longlong, seq C.longlong, guid *C.char, idlBase *C.char, outN *C.int) (rc C.int) {
	defer func() {
		r := recover()
		if r != nil {
			rc = C.int(weldCodeErrInternal)
		}
	}()
	key := int64(handle)
	if key <= 0 {
		return C.int(weldCodeErrHandle)
	}
	sid, code := weldCheckSeq(seq)
	if code != C.int(weldCodeOK) {
		return code
	}
	if outN == nil || idlBase == nil {
		return C.int(weldCodeErrNull)
	}
	guidBytes, code := weldReadGuid(guid)
	if code != C.int(weldCodeOK) {
		return code
	}
	base, code := weldReadCString(idlBase)
	if code != C.int(weldCodeOK) {
		return code
	}
	rkey := weldSvcReplyKey{svc: key, seq: sid}
	ready, parked, code := weldProbeSplitNB(rkey, guidBytes)
	if code != C.int(weldCodeOK) {
		return code
	}
	if parked {
		*outN = ready
		return C.int(weldCodeOK)
	}
	ready, parked, code = weldProbeSplitRTPS(rkey, guidBytes, base, uint64(sid))
	if code != C.int(weldCodeOK) {
		return code
	}
	if parked {
		*outN = ready
		return C.int(weldCodeOK)
	}
	return C.int(weldCodeErrHandle)
}

// weldProbeSplitNB peeks one in-memory waiter for readiness.
//
// It returns ready 1 with parked true when a reply or stash waits, 0
// with parked true when the waiter is still pending, and parked false
// when no waiter exists. A guid mismatch reports the handle code. It
// never panics.
func weldProbeSplitNB(rkey weldSvcReplyKey, guid [8]byte) (C.int, bool, C.int) {
	weldMu.Lock()
	waiter, parked := weldSplitNB[rkey]
	if !parked {
		weldMu.Unlock()
		return C.int(0), false, C.int(weldCodeOK)
	}
	if waiter.guid != guid {
		weldMu.Unlock()
		return C.int(0), false, C.int(weldCodeErrHandle)
	}
	_, stashed := weldSplitNBStash[rkey]
	reply := waiter.reply
	weldMu.Unlock()
	if stashed {
		return C.int(1), true, C.int(weldCodeOK)
	}
	return weldPeekSplitNB(rkey, reply), true, C.int(weldCodeOK)
}

// weldProbeSplitRTPS peeks one RTPS waiter for readiness.
//
// The reply stash and the wire overflow report ready without consuming.
// Otherwise one short wire poll runs; a match parks in the reply stash
// for the consuming take and anything else parks in the overflow, so C
// state never changes. It returns ready, parked, and a code: unknown
// waiters report parked false, guid or base mismatches report the
// handle code, and a missing backend reports internal. It never panics.
func weldProbeSplitRTPS(rkey weldSvcReplyKey, guid [8]byte, base string, seq uint64) (C.int, bool, C.int) {
	weldMu.Lock()
	waiter, parked := weldSplitRTPS[rkey]
	if !parked {
		weldMu.Unlock()
		return C.int(0), false, C.int(weldCodeOK)
	}
	if waiter.guid != guid || waiter.base != base {
		weldMu.Unlock()
		return C.int(0), false, C.int(weldCodeErrHandle)
	}
	_, stashed := weldSplitRTPSStash[rkey]
	backend := weldRTPS[waiter.bridgeID]
	bridgeID := waiter.bridgeID
	weldMu.Unlock()
	if stashed {
		return C.int(1), true, C.int(weldCodeOK)
	}
	if weldPeekOverflowReply(bridgeID, guid, seq) {
		return C.int(1), true, C.int(weldCodeOK)
	}
	if backend == nil {
		return C.int(0), true, C.int(weldCodeErrInternal)
	}
	ctx, cancel := context.WithTimeout(context.Background(), weldPollBound)
	defer cancel()
	resp, matched, err := weldPollRTPSReply(ctx, backend, bridgeID, guid, seq)
	if err != nil || !matched {
		return C.int(0), true, C.int(weldCodeOK)
	}
	weldMu.Lock()
	if _, still := weldSplitRTPS[rkey]; still {
		weldSplitRTPSStash[rkey] = resp
	}
	weldMu.Unlock()
	return C.int(1), true, C.int(weldCodeOK)
}

// weld_ffi_service_send_nb enqueues an in-memory request without blocking.
//
// The request joins the bridge Service rendezvous through a non-blocking
// send, so serve_nb dequeues it and the existing respond answers it;
// the reply waiter parks under the C supplied sequence for a later
// poll_reply. A full queue or an oversized payload reports -3, a
// duplicate sequence reports -3, and unknown handles report -2. It
// never panics.
//
//export weld_ffi_service_send_nb
func weld_ffi_service_send_nb(handle C.longlong, seq C.longlong, guid *C.char, req *C.char, reqLen C.int) (rc C.int) {
	defer func() {
		r := recover()
		if r != nil {
			rc = C.int(weldCodeErrInternal)
		}
	}()
	key := int64(handle)
	if key <= 0 {
		return C.int(weldCodeErrHandle)
	}
	sid, code := weldCheckSeq(seq)
	if code != C.int(weldCodeOK) {
		return code
	}
	guidBytes, code := weldReadGuid(guid)
	if code != C.int(weldCodeOK) {
		return code
	}
	payload, code := weldReadBytes(req, reqLen)
	if code != C.int(weldCodeOK) {
		return code
	}
	weldMu.Lock()
	e, ok := weldLoadSvcLocked(key)
	if !ok {
		weldMu.Unlock()
		return C.int(weldCodeErrHandle)
	}
	rkey := weldSvcReplyKey{svc: key, seq: sid}
	if _, dup := weldSplitNB[rkey]; dup {
		weldMu.Unlock()
		return C.int(weldCodeErrInternal)
	}
	svc := e.svc
	weldMu.Unlock()
	reply, err := svc.TrySendRequest(payload)
	if err != nil {
		return C.int(weldCodeErrInternal)
	}
	weldMu.Lock()
	if _, ok := weldServices[key]; !ok {
		weldMu.Unlock()
		return C.int(weldCodeErrHandle)
	}
	weldSplitNB[rkey] = weldSplitWaiter{guid: guidBytes, reply: reply}
	weldMu.Unlock()
	return C.int(weldCodeOK)
}

// weld_ffi_service_poll_reply waits bounded for one in-memory reply.
//
// The parked waiter from send_nb delivers exactly once; expiry reports
// 1 with 0 in outN so the C layer keeps the pending entry and retries.
// An oversized reply parks in the short stash and reports -4 without
// consuming the waiter. Unknown handles or sequences report -2 and
// guid mismatches report -2. It never panics.
//
//export weld_ffi_service_poll_reply
func weld_ffi_service_poll_reply(handle C.longlong, seq C.longlong, guid *C.char, respBuf *C.char, respBufLen C.int, outN *C.int) (rc C.int) {
	defer func() {
		r := recover()
		if r != nil {
			rc = C.int(weldCodeErrInternal)
		}
	}()
	key := int64(handle)
	if key <= 0 {
		return C.int(weldCodeErrHandle)
	}
	sid, code := weldCheckSeq(seq)
	if code != C.int(weldCodeOK) {
		return code
	}
	if code := weldCheckReplyArgs(respBuf, respBufLen, outN); code != C.int(weldCodeOK) {
		return code
	}
	guidBytes, code := weldReadGuid(guid)
	if code != C.int(weldCodeOK) {
		return code
	}
	rkey := weldSvcReplyKey{svc: key, seq: sid}
	reply, stashed, haveStash, code := weldLoadSplitNB(key, sid, guidBytes)
	if code != C.int(weldCodeOK) {
		return code
	}
	if haveStash {
		return weldDeliverSplitNB(rkey, respBuf, respBufLen, outN, stashed)
	}
	ctx, cancel := context.WithTimeout(context.Background(), weldSplitPollBound)
	defer cancel()
	resp, ready := weldWaitSplitReply(reply, ctx)
	if !ready {
		*outN = C.int(0)
		return C.int(weldCodeEmpty)
	}
	if code := weldWriteReplyToC(respBuf, respBufLen, outN, resp); code != C.int(weldCodeOK) {
		weldMu.Lock()
		if _, still := weldSplitNB[rkey]; still {
			weldSplitNBStash[rkey] = resp
		}
		weldMu.Unlock()
		return code
	}
	weldMu.Lock()
	weldForgetSplitKey(rkey)
	weldMu.Unlock()
	return C.int(weldCodeOK)
}

// weldDeliverSplitNB copies one stashed in-memory reply and forgets its waiter.
//
// It never panics.
func weldDeliverSplitNB(rkey weldSvcReplyKey, respBuf *C.char, respBufLen C.int, outN *C.int, stashed []byte) C.int {
	code := weldWriteReplyToC(respBuf, respBufLen, outN, stashed)
	if code != C.int(weldCodeOK) {
		return code
	}
	weldMu.Lock()
	weldForgetSplitKey(rkey)
	weldMu.Unlock()
	return C.int(weldCodeOK)
}

// weldDeclareServiceClient declares the RTPS client endpoints for one service.
//
// The reply reader carries responses back to take_reply and the request
// writer carries send_rtps publishes; both declares are idempotent, so
// repeats after the first send cost one lookup. It never panics.
func weldDeclareServiceClient(ctx context.Context, backend *dds.Backend, service, idlBase, typeHash string) error {
	err := backend.SubscribeService(ctx, service, idlBase, dds.ServiceRoleClient, typeHash)
	if err != nil {
		return fmt.Errorf("weld: declare service client subscribe: %w", err)
	}
	err = backend.DeclareServiceWriter(ctx, service, idlBase, dds.ServiceRoleClient, typeHash)
	if err != nil {
		return fmt.Errorf("weld: declare service client writer: %w", err)
	}
	return nil
}

// weld_ffi_service_send_rtps publishes an RTPS request immediately.
//
// The payload wraps with the service header plus guid plus sequence and
// publishes through the client role; the reply waiter parks under the C
// supplied sequence for a later take_reply. Endpoint declares run first
// so peers see the client before DATA. Failures report -3 loudly,
// duplicate sequences report -3, and unknown handles report -2. It
// never panics.
//
//export weld_ffi_service_send_rtps
func weld_ffi_service_send_rtps(handle C.longlong, seq C.longlong, guid *C.char, idlBase *C.char, req *C.char, reqLen C.int) (rc C.int) {
	defer func() {
		r := recover()
		if r != nil {
			rc = C.int(weldCodeErrInternal)
		}
	}()
	key := int64(handle)
	if key <= 0 {
		return C.int(weldCodeErrHandle)
	}
	sid, code := weldCheckSeq(seq)
	if code != C.int(weldCodeOK) {
		return code
	}
	guidBytes, code := weldReadGuid(guid)
	if code != C.int(weldCodeOK) {
		return code
	}
	base, code := weldReadCString(idlBase)
	if code != C.int(weldCodeOK) {
		return code
	}
	if base == "" {
		return C.int(weldCodeErrInternal)
	}
	payload, code := weldReadBytes(req, reqLen)
	if code != C.int(weldCodeOK) {
		return code
	}
	rkey := weldSvcReplyKey{svc: key, seq: sid}
	bridgeID, service, typeHash, backend, code := weldLoadRTPSAcquire(key, sid)
	if code != C.int(weldCodeOK) {
		return code
	}
	if backend == nil {
		return C.int(weldCodeErrInternal)
	}
	wire, err := dds.MarshalServiceRequest(payload, guidBytes, uint64(sid), true)
	if err != nil {
		return C.int(weldCodeErrInternal)
	}
	if code := weldSendServiceRTPS(backend, service, base, typeHash, wire); code != C.int(weldCodeOK) {
		return code
	}
	return weldParkSplitRTPS(key, rkey, weldRTPSWaiter{bridgeID: bridgeID, service: service, base: base, guid: guidBytes})
}

// weldLoadRTPSAcquire loads the service leg state for an RTPS send.
//
// It validates the handle and rejects duplicate sequences in one place.
// It never panics.
func weldLoadRTPSAcquire(key, sid int64) (int64, string, string, *dds.Backend, C.int) {
	weldMu.Lock()
	defer weldMu.Unlock()
	e, ok := weldLoadSvcLocked(key)
	if !ok {
		return 0, "", "", nil, C.int(weldCodeErrHandle)
	}
	if _, dup := weldSplitRTPS[weldSvcReplyKey{svc: key, seq: sid}]; dup {
		return 0, "", "", nil, C.int(weldCodeErrInternal)
	}
	typeHash := ""
	if stored, reported := weldSvcTypes[key]; reported {
		typeHash = stored.typeHash
	}
	return e.bridgeID, e.name, typeHash, weldRTPS[e.bridgeID], C.int(weldCodeOK)
}

// weldParkSplitRTPS parks one RTPS reply waiter.
//
// It reports the handle code when the service vanished mid-send. It
// never panics.
func weldParkSplitRTPS(key int64, rkey weldSvcReplyKey, waiter weldRTPSWaiter) C.int {
	weldMu.Lock()
	defer weldMu.Unlock()
	if _, ok := weldServices[key]; !ok {
		return C.int(weldCodeErrHandle)
	}
	weldSplitRTPS[rkey] = waiter
	return C.int(weldCodeOK)
}

// weldSendServiceRTPS declares the client endpoints and publishes one request.
//
// The declare plus publish run under separate bounded contexts so a slow
// announce cannot consume the publish bound. Backend errors wrap with
// context for the wrapcheck lint. It never panics.
func weldSendServiceRTPS(backend *dds.Backend, service, base, typeHash string, wire []byte) C.int {
	declareCtx, declareCancel := context.WithTimeout(context.Background(), weldRTPSSubscribeTimeout)
	defer declareCancel()
	err := weldDeclareServiceClient(declareCtx, backend, service, base, typeHash)
	if err != nil {
		return C.int(weldCodeErrInternal)
	}
	publishCtx, publishCancel := context.WithTimeout(context.Background(), weldRTPSPublishTimeout)
	defer publishCancel()
	err = backend.PublishService(publishCtx, service, base, dds.ServiceRoleClient, wire, typeHash)
	if err != nil {
		return C.int(weldCodeErrInternal)
	}
	return C.int(weldCodeOK)
}

// weld_ffi_service_take_reply polls the reply topic with the split bound.
//
// The overflow stash is scanned before the wire so has_response peeks
// stay consumable here; wire samples for other consumers park back in
// the overflow. An oversized match parks in the short stash and reports
// -4 without consuming the waiter. Expiry reports 1 with 0 in outN so
// the C layer keeps the pending entry and retries. Unknown handles or
// sequences report -2 and guid mismatches report -2. It never panics.
//
//export weld_ffi_service_take_reply
func weld_ffi_service_take_reply(handle C.longlong, seq C.longlong, guid *C.char, idlBase *C.char, respBuf *C.char, respBufLen C.int, outN *C.int) (rc C.int) {
	defer func() {
		r := recover()
		if r != nil {
			rc = C.int(weldCodeErrInternal)
		}
	}()
	key, sid, guidBytes, base, code := weldCheckTakeArgs(handle, seq, guid, idlBase, respBuf, respBufLen, outN)
	if code != C.int(weldCodeOK) {
		return code
	}
	rkey := weldSvcReplyKey{svc: key, seq: sid}
	_, backend, bridgeID, stashed, haveStash, code := weldLoadSplitRTPSFull(key, rkey, guidBytes, base)
	if code != C.int(weldCodeOK) {
		return code
	}
	if haveStash {
		return weldDeliverSplitRTPS(rkey, respBuf, respBufLen, outN, stashed)
	}
	if backend == nil {
		return C.int(weldCodeErrInternal)
	}
	if resp, found := weldTakeOverflowReply(bridgeID, guidBytes, uint64(sid)); found {
		return weldDeliverSplitRTPSOrStash(rkey, respBuf, respBufLen, outN, resp)
	}
	resp, code := weldTakeReplyWire(backend, bridgeID, guidBytes, uint64(sid))
	if code == C.int(weldCodeEmpty) {
		*outN = C.int(0)
		return code
	}
	if code != C.int(weldCodeOK) {
		return code
	}
	return weldDeliverSplitRTPSOrStash(rkey, respBuf, respBufLen, outN, resp)
}

// weldTakeReplyWire polls the reply topic until the split bound.
//
// Non-matching samples park in the overflow for their own consumer and
// polling continues; transport errors fail loudly while bound expiry
// reports empty. It never panics.
func weldTakeReplyWire(backend *dds.Backend, bridgeID int64, guid [8]byte, seq uint64) ([]byte, C.int) {
	deadline := time.Now().Add(weldSplitPollBound)
	for time.Now().Before(deadline) {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			break
		}
		ctx, cancel := context.WithTimeout(context.Background(), remaining)
		resp, matched, err := weldPollRTPSReply(ctx, backend, bridgeID, guid, seq)
		cancel()
		if matched {
			return resp, C.int(weldCodeOK)
		}
		if err != nil {
			if ctx.Err() != nil {
				break
			}
			return nil, C.int(weldCodeErrInternal)
		}
	}
	return nil, C.int(weldCodeEmpty)
}

// weldCheckTakeArgs validates take_reply arguments in one place.
//
// It returns the parsed handle, sequence, guid, and IDL base on success.
// It never panics.
func weldCheckTakeArgs(handle C.longlong, seq C.longlong, guid *C.char, idlBase *C.char, respBuf *C.char, respBufLen C.int, outN *C.int) (int64, int64, [8]byte, string, C.int) {
	var empty [8]byte
	key := int64(handle)
	if key <= 0 {
		return 0, 0, empty, "", C.int(weldCodeErrHandle)
	}
	sid, code := weldCheckSeq(seq)
	if code != C.int(weldCodeOK) {
		return 0, 0, empty, "", code
	}
	if outN == nil || idlBase == nil {
		return 0, 0, empty, "", C.int(weldCodeErrNull)
	}
	if code := weldCheckReplyArgs(respBuf, respBufLen, outN); code != C.int(weldCodeOK) {
		return 0, 0, empty, "", code
	}
	guidBytes, code := weldReadGuid(guid)
	if code != C.int(weldCodeOK) {
		return 0, 0, empty, "", code
	}
	base, code := weldReadCString(idlBase)
	if code != C.int(weldCodeOK) {
		return 0, 0, empty, "", code
	}
	return key, sid, guidBytes, base, C.int(weldCodeOK)
}

// weldLoadSplitRTPSFull loads a parked RTPS waiter plus its poll state.
//
// It reads the base itself so the take export shares one gate with the
// peek export. It never panics.
func weldLoadSplitRTPSFull(key int64, rkey weldSvcReplyKey, guid [8]byte, base string) (weldRTPSWaiter, *dds.Backend, int64, []byte, bool, C.int) {
	weldMu.Lock()
	defer weldMu.Unlock()
	if _, ok := weldServices[key]; !ok {
		return weldRTPSWaiter{}, nil, 0, nil, false, C.int(weldCodeErrHandle)
	}
	waiter, parked := weldSplitRTPS[rkey]
	if !parked || waiter.guid != guid || waiter.base != base {
		return weldRTPSWaiter{}, nil, 0, nil, false, C.int(weldCodeErrHandle)
	}
	stashed, haveStash := weldSplitRTPSStash[rkey]
	return waiter, weldRTPS[waiter.bridgeID], waiter.bridgeID, stashed, haveStash, C.int(weldCodeOK)
}

// weldDeliverSplitRTPS copies one stashed RTPS reply and forgets its waiter.
//
// It never panics.
func weldDeliverSplitRTPS(rkey weldSvcReplyKey, respBuf *C.char, respBufLen C.int, outN *C.int, stashed []byte) C.int {
	code := weldWriteReplyToC(respBuf, respBufLen, outN, stashed)
	if code != C.int(weldCodeOK) {
		return code
	}
	weldMu.Lock()
	weldForgetSplitKey(rkey)
	weldMu.Unlock()
	return C.int(weldCodeOK)
}

// weldDeliverSplitRTPSOrStash copies one RTPS reply or parks it when short.
//
// A short reply parks in the stash without consuming the waiter so the
// C layer retries with a larger buffer. It never panics.
func weldDeliverSplitRTPSOrStash(rkey weldSvcReplyKey, respBuf *C.char, respBufLen C.int, outN *C.int, resp []byte) C.int {
	if code := weldWriteReplyToC(respBuf, respBufLen, outN, resp); code != C.int(weldCodeOK) {
		weldMu.Lock()
		if _, still := weldSplitRTPS[rkey]; still {
			weldSplitRTPSStash[rkey] = resp
		}
		weldMu.Unlock()
		return code
	}
	weldMu.Lock()
	weldForgetSplitKey(rkey)
	weldMu.Unlock()
	return C.int(weldCodeOK)
}

// weldDeclareServiceServer declares the RTPS server endpoints for one service.
//
// The request reader carries take requests back to serve_rtps and the
// reply writer carries respond_rtps publishes; both declares are
// idempotent. It never panics.
func weldDeclareServiceServer(ctx context.Context, backend *dds.Backend, service, idlBase, typeHash string) error {
	err := backend.SubscribeService(ctx, service, idlBase, dds.ServiceRoleServer, typeHash)
	if err != nil {
		return fmt.Errorf("weld: declare service server subscribe: %w", err)
	}
	err = backend.DeclareServiceWriter(ctx, service, idlBase, dds.ServiceRoleServer, typeHash)
	if err != nil {
		return fmt.Errorf("weld: declare service server writer: %w", err)
	}
	return nil
}

// weldServeRTPSLoad returns the service name plus backend for serve_rtps.
//
// It validates the handle and reads the stored IDL base plus type hash.
// It never panics.
func weldServeRTPSLoad(key int64) (int64, string, string, string, *dds.Backend, C.int) {
	weldMu.Lock()
	defer weldMu.Unlock()
	e, ok := weldLoadSvcLocked(key)
	if !ok {
		return 0, "", "", "", nil, C.int(weldCodeErrHandle)
	}
	typeHash := ""
	base := ""
	if stored, reported := weldSvcTypes[key]; reported {
		typeHash = stored.typeHash
		base = stored.typeName
	}
	return e.bridgeID, e.name, base, typeHash, weldRTPS[e.bridgeID], C.int(weldCodeOK)
}

// weldServeRTPSPoll polls the wire once for one request and stashes the rest.
//
// A match for the service returns its payload plus guid plus seq. Anything
// else parks in the bridge overflow for its own consumer. Transport errors
// report err so the caller can tell a quiet wire from a broken one. It
// never panics.
func weldServeRTPSPoll(ctx context.Context, backend *dds.Backend, bridgeID int64, service string) ([]byte, [8]byte, uint64, bool, error) {
	msg, err := backend.TakeBytes(ctx)
	if err != nil {
		return nil, [8]byte{}, 0, false, fmt.Errorf("weld: poll rtps request: %w", err)
	}
	if msg.TopicName != "" && msg.TopicName != service {
		weldStashRTPSOverflow(bridgeID, msg)
		return nil, [8]byte{}, 0, false, nil
	}
	var metrics types.Metrics
	inner, guid, seq, _, umErr := dds.UnmarshalServiceRequest(msg.Payload, &metrics)
	if umErr != nil {
		// Malformed service packets park for no one; drop and continue.
		return nil, [8]byte{}, 0, false, nil //nolint:nilerr // malformed drops are not errors
	}
	return append([]byte(nil), inner...), guid, seq, true, nil
}

// weldServeRTPSDequeue returns one stashed request when present.
func weldServeRTPSDequeue(key int64, reqBuf *C.char, reqBufLen C.int, outN *C.int, outSeq *C.longlong, outGuid *C.char) ([]byte, [8]byte, uint64, bool, C.int) {
	var empty [8]byte
	weldMu.Lock()
	stashed := weldServeRTPSStash[key]
	haveStash := len(stashed) > 0
	var first weldServeRTPSReq
	if haveStash {
		first = stashed[0]
		weldServeRTPSStash[key] = stashed[1:]
		if len(weldServeRTPSStash[key]) == 0 {
			delete(weldServeRTPSStash, key)
		}
	}
	weldMu.Unlock()
	if !haveStash {
		return nil, empty, 0, false, C.int(weldCodeOK)
	}
	if len(first.payload) > int(reqBufLen) {
		weldMu.Lock()
		weldServeRTPSStash[key] = append([]weldServeRTPSReq{first}, weldServeRTPSStash[key]...)
		weldMu.Unlock()
		return nil, empty, 0, false, C.int(weldCodeErrShort)
	}
	if code := weldCopyToC(reqBuf, reqBufLen, first.payload); code != C.int(weldCodeOK) {
		weldMu.Lock()
		weldServeRTPSStash[key] = append([]weldServeRTPSReq{first}, weldServeRTPSStash[key]...)
		weldMu.Unlock()
		return nil, empty, 0, false, code
	}
	if outGuid != nil {
		dst := unsafe.Slice((*byte)(unsafe.Pointer(outGuid)), 8)
		copy(dst, first.guid[:])
	}
	*outN = C.int(len(first.payload))
	*outSeq = C.longlong(first.seq)
	return first.payload, first.guid, first.seq, true, C.int(weldCodeOK)
}

// weldCheckServeRTPSArgs validates serve_rtps arguments in one place.
//
// It returns the parsed handle plus base. It never panics.
func weldCheckServeRTPSArgs(handle C.longlong, idlBase *C.char, reqBuf *C.char, reqBufLen C.int, outN *C.int, outSeq *C.longlong, outGuid *C.char) (int64, string, C.int) {
	key := int64(handle)
	if key <= 0 {
		return 0, "", C.int(weldCodeErrHandle)
	}
	if outN == nil || outSeq == nil || outGuid == nil || idlBase == nil {
		return 0, "", C.int(weldCodeErrNull)
	}
	if reqBufLen < 0 {
		return 0, "", C.int(weldCodeErrNull)
	}
	if reqBufLen > 0 && reqBuf == nil {
		return 0, "", C.int(weldCodeErrNull)
	}
	base, code := weldReadCString(idlBase)
	if code != C.int(weldCodeOK) {
		return 0, "", code
	}
	if base == "" {
		return 0, "", C.int(weldCodeErrInternal)
	}
	return key, base, C.int(weldCodeOK)
}

// weld_ffi_service_serve_rtps dequeues one RTPS request without blocking long.
//
// The server endpoints declare first so peers see the server before DATA.
// The overflow stash is scanned before the wire so has_request peeks stay
// consumable here; wire samples for other services park back in the
// overflow. An oversized match parks in the short stash and reports -4
// without consuming. Expiry reports 1 with 0 in outN so the C layer keeps
// polling. Unknown handles report -2. It never panics.
//
//export weld_ffi_service_serve_rtps
func weld_ffi_service_serve_rtps(handle C.longlong, idlBase *C.char, reqBuf *C.char, reqBufLen C.int, outN *C.int, outSeq *C.longlong, outGuid *C.char) (rc C.int) {
	defer func() {
		r := recover()
		if r != nil {
			rc = C.int(weldCodeErrInternal)
		}
	}()
	key, base, code := weldCheckServeRTPSArgs(handle, idlBase, reqBuf, reqBufLen, outN, outSeq, outGuid)
	if code != C.int(weldCodeOK) {
		return code
	}
	if _, _, _, done, code := weldServeRTPSDequeue(key, reqBuf, reqBufLen, outN, outSeq, outGuid); done || code != C.int(weldCodeOK) {
		return code
	}
	bridgeID, service, _, typeHash, backend, bcode := weldServeRTPSLoad(key)
	if bcode != C.int(weldCodeOK) {
		return bcode
	}
	// Replay overflow first so has_request peeks stay consumable and
	// mock transports without a backend still dequeue.
	if resp, found := weldTakeOverflowServiceRequest(bridgeID, service); found {
		return weldDeliverServeRTPS(key, reqBuf, reqBufLen, outN, outSeq, outGuid, resp.payload, resp.guid, resp.seq)
	}
	if backend == nil {
		*outN = C.int(0)
		*outSeq = C.longlong(0)
		return C.int(weldCodeErrInternal)
	}
	if code := weldDeclareServeAndPoll(key, backend, bridgeID, service, base, typeHash, reqBuf, reqBufLen, outN, outSeq, outGuid); code != C.int(weldCodeEmpty) {
		return code
	}
	*outN = C.int(0)
	*outSeq = C.longlong(0)
	return C.int(weldCodeEmpty)
}

func weldDeclareServeAndPoll(key int64, backend *dds.Backend, bridgeID int64, service, base, typeHash string, reqBuf *C.char, reqBufLen C.int, outN *C.int, outSeq *C.longlong, outGuid *C.char) C.int {
	declareCtx, declareCancel := context.WithTimeout(context.Background(), weldRTPSSubscribeTimeout)
	err := weldDeclareServiceServer(declareCtx, backend, service, base, typeHash)
	declareCancel()
	if err != nil {
		return C.int(weldCodeErrInternal)
	}
	deadline := time.Now().Add(weldSplitPollBound)
	for time.Now().Before(deadline) {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			break
		}
		ctx, cancel := context.WithTimeout(context.Background(), remaining)
		payload, guid, seq, matched, perr := weldServeRTPSPoll(ctx, backend, bridgeID, service)
		cancel()
		if matched {
			return weldDeliverServeRTPS(key, reqBuf, reqBufLen, outN, outSeq, outGuid, payload, guid, seq)
		}
		if perr != nil {
			if ctx.Err() != nil {
				break
			}
			return C.int(weldCodeErrInternal)
		}
	}
	return C.int(weldCodeEmpty)
}

// weldTakeOverflowServiceRequest pops one stashed wire request for a service.
func weldTakeOverflowServiceRequest(bridgeID int64, service string) (weldServeRTPSReq, bool) {
	weldMu.Lock()
	defer weldMu.Unlock()
	queue := weldRTPSOverflow[bridgeID]
	for i, msg := range queue {
		if msg.TopicName != "" && msg.TopicName != service {
			continue
		}
		var metrics types.Metrics
		inner, guid, seq, _, err := dds.UnmarshalServiceRequest(msg.Payload, &metrics)
		if err != nil {
			continue
		}
		queue = append(queue[:i], queue[i+1:]...)
		if len(queue) == 0 {
			delete(weldRTPSOverflow, bridgeID)
		} else {
			weldRTPSOverflow[bridgeID] = queue
		}
		return weldServeRTPSReq{payload: append([]byte(nil), inner...), guid: guid, seq: seq}, true
	}
	return weldServeRTPSReq{}, false
}

// weldDeliverServeRTPS copies one RTPS request or parks it when short.
func weldDeliverServeRTPS(key int64, reqBuf *C.char, reqBufLen C.int, outN *C.int, outSeq *C.longlong, outGuid *C.char, payload []byte, guid [8]byte, seq uint64) C.int {
	if len(payload) > int(reqBufLen) {
		weldMu.Lock()
		weldServeRTPSStash[key] = append([]weldServeRTPSReq{{payload: payload, guid: guid, seq: seq}}, weldServeRTPSStash[key]...)
		weldMu.Unlock()
		return C.int(weldCodeErrShort)
	}
	if code := weldCopyToC(reqBuf, reqBufLen, payload); code != C.int(weldCodeOK) {
		weldMu.Lock()
		weldServeRTPSStash[key] = append([]weldServeRTPSReq{{payload: payload, guid: guid, seq: seq}}, weldServeRTPSStash[key]...)
		weldMu.Unlock()
		return code
	}
	dst := unsafe.Slice((*byte)(unsafe.Pointer(outGuid)), 8)
	copy(dst, guid[:])
	*outN = C.int(len(payload))
	*outSeq = C.longlong(seq)
	return C.int(weldCodeOK)
}

// weldCheckRespondRTPSArgs validates respond_rtps arguments in one place.
//
// It returns the parsed handle, guid, sequence, base, and payload.
// It never panics.
func weldCheckRespondRTPSArgs(handle C.longlong, guid *C.char, seq C.longlong, idlBase *C.char, resp *C.char, respLen C.int) (int64, [8]byte, int64, string, []byte, C.int) {
	var empty [8]byte
	key := int64(handle)
	if key <= 0 {
		return 0, empty, 0, "", nil, C.int(weldCodeErrHandle)
	}
	sid, code := weldCheckSeq(seq)
	if code != C.int(weldCodeOK) {
		return 0, empty, 0, "", nil, code
	}
	guidBytes, code := weldReadGuid(guid)
	if code != C.int(weldCodeOK) {
		return 0, empty, 0, "", nil, code
	}
	base, code := weldReadCString(idlBase)
	if code != C.int(weldCodeOK) {
		return 0, empty, 0, "", nil, code
	}
	if base == "" {
		return 0, empty, 0, "", nil, C.int(weldCodeErrInternal)
	}
	payload, code := weldReadBytes(resp, respLen)
	if code != C.int(weldCodeOK) {
		return 0, empty, 0, "", nil, code
	}
	return key, guidBytes, sid, base, payload, C.int(weldCodeOK)
}

func weldPublishRespondRTPS(backend *dds.Backend, service, base, typeHash string, payload []byte, guid [8]byte, sid int64) C.int {
	wire, err := dds.MarshalServiceReply(payload, guid, uint64(sid), true)
	if err != nil {
		return C.int(weldCodeErrInternal)
	}
	declareCtx, declareCancel := context.WithTimeout(context.Background(), weldRTPSSubscribeTimeout)
	err = weldDeclareServiceServer(declareCtx, backend, service, base, typeHash)
	declareCancel()
	if err != nil {
		return C.int(weldCodeErrInternal)
	}
	publishCtx, publishCancel := context.WithTimeout(context.Background(), weldRTPSPublishTimeout)
	defer publishCancel()
	err = backend.PublishService(publishCtx, service, base, dds.ServiceRoleServer, wire, typeHash)
	if err != nil {
		return C.int(weldCodeErrInternal)
	}
	return C.int(weldCodeOK)
}

// weld_ffi_service_respond_rtps publishes one RTPS reply.
//
// guid plus seq must match a request previously returned by
// weld_ffi_service_serve_rtps for the same handle. It declares the server
// endpoints and publishes through the server role. It returns 0 on
// success, -1 for NULL pointers, -2 for unknown handles, and -3 for
// publish failures. It never panics.
//
//export weld_ffi_service_respond_rtps
func weld_ffi_service_respond_rtps(handle C.longlong, guid *C.char, seq C.longlong, idlBase *C.char, resp *C.char, respLen C.int) (rc C.int) {
	defer func() {
		r := recover()
		if r != nil {
			rc = C.int(weldCodeErrInternal)
		}
	}()
	key, guidBytes, sid, base, payload, code := weldCheckRespondRTPSArgs(handle, guid, seq, idlBase, resp, respLen)
	if code != C.int(weldCodeOK) {
		return code
	}
	_, service, _, typeHash, backend, bcode := weldServeRTPSLoad(key)
	if bcode != C.int(weldCodeOK) {
		return bcode
	}
	if backend == nil {
		return C.int(weldCodeErrInternal)
	}
	return weldPublishRespondRTPS(backend, service, base, typeHash, payload, guidBytes, sid)
}

// weldCheckHasRequestRTPSArgs validates has_request_rtps arguments.
//
// It returns the parsed handle plus base. It never panics.
func weldCheckHasRequestRTPSArgs(handle C.longlong, idlBase *C.char, outN *C.int) (int64, string, C.int) {
	key := int64(handle)
	if key <= 0 {
		return 0, "", C.int(weldCodeErrHandle)
	}
	if outN == nil || idlBase == nil {
		return 0, "", C.int(weldCodeErrNull)
	}
	base, code := weldReadCString(idlBase)
	if code != C.int(weldCodeOK) {
		return 0, "", code
	}
	if base == "" {
		return 0, "", C.int(weldCodeErrInternal)
	}
	return key, base, C.int(weldCodeOK)
}

// weld_ffi_service_has_request_rtps counts RTPS requests servable by serve_rtps.
//
// The short stash plus the wire overflow report ready without consuming.
// Otherwise one short wire poll runs; a match parks in the serve stash
// for the consuming serve so C state never loses it. It returns 0 on
// success with the count in outN, -1 for NULL pointers, and -2 for
// unknown handles. It never panics.
//
//export weld_ffi_service_has_request_rtps
func weld_ffi_service_has_request_rtps(handle C.longlong, idlBase *C.char, outN *C.int) (rc C.int) {
	defer func() {
		r := recover()
		if r != nil {
			rc = C.int(weldCodeErrInternal)
		}
	}()
	key, _, code := weldCheckHasRequestRTPSArgs(handle, idlBase, outN)
	if code != C.int(weldCodeOK) {
		return code
	}
	weldMu.Lock()
	n := len(weldServeRTPSStash[key])
	weldMu.Unlock()
	if n > 0 {
		*outN = C.int(n)
		return C.int(weldCodeOK)
	}
	bridgeID, service, _, _, backend, bcode := weldServeRTPSLoad(key)
	if bcode != C.int(weldCodeOK) {
		return bcode
	}
	if _, found := weldPeekOverflowServiceRequest(bridgeID, service); found {
		*outN = C.int(1)
		return C.int(weldCodeOK)
	}
	if backend == nil {
		*outN = C.int(0)
		return C.int(weldCodeOK)
	}
	ctx, cancel := context.WithTimeout(context.Background(), weldPollBound)
	defer cancel()
	payload, guid, seq, matched, err := weldServeRTPSPoll(ctx, backend, bridgeID, service)
	if err != nil || !matched {
		*outN = C.int(0)
		return C.int(weldCodeOK)
	}
	weldMu.Lock()
	weldServeRTPSStash[key] = append(weldServeRTPSStash[key], weldServeRTPSReq{payload: payload, guid: guid, seq: seq})
	n = len(weldServeRTPSStash[key])
	weldMu.Unlock()
	*outN = C.int(n)
	return C.int(weldCodeOK)
}

// weldPeekOverflowServiceRequest reports a stashed wire request for a service.
func weldPeekOverflowServiceRequest(bridgeID int64, service string) (weldServeRTPSReq, bool) {
	weldMu.Lock()
	defer weldMu.Unlock()
	for _, msg := range weldRTPSOverflow[bridgeID] {
		if msg.TopicName != "" && msg.TopicName != service {
			continue
		}
		var metrics types.Metrics
		inner, guid, seq, _, err := dds.UnmarshalServiceRequest(msg.Payload, &metrics)
		if err != nil {
			continue
		}
		return weldServeRTPSReq{payload: append([]byte(nil), inner...), guid: guid, seq: seq}, true
	}
	return weldServeRTPSReq{}, false
}

// weld_ffi_sub_set_content_filter records one content filter for a subscription.
//
// The expression plus parameters compile through dds.CompileContentFilter
// now, so invalid expressions fail loudly with -3 instead of filtering
// nothing. A compiled filter attaches to the backend reader when the
// backend is attached and the subscription type is known; attach or
// declare failures stay best effort like the reader declare path, and
// in-memory takes filter through weldFilterMatchesInMemory with the
// same compiled filter. It
// returns 0 on success, -1 for NULL pointers, and -2 for an unknown
// handle. It never panics.
//
//export weld_ffi_sub_set_content_filter
func weld_ffi_sub_set_content_filter(handle C.longlong, expr *C.char, params *C.char, nparams C.int) (rc C.int) {
	defer func() {
		r := recover()
		if r != nil {
			rc = C.int(weldCodeErrInternal)
		}
	}()
	key := int64(handle)
	if key <= 0 {
		return C.int(weldCodeErrHandle)
	}
	if expr == nil {
		return C.int(weldCodeErrNull)
	}
	exprStr, code := weldReadCString(expr)
	if code != C.int(weldCodeOK) {
		return code
	}
	if exprStr == "" {
		return C.int(weldCodeErrNull)
	}
	paramList, code := weldReadFilterParams(params, nparams)
	if code != C.int(weldCodeOK) {
		return code
	}
	compiled, err := dds.CompileContentFilter(exprStr, paramList...)
	if err != nil {
		return C.int(weldCodeErrInternal)
	}
	weldMu.Lock()
	e, ok := weldSubs[key]
	if !ok {
		weldMu.Unlock()
		return C.int(weldCodeErrHandle)
	}
	weldSubFilters[key] = weldSubFilter{expr: exprStr, params: paramList, filter: compiled}
	st, hasType := weldSubTypes[key]
	backend := weldRTPS[e.bridgeID]
	topic := e.sub.Topic
	weldMu.Unlock()
	typeName := ""
	typeHash := ""
	if hasType {
		typeName = st.typeName
		typeHash = st.typeHash
	}
	return weldAttachContentFilter(backend, topic, typeName, typeHash, compiled.Expression())
}

// weldFilterMatchesInMemory reports whether one in-memory payload passes the filter.
//
// It uses the same compiled ContentFilter as the wire path. A missing
// filter passes. Unknown subscription types or unregistered descriptors
// pass fail-open with documentation so typeless takes keep working;
// wire takes still filter through the backend once the type is known.
// Eval errors drop fail-closed. It never panics.
func weldFilterMatchesInMemoryLocked(key int64, payload []byte) bool {
	// Caller must hold weldMu.
	entry, hasFilter := weldSubFilters[key]
	st, hasType := weldSubTypes[key]
	return weldFilterMatchesWith(entry, hasFilter, st, hasType, payload)
}

func weldFilterMatchesWith(entry weldSubFilter, hasFilter bool, st weldSubType, hasType bool, payload []byte) bool {
	if !hasFilter || entry.filter == nil {
		return true
	}
	if !hasType || st.typeName == "" {
		return true
	}
	ns, name := splitIDLType(st.typeName)
	if ns == "" || name == "" {
		return true
	}
	desc, err := types.LookupMessageMembers(ns, name)
	if err != nil || desc == nil {
		return true
	}
	matched, err := entry.filter.Evaluate(payload, desc)
	if err != nil {
		return false
	}
	return matched
}

func weldFilterMatchesInMemory(key int64, payload []byte) bool {
	weldMu.Lock()
	entry, hasFilter := weldSubFilters[key]
	st, hasType := weldSubTypes[key]
	weldMu.Unlock()
	return weldFilterMatchesWith(entry, hasFilter, st, hasType, payload)
}

// splitIDLType splits an IDL name into namespace plus base.
func splitIDLType(idl string) (string, string) {
	idx := -1
	for i := len(idl) - 2; i >= 0; i-- {
		if idl[i] == '_' && idl[i+1] == '_' {
			idx = i
			break
		}
	}
	if idx <= 0 || idx+2 >= len(idl) {
		return "", ""
	}
	return idl[:idx], idl[idx+2:]
}

// weldAttachContentFilter attaches a compiled filter to the backend reader.
//
// A missing backend or an unknown subscription type skips the attach:
// the filter stays recorded for get_content_filter while in-memory
// takes filter through weldFilterMatchesInMemory and wire takes filter
// through the backend once the type is known. Attach failures report
// internal. It never panics.
func weldAttachContentFilter(backend *dds.Backend, topic, typeName, typeHash, bound string) C.int {
	if backend == nil || typeName == "" {
		return C.int(weldCodeOK)
	}
	declareCtx, declareCancel := context.WithTimeout(context.Background(), weldRTPSSubscribeTimeout)
	defer declareCancel()
	err := backend.SubscribeWithContentFilter(declareCtx, topic, typeName, typeHash, bound)
	if err != nil {
		return C.int(weldCodeErrInternal)
	}
	return C.int(weldCodeOK)
}

// weldCheckRemoteArgs validates bridge plus topic plus role for remote queries.
//
// It returns the bridge id and the topic name on success. It never panics.
func weldCheckRemoteArgs(bridge C.longlong, topic *C.char, role C.int) (int64, string, C.int) {
	if topic == nil {
		return 0, "", C.int(weldCodeErrNull)
	}
	if int(role) < weldRoleWriter || int(role) > weldRoleClient {
		return 0, "", C.int(weldCodeErrNull)
	}
	bkey := int64(bridge)
	if bkey <= 0 {
		return 0, "", C.int(weldCodeErrHandle)
	}
	name, code := weldReadCString(topic)
	if code != C.int(weldCodeOK) {
		return 0, "", code
	}
	weldMu.Lock()
	_, ok := weldBridges[bkey]
	weldMu.Unlock()
	if !ok {
		return 0, "", C.int(weldCodeErrHandle)
	}
	return bkey, name, C.int(weldCodeOK)
}

// weld_ffi_remote_ep_count counts live SEDP remotes for a topic plus role.
//
// Roles match the WELD_FILTER_ROLE_* enum: 0 writer, 1 reader, 2
// server, 3 client. An absent backend contributes zero. It returns 0 on
// success, -1 for NULL pointers or a bad role, and -2 for an unknown
// bridge. It never panics.
//
//export weld_ffi_remote_ep_count
func weld_ffi_remote_ep_count(bridge C.longlong, topic *C.char, role C.int, out *C.longlong) (rc C.int) {
	defer func() {
		r := recover()
		if r != nil {
			rc = C.int(weldCodeErrInternal)
		}
	}()
	if out == nil {
		return C.int(weldCodeErrNull)
	}
	bkey, name, code := weldCheckRemoteArgs(bridge, topic, role)
	if code != C.int(weldCodeOK) {
		return code
	}
	*out = C.longlong(len(weldRemoteEpsFor(bkey, name, int(role))))
	return C.int(weldCodeOK)
}

// weld_ffi_remote_ep_at copies one live SEDP remote by index.
//
// typeBuf receives the NUL terminated IDL type with the DDS name as
// fallback. gidBuf receives the raw 16 byte SEDP GUID without a
// terminator. A short type buffer reports -4 and an index past the end
// reports -2; the C layer skips either quietly. Return codes match the
// count export. It never panics.
//
//export weld_ffi_remote_ep_at
func weld_ffi_remote_ep_at(bridge C.longlong, topic *C.char, role C.int, index C.longlong, typeBuf *C.char, typeLen C.int, gidBuf *C.char, gidLen C.int) (rc C.int) {
	defer func() {
		r := recover()
		if r != nil {
			rc = C.int(weldCodeErrInternal)
		}
	}()
	if typeBuf == nil || gidBuf == nil {
		return C.int(weldCodeErrNull)
	}
	if int64(index) < 0 {
		return C.int(weldCodeErrHandle)
	}
	bkey, name, code := weldCheckRemoteArgs(bridge, topic, role)
	if code != C.int(weldCodeOK) {
		return code
	}
	eps := weldRemoteEpsFor(bkey, name, int(role))
	if int64(index) >= int64(len(eps)) {
		return C.int(weldCodeErrHandle)
	}
	ep := eps[int(index)]
	return weldWriteRemoteEp(ep, typeBuf, typeLen, gidBuf, gidLen)
}

// weldWriteRemoteEp copies one remote endpoint into C buffers.
//
// typeBuf receives the NUL terminated IDL type with the DDS name as
// fallback. gidBuf receives the raw 16 byte SEDP GUID without a
// terminator. It never panics.
func weldWriteRemoteEp(ep dds.SEDPEndpoint, typeBuf *C.char, typeLen C.int, gidBuf *C.char, gidLen C.int) C.int {
	if code := weldWriteCString(typeBuf, typeLen, weldGraphType(ep.TypeIDL, ep.TypeDDS)); code != C.int(weldCodeOK) {
		return code
	}
	if int(gidLen) < 16 {
		return C.int(weldCodeErrShort)
	}
	var raw [16]byte
	copy(raw[0:12], ep.ID.Prefix[:])
	copy(raw[12:16], ep.ID.Entity[:])
	dst := unsafe.Slice((*byte)(unsafe.Pointer(gidBuf)), int(gidLen))
	copy(dst, raw[:])
	return C.int(weldCodeOK)
}

// weld_ffi_report_liveliness reports local endpoint liveliness for a bridge.
//
// participant names the asserting participant with the joined domain,
// alive carries 1 for live and 0 for dispose, and generation is caller
// owned and must increase per participant: repeating a generation with
// the same alive value is idempotent and any other reuse is stale. It
// returns 0 on success, -1 for NULL pointers, bad domains, or alive
// values outside 0 and 1, -2 for an unknown bridge, and -3 when the
// bridge reports an error (not joined, closed, or stale). It never
// panics.
//
// C wiring note: rmw_node_assert_liveliness and
// rmw_publisher_assert_liveliness are validation-only stubs with no FFI
// call yet. Wire them here with the node name, the context domain,
// alive 1, and a process-wide monotonic counter for generation.
//
//export weld_ffi_report_liveliness
func weld_ffi_report_liveliness(bridge C.longlong, participant *C.char, domain C.int, alive C.int, generation C.ulonglong) (rc C.int) {
	defer func() {
		r := recover()
		if r != nil {
			rc = C.int(weldCodeErrInternal)
		}
	}()
	bkey := int64(bridge)
	if bkey <= 0 {
		return C.int(weldCodeErrHandle)
	}
	if participant == nil {
		return C.int(weldCodeErrNull)
	}
	if domain < 0 {
		return C.int(weldCodeErrNull)
	}
	if int(alive) != 0 && int(alive) != 1 {
		return C.int(weldCodeErrNull)
	}
	name, code := weldReadCString(participant)
	if code != C.int(weldCodeOK) {
		return code
	}
	if name == "" {
		return C.int(weldCodeErrInternal)
	}
	weldMu.Lock()
	b, ok := weldBridges[bkey]
	weldMu.Unlock()
	if !ok {
		return C.int(weldCodeErrHandle)
	}
	report := rmwbridge.LivelinessReport{
		ParticipantName: name,
		DomainID:        uint32(domain),
		Alive:           int(alive) == 1,
		Generation:      uint64(generation),
	}
	err := b.ReportLiveliness(context.Background(), report)
	if err != nil {
		return C.int(weldCodeErrInternal)
	}
	return C.int(weldCodeOK)
}
