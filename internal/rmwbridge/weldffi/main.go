// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

// Package main exposes a c-shared entry point for the RMW FFI.
//
// It mirrors the C ABI of internal/rmwbridge exports with weld_ffi_*
// symbols and delegates only to the public rmwbridge API with its own
// mutex guarded handle registry. It never reuses ffi.go internals.
package main

/*
#include <stdlib.h>
#include <string.h>
*/
import "C"

import (
	"context"
	"fmt"
	"sync"
	"time"
	"unsafe"

	"github.com/podomy/weld/internal/rmwbridge"
)

// Return codes for the C ABI layer.
//
// Zero means success. One means a non-blocking call found no data yet
// and the caller may retry. Negative values report errors.
const (
	weldCodeOK          = 0
	weldCodeEmpty       = 1
	weldCodeErrNull     = -1
	weldCodeErrHandle   = -2
	weldCodeErrInternal = -3
	weldCodeErrShort    = -4
)

// weldCallBound bounds weld_ffi_service_call_nb wait.
const weldCallBound = 5 * time.Second

// weldPollBound bounds the non-blocking poll wait.
//
// Subscription Take and Service Serve from the public API block until
// context ends, so the poll wrappers use this short bound and map a
// timeout to empty. A ready message returns at once without waiting.
const weldPollBound = 5 * time.Millisecond

// weldMaxPayload caps one copied payload in bytes.
//
// It matches the bridge 32 MiB bound so a corrupt length cannot force a
// larger single allocation. Oversized input reports internal error.
const weldMaxPayload = 32 << 20

// weldPubEntry ties a publisher handle to its owning bridge.
type weldPubEntry struct {
	bridgeID int64
	pub      *rmwbridge.Publisher
}

// weldSubEntry ties a subscription handle to its owning bridge.
type weldSubEntry struct {
	bridgeID int64
	sub      *rmwbridge.Subscription
}

// weldSvcEntry ties a service handle to its server object.
type weldSvcEntry struct {
	bridgeID int64
	name     string
	svc      *rmwbridge.Service
}

// weldSvcReplyKey identifies one served request awaiting a response.
type weldSvcReplyKey struct {
	svc int64
	seq int64
}

// weldStashedReq holds a dequeued request that did not fit the buffer.
type weldStashedReq struct {
	payload []byte
	respond func([]byte) error
}

// weldStashedTake holds a take that did not fit the caller buffer.
//
// The payload keeps the dequeued bytes while gid plus seq keep the
// dequeue-time attribution, so a larger-buffer retry delivers the same
// sequence instead of incrementing twice for one sample.
type weldStashedTake struct {
	payload []byte
	gid     [16]byte
	seq     uint64
}

// weldTakeInfo is the attribution of the most recent delivered take.
//
// Seq is the per-subscription receive sequence starting at 1. GID is
// the publishing writer identity, zero when the wire leg carried none.
// Intra-process and network sequences are equal because the bridge is
// a single transport.
type weldTakeInfo struct {
	gid [16]byte
	seq uint64
}

// Weld FFI handle registry.
//
// Handles are int64 keys handed to C as opaque values. Every map here is
// guarded by weldMu. Lookups copy pointers under weldMu and then operate
// without holding it.
var (
	weldMu          sync.Mutex
	weldNext        int64 = 1
	weldBridges           = make(map[int64]*rmwbridge.Bridge)
	weldBridgeNodes       = make(map[int64]map[string]*rmwbridge.Node)
	weldPubs              = make(map[int64]weldPubEntry)
	weldSubs              = make(map[int64]weldSubEntry)
	weldSubPending        = make(map[int64]weldStashedTake)
	weldSubLastInfo       = make(map[int64]weldTakeInfo)
	weldServices          = make(map[int64]weldSvcEntry)
	weldGuards            = make(map[int64]*rmwbridge.Guard)
	weldSvcSeq            = make(map[int64]int64)
	weldSvcReplies        = make(map[weldSvcReplyKey]func([]byte) error)
	weldSvcStash          = make(map[int64][]weldStashedReq)
)

// weldNextLocked returns a fresh handle value.
//
// The caller must hold weldMu.
func weldNextLocked() int64 {
	id := weldNext
	weldNext++
	return id
}

// weldWriteCString writes s NUL terminated into out.
//
// It returns OK on success, ErrNull for NULL or bad length, and ErrShort
// when outLen cannot hold s plus the terminator.
func weldWriteCString(out *C.char, outLen C.int, s string) C.int {
	if out == nil {
		return C.int(weldCodeErrNull)
	}
	if outLen <= 0 {
		return C.int(weldCodeErrNull)
	}
	need := len(s) + 1
	if need > int(outLen) {
		return C.int(weldCodeErrShort)
	}
	dst := unsafe.Slice((*byte)(unsafe.Pointer(out)), int(outLen))
	copy(dst, s)
	dst[len(s)] = 0
	return C.int(weldCodeOK)
}

// weldReadCString converts a NUL terminated C string to a Go string.
//
// It returns ErrNull when ptr is NULL.
func weldReadCString(ptr *C.char) (string, C.int) {
	if ptr == nil {
		return "", C.int(weldCodeErrNull)
	}
	s := C.GoString(ptr)
	return s, C.int(weldCodeOK)
}

// weldReadBytes copies ln bytes from ptr into a new Go slice.
//
// A zero length returns nil with OK and allows a NULL pointer. A positive
// length requires a non-NULL pointer. Lengths above the payload cap report
// internal error without copying.
func weldReadBytes(ptr *C.char, ln C.int) ([]byte, C.int) {
	if ln < 0 {
		return nil, C.int(weldCodeErrNull)
	}
	if ln == 0 {
		return nil, C.int(weldCodeOK)
	}
	if ptr == nil {
		return nil, C.int(weldCodeErrNull)
	}
	if int64(ln) > int64(weldMaxPayload) {
		return nil, C.int(weldCodeErrInternal)
	}
	out := C.GoBytes(unsafe.Pointer(ptr), ln)
	return out, C.int(weldCodeOK)
}

// weldCopyToC copies data into a C buffer without truncation.
//
// It returns OK on success, ErrShort when the buffer is too small, and
// ErrNull for a NULL pointer with non-empty data. Empty data succeeds
// without touching out.
func weldCopyToC(out *C.char, outLen C.int, data []byte) C.int {
	if len(data) == 0 {
		return C.int(weldCodeOK)
	}
	if out == nil {
		return C.int(weldCodeErrNull)
	}
	if outLen <= 0 {
		return C.int(weldCodeErrShort)
	}
	if len(data) > int(outLen) {
		return C.int(weldCodeErrShort)
	}
	dst := unsafe.Slice((*byte)(unsafe.Pointer(out)), int(outLen))
	copy(dst, data)
	return C.int(weldCodeOK)
}

// weldEnsureNodeLocked returns the cached node or creates and caches it.
//
// The caller must hold weldMu.
func weldEnsureNodeLocked(bridgeID int64, b *rmwbridge.Bridge, name string) (*rmwbridge.Node, error) {
	cached, ok := weldBridgeNodes[bridgeID]
	if ok {
		n, found := cached[name]
		if found {
			return n, nil
		}
	}
	node, err := b.CreateNode(name)
	if err != nil {
		return nil, fmt.Errorf("weld: create node: %w", err)
	}
	if cached == nil {
		cached = make(map[string]*rmwbridge.Node)
		weldBridgeNodes[bridgeID] = cached
	}
	cached[name] = node
	return node, nil
}

// weldTakeOnce takes one payload with a bounded wait.
//
// It uses the public Subscription TakeWithInfo with a short timeout so
// the poll path never blocks long. A timeout maps to empty with no data.
// The returned gid plus seq carry the dequeue-time attribution for the
// take-info side channel.
func weldTakeOnce(sub *rmwbridge.Subscription) ([]byte, [16]byte, uint64, bool, error) {
	var empty [16]byte
	ctx, cancel := context.WithTimeout(context.Background(), weldPollBound)
	defer cancel()
	data, gid, seq, err := sub.TakeWithInfo(ctx)
	if err != nil {
		cerr := ctx.Err()
		if cerr != nil {
			return nil, empty, 0, false, nil
		}
		return nil, empty, 0, false, fmt.Errorf("weld: take: %w", err)
	}
	return data, gid, seq, true, nil
}

// weldServeOnce waits for one request with a bounded wait.
//
// It uses the public Service Serve with a short timeout and maps a
// timeout to empty.
func weldServeOnce(svc *rmwbridge.Service) ([]byte, func([]byte) error, bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), weldPollBound)
	defer cancel()
	payload, respond, err := svc.Serve(ctx)
	if err != nil {
		cerr := ctx.Err()
		if cerr != nil {
			return nil, nil, false, nil
		}
		return nil, nil, false, fmt.Errorf("weld: serve: %w", err)
	}
	return payload, respond, true, nil
}

// weldReadNodeTopic reads node plus topic C strings with empty rejection.
//
// It returns ErrNull for NULL pointers and ErrInternal for empty names.
// It never panics.
func weldReadNodeTopic(nodeName, topic *C.char) (string, string, C.int) {
	if nodeName == nil || topic == nil {
		return "", "", C.int(weldCodeErrNull)
	}
	ns, code := weldReadCString(nodeName)
	if code != C.int(weldCodeOK) {
		return "", "", code
	}
	ts, code := weldReadCString(topic)
	if code != C.int(weldCodeOK) {
		return "", "", code
	}
	if ns == "" || ts == "" {
		return "", "", C.int(weldCodeErrInternal)
	}
	return ns, ts, C.int(weldCodeOK)
}

// weldCollectBridgeLocked detaches one bridge and its owned handles.
//
// The caller must hold weldMu and have verified the bridge exists. It
// deletes pubs, subs, services, plus stash, sequence, pending, reply, and
// node entries, returning copies to close without the lock. It never panics.
func weldCollectBridgeLocked(key int64) ([]weldPubEntry, []weldSubEntry, []weldSvcEntry) {
	pubs := make([]weldPubEntry, 0)
	for id, e := range weldPubs {
		if e.bridgeID == key {
			pubs = append(pubs, e)
			delete(weldPubs, id)
		}
	}
	subs := make([]weldSubEntry, 0)
	for id, e := range weldSubs {
		if e.bridgeID == key {
			subs = append(subs, e)
			delete(weldSubs, id)
			delete(weldSubPending, id)
			delete(weldSubLastInfo, id)
		}
	}
	svcs := make([]weldSvcEntry, 0)
	svcIDs := make(map[int64]struct{})
	for id, e := range weldServices {
		if e.bridgeID == key {
			svcs = append(svcs, e)
			svcIDs[id] = struct{}{}
			delete(weldServices, id)
			delete(weldSvcStash, id)
			delete(weldSvcSeq, id)
		}
	}
	for rkey := range weldSvcReplies {
		_, gone := svcIDs[rkey.svc]
		if gone {
			delete(weldSvcReplies, rkey)
		}
	}
	delete(weldBridgeNodes, key)
	delete(weldBridges, key)
	weldDropBridgeExtrasLocked(key)
	return pubs, subs, svcs
}

// weldCloseCollected closes detached pubs, subs, and services.
//
// It destroys services through b and closes the bridge last, returning the
// first error. It never panics.
func weldCloseCollected(b *rmwbridge.Bridge, pubs []weldPubEntry, subs []weldSubEntry, svcs []weldSvcEntry) error {
	var firstErr error
	for _, e := range pubs {
		err := e.pub.Close()
		if err != nil && firstErr == nil {
			firstErr = err
		}
	}
	for _, e := range subs {
		err := e.sub.Close()
		if err != nil && firstErr == nil {
			firstErr = err
		}
	}
	for _, e := range svcs {
		err := e.svc.Close()
		if err != nil && firstErr == nil {
			firstErr = err
		}
	}
	err := b.Close()
	if err != nil && firstErr == nil {
		firstErr = err
	}
	return firstErr
}

// weldLoadSubLocked returns the sub entry plus stashed take state.
//
// It reports false for an unknown handle. The caller must hold weldMu.
func weldLoadSubLocked(key int64) (weldSubEntry, weldStashedTake, bool, bool) {
	e, ok := weldSubs[key]
	if !ok {
		return weldSubEntry{}, weldStashedTake{}, false, false
	}
	pending, stashed := weldSubPending[key]
	return e, pending, stashed, true
}

// weldStashSubPayload parks an oversized take for later delivery.
//
// The gid plus seq keep the dequeue-time attribution so the retry
// delivers the same sequence. Only live subscriptions keep the stash; a
// bridge freed mid-take drops it. It never panics.
func weldStashSubPayload(key int64, data []byte, gid [16]byte, seq uint64) {
	cp := append([]byte(nil), data...)
	weldMu.Lock()
	_, still := weldSubs[key]
	if still {
		weldSubPending[key] = weldStashedTake{payload: cp, gid: gid, seq: seq}
	}
	weldMu.Unlock()
}

// weldRecordSubTakeInfo stores the attribution of a delivered take.
//
// Only live subscriptions keep the record; a bridge freed mid-take
// drops it. It never panics.
func weldRecordSubTakeInfo(key int64, gid [16]byte, seq uint64) {
	weldMu.Lock()
	_, still := weldSubs[key]
	if still {
		weldSubLastInfo[key] = weldTakeInfo{gid: gid, seq: seq}
	}
	weldMu.Unlock()
}

// weldReplaySubTakeInfo records a stashed retry without incrementing.
//
// The retry dequeued once already, so the bridge counter replays the
// same gid plus seq while the FFI record updates to the redelivery. It
// never panics.
func weldReplaySubTakeInfo(sub *rmwbridge.Subscription, key int64, stashed weldStashedTake) {
	sub.ReplayTakeInfo(stashed.gid, stashed.seq)
	weldRecordSubTakeInfo(key, stashed.gid, stashed.seq)
}

// weldCheckServiceCallArgs validates call_nb pointer plus length args.
//
// It returns ErrNull for NULL pointers or negative lengths. It never panics.
func weldCheckServiceCallArgs(req *C.char, reqLen C.int, respBuf *C.char, respBufLen C.int, outN *C.int) C.int {
	if outN == nil {
		return C.int(weldCodeErrNull)
	}
	if reqLen < 0 || respBufLen < 0 {
		return C.int(weldCodeErrNull)
	}
	if reqLen > 0 && req == nil {
		return C.int(weldCodeErrNull)
	}
	if respBufLen > 0 && respBuf == nil {
		return C.int(weldCodeErrNull)
	}
	return C.int(weldCodeOK)
}

// weldWriteCallResp copies a service reply or reports short without truncation.
//
// An oversized reply writes 0 to outN so the caller retries with a larger
// buffer. It never panics.
func weldWriteCallResp(respBuf *C.char, respBufLen C.int, outN *C.int, resp []byte) C.int {
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

// weldCheckServeArgs validates serve_nb pointer plus length args.
//
// It returns ErrNull for NULL pointers or negative lengths. It never panics.
func weldCheckServeArgs(reqBuf *C.char, reqBufLen C.int, outN *C.int, outSeq *C.longlong) C.int {
	if outN == nil || outSeq == nil {
		return C.int(weldCodeErrNull)
	}
	if reqBufLen < 0 {
		return C.int(weldCodeErrNull)
	}
	if reqBufLen > 0 && reqBuf == nil {
		return C.int(weldCodeErrNull)
	}
	return C.int(weldCodeOK)
}

// weldPopServeStash pops the oldest stashed request when present.
//
// Stash order is FIFO with front push on retry, so pop plus front push
// preserves retry order. It never panics.
func weldPopServeStash(key int64) (weldStashedReq, bool) {
	weldMu.Lock()
	defer weldMu.Unlock()
	stashed := weldSvcStash[key]
	if len(stashed) == 0 {
		return weldStashedReq{}, false
	}
	first := stashed[0]
	weldSvcStash[key] = stashed[1:]
	if len(weldSvcStash[key]) == 0 {
		delete(weldSvcStash, key)
	}
	return first, true
}

// weldPushServeStashFront requeues a request at the front for retry.
//
// It never panics.
func weldPushServeStashFront(key int64, payload []byte, respond func([]byte) error) {
	weldMu.Lock()
	defer weldMu.Unlock()
	weldSvcStash[key] = append([]weldStashedReq{{payload: payload, respond: respond}}, weldSvcStash[key]...)
}

// weld_ffi_version copies the bridge version string into out.
//
// out must point to a buffer of outLen bytes. The string is NUL terminated.
// It returns 0 on success, -1 for NULL or bad length, and -4 when the
// buffer is too small. It never panics.
//
//export weld_ffi_version
func weld_ffi_version(out *C.char, outLen C.int) (rc C.int) {
	defer func() {
		r := recover()
		if r != nil {
			rc = C.int(weldCodeErrInternal)
		}
	}()
	rc = weldWriteCString(out, outLen, rmwbridge.Version())
	return rc
}

// weld_ffi_identifier copies the RMW implementation name into out.
//
// out must point to a buffer of outLen bytes. The string is NUL terminated.
// It returns 0 on success, -1 for NULL or bad length, and -4 when the
// buffer is too small. It never panics.
//
//export weld_ffi_identifier
func weld_ffi_identifier(out *C.char, outLen C.int) (rc C.int) {
	defer func() {
		r := recover()
		if r != nil {
			rc = C.int(weldCodeErrInternal)
		}
	}()
	rc = weldWriteCString(out, outLen, rmwbridge.Identifier())
	return rc
}

// weld_ffi_bridge_new creates a bridge and writes its int64 handle.
//
// outHandle must be non-NULL. It returns 0 on success and -1 for a NULL
// out pointer. It never panics.
//
//export weld_ffi_bridge_new
func weld_ffi_bridge_new(outHandle *C.longlong) (rc C.int) {
	defer func() {
		r := recover()
		if r != nil {
			rc = C.int(weldCodeErrInternal)
		}
	}()
	if outHandle == nil {
		return C.int(weldCodeErrNull)
	}
	b := rmwbridge.NewBridge()
	weldMu.Lock()
	id := weldNextLocked()
	weldBridges[id] = b
	weldMu.Unlock()
	*outHandle = C.longlong(id)
	return C.int(weldCodeOK)
}

// weld_ffi_bridge_free closes a bridge and invalidates its FFI handles.
//
// Pub, sub, service, and node entries owned by the bridge are removed so
// later use of those handles reports -2. It returns 0 on success and -2
// for an unknown handle. It never panics.
//
//export weld_ffi_bridge_free
func weld_ffi_bridge_free(handle C.longlong) (rc C.int) {
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
	b, ok := weldBridges[key]
	if !ok {
		weldMu.Unlock()
		return C.int(weldCodeErrHandle)
	}
	pubs, subs, svcs := weldCollectBridgeLocked(key)
	weldMu.Unlock()
	weldCloseRTPS(key)
	err := weldCloseCollected(b, pubs, subs, svcs)
	if err != nil {
		return C.int(weldCodeErrInternal)
	}
	return C.int(weldCodeOK)
}

// weld_ffi_join joins discovery with a participant name and domain.
//
// participant must be a NUL terminated C string. domain must be
// non-negative. It returns 0 on success, -1 for NULL input, -2 for an
// unknown bridge handle, and -3 for join failures. It never panics.
//
//export weld_ffi_join
func weld_ffi_join(handle C.longlong, participant *C.char, domain C.int) (rc C.int) {
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
	if participant == nil {
		return C.int(weldCodeErrNull)
	}
	if domain < 0 {
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
	b, ok := weldBridges[key]
	weldMu.Unlock()
	if !ok {
		return C.int(weldCodeErrHandle)
	}
	info := rmwbridge.DiscoveryInfo{ParticipantName: name, DomainID: uint32(domain)}
	err := b.JoinDiscovery(context.Background(), info)
	if err != nil {
		return C.int(weldCodeErrInternal)
	}
	weldAttachRTPS(key, name, uint32(domain))
	return C.int(weldCodeOK)
}

// weld_ffi_pub_create creates a publisher on a bridge and writes its handle.
//
// nodeName names the owning node and is created on first use, then reused
// for later pubs and subs with the same name on the same bridge. topic must
// be non-empty. The publisher uses the default QoS. It returns 0 on
// success, -1 for NULL pointers, -2 for an unknown bridge, and -3 for
// bridge errors. It never panics.
//
//export weld_ffi_pub_create
func weld_ffi_pub_create(bridgeHandle C.longlong, nodeName *C.char, topic *C.char, outHandle *C.longlong) (rc C.int) {
	defer func() {
		r := recover()
		if r != nil {
			rc = C.int(weldCodeErrInternal)
		}
	}()
	if outHandle == nil {
		return C.int(weldCodeErrNull)
	}
	ns, ts, code := weldReadNodeTopic(nodeName, topic)
	if code != C.int(weldCodeOK) {
		return code
	}
	bkey := int64(bridgeHandle)
	if bkey <= 0 {
		return C.int(weldCodeErrHandle)
	}
	weldMu.Lock()
	b, ok := weldBridges[bkey]
	if !ok {
		weldMu.Unlock()
		return C.int(weldCodeErrHandle)
	}
	node, err := weldEnsureNodeLocked(bkey, b, ns)
	if err != nil {
		weldMu.Unlock()
		return C.int(weldCodeErrInternal)
	}
	pub, err := b.CreatePublisher(node, ts, rmwbridge.DefaultQoS())
	if err != nil {
		weldMu.Unlock()
		return C.int(weldCodeErrInternal)
	}
	id := weldNextLocked()
	weldPubs[id] = weldPubEntry{bridgeID: bkey, pub: pub}
	weldPubTypes[id] = weldPubType{bridgeID: bkey, topic: ts}
	weldMu.Unlock()
	*outHandle = C.longlong(id)
	return C.int(weldCodeOK)
}

// weld_ffi_pub_destroy closes a publisher and releases its handle.
//
// It returns 0 on success and -2 for an unknown handle. It never panics.
//
//export weld_ffi_pub_destroy
func weld_ffi_pub_destroy(handle C.longlong) (rc C.int) {
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
	e, ok := weldPubs[key]
	if !ok {
		weldMu.Unlock()
		return C.int(weldCodeErrHandle)
	}
	delete(weldPubs, key)
	delete(weldPubTypes, key)
	delete(weldPubDeadlines, key)
	delete(weldIncompatSeen, key)
	weldMu.Unlock()
	err := e.pub.Close()
	if err != nil {
		return C.int(weldCodeErrInternal)
	}
	return C.int(weldCodeOK)
}

// weld_ffi_pub_publish publishes one payload copy through a publisher.
//
// data holds dataLen bytes. A zero length allows a NULL pointer and sends
// an empty sample. It returns 0 on success, -1 for NULL or bad input, -2
// for an unknown handle, and -3 for publish failures. It never panics.
//
//export weld_ffi_pub_publish
func weld_ffi_pub_publish(handle C.longlong, data *C.char, dataLen C.int) (rc C.int) {
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
	payload, code := weldReadBytes(data, dataLen)
	if code != C.int(weldCodeOK) {
		return code
	}
	weldMu.Lock()
	e, ok := weldPubs[key]
	weldMu.Unlock()
	if !ok {
		return C.int(weldCodeErrHandle)
	}
	err := e.pub.Publish(context.Background(), payload)
	if err != nil {
		return C.int(weldCodeErrInternal)
	}
	weldPublishRTPS(key, payload)
	weldNotePubPublish(key)
	return C.int(weldCodeOK)
}

// weld_ffi_sub_create creates a subscription on a bridge and writes its handle.
//
// nodeName names the owning node and is created on first use, then reused
// for later pubs and subs with the same name on the same bridge. topic must
// be non-empty. The subscription uses the default QoS. It returns 0 on
// success, -1 for NULL pointers, -2 for an unknown bridge, and -3 for
// bridge errors. It never panics.
//
//export weld_ffi_sub_create
func weld_ffi_sub_create(bridgeHandle C.longlong, nodeName *C.char, topic *C.char, outHandle *C.longlong) (rc C.int) {
	defer func() {
		r := recover()
		if r != nil {
			rc = C.int(weldCodeErrInternal)
		}
	}()
	if outHandle == nil {
		return C.int(weldCodeErrNull)
	}
	ns, ts, code := weldReadNodeTopic(nodeName, topic)
	if code != C.int(weldCodeOK) {
		return code
	}
	bkey := int64(bridgeHandle)
	if bkey <= 0 {
		return C.int(weldCodeErrHandle)
	}
	weldMu.Lock()
	b, ok := weldBridges[bkey]
	if !ok {
		weldMu.Unlock()
		return C.int(weldCodeErrHandle)
	}
	node, err := weldEnsureNodeLocked(bkey, b, ns)
	if err != nil {
		weldMu.Unlock()
		return C.int(weldCodeErrInternal)
	}
	sub, err := b.CreateSubscription(node, ts, rmwbridge.DefaultQoS())
	if err != nil {
		weldMu.Unlock()
		return C.int(weldCodeErrInternal)
	}
	id := weldNextLocked()
	weldSubs[id] = weldSubEntry{bridgeID: bkey, sub: sub}
	weldSubTypes[id] = weldSubType{bridgeID: bkey, topic: ts}
	weldMu.Unlock()
	weldRefreshRTPSReader(bkey, ts)
	*outHandle = C.longlong(id)
	return C.int(weldCodeOK)
}

// weld_ffi_sub_destroy closes a subscription and releases its handle.
//
// It returns 0 on success and -2 for an unknown handle. It never panics.
//
//export weld_ffi_sub_destroy
func weld_ffi_sub_destroy(handle C.longlong) (rc C.int) {
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
	delete(weldSubs, key)
	delete(weldSubPending, key)
	delete(weldSubLastInfo, key)
	delete(weldSubTypes, key)
	delete(weldSubDeadlines, key)
	delete(weldIncompatSeen, key)
	weldDropSubExtrasLocked(key)
	weldMu.Unlock()
	err := e.sub.Close()
	if err != nil {
		return C.int(weldCodeErrInternal)
	}
	return C.int(weldCodeOK)
}

// weldTakeLiveSub takes one live sample and copies it out.
//
// It merges the in-memory take with the RTPS wire take, stashes oversized
// payloads with their dequeue-time attribution for a later retry, and
// writes the byte count. The take-info side channel records the gid plus
// sequence of the delivered take: in-memory takes carry the publishing
// writer identity while wire takes carry zero because the backend take
// drops the writer GUID. It never panics.
func weldTakeLiveSub(key int64, sub *rmwbridge.Subscription, bkey int64, topic string, outBuf *C.char, outBufLen C.int, outN *C.int) C.int {
	data, gid, seq, ready, err := weldTakeOnce(sub)
	if err != nil {
		return C.int(weldCodeErrInternal)
	}
	if !ready {
		wire, ok := weldTakeRTPS(bkey, topic)
		if !ok {
			*outN = C.int(0)
			return C.int(weldCodeEmpty)
		}
		var zero [16]byte
		seq = sub.NoteExternalDelivery(zero)
		gid = zero
		data = wire
		ready = true
	}
	if !ready {
		*outN = C.int(0)
		return C.int(weldCodeEmpty)
	}
	if len(data) > int(outBufLen) {
		weldStashSubPayload(key, data, gid, seq)
		return C.int(weldCodeErrShort)
	}
	code := weldCopyToC(outBuf, outBufLen, data)
	if code != C.int(weldCodeOK) {
		return code
	}
	weldRecordSubTakeInfo(key, gid, seq)
	weldNoteSubDelivery(key)
	*outN = C.int(len(data))
	return C.int(weldCodeOK)
}

// weld_ffi_sub_take_poll takes one message without long blocking.
//
// When a message is ready it copies up to outBufLen bytes into outBuf,
// writes the byte count to outN, and returns 0. An empty sample delivers 0
// bytes with return 0. When no message is ready it writes 0 to outN and
// returns 1 so the caller can tell empty data from no data. A message
// larger than the buffer is stashed and reported as -4 without truncation;
// retry with a larger buffer. NULL pointers report -1 and unknown handles
// report -2. It never panics.
//
//export weld_ffi_sub_take_poll
func weld_ffi_sub_take_poll(handle C.longlong, outBuf *C.char, outBufLen C.int, outN *C.int) (rc C.int) {
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
	if outBufLen < 0 {
		return C.int(weldCodeErrNull)
	}
	if outBufLen > 0 && outBuf == nil {
		return C.int(weldCodeErrNull)
	}
	weldMu.Lock()
	e, pending, stashed, ok := weldLoadSubLocked(key)
	if !ok {
		weldMu.Unlock()
		return C.int(weldCodeErrHandle)
	}
	if stashed {
		if len(pending.payload) > int(outBufLen) {
			weldMu.Unlock()
			return C.int(weldCodeErrShort)
		}
		code := weldCopyToC(outBuf, outBufLen, pending.payload)
		if code != C.int(weldCodeOK) {
			weldMu.Unlock()
			return code
		}
		delete(weldSubPending, key)
		n := len(pending.payload)
		sub := e.sub
		weldMu.Unlock()
		weldReplaySubTakeInfo(sub, key, pending)
		*outN = C.int(n)
		return C.int(weldCodeOK)
	}
	sub := e.sub
	bkey := e.bridgeID
	topic := sub.Topic
	weldMu.Unlock()
	return weldTakeLiveSub(key, sub, bkey, topic, outBuf, outBufLen, outN)
}

// weld_ffi_sub_take_info reports the attribution of the last delivered take.
//
// outSeq receives the per-subscription receive sequence starting at 1
// and gidBuf receives the 16 byte writer GID without a terminator.
// Intra-process and network sequence numbers are equal because the
// bridge is a single transport, so the caller reports the same value
// for both. In-memory takes carry the publishing writer identity while
// wire takes carry zero because the backend take drops the writer GUID.
// A handle with no delivered take, or an unknown handle, reports -2.
// NULL pointers report -1 and an undersized gid buffer reports -4. It
// never panics.
//
//export weld_ffi_sub_take_info
func weld_ffi_sub_take_info(handle C.longlong, outSeq *C.ulonglong, gidBuf *C.char, gidLen C.int) (rc C.int) {
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
	if outSeq == nil || gidBuf == nil {
		return C.int(weldCodeErrNull)
	}
	if int(gidLen) < 16 {
		return C.int(weldCodeErrShort)
	}
	weldMu.Lock()
	info, ok := weldSubLastInfo[key]
	if !ok {
		_, still := weldSubs[key]
		weldMu.Unlock()
		if !still {
			return C.int(weldCodeErrHandle)
		}
		return C.int(weldCodeErrHandle)
	}
	weldMu.Unlock()
	gid := info.gid
	*outSeq = C.ulonglong(info.seq)
	dst := unsafe.Slice((*byte)(unsafe.Pointer(gidBuf)), int(gidLen))
	copy(dst, gid[:])
	return C.int(weldCodeOK)
}

// weld_ffi_service_create registers a service and writes its handle.
//
// name must be a non-empty NUL terminated C string. It returns 0 on
// success, -1 for NULL pointers, -2 for an unknown bridge, and -3 for
// bridge errors. It never panics.
//
//export weld_ffi_service_create
func weld_ffi_service_create(bridgeHandle C.longlong, name *C.char, outHandle *C.longlong) (rc C.int) {
	defer func() {
		r := recover()
		if r != nil {
			rc = C.int(weldCodeErrInternal)
		}
	}()
	if outHandle == nil {
		return C.int(weldCodeErrNull)
	}
	if name == nil {
		return C.int(weldCodeErrNull)
	}
	sname, code := weldReadCString(name)
	if code != C.int(weldCodeOK) {
		return code
	}
	if sname == "" {
		return C.int(weldCodeErrInternal)
	}
	bkey := int64(bridgeHandle)
	if bkey <= 0 {
		return C.int(weldCodeErrHandle)
	}
	weldMu.Lock()
	b, ok := weldBridges[bkey]
	if !ok {
		weldMu.Unlock()
		return C.int(weldCodeErrHandle)
	}
	svc, err := b.CreateService(sname)
	if err != nil {
		weldMu.Unlock()
		return C.int(weldCodeErrInternal)
	}
	id := weldNextLocked()
	weldServices[id] = weldSvcEntry{bridgeID: bkey, name: sname, svc: svc}
	weldMu.Unlock()
	*outHandle = C.longlong(id)
	return C.int(weldCodeOK)
}

// weld_ffi_service_destroy unregisters a service and releases its handle.
//
// It returns 0 on success and -2 for an unknown handle. It never panics.
//
//export weld_ffi_service_destroy
func weld_ffi_service_destroy(handle C.longlong) (rc C.int) {
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
	e, ok := weldServices[key]
	if !ok {
		weldMu.Unlock()
		return C.int(weldCodeErrHandle)
	}
	bkey := e.bridgeID
	delete(weldServices, key)
	delete(weldSvcStash, key)
	delete(weldSvcSeq, key)
	weldDropSvcExtrasLocked(key)
	for rkey := range weldSvcReplies {
		if rkey.svc == key {
			delete(weldSvcReplies, rkey)
		}
	}
	svc := e.svc
	_, bok := weldBridges[bkey]
	weldMu.Unlock()
	if !bok {
		return C.int(weldCodeErrHandle)
	}
	err := svc.Close()
	if err != nil {
		return C.int(weldCodeErrInternal)
	}
	return C.int(weldCodeOK)
}

// weld_ffi_service_call_nb calls a service with a bounded wait.
//
// req holds reqLen bytes. A zero length allows a NULL pointer. On a reply
// it copies up to respBufLen bytes into respBuf, writes the byte count to
// outN, and returns 0. When no reply arrives before the internal bound it
// writes 0 to outN and returns 1 so the caller may retry. A reply larger
// than the buffer reports -4 without truncation. NULL pointers report -1
// and unknown handles report -2. It never panics.
//
//export weld_ffi_service_call_nb
func weld_ffi_service_call_nb(handle C.longlong, req *C.char, reqLen C.int, respBuf *C.char, respBufLen C.int, outN *C.int) (rc C.int) {
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
	if code := weldCheckServiceCallArgs(req, reqLen, respBuf, respBufLen, outN); code != C.int(weldCodeOK) {
		return code
	}
	weldMu.Lock()
	e, ok := weldServices[key]
	weldMu.Unlock()
	if !ok {
		return C.int(weldCodeErrHandle)
	}
	payload, code := weldReadBytes(req, reqLen)
	if code != C.int(weldCodeOK) {
		return code
	}
	ctx, cancel := context.WithTimeout(context.Background(), weldCallBound)
	defer cancel()
	resp, err := e.svc.Call(ctx, payload)
	if err != nil {
		cerr := ctx.Err()
		if cerr != nil {
			*outN = C.int(0)
			return C.int(weldCodeEmpty)
		}
		return C.int(weldCodeErrInternal)
	}
	return weldWriteCallResp(respBuf, respBufLen, outN, resp)
}

// weld_ffi_service_serve_nb dequeues one service request without long blocking.
//
// When a request is ready it copies the payload into reqBuf, writes the
// byte count to outN, writes a server sequence id to outSeq, and returns
// 0. The sequence id selects the pending responder for a later
// weld_ffi_service_respond call with the same handle. When no request is
// ready it writes 0 to outN and outSeq and returns 1 so the caller can
// tell no data from empty data. A request larger than reqBufLen is stashed
// and reported as -4 without truncation; retry with a larger buffer. NULL
// pointers report -1 and unknown handles report -2. It never panics.
//
//export weld_ffi_service_serve_nb
func weld_ffi_service_serve_nb(handle C.longlong, reqBuf *C.char, reqBufLen C.int, outN *C.int, outSeq *C.longlong) (rc C.int) {
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
	if code := weldCheckServeArgs(reqBuf, reqBufLen, outN, outSeq); code != C.int(weldCodeOK) {
		return code
	}
	weldMu.Lock()
	e, ok := weldServices[key]
	weldMu.Unlock()
	if !ok {
		return C.int(weldCodeErrHandle)
	}
	var payload []byte
	var respond func([]byte) error
	first, haveStash := weldPopServeStash(key)
	if haveStash {
		payload = first.payload
		respond = first.respond
	} else {
		got, respFn, ready, err := weldServeOnce(e.svc)
		if err != nil {
			return C.int(weldCodeErrInternal)
		}
		if !ready {
			*outN = C.int(0)
			*outSeq = C.longlong(0)
			return C.int(weldCodeEmpty)
		}
		payload = got
		respond = respFn
	}
	if len(payload) > int(reqBufLen) {
		weldPushServeStashFront(key, payload, respond)
		return C.int(weldCodeErrShort)
	}
	code := weldCopyToC(reqBuf, reqBufLen, payload)
	if code != C.int(weldCodeOK) {
		weldPushServeStashFront(key, payload, respond)
		return code
	}
	weldMu.Lock()
	seq := weldSvcSeq[key] + 1
	weldSvcSeq[key] = seq
	weldSvcReplies[weldSvcReplyKey{svc: key, seq: seq}] = respond
	weldMu.Unlock()
	*outN = C.int(len(payload))
	*outSeq = C.longlong(seq)
	return C.int(weldCodeOK)
}

// weld_ffi_service_respond replies to one served request.
//
// seq must be a sequence id previously returned by
// weld_ffi_service_serve_nb for the same handle. resp holds respLen bytes;
// a zero length allows a NULL pointer and delivers an empty reply. The
// first respond for a seq delivers the reply and returns 0; an unknown or
// reused seq reports -2. A reply larger than the payload cap reports -3.
// NULL pointers report -1 and unknown handles report -2. It never blocks
// and never panics.
//
//export weld_ffi_service_respond
func weld_ffi_service_respond(handle C.longlong, seq C.longlong, resp *C.char, respLen C.int) (rc C.int) {
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
	sid := int64(seq)
	if sid <= 0 {
		return C.int(weldCodeErrHandle)
	}
	payload, code := weldReadBytes(resp, respLen)
	if code != C.int(weldCodeOK) {
		return code
	}
	weldMu.Lock()
	_, ok := weldServices[key]
	if !ok {
		weldMu.Unlock()
		return C.int(weldCodeErrHandle)
	}
	rkey := weldSvcReplyKey{svc: key, seq: sid}
	reply, ok := weldSvcReplies[rkey]
	if !ok {
		weldMu.Unlock()
		return C.int(weldCodeErrHandle)
	}
	delete(weldSvcReplies, rkey)
	weldMu.Unlock()
	err := reply(payload)
	if err != nil {
		return C.int(weldCodeErrHandle)
	}
	return C.int(weldCodeOK)
}

// weld_ffi_guard_create creates a guard and writes its int64 handle.
//
// outHandle must be non-NULL. It returns 0 on success and -1 for a NULL
// out pointer. It never panics.
//
//export weld_ffi_guard_create
func weld_ffi_guard_create(outHandle *C.longlong) (rc C.int) {
	defer func() {
		r := recover()
		if r != nil {
			rc = C.int(weldCodeErrInternal)
		}
	}()
	if outHandle == nil {
		return C.int(weldCodeErrNull)
	}
	g := rmwbridge.NewGuard()
	weldMu.Lock()
	id := weldNextLocked()
	weldGuards[id] = g
	weldMu.Unlock()
	*outHandle = C.longlong(id)
	return C.int(weldCodeOK)
}

// weld_ffi_guard_trigger sets a guard to the triggered state.
//
// It returns 0 on success and -2 for an unknown handle. It never panics.
//
//export weld_ffi_guard_trigger
func weld_ffi_guard_trigger(handle C.longlong) (rc C.int) {
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
	g, ok := weldGuards[key]
	weldMu.Unlock()
	if !ok {
		return C.int(weldCodeErrHandle)
	}
	g.Trigger()
	return C.int(weldCodeOK)
}

// weld_ffi_guard_reset clears a guard back to the untriggered state.
//
// It returns 0 on success and -2 for an unknown handle. It never panics.
//
//export weld_ffi_guard_reset
func weld_ffi_guard_reset(handle C.longlong) (rc C.int) {
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
	g, ok := weldGuards[key]
	weldMu.Unlock()
	if !ok {
		return C.int(weldCodeErrHandle)
	}
	g.Reset()
	return C.int(weldCodeOK)
}

// weld_ffi_guard_destroy releases a guard handle.
//
// It is idempotent: releasing an already released or never created handle
// still returns 0. It returns -2 for a non-positive handle value. It
// never panics.
//
//export weld_ffi_guard_destroy
func weld_ffi_guard_destroy(handle C.longlong) (rc C.int) {
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
	delete(weldGuards, key)
	weldMu.Unlock()
	return C.int(weldCodeOK)
}

// main is required for a c-shared package.
//
// It does nothing; the shared library exposes only the weld_ffi exports.
func main() {}
