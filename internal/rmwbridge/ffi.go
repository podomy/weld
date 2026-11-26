// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

package rmwbridge

/*
#include <stdlib.h>
#include <string.h>
*/
import "C"

import (
	"context"
	"sync"
	"time"
	"unsafe"
)

// FFI return codes for the C ABI layer.
//
// Zero means success. One means a non-blocking call found no data yet and
// the caller may retry. Negative values report errors and never panic.
const (
	ffiOK          = 0
	ffiEmpty       = 1
	ffiErrNull     = -1
	ffiErrHandle   = -2
	ffiErrInternal = -3
	ffiErrShort    = -4
)

// ffiCallTimeout bounds the wait inside weld_ffi_service_call_nb.
//
// The call stays bounded so a C caller without cancellation still regains
// control. A responder running Serve concurrently replies well before this
// limit in tests.
const ffiCallTimeout = 5 * time.Second

// ffiPubEntry ties a publisher handle to its owning bridge.
type ffiPubEntry struct {
	bridgeID int64
	pub      *Publisher
}

// ffiSubEntry ties a subscription handle to its owning bridge.
type ffiSubEntry struct {
	bridgeID int64
	sub      *Subscription
}

// ffiServiceEntry ties a service handle to its server object.
type ffiServiceEntry struct {
	bridgeID int64
	name     string
	svc      *Service
}

// ffiSvcReplyKey identifies one served request awaiting a response.
type ffiSvcReplyKey struct {
	svc int64
	seq int64
}

// ffiStashedTake holds a dequeued take that did not fit the take buffer.
//
// The payload keeps the dequeued bytes while gid plus seq keep the
// attribution assigned at dequeue, so a larger-buffer retry delivers the
// same sequence instead of incrementing twice for one sample.
type ffiStashedTake struct {
	payload []byte
	gid     [16]byte
	seq     uint64
}

// ffiTakeInfo is the attribution of the most recent delivered take.
//
// Seq is the per-subscription receive sequence starting at 1. GID is the
// publishing writer identity. Intra-process and network sequences are
// equal because the bridge is a single transport.
type ffiTakeInfo struct {
	gid [16]byte
	seq uint64
}

// ffiStashedReq holds a dequeued request that did not fit the serve buffer.
type ffiStashedReq struct {
	payload []byte
	reply   chan []byte
}

// Global FFI handle registry.
//
// Handles are int64 keys handed to C as opaque values. Every map here is
// guarded by ffiMu. Bridge methods stay thread safe on their own, so lookups
// copy pointers under ffiMu and then operate without holding it, except for
// the short non-blocking take_poll path which never blocks while holding it.
var (
	ffiMu          sync.Mutex
	ffiNext        int64 = 1
	ffiBridges           = make(map[int64]*Bridge)
	ffiBridgeNodes       = make(map[int64]map[string]*Node)
	ffiPubs              = make(map[int64]ffiPubEntry)
	ffiSubs              = make(map[int64]ffiSubEntry)
	ffiSubPending        = make(map[int64]ffiStashedTake)
	ffiSubLastInfo       = make(map[int64]ffiTakeInfo)
	ffiServices          = make(map[int64]ffiServiceEntry)
	ffiGuards            = make(map[int64]*Guard)
	ffiSvcSeq            = make(map[int64]int64)
	ffiSvcReplies        = make(map[ffiSvcReplyKey]chan []byte)
	ffiSvcStash          = make(map[int64][]ffiStashedReq)
)

// ffiNextLocked returns a fresh handle value.
//
// The caller must hold ffiMu.
func ffiNextLocked() int64 {
	id := ffiNext
	ffiNext++
	return id
}

// ffiWriteCString writes s NUL terminated into out.
//
// It returns ffiOK on success, ffiErrNull for a NULL pointer or bad length,
// and ffiErrShort when outLen cannot hold s plus the terminator.
func ffiWriteCString(out *C.char, outLen C.int, s string) C.int {
	if out == nil {
		return C.int(ffiErrNull)
	}
	if outLen <= 0 {
		return C.int(ffiErrNull)
	}
	need := len(s) + 1
	if need > int(outLen) {
		return C.int(ffiErrShort)
	}
	dst := unsafe.Slice((*byte)(unsafe.Pointer(out)), int(outLen))
	copy(dst, s)
	dst[len(s)] = 0
	return C.int(ffiOK)
}

// ffiReadCString converts a NUL terminated C string to a Go string.
//
// It returns ffiErrNull when ptr is NULL. The caller must pass a NUL
// terminated string.
func ffiReadCString(ptr *C.char) (string, C.int) {
	if ptr == nil {
		return "", C.int(ffiErrNull)
	}
	s := C.GoString(ptr)
	return s, C.int(ffiOK)
}

// ffiReadBytes copies ln bytes from ptr into a new Go slice.
//
// A zero length returns nil with ffiOK and allows a NULL pointer. A
// positive length requires a non-NULL pointer. Lengths above
// maxPayloadBytes report ffiErrInternal without copying.
func ffiReadBytes(ptr *C.char, ln C.int) ([]byte, C.int) {
	if ln < 0 {
		return nil, C.int(ffiErrNull)
	}
	if ln == 0 {
		return nil, C.int(ffiOK)
	}
	if ptr == nil {
		return nil, C.int(ffiErrNull)
	}
	if int64(ln) > int64(maxPayloadBytes) {
		return nil, C.int(ffiErrInternal)
	}
	out := C.GoBytes(unsafe.Pointer(ptr), ln)
	return out, C.int(ffiOK)
}

// ffiCopyToC copies data into a C buffer without truncation.
//
// It returns ffiOK on success, ffiErrShort when the buffer is too small, and
// ffiErrNull for a NULL pointer with non-empty data. Empty data succeeds
// without touching out.
func ffiCopyToC(out *C.char, outLen C.int, data []byte) C.int {
	if len(data) == 0 {
		return C.int(ffiOK)
	}
	if out == nil {
		return C.int(ffiErrNull)
	}
	if outLen <= 0 {
		return C.int(ffiErrShort)
	}
	if len(data) > int(outLen) {
		return C.int(ffiErrShort)
	}
	dst := unsafe.Slice((*byte)(unsafe.Pointer(out)), int(outLen))
	copy(dst, data)
	return C.int(ffiOK)
}

// ffiEnsureNode returns the cached node or creates and caches it.
//
// The caller must hold ffiMu. Bridge calls lock the bridge mutex, which is
// safe because no bridge method ever locks ffiMu back.
func ffiEnsureNode(bridgeID int64, b *Bridge, name string) (*Node, error) {
	cached, ok := ffiBridgeNodes[bridgeID]
	if ok {
		n, found := cached[name]
		if found {
			return n, nil
		}
	}
	node, err := b.CreateNode(name)
	if err != nil {
		return nil, err
	}
	if cached == nil {
		cached = make(map[string]*Node)
		ffiBridgeNodes[bridgeID] = cached
	}
	cached[name] = node
	return node, nil
}

// weld_ffi_version copies the bridge version string into out.
//
// out must point to a buffer of outLen bytes. The string is NUL terminated.
// It returns 0 on success, -1 for NULL or bad length, and -4 when the
// buffer is too small. It never panics.
func weld_ffi_version(out *C.char, outLen C.int) (rc C.int) {
	defer func() {
		r := recover()
		if r != nil {
			rc = C.int(ffiErrInternal)
		}
	}()
	rc = ffiWriteCString(out, outLen, Version())
	return rc
}

// weld_ffi_identifier copies the RMW implementation name into out.
//
// out must point to a buffer of outLen bytes. The string is NUL terminated.
// It returns 0 on success, -1 for NULL or bad length, and -4 when the
// buffer is too small. It never panics.
func weld_ffi_identifier(out *C.char, outLen C.int) (rc C.int) {
	defer func() {
		r := recover()
		if r != nil {
			rc = C.int(ffiErrInternal)
		}
	}()
	rc = ffiWriteCString(out, outLen, Identifier())
	return rc
}

// weld_ffi_bridge_new creates a bridge and writes its int64 handle.
//
// outHandle must be non-NULL. It returns 0 on success and -1 for a NULL
// out pointer. It never panics.
func weld_ffi_bridge_new(outHandle *C.longlong) (rc C.int) {
	defer func() {
		r := recover()
		if r != nil {
			rc = C.int(ffiErrInternal)
		}
	}()
	if outHandle == nil {
		return C.int(ffiErrNull)
	}
	b := NewBridge()
	ffiMu.Lock()
	id := ffiNextLocked()
	ffiBridges[id] = b
	ffiMu.Unlock()
	*outHandle = C.longlong(id)
	return C.int(ffiOK)
}

// mergeFirstErr returns first when set, otherwise err.
func mergeFirstErr(first, err error) error {
	if first != nil {
		return first
	}
	return err
}

// ffiDetachBridgeLocked removes bridge handles and returns owned entries.
//
// The caller must hold ffiMu.
func ffiDetachBridgeLocked(key int64) (*Bridge, []ffiPubEntry, []ffiSubEntry, []ffiServiceEntry, bool) {
	b, ok := ffiBridges[key]
	if !ok {
		return nil, nil, nil, nil, false
	}
	pubs := make([]ffiPubEntry, 0)
	for id, e := range ffiPubs {
		if e.bridgeID == key {
			pubs = append(pubs, e)
			delete(ffiPubs, id)
		}
	}
	subs := make([]ffiSubEntry, 0)
	for id, e := range ffiSubs {
		if e.bridgeID == key {
			subs = append(subs, e)
			delete(ffiSubs, id)
			delete(ffiSubPending, id)
			delete(ffiSubLastInfo, id)
		}
	}
	svcs := make([]ffiServiceEntry, 0)
	svcIDs := make(map[int64]struct{})
	for id, e := range ffiServices {
		if e.bridgeID == key {
			svcs = append(svcs, e)
			svcIDs[id] = struct{}{}
			delete(ffiServices, id)
			delete(ffiSvcStash, id)
			delete(ffiSvcSeq, id)
		}
	}
	for rkey := range ffiSvcReplies {
		if _, gone := svcIDs[rkey.svc]; gone {
			delete(ffiSvcReplies, rkey)
		}
	}
	delete(ffiBridgeNodes, key)
	delete(ffiBridges, key)
	return b, pubs, subs, svcs, true
}

// ffiCloseDetachedBridge closes detached entries and returns first error.
func ffiCloseDetachedBridge(b *Bridge, pubs []ffiPubEntry, subs []ffiSubEntry, svcs []ffiServiceEntry) error {
	var firstErr error
	for _, e := range pubs {
		err := e.pub.Close()
		firstErr = mergeFirstErr(firstErr, err)
	}
	for _, e := range subs {
		err := e.sub.Close()
		firstErr = mergeFirstErr(firstErr, err)
	}
	for _, e := range svcs {
		err := e.svc.Close()
		firstErr = mergeFirstErr(firstErr, err)
	}
	err := b.Close()
	firstErr = mergeFirstErr(firstErr, err)
	return firstErr
}

// weld_ffi_bridge_free closes a bridge and invalidates its FFI handles.
//
// Pub, sub, service, and node entries owned by the bridge are removed so
// later use of those handles reports -2. It returns 0 on success and -2
// for an unknown handle. It never panics.
func weld_ffi_bridge_free(handle C.longlong) (rc C.int) {
	defer func() {
		r := recover()
		if r != nil {
			rc = C.int(ffiErrInternal)
		}
	}()
	key := int64(handle)
	if key <= 0 {
		return C.int(ffiErrHandle)
	}
	ffiMu.Lock()
	b, pubs, subs, svcs, ok := ffiDetachBridgeLocked(key)
	if !ok {
		ffiMu.Unlock()
		return C.int(ffiErrHandle)
	}
	ffiMu.Unlock()
	firstErr := ffiCloseDetachedBridge(b, pubs, subs, svcs)
	if firstErr != nil {
		return C.int(ffiErrInternal)
	}
	return C.int(ffiOK)
}

// weld_ffi_join joins discovery with a participant name and domain.
//
// participant must be a NUL terminated C string. domain must be
// non-negative. It returns 0 on success, -1 for NULL input, -2 for an
// unknown bridge handle, and -3 for join failures. It never panics.
func weld_ffi_join(handle C.longlong, participant *C.char, domain C.int) (rc C.int) {
	defer func() {
		r := recover()
		if r != nil {
			rc = C.int(ffiErrInternal)
		}
	}()
	key := int64(handle)
	if key <= 0 {
		return C.int(ffiErrHandle)
	}
	if participant == nil {
		return C.int(ffiErrNull)
	}
	if domain < 0 {
		return C.int(ffiErrNull)
	}
	name, code := ffiReadCString(participant)
	if code != C.int(ffiOK) {
		return code
	}
	if name == "" {
		return C.int(ffiErrInternal)
	}
	ffiMu.Lock()
	b, ok := ffiBridges[key]
	ffiMu.Unlock()
	if !ok {
		return C.int(ffiErrHandle)
	}
	info := DiscoveryInfo{ParticipantName: name, DomainID: uint32(domain)}
	err := b.JoinDiscovery(context.Background(), info)
	if err != nil {
		return C.int(ffiErrInternal)
	}
	return C.int(ffiOK)
}

// ffiParseEndpointNames validates node and topic C strings.
func ffiParseEndpointNames(nodeName, topic *C.char) (string, string, C.int) {
	if nodeName == nil || topic == nil {
		return "", "", C.int(ffiErrNull)
	}
	ns, code := ffiReadCString(nodeName)
	if code != C.int(ffiOK) {
		return "", "", code
	}
	ts, code := ffiReadCString(topic)
	if code != C.int(ffiOK) {
		return "", "", code
	}
	if ns == "" || ts == "" {
		return "", "", C.int(ffiErrInternal)
	}
	return ns, ts, C.int(ffiOK)
}

// ffiEndpointCreate creates a publisher or subscription endpoint.
//
// When isPub is true it creates a publisher, otherwise a subscription.
// The caller must pass validated names and bridge handle.
func ffiEndpointCreate(bridgeHandle C.longlong, ns, ts string, outHandle *C.longlong, isPub bool) C.int {
	bkey := int64(bridgeHandle)
	if bkey <= 0 {
		return C.int(ffiErrHandle)
	}
	ffiMu.Lock()
	b, ok := ffiBridges[bkey]
	if !ok {
		ffiMu.Unlock()
		return C.int(ffiErrHandle)
	}
	node, err := ffiEnsureNode(bkey, b, ns)
	if err != nil {
		ffiMu.Unlock()
		return C.int(ffiErrInternal)
	}
	if isPub {
		pub, err := b.CreatePublisher(node, ts, DefaultQoS())
		if err != nil {
			ffiMu.Unlock()
			return C.int(ffiErrInternal)
		}
		id := ffiNextLocked()
		ffiPubs[id] = ffiPubEntry{bridgeID: bkey, pub: pub}
		ffiMu.Unlock()
		*outHandle = C.longlong(id)
		return C.int(ffiOK)
	}
	sub, err := b.CreateSubscription(node, ts, DefaultQoS())
	if err != nil {
		ffiMu.Unlock()
		return C.int(ffiErrInternal)
	}
	id := ffiNextLocked()
	ffiSubs[id] = ffiSubEntry{bridgeID: bkey, sub: sub}
	ffiMu.Unlock()
	*outHandle = C.longlong(id)
	return C.int(ffiOK)
}

// weld_ffi_pub_create creates a publisher on a bridge and writes its handle.
//
// nodeName names the owning node and is created on first use, then reused
// for later pubs and subs with the same name on the same bridge. topic must
// be non-empty. The publisher uses the default QoS. It returns 0 on
// success, -1 for NULL pointers, -2 for an unknown bridge, and -3 for
// bridge errors. It never panics.
func weld_ffi_pub_create(bridgeHandle C.longlong, nodeName *C.char, topic *C.char, outHandle *C.longlong) (rc C.int) {
	defer func() {
		r := recover()
		if r != nil {
			rc = C.int(ffiErrInternal)
		}
	}()
	if outHandle == nil {
		return C.int(ffiErrNull)
	}
	ns, ts, code := ffiParseEndpointNames(nodeName, topic)
	if code != C.int(ffiOK) {
		return code
	}
	rc = ffiEndpointCreate(bridgeHandle, ns, ts, outHandle, true)
	return rc
}

// weld_ffi_pub_destroy closes a publisher and releases its handle.
//
// It returns 0 on success and -2 for an unknown handle. It never panics.
func weld_ffi_pub_destroy(handle C.longlong) (rc C.int) {
	defer func() {
		r := recover()
		if r != nil {
			rc = C.int(ffiErrInternal)
		}
	}()
	key := int64(handle)
	if key <= 0 {
		return C.int(ffiErrHandle)
	}
	ffiMu.Lock()
	e, ok := ffiPubs[key]
	if !ok {
		ffiMu.Unlock()
		return C.int(ffiErrHandle)
	}
	delete(ffiPubs, key)
	ffiMu.Unlock()
	err := e.pub.Close()
	if err != nil {
		return C.int(ffiErrInternal)
	}
	return C.int(ffiOK)
}

// weld_ffi_pub_publish publishes one payload copy through a publisher.
//
// data holds dataLen bytes. A zero length allows a NULL pointer and sends
// an empty sample. It returns 0 on success, -1 for NULL or bad input, -2
// for an unknown handle, and -3 for publish failures. It never panics.
func weld_ffi_pub_publish(handle C.longlong, data *C.char, dataLen C.int) (rc C.int) {
	defer func() {
		r := recover()
		if r != nil {
			rc = C.int(ffiErrInternal)
		}
	}()
	key := int64(handle)
	if key <= 0 {
		return C.int(ffiErrHandle)
	}
	payload, code := ffiReadBytes(data, dataLen)
	if code != C.int(ffiOK) {
		return code
	}
	ffiMu.Lock()
	e, ok := ffiPubs[key]
	ffiMu.Unlock()
	if !ok {
		return C.int(ffiErrHandle)
	}
	err := e.pub.Publish(context.Background(), payload)
	if err != nil {
		return C.int(ffiErrInternal)
	}
	return C.int(ffiOK)
}

// weld_ffi_sub_create creates a subscription on a bridge and writes its handle.
//
// nodeName names the owning node and is created on first use, then reused
// for later pubs and subs with the same name on the same bridge. topic must
// be non-empty. The subscription uses the default QoS. It returns 0 on
// success, -1 for NULL pointers, -2 for an unknown bridge, and -3 for
// bridge errors. It never panics.
func weld_ffi_sub_create(bridgeHandle C.longlong, nodeName *C.char, topic *C.char, outHandle *C.longlong) (rc C.int) {
	defer func() {
		r := recover()
		if r != nil {
			rc = C.int(ffiErrInternal)
		}
	}()
	if outHandle == nil {
		return C.int(ffiErrNull)
	}
	ns, ts, code := ffiParseEndpointNames(nodeName, topic)
	if code != C.int(ffiOK) {
		return code
	}
	rc = ffiEndpointCreate(bridgeHandle, ns, ts, outHandle, false)
	return rc
}

// weld_ffi_sub_destroy closes a subscription and releases its handle.
//
// It returns 0 on success and -2 for an unknown handle. It never panics.
func weld_ffi_sub_destroy(handle C.longlong) (rc C.int) {
	defer func() {
		r := recover()
		if r != nil {
			rc = C.int(ffiErrInternal)
		}
	}()
	key := int64(handle)
	if key <= 0 {
		return C.int(ffiErrHandle)
	}
	ffiMu.Lock()
	e, ok := ffiSubs[key]
	if !ok {
		ffiMu.Unlock()
		return C.int(ffiErrHandle)
	}
	delete(ffiSubs, key)
	delete(ffiSubPending, key)
	delete(ffiSubLastInfo, key)
	ffiMu.Unlock()
	err := e.sub.Close()
	if err != nil {
		return C.int(ffiErrInternal)
	}
	return C.int(ffiOK)
}

// ffiValidateTakePollArgs checks take poll buffer arguments.
func ffiValidateTakePollArgs(outBuf *C.char, outBufLen C.int, outN *C.int) C.int {
	if outN == nil {
		return C.int(ffiErrNull)
	}
	if outBufLen < 0 {
		return C.int(ffiErrNull)
	}
	if outBufLen > 0 && outBuf == nil {
		return C.int(ffiErrNull)
	}
	return C.int(ffiOK)
}

// ffiBeginTakePoll resolves the subscription and drains stashed data.
//
// When handled is true the caller must return code directly. A stashed
// retry replays the dequeue-time attribution without incrementing the
// per-subscription sequence. When false sub holds the subscription for
// a fresh poll.
func ffiBeginTakePoll(key int64, outBuf *C.char, outBufLen C.int, outN *C.int) (*Subscription, bool, C.int) {
	ffiMu.Lock()
	e, ok := ffiSubs[key]
	if !ok {
		ffiMu.Unlock()
		return nil, true, C.int(ffiErrHandle)
	}
	stashed, hasStash := ffiSubPending[key]
	if hasStash {
		if len(stashed.payload) > int(outBufLen) {
			ffiMu.Unlock()
			return nil, true, C.int(ffiErrShort)
		}
		code := ffiCopyToC(outBuf, outBufLen, stashed.payload)
		if code != C.int(ffiOK) {
			ffiMu.Unlock()
			return nil, true, code
		}
		delete(ffiSubPending, key)
		ffiSubLastInfo[key] = ffiTakeInfo{gid: stashed.gid, seq: stashed.seq}
		n := len(stashed.payload)
		sub := e.sub
		ffiMu.Unlock()
		sub.ReplayTakeInfo(stashed.gid, stashed.seq)
		*outN = C.int(n)
		return nil, true, C.int(ffiOK)
	}
	sub := e.sub
	ffiMu.Unlock()
	return sub, false, C.int(ffiOK)
}

// ffiCheckSubUsable reports whether a subscription can be polled.
func ffiCheckSubUsable(sub *Subscription) C.int {
	sub.bridge.mu.Lock()
	bridgeClosed := sub.bridge.closed
	sub.bridge.mu.Unlock()
	if bridgeClosed {
		return C.int(ffiErrInternal)
	}
	sub.mu.Lock()
	subClosed := sub.closed
	sub.mu.Unlock()
	if subClosed {
		return C.int(ffiErrInternal)
	}
	return C.int(ffiOK)
}

// ffiStashTakePending stores an oversized payload for retry.
//
// The gid plus seq keep the dequeue-time attribution so the retry
// delivers the same sequence instead of incrementing twice.
func ffiStashTakePending(key int64, sample deliveredSample, seq uint64) {
	cp := append([]byte(nil), sample.payload...)
	ffiMu.Lock()
	_, still := ffiSubs[key]
	if still {
		ffiSubPending[key] = ffiStashedTake{payload: cp, gid: sample.gid, seq: seq}
	}
	ffiMu.Unlock()
}

// ffiRecordTakeInfo stores the attribution of a fresh delivered take.
func ffiRecordTakeInfo(key int64, gid [16]byte, seq uint64) {
	ffiMu.Lock()
	_, still := ffiSubs[key]
	if still {
		ffiSubLastInfo[key] = ffiTakeInfo{gid: gid, seq: seq}
	}
	ffiMu.Unlock()
}

// weld_ffi_sub_take_poll takes one message without blocking.
//
// When a message is ready it copies up to outBufLen bytes into outBuf,
// writes the byte count to outN, and returns 0. An empty sample delivers 0
// bytes with return 0. When no message is ready it writes 0 to outN and
// returns 1 so the caller can tell empty data from no data. A message
// larger than the buffer is stashed and reported as -4 without truncation;
// retry with a larger buffer. NULL pointers report -1 and unknown handles
// report -2. It never panics.
func weld_ffi_sub_take_poll(handle C.longlong, outBuf *C.char, outBufLen C.int, outN *C.int) (rc C.int) {
	defer func() {
		r := recover()
		if r != nil {
			rc = C.int(ffiErrInternal)
		}
	}()
	key := int64(handle)
	if key <= 0 {
		return C.int(ffiErrHandle)
	}
	if code := ffiValidateTakePollArgs(outBuf, outBufLen, outN); code != C.int(ffiOK) {
		return code
	}
	sub, handled, code := ffiBeginTakePoll(key, outBuf, outBufLen, outN)
	if handled {
		return code
	}
	if code := ffiCheckSubUsable(sub); code != C.int(ffiOK) {
		return code
	}
	var sample deliveredSample
	var ready bool
	select {
	case sample = <-sub.ch:
		ready = true
	default:
		ready = false
	}
	if !ready {
		*outN = C.int(0)
		return C.int(ffiEmpty)
	}
	seq := sub.noteDeliveredLocked(sample.gid)
	if len(sample.payload) > int(outBufLen) {
		ffiStashTakePending(key, sample, seq)
		return C.int(ffiErrShort)
	}
	code = ffiCopyToC(outBuf, outBufLen, sample.payload)
	if code != C.int(ffiOK) {
		return code
	}
	ffiRecordTakeInfo(key, sample.gid, seq)
	*outN = C.int(len(sample.payload))
	return C.int(ffiOK)
}

// weld_ffi_sub_take_info reports the attribution of the last delivered take.
//
// outSeq receives the per-subscription receive sequence starting at 1
// and gidBuf receives the 16 byte writer GID without a terminator.
// Intra-process and network sequence numbers are equal because the
// bridge is a single transport, so the caller reports the same value
// for both. A handle with no delivered take reports -2. NULL pointers
// report -1 and an undersized gid buffer reports -4. It never panics.
func weld_ffi_sub_take_info(handle C.longlong, outSeq *C.ulonglong, gidBuf *C.char, gidLen C.int) (rc C.int) {
	defer func() {
		r := recover()
		if r != nil {
			rc = C.int(ffiErrInternal)
		}
	}()
	key := int64(handle)
	if key <= 0 {
		return C.int(ffiErrHandle)
	}
	if outSeq == nil || gidBuf == nil {
		return C.int(ffiErrNull)
	}
	if int(gidLen) < 16 {
		return C.int(ffiErrShort)
	}
	ffiMu.Lock()
	info, ok := ffiSubLastInfo[key]
	if !ok {
		_, still := ffiSubs[key]
		ffiMu.Unlock()
		if !still {
			return C.int(ffiErrHandle)
		}
		return C.int(ffiErrHandle)
	}
	ffiMu.Unlock()
	gid := info.gid
	*outSeq = C.ulonglong(info.seq)
	dst := unsafe.Slice((*byte)(unsafe.Pointer(gidBuf)), int(gidLen))
	copy(dst, gid[:])
	return C.int(ffiOK)
}

// weld_ffi_service_create registers a service and writes its handle.
//
// name must be a non-empty NUL terminated C string. It returns 0 on
// success, -1 for NULL pointers, -2 for an unknown bridge, and -3 for
// bridge errors. It never panics.
func weld_ffi_service_create(bridgeHandle C.longlong, name *C.char, outHandle *C.longlong) (rc C.int) {
	defer func() {
		r := recover()
		if r != nil {
			rc = C.int(ffiErrInternal)
		}
	}()
	if outHandle == nil {
		return C.int(ffiErrNull)
	}
	if name == nil {
		return C.int(ffiErrNull)
	}
	sname, code := ffiReadCString(name)
	if code != C.int(ffiOK) {
		return code
	}
	if sname == "" {
		return C.int(ffiErrInternal)
	}
	bkey := int64(bridgeHandle)
	if bkey <= 0 {
		return C.int(ffiErrHandle)
	}
	ffiMu.Lock()
	b, ok := ffiBridges[bkey]
	if !ok {
		ffiMu.Unlock()
		return C.int(ffiErrHandle)
	}
	svc, err := b.CreateService(sname)
	if err != nil {
		ffiMu.Unlock()
		return C.int(ffiErrInternal)
	}
	id := ffiNextLocked()
	ffiServices[id] = ffiServiceEntry{bridgeID: bkey, name: sname, svc: svc}
	ffiMu.Unlock()
	*outHandle = C.longlong(id)
	return C.int(ffiOK)
}

// weld_ffi_service_destroy unregisters a service and releases its handle.
//
// It returns 0 on success and -2 for an unknown handle. It never panics.
func weld_ffi_service_destroy(handle C.longlong) (rc C.int) {
	defer func() {
		r := recover()
		if r != nil {
			rc = C.int(ffiErrInternal)
		}
	}()
	key := int64(handle)
	if key <= 0 {
		return C.int(ffiErrHandle)
	}
	ffiMu.Lock()
	e, ok := ffiServices[key]
	if !ok {
		ffiMu.Unlock()
		return C.int(ffiErrHandle)
	}
	delete(ffiServices, key)
	delete(ffiSvcStash, key)
	delete(ffiSvcSeq, key)
	for rkey := range ffiSvcReplies {
		if rkey.svc == key {
			delete(ffiSvcReplies, rkey)
		}
	}
	ffiMu.Unlock()
	err := e.svc.Close()
	if err != nil {
		return C.int(ffiErrInternal)
	}
	return C.int(ffiOK)
}

// ffiValidateServiceCallArgs checks service call buffer arguments.
func ffiValidateServiceCallArgs(req *C.char, reqLen C.int, respBuf *C.char, respBufLen C.int, outN *C.int) C.int {
	if outN == nil {
		return C.int(ffiErrNull)
	}
	if reqLen < 0 || respBufLen < 0 {
		return C.int(ffiErrNull)
	}
	if reqLen > 0 && req == nil {
		return C.int(ffiErrNull)
	}
	if respBufLen > 0 && respBuf == nil {
		return C.int(ffiErrNull)
	}
	return C.int(ffiOK)
}

// ffiCopyServiceReply copies a service reply into the caller buffer.
func ffiCopyServiceReply(resp []byte, respBuf *C.char, respBufLen C.int, outN *C.int) C.int {
	if len(resp) > int(respBufLen) {
		*outN = C.int(0)
		return C.int(ffiErrShort)
	}
	code := ffiCopyToC(respBuf, respBufLen, resp)
	if code != C.int(ffiOK) {
		*outN = C.int(0)
		return code
	}
	*outN = C.int(len(resp))
	return C.int(ffiOK)
}

// weld_ffi_service_call_nb calls a service with a bounded wait.
//
// req holds reqLen bytes. A zero length allows a NULL pointer. On a reply
// it copies up to respBufLen bytes into respBuf, writes the byte count to
// outN, and returns 0. When no reply arrives before the internal bound it
// writes 0 to outN and returns 1 so the caller may retry. A reply larger
// than the buffer reports -4 without truncation. NULL pointers report -1
// and unknown handles report -2. It never panics.
func weld_ffi_service_call_nb(handle C.longlong, req *C.char, reqLen C.int, respBuf *C.char, respBufLen C.int, outN *C.int) (rc C.int) {
	defer func() {
		r := recover()
		if r != nil {
			rc = C.int(ffiErrInternal)
		}
	}()
	key := int64(handle)
	if key <= 0 {
		return C.int(ffiErrHandle)
	}
	if code := ffiValidateServiceCallArgs(req, reqLen, respBuf, respBufLen, outN); code != C.int(ffiOK) {
		return code
	}
	ffiMu.Lock()
	e, ok := ffiServices[key]
	ffiMu.Unlock()
	if !ok {
		return C.int(ffiErrHandle)
	}
	payload, code := ffiReadBytes(req, reqLen)
	if code != C.int(ffiOK) {
		return code
	}
	ctx, cancel := context.WithTimeout(context.Background(), ffiCallTimeout)
	defer cancel()
	resp, err := e.svc.Call(ctx, payload)
	if err != nil {
		cerr := ctx.Err()
		if cerr != nil {
			*outN = C.int(0)
			return C.int(ffiEmpty)
		}
		return C.int(ffiErrInternal)
	}
	return ffiCopyServiceReply(resp, respBuf, respBufLen, outN)
}

// ffiValidateServeArgs checks serve request buffer arguments.
func ffiValidateServeArgs(reqBuf *C.char, reqBufLen C.int, outN *C.int, outSeq *C.longlong) C.int {
	if outN == nil || outSeq == nil {
		return C.int(ffiErrNull)
	}
	if reqBufLen < 0 {
		return C.int(ffiErrNull)
	}
	if reqBufLen > 0 && reqBuf == nil {
		return C.int(ffiErrNull)
	}
	return C.int(ffiOK)
}

// ffiCheckServiceUsable reports whether a service can serve requests.
func ffiCheckServiceUsable(svc *Service) C.int {
	svc.bridge.mu.Lock()
	bridgeClosed := svc.bridge.closed
	svc.bridge.mu.Unlock()
	if bridgeClosed {
		return C.int(ffiErrInternal)
	}
	svc.mu.Lock()
	svcClosed := svc.closed
	svc.mu.Unlock()
	if svcClosed {
		return C.int(ffiErrInternal)
	}
	return C.int(ffiOK)
}

// ffiDequeueServePayload returns one pending request payload and responder.
func ffiDequeueServePayload(svc *Service, key int64, outN *C.int, outSeq *C.longlong) ([]byte, chan []byte, C.int) {
	ffiMu.Lock()
	stashed := ffiSvcStash[key]
	haveStash := len(stashed) > 0
	var first ffiStashedReq
	if haveStash {
		first = stashed[0]
		ffiSvcStash[key] = stashed[1:]
		if len(ffiSvcStash[key]) == 0 {
			delete(ffiSvcStash, key)
		}
	}
	ffiMu.Unlock()
	if haveStash {
		return first.payload, first.reply, C.int(ffiOK)
	}
	select {
	case out := <-svc.requests:
		return out.Payload, out.reply, C.int(ffiOK)
	default:
		*outN = C.int(0)
		*outSeq = C.longlong(0)
		return nil, nil, C.int(ffiEmpty)
	}
}

// ffiRestashServePayload stores a request payload for retry.
func ffiRestashServePayload(key int64, payload []byte, reply chan []byte) {
	ffiMu.Lock()
	ffiSvcStash[key] = append([]ffiStashedReq{{payload: payload, reply: reply}}, ffiSvcStash[key]...)
	ffiMu.Unlock()
}

// ffiTrackServeReply records a dequeued responder and returns its sequence.
func ffiTrackServeReply(key int64, reply chan []byte) int64 {
	ffiMu.Lock()
	seq := ffiSvcSeq[key] + 1
	ffiSvcSeq[key] = seq
	ffiSvcReplies[ffiSvcReplyKey{svc: key, seq: seq}] = reply
	ffiMu.Unlock()
	return seq
}

// weld_ffi_service_serve_nb dequeues one service request without blocking.
//
// When a request is ready it copies the payload into reqBuf, writes the
// byte count to outN, writes a server sequence id to outSeq, and returns
// 0. The sequence id selects the pending responder for a later
// weld_ffi_service_respond call with the same handle. When no request is
// ready it writes 0 to outN and outSeq and returns 1 so the caller can
// tell no data from empty data. A request larger than reqBufLen is stashed
// and reported as -4 without truncation; retry with a larger buffer. NULL
// pointers report -1 and unknown handles report -2. It never panics.
func weld_ffi_service_serve_nb(handle C.longlong, reqBuf *C.char, reqBufLen C.int, outN *C.int, outSeq *C.longlong) (rc C.int) {
	defer func() {
		r := recover()
		if r != nil {
			rc = C.int(ffiErrInternal)
		}
	}()
	key := int64(handle)
	if key <= 0 {
		return C.int(ffiErrHandle)
	}
	if code := ffiValidateServeArgs(reqBuf, reqBufLen, outN, outSeq); code != C.int(ffiOK) {
		return code
	}
	ffiMu.Lock()
	e, ok := ffiServices[key]
	ffiMu.Unlock()
	if !ok {
		return C.int(ffiErrHandle)
	}
	svc := e.svc
	if code := ffiCheckServiceUsable(svc); code != C.int(ffiOK) {
		return code
	}
	payload, reply, code := ffiDequeueServePayload(svc, key, outN, outSeq)
	if code != C.int(ffiOK) {
		return code
	}
	if len(payload) > int(reqBufLen) {
		ffiRestashServePayload(key, payload, reply)
		return C.int(ffiErrShort)
	}
	code = ffiCopyToC(reqBuf, reqBufLen, payload)
	if code != C.int(ffiOK) {
		ffiRestashServePayload(key, payload, reply)
		return code
	}
	seq := ffiTrackServeReply(key, reply)
	*outN = C.int(len(payload))
	*outSeq = C.longlong(seq)
	return C.int(ffiOK)
}

// weld_ffi_service_respond replies to one served request.
//
// seq must be a sequence id previously returned by
// weld_ffi_service_serve_nb for the same handle. resp holds respLen bytes;
// a zero length allows a NULL pointer and delivers an empty reply. The
// first respond for a seq delivers the reply and returns 0; an unknown or
// reused seq reports -2. A reply larger than maxPayloadBytes reports -3.
// NULL pointers report -1 and unknown handles report -2. It never blocks
// and never panics.
func weld_ffi_service_respond(handle C.longlong, seq C.longlong, resp *C.char, respLen C.int) (rc C.int) {
	defer func() {
		r := recover()
		if r != nil {
			rc = C.int(ffiErrInternal)
		}
	}()
	key := int64(handle)
	if key <= 0 {
		return C.int(ffiErrHandle)
	}
	sid := int64(seq)
	if sid <= 0 {
		return C.int(ffiErrHandle)
	}
	payload, code := ffiReadBytes(resp, respLen)
	if code != C.int(ffiOK) {
		return code
	}
	ffiMu.Lock()
	_, ok := ffiServices[key]
	if !ok {
		ffiMu.Unlock()
		return C.int(ffiErrHandle)
	}
	rkey := ffiSvcReplyKey{svc: key, seq: sid}
	reply, ok := ffiSvcReplies[rkey]
	if !ok {
		ffiMu.Unlock()
		return C.int(ffiErrHandle)
	}
	delete(ffiSvcReplies, rkey)
	ffiMu.Unlock()
	select {
	case reply <- payload:
		return C.int(ffiOK)
	default:
		return C.int(ffiErrHandle)
	}
}

// weld_ffi_guard_create creates a guard and writes its int64 handle.
//
// outHandle must be non-NULL. It returns 0 on success and -1 for a NULL
// out pointer. It never panics.
func weld_ffi_guard_create(outHandle *C.longlong) (rc C.int) {
	defer func() {
		r := recover()
		if r != nil {
			rc = C.int(ffiErrInternal)
		}
	}()
	if outHandle == nil {
		return C.int(ffiErrNull)
	}
	g := NewGuard()
	ffiMu.Lock()
	id := ffiNextLocked()
	ffiGuards[id] = g
	ffiMu.Unlock()
	*outHandle = C.longlong(id)
	return C.int(ffiOK)
}

// weld_ffi_guard_trigger sets a guard to the triggered state.
//
// It returns 0 on success and -2 for an unknown handle. It never panics.
func weld_ffi_guard_trigger(handle C.longlong) (rc C.int) {
	defer func() {
		r := recover()
		if r != nil {
			rc = C.int(ffiErrInternal)
		}
	}()
	key := int64(handle)
	if key <= 0 {
		return C.int(ffiErrHandle)
	}
	ffiMu.Lock()
	g, ok := ffiGuards[key]
	ffiMu.Unlock()
	if !ok {
		return C.int(ffiErrHandle)
	}
	g.Trigger()
	return C.int(ffiOK)
}

// weld_ffi_guard_reset clears a guard back to the untriggered state.
//
// It returns 0 on success and -2 for an unknown handle. It never panics.
func weld_ffi_guard_reset(handle C.longlong) (rc C.int) {
	defer func() {
		r := recover()
		if r != nil {
			rc = C.int(ffiErrInternal)
		}
	}()
	key := int64(handle)
	if key <= 0 {
		return C.int(ffiErrHandle)
	}
	ffiMu.Lock()
	g, ok := ffiGuards[key]
	ffiMu.Unlock()
	if !ok {
		return C.int(ffiErrHandle)
	}
	g.Reset()
	return C.int(ffiOK)
}

// weld_ffi_guard_destroy releases a guard handle.
//
// It is idempotent: releasing an already released or never created handle
// still returns 0. It returns -2 for a non-positive handle value. It
// never panics.
func weld_ffi_guard_destroy(handle C.longlong) (rc C.int) {
	defer func() {
		r := recover()
		if r != nil {
			rc = C.int(ffiErrInternal)
		}
	}()
	key := int64(handle)
	if key <= 0 {
		return C.int(ffiErrHandle)
	}
	ffiMu.Lock()
	delete(ffiGuards, key)
	ffiMu.Unlock()
	return C.int(ffiOK)
}

// Pure Go shims for tests.
//
// Go test files cannot import "C", so these helpers live in this non-test
// file and drive the C ABI exports above with C memory managed internally.
// They use only Go builtin types so ffi_test.go stays free of cgo while
// exercising the exact export code paths, including handle validation.

// ffiGoVersion returns Version through weld_ffi_version.
func ffiGoVersion() (string, int32) {
	const bufLen = 64
	ptr := C.malloc(C.size_t(bufLen))
	if ptr == nil {
		return "", int32(ffiErrInternal)
	}
	defer C.free(ptr)
	out := (*C.char)(ptr)
	rc := weld_ffi_version(out, C.int(bufLen))
	if rc != C.int(ffiOK) {
		return "", int32(rc)
	}
	s := C.GoString(out)
	return s, int32(ffiOK)
}

// ffiGoIdentifier returns Identifier through weld_ffi_identifier.
func ffiGoIdentifier() (string, int32) {
	const bufLen = 64
	ptr := C.malloc(C.size_t(bufLen))
	if ptr == nil {
		return "", int32(ffiErrInternal)
	}
	defer C.free(ptr)
	out := (*C.char)(ptr)
	rc := weld_ffi_identifier(out, C.int(bufLen))
	if rc != C.int(ffiOK) {
		return "", int32(rc)
	}
	s := C.GoString(out)
	return s, int32(ffiOK)
}

// ffiGoVersionNull calls weld_ffi_version with a NULL buffer.
func ffiGoVersionNull() int32 {
	rc := weld_ffi_version(nil, C.int(64))
	return int32(rc)
}

// ffiGoVersionShort calls weld_ffi_version with a two byte buffer.
func ffiGoVersionShort() int32 {
	const bufLen = 2
	ptr := C.malloc(C.size_t(bufLen))
	if ptr == nil {
		return int32(ffiErrInternal)
	}
	defer C.free(ptr)
	out := (*C.char)(ptr)
	rc := weld_ffi_version(out, C.int(bufLen))
	return int32(rc)
}

// ffiGoBridgeNew creates a bridge through weld_ffi_bridge_new.
func ffiGoBridgeNew() (int64, int32) {
	var h C.longlong
	rc := weld_ffi_bridge_new(&h)
	if rc != C.int(ffiOK) {
		return 0, int32(rc)
	}
	return int64(h), int32(ffiOK)
}

// ffiGoBridgeNewNull calls weld_ffi_bridge_new with a NULL out pointer.
func ffiGoBridgeNewNull() int32 {
	rc := weld_ffi_bridge_new(nil)
	return int32(rc)
}

// ffiGoBridgeFree frees a bridge through weld_ffi_bridge_free.
func ffiGoBridgeFree(handle int64) int32 {
	rc := weld_ffi_bridge_free(C.longlong(handle))
	return int32(rc)
}

// ffiGoJoin joins discovery through weld_ffi_join.
func ffiGoJoin(handle int64, participant string, domain int32) int32 {
	cs := C.CString(participant)
	if cs == nil {
		return int32(ffiErrInternal)
	}
	defer C.free(unsafe.Pointer(cs))
	rc := weld_ffi_join(C.longlong(handle), cs, C.int(domain))
	return int32(rc)
}

// ffiGoJoinNullParticipant calls weld_ffi_join with a NULL participant.
func ffiGoJoinNullParticipant(handle int64) int32 {
	rc := weld_ffi_join(C.longlong(handle), nil, C.int(0))
	return int32(rc)
}

// ffiGoCreateWithStrings creates node and topic C strings and calls create.
func ffiGoCreateWithStrings(bridgeHandle int64, nodeName, topic string, create func(C.longlong, *C.char, *C.char, *C.longlong) C.int) (int64, int32) {
	ns := C.CString(nodeName)
	if ns == nil {
		return 0, int32(ffiErrInternal)
	}
	defer C.free(unsafe.Pointer(ns))
	ts := C.CString(topic)
	if ts == nil {
		return 0, int32(ffiErrInternal)
	}
	defer C.free(unsafe.Pointer(ts))
	var h C.longlong
	rc := create(C.longlong(bridgeHandle), ns, ts, &h)
	if rc != C.int(ffiOK) {
		return 0, int32(rc)
	}
	return int64(h), int32(ffiOK)
}

// ffiGoPubCreate creates a publisher through weld_ffi_pub_create.
func ffiGoPubCreate(bridgeHandle int64, nodeName string, topic string) (int64, int32) {
	return ffiGoCreateWithStrings(bridgeHandle, nodeName, topic, weld_ffi_pub_create)
}

// ffiGoPubDestroy destroys a publisher through weld_ffi_pub_destroy.
func ffiGoPubDestroy(handle int64) int32 {
	rc := weld_ffi_pub_destroy(C.longlong(handle))
	return int32(rc)
}

// ffiGoPubPublish publishes through weld_ffi_pub_publish.
func ffiGoPubPublish(handle int64, data []byte) int32 {
	var ptr *C.char
	if len(data) > 0 {
		ptr = (*C.char)(unsafe.Pointer(&data[0]))
	}
	rc := weld_ffi_pub_publish(C.longlong(handle), ptr, C.int(len(data)))
	return int32(rc)
}

// ffiGoSubCreate creates a subscription through weld_ffi_sub_create.
func ffiGoSubCreate(bridgeHandle int64, nodeName string, topic string) (int64, int32) {
	return ffiGoCreateWithStrings(bridgeHandle, nodeName, topic, weld_ffi_sub_create)
}

// ffiGoSubDestroy destroys a subscription through weld_ffi_sub_destroy.
func ffiGoSubDestroy(handle int64) int32 {
	rc := weld_ffi_sub_destroy(C.longlong(handle))
	return int32(rc)
}

// ffiGoSubTakePoll takes one message through weld_ffi_sub_take_poll.
//
// It returns the payload, the byte count, and the export code. Code 1 with
// nil data means no message was ready.
func ffiGoSubTakePoll(handle int64, capacity int) ([]byte, int32, int32) {
	if capacity < 0 {
		capacity = 0
	}
	var ptr *C.char
	var owned unsafe.Pointer
	if capacity > 0 {
		owned = C.malloc(C.size_t(capacity))
		if owned == nil {
			return nil, 0, int32(ffiErrInternal)
		}
		defer C.free(owned)
		ptr = (*C.char)(owned)
	}
	var n C.int
	rc := weld_ffi_sub_take_poll(C.longlong(handle), ptr, C.int(capacity), &n)
	if rc == C.int(ffiEmpty) {
		return nil, 0, int32(ffiEmpty)
	}
	if rc != C.int(ffiOK) {
		return nil, 0, int32(rc)
	}
	if int(n) == 0 {
		return []byte{}, 0, int32(ffiOK)
	}
	out := C.GoBytes(owned, n)
	return out, int32(n), int32(ffiOK)
}

// ffiGoTakePollNullN calls weld_ffi_sub_take_poll with a NULL outN.
func ffiGoTakePollNullN(handle int64) int32 {
	rc := weld_ffi_sub_take_poll(C.longlong(handle), nil, C.int(0), nil)
	return int32(rc)
}

// ffiGoSubTakeInfo reports the last take attribution for a subscription.
//
// It returns the 16 byte writer GID, the per-subscription receive
// sequence, and the export code. A handle with no delivered take
// reports the handle code.
func ffiGoSubTakeInfo(handle int64) ([16]byte, uint64, int32) {
	var empty [16]byte
	owned := C.malloc(C.size_t(16))
	if owned == nil {
		return empty, 0, int32(ffiErrInternal)
	}
	defer C.free(owned)
	var seq C.ulonglong
	rc := weld_ffi_sub_take_info(C.longlong(handle), &seq, (*C.char)(owned), C.int(16))
	if rc != C.int(ffiOK) {
		return empty, 0, int32(rc)
	}
	var gid [16]byte
	copy(gid[:], C.GoBytes(owned, 16))
	return gid, uint64(seq), int32(ffiOK)
}

// ffiGoServiceCreate registers a service through weld_ffi_service_create.
func ffiGoServiceCreate(bridgeHandle int64, name string) (int64, int32) {
	cs := C.CString(name)
	if cs == nil {
		return 0, int32(ffiErrInternal)
	}
	defer C.free(unsafe.Pointer(cs))
	var h C.longlong
	rc := weld_ffi_service_create(C.longlong(bridgeHandle), cs, &h)
	if rc != C.int(ffiOK) {
		return 0, int32(rc)
	}
	return int64(h), int32(ffiOK)
}

// ffiGoServiceDestroy destroys a service through weld_ffi_service_destroy.
func ffiGoServiceDestroy(handle int64) int32 {
	rc := weld_ffi_service_destroy(C.longlong(handle))
	return int32(rc)
}

// ffiGoServiceCallNB calls a service through weld_ffi_service_call_nb.
//
// It returns the reply, the byte count, and the export code. Code 1 with
// nil data means no reply arrived before the internal bound.
func ffiGoServiceCallNB(handle int64, req []byte, respCap int) ([]byte, int32, int32) {
	if respCap < 0 {
		respCap = 0
	}
	var rptr *C.char
	if len(req) > 0 {
		rptr = (*C.char)(unsafe.Pointer(&req[0]))
	}
	var sptr *C.char
	var owned unsafe.Pointer
	if respCap > 0 {
		owned = C.malloc(C.size_t(respCap))
		if owned == nil {
			return nil, 0, int32(ffiErrInternal)
		}
		defer C.free(owned)
		sptr = (*C.char)(owned)
	}
	var n C.int
	rc := weld_ffi_service_call_nb(C.longlong(handle), rptr, C.int(len(req)), sptr, C.int(respCap), &n)
	if rc == C.int(ffiEmpty) {
		return nil, 0, int32(ffiEmpty)
	}
	if rc != C.int(ffiOK) {
		return nil, 0, int32(rc)
	}
	if int(n) == 0 {
		return []byte{}, 0, int32(ffiOK)
	}
	out := C.GoBytes(owned, n)
	return out, int32(n), int32(ffiOK)
}

// ffiGoGuardCreate creates a guard through weld_ffi_guard_create.
func ffiGoGuardCreate() (int64, int32) {
	var h C.longlong
	rc := weld_ffi_guard_create(&h)
	if rc != C.int(ffiOK) {
		return 0, int32(rc)
	}
	return int64(h), int32(ffiOK)
}

// ffiGoGuardCreateNull calls weld_ffi_guard_create with a NULL pointer.
func ffiGoGuardCreateNull() int32 {
	rc := weld_ffi_guard_create(nil)
	return int32(rc)
}

// ffiGoGuardTrigger triggers a guard through weld_ffi_guard_trigger.
func ffiGoGuardTrigger(handle int64) int32 {
	rc := weld_ffi_guard_trigger(C.longlong(handle))
	return int32(rc)
}

// ffiGoGuardReset resets a guard through weld_ffi_guard_reset.
func ffiGoGuardReset(handle int64) int32 {
	rc := weld_ffi_guard_reset(C.longlong(handle))
	return int32(rc)
}
