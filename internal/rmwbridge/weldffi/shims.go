// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

package main

/*
#include <stdlib.h>
*/
import "C"

import (
	"unsafe"
)

// Pure Go shims for tests.
//
// Go test files cannot import "C", so these helpers live in this non-test
// file and drive the missing.go exports with C memory managed internally.
// They use only Go builtin types so missing_test.go stays free of cgo
// while exercising the exact export code paths, including handle
// validation.

// weldGoBridgeNewNoJoin creates a bridge without joining discovery.
func weldGoBridgeNewNoJoin() (int64, int32) {
	var h C.longlong
	rc := weld_ffi_bridge_new(&h)
	if rc != C.int(weldCodeOK) {
		return 0, int32(rc)
	}
	return int64(h), int32(weldCodeOK)
}

// weldGoBridgeFree frees a bridge through weld_ffi_bridge_free.
func weldGoBridgeFree(handle int64) int32 {
	rc := weld_ffi_bridge_free(C.longlong(handle))
	return int32(rc)
}

// weldGoJoin joins discovery through weld_ffi_join.
func weldGoJoin(handle int64, participant string, domain int32) int32 {
	cs := C.CString(participant)
	if cs == nil {
		return int32(weldCodeErrInternal)
	}
	defer C.free(unsafe.Pointer(cs))
	rc := weld_ffi_join(C.longlong(handle), cs, C.int(domain))
	return int32(rc)
}

// weldGoEndpointCreate builds node plus topic C strings and calls create.
func weldGoEndpointCreate(bridgeHandle int64, nodeName, topic string, create func(C.longlong, *C.char, *C.char, *C.longlong) C.int) (int64, int32) {
	ns := C.CString(nodeName)
	if ns == nil {
		return 0, int32(weldCodeErrInternal)
	}
	defer C.free(unsafe.Pointer(ns))
	ts := C.CString(topic)
	if ts == nil {
		return 0, int32(weldCodeErrInternal)
	}
	defer C.free(unsafe.Pointer(ts))
	var h C.longlong
	rc := create(C.longlong(bridgeHandle), ns, ts, &h)
	if rc != C.int(weldCodeOK) {
		return 0, int32(rc)
	}
	return int64(h), int32(weldCodeOK)
}

// weldGoSubCreate creates a subscription through weld_ffi_sub_create.
func weldGoSubCreate(bridgeHandle int64, nodeName string, topic string) (int64, int32) {
	return weldGoEndpointCreate(bridgeHandle, nodeName, topic, weld_ffi_sub_create)
}

// weldGoSubDestroy destroys a subscription through weld_ffi_sub_destroy.
func weldGoSubDestroy(handle int64) int32 {
	rc := weld_ffi_sub_destroy(C.longlong(handle))
	return int32(rc)
}

// weldGoPubCreate creates a publisher through weld_ffi_pub_create.
func weldGoPubCreate(bridgeHandle int64, nodeName string, topic string) (int64, int32) {
	return weldGoEndpointCreate(bridgeHandle, nodeName, topic, weld_ffi_pub_create)
}

// weldGoPubDestroy destroys a publisher through weld_ffi_pub_destroy.
func weldGoPubDestroy(handle int64) int32 {
	rc := weld_ffi_pub_destroy(C.longlong(handle))
	return int32(rc)
}

// weldGoPubPublish publishes through weld_ffi_pub_publish.
func weldGoPubPublish(handle int64, data []byte) int32 {
	var ptr *C.char
	if len(data) > 0 {
		ptr = (*C.char)(unsafe.Pointer(&data[0]))
	}
	rc := weld_ffi_pub_publish(C.longlong(handle), ptr, C.int(len(data)))
	return int32(rc)
}

// weldGoSvcCreate registers a service through weld_ffi_service_create.
func weldGoSvcCreate(bridgeHandle int64, name string) (int64, int32) {
	cs := C.CString(name)
	if cs == nil {
		return 0, int32(weldCodeErrInternal)
	}
	defer C.free(unsafe.Pointer(cs))
	var h C.longlong
	rc := weld_ffi_service_create(C.longlong(bridgeHandle), cs, &h)
	if rc != C.int(weldCodeOK) {
		return 0, int32(rc)
	}
	return int64(h), int32(weldCodeOK)
}

// weldGoSvcDestroy destroys a service through weld_ffi_service_destroy.
func weldGoSvcDestroy(handle int64) int32 {
	rc := weld_ffi_service_destroy(C.longlong(handle))
	return int32(rc)
}

// weldGoSubHasData reports queued samples through weld_ffi_sub_has_data.
func weldGoSubHasData(handle int64) int32 {
	rc := weld_ffi_sub_has_data(C.longlong(handle))
	return int32(rc)
}

// weldGoSubSetDynamicType sets a dynamic type through the export.
func weldGoSubSetDynamicType(handle int64, name, hash string) int32 {
	tn := C.CString(name)
	if tn == nil {
		return int32(weldCodeErrInternal)
	}
	defer C.free(unsafe.Pointer(tn))
	th := C.CString(hash)
	if th == nil {
		return int32(weldCodeErrInternal)
	}
	defer C.free(unsafe.Pointer(th))
	rc := weld_ffi_sub_set_dynamic_type(C.longlong(handle), tn, th)
	return int32(rc)
}

// weldGoSvcSetType records a service type through the export.
func weldGoSvcSetType(handle int64, name, hash string) int32 {
	tn := C.CString(name)
	if tn == nil {
		return int32(weldCodeErrInternal)
	}
	defer C.free(unsafe.Pointer(tn))
	th := C.CString(hash)
	if th == nil {
		return int32(weldCodeErrInternal)
	}
	defer C.free(unsafe.Pointer(th))
	rc := weld_ffi_service_set_type(C.longlong(handle), tn, th)
	return int32(rc)
}

// weldGoSvcHasRequest counts servable requests through the export.
func weldGoSvcHasRequest(handle int64) (int32, int32) {
	var n C.int
	rc := weld_ffi_service_has_request(C.longlong(handle), &n)
	return int32(n), int32(rc)
}

// weldGoSvcHasResponse peeks for one reply through the export.
func weldGoSvcHasResponse(handle, seq int64, guid [8]byte, base string) (int32, int32) {
	g := guid
	cb := C.CString(base)
	if cb == nil {
		return 0, int32(weldCodeErrInternal)
	}
	defer C.free(unsafe.Pointer(cb))
	var n C.int
	rc := weld_ffi_service_has_response(C.longlong(handle), C.longlong(seq), (*C.char)(unsafe.Pointer(&g[0])), cb, &n)
	return int32(n), int32(rc)
}

// weldGoSvcSendNB enqueues one in-memory request through the export.
func weldGoSvcSendNB(handle, seq int64, guid [8]byte, req []byte) int32 {
	g := guid
	var ptr *C.char
	if len(req) > 0 {
		ptr = (*C.char)(unsafe.Pointer(&req[0]))
	}
	rc := weld_ffi_service_send_nb(C.longlong(handle), C.longlong(seq), (*C.char)(unsafe.Pointer(&g[0])), ptr, C.int(len(req)))
	return int32(rc)
}

// weldGoSvcPollReply waits for one in-memory reply through the export.
func weldGoSvcPollReply(handle, seq int64, guid [8]byte, capacity int) ([]byte, int32, int32) {
	if capacity < 0 {
		capacity = 0
	}
	g := guid
	var ptr *C.char
	var owned unsafe.Pointer
	if capacity > 0 {
		owned = C.malloc(C.size_t(capacity))
		if owned == nil {
			return nil, 0, int32(weldCodeErrInternal)
		}
		defer C.free(owned)
		ptr = (*C.char)(owned)
	}
	var n C.int
	rc := weld_ffi_service_poll_reply(C.longlong(handle), C.longlong(seq), (*C.char)(unsafe.Pointer(&g[0])), ptr, C.int(capacity), &n)
	if rc == C.int(weldCodeEmpty) {
		return nil, 0, int32(weldCodeEmpty)
	}
	if rc != C.int(weldCodeOK) {
		return nil, 0, int32(rc)
	}
	if int(n) == 0 {
		return []byte{}, 0, int32(weldCodeOK)
	}
	out := C.GoBytes(owned, n)
	return out, int32(n), int32(weldCodeOK)
}

// weldGoSvcSendRTPS publishes one RTPS request through the export.
func weldGoSvcSendRTPS(handle, seq int64, guid [8]byte, base string, req []byte) int32 {
	g := guid
	cb := C.CString(base)
	if cb == nil {
		return int32(weldCodeErrInternal)
	}
	defer C.free(unsafe.Pointer(cb))
	var ptr *C.char
	if len(req) > 0 {
		ptr = (*C.char)(unsafe.Pointer(&req[0]))
	}
	rc := weld_ffi_service_send_rtps(C.longlong(handle), C.longlong(seq), (*C.char)(unsafe.Pointer(&g[0])), cb, ptr, C.int(len(req)))
	return int32(rc)
}

// weldGoSvcTakeReply polls one RTPS reply through the export.
func weldGoSvcTakeReply(handle, seq int64, guid [8]byte, base string, capacity int) ([]byte, int32, int32) {
	if capacity < 0 {
		capacity = 0
	}
	g := guid
	cb := C.CString(base)
	if cb == nil {
		return nil, 0, int32(weldCodeErrInternal)
	}
	defer C.free(unsafe.Pointer(cb))
	var ptr *C.char
	var owned unsafe.Pointer
	if capacity > 0 {
		owned = C.malloc(C.size_t(capacity))
		if owned == nil {
			return nil, 0, int32(weldCodeErrInternal)
		}
		defer C.free(owned)
		ptr = (*C.char)(owned)
	}
	var n C.int
	rc := weld_ffi_service_take_reply(C.longlong(handle), C.longlong(seq), (*C.char)(unsafe.Pointer(&g[0])), cb, ptr, C.int(capacity), &n)
	if rc == C.int(weldCodeEmpty) {
		return nil, 0, int32(weldCodeEmpty)
	}
	if rc != C.int(weldCodeOK) {
		return nil, 0, int32(rc)
	}
	if int(n) == 0 {
		return []byte{}, 0, int32(weldCodeOK)
	}
	out := C.GoBytes(owned, n)
	return out, int32(n), int32(weldCodeOK)
}

// weldGoSvcServeNB dequeues one request through the existing serve export.
func weldGoSvcServeNB(handle int64, capacity int) ([]byte, int64, int32) {
	if capacity < 0 {
		capacity = 0
	}
	var ptr *C.char
	var owned unsafe.Pointer
	if capacity > 0 {
		owned = C.malloc(C.size_t(capacity))
		if owned == nil {
			return nil, 0, int32(weldCodeErrInternal)
		}
		defer C.free(owned)
		ptr = (*C.char)(owned)
	}
	var n C.int
	var seq C.longlong
	rc := weld_ffi_service_serve_nb(C.longlong(handle), ptr, C.int(capacity), &n, &seq)
	if rc == C.int(weldCodeEmpty) {
		return nil, 0, int32(weldCodeEmpty)
	}
	if rc != C.int(weldCodeOK) {
		return nil, 0, int32(rc)
	}
	if int(n) == 0 {
		return []byte{}, int64(seq), int32(weldCodeOK)
	}
	out := C.GoBytes(owned, n)
	return out, int64(seq), int32(weldCodeOK)
}

// weldGoSvcRespond replies through the existing respond export.
func weldGoSvcRespond(handle, seq int64, resp []byte) int32 {
	var ptr *C.char
	if len(resp) > 0 {
		ptr = (*C.char)(unsafe.Pointer(&resp[0]))
	}
	rc := weld_ffi_service_respond(C.longlong(handle), C.longlong(seq), ptr, C.int(len(resp)))
	return int32(rc)
}

// weldGoSubSetFilter records one content filter through the export.
func weldGoSubSetFilter(handle int64, expr string, params []string) int32 {
	ce := C.CString(expr)
	if ce == nil {
		return int32(weldCodeErrInternal)
	}
	defer C.free(unsafe.Pointer(ce))
	var owned unsafe.Pointer
	var blob *C.char
	if len(params) > 0 {
		total := 0
		for _, p := range params {
			total += len(p) + 1
		}
		owned = C.malloc(C.size_t(total))
		if owned == nil {
			return int32(weldCodeErrInternal)
		}
		defer C.free(owned)
		raw := unsafe.Slice((*byte)(owned), total)
		off := 0
		for _, p := range params {
			copy(raw[off:], p)
			off += len(p)
			raw[off] = 0
			off++
		}
		blob = (*C.char)(owned)
	}
	rc := weld_ffi_sub_set_content_filter(C.longlong(handle), ce, blob, C.int(len(params)))
	return int32(rc)
}

// weldGoRemoteEpCount counts live remotes through the export.
func weldGoRemoteEpCount(bridge int64, topic string, role int32) (int64, int32) {
	ct := C.CString(topic)
	if ct == nil {
		return 0, int32(weldCodeErrInternal)
	}
	defer C.free(unsafe.Pointer(ct))
	var n C.longlong
	rc := weld_ffi_remote_ep_count(C.longlong(bridge), ct, C.int(role), &n)
	return int64(n), int32(rc)
}

// weldGoRemoteEpAt copies one live remote through the export.
func weldGoRemoteEpAt(bridge int64, topic string, role int32, index int64) (string, [16]byte, int32) {
	var gid [16]byte
	ct := C.CString(topic)
	if ct == nil {
		return "", gid, int32(weldCodeErrInternal)
	}
	defer C.free(unsafe.Pointer(ct))
	const typeCap = 1024
	typeOwned := C.malloc(C.size_t(typeCap))
	if typeOwned == nil {
		return "", gid, int32(weldCodeErrInternal)
	}
	defer C.free(typeOwned)
	gidOwned := C.malloc(C.size_t(16))
	if gidOwned == nil {
		return "", gid, int32(weldCodeErrInternal)
	}
	defer C.free(gidOwned)
	rc := weld_ffi_remote_ep_at(C.longlong(bridge), ct, C.int(role), C.longlong(index), (*C.char)(typeOwned), C.int(typeCap), (*C.char)(gidOwned), C.int(16))
	if rc != C.int(weldCodeOK) {
		return "", gid, int32(rc)
	}
	typ := C.GoString((*C.char)(typeOwned))
	copy(gid[:], C.GoBytes(gidOwned, 16))
	return typ, gid, int32(weldCodeOK)
}

// weldGoReportLiveliness reports liveliness through the export.
func weldGoReportLiveliness(bridge int64, participant string, domain int32, alive int32, generation uint64) int32 {
	cp := C.CString(participant)
	if cp == nil {
		return int32(weldCodeErrInternal)
	}
	defer C.free(unsafe.Pointer(cp))
	rc := weld_ffi_report_liveliness(C.longlong(bridge), cp, C.int(domain), C.int(alive), C.ulonglong(generation))
	return int32(rc)
}
