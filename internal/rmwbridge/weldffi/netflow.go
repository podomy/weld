// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

// This file exposes network flow endpoint data backed by the live RTPS backend.
//
// The local entry reports the node's advertised IPv4 from the same
// source the backend uses for SPDP locators with the bound unicast
// port. Remote entries report discovered unicast ports from the route
// table joined with live SEDP endpoints; entries with an unknown port
// never surface and torn discovery reads fail the entry so the C layer
// skips it rather than zero-filling. Flow label and DSCP stay 0: this
// transport applies no per-flow QoS marking.
//
// The backend offers no public accessor for its bound socket or its
// discovered routes, so the snapshot reads the same-process backend
// struct fields the SPDP announce path itself uses. Every field access
// is type-checked and the route copy runs under the backend read lock,
// so any layout drift fails closed instead of fabricating data.
package main

/*
#include <stdlib.h>
*/
import "C"

import (
	"reflect"
	"sort"
	"sync"
	"unsafe"

	"github.com/podomy/weld/internal/transports/dds"
	"github.com/podomy/weld/internal/transports/dds/rtps"
)

// weldNetflowLoopback is the destination IP reported for remote flows.
//
// User DATA fans out through sendToPorts to loopback ports discovered
// from SPDP, so the transport addresses every remote peer at loopback.
const weldNetflowLoopback = "127.0.0.1"

// weldNetflowEp is one flow endpoint: destination IP plus UDP port.
type weldNetflowEp struct {
	ip   string
	port int
}

// Expected private backend field types for the flow snapshot.
//
// Pointer element forms avoid copying lock values through reflect.
var (
	weldNetflowMuType     = reflect.TypeFor[sync.RWMutex]()
	weldNetflowUniType    = reflect.TypeFor[*rtps.UnicastConn]()
	weldNetflowRoutesType = reflect.TypeFor[map[dds.GUIDPrefix][]int]()
)

// weldNetflowField returns a pointer to the named backend field.
//
// It reports false for a nil backend, a missing field, or a type
// mismatch, so backend layout drift fails closed. The pointer is only
// safe to use as documented by each caller. It never panics.
func weldNetflowField(b *dds.Backend, name string, want reflect.Type) (unsafe.Pointer, bool) {
	if b == nil {
		return nil, false
	}
	v := reflect.ValueOf(b).Elem().FieldByName(name)
	if !v.IsValid() || v.Type() != want {
		return nil, false
	}
	return unsafe.Pointer(v.UnsafeAddr()), true
}

// weldNetflowLock returns the backend read lock.
//
// It reports false when the field is missing or mistyped. The caller
// must hold the lock while touching copied state. It never panics.
func weldNetflowLock(b *dds.Backend) (*sync.RWMutex, bool) {
	ptr, ok := weldNetflowField(b, "mu", weldNetflowMuType)
	if !ok {
		return nil, false
	}
	return (*sync.RWMutex)(ptr), true
}

// weldNetflowLocalAddr returns the advertised IPv4 plus bound unicast port.
//
// The IP comes from the same source the backend uses for SPDP locators.
// The port is the bound unicast socket port read under the backend read
// lock. It reports false when the backend is missing or the snapshot
// fails, and never fabricates an address or a zero port.
func weldNetflowLocalAddr(b *dds.Backend) (string, int, bool) {
	mu, ok := weldNetflowLock(b)
	if !ok {
		return "", 0, false
	}
	uniPtr, ok := weldNetflowField(b, "uni", weldNetflowUniType)
	if !ok {
		return "", 0, false
	}
	mu.RLock()
	defer mu.RUnlock()
	uni := *(**rtps.UnicastConn)(uniPtr)
	if uni == nil {
		return "", 0, false
	}
	port := uni.LocalPort()
	if port <= 0 || port > 65535 {
		return "", 0, false
	}
	ip := rtps.AdvertiseIPv4().String()
	if ip == "" {
		return "", 0, false
	}
	return ip, port, true
}

// weldNetflowRoutes returns a copy of the discovered unicast ports.
//
// The copy runs under the backend read lock so SPDP updates cannot race
// the read. It reports nil when the backend or its table is unavailable.
// It never panics.
func weldNetflowRoutes(b *dds.Backend) map[dds.GUIDPrefix][]int {
	mu, ok := weldNetflowLock(b)
	if !ok {
		return nil
	}
	routesPtr, ok := weldNetflowField(b, "routes", weldNetflowRoutesType)
	if !ok {
		return nil
	}
	mu.RLock()
	defer mu.RUnlock()
	src := *(*map[dds.GUIDPrefix][]int)(routesPtr)
	out := make(map[dds.GUIDPrefix][]int, len(src))
	for prefix, ports := range src {
		out[prefix] = append([]int(nil), ports...)
	}
	return out
}

// weldNetflowWantEp reports whether an endpoint fills a flow role.
//
// Only topic-kind endpoints matching the topic contribute, with writer
// peers for a local subscription and reader peers for a local
// publisher. It never panics.
func weldNetflowWantEp(ep dds.SEDPEndpoint, topic string, wantReader bool) bool {
	if ep.Kind != dds.TopicKindTopic {
		return false
	}
	if ep.ROSTopic != topic || ep.IsReader != wantReader {
		return false
	}
	return true
}

// weldNetflowAppendPorts adds one entry per known unicast port.
//
// Ports outside 1..65535 and duplicate entries are skipped rather than
// zero-filled. It never panics.
func weldNetflowAppendPorts(out []weldNetflowEp, seen map[weldNetflowEp]struct{}, ports []int) []weldNetflowEp {
	for _, port := range ports {
		if port <= 0 || port > 65535 {
			continue
		}
		key := weldNetflowEp{ip: weldNetflowLoopback, port: port}
		if _, dup := seen[key]; dup {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, key)
	}
	return out
}

// weldNetflowRemoteEps collects remote flow endpoints for a topic.
//
// Only topic-kind endpoints matching the topic contribute, with writer
// peers for a local subscription and reader peers for a local
// publisher. Every known unicast port for a matching participant prefix
// becomes one entry at the loopback destination the transport sends
// to; prefixes with no known port contribute nothing. Entries sort by
// port so C snapshots stay stable across discovery churn. It never
// panics.
func weldNetflowRemoteEps(b *dds.Backend, topic string, wantReader bool) []weldNetflowEp {
	if b == nil || topic == "" {
		return nil
	}
	routes := weldNetflowRoutes(b)
	if routes == nil {
		return nil
	}
	seen := make(map[weldNetflowEp]struct{})
	var out []weldNetflowEp
	for _, ep := range b.RemoteEndpoints() {
		if !weldNetflowWantEp(ep, topic, wantReader) {
			continue
		}
		out = weldNetflowAppendPorts(out, seen, routes[dds.GUIDPrefix(ep.ID.Prefix)])
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].port != out[j].port {
			return out[i].port < out[j].port
		}
		return out[i].ip < out[j].ip
	})
	return out
}

// weldNetflowBackend returns the attached RTPS backend for a bridge.
//
// It reports false for an unknown bridge. A nil backend means the join
// fell back to in-memory only with no transport socket to report. It
// never panics.
func weldNetflowBackend(bridgeID int64) (*dds.Backend, bool) {
	weldMu.Lock()
	defer weldMu.Unlock()
	_, ok := weldBridges[bridgeID]
	if !ok {
		return nil, false
	}
	return weldRTPS[bridgeID], true
}

// weld_ffi_netflow_local reports the local socket address for a bridge.
//
// ipBuf receives the NUL terminated advertised IPv4 and portOut the
// bound unicast port. It returns 0 on success, -1 for NULL pointers, -2
// for an unknown bridge, -3 when no backend is attached (in-memory
// only), and -4 when the buffer is too small. It never panics.
//
//export weld_ffi_netflow_local
func weld_ffi_netflow_local(bridge C.longlong, ipBuf *C.char, ipLen C.int, portOut *C.int) (rc C.int) {
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
	if ipBuf == nil || portOut == nil {
		return C.int(weldCodeErrNull)
	}
	backend, ok := weldNetflowBackend(bkey)
	if !ok {
		return C.int(weldCodeErrHandle)
	}
	if backend == nil {
		return C.int(weldCodeErrInternal)
	}
	ip, port, ok := weldNetflowLocalAddr(backend)
	if !ok {
		return C.int(weldCodeErrInternal)
	}
	if code := weldWriteCString(ipBuf, ipLen, ip); code != C.int(weldCodeOK) {
		return code
	}
	*portOut = C.int(port)
	return C.int(weldCodeOK)
}

// weldNetflowCheckRemote validates bridge plus topic plus role for remote queries.
//
// Roles mirror the WELD_FILTER_ROLE_* enum with local meaning: a reader
// role asks for reader peers (local publisher) and a writer role asks
// for writer peers (local subscription). Any other role reports the
// null code. It never panics.
func weldNetflowCheckRemote(bridge C.longlong, topic *C.char, role C.int) (int64, string, bool, C.int) {
	if topic == nil {
		return 0, "", false, C.int(weldCodeErrNull)
	}
	var wantReader bool
	switch int(role) {
	case weldRoleReader:
		wantReader = true
	case weldRoleWriter:
		wantReader = false
	default:
		return 0, "", false, C.int(weldCodeErrNull)
	}
	bkey := int64(bridge)
	if bkey <= 0 {
		return 0, "", false, C.int(weldCodeErrHandle)
	}
	name, code := weldReadCString(topic)
	if code != C.int(weldCodeOK) {
		return 0, "", false, code
	}
	backend, ok := weldNetflowBackend(bkey)
	if !ok {
		return 0, "", false, C.int(weldCodeErrHandle)
	}
	if backend == nil {
		return 0, "", false, C.int(weldCodeErrInternal)
	}
	return bkey, name, wantReader, C.int(weldCodeOK)
}

// weld_ffi_netflow_remote_count counts remote flow entries for a topic.
//
// Only peers with at least one known unicast port count; unknown ports
// never surface. It returns 0 on success, -1 for NULL pointers or a
// bad role, -2 for an unknown bridge, and -3 when no backend is
// attached. It never panics.
//
//export weld_ffi_netflow_remote_count
func weld_ffi_netflow_remote_count(bridge C.longlong, topic *C.char, role C.int, out *C.longlong) (rc C.int) {
	defer func() {
		r := recover()
		if r != nil {
			rc = C.int(weldCodeErrInternal)
		}
	}()
	if out == nil {
		return C.int(weldCodeErrNull)
	}
	bkey, name, wantReader, code := weldNetflowCheckRemote(bridge, topic, role)
	if code != C.int(weldCodeOK) {
		return code
	}
	weldMu.Lock()
	backend := weldRTPS[bkey]
	weldMu.Unlock()
	*out = C.longlong(len(weldNetflowRemoteEps(backend, name, wantReader)))
	return C.int(weldCodeOK)
}

// weld_ffi_netflow_remote_at copies one remote flow entry by index.
//
// ipBuf receives the NUL terminated destination IP and portOut the
// discovered unicast port. A short buffer reports -4 and an index past
// the end reports -2; the C layer skips either entry. Return codes
// match the count export. It never panics.
//
//export weld_ffi_netflow_remote_at
func weld_ffi_netflow_remote_at(bridge C.longlong, topic *C.char, role C.int, index C.longlong, ipBuf *C.char, ipLen C.int, portOut *C.int) (rc C.int) {
	defer func() {
		r := recover()
		if r != nil {
			rc = C.int(weldCodeErrInternal)
		}
	}()
	if ipBuf == nil || portOut == nil {
		return C.int(weldCodeErrNull)
	}
	if int64(index) < 0 {
		return C.int(weldCodeErrHandle)
	}
	bkey, name, wantReader, code := weldNetflowCheckRemote(bridge, topic, role)
	if code != C.int(weldCodeOK) {
		return code
	}
	weldMu.Lock()
	backend := weldRTPS[bkey]
	weldMu.Unlock()
	eps := weldNetflowRemoteEps(backend, name, wantReader)
	if int64(index) >= int64(len(eps)) {
		return C.int(weldCodeErrHandle)
	}
	ep := eps[int(index)]
	if code := weldWriteCString(ipBuf, ipLen, ep.ip); code != C.int(weldCodeOK) {
		return code
	}
	*portOut = C.int(ep.port)
	return C.int(weldCodeOK)
}
