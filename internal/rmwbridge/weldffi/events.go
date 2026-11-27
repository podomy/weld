// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

// This file backs RMW event statuses and remote endpoint QoS reads.
//
// Deadline misses are tracked per endpoint in this layer: the C shim
// reports each endpoint deadline once (from its stored QoS profile) and
// this layer records the last successful delivery per handle. A getter
// reports the cumulative missed periods as (now minus last) divided by
// deadline, monotonic across deliveries. Liveliness uses the existing
// matched-count exports with no new state: alive writers are the current
// matched publishers for a subscription and alive readers are the
// current matched subscriptions for a publisher. Incompatible QoS counts
// wire mismatches via CheckWireCompatibility between local SEDP offers
// and remote requests (or the reverse): identity, type, domain,
// partitions, and keying must match while RxO refuses, so partition or
// keying mismatches never count as QoS incompatibility. Endpoints that
// predate QoS capture or parse malformed stay fail-closed: unknown QoS
// never counts and malformed counts once as incompatible. Remote QoS
// reads expose the parsed RxO wire offer for info-by-topic queries.
//
// Local SEDP endpoints are read through the backend public accessor.
// It never panics.
package main

/*
#include <stdlib.h>
*/
import "C"

import (
	"time"

	"github.com/podomy/weld/internal/transports/dds"
)

// weldDeadlineState carries one endpoint deadline plus delivery progress.
//
// DeadlineNs is zero for infinite or unknown deadlines. LastNs is the
// unix nanos of the last successful delivery, or the setter time when
// nothing has delivered yet. Base is the cumulative total at LastNs, so
// the current total is base plus elapsed periods. Totals stay monotonic
// across deliveries by construction.
type weldDeadlineState struct {
	deadlineNs uint64
	lastNs     int64
	base       uint64
}

// weldEventDeadlineState is guarded by weldMu with the handle registry.
var (
	weldSubDeadlines = make(map[int64]*weldDeadlineState)
	weldPubDeadlines = make(map[int64]*weldDeadlineState)
)

// weldIncompatSeen tracks wire-incompatible remote GUIDs ever seen.
//
// Per endpoint handle the set holds hex GUID strings of remotes counted
// as QoS-incompatible, so totals stay monotonic when remotes come and
// go. Guarded by weldMu.
var weldIncompatSeen = make(map[int64]map[string]struct{})

// weldNowNanos returns the current unix nanos for deadline math.
func weldNowNanos() int64 {
	return time.Now().UnixNano()
}

// weldDeadlineNs converts sec plus nsec to nanos, saturating on range.
//
// Negative inputs and overflow saturate to zero meaning infinite or
// unknown, so callers never store a negative deadline. It never panics.
func weldDeadlineNs(sec, nsec int64) uint64 {
	if sec < 0 || nsec < 0 {
		return 0
	}
	const nanosPerSec = int64(1_000_000_000)
	if sec > int64(^uint64(0)>>1)/nanosPerSec {
		return 0
	}
	total := uint64(sec)*uint64(nanosPerSec) + uint64(nsec)
	if total == 0 {
		return 0
	}
	return total
}

// weldDeadlineSetLocked records one endpoint deadline under weldMu.
//
// The caller must hold weldMu. Last delivery starts at now with base
// zero so misses accrue only after a full period without delivery.
func weldDeadlineSetLocked(store map[int64]*weldDeadlineState, key int64, deadlineNs uint64, now int64) {
	st, ok := store[key]
	if !ok {
		st = &weldDeadlineState{}
		store[key] = st
	}
	st.deadlineNs = deadlineNs
	st.lastNs = now
	st.base = 0
}

// weldDeadlineNoteLocked records one successful delivery under weldMu.
//
// The caller must hold weldMu. The base advances to the current total
// before last moves to now, so the cumulative total never decreases.
func weldDeadlineNoteLocked(store map[int64]*weldDeadlineState, key int64, now int64) {
	st, ok := store[key]
	if !ok {
		return
	}
	if st.deadlineNs == 0 || st.lastNs == 0 {
		st.lastNs = now
		return
	}
	elapsed := now - st.lastNs
	if elapsed < 0 {
		st.lastNs = now
		return
	}
	periods := uint64(elapsed) / st.deadlineNs
	st.base += periods
	st.lastNs = now
}

// weldDeadlineTotalLocked reports the cumulative missed periods under weldMu.
//
// The caller must hold weldMu. A zero deadline or no delivery record
// reports zero. It never panics.
func weldDeadlineTotalLocked(store map[int64]*weldDeadlineState, key int64, now int64) uint64 {
	st, ok := store[key]
	if !ok {
		return 0
	}
	if st.deadlineNs == 0 || st.lastNs == 0 {
		return 0
	}
	elapsed := now - st.lastNs
	if elapsed <= 0 {
		return st.base
	}
	return st.base + uint64(elapsed)/st.deadlineNs
}

// weldNoteSubDelivery records one successful subscription take.
//
// It runs after a take copies bytes out, so deadline math sees fresh
// delivery. Missing deadline state is a no-op for endpoints that never
// reported a deadline. It never panics.
func weldNoteSubDelivery(key int64) {
	now := weldNowNanos()
	weldMu.Lock()
	defer weldMu.Unlock()
	if _, ok := weldSubs[key]; !ok {
		return
	}
	weldDeadlineNoteLocked(weldSubDeadlines, key, now)
}

// weldNotePubPublish records one successful publisher send.
//
// It runs after the in-memory publish succeeds, mirroring the take
// note. It never panics.
func weldNotePubPublish(key int64) {
	now := weldNowNanos()
	weldMu.Lock()
	defer weldMu.Unlock()
	if _, ok := weldPubs[key]; !ok {
		return
	}
	weldDeadlineNoteLocked(weldPubDeadlines, key, now)
}

// weldDropSubDeadlines removes sub deadline state for a bridge.
//
// The caller must hold weldMu.
func weldDropSubDeadlines(bridgeID int64) {
	for id := range weldSubDeadlines {
		if e, ok := weldSubs[id]; !ok || e.bridgeID == bridgeID {
			delete(weldSubDeadlines, id)
		}
	}
}

// weldDropPubDeadlines removes pub deadline state for a bridge.
//
// The caller must hold weldMu.
func weldDropPubDeadlines(bridgeID int64) {
	for id := range weldPubDeadlines {
		if e, ok := weldPubs[id]; !ok || e.bridgeID == bridgeID {
			delete(weldPubDeadlines, id)
		}
	}
}

// weldKeepIncompat reports whether one incompat entry belongs elsewhere.
//
// Entries for other bridges survive teardown. The caller must hold weldMu.
func weldKeepIncompat(id int64, bridgeID int64) bool {
	if e, ok := weldSubs[id]; ok && e.bridgeID != bridgeID {
		return true
	}
	if e, ok := weldPubs[id]; ok && e.bridgeID != bridgeID {
		return true
	}
	return false
}

// weldDropEventExtrasLocked removes deadline plus incompat state.
//
// The caller must hold weldMu. Entries whose handle is already gone
// drop too so bridge teardown never leaks per-handle state.
func weldDropEventExtrasLocked(bridgeID int64) {
	weldDropSubDeadlines(bridgeID)
	weldDropPubDeadlines(bridgeID)
	for id := range weldIncompatSeen {
		if weldKeepIncompat(id, bridgeID) {
			continue
		}
		delete(weldIncompatSeen, id)
	}
}

// weldLocalEndpoints returns the local SEDP endpoints for a bridge.
//
// It uses the backend public accessor, so no reflect or unsafe is
// needed. A missing bridge or missing backend reports false so callers
// fall back to malformed-only incompat counting. It never panics.
func weldLocalEndpoints(bridgeID int64) ([]dds.SEDPEndpoint, bool) {
	weldMu.Lock()
	backend := weldRTPS[bridgeID]
	if _, ok := weldBridges[bridgeID]; !ok {
		weldMu.Unlock()
		return nil, false
	}
	weldMu.Unlock()
	if backend == nil {
		return nil, false
	}
	return backend.LocalEndpoints(), true
}

// weldIdentityMatches reports whether non-QoS identity matches.
//
// ROSTopic, DDSTopic, kind, DDS type, type hash with empty wildcarding,
// and domain must agree. It never panics.
func weldIdentityMatches(local, remote dds.SEDPEndpoint) bool {
	if local.ROSTopic != remote.ROSTopic {
		return false
	}
	if local.DDSTopic != remote.DDSTopic {
		return false
	}
	if local.Kind != remote.Kind {
		return false
	}
	if local.TypeDDS != remote.TypeDDS {
		return false
	}
	if local.TypeHash != "" && remote.TypeHash != "" && local.TypeHash != remote.TypeHash {
		return false
	}
	return local.DomainID == remote.DomainID
}

// weldNonQoSMatches reports whether non-QoS identity matches.
//
// ROSTopic, DDS identity, partitions, and keying must agree; QoS is
// ignored so callers can isolate QoS causes. It never panics.
func weldNonQoSMatches(local, remote dds.SEDPEndpoint) bool {
	if !weldIdentityMatches(local, remote) {
		return false
	}
	if !dds.PartitionsMatch(local.Partitions, remote.Partitions) {
		return false
	}
	ok, err := dds.CheckKeyedMatch(local, remote)
	if err != nil || !ok {
		return false
	}
	return true
}

// weldRxODecides reports whether RxO wire offers refuse matching.
//
// Malformed QoS on either side counts as incompatible fail-closed;
// endpoints predating QoS capture stay unknown and never count.
// Otherwise CheckWireCompatibility decides with durability-service caps
// when both sides carry them. It never panics.
func weldRxODecides(local, remote dds.SEDPEndpoint, offeredIsLocal bool) bool {
	if local.RxOMalformed || remote.RxOMalformed {
		return true
	}
	if local.RxOWire == nil || remote.RxOWire == nil {
		return false
	}
	var offered, requested dds.WireQoS
	if offeredIsLocal {
		offered = *local.RxOWire
		requested = *remote.RxOWire
	} else {
		offered = *remote.RxOWire
		requested = *local.RxOWire
	}
	caps := weldDurabilityCaps(local, remote, offeredIsLocal)
	report := dds.CheckWireCompatibility(offered, requested, caps...)
	return !report.Compatible
}

// weldDurabilityCaps pairs durability-service caps for the compat check.
//
// Both sides must carry caps; the offered side comes first. It never panics.
func weldDurabilityCaps(local, remote dds.SEDPEndpoint, offeredIsLocal bool) []dds.DurabilityServiceQoS {
	if !local.HasDurabilityService || !remote.HasDurabilityService {
		return nil
	}
	if offeredIsLocal {
		return []dds.DurabilityServiceQoS{local.DurabilityService, remote.DurabilityService}
	}
	return []dds.DurabilityServiceQoS{remote.DurabilityService, local.DurabilityService}
}

// weldQoSMismatched reports whether one local plus remote pair is QoS-incompatible.
//
// Identity, partitions, and keying must match; the RxO decision then
// applies. It never panics.
func weldQoSMismatched(local, remote dds.SEDPEndpoint, offeredIsLocal bool) bool {
	if !weldNonQoSMatches(local, remote) {
		return false
	}
	return weldRxODecides(local, remote, offeredIsLocal)
}

// weldRemoteWantsRole reports whether a remote fills one incompat role.
func weldRemoteWantsRole(remote dds.SEDPEndpoint, topic string, offeredIsLocal bool) bool {
	if remote.ROSTopic != topic {
		return false
	}
	if offeredIsLocal && remote.IsReader {
		return false
	}
	if !offeredIsLocal && !remote.IsReader {
		return false
	}
	return true
}

// weldLocalWantsRole reports whether a local fills one incompat role.
func weldLocalWantsRole(local dds.SEDPEndpoint, topic string, offeredIsLocal bool) bool {
	if local.ROSTopic != topic {
		return false
	}
	if offeredIsLocal && local.IsReader {
		return false
	}
	if !offeredIsLocal && !local.IsReader {
		return false
	}
	return true
}

// weldRemoteMismatched reports whether one remote mismatches any local.
func weldRemoteMismatched(remote dds.SEDPEndpoint, locals []dds.SEDPEndpoint, topic string, offeredIsLocal bool) bool {
	for _, local := range locals {
		if !weldLocalWantsRole(local, topic, offeredIsLocal) {
			continue
		}
		if weldQoSMismatched(local, remote, offeredIsLocal) {
			return true
		}
	}
	return false
}

// weldCurrentWireIncompat returns the current wire-incompatible GUIDs.
//
// OfferedIsLocal selects the pub path (local offers) versus the sub path
// (remote offers). Unknown locals fall back to malformed-only counting
// so layout drift still reports real malformed peers. It never panics.
func weldCurrentWireIncompat(bridgeID int64, topic string, offeredIsLocal bool) []dds.SEDPEndpoint {
	weldMu.Lock()
	backend := weldRTPS[bridgeID]
	if _, ok := weldBridges[bridgeID]; !ok {
		weldMu.Unlock()
		return nil
	}
	weldMu.Unlock()
	if backend == nil {
		return nil
	}
	remotes := backend.RemoteEndpoints()
	locals, haveLocals := weldLocalEndpoints(bridgeID)
	var out []dds.SEDPEndpoint
	for _, remote := range remotes {
		if !weldRemoteWantsRole(remote, topic, offeredIsLocal) {
			continue
		}
		if !haveLocals {
			if remote.RxOMalformed {
				out = append(out, remote)
			}
			continue
		}
		if weldRemoteMismatched(remote, locals, topic, offeredIsLocal) {
			out = append(out, remote)
		}
	}
	return out
}

// weldIncompatTotalLocked accumulates the monotonic incompat total.
//
// The caller must hold weldMu. Current GUIDs join the seen set and the
// total is the set size. It never panics.
func weldIncompatTotalLocked(key int64, current []dds.SEDPEndpoint) uint64 {
	seen, ok := weldIncompatSeen[key]
	if !ok {
		seen = make(map[string]struct{})
		weldIncompatSeen[key] = seen
	}
	for _, ep := range current {
		seen[ep.ID.String()] = struct{}{}
	}
	return uint64(len(seen))
}

// weldWriteU64 validates an out pointer and writes a count.
func weldWriteU64(out *C.ulonglong, v uint64) C.int {
	if out == nil {
		return C.int(weldCodeErrNull)
	}
	*out = C.ulonglong(v)
	return C.int(weldCodeOK)
}

// weldCheckEndpointHandle validates a pub or sub handle and returns its topic.
func weldCheckEndpointHandle(key int64, wantPub bool) (int64, string, bool) {
	weldMu.Lock()
	defer weldMu.Unlock()
	if wantPub {
		e, ok := weldPubs[key]
		if !ok {
			return 0, "", false
		}
		return e.bridgeID, e.pub.Topic, true
	}
	e, ok := weldSubs[key]
	if !ok {
		return 0, "", false
	}
	return e.bridgeID, e.sub.Topic, true
}

// weldSetDeadlineLocked records one endpoint deadline under weldMu.
//
// The caller must hold weldMu and have validated the handle. Last
// delivery starts at now with base zero.
func weldSetDeadlineLocked(store map[int64]*weldDeadlineState, key int64, sec, nsec int64) {
	weldDeadlineSetLocked(store, key, weldDeadlineNs(sec, nsec), weldNowNanos())
}

// weldSetDeadlineExport records one endpoint deadline for an export.
//
// Subs selects subscriptions, otherwise publishers. It validates the
// handle and stores under weldMu. It never panics.
func weldSetDeadlineExport(subs bool, handle C.longlong, sec, nsec C.longlong) C.int {
	key := int64(handle)
	if key <= 0 {
		return C.int(weldCodeErrHandle)
	}
	weldMu.Lock()
	defer weldMu.Unlock()
	if subs {
		if _, ok := weldSubs[key]; !ok {
			return C.int(weldCodeErrHandle)
		}
		weldSetDeadlineLocked(weldSubDeadlines, key, int64(sec), int64(nsec))
		return C.int(weldCodeOK)
	}
	if _, ok := weldPubs[key]; !ok {
		return C.int(weldCodeErrHandle)
	}
	weldSetDeadlineLocked(weldPubDeadlines, key, int64(sec), int64(nsec))
	return C.int(weldCodeOK)
}

// weld_ffi_sub_set_deadline records one subscription deadline.
//
// sec plus nsec carry the RMW deadline with zero meaning infinite or
// unknown. Negative inputs clear to unknown. It returns 0 on success
// and -2 for an unknown subscription. It never panics.
//
//export weld_ffi_sub_set_deadline
func weld_ffi_sub_set_deadline(handle C.longlong, sec C.longlong, nsec C.longlong) (rc C.int) {
	defer func() {
		r := recover()
		if r != nil {
			rc = C.int(weldCodeErrInternal)
		}
	}()
	return weldSetDeadlineExport(true, handle, sec, nsec)
}

// weld_ffi_pub_set_deadline records one publisher deadline.
//
// It mirrors the subscription form for offered deadline misses. Codes
// match. It never panics.
//
//export weld_ffi_pub_set_deadline
func weld_ffi_pub_set_deadline(handle C.longlong, sec C.longlong, nsec C.longlong) (rc C.int) {
	defer func() {
		r := recover()
		if r != nil {
			rc = C.int(weldCodeErrInternal)
		}
	}()
	return weldSetDeadlineExport(false, handle, sec, nsec)
}

// weldReadDeadlineTotal returns the cumulative total for a handle.
//
// It validates under weldMu and writes without consuming. Subs selects
// the subscription maps, otherwise the publisher maps. It never panics.
func weldReadDeadlineTotal(subs bool, key int64, out *C.ulonglong) C.int {
	weldMu.Lock()
	defer weldMu.Unlock()
	if subs {
		if _, ok := weldSubs[key]; !ok {
			return C.int(weldCodeErrHandle)
		}
		return weldWriteU64(out, weldDeadlineTotalLocked(weldSubDeadlines, key, weldNowNanos()))
	}
	if _, ok := weldPubs[key]; !ok {
		return C.int(weldCodeErrHandle)
	}
	return weldWriteU64(out, weldDeadlineTotalLocked(weldPubDeadlines, key, weldNowNanos()))
}

// weld_ffi_sub_requested_deadline_total reports cumulative requested misses.
//
// It returns the monotonic total of missed periods with no consuming
// side effect, so wait peeks and take consumes via its own baseline. It
// returns 0 on success and -2 for an unknown subscription. It never panics.
//
//export weld_ffi_sub_requested_deadline_total
func weld_ffi_sub_requested_deadline_total(handle C.longlong, out *C.ulonglong) (rc C.int) {
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
	return weldReadDeadlineTotal(true, key, out)
}

// weld_ffi_pub_offered_deadline_total reports cumulative offered misses.
//
// It mirrors the subscription form. Codes match. It never panics.
//
//export weld_ffi_pub_offered_deadline_total
func weld_ffi_pub_offered_deadline_total(handle C.longlong, out *C.ulonglong) (rc C.int) {
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
	return weldReadDeadlineTotal(false, key, out)
}

// weldReadIncompatTotal accumulates the monotonic total for a handle.
//
// Subs selects subscriptions, otherwise publishers. Current mismatches
// join the ever-seen set under weldMu. It never panics.
func weldReadIncompatTotal(subs bool, key int64, current []dds.SEDPEndpoint, out *C.ulonglong) C.int {
	weldMu.Lock()
	defer weldMu.Unlock()
	if subs {
		if _, ok := weldSubs[key]; !ok {
			return C.int(weldCodeErrHandle)
		}
	} else {
		if _, ok := weldPubs[key]; !ok {
			return C.int(weldCodeErrHandle)
		}
	}
	return weldWriteU64(out, weldIncompatTotalLocked(key, current))
}

// weld_ffi_sub_incompatible_total reports cumulative requested incompatibilities.
//
// Current wire mismatches join the ever-seen set and the total is the
// set size, monotonic across discovery churn. Local in-memory QoS is
// uniform passthrough, so only wire mismatches count here; the C layer
// adds local profile mismatches counted at endpoint creation. It returns
// 0 on success and -2 for an unknown subscription. It never panics.
//
//export weld_ffi_sub_incompatible_total
func weld_ffi_sub_incompatible_total(handle C.longlong, out *C.ulonglong) (rc C.int) {
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
	bridgeID, topic, ok := weldCheckEndpointHandle(key, false)
	if !ok {
		return C.int(weldCodeErrHandle)
	}
	current := weldCurrentWireIncompat(bridgeID, topic, false)
	return weldReadIncompatTotal(true, key, current, out)
}

// weld_ffi_pub_incompatible_total reports cumulative offered incompatibilities.
//
// It mirrors the subscription form with local offers versus remote
// requests. Codes match. It never panics.
//
//export weld_ffi_pub_incompatible_total
func weld_ffi_pub_incompatible_total(handle C.longlong, out *C.ulonglong) (rc C.int) {
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
	bridgeID, topic, ok := weldCheckEndpointHandle(key, true)
	if !ok {
		return C.int(weldCodeErrHandle)
	}
	current := weldCurrentWireIncompat(bridgeID, topic, true)
	return weldReadIncompatTotal(false, key, current, out)
}

// weldQoSOut carries one remote QoS offer for the C info path.
//
// Infinite durations map to zero sec plus zero nsec matching the RMW
// convention where zero means infinite. Unknown or malformed offers
// report false so the C layer skips fail-closed instead of zero-filling.
type weldQoSOut struct {
	reliability int64
	durability  int64
	history     int64
	depth       int64
	deadlineSec int64
	deadlineNs  int64
	liveliness  int64
	leaseSec    int64
	leaseNs     int64
}

// weldQoSFromWire maps one parsed wire offer to RMW fields.
//
// It prefers RxOWire and falls back to WireOffer. Malformed or absent
// offers report false. Infinite durations map to zero. It never panics.
func weldQoSFromWire(ep dds.SEDPEndpoint) (weldQoSOut, bool) {
	wire := ep.RxOWire
	if wire == nil {
		wire = ep.WireOffer
	}
	if wire == nil || ep.RxOMalformed {
		return weldQoSOut{}, false
	}
	out := weldQoSOut{
		reliability: int64(wire.ReliabilityKind),
		durability:  int64(wire.DurabilityKind),
		history:     int64(wire.HistoryKind),
		depth:       int64(wire.Depth),
		liveliness:  int64(wire.LivelinessKind),
	}
	if !wire.Deadline.IsInfinite() {
		out.deadlineSec = int64(wire.Deadline.Seconds)
		out.deadlineNs = int64(wire.Deadline.Nanoseconds)
	}
	if !wire.LeaseDuration.IsInfinite() {
		out.leaseSec = int64(wire.LeaseDuration.Seconds)
		out.leaseNs = int64(wire.LeaseDuration.Nanoseconds)
	}
	if out.deadlineSec < 0 || out.deadlineNs < 0 || out.leaseSec < 0 || out.leaseNs < 0 {
		return weldQoSOut{}, false
	}
	return out, true
}

// weldQoSOutParams bundles the C out pointers for a remote QoS read.
type weldQoSOutParams struct {
	reliability *C.longlong
	durability  *C.longlong
	history     *C.longlong
	depth       *C.longlong
	deadlineSec *C.longlong
	deadlineNs  *C.longlong
	liveliness  *C.longlong
	leaseSec    *C.longlong
	leaseNs     *C.longlong
}

// weldCheckQoSOut validates the QoS out pointers.
func weldCheckQoSOut(p weldQoSOutParams) bool {
	return p.reliability != nil && p.durability != nil && p.history != nil && p.depth != nil && p.deadlineSec != nil && p.deadlineNs != nil && p.liveliness != nil && p.leaseSec != nil && p.leaseNs != nil
}

// weldFillQoSOut copies one mapped offer into C out pointers.
func weldFillQoSOut(p weldQoSOutParams, qos weldQoSOut) {
	*p.reliability = C.longlong(qos.reliability)
	*p.durability = C.longlong(qos.durability)
	*p.history = C.longlong(qos.history)
	*p.depth = C.longlong(qos.depth)
	*p.deadlineSec = C.longlong(qos.deadlineSec)
	*p.deadlineNs = C.longlong(qos.deadlineNs)
	*p.liveliness = C.longlong(qos.liveliness)
	*p.leaseSec = C.longlong(qos.leaseSec)
	*p.leaseNs = C.longlong(qos.leaseNs)
}

// weldRemoteQoSEndpoint selects one filtered remote endpoint.
func weldRemoteQoSEndpoint(bkey int64, name string, role int, index int64) (dds.SEDPEndpoint, bool) {
	eps := weldRemoteEpsFor(bkey, name, role)
	if index < 0 || index >= int64(len(eps)) {
		return dds.SEDPEndpoint{}, false
	}
	return eps[int(index)], true
}

// weld_ffi_remote_ep_qos copies one remote endpoint QoS offer.
//
// topic plus role select the filtered remote list matching
// weld_ffi_remote_ep_at; index selects the entry. Out params carry
// reliability, durability, history, depth, deadline sec/nsec,
// liveliness, and lease sec/nsec with infinite mapped to zero. Unknown
// bridges or indexes report -2, NULL pointers or bad roles report -1,
// and unknown or malformed offers report -2 so the C layer skips
// fail-closed. It never panics.
//
//export weld_ffi_remote_ep_qos
func weld_ffi_remote_ep_qos(bridge C.longlong, topic *C.char, role C.int, index C.longlong, reliability *C.longlong, durability *C.longlong, history *C.longlong, depth *C.longlong, deadlineSec *C.longlong, deadlineNs *C.longlong, liveliness *C.longlong, leaseSec *C.longlong, leaseNs *C.longlong) (rc C.int) {
	defer func() {
		r := recover()
		if r != nil {
			rc = C.int(weldCodeErrInternal)
		}
	}()
	out := weldQoSOutParams{reliability: reliability, durability: durability, history: history, depth: depth, deadlineSec: deadlineSec, deadlineNs: deadlineNs, liveliness: liveliness, leaseSec: leaseSec, leaseNs: leaseNs}
	if topic == nil || !weldCheckQoSOut(out) {
		return C.int(weldCodeErrNull)
	}
	bkey, name, code := weldCheckRemoteArgs(bridge, topic, role)
	if code != C.int(weldCodeOK) {
		return code
	}
	ep, ok := weldRemoteQoSEndpoint(bkey, name, int(role), int64(index))
	if !ok {
		return C.int(weldCodeErrHandle)
	}
	qos, ok := weldQoSFromWire(ep)
	if !ok {
		return C.int(weldCodeErrHandle)
	}
	weldFillQoSOut(out, qos)
	return C.int(weldCodeOK)
}
