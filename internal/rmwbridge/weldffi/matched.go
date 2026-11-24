// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

package main

/*
#include <stdlib.h>
*/
import "C"

import (
	"github.com/podomy/weld/internal/transports/dds"
)

// weldRemoteReaderCount counts remote SEDP readers for one topic.
//
// It scans the attached RTPS backend for the bridge and counts endpoints
// with a matching ROS topic name in reader role. An absent backend means
// in-memory only and contributes zero. It never panics.
func weldRemoteReaderCount(bridgeID int64, topic string) int {
	weldMu.Lock()
	backend := weldRTPS[bridgeID]
	weldMu.Unlock()
	if backend == nil {
		return 0
	}
	n := 0
	for _, ep := range backend.RemoteEndpoints() {
		if ep.ROSTopic == topic && ep.IsReader {
			n++
		}
	}
	return n
}

// weldRemoteWriterCount counts remote SEDP writers for one topic.
//
// It mirrors weldRemoteReaderCount with the writer role. It never panics.
func weldRemoteWriterCount(bridgeID int64, topic string) int {
	weldMu.Lock()
	backend := weldRTPS[bridgeID]
	weldMu.Unlock()
	if backend == nil {
		return 0
	}
	n := 0
	for _, ep := range backend.RemoteEndpoints() {
		if ep.ROSTopic == topic && !ep.IsReader {
			n++
		}
	}
	return n
}

// weldRemoteServiceCount counts remote service endpoints for one service.
//
// It scans the attached RTPS backend for request plus reply endpoints
// carrying the service name. Kind filtering uses the dds topic kinds so
// plain topics sharing the name never count toward service availability.
// An absent backend contributes zero. It never panics.
func weldRemoteServiceCount(bridgeID int64, name string) int {
	weldMu.Lock()
	backend := weldRTPS[bridgeID]
	weldMu.Unlock()
	if backend == nil {
		return 0
	}
	n := 0
	for _, ep := range backend.RemoteEndpoints() {
		if ep.ROSTopic != name {
			continue
		}
		if ep.Kind == dds.TopicKindRequest || ep.Kind == dds.TopicKindReply {
			n++
		}
	}
	return n
}

// weldMatchedPubTotal sums local plus remote subscriptions for a publisher.
//
// It reports false for an unknown handle. It never panics.
func weldMatchedPubTotal(key int64) (int, bool) {
	weldMu.Lock()
	e, ok := weldPubs[key]
	weldMu.Unlock()
	if !ok {
		return 0, false
	}
	n := e.pub.CountMatchedSubscriptions()
	n += weldRemoteReaderCount(e.bridgeID, e.pub.Topic)
	return n, true
}

// weldMatchedSubTotal sums local plus remote publishers for a subscription.
//
// It reports false for an unknown handle. It never panics.
func weldMatchedSubTotal(key int64) (int, bool) {
	weldMu.Lock()
	e, ok := weldSubs[key]
	weldMu.Unlock()
	if !ok {
		return 0, false
	}
	n := e.sub.CountMatchedPublishers()
	n += weldRemoteWriterCount(e.bridgeID, e.sub.Topic)
	return n, true
}

// weldWriteMatchedTotal validates handles and writes a matched count.
//
// Total computes the count for key and reports false for an unknown
// handle. It never panics.
func weldWriteMatchedTotal(handle C.longlong, outN *C.int, total func(int64) (int, bool)) C.int {
	key := int64(handle)
	if key <= 0 {
		return C.int(weldCodeErrHandle)
	}
	if outN == nil {
		return C.int(weldCodeErrNull)
	}
	n, ok := total(key)
	if !ok {
		return C.int(weldCodeErrHandle)
	}
	*outN = C.int(n)
	return C.int(weldCodeOK)
}

// weld_ffi_pub_matched_subs reports live subscriptions for a publisher.
//
// The count sums in-memory same-bridge subscriptions from the bridge
// registry with remote SEDP readers from the attached RTPS backend.
// Own announces never appear in the remote set because loopback
// self-traffic is filtered at ingress, so the two sources do not overlap.
// It returns 0 on success, -1 for a NULL out pointer, -2 for an unknown
// handle. It never panics.
//
//export weld_ffi_pub_matched_subs
func weld_ffi_pub_matched_subs(handle C.longlong, outN *C.int) (rc C.int) {
	defer func() {
		r := recover()
		if r != nil {
			rc = C.int(weldCodeErrInternal)
		}
	}()
	return weldWriteMatchedTotal(handle, outN, weldMatchedPubTotal)
}

// weld_ffi_sub_matched_pubs reports live publishers for a subscription.
//
// It mirrors weld_ffi_pub_matched_subs with the roles reversed: local
// in-memory publishers plus remote SEDP writers. Codes match. It never
// panics.
//
//export weld_ffi_sub_matched_pubs
func weld_ffi_sub_matched_pubs(handle C.longlong, outN *C.int) (rc C.int) {
	defer func() {
		r := recover()
		if r != nil {
			rc = C.int(weldCodeErrInternal)
		}
	}()
	return weldWriteMatchedTotal(handle, outN, weldMatchedSubTotal)
}

// weld_ffi_service_remote_available reports remote service endpoints.
//
// It counts request plus reply endpoints for the service name on the
// attached RTPS backend of the bridge. Local registrations are tracked
// on the C side, so only remotes count here. Codes match the other
// matched queries. It never panics.
//
//export weld_ffi_service_remote_available
func weld_ffi_service_remote_available(bridgeHandle C.longlong, name *C.char, outN *C.int) (rc C.int) {
	defer func() {
		r := recover()
		if r != nil {
			rc = C.int(weldCodeErrInternal)
		}
	}()
	bkey := int64(bridgeHandle)
	if bkey <= 0 {
		return C.int(weldCodeErrHandle)
	}
	if name == nil || outN == nil {
		return C.int(weldCodeErrNull)
	}
	sname, code := weldReadCString(name)
	if code != C.int(weldCodeOK) {
		return code
	}
	if sname == "" {
		return C.int(weldCodeErrInternal)
	}
	weldMu.Lock()
	_, ok := weldBridges[bkey]
	weldMu.Unlock()
	if !ok {
		return C.int(weldCodeErrHandle)
	}
	*outN = C.int(weldRemoteServiceCount(bkey, sname))
	return C.int(weldCodeOK)
}
