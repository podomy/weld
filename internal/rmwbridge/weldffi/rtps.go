// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

// This file attaches the Weld RTPS transport to the Go FFI bridge.
//
// The in-memory bridge stays the primary path so local behavior never
// changes. On weld_ffi_join a dds backend joins discovery beside it,
// publish fans out to the wire, and take merges wire samples with local
// ones. Every wire leg is best effort: any transport failure falls back
// to in-memory only without failing the FFI call. Service calls stay
// in-memory by design and never touch this path.
package main

/*
#include <stdlib.h>
*/
import "C"

import (
	"context"
	"time"

	"github.com/podomy/weld/internal/transports"
	"github.com/podomy/weld/internal/transports/dds"
)

// RTPS attach bounds for the FFI wire legs.
//
// Join and subscribe carry multi-second bounds because discovery dials
// sockets. Publish carries a bound past the one shot SEDP pause. Take
// polls reuse weldPollBound so the non-blocking contract holds.
const (
	// weldRTPSJoinTimeout bounds the backend JoinDiscovery dial.
	weldRTPSJoinTimeout = 5 * time.Second
	// weldRTPSSubscribeTimeout bounds one backend Subscribe declare.
	weldRTPSSubscribeTimeout = 2 * time.Second
	// weldRTPSPublishTimeout bounds one backend PublishBytes send.
	weldRTPSPublishTimeout = 2 * time.Second
	// weldRTPSTakeAttempts caps TakeBytes polls per take poll call.
	weldRTPSTakeAttempts = 4
	// weldRTPSOverflowCap caps stashed wire samples per bridge.
	weldRTPSOverflowCap = 128
)

// weldPubType records per-publisher wire type info.
//
// topic is captured at pub create; typeName and typeHash arrive later
// through weld_ffi_pub_set_type from the C introspection handle.
type weldPubType struct {
	bridgeID int64
	topic    string
	typeName string
	typeHash string
}

// weldSubType carries wire type info for a subscription.
//
// The C layer reports the introspection type name right after sub create,
// mirroring the publisher path. Empty means unknown.
type weldSubType struct {
	bridgeID int64
	topic    string
	typeName string
	typeHash string
}

// weldReaderKey identifies one backend reader declare.
type weldReaderKey struct {
	bridge int64
	topic  string
}

// weldReaderVal records the type a reader was declared with.
type weldReaderVal struct {
	typeName   string
	typeHash   string
	subscribed bool
}

// Wire state for attached RTPS backends. Guarded by weldMu like the rest
// of the handle registry. Backend calls never run under weldMu.
var (
	// weldRTPS maps bridge handle to its live backend. Absence means
	// in-memory only after a failed attach.
	weldRTPS = make(map[int64]*dds.Backend)
	// weldPubTypes maps publisher handle to its wire type info.
	weldPubTypes = make(map[int64]weldPubType)
	// weldSubTypes maps subscription handle to its wire type info.
	weldSubTypes = make(map[int64]weldSubType)
	// weldRTPSReaders records successful backend Subscribe declares.
	weldRTPSReaders = make(map[weldReaderKey]weldReaderVal)
	// weldRTPSOverflow stashes wire samples for other topics on a bridge.
	weldRTPSOverflow = make(map[int64][]transports.IncomingMessage)
)

// weldDiscardBackend closes a backend that will never attach.
//
// Close is idempotent and a teardown error carries no caller action,
// so the error ends here by design to keep attach log-free.
func weldDiscardBackend(backend *dds.Backend) {
	if backend == nil {
		return
	}
	err := backend.Close()
	if err != nil {
		return
	}
}

// weldAttachRTPS joins a dds backend beside an FFI bridge.
//
// It mirrors the MustOpenLiveBackend construction: loopback peer, system
// default range, participant name plus domain from Join. Any failure
// falls back to in-memory only without failing Join: the backend is
// closed best effort and nothing is recorded. A bridge freed mid-join is
// detected under weldMu and its backend is closed instead of stored. It
// never panics.
func weldAttachRTPS(bridgeID int64, participant string, domain uint32) {
	cfg := dds.SidecarConfig{
		DomainID:    domain,
		PeerAddress: dds.DefaultPeerAddress,
		Range:       transports.DiscoveryRangeSystemDefault,
	}
	backend, err := dds.NewBackend(cfg)
	if err != nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), weldRTPSJoinTimeout)
	defer cancel()
	info := transports.DiscoveryInfo{
		ParticipantName: participant,
		DomainID:        domain,
		Range:           transports.DiscoveryRangeSystemDefault,
	}
	err = backend.JoinDiscovery(ctx, info)
	if err != nil {
		weldDiscardBackend(backend)
		return
	}
	weldMu.Lock()
	_, ok := weldBridges[bridgeID]
	if !ok {
		weldMu.Unlock()
		weldDiscardBackend(backend)
		return
	}
	weldRTPS[bridgeID] = backend
	weldMu.Unlock()
}

// weldCloseRTPS detaches the backend for a bridge being freed.
//
// It drops the backend handle, per-publisher type info, reader records,
// and stashed samples for the bridge, then closes the backend best
// effort. Backend teardown errors end here because in-memory teardown
// already decided the result. It never panics.
func weldCloseRTPS(bridgeID int64) {
	weldMu.Lock()
	backend := weldRTPS[bridgeID]
	delete(weldRTPS, bridgeID)
	for id, pt := range weldPubTypes {
		if pt.bridgeID == bridgeID {
			delete(weldPubTypes, id)
		}
	}
	for id, st := range weldSubTypes {
		if st.bridgeID == bridgeID {
			delete(weldSubTypes, id)
		}
	}
	for key := range weldRTPSReaders {
		if key.bridge == bridgeID {
			delete(weldRTPSReaders, key)
		}
	}
	delete(weldRTPSOverflow, bridgeID)
	weldMu.Unlock()
	if backend == nil {
		return
	}
	weldDiscardBackend(backend)
}

// weldKnownWireTypeLocked returns the best-known wire type for a topic.
//
// Subscription reports win with same-bridge publisher types as fallback.
// An empty name means unknown. The caller must hold weldMu.
func weldKnownWireTypeLocked(bridgeID int64, topic string) (string, string) {
	for _, st := range weldSubTypes {
		if st.bridgeID == bridgeID && st.topic == topic && st.typeName != "" {
			return st.typeName, st.typeHash
		}
	}
	for _, pt := range weldPubTypes {
		if pt.bridgeID == bridgeID && pt.topic == topic && pt.typeName != "" {
			return pt.typeName, pt.typeHash
		}
	}
	return "", ""
}

// weldReadTypePair reads IDL type name plus hash from C strings.
//
// Either input may be empty to clear. It never panics.
func weldReadTypePair(typeName, typeHash *C.char) (string, string, C.int) {
	if typeName == nil || typeHash == nil {
		return "", "", C.int(weldCodeErrNull)
	}
	tn, code := weldReadCString(typeName)
	if code != C.int(weldCodeOK) {
		return "", "", code
	}
	th, code := weldReadCString(typeHash)
	if code != C.int(weldCodeOK) {
		return "", "", code
	}
	return tn, th, C.int(weldCodeOK)
}

// weldSetPubType records publisher wire type and returns bridge plus topic.
//
// It reports false for an unknown publisher handle. It never panics.
func weldSetPubType(key int64, tn, th string) (int64, string, bool) {
	weldMu.Lock()
	defer weldMu.Unlock()
	pt, ok := weldPubTypes[key]
	if !ok {
		return 0, "", false
	}
	pt.typeName = tn
	pt.typeHash = th
	weldPubTypes[key] = pt
	return pt.bridgeID, pt.topic, true
}

// weldSetSubType records subscription wire type and returns bridge plus topic.
//
// It reports false for an unknown subscription handle. It never panics.
func weldSetSubType(key int64, tn, th string) (int64, string, bool) {
	weldMu.Lock()
	defer weldMu.Unlock()
	st, ok := weldSubTypes[key]
	if !ok {
		return 0, "", false
	}
	st.typeName = tn
	st.typeHash = th
	weldSubTypes[key] = st
	return st.bridgeID, st.topic, true
}

// weldWriteWireType reads a type pair, stores it, and refreshes the reader.
//
// Save records the type for the handle and returns bridge plus topic; it
// reports false for an unknown handle. When refresh is true it declares
// the backend reader for subscriptions. Publishers must not declare
// readers: a pub declaring a reader makes `ros2 topic info` see a
// spurious subscriber and hides the missing writer. It never panics.
func weldWriteWireType(handle C.longlong, typeName, typeHash *C.char, save func(int64, string, string) (int64, string, bool), refresh bool) C.int {
	key := int64(handle)
	if key <= 0 {
		return C.int(weldCodeErrHandle)
	}
	tn, th, code := weldReadTypePair(typeName, typeHash)
	if code != C.int(weldCodeOK) {
		return code
	}
	bridgeID, topic, ok := save(key, tn, th)
	if !ok {
		return C.int(weldCodeErrHandle)
	}
	if refresh {
		weldRefreshRTPSReader(bridgeID, topic)
	}
	return C.int(weldCodeOK)
}

// weldRefreshRTPSReader declares the backend reader for one topic.
//
// The type comes from same-handle subscription info reported through
// weld_ffi_sub_set_type, falling back to the best-known same-bridge
// publisher type. Declares run once per bridge plus topic plus type and
// repeat only after the known type changes. Unknown types still attempt
// a declare; the backend rejects them and the take path keeps working
// in-memory. It never fails the caller and never panics.
func weldRefreshRTPSReader(bridgeID int64, topic string) {
	weldMu.Lock()
	backend := weldRTPS[bridgeID]
	if backend == nil {
		weldMu.Unlock()
		return
	}
	typeName, typeHash := weldKnownWireTypeLocked(bridgeID, topic)
	key := weldReaderKey{bridge: bridgeID, topic: topic}
	rec, ok := weldRTPSReaders[key]
	if ok && rec.subscribed && rec.typeName == typeName && rec.typeHash == typeHash {
		weldMu.Unlock()
		return
	}
	weldMu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), weldRTPSSubscribeTimeout)
	err := backend.Subscribe(ctx, topic, typeName, typeHash)
	cancel()
	if err != nil {
		return
	}
	weldMu.Lock()
	weldRTPSReaders[key] = weldReaderVal{typeName: typeName, typeHash: typeHash, subscribed: true}
	weldMu.Unlock()
}

// weldPublishRTPS mirrors one FFI publish to the wire best effort.
//
// The in-memory publish already succeeded; this leg only adds ROS peer
// visibility. It always attempts PublishBytes so the SEDP writer is
// announced even with no remote reader yet: discovery needs the writer
// announce before any reader appears, and PublishBytes announces the
// writer synchronously before checking for matching readers. DATA to no
// readers fails with ErrNoRoute and is ignored here. It never panics.
func weldPublishRTPS(pubHandle int64, payload []byte) {
	weldMu.Lock()
	entry, ok := weldPubs[pubHandle]
	if !ok {
		weldMu.Unlock()
		return
	}
	backend := weldRTPS[entry.bridgeID]
	pt := weldPubTypes[pubHandle]
	topic := entry.pub.Topic
	weldMu.Unlock()
	if backend == nil {
		return
	}
	msg := transports.OutgoingMessage{
		TopicName: topic,
		TypeName:  pt.typeName,
		Payload:   payload,
		TypeHash:  pt.typeHash,
	}
	ctx, cancel := context.WithTimeout(context.Background(), weldRTPSPublishTimeout)
	defer cancel()
	err := backend.PublishBytes(ctx, msg)
	if err != nil {
		return
	}
}

// weldReplayRTPSOverflow pops the oldest stashed sample for a topic.
//
// Samples for other topics stay parked. A stored sample with an empty
// topic matches any subscription, mirroring the live take filters. It
// never panics.
func weldReplayRTPSOverflow(bridgeID int64, topic string) ([]byte, bool) {
	weldMu.Lock()
	queue := weldRTPSOverflow[bridgeID]
	hit := -1
	for i, msg := range queue {
		if msg.TopicName == "" || msg.TopicName == topic {
			hit = i
			break
		}
	}
	if hit < 0 {
		weldMu.Unlock()
		return nil, false
	}
	payload := queue[hit].Payload
	queue = append(queue[:hit], queue[hit+1:]...)
	if len(queue) == 0 {
		delete(weldRTPSOverflow, bridgeID)
	} else {
		weldRTPSOverflow[bridgeID] = queue
	}
	weldMu.Unlock()
	return payload, true
}

// weldStashRTPSOverflow parks a wire sample for another topic.
//
// The stash is bounded; the oldest sample drops when full so a chatty
// foreign topic cannot grow memory without bound. It never panics.
func weldStashRTPSOverflow(bridgeID int64, msg transports.IncomingMessage) {
	weldMu.Lock()
	queue := weldRTPSOverflow[bridgeID]
	if len(queue) >= weldRTPSOverflowCap {
		trimmed := append([]transports.IncomingMessage(nil), queue[len(queue)-weldRTPSOverflowCap+1:]...)
		queue = trimmed
	}
	queue = append(queue, msg)
	weldRTPSOverflow[bridgeID] = queue
	weldMu.Unlock()
}

// weldTakeRTPS polls the wire for one sample on a topic.
//
// It first replays samples stashed for this topic while serving other
// subscriptions, then polls the backend with short timeouts. Samples for
// other topics move to the per-bridge overflow instead of being dropped.
// Timeouts and transport errors map to not ready so the caller keeps the
// non-blocking contract. It never panics.
func weldTakeRTPS(bridgeID int64, topic string) ([]byte, bool) {
	weldMu.Lock()
	backend := weldRTPS[bridgeID]
	weldMu.Unlock()
	if backend == nil {
		return nil, false
	}
	weldRefreshRTPSReader(bridgeID, topic)
	payload, ok := weldReplayRTPSOverflow(bridgeID, topic)
	if ok {
		return payload, true
	}
	for range weldRTPSTakeAttempts {
		ctx, cancel := context.WithTimeout(context.Background(), weldPollBound)
		msg, err := backend.TakeBytes(ctx)
		cancel()
		if err != nil {
			return nil, false
		}
		if msg.TopicName != "" && msg.TopicName != topic {
			weldStashRTPSOverflow(bridgeID, msg)
			continue
		}
		return msg.Payload, true
	}
	return nil, false
}

// weld_ffi_pub_set_type stores wire type info for a publisher.
//
// typeName is the ROS IDL name such as std_msgs__msg__String and typeHash
// is the RIHS01 hash; both are NUL terminated C strings and either may be
// empty to clear. The C layer calls this after weld_ffi_pub_create from
// its introspection handle because the create signature stays frozen.
// Stored info feeds PublishBytes addressing. It does not declare a
// backend reader: publishers are writers only. It returns 0 on success,
// -1 for NULL pointers, and -2 for an unknown publisher handle. It never
// panics.
//
//export weld_ffi_pub_set_type
func weld_ffi_pub_set_type(handle C.longlong, typeName *C.char, typeHash *C.char) (rc C.int) {
	defer func() {
		r := recover()
		if r != nil {
			rc = C.int(weldCodeErrInternal)
		}
	}()
	return weldWriteWireType(handle, typeName, typeHash, weldSetPubType, false)
}

// weld_ffi_sub_set_type stores wire type info for a subscription.
//
// typeName is the ROS IDL type name such as std_msgs__msg__String and
// typeHash is the RIHS01 hash string, or empty for hashless wildcard
// interop. The C layer calls this right after sub create from its
// introspection handle because the create signature carries no type.
// Stored info drives same-bridge reader declares and match reporting.
// It returns 0 on success and -1 for NULL pointers, -2 for an unknown
// subscription handle. It never panics.
//
//export weld_ffi_sub_set_type
func weld_ffi_sub_set_type(handle C.longlong, typeName *C.char, typeHash *C.char) (rc C.int) {
	defer func() {
		r := recover()
		if r != nil {
			rc = C.int(weldCodeErrInternal)
		}
	}()
	return weldWriteWireType(handle, typeName, typeHash, weldSetSubType, true)
}
