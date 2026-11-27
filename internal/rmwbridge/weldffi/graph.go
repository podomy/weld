// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

// This file exposes RMW graph queries backed by the live RTPS backend.
//
// Snapshots merge the in-memory bridge registries (nodes, topics with
// publisher type reports, services) with the dds backend remote endpoint
// view. An absent backend contributes nothing and never fails the query:
// every export returns OK with the local snapshot, and unknown bridges
// report the handle code so the C layer falls back to its own view.
package main

/*
#include <stdlib.h>
*/
import "C"

import (
	"sort"
	"strings"

	"github.com/podomy/weld/internal/rmwbridge"
	"github.com/podomy/weld/internal/transports/dds"
)

// weldGraphNode is one known node in the graph snapshot.
type weldGraphNode struct {
	name      string
	namespace string
	domain    uint32
}

// weldGraphPair is one named graph entry with a possibly empty type.
type weldGraphPair struct {
	name string
	typ  string
}

// weldGraphBridge returns the bridge for a handle, or nil when unknown.
//
// The bridge stays thread safe after the lookup, so callers use it
// without holding weldMu. It never panics.
func weldGraphBridge(bridgeID int64) *rmwbridge.Bridge {
	weldMu.Lock()
	defer weldMu.Unlock()
	return weldBridges[bridgeID]
}

// weldGraphBackend returns the live backend for a bridge, or nil.
//
// A nil backend means in-memory only after a failed attach, so callers
// treat it as an empty remote view without failing. It never panics.
func weldGraphBackend(bridgeID int64) *dds.Backend {
	weldMu.Lock()
	defer weldMu.Unlock()
	return weldRTPS[bridgeID]
}

// weldGraphNodes returns the sorted local nodes for a bridge.
//
// Nodes come from the bridge registry only: remote participants are not
// ROS nodes, so the backend contributes nothing here. It reports false
// for an unknown bridge. It never panics.
func weldGraphNodes(bridgeID int64) ([]weldGraphNode, bool) {
	b := weldGraphBridge(bridgeID)
	if b == nil {
		return nil, false
	}
	infos := b.NodeNames()
	nodes := make([]weldGraphNode, 0, len(infos))
	for _, info := range infos {
		nodes = append(nodes, weldGraphNode{name: info.Name, namespace: info.Namespace, domain: info.DomainID})
	}
	sort.Slice(nodes, func(i, j int) bool {
		if nodes[i].name != nodes[j].name {
			return nodes[i].name < nodes[j].name
		}
		if nodes[i].namespace != nodes[j].namespace {
			return nodes[i].namespace < nodes[j].namespace
		}
		return nodes[i].domain < nodes[j].domain
	})
	return nodes, true
}

// weldGraphType prefers the IDL type and demangles the DDS type on fallback.
//
// The graph reports ROS slash names (std_msgs/msg/String), so IDL
// (std_msgs__msg__String) converts with __ to /. Local topics with no
// known type stay empty so the name still lists. It never panics.
func weldGraphType(idl, ddsName string) string {
	if idl != "" {
		return weldIDLToSlash(idl)
	}
	if ddsName == "" {
		return ""
	}
	demangled, err := dds.DemangleTypeName(ddsName)
	if err != nil {
		return ""
	}
	return weldIDLToSlash(demangled)
}

// weldIDLToSlash converts IDL (std_msgs__msg__String) to ROS slash
// form (std_msgs/msg/String). Unknown shapes pass through unchanged.
func weldIDLToSlash(idl string) string {
	if idl == "" {
		return ""
	}
	if !strings.Contains(idl, "__") {
		return idl
	}
	return strings.ReplaceAll(idl, "__", "/")
}

// weldGraphServiceBase strips the request or response suffix to the base type.
//
// Service endpoints announce the directional IDL name such as
// example_interfaces__srv__AddTwoInts_Request while the graph reports
// the base service type in slash form. Unknown shapes pass through
// unchanged.
func weldGraphServiceBase(t string) string {
	if base, ok := strings.CutSuffix(t, "_Request"); ok {
		return weldIDLToSlash(base)
	}
	if base, ok := strings.CutSuffix(t, "_Response"); ok {
		return weldIDLToSlash(base)
	}
	return weldIDLToSlash(t)
}

// weldGraphFlattenPairs sorts a name to types map into flat pairs.
//
// Names sort ascending with types sorted ascending within a name. A name
// with no types emits one pair with an empty type so listings keep the
// name. It never panics.
func weldGraphFlattenPairs(byName map[string]map[string]struct{}) []weldGraphPair {
	names := make([]string, 0, len(byName))
	for name := range byName {
		names = append(names, name)
	}
	sort.Strings(names)
	var out []weldGraphPair
	for _, name := range names {
		types := make([]string, 0, len(byName[name]))
		for t := range byName[name] {
			types = append(types, t)
		}
		sort.Strings(types)
		if len(types) == 0 {
			out = append(out, weldGraphPair{name: name})
			continue
		}
		for _, t := range types {
			out = append(out, weldGraphPair{name: name, typ: t})
		}
	}
	return out
}

// weldGraphAddTypes merges types into the set for name, creating it on demand.
//
// Empty types are skipped so a name with no known types keeps an empty
// set and still lists. It never panics.
func weldGraphAddTypes(byName map[string]map[string]struct{}, name string, types []string) {
	set, exists := byName[name]
	if !exists {
		set = make(map[string]struct{})
		byName[name] = set
	}
	for _, t := range types {
		if t != "" {
			set[t] = struct{}{}
		}
	}
}

// weldGraphAddType merges one type into the set for name, creating it on demand.
//
// An empty type leaves an empty set so the name still lists. It never panics.
func weldGraphAddType(byName map[string]map[string]struct{}, name, typ string) {
	set, exists := byName[name]
	if !exists {
		set = make(map[string]struct{})
		byName[name] = set
	}
	if typ != "" {
		set[typ] = struct{}{}
	}
}

// weldGraphMergeRemoteTopics folds backend topic endpoints into byTopic.
//
// Only topic kind endpoints with a ROS name contribute, with IDL type
// preferred and DDS demangle as fallback. It never panics.
func weldGraphMergeRemoteTopics(byTopic map[string]map[string]struct{}, backend *dds.Backend) {
	for _, ep := range backend.RemoteEndpoints() {
		if ep.Kind != dds.TopicKindTopic || ep.ROSTopic == "" {
			continue
		}
		weldGraphAddType(byTopic, ep.ROSTopic, weldGraphType(ep.TypeIDL, ep.TypeDDS))
	}
}

// weldGraphMergeRemoteServices folds backend service endpoints into byService.
//
// Only request plus reply endpoints with a ROS name contribute, normalized
// to the base service type. It never panics.
func weldGraphMergeRemoteServices(byService map[string]map[string]struct{}, backend *dds.Backend) {
	for _, ep := range backend.RemoteEndpoints() {
		if ep.ROSTopic == "" {
			continue
		}
		if ep.Kind != dds.TopicKindRequest && ep.Kind != dds.TopicKindReply {
			continue
		}
		weldGraphAddType(byService, ep.ROSTopic, weldGraphServiceBase(weldGraphType(ep.TypeIDL, ep.TypeDDS)))
	}
}

// weldGraphWritePairAt validates index and writes one pair into C buffers.
//
// It returns ErrNull for NULL buffers, ErrHandle for an out of range
// index, and ErrShort when a buffer is too small. It never panics.
func weldGraphWritePairAt(pairs []weldGraphPair, index C.longlong, nameBuf *C.char, nameLen C.int, typeBuf *C.char, typeLen C.int) C.int {
	if nameBuf == nil || typeBuf == nil {
		return C.int(weldCodeErrNull)
	}
	if index < 0 {
		return C.int(weldCodeErrHandle)
	}
	if int64(index) >= int64(len(pairs)) {
		return C.int(weldCodeErrHandle)
	}
	entry := pairs[int(index)]
	if code := weldWriteCString(nameBuf, nameLen, entry.name); code != C.int(weldCodeOK) {
		return code
	}
	if code := weldWriteCString(typeBuf, typeLen, entry.typ); code != C.int(weldCodeOK) {
		return code
	}
	return C.int(weldCodeOK)
}

// weldGraphWriteCount reads one C string and writes one graph count.
//
// Count maps the name to a local plus remote total and reports false for
// an unknown bridge. It never panics.
func weldGraphWriteCount(bridge C.longlong, label *C.char, out *C.longlong, count func(int64, string) (int, bool)) C.int {
	if label == nil || out == nil {
		return C.int(weldCodeErrNull)
	}
	name, code := weldReadCString(label)
	if code != C.int(weldCodeOK) {
		return code
	}
	n, ok := count(int64(bridge), name)
	if !ok {
		return C.int(weldCodeErrHandle)
	}
	*out = C.longlong(n)
	return C.int(weldCodeOK)
}

// weldGraphTypeOverlay returns per-topic slash types from the bridge
// publisher plus subscription type reports for one bridge. Topics or
// type names still unknown contribute nothing. It never panics.
func weldGraphTypeOverlay(bridgeID int64) map[string][]string {
	overlay := make(map[string][]string)
	weldMu.Lock()
	defer weldMu.Unlock()
	for _, pt := range weldPubTypes {
		if pt.bridgeID != bridgeID || pt.topic == "" || pt.typeName == "" {
			continue
		}
		overlay[pt.topic] = append(overlay[pt.topic], weldIDLToSlash(pt.typeName))
	}
	for _, st := range weldSubTypes {
		if st.bridgeID != bridgeID || st.topic == "" || st.typeName == "" {
			continue
		}
		overlay[st.topic] = append(overlay[st.topic], weldIDLToSlash(st.typeName))
	}
	return overlay
}

// weldGraphTopics returns sorted (topic, type) pairs for a bridge.
//
// Local topics come from the bridge registries plus publisher type
// reports; remote topics come from the backend view with topic kind.
// Topics with no known type contribute one pair with an empty type so
// the C layer still lists the name. It reports false for an unknown
// bridge. It never panics.
func weldGraphTopics(bridgeID int64) ([]weldGraphPair, bool) {
	b := weldGraphBridge(bridgeID)
	if b == nil {
		return nil, false
	}
	local := b.TopicNamesAndTypes()
	overlay := weldGraphTypeOverlay(bridgeID)
	backend := weldGraphBackend(bridgeID)
	byTopic := make(map[string]map[string]struct{})
	for _, info := range local {
		slashed := make([]string, 0, len(info.Types))
		for _, t := range info.Types {
			slashed = append(slashed, weldIDLToSlash(t))
		}
		weldGraphAddTypes(byTopic, info.Name, slashed)
	}
	for topic, types := range overlay {
		weldGraphAddTypes(byTopic, topic, types)
	}
	if backend != nil {
		weldGraphMergeRemoteTopics(byTopic, backend)
	}
	return weldGraphFlattenPairs(byTopic), true
}

// weldGraphServices returns sorted (service, type) pairs for a bridge.
//
// Local services come from the bridge registry; remote services come
// from the backend view with request or reply kind, normalized to the
// base service type in slash form. Services with no known type
// contribute one pair with an empty type. It reports false for an
// unknown bridge. It never panics.
func weldGraphServices(bridgeID int64) ([]weldGraphPair, bool) {
	b := weldGraphBridge(bridgeID)
	if b == nil {
		return nil, false
	}
	local := b.ServiceNamesAndTypes()
	backend := weldGraphBackend(bridgeID)
	byService := make(map[string]map[string]struct{})
	for _, info := range local {
		if info.Name == "" {
			continue
		}
		slashed := make([]string, 0, len(info.Types))
		for _, t := range info.Types {
			slashed = append(slashed, weldIDLToSlash(weldGraphServiceBase(t)))
		}
		weldGraphAddTypes(byService, info.Name, slashed)
	}
	if backend == nil {
		return weldGraphFlattenPairs(byService), true
	}
	weldGraphMergeRemoteServices(byService, backend)
	return weldGraphFlattenPairs(byService), true
}

// weldGraphCountPublishers returns local plus remote writers on a topic.
//
// Remote writers are backend endpoints with topic kind matching the name
// that are not readers. It reports false for an unknown bridge. It never
// panics.
func weldGraphCountPublishers(bridgeID int64, topic string) (int, bool) {
	b := weldGraphBridge(bridgeID)
	if b == nil {
		return 0, false
	}
	n := b.CountPublishers(topic)
	backend := weldGraphBackend(bridgeID)
	if backend != nil {
		for _, ep := range backend.RemoteEndpoints() {
			if ep.Kind == dds.TopicKindTopic && ep.ROSTopic == topic && !ep.IsReader {
				n++
			}
		}
	}
	return n, true
}

// weldGraphCountSubscribers returns local plus remote readers on a topic.
//
// Remote readers are backend endpoints with topic kind matching the name
// that are readers. It reports false for an unknown bridge. It never
// panics.
func weldGraphCountSubscribers(bridgeID int64, topic string) (int, bool) {
	b := weldGraphBridge(bridgeID)
	if b == nil {
		return 0, false
	}
	n := b.CountSubscribers(topic)
	backend := weldGraphBackend(bridgeID)
	if backend != nil {
		for _, ep := range backend.RemoteEndpoints() {
			if ep.Kind == dds.TopicKindTopic && ep.ROSTopic == topic && ep.IsReader {
				n++
			}
		}
	}
	return n, true
}

// weldGraphCountServers returns local servers plus distinct remote servers.
//
// A remote server shows as a request reader or a reply writer; distinct
// participant prefixes count so two servers on one service report two.
// It reports false for an unknown bridge. It never panics.
func weldGraphCountServers(bridgeID int64, service string) (int, bool) {
	b := weldGraphBridge(bridgeID)
	if b == nil {
		return 0, false
	}
	n := b.CountServices(service)
	backend := weldGraphBackend(bridgeID)
	if backend != nil {
		seen := make(map[dds.SEDPGUIDPrefix]struct{})
		for _, ep := range backend.RemoteEndpoints() {
			if ep.ROSTopic != service {
				continue
			}
			server := (ep.Kind == dds.TopicKindRequest && ep.IsReader) ||
				(ep.Kind == dds.TopicKindReply && !ep.IsReader)
			if !server {
				continue
			}
			seen[ep.ID.Prefix] = struct{}{}
		}
		n += len(seen)
	}
	return n, true
}

// weldGraphCountClients returns local clients plus distinct remote clients.
//
// A remote client shows as a request writer or a reply reader; distinct
// participant prefixes count so two clients on one service report two.
// It reports false for an unknown bridge. It never panics.
func weldGraphCountClients(bridgeID int64, service string) (int, bool) {
	b := weldGraphBridge(bridgeID)
	if b == nil {
		return 0, false
	}
	n := b.CountClients(service)
	backend := weldGraphBackend(bridgeID)
	if backend != nil {
		seen := make(map[dds.SEDPGUIDPrefix]struct{})
		for _, ep := range backend.RemoteEndpoints() {
			if ep.ROSTopic != service {
				continue
			}
			client := (ep.Kind == dds.TopicKindRequest && !ep.IsReader) ||
				(ep.Kind == dds.TopicKindReply && ep.IsReader)
			if !client {
				continue
			}
			seen[ep.ID.Prefix] = struct{}{}
		}
		n += len(seen)
	}
	return n, true
}

// weld_ffi_graph_node_count writes the number of known nodes for a bridge.
//
// It returns 0 on success, -1 for a NULL out pointer, and -2 for an
// unknown bridge handle. It never panics.
//
//export weld_ffi_graph_node_count
func weld_ffi_graph_node_count(bridge C.longlong, out *C.longlong) (rc C.int) {
	defer func() {
		r := recover()
		if r != nil {
			rc = C.int(weldCodeErrInternal)
		}
	}()
	if out == nil {
		return C.int(weldCodeErrNull)
	}
	nodes, ok := weldGraphNodes(int64(bridge))
	if !ok {
		return C.int(weldCodeErrHandle)
	}
	*out = C.longlong(len(nodes))
	return C.int(weldCodeOK)
}

// weld_ffi_graph_node_at copies one known node by index for a bridge.
//
// nameBuf and nsBuf receive NUL terminated strings and domainOut
// receives the domain. It returns 0 on success, -1 for NULL pointers,
// -2 for an unknown bridge or out of range index, and -4 when a buffer
// is too small. It never panics.
//
//export weld_ffi_graph_node_at
func weld_ffi_graph_node_at(bridge C.longlong, index C.longlong, nameBuf *C.char, nameLen C.int, nsBuf *C.char, nsLen C.int, domainOut *C.longlong) (rc C.int) {
	defer func() {
		r := recover()
		if r != nil {
			rc = C.int(weldCodeErrInternal)
		}
	}()
	if nameBuf == nil || nsBuf == nil || domainOut == nil {
		return C.int(weldCodeErrNull)
	}
	if index < 0 {
		return C.int(weldCodeErrHandle)
	}
	nodes, ok := weldGraphNodes(int64(bridge))
	if !ok {
		return C.int(weldCodeErrHandle)
	}
	if int64(index) >= int64(len(nodes)) {
		return C.int(weldCodeErrHandle)
	}
	node := nodes[int(index)]
	if code := weldWriteCString(nameBuf, nameLen, node.name); code != C.int(weldCodeOK) {
		return code
	}
	if code := weldWriteCString(nsBuf, nsLen, node.namespace); code != C.int(weldCodeOK) {
		return code
	}
	*domainOut = C.longlong(node.domain)
	return C.int(weldCodeOK)
}

// weld_ffi_graph_topic_entry_count writes the flat (topic, type) count.
//
// Topics with no known type contribute one entry with an empty type so
// listings keep the name. It returns 0 on success, -1 for a NULL out
// pointer, and -2 for an unknown bridge handle. It never panics.
//
//export weld_ffi_graph_topic_entry_count
func weld_ffi_graph_topic_entry_count(bridge C.longlong, out *C.longlong) (rc C.int) {
	defer func() {
		r := recover()
		if r != nil {
			rc = C.int(weldCodeErrInternal)
		}
	}()
	if out == nil {
		return C.int(weldCodeErrNull)
	}
	pairs, ok := weldGraphTopics(int64(bridge))
	if !ok {
		return C.int(weldCodeErrHandle)
	}
	*out = C.longlong(len(pairs))
	return C.int(weldCodeOK)
}

// weld_ffi_graph_topic_entry_at copies one flat (topic, type) entry.
//
// Buffers receive NUL terminated strings; the type is empty for topics
// with no known type. It returns 0 on success, -1 for NULL pointers, -2
// for an unknown bridge or out of range index, and -4 when a buffer is
// too small. It never panics.
//
//export weld_ffi_graph_topic_entry_at
func weld_ffi_graph_topic_entry_at(bridge C.longlong, index C.longlong, topicBuf *C.char, topicLen C.int, typeBuf *C.char, typeLen C.int) (rc C.int) {
	defer func() {
		r := recover()
		if r != nil {
			rc = C.int(weldCodeErrInternal)
		}
	}()
	pairs, ok := weldGraphTopics(int64(bridge))
	if !ok {
		return C.int(weldCodeErrHandle)
	}
	return weldGraphWritePairAt(pairs, index, topicBuf, topicLen, typeBuf, typeLen)
}

// weld_ffi_graph_service_entry_count writes the flat (service, type) count.
//
// Services with no known type contribute one entry with an empty type so
// listings keep the name. It returns 0 on success, -1 for a NULL out
// pointer, and -2 for an unknown bridge handle. It never panics.
//
//export weld_ffi_graph_service_entry_count
func weld_ffi_graph_service_entry_count(bridge C.longlong, out *C.longlong) (rc C.int) {
	defer func() {
		r := recover()
		if r != nil {
			rc = C.int(weldCodeErrInternal)
		}
	}()
	if out == nil {
		return C.int(weldCodeErrNull)
	}
	pairs, ok := weldGraphServices(int64(bridge))
	if !ok {
		return C.int(weldCodeErrHandle)
	}
	*out = C.longlong(len(pairs))
	return C.int(weldCodeOK)
}

// weld_ffi_graph_service_entry_at copies one flat (service, type) entry.
//
// Buffers receive NUL terminated strings; the type is empty for services
// with no known type. It returns 0 on success, -1 for NULL pointers, -2
// for an unknown bridge or out of range index, and -4 when a buffer is
// too small. It never panics.
//
//export weld_ffi_graph_service_entry_at
func weld_ffi_graph_service_entry_at(bridge C.longlong, index C.longlong, serviceBuf *C.char, serviceLen C.int, typeBuf *C.char, typeLen C.int) (rc C.int) {
	defer func() {
		r := recover()
		if r != nil {
			rc = C.int(weldCodeErrInternal)
		}
	}()
	pairs, ok := weldGraphServices(int64(bridge))
	if !ok {
		return C.int(weldCodeErrHandle)
	}
	return weldGraphWritePairAt(pairs, index, serviceBuf, serviceLen, typeBuf, typeLen)
}

// weld_ffi_graph_count_publishers writes local plus remote writers on a topic.
//
// It returns 0 on success, -1 for NULL pointers, and -2 for an unknown
// bridge handle. It never panics.
//
//export weld_ffi_graph_count_publishers
func weld_ffi_graph_count_publishers(bridge C.longlong, topic *C.char, out *C.longlong) (rc C.int) {
	defer func() {
		r := recover()
		if r != nil {
			rc = C.int(weldCodeErrInternal)
		}
	}()
	return weldGraphWriteCount(bridge, topic, out, weldGraphCountPublishers)
}

// weld_ffi_graph_count_subscribers writes local plus remote readers on a topic.
//
// It returns 0 on success, -1 for NULL pointers, and -2 for an unknown
// bridge handle. It never panics.
//
//export weld_ffi_graph_count_subscribers
func weld_ffi_graph_count_subscribers(bridge C.longlong, topic *C.char, out *C.longlong) (rc C.int) {
	defer func() {
		r := recover()
		if r != nil {
			rc = C.int(weldCodeErrInternal)
		}
	}()
	return weldGraphWriteCount(bridge, topic, out, weldGraphCountSubscribers)
}

// weld_ffi_graph_count_services writes local plus remote servers for a service.
//
// It returns 0 on success, -1 for NULL pointers, and -2 for an unknown
// bridge handle. It never panics.
//
//export weld_ffi_graph_count_services
func weld_ffi_graph_count_services(bridge C.longlong, service *C.char, out *C.longlong) (rc C.int) {
	defer func() {
		r := recover()
		if r != nil {
			rc = C.int(weldCodeErrInternal)
		}
	}()
	return weldGraphWriteCount(bridge, service, out, weldGraphCountServers)
}

// weld_ffi_graph_count_clients writes local plus remote clients for a service.
//
// It returns 0 on success, -1 for NULL pointers, and -2 for an unknown
// bridge handle. It never panics.
//
//export weld_ffi_graph_count_clients
func weld_ffi_graph_count_clients(bridge C.longlong, service *C.char, out *C.longlong) (rc C.int) {
	defer func() {
		r := recover()
		if r != nil {
			rc = C.int(weldCodeErrInternal)
		}
	}()
	return weldGraphWriteCount(bridge, service, out, weldGraphCountClients)
}
