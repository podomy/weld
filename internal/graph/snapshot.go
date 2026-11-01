// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

// Package graph holds the read snapshot view for tools.
//
// Boundary: the write path truth lives in internal/core/graph.go. Discovery
// mutations, generation, and hook live there. This package keeps a local
// view in Index populated via FromCore conversion from core records, then
// serves tool queries from Snapshot copies. This package never mutates core
// state and never blocks the data path.
//
// Snapshots copy the view tables under a short read lock, then all queries
// run on the copy with no locks held. Tools get hidden filtering plus
// same-name disambiguation from the snapshot. The view describes the segment
// this node can currently see, never a global truth.
package graph

import (
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
)

// Sentinel node names for foreign endpoints.
//
// When an endpoint participant GID has no participant record, tools cannot
// resolve a node name, so entity info carries UnknownNodeName plus
// UnknownNodeNamespace. When the participant record exists but carries no
// node entities, the endpoint comes from a bare DDS application outside ROS,
// so entity info carries BareDDSAppName plus BareDDSAppNamespace.
const (
	// UnknownNodeName marks endpoints with no participant record.
	UnknownNodeName = "_UNKNOWN_NODE"
	// UnknownNodeNamespace marks endpoints with no participant record.
	UnknownNodeNamespace = "_UNKNOWN_NAMESPACE"
	// BareDDSAppName marks endpoints from bare DDS applications.
	BareDDSAppName = "_BARE_DDS_APP"
	// BareDDSAppNamespace is the namespace for bare DDS applications.
	BareDDSAppNamespace = "/"
)

// GID is the 16 byte global id for one endpoint or participant.
type GID [16]byte

// IsZero reports whether the GID carries no identity.
func (g GID) IsZero() bool {
	return g == GID{}
}

// String returns the GID as 32 lowercase hex digits.
func (g GID) String() string {
	var buf [16]byte
	copy(buf[:], g[:])
	return hex.EncodeToString(buf[:])
}

// GIDFromString parses 32 lowercase or uppercase hex digits into a GID.
func GIDFromString(s string) (GID, error) {
	raw, err := hex.DecodeString(s)
	if err != nil {
		return GID{}, fmt.Errorf("graph: parse GID %q: %w", s, err)
	}
	if len(raw) != 16 {
		return GID{}, fmt.Errorf("graph: parse GID %q: want 16 bytes, have %d", s, len(raw))
	}
	var out GID
	copy(out[:], raw)
	return out, nil
}

// NodeRef names one ROS node instance inside a participant.
type NodeRef struct {
	// Name is the node name such as talker.
	Name string
	// Namespace is the node namespace such as /ns.
	Namespace string
	// Enclave is the participant enclave such as /.
	Enclave string
}

// QoSProfile is the endpoint QoS carried in the index for tools.
//
// It mirrors the core RMW profile in display form: history plus depth plus
// reliability plus durability plus liveliness. Matching lives in core; the
// snapshot only carries the profile so info verbs can report it. The struct
// holds plain strings so the graph package reads from any index shape
// without importing core and without blocking the data path.
type QoSProfile struct {
	// History names the history kind such as keep_last.
	History string
	// Depth is the keep last depth.
	Depth uint64
	// Reliability names the reliability kind such as reliable.
	Reliability string
	// Durability names the durability kind such as volatile.
	Durability string
	// Liveliness names the liveliness kind such as automatic.
	Liveliness string
}

// DefaultQoS returns the standard tool default profile.
func DefaultQoS() QoSProfile {
	return QoSProfile{
		History:     "keep_last",
		Depth:       10,
		Reliability: "reliable",
		Durability:  "volatile",
		Liveliness:  "automatic",
	}
}

// ServicesDefaultQoS returns the standard service default profile.
func ServicesDefaultQoS() QoSProfile {
	return QoSProfile{
		History:     "keep_last",
		Depth:       10,
		Reliability: "reliable",
		Durability:  "volatile",
		Liveliness:  "automatic",
	}
}

// EntityInfo is one writer or reader record.
//
// TopicTypeHash identifies the exact type version from discovery. Empty
// means a pre-hash peer that did not advertise a hash. Participant is the
// owner participant GID used for node lookup.
type EntityInfo struct {
	// GID is the unique endpoint id.
	GID GID
	// TopicName is the fully qualified ROS name such as /chatter.
	TopicName string
	// TopicType is the ROS type name such as std_msgs/msg/String.
	TopicType string
	// TopicTypeHash is the RIHS01 type hash, empty for pre-hash peers.
	TopicTypeHash string
	// Participant is the owner participant GID.
	Participant GID
	// QoS is the endpoint QoS profile.
	QoS QoSProfile
}

// ServiceInfo is one service record.
//
// Only the service type hash identifies the exact type version. Participant
// is the owner participant GID used for node lookup.
type ServiceInfo struct {
	// GID is the unique service endpoint id.
	GID GID
	// ServiceName is the fully qualified ROS name such as /add_two_ints.
	ServiceName string
	// ServiceType is the ROS service type such as example_interfaces/srv/AddTwoInts.
	ServiceType string
	// ServiceTypeHash is the RIHS01 type hash, empty for pre-hash peers.
	ServiceTypeHash string
	// Participant is the owner participant GID.
	Participant GID
	// QoS is the service endpoint QoS profile.
	QoS QoSProfile
}

// ParticipantInfo is one participant record.
//
// Nodes is the node entity sequence for the participant plus the enclave
// string. Empty Nodes means a bare DDS application outside ROS.
type ParticipantInfo struct {
	// GID is the participant id.
	GID GID
	// Enclave is the participant enclave such as /.
	Enclave string
	// Nodes is the node entity sequence, empty for bare DDS apps.
	Nodes []NodeRef
}

// TopicEndpointInfo is entity info by topic for tools.
//
// NodeName plus NodeNamespace come from GID lookup through the participant
// table. Unknown participants use the unknown sentinels, bare DDS
// applications use the bare DDS sentinels.
type TopicEndpointInfo struct {
	// NodeName is the resolved node name or a sentinel.
	NodeName string
	// NodeNamespace is the resolved node namespace or a sentinel.
	NodeNamespace string
	// TopicType is the ROS type name.
	TopicType string
	// TopicTypeHash is the RIHS01 type hash.
	TopicTypeHash string
	// GID is the endpoint id.
	GID GID
	// QoS is the endpoint QoS profile.
	QoS QoSProfile
}

// ServiceEndpointInfo is entity info by service for tools.
//
// NodeName plus NodeNamespace plus ServiceType let callers match reader to
// writer by node plus type.
type ServiceEndpointInfo struct {
	// NodeName is the resolved node name or a sentinel.
	NodeName string
	// NodeNamespace is the resolved node namespace or a sentinel.
	NodeNamespace string
	// ServiceType is the ROS service type name.
	ServiceType string
	// ServiceTypeHash is the RIHS01 type hash.
	ServiceTypeHash string
	// GID is the service endpoint id.
	GID GID
	// QoS is the service endpoint QoS profile.
	QoS QoSProfile
}

// NameAndType pairs one topic or service name with its merged type list.
type NameAndType struct {
	// Name is the fully qualified ROS name.
	Name string
	// Types holds the merged sorted type names across readers plus writers.
	Types []string
}

// NodeCount pairs one node with its endpoint count.
type NodeCount struct {
	// Name is the node name.
	Name string
	// Namespace is the node namespace.
	Namespace string
	// Enclave is the node enclave.
	Enclave string
	// Count is the number of endpoints owned by the node.
	Count int
}

// Index is the local read view backing tool snapshots.
//
// It holds writer plus reader maps from GID to entity info, a service map
// from GID to service info, and a participant map from GID to participant
// info. Every method locks the mutex. The index spawns no goroutine. Populate
// it via FromCore conversion from the core write path; generation and hook
// here track view updates only. Use NewIndexWithCapacity for static
// topologies with presized tables.
type Index struct {
	mu           sync.RWMutex
	writers      map[GID]EntityInfo
	readers      map[GID]EntityInfo
	services     map[GID]ServiceInfo
	participants map[GID]ParticipantInfo
	generation   uint64
	onChange     func(uint64)
}

// NewIndex returns an empty index with unpresized tables.
func NewIndex() *Index {
	return &Index{
		writers:      make(map[GID]EntityInfo),
		readers:      make(map[GID]EntityInfo),
		services:     make(map[GID]ServiceInfo),
		participants: make(map[GID]ParticipantInfo),
	}
}

// NewIndexWithCapacity returns an empty index with presized tables.
//
// It serves static topologies on embedded targets: tables are allocated
// once with the given capacities and no thread is started. Zero or
// negative capacities fall back to unpresized tables.
func NewIndexWithCapacity(writers, readers, services, participants int) *Index {
	idx := NewIndex()
	if writers > 0 {
		idx.writers = make(map[GID]EntityInfo, writers)
	}
	if readers > 0 {
		idx.readers = make(map[GID]EntityInfo, readers)
	}
	if services > 0 {
		idx.services = make(map[GID]ServiceInfo, services)
	}
	if participants > 0 {
		idx.participants = make(map[GID]ParticipantInfo, participants)
	}
	return idx
}

// SetOnChange installs the hook fired with each new generation.
func (idx *Index) SetOnChange(fn func(uint64)) {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	idx.onChange = fn
}

// Generation returns the current mutation generation.
func (idx *Index) Generation() uint64 {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return idx.generation
}

// UpsertWriter adds or replaces one writer record.
func (idx *Index) UpsertWriter(info EntityInfo) error {
	err := validateEntity(info)
	if err != nil {
		return fmt.Errorf("graph: upsert writer: %w", err)
	}
	hook, gen, changed := idx.upsertWriterLocked(info)
	if hook != nil && changed {
		hook(gen)
	}
	return nil
}

// upsertWriterLocked implements UpsertWriter with the write lock held.
func (idx *Index) upsertWriterLocked(info EntityInfo) (func(uint64), uint64, bool) {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	existing, seen := idx.writers[info.GID]
	if seen && existing == info {
		return nil, idx.generation, false
	}
	idx.writers[info.GID] = info
	idx.generation++
	return idx.onChange, idx.generation, true
}

// UpsertReader adds or replaces one reader record.
func (idx *Index) UpsertReader(info EntityInfo) error {
	err := validateEntity(info)
	if err != nil {
		return fmt.Errorf("graph: upsert reader: %w", err)
	}
	hook, gen, changed := idx.upsertReaderLocked(info)
	if hook != nil && changed {
		hook(gen)
	}
	return nil
}

// upsertReaderLocked implements UpsertReader with the write lock held.
func (idx *Index) upsertReaderLocked(info EntityInfo) (func(uint64), uint64, bool) {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	existing, seen := idx.readers[info.GID]
	if seen && existing == info {
		return nil, idx.generation, false
	}
	idx.readers[info.GID] = info
	idx.generation++
	return idx.onChange, idx.generation, true
}

// RemoveWriter deletes one writer record by GID.
func (idx *Index) RemoveWriter(gid GID) bool {
	idx.mu.Lock()
	_, seen := idx.writers[gid]
	if !seen {
		idx.mu.Unlock()
		return false
	}
	delete(idx.writers, gid)
	idx.generation++
	gen := idx.generation
	hook := idx.onChange
	idx.mu.Unlock()
	if hook != nil {
		hook(gen)
	}
	return true
}

// RemoveReader deletes one reader record by GID.
func (idx *Index) RemoveReader(gid GID) bool {
	idx.mu.Lock()
	_, seen := idx.readers[gid]
	if !seen {
		idx.mu.Unlock()
		return false
	}
	delete(idx.readers, gid)
	idx.generation++
	gen := idx.generation
	hook := idx.onChange
	idx.mu.Unlock()
	if hook != nil {
		hook(gen)
	}
	return true
}

// UpsertService adds or replaces one service record.
func (idx *Index) UpsertService(info ServiceInfo) error {
	err := validateService(info)
	if err != nil {
		return fmt.Errorf("graph: upsert service: %w", err)
	}
	idx.mu.Lock()
	existing, seen := idx.services[info.GID]
	if seen && existing == info {
		idx.mu.Unlock()
		return nil
	}
	idx.services[info.GID] = info
	idx.generation++
	gen := idx.generation
	hook := idx.onChange
	idx.mu.Unlock()
	if hook != nil {
		hook(gen)
	}
	return nil
}

// RemoveService deletes one service record by GID.
func (idx *Index) RemoveService(gid GID) bool {
	idx.mu.Lock()
	_, seen := idx.services[gid]
	if !seen {
		idx.mu.Unlock()
		return false
	}
	delete(idx.services, gid)
	idx.generation++
	gen := idx.generation
	hook := idx.onChange
	idx.mu.Unlock()
	if hook != nil {
		hook(gen)
	}
	return true
}

// UpsertParticipant adds or replaces one participant record.
func (idx *Index) UpsertParticipant(info ParticipantInfo) error {
	err := validateParticipant(info)
	if err != nil {
		return fmt.Errorf("graph: upsert participant: %w", err)
	}
	idx.mu.Lock()
	existing, seen := idx.participants[info.GID]
	if seen && equalParticipants(existing, info) {
		idx.mu.Unlock()
		return nil
	}
	idx.participants[info.GID] = cloneParticipant(info)
	idx.generation++
	gen := idx.generation
	hook := idx.onChange
	idx.mu.Unlock()
	if hook != nil {
		hook(gen)
	}
	return nil
}

// RemoveParticipant deletes one participant record by GID.
func (idx *Index) RemoveParticipant(gid GID) bool {
	idx.mu.Lock()
	_, seen := idx.participants[gid]
	if !seen {
		idx.mu.Unlock()
		return false
	}
	delete(idx.participants, gid)
	idx.generation++
	gen := idx.generation
	hook := idx.onChange
	idx.mu.Unlock()
	if hook != nil {
		hook(gen)
	}
	return true
}

// Snapshot copies the index without blocking the data path.
//
// It holds the read lock only for the copy, then releases it. All snapshot
// queries run on the copy with no locks held, so tools never block
// discovery or the data plane. The snapshot is partition local: it
// describes the segment this node can currently see, never a global truth.
func (idx *Index) Snapshot() Snapshot {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	out := Snapshot{
		Generation:   idx.generation,
		writers:      make([]EntityInfo, 0, len(idx.writers)),
		readers:      make([]EntityInfo, 0, len(idx.readers)),
		services:     make([]ServiceInfo, 0, len(idx.services)),
		participants: make(map[GID]ParticipantInfo, len(idx.participants)),
	}
	for _, info := range idx.writers {
		out.writers = append(out.writers, info)
	}
	for _, info := range idx.readers {
		out.readers = append(out.readers, info)
	}
	for _, info := range idx.services {
		out.services = append(out.services, info)
	}
	for gid, info := range idx.participants {
		out.participants[gid] = cloneParticipant(info)
	}
	sortEntities(out.writers)
	sortEntities(out.readers)
	sortServices(out.services)
	return out
}

// validateEntity checks one entity record.
// Topic names must be canonical ROS form like core: leading slash, no empty
// segments, no trailing slash, and no whitespace.
func validateEntity(info EntityInfo) error {
	if info.GID.IsZero() {
		return errors.New("graph: entity GID is zero") //nolint:wrapcheck // new error, nothing to wrap
	}
	err := validateROSTopicName(info.TopicName)
	if err != nil {
		return fmt.Errorf("graph: entity topic: %w", err)
	}
	if info.TopicType == "" {
		return fmt.Errorf("graph: entity topic %q misses type", info.TopicName)
	}
	return nil
}

// validateService checks one service record.
// Service names must be canonical ROS form like core.
func validateService(info ServiceInfo) error {
	if info.GID.IsZero() {
		return errors.New("graph: service GID is zero") //nolint:wrapcheck // new error, nothing to wrap
	}
	err := validateROSTopicName(info.ServiceName)
	if err != nil {
		return fmt.Errorf("graph: service: %w", err)
	}
	if info.ServiceType == "" {
		return fmt.Errorf("graph: service %q misses type", info.ServiceName)
	}
	return nil
}

// validateROSTopicName rejects names that are not canonical ROS form.
// It mirrors core validation: leading slash, no empty segments, no trailing
// slash except root, and no whitespace.
func validateROSTopicName(name string) error {
	trimmed := strings.TrimSpace(name)
	if trimmed == "" {
		return errors.New("graph: topic name is empty") //nolint:wrapcheck // new error, nothing to wrap
	}
	if !strings.HasPrefix(trimmed, "/") {
		return fmt.Errorf("graph: topic name %q must start with slash", name)
	}
	if strings.Contains(trimmed, "//") {
		return fmt.Errorf("graph: topic name %q has an empty segment", name)
	}
	if strings.HasSuffix(trimmed, "/") && trimmed != "/" {
		return fmt.Errorf("graph: topic name %q has a trailing slash", name)
	}
	if strings.ContainsAny(trimmed, " \t\n\r") {
		return fmt.Errorf("graph: topic name %q has whitespace", name)
	}
	for seg := range strings.SplitSeq(strings.Trim(trimmed, "/"), "/") {
		if seg == "" && trimmed != "/" {
			return fmt.Errorf("graph: topic name %q has an empty segment", name)
		}
	}
	return nil
}

// validateParticipant checks one participant record.
func validateParticipant(info ParticipantInfo) error {
	if info.GID.IsZero() {
		return errors.New("graph: participant GID is zero") //nolint:wrapcheck // new error, nothing to wrap
	}
	for i := range info.Nodes {
		if info.Nodes[i].Name == "" {
			return fmt.Errorf("graph: participant node %d misses name", i)
		}
		if info.Nodes[i].Namespace == "" {
			return fmt.Errorf("graph: participant node %q misses namespace", info.Nodes[i].Name)
		}
		if info.Nodes[i].Namespace[0] != '/' {
			return fmt.Errorf("graph: participant node %q namespace %q must start with slash", info.Nodes[i].Name, info.Nodes[i].Namespace)
		}
	}
	return nil
}

// equalParticipants reports whether two participant records match.
func equalParticipants(a, b ParticipantInfo) bool {
	if a.GID != b.GID {
		return false
	}
	if a.Enclave != b.Enclave {
		return false
	}
	if len(a.Nodes) != len(b.Nodes) {
		return false
	}
	for i := range a.Nodes {
		if a.Nodes[i] != b.Nodes[i] {
			return false
		}
	}
	return true
}

// cloneParticipant returns a deep copy of one participant record.
func cloneParticipant(info ParticipantInfo) ParticipantInfo {
	out := ParticipantInfo{
		GID:     info.GID,
		Enclave: info.Enclave,
	}
	if info.Nodes != nil {
		out.Nodes = make([]NodeRef, len(info.Nodes))
		copy(out.Nodes, info.Nodes)
	} else {
		out.Nodes = []NodeRef{}
	}
	return out
}

// sortEntities orders endpoints by topic, then type, then GID.
func sortEntities(in []EntityInfo) {
	sort.Slice(in, func(i, j int) bool {
		if in[i].TopicName != in[j].TopicName {
			return in[i].TopicName < in[j].TopicName
		}
		if in[i].TopicType != in[j].TopicType {
			return in[i].TopicType < in[j].TopicType
		}
		return in[i].GID.String() < in[j].GID.String()
	})
}

// sortServices orders service records by name, then type, then GID.
func sortServices(in []ServiceInfo) {
	sort.Slice(in, func(i, j int) bool {
		if in[i].ServiceName != in[j].ServiceName {
			return in[i].ServiceName < in[j].ServiceName
		}
		if in[i].ServiceType != in[j].ServiceType {
			return in[i].ServiceType < in[j].ServiceType
		}
		return in[i].GID.String() < in[j].GID.String()
	})
}

// Snapshot is a partition-local copy of the graph index for tools.
//
// All queries run on the copy with no locks held, so tools never block
// discovery or the data path. The snapshot describes the segment this node
// can currently see, never a global truth: two nodes in different partitions
// report different snapshots by design.
type Snapshot struct {
	// Generation is the index generation the snapshot was taken at.
	Generation   uint64
	writers      []EntityInfo
	readers      []EntityInfo
	services     []ServiceInfo
	participants map[GID]ParticipantInfo
}

// Describe returns the partition-local description of the snapshot.
func (s Snapshot) Describe() string {
	return fmt.Sprintf("partition local graph: generation %d with %d writers, %d readers, %d services, %d participants; segment this node can currently see, never a global truth",
		s.Generation, len(s.writers), len(s.readers), len(s.services), len(s.participants))
}

// PublisherCount returns the writer count for one topic.
func (s Snapshot) PublisherCount(topic string) int {
	count := 0
	for _, info := range s.writers {
		if info.TopicName == topic {
			count++
		}
	}
	return count
}

// SubscriptionCount returns the reader count for one topic.
func (s Snapshot) SubscriptionCount(topic string) int {
	count := 0
	for _, info := range s.readers {
		if info.TopicName == topic {
			count++
		}
	}
	return count
}

// PublisherCounts returns writer counts by topic.
func (s Snapshot) PublisherCounts() map[string]int {
	out := make(map[string]int)
	for _, info := range s.writers {
		out[info.TopicName]++
	}
	return out
}

// SubscriptionCounts returns reader counts by topic.
func (s Snapshot) SubscriptionCounts() map[string]int {
	out := make(map[string]int)
	for _, info := range s.readers {
		out[info.TopicName]++
	}
	return out
}

// resolveNode fills node name plus namespace through GID lookup.
//
// Unknown participant GIDs use the unknown sentinels. Participants with no
// node entities are bare DDS applications and use the bare DDS sentinels.
// Each endpoint is attributed to its own participant node: a participant
// with exactly one node answers with that node. A participant with several
// nodes is ambiguous without per-endpoint node identity, so it answers with
// the unknown sentinels instead of silently picking the first node.
func (s Snapshot) resolveNode(participant GID) (string, string) {
	info, ok := s.participants[participant]
	if !ok {
		return UnknownNodeName, UnknownNodeNamespace
	}
	if len(info.Nodes) == 0 {
		return BareDDSAppName, BareDDSAppNamespace
	}
	if len(info.Nodes) > 1 {
		return UnknownNodeName, UnknownNodeNamespace
	}
	return info.Nodes[0].Name, info.Nodes[0].Namespace
}

// PublishersInfoByTopic returns writer entity info for one topic.
//
// Node names fill through GID lookup with unknown plus bare DDS sentinels
// for foreign endpoints. Results sort by node, then type, then GID.
func (s Snapshot) PublishersInfoByTopic(topic string) []TopicEndpointInfo {
	var out []TopicEndpointInfo
	for _, info := range s.writers {
		if info.TopicName != topic {
			continue
		}
		name, ns := s.resolveNode(info.Participant)
		out = append(out, TopicEndpointInfo{
			NodeName:      name,
			NodeNamespace: ns,
			TopicType:     info.TopicType,
			TopicTypeHash: info.TopicTypeHash,
			GID:           info.GID,
			QoS:           info.QoS,
		})
	}
	sortEndpointInfos(out)
	return out
}

// SubscriptionsInfoByTopic returns reader entity info for one topic.
//
// Node names fill through GID lookup with unknown plus bare DDS sentinels
// for foreign endpoints. Results sort by node, then type, then GID.
func (s Snapshot) SubscriptionsInfoByTopic(topic string) []TopicEndpointInfo {
	var out []TopicEndpointInfo
	for _, info := range s.readers {
		if info.TopicName != topic {
			continue
		}
		name, ns := s.resolveNode(info.Participant)
		out = append(out, TopicEndpointInfo{
			NodeName:      name,
			NodeNamespace: ns,
			TopicType:     info.TopicType,
			TopicTypeHash: info.TopicTypeHash,
			GID:           info.GID,
			QoS:           info.QoS,
		})
	}
	sortEndpointInfos(out)
	return out
}

// compareNodeName orders by namespace then name, returning minus one, zero, or one.
func compareNodeName(nsA, nameA, nsB, nameB string) int {
	if nsA != nsB {
		if nsA < nsB {
			return -1
		}
		return 1
	}
	if nameA != nameB {
		if nameA < nameB {
			return -1
		}
		return 1
	}
	return 0
}

// sortEndpointInfos orders topic endpoint infos by node, then type, then GID.
func sortEndpointInfos(in []TopicEndpointInfo) {
	sort.Slice(in, func(i, j int) bool {
		if c := compareNodeName(in[i].NodeNamespace, in[i].NodeName, in[j].NodeNamespace, in[j].NodeName); c != 0 {
			return c < 0
		}
		if in[i].TopicType != in[j].TopicType {
			return in[i].TopicType < in[j].TopicType
		}
		return in[i].GID.String() < in[j].GID.String()
	})
}

// ServiceInfosByService returns service entity info for one service name.
//
// Each entry carries node plus type, so callers match reader to writer by
// node plus type. Node names fill through GID lookup with unknown plus bare
// DDS sentinels for foreign endpoints. Results sort by node, then type,
// then GID.
func (s Snapshot) ServiceInfosByService(service string) []ServiceEndpointInfo {
	var out []ServiceEndpointInfo
	for _, info := range s.services {
		if info.ServiceName != service {
			continue
		}
		name, ns := s.resolveNode(info.Participant)
		out = append(out, ServiceEndpointInfo{
			NodeName:        name,
			NodeNamespace:   ns,
			ServiceType:     info.ServiceType,
			ServiceTypeHash: info.ServiceTypeHash,
			GID:             info.GID,
			QoS:             info.QoS,
		})
	}
	sort.Slice(out, func(i, j int) bool {
		if c := compareNodeName(out[i].NodeNamespace, out[i].NodeName, out[j].NodeNamespace, out[j].NodeName); c != 0 {
			return c < 0
		}
		if out[i].ServiceType != out[j].ServiceType {
			return out[i].ServiceType < out[j].ServiceType
		}
		return out[i].GID.String() < out[j].GID.String()
	})
	return out
}

// TopicNamesAndTypes returns names plus merged types across readers plus writers.
//
// Types merge across both directions: a topic with a writer of one type and
// a reader of another lists both types sorted. Hidden topics filter out
// unless includeHidden is true. Results sort by name.
func (s Snapshot) TopicNamesAndTypes(includeHidden bool) []NameAndType {
	merged := make(map[string]map[string]bool)
	for _, info := range s.writers {
		if !includeHidden && IsHiddenTopic(info.TopicName) {
			continue
		}
		addNameType(merged, info.TopicName, info.TopicType)
	}
	for _, info := range s.readers {
		if !includeHidden && IsHiddenTopic(info.TopicName) {
			continue
		}
		addNameType(merged, info.TopicName, info.TopicType)
	}
	return sortedNameTypes(merged)
}

// ServiceNamesAndTypes returns service names plus merged types.
//
// Hidden services filter out unless includeHidden is true. Results sort by name.
func (s Snapshot) ServiceNamesAndTypes(includeHidden bool) []NameAndType {
	merged := make(map[string]map[string]bool)
	for _, info := range s.services {
		if !includeHidden && IsHiddenService(info.ServiceName) {
			continue
		}
		addNameType(merged, info.ServiceName, info.ServiceType)
	}
	return sortedNameTypes(merged)
}

// addNameType merges one name plus type pair into the merge table.
func addNameType(merged map[string]map[string]bool, name, typ string) {
	set, ok := merged[name]
	if !ok {
		set = make(map[string]bool)
		merged[name] = set
	}
	set[typ] = true
}

// sortedNameTypes flattens the merge table into sorted names with sorted types.
func sortedNameTypes(merged map[string]map[string]bool) []NameAndType {
	out := make([]NameAndType, 0, len(merged))
	for name, set := range merged {
		types := make([]string, 0, len(set))
		for typ := range set {
			types = append(types, typ)
		}
		sort.Strings(types)
		out = append(out, NameAndType{Name: name, Types: types})
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].Name < out[j].Name
	})
	return out
}

// PublishersByNode returns topic names plus types published by one node.
//
// The node lookup matches resolved writer nodes by name plus namespace
// across enclaves. Hidden topics filter out unless includeHidden is true.
func (s Snapshot) PublishersByNode(nodeName, namespace string, includeHidden bool) []NameAndType {
	merged := make(map[string]map[string]bool)
	for _, info := range s.writers {
		name, ns := s.resolveNode(info.Participant)
		if name != nodeName || ns != namespace {
			continue
		}
		if !includeHidden && IsHiddenTopic(info.TopicName) {
			continue
		}
		addNameType(merged, info.TopicName, info.TopicType)
	}
	return sortedNameTypes(merged)
}

// SubscriptionsByNode returns topic names plus types subscribed by one node.
//
// The node lookup matches resolved reader nodes by name plus namespace
// across enclaves. Hidden topics filter out unless includeHidden is true.
func (s Snapshot) SubscriptionsByNode(nodeName, namespace string, includeHidden bool) []NameAndType {
	merged := make(map[string]map[string]bool)
	for _, info := range s.readers {
		name, ns := s.resolveNode(info.Participant)
		if name != nodeName || ns != namespace {
			continue
		}
		if !includeHidden && IsHiddenTopic(info.TopicName) {
			continue
		}
		addNameType(merged, info.TopicName, info.TopicType)
	}
	return sortedNameTypes(merged)
}

// ServicesByNode returns service names plus types owned by one node.
//
// The node lookup matches resolved service nodes by name plus namespace
// across enclaves. Hidden services filter out unless includeHidden is true.
func (s Snapshot) ServicesByNode(nodeName, namespace string, includeHidden bool) []NameAndType {
	merged := make(map[string]map[string]bool)
	for _, info := range s.services {
		name, ns := s.resolveNode(info.Participant)
		if name != nodeName || ns != namespace {
			continue
		}
		if !includeHidden && IsHiddenService(info.ServiceName) {
			continue
		}
		addNameType(merged, info.ServiceName, info.ServiceType)
	}
	return sortedNameTypes(merged)
}

// NodeNames returns the sorted unique node refs in the snapshot.
//
// Uniqueness keys on name plus namespace plus enclave. Hidden nodes filter
// out unless includeHidden is true. Foreign endpoints resolve to sentinel
// refs and are included like any other node.
func (s Snapshot) NodeNames(includeHidden bool) []NodeRef {
	seen := make(map[NodeRef]bool)
	collectParticipantNodes(s.participants, includeHidden, seen)
	collectEntityNodes(s.writers, s.resolveNode, s.enclaveFor, includeHidden, seen)
	collectEntityNodes(s.readers, s.resolveNode, s.enclaveFor, includeHidden, seen)
	collectServiceNodes(s.services, s.resolveNode, s.enclaveFor, includeHidden, seen)
	out := make([]NodeRef, 0, len(seen))
	for ref := range seen {
		out = append(out, ref)
	}
	sortNodeRefs(out)
	return out
}

// collectParticipantNodes adds participant node entities to the seen set.
func collectParticipantNodes(participants map[GID]ParticipantInfo, includeHidden bool, seen map[NodeRef]bool) {
	for _, info := range participants {
		for _, node := range info.Nodes {
			if !includeHidden && IsHiddenNode(node.Name) {
				continue
			}
			seen[NodeRef{Name: node.Name, Namespace: node.Namespace, Enclave: info.Enclave}] = true
		}
	}
}

// collectEntityNodes adds resolved topic endpoint nodes to the seen set.
func collectEntityNodes(
	infos []EntityInfo,
	resolve func(GID) (string, string),
	enclave func(GID) string,
	includeHidden bool,
	seen map[NodeRef]bool,
) {
	for _, info := range infos {
		name, ns := resolve(info.Participant)
		if !includeHidden && IsHiddenNode(name) {
			continue
		}
		seen[NodeRef{Name: name, Namespace: ns, Enclave: enclave(info.Participant)}] = true
	}
}

// collectServiceNodes adds resolved service endpoint nodes to the seen set.
func collectServiceNodes(
	infos []ServiceInfo,
	resolve func(GID) (string, string),
	enclave func(GID) string,
	includeHidden bool,
	seen map[NodeRef]bool,
) {
	for _, info := range infos {
		name, ns := resolve(info.Participant)
		if !includeHidden && IsHiddenNode(name) {
			continue
		}
		seen[NodeRef{Name: name, Namespace: ns, Enclave: enclave(info.Participant)}] = true
	}
}

// sortNodeRefs orders node refs by namespace, then name, then enclave.
func sortNodeRefs(out []NodeRef) {
	sort.Slice(out, func(i, j int) bool {
		if out[i].Namespace != out[j].Namespace {
			return out[i].Namespace < out[j].Namespace
		}
		if out[i].Name != out[j].Name {
			return out[i].Name < out[j].Name
		}
		return out[i].Enclave < out[j].Enclave
	})
}

// enclaveFor returns the enclave for one participant GID or empty when unknown.
func (s Snapshot) enclaveFor(participant GID) string {
	info, ok := s.participants[participant]
	if !ok {
		return ""
	}
	return info.Enclave
}

// NodeCounts returns per-node endpoint counts sorted by namespace plus name.
//
// Every writer, reader, and service endpoint counts once under its resolved
// node. Hidden nodes filter out unless includeHidden is true.
func (s Snapshot) NodeCounts(includeHidden bool) []NodeCount {
	counts := make(map[NodeRef]int)
	countEndpoint := func(participant GID) {
		name, ns := s.resolveNode(participant)
		if !includeHidden && IsHiddenNode(name) {
			return
		}
		key := NodeRef{Name: name, Namespace: ns, Enclave: s.enclaveFor(participant)}
		counts[key]++
	}
	for _, info := range s.writers {
		countEndpoint(info.Participant)
	}
	for _, info := range s.readers {
		countEndpoint(info.Participant)
	}
	for _, info := range s.services {
		countEndpoint(info.Participant)
	}
	out := make([]NodeCount, 0, len(counts))
	for ref, count := range counts {
		out = append(out, NodeCount{Name: ref.Name, Namespace: ref.Namespace, Enclave: ref.Enclave, Count: count})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Namespace != out[j].Namespace {
			return out[i].Namespace < out[j].Namespace
		}
		if out[i].Name != out[j].Name {
			return out[i].Name < out[j].Name
		}
		return out[i].Enclave < out[j].Enclave
	})
	return out
}

// DisambiguatedNodeNames returns display names with same-name disambiguation.
//
// Nodes sort by namespace, then name, then enclave. The first occurrence of
// a namespace plus name pair keeps the plain fully qualified form
// namespace plus name; later duplicates gain a space plus parenthesized
// index such as /ns/talker (2). Hidden nodes filter out unless includeHidden
// is true.
func (s Snapshot) DisambiguatedNodeNames(includeHidden bool) []string {
	refs := s.NodeNames(includeHidden)
	seen := make(map[string]int)
	out := make([]string, 0, len(refs))
	for _, ref := range refs {
		base := strings.TrimSuffix(ref.Namespace, "/") + "/" + ref.Name
		if strings.HasPrefix(base, "//") {
			base = base[1:]
		}
		count := seen[base]
		seen[base] = count + 1
		if count == 0 {
			out = append(out, base)
			continue
		}
		out = append(out, fmt.Sprintf("%s (%d)", base, count+1))
	}
	return out
}

// IsHiddenTopic reports whether a topic name is hidden.
//
// Any slash-separated segment starting with underscore hides the name, so
// /foo/_bar and /_hidden are hidden while /chatter is visible.
func IsHiddenTopic(name string) bool {
	return isHiddenPath(name)
}

// IsHiddenService reports whether a service name is hidden.
//
// The rule matches topics: any segment starting with underscore hides it.
func IsHiddenService(name string) bool {
	return isHiddenPath(name)
}

// IsHiddenNode reports whether a node name is hidden.
//
// A leading underscore hides the node, so _hidden is hidden while talker
// is visible. Namespaces do not affect the answer.
func IsHiddenNode(name string) bool {
	return strings.HasPrefix(name, "_")
}

// isHiddenPath reports whether any slash-separated segment starts with underscore.
func isHiddenPath(name string) bool {
	for part := range strings.SplitSeq(name, "/") {
		if strings.HasPrefix(part, "_") {
			return true
		}
	}
	return false
}
