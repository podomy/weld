// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

// Package core implements transport free matching, QoS, routing, and state
// machines for Weld.
//
// This file owns the partition local graph index. Discovery feeds one index
// with a generation counter and a change hook. The index describes the
// segment this node can currently see, never a global truth. No quorum is
// pretended, no query blocks across a dead link, and no decision assumes two
// nodes see the same graph. The data plane stays leaderless: every endpoint
// has exactly one owner, its creator, and discovery merges observations
// deterministically without a vote.
//
// Boundary: this file is the write path truth for the graph index.
// Discovery mutations, generation, and hook live here. Internal/graph is
// the read snapshot view for tools and populates its view via FromCore
// conversion in internal/graph/from_core.go without mutating this index.
package core

import (
	"encoding/hex"
	"errors"
	"fmt"
	"maps"
	"strings"
	"sync"
)

// GID identifies one endpoint or participant.
//
// It holds the 16 byte RMW GID: the DDS participant GUID bytes on DDS paths
// and the derived token bytes on Zenoh paths. The zero value means unset.
type GID [16]byte

// String renders the GID as 32 lowercase hex digits.
func (g GID) String() string {
	return hex.EncodeToString(g[:])
}

// Equal reports whether g and other hold the same bytes.
func (g GID) Equal(other GID) bool {
	return g == other
}

// IsZero reports whether g is the unset zero value.
func (g GID) IsZero() bool {
	return g == GID{}
}

// GIDFromBytes copies exactly 16 bytes into a GID.
func GIDFromBytes(b []byte) (GID, error) {
	var gid GID
	if len(b) != len(gid) {
		return GID{}, fmt.Errorf("core: gid needs %d bytes, got %d", len(gid), len(b))
	}
	copy(gid[:], b)
	return gid, nil
}

// ParseGID parses 32 hex digits into a GID.
func ParseGID(s string) (GID, error) {
	raw, err := hex.DecodeString(s)
	if err != nil {
		return GID{}, fmt.Errorf("core: parse gid %q: %w", s, err)
	}
	gid, err := GIDFromBytes(raw)
	if err != nil {
		return GID{}, fmt.Errorf("core: parse gid %q: %w", s, err)
	}
	return gid, nil
}

// EntityInfo is one writer or reader record in the index.
//
// TopicName is the demangled ROS name such as /chatter, never the DDS form
// such as rt/chatter. TopicType is the demangled IDL name such as
// std_msgs__msg__String, never the DDS form. TypeHash is the parsed type
// hash string such as RIHS01_ plus hex, or empty for the zero hash from
// pre hash peers. ParticipantGID links to the participant map for node name
// and namespace lookup. QoS is the translated RMW profile, already
// demangled and translated at the border before insert.
type EntityInfo struct {
	// TopicName is the demangled ROS topic name.
	TopicName string
	// TopicType is the demangled IDL type name.
	TopicType string
	// TypeHash is the parsed type hash, empty for the zero hash.
	TypeHash string
	// ParticipantGID links to the owning participant.
	ParticipantGID GID
	// QoS is the translated RMW profile.
	QoS QoSProfile
}

// ServiceInfo is one service endpoint record in the index.
//
// ServiceName is the demangled ROS service name such as /add_two_ints.
// ServiceType is the demangled IDL base name. ServiceTypeHash is the only
// hash stored for services. ParticipantGID links to the owning participant.
type ServiceInfo struct {
	// ServiceName is the demangled ROS service name.
	ServiceName string
	// ServiceType is the demangled IDL service base name.
	ServiceType string
	// ServiceTypeHash is the service type hash, empty for the zero hash.
	ServiceTypeHash string
	// ParticipantGID links to the owning participant.
	ParticipantGID GID
}

// ParticipantInfo is one participant record in the index.
//
// Enclave is the ROS enclave string such as /. NodeName plus NodeNamespace
// identify the node for GID lookup. Sequence is the node entity sequence
// carried by ros_discovery_info.
type ParticipantInfo struct {
	// Enclave is the ROS enclave string.
	Enclave string
	// NodeName is the node name for GID lookup.
	NodeName string
	// NodeNamespace is the node namespace for GID lookup.
	NodeNamespace string
	// Sequence is the node entity sequence.
	Sequence uint64
}

// Graph is the partition local discovery index.
//
// Writers maps endpoint GID to entity info for publishers. Readers maps
// endpoint GID to entity info for subscriptions. Services maps endpoint
// GID to service info. Participants maps participant GID to participant
// info. Generation counts applied mutations. OnChange fires with each new
// generation outside the lock. Every method locks the mutex. Queries read
// the cache only and never block on the network. Do not copy a Graph after
// first use.
//
// Liveliness is partition local. Participant announcements carry a
// discovery sequence plus a lease expiry in caller nanos. Alive reports
// with a stale or duplicate sequence never mutate and never bump.
// Dispose removes only with a newer sequence. Sweep removes only expired
// entries. Endpoint announcements stay retained for late joiners: writers
// plus readers are removed only by explicit Remove calls, never by sweep.
// ReplayRetained replays the current generation to a late subscriber
// without bumping.
type Graph struct {
	mu           sync.Mutex
	writers      map[GID]EntityInfo
	readers      map[GID]EntityInfo
	services     map[GID]ServiceInfo
	participants map[GID]ParticipantInfo
	liveness     map[GID]participantLiveness
	generation   uint64
	onChange     func(uint64)
}

// participantLiveness tracks SPDP ordering and lease for one participant.
//
// seq is the last accepted announcement sequence. expiryNanos is the
// lease expiry in caller supplied nanos. The record exists only while
// the participant is alive; dispose or sweep deletes it with the entry.
type participantLiveness struct {
	seq         uint64
	expiryNanos int64
}

// NewGraph returns an empty partition local index.
func NewGraph() *Graph {
	return &Graph{
		writers:      make(map[GID]EntityInfo),
		readers:      make(map[GID]EntityInfo),
		services:     make(map[GID]ServiceInfo),
		participants: make(map[GID]ParticipantInfo),
		liveness:     make(map[GID]participantLiveness),
	}
}

// SetOnChange installs the hook fired with each new generation.
//
// A nil hook clears the previous hook. The hook runs outside the index
// lock so it must not call back into the index while holding assumptions
// about atomicity. SetOnChange itself locks the mutex.
func (g *Graph) SetOnChange(fn func(uint64)) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.onChange = fn
}

// Generation returns the current mutation generation.
//
// The generation starts at zero and increments once per applied mutation.
// A repeated upsert with identical content does not increment. Every
// method locks the mutex.
func (g *Graph) Generation() uint64 {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.generation
}

// addGraphRecord validates info, inserts it under lock when absent, bumps
// the generation, and fires the hook outside the lock.
func addGraphRecord[V any](g *Graph, m map[GID]V, gid GID, info V, validate func(V) error, op string) error {
	err := validate(info)
	if err != nil {
		return fmt.Errorf("core: %s %s: %w", op, gid.String(), err)
	}
	g.mu.Lock()
	if _, exists := m[gid]; exists {
		g.mu.Unlock()
		return fmt.Errorf("core: %s %s: already exists", op, gid.String())
	}
	m[gid] = info
	gen, hook := g.bumpLocked()
	g.mu.Unlock()
	fireHook(hook, gen)
	return nil
}

// AddWriter inserts a writer record, failing when the GID already exists.
//
// The info must carry demangled names plus translated QoS plus a parsed
// type hash; validation rejects DDS wire forms and malformed hashes. On
// success the generation increments and the hook fires outside the lock.
func (g *Graph) AddWriter(gid GID, info EntityInfo) error {
	return addGraphRecord(g, g.writers, gid, info, validateEntityInfo, "add writer")
}

// upsertGraphRecord validates info, replaces it under lock unless identical,
// bumps the generation, and fires the hook outside the lock.
func upsertGraphRecord[V any](g *Graph, m map[GID]V, gid GID, info V, validate func(V) error, equal func(V, V) bool, op string) error {
	err := validate(info)
	if err != nil {
		return fmt.Errorf("core: %s %s: %w", op, gid.String(), err)
	}
	g.mu.Lock()
	existing, seen := m[gid]
	if seen && equal(existing, info) {
		g.mu.Unlock()
		return nil
	}
	m[gid] = info
	gen, hook := g.bumpLocked()
	g.mu.Unlock()
	fireHook(hook, gen)
	return nil
}

// UpsertWriter adds or replaces a writer record.
//
// Identical content is a no op with no generation increment and no hook
// fire, so a sub second fade with no attachment change does not tick the
// generation. Changed or new content increments the generation and fires
// the hook outside the lock.
func (g *Graph) UpsertWriter(gid GID, info EntityInfo) error {
	return upsertGraphRecord(g, g.writers, gid, info, validateEntityInfo, entitiesEqual, "upsert writer")
}

// RemoveWriter deletes a writer record by GID.
//
// It reports false when the GID is absent with no generation increment.
// On removal the generation increments and the hook fires outside the lock.
func (g *Graph) RemoveWriter(gid GID) bool {
	g.mu.Lock()
	if _, seen := g.writers[gid]; !seen {
		g.mu.Unlock()
		return false
	}
	delete(g.writers, gid)
	gen, hook := g.bumpLocked()
	g.mu.Unlock()
	fireHook(hook, gen)
	return true
}

// Writer returns a copy of one writer record by GID.
func (g *Graph) Writer(gid GID) (EntityInfo, bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	info, ok := g.writers[gid]
	return info, ok
}

// WriterCount returns the number of writer records.
func (g *Graph) WriterCount() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return len(g.writers)
}

// Writers returns a copy of all writer records keyed by GID.
func (g *Graph) Writers() map[GID]EntityInfo {
	g.mu.Lock()
	defer g.mu.Unlock()
	return copyEntities(g.writers)
}

// AddReader inserts a reader record, failing when the GID already exists.
//
// The info must carry demangled names plus translated QoS plus a parsed
// type hash. On success the generation increments and the hook fires
// outside the lock.
func (g *Graph) AddReader(gid GID, info EntityInfo) error {
	return addGraphRecord(g, g.readers, gid, info, validateEntityInfo, "add reader")
}

// UpsertReader adds or replaces a reader record.
//
// Identical content is a no op with no generation increment and no hook
// fire. Changed or new content increments the generation and fires the
// hook outside the lock.
func (g *Graph) UpsertReader(gid GID, info EntityInfo) error {
	return upsertGraphRecord(g, g.readers, gid, info, validateEntityInfo, entitiesEqual, "upsert reader")
}

// RemoveReader deletes a reader record by GID.
//
// It reports false when the GID is absent with no generation increment.
// On removal the generation increments and the hook fires outside the lock.
func (g *Graph) RemoveReader(gid GID) bool {
	g.mu.Lock()
	if _, seen := g.readers[gid]; !seen {
		g.mu.Unlock()
		return false
	}
	delete(g.readers, gid)
	gen, hook := g.bumpLocked()
	g.mu.Unlock()
	fireHook(hook, gen)
	return true
}

// Reader returns a copy of one reader record by GID.
func (g *Graph) Reader(gid GID) (EntityInfo, bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	info, ok := g.readers[gid]
	return info, ok
}

// ReaderCount returns the number of reader records.
func (g *Graph) ReaderCount() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return len(g.readers)
}

// Readers returns a copy of all reader records keyed by GID.
func (g *Graph) Readers() map[GID]EntityInfo {
	g.mu.Lock()
	defer g.mu.Unlock()
	return copyEntities(g.readers)
}

// AddService inserts a service record, failing when the GID already exists.
//
// The info must carry a demangled service name plus demangled service type
// plus the service type hash only. On success the generation increments
// and the hook fires outside the lock.
func (g *Graph) AddService(gid GID, info ServiceInfo) error {
	return addGraphRecord(g, g.services, gid, info, validateServiceInfo, "add service")
}

// UpsertService adds or replaces a service record.
//
// Identical content is a no op with no generation increment and no hook
// fire. Changed or new content increments the generation and fires the
// hook outside the lock.
func (g *Graph) UpsertService(gid GID, info ServiceInfo) error {
	return upsertGraphRecord(g, g.services, gid, info, validateServiceInfo, servicesEqual, "upsert service")
}

// RemoveService deletes a service record by GID.
//
// It reports false when the GID is absent with no generation increment.
// On removal the generation increments and the hook fires outside the lock.
func (g *Graph) RemoveService(gid GID) bool {
	g.mu.Lock()
	if _, seen := g.services[gid]; !seen {
		g.mu.Unlock()
		return false
	}
	delete(g.services, gid)
	gen, hook := g.bumpLocked()
	g.mu.Unlock()
	fireHook(hook, gen)
	return true
}

// Service returns a copy of one service record by GID.
func (g *Graph) Service(gid GID) (ServiceInfo, bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	info, ok := g.services[gid]
	return info, ok
}

// ServiceCount returns the number of service records.
func (g *Graph) ServiceCount() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return len(g.services)
}

// Services returns a copy of all service records keyed by GID.
func (g *Graph) Services() map[GID]ServiceInfo {
	g.mu.Lock()
	defer g.mu.Unlock()
	return copyServices(g.services)
}

// AddParticipant inserts a participant record, failing when it exists.
//
// The info must carry an enclave string plus node identity plus the node
// entity sequence. On success the generation increments and the hook fires
// outside the lock.
func (g *Graph) AddParticipant(gid GID, info ParticipantInfo) error {
	return addGraphRecord(g, g.participants, gid, info, validateParticipantInfo, "add participant")
}

// UpsertParticipant adds or replaces a participant record.
//
// Identical content is a no op with no generation increment and no hook
// fire. A stale node sequence below the stored sequence is ignored with
// no increment, so a late duplicate never overwrites newer state.
// Changed or new content increments the generation and fires the
// hook outside the lock.
func (g *Graph) UpsertParticipant(gid GID, info ParticipantInfo) error {
	err := validateParticipantInfo(info)
	if err != nil {
		return fmt.Errorf("core: upsert participant %s: %w", gid.String(), err)
	}
	g.mu.Lock()
	existing, seen := g.participants[gid]
	if seen && info.Sequence < existing.Sequence {
		g.mu.Unlock()
		return nil
	}
	if seen && participantsEqual(existing, info) {
		g.mu.Unlock()
		return nil
	}
	g.participants[gid] = info
	gen, hook := g.bumpLocked()
	g.mu.Unlock()
	fireHook(hook, gen)
	return nil
}

// RemoveParticipant deletes a participant record by GID.
//
// It reports false when the GID is absent with no generation increment.
// On removal the generation increments and the hook fires outside the lock.
// Removal also clears liveliness so a later rejoin starts from a clean
// sequence. Endpoints owned by the participant stay retained until their
// own explicit remove, so a late joiner still replays them.
func (g *Graph) RemoveParticipant(gid GID) bool {
	g.mu.Lock()
	if _, seen := g.participants[gid]; !seen {
		g.mu.Unlock()
		return false
	}
	delete(g.participants, gid)
	delete(g.liveness, gid)
	gen, hook := g.bumpLocked()
	g.mu.Unlock()
	fireHook(hook, gen)
	return true
}

// Participant returns a copy of one participant record by GID.
func (g *Graph) Participant(gid GID) (ParticipantInfo, bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	info, ok := g.participants[gid]
	return info, ok
}

// ParticipantCount returns the number of participant records.
func (g *Graph) ParticipantCount() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return len(g.participants)
}

// Participants returns a copy of all participant records keyed by GID.
func (g *Graph) Participants() map[GID]ParticipantInfo {
	g.mu.Lock()
	defer g.mu.Unlock()
	return copyParticipants(g.participants)
}

// DemangleTopicName converts a DDS topic name to its ROS form.
//
// It accepts rt, rq, and rr prefixed names plus bare names. Prefixed
// requests require the Request suffix and prefixed replies require the
// Reply suffix. Bare names use the same suffix rule. The result is the
// demangled ROS name such as /chatter. Transports perform the same mapping
// at the border; this helper lets core ingestion validate the same shape
// without importing transport packages.
func DemangleTopicName(ddsTopic string) (string, error) {
	trimmed := strings.TrimSpace(ddsTopic)
	err := validateDDSTopicName(trimmed, ddsTopic)
	if err != nil {
		return "", fmt.Errorf("core: demangle topic: %w", err)
	}
	if ros, handled, err := demanglePrefixedTopic(trimmed, ddsTopic); err != nil || handled {
		return ros, err
	}
	return demangleBareTopic(trimmed), nil
}

// DemangleTypeName converts a DDS type name to its IDL form.
//
// It requires exactly one ::dds_:: marker, strips the trailing underscore,
// and maps :: back to __. The result is the demangled IDL name such as
// std_msgs__msg__String with service Request and Response suffixes
// preserved. Transports perform the same mapping at the border; this
// helper lets core ingestion validate the same shape without importing
// transport packages.
func DemangleTypeName(ddsName string) (string, error) {
	const marker = "::dds_::"
	if strings.Count(ddsName, marker) != 1 {
		return "", fmt.Errorf("core: type name %q must contain one %q marker", ddsName, marker)
	}
	idx := strings.LastIndex(ddsName, marker)
	left := ddsName[:idx]
	right := ddsName[idx+len(marker):]
	if !strings.HasSuffix(right, "_") {
		return "", fmt.Errorf("core: type name %q misses trailing underscore", ddsName)
	}
	right = strings.TrimSuffix(right, "_")
	if strings.TrimSpace(left) == "" || strings.TrimSpace(right) == "" {
		return "", fmt.Errorf("core: type name %q has an empty segment", ddsName)
	}
	if strings.Contains(right, ":") {
		return "", fmt.Errorf("core: type name %q has unexpected scope in %q", ddsName, right)
	}
	err := validateIdentifier(right)
	if err != nil {
		return "", fmt.Errorf("core: type name %q: %w", ddsName, err)
	}
	for part := range strings.SplitSeq(left, "::") {
		if part == "" {
			return "", fmt.Errorf("core: type name %q has an empty namespace segment", ddsName)
		}
		err := validateIdentifier(part)
		if err != nil {
			return "", fmt.Errorf("core: type name %q: %w", ddsName, err)
		}
	}
	ns := strings.Join(strings.Split(left, "::"), "__")
	return ns + "__" + right, nil
}

// demanglePrefixedTopic maps prefixed DDS names to ROS form.
//
// It reports handled false for bare names without a known prefix.
func demanglePrefixedTopic(trimmed, raw string) (string, bool, error) {
	prefix, remainder, found := strings.Cut(trimmed, "/")
	if !found {
		return "", false, nil
	}
	switch prefix {
	case "rt":
		if remainder == "" {
			return "", true, fmt.Errorf("core: dds topic name %q has an empty remainder", raw)
		}
		if strings.Contains(remainder, "//") {
			return "", true, fmt.Errorf("core: dds topic name %q has an empty segment", raw)
		}
		return "/" + remainder, true, nil
	case "rq":
		if !strings.HasSuffix(remainder, "Request") {
			return "", true, fmt.Errorf("core: request topic %q misses %q suffix", raw, "Request")
		}
		base := strings.TrimSuffix(remainder, "Request")
		if base == "" {
			return "", true, fmt.Errorf("core: request topic %q has an empty base", raw)
		}
		return "/" + base, true, nil
	case "rr":
		if !strings.HasSuffix(remainder, "Reply") {
			return "", true, fmt.Errorf("core: reply topic %q misses %q suffix", raw, "Reply")
		}
		base := strings.TrimSuffix(remainder, "Reply")
		if base == "" {
			return "", true, fmt.Errorf("core: reply topic %q has an empty base", raw)
		}
		return "/" + base, true, nil
	default:
		return "", false, nil
	}
}

// demangleBareTopic maps a prefix free DDS name back to ROS form.
//
// A trailing Request marks a request and a trailing Reply marks a reply,
// otherwise the name is a normal topic. The exact names Request and Reply
// alone map to /Request and /Reply as normal topics.
func demangleBareTopic(trimmed string) string {
	if strings.HasSuffix(trimmed, "Request") && trimmed != "Request" {
		base := strings.TrimSuffix(trimmed, "Request")
		if base != "" && !strings.HasSuffix(base, "/") {
			return "/" + base
		}
	}
	if strings.HasSuffix(trimmed, "Reply") && trimmed != "Reply" {
		base := strings.TrimSuffix(trimmed, "Reply")
		if base != "" && !strings.HasSuffix(base, "/") {
			return "/" + base
		}
	}
	return "/" + trimmed
}

// validateDDSTopicName rejects malformed DDS topic names.
func validateDDSTopicName(trimmed, raw string) error {
	if trimmed == "" {
		return errors.New("core: dds topic name is empty") //nolint:wrapcheck // new error, nothing to wrap
	}
	if strings.HasPrefix(trimmed, "/") {
		return fmt.Errorf("core: dds topic name %q must not start with slash", raw)
	}
	if strings.HasSuffix(trimmed, "/") {
		return fmt.Errorf("core: dds topic name %q has a trailing slash", raw)
	}
	if strings.Contains(trimmed, "//") {
		return fmt.Errorf("core: dds topic name %q has an empty segment", raw)
	}
	if strings.ContainsAny(trimmed, " \t\n\r") {
		return fmt.Errorf("core: dds topic name %q has whitespace", raw)
	}
	return nil
}

// validateIdentifier reports whether s is a C style identifier.
func validateIdentifier(s string) error {
	if s == "" {
		return errors.New("core: identifier is empty") //nolint:wrapcheck // new error, nothing to wrap
	}
	for i, r := range s {
		if r == '_' || (r >= 'A' && r <= 'Z') || (r >= 'a' && r <= 'z') {
			continue
		}
		if i > 0 && r >= '0' && r <= '9' {
			continue
		}
		return fmt.Errorf("core: identifier %q has invalid character %q", s, string(r))
	}
	return nil
}

// validateEntityInfo rejects wire form names and malformed hashes.
//
// TopicName must be the demangled ROS form starting with slash. TopicType
// must be the demangled IDL form with double underscore separators.
// TypeHash must be empty for the zero hash or RIHS01_ plus 64 lowercase
// hex. QoS passes through without validation so exotic remote profiles
// stay visible in the index instead of being dropped.
func validateEntityInfo(info EntityInfo) error {
	err := validateROSTopicName(info.TopicName)
	if err != nil {
		return fmt.Errorf("core: entity topic: %w", err)
	}
	err = validateIDLName(info.TopicType)
	if err != nil {
		return fmt.Errorf("core: entity type: %w", err)
	}
	err = validateTypeHash(info.TypeHash)
	if err != nil {
		return fmt.Errorf("core: entity type hash: %w", err)
	}
	return nil
}

// validateServiceInfo rejects wire form names and malformed hashes.
//
// ServiceName must be the demangled ROS form. ServiceType must be the
// demangled IDL base name. ServiceTypeHash is the only hash stored for
// services: empty for the zero hash or RIHS01_ plus 64 lowercase hex.
func validateServiceInfo(info ServiceInfo) error {
	err := validateROSTopicName(info.ServiceName)
	if err != nil {
		return fmt.Errorf("core: service name: %w", err)
	}
	err = validateIDLName(info.ServiceType)
	if err != nil {
		return fmt.Errorf("core: service type: %w", err)
	}
	err = validateTypeHash(info.ServiceTypeHash)
	if err != nil {
		return fmt.Errorf("core: service type hash: %w", err)
	}
	return nil
}

// validateParticipantInfo rejects empty enclaves and empty node identity.
func validateParticipantInfo(info ParticipantInfo) error {
	if strings.TrimSpace(info.Enclave) == "" {
		return errors.New("core: participant enclave is empty") //nolint:wrapcheck // new error, nothing to wrap
	}
	if !strings.HasPrefix(info.Enclave, "/") {
		return fmt.Errorf("core: participant enclave %q must start with slash", info.Enclave)
	}
	if strings.TrimSpace(info.NodeName) == "" {
		return errors.New("core: participant node name is empty") //nolint:wrapcheck // new error, nothing to wrap
	}
	if strings.TrimSpace(info.NodeNamespace) == "" {
		return errors.New("core: participant node namespace is empty") //nolint:wrapcheck // new error, nothing to wrap
	}
	if !strings.HasPrefix(info.NodeNamespace, "/") {
		return fmt.Errorf("core: participant node namespace %q must start with slash", info.NodeNamespace)
	}
	return nil
}

// validateROSTopicName rejects names that are not demangled ROS form.
func validateROSTopicName(name string) error {
	trimmed := strings.TrimSpace(name)
	if trimmed == "" {
		return errors.New("core: topic name is empty") //nolint:wrapcheck // new error, nothing to wrap
	}
	if !strings.HasPrefix(trimmed, "/") {
		return fmt.Errorf("core: topic name %q must start with slash (demangled ROS form)", name)
	}
	if strings.Contains(trimmed, "//") {
		return fmt.Errorf("core: topic name %q has an empty segment", name)
	}
	if strings.HasSuffix(trimmed, "/") && trimmed != "/" {
		return fmt.Errorf("core: topic name %q has a trailing slash", name)
	}
	if strings.ContainsAny(trimmed, " \t\n\r") {
		return fmt.Errorf("core: topic name %q has whitespace", name)
	}
	for seg := range strings.SplitSeq(strings.Trim(trimmed, "/"), "/") {
		if seg == "" && trimmed != "/" {
			return fmt.Errorf("core: topic name %q has an empty segment", name)
		}
	}
	return nil
}

// validateIDLName rejects names that are not demangled IDL form.
func validateIDLName(name string) error {
	parts := strings.Split(name, "__")
	if len(parts) < 3 {
		return fmt.Errorf("core: idl name %q needs at least pkg__group__Name", name)
	}
	for _, part := range parts {
		if strings.TrimSpace(part) == "" {
			return fmt.Errorf("core: idl name %q has an empty segment", name)
		}
	}
	return nil
}

// validateTypeHash accepts empty for the zero hash or RIHS01_ plus hex.
func validateTypeHash(hash string) error {
	if hash == "" {
		return nil
	}
	const prefix = "RIHS01_"
	if !strings.HasPrefix(hash, prefix) {
		return fmt.Errorf("core: type hash %q must start with %q", hash, prefix)
	}
	hexPart := strings.TrimPrefix(hash, prefix)
	if len(hexPart) != 64 {
		return fmt.Errorf("core: type hash %q must carry 64 hex digits", hash)
	}
	for _, r := range hexPart {
		isDigit := r >= '0' && r <= '9'
		isLower := r >= 'a' && r <= 'f'
		if !isDigit && !isLower {
			return fmt.Errorf("core: type hash %q must be lowercase hex", hash)
		}
	}
	if len(hash) != 71 {
		return fmt.Errorf("core: type hash %q must be 71 chars", hash)
	}
	return nil
}

// entitiesEqual reports whether two entity records match exactly.
func entitiesEqual(a, b EntityInfo) bool {
	return a == b
}

// servicesEqual reports whether two service records match exactly.
func servicesEqual(a, b ServiceInfo) bool {
	return a == b
}

// participantsEqual reports whether two participant records match exactly.
func participantsEqual(a, b ParticipantInfo) bool {
	return a == b
}

// copyEntities returns a shallow copy of one entity map.
//
// QoSProfile holds only values so a shallow copy is sufficient. Callers
// mutating the returned map never affect index state.
func copyEntities(src map[GID]EntityInfo) map[GID]EntityInfo {
	out := make(map[GID]EntityInfo, len(src))
	maps.Copy(out, src)
	return out
}

// copyServices returns a shallow copy of one service map.
func copyServices(src map[GID]ServiceInfo) map[GID]ServiceInfo {
	out := make(map[GID]ServiceInfo, len(src))
	maps.Copy(out, src)
	return out
}

// copyParticipants returns a shallow copy of one participant map.
func copyParticipants(src map[GID]ParticipantInfo) map[GID]ParticipantInfo {
	out := make(map[GID]ParticipantInfo, len(src))
	maps.Copy(out, src)
	return out
}

// ReportParticipantAlive records one SPDP alive announcement.
//
// info carries demangled names plus the node entity sequence. seq is the
// discovery sequence and must be non-zero. nowNanos plus leaseNanos set
// the lease expiry. A stale or duplicate seq at or below the stored seq
// is ignored with no increment and no hook. A newer seq stores info plus
// new expiry and bumps once. When the node sequence in info is older than
// stored info, the stored info is kept while the lease still refreshes,
// so a reordered entity snapshot never downgrades newer state. Endpoint
// maps stay retained for late joiners and are never swept.
func (g *Graph) ReportParticipantAlive(gid GID, info ParticipantInfo, seq uint64, nowNanos, leaseNanos int64) error {
	err := validateParticipantInfo(info)
	if err != nil {
		return fmt.Errorf("core: report alive %s: %w", gid.String(), err)
	}
	if gid.IsZero() {
		return errors.New("core: report alive with zero gid") //nolint:wrapcheck // new error, nothing to wrap
	}
	if seq == 0 {
		return fmt.Errorf("core: report alive %s with zero seq", gid.String())
	}
	if nowNanos < 0 {
		return fmt.Errorf("core: report alive %s with negative now %d", gid.String(), nowNanos)
	}
	if leaseNanos <= 0 {
		return fmt.Errorf("core: report alive %s with non positive lease %d", gid.String(), leaseNanos)
	}
	expiry := nowNanos + leaseNanos
	if expiry < nowNanos {
		return fmt.Errorf("core: report alive %s expiry overflow", gid.String())
	}
	g.mu.Lock()
	if live, seen := g.liveness[gid]; seen && seq <= live.seq {
		g.mu.Unlock()
		return nil
	}
	stored, hasInfo := g.participants[gid]
	effective := info
	if hasInfo && info.Sequence < stored.Sequence {
		effective = stored
	}
	g.participants[gid] = effective
	g.liveness[gid] = participantLiveness{seq: seq, expiryNanos: expiry}
	gen, hook := g.bumpLocked()
	g.mu.Unlock()
	fireHook(hook, gen)
	return nil
}

// ReportParticipantDispose records one SPDP dispose announcement.
//
// seq must be newer than the stored seq; stale or duplicate disposes are
// ignored with no increment. A missing entry reports false with no
// increment. An entry without liveliness is removed explicitly. On
// removal the generation increments once and the hook fires. Endpoints
// stay retained until their own explicit remove.
func (g *Graph) ReportParticipantDispose(gid GID, seq uint64) bool {
	if gid.IsZero() {
		return false
	}
	if seq == 0 {
		return false
	}
	g.mu.Lock()
	_, hasInfo := g.participants[gid]
	live, hasLive := g.liveness[gid]
	if !hasInfo && !hasLive {
		g.mu.Unlock()
		return false
	}
	if hasLive && seq <= live.seq {
		g.mu.Unlock()
		return false
	}
	delete(g.participants, gid)
	delete(g.liveness, gid)
	gen, hook := g.bumpLocked()
	g.mu.Unlock()
	fireHook(hook, gen)
	return true
}

// SweepExpired removes participants whose lease expired.
//
// An entry expires when nowNanos reaches its expiry. Entries without
// liveliness never expire and stay until explicit remove. When no entry
// expires there is no increment. When at least one expires the generation
// increments once and the hook fires once. It returns the removed count.
// Endpoints stay retained; only participants sweep.
func (g *Graph) SweepExpired(nowNanos int64) int {
	g.mu.Lock()
	var expired []GID
	for gid, live := range g.liveness {
		if nowNanos < live.expiryNanos {
			continue
		}
		expired = append(expired, gid)
	}
	if len(expired) == 0 {
		g.mu.Unlock()
		return 0
	}
	for _, gid := range expired {
		delete(g.participants, gid)
		delete(g.liveness, gid)
	}
	gen, hook := g.bumpLocked()
	g.mu.Unlock()
	fireHook(hook, gen)
	return len(expired)
}

// ParticipantExpiry returns the lease expiry for one participant.
//
// It reports false when the participant has no liveliness record, which
// includes locally added participants without an alive report. Every
// method locks the mutex.
func (g *Graph) ParticipantExpiry(gid GID) (int64, bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	live, ok := g.liveness[gid]
	if !ok {
		return 0, false
	}
	return live.expiryNanos, true
}

// ParticipantSequence returns the last accepted discovery sequence.
//
// It reports false when no liveliness record exists. Local adds without
// an alive report have no discovery sequence.
func (g *Graph) ParticipantSequence(gid GID) (uint64, bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	live, ok := g.liveness[gid]
	if !ok {
		return 0, false
	}
	return live.seq, true
}

// ReplayRetained replays retained announcements to a late subscriber.
//
// It fires the change hook with the current generation without
// incrementing, so a subscriber that joined after the last mutation still
// observes the retained writers plus readers plus participants. A nil
// hook is a no op. It returns the current generation.
func (g *Graph) ReplayRetained() uint64 {
	g.mu.Lock()
	gen := g.generation
	hook := g.onChange
	g.mu.Unlock()
	fireHook(hook, gen)
	return gen
}

// bumpLocked increments the generation and captures the hook.
//
// The caller holds the index lock. The returned hook runs outside the lock
// through fireHook.
func (g *Graph) bumpLocked() (uint64, func(uint64)) {
	g.generation++
	return g.generation, g.onChange
}

// fireHook runs the change hook outside the index lock.
//
// A nil hook is a no op. The hook receives the new generation.
func fireHook(hook func(uint64), gen uint64) {
	if hook == nil {
		return
	}
	hook(gen)
}
