// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

package mqtt

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/podomy/weld/internal/transports"
)

// Retained discovery for the MQTT backend.
//
// Every endpoint announcement is a retained message on its own topic,
// so a late joiner learns the full index from the broker without any
// multicast: prefix/domain/sdp/<participant>/<kind>/<stripped> with a
// JSON body naming the type and hash. Disposals publish a retained
// tombstone in the same place. The local cache feeds HasRemoteEndpoint
// for CLI parity with the DDS backend. Discovery never touches the
// TakeBytes path; the receive loops route sdp topics here.

// endpointRecord is one retained announcement body.
type endpointRecord struct {
	// Type is the ROS IDL name such as std_msgs__msg__String.
	Type string `json:"type"`
	// TypeHash is the RIHS01 hash, empty when unknown.
	TypeHash string `json:"type_hash,omitempty"`
	// Alive is false for a dispose tombstone.
	Alive bool `json:"alive"`
	// Generation orders announcements from one writer: a greater
	// generation replaces, anything else is dropped as stale or
	// duplicate. Missing (zero) means a legacy writer without
	// generations; zero only wins an empty slot.
	Generation uint64 `json:"generation,omitempty"`
}

// endpointKey names one announced endpoint.
type endpointKey struct {
	// participant is the announcing participant.
	participant string
	// kind is the rt, rq, or rr leg served.
	kind Kind
	// rosTopic is the fully qualified ROS name.
	rosTopic string
}

// endpointCache stores the newest record per endpoint.
type endpointCache struct {
	// mu guards records.
	mu sync.Mutex
	// records maps endpoint keys to their newest record.
	records map[endpointKey]endpointRecord
	// installs counts effective installs for observability. Dropped
	// stale or duplicate records do not tick it.
	installs uint64
}

// newEndpointCache returns an empty discovery cache.
func newEndpointCache() *endpointCache {
	return &endpointCache{records: make(map[endpointKey]endpointRecord)}
}

// install stores one record when it is newer than the stored one.
//
// The first sighting always stores. Otherwise only a greater
// generation replaces: a delayed duplicate or stale retract carries
// an older or equal generation and is dropped, so it can never hide
// a newer announce. This is the greater-wins rule proven in
// proof/liveliness.tla (Announce/Retract).
func (c *endpointCache) install(key endpointKey, record endpointRecord) {
	c.mu.Lock()
	defer c.mu.Unlock()

	stored, ok := c.records[key]
	if !ok || record.Generation > stored.Generation {
		c.records[key] = record
		c.installs++
	}
}

// hasLive reports whether a live announcement names one topic plus kind.
func (c *endpointCache) hasLive(rosTopic string, kind Kind) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	for key, record := range c.records {
		if record.Alive && key.rosTopic == rosTopic && key.kind == kind {
			return true
		}
	}

	return false
}

// installCount returns the effective install count.
func (c *endpointCache) installCount() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.installs
}

// noteDiscoveryMessage routes one connection sample to the cache.
//
// It reports true when the topic parses as a discovery announcement,
// in which case the caller must skip the data path. Announcements for
// foreign domains are acknowledged and skipped. Malformed bodies are
// skipped without poisoning the cache.
func (b *Backend) noteDiscoveryMessage(wire ConnMessage) bool {
	b.mu.Lock()
	prefix := b.prefix
	domain := b.domain
	b.mu.Unlock()

	wireDomain, participant, kind, rosTopic, err := DecodeDiscoveryTopic(prefix, wire.Topic)
	if err != nil {
		// A discovery-leg topic that fails discovery decode is
		// malformed discovery, not data: skip it instead of feeding
		// the inbox, where TakeBytes would surface it as a data
		// sample and TakeService would park it where no role can
		// ever match it. Non-discovery topics still return false.
		if _, wireKind, _, kindErr := DecodeTopic(prefix, wire.Topic); kindErr == nil && wireKind == KindDiscovery {
			return true
		}

		return false
	}

	if wireDomain != domain {
		return true
	}

	var record endpointRecord
	err = json.Unmarshal(wire.Payload, &record)
	if err != nil {
		return true
	}

	b.discovery.install(endpointKey{participant: participant, kind: kind, rosTopic: rosTopic}, record)

	return true
}

// AnnounceEndpoint publishes one retained endpoint announcement.
//
// kind selects the rt, rq, or rr leg the endpoint serves. typeName and
// typeHash name the IDL type; empty hash means unknown. The payload is
// retained so late joiners observe it on subscribe without any query.
// The record carries the next discovery sequence number so delayed
// duplicates can never overwrite newer records. Requires a prior
// JoinDiscovery; after Close it reports ErrBackendClosed.
func (b *Backend) AnnounceEndpoint(ctx context.Context, participant string, kind Kind, rosTopic, typeName, typeHash string) error {
	err := ctx.Err()
	if err != nil {
		return fmt.Errorf("mqtt: announce context: %w", err)
	}

	if err := b.checkReady(); err != nil {
		return err
	}

	b.mu.Lock()
	closed := b.closed
	joined := b.joined
	b.mu.Unlock()

	if closed {
		return fmt.Errorf("mqtt: announce after close: %w", transports.ErrBackendClosed)
	}

	if !joined {
		return fmt.Errorf("mqtt: announce before join: %w", transports.ErrNotJoined)
	}

	b.mu.Lock()
	prefix := b.prefix
	domain := b.domain
	qos := b.qos
	b.discSeq++
	generation := b.discSeq
	b.mu.Unlock()

	topic, err := EncodeDiscoveryTopic(prefix, domain, participant, kind, rosTopic)
	if err != nil {
		return err
	}

	body, err := json.Marshal(endpointRecord{Type: typeName, TypeHash: typeHash, Alive: true, Generation: generation}) //nolint:errchkjson // record holds only strings, a bool, and a counter; encoding never fails
	if err != nil {
		return fmt.Errorf("mqtt: announce marshal: %w", err)
	}

	err = b.conn.Publish(ctx, topic, qos, true, body, Properties{})
	if err != nil {
		return fmt.Errorf("mqtt: announce publish: %w", err)
	}

	b.discovery.install(endpointKey{participant: participant, kind: kind, rosTopic: rosTopic}, endpointRecord{Type: typeName, TypeHash: typeHash, Alive: true, Generation: generation})

	return nil
}

// RetractEndpoint publishes one retained dispose tombstone.
//
// Requires a prior JoinDiscovery; after Close it reports ErrBackendClosed.
func (b *Backend) RetractEndpoint(ctx context.Context, participant string, kind Kind, rosTopic string) error {
	err := ctx.Err()
	if err != nil {
		return fmt.Errorf("mqtt: retract context: %w", err)
	}

	if err := b.checkReady(); err != nil {
		return err
	}

	b.mu.Lock()
	closed := b.closed
	joined := b.joined
	b.mu.Unlock()

	if closed {
		return fmt.Errorf("mqtt: retract after close: %w", transports.ErrBackendClosed)
	}

	if !joined {
		return fmt.Errorf("mqtt: retract before join: %w", transports.ErrNotJoined)
	}

	b.mu.Lock()
	prefix := b.prefix
	domain := b.domain
	qos := b.qos
	b.discSeq++
	generation := b.discSeq
	b.mu.Unlock()

	topic, err := EncodeDiscoveryTopic(prefix, domain, participant, kind, rosTopic)
	if err != nil {
		return err
	}

	body, err := json.Marshal(endpointRecord{Alive: false, Generation: generation}) //nolint:errchkjson // tombstone holds a bool plus a counter; encoding never fails
	if err != nil {
		return fmt.Errorf("mqtt: retract marshal: %w", err)
	}

	err = b.conn.Publish(ctx, topic, qos, true, body, Properties{})
	if err != nil {
		return fmt.Errorf("mqtt: retract publish: %w", err)
	}

	b.discovery.install(endpointKey{participant: participant, kind: kind, rosTopic: rosTopic}, endpointRecord{Alive: false, Generation: generation})

	return nil
}

// HasRemoteEndpoint reports whether a live announcement names the topic.
//
// Only the data leg is consulted; service legs announce separately
// and are invisible here. (The DDS backend matches any leg, so this
// is a documented single-leg limitation of the MQTT backend.)
func (b *Backend) HasRemoteEndpoint(rosTopic string) bool {
	if b == nil || b.discovery == nil {
		return false
	}

	return b.discovery.hasLive(rosTopic, KindData)
}

// DiscoveryGeneration returns the local effective-install count.
func (b *Backend) DiscoveryGeneration() uint64 {
	if b == nil || b.discovery == nil {
		return 0
	}

	return b.discovery.installCount()
}

// Reconnects returns the count of successful redials after drops.
//
// In-memory connections never redial and report zero. Live sessions
// bump the count on every resume, so gates assert at least one after
// a broker restart.
func (b *Backend) Reconnects() uint64 {
	if b == nil {
		return 0
	}

	type redialer interface {
		Reconnects() uint64
	}

	b.mu.Lock()
	conn := b.conn
	b.mu.Unlock()

	if redialing, ok := conn.(redialer); ok {
		return redialing.Reconnects()
	}

	return 0
}
