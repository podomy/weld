// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

package mqtt

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"maps"
	"sync"
	"time"

	"github.com/podomy/weld/internal/transports"
)

// _ asserts Backend implements transports.Backend.
var _ transports.Backend = (*Backend)(nil)

// ConnMessage is one wire sample exchanged with the broker.
//
// Publish copies its payload on entry in every Conn, so callers keep
// their slice. Received payloads must be treated as read-only:
// netConn hands out its queued copy (the backend copies again on
// decode and never mutates wire bytes) while memoryConn copies once
// more on exit. Extra user-property maps are never deep-copied on
// Receive; treat them as read-only as they may share storage.
type ConnMessage struct {
	// Topic is the full MQTT topic such as weld/0/rt/chatter.
	Topic string
	// Payload holds the envelope bytes. Nil means empty.
	Payload []byte
	// QoS echoes the publish quality of service level.
	QoS byte
	// Retained echoes the publish retained flag.
	Retained bool
	// Props carries the MQTT v5 user properties such as the type hash.
	Props Properties
}

// Conn hides the real MQTT broker behind a small testable surface.
//
// Publish copies its payload and properties; the caller keeps its
// slice. Receive blocks until the next queued sample arrives or ctx
// ends, and returns ErrBackendClosed after Close. After Close every
// method fails with ErrBackendClosed. All methods are safe for
// concurrent use. Post-Close drain order differs by implementation:
// the live session drains queued samples first, the in-memory double
// discards them; Backend-level takes fail fast either way.
type Conn interface {
	// Publish sends one envelope to the named MQTT topic with properties.
	Publish(ctx context.Context, topic string, qos byte, retained bool, payload []byte, props Properties) error
	// Receive returns the next queued sample, blocking until one arrives.
	Receive(ctx context.Context) (ConnMessage, error)
	// Close releases the connection. It is idempotent.
	Close() error
}

// memoryConn is an in-memory Conn with channel style queue semantics.
//
// It copies payloads on entry and on exit so callers never alias queued
// bytes. Receive blocks on a condition variable until Publish, Close, or
// context cancel wakes it. All methods are safe for concurrent use.
//
// Limits of the test double, by design: no retained store (late joiners
// see only still-queued samples, never broker-replayed history), no QoS 2
// redelivery dedup (duplicates enqueue twice), no 1024 inflight cap, and
// full-queue eviction is silent (no drop counter). Close discards queued
// samples while the live session drains them first; raw-Conn consumers
// must not depend on post-Close drain order.
type memoryConn struct {
	// mu guards the queue and the closed flag.
	mu sync.Mutex
	// cond wakes blocked receivers on publish or close.
	cond *sync.Cond
	// queue holds envelopes in publish order.
	queue []ConnMessage
	// closed marks teardown. After Close all data calls fail.
	closed bool
}

// newMemoryConn returns an empty in-memory connection.
func newMemoryConn() *memoryConn {
	conn := &memoryConn{}
	conn.cond = sync.NewCond(&conn.mu)

	return conn
}

// Publish appends one envelope copy to the queue.
func (m *memoryConn) Publish(ctx context.Context, topic string, qos byte, retained bool, payload []byte, props Properties) error {
	err := ctx.Err()
	if err != nil {
		return fmt.Errorf("mqtt: publish context: %w", err)
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		return fmt.Errorf("mqtt: publish after close: %w", transports.ErrBackendClosed)
	}

	if qos > maxQoS {
		return fmt.Errorf("mqtt: qos %d above maximum %d", qos, maxQoS)
	}

	stored := ConnMessage{
		Topic:    topic,
		Payload:  copyBytes(payload),
		QoS:      qos,
		Retained: retained,
		Props:    cloneProps(props),
	}

	if len(m.queue) >= maxNetQueue {
		copy(m.queue, m.queue[1:])
		m.queue[len(m.queue)-1] = stored
		m.cond.Broadcast()

		return nil
	}

	m.queue = append(m.queue, stored)
	m.cond.Broadcast()

	return nil
}

// Receive returns the head envelope, blocking until one is available.
func (m *memoryConn) Receive(ctx context.Context) (ConnMessage, error) {
	err := ctx.Err()
	if err != nil {
		return ConnMessage{}, fmt.Errorf("mqtt: receive context: %w", err)
	}

	stop := context.AfterFunc(ctx, func() {
		m.mu.Lock()
		m.cond.Broadcast()
		m.mu.Unlock()
	})
	defer stop()

	m.mu.Lock()
	defer m.mu.Unlock()

	for {
		err = ctx.Err()
		if err != nil {
			return ConnMessage{}, fmt.Errorf("mqtt: receive context: %w", err)
		}

		if m.closed {
			return ConnMessage{}, fmt.Errorf("mqtt: receive after close: %w", transports.ErrBackendClosed)
		}

		if len(m.queue) > 0 {
			head := m.queue[0]
			m.queue = append(m.queue[:0], m.queue[1:]...)

			out := ConnMessage{
				Topic:    head.Topic,
				Payload:  copyBytes(head.Payload),
				QoS:      head.QoS,
				Retained: head.Retained,
				Props:    head.Props,
			}

			return out, nil
		}

		m.cond.Wait()
	}
}

// Close marks the connection closed and wakes all receivers.
func (m *memoryConn) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.closed = true
	m.cond.Broadcast()

	return nil
}

// inboxCap bounds pump delivery when takes fall behind.
//
// When the inbox is full the pump evicts the oldest sample and counts
// one drop, so a slow consumer keeps the freshest traffic and loss
// stays visible instead of growing without bound.
const inboxCap = 256

// maxLegPending bounds each cross-leg take buffer.
//
// TakeService parks data samples for TakeBytes to drain and parks the
// other service leg for later service takes; TakeBytes itself never
// parks, it returns whatever leg the inbox head carries. A consumer
// that only ever takes one leg would otherwise grow the parked buffer
// without bound. A full buffer evicts its oldest sample and counts one
// drop, matching the inbox policy.
const maxLegPending = 1024

// maxEnvelopeTypeLen bounds one IDL type name in the MQTT envelope.
const maxEnvelopeTypeLen = 1024

// maxEnvelopePayload bounds one user payload in the MQTT envelope,
// matching the DDS sample cap so a corrupt length cannot force a
// larger single allocation.
const maxEnvelopePayload = 32 << 20

// checkEnvelopeBounds rejects oversize type names and payloads.
func checkEnvelopeBounds(typeName string, payload []byte) error {
	if len(typeName) > maxEnvelopeTypeLen {
		return fmt.Errorf("mqtt: type name %d bytes above maximum %d", len(typeName), maxEnvelopeTypeLen)
	}

	if len(payload) > maxEnvelopePayload {
		return fmt.Errorf("mqtt: payload %d bytes above maximum %d", len(payload), maxEnvelopePayload)
	}

	return nil
}

// Backend implements transports.Backend over an MQTT connection.
// Construct it with New or NewWithConn; the zero value is not usable
// and its methods report that instead of panicking.
//
// PublishBytes encodes the ROS topic to MQTT form, wraps the type name and
// payload in a length prefixed envelope, and publishes through Conn with
// the caller type hash as a user property. TakeBytes receives the next
// data envelope, unwraps it, and decodes the MQTT topic back to a ROS
// name; request and reply legs are consumed by TakeService instead.
// Liveliness keeps a per participant generation watermark with the same
// ordering rules as the transports fake. Close is idempotent. All
// methods are safe for concurrent use.
type Backend struct {
	// mu guards joined, closed, and the liveliness watermarks.
	mu sync.Mutex
	// joined marks successful JoinDiscovery. Data calls need it.
	joined bool
	// closed marks teardown. After Close all calls fail except Close.
	closed bool
	// lastGen holds the newest generation per participant.
	lastGen map[string]uint64
	// lastAlive holds the newest alive flag per participant.
	lastAlive map[string]bool
	// discSeq is the backend-owned discovery sequence, bumped on
	// every local announce or retract so retained records carry
	// strictly increasing generations per writer.
	discSeq uint64
	// prefix is the normalized MQTT root.
	prefix string
	// domain is the resolved DDS domain id.
	domain uint32
	// qos is the publish quality of service level.
	qos byte
	// retained selects retained publishes.
	retained bool
	// conn carries the wire traffic. Memory by default.
	conn Conn
	// version selects the packet framing. Zero means Version5.
	version Version
	// warnedHashDrop marks the logged v3 hash limitation.
	warnedHashDrop bool
	// takeMu serializes the take paths over the shared inbox.
	takeMu sync.Mutex
	// dataPending buffers data samples taken by the service path.
	dataPending []transports.IncomingMessage
	// svcPending buffers service samples taken by the data path.
	svcPending []serviceSample
	// discovery caches retained endpoint announcements by participant.
	discovery *endpointCache
	// inbox carries wire samples from the pump to the take paths.
	// Discovery never lands here; the pump routes it to the cache.
	inbox chan ConnMessage
	// done closes on Close to unblock take paths waiting on the inbox.
	done chan struct{}
	// pumpRunning marks the started pump so joins stay idempotent.
	pumpRunning bool
	// dropped counts evicted samples when takes fall behind.
	dropped uint64
}

// New validates the config and returns a backend with in-memory transport.
func New(cfg MqttConfig) (*Backend, error) {
	conn := newMemoryConn()

	backend, err := NewWithConn(cfg, conn)
	if err != nil {
		return nil, err
	}

	return backend, nil
}

// NewWithConn validates the config and returns a backend over conn.
//
// The caller owns conn before the call and the backend owns it after. Nil
// conn is rejected. Future broker dialers inject their paho or mosquitto
// Conn here while tests inject the in-memory channel or a scripted stub.
func NewWithConn(cfg MqttConfig, conn Conn) (*Backend, error) {
	if conn == nil {
		return nil, errors.New("mqtt: conn is nil") //nolint:wrapcheck // new error, nothing to wrap
	}

	err := cfg.Validate()
	if err != nil {
		return nil, fmt.Errorf("mqtt: invalid config: %w", err)
	}

	domain, err := cfg.ResolveDomain()
	if err != nil {
		return nil, err
	}

	backend := &Backend{
		lastGen:   make(map[string]uint64),
		lastAlive: make(map[string]bool),
		prefix:    cfg.NormalizedPrefix(),
		domain:    domain,
		qos:       cfg.QoS,
		retained:  cfg.Retained,
		conn:      conn,
		version:   cfg.Version,
		discovery: newEndpointCache(),
		inbox:     make(chan ConnMessage, inboxCap),
		done:      make(chan struct{}),
	}

	return backend, nil
}

// checkReady reports whether the backend was built by a constructor.
//
// The zero value is not usable: New and NewWithConn wire the
// connection, inbox, discovery cache, and watermark maps. Failing
// loudly here beats a nil dereference three calls later.
func (b *Backend) checkReady() error {
	if b == nil || b.conn == nil || b.done == nil || b.discovery == nil || b.inbox == nil {
		return errors.New("mqtt: backend not initialized, use New") //nolint:wrapcheck // new error, nothing to wrap
	}

	return nil
}

// publishSnapshot carries one validated publish path snapshot.
type publishSnapshot struct {
	// prefix is the normalized MQTT root.
	prefix string
	// domain is the resolved DDS domain id.
	domain uint32
	// qos is the publish quality of service level.
	qos byte
	// retained selects retained publishes.
	retained bool
	// version selects the packet framing.
	version Version
}

// snapshotPublish validates one publish call and snapshots the path.
//
// It checks the context, the join state, and the envelope bounds,
// and logs the 3.1.1 hash drop once. Callers encode and send from
// the snapshot without holding the lock.
func (b *Backend) snapshotPublish(ctx context.Context, topicName, typeName string, payload []byte, typeHash string, op string) (publishSnapshot, error) {
	var empty publishSnapshot

	err := ctx.Err()
	if err != nil {
		return empty, fmt.Errorf("mqtt: %s context: %w", op, err)
	}

	if err := b.checkReady(); err != nil {
		return empty, err
	}

	b.mu.Lock()
	closed := b.closed
	joined := b.joined
	snap := publishSnapshot{
		prefix:   b.prefix,
		domain:   b.domain,
		qos:      b.qos,
		retained: b.retained,
		version:  b.version,
	}
	warned := b.warnedHashDrop
	if typeHash != "" && snap.version == Version311 && !warned {
		b.warnedHashDrop = true
	}
	b.mu.Unlock()

	if closed {
		return empty, fmt.Errorf("mqtt: %s after close: %w", op, transports.ErrBackendClosed)
	}

	if !joined {
		return empty, fmt.Errorf("mqtt: %s before join: %w", op, transports.ErrNotJoined)
	}

	if typeHash != "" && snap.version == Version311 && !warned {
		log.Printf("mqtt: %s %s drops type hash on 3.1.1 without properties", op, topicName)
	}

	err = checkEnvelopeBounds(typeName, payload)
	if err != nil {
		return empty, err
	}

	return snap, nil
}

// PublishBytes publishes one serialized message to the MQTT wire form.
//
// The leg selects the rt, rq, or rr topic; an unknown leg fails
// loudly. Requires a prior JoinDiscovery; after Close it reports
// ErrBackendClosed. Wrap errors preserve ctx.Err, ErrNotJoined, and
// ErrBackendClosed for errors.Is.
func (b *Backend) PublishBytes(ctx context.Context, msg transports.OutgoingMessage) error {
	snap, err := b.snapshotPublish(ctx, msg.TopicName, msg.TypeName, msg.Payload, msg.TypeHash, "publish")
	if err != nil {
		return err
	}

	kind, err := legToKind(msg.Leg)
	if err != nil {
		return fmt.Errorf("mqtt: publish leg: %w", err)
	}

	mqttTopic, err := EncodeTopic(snap.prefix, snap.domain, kind, msg.TopicName)
	if err != nil {
		return fmt.Errorf("mqtt: publish topic: %w", err)
	}

	envelope := encodeEnvelope(msg.TypeName, msg.Payload)
	props := Properties{TypeHash: msg.TypeHash}

	err = b.conn.Publish(ctx, mqttTopic, snap.qos, snap.retained, envelope, props)
	if err != nil {
		if errors.Is(err, transports.ErrBackendClosed) {
			return fmt.Errorf("mqtt: publish after close: %w", transports.ErrBackendClosed)
		}

		return fmt.Errorf("mqtt: publish: %w", err)
	}

	return nil
}

// takeSnapshot validates a take call and captures the routing snapshot.
//
// It runs under the take lock so a Close or cancel that landed while
// queued is caught before any sample moves. Returns the topic prefix
// and domain the take must use.
func (b *Backend) takeSnapshot(ctx context.Context, op string) (string, uint32, error) {
	if err := ctx.Err(); err != nil {
		return "", 0, fmt.Errorf("mqtt: %s context: %w", op, err)
	}

	if err := b.checkReady(); err != nil {
		return "", 0, err
	}

	b.mu.Lock()
	closed := b.closed
	joined := b.joined
	prefix := b.prefix
	domain := b.domain
	b.mu.Unlock()

	if closed {
		return "", 0, fmt.Errorf("mqtt: %s after close: %w", op, transports.ErrBackendClosed)
	}

	if !joined {
		return "", 0, fmt.Errorf("mqtt: %s before join: %w", op, transports.ErrNotJoined)
	}

	return prefix, domain, nil
}

// TakeBytes receives one sample of any leg from the MQTT wire form.
//
// Data, request, and reply legs all arrive here with IncomingMessage.Leg
// set, matching the DDS backend where service samples flow through the
// same take path. TakeService stays as a typed filter for callers that
// want one role only. Discovery announcements never reach the take
// path: the pump routes them to the endpoint cache, so HasRemoteEndpoint
// works without any take. Samples buffered by the other take path are
// drained first. The sender type hash arrives in user properties and
// populates IncomingMessage.TypeHash. Blocks until a sample arrives;
// ctx cancellation reports ctx.Err, Close reports ErrBackendClosed.
func (b *Backend) TakeBytes(ctx context.Context) (transports.IncomingMessage, error) {
	err := ctx.Err()
	if err != nil {
		return transports.IncomingMessage{}, fmt.Errorf("mqtt: take context: %w", err)
	}

	b.takeMu.Lock()
	defer b.takeMu.Unlock()

	prefix, domain, err := b.takeSnapshot(ctx, "take")
	if err != nil {
		return transports.IncomingMessage{}, err
	}

	for {
		if len(b.dataPending) > 0 {
			msg := b.dataPending[0]
			b.dataPending = append(b.dataPending[:0], b.dataPending[1:]...)

			b.mu.Lock()
			closedAgain := b.closed
			b.mu.Unlock()

			if closedAgain {
				b.dataPending = append([]transports.IncomingMessage{msg}, b.dataPending...)

				return transports.IncomingMessage{}, fmt.Errorf("mqtt: take after close: %w", transports.ErrBackendClosed)
			}

			return msg, nil
		}

		select {
		case <-ctx.Done():
			err := ctx.Err()

			return transports.IncomingMessage{}, fmt.Errorf("mqtt: take context: %w", err)
		case <-b.done:
			return transports.IncomingMessage{}, fmt.Errorf("mqtt: take after close: %w", transports.ErrBackendClosed)
		case wire := <-b.inbox:
			kind, svc, err := b.decodeWireMessage(prefix, domain, wire)
			if err != nil {
				// Already consumed from the inbox, so count the
				// discard: the caller sees the error, and Dropped
				// stays the record of every take-path loss.
				b.mu.Lock()
				b.dropped++
				b.mu.Unlock()

				return transports.IncomingMessage{}, fmt.Errorf("mqtt: take decode: %w", err)
			}

			return incomingFromService(svc, kind), nil
		}
	}
}

// pump routes connection samples to the cache or the take inbox.
//
// It runs from the first JoinDiscovery until Close. Discovery topics
// feed the endpoint cache so late joiners and graph queries work with
// no take. Data legs queue for the take paths; a full inbox evicts
// the oldest sample and counts one drop. The lifetime context detaches
// cancel from the join call so the pump outlives it; Close ends the
// pump by closing the connection.
func (b *Backend) pump(ctx context.Context) {
	for {
		wire, err := b.conn.Receive(ctx)
		if err != nil {
			if b.isClosed() {
				return
			}

			select {
			case <-ctx.Done():
				return
			case <-time.After(10 * time.Millisecond):
			}

			continue
		}

		b.stageWire(wire)
	}
}

// isClosed reports whether Close has run.
func (b *Backend) isClosed() bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.closed
}

// stageWire routes one connection sample to the cache or the inbox.
//
// Discovery topics feed the endpoint cache. Data legs queue for the
// take paths; a full inbox evicts the oldest sample and counts one
// drop so delivery stays fresh instead of growing without bound.
func (b *Backend) stageWire(wire ConnMessage) {
	if b.noteDiscoveryMessage(wire) {
		return
	}

	select {
	case b.inbox <- wire:
	default:
		select {
		case <-b.inbox:
		default:
		}

		select {
		case b.inbox <- wire:
		default:
		}

		b.mu.Lock()
		b.dropped++
		b.mu.Unlock()
	}
}

// Dropped returns the count of take-path losses.
//
// Every eviction path on the Backend counts here: a full take inbox,
// a full cross-leg buffer, and malformed samples discarded on decode
// (the caller still sees the decode error). Mid-resume overrun is
// counted separately on the live connection (netConn.Dropped) and is
// not merged here, so Backend.Dropped stays scoped to take-path loss.
// Nothing on the take path is lost without this counter moving.
func (b *Backend) Dropped() uint64 {
	if b == nil || b.inbox == nil {
		return 0
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	return b.dropped
}

// legToKind maps one contract leg to its MQTT wire kind.
func legToKind(leg transports.Leg) (Kind, error) {
	switch leg {
	case "", transports.LegData:
		return KindData, nil
	case transports.LegRequest:
		return KindRequest, nil
	case transports.LegReply:
		return KindReply, nil
	default:
		return "", fmt.Errorf("mqtt: unknown leg %q", string(leg))
	}
}

// kindToLeg maps one MQTT wire kind to its contract leg.
func kindToLeg(kind Kind) transports.Leg {
	switch kind {
	case KindRequest:
		return transports.LegRequest
	case KindReply:
		return transports.LegReply
	case KindData, KindDiscovery:
		return transports.LegData
	default:
		return transports.LegData
	}
}

// decodeWireMessage decodes one connection sample to its kind plus payload.
func (b *Backend) decodeWireMessage(prefix string, domain uint32, wire ConnMessage) (Kind, serviceSample, error) {
	var empty serviceSample

	wireDomain, kind, rosTopic, err := DecodeTopic(prefix, wire.Topic)
	if err != nil {
		return "", empty, err
	}

	if wireDomain != domain {
		return "", empty, fmt.Errorf("mqtt: topic %q domain %d outside %d", wire.Topic, wireDomain, domain)
	}

	typeName, payload, err := decodeEnvelope(wire.Payload)
	if err != nil {
		return "", empty, err
	}

	return kind, serviceSample{
		kind:     kind,
		rosTopic: rosTopic,
		typeName: typeName,
		payload:  payload,
		typeHash: wire.Props.TypeHash,
	}, nil
}

// incomingFromService converts one decoded sample to a take message.
func incomingFromService(svc serviceSample, kind Kind) transports.IncomingMessage {
	return transports.IncomingMessage{
		TopicName: svc.rosTopic,
		TypeName:  svc.typeName,
		Payload:   svc.payload,
		TypeHash:  svc.typeHash,
		Leg:       kindToLeg(kind),
	}
}

// subscriber registers topic filters with a live broker.
//
// memoryConn ignores it; netConn speaks SUBSCRIBE. Backend join
// asserts this interface so in-memory tests need no broker while live
// sessions subscribe before announcing.
type subscriber interface {
	Subscribe(ctx context.Context, filter string, qos byte) error
}

// unsubscriber removes topic filters from a live broker.
//
// memoryConn ignores it; netConn speaks UNSUBSCRIBE. Backend leave
// asserts this interface so teardown unsubscribes without failing
// in-memory sessions.
type unsubscriber interface {
	Unsubscribe(ctx context.Context, filter string) error
}

// JoinDiscovery marks the backend joined and subscribes live sessions.
//
// Live connections subscribe to the data wildcard plus the discovery
// wildcard before returning, so retained announcements arrive before
// the first take. In-memory connections need no subscription.
func (b *Backend) JoinDiscovery(ctx context.Context, _ transports.DiscoveryInfo) error {
	err := ctx.Err()
	if err != nil {
		return fmt.Errorf("mqtt: join discovery: %w", err)
	}

	if err := b.checkReady(); err != nil {
		return err
	}

	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()

		return fmt.Errorf("mqtt: join discovery after close: %w", transports.ErrBackendClosed)
	}

	prefix := b.prefix
	domain := b.domain
	qos := b.qos
	conn := b.conn
	b.mu.Unlock()

	sub, ok := conn.(subscriber)
	if ok {
		filters := append(dataFilters(prefix, domain), discoveryFilter(prefix, domain))

		for _, filter := range filters {
			err := sub.Subscribe(ctx, filter, qos)
			if err != nil {
				return fmt.Errorf("mqtt: join subscribe %q: %w", filter, err)
			}
		}
	}

	// Re-check under the lock: a Close that landed mid-subscribe must
	// win over marking joined, or takes would proceed on a dead backend.
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.closed {
		return fmt.Errorf("mqtt: join discovery after close: %w", transports.ErrBackendClosed)
	}

	b.joined = true

	if !b.pumpRunning {
		b.pumpRunning = true

		go b.pump(context.WithoutCancel(ctx))
	}

	return nil
}

// LeaveDiscovery unsubscribes the join filters from a live broker.
//
// In-memory connections need no unsubscription and report success.
// Live sessions drop the data plus discovery wildcards so the broker
// stops delivering after the backend is done with them.
func (b *Backend) LeaveDiscovery(ctx context.Context) error {
	err := ctx.Err()
	if err != nil {
		return fmt.Errorf("mqtt: leave discovery context: %w", err)
	}

	if err := b.checkReady(); err != nil {
		return err
	}

	b.mu.Lock()
	closed := b.closed
	joined := b.joined
	conn := b.conn
	prefix := b.prefix
	domain := b.domain
	b.mu.Unlock()

	if closed {
		return fmt.Errorf("mqtt: leave discovery after close: %w", transports.ErrBackendClosed)
	}

	if !joined {
		return fmt.Errorf("mqtt: leave discovery before join: %w", transports.ErrNotJoined)
	}

	sub, ok := conn.(unsubscriber)
	if !ok {
		return nil
	}

	filters := append(dataFilters(prefix, domain), discoveryFilter(prefix, domain))

	for _, filter := range filters {
		err := sub.Unsubscribe(ctx, filter)
		if err != nil {
			return fmt.Errorf("mqtt: leave unsubscribe %q: %w", filter, err)
		}
	}

	return nil
}

// dataFilters returns the rt, rq, and rr wildcards for one domain.
//
// Three filters keep discovery traffic on the sdp leg out of the data
// queue without any client side demultiplexing on a live broker; the
// in-memory connection ignores subscriptions, so there the pump still
// demultiplexes via noteDiscoveryMessage.
func dataFilters(prefix string, domain uint32) []string {
	base := NormalizePrefix(prefix) + "/" + formatDomain(domain) + "/"
	kinds := []Kind{KindData, KindRequest, KindReply}
	filters := make([]string, 0, len(kinds))

	for _, kind := range kinds {
		filters = append(filters, base+string(kind)+"/#")
	}

	return filters
}

// discoveryFilter returns the discovery wildcard for one domain.
func discoveryFilter(prefix string, domain uint32) string {
	return NormalizePrefix(prefix) + "/" + formatDomain(domain) + "/" + string(KindDiscovery) + "/#"
}

// ReportLiveliness enforces caller owned generation ordering per participant.
//
// Greater generations store and advance the watermark; the same
// generation with the same alive flag is a no-op; anything else is
// rejected with ErrStaleGeneration and changes nothing. Requires a
// prior JoinDiscovery; after Close it reports ErrBackendClosed.
// Participant names are caller owned and never validated.
func (b *Backend) ReportLiveliness(ctx context.Context, report transports.LivelinessReport) error {
	err := ctx.Err()
	if err != nil {
		return fmt.Errorf("mqtt: report context: %w", err)
	}

	if err := b.checkReady(); err != nil {
		return err
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	if b.closed {
		return fmt.Errorf("mqtt: report after close: %w", transports.ErrBackendClosed)
	}

	if !b.joined {
		return fmt.Errorf("mqtt: report before join: %w", transports.ErrNotJoined)
	}

	last, seen := b.lastGen[report.ParticipantName]
	if !seen {
		b.lastGen[report.ParticipantName] = report.Generation
		b.lastAlive[report.ParticipantName] = report.Alive

		return nil
	}

	if report.Generation > last {
		b.lastGen[report.ParticipantName] = report.Generation
		b.lastAlive[report.ParticipantName] = report.Alive

		return nil
	}

	if report.Generation == last && report.Alive == b.lastAlive[report.ParticipantName] {
		return nil
	}

	return fmt.Errorf("mqtt: stale generation %d for %q: %w", report.Generation, report.ParticipantName, transports.ErrStaleGeneration)
}

// Close tears down the backend and its connection. It is idempotent.
//
// It closes the done channel first so take paths waiting on the inbox
// unblock, then closes the connection to end the pump. Later takes
// report ErrBackendClosed.
func (b *Backend) Close() error {
	if err := b.checkReady(); err != nil {
		return err
	}

	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()

		return nil
	}

	b.closed = true
	b.mu.Unlock()

	close(b.done)

	err := b.conn.Close()
	if err != nil {
		return fmt.Errorf("mqtt: close conn: %w", err)
	}

	return nil
}

// copyBytes copies one payload preserving nil.
func copyBytes(payload []byte) []byte {
	if payload == nil {
		return nil
	}

	out := make([]byte, len(payload))
	copy(out, payload)

	return out
}

// cloneProps deep copies user properties so callers never alias queued maps.
func cloneProps(props Properties) Properties {
	if props.Extra == nil {
		return props
	}

	out := props
	if props.Extra != nil {
		out.Extra = make(map[string]string, len(props.Extra))
		maps.Copy(out.Extra, props.Extra)
	}

	return out
}

// encodeEnvelope packs a type name and payload into length prefixed bytes.
//
// Layout is 4 byte big endian type name length, type name bytes, then raw
// payload bytes. Both inputs are copied so the caller may reuse its slice.
func encodeEnvelope(typeName string, payload []byte) []byte {
	nameBytes := []byte(typeName)
	out := make([]byte, 4+len(nameBytes)+len(payload))
	binary.BigEndian.PutUint32(out[:4], uint32(len(nameBytes))) //nolint:gosec // G115: publish paths bound inputs through checkEnvelopeBounds
	copy(out[4:4+len(nameBytes)], nameBytes)
	copy(out[4+len(nameBytes):], payload)

	return out
}

// decodeEnvelope unpacks one envelope into type name and fresh payload.
//
// Truncated buffers report an error. Empty payload decodes to nil so nil
// and zero length stay equivalent on the contract.
func decodeEnvelope(buf []byte) (string, []byte, error) {
	if len(buf) < 4 {
		return "", nil, fmt.Errorf("mqtt: envelope shorter than header: %d", len(buf))
	}

	nameLen := binary.BigEndian.Uint32(buf[:4])

	if uint64(nameLen) > uint64(len(buf)-4) { //nolint:gosec // G115: widening conversions only, no truncation
		return "", nil, fmt.Errorf("mqtt: envelope name length %d exceeds %d", nameLen, len(buf)-4)
	}

	nameBytes := buf[4 : 4+nameLen]
	payloadBytes := buf[4+nameLen:]

	typeName := string(append([]byte(nil), nameBytes...))

	if len(payloadBytes) == 0 {
		return typeName, nil, nil
	}

	payload := make([]byte, len(payloadBytes))
	copy(payload, payloadBytes)

	return typeName, payload, nil
}
