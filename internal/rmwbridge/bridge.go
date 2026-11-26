// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

// Package rmwbridge exposes a minimal C ABI for the future rmw_weld layer.
//
// The Go core stays pure Go and never imports this package. A C++ RMW shim
// links against this bridge and forwards RMW calls to Weld RTPS. Cgo is
// confined here; no other package may import "C".
package rmwbridge

/*
#include <stdlib.h>

// weld_rmw_identifier names this RMW implementation for RMW_IMPLEMENTATION.
static const char* weld_rmw_identifier = "rmw_weld";
*/
import "C"

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
	"time"
	"unsafe"
)

// ErrBridgeClosed reports use after Close.
var ErrBridgeClosed = errors.New("rmwbridge: bridge closed")

// ErrNotJoined reports Publish, Take, or Report before JoinDiscovery.
var ErrNotJoined = errors.New("rmwbridge: discovery not joined")

// ErrStaleGeneration reports a reused or backward liveliness generation.
var ErrStaleGeneration = errors.New("rmwbridge: stale generation")

// ErrNodeExists reports duplicate node creation.
var ErrNodeExists = errors.New("rmwbridge: node exists")

// ErrNodeNotFound reports use or destroy of an unknown node.
var ErrNodeNotFound = errors.New("rmwbridge: node not found")

// ErrServiceExists reports duplicate service creation.
//
// Creation no longer fails: every CreateService for a registered name
// returns a new handle sharing the same request queue. The sentinel stays
// for API compatibility.
var ErrServiceExists = errors.New("rmwbridge: service exists")

// ErrClientExists reports duplicate client creation.
//
// Creation no longer fails: every CreateClient for a name returns a new
// handle calling the shared queue. The sentinel stays for API
// compatibility.
var ErrClientExists = errors.New("rmwbridge: client exists")

// ErrClientNotFound reports use or destroy of an unknown client.
var ErrClientNotFound = errors.New("rmwbridge: client not found")

// ErrServiceNotFound reports use or destroy of an unknown service.
var ErrServiceNotFound = errors.New("rmwbridge: service not found")

// ErrServiceNotAvailable reports a client call with no server present.
var ErrServiceNotAvailable = errors.New("rmwbridge: service not available")

// ErrClientClosed reports Call after client destroy.
var ErrClientClosed = errors.New("rmwbridge: client closed")

// ErrSubscriptionClosed reports Take after subscription Close.
var ErrSubscriptionClosed = errors.New("rmwbridge: subscription closed")

// ErrPublisherClosed reports Publish after publisher Close.
var ErrPublisherClosed = errors.New("rmwbridge: publisher closed")

// ErrServiceClosed reports Call or Serve after service destroy.
var ErrServiceClosed = errors.New("rmwbridge: service closed")

// ErrDuplicateResponse reports a second respond for one Serve.
var ErrDuplicateResponse = errors.New("rmwbridge: duplicate response")

// ErrPayloadTooLarge reports a payload exceeding maxPayloadBytes.
var ErrPayloadTooLarge = errors.New("rmwbridge: payload too large")

// waitPollInterval bounds WaitSet polling delay for trigger delivery.
const waitPollInterval = 5 * time.Millisecond

// inboxCapacity bounds the backend mirror queue.
const inboxCapacity = 128

// subscriptionCapacity bounds each subscription channel.
const subscriptionCapacity = 16

// serviceCapacity bounds each service request channel.
const serviceCapacity = 16

// maxPayloadBytes caps one payload accepted by the publish path.
//
// The 32 MiB bound matches maxPayloadLen in
// internal/transports/dds/datapath.go so a corrupt or abusive length cannot
// force a larger single allocation in this bridge. Oversized payloads are
// rejected with ErrPayloadTooLarge. The bridge never panics and never
// truncates; callers must split or shrink the payload and retry.
const maxPayloadBytes = 32 << 20

// Identifier returns the RMW implementation name selected by RMW_IMPLEMENTATION.
func Identifier() string {
	return "rmw_weld"
}

// Version returns the bridge version string.
func Version() string {
	return "0.1.0"
}

// IdentifierC returns the identifier as a C string for the C++ shim.
//
// The caller owns the returned pointer and must release it with FreeC.
func IdentifierC() *C.char {
	return C.CString(Identifier())
}

// FreeC releases a string returned by IdentifierC.
func FreeC(ptr *C.char) {
	C.free(unsafe.Pointer(ptr))
}

// OutgoingMessage is one serialized message handed to the bridge for send.
//
// It mirrors transports.OutgoingMessage without importing that package.
// Payload ownership is copy once: the caller must not mutate Payload after
// PublishBytes returns, and the bridge copies retained bytes.
type OutgoingMessage struct {
	// TopicName is the fully qualified ROS name such as /chatter.
	TopicName string
	// TypeName is the ROS IDL name such as std_msgs__msg__String.
	TypeName string
	// Payload holds XCDR1 CDR bytes. Nil means an empty sample.
	Payload []byte
	// TypeHash is the optional RIHS01 type hash. Empty means unknown.
	TypeHash string
}

// IncomingMessage is one serialized message taken from the bridge.
//
// It mirrors transports.IncomingMessage without importing that package.
// Payload is newly owned by the caller.
type IncomingMessage struct {
	// TopicName is the fully qualified ROS name the bytes arrived on.
	TopicName string
	// TypeName is the ROS IDL name of the payload.
	TypeName string
	// Payload holds XCDR1 CDR bytes. Nil means an empty sample.
	Payload []byte
}

// DiscoveryInfo describes a local participant joining discovery.
//
// It mirrors transports.DiscoveryInfo with the range left as plain config
// carried by the caller. Only participant name and domain id affect join.
type DiscoveryInfo struct {
	// ParticipantName is the human readable participant name.
	ParticipantName string
	// DomainID is the resolved domain id the participant joins.
	DomainID uint32
}

// LivelinessReport carries an aliveness assertion for a local endpoint.
//
// It mirrors transports.LivelinessReport. Generation is caller owned and
// must increase per participant. Repeating a Generation with the same Alive
// value is idempotent. Any other reuse or backward step is stale.
type LivelinessReport struct {
	// ParticipantName names the participant asserting liveliness.
	ParticipantName string
	// DomainID is the domain the assertion belongs to.
	DomainID uint32
	// Alive is true for a live assertion and false for dispose.
	Alive bool
	// Generation is the caller owned monotonic sequence.
	Generation uint64
}

// QoS is the passthrough quality of service for topics.
//
// Only Depth is carried, as a string, and it is stored and returned
// unchanged. The bridge never interprets it and never contacts the core.
type QoS struct {
	// Depth is the opaque history depth marker passed through unchanged.
	Depth string
}

// DefaultQoS returns the default passthrough QoS with depth 10.
func DefaultQoS() QoS {
	return QoS{Depth: "10"}
}

// Node is a named participant handle owned by a Bridge.
//
// DomainID captures the bridge domain at creation. Nodes with the same
// name in different domains are distinct entries in per-domain registries.
type Node struct {
	// Name is the node name used at creation.
	Name string
	// Namespace is the node namespace, default "/".
	Namespace string
	// DomainID is the domain the node was created in.
	DomainID uint32
	bridge   *Bridge
}

// Publisher is a topic writer bound to a node with passthrough QoS.
//
// DomainID is inherited from the node at creation. GID is a 16 byte
// writer identity assigned at creation from a bridge counter with the
// domain in the first 4 bytes, mirroring dds.WriterGUID width.
type Publisher struct {
	// Topic is the fully qualified ROS name.
	Topic string
	// TypeName is the ROS IDL type name, empty when unknown.
	TypeName string
	// QoS is the passthrough profile stored at creation.
	QoS QoS
	// DomainID is the domain the publisher was created in.
	DomainID uint32
	node     *Node
	bridge   *Bridge
	mu       sync.Mutex
	closed   bool
	gid      [16]byte
}

// deliveredSample is one queued delivery with its writer identity.
//
// Payload is the copied sample bytes. GID is the publishing writer
// identity captured at publish time, mirroring dds.WriterGUID width.
type deliveredSample struct {
	payload []byte
	gid     [16]byte
}

// loanSlot is one queued in-process loan carrying opaque C handles.
//
// Ptr is the opaque C message pointer valid only in the publishing
// process. Members is the opaque C introspection members pointer for
// fini plus free on return. The bridge never dereferences either and
// never copies payload bytes; only the 16 bytes of handles move, so the
// in-process path is zero-copy. GID is the publishing writer identity
// captured at publish time for the take-info side channel.
type loanSlot struct {
	ptr     uint64
	members uint64
	gid     [16]byte
}

// loanQueueDepth bounds each per-topic in-process loan queue.
//
// A bounded queue keeps a misuse leak bounded: loaned publishes consumed
// only via regular takes leave their handles queued, so an unbounded
// queue would grow without limit. When full, PublishLoan reports false
// and the C caller frees the buffer and keeps the wire copy, so delivery
// still succeeds via the copying path.
const loanQueueDepth = 16

// Subscription is a topic reader bound to a node with passthrough QoS.
//
// DomainID is inherited from the node at creation. Delivery only happens
// from publishers in the same domain on the same topic.
type Subscription struct {
	// Topic is the fully qualified ROS name.
	Topic string
	// TypeName is the ROS IDL type name, empty when unknown.
	TypeName string
	// QoS is the passthrough profile stored at creation.
	QoS QoS
	// DomainID is the domain the subscription was created in.
	DomainID uint32
	node     *Node
	bridge   *Bridge
	ch       chan deliveredSample
	mu       sync.Mutex
	closed   bool
	// recvSeq is the per-subscription receive sequence. It starts at 0
	// and increments on every delivered take, so the first take reports
	// 1, the second 2, and so on. Intra-process and network sequence
	// numbers are equal here because the bridge is a single transport.
	recvSeq uint64
	// lastGID is the writer identity of the most recent delivered take.
	lastGID [16]byte
}

// ServiceRequest is one pending call from client to server.
type ServiceRequest struct {
	// Payload holds the request bytes owned by the service.
	Payload []byte
	reply   chan []byte
}

// Service is a named request plus reply rendezvous.
//
// One input QoS fans out to two stored entities: the request side and the
// reply side. The stored profiles are returned unchanged by the dual
// actual-QoS getters.
type Service struct {
	// Name is the service name used at creation.
	Name string
	// TypeName is the ROS IDL service type name, empty when unknown.
	TypeName string
	bridge   *Bridge
	requests chan ServiceRequest
	mu       sync.Mutex
	closed   bool
	// requestQoS is the stored request side profile.
	requestQoS QoS
	// replyQoS is the stored reply side profile.
	replyQoS QoS
}

// Client is a named service client bound to one service name.
//
// The client does not own the rendezvous. It resolves the server at call
// time so Exists reports availability and Call fails fast when no server
// is present, then waits with full context deadline passthrough.
type Client struct {
	// Name is the service name this client calls.
	Name string
	// TypeName is the ROS IDL service type name, empty when unknown.
	TypeName string
	bridge   *Bridge
	mu       sync.Mutex
	closed   bool
	// requestQoS is the stored request side profile.
	requestQoS QoS
	// replyQoS is the stored reply side profile.
	replyQoS QoS
}

// Guard is a manually triggered wake source for a WaitSet.
type Guard struct {
	mu        sync.Mutex
	triggered bool
	wake      chan struct{}
}

// WaitSet holds guards and waits until one triggers or context ends.
type WaitSet struct {
	mu     sync.Mutex
	guards []*Guard
}

// nodeKey identifies a node within one domain.
//
// Nodes with the same name in different domains are distinct entries,
// which is the basis for domain isolation in per-domain registries.
type nodeKey struct {
	domain uint32
	name   string
}

// topicKey identifies a topic within one domain.
//
// Publishers and subscriptions match only when both domain and topic
// are equal, so samples never cross domains.
type topicKey struct {
	domain uint32
	topic  string
}

// Bridge is a thread-safe in-memory RMW backend.
//
// It mirrors the transports.Backend contract with JoinDiscovery,
// PublishBytes, TakeBytes, ReportLiveliness, and Close, and adds RMW
// registries for nodes, topic pub/sub, services, guards, and wait sets.
// Services on one name share a single request queue: every CreateService
// for a registered name returns a new handle popping the same queue, so
// N servers share load work-queue style with first-available-server-wins
// FIFO fairness and each request served exactly once. Clients behave the
// same way: every CreateClient for a name returns a new handle calling
// the shared queue, so 1-server N-clients keeps working beside N servers.
// The zero value is not usable; construct with NewBridge.
type Bridge struct {
	mu          sync.Mutex
	closed      bool
	joined      bool
	participant string
	domainID    uint32
	nodes       map[nodeKey]*Node
	pubs        map[topicKey]map[*Publisher]struct{}
	subs        map[topicKey]map[*Subscription]struct{}
	services    map[string]*Service
	clients     map[string]*Client
	// serviceExtras holds non-primary server handles sharing the primary
	// queue for a name. The primary lives in services; extras pop the
	// same requests channel, so every handle is a work-queue consumer.
	serviceExtras map[string]map[*Service]struct{}
	// clientExtras holds non-primary client handles for a name. The
	// primary lives in clients; extras call the same shared queue.
	clientExtras map[string]map[*Client]struct{}
	// loans holds one FIFO of opaque in-process loan handles per topic.
	// The queue carries only handle values, never payload bytes, so the
	// in-process loaned path moves no payload bytes. The wire path still
	// copies: CDR encode plus channel copy plus CDR decode each allocate.
	loans map[topicKey][]loanSlot
	// loanTransfers counts zero-copy handoffs consumed by TakeLoan.
	loanTransfers uint64
	// loanCopies counts payload byte copies on the loan path, always zero
	// by construction. A nonzero value would prove a copy slipped into
	// the zero-copy path, so tests assert it stays zero.
	loanCopies      uint64
	inbox           chan IncomingMessage
	generations     map[string]uint64
	generationAlive map[string]bool
	generationSeen  map[string]bool
	typeSupports    map[string]TypeSupport
	nextGID         uint64
}

// NewBridge returns an empty in-memory bridge.
func NewBridge() *Bridge {
	return &Bridge{
		nodes:           make(map[nodeKey]*Node),
		pubs:            make(map[topicKey]map[*Publisher]struct{}),
		subs:            make(map[topicKey]map[*Subscription]struct{}),
		services:        make(map[string]*Service),
		clients:         make(map[string]*Client),
		serviceExtras:   make(map[string]map[*Service]struct{}),
		clientExtras:    make(map[string]map[*Client]struct{}),
		loans:           make(map[topicKey][]loanSlot),
		inbox:           make(chan IncomingMessage, inboxCapacity),
		generations:     make(map[string]uint64),
		generationAlive: make(map[string]bool),
		generationSeen:  make(map[string]bool),
		typeSupports:    make(map[string]TypeSupport),
	}
}

// JoinDiscovery joins discovery with the given participant info.
//
// It mirrors Backend.JoinDiscovery: it honors ctx cancellation and reports
// ErrBridgeClosed after Close.
func (b *Bridge) JoinDiscovery(ctx context.Context, info DiscoveryInfo) error {
	err := ctx.Err()
	if err != nil {
		return fmt.Errorf("rmwbridge: join discovery: %w", err)
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return fmt.Errorf("rmwbridge: join discovery: %w", ErrBridgeClosed)
	}
	b.joined = true
	b.participant = info.ParticipantName
	b.domainID = info.DomainID
	return nil
}

// assignGIDLocked returns a fresh 16 byte publisher identity.
//
// The caller must hold b.mu. The domain occupies the first 4 bytes
// little endian and a monotonic counter occupies the next 8 bytes,
// mirroring the 16 byte dds.WriterGUID width. The counter never wraps
// to zero reuse within one bridge lifetime.
func (b *Bridge) assignGIDLocked(domain uint32) [16]byte {
	b.nextGID++
	var gid [16]byte
	binary.LittleEndian.PutUint32(gid[0:4], domain)
	binary.LittleEndian.PutUint64(gid[4:12], b.nextGID)
	return gid
}

// PublishBytes publishes one serialized message.
//
// It mirrors Backend.PublishBytes: it requires a prior JoinDiscovery,
// copies the payload, blocks only until ctx ends or the send is accepted,
// and reports ErrBridgeClosed after Close. Payloads larger than
// maxPayloadBytes are rejected with ErrPayloadTooLarge without copying,
// panicking, or truncating.
func (b *Bridge) PublishBytes(ctx context.Context, msg OutgoingMessage) error {
	err := ctx.Err()
	if err != nil {
		return fmt.Errorf("rmwbridge: publish: %w", err)
	}
	b.mu.Lock()
	closed := b.closed
	joined := b.joined
	b.mu.Unlock()
	if closed {
		return fmt.Errorf("rmwbridge: publish: %w", ErrBridgeClosed)
	}
	if !joined {
		return fmt.Errorf("rmwbridge: publish: %w", ErrNotJoined)
	}
	if len(msg.Payload) > maxPayloadBytes {
		return fmt.Errorf("rmwbridge: publish: payload %d bytes exceeds %d bytes: %w", len(msg.Payload), maxPayloadBytes, ErrPayloadTooLarge)
	}
	payload := append([]byte(nil), msg.Payload...)
	out := IncomingMessage{
		TopicName: msg.TopicName,
		TypeName:  msg.TypeName,
		Payload:   payload,
	}
	select {
	case <-ctx.Done():
		err := ctx.Err()
		if err != nil {
			return fmt.Errorf("rmwbridge: publish: %w", err)
		}
		return fmt.Errorf("rmwbridge: publish: %w", context.Canceled)
	case b.inbox <- out:
		return nil
	}
}

// TakeBytes receives one serialized message.
//
// It mirrors Backend.TakeBytes: it requires a prior JoinDiscovery, blocks
// until a message arrives or ctx ends, and reports ErrBridgeClosed after
// Close. The returned payload is newly owned by the caller.
func (b *Bridge) TakeBytes(ctx context.Context) (IncomingMessage, error) {
	b.mu.Lock()
	closed := b.closed
	joined := b.joined
	b.mu.Unlock()
	if closed {
		return IncomingMessage{}, fmt.Errorf("rmwbridge: take: %w", ErrBridgeClosed)
	}
	if !joined {
		return IncomingMessage{}, fmt.Errorf("rmwbridge: take: %w", ErrNotJoined)
	}
	select {
	case <-ctx.Done():
		err := ctx.Err()
		if err != nil {
			return IncomingMessage{}, fmt.Errorf("rmwbridge: take: %w", err)
		}
		return IncomingMessage{}, fmt.Errorf("rmwbridge: take: %w", context.Canceled)
	case msg := <-b.inbox:
		payload := append([]byte(nil), msg.Payload...)
		msg.Payload = payload
		return msg, nil
	}
}

// ReportLiveliness reports local endpoint liveliness.
//
// It mirrors Backend.ReportLiveliness: it requires a prior JoinDiscovery,
// enforces caller owned monotonic generations, treats a repeated
// Generation with the same Alive value as idempotent, and reports
// ErrStaleGeneration otherwise. It reports ErrBridgeClosed after Close.
func (b *Bridge) ReportLiveliness(ctx context.Context, report LivelinessReport) error {
	err := ctx.Err()
	if err != nil {
		return fmt.Errorf("rmwbridge: report liveliness: %w", err)
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return fmt.Errorf("rmwbridge: report liveliness: %w", ErrBridgeClosed)
	}
	if !b.joined {
		return fmt.Errorf("rmwbridge: report liveliness: %w", ErrNotJoined)
	}
	key := report.ParticipantName
	seen := b.generationSeen[key]
	if !seen {
		b.generationSeen[key] = true
		b.generations[key] = report.Generation
		b.generationAlive[key] = report.Alive
		return nil
	}
	last := b.generations[key]
	lastAlive := b.generationAlive[key]
	if report.Generation > last {
		b.generations[key] = report.Generation
		b.generationAlive[key] = report.Alive
		return nil
	}
	if report.Generation == last && report.Alive == lastAlive {
		return nil
	}
	return fmt.Errorf("rmwbridge: report liveliness: %w", ErrStaleGeneration)
}

// Close releases the bridge handle.
//
// It is idempotent. The first call tears down and later calls return nil.
// After the first Close the other methods report ErrBridgeClosed.
// Channels are left open so concurrent senders never panic; flags gate use.
func (b *Bridge) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return nil
	}
	b.closed = true
	return nil
}

// CreateNode registers a node by name in the current bridge domain.
//
// The node inherits the domain from the last JoinDiscovery (0 before any
// join). The same name may exist in different domains as distinct entries.
func (b *Bridge) CreateNode(name string) (*Node, error) {
	b.mu.Lock()
	domain := b.domainID
	b.mu.Unlock()
	return b.CreateNodeInDomain(domain, name)
}

// CreateNodeInDomain registers a node by name in an explicit domain.
//
// It backs domain isolation: registries are keyed by domain plus name so
// topics in one domain never observe nodes in another. Duplicate names
// within the same domain report ErrNodeExists.
func (b *Bridge) CreateNodeInDomain(domain uint32, name string) (*Node, error) {
	if name == "" {
		return nil, errors.New("rmwbridge: create node: empty name") //nolint:wrapcheck // new error, nothing to wrap
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return nil, fmt.Errorf("rmwbridge: create node: %w", ErrBridgeClosed)
	}
	key := nodeKey{domain: domain, name: name}
	_, exists := b.nodes[key]
	if exists {
		return nil, fmt.Errorf("rmwbridge: create node %q: %w", name, ErrNodeExists)
	}
	node := &Node{Name: name, Namespace: "/", DomainID: domain, bridge: b}
	b.nodes[key] = node
	return node, nil
}

// DestroyNode unregisters a node by name across all domains.
//
// It deletes every entry with the given name in any domain registry so a
// node created before a domain switch is still removable. Unknown names
// report ErrNodeNotFound. Live publishers and subscriptions keep working
// until closed; destroy only removes the node handle.
func (b *Bridge) DestroyNode(name string) error {
	if name == "" {
		return errors.New("rmwbridge: destroy node: empty name") //nolint:wrapcheck // new error, nothing to wrap
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return fmt.Errorf("rmwbridge: destroy node: %w", ErrBridgeClosed)
	}
	found := false
	for key := range b.nodes {
		if key.name == name {
			delete(b.nodes, key)
			found = true
		}
	}
	if !found {
		return fmt.Errorf("rmwbridge: destroy node %q: %w", name, ErrNodeNotFound)
	}
	return nil
}

// CreatePublisher creates a topic writer for a registered node.
//
// The QoS value is stored unchanged as passthrough and returned by
// ActualQoS. The publisher inherits the node domain and is registered in
// the per-domain pubs registry under domain plus topic. The GID is a
// fresh 16 byte identity for rmw_get_gid_for_publisher.
func (b *Bridge) CreatePublisher(node *Node, topic string, qos QoS) (*Publisher, error) {
	if node == nil {
		return nil, errors.New("rmwbridge: create publisher: nil node") //nolint:wrapcheck // new error, nothing to wrap
	}
	if topic == "" {
		return nil, errors.New("rmwbridge: create publisher: empty topic") //nolint:wrapcheck // new error, nothing to wrap
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return nil, fmt.Errorf("rmwbridge: create publisher: %w", ErrBridgeClosed)
	}
	stored, exists := b.nodes[nodeKey{domain: node.DomainID, name: node.Name}]
	if !exists || stored != node {
		return nil, fmt.Errorf("rmwbridge: create publisher: %w", ErrNodeNotFound)
	}
	gid := b.assignGIDLocked(node.DomainID)
	pub := &Publisher{Topic: topic, QoS: qos, DomainID: node.DomainID, node: node, bridge: b, gid: gid}
	key := topicKey{domain: node.DomainID, topic: topic}
	set, ok := b.pubs[key]
	if !ok {
		set = make(map[*Publisher]struct{})
		b.pubs[key] = set
	}
	set[pub] = struct{}{}
	return pub, nil
}

// GID returns the 16 byte publisher identity.
//
// It mirrors dds.WriterGUID width and backs the rmw_get_gid_for_publisher
// concept: the domain occupies the first 4 bytes and a bridge counter the
// next 8 bytes.
func (p *Publisher) GID() [16]byte {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.gid
}

// ActualQoS returns the stored passthrough QoS.
//
// The value is exactly the profile given at creation; the bridge never
// interprets it. It backs rmw_publisher_get_actual_qos.
func (p *Publisher) ActualQoS() QoS {
	return p.QoS
}

// snapshotSubscriptions returns subscription targets plus bridge closed flag.
func (b *Bridge) snapshotSubscriptions(domain uint32, topic string) ([]*Subscription, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	set := b.subs[topicKey{domain: domain, topic: topic}]
	targets := make([]*Subscription, 0, len(set))
	for sub := range set {
		targets = append(targets, sub)
	}
	return targets, b.closed
}

// snapshotPublishers returns publisher targets plus bridge closed flag.
func (b *Bridge) snapshotPublishers(domain uint32, topic string) ([]*Publisher, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	set := b.pubs[topicKey{domain: domain, topic: topic}]
	targets := make([]*Publisher, 0, len(set))
	for pub := range set {
		targets = append(targets, pub)
	}
	return targets, b.closed
}

// countOpenSubscriptions counts non-closed subscriptions.
func countOpenSubscriptions(targets []*Subscription) int {
	n := 0
	for _, sub := range targets {
		sub.mu.Lock()
		closedSub := sub.closed
		sub.mu.Unlock()
		if !closedSub {
			n++
		}
	}
	return n
}

// countOpenPublishers counts non-closed publishers.
func countOpenPublishers(targets []*Publisher) int {
	n := 0
	for _, pub := range targets {
		pub.mu.Lock()
		closedPub := pub.closed
		pub.mu.Unlock()
		if !closedPub {
			n++
		}
	}
	return n
}

// removePublisherLocked removes pub from the pubs registry.
func (b *Bridge) removePublisherLocked(pub *Publisher) {
	key := topicKey{domain: pub.DomainID, topic: pub.Topic}
	set, ok := b.pubs[key]
	if ok {
		delete(set, pub)
		if len(set) == 0 {
			delete(b.pubs, key)
		}
	}
}

// removeSubscriptionLocked removes sub from the subs registry.
func (b *Bridge) removeSubscriptionLocked(sub *Subscription) {
	key := topicKey{domain: sub.DomainID, topic: sub.Topic}
	set, ok := b.subs[key]
	if ok {
		delete(set, sub)
		if len(set) == 0 {
			delete(b.subs, key)
		}
	}
}

// CountMatchedSubscriptions returns the number of live subscriptions on
// the same domain plus topic.
//
// Closed subscriptions are excluded. It backs
// rmw_publisher_count_matched_subscriptions.
func (p *Publisher) CountMatchedSubscriptions() int {
	targets, closed := p.bridge.snapshotSubscriptions(p.DomainID, p.Topic)
	if closed {
		return 0
	}
	p.mu.Lock()
	pubClosed := p.closed
	p.mu.Unlock()
	if pubClosed {
		return 0
	}
	return countOpenSubscriptions(targets)
}

// Close unregisters the publisher and marks it closed.
//
// It is idempotent and never closes subscription channels, so concurrent
// publishers and subscribers never panic. Publish after Close reports
// ErrPublisherClosed.
func (p *Publisher) Close() error {
	bridge := p.bridge
	bridge.mu.Lock()
	defer bridge.mu.Unlock()
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil
	}
	p.closed = true
	bridge.removePublisherLocked(p)
	return nil
}

// CreateSubscription creates a topic reader for a registered node.
//
// The QoS value is stored unchanged as passthrough and returned by
// ActualQoS. The subscription inherits the node domain and is registered
// in the per-domain subs registry under domain plus topic.
func (b *Bridge) CreateSubscription(node *Node, topic string, qos QoS) (*Subscription, error) {
	if node == nil {
		return nil, errors.New("rmwbridge: create subscription: nil node") //nolint:wrapcheck // new error, nothing to wrap
	}
	if topic == "" {
		return nil, errors.New("rmwbridge: create subscription: empty topic") //nolint:wrapcheck // new error, nothing to wrap
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return nil, fmt.Errorf("rmwbridge: create subscription: %w", ErrBridgeClosed)
	}
	stored, exists := b.nodes[nodeKey{domain: node.DomainID, name: node.Name}]
	if !exists || stored != node {
		return nil, fmt.Errorf("rmwbridge: create subscription: %w", ErrNodeNotFound)
	}
	sub := &Subscription{
		Topic:    topic,
		QoS:      qos,
		DomainID: node.DomainID,
		node:     node,
		bridge:   b,
		ch:       make(chan deliveredSample, subscriptionCapacity),
	}
	key := topicKey{domain: node.DomainID, topic: topic}
	set, ok := b.subs[key]
	if !ok {
		set = make(map[*Subscription]struct{})
		b.subs[key] = set
	}
	set[sub] = struct{}{}
	return sub, nil
}

// ActualQoS returns the stored passthrough QoS.
//
// The value is exactly the profile given at creation; the bridge never
// interprets it. It backs rmw_subscription_get_actual_qos.
func (s *Subscription) ActualQoS() QoS {
	return s.QoS
}

// CountMatchedPublishers returns the number of live publishers on the
// same domain plus topic.
//
// Closed publishers are excluded. It backs
// rmw_subscription_count_matched_publishers.
func (s *Subscription) CountMatchedPublishers() int {
	targets, closed := s.bridge.snapshotPublishers(s.DomainID, s.Topic)
	if closed {
		return 0
	}
	s.mu.Lock()
	subClosed := s.closed
	s.mu.Unlock()
	if subClosed {
		return 0
	}
	return countOpenPublishers(targets)
}

// Publish delivers a payload copy to every subscription on the topic in
// the same domain.
//
// Copy path: the payload bytes are copied once here and once per Take,
// plus CDR encode plus decode each allocate, because serialization plus
// Go channel ownership inherently copy. The loaned path PublishLoan is
// zero-copy instead: only opaque handle values move with no payload byte
// copy. Domain isolation: only subscriptions with DomainID equal to the
// publisher domain on the same topic receive the sample, mirroring
// dds decodePlainCDR plus TakeFrom domain filtering. Payload ownership is
// copy once per transports copy once: the bridge copies once and each
// Take returns a newly owned copy. Payloads larger than maxPayloadBytes
// are rejected with ErrPayloadTooLarge without copying, panicking, or
// truncating. It honors ctx cancellation while
// sending, reports ErrPublisherClosed after Close, and reports
// ErrBridgeClosed after Bridge Close. With no matches it succeeds
// without delivery. It backs rmw_publish.
func (p *Publisher) Publish(ctx context.Context, payload []byte) error {
	err := ctx.Err()
	if err != nil {
		return fmt.Errorf("rmwbridge: publisher publish: %w", err)
	}
	if len(payload) > maxPayloadBytes {
		return fmt.Errorf("rmwbridge: publisher publish: payload %d bytes exceeds %d bytes: %w", len(payload), maxPayloadBytes, ErrPayloadTooLarge)
	}
	bridge := p.bridge
	p.mu.Lock()
	pubClosed := p.closed
	gid := p.gid
	p.mu.Unlock()
	if pubClosed {
		return fmt.Errorf("rmwbridge: publisher publish: %w", ErrPublisherClosed)
	}
	bridge.mu.Lock()
	if bridge.closed {
		bridge.mu.Unlock()
		return fmt.Errorf("rmwbridge: publisher publish: %w", ErrBridgeClosed)
	}
	set := bridge.subs[topicKey{domain: p.DomainID, topic: p.Topic}]
	targets := make([]*Subscription, 0, len(set))
	for sub := range set {
		if sub.DomainID != p.DomainID {
			continue
		}
		targets = append(targets, sub)
	}
	bridge.mu.Unlock()
	data := append([]byte(nil), payload...)
	sample := deliveredSample{payload: data, gid: gid}
	for _, sub := range targets {
		sub.mu.Lock()
		closed := sub.closed
		sub.mu.Unlock()
		if closed {
			continue
		}
		select {
		case <-ctx.Done():
			err := ctx.Err()
			if err != nil {
				return fmt.Errorf("rmwbridge: publisher publish: %w", err)
			}
			return fmt.Errorf("rmwbridge: publisher publish: %w", context.Canceled)
		case sub.ch <- sample:
		}
	}
	return nil
}

// Take receives one payload from the subscription.
//
// It blocks until a message arrives or ctx ends. The returned slice is
// newly owned by the caller per copy once, mirroring dds TakeFrom
// ownership. Every delivered take, including this one, increments the
// per-subscription receive sequence. It backs rmw_take.
func (s *Subscription) Take(ctx context.Context) ([]byte, error) {
	payload, _, _, err := s.TakeWithInfo(ctx)
	return payload, err
}

// TakeWithInfo receives one payload with writer identity and sequence.
//
// Copy path: the returned payload is newly owned by the caller per copy
// once, because channel delivery plus ownership inherently copy; CDR
// decode on the C side allocates a fresh message for the same reason.
// The loaned path TakeLoan is zero-copy instead: it returns the same
// opaque handle enqueued by PublishLoan with no payload byte copy. It
// blocks until a message arrives or ctx ends. The GID is the publishing
// writer identity captured at publish time. The sequence is the
// per-subscription receive counter starting at 1 and incrementing on
// every delivered take, including Takes that discard the attribution.
// Intra-process and network sequence numbers are equal here because the
// bridge is a single transport: callers report the same value for both.
// It backs the rmw_take_with_info attribution path.
func (s *Subscription) TakeWithInfo(ctx context.Context) ([]byte, [16]byte, uint64, error) {
	var empty [16]byte
	s.bridge.mu.Lock()
	bridgeClosed := s.bridge.closed
	s.bridge.mu.Unlock()
	if bridgeClosed {
		return nil, empty, 0, fmt.Errorf("rmwbridge: subscription take: %w", ErrBridgeClosed)
	}
	s.mu.Lock()
	subClosed := s.closed
	s.mu.Unlock()
	if subClosed {
		return nil, empty, 0, fmt.Errorf("rmwbridge: subscription take: %w", ErrSubscriptionClosed)
	}
	select {
	case <-ctx.Done():
		err := ctx.Err()
		if err != nil {
			return nil, empty, 0, fmt.Errorf("rmwbridge: subscription take: %w", err)
		}
		return nil, empty, 0, fmt.Errorf("rmwbridge: subscription take: %w", context.Canceled)
	case sample := <-s.ch:
		out := append([]byte(nil), sample.payload...)
		seq := s.noteDeliveredLocked(sample.gid)
		return out, sample.gid, seq, nil
	}
}

// noteDeliveredLocked records one delivery and returns its sequence.
//
// The caller must not hold s.mu. The GID is the writer identity of the
// delivered sample. The sequence increments on every call, so stashed
// retries must not call it twice for one sample.
func (s *Subscription) noteDeliveredLocked(gid [16]byte) uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.recvSeq++
	s.lastGID = gid
	return s.recvSeq
}

// ReplayTakeInfo records a stashed retry delivery without incrementing.
//
// A short-buffer retry dequeued once already: its sequence was assigned
// at dequeue, so redelivery replays the same gid plus seq. When seq
// advances past the counter (no interleaving in practice) the counter
// follows it to keep the stream contiguous. It never blocks.
func (s *Subscription) ReplayTakeInfo(gid [16]byte, seq uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if seq > s.recvSeq {
		s.recvSeq = seq
	}
	s.lastGID = gid
}

// NoteExternalDelivery records one wire delivery from outside the queue.
//
// Wire takes bypass the subscription channel, so this assigns them the
// next per-subscription receive sequence and records the writer identity
// for the take-info side channel. A zero GID means the wire leg carried
// no writer identity. It returns the assigned sequence.
func (s *Subscription) NoteExternalDelivery(gid [16]byte) uint64 {
	return s.noteDeliveredLocked(gid)
}

// LastTakeInfo returns the attribution of the most recent delivered take.
//
// It reports the writer identity and the per-subscription receive
// sequence of the last delivery. A zero sequence means no take has
// delivered yet. It never blocks.
func (s *Subscription) LastTakeInfo() ([16]byte, uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastGID, s.recvSeq
}

// PublishLoan enqueues one in-process loan handle without copying.
//
// Ptr is the opaque C message pointer and members is the opaque C
// introspection members pointer, both valid only in the publishing
// process. The bridge never dereferences either and never copies payload
// bytes; only the handle values move, so this path is zero-copy. The
// wire path still copies: CDR encode allocates wire bytes, Publish copies
// into each subscription channel, and Take copies out plus CDR decode
// allocates a fresh message. Domain isolation matches Publish: only
// subscriptions in the publisher domain on the same topic may later
// dequeue the handle.
//
// Single-owner transfer: the first TakeLoan dequeues the handle and owns
// it until C return frees it exactly once via the C loan tracking. Other
// matched subscriptions still receive the wire copy published alongside,
// so every subscriber gets exactly one delivery with at most one
// zero-copy handoff per publish. When no subscription matches or the
// per-topic queue is full it reports false with nil error and queues
// nothing; the caller keeps the wire copy path and frees the buffer, so
// delivery still succeeds via copying without leaking. It reports
// ErrPublisherClosed after Close and ErrBridgeClosed after Bridge Close.
// It never blocks.
func (p *Publisher) PublishLoan(ptr uint64, members uint64) (bool, error) {
	if ptr == 0 || members == 0 {
		return false, errors.New("rmwbridge: publish loan: zero handle") //nolint:wrapcheck // new error, nothing to wrap
	}
	bridge := p.bridge
	p.mu.Lock()
	pubClosed := p.closed
	gid := p.gid
	p.mu.Unlock()
	if pubClosed {
		return false, fmt.Errorf("rmwbridge: publisher publish loan: %w", ErrPublisherClosed)
	}
	bridge.mu.Lock()
	defer bridge.mu.Unlock()
	if bridge.closed {
		return false, fmt.Errorf("rmwbridge: publisher publish loan: %w", ErrBridgeClosed)
	}
	key := topicKey{domain: p.DomainID, topic: p.Topic}
	matched := 0
	for sub := range bridge.subs[key] {
		sub.mu.Lock()
		closedSub := sub.closed
		subDomain := sub.DomainID
		sub.mu.Unlock()
		if !closedSub && subDomain == p.DomainID {
			matched++
		}
	}
	if matched == 0 {
		return false, nil
	}
	queued := bridge.loans[key]
	if len(queued) >= loanQueueDepth {
		return false, nil
	}
	bridge.loans[key] = append(queued, loanSlot{ptr: ptr, members: members, gid: gid})
	return true, nil
}

// TakeLoan dequeues one in-process loan handle without copying.
//
// It returns the same ptr plus members values passed to PublishLoan with
// no payload byte copy, so this path is zero-copy where the wire path
// copies (channel copy plus CDR decode into a fresh buffer). The returned
// GID is the publishing writer identity and seq is the per-subscription
// receive sequence starting at 1, recorded like TakeWithInfo for the
// take-info side channel. Ok is false when no loan is queued; the caller
// falls back to the copying wire take. Single-owner transfer: the dequeued
// handle is owned by the caller until C return frees it exactly once.
// It reports ErrSubscriptionClosed after Close and ErrBridgeClosed after
// Bridge Close. It never blocks.
func (s *Subscription) TakeLoan() (ptr uint64, members uint64, gid [16]byte, seq uint64, ok bool, err error) {
	var empty [16]byte
	bridge := s.bridge
	bridge.mu.Lock()
	if bridge.closed {
		bridge.mu.Unlock()
		return 0, 0, empty, 0, false, fmt.Errorf("rmwbridge: subscription take loan: %w", ErrBridgeClosed)
	}
	s.mu.Lock()
	subClosed := s.closed
	s.mu.Unlock()
	if subClosed {
		bridge.mu.Unlock()
		return 0, 0, empty, 0, false, fmt.Errorf("rmwbridge: subscription take loan: %w", ErrSubscriptionClosed)
	}
	key := topicKey{domain: s.DomainID, topic: s.Topic}
	queued := bridge.loans[key]
	if len(queued) == 0 {
		bridge.mu.Unlock()
		return 0, 0, empty, 0, false, nil
	}
	slot := queued[0]
	rest := queued[1:]
	if len(rest) == 0 {
		delete(bridge.loans, key)
	} else {
		bridge.loans[key] = rest
	}
	bridge.loanTransfers++
	bridge.mu.Unlock()
	seq = s.noteDeliveredLocked(slot.gid)
	return slot.ptr, slot.members, slot.gid, seq, true, nil
}

// LoanTransfers returns the count of zero-copy loan handoffs consumed.
//
// Each TakeLoan that dequeues a handle increments it. It never blocks.
func (b *Bridge) LoanTransfers() uint64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.loanTransfers
}

// LoanCopies returns the count of payload byte copies on the loan path.
//
// It stays zero by construction: PublishLoan plus TakeLoan move only
// handle values and never copy payload bytes. A nonzero value would prove
// a copy slipped into the zero-copy path. It never blocks.
func (b *Bridge) LoanCopies() uint64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.loanCopies
}

// Close unregisters the subscription and marks it closed.
//
// It is idempotent and never closes the delivery channel, so concurrent
// publishers never panic.
func (s *Subscription) Close() error {
	bridge := s.bridge
	bridge.mu.Lock()
	defer bridge.mu.Unlock()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	bridge.removeSubscriptionLocked(s)
	return nil
}

// resolveServiceQoS fans one optional QoS input out to two stored profiles.
//
// Zero inputs means both sides use DefaultQoS. One input fans out to both
// sides. Two inputs set request then reply. More than two is an error.
func resolveServiceQoS(qos []QoS) (QoS, QoS, error) {
	switch len(qos) {
	case 0:
		return DefaultQoS(), DefaultQoS(), nil
	case 1:
		return qos[0], qos[0], nil
	case 2:
		return qos[0], qos[1], nil
	default:
		return QoS{}, QoS{}, fmt.Errorf("rmwbridge: too many QoS profiles: %d", len(qos))
	}
}

// CreateService registers a service by name with request plus reply channels.
//
// Optional QoS inputs fan out per resolveServiceQoS: zero means both sides
// default, one fans out to both, two set request then reply. Every call
// for a registered name returns a new handle sharing the same request
// queue: N servers on one name pop the same channel work-queue style, so
// first-available-server-wins with FIFO fairness and each request is
// served exactly once. Later handles inherit the queue profiles stored
// at first create.
func (b *Bridge) CreateService(name string, qos ...QoS) (*Service, error) {
	if name == "" {
		return nil, errors.New("rmwbridge: create service: empty name") //nolint:wrapcheck // new error, nothing to wrap
	}
	requestQoS, replyQoS, err := resolveServiceQoS(qos)
	if err != nil {
		return nil, fmt.Errorf("rmwbridge: create service %q: %w", name, err)
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return nil, fmt.Errorf("rmwbridge: create service: %w", ErrBridgeClosed)
	}
	if primary, exists := b.services[name]; exists {
		extra := &Service{
			Name:       name,
			bridge:     b,
			requests:   primary.requests,
			requestQoS: primary.requestQoS,
			replyQoS:   primary.replyQoS,
		}
		set, ok := b.serviceExtras[name]
		if !ok {
			set = make(map[*Service]struct{})
			b.serviceExtras[name] = set
		}
		set[extra] = struct{}{}
		return extra, nil
	}
	svc := &Service{
		Name:       name,
		bridge:     b,
		requests:   make(chan ServiceRequest, serviceCapacity),
		requestQoS: requestQoS,
		replyQoS:   replyQoS,
	}
	b.services[name] = svc
	return svc, nil
}

// liveServiceLocked reports whether s is a registered handle for name.
//
// The primary lives in services and extras live in serviceExtras. The
// caller must hold b.mu.
func (b *Bridge) liveServiceLocked(name string, s *Service) bool {
	if primary, ok := b.services[name]; ok && primary == s {
		return true
	}
	set, ok := b.serviceExtras[name]
	if !ok {
		return false
	}
	_, ok = set[s]
	return ok
}

// closeServiceLocked marks one service handle closed and unregisters it.
//
// A closed primary promotes one extra to primary so the shared queue
// stays registered while servers remain; the last handle closes the
// queue and deletes the registration. It is idempotent. The caller
// must hold b.mu.
func (b *Bridge) closeServiceLocked(s *Service) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.closed = true
	s.mu.Unlock()
	if primary, ok := b.services[s.Name]; ok && primary == s {
		if set, has := b.serviceExtras[s.Name]; has {
			for extra := range set {
				delete(set, extra)
				if len(set) == 0 {
					delete(b.serviceExtras, s.Name)
				}
				b.services[s.Name] = extra
				return
			}
		}
		delete(b.services, s.Name)
		return
	}
	if set, has := b.serviceExtras[s.Name]; has {
		delete(set, s)
		if len(set) == 0 {
			delete(b.serviceExtras, s.Name)
		}
	}
}

// destroyServiceOrClient removes a service or client registration.
//
// Destroying a service closes every handle sharing its queue: the queue
// is gone only when no server remains, so one DestroyService call ends
// service for the name. The same holds for clients.
func (b *Bridge) destroyServiceOrClient(name, kind string) error {
	if name == "" {
		return errors.New("rmwbridge: destroy " + kind + ": empty name") //nolint:wrapcheck // new error, nothing to wrap
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return fmt.Errorf("rmwbridge: destroy %s: %w", kind, ErrBridgeClosed)
	}
	if kind == "service" {
		primary, exists := b.services[name]
		if !exists {
			return fmt.Errorf("rmwbridge: destroy service %q: %w", name, ErrServiceNotFound)
		}
		primary.mu.Lock()
		primary.closed = true
		primary.mu.Unlock()
		if set, has := b.serviceExtras[name]; has {
			for extra := range set {
				extra.mu.Lock()
				extra.closed = true
				extra.mu.Unlock()
			}
			delete(b.serviceExtras, name)
		}
		delete(b.services, name)
		return nil
	}
	primary, exists := b.clients[name]
	if !exists {
		return fmt.Errorf("rmwbridge: destroy client %q: %w", name, ErrClientNotFound)
	}
	primary.mu.Lock()
	primary.closed = true
	primary.mu.Unlock()
	if set, has := b.clientExtras[name]; has {
		for extra := range set {
			extra.mu.Lock()
			extra.closed = true
			extra.mu.Unlock()
		}
		delete(b.clientExtras, name)
	}
	delete(b.clients, name)
	return nil
}

// DestroyService unregisters a service by name.
//
// Pending callers observe context timeout since the request channel is
// left open to avoid panics; new Call and Serve report ErrServiceClosed.
func (b *Bridge) DestroyService(name string) error {
	return b.destroyServiceOrClient(name, "service")
}

// Exists reports whether the service is still registered and usable.
//
// It returns false after DestroyService, after Bridge Close, after the
// handle was closed, or when the handle is unknown. It never blocks.
func (s *Service) Exists() bool {
	s.bridge.mu.Lock()
	defer s.bridge.mu.Unlock()
	if s.bridge.closed {
		return false
	}
	s.mu.Lock()
	closed := s.closed
	s.mu.Unlock()
	if closed {
		return false
	}
	return s.bridge.liveServiceLocked(s.Name, s)
}

// Close unregisters one service handle without disturbing the queue.
//
// Other handles sharing the name keep serving: a closed primary
// promotes one extra so the queue stays registered, and the queue is
// deleted only when the last handle closes. It is idempotent and never
// closes the request channel, so concurrent callers never panic.
func (s *Service) Close() error {
	s.bridge.mu.Lock()
	defer s.bridge.mu.Unlock()
	s.bridge.closeServiceLocked(s)
	return nil
}

// RequestActualQoS returns the stored request side profile.
func (s *Service) RequestActualQoS() QoS {
	return s.requestQoS
}

// ReplyActualQoS returns the stored reply side profile.
func (s *Service) ReplyActualQoS() QoS {
	return s.replyQoS
}

// RequestSubscriptionActualQoS returns the stored request side profile.
//
// It aliases RequestActualQoS with the RMW service side name.
func (s *Service) RequestSubscriptionActualQoS() QoS {
	return s.requestQoS
}

// ResponsePublisherActualQoS returns the stored reply side profile.
//
// It aliases ReplyActualQoS with the RMW service side name.
func (s *Service) ResponsePublisherActualQoS() QoS {
	return s.replyQoS
}

// CreateClient registers a client handle for one service name.
//
// The server need not exist yet. Optional QoS inputs fan out per
// resolveServiceQoS: zero means both sides default, one fans out to both,
// two set request then reply. Every call for a name returns a new handle
// calling the shared queue, so N clients share one service beside N
// servers. Later handles inherit the profiles stored at first create.
func (b *Bridge) CreateClient(name string, qos ...QoS) (*Client, error) {
	if name == "" {
		return nil, errors.New("rmwbridge: create client: empty name") //nolint:wrapcheck // new error, nothing to wrap
	}
	requestQoS, replyQoS, err := resolveServiceQoS(qos)
	if err != nil {
		return nil, fmt.Errorf("rmwbridge: create client %q: %w", name, err)
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return nil, fmt.Errorf("rmwbridge: create client: %w", ErrBridgeClosed)
	}
	if primary, exists := b.clients[name]; exists {
		extra := &Client{
			Name:       name,
			bridge:     b,
			requestQoS: primary.requestQoS,
			replyQoS:   primary.replyQoS,
		}
		set, ok := b.clientExtras[name]
		if !ok {
			set = make(map[*Client]struct{})
			b.clientExtras[name] = set
		}
		set[extra] = struct{}{}
		return extra, nil
	}
	client := &Client{
		Name:       name,
		bridge:     b,
		requestQoS: requestQoS,
		replyQoS:   replyQoS,
	}
	b.clients[name] = client
	return client, nil
}

// DestroyClient unregisters a client handle by service name.
//
// Pending Call waiters observe context timeout since reply channels stay
// open; new Call reports ErrClientClosed.
func (b *Bridge) DestroyClient(name string) error {
	return b.destroyServiceOrClient(name, "client")
}

// Exists reports whether a server is available for this client.
//
// It mirrors rmw_service_server_is_available: true only when the bridge
// is open, the client is open, and a live service with the same name is
// registered. It never blocks.
func (c *Client) Exists() bool {
	c.bridge.mu.Lock()
	defer c.bridge.mu.Unlock()
	if c.bridge.closed {
		return false
	}
	c.mu.Lock()
	clientClosed := c.closed
	c.mu.Unlock()
	if clientClosed {
		return false
	}
	svc, ok := c.bridge.services[c.Name]
	if !ok {
		return false
	}
	svc.mu.Lock()
	defer svc.mu.Unlock()
	return !svc.closed
}

// RequestActualQoS returns the stored request side profile.
func (c *Client) RequestActualQoS() QoS {
	return c.requestQoS
}

// ReplyActualQoS returns the stored reply side profile.
func (c *Client) ReplyActualQoS() QoS {
	return c.replyQoS
}

// RequestPublisherActualQoS returns the stored request side profile.
//
// It aliases RequestActualQoS with the RMW client side name.
func (c *Client) RequestPublisherActualQoS() QoS {
	return c.requestQoS
}

// ResponseSubscriptionActualQoS returns the stored reply side profile.
//
// It aliases ReplyActualQoS with the RMW client side name.
func (c *Client) ResponseSubscriptionActualQoS() QoS {
	return c.replyQoS
}

// resolveCallService returns the live service for a client call.
func (c *Client) resolveCallService() (*Service, error) {
	c.bridge.mu.Lock()
	bridgeClosed := c.bridge.closed
	svc, ok := c.bridge.services[c.Name]
	c.bridge.mu.Unlock()
	if bridgeClosed {
		return nil, fmt.Errorf("rmwbridge: client call: %w", ErrBridgeClosed)
	}
	c.mu.Lock()
	clientClosed := c.closed
	c.mu.Unlock()
	if clientClosed {
		return nil, fmt.Errorf("rmwbridge: client call: %w", ErrClientClosed)
	}
	if !ok {
		return nil, fmt.Errorf("rmwbridge: client call %q: %w", c.Name, ErrServiceNotAvailable)
	}
	svc.mu.Lock()
	svcClosed := svc.closed
	svc.mu.Unlock()
	if svcClosed {
		return nil, fmt.Errorf("rmwbridge: client call %q: %w", c.Name, ErrServiceNotAvailable)
	}
	return svc, nil
}

// Call sends one request to the named server and waits for its reply.
//
// The server is resolved at call time. When no live server is registered
// it fails fast with ErrServiceNotAvailable. Otherwise the request is
// queued and the wait carries the caller's context deadline end to end:
// cancel or timeout aborts both the send and the reply wait. Payloads
// larger than maxPayloadBytes are rejected with ErrPayloadTooLarge without
// copying. It never panics and never truncates.
func (c *Client) Call(ctx context.Context, req []byte) ([]byte, error) {
	err := ctx.Err()
	if err != nil {
		return nil, fmt.Errorf("rmwbridge: client call: %w", err)
	}
	svc, err := c.resolveCallService()
	if err != nil {
		return nil, err
	}
	if len(req) > maxPayloadBytes {
		return nil, fmt.Errorf("rmwbridge: client call: payload %d bytes exceeds %d bytes: %w", len(req), maxPayloadBytes, ErrPayloadTooLarge)
	}
	data := append([]byte(nil), req...)
	reply := make(chan []byte, 1)
	out := ServiceRequest{Payload: data, reply: reply}
	select {
	case <-ctx.Done():
		err := ctx.Err()
		if err != nil {
			return nil, fmt.Errorf("rmwbridge: client call send: %w", err)
		}
		return nil, fmt.Errorf("rmwbridge: client call send: %w", context.Canceled)
	case svc.requests <- out:
	}
	select {
	case <-ctx.Done():
		err := ctx.Err()
		if err != nil {
			return nil, fmt.Errorf("rmwbridge: client call wait: %w", err)
		}
		return nil, fmt.Errorf("rmwbridge: client call wait: %w", context.Canceled)
	case resp := <-reply:
		data := append([]byte(nil), resp...)
		return data, nil
	}
}

// Call sends one request and waits for the server reply.
//
// The caller context deadline passes through end to end: cancel or
// timeout aborts both the send and the reply wait. Payloads larger than
// maxPayloadBytes are rejected with ErrPayloadTooLarge without copying.
// It never panics and never truncates.
func (s *Service) Call(ctx context.Context, req []byte) ([]byte, error) {
	err := ctx.Err()
	if err != nil {
		return nil, fmt.Errorf("rmwbridge: service call: %w", err)
	}
	s.bridge.mu.Lock()
	bridgeClosed := s.bridge.closed
	s.bridge.mu.Unlock()
	if bridgeClosed {
		return nil, fmt.Errorf("rmwbridge: service call: %w", ErrBridgeClosed)
	}
	s.mu.Lock()
	svcClosed := s.closed
	s.mu.Unlock()
	if svcClosed {
		return nil, fmt.Errorf("rmwbridge: service call: %w", ErrServiceClosed)
	}
	if len(req) > maxPayloadBytes {
		return nil, fmt.Errorf("rmwbridge: service call: payload %d bytes exceeds %d bytes: %w", len(req), maxPayloadBytes, ErrPayloadTooLarge)
	}
	data := append([]byte(nil), req...)
	reply := make(chan []byte, 1)
	out := ServiceRequest{Payload: data, reply: reply}
	select {
	case <-ctx.Done():
		err := ctx.Err()
		if err != nil {
			return nil, fmt.Errorf("rmwbridge: service call send: %w", err)
		}
		return nil, fmt.Errorf("rmwbridge: service call send: %w", context.Canceled)
	case s.requests <- out:
	}
	select {
	case <-ctx.Done():
		err := ctx.Err()
		if err != nil {
			return nil, fmt.Errorf("rmwbridge: service call wait: %w", err)
		}
		return nil, fmt.Errorf("rmwbridge: service call wait: %w", context.Canceled)
	case resp := <-reply:
		data := append([]byte(nil), resp...)
		return data, nil
	}
}

// Serve waits for one request and returns its payload plus a responder.
//
// The responder sends one reply and must be called once per Serve. It
// honors ctx cancellation while waiting. The responder is non-blocking:
// the first call delivers the reply and returns nil, and a second call
// returns ErrDuplicateResponse instead of hanging. Reply payloads larger
// than maxPayloadBytes are rejected with ErrPayloadTooLarge without
// copying. It never panics and never truncates.
func (s *Service) Serve(ctx context.Context) ([]byte, func([]byte) error, error) {
	err := ctx.Err()
	if err != nil {
		return nil, nil, fmt.Errorf("rmwbridge: service serve: %w", err)
	}
	s.bridge.mu.Lock()
	bridgeClosed := s.bridge.closed
	s.bridge.mu.Unlock()
	if bridgeClosed {
		return nil, nil, fmt.Errorf("rmwbridge: service serve: %w", ErrBridgeClosed)
	}
	s.mu.Lock()
	svcClosed := s.closed
	s.mu.Unlock()
	if svcClosed {
		return nil, nil, fmt.Errorf("rmwbridge: service serve: %w", ErrServiceClosed)
	}
	select {
	case <-ctx.Done():
		err := ctx.Err()
		if err != nil {
			return nil, nil, fmt.Errorf("rmwbridge: service serve wait: %w", err)
		}
		return nil, nil, fmt.Errorf("rmwbridge: service serve wait: %w", context.Canceled)
	case out := <-s.requests:
		data := append([]byte(nil), out.Payload...)
		reply := out.reply
		respond := func(resp []byte) error {
			if len(resp) > maxPayloadBytes {
				return fmt.Errorf("rmwbridge: service respond: payload %d bytes exceeds %d bytes: %w", len(resp), maxPayloadBytes, ErrPayloadTooLarge)
			}
			cp := append([]byte(nil), resp...)
			select {
			case reply <- cp:
				return nil
			default:
				return fmt.Errorf("rmwbridge: service respond: duplicate response: %w", ErrDuplicateResponse)
			}
		}
		return data, respond, nil
	}
}

// NewGuard returns an untriggered guard.
func NewGuard() *Guard {
	return &Guard{wake: make(chan struct{}, 1)}
}

// Trigger sets the triggered flag and wakes one waiter.
func (g *Guard) Trigger() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.triggered = true
	select {
	case g.wake <- struct{}{}:
	default:
	}
}

// Reset clears the triggered flag and drains a pending wakeup.
func (g *Guard) Reset() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.triggered = false
	select {
	case <-g.wake:
	default:
	}
}

// IsTriggered reports whether the guard is triggered.
func (g *Guard) IsTriggered() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.triggered
}

// NewWaitSet returns an empty wait set.
func NewWaitSet() *WaitSet {
	return &WaitSet{}
}

// AddGuard adds a guard to the set.
//
// A nil guard is ignored.
func (w *WaitSet) AddGuard(g *Guard) {
	if g == nil {
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	w.guards = append(w.guards, g)
}

// Wait blocks until any guard triggers or ctx ends.
//
// It returns nil on trigger and the context error wrapped with context on
// timeout or cancellation. An empty set waits until ctx ends.
func (w *WaitSet) Wait(ctx context.Context) error {
	for {
		w.mu.Lock()
		snapshot := make([]*Guard, len(w.guards))
		copy(snapshot, w.guards)
		w.mu.Unlock()
		for _, g := range snapshot {
			if g == nil {
				continue
			}
			if g.IsTriggered() {
				return nil
			}
		}
		select {
		case <-ctx.Done():
			err := ctx.Err()
			if err != nil {
				return fmt.Errorf("rmwbridge: wait: %w", err)
			}
			return fmt.Errorf("rmwbridge: wait: %w", context.Canceled)
		case <-time.After(waitPollInterval):
		}
	}
}
