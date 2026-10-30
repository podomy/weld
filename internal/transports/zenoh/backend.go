// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package zenoh

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/podomy/weld/internal/transports"
)

// LivelinessPrefix is the hermetic reserved prefix for liveliness keys.
//
// Wildcards never match this prefix. All liveliness traffic stays under
// it so discovery tokens cannot leak into data key space.
const LivelinessPrefix = "@ros2_lv"

// AttachmentSize is the encoded attachment length in bytes.
//
// Layout is 8 byte sequence plus 8 byte source timestamp in nanoseconds
// plus 16 byte source GID.
const AttachmentSize = 32

// FirstSequenceNumber is the first publisher sequence number.
//
// Publisher and client sequences start at 1. Zero never appears on the
// wire and marshal plus parse reject it.
const FirstSequenceNumber uint64 = 1

// DDSMarker separates the namespace from the type name in DDS type names.
const DDSMarker = "::dds_::"

// TypeSuffix is the trailing underscore on every DDS type name.
const TypeSuffix = "_"

// ErrZenohNotImplemented reports stubbed network I/O.
//
// Pure helpers for keys, names, and attachments work now. Session open,
// publish send, and take receive need the Zenoh client library and
// return this error after lifecycle checks pass.
var ErrZenohNotImplemented = errors.New("zenoh: client library absent, network I/O not implemented")

// Config configures a Zenoh backend.
//
// DomainID is the caller domain joined when ROS_DOMAIN_ID carries no
// override. Actual domain resolution happens in NewBackend through
// transports.ResolveDomainID so zero means domain zero unless the
// environment overrides it. Session endpoint plus router plus TLS knobs
// land with the client library and are not part of this skeleton.
type Config struct {
	// DomainID is the caller domain used when the environment is unset.
	DomainID uint32
}

// DefaultConfig returns a Config that joins domain zero.
func DefaultConfig() Config {
	return Config{DomainID: 0}
}

// Validate checks the config for usable values.
func (c Config) Validate() error {
	if c.DomainID > transports.MaxDomainID {
		return fmt.Errorf("zenoh: domain id %d above maximum %d", c.DomainID, transports.MaxDomainID)
	}

	return nil
}

// Backend is the Zenoh lifecycle plus naming skeleton.
//
// One Backend owns one logical Zenoh session for one ROS context.
// Lifecycle order follows the transports.Backend contract: JoinDiscovery
// then PublishBytes plus TakeBytes plus ReportLiveliness then Close.
// PublishBytes and TakeBytes check lifecycle first and then report
// ErrZenohNotImplemented because the client library is absent.
// ReportLiveliness tracks the generation watermark locally with no
// network. Close is idempotent. The struct is safe for concurrent use.
type Backend struct {
	mu sync.RWMutex
	// domainID is the resolved domain from Config.
	domainID uint32
	// participant is the last joined participant name.
	participant string
	// joined is true after a successful JoinDiscovery.
	joined bool
	// closed is true after the first Close.
	closed bool
	// lastGen holds the liveliness watermark per participant.
	lastGen map[string]uint64
	// lastAlive holds the last aliveness per participant.
	lastAlive map[string]bool
	// lastSeen holds the last report time per participant.
	lastSeen map[string]time.Time
	// seq is the next attachment sequence number.
	seq uint64
	// gid is the placeholder source GID derived at join.
	gid [16]byte
}

// NewBackend validates the config and returns a lifecycle Backend.
func NewBackend(cfg Config) (*Backend, error) {
	err := cfg.Validate()
	if err != nil {
		return nil, fmt.Errorf("zenoh: invalid backend config: %w", err)
	}

	resolved, err := transports.ResolveDomainID(cfg.DomainID)
	if err != nil {
		return nil, fmt.Errorf("zenoh: resolve domain: %w", err)
	}

	return &Backend{
		domainID:  resolved,
		lastGen:   make(map[string]uint64),
		lastAlive: make(map[string]bool),
		lastSeen:  make(map[string]time.Time),
		seq:       FirstSequenceNumber,
	}, nil
}

// Config returns a copy of the backend configuration.
func (b *Backend) Config() Config {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return Config{DomainID: b.domainID}
}

// DomainID returns the resolved domain id.
func (b *Backend) DomainID() uint32 {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return b.domainID
}

// Joined reports whether JoinDiscovery has succeeded.
func (b *Backend) Joined() bool {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return b.joined
}

// Closed reports whether Close has been called.
func (b *Backend) Closed() bool {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return b.closed
}

// Participant returns the joined participant name.
func (b *Backend) Participant() string {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return b.participant
}

// SourceGID returns the placeholder source GID derived at join.
func (b *Backend) SourceGID() [16]byte {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return b.gid
}

// StripFQN removes leading plus trailing slashes from a ROS topic name.
//
// Only slash stripping applies. There is no percent mangling here,
// unlike liveliness names where slash maps to percent. Inner slashes
// stay so /a/b becomes a/b. Empty input plus slash only input plus
// double slash segments plus inner whitespace are rejected.
func StripFQN(raw string) (string, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return "", errors.New("zenoh: topic name is empty") //nolint:wrapcheck // new error, nothing to wrap
	}

	if strings.ContainsAny(trimmed, " \t\n\r") {
		return "", fmt.Errorf("zenoh: topic name %q has whitespace", raw)
	}

	stripped := strings.Trim(trimmed, "/")
	if stripped == "" {
		return "", fmt.Errorf("zenoh: topic name %q is empty", raw)
	}

	if strings.Contains(stripped, "//") {
		return "", fmt.Errorf("zenoh: topic name %q has an empty segment", raw)
	}

	return stripped, nil
}

// splitIDL splits an IDL name on double underscores and validates parts.
func splitIDL(idlName string) ([]string, error) {
	parts := strings.Split(idlName, "__")
	if len(parts) < 3 {
		return nil, fmt.Errorf("zenoh: idl name %q needs at least pkg__group__Name", idlName)
	}

	for _, part := range parts {
		if strings.TrimSpace(part) == "" {
			return nil, fmt.Errorf("zenoh: idl name %q has an empty segment", idlName)
		}
	}

	return parts, nil
}

// IDLToDDSType maps an IDL message name to its FastRTPS DDS type name.
//
// It maps double underscores to scope separators and forms
// <ns>::dds_::<Name>_ . For example std_msgs__msg__String maps to
// std_msgs::msg::dds_::String_ . Service suffix handling lands with
// service support and is not part of this skeleton.
func IDLToDDSType(idlName string) (string, error) {
	parts, err := splitIDL(strings.TrimSpace(idlName))
	if err != nil {
		return "", err
	}

	ns := strings.Join(parts[:len(parts)-1], "::")
	name := parts[len(parts)-1]

	return ns + DDSMarker + name + TypeSuffix, nil
}

// DataKey builds the Zenoh data key for a topic.
//
// Format is <domain>/<stripped_fqn>/<dds_type>/<type_hash> where domain
// is decimal actual domain id, stripped_fqn removes leading plus
// trailing slashes only, dds_type is the FastRTPS DDS name, and
// type_hash is RIHS01_ hex. An empty hash yields the hashless
// <domain>/<stripped_fqn>/<dds_type> form for Humble peers without
// hashes. Mismatch on any segment addresses a different key with no
// separate type negotiation.
func DataKey(domain uint32, topic, ddsType, typeHash string) (string, error) {
	if domain > transports.MaxDomainID {
		return "", fmt.Errorf("zenoh: domain id %d above maximum %d", domain, transports.MaxDomainID)
	}

	stripped, err := StripFQN(topic)
	if err != nil {
		return "", fmt.Errorf("zenoh: data key topic: %w", err)
	}

	trimmedType := strings.TrimSpace(ddsType)
	if trimmedType == "" {
		return "", errors.New("zenoh: data key type name is empty") //nolint:wrapcheck // new error, nothing to wrap
	}

	if strings.ContainsAny(trimmedType, " \t\n\r/") {
		return "", fmt.Errorf("zenoh: data key type %q has whitespace or slash", ddsType)
	}

	hash := strings.TrimSpace(typeHash)
	if hash != "" && strings.ContainsAny(hash, " \t\n\r/") {
		return "", fmt.Errorf("zenoh: data key hash %q has whitespace or slash", typeHash)
	}

	domainStr := strconv.FormatUint(uint64(domain), 10)
	if hash == "" {
		return domainStr + "/" + stripped + "/" + trimmedType, nil
	}

	return domainStr + "/" + stripped + "/" + trimmedType + "/" + hash, nil
}

// KeyFor builds the data key for this backend domain with empty hash.
//
// OutgoingMessage in v1 carries no type hash, so publish uses the
// hashless form until the type registry lands. Callers that know the
// hash call DataKey directly.
func (b *Backend) KeyFor(topic, idlType string) (string, error) {
	ddsType, err := IDLToDDSType(idlType)
	if err != nil {
		return "", fmt.Errorf("zenoh: key type: %w", err)
	}

	b.mu.RLock()
	domain := b.domainID
	b.mu.RUnlock()

	key, err := DataKey(domain, topic, ddsType, "")
	if err != nil {
		return "", err
	}

	return key, nil
}

// MangleLiveliness maps a liveliness name segment to token form.
//
// Slash becomes percent. Empty input maps to percent for empty
// namespace plus empty enclave. ROS names never contain percent so the
// mapping has no collision on valid input.
func MangleLiveliness(s string) string {
	if s == "" {
		return "%"
	}

	return strings.ReplaceAll(s, "/", "%")
}

// UnmangleLiveliness maps a token segment back to liveliness name form.
//
// Percent becomes slash. A lone percent maps back to empty. Inputs with
// literal percent outside valid ROS names are caller errors and still
// map percent to slash.
func UnmangleLiveliness(s string) string {
	if s == "%" {
		return ""
	}

	return strings.ReplaceAll(s, "%", "/")
}

// LivelinessDomainPrefix returns the liveliness prefix for a domain.
func LivelinessDomainPrefix(domain uint32) string {
	return LivelinessPrefix + "/" + strconv.FormatUint(uint64(domain), 10)
}

// LivelinessFilter returns the subscription filter for a domain.
func LivelinessFilter(domain uint32) string {
	return LivelinessDomainPrefix(domain) + "/**"
}

// LivelinessKey builds the entity token key for a participant.
//
// Format is @ros2_lv/<domain>/<mangled_participant> where the
// participant segment uses percent mangling. Empty participant maps to
// percent. Full entity tokens with namespace plus enclave segments land
// with graph support and extend this two segment form.
func LivelinessKey(domain uint32, participant string) (string, error) {
	if domain > transports.MaxDomainID {
		return "", fmt.Errorf("zenoh: domain id %d above maximum %d", domain, transports.MaxDomainID)
	}

	mangled := MangleLiveliness(strings.TrimSpace(participant))

	return LivelinessDomainPrefix(domain) + "/" + mangled, nil
}

// Token returns the liveliness key for the joined participant.
func (b *Backend) Token() (string, error) {
	b.mu.RLock()
	domain := b.domainID
	participant := b.participant
	b.mu.RUnlock()

	key, err := LivelinessKey(domain, participant)
	if err != nil {
		return "", err
	}

	return key, nil
}

// Attachment is the Zenoh message attachment.
//
// Sequence starts at 1 per publisher GID. TimestampNs is source time in
// nanoseconds. SourceGID identifies the publisher. Wire order is
// sequence plus timestamp plus GID in little endian for integers.
type Attachment struct {
	// Sequence is the per publisher sequence number starting at 1.
	Sequence uint64
	// TimestampNs is the source timestamp in nanoseconds.
	TimestampNs uint64
	// SourceGID is the 16 byte publisher GID.
	SourceGID [16]byte
}

// Marshal encodes the attachment to 32 bytes.
func (a Attachment) Marshal() ([]byte, error) {
	if a.Sequence == 0 {
		return nil, errors.New("zenoh: attachment sequence is zero") //nolint:wrapcheck // new error, nothing to wrap
	}

	out := make([]byte, AttachmentSize)
	binary.LittleEndian.PutUint64(out[0:8], a.Sequence)
	binary.LittleEndian.PutUint64(out[8:16], a.TimestampNs)
	copy(out[16:32], a.SourceGID[:])

	return out, nil
}

// ParseAttachment decodes 32 attachment bytes.
func ParseAttachment(raw []byte) (Attachment, error) {
	var empty Attachment
	if len(raw) != AttachmentSize {
		return empty, fmt.Errorf("zenoh: attachment length %d wants %d", len(raw), AttachmentSize)
	}

	seq := binary.LittleEndian.Uint64(raw[0:8])
	if seq == 0 {
		return empty, errors.New("zenoh: attachment sequence is zero") //nolint:wrapcheck // new error, nothing to wrap
	}

	ts := binary.LittleEndian.Uint64(raw[8:16])

	var gid [16]byte
	copy(gid[:], raw[16:32])

	return Attachment{Sequence: seq, TimestampNs: ts, SourceGID: gid}, nil
}

// derivePlaceholderGID derives a deterministic 16 byte GID from a key.
//
// This is a skeleton stand in using two FNV-1a 64 passes with different
// seeds. The wire derives GID as XXH3-128 of the liveliness key
// expression split low plus high 64 bits. That lands with the client
// library. Callers must treat this value as opaque test routing only.
func derivePlaceholderGID(key string) [16]byte {
	const offset uint64 = 14695981039346656037
	const prime uint64 = 1099511628211
	const seedXor uint64 = 0x9e3779b97f4a7c15

	h1 := offset
	h2 := offset ^ seedXor

	for i := range len(key) {
		b := key[i]
		h1 ^= uint64(b)
		h1 *= prime
		h2 ^= uint64(b)
		h2 *= prime
	}

	var gid [16]byte
	binary.LittleEndian.PutUint64(gid[0:8], h1)
	binary.LittleEndian.PutUint64(gid[8:16], h2)

	return gid
}

// nextSequence claims the next attachment sequence number.
func (b *Backend) nextSequence() uint64 {
	b.mu.Lock()
	defer b.mu.Unlock()

	seq := b.seq
	b.seq++

	if b.seq == 0 {
		b.seq = FirstSequenceNumber
	}

	return seq
}

// buildAttachment claims a sequence and stamps source time plus GID.
func (b *Backend) buildAttachment() Attachment {
	seq := b.nextSequence()

	b.mu.RLock()
	gid := b.gid
	b.mu.RUnlock()

	return Attachment{
		Sequence:    seq,
		TimestampNs: uint64(time.Now().UnixNano()),
		SourceGID:   gid,
	}
}

// JoinDiscovery joins discovery with the given participant info.
//
// One Backend owns one logical session for its ROS context. Join records
// the participant, derives the placeholder source GID from the
// liveliness key, and marks joined. It is idempotent: repeat joins
// return nil. Actual session open plus liveliness subscriber plus
// snapshot get land with the client library. After Close it reports
// ErrBackendClosed.
func (b *Backend) JoinDiscovery(ctx context.Context, info transports.DiscoveryInfo) error {
	err := ctx.Err()
	if err != nil {
		return fmt.Errorf("zenoh: join discovery: %w", err)
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	if b.closed {
		return fmt.Errorf("zenoh: join discovery after close: %w", transports.ErrBackendClosed)
	}

	if b.joined {
		return nil
	}

	key, err := LivelinessKey(b.domainID, info.ParticipantName)
	if err != nil {
		return err
	}

	b.participant = info.ParticipantName
	b.gid = derivePlaceholderGID(key)
	b.joined = true

	return nil
}

// PublishBytes publishes one serialized message.
//
// It validates topic plus IDL type, claims an attachment sequence, and
// then reports ErrZenohNotImplemented because send needs the client
// library. The stub retains nothing so no payload copy is kept. It
// requires a prior JoinDiscovery and reports ErrNotJoined otherwise.
// After Close it reports ErrBackendClosed. Nil payload means empty.
func (b *Backend) PublishBytes(ctx context.Context, msg transports.OutgoingMessage) error {
	err := ctx.Err()
	if err != nil {
		return fmt.Errorf("zenoh: publish: %w", err)
	}

	b.mu.RLock()
	closed := b.closed
	joined := b.joined
	b.mu.RUnlock()

	if closed {
		return fmt.Errorf("zenoh: publish after close: %w", transports.ErrBackendClosed)
	}

	if !joined {
		return fmt.Errorf("zenoh: publish before join: %w", transports.ErrNotJoined)
	}

	key, err := b.KeyFor(msg.TopicName, msg.TypeName)
	if err != nil {
		return err
	}

	attachment := b.buildAttachment()

	_, err = attachment.Marshal()
	if err != nil {
		return err
	}

	return fmt.Errorf("zenoh: publish %q seq %d: %w", key, attachment.Sequence, ErrZenohNotImplemented)
}

// TakeBytes receives one serialized message.
//
// The live path subscribes with history on the data key expression and
// raises message lost on attachment sequence gaps per publisher GID.
// The stub has no subscriber until the client library lands so it
// reports ErrZenohNotImplemented after lifecycle checks. It requires a
// prior JoinDiscovery and reports ErrNotJoined otherwise. After Close
// it reports ErrBackendClosed.
func (b *Backend) TakeBytes(ctx context.Context) (transports.IncomingMessage, error) {
	err := ctx.Err()
	if err != nil {
		return transports.IncomingMessage{}, fmt.Errorf("zenoh: take: %w", err)
	}

	b.mu.RLock()
	closed := b.closed
	joined := b.joined
	b.mu.RUnlock()

	if closed {
		return transports.IncomingMessage{}, fmt.Errorf("zenoh: take after close: %w", transports.ErrBackendClosed)
	}

	if !joined {
		return transports.IncomingMessage{}, fmt.Errorf("zenoh: take before join: %w", transports.ErrNotJoined)
	}

	return transports.IncomingMessage{}, fmt.Errorf("zenoh: take: %w", ErrZenohNotImplemented)
}

// ReportLiveliness reports local endpoint liveliness.
//
// The skeleton tracks the generation watermark locally with the same
// rules as the Backend contract: greater generation advances, repeat
// generation with equal alive is idempotent, otherwise stale reports
// ErrStaleGeneration. Token declare plus undeclare on the liveliness
// prefix lands with the client library. It requires a prior
// JoinDiscovery and reports ErrNotJoined otherwise. After Close it
// reports ErrBackendClosed.
func (b *Backend) ReportLiveliness(ctx context.Context, report transports.LivelinessReport) error {
	err := ctx.Err()
	if err != nil {
		return fmt.Errorf("zenoh: report liveliness: %w", err)
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	if b.closed {
		return fmt.Errorf("zenoh: report liveliness after close: %w", transports.ErrBackendClosed)
	}

	if !b.joined {
		return fmt.Errorf("zenoh: report liveliness before join: %w", transports.ErrNotJoined)
	}

	last, seen := b.lastGen[report.ParticipantName]
	if !seen {
		b.lastGen[report.ParticipantName] = report.Generation
		b.lastAlive[report.ParticipantName] = report.Alive
		b.lastSeen[report.ParticipantName] = time.Now()

		return nil
	}

	if report.Generation > last {
		b.lastGen[report.ParticipantName] = report.Generation
		b.lastAlive[report.ParticipantName] = report.Alive
		b.lastSeen[report.ParticipantName] = time.Now()

		return nil
	}

	if report.Generation == last && report.Alive == b.lastAlive[report.ParticipantName] {
		return nil
	}

	return fmt.Errorf(
		"zenoh: stale generation %d for %q: %w",
		report.Generation,
		report.ParticipantName,
		transports.ErrStaleGeneration,
	)
}

// Close releases the backend handle.
//
// It is idempotent. The first call marks closed and later calls return
// nil. The skeleton holds no session so there is no socket to shut.
// The live path cancels the session plus subscriber here.
func (b *Backend) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.closed {
		return nil
	}

	b.closed = true

	return nil
}

// Compile time check that Backend exposes the Backend contract.
var _ transports.Backend = (*Backend)(nil)
