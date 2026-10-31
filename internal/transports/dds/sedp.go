// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

// Package dds holds the CycloneDDS sidecar contract.
//
// This file implements SEDP endpoint discovery for first contact. It
// tracks local readers and writers, exchanges RTPS DATA samples with
// builtin SEDP writers, and matches local readers against remote
// writers. The index is partition local and domain scoped: matching
// requires the same domain and never assumes a global graph.
//
// Each endpoint announcement is one RTPS message: the RTPS header with
// the participant prefix plus a single DATA submessage from a builtin
// SEDP writer. Publications use writer entity 0x000002c2 and
// subscriptions use writer entity 0x000004c2, with inline QoS PID
// params carrying the topic name, type name, endpoint GUID,
// reliable transient local QoS with keep-last history, protocol version,
// vendor id, and userdata with the type hash. Parsing
// accepts the same shape from any peer and additionally tolerates the
// observed CycloneDDS wire form, which carries endpoint params in the
// DATA payload instead of inline QoS and announces publications from
// writer entity 0x000003c2. See the capture notes on the Cyclone
// fixture tests for the observed values.
//
// Names here carry an SEDP prefix so parallel SPDP and datapath work in
// the same package keeps its own GUID, Sender, and Receiver types.
package dds

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/podomy/weld/internal/core"
	"github.com/podomy/weld/internal/transports/dds/rtps"
)

// SEDP RTPS framing constants.
const (
	// sedpRTPSMagic is the 4 byte RTPS protocol identifier.
	sedpRTPSMagic = "RTPS"
	// sedpRTPSHeaderLen is the fixed 20 byte RTPS message header.
	sedpRTPSHeaderLen = 20
	// sedpSubmessageHeaderLen is the fixed 4 byte submessage header.
	sedpSubmessageHeaderLen = 4
	// sedpDataFixedLen is the fixed DATA body prefix before inline QoS.
	sedpDataFixedLen = 20
	// sedpOctetsToInlineQoS is the standard offset written from reader id
	// to inline QoS. Parsing ignores the field and walks sequentially.
	sedpOctetsToInlineQoS = 16
	// sedpRTPSMajor is the only accepted RTPS major version.
	sedpRTPSMajor byte = 2
	// sedpRTPSMinor is the sent RTPS minor version. Peers may send any
	// 2.x minor; decode accepts every minor under major 2.
	sedpRTPSMinor byte = 1
	// sedpVendorID0 is the first sent vendor id byte, Eclipse CycloneDDS.
	sedpVendorID0 byte = 0x01
	// sedpVendorID1 is the second sent vendor id byte, Eclipse CycloneDDS.
	sedpVendorID1 byte = 0x10
	// sedpSubmessageDATA selects DATA submessages.
	sedpSubmessageDATA byte = 0x15
	// sedpFlagLittleEndian selects little endian order for the length
	// and content that follow the submessage header.
	sedpFlagLittleEndian byte = 0x01
	// sedpFlagInlineQoS marks inline QoS params present in DATA.
	sedpFlagInlineQoS byte = 0x02
	// sedpFlagDataPresent marks a serialized payload present in DATA.
	sedpFlagDataPresent byte = 0x04
	// sedpFlagKeyPresent marks a serialized key present in DATA.
	sedpFlagKeyPresent byte = 0x08
	// sedpMaxDomainID mirrors transports.MaxDomainID for validation.
	sedpMaxDomainID uint32 = 101
	// sedpUserEntityBase is the first 24 bit counter for user entities.
	sedpUserEntityBase uint32 = 0x001000
	// sedpUserEntityMax is the largest 24 bit counter for user entities.
	sedpUserEntityMax uint32 = 0xFFFFFF
	// sedpUserWriterKind is the RTPS user writer kind for keyless topics.
	sedpUserWriterKind byte = 0x03
	// sedpUserReaderKind is the RTPS user reader kind for keyless topics.
	sedpUserReaderKind byte = 0x04
	// sedpAnnouncePollInterval polls local endpoints for announce changes.
	sedpAnnouncePollInterval = 200 * time.Millisecond
	// sedpMaxNameLen caps one topic, type, or hash string on the wire.
	sedpMaxNameLen = 1024
	// sedpMaxParams caps one decoded parameter list for bounded work.
	sedpMaxParams = 128
	// sedpMaxBodyLen caps one DATA body to the uint16 submessage length.
	sedpMaxBodyLen = 0xFFFF
	// sedpDATAReaderOff is the reader entity offset in a DATA body.
	sedpDATAReaderOff = 4
	// sedpDATAWriterOff is the writer entity offset in a DATA body.
	sedpDATAWriterOff = 8
	// sedpDATASeqOff is the sequence number offset in a DATA body.
	sedpDATASeqOff = 12
	// sedpSeqLen is the 8 byte sequence number length.
	sedpSeqLen = 8
	// sedpEntityLen is one RTPS entity id length.
	sedpEntityLen = 4
	// sedpGUIDLen is one endpoint GUID length.
	sedpGUIDLen = 16
	// sedpPrefixLen is one participant prefix length.
	sedpPrefixLen = 12
	// sedpEncapLen is the CDR encapsulation prefix length.
	sedpEncapLen = 4
	// sedpLengthPrefixLen is the 4 byte CDR length prefix.
	sedpLengthPrefixLen = 4
	// sedpSubmessageFlagsOff is the flags offset in a submessage header.
	sedpSubmessageFlagsOff = 1
	// sedpSubmessageLengthOff is the length offset in a submessage header.
	sedpSubmessageLengthOff = 2
	// sedpRTPSMajorOff is the major version offset in an RTPS header.
	sedpRTPSMajorOff = 4
	// sedpDATAReserved is the zero reserved octet starting a DATA body.
	sedpDATAReserved byte = 0x00
)

// sedpHeartbeatKind gathers RTPS Heartbeat constants for reliable discovery.
const (
	// sedpSubmessageHeartbeat selects HEARTBEAT submessages.
	sedpSubmessageHeartbeat byte = 0x07
	// sedpSubmessageAckNack selects ACKNACK submessages.
	sedpSubmessageAckNack byte = 0x06
	// sedpHeartbeatSize is the fixed 28 byte HEARTBEAT content size.
	sedpHeartbeatSize = 28
	// sedpHeartbeatWriterOff is the writer id offset in a HEARTBEAT body.
	sedpHeartbeatWriterOff = 4
	// sedpHeartbeatFirstSeq is the first available sequence announced.
	sedpHeartbeatFirstSeq uint64 = 1
	// sedpAnnounceResendInterval resends SEDP DATA with fresh sequences.
	sedpAnnounceResendInterval = 1 * time.Second
	// sedpProvisionalTTL bounds a provisional placeholder lifetime.
	//
	// Thirty seconds is three times the SPDP default lease and thirty
	// times the SEDP resend interval, so a confirmed announce has ample
	// time to promote the placeholder while a stale fast-path entry
	// cannot linger an order above the lease.
	sedpProvisionalTTL = 30 * time.Second
)

// SEDP builtin endpoint entities.
//
// Publications carry writer announcements and subscriptions carry reader
// announcements. CycloneDDS announces publications from 0x000003c2, which
// differs from the spec value 0x000002c2. This package sends the Cyclone
// variant for interop and parses both. Reader ids are always sent as
// unknown and any received reader id is accepted.
var (
	// sedpPublicationsWriter is the spec SEDP publications writer, kept
	// for parsing peers that still send it.
	sedpPublicationsWriter = SEDPEntityID{0x00, 0x00, 0x02, 0xC2}
	// sedpPublicationsWriterCyclone is the sent SEDP publications writer,
	// matching the observed CycloneDDS wire form.
	sedpPublicationsWriterCyclone = SEDPEntityID{0x00, 0x00, 0x03, 0xC2}
	// sedpSubscriptionsWriter is the SEDP subscriptions writer.
	sedpSubscriptionsWriter = SEDPEntityID{0x00, 0x00, 0x04, 0xC2}
	// sedpEntityUnknown is the sent DATA reader id, also the zero GUID.
	sedpEntityUnknown = SEDPEntityID{}
)

// Inline QoS reliability and durability values for reliable transient local.
const (
	// sedpReliabilityReliable is the DDS reliability kind for reliable.
	sedpReliabilityReliable uint32 = 2
	// sedpReliabilityInfiniteSec is the infinite blocking seconds.
	sedpReliabilityInfiniteSec uint32 = 0x7FFFFFFF
	// sedpReliabilityInfiniteFrac is the infinite blocking fraction.
	sedpReliabilityInfiniteFrac uint32 = 0xFFFFFFFF
	// sedpDurabilityVolatile is the DDS durability kind for volatile.
	sedpDurabilityVolatile uint32 = 0
	// sedpDurabilityTransientLocal is the DDS durability kind for transient local.
	sedpDurabilityTransientLocal uint32 = 1
	// sedpHistoryKeepLast is the DDS history kind for keep last.
	sedpHistoryKeepLast uint32 = 0
	// sedpHistoryDepth is the keep last depth matching the facade offer.
	// History kind and depth stay RxO neutral for matching.
	sedpHistoryDepth uint32 = 100
)

// CDR encapsulation kinds for parameter list DATA payloads.
//
// The kind is an octet pair in fixed order, not subject to the DATA
// endianness. Plain CDR kinds 0x0000 and 0x0001 stay opaque; only the
// parameter list kinds below are parsed.
const (
	// sedpEncapPLCDRBE marks a big endian parameter list payload.
	sedpEncapPLCDRBE uint16 = 0x0002
	// sedpEncapPLCDRLE marks a little endian parameter list payload.
	sedpEncapPLCDRLE uint16 = 0x0003
)

// Userdata keys carried in the USER_DATA octet sequence.
const (
	// sedpUserdataKeyTypeHash carries the type hash passthrough.
	sedpUserdataKeyTypeHash = "typehash"
	// sedpUserdataKeyDDSTopic carries the DDS topic echo for Weld peers.
	sedpUserdataKeyDDSTopic = "dds_topic"
	// sedpUserdataKeyDomain carries the domain id for Weld peers.
	sedpUserdataKeyDomain = "domain"
)

// StringTypeHash is the interop type hash for std_msgs/String on /chatter.
const StringTypeHash = "RIHS01_df668c740482bbd48fb39d76a70dfd4bd59db1288021743503259e948f6b1a18"

// Sentinel errors for explicit SEDP failures.
var (
	// ErrSEDPInvalidDomain reports a domain id above the ROS maximum.
	ErrSEDPInvalidDomain = errors.New("dds: invalid sedp domain")
	// ErrSEDPInvalidTopic reports a bad topic name or topic mapping.
	ErrSEDPInvalidTopic = errors.New("dds: invalid sedp topic")
	// ErrSEDPInvalidType reports a bad type name or type mapping.
	ErrSEDPInvalidType = errors.New("dds: invalid sedp type")
	// ErrSEDPInvalidEndpoint reports a bad endpoint record.
	ErrSEDPInvalidEndpoint = errors.New("dds: invalid sedp endpoint")
	// ErrSEDPInvalidDATA reports a malformed SEDP DATA sample.
	ErrSEDPInvalidDATA = errors.New("dds: invalid sedp data")
	// ErrSEDPUnknownEndpoint reports a GUID with no local endpoint.
	ErrSEDPUnknownEndpoint = errors.New("dds: unknown sedp endpoint")
)

// sedpWriterSeq tracks the last DATA sequence per builtin SEDP writer.
//
// Sequence numbers order samples per writer GUID, so each builtin writer
// (publications 0x000003c2 and subscriptions 0x000004c2) needs its own
// contiguous stream starting at 1 for every participant prefix. One
// process wide counter shared by both writers gives every stream gaps
// (1, 3, 5 interleaved), and reliable SEDP readers such as CycloneDDS
// hold the out-of-order samples while Weld never repairs SEDP, so
// endpoints randomly never appear. The map keys on participant prefix
// plus writer entity so concurrent backends keep independent streams.
var sedpWriterSeq sync.Mutex

// sedpWriterSeqLast holds the per writer sequence state.
var sedpWriterSeqLast = map[sedpWriterKey]uint64{}

// sedpWriterKey identifies one builtin SEDP writer stream.
type sedpWriterKey struct {
	// prefix is the participant GUID prefix.
	prefix SEDPGUIDPrefix
	// writer is the builtin writer entity id.
	writer SEDPEntityID
}

// heartbeatCount counts sent SEDP and user DATA HEARTBEAT submessages process wide.
var heartbeatCount atomic.Int32

// SEDPGUIDPrefix is the 12 byte RTPS participant prefix for SEDP.
type SEDPGUIDPrefix [12]byte

// SEDPEntityID is the 4 byte RTPS entity id with kind in the last octet.
type SEDPEntityID [4]byte

// SEDPGUID pairs a participant prefix with an entity id.
type SEDPGUID struct {
	// Prefix identifies the participant.
	Prefix SEDPGUIDPrefix
	// Entity identifies the endpoint within the participant.
	Entity SEDPEntityID
}

// String returns the GUID as hex prefix and hex entity.
func (g SEDPGUID) String() string {
	prefix := hex.EncodeToString(g.Prefix[:])
	entity := hex.EncodeToString(g.Entity[:])

	return prefix + ":" + entity
}

// Equal reports whether two GUIDs match.
func (g SEDPGUID) Equal(other SEDPGUID) bool {
	return g.Prefix == other.Prefix && g.Entity == other.Entity
}

// SEDPEndpoint is one discovered reader or writer.
//
// XTypes TypeObject rides as opaque passthrough for v1: the raw TypeObject
// bytes are parsed and stored when present, but full TypeObject lookup with
// dependency resolution and validation is out of scope. Matching still uses
// TypeHash from USER_DATA.
type SEDPEndpoint struct {
	// ID is the GUID for upsert and remove by identity.
	ID SEDPGUID
	// ROSTopic is the ROS name such as /chatter.
	ROSTopic string
	// DDSTopic is the DDS name such as rt/chatter.
	DDSTopic string
	// Kind is the reverse mapped kind from ParseTopicName.
	Kind TopicKind
	// TypeDDS is the DDS type name on the wire.
	TypeDDS string
	// TypeIDL is the demangled IDL name.
	TypeIDL string
	// TypeHash is the passthrough type hash, empty for pre hash peers.
	TypeHash string
	// QoS is the passthrough QoS offer bytes, copied on store.
	QoS []byte
	// IsReader is true for readers and false for writers.
	IsReader bool
	// ServiceID is the hex service identifier announced in USER_DATA.
	//
	// ROS matches reader plus writer pairs to one service through this
	// identifier, so service endpoints always carry one derived from the
	// ROS service name. Empty means a pre service ID peer that never
	// announced one.
	ServiceID string
	// ClientID is the client identifier announced by ROS peers under the
	// clientid key, empty when absent. Stored so availability checks can
	// pair halves without another parse.
	ClientID string
	// VolatileDurability requests volatile durability instead of the
	// default transient local. All service endpoints announce volatile to
	// match ROS, which offers volatile on service readers and writers:
	// a transient local request against a volatile offer is RxO
	// incompatible, while volatile against volatile always matches.
	VolatileDurability bool
	// DomainID is the domain the endpoint belongs to.
	DomainID uint32
	// BuiltinEndpointQos is the builtin endpoint QoS mask, zero when absent.
	BuiltinEndpointQos uint32
	// TypeObject holds the raw XTypes TypeObject bytes when present, nil otherwise.
	TypeObject []byte
	// Provisional marks a datapath fast-path placeholder not yet confirmed by SEDP.
	Provisional bool
	// FirstSeen is when the provisional placeholder was installed, zero for confirmed.
	FirstSeen time.Time
	// Security carries the parsed DDS-Security attributes for the
	// endpoint, zero when the peer announced none. Secured endpoints are
	// detected, never decrypted.
	Security SecurityInfo
	// Partitions lists the DDS partitions the endpoint joins, empty for
	// the default partition. Matching requires a common partition.
	Partitions []string
	// DurabilityService carries the parsed durability-service caps, zero
	// when the peer announced none. It bounds late-joiner replay.
	DurabilityService DurabilityServiceQoS
	// HasDurabilityService reports whether durability-service was present.
	HasDurabilityService bool
	// WireOffer carries the per-profile translated wire QoS for locally
	// declared endpoints, nil for the facade default. SerializeSEDPDATA
	// announces WireOffer params when set and facade params otherwise,
	// so existing paths keep byte-identical wire output.
	WireOffer *WireQoS
}

// SEDPSender sends SEDP DATA bytes toward the discovery bus.
type SEDPSender interface {
	// SendSEDP sends one serialized DATA sample.
	SendSEDP(ctx context.Context, data []byte) error
}

// SEDPReceiver receives SEDP DATA bytes from the discovery bus.
type SEDPReceiver interface {
	// ReceiveSEDP blocks until one serialized DATA sample arrives.
	ReceiveSEDP(ctx context.Context) ([]byte, error)
}

// SEDPIndex tracks local endpoints and remote observations.
//
// Generation counts remote mutations only: each upsert that adds or
// changes an entry and each remove that deletes one bumps the counter
// and fires the change hook outside the lock. Local declares do not
// affect the generation. Use NewSEDPIndex. It is safe for concurrent use.
type SEDPIndex struct {
	mu         sync.Mutex
	prefix     SEDPGUIDPrefix
	domain     uint32
	next       uint32
	locals     map[SEDPGUID]SEDPEndpoint
	remotes    map[SEDPGUID]SEDPEndpoint
	generation uint64
	onChange   func(uint64)
}

// NewSEDPIndex returns an SEDP index for one participant prefix and domain.
func NewSEDPIndex(prefix SEDPGUIDPrefix, domain uint32) (*SEDPIndex, error) {
	if domain > sedpMaxDomainID {
		return nil, fmt.Errorf("dds: sedp domain %d above maximum %d: %w", domain, sedpMaxDomainID, ErrSEDPInvalidDomain)
	}

	return &SEDPIndex{
		prefix:  prefix,
		domain:  domain,
		next:    sedpUserEntityBase,
		locals:  make(map[SEDPGUID]SEDPEndpoint),
		remotes: make(map[SEDPGUID]SEDPEndpoint),
	}, nil
}

// SetOnChange installs the hook fired with each new generation.
func (idx *SEDPIndex) SetOnChange(fn func(uint64)) {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	idx.onChange = fn
}

// Generation returns the current remote mutation generation.
func (idx *SEDPIndex) Generation() uint64 {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	return idx.generation
}

// stampDomain returns the index domain for ListenLoop stamping.
func (idx *SEDPIndex) stampDomain() uint32 {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	return idx.domain
}

// DeclareWriter declares a local writer for a ROS topic.
func (idx *SEDPIndex) DeclareWriter(rosTopic, idlName, typeHash string, qos []byte) (SEDPEndpoint, error) {
	ep, err := idx.declareEndpoint(false, rosTopic, idlName, typeHash, qos, false)
	if err != nil {
		return SEDPEndpoint{}, fmt.Errorf("dds: sedp declare writer: %w", err)
	}

	return ep, nil
}

// DeclareReader declares a local reader for a ROS topic.
func (idx *SEDPIndex) DeclareReader(rosTopic, idlName, typeHash string, qos []byte) (SEDPEndpoint, error) {
	ep, err := idx.declareEndpoint(true, rosTopic, idlName, typeHash, qos, true)
	if err != nil {
		return SEDPEndpoint{}, fmt.Errorf("dds: sedp declare reader: %w", err)
	}

	return ep, nil
}

// DeclareWriterWithQoS declares a local writer announcing the translated
// per-profile wire QoS instead of the facade default.
//
// ForwardQoS refusal propagates as a loud declare error naming the topic,
// endpoint pair, and policy kind; nothing downgrades quietly. A nil
// profile keeps the facade path.
func (idx *SEDPIndex) DeclareWriterWithQoS(rosTopic, idlName, typeHash string, qos []byte, profile *core.QoSProfile) (SEDPEndpoint, error) {
	ep, err := idx.declareEndpointFull(false, false, rosTopic, idlName, typeHash, qos, false, profile, "publisher", "subscriber")
	if err != nil {
		return SEDPEndpoint{}, fmt.Errorf("dds: sedp declare writer with qos: %w", err)
	}

	return ep, nil
}

// DeclareReaderWithQoS declares a local reader announcing the translated
// per-profile wire QoS instead of the facade default.
//
// The volatile flag keeps echo semantics for readers; the profile drives
// reliability, history, deadline, lifespan, and liveliness. Refusal is
// loud. A nil profile keeps the facade path.
func (idx *SEDPIndex) DeclareReaderWithQoS(rosTopic, idlName, typeHash string, qos []byte, volatile bool, profile *core.QoSProfile) (SEDPEndpoint, error) {
	ep, err := idx.declareEndpointFull(true, false, rosTopic, idlName, typeHash, qos, volatile, profile, "publisher", "subscriber")
	if err != nil {
		return SEDPEndpoint{}, fmt.Errorf("dds: sedp declare reader with qos: %w", err)
	}

	return ep, nil
}

// DeclareKeyedWriter declares a local with-key writer with kind 0x02.
//
// Keyed writers match only keyed readers; keyed-vs-unkeyed mismatch is a
// loud incompatible at match time, never a silent filter drop.
func (idx *SEDPIndex) DeclareKeyedWriter(rosTopic, idlName, typeHash string, qos []byte) (SEDPEndpoint, error) {
	ep, err := idx.declareEndpointFull(false, true, rosTopic, idlName, typeHash, qos, false, nil, "publisher", "subscriber")
	if err != nil {
		return SEDPEndpoint{}, fmt.Errorf("dds: sedp declare keyed writer: %w", err)
	}

	return ep, nil
}

// DeclareKeyedReader declares a local with-key reader with kind 0x07.
func (idx *SEDPIndex) DeclareKeyedReader(rosTopic, idlName, typeHash string, qos []byte, volatile bool) (SEDPEndpoint, error) {
	ep, err := idx.declareEndpointFull(true, true, rosTopic, idlName, typeHash, qos, volatile, nil, "publisher", "subscriber")
	if err != nil {
		return SEDPEndpoint{}, fmt.Errorf("dds: sedp declare keyed reader: %w", err)
	}

	return ep, nil
}

// DeclareReaderWithDurability declares a local reader with explicit durability.
//
// A volatile reader matches ros2 topic echo semantics and flows against
// both volatile and transient offers. A transient local reader refuses
// volatile writers through peer side RxO matching, which is how QoS gates
// prove incompatible pairs stay silent. Writers always offer transient
// local per the facade, so the flag only affects readers.
func (idx *SEDPIndex) DeclareReaderWithDurability(rosTopic, idlName, typeHash string, qos []byte, volatile bool) (SEDPEndpoint, error) {
	ep, err := idx.declareEndpoint(true, rosTopic, idlName, typeHash, qos, volatile)
	if err != nil {
		return SEDPEndpoint{}, fmt.Errorf("dds: sedp declare reader: %w", err)
	}

	return ep, nil
}

// FindLocalWriter returns the first local writer matching topic and type.
//
// It normalizes topic and type with the same mapping as DeclareWriter and
// scans locals for a writer with equal ROS topic, DDS type, and type hash.
// The second result reports whether a match was found. It is used to reuse
// one stable writer GUID per topic so DATA writer entities match the SEDP
// announcement instead of allocating a new GUID per publish.
func (idx *SEDPIndex) FindLocalWriter(rosTopic, idlName, typeHash string) (SEDPEndpoint, bool) {
	inputs, err := sedpValidateDeclare(rosTopic, idlName)
	if err != nil {
		return SEDPEndpoint{}, false
	}

	idx.mu.Lock()
	defer idx.mu.Unlock()

	for _, ep := range idx.locals {
		if ep.IsReader {
			continue
		}

		if ep.ROSTopic != inputs.rosTopic {
			continue
		}

		if ep.TypeDDS != inputs.ddsType {
			continue
		}

		if ep.TypeIDL != inputs.idlName {
			continue
		}

		if ep.TypeHash != typeHash {
			continue
		}

		return sedpCloneEndpoint(ep), true
	}

	return SEDPEndpoint{}, false
}

// sedpDeclareInputs holds validated topic and type mapping for a declare.
type sedpDeclareInputs struct {
	// ddsTopic is the DDS name such as rt/chatter.
	ddsTopic string
	// ddsType is the DDS type name on the wire.
	ddsType string
	// rosTopic is the reverse mapped ROS name such as /chatter.
	rosTopic string
	// kind is the reverse mapped kind from ParseTopicName.
	kind TopicKind
	// idlName is the demangled IDL name echoed back for the record.
	idlName string
}

// sedpValidateDeclare maps topic and type names and cross checks the round
// trip before any entity is allocated.
func sedpValidateDeclare(rosTopic, idlName string) (sedpDeclareInputs, error) {
	var inputs sedpDeclareInputs

	ddsTopic, err := TopicName(rosTopic, false)
	if err != nil {
		return inputs, fmt.Errorf("dds: sedp declare topic: %w", err)
	}

	ddsType, err := MessageTypeName(idlName)
	if err != nil {
		return inputs, fmt.Errorf("dds: sedp declare type: %w", err)
	}

	decodedIDL, err := DemangleTypeName(ddsType)
	if err != nil {
		return inputs, fmt.Errorf("dds: sedp declare demangle: %w", err)
	}

	if decodedIDL != idlName {
		return inputs, fmt.Errorf("dds: sedp declare type round trip %q vs %q: %w", decodedIDL, idlName, ErrSEDPInvalidType)
	}

	decodedROS, kind, err := ParseTopicName(ddsTopic)
	if err != nil {
		return inputs, fmt.Errorf("dds: sedp declare parse topic: %w", err)
	}

	inputs = sedpDeclareInputs{
		ddsTopic: ddsTopic,
		ddsType:  ddsType,
		rosTopic: decodedROS,
		kind:     kind,
		idlName:  idlName,
	}

	return inputs, nil
}

// declareEndpoint validates mapping, allocates a user entity, and stores it.
func (idx *SEDPIndex) declareEndpoint(isReader bool, rosTopic, idlName, typeHash string, qos []byte, readerVolatile bool) (SEDPEndpoint, error) {
	return idx.declareEndpointFull(isReader, false, rosTopic, idlName, typeHash, qos, readerVolatile, nil, "publisher", "subscriber")
}

// sedpTranslateDeclareOffer translates an optional profile to a wire offer.
func sedpTranslateDeclareOffer(rosTopic, publisherName, subscriberName string, profile *core.QoSProfile) (*WireQoS, error) {
	if profile == nil {
		return nil, nil
	}

	translated, err := ForwardQoS(rosTopic, publisherName, subscriberName, *profile)
	if err != nil {
		return nil, fmt.Errorf("dds: sedp declare wire qos: %w", err)
	}

	return &translated, nil
}

// sedpDeclarePartitions extracts validated partition names from raw QoS.
func sedpDeclarePartitions(qos []byte) ([]string, error) {
	if len(qos) == 0 {
		return nil, nil
	}

	params, perr := rtps.DecodeParams(qos)
	if perr == nil {
		return sedpValidatedPartitionNames(params)
	}

	return nil, nil
}

// sedpValidatedPartitionNames validates partition names from decoded params.
func sedpValidatedPartitionNames(params []rtps.Param) ([]string, error) {
	names, ferr := FindPartitionParam(params)
	if ferr == nil && len(names) != 0 {
		if verr := ValidatePartitionList(names); verr != nil {
			return nil, fmt.Errorf("dds: sedp declare partitions: %w", verr)
		}

		return append([]string(nil), names...), nil
	}

	return nil, nil
}

// sedpBuildDeclareEndpoint assembles the endpoint record before storing.
func sedpBuildDeclareEndpoint(id SEDPGUID, inputs sedpDeclareInputs, typeHash string, qos []byte, isReader bool, domain uint32, readerVolatile bool, wireOffer *WireQoS) SEDPEndpoint {
	return SEDPEndpoint{
		ID:       id,
		ROSTopic: inputs.rosTopic,
		DDSTopic: inputs.ddsTopic,
		Kind:     inputs.kind,
		TypeDDS:  inputs.ddsType,
		TypeIDL:  inputs.idlName,
		TypeHash: typeHash,
		QoS:      sedpCloneBytes(qos),
		IsReader: isReader,
		DomainID: domain,
		// Readers default to volatile durability, matching ros2 topic
		// echo semantics: a volatile request flows against both
		// volatile and transient offers including the transient
		// backlog, while a transient request is incompatible with the
		// volatile talkers that ros2 topic pub creates by default and
		// would receive nothing. The echo --qos-durability flag
		// selects transient local when a gate must prove an
		// incompatible pair stays silent. Writers keep offering
		// transient local per the facade.
		VolatileDurability: isReader && readerVolatile,
		WireOffer:          wireOffer,
	}
}

// declareEndpointFull validates mapping, allocates a user entity with an
// optional with-key kind and an optional translated wire offer, and
// stores the record.
//
// The keyed flag mints kind 0x02/0x07 entities via KeyedUserEntity so
// keyed endpoints match only keyed peers. The profile translates through
// ForwardQoS into WireOffer; refusal is loud and aborts the declare. A
// nil profile keeps the facade announcement path byte-identical.
//
//nolint:unparam // publisher and subscriber name the refusing endpoint pair for all current callers
func (idx *SEDPIndex) declareEndpointFull(isReader, keyed bool, rosTopic, idlName, typeHash string, qos []byte, readerVolatile bool, profile *core.QoSProfile, publisherName, subscriberName string) (SEDPEndpoint, error) {
	inputs, err := sedpValidateDeclare(rosTopic, idlName)
	if err != nil {
		return SEDPEndpoint{}, fmt.Errorf("dds: sedp declare validate: %w", err)
	}

	idx.mu.Lock()
	defer idx.mu.Unlock()

	if idx.next > sedpUserEntityMax {
		return SEDPEndpoint{}, fmt.Errorf("dds: sedp declare entity space exhausted: %w", ErrSEDPInvalidEndpoint)
	}

	entity := sedpEntityFor(idx.next, isReader)
	if keyed {
		entity = KeyedUserEntity(idx.next, isReader)
	}
	idx.next++

	id := SEDPGUID{Prefix: idx.prefix, Entity: entity}

	_, exists := idx.locals[id]
	if exists {
		return SEDPEndpoint{}, fmt.Errorf("dds: sedp declare duplicate entity: %w", ErrSEDPInvalidEndpoint)
	}

	wireOffer, err := sedpTranslateDeclareOffer(rosTopic, publisherName, subscriberName, profile)
	if err != nil {
		return SEDPEndpoint{}, err
	}

	ep := sedpBuildDeclareEndpoint(id, inputs, typeHash, qos, isReader, idx.domain, readerVolatile, wireOffer)

	err = sedpCheckWireValues(ep.DDSTopic, ep.TypeHash)
	if err != nil {
		return SEDPEndpoint{}, fmt.Errorf("dds: sedp declare wire values: %w", err)
	}

	partitions, err := sedpDeclarePartitions(qos)
	if err != nil {
		return SEDPEndpoint{}, err
	}

	ep.Partitions = partitions

	idx.locals[id] = sedpCloneEndpoint(ep)

	return sedpCloneEndpoint(ep), nil
}

// StampLocalPartitions records partitions on matching local readers.
//
// It validates the names loudly, updates every local reader for the ROS
// topic, and bumps generation once when any record changes so the next
// announce carries the set. Unknown topics change nothing and report nil.
func (idx *SEDPIndex) StampLocalPartitions(rosTopic, typeName string, partitions []string) {
	err := ValidatePartitionList(partitions)
	if err != nil {
		return
	}

	idx.mu.Lock()
	defer idx.mu.Unlock()

	for id, ep := range idx.locals {
		if !ep.IsReader {
			continue
		}

		if ep.ROSTopic != rosTopic || ep.TypeIDL != typeName {
			continue
		}

		ep.Partitions = append([]string(nil), partitions...)
		idx.locals[id] = ep
		idx.generation++
	}
}

// sedpSnapshotEndpoints returns a sorted copy of one endpoint map.
//
// The caller holds the index lock. QoS bytes are copied per endpoint so
// later input reuse cannot corrupt the snapshot.
func sedpSnapshotEndpoints(src map[SEDPGUID]SEDPEndpoint) []SEDPEndpoint {
	out := make([]SEDPEndpoint, 0, len(src))
	for _, ep := range src {
		out = append(out, sedpCloneEndpoint(ep))
	}

	sedpSortEndpoints(out)

	return out
}

// LocalEndpoints returns a sorted copy of local endpoints.
func (idx *SEDPIndex) LocalEndpoints() []SEDPEndpoint {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	return sedpSnapshotEndpoints(idx.locals)
}

// RemoteEndpoints returns a sorted copy of remote endpoints.
func (idx *SEDPIndex) RemoteEndpoints() []SEDPEndpoint {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	return sedpSnapshotEndpoints(idx.remotes)
}

// UpsertRemote adds or replaces a remote endpoint by GUID.
//
// The input is always stored as confirmed: Provisional is cleared and
// FirstSeen is reset because SEDP QoS, type, and hash are authoritative.
// Promotion of a provisional placeholder therefore bumps generation.
func (idx *SEDPIndex) UpsertRemote(ep SEDPEndpoint) error {
	err := sedpValidateRemote(ep)
	if err != nil {
		sedpTracef("upsert reject guid=%s ros=%q dds=%q type=%q domain=%d err=%v", ep.ID.String(), ep.ROSTopic, ep.DDSTopic, ep.TypeDDS, ep.DomainID, err)
		return fmt.Errorf("dds: sedp upsert remote: %w", err)
	}

	ep.Provisional = false
	ep.FirstSeen = time.Time{}

	idx.mu.Lock()

	existing, seen := idx.remotes[ep.ID]
	if seen && sedpEndpointsEqual(existing, ep) {
		idx.mu.Unlock()
		sedpTracef("upsert skipped duplicate guid=%s ros=%q domain=%d", ep.ID.String(), ep.ROSTopic, ep.DomainID)

		return nil
	}

	installed := !seen
	idx.remotes[ep.ID] = sedpCloneEndpoint(ep)
	idx.generation++

	gen := idx.generation
	hook := idx.onChange

	idx.mu.Unlock()

	if installed {
		sedpTracef("upsert installed guid=%s ros=%q dds=%q typeDDS=%q hash=%q domain=%d reader=%v gen=%d", ep.ID.String(), ep.ROSTopic, ep.DDSTopic, ep.TypeDDS, ep.TypeHash, ep.DomainID, ep.IsReader, gen)
	} else {
		sedpTracef("upsert updated guid=%s ros=%q dds=%q typeDDS=%q hash=%q domain=%d reader=%v gen=%d", ep.ID.String(), ep.ROSTopic, ep.DDSTopic, ep.TypeDDS, ep.TypeHash, ep.DomainID, ep.IsReader, gen)
	}

	if hook != nil {
		hook(gen)
	}

	return nil
}

// RemoveRemote deletes a remote endpoint by GUID.
func (idx *SEDPIndex) RemoveRemote(id SEDPGUID) bool {
	idx.mu.Lock()

	_, seen := idx.remotes[id]
	if !seen {
		idx.mu.Unlock()

		return false
	}

	delete(idx.remotes, id)
	idx.generation++

	gen := idx.generation
	hook := idx.onChange

	idx.mu.Unlock()

	if hook != nil {
		hook(gen)
	}

	return true
}

// ProvisionalInstall installs a datapath fast-path placeholder by GUID.
//
// The record is stored with Provisional true and FirstSeen set to now
// when the caller leaves it zero. A confirmed record is never
// overwritten: install over a non-provisional entry is a no-op without
// bumping generation. Install over a provisional entry preserves the
// original FirstSeen so the TTL stays bounded from first sight, and is
// a no-op when all other fields already match. Otherwise it stores the
// placeholder, bumps generation, and fires the change hook.
func (idx *SEDPIndex) ProvisionalInstall(ep SEDPEndpoint) error {
	err := sedpValidateRemote(ep)
	if err != nil {
		return fmt.Errorf("dds: sedp provisional install: %w", err)
	}

	ep.Provisional = true
	if ep.FirstSeen.IsZero() {
		ep.FirstSeen = time.Now()
	}

	idx.mu.Lock()

	existing, seen := idx.remotes[ep.ID]
	if seen && !existing.Provisional {
		idx.mu.Unlock()

		return nil
	}

	if seen {
		candidate := sedpCloneEndpoint(ep)
		candidate.FirstSeen = existing.FirstSeen
		if sedpEndpointsEqual(existing, candidate) {
			idx.mu.Unlock()

			return nil
		}

		ep.FirstSeen = existing.FirstSeen
	}

	idx.remotes[ep.ID] = sedpCloneEndpoint(ep)
	idx.generation++

	gen := idx.generation
	hook := idx.onChange

	idx.mu.Unlock()

	if hook != nil {
		hook(gen)
	}

	return nil
}

// ExpireProvisional removes provisional entries older than sedpProvisionalTTL.
//
// An entry expires when now is at or past FirstSeen plus the TTL. A zero
// FirstSeen is treated as expired so a malformed placeholder cannot
// linger. It bumps generation once when any entry expires, fires the
// change hook once, and returns the number removed.
func (idx *SEDPIndex) ExpireProvisional(now time.Time) int {
	idx.mu.Lock()

	removed := 0

	for id, ep := range idx.remotes {
		if !ep.Provisional {
			continue
		}

		if ep.FirstSeen.IsZero() {
			delete(idx.remotes, id)
			removed++

			continue
		}

		if now.Before(ep.FirstSeen.Add(sedpProvisionalTTL)) {
			continue
		}

		delete(idx.remotes, id)
		removed++
	}

	if removed == 0 {
		idx.mu.Unlock()

		return 0
	}

	idx.generation++

	gen := idx.generation
	hook := idx.onChange

	idx.mu.Unlock()

	if hook != nil {
		hook(gen)
	}

	return removed
}

// Sweep removes expired provisional entries and bumps generation once when any expire.
//
// It is an alias for ExpireProvisional kept for symmetry with the SPDP
// remote table sweep. Confirmed entries never expire here.
func (idx *SEDPIndex) Sweep(now time.Time) int {
	return idx.ExpireProvisional(now)
}

// AnnounceLoop publishes all local endpoints immediately then re-announces
// on local changes until ctx is done.
//
// It snapshots LocalEndpoints once, sends each sample with
// SerializeSEDPDATA through s, then polls LocalEndpoints every 200ms and
// re-sends all locals when the snapshot differs. It also resends every
// second with fresh DATA sequences and HEARTBEATs so reliable peers that
// dropped the first sample still match. Generation is not polled
// because it counts remote mutations only and local declares leave it
// unchanged, so the loop compares local snapshots instead. Sends run
// outside the index lock because LocalEndpoints and SerializeSEDPDATA copy
// their inputs.
//
// A snapshot with zero locals sends nothing.
//
// It returns ctx.Err when ctx is done and is safe for concurrent use with
// DeclareWriter, DeclareReader, UpsertRemote, and RemoveRemote.
func (idx *SEDPIndex) AnnounceLoop(ctx context.Context, s SEDPSender) error {
	if idx == nil {
		return fmt.Errorf("dds: sedp announce nil index: %w", ErrSEDPInvalidEndpoint)
	}

	if ctx == nil {
		return fmt.Errorf("dds: sedp announce nil context: %w", ErrSEDPInvalidEndpoint)
	}

	if s == nil {
		return fmt.Errorf("dds: sedp announce nil sender: %w", ErrSEDPInvalidEndpoint)
	}

	err := ctx.Err()
	if err != nil {
		return err //nolint:wrapcheck // cancel semantics require unwrapped ctx.Err
	}

	last := idx.LocalEndpoints()

	err = sedpAnnounceSnapshot(ctx, s, last)
	if err != nil {
		return sedpMapLoopError(ctx, fmt.Errorf("dds: sedp announce snapshot: %w", err))
	}

	lastSend := time.Now()

	ticker := time.NewTicker(sedpAnnouncePollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err() //nolint:wrapcheck // cancel semantics require unwrapped ctx.Err
		case <-ticker.C:
			current := idx.LocalEndpoints()
			if sedpEndpointsSlicesEqual(last, current) && time.Since(lastSend) < sedpAnnounceResendInterval {
				continue
			}

			err := sedpAnnounceSnapshot(ctx, s, current)
			if err != nil {
				return sedpMapLoopError(ctx, fmt.Errorf("dds: sedp announce snapshot: %w", err))
			}

			last = current
			lastSend = time.Now()
		}
	}
}

// ListenLoop receives DATA samples until ctx is done.
//
// Each non-empty sample feeds ParseSEDPDATA then UpsertRemote, so every
// valid announce bumps the remote generation through the existing upsert
// path and fires the change hook. Empty samples are treated as no-data
// polls from non-blocking receivers and are skipped. Malformed samples
// return the parse error instead of being dropped silently.
//
// A sender that omits the domain, such as CycloneDDS, parses with domain
// zero. The loop stamps such samples with the index domain before upsert
// because SEDP traffic arrives on domain scoped ports, so a missing value
// means the local domain. Samples that carry an explicit domain keep it.
// Matching still requires equal domains on both records.
//
// Disposal has no grace or expiry here: absence from later announces does
// not remove entries. Call RemoveRemote explicitly to dispose a remote
// endpoint, which bumps generation through the existing remove path.
//
// It returns ctx.Err when ctx is done and is safe for concurrent use.
func (idx *SEDPIndex) ListenLoop(ctx context.Context, r SEDPReceiver) error {
	if idx == nil {
		return fmt.Errorf("dds: sedp listen nil index: %w", ErrSEDPInvalidEndpoint)
	}

	if ctx == nil {
		return fmt.Errorf("dds: sedp listen nil context: %w", ErrSEDPInvalidEndpoint)
	}

	if r == nil {
		return fmt.Errorf("dds: sedp listen nil receiver: %w", ErrSEDPInvalidEndpoint)
	}

	for {
		data, err := r.ReceiveSEDP(ctx)
		if err != nil {
			return sedpMapLoopError(ctx, fmt.Errorf("dds: sedp listen receive: %w", err))
		}

		err = ctx.Err()
		if err != nil {
			return err //nolint:wrapcheck // cancel semantics require unwrapped ctx.Err
		}

		if len(data) == 0 {
			continue
		}

		err = idx.ingestSample(data)
		if err != nil {
			// A single poison datagram must not kill discovery: log
			// through the SEDP trace and keep listening for the next
			// sample, matching the backend receiver which already
			// continues on unparseable or off-domain DATA. Only a
			// transport error or context cancel ends the loop.
			if ctxErr := ctx.Err(); ctxErr != nil {
				return ctxErr //nolint:wrapcheck // cancel semantics require unwrapped ctx.Err
			}
			sedpTracef("ingest skip poison len=%d err=%v", len(data), err)
			continue
		}
	}
}

// ingestSample parses all DATA samples in one datagram and upserts each.
//
// Samples that omit the domain, such as CycloneDDS endpoint DATA, are
// stamped with the index domain because SEDP traffic arrives on domain
// scoped ports. An explicit domain is kept as sent. Datagrams carrying
// only SEDP HEARTBEAT or ACKNACK without endpoint DATA are skipped
// without error so reliable peers do not break the listen loop. Each
// entry upserts independently so one malformed or invalid endpoint does
// not drop the others. It returns nil when at least one entry installs,
// otherwise the first parse or upsert error.
func (idx *SEDPIndex) ingestSample(data []byte) error {
	sedpTracef("ingest recv wire len=%d", len(data))
	eps, err := ParseSEDPDATAs(data)
	if err != nil {
		sedpTracef("ingest parse reject len=%d err=%v", len(data), err)
		if errors.Is(err, ErrSEDPInvalidDATA) && sedpIsSEDPControl(data) {
			sedpTracef("ingest control skip len=%d", len(data))
			return nil
		}
		return fmt.Errorf("dds: sedp listen parse: %w", err)
	}

	var firstErr error

	installed := 0

	for _, ep := range eps {
		sedpTracef("ingest parse accept guid=%s ros=%q dds=%q typeDDS=%q typeIDL=%q hash=%q domain=%d reader=%v", ep.ID.String(), ep.ROSTopic, ep.DDSTopic, ep.TypeDDS, ep.TypeIDL, ep.TypeHash, ep.DomainID, ep.IsReader)

		if ep.DomainID == 0 {
			ep.DomainID = idx.stampDomain()
			sedpTracef("ingest stamp domain=%d guid=%s", ep.DomainID, ep.ID.String())
		}

		err = idx.UpsertRemote(ep)
		if err != nil {
			sedpTracef("ingest upsert reject guid=%s err=%v", ep.ID.String(), err)
			if firstErr == nil {
				firstErr = err
			}

			continue
		}

		installed++

		sedpTracef("ingest upsert ok guid=%s ros=%q domain=%d", ep.ID.String(), ep.ROSTopic, ep.DomainID)
	}

	if installed == 0 {
		if firstErr != nil {
			return fmt.Errorf("dds: sedp listen upsert: %w", firstErr)
		}

		return fmt.Errorf("dds: sedp listen ingest no endpoint installed: %w", ErrSEDPInvalidDATA)
	}

	return nil
}

// sedpAnnounceSnapshot serializes and sends one local snapshot.
func sedpAnnounceSnapshot(ctx context.Context, s SEDPSender, eps []SEDPEndpoint) error {
	for _, ep := range eps {
		select {
		case <-ctx.Done():
			return ctx.Err() //nolint:wrapcheck // cancel semantics require unwrapped ctx.Err
		default:
		}

		data, err := SerializeSEDPDATA(ep)
		if err != nil {
			return fmt.Errorf("dds: sedp announce serialize: %w", err)
		}

		err = s.SendSEDP(ctx, data)
		if err != nil {
			return sedpMapLoopError(ctx, fmt.Errorf("dds: sedp announce send: %w", err))
		}
	}

	return nil
}

// sedpEndpointsSlicesEqual reports whether two sorted snapshots match.
func sedpEndpointsSlicesEqual(a, b []SEDPEndpoint) bool {
	if len(a) != len(b) {
		return false
	}

	for i := range a {
		if !sedpEndpointsEqual(a[i], b[i]) {
			return false
		}
	}

	return true
}

// sedpMapLoopError returns ctx.Err when ctx is done, else the loop error.
func sedpMapLoopError(ctx context.Context, err error) error {
	ctxErr := ctx.Err()
	if ctxErr != nil {
		return ctxErr //nolint:wrapcheck // cancel semantics require unwrapped ctx.Err
	}

	return err
}

// MatchingWriters returns remote writers matching a local reader.
func (idx *SEDPIndex) MatchingWriters(readerID SEDPGUID) ([]SEDPEndpoint, error) {
	idx.mu.Lock()

	local, seen := idx.locals[readerID]
	if !seen {
		idx.mu.Unlock()

		return nil, fmt.Errorf("dds: sedp match reader %s: %w", readerID.String(), ErrSEDPUnknownEndpoint)
	}

	if !local.IsReader {
		idx.mu.Unlock()

		return nil, fmt.Errorf("dds: sedp match endpoint %s is not a reader: %w", readerID.String(), ErrSEDPInvalidEndpoint)
	}

	remotes := make([]SEDPEndpoint, 0, len(idx.remotes))
	for _, ep := range idx.remotes {
		remotes = append(remotes, sedpCloneEndpoint(ep))
	}

	localCopy := sedpCloneEndpoint(local)
	idx.mu.Unlock()

	return sedpFilterRemoteWriters(localCopy, remotes), nil
}

// sedpFilterRemoteWriters returns the sorted remotes matching a local reader.
//
// Matching compares the DDS topic and type names on the wire. TypeIDL is
// not compared: it is the demangled view of the same TypeDDS value, so a
// record with a stale IDL label still matches when the wire type agrees.
func sedpFilterRemoteWriters(local SEDPEndpoint, remotes []SEDPEndpoint) []SEDPEndpoint {
	out := make([]SEDPEndpoint, 0, len(remotes))

	for _, remote := range remotes {
		if SEDPEndpointsMatch(local, remote) {
			out = append(out, remote)
		}
	}

	sedpSortEndpoints(out)

	return out
}

// sedpMatchIdentity compares routing identity fields for a reader and writer.
func sedpMatchIdentity(reader, writer SEDPEndpoint) bool {
	if !reader.IsReader {
		return false
	}

	if writer.IsReader {
		return false
	}

	if reader.DomainID != writer.DomainID {
		return false
	}

	if reader.ROSTopic != writer.ROSTopic {
		return false
	}

	if reader.DDSTopic != writer.DDSTopic {
		return false
	}

	if reader.Kind != writer.Kind {
		return false
	}

	return reader.TypeDDS == writer.TypeDDS
}

// sedpMatchTypeHash compares type hashes with empty wildcarding.
func sedpMatchTypeHash(reader, writer SEDPEndpoint) bool {
	// Empty hash wildcards for Humble pre-hash peers without user-data hash.
	// When both hashes are set they must be equal.
	if reader.TypeHash != "" && writer.TypeHash != "" && reader.TypeHash != writer.TypeHash {
		return false
	}

	return true
}

// sedpMatchPartitions compares partitions with a trace on mismatch.
func sedpMatchPartitions(reader, writer SEDPEndpoint) bool {
	// Partition failure is a quiet non-match, not an RxO error, per the
	// DDS rule that a missing common partition is not incompatible QoS.
	// An empty list means the default partition on both sides.
	if !PartitionsMatch(reader.Partitions, writer.Partitions) {
		sedpTracef("match reject partition reader=%s writer=%s", reader.ID.String(), writer.ID.String())
		return false
	}

	return true
}

// sedpMatchKeyed compares keyed kinds with a trace on mismatch.
func sedpMatchKeyed(reader, writer SEDPEndpoint) bool {
	// Keyed-vs-unkeyed mismatch is a loud incompatible, never a silent
	// filter drop. CheckKeyedMatch names the mismatch; here it gates
	// matching while the caller surfaces the reason.
	ok, err := CheckKeyedMatch(reader, writer)
	if err != nil || !ok {
		sedpTracef("match reject keyed reader=%s writer=%s err=%v", reader.ID.String(), writer.ID.String(), err)
		return false
	}

	return true
}

// SEDPEndpointsMatch reports whether a local reader matches a remote writer.
func SEDPEndpointsMatch(reader, writer SEDPEndpoint) bool {
	if !sedpMatchIdentity(reader, writer) {
		return false
	}

	if !sedpMatchTypeHash(reader, writer) {
		return false
	}

	if !sedpMatchPartitions(reader, writer) {
		return false
	}

	return sedpMatchKeyed(reader, writer)
}

// sedpUserdataFields holds the parsed USER_DATA key value pairs.
type sedpUserdataFields struct {
	// hash is the type hash passthrough, empty when absent.
	hash string
	// ddsTopic echoes the DDS topic when present.
	ddsTopic string
	// hasDDSTopic reports whether the DDS topic echo was present.
	hasDDSTopic bool
	// domain is the Weld domain id when present.
	domain uint32
	// hasDomain reports whether the domain was present.
	hasDomain bool
	// serviceID is the service identifier advertised by ROS peers under
	// the serviceid key, empty when absent. Servers advertise one ID on
	// both reader and writer so discovery can pair halves of one service.
	serviceID string
	// hasServiceID reports whether a serviceid pair was present.
	hasServiceID bool
	// clientID is the client identifier advertised by ROS peers under
	// the clientid key, empty when absent. Clients advertise it instead
	// of serviceid; stored for the same pairing use.
	clientID string
	// hasClientID reports whether a clientid pair was present.
	hasClientID bool
}

// nextSedpSeq returns the next DATA sequence for one builtin writer.
//
// The stream is keyed by participant prefix plus writer entity and
// starts at 1. Zero is never emitted: the counter restarts at 1 after
// overflow. Contiguity matters because reliable SEDP readers hold
// samples behind a gap and Weld does not repair SEDP.
func nextSedpSeq(prefix SEDPGUIDPrefix, writer SEDPEntityID) uint64 {
	sedpWriterSeq.Lock()
	defer sedpWriterSeq.Unlock()

	key := sedpWriterKey{prefix: prefix, writer: writer}
	seq := sedpWriterSeqLast[key] + 1
	if seq == 0 {
		seq = 1
	}

	sedpWriterSeqLast[key] = seq

	return seq
}

// nextHeartbeatCount returns the next HEARTBEAT count, never zero.
//
// The count is shared by SEDP and user DATA heartbeats so one process wide
// counter keeps every heartbeat increasing.
func nextHeartbeatCount() int32 {
	for {
		count := heartbeatCount.Add(1)
		if count == 0 {
			continue
		}

		if count < 0 {
			heartbeatCount.Store(1)

			return 1
		}

		return count
	}
}

// sedpEncodeHeartbeatBody renders a 28 byte HEARTBEAT body in little endian.
func sedpEncodeHeartbeatBody(writer SEDPEntityID, first, last uint64, count int32) []byte {
	out := make([]byte, 0, sedpHeartbeatSize)
	out = append(out, sedpEntityUnknown[:]...)
	out = append(out, writer[:]...)
	out = binary.LittleEndian.AppendUint32(out, uint32(first>>32))
	out = binary.LittleEndian.AppendUint32(out, uint32(first)) //nolint:gosec // G115: low word truncation is the layout
	out = binary.LittleEndian.AppendUint32(out, uint32(last>>32))
	out = binary.LittleEndian.AppendUint32(out, uint32(last))  //nolint:gosec // G115: low word truncation is the layout
	out = binary.LittleEndian.AppendUint32(out, uint32(count)) //nolint:gosec // G115: two's complement reinterpret for the wire

	return out
}

// sedpAppendHeartbeat appends one HEARTBEAT submessage for a SEDP writer.
func sedpAppendHeartbeat(out []byte, writer SEDPEntityID, first, last uint64, count int32) []byte {
	body := sedpEncodeHeartbeatBody(writer, first, last, count)
	out = append(out, sedpSubmessageHeartbeat, sedpFlagLittleEndian)
	out = binary.LittleEndian.AppendUint16(out, uint16(len(body))) //nolint:gosec // G115: heartbeat body is fixed 28 bytes
	return append(out, body...)
}

// sedpHeartbeatWriters returns the writer needing a HEARTBEAT for one DATA.
//
// Only the DATA writer is returned. The spec publications writer carries
// no DATA, so a heartbeat for it would advertise sequences that never
// exist and a reliable peer could never repair them.
func sedpHeartbeatWriters(writer SEDPEntityID) []SEDPEntityID {
	return []SEDPEntityID{writer}
}

// sedpIsSEDPControl reports whether a datagram carries SEDP HEARTBEAT or ACKNACK.
func sedpIsSEDPControl(data []byte) bool {
	err := sedpValidateRTPSHeader(data)
	if err != nil {
		return false
	}
	off := sedpRTPSHeaderLen
	for off < len(data) {
		id, body, _, next, err := sedpSplitSubmessage(data, off)
		if err != nil {
			return false
		}
		off = next
		if id != sedpSubmessageHeartbeat && id != sedpSubmessageAckNack {
			continue
		}
		if len(body) < sedpEntityLen*2 {
			continue
		}
		var writer SEDPEntityID
		copy(writer[:], body[sedpHeartbeatWriterOff:sedpHeartbeatWriterOff+sedpEntityLen])
		isEndpoint, _ := sedpClassifyWriter(writer)
		if isEndpoint {
			return true
		}
	}
	return false
}

// sedpEncodeParamListPayload renders params as a PL_CDR little endian payload.
//
// The 4 byte encapsulation header plus the sentinel terminated list mirror
// the CycloneDDS endpoint DATA shape, which carries discovery params in the
// payload. CycloneDDS drops inline-only endpoint DATA with missing payload,
// so Weld sends both forms: inline params for Weld peers plus this payload
// copy for CycloneDDS. Identity stays first-wins on parse.
func sedpEncodeParamListPayload(params []rtps.Param) []byte {
	out := []byte{0x00, 0x03, 0x00, 0x00}
	out = append(out, rtps.EncodeParams(params)...)
	return out
}

// SerializeSEDPDATA serializes an endpoint as an RTPS DATA sample.
//
// The output is one RTPS message: the 20 byte header with the endpoint
// participant prefix plus a DATA submessage from the builtin SEDP writer
// followed by one HEARTBEAT submessage. Publications announce from
// 0x000003c2 matching CycloneDDS and subscriptions from 0x000004c2, both
// addressed to the unknown reader. The HEARTBEAT covers only the DATA
// writer because only that writer carries DATA. Inline QoS carries the topic name, type name, endpoint GUID,
// reliable transient local QoS with keep-last history, protocol version, vendor
// id, userdata with the type hash, plus builtin endpoint QoS when set and
// TypeObject bytes when present. The payload carries a PL_CDR copy of the
// same params so CycloneDDS accepts the sample, with the opaque QoS bytes
// after the list when set. Flags are always LE plus InlineQoS plus
// DataPresent (0x07). Unknown vendor PIDs such as 0x8003 and 0x800c are
// tolerated on receive.
// XTypes TypeObject rides as opaque passthrough; full lookup is out of scope for v1.
// Sequence numbers increase process wide and each HEARTBEAT announces
// first 1 to last DATA sequence with a fresh count.
// sedpSerializeUserdata encodes endpoint or service userdata for announce.
func sedpSerializeUserdata(ep SEDPEndpoint) (string, error) {
	userdata, err := sedpEncodeEndpointUserdata(ep.TypeHash, ep.DDSTopic, ep.DomainID)
	if err != nil {
		return "", fmt.Errorf("dds: sedp serialize userdata: %w", err)
	}

	if ep.ServiceID != "" {
		userdata, err = EncodeServiceUserdata(ep.ServiceID, ep.TypeHash, ep.DDSTopic, ep.DomainID)
		if err != nil {
			return "", fmt.Errorf("dds: sedp serialize service userdata: %w", err)
		}
	}

	return userdata, nil
}

// sedpSerializeBaseParams builds identity plus QoS params for announce.
func sedpSerializeBaseParams(ep SEDPEndpoint, userdata string) []rtps.Param {
	params := []rtps.Param{
		{ID: rtps.PIDTopicName, Value: sedpEncodeCDRString(ep.DDSTopic)},
		{ID: rtps.PIDTypeName, Value: sedpEncodeCDRString(ep.TypeDDS)},
		{ID: rtps.PIDEndpointGUIDWire, Value: sedpEncodeEndpointGUID(ep.ID)},
	}

	if ep.WireOffer != nil {
		params = append(params, ep.WireOffer.Params()...)
	} else {
		params = append(params,
			rtps.Param{ID: rtps.PIDReliability, Value: sedpEncodeReliability()},
			rtps.Param{ID: rtps.PIDHistory, Value: sedpEncodeHistory()},
			rtps.Param{ID: rtps.PIDDurability, Value: sedpEncodeDurabilityFor(ep)},
		)
	}

	return append(params, []rtps.Param{
		{ID: rtps.PIDProtocolVersion, Value: sedpEncodeProtocolVersion()},
		{ID: rtps.PIDVendorID, Value: sedpEncodeVendorID()},
		{ID: rtps.PIDUserData, Value: sedpEncodeOctetSequence([]byte(userdata))},
	}...)
}

// sedpSerializeOptionalParams appends durability, partitions, and extensions.
func sedpSerializeOptionalParams(ep SEDPEndpoint, params []rtps.Param) ([]rtps.Param, error) {
	if durabilityParam, err := DurabilityServiceParam(WeldDurabilityServiceQoS()); err != nil {
		return nil, fmt.Errorf("dds: sedp serialize durability service: %w", err)
	} else {
		params = append(params, durabilityParam)
	}

	if len(ep.Partitions) != 0 {
		partitionParam, err := PartitionParam(ep.Partitions)
		if err != nil {
			return nil, fmt.Errorf("dds: sedp serialize partitions: %w", err)
		}

		params = append(params, partitionParam)
	}

	if ep.BuiltinEndpointQos != 0 {
		params = append(params, rtps.Param{ID: rtps.PIDBuiltinEndpointQos, Value: sedpEncodeBuiltinQos(ep.BuiltinEndpointQos)})
	}

	if len(ep.TypeObject) != 0 {
		params = append(params, rtps.Param{ID: rtps.PIDTypeObject, Value: append([]byte(nil), ep.TypeObject...)})
	}

	return params, nil
}

// SerializeSEDPDATA serializes an endpoint as an RTPS DATA sample.
func SerializeSEDPDATA(ep SEDPEndpoint) ([]byte, error) {
	err := sedpValidateMarshalEndpoint(ep)
	if err != nil {
		return nil, fmt.Errorf("dds: sedp serialize endpoint: %w", err)
	}

	writer := sedpPublicationsWriterCyclone
	if ep.IsReader {
		writer = sedpSubscriptionsWriter
	}

	userdata, err := sedpSerializeUserdata(ep)
	if err != nil {
		return nil, err
	}

	params := sedpSerializeBaseParams(ep, userdata)

	params, err = sedpSerializeOptionalParams(ep, params)
	if err != nil {
		return nil, err
	}

	inline := rtps.EncodeParams(params)

	payload := sedpEncodeParamListPayload(params)
	payload = append(payload, ep.QoS...)

	seq := nextSedpSeq(ep.ID.Prefix, writer)

	body := make([]byte, 0, sedpDataFixedLen+len(inline)+len(payload))
	body = append(body, sedpDATAReserved, sedpDATAReserved)
	body = binary.LittleEndian.AppendUint16(body, sedpOctetsToInlineQoS)
	body = append(body, sedpEntityUnknown[:]...)
	body = append(body, writer[:]...)
	body = binary.LittleEndian.AppendUint32(body, uint32(seq>>32))
	body = binary.LittleEndian.AppendUint32(body, uint32(seq)) //nolint:gosec // G115: low word truncation is the layout
	body = append(body, inline...)
	body = append(body, payload...)

	if len(body) > sedpMaxBodyLen {
		return nil, fmt.Errorf("dds: sedp serialize body %d above %d: %w", len(body), sedpMaxBodyLen, ErrSEDPInvalidEndpoint)
	}

	out := make([]byte, 0, sedpRTPSHeaderLen+sedpSubmessageHeaderLen+len(body)+2*(sedpSubmessageHeaderLen+sedpHeartbeatSize))
	out = append(out, sedpRTPSMagic...)
	out = append(out, sedpRTPSMajor, sedpRTPSMinor, sedpVendorID0, sedpVendorID1)
	out = append(out, ep.ID.Prefix[:]...)

	flags := sedpFlagLittleEndian | sedpFlagInlineQoS | sedpFlagDataPresent

	out = append(out, sedpSubmessageDATA, flags)
	out = binary.LittleEndian.AppendUint16(out, uint16(len(body))) //nolint:gosec // G115: length guarded by the sedpMaxBodyLen check above

	out = append(out, body...)

	for _, hbWriter := range sedpHeartbeatWriters(writer) {
		count := nextHeartbeatCount()
		out = sedpAppendHeartbeat(out, hbWriter, sedpHeartbeatFirstSeq, seq, count)
	}

	return out, nil
}

// ParseSEDPDATA parses an RTPS DATA sample back into an endpoint.
//
// It validates the RTPS header, walks the submessages, and decodes the
// first DATA from a builtin SEDP writer. Unknown submessages and DATA
// from other writers are skipped. Inline QoS params supply the topic,
// type, and endpoint GUID; when they are absent or incomplete, a
// parameter list payload with CDR encapsulation supplies the remainder,
// which is the shape CycloneDDS sends. The type hash is optional: a
// missing userdata hash parses as empty. The domain defaults to zero
// when the userdata carries none; ListenLoop stamps the local domain.
// Builtin endpoint QoS and XTypes TypeObject are optional and stored
// when present. Vendor PIDs such as 0x8003 and 0x800c are tolerated.
func ParseSEDPDATA(data []byte) (SEDPEndpoint, error) {
	body, flags, err := sedpFindEndpointDATA(data)
	if err != nil {
		sedpTracef("parse reject find-data len=%d err=%v", len(data), err)
		return SEDPEndpoint{}, fmt.Errorf("dds: sedp parse find data: %w", err)
	}

	ep, err := sedpDecodeEndpoint(body, flags)
	if err != nil {
		sedpTracef("parse reject decode len=%d flags=%#02x err=%v", len(data), flags, err)
		return SEDPEndpoint{}, fmt.Errorf("dds: sedp parse decode: %w", err)
	}

	sedpTracef("parse accept guid=%s ros=%q dds=%q typeDDS=%q typeIDL=%q hash=%q domain=%d reader=%v len=%d", ep.ID.String(), ep.ROSTopic, ep.DDSTopic, ep.TypeDDS, ep.TypeIDL, ep.TypeHash, ep.DomainID, ep.IsReader, len(data))

	return ep, nil
}

// ParseSEDPDATAs parses all endpoint DATA samples in one datagram.
//
// It validates the RTPS header, walks the submessages, and decodes every
// DATA from a builtin SEDP writer. Unknown submessages and DATA from
// other writers are skipped. Decoding uses the same inline plus payload
// rules as ParseSEDPDATA with per entry isolation: one malformed entry
// is skipped without dropping the others. It returns the decoded
// endpoints in wire order, or an error when no entry decodes.
func ParseSEDPDATAs(data []byte) ([]SEDPEndpoint, error) {
	bodies, flags, err := sedpFindAllEndpointDATA(data)
	if err != nil {
		sedpTracef("parse reject find-data len=%d err=%v", len(data), err)
		return nil, fmt.Errorf("dds: sedp parse find data: %w", err)
	}

	out := make([]SEDPEndpoint, 0, len(bodies))

	var firstErr error

	for i := range bodies {
		ep, err := sedpDecodeEndpoint(bodies[i], flags[i])
		if err != nil {
			sedpTracef("parse reject decode index=%d/%d len=%d flags=%#02x err=%v", i, len(bodies), len(data), flags[i], err)
			if firstErr == nil {
				firstErr = err
			}

			continue
		}

		sedpTracef("parse accept index=%d/%d guid=%s ros=%q dds=%q typeDDS=%q typeIDL=%q hash=%q domain=%d reader=%v len=%d", i, len(bodies), ep.ID.String(), ep.ROSTopic, ep.DDSTopic, ep.TypeDDS, ep.TypeIDL, ep.TypeHash, ep.DomainID, ep.IsReader, len(data))
		out = append(out, ep)
	}

	if len(out) == 0 {
		if firstErr != nil {
			return nil, fmt.Errorf("dds: sedp parse decode: %w", firstErr)
		}

		return nil, fmt.Errorf("dds: sedp parse finds no decodable endpoint DATA: %w", ErrSEDPInvalidDATA)
	}

	return out, nil
}

// sedpFindAllEndpointDATA validates the RTPS header and returns every
// DATA body from a builtin SEDP writer with its flags in wire order.
func sedpFindAllEndpointDATA(data []byte) ([][]byte, []byte, error) {
	err := sedpValidateRTPSHeader(data)
	if err != nil {
		return nil, nil, fmt.Errorf("dds: sedp parse header: %w", err)
	}

	off := sedpRTPSHeaderLen

	var endpointBodies [][]byte

	var endpointFlags []byte

	for off < len(data) {
		id, body, flags, next, err := sedpSplitSubmessage(data, off)
		if err != nil {
			return nil, nil, fmt.Errorf("dds: sedp parse submessage at %d: %w", off, err)
		}

		off = next

		if id != sedpSubmessageDATA {
			continue
		}

		writer, err := sedpDATAWriter(body)
		if err != nil {
			return nil, nil, fmt.Errorf("dds: sedp parse data writer: %w", err)
		}

		isEndpoint, _ := sedpClassifyWriter(writer)
		if !isEndpoint {
			continue
		}

		endpointBodies = append(endpointBodies, body)
		endpointFlags = append(endpointFlags, flags)
	}

	if len(endpointBodies) == 0 {
		return nil, nil, fmt.Errorf("dds: sedp parse finds no endpoint DATA: %w", ErrSEDPInvalidDATA)
	}

	sedpTracef("find-data datagram len=%d endpoint-datas=%d first-writer=%02x%02x%02x%02x", len(data), len(endpointBodies), endpointBodies[0][sedpDATAWriterOff], endpointBodies[0][sedpDATAWriterOff+1], endpointBodies[0][sedpDATAWriterOff+2], endpointBodies[0][sedpDATAWriterOff+3])
	if len(endpointBodies) > 1 {
		for i, b := range endpointBodies {
			sedpTracef("find-data batched index=%d/%d %s", i, len(endpointBodies), sedpTraceEndpointSummary(b, endpointFlags[i]))
		}
	}

	return endpointBodies, endpointFlags, nil
}

// sedpFindEndpointDATA validates the RTPS header and returns the first
// DATA body from a builtin SEDP writer with its flags.
func sedpFindEndpointDATA(data []byte) ([]byte, byte, error) {
	bodies, flags, err := sedpFindAllEndpointDATA(data)
	if err != nil {
		return nil, 0, err
	}

	return bodies[0], flags[0], nil
}

// sedpTraceEndpointSummary best-effort summarizes one endpoint DATA body for trace.
//
// It decodes params to extract DDS topic, type, and endpoint GUID without
// failing the caller. On decode failure it reports the error as the summary.
func sedpTraceEndpointSummary(body []byte, flags byte) string {
	params, _, err := sedpCollectParams(body, flags)
	if err != nil {
		return "collect-err=" + err.Error()
	}

	littleEndian := flags&sedpFlagLittleEndian != 0

	fields, err := sedpEndpointFields(params, littleEndian)
	if err != nil {
		return "fields-err=" + err.Error()
	}

	return "guid=" + fields.id.String() + " dds=" + fields.ddsTopic + " type=" + fields.typeDDS
}

// sedpSplitSubmessage slices one submessage body with its id and flags.
//
// Lengths use the per submessage endianness bit. The body excludes the
// header and is copied so later input reuse cannot corrupt it.
// Truncation and overruns are explicit errors.
func sedpSplitSubmessage(data []byte, off int) (byte, []byte, byte, int, error) {
	if len(data)-off < sedpSubmessageHeaderLen {
		return 0, nil, 0, 0, fmt.Errorf("dds: sedp parse submessage header need %d have %d: %w", sedpSubmessageHeaderLen, len(data)-off, ErrSEDPInvalidDATA)
	}

	id := data[off]
	flags := data[off+sedpSubmessageFlagsOff]

	var length int
	if flags&sedpFlagLittleEndian != 0 {
		length = int(binary.LittleEndian.Uint16(data[off+sedpSubmessageLengthOff : off+sedpSubmessageHeaderLen]))
	} else {
		length = int(binary.BigEndian.Uint16(data[off+sedpSubmessageLengthOff : off+sedpSubmessageHeaderLen]))
	}

	end := off + sedpSubmessageHeaderLen + length
	if end > len(data) {
		return 0, nil, 0, 0, fmt.Errorf("dds: sedp parse submessage %#02x length %d overruns %d left: %w", id, length, len(data)-off, ErrSEDPInvalidDATA)
	}

	body := append([]byte(nil), data[off+sedpSubmessageHeaderLen:end]...)

	return id, body, flags, end, nil
}

// sedpDATAWriter returns the writer entity of a DATA submessage body.
func sedpDATAWriter(body []byte) (SEDPEntityID, error) {
	var writer SEDPEntityID

	if len(body) < sedpDataFixedLen {
		return writer, fmt.Errorf("dds: sedp parse data need %d have %d: %w", sedpDataFixedLen, len(body), ErrSEDPInvalidDATA)
	}

	copy(writer[:], body[sedpDATAWriterOff:sedpDATAWriterOff+sedpEntityLen])

	return writer, nil
}

// sedpClassifyWriter reports whether a writer entity is a builtin SEDP
// endpoint writer and whether it announces readers.
func sedpClassifyWriter(writer SEDPEntityID) (isEndpoint, isReader bool) {
	if writer == sedpPublicationsWriter || writer == sedpPublicationsWriterCyclone {
		return true, false
	}

	if writer == sedpSubscriptionsWriter {
		return true, true
	}

	return false, false
}

// sedpDecodedIdentity holds the wire identity of one endpoint DATA body.
type sedpDecodedIdentity struct {
	// fields holds the endpoint GUID, topic, type, and userdata.
	fields sedpEndpointWireFields
	// payload holds the opaque QoS bytes after the inline params.
	payload []byte
	// isReader reports whether the writer announces readers.
	isReader bool
}

// sedpDecodeEndpoint decodes one DATA submessage body into an endpoint.
func sedpDecodeEndpoint(body []byte, flags byte) (SEDPEndpoint, error) {
	identity, err := sedpParseEndpointIdentity(body, flags)
	if err != nil {
		return SEDPEndpoint{}, fmt.Errorf("dds: sedp decode identity: %w", err)
	}

	ep, err := sedpAssembleEndpoint(identity)
	if err != nil {
		return SEDPEndpoint{}, fmt.Errorf("dds: sedp decode assemble: %w", err)
	}

	return ep, nil
}

// sedpParseEndpointIdentity decodes the writer, params, and identity fields
// of one DATA submessage body.
func sedpParseEndpointIdentity(body []byte, flags byte) (sedpDecodedIdentity, error) {
	var identity sedpDecodedIdentity

	writer, err := sedpDATAWriter(body)
	if err != nil {
		return identity, fmt.Errorf("dds: sedp parse identity writer: %w", err)
	}

	isEndpoint, isReader := sedpClassifyWriter(writer)
	if !isEndpoint {
		return identity, fmt.Errorf("dds: sedp parse data not from endpoint writer: %w", ErrSEDPInvalidDATA)
	}

	params, payload, err := sedpCollectParams(body, flags)
	if err != nil {
		return identity, fmt.Errorf("dds: sedp parse identity params: %w", err)
	}

	littleEndian := flags&sedpFlagLittleEndian != 0

	fields, err := sedpEndpointFields(params, littleEndian)
	if err != nil {
		return identity, fmt.Errorf("dds: sedp parse identity fields: %w", err)
	}

	identity = sedpDecodedIdentity{
		fields:   fields,
		payload:  payload,
		isReader: isReader,
	}

	return identity, nil
}

// sedpAssembleEndpoint maps wire identity to ROS names and builds the record.
func sedpAssembleEndpoint(identity sedpDecodedIdentity) (SEDPEndpoint, error) {
	decodedROS, kind, err := ParseTopicName(identity.fields.ddsTopic)
	if err != nil {
		sedpTracef("assemble reject topic dds=%q guid=%s err=%v", identity.fields.ddsTopic, identity.fields.id.String(), err)
		return SEDPEndpoint{}, fmt.Errorf("dds: sedp parse data topic: %w", err)
	}

	decodedIDL, err := DemangleTypeName(identity.fields.typeDDS)
	if err != nil {
		sedpTracef("assemble reject type typeDDS=%q guid=%s err=%v", identity.fields.typeDDS, identity.fields.id.String(), err)
		return SEDPEndpoint{}, fmt.Errorf("dds: sedp parse data type: %w", err)
	}

	userdata, err := sedpParseEndpointUserdata(identity.fields.userdata)
	if err != nil {
		sedpTracef("assemble reject userdata guid=%s err=%v", identity.fields.id.String(), err)
		return SEDPEndpoint{}, fmt.Errorf("dds: sedp assemble userdata: %w", err)
	}

	if userdata.hasDDSTopic && userdata.ddsTopic != identity.fields.ddsTopic {
		sedpTracef("assemble reject dds-topic-mismatch userdata=%q wire=%q guid=%s", userdata.ddsTopic, identity.fields.ddsTopic, identity.fields.id.String())
		return SEDPEndpoint{}, fmt.Errorf("dds: sedp parse data topic %q vs %q: %w", userdata.ddsTopic, identity.fields.ddsTopic, ErrSEDPInvalidDATA)
	}

	var domain uint32
	if userdata.hasDomain {
		domain = userdata.domain
	}

	ep := SEDPEndpoint{
		ID:                   identity.fields.id,
		ROSTopic:             decodedROS,
		DDSTopic:             identity.fields.ddsTopic,
		Kind:                 kind,
		TypeDDS:              identity.fields.typeDDS,
		TypeIDL:              decodedIDL,
		TypeHash:             userdata.hash,
		QoS:                  identity.payload,
		IsReader:             identity.isReader,
		ServiceID:            userdata.serviceID,
		ClientID:             userdata.clientID,
		DomainID:             domain,
		BuiltinEndpointQos:   identity.fields.builtinQos,
		TypeObject:           sedpCloneBytes(identity.fields.typeObject),
		Security:             identity.fields.security,
		Partitions:           append([]string(nil), identity.fields.partitions...),
		DurabilityService:    identity.fields.durabilityService,
		HasDurabilityService: identity.fields.hasDurabilityService,
	}
	sedpTracef("assemble accept guid=%s ros=%q dds=%q typeDDS=%q typeIDL=%q hash=%q domain=%d reader=%v", ep.ID.String(), ep.ROSTopic, ep.DDSTopic, ep.TypeDDS, ep.TypeIDL, ep.TypeHash, ep.DomainID, ep.IsReader)

	return ep, nil
}

// sedpEndpointWireFields holds the raw wire values for one endpoint.
type sedpEndpointWireFields struct {
	// id is the endpoint GUID from the ENDPOINT_GUID param.
	id SEDPGUID
	// ddsTopic is the DDS topic from the TOPIC_NAME param.
	ddsTopic string
	// typeDDS is the DDS type from the TYPE_NAME param.
	typeDDS string
	// userdata is the raw USER_DATA string, empty when absent.
	userdata string
	// builtinQos is the builtin endpoint QoS mask, zero when absent.
	builtinQos uint32
	// hasBuiltinQos reports whether the builtin QoS param was present.
	hasBuiltinQos bool
	// typeObject holds the raw XTypes TypeObject bytes when present.
	typeObject []byte
	// security carries the parsed DDS-Security attributes, zero when absent.
	security SecurityInfo
	// partitions lists the DDS partitions, empty for the default partition.
	partitions []string
	// durabilityService carries the parsed durability-service caps.
	durabilityService DurabilityServiceQoS
	// hasDurabilityService reports whether durability-service was present.
	hasDurabilityService bool
}

// sedpCollectParams gathers inline QoS params and the payload bytes.
//
// Payload params follow when the payload carries a parameter list
// encapsulation, which is the shape CycloneDDS sends. Weld sends inline
// params plus opaque QoS bytes only, so the payload is opaque and never a
// duplicate list. When inline already names the endpoint fully, a payload
// list is still stripped for backward compat with older Weld duplicates,
// but the list is ignored for identity with first-wins and the returned
// QoS holds only the opaque bytes after the list. When inline leaves
// identity incomplete, payload params supply the remainder and the
// returned QoS stays the full opaque payload for Cyclone compat. Otherwise
// the payload stays opaque.
func sedpCollectParams(body []byte, flags byte) ([]rtps.Param, []byte, error) {
	littleEndian := flags&sedpFlagLittleEndian != 0

	if len(body) < sedpDataFixedLen {
		return nil, nil, fmt.Errorf("dds: sedp collect params need %d have %d: %w", sedpDataFixedLen, len(body), ErrSEDPInvalidDATA)
	}

	var params []rtps.Param

	rest := body[sedpDataFixedLen:]

	if flags&sedpFlagInlineQoS != 0 {
		parsed, consumed, err := sedpDecodeParams(rest, littleEndian)
		if err != nil {
			return nil, nil, fmt.Errorf("dds: sedp collect inline params: %w", err)
		}

		params = append(params, parsed...)
		rest = rest[consumed:]
	}

	var payload []byte
	if flags&(sedpFlagDataPresent|sedpFlagKeyPresent) == 0 {
		return params, nil, nil
	}

	hasIdentity := sedpHasEndpointIdentity(params)

	extra, consumed, err := sedpDecodePayloadParams(rest, littleEndian, hasIdentity)
	if err != nil {
		return nil, nil, fmt.Errorf("dds: sedp collect payload params: %w", err)
	}

	if hasIdentity {
		if consumed > 0 {
			payload = append([]byte(nil), rest[consumed:]...)
		} else {
			payload = append([]byte(nil), rest...)
		}

		return params, payload, nil
	}

	payload = append([]byte(nil), rest...)

	return append(params, extra...), payload, nil
}

// sedpDecodePayloadParams parses a parameter list payload when present.
//
// It returns the param list plus the total consumed bytes including the 4
// byte encapsulation header. When inline already names the endpoint fully,
// the list is still parsed so the caller can strip the prefix and recover
// the opaque QoS after it for backward compat with older Weld duplicates;
// the caller ignores the list for identity with first-wins. When inline
// leaves identity incomplete, the list supplies the remainder, which is
// the shape CycloneDDS sends. Otherwise nothing is returned with zero
// consumed.
func sedpDecodePayloadParams(rest []byte, littleEndian, hasEndpointIdentity bool) ([]rtps.Param, int, error) {
	if len(rest) < sedpEncapLen {
		return nil, 0, nil
	}

	if !sedpIsParamListEncap(rest[:sedpEncapLen]) {
		return nil, 0, nil
	}

	list, consumed, err := sedpDecodeParams(rest[sedpEncapLen:], littleEndian)
	if err != nil {
		return nil, 0, fmt.Errorf("dds: sedp parse payload params: %w", err)
	}

	if hasEndpointIdentity {
		return nil, sedpEncapLen + consumed, nil
	}

	return list, sedpEncapLen + consumed, nil
}

// sedpHasEndpointIdentity reports whether params name the endpoint fully.
func sedpHasEndpointIdentity(params []rtps.Param) bool {
	var topic, dtype, guid bool

	for _, param := range params {
		switch param.ID {
		case rtps.PIDTopicName:
			topic = true
		case rtps.PIDTypeName:
			dtype = true
		case rtps.PIDEndpointGUIDWire, rtps.PIDEndpointGUID:
			guid = true
		}
	}

	return topic && dtype && guid
}

// sedpIsParamListEncap reports whether 4 encapsulation bytes mark a
// parameter list payload.
//
// The encapsulation kind is an octet pair in fixed order, not subject
// to the DATA endianness: 0x0002 and 0x0003 select big and little endian
// parameter lists, which CycloneDDS uses for endpoint DATA. Plain CDR
// kinds 0x0000 and 0x0001 stay opaque.
func sedpIsParamListEncap(raw []byte) bool {
	if len(raw) < sedpEncapLen {
		return false
	}

	kind := uint16(raw[0])<<8 | uint16(raw[1])

	return kind == sedpEncapPLCDRBE || kind == sedpEncapPLCDRLE
}

// sedpFieldsIdentity extracts topic, type, and GUID identity fields.
func sedpFieldsIdentity(byID map[uint16][]byte, littleEndian bool, fields *sedpEndpointWireFields) error {
	topic, err := sedpRequiredString(byID, rtps.PIDTopicName, littleEndian)
	if err != nil {
		return fmt.Errorf("dds: sedp parse fields topic: %w", err)
	}

	fields.ddsTopic = topic

	dtype, err := sedpRequiredString(byID, rtps.PIDTypeName, littleEndian)
	if err != nil {
		return fmt.Errorf("dds: sedp parse fields type: %w", err)
	}

	fields.typeDDS = dtype

	rawGUID, seen := byID[rtps.PIDEndpointGUIDWire]
	if !seen {
		rawGUID, seen = byID[rtps.PIDEndpointGUID]
	}
	if !seen {
		return fmt.Errorf("dds: sedp parse data misses endpoint GUID: %w", ErrSEDPInvalidDATA)
	}

	id, err := sedpDecodeEndpointGUID(rawGUID)
	if err != nil {
		return fmt.Errorf("dds: sedp parse fields guid: %w", err)
	}

	fields.id = id

	if fields.id.Entity == sedpEntityUnknown {
		return fmt.Errorf("dds: sedp parse data zero entity: %w", ErrSEDPInvalidDATA)
	}

	return nil
}

// sedpFieldsOptionals extracts userdata, builtin QoS, and type object.
func sedpFieldsOptionals(byID map[uint16][]byte, littleEndian bool, fields *sedpEndpointWireFields) error {
	if rawUserdata, seen := byID[rtps.PIDUserData]; seen {
		userdata, err := sedpDecodeOctetSequence(rawUserdata, littleEndian)
		if err != nil {
			return fmt.Errorf("dds: sedp parse fields userdata: %w", err)
		}

		fields.userdata = userdata
	}

	if rawQos, seen := byID[rtps.PIDBuiltinEndpointQos]; seen {
		qos, err := sedpDecodeBuiltinQos(rawQos, littleEndian)
		if err != nil {
			return fmt.Errorf("dds: sedp parse fields builtin qos: %w", err)
		}

		fields.builtinQos = qos
		fields.hasBuiltinQos = true
	}

	if rawObject, seen := byID[rtps.PIDTypeObject]; seen {
		fields.typeObject = append([]byte(nil), rawObject...)
	}

	return nil
}

// sedpFieldsExtensions extracts security, partitions, and durability service.
func sedpFieldsExtensions(params []rtps.Param, byID map[uint16][]byte, littleEndian bool, fields *sedpEndpointWireFields) {
	secInfo, secErrs := ExtractSecurityInfo(params)
	for _, secErr := range secErrs {
		sedpTracef("parse security skip err=%v", secErr)
	}

	fields.security = secInfo

	err := CheckProtectedDiscovery(secInfo)
	if err != nil {
		sedpTracef("parse secured peer crypto unavailable err=%v", err)
	}

	partitionNames, err := FindPartitionParam(params)
	if err != nil {
		sedpTracef("parse partition skip err=%v", err)
	} else {
		fields.partitions = partitionNames
	}

	if rawDurabilityService, seen := byID[PIDDurabilityService]; seen {
		durabilityService, err := DecodeDurabilityService(rawDurabilityService, littleEndian)
		if err != nil {
			sedpTracef("parse durability-service skip err=%v", err)
		} else {
			fields.durabilityService = durabilityService
			fields.hasDurabilityService = true
		}
	}
}

// sedpEndpointFields extracts the endpoint identity from params.
//
// The first TOPIC_NAME, TYPE_NAME, ENDPOINT_GUID, and USER_DATA params
// win; duplicates and unknown ids, including vendor 0x8003, 0x8007,
// 0x800c, and 0x8019, are ignored. All three identity
// params are required. The endpoint GUID must be 16 bytes with a non
// zero entity. PIDBuiltinEndpointQos and PIDTypeObject are optional:
// builtin QoS must be 4 bytes when present, TypeObject bytes ride as opaque
// passthrough.
func sedpEndpointFields(params []rtps.Param, littleEndian bool) (sedpEndpointWireFields, error) {
	var fields sedpEndpointWireFields

	byID := sedpFirstParams(params)

	err := sedpFieldsIdentity(byID, littleEndian, &fields)
	if err != nil {
		return fields, err
	}

	err = sedpFieldsOptionals(byID, littleEndian, &fields)
	if err != nil {
		return fields, err
	}

	sedpFieldsExtensions(params, byID, littleEndian, &fields)

	return fields, nil
}

// sedpFirstParams keeps the first value per parameter id.
//
// Duplicates and unknown ids are skipped by callers; the first
// occurrence of each identity param wins.
func sedpFirstParams(params []rtps.Param) map[uint16][]byte {
	byID := make(map[uint16][]byte, len(params))

	for _, param := range params {
		if _, seen := byID[param.ID]; seen {
			continue
		}

		byID[param.ID] = param.Value
	}

	return byID
}

// sedpRequiredString extracts a required CDR string param.
func sedpRequiredString(byID map[uint16][]byte, id uint16, littleEndian bool) (string, error) {
	raw, seen := byID[id]
	if !seen {
		return "", fmt.Errorf("dds: sedp parse data misses param %#04x: %w", id, ErrSEDPInvalidDATA)
	}

	value, err := sedpDecodeCDRString(raw, littleEndian)
	if err != nil {
		return "", fmt.Errorf("dds: sedp parse string param %#04x: %w", id, err)
	}

	return value, nil
}

// sedpDecodeEndpointGUID decodes a 16 byte endpoint GUID value.
func sedpDecodeEndpointGUID(value []byte) (SEDPGUID, error) {
	var id SEDPGUID

	if len(value) != sedpGUIDLen {
		return id, fmt.Errorf("dds: sedp parse data endpoint GUID length %d: %w", len(value), ErrSEDPInvalidDATA)
	}

	copy(id.Prefix[:], value[:sedpPrefixLen])
	copy(id.Entity[:], value[sedpPrefixLen:sedpGUIDLen])

	return id, nil
}

// sedpValidateRTPSHeader validates the 20 byte RTPS message header.
func sedpValidateRTPSHeader(data []byte) error {
	if len(data) < sedpRTPSHeaderLen {
		return fmt.Errorf("dds: sedp parse header need %d have %d: %w", sedpRTPSHeaderLen, len(data), ErrSEDPInvalidDATA)
	}

	if string(data[0:4]) != sedpRTPSMagic {
		return fmt.Errorf("dds: sedp parse header bad magic: %w", ErrSEDPInvalidDATA)
	}

	if data[sedpRTPSMajorOff] != sedpRTPSMajor {
		return fmt.Errorf("dds: sedp parse header major version %d: %w", data[sedpRTPSMajorOff], ErrSEDPInvalidDATA)
	}

	return nil
}

// sedpDecodeParams parses a sentinel terminated parameter list in the
// given byte order via the shared rtps PID codec.
//
// Unknown ids are kept so callers skip what they do not understand.
// Senders pad two ways: the length may exclude the pad with external
// padding, or include it as observed from CycloneDDS. Both consume the
// same bytes because the pad rounds the total to 4. The list is capped
// at sedpMaxParams entries for bounded work, and rtps errors join
// ErrSEDPInvalidDATA so errors.Is keeps working.
func sedpDecodeParams(blob []byte, littleEndian bool) ([]rtps.Param, int, error) {
	params, consumed, err := rtps.DecodeParamsEndian(blob, littleEndian)
	if err != nil {
		return nil, 0, fmt.Errorf("dds: sedp parse params: %w", errors.Join(err, ErrSEDPInvalidDATA))
	}

	if len(params) > sedpMaxParams {
		return nil, 0, fmt.Errorf("dds: sedp parse param count above %d: %w", sedpMaxParams, ErrSEDPInvalidDATA)
	}

	return params, consumed, nil
}

// sedpAppendLengthPrefix appends a little endian CDR length prefix.
func sedpAppendLengthPrefix(out []byte, length int) []byte {
	return binary.LittleEndian.AppendUint32(out, uint32(length)) //nolint:gosec // G115: lengths are capped by sedpMaxNameLen
}

// sedpLengthPrefix parses the 4 byte CDR length prefix in the given order.
func sedpLengthPrefix(value []byte, littleEndian bool) (int, error) {
	if len(value) < sedpLengthPrefixLen {
		return 0, fmt.Errorf("dds: sedp parse length need %d have %d: %w", sedpLengthPrefixLen, len(value), ErrSEDPInvalidDATA)
	}

	if littleEndian {
		return int(binary.LittleEndian.Uint32(value[:sedpLengthPrefixLen])), nil
	}

	return int(binary.BigEndian.Uint32(value[:sedpLengthPrefixLen])), nil
}

// sedpEncodeCDRString renders a CDR string in little endian order.
//
// Layout is the 4 byte length including the NUL terminator plus pad,
// then the bytes plus NUL plus zero pad to 4 bytes. The length includes
// pad, matching CycloneDDS on the wire, whose quickscan rejects lengths
// with mod 4 != 0.
func sedpEncodeCDRString(s string) []byte {
	out := sedpAppendLengthPrefix(make([]byte, 0, sedpLengthPrefixLen+len(s)+4), len(s)+1)
	out = append(out, s...)
	out = append(out, 0x00)

	for len(out)%4 != 0 {
		out = append(out, 0x00)
	}

	// Rewrite the length prefix to include pad.
	binary.LittleEndian.PutUint32(out[:sedpLengthPrefixLen], uint32(len(out)-sedpLengthPrefixLen)) //nolint:gosec // G115: names are capped by sedpMaxNameLen plus pad.

	return out
}

// sedpDecodeCDRString parses a CDR string value in the given order.
//
// The length includes the NUL terminator and may include zero pad to 4
// bytes, matching CycloneDDS. The first NUL terminates the string; bytes
// after NUL up to length are pad and ignored. Lengths above the name cap
// plus pad and content that overruns the value are rejected. The decoded
// string itself must fit the cap.
func sedpDecodeCDRString(value []byte, littleEndian bool) (string, error) {
	length, err := sedpLengthPrefix(value, littleEndian)
	if err != nil {
		return "", fmt.Errorf("dds: sedp parse string length: %w", err)
	}

	if length < 1 || length > sedpMaxNameLen+4 {
		return "", fmt.Errorf("dds: sedp parse string length %d: %w", length, ErrSEDPInvalidDATA)
	}

	if len(value) < sedpLengthPrefixLen+length {
		return "", fmt.Errorf("dds: sedp parse string need %d have %d: %w", sedpLengthPrefixLen+length, len(value), ErrSEDPInvalidDATA)
	}

	raw := value[sedpLengthPrefixLen : sedpLengthPrefixLen+length]

	nul := -1

	for i, b := range raw {
		if b == 0x00 {
			nul = i

			break
		}
	}

	if nul < 0 {
		return "", fmt.Errorf("dds: sedp parse string misses NUL terminator: %w", ErrSEDPInvalidDATA)
	}

	out := string(raw[:nul])

	if len(out) > sedpMaxNameLen {
		return "", fmt.Errorf("dds: sedp parse string length %d above cap: %w", len(out), ErrSEDPInvalidDATA)
	}

	return out, nil
}

// sedpEncodeOctetSequence renders a CDR octet sequence in little endian.
//
// Layout is the 4 byte octet count including pad plus the raw bytes plus
// zero pad to 4 bytes. The count includes pad, matching CycloneDDS.
func sedpEncodeOctetSequence(raw []byte) []byte {
	out := sedpAppendLengthPrefix(make([]byte, 0, sedpLengthPrefixLen+len(raw)+4), len(raw))
	out = append(out, raw...)

	for len(out)%4 != 0 {
		out = append(out, 0x00)
	}

	binary.LittleEndian.PutUint32(out[:sedpLengthPrefixLen], uint32(len(out)-sedpLengthPrefixLen)) //nolint:gosec // G115: UserData is capped by sedpMaxNameLen plus pad.

	return out
}

// sedpDecodeOctetSequence parses a CDR octet sequence in the given order.
//
// The count may include zero pad to 4 bytes, matching CycloneDDS. Trailing
// zero pad is trimmed so UserData parses without stray NULs. Counts above
// the cap plus pad, and decoded content above the cap, are rejected.
func sedpDecodeOctetSequence(value []byte, littleEndian bool) (string, error) {
	count, err := sedpLengthPrefix(value, littleEndian)
	if err != nil {
		return "", fmt.Errorf("dds: sedp parse sequence length: %w", err)
	}

	if count > sedpMaxNameLen+4 {
		return "", fmt.Errorf("dds: sedp parse sequence length %d: %w", count, ErrSEDPInvalidDATA)
	}

	if len(value) < sedpLengthPrefixLen+count {
		return "", fmt.Errorf("dds: sedp parse sequence need %d have %d: %w", sedpLengthPrefixLen+count, len(value), ErrSEDPInvalidDATA)
	}

	raw := string(value[sedpLengthPrefixLen : sedpLengthPrefixLen+count])

	for len(raw) > 0 && raw[len(raw)-1] == 0x00 {
		raw = raw[:len(raw)-1]
	}

	if len(raw) > sedpMaxNameLen {
		return "", fmt.Errorf("dds: sedp parse sequence length %d above cap: %w", len(raw), ErrSEDPInvalidDATA)
	}

	return raw, nil
}

// sedpEncodeEndpointGUID renders the 16 byte endpoint GUID value.
func sedpEncodeEndpointGUID(id SEDPGUID) []byte {
	out := make([]byte, 0, 16)
	out = append(out, id.Prefix[:]...)
	out = append(out, id.Entity[:]...)

	return out
}

// sedpEncodeReliability renders reliable reliability with infinite blocking.
func sedpEncodeReliability() []byte {
	out := make([]byte, 0, 12)
	out = binary.LittleEndian.AppendUint32(out, sedpReliabilityReliable)
	out = binary.LittleEndian.AppendUint32(out, sedpReliabilityInfiniteSec)
	out = binary.LittleEndian.AppendUint32(out, sedpReliabilityInfiniteFrac)

	return out
}

// sedpEncodeHistory renders keep-last history with the facade depth.
// History kind and depth stay RxO neutral for matching.
func sedpEncodeHistory() []byte {
	out := make([]byte, 0, 8)
	out = binary.LittleEndian.AppendUint32(out, sedpHistoryKeepLast)
	out = binary.LittleEndian.AppendUint32(out, sedpHistoryDepth)

	return out
}

// sedpEncodeProtocolVersion renders RTPS version 2.1 with pad for CycloneDDS.
func sedpEncodeProtocolVersion() []byte {
	return []byte{sedpRTPSMajor, sedpRTPSMinor, 0x00, 0x00}
}

// sedpEncodeVendorID renders the Eclipse CycloneDDS vendor id with pad.
func sedpEncodeVendorID() []byte {
	return []byte{sedpVendorID0, sedpVendorID1, 0x00, 0x00}
}

// sedpEncodeDurability renders the durability kind for one endpoint.
//
// Service readers announce volatile to match ROS service writers, which
// offer volatile. All other endpoints announce transient local.
func sedpEncodeDurabilityFor(ep SEDPEndpoint) []byte {
	if ep.VolatileDurability {
		return binary.LittleEndian.AppendUint32(nil, sedpDurabilityVolatile)
	}
	return binary.LittleEndian.AppendUint32(nil, sedpDurabilityTransientLocal)
}

// sedpEncodeBuiltinQos renders the 4 byte builtin endpoint QoS mask.
func sedpEncodeBuiltinQos(mask uint32) []byte {
	return binary.LittleEndian.AppendUint32(nil, mask)
}

// sedpDecodeBuiltinQos parses a 4 byte builtin endpoint QoS mask.
func sedpDecodeBuiltinQos(value []byte, littleEndian bool) (uint32, error) {
	if len(value) != 4 {
		return 0, fmt.Errorf("dds: sedp parse builtin qos length %d: %w", len(value), ErrSEDPInvalidDATA)
	}

	if littleEndian {
		return binary.LittleEndian.Uint32(value), nil
	}

	return binary.BigEndian.Uint32(value), nil
}

// sedpEncodeEndpointUserdata builds the USER_DATA content for Weld peers.
//
// Layout is typehash plus DDS topic plus domain in fixed order, split on
// semicolons when parsing. Values must not carry separators.
func sedpEncodeEndpointUserdata(typeHash, ddsTopic string, domain uint32) (string, error) {
	err := sedpCheckWireValues(ddsTopic, typeHash)
	if err != nil {
		return "", fmt.Errorf("dds: sedp encode userdata values: %w", err)
	}

	return sedpUserdataKeyTypeHash + "=" + typeHash + ";" +
		sedpUserdataKeyDDSTopic + "=" + ddsTopic + ";" +
		sedpUserdataKeyDomain + "=" + strconv.FormatUint(uint64(domain), 10), nil
}

// sedpParseEndpointUserdata parses USER_DATA pairs and tolerates unknown keys.
func sedpParseEndpointUserdata(raw string) (sedpUserdataFields, error) {
	var fields sedpUserdataFields

	for chunk := range strings.SplitSeq(raw, ";") {
		if chunk == "" {
			continue
		}

		key, value, found := strings.Cut(chunk, "=")
		if !found {
			return fields, fmt.Errorf("dds: sedp parse userdata %q misses equals: %w", chunk, ErrSEDPInvalidDATA)
		}

		err := sedpApplyUserdataField(&fields, key, value)
		if err != nil {
			return fields, err
		}
	}

	return fields, nil
}

// sedpApplyUserdataField applies one USER_DATA pair to fields.
func sedpApplyUserdataField(fields *sedpUserdataFields, key, value string) error {
	switch key {
	case sedpUserdataKeyTypeHash:
		fields.hash = value
		return nil
	case sedpUserdataKeyDDSTopic:
		fields.ddsTopic = value
		fields.hasDDSTopic = true
		return nil
	case sedpUserdataKeyDomain:
		domain, err := sedpParseUserdataDomain(value)
		if err != nil {
			return err
		}
		fields.domain = domain
		fields.hasDomain = true
		return nil
	case serviceUserdataKey:
		// Tolerate any non-empty value, including ROS dot-separated
		// form and legacy plain hex: matching compares opaquely and
		// strict validation here would drop discovery of valid peers.
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			fields.serviceID = trimmed
			fields.hasServiceID = true
		}
		return nil
	case clientUserdataKey:
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			fields.clientID = trimmed
			fields.hasClientID = true
		}
		return nil
	default:
		return nil
	}
}

// sedpParseUserdataDomain parses one domain value for USER_DATA.
func sedpParseUserdataDomain(value string) (uint32, error) {
	domain, err := strconv.ParseUint(value, 10, 32)
	if err != nil {
		return 0, fmt.Errorf("dds: sedp parse userdata domain %q: %w", value, ErrSEDPInvalidDomain)
	}
	if domain > uint64(sedpMaxDomainID) {
		return 0, fmt.Errorf("dds: sedp parse userdata domain %d: %w", domain, ErrSEDPInvalidDomain)
	}
	return uint32(domain), nil
}

// sedpCheckWireValues rejects separators that would break the encoding.
func sedpCheckWireValues(ddsTopic, typeHash string) error {
	for _, value := range []string{ddsTopic, typeHash} {
		if strings.ContainsAny(value, ";=\n\r") {
			return fmt.Errorf("dds: sedp userdata value %q has a separator: %w", value, ErrSEDPInvalidEndpoint)
		}
	}

	if strings.TrimSpace(ddsTopic) == "" {
		return fmt.Errorf("dds: sedp userdata dds topic is empty: %w", ErrSEDPInvalidTopic)
	}

	return nil
}

// sedpValidateRemote checks minimal fields for a remote record.
func sedpValidateRemote(ep SEDPEndpoint) error {
	if ep.ID.Entity == (SEDPEntityID{}) {
		return fmt.Errorf("dds: sedp remote zero entity: %w", ErrSEDPInvalidEndpoint)
	}

	if strings.TrimSpace(ep.ROSTopic) == "" {
		return fmt.Errorf("dds: sedp remote empty ros topic: %w", ErrSEDPInvalidTopic)
	}

	if strings.TrimSpace(ep.DDSTopic) == "" {
		return fmt.Errorf("dds: sedp remote empty dds topic: %w", ErrSEDPInvalidTopic)
	}

	if strings.TrimSpace(ep.TypeDDS) == "" {
		return fmt.Errorf("dds: sedp remote empty type: %w", ErrSEDPInvalidType)
	}

	if ep.DomainID > sedpMaxDomainID {
		return fmt.Errorf("dds: sedp remote domain %d: %w", ep.DomainID, ErrSEDPInvalidDomain)
	}

	return nil
}

// sedpValidateMarshalEndpoint checks mapping consistency before serializing.
func sedpValidateMarshalEndpoint(ep SEDPEndpoint) error {
	if ep.ID.Entity == (SEDPEntityID{}) {
		return fmt.Errorf("dds: sedp serialize zero entity: %w", ErrSEDPInvalidEndpoint)
	}

	if ep.DomainID > sedpMaxDomainID {
		return fmt.Errorf("dds: sedp serialize domain %d: %w", ep.DomainID, ErrSEDPInvalidDomain)
	}

	decodedROS, kind, err := ParseTopicName(ep.DDSTopic)
	if err != nil {
		return fmt.Errorf("dds: sedp serialize topic: %w", err)
	}

	if decodedROS != ep.ROSTopic {
		return fmt.Errorf("dds: sedp serialize topic %q vs %q: %w", ep.ROSTopic, decodedROS, ErrSEDPInvalidTopic)
	}

	if ep.Kind != "" && ep.Kind != kind {
		return fmt.Errorf("dds: sedp serialize kind %q vs %q: %w", ep.Kind, kind, ErrSEDPInvalidTopic)
	}

	decodedIDL, err := DemangleTypeName(ep.TypeDDS)
	if err != nil {
		return fmt.Errorf("dds: sedp serialize type: %w", err)
	}

	if ep.TypeIDL != "" && ep.TypeIDL != decodedIDL {
		return fmt.Errorf("dds: sedp serialize idl %q vs %q: %w", ep.TypeIDL, decodedIDL, ErrSEDPInvalidType)
	}

	err = ValidatePartitionList(ep.Partitions)
	if err != nil {
		return fmt.Errorf("dds: sedp serialize partitions: %w", err)
	}

	return nil
}

// sedpEntityFor builds a user entity id above the builtin range.
func sedpEntityFor(counter uint32, isReader bool) SEDPEntityID {
	kind := sedpUserWriterKind
	if isReader {
		kind = sedpUserReaderKind
	}

	var raw [4]byte
	binary.BigEndian.PutUint32(raw[:], counter)

	return SEDPEntityID{
		raw[1],
		raw[2],
		raw[3],
		kind,
	}
}

// sedpEndpointsEqual compares records including QoS bytes.
//
// QoS bytes compare with bytes.Equal, so nil and empty QoS compare equal.
// Both mean no opaque payload was announced, and the distinction carries
// no matching or generation semantics. Builtin QoS and TypeObject also
// compare, with nil and empty TypeObject equal. Provisional compares so
// promotion bumps generation; FirstSeen is metadata and is ignored.
func sedpEndpointsEqual(a, b SEDPEndpoint) bool {
	return sedpEndpointsIdentityEqual(a, b) && sedpEndpointsAttrsEqual(a, b)
}

// sedpEndpointsIdentityEqual compares routing identity fields.
func sedpEndpointsIdentityEqual(a, b SEDPEndpoint) bool {
	if a.Provisional != b.Provisional {
		return false
	}
	if a.ID != b.ID {
		return false
	}
	if a.ROSTopic != b.ROSTopic {
		return false
	}
	if a.DDSTopic != b.DDSTopic {
		return false
	}
	if a.Kind != b.Kind {
		return false
	}
	if a.TypeDDS != b.TypeDDS {
		return false
	}
	return true
}

// sedpEndpointsTypeEqual compares type identity fields.
func sedpEndpointsTypeEqual(a, b SEDPEndpoint) bool {
	if a.TypeIDL != b.TypeIDL {
		return false
	}

	if a.TypeHash != b.TypeHash {
		return false
	}

	if a.IsReader != b.IsReader {
		return false
	}

	return a.DomainID == b.DomainID
}

// sedpEndpointsQoSBytesEqual compares opaque QoS payloads and offers.
func sedpEndpointsQoSBytesEqual(a, b SEDPEndpoint) bool {
	if (a.WireOffer == nil) != (b.WireOffer == nil) {
		return false
	}

	if a.WireOffer != nil && *a.WireOffer != *b.WireOffer {
		return false
	}

	return bytes.Equal(a.QoS, b.QoS)
}

// sedpEndpointsDurabilityEqual compares durability service records.
func sedpEndpointsDurabilityEqual(a, b SEDPEndpoint) bool {
	if a.HasDurabilityService != b.HasDurabilityService {
		return false
	}

	return !a.HasDurabilityService || a.DurabilityService == b.DurabilityService
}

// sedpEndpointsAttrsEqual compares type and QoS attributes.
func sedpEndpointsAttrsEqual(a, b SEDPEndpoint) bool {
	if !sedpEndpointsTypeEqual(a, b) {
		return false
	}
	if a.BuiltinEndpointQos != b.BuiltinEndpointQos {
		return false
	}
	if !bytes.Equal(a.TypeObject, b.TypeObject) {
		return false
	}
	if !slices.Equal(a.Partitions, b.Partitions) {
		return false
	}
	if !securityInfoEqual(a.Security, b.Security) {
		return false
	}
	if !sedpEndpointsDurabilityEqual(a, b) {
		return false
	}
	return sedpEndpointsQoSBytesEqual(a, b)
}

// securityInfoFlagsEqual compares scalar security flags and classification.
func securityInfoFlagsEqual(a, b SecurityInfo) bool {
	return a.Present == b.Present &&
		a.HasParticipantSecurityInfo == b.HasParticipantSecurityInfo &&
		a.ParticipantSecurityAttributes == b.ParticipantSecurityAttributes &&
		a.HasEndpointSecurityInfo == b.HasEndpointSecurityInfo &&
		a.EndpointSecurityAttributes == b.EndpointSecurityAttributes &&
		a.IdentityTokenPresent == b.IdentityTokenPresent &&
		a.PermissionsTokenPresent == b.PermissionsTokenPresent &&
		a.DataTagsPresent == b.DataTagsPresent &&
		a.IdentityStatusPresent == b.IdentityStatusPresent &&
		a.Classification == b.Classification
}

// securityInfoPropertiesEqual compares decoded property maps by content.
func securityInfoPropertiesEqual(a, b SecurityInfo) bool {
	if len(a.Properties) != len(b.Properties) {
		return false
	}

	for k, v := range a.Properties {
		if bv, ok := b.Properties[k]; !ok || bv != v {
			return false
		}
	}

	return true
}

// securityInfoEqual compares security records including properties.
//
// Maps compare by content; nil and empty maps compare equal since both
// mean no decoded properties. A classification change bumps generation
// so the graph reflects a newly secured peer.
func securityInfoEqual(a, b SecurityInfo) bool {
	if !securityInfoFlagsEqual(a, b) {
		return false
	}

	return securityInfoPropertiesEqual(a, b)
}

// sedpCloneEndpoint copies an endpoint including its QoS bytes.
func sedpCloneEndpoint(ep SEDPEndpoint) SEDPEndpoint {
	ep.QoS = sedpCloneBytes(ep.QoS)
	ep.TypeObject = sedpCloneBytes(ep.TypeObject)
	ep.Partitions = append([]string(nil), ep.Partitions...)
	if ep.WireOffer != nil {
		offer := *ep.WireOffer
		ep.WireOffer = &offer
	}

	return ep
}

// sedpCloneBytes copies bytes and preserves nil.
func sedpCloneBytes(b []byte) []byte {
	if b == nil {
		return nil
	}

	out := make([]byte, len(b))
	copy(out, b)

	return out
}

// sedpSortEndpoints orders endpoints by GUID for deterministic output.
func sedpSortEndpoints(eps []SEDPEndpoint) {
	slices.SortFunc(eps, func(a, b SEDPEndpoint) int {
		prefixCmp := bytes.Compare(a.ID.Prefix[:], b.ID.Prefix[:])
		if prefixCmp != 0 {
			return prefixCmp
		}

		return bytes.Compare(a.ID.Entity[:], b.ID.Entity[:])
	})
}
