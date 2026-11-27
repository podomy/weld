// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

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
// SEDP writer. Publications use writer entity 0x000003c2 and
// subscriptions use writer entity 0x000004c2, with inline QoS PID
// params carrying the topic name, type name, endpoint GUID,
// reliable transient local QoS with keep-last history, protocol version,
// vendor id, and userdata with the type hash. Topic announcements use
// writer entity 0x000002c2 and carry only the topic plus type names, never
// endpoint DATA. Parsing
// accepts the same shape from any peer and additionally tolerates the
// observed CycloneDDS wire form, which carries endpoint params in the
// DATA payload instead of inline QoS.
// See the capture notes on the Cyclone
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
	"log"
	"maps"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/podomy/weld/internal/core"
	"github.com/podomy/weld/internal/transports/dds/rtps"
	ddssecurity "github.com/podomy/weld/internal/transports/dds/security"
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
	// sedpRTPSMinor is the sent RTPS minor version per DDSI-RTPS 2.5.
	// Peers may send any 2.x minor; decode accepts every minor under major 2.
	sedpRTPSMinor byte = 5
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
	// sedpMaxSubmessages caps one datagram submessage walk for bounded work.
	//
	// The bound matches rtps maxSubmessagesPerDatagram (4096) so the SEDP
	// split walk and the rtps decode path agree; over-cap datagrams drop
	// loudly before dispatch. The walk stays cheap (one header parse per
	// entry with no allocation past collected DATA bodies), so the wider
	// bound keeps batched discovery with headroom while still bounding
	// unauthenticated work.
	sedpMaxSubmessages = 4096
	// sedpGapNoBoundCap caps a GAP advance with no heartbeat bound known.
	//
	// Sixty four is one user repair window: a writer that never announced
	// a heartbeat bound cannot jump the high more than one window past
	// the current high, so a crafted GAP cannot teleport tracking.
	sedpGapNoBoundCap uint64 = 64
	// sedpSubmessageGap selects GAP submessages.
	sedpSubmessageGap byte = 0x08
	// sedpSubmessageInfoDst selects INFO_DST submessages.
	sedpSubmessageInfoDst byte = 0x0E
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

// SEDP builtin endpoint entities per DDSI-RTPS 2.5 section 8.5.4.3.
//
// Publications carry writer announcements and subscriptions carry reader
// announcements. The Topics writer carries TopicBuiltinTopicData, never
// endpoint DATA. The entity values are the spec values, unambiguous by
// low octet: 0x02 Topics, 0x03 publications, 0x04 subscriptions. The
// values match the observed CycloneDDS wire form: the live captures in
// sedp_test.go show publications from 0x000003c2 and subscriptions from
// 0x000004c2. Reader ids are always sent as unknown and any received
// reader id is accepted.
var (
	// sedpTopicsWriter is the SEDP Topics writer for TopicBuiltinTopicData.
	sedpTopicsWriter = SEDPEntityID{0x00, 0x00, 0x02, 0xC2}
	// sedpTopicsReader is the SEDP Topics reader.
	sedpTopicsReader = SEDPEntityID{0x00, 0x00, 0x02, 0xC7}
	// sedpPublicationsWriter is the SEDP publications writer.
	sedpPublicationsWriter = SEDPEntityID{0x00, 0x00, 0x03, 0xC2}
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
	// ErrSEDPStale reports a replayed DATA sequence at or below the last accepted value.
	ErrSEDPStale = errors.New("dds: stale sedp sequence")
	// ErrSEDPInfoDst reports an INFO_DST naming another participant on unicast ingress.
	ErrSEDPInfoDst = errors.New("dds: sedp INFO_DST mismatch")
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
//
// The map is process wide: two SEDPIndex values sharing a participant
// prefix share one counter. That is safe because production runs one
// index per backend, and tests must not assert exact sequence values,
// only contiguity within one publisher stream.
var sedpWriterSeqLast = map[sedpWriterKey]uint64{}

// sedpWriterKey identifies one builtin SEDP writer stream.
type sedpWriterKey struct {
	// prefix is the participant GUID prefix.
	prefix SEDPGUIDPrefix
	// writer is the builtin writer entity id.
	writer SEDPEntityID
}

// heartbeatCount counts sent SEDP and user DATA HEARTBEAT submessages process wide.
//
// Like sedpWriterSeqLast this is intentionally global: heartbeat counts
// only need process-wide monotonicity for duplicate suppression, never
// per-writer contiguity, so tests must not assert exact values.
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
	// DomainTag is the DDS domain tag the endpoint belongs to, empty for
	// the default tag. The wire carries no tag today, so decoded endpoints
	// always carry empty; UpsertRemote enforces exact equality with the
	// index tag fail-closed per 8.5.5.1 (empty equals only empty, no
	// wildcards).
	DomainTag string
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
	// RxOWire carries the effective wire QoS for RxO matching. Local
	// declares populate it from the facade or WireOffer; remote ingest
	// populates it on successful parse and leaves it nil on failure with
	// RxOMalformed set. Match requires both sides populated; malformed
	// fails closed, while endpoints predating QoS capture keep legacy
	// behavior until repopulated.
	RxOWire *WireQoS
	// RxOMalformed reports a failed remote QoS parse. Malformed peers
	// never match, per fail-closed default.
	RxOMalformed bool
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
// changes an entry and each single remove bumps the counter once and
// fires the change hook outside the lock. Batch removals in Sweep,
// ExpireProvisional, and ExpirePendingEvictions bump once per call, and
// hidden state such as lastSeq, hbLast, hbCount, writerHigh, and
// pendingEvict never bumps alone. Local declares do not affect the
// generation. Use NewSEDPIndex. It is safe for concurrent use.
type SEDPIndex struct {
	mu           sync.Mutex
	prefix       SEDPGUIDPrefix
	domain       uint32
	domainTag    string
	next         uint32
	locals       map[SEDPGUID]SEDPEndpoint
	remotes      map[SEDPGUID]SEDPEndpoint
	remoteTopics map[SEDPTopic]struct{}
	generation   uint64
	onChange     func(uint64)
	onRemove     func(SEDPGUID)
	// lastSeq tracks the last SEDP DATA sequence per endpoint GUID for
	// monotonic ingest gating. Stale or reordered samples are rejected
	// without upserting. Zero never advances this map.
	lastSeq map[SEDPGUID]uint64
	// hbLast tracks the last HEARTBEAT LastSequence per builtin writer.
	hbLast map[sedpWriterKey]uint64
	// hbCount tracks the last HEARTBEAT Count per builtin writer.
	hbCount map[sedpWriterKey]int32
	// writerHigh tracks the highest DATA or GAP sequence per builtin
	// writer for heartbeat clamp bounds. It is deliberately not an ingest
	// gate: several endpoints share one writer stream with interleaved
	// per-writer sequences, so an endpoint's newest sample can carry a
	// lower sequence than a sibling endpoint's newer sample. Gating
	// ingest on writerHigh would falsely drop such newest-per-endpoint
	// samples. Replay gates only on the per-endpoint lastSeq; writerHigh
	// only bounds GAP clamping.
	writerHigh map[sedpWriterKey]uint64
	// pendingEvict holds participant prefixes whose SEDP remotes drop
	// after sedpEvictGracePeriod. SPDP dispose removes routes at once
	// but defers endpoint removal so in-flight user DATA still takes;
	// a re-announce cancels through UpsertRemote.
	pendingEvict map[SEDPGUIDPrefix]time.Time
	// sec holds the Phase 1 decision suite, nil for tolerance mode.
	sec *ddssecurity.Suite
}

// sedpEvictGracePeriod bounds how long SEDP remotes survive their SPDP
// dispose. The window covers in-flight user DATA plus a slow take loop
// so single-shot publishers still deliver. Lease-expiry eviction stays
// immediate: ten seconds of silence already exceed any in-flight window.
// A package var (not const) so tests shrink the window deterministically.
var sedpEvictGracePeriod = 5 * time.Second

// sedpAuxCap bounds auxiliary discovery maps under writer churn.
//
// It mirrors livelyTrackerCap so discovery churn stays bounded without
// trusting unauthenticated GUIDs: at cap one entry is evicted loudly to
// make room. It covers hbLast, hbCount, writerHigh, and pendingEvict.
// lastSeq stays gated on matched endpoints instead of capped, and remotes
// stay the authoritative matched set. Insert paths sweep nothing because
// these maps carry no TTL; eviction is oldest-first where order exists
// and arbitrary otherwise, always with a loud log.
const sedpAuxCap = 1024

// sedpEvictOldestPendingLocked drops the earliest pending prefix to hold the cap.
//
// Caller holds idx.mu and inserts a new prefix not yet present. It deletes
// the entry with the smallest deadline loudly so writer churn cannot grow
// pendingEvict without bound. It is a no-op when under cap.
func (idx *SEDPIndex) sedpEvictOldestPendingLocked() {
	var (
		oldest SEDPGUIDPrefix
		have   bool
		best   time.Time
	)

	for prefix, at := range idx.pendingEvict {
		if !have || at.Before(best) {
			oldest = prefix
			best = at
			have = true
		}
	}

	if !have {
		return
	}

	delete(idx.pendingEvict, oldest)
	log.Printf("dds: sedp pending table full evict prefix=%x", oldest)
}

// sedpCapHeartbeatLocked ensures heartbeat bound tables stay bounded.
//
// Caller holds idx.mu for write. When both tables hold sedpAuxCap entries
// and the key is unseen, it evicts one arbitrary writer loudly from both
// tables so writer churn cannot grow the bounds without limit.
func (idx *SEDPIndex) sedpCapHeartbeatLocked(except sedpWriterKey) {
	_, seenLast := idx.hbLast[except]
	_, seenCount := idx.hbCount[except]
	if seenLast || seenCount {
		return
	}

	if len(idx.hbLast) < sedpAuxCap && len(idx.hbCount) < sedpAuxCap {
		return
	}

	for key := range idx.hbLast {
		if key == except {
			continue
		}

		delete(idx.hbLast, key)
		delete(idx.hbCount, key)
		log.Printf("dds: sedp heartbeat table full evict writer=%x", key.writer)

		return
	}

	for key := range idx.hbCount {
		if key == except {
			continue
		}

		delete(idx.hbLast, key)
		delete(idx.hbCount, key)
		log.Printf("dds: sedp heartbeat table full evict writer=%x", key.writer)

		return
	}
}

// sedpCapWriterHighLocked ensures writerHigh stays bounded.
//
// Caller holds idx.mu for write. When the table holds sedpAuxCap entries
// and the key is unseen, it evicts one arbitrary writer loudly.
func (idx *SEDPIndex) sedpCapWriterHighLocked(except sedpWriterKey) {
	if _, seen := idx.writerHigh[except]; seen {
		return
	}

	if len(idx.writerHigh) < sedpAuxCap {
		return
	}

	for key := range idx.writerHigh {
		if key == except {
			continue
		}

		delete(idx.writerHigh, key)
		log.Printf("dds: sedp writer high table full evict writer=%x", key.writer)

		return
	}
}

// sedpHasPrefixLocked reports whether one prefix has any remote endpoint.
//
// Caller holds idx.mu. It scans remotes for the prefix so the lastSeq gate
// can admit sequences for known participants without trusting random
// prefixes. The scan stays bounded because remotes stays the authoritative
// set and auxiliary maps stay capped.
func (idx *SEDPIndex) sedpHasPrefixLocked(prefix SEDPGUIDPrefix) bool {
	for id := range idx.remotes {
		if id.Prefix == prefix {
			return true
		}
	}

	return false
}

// NewSEDPIndex returns an SEDP index for one participant prefix and domain.
func NewSEDPIndex(prefix SEDPGUIDPrefix, domain uint32) (*SEDPIndex, error) {
	if domain > sedpMaxDomainID {
		return nil, fmt.Errorf("dds: sedp domain %d above maximum %d: %w", domain, sedpMaxDomainID, ErrSEDPInvalidDomain)
	}

	return &SEDPIndex{
		prefix:       prefix,
		domain:       domain,
		next:         sedpUserEntityBase,
		locals:       make(map[SEDPGUID]SEDPEndpoint),
		remotes:      make(map[SEDPGUID]SEDPEndpoint),
		pendingEvict: make(map[SEDPGUIDPrefix]time.Time),
	}, nil
}

// SetOnChange installs the hook fired with each new generation.
func (idx *SEDPIndex) SetOnChange(fn func(uint64)) {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	idx.onChange = fn
}

// SetOnRemove installs the hook fired for each removed remote endpoint.
//
// The backend wires this to evict per-writer reliability state so SEDP
// removal does not leave stale NACK bases or sequence tracking behind.
// It self-locks and is safe for concurrent use.
func (idx *SEDPIndex) SetOnRemove(fn func(SEDPGUID)) {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	idx.onRemove = fn
}

// SetSecuritySuite installs the Phase 1 decision suite.
//
// A nil suite keeps tolerance mode: endpoints install exactly as today.
// A non-nil enabled suite refuses remote installs, provisional installs,
// and local declares before they enter the index. It self-locks and is
// safe for concurrent use.
func (idx *SEDPIndex) SetSecuritySuite(s *ddssecurity.Suite) {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	idx.sec = s
}

// sedpPeerDescriptor builds the Phase 1 decision input for one endpoint.
//
// Identity is the hex participant prefix. Class follows the parsed
// endpoint posture: no markers means unsecured, hard-require properties
// mean crypto, and anything else means compatible-unknown. Properties,
// domain, and partitions pass through untouched; the suite decides.
// The DDS topic and reader direction travel as the check arguments, and
// the DDS type rides in the endpoint record for match-time consistency.
func sedpPeerDescriptor(ep SEDPEndpoint) ddssecurity.PeerDescriptor {
	var class ddssecurity.PeerClass

	switch ep.Security.Classification {
	case SecurityUnsecured:
		class = ddssecurity.PeerUnsecured
	case SecurityRequiresCrypto:
		class = ddssecurity.PeerRequiresCrypto
	case SecurityCompatibleUnknown:
		class = ddssecurity.PeerCompatible
	default:
		class = ddssecurity.PeerUnsecured
	}

	return ddssecurity.PeerDescriptor{
		Identity:   hex.EncodeToString(ep.ID.Prefix[:]),
		Class:      class,
		Properties: ep.Security.Properties,
		DomainID:   ep.DomainID,
		Partitions: append([]string(nil), ep.Partitions...),
	}
}

// sedpCheckRemoteSecurityLocked enforces the Phase 1 decision suite on one
// remote endpoint. Caller holds idx.mu.
//
// A nil or disabled suite keeps tolerance mode: it returns nil without
// consulting the suite, preserving byte-identical behavior. An enabled
// suite refuses endpoints that fail CheckRemoteEndpoint fail-closed with
// the DDS topic, reader direction, domain, partitions, and security
// posture as inputs. The DDS type rides in the endpoint record and is
// enforced at match time, not at install.
func (idx *SEDPIndex) sedpCheckRemoteSecurityLocked(ep SEDPEndpoint) error {
	suite := idx.sec
	if suite == nil || !suite.Enabled() {
		return nil
	}

	err := suite.CheckRemoteEndpoint(sedpPeerDescriptor(ep), ep.DDSTopic, ep.IsReader)
	if err != nil {
		return fmt.Errorf("dds: sedp refuse remote endpoint: %w", err)
	}

	return nil
}

// sedpCheckLocalSecurityLocked enforces the Phase 1 decision suite on one
// local declare. Caller holds idx.mu.
//
// A nil or disabled suite keeps tolerance mode: it returns nil without
// consulting the suite, preserving byte-identical behavior. An enabled
// suite refuses declares that fail the local create gate fail-closed
// with the participant identity, DDS topic, reader direction, domain,
// and partitions as inputs. This is the creation-time gate
// (CheckCreateEndpointInDomainacc8Scoped); the local match gates apply at
// match time with a live remote and stay out of the declare path. The
// DDS type rides in the endpoint record and is enforced at match time.
func (idx *SEDPIndex) sedpCheckLocalSecurityLocked(ep SEDPEndpoint) error {
	suite := idx.sec
	if suite == nil || !suite.Enabled() {
		return nil
	}

	identity := hex.EncodeToString(idx.prefix[:])

	err := suite.CheckCreateEndpointInDomainacc8Scoped(identity, ep.DDSTopic, ep.IsReader, ep.DomainID, ep.Partitions, nil)
	if err != nil {
		return fmt.Errorf("dds: sedp refuse local declare: %w", err)
	}

	return nil
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

// SetSEDPDomainTag pins the local DDS domain tag for SEDP gating per 8.5.5.1.
//
// Empty selects the default domain tag. UpsertRemote and
// ProvisionalInstall refuse endpoints whose DomainTag differs with an
// exact match (empty equals only empty, no wildcards). The wire carries
// no tag today so decoded endpoints stamp empty; API callers with an
// explicit foreign tag are refused fail-closed. It is safe for
// concurrent use.
func (idx *SEDPIndex) SetSEDPDomainTag(tag string) {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	idx.domainTag = tag
}

// sedpCheckDomainTagLocked rejects foreign domain tags. Caller holds idx.mu.
//
// Empty tag means the default domain tag per 8.5.5.1 and matches only
// empty: there are no wildcards, so a tagged local index refuses untagged
// wire endpoints and vice versa.
func (idx *SEDPIndex) sedpCheckDomainTagLocked(ep SEDPEndpoint) error {
	if ep.DomainTag != idx.domainTag {
		return fmt.Errorf("dds: sedp domain tag %q mismatches local %q: %w", ep.DomainTag, idx.domainTag, ErrSEDPInvalidDomain)
	}

	return nil
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
//
// Fail-closed: any decode failure or any malformed partition value rejects
// loudly instead of degrading to the default partition, so a truncated
// param list or a bad partition name never silently meets the wrong peers.
// A nil or empty QoS carries no partitions and maps to the default.
// Opaque non-params QoS stays default only when it decodes to no
// partition PID; present-but-malformed partition params always error.
func sedpDeclarePartitions(qos []byte) ([]string, error) {
	if len(qos) == 0 {
		return nil, nil
	}

	params, err := rtps.DecodeParams(qos)
	if err != nil {
		return nil, fmt.Errorf("dds: sedp declare partitions decode: %w", err)
	}

	return sedpValidatedPartitionNames(params)
}

// sedpValidatedPartitionNames validates partition names from decoded params.
//
// Fail-closed: a present-but-malformed PIDPartition value rejects loudly
// with the partition cause preserved, so declares never fall back to the
// default partition on bad input. Absent partition params map to nil for
// the default partition.
func sedpValidatedPartitionNames(params []rtps.Param) ([]string, error) {
	names, err := FindPartitionParam(params)
	if err != nil {
		return nil, fmt.Errorf("dds: sedp declare partitions: %w", err)
	}

	if len(names) == 0 {
		return nil, nil
	}

	err = ValidatePartitionList(names)
	if err != nil {
		return nil, fmt.Errorf("dds: sedp declare partitions: %w", err)
	}

	return append([]string(nil), names...), nil
}

// sedpBuildDeclareEndpoint assembles the endpoint record before storing.
func sedpBuildDeclareEndpoint(id SEDPGUID, inputs sedpDeclareInputs, typeHash string, qos []byte, isReader bool, domain uint32, readerVolatile bool, wireOffer *WireQoS) SEDPEndpoint {
	rxo := sedp3FacadeWireQoSsedp3(isReader && readerVolatile)
	if wireOffer != nil {
		rxo = *wireOffer
	}

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
		RxOWire:            &rxo,
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

	id := SEDPGUID{Prefix: idx.prefix, Entity: entity}

	_, exists := idx.locals[id]
	if exists {
		return SEDPEndpoint{}, fmt.Errorf("dds: sedp declare duplicate entity: %w", ErrSEDPInvalidEndpoint)
	}

	idx.next++

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

	// Fail-closed local declare gate: an enabled suite refuses before the
	// record enters the index. A refused declare consumes the entity
	// counter but installs nothing and bumps no generation. A nil or
	// disabled suite skips the check with byte-identical behavior.
	if err := idx.sedpCheckLocalSecurityLocked(ep); err != nil {
		return SEDPEndpoint{}, err
	}

	idx.locals[id] = sedpCloneEndpoint(ep)

	return sedpCloneEndpoint(ep), nil
}

// StampLocalPartitions records partitions on matching local readers.
//
// It validates the names loudly, updates every local reader for the ROS
// topic, and local stamps bump generation once when any record actually
// changes so the next announce carries the set. Unchanged partitions bump
// nothing and fire no hook, matching the once-per-change contract.
// Unknown topics change nothing and report nil. Writers stay exempt by
// design: the publish path carries no partition API, and stamping writers
// on a subscribe would leak subscription state into publication, so
// writers join partitions only through declare QoS, never through this
// reader-only backfill.
func (idx *SEDPIndex) StampLocalPartitions(rosTopic, typeName string, partitions []string) {
	err := ValidatePartitionList(partitions)
	if err != nil {
		return
	}

	idx.mu.Lock()
	changed := false
	for id, ep := range idx.locals {
		if !ep.IsReader {
			continue
		}

		if ep.ROSTopic != rosTopic || ep.TypeIDL != typeName {
			continue
		}

		if slices.Equal(ep.Partitions, partitions) {
			continue
		}

		ep.Partitions = append([]string(nil), partitions...)
		idx.locals[id] = ep
		changed = true
	}
	var gen uint64
	var hook func(uint64)
	if changed {
		idx.generation++
		gen = idx.generation
		hook = idx.onChange
	}
	idx.mu.Unlock()

	if changed && hook != nil {
		hook(gen)
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

// RemoteTopics returns a sorted copy of remotely announced topics.
//
// TopicBuiltinTopicData is advisory: it records which topics exist
// without endpoint state, and matching still uses endpoints. Topic
// installs never bump the endpoint generation and never fire onChange,
// so a Topics-only datagram installs topics while leaving generation at
// zero. It is safe for concurrent use.
func (idx *SEDPIndex) RemoteTopics() []SEDPTopic {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	out := make([]SEDPTopic, 0, len(idx.remoteTopics))
	for topic := range idx.remoteTopics {
		out = append(out, topic)
	}
	slices.SortFunc(out, func(a, b SEDPTopic) int {
		if a.DDSTopic != b.DDSTopic {
			return strings.Compare(a.DDSTopic, b.DDSTopic)
		}
		if a.TypeDDS != b.TypeDDS {
			return strings.Compare(a.TypeDDS, b.TypeDDS)
		}
		return strings.Compare(a.TypeIDL, b.TypeIDL)
	})
	return out
}

// installRemoteTopic records one remotely announced topic.
func (idx *SEDPIndex) installRemoteTopic(topic SEDPTopic) {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	if idx.remoteTopics == nil {
		idx.remoteTopics = make(map[SEDPTopic]struct{})
	}
	idx.remoteTopics[topic] = struct{}{}
}

// removeRemoteTopic drops one remotely announced topic when present.
func (idx *SEDPIndex) removeRemoteTopic(topic SEDPTopic) {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	delete(idx.remoteTopics, topic)
}

// UpsertRemote adds or replaces a remote endpoint by GUID.
//
// The input is always stored as confirmed: Provisional is cleared and
// FirstSeen is reset because SEDP QoS, type, and hash are authoritative.
// Promotion of a provisional placeholder therefore bumps generation.
// The domain tag must exactly equal the index tag (empty equals only
// empty, no wildcards) per 8.5.5.1 or the upsert refuses fail-closed.
// Caller must validate the endpoint prefix against the RTPS header prefix
// before calling: UpsertRemote installs any prefix it is given and never
// checks header binding itself, so ingest paths enforce the binding and
// direct callers must do the same. Prefer UpsertRemoteChecked, which
// enforces the binding, for all new callers.
func (idx *SEDPIndex) UpsertRemote(ep SEDPEndpoint) error {
	return idx.upsertRemote(ep)
}

// UpsertRemoteChecked adds or replaces a remote endpoint by GUID after
// enforcing the RTPS header binding fail-closed.
//
// Prefix must equal the endpoint GUID prefix: a mismatch drops loudly
// without installing, recording sequence, or bumping generation, so
// spoofed endpoints never enter the index. All other semantics match
// UpsertRemote. Ingest paths must call this instead of UpsertRemote.
func (idx *SEDPIndex) UpsertRemoteChecked(prefix SEDPGUIDPrefix, ep SEDPEndpoint) error {
	if ep.ID.Prefix != prefix {
		sedpTracef("upsert prefix mismatch guid=%s header=%x", ep.ID.String(), prefix)
		log.Printf("dds: sedp guid prefix mismatches header, dropping")
		return fmt.Errorf("dds: sedp guid prefix mismatches header: %w", ErrSEDPInvalidDATA)
	}

	return idx.upsertRemote(ep)
}

// upsertRemote implements UpsertRemote plus UpsertRemoteChecked.
func (idx *SEDPIndex) upsertRemote(ep SEDPEndpoint) error {
	err := sedpValidateRemote(ep)
	if err != nil {
		sedpTracef("upsert reject guid=%s ros=%q dds=%q type=%q domain=%d err=%v", ep.ID.String(), ep.ROSTopic, ep.DDSTopic, ep.TypeDDS, ep.DomainID, err)
		return fmt.Errorf("dds: sedp upsert remote: %w", err)
	}

	ep.Provisional = false
	ep.FirstSeen = time.Time{}

	idx.mu.Lock()

	// Fail-closed remote gate: an enabled suite refuses before any
	// mutation, so a refused re-announce neither cancels a scheduled
	// dispose eviction nor installs nor bumps generation. A nil or
	// disabled suite skips the check with byte-identical behavior.
	if err := idx.sedpCheckRemoteSecurityLocked(ep); err != nil {
		idx.mu.Unlock()
		sedpTracef("upsert security refuse guid=%s ros=%q err=%v", ep.ID.String(), ep.ROSTopic, err)

		return fmt.Errorf("dds: sedp upsert remote: %w", err)
	}

	// Fail-closed domain tag gate per 8.5.5.1: exact match, no wildcards.
	// A refused re-announce neither cancels a scheduled dispose eviction
	// nor installs nor bumps generation, matching the security gate.
	if err := idx.sedpCheckDomainTagLocked(ep); err != nil {
		idx.mu.Unlock()
		sedpTracef("upsert tag refuse guid=%s ros=%q err=%v", ep.ID.String(), ep.ROSTopic, err)

		return fmt.Errorf("dds: sedp upsert remote: %w", err)
	}

	// A re-announce proves liveness even when identical, so it cancels a
	// scheduled dispose eviction before the duplicate short-circuit below.
	delete(idx.pendingEvict, ep.ID.Prefix)

	existing, seen := idx.remotes[ep.ID]
	if seen && sedpEndpointsEqual(existing, ep) {
		idx.mu.Unlock()
		sedpTracef("upsert skipped duplicate guid=%s ros=%q domain=%d", ep.ID.String(), ep.ROSTopic, ep.DomainID)

		return nil
	}

	if idx.remotes == nil {
		idx.remotes = make(map[SEDPGUID]SEDPEndpoint)
	}
	if !seen && len(idx.remotes) >= sedpAuxCap {
		idx.sedpEvictOldestRemoteLocked()
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

// sedpEvictOldestRemoteLocked drops one remote entry to hold the cap.
//
// Caller holds idx.mu and inserts a GUID not yet present. It prefers the
// provisional placeholder with the earliest FirstSeen (placeholders carry
// age while confirmed entries carry none), else drops one arbitrary
// confirmed entry, always with a loud log so writer churn cannot grow
// remotes without bound. It is a no-op when under cap.
func (idx *SEDPIndex) sedpEvictOldestRemoteLocked() {
	var (
		oldest SEDPGUID
		have   bool
		best   time.Time
	)

	for id, ep := range idx.remotes {
		if !ep.Provisional {
			continue
		}
		if !have || ep.FirstSeen.Before(best) {
			oldest = id
			best = ep.FirstSeen
			have = true
		}
	}

	if !have {
		for id := range idx.remotes {
			oldest = id
			have = true

			break
		}
	}

	if !have {
		return
	}

	delete(idx.remotes, oldest)
	delete(idx.lastSeq, oldest)
	log.Printf("dds: sedp remotes table full evict guid=%s", oldest.String())
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
	delete(idx.lastSeq, id)
	idx.generation++

	gen := idx.generation
	hook := idx.onChange
	removeHook := idx.onRemove

	idx.mu.Unlock()

	if hook != nil {
		hook(gen)
	}

	if removeHook != nil {
		removeHook(id)
	}

	return true
}

// SchedulePrefixEvict defers SEDP remote removal for one participant prefix.
//
// SPDP dispose removes routes at once but in-flight user DATA may still
// take, so endpoint removal waits out sedpEvictGracePeriod. The first
// dispose starts the clock: duplicate disposes never re-arm the deadline,
// so the grace window stays bounded from first sight. Expiry processing
// deletes the entry. Any re-announce cancels the eviction: a duplicate at
// the stored sequence cancels in ingest, and a newer sequence cancels
// through UpsertRemote on install. It never removes anything itself.
func (idx *SEDPIndex) SchedulePrefixEvict(prefix SEDPGUIDPrefix, now time.Time) {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	if idx.pendingEvict == nil {
		idx.pendingEvict = make(map[SEDPGUIDPrefix]time.Time)
	}

	if _, seen := idx.pendingEvict[prefix]; seen {
		return
	}

	if len(idx.pendingEvict) >= sedpAuxCap {
		idx.sedpEvictOldestPendingLocked()
	}

	idx.pendingEvict[prefix] = now.Add(sedpEvictGracePeriod)
}

// ExpirePendingEvictions removes SEDP remotes whose dispose grace elapsed.
//
// It returns the count removed. Prefixes re-announced since scheduling
// were cancelled by UpsertRemote and survive. It holds the index lock
// across collect and remove so an UpsertRemote between pending delete and
// endpoint removal cannot lose a re-announced endpoint: the upsert either
// cancels before the hold and survives, or blocks until after and
// reinstalls. Generation bumps once per call like Sweep and
// ExpireProvisional, not per endpoint; hidden state such as lastSeq
// deletions never bumps alone. The change hook fires once with the new
// generation and the remove hook fires per removed GUID. It self-locks
// and is safe for concurrent use.
func (idx *SEDPIndex) ExpirePendingEvictions(now time.Time) int {
	idx.mu.Lock()

	var due []SEDPGUIDPrefix
	for prefix, at := range idx.pendingEvict {
		if !now.Before(at) {
			due = append(due, prefix)
		}
	}

	if len(due) == 0 {
		idx.mu.Unlock()

		return 0
	}

	dueSet := make(map[SEDPGUIDPrefix]struct{}, len(due))
	for _, prefix := range due {
		dueSet[prefix] = struct{}{}
		delete(idx.pendingEvict, prefix)
	}

	var removedIDs []SEDPGUID
	for id := range idx.remotes {
		if _, ok := dueSet[id.Prefix]; ok {
			delete(idx.remotes, id)
			delete(idx.lastSeq, id)
			removedIDs = append(removedIDs, id)
		}
	}

	if len(removedIDs) == 0 {
		idx.mu.Unlock()

		return 0
	}

	slices.SortFunc(removedIDs, func(a, b SEDPGUID) int {
		if a.Prefix != b.Prefix {
			return bytes.Compare(a.Prefix[:], b.Prefix[:])
		}

		return bytes.Compare(a.Entity[:], b.Entity[:])
	})

	idx.generation++

	gen := idx.generation
	hook := idx.onChange
	removeHook := idx.onRemove

	idx.mu.Unlock()

	if hook != nil {
		hook(gen)
	}

	if removeHook != nil {
		for _, id := range removedIDs {
			removeHook(id)
		}
	}

	return len(removedIDs)
}

// ProvisionalInstall installs a datapath fast-path placeholder by GUID.
//
// Seq is the DATA sequence that observed the endpoint and gates the
// install like the confirmed path: zero sequences reject loudly without
// installing (mirroring SPDP and the ingest seq-zero reject), and
// sequences at or below the per-endpoint high reject as stale with
// ErrSEDPStale so an old retransmit cannot clobber newer state. The
// record is stored with Provisional true and FirstSeen set to now
// when the caller leaves it zero. A confirmed record is never
// overwritten: install over a non-provisional entry is a no-op without
// bumping generation. Install over a provisional entry preserves the
// original FirstSeen so the TTL stays bounded from first sight, and is
// a no-op when all other fields already match. Otherwise it stores the
// placeholder, bumps generation, and fires the change hook. The domain
// tag must exactly equal the index tag per 8.5.5.1. Inserts at the
// remotes cap evict one entry loudly like the confirmed path.
func (idx *SEDPIndex) ProvisionalInstall(ep SEDPEndpoint, seq uint64) error {
	err := checkProvisionalArgs(ep, seq)
	if err != nil {
		return err
	}

	ep.Provisional = true
	if ep.FirstSeen.IsZero() {
		ep.FirstSeen = time.Now()
	}

	idx.mu.Lock()
	hook, gen, stored, err := idx.provisionalInstallLocked(ep, seq)
	idx.mu.Unlock()
	if err != nil {
		return err
	}
	if stored && hook != nil {
		hook(gen)
	}

	return nil
}

// checkProvisionalArgs validates the endpoint plus DATA sequence before locking.
func checkProvisionalArgs(ep SEDPEndpoint, seq uint64) error {
	err := sedpValidateRemote(ep)
	if err != nil {
		return fmt.Errorf("dds: sedp provisional install: %w", err)
	}

	if seq == 0 {
		return fmt.Errorf("dds: sedp provisional sequence must be non-zero: %w", ErrSEDPInvalidDATA)
	}

	return nil
}

// provisionalInstallLocked gates and stores one placeholder. Caller holds
// idx.mu. It reports whether the placeholder stored plus the hook and
// generation to fire outside the lock when it did.
func (idx *SEDPIndex) provisionalInstallLocked(ep SEDPEndpoint, seq uint64) (func(uint64), uint64, bool, error) {
	// Fail-closed fast-path gate: an enabled suite refuses the placeholder
	// before it enters the index, so nothing installs and nothing bumps
	// generation. A nil or disabled suite skips the check with
	// byte-identical behavior.
	if err := idx.sedpCheckRemoteSecurityLocked(ep); err != nil {
		return nil, 0, false, fmt.Errorf("dds: sedp provisional install: %w", err)
	}

	// Fail-closed domain tag gate per 8.5.5.1: exact match, no wildcards.
	if err := idx.sedpCheckDomainTagLocked(ep); err != nil {
		return nil, 0, false, fmt.Errorf("dds: sedp provisional install: %w", err)
	}

	// DATA sequence gate mirroring the confirmed path: stale or reordered
	// samples never install and never advance the high.
	if last, seen := idx.lastSeq[ep.ID]; seen && seq <= last {
		return nil, 0, false, fmt.Errorf("dds: sedp provisional stale seq=%d last=%d: %w", seq, last, ErrSEDPStale)
	}

	existing, seen := idx.remotes[ep.ID]
	if seen && !existing.Provisional {
		return nil, 0, false, nil
	}

	if seen {
		unchanged, merged := provisionalMerge(existing, ep)
		if unchanged {
			return nil, 0, false, nil
		}
		ep = merged
	}

	provisionalStoreLocked(idx, ep, seen, seq)

	return idx.onChange, idx.generation, true, nil
}

// provisionalMerge preserves the original FirstSeen and reports whether
// the stored placeholder already matches.
func provisionalMerge(existing, candidate SEDPEndpoint) (bool, SEDPEndpoint) {
	merged := sedpCloneEndpoint(candidate)
	merged.FirstSeen = existing.FirstSeen
	if sedpEndpointsEqual(existing, merged) {
		return true, merged
	}
	candidate.FirstSeen = existing.FirstSeen

	return false, candidate
}

// provisionalStoreLocked stores one gated placeholder and bumps generation.
// Caller holds idx.mu; seen reports whether the GUID was already present.
func provisionalStoreLocked(idx *SEDPIndex, ep SEDPEndpoint, seen bool, seq uint64) {
	if idx.remotes == nil {
		idx.remotes = make(map[SEDPGUID]SEDPEndpoint)
	}
	if !seen && len(idx.remotes) >= sedpAuxCap {
		idx.sedpEvictOldestRemoteLocked()
	}

	// A provisional install proves liveness like a confirmed re-announce.
	delete(idx.pendingEvict, ep.ID.Prefix)

	idx.remotes[ep.ID] = sedpCloneEndpoint(ep)
	if idx.lastSeq == nil {
		idx.lastSeq = make(map[SEDPGUID]uint64)
	}
	if last, tracked := idx.lastSeq[ep.ID]; !tracked || seq > last {
		idx.lastSeq[ep.ID] = seq
	}
	idx.generation++
}

// ExpireProvisional removes provisional entries older than sedpProvisionalTTL.
//
// An entry expires when now is at or past FirstSeen plus the TTL. A zero
// FirstSeen is treated as expired so a malformed placeholder cannot
// linger. It bumps generation once when any entry expires, fires the
// change hook once, fires the remove hook per removed GUID for backend
// eviction, and returns the number removed.
func (idx *SEDPIndex) ExpireProvisional(now time.Time) int {
	idx.mu.Lock()

	removed := 0

	var removedIDs []SEDPGUID

	for id, ep := range idx.remotes {
		if !ep.Provisional {
			continue
		}

		if ep.FirstSeen.IsZero() {
			delete(idx.remotes, id)
			delete(idx.lastSeq, id)
			removed++
			removedIDs = append(removedIDs, id)

			continue
		}

		if now.Before(ep.FirstSeen.Add(sedpProvisionalTTL)) {
			continue
		}

		delete(idx.remotes, id)
		delete(idx.lastSeq, id)
		removed++
		removedIDs = append(removedIDs, id)
	}

	if removed == 0 {
		idx.mu.Unlock()

		return 0
	}

	idx.generation++

	gen := idx.generation
	hook := idx.onChange
	removeHook := idx.onRemove

	idx.mu.Unlock()

	if hook != nil {
		hook(gen)
	}

	if removeHook != nil {
		for _, id := range removedIDs {
			removeHook(id)
		}
	}

	return removed
}

// Sweep removes expired provisional entries and bumps generation once when any expire.
//
// It is an alias for ExpireProvisional kept for symmetry with the SPDP
// remote table sweep. Confirmed entries never expire here. Sweep is
// table-only and self-contained: it takes only the index lock, touches
// only index state, and is safe to call standalone or under the backend
// joint lock without ordering concerns. Joint paths that also touch
// backend store or route state live in backend-owned code and own their
// lock order; this helper never takes the backend lock itself.
func (idx *SEDPIndex) Sweep(now time.Time) int {
	return idx.ExpireProvisional(now)
}

// sedpAnnounceEndpointsAndTopics sends one endpoint snapshot plus one
// topics snapshot, mapping loop errors for the announce loop.
func sedpAnnounceEndpointsAndTopics(ctx context.Context, s SEDPSender, eps []SEDPEndpoint, prefix SEDPGUIDPrefix, topics []SEDPTopic) error {
	err := sedpAnnounceSnapshot(ctx, s, eps)
	if err != nil {
		return sedpMapLoopError(ctx, fmt.Errorf("dds: sedp announce snapshot: %w", err))
	}

	err = sedpAnnounceTopicsSnapshot(ctx, s, prefix, topics)
	if err != nil {
		return sedpMapLoopError(ctx, fmt.Errorf("dds: sedp announce topics snapshot: %w", err))
	}

	return nil
}

// AnnounceLoop publishes all local endpoints immediately then re-announces
// on local changes until ctx is done.
//
// It snapshots LocalEndpoints plus TopicsSnapshot once, sends endpoint
// DATA samples with SerializeSEDPDATA and topic DATA samples with
// SerializeSEDPTopicsDATA through s, then polls LocalEndpoints every
// 200ms and re-sends all when either snapshot differs. It also resends
// every second with fresh DATA sequences and HEARTBEATs so reliable
// peers that dropped the first sample still match. Generation is not
// polled because it counts remote mutations only and local declares
// leave it unchanged, so the loop compares local snapshots instead.
// Sends run outside the index lock because LocalEndpoints,
// TopicsSnapshot, and SerializeSEDPDATA copy their inputs.
//
// A snapshot with zero endpoints and zero topics sends nothing.
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
	lastTopics := idx.TopicsSnapshot()
	prefix := idx.snapshotPrefix()

	err = sedpAnnounceEndpointsAndTopics(ctx, s, last, prefix, lastTopics)
	if err != nil {
		return err
	}

	lastSend := time.Now()

	ticker := time.NewTicker(sedpAnnouncePollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err() //nolint:wrapcheck // cancel semantics require unwrapped ctx.Err
		case <-ticker.C:
			current, currentTopics, resend := idx.sedpAnnounceTickSnapshot(last, lastTopics, lastSend)
			if !resend {
				continue
			}

			err := sedpAnnounceEndpointsAndTopics(ctx, s, current, prefix, currentTopics)
			if err != nil {
				return err
			}

			last = current
			lastTopics = currentTopics
			lastSend = time.Now()
		}
	}
}

// sedpAnnounceTickSnapshot snapshots locals plus topics and reports
// whether the tick must resend: either snapshot differs or the resend
// interval elapsed since lastSend.
func (idx *SEDPIndex) sedpAnnounceTickSnapshot(last []SEDPEndpoint, lastTopics []SEDPTopic, lastSend time.Time) ([]SEDPEndpoint, []SEDPTopic, bool) {
	current := idx.LocalEndpoints()
	currentTopics := idx.TopicsSnapshot()

	if sedpEndpointsSlicesEqual(last, current) && sedpTopicsSlicesEqual(lastTopics, currentTopics) && time.Since(lastSend) < sedpAnnounceResendInterval {
		return last, lastTopics, false
	}

	return current, currentTopics, true
}

// ListenLoop receives DATA samples until ctx is done.
//
// Each non-empty sample feeds ingestSample then UpsertRemote, so every
// valid announce bumps the remote generation through the existing upsert
// path and fires the change hook. Empty samples are treated as no-data
// polls from non-blocking receivers and are skipped. Availability stays:
// a single poison datagram never ends the loop; it counts loudly per
// failure class (receive, parse, upsert) via log plus trace and the loop
// keeps listening. Only transport errors or context cancel end the loop.
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
			// A single poison datagram must not kill discovery: count
			// loudly per failure class and keep listening for the next
			// sample, matching the backend receiver which already
			// continues on unparseable or off-domain DATA. Only a
			// transport error or context cancel ends the loop.
			if ctxErr := ctx.Err(); ctxErr != nil {
				return ctxErr //nolint:wrapcheck // cancel semantics require unwrapped ctx.Err
			}
			sedpTracef("ingest skip poison len=%d err=%v", len(data), err)
			log.Printf("dds: sedp listen ingest dropped len=%d: %v", len(data), err)
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
// not drop the others. Endpoint upserts gate on SEDP DATA sequence per
// endpoint GUID: stale or reordered samples are rejected without upsert
// so an old retransmit cannot clobber newer state. It returns nil when at
// least one entry installs or when all entries are stale replays,
// otherwise the first parse or upsert error.
// sedpHeaderPrefix returns the RTPS header participant prefix.
func sedpHeaderPrefix(data []byte) (SEDPGUIDPrefix, error) {
	var prefix SEDPGUIDPrefix
	err := sedpValidateRTPSHeader(data)
	if err != nil {
		return prefix, err
	}
	copy(prefix[:], data[8:20])
	return prefix, nil
}

// sedpValidateHeaderExtension enforces RTPS_HE placement per 8.3.7.3.
//
// It splits every submessage in the datagram and validates the header
// extension layout through rtps.ValidateHeaderExtensionPlacement: a
// malformed extension body, a checksum mismatch, or an extension in any
// position but first drops fail-closed. Standalone walks must call this
// before dispatch because sedpSplitSubmessage alone skips unknown ids by
// length and would otherwise ignore a mispositioned extension.
func sedpValidateHeaderExtension(data []byte) error {
	err := sedpValidateRTPSHeader(data)
	if err != nil {
		return err
	}
	off := sedpRTPSHeaderLen
	seen := 0
	var raws []rtps.RawSubmessage
	for off < len(data) {
		seen++
		if seen > sedpMaxSubmessages {
			return fmt.Errorf("dds: sedp parse submessages above %d: %w", sedpMaxSubmessages, ErrSEDPInvalidDATA)
		}
		id, body, flags, next, serr := sedpSplitSubmessage(data, off)
		if serr != nil {
			return serr
		}
		raws = append(raws, rtps.RawSubmessage{ID: id, Flags: flags, Body: body})
		off = next
	}
	var drops rtps.Counter
	if err := rtps.ValidateHeaderExtensionPlacement(data, raws, &drops); err != nil {
		return fmt.Errorf("dds: sedp header extension: %w", errors.Join(err, ErrSEDPInvalidDATA))
	}
	return nil
}

// sedpCheckInfoDstUnicast enforces INFO_DST on unicast ingress.
//
// It shares the walk with the SPDP helper and the backend demux through
// rtps.ClassifyUnicastInfoDst so all three agree: absent, zero-length, or
// all-zero INFO_DST processes normally, mismatch drops loudly with
// ErrSEDPInfoDst, and short non-empty or undecodable INFO_DST bodies drop
// loudly fail-closed with ErrSEDPInfoDst. Multicast ingress must never
// call this helper: group sockets carry every sender, so per-sender
// identity has no meaning there and enforcement would drop valid peers.
func sedpCheckInfoDstUnicast(data []byte, local SEDPGUIDPrefix) error {
	var want [12]byte
	copy(want[:], local[:])

	switch rtps.ClassifyUnicastInfoDst(data, want) {
	case rtps.InfoDstAbsent:
		return nil
	case rtps.InfoDstMismatch:
		log.Printf("dds: sedp unicast INFO_DST mismatch, dropping")

		return fmt.Errorf("dds: sedp INFO_DST mismatch: %w", ErrSEDPInfoDst)
	case rtps.InfoDstMalformed:
		log.Printf("dds: sedp unicast INFO_DST malformed, dropping")

		return fmt.Errorf("dds: sedp INFO_DST malformed body: %w", ErrSEDPInfoDst)
	default:
		return nil
	}
}

// sedpHeartbeatNote records one builtin writer heartbeat bound.
//
// It reports false for replays: a nonzero Count at or below the last Count
// with no new LastSequence draws no further work. Last and Count only move
// forward. Count==0 heartbeats carry no duplicate suppression, so the first
// Count==0 per LastSequence updates the bound and repeats with the same
// LastSequence are replays: single processing per LastSequence without a
// NACK storm. The standalone index sends no ACKs; suppression here only
// bounds GAP clamping and reprocessing.
// sedpHeartbeatReplay reports whether a nonzero count heartbeat is a replay.
func sedpHeartbeatReplay(count int32, last uint64, lastCount int32, lastLast uint64) bool {
	return count != 0 && lastCount != 0 && count <= lastCount && last <= lastLast
}

// sedpNoteZeroHeartbeat records a Count==0 bound, reporting false on repeats.
func sedpNoteZeroHeartbeat(high map[sedpWriterKey]uint64, key sedpWriterKey, last uint64) bool {
	if last != 0 && last == high[key] {
		return false
	}
	if last > high[key] {
		high[key] = last
	}
	return true
}

func (idx *SEDPIndex) sedpHeartbeatNote(key sedpWriterKey, count int32, last uint64) bool {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	if idx.hbLast == nil {
		idx.hbLast = make(map[sedpWriterKey]uint64)
	}
	if idx.hbCount == nil {
		idx.hbCount = make(map[sedpWriterKey]int32)
	}
	lastCount := idx.hbCount[key]
	lastLast := idx.hbLast[key]
	if sedpHeartbeatReplay(count, last, lastCount, lastLast) {
		return false
	}
	idx.sedpCapHeartbeatLocked(key)
	if count == 0 {
		return sedpNoteZeroHeartbeat(idx.hbLast, key, last)
	}
	if last > lastLast {
		idx.hbLast[key] = last
	}
	if count > lastCount {
		idx.hbCount[key] = count
	}
	return true
}

// sedpApplyGapList advances writerHigh over the full skipped list.
//
// Sequences apply in increasing order. When a heartbeat bound is known,
// each step clamps to last plus one so a crafted GAP cannot jump past
// announced history. When no bound is known, each step caps at the current
// high plus sedpGapNoBoundCap (64, one repair window). Zeros are ignored.
// New writer keys hold sedpAuxCap with oldest-eviction plus loud log.
// It self-locks and is safe for concurrent use.
func (idx *SEDPIndex) sedpApplyGapList(key sedpWriterKey, seqs []uint64) {
	filtered := make([]uint64, 0, len(seqs))
	for _, seq := range seqs {
		if seq == 0 {
			continue
		}
		filtered = append(filtered, seq)
	}
	if len(filtered) == 0 {
		return
	}
	slices.Sort(filtered)
	idx.mu.Lock()
	defer idx.mu.Unlock()
	if idx.writerHigh == nil {
		idx.writerHigh = make(map[sedpWriterKey]uint64)
	}
	if idx.hbLast == nil {
		idx.hbLast = make(map[sedpWriterKey]uint64)
	}
	idx.sedpCapWriterHighLocked(key)
	bound, hasBound := idx.hbLast[key]
	for _, seq := range filtered {
		seq = sedpClampGapSeq(seq, bound, hasBound, idx.writerHigh[key])
		if seq > idx.writerHigh[key] {
			idx.writerHigh[key] = seq
		}
	}
}

// sedpClampGapSeq bounds one GAP sequence.
//
// A known heartbeat bound clamps to last plus one so a crafted GAP cannot
// jump past announced history. Without a bound the advance caps at the
// current high plus sedpGapNoBoundCap, one repair window.
func sedpClampGapSeq(seq, bound uint64, hasBound bool, current uint64) uint64 {
	if hasBound && bound != 0 {
		return sedpClampGapBounded(seq, bound)
	}
	return sedpClampGapUnbounded(seq, current)
}

// sedpClampGapBounded clamps one GAP sequence to a heartbeat bound plus one.
func sedpClampGapBounded(seq, bound uint64) uint64 {
	maxAllowed := bound + 1
	if maxAllowed == 0 {
		maxAllowed = ^uint64(0)
	}
	if seq > maxAllowed {
		return maxAllowed
	}
	return seq
}

// sedpClampGapUnbounded caps one GAP sequence without a heartbeat bound.
func sedpClampGapUnbounded(seq, current uint64) uint64 {
	limit := current + sedpGapNoBoundCap
	if limit < current {
		limit = ^uint64(0)
	}
	if seq > limit {
		return limit
	}
	return seq
}

// sedpHeartbeatWriterOf returns the builtin SEDP writer for a HEARTBEAT body.
func sedpHeartbeatWriterOf(body []byte) (SEDPEntityID, bool) {
	var writer SEDPEntityID
	if len(body) < sedpEntityLen*2 {
		return writer, false
	}
	copy(writer[:], body[sedpHeartbeatWriterOff:sedpHeartbeatWriterOff+sedpEntityLen])
	isEndpoint, _ := sedpClassifyWriter(writer)
	return writer, isEndpoint
}

// scanSedpControl walks one datagram for want submessages.
// It validates each sub via knownInvalidSubmessage; a known invalid drops
// the rest per 8.3.4.1 rule 6. RTPS_HE placement validates first per
// 8.3.7.3: a malformed or mispositioned extension drops the whole walk
// before any handle runs. For each matching want sub it calls handle;
// when handle reports true the scan stops. Split failures stop the scan.
func scanSedpControl(data []byte, want byte, handle func(body []byte, flags byte) bool) {
	err := sedpValidateRTPSHeader(data)
	if err != nil {
		return
	}
	if err := sedpValidateHeaderExtension(data); err != nil {
		log.Printf("dds: sedp scan header extension invalid, dropping: %v", err)
		return
	}
	off := sedpRTPSHeaderLen
	seen := 0
	for off < len(data) {
		seen++
		if seen > sedpMaxSubmessages {
			return
		}
		id, body, flags, next, serr := sedpSplitSubmessage(data, off)
		if serr != nil {
			return
		}
		raw := rtps.RawSubmessage{ID: id, Flags: flags, Body: body}
		if knownInvalidSubmessage(raw) {
			log.Printf("dds: sedp scan invalid submessage %#02x drops rest", id)
			return
		}
		off = next
		if id != want {
			continue
		}
		if handle(body, flags) {
			return
		}
	}
}

// noteSedpHeartbeats scans one datagram for builtin SEDP HEARTBEATs.
//
// A known invalid submessage drops the rest of the datagram per 8.3.4.1
// rule 6. Replays are ignored quietly.
func (idx *SEDPIndex) noteSedpHeartbeats(data []byte, prefix SEDPGUIDPrefix) {
	scanSedpControl(data, sedpSubmessageHeartbeat, func(body []byte, flags byte) bool {
		return idx.noteOneHeartbeat(prefix, body, flags)
	})
}

// noteOneHeartbeat records one HEARTBEAT submessage body.
//
// It reports true when the body is RTPS-invalid so the caller drops the
// rest of the datagram per 8.3.4.1 rule 6. Short and undecodable bodies
// are invalid; non-endpoint writers are ignored. Replays trace quietly.
func (idx *SEDPIndex) noteOneHeartbeat(prefix SEDPGUIDPrefix, body []byte, flags byte) bool {
	if len(body) < sedpEntityLen*2 {
		log.Printf("dds: sedp heartbeat short body, dropping rest")
		return true
	}
	writer, ok := sedpHeartbeatWriterOf(body)
	if !ok {
		return false
	}
	var drops rtps.Counter
	beat, err := rtps.DecodeHeartbeat(body, flags, &drops)
	if err != nil {
		log.Printf("dds: sedp heartbeat decode drops rest: %v", err)
		return true
	}
	key := sedpWriterKey{prefix: prefix, writer: writer}
	if !idx.sedpHeartbeatNote(key, beat.Count, beat.LastSequence) {
		sedpTracef("heartbeat replay writer=%02x%02x%02x%02x count=%d last=%d", writer[0], writer[1], writer[2], writer[3], beat.Count, beat.LastSequence)
	}
	return false
}

// applySedpGapsFromDatagram decodes every builtin SEDP GAP list and applies it.
//
// The full skipped list applies via sedpApplyGapList, not just GapStart.
// A known invalid submessage drops the rest of the datagram per 8.3.4.1
// rule 6.
func (idx *SEDPIndex) applySedpGapsFromDatagram(data []byte, prefix SEDPGUIDPrefix) {
	scanSedpControl(data, sedpSubmessageGap, func(body []byte, flags byte) bool {
		return idx.applyOneGap(prefix, body, flags)
	})
}

// applyOneGap applies one GAP submessage body.
//
// It reports true when the body is RTPS-invalid so the caller drops the
// rest of the datagram per 8.3.4.1 rule 6. Short and undecodable bodies
// are invalid; non-endpoint writers are ignored.
func (idx *SEDPIndex) applyOneGap(prefix SEDPGUIDPrefix, body []byte, flags byte) bool {
	if len(body) < sedpEntityLen*2 {
		log.Printf("dds: sedp gap short body, dropping rest")
		return true
	}
	writer, ok := sedpHeartbeatWriterOf(body)
	if !ok {
		return false
	}
	var drops rtps.Counter
	gap, err := rtps.DecodeGap(body, flags, &drops)
	if err != nil {
		log.Printf("dds: sedp gap decode drops rest: %v", err)
		return true
	}
	key := sedpWriterKey{prefix: prefix, writer: writer}
	idx.sedpApplyGapList(key, gap.SkippedSequences())
	return false
}

func (idx *SEDPIndex) ingestSample(data []byte) error {
	sedpTracef("ingest recv wire len=%d", len(data))
	prefix, err := sedpHeaderPrefix(data)
	if err != nil {
		sedpTracef("ingest header reject len=%d err=%v", len(data), err)
		return fmt.Errorf("dds: sedp listen parse: %w", err)
	}
	idx.noteSedpHeartbeats(data, prefix)
	idx.applySedpGapsFromDatagram(data, prefix)
	idx.noteRemoteTopics(data)
	bodies, flags, err := sedpFindAllEndpointDATA(data)
	if err != nil {
		return ingestFindError(data, err)
	}

	var firstErr error

	installed := 0

	stale := 0

	for i := range bodies {
		didInstall, wasStale, err := idx.ingestOneEndpoint(prefix, bodies[i], flags[i], i, len(bodies))
		if err != nil && firstErr == nil {
			firstErr = err
		}
		if didInstall {
			installed++
		}
		if wasStale {
			stale++
		}
	}
	return ingestOutcomeError(installed, stale, firstErr)
}

// noteRemoteTopics installs remotely announced Topics DATA best-effort.
//
// A spec peer announcing TopicBuiltinTopicData sends no endpoint DATA,
// which is a known builtin, not poison: each decodable topic installs
// into the remote topic set without touching endpoints, sequence gates,
// or the endpoint generation. Topics DATA from the 0x000002c2 writer
// never installs an endpoint: endpoint decode rejects the Topics writer
// and topic bodies never reach the endpoint path. Key-flagged or
// unregister samples remove the topic when present. Malformed entries
// skip with a trace so one poison entry never drops endpoint DATA in a
// mixed datagram; the endpoint path still reports its own errors.
func (idx *SEDPIndex) noteRemoteTopics(data []byte) {
	bodies, flags, err := sedpFindAllTopicsDATA(data)
	if err != nil {
		return
	}
	for i := range bodies {
		topic, derr := sedpDecodeTopicsBody(bodies[i], flags[i])
		if derr != nil {
			sedpTracef("topics note skip index=%d/%d err=%v", i, len(bodies), derr)
			continue
		}
		if flags[i]&sedpFlagKeyPresent != 0 || sedpIsUnregister(bodies[i], flags[i]) {
			sedpTracef("topics note remove dds=%q typeDDS=%q", topic.DDSTopic, topic.TypeDDS)
			idx.removeRemoteTopic(topic)
			continue
		}
		sedpTracef("topics note install dds=%q typeDDS=%q", topic.DDSTopic, topic.TypeDDS)
		idx.installRemoteTopic(topic)
	}
}

// ingestFindError maps an endpoint walk failure to a loud or quiet result.
//
// An RTPS_HE violation is never a quiet control skip: the heartbeat, gap,
// and topic walks already dropped the datagram fail-closed above, so it
// reports loudly instead of masking the attack as a benign control
// sample. Control-only and Topics-only datagrams skip quietly; anything
// else reports loudly.
func ingestFindError(data []byte, err error) error {
	sedpTracef("ingest parse reject len=%d err=%v", len(data), err)
	if herr := sedpValidateHeaderExtension(data); herr != nil {
		return fmt.Errorf("dds: sedp listen parse: %w", herr)
	}
	if errors.Is(err, ErrSEDPInvalidDATA) && (sedpIsSEDPControl(data) || sedpHasTopicsDATA(data)) {
		sedpTracef("ingest control skip len=%d", len(data))
		return nil
	}

	return fmt.Errorf("dds: sedp listen parse: %w", err)
}

// ingestOutcomeError maps an ingest tally to its result.
//
// Installed endpoints succeed. With none installed, the first parse or
// upsert error reports loudly; all-stale replays succeed quietly since the
// peer already has newer state; anything else is invalid input.
func ingestOutcomeError(installed, stale int, firstErr error) error {
	if installed != 0 {
		return nil
	}
	if firstErr != nil {
		return fmt.Errorf("dds: sedp listen upsert: %w", firstErr)
	}
	if stale > 0 {
		return nil
	}

	return fmt.Errorf("dds: sedp listen ingest no endpoint installed: %w", ErrSEDPInvalidDATA)
}

// ingestOneEndpoint decodes, gates, and upserts one endpoint DATA body.
//
// The header prefix binds the endpoint identity: a GUID prefix mismatch
// drops loudly without installing or recording sequence. Sequence zero
// is invalid and drops loudly without installing, mirroring SPDP, while
// the store backstop still skips zero. DATA with the key flag or a
// disposed or unregistered StatusInfo removes the confirmed remote
// instead of installing so endpoints never linger. Replay gates on the
// per-endpoint DATA sequence after successful upsert: the stale check
// runs before upsert without storing, and the sequence records only when
// the endpoint installs. Security enforcement flows through UpsertRemote:
// an enabled suite refuses before install, so a refused sample also
// records no sequence and bumps no generation. Dispose and unregister
// removals need no suite check. It reports whether the endpoint installed,
// whether it was a stale replay, and any parse or upsert error for the
// caller to keep as first failure.
func (idx *SEDPIndex) ingestOneEndpoint(header SEDPGUIDPrefix, body []byte, flag uint8, index, total int) (bool, bool, error) {
	littleEndian := flag&sedpFlagLittleEndian != 0
	seq := sedpDecodeDataSeq(body, littleEndian)
	if seq == 0 {
		sedpTracef("ingest seq-zero reject index=%d/%d flags=%#02x", index, total, flag)
		log.Printf("dds: sedp guid seq zero, dropping")

		return false, false, fmt.Errorf("dds: sedp DATA sequence must be non-zero: %w", ErrSEDPInvalidDATA)
	}
	if flag&sedpFlagKeyPresent != 0 {
		return idx.ingestDispose(header, body, flag, seq, index, total)
	}
	ep, err := sedpDecodeEndpoint(body, flag)
	if err != nil {
		sedpTracef("ingest parse reject index=%d/%d flags=%#02x err=%v", index, total, flag, err)

		return false, false, err
	}
	if sedpIsUnregister(body, flag) {
		sedpTracef("ingest unregister guid=%s seq=%d", ep.ID.String(), seq)
		if ep.ID.Prefix != header {
			sedpTracef("ingest prefix mismatch guid=%s header=%x", ep.ID.String(), header)
			log.Printf("dds: sedp guid prefix mismatches header, dropping")

			return false, false, fmt.Errorf("dds: sedp guid prefix mismatches header: %w", ErrSEDPInvalidDATA)
		}
		if idx.sedpStaleCheck(ep.ID, seq) {
			sedpTracef("ingest stale dispose seq=%d guid=%s", seq, ep.ID.String())

			return false, true, nil
		}
		idx.RemoveRemote(ep.ID)
		idx.sedpStoreSeq(ep.ID, seq)
		idx.sedpNoteWriterHigh(ep.ID.Prefix, body, flag, seq)
		sedpTracef("ingest dispose ok guid=%s", ep.ID.String())

		return true, false, nil
	}
	sedpTracef("ingest parse accept guid=%s ros=%q dds=%q typeDDS=%q typeIDL=%q hash=%q domain=%d reader=%v", ep.ID.String(), ep.ROSTopic, ep.DDSTopic, ep.TypeDDS, ep.TypeIDL, ep.TypeHash, ep.DomainID, ep.IsReader)
	if ep.ID.Prefix != header {
		sedpTracef("ingest prefix mismatch guid=%s header=%x", ep.ID.String(), header)
		log.Printf("dds: sedp guid prefix mismatches header, dropping")
		return false, false, fmt.Errorf("dds: sedp guid prefix mismatches header: %w", ErrSEDPInvalidDATA)
	}
	if ep.DomainID == 0 {
		ep.DomainID = idx.stampDomain()
		sedpTracef("ingest stamp domain=%d guid=%s", ep.ID.String())
	}
	idx.mu.Lock()
	stale, duplicate := idx.ingestStaleCheckLocked(ep.ID, seq)
	idx.mu.Unlock()

	if stale {
		if duplicate {
			sedpTracef("ingest duplicate seq=%d guid=%s evict cancelled", seq, ep.ID.String())
		} else {
			sedpTracef("ingest stale seq=%d guid=%s", seq, ep.ID.String())
		}

		return false, true, nil
	}
	err = idx.UpsertRemoteChecked(header, ep)
	if err != nil {
		sedpTracef("ingest upsert reject guid=%s err=%v", ep.ID.String(), err)
		return false, false, err
	}
	idx.sedpStoreSeq(ep.ID, seq)
	idx.sedpNoteWriterHigh(ep.ID.Prefix, body, flag, seq)
	sedpTracef("ingest upsert ok guid=%s ros=%q domain=%d", ep.ID.String(), ep.ROSTopic, ep.DomainID)
	return true, false, nil
}

// ingestStaleCheckLocked gates one endpoint DATA on its stored sequence.
//
// It holds no lock itself; the caller must hold idx.mu. A duplicate at
// the stored sequence cancels a scheduled dispose eviction as proof of
// liveness and reports stale-quiet without reinstalling. Strictly older
// sequences stay stale without cancelling anything.
//
// The gate is per endpoint GUID on purpose even though sequences run per
// builtin writer: endpoints sharing one writer interleave their samples
// on a single stream, so writerHigh can sit above an endpoint's newest
// sample without that sample being stale. A writer-level gate would drop
// live endpoints; only the per-endpoint high decides replays.
func (idx *SEDPIndex) ingestStaleCheckLocked(id SEDPGUID, seq uint64) (stale, duplicate bool) {
	last, seen := idx.sedpLastSeqLocked(id)
	if !seen || seq > last {
		return false, false
	}
	if seq == last {
		idx.cancelPendingEvictLocked(id.Prefix)
		return true, true
	}
	return true, false
}

// ingestDispose handles one key-flagged DATA as removal.
//
// It extracts the endpoint GUID from inline QoS, enforces the header
// prefix binding, gates on sequence, removes the remote when present,
// and records the sequence so a stale alive cannot reinstall. A missing
// remote still records the sequence and reports success so reordered
// disposes never surface as ingest errors. It reports install success
// for the outcome tally.
func (idx *SEDPIndex) ingestDispose(header SEDPGUIDPrefix, body []byte, flag uint8, seq uint64, index, total int) (bool, bool, error) {
	id, err := sedpDisposeGUID(body, flag)
	if err != nil {
		sedpTracef("ingest dispose parse reject index=%d/%d flags=%#02x err=%v", index, total, flag, err)

		return false, false, err
	}
	if id.Prefix != header {
		sedpTracef("ingest dispose prefix mismatch guid=%s header=%x", id.String(), header)
		log.Printf("dds: sedp guid prefix mismatches header, dropping")

		return false, false, fmt.Errorf("dds: sedp guid prefix mismatches header: %w", ErrSEDPInvalidDATA)
	}
	if idx.sedpStaleCheck(id, seq) {
		sedpTracef("ingest stale dispose seq=%d guid=%s", seq, id.String())

		return false, true, nil
	}
	idx.RemoveRemote(id)
	idx.sedpStoreSeq(id, seq)
	idx.sedpNoteWriterHigh(id.Prefix, body, flag, seq)
	sedpTracef("ingest dispose ok guid=%s", id.String())

	return true, false, nil
}

// sedpDisposeGUID extracts the endpoint GUID for a dispose DATA body.
//
// It collects inline QoS plus payload params and returns the first
// endpoint GUID, which dispose announcements carry without topic or type.
func sedpDisposeGUID(body []byte, flag uint8) (SEDPGUID, error) {
	params, _, err := sedpCollectParams(body, flag)
	if err != nil {
		return SEDPGUID{}, fmt.Errorf("dds: sedp dispose params: %w", err)
	}
	byID := sedpFirstParams(params)
	raw, seen := byID[rtps.PIDEndpointGUIDWire]
	if !seen {
		raw, seen = byID[rtps.PIDEndpointGUID]
	}
	if !seen {
		return SEDPGUID{}, fmt.Errorf("dds: sedp dispose misses endpoint GUID: %w", ErrSEDPInvalidDATA)
	}
	id, err := sedpDecodeEndpointGUID(raw)
	if err != nil {
		return SEDPGUID{}, fmt.Errorf("dds: sedp dispose guid: %w", err)
	}

	return id, nil
}

// sedpIsUnregister reports whether DATA carries disposed or unregistered StatusInfo.
//
// It collects params and checks the first StatusInfo 0x0071 value: either
// the disposed 0x01 or unregistered 0x02 bit marks removal. Absent or
// malformed StatusInfo never marks removal here; malformed values fail
// loudly in the full decode path instead.
func sedpIsUnregister(body []byte, flag uint8) bool {
	littleEndian := flag&sedpFlagLittleEndian != 0
	params, _, err := sedpCollectParams(body, flag)
	if err != nil {
		return false
	}
	status, found, err := LookupStatusInfoParam(params, littleEndian)
	if err != nil || !found {
		return false
	}

	return status&(StatusInfoDisposedFlag|StatusInfoUnregisteredFlag) != 0
}

// sedpLastSeqLocked reports the stored sequence for one endpoint.
//
// The second result reports whether any sequence was recorded. The caller
// must hold idx.mu; use sedpStaleCheck for the locking form.
func (idx *SEDPIndex) sedpLastSeqLocked(id SEDPGUID) (uint64, bool) {
	last, seen := idx.lastSeq[id]

	return last, seen
}

// cancelPendingEvictLocked drops a scheduled dispose eviction for one prefix.
//
// The caller must hold idx.mu. UpsertRemote calls this on every install
// and ingest calls it on duplicate re-announces, so a live writer always
// cancels its eviction before expiry.
func (idx *SEDPIndex) cancelPendingEvictLocked(prefix SEDPGUIDPrefix) {
	delete(idx.pendingEvict, prefix)
}

// sedpStaleCheck reports whether seq is a stale replay without storing.
//
// Zero never gates and always reports false. A sequence at or below the
// last accepted value reports true. It self-locks and is safe for
// concurrent use.
func (idx *SEDPIndex) sedpStaleCheck(id SEDPGUID, seq uint64) bool {
	if seq == 0 {
		return false
	}
	idx.mu.Lock()
	defer idx.mu.Unlock()
	last, seen := idx.lastSeq[id]
	return seen && seq <= last
}

// sedpStoreSeq records seq after a successful upsert.
//
// Zero never advances lastSeq. Smaller or equal sequences never regress
// the high. Insertion gates on matched endpoints: exact GUIDs in remotes,
// GUIDs already tracked, prefixes with any remote, or prefixes with a
// pending eviction store; random prefixes for never-seen GUIDs skip
// without growing the map, consistent with the take-path matched-writer
// gate. It self-locks and is safe for concurrent use.
func (idx *SEDPIndex) sedpStoreSeq(id SEDPGUID, seq uint64) {
	if seq == 0 {
		return
	}
	idx.mu.Lock()
	defer idx.mu.Unlock()
	if _, ok := idx.remotes[id]; !ok {
		if _, ok := idx.lastSeq[id]; !ok {
			if _, ok := idx.pendingEvict[id.Prefix]; !ok && !idx.sedpHasPrefixLocked(id.Prefix) {
				return
			}
		}
	}
	if idx.lastSeq == nil {
		idx.lastSeq = make(map[SEDPGUID]uint64)
	}
	if last, seen := idx.lastSeq[id]; !seen || seq > last {
		idx.lastSeq[id] = seq
	}
}

// sedpNoteWriterHigh records the writer high after a successful DATA upsert.
//
// The key is the header prefix plus the DATA writer entity so GAP clamping
// has a bound even before any heartbeat arrives. Zero sequences are
// ignored. New keys hold sedpAuxCap with oldest-eviction plus loud log.
// The high never gates ingest (see writerHigh): it only bounds GAP
// clamping. It self-locks and is safe for concurrent use.
func (idx *SEDPIndex) sedpNoteWriterHigh(prefix SEDPGUIDPrefix, body []byte, _ uint8, seq uint64) {
	if seq == 0 {
		return
	}
	writer, err := sedpDATAWriter(body)
	if err != nil {
		return
	}
	isEndpoint, _ := sedpClassifyWriter(writer)
	if !isEndpoint {
		return
	}
	key := sedpWriterKey{prefix: prefix, writer: writer}
	idx.mu.Lock()
	defer idx.mu.Unlock()
	if idx.writerHigh == nil {
		idx.writerHigh = make(map[sedpWriterKey]uint64)
	}
	idx.sedpCapWriterHighLocked(key)
	if seq > idx.writerHigh[key] {
		idx.writerHigh[key] = seq
	}
}

// sedpDecodeDataSeq returns the DATA sequence number from one endpoint body.
//
// The caller guarantees the body holds the fixed prefix. Zero means the
// sequence was absent or invalid and never gates upserts.
func sedpDecodeDataSeq(body []byte, littleEndian bool) uint64 {
	if len(body) < sedpDATASeqOff+sedpSeqLen {
		return 0
	}
	raw := body[sedpDATASeqOff : sedpDATASeqOff+sedpSeqLen]
	if littleEndian {
		high := binary.LittleEndian.Uint32(raw[0:4])
		low := binary.LittleEndian.Uint32(raw[4:8])
		return uint64(high)<<32 | uint64(low)
	}
	high := binary.BigEndian.Uint32(raw[0:4])
	low := binary.BigEndian.Uint32(raw[4:8])
	return uint64(high)<<32 | uint64(low)
}

// checkAndStoreSedpSeq is kept as a deprecated alias for sedpStaleCheck plus sedpStoreSeq.
//
// New code calls sedpStaleCheck before UpsertRemote and sedpStoreSeq after,
// so invalid endpoints never pollute lastSeq. It remains for older tests.

// snapshotPrefix returns the index participant prefix for announce stamping.
func (idx *SEDPIndex) snapshotPrefix() SEDPGUIDPrefix {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	return idx.prefix
}

// TopicsSnapshot returns a sorted copy of the distinct announced topics.
//
// It derives one SEDPTopic per distinct DDS topic plus type pair from the
// local endpoints, so the Topics builtin announces what exists without
// duplicating per-endpoint state. It self-locks and is safe for
// concurrent use.
func (idx *SEDPIndex) TopicsSnapshot() []SEDPTopic {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	seen := make(map[SEDPTopic]struct{})

	for _, ep := range idx.locals {
		seen[SEDPTopic{DDSTopic: ep.DDSTopic, TypeDDS: ep.TypeDDS, TypeIDL: ep.TypeIDL}] = struct{}{}
	}

	out := make([]SEDPTopic, 0, len(seen))
	for topic := range seen {
		out = append(out, topic)
	}

	slices.SortFunc(out, func(a, b SEDPTopic) int {
		if a.DDSTopic != b.DDSTopic {
			return strings.Compare(a.DDSTopic, b.DDSTopic)
		}

		if a.TypeDDS != b.TypeDDS {
			return strings.Compare(a.TypeDDS, b.TypeDDS)
		}

		return strings.Compare(a.TypeIDL, b.TypeIDL)
	})

	return out
}

// sedpTopicsSlicesEqual reports whether two sorted topic snapshots match.
func sedpTopicsSlicesEqual(a, b []SEDPTopic) bool {
	return slices.Equal(a, b)
}

// sedpAnnounceTopicsSnapshot serializes and sends one topics snapshot.
//
// It mirrors sedpAnnounceSnapshot for the Topics builtin writer so spec
// peers learn TopicBuiltinTopicData alongside endpoint DATA. A snapshot
// with zero topics sends nothing. Backend-driven announces in backend.go
// (announceWithRoutes, announceTick, announceInitial) carry topics via
// this helper per 8.5.4.
func sedpAnnounceTopicsSnapshot(ctx context.Context, s SEDPSender, prefix SEDPGUIDPrefix, topics []SEDPTopic) error {
	for _, topic := range topics {
		select {
		case <-ctx.Done():
			return ctx.Err() //nolint:wrapcheck // cancel semantics require unwrapped ctx.Err
		default:
		}

		data, err := SerializeSEDPTopicsDATA(prefix, topic)
		if err != nil {
			return fmt.Errorf("dds: sedp announce topics serialize: %w", err)
		}

		err = s.SendSEDP(ctx, data)
		if err != nil {
			return sedpMapLoopError(ctx, fmt.Errorf("dds: sedp announce topics send: %w", err))
		}
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

	if !sedpMatchKeyed(reader, writer) {
		return false
	}

	return sedp3MatchRxOsedp3(reader, writer)
}

// sedp3MatchRxOsedp3 gates matching on offered versus requested QoS.
//
// Malformed QoS on either side fails closed. Endpoints predating QoS
// capture (both sides without RxO data, as in unit literals) keep legacy
// behavior so discovery stays visible. Otherwise the writer offer must
// satisfy the reader request per DDS 1.4 RxO.
func sedp3MatchRxOsedp3(reader, writer SEDPEndpoint) bool {
	if reader.RxOMalformed || writer.RxOMalformed {
		sedpTracef("match reject malformed qos reader=%s writer=%s", reader.ID.String(), writer.ID.String())

		return false
	}

	if reader.RxOWire == nil || writer.RxOWire == nil {
		return true
	}

	report := CheckWireCompatibility(*writer.RxOWire, *reader.RxOWire)
	if !report.Compatible {
		sedpTracef("match reject rxo reader=%s writer=%s", reader.ID.String(), writer.ID.String())

		return false
	}

	return true
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

// sedpWriterLow tracks the first DATA sequence issued per builtin SEDP writer.
//
// Heartbeats advertise the lowest available sequence per DDSI-RTPS 2.5
// section 8.3.8.6, and this map is that tracked value: streams start at 1
// and history is never pruned, so the low stays at the stream start. It
// shares the sedpWriterSeq lock with sedpWriterSeqLast. Tests must not
// assert exact values, only that first stays at the tracked low.
var sedpWriterLow = map[sedpWriterKey]uint64{}

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

	if _, seen := sedpWriterLow[key]; !seen {
		sedpWriterLow[key] = seq
	}

	return seq
}

// sedpWriterFirstAvailable returns the tracked lowest available sequence
// for one builtin writer, defaulting to 1 for unseen streams.
//
// The low is the first sequence ever issued on the stream, which stays
// available because SEDP history is never pruned. Callers advertise this
// instead of a constant so the heartbeat first field derives from tracked
// per-writer state.
func sedpWriterFirstAvailable(prefix SEDPGUIDPrefix, writer SEDPEntityID) uint64 {
	sedpWriterSeq.Lock()
	defer sedpWriterSeq.Unlock()

	if low, seen := sedpWriterLow[sedpWriterKey{prefix: prefix, writer: writer}]; seen {
		return low
	}

	return sedpHeartbeatFirstSeq
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
// Only the DATA writer is returned: the heartbeat covers exactly the
// writer that carried DATA, so a reliable peer never waits on sequences
// from a writer that sent nothing in this datagram. Writers are the
// DDSI-RTPS 2.5 section 8.5.4.3 values, unambiguous by low octet:
// 0x02 Topics, 0x03 publications, 0x04 subscriptions.
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
	count := 0
	for off < len(data) {
		count++
		if count > sedpMaxSubmessages {
			return false
		}
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
// copy for CycloneDDS. Identity stays first-wins on parse. Oversize values
// fail loudly instead of silent truncation.
func sedpEncodeParamListPayload(params []rtps.Param) ([]byte, error) {
	out := []byte{0x00, 0x03, 0x00, 0x00}
	encoded, err := rtps.EncodeParams(params)
	if err != nil {
		return nil, fmt.Errorf("dds: encode sedp params: %w", err)
	}
	out = append(out, encoded...)
	return out, nil
}

// SerializeSEDPDATA serializes an endpoint as an RTPS DATA sample.
//
// The output is one RTPS message: the 20 byte header with the endpoint
// participant prefix plus a DATA submessage from the builtin SEDP writer
// followed by one HEARTBEAT submessage. Publications announce from
// 0x000003c2 and subscriptions from 0x000004c2, both addressed to the
// unknown reader; both values are the spec entities and match the
// observed CycloneDDS wire form. The HEARTBEAT covers only the DATA
// writer because only that writer carries DATA. Inline QoS carries the topic name, type name, endpoint GUID,
// reliable transient local QoS with keep-last history, protocol version, vendor
// id, userdata with the type hash, plus builtin endpoint QoS when set and
// TypeObject bytes when present. The payload carries a PL_CDR copy of the
// same params so CycloneDDS accepts the sample, with the opaque QoS bytes
// after the list when set. Flags are always LE plus InlineQoS plus
// DataPresent (0x07). Unknown vendor PIDs such as 0x8003 and 0x800c are
// tolerated on receive.
// XTypes TypeObject rides as opaque passthrough; full lookup is out of scope for v1.
// Sequence numbers increase per builtin writer stream and each HEARTBEAT
// announces the tracked lowest-available first to the last DATA sequence
// with a fresh count.
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
//
// Body size validates before the SEDP sequence number is consumed so
// oversize samples fail without burning a sequence and breaking
// contiguity for reliable peers.
func SerializeSEDPDATA(ep SEDPEndpoint) ([]byte, error) {
	err := sedpValidateMarshalEndpoint(ep)
	if err != nil {
		return nil, fmt.Errorf("dds: sedp serialize endpoint: %w", err)
	}

	writer := sedpPublicationsWriter
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

	inline, err := rtps.EncodeParams(params)
	if err != nil {
		return nil, fmt.Errorf("dds: encode sedp inline params: %w", err)
	}

	payload, err := sedpEncodeParamListPayload(params)
	if err != nil {
		return nil, err
	}
	payload = append(payload, ep.QoS...)

	bodyLen := sedpDataFixedLen + len(inline) + len(payload)
	if bodyLen > sedpMaxBodyLen {
		return nil, fmt.Errorf("dds: sedp serialize body %d above %d: %w", bodyLen, sedpMaxBodyLen, ErrSEDPInvalidEndpoint)
	}

	seq := nextSedpSeq(ep.ID.Prefix, writer)

	body := make([]byte, 0, bodyLen)
	body = append(body, sedpDATAReserved, sedpDATAReserved)
	body = binary.LittleEndian.AppendUint16(body, sedpOctetsToInlineQoS)
	body = append(body, sedpEntityUnknown[:]...)
	body = append(body, writer[:]...)
	body = binary.LittleEndian.AppendUint32(body, uint32(seq>>32))
	body = binary.LittleEndian.AppendUint32(body, uint32(seq)) //nolint:gosec // G115: low word truncation is the layout
	body = append(body, inline...)
	body = append(body, payload...)

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
		out = sedpAppendHeartbeat(out, hbWriter, sedpWriterFirstAvailable(ep.ID.Prefix, hbWriter), seq, count)
	}

	return out, nil
}

// SEDPTopic is one announced DDS topic for the SEDP Topics builtin.
//
// It mirrors the endpoint identity without endpoint state: the Topics
// writer announces TopicBuiltinTopicData (topic plus type names) so peers
// learn which topics exist, while endpoint matching still uses the
// publication and subscription DATA. DomainID stays zero: topic samples
// carry no domain echo and the transport scope implies the domain.
type SEDPTopic struct {
	// DDSTopic is the DDS name such as rt/chatter.
	DDSTopic string
	// TypeDDS is the DDS type name on the wire.
	TypeDDS string
	// TypeIDL is the demangled IDL name echoed back for the record.
	TypeIDL string
}

// sedpValidateTopic checks topic mapping consistency before serializing.
func sedpValidateTopic(topic SEDPTopic) error {
	if strings.TrimSpace(topic.DDSTopic) == "" {
		return fmt.Errorf("dds: sedp topics empty dds topic: %w", ErrSEDPInvalidTopic)
	}

	if strings.TrimSpace(topic.TypeDDS) == "" {
		return fmt.Errorf("dds: sedp topics empty type: %w", ErrSEDPInvalidType)
	}

	_, _, err := ParseTopicName(topic.DDSTopic)
	if err != nil {
		return fmt.Errorf("dds: sedp topics parse topic: %w", err)
	}

	decodedIDL, err := DemangleTypeName(topic.TypeDDS)
	if err != nil {
		return fmt.Errorf("dds: sedp topics demangle type: %w", err)
	}

	if topic.TypeIDL != "" && decodedIDL != topic.TypeIDL {
		return fmt.Errorf("dds: sedp topics type round trip %q vs %q: %w", decodedIDL, topic.TypeIDL, ErrSEDPInvalidType)
	}

	return nil
}

// SerializeSEDPTopicsDATA serializes one topic as an RTPS DATA sample.
//
// The output mirrors SerializeSEDPDATA: one RTPS message with the
// participant prefix plus a DATA submessage from the Topics writer
// 0x000002c2 followed by one HEARTBEAT submessage for the same writer.
// Inline QoS carries the topic plus type names and the payload carries a
// PL_CDR copy so spec peers accept the sample. Flags are always LE plus
// InlineQoS plus DataPresent. The DATA sequence comes from the per-writer
// stream and the heartbeat first is the tracked lowest-available value.
//
// Body size validates before the sequence is consumed so oversize samples
// fail without burning a sequence and breaking contiguity for reliable
// peers.
func SerializeSEDPTopicsDATA(prefix SEDPGUIDPrefix, topic SEDPTopic) ([]byte, error) {
	err := sedpValidateTopic(topic)
	if err != nil {
		return nil, fmt.Errorf("dds: sedp topics serialize: %w", err)
	}

	params := []rtps.Param{
		{ID: rtps.PIDTopicName, Value: sedpEncodeCDRString(topic.DDSTopic)},
		{ID: rtps.PIDTypeName, Value: sedpEncodeCDRString(topic.TypeDDS)},
	}

	inline, err := rtps.EncodeParams(params)
	if err != nil {
		return nil, fmt.Errorf("dds: encode sedp topics inline params: %w", err)
	}

	payload, err := sedpEncodeParamListPayload(params)
	if err != nil {
		return nil, err
	}

	bodyLen := sedpDataFixedLen + len(inline) + len(payload)
	if bodyLen > sedpMaxBodyLen {
		return nil, fmt.Errorf("dds: sedp topics serialize body %d above %d: %w", bodyLen, sedpMaxBodyLen, ErrSEDPInvalidEndpoint)
	}

	seq := nextSedpSeq(prefix, sedpTopicsWriter)

	body := make([]byte, 0, bodyLen)
	body = append(body, sedpDATAReserved, sedpDATAReserved)
	body = binary.LittleEndian.AppendUint16(body, sedpOctetsToInlineQoS)
	body = append(body, sedpEntityUnknown[:]...)
	body = append(body, sedpTopicsWriter[:]...)
	body = binary.LittleEndian.AppendUint32(body, uint32(seq>>32))
	body = binary.LittleEndian.AppendUint32(body, uint32(seq)) //nolint:gosec // G115: low word truncation is the layout
	body = append(body, inline...)
	body = append(body, payload...)

	out := make([]byte, 0, sedpRTPSHeaderLen+sedpSubmessageHeaderLen+len(body)+2*(sedpSubmessageHeaderLen+sedpHeartbeatSize))
	out = append(out, sedpRTPSMagic...)
	out = append(out, sedpRTPSMajor, sedpRTPSMinor, sedpVendorID0, sedpVendorID1)
	out = append(out, prefix[:]...)

	flags := sedpFlagLittleEndian | sedpFlagInlineQoS | sedpFlagDataPresent

	out = append(out, sedpSubmessageDATA, flags)
	out = binary.LittleEndian.AppendUint16(out, uint16(len(body))) //nolint:gosec // G115: length guarded by the sedpMaxBodyLen check above

	out = append(out, body...)

	for _, hbWriter := range sedpHeartbeatWriters(sedpTopicsWriter) {
		count := nextHeartbeatCount()
		out = sedpAppendHeartbeat(out, hbWriter, sedpWriterFirstAvailable(prefix, hbWriter), seq, count)
	}

	return out, nil
}

// sedpDecodeTopicsBody decodes one Topics DATA body into a topic record.
//
// It mirrors the endpoint identity path with inline plus payload params
// and first-wins, but requires only the topic plus type names: Topics
// DATA carries no endpoint GUID, so endpoint decode rejects it and this
// path must run instead. Unknown ids are ignored. Fail-closed
// discriminator: a body carrying an endpoint GUID param is endpoint
// DATA, never Topics DATA, and rejects loudly instead of decoding as a
// topic.
func sedpDecodeTopicsBody(body []byte, flags byte) (SEDPTopic, error) {
	var topic SEDPTopic

	littleEndian := flags&sedpFlagLittleEndian != 0

	params, _, err := sedpCollectParams(body, flags)
	if err != nil {
		return topic, fmt.Errorf("dds: sedp topics collect params: %w", err)
	}

	byID := sedpFirstParams(params)

	if _, seen := byID[rtps.PIDEndpointGUIDWire]; seen {
		return topic, fmt.Errorf("dds: sedp topics carries endpoint GUID: %w", ErrSEDPInvalidDATA)
	}

	if _, seen := byID[rtps.PIDEndpointGUID]; seen {
		return topic, fmt.Errorf("dds: sedp topics carries endpoint GUID: %w", ErrSEDPInvalidDATA)
	}

	ddsTopic, err := sedpRequiredString(byID, rtps.PIDTopicName, littleEndian)
	if err != nil {
		return topic, fmt.Errorf("dds: sedp topics fields topic: %w", err)
	}

	typeDDS, err := sedpRequiredString(byID, rtps.PIDTypeName, littleEndian)
	if err != nil {
		return topic, fmt.Errorf("dds: sedp topics fields type: %w", err)
	}

	decodedIDL, err := DemangleTypeName(typeDDS)
	if err != nil {
		return topic, fmt.Errorf("dds: sedp topics demangle type: %w", err)
	}

	if _, _, err := ParseTopicName(ddsTopic); err != nil {
		return topic, fmt.Errorf("dds: sedp topics parse topic: %w", err)
	}

	topic = SEDPTopic{DDSTopic: ddsTopic, TypeDDS: typeDDS, TypeIDL: decodedIDL}

	return topic, nil
}

// sedpFindAllTopicsDATA validates the RTPS header and returns every DATA
// body from the SEDP Topics writer with its flags in wire order.
func sedpFindAllTopicsDATA(data []byte) ([][]byte, []byte, error) {
	bodies, flags, err := sedpFindAllBuiltinDATA(data, sedpIsTopicsBuiltin, "topics")
	if err != nil {
		return nil, nil, err
	}

	sedpTracef("find-data datagram len=%d topics-datas=%d", len(data), len(bodies))

	return bodies, flags, nil
}

// sedpFindTopicsDATA validates the RTPS header and returns the first DATA
// body from the SEDP Topics writer with its flags.
func sedpFindTopicsDATA(data []byte) ([]byte, byte, error) {
	bodies, flags, err := sedpFindAllTopicsDATA(data)
	if err != nil {
		return nil, 0, err
	}

	return bodies[0], flags[0], nil
}

// ParseSEDPTopicsDATA parses one Topics DATA sample back into a topic.
//
// It validates the RTPS header, walks the submessages, and decodes the
// first DATA from the Topics writer. Endpoint DATA and unknown
// submessages are skipped. The topic plus type names are required;
// sequence zero rejection is ingest-only like the endpoint path.
func ParseSEDPTopicsDATA(data []byte) (SEDPTopic, error) {
	body, flags, err := sedpFindTopicsDATA(data)
	if err != nil {
		sedpTracef("topics parse reject find-data len=%d err=%v", len(data), err)
		return SEDPTopic{}, fmt.Errorf("dds: sedp topics parse find data: %w", err)
	}

	topic, err := sedpDecodeTopicsBody(body, flags)
	if err != nil {
		sedpTracef("topics parse reject decode len=%d flags=%#02x err=%v", len(data), flags, err)
		return SEDPTopic{}, fmt.Errorf("dds: sedp topics parse decode: %w", err)
	}

	sedpTracef("topics parse accept dds=%q typeDDS=%q len=%d", topic.DDSTopic, topic.TypeDDS, len(data))

	return topic, nil
}

// ParseSEDPTopicsDATAs parses all Topics DATA samples in one datagram.
//
// It mirrors ParseSEDPTopicsDATA per entry with per-entry isolation: one
// malformed entry is skipped without dropping the others. It returns the
// decoded topics in wire order, or an error when no entry decodes.
func ParseSEDPTopicsDATAs(data []byte) ([]SEDPTopic, error) {
	bodies, flags, err := sedpFindAllTopicsDATA(data)
	if err != nil {
		sedpTracef("topics parse reject find-data len=%d err=%v", len(data), err)
		return nil, fmt.Errorf("dds: sedp topics parse find data: %w", err)
	}

	out := make([]SEDPTopic, 0, len(bodies))

	var firstErr error

	for i := range bodies {
		topic, err := sedpDecodeTopicsBody(bodies[i], flags[i])
		if err != nil {
			sedpTracef("topics parse reject decode index=%d/%d len=%d flags=%#02x err=%v", i, len(bodies), len(data), flags[i], err)
			if firstErr == nil {
				firstErr = err
			}

			continue
		}

		sedpTracef("topics parse accept index=%d/%d dds=%q typeDDS=%q len=%d", i, len(bodies), topic.DDSTopic, topic.TypeDDS, len(data))
		out = append(out, topic)
	}

	if len(out) == 0 {
		if firstErr != nil {
			return nil, fmt.Errorf("dds: sedp topics parse decode: %w", firstErr)
		}

		return nil, fmt.Errorf("dds: sedp topics parse finds no decodable topics DATA: %w", ErrSEDPInvalidDATA)
	}

	return out, nil
}

// sedpHasTopicsDATA reports whether a datagram carries Topics DATA.
//
// Endpoint ingest uses it to skip Topics-only datagrams quietly: a spec
// peer announcing TopicBuiltinTopicData sends no endpoint DATA, which is
// a known builtin, not poison.
func sedpHasTopicsDATA(data []byte) bool {
	_, _, err := sedpFindAllTopicsDATA(data)

	return err == nil
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
// Sequence zero rejection is ingest-only: this parser never gates on the
// DATA sequence, so a seq-0 sample decodes here and only the ingest path
// rejects it without installing.
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

// sedpEndpointWriterMatch reports whether a writer entity carries endpoint DATA.
func sedpEndpointWriterMatch(writer SEDPEntityID) bool {
	isEndpoint, _ := sedpClassifyWriter(writer)

	return isEndpoint
}

// sedpCollectBuiltinSub returns one DATA body with its flags when the writer matches.
//
// It returns a nil body for other submessages. Truncation and writer
// errors fail loudly with the submessage offset for the caller.
func sedpCollectBuiltinSub(data []byte, off int, match func(SEDPEntityID) bool) ([]byte, byte, int, error) {
	id, body, flags, next, err := sedpSplitSubmessage(data, off)
	if err != nil {
		return nil, 0, 0, fmt.Errorf("dds: sedp parse submessage at %d: %w", off, err)
	}
	if id != sedpSubmessageDATA {
		return nil, 0, next, nil
	}
	writer, err := sedpDATAWriter(body)
	if err != nil {
		return nil, 0, 0, fmt.Errorf("dds: sedp parse data writer: %w", err)
	}
	if !match(writer) {
		return nil, 0, next, nil
	}
	return body, flags, next, nil
}

// sedpFindAllEndpointDATA validates the RTPS header and returns every
// DATA body from a builtin SEDP writer with its flags in wire order.
//
// The submessage walk caps at sedpMaxSubmessages before dispatch; over-cap
// datagrams drop loudly instead of trusting an unbounded header chain.
func sedpFindAllEndpointDATA(data []byte) ([][]byte, []byte, error) {
	bodies, flags, err := sedpFindAllBuiltinDATA(data, sedpEndpointWriterMatch, "endpoint")
	if err != nil {
		return nil, nil, err
	}

	sedpTracef("find-data datagram len=%d endpoint-datas=%d first-writer=%02x%02x%02x%02x", len(data), len(bodies), bodies[0][sedpDATAWriterOff], bodies[0][sedpDATAWriterOff+1], bodies[0][sedpDATAWriterOff+2], bodies[0][sedpDATAWriterOff+3])
	if len(bodies) > 1 {
		for i, b := range bodies {
			sedpTracef("find-data batched index=%d/%d %s", i, len(bodies), sedpTraceEndpointSummary(b, flags[i]))
		}
	}

	return bodies, flags, nil
}

// sedpFindAllBuiltinDATA validates the RTPS header and returns every DATA
// body whose writer matches, with flags in wire order.
//
// The submessage walk caps at sedpMaxSubmessages before dispatch; over-cap
// datagrams drop loudly instead of trusting an unbounded header chain.
// RTPS_HE placement validates first per 8.3.7.3 so a malformed or
// mispositioned extension drops fail-closed instead of walking past it.
// what names the builtin for the empty error, such as endpoint or topics.
func sedpFindAllBuiltinDATA(data []byte, match func(SEDPEntityID) bool, what string) ([][]byte, []byte, error) {
	err := sedpValidateRTPSHeader(data)
	if err != nil {
		return nil, nil, fmt.Errorf("dds: sedp parse header: %w", err)
	}

	if err := sedpValidateHeaderExtension(data); err != nil {
		return nil, nil, err
	}

	off := sedpRTPSHeaderLen

	var bodies [][]byte

	var flags []byte

	seen := 0

	for off < len(data) {
		seen++
		if seen > sedpMaxSubmessages {
			return nil, nil, fmt.Errorf("dds: sedp parse submessages above %d: %w", sedpMaxSubmessages, ErrSEDPInvalidDATA)
		}
		body, flag, next, err := sedpCollectBuiltinSub(data, off, match)
		if err != nil {
			return nil, nil, err
		}
		off = next
		if body == nil {
			continue
		}
		bodies = append(bodies, body)
		flags = append(flags, flag)
	}

	if len(bodies) == 0 {
		return nil, nil, fmt.Errorf("dds: sedp parse finds no %s DATA: %w", what, ErrSEDPInvalidDATA)
	}

	return bodies, flags, nil
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
//
// The Topics writer is not an endpoint writer: it carries
// TopicBuiltinTopicData without an endpoint GUID, so endpoint ingest
// skips it through sedpIsTopicsBuiltin instead of installing it.
func sedpClassifyWriter(writer SEDPEntityID) (isEndpoint, isReader bool) {
	if writer == sedpPublicationsWriter {
		return true, false
	}

	if writer == sedpSubscriptionsWriter {
		return true, true
	}

	return false, false
}

// sedpIsTopicsBuiltin reports whether an entity is a SEDP Topics builtin.
//
// Both the Topics writer and reader match: neither carries endpoint
// DATA, so endpoint paths skip them quietly while the Topics parse path
// below decodes them.
func sedpIsTopicsBuiltin(writer SEDPEntityID) bool {
	return writer == sedpTopicsWriter || writer == sedpTopicsReader
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
		RxOMalformed:         identity.fields.wireQoSMalformed,
	}
	if identity.fields.hasWireQoS {
		parsed := identity.fields.wireQoS
		ep.RxOWire = &parsed
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
	// wireQoS carries the parsed RxO wire QoS on success.
	wireQoS WireQoS
	// hasWireQoS reports a successful QoS parse.
	hasWireQoS bool
	// wireQoSMalformed reports a failed QoS parse, failing closed at match.
	wireQoSMalformed bool
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
// the payload stays opaque. Identity stays first-wins and the payload
// decodes once: inline plus payload supply params in one walk, so the
// Cyclone path carries no second decode.
func sedpCollectParams(body []byte, flags byte) ([]rtps.Param, []byte, error) {
	littleEndian := flags&sedpFlagLittleEndian != 0

	if len(body) < sedpDataFixedLen {
		return nil, nil, fmt.Errorf("dds: sedp collect params need %d have %d: %w", sedpDataFixedLen, len(body), ErrSEDPInvalidDATA)
	}

	if len(body) > sedpMaxBodyLen {
		return nil, nil, fmt.Errorf("dds: sedp collect params body %d above cap: %w", len(body), ErrSEDPInvalidDATA)
	}

	var params []rtps.Param

	rest := body[sedpDataFixedLen:]

	if flags&sedpFlagInlineQoS != 0 {
		if len(rest) < 4 {
			return nil, nil, fmt.Errorf("dds: sedp collect inline need 4 have %d: %w", len(rest), ErrSEDPInvalidDATA)
		}
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
//
// It returns errors loudly for malformed partition or durability-service
// params instead of installing defaults. Security decode skips stay
// best-effort with trace because unknown vendor PIDs must not break
// matching.
func sedpFieldsExtensions(params []rtps.Param, byID map[uint16][]byte, littleEndian bool, fields *sedpEndpointWireFields) error {
	secInfo, secErrs := ExtractSecurityInfoEndian(params, littleEndian)
	for _, secErr := range secErrs {
		sedpTracef("parse security skip err=%v", secErr)
	}

	fields.security = secInfo

	err := CheckProtectedDiscovery(secInfo)
	if err != nil {
		sedpTracef("parse secured peer crypto unavailable err=%v", err)
	}

	partitionNames, err := FindPartitionParamEndian(params, littleEndian)
	if err != nil {
		return fmt.Errorf("dds: sedp parse fields partitions: %w", err)
	}
	fields.partitions = partitionNames

	if rawDurabilityService, seen := byID[PIDDurabilityService]; seen {
		durabilityService, err := DecodeDurabilityService(rawDurabilityService, littleEndian)
		if err != nil {
			return fmt.Errorf("dds: sedp parse fields durability-service: %w", err)
		}
		fields.durabilityService = durabilityService
		fields.hasDurabilityService = true
	}

	return nil
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

	err = sedpFieldsExtensions(params, byID, littleEndian, &fields)
	if err != nil {
		return fields, err
	}

	sedp3FieldsWireQoSsedp3(params, littleEndian, &fields)

	return fields, nil
}

// sedp3FieldsWireQoSsedp3 parses RxO wire QoS without failing ingest.
//
// Success stores the parsed offer for matching, with absent policies
// filled from DDS spec defaults per ParseWireQoSsedp3WithDefaults.
// Failure marks the record malformed so the match gate fails closed while
// discovery stays visible for INCOMPATIBLE status. A payload that carries
// a parameter-list encapsulation but fails to decode never silently falls
// back to inline-only params: the single decode in sedpCollectParams
// already fails loudly for identity, and QoS value failures mark
// malformed here.
func sedp3FieldsWireQoSsedp3(params []rtps.Param, littleEndian bool, fields *sedpEndpointWireFields) {
	parsed, err := ParseWireQoSsedp3WithDefaults(params, littleEndian)
	if err != nil {
		sedpTracef("parse qos skip err=%v", err)
		fields.wireQoSMalformed = true

		return
	}

	fields.wireQoS = parsed
	fields.hasWireQoS = true
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
// sedpCountParamHeader loads one parameter id and length.
func sedpCountParamHeader(blob []byte, off int, littleEndian bool) (int, int, error) {
	if len(blob)-off < 4 {
		return 0, 0, fmt.Errorf("dds: sedp parse params header need 4 have %d: %w", len(blob)-off, ErrSEDPInvalidDATA)
	}
	var id, length int
	if littleEndian {
		id = int(binary.LittleEndian.Uint16(blob[off : off+2]))
		length = int(binary.LittleEndian.Uint16(blob[off+2 : off+4]))
	} else {
		id = int(binary.BigEndian.Uint16(blob[off : off+2]))
		length = int(binary.BigEndian.Uint16(blob[off+2 : off+4]))
	}
	return id, length, nil
}

// sedpCountParamsStep advances one parameter entry.
//
// It returns the next offset and whether the sentinel terminated the list.
func sedpCountParamsStep(blob []byte, off int, littleEndian bool) (int, bool, error) {
	id, length, err := sedpCountParamHeader(blob, off, littleEndian)
	if err != nil {
		return 0, false, err
	}
	off += 4
	if id == int(rtps.ParamSentinel) {
		if length != 0 {
			return 0, false, fmt.Errorf("dds: sedp parse params sentinel carries %d bytes: %w", length, ErrSEDPInvalidDATA)
		}
		return off, true, nil
	}
	if length < 0 || len(blob)-off < length {
		return 0, false, fmt.Errorf("dds: sedp parse params value need %d have %d: %w", length, len(blob)-off, ErrSEDPInvalidDATA)
	}
	off += length
	pad := (4 - length%4) % 4
	if len(blob)-off < pad {
		return 0, false, fmt.Errorf("dds: sedp parse params pad need %d have %d: %w", pad, len(blob)-off, ErrSEDPInvalidDATA)
	}
	return off + pad, false, nil
}

// sedpCountParams walks parameter headers counting entries without allocating values.
//
// Each entry needs at least a 4 byte header, so the walk bounds work before
// the full decode allocates. It stops and reports over-cap loudly instead
// of trusting a crafted length chain.
func sedpCountParams(blob []byte, littleEndian bool) error {
	off := 0
	count := 0
	for {
		step, done, err := sedpCountParamsStep(blob, off, littleEndian)
		if err != nil {
			return err
		}
		if done {
			return nil
		}
		count++
		if count > sedpMaxParams {
			return fmt.Errorf("dds: sedp parse param count above %d: %w", sedpMaxParams, ErrSEDPInvalidDATA)
		}
		off = step
	}
}

func sedpDecodeParams(blob []byte, littleEndian bool) ([]rtps.Param, int, error) {
	err := sedpCountParams(blob, littleEndian)
	if err != nil {
		return nil, 0, err
	}
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
//
// Counters above 0xFFFFFF panic loudly instead of truncating silently,
// matching KeyedUserEntity, so a caller passing a wider counter never
// mints a colliding id.
func sedpEntityFor(counter uint32, isReader bool) SEDPEntityID {
	if counter > sedpUserEntityMax {
		panic(fmt.Sprintf("dds: sedp entity counter %#x above 24 bits", counter))
	}

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

	if a.DomainTag != b.DomainTag {
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

	if (a.RxOWire == nil) != (b.RxOWire == nil) {
		return false
	}

	if a.RxOWire != nil && *a.RxOWire != *b.RxOWire {
		return false
	}

	if a.RxOMalformed != b.RxOMalformed {
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
	if ep.Security.Properties != nil {
		ep.Security.Properties = maps.Clone(ep.Security.Properties)
	}
	if ep.WireOffer != nil {
		offer := *ep.WireOffer
		ep.WireOffer = &offer
	}
	if ep.RxOWire != nil {
		rxo := *ep.RxOWire
		ep.RxOWire = &rxo
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

// INTEGRATION state for SEDP discovery hardening.
//
// Done in this file: endpoint GUID binds to the RTPS header prefix with a
// loud drop on mismatch, so spoofed endpoints never install; UpsertRemote
// documents the trust requirement while UpsertRemoteChecked enforces the
// binding fail-closed and ingest uses the checked form; replay gates
// on the per-endpoint DATA sequence after successful upsert, with the
// sequence recorded only when the endpoint installs and never before
// validation; provisional installs gate on the DATA sequence (zero and
// stale reject) with the same remotes cap; seq 0 never advances lastSeq;
// domain tags enforce exact equality per 8.5.5.1 (empty equals only
// empty, no wildcards) at upsert and provisional install; remote topics
// install from the Topics writer into a separate advisory set that never
// installs endpoints nor bumps the endpoint generation; the parameter
// list caps at sedpMaxParams entries before full decode via a header
// pre-walk, with NUL termination checks as before; the datagram submessage
// walk caps at sedpMaxSubmessages (4096, matching rtps) before dispatch
// with a loud drop on over-cap; RTPS_HE bodies validate as known typed
// submessages and placement validates first per 8.3.7.3 (malformed or
// mispositioned extensions drop fail-closed) in both the builtin DATA
// walk and the control scan; HEARTBEAT Count tracks per builtin writer
// with replay ignore (Count at or below last with no new LastSequence
// draws no work) and Count==0 handled as single processing per
// LastSequence without a NACK storm, documented on sedpHeartbeatNote;
// GAP decodes the full skipped list and applies it in order via
// sedpApplyGapList, clamped to the known heartbeat bound plus one and
// capped at current high plus sedpGapNoBoundCap (64, one repair window)
// when no bound is known; unicast INFO_DST enforcement lives in
// sedpCheckInfoDstUnicast, which unicast callers must call (multicast
// never calls it).
//
// Done elsewhere: backend shouldDropUnicastForInfoDst enforces the same
// unicast INFO_DST on the live socket path; backend noteSedpHeartbeatBound
// plus applySedpGaps mirror the writer bound and full gap list for ACK
// decisions; backend sedpBuiltinWriter routes the Topics (0x02) plus
// publications (0x03) plus subscriptions (0x04) writers to the SEDP queue
// and excludes all three from the user data path; rtps DecodeMessage caps
// submessages at 4096 before dispatch and DecodeGap bounds the trailing
// set.
//
// Explicitly open and unenforced: multicast ingress never calls the
// INFO_DST helper because group sockets carry every sender and have no
// per-sender identity; standalone SEDPIndex.ingestSample stays transport
// agnostic and relies on the unicast caller for INFO_DST, while the index
// itself sends no ACKs so heartbeat suppression only bounds local
// reprocessing. No rejected scaffolding remains: inline-only plus payload
// duplicate handling stays for Cyclone interop and is covered by fixture
// tests.
//
// TODO(backend-owner): backend.go names sedpWriterEntityB14 (0x02) for
// publications and documents sedpBuiltinReaderForWriter as publications to
// 0x000002c7; per DDSI-RTPS 2.5 section 8.5.4.3 third byte 0x02 is the Topics
// builtin (publications 0x03, subscriptions 0x04), matching
// sedpTopicsWriter plus sedpPublicationsWriter plus sedpSubscriptionsWriter
// here and the testbed conformance pins. The pairing itself is mechanical
// (same third byte, kind 0xC7) and demux routes all three writers to the
// SEDP path, so wire behavior is unaffected and only the backend names plus
// docs drift. sedpBuiltinReaderForWriter lives in backend.go and is not
// owned here.
