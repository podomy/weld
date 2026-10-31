// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package dds

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/podomy/weld/internal/core"
	"github.com/podomy/weld/internal/transports"
	"github.com/podomy/weld/internal/types"
)

// Data path for first contact over loopback bootstrap.
//
// Scope is loopback unicast with DiscoveryRangeLocalhost semantics: SPDP
// advertises the LAN address and binds wildcard, but DATA and SEDP fanout
// send to discovered unicast ports via loopback. The path is best effort
// with minimal repair: Take gaps count as lost, ACKNACK repairs from
// repairHistory, GAP skips without lost, and DATA_FRAG reassembles with
// bounds in frag.go with best effort fallback for incomplete samples.
// Payloads are generic bytes with no IDL codegen: the type
// name travels as an opaque string and the payload is never deserialized
// here. There is no cgo and no import of internal/core.
//
// Wire framing is one Publish to one DATA frame in v1. A DATA frame carries
// a 16 byte writer GUID plus a uint64 sequence in little endian plus three
// uint32 little endian lengths plus DDS topic bytes plus IDL type bytes
// plus XCDR1 envelope bytes from types.MarshalMessage in little endian.
// The envelope header is validated plus length checked before parsing via
// types.UnmarshalMessage. Truncated input counts a drop and never panics.
//
// Ownership follows transports copy once. Publish copies the caller payload
// into a fresh envelope and frame, so the caller must not mutate after
// Publish returns but may reuse the slice immediately. The Writer sink must
// copy when it retains beyond the call. Take copies user bytes out and
// returns uniquely owned slices. Take never retains the input frame.
//
// Sequence numbers start at FirstSequenceNumber per writer GUID. The first
// sequence on a fresh Publisher is 1. The receiver accepts the first sample
// from each GUID with no lost count, counts later gaps as lost, and drops
// duplicates with a drop counter.
//
// RTPS bridging embeds one DATA v1 frame verbatim as the payload of one
// RTPS DATA submessage. Layering is datapath frame (GUID plus sequence
// plus lengths plus envelope) inside RTPS DATA payload inside RTPS
// message. MarshalRTPSPayload validates a frame with the same length
// checks as Take and returns an owned copy ready to embed as an RTPS
// DATA payload. ParseRTPSPayload validates a received RTPS DATA payload
// the same way and returns an owned copy ready for Take. FrameSink and
// FrameSource are minimal RTPS adapters that carry the writer GUID
// alongside the payload without importing the rtps package, so the rtps
// layer stays decoupled. PublishTo and TakeFrom reuse the exact Publish
// and Take encoding, ordering, and ownership rules and only swap the
// transport hook. Existing Publish and Take behavior is unchanged.

// FirstSequenceNumber is the first DATA sequence per writer GUID.
//
// A fresh Publisher emits 1 first, then 2, and so on. Zero is never emitted
// and is rejected on receive as invalid.
const FirstSequenceNumber uint64 = 1

// frameFixedSize is GUID plus sequence plus three lengths.
//
// Layout is 16 byte GUID plus 8 byte sequence plus 4 byte topic length plus
// 4 byte type length plus 4 byte user length, all lengths little endian.
// The GUID is opaque and never swapped. Lengths are byte counts with no NUL.
const frameFixedSize = 36

// guidSize is the 16 byte DDS writer GUID width.
//
// This 16 byte participant GUID is distinct from the 8 byte service GUID in
// types.ServiceGUIDSize. The two must not be mixed.
const guidSize = 16

// seqSize is the 8 byte sequence number width on the DATA frame.
const seqSize = 8

// lenSize is the 4 byte width of each length prefix on the DATA frame.
const lenSize = 4

// maxTopicLen caps one DDS topic name on the DATA frame.
//
// The cap keeps worst case allocation small when a corrupt prefix claims a
// huge length. ROS topic names in v1 stay far below this bound.
const maxTopicLen = 1024

// maxTypeLen caps one IDL type name on the DATA frame.
//
// Type names are capped at 255 chars by the type system. The 1024 bound
// leaves headroom while still bounding corrupt prefixes.
const maxTypeLen = 1024

// maxPayloadLen caps one user payload on the DATA frame.
//
// The 32 MiB bound matches the shared DDS RTPS sample cap used to bound one
// sample on the wire in internal/types, so a corrupt user length cannot
// force more than 32 MiB.
const maxPayloadLen = 32 << 20

// ErrTruncated reports a truncated DATA frame or envelope.
var ErrTruncated = errors.New("dds: truncated frame")

// ErrInvalidFrame reports a malformed DATA frame.
var ErrInvalidFrame = errors.New("dds: invalid frame")

// ErrStaleSequence reports a duplicate or reordered sample dropped under best effort.
var ErrStaleSequence = errors.New("dds: stale sequence")

// WriterGUID is the 16 byte DDS writer GUID for one DATA publisher.
//
// Bytes are opaque and never byte swapped. This 16 byte participant GUID is
// distinct from the 8 byte service GUID used in service envelopes.
type WriterGUID [16]byte

// Writer is a byte sink for encoded DATA frames.
//
// The backend wires this later to the sidecar transport. WriteFrame must not
// retain the frame beyond the call without copying, per copy once alignment
// with transports.Backend. Implementations must be safe for concurrent use.
type Writer interface {
	// WriteFrame accepts one encoded DATA frame.
	WriteFrame(frame []byte) error
}

// FrameSink accepts one RTPS DATA payload alongside its writer GUID.
//
// The payload is one DATA v1 frame validated by MarshalRTPSPayload, ready
// to embed verbatim as an RTPS DATA submessage payload. The guid duplicates
// the GUID already stored inside the frame so the RTPS layer can route
// without parsing the payload. WriteFrame must not retain payload beyond
// the call without copying, matching the Writer copy once rule.
// Implementations must be safe for concurrent use.
type FrameSink interface {
	// WriteFrame accepts one RTPS payload for the given writer GUID.
	// It honors ctx cancellation and returns ctx.Err when ctx ends first.
	WriteFrame(ctx context.Context, guid WriterGUID, payload []byte) error
}

// FragSink emits ordered fragments for one large RTPS payload.
//
// Each call carries one fragment of the sample named by seq. fragNum is one
// based in emission order 1 through total. fragmentSize is the nominal size
// for all but the last fragment, sampleSize is the full reassembled size,
// and data holds exactly the fragment octets. The seq is the Publisher
// sample sequence for the fragmented sample. Implementations must preserve
// emission order on the wire and must copy data when retaining beyond the
// call. Implementations must be safe for concurrent use.
type FragSink interface {
	// WriteFrag accepts one fragment for the given writer GUID and seq.
	// It honors ctx cancellation and returns ctx.Err when ctx ends first.
	WriteFrag(ctx context.Context, guid WriterGUID, seq uint64, fragNum, total uint32, fragmentSize uint16, sampleSize uint32, data []byte) error
}

// FrameSource supplies one RTPS DATA payload alongside its writer GUID.
//
// The returned payload must be one DATA v1 frame ready for
// ParseRTPSPayload, with guid matching the GUID stored inside the frame.
// The sequence is the RTPS DATA submessage number, zero when the queue
// predates sequence tracking; plain vendor takes need it for gap NACKs
// while framed takes ignore it in favor of the frame header.
// The returned payload is newly owned by the caller; the source must not
// retain or mutate it after return, and the caller must not mutate it
// after TakeFrom returns without copying. Implementations must be safe
// for concurrent use.
type FrameSource interface {
	// ReadFrame returns the next RTPS payload for its writer GUID.
	// It blocks until a frame arrives or ctx ends and returns ctx.Err
	// on cancellation.
	ReadFrame(ctx context.Context) (guid WriterGUID, seq uint64, payload []byte, err error)
}

// Publisher emits one DATA frame per Publish with a per GUID sequence.
//
// The zero value is not usable. Use NewPublisher. Publisher is safe for
// concurrent use. Publishes serialize on an internal mutex so sequence
// order matches Publish call order. The mutex must not be copied after
// first use; getters self-lock with RLock while publishes hold Lock.
type Publisher struct {
	mu   sync.RWMutex
	guid WriterGUID
	next uint64
	sink Writer
}

// NewPublisher returns a Publisher starting at FirstSequenceNumber.
//
// It reports an error when sink is nil. The GUID is stored by value and the
// sink is retained without copying. The first Publish emits sequence 1.
func NewPublisher(guid WriterGUID, sink Writer) (*Publisher, error) {
	if sink == nil {
		return nil, fmt.Errorf("dds: new publisher nil sink: %w", ErrInvalidFrame)
	}

	return &Publisher{guid: guid, next: FirstSequenceNumber, sink: sink}, nil
}

// GUID returns the writer GUID for this publisher.
//
// It self-locks with RLock and is safe for concurrent use.
func (p *Publisher) GUID() WriterGUID {
	p.mu.RLock()
	defer p.mu.RUnlock()

	return p.guid
}

// NextSequence returns the sequence the next Publish will emit.
//
// It self-locks with RLock and is safe for concurrent use.
func (p *Publisher) NextSequence() uint64 {
	p.mu.RLock()
	defer p.mu.RUnlock()

	return p.next
}

// setGUID moves the publisher to a new writer GUID.
//
// It self-locks with Lock and is safe for concurrent use. It is used to
// align the static publisher with the SEDP announced writer so DATA
// entities match the announcement. Callers must ensure sequence
// continuity: reusing one writer per topic keeps the counter monotonic.
func (p *Publisher) setGUID(guid WriterGUID) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.guid = guid
}

// encodeNextFrame builds one DATA frame for msg with the given guid and seq.
//
// It maps the ROS topic via TopicName, checks the payload cap, wraps the
// payload with an envelope via types.MarshalMessage, and encodes the
// frame. It shares validation and wire framing used by Publish and
// PublishTo so both paths emit identical bytes. Sequence zero and over
// cap lengths report ErrInvalidFrame. The op names the caller for error
// prefixes, either publish or publish to, so error strings stay distinct.
func encodeNextFrame(guid WriterGUID, seq uint64, msg transports.OutgoingMessage, op string) ([]byte, error) {
	ddsTopic, userLen, envelope, err := preparePublishPayload(msg, op)
	if err != nil {
		return nil, err
	}

	frame, err := encodeFrame(guid, seq, ddsTopic, msg.TypeName, userLen, envelope)
	if err != nil {
		return nil, fmt.Errorf("dds: %s encode seq %d: %w", op, seq, err)
	}

	return frame, nil
}

// preparePublishPayload validates one outgoing message for framing.
//
// It shares topic mapping, payload cap checks, and envelope wrapping used
// by Publish and PublishTo. The returned user length and envelope are
// ready for encodeFrame. The op names the caller for error prefixes.
func preparePublishPayload(msg transports.OutgoingMessage, op string) (string, uint32, []byte, error) {
	ddsTopic, err := TopicName(msg.TopicName, false)
	if err != nil {
		return "", 0, nil, fmt.Errorf("dds: %s topic %q: %w", op, msg.TopicName, err)
	}

	if len(msg.Payload) > maxPayloadLen {
		return "", 0, nil, fmt.Errorf(
			"dds: %s payload length %d above cap %d: %w",
			op,
			len(msg.Payload),
			maxPayloadLen,
			ErrInvalidFrame,
		)
	}

	userLen := uint32(len(msg.Payload)) //nolint:gosec // G115: length checked against maxPayloadLen above

	envelope := types.MarshalMessage(msg.Payload, true)

	return ddsTopic, userLen, envelope, nil
}

// Publish encodes one message into one DATA frame and emits it.
//
// It maps the ROS topic via TopicName with standard conventions, wraps the
// raw payload with a little endian envelope via types.MarshalMessage, and
// writes one DATA frame to the sink. The payload is generic bytes and is
// never deserialized. Nil payload means an empty sample and still emits a
// dummy byte on the wire per the envelope rule.
//
// Ownership is copy once. Publish copies the payload into fresh envelope
// and frame storage before WriteFrame returns, so the caller must not mutate
// msg.Payload after Publish returns but may reuse it immediately. The sink
// must copy when it retains beyond the call.
//
// Sequence starts at FirstSequenceNumber per writer GUID. The counter
// advances only on successful write, so a failed Publish may be retried with
// the same sequence. Sequence zero is never emitted. Overflow past the
// maximum uint64 reports ErrInvalidFrame instead of wrapping.
func (p *Publisher) Publish(msg transports.OutgoingMessage) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.next == 0 {
		return fmt.Errorf("dds: publish sequence overflow: %w", ErrInvalidFrame)
	}

	seq := p.next

	frame, err := encodeNextFrame(p.guid, seq, msg, "publish")
	if err != nil {
		return err
	}

	err = p.sink.WriteFrame(frame)
	if err != nil {
		return fmt.Errorf("dds: publish write seq %d: %w", seq, err)
	}

	p.next++

	return nil
}

// PublishTo encodes one message into one DATA frame and emits it through a FrameSink.
//
// It reuses the exact Publish encoding, sequence, and ownership rules and
// only swaps the transport hook from Writer to FrameSink. The frame is
// validated with MarshalRTPSPayload and passed as an owned RTPS DATA
// payload alongside the publisher GUID, so the RTPS layer can embed it
// without parsing. The sink must copy when it retains beyond the call.
// The counter advances only on successful write, matching Publish. A nil
// sink reports ErrInvalidFrame. Context cancellation reports ctx.Err.
//
// Large payloads above the single DATA limit fragment into ordered pieces
// using the frag bounds in frag.go. Small payloads use one WriteFrame.
// Large payloads require the sink to also implement FragSink and emit one
// WriteFrag per fragment in order 1 through total with the same seq. The
// counter advances only after all fragments succeed, so a failed publish
// may be retried with the same sequence. Samples above maxPayloadLen or
// with fragment totals above fragMaxCount report an error without advancing.
func (p *Publisher) PublishTo(ctx context.Context, s FrameSink, msg transports.OutgoingMessage) error {
	if s == nil {
		return fmt.Errorf("dds: publish to nil sink: %w", ErrInvalidFrame)
	}

	err := ctx.Err()
	if err != nil {
		return err //nolint:wrapcheck // ctx error passes through unchanged
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if p.next == 0 {
		return fmt.Errorf("dds: publish to sequence overflow: %w", ErrInvalidFrame)
	}

	seq := p.next

	frame, err := encodeNextFrame(p.guid, seq, msg, "publish to")
	if err != nil {
		return err
	}

	payload, err := MarshalRTPSPayload(frame)
	if err != nil {
		return fmt.Errorf("dds: publish to validate seq %d: %w", seq, err)
	}

	guid := p.guid

	if !fragNeedsSplit(uint32(len(payload))) { //nolint:gosec // G115: length checked against maxPayloadLen in preparePublishPayload
		err = s.WriteFrame(ctx, guid, payload)
		if err != nil {
			return fmt.Errorf("dds: publish to write seq %d: %w", seq, err)
		}

		p.next++

		return nil
	}

	err = p.publishFrags(ctx, s, guid, seq, payload)
	if err != nil {
		return err
	}

	p.next++

	return nil
}

// publishFrags emits one large payload as ordered fragments.
//
// The caller must hold p.mu and must have validated payload. It requires
// the sink to implement FragSink, splits payload with planFragSend, and
// emits fragments 1 through total in order with the same seq. A missing
// FragSink or a bounds failure reports an error. The caller advances the
// sequence only after all fragments succeed.
func (p *Publisher) publishFrags(ctx context.Context, s FrameSink, guid WriterGUID, seq uint64, payload []byte) error {
	fragSink, ok := s.(FragSink)
	if !ok {
		return fmt.Errorf("dds: publish to payload %d above single %d without fragment sink: %w", len(payload), fragSingleMax, ErrInvalidFrame)
	}

	sampleSize := uint32(len(payload)) //nolint:gosec // G115: length checked against maxPayloadLen in preparePublishPayload

	fragmentSize, total, err := planFragSend(sampleSize)
	if err != nil {
		return fmt.Errorf("dds: publish to fragment seq %d: %w", seq, err)
	}

	for fragNum := uint32(1); fragNum <= total; fragNum++ {
		err = ctx.Err()
		if err != nil {
			return err //nolint:wrapcheck // ctx error passes through unchanged
		}

		fragData := fragSlice(payload, fragNum, total, fragmentSize)

		err = fragSink.WriteFrag(ctx, guid, seq, fragNum, total, fragmentSize, sampleSize, fragData)
		if err != nil {
			return fmt.Errorf("dds: publish to fragment seq %d frag %d of %d: %w", seq, fragNum, total, err)
		}
	}

	return nil
}

// DataReceiver accepts DATA frames in best effort order and strips the envelope.
//
// The zero value is not usable. Use NewDataReceiver. DataReceiver is safe for
// concurrent use. Take gaps count missing samples as lost; GAP via ApplyGap
// skips without lost and ACKNACK repair arrives as duplicate DATA through the
// demux. Duplicates and reordered samples are dropped with ErrStaleSequence.
// The mutex must not be copied after first use; counters self-lock with
// RLock while takes hold Lock for drops and ordering.
type DataReceiver struct {
	mu    sync.RWMutex
	last  map[WriterGUID]uint64
	lost  uint64
	drops uint64
	sedp  *SEDPIndex
	// provisional holds opportunistic endpoints for unknown writers.
	//
	// Each entry is tagged Provisional true with its FirstSeen time. It never
	// overwrites a confirmed SEDP record: lookups always check SEDP remotes
	// first, so a confirmed record takes precedence immediately. Promotion is
	// owned by the sedp agent via SEDP upsert clearing Provisional; this map
	// only marks provisional and re-checks SEDP on each sample.
	provisional map[WriterGUID]ProvisionalEndpoint
	// plainNack tracks per-writer plain-path repair state for vendor
	// samples that bypass framed sequencing. Late joins NACK base 1 on
	// first sight past seq 1; later gaps NACK once per advance.
	plainNack map[WriterGUID]*plainNackState
	// nackSender transmits ACKNACKs for plain-path gaps, wired by the
	// backend at join. Nil disables gap NACKs; delivery flows unchanged.
	nackSender func(ctx context.Context, guid WriterGUID, base uint64) error
	// reasonCounts tallies drops by machine readable cause for dashboards.
	reasonCounts map[DropReason]uint64
	// contentFilters holds compiled content filters by ROS topic, set via
	// SetContentFilter and consulted on every take. Absent means unfiltered.
	contentFilters map[string]*contentFilterEntry
	// lively tracks per-writer liveliness plus per-topic deadlines, set via
	// SetLivelinessTracker and SetDeadlineTracker. Absent means untracked:
	// delivery flows exactly as before.
	lively map[WriterGUID]*core.LivelinessTracker
	// livelyCfg holds per-topic liveliness configs for lazy tracker
	// creation. Writers discovered after subscribe still get tracked on
	// their first take, so enforcement needs no re-subscribe.
	livelyCfg map[string]livelyConfig
	// deadlines holds per-topic deadline trackers by ROS topic.
	deadlines map[string]*core.DeadlineTracker
}

// livelyConfig names the liveliness contract for one subscribed topic.
type livelyConfig struct {
	// kind selects manual versus automatic assertion.
	kind core.LivelinessPolicy
	// lease bounds the assert interval.
	lease core.Duration
}

// DropReason names one counted drop cause for dashboards.
type DropReason string

// Drop reason codes for unknown versus provisional failures.
const (
	// DropReasonUnknownWriter counts drops with no SEDP or provisional record.
	DropReasonUnknownWriter DropReason = "unknown-writer"
	// DropReasonProvisionalDecodeFail counts undecodable provisional samples.
	DropReasonProvisionalDecodeFail DropReason = "provisional-decode-fail"
	// DropReasonDeadlineMissed counts samples dropped past the topic deadline.
	DropReasonDeadlineMissed DropReason = "deadline-missed"
	// DropReasonLivelinessExpired counts samples dropped from expired writers.
	DropReasonLivelinessExpired DropReason = "liveliness-expired"
	// DropReasonNackSendFailed counts gap NACKs that failed to send.
	DropReasonNackSendFailed DropReason = "nack-send-failed"
)

var (
	// ErrUnknownWriter reports a drop with no SEDP or provisional record.
	ErrUnknownWriter = errors.New("dds: unknown writer")
	// ErrProvisionalDecodeFail reports an undecodable provisional sample.
	ErrProvisionalDecodeFail = errors.New("dds: provisional decode fail")
	// ErrDeadlineMissed reports a sample dropped past the topic deadline.
	ErrDeadlineMissed = errors.New("dds: deadline missed")
	// ErrLivelinessExpired reports a sample dropped from an expired writer.
	ErrLivelinessExpired = errors.New("dds: liveliness expired")
)

// ProvisionalQoS carries DATA inline QoS for one unknown writer sample.
//
// DDSTopic and TypeDDS come from the DATA inline QoS where present. TypeHash
// rides alongside when the peer sends it, else HasHash stays false to mark
// hash-unset. Empty topic or type means the inline QoS was absent.
type ProvisionalQoS struct {
	// DDSTopic is the DDS topic from inline QoS, empty when absent.
	DDSTopic string
	// TypeDDS is the DDS type name from inline QoS, empty when absent.
	TypeDDS string
	// TypeHash is the type hash passthrough, empty when absent.
	TypeHash string
	// HasHash reports whether TypeHash was present.
	HasHash bool
}

// ProvisionalEndpoint is one opportunistic endpoint record for an unknown writer.
//
// It mirrors the shared contract: an endpoint record with a Provisional bool
// plus a FirstSeen time. Promotion clears Provisional through SEDP upsert,
// which lives in sedp.go and is owned by the sedp agent.
type ProvisionalEndpoint struct {
	// GUID is the unknown writer GUID this record was installed for.
	GUID WriterGUID
	// DDSTopic is the DDS topic from inline QoS, empty when absent.
	DDSTopic string
	// TypeDDS is the DDS type name from inline QoS, empty when absent.
	TypeDDS string
	// TypeIDL is the demangled IDL name, empty when TypeDDS is absent.
	TypeIDL string
	// TypeHash is the passthrough hash, empty when hash-unset.
	TypeHash string
	// HashSet reports whether TypeHash was present.
	HashSet bool
	// Provisional is always true for entries in the provisional map.
	Provisional bool
	// FirstSeen is the wall time of the first DATA that installed this record.
	FirstSeen time.Time
}

// qoSFrameSource optionally carries DATA inline QoS per sample.
//
// TakeFrom uses ReadFrame when the source only implements FrameSource. When
// the source also implements this interface, TakeFrom uses ReadFrameWithQoS
// so the provisional install can take topic, type, and hash from the DATA
// inline QoS where present. The backend demux strips inline QoS today, so
// live sources report hasQoS false and provisional installs hash-unset.
type qoSFrameSource interface {
	// ReadFrameWithQoS returns the next payload plus its inline QoS.
	ReadFrameWithQoS(ctx context.Context) (WriterGUID, uint64, []byte, ProvisionalQoS, bool, error)
}

// NewDataReceiver returns an empty DataReceiver with no observed writers.
func NewDataReceiver() *DataReceiver {
	return &DataReceiver{
		last:         make(map[WriterGUID]uint64),
		provisional:  make(map[WriterGUID]ProvisionalEndpoint),
		reasonCounts: make(map[DropReason]uint64),
	}
}

// SetSEDPIndex installs the SEDP index for plain CDR topic resolution.
//
// It retains the index pointer without copying; the index stays safe for
// concurrent use. A nil index disables generic resolution and leaves the
// String-only fallback. It self-locks with Lock and is safe for concurrent use.
func (r *DataReceiver) SetSEDPIndex(idx *SEDPIndex) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.sedp = idx
}

// remoteForGUID resolves a writer GUID to a remote writer endpoint.
//
// It snapshots remotes once and requires an exact GUID match. The second
// result reports whether a writer was found. A nil index reports false.
// Unknown writers return false so callers count the sample as a drop
// instead of attributing it to another writer on the same participant.
func (r *DataReceiver) remoteForGUID(guid WriterGUID) (SEDPEndpoint, bool) {
	r.mu.RLock()
	idx := r.sedp
	r.mu.RUnlock()

	if idx == nil {
		return SEDPEndpoint{}, false
	}

	remotes := idx.RemoteEndpoints()

	var prefix SEDPGUIDPrefix
	copy(prefix[:], guid[:12])

	var entity SEDPEntityID
	copy(entity[:], guid[12:16])

	want := SEDPGUID{Prefix: prefix, Entity: entity}

	for _, ep := range remotes {
		if ep.IsReader {
			continue
		}

		if ep.ID == want {
			return ep, true
		}
	}

	return SEDPEndpoint{}, false
}

// LostCount returns the count of samples declared lost across gaps.
//
// It self-locks with RLock and is safe for concurrent use.
func (r *DataReceiver) LostCount() uint64 {
	r.mu.RLock()
	defer r.mu.RUnlock()

	return r.lost
}

// DroppedCount returns the count of dropped frames and envelopes.
//
// It self-locks with RLock and is safe for concurrent use.
func (r *DataReceiver) DroppedCount() uint64 {
	r.mu.RLock()
	defer r.mu.RUnlock()

	return r.drops
}

// countDrop records one dropped frame under lock.
func (r *DataReceiver) countDrop() {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.drops++
}

// countDropReason records one dropped frame with a machine readable cause.
//
// It increments the total DroppedCount plus the per-reason tally so
// dashboards can tell unknown-writer drops apart from provisional decode
// failures. It self-locks and is safe for concurrent use.
func (r *DataReceiver) countDropReason(reason DropReason) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.drops++

	if r.reasonCounts == nil {
		r.reasonCounts = make(map[DropReason]uint64)
	}

	r.reasonCounts[reason]++
}

// DropReasonCount returns the count of drops for one reason code.
//
// It self-locks with RLock and is safe for concurrent use.
func (r *DataReceiver) DropReasonCount(reason DropReason) uint64 {
	r.mu.RLock()
	defer r.mu.RUnlock()

	return r.reasonCounts[reason]
}

// ProvisionalForGUID returns the provisional record for one writer GUID.
//
// The second result reports whether a provisional was installed. Confirmed
// SEDP records are not returned here: callers must check remoteForGUID
// first so a confirmed record always takes precedence over stale provisional
// fields. It self-locks with RLock and is safe for concurrent use.
func (r *DataReceiver) ProvisionalForGUID(guid WriterGUID) (ProvisionalEndpoint, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	ep, ok := r.provisional[guid]

	return ep, ok
}

// ensureProvisional installs a provisional record for an unknown writer.
//
// It first re-checks SEDP remotes and does nothing when a confirmed record
// exists, so provisional never overwrites SEDP. When no confirmed record
// exists and no provisional exists yet, it installs one tagged provisional
// with the first-seen time and the inline QoS topic, type, and hash where
// present, else hash-unset. An existing provisional is never overwritten, so
// FirstSeen stays stable and stale fields cannot clobber newer state. It
// returns the stored record and whether a record is present after the call.
// It self-locks and is safe for concurrent use.
// provisionalCap bounds the receiver-local provisional map under writer
// churn. Insert is rare (one per unknown writer) so a lazy scan is cheap.
const provisionalCap = 1024

// evictStaleProvisionalLocked removes unconfirmed placeholders older than
// the SEDP provisional TTL and, if still over cap, the oldest first. The
// TTL matches sedpProvisionalTTL rationale: 3x the SPDP lease gives a
// confirmed announce ample time to promote while stale entries cannot
// linger. Caller must hold r.mu.
func (r *DataReceiver) evictStaleProvisionalLocked(now time.Time) {
	for id, ep := range r.provisional {
		if now.Sub(ep.FirstSeen) >= sedpProvisionalTTL {
			delete(r.provisional, id)
		}
	}

	for len(r.provisional) >= provisionalCap {
		var (
			oldest  WriterGUID
			oldestT time.Time
			first   = true
		)

		for id, ep := range r.provisional {
			if first || ep.FirstSeen.Before(oldestT) {
				oldest, oldestT, first = id, ep.FirstSeen, false
			}
		}

		delete(r.provisional, oldest)
	}
}

// ensureProvisional returns the stored provisional for an unknown writer.
func (r *DataReceiver) ensureProvisional(guid WriterGUID, qos ProvisionalQoS, hasQoS bool) (ProvisionalEndpoint, bool) {
	if _, ok := r.remoteForGUID(guid); ok {
		r.mu.Lock()
		delete(r.provisional, guid)
		r.mu.Unlock()

		return ProvisionalEndpoint{}, false
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if r.provisional == nil {
		r.provisional = make(map[WriterGUID]ProvisionalEndpoint)
	}

	if existing, ok := r.provisional[guid]; ok {
		return existing, true
	}

	r.evictStaleProvisionalLocked(time.Now())

	ep := ProvisionalEndpoint{
		GUID:        guid,
		Provisional: true,
		FirstSeen:   time.Now(),
	}

	if hasQoS {
		ep.DDSTopic = qos.DDSTopic
		ep.TypeDDS = qos.TypeDDS
		ep.TypeHash = qos.TypeHash
		ep.HashSet = qos.HasHash

		if qos.TypeDDS != "" {
			if idl, err := DemangleTypeName(qos.TypeDDS); err == nil {
				ep.TypeIDL = idl
			}
		}
	}

	r.provisional[guid] = ep

	return ep, true
}

// decodeCycloneChatter validates a foreign plain CDR String payload for a known writer.
//
// CycloneDDS sends user DATA as raw XCDR directly, not as a Weld DATA
// frame. When Weld frame validation fails, this resolves the writer GUID
// through SEDP remotes, maps the DDS topic with ParseTopicName, demangles
// the DDS type, and checks the payload holds exactly one XCDR1 String with
// no trailing bytes. The returned topic is the mapped ROS topic for that
// writer, such as a per-run topic, and the type is the demangled IDL, never
// a hardcoded /chatter value. The returned payload is an owned copy of the
// input XCDR bytes. It reports false when the writer is unknown, mapping
// fails, the IDL is not String, or the payload is not a valid String. It
// skips Weld sequence tracking. The String-only scope stays here and the
// generic path stays in decodePlainCDR.
func (r *DataReceiver) decodeCycloneChatter(guid WriterGUID, payload []byte) (transports.IncomingMessage, bool) {
	var empty transports.IncomingMessage

	if len(payload) == 0 {
		return empty, false
	}

	if len(payload) > maxPayloadLen+types.HeaderSize+4 {
		return empty, false
	}

	ep, ok := r.remoteForGUID(guid)
	if !ok {
		return empty, false
	}

	rosTopic, _, err := ParseTopicName(ep.DDSTopic)
	if err != nil {
		return empty, false
	}

	idl, err := DemangleTypeName(ep.TypeDDS)
	if err != nil {
		return empty, false
	}

	if idl != "std_msgs__msg__String" {
		return empty, false
	}

	dec, err := types.NewDecoder(payload)
	if err != nil {
		return empty, false
	}

	_, err = dec.ReadString()
	if err != nil {
		return empty, false
	}

	err = dec.Finish()
	if err != nil {
		return empty, false
	}

	cp := append([]byte(nil), payload...)

	return transports.IncomingMessage{
		TopicName: rosTopic,
		TypeName:  idl,
		Payload:   cp,
	}, true
}

// isValidPlainString reports whether payload holds exactly one XCDR1 String.
func isValidPlainString(payload []byte) bool {
	dec, err := types.NewDecoder(payload)
	if err != nil {
		return false
	}

	_, err = dec.ReadString()
	if err != nil {
		return false
	}

	err = dec.Finish()

	return err == nil
}

// isValidPlainInt32 reports whether payload holds exactly one XCDR1 Int32.
func isValidPlainInt32(payload []byte) bool {
	dec, err := types.NewDecoder(payload)
	if err != nil {
		return false
	}

	_, err = dec.ReadInt32()
	if err != nil {
		return false
	}

	err = dec.Finish()

	return err == nil
}

// isValidPlainFloat64 reports whether payload holds exactly one XCDR1 Float64.
func isValidPlainFloat64(payload []byte) bool {
	dec, err := types.NewDecoder(payload)
	if err != nil {
		return false
	}

	_, err = dec.ReadFloat64()
	if err != nil {
		return false
	}

	err = dec.Finish()

	return err == nil
}

// isValidPlainBool reports whether payload holds one XCDR1 Bool.
//
// A single Bool leaves 3 zero pad bytes from Bytes, so Finish would report
// trailing bytes. The loop below accepts only zero pad here.
func isValidPlainBool(payload []byte) bool {
	dec, err := types.NewDecoder(payload)
	if err != nil {
		return false
	}

	_, err = dec.ReadBool()
	if err != nil {
		return false
	}

	for dec.Remaining() > 0 {
		pad, err := dec.ReadUint8()
		if err != nil {
			return false
		}

		if pad != 0 {
			return false
		}
	}

	return true
}

// isValidPlainVector3 reports whether payload holds three XCDR1 Float64 values.
func isValidPlainVector3(payload []byte) bool {
	dec, err := types.NewDecoder(payload)
	if err != nil {
		return false
	}

	for range 3 {
		_, err = dec.ReadFloat64()
		if err != nil {
			return false
		}
	}

	err = dec.Finish()

	return err == nil
}

// readPlainHeader reads Header stamp plus frame_id for validation.
func readPlainHeader(dec *types.Decoder) bool {
	_, err := dec.ReadInt32()
	if err != nil {
		return false
	}

	_, err = dec.ReadUint32()
	if err != nil {
		return false
	}

	_, err = dec.ReadString()
	return err == nil
}

// isValidPlainPointStamped reports whether payload holds PointStamped fields.
func isValidPlainPointStamped(payload []byte) bool {
	dec, err := types.NewDecoder(payload)
	if err != nil {
		return false
	}

	if !readPlainHeader(dec) {
		return false
	}

	for range 3 {
		_, err = dec.ReadFloat64()
		if err != nil {
			return false
		}
	}

	err = dec.Finish()

	return err == nil
}

// isValidPlainHeaderPair reports whether payload holds header plus two Float64 values.
//
// Temperature and FluidPressure share the same wire shape: Header plus value
// plus variance.
func isValidPlainHeaderPair(payload []byte) bool {
	dec, err := types.NewDecoder(payload)
	if err != nil {
		return false
	}

	if !readPlainHeader(dec) {
		return false
	}

	for range 2 {
		_, err = dec.ReadFloat64()
		if err != nil {
			return false
		}
	}

	err = dec.Finish()

	return err == nil
}

// validatePlainPayload checks one plain XCDR sample for the given IDL.
//
// It uses strict per-type checks for the live matrix types. Unknown IDL
// accepts any header valid payload. It returns false on any decode failure.
func validatePlainPayload(payload []byte, idl string) bool {
	switch idl {
	case "std_msgs__msg__String":
		return isValidPlainString(payload)
	case "std_msgs__msg__Int32":
		return isValidPlainInt32(payload)
	case "std_msgs__msg__Float64":
		return isValidPlainFloat64(payload)
	case "std_msgs__msg__Bool":
		return isValidPlainBool(payload)
	case "geometry_msgs__msg__Vector3":
		return isValidPlainVector3(payload)
	case "geometry_msgs__msg__PointStamped":
		return isValidPlainPointStamped(payload)
	case "sensor_msgs__msg__Temperature":
		return isValidPlainHeaderPair(payload)
	case "sensor_msgs__msg__FluidPressure":
		return isValidPlainHeaderPair(payload)
	default:
		_, err := types.NewDecoder(payload)
		return err == nil
	}
}

// isPlainCDRSample reports whether payload is a valid plain XCDR sample for typeName.
//
// It returns false for empty type names so opaque Weld frames keep framed
// handling. Known matrix types use strict per-type checks via
// validatePlainPayload; unknown non-empty types accept any header valid
// payload. Publish uses it to select plain interop DATA for CycloneDDS.
func isPlainCDRSample(payload []byte, typeName string) bool {
	if typeName == "" {
		return false
	}

	return validatePlainPayload(payload, typeName)
}

// decodePlainCDR validates a foreign plain CDR sample using SEDP remotes.
//
// It resolves the writer GUID to a remote writer, maps the DDS topic via
// ParseTopicName, demangles the DDS type, and validates the payload for the
// IDL. When the writer is unknown it installs a provisional endpoint tagged
// provisional with the first-seen time, taking topic, type, and hash from
// the inline QoS where present else hash-unset, then decodes through the
// normal path instead of dropping. It re-checks SEDP remotes on each sample
// so a confirmed record always takes precedence over stale provisional
// fields, and provisional never overwrites SEDP. It returns false when the
// payload is empty or oversize, when mapping fails, or when the payload is
// not valid for the IDL. The returned payload is an owned copy. It skips
// Weld sequence tracking.
func (r *DataReceiver) decodePlainCDR(guid WriterGUID, payload []byte) (transports.IncomingMessage, bool) {
	return r.decodePlainCDRWithQoS(guid, payload, ProvisionalQoS{}, false)
}

// decodeConfirmedPlainCDR validates a plain sample against a confirmed endpoint.
func decodeConfirmedPlainCDR(ep SEDPEndpoint, payload []byte) (transports.IncomingMessage, bool) {
	var empty transports.IncomingMessage
	rosTopic, _, err := ParseTopicName(ep.DDSTopic)
	if err != nil {
		return empty, false
	}
	idl, err := DemangleTypeName(ep.TypeDDS)
	if err != nil {
		return empty, false
	}
	if !validatePlainPayload(payload, idl) {
		return empty, false
	}
	cp := append([]byte(nil), payload...)
	return transports.IncomingMessage{
		TopicName: rosTopic,
		TypeName:  idl,
		Payload:   cp,
	}, true
}

// decodeProvisionalPlainCDR validates a plain sample against a provisional endpoint.
func decodeProvisionalPlainCDR(prov ProvisionalEndpoint, payload []byte) (transports.IncomingMessage, bool) {
	var empty transports.IncomingMessage
	if prov.DDSTopic == "" || prov.TypeDDS == "" {
		return empty, false
	}
	rosTopic, _, err := ParseTopicName(prov.DDSTopic)
	if err != nil {
		return empty, false
	}
	idl := prov.TypeIDL
	if idl == "" {
		demangled, err := DemangleTypeName(prov.TypeDDS)
		if err != nil {
			return empty, false
		}
		idl = demangled
	}
	if !validatePlainPayload(payload, idl) {
		return empty, false
	}
	cp := append([]byte(nil), payload...)
	return transports.IncomingMessage{
		TopicName: rosTopic,
		TypeName:  idl,
		Payload:   cp,
	}, true
}

// decodePlainCDRWithQoS validates a foreign plain CDR sample with inline QoS.
//
// It shares the confirmed path used by decodePlainCDR: SEDP remotes are
// checked first and a confirmed endpoint always wins. When the writer is
// unknown it installs a provisional endpoint from qos when hasQoS is true,
// else hash-unset, then validates the payload against the provisional type
// through the normal validatePlainPayload path. Empty topic or type with no
// confirmed record means undecodable and returns false so the caller counts
// a provisional-decode-fail drop. It never panics.
func (r *DataReceiver) decodePlainCDRWithQoS(guid WriterGUID, payload []byte, qos ProvisionalQoS, hasQoS bool) (transports.IncomingMessage, bool) {
	var empty transports.IncomingMessage

	if len(payload) == 0 {
		return empty, false
	}

	if len(payload) > maxPayloadLen+types.HeaderSize+4 {
		return empty, false
	}

	ep, ok := r.remoteForGUID(guid)
	if ok {
		return decodeConfirmedPlainCDR(ep, payload)
	}

	prov, _ := r.ensureProvisional(guid, qos, hasQoS)
	return decodeProvisionalPlainCDR(prov, payload)
}

// takeDecoded enforces ordering and builds the message from validated parts.
//
// It shares sequence tracking and copy once ownership used by Take and
// TakeFrom so both paths handle gaps, duplicates, and payload copies
// identically. The decoded frame, payload view, and ROS topic must already
// be validated. It locks internally for ordering.
func (r *DataReceiver) takeDecoded(
	decoded decodedFrame,
	payloadView []byte,
	rosTopic string,
) (transports.IncomingMessage, error) {
	var empty transports.IncomingMessage

	snap := r.snapshotTakeHooks(rosTopic, decoded.guid)

	r.mu.Lock()

	err := r.trackSequenceLocked(decoded.guid, decoded.seq)
	if err != nil {
		r.mu.Unlock()
		return empty, fmt.Errorf("dds: take sequence: %w", err)
	}

	var outPayload []byte

	if decoded.userLen > 0 {
		outPayload = make([]byte, decoded.userLen)
		copy(outPayload, payloadView[:decoded.userLen])
	}

	r.mu.Unlock()

	if rej := evalTakeHooks(snap, rosTopic, decoded.typeName, payloadView[:decoded.userLen], decoded.guid, writerGUIDKeyed(decoded.guid)); rej != nil {
		return empty, r.countTakeHookReject(rej)
	}

	msg := transports.IncomingMessage{
		TopicName: rosTopic,
		TypeName:  decoded.typeName,
		Payload:   outPayload,
	}

	return msg, nil
}

// Take decodes one DATA frame and returns the user message.
//
// It validates DATA lengths before parsing, validates the envelope header
// plus length before parse via types.UnmarshalMessage, demangles the DDS
// topic via ParseTopicName, enforces best effort ordering, strips the
// envelope, and returns an IncomingMessage with the ROS topic, the opaque
// IDL type name, and exactly userLen payload bytes.
//
// Best effort tolerance works per writer GUID. The first sample from a GUID
// is always accepted with no lost count. A sequence equal to last plus one
// is accepted. A larger sequence counts the skipped samples as lost and
// accepts the current frame with no repair in v1. A sequence at or below
// the last observed value is dropped with ErrStaleSequence and a drop
// counter increment.
//
// Truncated or malformed input increments the drop counter and returns an
// explicit error. Take never panics on short or corrupt input. Nil user
// length returns nil Payload per the transports empty sample rule. Nonempty
// user length returns a freshly allocated copy holding exactly those bytes.
//
// Ownership is copy once. Take never retains the input frame. The returned
// Payload is uniquely owned by the caller.
func (r *DataReceiver) Take(frame []byte) (transports.IncomingMessage, error) {
	var empty transports.IncomingMessage

	decoded, err := decodeFrame(frame)
	if err != nil {
		r.countDrop()

		return empty, fmt.Errorf("dds: take decode: %w", err)
	}

	payloadView, err := validateEnvelopePayload(decoded.envelope, decoded.userLen)
	if err != nil {
		r.countDrop()

		return empty, fmt.Errorf("dds: take envelope: %w", err)
	}

	rosTopic, _, err := ParseTopicName(decoded.ddsTopic)
	if err != nil {
		r.countDrop()

		return empty, fmt.Errorf("dds: take topic %q: %w", decoded.ddsTopic, err)
	}

	msg, err := r.takeDecoded(decoded, payloadView, rosTopic)
	if err != nil {
		return empty, err
	}

	return msg, nil
}

// tryPlainFallbacks decodes foreign plain samples before counting a drop.
//
// It returns the hook rejection when a decoded sample is filtered, keyed
// invalid, or liveliness gated, so TakeFrom counts exactly once under the
// right reason instead of falling through to the unknown-writer path.
func (r *DataReceiver) tryPlainFallbacks(ctx context.Context, guid WriterGUID, seq uint64, payload []byte, qos ProvisionalQoS, hasQoS bool) (transports.IncomingMessage, *takeHookReject, bool) {
	var empty transports.IncomingMessage

	if msg, ok := r.decodePlainCDRWithQoS(guid, payload, qos, hasQoS); ok {
		r.notePlainSample(ctx, guid, seq)
		snap := r.snapshotTakeHooks(msg.TopicName, guid)
		if rej := evalTakeHooks(snap, msg.TopicName, msg.TypeName, payload, guid, writerGUIDKeyed(guid)); rej != nil {
			return empty, rej, false
		}

		return msg, nil, true
	}
	if msg, ok := r.decodeCycloneChatter(guid, payload); ok {
		r.notePlainSample(ctx, guid, seq)
		snap := r.snapshotTakeHooks(msg.TopicName, guid)
		if rej := evalTakeHooks(snap, msg.TopicName, msg.TypeName, payload, guid, writerGUIDKeyed(guid)); rej != nil {
			return empty, rej, false
		}

		return msg, nil, true
	}
	return empty, nil, false
}

// takeValidatedFrame finishes TakeFrom for one validated Weld frame.
func (r *DataReceiver) takeValidatedFrame(decoded decodedFrame) (transports.IncomingMessage, error) {
	var empty transports.IncomingMessage
	payloadView, err := validateEnvelopePayload(decoded.envelope, decoded.userLen)
	if err != nil {
		r.countDrop()
		return empty, fmt.Errorf("dds: take from envelope: %w", err)
	}
	rosTopic, _, err := ParseTopicName(decoded.ddsTopic)
	if err != nil {
		r.countDrop()
		return empty, fmt.Errorf("dds: take from topic %q: %w", decoded.ddsTopic, err)
	}
	return r.takeDecoded(decoded, payloadView, rosTopic)
}

// TakeFrom reads one RTPS DATA payload from a FrameSource and returns the user message.
//
// It blocks on ReadFrame until a frame arrives or ctx ends, validates the
// payload with ParseRTPSPayload, checks the outer GUID matches the GUID
// inside the frame, then reuses takeDecoded for ordering, demangling, and
// copy once ownership. A GUID mismatch counts a drop and reports
// ErrInvalidFrame. Parse failures try the generic plain CDR path via SEDP
// remotes with opportunistic provisional install, then the String-only
// fallback with SEDP topic mapping, before counting a drop. When the writer
// is unknown a provisional endpoint is installed and the payload decodes
// through the normal path instead of dropping; SEDP remotes are re-checked
// on each sample so a confirmed record takes precedence immediately and
// provisional never overwrites SEDP. Drops count only when all paths fail,
// with DropReasonProvisionalDecodeFail for undecodable provisional samples
// and DropReasonUnknownWriter otherwise. Sequence handling matches Take
// exactly. A nil source reports ErrInvalidFrame. Context cancellation
// reports ctx.Err. When the source also implements ReadFrameWithQoS the
// inline QoS supplies the provisional topic, type, and hash where present,
// else the provisional marks hash-unset.
func (r *DataReceiver) TakeFrom(ctx context.Context, s FrameSource) (transports.IncomingMessage, error) {
	var empty transports.IncomingMessage

	if s == nil {
		return empty, fmt.Errorf("dds: take from nil source: %w", ErrInvalidFrame)
	}

	err := ctx.Err()
	if err != nil {
		return empty, err //nolint:wrapcheck // ctx error passes through unchanged
	}

	guid, seq, payload, qos, hasQoS, err := readFrameWithQoS(ctx, s)
	if err != nil {
		return empty, fmt.Errorf("dds: take from read: %w", err)
	}

	if os.Getenv("WELD_TAKE_DEBUG") != "" {
		n := min(len(payload), 32)
		_, _ = fmt.Fprintf(os.Stderr, "weld-take-debug: frame guid=%x len=%d head=% x\n", guid, len(payload), payload[:n]) //nolint:errcheck // diagnostics are best effort
	}

	frame, err := ParseRTPSPayload(payload)
	if err != nil {
		if msg, rej, ok := r.tryPlainFallbacks(ctx, guid, seq, payload, qos, hasQoS); ok {
			return msg, nil
		} else if rej != nil {
			return empty, r.countTakeHookReject(rej)
		}
		return empty, r.countPlainDrop(guid, err, "payload")
	}

	decoded, err := decodeFrame(frame)
	if err != nil {
		r.countDrop()
		return empty, fmt.Errorf("dds: take from decode: %w", err)
	}

	if decoded.guid != guid {
		if msg, rej, ok := r.tryPlainFallbacks(ctx, guid, seq, payload, qos, hasQoS); ok {
			return msg, nil
		} else if rej != nil {
			return empty, r.countTakeHookReject(rej)
		}
		return empty, r.countPlainDrop(guid, ErrInvalidFrame, "guid mismatch")
	}

	return r.takeValidatedFrame(decoded)
}

// readFrameWithQoS reads one payload plus optional inline QoS.
//
// When the source implements ReadFrameWithQoS the inline QoS supplies the
// provisional topic, type, and hash where present. Otherwise it falls back
// to ReadFrame with hasQoS false so the provisional marks hash-unset.
func readFrameWithQoS(ctx context.Context, s FrameSource) (WriterGUID, uint64, []byte, ProvisionalQoS, bool, error) {
	var empty WriterGUID

	if qs, ok := s.(qoSFrameSource); ok {
		guid, seq, payload, qos, hasQoS, err := qs.ReadFrameWithQoS(ctx)
		if err != nil {
			return empty, 0, nil, ProvisionalQoS{}, false, fmt.Errorf("dds: read frame with qos: %w", err)
		}

		return guid, seq, payload, qos, hasQoS, nil
	}

	guid, seq, payload, err := s.ReadFrame(ctx)
	if err != nil {
		return empty, 0, nil, ProvisionalQoS{}, false, fmt.Errorf("dds: read frame: %w", err)
	}

	return guid, seq, payload, ProvisionalQoS{}, false, nil
}

// countPlainDrop counts one plain CDR drop with a distinct reason code.
//
// When a provisional record exists for guid the drop counts as
// DropReasonProvisionalDecodeFail with ErrProvisionalDecodeFail; otherwise
// it counts as DropReasonUnknownWriter with ErrUnknownWriter. The returned
// error joins the reason sentinel with the cause so errors.Is still sees
// ErrTruncated or ErrInvalidFrame from the cause for backward compat while
// dashboards tell the two reasons apart. It never panics.
func (r *DataReceiver) countPlainDrop(guid WriterGUID, cause error, op string) error {
	if _, ok := r.ProvisionalForGUID(guid); ok {
		r.countDropReason(DropReasonProvisionalDecodeFail)

		return fmt.Errorf("dds: take from %s provisional: %w", op, errors.Join(ErrProvisionalDecodeFail, cause))
	}

	r.countDropReason(DropReasonUnknownWriter)

	return fmt.Errorf("dds: take from %s unknown writer: %w", op, errors.Join(ErrUnknownWriter, cause))
}

// trackSequenceLocked records one sequence for best effort ordering.
//
// The caller must hold mu. The first sample from a GUID is accepted with no
// lost count. A gap adds the skipped count to lost and accepts the current
// frame. A duplicate or old sequence counts a drop and returns
// ErrStaleSequence.
func (r *DataReceiver) trackSequenceLocked(guid WriterGUID, seq uint64) error {
	last, seen := r.last[guid]
	if !seen {
		r.last[guid] = seq

		return nil
	}

	if seq <= last {
		r.drops++

		return fmt.Errorf("dds: stale seq %d last %d: %w", seq, last, ErrStaleSequence)
	}

	if seq > last+1 {
		r.lost += seq - last - 1
	}

	r.last[guid] = seq

	return nil
}

// decodedFrame holds one parsed DATA frame.
//
// The envelope aliases the input frame. The caller must copy user bytes out
// before reusing the input.
type decodedFrame struct {
	guid     WriterGUID
	seq      uint64
	ddsTopic string
	typeName string
	userLen  uint32
	envelope []byte
}

// encodeFrame builds one DATA frame from its parts.
//
// The caller supplies the DDS topic already mapped via TopicName, the opaque
// IDL type name, the original user length, and the envelope from
// types.MarshalMessage. Lengths are stored little endian. The GUID is copied
// verbatim. Sequence zero is rejected. Empty DDS topic and over cap lengths
// are rejected.
func encodeFrame(guid WriterGUID, seq uint64, ddsTopic, typeName string, userLen uint32, envelope []byte) ([]byte, error) {
	if seq == 0 {
		return nil, fmt.Errorf("dds: encode sequence 0: %w", ErrInvalidFrame)
	}

	if len(ddsTopic) == 0 || len(ddsTopic) > maxTopicLen {
		return nil, fmt.Errorf("dds: encode topic length %d: %w", len(ddsTopic), ErrInvalidFrame)
	}

	if len(typeName) > maxTypeLen {
		return nil, fmt.Errorf("dds: encode type length %d: %w", len(typeName), ErrInvalidFrame)
	}

	if uint64(userLen) > uint64(maxPayloadLen) {
		return nil, fmt.Errorf("dds: encode user length %d above cap %d: %w", userLen, maxPayloadLen, ErrInvalidFrame)
	}

	if len(envelope) < types.HeaderSize {
		return nil, fmt.Errorf("dds: encode envelope length %d below header %d: %w", len(envelope), types.HeaderSize, ErrTruncated)
	}

	total := frameFixedSize + len(ddsTopic) + len(typeName) + len(envelope)
	out := make([]byte, 0, total)
	out = append(out, guid[:]...)

	var seqBuf [seqSize]byte

	binary.LittleEndian.PutUint64(seqBuf[:], seq)
	out = append(out, seqBuf[:]...)

	var lenBuf [lenSize]byte

	binary.LittleEndian.PutUint32(lenBuf[:], uint32(len(ddsTopic))) //nolint:gosec // G115: length checked against maxTopicLen above
	out = append(out, lenBuf[:]...)

	binary.LittleEndian.PutUint32(lenBuf[:], uint32(len(typeName))) //nolint:gosec // G115: length checked against maxTypeLen above
	out = append(out, lenBuf[:]...)

	binary.LittleEndian.PutUint32(lenBuf[:], userLen)
	out = append(out, lenBuf[:]...)

	out = append(out, ddsTopic...)
	out = append(out, typeName...)
	out = append(out, envelope...)

	return out, nil
}

// decodeFrame parses one DATA frame without panicking on short input.
//
// It validates the fixed header, rejects sequence zero, bounds each length
// against its cap, and checks that topic plus type plus envelope header all
// fit before slicing. The returned envelope aliases the input frame.
func decodeFrame(frame []byte) (decodedFrame, error) {
	var empty decodedFrame

	if len(frame) < frameFixedSize {
		return empty, fmt.Errorf("dds: decode need %d have %d: %w", frameFixedSize, len(frame), ErrTruncated)
	}

	var guid WriterGUID

	copy(guid[:], frame[0:guidSize])

	seq := binary.LittleEndian.Uint64(frame[guidSize : guidSize+seqSize])
	if seq == 0 {
		return empty, fmt.Errorf("dds: decode sequence 0: %w", ErrInvalidFrame)
	}

	topicLen := binary.LittleEndian.Uint32(frame[guidSize+seqSize : guidSize+seqSize+lenSize])
	typeLen := binary.LittleEndian.Uint32(frame[guidSize+seqSize+lenSize : guidSize+seqSize+lenSize*2])
	userLen := binary.LittleEndian.Uint32(frame[guidSize+seqSize+lenSize*2 : frameFixedSize])

	if topicLen == 0 || topicLen > maxTopicLen {
		return empty, fmt.Errorf("dds: decode topic length %d: %w", topicLen, ErrInvalidFrame)
	}

	if typeLen > maxTypeLen {
		return empty, fmt.Errorf("dds: decode type length %d: %w", typeLen, ErrInvalidFrame)
	}

	if userLen > maxPayloadLen {
		return empty, fmt.Errorf("dds: decode user length %d above cap %d: %w", userLen, maxPayloadLen, ErrInvalidFrame)
	}

	need := uint64(frameFixedSize) + uint64(topicLen) + uint64(typeLen) + uint64(types.HeaderSize)
	if uint64(len(frame)) < need {
		return empty, fmt.Errorf("dds: decode truncated need %d have %d: %w", need, len(frame), ErrTruncated)
	}

	off := frameFixedSize
	topicEnd := off + int(topicLen)
	typeEnd := topicEnd + int(typeLen)

	topicBytes := frame[off:topicEnd]
	typeBytes := frame[topicEnd:typeEnd]
	envelope := frame[typeEnd:]

	if len(envelope) < types.HeaderSize {
		return empty, fmt.Errorf("dds: decode envelope need %d have %d: %w", types.HeaderSize, len(envelope), ErrTruncated)
	}

	out := decodedFrame{
		guid:     guid,
		seq:      seq,
		ddsTopic: string(topicBytes),
		typeName: string(typeBytes),
		userLen:  userLen,
		envelope: envelope,
	}

	return out, nil
}

// validateEnvelopePayload checks the envelope header plus user length.
//
// It runs types.UnmarshalMessage to validate the 4 byte header before
// parsing and returns the payload view after the header, including any zero
// pad tail. Empty user length requires at least one dummy byte starting
// with zero. Nonempty user length requires the view to hold that many bytes.
// The returned slice aliases the envelope input.
func validateEnvelopePayload(envelope []byte, userLen uint32) ([]byte, error) {
	var m types.Metrics

	payload, _, err := types.UnmarshalMessage(envelope, &m)
	if err != nil {
		return nil, fmt.Errorf("dds: envelope header: %w", err)
	}

	if userLen == 0 {
		if len(payload) < 1 {
			return nil, fmt.Errorf("dds: envelope dummy missing have %d: %w", len(payload), ErrTruncated)
		}

		if payload[0] != 0x00 {
			return nil, fmt.Errorf("dds: envelope dummy 0x%02x: %w", payload[0], ErrInvalidFrame)
		}

		return payload, nil
	}

	if uint64(len(payload)) < uint64(userLen) {
		return nil, fmt.Errorf("dds: envelope need %d have %d: %w", userLen, len(payload), ErrTruncated)
	}

	return payload, nil
}

// validateRTPSFrame runs Take path validation without sequence tracking.
//
// It reuses decodeFrame length checks, validateEnvelopePayload header plus
// length checks, and ParseTopicName demangling. Sequence values are checked
// for nonzero but ordering is left to Take. Envelope header and topic
// errors that lack a sentinel are mapped to ErrInvalidFrame so callers can
// rely on errors.Is for ErrTruncated or ErrInvalidFrame.
func validateRTPSFrame(frame []byte) error {
	decoded, err := decodeFrame(frame)
	if err != nil {
		return err
	}

	_, err = validateEnvelopePayload(decoded.envelope, decoded.userLen)
	if err != nil {
		if errors.Is(err, ErrTruncated) || errors.Is(err, ErrInvalidFrame) {
			return fmt.Errorf("dds: rtps validate envelope: %w", err)
		}

		return fmt.Errorf("dds: rtps validate envelope: %w", errors.Join(err, ErrInvalidFrame))
	}

	_, _, err = ParseTopicName(decoded.ddsTopic)
	if err != nil {
		return fmt.Errorf("dds: rtps validate topic: %w", errors.Join(err, ErrInvalidFrame))
	}

	return nil
}

// copyValidatedRTPSPayload validates one RTPS payload and returns an owned copy.
//
// It shares validateRTPSFrame checks plus copy logic used by Marshal and
// Parse paths so both directions enforce identical Take path validation
// without sequence tracking. Callers wrap errors with their marshal or
// parse operation prefix.
func copyValidatedRTPSPayload(payload []byte) ([]byte, error) {
	err := validateRTPSFrame(payload)
	if err != nil {
		return nil, err
	}

	out := make([]byte, len(payload))
	copy(out, payload)

	return out, nil
}

// MarshalRTPSPayload validates one DATA v1 frame and returns an owned copy for RTPS.
//
// The returned bytes embed verbatim as the payload of one RTPS DATA
// submessage inside one RTPS message. Validation reuses the Take path
// length checks via decodeFrame plus envelope checks plus topic parsing,
// without sequence tracking. Invalid input reports ErrTruncated or
// ErrInvalidFrame. The result is newly owned; mutating frame after return
// does not affect it and the caller may mutate the result.
func MarshalRTPSPayload(frame []byte) ([]byte, error) {
	out, err := copyValidatedRTPSPayload(frame)
	if err != nil {
		return nil, fmt.Errorf("dds: marshal rtps payload: %w", err)
	}

	return out, nil
}

// ParseRTPSPayload validates one RTPS DATA payload and returns an owned copy for Take.
//
// The input is one DATA v1 frame carried verbatim as an RTPS DATA payload.
// Validation matches MarshalRTPSPayload with the same Take path checks.
// Invalid input reports ErrTruncated or ErrInvalidFrame. The result is
// newly owned and ready for Take; mutating payload after return does not
// affect it and the caller may mutate the result.
func ParseRTPSPayload(payload []byte) ([]byte, error) {
	out, err := copyValidatedRTPSPayload(payload)
	if err != nil {
		return nil, fmt.Errorf("dds: parse rtps payload: %w", err)
	}

	return out, nil
}
