// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

package dds

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/podomy/weld/internal/transports"
)

// DDS 1.4 sample info for every take path, section 2.2.2.5.
//
// Transport.go IncomingMessage carries topic, type, and payload only, so
// SampleInfo travels on a side channel that never alters transport.go:
// takeDecoded records one SampledMessage per validated sample in the
// receiver state, and callers read it back through TakeSampled,
// TakeFromSampled, LastSampleInfo, or the retained history. Samples that
// arrive while no state exists (zero value receivers in unit tests)
// still deliver; only the side channel stays empty.

// Sample state bits per DDS 1.4 section 2.2.2.5.2.
type SampleStateKind uint32

// Sample states plus the any mask.
const (
	// SampleStateRead marks a sample already returned by read or take.
	SampleStateRead SampleStateKind = 0x0001
	// SampleStateNotRead marks a sample never returned yet.
	SampleStateNotRead SampleStateKind = 0x0002
	// SampleStateAny matches either sample state.
	SampleStateAny SampleStateKind = 0xFFFF
)

// String renders the sample state in lowercase form.
func (s SampleStateKind) String() string {
	switch s {
	case SampleStateRead:
		return "read"
	case SampleStateNotRead:
		return "not_read"
	case SampleStateAny:
		return "any"
	default:
		return "invalid"
	}
}

// View state bits per DDS 1.4 section 2.2.2.5.3.
type ViewStateKind uint32

// View states plus the any mask.
const (
	// ViewStateNew marks an instance never seen by this reader.
	ViewStateNew ViewStateKind = 0x0001
	// ViewStateNotNew marks an instance already accessed.
	ViewStateNotNew ViewStateKind = 0x0002
	// ViewStateAny matches either view state.
	ViewStateAny ViewStateKind = 0xFFFF
)

// String renders the view state in lowercase form.
func (v ViewStateKind) String() string {
	switch v {
	case ViewStateNew:
		return "new"
	case ViewStateNotNew:
		return "not_new"
	case ViewStateAny:
		return "any"
	default:
		return "invalid"
	}
}

// Instance state bits per DDS 1.4 section 2.2.2.5.4.
type InstanceStateKind uint32

// Instance states plus the any mask.
const (
	// InstanceStateAlive marks an instance with a live writer.
	InstanceStateAlive InstanceStateKind = 0x0001
	// InstanceStateDisposed marks an instance removed by dispose.
	InstanceStateDisposed InstanceStateKind = 0x0002
	// InstanceStateNoWriters marks an instance with no live writer.
	InstanceStateNoWriters InstanceStateKind = 0x0004
	// InstanceStateAny matches any instance state.
	InstanceStateAny InstanceStateKind = 0xFFFF
	// InstanceStateNotAlive matches both not-alive states.
	InstanceStateNotAlive InstanceStateKind = 0x0006
)

// String renders the instance state in lowercase form.
func (s InstanceStateKind) String() string {
	switch s {
	case InstanceStateAlive:
		return "alive"
	case InstanceStateDisposed:
		return "disposed"
	case InstanceStateNoWriters:
		return "no_writers"
	case InstanceStateAny:
		return "any"
	case InstanceStateNotAlive:
		return "not_alive"
	default:
		return "invalid"
	}
}

// SampleInfo carries DDS 1.4 section 2.2.2.5 metadata for one sample.
//
// SourceTimestamp is the sender time when known, else the reception time:
// v1 DATA frames carry no source stamp, so both stamps coincide there and
// stay documented as reception bound. PublicationHandle holds the writer
// GUID bytes. SampleSequence is a Weld extension carrying the per-writer
// DATA sequence, documented as such.
type SampleInfo struct {
	// SampleState is read once returned by read or take.
	SampleState SampleStateKind
	// ViewState is new until the instance is first accessed.
	ViewState ViewStateKind
	// InstanceState is alive, disposed, or no writers.
	InstanceState InstanceStateKind
	// ValidData is false for lifecycle control samples with no payload.
	ValidData bool
	// SampleRank counts newer samples of the same instance in history.
	SampleRank int32
	// GenerationRank counts generations between this sample and the newest.
	GenerationRank int32
	// AbsoluteGenerationRank counts generations since instance creation.
	AbsoluteGenerationRank int32
	// SourceTimestamp is the sender time when known.
	SourceTimestamp time.Time
	// ReceptionTimestamp is the local take time.
	ReceptionTimestamp time.Time
	// InstanceHandle identifies the keyed instance, zero for unkeyed.
	InstanceHandle [16]byte
	// PublicationHandle holds the writer GUID bytes.
	PublicationHandle [16]byte
	// DisposedGenerationCount counts dispose transitions of the instance.
	DisposedGenerationCount int32
	// NoWritersGenerationCount counts unregister transitions of the instance.
	NoWritersGenerationCount int32
	// SampleSequence is the per-writer DATA sequence, zero when unknown.
	SampleSequence uint64
}

// SampledMessage pairs one taken message with its sample info.
//
// It is the extended internal struct for the take side channel: legacy
// Take keeps returning IncomingMessage alone while TakeSampled and the
// read/take-next variants return this pair.
type SampledMessage struct {
	// Msg is the taken payload with topic and type.
	Msg transports.IncomingMessage
	// Info is the DDS sample info for the message.
	Info SampleInfo
}

// ErrSampleInfoMissing reports a take with no recorded side-channel info.
var ErrSampleInfoMissing = errors.New("dds: take sampled without recorded info")

// sampleHistoryCap bounds retained samples per receiver.
const sampleHistoryCap = 1024

// storedSample is one retained history entry with its read flag.
type storedSample struct {
	// sample is the message plus info as taken.
	sample SampledMessage
	// read is true once returned by read or the legacy take path.
	read bool
}

// readerState bundles the DDS 1.4 API-surface state for one receiver.
//
// It hangs off DataReceiver as a single additive field so existing take
// paths keep their behavior: recording only appends history and updates
// instance generations. All methods self-lock and are safe for concurrent
// use. A nil receiver state never occurs in practice; methods guard
// against it so zero value receivers in tests still deliver.
type readerState struct {
	mu                     sync.Mutex
	seq                    uint64
	history                []*storedSample
	instances              map[[16]byte]*instanceRecord
	keyIndex               map[string][16]byte
	controls               []SampledMessage
	ownership              *ownershipState
	timeFilter             *timeFilterState
	limits                 ResourceLimits
	violations             uint64
	participant            *participantState
	offeredMiss            *deadlineMissMonitor
	requestedMiss          *deadlineMissMonitor
	lost                   *livelinessLostMonitor
	changed                *livelinessChangedMonitor
	listeners              *listenerRegistry
	reliableLife5          bool
	keepAllLife5           bool
	historyDepthLife5      int64
	autopurgeLife5NoWriter int64
	autopurgeLife5Disposed int64
	destinationOrderLife5  DestinationOrderKind
	autodisposeLife5       bool
}

// newReaderState returns an empty reader state with defaults.
func newReaderState() *readerState {
	return &readerState{
		instances:             make(map[[16]byte]*instanceRecord),
		keyIndex:              make(map[string][16]byte),
		ownership:             newOwnershipState(),
		timeFilter:            newTimeFilterState(0),
		limits:                UnlimitedResources(),
		participant:           newParticipantState(),
		offeredMiss:           newDeadlineMissMonitor(),
		requestedMiss:         newDeadlineMissMonitor(),
		lost:                  newLivelinessLostMonitor(),
		changed:               newLivelinessChangedMonitor(),
		listeners:             newListenerRegistry(),
		historyDepthLife5:     -1,
		destinationOrderLife5: DestinationOrderByReception,
	}
}

// readerStateFor returns the receiver state, creating it on first use.
//
// Lazy creation keeps zero value receivers usable: datapath tests build
// DataReceiver values without the constructor. Callers must not hold the
// receiver lock across this call; it takes no receiver locks itself.
func (r *DataReceiver) readerStateFor() *readerState {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.sample == nil {
		r.sample = newReaderState()
	}

	return r.sample
}

// observeData records one validated DATA sample and reports its info.
//
// The caller supplies the reception time so fake clocks pin stamps
// without touching wall clock. Unkeyed topics share one implicit
// instance per topic plus type; keyed topics resolve the descriptor and
// hash the ordered key bytes into the instance handle. Unknown keyed
// types fall back to the implicit instance instead of dropping: the take
// hook already gated delivery, so info must never fail a delivered
// sample. Arrival never flips view: the info snapshots the live view and
// only a delivered take or collect flips it on access, so a filtered or
// dropped take leaves the instance unviewed. Rebirth on arrival resets
// view to new with the matching generation increment. Generations come
// from the instance counters. The returned info carries the pre-take
// state while storage is marked read, so take-plus-info pairing is
// atomic under one lock with no separate lookup.
func (r *DataReceiver) observeData(topic, typeIDL string, guid WriterGUID, seq uint64, payload []byte, at time.Time) (SampleInfo, error) {
	st := r.readerStateFor()

	return st.observeData(topic, typeIDL, guid, seq, payload, at)
}

// observeData records one sample in the state bundle.
//
// Reliable keep-all blocks instead of evicting when limits would be
// exceeded; best-effort evicts oldest-first. It reports resource exhaustion
// joining ErrResourceExhausted. New instances admit against MaxInstances
// before creation per DDS 1.4 section 2.2.3.19; exhaustion fails closed
// without creating the instance.
func (st *readerState) observeData(topic, typeIDL string, guid WriterGUID, seq uint64, payload []byte, at time.Time) (SampleInfo, error) {
	st.mu.Lock()
	defer st.mu.Unlock()

	var empty SampleInfo

	st.purgeLife5NotAliveLocked(at.UnixNano())

	if !st.instanceExistsLocked(topic, typeIDL, guid, payload) {
		if err := st.admitLife5InstanceLocked(); err != nil {
			st.violations++
			return empty, err
		}
	}

	rec := st.instanceForLocked(topic, typeIDL, guid, payload)

	if rec.state != InstanceStateAlive {
		if rec.state == InstanceStateDisposed {
			rec.disposedCount++
		}
		if rec.state == InstanceStateNoWriters {
			rec.noWritersCount++
		}
		rec.state = InstanceStateAlive
		rec.view = ViewStateNew
		rec.notAliveAtLife5 = 0
	}

	if st.blockingLife5ExceededLocked(rec.handle) {
		st.violations++

		return empty, fmt.Errorf("dds: reliable keep-all resource exhausted: %w", ErrResourceExhausted)
	}

	st.seq++

	info := SampleInfo{
		SampleState:        SampleStateNotRead,
		ValidData:          true,
		SourceTimestamp:    at,
		ReceptionTimestamp: at,
		SampleSequence:     seq,
	}
	copy(info.PublicationHandle[:], guid[:])

	info.ViewState = rec.view
	info.InstanceState = rec.state
	info.InstanceHandle = rec.handle
	info.DisposedGenerationCount = rec.disposedCount
	info.NoWritersGenerationCount = rec.noWritersCount

	// View flips only on delivered access, never on arrival: the immediate
	// take path flips after gating passes and the collect path flips on
	// read or take, both for most-recent generations only. A filtered take
	// therefore leaves the instance unviewed.
	stored := info
	stored.SampleState = SampleStateRead
	sample := SampledMessage{
		Msg: transports.IncomingMessage{
			TopicName: topic,
			TypeName:  typeIDL,
			Payload:   append([]byte(nil), payload...),
		},
		Info: stored,
	}

	st.appendHistoryLocked(sample, true)
	st.enforceSampleLimitLocked()
	st.enforcePerInstanceLimitLocked()
	st.enforceHistoryDepthLocked()

	rec.lastSeq = seq

	return info, nil
}

// assignRanksLocked fills per-collection ranks per DDS 1.4 section 2.2.2.5.1.6.
//
// SampleRank counts same-instance samples following the entry in out.
// GenerationRank is the MRSIC generation sum minus the entry sum, where
// MRSIC is the most recent same-instance sample in out. Absolute rank is
// the middleware generation sum minus the entry sum, using the live
// instance record when present else the MRSIC sum. The caller must hold
// st.mu.
func (st *readerState) assignRanksLocked(out []SampledMessage) {
	lastPos := make(map[[16]byte]int, len(out))
	for i, sample := range out {
		lastPos[sample.Info.InstanceHandle] = i
	}

	for i := range out {
		handle := out[i].Info.InstanceHandle

		var following int32
		for j := i + 1; j < len(out); j++ {
			if out[j].Info.InstanceHandle == handle {
				following++
			}
		}

		out[i].Info.SampleRank = following

		mrsic := out[lastPos[handle]].Info
		entrySum := out[i].Info.DisposedGenerationCount + out[i].Info.NoWritersGenerationCount
		mrsicSum := mrsic.DisposedGenerationCount + mrsic.NoWritersGenerationCount
		out[i].Info.GenerationRank = mrsicSum - entrySum

		mrsSum := mrsicSum
		if rec, ok := st.instances[handle]; ok {
			mrsSum = rec.disposedCount + rec.noWritersCount
		}

		out[i].Info.AbsoluteGenerationRank = mrsSum - entrySum
	}
}

// enforcePerInstanceLimitLocked evicts oldest history of over-bound instances.
//
// Unlimited bounds never evict. Reliable keep-all never evicts; it blocks on
// append instead. The caller must hold st.mu.
func (st *readerState) enforcePerInstanceLimitLocked() {
	if st.reliableLife5 && st.keepAllLife5 {
		return
	}
	bound := st.limits.MaxSamplesPerInstance
	if bound < 0 {
		return
	}

	totals := make(map[[16]byte]int64, len(st.history))
	for _, entry := range st.history {
		totals[entry.sample.Info.InstanceHandle]++
	}

	skip := make(map[[16]byte]int64, len(totals))
	for handle, depth := range totals {
		if depth > bound {
			skip[handle] = depth - bound
		}
	}
	if len(skip) == 0 {
		return
	}

	kept := st.history[:0]
	for _, entry := range st.history {
		handle := entry.sample.Info.InstanceHandle
		if skip[handle] > 0 {
			skip[handle]--
			st.violations++

			continue
		}
		kept = append(kept, entry)
	}

	for i := len(kept); i < len(st.history); i++ {
		st.history[i] = nil
	}

	st.history = kept
}

// TakeSampled takes one frame and returns the message plus sample info.
//
// It shares Take decoding, ordering, and hook enforcement exactly and
// reports the info recorded for the sample in the same critical section,
// so concurrent takes never swap infos. The info carries the pre-take
// sample state, matching DDS take semantics where the returned info
// reflects the state before the take.
func (r *DataReceiver) TakeSampled(frame []byte) (SampledMessage, error) {
	var empty SampledMessage

	decoded, err := decodeFrame(frame)
	if err != nil {
		r.countDrop()

		return empty, fmt.Errorf("dds: take sampled decode: %w", err)
	}

	payloadView, err := validateEnvelopePayload(decoded.envelope, decoded.userLen)
	if err != nil {
		r.countDrop()

		return empty, fmt.Errorf("dds: take sampled envelope: %w", err)
	}

	rosTopic, _, err := ParseTopicName(decoded.ddsTopic)
	if err != nil {
		r.countDrop()

		return empty, fmt.Errorf("dds: take sampled topic %q: %w", decoded.ddsTopic, err)
	}

	msg, info, err := r.takeDecoded(decoded, payloadView, rosTopic)
	if err != nil {
		return empty, err
	}

	return SampledMessage{Msg: msg, Info: info}, nil
}

// LastSampleInfo returns the most recently recorded sample info.
//
// It reports false when no sample was recorded yet. It is the parallel
// lookup side channel for callers that must keep the IncomingMessage
// shape, such as the Backend TakeBytes contract.
func (r *DataReceiver) LastSampleInfo() (SampleInfo, bool) {
	st := r.readerStateFor()
	st.mu.Lock()
	defer st.mu.Unlock()

	if len(st.history) == 0 {
		return SampleInfo{}, false
	}

	return st.history[len(st.history)-1].sample.Info, true
}

// SampleHistory returns a copy of the retained sample history oldest first.
func (r *DataReceiver) SampleHistory() []SampledMessage {
	st := r.readerStateFor()
	st.mu.Lock()
	defer st.mu.Unlock()

	out := make([]SampledMessage, 0, len(st.history))
	for _, entry := range st.history {
		out = append(out, entry.sample)
	}

	return out
}

// TakeFromSampled reads one RTPS payload and returns the message plus info.
//
// It shares TakeFrom decoding and recording exactly and reports the info
// recorded for the sample in the same path, so concurrent takes never
// swap infos. The info carries the pre-take sample state.
func (r *DataReceiver) TakeFromSampled(ctx context.Context, s FrameSource) (SampledMessage, error) {
	var empty SampledMessage

	msg, info, err := r.takeFromWithInfo(ctx, s)
	if err != nil {
		return empty, err
	}

	return SampledMessage{Msg: msg, Info: info}, nil
}

// instanceHandleForDrop resolves the status handle for one dropped sample.
//
// It returns the existing instance handle without creating records so
// drops never install instances; unknown samples report the zero handle
// while the miss still counts.
// lookupKeyedHandleLocked returns the existing handle for one keyed drop.
//
// It reports false for unregistered types, malformed keys, or unknown
// handles so drops never install instances. The caller must hold st.mu.
func lookupKeyedHandleLocked(st *readerState, typeIDL string, payload []byte) ([16]byte, bool) {
	desc, err := lookupDescriptorForIDL(typeIDL)
	if err != nil {
		return [16]byte{}, false
	}

	key, err := ExtractKeyBytes(payload, desc)
	if err != nil {
		return [16]byte{}, false
	}

	handle := InstanceHandle(typeIDL, key)
	if _, ok := st.instances[handle]; !ok {
		return [16]byte{}, false
	}

	return handle, true
}

func (r *DataReceiver) instanceHandleForDrop(topic, typeIDL string, guid WriterGUID, payload []byte) [16]byte {
	st := r.readerStateFor()
	st.mu.Lock()
	defer st.mu.Unlock()

	if writerGUIDKeyed(guid) {
		handle, ok := lookupKeyedHandleLocked(st, typeIDL, payload)
		if ok {
			return handle
		}

		return [16]byte{}
	}

	slot := implicitInstanceKey(topic, typeIDL)
	if handle, ok := st.keyIndex[slot]; ok {
		return handle
	}

	for _, rec := range st.instances {
		if rec.topic == topic {
			return rec.handle
		}
	}

	return [16]byte{}
}

// appendHistoryLocked retains one sample, evicting the oldest past cap.
//
// The caller must hold st.mu. The read flag marks legacy take deliveries
// as read; cache fills for the read-next path pass false.
func (st *readerState) appendHistoryLocked(sample SampledMessage, read bool) {
	st.history = append(st.history, &storedSample{sample: sample, read: read})

	for len(st.history) > sampleHistoryCap {
		copy(st.history, st.history[1:])
		st.history[len(st.history)-1] = nil
		st.history = st.history[:len(st.history)-1]
	}
}

// enforceSampleLimitLocked evicts oldest history past the resource limit.
//
// The caller must hold st.mu. Unlimited caps never evict; reliable keep-all
// never evicts and blocks on append instead; violations count each eviction
// for dashboards.
func (st *readerState) enforceSampleLimitLocked() {
	if st.reliableLife5 && st.keepAllLife5 {
		return
	}
	bound := st.limits.MaxSamples
	if bound < 0 {
		return
	}

	for int64(len(st.history)) > bound {
		copy(st.history, st.history[1:])
		st.history[len(st.history)-1] = nil
		st.history = st.history[:len(st.history)-1]
		st.violations++
	}
}

// ResourceViolations returns the count of resource-limit evictions.
func (r *DataReceiver) ResourceViolations() uint64 {
	st := r.readerStateFor()
	st.mu.Lock()
	defer st.mu.Unlock()

	return st.violations
}

// SetReliabilityHistoryLife5 records reliability plus history kinds for blocking.
//
// Reliable keep-all blocks instead of evicting when limits would be
// exceeded; all other combinations evict oldest-first. It is additive and
// never changes delivery when limits are unlimited.
func (r *DataReceiver) SetReliabilityHistoryLife5(reliable, keepAll bool) {
	st := r.readerStateFor()
	st.mu.Lock()
	defer st.mu.Unlock()

	st.reliableLife5 = reliable
	st.keepAllLife5 = keepAll
}

// SetAutopurgeLife5Delays records reader autopurge delays in nanoseconds.
//
// Non-positive disables the corresponding purge. Purge runs lazily on read
// and take paths using the receiver clock so fake clocks pin expiry.
func (r *DataReceiver) SetAutopurgeLife5Delays(noWriterNanos, disposedNanos int64) {
	st := r.readerStateFor()
	st.mu.Lock()
	defer st.mu.Unlock()

	st.autopurgeLife5NoWriter = noWriterNanos
	st.autopurgeLife5Disposed = disposedNanos
}

// purgeLife5NotAliveLocked removes not-alive instances past delay when empty.
//
// Only instances with no retained history samples purge; controls for the
// purged handle drop with the instance. The caller must hold st.mu.
// purgeLife5DelayLocked returns the autopurge delay for one record.
//
// It returns non-positive when purging is disabled for the state.
func (st *readerState) purgeLife5DelayLocked(rec *instanceRecord) int64 {
	if rec.state == InstanceStateNoWriters {
		return st.autopurgeLife5NoWriter
	}

	return st.autopurgeLife5Disposed
}

// purgeLife5ShouldDropLocked reports whether one handle may purge now.
//
// Only not-alive instances past delay with empty history purge.
func (st *readerState) purgeLife5ShouldDropLocked(handle [16]byte, rec *instanceRecord, nowNanos int64) bool {
	if rec.state == InstanceStateAlive {
		return false
	}

	if rec.notAliveAtLife5 == 0 {
		return false
	}

	delay := st.purgeLife5DelayLocked(rec)
	if delay <= 0 {
		return false
	}

	if nowNanos-rec.notAliveAtLife5 < delay {
		return false
	}

	for _, entry := range st.history {
		if entry.sample.Info.InstanceHandle == handle {
			return false
		}
	}

	return true
}

// purgeLife5DeleteLocked removes one instance and its controls.
func (st *readerState) purgeLife5DeleteLocked(handle [16]byte) {
	delete(st.instances, handle)
	for slot, h := range st.keyIndex {
		if h == handle {
			delete(st.keyIndex, slot)
		}
	}

	kept := st.controls[:0]
	for _, control := range st.controls {
		if control.Info.InstanceHandle != handle {
			kept = append(kept, control)
		}
	}

	for i := len(kept); i < len(st.controls); i++ {
		st.controls[i] = SampledMessage{}
	}

	st.controls = kept
}

func (st *readerState) purgeLife5NotAliveLocked(nowNanos int64) {
	if st.autopurgeLife5NoWriter <= 0 && st.autopurgeLife5Disposed <= 0 {
		return
	}

	for handle, rec := range st.instances {
		if !st.purgeLife5ShouldDropLocked(handle, rec, nowNanos) {
			continue
		}

		st.purgeLife5DeleteLocked(handle)
	}
}

// blockingLife5ExceededLocked reports whether appending would exceed limits
// under reliable keep-all blocking.
//
// Unlimited bounds never block. Best-effort never blocks; it evicts. Only
// reliable keep-all blocks to preserve existing samples. MaxInstances
// blocks when tracked instances exceed the bound (for example after the
// bound was lowered); new-instance admission itself fails closed in
// observeData via admitLife5InstanceLocked before creation.
func (st *readerState) blockingLife5ExceededLocked(handle [16]byte) bool {
	if !st.reliableLife5 || !st.keepAllLife5 {
		return false
	}

	if st.limits.MaxSamples >= 0 && int64(len(st.history)) >= st.limits.MaxSamples {
		return true
	}

	if st.limits.MaxInstances >= 0 && int64(len(st.instances)) > st.limits.MaxInstances {
		return true
	}

	if st.limits.MaxSamplesPerInstance >= 0 {
		var depth int64
		for _, entry := range st.history {
			if entry.sample.Info.InstanceHandle == handle {
				depth++
			}
		}

		if depth >= st.limits.MaxSamplesPerInstance {
			return true
		}
	}

	return false
}

// SweepAgedInstances removes not-alive instances past their autopurge delay.
//
// It mirrors purgeLife5NotAliveLocked but runs on demand with an explicit
// timestamp so a background sweep loop can drive time-based expiry without
// waiting for the next read or take. Only instances with no retained
// history samples purge; controls for purged handles drop with the
// instance. It returns the purged handle count for loud accounting.
// TODO backend agent - drive SweepAgedInstances from the sweepLoop owner on
// a timer (for example once per second with receiverNow) instead of only on
// observe/collect paths; see purgeLife5NotAliveLocked for the lazy path.
func (st *readerState) SweepAgedInstances(nowNanos int64) int {
	purged := 0
	for handle, rec := range st.instances {
		if !st.purgeLife5ShouldDropLocked(handle, rec, nowNanos) {
			continue
		}
		st.purgeLife5DeleteLocked(handle)
		purged++
	}
	return purged
}

// SweepAgedInstances removes not-alive instances past delay on the receiver.
//
// It takes the state lock, delegates to the locked helper, and reports the
// purged count. Backend sweep wiring should call this on a timer.
func (r *DataReceiver) SweepAgedInstances(nowNanos int64) int {
	st := r.readerStateFor()
	st.mu.Lock()
	defer st.mu.Unlock()
	return st.SweepAgedInstances(nowNanos)
}
