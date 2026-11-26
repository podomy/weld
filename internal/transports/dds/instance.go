// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

package dds

import (
	"errors"
	"fmt"
	"time"

	"github.com/podomy/weld/internal/transports"
)

// Instance handles plus explicit lifecycle for DDS 1.4 section 2.2.2.5.
//
// Handles derive from InstanceHandle over the ordered key bytes from
// ExtractKeyBytes, so equal keys on one type always yield equal handles.
// Register, dispose, and unregister enqueue NOT_ALIVE control samples
// with valid_data false; the take-next variants drain the control queue
// before history. Unkeyed topics share one implicit instance per topic
// plus type with a stable derived handle.

// ErrUnknownInstance reports lookup, dispose, or unregister of a handle
// with no registered instance.
var ErrUnknownInstance = errors.New("dds: unknown instance")

// instanceRecord is one tracked keyed or implicit instance.
type instanceRecord struct {
	// handle is the 16 byte instance handle.
	handle [16]byte
	// key holds the ordered key bytes, empty for implicit instances.
	key []byte
	// typeIDL is the demangled IDL name owning the key layout.
	typeIDL string
	// topic is the ROS topic the instance was first seen on.
	topic string
	// view is new until first access.
	view ViewStateKind
	// state is alive, disposed, or no writers.
	state InstanceStateKind
	// disposedCount counts dispose transitions.
	disposedCount int32
	// noWritersCount counts unregister transitions.
	noWritersCount int32
	// lastSeq is the newest per-writer DATA sequence seen.
	lastSeq uint64
	// notAliveAtLife5 is the nanos when the instance became not alive, zero when alive.
	notAliveAtLife5 int64
}

// implicitInstanceKey names the key index slot for unkeyed topics.
func implicitInstanceKey(topic, typeIDL string) string {
	return "implicit\x00" + topic + "\x00" + typeIDL
}

// keyedInstanceKey names the key index slot for one ordered key.
func keyedInstanceKey(typeIDL string, key []byte) string {
	out := make([]byte, 0, len(typeIDL)+1+len(key))
	out = append(out, typeIDL...)
	out = append(out, 0x00)
	out = append(out, key...)

	return string(out)
}

// instanceExistsLocked reports whether observeData would find an instance
// without creating one.
//
// It mirrors instanceForLocked routing without side effects so observeData
// can admit against MaxInstances before auto-creation per DDS 1.4 section
// 2.2.3.19. The caller must hold st.mu.
func (st *readerState) instanceExistsLocked(topic, typeIDL string, guid WriterGUID, payload []byte) bool {
	if writerGUIDKeyed(guid) {
		if rec, ok := st.keyedInstanceExistsLocked(typeIDL, payload); ok {
			_ = rec
			return true
		}
		// Fall through to implicit when keyed lookup fails, matching
		// instanceForLocked fallback.
	}
	slot := implicitInstanceKey(topic, typeIDL)
	if handle, ok := st.keyIndex[slot]; ok {
		if _, ok := st.instances[handle]; ok {
			return true
		}
	}
	return false
}

// keyedInstanceExistsLocked reports whether one keyed instance exists.
//
// It returns false when the type is unregistered or the key is malformed
// so the caller falls back to the implicit instance. The caller must hold
// st.mu.
func (st *readerState) keyedInstanceExistsLocked(typeIDL string, payload []byte) (*instanceRecord, bool) {
	desc, err := lookupDescriptorForIDL(typeIDL)
	if err != nil {
		return nil, false
	}
	key, err := ExtractKeyBytes(payload, desc)
	if err != nil {
		return nil, false
	}
	handle := InstanceHandle(typeIDL, key)
	if rec, ok := st.instances[handle]; ok {
		return rec, true
	}
	return nil, false
}

// instanceForLocked resolves the record for one DATA sample.
//
// Keyed writers (kind byte 0x02 or 0x07 per writerGUIDKeyed) hash the
// ordered key bytes; all other writers share the implicit instance for
// the topic plus type. Callers that auto-create must admit against
// MaxInstances first via admitLife5InstanceLocked; observeData does so via
// instanceExistsLocked. The caller must hold st.mu.
func (st *readerState) instanceForLocked(topic, typeIDL string, guid WriterGUID, payload []byte) *instanceRecord {
	if writerGUIDKeyed(guid) {
		if rec, ok := st.keyedInstanceLocked(typeIDL, payload); ok {
			rec.topic = topic

			return rec
		}
	}

	return st.implicitInstanceLocked(topic, typeIDL)
}

// keyedInstanceLocked resolves the record for one keyed payload.
//
// It reports false when the type is unregistered or the key is malformed
// so the caller falls back to the implicit instance. The caller must hold
// st.mu.
func (st *readerState) keyedInstanceLocked(typeIDL string, payload []byte) (*instanceRecord, bool) {
	desc, err := lookupDescriptorForIDL(typeIDL)
	if err != nil {
		return nil, false
	}

	key, err := ExtractKeyBytes(payload, desc)
	if err != nil {
		return nil, false
	}

	handle := InstanceHandle(typeIDL, key)
	if rec, ok := st.instances[handle]; ok {
		return rec, true
	}

	rec := &instanceRecord{
		handle:  handle,
		key:     append([]byte(nil), key...),
		typeIDL: typeIDL,
		view:    ViewStateNew,
		state:   InstanceStateAlive,
	}
	st.instances[handle] = rec
	st.keyIndex[keyedInstanceKey(typeIDL, key)] = handle

	return rec, true
}

// implicitInstanceLocked resolves the shared record for unkeyed samples.
//
// The caller must hold st.mu.
func (st *readerState) implicitInstanceLocked(topic, typeIDL string) *instanceRecord {
	slot := implicitInstanceKey(topic, typeIDL)
	if handle, ok := st.keyIndex[slot]; ok {
		if rec, ok := st.instances[handle]; ok {
			return rec
		}
	}

	handle := InstanceHandle(typeIDL, nil)
	rec := &instanceRecord{
		handle:  handle,
		typeIDL: typeIDL,
		topic:   topic,
		view:    ViewStateNew,
		state:   InstanceStateAlive,
	}
	st.instances[handle] = rec
	st.keyIndex[slot] = handle

	return rec
}

// RegisterInstance registers one keyed instance and returns its handle.
//
// The key must be the extracted ordered key bytes from ExtractKeyBytes,
// not the full XCDR payload: handles derive from InstanceHandle over the
// extracted bytes, so only extracted keys match the handle the DATA path
// assigns the same instance. Use InstanceHandleForPayload to derive the
// handle from a payload when no registration is needed.
// Re-registering a live instance is idempotent; re-registering a disposed
// or unregistered instance revives it to alive with view reset to new.
// Generation counts increment on the revival per DDS 1.4 section 2.2.2.5.1.
// Unkeyed topics need no registration: use the implicit handle from
// LookupInstance with an empty key.
func (r *DataReceiver) RegisterInstance(topic, typeIDL string, key []byte) ([16]byte, error) {
	var empty [16]byte

	if typeIDL == "" {
		return empty, fmt.Errorf("dds: register instance without type: %w", ErrUnknownInstance)
	}

	st := r.readerStateFor()
	st.mu.Lock()
	defer st.mu.Unlock()

	if len(key) == 0 {
		slot := implicitInstanceKey(topic, typeIDL)
		if _, ok := st.keyIndex[slot]; !ok {
			if err := st.admitLife5InstanceLocked(); err != nil {
				return empty, err
			}
		}
		rec := st.implicitInstanceLocked(topic, typeIDL)
		reviveLife5InstanceLocked(rec)

		return rec.handle, nil
	}

	handle := InstanceHandle(typeIDL, key)
	if _, ok := st.instances[handle]; !ok {
		if err := st.admitLife5InstanceLocked(); err != nil {
			return empty, err
		}
	}
	if rec, ok := st.instances[handle]; ok {
		reviveLife5InstanceLocked(rec)
		rec.topic = topic

		return handle, nil
	}

	cp := append([]byte(nil), key...)
	rec := &instanceRecord{
		handle:  handle,
		key:     cp,
		typeIDL: typeIDL,
		topic:   topic,
		view:    ViewStateNew,
		state:   InstanceStateAlive,
	}
	st.instances[handle] = rec
	st.keyIndex[keyedInstanceKey(typeIDL, key)] = handle

	return handle, nil
}

// admitLife5InstanceLocked checks MaxInstances admission without creating.
//
// The caller must hold st.mu.
func (st *readerState) admitLife5InstanceLocked() error {
	if st.limits.MaxInstances >= 0 && int64(len(st.instances)) >= st.limits.MaxInstances {
		return fmt.Errorf("dds: register instance max instances %d reached: %w", st.limits.MaxInstances, ErrResourceExhausted)
	}

	return nil
}

// reviveLife5InstanceLocked revives a not-alive instance to alive.
//
// Generation counts increment on the not-alive to alive revival per DDS 1.4
// section 2.2.2.5.1 (disposed count for disposed revivals, no-writers count
// for no-writers revivals): revive only resets view to new with the
// autopurge stamp cleared. Live instances stay untouched. The caller
// must hold st.mu.
func reviveLife5InstanceLocked(rec *instanceRecord) {
	if rec.state == InstanceStateAlive {
		return
	}

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

// LookupInstance returns the handle for one ordered key.
//
// The key must be the extracted ordered key bytes from ExtractKeyBytes,
// matching RegisterInstance, so DATA-path instances resolve to the same
// handle. It reports false for unknown keys, matching DDS HANDLE_NIL
// semantics. An empty key looks up the implicit unkeyed instance, reporting
// false until a sample or registration creates it.
func (r *DataReceiver) LookupInstance(typeIDL string, key []byte) ([16]byte, bool) {
	var empty [16]byte

	st := r.readerStateFor()
	st.mu.Lock()
	defer st.mu.Unlock()

	if len(key) == 0 {
		for _, rec := range st.instances {
			if rec.typeIDL == typeIDL && len(rec.key) == 0 {
				return rec.handle, true
			}
		}

		return empty, false
	}

	handle := InstanceHandle(typeIDL, key)
	if _, ok := st.instances[handle]; ok {
		return handle, true
	}

	return empty, false
}

// GetKeyValue returns a copy of the ordered key bytes for one handle.
//
// It reports false for unknown handles.
func (r *DataReceiver) GetKeyValue(handle [16]byte) ([]byte, bool) {
	st := r.readerStateFor()
	st.mu.Lock()
	defer st.mu.Unlock()

	rec, ok := st.instances[handle]
	if !ok {
		return nil, false
	}

	return append([]byte(nil), rec.key...), true
}

// InstanceStateOf returns the tracked instance state for one handle.
//
// It reports false for unknown handles.
func (r *DataReceiver) InstanceStateOf(handle [16]byte) (InstanceStateKind, bool) {
	st := r.readerStateFor()
	st.mu.Lock()
	defer st.mu.Unlock()

	rec, ok := st.instances[handle]
	if !ok {
		return InstanceStateAlive, false
	}

	return rec.state, true
}

// DisposeInstance marks one instance disposed and enqueues its control sample.
//
// The control sample carries the instance topic and type with no payload
// and valid_data false. Generation counts increment on the not-alive to
// alive revival per DDS 1.4 section 2.2.2.5.1, so the control snapshot
// carries the pre-transition counts. Repeated dispose while already
// disposed is idempotent for state but still enqueues one sample per call
// so every dispose edge is observable. Exclusive instances fail closed
// without a writer: use DisposeInstanceByWriter when ownership is
// exclusive, since the legacy handle-only path cannot prove ownership and
// refuses with ErrOwnershipRefused rather than misrouting a dispose.
func (r *DataReceiver) DisposeInstance(handle [16]byte) error {
	st := r.readerStateFor()
	st.mu.Lock()
	defer st.mu.Unlock()

	rec, ok := st.instances[handle]
	if !ok {
		return fmt.Errorf("dds: dispose instance %x: %w", handle, ErrUnknownInstance)
	}

	if !st.disposeOwnerAllowedLocked(handle, nil) {
		return fmt.Errorf("dds: dispose instance %x exclusive owner check: %w", handle, ErrOwnershipRefused)
	}

	rec.state = InstanceStateDisposed
	rec.notAliveAtLife5 = r.receiverNow()().UnixNano()
	st.enqueueControlLocked(rec, transports.IncomingMessage{
		TopicName: rec.topic,
		TypeName:  rec.typeIDL,
	})

	return nil
}

// DisposeInstanceByWriter marks one instance disposed when the writer owns it.
//
// Exclusive instances admit only the arbitrated owner; mismatched kinds,
// ineligible owners, and non-owners fail closed with ErrOwnershipRefused per
// DDS 1.4 sections 2.2.2.5.1.3 and 2.2.3.9. Shared and unconfigured instances
// admit any writer.
func (r *DataReceiver) DisposeInstanceByWriter(handle [16]byte, writer WriterGUID) error {
	st := r.readerStateFor()
	st.mu.Lock()
	defer st.mu.Unlock()

	rec, ok := st.instances[handle]
	if !ok {
		return fmt.Errorf("dds: dispose instance %x: %w", handle, ErrUnknownInstance)
	}

	if !st.disposeOwnerAllowedLocked(handle, &writer) {
		return fmt.Errorf("dds: dispose instance %x exclusive owner check: %w", handle, ErrOwnershipRefused)
	}

	rec.state = InstanceStateDisposed
	rec.notAliveAtLife5 = r.receiverNow()().UnixNano()
	st.enqueueControlLocked(rec, transports.IncomingMessage{
		TopicName: rec.topic,
		TypeName:  rec.typeIDL,
	})

	return nil
}

// disposeOwnerAllowedLocked reports whether a dispose or unregister may proceed.
//
// Shared and unconfigured instances admit any writer. Exclusive instances
// admit only the arbitrated owner among alive, deadline-clean, and
// kind-compatible writers; no eligible owner fails closed, and the legacy
// handle-only path (nil writer) always fails closed for exclusive so
// ownership cannot be bypassed, including the empty-offers case with no
// proven owner. The caller must hold st.mu; only the ownership sublock is
// taken here.
func (st *readerState) disposeOwnerAllowedLocked(handle [16]byte, writer *WriterGUID) bool {
	if st == nil || st.ownership == nil {
		return true
	}

	st.ownership.mu.Lock()
	defer st.ownership.mu.Unlock()

	kind := st.ownership.kinds[handle]
	if kind == OwnershipUnknown {
		kind = OwnershipShared
	}
	if kind != OwnershipExclusive {
		return true
	}

	claims, ok := st.ownership.offers[handle]
	if !ok || len(claims) == 0 {
		return false
	}

	winner, ok := exclusiveWinnerLocked(st.ownership, handle, claims)
	if !ok {
		return false
	}
	if writer == nil {
		return false
	}

	return *writer == winner
}

// SetAutodisposeUnregisteredInstances records the writer-lifecycle autodispose rule.
//
// When enabled, unregister also disposes the instance per DDS 1.4 section
// 2.2.3.23.3: the unregister transition lands in disposed with a disposed
// control sample instead of no-writers. Disabled by default for backward
// compat; enable for spec lifecycles.
func (r *DataReceiver) SetAutodisposeUnregisteredInstances(enabled bool) {
	st := r.readerStateFor()
	st.mu.Lock()
	defer st.mu.Unlock()

	st.autodisposeLife5 = enabled
}

// UnregisterInstance marks one instance without writers and enqueues its sample.
//
// Semantics match DisposeInstance with the no-writers state instead of
// disposed, unless autodispose is enabled via
// SetAutodisposeUnregisteredInstances, in which case the transition lands
// in disposed per DDS 1.4 section 2.2.3.23.3. Generation counts increment on
// the not-alive to alive revival, so the control snapshot carries the
// pre-transition counts. Exclusive instances fail closed without a writer
// like DisposeInstance: use UnregisterInstanceByWriter when ownership is
// exclusive.
func (r *DataReceiver) UnregisterInstance(handle [16]byte) error {
	st := r.readerStateFor()
	st.mu.Lock()
	defer st.mu.Unlock()

	rec, ok := st.instances[handle]
	if !ok {
		return fmt.Errorf("dds: unregister instance %x: %w", handle, ErrUnknownInstance)
	}

	if !st.disposeOwnerAllowedLocked(handle, nil) {
		return fmt.Errorf("dds: unregister instance %x exclusive owner check: %w", handle, ErrOwnershipRefused)
	}

	if st.autodisposeLife5 {
		rec.state = InstanceStateDisposed
	} else {
		rec.state = InstanceStateNoWriters
	}
	rec.notAliveAtLife5 = r.receiverNow()().UnixNano()
	st.enqueueControlLocked(rec, transports.IncomingMessage{
		TopicName: rec.topic,
		TypeName:  rec.typeIDL,
	})

	return nil
}

// UnregisterInstanceByWriter marks one instance without writers when the writer owns it.
//
// Ownership gating matches DisposeInstanceByWriter. When autodispose is
// enabled the transition lands in disposed per DDS 1.4 section 2.2.3.23.3.
func (r *DataReceiver) UnregisterInstanceByWriter(handle [16]byte, writer WriterGUID) error {
	st := r.readerStateFor()
	st.mu.Lock()
	defer st.mu.Unlock()

	rec, ok := st.instances[handle]
	if !ok {
		return fmt.Errorf("dds: unregister instance %x: %w", handle, ErrUnknownInstance)
	}

	if !st.disposeOwnerAllowedLocked(handle, &writer) {
		return fmt.Errorf("dds: unregister instance %x exclusive owner check: %w", handle, ErrOwnershipRefused)
	}

	if st.autodisposeLife5 {
		rec.state = InstanceStateDisposed
	} else {
		rec.state = InstanceStateNoWriters
	}
	rec.notAliveAtLife5 = r.receiverNow()().UnixNano()
	st.enqueueControlLocked(rec, transports.IncomingMessage{
		TopicName: rec.topic,
		TypeName:  rec.typeIDL,
	})

	return nil
}

// enqueueControlLocked appends one NOT_ALIVE control sample.
//
// The caller must hold st.mu. The sample carries no payload with
// valid_data false per DDS 1.4 section 2.2.2.5.8. View and generation
// counts snapshot the instance before the transition: counts increment on
// revival, so a dispose or unregister control carries the counts as of the
// transition; reading the control later flips view to not new. Ranks stay
// zero here; collectMatching fills per-collection ranks per section
// 2.2.2.5.1.6.
func (st *readerState) enqueueControlLocked(rec *instanceRecord, msg transports.IncomingMessage) {
	st.seq++

	info := SampleInfo{
		SampleState:              SampleStateNotRead,
		ViewState:                rec.view,
		InstanceState:            rec.state,
		ValidData:                false,
		SourceTimestamp:          time.Now(),
		ReceptionTimestamp:       time.Now(),
		InstanceHandle:           rec.handle,
		DisposedGenerationCount:  rec.disposedCount,
		NoWritersGenerationCount: rec.noWritersCount,
	}
	st.controls = append(st.controls, SampledMessage{Msg: msg, Info: info})
}

// PendingControls returns the count of queued NOT_ALIVE control samples.
func (r *DataReceiver) PendingControls() int {
	st := r.readerStateFor()
	st.mu.Lock()
	defer st.mu.Unlock()

	return len(st.controls)
}

// TakeControlSample dequeues one pending NOT_ALIVE control sample.
//
// It reports ErrNoSample when the queue is empty. Taking flips view to
// not new when the control belongs to the most recent generation and
// notifies once per delivery.
func (r *DataReceiver) TakeControlSample() (SampledMessage, error) {
	st := r.readerStateFor()
	st.mu.Lock()
	st.purgeLife5NotAliveLocked(r.receiverNow()().UnixNano())

	if len(st.controls) == 0 {
		st.mu.Unlock()

		return SampledMessage{}, ErrNoSample
	}

	out := st.controls[0]
	copy(st.controls, st.controls[1:])
	st.controls[len(st.controls)-1] = SampledMessage{}
	st.controls = st.controls[:len(st.controls)-1]

	if rec, ok := st.instances[out.Info.InstanceHandle]; ok {
		out.Info.ViewState = rec.view
		sampleSum := out.Info.DisposedGenerationCount + out.Info.NoWritersGenerationCount
		recSum := rec.disposedCount + rec.noWritersCount
		if sampleSum == recSum {
			rec.view = ViewStateNotNew
		}
	}

	st.mu.Unlock()

	r.NotifyData(out.Msg.TopicName, out)

	return out, nil
}
