// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

// Per-profile writer QoS translation for the CycloneDDS SEDP path.
//
// Weld always announced reliable plus transient local depth 100 for every
// topic endpoint while services carved out volatile readers, so exotic
// requested profiles misbehaved natively: a best effort writer still
// announced reliable and a volatile writer still announced transient local.
// This file translates a resolved best available core profile to the exact
// SEDP-announced DDS QoS (reliability kind plus blocking, durability kind,
// history kind plus depth, deadline, lifespan, liveliness plus lease) and
// back, following the internal/transports/fastdds ForwardQoS table shape.
//
// Nothing narrows silently: a setting with no exact wire encoding reports a
// typed *WireQoSRefusal naming topic, endpoint pair, and policy kind, never
// a quiet downgrade. The compatibility gate reuses core.CheckCompatibility
// on the translated values so translated offers match identically to facade
// offers for all standard profiles.
package dds

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/podomy/weld/internal/core"
	"github.com/podomy/weld/internal/transports/dds/rtps"
)

// Writer QoS PID values not present in rtps/pid.go.
//
// The DDSI-RTPS parameter table assigns deadline 0x0023, lifespan 0x002b,
// and liveliness 0x001b; reliability 0x001a, durability 0x001d, and history
// 0x0040 reuse the existing rtps constants. The values below match the
// Wireshark packet-rtps.c dissector PID table.
const (
	// writerQoSPIDDeadline carries the deadline period.
	writerQoSPIDDeadline uint16 = 0x0023
	// writerQoSPIDLifespan carries the lifespan duration.
	writerQoSPIDLifespan uint16 = 0x002b
	// writerQoSPIDLiveliness carries the liveliness kind plus lease.
	writerQoSPIDLiveliness uint16 = 0x001b
)

// Writer QoS wire kind values.
//
// DDS reliability kinds are best effort 1 and reliable 2. DDS durability
// kinds are volatile 0, transient local 1, transient 2, and persistent 3.
// DDS history kinds are keep last 0 and keep all 1. DDS liveliness kinds
// are automatic 0, manual by participant 1, and manual by topic 2. Core
// models only automatic plus manual by topic; transient, persistent, and
// manual by participant have no core counterpart and reverse to unknown.
const (
	// writerQoSReliabilityBestEffort is the DDS best effort kind.
	writerQoSReliabilityBestEffort uint32 = 1
	// writerQoSDurabilityTransient is the wire only transient kind.
	writerQoSDurabilityTransient uint32 = 2
	// writerQoSDurabilityPersistent is the wire only persistent kind.
	writerQoSDurabilityPersistent uint32 = 3
	// writerQoSHistoryKeepAll is the DDS keep all kind.
	writerQoSHistoryKeepAll uint32 = 1
	// writerQoSLivelinessAutomatic is the DDS automatic kind.
	writerQoSLivelinessAutomatic uint32 = 0
	// writerQoSLivelinessManualByParticipant is the wire only participant kind.
	writerQoSLivelinessManualByParticipant uint32 = 1
	// writerQoSLivelinessManualByTopic is the DDS manual by topic kind.
	writerQoSLivelinessManualByTopic uint32 = 2
)

// Writer QoS duration bounds for the DDS Duration_t range.
const (
	// writerQoSInfiniteSeconds encodes infinite seconds.
	writerQoSInfiniteSeconds = int32(0x7FFFFFFF)
	// writerQoSInfiniteFraction encodes infinite fractional nanoseconds.
	writerQoSInfiniteFraction = uint32(0xFFFFFFFF)
	// writerQoSMaxFiniteSeconds is the largest exactly encodable finite second count.
	writerQoSMaxFiniteSeconds = int32(0x7FFFFFFE)
	// writerQoSNanosPerSecond bounds the fractional nanosecond field.
	writerQoSNanosPerSecond = uint32(1000000000)
)

// WriterPolicyKind names a writer QoS policy in loud refusals.
//
// It aliases core.PolicyKind so the five RxO policies reuse the core names
// and history plus lifespan add local names; core has no kind for either
// because neither affects matching.
type WriterPolicyKind = core.PolicyKind

// Local writer policy names for refusals outside the RxO set.
const (
	// WriterPolicyHistory marks history kind and depth refusals.
	WriterPolicyHistory WriterPolicyKind = "history"
	// WriterPolicyLifespan marks lifespan duration refusals.
	WriterPolicyLifespan WriterPolicyKind = "lifespan"
)

// WireDuration is the DDS Duration_t wire value: seconds plus fractional nanoseconds.
type WireDuration struct {
	// Seconds carries whole seconds, or infinite seconds for infinity.
	Seconds int32
	// Nanoseconds carries fractional nanoseconds, or infinite fraction for infinity.
	Nanoseconds uint32
}

// InfiniteWireDuration returns the DDS infinite duration.
func InfiniteWireDuration() WireDuration {
	return WireDuration{Seconds: writerQoSInfiniteSeconds, Nanoseconds: writerQoSInfiniteFraction}
}

// IsInfinite reports whether d is the DDS infinite encoding.
func (d WireDuration) IsInfinite() bool {
	return d.Seconds == writerQoSInfiniteSeconds && d.Nanoseconds == writerQoSInfiniteFraction
}

// IsZero reports whether d is the zero duration.
func (d WireDuration) IsZero() bool {
	return d.Seconds == 0 && d.Nanoseconds == 0
}

// encode renders d as 8 little endian bytes: seconds plus nanoseconds.
func (d WireDuration) encode() []byte {
	out := make([]byte, 0, 8)
	out = binary.LittleEndian.AppendUint32(out, uint32(d.Seconds)) //nolint:gosec // G115: intentional two's complement reinterpret for the wire
	out = binary.LittleEndian.AppendUint32(out, d.Nanoseconds)

	return out
}

// decodeWireDuration parses 8 little endian bytes into a wire duration.
func decodeWireDuration(raw []byte) (WireDuration, error) {
	if len(raw) != 8 {
		return WireDuration{}, fmt.Errorf("dds: writerqos duration length %d, want 8", len(raw))
	}

	return WireDuration{
		Seconds:     int32(binary.LittleEndian.Uint32(raw[:4])), //nolint:gosec // G115: intentional two's complement reinterpret from the wire
		Nanoseconds: binary.LittleEndian.Uint32(raw[4:8]),
	}, nil
}

// WireQoS is the SEDP-announced DDS QoS for one writer endpoint.
//
// It follows the FastDDS ForwardQoS table shape: reliability kind plus
// blocking time, durability kind only, history kind plus depth, deadline,
// lifespan, and liveliness kind plus lease. Unspecified core durations map
// to DDS infinite, matching the FastDDS forward rule; core matching treats
// an unspecified offer and an infinite offer identically, so the mapping
// preserves every RxO verdict.
type WireQoS struct {
	// ReliabilityKind is reliable 2 or best effort 1.
	ReliabilityKind uint32
	// MaxBlockingTime is infinite for reliable and zero for best effort.
	MaxBlockingTime WireDuration
	// DurabilityKind is volatile 0 or transient local 1 on forward. Reverse
	// can also carry transient 2, persistent 3, or an unknown kind.
	DurabilityKind uint32
	// HistoryKind is keep last 0 or keep all 1.
	HistoryKind uint32
	// Depth is meaningful for keep last only. Keep all encodes zero.
	Depth uint32
	// Deadline is the freshness contract in DDS range.
	Deadline WireDuration
	// Lifespan is message expiry in DDS range.
	Lifespan WireDuration
	// LivelinessKind is automatic 0 or manual by topic 2 on forward.
	LivelinessKind uint32
	// LeaseDuration is liveliness lease in DDS range.
	LeaseDuration WireDuration
}

// WireQoSRefusal is the typed loud refusal for an untranslatable setting.
//
// It names the topic, the endpoint pair, and the policy kind so operators
// can trace the refusal to one requested setting. ForwardQoS returns it
// instead of downgrading quietly; callers test for it with IsWireQoSRefusal.
//
//nolint:errname // refusal names policy context, Error suffix would obscure loud refusal intent
type WireQoSRefusal struct {
	// Topic is the ROS topic the writer was declared for.
	Topic string
	// Publisher names the offering endpoint.
	Publisher string
	// Subscriber names the requesting endpoint.
	Subscriber string
	// Policy is the refusing policy kind.
	Policy WriterPolicyKind
	// Detail explains the untranslatable setting in lowercase form.
	Detail string
}

// Error renders the refusal with topic, endpoint pair, and policy kind.
func (e *WireQoSRefusal) Error() string {
	return fmt.Sprintf("dds: writerqos forward topic %q publisher %q subscriber %q: policy %s: %s",
		e.Topic, e.Publisher, e.Subscriber, string(e.Policy), e.Detail)
}

// IsWireQoSRefusal reports whether err is a writer QoS loud refusal.
func IsWireQoSRefusal(err error) bool {
	var refusal *WireQoSRefusal

	return errors.As(err, &refusal)
}

// refuseWriterQoSError builds the loud refusal for one untranslatable setting.
func refuseWriterQoSError(topic, publisher, subscriber string, policy WriterPolicyKind, format string, args ...any) error {
	return &WireQoSRefusal{
		Topic:      topic,
		Publisher:  publisher,
		Subscriber: subscriber,
		Policy:     policy,
		Detail:     fmt.Sprintf(format, args...),
	}
}

// forwardWireDuration maps a core duration to DDS range for one policy.
//
// Unspecified maps to DDS infinite: both mean no constraint and core
// matching treats an unspecified offer exactly like an infinite offer, so
// the mapping preserves every RxO verdict. Infinite maps to infinite.
// Best available sentinels, negative seconds, nanoseconds at or above one
// second, and finite values above the int32 wire range refuse loudly;
// saturating to infinite would narrow the offer silently.
func forwardWireDuration(topic, publisher, subscriber string, d core.Duration, policy WriterPolicyKind) (WireDuration, error) {
	if d.IsUnspecified() {
		return InfiniteWireDuration(), nil
	}

	if core.IsBestAvailableDuration(d) {
		return WireDuration{}, &WireQoSRefusal{
			Topic:      topic,
			Publisher:  publisher,
			Subscriber: subscriber,
			Policy:     policy,
			Detail:     "best available duration sentinel survived resolution; resolve at endpoint creation first",
		}
	}

	if d.IsInfinite() {
		return InfiniteWireDuration(), nil
	}

	if d.Seconds < 0 {
		return WireDuration{}, &WireQoSRefusal{
			Topic:      topic,
			Publisher:  publisher,
			Subscriber: subscriber,
			Policy:     policy,
			Detail:     fmt.Sprintf("negative seconds in %v; call Validate first", d),
		}
	}

	if d.Nanoseconds >= writerQoSNanosPerSecond {
		return WireDuration{}, &WireQoSRefusal{
			Topic:      topic,
			Publisher:  publisher,
			Subscriber: subscriber,
			Policy:     policy,
			Detail:     fmt.Sprintf("nanoseconds %d above range in %v; normalize with NewDuration first", d.Nanoseconds, d),
		}
	}

	if d.Seconds > int64(writerQoSMaxFiniteSeconds) {
		return WireDuration{}, &WireQoSRefusal{
			Topic:      topic,
			Publisher:  publisher,
			Subscriber: subscriber,
			Policy:     policy,
			Detail:     fmt.Sprintf("finite seconds %d above DDS range; refusing instead of clamping to infinite", d.Seconds),
		}
	}

	return WireDuration{Seconds: int32(d.Seconds), Nanoseconds: d.Nanoseconds}, nil
}

// forwardReliabilityKind maps reliability to wire kind plus blocking time.
func forwardReliabilityKind(topic, publisher, subscriber string, r core.ReliabilityPolicy) (uint32, WireDuration, error) {
	switch r { //nolint:exhaustive // default refuses unresolved markers with a typed error
	case core.ReliabilityReliable:
		return sedpReliabilityReliable, WireDuration{Seconds: int32(sedpReliabilityInfiniteSec), Nanoseconds: sedpReliabilityInfiniteFrac}, nil
	case core.ReliabilityBestEffort:
		return writerQoSReliabilityBestEffort, WireDuration{}, nil
	default:
		err := refuseWriterQoSError(topic, publisher, subscriber, core.PolicyReliability,
			"reliability %v needs resolution before forward translation", r)
		return 0, WireDuration{}, err
	}
}

// forwardDurabilityKind maps durability to the wire kind.
func forwardDurabilityKind(topic, publisher, subscriber string, d core.DurabilityPolicy) (uint32, error) {
	switch d { //nolint:exhaustive // default refuses unresolved markers with a typed error
	case core.DurabilityVolatile:
		return sedpDurabilityVolatile, nil
	case core.DurabilityTransientLocal:
		return sedpDurabilityTransientLocal, nil
	default:
		err := refuseWriterQoSError(topic, publisher, subscriber, core.PolicyDurability,
			"durability %v needs resolution before forward translation", d)
		return 0, err
	}
}

// forwardHistoryKind maps history plus depth to wire kind plus depth.
func forwardHistoryKind(topic, publisher, subscriber string, h core.HistoryPolicy, depth uint64) (uint32, uint32, error) {
	switch h { //nolint:exhaustive // default refuses unresolved markers with a typed error
	case core.HistoryKeepLast:
		if depth == 0 {
			err := refuseWriterQoSError(topic, publisher, subscriber, WriterPolicyHistory,
				"keep last depth 0 means not set; call ResolveSystemDefaults first")
			return 0, 0, err
		}

		if depth > uint64(^uint32(0)) {
			err := refuseWriterQoSError(topic, publisher, subscriber, WriterPolicyHistory,
				"keep last depth %d above DDS range; refusing instead of narrowing", depth)
			return 0, 0, err
		}

		return sedpHistoryKeepLast, uint32(depth), nil
	case core.HistoryKeepAll:
		return writerQoSHistoryKeepAll, 0, nil
	default:
		err := refuseWriterQoSError(topic, publisher, subscriber, WriterPolicyHistory,
			"history %v needs resolution before forward translation", h)
		return 0, 0, err
	}
}

// forwardLivelinessKind maps liveliness to the wire kind.
func forwardLivelinessKind(topic, publisher, subscriber string, l core.LivelinessPolicy) (uint32, error) {
	switch l { //nolint:exhaustive // default refuses unresolved markers with a typed error
	case core.LivelinessAutomatic:
		return writerQoSLivelinessAutomatic, nil
	case core.LivelinessManualByTopic:
		return writerQoSLivelinessManualByTopic, nil
	default:
		err := refuseWriterQoSError(topic, publisher, subscriber, core.PolicyLiveliness,
			"liveliness %v needs resolution before forward translation", l)
		return 0, err
	}
}

// forwardLifeDurations maps deadline, lifespan, and lease to DDS range.
func forwardLifeDurations(topic, publisher, subscriber string, p core.QoSProfile) (WireDuration, WireDuration, WireDuration, error) {
	deadline, err := forwardWireDuration(topic, publisher, subscriber, p.Deadline, core.PolicyDeadline)
	if err != nil {
		return WireDuration{}, WireDuration{}, WireDuration{}, err
	}

	lifespan, err := forwardWireDuration(topic, publisher, subscriber, p.Lifespan, WriterPolicyLifespan)
	if err != nil {
		return WireDuration{}, WireDuration{}, WireDuration{}, err
	}

	lease, err := forwardWireDuration(topic, publisher, subscriber, p.LeaseDuration, core.PolicyLeaseDuration)
	if err != nil {
		return WireDuration{}, WireDuration{}, WireDuration{}, err
	}

	return deadline, lifespan, lease, nil
}

// ForwardQoS translates a resolved core profile to the SEDP-announced wire QoS.
//
// The input must carry concrete history, reliability, durability, and
// liveliness: system default, best available, unknown, and invalid values
// refuse loudly with the policy kind named. Keep last with depth zero
// refuses; keep last depth above the uint32 wire range refuses; keep all
// encodes depth zero because depth is ignored for keep all. Reliable maps
// to infinite blocking and best effort to zero blocking. Unspecified
// deadline, lifespan, and lease map to DDS infinite, matching the FastDDS
// forward rule. Topic plus publisher plus subscriber scope every refusal so
// the loud event carries the full endpoint pair.
func ForwardQoS(topic, publisher, subscriber string, p core.QoSProfile) (WireQoS, error) {
	var out WireQoS

	reliabilityKind, blocking, err := forwardReliabilityKind(topic, publisher, subscriber, p.Reliability)
	if err != nil {
		return WireQoS{}, err
	}

	out.ReliabilityKind = reliabilityKind
	out.MaxBlockingTime = blocking

	durabilityKind, err := forwardDurabilityKind(topic, publisher, subscriber, p.Durability)
	if err != nil {
		return WireQoS{}, err
	}

	out.DurabilityKind = durabilityKind

	historyKind, depth, err := forwardHistoryKind(topic, publisher, subscriber, p.History, p.Depth)
	if err != nil {
		return WireQoS{}, err
	}

	out.HistoryKind = historyKind
	out.Depth = depth

	livelinessKind, err := forwardLivelinessKind(topic, publisher, subscriber, p.Liveliness)
	if err != nil {
		return WireQoS{}, err
	}

	out.LivelinessKind = livelinessKind

	deadline, lifespan, lease, err := forwardLifeDurations(topic, publisher, subscriber, p)
	if err != nil {
		return WireQoS{}, err
	}

	out.Deadline = deadline
	out.Lifespan = lifespan
	out.LeaseDuration = lease

	return out, nil
}

// encodeReliability renders the reliability PID value: kind plus blocking time.
func (q WireQoS) encodeReliability() []byte {
	out := make([]byte, 0, 12)
	out = binary.LittleEndian.AppendUint32(out, q.ReliabilityKind)
	out = append(out, q.MaxBlockingTime.encode()...)

	return out
}

// encodeHistory renders the history PID value: kind plus depth.
func (q WireQoS) encodeHistory() []byte {
	out := make([]byte, 0, 8)
	out = binary.LittleEndian.AppendUint32(out, q.HistoryKind)
	out = binary.LittleEndian.AppendUint32(out, q.Depth)

	return out
}

// encodeDurability renders the durability PID value: kind only.
func (q WireQoS) encodeDurability() []byte {
	return binary.LittleEndian.AppendUint32(nil, q.DurabilityKind)
}

// encodeLiveliness renders the liveliness PID value: kind plus lease.
func (q WireQoS) encodeLiveliness() []byte {
	out := make([]byte, 0, 12)
	out = binary.LittleEndian.AppendUint32(out, q.LivelinessKind)
	out = append(out, q.LeaseDuration.encode()...)

	return out
}

// Params renders q as SEDP inline QoS params in canonical order.
//
// Order is reliability, history, durability, then deadline, lifespan, and
// liveliness: the first three keep their existing SerializeSEDPDATA
// relative order and the new PIDs append after, so a future announce call
// site can splice this list in without reordering the old bytes.
func (q WireQoS) Params() []rtps.Param {
	return []rtps.Param{
		{ID: rtps.PIDReliability, Value: q.encodeReliability()},
		{ID: rtps.PIDHistory, Value: q.encodeHistory()},
		{ID: rtps.PIDDurability, Value: q.encodeDurability()},
		{ID: writerQoSPIDDeadline, Value: q.Deadline.encode()},
		{ID: writerQoSPIDLifespan, Value: q.Lifespan.encode()},
		{ID: writerQoSPIDLiveliness, Value: q.encodeLiveliness()},
	}
}

// parseWireReliability decodes the required reliability PID value.
func parseWireReliability(byID map[uint16][]byte) (uint32, WireDuration, error) {
	raw, seen := byID[rtps.PIDReliability]
	if !seen {
		return 0, WireDuration{}, fmt.Errorf("dds: writerqos parse misses reliability: policy %s", string(core.PolicyReliability))
	}

	if len(raw) != 12 {
		return 0, WireDuration{}, fmt.Errorf("dds: writerqos parse reliability length %d, want 12: policy %s", len(raw), string(core.PolicyReliability))
	}

	kind := binary.LittleEndian.Uint32(raw[:4])

	blocking, err := decodeWireDuration(raw[4:12])
	if err != nil {
		return 0, WireDuration{}, fmt.Errorf("dds: writerqos parse blocking time: %w: policy %s", err, string(core.PolicyReliability))
	}

	return kind, blocking, nil
}

// parseWireDurability decodes the required durability PID value.
func parseWireDurability(byID map[uint16][]byte) (uint32, error) {
	raw, seen := byID[rtps.PIDDurability]
	if !seen {
		return 0, fmt.Errorf("dds: writerqos parse misses durability: policy %s", string(core.PolicyDurability))
	}

	if len(raw) != 4 {
		return 0, fmt.Errorf("dds: writerqos parse durability length %d, want 4: policy %s", len(raw), string(core.PolicyDurability))
	}

	return binary.LittleEndian.Uint32(raw), nil
}

// parseWireHistory decodes the required history PID value.
func parseWireHistory(byID map[uint16][]byte) (uint32, uint32, error) {
	raw, seen := byID[rtps.PIDHistory]
	if !seen {
		return 0, 0, fmt.Errorf("dds: writerqos parse misses history: policy %s", string(WriterPolicyHistory))
	}

	if len(raw) != 8 {
		return 0, 0, fmt.Errorf("dds: writerqos parse history length %d, want 8: policy %s", len(raw), string(WriterPolicyHistory))
	}

	return binary.LittleEndian.Uint32(raw[:4]), binary.LittleEndian.Uint32(raw[4:8]), nil
}

// parseWireOptionalDurations decodes optional deadline, lifespan, and liveliness.
func parseWireOptionalDurations(byID map[uint16][]byte, out *WireQoS) error {
	if raw, seen := byID[writerQoSPIDDeadline]; seen {
		deadline, err := decodeWireDuration(raw)
		if err != nil {
			return fmt.Errorf("dds: writerqos parse deadline: %w: policy %s", err, string(core.PolicyDeadline))
		}

		out.Deadline = deadline
	}

	if raw, seen := byID[writerQoSPIDLifespan]; seen {
		lifespan, err := decodeWireDuration(raw)
		if err != nil {
			return fmt.Errorf("dds: writerqos parse lifespan: %w: policy %s", err, string(WriterPolicyLifespan))
		}

		out.Lifespan = lifespan
	}

	if raw, seen := byID[writerQoSPIDLiveliness]; seen {
		if len(raw) != 12 {
			return fmt.Errorf("dds: writerqos parse liveliness length %d, want 12: policy %s", len(raw), string(core.PolicyLiveliness))
		}

		out.LivelinessKind = binary.LittleEndian.Uint32(raw[:4])

		lease, err := decodeWireDuration(raw[4:12])
		if err != nil {
			return fmt.Errorf("dds: writerqos parse lease: %w: policy %s", err, string(core.PolicyLeaseDuration))
		}

		out.LeaseDuration = lease
	}

	return nil
}

// ParseWireQoS decodes observed vendor SEDP QoS params into wire values.
//
// The first occurrence of each QoS PID wins; unknown PIDs are ignored so
// vendor extensions never break parsing. Reliability, durability, and
// history are required because Weld has always announced them and matching
// without them is meaningless; absent deadline, lifespan, or liveliness
// maps to DDS infinite, which core matching treats exactly like the
// unspecified default. Malformed value lengths refuse loudly with the
// policy kind named.
func ParseWireQoS(params []rtps.Param) (WireQoS, error) {
	byID := make(map[uint16][]byte, len(params))

	for _, param := range params {
		if _, seen := byID[param.ID]; seen {
			continue
		}

		byID[param.ID] = param.Value
	}

	var out WireQoS

	kind, blocking, err := parseWireReliability(byID)
	if err != nil {
		return WireQoS{}, err
	}

	out.ReliabilityKind = kind
	out.MaxBlockingTime = blocking

	durabilityKind, err := parseWireDurability(byID)
	if err != nil {
		return WireQoS{}, err
	}

	out.DurabilityKind = durabilityKind

	historyKind, depth, err := parseWireHistory(byID)
	if err != nil {
		return WireQoS{}, err
	}

	out.HistoryKind = historyKind
	out.Depth = depth

	out.Deadline = InfiniteWireDuration()
	out.Lifespan = InfiniteWireDuration()
	out.LivelinessKind = writerQoSLivelinessAutomatic
	out.LeaseDuration = InfiniteWireDuration()

	if err := parseWireOptionalDurations(byID, &out); err != nil {
		return WireQoS{}, err
	}

	return out, nil
}

// reverseWireDuration maps a wire duration back to a core duration.
//
// DDS infinite maps to core infinite. Zero maps to unspecified through
// core NewDuration, which coincides with the default. Negative seconds and
// nanoseconds at or above one second refuse loudly with the policy named.
func reverseWireDuration(d WireDuration, policy WriterPolicyKind) (core.Duration, error) {
	if d.IsInfinite() {
		return core.InfiniteDuration(), nil
	}

	if d.Seconds < 0 {
		return core.Duration{}, fmt.Errorf("dds: writerqos reverse negative wire seconds %d: policy %s", d.Seconds, string(policy))
	}

	if d.Nanoseconds >= writerQoSNanosPerSecond {
		return core.Duration{}, fmt.Errorf("dds: writerqos reverse nanoseconds %d above range: policy %s", d.Nanoseconds, string(policy))
	}

	return core.NewDuration(int64(d.Seconds), d.Nanoseconds), nil
}

// reverseHistory maps wire history kind plus depth to core history.
func reverseHistory(q WireQoS) (core.HistoryPolicy, uint64, WriterPolicyKind, error) {
	switch q.HistoryKind {
	case sedpHistoryKeepLast:
		if q.Depth == 0 {
			return core.HistoryPolicy(0), 0, WriterPolicyHistory, errors.New("dds: writerqos reverse keep last depth 0 is not resolved")
		}

		return core.HistoryKeepLast, uint64(q.Depth), "", nil
	case writerQoSHistoryKeepAll:
		return core.HistoryKeepAll, 0, "", nil
	default:
		return core.HistoryPolicy(0), 0, WriterPolicyHistory, fmt.Errorf("dds: writerqos reverse history kind %d is unknown", q.HistoryKind)
	}
}

// reverseReliability maps wire reliability kind to core reliability.
func reverseReliability(q WireQoS) (core.ReliabilityPolicy, WriterPolicyKind, error) {
	switch q.ReliabilityKind {
	case sedpReliabilityReliable:
		return core.ReliabilityReliable, "", nil
	case writerQoSReliabilityBestEffort:
		return core.ReliabilityBestEffort, "", nil
	default:
		return core.ReliabilityPolicy(0), core.PolicyReliability, fmt.Errorf("dds: writerqos reverse reliability kind %d is unknown", q.ReliabilityKind)
	}
}

// reverseDurability maps wire durability kind to core durability.
func reverseDurability(q WireQoS) core.DurabilityPolicy {
	switch q.DurabilityKind {
	case sedpDurabilityVolatile:
		return core.DurabilityVolatile
	case sedpDurabilityTransientLocal:
		return core.DurabilityTransientLocal
	case writerQoSDurabilityTransient, writerQoSDurabilityPersistent:
		return core.DurabilityUnknown
	default:
		return core.DurabilityUnknown
	}
}

// reverseLiveliness maps wire liveliness kind to core liveliness.
func reverseLiveliness(q WireQoS) (core.LivelinessPolicy, WriterPolicyKind, error) {
	switch q.LivelinessKind {
	case writerQoSLivelinessAutomatic:
		return core.LivelinessAutomatic, "", nil
	case writerQoSLivelinessManualByTopic:
		return core.LivelinessManualByTopic, "", nil
	case writerQoSLivelinessManualByParticipant:
		return core.LivelinessUnknown, "", nil
	default:
		return core.LivelinessPolicy(0), core.PolicyLiveliness, fmt.Errorf("dds: writerqos reverse liveliness kind %d is unknown", q.LivelinessKind)
	}
}

// reverseLifeDurations maps wire deadline, lifespan, and lease to core durations.
func reverseLifeDurations(q WireQoS) (core.Duration, core.Duration, core.Duration, WriterPolicyKind, error) {
	deadline, err := reverseWireDuration(q.Deadline, core.PolicyDeadline)
	if err != nil {
		return core.Duration{}, core.Duration{}, core.Duration{}, core.PolicyDeadline, err
	}

	lifespan, err := reverseWireDuration(q.Lifespan, WriterPolicyLifespan)
	if err != nil {
		return core.Duration{}, core.Duration{}, core.Duration{}, WriterPolicyLifespan, err
	}

	lease, err := reverseWireDuration(q.LeaseDuration, core.PolicyLeaseDuration)
	if err != nil {
		return core.Duration{}, core.Duration{}, core.Duration{}, core.PolicyLeaseDuration, err
	}

	return deadline, lifespan, lease, "", nil
}

// reverseWithPolicy translates wire QoS back to a core profile, tagging
// failures with the refusing policy kind for the compatibility gate.
func reverseWithPolicy(q WireQoS) (core.QoSProfile, WriterPolicyKind, error) {
	var out core.QoSProfile

	history, depth, policy, err := reverseHistory(q)
	if err != nil {
		return core.QoSProfile{}, policy, err
	}

	out.History = history
	out.Depth = depth

	reliability, policy, err := reverseReliability(q)
	if err != nil {
		return core.QoSProfile{}, policy, err
	}

	out.Reliability = reliability
	out.Durability = reverseDurability(q)

	liveliness, policy, err := reverseLiveliness(q)
	if err != nil {
		return core.QoSProfile{}, policy, err
	}

	out.Liveliness = liveliness

	deadline, lifespan, lease, policy, err := reverseLifeDurations(q)
	if err != nil {
		return core.QoSProfile{}, policy, err
	}

	out.Deadline = deadline
	out.Lifespan = lifespan
	out.LeaseDuration = lease

	return out, "", nil
}

// ReverseQoS translates a wire QoS back to a core profile for matching.
//
// It mirrors the FastDDS ReverseQoS shape: keep all maps directly,
// reliable and best effort map directly, transient and persistent
// durability become unknown, manual by participant liveliness becomes
// unknown, and durations convert with infinite mapping. Unknown history,
// reliability, and liveliness kinds refuse loudly with the policy kind
// named. The avoidance flag is never carried on the wire and returns
// false. Unknown durability and liveliness markers map to core unknown
// instead of flowing silently, so Validate still rejects them loudly
// before entity creation.
func ReverseQoS(q WireQoS) (core.QoSProfile, error) {
	out, policy, err := reverseWithPolicy(q)
	if err != nil {
		return core.QoSProfile{}, fmt.Errorf("dds: writerqos reverse: policy %s: %w", string(policy), err)
	}

	return out, nil
}

// CheckWireCompatibility applies the core RxO rules to translated wire values.
//
// It reverses both sides back to core profiles and delegates to
// core.CheckCompatibility, so the verdict reuses the exact matcher
// semantics for reliability, durability, deadline, liveliness, and lease:
// translated offers match identically to facade offers. A side that fails
// reverse translation yields an incompatible report naming the refusing
// policy instead of flowing silently. History, depth, lifespan, and the
// avoidance flag never affect the result, matching core.
func CheckWireCompatibility(offered, requested WireQoS) core.CompatibilityReport {
	offeredProfile, offeredPolicy, offeredErr := reverseWithPolicy(offered)
	requestedProfile, requestedPolicy, requestedErr := reverseWithPolicy(requested)

	if offeredErr != nil || requestedErr != nil {
		report := core.CompatibilityReport{Compatible: false}

		if offeredErr != nil {
			report.Errors = append(report.Errors, core.CompatibilityIssue{
				Kind:   offeredPolicy,
				Detail: "offered wire QoS refuses reverse translation: " + offeredErr.Error(),
			})
		}

		if requestedErr != nil {
			report.Errors = append(report.Errors, core.CompatibilityIssue{
				Kind:   requestedPolicy,
				Detail: "requested wire QoS refuses reverse translation: " + requestedErr.Error(),
			})
		}

		return report
	}

	return core.CheckCompatibility(offeredProfile, requestedProfile)
}
