// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

// Per-profile writer QoS translation for the CycloneDDS SEDP path.
//
// Weld always announced reliable plus transient local depth 100 for every
// topic endpoint while services carved out volatile readers, so exotic
// requested profiles misbehaved natively: a best effort writer still
// announced reliable and a volatile writer still announced transient local.
// This file translates a resolved best available core profile to the exact
// SEDP-announced DDS QoS (reliability kind plus blocking, durability kind,
// history kind plus depth, deadline, lifespan, liveliness plus lease) and
// back through the ForwardQoS and ReverseQoS tables below.
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
// liveliness 0x001b, presentation 0x0021 (DDS 1.4 section 2.2.3.6), and
// latency budget 0x0027 (DDS 1.4 section 2.2.3.8); reliability 0x001a,
// durability 0x001d, and history 0x0040 reuse the existing rtps constants.
// Values match the DDSI-RTPS Table 9.13 PID table and the Wireshark
// packet-rtps.c dissector PID table. rtps/pid.go carries no presentation or
// latency constants, so they live here to avoid touching the frozen file.
const (
	// writerQoSPIDDeadline carries the deadline period.
	writerQoSPIDDeadline uint16 = 0x0023
	// writerQoSPIDLifespan carries the lifespan duration.
	writerQoSPIDLifespan uint16 = 0x002b
	// writerQoSPIDLiveliness carries the liveliness kind plus lease.
	writerQoSPIDLiveliness uint16 = 0x001b
	// writerQoSPIDPresentation carries the presentation access scope plus flags.
	writerQoSPIDPresentation uint16 = 0x0021
	// writerQoSPIDLatencyBudget carries the latency budget duration.
	writerQoSPIDLatencyBudget uint16 = 0x0027
	// writerQoSPIDOwnership carries the ownership kind (SHARED 0, EXCLUSIVE 1)
	// per DDS 1.4 section 2.2.3.9 and DDSI-RTPS Table 9.13 PID_OWNERSHIP 0x0011.
	writerQoSPIDOwnership uint16 = 0x0011
)

// Writer QoS wire kind values.
//
// DDS reliability kinds are best effort 1 and reliable 2. DDS durability
// kinds are volatile 0, transient local 1, transient 2, and persistent 3.
// DDS history kinds are keep last 0 and keep all 1. DDS liveliness kinds
// are automatic 0, manual by participant 1, and manual by topic 2. Forward
// announces all 4 durability and all 3 liveliness kinds per DDS 1.4 sections
// 2.2.3.4 and 2.2.3.11 so peers see our offer; RxO ordering lives in
// core/matcher. Reverse keeps a lenient form for caps-aware matching.
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
	// writerQoSOwnershipShared is the DDS SHARED ownership kind.
	writerQoSOwnershipShared uint32 = 0
	// writerQoSOwnershipExclusive is the DDS EXCLUSIVE ownership kind.
	writerQoSOwnershipExclusive uint32 = 1
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

// WireQoSParseRefusal is the typed loud refusal for an unparseable wire value.
//
// It mirrors WireQoSRefusal for the parse path, which has no topic or
// endpoint pair context: only the refusing policy kind plus a lowercase
// detail travel. Callers test for it with IsWireQoSParseRefusal, never
// with string matching.
//
//nolint:errname // refusal names policy context, Error suffix would obscure loud refusal intent
type WireQoSParseRefusal struct {
	// Policy is the refusing policy kind.
	Policy WriterPolicyKind
	// Detail explains the unparseable value in lowercase form.
	Detail string
}

// Error renders the refusal with the policy kind.
func (e *WireQoSParseRefusal) Error() string {
	return fmt.Sprintf("dds: writerqos parse: policy %s: %s", string(e.Policy), e.Detail)
}

// IsWireQoSParseRefusal reports whether err is a wire QoS parse refusal.
func IsWireQoSParseRefusal(err error) bool {
	var refusal *WireQoSParseRefusal

	return errors.As(err, &refusal)
}

// WireQoSReverseRefusal is the typed loud refusal for an untranslatable wire value.
//
// It mirrors WireQoSRefusal for the reverse path, which has no topic or
// endpoint pair context: only the refusing policy kind plus a lowercase
// detail travel. Transient, persistent, and manual-by-participant kinds
// refuse here instead of mapping to Unknown. Callers test for it with
// IsWireQoSReverseRefusal, never with string matching.
//
//nolint:errname // refusal names policy context, Error suffix would obscure loud refusal intent
type WireQoSReverseRefusal struct {
	// Policy is the refusing policy kind.
	Policy WriterPolicyKind
	// Detail explains the untranslatable kind in lowercase form.
	Detail string
}

// Error renders the refusal with the policy kind.
func (e *WireQoSReverseRefusal) Error() string {
	return fmt.Sprintf("dds: writerqos reverse: policy %s: %s", string(e.Policy), e.Detail)
}

// IsWireQoSReverseRefusal reports whether err is a wire QoS reverse refusal.
func IsWireQoSReverseRefusal(err error) bool {
	var refusal *WireQoSReverseRefusal

	return errors.As(err, &refusal)
}

// refuseParseError builds one loud parse refusal for one policy.
func refuseParseError(policy WriterPolicyKind, format string, args ...any) error {
	return &WireQoSParseRefusal{
		Policy: policy,
		Detail: fmt.Sprintf(format, args...),
	}
}

// refuseReverseError builds one loud reverse refusal for one policy.
func refuseReverseError(policy WriterPolicyKind, format string, args ...any) error {
	return &WireQoSReverseRefusal{
		Policy: policy,
		Detail: fmt.Sprintf(format, args...),
	}
}

// encode renders d as 8 little endian bytes: seconds plus nanoseconds.
func (d WireDuration) encode() []byte {
	out := make([]byte, 0, 8)
	out = binary.LittleEndian.AppendUint32(out, uint32(d.Seconds)) //nolint:gosec // G115: intentional two's complement reinterpret for the wire
	out = binary.LittleEndian.AppendUint32(out, d.Nanoseconds)

	return out
}

// decodeWireDuration parses 8 little endian bytes into a wire duration.
func decodeWireDuration(raw []byte, policy WriterPolicyKind) (WireDuration, error) {
	if len(raw) != 8 {
		return WireDuration{}, refuseParseError(policy, "duration length %d, want 8", len(raw))
	}

	return WireDuration{
		Seconds:     int32(binary.LittleEndian.Uint32(raw[:4])), //nolint:gosec // G115: intentional two's complement reinterpret from the wire
		Nanoseconds: binary.LittleEndian.Uint32(raw[4:8]),
	}, nil
}

// WireQoS is the SEDP-announced DDS QoS for one writer endpoint.
//
// It follows the ForwardQoS table shape: reliability kind plus
// blocking time, durability kind only, history kind plus depth, deadline,
// lifespan, liveliness kind plus lease, presentation scope plus flags, and
// latency budget. Unspecified core durations map
// to DDS infinite, matching the forward rule; core matching treats
// an unspecified offer and an infinite offer identically, so the mapping
// preserves every RxO verdict.
type WireQoS struct {
	// ReliabilityKind is reliable 2 or best effort 1.
	ReliabilityKind uint32
	// MaxBlockingTime is infinite for reliable and zero for best effort.
	MaxBlockingTime WireDuration
	// DurabilityKind is volatile 0, transient local 1, transient 2, or
	// persistent 3 on forward per DDS 1.4 section 2.2.3.4. Unknown kinds
	// refuse loudly on strict reverse; the lenient caps path maps 2 and 3.
	DurabilityKind uint32
	// HistoryKind is keep last 0 or keep all 1.
	HistoryKind uint32
	// Depth is meaningful for keep last only. Keep all encodes zero.
	Depth uint32
	// Deadline is the freshness contract in DDS range.
	Deadline WireDuration
	// Lifespan is message expiry in DDS range.
	Lifespan WireDuration
	// LivelinessKind is automatic 0, manual by participant 1, or manual by
	// topic 2 on forward per DDS 1.4 section 2.2.3.11.
	LivelinessKind uint32
	// LeaseDuration is liveliness lease in DDS range.
	LeaseDuration WireDuration
	// PresentationScope is instance 0, topic 1, or group 2 per DDS 1.4 section 2.2.3.6.
	PresentationScope uint32
	// PresentationCoherent is 1 when coherent access is offered.
	PresentationCoherent uint32
	// PresentationOrdered is 1 when ordered access is offered.
	PresentationOrdered uint32
	// LatencyBudget is the maximum acceptable delay per DDS 1.4 section 2.2.3.8.
	LatencyBudget WireDuration
	// OwnershipKind is shared 0 or exclusive 1 per DDS 1.4 section 2.2.3.9.
	// Forward announces it; reverse fails closed on unknown kinds so a
	// SHARED versus EXCLUSIVE mismatch never matches silently.
	OwnershipKind uint32
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
//
// All four DDS 1.4 section 2.2.3.4 kinds announce: volatile 0, transient
// local 1, transient 2, persistent 3. Peers must see our offer for interop;
// RxO ordering lives in core/matcher. Only unresolved markers refuse.
func forwardDurabilityKind(topic, publisher, subscriber string, d core.DurabilityPolicy) (uint32, error) {
	switch d { //nolint:exhaustive // default refuses unresolved markers with a typed error
	case core.DurabilityVolatile:
		return sedpDurabilityVolatile, nil
	case core.DurabilityTransientLocal:
		return sedpDurabilityTransientLocal, nil
	case core.Durabilityqos4Transient:
		return writerQoSDurabilityTransient, nil
	case core.Durabilityqos4Persistent:
		return writerQoSDurabilityPersistent, nil
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
//
// All three DDS 1.4 section 2.2.3.11 kinds announce: automatic 0, manual by
// participant 1, manual by topic 2. Participant egress (ParticipantMessage)
// stays backend-owned; this only announces the data-writer offer.
func forwardLivelinessKind(topic, publisher, subscriber string, l core.LivelinessPolicy) (uint32, error) {
	switch l { //nolint:exhaustive // default refuses unresolved markers with a typed error
	case core.LivelinessAutomatic:
		return writerQoSLivelinessAutomatic, nil
	case core.Livelinessqos4ManualByParticipant:
		return writerQoSLivelinessManualByParticipant, nil
	case core.LivelinessManualByTopic:
		return writerQoSLivelinessManualByTopic, nil
	default:
		err := refuseWriterQoSError(topic, publisher, subscriber, core.PolicyLiveliness,
			"liveliness %v needs resolution before forward translation", l)
		return 0, err
	}
}

// forwardOwnershipKind maps ownership to the wire kind.
//
// SHARED 0 and EXCLUSIVE 1 announce per DDS 1.4 section 2.2.3.9; the zero
// value (SHARED) is the DDS default so omitted profiles stay concrete.
// Unresolved markers refuse loudly instead of downgrading silently.
func forwardOwnershipKind(topic, publisher, subscriber string, o core.Ownershipqos4Policy) (uint32, error) {
	switch o { //nolint:exhaustive // default refuses unresolved markers with a typed error
	case core.Ownershipqos4Shared:
		return writerQoSOwnershipShared, nil
	case core.Ownershipqos4Exclusive:
		return writerQoSOwnershipExclusive, nil
	default:
		err := refuseWriterQoSError(topic, publisher, subscriber, core.Policyqos4Ownership,
			"ownership %v needs resolution before forward translation", o)
		return 0, err
	}
}

// forwardPresentation maps presentation to wire scope plus flags.
//
// Access scope instance 0, topic 1, or group 2 announces directly per DDS 1.4
// section 2.2.3.6; invalid scopes refuse loudly. Flags announce as 0/1.
func forwardPresentation(topic, publisher, subscriber string, p core.PresentationQosPolicy) (uint32, uint32, uint32, error) {
	switch p.AccessScope {
	case core.PresentationInstance:
		return 0, boolToWire(p.CoherentAccess), boolToWire(p.OrderedAccess), nil
	case core.PresentationTopic:
		return 1, boolToWire(p.CoherentAccess), boolToWire(p.OrderedAccess), nil
	case core.PresentationGroup:
		return 2, boolToWire(p.CoherentAccess), boolToWire(p.OrderedAccess), nil
	default:
		err := refuseWriterQoSError(topic, publisher, subscriber, core.PolicyPresentation,
			"presentation scope %v needs resolution before forward translation", p.AccessScope)
		return 0, 0, 0, err
	}
}

// boolToWire renders one flag as 0/1 for the wire.
func boolToWire(b bool) uint32 {
	if b {
		return 1
	}
	return 0
}

// forwardLatencyBudget maps a latency budget to DDS range.
//
// Zero (unspecified) stays zero per DDS 1.4 section 2.2.3.8: it is the
// concrete default of no extra delay, not infinite. Infinite maps to
// infinite. All other values refuse like forwardWireDuration.
func forwardLatencyBudget(topic, publisher, subscriber string, d core.Duration) (WireDuration, error) {
	if d.IsUnspecified() {
		return WireDuration{}, nil
	}
	return forwardWireDuration(topic, publisher, subscriber, d, core.PolicyLatencyBudget)
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
// The input must carry concrete history, reliability, durability,
// liveliness, and presentation: system default, best available, unknown,
// and invalid values refuse loudly with the policy kind named. Keep last
// with depth zero refuses; keep last depth above the uint32 wire range
// refuses; keep all encodes depth zero because depth is ignored for keep
// all. Reliable maps to infinite blocking and best effort to zero blocking.
// Unspecified deadline, lifespan, and lease map to DDS infinite, matching
// the forward rule. Presentation scope plus flags and latency budget announce
// directly per DDS 1.4 sections 2.2.3.6 and 2.2.3.8. ParticipantMessage
// egress for manual-by-participant assertion stays backend-owned (see
// backend TODO); this only announces the data-writer offer. Topic plus
// publisher plus subscriber scope every refusal so the loud event carries
// the full endpoint pair.
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

	scope, coherent, ordered, err := forwardPresentation(topic, publisher, subscriber, p.Presentation)
	if err != nil {
		return WireQoS{}, err
	}

	out.PresentationScope = scope
	out.PresentationCoherent = coherent
	out.PresentationOrdered = ordered

	latency, err := forwardLatencyBudget(topic, publisher, subscriber, p.LatencyBudget.Duration)
	if err != nil {
		return WireQoS{}, err
	}

	out.LatencyBudget = latency

	deadline, lifespan, lease, err := forwardLifeDurations(topic, publisher, subscriber, p)
	if err != nil {
		return WireQoS{}, err
	}

	out.Deadline = deadline
	out.Lifespan = lifespan
	out.LeaseDuration = lease

	ownershipKind, err := forwardOwnershipKind(topic, publisher, subscriber, p.Ownershipqos4)
	if err != nil {
		return WireQoS{}, err
	}

	out.OwnershipKind = ownershipKind

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

// encodePresentation renders the presentation PID value: scope plus flags.
//
// Layout is scope uint32 plus coherent uint32 plus ordered uint32 (12 bytes
// LE) per the DDS PresentationQosPolicy shape in 2.2.3.6; flags are 0/1.
func (q WireQoS) encodePresentation() []byte {
	out := make([]byte, 0, 12)
	out = binary.LittleEndian.AppendUint32(out, q.PresentationScope)
	out = binary.LittleEndian.AppendUint32(out, q.PresentationCoherent)
	out = binary.LittleEndian.AppendUint32(out, q.PresentationOrdered)

	return out
}

// encodeLatencyBudget renders the latency budget PID value: duration only.
func (q WireQoS) encodeLatencyBudget() []byte {
	return q.LatencyBudget.encode()
}

// encodeOwnership renders the ownership PID value: kind only.
func (q WireQoS) encodeOwnership() []byte {
	return binary.LittleEndian.AppendUint32(nil, q.OwnershipKind)
}

// decodePresentation parses one 12 byte presentation value.
func decodePresentation(raw []byte) (uint32, uint32, uint32, error) {
	if len(raw) != 12 {
		return 0, 0, 0, refuseParseError(core.PolicyPresentation, "presentation length %d, want 12", len(raw))
	}

	return binary.LittleEndian.Uint32(raw[:4]), binary.LittleEndian.Uint32(raw[4:8]), binary.LittleEndian.Uint32(raw[8:12]), nil
}

// Validate checks that q carries only announcable wire values.
//
// It mirrors the forward contract: reliability best effort or reliable,
// durability volatile, transient local, transient, or persistent, history
// keep last with positive depth or keep all with zero depth, liveliness
// automatic, manual by participant, or manual by topic, presentation scope
// 0-2 with 0/1 flags, and durations either infinite or finite in DDS range
// with nanoseconds below one second. Blocking matches reliability: infinite
// for reliable, zero for best effort. Failures return a loud
// WireQoSParseRefusal naming the policy so callers fail closed before
// encoding. It never touches topic context; forward refusals with topic
// come from ForwardQoS.
// validateWireReliability checks the reliability kind plus blocking time.
func (q WireQoS) validateWireReliability() error {
	if q.ReliabilityKind != sedpReliabilityReliable && q.ReliabilityKind != writerQoSReliabilityBestEffort {
		return refuseParseError(core.PolicyReliability, "reliability kind %d is unknown", q.ReliabilityKind)
	}

	if q.ReliabilityKind == sedpReliabilityReliable {
		if !q.MaxBlockingTime.IsInfinite() {
			return refuseParseError(core.PolicyReliability, "reliable blocking must be infinite")
		}
	} else if !q.MaxBlockingTime.IsZero() {
		return refuseParseError(core.PolicyReliability, "best effort blocking must be zero")
	}

	return nil
}

// validateWireHistory checks the history kind plus depth.
func (q WireQoS) validateWireHistory() error {
	if q.HistoryKind != sedpHistoryKeepLast && q.HistoryKind != writerQoSHistoryKeepAll {
		return refuseParseError(WriterPolicyHistory, "history kind %d is unknown", q.HistoryKind)
	}

	if q.HistoryKind == sedpHistoryKeepLast {
		if q.Depth == 0 {
			return refuseParseError(WriterPolicyHistory, "keep last depth 0 is not resolved")
		}
	} else if q.Depth != 0 {
		return refuseParseError(WriterPolicyHistory, "keep all depth %d must be zero", q.Depth)
	}

	return nil
}

func (q WireQoS) Validate() error {
	if err := q.validateWireReliability(); err != nil {
		return err
	}
	if err := q.validateWireDurability(); err != nil {
		return err
	}
	if err := q.validateWireHistory(); err != nil {
		return err
	}
	if err := q.validateWireDeadlines(); err != nil {
		return err
	}
	if err := q.validateWireLiveliness(); err != nil {
		return err
	}
	if err := q.validateWirePresentation(); err != nil {
		return err
	}
	return q.validateWireOwnership()
}

// validateWireOwnership checks ownership kind announcability.
//
// Only SHARED 0 and EXCLUSIVE 1 announce per DDS 1.4 section 2.2.3.9;
// unknown kinds refuse fail-closed so a mismatch never matches silently.
func (q WireQoS) validateWireOwnership() error {
	if q.OwnershipKind == writerQoSOwnershipShared || q.OwnershipKind == writerQoSOwnershipExclusive {
		return nil
	}
	return refuseParseError(core.Policyqos4Ownership, "ownership kind %d is not announcable", q.OwnershipKind)
}

// validateWireDurability checks durability kind announcability.
func (q WireQoS) validateWireDurability() error {
	if q.DurabilityKind == sedpDurabilityVolatile || q.DurabilityKind == sedpDurabilityTransientLocal ||
		q.DurabilityKind == writerQoSDurabilityTransient || q.DurabilityKind == writerQoSDurabilityPersistent {
		return nil
	}
	return refuseParseError(core.PolicyDurability, "durability kind %d is not announcable", q.DurabilityKind)
}

// validateWireDeadlines checks deadline plus lifespan durations.
func (q WireQoS) validateWireDeadlines() error {
	if err := validateWireDuration(q.Deadline, core.PolicyDeadline); err != nil {
		return err
	}
	return validateWireDuration(q.Lifespan, WriterPolicyLifespan)
}

// validateWireLiveliness checks liveliness kind plus lease duration.
func (q WireQoS) validateWireLiveliness() error {
	if q.LivelinessKind != writerQoSLivelinessAutomatic && q.LivelinessKind != writerQoSLivelinessManualByParticipant &&
		q.LivelinessKind != writerQoSLivelinessManualByTopic {
		return refuseParseError(core.PolicyLiveliness, "liveliness kind %d is not announcable", q.LivelinessKind)
	}
	return validateWireDuration(q.LeaseDuration, core.PolicyLeaseDuration)
}

// validateWirePresentation checks presentation scope plus flags and latency.
func (q WireQoS) validateWirePresentation() error {
	if q.PresentationScope > 2 {
		return refuseParseError(core.PolicyPresentation, "presentation scope %d is unknown", q.PresentationScope)
	}
	if q.PresentationCoherent > 1 || q.PresentationOrdered > 1 {
		return refuseParseError(core.PolicyPresentation, "presentation flags must be 0/1")
	}
	return validateWireDuration(q.LatencyBudget, core.PolicyLatencyBudget)
}

// validateWireDuration checks one wire duration is infinite or finite in range.
func validateWireDuration(d WireDuration, policy WriterPolicyKind) error {
	if d.IsInfinite() {
		return nil
	}

	if d.Seconds < 0 {
		return refuseParseError(policy, "negative wire seconds %d", d.Seconds)
	}

	if d.Seconds > writerQoSMaxFiniteSeconds {
		return refuseParseError(policy, "finite seconds %d above DDS range", d.Seconds)
	}

	if d.Nanoseconds >= writerQoSNanosPerSecond {
		return refuseParseError(policy, "nanoseconds %d above range", d.Nanoseconds)
	}

	return nil
}

// ValidatedParams renders q as SEDP params after validation.
//
// It validates before encoding so an invalid WireQoS fails loudly with the
// refusing policy instead of announcing bad bytes. The error preserves the
// loud refusal end to end through fallible announce paths. Peer-controlled
// inputs must flow through this method, never through Params, so malformed
// peer data returns a loud refusal instead of panicking.
func (q WireQoS) ValidatedParams() ([]rtps.Param, error) {
	err := q.Validate()
	if err != nil {
		return nil, err
	}

	return []rtps.Param{
		{ID: rtps.PIDReliability, Value: q.encodeReliability()},
		{ID: rtps.PIDHistory, Value: q.encodeHistory()},
		{ID: rtps.PIDDurability, Value: q.encodeDurability()},
		{ID: writerQoSPIDDeadline, Value: q.Deadline.encode()},
		{ID: writerQoSPIDLifespan, Value: q.Lifespan.encode()},
		{ID: writerQoSPIDLiveliness, Value: q.encodeLiveliness()},
		{ID: writerQoSPIDPresentation, Value: q.encodePresentation()},
		{ID: writerQoSPIDLatencyBudget, Value: q.encodeLatencyBudget()},
		{ID: writerQoSPIDOwnership, Value: q.encodeOwnership()},
	}, nil
}

// Params renders q as SEDP inline QoS params in canonical order.
//
// Order is reliability, history, durability, then deadline, lifespan,
// liveliness, presentation, latency budget, and ownership: the first three keep their
// existing SerializeSEDPDATA relative order and the new PIDs append after,
// so a future announce call site can splice this list in without reordering
// the old bytes. SEDP announce wiring lives in sedp.go (NOT owned here):
// TODO sedp owner - splice ValidatedParams/Params into SerializeSEDPDATA
// and decode PID 0x0021/0x0027/0x0011 in sedpEndpointFields via the
// DecodePresentation/DecodeLatencyBudget/DecodeOwnership helpers below.
// validates before encoding and panics loudly on invalid input because a
// manually constructed invalid WireQoS is a programmer error; production
// ForwardQoS output always validates. Never call Params with
// peer-controlled values: peer inputs must use ValidatedParams so malformed
// wire data returns a loud refusal instead of panicking. No take or data
// path reaches Params with peer values; only local ForwardQoS offers flow
// here, while peer wire values flow through ParseWireQoS and
// CheckWireCompatibility.
func (q WireQoS) Params() []rtps.Param {
	params, err := q.ValidatedParams()
	if err != nil {
		panic(err)
	}

	return params
}

// parseWireReliability decodes the required reliability PID value.
func parseWireReliability(byID map[uint16][]byte) (uint32, WireDuration, error) {
	raw, seen := byID[rtps.PIDReliability]
	if !seen {
		return 0, WireDuration{}, refuseParseError(core.PolicyReliability, "misses reliability")
	}

	if len(raw) != 12 {
		return 0, WireDuration{}, refuseParseError(core.PolicyReliability, "reliability length %d, want 12", len(raw))
	}

	kind := binary.LittleEndian.Uint32(raw[:4])

	blocking, err := decodeWireDuration(raw[4:12], core.PolicyReliability)
	if err != nil {
		return 0, WireDuration{}, err
	}

	return kind, blocking, nil
}

// parseWireDurability decodes the required durability PID value.
func parseWireDurability(byID map[uint16][]byte) (uint32, error) {
	raw, seen := byID[rtps.PIDDurability]
	if !seen {
		return 0, refuseParseError(core.PolicyDurability, "misses durability")
	}

	if len(raw) != 4 {
		return 0, refuseParseError(core.PolicyDurability, "durability length %d, want 4", len(raw))
	}

	return binary.LittleEndian.Uint32(raw), nil
}

// parseWireHistory decodes the required history PID value.
func parseWireHistory(byID map[uint16][]byte) (uint32, uint32, error) {
	raw, seen := byID[rtps.PIDHistory]
	if !seen {
		return 0, 0, refuseParseError(WriterPolicyHistory, "misses history")
	}

	if len(raw) != 8 {
		return 0, 0, refuseParseError(WriterPolicyHistory, "history length %d, want 8", len(raw))
	}

	return binary.LittleEndian.Uint32(raw[:4]), binary.LittleEndian.Uint32(raw[4:8]), nil
}

// parseWireOptionalDurations decodes optional deadline, lifespan, liveliness, presentation, latency budget, and ownership.
func parseWireOptionalDurations(byID map[uint16][]byte, out *WireQoS) error {
	if err := parseWireDeadline(byID, out); err != nil {
		return err
	}
	if err := parseWireLifespan(byID, out); err != nil {
		return err
	}
	if err := parseWireLiveliness(byID, out); err != nil {
		return err
	}
	if err := parseWirePresentation(byID, out); err != nil {
		return err
	}
	if err := parseWireLatencyBudget(byID, out); err != nil {
		return err
	}
	return parseWireOwnership(byID, out)
}

// parseWireDeadline decodes optional deadline duration.
func parseWireDeadline(byID map[uint16][]byte, out *WireQoS) error {
	raw, seen := byID[writerQoSPIDDeadline]
	if !seen {
		return nil
	}
	deadline, err := decodeWireDuration(raw, core.PolicyDeadline)
	if err != nil {
		return err
	}
	out.Deadline = deadline
	return nil
}

// parseWireLifespan decodes optional lifespan duration.
func parseWireLifespan(byID map[uint16][]byte, out *WireQoS) error {
	raw, seen := byID[writerQoSPIDLifespan]
	if !seen {
		return nil
	}
	lifespan, err := decodeWireDuration(raw, WriterPolicyLifespan)
	if err != nil {
		return err
	}
	out.Lifespan = lifespan
	return nil
}

// parseWireLiveliness decodes optional liveliness kind plus lease.
func parseWireLiveliness(byID map[uint16][]byte, out *WireQoS) error {
	raw, seen := byID[writerQoSPIDLiveliness]
	if !seen {
		return nil
	}
	if len(raw) != 12 {
		return refuseParseError(core.PolicyLiveliness, "liveliness length %d, want 12", len(raw))
	}
	out.LivelinessKind = binary.LittleEndian.Uint32(raw[:4])
	lease, err := decodeWireDuration(raw[4:12], core.PolicyLeaseDuration)
	if err != nil {
		return err
	}
	out.LeaseDuration = lease
	return nil
}

// parseWirePresentation decodes optional presentation value.
func parseWirePresentation(byID map[uint16][]byte, out *WireQoS) error {
	raw, seen := byID[writerQoSPIDPresentation]
	if !seen {
		return nil
	}
	scope, coherent, ordered, err := decodePresentation(raw)
	if err != nil {
		return err
	}
	out.PresentationScope = scope
	out.PresentationCoherent = coherent
	out.PresentationOrdered = ordered
	return nil
}

// parseWireLatencyBudget decodes optional latency budget duration.
func parseWireLatencyBudget(byID map[uint16][]byte, out *WireQoS) error {
	raw, seen := byID[writerQoSPIDLatencyBudget]
	if !seen {
		return nil
	}
	budget, err := decodeWireDuration(raw, core.PolicyLatencyBudget)
	if err != nil {
		return err
	}
	out.LatencyBudget = budget
	return nil
}

// parseWireOwnership decodes the optional ownership kind.
//
// Absent ownership defaults to SHARED 0 per DDS 1.4 section 2.2.3.9 (the
// DDS default); present values must be 4 bytes. Malformed lengths refuse
// loudly fail-closed so a truncated kind never matches silently.
func parseWireOwnership(byID map[uint16][]byte, out *WireQoS) error {
	raw, seen := byID[writerQoSPIDOwnership]
	if !seen {
		out.OwnershipKind = writerQoSOwnershipShared
		return nil
	}
	if len(raw) != 4 {
		return refuseParseError(core.Policyqos4Ownership, "ownership length %d, want 4", len(raw))
	}
	out.OwnershipKind = binary.LittleEndian.Uint32(raw)
	return nil
}

// DecodeOwnership parses one ownership PID value for SEDP wiring.
//
// It exists here so sedp.go (NOT owned) can wire PID 0x0011 without
// touching this file: TODO sedp owner - call DecodeOwnership in
// sedpEndpointFields for PID 0x0011 and carry the kind on SEDPEndpoint,
// defaulting absent to SHARED 0 per DDS 1.4 section 2.2.3.9.
func DecodeOwnership(raw []byte) (uint32, error) {
	if len(raw) != 4 {
		return 0, refuseParseError(core.Policyqos4Ownership, "ownership length %d, want 4", len(raw))
	}
	return binary.LittleEndian.Uint32(raw), nil
}

// EncodeOwnership renders one ownership PID value for SEDP announce.
func EncodeOwnership(kind uint32) []byte {
	return binary.LittleEndian.AppendUint32(nil, kind)
}

// CheckOwnershipWireCompatible reports whether two wire ownership kinds match.
//
// SHARED versus EXCLUSIVE mismatch is incompatible fail-closed per DDS 1.4
// section 2.2.3.9.2; unknown kinds refuse loudly. SEDP callers (sedp.go NOT
// owned) should call this after DecodeOwnership on both endpoints when the
// full CheckWireCompatibility path is unavailable:
// TODO sedp owner - call CheckOwnershipWireCompatible in the SEDP match
// path (near SEDPEndpointsMatch) for the ownership RxO check.
func CheckOwnershipWireCompatible(offered, requested uint32) error {
	if offered != writerQoSOwnershipShared && offered != writerQoSOwnershipExclusive {
		return refuseReverseError(core.Policyqos4Ownership, "ownership kind %d is unknown", offered)
	}
	if requested != writerQoSOwnershipShared && requested != writerQoSOwnershipExclusive {
		return refuseReverseError(core.Policyqos4Ownership, "ownership kind %d is unknown", requested)
	}
	if offered != requested {
		return refuseReverseError(core.Policyqos4Ownership, "offered ownership %d does not match requested %d", offered, requested)
	}
	return nil
}

// DecodePresentation parses one presentation value for SEDP wiring.
//
// Layout is scope plus coherent plus ordered (12 bytes LE). It exists here
// so sedp.go (NOT owned) can wire PID 0x0021 without touching this file:
// TODO backend agent - call DecodePresentation in sedpEndpointFields for
// PID 0x0021 and carry scope plus flags on SEDPEndpoint.
func DecodePresentation(raw []byte) (scope, coherent, ordered uint32, err error) {
	return decodePresentation(raw)
}

// EncodePresentation renders one presentation value for SEDP announce.
func EncodePresentation(scope, coherent, ordered uint32) []byte {
	out := make([]byte, 0, 12)
	out = binary.LittleEndian.AppendUint32(out, scope)
	out = binary.LittleEndian.AppendUint32(out, coherent)
	out = binary.LittleEndian.AppendUint32(out, ordered)

	return out
}

// DecodeLatencyBudget parses one latency budget value for SEDP wiring.
//
// TODO backend agent - call DecodeLatencyBudget in sedpEndpointFields for
// PID 0x0027 and carry the duration on SEDPEndpoint.
func DecodeLatencyBudget(raw []byte) (WireDuration, error) {
	return decodeWireDuration(raw, core.PolicyLatencyBudget)
}

// EncodeLatencyBudget renders one latency budget value for SEDP announce.
func EncodeLatencyBudget(d WireDuration) []byte {
	return d.encode()
}

// ParseWireQoS decodes observed vendor SEDP QoS params into wire values.
//
// The first occurrence of each QoS PID wins; unknown PIDs are ignored so
// vendor extensions never break parsing. Reliability, durability, and
// history are required because Weld has always announced them and matching
// without them is meaningless; absent deadline, lifespan, or liveliness
// maps to DDS infinite, which core matching treats exactly like the
// unspecified default. Malformed value lengths refuse loudly with a typed
// WireQoSParseRefusal naming the policy kind.
//
// Wire values are little endian only, matching every peer observed on the
// wire; use ParseWireQoSEndian to refuse big-endian input loudly instead
// of mis-decoding it.
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
	out.PresentationScope = 0
	out.PresentationCoherent = 0
	out.PresentationOrdered = 0
	out.LatencyBudget = WireDuration{}
	out.OwnershipKind = writerQoSOwnershipShared

	err = parseWireOptionalDurations(byID, &out)
	if err != nil {
		return WireQoS{}, err
	}

	return out, nil
}

// ParseWireQoSEndian decodes observed vendor SEDP QoS params with an explicit byte order.
//
// Big-endian peers are refused loudly instead of being mis-decoded as
// little-endian values; wire values from the only observed peers
// (little-endian CycloneDDS) decode through ParseWireQoS unchanged.
func ParseWireQoSEndian(params []rtps.Param, littleEndian bool) (WireQoS, error) {
	if !littleEndian {
		return WireQoS{}, &WireQoSParseRefusal{Policy: core.PolicyReliability, Detail: "big-endian SEDP QoS params are not supported"}
	}

	return ParseWireQoS(params)
}

// sedp3FacadeWireQoSsedp3 returns the wire QoS matching the facade announce.
//
// The facade always announces reliable keep-last depth 100 with infinite
// deadline, lifespan, lease and automatic liveliness, varying only
// durability: volatile for readers that request volatile (ros2 topic echo
// semantics) and transient local otherwise. This differs from the DDS spec
// defaults for absent QoS (best-effort, volatile, keep-last depth 1 per DDS
// 1.4 2.2.3), which ParseWireQoSsedp3WithDefaults applies to peers that omit
// PIDs: the facade is Weld's chosen interop offer, not the spec default,
// so a blank peer decodes to best-effort while Weld announces reliable,
// and the blank-peer matrix stays asymmetric by design.
func sedp3FacadeWireQoSsedp3(volatile bool) WireQoS {
	durability := sedpDurabilityTransientLocal
	if volatile {
		durability = sedpDurabilityVolatile
	}

	return WireQoS{
		ReliabilityKind: sedpReliabilityReliable,
		MaxBlockingTime: InfiniteWireDuration(),
		DurabilityKind:  durability,
		HistoryKind:     sedpHistoryKeepLast,
		Depth:           sedpHistoryDepth,
		Deadline:        InfiniteWireDuration(),
		Lifespan:        InfiniteWireDuration(),
		LivelinessKind:  writerQoSLivelinessAutomatic,
		LeaseDuration:   InfiniteWireDuration(),
	}
}

// ParseWireQoSsedp3WithDefaults decodes vendor SEDP QoS params with DDS spec
// defaults for absent policies.
//
// DDS 1.4 2.2.3: an unspecified QoS policy means its default value, so
// absent reliability, durability, or history PIDs decode to best-effort
// with zero blocking, volatile, and keep-last depth 1 instead of failing.
// Present-but-malformed values still fail loudly, and big-endian params
// refuse loudly like ParseWireQoSEndian. First-wins preserves inline over
// payload on conflict.
func ParseWireQoSsedp3WithDefaults(params []rtps.Param, littleEndian bool) (WireQoS, error) {
	if !littleEndian {
		return WireQoS{}, &WireQoSParseRefusal{Policy: core.PolicyReliability, Detail: "big-endian SEDP QoS params are not supported"}
	}

	byID := make(map[uint16][]byte, len(params))
	for _, param := range params {
		if _, seen := byID[param.ID]; seen {
			continue
		}
		byID[param.ID] = param.Value
	}

	filled := append([]rtps.Param(nil), params...)
	if _, seen := byID[rtps.PIDReliability]; !seen {
		def := WireQoS{ReliabilityKind: writerQoSReliabilityBestEffort, MaxBlockingTime: WireDuration{}}
		filled = append(filled, rtps.Param{ID: rtps.PIDReliability, Value: def.encodeReliability()})
	}
	if _, seen := byID[rtps.PIDDurability]; !seen {
		def := WireQoS{DurabilityKind: sedpDurabilityVolatile}
		filled = append(filled, rtps.Param{ID: rtps.PIDDurability, Value: def.encodeDurability()})
	}
	if _, seen := byID[rtps.PIDHistory]; !seen {
		def := WireQoS{HistoryKind: sedpHistoryKeepLast, Depth: 1}
		filled = append(filled, rtps.Param{ID: rtps.PIDHistory, Value: def.encodeHistory()})
	}

	return ParseWireQoS(filled)
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
		return core.Duration{}, refuseReverseError(policy, "negative wire seconds %d", d.Seconds)
	}

	if d.Nanoseconds >= writerQoSNanosPerSecond {
		return core.Duration{}, refuseReverseError(policy, "nanoseconds %d above range", d.Nanoseconds)
	}

	return core.NewDuration(int64(d.Seconds), d.Nanoseconds), nil
}

// reverseHistory maps wire history kind plus depth to core history.
func reverseHistory(q WireQoS) (core.HistoryPolicy, uint64, WriterPolicyKind, error) {
	switch q.HistoryKind {
	case sedpHistoryKeepLast:
		if q.Depth == 0 {
			return core.HistoryPolicy(0), 0, WriterPolicyHistory, refuseReverseError(WriterPolicyHistory, "keep last depth 0 is not resolved")
		}

		return core.HistoryKeepLast, uint64(q.Depth), "", nil
	case writerQoSHistoryKeepAll:
		return core.HistoryKeepAll, 0, "", nil
	default:
		return core.HistoryPolicy(0), 0, WriterPolicyHistory, refuseReverseError(WriterPolicyHistory, "history kind %d is unknown", q.HistoryKind)
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
		return core.ReliabilityPolicy(0), core.PolicyReliability, refuseReverseError(core.PolicyReliability, "reliability kind %d is unknown", q.ReliabilityKind)
	}
}

// reverseDurability maps wire durability kind to core durability.
//
// All four DDS 1.4 section 2.2.3.4 kinds map directly for interop; unknown
// kinds refuse loudly instead of mapping to Unknown that would pass compat
// silently and misroute durability enforcement.
func reverseDurability(q WireQoS) (core.DurabilityPolicy, WriterPolicyKind, error) {
	switch q.DurabilityKind {
	case sedpDurabilityVolatile:
		return core.DurabilityVolatile, "", nil
	case sedpDurabilityTransientLocal:
		return core.DurabilityTransientLocal, "", nil
	case writerQoSDurabilityTransient:
		return core.Durabilityqos4Transient, "", nil
	case writerQoSDurabilityPersistent:
		return core.Durabilityqos4Persistent, "", nil
	default:
		return core.DurabilityPolicy(0), core.PolicyDurability, refuseReverseError(core.PolicyDurability, "durability kind %d is unknown", q.DurabilityKind)
	}
}

// reverseLiveliness maps wire liveliness kind to core liveliness.
//
// All three DDS 1.4 section 2.2.3.11 kinds map directly for interop. Unknown
// kinds refuse loudly with the policy named.
func reverseLiveliness(q WireQoS) (core.LivelinessPolicy, WriterPolicyKind, error) {
	switch q.LivelinessKind {
	case writerQoSLivelinessAutomatic:
		return core.LivelinessAutomatic, "", nil
	case writerQoSLivelinessManualByParticipant:
		return core.Livelinessqos4ManualByParticipant, "", nil
	case writerQoSLivelinessManualByTopic:
		return core.LivelinessManualByTopic, "", nil
	default:
		return core.LivelinessPolicy(0), core.PolicyLiveliness, refuseReverseError(core.PolicyLiveliness, "liveliness kind %d is unknown", q.LivelinessKind)
	}
}

// reversePresentation maps wire presentation to the core policy.
//
// Scope 0-2 maps to instance/topic/group; flags nonzero mean true. Out of
// range scopes or flags above 1 refuse loudly fail-closed per DDS 1.4
// section 2.2.3.6.
func reversePresentation(q WireQoS) (core.PresentationQosPolicy, WriterPolicyKind, error) {
	var out core.PresentationQosPolicy
	switch q.PresentationScope {
	case 0:
		out.AccessScope = core.PresentationInstance
	case 1:
		out.AccessScope = core.PresentationTopic
	case 2:
		out.AccessScope = core.PresentationGroup
	default:
		return core.PresentationQosPolicy{}, core.PolicyPresentation, refuseReverseError(core.PolicyPresentation, "presentation scope %d is unknown", q.PresentationScope)
	}
	if q.PresentationCoherent > 1 || q.PresentationOrdered > 1 {
		return core.PresentationQosPolicy{}, core.PolicyPresentation, refuseReverseError(core.PolicyPresentation, "presentation flags must be 0/1")
	}
	out.CoherentAccess = q.PresentationCoherent == 1
	out.OrderedAccess = q.PresentationOrdered == 1
	return out, "", nil
}

// reverseOwnership maps wire ownership kind to core ownership.
//
// Only SHARED 0 and EXCLUSIVE 1 map per DDS 1.4 section 2.2.3.9; unknown
// kinds refuse loudly fail-closed so a SHARED versus EXCLUSIVE mismatch
// never matches silently via the zero value.
func reverseOwnership(q WireQoS) (core.Ownershipqos4Policy, WriterPolicyKind, error) {
	switch q.OwnershipKind {
	case writerQoSOwnershipShared:
		return core.Ownershipqos4Shared, "", nil
	case writerQoSOwnershipExclusive:
		return core.Ownershipqos4Exclusive, "", nil
	default:
		return core.Ownershipqos4Policy(0), core.Policyqos4Ownership, refuseReverseError(core.Policyqos4Ownership, "ownership kind %d is unknown", q.OwnershipKind)
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

	durability, policy, err := reverseDurability(q)
	if err != nil {
		return core.QoSProfile{}, policy, err
	}

	out.Durability = durability

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

	presentation, policy, err := reversePresentation(q)
	if err != nil {
		return core.QoSProfile{}, policy, err
	}

	out.Presentation = presentation

	latency, err := reverseWireDuration(q.LatencyBudget, core.PolicyLatencyBudget)
	if err != nil {
		return core.QoSProfile{}, core.PolicyLatencyBudget, err
	}

	out.LatencyBudget = core.LatencyBudgetQosPolicy{Duration: latency}

	ownership, policy, err := reverseOwnership(q)
	if err != nil {
		return core.QoSProfile{}, policy, err
	}

	out.Ownershipqos4 = ownership

	return out, "", nil
}

// ReverseQoS translates a wire QoS back to a core profile for matching.
//
// All durability, liveliness, presentation, and latency kinds map directly
// per DDS 1.4 sections 2.2.3.4, 2.2.3.11, 2.2.3.6, and 2.2.3.8, plus
// ownership SHARED/EXCLUSIVE per 2.2.3.9. Unknown
// history, reliability, durability, liveliness, and presentation values
// refuse loudly with the policy kind named. Durations convert with infinite
// mapping. The avoidance flag is never carried on the wire and returns
// false.
func ReverseQoS(q WireQoS) (core.QoSProfile, error) {
	out, policy, err := reverseWithPolicy(q)
	if err != nil {
		return core.QoSProfile{}, fmt.Errorf("dds: writerqos reverse: policy %s: %w", string(policy), err)
	}

	return out, nil
}

// reverseRxOWithPolicy translates only RxO-relevant wire fields back for matching.
//
// It reverses reliability, durability, deadline, liveliness, lease,
// presentation, latency budget, and ownership and delegates to core.CheckCompatibility,
// so the verdict reuses the exact matcher semantics for those policies:
// translated offers match identically to facade offers, and a SHARED versus
// EXCLUSIVE mismatch fails closed per DDS 1.4 section 2.2.3.9.2. History, depth,
// lifespan, and the avoidance flag never affect the verdict, matching the
// documented contract and core semantics. Non-RxO translation failures are
// ignored; RxO failures yield a refusing policy for the loud incompatible
// report. ReverseQoS stays for callers that need the full profile including
// history and lifespan.
func reverseRxOWithPolicy(q WireQoS) (core.QoSProfile, WriterPolicyKind, error) {
	var out core.QoSProfile

	reliability, policy, err := reverseReliability(q)
	if err != nil {
		return core.QoSProfile{}, policy, err
	}

	out.Reliability = reliability

	durability, policy, err := reverseDurability(q)
	if err != nil {
		return core.QoSProfile{}, policy, err
	}

	out.Durability = durability

	liveliness, policy, err := reverseLiveliness(q)
	if err != nil {
		return core.QoSProfile{}, policy, err
	}

	out.Liveliness = liveliness

	deadline, err := reverseWireDuration(q.Deadline, core.PolicyDeadline)
	if err != nil {
		return core.QoSProfile{}, core.PolicyDeadline, err
	}

	out.Deadline = deadline

	lease, err := reverseWireDuration(q.LeaseDuration, core.PolicyLeaseDuration)
	if err != nil {
		return core.QoSProfile{}, core.PolicyLeaseDuration, err
	}

	out.LeaseDuration = lease

	presentation, policy, err := reversePresentation(q)
	if err != nil {
		return core.QoSProfile{}, policy, err
	}

	out.Presentation = presentation

	latency, err := reverseWireDuration(q.LatencyBudget, core.PolicyLatencyBudget)
	if err != nil {
		return core.QoSProfile{}, core.PolicyLatencyBudget, err
	}

	out.LatencyBudget = core.LatencyBudgetQosPolicy{Duration: latency}

	ownership, policy, err := reverseOwnership(q)
	if err != nil {
		return core.QoSProfile{}, policy, err
	}

	out.Ownershipqos4 = ownership

	return out, "", nil
}

// CheckWireCompatibility applies the core RxO rules to translated wire values.
//
// It reverses RxO-relevant fields (reliability, durability, deadline,
// liveliness, lease, presentation, latency budget, ownership) back to core profiles
// and delegates to core.CheckCompatibility, so the verdict reuses the exact
// matcher semantics: translated offers match identically to facade offers.
// History, depth, lifespan, and the avoidance flag never affect the result,
// matching core. A side that fails RxO reverse translation yields an
// incompatible report naming the refusing policy instead of flowing
// silently. ReverseQoS stays for callers that need the full profile.
//
// Durability-service caps are optional: with no caps the verdict is the
// plain core check and VOLATILE plus TRANSIENT_LOCAL pairs stay
// byte-identical. With exactly two caps (offered, requested) the path
// reverses durability leniently (wire 2 maps to core transient, wire 3 to
// core persistent) then delegates to Checkqos4CompatibilityWithDurabilityService,
// so TRANSIENT and PERSISTENT pairs enforce caps while VOLATILE and
// TRANSIENT_LOCAL still bypass. Any other caps count fails closed as
// incompatible, never silently. SEDP callers should pass each endpoint's
// DurabilityService caps once both sides carry them; see durability.go.
func CheckWireCompatibility(offered, requested WireQoS, caps ...DurabilityServiceQoS) core.CompatibilityReport {
	if len(caps) == 0 {
		return checkWireCompatibilityStrict(offered, requested)
	}
	if len(caps) != 2 {
		return core.CompatibilityReport{
			Compatible: false,
			Errors: []core.CompatibilityIssue{
				{Kind: core.PolicyDurability, Detail: "durability service caps must be offered plus requested pair"},
			},
		}
	}
	offeredProfile, offeredPolicy, offeredErr := reverseRxOLenient(offered)
	requestedProfile, requestedPolicy, requestedErr := reverseRxOLenient(requested)
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
	return Checkqos4CompatibilityWithDurabilityService(offeredProfile, requestedProfile, caps[0], caps[1])
}

// checkWireCompatibilityStrict applies the plain core RxO rules without caps.
func checkWireCompatibilityStrict(offered, requested WireQoS) core.CompatibilityReport {
	offeredProfile, offeredPolicy, offeredErr := reverseRxOWithPolicy(offered)
	requestedProfile, requestedPolicy, requestedErr := reverseRxOWithPolicy(requested)

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

// CheckWireCompatibilityWithDurabilityService applies the core RxO rules
// plus durability-service caps to translated wire values.
//
// It reverses RxO fields (all durability and liveliness kinds map directly
// since forward announces every kind) then delegates to
// Checkqos4CompatibilityWithDurabilityService. VOLATILE and TRANSIENT_LOCAL
// pairs return the core report unchanged; TRANSIENT and PERSISTENT pairs
// enforce caps fail-closed. A side that fails reverse still yields
// an incompatible report naming the refusing policy. Prefer passing caps
// through CheckWireCompatibility; this named form exists for callers that
// always carry caps.
func CheckWireCompatibilityWithDurabilityService(offered, requested WireQoS, offeredCaps, requestedCaps DurabilityServiceQoS) core.CompatibilityReport {
	return CheckWireCompatibility(offered, requested, offeredCaps, requestedCaps)
}

// reverseRxOLenient translates RxO wire fields with transient and persistent
// durability mapped instead of refused.
//
// All durability, liveliness, presentation, latency, and ownership kinds map
// exactly like reverseRxOWithPolicy; this lenient form is kept for the
// caps-aware compatibility path so durability-service enforces per DDS 1.4
// section 2.2.3.5. It delegates to reverseRxOWithPolicy since strict reverse
// already maps all four durability kinds.
func reverseRxOLenient(q WireQoS) (core.QoSProfile, WriterPolicyKind, error) {
	return reverseRxOWithPolicy(q)
}
