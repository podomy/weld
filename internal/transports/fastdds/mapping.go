// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package fastdds

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"strings"
	"sync"
	"time"

	"github.com/podomy/weld/internal/core"
	"github.com/podomy/weld/internal/transports"
)

// ErrFastDDSNotImplemented reports that the FastDDS network path is not yet wired.
var ErrFastDDSNotImplemented = errors.New("fastdds: backend not implemented")

// DataRepresentation names the CDR encoding selected for an endpoint.
type DataRepresentation string

// Data representation selected for FastDDS endpoints.
const (
	// DataRepresentationXCDR1 selects XCDR1 encoding.
	DataRepresentationXCDR1 DataRepresentation = "XCDR1"
)

// ForcedDataRepresentation returns the forced representation for endpoints.
//
// FastDDS forces XCDR1 on every reader and writer. XCDR2 is never used.
func ForcedDataRepresentation() []DataRepresentation {
	return []DataRepresentation{DataRepresentationXCDR1}
}

// checkForcedRepresentation reports whether reps force XCDR1 only.
func checkForcedRepresentation(reps []DataRepresentation) bool {
	if len(reps) != 1 {
		return false
	}

	return reps[0] == DataRepresentationXCDR1
}

// Discovery wire constants for the FastDDS transport mapping.
const (
	// DummyUnicastPeer is the placeholder unicast peer for range off.
	DummyUnicastPeer = "127.0.0.1:65534"
	// LoopbackPeer is the UDP loopback peer for localhost discovery.
	LoopbackPeer = "127.0.0.1"
	// MulticastGroup is the SPDP multicast group appended for subnet discovery.
	MulticastGroup = "239.255.0.1"
	// MaxStaticPeers caps static peer entries.
	MaxStaticPeers = 256
	// ClientFallbackProfileName selects the client fallback XML profile for services.
	ClientFallbackProfileName = "client"
)

// DiscoveryTransport describes the FastDDS transport for one discovery range.
type DiscoveryTransport struct {
	// UseMulticast selects SPDP multicast send and receive.
	UseMulticast bool
	// UseSharedMemory selects the shared memory transport.
	UseSharedMemory bool
	// UseUDP selects UDPv4 unicast.
	UseUDP bool
	// Peers holds static peers plus group addresses in join order.
	Peers []string
	// MinimalAllocation selects minimal allocation for range off.
	MinimalAllocation bool
}

// NormalizeDiscoveryRange maps NotSet to SystemDefault.
//
// Transports pass Range through unchanged elsewhere. FastDDS join paths
// normalize before mapping so a zero value config still joins with a
// concrete range.
func NormalizeDiscoveryRange(r transports.DiscoveryRange) transports.DiscoveryRange {
	if r == transports.DiscoveryRangeNotSet {
		return transports.DiscoveryRangeSystemDefault
	}

	return r
}

// MapDiscoveryRange maps a discovery range to a FastDDS transport.
//
// Off uses minimal allocation with cleared multicast plus a dummy unicast
// peer. Localhost clears multicast with shared memory plus UDP loopback
// peers. Subnet keeps peers plus the multicast group. SystemDefault defers
// to middleware defaults with multicast, shared memory, and UDP enabled
// and no static peers. NotSet and unknown values report an error so the
// caller normalizes explicitly with NormalizeDiscoveryRange first.
//
// Localhost clears multicast while CycloneDDS defaults keep multicast on,
// so localhost cross vendor paths need an explicit interop check proving
// Weld still discovers Cyclone peers through unicast bootstrap.
func MapDiscoveryRange(r transports.DiscoveryRange) (DiscoveryTransport, error) {
	switch r {
	case transports.DiscoveryRangeOff:
		return DiscoveryTransport{
			UseMulticast:      false,
			UseSharedMemory:   false,
			UseUDP:            true,
			Peers:             []string{DummyUnicastPeer},
			MinimalAllocation: true,
		}, nil
	case transports.DiscoveryRangeLocalhost:
		return DiscoveryTransport{
			UseMulticast:      false,
			UseSharedMemory:   true,
			UseUDP:            true,
			Peers:             []string{LoopbackPeer},
			MinimalAllocation: false,
		}, nil
	case transports.DiscoveryRangeSubnet:
		return DiscoveryTransport{
			UseMulticast:      true,
			UseSharedMemory:   false,
			UseUDP:            true,
			Peers:             []string{MulticastGroup},
			MinimalAllocation: false,
		}, nil
	case transports.DiscoveryRangeSystemDefault:
		return DiscoveryTransport{
			UseMulticast:      true,
			UseSharedMemory:   true,
			UseUDP:            true,
			Peers:             nil,
			MinimalAllocation: false,
		}, nil
	case transports.DiscoveryRangeNotSet:
		return DiscoveryTransport{}, errors.New("fastdds: discovery range not set: normalize with NormalizeDiscoveryRange first") //nolint:wrapcheck // new error, nothing to wrap
	default:
		return DiscoveryTransport{}, fmt.Errorf("fastdds: unknown discovery range %v", r)
	}
}

// RequiresCycloneInteropCheck reports whether t needs a Cyclone interop check.
//
// Localhost clears multicast while Cyclone peers default to multicast, so
// a cleared multicast transport needs proof that unicast bootstrap still
// discovers Cyclone peers. Off needs no check because discovery is off,
// and multicast enabled transports need no check.
func RequiresCycloneInteropCheck(t DiscoveryTransport) bool {
	return !t.UseMulticast && !t.MinimalAllocation
}

// FastDDS duration bounds for the DDS Duration_t range.
const (
	// FastDDSMaxSeconds is the largest DDS seconds value before infinite.
	FastDDSMaxSeconds = int64(0x7FFFFFFF)
	// FastDDSInfiniteSeconds encodes infinite seconds.
	FastDDSInfiniteSeconds = int32(0x7FFFFFFF)
	// FastDDSInfiniteNanoseconds encodes infinite fractional nanoseconds.
	FastDDSInfiniteNanoseconds = uint32(0xFFFFFFFF)
)

// FastDDSDuration is the DDS Duration_t wire value: seconds plus nanoseconds.
type FastDDSDuration struct {
	// Seconds carries whole seconds, or infinite seconds for infinity.
	Seconds int32
	// Nanoseconds carries fractional nanoseconds, or infinite nanos for infinity.
	Nanoseconds uint32
}

// InfiniteFastDDSDuration returns the DDS infinite duration.
func InfiniteFastDDSDuration() FastDDSDuration {
	return FastDDSDuration{Seconds: FastDDSInfiniteSeconds, Nanoseconds: FastDDSInfiniteNanoseconds}
}

// IsInfinite reports whether d is the DDS infinite encoding.
func (d FastDDSDuration) IsInfinite() bool {
	return d.Seconds == FastDDSInfiniteSeconds && d.Nanoseconds == FastDDSInfiniteNanoseconds
}

// IsZero reports whether d is the zero duration.
func (d FastDDSDuration) IsZero() bool {
	return d.Seconds == 0 && d.Nanoseconds == 0
}

// CoreDurationToFastDDS converts a resolved core duration to DDS range.
//
// Unspecified and best available sentinels report an error: the caller
// resolves system defaults in core before translation. Infinite maps to
// DDS infinite. Finite values above the DDS seconds range clamp to
// infinite with a saturating clamp at max seconds. Negative seconds and
// nanoseconds at or above one second report an error.
func CoreDurationToFastDDS(d core.Duration) (FastDDSDuration, error) {
	if d.IsUnspecified() {
		return FastDDSDuration{}, errors.New("fastdds: unspecified duration needs ResolveSystemDefaults first") //nolint:wrapcheck // new error, nothing to wrap
	}

	if core.IsBestAvailableDuration(d) {
		return FastDDSDuration{}, errors.New("fastdds: best available duration needs endpoint resolution first") //nolint:wrapcheck // new error, nothing to wrap
	}

	if d.IsInfinite() {
		return InfiniteFastDDSDuration(), nil
	}

	if d.Seconds < 0 {
		return FastDDSDuration{}, fmt.Errorf("fastdds: negative duration %v", d)
	}

	if d.Nanoseconds >= 1000000000 {
		return FastDDSDuration{}, fmt.Errorf("fastdds: nanoseconds %d above range in %v", d.Nanoseconds, d)
	}

	if d.Seconds > FastDDSMaxSeconds {
		return InfiniteFastDDSDuration(), nil
	}

	return FastDDSDuration{Seconds: int32(d.Seconds), Nanoseconds: d.Nanoseconds}, nil
}

// FastDDSDurationToCore converts a DDS duration back to a core duration.
//
// DDS infinite maps to core infinite. Other values validate the
// nanoseconds range and map through core NewDuration. Malformed
// nanoseconds report an error.
func FastDDSDurationToCore(d FastDDSDuration) (core.Duration, error) {
	if d.IsInfinite() {
		return core.InfiniteDuration(), nil
	}

	if d.Seconds < 0 {
		return core.Duration{}, fmt.Errorf("fastdds: negative DDS duration %v seconds", d.Seconds)
	}

	if d.Nanoseconds >= 1000000000 {
		return core.Duration{}, fmt.Errorf("fastdds: DDS nanoseconds %d above range", d.Nanoseconds)
	}

	out := core.NewDuration(int64(d.Seconds), d.Nanoseconds)

	return out, nil
}

// HistoryKind names the FastDDS history kind.
type HistoryKind int

// FastDDS history kinds.
const (
	// HistoryKeepLast keeps the newest Depth samples.
	HistoryKeepLast HistoryKind = iota
	// HistoryKeepAll keeps samples up to the core retention bound.
	HistoryKeepAll
	// HistoryUnknown marks an unrecognized history kind.
	HistoryUnknown
)

// String returns the lowercase name of the history kind.
func (k HistoryKind) String() string {
	switch k {
	case HistoryKeepLast:
		return "keep_last"
	case HistoryKeepAll:
		return "keep_all"
	case HistoryUnknown:
		return "unknown"
	default:
		return "invalid"
	}
}

// ReliabilityKind names the FastDDS reliability kind.
type ReliabilityKind int

// FastDDS reliability kinds.
const (
	// ReliabilityReliable retries until delivery.
	ReliabilityReliable ReliabilityKind = iota
	// ReliabilityBestEffort sends once without retry.
	ReliabilityBestEffort
	// ReliabilityUnknown marks an unrecognized reliability kind.
	ReliabilityUnknown
)

// String returns the lowercase name of the reliability kind.
func (k ReliabilityKind) String() string {
	switch k {
	case ReliabilityReliable:
		return "reliable"
	case ReliabilityBestEffort:
		return "best_effort"
	case ReliabilityUnknown:
		return "unknown"
	default:
		return "invalid"
	}
}

// DurabilityKind names the FastDDS durability kind.
type DurabilityKind int

// FastDDS durability kinds. Transient and Persistent exist on the wire
// for reverse mapping only and become unknown in core.
const (
	// DurabilityVolatile keeps no history for late joiners.
	DurabilityVolatile DurabilityKind = iota
	// DurabilityTransientLocal keeps history for late joiners.
	DurabilityTransientLocal
	// DurabilityTransient is a wire only kind that reverses to unknown.
	DurabilityTransient
	// DurabilityPersistent is a wire only kind that reverses to unknown.
	DurabilityPersistent
	// DurabilityUnknown marks an unrecognized durability kind.
	DurabilityUnknown
)

// String returns the lowercase name of the durability kind.
func (k DurabilityKind) String() string {
	switch k {
	case DurabilityVolatile:
		return "volatile"
	case DurabilityTransientLocal:
		return "transient_local"
	case DurabilityTransient:
		return "transient"
	case DurabilityPersistent:
		return "persistent"
	case DurabilityUnknown:
		return "unknown"
	default:
		return "invalid"
	}
}

// LivelinessKind names the FastDDS liveliness kind.
type LivelinessKind int

// FastDDS liveliness kinds.
const (
	// LivelinessAutomatic asserts liveliness without user action.
	LivelinessAutomatic LivelinessKind = iota
	// LivelinessManualByTopic requires a user assertion per topic.
	LivelinessManualByTopic
	// LivelinessUnknown marks an unrecognized liveliness kind.
	LivelinessUnknown
)

// String returns the lowercase name of the liveliness kind.
func (k LivelinessKind) String() string {
	switch k {
	case LivelinessAutomatic:
		return "automatic"
	case LivelinessManualByTopic:
		return "manual_by_topic"
	case LivelinessUnknown:
		return "unknown"
	default:
		return "invalid"
	}
}

// FastDDSQoS is the FastDDS view of one endpoint QoS profile.
//
// It follows the CycloneDDS forward and reverse tables with FastDDS
// deltas. Durability sets kind only with no durability service history,
// unlike Cyclone which programs transient local durability service with
// history settings. Liveliness carries a synthesized announcement period
// of two thirds of the lease with no Cyclone equivalent. Lease handling
// distinguishes DDS default from infinity: LeaseIsDefault is true when
// the core lease was unspecified, so FastDDS leaves the middleware
// default in place, while Cyclone writes infinity explicitly.
type FastDDSQoS struct {
	// History is keep last or keep all.
	History HistoryKind
	// Depth is meaningful for keep last only. Keep all ignores it.
	Depth uint32
	// Reliability is reliable or best effort.
	Reliability ReliabilityKind
	// MaxBlockingTime is infinite for reliable and zero for best effort.
	MaxBlockingTime FastDDSDuration
	// Durability is volatile or transient local on forward. Reverse can
	// also carry transient, persistent, or unknown.
	Durability DurabilityKind
	// Deadline is the freshness contract in DDS range.
	Deadline FastDDSDuration
	// Lifespan is message expiry in DDS range.
	Lifespan FastDDSDuration
	// LeaseDuration is liveliness lease in DDS range.
	LeaseDuration FastDDSDuration
	// LeaseIsDefault is true when the core lease was unspecified and
	// FastDDS keeps the DDS default instead of writing infinity.
	LeaseIsDefault bool
	// Liveliness is automatic or manual by topic.
	Liveliness LivelinessKind
	// AnnouncementPeriod is two thirds of a finite lease when set.
	AnnouncementPeriod FastDDSDuration
	// AnnouncementIsSet is true when AnnouncementPeriod carries a period.
	AnnouncementIsSet bool
	// IgnoreLocalParticipant ignores local endpoints on the participant.
	IgnoreLocalParticipant bool
	// AutodisposeUnregisteredInstances is always false: writers never autodispose.
	AutodisposeUnregisteredInstances bool
	// DataRepresentation forces XCDR1 on every reader and writer.
	DataRepresentation []DataRepresentation
	// TypeHash is the encoded type hash carried in userdata.
	TypeHash string
}

// checkForwardMarkers rejects unresolved and unknown core markers for forward translation.
func checkForwardMarkers(p core.QoSProfile) error {
	switch p.History { //nolint:exhaustive // default rejects unresolved markers with error
	case core.HistoryKeepLast, core.HistoryKeepAll:
	default:
		return fmt.Errorf("fastdds: history %v needs resolution before forward translation", p.History)
	}

	switch p.Reliability { //nolint:exhaustive // default rejects unresolved markers with error
	case core.ReliabilityReliable, core.ReliabilityBestEffort:
	default:
		return fmt.Errorf("fastdds: reliability %v needs resolution before forward translation", p.Reliability)
	}

	switch p.Durability { //nolint:exhaustive // default rejects unresolved markers with error
	case core.DurabilityVolatile, core.DurabilityTransientLocal:
	default:
		return fmt.Errorf("fastdds: durability %v needs resolution before forward translation", p.Durability)
	}

	switch p.Liveliness { //nolint:exhaustive // default rejects unresolved markers with error
	case core.LivelinessAutomatic, core.LivelinessManualByTopic:
	default:
		return fmt.Errorf("fastdds: liveliness %v needs resolution before forward translation", p.Liveliness)
	}

	return nil
}

// forwardHistory maps core history and depth to FastDDS values.
func forwardHistory(p core.QoSProfile) (HistoryKind, uint32, error) {
	switch p.History { //nolint:exhaustive // default rejects unresolved history with error
	case core.HistoryKeepLast:
		if p.Depth == 0 {
			return HistoryUnknown, 0, errors.New("fastdds: keep last depth 0 needs ResolveSystemDefaults first") //nolint:wrapcheck // new error, nothing to wrap
		}

		if p.Depth > uint64(FastDDSMaxSeconds) {
			return HistoryUnknown, 0, fmt.Errorf("fastdds: keep last depth %d above DDS range", p.Depth)
		}

		return HistoryKeepLast, uint32(p.Depth), nil
	case core.HistoryKeepAll:
		return HistoryKeepAll, 0, nil
	default:
		return HistoryUnknown, 0, fmt.Errorf("fastdds: history %v needs resolution before forward translation", p.History)
	}
}

// forwardReliability maps core reliability to kind plus blocking time.
func forwardReliability(p core.QoSProfile) (ReliabilityKind, FastDDSDuration, error) {
	switch p.Reliability { //nolint:exhaustive // default rejects unresolved reliability with error
	case core.ReliabilityReliable:
		return ReliabilityReliable, InfiniteFastDDSDuration(), nil
	case core.ReliabilityBestEffort:
		return ReliabilityBestEffort, FastDDSDuration{}, nil
	default:
		return ReliabilityUnknown, FastDDSDuration{}, fmt.Errorf("fastdds: reliability %v needs resolution before forward translation", p.Reliability)
	}
}

// forwardDurability maps core durability to FastDDS kind only.
func forwardDurability(p core.QoSProfile) (DurabilityKind, error) {
	switch p.Durability { //nolint:exhaustive // default rejects unresolved durability with error
	case core.DurabilityVolatile:
		return DurabilityVolatile, nil
	case core.DurabilityTransientLocal:
		return DurabilityTransientLocal, nil
	default:
		return DurabilityUnknown, fmt.Errorf("fastdds: durability %v needs resolution before forward translation", p.Durability)
	}
}

// forwardLiveliness maps core liveliness to FastDDS kind.
func forwardLiveliness(p core.QoSProfile) (LivelinessKind, error) {
	switch p.Liveliness { //nolint:exhaustive // default rejects unresolved liveliness with error
	case core.LivelinessAutomatic:
		return LivelinessAutomatic, nil
	case core.LivelinessManualByTopic:
		return LivelinessManualByTopic, nil
	default:
		return LivelinessUnknown, fmt.Errorf("fastdds: liveliness %v needs resolution before forward translation", p.Liveliness)
	}
}

// forwardDeadline maps a core deadline to DDS range, treating unspecified as infinite.
func forwardDeadline(d core.Duration) (FastDDSDuration, error) {
	if d.IsUnspecified() {
		return InfiniteFastDDSDuration(), nil
	}

	out, err := CoreDurationToFastDDS(d)
	if err != nil {
		return FastDDSDuration{}, fmt.Errorf("fastdds: forward deadline: %w", err)
	}

	return out, nil
}

// forwardLifespan maps a core lifespan to DDS range, treating unspecified as infinite.
func forwardLifespan(d core.Duration) (FastDDSDuration, error) {
	if d.IsUnspecified() {
		return InfiniteFastDDSDuration(), nil
	}

	out, err := CoreDurationToFastDDS(d)
	if err != nil {
		return FastDDSDuration{}, fmt.Errorf("fastdds: forward lifespan: %w", err)
	}

	return out, nil
}

// ForwardLeaseDuration maps a core lease to DDS range with the FastDDS default rule.
//
// Unspecified maps to the DDS default with IsDefault true, so FastDDS
// leaves the middleware default in place. Cyclone maps the same input to
// infinity explicitly. Resolved infinite maps to infinite with IsDefault
// false. Finite values clamp to DDS range. Best available and malformed
// durations report an error. Core normalization still runs first: callers
// resolve system defaults before entity creation, and this helper pins the
// one place where unspecified survives as DDS default on FastDDS.
func ForwardLeaseDuration(lease core.Duration) (FastDDSDuration, bool, error) {
	if lease.IsUnspecified() {
		return InfiniteFastDDSDuration(), true, nil
	}

	if lease.IsInfinite() {
		return InfiniteFastDDSDuration(), false, nil
	}

	out, err := CoreDurationToFastDDS(lease)
	if err != nil {
		return FastDDSDuration{}, false, fmt.Errorf("fastdds: forward lease: %w", err)
	}

	return out, false, nil
}

// AnnouncementPeriodForLease synthesizes the FastDDS announcement period.
//
// The period is two thirds of a finite lease. Unspecified, infinite, and
// best available leases carry no period and report set false. Negative
// seconds and nanoseconds at or above one second report an error. There
// is no Cyclone equivalent for this period.
func AnnouncementPeriodForLease(lease core.Duration) (time.Duration, bool, error) {
	if lease.IsUnspecified() {
		return 0, false, nil
	}

	if lease.IsInfinite() {
		return 0, false, nil
	}

	if core.IsBestAvailableDuration(lease) {
		return 0, false, nil
	}

	if lease.Seconds < 0 {
		return 0, false, fmt.Errorf("fastdds: negative lease %v has no announcement period", lease)
	}

	if lease.Nanoseconds >= 1000000000 {
		return 0, false, fmt.Errorf("fastdds: lease nanoseconds %d above range", lease.Nanoseconds)
	}

	total := lease.TotalNanoseconds()
	if total <= 0 {
		return 0, false, fmt.Errorf("fastdds: non positive lease %v has no announcement period", lease)
	}

	period := total/3*2 + (total%3)*2/3

	return time.Duration(period), true, nil
}

// forwardAnnouncement converts a lease announcement period to DDS range.
func forwardAnnouncement(lease core.Duration) (FastDDSDuration, bool, error) {
	period, set, err := AnnouncementPeriodForLease(lease)
	if err != nil {
		return FastDDSDuration{}, false, err
	}

	if !set {
		return FastDDSDuration{}, false, nil
	}

	sec := int64(period / time.Second)
	nsec := uint32(period % time.Second) //nolint:gosec // G115: period is non-negative from lease checks, remainder below 1e9 fits uint32

	if sec > FastDDSMaxSeconds {
		return InfiniteFastDDSDuration(), true, nil
	}

	return FastDDSDuration{Seconds: int32(sec), Nanoseconds: nsec}, true, nil //nolint:gosec // G115: sec is non-negative and capped by FastDDSMaxSeconds above
}

// ForwardQoS translates a resolved core profile to FastDDS QoS.
//
// The input must carry concrete history, reliability, durability, and
// liveliness: system default, best available, unknown, and invalid values
// report an error. FastDDS valid QoS checks always pass, so invalid input
// would fail late on the wire; core rejects it early here instead. Keep
// last with depth zero reports an error. Deadline and lifespan map
// unspecified to infinite. Lease maps unspecified to DDS default with
// LeaseIsDefault true. History depth, reliable with infinite blocking,
// best effort with zero blocking, durability kind only, deadline and
// lifespan from nanoseconds with saturating clamp, ignorelocal
// participant true, writer data lifecycle without autodispose, XCDR1
// forced, and userdata carrying the type hash follow the CycloneDDS table
// with the FastDDS deltas noted on FastDDSQoS. Type hash rides as opaque
// passthrough and may be empty for pre hash peers.
func ForwardQoS(p core.QoSProfile, typeHash string) (FastDDSQoS, error) {
	err := checkForwardMarkers(p)
	if err != nil {
		return FastDDSQoS{}, err
	}

	history, depth, err := forwardHistory(p)
	if err != nil {
		return FastDDSQoS{}, err
	}

	reliability, blocking, err := forwardReliability(p)
	if err != nil {
		return FastDDSQoS{}, err
	}

	durability, err := forwardDurability(p)
	if err != nil {
		return FastDDSQoS{}, err
	}

	liveliness, err := forwardLiveliness(p)
	if err != nil {
		return FastDDSQoS{}, err
	}

	deadline, err := forwardDeadline(p.Deadline)
	if err != nil {
		return FastDDSQoS{}, err
	}

	lifespan, err := forwardLifespan(p.Lifespan)
	if err != nil {
		return FastDDSQoS{}, err
	}

	lease, leaseDefault, err := ForwardLeaseDuration(p.LeaseDuration)
	if err != nil {
		return FastDDSQoS{}, err
	}

	announcement, announcementSet, err := forwardAnnouncement(p.LeaseDuration)
	if err != nil {
		return FastDDSQoS{}, err
	}

	reps := ForcedDataRepresentation()
	if !checkForcedRepresentation(reps) {
		return FastDDSQoS{}, errors.New("fastdds: forced representation is not XCDR1 only") //nolint:wrapcheck // new error, nothing to wrap
	}

	return FastDDSQoS{
		History:                          history,
		Depth:                            depth,
		Reliability:                      reliability,
		MaxBlockingTime:                  blocking,
		Durability:                       durability,
		Deadline:                         deadline,
		Lifespan:                         lifespan,
		LeaseDuration:                    lease,
		LeaseIsDefault:                   leaseDefault,
		Liveliness:                       liveliness,
		AnnouncementPeriod:               announcement,
		AnnouncementIsSet:                announcementSet,
		IgnoreLocalParticipant:           true,
		AutodisposeUnregisteredInstances: false,
		DataRepresentation:               reps,
		TypeHash:                         typeHash,
	}, nil
}

// reverseHistory maps FastDDS history back to core history and depth.
func reverseHistory(q FastDDSQoS) (core.HistoryPolicy, uint64, error) {
	switch q.History { //nolint:exhaustive // default rejects unknown wire kind with error
	case HistoryKeepLast:
		if q.Depth == 0 {
			return core.HistoryUnknown, 0, errors.New("fastdds: reverse keep last depth 0 is not resolved") //nolint:wrapcheck // new error, nothing to wrap
		}

		return core.HistoryKeepLast, uint64(q.Depth), nil
	case HistoryKeepAll:
		return core.HistoryKeepAll, 0, nil
	default:
		return core.HistoryUnknown, 0, fmt.Errorf("fastdds: reverse history %v is unknown", q.History)
	}
}

// reverseReliability maps FastDDS reliability back to core reliability.
func reverseReliability(q FastDDSQoS) (core.ReliabilityPolicy, error) {
	switch q.Reliability { //nolint:exhaustive // default rejects unknown wire kind with error
	case ReliabilityReliable:
		return core.ReliabilityReliable, nil
	case ReliabilityBestEffort:
		return core.ReliabilityBestEffort, nil
	default:
		return core.ReliabilityUnknown, fmt.Errorf("fastdds: reverse reliability %v is unknown", q.Reliability)
	}
}

// reverseDurability maps FastDDS durability back to core durability.
//
// Transient and persistent have no core counterpart and become unknown,
// per the CycloneDDS reverse table.
func reverseDurability(q FastDDSQoS) (core.DurabilityPolicy, error) {
	switch q.Durability {
	case DurabilityVolatile:
		return core.DurabilityVolatile, nil
	case DurabilityTransientLocal:
		return core.DurabilityTransientLocal, nil
	case DurabilityTransient, DurabilityPersistent, DurabilityUnknown:
		return core.DurabilityUnknown, nil
	default:
		return core.DurabilityUnknown, fmt.Errorf("fastdds: reverse durability %v is invalid", q.Durability)
	}
}

// reverseLiveliness maps FastDDS liveliness back to core liveliness.
func reverseLiveliness(q FastDDSQoS) (core.LivelinessPolicy, error) {
	switch q.Liveliness { //nolint:exhaustive // default rejects unknown wire kind with error
	case LivelinessAutomatic:
		return core.LivelinessAutomatic, nil
	case LivelinessManualByTopic:
		return core.LivelinessManualByTopic, nil
	default:
		return core.LivelinessUnknown, fmt.Errorf("fastdds: reverse liveliness %v is unknown", q.Liveliness)
	}
}

// reverseLease maps a FastDDS lease back to a core lease.
//
// LeaseIsDefault true maps to unspecified, pinning the FastDDS rule that
// unspecified stays DDS default instead of infinity. Otherwise DDS
// infinite maps to core infinite and finite values convert directly.
func reverseLease(q FastDDSQoS) (core.Duration, error) {
	if q.LeaseIsDefault {
		return core.UnspecifiedDuration(), nil
	}

	out, err := FastDDSDurationToCore(q.LeaseDuration)
	if err != nil {
		return core.Duration{}, fmt.Errorf("fastdds: reverse lease: %w", err)
	}

	return out, nil
}

// ReverseQoS translates a FastDDS QoS back to a core profile.
//
// Keep all maps directly, reliable and best effort map directly,
// transient and persistent durability become unknown, and durations
// convert with infinite mapping. A default lease maps to unspecified.
// The avoidance flag is never carried on FastDDS QoS and returns false.
// Unknown wire kinds map to core unknown markers instead of flowing
// silently, so Validate still rejects them loudly before entity creation.
func ReverseQoS(q FastDDSQoS) (core.QoSProfile, error) {
	history, depth, err := reverseHistory(q)
	if err != nil {
		return core.QoSProfile{}, err
	}

	reliability, err := reverseReliability(q)
	if err != nil {
		return core.QoSProfile{}, err
	}

	durability, err := reverseDurability(q)
	if err != nil {
		return core.QoSProfile{}, err
	}

	liveliness, err := reverseLiveliness(q)
	if err != nil {
		return core.QoSProfile{}, err
	}

	deadline, err := FastDDSDurationToCore(q.Deadline)
	if err != nil {
		return core.QoSProfile{}, fmt.Errorf("fastdds: reverse deadline: %w", err)
	}

	lifespan, err := FastDDSDurationToCore(q.Lifespan)
	if err != nil {
		return core.QoSProfile{}, fmt.Errorf("fastdds: reverse lifespan: %w", err)
	}

	lease, err := reverseLease(q)
	if err != nil {
		return core.QoSProfile{}, err
	}

	if !checkForcedRepresentation(q.DataRepresentation) {
		return core.QoSProfile{}, errors.New("fastdds: reverse representation is not XCDR1 only") //nolint:wrapcheck // new error, nothing to wrap
	}

	return core.QoSProfile{
		History:       history,
		Depth:         depth,
		Reliability:   reliability,
		Durability:    durability,
		Deadline:      deadline,
		Lifespan:      lifespan,
		Liveliness:    liveliness,
		LeaseDuration: lease,
	}, nil
}

// PublicationMode names the FastDDS publication mode knob.
type PublicationMode string

// FastDDS publication modes. Batching is XML only.
const (
	// PublicationModeSync sends each sample without batching.
	PublicationModeSync PublicationMode = "sync"
	// PublicationModeAsyncBatch batches samples before sending.
	PublicationModeAsyncBatch PublicationMode = "async_batch"
)

// DefaultPublicationMode returns the cross vendor publication mode.
//
// Code paths use sync. Async batching is an XML only knob and stays off
// on cross vendor paths so late joiner replay stays comparable.
func DefaultPublicationMode() PublicationMode {
	return PublicationModeSync
}

// LookupXMLProfile selects a per topic XML profile by topic name.
//
// An exact topic match wins. Service clients fall back to the client
// profile name when the topic has no entry. Other endpoints report not
// found. Profile names and topics trim surrounding whitespace; empty
// topics report not found without error.
func LookupXMLProfile(profiles map[string]core.QoSProfile, topic string, isServiceClient bool) (core.QoSProfile, bool) {
	trimmed := strings.TrimSpace(topic)
	if trimmed == "" {
		return core.QoSProfile{}, false
	}

	qos, ok := profiles[trimmed]
	if ok {
		return qos, true
	}

	if isServiceClient {
		fallback, found := profiles[ClientFallbackProfileName]
		if found {
			return fallback, true
		}
	}

	return core.QoSProfile{}, false
}

// EffectiveQoS applies the XML knob policy: code QoS always wins.
//
// XML QoS override skips code QoS entirely when enabled, and per topic
// XML profiles override by topic name with a client fallback for
// services. Weld keeps XML knobs off on cross vendor paths, so this
// helper returns code verbatim and reports whether an XML profile was
// present and ignored. Callers log the ignored XML profile with topic
// and client fallback state instead of applying it silently.
func EffectiveQoS(code, xml core.QoSProfile, xmlPresent bool) (core.QoSProfile, bool) {
	if !xmlPresent {
		return code, false
	}

	_ = xml

	return code, true
}

// Config configures the FastDDS stub Backend.
//
// DomainID selects the DDS domain. Range is explicit config only with
// no environment lookup beyond domain resolution. XMLProfiles carries
// optional per topic profiles selected by LookupXMLProfile; EffectiveQoS
// keeps code QoS winning so XML stays off on cross vendor paths.
// PublicationMode stays sync on cross vendor paths; async batching is an
// XML only knob.
type Config struct {
	// DomainID is the DDS domain the backend joins.
	DomainID uint32
	// Range is the discovery range for JoinDiscovery.
	Range transports.DiscoveryRange
	// XMLProfiles holds optional per topic XML profiles by topic name.
	XMLProfiles map[string]core.QoSProfile
	// PublicationMode selects sync or batched sends.
	PublicationMode PublicationMode
}

// DefaultConfig returns a Config with system default range and sync sends.
func DefaultConfig() Config {
	return Config{
		DomainID:        0,
		Range:           transports.DiscoveryRangeSystemDefault,
		PublicationMode: PublicationModeSync,
	}
}

// Validate checks the config for usable values.
func (c Config) Validate() error {
	if c.DomainID > transports.MaxDomainID {
		return fmt.Errorf("fastdds: domain id %d above maximum %d", c.DomainID, transports.MaxDomainID)
	}

	switch c.Range {
	case transports.DiscoveryRangeNotSet,
		transports.DiscoveryRangeOff,
		transports.DiscoveryRangeLocalhost,
		transports.DiscoveryRangeSubnet,
		transports.DiscoveryRangeSystemDefault:
	default:
		return fmt.Errorf("fastdds: unknown discovery range %v", c.Range)
	}

	switch c.PublicationMode {
	case PublicationModeSync, PublicationModeAsyncBatch, "":
	default:
		return fmt.Errorf("fastdds: unknown publication mode %q", c.PublicationMode)
	}

	return nil
}

// EffectivePublicationMode returns the configured mode or sync when empty.
func (c Config) EffectivePublicationMode() PublicationMode {
	if c.PublicationMode == "" {
		return PublicationModeSync
	}

	return c.PublicationMode
}

// DiscoveryTransportForConfig maps the config range to a FastDDS transport.
//
// NotSet normalizes to SystemDefault before mapping. Unknown ranges
// report an error.
func (c Config) DiscoveryTransportForConfig() (DiscoveryTransport, error) {
	normalized := NormalizeDiscoveryRange(c.Range)

	out, err := MapDiscoveryRange(normalized)
	if err != nil {
		return DiscoveryTransport{}, err
	}

	return out, nil
}

// Backend is the FastDDS lifecycle and mapping stub.
//
// Lifecycle follows the Backend contract: JoinDiscovery then the data
// path then Close. JoinDiscovery marks the backend joined without
// dialing. PublishBytes and TakeBytes keep lifecycle checks but report
// ErrFastDDSNotImplemented for the wire path until the FastDDS library
// lands. ReportLiveliness keeps the generation watermark so ordering
// tests pin behavior early. Data representation is forced to XCDR1 on
// every reader and writer. The struct is safe for concurrent use.
type Backend struct {
	mu sync.RWMutex
	// cfg is the normalized configuration.
	cfg Config
	// domainID is the resolved domain from DomainConfig.
	domainID uint32
	// joined is true after a successful JoinDiscovery.
	joined bool
	// closed is true after the first Close.
	closed bool
	// lastGen holds the liveliness watermark per participant.
	lastGen map[string]uint64
	// lastAlive holds the last aliveness per participant.
	lastAlive map[string]bool
}

// cloneXMLProfiles returns a copy of the XML profile map.
func cloneXMLProfiles(src map[string]core.QoSProfile) map[string]core.QoSProfile {
	if src == nil {
		return make(map[string]core.QoSProfile)
	}

	out := make(map[string]core.QoSProfile, len(src))
	maps.Copy(out, src)

	return out
}

// NewBackend validates the config and returns a stub Backend.
func NewBackend(cfg Config) (*Backend, error) {
	err := cfg.Validate()
	if err != nil {
		return nil, fmt.Errorf("fastdds: invalid backend config: %w", err)
	}

	resolved, err := transports.ResolveDomainID(cfg.DomainID)
	if err != nil {
		return nil, fmt.Errorf("fastdds: resolve domain: %w", err)
	}

	cfg.DomainID = resolved
	if cfg.Range == transports.DiscoveryRangeNotSet {
		cfg.Range = transports.DiscoveryRangeSystemDefault
	}

	if cfg.PublicationMode == "" {
		cfg.PublicationMode = PublicationModeSync
	}

	cfg.XMLProfiles = cloneXMLProfiles(cfg.XMLProfiles)

	return &Backend{
		cfg:       cfg,
		domainID:  resolved,
		lastGen:   make(map[string]uint64),
		lastAlive: make(map[string]bool),
	}, nil
}

// Config returns a copy of the backend configuration.
func (b *Backend) Config() Config {
	b.mu.RLock()
	defer b.mu.RUnlock()

	out := b.cfg
	out.XMLProfiles = cloneXMLProfiles(b.cfg.XMLProfiles)

	return out
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

// DataRepresentation returns the forced XCDR1 representation.
func (b *Backend) DataRepresentation() []DataRepresentation {
	return ForcedDataRepresentation()
}

// DiscoveryTransport returns the FastDDS transport for the stored range.
func (b *Backend) DiscoveryTransport() (DiscoveryTransport, error) {
	b.mu.RLock()
	cfg := b.cfg
	b.mu.RUnlock()

	out, err := cfg.DiscoveryTransportForConfig()
	if err != nil {
		return DiscoveryTransport{}, err
	}

	return out, nil
}

// JoinDiscovery marks the backend joined without dialing FastDDS.
//
// It honors ctx cancellation, reports ErrBackendClosed after Close, and
// is idempotent: a second join returns nil without changing state.
func (b *Backend) JoinDiscovery(ctx context.Context, _ transports.DiscoveryInfo) error {
	err := ctx.Err()
	if err != nil {
		return fmt.Errorf("fastdds: join discovery: %w", err)
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	if b.closed {
		return fmt.Errorf("fastdds: join discovery after close: %w", transports.ErrBackendClosed)
	}

	b.joined = true

	return nil
}

// PublishBytes keeps lifecycle checks and reports not implemented.
//
// It honors ctx cancellation, requires a prior JoinDiscovery, reports
// ErrBackendClosed after Close, copies the payload to honor copy once,
// and then reports ErrFastDDSNotImplemented because the FastDDS dial has
// not landed. Data representation stays forced to XCDR1.
func (b *Backend) PublishBytes(ctx context.Context, msg transports.OutgoingMessage) error {
	err := ctx.Err()
	if err != nil {
		return fmt.Errorf("fastdds: publish bytes: %w", err)
	}

	b.mu.RLock()
	closed := b.closed
	joined := b.joined
	b.mu.RUnlock()

	if closed {
		return fmt.Errorf("fastdds: publish bytes after close: %w", transports.ErrBackendClosed)
	}

	if !joined {
		return fmt.Errorf("fastdds: publish bytes before join: %w", transports.ErrNotJoined)
	}

	if len(msg.Payload) != 0 {
		_ = append([]byte(nil), msg.Payload...)
	}

	return fmt.Errorf("fastdds: publish bytes on %q: %w", msg.TopicName, ErrFastDDSNotImplemented)
}

// TakeBytes keeps lifecycle checks and reports not implemented.
//
// It honors ctx cancellation, requires a prior JoinDiscovery, reports
// ErrBackendClosed after Close, and then reports
// ErrFastDDSNotImplemented because the FastDDS dial has not landed.
func (b *Backend) TakeBytes(ctx context.Context) (transports.IncomingMessage, error) {
	err := ctx.Err()
	if err != nil {
		return transports.IncomingMessage{}, fmt.Errorf("fastdds: take bytes: %w", err)
	}

	b.mu.RLock()
	closed := b.closed
	joined := b.joined
	b.mu.RUnlock()

	if closed {
		return transports.IncomingMessage{}, fmt.Errorf("fastdds: take bytes after close: %w", transports.ErrBackendClosed)
	}

	if !joined {
		return transports.IncomingMessage{}, fmt.Errorf("fastdds: take bytes before join: %w", transports.ErrNotJoined)
	}

	return transports.IncomingMessage{}, fmt.Errorf("fastdds: take bytes: %w", ErrFastDDSNotImplemented)
}

// ReportLiveliness records the generation watermark for one participant.
//
// The first report for a name is accepted. A greater generation advances
// the watermark. A repeated generation with the same alive value is
// idempotent. A repeated generation with a different alive value and any
// smaller generation report ErrStaleGeneration. It requires a prior
// JoinDiscovery and reports ErrBackendClosed after Close.
func (b *Backend) ReportLiveliness(ctx context.Context, report transports.LivelinessReport) error {
	err := ctx.Err()
	if err != nil {
		return fmt.Errorf("fastdds: report liveliness: %w", err)
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	if b.closed {
		return fmt.Errorf("fastdds: report liveliness after close: %w", transports.ErrBackendClosed)
	}

	if !b.joined {
		return fmt.Errorf("fastdds: report liveliness before join: %w", transports.ErrNotJoined)
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

	return fmt.Errorf(
		"fastdds: report liveliness generation %d for %q: %w",
		report.Generation,
		report.ParticipantName,
		transports.ErrStaleGeneration,
	)
}

// Close releases the backend handle.
//
// It is idempotent. The first call marks the backend closed, later calls
// return nil. No FastDDS connection exists yet so there is nothing to shut
// beyond the flag.
func (b *Backend) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.closed = true

	return nil
}
