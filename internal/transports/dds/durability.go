// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

// Durability-service QoS plus depth normalization for CycloneDDS interop.
//
// Weld advertises transient-local depth 100 on the wire (see
// FacadeHistoryDepth in internal/core/facade.go) while CycloneDDS uses
// depth 1 and negotiates durability-service (history kind and depth plus
// resource limits). Late-joiner replay therefore differs cross vendor.
// This file owns the durability-service parameter value codec with a
// Cyclone-compatible layout, the requested to effective depth mapping per
// peer vendor, the late-joiner replay bound, and the offered versus
// requested durability-service check.
//
// The codec covers the parameter value only. Wiring the parameter into
// SEDP announce and parse, the matcher, and the actual-QoS getters is
// staged through the INTEGRATION hooks at the bottom of this file.
package dds

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/podomy/weld/internal/transports/dds/rtps"
)

// DDSI-RTPS durability-service parameter id.
//
// Value 0x001e is the spec DURABILITY_SERVICE PID. It lives here instead
// of rtps/pid.go because existing files are frozen; see INTEGRATION.
const PIDDurabilityService uint16 = 0x001e

// Durability-service wire layout constants.
//
// The parameter value is 28 bytes: cleanup delay seconds plus fraction,
// history kind plus depth, max samples, max instances, and max samples
// per instance, each 32 bits in the param endianness.
const (
	// durabilityServiceValueLen is the fixed 28 byte parameter value length.
	durabilityServiceValueLen = 28
	// durabilityServiceHistoryKeepLast is the history kind for keep last.
	durabilityServiceHistoryKeepLast uint32 = 0
	// durabilityServiceHistoryKeepAll is the history kind for keep all.
	durabilityServiceHistoryKeepAll uint32 = 1
	// durabilityServiceUnlimited marks an unlimited resource limit, -1.
	durabilityServiceUnlimited int32 = -1
	// durabilityKindTransient is the DDS durability kind for transient.
	durabilityKindTransient uint32 = 2
	// durabilityKindPersistent is the DDS durability kind for persistent.
	durabilityKindPersistent uint32 = 3
	// durabilityServiceInfiniteSec marks an infinite delay seconds field.
	durabilityServiceInfiniteSec uint32 = 0x7FFFFFFF
	// durabilityServiceInfiniteFrac marks an infinite delay fraction field.
	durabilityServiceInfiniteFrac uint32 = 0xFFFFFFFF
)

// Vendor unset-depth defaults.
//
// An unset keep-last depth (zero) means something different per stack:
// CycloneDDS maps it to 1 and Zenoh maps it to 42, while core normalizes
// to 10 before entity creation (see DefaultHistoryDepth in
// internal/core/matcher.go) and the Weld facade advertises 100 (see
// FacadeHistoryDepth in internal/core/facade.go).
const (
	// CycloneUnsetDepth is the keep-last depth CycloneDDS assumes when unset.
	CycloneUnsetDepth uint64 = 1
	// WeldCoreUnsetDepth is the depth core normalizes an unset value to.
	WeldCoreUnsetDepth uint64 = 10
	// WeldFacadeDepth is the depth the Weld facade advertises on the wire.
	WeldFacadeDepth uint64 = 100
	// ZenohUnsetDepth is the keep-last depth Zenoh assumes when unset.
	ZenohUnsetDepth uint64 = 42
	// UnknownVendorUnsetDepth is the fallback for unrecognized vendors.
	//
	// It matches the core normalized default so unknown peers converge
	// with local entities instead of with any one vendor extreme.
	UnknownVendorUnsetDepth uint64 = 10
)

// Sentinel errors for durability-service failures.
var (
	// ErrDurabilityService reports a malformed durability-service value.
	ErrDurabilityService = errors.New("dds: invalid durability service")
)

// DurabilityServiceQoS is the durability-service QoS policy.
//
// CleanupDelay is the service cleanup delay as seconds plus fraction.
// HistoryKind is keep last (0) or keep all (1). HistoryDepth bounds keep
// last. The three resource limits use -1 for unlimited and zero for
// unstated, meaning the peer omitted the parameter and imposes no cap;
// only positive values constrain replay. Durability-service constrains
// transient and persistent durability only; transient-local replay is
// bounded by writer history depth per the DDSI-RTPS spec.
type DurabilityServiceQoS struct {
	// CleanupDelaySec is the service cleanup delay seconds.
	CleanupDelaySec int32
	// CleanupDelayFrac is the service cleanup delay fraction.
	CleanupDelayFrac uint32
	// HistoryKind is keep last (0) or keep all (1).
	HistoryKind uint32
	// HistoryDepth bounds keep last; ignored for keep all.
	HistoryDepth int32
	// MaxSamples caps total samples; -1 unlimited, 0 unstated.
	MaxSamples int32
	// MaxInstances caps instances; -1 unlimited, 0 unstated.
	MaxInstances int32
	// MaxSamplesPerInstance caps samples per instance; -1 unlimited, 0 unstated.
	MaxSamplesPerInstance int32
}

// DefaultDurabilityServiceQoS returns Cyclone-compatible defaults.
//
// Cleanup delay zero, keep-last history depth 1, and unlimited resource
// limits match the CycloneDDS default QoS.
func DefaultDurabilityServiceQoS() DurabilityServiceQoS {
	return DurabilityServiceQoS{
		HistoryKind:           durabilityServiceHistoryKeepLast,
		HistoryDepth:          1,
		MaxSamples:            durabilityServiceUnlimited,
		MaxInstances:          durabilityServiceUnlimited,
		MaxSamplesPerInstance: durabilityServiceUnlimited,
	}
}

// WeldDurabilityServiceQoS returns the durability-service offer matching
// the Weld facade: Cyclone-compatible defaults with history depth 100.
func WeldDurabilityServiceQoS() DurabilityServiceQoS {
	out := DefaultDurabilityServiceQoS()
	out.HistoryDepth = int32(WeldFacadeDepth)

	return out
}

// Validate rejects an offer that must never reach the wire.
func (q DurabilityServiceQoS) Validate() error {
	if q.HistoryKind != durabilityServiceHistoryKeepLast && q.HistoryKind != durabilityServiceHistoryKeepAll {
		return fmt.Errorf("dds: durability service history kind %d: %w", q.HistoryKind, ErrDurabilityService)
	}

	if q.HistoryKind == durabilityServiceHistoryKeepLast && q.HistoryDepth < 1 {
		return fmt.Errorf("dds: durability service keep last depth %d: %w", q.HistoryDepth, ErrDurabilityService)
	}

	if q.CleanupDelaySec < 0 {
		return fmt.Errorf("dds: durability service cleanup delay seconds %d: %w", q.CleanupDelaySec, ErrDurabilityService)
	}

	for _, limit := range []struct {
		name  string
		value int32
	}{
		{"max_samples", q.MaxSamples},
		{"max_instances", q.MaxInstances},
		{"max_samples_per_instance", q.MaxSamplesPerInstance},
	} {
		if limit.value < -1 {
			return fmt.Errorf("dds: durability service %s %d: %w", limit.name, limit.value, ErrDurabilityService)
		}
	}

	return nil
}

// EncodeDurabilityService renders the 28 byte little endian value.
func EncodeDurabilityService(q DurabilityServiceQoS) ([]byte, error) {
	err := q.Validate()
	if err != nil {
		return nil, fmt.Errorf("dds: encode durability service: %w", err)
	}

	out := make([]byte, 0, durabilityServiceValueLen)
	out = binary.LittleEndian.AppendUint32(out, uint32(q.CleanupDelaySec)) //nolint:gosec // G115: validated non-negative seconds reinterpret for the wire
	out = binary.LittleEndian.AppendUint32(out, q.CleanupDelayFrac)
	out = binary.LittleEndian.AppendUint32(out, q.HistoryKind)
	out = binary.LittleEndian.AppendUint32(out, uint32(q.HistoryDepth))          //nolint:gosec // G115: validated positive depth reinterprets for the wire
	out = binary.LittleEndian.AppendUint32(out, uint32(q.MaxSamples))            //nolint:gosec // G115: validated limit reinterprets for the wire
	out = binary.LittleEndian.AppendUint32(out, uint32(q.MaxInstances))          //nolint:gosec // G115: validated limit reinterprets for the wire
	out = binary.LittleEndian.AppendUint32(out, uint32(q.MaxSamplesPerInstance)) //nolint:gosec // G115: validated limit reinterprets for the wire

	return out, nil
}

// DecodeDurabilityService parses one 28 byte parameter value.
//
// The value must be exactly 28 bytes; PID padding never belongs to the
// value because rtps decode strips it. Decoding stays tolerant: no
// semantic validation runs here, so peers with odd but parseable values
// still match through CheckDurabilityServiceCompat instead of dropping
// discovery.
func DecodeDurabilityService(value []byte, littleEndian bool) (DurabilityServiceQoS, error) {
	var q DurabilityServiceQoS

	if len(value) != durabilityServiceValueLen {
		return q, fmt.Errorf("dds: durability service length %d, want %d: %w", len(value), durabilityServiceValueLen, ErrDurabilityService)
	}

	var order func([]byte) uint32
	if littleEndian {
		order = binary.LittleEndian.Uint32
	} else {
		order = binary.BigEndian.Uint32
	}

	ints := func(off int) int32 {
		return int32(order(value[off : off+4])) //nolint:gosec // G115: intentional two's complement reinterpret from the wire
	}

	q = DurabilityServiceQoS{
		CleanupDelaySec:       ints(0),
		CleanupDelayFrac:      order(value[4:8]),
		HistoryKind:           order(value[8:12]),
		HistoryDepth:          ints(12),
		MaxSamples:            ints(16),
		MaxInstances:          ints(20),
		MaxSamplesPerInstance: ints(24),
	}

	return q, nil
}

// DurabilityServiceParam wraps an offer as one RTPS SEDP parameter.
//
// It is the one-line call site for the sedp.go announce path; see
// INTEGRATION.
func DurabilityServiceParam(q DurabilityServiceQoS) (rtps.Param, error) {
	value, err := EncodeDurabilityService(q)
	if err != nil {
		return rtps.Param{}, fmt.Errorf("dds: durability service param: %w", err)
	}

	return rtps.Param{ID: PIDDurabilityService, Value: value}, nil
}

// PeerVendor names a DDS peer family with a known unset-depth default.
type PeerVendor int

// Peer vendor families.
const (
	// VendorUnknown covers unrecognized peers; defaults to core convergence.
	VendorUnknown PeerVendor = iota
	// VendorCyclone is Eclipse CycloneDDS with unset depth 1.
	VendorCyclone
	// VendorWeldCore is the normalized core default depth 10.
	VendorWeldCore
	// VendorWeldFacade is the advertised facade depth 100.
	VendorWeldFacade
	// VendorZenoh is Zenoh with unset depth 42.
	VendorZenoh
)

// String renders the vendor in lowercase form.
func (v PeerVendor) String() string {
	switch v {
	case VendorCyclone:
		return "cyclone"
	case VendorWeldCore:
		return "weld_core"
	case VendorWeldFacade:
		return "weld_facade"
	case VendorZenoh:
		return "zenoh"
	case VendorUnknown:
		return "unknown"
	default:
		return "unknown"
	}
}

// DefaultDepth returns the keep-last depth the vendor assumes when unset.
func (v PeerVendor) DefaultDepth() uint64 {
	switch v {
	case VendorCyclone:
		return CycloneUnsetDepth
	case VendorWeldCore:
		return WeldCoreUnsetDepth
	case VendorWeldFacade:
		return WeldFacadeDepth
	case VendorZenoh:
		return ZenohUnsetDepth
	case VendorUnknown:
		return UnknownVendorUnsetDepth
	default:
		return UnknownVendorUnsetDepth
	}
}

// NormalizeDepthForVendor maps a requested depth to the effective wire
// depth for one peer vendor.
//
// A set depth passes through untouched. An unset depth (zero) resolves to
// the vendor default, so Cyclone gets 1, core gets 10, the facade gets
// 100, and Zenoh gets 42. Normalization never invents a smaller promise
// for a set depth; peer side shortfalls surface through NegotiateDepth.
func NormalizeDepthForVendor(requested uint64, vendor PeerVendor) uint64 {
	if requested != 0 {
		return requested
	}

	return vendor.DefaultDepth()
}

// EffectiveDepthReport is the actual-getter shaped record for one depth
// negotiation.
//
// It mirrors the ActualQoS getter shape (requested plus resolved profile)
// without touching endpoint.go: RequestedDepth and PeerDepth carry the
// raw inputs, ResolvedRequested and ResolvedPeer carry the unset-resolved
// values, EffectiveDepth carries the negotiated bound, and Reduced flags
// a silent-looking guarantee cut loudly instead of hiding it.
type EffectiveDepthReport struct {
	// Vendor is the peer vendor whose default resolved unset values.
	Vendor PeerVendor
	// RequestedDepth is the raw local request; zero means unset.
	RequestedDepth uint64
	// PeerDepth is the raw peer depth; zero means unset.
	PeerDepth uint64
	// ResolvedRequested is the request with unset mapped to core default.
	ResolvedRequested uint64
	// ResolvedPeer is the peer depth with unset mapped to vendor default.
	ResolvedPeer uint64
	// EffectiveDepth is the negotiated bound both sides can meet.
	EffectiveDepth uint64
	// Reduced is true when the bound sits below an explicit request.
	Reduced bool
}

// NegotiateDepth resolves requested and peer depths to one bound.
//
// Unset local depth resolves to the core default and unset peer depth to
// the peer vendor default. The bound is the minimum of both. Reduced is
// true only for an explicit request cut by a smaller peer, so callers
// surface the cut through actual-QoS shaped getters instead of silently
// keeping less history than promised. Zero means unstated per the
// DurabilityServiceQoS struct contract, where only positive values
// constrain; resolveWriterDepth floors the same unset marker to 1 for
// replay bounds while negotiation resolves it to vendor defaults. The
// two resolutions disagree on purpose because they answer different
// questions (negotiated promise versus replay floor), so neither path
// changes the other.
func NegotiateDepth(requestedDepth, peerDepth uint64, peerVendor PeerVendor) EffectiveDepthReport {
	resolvedRequested := requestedDepth
	if resolvedRequested == 0 {
		resolvedRequested = WeldCoreUnsetDepth
	}

	resolvedPeer := peerDepth
	if resolvedPeer == 0 {
		resolvedPeer = peerVendor.DefaultDepth()
	}

	effective := resolvedRequested
	effective = min(effective, resolvedPeer)

	report := EffectiveDepthReport{
		Vendor:            peerVendor,
		RequestedDepth:    requestedDepth,
		PeerDepth:         peerDepth,
		ResolvedRequested: resolvedRequested,
		ResolvedPeer:      resolvedPeer,
		EffectiveDepth:    effective,
	}

	if requestedDepth != 0 && effective < requestedDepth {
		report.Reduced = true
	}

	return report
}

// ReplayBound computes how many samples a late joiner receives.
//
// Published counts samples the writer sent. WriterDepth is the writer
// keep-last depth with zero meaning unset, floored to the smallest vendor
// guarantee of 1. Volatile writers keep nothing and always yield zero.
// Transient-local replay is bounded by writer history depth only;
// durability-service caps constrain transient and persistent durability
// per the DDSI-RTPS spec and never transient-local, so caps pass through
// untouched for kind 1. Unknown durability kinds yield zero fail-closed:
// no replay is promised for an unrecognized kind.
//
// Caps with keep-all history apply max samples, max instances, and max
// samples per instance when positive. Caps with keep-last history also
// apply the caps history depth when positive. Zero caps fields mean
// unstated (the peer omitted the parameter, as Weld does today) and
// impose no bound; -1 means unlimited. Zero here follows the
// DurabilityServiceQoS struct contract where 0 is unstated and only
// positive values constrain; see CheckDurabilityServiceCompat for the
// same rule on the matching path. This is the keyless single-instance
// entry: it assumes one instance so per-instance caps bound the total.
// Keyed topics must use ReplayBoundWithInstances with the live instance
// count instead of sharing this total.
func ReplayBound(published, writerDepth uint64, durabilityKind uint32, caps DurabilityServiceQoS) uint64 {
	return ReplayBoundWithInstances(published, writerDepth, durabilityKind, caps, 1)
}

// ReplayBoundWithInstances computes late-joiner replay for an explicit instance count.
//
// InstanceCount names distinct keyed instances in history; zero means one
// for keyless callers. One instance returns exactly ReplayBound so keyless
// behavior never changes. More instances scale per-instance caps instead of
// applying them to the total: max samples per instance multiplies by the
// effective instance total, max instances caps the instance total, and max
// samples still caps the grand total. Unstated (0) and unlimited (-1) keep
// the struct contract.
func ReplayBoundWithInstances(published, writerDepth uint64, durabilityKind uint32, caps DurabilityServiceQoS, instanceCount uint64) uint64 {
	if instanceCount == 0 {
		instanceCount = 1
	}

	if instanceCount == 1 {
		return replayBoundKeyless(published, writerDepth, durabilityKind, caps)
	}

	if durabilityKind == sedpDurabilityVolatile {
		return 0
	}

	if durabilityKind == sedpDurabilityTransientLocal {
		return min(published, resolveWriterDepth(writerDepth)*instanceCount)
	}

	if durabilityKind != durabilityKindTransient && durabilityKind != durabilityKindPersistent {
		return 0
	}

	perInstance := resolveWriterDepth(writerDepth)

	if caps.HistoryKind == durabilityServiceHistoryKeepLast && caps.HistoryDepth > 0 {
		perInstance = min(perInstance, uint64(caps.HistoryDepth))
	}

	if caps.MaxSamplesPerInstance > 0 {
		perInstance = min(perInstance, uint64(caps.MaxSamplesPerInstance))
	}

	effectiveInstances := instanceCount
	if caps.MaxInstances > 0 {
		effectiveInstances = min(effectiveInstances, uint64(caps.MaxInstances))
	}

	bound := effectiveInstances * perInstance
	bound = min(bound, published)

	if caps.MaxSamples > 0 {
		bound = min(bound, uint64(caps.MaxSamples))
	}

	return bound
}

// replayBoundKeyless is the single-instance bound preserved for keyless topics.
func replayBoundKeyless(published, writerDepth uint64, durabilityKind uint32, caps DurabilityServiceQoS) uint64 {
	if durabilityKind == sedpDurabilityVolatile {
		return 0
	}

	if durabilityKind == sedpDurabilityTransientLocal {
		return min(published, resolveWriterDepth(writerDepth))
	}

	if durabilityKind != durabilityKindTransient && durabilityKind != durabilityKindPersistent {
		return 0
	}

	bound := min(published, resolveWriterDepth(writerDepth))

	if caps.HistoryKind == durabilityServiceHistoryKeepLast && caps.HistoryDepth > 0 {
		bound = min(bound, uint64(caps.HistoryDepth))
	}

	if caps.MaxSamples > 0 {
		bound = min(bound, uint64(caps.MaxSamples))
	}

	if caps.MaxInstances > 0 {
		// Keyless topics carry one instance, so the instance cap
		// bounds the whole replay like the per-instance cap below.
		bound = min(bound, uint64(caps.MaxInstances))
	}

	if caps.MaxSamplesPerInstance > 0 {
		// Keyless topics carry one instance, so the per-instance cap
		// bounds the whole replay.
		bound = min(bound, uint64(caps.MaxSamplesPerInstance))
	}

	return bound
}

// resolveWriterDepth floors an unset writer depth to the smallest vendor
// guarantee so an unset depth never promises zero replay. Zero means
// unstated per the DurabilityServiceQoS struct contract; NegotiateDepth
// resolves the same marker to vendor defaults for matching while this
// floors it to 1 for replay. The disagreement is intentional and neither
// bound nor matching changes.
func resolveWriterDepth(depth uint64) uint64 {
	if depth == 0 {
		return CycloneUnsetDepth
	}

	return depth
}

// DurabilityServiceResult is the verdict of one offered versus requested
// durability-service check.
type DurabilityServiceResult struct {
	// Compatible is true when the offer satisfies the request.
	Compatible bool
	// Reason is empty when compatible and names the first mismatch else.
	Reason string
}

// CheckDurabilityServiceCompat checks offered versus requested
// durability-service without touching the core matcher.
//
// Every mismatch carries a distinct reason string so wiring can surface
// which bound failed. Cleanup delay requests must not exceed the offer,
// with an infinite offer satisfying any request. Keep-all history
// requests need a keep-all offer. Keep-last depth requests must not
// exceed a keep-last offer; a keep-all offer satisfies any depth. A zero
// keep-last depth on either side means unstated per the
// DurabilityServiceQoS struct contract and imposes no constraint, so it
// never mismatches here and matching stays unchanged. Each
// resource limit request must not exceed the offer, where unlimited (-1)
// needs unlimited and unstated (0) imposes no constraint on either side.
func CheckDurabilityServiceCompat(offered, requested DurabilityServiceQoS) DurabilityServiceResult {
	if !cleanupDelaySatisfies(offered, requested) {
		return mismatch("durability_service: requested service_cleanup_delay %s exceeds offered %s",
			formatCleanupDelay(requested), formatCleanupDelay(offered))
	}

	if requested.HistoryKind == durabilityServiceHistoryKeepAll && offered.HistoryKind != durabilityServiceHistoryKeepAll {
		return mismatch("durability_service: requested keep_all history exceeds offered keep_last")
	}

	if requested.HistoryKind == durabilityServiceHistoryKeepLast &&
		offered.HistoryKind == durabilityServiceHistoryKeepLast &&
		requested.HistoryDepth > 0 && offered.HistoryDepth > 0 &&
		requested.HistoryDepth > offered.HistoryDepth {
		return mismatch("durability_service: requested history depth %d exceeds offered %d",
			requested.HistoryDepth, offered.HistoryDepth)
	}

	if reason := checkResourceLimit("max_samples", offered.MaxSamples, requested.MaxSamples); reason != "" {
		return DurabilityServiceResult{Reason: reason}
	}

	if reason := checkResourceLimit("max_instances", offered.MaxInstances, requested.MaxInstances); reason != "" {
		return DurabilityServiceResult{Reason: reason}
	}

	if reason := checkResourceLimit("max_samples_per_instance", offered.MaxSamplesPerInstance, requested.MaxSamplesPerInstance); reason != "" {
		return DurabilityServiceResult{Reason: reason}
	}

	return DurabilityServiceResult{Compatible: true}
}

// mismatch builds one incompatible result with a formatted reason.
func mismatch(format string, args ...any) DurabilityServiceResult {
	return DurabilityServiceResult{Reason: fmt.Sprintf(format, args...)}
}

// cleanupDelaySatisfies reports whether the offered cleanup delay meets
// the request. An infinite offer satisfies anything; an infinite request
// needs an infinite offer; else compare seconds then fraction.
func cleanupDelaySatisfies(offered, requested DurabilityServiceQoS) bool {
	if isInfiniteCleanupDelay(offered) {
		return true
	}

	if isInfiniteCleanupDelay(requested) {
		return false
	}

	if requested.CleanupDelaySec != offered.CleanupDelaySec {
		return requested.CleanupDelaySec < offered.CleanupDelaySec
	}

	return requested.CleanupDelayFrac <= offered.CleanupDelayFrac
}

// isInfiniteCleanupDelay reports whether the delay is the wire infinite.
func isInfiniteCleanupDelay(q DurabilityServiceQoS) bool {
	return uint32(q.CleanupDelaySec) == durabilityServiceInfiniteSec && q.CleanupDelayFrac == durabilityServiceInfiniteFrac //nolint:gosec // G115: two's complement reinterpret matches the wire infinite
}

// formatCleanupDelay renders a delay as seconds plus fraction.
func formatCleanupDelay(q DurabilityServiceQoS) string {
	if isInfiniteCleanupDelay(q) {
		return "infinite"
	}

	return fmt.Sprintf("%ds+%d", q.CleanupDelaySec, q.CleanupDelayFrac)
}

// checkResourceLimit checks one requested limit against the offer.
//
// Unstated (0) on either side imposes no constraint. Unlimited (-1)
// requested needs unlimited offered. Else the request must not exceed a
// positive offer. It returns the mismatch reason or empty when satisfied.
func checkResourceLimit(name string, offered, requested int32) string {
	if offered == 0 || requested == 0 {
		return ""
	}

	if requested == durabilityServiceUnlimited && offered != durabilityServiceUnlimited {
		return fmt.Sprintf("durability_service: requested %s unlimited exceeds offered %d", name, offered)
	}

	if offered != durabilityServiceUnlimited && requested > offered {
		return fmt.Sprintf("durability_service: requested %s %d exceeds offered %d", name, requested, offered)
	}

	return ""
}

// INTEGRATION state for durability-service wiring:
//
//  1. DONE sedp.go announce: DurabilityServiceParam(WeldDurabilityServiceQoS())
//     is appended in SerializeSEDPDATA, so writers advertise history
//     depth 100 consistently with the facade.
//  2. DONE sedp.go parse: PIDDurabilityService is read in sedpEndpointFields via
//     DecodeDurabilityService and carried on SEDPEndpoint, so late
//     joiners learn the peer caps that bound ReplayBound.
//  3. DONE replay bound: ReplayBoundWithInstances threads an explicit
//     instance count so per-instance caps scale instead of capping the
//     total; ReplayBound stays as the keyless single-instance wrapper with
//     identical behavior. The single reliability.go caller still passes the
//     keyless path (read only here); keyed callers pass the live count next.
//  4. matcher.go extension: call CheckDurabilityServiceCompat from the
//     endpoint pair path next to CheckCompatibility
//     (internal/core/matcher.go:234) once SEDPEndpoint carries both sides;
//     history, depth, and lifespan stay RxO neutral there as today.
//  5. actual-QoS getter: surface NegotiateDepth through an EffectiveDepth
//     field on the endpoint actual profile shape (see ActualQoS in
//     internal/core/endpoint.go:169) without changing RequestedQoS.
