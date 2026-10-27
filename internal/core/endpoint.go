// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package core

import (
	"errors"
	"fmt"
	"sync/atomic"
)

// EndpointKind names the direction of an endpoint for best available resolution.
type EndpointKind int

// Endpoint kinds.
const (
	// EndpointPublisher resolves best available as an offer.
	EndpointPublisher EndpointKind = iota
	// EndpointSubscription resolves best available as a request.
	EndpointSubscription
)

// String renders the kind in lowercase form.
func (k EndpointKind) String() string {
	switch k {
	case EndpointPublisher:
		return "publisher"
	case EndpointSubscription:
		return "subscription"
	default:
		return "invalid"
	}
}

// Peer is one endpoint visible at best available resolution time.
type Peer struct {
	// Name identifies the visible peer, such as its endpoint and node name.
	Name string
	// Profile is the peer resolved QoS profile. Unresolved markers count as
	// non concrete and fall to the else branch of each rule.
	Profile QoSProfile
}

// Endpoint is a created topic endpoint with its resolved QoS.
//
// Best available resolves once at creation from the peers visible then. The
// stored profile is exposed through ActualQoS, the creation time peer set
// through VisiblePeers, and later discovery never updates either: observe it
// through ObserveDiscovery, which only counts observations. The zero value
// is usable for getters (empty names, no peers, zero count) but not for
// creation; build endpoints with CreateEndpoint. CreateEndpoint returns an
// Endpoint value by frozen signature (returning *Endpoint is not allowed),
// so do not copy an endpoint after first use: the struct embeds noCopy and
// vet copylocks flags copies; use a pointer. VisiblePeers still returns a
// fresh slice copy so caller mutation cannot rewrite history.
type Endpoint struct {
	noCopy    noCopy
	topic     string
	name      string
	kind      EndpointKind
	service   bool
	requested QoSProfile
	actual    QoSProfile
	visible   []Peer
	laterSeen atomic.Pointer[atomic.Uint64]
}

// CreateEndpoint resolves requested (including best available and system
// defaults) once from the currently visible peers and stores the result.
//
// Resolution order: validate topic, name, and kind; resolve system defaults;
// when the endpoint serves a service and any best available marker is set,
// replace only the marker fields with the services default, preserving
// explicit non marker fields; otherwise resolve each best available marker
// from visible peers per the rules below; reject any surviving duration
// sentinel locally because Validate cannot see it; then Validate the stored
// profile. The visible slice is copied, so later caller mutation cannot
// rewrite history.
//
// Subscriber best available: reliable exactly when at least one peer exists
// and all are reliable, else best effort; volatile unless at least one peer
// exists and all are transient local, else volatile; automatic unless at
// least one peer exists and all are manual, else automatic; deadline plus
// lease default when all peers are default, else the maximum; lifespan
// always unspecified because lifespan is publisher local expiry, not a
// negotiated request. Publisher best available: always reliable plus always
// transient local; manual when any subscriber is manual, else automatic;
// deadline plus lease default when all peers are default, else the minimum;
// lifespan always unspecified. Durations treat unspecified and unresolved
// sentinels as infinite for the extremum and normalize an infinite result
// back to unspecified, so a mixed default plus finite peer set collapses to
// default on request and to the finite minimum on offer.
//
// With zero visible peers the empty guards return false, so durability
// resolves volatile and liveliness resolves automatic, matching the
// best effort reliability rule that already requires at least one peer.
// The universal rules therefore read with an explicit non empty guard:
// for all peers P, P concrete implies the value, and empty yields the else
// branch. Later discovery never revisits these answers; see
// ObserveDiscovery.
func CreateEndpoint(topic, name string, kind EndpointKind, requested QoSProfile, visible []Peer, service bool) (Endpoint, error) {
	if topic == "" {
		return Endpoint{}, errors.New("core: create endpoint: topic name is empty") //nolint:wrapcheck // new error, nothing to wrap
	}
	if name == "" {
		return Endpoint{}, fmt.Errorf("core: create endpoint on %q: endpoint name is empty", topic)
	}
	switch kind {
	case EndpointPublisher, EndpointSubscription:
	default:
		return Endpoint{}, fmt.Errorf("core: create endpoint %q on %q: invalid endpoint kind %d", name, topic, kind)
	}
	resolved := ResolveSystemDefaults(requested)
	var actual QoSProfile
	if service && hasBestAvailable(requested) {
		actual = resolveServiceBestAvailable(requested, resolved)
	} else {
		actual = resolveBestAvailable(kind, resolved, visible)
	}
	if IsBestAvailableDuration(actual.Deadline) || IsBestAvailableDuration(actual.LeaseDuration) || IsBestAvailableDuration(actual.Lifespan) {
		return Endpoint{}, fmt.Errorf("core: create endpoint %q on %q: best available duration sentinel survived resolution", name, topic)
	}
	err := Validate(actual)
	if err != nil {
		return Endpoint{}, fmt.Errorf("core: create endpoint %q on %q: %w", name, topic, err)
	}
	seen := make([]Peer, len(visible))
	copy(seen, visible)
	return Endpoint{
		topic:     topic,
		name:      name,
		kind:      kind,
		service:   service,
		requested: requested,
		actual:    actual,
		visible:   seen,
	}, nil
}

// Topic returns the endpoint topic name.
func (e *Endpoint) Topic() string {
	return e.topic
}

// Name returns the endpoint name.
func (e *Endpoint) Name() string {
	return e.name
}

// Kind returns the endpoint direction.
func (e *Endpoint) Kind() EndpointKind {
	return e.kind
}

// IsService reports whether the endpoint serves a service.
func (e *Endpoint) IsService() bool {
	return e.service
}

// RequestedQoS returns the profile as configured, markers included.
func (e *Endpoint) RequestedQoS() QoSProfile {
	return e.requested
}

// ActualQoS returns the resolved profile stored at creation.
//
// System defaults are concrete here; the avoidance flag passes through
// unresolved per the actual getter rule.
func (e *Endpoint) ActualQoS() QoSProfile {
	return e.actual
}

// VisiblePeers returns a copy of the peer set recorded at creation time.
//
// The slice is copied so caller mutation cannot rewrite history. The copy
// is shallow: Peer holds a value profile, so element mutation through the
// returned slice never aliases internal state, but callers must still treat
// the returned slice as owned output.
func (e *Endpoint) VisiblePeers() []Peer {
	out := make([]Peer, len(e.visible))
	copy(out, e.visible)
	return out
}

// LaterDiscoveryCount returns how many discovery observations arrived after
// creation. The count is introspection only; the stored profile never moves.
// The zero value reports zero. A nil receiver reports zero.
func (e *Endpoint) LaterDiscoveryCount() uint64 {
	if e == nil {
		return 0
	}
	seen := e.laterSeen.Load()
	if seen == nil {
		return 0
	}
	return seen.Load()
}

// ObserveDiscovery records one post creation discovery observation without
// changing the stored profile or the creation time peer set.
//
// It exists so tests and operators can prove stability: ActualQoS and
// VisiblePeers are identical before and after any number of observations.
// The peer slice argument is accepted for API stability and ignored; only
// the count advances. The receiver must be addressable and must not be
// copied after first use. The count uses atomic.Pointer CAS for lock free
// concurrent init, safe for concurrent use including the zero value; a nil
// receiver is a no op matching MismatchCounter.Record.
func (e *Endpoint) ObserveDiscovery(_ []Peer) {
	if e == nil {
		return
	}
	seen := e.laterSeen.Load()
	if seen == nil {
		fresh := &atomic.Uint64{}
		if e.laterSeen.CompareAndSwap(nil, fresh) {
			seen = fresh
		} else {
			seen = e.laterSeen.Load()
		}
	}
	if seen == nil {
		return
	}
	seen.Add(1)
}

// hasBestAvailable reports whether p carries any best available marker,
// including the infinite minus one duration sentinel on deadline, lifespan,
// or lease duration.
func hasBestAvailable(p QoSProfile) bool {
	return p.Reliability == ReliabilityBestAvailable ||
		p.Durability == DurabilityBestAvailable ||
		p.Liveliness == LivelinessBestAvailable ||
		IsBestAvailableDuration(p.Deadline) ||
		IsBestAvailableDuration(p.Lifespan) ||
		IsBestAvailableDuration(p.LeaseDuration)
}

// resolveServiceBestAvailable merges per field: every best available marker
// field in requested takes the resolved services default, every explicit
// field keeps the system default resolved value. This preserves explicit
// non marker choices instead of replacing the whole profile.
func resolveServiceBestAvailable(requested, resolved QoSProfile) QoSProfile {
	svc := ResolveSystemDefaults(ServicesDefaultQoS())
	out := resolved
	if requested.Reliability == ReliabilityBestAvailable {
		out.Reliability = svc.Reliability
	}
	if requested.Durability == DurabilityBestAvailable {
		out.Durability = svc.Durability
	}
	if requested.Liveliness == LivelinessBestAvailable {
		out.Liveliness = svc.Liveliness
	}
	if IsBestAvailableDuration(requested.Deadline) {
		out.Deadline = svc.Deadline
	}
	if IsBestAvailableDuration(requested.Lifespan) {
		out.Lifespan = svc.Lifespan
	}
	if IsBestAvailableDuration(requested.LeaseDuration) {
		out.LeaseDuration = svc.LeaseDuration
	}
	return out
}

// resolveBestAvailable resolves each marker per endpoint direction.
func resolveBestAvailable(kind EndpointKind, resolved QoSProfile, visible []Peer) QoSProfile {
	out := resolved
	if kind == EndpointPublisher {
		resolvePublisherBestAvailable(&out, visible)
	} else {
		resolveSubscriberBestAvailable(&out, visible)
	}
	return out
}

// resolvePublisherBestAvailable applies the publisher rules: always reliable
// plus always transient local; manual when any subscriber is manual, else
// automatic; deadline plus lease default when all peers are default, else
// the minimum; lifespan always unspecified because expiry is publisher local
// and never negotiated.
func resolvePublisherBestAvailable(out *QoSProfile, visible []Peer) {
	if out.Reliability == ReliabilityBestAvailable {
		out.Reliability = ReliabilityReliable
	}
	if out.Durability == DurabilityBestAvailable {
		out.Durability = DurabilityTransientLocal
	}
	if out.Liveliness == LivelinessBestAvailable {
		out.Liveliness = LivelinessAutomatic
		if anyPeerManual(visible) {
			out.Liveliness = LivelinessManualByTopic
		}
	}
	if IsBestAvailableDuration(out.Deadline) {
		out.Deadline = minDurationFold(visible, func(p QoSProfile) Duration {
			return p.Deadline
		})
	}
	if IsBestAvailableDuration(out.LeaseDuration) {
		out.LeaseDuration = minDurationFold(visible, func(p QoSProfile) Duration {
			return p.LeaseDuration
		})
	}
	if IsBestAvailableDuration(out.Lifespan) {
		out.Lifespan = UnspecifiedDuration()
	}
}

// resolveSubscriberBestAvailable applies the subscriber rules: reliable
// exactly when at least one peer exists and all are reliable, else best
// effort; transient local exactly when at least one peer exists and all are
// transient local, else volatile; manual exactly when at least one peer
// exists and all are manual, else automatic; deadline plus lease default
// when all peers are default, else the maximum; lifespan always unspecified
// because expiry is publisher local and never a request.
func resolveSubscriberBestAvailable(out *QoSProfile, visible []Peer) {
	if out.Reliability == ReliabilityBestAvailable {
		out.Reliability = ReliabilityBestEffort
		if allPeersReliable(visible) {
			out.Reliability = ReliabilityReliable
		}
	}
	if out.Durability == DurabilityBestAvailable {
		out.Durability = DurabilityVolatile
		if allPeersTransientLocal(visible) {
			out.Durability = DurabilityTransientLocal
		}
	}
	if out.Liveliness == LivelinessBestAvailable {
		out.Liveliness = LivelinessAutomatic
		if allPeersManual(visible) {
			out.Liveliness = LivelinessManualByTopic
		}
	}
	if IsBestAvailableDuration(out.Deadline) {
		out.Deadline = maxDurationFold(visible, func(p QoSProfile) Duration {
			return p.Deadline
		})
	}
	if IsBestAvailableDuration(out.LeaseDuration) {
		out.LeaseDuration = maxDurationFold(visible, func(p QoSProfile) Duration {
			return p.LeaseDuration
		})
	}
	if IsBestAvailableDuration(out.Lifespan) {
		out.Lifespan = UnspecifiedDuration()
	}
}

// minDurationFold folds one duration field across visible peers taking the
// minimum.
//
// Unspecified, best available sentinels, and malformed negative seconds all
// count as infinite (no constraint), so malformed peers stay quiet instead
// of skewing the extremum. An infinite fold result normalizes back to
// unspecified, so a mixed default plus finite peer set collapses to the
// finite minimum on the offer side. Zero peers means all default, hence
// unspecified.
func minDurationFold(peers []Peer, get func(QoSProfile) Duration) Duration {
	best := InfiniteDuration()
	set := false
	for _, peer := range peers {
		eff := get(peer.Profile)
		if eff.IsUnspecified() || IsBestAvailableDuration(eff) || eff.Seconds < 0 {
			eff = InfiniteDuration()
		}
		if !set {
			best = eff
			set = true
			continue
		}
		if eff.Compare(best) < 0 {
			best = eff
		}
	}
	if !set || best.IsInfinite() {
		return UnspecifiedDuration()
	}
	return best
}

// maxDurationFold folds one duration field across visible peers taking the
// maximum.
//
// Unspecified, best available sentinels, and malformed negative seconds all
// count as infinite (no constraint), so malformed peers stay quiet instead
// of skewing the extremum. An infinite fold result normalizes back to
// unspecified, so a mixed default plus finite peer set collapses to default
// on the request side because a default publisher cannot satisfy a finite
// request. Zero peers means all default, hence unspecified.
func maxDurationFold(peers []Peer, get func(QoSProfile) Duration) Duration {
	best := InfiniteDuration()
	set := false
	for _, peer := range peers {
		eff := get(peer.Profile)
		if eff.IsUnspecified() || IsBestAvailableDuration(eff) || eff.Seconds < 0 {
			eff = InfiniteDuration()
		}
		if !set {
			best = eff
			set = true
			continue
		}
		if eff.Compare(best) > 0 {
			best = eff
		}
	}
	if !set || best.IsInfinite() {
		return UnspecifiedDuration()
	}
	return best
}

// allPeersReliable reports whether at least one peer exists and all are reliable.
//
// The check carries an explicit non-empty guard: len(peers) > 0 and every
// peer is reliable. The empty set returns false so a peerless subscriber
// resolves best effort.
func allPeersReliable(peers []Peer) bool {
	if len(peers) == 0 {
		return false
	}
	for _, peer := range peers {
		if peer.Profile.Reliability != ReliabilityReliable {
			return false
		}
	}
	return true
}

// allPeersTransientLocal reports whether at least one peer exists and all
// peers are transient local.
//
// The check carries an explicit non-empty guard: len(peers) > 0 and every
// peer is transient local. The empty set returns false so a peerless
// subscriber resolves volatile like the best effort reliability rule.
func allPeersTransientLocal(peers []Peer) bool {
	if len(peers) == 0 {
		return false
	}
	for _, peer := range peers {
		if peer.Profile.Durability != DurabilityTransientLocal {
			return false
		}
	}
	return true
}

// allPeersManual reports whether at least one peer exists and all peers use
// manual by topic liveliness.
//
// The check carries an explicit non-empty guard: len(peers) > 0 and every
// peer is manual. The empty set returns false so a peerless subscriber
// resolves automatic like the best effort reliability rule.
func allPeersManual(peers []Peer) bool {
	if len(peers) == 0 {
		return false
	}
	for _, peer := range peers {
		if peer.Profile.Liveliness != LivelinessManualByTopic {
			return false
		}
	}
	return true
}

// anyPeerManual reports whether any peer uses manual by topic liveliness.
// The empty set returns false.
func anyPeerManual(peers []Peer) bool {
	for _, peer := range peers {
		if peer.Profile.Liveliness == LivelinessManualByTopic {
			return true
		}
	}
	return false
}

// PairShouldDeliver reports whether offered publisher profile can flow to
// requested subscription profile.
//
// It returns CheckCompatibility(offered, requested).Compatible. Incompatible
// pairs must stay silent with a loud event. History, depth, lifespan, and the
// avoidance flag never affect the result.
func PairShouldDeliver(offered, requested QoSProfile) bool {
	report := CheckCompatibility(offered, requested)
	return report.Compatible
}

// CheckEndpointPair checks publisher offered ActualQoS against subscriber
// requested ActualQoS.
//
// The first argument is the offered side and the second is the requested
// side regardless of EndpointKind. A nil endpoint yields an incompatible
// report so callers stay silent instead of flowing. History, depth,
// lifespan, and the avoidance flag never affect the result.
func CheckEndpointPair(publisher, subscriber *Endpoint) CompatibilityReport {
	if publisher == nil || subscriber == nil {
		return CompatibilityReport{
			Compatible: false,
			Errors: []CompatibilityIssue{
				{Kind: PolicyReliability, Detail: "nil endpoint cannot flow"},
			},
		}
	}
	return CheckCompatibility(publisher.ActualQoS(), subscriber.ActualQoS())
}

// EndpointsCanDeliver reports whether two endpoints can flow.
//
// It returns CheckEndpointPair(publisher, subscriber).Compatible.
// Incompatible pairs must stay silent while the loud event from
// CompatibilityEvents or LiveMismatchReporter carries topic plus endpoint
// pair plus policy kind. Nil endpoints never deliver.
func EndpointsCanDeliver(publisher, subscriber *Endpoint) bool {
	if publisher == nil || subscriber == nil {
		return false
	}
	report := CheckEndpointPair(publisher, subscriber)
	return report.Compatible
}

// ShouldRetainForLateJoiner reports whether offered history must be kept
// for late joiners.
//
// It returns true only when the pair is compatible and the offered
// durability is transient local. Volatile offered keeps no history.
// Incompatible pairs keep no servable history even when offered is
// transient local because they stay silent. History kind and depth size
// retention through RetentionCapacity but never change this decision.
func ShouldRetainForLateJoiner(offered, requested QoSProfile) bool {
	if !PairShouldDeliver(offered, requested) {
		return false
	}
	return offered.Durability == DurabilityTransientLocal
}

// EndpointsRetainForLateJoiner reports whether publisher history must be
// kept for subscriber as late joiner.
//
// It applies ShouldRetainForLateJoiner to ActualQoS. Nil endpoints never
// retain. Compatible transient local pairs keep history so Snapshot serves
// late joiners. Incompatible pairs stay silent with no servable history.
func EndpointsRetainForLateJoiner(publisher, subscriber *Endpoint) bool {
	if publisher == nil || subscriber == nil {
		return false
	}
	return ShouldRetainForLateJoiner(publisher.ActualQoS(), subscriber.ActualQoS())
}
