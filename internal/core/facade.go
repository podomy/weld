// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

// QoS worn as a facade: Weld speaks QoS on the wire and does not run a QoS
// engine inside.
//
// A Weld endpoint advertises reliable plus transient local with a bounded
// keep last (see FacadeAdvertisedQoS), which satisfies every standard ROS
// subscription, so matching almost never fails. The profile answers only
// the matching question. Behavior comes from core: backpressure between
// Weld endpoints instead of blind drops or unbounded growth, bounded
// retention with explicit eviction for late joiners, and drop oldest with
// lost counters on overflow. Deadlines are monitoring, firing alerts with
// topic plus endpoint names, never silently unmatching a pair. Exotic
// profiles surface loudly in logs, metrics, and CLI with topic, endpoint
// pair, and policy kind.
//
// Best available resolves once at endpoint creation from currently visible
// peers, stores the resolved profile, exposes it through actual getters,
// records which peers were visible at resolve time, and never updates on
// later discovery. System defaults resolve explicitly in core before entity
// creation. Core never relies on XML profiles, keyed topics, content
// filters, or loaning for cross vendor paths.
package core

import (
	"errors"
	"fmt"
)

// FacadeHistoryDepth is the bounded keep last depth Weld advertises on the
// wire.
//
// Derivation: DefaultHistoryDepth is 10 (core normalized keep last when
// depth is unset; CycloneDDS would use 1 and Zenoh would use 42, core
// converges to 10 before entity creation). FacadeHistoryDepth is 100, ten
// times the default, large enough to serve latched static topics such as
// parameters to late joiners without replaying a full log. Rosout and
// parameter presets use 1000 because logging and parameter events are
// high volume and loss sensitive. Facade depth is wire only for matching;
// actual retention comes from RetentionCapacity plus HistoryCache, and
// best available subscribers resolve durability from visible peers, not
// from this facade value.
//
// Byte budget: core scopes to counts only. The count bound here must sit
// alongside a byte cap enforced where samples are stored, in transports.
// One hundred large point clouds can still exceed memory, so transports
// bound bytes in addition to counts; see transport byte cap tests. Slow
// consumers hold at most capacity samples; overflow drops oldest, counts
// lost, and signals backpressure instead of growing without bound.
const FacadeHistoryDepth = 100

// KeepAllRetentionBound is the retention bound applied to keep all histories.
//
// Keep all must be bounded with an explicit drop policy rather than growing
// without bound, so core caps it here with drop oldest plus a lost counter
// and backpressure once full. Transports must not invent larger silent bounds.
// Core scopes to counts only: a keep all cache of large samples is still
// bounded in count but needs a byte ceiling in transports to bound memory
// for slow consumers; see transport byte cap tests.
const KeepAllRetentionBound = 1000

// MaxHistoryCacheEntries caps every history cache entry count.
//
// No existing test expects more than 1000 entries (facade 100, keep all
// 1000, slow consumer 100); 100000 is the documented ceiling. It bounds
// count memory for small samples to a few megabytes while transports bound
// bytes separately for large samples. RetentionCapacity clamps larger keep
// last depths to this ceiling and NewHistoryCache clamps direct
// constructions, so overflow past the ceiling evicts oldest with loud Lost
// counting instead of growing without bound.
const MaxHistoryCacheEntries = 100000

// FacadeAdvertisedQoS returns the profile Weld advertises on the wire:
// reliable plus transient local with a bounded keep last.
//
// Reliable offered to best effort requested flows, and transient local
// offered to volatile requested flows, so every standard ROS subscription
// matches. Deadlines, lifespan, and lease stay unspecified (the default) and
// liveliness stays automatic; genuinely exotic requests against those still
// mismatch loudly through CompatibilityEvents.
//
// The returned Depth is wire only for matching. It does not size retention;
// use RetentionCapacity plus NewHistoryCache for storage. Best available
// subscribers resolve durability from visible peers, not from this facade.
func FacadeAdvertisedQoS() QoSProfile {
	return QoSProfile{
		History:     HistoryKeepLast,
		Depth:       FacadeHistoryDepth,
		Reliability: ReliabilityReliable,
		Durability:  DurabilityTransientLocal,
		Liveliness:  LivelinessAutomatic,
	}
}

// OverflowPolicy names the queue overflow behavior. Only drop oldest ships
// in v1: drops stay visible through HistoryCache lost counters, never silent.
type OverflowPolicy int

// Overflow policies.
const (
	// OverflowDropOldest drops the oldest sample on overflow and counts it.
	OverflowDropOldest OverflowPolicy = iota
)

// String renders the policy in lowercase form.
func (p OverflowPolicy) String() string {
	switch p {
	case OverflowDropOldest:
		return "drop_oldest"
	default:
		return "invalid"
	}
}

// DeliveryBehavior describes how core carries bytes behind the facade.
//
// Caller contract for backpressure: treat Full as an advisory hint and the
// Push eviction report as authoritative. When Push returns true the caller
// must slow the producer (block, shed load, or signal upstream) instead of
// outrunning the consumer. Lost counts every applied drop so overflow stays
// visible on the loud channel; silence is a defect, never a state.
type DeliveryBehavior struct {
	// Backpressure is true: core slows producers between Weld endpoints
	// instead of blind drops or unbounded growth. Callers honor the Push
	// eviction report and slow the producer when it reports true.
	Backpressure bool
	// BoundedRetention is true: history caches evict explicitly for late
	// joiners instead of growing without bound.
	BoundedRetention bool
	// Overflow is the queue overflow policy, always drop oldest in v1.
	Overflow OverflowPolicy
	// DeadlineMonitoring is true: deadlines fire alerts, never unmatch pairs.
	DeadlineMonitoring bool
}

// FacadeDeliveryBehavior returns the core delivery behavior behind the facade.
func FacadeDeliveryBehavior() DeliveryBehavior {
	return DeliveryBehavior{
		Backpressure:       true,
		BoundedRetention:   true,
		Overflow:           OverflowDropOldest,
		DeadlineMonitoring: true,
	}
}

// RetentionCapacity returns the history cache bound for a resolved profile:
// depth for keep last clamped to MaxHistoryCacheEntries, KeepAllRetentionBound for keep all.
//
// Unresolved history or keep last with depth zero is an error: call
// ResolveSystemDefaults first so vendors converge before entity creation.
// Feed the returned capacity directly into NewHistoryCache, which panics on
// zero so an unresolved depth can never silently become a capacity of one.
// Unknown, invalid, and system default histories are errors. Depths past
// MaxHistoryCacheEntries clamp to the ceiling so a huge depth cannot force
// unbounded allocation; overflow past the ceiling evicts oldest with loud
// Lost counting in HistoryCache.
func RetentionCapacity(p QoSProfile) (uint64, error) {
	switch p.History {
	case HistoryKeepLast:
		if p.Depth == 0 {
			return 0, errors.New("core: retention capacity: keep last depth 0 means not set; call ResolveSystemDefaults first") //nolint:wrapcheck // new error, nothing to wrap
		}
		if p.Depth > MaxHistoryCacheEntries {
			return MaxHistoryCacheEntries, nil
		}
		return p.Depth, nil
	case HistoryKeepAll:
		return KeepAllRetentionBound, nil
	case HistorySystemDefault:
		return 0, errors.New("core: retention capacity: system default history is not resolved; call ResolveSystemDefaults first") //nolint:wrapcheck // new error, nothing to wrap
	case HistoryUnknown:
		return 0, errors.New("core: retention capacity: unknown history policy is not supported") //nolint:wrapcheck // new error, nothing to wrap
	default:
		return 0, fmt.Errorf("core: retention capacity: invalid history policy %d", p.History)
	}
}
