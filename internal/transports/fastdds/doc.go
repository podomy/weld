// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

// Package fastdds holds the FastDDS mapping tables plus a stub Backend.
//
// Transports are translators only. Core owns matching, QoS, routing,
// timeouts, retries, and state machines, and never imports this package.
// This package turns core profiles into FastDDS QoS and back, maps
// discovery ranges to FastDDS transports, and applies the XML knob
// policy. XCDR1 is forced on every reader and writer. XCDR2 is never
// emitted. No vendor drift leaks into core: all FastDDS specifics stay
// behind the Backend interface in this package.
//
// Mapping summary: discovery off uses minimal allocation with cleared
// multicast plus a dummy unicast peer, localhost clears multicast with
// shared memory plus UDP loopback peers, subnet keeps peers plus the
// multicast group. QoS forward and reverse follow the CycloneDDS table
// with FastDDS deltas: durability sets kind only with no durability
// service history, liveliness synthesizes an announcement period of two
// thirds of the lease with no Cyclone equivalent, unspecified lease maps
// to the DDS default on FastDDS but to infinity on Cyclone. Code QoS
// wins over XML: per topic profiles select by topic name with a client
// fallback for services, and cross vendor paths keep XML knobs off.
package fastdds
