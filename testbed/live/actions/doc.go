// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

// Package actions holds live action workflow checks that need a peer and network.
//
// Tests drive the built weld binary and the already running weld-jazzy
// peer with structured argv and no shell. They pin goal admission,
// cancel propagation, feedback buffering before handle attachment, and
// result caching independent of handle lifetime on isolated domains.
// Every run uses one per-run live ID for payload isolation
// (testbed/live/helpers/helpers.go:130) and one per-run topic derived
// from it (testbed/live/helpers/helpers.go:143) on the shared live
// domain (testbed/live/helpers/helpers.go:40), so concurrent or stale
// runs never match. Ephemeral domains stay separate
// (testbed/live/helpers/helpers.go:49). This package asserts only
// action goal plus feedback plus result plus status topics and never
// asserts service request plus reply pairing. Tests skip cleanly when
// no daemon or peer is present.
package actions
