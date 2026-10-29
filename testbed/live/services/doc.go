// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

// Package services holds live service call checks that need a peer and network.
//
// Tests drive the built weld binary and the already running weld-jazzy
// peer with structured argv and no shell. They pin request plus reply
// pairing with per-call context deadlines on isolated domains. Every run
// uses one per-run live ID for payload isolation
// (testbed/live/helpers/helpers.go:130) and one per-run topic derived
// from it (testbed/live/helpers/helpers.go:143) on the shared live
// domain (testbed/live/helpers/helpers.go:40), so concurrent or stale
// runs never match. Ephemeral domains stay separate
// (testbed/live/helpers/helpers.go:49). This package asserts only
// service request plus reply topics and never asserts action goal plus
// feedback plus result topics. Tests skip cleanly when no daemon or
// peer is present.
package services
