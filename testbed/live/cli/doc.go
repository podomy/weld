// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

// Package live_cli holds live CLI checks that need a peer and network.
//
// Tests drive the built weld binary and the already running weld-jazzy
// peer with structured argv and no shell. They pin text selection, pub
// confirmation, echo payload lines, weld to weld round trip, and forward
// plus reverse interop on allocated exclusive domains. Every run uses a
// per-run live ID so stale runs never mix. Tests skip cleanly when no
// daemon or peer is present.
package live_cli
