// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

// Package live_cli holds live CLI checks that need a peer and network.
//
// Tests drive the built weld binary and the already running ros-jazzy
// peer with structured argv and no shell. They pin text selection, pub
// confirmation, echo payload lines, weld to weld round trip, and forward
// plus reverse interop on allocated exclusive domains. Every run uses a
// per-run live ID so stale runs never mix. Tests skip cleanly when no
// daemon or peer is present.
package live_cli
