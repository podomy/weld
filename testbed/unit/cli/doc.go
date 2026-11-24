// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

// Package unit_cli holds pure Go ros2 CLI conformance checks.
//
// Tests run with no peer and no network. They use in memory fakes for
// the transport backend plus direct checks of public core, transports,
// dds, and types APIs and static reads of
// testbed/live/helpers/roscli.go. They pin topic validation, domain
// range and defaults, timeout handling, text selection, help output,
// exit codes, gap and drop counters, pub confirmation, echo payload
// lines, route waiting, and SEDP reader declaration, with ros2 topic
// echo and pub argv shapes pinned against RunRosEcho and
// RosPubUntilSeen. Live peer checks live in testbed/live with per-run
// IDs and context timeouts.
package unit_cli
