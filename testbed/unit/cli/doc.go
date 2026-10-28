// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

// Package unit_cli holds pure Go CLI conformance checks for cmd/weld.
//
// Tests run with no peer and no network. They use in memory fakes for
// the transport backend plus direct checks of public core, transports,
// dds, and types APIs and static reads of cmd/weld/main.go. They pin
// topic validation, domain range and defaults, timeout handling, text
// selection, help output, exit codes, gap and drop counters, pub
// confirmation, echo payload lines, route waiting, and SEDP reader
// declaration. Live peer checks live in testbed/live/cli with per-run
// IDs and context timeouts.
package unit_cli
