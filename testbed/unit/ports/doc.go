// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

// Package unit_ports holds port and bootstrap conformance tests.
//
// Tests run pure Go against public rtps and dds APIs only and assume
// already running containers without starting compose. They pin the DDSI
// port table, ephemeral versus standard binds, DialUnicast probing, the
// 32 target bootstrap probe shape, SPDP locator patching, and EADDRINUSE
// avoidance on shared host networking.
package unit_ports
