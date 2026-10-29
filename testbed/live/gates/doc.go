// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

// Package gates holds gate wiring for the live interop gates.
//
// Gate 0 is offline port math and SPDP round trip. Gate 1 is live SPDP
// acceptance. Gate 2 is the forward and reverse String round trip with
// per-run live IDs. Scripts presence checks the Go gate files and compose.
package gates
