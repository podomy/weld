// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

// Package unit_chaos holds wireless chaos conformance tests.
//
// Tests run pure Go against the public chaos harness API with virtual
// time only. They pin the two state bursty link matrix from
// docs/wireless.md sections 3 and 8, partition split plus heal with local
// queries, debounced 200 ms discovery flap, teardown mid run, clock jumps
// across link states, the large message matrix, and late joiner retention.
package unit_chaos
