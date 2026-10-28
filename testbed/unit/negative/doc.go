// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

// Package unit_negative holds negative and limits conformance tests.
//
// Scope is pure Go via public dds, rtps, and types APIs with no Docker.
// Each test asserts that invalid input is rejected with an explicit error,
// that truncated input never panics, and that drop counters advance.
package unit_negative
