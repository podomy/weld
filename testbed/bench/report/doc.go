// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

// Package report carries bench scenario results plus HTML and SVG
// rendering with no external assets.
//
// A Result holds one scenario and leg: latency summaries in nanoseconds,
// throughput counters, loss signals, and host signals. Acc accumulates raw
// latency samples and summarizes them with the percentile method below.
// HTML renders the summary table and the SVG helpers render throughput
// bars, latency marker curves, per-scenario latency percentile bars, and
// the load-latency sweep curve with a fixed 960x420 viewBox.
//
// Percentile method: nearest-rank on a sorted copy, index ceil(p*n)-1
// clamped to [0,n-1]; mean and stddev are two-pass population statistics.
package report
