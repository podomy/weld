// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package report

import (
	"math"
	"slices"
)

// Acc accumulates latency samples in nanoseconds.
type Acc struct {
	vals []float64
}

// Add records one sample.
func (a *Acc) Add(v float64) {
	if a == nil {
		return
	}
	a.vals = append(a.vals, v)
}

// Summary returns latency statistics over the recorded samples.
//
// Percentiles use the nearest-rank method on a sorted copy. Mean and
// stddev are two-pass population statistics. An empty accumulator
// returns zeros so callers never see NaN.
func (a *Acc) Summary() Summary {
	if a == nil || len(a.vals) == 0 {
		return Summary{}
	}
	sorted := make([]float64, len(a.vals))
	copy(sorted, a.vals)
	slices.Sort(sorted)
	return summarizeSorted(sorted)
}

// summarizeSorted computes statistics over ascending samples.
func summarizeSorted(sorted []float64) Summary {
	n := float64(len(sorted))
	total := 0.0
	for _, v := range sorted {
		total += v
	}
	mean := total / n
	sq := 0.0
	for _, v := range sorted {
		d := v - mean
		sq += d * d
	}
	return Summary{
		Min:    sorted[0],
		P50:    nearestRank(sorted, 0.50),
		P95:    nearestRank(sorted, 0.95),
		P99:    nearestRank(sorted, 0.99),
		Max:    sorted[len(sorted)-1],
		Mean:   mean,
		Stddev: math.Sqrt(sq / n),
	}
}

// nearestRank returns the nearest-rank percentile of ascending samples.
func nearestRank(sorted []float64, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(math.Ceil(p*float64(len(sorted)))) - 1
	idx = max(idx, 0)
	idx = min(idx, len(sorted)-1)
	return sorted[idx]
}
