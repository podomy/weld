// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package report

// percentileFooter states the percentile method for table footers.
const percentileFooter = "Percentile method: nearest-rank on a sorted copy, " +
	"index ceil(p*n)-1 clamped to [0,n-1]; mean and stddev are two-pass population statistics."

// Summary carries latency statistics in nanoseconds.
type Summary struct {
	// Min is the smallest sample, zero when empty.
	Min float64 `json:"min"`
	// P50 is the nearest-rank median.
	P50 float64 `json:"p50"`
	// P95 is the nearest-rank 95th percentile.
	P95 float64 `json:"p95"`
	// P99 is the nearest-rank 99th percentile.
	P99 float64 `json:"p99"`
	// Max is the largest sample, zero when empty.
	Max float64 `json:"max"`
	// Mean is the two-pass population mean.
	Mean float64 `json:"mean"`
	// Stddev is the two-pass population standard deviation.
	Stddev float64 `json:"stddev"`
}

// Result carries one bench scenario measurement.
type Result struct {
	// Scenario names the measured scenario.
	Scenario string `json:"scenario"`
	// Leg names the transport leg, empty when not applicable.
	Leg string `json:"leg"`
	// PayloadBytes is the payload size in bytes.
	PayloadBytes int `json:"payloadBytes"`
	// RateHz is the target send rate in hertz.
	RateHz float64 `json:"rateHz"`
	// WarmupN counts warmup messages excluded from measurement.
	WarmupN int `json:"warmupN"`
	// MeasuredN counts messages included in measurement.
	MeasuredN int `json:"measuredN"`
	// Sent counts messages sent.
	Sent int `json:"sent"`
	// Recv counts messages received.
	Recv int `json:"recv"`
	// Dups counts duplicate deliveries.
	Dups int `json:"dups"`
	// Gaps lists missing sequence numbers.
	Gaps []int `json:"gaps"`
	// LossPct is the loss percentage.
	LossPct float64 `json:"lossPct"`
	// LatNs carries latency statistics in nanoseconds.
	LatNs Summary `json:"latNs"`
	// ThrMsgs is throughput in messages per second.
	ThrMsgs float64 `json:"thrMsgs"`
	// ThrMiBs is throughput in mebibytes per second.
	ThrMiBs float64 `json:"thrMiBs"`
	// CPUS is CPU consumption in seconds.
	CPUS float64 `json:"cpuS"`
	// RSSMiB is resident memory in mebibytes.
	RSSMiB float64 `json:"rssMiB"`
	// DiscoveryMs is discovery time in milliseconds.
	DiscoveryMs float64 `json:"discoveryMs"`
	// Notes carries free form annotations.
	Notes string `json:"notes"`
}
