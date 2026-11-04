// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

// Command bench-agent is the containerized Weld counterpart of the rclpy
// bench probe at testbed/bench/probes/bench_probe.py.
//
// It speaks the same CLI contract and emits the same single JSON document
// so the bench harness can swap Weld endpoints into containers with zero
// harness logic change. Payload framing mirrors the harness
// encodeBenchPayload, pacing mirrors the probe loops, and only the pinned
// reliable/transient_local/keep_last/100 QoS set is accepted so the bench
// matrix stays honest.
package main
