// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

// Package bench measures Weld DDS throughput and latency.
//
// A bench run drives one leg at a time on a fresh pooled DDS domain with a
// per-run topic, records one report.Result row per scenario in memory, and
// renders one self-contained HTML report under testbed/logs via TestMain.
// The entrypoints in bench_test.go map to legs:
//
//	L1 weld to weld in process (TestBenchWeldWeld, offline smoke and
//	zero-dependency baseline; no fleet traffic, but pooled domains still
//	need the daemon plus fleet for the pool purge step)
//	L1c weld to weld containerized (TestBenchWeldWeldContainer, agent pub
//	plus agent sub, both in weld; the containerization-tax check
//	against L1)
//	L2 ros to ros (TestBenchRosRos, two probe nodes in one peer container)
//	L3 weld to ros (TestBenchForward, agent pub in weld, probe sub
//	in the peer container)
//	L4 ros to weld (TestBenchReverse, probe pub in the peer container,
//	agent sub in weld)
//	Fanout (TestBenchFanout, one publisher to five subscribers on each
//	of L1, L1c, and L2; per-subscriber rows expose fanout tail latency)
//	Fanin (TestBenchFanin, five publishers with disjoint seq spaces
//	tiling [1..total] into one subscriber on each of L1, L1c, and L2)
//	Sweep (TestBenchSweep, fixed 1024 B payloads (tier P1, 1 KiB) at
//	50, 100, 200, 400, 800, 1600, 5000, 10000, 30000, 60000, 100000 Hz
//	on all five legs; headroom rows where loss is
//	the choke signal, read off the latency-vs-throughput chart. Each rate
//	lands as five rows sharing one Scenario (sw50 through sw100000) with a
//	different Leg; per-run topics look like sweep_l1_r50. Run it alone
//	with WELD_BENCH_FULL=1 go test ./testbed/bench/ -run TestBenchSweep
//	-count=1 -timeout 60m and the fleet up.)
//	Saturation (TestBenchSaturation, uncapped P1 blast on L1 and L1c;
//	headroom with loss reported alongside the rate)
//
// What L1c means in plain terms: L1 runs both Weld endpoints inside the
// test process on the host, with no containers and no network hops. L1c
// runs the exact same Weld code, but as two separate programs inside the
// weld container. So L1-vs-L1c is the pure cost of containerization, and
// L1c-vs-L2 is the fair fight: containerized Weld against containerized
// ROS on the same box with the same payloads.
//
// Topology. Every leg except L1 runs fully containerized: both endpoints
// live in containers (weld for the Weld side, ros-jazzy for the ROS side)
// on the host network, and the orchestrator (the test binary) runs on
// the host. L1 stays in-process as the offline smoke and zero-dependency
// baseline, and L1c mirrors L1 with both endpoints in weld so L1-vs-L1c
// isolates the containerization tax.
//
// Payload layout. Every bench sample is a std_msgs String holding exactly
// size chars: the 32 ASCII hex chars of 8 byte little endian send_ns plus
// 8 byte little endian seq, then fill chr(33 + ((seq+i)%94)) for fill
// position i. Seqs start at 1 like the probe. The hex plus ASCII shape is
// a deliberate wire deviation: rclpy truncates String at the first NUL
// byte, so a binary header plus byte fill cannot cross the ROS wire. Weld
// encodes and parses the same shape, so L1 legs validate the exact codec
// the probes use on every take.
//
// Every leg runs the single P0 tier by default: 256 B payloads, N=300 at
// 100 Hz with a 100 message warmup. WELD_BENCH_FULL=1 selects the full
// matrix instead: P0 through P6 (64 B control, 1/4 KiB, 64 KiB just
// above the fragmentation boundary at 65515, 256 KiB mid large, 1 MiB large),
// 1-to-5 fanout plus 5-to-1 fanin with disjoint seq spaces, an uncapped
// P1 saturation blast, and a P1 load-latency sweep from 50 Hz to 100 kHz
// in eleven steps on all five legs (headroom rows; the choke point is read off the
// latency-vs-throughput chart with red loss rings). Each full-mode
// entrypoint carries a 50 minute budget and the fleet must be up (the sweep
// needs both peers, weld plus ros-jazzy, and skips otherwise); entrypoints
// run serially, so the whole suite wall time is the sum of its legs.
// The sweep alone runs about five minutes lossless; lossy steps each burn
// their full take budget, so a choking sweep runs longer. The 50 minute
// budget bounds one entrypoint; a timeout fails loudly, never silently. Budget package timeout for the
// sum, not one leg: -timeout 60m fits the paced matrix, and raise it if the
// full run grows past that.
// Saturation is headroom, not a loss gate: the row records rate plus loss
// without failing, since an uncapped 20000-message blast against a
// depth-100 history must drop by construction. Sweep steps are headroom
// the same way: each rate records without failing, and the choke point
// where loss appears or p99 climbs is the verdict, read off the
// latency-vs-throughput chart (red rings mark lossy points).
//
// Environment gates: container legs skip cleanly via helpers.SkipIfNoPeer
// when the daemon or the needed peer (ROS peer or weld) is absent.
// Discovery failure never skips: an agent or probe exit code 2 with zero
// received fails loudly like a Weld-side discovery timeout. Throughput
// loss fails loudly after the result row is recorded, except the sat and
// sweep headroom tiers which record loss without failing.
//
// Fairness pointer. QoS is pinned symmetric everywhere: publishers offer
// reliable, transient local, keep last depth 100, and subscribers request
// reliable, transient local, keep last depth 100. The probe and the agent
// receive the same QoS on their argv; Weld subscribers use
// SubscribeWithDurability with volatile false (transient local request)
// and Weld publishers use the default writer path, which announces
// transient local. L1 drains warmup messages in process before
// measurement. Container legs run warmup free on purpose: a transient
// local late joiner replays history, so warmup samples would pollute the
// agent and probe loss and latency counts; a discovery wait (agent or
// probe match wait, or a host-observed discovery wait) plus a settle
// sleep replaces warmup there. Both containerized endpoints share the
// host network namespace and clock, so L3/L4 timestamps stay comparable
// across the container boundary and the L1-vs-L1c delta isolates the
// containerization tax. CPU seconds come from /proc/self/stat utime plus
// stime deltas over the measured window assuming HZ=100, and RSS comes
// from /proc/self/status VmRSS; both are host process numbers, so
// container legs report no host cost (like L2) while L1 rows carry the
// in-process cost.
//
// No hiding, in plain terms: every leg warms up the same way and warmup
// never counts toward the numbers. A missing message is never smoothed
// over: any gap fails the run loudly after the evidence row is recorded,
// except the sat and sweep headroom tiers where loss is the measured
// signal and rows record without failing. We have watched runs fail on a
// single missing seq rather than hide it. Duplicates from reliable
// redelivery are counted separately and never inflate the received
// count. Container pubs also send one uncounted copy of a fragmented
// first sample ahead of the measured stream; a transient that eats the
// copy lands in dups while unique Recv and LossPct are unaffected, and a
// sustained outage still gaps loudly.
//
// Known asymmetries, stated openly: L1 skips the container overhead that
// L2 always pays, so comparing L1 directly against L2 would flatter Weld.
// That is exactly why L1c exists: it pays the same containerization cost
// as L2, so L1c-vs-L2 compares like with like. One more sameness: when a
// corrupt or misrouted frame arrives, the Weld agent and the ROS probe
// both skip it the same way and let the missing seq show up as a gap,
// so neither side gets a quiet pass on bad frames.
//
// Isolation. Scenario backends close before the domain releases, so a
// released-then-reused domain never carries lingering writers or readers
// into the next scenario; an open writer on a reused domain was observed
// leaking its traffic into the next scenario takes. Per-run topics plus
// pooled domains keep containerized endpoints isolated the same way: an
// agent or probe proc outlives its docker exec kill only on timeout, and
// any such leak surfaces as loud loss on its per-run topic, never as a
// skip.
//
// Preconditions: every scenario allocates a fresh pooled domain with a
// per-scenario lease (AllocateDomainLease plus ReleaseDomainLease, so
// sequential scenarios never hold the 32 domain pool concurrently),
// whose purge step shells to docker, so even the fleet free L1 leg needs
// a live docker daemon with the fleet containers (including weld)
// present. ROS_DOMAIN_ID must be unset or equal to the scenario domain;
// a conflicting host value fails loudly instead of silently remapping
// the backend domain.
package bench
