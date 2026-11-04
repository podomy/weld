// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package bench

import (
	"context"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/podomy/weld/testbed/live/helpers"
)

// TestMain runs the bench entrypoints then renders the single shared HTML
// report for the whole invocation, so one `go test` run leaves exactly
// one HTML report no matter how many scenarios ran.
//
// Entrypoints run serially on purpose: parallel legs share one host, one
// daemon, and two containers, and docker-exec plus interpreter startup
// storms under parallel load produced route flaps and skewed tails. The
// pooled-domain isolation still protects against concurrent `go test`
// binaries (for example the pre-commit hook running other packages
// alongside this one).
func TestMain(m *testing.M) {
	code := m.Run()
	benchRenderShared()
	os.Exit(code)
}

// scenarioTopic derives one per-run topic per scenario from the run topic.
// Suffix dashes map to underscores: rclpy rejects dash in topic names, and
// the agent accepts both, so sanitize here to keep one topic valid on both
// stacks. Mirrors helpers.LiveTopic sanitization for the base ID.
func scenarioTopic(base, suffix string) string {
	suffix = strings.ReplaceAll(suffix, "-", "_")
	suffix = strings.ReplaceAll(suffix, " ", "_")
	return base + "_" + suffix
}

// benchTestTimeout bounds one entrypoint: the smoke budget by default, a
// full-matrix budget when WELD_BENCH_FULL=1 runs every tier plus scale.
func benchTestTimeout() time.Duration {
	if benchFull() {
		return benchFullTimeout
	}
	return benchDefaultTimeout
}

// runTier runs every selected tier on one leg with a fresh pooled domain
// per tier, so a reused domain never carries lingering writers into the
// next tier. Defers pair close with release so even a loud failure leaves
// no traffic behind.
func runTier(t *testing.T, ctx context.Context, run *benchRun, leg func(t *testing.T, ctx context.Context, domain int, topic string, tr tier, rateHz float64)) {
	t.Helper()
	for _, tr := range selectTiers(benchFull()) {
		domain, release := leaseDomain(t, ctx)
		topic := scenarioTopic(helpers.LiveTopic(run.liveID), tr.name)
		func() {
			defer release()
			defer run.closeScenarioBackends() //nolint:contextcheck // backend Close is cleanup without caller ctx, goodbye uses bounded Background
			leg(t, ctx, domain, topic, tr, tr.rateHz)
		}()
	}
}

// TestBenchWeldWeld runs L1 throughput in process with no peer needed.
// It still allocates pooled domains for port discipline, so it needs the
// docker daemon plus fleet containers for the pool purge step even though
// no fleet traffic flows.
func TestBenchWeldWeld(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), benchTestTimeout())
	defer cancel()
	run := newBenchRun(t, "bench-l1")
	runTier(t, ctx, run, func(t *testing.T, ctx context.Context, domain int, topic string, tr tier, rateHz float64) {
		t.Helper()
		runWeldThroughput(t, ctx, run, domain, topic, tr, rateHz)
	})
}

// TestBenchWeldWeldContainer runs L1c throughput with two agent nodes in
// weld: an agent sub plus an agent pub on the same per-run topic.
// It is the containerization-tax check against L1: same tier, both
// endpoints containerized.
func TestBenchWeldWeldContainer(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), benchTestTimeout())
	defer cancel()
	helpers.SkipIfNoPeer(t, ctx, weldContainer)
	run := newBenchRun(t, "bench-l1c")
	runTier(t, ctx, run, func(t *testing.T, ctx context.Context, domain int, topic string, tr tier, rateHz float64) {
		t.Helper()
		runWeldContainerThroughput(t, ctx, run, domain, weldDockerPrefix(domain), topic, tr, rateHz)
	})
}

// runContainerLeg runs one Jazzy container leg on the single P0 tier.
// The leg binds its docker prefix per pooled domain.
func runContainerLeg(t *testing.T, prefix string, leg func(t *testing.T, ctx context.Context, run *benchRun, domain int, dockerPrefix []string, topic string, tr tier, rateHz float64)) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), benchTestTimeout())
	defer cancel()
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)
	run := newBenchRun(t, prefix)
	runTier(t, ctx, run, func(t *testing.T, ctx context.Context, domain int, topic string, tr tier, rateHz float64) {
		t.Helper()
		leg(t, ctx, run, domain, benchDockerPrefix(domain), topic, tr, rateHz)
	})
}

// TestBenchForward runs L3 throughput: an agent pub in weld plus a
// probe sub in the Jazzy peer.
func TestBenchForward(t *testing.T) {
	runContainerLeg(t, "bench-l3", runForward)
}

// TestBenchReverse runs L4 throughput: a probe pub in the Jazzy peer plus
// an agent sub in weld. Gates mirror forward.
func TestBenchReverse(t *testing.T) {
	runContainerLeg(t, "bench-l4", runReverse)
}

// TestBenchRosRos runs L2 throughput with two probe nodes in the Jazzy
// peer: a sub probe plus a pub probe on the same per-run topic.
func TestBenchRosRos(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), benchTestTimeout())
	defer cancel()
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)
	run := newBenchRun(t, "bench-l2")
	runTier(t, ctx, run, func(t *testing.T, ctx context.Context, domain int, topic string, tr tier, rateHz float64) {
		t.Helper()
		runRosRos(t, ctx, run, domain, benchDockerPrefix(domain), topic, tr, rateHz)
	})
}

// scaleLeg binds one scale leg: its topic prefix, the container it needs
// (empty for in-process legs), and the runner.
type scaleLeg struct {
	prefix    string
	container string
	run       func(t *testing.T, ctx context.Context, run *benchRun, domain int, topic string)
}

// runScaleSet runs one scale leg per entry with a fresh pooled domain per
// leg, so a reused domain never carries discovery state from the previous
// leg. Legs run serially like the rest of the bench. Container legs skip
// cleanly when their peer is absent.
func runScaleSet(t *testing.T, ctx context.Context, run *benchRun, legs []scaleLeg) {
	t.Helper()
	for _, leg := range legs {
		if leg.container != "" {
			helpers.SkipIfNoPeer(t, ctx, leg.container)
		}
		domain, release := leaseDomain(t, ctx)
		topic := scenarioTopic(helpers.LiveTopic(run.liveID), leg.prefix)
		func() {
			defer release()
			defer run.closeScenarioBackends() //nolint:contextcheck // backend Close is cleanup without caller ctx, goodbye uses bounded Background
			leg.run(t, ctx, run, domain, topic)
		}()
	}
}

// TestBenchFanout runs 1-to-5 fanout on L1, L1c, and L2 in full mode only.
func TestBenchFanout(t *testing.T) {
	if !benchFull() {
		t.Skip("fanout runs in full mode only (WELD_BENCH_FULL=1)")
	}
	ctx, cancel := context.WithTimeout(context.Background(), benchTestTimeout())
	defer cancel()
	run := newBenchRun(t, "bench-fanout")
	runScaleSet(t, ctx, run, []scaleLeg{
		{prefix: "fanout-l1", run: runFanoutInproc},
		{prefix: "fanout-l1c", container: weldContainer, run: runFanoutAgent},
		{prefix: "fanout-l2", container: helpers.Container, run: runFanoutProbe},
	})
}

// TestBenchFanin runs 5-to-1 fanin on L1, L1c, and L2 in full mode only.
// Publishers hold disjoint seq spaces tiling [1..total] so the single
// subscriber sees one gapless stream.
func TestBenchFanin(t *testing.T) {
	if !benchFull() {
		t.Skip("fanin runs in full mode only (WELD_BENCH_FULL=1)")
	}
	ctx, cancel := context.WithTimeout(context.Background(), benchTestTimeout())
	defer cancel()
	run := newBenchRun(t, "bench-fanin")
	runScaleSet(t, ctx, run, []scaleLeg{
		{prefix: "fanin-l1", run: runFaninInproc},
		{prefix: "fanin-l1c", container: weldContainer, run: runFaninAgent},
		{prefix: "fanin-l2", container: helpers.Container, run: runFaninProbe},
	})
}

// TestBenchSweep runs the load-latency sweep in full mode only: fixed P1
// payloads from 50 Hz to 100 kHz in eleven steps on all five legs. Rows are headroom:
// rate plus loss is recorded without failing, and the choke point where
// loss appears or p99 climbs is read off the latency-vs-throughput chart.
// Each leg-rate pair gets a fresh pooled domain plus per-run topic and
// runs serially like every other scale leg.
func TestBenchSweep(t *testing.T) {
	if !benchFull() {
		t.Skip("sweep runs in full mode only (WELD_BENCH_FULL=1)")
	}
	ctx, cancel := context.WithTimeout(context.Background(), benchTestTimeout())
	defer cancel()
	run := newBenchRun(t, "bench-sweep")
	legs := []scaleLeg{}
	for _, rate := range sweepRates() {
		tr := sweepTier(rate)
		suffix := "_r" + strconv.Itoa(int(rate))
		legs = append(legs,
			scaleLeg{prefix: "sweep_l1" + suffix, run: func(t *testing.T, ctx context.Context, run *benchRun, domain int, topic string) {
				runWeldThroughput(t, ctx, run, domain, topic, tr, rate)
			}},
			scaleLeg{prefix: "sweep_l1c" + suffix, container: weldContainer, run: func(t *testing.T, ctx context.Context, run *benchRun, domain int, topic string) {
				runWeldContainerThroughput(t, ctx, run, domain, weldDockerPrefix(domain), topic, tr, rate)
			}},
			scaleLeg{prefix: "sweep_l2" + suffix, container: helpers.Container, run: func(t *testing.T, ctx context.Context, run *benchRun, domain int, topic string) {
				runRosRos(t, ctx, run, domain, benchDockerPrefix(domain), topic, tr, rate)
			}},
			scaleLeg{prefix: "sweep_l3" + suffix, container: helpers.Container, run: func(t *testing.T, ctx context.Context, run *benchRun, domain int, topic string) {
				runForward(t, ctx, run, domain, benchDockerPrefix(domain), topic, tr, rate)
			}},
			scaleLeg{prefix: "sweep_l4" + suffix, container: helpers.Container, run: func(t *testing.T, ctx context.Context, run *benchRun, domain int, topic string) {
				runReverse(t, ctx, run, domain, benchDockerPrefix(domain), topic, tr, rate)
			}},
		)
	}
	runScaleSet(t, ctx, run, legs)
}

// TestBenchSaturation runs uncapped headroom on L1 and L1c in full mode
// only: fixed P1 payloads blasted as fast as the stack allows until the
// count lands, with loss reported alongside the rate.
func TestBenchSaturation(t *testing.T) {
	if !benchFull() {
		t.Skip("saturation runs in full mode only (WELD_BENCH_FULL=1)")
	}
	ctx, cancel := context.WithTimeout(context.Background(), benchTestTimeout())
	defer cancel()
	run := newBenchRun(t, "bench-sat")
	tr := saturationTier()
	runScaleSet(t, ctx, run, []scaleLeg{
		{prefix: "sat-l1", run: func(t *testing.T, ctx context.Context, run *benchRun, domain int, topic string) {
			runWeldThroughput(t, ctx, run, domain, topic, tr, 0)
		}},
		{prefix: "sat-l1c", container: weldContainer, run: func(t *testing.T, ctx context.Context, run *benchRun, domain int, topic string) {
			runWeldContainerThroughput(t, ctx, run, domain, weldDockerPrefix(domain), topic, tr, 0)
		}},
	})
}

// TestScenarioTopicSanitizesDashes pins ROS topic validity: dashes and
// spaces in the suffix map to underscores because rclpy rejects dashes,
// while the agent accepts both. The probe and the agent must see the same
// valid topic.
func TestScenarioTopicSanitizesDashes(t *testing.T) {
	t.Parallel()
	got := scenarioTopic("/chatter_bench_x", "fanout-l2")
	if got != "/chatter_bench_x_fanout_l2" {
		t.Fatalf("topic = %q, want underscore-sanitized", got)
	}
	const rosTopicChars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789/_~{}"
	for _, c := range got {
		if !strings.ContainsRune(rosTopicChars, c) {
			t.Fatalf("topic %q holds invalid char %q", got, string(c))
		}
	}
}

// TestSweepTiersCoverElevenRates pins the sweep shape: eleven offered
// rates ending at 100 kHz, all at fixed P1 payloads with loss-tolerant
// headroom names, so the choke search spans three orders of magnitude.
func TestSweepTiersCoverElevenRates(t *testing.T) {
	t.Parallel()
	rates := sweepRates()
	if len(rates) != 11 || rates[0] != 50 || rates[len(rates)-1] != 100000 {
		t.Fatalf("rates = %v, want 11 steps from 50 to 100000", rates)
	}
	for _, rate := range rates {
		tr := sweepTier(rate)
		if tr.size != 1024 || tr.rateHz != rate {
			t.Fatalf("tier %+v breaks fixed-P1 contract at rate %v", tr, rate)
		}
		if !isHeadroomTier(tr) {
			t.Fatalf("tier %q must be headroom (record without failing)", tr.name)
		}
	}
}

// TestIsHeadroomTierOnlyMatchesHeadroom pins the fail-loud boundary:
// saturation plus sweep tiers record without failing, every paced tier
// fails loudly on gaps.
func TestIsHeadroomTierOnlyMatchesHeadroom(t *testing.T) {
	t.Parallel()
	for _, tr := range append([]tier{saturationTier()}, fullTiers()...) {
		want := tr.name == "sat" || len(tr.name) > 2 && tr.name[:2] == "sw"
		if isHeadroomTier(tr) != want {
			t.Fatalf("tier %q headroom = %v, want %v", tr.name, !want, want)
		}
	}
}
