# Weld vs ROS 2 bench results

How to run, from the repo root with the fleet up (`cd testbed && docker compose up -d`):

```sh
go test ./testbed/bench/ -count=1                                            # P0 smoke, ~35 s
WELD_BENCH_FULL=1 go test ./testbed/bench/ -count=1 -timeout 60m              # full matrix, ~25-40 min
WELD_BENCH_FULL=1 go test ./testbed/bench/ -run TestBenchSweep -count=1 -timeout 60m   # sweep alone
```

Every run writes one self-contained HTML report to `testbed/logs/bench-*.html`.
Legs: **L1** weld→weld in-process, **L1c** weld→weld containerized,
**L2** ros→ros, **L3** weld→ros, **L4** ros→weld. Same host, host network,
per-run topics and pooled domains, pinned reliable + transient_local +
keep_last depth 100 both sides.

## Latency: weld-to-weld is faster than ros-to-ros

p50, all rows 0% loss on both sides (full matrix 2026-11-03, 110 rows, exit 0):

| payload | L1 weld-weld | L2 ros-ros | Weld faster by |
|---|---|---|---|
| 64 B | 275µs | 492µs | **44%** |
| 256 B | 274µs | 502µs | **45%** |
| 1 KiB | 273µs | 561µs | **51%** |
| 4 KiB | 325µs | 1,015µs | **68%** |
| 64 KiB | 5.2ms | 10.3ms | **50%** |
| 256 KiB | 19.4ms | 18.7ms | tie (−4%) |
| 1 MiB | 76.4ms | 91.6ms | **17%** |

Same story on the rate sweep at 1 KiB while lossless (50 Hz–5 kHz):
about 50% lower p50 through 100–800 Hz (up to 62% at 50 Hz),
converging toward ~10% at 5 kHz as both take loops saturate.

## Scaling: Weld holds zero loss roughly 10x further up the dial

1 KiB offered load, loss per leg:

| offered | L1 | L4 (ros→weld) | L2 (ros→ros) | L3 (weld→ros) | L1c |
|---|---|---|---|---|---|
| 50 Hz–5 kHz | 0% | 0% | 0% | 0% | 0% |
| 10 kHz | 0% | 0% | **2.6%** | **25.7%** | **13.5%** |
| 30–100 kHz | 0% | 0% | 0–5.3% | 26–30% | 7–33% |

Sustained achieved throughput at 1 KiB: weld-to-weld about
**11.5k msgs/s**, ros-to-ros about **9.0k msgs/s** — roughly 25% more
headroom, and every Weld-receiving leg stays lossless throughout.

![Latency vs achieved throughput, 1 KiB sweep. One p50 curve per leg in leg
colors with circle markers, p99 dashed with triangles; red rings mark
lossy rates.](latency-vs-throughput.svg)

## Reading the chart

Each point is one measured rate step, plotted at *achieved* (not offered)
throughput against p50/p99 latency. The p50 lines fall left to right
because idle time disappears as load rises; the dashed p99 lines climbing
first mark each stack's knee. Red rings are the only loss in the matrix.

## Caveats, stated plainly

- The ROS legs subscribe in Python rclpy, so part of their gap is
  subscriber-side; the L1-vs-L4 comparison (same Go subscriber, 274µs vs
  264µs at 256 B) shows the transports themselves at parity on publish.
- Above ~1 kHz offered, Weld legs achieve less than offered (route settle
  on fresh domains is the standing hypothesis, still open).
- Choke onset jitters run to run: treat 10 kHz as the onset zone, not a cliff.
- Container tax (L1 vs L1c) is ≤15% at 256 B and below, within a few
  percent elsewhere, about 3% at 1 MiB.
