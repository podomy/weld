# Testbed

Canonical live fleet and gates. Go is the only harness.

## Fleet

- Images: `ros-humble`, `ros-jazzy`, `ros-rolling` (`ros:<distro>-ros-base` plus `rmw-cyclonedds-cpp`).
- `ros-jazzy` is the canonical gate peer; Humble and Rolling are manual cross-distro checks.
- Host networking with `sleep infinity`, so DDS multicast discovery works with no extra config.

```bash
cd testbed
docker compose build
docker compose up -d
docker compose ps
```

Restart only on compose or volume change: `cd testbed && docker compose down && docker compose up -d`. Tests never start or stop the fleet.

## Run

```bash
go test ./testbed/unit/... -count=1
go test -timeout 60m ./testbed/live/... -count=1
go test ./testbed/bench/ -count=1
```

Live runs need `-timeout 60m`; the default 10m fires mid-suite. Plain `go test` skips without a peer; `-tags live` is a no-op selector. Serial ROS suites run via `testbed/live/serial.sh` with the fleet already up.

Skip rule: every live test calls `SkipIfNoPeer` first; type matrix tests also call `SkipIfNoTypeInterface`. A missing daemon, stopped peer, or missing `ros2 interface show` type skips cleanly. Failed wire, payload, or route asserts still fail (`testbed/live/helpers/helpers.go:444`, `testbed/live/helpers/live.go:326`).

## Domains

| Domain | Use |
|---|---|
| 42 | Canonical ROS String peer (`ros-jazzy`) |
| 48 | SPDP acceptance on allocated exclusive domains |
| 47 | Serial single-entrypoint gate; pool never hands it out |
| 43 / 44 | Distro matrix: Humble on 43, Rolling on 44 |
| 60-69 | Ephemeral isolation for liveliness and CLI checks |
| 70-101 | Pooled exclusive domains via `AllocateDomainLease` |

One run per domain and topic at a time. Every live run uses one per-run live ID (`testbed/live/helpers/helpers.go:131`).

## Gates

- Gate 0 (offline): port math for 42/47/48, SPDP marshal plus parse round trip, launch corpus pin (`testbed/live/gates/gate0_test.go:41`).
- Gate 1 (live SPDP): join allocated domain multicast, decode with `ParseAnnouncement`, prove non-local by GUID prefix inequality, zero deserialization failures (`testbed/live/gates/gate1_test.go:32`).
- Gate 2 (live String): forward Weld echo sees ROS talker plus reverse `ros2 topic echo` sees Weld pub, per-run live ID, context timeouts (`testbed/live/gates/gate2_test.go:33,107`).
- Type matrix extends Gate 2 to Int32, Float64, Bool, Vector3, PointStamped, Temperature, FluidPressure (`testbed/live/ros/gate_type_matrix_test.go:100,797`).
- SPDP checks assert wire fields only; ROS checks assert payloads only (`testbed/live/ros/doc.go`).
- Full wire record: `docs/conformance.md`.

## Type coverage

124 corpus rows (`internal/types/corpus.go:109`): live 115, pure-Go-only 2, service 7, missing 0.
Services have no `rt` topic gate except `std_srvs/Trigger` (live forward plus reverse plus deadline). Per-type gates in `docs/conformance.md`.

## Bench

Benchmarks live in `testbed/bench/` (legs L1, L1c, L2, L3, L4). Run, legs, fairness, and results: `testbed/bench/README.md`.
