# Testbed

Reproducible ROS peers for the conformance gates: one container per supported distro, each able to speak every RMW Weld must interoperate with. All live checks are Go only. No shell harnesses remain under `testbed/`.

## Images

- `weld-humble`: `ros:humble-ros-base` plus `rmw-cyclonedds-cpp` plus `rmw-zenoh-cpp`
- `weld-jazzy`: `ros:jazzy-ros-base` plus `rmw-cyclonedds-cpp` plus `rmw-zenoh-cpp`
- `weld-rolling`: `ros:rolling-ros-base` plus `rmw-cyclonedds-cpp` plus `rmw-zenoh-cpp`

FastDDS ships in each base image. Dockerfiles pin nothing beyond the ROS apt snapshots.

## Runtime

Each container idles on `sleep infinity` with host networking, so DDS multicast discovery works without extra configuration (`testbed/docker-compose.yml:1-7`).

```bash
cd testbed
docker compose build
docker compose up -d
docker compose ps
```

`weld-jazzy` is the canonical peer for the gates below. The Humble and Rolling images exist for manual cross-distro checks.

Live tests use docker argv with no shell. `ImageEnv` replicates the Jazzy setup as `-e` flags so `ros2` runs directly as argv (`testbed/live/helpers/helpers.go:109-122`, `testbed/live/helpers/helpers.go:191`). Distro peers use the same pattern (`testbed/live/helpers/distro.go:110`). Context timeouts bound every docker and weld call. No `timeout` binary and no `mktemp`.

## Layout

- `testbed/unit/*`: pure Go, no Docker. Port math, SPDP marshal and parse, SEDP shape, data path. Each package holds one conformance_test.go file. `testbed/unit/launch/` is the launch-argument gate with fixtures plus expected dumps (see `testbed/unit/launch/conformance_test.go:199`).
- `testbed/live/*`: needs Docker and the already running `weld-jazzy` peer. Single Go entrypoint with docker argv and no shell. Tests use gate star test files plus roundtrip files with no generic live_test.go file and no empty files. Gates live in `testbed/live/gates`, per-peer checks in `testbed/live/cyclone` (domain 48 SPDP) and `testbed/live/ros` (domain 42 String round trip plus type matrix).
- `testbed/logs/`: compose volume for peer logs. Restart the fleet with `cd testbed && docker compose down && docker compose up -d`. Never run `compose up` per test; bring the fleet up once and reuse it. See `testbed/live/helpers/helpers.go:412`.

## Run

Canonical Go gates with no shell:

```bash
go test ./testbed/unit/... -count=1
```

```bash
go test -tags live -timeout 60m ./testbed/live/... -count=1
```

Live runs need `-timeout 60m`: the default 10m package timeout fires mid-suite with the peer present. The ros package holds 187 top-level tests and QoS cases budget 30m each (see `testbed/live/ros/gate_qos_full_test.go:38`); raise the timeout further if the ros matrix still exceeds it.

Per-package matrix (unit first, then live by package):

```bash
go test ./testbed/unit/cli/... -count=1
go test ./testbed/unit/datapath/... -count=1
go test ./testbed/unit/negative/... -count=1
go test ./testbed/unit/ports/... -count=1
go test ./testbed/unit/sedp/... -count=1
go test ./testbed/unit/spdp/... -count=1
```

```bash
go test -tags live ./testbed/live/gates/... -count=1
go test -tags live ./testbed/live/cyclone/... -count=1
go test -tags live -timeout 60m ./testbed/live/ros/... -count=1
go test -tags live ./testbed/live/cli/... -count=1
```

Skip-when-absent rule: every live test calls `SkipIfNoPeer` first and type
matrix tests also call `SkipIfNoTypeInterface`. A missing docker daemon, a
stopped peer, or a missing `ros2 interface show` type skips cleanly. Failed
wire, payload, or route asserts still fail. See
`testbed/live/helpers/helpers.go:412`, `testbed/live/helpers/live.go:271`,
and `testbed/live/helpers/distro.go:160`.

Sentinel versus live-ID rule: header types carry the per-run live ID in
`frame_id`; types without a string field use a fixed sentinel and rely on
per-run topic plus domain isolation. See
`testbed/live/ros/gate_type_matrix_test.go:43` plus live `frame_id` at
`testbed/live/ros/gate_type_matrix_test.go:382`, ID creation at
`testbed/live/helpers/helpers.go:130`, and topic derivation at
`testbed/live/helpers/helpers.go:143`. A missing type skips, a known
no-route pub skips after the local codec passes
(`testbed/live/helpers/live.go:351`), and backend errors fail
(`testbed/live/helpers/live.go:324`).

Unit is pure Go with no container. Live needs the already running `weld-jazzy` peer and skips otherwise. Go uses context timeouts on every docker and weld call plus volume log outputs under `testbed/logs/` and per-run live IDs: no `timeout` binary, no `mktemp`. Detached peers run via `docker exec -d` writing to `/testbed/logs/<run-id>.out` (host `./testbed/logs/<run-id>.out`), the host polls that file directly with no `docker exec cat` per poll, and cleanup kills the pidfile PID plus one scoped pkill per run. Per-run topics derive from `/chatter` plus the live ID (`testbed/live/helpers/helpers.go:130`, `testbed/live/helpers/helpers.go:143`).

One-time restart after a compose or logs volume change (user restarts, tests never start containers):

```bash
cd testbed && docker compose down && docker compose up -d
```

See [CLI](./cli.md) for the CLI flags the checks exercise.

Domain split (see [Testbed Go](./testbed-go.md)):

- 42: ROS String round trip on per-run topics derived from `/chatter`. Forward at `testbed/live/ros/roundtrip_test.go:45`, reverse at `testbed/live/ros/roundtrip_test.go:94` (`rosDomain` at `testbed/live/ros/roundtrip_test.go:24`).
- 48: SPDP acceptance. See `testbed/live/cyclone/gate1_spdp_test.go:20` (`gateDomain`).
- 47: single entrypoint live gate. See `testbed/live/helpers/helpers.go:40` (`LiveDomain`), gate 0 at `testbed/live/gates/gate0_test.go:37`, gate 1 at `testbed/live/gates/gate1_test.go:31`, gate 2 forward at `testbed/live/gates/gate2_test.go:27` and reverse at `testbed/live/gates/gate2_test.go:81`.
- 43 plus 44: distro matrix. Humble on 43 (`testbed/live/helpers/distro.go:46-50`), Rolling on 44 (`testbed/live/helpers/distro.go:54-58`), Jazzy canonical on 42 (`testbed/live/helpers/distro.go:38-42`).
- 60 through 69: ephemeral isolation. Cyclone liveliness uses 60 through 64 and ROS liveliness uses 65 through 69 plus per-process CLI isolation in 60 through 69, so ephemeral runs never hear 42, 47, or 48 (`testbed/live/helpers/helpers.go:49-52`).

Only one run per domain and topic at a time. Every live run uses one per-run live ID for payload isolation and reuses that ID for start plus assert. See `testbed/live/helpers/helpers.go:130`.

## Gates

Canonical Go entrypoint is `go test -tags live -timeout 60m ./testbed/live/... -count=1`.

- Gate 0 is offline via plain `go test` with no shell: port math for domains 42, 47, and 48 plus a Marshal and Parse SPDP round trip plus a launch corpus presence pin. See `testbed/live/gates/gate0_test.go:37` and `testbed/live/gates/gate0_test.go:149`. The launch argument corpus is `testbed/unit/launch/conformance_test.go:199` with fixtures such as `testbed/unit/launch/shared_nodes.yaml` and expected dumps such as `testbed/unit/launch/shared_nav.expected`.
- Gate 1 is live SPDP acceptance via direct RTPS with no trace grep: join the domain 47 SPDP multicast group, decode with `ParseAnnouncement`, and prove non-local receipt by GUID prefix inequality. It pins RTPS 2.1, vendor 01 10, unknown reader DATA, and zero deserialization failures. See `testbed/live/gates/gate1_test.go:31` and `testbed/live/cyclone/gate1_spdp_test.go:370`.
- Gate 2 is the ROS String round trip with per-run live IDs and no `timeout` binary, no `mktemp`: forward (Weld echo sees the ROS talker) plus reverse (`ros2 topic echo` sees Weld pub), each asserting only its own run ID with context timeouts and volume log outputs polled directly from the host. See `testbed/live/gates/gate2_test.go:27` and `testbed/live/gates/gate2_test.go:81` plus `testbed/live/ros/roundtrip_test.go:45` and `testbed/live/ros/roundtrip_test.go:94`. The ROS type matrix in `testbed/live/ros/gate_type_matrix_test.go:105` (cases) with tests at `testbed/live/ros/gate_type_matrix_test.go:770` extends Gate 2 to Int32 plus Float64 plus Bool plus Vector3 plus PointStamped plus Temperature plus FluidPressure with the same forward plus reverse shape and `ros2 interface show` skip (`testbed/live/helpers/live.go:271`).

Cyclone SPDP and ROS String stay separate. SPDP checks assert wire fields and never assert String payloads. ROS checks assert String payloads and never parse SPDP trace. Scope is pinned in `testbed/live/ros/doc.go:4-13`. Full wire end state is in [Conformance](./conformance.md).

## Conflict

A stale container name fails `up` with `Conflict. The container name "/weld-rolling" is already in use`. Remove it first:

```bash
docker rm -f weld-rolling weld-humble weld-jazzy
docker compose up -d
```

## Teardown

```bash
docker compose down
```

`docker compose down --rmi all` frees about 8 GB.

## Current status

The wire path is live: `cmd/weld` publishes and echoes `std_msgs/String` through the backend in `internal/transports/dds`, and the gates above check it end to end.

Gate record: gate 0 green; gate 1 green (about 900 non-local SPDP ST0 NEW rows, `bes 3f`, zero deserialization failures); gate 2 forward green after plain CDR receive plus SEDP fixes and reverse green after writer `03c2` plus announce now plus Heartbeat. Forward plain CDR is in `internal/transports/dds/datapath.go:746` with String fallback at `internal/transports/dds/datapath.go:479` and the `TakeFrom` fallback at `internal/transports/dds/datapath.go:887`. SEDP writer `03c2` is in `internal/transports/dds/sedp.go:149-157`, announce loop is in `internal/transports/dds/sedp.go:633`, DATA plus Heartbeat is in `internal/transports/dds/backend.go:1347`. The Go testbed mirrors this: `go test ./testbed/unit/...` runs offline, `go test -tags live ./testbed/live/...` runs the domain 47 round trip. See [Conformance](./conformance.md) for the wire record.
