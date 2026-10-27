# Testbed

Layout: `testbed/unit/*` is pure Go with no Docker; `testbed/unit/launch/` is the launch-argument gate (`testbed/unit/launch/conformance_test.go:199`); `testbed/live/*` needs Docker and the already running `weld-jazzy` peer; `testbed/logs/` is the compose log volume. ROS type gates live in `testbed/live/ros/gate_type_*_test.go` including rest2 gates `testbed/live/ros/gate_type_std_rest2_test.go:100`, `testbed/live/ros/gate_type_geometry_rest2_test.go:71`, `testbed/live/ros/gate_type_viz_rest2_test.go:33`, `testbed/live/ros/gate_type_diag_rest2_test.go:398`, and `testbed/live/ros/gate_type_misc_rest2_test.go:86`.

File names: `testbed/unit/*` uses conformance_test.go files; `testbed/live/*` uses gate star test files plus roundtrip files; no generic live_test.go file remains and no empty files. Test names use `TestCyclone*` for Cyclone SPDP, `TestROSType*` for ROS types, and `TestGate*` for single entrypoint gates.

## Run

```bash
go test ./testbed/unit/... -count=1
go test -tags live -timeout 60m ./testbed/live/... -count=1
```

Live runs need `-timeout 60m`: the default 10m package timeout fires mid-suite with the peer present. The ros package holds 187 top-level tests and QoS cases budget 30m each (see `testbed/live/ros/gate_qos_full_test.go:38`); raise the timeout further if the ros matrix still exceeds it.

Live tests use docker argv with no shell, context timeouts on every docker and weld call, and skip when no daemon or peer is present. See `testbed/live/helpers/helpers.go:191`, `testbed/live/helpers/helpers.go:327`, `testbed/live/helpers/helpers.go:412`. Never run `compose up` per test. Bring the fleet up once with `cd testbed && docker compose up -d` and reuse it. Restart a stale fleet with `cd testbed && docker compose down && docker compose up -d`.

## Domains

- 42: ROS `std_msgs/String` on per-run topics from `/chatter`. See `testbed/live/ros/roundtrip_test.go:24`, forward at `testbed/live/ros/roundtrip_test.go:45`, reverse at `testbed/live/ros/roundtrip_test.go:94`.
- 48: SPDP acceptance. See `testbed/live/cyclone/gate1_spdp_test.go:20`, pure Go at `testbed/live/cyclone/gate1_spdp_test.go:370`, live at `testbed/live/cyclone/gate1_spdp_test.go:411`.
- 47: single entrypoint live gate. See `testbed/live/helpers/helpers.go:40`, gate 0 at `testbed/live/gates/gate0_test.go:37`, gate 1 at `testbed/live/gates/gate1_test.go:31`, gate 2 forward at `testbed/live/gates/gate2_test.go:27` and reverse at `testbed/live/gates/gate2_test.go:81`.
- 43 plus 44: distro matrix. Humble on 43 (`testbed/live/helpers/distro.go:46-50`), Rolling on 44 (`testbed/live/helpers/distro.go:54-58`).
- 60 through 69: ephemeral isolation for liveliness and CLI checks (`testbed/live/helpers/helpers.go:49-52`).

Only one run per domain and topic at a time. Every live run uses one per-run live ID for payload isolation. See `testbed/live/helpers/helpers.go:130`.

## Split

Cyclone SPDP and ROS String stay separate. SPDP checks assert wire fields and GUID prefix inequality, never String payloads. ROS checks assert String payloads, never SPDP wire data. Scope is pinned in `testbed/live/ros/doc.go:4-13`. See [Testbed Go](../docs/testbed-go.md).

## Gates

Gate 0 green (pure Go). Gate 1 green (about 900 non-local SPDP ST0 NEW rows, `bes 3f`, zero deserialization failures). Gate 2 forward green after plain CDR receive (`internal/transports/dds/datapath.go:746`, fallback at `internal/transports/dds/datapath.go:479`) plus SEDP fixes; reverse green after writer `03c2` (`internal/transports/dds/sedp.go:149-157`) plus announce loop (`internal/transports/dds/sedp.go:633`) plus Heartbeat (`internal/transports/dds/backend.go:1347`). Full record is in [Conformance](../docs/conformance.md) and [Testbed](../docs/testbed.md).

## Type coverage

124 corpus rows (`internal/types/corpus.go:108`): live 114, pure-Go-only 3, service 7, missing 0. Every live gate calls `SkipIfNoPeer` first and skips when the peer or type is absent. Services have no rt topic gate; no missing rows remain. The 13 former `std_msgs` missing rows (`Int16`, `Int64`, `UInt64` plus 10 multi-array types) now have live gates in `testbed/live/ros/gate_type_std_multi_test.go` with pure-Go pins in `testbed/unit/std_multis_extra_test.go`. Rest2 alias gates in `testbed/live/ros/gate_type_std_rest2_test.go:100`, `testbed/live/ros/gate_type_geometry_rest2_test.go:71`, `testbed/live/ros/gate_type_viz_rest2_test.go:33`, `testbed/live/ros/gate_type_diag_rest2_test.go:398`, and `testbed/live/ros/gate_type_misc_rest2_test.go:86` reuse existing corpus rows. `std_msgs/ByteMultiArray` forward uses per-element `!!binary` YAML (`testbed/live/ros/gate_type_std_rest_test.go:300`) because the integer list form crashes the ROS CLI talker with SIGABRT (see `docs/interop.md:124`). Per-type gates and pins are in [Conformance](../docs/conformance.md).
