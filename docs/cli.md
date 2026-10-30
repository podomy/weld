# CLI

`cmd/weld` is the Weld command you run in a terminal. It is the composition root: the only place allowed to join the core (matching, QoS) with the transports (DDS bytes). Libraries stay separate; the binary wires them together.

## Commands

- `echo`: prints `std_msgs/String` payloads on a topic until the timeout or interrupt. This is the interop check: if Weld sees the ROS talker, bytes flow.
- `pub`: sends one `std_msgs/String` message on a topic, then exits. The message comes from `--text` or from one positional arg, so `weld pub --text hello` and `weld pub hello` send the same bytes. `--text` wins when both are given.

Both take a topic (default `/chatter`) and a domain flag (default from `ROS_DOMAIN_ID`, fallback 0). An invalid `ROS_DOMAIN_ID` is reported at startup with exit 2. Give each test run its own domain id so fleets stay apart.

## Exit codes

- `0`: ok. `echo` also returns 0 on timeout or interrupt after printing what it saw.
- `1`: runtime failure (backend, publish, decode, print). The message goes to stderr, first line only, with a `weld:` prefix naming the operation, for example `weld: take bytes: ...` or `weld: publish bytes: ...`.
- `2`: usage failure (bad flags, bad topic, bad domain, missing pub text). Flag parse errors go to stderr; `-h` and `--help` print help to stdout with exit 0.

## Options

- `--topic`: ROS topic name, must start with `/`. Default `/chatter`.
- `--domain`: DDS domain id, 0 through 101. Defaults to `ROS_DOMAIN_ID` with fallback 0. A bad value fails fast with exit 2 before any socket opens.
- `--timeout` (echo only): how long echo runs. Default `30s`. `0` runs until interrupt; negative values are rejected with exit 2.
- `--text` (pub only): message data. One positional arg works instead; giving two or more args fails with exit 2.

`ROS_DOMAIN_ID` is read once at startup to set the `--domain` default. A bad value there does not break help output; the failure is deferred to command setup, where it is reported as a usage error.

## Counters

Gap and drop counters go to stderr at most once per second so logs stay readable: `weld: lost N messages` when the backend reports skipped sequence numbers, and `weld: dropped N messages` when a corrupt or duplicate sample is skipped. Payload text always goes to stdout, one message per line.

## Build and run

```bash
cd cmd/weld && GOWORK=off go build -o /tmp/weld ./...
/tmp/weld echo --domain 42
/tmp/weld pub --domain 42 --topic /chatter --text hello
```

`GOWORK=off` is needed because `cmd/weld` is its own Go module with a replace directive to the parent (see the comment in `cmd/weld/go.mod`). The parent `go.work` covers only the root and `sdk`, so building the CLI with the workspace enabled would miss the replace.

## Current state

Both commands run against the live backend in `internal/transports/dds`: `echo` joins discovery and prints what the ROS talker publishes, `pub` publishes one message and prints `sent: <text>`. `echo` declares its SEDP reader through `Subscribe` (`cmd/weld/main.go:242`, used at `cmd/weld/main.go:264`). `pub` waits for SPDP routes: it retries `PublishBytes` on `ErrNoRoute` every 50 ms up to 5 s (`cmd/weld/main.go:47-50`, `cmd/weld/main.go:370`). The Go gates automate the full check on isolated domains with `go test ./testbed/unit/... -count=1` for offline checks and `go test -tags live ./testbed/live/... -count=1` for live checks: they build the CLI, start a CycloneDDS talker in `weld-jazzy`, run `echo` with a timeout, then reverse the direction with `weld pub` against `ros2 topic echo`. The checks are `testbed/live/ros/roundtrip_test.go:45` plus `testbed/live/ros/roundtrip_test.go:94` on domain 42 and `testbed/live/gates/gate2_test.go:27` plus `testbed/live/gates/gate2_test.go:81` on domain 47 with per-run live IDs (`testbed/live/helpers/helpers.go:130`). Domain split is 42 for ROS String, 48 for SPDP, 47 for the single entrypoint live gate. Only one run per domain and topic at a time; never start the compose stack per run.
