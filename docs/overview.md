# Overview

Weld is Go middleware for distributed robot communication. It is ROS 2 wire compatible, speaks RTPS with XCDR1 CDR against CycloneDDS peers, and runs a leaderless data plane with zero coordination.

The relation is CockroachDB to Postgres: same wire protocol and operator surface, different system behind it. Compatibility lives on the wire. Quality lives inside Weld.

## Wire vs system

- Wire: RTPS discovery and liveliness, topic and type naming, type hashes.
- Wire: XCDR1 CDR for standard messages, services, actions; TF topics, `/clock`, bags, launch args.
- Wire: every `ros2cli` verb and graph query returns the same answers.
- System: one node type with supervision, no `rmw` handles or `rcl` wait sets.
- System: channel delivery with explicit overflow, no executor plus callback group split.
- System: loud QoS and type mismatch, no silent drop or global error state.

## Goals

- Go native system an operator would choose even with no ROS present.
- Bit level wire compatibility with CycloneDDS peers in v1, proven by interop gates.
- Nav2 and MoveIt 2 run unmodified on Weld.
- Recurring ROS 2 failure classes are structurally impossible, not merely handled.

## Non-goals for v1

- No source compatibility with `rclcpp`.
- No intra process transport.
- No zero copy loans or shared memory.
- No bag storage engine, only exchange formats.
- Direct RTPS backend against CycloneDDS peers is the only transport.

## One binary

Weld ships no C ABI. One Go binary talks to Humble, Jazzy, and Rolling by negotiating per peer on type hashes and introspection topics. Parsing is tolerant and sending is strict; version patchwork lives in backends, not core.

## Next

- Design: `docs/design.md`.
- Wire encoding and transport: `docs/wire.md`, `docs/conformance.md`.
- Core system: `docs/core.md`, `internal/core/`.
- Peers and live gates: `docs/testbed.md`.
