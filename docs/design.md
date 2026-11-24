# Why Weld Exists

Weld is Go middleware for distributed communication that happens to be ROS 2 compatible. The relation is CockroachDB to Postgres: same wire protocol and observable surface, different system behind it. In a warehouse deployment a Go program named `fleet_planner` publishes `geometry_msgs/msg/Twist` on `/cmd_vel` at 20 Hz while a C++ Nav2 stack on `rclcpp` subscribes on the same domain; `ros2 topic echo /cmd_vel` and `ros2 node list` show no difference, and scheduling, configuration, time, errors, and supervision all differ behind the wire.

## Wire vs system

Fixed on the wire: DDS RTPS with XCDR1 CDR, SPDP and SEDP discovery, topic and type naming with type hashes, standard messages, services, actions, parameters, TF topics, `/clock`, bags on disk, launch args, every `ros2cli` verb.
Replaced inside Weld: `rmw` handles, `rcl` wait sets, executor plus callback group split, Node versus LifecycleNode duality, global error state, allocator plumbing, silent QoS mismatch, daemon cached graphs.

```
    operator view (unchanged)          system view (replaced)
    -------------------------          ----------------------
    ros2 topic echo /odom              executor, wait set, callback groups
    ros2 service call /plan            rmw handles, global error state
    rviz2 rendering /tf                daemon cached graph
           |                                    |
           v                                    v
    wire bytes (RTPS, CDR)  Go core (matching, routing, supervision)
```

Above the line bytes and verbs stay fixed. Below the line Weld is free to design.

## Goals and non-goals

- Go native system an operator would choose even with no ROS present.
- Bit level wire compatibility with CycloneDDS peers in v1, proven by interop gates.
- Nav2 and MoveIt 2 run unmodified on Weld.
- Recurring ROS 2 failure classes are structurally impossible, not merely handled.
- Non-goal: no source compatibility with `rclcpp`; the Go API is designed.
- Non-goal: no intra process transport in v1; local delivery honors QoS or rejects.
- Non-goal: no zero copy loans or shared memory in v1.
- Non-goal: no bag storage engine in v1, only exchange formats.
- Non-goal: direct RTPS backend against CycloneDDS peers is the only v1 transport.

## Invariants

- Compatibility is proven on the wire against real ROS 2, never assumed from shared code.
- One concept per concern: one node type, one config store, one clock per context, one error path.
- Cancellation is total: every blocking call takes `context.Context`; teardown is idempotent.
- Mismatch is loud: QoS, type hash, and clock disagreements surface in logs, metrics, CLI.
- No global state: no shared default context, no error register, no ambient allocator.
- Determinism is testable: clocks, networks, and discovery are interfaces with fakes.
- Optimizations are invisible: fast and slow paths share one contract or reject at creation.

## Architecture

Three layers: `internal/core/` is the transport free system with entities, matching, routing, state machines, config, clock, graph index; `internal/transports/` holds the DDS RTPS backend fed by `internal/types/` for schema, hashes, and codec.
The rule that keeps vendor drift out: `internal/core/` never imports a transport package. The backend implements publish bytes, take bytes, join discovery, and report liveliness; all matching, routing, timeouts, retries, and state machines live once in core.
The backend is a translator at the border. Inbound it parses CDR, checks type hash, matches QoS, and delivers. Outbound it serializes and announces in discovery as ROS peers expect. Past vendor drift notes are archived in `docs/interop.md`; v1 targets CycloneDDS only.

## Core designs

### One node with supervised lifecycle

One node type only. Lifecycle is a supervision policy on entities, not a second class.
The supervisor brings entities up forward and down in reverse with bond style liveliness and respawn. Late created entities auto activate when the node is active.
Bringup order such as `motor_driver` before `odometry_filter` before `planner_server` lives in the supervisor and is tested once. Deactivation ends terminal results and stops new work.

### Structured delivery instead of executors

Subscriptions deliver to buffered channels with QoS depth and explicit overflow such as drop oldest with a lost counter.
User code ranges over channels or joins a supervision group: serial for ordered single flight, pooled for bounded parallelism with fairness. Ordering is stated per mode and tested.
Copy once at ingress is default: share by reference after entry, serialize only at the network edge. Delivery is benchmarked with idle timers plus bursty topics.

### QoS worn as a facade

Weld advertises reliable plus transient local with bounded keep last, which satisfies standard ROS subscriptions, so matching rarely fails.
Behavior comes from core: backpressure, bounded retention with explicit eviction, drop oldest with lost counters, deadlines as monitoring alerts.
Intent is three values only: guarantee for retry, retention bound for how many and how long, liveness for aliveness plus deadline both ways. Priority is scheduling in supervision groups.

### Services as calls and actions as workflows

Services are plain RPC with context deadlines: exact one to one routing, per call timeouts, gone servers and gone clients error one call.
Actions are durable core workflows: goal admission, cancel propagation, buffered feedback before handle attachment, result caching, guaranteed terminal states.
The wire stays standard request plus reply and goal plus feedback plus result plus status topics. Weld validates limits, names, QoS, and clocks before sending.

### Config as a versioned store

Parameters are a versioned config store in `internal/core/` with schema, validation, atomic updates, watches, and history.
Startup applies files, CLI, and code in fixed precedence with one batched event. `on_set` validation splits from post set apply.
The standard parameter services stay on the wire for `ros2cli` and Nav2, served from the store. Schemas catch wrong units and unknown joints at build time.

### Time as one domain per context

One clock domain per context with explicit wall, steady, and sim sources plus a construction time fallback, never silent.
Jump callbacks are allocation free with ordered deregistration; overload policy is explicit and observable.
When `/clock` jumps back five seconds in a bag loop, every timer in that context observes the same jump. Fake clocks drive chaos tests.

### Discovery as an index

Discovery feeds one index with a generation counter and change hook. The index is partition local, never global truth.
Records carry type hash plus unique GID from day one; send is strict and receive is tolerant. No quorum is pretended and no query blocks across a dead link.
Graph snapshots serve tools from the index without blocking the data path. A WiFi flap from generation 42 to 43 shows each node exactly once.

### No elections in the data plane

Weld elects nothing. Publish, subscribe, services, actions, discovery, and the graph index work with zero coordination.
Every topic, service, and parameter has exactly one owner, its creator, so there is nothing to agree on. One node behaves like one hundred.
Where ROS expects exactly one of something, configuration names it explicitly; on death Weld reports absence loudly. Fleet brains are Concord on top of Weld.

### Types content addressed from day one

The registry in `internal/types/` keys everything by type hash with exact canonicalization. Unknown types are first class without codegen.
Generic byte endpoints, bag reads, and CLI echo work from hashes alone. Deserialization validates header plus length, then fuzzes truncated input.
It serves descriptions and introspection fields and reproduces string, sequence, handle, symbol, and bound shapes byte for byte.

### Observability built in rather than bolted on

Delivery introspection, QoS mismatch events with counts and policy kinds, service and action tracing hooks, and TF lookup error taxonomy ship in v1.
Tracepoints reuse tracetools names where they overlap so existing analysis tooling keeps working.
When `/tf` from `base_link` to `odom` stops, the trace names matching, history, or delivery as the drop point. Silence is a defect.

## Compatibility surface

- RTPS discovery and liveliness with domain isolation and localhost modes.
- Topic and type naming with demangling, for example `/cmd_vel` round trips.
- XCDR1 CDR for standard messages plus service envelopes with GUID and sequence.
- QoS matching per shared truth table with actual getters for resolved values.
- Services with sequence pairing and dual QoS fanout on the wire.
- Actions as goal, cancel, feedback, result, and status with preempt and terminal states.
- Parameter declare, get, set, describe, list, events, and overrides.
- TF topic split with latched statics and lookup error taxonomy.
- `/clock` publishing and sim time agreement across Weld and `rclcpp`.
- Graph queries with counts, endpoint info, and type hashes.
- Bag exchange formats on disk for info and replay.
- Launch argument acceptance plus CLI verbs: list, echo, pub, info, call, params, doctor.

## One binary

Weld ships no C ABI. Per peer negotiation covers type hash presence, introspection topics, and message hashes, so Humble, Jazzy, and Rolling peers each match at once. The patchwork lives in `internal/transports/`; core and the Go API stay single.

## Packages

- `internal/types/`: schema registry, hashes, descriptions, codec, dynamic access, fuzz corpus.
- `internal/transports/`: backend interface plus `dds`; no core logic, no plugin registry.
- `internal/core/`: entities, matcher, router, services, workflows, config, clock, graph, supervision.
- `internal/time/`: clock sources, fake clocks, jump handling, rate policies.
- `internal/graph/`: snapshots, compatibility reporting, tool queries.

## Proof plan

Gate 1: unit tables with fake clocks where jumps and partitions are deterministic.
Gate 2: interop runs against `rclcpp` on CycloneDDS with `ros2cli`, for example echoing `/chatter` at 10 Hz across implementations.
Gate 3: chaos runs under race detection with loss, late joiners, discovery flaps, teardown mid run, and clock jumps, closed by Nav2 plus MoveIt 2 soaks with RViz rendering.

## Build order

1. Foundations: types plus CDR plus hash plus fuzz.
2. First contact: DDS unreliable round trip plus CLI echo.
3. Matching: loud mismatch events with policy kinds.
4. Visibility: graph index plus topic verbs.
5. Calls: services with deadlines and one to one routing.
6. Configuration: versioned store with batched events.
7. Long tasks: action workflows with buffered feedback.
8. Supervision, fidelity, soak: groups plus bags, TF, `/clock`, Nav2 and MoveIt 2.

## Open questions

- RESOLVED: v1 interoperates with Humble, Jazzy, Rolling against CycloneDDS.
- RESOLVED: Weld links no C code and needs no C toolchain in builds.
- RESOLVED: v1 scope is CDR plus types plus DDS unreliable round trip plus CLI echo.
- RESOLVED: DDS via CycloneDDS only in v1.
- RESOLVED: static transform overwrite replaces on rebroadcast; clear is sticky.
- RESOLVED: `/tf` reliability default is reliable volatile depth 100.
- RESOLVED: action goal timeout defaults to 60 s unless the client sets a value.
- RESOLVED: service history honors each entity profile; mismatch surfaces loudly.
