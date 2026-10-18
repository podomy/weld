# Why Weld Exists

Weld is a superior robotics middleware that happens to be ROS 2 compatible. The relationship is CockroachDB to Postgres: same wire protocol, same observable surface, entirely different system behind it.

Why this comparison matters can be seen in a concrete deployment. Imagine a warehouse robot where a Go program called `fleet_planner` publishes `geometry_msgs/msg/Twist` on `/cmd_vel` at 20 Hz, while a C++ Nav2 stack built on `rclcpp` subscribes on the same domain. The operator runs `ros2 topic echo /cmd_vel` and `ros2 node list` and sees no difference. Discovery finds both sides. Bytes flow. Tools work. What changed is everything the operator cannot see on the wire: scheduling, configuration, time handling, error reporting, and supervision. That split is the whole book. Compatibility lives on the wire. Quality lives inside Weld.

What stays compatible is the wire and the surface that tools touch. This includes DDS RTPS with XCDR1 CDR, Zenoh keys with liveliness tokens and attachments, SPDP and SEDP discovery, topic and type naming, type hashes, graph queries, standard messages, services, actions, parameters, TF topics, `/clock`, bags on disk, launch arguments, and every `ros2cli` verb. A Weld node and an `rclcpp` node on the same domain discover each other and exchange data with no forked protocol.

What gets thrown away is everything that never appeared on the wire. This includes the `rmw` handle model, the `rcl` wait set mechanics, the executor plus callback group split, the Node versus LifecycleNode duality, global error state, allocator plumbing, fixed size parsing, per vendor QoS drift, silent mismatch, and daemon cached graphs. None of that is wire. All of it is replaceable. Full evidence appears in the companion chapters on the ROS world, and in the chapter that distills the cross repository issue analysis.

```
  operator view (unchanged)          system view (replaced)
  -------------------------          ----------------------
  ros2 topic echo /odom              executor, wait set, callback groups
  ros2 service call /plan            rmw handles, global error state
  rviz2 rendering /tf                daemon cached graph
         |                                    |
         v                                    v
  wire bytes (RTPS, CDR, Zenoh keys)  Go core (matching, routing, supervision)
```

The diagram states the contract. Above the line, bytes and verbs stay fixed. Below the line, Weld is free to design.

## Goals and Non Goals

A goal here means a property the project will defend in review. A non goal means a property the project explicitly defers so the first release stays coherent.

The goals are:

- A Go native system an operator would choose even with no ROS present: one node model, one config model, one clock model, one error model, observable by default.
- Bit level wire compatibility with CycloneDDS, FastDDS, and `rmw_zenoh` peers, verified by interop gates, not by sharing code.
- Nav2 and MoveIt 2 run on Weld unmodified, because the surface they touch is compatible, not because Weld imitates ROS internals.
- Every recurring ROS 2 failure class in `docs/ros/19-failure-analysis.md` is structurally impossible in Weld, not merely handled.

The non goals for the first version are deliberate limits:

- No source compatibility with `rclcpp`. The Go API is designed, not transliterated.
- No intra process transport in version one. Local delivery honors QoS or rejects at creation; that work follows network interop.
- No zero copy loans or shared memory in version one. Shared pointer delivery plus the byte path covers throughput first.
- No bag storage engine in version one. Weld reads and writes the exchange formats.

Consider what this means for a perception pipeline. A camera node named `usb_cam` publishes `sensor_msgs/msg/Image` on `/image_raw` at 30 Hz with a 640 by 480 frame. Weld does not promise zero copy delivery of that frame in version one. It does promise that the bytes on the wire decode identically under CycloneDDS and FastDDS, that a late joining recorder still reads the bag, and that a QoS disagreement surfaces loudly instead of producing silence.

## Invariants

Invariants are rules that hold across the whole codebase. Review can appeal to them without rearguing the case.

- Compatibility is proven on the wire, never assumed from shared code. Every feature carries an interop test against real ROS 2.
- One concept per concern. One node type, one config store, one clock source per context, one error path. Duplicated concepts such as two node classes, three time types leaking into user code, or two parameter paths are rejected in review.
- Cancellation is total. Every blocking call takes a `context.Context`. Teardown is idempotent and never throws, crashes, or deadlocks. A node named `localizer` that shuts down while a service call to `/global_localize` is in flight must return the call with cancellation, then release resources, then return cleanly on a second close.
- Mismatch is loud. QoS, type hash, and clock domain disagreements surface in logs, metrics, and CLI. Silence is a defect, never a state. If `/scan` stops flowing because reliability was requested as reliable but offered as best effort, the operator learns the topic name, the endpoint pair, and the policy kind.
- No global state. No shared default context, no error register, no ambient allocator. Handles are explicit. Two nodes in one process, for example `mapper` and `tracker`, never share hidden singletons.
- Determinism is testable. Clocks, networks, and discovery are interfaces with fake implementations, so every timing and partition behavior runs deterministically in tests. A bag loop with a backward jump of five seconds or a discovery flap that drops `slam_toolbox` for 200 ms becomes a unit test, not a field report.
- Optimizations are observationally invisible. A fast path and a slow path share one contract covering ordering, delivery, ownership, lifetime, and failure modes. If a shortcut cannot honor the contract it is rejected at creation with an error, never silently engaged. Two behaviors means two named features, never one API with split personality. Copy once at ingress is the standing example: user code cannot tell which path core picked.

## Architecture

Weld separates what the robot program sees, what the system decides, and what the wire carries. Three layers carry those jobs.

```
user robot code (Go)
  |
  v
sdk/                     /designed API, not transliterated/
  Node, Topics, Services, Workflows (actions),
  Config, Time, Supervision
  |
  v
internal/core/            /the system, transport free/
  entities, matching, routing, state machines,
  config store, clock domain, graph index
  |
  v
internal/transports/      /three industry buses in the box/
  dds/        zenoh/       mqtt/
  RTPS/CDR    keys/tokens  topics/retained
              attachments  will/QoS mapping
  ^
  | all three fed by
internal/types/  (IDL registry, RIHS01 hashes, XCDR1 codec)
```

The rule that keeps vendor drift out is simple to state. `internal/core` never imports a transport package. Transports implement a small backend interface: publish bytes, take bytes, join discovery, report liveliness. All matching, QoS, routing, timeouts, retries, and state machines live once in core, so DDS, Zenoh, and MQTT behave identically by construction. Vendor drift, the largest ROS interop cost, has nowhere to hide.

Three buses cover the industry: DDS, Zenoh, and MQTT all ship in the box. Anything else arrives the same way everything else in this project arrives: a pull request adding a backend behind the interface, with interop tests. No plugin registry, no dynamic loading, no out of tree drivers to version skew against.

## Core Designs

Each design below removes a class of field failure by removing the mechanism that allowed it. The pattern is elimination, not handling.

### One Node with Supervised Lifecycle

There is one node type. Lifecycle is a supervision policy attached to entities, not a second node class. Managed entities declare startup and shutdown order; the supervisor brings them up forward and down in reverse, with bond style liveliness and respawn policy built in. Late created entities auto activate when the node is active. Deactivation ends terminal results in flight and stops accepting new work.

Why this matters shows up in bringup. A mobile base often starts `motor_driver` before `odometry_filter` before `planner_server`. Today that order lives in launch files and shell scripts. In Weld it lives in the supervisor, tested once. This collapses the Node versus LifecycleNode duality, the activation gap recorded as `navigation2#5298`, and the bringup ordering races into one tested supervisor.

### Structured Delivery Instead of Executors

There is no executor with wait sets and callback groups. Subscriptions deliver to buffered channels owned by the subscriber, with depth from QoS and explicit overflow policy such as drop oldest with a lost counter, never silent. User code ranges over the channel, runs callbacks in its own goroutines, or attaches to a supervision group with declared concurrency: serial (ordered, one at a time) or pooled (bounded parallelism with fairness). Serial replaces exclusive groups without starvation ambiguity; pooled replaces reentrant groups without unbounded fan out.

No warmup spins, no per cycle allocation, no shared status conditions. Ordering guarantees are stated per mode and tested. A concrete case is `/odom` at 50 Hz feeding both a logger and a controller. The logger can use a pooled group with depth 100 and tolerate drops with a counter. The controller can use a serial group with depth 10 and require order. This eliminates the ordering dispute, group starvation, busy return storms, and temp churn classes.

Copy once at ingress is the default, not an optimization mode. The first copy happens where data enters the system, from the device or kernel buffer Weld does not own. After that everything shares by reference: multiple subscribers, history caches, retransmits, intra process handoff. One copy, reference counted, freed when the last reader releases it. Users never see loans; core picks the shared path wherever the transport allows and serializes only at the network edge.

### Services as Calls and Actions as Workflows

Services are plain RPC with context deadlines: exact one to one routing, per call timeouts, gone servers and gone clients error one call. Actions are durable workflows in core, not user assembled topics: goal admission, cancel propagation, feedback streaming with buffering before handle attachment, result caching independent of handle lifetime, terminal states guaranteed. Timeouts attach at the transition that starts them.

The wire stays standard (request plus reply topics, goal plus feedback plus result plus status topics), but Weld users never hand roll the machine. Consider `NavigateToPose` sent to `/navigate_to_pose` with a goal UUID of `a3f1`. Feedback at 5 Hz buffers even if the client attaches late. The result survives after the server handle closes. Gone before result, feedback loss, take races, broadcast responses, and missing timeout classes disappear because the workflow owns the state.

### Config as a Versioned Store

Parameters become a versioned config store with schema, types, validation, atomic updates, watches, and history. Startup applies files, CLI, and code in a fixed precedence with one batched event, not N events. `on_set` validation splits from post set apply. Schemas validate generated YAML before launch, catching missing wrappers, wrong units, and unknown joints at build time instead of runtime.

The standard parameter services stay on the wire for `ros2cli` and Nav2, served from the store. A practical example is `controller_server` loading `planner.yaml` where `max_vel_x` must be a double in meters per second. A string value of `fast` fails schema validation before the robot moves. This eliminates event floods, precedence ambiguity, cap crashes, and config drift.

### Time as One Domain per Context

One clock domain per context with explicit wall, steady, and sim sources and a construction time fallback, never silent. Jump callbacks are allocation free with ordered deregistration. Overload policy is explicit and observable. Per subsystem rate choices (Nav2 wall loops versus ROS behaviors) are declared policy, not accidents. Deterministic fake clocks drive chaos tests: bag loops, backward jumps, pauses, and multi rate sim.

Why one domain matters is easiest to see with `/clock`. When sim time publishes 1000 as the current second and then jumps back to 900 during a bag loop, every timer in that context observes the same jump through the same callbacks. There is no hidden fallback to wall time. This eliminates fallback surprises, hidden backlog, and deregistration crash classes.

### Discovery as an Index

Discovery feeds one index with a generation counter and a change hook. The index is explicitly partition local: it describes the segment this node can currently see, never a global truth. No quorum is pretended, no query blocks across a dead link, and no Weld decision assumes two nodes see the same graph. Coordination that genuinely needs agreement stays out of the data plane; that is Concord's territory, not Weld's. Endpoint records carry type hash plus unique GID from day one. Strict send with tolerant receive applies to tokens and attachments. Optional static topology offers presized tables and no thread for embedded targets. Graph snapshots serve tools with hidden filtering and same name disambiguation.

For an operator, this means `ros2 node list` after a flaky WiFi reconnect shows `talker` and `listener` exactly once, with generation `42` to `43` marking the flap. This eliminates stale lists, daemon divergence, invisible nodes, and missing introspection.

### Types Content Addressed from Day One

The registry keys everything by type hash with exact canonicalization. Unknown types are first class: generic byte endpoints, bag reads, and CLI echo work without codegen. Deserialization validates header plus length before parsing and fuzzes truncated input.

A field example helps. A bag containing `velodyne_msgs/msg/VelodyneScan` with 300 packets recorded on another machine opens on a laptop with no Velodyne code installed. The CLI echoes the raw fields, the type hash `RIHS01_abc` matches the discovery record, and truncated packets fail with a bounds error rather than a crash. This eliminates hash mismatch, unknown type second class status, and CDR crash classes.

### Observability Built In Rather Than Bolted On

Delivery introspection (which path a message took), QoS mismatch events with counts plus policy kinds, service and action tracing hooks, and TF style lookup error taxonomy with exact strings ship in version one. Tracepoints use the same names as `tracetools` where they overlap so existing analysis tooling keeps working.

Concretely, when `/tf` from `base_link` to `odom` stops at 10 Hz, the trace shows whether the drop happened at matching, at the history cache, or at delivery, with counters. This eliminates the silent failure and undebuggable drop classes.

## Compatibility Surface

Weld proves each item below against real ROS 2; the list is closed, and anything unlisted is free to be Weld native. Each bullet is a promise that interop tests enforce.

- RTPS discovery and liveliness with domain isolation, localhost modes, and multi NIC.
- Topic and type naming with namespace avoidance rules, demangling both directions. For example `/cmd_vel` maps to its RTPS name and back without loss.
- XCDR1 CDR for every standard message plus service envelopes with GUID plus sequence.
- QoS matching per the shared truth table with actual getters exposing resolved values.
- Services with sequence pairing and dual QoS fanout semantics on the wire.
- Actions as goal, cancel, feedback, result, and status exchanges with preempt and terminal states.
- Parameter services with declare, get, set, describe, list, events, and overrides.
- TF topic split with latched statics, lookup error taxonomy, and jump behavior.
- `/clock` publishing and sim time agreement. Sim second `1234` means the same tick in Weld and in `rclcpp`.
- Graph queries with counts, endpoint info, and type hashes.
- Bag formats on disk for info and replay.
- Launch argument acceptance with full fidelity.
- CLI verbs: list, echo, pub, info, call, params, doctor checks, node info.

The contract is interaction, not structure. A program written against `rclcpp` or `rclpy` interacts with Weld flawlessly: same topic names and types, same service and action exchanges, same parameters, same graph answers, same TF behavior. Underneath, Weld carries no `rcl` equivalent structs: no node handles mirroring `rcl_node_t`, no wait sets, no callback groups, no allocator plumbing. Any struct that exists only to serve ROS internals rather than the interaction simply does not exist in Weld. Compatibility is proven by mixed fleet behavior, never by shape matching.

## Packages

Packages follow the architecture diagram. Each package has one job and one direction of dependency.

- `internal/types`: registry, hashes, descriptions, XCDR1 codec, dynamic access, fuzz corpus.
- `internal/transports`: backend interface plus `dds`, `zenoh`, and `mqtt` implementations. No core logic. New buses arrive as pull requests against the interface, with interop tests, never as plugins.
- `internal/core`: entities, RxO matcher, router, service calls, action workflows, config store, clock domain, graph index, supervision groups.
- `internal/time`: clock sources, fake clocks, jump handling, rate policies.
- `internal/graph`: snapshots, compatibility reporting, tool queries.
- `sdk` (separate module): Node, Topics with generic bytes, Services with deadlines, Workflows for actions, Config with schema and watch, Time, Supervision groups. No transport imports.
- `cmd/weld`: conformance CLI covering the compatibility list.

## Proof Plan

Each feature ships with three gates, all green before merge. First comes unit tables with fake clocks, where time jumps and partitions are deterministic. Then come interop runs against `rclcpp` nodes on both DDS vendors plus Zenoh peers with `ros2cli` as the harness, for example echoing `/chatter` as strings at 10 Hz across implementations. Finally come chaos runs under race detection with loss, large message matrices, late joiners, discovery flaps, teardown mid run, and clock jumps.

Nav2 plus MoveIt 2 soak runs with RViz rendering close the loop per release. A typical soak drives a simulated TurtleBot through three goals while MoveIt plans five arm motions, with RViz rendering `/tf` and `/scan`. Green means no mismatch silence, no crash on teardown, and byte identical replays.

## Build Order in Named Phases

Work proceeds in phases that each end with a demonstrable interop check. The order respects dependencies: bytes before matching, matching before tools, tools before workflows, workflows before alternate transports, everything before full system soaks.

First, foundations. Types plus CDR with hash and fuzz tests prove that bytes encode and decode exactly.

Then, first contact. A DDS backend completes one unreliable topic round trip against real ROS 2, generic bytes first, for example a Weld publisher on `/chatter` seen by a ROS subscriber.

Then, correctness of matching. Core matching lands with loud mismatch events, so a reliable subscription against a best effort publisher raises the incompatible event with policy kind instead of staying silent.

Then, visibility. The graph index plus topic verbs let `ros2 topic list` and `ros2 node info` read from Weld state.

Then, calls. Services land with deadlines and one to one routing, for example `/add_two_ints` with `a` set to 2 and `b` set to 3 returning 5 within 100 ms.

Then, configuration. The config store lands with batched events and schemas, so startup applies files plus CLI plus code once.

Then, long tasks. Action workflows land with buffered feedback and cached results, demonstrated with a navigation goal that survives late attachment.

Then, second transport. The Zenoh backend lands behind the same core, reusing matching and supervision unchanged.

Then, supervision. Supervision groups land with ordering tests for serial and pooled modes.

Finally, fidelity and soak. Bags, launch args, TF exactness, and `/clock` land together, followed by Nav2 and MoveIt 2 soak with RViz checks.

## Open Questions

These are decisions recorded early so later work does not reopen them silently. Each names the default and the test that will settle it.

- Zenoh domain isolation by key prefix versus separate sessions. Users expect real isolation; default to separate sessions unless interop forces prefix mode.
- MQTT mapping for the three buses: topic mapping, retained messages for latched state, will messages for liveliness, QoS mapping onto the core model. Design with the DDS and Zenoh backends, never after.
- Static transform overwrite and clear semantics. Pin replace on rebroadcast and sticky across clear with tests. For example repeated `/tf_static` from `map` to `odom` with translation `x` equal to 1.0 must replace, not duplicate.
- `/tf` reliability default under lossy links. Match reliable volatile depth 100, revisit with loss matrix results.
