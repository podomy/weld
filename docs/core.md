# Core

Services, actions, config, time, and supervision. Core owns state; users never hand roll the machine.

## Services

One request gets one reply, each call with its own timeout and context deadline. A gone server or gone client errors that one call and never stalls the node (`docs/design.md`, `docs/invariants.md`).
Wire stays standard: request topic `rq/<svc>Request`, reply topic `rr/<svc>Reply`, packet is 4 byte XCDR1 header plus 8 byte GUID plus 8 byte sequence plus fields, with client GUID plus atomic sequence on send (`docs/design.md`, `docs/invariants.md`).
One input QoS fans out to two wire entities with dual actual getters, mirrored on clients and servers (`docs/invariants.md`).
Discovery is event driven with finite waits and clear errors; send with no ready reader maps to a timeout error with a service metric (`docs/invariants.md`, `docs/invariants.md`).
Gates: fake-clock unit tables, interop `/add_two_ints` `a=2 b=3` returns `5` within 100 ms (`docs/design.md`), chaos under race detection.
Live checks are `testbed/live/ros/gate_service_call_test.go:442` plus params gates, one per-run id and topic.

## Actions

A long task with feedback and a final result, a durable workflow owned by core (`docs/design.md`, `docs/invariants.md`).
Core owns admission, cancel propagation, feedback buffering, result caching, and terminal states. Feedback at 5 Hz buffers even if the client attaches late; the result survives after the server handle closes (`docs/design.md`).
Managed servers gate on active state with 500 ms admission and deactivation timeouts; one active plus one pending preempt slot with a detached work thread gives deterministic replacement (`docs/invariants.md`).
Result timeout starts at completion, not acceptance; default per-goal timeout is 60 s from acceptance, unset takes the default, never infinite wait (`docs/invariants.md`).
Clock mismatch surfaces as an explicit error with goal stamp and clock domain; limits, joints, QoS, and clocks validate before send (`docs/invariants.md`, `docs/design.md`).
Gates: fake-clock unit tables (late attach, cancel races, jumps), interop navigation goal on CycloneDDS (`docs/design.md`), chaos under race detection.

## Config

Robot settings with history: one versioned store with schema, types, validation, atomic updates, watches, and history.
Startup applies files plus CLI plus code in fixed precedence with one batched event, not N events (`docs/design.md`, `docs/invariants.md`).
Split validation from apply: `on_set` validation runs first, post-set apply runs only when validation accepts (`docs/design.md`).
Remap is a pure first-class rewrite table at creation: first match across global plus local arrays wins, with node scoped expansion (`docs/invariants.md`).
Watches are versioned with history, so late subscribers read what changed and when (`docs/invariants.md`).
Wire keeps standard parameter services served from the store: declare, get, set, describe, list, events, overrides (`docs/design.md`).
Gates: schema reject plus precedence plus watch plus remap tables, `ros2 param` interop on CycloneDDS, chaos under race detection.

## Time

One clock per task context, with explicit wall, steady, and sim sources plus a never-silent construction fallback.
Each context has exactly one domain; steady timers are the default and `use_sim_time` mismatch on action clients is a loud error (`docs/design.md`, `docs/invariants.md`).
Jump callbacks are allocation free with ordered deregistration; backward jumps clear or invalidate caches (`docs/invariants.md`).
Timer jump handling saves next minus now as credit before the jump, restores after, triggers the guard on forward pass, and resets to now plus period on backward pass (`docs/invariants.md`).
Overload policy is explicit with backlog counters; silent coalescing is not allowed (`docs/invariants.md`).
`/clock` publishing keeps sim second `1234` the same tick in Weld and `rclcpp`; lookup accounting uses the buffer clock (`docs/design.md`, `docs/invariants.md`).
Gates: deterministic fake clocks (bag loops, jumps, pauses, multi rate), interop on CycloneDDS, chaos under race detection.

## Supervision

Ordered work with declared concurrency: serial for one at a time, pooled for bounded parallelism with fairness.
One node type; lifecycle is a supervision policy, not a second class. Supervisor owns order: forward for configure plus activate, reverse for cleanup plus deactivate plus shutdown (`docs/design.md`, `docs/invariants.md`).
Serial means ordered delivery one at a time, equal priority in insertion order; destroy while spinning skips pending callbacks and joins cleanly (`docs/invariants.md`).
Pooled means bounded workers with fairness, no timer plus thread per goal and no unbounded fan out; contending subscriptions each make progress (`docs/invariants.md`).
Each channel declares overflow at creation; bounded default is drop oldest with a lost counter, never silent; keep-all carries an explicit drop or block policy (`docs/invariants.md`).
Gates: ordering plus bounds plus fairness tables (`docs/design.md`), teardown and SIGINT joins (`docs/invariants.md`), Nav2 and MoveIt soak per release.
