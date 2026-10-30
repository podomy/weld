# Actions

Plain words first: a long task with feedback and a final result. The term is action, a durable workflow owned by core.

## Contract

Users never hand roll the machine. Core owns goal admission, cancel propagation, feedback buffering, result caching, and terminal states. Feedback at 5 Hz buffers even if the client attaches late. The result survives after the server handle closes, independent of handle lifetime (`docs/design.md:116`, `docs/invariants.md:204`).

Timeouts attach at the transition that starts them. Result timeout starts at completion, not at acceptance (`docs/invariants.md:206`).

## Admission and preempt

Managed servers gate on active state: reject goals when inactive, accept cancel when active, with a 500 ms admission and deactivation timeout (`docs/invariants.md:203`). Servers run one active plus one pending preempt slot with a detached work thread; back to back replacement preempts deterministically (`docs/invariants.md:204`).

Deactivation ends in-flight goals with a terminal result and rejects new work, bounded by a 500 ms deactivate timeout (`docs/invariants.md:167`). SIGINT teardown ends in terminal results, never use after free (`docs/invariants.md:205`).

## Clocks and validation

A receding horizon goal stamped with the wall clock against sim time must not freeze silently; clock mismatch surfaces as an explicit error with goal stamp and active clock domain (`docs/invariants.md:207`). Weld validates before sending: limits, joint names, touch links, QoS, and clocks are checked first, and success with dirty diagnostics counts as failure (`docs/design.md:118`).

## Wire

The wire stays standard: goal plus cancel plus feedback plus result plus status topics with preempt and terminal states. Weld speaks that exchange; users see only the workflow (`docs/design.md:116`). Hash is part of the key on Zenoh paths with no separate type negotiation (`docs/invariants.md:133`), so type plus hash must match before first byte.

## Proof

Three gates before merge (`docs/design.md:196`, `docs/invariants.md:318`):

1. Unit tables with fake clocks: late attach, cancel races, backward jumps, deactivation mid goal.
2. Interop against `rclcpp` on CycloneDDS with a navigation goal that survives late attachment (`docs/design.md:217`).
3. Chaos under race detection: loss, duplicate feedback, teardown mid result, multi rate sim.

OPEN ITEM: whether core requires an explicit per-goal timeout with a default is decided before Nav2 conformance (`docs/invariants.md:209`).

## Live gate

Action checks live in `testbed/live/actions/` and assert only goal plus feedback plus result plus status. They never assert service sequence pairing; that is the services package. Every run uses one per-run id (`testbed/live/helpers/helpers.go:130`) and one per-run topic (`testbed/live/helpers/helpers.go:143`) on the shared live domain (`testbed/live/helpers/helpers.go:40`).
