# Supervision

Plain words first: ordered work with declared concurrency. The term is supervision group, serial for one at a time or pooled for bounded parallelism with fairness.

## One node

There is one node type. Lifecycle is a supervision policy attached to entities, not a second node class. The supervisor owns order: forward for configure plus activate, reverse for cleanup plus deactivate plus shutdown (`docs/design.md:96`, `docs/invariants.md:212`). Late created entities auto activate when the node is active. A mobile base brings up `motor_driver` before `odometry_filter` before `planner_server` in the supervisor, tested once, not in shell scripts (`docs/design.md:96`).

Bond liveness uses a 0.25 s heartbeat with manager bond timeout 4.0 s and service timeout 5.0 s plus respawn after bond loss (`docs/invariants.md:214`).

## Serial groups

Serial means ordered delivery one at a time with no starvation ambiguity. Ready entities of equal priority run in insertion order. Destroying a group while spinning skips pending callbacks and joins cleanly with no segfault or deadlock (`docs/invariants.md:188`). A controller on `/odom` at 50 Hz uses a serial group with depth 10 and requires order (`docs/invariants.md:190`).

## Pooled groups

Pooled means bounded parallelism with fairness and a declared worker bound. No timer plus thread per goal, no unbounded fan out, no permanently dropped exclusive declarations. A soak with parallel goals shows worker count at or below the bound (`docs/invariants.md:188`). Fairness means contending subscriptions each make progress, so group isolation starvation cannot occur (`docs/invariants.md:189`). A logger on `/odom` at 50 Hz uses a pooled group with depth 100 and tolerates drops with a counter (`docs/invariants.md:190`).

Detached groups with dedicated threads serve where ROS uses detached executors, including MoveIt private nodes with dedicated TF listener threads (`docs/invariants.md:191`).

## Overflow

Each channel declares overflow at creation. The bounded default is drop oldest with a lost counter, never silent. Keep all is bounded with an explicit drop or block policy plus a lost counter (`docs/invariants.md:235`). Core applies backpressure between Weld endpoints instead of blind drops or unbounded growth (`docs/invariants.md:237`).

## Proof

Ordering tests pin serial order and pooled bounds plus fairness (`docs/design.md:220`). Teardown interleaves plus SIGINT mid run prove clean joins (`docs/invariants.md:316`). Nav2 bringup and MoveIt private nodes soak the mapping per release (`docs/invariants.md:320`).
