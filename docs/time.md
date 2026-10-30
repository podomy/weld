# Time

Plain words first: one clock per task context. The term is clock domain, with explicit wall, steady, and sim sources plus a construction time fallback that is never silent.

## One domain

Each context has exactly one domain. Steady timers are the default. Per subsystem rate choices, such as Nav2 wall loops versus ROS behaviors, are declared policy, never leaked across contexts by default (`docs/design.md:128`, `docs/invariants.md:179`).

`use_sim_time` agreement on action clients is enforced with a loud error on mismatch (`docs/invariants.md:179`). When sim time publishes 1000 then jumps back to 900 during a bag loop, every timer in that context observes the same jump through the same callbacks (`docs/design.md:130`).

## Jumps

Jump callbacks are allocation free with ordered deregistration, and backward jumps clear or invalidate caches (`docs/invariants.md:180`). Timer jump handling saves next minus now as credit before the jump, restores it after, triggers the guard when a forward jump passes the next call, and resets to now plus period on any backward jump past the last call (`docs/invariants.md:221`).

Timer init reads now from the clock, sets last call to now and next call to now plus period, and uses atomics for callback, period, and canceled flag (`docs/invariants.md:223`). Reset sets next call to now plus period, clears canceled, and triggers the guard (`docs/invariants.md:225`).

## Overload

Overload policy is explicit and observable with backlog counters. Silent coalescing that hides backlog is not allowed (`docs/invariants.md:181`). QoS depth is honored at delivery or refused at creation loudly with topic, endpoint pair, and policy kind (`docs/invariants.md:238`).

## Wire

`/clock` publishing and sim time agreement mean sim second `1234` is the same tick in Weld and in `rclcpp` (`docs/design.md:168`). lookup timeout accounting uses the buffer clock, and caches clear on ROS time activation, deactivation, or backward jump (`docs/invariants.md:254`).

## Proof

Deterministic fake clocks drive chaos tests: bag loops, backward jumps, pauses, and multi rate sim (`docs/design.md:130`). Each feature passes unit tables with fake clocks, interop against `rclcpp` on CycloneDDS, and chaos under race detection (`docs/design.md:196`, `docs/invariants.md:318`).

OPEN ITEM: the timer overload default across coalesce, skip ahead, and drift is recorded before control soak passes (`docs/invariants.md:226`).
