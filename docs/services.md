# Services

Plain words first: one request gets one reply. The term is service, a plain call with a context deadline.

## Contract

Each call routes exact one to one with its own timeout. A gone server or a gone client errors that one call and never stalls the node. Every blocking call takes a `context.Context` and teardown is idempotent, so a close during `/global_localize` returns cancellation then releases resources (`docs/design.md:51`).

Core owns the call state. Users never hand assemble request and reply topics. Take plus dispatch is one atomic step: take with nothing ready returns an empty signal with a metric, never a crash (`docs/invariants.md:164`).

## Wire

The wire stays standard so `ros2cli` and Nav2 see a normal peer (`docs/design.md:116`):

- Request topic `rq/<svc>Request`, reply topic `rr/<svc>Reply`, bare names when avoid-namespace-conventions is set (`docs/invariants.md:115`)
- Packet shape is 4 byte XCDR1 header plus 8 byte GUID plus 8 byte sequence plus user fields, with client GUID plus atomic increment sequence on send (`docs/invariants.md:117`)
- Split response handling rebuilds the take request id from header GUID plus publication handle and filters client side on its own publisher handle (`docs/invariants.md:118`)
- One input QoS fans out to two wire entities with dual actual getters: request publisher plus response subscription on clients, mirrored on servers (`docs/invariants.md:194`)

Service discovery is event driven with finite waits and clear errors. It never assumes symmetric request plus response discovery (`docs/invariants.md:166`). Send response with no ready reader maps to a timeout error with a service name metric (`docs/invariants.md:199`).

## Proof

Three gates before merge (`docs/design.md:196`, `docs/invariants.md:318`):

1. Unit tables with fake clocks for timeouts, gone peers, and teardown mid call.
2. Interop against `rclcpp` on CycloneDDS: `/add_two_ints` with `a=2 b=3` returns `5` within 100 ms (`docs/design.md:211`).
3. Chaos under race detection: loss, late joiners, discovery flaps, clock jumps.

Mismatch and absence are loud in logs, metrics, and CLI with topic, endpoint pair, and policy kind. Silence is a defect (`docs/invariants.md:305`).

## Live gate

Service checks live in `testbed/live/services/` and assert only request plus reply pairing. They never assert goal plus feedback plus result topics; that is the actions package. Every run uses one per-run id for payload isolation (`testbed/live/helpers/helpers.go:130`) and one per-run topic derived from it (`testbed/live/helpers/helpers.go:143`) on the shared live domain (`testbed/live/helpers/helpers.go:40`).

## Current state

v1 is topics-only best effort. Type hash, QoS offer, GID, and sequence are reserved as the documented v2 extension so services do not force a breaking change (`docs/transports.md:17`).
