# Reference

Short answers plus terms. Details in `docs/design.md`, `docs/wire.md`, `docs/conformance.md`.

## FAQ

- Why IDL constants? Labels only; only the field value travels. Pins in `internal/types/bounds.go`.
- What is the identity quaternion? No rotation: `x=0, y=0, z=0, w=1`; the one IDL default Weld applies.
- Why one binary for Humble, Jazzy, Rolling? No C ABI; negotiate per peer by name plus type hash. Live gates, not CI.
- Why only CycloneDDS transport? One transport keeps matching and routing identical with no vendor drift.
- Why loud mismatch? Silent pubs/subs that never connect were the top interop cost; Weld surfaces topic, endpoints, and policy in logs, metrics, CLI.
- Why drop oldest with lost counter? Bounded topics keep the freshest sample for control loops and make loss visible.
- Where to start reading code? Registry `internal/types/registry.go`; bytes `internal/types/cdr.go`, `envelope.go`; truth table `internal/core/matcher.go`; behavior `internal/core/endpoint.go`, `facade.go`; backend contract `internal/transports/transport.go`.

## Glossary

- Message: one sample on a topic, e.g. `Twist` on `/cmd_vel`.
- Topic: named stream of messages; `/chatter` maps to DDS `rt/chatter`.
- Service: one request gets one reply, e.g. `/add_two_ints`.
- Action: long task with goal plus feedback plus result plus status.
- IDL: schema file declaring fields, constants, and defaults.
- CDR / XCDR1: byte layout for fields; Weld uses XCDR1 only.
- RTPS: DDS discovery and data protocol on the wire.
- SPDP / SEDP: discovery announcements for participants / endpoints.
- GID: 16 byte participant GUID identifying one endpoint.
- QoS: offer versus request matching before first byte (reliability, durability, deadline, liveliness, lease).
- Facade: advertises reliable plus transient-local; best-available freezes at endpoint creation.
- Type hash: `RIHS01_` plus 64 hex chars; name plus hash keys the registry.
- Domain id: integer isolating one fleet on shared multicast, set via `ROS_DOMAIN_ID`.
- Liveliness: endpoint aliveness plus lease; silence past lease means gone.
- Backpressure: slow the producer on `Full` plus eviction signals instead of unbounded growth.
