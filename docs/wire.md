# Wire

Wire format, types, QoS, naming, and transport border. Core never sees ROS specifics.

## Envelope

Weld emits XCDR1 only. XCDR2 is never emitted.

Every sample starts with a 4 byte header: byte 0 must be `0x00`, byte 1 is endian (`0x01` LE, `0x00` BE), bytes 2-3 are reserved and ignored. Payload is rebased 4 bytes forward, padded to 4 bytes with zero tail. Header plus length are validated before parsing.

Services add 8 byte GUID plus 8 byte sequence after the header in negotiated endianness, for a 20 byte prefix. The service GUID is opaque bytes, never swapped, and distinct from the 16 byte DDS GUID. The empty service message encodes as one dummy byte. `Align(8)` after the 20 byte prefix moves `20` to `24`.

Code: `internal/types/envelope.go`, `internal/types/cdr.go`.

Reject on nonzero first byte, second byte above `1`, length beyond 32 MiB caps, missing NUL, truncated content, or service header at wrong offset. Drops increment a metric and never panic. `Decoder` plus `Finish` detects trailing garbage that prefix views accept.

## Type hashes

Weld keys every type by name plus hash. Same name with different fields never falsely matches.

Format is `RIHS01_` plus 64 lowercase hex digits (SHA-256), 71 chars total. Version byte `1` plus 32 hash bytes on the wire; version `0` means unset. Parse accepts only version `1` with exact length; uppercase hex is rejected.

Computation: deep copy the description, delete every `default_value`, JSON dump with separators `(', ', ': ')` and unsorted keys as stored, ASCII enforced, then SHA-256 hex. `CanonicalJSON` sorts a cloned copy and validates order, so unsorted input still yields the deterministic digest.

Humble peers may omit hashes (hashless keys); Iron and newer include them. Weld treats hashless versus hashed keys as different rendezvous strings. Demangled name plus hash plus domain must match before first byte flows. Mismatch errors distinguish unknown type from hash mismatch from validation failure.

Code: `internal/types/hash.go`.

## IDL mapping

`internal/types/` models ROS IDL in Go without embedding ROS structs. Struct fields emit in IDL order; order is never rearranged.

| IDL | Go |
| --- | --- |
| `float` / `double` | `float32` / `float64` |
| `char` / `wchar` | `int8` / `uint16` |
| `bool` / `octet` | `bool` / `uint8` |
| `string` | `RosString` (data plus size plus capacity, NUL included) |
| fixed array | inline Go array, no length prefix |
| sequence | data plus size plus capacity triple, `uint32` length prefix |

Schema declares shape once; runtime structs carry values. Introspection plus registry describe shapes. `RosString`, `Sequence[T]`, and message structs carry values.

Constants never travel on the wire. They compile to typed Go consts only (`L` to `int32`, `UL` to `uint32`, `F` to `float32`, booleans stay `bool`).

Header semantics:

- `Odometry` header frame is pose parent, twist in child frame
- `LaserScan` stamp is first ray acquisition, angles about positive Z from positive X
- `Image` stamp is acquisition in optical frame (X right, Y down, Z forward)
- `Path` header is path frame with per-pose headers as fallback
- `NavSatFix` stamp is ROS time in antenna frame
- `Marker` lifetime zero means forever, per element

Code: `internal/types/introspection.go`, `internal/types/registry.go`, `internal/types/description.go`, `internal/types/bounds.go`.

## QoS

Weld speaks QoS on the wire and runs no QoS engine inside. The profile answers only the matching question.

RMW profile: history (keep last, keep all, system default) plus depth, reliability, durability, deadline, lifespan, liveliness, lease duration, plus avoid-namespace-conventions flag. Time base is seconds plus nanos; zero means unspecified. Infinite is max `int64` split as `9223372036` s plus `854775807` ns. Presets: sensor data (keep 5, best effort, volatile), parameters (keep 1000), default (keep 10, reliable, volatile), rosout (keep 1000, transient local, 10 s lifespan), plus services default and best available.

Offered versus requested (RxO):

- Reliable publisher to best-effort subscription flows; reverse is an error
- Transient-local publisher to volatile subscription flows; reverse is an error
- Publisher deadline faster than requested flows; slower is an error
- Automatic versus manual-by-topic mismatch is an error
- History, depth, lifespan, and avoid-conventions are never checked

`ResolveSystemDefaults` runs in core before entity creation (history to keep last depth 10, reliability to reliable, durability to volatile, liveliness to automatic). Best-available resolves once at endpoint creation from visible peers and never updates on later discovery. Zero peers resolve to least demanding (best effort, volatile, automatic).

Advertised facade: reliable plus transient local with bounded keep last (`FacadeHistoryDepth = 100`). It satisfies every standard ROS subscription; depth is wire-only and never affects matching. Mismatch is loud in logs, metrics, and CLI with topic, endpoint pair, and policy kind. Overflow is drop-oldest with a lost counter; keep-all is bounded by `KeepAllRetentionBound = 1000`.

Code: `internal/core/qos_types.go`, `internal/core/matcher.go`, `internal/core/facade.go`, `internal/core/endpoint.go`, `internal/core/retention.go`, `internal/core/deadline.go`.

## Naming

Topics: `rt/<name>`. Requests: `rq/<name>Request`. Replies: `rr/<name>Reply`. Bare names apply when avoid-namespace-conventions is set.

Types: `::` for double underscore, then `<ns>::dds_::<Name>_` with `Request_` and `Response_` suffixes for services. Note the split: topic suffix is `Reply` while type suffix is `Response`, per ROS naming. Reverse via `ParseTopicName`, which handles `rt`, `rq`, `rr`, and bare forms.

## Backend interface

`Backend` is publish bytes, take bytes, join discovery, report liveliness, plus idempotent `Close`:

- `PublishBytes` and `TakeBytes` move uniquely owned payloads; caller must not mutate after send, callee copies if retained
- `TakeBytes` blocks until a message or context done and returns `ctx.Err`
- `JoinDiscovery` must precede publish and take; early use reports `ErrNotJoined`
- `ReportLiveliness` carries per-participant generation; greater wins, same plus same aliveness is a no-op, else `ErrStaleGeneration`
- After first `Close`, other methods report `ErrBackendClosed`
- Backends must be safe for concurrent use

Code: `internal/transports/transport.go`, `internal/transports/dds/config.go`, `internal/transports/dds/backend.go`.
