# QoS

Weld speaks QoS on the wire and does not run a QoS engine inside. The profile answers only the matching question. Behavior comes from core.

## Profiles

RMW profile: history (keep last, keep all, system default) plus depth, reliability, durability, deadline, lifespan, liveliness, lease duration, plus avoid-namespace-conventions flag.

Time base is seconds plus nanoseconds. Zero means unspecified. Infinite is max `int64` split as `9223372036` seconds plus `854775807` nanos, with named consts. Infinite-minus-one is the best-available sentinel for deadline and lease.

Presets: sensor data (keep 5, best effort, volatile), parameters (keep 1000), default (keep 10, reliable, volatile), services default, parameter events, rosout (keep 1000, transient local, 10 s lifespan), system default (all markers), best available (keep 10, markers, infinite-minus-one durations), unknown.

Concrete presets carry `automatic` liveliness so they validate without prior resolve. Only `SystemDefaultQoS` keeps system-default markers.

Code: `internal/core/qos_types.go` for profiles and presets, `internal/core/matcher.go` for resolve, validate, and check.

## Matching

Offered versus requested (RxO):

- Reliable publisher to best-effort subscription flows; reverse is an error
- Transient-local publisher to volatile subscription flows; reverse is an error
- Publisher deadline faster than requested flows; slower is an error
- Automatic versus manual-by-topic mismatch is an error
- History, depth, lifespan, and avoid-conventions are never checked

Exactly six DDS error combos yield incompatibility. Any system-default, unknown, best-available, or invalid enum marker yields a warning, never silent flow. Errors come before warnings joined by `; `, truncated to 2048 chars on all three texts (`Reason`, `ErrorText`, `WarningText`).

`Validate` rejects negative seconds, `Nanoseconds >= 1e9`, sentinel durations, and invalid enums. `CheckCompatibility` also rejects negative deadline and lease loudly. Requested unspecified or infinite means no constraint and flows; offered infinite stays slow.

## Resolve and best-available

`ResolveSystemDefaults` runs in core before entity creation: history to keep last (zero depth to `DefaultHistoryDepth = 10`, documented against Cyclone `1` and Zenoh `42`), reliability to reliable, durability to volatile, liveliness to automatic, unspecified deadline and lease to infinite, avoid flag never resolved.

Best-available resolves once at endpoint creation from currently visible peers, stores the profile plus a peer-set copy, exposes it via actual getters, and never updates on later discovery:

- Subscriber: reliable iff at least one peer exists and all are reliable else best effort; transient local iff at least one peer exists and all are transient local else volatile; manual iff at least one peer exists and all are manual else automatic; deadline and lease default iff all default else maximum
- Publisher: always reliable plus transient local; manual iff any subscriber is manual; deadline and lease default iff all default else minimum
- Services substitute the services-default profile per field, preserving explicit non-marker fields

Zero peers resolve to the least demanding request (best effort, volatile, automatic). Lifespan sentinel resolves to unspecified.

## Facade and loudness

Advertised facade: reliable plus transient local with bounded keep last (`FacadeHistoryDepth = 100`). It satisfies every standard ROS subscription. Depth is wire-only and never affects matching.

Mismatch is loud in logs, metrics, and CLI with topic, endpoint pair, and policy kind. Silence is a defect. Overflow follows an explicit drop-oldest policy with a lost counter; keep-all is bounded by `KeepAllRetentionBound = 1000` retention entries. Counts bound entries, transports bound bytes. `HistoryCache.Clear` preserves lost counts across jumps. Deadline tracking rebases on backward jumps and never unmatches; alerts carry topic, endpoint, deadline, and elapsed.

Code: `internal/core/facade.go` for advertised profile, `internal/core/endpoint.go` for resolution, `internal/core/retention.go` for history, `internal/core/deadline.go` for monitoring, `internal/core/mismatch.go` for loud events.
