# Public Go Surface

External Go programs must import only these paths. No `internal` import is required.

| Path | Purpose |
|------|---------|
| `github.com/podomy/weld/bus` | Embedding surface: backend contract plus DDS and MQTT constructors. |
| `github.com/podomy/weld/msg` | Type registry and XCDR envelope helpers. |
| `github.com/podomy/weld/node` | Node lifecycle plus endpoints, QoS, config, and observation. |
| `github.com/podomy/weld/wtime` | Clock and fake clock for deterministic tests. |
| `github.com/podomy/weld/graph` | Read snapshot view for tools: index, snapshot, and hidden filtering. |
| `github.com/podomy/weld/bag` | Bag exchange for info and replay. |
| `github.com/podomy/weld/launch` | Launch argument surface: ros args, remaps, and params. |

## Type identity notes

- `graph.CoreGraph` is identical to `node.Graph`. Obtain a value from `node.NewGraph` and pass it to `graph.IndexFromCore` or `graph.SnapshotFromCore`.
- `graph.CoreGID`, `graph.CoreEntityInfo`, `graph.CoreServiceInfo`, `graph.CoreParticipantInfo`, and `graph.CoreQoSProfile` are identical to the matching `node` and core types. They exist so callers can name conversion inputs without an internal import.
- `bag.Registry` is identical to `msg.Registry`. Obtain a value from `msg.NewRegistry` and pass it to `Bag.Append` or `Bag.Verify`.
- `launch.ParamValue` is shared with core. File, CLI, and code maps use this type directly.
