# Introspection and bounds

Introspection describes a message layout so the codec can read and write it without generated code per type. Bounds enforce IDL limits at serialize time.

## Tables

Identifier: `rosidl_typesupport_introspection_c`.

- Member record: name, type id, string upper bound, nested members or namespace plus name refs, key flag, array flags, size, offset, plus get, fetch, assign, and resize functions
- Container: namespace, name, member count, struct size, key presence, init and fini functions
- Service tables: request plus response plus event

Offsets use Go layout for reflection only, not C ABI. This is documented at the table builders with an overflow assert. Nested patching is lazy and mutex guarded with cycle detection; concurrent patch has a race test.

`DefaultValue` is always nil. Resize is fini plus init in shape: dropped tail is finalized, grown head is initialized, but nested contents use shallow preserve plus zero fill and this is documented.

Code: `internal/types/introspection.go`.

## Value kinds and loans

`ValueKindFor` maps introspection to wire value kinds. Fixed and variable string arrays map to a string sequence kind, not scalar string. `IsBoolVector` is `bool` plus sequence only; fixed `bool[3]` is not a bool vector.

`IsSelfContained`, `CanLoan`, and `IsMemcpySafe` return `(bool, error)`. Unresolvable nested references propagate an error instead of silently reporting not loanable. Cycles are explicit errors. v1 answers loan availability exactly as the vendor would for ROS peer queries, without implementing loan borrow or return APIs.

## Strings and sequences

`RosString` is data plus size plus capacity with NUL included: valid when `Capacity >= Size+1`. Empty valid triple is `(nil, 0, 1)`.

`Sequence[T]` is data plus size plus capacity with no terminator: valid when `Capacity >= Size`. Empty valid triple is `(nil, 0, 0)`. Both have `Sync` that preserves spare capacity.

Init modes: `all`, `skip`, `zero`, `defaults only`. `New*` delegates to `Init*` so the two cannot drift. `DefaultsOnly` fills defaulted fields and leaves plain data untouched. `is_rosidl_buffer` on `uint8[]` is honored; the flag on other shapes is rejected.

## Bounds

Bounds are transient validation inputs, not message fields and never on the wire. Each bound is built per send from the local introspection table plus the description resolved for that peer by name plus type hash, so a remote machine's type version sets the caps through discovery. This is required because Humble, Jazzy, and Rolling can ship different limits under one type name; baking one static cap into the struct would accept oversized data for strict peers or reject valid data for loose ones. Keeping the bound as an explicit parameter also keeps tables declarative and checks testable with no hidden global. One merge path per kind: `StringBound.Merge` over introspection bound, `__MAX_SIZE`, `__MAX_STRING_SIZE`, and description capacities; `SequenceBound.Merge` over array size, upper bound flag, and description capacity. Only tighter values win. `Validate*ForSerialize` merges once then checks once.

Fixed `ArraySize == 0` is rejected. Unbounded sequences must carry `Capacity == 0` in the bound record. `EffectiveMax` documents `ok == false` as exact length, not bounded.

Fixtures live in `internal/types/fixtures_test.go`: `SolidPrimitive` (single bounded `float64` max 3), `CovarianceHolder` (`CovarianceSize = 36`), `BufferHolder`, `DefaultedHolder`, `BoundedStringHolder`, `HeaderFixture`. A dedicated test asserts only `SolidPrimitive` mentions bounded in the corpus, per the single bounded path rule.

## Why fixtures exist

Think of learning to drive in an empty parking lot instead of downtown traffic.

Real ROS messages need the full stack: IDL files, generated code, a DDS peer, and three distro installs. That stack is slow, flaky, and hard to reason about when a test fails. A fixture is the parking lot: a tiny fake message with exactly one hard feature.

- `SolidPrimitive` is a box-or-sphere shape with a short list of sizes. It proves the bounded-list path: at most 3 numbers, no more.
- `CovarianceHolder` is a fixed box of 36 numbers. It proves fixed arrays travel inline with no length prefix.
- `BoundedStringHolder` is one short text field with max 10. It proves bounded text is rejected when too long.
- `HeaderFixture` is a stamp plus frame id. It proves nested messages encode in order.

Each fixture pairs a struct with its bound and its reflection table, so one test can check all three stay in sync: build it, check the limit, resize it, read it back. When the real messages arrive later, they reuse the same proven code path with no new logic.
