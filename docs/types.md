# Types

`internal/types/` models ROS IDL in Go without embedding ROS structs.

## IDL to Go mapping

Each IDL shape maps to one Go shape with identical size and layout intent:

- `float` to `float32`, `double` to `float64`
- `char` to signed `int8`, `wchar` to `uint16`
- `bool` to `bool`, `octet` and `uint8` to `uint8`
- `string` to a runtime string struct with NUL capacity rule
- arrays to inline Go arrays, sequences to data plus size plus capacity triples

Struct fields emit in IDL order. Order is never rearranged. Offsets increase in IDL order and are pinned by tests.

Code: `internal/types/bounds.go:12-30`, `internal/types/introspection.go`.

## Schema versus value

IDL declares the shape once. Runtime structs carry each message value.

- IDL `string` names the kind; `RosString` holds one text value (data plus size plus capacity)
- IDL `float64[]` names the kind; `Sequence[float64]` holds one array value
- IDL `SolidPrimitive` names the message; a `SolidPrimitive` struct holds one sample with a `uint8` type plus dimensions

Introspection and the registry describe shapes. `RosString`, sequences, and message structs carry values. Constants are neither: they are compile-time labels and never travel.

## Field types

FieldType IDs are distinct from introspection `ROS_TYPE_*` numbers:

- `0` not set
- `1` nested message
- `2` to `9` signed and unsigned integers 8 to 64
- `10` to `12` float, double, long double
- `13` to `14` char, wchar
- `15` bool, `16` byte
- `17` to `18` string, wstring
- `19` to `20` fixed string, fixed wstring
- `21` to `22` bounded string, bounded wstring
- `49` to `70` fixed arrays
- `97` to `118` bounded sequences
- `145` to `166` unbounded sequences

`ArrayOf`, `BoundedSequenceOf`, and `UnboundedSequenceOf` validate the base range `1` to `22` and derive `Offset+1` to `Offset+22`. Invalid IDs, scalar `Capacity != 0`, and `StringCapacity != 0` on non-strings are rejected.

Code: `internal/types/description.go`.

## Registry

`Registry` maps type name plus hash to a full description. It is a struct with a mutex. There is no process global.

Operations:

- `RegisterType` for one description
- `RegisterFull` for a root plus its referenced descriptions
- `TransitiveClosure` returns the root plus every nested dependency, sorted
- `LookupWithHash` distinguishes unknown type from hash mismatch with errors, so callers can log topic plus endpoint plus policy

`RegisterFull` normalizes (validate plus sort plus hash), checks closure completeness (every nested name present), and rejects conflicting redefinition. `RegisterSource` overwrite wins and is documented as idempotent for the dynamic placeholder case.

Code: `internal/types/registry.go`.

## Descriptions and sources

- Field record: name plus type plus default
- Individual type: name plus fields
- Full description: one description plus referenced descriptions
- Source record: type name plus encoding (`idl`, `msg`, `srv`, `action`, `dynamic`, `implicit`) plus raw contents

Type names cap at 255 bytes. Rejects cover duplicate fields, duplicate references, root in references, and unsorted references. Empty source contents are allowed only for `dynamic` and `implicit`.

## Constants

Constants never travel on the wire. They compile to typed Go consts only.

Suffix mapping:

- `L` means `int32`
- `UL` means `uint32`
- `F` means `float32`
- booleans stay `bool`

The values in `internal/types/fixtures_test.go` (`-123`, `456`, `1.5`, `true`, `false`) are arbitrary fixtures, not IDL defaults. They pin the mapping with distinct signed, unsigned, fractional, and bool cases. The test asserts the values and asserts no member named `l`, `ul`, `f`, `true`, `false`, or `BOX` appears in introspection tables.

The only real IDL default in the corpus is the identity quaternion. See `faq.md`.

## Header semantics

`Header` carries stamp plus `frame_id`. Key rules:

- `Odometry` header frame is pose parent, twist in child frame
- `LaserScan` stamp is first ray acquisition, angles about positive Z from positive X
- `Image` stamp is acquisition in optical frame (X right, Y down, Z forward)
- `Path` header is path frame with per-pose headers as fallback
- `NavSatFix` stamp is ROS time in antenna frame
- `Marker` lifetime zero means forever (per element)

Float rules: NaN means unmeasured on float fields only, IMU `-1` means disregard with all-zero meaning unknown, signed occupancy bytes decode via resolution plus origin, `uint8` enums are plain status codes with no NaN semantics.
