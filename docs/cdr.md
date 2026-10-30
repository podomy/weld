# CDR

Weld uses XCDR1 only on DDS paths. XCDR2 is never emitted. FastDDS forces XCDR1 on every reader and writer. Cyclone advertises XCDR1 allowed.

## Envelope

Every sample starts with a 4 byte header:

- byte 0 must be `0x00`
- byte 1 is endian flag: `0x01` little endian, `0x00` big endian
- bytes 2 and 3 are reserved and ignored

Payload is rebased 4 bytes forward. Serialized output pads to 4 bytes and zeroes the tail. Header plus length are validated before parsing. Sizes include the header.

Services add 8 byte GUID plus 8 byte sequence after the header in negotiated endianness. The 8 byte service GUID is opaque bytes and is never swapped. It is distinct from the 16 byte DDS participant GUID. The empty service message encodes as one dummy byte.

Code: `internal/types/envelope.go`, `internal/types/cdr.go`.

## Encoding model

One message encodes as one CDR byte stream. The encoder walks struct fields in IDL order and recurses:

- Nested message: encoded inline in place
- Sequence field: length prefix plus elements, embedded in the same stream
- String field: length plus bytes plus NUL with no trailing pad, embedded in the same stream

A message can therefore contain sequences while itself becoming a CDR stream. There is no separate CDR message per inner array. The outer stream contains the inner length prefixes and elements.

## Encoding rules

Plain XCDR1 per introspection:

- Aligned primitives, `bool` as one byte, `wchar` as 2 bytes
- Strings: 4 byte length plus bytes plus NUL with no trailing pad; the next member's `Align` absorbs the gap and `Bytes` pads a terminal string to 4. Example: `sensor_msgs/Image` with encoding `mono8` places `is_bigendian` immediately after the NUL, and the following `step` aligns from there.
- Bounded strings truncate on rune boundary and report truncation; negative bounds error without mutation
- Fixed arrays inline with 8 byte alignment and no length prefix; `float64[36]` covariance is the reference case
- Unbounded sequences carry a `uint32` length prefix; bounded sequences have checked helpers
- Caps: strings and sequences bounded at 32 MiB elements or bytes with documented origin; wstring units at 16 MiB

`Encoder.Bytes()` is a pure finalize: it does not mutate the encoder, so length accounting is stable across calls. `Decoder.drops` is private with a `Dropped()` getter; caller-owned `Metrics` is nil safe and kept separate.

## Decoding rules

Reject on:

- nonzero first byte or second byte above `1`
- length prefix beyond caps
- missing NUL or truncated content
- service header at wrong offset

Drops never panic. Truncated, empty, and extra-field inputs increment drops with a metric. `UnmarshalMessage` returns an unvalidated prefix view; strict validation requires `Decoder` plus `Finish`, which detects trailing garbage that the prefix view accepts. Interior NUL, invalid UTF-8, and unpaired surrogates (to `U+FFFD`) are preserved by tolerant receive and documented as such.

## Tests that pin it

- LE and BE primitive round trips with golden bytes
- String lazy pad, Image-shaped string plus `uint8` golden, empty string, bounded ASCII and astral (`U+1F600`) edges
- Fixed array 8 byte alignment, sequence prefixes
- Service GUID plus sequence LE and BE goldens, empty service `0x00`
- `Align(8)` after the 20 byte service prefix (`20` to `24`)
- Truncated fuzz, extra-field drops, invalid alignment without drop pollution, `Bytes()` purity
