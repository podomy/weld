# Proofs: DDS reliability, discovery, QoS, XTypes, security

TLA+ specs for `../reliability.go` (plus `../reliability_nack_test.go`
for intended semantics and `../rtps/packet.go` framing only as
needed for sequence-number shapes), for SPDP/SEDP discovery
(`../spdp.go`, `../sedp.go`), for RxO matching
(`internal/core/matcher.go`, `../writerqos.go`), for type consistency
(`internal/types/hash.go`, `internal/types/registry.go`), and for
security handshake plus rekey (`security/`). Run from this directory, e.g.:

```sh
java -XX:+UseParallelGC -cp /tmp/opencode/tla2tools.jar \
  tlc2.TLC -workers 8 -config reliability_2.cfg reliability.tla
```

Config notes: constants use explicit sets (this TLC build rejects
`..` ranges). `states/` and `*_TTrace*` are TLC working files and
are git-ignored, not proof artifacts.

## reliability.tla — writer/reader repair

Models one writer send window and one reader receive window over a
lossy duplicating network with bounded loss budget: send,
heartbeat, AckNack listing exactly the missing set, repair resend,
exactly-once delivery by sequence-number dedup.

- `reliability_1.cfg`: sequence numbers 1..2, loss budget 1.
- `reliability_2.cfg`: sequence numbers 1..3, loss budget 2.

Proven: type safety, no duplicate delivery, convergence (every
sent sequence delivered exactly once under bounded loss),
quiescence (no repair traffic once all acked, drained network
stable), gap exactness (each answered ACKNACK lists exactly the
missing set).

Known abstractions (code behaves wider, convergence unaffected):
the writer repairs the whole `base..end` window including already
seen out-of-order seqs (reader dedups); the 4096-entry seen cap
can advance past an unreceived gap; the GAP-for-evicted-history
path, prompt-NACK attempt cap, heartbeat replay suppression,
lossless control traffic, and DATA_FRAG/NACKFRAG are folded or
out of scope.

## qos_match.tla — DDS 1.4 RxO compatibility

Models offered vs requested QoS from `internal/core/matcher.go`
`CheckCompatibility` (reliability, durability, liveliness, deadline,
lease, malformed-duration fail-closed) plus `writerqos.go` reverse
mapping. `TryMatch` sets `matched = Compatible`; nothing matches
without the gate.

- `qos_match_1.cfg`: core soundness (reliability, durability,
  liveliness, fail-closed malformed).
- `qos_match_2.cfg`: full rate rules (deadline/lease default vs
  finite, fast vs slow) plus strong-offer and no-constraint flows.

Proven: type safety, no bad match, malformed never compatible,
each RxO direction sound, strong offers flow, unconstrained
requests flow.

Known gaps (code refuses or ignores, spec models the subset):
ownership, presentation, latency-budget, destination-order,
time-based-filter, resource-limits unwired; durability and
liveliness truncated to two kinds each; MaxBlockingTime dropped;
DurabilityService compat unwired; SEDP never consults the matcher
(audit gap); partition gates separately (see discovery).

## discovery.tla — SPDP/SEDP stale and tombstone safety

Models one participant prefix in `spdp.go` `RemoteTable`
(`handleAliveLocked`, `handleDisposeLocked`, tombstones,
`generation`) plus `sedp.go` `SEDPIndex` (`UpsertRemote`,
`RemoveRemote`, `sedpStaleCheck`, `sedpStoreSeq`, sweep/expiry).

- `discovery_1.cfg`: `MaxSeq = 2`.
- `discovery_2.cfg`: `MaxSeq = 3`.

Proven: type safety, seq soundness, tombstone blocks stale alive,
tomb covers last seq, stale-safe install, generation monotonic,
lease implies alive.

Known abstractions: single prefix (prefixes never interact);
leases as nondeterministic expiry (fake clocks in tests);
HEARTBEAT/GAP replay, InfoDst, secure-refusal, and hash parsing
orthogonal; sequential locked ingest (code holds the table lock).

## xtypes_consistency.tla — DDS-XTypes hash gate

Models `internal/types/hash.go` `ComputeTypeHash` plus
`registry.go` `LookupWithHash` (exact equal only) plus `sedp.go`
`sedpMatchTypeHash` (endpoints match only on equal hashes,
TypeObject opaque) plus `extensible.go` loan rule (FINAL exact
only). Extensibility never widens a hash mismatch.

- `xtypes_consistency_1.cfg`: two hashes, up to 2 members.
- `xtypes_consistency_2.cfg`: three hashes, up to 3 members.

Proven: no hash-mismatch match, FINAL exact shape, no silent
coercion, loan needs FINAL exact.

Known non-goals (same as code): TypeIdentifier/EquivalenceHash,
Minimal/Complete TypeObject parsing, TypeConsistencyEnforcement
`is_assignable`, TypeLookup, XCDR2 emit, optional members,
mutable/appendable evolution.

## crypto_rekey.tla — DDS-SECURITY revision ladder

Models `security/crypto/factory.go` `Revise`/`Activate`,
`transform.go` `Encode`/`Decode` missing-key drop, `keystore.go`
`RemoveAllRevisions` eviction. Encode stamps `lastEncoded =
activeRev`; revise stages without changing encode; activate moves
the window; evict clears keys and last-encoded.

- `crypto_rekey_1.cfg`: `MaxRev = 2`, `MaxEvil = 1`.
- `crypto_rekey_2.cfg`: `MaxRev = 3`, `MaxEvil = 2`.

Proven: no unkeyed encode, revise stages strictly above active,
old key used until activate, evict clears keys and encoding.

Known gaps (same as code): volatile Kx wrapping in clear assumed
established; receiver-specific keys stored but unused; no
automatic `EnableKeyRevision` rekey (operator-driven).
