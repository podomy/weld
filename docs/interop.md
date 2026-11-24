# Interop field notes

What the wire taught us against CycloneDDS on Humble, Jazzy, and Rolling. Plain words first, then the term. Each note gives symptom, fix, and rule.

## 1. String padding is minimal, not fixed

Symptom: our encoder padded strings to 4 bytes; Cyclone pads only for the next member's alignment, so `Image` with `mono8` shifted 4 bytes and `step` read garbage.
Fix: `internal/types/cdr.go` adds no pad after string or wstring NUL; the next member aligns itself; whole-sample trailing pad stays.
Rule: test any new codec path with a small member directly after a 5-char string.

## 2. DATA_FRAG body offset is 28, not 16

Symptom: large samples reassembled shifted; we wrote 16 (the DATA value) while DATA_FRAG needs 28 (reader 4 plus writer 4 plus seq 8 plus frag fields 12).
Fix: `encodeFragBody` in `internal/transports/dds/frag.go` writes 28; loopback plus Cyclone large payloads pass both ways.
Rule: DATA (20/16) and DATA_FRAG (32/28) constants stay separate with round-trip pins; never unify them.

## 3. One datagram can carry several SEDP announcements

Symptom: Humble batches 3 to 4 SEDP DATA per datagram; we installed the first and dropped the topic writer at slot 3, so every take failed as unknown-writer.
Fix: parse and install every DATA entry with per-entry error isolation (`WELD_SEDP_TRACE=1`, `internal/transports/dds/sedp_trace.go`).
Rule: receive path must handle any submessage count; never assume one per datagram.

## 4. Hashless peers are first-class

Symptom: Humble sends no `RIHS01_` hash; we required hash equality and ignored Humble entirely.
Fix: empty hash on either side is a wildcard; two set hashes must be equal; our announcements always carry hashes.
Rule: every new matching predicate must state its empty-versus-set behavior with Humble as the hashless peer.

## 5. Advertise what the facade promises

Symptom: we promised reliable transient-local depth 100 in `internal/core/facade.go` but announced volatile depth 10 on the wire, causing incompatible-QoS and lost history replay.
Fix: SEDP now announces transient-local depth 100 on both reader and writer paths; unit test pins durability kind plus depth.
Rule: change the wire offer and the facade together in the same commit.

## 6. NACKs must reach the right engine

Symptom: SEDP ACKs went to the first known port (data port) instead of the metatraffic engine; plus missing Final flag caused a heartbeat storm, base 0 was rejected, and writer `0x000002C2` mapped to the wrong reader.
Fix: fan out to all known peer ports, set Final on caught-up pure ACKs, accept base 0 as no-op, map `0x000002C2` to `0x000002C7` (`internal/transports/dds/reliability.go`, `routeBuiltinDiscovery` GAP branch).
Rule: re-run the late-joiner probe (converges in about 20 ms) for any ACK path change, not just unit tests.

## 7. Wait for discovery before taking

Symptom: taking before discovery finished turned slow joins into full-timeout failures, since the first SEDP burst often needs NACK repair (about 1 s).
Fix: `waitForEchoDiscovery` in `cmd/weld` and `WaitForTopicDiscovery` in `testbed/live/helpers` poll `HasRemoteEndpoint` bounded (about 5 s, context-aware) before taking.
Rule: never take blindly in a test or CLI verb; keep the wait bounded and visible.

## 8. Multicast receive is best-effort, unicast remains the contract

Symptom: proactive SEDP bursts go to multicast, which unicast-only receivers miss until NACK repair.
Fix: `joinMulticastBestEffort` joins with address reuse and falls back silently to unicast-only when taken; unknown-writer DATA installs a provisional endpoint (30 s TTL, 1024-entry cap, SEDP always wins) plus prompt NACK on initial gaps (3 attempts max).
Rule: unicast is the contract; multicast is additive; opportunistic learning stays provisional until SEDP confirms.

## 9. Test discipline: fail loudly, never silently green

Symptom: blanket skips hid real gaps; a skipped test proves nothing.
Fix: only `SkipIfNoPeer` and `SkipIfNoTypeInterface` are blanket skips (`testbed/live/helpers` skip budget); known gaps use allowlist entries with distro, reason, expiry; takes report drops versus lost separately; `ByteMultiArray` uses per-element `!!binary` YAML since integer-list form crashes `ros2 topic pub` (`testbed/live/ros/gate_type_std_rest_test.go:300`).
Rule: missing payload with healthy echo is FAIL, never skip; expired allowlist entries fail closed.

## 10. Domains and gates cheat sheet

Moved to `docs/testbed.md`. Canonical domains: 42 ROS peer, 47 serial gate, 48 SPDP, 43/44 Humble/Rolling, 55 large payload, 60-69 ephemeral, 70-101 pooled. Debug with `WELD_SEDP_TRACE=1` and `WELD_TAKE_DEBUG=1` (`internal/transports/dds/datapath.go:1339`).

## 11. Service wire notes

Verified against CycloneDDS on Jazzy by live execution. Full service hash values are in `docs/wire.md`.

- Header is 20 bytes: 4-byte XCDR plus 8-byte opaque request GUID plus 8-byte LE seq (`cdds_request_header_t`); Trigger request is header only, no dummy byte (`internal/types/envelope.go`, `internal/transports/dds/service.go`, `testbed/live/ros/gate_service_call_test.go`).
- Service SEDP endpoints require type hashes; empty-hash wildcard does not apply as for Humble topics; pins in `sedpTypeHashForIDL` (`internal/transports/dds/backend.go`).
- Service endpoints announce volatile durability kind 0 via `DeclareServiceEndpoint` and `sedpEncodeDurabilityFor`; topic endpoints use transient-local (`internal/transports/dds/service.go`, `internal/transports/dds/sedp.go`).
- Dot-format `serviceid`/`clientid` USER_DATA pairing (16 bytes: 12-byte prefix plus 4-byte name hash) is required; `CheckServiceAvailable` matches want ID with empty-only fallback (`serviceIDForName`, `CheckServiceAvailable`, `ParseServiceUserdata`).
- SEDP DATA uses per-writer sequence streams keyed on prefix plus entity; `announceWithRoutes` skips sends with empty routes so no seq is consumed (`internal/transports/dds/sedp.go`, `internal/transports/dds/backend.go`).
- Trigger forward plus reverse plus deadline PASS (`testbed/live/ros/gate_service_call_test.go`); params Get plus atomic Set plus validation reject PASS single-backend.
- Large payloads all 4 PASS on domain 55, no skips; domain 46 abandoned for host-container traffic.
- Reverse availability quirk resolved via synchronous SEDP announce.

## 12. Extended wire coverage

- Keyed topics: introspection `IsKey` extract, truncated SHA-256 handles, loud mismatch at match; with-key kinds writer `0x02` reader `0x07` round-trip (`internal/transports/dds/keyed.go`, `internal/types/keydefs.go`; `testbed/live/ros/gate_keyed_live_test.go`).
- Durability: 28-byte `DURABILITY_SERVICE` PID `0x001e` golden bytes; late-joiner replay both directions at-least-once bounded by depth (`internal/transports/dds/durability.go`; `testbed/live/ros/gate_durability_live_test.go`).
- Partitions: ASCII validation, DDS wildcard match, missing common partition is quiet non-match; enforced at SEDP match, routes stay per-participant (`internal/transports/dds/partition.go`; `testbed/live/ros/gate_partition_live_test.go`).
- Per-profile writer QoS: `ForwardQoS` to exact wire values with loud refusal, `ReverseQoS` back; default announce byte-identical without `WithQoS` (`internal/transports/dds/writerqos.go`; `testbed/live/ros/gate_writerqos_live_test.go`).
- Content filters: SQL subset (comparisons, BETWEEN, IN, LIKE, AND/OR/NOT, nested paths) compiled once, evaluated per sample with miss versus error reasons (`internal/transports/dds/contentfilter.go`, `takehooks.go`; `testbed/live/ros/gate_contentfilter_live_test.go`).
- Loans: bounded pool with consume-on-return; can-loan true only for self-contained memcpy-safe in-process types, false with reason elsewhere (`internal/transports/dds/loan.go`, `sdk/topic.go`; `testbed/live/ros/gate_loan_live_test.go`).
- Deadline plus liveliness: alive/stale/expired trackers with hysteresis and clock injection; wired on `SubscribeWithQoS` only (`internal/core/liveliness.go`; `testbed/live/ros/gate_liveliness_live_test.go`).
- Security posture: security PIDs tolerated, peers classed compatible-unknown or requires-crypto, protected submessages dropped loudly; no auth/crypto/ACL claimed (`internal/transports/dds/security.go`; `testbed/live/ros/gate_security_live_test.go`).
