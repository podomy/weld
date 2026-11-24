# Conformance

End state after locator, port, and pad fixes. Cyclone SPDP probe and ROS String round trip stay separate.

## Scope

v1 gates cover CycloneDDS only. Design pins bit level compatibility
with CycloneDDS peers in v1 (`docs/design.md`, `docs/overview.md`)
and product scope stays CycloneDDS-only (`docs/design.md`, `docs/wire.md`).
Cyclone checks assert wire fields only; ROS checks assert String payloads only (`testbed/live/ros/doc.go:4-14`).

## Wire identity

- Domain ports: `B = 7400 + 250 * domain`, multicast `B/B+1`, unicast `B+10/B+11` for participant 0 (`internal/transports/dds/rtps/udp.go:25,95-114`). Domain 48 is `19400/19401` and `19410/19411`.
- LAN IP advertised via `rtps.AdvertiseIPv4` (`internal/transports/dds/rtps/udp.go:1309`): first up multicast-capable non-loopback IPv4, loopback fallback. SPDP locators carry it (`internal/transports/dds/spdp.go:568`).
- Unicast bind is wildcard `0.0.0.0` (`internal/transports/dds/rtps/udp.go:44-46,550`); `DialUnicast` takes first free user slot from participant 0 (`internal/transports/dds/rtps/udp.go:708`). Live port is patched into SPDP locators before send (`internal/transports/dds/backend.go:1415`, `internal/transports/dds/spdp.go:1131`).

## SPDP

- Alive packet is 264 bytes (`internal/transports/dds/spdp_test.go:562`).
- `UserData` is pad-in-length: length includes zero pad to 4 bytes, matching CycloneDDS (`internal/transports/dds/spdp.go:597-600`, `testbed/unit/spdp/conformance_test.go:385`).
- Builtin endpoint mask `0x0000003F` (`internal/transports/dds/spdp.go:400-404`).
- Multi-port routes: receivers collect unicast locator ports in wire order, multicast never counts (`internal/transports/dds/spdp.go:1061`); bootstrap probes meta plus user unicast for 16 participants (`internal/transports/dds/backend.go:1218,1315,1962`).
- Sequence gating: repeat at stored sequence returns quietly; strictly older fails with `ErrStaleSPDP`; newer installs or removes (`internal/transports/dds/spdp.go:1395-1465`, `internal/transports/dds/spdp_test.go:1236`).

## SEDP

- CDR strings and octet sequences are pad-in-length (`internal/transports/dds/sedp.go:3435,3500`; `testbed/unit/sedp/conformance_test.go:408,445`).
- Flags always `LE | InlineQoS | Data` (`0x07`); endpoint DATA carries PL_CDR copy of params (`internal/transports/dds/sedp.go:2593`; `testbed/unit/sedp/conformance_test.go:497`).
- Per-writer sequence streams keyed on participant prefix plus writer entity, high word then low word (`testbed/unit/sedp/conformance_test.go:1176`).
- Every DATA carries HEARTBEATs: publications from `0x000002c2` plus Cyclone variant `0x000003c2`, subscriptions from `0x000004c2`; 28-byte body; 1 s resend (`internal/transports/dds/sedp.go:150-157,177-182,1350`; `testbed/unit/sedp/conformance_test.go:620`).
- Inline QoS: reliability kind 2 infinite blocking, KEEP_LAST depth 100, protocol 2.1, vendor `01 10`; writers offer transient-local, readers request volatile (`internal/transports/dds/sedp.go:189-203,2434`; `testbed/unit/sedp/conformance_test.go:664`, `internal/transports/dds/sedp_test.go:2710`).
- Builtin writer set `0x000002c2` plus `0x000003c2` plus `0x000004c2`; SPDP announces bound unicast ports; user multicast joined for fanout (`internal/transports/dds/sedp.go:177-182`; `testbed/unit/sedp/conformance_test.go:815`).

## Type hashes

All 8 IDLs pinned in `testbed/unit/sedp/conformance_matrix_hash_test.go:88`; dispatcher at `internal/transports/dds/backend.go:3338`. Full values in `docs/wire.md`.

- `std_msgs__msg__String`: `RIHS01_df668c74...f6b1a18` (`internal/transports/dds/sedp.go:228-229`)
- `std_msgs__msg__Int32`: `RIHS01_b6578ded...1cef0deb`
- `std_msgs__msg__Float64`: `RIHS01_705ba9c3...790182a`
- `std_msgs__msg__Bool`: `RIHS01_feb91e99...74528f6c9`
- `geometry_msgs__msg__Vector3`: `RIHS01_cc12fe83...7dbb0765d`
- `geometry_msgs__msg__PointStamped`: `RIHS01_4c0296af...ac1045897`
- `sensor_msgs__msg__Temperature`: `RIHS01_72514a14...2d5bad4b8`
- `sensor_msgs__msg__FluidPressure`: `RIHS01_22dfb2b1...3595bc32`

## Type coverage

Corpus has 124 rows in topological order (`internal/types/corpus.go:109`; manifest `internal/types/corpus_test.go:450`; shape `internal/types/corpus_test.go:13`). Counts: live 115, pure-Go-only 2 (`std_srvs/Empty`, `SetBool`), service 7 (`KindService` rq/rr, no rt gate), missing 0.
Live means forward plus reverse pub and echo on pooled domains 70-101 (`testbed/live/ros/roundtrip_test.go:107,160`; large payloads domain 55 `testbed/live/ros/gate_large_payload_test.go`). Every live gate calls `SkipIfNoPeer` first (`testbed/live/helpers/helpers.go:444`, `testbed/live/ros/gate_type_matrix_test.go:179`). `std_srvs/Trigger` is live forward plus reverse plus deadline (`testbed/live/ros/gate_service_call_test.go:442,481,545`). Rest2 alias gates reuse existing rows (`gate_type_*_rest2_test.go`). Per-type row detail lives in corpus and gate files, not here.

## DATA

- Generic CDR `decodePlainCDR` maps topic, demangles type, validates shape, skips seq tracking (`internal/transports/dds/datapath.go:1194,1143,976`; `testbed/unit/datapath/conformance_plain_cdr_matrix_test.go:317`).
- String fallback `decodeCycloneChatter` requires one valid XCDR1 String, no trailing bytes (`internal/transports/dds/datapath.go:921`; `testbed/unit/datapath/conformance_test.go:491`).
- `TakeFrom` validates with `ParseRTPSPayload`, tries generic then fallback, counts drop only when both fail (`internal/transports/dds/datapath.go:1473,1349,106,68`).
- Frag bounded by `fragMaxCount`, reassembles one payload (`internal/transports/dds/frag.go:37,145,355`).
- GAP `ApplyGap` skips one seq, ignores 0 and duplicates, never routes to `dataCh` (`internal/transports/dds/reliability.go:625,590,2422`).
- ACKNACK validates length, caps bits at 1024; `repairFromAck` resends window plus GAP (`internal/transports/dds/reliability.go:479,2022,1207,2374,2389`).
- Receipt assert: forward and reverse gates each use one per-run live ID, so a pass proves one DATA sample arrived.

## Sustained scope

Proven: 4 large-payload gates PASS on domain 55 (`gate_large_payload_test.go`; 320x320 mono8 Image reassembles). Reassembly bounded by 32 MiB cap plus frag count bound (`datapath.go:106`, `frag.go:37,149-150`). Ordering best effort (`datapath.go:1580`; `reliability.go:625`; `datapath_test.go:243,884`).
Out of scope: loss storms, N-participant stress, multi-MB streams above cap. No gate claims them.

## Gates

Run unit: `go test ./testbed/unit/... -count=1`. Run live: `go test ./testbed/live/... -count=1` against running `ros-jazzy` peer (`testbed/live/helpers/helpers.go:444`); skips without peer. One run per domain/topic. Pooled domains 70-101 (`testbed/live/helpers/domainpool.go:196,464`); fixed: 42 ROS peer, 48 SPDP, 47 serial gate, 43/44 Humble/Rolling, 55 large payload, 60-69 ephemeral. Detail: `docs/testbed.md`.
- Gate 0: `go build`, `go vet`, `gofumpt`, `golangci-lint`, `go test` (`testbed/live/gates/gate0_test.go:41,150`).
- Gate 1: direct UDP SPDP acceptance on domain 48, GUID prefix inequality, zero deserialization failures (`testbed/live/cyclone/gate1_spdp_test.go:370,411`; `testbed/live/gates/gate1_test.go:31`).
- Gate 2: String round trip forward plus reverse (`testbed/live/ros/roundtrip_test.go:107,160`; `testbed/live/gates/gate2_test.go:33,107`); SPDP probe and String trip stay separate.
- Matrix (all PASS live): full type matrix; distro Humble/Jazzy/Rolling fwd plus rev; gates entrypoint; CLI; Cyclone user-data receipt; Trigger fwd plus rev plus deadline; params declare/set/reject; TF static/dynamic/unknown; actions accept/cancel/timeout.
- Extended wire (all PASS): keyed; durability backlog both dirs; partitions default/rev/isolation; per-profile writer QoS; content-filter mixed/always-false; loan honesty/zero-copy; liveliness stale/deadline; secured-payload loud drop.
- Verified green 2026-11-01: 17 runs PASS, 0 FAIL; one environmental skip only.
