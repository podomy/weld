# MQTT Transport

MQTT backend beside DDS. Same `transports.Backend` contract, smaller wire, brokered delivery. Core never imports either backend.

Code: `internal/transports/transport.go`, `internal/transports/mqtt/`, `internal/transports/router.go`, `internal/transports/forward.go`.

## Scope

- Topics plus services in v1. Topics ride `rt`, services ride `rq` / `rr` with opaque service envelopes; request and reply legs are first class on the contract (`transports.Leg`). Actions reuse the same legs: send goal, cancel goal, and get result are services, feedback and status are data topics.
- One-way bridge today: DDS Take to MQTT Publish via `ForwardOnce` / `ForwardLoop`, legs and hashes preserved.
- Real MQTT v5 session over TCP via `mqtt.Dial` against mosquitto or any v5 broker; in-memory channel carries contract tests. TLS via `mqtts` scheme with host trust plus optional CA bundle and client pair.

## Topic map

Form: `prefix/domain/kind/stripped`

- `prefix`: normalized MQTT root, default `weld`. No leading or trailing slash, no `+`, `#`, whitespace, empty level, or `$` prefix (the broker reserves `$`).
- `domain`: decimal DDS domain id, 0 through 101. Resolved with `transports.ResolveDomainID` so `ROS_DOMAIN_ID` wins.
- `kind`: `rt` data, `rq` request, `rr` reply, `sdp` retained discovery.
- `stripped`: ROS name without leading slash. Root `/` maps to no trailing level.

Examples:

- `/chatter` on domain 0 becomes `weld/0/rt/chatter`
- `/add_two_ints` request becomes `weld/0/rq/add_two_ints`
- Endpoint announce becomes `weld/0/sdp/talker/rt/chatter`

Encode: `internal/transports/mqtt/topic.go:EncodeTopic`. Decode reverses it and rejects wildcards, whitespace, bad domains, unknown kinds, and prefix mismatches. Take also rejects wire domains outside the backend domain. Discovery topics use `EncodeDiscoveryTopic`.

## Envelope

MQTT body layout:

```text
0..3   type name length, big endian uint32
4..    type name bytes (IDL form, e.g. std_msgs__msg__String)
...    raw payload bytes (XCDR1 bytes as carried in OutgoingMessage.Payload)
```

Empty payload decodes to nil so nil and zero length stay equivalent per the contract. Truncated buffers error. No CDR parse happens here. Code: `internal/transports/mqtt/backend.go:encodeEnvelope`, `decodeEnvelope`.

Type hashes ride in MQTT v5 user properties as the `type-hash` pair (`internal/transports/mqtt/v5.go`). Publish sends the caller hash; Take populates `IncomingMessage.TypeHash`, which is new as an optional field on the take path. Unknown properties fail loudly on encode and in validation: decoders reject unknown property IDs instead of silently dropping metadata, and the live receive path drops malformed inbound packets (a hostile broker cannot smuggle semantics past the queue; refusals always surface as errors).

## QoS and retention

- Config `QoS` 0 through 2, `Retained` bool per backend (`internal/transports/mqtt/config.go`). QoS 2 runs the exactly-once handshake proven in `internal/transports/mqtt/proof/qos2.tla`. The in-memory connection is a test double: it neither dedups QoS 2 redeliveries nor enforces the 1024 inflight cap, and it keeps no retained store.
- v1 default: QoS 0 data, no retained data. Retained is reserved for discovery announcements.
- MQTT QoS is transport reliability only. Offer versus request matching stays in core. Do not read MQTT QoS as a second QoS engine.

## Connecting

`mqtt.Dial(ctx, cfg)` resolves the broker URL (`tcp` or `mqtt` plain, `tls`, `mqtts`, or `ssl` for TLS, default ports 1883 and 8883), loads the optional CA bundle plus client pair, and handshakes CONNECT (v5 or 3.1.1 per `MqttConfig.Version`, v5 by default) with username, password, and the last will. Wrong credentials fail with the broker reason code. Passwords and keys never appear in errors.

Drops redial with backoff (100 ms doubling to 5 s, context bounded), resubscribe the stored filters, and replay unacked publishes per phase: QoS 1 with DUP set, QoS 2 as PUBLISH with DUP while PUBREC is awaited and bare PUBREL while PUBCOMP is awaited. The receive queue survives across redials, so takes block through the outage instead of failing. `Backend.Reconnects` counts successful redials. QoS 0 in flight during a drop is lost by protocol design; use QoS 1 where it matters.

## Versions: 5.0 and 3.1.1 on one transport

`MqttConfig.Version` selects the framing; the default zero value is `Version5`. Topic mapping, envelope, discovery, services, actions, reconnect, and routing are identical on both — only the packet border branches (`internal/transports/mqtt/packet.go`):

- CONNECT: v5 sends level 5 with property lengths; 3.1.1 sends level 4 with no properties. Username, password, will, and keepalive work on both. AUTH packets exist only on v5 and are rejected on 3.1.1; the client never initiates enhanced auth.
- PUBLISH: v5 carries the type hash in user properties; 3.1.1 has no properties, so takes report an empty hash and matching degrades to the hashless path (the same path hashless Humble peers use). The first publish with a hash on 3.1.1 logs one loud line instead of failing the flow.
- SUBSCRIBE omits properties on 3.1.1; UNSUBSCRIBE likewise. SUBACK, PUBACK, PUBREC, PUBCOMP, and UNSUBACK accept shapes are checked per version (v3.1.1 bodies are always bare, v5 bodies carry a validated property section); CONNACK is exactly 2 bytes on 3.1.1 and flags plus reason plus a validated property section on v5. Fixed header flags are checked on every ack (zero everywhere except PUBREL `0010`).
- DISCONNECT is 3 bytes on v5 and 2 bytes on 3.1.1.
- Unknown versions are rejected at config validation and at every codec entry.

Do not add a second transport package for 3.1.1: that would duplicate the topic, discovery, service, action, reconnect, and test layers for a framing difference. The live gate proves both versions against the same broker (`TestMQTTGateV311RoundTrip` plus the v5 gates).

## Discovery and liveliness

- `JoinDiscovery` marks joined, idempotent. It starts the pump and live sessions also SUBSCRIBE to the `rt`, `rq`, `rr` wildcards plus the `sdp` wildcard before returning, so retained announcements arrive before the first take. No multicast, no SPDP.
- `LeaveDiscovery` unsubscribes the same filters with UNSUBACK validation, dropping them from the resubscribe set so a racing reconnect cannot resurrect them. QoS on every leg, including the discovery wildcard, may be 0, 1, or 2.
- The pump routes connection samples: `sdp` topics feed the endpoint cache, data legs queue for the take paths. A full take inbox evicts the oldest sample and counts one drop. Discovery therefore works with no take, matching the DDS SEDP loops.
- `ReportLiveliness` enforces the same caller owned generation watermark as every backend: greater wins, same generation with same alive is a no-op, else `ErrStaleGeneration`. The last will covers ungraceful kills: configure `MqttWill` so the broker publishes the gone marker when the session drops without DISCONNECT.
- `AnnounceEndpoint` publishes one retained JSON body (`type`, `type_hash`, `alive`) on its discovery topic; `RetractEndpoint` publishes the tombstone. Late joiners learn the index from the broker. `HasRemoteEndpoint` reads the local cache for CLI parity with the DDS backend.

## Services on rq / rr

- `PublishService` with `ServiceRoleClient` publishes requests and `TakeService` with `ServiceRoleServer` takes them; the reverse serves replies (`internal/transports/mqtt/service.go`). Payloads stay opaque service envelopes; type hashes ride user properties like data.
- `TakeBytes` delivers every leg with `IncomingMessage.Leg` set, so `ForwardOnce` bridges services without a second path. `TakeService` stays as a typed filter for callers that want one role only. `TakeService` parks data-leg samples for `TakeBytes` and parks wrong-kind service samples for later service takes, each buffer bounded at 1024 entries with oldest-evicted counting, so a consumer that only ever takes one leg cannot grow memory without bound. `TakeBytes` itself never parks: it returns whatever leg the inbox head carries, tagged, so a data-only loop can starve a concurrent service consumer of its leg (and vice versa for a service-only loop and data).

## Actions on the same legs

Action operations reuse service and data legs: send goal, cancel goal, and get result are services on `rq` / `rr`, feedback and status are data topics on `rt`. `mqtt/action.go` owns only the ROS exchange names (`/fibonacci/_action/send_goal` and parallels, mirroring the DDS backend); service calls use `PublishService` plus `TakeService` with those names, feedback and status use `PublishBytes` plus `TakeBytes`.

## Bridge: DDS in, MQTT out

```go
msg, err := transports.ForwardOnce(ctx, ddsBackend, mqttBackend)
err = transports.ForwardLoop(ctx, ddsBackend, mqttBackend)
```

Take one sample from `src`, publish the same topic plus type plus hash plus leg to `dst`, return the taken sample. Takes are destructive: a publish failure after a successful take drops the sample, so the error names the failed leg. Context cancel aborts either leg. Nil backends and unknown legs fail loudly (an empty leg reads as data). Code: `internal/transports/forward.go`. Test: `internal/transports/forward_test.go` publishes to src, forwards once, takes from dst, including a request leg case.

Leg caveat: DDS takes predate legs and arrive unset, which reads as data. MQTT to MQTT bridges preserve request and reply legs exactly; DDS service samples bridge as data. Closing that gap belongs to the DDS backend and is out of scope here.

Router note: `Router.TakeBytes` races all backends for generic multiplexing. For a directed DDS to MQTT pipe use `ForwardLoop`, not the router, so samples cannot be consumed by the wrong leg.

## Size: MQTT is smaller

Per sample overhead, user payload excluded:

DDS RTPS path:

- RTPS message header 20 (`rtps/packet.go:HeaderSize`)
- Submessage header 4 (`SubmessageHeaderSize`)
- DATA body fixed 20 (2 reserved plus 2 offsets plus 4 reader plus 4 writer plus 8 seq)
- Weld DATA frame fixed 36 (16 GUID plus 8 seq plus 3x4 lengths) plus topic bytes plus DDS type bytes
- XCDR envelope header 4 plus pad to 4 (`internal/types/envelope.go:MarshalMessage`)

Fixed subtotal: 20 plus 4 plus 20 plus 36 plus 4 = 84, plus `len(ddsTopic)` plus `len(ddsType)`.

MQTT path (QoS 0, remaining length under 128):

- Fixed header 2
- Topic name field 2 plus `len(mqttTopic)`
- Properties 1 when empty, more with a hash (hash pair is 1 id plus key plus value bytes)
- Envelope 4 plus `len(idlType)`

Fixed subtotal: 9, plus `len(mqttTopic)` plus `len(idlType)`, plus hash pair bytes when present.

Worked example with `/chatter` and `std_msgs__msg__String`:

- DDS topic `rt/chatter` is 10 bytes. DDS type `std_msgs::msg::dds_::String_` is 28 bytes. Overhead = 84 plus 10 plus 28 = 122 bytes plus user payload plus 0-3 pad.
- MQTT topic `weld/0/rt/chatter` is 17 bytes. IDL type `std_msgs__msg__String` is 21 bytes. Overhead empty props = 9 plus 17 plus 21 = 47 bytes plus user payload, no pad. With an 11 char hash the user property adds 26 bytes, totaling 72, still 50 under DDS.
- Saving: 122 minus 47 = 75 bytes per sample without hash, about 61 percent less overhead.

Background traffic also drops: no 50 ms SPDP plus SEDP multicast ticks (`internal/transports/dds/backend.go`), replaced by broker retained state. TCP plus TLS framing after connect is amortized and not counted above. The tradeoff is a broker in the path instead of peer multicast.

## Security and limits

- TLS uses the host trust store plus an optional CA bundle and client pair from config (`MqttTLS`). Only `tls`, `mqtts`, and `ssl` schemes dial TLS; plain schemes never upgrade. A lone cert or key file is rejected. Secured DDS peers stay refused loudly; the bridge must never launder them across.
- Auth is username plus password in CONNECT. A password without a username is rejected at config validation. Credentials never appear in errors or logs. The gate proves refusal on a wrong password and a round trip on the right one.
- Caps follow the contract: 32 MiB user payload bound, 1024 topic and type bounds on the DDS side, MQTT prefix rules above. QoS 3 is never sent and fails loudly on both encode and decode.
- Domain isolation is by topic prefix plus broker ACL. Cross domain takes error instead of leaking.

## Wire hardening

Every bound below is pinned by unit tests plus the scripted-broker suite, and the live gate proves the same bytes against mosquitto.

- Inbound cap: one framed packet is at most 40 MiB on the wire, sized to admit the largest sample weld itself can send (32 MiB payload plus topic plus 32 max-size user properties). The remaining length is validated before the body allocates, so a corrupt or hostile length fails fast instead of allocating up to 256 MiB.
- UTF-8: ROS topic names, participant names, will topics, wire topics, and all property strings must be valid UTF-8 with no NUL byte, on encode and decode. The broker would drop the session on malformed UTF-8, so weld refuses to send or route it.
- Auth: broker URLs with embedded userinfo are rejected instead of silently dropping the credentials. A password without a username is rejected at config validation and at CONNECT encode.
- Acks: PUBACK reasons at or above 0x80 fail the publish, and a SUBACK with no granted code or any grant at or above 0x80 fails the subscribe. PUBACK property sections and CONNACK shapes are length checked per version (v3.1.1 CONNACK is exactly 2 bytes, v5 carries a validated property length); fixed header flags must be zero. A broker refusal surfaces as an error, never a silent stall.
- Half-open links: a keepalive watchdog closes a connection that delivers nothing for 1.5 keepalive intervals (default keepalive 30 s), which triggers resubscribe plus DUP republish through the normal reconnect path. Ping writes never end the keepalive loop, and every dial (30 s bound when the caller sets none) and handshake plus resume round (10 s bound) carries a deadline. Steady-state writes carry a size-scaled floor of 15 s; the goodbye DISCONNECT gets 5 s.
- In-flight bound: at most 1024 unacked QoS 1 plus QoS 2 samples combined. Packet ids skip live waiter and inflight ids and collide loudly instead of aliasing; waiter cleanup is identity checked. A cancelled or failed QoS 1 publish abandons its inflight slot so the cap cannot jam; a cancelled QoS 2 publish keeps its slot for resume replay so the handshake still converges.
- Queues: the live receive queue holds at most 1024 samples with evict-oldest counting; the in-memory queue holds the same bound but evicts silently (dev/test mode only, no counter). The backend inbox behind them holds 256 with evict-oldest counting. Mid-resume arrivals are buffered up to the same 1024 bound and any excess is counted in the connection drop counter.
- Properties: at most 32 extra user property pairs per sample, each length capped, so a hostile property section cannot force unbounded map growth.
- Outbound sizes: type names cap at 1024 bytes, payloads at 32 MiB, matching the DDS sample cap on the same contract.

## Proof

- TLA+: `internal/transports/mqtt/proof/qos2.tla` models the whole QoS 2 handshake (sender, receiver, lossy duplicating network, reconnect epochs). TLC proves type safety, at-most-once delivery, sender-done implies delivered-exactly-once, receiver-broker state consistency, and convergence under bounded loss (`qos2_1.cfg` single id, `qos2_2.cfg` two-id aliasing). The Go sender (`netconn.go`), receiver (`netio.go`), and resume (`reconnect.go`) model themselves after the spec actions. Three more specs live beside it: `reconnect.tla` (no zombie socket, read loop halts on Close, silent links redial, one live socket per epoch), `liveliness.tla` (generation watermarks never regress, no lost updates, retracts never hide newer announces), and `takelegs.tla` (no duplicate delivery, counted-only loss including bounded leg-buffer eviction, no starvation). `internal/transports/proof/router.tla` covers the TakeBytes multiplexer the same way: first win returns at once, stragglers drain into pending, cancellation never drops a consumed sample. Each proof folder has a README mapping specs to code. Deliberate boundaries stay documented there: mixed-version peers without generation fields resolve by arrival order, TakeBytes returns whatever leg the inbox head carries (only TakeService parks cross-leg samples), and will topics carry no generation.
- Unit: `go test ./internal/transports/ ./internal/transports/mqtt/`
- Wire: scripted broker test proves CONNECT, SUBSCRIBE, PUBLISH with user properties, retained discovery, service legs, and a drop plus DUP republish, byte for byte (`internal/transports/mqtt/netconn_test.go`).
- Live gate, always compatible: `cd testbed && docker compose up -d mosquitto`, then `go test -tags live -count=1 -timeout 10m ./testbed/live/mqtt/`. The sidecar ships in `testbed/docker-compose.yml` beside the ROS fleet. Eight gates against real mosquitto: data with hash and leg (v5 and v3.1.1), QoS 2 exactly-once, retained discovery for late joiners plus retract, services on `rq` / `rr`, TLS on both versions, auth plus refusal, and a broker restart under QoS 1 with reconnect plus post restart traffic. SUBACK and PUBACK refusals are proven byte for byte against the scripted broker in unit tests.
