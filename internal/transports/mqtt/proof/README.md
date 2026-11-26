# Proofs: MQTT transport

TLA+ specs for the MQTT transport. Each spec models the Go code
listed beside it; TLC checks the stated properties. Run from this
directory, e.g.:

```sh
java -XX:+UseParallelGC -cp /tmp/opencode/tla2tools.jar \
  tlc2.TLC -workers 8 -config qos2_2.cfg qos2.tla
```

Config notes: constants use explicit sets (this TLC build rejects
`..` ranges). `states/` and `*_TTrace*` are TLC working files and
are git-ignored, not proof artifacts.

## qos2.tla — exactly-once handshake

Models the QoS 2 sender and receiver over a lossy duplicating
network plus reconnect epochs. Code: `../netconn.go` (sender,
`publishQoS2`, `registerInflight`, `nextPacketID`), `../netio.go`
(receiver `receiveQoS2`, `dispatchPubRel`, `handlePubRec`,
`ackPublish`, ack senders), `../reconnect.go` (per-phase replay in
`republishPending`), `../packet.go` (framing validated by unit
vectors, out of scope here), `../v5.go` (property table, out of
scope here). Refusals (`>= 0x80`) complete the waiter with an error
and are unit-tested, not modeled.

- `qos2_1.cfg`: one packet id, loss budget 2, one reconnect epoch.
- `qos2_2.cfg`: two packet ids (aliasing check), loss budget 2, one
  reconnect epoch.

Proven: type safety, at-most-once delivery, sender-done implies
delivered-exactly-once with receiver done, receiver-half-open
implies sender-live (no-confusion), convergence under bounded loss,
deadlock freedom.

## reconnect.tla — reconnect lifecycle

Models the read loop, reconnect attempts, Close, and the ping
watchdog. Code: `../netconn.go` (`Close`, `sendGoodbye`),
`../netio.go` (`readLoop`, `readOne`,
`handleReadError`, `handleDisconnect`, `pingLoop`,
`watchdogFired`), `../reconnect.go` (`reconnect`,
`tryReconnect`, `resume`, `resubscribeAll`, `readSubscribeAck`,
`currentConn`). Locks (`reconMu`, `writeMu`, `mu`) are abstracted to the atomic
check-then-swap vs atomic close both sides hold in code.

- `reconnect_1.cfg`: loss budget 2, one reconnect epoch.
- `reconnect_2.cfg`: loss budget 2, two reconnect epochs.

Proven: type safety, no zombie socket after Close, read loop halts
on Close, silent links eventually redial, one live socket per
epoch with strictly increasing epochs, deadlock freedom.

## liveliness.tla — discovery generation ordering

Models per-participant generation watermarks across reports,
announcements, retracts, and last-will markers. Code:
`../backend.go` (`ReportLiveliness`, `lastGen`, `lastAlive`),
`../discovery.go` (endpoint cache, `AnnounceEndpoint`,
`RetractEndpoint`), `../config.go` (`MqttWill`).

- `liveliness_1.cfg`: one participant, generations 0..2.
- `liveliness_2.cfg`: two participants, generations 0..2.

Proven: generations never decrease, stale reports never overwrite,
no lost update (max generation always lands), retracts never hide
strictly newer announces.

Implemented in code since the proof: `discovery.go` carries a
per-writer discovery sequence on every retained announcement and
tombstone (`endpointRecord.Generation`), and `install` applies only
first sightings and strictly newer generations — the same
greater-wins rule the spec verifies. A delayed retract can no longer
hide a newer announce, in either direction. Remaining gap: will
topics carry no generation, so a stale broker will cannot be ordered
against live reports; that path stays unmodeled by design.

## takelegs.tla — take-path leg buffering

Models the pump, the bounded inbox with evict-oldest counting, the
two leg buffers, and both take paths. Code: `../backend.go`
(`pump`, `stageWire`, `TakeBytes`, `dataPending`), `../service.go`
(`TakeService`, `svcPending`), inbox cap in `../backend.go`.

- `takelegs_1.cfg`: 2 data + 2 service samples, inbox cap 2.
- `takelegs_2.cfg`: 3 data + 3 service samples, inbox cap 2.

Proven: type safety, no duplication across both paths, eviction
(inbox or leg buffer) is the only loss and every loss increments the
drop counter, no starvation under fair takes, leg separation, leg
buffers bounded with evict-oldest and counted drops.

Note: real `TakeBytes` returns whatever leg the inbox head carries
rather than parking cross-leg samples as the spec's symmetric
buffering does; the no-dup/no-silent-loss arguments carry over
unchanged. Evicting a full leg buffer can drop cross-leg samples a
data-only or service-only consumer never takes; the drop is counted
in `dropped`, never silent.
