---- MODULE reliability ----
(*
  Reliable user-DATA delivery for weld DDS: one writer plus one reader
  over a lossy duplicating network with a bounded loss budget.

  Mapping to the Go implementation (internal/transports/dds):
  - Send models the writer storing a sample in userHistory and emitting
    DATA with the next outer sequence number (SequenceNumber starts at
    1, rtps/submessage.go). Sends are in order, so sent = 1..maxSent.
  - Drop models lossy UDP with a bounded budget, the same pattern as
    the QoS 2 proof (mqtt/proof/qos2.tla): drops decrement lossLeft
    only while work remains (Live).
  - Duplication arises ONLY from redelivery (Repair re-putting a seq),
    never from a spontaneous network-dup action, and each seq holds at
    most one copy in flight (net cap MaxCopies = 1, exactly like the
    QoS 2 Put cap). A repair of an in-flight copy is a no-op.
  - Heartbeat models the writer announcing First..Last. The reader
    records the bound (recordHeartbeatBound, reliability.go) and the
    hbOpen flag marks the announcement as not yet answered.
  - AckNack models ackUserHeartbeat plus sendSelectiveHeartbeatAck:
    the reader answers with base = contig+1 and a selective bitmap
    (selectiveNackBitmap) covering exactly the missing set Missing.
  - Repair models repairFromAck plus serveRepairWindow: the writer
    resends a nacked seq still in history. The model is NARROWER than
    the code on purpose: the code walks base..end and also resends
    already-seen out-of-order seqs as duplicates (it ignores per-bit
    selectivity); the model repairs only nacked (missing) seqs, so a
    green Repair set implies the code's superset also converges, with
    the extra copies absorbed by reader dedup.
  - Deliver models noteRemoteData plus observe (remoteTracker,
    reliability.go) together with DataReceiver.trackSequenceLocked
    plus markDeliveredLocked (datapath.go): the first receipt of a seq
    advances contiguity (contig) through filled gaps, parks
    out-of-order seqs in seen, records got, and stops nacking the seq;
    a repeat receipt of an already-got seq only consumes the copy
    (dedup, cf. ErrStaleSequence counting a drop, never re-presenting).

  Out of scope (bounded in the code, abstracted away here):
  - History eviction plus GAP-for-missing (repairOneSeq gap path): the
    model window covers every sent seq, so repair always finds history.
  - DATA_FRAG reassembly plus NACKFRAG fragment repair (frag.go).
  - Prompt-NACK cap (maybePromptNackOnData, 3 attempts): folded into
    the heartbeat-driven AckNack loop, which is what converges.
  - Heartbeat replay suppression by Count (heartbeatIsReplay): folded
    into the hbOpen answered-flag.
  - ACK/heartbeat transport loss: control is shared state; only DATA
    copies are droppable. Lost accounting (lost/drops counters) is not
    modeled; delivery is.
*)
EXTENDS Naturals, FiniteSets, TLC

CONSTANTS SEQS, MaxCopies, MaxLoss

ASSUME SEQS \subseteq 1 .. 3
ASSUME 1 \in SEQS
ASSUME MaxCopies = 1
ASSUME MaxLoss \in 1 .. 2

VARIABLES maxSent, net, lossLeft, hbLast, hbOpen, contig, seen, got, nack

vars == <<maxSent, net, lossLeft, hbLast, hbOpen, contig, seen, got, nack>>

Width == Cardinality(SEQS)

TypeOK ==
    /\ maxSent \in 0 .. Width
    /\ net \in [SEQS -> 0 .. MaxCopies]
    /\ lossLeft \in 0 .. MaxLoss
    /\ hbLast \in 0 .. Width
    /\ hbOpen \in BOOLEAN
    /\ contig \in 0 .. Width
    /\ seen \subseteq SEQS
    /\ got \in [SEQS -> 0 .. 1]
    /\ nack \subseteq SEQS

Init ==
    /\ maxSent = 0
    /\ net = [s \in SEQS |-> 0]
    /\ lossLeft = MaxLoss
    /\ hbLast = 0
    /\ hbOpen = FALSE
    /\ contig = 0
    /\ seen = {}
    /\ got = [s \in SEQS |-> 0]
    /\ nack = {}

Missing == {s \in SEQS : s <= hbLast /\ s > contig /\ s \notin seen}

AllDone == \A s \in SEQS : got[s] = 1

Live == \E s \in SEQS : got[s] = 0

Same(v) == UNCHANGED v

Send ==
    LET s == maxSent + 1
    IN  /\ s \in SEQS
        /\ maxSent' = s
        /\ net' = [x \in SEQS |->
                IF x = s THEN IF net[x] < MaxCopies THEN net[x] + 1 ELSE net[x]
                           ELSE net[x]]
        /\ Same(lossLeft) /\ Same(hbLast) /\ Same(hbOpen)
        /\ Same(contig) /\ Same(seen) /\ Same(got) /\ Same(nack)

Drop(s) ==
    /\ s \in SEQS
    /\ net[s] > 0
    /\ IF Live THEN lossLeft > 0 /\ lossLeft' = lossLeft - 1
                ELSE Same(lossLeft)
    /\ net' = [net EXCEPT ![s] = @ - 1]
    /\ Same(maxSent) /\ Same(hbLast) /\ Same(hbOpen)
    /\ Same(contig) /\ Same(seen) /\ Same(got) /\ Same(nack)

Heartbeat ==
    /\ maxSent > 0
    /\ hbLast < maxSent
    /\ hbLast' = maxSent
    /\ hbOpen' = TRUE
    /\ Same(maxSent) /\ Same(net) /\ Same(lossLeft)
    /\ Same(contig) /\ Same(seen) /\ Same(got) /\ Same(nack)

AckNack ==
    /\ hbOpen
    /\ nack' = Missing
    /\ hbOpen' = FALSE
    /\ Same(maxSent) /\ Same(net) /\ Same(lossLeft)
    /\ Same(hbLast) /\ Same(contig) /\ Same(seen) /\ Same(got)

Repair(s) ==
    /\ s \in SEQS
    /\ s \in nack
    /\ s <= maxSent
    /\ net[s] = 0
    /\ net' = [x \in SEQS |-> IF x = s THEN 1 ELSE net[x]]
    /\ Same(maxSent) /\ Same(lossLeft) /\ Same(hbLast) /\ Same(hbOpen)
    /\ Same(contig) /\ Same(seen) /\ Same(got) /\ Same(nack)

Deliver(s) ==
    /\ s \in SEQS
    /\ net[s] > 0
    /\ net' = [net EXCEPT ![s] = @ - 1]
    /\ IF got[s] = 0
       THEN LET newSeen == seen \cup {s}
                newContig == CHOOSE c \in contig .. Width :
                    /\ c + 1 \notin newSeen
                    /\ \A k \in (contig + 1) .. c : k \in newSeen
            IN  /\ got' = [got EXCEPT ![s] = 1]
                /\ contig' = newContig
                /\ seen' = {k \in newSeen : k > newContig}
                /\ nack' = nack \ {s}
       ELSE /\ Same(got) /\ Same(contig) /\ Same(seen) /\ Same(nack)
    /\ Same(maxSent) /\ Same(lossLeft) /\ Same(hbLast) /\ Same(hbOpen)

Halt ==
    /\ AllDone
    /\ nack = {}
    /\ \A s \in SEQS : net[s] = 0
    /\ ~hbOpen
    /\ UNCHANGED vars

Next ==
    \/ Send
    \/ Heartbeat
    \/ AckNack
    \/ \E s \in SEQS : Repair(s)
    \/ \E s \in SEQS : Deliver(s)
    \/ \E s \in SEQS : Drop(s)
    \/ Halt

Fair ==
    /\ WF_vars(Send)
    /\ WF_vars(Heartbeat)
    /\ WF_vars(AckNack)
    /\ \A s \in SEQS : WF_vars(Repair(s))
    /\ \A s \in SEQS : WF_vars(Deliver(s))

Spec == Init /\ [][Next]_vars /\ Fair

NoDup == \A s \in SEQS : got[s] <= 1

ReceivedConsistent == \A s \in SEQS : got[s] = 1 <=> (s <= contig \/ s \in seen)

SeenAbove == \A k \in seen : k > contig

NackSound == \A s \in nack : s <= hbLast /\ s > contig /\ s \notin seen

AckExact == ~hbOpen => nack = Missing

InFlightUnreceived == \A s \in SEQS : net[s] > 0 => got[s] = 0

Quiescent == AllDone => nack = {}

QuiescentNetDrained == (AllDone /\ ~hbOpen /\ nack = {}) => (\A s \in SEQS : net[s] = 0)

Converge == \A s \in SEQS : <>(got[s] = 1)
====
