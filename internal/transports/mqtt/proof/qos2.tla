---- MODULE qos2 ----
(*
  Exactly-once handshake for weld MQTT QoS 2, sender plus receiver plus
  lossy network plus reconnect epochs. This is the whole protocol: the
  Go implementation in netconn.go, netio.go, and reconnect.go models
  itself after these actions, and qos2_test.go pins the same traces.
  Refusals (>= 0x80 reason codes) are out of scope here: they complete
  the waiter with an error and free the id, proven by unit tests, and
  never reach the steady handshake below.
*)
EXTENDS Naturals, FiniteSets, TLC

CONSTANTS IDS, MaxCopies, MaxLoss, MaxEpoch

ASSUME Cardinality(IDS) >= 1

SPhase == {"idle", "pub", "rel", "done"}
RState == {"idle", "gotPub", "done"}

VARIABLES sPhase, rState, delivered, net, lossLeft, epochLeft

vars == <<sPhase, rState, delivered, net, lossLeft, epochLeft>>

Msg == [type : {"PUB", "REC", "REL", "COMP"}, id : IDS]

EmptyNet == [m \in Msg |-> 0]

TypeOK ==
    /\ sPhase \in [IDS -> SPhase]
    /\ rState \in [IDS -> RState]
    /\ delivered \in [IDS -> 0 .. 1]
    /\ \A m \in Msg : net[m] \in 0 .. MaxCopies
    /\ lossLeft \in 0 .. MaxLoss
    /\ epochLeft \in 0 .. MaxEpoch

Init ==
    /\ sPhase = [id \in IDS |-> "idle"]
    /\ rState = [id \in IDS |-> "idle"]
    /\ delivered = [id \in IDS |-> 0]
    /\ net = EmptyNet
    /\ lossLeft = MaxLoss
    /\ epochLeft = MaxEpoch

Live == \E id \in IDS : sPhase[id] \in {"pub", "rel"}

Put(m) == net' = [x \in Msg |->
    IF x = m THEN IF net[x] < MaxCopies THEN net[x] + 1 ELSE net[x]
               ELSE net[x]]

Take(m) == net[m] > 0 /\ net' = [net EXCEPT ![m] = @ - 1]

Same(v) == UNCHANGED v

SendP(id) ==
    /\ sPhase[id] = "idle"
    /\ sPhase' = [sPhase EXCEPT ![id] = "pub"]
    /\ Put([type |-> "PUB", id |-> id])
    /\ Same(rState) /\ Same(delivered)
    /\ Same(lossLeft) /\ Same(epochLeft)

RePub(id) ==
    /\ sPhase[id] = "pub"
    /\ Put([type |-> "PUB", id |-> id])
    /\ Same(sPhase) /\ Same(rState) /\ Same(delivered)
    /\ Same(lossLeft) /\ Same(epochLeft)

ReRel(id) ==
    /\ sPhase[id] = "rel"
    /\ Put([type |-> "REL", id |-> id])
    /\ Same(sPhase) /\ Same(rState) /\ Same(delivered)
    /\ Same(lossLeft) /\ Same(epochLeft)

OnPub(id) ==
    /\ net[[type |-> "PUB", id |-> id]] > 0
    /\ IF rState[id] = "idle"
       THEN /\ rState' = [rState EXCEPT ![id] = "gotPub"]
            /\ delivered' = [delivered EXCEPT ![id] = 1]
            /\ net' = [m \in Msg |->
                    IF m = [type |-> "REC", id |-> id]
                    THEN IF net[m] < MaxCopies THEN net[m] + 1 ELSE net[m]
                    ELSE (IF m = [type |-> "PUB", id |-> id]
                          THEN net[m] - 1 ELSE net[m])]
       ELSE /\ net' = [m \in Msg |->
                    IF m = [type |-> "REC", id |-> id]
                    THEN IF net[m] < MaxCopies THEN net[m] + 1 ELSE net[m]
                    ELSE (IF m = [type |-> "PUB", id |-> id]
                          THEN net[m] - 1 ELSE net[m])]
            /\ Same(rState) /\ Same(delivered)
    /\ Same(sPhase) /\ Same(lossLeft) /\ Same(epochLeft)

OnRec(id) ==
    /\ net[[type |-> "REC", id |-> id]] > 0
    /\ IF sPhase[id] = "pub"
       THEN /\ sPhase' = [sPhase EXCEPT ![id] = "rel"]
            /\ net' = [m \in Msg |->
                    IF m = [type |-> "REL", id |-> id]
                    THEN IF net[m] < MaxCopies THEN net[m] + 1 ELSE net[m]
                    ELSE (IF m = [type |-> "REC", id |-> id]
                          THEN net[m] - 1 ELSE net[m])]
       ELSE /\ net' = [m \in Msg |->
                    IF m = [type |-> "REC", id |-> id]
                    THEN net[m] - 1 ELSE net[m]]
            /\ Same(sPhase)
    /\ Same(rState) /\ Same(delivered)
    /\ Same(lossLeft) /\ Same(epochLeft)

OnRel(id) ==
    /\ net[[type |-> "REL", id |-> id]] > 0
    /\ net' = [m \in Msg |->
            IF m = [type |-> "COMP", id |-> id]
            THEN IF net[m] < MaxCopies THEN net[m] + 1 ELSE net[m]
            ELSE (IF m = [type |-> "REL", id |-> id]
                  THEN net[m] - 1 ELSE net[m])]
    /\ IF rState[id] = "gotPub"
       THEN rState' = [rState EXCEPT ![id] = "done"]
       ELSE Same(rState)
    /\ Same(sPhase) /\ Same(delivered)
    /\ Same(lossLeft) /\ Same(epochLeft)

OnComp(id) ==
    /\ net[[type |-> "COMP", id |-> id]] > 0
    /\ net' = [net EXCEPT ![[type |-> "COMP", id |-> id]] = @ - 1]
    /\ IF sPhase[id] = "rel"
       THEN sPhase' = [sPhase EXCEPT ![id] = "done"]
       ELSE Same(sPhase)
    /\ Same(rState) /\ Same(delivered)
    /\ Same(lossLeft) /\ Same(epochLeft)

Drop(m) ==
    /\ net[m] > 0
    /\ IF Live THEN lossLeft > 0 /\ lossLeft' = lossLeft - 1
                 ELSE Same(lossLeft)
    /\ net' = [net EXCEPT ![m] = @ - 1]
    /\ Same(sPhase) /\ Same(rState) /\ Same(delivered) /\ Same(epochLeft)

Reconnect ==
    /\ epochLeft > 0
    /\ Live
    /\ epochLeft' = epochLeft - 1
    /\ net' = EmptyNet
    /\ Same(sPhase) /\ Same(rState) /\ Same(delivered) /\ Same(lossLeft)

Halt ==
    /\ \A id \in IDS : sPhase[id] = "done"
    /\ UNCHANGED vars

Next ==
    \/ \E id \in IDS : SendP(id)
    \/ \E id \in IDS : RePub(id)
    \/ \E id \in IDS : ReRel(id)
    \/ \E id \in IDS : OnPub(id)
    \/ \E id \in IDS : OnRec(id)
    \/ \E id \in IDS : OnRel(id)
    \/ \E id \in IDS : OnComp(id)
    \/ \E m \in Msg : Drop(m)
    \/ Reconnect
    \/ Halt

Fair ==
    /\ \A id \in IDS : WF_vars(SendP(id))
    /\ \A id \in IDS : WF_vars(RePub(id))
    /\ \A id \in IDS : WF_vars(ReRel(id))
    /\ \A id \in IDS : WF_vars(OnPub(id))
    /\ \A id \in IDS : WF_vars(OnRec(id))
    /\ \A id \in IDS : WF_vars(OnRel(id))
    /\ \A id \in IDS : WF_vars(OnComp(id))

Spec == Init /\ [][Next]_vars /\ Fair

RecvConsistent == \A id \in IDS :
    rState[id] = "gotPub" => sPhase[id] \in {"pub", "rel"}

DoneImpliesDelivered == \A id \in IDS :
    sPhase[id] = "done" => delivered[id] = 1 /\ rState[id] = "done"

LiveEventually == \A id \in IDS : <>(sPhase[id] = "done")
====
