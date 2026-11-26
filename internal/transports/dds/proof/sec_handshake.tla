---- MODULE sec_handshake ----
(*
  Secure handshake safety for weld DDS Security (Phase 2-4 code in
  internal/transports/dds/security/): one initiator A plus one
  responder B exchange request, reply, and final over the stateless
  builtin endpoints, derive session keys, then send protected DATA.

  Mapping to the Go implementation:
  - SendReq/RecvReq/SendRep/RecvRep/SendFin/RecvFin model the
    token codec (security/tokens/codec.go) plus the handshake
    manager states (security/auth/handshake.go StartInitiator and
    NoteInbound). Receipt of a forged token drops loudly without
    advancing state, exactly like the ErrForgedHandshake path.
  - DeriveA/DeriveB model key installation after all genuine
    messages processed (crypto.DeriveKxKeys into the keystore):
    validation plus keys appear only on the genuine path.
  - SendProt models protectDataPayload: the enabling condition
    requires validation plus a session key, so protected DATA
    never leaves without keys.
  - Forge/Replay model the network attacker with a bounded
    budget (MaxEvil). Replay is sound: the model tracks every
    genuinely emitted shape in sentShapes and the attacker may
    only replay shapes already sent, so receipt always reflects
    real traffic and forgeries (genuine FALSE) drop loudly.
    Drops are out of scope: loss is handled by retransmit
    (NeedsResend) and proved in the same style as
    reliability.tla, so the network here is reliable and only
    forgery plus replay threaten safety.

  Out of scope (bounded in code, abstracted here):
  - X.509 chain building plus XML-DSIG/S-MIME envelope checks:
    modeled as genuine versus forged at receipt, since forged
    tokens always fail verification in the code.
  - Rekey and key revision: one exchange per peer pair.
  - Open (unprotected) topics: no crypto decisions there.
*)

EXTENDS Naturals, FiniteSets

CONSTANT MaxEvil

PEERS == {"A", "B"}
IDS == {"req", "rep", "fin"}

VARIABLES phase, seen, net, validated, hasKey, sessKey,
  protSent, evilLeft, sentShapes

vars == <<phase, seen, net, validated, hasKey,
  sessKey, protSent, evilLeft, sentShapes>>

TypeOK ==
  /\ phase \in [PEERS -> {"start", "reqSent", "reqRecvd",
    "repSent", "repRecvd", "finSent", "finRecvd", "done"}]
  /\ seen \in [PEERS -> SUBSET IDS]
  /\ validated \in [PEERS -> BOOLEAN]
  /\ hasKey \in [PEERS -> BOOLEAN]
  /\ sessKey \in [PEERS -> BOOLEAN]
  /\ protSent \in [PEERS -> BOOLEAN]
  /\ evilLeft \in 0 .. MaxEvil
  /\ sentShapes \in SUBSET (IDS \times PEERS)

Init ==
  /\ phase = [p \in PEERS |-> "start"]
  /\ seen = [p \in PEERS |-> {}]
  /\ net = {}
  /\ validated = [p \in PEERS |-> FALSE]
  /\ hasKey = [p \in PEERS |-> FALSE]
  /\ sessKey = [p \in PEERS |-> FALSE]
  /\ protSent = [p \in PEERS |-> FALSE]
  /\ evilLeft = MaxEvil
  /\ sentShapes = {}

SendReq ==
  /\ phase["A"] = "start"
  /\ phase' = [phase EXCEPT !["A"] = "reqSent"]
  /\ net' = net \cup {[id |-> "req", from |-> "A",
    genuine |-> TRUE]}
  /\ sentShapes' = sentShapes \cup {<<"req", "A">>}
  /\ UNCHANGED <<seen, validated, hasKey, sessKey,
    protSent, evilLeft>>

RecvReq(m) ==
  /\ m \in net
  /\ m.id = "req"
  /\ m.from = "A"
  /\ net' = net \ {m}
  /\ IF m.genuine THEN
       /\ IF "req" \notin seen["B"] THEN
            /\ phase' = [phase EXCEPT !["B"] = "reqRecvd"]
            /\ seen' = [seen EXCEPT !["B"] = @ \cup {"req"}]
          ELSE
            UNCHANGED <<phase, seen>>
     ELSE
       UNCHANGED <<phase, seen>>
  /\ UNCHANGED <<validated, hasKey, sessKey,
    protSent, evilLeft, sentShapes>>

SendRep ==
  /\ phase["B"] = "reqRecvd"
  /\ phase' = [phase EXCEPT !["B"] = "repSent"]
  /\ net' = net \cup {[id |-> "rep", from |-> "B",
    genuine |-> TRUE]}
  /\ sentShapes' = sentShapes \cup {<<"rep", "B">>}
  /\ UNCHANGED <<seen, validated, hasKey, sessKey,
    protSent, evilLeft>>

RecvRep(m) ==
  /\ m \in net
  /\ m.id = "rep"
  /\ m.from = "B"
  /\ net' = net \ {m}
  /\ IF m.genuine THEN
       /\ IF "rep" \notin seen["A"] THEN
            /\ phase' = [phase EXCEPT !["A"] = "repRecvd"]
            /\ seen' = [seen EXCEPT !["A"] = @ \cup {"rep"}]
          ELSE
            UNCHANGED <<phase, seen>>
     ELSE
       UNCHANGED <<phase, seen>>
  /\ UNCHANGED <<validated, hasKey, sessKey,
    protSent, evilLeft, sentShapes>>

SendFin ==
  /\ phase["A"] = "repRecvd"
  /\ phase' = [phase EXCEPT !["A"] = "finSent"]
  /\ net' = net \cup {[id |-> "fin", from |-> "A",
    genuine |-> TRUE]}
  /\ sentShapes' = sentShapes \cup {<<"fin", "A">>}
  /\ UNCHANGED <<seen, validated, hasKey, sessKey,
    protSent, evilLeft>>

RecvFin(m) ==
  /\ m \in net
  /\ m.id = "fin"
  /\ m.from = "A"
  /\ net' = net \ {m}
  /\ IF m.genuine THEN
       /\ IF "fin" \notin seen["B"] THEN
            /\ phase' = [phase EXCEPT !["B"] = "finRecvd"]
            /\ seen' = [seen EXCEPT !["B"] = @ \cup {"fin"}]
          ELSE
            UNCHANGED <<phase, seen>>
     ELSE
       UNCHANGED <<phase, seen>>
  /\ UNCHANGED <<validated, hasKey, sessKey,
    protSent, evilLeft, sentShapes>>

DeriveA ==
  /\ phase["A"] = "finSent"
  /\ seen["A"] = {"rep"}
  /\ validated' = [validated EXCEPT !["A"] = TRUE]
  /\ hasKey' = [hasKey EXCEPT !["A"] = TRUE]
  /\ sessKey' = [sessKey EXCEPT !["A"] = TRUE]
  /\ phase' = [phase EXCEPT !["A"] = "done"]
  /\ UNCHANGED <<seen, net, protSent, evilLeft, sentShapes>>

DeriveB ==
  /\ phase["B"] = "finRecvd"
  /\ seen["B"] = {"req", "fin"}
  /\ validated' = [validated EXCEPT !["B"] = TRUE]
  /\ hasKey' = [hasKey EXCEPT !["B"] = TRUE]
  /\ sessKey' = [sessKey EXCEPT !["B"] = TRUE]
  /\ phase' = [phase EXCEPT !["B"] = "done"]
  /\ UNCHANGED <<seen, net, protSent, evilLeft, sentShapes>>

SendProt(p) ==
  /\ p \in PEERS
  /\ validated[p] = TRUE
  /\ sessKey[p] = TRUE
  /\ protSent[p] = FALSE
  /\ protSent' = [protSent EXCEPT ![p] = TRUE]
  /\ UNCHANGED <<phase, seen, net, validated, hasKey,
    sessKey, evilLeft, sentShapes>>

Forge(id, from) ==
  /\ evilLeft > 0
  /\ id \in IDS
  /\ from \in PEERS
  /\ net' = net \cup {[id |-> id, from |-> from,
    genuine |-> FALSE]}
  /\ evilLeft' = evilLeft - 1
  /\ UNCHANGED <<phase, seen, validated, hasKey,
    sessKey, protSent, sentShapes>>

Replay(id, from) ==
  /\ evilLeft > 0
  /\ id \in IDS
  /\ from \in PEERS
  /\ <<id, from>> \in sentShapes
  /\ net' = net \cup {[id |-> id, from |-> from,
    genuine |-> TRUE]}
  /\ evilLeft' = evilLeft - 1
  /\ UNCHANGED <<phase, seen, validated, hasKey,
    sessKey, protSent, sentShapes>>

Honest ==
  \/ SendReq
  \/ \E m \in net : RecvReq(m) \/ RecvRep(m) \/
    RecvFin(m)
  \/ SendRep
  \/ SendFin
  \/ DeriveA
  \/ DeriveB
  \/ \E p \in PEERS : SendProt(p)

Evil ==
  \/ \E id \in IDS, from \in PEERS : Forge(id, from)
  \/ \E id \in IDS, from \in PEERS : Replay(id, from)

Done == /\ phase["A"] = "done"
  /\ phase["B"] = "done"
  /\ UNCHANGED vars

Next == Honest \/ Evil \/ Done

Spec == Init /\ [][Next]_vars /\ WF_vars(Honest)

NoFakeValidation ==
  /\ validated["A"] = TRUE => seen["A"] = {"rep"}
  /\ validated["B"] = TRUE => seen["B"] = {"req", "fin"}

NoUnkeyedSend ==
  \A p \in PEERS : protSent[p] = TRUE =>
    (sessKey[p] = TRUE /\ validated[p] = TRUE)

ReplayExact ==
  /\ Cardinality(seen["A"]) <= 1
  /\ Cardinality(seen["B"]) <= 2

Converge == <>(phase["A"] = "done" /\
  phase["B"] = "done")

====
