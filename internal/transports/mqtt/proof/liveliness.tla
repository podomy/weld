---- MODULE liveliness ----
(*
  Discovery plus liveliness generation ordering for weld MQTT.

  Go sources (read-only): backend.go ReportLiveliness with the
  lastGen/lastAlive watermarks, discovery.go endpointCache with
  install plus AnnounceEndpoint/RetractEndpoint, config.go MqttWill.
  This spec is the whole ordering protocol the Go code implements.

  Abstraction: one endpoint per participant collapses the
  (participant, kind, rosTopic) key; kind and topic are fixed so only
  the alive bit plus the caller-owned generation matter. Live reports
  are synchronous local calls (no loss, no reorder) while announce,
  retract, and will publications race by interleaving. The broker
  last-will is a retained gone marker with its own generation that
  contends with live reports under the same greater-wins rule.

  Report rule mirrors backend.go exactly: first report for a
  participant stores unconditionally; a greater generation stores and
  advances the watermark; same generation with same alive is a
  no-op success; same generation with different alive and any smaller
  generation are stale and leave state unchanged. Announce stores
  TRUE and retract/will store FALSE under the same comparison. The
  discovery cache guards installs the same way
  (discovery.go install), so RetractNoHide holds in code as in
  model. Same-generation alive flips are rejected in both.
*)
EXTENDS Naturals, FiniteSets, TLC

CONSTANTS PIDS, GENS

ASSUME Cardinality(PIDS) >= 1
ASSUME Cardinality(GENS) >= 1

VARIABLES liveGen, liveAlive, liveSeen, liveMax, liveMaxAlive,
    discGen, discAlive, discSeen, discMax, discMaxAlive

vars == <<liveGen, liveAlive, liveSeen, liveMax, liveMaxAlive,
    discGen, discAlive, discSeen, discMax, discMaxAlive>>

MinGen == CHOOSE g \in GENS : \A h \in GENS : g <= h
MaxGen == CHOOSE g \in GENS : \A h \in GENS : g >= h

TypeOK ==
    /\ liveGen \in [PIDS -> GENS]
    /\ liveAlive \in [PIDS -> BOOLEAN]
    /\ liveSeen \in [PIDS -> BOOLEAN]
    /\ liveMax \in [PIDS -> GENS]
    /\ liveMaxAlive \in [PIDS -> BOOLEAN]
    /\ discGen \in [PIDS -> GENS]
    /\ discAlive \in [PIDS -> BOOLEAN]
    /\ discSeen \in [PIDS -> BOOLEAN]
    /\ discMax \in [PIDS -> GENS]
    /\ discMaxAlive \in [PIDS -> BOOLEAN]

Init ==
    /\ liveGen = [p \in PIDS |-> MinGen]
    /\ liveAlive = [p \in PIDS |-> FALSE]
    /\ liveSeen = [p \in PIDS |-> FALSE]
    /\ liveMax = [p \in PIDS |-> MinGen]
    /\ liveMaxAlive = [p \in PIDS |-> FALSE]
    /\ discGen = [p \in PIDS |-> MinGen]
    /\ discAlive = [p \in PIDS |-> FALSE]
    /\ discSeen = [p \in PIDS |-> FALSE]
    /\ discMax = [p \in PIDS |-> MinGen]
    /\ discMaxAlive = [p \in PIDS |-> FALSE]

(*
  Report mirrors backend.go ReportLiveliness: first store, greater
  wins, same plus same is a no-op, otherwise stale with no state
  change. Stale branches are stuttering, so WF forces fresh progress.
*)
Report(p, g, a) ==
    /\ g \in GENS
    /\ a \in BOOLEAN
    /\ \/ /\ \/ \neg liveSeen[p]
              \/ g > liveGen[p]
           /\ liveGen' = [liveGen EXCEPT ![p] = g]
           /\ liveAlive' = [liveAlive EXCEPT ![p] = a]
           /\ liveSeen' = [liveSeen EXCEPT ![p] = TRUE]
           /\ liveMax' = [liveMax EXCEPT ![p] = IF g > liveMax[p] THEN g ELSE liveMax[p]]
           /\ liveMaxAlive' = [liveMaxAlive EXCEPT ![p] = IF \neg liveSeen[p] THEN a ELSE IF g > liveMax[p] THEN a ELSE liveMaxAlive[p]]
           /\ UNCHANGED <<discGen, discAlive, discSeen, discMax, discMaxAlive>>
        \/ /\ liveSeen[p]
           /\ g <= liveGen[p]
           /\ UNCHANGED vars

(*
  WillFires is the broker gone marker for a dead participant racing a
  live Report. Same comparison with alive FALSE. A stale will leaves
  state unchanged; a fresh will with a greater generation wins like a
  fresh report, which is why the caller must advance past it.
*)
WillFires(p, g) ==
    /\ g \in GENS
    /\ \/ /\ \/ \neg liveSeen[p]
              \/ g > liveGen[p]
           /\ liveGen' = [liveGen EXCEPT ![p] = g]
           /\ liveAlive' = [liveAlive EXCEPT ![p] = FALSE]
           /\ liveSeen' = [liveSeen EXCEPT ![p] = TRUE]
           /\ liveMax' = [liveMax EXCEPT ![p] = IF g > liveMax[p] THEN g ELSE liveMax[p]]
           /\ liveMaxAlive' = [liveMaxAlive EXCEPT ![p] = IF \neg liveSeen[p] THEN FALSE ELSE IF g > liveMax[p] THEN FALSE ELSE liveMaxAlive[p]]
           /\ UNCHANGED <<discGen, discAlive, discSeen, discMax, discMaxAlive>>
        \/ /\ liveSeen[p]
           /\ g <= liveGen[p]
           /\ UNCHANGED vars

(*
  Announce stores TRUE under greater-wins. Fresh needs first or a
  generation above the discovery max; stale or duplicate leaves the
  cache unchanged.
*)
Announce(p, g) ==
    /\ g \in GENS
    /\ \/ /\ \/ \neg discSeen[p]
              \/ g > discGen[p]
           /\ discGen' = [discGen EXCEPT ![p] = g]
           /\ discAlive' = [discAlive EXCEPT ![p] = TRUE]
           /\ discSeen' = [discSeen EXCEPT ![p] = TRUE]
           /\ discMax' = [discMax EXCEPT ![p] = IF g > discMax[p] THEN g ELSE discMax[p]]
           /\ discMaxAlive' = [discMaxAlive EXCEPT ![p] = IF \neg discSeen[p] THEN TRUE ELSE IF g > discMax[p] THEN TRUE ELSE discMaxAlive[p]]
           /\ UNCHANGED <<liveGen, liveAlive, liveSeen, liveMax, liveMaxAlive>>
        \/ /\ discSeen[p]
           /\ g <= discGen[p]
           /\ UNCHANGED vars

(*
  Retract stores FALSE under the same comparison. A stale retract
  with an older generation, or a same-generation conflict against a
  newer announce, leaves state unchanged, so it never permanently
  hides that announce.
*)
Retract(p, g) ==
    /\ g \in GENS
    /\ \/ /\ \/ \neg discSeen[p]
              \/ g > discGen[p]
           /\ discGen' = [discGen EXCEPT ![p] = g]
           /\ discAlive' = [discAlive EXCEPT ![p] = FALSE]
           /\ discSeen' = [discSeen EXCEPT ![p] = TRUE]
           /\ discMax' = [discMax EXCEPT ![p] = IF g > discMax[p] THEN g ELSE discMax[p]]
           /\ discMaxAlive' = [discMaxAlive EXCEPT ![p] = IF \neg discSeen[p] THEN FALSE ELSE IF g > discMax[p] THEN FALSE ELSE discMaxAlive[p]]
           /\ UNCHANGED <<liveGen, liveAlive, liveSeen, liveMax, liveMaxAlive>>
        \/ /\ discSeen[p]
           /\ g <= discGen[p]
           /\ UNCHANGED vars

Halt ==
    /\ \A p \in PIDS : liveMax[p] = MaxGen /\ discMax[p] = MaxGen
    /\ UNCHANGED vars

Next ==
    \/ \E p \in PIDS, g \in GENS, a \in BOOLEAN : Report(p, g, a)
    \/ \E p \in PIDS, g \in GENS : Announce(p, g)
    \/ \E p \in PIDS, g \in GENS : Retract(p, g)
    \/ \E p \in PIDS, g \in GENS : WillFires(p, g)
    \/ Halt

Fair ==
    /\ \A p \in PIDS : WF_vars(\E g \in GENS, a \in BOOLEAN : Report(p, g, a))
    /\ \A p \in PIDS : WF_vars(\E g \in GENS : Announce(p, g))
    /\ \A p \in PIDS : WF_vars(\E g \in GENS : Retract(p, g))
    /\ \A p \in PIDS : WF_vars(\E g \in GENS : WillFires(p, g))

Spec == Init /\ [][Next]_vars /\ Fair

(*
  Property (2), liveliness half: stored generation and alive always
  equal the maximum ever accepted. A stale report leaves both
  unchanged, so a smaller generation never overwrites a newer one
  and a same-generation alive flip never lands.
*)
LiveMatchesMax ==
    \A p \in PIDS : liveSeen[p] => liveGen[p] = liveMax[p] /\ liveAlive[p] = liveMaxAlive[p]

(*
  Property (2), discovery half: same guard for the endpoint cache.
  Stored generation never decreases and a stale announce or retract
  never overwrites a newer record.
*)
DiscMatchesMax ==
    \A p \in PIDS : discSeen[p] => discGen[p] = discMax[p] /\ discAlive[p] = discMaxAlive[p]

(*
  Property (4) safety: a retract never permanently hides a strictly
  newer announce. When the maximum announced generation carries alive
  TRUE, the visible cache must also be TRUE, so any older retract
  that arrives late is rejected.
*)
RetractNoHide ==
    \A p \in PIDS : discSeen[p] => (discMaxAlive[p] => discAlive[p])

(*
  Property (3) liveness: the maximum generation ever reported is
  eventually stored. Reports are local calls with no loss, so weak
  fairness on Report plus Will suffices to drive every participant to
  MaxGen. State count stays small with GENS {0, 1, 2}.
*)
NoLostUpdate == \A p \in PIDS : <>(liveGen[p] = MaxGen /\ liveMax[p] = MaxGen)

(*
  Property (4) liveness: reannounce is eventually visible. Weak
  fairness on Announce plus Retract drives every participant cache to
  MaxGen, and DiscMatchesMax ties visibility to that maximum, so a
  stale retract cannot stick.
*)
DiscEventual == \A p \in PIDS : <>(discGen[p] = MaxGen /\ discMax[p] = MaxGen)
====
