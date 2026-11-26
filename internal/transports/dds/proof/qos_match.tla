---- MODULE qos_match ----
(*
  RxO compatibility for weld DDS (DDS 1.4 section 2.2.3 QoS Requested vs
  Offered compatibility, plus 2.2.3 Deadline/Liveliness/Lease rules).

  Mapping to the Go implementation:
  - Compatible models internal/core/matcher.go CheckCompatibility:
    checkReliability (matcher.go:320), checkDurability (:331),
    checkDeadline (:351), checkLiveliness (:377), checkLease (:395),
    plus checkNegativeDurations (:291) fail-closed on malformed
    durations. History/Depth/Lifespan/Avoidance are RxO-neutral by
    design (qos_types.go) and absent here.
  - Offer/Request model writerqos.go reverseRxOWithPolicy plus
    CheckWireCompatibility: only reliability, durability, deadline,
    liveliness, lease cross the wire; transient/persistent and
    manual-by-participant refuse at reverse (writerqos.go:885,903)
    and never reach this predicate.
  - SetOffer/SetRequest model SEDP endpoint announcements carrying
    per-peer QoS (sedp.go sedpEndpointFields); TryMatch models the
    matcher gate that must run before installing a match.

  Out of scope (documented gaps, code refuses or ignores):
  - Ownership, Presentation, LatencyBudget, DestinationOrder,
    TimeBasedFilter, ResourceLimits have no QoSProfile field and no
    matcher branch; SEDP SEDPEndpointsMatch never consults the
    matcher (audit gap 1).
  - Durability truncated to VOLATILE/TRANSIENT_LOCAL; TRANSIENT and
    PERSISTENT refuse at reverse instead of VOL<TL<T<P ordering.
  - Liveliness truncated to AUTOMATIC/MANUAL_BY_TOPIC; MANUAL_BY_
    PARTICIPANT refuses at reverse instead of AUTO<MAN_P<MAN_T.
  - Reliability MaxBlockingTime validated but dropped before match.
  - DurabilityService compat (durability.go:650) exists but is unwired.
  - Partition is not RxO: it gates via MatchPartitions, modeled in
    discovery.tla, not here.

  References (local spec dumps):
  - /tmp/opencode/dds14.txt sections on Requested/Offered compat.
  - DDS 1.4 PDF 2.2.3 (offered vs requested), Table QoS policies.
*)
EXTENDS Naturals, FiniteSets

VARIABLES offRel, reqRel, offDur, reqDur, offLive, reqLive,
  offDead, reqDead, offLease, reqLease, malformed, matched

vars == <<offRel, reqRel, offDur, reqDur, offLive, reqLive,
  offDead, reqDead, offLease, reqLease, malformed, matched>>

TypeOK ==
  /\ offRel \in {0, 1}
  /\ reqRel \in {0, 1}
  /\ offDur \in {0, 1}
  /\ reqDur \in {0, 1}
  /\ offLive \in {0, 2}
  /\ reqLive \in {0, 2}
  /\ offDead \in {0, 1, 2}
  /\ reqDead \in {0, 1, 2}
  /\ offLease \in {0, 1, 2}
  /\ reqLease \in {0, 1, 2}
  /\ malformed \in BOOLEAN
  /\ matched \in BOOLEAN

(*
  Deadline/lease encoding: 0 = unspecified/default, 1 = finite fast,
  2 = finite slow, with 1 < 2 (1 is faster/shorter). Infinite is
  folded into 0 (no constraint) per matcher.go:355,398: requested
  unspecified/infinite always flows. Offered 0 with finite request
  fails (matcher.go:359, offered default cannot satisfy).
*)
DeadlineOK(off, req) ==
  \/ req = 0
  \/ off = 0 /\ req = 0
  \/ off /= 0 /\ req /= 0 /\ req >= off

ReliabilityOK == ~(offRel = 0 /\ reqRel = 1)
DurabilityOK == ~(offDur = 0 /\ reqDur = 1)
LivelinessOK == ~(offLive = 0 /\ reqLive = 2)
DeadlineCompat == DeadlineOK(offDead, reqDead)
LeaseCompat == DeadlineOK(offLease, reqLease)

Compatible ==
  /\ ~malformed
  /\ ReliabilityOK
  /\ DurabilityOK
  /\ LivelinessOK
  /\ DeadlineCompat
  /\ LeaseCompat

Init ==
  /\ offRel = 1 /\ reqRel = 1
  /\ offDur = 0 /\ reqDur = 0
  /\ offLive = 0 /\ reqLive = 0
  /\ offDead = 0 /\ reqDead = 0
  /\ offLease = 0 /\ reqLease = 0
  /\ malformed = FALSE
  /\ matched = FALSE

SetOffer(oR, oD, oL, oDD, oLL) ==
  /\ offRel' = oR /\ offDur' = oD /\ offLive' = oL
  /\ offDead' = oDD /\ offLease' = oLL
  /\ matched' = FALSE
  /\ UNCHANGED <<reqRel, reqDur, reqLive, reqDead, reqLease, malformed>>

SetRequest(rR, rD, rL, rDD, rLL) ==
  /\ reqRel' = rR /\ reqDur' = rD /\ reqLive' = rL
  /\ reqDead' = rDD /\ reqLease' = rLL
  /\ matched' = FALSE
  /\ UNCHANGED <<offRel, offDur, offLive, offDead, offLease, malformed>>

InjectMalformed ==
  /\ malformed' = TRUE
  /\ matched' = FALSE
  /\ UNCHANGED <<offRel, reqRel, offDur, reqDur, offLive, reqLive,
    offDead, reqDead, offLease, reqLease>>

ClearMalformed ==
  /\ malformed' = FALSE
  /\ matched' = FALSE
  /\ UNCHANGED <<offRel, reqRel, offDur, reqDur, offLive, reqLive,
    offDead, reqDead, offLease, reqLease>>

TryMatch ==
  /\ matched' = Compatible
  /\ UNCHANGED <<offRel, reqRel, offDur, reqDur, offLive, reqLive,
    offDead, reqDead, offLease, reqLease, malformed>>

Next ==
  \/ \E oR \in {0,1}, oD \in {0,1}, oL \in {0,2}, oDD \in {0,1,2}, oLL \in {0,1,2} :
       SetOffer(oR, oD, oL, oDD, oLL)
  \/ \E rR \in {0,1}, rD \in {0,1}, rL \in {0,2}, rDD \in {0,1,2}, rLL \in {0,1,2} :
       SetRequest(rR, rD, rL, rDD, rLL)
  \/ InjectMalformed
  \/ ClearMalformed
  \/ TryMatch
  \/ UNCHANGED vars

Spec == Init /\ [][Next]_vars /\ WF_vars(TryMatch)

NoBadMatch == matched => Compatible

FailClosedMalformed == malformed => ~Compatible

ReliabilitySound == (offRel = 0 /\ reqRel = 1) => ~Compatible
DurabilitySound == (offDur = 0 /\ reqDur = 1) => ~Compatible
LivelinessSound == (offLive = 0 /\ reqLive = 2) => ~Compatible
DeadlineSound == (offDead = 0 /\ reqDead /= 0) => ~Compatible
LeaseSound == (offLease = 0 /\ reqLease /= 0) => ~Compatible
DeadlineRateSound == (offDead = 2 /\ reqDead = 1) => ~Compatible
LeaseRateSound == (offLease = 2 /\ reqLease = 1) => ~Compatible

StrongOfferFlows ==
  (offRel = 1 /\ reqRel = 1 /\ offDur = 1 /\ reqDur = 1 /\
   offLive = 2 /\ reqLive = 2 /\ offDead = 1 /\ reqDead = 2 /\
   offLease = 1 /\ reqLease = 2 /\ ~malformed) => Compatible

NoConstraintFlows ==
  (reqDead = 0 /\ reqLease = 0 /\ ~malformed /\
   ReliabilityOK /\ DurabilityOK /\ LivelinessOK) => Compatible
====
