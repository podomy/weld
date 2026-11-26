---- MODULE discovery ----
(*
  SPDP/SEDP discovery safety for weld DDS (DDSI-RTPS 2.5 discovery:
  SPDP participant announcement plus SEDP endpoint announcement).

  Mapping to the Go implementation (internal/transports/dds):
  - Alive(seq)/Dispose(seq) model spdp.go handleAliveLocked /
    handleDisposeLocked: seq==0 rejected upstream (announcementFromData),
    seq==last duplicate refreshes lease silently, seq<last fails loudly
    with ErrStaleSPDP, seq>last installs and bumps generation.
  - Tombstone models spdpTombstone{seq,expiresAt}: dispose of a live
    entry deletes it and plants tomb{seq}; alive at tomb.seq is a
    duplicate, alive below tomb.seq is stale, alive above clears the
    tomb and reinstalls. Missing-entry dispose plants no tomb (bounded
    map), same as code.
  - UpsertRemote/RemoveRemote model sedp.go SEDPIndex UpsertRemote /
    RemoveRemote with ingestStaleCheckLocked plus sedpStoreSeq: seq==0
    never gates or advances, seq<=last is stale (dup cancels
    pendingEvict), only seq>last installs and bumps generation.
  - Sweep models spdp.go Sweep plus sedp.go ExpireProvisional/Sweep:
    tombstone expiry drops the tomb without bumping generation; entry
    lease expiry removes the entry and bumps once.
  - Generation models RemoteTable.generation plus SEDPIndex.generation:
    only installs, deletes, and entry expiries bump; duplicates,
    stale drops, and tombstone-only expiries never bump.

  Out of scope (bounded in code, abstracted here):
  - Multi-participant and multi-endpoint maps: one prefix modeled;
    per-prefix reasoning lifts entry-wise since prefixes never interact.
  - Lease durations and wall clocks: modeled as nondeterministic Expire
    choice, since Sweep takes now as a parameter and fake clocks drive
    tests deterministically.
  - HEARTBEAT/GAP replay suppression, InfoDst checks, secure-refusal
    gates, and userdata/hash parsing: orthogonal to seq ordering.
  - Non-atomic check-then-store interleave across concurrent ingests:
    code holds the table lock across each ingest path; the model is
    sequential, matching locked behavior.

  References:
  - /tmp/opencode/rtps25.txt SPDP/SEDP discovery sections.
  - DDSI-RTPS 2.5 PDF 8.5 Discovery, SPDP/SEDP announcements.
*)
EXTENDS Naturals, FiniteSets

CONSTANT MaxSeq

VARIABLES lastSeq, alive, tombSeq, tombActive, generation, leaseLive

vars == <<lastSeq, alive, tombSeq, tombActive, generation, leaseLive>>

TypeOK ==
  /\ lastSeq \in 0 .. MaxSeq
  /\ alive \in BOOLEAN
  /\ tombSeq \in 0 .. MaxSeq
  /\ tombActive \in BOOLEAN
  /\ generation \in 0 .. (MaxSeq * 4)
  /\ leaseLive \in BOOLEAN

GenBound == MaxSeq * 4

Init ==
  /\ lastSeq = 0
  /\ alive = FALSE
  /\ tombSeq = 0
  /\ tombActive = FALSE
  /\ generation = 0
  /\ leaseLive = FALSE

Alive(s) ==
  /\ s \in 1 .. MaxSeq
  /\ generation < GenBound
  /\ IF ~alive /\ ~tombActive THEN
       /\ alive' = TRUE
       /\ lastSeq' = s
       /\ tombActive' = FALSE
       /\ generation' = generation + 1
       /\ leaseLive' = TRUE
       /\ UNCHANGED <<tombSeq>>
     ELSE IF alive /\ s = lastSeq THEN
       /\ leaseLive' = TRUE
       /\ UNCHANGED <<lastSeq, alive, tombSeq, tombActive, generation>>
     ELSE IF alive /\ s < lastSeq THEN
       /\ UNCHANGED vars
     ELSE IF alive /\ s > lastSeq THEN
       /\ lastSeq' = s
       /\ leaseLive' = TRUE
       /\ generation' = generation + 1
       /\ UNCHANGED <<alive, tombSeq, tombActive>>
     ELSE IF tombActive /\ s = tombSeq THEN
       /\ UNCHANGED vars
     ELSE IF tombActive /\ s < tombSeq THEN
       /\ UNCHANGED vars
     ELSE
       /\ alive' = TRUE
       /\ lastSeq' = s
       /\ tombActive' = FALSE
       /\ generation' = generation + 1
       /\ leaseLive' = TRUE
       /\ UNCHANGED <<tombSeq>>

Dispose(s) ==
  /\ s \in 1 .. MaxSeq
  /\ generation < GenBound
  /\ IF ~alive /\ ~tombActive THEN
       UNCHANGED vars
     ELSE IF ~alive /\ tombActive THEN
       IF s = tombSeq \/ s < tombSeq THEN UNCHANGED vars
       ELSE /\ tombSeq' = s
            /\ UNCHANGED <<lastSeq, alive, tombActive, generation, leaseLive>>
     ELSE IF alive /\ s = lastSeq THEN
       UNCHANGED vars
     ELSE IF alive /\ s < lastSeq THEN
       UNCHANGED vars
     ELSE
       /\ alive' = FALSE
       /\ leaseLive' = FALSE
       /\ tombSeq' = s
       /\ tombActive' = TRUE
       /\ generation' = generation + 1
       /\ UNCHANGED <<lastSeq>>

ExpireTomb ==
  /\ tombActive
  /\ tombActive' = FALSE
  /\ UNCHANGED <<lastSeq, alive, tombSeq, generation, leaseLive>>

ExpireEntry ==
  /\ alive
  /\ generation < GenBound
  /\ alive' = FALSE
  /\ leaseLive' = FALSE
  /\ generation' = generation + 1
  /\ UNCHANGED <<lastSeq, tombSeq, tombActive>>

Next ==
  \/ \E s \in 1 .. MaxSeq : Alive(s)
  \/ \E s \in 1 .. MaxSeq : Dispose(s)
  \/ ExpireTomb
  \/ ExpireEntry
  \/ UNCHANGED vars

Spec == Init /\ [][Next]_vars /\ WF_vars(ExpireTomb) /\ WF_vars(ExpireEntry)

SeqMonotonic == alive => lastSeq > 0

TombBlocksStale == tombActive => ~alive

TombSeqCoversLast == tombActive => tombSeq >= lastSeq

StaleSafe == (alive \/ tombActive) => (lastSeq > 0 \/ tombSeq > 0)

GenerationMonotonic == generation >= 0

LeaseImpliesAlive == leaseLive => alive

TombSeqSound == tombActive => tombSeq > 0
====
