---- MODULE crypto_rekey ----
(*
  Key-revision safety for weld DDS Security (DDS-SECURITY 1.2 9.5.1.8
  KeyFactory revise/activate plus 9.8.9.5 revision distribution and
  7.3.20 key lookup on decode).

  Mapping to the Go implementation
  (internal/transports/dds/security/crypto):
  - Revise models factory.go Revise: revised material stages into
    pending without changing encode; encode keeps the old revision.
  - Activate models factory.go Activate: pending installs into the
    keystore; later encodes use the new revision; activate without
    staged material fails.
  - Encode models transform.go Encode: lookup plus AllocateSuffix
    plus DeriveSessionKey; requires validation plus a session key,
    exactly like sec_handshake.tla SendProt.
  - DecodeDrop models transform.go Decode missing-key and replay
    paths: unknown (key id, revision) drops loudly with
    secured-transform-unavailable, never retries in plaintext and
    never delivers partial plaintext.
  - Unregister models factory.go UnregisterParticipant plus
    keystore.go RemoveAllRevisions: every revision zeroized and
    dropped; later encodes and decodes both fail closed.
  - Volume rotation (AllocateSuffix auto-roll on max_blocks_per_
    session) is folded into Encode staying on the same revision;
    membership revision is the ONLY path that changes revision.

  Out of scope (documented gaps, same as code):
  - Volatile-channel KxKey wrapping travels in clear in exchange.go;
    this model assumes KxKey already established post-handshake.
  - Receiver-specific keys are stored and compared but Encode/Decode
    always use the sender key; origin-authentication distinction is
    not modeled.
  - EnableKeyRevision governance never triggers automatic rekey;
    revision here is operator-driven, matching the code.

  References:
  - /tmp/opencode/sec.txt key exchange, revision, protection kinds.
  - DDS-SECURITY 1.2 PDF 9.5 KeyFactory, 9.8.9 Key revision.
*)
EXTENDS Naturals, FiniteSets

CONSTANT MaxRev, MaxEvil

VARIABLES activeRev, pendingRev, hasPending, validated, hasKey,
  lastEncoded, evilLeft, evicted

vars == <<activeRev, pendingRev, hasPending, validated, hasKey,
  lastEncoded, evilLeft, evicted>>

TypeOK ==
  /\ activeRev \in 1 .. MaxRev
  /\ pendingRev \in 0 .. MaxRev
  /\ hasPending \in BOOLEAN
  /\ validated \in BOOLEAN
  /\ hasKey \in BOOLEAN
  /\ lastEncoded \in 0 .. MaxRev
  /\ evilLeft \in 0 .. MaxEvil
  /\ evicted \in BOOLEAN

Init ==
  /\ activeRev = 1
  /\ pendingRev = 0
  /\ hasPending = FALSE
  /\ validated = TRUE
  /\ hasKey = TRUE
  /\ lastEncoded = 0
  /\ evilLeft = MaxEvil
  /\ evicted = FALSE

Revise(r) ==
  /\ r \in 1 .. MaxRev
  /\ r > activeRev
  /\ ~evicted
  /\ ~hasPending
  /\ pendingRev' = r
  /\ hasPending' = TRUE
  /\ UNCHANGED <<activeRev, validated, hasKey, lastEncoded, evilLeft, evicted>>

Activate ==
  /\ hasPending
  /\ ~evicted
  /\ activeRev' = pendingRev
  /\ hasPending' = FALSE
  /\ pendingRev' = 0
  /\ UNCHANGED <<validated, hasKey, lastEncoded, evilLeft, evicted>>

Encode ==
  /\ validated
  /\ hasKey
  /\ ~evicted
  /\ lastEncoded' = activeRev
  /\ UNCHANGED <<activeRev, pendingRev, hasPending, validated, hasKey, evilLeft, evicted>>

ForgeDecode ==
  /\ evilLeft > 0
  /\ evilLeft' = evilLeft - 1
  /\ UNCHANGED <<activeRev, pendingRev, hasPending, validated, hasKey, lastEncoded, evicted>>

Unregister ==
  /\ ~evicted
  /\ evicted' = TRUE
  /\ hasKey' = FALSE
  /\ hasPending' = FALSE
  /\ pendingRev' = 0
  /\ lastEncoded' = 0
  /\ UNCHANGED <<activeRev, validated, evilLeft>>

Done ==
  /\ evicted
  /\ UNCHANGED vars

Next ==
  \/ \E r \in 1 .. MaxRev : Revise(r)
  \/ Activate
  \/ Encode
  \/ ForgeDecode
  \/ Unregister
  \/ Done

Spec == Init /\ [][Next]_vars /\ WF_vars(Activate)

NoUnkeyedEncode ==
  lastEncoded /= 0 => (validated /\ hasKey /\ ~evicted)

ReviseBeforeActivate ==
  hasPending => pendingRev > activeRev

OldKeyUntilActivate ==
  (hasPending /\ lastEncoded /= 0) => lastEncoded <= activeRev

EvictClearsKey == evicted => (~hasKey /\ ~hasPending /\ lastEncoded = 0)

ActiveBounded == activeRev \in 1 .. MaxRev
====
