---- MODULE xtypes_consistency ----
(*
  XTypes type-consistency safety for weld DDS (DDS-XTypes 1.4 7.6.3
  assignability plus 7.3.4 TypeIdentifier equality, as pinned to the
  ROS RIHS01 hash used on the wire).

  Mapping to the Go implementation:
  - ComputeTypeHash models internal/types/hash.go ComputeTypeHash:
    SHA-256 over CanonicalJSON with defaults omitted and referenced
    descriptions sorted, so field order and default values never
    affect the digest.
  - ParseTypeHash/String models hash.go ParseTypeHash: only version 1
    with exact length parses; all other lengths and versions reject.
  - LookupWithHash models internal/types/registry.go LookupWithHash:
    exact Equal match only, no coercion between different hashes.
  - SEDPEndpointsMatch type gate models sedp.go sedpMatchTypeHash:
    endpoints match only on equal type hashes carried in USER_DATA;
    TypeObject bytes stay opaque (sedp.go:323) and never decide.
  - Extensibility models internal/types/extensible.go: FINAL needs
    exact shape, APPENDABLE and MUTABLE framing exists but the plain
    CDR encode path is XCDR1 FINAL only, so non-FINAL never claims
    zero-copy loan safety.

  Out of scope (explicit non-goals, same as code):
  - TypeIdentifier/EquivalenceHash (MD5 over marshalled TypeObject),
    Minimal/Complete TypeObject body parsing, TypeConsistencyEnforcement
    QoS is_assignable rules, and TypeLookup service: all rejected as
    non-goals in typeobject.go, weld_cdr.h, sedp.go, rmw_weld README.
  - XCDR2 emit, optional members, and mutable/appendable evolution:
    helpers exist (xcdr2.go, extensible.go) but plain encode stays
    XCDR1 FINAL.

  References:
  - /tmp/opencode/xtypes.txt TypeSystem, TypeObject, assignability.
  - DDS-XTypes 1.4 PDF 7.3 TypeIdentifiers, 7.6 Assignability.
*)
EXTENDS Naturals, FiniteSets

CONSTANT Hashes, MaxMembers

VARIABLES wHash, rHash, wExt, rExt, wMembers, rMembers, matched

vars == <<wHash, rHash, wExt, rExt, wMembers, rMembers, matched>>

TypeOK ==
  /\ wHash \in Hashes
  /\ rHash \in Hashes
  /\ wExt \in {"FINAL", "APPENDABLE", "MUTABLE"}
  /\ rExt \in {"FINAL", "APPENDABLE", "MUTABLE"}
  /\ wMembers \in 1 .. MaxMembers
  /\ rMembers \in 1 .. MaxMembers
  /\ matched \in BOOLEAN

(*
  Weld rule (exact-hash gate): endpoints match only when the RIHS01
  hashes are equal. Extensibility never widens a mismatched hash to
  a match; it only records whether the loan fast path is allowed
  (self-contained FINAL only). This is narrower than full XTypes
  assignability on purpose: no coercion without equal hashes.
*)
WeldAssignable ==
  /\ wHash = rHash
  /\ (wExt = "FINAL" /\ rExt = "FINAL" => wMembers = rMembers)

LoanSafe == (wExt = "FINAL" /\ rExt = "FINAL" /\ wMembers = rMembers)

Init ==
  /\ wHash \in Hashes /\ rHash \in Hashes
  /\ wExt = "FINAL" /\ rExt = "FINAL"
  /\ wMembers = 1 /\ rMembers = 1
  /\ matched = FALSE

SetWriter(h, e, m) ==
  /\ h \in Hashes /\ e \in {"FINAL", "APPENDABLE", "MUTABLE"} /\ m \in 1 .. MaxMembers
  /\ wHash' = h /\ wExt' = e /\ wMembers' = m
  /\ matched' = FALSE
  /\ UNCHANGED <<rHash, rExt, rMembers>>

SetReader(h, e, m) ==
  /\ h \in Hashes /\ e \in {"FINAL", "APPENDABLE", "MUTABLE"} /\ m \in 1 .. MaxMembers
  /\ rHash' = h /\ rExt' = e /\ rMembers' = m
  /\ matched' = FALSE
  /\ UNCHANGED <<wHash, wExt, wMembers>>

TryMatch ==
  /\ matched' = WeldAssignable
  /\ UNCHANGED <<wHash, rHash, wExt, rExt, wMembers, rMembers>>

Next ==
  \/ \E h \in Hashes, e \in {"FINAL", "APPENDABLE", "MUTABLE"}, m \in 1 .. MaxMembers :
       SetWriter(h, e, m)
  \/ \E h \in Hashes, e \in {"FINAL", "APPENDABLE", "MUTABLE"}, m \in 1 .. MaxMembers :
       SetReader(h, e, m)
  \/ TryMatch
  \/ UNCHANGED vars

Spec == Init /\ [][Next]_vars /\ WF_vars(TryMatch)

NoHashMismatchMatch == matched => wHash = rHash

FinalExact == (matched /\ wExt = "FINAL" /\ rExt = "FINAL") => wMembers = rMembers

NoSilentCoercion == (wHash /= rHash) => ~matched

LoanNeedsFinalExact == matched => (LoanSafe => wMembers = rMembers)
====
