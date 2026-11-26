---- MODULE router ----
(*
  Router.TakeBytes multiplex over R backends (R=2 in router_1.cfg,
  R=3 in router_2.cfg). One Router.TakeBytes caller races all backends.

  Mapping to internal/transports/router.go:
  - Stage(b, s) models a sample arriving on backend b (bounded FIFO,
    Depth slots). A sample is staged at most once.
  - RaceStart models TakeBytes (router.go:378) passing the ctx check
    (router.go:382), finding pending empty (drainPending miss,
    router.go:403), and snapshotting open backends (router.go:407).
  - ResolveOne(b) models backend b winning the race: its TakeBytes
    returns Head(q[b]), consumed into race-local extras (raceTakes at
    router.go:285, late arrivals drained by drainRaceResults at
    router.go:362).
  - ResolveErr(b) models a backend resolving with error: closed,
    empty, or post-cancel child ctx (the fake in router_test.go checks
    ctx first at router_test.go:75 and consumes nothing).
  - RaceFinishOk(w) models the race returning the first success w
    immediately, without waiting for quiet backends (router.go: early
    return in raceTakes). Remaining backends resolve late through
    LateOk/LateErr into pending, mirroring drainRaceResults.
  - RaceFinishErr models the all-error race (lastErr).
  - RaceFinishCancel models ctx firing mid-race: arrived extras are
    buffered into pending (abortRace harvest), stragglers drain late
    the same way, and the error return carries the context cause.
    Nothing consumed is dropped.
  - LateOk(b)/LateErr(b) model straggler resolution after the winner
    returned (or after abort): a late success buffers into pending, a
    late error just marks the backend resolved. Guarded to non-racing
    periods; overlapping stragglers across back-to-back takes are
    abstracted as settling between races (safe: pending appends are
    atomic and race-local extras never alias).
  - Drain(s) models drainPending serving a buffered extra ahead of any
    new race (router.go:403).
  - TakeEmptyErr models the nothing-ready race returning an error
    without consuming (also covers takes after a backend Close).
  - TakeErrCancelled models the pre-race ctx check (router.go:382):
    error return, no consumption.
  - Cancel models ctx cancellation (sticky, at most once).
  - Close(c) models Close of one backend mid-race (at most one close
    per behavior; pending samples stay deliverable since drainPending
    never consults backend state).

  Abstractions (sound for exactly-once, unsuitable for ordering):
  - pending is a set, not a FIFO: delivery order among simultaneous
    winners is generalized. Safety (no dup, conservation) over all
    orders implies safety for the true arrival order.
  - Empty backends resolve with error immediately (test-fake semantics,
    router_test.go:103). Truly blocking backends simply never win; the
    first success returns at once and stragglers drain late.
  - Real backends consume on success (transport.go TakeBytes contract);
    the test fake returns takeMsgs[0] without popping (router_test.go:107),
    which the model does NOT follow.

  Proven green (see cfgs): TypeOK, Conservation, AtMostOnce,
  NoDrops, CancelFrozen, RaceClean, CancelLeadsToErr, EventualSettle.

  Closed divergences (fixed in code, proven here): D1 mid-race cancel
  used to drop consumed extras; abortRace now harvests arrivals and
  parks stragglers, so NoDrops holds. D2 quiet-backend stall is gone:
  the first win returns immediately while stragglers drain late.
*)
EXTENDS Naturals, FiniteSets, Sequences, TLC

CONSTANTS Backends, Samples, Depth

ASSUME Cardinality(Backends) >= 1
ASSUME Depth >= 1

VARIABLES q, pending, staged, taken, cancelled, closed,
    racing, resolved, extras, lastRet, dropped, snap

vars == <<q, pending, staged, taken, cancelled, closed,
    racing, resolved, extras, lastRet, dropped, snap>>

SeqToSet(sq) == {sq[i] : i \in 1..Len(sq)}

TypeOK ==
    /\ \A b \in Backends : Len(q[b]) <= Depth /\ SeqToSet(q[b]) \subseteq Samples
    /\ pending \subseteq Samples
    /\ extras \subseteq Samples
    /\ dropped \subseteq Samples
    /\ staged \subseteq Samples
    /\ taken \in [Samples -> 0 .. 1]
    /\ snap \in [Samples -> 0 .. 1]
    /\ cancelled \in BOOLEAN
    /\ racing \in BOOLEAN
    /\ closed \subseteq Backends
    /\ resolved \subseteq Backends
    /\ lastRet \in {"none", "ok", "err"}

Init ==
    /\ q = [b \in Backends |-> <<>>]
    /\ pending = {}
    /\ extras = {}
    /\ dropped = {}
    /\ staged = {}
    /\ taken = [s \in Samples |-> 0]
    /\ snap = [s \in Samples |-> 0]
    /\ cancelled = FALSE
    /\ racing = FALSE
    /\ closed = {}
    /\ resolved = {}
    /\ lastRet = "none"

Stage(b, s) ==
    /\ b \in Backends \ closed
    /\ s \in Samples \ staged
    /\ Len(q[b]) < Depth
    /\ q' = [q EXCEPT ![b] = Append(@, s)]
    /\ staged' = staged \cup {s}
    /\ UNCHANGED <<pending, taken, cancelled, closed, racing,
        resolved, extras, lastRet, dropped, snap>>

RaceStart ==
    /\ ~racing
    /\ ~cancelled
    /\ pending = {}
    /\ \E b \in Backends \ closed : q[b] /= <<>>
    /\ racing' = TRUE
    /\ resolved' = {}
    /\ UNCHANGED <<q, pending, staged, taken, cancelled, closed,
        extras, lastRet, dropped, snap>>

ResolveOne(b) ==
    /\ racing
    /\ ~cancelled
    /\ b \in Backends \ resolved
    /\ b \notin closed
    /\ q[b] /= <<>>
    /\ q' = [q EXCEPT ![b] = Tail(@)]
    /\ extras' = extras \cup {Head(q[b])}
    /\ resolved' = resolved \cup {b}
    /\ UNCHANGED <<pending, staged, taken, cancelled, closed, racing,
        lastRet, dropped, snap>>

ResolveErr(b) ==
    /\ racing
    /\ b \in Backends \ resolved
    /\ (cancelled \/ b \in closed \/ q[b] = <<>>)
    /\ resolved' = resolved \cup {b}
    /\ UNCHANGED <<q, pending, staged, taken, cancelled, closed, racing,
        extras, lastRet, dropped, snap>>

RaceFinishOk(w) ==
    /\ racing
    /\ ~cancelled
    /\ w \in extras
    /\ taken' = [taken EXCEPT ![w] = @ + 1]
    /\ pending' = pending \cup (extras \ {w})
    /\ racing' = FALSE
    /\ resolved' = resolved
    /\ extras' = {}
    /\ lastRet' = "ok"
    /\ UNCHANGED <<q, staged, cancelled, closed, dropped, snap>>

LateOk(b) ==
    /\ ~racing
    /\ b \in Backends \ resolved
    /\ b \notin closed
    /\ q[b] /= <<>>
    /\ q' = [q EXCEPT ![b] = Tail(@)]
    /\ pending' = pending \cup {Head(q[b])}
    /\ resolved' = resolved \cup {b}
    /\ UNCHANGED <<staged, taken, cancelled, closed, racing, resolved,
        extras, lastRet, dropped, snap>>

LateErr(b) ==
    /\ ~racing
    /\ b \in Backends \ resolved
    /\ (cancelled \/ b \in closed \/ q[b] = <<>>)
    /\ resolved' = resolved \cup {b}
    /\ UNCHANGED <<q, pending, staged, taken, cancelled, closed, racing,
        resolved, extras, lastRet, dropped, snap>>

RaceFinishErr ==
    /\ racing
    /\ ~cancelled
    /\ resolved = Backends
    /\ extras = {}
    /\ racing' = FALSE
    /\ resolved' = {}
    /\ lastRet' = "err"
    /\ UNCHANGED <<q, pending, staged, taken, cancelled, closed,
        extras, dropped, snap>>

RaceFinishCancel ==
    /\ racing
    /\ cancelled
    /\ pending' = pending \cup extras
    /\ extras' = {}
    /\ racing' = FALSE
    /\ resolved' = resolved
    /\ lastRet' = "err"
    /\ UNCHANGED <<q, staged, taken, cancelled, closed, dropped, snap>>

Drain(s) ==
    /\ ~racing
    /\ ~cancelled
    /\ s \in pending
    /\ pending' = pending \ {s}
    /\ taken' = [taken EXCEPT ![s] = @ + 1]
    /\ lastRet' = "ok"
    /\ UNCHANGED <<q, staged, cancelled, closed, racing, resolved,
        extras, dropped, snap>>

TakeEmptyErr ==
    /\ ~racing
    /\ ~cancelled
    /\ pending = {}
    /\ \A b \in Backends \ closed : q[b] = <<>>
    /\ lastRet' = "err"
    /\ UNCHANGED <<q, pending, staged, taken, cancelled, closed, racing,
        resolved, extras, dropped, snap>>

TakeErrCancelled ==
    /\ ~racing
    /\ cancelled
    /\ lastRet' = "err"
    /\ UNCHANGED <<q, pending, staged, taken, cancelled, closed, racing,
        resolved, extras, dropped, snap>>

Cancel ==
    /\ ~cancelled
    /\ cancelled' = TRUE
    /\ snap' = taken
    /\ UNCHANGED <<q, pending, staged, taken, closed, racing, resolved,
        extras, lastRet, dropped>>

Close(c) ==
    /\ closed = {}
    /\ c \in Backends
    /\ closed' = {c}
    /\ UNCHANGED <<q, pending, staged, taken, cancelled, racing, resolved,
        extras, lastRet, dropped, snap>>

Next ==
    \/ \E b \in Backends, s \in Samples : Stage(b, s)
    \/ RaceStart
    \/ \E b \in Backends : ResolveOne(b)
    \/ \E b \in Backends : ResolveErr(b)
    \/ \E w \in Samples : RaceFinishOk(w)
    \/ RaceFinishErr
    \/ RaceFinishCancel
    \/ \E b \in Backends : LateOk(b)
    \/ \E b \in Backends : LateErr(b)
    \/ \E s \in Samples : Drain(s)
    \/ TakeEmptyErr
    \/ TakeErrCancelled
    \/ Cancel
    \/ \E c \in Backends : Close(c)

Fair ==
    /\ \A b \in Backends, s \in Samples : WF_vars(Stage(b, s))
    /\ WF_vars(RaceStart)
    /\ \A b \in Backends : WF_vars(ResolveOne(b))
    /\ \A b \in Backends : WF_vars(ResolveErr(b))
    /\ \A w \in Samples : WF_vars(RaceFinishOk(w))
    /\ WF_vars(RaceFinishErr)
    /\ WF_vars(RaceFinishCancel)
    /\ \A b \in Backends : WF_vars(LateOk(b))
    /\ \A b \in Backends : WF_vars(LateErr(b))
    /\ \A s \in Samples : WF_vars(Drain(s))
    /\ WF_vars(TakeErrCancelled)

Spec == Init /\ [][Next]_vars /\ Fair

AtMostOnce == \A s \in Samples : taken[s] <= 1

LocCount(s) ==
    Cardinality({b \in Backends : s \in SeqToSet(q[b])})
    + (IF s \in pending THEN 1 ELSE 0)
    + (IF s \in extras THEN 1 ELSE 0)
    + taken[s]
    + (IF s \in dropped THEN 1 ELSE 0)

Conservation ==
    \A s \in Samples : LocCount(s) = (IF s \in staged THEN 1 ELSE 0)

CancelFrozen == cancelled => taken = snap

RaceClean == racing => pending = {}

NoDrops == dropped = {}

Stranded(s) == \E c \in closed : s \in SeqToSet(q[c])

Settled == \A s \in Samples : taken[s] = 1 \/ Stranded(s)

CancelLeadsToErr == cancelled ~> (lastRet = "err")

EventualSettle == <>(cancelled \/ Settled)
====
