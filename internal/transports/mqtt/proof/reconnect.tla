---- MODULE reconnect ----
(*
  MQTT reconnect lifecycle for weld: one read loop plus reconnect
  attempts plus Close plus ping/watchdog as abstract events.

  Go sources (read-only): netconn.go (Close, Publish paths),
  netio.go (readLoop/readOne/handleReadError, pingLoop/watchdogFired),
  reconnect.go (reconnect/tryReconnect/resume check-then-swap,
  currentConn).

  Abstraction: reconMu serializes whole reconnect attempts against
  Close; inside an attempt, resume holds writeMu+mu across the
  closed-check plus socket swap, while Close holds writeMu only for
  the goodbye write and closes the socket outside any lock. Mutual
  exclusion of swap-vs-close is by reconMu (Close waits out an
  in-progress resume; a waiting reconnect sees the closed flag),
  not by a shared writeMu section. Only the read loop calls
  reconnect, so attempts never overlap; the model has one pending
  slot (backoff/resuming).

  State: mode in {"reading","backoff","resuming","closed"} is the reader
  mode; epoch is the socket epoch bumped on each successful swap;
  closed is the closed flag; needRecon is the pending-reconnect flag.
  Ghosts: lossLeft bounds redial failures (finite budget like qos2
  MaxLoss); silent marks a half-open link with no inbound packets;
  epochAtClose snapshots epoch at Close to prove no zombie swap;
  seen records every live epoch to prove strict increase and no reuse.
*)
EXTENDS Naturals, FiniteSets, TLC

CONSTANTS MaxLoss, MaxEpoch

ASSUME MaxLoss \in Nat /\ MaxEpoch \in Nat /\ MaxLoss >= 1 /\ MaxEpoch >= 1

VARIABLES mode, epoch, closed, needRecon, lossLeft, silent, epochAtClose, seen

vars == <<mode, epoch, closed, needRecon, lossLeft, silent, epochAtClose, seen>>

TypeOK ==
    /\ mode \in {"reading", "backoff", "resuming", "closed"}
    /\ epoch \in 0 .. MaxEpoch
    /\ closed \in BOOLEAN
    /\ needRecon \in BOOLEAN
    /\ lossLeft \in 0 .. MaxLoss
    /\ silent \in BOOLEAN
    /\ epochAtClose \in 0 .. MaxEpoch
    /\ seen \subseteq 0 .. MaxEpoch

Init ==
    /\ mode = "reading"
    /\ epoch = 0
    /\ closed = FALSE
    /\ needRecon = FALSE
    /\ lossLeft = MaxLoss
    /\ silent = FALSE
    /\ epochAtClose = 0
    /\ seen = {0}

(*
  Environment fault: the peer goes half-open. No inbound packet arrives,
  so noteRecv stops advancing and the watchdog must fire. Analogous to
  Drop in qos2.tla. Bounded by MaxEpoch so the checked redial fits.
*)
LinkGoesSilent ==
    /\ mode = "reading"
    /\ ~closed
    /\ ~silent
    /\ epoch < MaxEpoch
    /\ silent' = TRUE
    /\ UNCHANGED <<mode, epoch, closed, needRecon, lossLeft, epochAtClose, seen>>

(* readLoop/readOne success: one packet dispatched, noteRecv stamps. *)
ReadOk ==
    /\ mode = "reading"
    /\ ~closed
    /\ ~silent
    /\ silent' = FALSE
    /\ UNCHANGED <<mode, epoch, closed, needRecon, lossLeft, epochAtClose, seen>>

(* readPacket fails: handleReadError broadcasts and enters reconnect. *)
ReadFails ==
    /\ mode = "reading"
    /\ ~closed
    /\ mode' = "backoff"
    /\ needRecon' = TRUE
    /\ UNCHANGED <<epoch, closed, lossLeft, silent, epochAtClose, seen>>

(*
  Broker DISCONNECT: handleDisconnect reconnects like a socket drop
  but without a read error. Same effect as ReadFails; no stale-reader
  identity check applies since only the read loop drives it.
*)
Disconnected ==
    /\ mode = "reading"
    /\ ~closed
    /\ mode' = "backoff"
    /\ needRecon' = TRUE
    /\ UNCHANGED <<epoch, closed, lossLeft, silent, epochAtClose, seen>>

(*
  pingLoop probe fails: per netio.go the loop survives. The watchdog or
  the read loop owns recovery, so this step changes nothing.
*)
PingWriteFails ==
    /\ mode \in {"reading", "backoff", "resuming"}
    /\ ~closed
    /\ UNCHANGED vars

(*
  watchdogFired: silent link trips the watchdog, the socket is closed so
  the read loop redials instead of stalling. Forces the reconnect path;
  silent stays TRUE until a fresh socket stamps noteRecv.
*)
WatchdogFires ==
    /\ mode = "reading"
    /\ ~closed
    /\ silent
    /\ mode' = "resuming"
    /\ needRecon' = TRUE
    /\ UNCHANGED <<epoch, closed, lossLeft, silent, epochAtClose, seen>>

(*
  tryReconnect+resume success: dial plus handshake plus resubscribe plus
  republish abstracted as one atomic step. The closed check and the
  socket swap hold writeMu+mu together while Close is excluded by
  reconMu, so this single action checks ~closed then installs the new
  epoch (epoch+1). Clears silent via noteRecv and wakes receivers.
*)
ReconnectSucceeds ==
    /\ mode \in {"backoff", "resuming"}
    /\ ~closed
    /\ needRecon
    /\ epoch < MaxEpoch
    /\ mode' = "reading"
    /\ epoch' = epoch + 1
    /\ needRecon' = FALSE
    /\ silent' = FALSE
    /\ seen' = seen \cup {epoch + 1}
    /\ UNCHANGED <<closed, lossLeft, epochAtClose>>

(*
  One redial round fails (dial/handshake/resume). Consumes the finite
  loss budget, so only finitely many failures precede success. Stays
  pending; toggles backoff/resuming so both pending modes are covered.
*)
ReconnectFails ==
    /\ mode \in {"backoff", "resuming"}
    /\ ~closed
    /\ needRecon
    /\ lossLeft > 0
    /\ lossLeft' = lossLeft - 1
    /\ mode' = IF mode = "backoff" THEN "resuming" ELSE "backoff"
    /\ UNCHANGED <<epoch, closed, needRecon, silent, epochAtClose, seen>>

(*
  netconn.go Close: sets the closed flag and snapshots the epoch under
  mu, then takes reconMu so an in-progress resume finishes first. A
  waiting reconnect then sees the closed flag; an in-progress resume
  sees it at its writeMu+mu check and discards the fresh socket.
  Leaves mode for ReaderHalts to observe, like handleReadError
  seeing snapshotClosed.
*)
Close ==
    /\ ~closed
    /\ closed' = TRUE
    /\ epochAtClose' = epoch
    /\ UNCHANGED <<mode, epoch, needRecon, lossLeft, silent, seen>>

(* Read loop observes closed and ends, like reconnect returning FALSE. *)
ReaderHalts ==
    /\ closed
    /\ mode /= "closed"
    /\ mode' = "closed"
    /\ needRecon' = FALSE
    /\ silent' = FALSE
    /\ UNCHANGED <<epoch, closed, lossLeft, epochAtClose, seen>>

Halt ==
    /\ closed
    /\ mode = "closed"
    /\ UNCHANGED vars

Next ==
    \/ ReadOk
    \/ LinkGoesSilent
    \/ ReadFails
    \/ Disconnected
    \/ WatchdogFires
    \/ PingWriteFails
    \/ ReconnectSucceeds
    \/ ReconnectFails
    \/ Close
    \/ ReaderHalts
    \/ Halt

Fair ==
    /\ WF_vars(WatchdogFires)
    /\ WF_vars(ReconnectSucceeds)
    /\ WF_vars(ReaderHalts)

Spec == Init /\ [][Next]_vars /\ Fair

LiveSet == IF mode = "reading" /\ ~closed THEN {epoch} ELSE {}

(*
  (1) type safety is TypeOK.
  (2) no zombie socket: after Close no action installs a new live epoch.
      epochAtClose snapshots Close; success needs ~closed, so this holds.
  (5a) at most one live socket: LiveSet has at most one member.
  (5b) epochs strictly increase: seen is exactly the prefix 0..epoch.
*)
NoZombie == (closed => epoch = epochAtClose)
SingleLive == Cardinality(LiveSet) <= 1
EpochIncrease == seen = (0 .. epoch)

(* (3) read loop terminates on Close. *)
ClosedHalts == closed ~> (mode = "closed")

(* (4) a silent link eventually redials (or Close wins first). *)
SilentRedials == (silent /\ ~closed) ~> ((mode = "reading") \/ closed)
====
