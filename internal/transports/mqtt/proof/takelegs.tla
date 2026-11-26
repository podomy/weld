---- MODULE takelegs ----
(*
  Take-path leg buffering for the weld MQTT backend.

  Models backend.go stageWire/pump plus the two take paths and the two
  leg buffers, with the request/reply distinction abstracted to one
  service leg (two legs total: data, service):

    staged set    staged samples waiting for pump delivery
                  (models conn arrivals feeding stageWire)
    inbox         bounded FIFO, cap Cap (models Backend.inbox,
                  backend.go inboxCap / chan ConnMessage)
    dataBuf       bounded FIFO, cap BufCap, evict-oldest with counting
                  (models Backend.dataPending with maxLegPending)
    svcBuf        bounded FIFO, cap BufCap, evict-oldest with counting
                  (models Backend.svcPending with maxLegPending)
    taken         samples delivered to either taker
    evicted       samples dropped by EvictOldest
    dropped       explicit drop counter (models Backend.dropped,
                  backend.go Dropped)
    maxData/maxSvc high-water marks of the two leg buffers

  Actions:
    StageData(s)    pump delivers one staged data sample to the inbox;
                    blocked when the inbox is full (backend.go stageWire,
                    non-blocking send path)
    StageSvc(s)     same for one service-leg sample
    EvictOldest     full inbox evicts the oldest sample and counts one
                    drop (backend.go stageWire default branch: receive
                    one, re-send, dropped++). Standalone here instead of
                    atomic with Stage; reachable end states coincide.
    TakeDataPath    idealized TakeBytes (backend.go TakeBytes): serve
                    dataBuf head first; else consume the inbox head,
                    delivering data samples and parking service samples
                    in svcBuf
    TakeServicePath TakeService (service.go TakeService): mirror image
    Halt            quiescent stutter

  NOTE on idealization: real TakeBytes returns whatever leg the inbox
  head carries (backend.go TakeBytes decodes and returns without
  parking cross-leg samples); the spec models the symmetric buffering
  discipline for the parking direction the code implements. Buffer
  bounds and the counted evictions match the code exactly
  (maxLegPending, evict-oldest, Backend.dropped).

  Proves: (1) TypeOK including the leg-buffer caps; (2) NoDup, no
  sample delivered twice across both paths; (3) DropCount plus
  Conservation, eviction (inbox or leg buffer) is the only loss and
  every loss is counted; (4) StarvationFree/Terminates, under fair
  takes every staged sample is eventually taken or evicted.
  maxData/maxSvc record buffer high-water marks; MaxBounded checks
  the reported tight bounds under fair takes. A one-legged taker no
  longer grows the other buffer without bound: the cap plus counted
  eviction bounds it, matching the code.
*)
EXTENDS Naturals, Sequences, FiniteSets, TLC

CONSTANTS DataIds, SvcIds, Cap, MaxData, MaxSvc, BufCap

ASSUME DataIds \cap SvcIds = {}
ASSUME Cardinality(DataIds) >= 1 /\ Cardinality(SvcIds) >= 1
ASSUME Cap >= 1
ASSUME BufCap >= 1

VARIABLES staged, inbox, dataBuf, svcBuf, taken, evicted,
          dropped, maxData, maxSvc

vars == <<staged, inbox, dataBuf, svcBuf, taken, evicted,
          dropped, maxData, maxSvc>>

Samples == DataIds \cup SvcIds

Max(a, b) == IF a > b THEN a ELSE b

SeqSet(s) == {s[i] : i \in 1 .. Len(s)}

TypeOK ==
    /\ staged \in SUBSET Samples
    /\ inbox \in Seq(Samples)
    /\ Len(inbox) <= Cap
    /\ dataBuf \in Seq(Samples)
    /\ Len(dataBuf) <= BufCap
    /\ svcBuf \in Seq(Samples)
    /\ Len(svcBuf) <= BufCap
    /\ taken \in SUBSET Samples
    /\ evicted \in SUBSET Samples
    /\ dropped \in Nat
    /\ maxData \in Nat
    /\ maxSvc \in Nat

Init ==
    /\ staged = {}
    /\ inbox = <<>>
    /\ dataBuf = <<>>
    /\ svcBuf = <<>>
    /\ taken = {}
    /\ evicted = {}
    /\ dropped = 0
    /\ maxData = 0
    /\ maxSvc = 0

StageData(s) ==
    /\ s \in DataIds \ staged
    /\ Len(inbox) < Cap
    /\ staged' = staged \cup {s}
    /\ inbox' = Append(inbox, s)
    /\ UNCHANGED <<dataBuf, svcBuf, taken, evicted, dropped, maxData, maxSvc>>

StageSvc(s) ==
    /\ s \in SvcIds \ staged
    /\ Len(inbox) < Cap
    /\ staged' = staged \cup {s}
    /\ inbox' = Append(inbox, s)
    /\ UNCHANGED <<dataBuf, svcBuf, taken, evicted, dropped, maxData, maxSvc>>

EvictOldest ==
    /\ Len(inbox) = Cap
    /\ inbox' = Tail(inbox)
    /\ evicted' = evicted \cup {Head(inbox)}
    /\ dropped' = dropped + 1
    /\ UNCHANGED <<staged, dataBuf, svcBuf, taken, maxData, maxSvc>>

TakeDataPath ==
    \/ (/\ Len(dataBuf) > 0
        /\ taken' = taken \cup {Head(dataBuf)}
        /\ dataBuf' = Tail(dataBuf)
        /\ UNCHANGED <<staged, inbox, svcBuf, evicted, dropped, maxData, maxSvc>>)
    \/ (/\ Len(dataBuf) = 0
        /\ Len(inbox) > 0
        /\ Head(inbox) \in DataIds
        /\ taken' = taken \cup {Head(inbox)}
        /\ inbox' = Tail(inbox)
        /\ UNCHANGED <<staged, dataBuf, svcBuf, evicted, dropped, maxData, maxSvc>>)
    \/ (/\ Len(dataBuf) = 0
        /\ Len(inbox) > 0
        /\ Head(inbox) \in SvcIds
        /\ Len(svcBuf) < BufCap
        /\ inbox' = Tail(inbox)
        /\ svcBuf' = Append(svcBuf, Head(inbox))
        /\ maxSvc' = Max(maxSvc, Len(svcBuf'))
        /\ UNCHANGED <<staged, dataBuf, taken, evicted, dropped, maxData>>)

TakeServicePath ==
    \/ (/\ Len(svcBuf) > 0
        /\ taken' = taken \cup {Head(svcBuf)}
        /\ svcBuf' = Tail(svcBuf)
        /\ UNCHANGED <<staged, inbox, dataBuf, evicted, dropped, maxData, maxSvc>>)
    \/ (/\ Len(svcBuf) = 0
        /\ Len(inbox) > 0
        /\ Head(inbox) \in SvcIds
        /\ taken' = taken \cup {Head(inbox)}
        /\ inbox' = Tail(inbox)
        /\ UNCHANGED <<staged, dataBuf, svcBuf, evicted, dropped, maxData, maxSvc>>)
    \/ (/\ Len(svcBuf) = 0
        /\ Len(inbox) > 0
        /\ Head(inbox) \in DataIds
        /\ Len(dataBuf) < BufCap
        /\ inbox' = Tail(inbox)
        /\ dataBuf' = Append(dataBuf, Head(inbox))
        /\ maxData' = Max(maxData, Len(dataBuf'))
        /\ UNCHANGED <<staged, svcBuf, taken, evicted, dropped, maxSvc>>)

EvictData ==
    /\ Len(dataBuf) = BufCap
    /\ dataBuf' = Tail(dataBuf)
    /\ evicted' = evicted \cup {Head(dataBuf)}
    /\ dropped' = dropped + 1
    /\ UNCHANGED <<staged, inbox, svcBuf, taken, maxData, maxSvc>>

EvictSvc ==
    /\ Len(svcBuf) = BufCap
    /\ svcBuf' = Tail(svcBuf)
    /\ evicted' = evicted \cup {Head(svcBuf)}
    /\ dropped' = dropped + 1
    /\ UNCHANGED <<staged, inbox, dataBuf, taken, maxData, maxSvc>>

Quiescent ==
    /\ staged = Samples
    /\ inbox = <<>>
    /\ dataBuf = <<>>
    /\ svcBuf = <<>>

Halt ==
    /\ Quiescent
    /\ UNCHANGED vars

Next ==
    \/ \E s \in DataIds : StageData(s)
    \/ \E s \in SvcIds : StageSvc(s)
    \/ EvictOldest
    \/ EvictData
    \/ EvictSvc
    \/ TakeDataPath
    \/ TakeServicePath
    \/ Halt

Fair ==
    /\ \A s \in DataIds : WF_vars(StageData(s))
    /\ \A s \in SvcIds : WF_vars(StageSvc(s))
    /\ WF_vars(EvictOldest)
    /\ WF_vars(EvictData)
    /\ WF_vars(EvictSvc)
    /\ WF_vars(TakeDataPath)
    /\ WF_vars(TakeServicePath)

Spec == Init /\ [][Next]_vars /\ Fair

Conservation ==
    /\ staged = taken \cup evicted \cup SeqSet(inbox) \cup
                SeqSet(dataBuf) \cup SeqSet(svcBuf)
    /\ taken \cap evicted = {}
    /\ taken \cap SeqSet(inbox) = {}
    /\ taken \cap SeqSet(dataBuf) = {}
    /\ taken \cap SeqSet(svcBuf) = {}
    /\ evicted \cap SeqSet(inbox) = {}
    /\ evicted \cap SeqSet(dataBuf) = {}
    /\ evicted \cap SeqSet(svcBuf) = {}
    /\ SeqSet(inbox) \cap SeqSet(dataBuf) = {}
    /\ SeqSet(inbox) \cap SeqSet(svcBuf) = {}
    /\ SeqSet(dataBuf) \cap SeqSet(svcBuf) = {}

NoDup ==
    /\ \A s \in taken :
        s \notin evicted /\ s \notin SeqSet(inbox) /\
        s \notin SeqSet(dataBuf) /\ s \notin SeqSet(svcBuf)
    /\ Cardinality(SeqSet(inbox)) = Len(inbox)
    /\ Cardinality(SeqSet(dataBuf)) = Len(dataBuf)
    /\ Cardinality(SeqSet(svcBuf)) = Len(svcBuf)

DropCount == dropped = Cardinality(evicted)

LegSeparation ==
    /\ SeqSet(dataBuf) \subseteq DataIds
    /\ SeqSet(svcBuf) \subseteq SvcIds

TrackerOK ==
    /\ maxData >= Len(dataBuf)
    /\ maxSvc >= Len(svcBuf)

MaxBounded ==
    /\ maxData <= MaxData
    /\ maxSvc <= MaxSvc

StarvationFree == (staged = Samples) ~> Quiescent

Terminates == <>Quiescent
====
