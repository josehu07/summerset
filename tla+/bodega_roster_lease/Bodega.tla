(********************************************************************************)
(* Bodega with roster leases, a single self-contained spec. Roster leases       *)
(* protocol's per-node state is folded into each node's record, and the lease   *)
(* protocol messages share the single `msgs` bag with consensus messages. The   *)
(* stable-roster check reads directly from the lease state of nodes; there is   *)
(* no separate refinement layer.                                                *)
(********************************************************************************)

---- MODULE Bodega ----
EXTENDS FiniteSets, Sequences, Integers, TLC

(*******************************)
(* Model inputs & assumptions. *)
(*******************************)
CONSTANT Replicas,       \* symmetric set of server nodes
         Writes,         \* symmetric set of write commands (each w/ unique value)
         Reads,          \* symmetric set of read commands
         MaxBallot,      \* maximum ballot pickable for leader preemption
         TGuard,         \* lease guard phase window length (in abstract ticks)
         TLease,         \* lease renewal extend window length (in abstract ticks)
         MaxTime,        \* upper bound on abstract time for model checking
         NodeFailuresOn  \* if true, turn on node failures injection

ReplicasAssumption == /\ IsFiniteSet(Replicas)
                      /\ Cardinality(Replicas) >= 1
                      /\ "none" \notin Replicas

Population == Cardinality(Replicas)

MajorityNum == (Population \div 2) + 1

WritesAssumption == /\ IsFiniteSet(Writes)
                    /\ Cardinality(Writes) >= 1
                    /\ "nil" \notin Writes

ReadsAssumption == /\ IsFiniteSet(Reads)
                   /\ Cardinality(Reads) >= 0
                   /\ "nil" \notin Writes

MaxBallotAssumption == /\ MaxBallot \in Nat
                       /\ MaxBallot >= 2

TGuardAssumption == /\ TGuard \in Nat
                    /\ TGuard >= 1

TLeaseAssumption == /\ TLease \in Nat
                    /\ TLease >= 1

MaxTimeAssumption == /\ MaxTime \in Nat
                     /\ MaxTime >= TGuard + TLease

NodeFailuresOnAssumption == NodeFailuresOn \in BOOLEAN

ASSUME /\ ReplicasAssumption
       /\ WritesAssumption
       /\ ReadsAssumption
       /\ MaxBallotAssumption
       /\ TGuardAssumption
       /\ TLeaseAssumption
       /\ MaxTimeAssumption
       /\ NodeFailuresOnAssumption

----------

(********************************)
(* Useful constants & typedefs. *)
(********************************)
Commands == Writes \cup Reads

NumWrites == Cardinality(Writes)

NumReads == Cardinality(Reads)

NumCommands == Cardinality(Commands)

Range(seq) == {seq[i]: i \in 1..Len(seq)}

\* Client observable events.
ClientEvents ==      [type: {"Req"}, cmd: Commands]
                \cup [type: {"Ack"}, cmd: Commands,
                                     val: {"nil"} \cup Writes,
                                     by: Replicas]

ReqEvent(c) == [type |-> "Req", cmd |-> c]

AckEvent(c, v, n) == [type |-> "Ack", cmd |-> c, val |-> v, by |-> n]
                        \* val is the old value for a write command

InitPending ==    (CHOOSE ws \in [1..Cardinality(Writes) -> Writes]
                        : Range(ws) = Writes)
               \o (CHOOSE rs \in [1..Cardinality(Reads) -> Reads]
                        : Range(rs) = Reads)

\* Server-side consensus constants & states.
Ballots == 1..MaxBallot

Slots == 1..NumWrites

InfinitySlot == NumWrites + 1
                    \* sentinel "infinitely-high" slot, used as commitPrev's
                    \* safe "haven't learned yet" marker

Rosters == {ros \in [bal: Ballots,
                     leader: Replicas,
                     responders: SUBSET Replicas]:
            ros.leader \notin ros.responders}
                \* for a smaller state space we exclude leader from the literal
                \* set of responders (but always treating it as a responder)

Roster(b, l, resps) == [bal |-> b, leader |-> l, responders |-> resps]
                        \* each new ballot number maps to a new roster; this
                        \* includes the change of leader (as in classic
                        \* MultiPaxos) and/or the change of who're responders

NullRoster == [bal |-> 0, leader |-> "none", responders |-> {}]

Statuses == {"Preparing", "Accepting", "Committed"}

InstStates == [status: {"Empty"} \cup Statuses,
               write: {"nil"} \cup Writes,
               voted: [bal: {0} \cup Ballots,
                       write: {"nil"} \cup Writes]]

NullInst == [status |-> "Empty",
             write |-> "nil",
             voted |-> [bal |-> 0, write |-> "nil"]]

\* Lease-side constants & typedefs.
Times == 1..MaxTime
ExpireTimes == 0..(MaxTime + TGuard + TLease)
                \* stored expiration / guard deadlines; 0 is the "null" time

SeqNums == Nat
            \* per-pair monotone seq nums on lease messages for dedup

GrantorStatuses == {"None", "Guarding", "Renewing", "Revoking"}
GranteeStatuses == {"None", "Guarded", "Renewed"}

GrantorState == [status: GrantorStatuses,
                 guardExpire: ExpireTimes,
                 leaseExpire: ExpireTimes,
                 ros: {NullRoster} \cup Rosters,
                 seq: SeqNums]

GranteeState == [status: GranteeStatuses,
                 guardExpire: ExpireTimes,
                 leaseExpire: ExpireTimes,
                 ros: {NullRoster} \cup Rosters,
                 seq: SeqNums]

NullGrantorState == [status |-> "None",
                     guardExpire |-> 0,
                     leaseExpire |-> 0,
                     ros |-> NullRoster,
                     seq |-> 0]

NullGranteeState == [status |-> "None",
                     guardExpire |-> 0,
                     leaseExpire |-> 0,
                     ros |-> NullRoster,
                     seq |-> 0]

\* Merged per-node state: consensus fields + lease fields.
NodeStates == [leader: {"none"} \cup Replicas,
               commitUpTo: {0} \cup Slots,
               commitPrev: {0} \cup Slots \cup {InfinitySlot},
               balPrepared: {0} \cup Ballots,
               balMaxKnown: {0} \cup Ballots,
               rosMaxKnown: {NullRoster} \cup Rosters,
               insts: [Slots -> InstStates],
               asGrantor: [Replicas -> GrantorState],
               asGrantee: [Replicas -> GranteeState]]

NullNode == [leader |-> "none",
             commitUpTo |-> 0,
             commitPrev |-> 0,
             balPrepared |-> 0,
             balMaxKnown |-> 0,
             rosMaxKnown |-> NullRoster,
             insts |-> [s \in Slots |-> NullInst],
             asGrantor |-> [p \in Replicas |-> NullGrantorState],
             asGrantee |-> [f \in Replicas |-> NullGranteeState]]
                \* commitPrev is the last slot which might have been
                \* committed by an old leader; a newly joined node
                \* can safely serve reads locally only after its log has
                \* been committed up to this slot
                \*
                \* rosMaxKnown is the roster that came with balMaxKnown
                \*
                \* asGrantor[p] is the grantor-side lease state & timers
                \* toward grantee p; asGrantee[f] is the grantee-side
                \* state toward grantor f

FirstEmptySlot(insts) ==
    IF \A s \in Slots: insts[s].status # "Empty"
        THEN InfinitySlot
        ELSE CHOOSE s \in Slots:
                /\ insts[s].status = "Empty"
                /\ \A t \in 1..(s - 1): insts[t].status # "Empty"

LastNonEmptySlot(insts) ==
    IF \A s \in Slots: insts[s].status = "Empty"
        THEN 0
        ELSE CHOOSE s \in Slots:
                /\ insts[s].status # "Empty"
                /\ \A t \in (s + 1)..NumWrites: insts[t].status = "Empty"

\* Service-internal consensus messages.
PrepareMsgs == [type: {"Prepare"}, src: Replicas,
                                   bal: Ballots]

PrepareMsg(r, b) == [type |-> "Prepare", src |-> r,
                                         bal |-> b]

InstsVotes == [Slots -> [bal: {0} \cup Ballots,
                         write: {"nil"} \cup Writes]]

VotesByNode(n) == [s \in Slots |-> n.insts[s].voted]

PrepareReplyMsgs == [type: {"PrepareReply"}, src: Replicas,
                                             bal: Ballots,
                                             votes: InstsVotes]

PrepareReplyMsg(r, b, iv) == [type |-> "PrepareReply", src |-> r,
                                                       bal |-> b,
                                                       votes |-> iv]

PeakVotedWrite(prs, s) ==
    IF \A pr \in prs: pr.votes[s].bal = 0
        THEN "nil"
        ELSE LET ppr ==
                    CHOOSE ppr \in prs:
                        \A pr \in prs: pr.votes[s].bal =< ppr.votes[s].bal
             IN  ppr.votes[s].write

LastTouchedSlot(prs) ==
    IF \A s \in Slots: PeakVotedWrite(prs, s) = "nil"
        THEN 0
        ELSE CHOOSE s \in Slots:
                /\ PeakVotedWrite(prs, s) # "nil"
                /\ \A t \in (s + 1)..NumWrites: PeakVotedWrite(prs, t) = "nil"

PrepareNoticeMsgs == [type: {"PrepareNotice"}, src: Replicas,
                                               bal: Ballots,
                                               commit_prev: {0} \cup Slots]

PrepareNoticeMsg(r, b, cp) == [type |-> "PrepareNotice", src |-> r,
                                                         bal |-> b,
                                                         commit_prev |-> cp]

AcceptMsgs == [type: {"Accept"}, src: Replicas,
                                 bal: Ballots,
                                 slot: Slots,
                                 write: Writes]

AcceptMsg(r, b, s, c) == [type |-> "Accept", src |-> r,
                                             bal |-> b,
                                             slot |-> s,
                                             write |-> c]

AcceptReplyMsgs == [type: {"AcceptReply"}, src: Replicas,
                                           bal: Ballots,
                                           slot: Slots]

AcceptReplyMsg(r, b, s) == [type |-> "AcceptReply", src |-> r,
                                                    bal |-> b,
                                                    slot |-> s]

CommitNoticeMsgs == [type: {"CommitNotice"}, upto: Slots]

CommitNoticeMsg(u) == [type |-> "CommitNotice", upto |-> u]

\* Lease protocol messages. Guard/Renew carry the roster and a per-pair seq
\* num; replies and Revoke(Reply) carry only the ballot + seq.
GuardMsgs == [type: {"Guard"}, grantor: Replicas,
                               grantee: Replicas,
                               bal: Ballots,
                               ros: Rosters,
                               seq: SeqNums]

GuardMsg(f, p, b, ro, s) == [type |-> "Guard", grantor |-> f,
                                               grantee |-> p,
                                               bal |-> b,
                                               ros |-> ro,
                                               seq |-> s]

GuardReplyMsgs == [type: {"GuardReply"}, grantee: Replicas,
                                         grantor: Replicas,
                                         bal: Ballots,
                                         seq: SeqNums]

GuardReplyMsg(p, f, b, s) == [type |-> "GuardReply", grantee |-> p,
                                                     grantor |-> f,
                                                     bal |-> b,
                                                     seq |-> s]

RenewMsgs == [type: {"Renew"}, grantor: Replicas,
                               grantee: Replicas,
                               bal: Ballots,
                               ros: Rosters,
                               seq: SeqNums]

RenewMsg(f, p, b, ro, s) == [type |-> "Renew", grantor |-> f,
                                               grantee |-> p,
                                               bal |-> b,
                                               ros |-> ro,
                                               seq |-> s]

RenewReplyMsgs == [type: {"RenewReply"}, grantee: Replicas,
                                         grantor: Replicas,
                                         bal: Ballots,
                                         seq: SeqNums]

RenewReplyMsg(p, f, b, s) == [type |-> "RenewReply", grantee |-> p,
                                                     grantor |-> f,
                                                     bal |-> b,
                                                     seq |-> s]

RevokeMsgs == [type: {"Revoke"}, grantor: Replicas,
                                 grantee: Replicas,
                                 bal: Ballots,
                                 seq: SeqNums]

RevokeMsg(f, p, b, s) == [type |-> "Revoke", grantor |-> f,
                                             grantee |-> p,
                                             bal |-> b,
                                             seq |-> s]

RevokeReplyMsgs == [type: {"RevokeReply"}, grantee: Replicas,
                                           grantor: Replicas,
                                           bal: Ballots,
                                           seq: SeqNums]

RevokeReplyMsg(p, f, b, s) == [type |-> "RevokeReply", grantee |-> p,
                                                       grantor |-> f,
                                                       bal |-> b,
                                                       seq |-> s]

Messages ==      PrepareMsgs
            \cup PrepareReplyMsgs
            \cup PrepareNoticeMsgs
            \cup AcceptMsgs
            \cup AcceptReplyMsgs
            \cup CommitNoticeMsgs
            \cup GuardMsgs
            \cup GuardReplyMsgs
            \cup RenewMsgs
            \cup RenewReplyMsgs
            \cup RevokeMsgs
            \cup RevokeReplyMsgs

----------

(******************************)
(* Main algorithm in PlusCal. *)
(******************************)
(*--algorithm Bodega

variable msgs = {},                             \* messages in the network
         node = [r \in Replicas |-> NullNode],  \* replica merged state
         pending = InitPending,                 \* sequence of pending reqs
         observed = <<>>,                       \* client observed events
         crashed = [r \in Replicas |-> FALSE],  \* replica crashed flag
         time = [r \in Replicas |-> 1];         \* per-node monotone clock

define
    \* A lease from grantee p's perspective is "active" iff p's asGrantee[f]
    \* is in Renewed status, un-expired, and recording roster ros.
    FGrantsPWithRos(f, p, ros) == /\ node[p].asGrantee[f].status = "Renewed"
                                  /\ node[p].asGrantee[f].leaseExpire > time[p]
                                  /\ node[p].asGrantee[f].ros = ros

    \* True if a node r thinks of itself as leader of the latest roster.
    ThinkAmLeader(r) ==
        LET ros == node[r].rosMaxKnown
        IN  /\ node[r].leader = r
            /\ node[r].balPrepared = node[r].balMaxKnown
            /\ ros.bal = node[r].balMaxKnown
            /\ ros.leader = r
            /\ Cardinality({f \in Replicas:
                            FGrantsPWithRos(f, r, ros)})
               >= MajorityNum

    \* True if a node r thinks of itself as a follower of the latest roster.
    ThinkAmFollower(r) ==
        LET ros == node[r].rosMaxKnown
        IN  /\ node[r].leader # r
            /\ ros.bal = node[r].balMaxKnown
            /\ ros.leader # r
            /\ ros.leader # "none"
            /\ Cardinality({f \in Replicas:
                            FGrantsPWithRos(f, ros.leader, ros)})
               >= MajorityNum

    \* True if a node r thinks of itself as a responder of the latest roster.
    ThinkAmResponder(r) ==
        /\ ThinkAmFollower(r)
        /\ r \in node[r].rosMaxKnown.responders

    \* The "safety threshold" condition: a node serves a read locally only if
    \* it has committed up to where previous leaders could have committed.
    BallotTransferred(r) == node[r].commitUpTo >= node[r].commitPrev

    \* Given caller node r (a leader or a responder) and a set of AcceptReplies,
    \* decide whether the write is committable: majority quorum AND the caller's
    \* known roster's responders all voted.
    WriteCommittable(r, ars) ==
        LET acceptors == {ar.src: ar \in ars}
        IN  /\ Cardinality(acceptors) >= MajorityNum
            /\ node[r].rosMaxKnown.responders \subseteq acceptors

    \* When node r's ballot rises to nb, any live outgoing grant whose
    \* roster is at an older ballot must be revoked: Guarding grants reset
    \* locally, Renewing grants move to Revoking with seq bumped. Expired
    \* grants are untouched (cleared later by TimeTick).
    RevokeStaleAsGrantor(r, nb) ==
        [p \in Replicas |->
            LET g == node[r].asGrantor[p]
            IN  IF      /\ g.ros.bal < nb
                        /\ g.status = "Guarding"
                        /\ g.guardExpire > time[r]
                    THEN [NullGrantorState EXCEPT !.seq = g.seq]
                ELSE IF /\ g.ros.bal < nb
                        /\ g.status = "Renewing"
                        /\ g.leaseExpire > time[r]
                    THEN [g EXCEPT !.status = "Revoking", !.seq = g.seq + 1]
                ELSE g]

    \* Revoke messages to emit alongside RevokeStaleAsGrantor(r, nb). Seq num
    \* matches the bumped seq in the Revoking state above.
    RevokeStaleSendMsgs(r, nb) ==
        {RevokeMsg(r, p, nb, node[r].asGrantor[p].seq + 1):
            p \in {p \in Replicas:
                     /\ node[r].asGrantor[p].ros.bal < nb
                     /\ node[r].asGrantor[p].status = "Renewing"
                     /\ node[r].asGrantor[p].leaseExpire > time[r]}}

    \* Miscellaneous model checking helpers:
    reqsMade == {e.cmd: e \in {e \in Range(observed): e.type = "Req"}}

    acksRecv == {e.cmd: e \in {e \in Range(observed): e.type = "Ack"}}

    AppendObserved(seq) ==
        LET filter(e) == IF e.type = "Req" THEN e.cmd \notin reqsMade
                                           ELSE e.cmd \notin acksRecv
        IN  observed \o SelectSeq(seq, filter)

    UnseenPending(r) ==
        LET filter(c) == \A s \in Slots: node[r].insts[s].write # c
        IN  SelectSeq(pending, filter)

    RemovePending(cmd) ==
        LET filter(c) == c # cmd
        IN  SelectSeq(pending, filter)

    terminated == /\ Len(pending) = 0
                  /\ Cardinality(reqsMade) = NumCommands
                  /\ Cardinality(acksRecv) = NumCommands

    numCrashed == Cardinality({r \in Replicas: crashed[r]})

    timeExhausted == \A r \in Replicas: time[r] = MaxTime
end define;

\* Send a set of messages helper.
macro Send(set) begin
    msgs := msgs \cup set;
end macro;

\* Observe client events helper.
macro Observe(seq) begin
    observed := AppendObserved(seq);
end macro;

\* Resolve a pending command helper.
macro Resolve(c) begin
    pending := RemovePending(c);
end macro;

\* Someone steps up as leader: picks a greater ballot, picks a fresh
\* responders set, and broadcasts Prepare. Also revokes any stale outgoing
\* lease grants: Guarding ones are reset locally; Renewing ones transition
\* to Revoking and send Revoke to the respective grantees.
macro BecomeLeader(r) begin
    \* if I'm not a current leader
    await node[r].leader # r;
    \* pick a greater ballot and an arbitrary responders set for new roster
    with b \in Ballots,
         resps \in SUBSET {f \in Replicas: f # r},
         ros = Roster(b, r, resps)
    do
        await /\ b > node[r].balMaxKnown
              /\ ~\E m \in msgs: (m.type = "Prepare") /\ (m.bal = b);
                    \* W.L.O.G., using this clause to model that ballot
                    \* numbers from different proposers be unique
        \* update states and restart Prepare phase for in-progress instances;
        \* also revoke any stale outgoing lease grants
        node[r].leader := r ||
        node[r].commitPrev := InfinitySlot ||
        node[r].balPrepared := 0 ||
        node[r].balMaxKnown := b ||
        node[r].rosMaxKnown := ros ||
        node[r].insts :=
            [s \in Slots |->
                [node[r].insts[s]
                    EXCEPT !.status = IF @ = "Accepting"
                                        THEN "Preparing"
                                        ELSE @]] ||
        node[r].asGrantor := RevokeStaleAsGrantor(r, b);
        \* broadcast Prepare and reply to myself instantly; also send Revokes
        \* for the just-Revoking grants
        Send(      {PrepareMsg(r, b),
                    PrepareReplyMsg(r, b, VotesByNode(node[r]))}
              \cup RevokeStaleSendMsgs(r, b));
    end with;
end macro;

\* Replica replies to a Prepare message. Also revokes any stale outgoing lease
\* grants the same way as BecomeLeader.
macro HandlePrepare(r) begin
    \* if receiving a Prepare message with larger ballot than ever seen
    with m \in msgs do
        await /\ m.type = "Prepare"
              /\ m.bal > node[r].balMaxKnown
              \* Prepare arrives along with an active lease from its sender;
              \* here we assert there's a Renewed grantee-side record for it.
              \* This stands in for "Prepare piggybacks the new roster"
              /\ node[r].asGrantee[m.src].status = "Renewed"
              /\ node[r].asGrantee[m.src].leaseExpire > time[r]
              /\ node[r].asGrantee[m.src].ros.bal = m.bal;
        with ros = node[r].asGrantee[m.src].ros do
            \* update states and reset statuses; also revoke stale outgoing
            \* lease grants
            node[r].leader := m.src ||
            node[r].commitPrev := InfinitySlot ||
            node[r].balMaxKnown := m.bal ||
            node[r].rosMaxKnown := ros ||
            node[r].insts :=
                [s \in Slots |->
                    [node[r].insts[s]
                        EXCEPT !.status = IF @ = "Accepting"
                                            THEN "Preparing"
                                            ELSE @]] ||
            node[r].asGrantor := RevokeStaleAsGrantor(r, m.bal);
            \* send back PrepareReply with my voted list; also send Revokes
            \* for the just-Revoking grants
            Send(      {PrepareReplyMsg(r, m.bal, VotesByNode(node[r]))}
                  \cup RevokeStaleSendMsgs(r, m.bal));
        end with;
    end with;
end macro;

\* Leader gathers PrepareReply messages until condition met, then marks
\* the corresponding ballot as prepared and saves highest voted commands.
macro HandlePrepareReplies(r) begin
    \* if I'm waiting for PrepareReplies
    await /\ node[r].leader = r
          /\ node[r].balPrepared = 0;
    \* when there are enough number of PrepareReplies of desired ballot
    with prs = {m \in msgs: /\ m.type = "PrepareReply"
                            /\ m.bal = node[r].balMaxKnown}
    do
        await Cardinality({pr.src: pr \in prs}) >= MajorityNum;
        with prsGot \in {prsGot \in SUBSET prs:
                         Cardinality({pr.src: pr \in prsGot}) >= MajorityNum},
             lts = LastTouchedSlot(prsGot)
        do
            \* marks this ballot as prepared and saves highest voted command
            \* in each slot if any
            node[r].balPrepared := node[r].balMaxKnown ||
            node[r].insts :=
                [s \in Slots |->
                    LET pvw == PeakVotedWrite(prsGot, s)
                        adopted == \/ node[r].insts[s].status = "Preparing"
                                   \/ /\ node[r].insts[s].status = "Empty"
                                      /\ pvw # "nil"
                    IN  [node[r].insts[s]
                            EXCEPT !.status = IF adopted
                                                THEN "Accepting"
                                                ELSE @,
                                   !.write  = pvw,
                                   !.voted  = IF adopted
                                                THEN [bal |-> node[r].balMaxKnown,
                                                      write |-> pvw]
                                                ELSE @]] ||
            node[r].commitPrev := lts;
            \* send Accept messages for in-progress instances and reply to myself
            \* instantly; send PrepareNotices as well (mimicking threshold-carrying
            \* messages, in paper manuscript this is achieved via Guard messages)
            Send(    UNION
                     {{AcceptMsg(r, node[r].balPrepared, s, node[r].insts[s].write),
                       AcceptReplyMsg(r, node[r].balPrepared, s)}:
                      s \in {s \in Slots: node[r].insts[s].status = "Accepting"}}
                \cup {PrepareNoticeMsg(r, node[r].balPrepared, lts)});
        end with;
    end with;
end macro;

\* Follower receives PrepareNotice from a prepared and recovered leader,
\* updating its commitPrev accordingly.
macro HandlePrepareNotice(r) begin
    \* if I'm a follower waiting on PrepareNotice
    await /\ ThinkAmFollower(r)
          /\ node[r].commitPrev = InfinitySlot;
    \* when there's a PrepareNotice message in effect
    with m \in msgs do
        await /\ m.type = "PrepareNotice"
              /\ m.bal = node[r].balMaxKnown;
        \* update my commitPrev
        node[r].commitPrev := m.commit_prev;
    end with;
end macro;

\* A prepared leader takes a new write request into the next empty slot.
macro TakeNewWriteRequest(r) begin
    \* if I'm a prepared leader and there's pending write request
    with unseen = UnseenPending(r) do
        await /\ ThinkAmLeader(r)
              /\ \E s \in Slots: node[r].insts[s].status = "Empty"
              /\ Len(unseen) > 0
              /\ Head(unseen) \in Writes;
        \* find the next empty slot and pick a pending request
        with s = FirstEmptySlot(node[r].insts),
             c = Head(unseen)
        do
            \* update slot status and voted
            node[r].insts[s].status := "Accepting" ||
            node[r].insts[s].write := c ||
            node[r].insts[s].voted.bal := node[r].balPrepared ||
            node[r].insts[s].voted.write := c;
            \* broadcast Accept and reply to myself instantly
            Send({AcceptMsg(r, node[r].balPrepared, s, c),
                  AcceptReplyMsg(r, node[r].balPrepared, s)});
            \* append to observed events sequence if haven't yet
            Observe(<<ReqEvent(c)>>);
        end with;
    end with;
end macro;

\* Replica replies to an Accept message. If the Accept's ballot is higher than
\* what we've known, also revokes any stale outgoing lease grants.
macro HandleAccept(r) begin
    \* if I'm a follower
    await ThinkAmFollower(r);
    \* if receiving an unreplied Accept message with valid ballot, piggybacked
    \* with an active lease from the sender
    with m \in msgs do
        await /\ m.type = "Accept"
              /\ m.bal >= node[r].balMaxKnown
              /\ m.bal >= node[r].insts[m.slot].voted.bal
              /\ node[r].asGrantee[m.src].status = "Renewed"
              /\ node[r].asGrantee[m.src].leaseExpire > time[r]
              /\ node[r].asGrantee[m.src].ros.bal = m.bal;
        \* update node states and corresponding instance's states; also
        \* revoke any stale outgoing lease grants
        node[r].leader := m.src ||
        node[r].balMaxKnown := m.bal ||
        node[r].rosMaxKnown := node[r].asGrantee[m.src].ros ||
        node[r].insts[m.slot].status := "Accepting" ||
        node[r].insts[m.slot].write := m.write ||
        node[r].insts[m.slot].voted.bal := m.bal ||
        node[r].insts[m.slot].voted.write := m.write ||
        node[r].asGrantor := RevokeStaleAsGrantor(r, m.bal);
        \* send back AcceptReply; also send Revokes for the just-Revoking grants
        Send(     {AcceptReplyMsg(r, m.bal, m.slot)}
             \cup RevokeStaleSendMsgs(r, m.bal));
    end with;
end macro;

\* Leader gathers AcceptReply messages for a slot until condition met,
\* then marks the slot as committed and acknowledges the client.
macro HandleAcceptReplies(r) begin
    \* if I'm a prepared leader
    await /\ ThinkAmLeader(r)
          /\ node[r].commitUpTo < NumWrites
          /\ node[r].insts[node[r].commitUpTo + 1].status = "Accepting";
                \* W.L.O.G., only enabling the next slot after commitUpTo
    \* for this slot, when there is a good set of AcceptReplies that is at
    \* least a majority number and that covers all responders
    with s = node[r].commitUpTo + 1,
         c = node[r].insts[s].write,
         ls = s - 1,
         v = IF ls = 0 THEN "nil" ELSE node[r].insts[ls].write,
         ars = {m \in msgs: /\ m.type = "AcceptReply"
                            /\ m.slot = s
                            /\ m.bal = node[r].balPrepared}
    do
        await WriteCommittable(r, ars);
        with arsGot \in {arsGot \in SUBSET ars:
                         WriteCommittable(r, arsGot)}
        do
            \* marks this slot as committed and apply command
            node[r].insts[s].status := "Committed" ||
            node[r].commitUpTo := s;
            \* append to observed events sequence if haven't yet, and remove
            \* the command from pending
            Observe(<<AckEvent(c, v, r)>>);
            Resolve(c);
            \* broadcast CommitNotice to followers
            Send({CommitNoticeMsg(s)});
        end with;
    end with;
end macro;

\* Replica receives new commit notification.
macro HandleCommitNotice(r) begin
    \* if I'm a follower waiting on CommitNotice
    await /\ ThinkAmFollower(r)
          /\ node[r].commitUpTo < NumWrites
          /\ node[r].insts[node[r].commitUpTo + 1].status = "Accepting";
    \* for this slot, when there's a CommitNotice message
    with s = node[r].commitUpTo + 1,
         c = node[r].insts[s].write,
         m \in msgs
    do
        await /\ m.type = "CommitNotice"
              /\ m.upto = s;
        \* marks this slot as committed and apply command
        node[r].insts[s].status := "Committed" ||
        node[r].commitUpTo := s;
    end with;
end macro;

\* A prepared leader or a responder follower takes a new read request and
\* serves it locally.
macro TakeNewReadRequest(r) begin
    \* if I'm a caught-up leader or responder follower
    with unseen = UnseenPending(r) do
        await /\ \/ ThinkAmLeader(r)
                 \/ ThinkAmResponder(r)
              /\ BallotTransferred(r)
              /\ Len(unseen) > 0
              /\ Head(unseen) \in Reads;
        \* pick a pending request; examine my log and find the last non-empty
        \* slot, check its status
        with s = LastNonEmptySlot(node[r].insts),
             v = IF s = 0 THEN "nil" ELSE node[r].insts[s].write,
             c = Head(unseen)
        do
            \* if the latest value is in Committed status, can directly reply;
            \* otherwise, should hold until I've received enough broadcasted
            \* AcceptReplies indicating that the write is surely to be committed
            await \/ s = 0
                  \/ s > 0 /\ node[r].insts[s].status = "Committed"
                  \/ LET ars == {m \in msgs: /\ m.type = "AcceptReply"
                                             /\ m.slot = s
                                             /\ m.bal = node[r].balMaxKnown}
                     IN  WriteCommittable(r, ars);
            \* acknowledge client with the latest value, and remove the command
            \* from pending
            Observe(<<ReqEvent(c), AckEvent(c, v, r)>>);
            Resolve(c);
        end with;
    end with;
end macro;

\* Replica node crashes itself under promised conditions.
macro ReplicaCrashes(r) begin
    \* if less than (N - MajorityNum) number of replicas have failed
    await /\ MajorityNum + numCrashed < Cardinality(Replicas)
          /\ ~crashed[r]
          /\ node[r].balMaxKnown < MaxBallot;
    \* mark myself as crashed
    crashed[r] := TRUE;
end macro;

\* Grantor f opens new leases to peers for its known current roster. Gated
\* by the condition that f has a non-null rosMaxKnown and f is not actively
\* granting to anyone.
macro GrantorInitiateLeases(f) begin
    await /\ node[f].rosMaxKnown # NullRoster
          /\ \A p \in Replicas: node[f].asGrantor[p].status = "None";
    node[f].asGrantor :=
        [p \in Replicas |->
            [status |-> "Guarding",
             guardExpire |-> time[f] + TGuard,
             leaseExpire |-> 0,
             ros |-> node[f].rosMaxKnown,
             seq |-> node[f].asGrantor[p].seq + 1]];
    Send({GuardMsg(f, p,
                   node[f].balMaxKnown,
                   node[f].rosMaxKnown,
                   node[f].asGrantor[p].seq + 1):
          p \in Replicas});
end macro;

\* Grantee p receives a Guard from grantor f. Accept iff ballot is at least
\* as high as p's view and the message's seq is strictly higher than any seq
\* ever observed on this pair. If ballot rises, also revokes stale outgoing
\* lease grants from p.
macro HandleGuard(p) begin
    with m \in msgs do
        await /\ m.type = "Guard"
              /\ m.grantee = p
              /\ m.bal >= node[p].balMaxKnown
              /\ m.seq > node[p].asGrantee[m.grantor].seq
              /\ node[p].asGrantee[m.grantor].status = "None";
        \* update ballot in case higher, and start Guarded state; bump seq;
        \* also revoke any stale outgoing lease grants
        node[p].balMaxKnown := m.bal ||
        node[p].asGrantee[m.grantor] :=
            [status |-> "Guarded",
             guardExpire |-> time[p] + TGuard,
             leaseExpire |-> 0,
             ros |-> m.ros,
             seq |-> m.seq + 1] ||
        node[p].asGrantor := RevokeStaleAsGrantor(p, m.bal);
        \* reply GuardReply back to grantor, stamped with the new seq; also
        \* send Revokes for the just-Revoking outgoing grants
        Send(      {GuardReplyMsg(p, m.grantor, m.bal, m.seq + 1)}
              \cup RevokeStaleSendMsgs(p, m.bal));
    end with;
end macro;

\* Grantor f receives a GuardReply: transition asGrantor[m.grantee] from
\* Guarding to Renewing; send first Renew.
macro HandleGuardReply(f) begin
    with m \in msgs do
        await /\ m.type = "GuardReply"
              /\ m.grantor = f
              /\ m.bal = node[f].balMaxKnown
              /\ m.seq > node[f].asGrantor[m.grantee].seq
              /\ node[f].asGrantor[m.grantee].status = "Guarding";
        \* loose expiry for safety, tightened at first RenewReply
        node[f].asGrantor[m.grantee] :=
            [node[f].asGrantor[m.grantee] EXCEPT
                !.status = "Renewing",
                !.leaseExpire = time[f] + TGuard + TLease,
                !.seq = m.seq + 1];
        Send({RenewMsg(f, m.grantee, m.bal,
                       node[f].rosMaxKnown, m.seq + 1)});
    end with;
end macro;

\* Grantor f spontaneously sends subsequent Renews to its current grantees.
macro SpontaneousRenew(f) begin
    await \E p \in Replicas:
            /\ node[f].asGrantor[p].ros.bal = node[f].balMaxKnown
            /\ node[f].asGrantor[p].status = "Renewing"
            /\ time[f] < node[f].asGrantor[p].leaseExpire
            /\ time[f] + TLease > node[f].asGrantor[p].leaseExpire;
    with ps = {p \in Replicas:
                  /\ node[f].asGrantor[p].ros.bal = node[f].balMaxKnown
                  /\ node[f].asGrantor[p].status = "Renewing"
                  /\ time[f] < node[f].asGrantor[p].leaseExpire
                  /\ time[f] + TLease > node[f].asGrantor[p].leaseExpire} do
        node[f].asGrantor :=
            [p \in Replicas |->
                IF p \in ps
                  THEN [node[f].asGrantor[p] EXCEPT
                            !.leaseExpire = node[f].asGrantor[p].leaseExpire + TLease,
                            !.seq = node[f].asGrantor[p].seq + 1]
                  ELSE node[f].asGrantor[p]];
        Send({RenewMsg(f, p,
                       node[f].balMaxKnown,
                       node[f].rosMaxKnown,
                       node[f].asGrantor[p].seq + 1):
              p \in ps});
    end with;
end macro;

\* Grantee p receives a Renew.
macro HandleRenew(p) begin
    with m \in msgs do
        await /\ m.type = "Renew"
              /\ m.grantee = p
              /\ m.bal = node[p].balMaxKnown
              /\ m.seq > node[p].asGrantee[m.grantor].seq
              /\ \/ /\ node[p].asGrantee[m.grantor].status = "Guarded"
                    /\ time[p] < node[p].asGrantee[m.grantor].guardExpire
                 \/ /\ node[p].asGrantee[m.grantor].status = "Renewed"
                    /\ time[p] < node[p].asGrantee[m.grantor].leaseExpire
                    /\ time[p] + TLease > node[p].asGrantee[m.grantor].leaseExpire;
        node[p].asGrantee[m.grantor] :=
            [node[p].asGrantee[m.grantor] EXCEPT
                !.status = "Renewed",
                !.leaseExpire = time[p] + TLease,
                !.ros = m.ros,
                !.seq = m.seq + 1];
        Send({RenewReplyMsg(p, m.grantor, m.bal, m.seq + 1)});
    end with;
end macro;

\* Grantor f receives a RenewReply; tightens down the loose expiry.
macro HandleRenewReply(f) begin
    with m \in msgs do
        await /\ m.type = "RenewReply"
              /\ m.grantor = f
              /\ m.bal = node[f].balMaxKnown
              /\ m.seq > node[f].asGrantor[m.grantee].seq
              /\ node[f].asGrantor[m.grantee].status = "Renewing"
              /\ time[f] + TLease > node[f].asGrantor[m.grantee].leaseExpire;
        node[f].asGrantor[m.grantee] :=
            [node[f].asGrantor[m.grantee] EXCEPT
                !.leaseExpire = time[f] + TLease,
                !.seq = m.seq];
    end with;
end macro;

\* Grantee p processes a Revoke from grantor f. If ballot rises, also
\* revokes stale outgoing lease grants from p.
macro HandleRevoke(p) begin
    with m \in msgs do
        await /\ m.type = "Revoke"
              /\ m.grantee = p
              /\ m.bal >= node[p].balMaxKnown
              /\ m.seq > node[p].asGrantee[m.grantor].seq
              /\ node[p].asGrantee[m.grantor].status \in {"Guarded", "Renewed"};
        \* update ballot in case higher, clear grantee state but preserve seq;
        \* also revoke any stale outgoing lease grants
        node[p].balMaxKnown := m.bal ||
        node[p].asGrantee[m.grantor] :=
            [NullGranteeState EXCEPT !.seq = m.seq + 1] ||
        node[p].asGrantor := RevokeStaleAsGrantor(p, m.bal);
        Send(      {RevokeReplyMsg(p, m.grantor, m.bal, m.seq + 1)}
              \cup RevokeStaleSendMsgs(p, m.bal));
    end with;
end macro;

\* Grantor f receives a RevokeReply; drops the lease promptly.
macro HandleRevokeReply(f) begin
    with m \in msgs do
        await /\ m.type = "RevokeReply"
              /\ m.grantor = f
              /\ m.bal = node[f].balMaxKnown
              /\ m.seq > node[f].asGrantor[m.grantee].seq
              /\ node[f].asGrantor[m.grantee].status = "Revoking";
        node[f].asGrantor[m.grantee] :=
            [NullGrantorState EXCEPT !.seq = m.seq];
    end with;
end macro;

\* Advances time by one tick globally, and garbage-collects expired lease
\* state. Per-pair seq counters are preserved across GC.
macro TimeTick() begin
    await \A r \in Replicas: time[r] < MaxTime;
    time := [r \in Replicas |-> time[r] + 1];
    node := [r \in Replicas |->
        [node[r] EXCEPT
            !.asGrantor =
                [p \in Replicas |->
                    IF \/ /\ node[r].asGrantor[p].status \in {"Renewing", "Revoking"}
                          /\ node[r].asGrantor[p].leaseExpire =< time[r]
                       \/ /\ node[r].asGrantor[p].status = "Guarding"
                          /\ node[r].asGrantor[p].guardExpire =< time[r]
                      THEN [NullGrantorState EXCEPT !.seq = node[r].asGrantor[p].seq]
                      ELSE node[r].asGrantor[p]],
            !.asGrantee =
                [f \in Replicas |->
                    IF \/ /\ node[r].asGrantee[f].status = "Renewed"
                          /\ node[r].asGrantee[f].leaseExpire =< time[r]
                       \/ /\ node[r].asGrantee[f].status = "Guarded"
                          /\ node[r].asGrantee[f].guardExpire =< time[r]
                      THEN [NullGranteeState EXCEPT !.seq = node[r].asGrantee[f].seq]
                      ELSE node[r].asGrantee[f]]]];
end macro;

\* Replica server node main loop: consensus actions + lease protocol actions
\* + global time tick are all available via either/or.
process Replica \in Replicas
begin
    rloop: while (~terminated) /\ (~timeExhausted) /\ (~crashed[self]) do
        either
            BecomeLeader(self);
        or
            HandlePrepare(self);
        or
            HandlePrepareReplies(self);
        or
            HandlePrepareNotice(self);
        or
            TakeNewWriteRequest(self);
        or
            HandleAccept(self);
        or
            HandleAcceptReplies(self);
        or
            HandleCommitNotice(self);
        or
            TakeNewReadRequest(self);
        or
            GrantorInitiateLeases(self);
        or
            HandleGuard(self);
        or
            HandleGuardReply(self);
        or
            SpontaneousRenew(self);
        or
            HandleRenew(self);
        or
            HandleRenewReply(self);
        or
            HandleRevoke(self);
        or
            HandleRevokeReply(self);
        or
            TimeTick();
        or
            if NodeFailuresOn then
                ReplicaCrashes(self);
            end if;
        end either;
    end while;
end process;

end algorithm; *)

\* BEGIN TRANSLATION
VARIABLES pc, msgs, node, pending, observed, crashed, time

(* define statement *)
FGrantsPWithRos(f, p, ros) == /\ node[p].asGrantee[f].status = "Renewed"
                              /\ node[p].asGrantee[f].leaseExpire > time[p]
                              /\ node[p].asGrantee[f].ros = ros


ThinkAmLeader(r) ==
    LET ros == node[r].rosMaxKnown
    IN  /\ node[r].leader = r
        /\ node[r].balPrepared = node[r].balMaxKnown
        /\ ros.bal = node[r].balMaxKnown
        /\ ros.leader = r
        /\ Cardinality({f \in Replicas:
                        FGrantsPWithRos(f, r, ros)})
           >= MajorityNum


ThinkAmFollower(r) ==
    LET ros == node[r].rosMaxKnown
    IN  /\ node[r].leader # r
        /\ ros.bal = node[r].balMaxKnown
        /\ ros.leader # r
        /\ ros.leader # "none"
        /\ Cardinality({f \in Replicas:
                        FGrantsPWithRos(f, ros.leader, ros)})
           >= MajorityNum


ThinkAmResponder(r) ==
    /\ ThinkAmFollower(r)
    /\ r \in node[r].rosMaxKnown.responders



BallotTransferred(r) == node[r].commitUpTo >= node[r].commitPrev




WriteCommittable(r, ars) ==
    LET acceptors == {ar.src: ar \in ars}
    IN  /\ Cardinality(acceptors) >= MajorityNum
        /\ node[r].rosMaxKnown.responders \subseteq acceptors





RevokeStaleAsGrantor(r, nb) ==
    [p \in Replicas |->
        LET g == node[r].asGrantor[p]
        IN  IF      /\ g.ros.bal < nb
                    /\ g.status = "Guarding"
                    /\ g.guardExpire > time[r]
                THEN [NullGrantorState EXCEPT !.seq = g.seq]
            ELSE IF /\ g.ros.bal < nb
                    /\ g.status = "Renewing"
                    /\ g.leaseExpire > time[r]
                THEN [g EXCEPT !.status = "Revoking", !.seq = g.seq + 1]
            ELSE g]



RevokeStaleSendMsgs(r, nb) ==
    {RevokeMsg(r, p, nb, node[r].asGrantor[p].seq + 1):
        p \in {p \in Replicas:
                 /\ node[r].asGrantor[p].ros.bal < nb
                 /\ node[r].asGrantor[p].status = "Renewing"
                 /\ node[r].asGrantor[p].leaseExpire > time[r]}}


reqsMade == {e.cmd: e \in {e \in Range(observed): e.type = "Req"}}

acksRecv == {e.cmd: e \in {e \in Range(observed): e.type = "Ack"}}

AppendObserved(seq) ==
    LET filter(e) == IF e.type = "Req" THEN e.cmd \notin reqsMade
                                       ELSE e.cmd \notin acksRecv
    IN  observed \o SelectSeq(seq, filter)

UnseenPending(r) ==
    LET filter(c) == \A s \in Slots: node[r].insts[s].write # c
    IN  SelectSeq(pending, filter)

RemovePending(cmd) ==
    LET filter(c) == c # cmd
    IN  SelectSeq(pending, filter)

terminated == /\ Len(pending) = 0
              /\ Cardinality(reqsMade) = NumCommands
              /\ Cardinality(acksRecv) = NumCommands

numCrashed == Cardinality({r \in Replicas: crashed[r]})

timeExhausted == \A r \in Replicas: time[r] = MaxTime


vars == << pc, msgs, node, pending, observed, crashed, time >>

ProcSet == (Replicas)

Init == (* Global variables *)
        /\ msgs = {}
        /\ node = [r \in Replicas |-> NullNode]
        /\ pending = InitPending
        /\ observed = <<>>
        /\ crashed = [r \in Replicas |-> FALSE]
        /\ time = [r \in Replicas |-> 1]
        /\ pc = [self \in ProcSet |-> "rloop"]

rloop(self) == /\ pc[self] = "rloop"
               /\ IF (~terminated) /\ (~timeExhausted) /\ (~crashed[self])
                     THEN /\ \/ /\ node[self].leader # self
                                /\ \E b \in Ballots:
                                     \E resps \in SUBSET {f \in Replicas: f # self}:
                                       LET ros == Roster(b, self, resps) IN
                                         /\ /\ b > node[self].balMaxKnown
                                            /\ ~\E m \in msgs: (m.type = "Prepare") /\ (m.bal = b)
                                         /\ node' = [node EXCEPT ![self].leader = self,
                                                                 ![self].commitPrev = InfinitySlot,
                                                                 ![self].balPrepared = 0,
                                                                 ![self].balMaxKnown = b,
                                                                 ![self].rosMaxKnown = ros,
                                                                 ![self].insts = [s \in Slots |->
                                                                                     [node[self].insts[s]
                                                                                         EXCEPT !.status = IF @ = "Accepting"
                                                                                                             THEN "Preparing"
                                                                                                             ELSE @]],
                                                                 ![self].asGrantor = RevokeStaleAsGrantor(self, b)]
                                         /\ msgs' = (msgs \cup (     {PrepareMsg(self, b),
                                                                      PrepareReplyMsg(self, b, VotesByNode(node'[self]))}
                                                     \cup RevokeStaleSendMsgs(self, b)))
                                /\ UNCHANGED <<pending, observed, crashed, time>>
                             \/ /\ \E m \in msgs:
                                     /\ /\ m.type = "Prepare"
                                        /\ m.bal > node[self].balMaxKnown
                                        
                                        
                                        
                                        /\ node[self].asGrantee[m.src].status = "Renewed"
                                        /\ node[self].asGrantee[m.src].leaseExpire > time[self]
                                        /\ node[self].asGrantee[m.src].ros.bal = m.bal
                                     /\ LET ros == node[self].asGrantee[m.src].ros IN
                                          /\ node' = [node EXCEPT ![self].leader = m.src,
                                                                  ![self].commitPrev = InfinitySlot,
                                                                  ![self].balMaxKnown = m.bal,
                                                                  ![self].rosMaxKnown = ros,
                                                                  ![self].insts = [s \in Slots |->
                                                                                      [node[self].insts[s]
                                                                                          EXCEPT !.status = IF @ = "Accepting"
                                                                                                              THEN "Preparing"
                                                                                                              ELSE @]],
                                                                  ![self].asGrantor = RevokeStaleAsGrantor(self, m.bal)]
                                          /\ msgs' = (msgs \cup (     {PrepareReplyMsg(self, m.bal, VotesByNode(node'[self]))}
                                                      \cup RevokeStaleSendMsgs(self, m.bal)))
                                /\ UNCHANGED <<pending, observed, crashed, time>>
                             \/ /\ /\ node[self].leader = self
                                   /\ node[self].balPrepared = 0
                                /\ LET prs == {m \in msgs: /\ m.type = "PrepareReply"
                                                           /\ m.bal = node[self].balMaxKnown} IN
                                     /\ Cardinality({pr.src: pr \in prs}) >= MajorityNum
                                     /\ \E prsGot \in {prsGot \in SUBSET prs:
                                                       Cardinality({pr.src: pr \in prsGot}) >= MajorityNum}:
                                          LET lts == LastTouchedSlot(prsGot) IN
                                            /\ node' = [node EXCEPT ![self].balPrepared = node[self].balMaxKnown,
                                                                    ![self].insts = [s \in Slots |->
                                                                                        LET pvw == PeakVotedWrite(prsGot, s)
                                                                                            adopted == \/ node[self].insts[s].status = "Preparing"
                                                                                                       \/ /\ node[self].insts[s].status = "Empty"
                                                                                                          /\ pvw # "nil"
                                                                                        IN  [node[self].insts[s]
                                                                                                EXCEPT !.status = IF adopted
                                                                                                                    THEN "Accepting"
                                                                                                                    ELSE @,
                                                                                                       !.write  = pvw,
                                                                                                       !.voted  = IF adopted
                                                                                                                    THEN [bal |-> node[self].balMaxKnown,
                                                                                                                          write |-> pvw]
                                                                                                                    ELSE @]],
                                                                    ![self].commitPrev = lts]
                                            /\ msgs' = (msgs \cup (     UNION
                                                                        {{AcceptMsg(self, node'[self].balPrepared, s, node'[self].insts[s].write),
                                                                          AcceptReplyMsg(self, node'[self].balPrepared, s)}:
                                                                         s \in {s \in Slots: node'[self].insts[s].status = "Accepting"}}
                                                        \cup {PrepareNoticeMsg(self, node'[self].balPrepared, lts)}))
                                /\ UNCHANGED <<pending, observed, crashed, time>>
                             \/ /\ /\ ThinkAmFollower(self)
                                   /\ node[self].commitPrev = InfinitySlot
                                /\ \E m \in msgs:
                                     /\ /\ m.type = "PrepareNotice"
                                        /\ m.bal = node[self].balMaxKnown
                                     /\ node' = [node EXCEPT ![self].commitPrev = m.commit_prev]
                                /\ UNCHANGED <<msgs, pending, observed, crashed, time>>
                             \/ /\ LET unseen == UnseenPending(self) IN
                                     /\ /\ ThinkAmLeader(self)
                                        /\ \E s \in Slots: node[self].insts[s].status = "Empty"
                                        /\ Len(unseen) > 0
                                        /\ Head(unseen) \in Writes
                                     /\ LET s == FirstEmptySlot(node[self].insts) IN
                                          LET c == Head(unseen) IN
                                            /\ node' = [node EXCEPT ![self].insts[s].status = "Accepting",
                                                                    ![self].insts[s].write = c,
                                                                    ![self].insts[s].voted.bal = node[self].balPrepared,
                                                                    ![self].insts[s].voted.write = c]
                                            /\ msgs' = (msgs \cup ({AcceptMsg(self, node'[self].balPrepared, s, c),
                                                                    AcceptReplyMsg(self, node'[self].balPrepared, s)}))
                                            /\ observed' = AppendObserved((<<ReqEvent(c)>>))
                                /\ UNCHANGED <<pending, crashed, time>>
                             \/ /\ ThinkAmFollower(self)
                                /\ \E m \in msgs:
                                     /\ /\ m.type = "Accept"
                                        /\ m.bal >= node[self].balMaxKnown
                                        /\ m.bal >= node[self].insts[m.slot].voted.bal
                                        /\ node[self].asGrantee[m.src].status = "Renewed"
                                        /\ node[self].asGrantee[m.src].leaseExpire > time[self]
                                        /\ node[self].asGrantee[m.src].ros.bal = m.bal
                                     /\ node' = [node EXCEPT ![self].leader = m.src,
                                                             ![self].balMaxKnown = m.bal,
                                                             ![self].rosMaxKnown = node[self].asGrantee[m.src].ros,
                                                             ![self].insts[m.slot].status = "Accepting",
                                                             ![self].insts[m.slot].write = m.write,
                                                             ![self].insts[m.slot].voted.bal = m.bal,
                                                             ![self].insts[m.slot].voted.write = m.write,
                                                             ![self].asGrantor = RevokeStaleAsGrantor(self, m.bal)]
                                     /\ msgs' = (msgs \cup (     {AcceptReplyMsg(self, m.bal, m.slot)}
                                                 \cup RevokeStaleSendMsgs(self, m.bal)))
                                /\ UNCHANGED <<pending, observed, crashed, time>>
                             \/ /\ /\ ThinkAmLeader(self)
                                   /\ node[self].commitUpTo < NumWrites
                                   /\ node[self].insts[node[self].commitUpTo + 1].status = "Accepting"
                                /\ LET s == node[self].commitUpTo + 1 IN
                                     LET c == node[self].insts[s].write IN
                                       LET ls == s - 1 IN
                                         LET v == IF ls = 0 THEN "nil" ELSE node[self].insts[ls].write IN
                                           LET ars == {m \in msgs: /\ m.type = "AcceptReply"
                                                                   /\ m.slot = s
                                                                   /\ m.bal = node[self].balPrepared} IN
                                             /\ WriteCommittable(self, ars)
                                             /\ \E arsGot \in {arsGot \in SUBSET ars:
                                                               WriteCommittable(self, arsGot)}:
                                                  /\ node' = [node EXCEPT ![self].insts[s].status = "Committed",
                                                                          ![self].commitUpTo = s]
                                                  /\ observed' = AppendObserved((<<AckEvent(c, v, self)>>))
                                                  /\ pending' = RemovePending(c)
                                                  /\ msgs' = (msgs \cup ({CommitNoticeMsg(s)}))
                                /\ UNCHANGED <<crashed, time>>
                             \/ /\ /\ ThinkAmFollower(self)
                                   /\ node[self].commitUpTo < NumWrites
                                   /\ node[self].insts[node[self].commitUpTo + 1].status = "Accepting"
                                /\ LET s == node[self].commitUpTo + 1 IN
                                     LET c == node[self].insts[s].write IN
                                       \E m \in msgs:
                                         /\ /\ m.type = "CommitNotice"
                                            /\ m.upto = s
                                         /\ node' = [node EXCEPT ![self].insts[s].status = "Committed",
                                                                 ![self].commitUpTo = s]
                                /\ UNCHANGED <<msgs, pending, observed, crashed, time>>
                             \/ /\ LET unseen == UnseenPending(self) IN
                                     /\ /\ \/ ThinkAmLeader(self)
                                           \/ ThinkAmResponder(self)
                                        /\ BallotTransferred(self)
                                        /\ Len(unseen) > 0
                                        /\ Head(unseen) \in Reads
                                     /\ LET s == LastNonEmptySlot(node[self].insts) IN
                                          LET v == IF s = 0 THEN "nil" ELSE node[self].insts[s].write IN
                                            LET c == Head(unseen) IN
                                              /\ \/ s = 0
                                                 \/ s > 0 /\ node[self].insts[s].status = "Committed"
                                                 \/ LET ars == {m \in msgs: /\ m.type = "AcceptReply"
                                                                            /\ m.slot = s
                                                                            /\ m.bal = node[self].balMaxKnown}
                                                    IN  WriteCommittable(self, ars)
                                              /\ observed' = AppendObserved((<<ReqEvent(c), AckEvent(c, v, self)>>))
                                              /\ pending' = RemovePending(c)
                                /\ UNCHANGED <<msgs, node, crashed, time>>
                             \/ /\ /\ node[self].rosMaxKnown # NullRoster
                                   /\ \A p \in Replicas: node[self].asGrantor[p].status = "None"
                                /\ node' = [node EXCEPT ![self].asGrantor = [p \in Replicas |->
                                                                                [status |-> "Guarding",
                                                                                 guardExpire |-> time[self] + TGuard,
                                                                                 leaseExpire |-> 0,
                                                                                 ros |-> node[self].rosMaxKnown,
                                                                                 seq |-> node[self].asGrantor[p].seq + 1]]]
                                /\ msgs' = (msgs \cup ({GuardMsg(self, p,
                                                                 node'[self].balMaxKnown,
                                                                 node'[self].rosMaxKnown,
                                                                 node'[self].asGrantor[p].seq + 1):
                                                        p \in Replicas}))
                                /\ UNCHANGED <<pending, observed, crashed, time>>
                             \/ /\ \E m \in msgs:
                                     /\ /\ m.type = "Guard"
                                        /\ m.grantee = self
                                        /\ m.bal >= node[self].balMaxKnown
                                        /\ m.seq > node[self].asGrantee[m.grantor].seq
                                        /\ node[self].asGrantee[m.grantor].status = "None"
                                     /\ node' = [node EXCEPT ![self].balMaxKnown = m.bal,
                                                             ![self].asGrantee[m.grantor] = [status |-> "Guarded",
                                                                                             guardExpire |-> time[self] + TGuard,
                                                                                             leaseExpire |-> 0,
                                                                                             ros |-> m.ros,
                                                                                             seq |-> m.seq + 1],
                                                             ![self].asGrantor = RevokeStaleAsGrantor(self, m.bal)]
                                     /\ msgs' = (msgs \cup (     {GuardReplyMsg(self, m.grantor, m.bal, m.seq + 1)}
                                                 \cup RevokeStaleSendMsgs(self, m.bal)))
                                /\ UNCHANGED <<pending, observed, crashed, time>>
                             \/ /\ \E m \in msgs:
                                     /\ /\ m.type = "GuardReply"
                                        /\ m.grantor = self
                                        /\ m.bal = node[self].balMaxKnown
                                        /\ m.seq > node[self].asGrantor[m.grantee].seq
                                        /\ node[self].asGrantor[m.grantee].status = "Guarding"
                                     /\ node' = [node EXCEPT ![self].asGrantor[m.grantee] = [node[self].asGrantor[m.grantee] EXCEPT
                                                                                                !.status = "Renewing",
                                                                                                !.leaseExpire = time[self] + TGuard + TLease,
                                                                                                !.seq = m.seq + 1]]
                                     /\ msgs' = (msgs \cup ({RenewMsg(self, m.grantee, m.bal,
                                                                      node'[self].rosMaxKnown, m.seq + 1)}))
                                /\ UNCHANGED <<pending, observed, crashed, time>>
                             \/ /\ \E p \in Replicas:
                                     /\ node[self].asGrantor[p].ros.bal = node[self].balMaxKnown
                                     /\ node[self].asGrantor[p].status = "Renewing"
                                     /\ time[self] < node[self].asGrantor[p].leaseExpire
                                     /\ time[self] + TLease > node[self].asGrantor[p].leaseExpire
                                /\ LET ps == {p \in Replicas:
                                                 /\ node[self].asGrantor[p].ros.bal = node[self].balMaxKnown
                                                 /\ node[self].asGrantor[p].status = "Renewing"
                                                 /\ time[self] < node[self].asGrantor[p].leaseExpire
                                                 /\ time[self] + TLease > node[self].asGrantor[p].leaseExpire} IN
                                     /\ node' = [node EXCEPT ![self].asGrantor = [p \in Replicas |->
                                                                                     IF p \in ps
                                                                                       THEN [node[self].asGrantor[p] EXCEPT
                                                                                                 !.leaseExpire = node[self].asGrantor[p].leaseExpire + TLease,
                                                                                                 !.seq = node[self].asGrantor[p].seq + 1]
                                                                                       ELSE node[self].asGrantor[p]]]
                                     /\ msgs' = (msgs \cup ({RenewMsg(self, p,
                                                                      node'[self].balMaxKnown,
                                                                      node'[self].rosMaxKnown,
                                                                      node'[self].asGrantor[p].seq + 1):
                                                             p \in ps}))
                                /\ UNCHANGED <<pending, observed, crashed, time>>
                             \/ /\ \E m \in msgs:
                                     /\ /\ m.type = "Renew"
                                        /\ m.grantee = self
                                        /\ m.bal = node[self].balMaxKnown
                                        /\ m.seq > node[self].asGrantee[m.grantor].seq
                                        /\ \/ /\ node[self].asGrantee[m.grantor].status = "Guarded"
                                              /\ time[self] < node[self].asGrantee[m.grantor].guardExpire
                                           \/ /\ node[self].asGrantee[m.grantor].status = "Renewed"
                                              /\ time[self] < node[self].asGrantee[m.grantor].leaseExpire
                                              /\ time[self] + TLease > node[self].asGrantee[m.grantor].leaseExpire
                                     /\ node' = [node EXCEPT ![self].asGrantee[m.grantor] = [node[self].asGrantee[m.grantor] EXCEPT
                                                                                                !.status = "Renewed",
                                                                                                !.leaseExpire = time[self] + TLease,
                                                                                                !.ros = m.ros,
                                                                                                !.seq = m.seq + 1]]
                                     /\ msgs' = (msgs \cup ({RenewReplyMsg(self, m.grantor, m.bal, m.seq + 1)}))
                                /\ UNCHANGED <<pending, observed, crashed, time>>
                             \/ /\ \E m \in msgs:
                                     /\ /\ m.type = "RenewReply"
                                        /\ m.grantor = self
                                        /\ m.bal = node[self].balMaxKnown
                                        /\ m.seq > node[self].asGrantor[m.grantee].seq
                                        /\ node[self].asGrantor[m.grantee].status = "Renewing"
                                        /\ time[self] + TLease > node[self].asGrantor[m.grantee].leaseExpire
                                     /\ node' = [node EXCEPT ![self].asGrantor[m.grantee] = [node[self].asGrantor[m.grantee] EXCEPT
                                                                                                !.leaseExpire = time[self] + TLease,
                                                                                                !.seq = m.seq]]
                                /\ UNCHANGED <<msgs, pending, observed, crashed, time>>
                             \/ /\ \E m \in msgs:
                                     /\ /\ m.type = "Revoke"
                                        /\ m.grantee = self
                                        /\ m.bal >= node[self].balMaxKnown
                                        /\ m.seq > node[self].asGrantee[m.grantor].seq
                                        /\ node[self].asGrantee[m.grantor].status \in {"Guarded", "Renewed"}
                                     /\ node' = [node EXCEPT ![self].balMaxKnown = m.bal,
                                                             ![self].asGrantee[m.grantor] = [NullGranteeState EXCEPT !.seq = m.seq + 1],
                                                             ![self].asGrantor = RevokeStaleAsGrantor(self, m.bal)]
                                     /\ msgs' = (msgs \cup (     {RevokeReplyMsg(self, m.grantor, m.bal, m.seq + 1)}
                                                 \cup RevokeStaleSendMsgs(self, m.bal)))
                                /\ UNCHANGED <<pending, observed, crashed, time>>
                             \/ /\ \E m \in msgs:
                                     /\ /\ m.type = "RevokeReply"
                                        /\ m.grantor = self
                                        /\ m.bal = node[self].balMaxKnown
                                        /\ m.seq > node[self].asGrantor[m.grantee].seq
                                        /\ node[self].asGrantor[m.grantee].status = "Revoking"
                                     /\ node' = [node EXCEPT ![self].asGrantor[m.grantee] = [NullGrantorState EXCEPT !.seq = m.seq]]
                                /\ UNCHANGED <<msgs, pending, observed, crashed, time>>
                             \/ /\ \A r \in Replicas: time[r] < MaxTime
                                /\ time' = [r \in Replicas |-> time[r] + 1]
                                /\ node' =     [r \in Replicas |->
                                           [node[r] EXCEPT
                                               !.asGrantor =
                                                   [p \in Replicas |->
                                                       IF \/ /\ node[r].asGrantor[p].status \in {"Renewing", "Revoking"}
                                                             /\ node[r].asGrantor[p].leaseExpire =< time'[r]
                                                          \/ /\ node[r].asGrantor[p].status = "Guarding"
                                                             /\ node[r].asGrantor[p].guardExpire =< time'[r]
                                                         THEN [NullGrantorState EXCEPT !.seq = node[r].asGrantor[p].seq]
                                                         ELSE node[r].asGrantor[p]],
                                               !.asGrantee =
                                                   [f \in Replicas |->
                                                       IF \/ /\ node[r].asGrantee[f].status = "Renewed"
                                                             /\ node[r].asGrantee[f].leaseExpire =< time'[r]
                                                          \/ /\ node[r].asGrantee[f].status = "Guarded"
                                                             /\ node[r].asGrantee[f].guardExpire =< time'[r]
                                                         THEN [NullGranteeState EXCEPT !.seq = node[r].asGrantee[f].seq]
                                                         ELSE node[r].asGrantee[f]]]]
                                /\ UNCHANGED <<msgs, pending, observed, crashed>>
                             \/ /\ IF NodeFailuresOn
                                      THEN /\ /\ MajorityNum + numCrashed < Cardinality(Replicas)
                                              /\ ~crashed[self]
                                              /\ node[self].balMaxKnown < MaxBallot
                                           /\ crashed' = [crashed EXCEPT ![self] = TRUE]
                                      ELSE /\ TRUE
                                           /\ UNCHANGED crashed
                                /\ UNCHANGED <<msgs, node, pending, observed, time>>
                          /\ pc' = [pc EXCEPT ![self] = "rloop"]
                     ELSE /\ pc' = [pc EXCEPT ![self] = "Done"]
                          /\ UNCHANGED << msgs, node, pending, observed, 
                                          crashed, time >>

Replica(self) == rloop(self)

(* Allow infinite stuttering to prevent deadlock on termination. *)
Terminating == /\ \A self \in ProcSet: pc[self] = "Done"
               /\ UNCHANGED vars

Next == (\E self \in Replicas: Replica(self))
           \/ Terminating

Spec == Init /\ [][Next]_vars

Termination == <>(\A self \in ProcSet: pc[self] = "Done")

\* END TRANSLATION

====
