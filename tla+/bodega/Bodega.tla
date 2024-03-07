(**********************************************************************************)
(* Bodega protocol for WAN-scale read-optimized consensus by employing the nearby *)
(* read technique with config leases.                                             *)
(*                                                                                *)
(* See multipaxos_smr_addon/MultiPaxos.tla for the base spec it extends from.     *)
(**********************************************************************************)

---- MODULE Bodega ----
EXTENDS FiniteSets, Sequences, Integers, TLC

(*******************************)
(* Model inputs & assumptions. *)
(*******************************)
CONSTANT Replicas,   \* symmetric set of server nodes
         Writes,     \* symmetric set of write commands (each w/ unique value)
         Reads,      \* symmetric set of read commands
         MaxBallot,  \* maximum ballot pickable for leader preemption
         NodeFailuresOn   \* if true, turn on node failures injection

ReplicasAssumption == /\ IsFiniteSet(Replicas)
                      /\ Cardinality(Replicas) >= 1
                      /\ "none" \notin Replicas

Population == Cardinality(Replicas)

MajorityNum == (Population \div 2) + 1

WritesAssumption == /\ IsFiniteSet(Writes)
                    /\ Cardinality(Writes) >= 1
                    /\ "nil" \notin Writes
                            \* a write command model value serves as both the
                            \* ID of the command and the value to be written

ReadsAssumption == /\ IsFiniteSet(Reads)
                   /\ Cardinality(Reads) >= 0
                   /\ "nil" \notin Writes

MaxBallotAssumption == /\ MaxBallot \in Nat
                       /\ MaxBallot >= 2

NodeFailuresOnAssumption == NodeFailuresOn \in BOOLEAN

ASSUME /\ ReplicasAssumption
       /\ WritesAssumption
       /\ ReadsAssumption
       /\ MaxBallotAssumption
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
                                     val: {"nil"} \cup Writes]

ReqEvent(c) == [type |-> "Req", cmd |-> c]

AckEvent(c, v) == [type |-> "Ack", cmd |-> c, val |-> v]
                        \* val is the old value for a write command

InitPending ==    (CHOOSE ws \in [1..Cardinality(Writes) -> Writes]
                        : Range(ws) = Writes)
               \o (CHOOSE rs \in [1..Cardinality(Reads) -> Reads]
                        : Range(rs) = Reads)
                    \* W.L.O.G., choose any sequence contatenating writes
                    \* commands and read commands as the sequence of reqs;
                    \* all other cases are either symmetric or less useful
                    \* than this one

\* Server-side constants & states.
Ballots == 1..MaxBallot

Slots == 1..NumWrites

Statuses == {"Preparing", "Accepting", "Committed"}

InstStates == [status: {"Empty"} \cup Statuses,
               write: {"nil"} \cup Writes,
               voted: [bal: {0} \cup Ballots,
                       write: {"nil"} \cup Writes]]

NullInst == [status |-> "Empty",
             write |-> "nil",
             voted |-> [bal |-> 0, write |-> "nil"]]

NodeStates == [leader: {"none"} \cup Replicas,
               commitUpTo: {0} \cup Slots,
               commitPrev: {0} \cup Slots \cup {NumWrites+1},
               balPrepared: {0} \cup Ballots,
               balMaxKnown: {0} \cup Ballots,
               insts: [Slots -> InstStates],
               reads: SUBSET Reads]

NullNode == [leader |-> "none",
             commitUpTo |-> 0,
             commitPrev |-> 0,
             balPrepared |-> 0,
             balMaxKnown |-> 0,
             insts |-> [s \in Slots |-> NullInst],
             reads |-> {}]
                \* commitPrev is the last slot which might have been
                \* committed by an old leader; a newly prepared leader
                \* can safely serve reads locally only after its log has
                \* been committed up to this slot. The time before this
                \* condition becomes satisfied may be considered the
                \* "recovery" or "ballot transfer" time
                \* reads is the set of on-the-fly read requests issued
                \* by this node (in practice, this should be tracked
                \* by the client; here, think of the node itself being
                \* the client)

FirstEmptySlot(insts) ==
    IF \A s \in Slots: insts[s].status # "Empty"
        THEN NumWrites + 1
        ELSE CHOOSE s \in Slots:
                /\ insts[s].status = "Empty"
                /\ \A t \in 1..(s-1): insts[t].status # "Empty"

LastNonEmptySlot(insts) ==
    IF \A s \in Slots: insts[s].status = "Empty"
        THEN 0
        ELSE CHOOSE s \in Slots:
                /\ insts[s].status # "Empty"
                /\ \A t \in (s+1)..NumWrites: insts[t].status = "Empty"
                \* note that this is not the same as FirstEmptySlot - 1
                \* due to possible existence of holes

\* Service-internal messages.
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
                /\ \A t \in (s+1)..NumWrites: PeakVotedWrite(prs, t) = "nil"

PrepareNoticeMsgs == [type: {"PrepareNotice"}, src: Replicas,
                                               bal: Ballots,
                                               commit_prev: {0} \cup Slots]

PrepareNoticeMsg(r, b, cp) == [type |-> "PrepareNotice", src |-> r,
                                                         bal |-> b,
                                                         commit_prev |-> cp]
                                    \* this messasge is added to allow
                                    \* followers to learn about commitPrev

AcceptWriteMsgs == [type: {"AcceptWrite"}, src: Replicas,
                                           bal: Ballots,
                                           slot: Slots,
                                           write: Writes]

AcceptWriteMsg(r, b, s, c) == [type |-> "AcceptWrite", src |-> r,
                                                       bal |-> b,
                                                       slot |-> s,
                                                       write |-> c]

AcceptWriteReplyMsgs == [type: {"AcceptWriteReply"}, src: Replicas,
                                                     bal: Ballots,
                                                     slot: Slots]

AcceptWriteReplyMsg(r, b, s) == [type |-> "AcceptWriteReply", src |-> r,
                                                              bal |-> b,
                                                              slot |-> s]
                                    \* no need to carry command ID in
                                    \* AcceptWriteReply because ballot and
                                    \* slot uniquely identifies the write

NearbyReadMsgs == [type: {"NearbyRead"}, src: Replicas,
                                         bal: Ballots,
                                         read: Reads]

NearbyReadMsg(r, b, c) == [type |-> "NearbyRead", src |-> r,
                                                  bal |-> b,
                                                  read |-> c]

NearbyReadReplyMsgs == [type: {"NearbyReadReply"}, src: Replicas,
                                                   bal: Ballots,
                                                   read: Reads,
                                                   is_leader: BOOLEAN,
                                                   last_slot: {0} \cup Slots,
                                                   committed: BOOLEAN,
                                                   val: {"nil"} \cup Writes]

NearbyReadReplyMsg(r, b, c, il, ls, cm, v) ==
    [type |-> "NearbyReadReply", src |-> r,
                                 bal |-> b,
                                 read |-> c,
                                 is_leader |-> il,
                                 last_slot |-> ls,
                                 committed |-> cm,
                                 val |-> v]
        \* read here is just a command ID

CommitNoticeMsgs == [type: {"CommitNotice"}, upto: Slots]

CommitNoticeMsg(u) == [type |-> "CommitNotice", upto |-> u]

Messages ==      PrepareMsgs
            \cup PrepareReplyMsgs
            \cup PrepareNoticeMsgs
            \cup AcceptWriteMsgs
            \cup AcceptWriteReplyMsgs
            \cup NearbyReadMsgs
            \cup NearbyReadReplyMsgs
            \cup CommitNoticeMsgs

\* Config lease related typedefs.
Configs == [bal: Ballots, leader: Replicas, rqSize: 1..MajorityNum]

Config(b, l, rq) == [bal |-> b, leader |-> l, rqSize |-> rq]
                        \* each new ballot number maps to a new config; this
                        \* includes the change of leader (as in classic
                        \* MultiPaxos) and also the change of quorum size

LeaseGrants == [from: Replicas, config: Configs]

LeaseGrant(f, cfg) == [from |-> f, config |-> cfg]
                        \* this is the only type of message that may be
                        \* "removed" from the global set of messages to make
                        \* a "cheated" model of leasing: if a LeaseGrant
                        \* message is removed, it means that promise has
                        \* expired and the grantor did not refresh, probably
                        \* in order to switch to a differnt config

----------

(******************************)
(* Main algorithm in PlusCal. *)
(******************************)
(*--algorithm Bodega

variable msgs = {},                             \* messages in the network
         grants = {},                           \* lease msgs in the network
         node = [r \in Replicas |-> NullNode],  \* replica node state
         pending = InitPending,                 \* sequence of pending reqs
         observed = <<>>,                       \* client observed events
         crashed = [r \in Replicas |-> FALSE];  \* replica crashed flag

define
    CurrentConfig ==
        LET leased(b) == Cardinality({g \in grants:
                                      g.config.bal = b}) >= MajorityNum
        IN  IF ~\E b \in Ballots: leased(b)
                THEN Config(0, "none", 0)
                ELSE (CHOOSE g \in grants: leased(g.config.bal)).config
                        \* the leasing mechanism ensures that at any
                        \* time, there's at most one leader

    ThinkAmLeader(r) == /\ node[r].leader = r
                        /\ node[r].balPrepared = node[r].balMaxKnown
                        /\ CurrentConfig.bal > 0
                        /\ CurrentConfig.bal = node[r].balMaxKnown
                        /\ CurrentConfig.leader = r

    ThinkAmFollower(r) == /\ node[r].leader # r
                          /\ CurrentConfig.bal > 0
                          /\ CurrentConfig.bal = node[r].balMaxKnown
                          /\ CurrentConfig.leader # r

    BallotTransfered(r) == node[r].commitUpTo >= node[r].commitPrev

    reqsMade == {e.cmd: e \in {e \in Range(observed): e.type = "Req"}}
    
    acksRecv == {e.cmd: e \in {e \in Range(observed): e.type = "Ack"}}

    AppendObserved(seq) ==
        LET filter(e) == IF e.type = "Req" THEN e.cmd \notin reqsMade
                                           ELSE e.cmd \notin acksRecv
        IN  observed \o SelectSeq(seq, filter)

    UnseenPending(r) ==
        LET filter(c) ==
                /\ \A s \in Slots: node[r].insts[s].write # c
                /\ c \notin node[r].reads
        IN  SelectSeq(pending, filter)
    
    RemovePending(cmd) ==
        LET filter(c) == c # cmd
        IN  SelectSeq(pending, filter)

    terminated == /\ Len(pending) = 0
                  /\ Cardinality(reqsMade) = NumCommands
                  /\ Cardinality(acksRecv) = NumCommands
    
    numCrashed == Cardinality({r \in Replicas: crashed[r]})
end define;

\* Send a set of messages helper.
macro Send(set) begin
    msgs := msgs \cup set;
end macro;

\* Expire existing lease grant from f, and make a new repeatedly refreshed
\* lease grant to new config cfg.
macro Lease(f, cfg) begin
    grants := {g \in grants: g.from # f} \cup {LeaseGrant(f, cfg)};
end macro;

\* Observe client events helper.
macro Observe(seq) begin
    observed := AppendObserved(seq);
end macro;

\* Resolve a pending command helper.
macro Resolve(c) begin
    pending := RemovePending(c);
end macro;

\* Someone steps up as leader and sends Prepare message to followers.
\* To simplify this spec W.L.O.G., we change the quorum size config when
\* a new leader steps up; in practice, a separate and independent type of
\* trigger will be used to change the quorum size config.
macro BecomeLeader(r) begin
    \* if I'm not a current leader
    await node[r].leader # r;
    \* pick a greater ballot number and a quorum size config
    with b \in Ballots,
         rq \in (1+numCrashed)..MajorityNum,
    do
        await /\ b > node[r].balMaxKnown
              /\ ~\E m \in msgs: (m.type = "Prepare") /\ (m.bal = b);
                    \* W.L.O.G., using this clause to model that ballot
                    \* numbers from different proposers be unique
        \* update states and restart Prepare phase for in-progress instances
        node[r].leader := r ||
        node[r].commitPrev := NumWrites + 1 ||
        node[r].balPrepared := 0 ||
        node[r].balMaxKnown := b ||
        node[r].insts :=
            [s \in Slots |->
                [node[r].insts[s]
                    EXCEPT !.status = IF @ = "Accepting"
                                        THEN "Preparing"
                                        ELSE @]] ||
        node[r].reads := {};
        \* broadcast Prepare and reply to myself instantly
        Send({PrepareMsg(r, b),
              PrepareReplyMsg(r, b, VotesByNode(node[r]))});
        \* expire my old lease grant if any and grant to myself
        Lease(r, Config(b, r, rq));
    end with;
end macro;

\* Replica replies to a Prepare message.
macro HandlePrepare(r) begin
    \* if receiving a Prepare message with larger ballot than ever seen
    with m \in msgs do
        await /\ m.type = "Prepare"
              /\ m.bal > node[r].balMaxKnown;
        \* update states and reset statuses
        node[r].leader := m.src ||
        node[r].commitPrev := NumWrites + 1 ||
        node[r].balMaxKnown := m.bal ||
        node[r].insts :=
            [s \in Slots |->
                [node[r].insts[s]
                    EXCEPT !.status = IF @ = "Accepting"
                                        THEN "Preparing"
                                        ELSE @]] ||
        node[r].reads := {};
        \* send back PrepareReply with my voted list
        Send({PrepareReplyMsg(r, m.bal, VotesByNode(node[r]))});
        \* expire my old lease grant if any and grant to new leader
        \* remember that we simplify this spec by merging read quorum
        \* config change into leader change Prepares
        Lease(r, (CHOOSE g \in grants: g.from = m.src).config);
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
        \* marks this ballot as prepared and saves highest voted command
        \* in each slot if any
        node[r].balPrepared := node[r].balMaxKnown ||
        node[r].insts :=
            [s \in Slots |->
                [node[r].insts[s]
                    EXCEPT !.status = IF \/ @ = "Preparing"
                                         \/ /\ @ = "Empty"
                                            /\ PeakVotedWrite(prs, s) # "nil"
                                        THEN "Accepting"
                                        ELSE @,
                           !.write  = PeakVotedWrite(prs, s)]] ||
        node[r].commitPrev := LastTouchedSlot(prs);
        \* send AcceptWrite messages for in-progress instances and reply to
        \* myself instantly; send PrepareNotice as well
        Send(UNION
             {{AcceptWriteMsg(r, node[r].balPrepared, s,
                              node[r].insts[s].write),
               AcceptWriteReplyMsg(r, node[r].balPrepared, s)}:
              s \in {s \in Slots: node[r].insts[s].status = "Accepting"}}
        \cup {PrepareNoticeMsg(r, node[r].balPrepared,
                               LastTouchedSlot(prs))});
    end with;
end macro;

\* Follower receives PrepareNotice from a prepared and recovered leader, and
\* updates its commitPrev accordingly.
macro HandlePrepareNotice(r) begin
    \* if I'm a follower waiting on CommitNotice
    await /\ ThinkAmFollower(r)
          /\ node[r].commitPrev = NumWrites + 1;
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
    await /\ ThinkAmLeader(r)
          /\ \E s \in Slots: node[r].insts[s].status = "Empty"
          /\ Len(UnseenPending(r)) > 0
          /\ Head(UnseenPending(r)) \in Writes;
    \* find the next empty slot and pick a pending request
    with s = FirstEmptySlot(node[r].insts),
         c = Head(UnseenPending(r))
                \* W.L.O.G., only pick a command not seen in current
                \* prepared log to have smaller state space; in practice,
                \* duplicated client requests should be treated by some
                \* idempotency mechanism such as using request IDs
    do
        \* update slot status and voted
        node[r].insts[s].status := "Accepting" ||
        node[r].insts[s].write := c ||
        node[r].insts[s].voted.bal := node[r].balPrepared ||
        node[r].insts[s].voted.write := c;
        \* broadcast AcceptWrite and reply to myself instantly
        Send({AcceptWriteMsg(r, node[r].balPrepared, s, c),
              AcceptWriteReplyMsg(r, node[r].balPrepared, s)});
        \* append to observed events sequence if haven't yet
        Observe(<<ReqEvent(c)>>);
    end with;
end macro;

\* Replica replies to an AcceptWrite message.
macro HandleAcceptWrite(r) begin
    \* if I'm a follower
    await ThinkAmFollower(r);
    \* if receiving an unreplied AcceptWrite message with valid ballot
    with m \in msgs do
        await /\ m.type = "AcceptWrite"
              /\ m.bal >= node[r].balMaxKnown
              /\ m.bal >= node[r].insts[m.slot].voted.bal;
        \* update node states and corresponding instance's states
        node[r].leader := m.src ||
        node[r].balMaxKnown := m.bal ||
        node[r].insts[m.slot].status := "Accepting" ||
        node[r].insts[m.slot].write := m.write ||
        node[r].insts[m.slot].voted.bal := m.bal ||
        node[r].insts[m.slot].voted.write := m.write;
        \* send back AcceptWriteReply
        Send({AcceptWriteReplyMsg(r, m.bal, m.slot)});
    end with;
end macro;

\* Leader gathers AcceptWriteReply messages for a slot until condition met,
\* then marks the slot as committed and acknowledges the client.
macro HandleAcceptWriteReplies(r) begin
    \* if I'm a prepared leader
    await /\ ThinkAmLeader(r)
          /\ node[r].commitUpTo < NumWrites
          /\ node[r].insts[node[r].commitUpTo+1].status = "Accepting";
                \* W.L.O.G., only enabling the next slot after commitUpTo
                \* here to make the body of this macro simpler; in practice,
                \* messages are received proactively and there should be a
                \* separate "Executed" status
    \* for this slot, when there are enough number of AcceptReplies
    with s = node[r].commitUpTo + 1,
         c = node[r].insts[s].write,
         ls = s - 1,
         v = IF ls = 0 THEN "nil" ELSE node[r].insts[ls].write,
         ars = {m \in msgs: /\ m.type = "AcceptWriteReply"
                            /\ m.slot = s
                            /\ m.bal = node[r].balPrepared},
         wqSize = (Population + 1) - CurrentConfig.rqSize
    do
        await Cardinality({ar.src: ar \in ars}) >= wqSize;
        \* marks this slot as committed and apply command
        node[r].insts[s].status := "Committed" ||
        node[r].commitUpTo := s;
        \* append to observed events sequence if haven't yet, and remove
        \* the command from pending
        Observe(<<AckEvent(c, v)>>);
        Resolve(c);
        \* broadcast CommitNotice to followers
        Send({CommitNoticeMsg(s)});
    end with;
end macro;

\* Replica receives new commit notification.
macro HandleCommitNotice(r) begin
    \* if I'm a follower waiting on CommitNotice
    await /\ ThinkAmFollower(r)
          /\ node[r].commitUpTo < NumWrites
          /\ node[r].insts[node[r].commitUpTo+1].status = "Accepting";
                \* W.L.O.G., only enabling the next slot after commitUpTo
                \* here to make the body of this macro simpler
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

\* A prepared leader takes a new read request and serves it locally.
macro TakeNewReadRequestAsLeader(r) begin
    \* if I'm a prepared and recovered leader that has committed all slots
    \* of old ballots, and there's pending read request
    await /\ ThinkAmLeader(r)
          /\ BallotTransfered(r)
          /\ Len(UnseenPending(r)) > 0
          /\ Head(UnseenPending(r)) \in Reads;
    \* find the next empty slot and pick a pending request
    with s = node[r].commitUpTo,
         v = IF s = 0 THEN "nil" ELSE node[r].insts[s].write,
         c = Head(UnseenPending(r))
                \* W.L.O.G., only pick a command not seen in current
                \* prepared log to have smaller state space; in practice,
                \* duplicated client requests should be treated by some
                \* idempotency mechanism such as using request IDs
    do
        \* acknowledge client directly with the latest committed value, and
        \* remove the command from pending
        Observe(<<ReqEvent(c), AckEvent(c, v)>>);
        Resolve(c);
    end with;
end macro;

\* A follower takes a new read request and serves it by doing nearby read.
macro TakeNewReadRequestAsFollower(r) begin
    \* if I'm a caught-up follower
    await /\ ThinkAmFollower(r)
          /\ BallotTransfered(r)
          /\ Len(UnseenPending(r)) > 0
          /\ Head(UnseenPending(r)) \in Reads;
    \* pick a pending request; examine my log and find the last non-empty
    \* slot, check its status
    with s = LastNonEmptySlot(node[r].insts),
         cm = IF s = 0 THEN TRUE ELSE node[r].insts[s].status = "Committed",
         v = IF s = 0 THEN "nil" ELSE node[r].insts[s].write,
         c = Head(UnseenPending(r))
                \* W.L.O.G., only pick a command not seen in current
                \* prepared log to have smaller state space; in practice,
                \* duplicated client requests should be treated by some
                \* idempotency mechanism such as using request IDs
    do
        \* broadcast NearbyRead messages and instantly reply to myself
        Send({NearbyReadMsg(r, node[r].balMaxKnown, c),
              NearbyReadReplyMsg(r, node[r].balMaxKnown, c, FALSE, s, cm, v)});
        \* add to the set of on-the-fly reads
        node[r].reads := @ \cup {c};
        \* append to observed events sequence if haven't yet
        Observe(<<ReqEvent(c)>>);
    end with;
end macro;

\* Leader replies to a NearbyRead message by directly using the last
\* committed slot's value.
macro HandleNearbyReadAsLeader(r) begin
    \* if I'm a prepared and recovered leader that has committed all slots
    \* of old ballots
    await /\ ThinkAmLeader(r)
          /\ BallotTransfered(r);
    \* if receiving an unreplied NearbyRead message with valid ballot
    with m \in msgs,
         s = node[r].commitUpTo,
         v = IF s = 0 THEN "nil" ELSE node[r].insts[s].write
    do
        await /\ m.type = "NearbyRead"
              /\ m.bal = node[r].balMaxKnown;
        \* send back NearbyReadReply
        Send({NearbyReadReplyMsg(r, m.bal, m.read, TRUE, s, TRUE, v)});
    end with;
end macro;

\* Follower replies to a NearbyRead message with its last non-empty slot.
macro HandleNearbyReadAsFollower(r) begin
    \* if I'm a caught-up follower
    await /\ ThinkAmFollower(r)
          /\ BallotTransfered(r);
    \* if receiving an unreplied NearbyRead message with valid ballot
    with m \in msgs,
         s = LastNonEmptySlot(node[r].insts),
         cm = IF s = 0 THEN TRUE ELSE node[r].insts[s].status = "Committed",
         v = IF s = 0 THEN "nil" ELSE node[r].insts[s].write
    do
        await /\ m.type = "NearbyRead"
              /\ m.bal = node[r].balMaxKnown;
        \* send back NearbyReadReply
        Send({NearbyReadReplyMsg(r, m.bal, m.read, FALSE, s, cm, v)});
    end with;
end macro;

\* Follower gathers NearbyReadReply messages for a read request and finds
\* that the leader's reply is received.
macro HandleNearbyReadReplyFromLeader(r) begin
    \* if I'm a caught-up follower
    await /\ ThinkAmFollower(r)
          /\ BallotTransfered(r);
    \* for an on-the-fly read, when the leader's reply is received
    with c \in node[r].reads,
         nr \in {m \in msgs: /\ m.type = "NearbyReadReply"
                             /\ m.read = c
                             /\ m.bal = node[r].balMaxKnown
                             /\ m.src = node[r].leader
                             /\ m.is_leader}
    do
        \* append to observed events sequence if haven't yet, and remove
        \* the command from pending
        Observe(<<AckEvent(c, nr.val)>>);
        Resolve(c);
        \* remove from the set of on-the-fly reads
        node[r].reads := @ \ {c};
    end with;
end macro;

\* Follower gathers NearbyReadReply messages for a read request and finds
\* that leader is not reached yet but a good read quorum has been formed.
macro HandleNearbyReadRepliesNoLeader(r) begin
    \* if I'm a caught-up follower
    await /\ ThinkAmFollower(r)
          /\ BallotTransfered(r);
    \* for an on-the-fly read, when a good read quorum has been formed,
    \* the value with the latest slot number can be safetly acked
    with c \in node[r].reads,
         nrs = {m \in msgs: /\ m.type = "NearbyReadReply"
                            /\ m.read = c
                            /\ m.bal = node[r].balMaxKnown
                            /\ ~m.is_leader},
                    \* due to the design of this spec, there might be
                    \* multiple NearbyReadReplies from the same replica for
                    \* the same read command in the `msgs` set
         lnr = CHOOSE lnr \in nrs:
                    /\ \A nr \in nrs: nr.last_slot =< lnr.last_slot
                    /\ (~lnr.committed) => \A nr \in nrs:
                                                \/ nr.last_slot < lnr.last_slot
                                                \/ ~nr.committed,
         rqSize = CurrentConfig.rqSize
    do
        await /\ Cardinality({nr.src: nr \in nrs}) >= rqSize
              /\ lnr.committed;
                    \* the "committed" condition check on the lastest version
                    \* is necessary when write requests are not assumed to
                    \* last forever until acknowledged (they usually don't in
                    \* practice; but in this spec, user requests are modeled
                    \* as a sequence of pending requests and a write request,
                    \* once taken, lasts until it's acknowledged)
                    \*   to get a counter-example to this condition, we would
                    \* need >= 5 replicas and a modeling where a different
                    \* write request could be issued by a new leader and take
                    \* the position of an old uncommitted write
        \* append to observed events sequence if haven't yet, and remove
        \* the command from pending
        Observe(<<AckEvent(c, lnr.val)>>);
        Resolve(c);
        \* remove from the set of on-the-fly reads in anchored slot
        node[r].reads := @ \ {c};
    end with;
end macro;

\* Replica node crashes itself under promised conditions.
macro ReplicaCrashes(r) begin
    \* if less than (N - majority) number of replicas have failed
    await /\ MajorityNum + numCrashed < Cardinality(Replicas)
          /\ ~crashed[r]
          /\ node[r].balMaxKnown < MaxBallot;
                \* this clause is needed only because we have an upper
                \* bound ballot number for modeling checking; in practice
                \* someone else could always come up with a higher ballot
    \* mark myself as crashed
    crashed[r] := TRUE;
end macro;

\* Replica server node main loop.
process Replica \in Replicas
begin
    rloop: while (~terminated) /\ (~crashed[self]) do
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
            HandleAcceptWrite(self); 
        or
            HandleAcceptWriteReplies(self);
        or
            HandleCommitNotice(self);
        or
            TakeNewReadRequestAsLeader(self);
        or
            TakeNewReadRequestAsFollower(self);
        or
            HandleNearbyReadAsLeader(self);
        or
            HandleNearbyReadAsFollower(self);
        or
            HandleNearbyReadReplyFromLeader(self);
        or
            HandleNearbyReadRepliesNoLeader(self);
        or
            if NodeFailuresOn then
                ReplicaCrashes(self);
            end if;
        end either;
    end while;
end process;

end algorithm; *)

----------

\* BEGIN TRANSLATION (chksum(pcal) = "67acf5e1" /\ chksum(tla) = "74396425")
VARIABLES msgs, grants, node, pending, observed, crashed, pc

(* define statement *)
CurrentConfig ==
    LET leased(b) == Cardinality({g \in grants:
                                  g.config.bal = b}) >= MajorityNum
    IN  IF ~\E b \in Ballots: leased(b)
            THEN Config(0, "none", 0)
            ELSE (CHOOSE g \in grants: leased(g.config.bal)).config



ThinkAmLeader(r) == /\ node[r].leader = r
                    /\ node[r].balPrepared = node[r].balMaxKnown
                    /\ CurrentConfig.bal > 0
                    /\ CurrentConfig.bal = node[r].balMaxKnown
                    /\ CurrentConfig.leader = r

ThinkAmFollower(r) == /\ node[r].leader # r
                      /\ CurrentConfig.bal > 0
                      /\ CurrentConfig.bal = node[r].balMaxKnown
                      /\ CurrentConfig.leader # r

BallotTransfered(r) == node[r].commitUpTo >= node[r].commitPrev

reqsMade == {e.cmd: e \in {e \in Range(observed): e.type = "Req"}}

acksRecv == {e.cmd: e \in {e \in Range(observed): e.type = "Ack"}}

AppendObserved(seq) ==
    LET filter(e) == IF e.type = "Req" THEN e.cmd \notin reqsMade
                                       ELSE e.cmd \notin acksRecv
    IN  observed \o SelectSeq(seq, filter)

UnseenPending(r) ==
    LET filter(c) ==
            /\ \A s \in Slots: node[r].insts[s].write # c
            /\ c \notin node[r].reads
    IN  SelectSeq(pending, filter)

RemovePending(cmd) ==
    LET filter(c) == c # cmd
    IN  SelectSeq(pending, filter)

terminated == /\ Len(pending) = 0
              /\ Cardinality(reqsMade) = NumCommands
              /\ Cardinality(acksRecv) = NumCommands

numCrashed == Cardinality({r \in Replicas: crashed[r]})


vars == << msgs, grants, node, pending, observed, crashed, pc >>

ProcSet == (Replicas)

Init == (* Global variables *)
        /\ msgs = {}
        /\ grants = {}
        /\ node = [r \in Replicas |-> NullNode]
        /\ pending = InitPending
        /\ observed = <<>>
        /\ crashed = [r \in Replicas |-> FALSE]
        /\ pc = [self \in ProcSet |-> "rloop"]

rloop(self) == /\ pc[self] = "rloop"
               /\ IF (~terminated) /\ (~crashed[self])
                     THEN /\ \/ /\ node[self].leader # self
                                /\ \E b \in Ballots:
                                     \E rq \in (1+numCrashed)..MajorityNum:
                                       /\ /\ b > node[self].balMaxKnown
                                          /\ ~\E m \in msgs: (m.type = "Prepare") /\ (m.bal = b)
                                       /\ node' = [node EXCEPT ![self].leader = self,
                                                               ![self].commitPrev = NumWrites + 1,
                                                               ![self].balPrepared = 0,
                                                               ![self].balMaxKnown = b,
                                                               ![self].insts = [s \in Slots |->
                                                                                   [node[self].insts[s]
                                                                                       EXCEPT !.status = IF @ = "Accepting"
                                                                                                           THEN "Preparing"
                                                                                                           ELSE @]],
                                                               ![self].reads = {}]
                                       /\ msgs' = (msgs \cup ({PrepareMsg(self, b),
                                                               PrepareReplyMsg(self, b, VotesByNode(node'[self]))}))
                                       /\ grants' = ({g \in grants: g.from # self} \cup {LeaseGrant(self, (Config(b, self, rq)))})
                                /\ UNCHANGED <<pending, observed, crashed>>
                             \/ /\ \E m \in msgs:
                                     /\ /\ m.type = "Prepare"
                                        /\ m.bal > node[self].balMaxKnown
                                     /\ node' = [node EXCEPT ![self].leader = m.src,
                                                             ![self].commitPrev = NumWrites + 1,
                                                             ![self].balMaxKnown = m.bal,
                                                             ![self].insts = [s \in Slots |->
                                                                                 [node[self].insts[s]
                                                                                     EXCEPT !.status = IF @ = "Accepting"
                                                                                                         THEN "Preparing"
                                                                                                         ELSE @]],
                                                             ![self].reads = {}]
                                     /\ msgs' = (msgs \cup ({PrepareReplyMsg(self, m.bal, VotesByNode(node'[self]))}))
                                     /\ grants' = ({g \in grants: g.from # self} \cup {LeaseGrant(self, ((CHOOSE g \in grants: g.from = m.src).config))})
                                /\ UNCHANGED <<pending, observed, crashed>>
                             \/ /\ /\ node[self].leader = self
                                   /\ node[self].balPrepared = 0
                                /\ LET prs == {m \in msgs: /\ m.type = "PrepareReply"
                                                           /\ m.bal = node[self].balMaxKnown} IN
                                     /\ Cardinality({pr.src: pr \in prs}) >= MajorityNum
                                     /\ node' = [node EXCEPT ![self].balPrepared = node[self].balMaxKnown,
                                                             ![self].insts = [s \in Slots |->
                                                                                 [node[self].insts[s]
                                                                                     EXCEPT !.status = IF \/ @ = "Preparing"
                                                                                                          \/ /\ @ = "Empty"
                                                                                                             /\ PeakVotedWrite(prs, s) # "nil"
                                                                                                         THEN "Accepting"
                                                                                                         ELSE @,
                                                                                            !.write  = PeakVotedWrite(prs, s)]],
                                                             ![self].commitPrev = LastTouchedSlot(prs)]
                                     /\ msgs' = (msgs \cup (     UNION
                                                                 {{AcceptWriteMsg(self, node'[self].balPrepared, s,
                                                                                  node'[self].insts[s].write),
                                                                   AcceptWriteReplyMsg(self, node'[self].balPrepared, s)}:
                                                                  s \in {s \in Slots: node'[self].insts[s].status = "Accepting"}}
                                                 \cup {PrepareNoticeMsg(self, node'[self].balPrepared,
                                                                        LastTouchedSlot(prs))}))
                                /\ UNCHANGED <<grants, pending, observed, crashed>>
                             \/ /\ /\ ThinkAmFollower(self)
                                   /\ node[self].commitPrev = NumWrites + 1
                                /\ \E m \in msgs:
                                     /\ /\ m.type = "PrepareNotice"
                                        /\ m.bal = node[self].balMaxKnown
                                     /\ node' = [node EXCEPT ![self].commitPrev = m.commit_prev]
                                /\ UNCHANGED <<msgs, grants, pending, observed, crashed>>
                             \/ /\ /\ ThinkAmLeader(self)
                                   /\ \E s \in Slots: node[self].insts[s].status = "Empty"
                                   /\ Len(UnseenPending(self)) > 0
                                   /\ Head(UnseenPending(self)) \in Writes
                                /\ LET s == FirstEmptySlot(node[self].insts) IN
                                     LET c == Head(UnseenPending(self)) IN
                                       /\ node' = [node EXCEPT ![self].insts[s].status = "Accepting",
                                                               ![self].insts[s].write = c,
                                                               ![self].insts[s].voted.bal = node[self].balPrepared,
                                                               ![self].insts[s].voted.write = c]
                                       /\ msgs' = (msgs \cup ({AcceptWriteMsg(self, node'[self].balPrepared, s, c),
                                                               AcceptWriteReplyMsg(self, node'[self].balPrepared, s)}))
                                       /\ observed' = AppendObserved((<<ReqEvent(c)>>))
                                /\ UNCHANGED <<grants, pending, crashed>>
                             \/ /\ ThinkAmFollower(self)
                                /\ \E m \in msgs:
                                     /\ /\ m.type = "AcceptWrite"
                                        /\ m.bal >= node[self].balMaxKnown
                                        /\ m.bal >= node[self].insts[m.slot].voted.bal
                                     /\ node' = [node EXCEPT ![self].leader = m.src,
                                                             ![self].balMaxKnown = m.bal,
                                                             ![self].insts[m.slot].status = "Accepting",
                                                             ![self].insts[m.slot].write = m.write,
                                                             ![self].insts[m.slot].voted.bal = m.bal,
                                                             ![self].insts[m.slot].voted.write = m.write]
                                     /\ msgs' = (msgs \cup ({AcceptWriteReplyMsg(self, m.bal, m.slot)}))
                                /\ UNCHANGED <<grants, pending, observed, crashed>>
                             \/ /\ /\ ThinkAmLeader(self)
                                   /\ node[self].commitUpTo < NumWrites
                                   /\ node[self].insts[node[self].commitUpTo+1].status = "Accepting"
                                /\ LET s == node[self].commitUpTo + 1 IN
                                     LET c == node[self].insts[s].write IN
                                       LET ls == s - 1 IN
                                         LET v == IF ls = 0 THEN "nil" ELSE node[self].insts[ls].write IN
                                           LET ars == {m \in msgs: /\ m.type = "AcceptWriteReply"
                                                                   /\ m.slot = s
                                                                   /\ m.bal = node[self].balPrepared} IN
                                             LET wqSize == (Population + 1) - CurrentConfig.rqSize IN
                                               /\ Cardinality({ar.src: ar \in ars}) >= wqSize
                                               /\ node' = [node EXCEPT ![self].insts[s].status = "Committed",
                                                                       ![self].commitUpTo = s]
                                               /\ observed' = AppendObserved((<<AckEvent(c, v)>>))
                                               /\ pending' = RemovePending(c)
                                               /\ msgs' = (msgs \cup ({CommitNoticeMsg(s)}))
                                /\ UNCHANGED <<grants, crashed>>
                             \/ /\ /\ ThinkAmFollower(self)
                                   /\ node[self].commitUpTo < NumWrites
                                   /\ node[self].insts[node[self].commitUpTo+1].status = "Accepting"
                                /\ LET s == node[self].commitUpTo + 1 IN
                                     LET c == node[self].insts[s].write IN
                                       \E m \in msgs:
                                         /\ /\ m.type = "CommitNotice"
                                            /\ m.upto = s
                                         /\ node' = [node EXCEPT ![self].insts[s].status = "Committed",
                                                                 ![self].commitUpTo = s]
                                /\ UNCHANGED <<msgs, grants, pending, observed, crashed>>
                             \/ /\ /\ ThinkAmLeader(self)
                                   /\ BallotTransfered(self)
                                   /\ Len(UnseenPending(self)) > 0
                                   /\ Head(UnseenPending(self)) \in Reads
                                /\ LET s == node[self].commitUpTo IN
                                     LET v == IF s = 0 THEN "nil" ELSE node[self].insts[s].write IN
                                       LET c == Head(UnseenPending(self)) IN
                                         /\ observed' = AppendObserved((<<ReqEvent(c), AckEvent(c, v)>>))
                                         /\ pending' = RemovePending(c)
                                /\ UNCHANGED <<msgs, grants, node, crashed>>
                             \/ /\ /\ ThinkAmFollower(self)
                                   /\ BallotTransfered(self)
                                   /\ Len(UnseenPending(self)) > 0
                                   /\ Head(UnseenPending(self)) \in Reads
                                /\ LET s == LastNonEmptySlot(node[self].insts) IN
                                     LET cm == IF s = 0 THEN TRUE ELSE node[self].insts[s].status = "Committed" IN
                                       LET v == IF s = 0 THEN "nil" ELSE node[self].insts[s].write IN
                                         LET c == Head(UnseenPending(self)) IN
                                           /\ msgs' = (msgs \cup ({NearbyReadMsg(self, node[self].balMaxKnown, c),
                                                                   NearbyReadReplyMsg(self, node[self].balMaxKnown, c, FALSE, s, cm, v)}))
                                           /\ node' = [node EXCEPT ![self].reads = @ \cup {c}]
                                           /\ observed' = AppendObserved((<<ReqEvent(c)>>))
                                /\ UNCHANGED <<grants, pending, crashed>>
                             \/ /\ /\ ThinkAmLeader(self)
                                   /\ BallotTransfered(self)
                                /\ \E m \in msgs:
                                     LET s == node[self].commitUpTo IN
                                       LET v == IF s = 0 THEN "nil" ELSE node[self].insts[s].write IN
                                         /\ /\ m.type = "NearbyRead"
                                            /\ m.bal = node[self].balMaxKnown
                                         /\ msgs' = (msgs \cup ({NearbyReadReplyMsg(self, m.bal, m.read, TRUE, s, TRUE, v)}))
                                /\ UNCHANGED <<grants, node, pending, observed, crashed>>
                             \/ /\ /\ ThinkAmFollower(self)
                                   /\ BallotTransfered(self)
                                /\ \E m \in msgs:
                                     LET s == LastNonEmptySlot(node[self].insts) IN
                                       LET cm == IF s = 0 THEN TRUE ELSE node[self].insts[s].status = "Committed" IN
                                         LET v == IF s = 0 THEN "nil" ELSE node[self].insts[s].write IN
                                           /\ /\ m.type = "NearbyRead"
                                              /\ m.bal = node[self].balMaxKnown
                                           /\ msgs' = (msgs \cup ({NearbyReadReplyMsg(self, m.bal, m.read, FALSE, s, cm, v)}))
                                /\ UNCHANGED <<grants, node, pending, observed, crashed>>
                             \/ /\ /\ ThinkAmFollower(self)
                                   /\ BallotTransfered(self)
                                /\ \E c \in node[self].reads:
                                     \E nr \in {m \in msgs: /\ m.type = "NearbyReadReply"
                                                            /\ m.read = c
                                                            /\ m.bal = node[self].balMaxKnown
                                                            /\ m.src = node[self].leader
                                                            /\ m.is_leader}:
                                       /\ observed' = AppendObserved((<<AckEvent(c, nr.val)>>))
                                       /\ pending' = RemovePending(c)
                                       /\ node' = [node EXCEPT ![self].reads = @ \ {c}]
                                /\ UNCHANGED <<msgs, grants, crashed>>
                             \/ /\ /\ ThinkAmFollower(self)
                                   /\ BallotTransfered(self)
                                /\ \E c \in node[self].reads:
                                     LET nrs == {m \in msgs: /\ m.type = "NearbyReadReply"
                                                             /\ m.read = c
                                                             /\ m.bal = node[self].balMaxKnown
                                                             /\ ~m.is_leader} IN
                                       LET lnr == CHOOSE lnr \in nrs:
                                                       /\ \A nr \in nrs: nr.last_slot =< lnr.last_slot
                                                       /\ (~lnr.committed) => \A nr \in nrs:
                                                                                   \/ nr.last_slot < lnr.last_slot
                                                                                   \/ ~nr.committed IN
                                         LET rqSize == CurrentConfig.rqSize IN
                                           /\ /\ Cardinality({nr.src: nr \in nrs}) >= rqSize
                                              /\ lnr.committed
                                           /\ observed' = AppendObserved((<<AckEvent(c, lnr.val)>>))
                                           /\ pending' = RemovePending(c)
                                           /\ node' = [node EXCEPT ![self].reads = @ \ {c}]
                                /\ UNCHANGED <<msgs, grants, crashed>>
                             \/ /\ IF NodeFailuresOn
                                      THEN /\ /\ MajorityNum + numCrashed < Cardinality(Replicas)
                                              /\ ~crashed[self]
                                              /\ node[self].balMaxKnown < MaxBallot
                                           /\ crashed' = [crashed EXCEPT ![self] = TRUE]
                                      ELSE /\ TRUE
                                           /\ UNCHANGED crashed
                                /\ UNCHANGED <<msgs, grants, node, pending, observed>>
                          /\ pc' = [pc EXCEPT ![self] = "rloop"]
                     ELSE /\ pc' = [pc EXCEPT ![self] = "Done"]
                          /\ UNCHANGED << msgs, grants, node, pending, 
                                          observed, crashed >>

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
