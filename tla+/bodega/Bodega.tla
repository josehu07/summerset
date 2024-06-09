(**********************************************************************************)
(* Bodega protocol enabling always-local follower reads in WAN-scale consensus by *)
(* employing a lease-less always-local follower read technique on the critical    *)
(* path and using off-the-critical-path config leases to retain fault-tolerance.  *)
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
                                     val: {"nil"} \cup Writes,
                                     by: Replicas]

ReqEvent(c) == [type |-> "Req", cmd |-> c]

AckEvent(c, v, n) == [type |-> "Ack", cmd |-> c, val |-> v, by |-> n]
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
               insts: [Slots -> InstStates]]

NullNode == [leader |-> "none",
             commitUpTo |-> 0,
             commitPrev |-> 0,
             balPrepared |-> 0,
             balMaxKnown |-> 0,
             insts |-> [s \in Slots |-> NullInst]]
                \* commitPrev is the last slot which might have been
                \* committed by an old leader; a newly prepared leader
                \* can safely serve reads locally only after its log has
                \* been committed up to this slot. The time before this
                \* condition becomes satisfied may be considered the
                \* "recovery" or "ballot transfer" time

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
                                    \* no need to carry command ID in
                                    \* AcceptReply because ballot and
                                    \* slot uniquely identifies the write

CommitNoticeMsgs == [type: {"CommitNotice"}, upto: Slots]

CommitNoticeMsg(u) == [type |-> "CommitNotice", upto |-> u]

Messages ==      PrepareMsgs
            \cup PrepareReplyMsgs
            \cup PrepareNoticeMsgs
            \cup AcceptMsgs
            \cup AcceptReplyMsgs
            \cup CommitNoticeMsgs

\* Config lease related typedefs.
Configs == {cfg \in [bal: Ballots, leader: Replicas, responders: SUBSET Replicas]:
            cfg.leader \notin cfg.responders}

Config(b, l, resps) == [bal |-> b, leader |-> l, responders |-> resps]
                        \* each new ballot number maps to a new config; this
                        \* includes the change of leader (as in classic
                        \* MultiPaxos) and/or the change of who're responders

LeaseGrants == [from: Replicas, config: Configs]

LeaseGrant(f, cfg) == [from |-> f, config |-> cfg]
                        \* this is the only type of message that may be
                        \* "removed" from the global set of messages to make
                        \* a "cheated" model of leasing: if a LeaseGrant
                        \* message is removed, it means that promise has
                        \* expired and the grantor did not refresh, probably
                        \* making way for switching to a differnt config

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
    
    ThinkAmResponder(r) == /\ ThinkAmFollower(r)
                           /\ r \in CurrentConfig.responders

    BallotTransfered(r) == node[r].commitUpTo >= node[r].commitPrev

    WriteCommittable(ars) ==
        /\ Cardinality({ar.src: ar \in ars}) >= MajorityNum
        /\ CurrentConfig.responders \subseteq {ar.src: ar \in ars}

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
\* To simplify this spec W.L.O.G., we change the responders config only when
\* a new leader steps up; in practice, a separate and independent type of
\* trigger will be used to change the config.
macro BecomeLeader(r) begin
    \* if I'm not a current leader
    await node[r].leader # r;
    \* pick a greater ballot number and a config
    with b \in Ballots,
         resps \in SUBSET {f \in Replicas: f # r},
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
                                        ELSE @]];
        \* broadcast Prepare and reply to myself instantly
        Send({PrepareMsg(r, b),
              PrepareReplyMsg(r, b, VotesByNode(node[r]))});
        \* expire my old lease grant if any and grant to myself
        Lease(r, Config(b, r, resps));
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
                                        ELSE @]];
        \* send back PrepareReply with my voted list
        Send({PrepareReplyMsg(r, m.bal, VotesByNode(node[r]))});
        \* expire my old lease grant if any and grant to new leader
        \* remember that we simplify this spec by merging responders
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
        \* send Accept messages for in-progress instances and reply to myself
        \* instantly; send PrepareNotices as well
        Send(UNION
             {{AcceptMsg(r, node[r].balPrepared, s, node[r].insts[s].write),
               AcceptReplyMsg(r, node[r].balPrepared, s)}:
              s \in {s \in Slots: node[r].insts[s].status = "Accepting"}}
        \cup {PrepareNoticeMsg(r, node[r].balPrepared, LastTouchedSlot(prs))});
    end with;
end macro;

\* Follower receives PrepareNotice from a prepared and recovered leader, and
\* updates its commitPrev accordingly.
macro HandlePrepareNotice(r) begin
    \* if I'm a follower waiting on PrepareNotice
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
        \* broadcast Accept and reply to myself instantly
        Send({AcceptMsg(r, node[r].balPrepared, s, c),
              AcceptReplyMsg(r, node[r].balPrepared, s)});
        \* append to observed events sequence if haven't yet
        Observe(<<ReqEvent(c)>>);
    end with;
end macro;

\* Replica replies to an Accept message.
macro HandleAccept(r) begin
    \* if I'm a follower
    await ThinkAmFollower(r);
    \* if receiving an unreplied Accept message with valid ballot
    with m \in msgs do
        await /\ m.type = "Accept"
              /\ m.bal >= node[r].balMaxKnown
              /\ m.bal >= node[r].insts[m.slot].voted.bal;
        \* update node states and corresponding instance's states
        node[r].leader := m.src ||
        node[r].balMaxKnown := m.bal ||
        node[r].insts[m.slot].status := "Accepting" ||
        node[r].insts[m.slot].write := m.write ||
        node[r].insts[m.slot].voted.bal := m.bal ||
        node[r].insts[m.slot].voted.write := m.write;
        \* send back AcceptReply
        Send({AcceptReplyMsg(r, m.bal, m.slot)});
    end with;
end macro;

\* Leader gathers AcceptReply messages for a slot until condition met,
\* then marks the slot as committed and acknowledges the client.
macro HandleAcceptReplies(r) begin
    \* if I'm a prepared leader
    await /\ ThinkAmLeader(r)
          /\ node[r].commitUpTo < NumWrites
          /\ node[r].insts[node[r].commitUpTo+1].status = "Accepting";
                \* W.L.O.G., only enabling the next slot after commitUpTo
                \* here to make the body of this macro simpler; in practice,
                \* messages are received proactively and there should be a
                \* separate "Executed" status
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
        await WriteCommittable(ars);
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

\* A prepared leader or a responder follower takes a new read request and
\* serves it locally.
macro TakeNewReadRequest(r) begin
    \* if I'm a caught-up leader or responder follower
    await /\ \/ ThinkAmLeader(r)
             \/ ThinkAmResponder(r)
          /\ BallotTransfered(r)
          /\ Len(UnseenPending(r)) > 0
          /\ Head(UnseenPending(r)) \in Reads;
    \* pick a pending request; examine my log and find the last non-empty
    \* slot, check its status
    with s = LastNonEmptySlot(node[r].insts),
         v = IF s = 0 THEN "nil" ELSE node[r].insts[s].write,
         c = Head(UnseenPending(r))
                \* W.L.O.G., only pick a command not seen in current
                \* prepared log to have smaller state space; in practice,
                \* duplicated client requests should be treated by some
                \* idempotency mechanism such as using request IDs
    do
        \* if the latest value is in Committed status, can directly reply;
        \* otherwise, should hold until I've received enough broadcasted
        \* AcceptReplies indicating that the write is surely to be committed
        await \/ s = 0
              \/ node[r].insts[s].status = "Committed"
              \/ LET ars == {m \in msgs: /\ m.type = "AcceptReply"
                                         /\ m.slot = s
                                         /\ m.bal = node[r].balMaxKnown}
                 IN  WriteCommittable(ars);
        \* acknowledge client with the latest value, and remove the command
        \* from pending
        Observe(<<ReqEvent(c), AckEvent(c, v, r)>>);
        Resolve(c);
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
            HandleAccept(self); 
        or
            HandleAcceptReplies(self);
        or
            HandleCommitNotice(self);
        or
            TakeNewReadRequest(self);
        or
            if NodeFailuresOn then
                ReplicaCrashes(self);
            end if;
        end either;
    end while;
end process;

end algorithm; *)

----------

\* BEGIN TRANSLATION (chksum(pcal) = "ed44672" /\ chksum(tla) = "387844b7")
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

ThinkAmResponder(r) == /\ ThinkAmFollower(r)
                       /\ r \in CurrentConfig.responders

BallotTransfered(r) == node[r].commitUpTo >= node[r].commitPrev

WriteCommittable(ars) ==
    /\ Cardinality({ar.src: ar \in ars}) >= MajorityNum
    /\ CurrentConfig.responders \subseteq {ar.src: ar \in ars}

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
                                     \E resps \in SUBSET {f \in Replicas: f # self}:
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
                                                                                                           ELSE @]]]
                                       /\ msgs' = (msgs \cup ({PrepareMsg(self, b),
                                                               PrepareReplyMsg(self, b, VotesByNode(node'[self]))}))
                                       /\ grants' = ({g \in grants: g.from # self} \cup {LeaseGrant(self, (Config(b, self, resps)))})
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
                                                                                                         ELSE @]]]
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
                                                                 {{AcceptMsg(self, node'[self].balPrepared, s, node'[self].insts[s].write),
                                                                   AcceptReplyMsg(self, node'[self].balPrepared, s)}:
                                                                  s \in {s \in Slots: node'[self].insts[s].status = "Accepting"}}
                                                 \cup {PrepareNoticeMsg(self, node'[self].balPrepared, LastTouchedSlot(prs))}))
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
                                       /\ msgs' = (msgs \cup ({AcceptMsg(self, node'[self].balPrepared, s, c),
                                                               AcceptReplyMsg(self, node'[self].balPrepared, s)}))
                                       /\ observed' = AppendObserved((<<ReqEvent(c)>>))
                                /\ UNCHANGED <<grants, pending, crashed>>
                             \/ /\ ThinkAmFollower(self)
                                /\ \E m \in msgs:
                                     /\ /\ m.type = "Accept"
                                        /\ m.bal >= node[self].balMaxKnown
                                        /\ m.bal >= node[self].insts[m.slot].voted.bal
                                     /\ node' = [node EXCEPT ![self].leader = m.src,
                                                             ![self].balMaxKnown = m.bal,
                                                             ![self].insts[m.slot].status = "Accepting",
                                                             ![self].insts[m.slot].write = m.write,
                                                             ![self].insts[m.slot].voted.bal = m.bal,
                                                             ![self].insts[m.slot].voted.write = m.write]
                                     /\ msgs' = (msgs \cup ({AcceptReplyMsg(self, m.bal, m.slot)}))
                                /\ UNCHANGED <<grants, pending, observed, crashed>>
                             \/ /\ /\ ThinkAmLeader(self)
                                   /\ node[self].commitUpTo < NumWrites
                                   /\ node[self].insts[node[self].commitUpTo+1].status = "Accepting"
                                /\ LET s == node[self].commitUpTo + 1 IN
                                     LET c == node[self].insts[s].write IN
                                       LET ls == s - 1 IN
                                         LET v == IF ls = 0 THEN "nil" ELSE node[self].insts[ls].write IN
                                           LET ars == {m \in msgs: /\ m.type = "AcceptReply"
                                                                   /\ m.slot = s
                                                                   /\ m.bal = node[self].balPrepared} IN
                                             /\ WriteCommittable(ars)
                                             /\ node' = [node EXCEPT ![self].insts[s].status = "Committed",
                                                                     ![self].commitUpTo = s]
                                             /\ observed' = AppendObserved((<<AckEvent(c, v, self)>>))
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
                             \/ /\ /\ \/ ThinkAmLeader(self)
                                      \/ ThinkAmResponder(self)
                                   /\ BallotTransfered(self)
                                   /\ Len(UnseenPending(self)) > 0
                                   /\ Head(UnseenPending(self)) \in Reads
                                /\ LET s == LastNonEmptySlot(node[self].insts) IN
                                     LET v == IF s = 0 THEN "nil" ELSE node[self].insts[s].write IN
                                       LET c == Head(UnseenPending(self)) IN
                                         /\ \/ s = 0
                                            \/ node[self].insts[s].status = "Committed"
                                            \/ LET ars == {m \in msgs: /\ m.type = "AcceptReply"
                                                                       /\ m.slot = s
                                                                       /\ m.bal = node[self].balMaxKnown}
                                               IN  WriteCommittable(ars)
                                         /\ observed' = AppendObserved((<<ReqEvent(c), AckEvent(c, v, self)>>))
                                         /\ pending' = RemovePending(c)
                                /\ UNCHANGED <<msgs, grants, node, crashed>>
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
