(**********************************************************************************)
(* This spec extends multipaxos_smr_style/MultiPaxos.tla with extra features:     *)
(*   [x] Only keep writes in the log (reads squeeze in between writes)            *)
(*   [x] Asymmetric write/read quorum size                                        *)
(*   [ ] Stable leader lease and local read at leader                             *)
(**********************************************************************************)

---- MODULE MultiPaxos ----
EXTENDS FiniteSets, Sequences, Integers, TLC

(*******************************)
(* Model inputs & assumptions. *)
(*******************************)
CONSTANT Replicas,   \* symmetric set of server nodes
         Writes,     \* symmetric set of write commands (each w/ unique value)
         Reads,      \* symmetric set of read commands
         MaxBallot,  \* maximum ballot pickable for leader preemption
         ReadQuorumSize,  \* read quorum size to allow for asymmetry
         CommitNoticeOn,  \* if true, turn on CommitNotice messages
         NodeFailuresOn   \* if true, turn on node failures injection

ReplicasAssumption == /\ IsFiniteSet(Replicas)
                      /\ Cardinality(Replicas) >= 1

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

ReadQuorumSizeAssumption == /\ ReadQuorumSize \in Nat
                            /\ ReadQuorumSize >= 1
                            /\ ReadQuorumSize =< MajorityNum

WriteQuorumSize == (Population + 1) - ReadQuorumSize

CommitNoticeOnAssumption == CommitNoticeOn \in BOOLEAN

NodeFailuresOnAssumption == NodeFailuresOn \in BOOLEAN

ASSUME /\ ReplicasAssumption
       /\ WritesAssumption
       /\ ReadsAssumption
       /\ MaxBallotAssumption
       /\ ReadQuorumSizeAssumption
       /\ CommitNoticeOnAssumption
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
               balPrepared: {0} \cup Ballots,
               balMaxKnown: {0} \cup Ballots,
               insts: [Slots -> InstStates],
               reads: [Slots \cup {NumWrites+1} -> SUBSET Reads]]
                    \* reads is the set of read commands "anchored" at
                    \* each instance, i.e., reads that squeeze in between
                    \* an instance and its predecessor

NullNode == [leader |-> "none",
             commitUpTo |-> 0,
             balPrepared |-> 0,
             balMaxKnown |-> 0,
             insts |-> [s \in Slots |-> NullInst],
             reads |-> [s \in Slots \cup {NumWrites+1} |-> {}]]

FirstEmptySlot(insts) ==
    IF \A s \in Slots: insts[s].status # "Empty"
        THEN NumWrites + 1
        ELSE CHOOSE s \in Slots:
                /\ insts[s].status = "Empty"
                /\ \A t \in 1..(s-1): insts[t].status # "Empty"

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
                                \* AcceptReply because ballot and slot
                                \* uniquely identifies the write

DoReadMsgs == [type: {"DoRead"}, src: Replicas,
                                 bal: Ballots,
                                 slot: Slots \cup {NumWrites+1},
                                 read: Reads]

DoReadMsg(r, b, s, c) == [type |-> "DoRead", src |-> r,
                                             bal |-> b,
                                             slot |-> s,
                                             read |-> c]

DoReadReplyMsgs == [type: {"DoReadReply"}, src: Replicas,
                                           bal: Ballots,
                                           slot: Slots \cup {NumWrites+1},
                                           read: Reads]

DoReadReplyMsg(r, b, s, c) == [type |-> "DoReadReply", src |-> r,
                                                       bal |-> b,
                                                       slot |-> s,
                                                       read |-> c]
                                    \* read here is just a command ID

CommitNoticeMsgs == [type: {"CommitNotice"}, upto: Slots]

CommitNoticeMsg(u) == [type |-> "CommitNotice", upto |-> u]

Messages ==      PrepareMsgs
            \cup PrepareReplyMsgs
            \cup AcceptMsgs
            \cup AcceptReplyMsgs
            \cup DoReadMsgs
            \cup DoReadReplyMsgs
            \cup CommitNoticeMsgs

----------

(******************************)
(* Main algorithm in PlusCal. *)
(******************************)
(*--algorithm MultiPaxos

variable msgs = {},                             \* messages in the network
         node = [r \in Replicas |-> NullNode],  \* replica node state
         pending = InitPending,                 \* sequence of pending reqs
         observed = <<>>,                       \* client observed events
         crashed = [r \in Replicas |-> FALSE];  \* replica crashed flag

define
    UnseenPending(ns) ==
        LET filter(c) ==
                /\ \A s \in Slots: ns.insts[s].write # c
                /\ \A s \in Slots \cup {NumWrites+1}: c \notin ns.reads[s]
        IN  SelectSeq(pending, filter)
    
    RemovePending(cmd) ==
        LET filter(c) == c # cmd
        IN  SelectSeq(pending, filter)

    reqsMade == {e.cmd: e \in {e \in Range(observed): e.type = "Req"}}
    
    acksRecv == {e.cmd: e \in {e \in Range(observed): e.type = "Ack"}}

    terminated == /\ Len(pending) = 0
                  /\ Cardinality(reqsMade) = NumCommands
                  /\ Cardinality(acksRecv) = NumCommands
    
    numCrashed == Cardinality({r \in Replicas: crashed[r]})
end define;

\* Send a set of messages helper.
macro Send(set) begin
    msgs := msgs \cup set;
end macro;

\* Observe a client event helper.
macro Observe(e) begin
    if e \notin Range(observed) then
        observed := Append(observed, e);
    end if;
end macro;

\* Resolve a pending command helper.
macro Resolve(c) begin
    pending := RemovePending(c);
end macro;

\* Someone steps up as leader and sends Prepare message to followers.
macro BecomeLeader(r) begin
    \* if I'm not a leader
    await node[r].leader # r;
    \* pick a greater ballot number
    with b \in Ballots do
        await /\ b > node[r].balMaxKnown
              /\ ~\E m \in msgs: (m.type = "Prepare") /\ (m.bal = b);
                    \* W.L.O.G., using this clause to model that ballot
                    \* numbers from different proposers be unique
        \* update states and restart Prepare phase for in-progress instances
        node[r].leader := r ||
        node[r].balPrepared := 0 ||
        node[r].balMaxKnown := b ||
        node[r].insts :=
            [s \in Slots |->
                [node[r].insts[s]
                    EXCEPT !.status = IF @ = "Accepting"
                                        THEN "Preparing"
                                        ELSE @]] ||
        node[r].reads :=
            [s \in Slots \cup {NumWrites+1} |-> {}];
        \* broadcast Prepare and reply to myself instantly
        Send({PrepareMsg(r, b),
              PrepareReplyMsg(r, b, VotesByNode(node[r]))});
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
        node[r].balMaxKnown := m.bal ||
        node[r].insts :=
            [s \in Slots |->
                [node[r].insts[s]
                    EXCEPT !.status = IF @ = "Accepting"
                                        THEN "Preparing"
                                        ELSE @]];
        \* send back PrepareReply with my voted list
        Send({PrepareReplyMsg(r, m.bal, VotesByNode(node[r]))});
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
        await Cardinality(prs) >= MajorityNum;
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
                           !.write  = PeakVotedWrite(prs, s)]];
        \* send Accept messages for in-progress instances and reply to
        \* myself instantly
        Send(UNION
             {{AcceptMsg(r, node[r].balPrepared, s, node[r].insts[s].write),
               AcceptReplyMsg(r, node[r].balPrepared, s)}:
              s \in {s \in Slots: node[r].insts[s].status = "Accepting"}});
    end with;
end macro;

\* A prepared leader takes a new write request into the next empty slot.
macro TakeNewWriteRequest(r) begin
    \* if I'm a prepared leader and there's pending write request
    await /\ node[r].leader = r
          /\ node[r].balPrepared = node[r].balMaxKnown
          /\ \E s \in Slots: node[r].insts[s].status = "Empty"
          /\ Len(UnseenPending(node[r])) > 0
          /\ Head(UnseenPending(node[r])) \in Writes;
    \* find the next empty slot and pick a pending request
    with s = FirstEmptySlot(node[r].insts),
         c = Head(UnseenPending(node[r]))
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
        Observe(ReqEvent(c));
    end with;
end macro;

\* A prepared leader takes a new read request and anchor it to the next
\* empty slot.
macro TakeNewReadRequest(r) begin
    \* if I'm a prepared leader and there's pending read request
    await /\ node[r].leader = r
          /\ node[r].balPrepared = node[r].balMaxKnown
          /\ Len(UnseenPending(node[r])) > 0
          /\ Head(UnseenPending(node[r])) \in Reads;
    \* pick a pending request and use the slot right after last committed
    with s = FirstEmptySlot(node[r].insts),
         c = Head(UnseenPending(node[r]))
                \* W.L.O.G., only pick a command not seen in current
                \* prepared log to have smaller state space; in practice,
                \* duplicated client requests should be treated by some
                \* idempotency mechanism such as using request IDs
    do
        \* broadcast DoRead and reply to myself instantly
        Send({DoReadMsg(r, node[r].balPrepared, s, c),
              DoReadReplyMsg(r, node[r].balPrepared, s, c)});
        \* add to the set of on-the-fly reads anchored at this slot
        node[r].reads[s] := @ \cup {c};
        \* append to observed events sequence if haven't yet
        Observe(ReqEvent(c));
    end with;
end macro;
    
\* Replica replies to an Accept message.
macro HandleAccept(r) begin
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

\* Leader gathers AcceptReply messages for a slot until condition met, then
\* marks the slot as committed and acknowledges the client.
macro HandleAcceptReplies(r) begin
    \* if I think I'm a current leader
    await /\ node[r].leader = r
          /\ node[r].balPrepared = node[r].balMaxKnown
          /\ node[r].commitUpTo < NumWrites
          /\ node[r].insts[node[r].commitUpTo+1].status = "Accepting";
                \* W.L.O.G., only enabling the next slot after commitUpTo
                \* here to make the body of this macro simpler; in practice,
                \* messages are received proactively and there should be a
                \* separate "Executed" status
    \* for this slot, when there are enough number of AcceptReplies
    with s = node[r].commitUpTo+1,
         c = node[r].insts[s].write,
         ps = s - 1,
         v = IF ps = 0 THEN "nil" ELSE node[r].insts[ps].write,
         ars = {m \in msgs: /\ m.type = "AcceptReply"
                            /\ m.slot = s
                            /\ m.bal = node[r].balPrepared}
    do
        await Cardinality(ars) >= MajorityNum;
        \* marks this slot as committed and apply command
        node[r].insts[s].status := "Committed" ||
        node[r].commitUpTo := s;
        \* append to observed events sequence if haven't yet, and remove
        \* the command from pending
        Observe(AckEvent(c, v));
        Resolve(c);
        \* broadcast CommitNotice to followers
        Send({CommitNoticeMsg(s)});
    end with;
end macro;

\* Replica replies to a DoRead message.
macro HandleDoRead(r) begin
    \* if receiving an unreplied DoRead message with valid ballot
    with m \in msgs do
        await /\ m.type = "DoRead"
              /\ m.bal >= node[r].balMaxKnown
              /\ \/ m.slot > NumWrites
                 \/ /\ m.slot =< NumWrites
                    /\ m.bal >= node[r].insts[m.slot].voted.bal;
        \* send back DoReadReply
        Send({DoReadReplyMsg(r, m.bal, m.slot, m.read)});
    end with;
end macro;

\* Leader gathers DoReadReply messages for a read request until read quorum
\* formed, then acknowledges the client.
macro HandleDoReadReplies(r) begin
    \* if I think I'm a current leader
    await /\ node[r].leader = r
          /\ node[r].balPrepared = node[r].balMaxKnown;
    \* for an on-the-fly read, when there are enough DoReadReplies and that
    \* the predecessor write has been committed
    with s \in (Slots \cup {NumWrites+1}),
         c \in node[r].reads[s],
         ps = s - 1,
         v = IF ps = 0 THEN "nil" ELSE node[r].insts[ps].write,
         drs = {m \in msgs: /\ m.type = "DoReadReply"
                            /\ m.slot = s
                            /\ m.read = c
                            /\ m.bal = node[r].balPrepared}
    do
        await /\ Cardinality(drs) >= MajorityNum
              /\ node[r].commitUpTo >= ps;
                    \* W.L.O.G., only enabling slots at or before commitUpTo
                    \* here to make the body of this macro simpler; in
                    \* practice, messages are received proactively and there
                    \* should be separate status tracking for these reads
        \* append to observed events sequence if haven't yet, and remove
        \* the command from pending
        Observe(AckEvent(c, v));
        Resolve(c);
        \* remove from the set of on-the-fly reads in anchored slot
        node[r].reads[s] := @ \ {c};
    end with;
end macro;

\* Replica receives new commit notification.
macro HandleCommitNotice(r) begin
    \* if I'm a follower waiting on CommitNotice
    await /\ node[r].leader # r
          /\ node[r].commitUpTo < NumWrites
          /\ node[r].insts[node[r].commitUpTo+1].status = "Accepting";
                \* W.L.O.G., only enabling the next slot after commitUpTo
                \* here to make the body of this macro simpler
    \* for this slot, when there's a CommitNotice message
    with s = node[r].commitUpTo+1,
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
            TakeNewWriteRequest(self);
        or
            TakeNewReadRequest(self);
        or
            HandleAccept(self);
        or
            HandleAcceptReplies(self);
        or
            HandleDoRead(self);
        or
            HandleDoReadReplies(self);
        or
            if CommitNoticeOn then
                HandleCommitNotice(self);
            end if;
        or
            if NodeFailuresOn then
                ReplicaCrashes(self);
            end if;
        end either;
    end while;
end process;

end algorithm; *)

----------

\* BEGIN TRANSLATION (chksum(pcal) = "1b885764" /\ chksum(tla) = "c6a9069f")
VARIABLES msgs, node, pending, observed, crashed, pc

(* define statement *)
UnseenPending(ns) ==
    LET filter(c) ==
            /\ \A s \in Slots: ns.insts[s].write # c
            /\ \A s \in Slots \cup {NumWrites+1}: c \notin ns.reads[s]
    IN  SelectSeq(pending, filter)

RemovePending(cmd) ==
    LET filter(c) == c # cmd
    IN  SelectSeq(pending, filter)

reqsMade == {e.cmd: e \in {e \in Range(observed): e.type = "Req"}}

acksRecv == {e.cmd: e \in {e \in Range(observed): e.type = "Ack"}}

terminated == /\ Len(pending) = 0
              /\ Cardinality(reqsMade) = NumCommands
              /\ Cardinality(acksRecv) = NumCommands

numCrashed == Cardinality({r \in Replicas: crashed[r]})


vars == << msgs, node, pending, observed, crashed, pc >>

ProcSet == (Replicas)

Init == (* Global variables *)
        /\ msgs = {}
        /\ node = [r \in Replicas |-> NullNode]
        /\ pending = InitPending
        /\ observed = <<>>
        /\ crashed = [r \in Replicas |-> FALSE]
        /\ pc = [self \in ProcSet |-> "rloop"]

rloop(self) == /\ pc[self] = "rloop"
               /\ IF (~terminated) /\ (~crashed[self])
                     THEN /\ \/ /\ node[self].leader # self
                                /\ \E b \in Ballots:
                                     /\ /\ b > node[self].balMaxKnown
                                        /\ ~\E m \in msgs: (m.type = "Prepare") /\ (m.bal = b)
                                     /\ node' = [node EXCEPT ![self].leader = self,
                                                             ![self].balPrepared = 0,
                                                             ![self].balMaxKnown = b,
                                                             ![self].insts = [s \in Slots |->
                                                                                 [node[self].insts[s]
                                                                                     EXCEPT !.status = IF @ = "Accepting"
                                                                                                         THEN "Preparing"
                                                                                                         ELSE @]],
                                                             ![self].reads = [s \in Slots \cup {NumWrites+1} |-> {}]]
                                     /\ msgs' = (msgs \cup ({PrepareMsg(self, b),
                                                             PrepareReplyMsg(self, b, VotesByNode(node'[self]))}))
                                /\ UNCHANGED <<pending, observed, crashed>>
                             \/ /\ \E m \in msgs:
                                     /\ /\ m.type = "Prepare"
                                        /\ m.bal > node[self].balMaxKnown
                                     /\ node' = [node EXCEPT ![self].leader = m.src,
                                                             ![self].balMaxKnown = m.bal,
                                                             ![self].insts = [s \in Slots |->
                                                                                 [node[self].insts[s]
                                                                                     EXCEPT !.status = IF @ = "Accepting"
                                                                                                         THEN "Preparing"
                                                                                                         ELSE @]]]
                                     /\ msgs' = (msgs \cup ({PrepareReplyMsg(self, m.bal, VotesByNode(node'[self]))}))
                                /\ UNCHANGED <<pending, observed, crashed>>
                             \/ /\ /\ node[self].leader = self
                                   /\ node[self].balPrepared = 0
                                /\ LET prs == {m \in msgs: /\ m.type = "PrepareReply"
                                                           /\ m.bal = node[self].balMaxKnown} IN
                                     /\ Cardinality(prs) >= MajorityNum
                                     /\ node' = [node EXCEPT ![self].balPrepared = node[self].balMaxKnown,
                                                             ![self].insts = [s \in Slots |->
                                                                                 [node[self].insts[s]
                                                                                     EXCEPT !.status = IF \/ @ = "Preparing"
                                                                                                          \/ /\ @ = "Empty"
                                                                                                             /\ PeakVotedWrite(prs, s) # "nil"
                                                                                                         THEN "Accepting"
                                                                                                         ELSE @,
                                                                                            !.write  = PeakVotedWrite(prs, s)]]]
                                     /\ msgs' = (msgs \cup (UNION
                                                            {{AcceptMsg(self, node'[self].balPrepared, s, node'[self].insts[s].write),
                                                              AcceptReplyMsg(self, node'[self].balPrepared, s)}:
                                                             s \in {s \in Slots: node'[self].insts[s].status = "Accepting"}}))
                                /\ UNCHANGED <<pending, observed, crashed>>
                             \/ /\ /\ node[self].leader = self
                                   /\ node[self].balPrepared = node[self].balMaxKnown
                                   /\ \E s \in Slots: node[self].insts[s].status = "Empty"
                                   /\ Len(UnseenPending(node[self])) > 0
                                   /\ Head(UnseenPending(node[self])) \in Writes
                                /\ LET s == FirstEmptySlot(node[self].insts) IN
                                     LET c == Head(UnseenPending(node[self])) IN
                                       /\ node' = [node EXCEPT ![self].insts[s].status = "Accepting",
                                                               ![self].insts[s].write = c,
                                                               ![self].insts[s].voted.bal = node[self].balPrepared,
                                                               ![self].insts[s].voted.write = c]
                                       /\ msgs' = (msgs \cup ({AcceptMsg(self, node'[self].balPrepared, s, c),
                                                               AcceptReplyMsg(self, node'[self].balPrepared, s)}))
                                       /\ IF (ReqEvent(c)) \notin Range(observed)
                                             THEN /\ observed' = Append(observed, (ReqEvent(c)))
                                             ELSE /\ TRUE
                                                  /\ UNCHANGED observed
                                /\ UNCHANGED <<pending, crashed>>
                             \/ /\ /\ node[self].leader = self
                                   /\ node[self].balPrepared = node[self].balMaxKnown
                                   /\ Len(UnseenPending(node[self])) > 0
                                   /\ Head(UnseenPending(node[self])) \in Reads
                                /\ LET s == FirstEmptySlot(node[self].insts) IN
                                     LET c == Head(UnseenPending(node[self])) IN
                                       /\ msgs' = (msgs \cup ({DoReadMsg(self, node[self].balPrepared, s, c),
                                                               DoReadReplyMsg(self, node[self].balPrepared, s, c)}))
                                       /\ node' = [node EXCEPT ![self].reads[s] = @ \cup {c}]
                                       /\ IF (ReqEvent(c)) \notin Range(observed)
                                             THEN /\ observed' = Append(observed, (ReqEvent(c)))
                                             ELSE /\ TRUE
                                                  /\ UNCHANGED observed
                                /\ UNCHANGED <<pending, crashed>>
                             \/ /\ \E m \in msgs:
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
                                /\ UNCHANGED <<pending, observed, crashed>>
                             \/ /\ /\ node[self].leader = self
                                   /\ node[self].balPrepared = node[self].balMaxKnown
                                   /\ node[self].commitUpTo < NumWrites
                                   /\ node[self].insts[node[self].commitUpTo+1].status = "Accepting"
                                /\ LET s == node[self].commitUpTo+1 IN
                                     LET c == node[self].insts[s].write IN
                                       LET ps == s - 1 IN
                                         LET v == IF ps = 0 THEN "nil" ELSE node[self].insts[ps].write IN
                                           LET ars == {m \in msgs: /\ m.type = "AcceptReply"
                                                                   /\ m.slot = s
                                                                   /\ m.bal = node[self].balPrepared} IN
                                             /\ Cardinality(ars) >= MajorityNum
                                             /\ node' = [node EXCEPT ![self].insts[s].status = "Committed",
                                                                     ![self].commitUpTo = s]
                                             /\ IF (AckEvent(c, v)) \notin Range(observed)
                                                   THEN /\ observed' = Append(observed, (AckEvent(c, v)))
                                                   ELSE /\ TRUE
                                                        /\ UNCHANGED observed
                                             /\ pending' = RemovePending(c)
                                             /\ msgs' = (msgs \cup ({CommitNoticeMsg(s)}))
                                /\ UNCHANGED crashed
                             \/ /\ \E m \in msgs:
                                     /\ /\ m.type = "DoRead"
                                        /\ m.bal >= node[self].balMaxKnown
                                        /\ \/ m.slot > NumWrites
                                           \/ /\ m.slot =< NumWrites
                                              /\ m.bal >= node[self].insts[m.slot].voted.bal
                                     /\ msgs' = (msgs \cup ({DoReadReplyMsg(self, m.bal, m.slot, m.read)}))
                                /\ UNCHANGED <<node, pending, observed, crashed>>
                             \/ /\ /\ node[self].leader = self
                                   /\ node[self].balPrepared = node[self].balMaxKnown
                                /\ \E s \in (Slots \cup {NumWrites+1}):
                                     \E c \in node[self].reads[s]:
                                       LET ps == s - 1 IN
                                         LET v == IF ps = 0 THEN "nil" ELSE node[self].insts[ps].write IN
                                           LET drs == {m \in msgs: /\ m.type = "DoReadReply"
                                                                   /\ m.slot = s
                                                                   /\ m.read = c
                                                                   /\ m.bal = node[self].balPrepared} IN
                                             /\ /\ Cardinality(drs) >= MajorityNum
                                                /\ node[self].commitUpTo >= ps
                                             /\ IF (AckEvent(c, v)) \notin Range(observed)
                                                   THEN /\ observed' = Append(observed, (AckEvent(c, v)))
                                                   ELSE /\ TRUE
                                                        /\ UNCHANGED observed
                                             /\ pending' = RemovePending(c)
                                             /\ node' = [node EXCEPT ![self].reads[s] = @ \ {c}]
                                /\ UNCHANGED <<msgs, crashed>>
                             \/ /\ IF CommitNoticeOn
                                      THEN /\ /\ node[self].leader # self
                                              /\ node[self].commitUpTo < NumWrites
                                              /\ node[self].insts[node[self].commitUpTo+1].status = "Accepting"
                                           /\ LET s == node[self].commitUpTo+1 IN
                                                LET c == node[self].insts[s].write IN
                                                  \E m \in msgs:
                                                    /\ /\ m.type = "CommitNotice"
                                                       /\ m.upto = s
                                                    /\ node' = [node EXCEPT ![self].insts[s].status = "Committed",
                                                                            ![self].commitUpTo = s]
                                      ELSE /\ TRUE
                                           /\ node' = node
                                /\ UNCHANGED <<msgs, pending, observed, crashed>>
                             \/ /\ IF NodeFailuresOn
                                      THEN /\ /\ MajorityNum + numCrashed < Cardinality(Replicas)
                                              /\ ~crashed[self]
                                              /\ node[self].balMaxKnown < MaxBallot
                                           /\ crashed' = [crashed EXCEPT ![self] = TRUE]
                                      ELSE /\ TRUE
                                           /\ UNCHANGED crashed
                                /\ UNCHANGED <<msgs, node, pending, observed>>
                          /\ pc' = [pc EXCEPT ![self] = "rloop"]
                     ELSE /\ pc' = [pc EXCEPT ![self] = "Done"]
                          /\ UNCHANGED << msgs, node, pending, observed, 
                                          crashed >>

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
