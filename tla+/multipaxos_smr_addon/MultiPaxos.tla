(**********************************************************************************)
(* This spec extends multipaxos_smr_style/MultiPaxos.tla with extra features:     *)
(*   [ ] Only keep writes in the log (reads squeeze in between writes)            *)
(*   [ ] Asymmetric write/read quorum size                                        *)
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
         CommitNoticeOn,  \* if true, turn on CommitNotice messages
         NodeFailuresOn   \* if true, turn on node failures injection

ReplicasAssumption == /\ IsFiniteSet(Replicas)
                      /\ Cardinality(Replicas) >= 1

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

CommitNoticeOnAssumption == CommitNoticeOn \in BOOLEAN

NodeFailuresOnAssumption == NodeFailuresOn \in BOOLEAN

ASSUME /\ ReplicasAssumption
       /\ WritesAssumption
       /\ ReadsAssumption
       /\ MaxBallotAssumption
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
Population == Cardinality(Replicas)

MajorityNum == (Population \div 2) + 1

Ballots == 1..MaxBallot

Slots == 1..NumWrites

Statuses == {"Preparing", "Accepting", "Committed"}

InstStates == [status: {"Empty"} \cup Statuses,
               cmd: {"nil"} \cup Writes,
               voted: [bal: {0} \cup Ballots,
                       cmd: {"nil"} \cup Writes]]

NullInst == [status |-> "Empty",
             cmd |-> "nil",
             voted |-> [bal |-> 0, cmd |-> "nil"]]

NodeStates == [leader: {"none"} \cup Replicas,
               kvalue: {"nil"} \cup Writes,
               commitUpTo: {0} \cup Slots,
               onflyReads: SUBSET Reads,
               onflyReadValue: [Reads -> {"nil"} \cup Writes],
               balPrepared: {0} \cup Ballots,
               balMaxKnown: {0} \cup Ballots,
               insts: [Slots -> InstStates]]

NullNode == [leader |-> "none",
             kvalue |-> "nil",
             commitUpTo |-> 0,
             onflyReads |-> {},
             onflyReadValue |-> [c \in Reads |-> "nil"],
             balPrepared |-> 0,
             balMaxKnown |-> 0,
             insts |-> [s \in Slots |-> NullInst]]

FirstEmptySlot(insts) ==
    CHOOSE s \in Slots:
        /\ insts[s].status = "Empty"
        /\ \A t \in 1..(s-1): insts[t].status # "Empty"

\* Service-internal messages.
PrepareMsgs == [type: {"Prepare"}, src: Replicas,
                                   bal: Ballots]

PrepareMsg(r, b) == [type |-> "Prepare", src |-> r,
                                         bal |-> b]

InstsVotes == [Slots -> [bal: {0} \cup Ballots,
                         cmd: {"nil"} \cup Writes]]

VotesByNode(n) == [s \in Slots |-> n.insts[s].voted]

PrepareReplyMsgs == [type: {"PrepareReply"}, src: Replicas,
                                             bal: Ballots,
                                             votes: InstsVotes]

PrepareReplyMsg(r, b, iv) ==
    [type |-> "PrepareReply", src |-> r,
                              bal |-> b,
                              votes |-> iv]

PeakVotedCmd(prs, s) ==
    IF \A pr \in prs: pr.votes[s].bal = 0
        THEN "nil"
        ELSE LET ppr ==
                    CHOOSE ppr \in prs:
                        \A pr \in prs: pr.votes[s].bal =< ppr.votes[s].bal
             IN  ppr.votes[s].cmd

AcceptMsgs == [type: {"Accept"}, src: Replicas,
                                 bal: Ballots,
                                 slot: Slots,
                                 cmd: Writes]

AcceptMsg(r, b, s, c) == [type |-> "Accept", src |-> r,
                                             bal |-> b,
                                             slot |-> s,
                                             cmd |-> c]

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
                                 slot: Slots \cup {NumWrites + 1},
                                 cmd: Reads]

DoReadMsg(r, b, s, c) == [type |-> "DoRead", src |-> r,
                                             bal |-> b,
                                             slot |-> s,
                                             cmd |-> c]

DoReadReplyMsgs == [type: {"DoReadReply"}, src: Replicas,
                                           bal: Ballots,
                                           cmd: Reads]

DoReadReplyMsg(r, b, c) == [type |-> "DoReadReply", src |-> r,
                                                    bal |-> b,
                                                    cmd |-> c]
                                \* cmd here is just a command ID

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
    UnseenPending(insts) ==
        LET filter(c) == c \notin {insts[s].cmd: s \in Slots}
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
                                        ELSE @]];
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
                                            /\ PeakVotedCmd(prs, s) # "nil"
                                        THEN "Accepting"
                                        ELSE @,
                           !.cmd = PeakVotedCmd(prs, s)]];
        \* send Accept messages for in-progress instances and reply to
        \* myself instantly
        Send(UNION
             {{AcceptMsg(r, node[r].balPrepared, s, node[r].insts[s].cmd),
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
          /\ Len(UnseenPending(node[r].insts)) > 0
          /\ Head(UnseenPending(node[r].insts)) \in Writes;
    \* find the next empty slot and pick a pending request
    with s = FirstEmptySlot(node[r].insts),
         c = Head(UnseenPending(node[r].insts))
                \* W.L.O.G., only pick a command not seen in current
                \* prepared log to have smaller state space; in practice,
                \* duplicated client requests should be treated by some
                \* idempotency mechanism such as using request IDs
    do
        \* update slot status and voted
        node[r].insts[s].status := "Accepting" ||
        node[r].insts[s].cmd := c ||
        node[r].insts[s].voted.bal := node[r].balPrepared ||
        node[r].insts[s].voted.cmd := c;
        \* broadcast Accept and reply to myself instantly
        Send({AcceptMsg(r, node[r].balPrepared, s, c),
              AcceptReplyMsg(r, node[r].balPrepared, s)});
        \* append to observed events sequence if haven't yet
        Observe(ReqEvent(c));
    end with;
end macro;

\* A prepared leader takes a new read request and try to place it after
\* the current last committed slot.
macro TakeNewReadRequest(r) begin
    \* if I'm a prepared leader and there's pending read request
    await /\ node[r].leader = r
          /\ node[r].balPrepared = node[r].balMaxKnown
          /\ Len(UnseenPending(node[r].insts)) > 0
          /\ Head(UnseenPending(node[r].insts)) \in Reads;
    \* pick a pending request and use the slot right after last committed
    with s = node[r].commitUpTo + 1,
         c = Head(UnseenPending(node[r].insts))
                \* W.L.O.G., only pick a command not seen in current
                \* prepared log to have smaller state space; in practice,
                \* duplicated client requests should be treated by some
                \* idempotency mechanism such as using request IDs
    do
        \* broadcast DoRead and reply to myself instantly
        Send({DoReadMsg(r, node[r].balPrepared, s, c),
              DoReadReplyMsg(r, node[r].balPrepared, c)});
        \* add to the set of on-the-fly reads and save the latest value
        \* up to it (in practice, could just save a slot number)
        node[r].onflyReads := @ \cup {c} ||
        node[r].onflyReadValue[c] := node[r].kvalue;
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
              /\ m.bal > node[r].insts[m.slot].voted.bal;
        \* update node states and corresponding instance's states
        node[r].leader := m.src ||
        node[r].balMaxKnown := m.bal ||
        node[r].insts[m.slot].status := "Accepting" ||
        node[r].insts[m.slot].cmd := m.cmd ||
        node[r].insts[m.slot].voted.bal := m.bal ||
        node[r].insts[m.slot].voted.cmd := m.cmd;
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
                \* here to make the body of this macro simpler; in practice
                \* there should be a separate "Executed" status
    \* for this slot, when there are enough number of AcceptReplies
    with s = node[r].commitUpTo + 1,
         c = node[r].insts[s].cmd,
         v = node[r].kvalue,
         ars = {m \in msgs: /\ m.type = "AcceptReply"
                            /\ m.slot = s
                            /\ m.bal = node[r].balPrepared}
    do
        await Cardinality(ars) >= MajorityNum;
        \* marks this slot as committed and apply command
        node[r].insts[s].status := "Committed" ||
        node[r].commitUpTo := s ||
        node[r].kvalue := c;
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
        Send({DoReadReplyMsg(r, m.bal, m.cmd)});
    end with;
end macro;

\* Leader gathers DoReadReply messages for a read request until read quorum
\* formed, then acknowledges the client.
macro HandleDoReadReplies(r) begin
    \* if I think I'm a current leader
    await /\ node[r].leader = r
          /\ node[r].balPrepared = node[r].balMaxKnown;
    \* for an on-the-fly read, when there are enough DoReadReplies
    with c \in node[r].onflyReads,
         v = node[r].onflyReadValue[c],
         drs = {m \in msgs: /\ m.type = "DoReadReply"
                            /\ m.cmd = c
                            /\ m.bal = node[r].balPrepared}
    do
        await Cardinality(drs) >= MajorityNum;
        \* append to observed events sequence if haven't yet, and remove
        \* the command from pending
        Observe(AckEvent(c, v));
        Resolve(c);
        \* remove from the set of on-the-fly reads
        node[r].onflyReads := @ \ {c};
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
    with s = node[r].commitUpTo + 1,
         c = node[r].insts[s].cmd,
         m \in msgs
    do
        await /\ m.type = "CommitNotice"
              /\ m.upto = s;
        \* marks this slot as committed and apply command
        node[r].insts[s].status := "Committed" ||
        node[r].commitUpTo := s ||
        node[r].kvalue := IF c \in Writes THEN c ELSE @;
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

\* BEGIN TRANSLATION (chksum(pcal) = "7c6f57c6" /\ chksum(tla) = "1a94ad63")
VARIABLES msgs, node, pending, observed, crashed, pc

(* define statement *)
UnseenPending(insts) ==
    LET filter(c) == c \notin {insts[s].cmd: s \in Slots}
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
                                                                                                         ELSE @]]]
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
                                                                                                             /\ PeakVotedCmd(prs, s) # "nil"
                                                                                                         THEN "Accepting"
                                                                                                         ELSE @,
                                                                                            !.cmd = PeakVotedCmd(prs, s)]]]
                                     /\ msgs' = (msgs \cup (UNION
                                                            {{AcceptMsg(self, node'[self].balPrepared, s, node'[self].insts[s].cmd),
                                                              AcceptReplyMsg(self, node'[self].balPrepared, s)}:
                                                             s \in {s \in Slots: node'[self].insts[s].status = "Accepting"}}))
                                /\ UNCHANGED <<pending, observed, crashed>>
                             \/ /\ /\ node[self].leader = self
                                   /\ node[self].balPrepared = node[self].balMaxKnown
                                   /\ \E s \in Slots: node[self].insts[s].status = "Empty"
                                   /\ Len(UnseenPending(node[self].insts)) > 0
                                   /\ Head(UnseenPending(node[self].insts)) \in Writes
                                /\ LET s == FirstEmptySlot(node[self].insts) IN
                                     LET c == Head(UnseenPending(node[self].insts)) IN
                                       /\ node' = [node EXCEPT ![self].insts[s].status = "Accepting",
                                                               ![self].insts[s].cmd = c,
                                                               ![self].insts[s].voted.bal = node[self].balPrepared,
                                                               ![self].insts[s].voted.cmd = c]
                                       /\ msgs' = (msgs \cup ({AcceptMsg(self, node'[self].balPrepared, s, c),
                                                               AcceptReplyMsg(self, node'[self].balPrepared, s)}))
                                       /\ IF (ReqEvent(c)) \notin Range(observed)
                                             THEN /\ observed' = Append(observed, (ReqEvent(c)))
                                             ELSE /\ TRUE
                                                  /\ UNCHANGED observed
                                /\ UNCHANGED <<pending, crashed>>
                             \/ /\ /\ node[self].leader = self
                                   /\ node[self].balPrepared = node[self].balMaxKnown
                                   /\ Len(UnseenPending(node[self].insts)) > 0
                                   /\ Head(UnseenPending(node[self].insts)) \in Reads
                                /\ LET s == node[self].commitUpTo + 1 IN
                                     LET c == Head(UnseenPending(node[self].insts)) IN
                                       /\ msgs' = (msgs \cup ({DoReadMsg(self, node[self].balPrepared, s, c),
                                                               DoReadReplyMsg(self, node[self].balPrepared, c)}))
                                       /\ node' = [node EXCEPT ![self].onflyReads = @ \cup {c},
                                                               ![self].onflyReadValue[c] = node[self].kvalue]
                                       /\ IF (ReqEvent(c)) \notin Range(observed)
                                             THEN /\ observed' = Append(observed, (ReqEvent(c)))
                                             ELSE /\ TRUE
                                                  /\ UNCHANGED observed
                                /\ UNCHANGED <<pending, crashed>>
                             \/ /\ \E m \in msgs:
                                     /\ /\ m.type = "Accept"
                                        /\ m.bal >= node[self].balMaxKnown
                                        /\ m.bal > node[self].insts[m.slot].voted.bal
                                     /\ node' = [node EXCEPT ![self].leader = m.src,
                                                             ![self].balMaxKnown = m.bal,
                                                             ![self].insts[m.slot].status = "Accepting",
                                                             ![self].insts[m.slot].cmd = m.cmd,
                                                             ![self].insts[m.slot].voted.bal = m.bal,
                                                             ![self].insts[m.slot].voted.cmd = m.cmd]
                                     /\ msgs' = (msgs \cup ({AcceptReplyMsg(self, m.bal, m.slot)}))
                                /\ UNCHANGED <<pending, observed, crashed>>
                             \/ /\ /\ node[self].leader = self
                                   /\ node[self].balPrepared = node[self].balMaxKnown
                                   /\ node[self].commitUpTo < NumWrites
                                   /\ node[self].insts[node[self].commitUpTo+1].status = "Accepting"
                                /\ LET s == node[self].commitUpTo + 1 IN
                                     LET c == node[self].insts[s].cmd IN
                                       LET v == node[self].kvalue IN
                                         LET ars == {m \in msgs: /\ m.type = "AcceptReply"
                                                                 /\ m.slot = s
                                                                 /\ m.bal = node[self].balPrepared} IN
                                           /\ Cardinality(ars) >= MajorityNum
                                           /\ node' = [node EXCEPT ![self].insts[s].status = "Committed",
                                                                   ![self].commitUpTo = s,
                                                                   ![self].kvalue = c]
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
                                     /\ msgs' = (msgs \cup ({DoReadReplyMsg(self, m.bal, m.cmd)}))
                                /\ UNCHANGED <<node, pending, observed, crashed>>
                             \/ /\ /\ node[self].leader = self
                                   /\ node[self].balPrepared = node[self].balMaxKnown
                                /\ \E c \in node[self].onflyReads:
                                     LET v == node[self].onflyReadValue[c] IN
                                       LET drs == {m \in msgs: /\ m.type = "DoReadReply"
                                                               /\ m.cmd = c
                                                               /\ m.bal = node[self].balPrepared} IN
                                         /\ Cardinality(drs) >= MajorityNum
                                         /\ IF (AckEvent(c, v)) \notin Range(observed)
                                               THEN /\ observed' = Append(observed, (AckEvent(c, v)))
                                               ELSE /\ TRUE
                                                    /\ UNCHANGED observed
                                         /\ pending' = RemovePending(c)
                                         /\ node' = [node EXCEPT ![self].onflyReads = @ \ {c}]
                                /\ UNCHANGED <<msgs, crashed>>
                             \/ /\ IF CommitNoticeOn
                                      THEN /\ /\ node[self].leader # self
                                              /\ node[self].commitUpTo < NumWrites
                                              /\ node[self].insts[node[self].commitUpTo+1].status = "Accepting"
                                           /\ LET s == node[self].commitUpTo + 1 IN
                                                LET c == node[self].insts[s].cmd IN
                                                  \E m \in msgs:
                                                    /\ /\ m.type = "CommitNotice"
                                                       /\ m.upto = s
                                                    /\ node' = [node EXCEPT ![self].insts[s].status = "Committed",
                                                                            ![self].commitUpTo = s,
                                                                            ![self].kvalue = IF c \in Writes THEN c ELSE @]
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
