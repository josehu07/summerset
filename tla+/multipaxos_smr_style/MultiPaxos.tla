(**********************************************************************************)
(* MultiPaxos in state machine replication (SMR) style with write/read commands   *)
(* on a single key.                                                               *)
(*                                                                                *)
(* Linearizability is checked from global client's point of view on the sequence  *)
(* of client observed events after termination.                                   *)
(*                                                                                *)
(* Liveness is checked by not having deadlocks before termination.                *)
(**********************************************************************************)

---- MODULE MultiPaxos ----
EXTENDS FiniteSets, Sequences, Integers, TLC

(*******************************)
(* Model inputs & assumptions. *)
(*******************************)
CONSTANT Replicas,  \* symmetric set of server nodes
         Writes,    \* symmetric set of write commands (each w/ unique value)
         Reads,     \* symmetric set of read commands following writes
         MaxBallot  \* maximum ballot pickable for leader preemption

ReplicasAssumption == /\ IsFiniteSet(Replicas)
                      /\ Cardinality(Replicas) >= 1

WritesAssumption == /\ IsFiniteSet(Writes)
                    /\ Cardinality(Writes) >= 1
                    /\ "nil" \notin Writes
                         \* a write command model value serves as both the ID
                         \* of the command and the value to be written

ReadsAssumption == /\ IsFiniteSet(Reads)
                   /\ Cardinality(Reads) >= 0
                   /\ "nil" \notin Writes

MaxBallotAssumption == /\ MaxBallot \in Nat
                       /\ MaxBallot >= 2

ASSUME /\ ReplicasAssumption
       /\ WritesAssumption
       /\ ReadsAssumption
       /\ MaxBallotAssumption

----------

(********************************)
(* Useful constants & typedefs. *)
(********************************)
Commands == Writes \cup Reads

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
                        \* choose any sequence contatenating writes commands
                        \* and read commands as the sequence of requests;
                        \* all other cases are all symmetric or less useful
                        \* than this one

\* Server-side constants & states.
MajorityNum == (Cardinality(Replicas) \div 2) + 1

Ballots == 1..MaxBallot

Slots == 1..NumCommands

Statuses == {"Preparing", "Accepting", "Committed"}

InProgress(status) == \/ status = "Preparing"
                      \/ status = "Accepting"

InstStates == [status: {"Empty"} \cup Statuses,
               cmd: {"nil"} \cup Commands,
               voted: [bal: {0} \cup Ballots,
                       cmd: {"nil"} \cup Commands]]

NullInst == [status |-> "Empty",
             cmd |-> "nil",
             voted |-> [bal |-> 0, cmd |-> "nil"]]

NodeStates == [leader: {"none"} \cup Replicas,
               kvalue: {"nil"} \cup Writes,
               commitUpTo: {0} \cup Slots,
               balPrepared: {0} \cup Ballots,
               balMaxKnown: {0} \cup Ballots,
               insts: [Slots -> InstStates]]

NullNode == [leader |-> "none",
             kvalue |-> "nil",
             commitUpTo |-> 0,
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
                         cmd: {"nil"} \cup Commands]]
                             
VotesByNode(n) == [s \in Slots |-> n.insts[s].voted]

PrepareReplyMsgs == [type: {"PrepareReply"}, src: Replicas,
                                             bal: Ballots,
                                             votes: InstsVotes]

PrepareReplyMsg(r, b, iv) ==
    [type |-> "PrepareReply", src |-> r,
                              bal |-> b,
                              votes |-> iv]

HighestVotedCmd(prs, s) ==
    IF \A pr \in prs: pr.votes[s].bal = 0
        THEN "nil"
        ELSE LET bc == CHOOSE bc \in (Ballots \X Commands):
                            /\ \E pr \in prs: /\ pr.votes[s].bal = bc[1]
                                              /\ pr.votes[s].cmd = bc[2]
                            /\ \A pr \in prs: pr.votes[s].bal =< bc[1]
             IN  bc[2]

AcceptMsgs == [type: {"Accept"}, src: Replicas,
                                 bal: Ballots,
                                 slot: Slots,
                                 cmd: Commands]

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

CommitNoticeMsgs == [type: {"CommitNotice"}, upto: Slots]

CommitNoticeMsg(u) == [type |-> "CommitNotice", upto |-> u]

Messages ==      PrepareMsgs
            \cup PrepareReplyMsgs
            \cup AcceptMsgs
            \cup AcceptReplyMsgs
            \cup CommitNoticeMsgs

----------

(******************************)
(* Main algorithm in PlusCal. *)
(******************************)
(*--algorithm MultiPaxos

variable msgs = {},                             \* messages in the network
         node = [r \in Replicas |-> NullNode],  \* replica node state
         pending = InitPending,                 \* sequence of pending reqs
         observed = <<>>;                       \* client observed events

define
    Unobserved(e) == e \notin Range(observed)

    requestsMade == {e.cmd: e \in {e \in Range(observed): e.type = "Req"}}
    
    responsesGot == {e.cmd: e \in {e \in Range(observed): e.type = "Ack"}}

    terminated == /\ Len(pending) = 0
                  /\ Cardinality(requestsMade) = NumCommands
                  /\ Cardinality(responsesGot) = NumCommands
end define;

\* Send messages helper.
macro Send(set) begin
    msgs := msgs \cup set;
end macro;

\* Observe events helper.
macro Observe(seq) begin
    observed := observed \o SelectSeq(seq, Unobserved);
end macro;

\* Someone steps up as leader and sends Prepare message to followers.
macro BecomeLeader(r) begin
    \* if I'm not a leader
    await node[r].leader # r;
    \* pick a greater ballot number
    with b \in Ballots do
        await /\ b > node[r].balMaxKnown
              /\ ~\E m \in msgs: (m.type = "Prepare") /\ (m.bal = b);
                    \* using this clause to model that ballot numbers from
                    \* different proposers be unique
        \* update states and restart Prepare phase for in-progress instances
        node[r].leader := r ||
        node[r].balPrepared := 0 ||
        node[r].balMaxKnown := b ||
        node[r].insts := [s \in Slots |->
                            [node[r].insts[s]
                                EXCEPT !.status = IF InProgress(@)
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
        node[r].insts := [s \in Slots |->
                            [node[r].insts[s]
                                EXCEPT !.status = IF InProgress(@)
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
        node[r].insts := [s \in Slots |->
                            [node[r].insts[s]
                                EXCEPT !.status = IF InProgress(@)
                                                    THEN "Accepting"
                                                    ELSE @,
                                       !.cmd = HighestVotedCmd(prs, s)]];
        \* send Accept messages for in-progress instances
        Send({AcceptMsg(r, node[r].balPrepared, s, node[r].insts[s].cmd):
              s \in {s \in Slots: InProgress(node[r].insts[s].status)}});
    end with;
end macro;

\* A prepared leader takes a new request to fill the next empty slot.
macro TakeNewRequest(r) begin
    \* if I'm a prepared leader and there's pending request
    await /\ node[r].leader = r
          /\ node[r].balPrepared = node[r].balMaxKnown
          /\ \E s \in Slots: node[r].insts[s].status = "Empty"
          /\ Len(pending) > 0;
    \* find the next empty slot and pick a pending request
    with s = FirstEmptySlot(node[r].insts),
         c = Head(pending)
    do
        \* update slot status and voted
        node[r].insts[s].status := "Accepting" ||
        node[r].insts[s].cmd := c ||
        node[r].insts[s].voted.bal := node[r].balPrepared ||
        node[r].insts[s].voted.cmd := c;
        \* broadcast Accept and reply to myself instantly
        Send({AcceptMsg(r, node[r].balPrepared, s, c),
              AcceptReplyMsg(r, node[r].balPrepared, s)});
        \* TODO: change this
        Observe(<<ReqEvent(c)>>);
        pending := Tail(pending);
    end with;
end macro;

\* Replica replies to an Accept message.
macro HandleAccept(r) begin
    \* if receiving an unreplied Accept message with valid ballot
    with m \in msgs do
        await /\ m.type = "Accept"
              /\ m.bal >= node[r].balMaxKnown
              /\ node[r].insts[m.slot].voted.bal < m.bal;
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
          /\ node[r].commitUpTo < NumCommands
          /\ node[r].insts[node[r].commitUpTo+1].status = "Accepting";
                \* only enabling the next slot after commitUpTo here to
                \* make the body of this macro simpler
    \* for a slot, when there are enough number of AcceptReplies
    with s = node[r].commitUpTo + 1,
         cmd = node[r].insts[s].cmd,
         val = node[r].kvalue,
         ars = {m \in msgs: /\ m.type = "AcceptReply"
                            /\ m.slot = s
                            /\ m.bal = node[r].balPrepared}
    do
        await Cardinality(ars) >= MajorityNum;
        \* marks this slot as committed and apply command
        node[r].insts[s].status := "Committed" ||
        node[r].commitUpTo := s ||
        node[r].kvalue := IF cmd \in Writes THEN cmd ELSE @;
        \* TODO: change this
        Observe(<<AckEvent(cmd, val)>>);
        \* broadcast CommitNotice to followers
        Send({CommitNoticeMsg(s)});
    end with;
end macro;

\* Replica server node logic.
process Replica \in Replicas
begin
    rloop: while ~terminated do
        either
            BecomeLeader(self);
        or
            HandlePrepare(self);
        or
            HandlePrepareReplies(self);
        or
            TakeNewRequest(self);
        or
            HandleAccept(self);
        or
            HandleAcceptReplies(self);
        \* or
        \*     HandleCommitNotice(self);
        end either;
    end while;
end process;

end algorithm; *)

----------

\* BEGIN TRANSLATION (chksum(pcal) = "6a27524b" /\ chksum(tla) = "de08f6a6")
VARIABLES msgs, node, pending, observed, pc

(* define statement *)
Unobserved(e) == e \notin Range(observed)

requestsMade == {e.cmd: e \in {e \in Range(observed): e.type = "Req"}}

responsesGot == {e.cmd: e \in {e \in Range(observed): e.type = "Ack"}}

terminated == /\ Len(pending) = 0
              /\ Cardinality(requestsMade) = NumCommands
              /\ Cardinality(responsesGot) = NumCommands


vars == << msgs, node, pending, observed, pc >>

ProcSet == (Replicas)

Init == (* Global variables *)
        /\ msgs = {}
        /\ node = [r \in Replicas |-> NullNode]
        /\ pending = InitPending
        /\ observed = <<>>
        /\ pc = [self \in ProcSet |-> "rloop"]

rloop(self) == /\ pc[self] = "rloop"
               /\ IF ~terminated
                     THEN /\ \/ /\ node[self].leader # self
                                /\ \E b \in Ballots:
                                     /\ /\ b > node[self].balMaxKnown
                                        /\ ~\E m \in msgs: (m.type = "Prepare") /\ (m.bal = b)
                                     /\ node' = [node EXCEPT ![self].leader = self,
                                                             ![self].balPrepared = 0,
                                                             ![self].balMaxKnown = b,
                                                             ![self].insts = [s \in Slots |->
                                                                                [node[self].insts[s]
                                                                                    EXCEPT !.status = IF InProgress(@)
                                                                                                        THEN "Preparing"
                                                                                                        ELSE @]]]
                                     /\ msgs' = (msgs \cup ({PrepareMsg(self, b),
                                                             PrepareReplyMsg(self, b, VotesByNode(node'[self]))}))
                                /\ UNCHANGED <<pending, observed>>
                             \/ /\ \E m \in msgs:
                                     /\ /\ m.type = "Prepare"
                                        /\ m.bal > node[self].balMaxKnown
                                     /\ node' = [node EXCEPT ![self].leader = m.src,
                                                             ![self].balMaxKnown = m.bal,
                                                             ![self].insts = [s \in Slots |->
                                                                                [node[self].insts[s]
                                                                                    EXCEPT !.status = IF InProgress(@)
                                                                                                        THEN "Preparing"
                                                                                                        ELSE @]]]
                                     /\ msgs' = (msgs \cup ({PrepareReplyMsg(self, m.bal, VotesByNode(node'[self]))}))
                                /\ UNCHANGED <<pending, observed>>
                             \/ /\ /\ node[self].leader = self
                                   /\ node[self].balPrepared = 0
                                /\ LET prs == {m \in msgs: /\ m.type = "PrepareReply"
                                                           /\ m.bal = node[self].balMaxKnown} IN
                                     /\ Cardinality(prs) >= MajorityNum
                                     /\ node' = [node EXCEPT ![self].balPrepared = node[self].balMaxKnown,
                                                             ![self].insts = [s \in Slots |->
                                                                                [node[self].insts[s]
                                                                                    EXCEPT !.status = IF InProgress(@)
                                                                                                        THEN "Accepting"
                                                                                                        ELSE @,
                                                                                           !.cmd = HighestVotedCmd(prs, s)]]]
                                     /\ msgs' = (msgs \cup ({AcceptMsg(self, node'[self].balPrepared, s, node'[self].insts[s].cmd):
                                                             s \in {s \in Slots: InProgress(node'[self].insts[s].status)}}))
                                /\ UNCHANGED <<pending, observed>>
                             \/ /\ /\ node[self].leader = self
                                   /\ node[self].balPrepared = node[self].balMaxKnown
                                   /\ \E s \in Slots: node[self].insts[s].status = "Empty"
                                   /\ Len(pending) > 0
                                /\ LET s == FirstEmptySlot(node[self].insts) IN
                                     LET c == Head(pending) IN
                                       /\ node' = [node EXCEPT ![self].insts[s].status = "Accepting",
                                                               ![self].insts[s].cmd = c,
                                                               ![self].insts[s].voted.bal = node[self].balPrepared,
                                                               ![self].insts[s].voted.cmd = c]
                                       /\ msgs' = (msgs \cup ({AcceptMsg(self, node'[self].balPrepared, s, c),
                                                               AcceptReplyMsg(self, node'[self].balPrepared, s)}))
                                       /\ observed' = observed \o SelectSeq((<<ReqEvent(c)>>), Unobserved)
                                       /\ pending' = Tail(pending)
                             \/ /\ \E m \in msgs:
                                     /\ /\ m.type = "Accept"
                                        /\ m.bal >= node[self].balMaxKnown
                                        /\ node[self].insts[m.slot].voted.bal < m.bal
                                     /\ node' = [node EXCEPT ![self].leader = m.src,
                                                             ![self].balMaxKnown = m.bal,
                                                             ![self].insts[m.slot].status = "Accepting",
                                                             ![self].insts[m.slot].cmd = m.cmd,
                                                             ![self].insts[m.slot].voted.bal = m.bal,
                                                             ![self].insts[m.slot].voted.cmd = m.cmd]
                                     /\ msgs' = (msgs \cup ({AcceptReplyMsg(self, m.bal, m.slot)}))
                                /\ UNCHANGED <<pending, observed>>
                             \/ /\ /\ node[self].leader = self
                                   /\ node[self].balPrepared = node[self].balMaxKnown
                                   /\ node[self].commitUpTo < NumCommands
                                   /\ node[self].insts[node[self].commitUpTo+1].status = "Accepting"
                                /\ LET s == node[self].commitUpTo + 1 IN
                                     LET cmd == node[self].insts[s].cmd IN
                                       LET val == node[self].kvalue IN
                                         LET ars == {m \in msgs: /\ m.type = "AcceptReply"
                                                                 /\ m.slot = s
                                                                 /\ m.bal = node[self].balPrepared} IN
                                           /\ Cardinality(ars) >= MajorityNum
                                           /\ node' = [node EXCEPT ![self].insts[s].status = "Committed",
                                                                   ![self].commitUpTo = s,
                                                                   ![self].kvalue = IF cmd \in Writes THEN cmd ELSE @]
                                           /\ observed' = observed \o SelectSeq((<<AckEvent(cmd, val)>>), Unobserved)
                                           /\ msgs' = (msgs \cup ({CommitNoticeMsg(s)}))
                                /\ UNCHANGED pending
                          /\ pc' = [pc EXCEPT ![self] = "rloop"]
                     ELSE /\ pc' = [pc EXCEPT ![self] = "Done"]
                          /\ UNCHANGED << msgs, node, pending, observed >>

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
