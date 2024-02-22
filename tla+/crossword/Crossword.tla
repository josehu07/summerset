(**********************************************************************************)
(* Crossword protocol in state machine replication (SMR) style with write/read    *)
(* commands on a single key. Payload of each write is allowed to be erasure-coded *)
(* in a (N, M) scheme and follow balanced Round-Robin assignment policies.        *)
(* Careful adjustments to the accpetance condition are made to retaining the same *)
(* fault-tolerance level as classic MultiPaxos.                                   *)
(*                                                                                *)
(* See multipaxos_smr_style/MultiPaxos.tla for detailed description of base spec. *)
(**********************************************************************************)

---- MODULE Crossword ----
EXTENDS FiniteSets, Sequences, Integers, TLC

(*******************************)
(* Model inputs & assumptions. *)
(*******************************)
CONSTANT Replicas,        \* symmetric set of server nodes
         Writes,          \* symmetric set of write commands (each w/ unique value)
         Reads,           \* symmetric set of read commands
         MaxBallot,       \* maximum ballot pickable for leader preemption
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

NumCommands == Cardinality(Commands)

Range(func) == {func[i]: i \in DOMAIN func}

MajorityNum == (Cardinality(Replicas) \div 2) + 1

Shards == Replicas

NumDataShards == MajorityNum

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

Slots == 1..NumCommands

Statuses == {"Preparing", "Accepting", "Committed"}

InstStates == [status: {"Empty"} \cup Statuses,
               cmd: {"nil"} \cup Commands,
               shards: SUBSET Shards,
               voted: [bal: {0} \cup Ballots,
                       cmd: {"nil"} \cup Commands,
                       shards: SUBSET Shards]]

NullInst == [status |-> "Empty",
             cmd |-> "nil",
             shards |-> {},
             voted |-> [bal |-> 0, cmd |-> "nil", shards |-> {}]]

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

\* Erasure-coding related expressions.
BigEnoughUnderFaults(g, u) == 
    \* Is g a large enough subset of u under promised fault-tolerance?
    Cardinality(g) >= (Cardinality(u) + MajorityNum - Cardinality(Replicas))

SubsetsUnderFaults(u) ==
    \* Set of subsets of u we consider under promised fault-tolerance.
    {g \in SUBSET u: BigEnoughUnderFaults(g, u)}
        
IsGoodCoverageSet(cs) ==
    \* Is cs a coverage set (i.e., a set of sets of shards) from which
    \* we can reconstruct the original data?
    Cardinality(UNION cs) >= NumDataShards

ShardToIdx == CHOOSE map \in [Shards -> 1..Cardinality(Shards)]:
                    Cardinality(Range(map)) = Cardinality(Shards)

IdxToShard == [i \in 1..Cardinality(Shards) |->
                    CHOOSE r \in Shards: ShardToIdx[r] = i]

ValidAssignments ==
    \* Set of all valid shard assignments.
    {[r \in Replicas |-> {IdxToShard[((i-1) % Cardinality(Shards)) + 1]:
                          i \in (ShardToIdx[r])..(ShardToIdx[r]+na-1)}]:
     na \in 1..MajorityNum}

\* ASSUME Print(ValidAssignments, TRUE)

\* Service-internal messages.
PrepareMsgs == [type: {"Prepare"}, src: Replicas,
                                   bal: Ballots]

PrepareMsg(r, b) == [type |-> "Prepare", src |-> r,
                                         bal |-> b]

InstsVotes == [Slots -> [bal: {0} \cup Ballots,
                         cmd: {"nil"} \cup Commands,
                         shards: SUBSET Shards]]

VotesByNode(n) == [s \in Slots |-> n.insts[s].voted]

PrepareReplyMsgs == [type: {"PrepareReply"}, src: Replicas,
                                             bal: Ballots,
                                             votes: InstsVotes]

PrepareReplyMsg(r, b, iv) ==
    [type |-> "PrepareReply", src |-> r,
                              bal |-> b,
                              votes |-> iv]

PreparedConditionAndCommand(prs, s) ==
    \* examines a set of PrepareReplies and returns a tuple:
    \* (if the given slot can be decided as prepared,
    \*  the prepared command if forced,
    \*  known shards of the command if forced)
    LET ppr == CHOOSE ppr \in prs:
                    \A pr \in prs: pr.votes[s].bal =< ppr.votes[s].bal
    IN  IF      /\ BigEnoughUnderFaults(prs, Replicas)
                /\ \A pr \in prs: pr.votes[s].bal = 0
            THEN [prepared |-> TRUE, cmd |-> "nil", shards |-> {}]
                    \* prepared, can choose any
        ELSE IF /\ BigEnoughUnderFaults(prs, Replicas)
                /\ IsGoodCoverageSet(
                        {pr.votes[s].shards:
                         pr \in {pr \in prs:
                                 pr.votes[s].cmd = ppr.votes[s].cmd}})
            THEN [prepared |-> TRUE,
                  cmd |-> ppr.votes[s].cmd,
                  shards |-> UNION
                             {pr.votes[s].shards:
                              pr \in {pr \in prs:
                                      pr.votes[s].cmd = ppr.votes[s].cmd}}]
                    \* prepared, command forced
        ELSE IF /\ BigEnoughUnderFaults(prs, Replicas)
                /\ ~IsGoodCoverageSet(
                        {pr.votes[s].shards:
                         pr \in {pr \in prs:
                                 pr.votes[s].cmd = ppr.votes[s].cmd}})
            THEN [prepared |-> TRUE, cmd |-> "nil", shards |-> {}]
                    \* prepared, can choose any
        ELSE [prepared |-> FALSE, cmd |-> "nil", shard |-> {}]
                    \* not prepared

AcceptMsgs == [type: {"Accept"}, src: Replicas,
                                 dst: Replicas,
                                 bal: Ballots,
                                 slot: Slots,
                                 cmd: Commands,
                                 shards: SUBSET Shards]

AcceptMsg(r, d, b, s, c, sds) == [type |-> "Accept", src |-> r,
                                                     dst |-> d,
                                                     bal |-> b,
                                                     slot |-> s,
                                                     cmd |-> c,
                                                     shards |-> sds]

AcceptReplyMsgs == [type: {"AcceptReply"}, src: Replicas,
                                           bal: Ballots,
                                           slot: Slots,
                                           shards: SUBSET Shards]

AcceptReplyMsg(r, b, s, sds) ==
    [type |-> "AcceptReply", src |-> r,
                             bal |-> b,
                             slot |-> s,
                             shards |-> sds]

CommittedCondition(ars, s) ==
    \* the condition which decides if a set of AcceptReplies makes an
    \* instance committed
    /\ BigEnoughUnderFaults(ars, Replicas)
    /\ \A group \in SubsetsUnderFaults(ars):
            IsGoodCoverageSet({ar.shards: ar \in group})

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
(*--algorithm Crossword

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
    \* when there are a set of PrepareReplies of desired ballot that satisfy
    \* the prepared condition
    with prs = {m \in msgs: /\ m.type = "PrepareReply"
                            /\ m.bal = node[r].balMaxKnown},
         exam = [s \in Slots |-> PreparedConditionAndCommand(prs, s)]
    do
        await \A s \in Slots: exam[s].prepared;
        \* marks this ballot as prepared and saves highest voted command
        \* in each slot if any
        node[r].balPrepared := node[r].balMaxKnown ||
        node[r].insts :=
            [s \in Slots |->
                [node[r].insts[s]
                    EXCEPT !.status = IF /\ \/ @ = "Empty"
                                            \/ @ = "Preparing"
                                            \/ @ = "Accepting"
                                         /\ exam[s].cmd # "nil"
                                        THEN "Accepting"
                                      ELSE IF @ = "Committed"
                                        THEN "Committed"
                                      ELSE "Empty",
                           !.cmd    = exam[s].cmd,
                           !.shards = exam[s].shards]];
        \* pick a reasonable shard assignment and send Accept messages for
        \* in-progress instances according to it
        with assign \in ValidAssignments do
            Send({AcceptMsg(r, d, node[r].balPrepared, s,
                            node[r].insts[s].cmd, assign[d]):
                  s \in {s \in Slots:
                            node[r].insts[s].status = "Accepting"},
                  d \in Replicas}
            \cup {AcceptReplyMsg(r, node[r].balPrepared, s, assign[r]):
                  s \in {s \in Slots:
                            node[r].insts[s].status = "Accepting"}});
        end with;
    end with;
end macro;

\* A prepared leader takes a new request to fill the next empty slot.
macro TakeNewRequest(r) begin
    \* if I'm a prepared leader and there's pending request
    await /\ node[r].leader = r
          /\ node[r].balPrepared = node[r].balMaxKnown
          /\ \E s \in Slots: node[r].insts[s].status = "Empty"
          /\ Len(UnseenPending(node[r].insts)) > 0;
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
        node[r].insts[s].voted.cmd := c ||
        node[r].insts[s].voted.shards := Shards;
        \* pick a reasonable shard assignment, send Accept messages, and
        \* reply to myself instantly
        with assign \in ValidAssignments do
            Send({AcceptMsg(r, d, node[r].balPrepared, s, c, assign[d]):
                  d \in Replicas}
            \cup {AcceptReplyMsg(r, node[r].balPrepared, s, assign[r])});
        end with;
        \* append to observed events sequence if haven't yet
        Observe(ReqEvent(c));
    end with;
end macro;

\* Replica replies to an Accept message.
macro HandleAccept(r) begin
    \* if receiving an unreplied Accept message with valid ballot
    with m \in msgs do
        await /\ m.type = "Accept"
              /\ m.dst = r
              /\ m.bal >= node[r].balMaxKnown
              /\ m.bal > node[r].insts[m.slot].voted.bal;
        \* update node states and corresponding instance's states
        node[r].leader := m.src ||
        node[r].balMaxKnown := m.bal ||
        node[r].insts[m.slot].status := "Accepting" ||
        node[r].insts[m.slot].cmd := m.cmd ||
        node[r].insts[m.slot].shards := m.shards ||
        node[r].insts[m.slot].voted.bal := m.bal ||
        node[r].insts[m.slot].voted.cmd := m.cmd ||
        node[r].insts[m.slot].voted.shards := m.shards;
        \* send back AcceptReply
        Send({AcceptReplyMsg(r, m.bal, m.slot, m.shards)});
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
                \* W.L.O.G., only enabling the next slot after commitUpTo
                \* here to make the body of this macro simpler
    \* for this slot, when there is a set of AcceptReplies that satisfy the
    \* committed condition
    with s = node[r].commitUpTo + 1,
         c = node[r].insts[s].cmd,
         v = node[r].kvalue,
         ars = {m \in msgs: /\ m.type = "AcceptReply"
                            /\ m.slot = s
                            /\ m.bal = node[r].balPrepared}
    do
        await CommittedCondition(ars, s);
        \* marks this slot as committed and apply command
        node[r].insts[s].status := "Committed" ||
        node[r].commitUpTo := s ||
        node[r].kvalue := IF c \in Writes THEN c ELSE @;
        \* append to observed events sequence if haven't yet, and remove
        \* the command from pending
        Observe(AckEvent(c, v));
        Resolve(c);
        \* broadcast CommitNotice to followers
        Send({CommitNoticeMsg(s)});
    end with;
end macro;

\* Replica receives new commit notification.
macro HandleCommitNotice(r) begin
    \* if I'm a follower waiting on CommitNotice
    await /\ node[r].leader # r
          /\ node[r].commitUpTo < NumCommands
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
            TakeNewRequest(self);
        or
            HandleAccept(self);
        or
            HandleAcceptReplies(self);
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

\* BEGIN TRANSLATION (chksum(pcal) = "d7fc8acf" /\ chksum(tla) = "73b650be")
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
                                     LET exam == [s \in Slots |-> PreparedConditionAndCommand(prs, s)] IN
                                       /\ \A s \in Slots: exam[s].prepared
                                       /\ node' = [node EXCEPT ![self].balPrepared = node[self].balMaxKnown,
                                                               ![self].insts = [s \in Slots |->
                                                                                   [node[self].insts[s]
                                                                                       EXCEPT !.status = IF /\ \/ @ = "Empty"
                                                                                                               \/ @ = "Preparing"
                                                                                                               \/ @ = "Accepting"
                                                                                                            /\ exam[s].cmd # "nil"
                                                                                                           THEN "Accepting"
                                                                                                         ELSE IF @ = "Committed"
                                                                                                           THEN "Committed"
                                                                                                         ELSE "Empty",
                                                                                              !.cmd    = exam[s].cmd,
                                                                                              !.shards = exam[s].shards]]]
                                       /\ \E assign \in ValidAssignments:
                                            msgs' = (msgs \cup (     {AcceptMsg(self, d, node'[self].balPrepared, s,
                                                                                node'[self].insts[s].cmd, assign[d]):
                                                                      s \in {s \in Slots:
                                                                                node'[self].insts[s].status = "Accepting"},
                                                                      d \in Replicas}
                                                     \cup {AcceptReplyMsg(self, node'[self].balPrepared, s, assign[self]):
                                                           s \in {s \in Slots:
                                                                     node'[self].insts[s].status = "Accepting"}}))
                                /\ UNCHANGED <<pending, observed, crashed>>
                             \/ /\ /\ node[self].leader = self
                                   /\ node[self].balPrepared = node[self].balMaxKnown
                                   /\ \E s \in Slots: node[self].insts[s].status = "Empty"
                                   /\ Len(UnseenPending(node[self].insts)) > 0
                                /\ LET s == FirstEmptySlot(node[self].insts) IN
                                     LET c == Head(UnseenPending(node[self].insts)) IN
                                       /\ node' = [node EXCEPT ![self].insts[s].status = "Accepting",
                                                               ![self].insts[s].cmd = c,
                                                               ![self].insts[s].voted.bal = node[self].balPrepared,
                                                               ![self].insts[s].voted.cmd = c,
                                                               ![self].insts[s].voted.shards = Shards]
                                       /\ \E assign \in ValidAssignments:
                                            msgs' = (msgs \cup (     {AcceptMsg(self, d, node'[self].balPrepared, s, c, assign[d]):
                                                                      d \in Replicas}
                                                     \cup {AcceptReplyMsg(self, node'[self].balPrepared, s, assign[self])}))
                                       /\ IF (ReqEvent(c)) \notin Range(observed)
                                             THEN /\ observed' = Append(observed, (ReqEvent(c)))
                                             ELSE /\ TRUE
                                                  /\ UNCHANGED observed
                                /\ UNCHANGED <<pending, crashed>>
                             \/ /\ \E m \in msgs:
                                     /\ /\ m.type = "Accept"
                                        /\ m.dst = self
                                        /\ m.bal >= node[self].balMaxKnown
                                        /\ m.bal > node[self].insts[m.slot].voted.bal
                                     /\ node' = [node EXCEPT ![self].leader = m.src,
                                                             ![self].balMaxKnown = m.bal,
                                                             ![self].insts[m.slot].status = "Accepting",
                                                             ![self].insts[m.slot].cmd = m.cmd,
                                                             ![self].insts[m.slot].shards = m.shards,
                                                             ![self].insts[m.slot].voted.bal = m.bal,
                                                             ![self].insts[m.slot].voted.cmd = m.cmd,
                                                             ![self].insts[m.slot].voted.shards = m.shards]
                                     /\ msgs' = (msgs \cup ({AcceptReplyMsg(self, m.bal, m.slot, m.shards)}))
                                /\ UNCHANGED <<pending, observed, crashed>>
                             \/ /\ /\ node[self].leader = self
                                   /\ node[self].balPrepared = node[self].balMaxKnown
                                   /\ node[self].commitUpTo < NumCommands
                                   /\ node[self].insts[node[self].commitUpTo+1].status = "Accepting"
                                /\ LET s == node[self].commitUpTo + 1 IN
                                     LET c == node[self].insts[s].cmd IN
                                       LET v == node[self].kvalue IN
                                         LET ars == {m \in msgs: /\ m.type = "AcceptReply"
                                                                 /\ m.slot = s
                                                                 /\ m.bal = node[self].balPrepared} IN
                                           /\ CommittedCondition(ars, s)
                                           /\ node' = [node EXCEPT ![self].insts[s].status = "Committed",
                                                                   ![self].commitUpTo = s,
                                                                   ![self].kvalue = IF c \in Writes THEN c ELSE @]
                                           /\ IF (AckEvent(c, v)) \notin Range(observed)
                                                 THEN /\ observed' = Append(observed, (AckEvent(c, v)))
                                                 ELSE /\ TRUE
                                                      /\ UNCHANGED observed
                                           /\ pending' = RemovePending(c)
                                           /\ msgs' = (msgs \cup ({CommitNoticeMsg(s)}))
                                /\ UNCHANGED crashed
                             \/ /\ IF CommitNoticeOn
                                      THEN /\ /\ node[self].leader # self
                                              /\ node[self].commitUpTo < NumCommands
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
