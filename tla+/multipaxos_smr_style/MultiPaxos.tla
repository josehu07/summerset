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
CONSTANT Servers,   \* symmetric set of server nodes
         Writes,    \* symmetric set of write commands (each w/ unique value)
         Reads,     \* symmetric set of read commands
         MaxBallot  \* maximum ballot pickable for leader preemption

ServersAssumption == /\ IsFiniteSet(Servers)
                     /\ Cardinality(Servers) >= 1

WritesAssumption == /\ IsFiniteSet(Writes)
                    /\ Cardinality(Writes) >= 1
                    /\ 0 \notin Writes  \* use 0 as the null value

ReadsAssumption == /\ IsFiniteSet(Reads)
                   /\ Cardinality(Reads) >= 0

MaxBallotAssumption == /\ MaxBallot \in Nat
                       /\ MaxBallot >= 2

ASSUME /\ ServersAssumption
       /\ WritesAssumption
       /\ ReadsAssumption
       /\ MaxBallotAssumption

----------

(********************************)
(* Useful constants & typedefs. *)
(********************************)
AllValues == {0} \cup Writes

Commands == Writes \cup Reads

NumCommands == Cardinality(Commands)

ClientEvents ==      [type: {"Req"}, cmd: Commands]
                \cup [type: {"Ack"}, cmd: Commands, val: AllValues]

ReqEvent(c) == [type |-> "Req", cmd |-> c]

AckEvent(c, v) == [type |-> "Ack", cmd |-> c, val |-> v]
                        \* val is the old value for a write command

InitReqs == {ReqEvent(c): c \in Commands}

\* MajorityNum == (Cardinality(Servers) \div 2) + 1

\* Ballots == 1..MaxBallot

\* Slots == 1..NumReqs

\* Statuses == {"Preparing", "Accepting", "Committed"}

\* SlotStates == [bal: {0} \cup Ballots,
\*                status: {"Null"} \cup Statuses,
\*                cmd: {"Empty"} \cup Commands,
\*                external: BOOLEAN]

\* NullSlot == [bal |-> 0,
\*              status |-> "Null",
\*              cmd |-> "Empty",
\*              external |-> FALSE]

\* NodeStates == [balPrepSent: {0} \cup Ballots,
\*                balPrepared: {0} \cup Ballots,
\*                balMaxKnown: {0} \cup Ballots,
\*                insts: [Slots -> SlotStates]]

\* NullNode == [balPrepSent |-> 0,
\*              balPrepared |-> 0,
\*              balMaxKnown |-> 0,
\*              insts |-> [s \in Slots |-> NullSlot]]

NodeStates == [kvalue: AllValues,
               served: SUBSET Commands]

NullNode == [kvalue |-> 0,
             served |-> {}]

Messages == {}

----------

(******************************)
(* Main algorithm in PlusCal. *)
(******************************)
(*--algorithm MultiPaxos

variable msgs = {},                            \* messages in the network
         node = [r \in Servers |-> NullNode],  \* server node state
         pending = InitReqs,                   \* set of pending reqs
         observed = <<>>;                      \* client observed events

define
    observedSet == {observed[n]: n \in 1..Len(observed)}

    requestsMade == {e.cmd: e \in {e \in observedSet: e.type = "Req"}}
    
    responsesGot == {e.cmd: e \in {e \in observedSet: e.type = "Ack"}}

    terminated == /\ Cardinality(requestsMade) = NumCommands
                  /\ Cardinality(responsesGot) = NumCommands
end define;

\* Send message helper.
macro Send(msg) begin
    msgs := msgs \cup {msg};
end macro;

\* Observe events helper.
macro Observe(seq) begin
    observed := observed \o seq;
end macro;

\* TODO: remove me
macro ServeRead(s) begin
    with e \in pending do
        await /\ e.type = "Req"
              /\ e.cmd \in Reads
              /\ e.cmd \notin node[s].served
              /\ e \notin observedSet;
        Observe(<<e, AckEvent(e.cmd, node[s].kvalue)>>);
        node[s].served := node[s].served \cup {e.cmd};
        pending := pending \ {e};
    end with;
end macro;

\* TODO: remove me
macro ServeWrite(s) begin
    with e \in pending do
        await /\ e.type = "Req"
              /\ e.cmd \in Writes
              /\ e.cmd \notin node[s].served
              /\ e \notin observedSet;
        Observe(<<e, AckEvent(e.cmd, node[s].kvalue)>>);
        node[s].kvalue := e.cmd ||  \* treat the write itself as the value
        node[s].served := node[s].served \cup {e.cmd};
        pending := pending \ {e};
    end with;
end macro;

\* Server server node logic.
process Server \in Servers
begin
    rloop: while ~terminated do
        either
            ServeRead(self);
        or
            ServeWrite(self);
        end either;
    end while;
end process;

end algorithm; *)

----------

\* BEGIN TRANSLATION (chksum(pcal) = "a9e7c72e" /\ chksum(tla) = "66bffa97")
VARIABLES msgs, node, pending, observed, pc

(* define statement *)
observedSet == {observed[n]: n \in 1..Len(observed)}

requestsMade == {e.cmd: e \in {e \in observedSet: e.type = "Req"}}

responsesGot == {e.cmd: e \in {e \in observedSet: e.type = "Ack"}}

terminated == /\ Cardinality(requestsMade) = NumCommands
              /\ Cardinality(responsesGot) = NumCommands


vars == << msgs, node, pending, observed, pc >>

ProcSet == (Servers)

Init == (* Global variables *)
        /\ msgs = {}
        /\ node = [r \in Servers |-> NullNode]
        /\ pending = InitReqs
        /\ observed = <<>>
        /\ pc = [self \in ProcSet |-> "rloop"]

rloop(self) == /\ pc[self] = "rloop"
               /\ IF ~terminated
                     THEN /\ \/ /\ \E e \in pending:
                                     /\ /\ e.type = "Req"
                                        /\ e.cmd \in Reads
                                        /\ e.cmd \notin node[self].served
                                        /\ e \notin observedSet
                                     /\ observed' = observed \o (<<e, AckEvent(e.cmd, node[self].kvalue)>>)
                                     /\ node' = [node EXCEPT ![self].served = node[self].served \cup {e.cmd}]
                                     /\ pending' = pending \ {e}
                             \/ /\ \E e \in pending:
                                     /\ /\ e.type = "Req"
                                        /\ e.cmd \in Writes
                                        /\ e.cmd \notin node[self].served
                                        /\ e \notin observedSet
                                     /\ observed' = observed \o (<<e, AckEvent(e.cmd, node[self].kvalue)>>)
                                     /\ node' = [node EXCEPT ![self].kvalue = e.cmd,
                                                             ![self].served = node[self].served \cup {e.cmd}]
                                     /\ pending' = pending \ {e}
                          /\ pc' = [pc EXCEPT ![self] = "rloop"]
                     ELSE /\ pc' = [pc EXCEPT ![self] = "Done"]
                          /\ UNCHANGED << node, pending, observed >>
               /\ msgs' = msgs

Server(self) == rloop(self)

(* Allow infinite stuttering to prevent deadlock on termination. *)
Terminating == /\ \A self \in ProcSet: pc[self] = "Done"
               /\ UNCHANGED vars

Next == (\E self \in Servers: Server(self))
           \/ Terminating

Spec == Init /\ [][Next]_vars

Termination == <>(\A self \in ProcSet: pc[self] = "Done")

\* END TRANSLATION

====
