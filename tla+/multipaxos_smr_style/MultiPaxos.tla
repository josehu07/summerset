(**********************************************************************************)
(* MultiPaxos in state machine replication (SMR) style with read/write commands.  *)
(* Linearizability checked from global client's point of view.                    *)
(**********************************************************************************)

---- MODULE MultiPaxos ----
EXTENDS FiniteSets, Sequences, Integers, TLC

(*******************************)
(* Model inputs & assumptions. *)
(*******************************)
CONSTANT Replicas, Keys, Vals, NumReqs, MaxBallot

ReplicasAssumption == /\ IsFiniteSet(Replicas)
                      /\ Cardinality(Replicas) >= 3

KeysAssumption == /\ IsFiniteSet(Keys)
                  /\ Keys # {}

ValsAssumption == /\ IsFiniteSet(Vals)
                  /\ Cardinality(Vals) >= 2
                  /\ 0 \notin Vals  \* use 0 as nil value

NumReqsAssumption == /\ NumReqs \in Nat
                     /\ NumReqs >= 2

MaxBallotAssumption == /\ MaxBallot \in Nat
                       /\ MaxBallot >= 2

ASSUME /\ ReplicasAssumption
       /\ KeysAssumption
       /\ ValsAssumption
       /\ NumReqsAssumption
       /\ MaxBallotAssumption

----------

(********************************)
(* Useful constants & typedefs. *)
(********************************)
\* MajorityNum == (Cardinality(Replicas) \div 2) + 1

\* Ballots == 1..MaxBallot

\* Slots == 1..NumReqs

\* Statuses == {"Preparing", "Accepting", "Committed"}

Commands ==      [op: {"Read"}, id: 1..NumReqs, key: Keys]
            \cup [op: {"Write"}, id: 1..NumReqs, key: Keys, val: Vals]

ReadCommand(i, k) == [op |-> "Read", id |-> i, key |-> k]

WriteCommand(i, k, v) == [op |-> "Write", id |-> i, key |-> k, val |-> v]

Results ==      [op: {"Read"}, id: 1..NumReqs,
                               val: {0} \cup Vals]
           \cup [op: {"Write"}, id: 1..NumReqs,
                                old: {0} \cup Vals]

ReadResult(i, v) == [op |-> "Read", id |-> i, val |-> v]

WriteResult(i, o) == [op |-> "Write", id |-> i, old |-> o]

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

NodeStates == [kvmap: [Keys -> {0} \cup Vals],
               served: SUBSET (1..NumReqs)]

NullNode == [kvmap |-> [k \in Keys |-> 0],
             served |-> {}]

RequestMsgs == [type: {"Request"}, cmd: Commands]

RequestMsg(c) == [type |-> "Request", cmd |-> c]

ResponseMsgs == [type: {"Response"}, res: Results]

ResponseMsg(r) == [type |-> "Response", res |-> r]

ClientEvents == RequestMsgs \cup ResponseMsgs

Messages == RequestMsgs \cup ResponseMsgs

----------

(******************************)
(* Main algorithm in PlusCal. *)
(******************************)
(*--algorithm MultiPaxos

variable msgs = {},                            \* messages in the network
         node = [r \in Replicas |-> NullNode], \* replica node state
         observed = <<>>;                      \* client observed events

define
    observedSet == {observed[n]: n \in 1..Len(observed)}

    requestsMade == {e.cmd.id: e \in {e \in observedSet: e.type = "Request"}}
    
    responsesGot == {e.res.id: e \in {e \in observedSet: e.type = "Response"}}

    terminated == /\ Cardinality(requestsMade) = NumReqs
                  /\ Cardinality(responsesGot) = NumReqs
end define;

\* Send message helper.
macro Send(m) begin
    msgs := msgs \cup {m};
end macro;

\* TODO: remove me
macro ServeRead(r) begin
    with m \in msgs do
        await /\ (m.type = "Request")
              /\ (m.cmd.op = "Read")
              /\ (m.cmd.id \notin node[r].served);
        with res = ReadResult(m.cmd.id, node[r].kvmap[m.cmd.key]),
             rep = ResponseMsg(res)
        do
            Send(rep);
            node[r].served := node[r].served \cup {m.cmd.id};
        end with;
    end with;
end macro;

\* TODO: remove me
macro ServeWrite(r) begin
    with m \in msgs do
        await /\ (m.type = "Request")
              /\ (m.cmd.op = "Write")
              /\ (m.cmd.id \notin node[r].served);
        with res = WriteResult(m.cmd.id, node[r].kvmap[m.cmd.key]),
             rep = ResponseMsg(res)
        do
            Send(rep);
            node[r].kvmap[m.cmd.key] := m.cmd.val ||
            node[r].served := node[r].served \cup {m.cmd.id};
        end with;
    end with;
end macro;

\* Some client makes a new read request to the service.
macro RequestRead() begin
    await Cardinality(requestsMade) < NumReqs;
    with k \in Keys,
         id = Cardinality(requestsMade) + 1,
         cmd = ReadCommand(id, k),
         req = RequestMsg(cmd)
    do
        Send(req);
        observed := Append(observed, req);
    end with;
end macro;

\* Some client makes a new write request to the service.
macro RequestWrite() begin
    await Cardinality(requestsMade) < NumReqs;
    with k \in Keys,
         v \in Vals,
         id = Cardinality(requestsMade) + 1,
         cmd = WriteCommand(id, k, v),
         req = RequestMsg(cmd)
    do
        Send(req);
        observed := Append(observed, req);
    end with;
end macro;

\* Client receives response from the service.
macro GetResponse() begin
    await Cardinality(responsesGot) < Cardinality(requestsMade);
    with m \in msgs do
        await (m.type = "Response") /\ (m.res.id \notin responsesGot);
        observed := Append(observed, m);
    end with;
end macro;

\* Server replica node logic.
process Replica \in Replicas
begin
    rloop: while ~terminated do
        either
            ServeRead(self);
        or
            ServeWrite(self);
        end either;
    end while;
end process;

\* Global client(s) logic.
process Client = "Client"
begin
    cloop: while ~terminated do
        either
            RequestRead();
        or
            RequestWrite();
        or
            GetResponse();
        end either;
    end while;
end process;

end algorithm; *)

----------

\* BEGIN TRANSLATION (chksum(pcal) = "c98c4bfe" /\ chksum(tla) = "338c51ee")
VARIABLES msgs, node, observed, pc

(* define statement *)
observedSet == {observed[n]: n \in 1..Len(observed)}

requestsMade == {e.cmd.id: e \in {e \in observedSet: e.type = "Request"}}

responsesGot == {e.res.id: e \in {e \in observedSet: e.type = "Response"}}

terminated == /\ Cardinality(requestsMade) = NumReqs
              /\ Cardinality(responsesGot) = NumReqs


vars == << msgs, node, observed, pc >>

ProcSet == (Replicas) \cup {"Client"}

Init == (* Global variables *)
        /\ msgs = {}
        /\ node = [r \in Replicas |-> NullNode]
        /\ observed = <<>>
        /\ pc = [self \in ProcSet |-> CASE self \in Replicas -> "rloop"
                                        [] self = "Client" -> "cloop"]

rloop(self) == /\ pc[self] = "rloop"
               /\ IF ~terminated
                     THEN /\ \/ /\ \E m \in msgs:
                                     /\ /\ (m.type = "Request")
                                        /\ (m.cmd.op = "Read")
                                        /\ (m.cmd.id \notin node[self].served)
                                     /\ LET res == ReadResult(m.cmd.id, node[self].kvmap[m.cmd.key]) IN
                                          LET rep == ResponseMsg(res) IN
                                            /\ msgs' = (msgs \cup {rep})
                                            /\ node' = [node EXCEPT ![self].served = node[self].served \cup {m.cmd.id}]
                             \/ /\ \E m \in msgs:
                                     /\ /\ (m.type = "Request")
                                        /\ (m.cmd.op = "Write")
                                        /\ (m.cmd.id \notin node[self].served)
                                     /\ LET res == WriteResult(m.cmd.id, node[self].kvmap[m.cmd.key]) IN
                                          LET rep == ResponseMsg(res) IN
                                            /\ msgs' = (msgs \cup {rep})
                                            /\ node' = [node EXCEPT ![self].kvmap[m.cmd.key] = m.cmd.val,
                                                                    ![self].served = node[self].served \cup {m.cmd.id}]
                          /\ pc' = [pc EXCEPT ![self] = "rloop"]
                     ELSE /\ pc' = [pc EXCEPT ![self] = "Done"]
                          /\ UNCHANGED << msgs, node >>
               /\ UNCHANGED observed

Replica(self) == rloop(self)

cloop == /\ pc["Client"] = "cloop"
         /\ IF ~terminated
               THEN /\ \/ /\ Cardinality(requestsMade) < NumReqs
                          /\ \E k \in Keys:
                               LET id == Cardinality(requestsMade) + 1 IN
                                 LET cmd == ReadCommand(id, k) IN
                                   LET req == RequestMsg(cmd) IN
                                     /\ msgs' = (msgs \cup {req})
                                     /\ observed' = Append(observed, req)
                       \/ /\ Cardinality(requestsMade) < NumReqs
                          /\ \E k \in Keys:
                               \E v \in Vals:
                                 LET id == Cardinality(requestsMade) + 1 IN
                                   LET cmd == WriteCommand(id, k, v) IN
                                     LET req == RequestMsg(cmd) IN
                                       /\ msgs' = (msgs \cup {req})
                                       /\ observed' = Append(observed, req)
                       \/ /\ Cardinality(responsesGot) < Cardinality(requestsMade)
                          /\ \E m \in msgs:
                               /\ (m.type = "Response") /\ (m.res.id \notin responsesGot)
                               /\ observed' = Append(observed, m)
                          /\ msgs' = msgs
                    /\ pc' = [pc EXCEPT !["Client"] = "cloop"]
               ELSE /\ pc' = [pc EXCEPT !["Client"] = "Done"]
                    /\ UNCHANGED << msgs, observed >>
         /\ node' = node

Client == cloop

(* Allow infinite stuttering to prevent deadlock on termination. *)
Terminating == /\ \A self \in ProcSet: pc[self] = "Done"
               /\ UNCHANGED vars

Next == Client
           \/ (\E self \in Replicas: Replica(self))
           \/ Terminating

Spec == Init /\ [][Next]_vars

Termination == <>(\A self \in ProcSet: pc[self] = "Done")

\* END TRANSLATION

====
