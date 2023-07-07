(**********************************************************************)
(* Single-decree Paxos generated from PlusCal. Leader election is     *)
(* omitted and the leader is assumed fixed.                           *)
(*                                                                    *)
(* Adapted from:                                                      *)
(*   https://github.com/tlaplus/DrTLAPlus/blob/master/Paxos/Paxos.tla *)
(**********************************************************************)

---- MODULE PaxosPlusCal ----
EXTENDS Integers, FiniteSets, TLC

CONSTANT Acceptors, Quorums, Values, NullValue, Ballots

AcceptorsAssumption == /\ IsFiniteSet(Acceptors)
                       /\ Cardinality(Acceptors) >= 3

QuorumsAssumption == /\ Quorums \subseteq SUBSET Acceptors
                     /\ \A Q1, Q2 \in Quorums: Q1 \cap Q2 # {}

ValuesAssumption == /\ IsFiniteSet(Values)
                    /\ Cardinality(Values) >= 2
                    /\ NullValue \notin Values

BallotsAssumption == /\ IsFiniteSet(Ballots)
                     /\ Ballots \subseteq Nat

ASSUME /\ AcceptorsAssumption
       /\ QuorumsAssumption
       /\ ValuesAssumption
       /\ BallotsAssumption

(*--algorithm PaxosPlusCal
variable msgs = {},
         maxPrepared = [a \in Acceptors |-> -1],
         maxAccepted = [a \in Acceptors |-> -1],
         valAccepted = [a \in Acceptors |-> NullValue];

\* Send message helper.
macro Send(m) begin
    msgs := msgs \cup {m};
end macro;

\* Phase 1a:
macro Phase1a() begin
    with b \in Ballots do
        await ~\E m \in msgs: (m.type = "1a") /\ (m.bal = b);
        Send([type |-> "1a", bal |-> b]);
    end with;
end macro;

\* Phase 1b:
macro Phase1b(a) begin
    with m \in msgs do
        await (m.type = "1a") /\ (m.bal > maxPrepared[a]);
        Send([type |-> "1b",
              bal |-> m.bal,
              maxAccepted |-> maxAccepted[a],
              valAccepted |-> valAccepted[a],
              acceptor |-> a]);
        maxPrepared[a] := m.bal;
    end with;
end macro;

\* Phase 2a:
macro Phase2a() begin
    with b \in Ballots do
        await ~\E m \in msgs: (m.type = "2a") /\ (m.bal = b);
        with v \in Values do
            await \E Q \in Quorums:
                    \E S \in SUBSET {m \in msgs: (m.type = "1b") /\ (m.bal = b)}:
                        /\ \A a \in Q:
                            \E m \in S: m.acceptor = a
                        /\ \/ \A m \in S: m.maxAccepted = -1
                           \/ \E c \in 0..(b-1):
                                /\ \A m \in S: m.maxAccepted =< c
                                /\ \E m \in S: /\ m.maxAccepted = c
                                               /\ m.valAccepted = v;
            Send([type |-> "2a", bal |-> b, val |-> v]);
        end with;
    end with;
end macro;

\* Phase 2b:
macro Phase2b(a) begin
    with m \in msgs do
        await (m.type = "2a") /\ (m.bal >= maxPrepared[a]);
        Send([type |-> "2b",
              bal |-> m.bal,
              val |-> m.val,
              acceptor |-> a]);
        maxPrepared[a] := m.bal;
        maxAccepted[a] := m.bal;
        valAccepted[a] := m.val;
    end with;
end macro;

\* Proposer logic for phase 1a and 2a triggers.
process Proposer = "Proposer"
begin
    lbl_p: while TRUE do
        either
            Phase1a();
        or
            Phase2a();
        end either;
    end while;
end process;

\* Acceptor logic for phase 1b and 2b triggers.
process Acceptor \in Acceptors
begin
    lbl_a: while TRUE do
        either
            Phase1b(self)
        or
            Phase2b(self)
        end either;
    end while;
end process;
end algorithm; *)

\* BEGIN TRANSLATION (chksum(pcal) = "e0e9ea61" /\ chksum(tla) = "427badc4")
VARIABLES msgs, maxPrepared, maxAccepted, valAccepted

vars == << msgs, maxPrepared, maxAccepted, valAccepted >>

ProcSet == {"Proposer"} \cup (Acceptors)

Init == (* Global variables *)
        /\ msgs = {}
        /\ maxPrepared = [a \in Acceptors |-> -1]
        /\ maxAccepted = [a \in Acceptors |-> -1]
        /\ valAccepted = [a \in Acceptors |-> NullValue]

Proposer == /\ \/ /\ \E b \in Ballots:
                       /\ ~\E m \in msgs: (m.type = "1a") /\ (m.bal = b)
                       /\ msgs' = (msgs \cup {([type |-> "1a", bal |-> b])})
               \/ /\ \E b \in Ballots:
                       /\ ~\E m \in msgs: (m.type = "2a") /\ (m.bal = b)
                       /\ \E v \in Values:
                            /\ \E Q \in Quorums:
                                 \E S \in SUBSET {m \in msgs: (m.type = "1b") /\ (m.bal = b)}:
                                     /\ \A a \in Q:
                                         \E m \in S: m.acceptor = a
                                     /\ \/ \A m \in S: m.maxAccepted = -1
                                        \/ \E c \in 0..(b-1):
                                             /\ \A m \in S: m.maxAccepted =< c
                                             /\ \E m \in S: /\ m.maxAccepted = c
                                                            /\ m.valAccepted = v
                            /\ msgs' = (msgs \cup {([type |-> "2a", bal |-> b, val |-> v])})
            /\ UNCHANGED << maxPrepared, maxAccepted, valAccepted >>

Acceptor(self) == \/ /\ \E m \in msgs:
                          /\ (m.type = "1a") /\ (m.bal > maxPrepared[self])
                          /\ msgs' = (msgs \cup {([type |-> "1b",
                                                   bal |-> m.bal,
                                                   maxAccepted |-> maxAccepted[self],
                                                   valAccepted |-> valAccepted[self],
                                                   acceptor |-> self])})
                          /\ maxPrepared' = [maxPrepared EXCEPT ![self] = m.bal]
                     /\ UNCHANGED <<maxAccepted, valAccepted>>
                  \/ /\ \E m \in msgs:
                          /\ (m.type = "2a") /\ (m.bal >= maxPrepared[self])
                          /\ msgs' = (msgs \cup {([type |-> "2b",
                                                   bal |-> m.bal,
                                                   val |-> m.val,
                                                   acceptor |-> self])})
                          /\ maxPrepared' = [maxPrepared EXCEPT ![self] = m.bal]
                          /\ maxAccepted' = [maxAccepted EXCEPT ![self] = m.bal]
                          /\ valAccepted' = [valAccepted EXCEPT ![self] = m.val]

Next == Proposer
           \/ (\E self \in Acceptors: Acceptor(self))

Spec == Init /\ [][Next]_vars

\* END TRANSLATION 

====
