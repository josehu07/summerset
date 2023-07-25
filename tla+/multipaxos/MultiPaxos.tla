(*********************************************************************************)
(* Multi-decree Paxos with aggregated prepare phase. This spec explicitly models *)
(* proposers and learners.                                                       *)
(*                                                                               *)
(* Useful links:                                                                 *)
(*   https://arxiv.org/pdf/1606.01387.pdf                                        *)
(*   https://github.com/tlaplus/Examples/tree/master/specifications/MultiPaxos   *)
(*********************************************************************************)

---- MODULE MultiPaxos ----
EXTENDS FiniteSets, Integers, TLC

CONSTANT Proposers, Acceptors, Quorums, Learners, Values, Slots, Ballots

ProposersAssumption == IsFiniteSet(Proposers)

AcceptorsAssumption == /\ IsFiniteSet(Acceptors)
                       /\ Cardinality(Acceptors) >= 3

QuorumsAssumption == /\ Quorums \subseteq SUBSET Acceptors
                     /\ \A Q1, Q2 \in Quorums: Q1 \cap Q2 # {}

LearnersAssumption == IsFiniteSet(Learners)

ValuesAssumption == /\ IsFiniteSet(Values)
                    /\ Cardinality(Values) >= 2
                    /\ 0 \notin Values

SlotsAssumption == /\ IsFiniteSet(Slots)
                   /\ Slots # {}

BallotsAssumption == /\ IsFiniteSet(Ballots)
                     /\ Ballots # {}
                     /\ Ballots \subseteq Nat

ASSUME /\ ProposersAssumption
       /\ AcceptorsAssumption
       /\ QuorumsAssumption
       /\ LearnersAssumption
       /\ ValuesAssumption
       /\ SlotsAssumption
       /\ BallotsAssumption

(*--algorithm MultiPaxos
variable msgs = {},  \* set of all messages that have been sent
         pBallot = [p \in Proposers |-> -1],  \* current ballot number of proposer
         aBallot = [a \in Acceptors |-> -1],  \* highest ballot number seen by acceptor
         aVoted  = [a \in Acceptors |->       \* highest ballot accepted, along with the accepted value, by acceptor at slot
                        [s \in Slots |->
                            [bal |-> -1, val |-> 0]]],
         proposed = [s \in Slots |-> {}],  \* proposed values for each slot, for model checking purpose
         learned  = [s \in Slots |-> {}];  \* learned values for each slot, for model checking purpose

\* Send message helper.
macro Send(m) begin
    msgs := msgs \cup {m};
end macro;

\* Phase 1a:
macro Phase1a(p) begin
    with b \in Ballots do
        await b > pBallot[p] /\ ~\E m \in msgs: (m.type = "1a") /\ (m.bal = b);
        Send([type |-> "1a", from |-> p, bal |-> b]);
        pBallot[p] := b;
    end with;
end macro;

\* Phase 1b:
macro Phase1b(a) begin
    with m \in msgs do
        await (m.type = "1a") /\ (m.bal > aBallot[a]);
        Send([type |-> "1b",
              from |-> a,
              bal |-> m.bal,
              voted |-> aVoted[a]]);
        aBallot[a] := m.bal;
    end with;
end macro;

\* Phase 2a:
macro Phase2a(p, s) begin
    await ~\E m \in msgs: (m.type = "2a") /\ (m.slot = s) /\ (m.bal = pBallot[p]);
    with v \in Values do
        await \E Q \in Quorums:
                \E MS \in SUBSET {m \in msgs: (m.type = "1b") /\ (m.bal = pBallot[p])}:
                    /\ \A a \in Q:
                        \E m \in MS: m.from = a
                    /\ \/ \A m \in MS: m.voted[s].bal = -1
                       \/ \E c \in 0..(pBallot[p]-1):
                            /\ \A m \in MS: m.voted[s].bal =< c
                            /\ \E m \in MS: m.voted[s] = [bal |-> c, val |-> v];
        Send([type |-> "2a",
              from |-> p,
              slot |-> s,
              bal |-> pBallot[p],
              val |-> v]);
        proposed[s] := proposed[s] \cup {v};
    end with;
end macro;

\* Phase 2b:
macro Phase2b(a) begin
    with m \in msgs do
        await (m.type = "2a") /\ (m.bal >= aBallot[a]);
        Send([type |-> "2b",
              from |-> a,
              slot |-> m.slot,
              bal |-> m.bal,
              val |-> m.val]);
        aBallot[a] := m.bal;
        aVoted[a][m.slot] := [bal |-> m.bal, val |-> m.val];
    end with;
end macro;

\* Learn a chosen value:
macro Learn() begin
    with s \in Slots do
        await learned[s] = {};
        with v \in Values do
            await \E Q \in Quorums:
                    \A a \in Q:
                        \E m \in msgs: /\ m.type = "2b"
                                       /\ m.from = a
                                       /\ m.slot = s
                                       /\ m.val = v;
            learned[s] := learned[s] \cup {v};
        end with;
    end with;
end macro;

process Proposer \in Proposers
begin
    lbl_p: while TRUE do
        either
            Phase1a(self);
        or
            with s \in Slots do
                Phase2a(self, s);
            end with;
        end either;
    end while;
end process;

process Acceptor \in Acceptors
begin
    lbl_a: while TRUE do
        either
            Phase1b(self)
        or
            Phase2b(self);
        end either;
    end while;
end process;

process Learner \in Learners
begin
    lbl_l: while TRUE do
        Learn();
    end while;
end process;
end algorithm; *)

\* BEGIN TRANSLATION (chksum(pcal) = "d4513032" /\ chksum(tla) = "28008c19")
VARIABLES msgs, pBallot, aBallot, aVoted, proposed, learned

vars == << msgs, pBallot, aBallot, aVoted, proposed, learned >>

ProcSet == (Proposers) \cup (Acceptors) \cup (Learners)

Init == (* Global variables *)
        /\ msgs = {}
        /\ pBallot = [p \in Proposers |-> -1]
        /\ aBallot = [a \in Acceptors |-> -1]
        /\ aVoted = [a \in Acceptors |->
                         [s \in Slots |->
                             [bal |-> -1, val |-> 0]]]
        /\ proposed = [s \in Slots |-> {}]
        /\ learned = [s \in Slots |-> {}]

Proposer(self) == /\ \/ /\ \E b \in Ballots:
                             /\ b > pBallot[self] /\ ~\E m \in msgs: (m.type = "1a") /\ (m.bal = b)
                             /\ msgs' = (msgs \cup {([type |-> "1a", from |-> self, bal |-> b])})
                             /\ pBallot' = [pBallot EXCEPT ![self] = b]
                        /\ UNCHANGED proposed
                     \/ /\ \E s \in Slots:
                             /\ ~\E m \in msgs: (m.type = "2a") /\ (m.slot = s) /\ (m.bal = pBallot[self])
                             /\ \E v \in Values:
                                  /\ \E Q \in Quorums:
                                       \E MS \in SUBSET {m \in msgs: (m.type = "1b") /\ (m.bal = pBallot[self])}:
                                           /\ \A a \in Q:
                                               \E m \in MS: m.from = a
                                           /\ \/ \A m \in MS: m.voted[s].bal = -1
                                              \/ \E c \in 0..(pBallot[self]-1):
                                                   /\ \A m \in MS: m.voted[s].bal =< c
                                                   /\ \E m \in MS: m.voted[s] = [bal |-> c, val |-> v]
                                  /\ msgs' = (msgs \cup {([type |-> "2a",
                                                           from |-> self,
                                                           slot |-> s,
                                                           bal |-> pBallot[self],
                                                           val |-> v])})
                                  /\ proposed' = [proposed EXCEPT ![s] = proposed[s] \cup {v}]
                        /\ UNCHANGED pBallot
                  /\ UNCHANGED << aBallot, aVoted, learned >>

Acceptor(self) == /\ \/ /\ \E m \in msgs:
                             /\ (m.type = "1a") /\ (m.bal > aBallot[self])
                             /\ msgs' = (msgs \cup {([type |-> "1b",
                                                      from |-> self,
                                                      bal |-> m.bal,
                                                      voted |-> aVoted[self]])})
                             /\ aBallot' = [aBallot EXCEPT ![self] = m.bal]
                        /\ UNCHANGED aVoted
                     \/ /\ \E m \in msgs:
                             /\ (m.type = "2a") /\ (m.bal >= aBallot[self])
                             /\ msgs' = (msgs \cup {([type |-> "2b",
                                                      from |-> self,
                                                      slot |-> m.slot,
                                                      bal |-> m.bal,
                                                      val |-> m.val])})
                             /\ aBallot' = [aBallot EXCEPT ![self] = m.bal]
                             /\ aVoted' = [aVoted EXCEPT ![self][m.slot] = [bal |-> m.bal, val |-> m.val]]
                  /\ UNCHANGED << pBallot, proposed, learned >>

Learner(self) == /\ \E s \in Slots:
                      /\ learned[s] = {}
                      /\ \E v \in Values:
                           /\ \E Q \in Quorums:
                                \A a \in Q:
                                    \E m \in msgs: /\ m.type = "2b"
                                                   /\ m.from = a
                                                   /\ m.slot = s
                                                   /\ m.val = v
                           /\ learned' = [learned EXCEPT ![s] = learned[s] \cup {v}]
                 /\ UNCHANGED << msgs, pBallot, aBallot, aVoted, proposed >>

Next == (\E self \in Proposers: Proposer(self))
           \/ (\E self \in Acceptors: Acceptor(self))
           \/ (\E self \in Learners: Learner(self))

Spec == Init /\ [][Next]_vars

\* END TRANSLATION 

====
