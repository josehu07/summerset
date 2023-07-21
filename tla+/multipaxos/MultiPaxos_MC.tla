---- MODULE MultiPaxos_MC ----
EXTENDS MultiPaxos

SymmetricPerms ==      Permutations(Proposers)
                  \cup Permutations(Acceptors)
                  \cup Permutations(Values)
                  \cup Permutations(Slots)

ConstBallots == 0..2

----------

(***********************)
(* Helper definitions. *)
(***********************)
ProposedIn(v, s, b) == \E m \in msgs: /\ m.type = "2a"
                                      /\ m.slot = s
                                      /\ m.bal = b
                                      /\ m.val = v

Proposed(v, s) == \E b \in Ballots: ProposedIn(v, s, b)

VotedForIn(a, v, s, b) == \E m \in msgs: /\ m.type = "2b"
                                         /\ m.from = a
                                         /\ m.slot = s
                                         /\ m.bal = b
                                         /\ m.val = v

WontVoteIn(a, s, b) == /\ \A v \in Values: ~VotedForIn(a, v, s, b)
                       /\ aBallot[a] > b

ChosenIn(v, s, b) == \E Q \in Quorums: \A a \in Q: VotedForIn(a, v, s, b)

Chosen(v, s) == \E b \in Ballots: ChosenIn(v, s, b)

----------

(*************************)
(* Type check invariant. *)
(*************************)
SlotVotes == [Slots -> [bal: Ballots \cup {-1},
                        val: Values \cup {NullValue}]]

Messages ==      [type: {"1a"}, from: Proposers,
                                bal: Ballots]
            \cup [type: {"1b"}, from: Acceptors,
                                bal: Ballots,
                                voted: SlotVotes]
            \cup [type: {"2a"}, from: Proposers,
                                slot: Slots,
                                bal: Ballots,
                                val: Values]
            \cup [type: {"2b"}, from: Acceptors,
                                slot: Slots,
                                bal: Ballots,
                                val: Values]

TypeOK == /\ msgs \in SUBSET Messages
          /\ pBallot \in [Proposers -> Ballots \cup {-1}]
          /\ aBallot \in [Acceptors -> Ballots \cup {-1}]
          /\ aVoted \in [Acceptors -> SlotVotes]

----------

(*****************************************************************************)
(* Check that it implements the ConsensusMulti spec. This transitively means *)
(* that it satisfies the following three properties:                         *)
(*   - Nontriviality                                                         *)
(*   - Stability                                                             *)
(*   - Consistency                                                           *)
(*                                                                           *)
(* Only check this property on very small model constants inputs, otherwise  *)
(* it would take a prohibitively long time due to state bloating.            *)
(*****************************************************************************)
proposedValues == [s \in Slots |-> {v \in Values: Proposed(v, s)}]
proposedSet == UNION {proposedValues[s]: s \in Slots}

chosenValues == [s \in Slots |-> {v \in Values: Chosen(v, s)}]

ConsensusModule == INSTANCE ConsensusMulti WITH proposed <- proposedSet,
                                                chosen <- chosenValues
ConsensusSpec == ConsensusModule!Spec

----------

(********************************************************************************)
(* The non-triviality and consistency properties stated in invariant flavor.    *)
(*                                                                              *)
(* Checking invariants takes significantly less time than checking more complex *)
(* temporal properties. Hence, first check these as invariants on larger        *)
(* constants inputs, then check the ConsensusSpec property on small inputs.     *)
(********************************************************************************)
NontrivialityInv ==
    \A s \in Slots: \A v \in chosenValues[s]: v \in proposedSet

ConsistencyInv ==
    \A s \in Slots: Cardinality(chosenValues[s]) <= 1

====
