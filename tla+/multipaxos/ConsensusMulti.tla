(***************************************************************************)
(* The consensus problem specification, extended with an array of          *)
(* istances where each instance is a basic consensus problem.              *)
(*                                                                         *)
(* Adapted from:                                                           *)
(*   https://lamport.azurewebsites.net/tla/Consensus.tla                   *)
(*   and ../paxos/Consensus.tla                                            *)
(***************************************************************************)

---- MODULE ConsensusMulti ----- 
EXTENDS Naturals, FiniteSets

CONSTANT Values, Slots

ASSUME /\ Values # {}
       /\ Slots # {}

(*************************)
(* Consensus model spec. *)
(*************************)
(*--algorithm ConsensusMulti
variable proposed = {},
         chosen = [s \in Slots |-> {}];

\* Propose new value:
macro Propose() begin
    with v \in Values do
        await v \notin proposed;
        proposed := proposed \cup {v};
    end with;
end macro;

\* Choose a proposed value for a given empty slot:
macro Choose(s) begin
    await chosen[s] = {};
    with v \in proposed do
        chosen[s] := {v};
    end with;
end macro;

begin
    lbl: while TRUE do
        either
            Propose();
        or
            with s \in Slots do
                Choose(s);
            end with;
        end either;
    end while;
end algorithm; *)

\* BEGIN TRANSLATION (chksum(pcal) = "dbf82723" /\ chksum(tla) = "39f644db")
VARIABLES proposed, chosen

vars == << proposed, chosen >>

Init == (* Global variables *)
        /\ proposed = {}
        /\ chosen = [s \in Slots |-> {}]

Next == \/ /\ \E v \in Values:
                /\ v \notin proposed
                /\ proposed' = (proposed \cup {v})
           /\ UNCHANGED chosen
        \/ /\ \E s \in Slots:
                /\ chosen[s] = {}
                /\ \E v \in proposed:
                     chosen' = [chosen EXCEPT ![s] = {v}]
           /\ UNCHANGED proposed

Spec == Init /\ [][Next]_vars

\* END TRANSLATION 

----------

(**********************)
(* Safety properties. *)
(**********************)
TypeOK == /\ chosen \in [Slots -> SUBSET Values]
          /\ \A s \in Slots: IsFiniteSet(chosen[s])

Nontriviality ==
    [](\A s \in Slots: \A v \in chosen[s]: v \in proposed)

Stability ==
    \A s \in Slots:
        \A v \in Values:
            (v \in chosen[s]) => [](v \in chosen[s])

Consistency ==
    [](\A s \in Slots: Cardinality(chosen[s]) <= 1)

THEOREM Spec => ([]TypeOK) /\ Nontriviality /\ Stability /\ Consistency

----------

(************************)
(* Liveness properties. *)
(************************)
LiveSpec == Spec /\ WF_vars(Next)

Liveness == <>(\A s \in Slots: chosen[s] # {})

THEOREM LiveSpec => Liveness

====
