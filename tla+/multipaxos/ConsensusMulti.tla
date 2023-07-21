(***************************************************************************)
(* The consensus problem specification, extended with an array of          *)
(* where each instance is a basic consensus problem.                       *)
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
       /\ IsFiniteSet(Slots)

(*--algorithm Consensus
variable chosen = [s \in Slots |-> {}];

macro Choose(s) begin
    await chosen[s] = {};
    with v \in Values do
        chosen[s] := {v};
    end with;
end macro;

begin
    lbl: while TRUE do
        with s \in Slots do
            Choose(s);
        end with;
    end while;
end algorithm; *)

\* BEGIN TRANSLATION (chksum(pcal) = "4c2f4817" /\ chksum(tla) = "5cb115d6")
VARIABLE chosen

vars == << chosen >>

Init == (* Global variables *)
        /\ chosen = [s \in Slots |-> {}]

Next == \E s \in Slots:
          /\ chosen[s] = {}
          /\ \E v \in Values:
               chosen' = [chosen EXCEPT ![s] = {v}]

Spec == Init /\ [][Next]_vars

\* END TRANSLATION 

----------

TypeOK == /\ chosen \in [Slots -> SUBSET Values]
          /\ \A s \in Slots: IsFiniteSet(chosen[s])

Inv == /\ TypeOK
       /\ \A s \in Slots: Cardinality(chosen[s]) \leq 1
\* provable: Spec => []Inv

----------

LiveSpec == Spec /\ WF_vars(Next)

Success == <>(\A s \in Slots: chosen[s] # {})
\* provable: LiveSpec => Success

====
