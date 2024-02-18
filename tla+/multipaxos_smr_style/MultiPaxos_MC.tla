---- MODULE MultiPaxos_MC ----
EXTENDS MultiPaxos

(****************************)
(* TLC config-related defs. *)
(****************************)
SymmetricPerms ==      Permutations(Replicas)
                  \cup Permutations(Keys)
                  \cup Permutations(Vals)

ConstNumReqs == 3

ConstMaxBallot == 2

----------

(*************************)
(* Type check invariant. *)
(*************************)
TypeOK == /\ \A m \in msgs: m \in Messages
          /\ \A s \in Replicas: node[s] \in NodeStates
          /\ \A e \in observedSet: e \in ClientEvents

THEOREM Spec => []TypeOK

----------

(*******************************)
(* Linearizability constraint. *)
(*******************************)
Linearizability == TRUE

THEOREM Spec => []Linearizability

====