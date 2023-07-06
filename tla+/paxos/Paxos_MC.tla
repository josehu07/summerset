---- MODULE Paxos_MC ----
EXTENDS Paxos

PermAcceptors == Permutations(Acceptors)

BoundedBallots == 0..3

(***************************)
(* Definition of "chosen". *)
(***************************)
VotedForIn(a, v, b) == \E m \in msgs: /\ m.type = "2b"
                                      /\ m.acceptor = a
                                      /\ m.val = v
                                      /\ m.bal = b

ChosenIn(v, b) == \E Q \in Quorums: \A a \in Q: VotedForIn(a, v, b)

Chosen(v) == \E b \in Ballots : ChosenIn(v, b)

(************************************)
(* Consistency condition invariant. *)
(************************************)
ConsistencyInv == \A v1, v2 \in Values: Chosen(v1) /\ Chosen(v2) => (v1 = v2)

====
