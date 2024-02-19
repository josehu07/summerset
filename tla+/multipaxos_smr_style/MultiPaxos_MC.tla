---- MODULE MultiPaxos_MC ----
EXTENDS MultiPaxos

(****************************)
(* TLC config-related defs. *)
(****************************)
ConditionalPerm(set) == IF Cardinality(set) > 1 THEN Permutations(set)
                                                ELSE {}

SymmetricPerms ==      ConditionalPerm(Servers)
                  \cup ConditionalPerm(Writes)
                  \cup ConditionalPerm(Reads)

ConstMaxBallot == 2

----------

(*************************)
(* Type check invariant. *)
(*************************)
TypeOK == /\ \A m \in msgs: m \in Messages
          /\ \A s \in Servers: node[s] \in NodeStates
          /\ Cardinality(pending) =< NumCommands
          /\ \A e \in pending: e \in ClientEvents /\ e.type = "Req"
          /\ Len(observed) =< 2 * NumCommands
          /\ Cardinality(observedSet) = Len(observed)
          /\ \A e \in observedSet: e \in ClientEvents

THEOREM Spec => []TypeOK

----------

(*******************************)
(* Linearizability constraint. *)
(*******************************)
ReqPosOfCmd(c) == CHOOSE i \in 1..Len(observed):
                        /\ observed[i].type = "Req"
                        /\ observed[i].cmd = c

AckPosOfCmd(c) == CHOOSE i \in 1..Len(observed):
                        /\ observed[i].type = "Ack"
                        /\ observed[i].cmd = c

ResultOfCmd(c) == observed[AckPosOfCmd(c)].val

OrderIdxOfCmd(order, c) == CHOOSE j \in 1..Len(order): order[j] = c

LastWriteBefore(order, j) ==
    LET k == CHOOSE k \in 0..(j-1):
                    /\ (k = 0 \/ order[k] \in Writes)
                    /\ \A l \in (k+1)..(j-1): order[l] \in Reads
    IN  IF k = 0 THEN 0 ELSE order[k]

IsLinearOrder(order) ==
    /\ {order[j]: j \in 1..Len(order)} = Commands
    /\ \A j \in 1..Len(order):
            ResultOfCmd(order[j]) = LastWriteBefore(order, j)

ObeysRealTime(order) ==
    \A c1, c2 \in Commands:
        (AckPosOfCmd(c1) < ReqPosOfCmd(c2))
            => (OrderIdxOfCmd(order, c1) < OrderIdxOfCmd(order, c2))

Linearizability ==
    terminated => 
        \E order \in [1..NumCommands -> Commands]:
            /\ IsLinearOrder(order)
            /\ ObeysRealTime(order)

THEOREM Spec => Linearizability

====