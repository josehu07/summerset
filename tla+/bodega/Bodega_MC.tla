---- MODULE Bodega_MC ----
EXTENDS Bodega

(****************************)
(* TLC config-related defs. *)
(****************************)
ConditionalPerm(set) == IF Cardinality(set) > 1
                          THEN Permutations(set)
                          ELSE {}

SymmetricPerms ==      ConditionalPerm(Replicas)
                  \cup ConditionalPerm(Writes)
                  \cup ConditionalPerm(Reads)

ConstMaxBallot == 2

ConstTGuard == 1
ConstTLease == 1
ConstMaxTime == 3

----------

(*************************)
(* Type check invariant. *)
(*************************)
TypeOK == /\ \A m \in msgs: m \in Messages
          /\ \A r \in Replicas: node[r] \in NodeStates
          /\ \A r \in Replicas: time[r] \in Times
          /\ Len(pending) =< NumCommands
          /\ Cardinality(Range(pending)) = Len(pending)
          /\ \A c \in Range(pending): c \in Commands
          /\ Len(observed) =< 2 * NumCommands
          /\ Cardinality(Range(observed)) = Len(observed)
          /\ Cardinality(reqsMade) >= Cardinality(acksRecv)
          /\ \A e \in Range(observed): e \in ClientEvents
          /\ \A r \in Replicas: crashed[r] \in BOOLEAN

THEOREM Spec => []TypeOK

----------

(*************************************)
(* Lease expiration safety property. *)
(*************************************)
LeaseExpirationSafety ==
    \A f, p \in Replicas:
        (/\ node[p].asGrantee[f].status = "Renewed"
         /\ node[p].asGrantee[f].leaseExpire > time[p])
            => (/\ node[f].asGrantor[p].status \in {"Renewing", "Revoking"}
                /\ node[f].asGrantor[p].leaseExpire
                   >= node[p].asGrantee[f].leaseExpire)

THEOREM Spec => []LeaseExpirationSafety

----------

(******************************************)
(* Lease uniqueness guarantee assertions. *)
(******************************************)
AtMostGrantsOneRoster ==
    \A f \in Replicas, b \in Ballots:
        Cardinality({ros \in Rosters:
                     \E p \in Replicas: /\ FGrantsPWithRos(f, p, ros)
                                        /\ ros.bal = b}) =< 1

AtMostOneStableRoster ==
    \A ros1, ros2 \in Rosters:
        (/\ Cardinality({f \in Replicas:
                         \E p \in Replicas: FGrantsPWithRos(f, p, ros1)})
                >= MajorityNum
         /\ Cardinality({f \in Replicas:
                         \E p \in Replicas: FGrantsPWithRos(f, p, ros2)})
                >= MajorityNum)
        => (ros1 = ros2)

THEOREM Spec => /\ []AtMostGrantsOneRoster
                /\ []AtMostOneStableRoster

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
    IN  IF k = 0 THEN "nil" ELSE order[k]

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
