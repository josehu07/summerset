---- MODULE RosterLeases_MC ----
EXTENDS RosterLeases

(****************************)
(* TLC config-related defs. *)
(****************************)
ConditionalPerm(set) == IF Cardinality(set) > 1
                          THEN Permutations(set)
                          ELSE {}

SymmetricPerms == ConditionalPerm(Replicas)

ConstMaxBallot == 2

ConstTGuard == 1

ConstTLease == 1

ConstMaxTime == 3

----------

(*************************)
(* Type check invariant. *)
(*************************)
TypeOK == /\ \A lm \in leaseMsgs: lm \in LeaseMsgs
          /\ \A r \in Replicas: leaseState[r] \in LeaseStates
          /\ \A r \in Replicas: time[r] \in Times

THEOREM Spec => []TypeOK

----------

(*************************************)
(* Lease expiration safety property. *)
(*************************************)
\* Grantor-side exp is never earlier than grantee-side exp for the same pair,
\* whenever the grantee currently believes it holds a live lease from f.
LeaseExpirationSafety ==
    \A f, p \in Replicas:
        (/\ leaseState[p].asGrantee[f].status = "Renewed"
         /\ leaseState[p].asGrantee[f].leaseExpire > time[p])
            => (/\ leaseState[f].asGrantor[p].status \in {"Renewing", "Revoking"}
                /\ leaseState[f].asGrantor[p].leaseExpire
                   >= leaseState[p].asGrantee[f].leaseExpire)

THEOREM Spec => []LeaseExpirationSafety

====
