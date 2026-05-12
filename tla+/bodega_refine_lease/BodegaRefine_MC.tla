---- MODULE BodegaRefine_MC ----
EXTENDS BodegaRefine

\* INSTANCE the `_MC` definitions of the two underlying modules so we can
\* reuse the invariants they already defined.
RLMC == INSTANCE RosterLeases_MC
BDMC == INSTANCE Bodega_MC

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
FullTypeOK == /\ \A m \in msgs: m \in BD!Messages
              /\ \A lm \in leaseMsgs: lm \in RL!LeaseMsgs
              /\ \A g \in grants: g \in BD!LeaseGrants
              /\ \A r \in Replicas: node[r] \in BD!NodeStates
              /\ \A r \in Replicas: leaseState[r] \in RL!LeaseStates
              /\ \A r \in Replicas: time[r] \in RL!Times
              /\ Len(pending) =< BD!NumCommands
              /\ Cardinality(BD!Range(pending)) = Len(pending)
              /\ \A c \in BD!Range(pending): c \in BD!Commands
              /\ Len(observed) =< 2 * BD!NumCommands
              /\ Cardinality(BD!Range(observed)) = Len(observed)
              /\ Cardinality(BD!reqsMade) >= Cardinality(BD!acksRecv)
              /\ \A e \in BD!Range(observed): e \in BD!ClientEvents
              /\ \A r \in Replicas: crashed[r] \in BOOLEAN

THEOREM Spec => []FullTypeOK

----------

(*******************************************)
(* Plain-name re-definition of properties. *)
(*******************************************)
LeaseExpirationSafety == RLMC!LeaseExpirationSafety

AtMostOneGrantPerNode == BDMC!AtMostOneGrantPerNode
AtMostOneStableRoster == BDMC!AtMostOneStableRoster

\* the refinement: every behavior of this composed spec is, when viewed through
\* `grants = RL!ActiveGrants` lens, a legal behavior of the higher Bodega spec
RefinesBodega == BD!AbstractSpec

----------

(************************)
(* Refinement theorem.  *)
(************************)
GrantsEqualsActive == grants = RL!ActiveGrants

THEOREM Spec => /\ BD!AbstractSpec
                /\ []GrantsEqualsActive

====
