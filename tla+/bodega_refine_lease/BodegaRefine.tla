(*********************************************************************************)
(* Refinement composition. This module contains NO algorithm logic of its own -- *)
(* it INSTANCEs RosterLeases.tla and bodega.tla, composes their Init/Next into   *)
(* a single spec, and states the refinement theorem                              *)
(*     RefineSpec => BD!AbstractSpec                                             *)
(* under the mapping `grants <- BD!ActiveGrants`.                                *)
(*********************************************************************************)

---- MODULE BodegaRefine ----
EXTENDS FiniteSets, Sequences, Integers, TLC

(*******************************)
(* Model inputs & assumptions. *)
(*******************************)
CONSTANT Replicas,   \* symmetric set of server nodes
         Writes,     \* symmetric set of write commands (each w/ unique value)
         Reads,      \* symmetric set of read commands
         MaxBallot,  \* maximum ballot pickable for leader preemption
         NodeFailuresOn,  \* if true, turn on node failures injection
         TGuard,          \* lease guard phase window length (ticks)
         TLease,          \* lease renewal extend window length (ticks)
         MaxTime          \* upper bound on abstract time for model checking

\* Bodega variables:
VARIABLES msgs, grants, node, pending, observed, crashed, pc
bdVars == <<msgs, grants, node, pending, observed, crashed, pc>>
bdVarsExceptGrants == <<msgs, node, pending, observed, crashed, pc>>

\* Leasing protocol variables:
VARIABLES leaseState, leaseMsgs, time
rlVars == <<leaseState, leaseMsgs, time>>

allVars == <<bdVars, rlVars>>

----------

(*****************************************)
(* Instantiate both underlying modules.  *)
(*****************************************)
\* Abstract Bodega, using our local `grants` variable, which will be the
\* one we prove equal to RL!ActiveGrants.
BD == INSTANCE Bodega

\* Concrete roster leases protocol. Constants are matched by name.
RL == INSTANCE RosterLeases

----------

(***********************************************)
(* Spec Composing consensus and leasing layer. *)
(***********************************************)
Init == /\ BD!Init
        /\ RL!Init

\* The lease protocol's LearnsNewRoster is unconstrained in isolation; this
\* condition explicitly binds it to the consensus layer `node[self].rosMaxKnown`
\* so the lease layer only serves rosters actually seen by a replica.
BindsRosterDiscovery(self) ==
    /\ ~crashed[self]
    /\ RL!Replica(self)
    /\ leaseState'[self].leaseBal = node[self].balMaxKnown
    /\ leaseState'[self].leaseRoster = node[self].rosMaxKnown

LeasingStep == /\ \E self \in Replicas: BindsRosterDiscovery(self)
               /\ grants' = RL!ActiveGrants'
               /\ UNCHANGED bdVarsExceptGrants

ConsensusStep == /\ \E self \in Replicas: BD!Replica(self)
                 /\ UNCHANGED rlVars

\* Time horizon stutter: once every replica's clock has reached MaxTime, no
\* further TimeTick is possible, so lease-layer GC and subsequent sessions
\* would not progress; we should consider this a termination condition and
\* let user control the time horizon length to check with.
TimeExhausted == \A r \in Replicas: time[r] = MaxTime

\* Combined termination condition.
Terminating == \/ /\ BD!Terminating
                  /\ UNCHANGED rlVars
               \/ /\ TimeExhausted
                  /\ UNCHANGED allVars

Next == \/ ConsensusStep
        \/ LeasingStep
        \/ Terminating

Spec == Init /\ [][Next]_allVars

====
