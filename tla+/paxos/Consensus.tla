(***************************************************************************)
(* The consensus problem requires a set of processes to choose a single    *)
(* value.  This module specifies the problem by specifying exactly what    *)
(* the requirements are for choosing a value.                              *)
(*                                                                         *)
(* Adapted from:                                                           *)
(*   https://lamport.azurewebsites.net/tla/Consensus.tla                   *)
(***************************************************************************)

----------------------------- MODULE Consensus ------------------------------ 
EXTENDS Naturals, FiniteSets

(***************************************************************************)
(* We let the constant parameter Values be the set of all values that can  *)
(* be chosen.                                                              *)
(***************************************************************************)
CONSTANT Values

(****************************************************************************
We now specify the safety property of consensus as a trivial algorithm
that describes the allowed behaviors of a consensus algorithm.  It uses
the variable `chosen' to represent the set of all chosen values.  The
algorithm is trivial; it allows only behaviors that contain a single
state-change in which the variable `chosen' is changed from its initial
value {} to the value {v} for an arbitrary value v in Value.  The
algorithm itself does not specify any fairness properties, so it also
allows a behavior in which `chosen' is not changed.  We could use a
translator option to have the translation include a fairness
requirement, but we don't bother because it is easy enough to add it by
hand to the safety specification that the translator produces.

A real specification of consensus would also include additional
variables and actions.  In particular, it would have Propose actions in
which clients propose values and Learn actions in which clients learn
what value has been chosen.  It would allow only a proposed value to be
chosen.  However, the interesting part of a consensus algorithm is the
choosing of a single value.  We therefore restrict our attention to
that aspect of consensus algorithms.  In practice, given the algorithm
for choosing a value, it is obvious how to implement the Propose and
Learn actions.

For convenience, we define the macro Choose() that describes the action
of changing the value of `chosen' from {} to {v}, for a
nondeterministically chosen v in the set Value.  (There is little
reason to encapsulate such a simple action in a macro; however our
other specs are easier to read when written with such macros, so we
start using them now.) The `when' statement can be executed only when
its condition, chosen = {}, is true.  Hence, at most one Choose()
action can be performed in any execution.  The `with' statement
executes its body for a nondeterministically chosen v in Value.
Execution of this statement is enabled only if Value is
non-empty--something we do not assume at this point because it is not
required for the safety part of consensus, which is satisfied if no
value is chosen.

We put the Choose() action inside a `while' statement that loops
forever.  Of course, only a single Choose() action can be executed.
The algorithm stops after executing a Choose() action.  Technically,
the algorithm deadlocks after executing a Choose() action because
control is at a statement whose execution is never enabled.  Formally,
termination is simply deadlock that we want to happen.  We could just
as well have omitted the `while' and let the algorithm terminate.
However, adding the `while' loop makes the TLA+ representation of the
algorithm a tiny bit simpler.

The PlusCal translator writes the TLA+ translation of this algorithm
below.  The formula Spec is the TLA+ specification described by the
algorithm's code.  For now, you should just understand its two
subformulas Init and Next.  Formula Init is the initial predicate and
describes all possible initial states of an execution.  Formula Next is
the next-state relation; it describes the possible state changes
(changes of the values of variables), where unprimed variables
represent their values in the old state and primed variables represent
their values in the new state.
*****************************************************************************)
(*--algorithm Consensus
variable chosen = {};

macro Choose() begin
    await chosen = {};
    with v \in Values do
        chosen := {v};
    end with;
end macro;

begin
    lbl: while TRUE do
        Choose();
    end while;
end algorithm; *)

\* BEGIN TRANSLATION (chksum(pcal) = "27e8df09" /\ chksum(tla) = "4f621891")
VARIABLE chosen

vars == << chosen >>

Init == (* Global variables *)
        /\ chosen = {}

Next == /\ chosen = {}
        /\ \E v \in Values:
             chosen' = {v}

Spec == Init /\ [][Next]_vars

\* END TRANSLATION 

----------

(***************************************************************************)
(* We now prove the safety property that at most one value is chosen.  We  *)
(* first define the type-correctness invariant TypeOK, and then define Inv *)
(* to be the inductive invariant that asserts TypeOK and that the          *)
(* cardinality of the set `chosen' is at most 1.  We then prove that, in   *)
(* any behavior satisfying the safety specification Spec, the invariant    *)
(* Inv is true in all states.  This means that at most one value is chosen *)
(* in any behavior.                                                        *)
(***************************************************************************)
TypeOK == /\ chosen \subseteq Values
          /\ IsFiniteSet(chosen) 

Inv == /\ TypeOK
       /\ Cardinality(chosen) \leq 1
\* provable: Spec => []Inv

----------

(***************************************************************************)
(* We now define LiveSpec to be the algorithm's specification with the     *)
(* added fairness condition of weak fairness of the next-state relation,   *)
(* which asserts that execution does not stop if some action is enabled.   *)
(* The temporal formula Success asserts that some value is eventually      *)
(* chosen.  Below, we prove that LiveSpec implies Success This means that, *)
(* in every behavior satisfying LiveSpec, some value is chosen.            *)
(***************************************************************************)
LiveSpec == Spec /\ WF_vars(Next)

Success == <>(chosen # {})
\* provable: LiveSpec => Success

====
