(********************************************************************************)
(* Crossword protocol in its leader-based flavor. The leader acts as one of the *)
(* replicas, the distinguished proposer, and the only learner.                  *)
(********************************************************************************)

---- MODULE Crossword ----
EXTENDS FiniteSets, Integers, TLC

CONSTANT Replicas, Leader, Quorums, Values, NullValue, Slots, Ballots

(*--algorithm Crossword

begin
    skip;
end algorithm; *)

\* BEGIN TRANSLATION (chksum(pcal) = "12efd4d9" /\ chksum(tla) = "af3d9146")
VARIABLE pc

vars == << pc >>

Init == /\ pc = "Lbl_1"

Lbl_1 == /\ pc = "Lbl_1"
         /\ TRUE
         /\ pc' = "Done"

(* Allow infinite stuttering to prevent deadlock on termination. *)
Terminating == pc = "Done" /\ UNCHANGED vars

Next == Lbl_1
           \/ Terminating

Spec == Init /\ [][Next]_vars

Termination == <>(pc = "Done")

\* END TRANSLATION

====
