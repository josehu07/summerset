(**********************************************************************)
(* Single-decree Paxos. Leader election is omitted and the leader is  *)
(* assumed fixed.                                                     *)
(*                                                                    *)
(* Adapted from:                                                      *)
(*   https://github.com/tlaplus/DrTLAPlus/blob/master/Paxos/Paxos.tla *)
(**********************************************************************)

---- MODULE Paxos ----
EXTENDS Integers, FiniteSets, TLC

CONSTANT Acceptors, Quorums, Values, NullValue, Ballots

AcceptorsAssumption == /\ IsFiniteSet(Acceptors)
                       /\ Cardinality(Acceptors) >= 3

QuorumsAssumption == /\ Quorums \subseteq SUBSET Acceptors
                     /\ \A Q1, Q2 \in Quorums: Q1 \cap Q2 # {}

ValuesAssumption == /\ IsFiniteSet(Values)
                    /\ Cardinality(Values) >= 2
                    /\ NullValue \notin Values

BallotsAssumption == /\ IsFiniteSet(Ballots)
                     /\ Ballots \subseteq Nat

ASSUME /\ AcceptorsAssumption
       /\ QuorumsAssumption
       /\ ValuesAssumption
       /\ BallotsAssumption

VARIABLES msgs,        \* The set of messages that have been sent.
          maxPrepared, \* maxPrepared[a] is the highest Prepare request ballot
                       \* number acceptor a has seen.
          maxAccepted, \* maxAccepted[a] is the highest Accept ballot number
                       \* acceptor a has voted for.
          valAccepted  \* valAccepted[a] is the value of ballot maxAccepted[a],
                       \* initially None.

vars == <<msgs, maxPrepared, maxAccepted, valAccepted>>

Send(m) == msgs' = msgs \cup {m}

(******************)
(* Initial state. *)
(******************)
Init == /\ msgs = {}
        /\ maxPrepared = [a \in Acceptors |-> -1]
        /\ maxAccepted = [a \in Acceptors |-> -1]
        /\ valAccepted = [a \in Acceptors |-> NullValue]

(************************************************************************)
(* Phase 1a: A leader selects a ballot number b and sends a 1a message  *)
(* with ballot b to a majority of acceptors.  It can do this only if it *)
(* has not already sent a 1a message for ballot b.                      *)
(************************************************************************)
Phase1a(b) ==
    /\ ~\E m \in msgs: (m.type = "1a") /\ (m.bal = b)
    /\ Send([type |-> "1a", bal |-> b])
    /\ UNCHANGED <<maxPrepared, maxAccepted, valAccepted>>

(***************************************************************************)
(* Phase 1b: If an acceptor receives a 1a message with ballot b greater    *)
(* than that of any 1a message to which it has already responded, then it  *)
(* responds to the request with a promise not to accept any more proposals *)
(* for ballots numbered less than b and with the highest-numbered ballot   *)
(* (if any) for which it has voted for a value and the value it voted for  *)
(* in that ballot.  That promise is made in a 1b message.                  *)
(***************************************************************************)
Phase1b(a) ==
    \E m \in msgs:
        /\ m.type = "1a"
        /\ m.bal > maxPrepared[a]
        /\ Send([type |-> "1b",
                 bal |-> m.bal,
                 maxAccepted |-> maxAccepted[a],
                 valAccepted |-> valAccepted[a],
                 acceptor |-> a])
        /\ maxPrepared' = [maxPrepared EXCEPT ![a] = m.bal]
        /\ UNCHANGED <<maxAccepted, valAccepted>>

(***************************************************************************)
(* Phase 2a: If the leader receives a response to its 1b message (for      *)
(* ballot b) from a quorum of acceptors, then it sends a 2a message to all *)
(* acceptors for a proposal in ballot b with a value v, where v is the     *)
(* value of the highest-numbered proposal among the responses, or is any   *)
(* value if the responses reported no proposals.  The leader can send only *)
(* one 2a message for any ballot.                                          *)
(***************************************************************************)
Phase2a(b) ==
    /\ ~\E m \in msgs: (m.type = "2a") /\ (m.bal = b)
    /\ \E v \in Values:
        /\ \E Q \in Quorums:
            \E S \in SUBSET {m \in msgs: (m.type = "1b") /\ (m.bal = b)}:
                /\ \A a \in Q:
                    \E m \in S: m.acceptor = a
                /\ \/ \A m \in S: m.maxAccepted = -1
                   \/ \E c \in 0..(b-1):
                        /\ \A m \in S: m.maxAccepted =< c
                        /\ \E m \in S: /\ m.maxAccepted = c
                                       /\ m.valAccepted = v
        /\ Send([type |-> "2a", bal |-> b, val |-> v])
    /\ UNCHANGED <<maxPrepared, maxAccepted, valAccepted>>

(***************************************************************************)
(* Phase 2b: If an acceptor receives a 2a message for a ballot numbered    *)
(* b, it votes for the message's value in ballot b unless it has already   *)
(* responded to a 1a request for a ballot number greater than or equal to  *)
(* b.                                                                      *)
(***************************************************************************)
Phase2b(a) ==
    \E m \in msgs:
        /\ m.type = "2a"
        /\ m.bal >= maxPrepared[a]
        /\ Send([type |-> "2b",
                 bal |-> m.bal,
                 val |-> m.val,
                 acceptor |-> a])
        /\ maxPrepared' = [maxPrepared EXCEPT ![a] = m.bal]
        /\ maxAccepted' = [maxAccepted EXCEPT ![a] = m.bal]
        /\ valAccepted' = [valAccepted EXCEPT ![a] = m.val]

(*********************)
(* Possible actions. *)
(*********************)
Next == \/ \E b \in Ballots: Phase1a(b) \/ Phase2a(b)
        \/ \E a \in Acceptors: Phase1b(a) \/ Phase2b(a)

Spec == Init /\ [][Next]_vars

====
