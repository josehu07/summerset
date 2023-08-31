(**********************************************************************************)
(* Multi-decree Paxos with aggregated prepare phase. This spec lets each replica  *)
(* act as a proposer, an acceptor, and a learner, and also adds several practical *)
(* details to make it closer to real code.                                        *)
(*                                                                                *)
(* See '../multipaxos_canonical/*' for other comments.                            *)
(**********************************************************************************)

---- MODULE MultiPaxos ----
EXTENDS FiniteSets, Integers, TLC

CONSTANT Replicas, Values, Slots, Ballots

MajorityNum == (Cardinality(Replicas) \div 2) + 1

ReplicasAssumption == /\ IsFiniteSet(Replicas)
                      /\ Cardinality(Replicas) >= 3

ValuesAssumption == /\ IsFiniteSet(Values)
                    /\ Cardinality(Values) >= 2
                    /\ 0 \notin Values

SlotsAssumption == /\ IsFiniteSet(Slots)
                   /\ Slots # {}

BallotsAssumption == /\ IsFiniteSet(Ballots)
                     /\ Ballots # {}
                     /\ Ballots \subseteq Nat

ASSUME /\ ReplicasAssumption
       /\ ValuesAssumption
       /\ SlotsAssumption
       /\ BallotsAssumption

(*--algorithm MultiPaxos
variable msgs = {},
         lBallot = [r \in Replicas |-> -1],
         lStatus = [r \in Replicas |->
                        [s \in Slots |-> ""]],
         rBallot = [r \in Replicas |-> -1],
         rVoted  = [r \in Replicas |->
                        [s \in Slots |->
                            [bal |-> -1, val |-> 0]]],
         proposed = [s \in Slots |-> {}],
         learned = [s \in Slots |-> {}];

define
    \* Is v a safely prepared value given the prepare reply pattern and ballot?
    ValuePreparedIn(v, prPat, pBal) ==
        /\ Cardinality(prPat) >= MajorityNum
        /\ \/ \A pr \in prPat: pr.vBal = -1
           \/ \E c \in 0..(pBal-1):
                /\ \A pr \in prPat: pr.vBal =< c
                /\ \E pr \in prPat: pr.vBal = c /\ pr.vVal = v

    \* Does the given accept reply pattern decide a value to be chosen?
    PatternDecidesChosen(arPat) ==
        Cardinality(arPat) >= MajorityNum
end define;

\* Send message helpers.
macro Send(m) begin
    msgs := msgs \cup {m};
end macro;

\* Leader sends Prepare message to replicas.
\* This is the first message a leader makes after being elected. Think of this
\* as a Prepare message that covers infinitely many slots up to infinity.
macro Prepare(r) begin
    with b \in Ballots do
        await /\ b > lBallot[r]
              /\ ~\E m \in msgs: (m.type = "Prepare") /\ (m.bal = b);
                 \* using this clause to model that ballot numbers from different
                 \* proposers should be unique
        Send([type |-> "Prepare",
              from |-> r,
              bal |-> b]);
        lBallot[r] := b;
        lStatus[r] := [s \in Slots |->
                            IF lStatus[r][s] = "Learned" THEN "Learned"
                                                         ELSE "Preparing"];
    end with;
end macro;

\* Replica replies to a Prepare message.
macro PrepareReply(r) begin
    with m \in msgs do
        await (m.type = "Prepare") /\ (m.bal > rBallot[r]);
        Send([type |-> "PrepareReply",
              from |-> r,
              bal |-> m.bal,
              voted |-> rVoted[r]]);
        rBallot[r] := m.bal;
    end with;
end macro;

\* Leader sends Accept message to replicas for a slot.
macro Accept(r, s) begin
    await lStatus[r][s] = "Preparing";
    with v \in Values do
        await \E MS \in SUBSET {m \in msgs: /\ m.type = "PrepareReply"
                                            /\ m.bal = lBallot[r]}:
                LET prPat == {[replica |-> m.from,
                               vBal |-> m.voted[s].bal,
                               vVal |-> m.voted[s].val]: m \in MS}
                IN  ValuePreparedIn(v, prPat, lBallot[r]);
        Send([type |-> "Accept",
              from |-> r,
              slot |-> s,
              bal |-> lBallot[r],
              val |-> v]);
        lStatus[r][s] := "Accepting";
        proposed[s] := proposed[s] \cup {v};
    end with;
end macro;

\* Replica replies to an Accept message.
macro AcceptReply(r) begin
    with m \in msgs do
        await (m.type = "Accept") /\ (m.bal >= rBallot[r]);
        Send([type |-> "AcceptReply",
              from |-> r,
              slot |-> m.slot,
              bal |-> m.bal,
              val |-> m.val]);
        rBallot[r] := m.bal;
        rVoted[r][m.slot] := [bal |-> m.bal, val |-> m.val];
    end with;
end macro;

\* Leader learns a chosen value at a slot.
macro Learn(r, s) begin
    await lStatus[r][s] = "Accepting";
    with v \in Values do
        await \E MS \in SUBSET {m \in msgs: /\ m.type = "AcceptReply"
                                            /\ m.slot = s
                                            /\ m.bal = lBallot[r]
                                            /\ m.val = v}:
                LET arPat == {[replica |-> m.from]: m \in MS}
                IN  PatternDecidesChosen(arPat);
        lStatus[r][s] := "Learned";
        learned[s] := learned[s] \cup {v};
    end with;
end macro;

process Replica \in Replicas
begin
    r: while TRUE do
        either
            \* p: Prepare(self);
            Prepare(self);
        or
            \* pr: PrepareReply(self);
            PrepareReply(self);
        or
            \* a: with s \in Slots do
            with s \in Slots do
                Accept(self, s);
            end with;
        or
            \* ar: AcceptReply(self);
            AcceptReply(self);
        or
            \* l: with s \in Slots do
            with s \in Slots do
                Learn(self, s);
            end with;
        end either;
    end while;
end process;
end algorithm; *)

\* BEGIN TRANSLATION (chksum(pcal) = "88c9342c" /\ chksum(tla) = "c40b8299")
VARIABLES msgs, lBallot, lStatus, rBallot, rVoted, proposed, learned

(* define statement *)
ValuePreparedIn(v, prPat, pBal) ==
    /\ Cardinality(prPat) >= MajorityNum
    /\ \/ \A pr \in prPat: pr.vBal = -1
       \/ \E c \in 0..(pBal-1):
            /\ \A pr \in prPat: pr.vBal =< c
            /\ \E pr \in prPat: pr.vBal = c /\ pr.vVal = v


PatternDecidesChosen(arPat) ==
    Cardinality(arPat) >= MajorityNum


vars == << msgs, lBallot, lStatus, rBallot, rVoted, proposed, learned >>

ProcSet == (Replicas)

Init == (* Global variables *)
        /\ msgs = {}
        /\ lBallot = [r \in Replicas |-> -1]
        /\ lStatus = [r \in Replicas |->
                          [s \in Slots |-> ""]]
        /\ rBallot = [r \in Replicas |-> -1]
        /\ rVoted = [r \in Replicas |->
                         [s \in Slots |->
                             [bal |-> -1, val |-> 0]]]
        /\ proposed = [s \in Slots |-> {}]
        /\ learned = [s \in Slots |-> {}]

Replica(self) == \/ /\ \E b \in Ballots:
                         /\ /\ b > lBallot[self]
                            /\ ~\E m \in msgs: (m.type = "Prepare") /\ (m.bal = b)
                         /\ msgs' = (msgs \cup {([type |-> "Prepare",
                                                  from |-> self,
                                                  bal |-> b])})
                         /\ lBallot' = [lBallot EXCEPT ![self] = b]
                         /\ lStatus' = [lStatus EXCEPT ![self] = [s \in Slots |->
                                                                       IF lStatus[self][s] = "Learned" THEN "Learned"
                                                                                                       ELSE "Preparing"]]
                    /\ UNCHANGED <<rBallot, rVoted, proposed, learned>>
                 \/ /\ \E m \in msgs:
                         /\ (m.type = "Prepare") /\ (m.bal > rBallot[self])
                         /\ msgs' = (msgs \cup {([type |-> "PrepareReply",
                                                  from |-> self,
                                                  bal |-> m.bal,
                                                  voted |-> rVoted[self]])})
                         /\ rBallot' = [rBallot EXCEPT ![self] = m.bal]
                    /\ UNCHANGED <<lBallot, lStatus, rVoted, proposed, learned>>
                 \/ /\ \E s \in Slots:
                         /\ lStatus[self][s] = "Preparing"
                         /\ \E v \in Values:
                              /\ \E MS \in SUBSET {m \in msgs: /\ m.type = "PrepareReply"
                                                               /\ m.bal = lBallot[self]}:
                                   LET prPat == {[replica |-> m.from,
                                                  vBal |-> m.voted[s].bal,
                                                  vVal |-> m.voted[s].val]: m \in MS}
                                   IN  ValuePreparedIn(v, prPat, lBallot[self])
                              /\ msgs' = (msgs \cup {([type |-> "Accept",
                                                       from |-> self,
                                                       slot |-> s,
                                                       bal |-> lBallot[self],
                                                       val |-> v])})
                              /\ lStatus' = [lStatus EXCEPT ![self][s] = "Accepting"]
                              /\ proposed' = [proposed EXCEPT ![s] = proposed[s] \cup {v}]
                    /\ UNCHANGED <<lBallot, rBallot, rVoted, learned>>
                 \/ /\ \E m \in msgs:
                         /\ (m.type = "Accept") /\ (m.bal >= rBallot[self])
                         /\ msgs' = (msgs \cup {([type |-> "AcceptReply",
                                                  from |-> self,
                                                  slot |-> m.slot,
                                                  bal |-> m.bal,
                                                  val |-> m.val])})
                         /\ rBallot' = [rBallot EXCEPT ![self] = m.bal]
                         /\ rVoted' = [rVoted EXCEPT ![self][m.slot] = [bal |-> m.bal, val |-> m.val]]
                    /\ UNCHANGED <<lBallot, lStatus, proposed, learned>>
                 \/ /\ \E s \in Slots:
                         /\ lStatus[self][s] = "Accepting"
                         /\ \E v \in Values:
                              /\ \E MS \in SUBSET {m \in msgs: /\ m.type = "AcceptReply"
                                                               /\ m.slot = s
                                                               /\ m.bal = lBallot[self]
                                                               /\ m.val = v}:
                                   LET arPat == {[replica |-> m.from]: m \in MS}
                                   IN  PatternDecidesChosen(arPat)
                              /\ lStatus' = [lStatus EXCEPT ![self][s] = "Learned"]
                              /\ learned' = [learned EXCEPT ![s] = learned[s] \cup {v}]
                    /\ UNCHANGED <<msgs, lBallot, rBallot, rVoted, proposed>>

Next == (\E self \in Replicas: Replica(self))

Spec == Init /\ [][Next]_vars

\* END TRANSLATION

====
