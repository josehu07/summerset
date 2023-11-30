(*********************************************************************************)
(* Crossword protocol combining MultiPaxos and erasure code sharding, built upon *)
(* the practical version of MultiPaxos spec.                                     *)
(*                                                                               *)
(* Leader shards recovery is not explicitly modeled in this spec, but should be  *)
(* quite straightforward to add.                                                 *)
(*********************************************************************************)

---- MODULE Crossword ----
EXTENDS FiniteSets, Integers, TLC

CONSTANT
    Replicas, \* set of servers
    \* set of values
    Values,
    \* set of slots (instances); 1 should be enough
    Slots,
    \* set of ballot numbers
    Ballots,
    \* set of numbers representing shards (columns)
    Shards,
    \* number of data shards needed to reconstruct
    NumDataShards,
    \* fault-tolerance level
    MaxFaults

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

ShardsAssumption == /\ IsFiniteSet(Shards)
                    /\ Shards # {}

NumDataShardsAssumption ==
    /\ NumDataShards > 0
    /\ NumDataShards =< Cardinality(Shards)

MaxFaultsAssumption ==
    /\ MaxFaults >= 0
    /\ MaxFaults =< (Cardinality(Replicas) - MajorityNum)

ASSUME /\ ReplicasAssumption
       /\ ValuesAssumption
       /\ SlotsAssumption
       /\ BallotsAssumption
       /\ ShardsAssumption
       /\ NumDataShardsAssumption
       /\ MaxFaultsAssumption

(*--algorithm Crossword
variable msgs = {},
         lBallot = [r \in Replicas |-> -1],
         lStatus = [r \in Replicas |->
                        [s \in Slots |-> ""]],
         rBallot = [r \in Replicas |-> -1],
         rVoted  = [r \in Replicas |->
                        [s \in Slots |->
                            [bal |-> -1, val |-> 0,
                             shards |-> {}]]],
         proposed = [s \in Slots |-> {}],
         learned = [s \in Slots |-> {}];

define
    \* Is g a large enough subset of u under MaxFaults?
    BigEnoughUnderFaults(g, u) ==
        Cardinality(g) >= (Cardinality(u) - MaxFaults)

    \* Set of subsets of u that we consider under MaxFaults.
    SubsetsUnderFaults(u) ==
        {g \in SUBSET u: BigEnoughUnderFaults(g, u)}
    
    \* Is cs a coverage set (i.e., a set of sets of shards)
    \* from which we can reconstruct the original data?
    IsGoodCoverageSet(cs) ==
        Cardinality(UNION cs) >= NumDataShards

    \* Set of all valid shard assignments.
    ValidAssignments ==
        {assign \in [Replicas -> SUBSET Shards]:
            \A group \in SubsetsUnderFaults(Replicas):
                IsGoodCoverageSet({assign[r]: r \in group})}

    \* Is v a safely prepared value given the prepare reply
    \* pattern and ballot?
    ValuePreparedIn(v, prPat, pBal) ==
        \/ /\ Cardinality(prPat) >= MajorityNum
           /\ \A pr \in prPat: pr.vBal = -1
        \/ /\ Cardinality(prPat) >= MajorityNum
           /\ \E c \in 0..(pBal-1):
                /\ \A pr \in prPat: pr.vBal =< c
                /\ \E pr \in prPat: pr.vBal = c /\ pr.vVal = v
                /\ IsGoodCoverageSet(
                    {pr.vShards: pr \in
                        {prr \in prPat: prr.vVal = v}})
        \/ /\ BigEnoughUnderFaults(prPat, Replicas)
           /\ ~\E vv \in Values:
                IsGoodCoverageSet(
                    {pr.vShards: pr \in
                        {prr \in prPat: prr.vVal = vv}})

    \* Does the given accept reply pattern decide a value to
    \* be chosen?
    PatternDecidesChosen(arPat) ==
        /\ Cardinality(arPat) >= MajorityNum
        /\ \A group \in SubsetsUnderFaults(arPat):
                IsGoodCoverageSet({ar.aShards: ar \in group})
end define;

\* Send message helpers.
macro Send(m) begin
    msgs := msgs \cup {m};
end macro;

macro SendAll(ms) begin
    msgs := msgs \cup ms;
end macro;

\* Leader sends Prepare message to replicas.
\* This is the first message a leader makes after election.
\* Think of this as a Prepare message that covers infinitely
\* many slots up to infinity.
macro Prepare(r) begin
    with b \in Ballots do
        await /\ b > lBallot[r]
              /\ ~\E m \in msgs:
                    (m.type = "Prepare") /\ (m.bal = b);
                 \* using this clause to model that ballot
                 \* nums from different proposers be unique
        Send([type |-> "Prepare",
              from |-> r,
              bal |-> b]);
        lBallot[r] := b;
        lStatus[r] := [s \in Slots |->
                            IF lStatus[r][s] = "Learned"
                                THEN "Learned"
                                ELSE "Preparing"];
    end with;
end macro;

\* Replica replies to a Prepare message.
\* Replicas reply with known value shards for necessary
\* recovery reconstruction.
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
\* Shards are assigned according to some assignment policy,
\* from a set where all reasonable assignments considered.
macro Accept(r, s) begin
    await lStatus[r][s] = "Preparing";
    with v \in Values do
        await \E MS \in SUBSET {
                m \in msgs:
                    /\ m.type = "PrepareReply"
                    /\ m.bal = lBallot[r]}:
                LET prPat == {[replica |-> m.from,
                               vBal |-> m.voted[s].bal,
                               vVal |-> m.voted[s].val,
                               vShards |-> m.voted[s].shards]:
                              m \in MS}
                IN  ValuePreparedIn(v, prPat, lBallot[r]);
        with assign \in ValidAssignments do
            SendAll({[type |-> "Accept",
                      from |-> r,
                      to |-> rt,
                      slot |-> s,
                      bal |-> lBallot[r],
                      val |-> v,
                      shards |-> assign[rt]]:
                     rt \in Replicas});
        end with;
        lStatus[r][s] := "Accepting";
        proposed[s] := proposed[s] \cup {v};
    end with;
end macro;

\* Replica replies to an Accept message.
\* Such a reply will not carry actual value data in practice;
\* just the shards assignment metadata is enough for the
\* leader to gather Acceptance Patterns.
macro AcceptReply(r) begin
    with m \in msgs do
        await /\ (m.type = "Accept")
              /\ (m.to = r)
              /\ (m.bal >= rBallot[r]);
        Send([type |-> "AcceptReply",
              from |-> r,
              slot |-> m.slot,
              bal |-> m.bal,
              val |-> m.val,
              shards |-> m.shards]);
        rBallot[r] := m.bal;
        rVoted[r][m.slot] := [bal |-> m.bal, val |-> m.val,
                              shards |-> m.shards];
    end with;
end macro;

\* Leader learns a chosen value at a slot.
macro Learn(r, s) begin
    await lStatus[r][s] = "Accepting";
    with v \in Values do
        await \E MS \in SUBSET {
            m \in msgs:
                /\ m.type = "AcceptReply"
                /\ m.slot = s
                /\ m.bal = lBallot[r]
                /\ m.val = v}:
            LET arPat == {[replica |-> m.from,
                           aShards |-> m.shards]: m \in MS}
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

\* BEGIN TRANSLATION (chksum(pcal) = "50e87803" /\ chksum(tla) = "228525c6")
VARIABLES msgs, lBallot, lStatus, rBallot, rVoted, proposed, learned

(* define statement *)
BigEnoughUnderFaults(g, u) ==
    Cardinality(g) >= (Cardinality(u) - MaxFaults)


SubsetsUnderFaults(u) ==
    {g \in SUBSET u: BigEnoughUnderFaults(g, u)}



IsGoodCoverageSet(cs) ==
    Cardinality(UNION cs) >= NumDataShards


ValidAssignments ==
    {assign \in [Replicas -> SUBSET Shards]:
        \A group \in SubsetsUnderFaults(Replicas):
            IsGoodCoverageSet({assign[r]: r \in group})}



ValuePreparedIn(v, prPat, pBal) ==
    \/ /\ Cardinality(prPat) >= MajorityNum
       /\ \A pr \in prPat: pr.vBal = -1
    \/ /\ Cardinality(prPat) >= MajorityNum
       /\ \E c \in 0..(pBal-1):
            /\ \A pr \in prPat: pr.vBal =< c
            /\ \E pr \in prPat: pr.vBal = c /\ pr.vVal = v
            /\ IsGoodCoverageSet(
                {pr.vShards: pr \in
                    {prr \in prPat: prr.vVal = v}})
    \/ /\ BigEnoughUnderFaults(prPat, Replicas)
       /\ ~\E vv \in Values:
            IsGoodCoverageSet(
                {pr.vShards: pr \in
                    {prr \in prPat: prr.vVal = vv}})



PatternDecidesChosen(arPat) ==
    /\ Cardinality(arPat) >= MajorityNum
    /\ \A group \in SubsetsUnderFaults(arPat):
            IsGoodCoverageSet({ar.aShards: ar \in group})


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
                             [bal |-> -1, val |-> 0,
                              shards |-> {}]]]
        /\ proposed = [s \in Slots |-> {}]
        /\ learned = [s \in Slots |-> {}]

Replica(self) == \/ /\ \E b \in Ballots:
                         /\ /\ b > lBallot[self]
                            /\ ~\E m \in msgs:
                                  (m.type = "Prepare") /\ (m.bal = b)
                         /\ msgs' = (msgs \cup {([type |-> "Prepare",
                                                  from |-> self,
                                                  bal |-> b])})
                         /\ lBallot' = [lBallot EXCEPT ![self] = b]
                         /\ lStatus' = [lStatus EXCEPT ![self] = [s \in Slots |->
                                                                       IF lStatus[self][s] = "Learned"
                                                                           THEN "Learned"
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
                              /\ \E MS \in SUBSET {
                                   m \in msgs:
                                       /\ m.type = "PrepareReply"
                                       /\ m.bal = lBallot[self]}:
                                   LET prPat == {[replica |-> m.from,
                                                  vBal |-> m.voted[s].bal,
                                                  vVal |-> m.voted[s].val,
                                                  vShards |-> m.voted[s].shards]:
                                                 m \in MS}
                                   IN  ValuePreparedIn(v, prPat, lBallot[self])
                              /\ \E assign \in ValidAssignments:
                                   msgs' = (msgs \cup ({[type |-> "Accept",
                                                         from |-> self,
                                                         to |-> rt,
                                                         slot |-> s,
                                                         bal |-> lBallot[self],
                                                         val |-> v,
                                                         shards |-> assign[rt]]:
                                                        rt \in Replicas}))
                              /\ lStatus' = [lStatus EXCEPT ![self][s] = "Accepting"]
                              /\ proposed' = [proposed EXCEPT ![s] = proposed[s] \cup {v}]
                    /\ UNCHANGED <<lBallot, rBallot, rVoted, learned>>
                 \/ /\ \E m \in msgs:
                         /\ /\ (m.type = "Accept")
                            /\ (m.to = self)
                            /\ (m.bal >= rBallot[self])
                         /\ msgs' = (msgs \cup {([type |-> "AcceptReply",
                                                  from |-> self,
                                                  slot |-> m.slot,
                                                  bal |-> m.bal,
                                                  val |-> m.val,
                                                  shards |-> m.shards])})
                         /\ rBallot' = [rBallot EXCEPT ![self] = m.bal]
                         /\ rVoted' = [rVoted EXCEPT ![self][m.slot] = [bal |-> m.bal, val |-> m.val,
                                                                        shards |-> m.shards]]
                    /\ UNCHANGED <<lBallot, lStatus, proposed, learned>>
                 \/ /\ \E s \in Slots:
                         /\ lStatus[self][s] = "Accepting"
                         /\ \E v \in Values:
                              /\   \E MS \in SUBSET {
                                 m \in msgs:
                                     /\ m.type = "AcceptReply"
                                     /\ m.slot = s
                                     /\ m.bal = lBallot[self]
                                     /\ m.val = v}:
                                 LET arPat == {[replica |-> m.from,
                                                aShards |-> m.shards]: m \in MS}
                                 IN  PatternDecidesChosen(arPat)
                              /\ lStatus' = [lStatus EXCEPT ![self][s] = "Learned"]
                              /\ learned' = [learned EXCEPT ![s] = learned[s] \cup {v}]
                    /\ UNCHANGED <<msgs, lBallot, rBallot, rVoted, proposed>>

Next == (\E self \in Replicas: Replica(self))

Spec == Init /\ [][Next]_vars

\* END TRANSLATION

====
