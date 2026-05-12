(*****************************************************************************)
(* Roster leases protocol spec. Exports an `ActiveGrants` view which the     *)
(* composing upper-layer spec (bodegaRefine.tla) uses as its `grants` bag.   *)
(*****************************************************************************)

---- MODULE RosterLeases ----
EXTENDS FiniteSets, Integers, TLC

(*******************************)
(* Model inputs & assumptions. *)
(*******************************)
CONSTANT Replicas,    \* symmetric set of server nodes
         MaxBallot,   \* upper bound on ballot numbers (for model checking)
         TGuard,      \* lease guard phase window length (in abstract ticks)
         TLease,      \* lease renewal extend window length (in abstract ticks)
         MaxTime      \* upper bound on abstract time for model checking

ReplicasAssumption == /\ IsFiniteSet(Replicas)
                      /\ Cardinality(Replicas) >= 1
                      /\ "none" \notin Replicas

MaxBallotAssumption == /\ MaxBallot \in Nat
                       /\ MaxBallot >= 1

TGuardAssumption == /\ TGuard \in Nat
                    /\ TGuard >= 1

TLeaseAssumption == /\ TLease \in Nat
                    /\ TLease >= 1

MaxTimeAssumption == /\ MaxTime \in Nat
                     /\ MaxTime >= TGuard + TLease

ASSUME /\ ReplicasAssumption
       /\ MaxBallotAssumption
       /\ TGuardAssumption
       /\ TLeaseAssumption
       /\ MaxTimeAssumption

Population == Cardinality(Replicas)

MajorityNum == (Population \div 2) + 1

----------

(********************************)
(* Useful constants & typedefs. *)
(********************************)
\* Time domain: node-local time tick. Clocks are strictly per-node; no message
\* carries a timestamp.
Times == 1..MaxTime
ExpireTimes == 0..(MaxTime + TGuard + TLease)
                \* stored expiration / guard deadlines; 0 is the "null" time

\* Ballots: like Paxos ballots. 0 is the "null" / never-seen sentinel.
Ballots == 1..MaxBallot

\* Roster definition.
Rosters == {ros \in [bal: Ballots, leader: Replicas, responders: SUBSET Replicas]:
            ros.leader \notin ros.responders}
                \* for smaller state space we exclude leader from "responders"
                \* in this spec; in practice can think of leader as a responder

Roster(b, l, resps) == [bal |-> b, leader |-> l, responders |-> resps]
                        \* each new ballot number maps to a new roster; this
                        \* includes the change of leader (as in classic
                        \* MultiPaxos) and/or the change of who're responders

NullRoster == [bal |-> 0, leader |-> "none", responders |-> {}]

\* Per-pair monotone sequence number. Bumped on every outgoing lease message
\* (request or reply). Both sides keep seq num counter to achieve message dedup,
\* a common property assumed by the paper for lease messages.
SeqNums == Nat

\* Lease protocol messages. All carry ballot, roster, and a per-pair seq num.
\*
\* Two optimizations trivial to reason about are not included for simplicity:
\*   - Piggybacking lease messages onto consensus heartbeats
\*   - Lightweight heartbeats that skip roster payload if ballot unchanged
GuardMsgs == [type: {"Guard"}, grantor: Replicas,
                               grantee: Replicas,
                               bal: Ballots,
                               ros: Rosters,
                               seq: SeqNums]

GuardMsg(f, p, b, ro, s) == [type |-> "Guard", grantor |-> f,
                                               grantee |-> p,
                                               bal |-> b,
                                               ros |-> ro,
                                               seq |-> s]

GuardReplyMsgs == [type: {"GuardReply"}, grantee: Replicas,
                                         grantor: Replicas,
                                         bal: Ballots,
                                         seq: SeqNums]

GuardReplyMsg(p, f, b, s) == [type |-> "GuardReply", grantee |-> p,
                                                     grantor |-> f,
                                                     bal |-> b,
                                                     seq |-> s]

RenewMsgs == [type: {"Renew"}, grantor: Replicas,
                               grantee: Replicas,
                               bal: Ballots,
                               ros: Rosters,
                               seq: SeqNums]

RenewMsg(f, p, b, ro, s) == [type |-> "Renew", grantor |-> f,
                                               grantee |-> p,
                                               bal |-> b,
                                               ros |-> ro,
                                               seq |-> s]

RenewReplyMsgs == [type: {"RenewReply"}, grantee: Replicas,
                                         grantor: Replicas,
                                         bal: Ballots,
                                         seq: SeqNums]

RenewReplyMsg(p, f, b, s) == [type |-> "RenewReply", grantee |-> p,
                                                     grantor |-> f,
                                                     bal |-> b,
                                                     seq |-> s]

RevokeMsgs == [type: {"Revoke"}, grantor: Replicas,
                                 grantee: Replicas,
                                 bal: Ballots,
                                 seq: SeqNums]

RevokeMsg(f, p, b, s) == [type |-> "Revoke", grantor |-> f,
                                             grantee |-> p,
                                             bal |-> b,
                                             seq |-> s]

RevokeReplyMsgs == [type: {"RevokeReply"}, grantee: Replicas,
                                           grantor: Replicas,
                                           bal: Ballots,
                                           seq: SeqNums]

RevokeReplyMsg(p, f, b, s) == [type |-> "RevokeReply", grantee |-> p,
                                                       grantor |-> f,
                                                       bal |-> b,
                                                       seq |-> s]

LeaseMsgs ==      GuardMsgs
             \cup GuardReplyMsgs
             \cup RenewMsgs
             \cup RenewReplyMsgs
             \cup RevokeMsgs
             \cup RevokeReplyMsgs

\* Per-node lease state. A node tracks:
\*   - leaseBal     : highest ballot seen (from LearnsNewRoster or any lease msg)
\*   - leaseRoster  : most recent roster knowledge (set by LearnsNewRoster)
\*   - asGrantor[p] : grantor-side state and timers
\*   - asGrantee[f] : grantee-side state and timers
GrantorStatuses == {"None", "Guarding", "Renewing", "Revoking"}
GranteeStatuses == {"None", "Guarded", "Renewed"}

GrantorState == [status: GrantorStatuses,
                 guardExpire: ExpireTimes,
                 leaseExpire: ExpireTimes,
                 ros: {NullRoster} \cup Rosters,
                 seq: SeqNums]

GranteeState == [status: GranteeStatuses,
                 guardExpire: ExpireTimes,
                 leaseExpire: ExpireTimes,
                 ros: {NullRoster} \cup Rosters,
                 seq: SeqNums]

NullGrantorState == [status |-> "None",
                     guardExpire |-> 0,
                     leaseExpire |-> 0,
                     ros |-> NullRoster,
                     seq |-> 0]

NullGranteeState == [status |-> "None",
                     guardExpire |-> 0,
                     leaseExpire |-> 0,
                     ros |-> NullRoster,
                     seq |-> 0]

LeaseStates == [leaseBal: {0} \cup Ballots,
                leaseRoster: {NullRoster} \cup Rosters,
                asGrantor: [Replicas -> GrantorState],
                asGrantee: [Replicas -> GranteeState]]

NullLeaseState == [leaseBal |-> 0,
                   leaseRoster |-> NullRoster,
                   asGrantor |-> [p \in Replicas |-> NullGrantorState],
                   asGrantee |-> [f \in Replicas |-> NullGranteeState]]

----------

(******************************)
(* Main algorithm in PlusCal. *)
(******************************)
(*--algorithm RosterLeases

variable leaseState = [r \in Replicas |-> NullLeaseState],
                                           \* per-node lease state
         leaseMsgs = {},                   \* lease-protocol messages
         time = [r \in Replicas |-> 1];    \* per-node monotone clock

define
    IsGranted(f, ros) ==
        \E p \in Replicas:
            /\ leaseState[p].asGrantee[f].status = "Renewed"
            /\ leaseState[p].asGrantee[f].leaseExpire > time[p]
            /\ leaseState[p].asGrantee[f].ros = ros

    \* Abstract view exposed to the upper layer: the set of (from, roster) grants
    \* currently in effect, viewed from grantees' angle.
    ActiveGrants ==
        {g \in [from: Replicas, roster: Rosters]: IsGranted(g.from, g.roster)}
end define;

\* Send a set of lease messages helper.
macro SendLease(set) begin
    leaseMsgs := leaseMsgs \cup set;
end macro;

\* Replica r learns, from some source, that ros is the known roster at
\* ballot b. Entry point for starting or changing lease sessions. The
\* protocol doesn't specify where (ros, b) comes from -- the refinement
\* composition (BodegaRefine.tla) binds this to upper-layer transitions.
\*
\* This action is what triggers revocation of old leases I was granting,
\* and initiates the granting of leases for the new roster.
macro LearnsNewRoster(r) begin
    with ros \in Rosters, b \in Ballots do
        await /\ b > leaseState[r].leaseBal
              /\ ros # leaseState[r].leaseRoster;
        leaseState[r].leaseBal := b ||
        leaseState[r].leaseRoster := ros;
    end with;
end macro;

\* Grantor r is in the middle of guarding or renewing to some peer p that is no
\* longer desired -- attempt to revoke lease grant.
macro GrantorRevokeLeases(f) begin
    with p \in Replicas do
        either
            \* r is guarding, no commitment has been made yet, so we can simply
            \* reset it locally
            await /\ leaseState[f].asGrantor[p].ros.bal < leaseState[f].leaseBal
                  /\ leaseState[f].asGrantor[p].status = "Guarding"
                  /\ leaseState[f].asGrantor[p].guardExpire > time[f];
            leaseState[f].asGrantor[p] :=
                [NullGrantorState EXCEPT !.seq = leaseState[f].asGrantor[p].seq];
        or
            \* r is in the middle of renewing, so mark Revoking and send Revoke;
            \* the state keeps its leaseExpire so the expiration safety is still
            \* honored. Lease is dropped on RevokeReply receipt (early) or natural
            \* expiry (by TimeTick)
            await /\ leaseState[f].asGrantor[p].ros.bal < leaseState[f].leaseBal
                  /\ leaseState[f].asGrantor[p].status = "Renewing"
                  /\ leaseState[f].asGrantor[p].leaseExpire > time[f];
            with newSeq = leaseState[f].asGrantor[p].seq + 1 do
                leaseState[f].asGrantor[p] :=
                    [leaseState[f].asGrantor[p] EXCEPT
                        !.status = "Revoking",
                        !.seq = newSeq];
                SendLease({RevokeMsg(f, p, leaseState[f].leaseBal, newSeq)});
            end with;
        end either;
    end with;
end macro;

\* Grantor r opens new leases to peers for new leaseRoster. Gated by the condition
\* that r knows a new roster and r is not actively granting to anyone.
macro GrantorInitiateLeases(f) begin
    await /\ leaseState[f].leaseRoster # NullRoster
          /\ \A p \in Replicas: leaseState[f].asGrantor[p].status = "None";
    leaseState[f].asGrantor :=
        [p \in Replicas |->
            [status |-> "Guarding",
             guardExpire |-> time[f] + TGuard,
             leaseExpire |-> 0,
             ros |-> leaseState[f].leaseRoster,
             seq |-> leaseState[f].asGrantor[p].seq + 1]];
    SendLease({GuardMsg(f, p,
                        leaseState[f].leaseBal,
                        leaseState[f].leaseRoster,
                        leaseState[f].asGrantor[p].seq + 1):
               p \in Replicas});
end macro;

\* Grantee p receives a Guard from grantor f. Accept iff ballot is at least
\* as high as p's view and the message's seq is strictly higher than any seq
\* ever observed on this pair.
macro HandleGuard(p) begin
    with m \in leaseMsgs do
        await /\ m.type = "Guard"
              /\ m.grantee = p
              /\ m.bal >= leaseState[p].leaseBal
              /\ m.seq > leaseState[p].asGrantee[m.grantor].seq
              /\ leaseState[p].asGrantee[m.grantor].status = "None";
        \* update ballot in case higher
        leaseState[p].leaseBal := m.bal ||
        \* start Guarded with guardExpire = time[p] + TGuard; bump seq num
        leaseState[p].asGrantee[m.grantor] :=
            [status |-> "Guarded",
             guardExpire |-> time[p] + TGuard,
             leaseExpire |-> 0,
             ros |-> m.ros,
             seq |-> m.seq + 1];
        \* reply GuardReply back to grantor, stamped with the new seq
        SendLease({GuardReplyMsg(p, m.grantor, m.bal, m.seq + 1)});
    end with;
end macro;

\* Grantor f receives a GuardReply: transition asGrantor[m.grantee] from
\* Guarding to Renewing; send first Renew.
macro HandleGuardReply(f) begin
    with m \in leaseMsgs do
        await /\ m.type = "GuardReply"
              /\ m.grantor = f
              /\ m.bal = leaseState[f].leaseBal
              /\ m.seq > leaseState[f].asGrantor[m.grantee].seq
              /\ leaseState[f].asGrantor[m.grantee].status = "Guarding";
        leaseState[f].asGrantor[m.grantee] :=
            [leaseState[f].asGrantor[m.grantee] EXCEPT
                !.status = "Renewing",
                !.leaseExpire = time[f] + TGuard + TLease,
                !.seq = m.seq + 1];
        \* send first Renew with bumped seq
        SendLease({RenewMsg(f, m.grantee,
                            m.bal, leaseState[f].leaseRoster, m.seq + 1)});
    end with;
end macro;

\* Grantor f spontaneously sends subsequent Renews to its current grantees.
\* Gated on: the lease to grantee is still Renewing; still before its leaseExpire;
\* and there's "room" to extend (time[f] + TLease past current leaseExpire) given
\* the time range we are checking.
macro SpontaneousRenew(f) begin
    await \E p \in Replicas:
            /\ leaseState[f].asGrantor[p].ros.bal = leaseState[f].leaseBal
            /\ leaseState[f].asGrantor[p].status = "Renewing"
            /\ time[f] < leaseState[f].asGrantor[p].leaseExpire
            /\ time[f] + TLease > leaseState[f].asGrantor[p].leaseExpire;
    with ps = {p \in Replicas:
                  /\ leaseState[f].asGrantor[p].ros.bal = leaseState[f].leaseBal
                  /\ leaseState[f].asGrantor[p].status = "Renewing"
                  /\ time[f] < leaseState[f].asGrantor[p].leaseExpire
                  /\ time[f] + TLease > leaseState[f].asGrantor[p].leaseExpire} do
        leaseState[f].asGrantor :=
            [p \in Replicas |->
                IF p \in ps
                  THEN [leaseState[f].asGrantor[p] EXCEPT
                            !.leaseExpire = leaseState[f].asGrantor[p].leaseExpire + TLease,
                            !.seq = leaseState[f].asGrantor[p].seq + 1]
                  ELSE leaseState[f].asGrantor[p]];
        SendLease({RenewMsg(f, p,
                            leaseState[f].leaseBal,
                            leaseState[f].leaseRoster,
                            leaseState[f].asGrantor[p].seq + 1):
                   p \in ps});
    end with;
end macro;

\* Grantee p receives a Renew. Dedup on the per-pair seq counter: each Renew
\* is acted upon at most once, and any stale Renew from a prior session
\* (whose seq <= current pair seq) is rejected.
\* First Renew (Guarded): accept only while time[p] < guardExpire.
\* Subsequent (Renewed): accept only while time[p] < leaseExpire.
macro HandleRenew(p) begin
    with m \in leaseMsgs do
        await /\ m.type = "Renew"
              /\ m.grantee = p
              /\ m.bal = leaseState[p].leaseBal
              /\ m.seq > leaseState[p].asGrantee[m.grantor].seq
              /\ \/ /\ leaseState[p].asGrantee[m.grantor].status = "Guarded"
                    /\ time[p] < leaseState[p].asGrantee[m.grantor].guardExpire
                 \/ /\ leaseState[p].asGrantee[m.grantor].status = "Renewed"
                    /\ time[p] < leaseState[p].asGrantee[m.grantor].leaseExpire
                    /\ time[p] + TLease > leaseState[p].asGrantee[m.grantor].leaseExpire;
        leaseState[p].asGrantee[m.grantor] :=
            [leaseState[p].asGrantee[m.grantor] EXCEPT
                !.status = "Renewed",
                !.leaseExpire = time[p] + TLease,
                !.bal = m.bal,
                !.seq = m.seq + 1];
        SendLease({RenewReplyMsg(p, m.grantor, m.bal, m.seq + 1)});
    end with;
end macro;

\* Grantor f receives a RenewReply. The send-time timer extension was an
\* over-commitment; tightening down happens here.
macro HandleRenewReply(f) begin
    with m \in leaseMsgs do
        await /\ m.type = "RenewReply"
              /\ m.grantor = f
              /\ m.bal = leaseState[f].leaseBal
              /\ m.seq > leaseState[f].asGrantor[m.grantee].seq
              /\ leaseState[f].asGrantor[m.grantee].status = "Renewing"
              /\ time[f] + TLease > leaseState[f].asGrantor[m.grantee].leaseExpire;
        leaseState[f].asGrantor[m.grantee] :=
            [leaseState[f].asGrantor[m.grantee] EXCEPT
                !.leaseExpire = time[f] + TLease,
                !.seq = m.seq];
    end with;
end macro;

\* Grantee p processes a Revoke from grantor f; acks so f can drop its lease
\* immediately without waiting for natural expiry.
macro HandleRevoke(p) begin
    with m \in leaseMsgs do
        await /\ m.type = "Revoke"
              /\ m.grantee = p
              /\ m.bal >= leaseState[p].leaseBal
              /\ m.seq > leaseState[p].asGrantee[m.grantor].seq
              /\ leaseState[p].asGrantee[m.grantor].status \in {"Guarded", "Renewed"};
        \* update ballot in case higher
        leaseState[p].leaseBal := m.bal ||
        \* clear my grantee state but preserve pair seq (bump to m.seq + 1)
        leaseState[p].asGrantee[m.grantor] := [NullGranteeState EXCEPT !.seq = m.seq + 1];
        SendLease({RevokeReplyMsg(p, m.grantor, m.bal, m.seq + 1)});
    end with;
end macro;

\* Grantor f receives a RevokeReply; drops the lease promptly.
macro HandleRevokeReply(f) begin
    with m \in leaseMsgs do
        await /\ m.type = "RevokeReply"
              /\ m.grantor = f
              /\ m.bal = leaseState[f].leaseBal
              /\ m.seq > leaseState[f].asGrantor[m.grantee].seq
              /\ leaseState[f].asGrantor[m.grantee].status = "Revoking";
        \* clear my grantor state but preserve pair seq
        leaseState[f].asGrantor[m.grantee] := [NullGrantorState EXCEPT !.seq = m.seq];
    end with;
end macro;

\* Advances time by one tick, and garbage-collects all expired lease states
\* because of this. The per-pair seq counter is preserved across GC so that
\* stale messages from past sessions cannot be re-admitted.
macro TimeTick() begin
    await \A r \in Replicas: time[r] < MaxTime;
    time := [r \in Replicas |-> time[r] + 1];
    leaseState := [r \in Replicas |->
        [leaseState[r] EXCEPT
            !.asGrantor =
                [p \in Replicas |->
                    IF \/ /\ leaseState[r].asGrantor[p].status \in {"Renewing", "Revoking"}
                          /\ leaseState[r].asGrantor[p].leaseExpire =< time[r]
                       \/ /\ leaseState[r].asGrantor[p].status = "Guarding"
                          /\ leaseState[r].asGrantor[p].guardExpire =< time[r]
                      THEN [NullGrantorState EXCEPT !.seq = leaseState[r].asGrantor[p].seq]
                      ELSE leaseState[r].asGrantor[p]],
            !.asGrantee  =
                [f \in Replicas |->
                    IF \/ /\ leaseState[r].asGrantee[f].status = "Renewed"
                          /\ leaseState[r].asGrantee[f].leaseExpire =< time[r]
                       \/ /\ leaseState[r].asGrantee[f].status = "Guarded"
                          /\ leaseState[r].asGrantee[f].guardExpire =< time[r]
                      THEN [NullGranteeState EXCEPT !.seq = leaseState[r].asGrantee[f].seq]
                      ELSE leaseState[r].asGrantee[f]]]];
end macro;

\* Replica main loop. Lease protocol is a modeled here as a background daemon
\* loop without explicit termination condition.
process Replica \in Replicas
begin
    lloop: while TRUE do
        either
            LearnsNewRoster(self);
        or
            GrantorRevokeLeases(self);
        or
            GrantorInitiateLeases(self);
        or
            HandleGuard(self);
        or
            HandleGuardReply(self);
        or
            SpontaneousRenew(self);
        or
            HandleRenew(self);
        or
            HandleRenewReply(self);
        or
            HandleRevoke(self);
        or
            HandleRevokeReply(self);
        or
            TimeTick();
        end either;
    end while;
end process;

end algorithm; *)

----------

\* BEGIN TRANSLATION (chksum(pcal) = "28937c73" /\ chksum(tla) = "456b4bdb")
VARIABLES leaseState, leaseMsgs, time

(* define statement *)
IsGranted(f, ros) ==
    \E p \in Replicas:
        /\ leaseState[p].asGrantee[f].status = "Renewed"
        /\ leaseState[p].asGrantee[f].leaseExpire > time[p]
        /\ leaseState[p].asGrantee[f].ros = ros



ActiveGrants ==
    {g \in [from: Replicas, roster: Rosters]: IsGranted(g.from, g.roster)}


vars == << leaseState, leaseMsgs, time >>

ProcSet == (Replicas)

Init == (* Global variables *)
        /\ leaseState = [r \in Replicas |-> NullLeaseState]
        /\ leaseMsgs = {}
        /\ time = [r \in Replicas |-> 1]

Replica(self) == \/ /\ \E ros \in Rosters:
                         \E b \in Ballots:
                           /\ /\ b > leaseState[self].leaseBal
                              /\ ros # leaseState[self].leaseRoster
                           /\ leaseState' = [leaseState EXCEPT ![self].leaseBal = b,
                                                               ![self].leaseRoster = ros]
                    /\ UNCHANGED <<leaseMsgs, time>>
                 \/ /\ \E p \in Replicas:
                         \/ /\ /\ leaseState[self].asGrantor[p].ros.bal < leaseState[self].leaseBal
                               /\ leaseState[self].asGrantor[p].status = "Guarding"
                               /\ leaseState[self].asGrantor[p].guardExpire > time[self]
                            /\ leaseState' = [leaseState EXCEPT ![self].asGrantor[p] = [NullGrantorState EXCEPT !.seq = leaseState[self].asGrantor[p].seq]]
                            /\ UNCHANGED leaseMsgs
                         \/ /\ /\ leaseState[self].asGrantor[p].ros.bal < leaseState[self].leaseBal
                               /\ leaseState[self].asGrantor[p].status = "Renewing"
                               /\ leaseState[self].asGrantor[p].leaseExpire > time[self]
                            /\ LET newSeq == leaseState[self].asGrantor[p].seq + 1 IN
                                 /\ leaseState' = [leaseState EXCEPT ![self].asGrantor[p] = [leaseState[self].asGrantor[p] EXCEPT
                                                                                                !.status = "Revoking",
                                                                                                !.seq = newSeq]]
                                 /\ leaseMsgs' = (leaseMsgs \cup ({RevokeMsg(self, p, leaseState'[self].leaseBal, newSeq)}))
                    /\ time' = time
                 \/ /\ /\ leaseState[self].leaseRoster # NullRoster
                       /\ \A p \in Replicas: leaseState[self].asGrantor[p].status = "None"
                    /\ leaseState' = [leaseState EXCEPT ![self].asGrantor = [p \in Replicas |->
                                                                                [status |-> "Guarding",
                                                                                 guardExpire |-> time[self] + TGuard,
                                                                                 leaseExpire |-> 0,
                                                                                 ros |-> leaseState[self].leaseRoster,
                                                                                 seq |-> leaseState[self].asGrantor[p].seq + 1]]]
                    /\ leaseMsgs' = (leaseMsgs \cup ({GuardMsg(self, p,
                                                               leaseState'[self].leaseBal,
                                                               leaseState'[self].leaseRoster,
                                                               leaseState'[self].asGrantor[p].seq + 1):
                                                      p \in Replicas}))
                    /\ time' = time
                 \/ /\ \E m \in leaseMsgs:
                         /\ /\ m.type = "Guard"
                            /\ m.grantee = self
                            /\ m.bal >= leaseState[self].leaseBal
                            /\ m.seq > leaseState[self].asGrantee[m.grantor].seq
                            /\ leaseState[self].asGrantee[m.grantor].status = "None"
                         /\ leaseState' = [leaseState EXCEPT ![self].leaseBal = m.bal,
                                                             ![self].asGrantee[m.grantor] = [status |-> "Guarded",
                                                                                             guardExpire |-> time[self] + TGuard,
                                                                                             leaseExpire |-> 0,
                                                                                             ros |-> m.ros,
                                                                                             seq |-> m.seq + 1]]
                         /\ leaseMsgs' = (leaseMsgs \cup ({GuardReplyMsg(self, m.grantor, m.bal, m.seq + 1)}))
                    /\ time' = time
                 \/ /\ \E m \in leaseMsgs:
                         /\ /\ m.type = "GuardReply"
                            /\ m.grantor = self
                            /\ m.bal = leaseState[self].leaseBal
                            /\ m.seq > leaseState[self].asGrantor[m.grantee].seq
                            /\ leaseState[self].asGrantor[m.grantee].status = "Guarding"
                         /\ leaseState' = [leaseState EXCEPT ![self].asGrantor[m.grantee] = [leaseState[self].asGrantor[m.grantee] EXCEPT
                                                                                                !.status = "Renewing",
                                                                                                !.leaseExpire = time[self] + TGuard + TLease,
                                                                                                !.seq = m.seq + 1]]
                         /\ leaseMsgs' = (leaseMsgs \cup ({RenewMsg(self, m.grantee,
                                                                    m.bal, leaseState'[self].leaseRoster, m.seq + 1)}))
                    /\ time' = time
                 \/ /\ \E p \in Replicas:
                         /\ leaseState[self].asGrantor[p].ros.bal = leaseState[self].leaseBal
                         /\ leaseState[self].asGrantor[p].status = "Renewing"
                         /\ time[self] < leaseState[self].asGrantor[p].leaseExpire
                         /\ time[self] + TLease > leaseState[self].asGrantor[p].leaseExpire
                    /\ LET ps == {p \in Replicas:
                                     /\ leaseState[self].asGrantor[p].ros.bal = leaseState[self].leaseBal
                                     /\ leaseState[self].asGrantor[p].status = "Renewing"
                                     /\ time[self] < leaseState[self].asGrantor[p].leaseExpire
                                     /\ time[self] + TLease > leaseState[self].asGrantor[p].leaseExpire} IN
                         /\ leaseState' = [leaseState EXCEPT ![self].asGrantor = [p \in Replicas |->
                                                                                     IF p \in ps
                                                                                       THEN [leaseState[self].asGrantor[p] EXCEPT
                                                                                                 !.leaseExpire = leaseState[self].asGrantor[p].leaseExpire + TLease,
                                                                                                 !.seq = leaseState[self].asGrantor[p].seq + 1]
                                                                                       ELSE leaseState[self].asGrantor[p]]]
                         /\ leaseMsgs' = (leaseMsgs \cup ({RenewMsg(self, p,
                                                                    leaseState'[self].leaseBal,
                                                                    leaseState'[self].leaseRoster,
                                                                    leaseState'[self].asGrantor[p].seq + 1):
                                                           p \in ps}))
                    /\ time' = time
                 \/ /\ \E m \in leaseMsgs:
                         /\ /\ m.type = "Renew"
                            /\ m.grantee = self
                            /\ m.bal = leaseState[self].leaseBal
                            /\ m.seq > leaseState[self].asGrantee[m.grantor].seq
                            /\ \/ /\ leaseState[self].asGrantee[m.grantor].status = "Guarded"
                                  /\ time[self] < leaseState[self].asGrantee[m.grantor].guardExpire
                               \/ /\ leaseState[self].asGrantee[m.grantor].status = "Renewed"
                                  /\ time[self] < leaseState[self].asGrantee[m.grantor].leaseExpire
                                  /\ time[self] + TLease > leaseState[self].asGrantee[m.grantor].leaseExpire
                         /\ leaseState' = [leaseState EXCEPT ![self].asGrantee[m.grantor] = [leaseState[self].asGrantee[m.grantor] EXCEPT
                                                                                                !.status = "Renewed",
                                                                                                !.leaseExpire = time[self] + TLease,
                                                                                                !.bal = m.bal,
                                                                                                !.seq = m.seq + 1]]
                         /\ leaseMsgs' = (leaseMsgs \cup ({RenewReplyMsg(self, m.grantor, m.bal, m.seq + 1)}))
                    /\ time' = time
                 \/ /\ \E m \in leaseMsgs:
                         /\ /\ m.type = "RenewReply"
                            /\ m.grantor = self
                            /\ m.bal = leaseState[self].leaseBal
                            /\ m.seq > leaseState[self].asGrantor[m.grantee].seq
                            /\ leaseState[self].asGrantor[m.grantee].status = "Renewing"
                            /\ time[self] + TLease > leaseState[self].asGrantor[m.grantee].leaseExpire
                         /\ leaseState' = [leaseState EXCEPT ![self].asGrantor[m.grantee] = [leaseState[self].asGrantor[m.grantee] EXCEPT
                                                                                                !.leaseExpire = time[self] + TLease,
                                                                                                !.seq = m.seq]]
                    /\ UNCHANGED <<leaseMsgs, time>>
                 \/ /\ \E m \in leaseMsgs:
                         /\ /\ m.type = "Revoke"
                            /\ m.grantee = self
                            /\ m.bal >= leaseState[self].leaseBal
                            /\ m.seq > leaseState[self].asGrantee[m.grantor].seq
                            /\ leaseState[self].asGrantee[m.grantor].status \in {"Guarded", "Renewed"}
                         /\ leaseState' = [leaseState EXCEPT ![self].leaseBal = m.bal,
                                                             ![self].asGrantee[m.grantor] = [NullGranteeState EXCEPT !.seq = m.seq + 1]]
                         /\ leaseMsgs' = (leaseMsgs \cup ({RevokeReplyMsg(self, m.grantor, m.bal, m.seq + 1)}))
                    /\ time' = time
                 \/ /\ \E m \in leaseMsgs:
                         /\ /\ m.type = "RevokeReply"
                            /\ m.grantor = self
                            /\ m.bal = leaseState[self].leaseBal
                            /\ m.seq > leaseState[self].asGrantor[m.grantee].seq
                            /\ leaseState[self].asGrantor[m.grantee].status = "Revoking"
                         /\ leaseState' = [leaseState EXCEPT ![self].asGrantor[m.grantee] = [NullGrantorState EXCEPT !.seq = m.seq]]
                    /\ UNCHANGED <<leaseMsgs, time>>
                 \/ /\ \A r \in Replicas: time[r] < MaxTime
                    /\ time' = [r \in Replicas |-> time[r] + 1]
                    /\ leaseState' =           [r \in Replicas |->
                                     [leaseState[r] EXCEPT
                                         !.asGrantor =
                                             [p \in Replicas |->
                                                 IF \/ /\ leaseState[r].asGrantor[p].status \in {"Renewing", "Revoking"}
                                                       /\ leaseState[r].asGrantor[p].leaseExpire =< time'[r]
                                                    \/ /\ leaseState[r].asGrantor[p].status = "Guarding"
                                                       /\ leaseState[r].asGrantor[p].guardExpire =< time'[r]
                                                   THEN [NullGrantorState EXCEPT !.seq = leaseState[r].asGrantor[p].seq]
                                                   ELSE leaseState[r].asGrantor[p]],
                                         !.asGrantee  =
                                             [f \in Replicas |->
                                                 IF \/ /\ leaseState[r].asGrantee[f].status = "Renewed"
                                                       /\ leaseState[r].asGrantee[f].leaseExpire =< time'[r]
                                                    \/ /\ leaseState[r].asGrantee[f].status = "Guarded"
                                                       /\ leaseState[r].asGrantee[f].guardExpire =< time'[r]
                                                   THEN [NullGranteeState EXCEPT !.seq = leaseState[r].asGrantee[f].seq]
                                                   ELSE leaseState[r].asGrantee[f]]]]
                    /\ UNCHANGED leaseMsgs

Next == (\E self \in Replicas: Replica(self))

Spec == Init /\ [][Next]_vars

\* END TRANSLATION

====
