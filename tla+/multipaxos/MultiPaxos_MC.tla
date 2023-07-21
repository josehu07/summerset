---- MODULE MultiPaxos_MC ----
EXTENDS MultiPaxos

SymmetricPerms ==      Permutations(Proposers)
                  \cup Permutations(Acceptors)
                  \cup Permutations(Values)

BoundedBallots == 0..2
BoundedSlots == 0..1

(***********************)
(* Helper definitions. *)
(***********************)
ProposedIn(p, v, s, b) == \E m \in msgs: /\ m.type = "2a"
                                         /\ m.from = p
                                         /\ m.slot = s
                                         /\ m.bal = b
                                         /\ m.val = v

VotedForIn(a, v, s, b) == \E m \in msgs: /\ m.type = "2b"
                                         /\ m.from = a
                                         /\ m.slot = s
                                         /\ m.bal = b
                                         /\ m.val = v

WontVoteIn(a, s, b) == /\ \A v \in Values: ~VotedForIn(a, v, s, b)
                       /\ aBallot[a] > b

ChosenIn(v, s, b) == \E Q \in Quorums: \A a \in Q: VotedForIn(a, v, s, b)

Chosen(v, s) == \E b \in Ballots: ChosenIn(v, s, b)

(***************************)
(* TypeOK check invariant. *)
(***************************)
SlotVotes == [Slots -> [bal: Ballots \cup {-1},
                        val: Values \cup {NullValue}]]

Messages ==      [type: {"1a"}, from: Proposers,
                                bal: Ballots]
            \cup [type: {"1b"}, from: Acceptors,
                                bal: Ballots,
                                voted: SlotVotes]
            \cup [type: {"2a"}, from: Proposers,
                                slot: Slots,
                                bal: Ballots,
                                val: Values]
            \cup [type: {"2b"}, from: Acceptors,
                                slot: Slots,
                                bal: Ballots,
                                val: Values]

TypeOK == /\ msgs \in SUBSET Messages
          /\ pBallot \in [Proposers -> Ballots \cup {-1}]
          /\ aBallot \in [Acceptors -> Ballots \cup {-1}]
          /\ aVoted \in [Acceptors -> SlotVotes]

(*******************************)
(* Message validity invariant. *)
(*******************************)
SafeAt(v, s, b) ==
    \A c \in 0..(b-1):
        \E Q \in Quorums:
            \A a \in Q: VotedForIn(a, v, s, c) \/ WontVoteIn(a, s, c)

MessageInv ==
    \A m \in msgs:
        /\ (m.type = "1b") => /\ m.bal =< aBallot[m.from]
                              /\ \A s \in Slots:
                                    /\ \/ /\ m.voted[s] \in [bal: Ballots, val: Values]
                                          /\ VotedForIn(m.from, m.voted[s].val, s, m.voted[s].bal)
                                       \/ m.voted[s] = [bal |-> -1, val |-> NullValue]
                                    /\ \A c \in (m.voted[s].bal+1)..(m.bal-1):
                                            ~\E v \in Values: VotedForIn(m.from, v, s, c)
        /\ (m.type = "2a") => /\ SafeAt(m.val, m.slot, m.bal)
                              /\ \A ma \in msgs:
                                    (ma.type = "2a") /\ (ma.slot = m.slot) /\ (ma.bal = m.bal) => (ma.val = m.val)
        /\ (m.type = "2b") => /\ \E ma \in msgs: /\ ma.type = "2a"
                                                 /\ ma.slot = m.slot
                                                 /\ ma.bal = m.bal
                                                 /\ ma.val = m.val
                              /\ m.bal =< aVoted[m.from][m.slot].bal

(********************************)
(* Acceptor validity invariant. *)
(********************************)
AcceptorInv ==
    \A a \in Acceptors:
        \A s \in Slots:
            /\ (aVoted[a][s].bal = -1) <=> (aVoted[a][s].val = NullValue)
            /\ aBallot[a] >= aVoted[a][s].bal
            /\ (aVoted[a][s].bal >= 0) => VotedForIn(a, aVoted[a][s].val, s, aVoted[a][s].bal)
            /\ \A c \in {b \in Ballots: b > aVoted[a][s].bal}:
                    ~\E v \in Values: VotedForIn(a, v, s, c)

(************************************)
(* Consistency condition invariant. *)
(************************************)
ConsistencyInv ==
    \A s \in Slots:
        \A v1, v2 \in Values: Chosen(v1, s) /\ Chosen(v2, s) => (v1 = v2)

(******************************)
(* Implements Consensus spec. *)
(******************************)
chosenValues == [s \in Slots |-> {v \in Values: Chosen(v, s)}]

ConsensusModule == INSTANCE ConsensusMulti WITH chosen <- chosenValues
ConsensusProperty == ConsensusModule!Spec

====
