---- MODULE Paxos_MC ----
\* EXTENDS PaxosByHand
EXTENDS PaxosPlusCal

SymmetricPerms ==      Permutations(Acceptors)
                  \cup Permutations(Values)

BoundedBallots == 0..2

(***********************)
(* Helper definitions. *)
(***********************)
VotedForIn(a, v, b) == \E m \in msgs: /\ m.type = "2b"
                                      /\ m.acceptor = a
                                      /\ m.val = v
                                      /\ m.bal = b

WontVoteIn(a, b) == /\ \A v \in Values: ~VotedForIn(a, v, b)
                    /\ maxPrepared[a] > b

ChosenIn(v, b) == \E Q \in Quorums: \A a \in Q: VotedForIn(a, v, b)

Chosen(v) == \E b \in Ballots : ChosenIn(v, b)

(***************************)
(* TypeOK check invariant. *)
(***************************)
Messages ==      [type: {"1a"}, bal: Ballots]
            \cup [type: {"1b"}, bal: Ballots,
                                maxAccepted: Ballots \cup {-1},
                                valAccepted: Values \cup {NullValue},
                                acceptor: Acceptors]
            \cup [type: {"2a"}, bal: Ballots,
                                val: Values]
            \cup [type: {"2b"}, bal: Ballots,
                                val: Values,
                                acceptor: Acceptors]

TypeOK == /\ msgs \in SUBSET Messages
          /\ maxPrepared \in [Acceptors -> Ballots \cup {-1}]
          /\ maxAccepted \in [Acceptors -> Ballots \cup {-1}]
          /\ valAccepted \in [Acceptors -> Values \cup {NullValue}]

(*******************************)
(* Message validity invariant. *)
(*******************************)
SafeAt(v, b) ==
    \A c \in 0..(b-1):
        \E Q \in Quorums:
            \A a \in Q: VotedForIn(a, v, c) \/ WontVoteIn(a, c)

MessageInv ==
    \A m \in msgs:
        /\ (m.type = "1b") => /\ m.bal =< maxPrepared[m.acceptor]
                              /\ \/ /\ m.maxAccepted \in Ballots
                                    /\ m.valAccepted \in Values
                                    /\ VotedForIn(m.acceptor, m.valAccepted, m.maxAccepted)
                                 \/ /\ m.maxAccepted = -1
                                    /\ m.valAccepted = NullValue
                              /\ \A c \in (m.maxAccepted+1)..(m.bal-1):
                                    ~\E v \in Values: VotedForIn(m.acceptor, v, c)
        /\ (m.type = "2a") => /\ SafeAt(m.val, m.bal)
                              /\ \A ma \in msgs:
                                    (ma.type = "2a") /\ (ma.bal = m.bal) => (ma.val = m.val)
        /\ (m.type = "2b") => /\ \E ma \in msgs: /\ ma.type = "2a"
                                                 /\ ma.bal = m.bal
                                                 /\ ma.val = m.val
                              /\ m.bal =< maxAccepted[m.acceptor]

(********************************)
(* Acceptor validity invariant. *)
(********************************)
AcceptorInv ==
    \A a \in Acceptors:
        /\ (maxAccepted[a] = -1) <=> (valAccepted[a] = NullValue)
        /\ maxPrepared[a] >= maxAccepted[a]
        /\ (maxAccepted[a] >= 0) => VotedForIn(a, valAccepted[a], maxAccepted[a])
        /\ \A c \in {b \in Ballots: b > maxAccepted[a]}:
                ~\E v \in Values: VotedForIn(a, v, c)

(************************************)
(* Consistency condition invariant. *)
(************************************)
ConsistencyInv ==
    \A v1, v2 \in Values: Chosen(v1) /\ Chosen(v2) => (v1 = v2)

(******************************)
(* Implements Consensus spec. *)
(******************************)
chosenValues == {v \in Values: Chosen(v)}

ConsensusModule == INSTANCE ConsensusSingle WITH chosen <- chosenValues
ConsensusProperty == ConsensusModule!Spec

====
