\* Single-decree Paxos.

\* Adapted from https://github.com/tlaplus/DrTLAPlus/blob/master/Paxos/Paxos.tla

---- MODULE Paxos ----
EXTENDS Integers, TLC

CONSTANT N

ASSUME N >= 5

====
