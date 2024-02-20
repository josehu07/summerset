## Practical SMR-style MultiPaxos Spec

This folder contains a practical state machine replication (SMR) style TLA+ specification of the MultiPaxos protocol. It very closely models how a real SMR system would implement MultiPaxos for strongly-consistent replication of a log of state machine commands.

### Files List

- `MultiPaxos.tla`: main protocol spec written in PlusCal and with translation attached
- `MultiPaxos_MC.tla`: entrance of running model checking; contains the checked `TypeOK` and `Linearizability` constraints
- `MultiPaxos_MS.cfg`: recommended model checking inputs and configurations
- TODO

### What’s Good About This Spec

This spec differs from traditional, general descriptions of Paxos/MultiPaxos in the following aspects:

- It models MultiPaxos in a practical SMR system style that’s much closer to real implementations than those classic, abstract specs (e.g., [here](https://github.com/josehu07/tla-examples/tree/master/specifications/Paxos))
  - All servers explicitly replicate a log of instances, each holding a command
  - Numbers of client write/read commands are made model inputs
  - Explicit termination condition is defined, thus semi-liveness can be checked by not having deadlocks
  - Safety constraint is defined as a clean client-viewed linearizability property upon termination
  - See the comments in the source files for more details…
- Optimizations are applied to the spec to reduce the state space W.L.O.G.
  - Model checking with recommended inputs completes in < 8 min on a 40-core server machine
  - Commenting out the `HandleCommitNotice` action (which is the least significant) reduces check time down to < 2 min
- It is easy to extend this spec and add more interesting features, for example:
  - Node failure injection
  - Leader lease and local read
