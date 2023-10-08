This is a private mirror of [Summerset](https://github.com/josehu07/summerset). Below are a memo of development commands...

[![Format check](https://github.com/josehu07/summerset-private/actions/workflows/format.yml/badge.svg)](https://github.com/josehu07/summerset-private/actions?query=josehu07%3Aformat)
[![Build status](https://github.com/josehu07/summerset-private/actions/workflows/build.yml/badge.svg)](https://github.com/josehu07/summerset-private/actions?query=josehu07%3Abuild)
[![Unit tests status](https://github.com/josehu07/summerset-private/actions/workflows/tests_unit.yml/badge.svg)](https://github.com/josehu07/summerset-private/actions?query=josehu07%3Atests_unit)
[![Proc tests status](https://github.com/josehu07/summerset-private/actions/workflows/tests_proc.yml/badge.svg)](https://github.com/josehu07/summerset-private/actions?query=josehu07%3Atests_proc)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)

To create a branch to track public repo `main`, pull new things from it, and merge into the private `main`:

```bash
# in the private repo:
git remote add public git@github.com:josehu07/summerset.git
git config --add --local checkout.defaultRemote origin
git checkout -b public-main
git branch --set-upstream-to=public/main public-main
git checkout main
# skip the above for later times
git pull public
git merge public-main
git push
```

To create a pull request on the public repo to make batched contributions from private repo `main`:

```bash
# in the public repo:
git remote add private git@github.com:josehu07/summerset-private.git
git config --add --local checkout.defaultRemote origin
# skip the above for later times
git checkout -b <PR_name>
git branch --set-upstream-to=private/main <PR_name>
git pull private
git push origin <PR_name>
# then, on GitHub, make a squashing PR from <PR_name> branch to main
```

# Summerset

[![Format check](https://github.com/josehu07/summerset/actions/workflows/format.yml/badge.svg)](https://github.com/josehu07/summerset/actions?query=josehu07%3Aformat)
[![Build status](https://github.com/josehu07/summerset/actions/workflows/build.yml/badge.svg)](https://github.com/josehu07/summerset/actions?query=josehu07%3Abuild)
[![Unit tests status](https://github.com/josehu07/summerset/actions/workflows/tests_unit.yml/badge.svg)](https://github.com/josehu07/summerset/actions?query=josehu07%3Atests_unit)
[![Proc tests status](https://github.com/josehu07/summerset/actions/workflows/tests_proc.yml/badge.svg)](https://github.com/josehu07/summerset/actions?query=josehu07%3Atests_proc)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)

Summerset is a distributed, replicated, protocol-generic key-value store supporting a wide range of state machine replication (SMR) protocols for research purposes. More protocols are actively being added.

<p align="center">
  <img width="360" src="./README.png">
</p>

<details>
<summary>List of currently implemented protocols...</summary>

| Name | Description |
| :--: | :---------- |
| `RepNothing` | Simplest protocol w/o any replication |
| `SimplePush` | Pushing to peers w/o any consistency guarantees |
| `MultiPaxos` | Classic [MultiPaxos](https://www.microsoft.com/en-us/research/uploads/prod/2016/12/paxos-simple-Copy.pdf) protocol |
| `RS-Paxos` | MultiPaxos w/ Reed-Solomon erasure code sharding |
| `Raft` | Explicit notion of log and strong leadership |

Formal TLA+ specification of some protocols are provided in `tla+/`.

</details>

<details>
<summary>Why is Summerset different from other codebases...</summary>

- **Async Rust**: Summerset is written in Rust and demonstrates canonical usage of async programming structures backed by the [`tokio`](https://tokio.rs/) framework;
- **Event-based**: Summerset adopts a channel-oriented, event-based system architecture; each replication protocol is basically just a set of event handlers plus a `tokio::select!` loop;
- **Modularized**: Common components of a distributed KV store, e.g. network transport and durable logger, are cleanly separated from each other and connected through channels.
- **Protocol-generic**: With the above two points combined, Summerset is able to support a set of different replication protocols in one codebase, each being just a single file, with common functionalities abstracted out.

These design choices make protocol implementation in Summerset surprisingly straight-forward and **understandable**, without any sacrifice on performance. Comments / issues / PRs are always welcome!

</details>

## Build

Install the [Rust toolchain](https://rustup.rs/) if haven't. For \*nix:

```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

Build everything in debug or release (`-r`) mode:

```bash
cargo build [-r] --workspace
```

Run all unit tests:

```bash
cargo test --workspace
```

Generate & open documentation for the core library in browser:

```bash
cargo doc --open
```

## Usage

### Launch a Cluster

First, launch the cluster manager oracle (which only serves setup & testing purposes and does not participate in any of the protocol logic):

```bash
cargo run [-r] -p summerset_manager -- -h
```

Then, launch server replica executables:

```bash
cargo run [-r] -p summerset_server -- -h
```

The default logging level is set as >= `info`. To display debugging or even tracing logs, set the `RUST_LOG` environment variable to `debug` or `trace`, e.g.:

```bash
RUST_LOG=debug cargo run ...
```

### Run Client Endpoints

To run a client endpoint executable:

```bash
cargo run [-r] -p summerset_client -- -h
```

Currently supported client utility modes include: `repl` for an interactive CLI, `bench` for performance benchmarking, and `tester` for correctness testing (not yet complete).

### Helper Scripts

Some helper scripts for running everything as local processes are available in `scripts/`:

```bash
python3 scripts/local_cluster.py -h
python3 scripts/local_client.py -h
```

Complete cluster management and benchmarking scripts are available in another repo, [Wayrest](https://github.com/josehu07/wayrest) (not yet public), which is a Python module for managing replication protocol clusters and running distributed experiments.

## TODO List

- [x] event-based programming structure
- [x] cluster manager oracle impl.
- [x] implementation of MultiPaxos
  - [x] client-side timeout/retry logic
  - [x] state persistence & restart check
  - [x] automatic leader election, backoffs
  - [x] snapshotting & garbage collection
  - [ ] specialize read-only commands?
  - [ ] separate commit vs. exec responses?
  - [ ] membership discovery & view changes
  - [x] TLA+ spec
- [x] implementation of RS-Paxos
  - [ ] TLA+ spec
- [ ] implementation of Raft
  - [ ] snapshotting & garbage collection
  - [ ] membership discovery & view changes
  - [ ] TLA+ spec
- [ ] implementation of CRaft
  - [ ] TLA+ spec
- [x] implementation of Crossword prototype
  - [x] fault recovery reads
  - [x] follower gossiping
  - [x] fall-back mechanism
  - [ ] workload adaptiveness
  - [ ] unbalanced assignment
  - [ ] TLA+ spec
- [x] client-side utilities
  - [x] REPL-style client
  - [x] random benchmarking client
  - [x] testing client
  - [ ] YCSB-driven client
- [ ] better README & documentation

---

**Lore**: [Summerset Isles](https://en.uesp.net/wiki/Online:Summerset) is the name of an elvish archipelagic province in the Elder Scrolls series.
