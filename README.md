This is a private mirror of [Summerset](https://github.com/josehu07/summerset).

[![Format check](https://github.com/josehu07/summerset-private/actions/workflows/format.yml/badge.svg)](https://github.com/josehu07/summerset-private/actions?query=josehu07%3Aformat)
[![Build status](https://github.com/josehu07/summerset-private/actions/workflows/build.yml/badge.svg)](https://github.com/josehu07/summerset-private/actions?query=josehu07%3Abuild)
[![Unit tests status](https://github.com/josehu07/summerset-private/actions/workflows/tests_unit.yml/badge.svg)](https://github.com/josehu07/summerset-private/actions?query=josehu07%3Atests_unit)
[![Proc tests status](https://github.com/josehu07/summerset-private/actions/workflows/tests_proc.yml/badge.svg)](https://github.com/josehu07/summerset-private/actions?query=josehu07%3Atests_proc)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)

## Private-Public Sync Commands

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
git pull private
git checkout -b <PR_name> private/main
git merge -s ours main
python3 publish/public_repo_trim.py
# double check the trim and commit
git commit -am
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
| `SimplePush` | Pushing to peers w/o consistency guarantees |
| `ChainRep` | Bare implementation of Chain Replication ([paper](https://www.cs.cornell.edu/home/rvr/papers/OSDI04.pdf)) |
| `MultiPaxos` | Classic MultiPaxos ([paper](https://www.microsoft.com/en-us/research/uploads/prod/2016/12/paxos-simple-Copy.pdf)) w/ modern features |
| `EPaxos` | Leaderless Egalitarian Paxos ([paper](https://www.cs.cmu.edu/~dga/papers/epaxos-sosp2013.pdf)) |
| `Raft` | Raft with explicit log and strong leadership ([paper](https://raft.github.io/raft.pdf)) |
| `RSPaxos` | MultiPaxos w/ RS erasure code sharding ([paper](https://madsys.cs.tsinghua.edu.cn/publications/HPDC2014-mu.pdf)) |
| `CRaft` | Raft w/ erasure code sharding and fallback ([paper](https://www.usenix.org/system/files/fast20-wang_zizhong.pdf)) |
| `QuorumLeases` | Local reads at leaseholders when quiescent ([paper](https://www.cs.cmu.edu/~imoraru/papers/qrl.pdf)) |

Formal TLA+ specification of some protocols are provided in `tla+/`.

</details>

<details>
<summary>Why is Summerset different from other codebases...</summary>

- **Async Rust**: Summerset is written in Rust and demonstrates canonical usage of async programming structures backed by the [`tokio`](https://tokio.rs/) framework;
- **Event-based**: Summerset adopts a channel-oriented, event-based system architecture; each replication protocol is basically just a set of event handlers plus a `tokio::select!` loop;
- **Modularized**: Common components of a distributed KV store, e.g. network transport and durable logger, are cleanly separated from each other and connected through channels.
- **Protocol-generic**: With the above two points combined, Summerset is able to support a set of different replication protocols in one codebase, with common functionalities abstracted out.

These design choices make protocol implementation in Summerset straight-forward and understandable, without any sacrifice on performance. Comments / issues / PRs are always welcome!

</details>

## Build

Install the [Rust toolchain](https://rustup.rs/) if haven't. For \*nix:

```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

Build everything in debug or release (`-r`) mode:

```bash
cargo build [-r] --workspace [--features ...]
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

Summerset currently assumes a Linux environment and is tested on Ubuntu 24.04.

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

Currently supported client utility modes include: `repl` for an interactive CLI, `bench` for performance benchmarking, and `tester` for correctness testing.

### Helper Scripts

Some helper scripts for running Summerset processes are provided. First, install dependencies:

```bash
pip3 install toml
```

You can find the scripts for running Summerset processes locally in `scripts/`:

```bash
python3 scripts/local_cluster.py -h
python3 scripts/local_clients.py -h
```

And for a set of distributed machines (requiring correctly filled `scripts/remote_hosts.toml` file):

```bash
python3 scripts/distr_cluster.py -h
python3 scripts/distr_clients.py -h
```

Note that these scripts use `sudo` and assume specific ranges of available ports, so a Linux server machine environment is recommended.

## TODO List

- [x] async event-loop foundation
- [x] implementation of Chain Replication
  - [ ] failure detection & recovery
  - [ ] TLA+ spec
- [x] implementation of MultiPaxos
  - [x] TLA+ spec
- [x] implementation of RS-Paxos
- [x] implementation of Raft
  - [ ] TLA+ spec
- [x] implementation of CRaft
- [x] implementation of Crossword
  - [x] TLA+ spec
- [ ] long-term planned improvements
  - [ ] use a sophisticated storage backend
  - [ ] efficient state-transfer snapshotting
  - [ ] apply leasing whenever appropriate
  - [ ] more robust TCP msg infrastructure
  - [ ] membership discovery & view change
  - [ ] multi-versioning & stale reads
  - [ ] partitioned groups service structure
  - [ ] others: see in-code TODO comments
- [ ] client-side utilities
  - [x] interactive REPL
  - [x] benchmarking client
  - [x] unit tester
  - [ ] linearizability fuzzer integration
- [ ] better README & documentation

---

**Lore**: [Summerset Isles](https://en.uesp.net/wiki/Online:Summerset) is the name of an elvish archipelagic province in the Elder Scrolls series.
