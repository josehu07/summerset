# Summerset

[![Format check](https://github.com/josehu07/summerset/actions/workflows/format.yml/badge.svg)](https://github.com/josehu07/summerset/actions?query=josehu07%3Aformat)
[![Build status](https://github.com/josehu07/summerset/actions/workflows/build.yml/badge.svg)](https://github.com/josehu07/summerset/actions?query=josehu07%3Abuild)
[![Tests status](https://github.com/josehu07/summerset/actions/workflows/tests.yml/badge.svg)](https://github.com/josehu07/summerset/actions?query=josehu07%3Atests)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)

Summerset is a distributed key-value store supporting a wide range of state machine replication (SMR) protocols for research purposes. More protocols are actively being added.

<details>
<summary>List of currently implemented protocols...</summary>

| :--: | :---------- |
| Name | Description |
| `RepNothing` | Simplest protocol w/o any replication |
| `SimplePush` | Pushing to peers w/o any consistency guarantees |
| `MultiPaxos` | Classic [MultiPaxos](https://www.microsoft.com/en-us/research/uploads/prod/2016/12/paxos-simple-Copy.pdf) protocol |

Formal TLA+ specification of some protocols are provided in `tla+/`.

</details>

<details>
<summary>Why is Summerset different from other codebases...</summary>

- **Async Rust**: Summerset is written in Rust and demonstrates canonical usage of async programming structures backed by the [`tokio`](https://tokio.rs/) framework;
- **Event-based**: Summerset adopts a channel-oriented, event-based system architecture; each replication protocol is basically just a set of event handlers plus a `tokio::select!` loop;
- **Modularized**: Common components of a distributed KV store, e.g. network transport and durable logger, are cleanly separated from each other and connected through channels.

These design choices make protocol implementation in Summerset surprisingly straight-forward and **understandable**, without any sacrifice on performance. Comments / issues / PRs are always welcome!

</details>

## Build

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
  - [ ] state persistence & restart check
  - [ ] automatic leader election, backoffs
  - [ ] snapshotting & garbage collection
  - [ ] specialize read-only commands?
  - [ ] separate commit vs. exec responses?
  - [ ] membership discovery & view changes
- [ ] implementation of Raft
- [ ] implementation of Crossword prototype
- [x] client-side utilities
  - [x] REPL-style client
  - [x] random benchmarking client
  - [x] testing client
  - [ ] benchmarking with YCSB input
- [ ] better README

---

**Lore**: [Summerset Isles](https://en.uesp.net/wiki/Online:Summerset) is the name of an elvish archipelagic province in the Elder Scrolls series.
