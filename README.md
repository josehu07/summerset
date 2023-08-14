# Summerset

[![Format check](https://github.com/josehu07/summerset/actions/workflows/format.yml/badge.svg)](https://github.com/josehu07/summerset/actions?query=josehu07%3Aformat)
[![Build status](https://github.com/josehu07/summerset/actions/workflows/build.yml/badge.svg)](https://github.com/josehu07/summerset/actions?query=josehu07%3Abuild)
[![Tests status](https://github.com/josehu07/summerset/actions/workflows/tests.yml/badge.svg)](https://github.com/josehu07/summerset/actions?query=josehu07%3Atests)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)

Summerset is a distributed key-value store supporting a wide range of state machine replication (SMR) protocols for research purposes. Summerset adopts a modularized, channel-oriented, and event-based programming structure. This makes protocol implementation much more straightforward and intuitive; a protocol is basically a set of handlers plus an event selection loop.

More protocols are actively being added.

## Build

Build everything in debug or release (`-r`) mode:

```bash
cargo build [-r] --workspace
```

Run all unit tests:

```bash
cargo test [-r] --workspace
```

Generate & open documentation for the core library in browser:

```bash
cargo doc --open
```

## Usage

### Run Servers

Run a server executable:

```bash
cargo run [-r] -p summerset_server -- -h
```

The default logging level is set as >= `info`. To display debugging or even tracing logs, set the `RUST_LOG` environment variable to `debug` or `trace`, e.g.:

```bash
RUST_LOG=debug cargo run ...
```

### Run Clients

Run a client executable:

```bash
cargo run [-r] -p summerset_client -- -h
```

Some helper scripts for running server nodes and clients as local processes are available in `scripts/`:

```bash
python3 scripts/local_cluster.py -h
python3 scripts/local_client.py -h
```

Complete cluster management and benchmarking scripts are available in another repo, [Wayrest](https://github.com/josehu07/wayrest), which is a Python module for managing replication protocol clusters and running distributed experiments.

## TODO List

- [x] event-based programming structure
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
  - [ ] replica management commands
  - [ ] benchmarking with YCSB input
- [ ] better README

---

**Lore**: [Summerset Isles](https://en.uesp.net/wiki/Online:Summerset) is the name of an elvish archipelagic province in the Elder Scrolls series.
