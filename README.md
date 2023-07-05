# Summerset

[![Format check](https://github.com/josehu07/summerset/actions/workflows/format.yml/badge.svg)](https://github.com/josehu07/summerset/actions?query=josehu07%3Aformat)
[![Build status](https://github.com/josehu07/summerset/actions/workflows/build.yml/badge.svg)](https://github.com/josehu07/summerset/actions?query=josehu07%3Abuild)
[![Tests status](https://github.com/josehu07/summerset/actions/workflows/tests.yml/badge.svg)](https://github.com/josehu07/summerset/actions?query=josehu07%3Atests)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)

Summerset is a distributed key-value store incorporating a wide range of state machine replication (SMR) protocols for research purposes.

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

A helper script, `cluster.py`, for running a set of server nodes as local processes is also provided:

```bash
python3 cluster.py -h
```

Complete cluster management and benchmarking scripts are available in another repo, [Wayrest](https://github.com/josehu07/wayrest), which is a Python module for managing replication protocol clusters and running distributed experiments.

### Run Clients

Run a client executable:

```bash
cargo run [-r] -p summerset_client -- -h
```

## TODO List

- [x] event-based programming structure
- [ ] implementation of classic protocols
- [ ] complete client, tests, & benchmarks
- [ ] differentiate read/non-read commands
- [ ] membership discovery & view changes
- [ ] snapshotting & garbage collection
- [ ] better README

---

**Lore**: [Summerset Isles](https://en.uesp.net/wiki/Online:Summerset) is the name of an elvish archipelagic province in the Elder Scrolls series.
