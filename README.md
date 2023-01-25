# Summerset

[![Format check](https://github.com/josehu07/summerset/actions/workflows/format.yml/badge.svg)](https://github.com/josehu07/summerset/actions?query=josehu07%3Aformat)
[![Build status](https://github.com/josehu07/summerset/actions/workflows/build.yml/badge.svg)](https://github.com/josehu07/summerset/actions?query=josehu07%3Abuild)
[![Tests status](https://github.com/josehu07/summerset/actions/workflows/tests.yml/badge.svg)](https://github.com/josehu07/summerset/actions?query=josehu07%3Atests)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)

Summerset is a distributed key-value store incorporating a wide range of state machine replication (SMR) protocols for research purposes.

## Code Structure

This codebase comprises the following pieces:

* `proto/`: protobuf definitions for various APIs and replication protocols
* `src/`: the core Summerset library, linked by both `_server` and `_client`
* `summerset_server`: the Summerset server standalone executable
* `summerset_client`: the Summerset client-side library, linked by all client executables
* `summerset_bench`: a client executable for benchmarking purposes

## Build

Build in debug mode:

```bash
cargo build --all
```

Run all unit tests:

```bash
cargo test --all
```

Build in release mode:

```bash
cargo build --all --release
```

## Usage

Run a server executable:

```bash
cargo run -p summerset_server -- -h
```

Run the benchmarking client:

```bash
cargo run -p summerset_bench -- -h
```

The default logging level is set as >= `info`. To display debugging or even tracing logs, set the `RUST_LOG` environment variable to `debug` or `trace`:

```bash
RUST_LOG=debug cargo run ...
```

## TODO List

* [ ] transport layer abstraction
* [ ] user-friendly swarm run scripts
* [ ] Tonic RPC timeout handling
* [ ] differentiate between read/non-read commands
* [ ] finer-grained state machine locking
* [ ] more protocols, comprehensive tests & CI
* [ ] true benchmarking client
* [ ] better usage README
