//! Summerset client side executable.

use std::collections::{HashSet, HashMap};
use std::net::SocketAddr;

use clap::Parser;

use env_logger::Env;

use tokio::runtime::Builder;
use tokio::time::Duration;

use summerset::{SMRProtocol, ClientId, ReplicaId, SummersetError, pf_error};

mod drivers;
mod clients;

use crate::clients::{ClientMode, ClientRepl, ClientBench, ClientTester};

/// Command line arguments definition.
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct CliArgs {
    /// Name of SMR protocol to use.
    #[arg(short, long, default_value_t = String::from("RepNothing"))]
    protocol: String,

    /// Protocol-specific client configuration TOML string.
    /// Every '+' is treated as newline.
    #[arg(long, default_value_t = String::from(""))]
    config: String,

    /// Client ID.
    #[arg(short, long, default_value_t = 2857)]
    id: ClientId,

    /// List of server replica nodes, the order of which maps to replica IDs.
    /// Example: '-r host0:api_port0 -r host1:api_port1 -r host2:api_port2'.
    #[arg(short, long)]
    replicas: Vec<SocketAddr>,

    /// Client utility mode to run: repl|bench|tester.
    #[arg(short, long)]
    mode: String,

    /// Mode-specific client parameters TOML string.
    /// Every '+' is treated as newline.
    #[arg(long, default_value_t = String::from(""))]
    params: String,

    /// Number of tokio worker threads.
    #[arg(long, default_value_t = 2)]
    threads: usize,

    /// Reply timeout duration in millisecs.
    #[arg(long, default_value_t = 5000)]
    timeout_ms: u64,
}

impl CliArgs {
    /// Sanitize command line arguments, return `Ok(protocol)` on success
    /// or `Err(SummersetError)` on any error.
    fn sanitize(&self) -> Result<(ClientMode, SMRProtocol), SummersetError> {
        if self.replicas.is_empty() {
            return Err(SummersetError("replicas list is empty".into()));
        }

        // check for duplicate peers
        let mut replicas_set = HashSet::new();
        for addr in self.replicas.iter() {
            if replicas_set.contains(addr) {
                return Err(SummersetError(format!(
                    "duplicate replica address '{}' given",
                    addr
                )));
            }
            replicas_set.insert(addr);
        }

        if self.threads == 0 {
            Err(SummersetError(format!(
                "invalid number of threads {}",
                self.threads
            )))
        } else if self.timeout_ms == 0 {
            Err(SummersetError(format!(
                "invalid timeout duration {} ms",
                self.timeout_ms
            )))
        } else {
            let mode =
                ClientMode::parse_name(&self.mode).ok_or(SummersetError(
                    format!("utility mode '{}' unrecognized", self.mode),
                ))?;
            let protocol = SMRProtocol::parse_name(&self.protocol).ok_or(
                SummersetError(format!(
                    "protocol name '{}' unrecognized",
                    self.protocol
                )),
            )?;
            Ok((mode, protocol))
        }
    }
}

// Client side executable main entrance.
fn client_main() -> Result<(), SummersetError> {
    // read in and parse command line arguments
    let mut args = CliArgs::parse();
    let (mode, protocol) = args.sanitize()?;
    let mut servers = HashMap::new();
    for (id, &addr) in args.replicas.iter().enumerate() {
        servers.insert(id as ReplicaId, addr);
    }

    // parse optional config string if given
    let config_str = if args.config.is_empty() {
        None
    } else {
        args.config = args.config.replace('+', "\n");
        Some(&args.config[..])
    };

    // parse optional params string if given
    let params_str = if args.params.is_empty() {
        None
    } else {
        args.params = args.params.replace('+', "\n");
        Some(&args.params[..])
    };

    // create client struct with given servers list
    let mut stub = protocol.new_client_stub(args.id, servers, config_str)?;

    // create tokio multi-threaded runtime
    let runtime = Builder::new_multi_thread()
        .enable_all()
        .worker_threads(args.threads)
        .thread_name(format!("tokio-worker-client{}", args.id))
        .build()?;

    // enter tokio runtime, connect to the service, and do work
    runtime.block_on(async move {
        stub.setup().await?;

        match mode {
            ClientMode::Repl => {
                // run interactive REPL loop
                let mut repl = ClientRepl::new(
                    args.id,
                    stub,
                    Duration::from_millis(args.timeout_ms),
                );
                repl.run().await;
            }
            ClientMode::Bench => {
                // run benchmarking client
                let mut bench = ClientBench::new(
                    args.id,
                    stub,
                    Duration::from_millis(args.timeout_ms),
                    params_str,
                )?;
                bench.run().await?;
            }
            ClientMode::Tester => {
                // run correctness testing client
                let mut tester = ClientTester::new(
                    args.id,
                    stub,
                    Duration::from_millis(args.timeout_ms),
                    params_str,
                )?;
                tester.run().await?;
            }
        }

        Ok::<(), SummersetError>(()) // give type hint for this async closure
    })
}

fn main() -> Result<(), SummersetError> {
    env_logger::Builder::from_env(Env::default().default_filter_or("info"))
        .format_timestamp(None)
        .format_module_path(true)
        .format_target(false)
        .init();

    if let Err(e) = client_main() {
        pf_error!("client"; "client_main exitted: {}", e);
        Err(e)
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod client_args_tests {
    use super::*;

    #[test]
    fn sanitize_valid() -> Result<(), SummersetError> {
        let args = CliArgs {
            protocol: "RepNothing".into(),
            id: 7456,
            replicas: vec![
                "127.0.0.1:52700".parse()?,
                "127.0.0.1:52701".parse()?,
                "127.0.0.1:52702".parse()?,
            ],
            mode: "repl".into(),
            threads: 1,
            timeout_ms: 5000,
            config: "".into(),
            params: "".into(),
        };
        assert_eq!(
            args.sanitize(),
            Ok((ClientMode::Repl, SMRProtocol::RepNothing))
        );
        Ok(())
    }

    #[test]
    fn sanitize_invalid_protocol() -> Result<(), SummersetError> {
        let args = CliArgs {
            protocol: "InvalidProtocol".into(),
            id: 7456,
            replicas: vec![
                "127.0.0.1:52700".parse()?,
                "127.0.0.1:52701".parse()?,
                "127.0.0.1:52702".parse()?,
            ],
            mode: "repl".into(),
            threads: 1,
            timeout_ms: 5000,
            config: "".into(),
            params: "".into(),
        };
        assert!(args.sanitize().is_err());
        Ok(())
    }

    #[test]
    fn sanitize_empty_servers() -> Result<(), SummersetError> {
        let args = CliArgs {
            protocol: "RepNothing".into(),
            id: 7456,
            replicas: vec![],
            mode: "repl".into(),
            threads: 1,
            timeout_ms: 5000,
            config: "".into(),
            params: "".into(),
        };
        assert!(args.sanitize().is_err());
        Ok(())
    }

    #[test]
    fn sanitize_duplicate_server() -> Result<(), SummersetError> {
        let args = CliArgs {
            protocol: "RepNothing".into(),
            id: 7456,
            replicas: vec![
                "127.0.0.1:52700".parse()?,
                "127.0.0.1:52700".parse()?,
            ],
            mode: "repl".into(),
            threads: 1,
            timeout_ms: 5000,
            config: "".into(),
            params: "".into(),
        };
        assert!(args.sanitize().is_err());
        Ok(())
    }

    #[test]
    fn sanitize_invalid_mode() -> Result<(), SummersetError> {
        let args = CliArgs {
            protocol: "RepNothing".into(),
            id: 7456,
            replicas: vec![
                "127.0.0.1:52700".parse()?,
                "127.0.0.1:52701".parse()?,
                "127.0.0.1:52702".parse()?,
            ],
            mode: "invalid_mode".into(),
            threads: 1,
            timeout_ms: 5000,
            config: "".into(),
            params: "".into(),
        };
        assert!(args.sanitize().is_err());
        Ok(())
    }

    #[test]
    fn sanitize_invalid_threads() -> Result<(), SummersetError> {
        let args = CliArgs {
            protocol: "RepNothing".into(),
            id: 7456,
            replicas: vec![
                "127.0.0.1:52700".parse()?,
                "127.0.0.1:52701".parse()?,
                "127.0.0.1:52702".parse()?,
            ],
            mode: "repl".into(),
            threads: 0,
            timeout_ms: 5000,
            config: "".into(),
            params: "".into(),
        };
        assert!(args.sanitize().is_err());
        Ok(())
    }

    #[test]
    fn sanitize_invalid_timeout_ms() -> Result<(), SummersetError> {
        let args = CliArgs {
            protocol: "RepNothing".into(),
            id: 7456,
            replicas: vec![
                "127.0.0.1:52700".parse()?,
                "127.0.0.1:52701".parse()?,
                "127.0.0.1:52702".parse()?,
            ],
            mode: "repl".into(),
            threads: 1,
            timeout_ms: 0,
            config: "".into(),
            params: "".into(),
        };
        assert!(args.sanitize().is_err());
        Ok(())
    }
}
