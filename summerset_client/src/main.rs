//! Summerset client side executable.

use std::net::SocketAddr;
use std::process::ExitCode;

use clap::Parser;

use tokio::runtime::Builder;
use tokio::time::Duration;

use summerset::{logger_init, pf_error, pf_warn, SmrProtocol, SummersetError};

mod clients;
mod drivers;

use crate::clients::{
    ClientBench, ClientMess, ClientMode, ClientRepl, ClientTester,
};

/// Command line arguments definition.
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct CliArgs {
    /// Name of SMR protocol to use.
    #[arg(short, long)]
    protocol: String,

    /// Protocol-specific client configuration TOML string.
    /// Every '+' is treated as newline.
    #[arg(long, default_value_t = String::from(""))]
    config: String,

    /// Client utility mode to run: repl|bench|tester.
    #[arg(short, long)]
    utility: String,

    /// Mode-specific client parameters TOML string.
    /// Every '+' is treated as newline.
    #[arg(long, default_value_t = String::from(""))]
    params: String,

    /// Base address 'localip:port' to use in bind addresses for sockets
    /// that communicate with server nodes.
    /// Ports [port, port + N] must be available at process launch.
    #[arg(short, long)]
    bind_base: SocketAddr,

    /// Cluster manager oracle's client-facing address.
    #[arg(short, long)]
    manager: SocketAddr,

    /// Number of tokio worker threads.
    #[arg(long, default_value_t = 4)]
    threads: usize,

    /// Reply timeout duration in millisecs.
    #[arg(long, default_value_t = 5000)]
    timeout_ms: u64,
}

impl CliArgs {
    /// Sanitize command line arguments, return `Ok(protocol)` on success
    /// or `Err(SummersetError)` on any error.
    fn sanitize(&self) -> Result<(ClientMode, SmrProtocol), SummersetError> {
        if self.threads < 2 {
            Err(SummersetError::msg(format!(
                "invalid number of threads {}",
                self.threads
            )))
        } else if self.timeout_ms == 0 {
            Err(SummersetError::msg(format!(
                "invalid timeout duration {} ms",
                self.timeout_ms
            )))
        } else {
            let mode = ClientMode::parse_name(&self.utility).ok_or(
                SummersetError::msg(format!(
                    "utility mode '{}' unrecognized",
                    self.utility
                )),
            )?;
            let protocol = SmrProtocol::parse_name(&self.protocol).ok_or(
                SummersetError::msg(format!(
                    "protocol name '{}' unrecognized",
                    self.protocol
                )),
            )?;
            Ok((mode, protocol))
        }
    }
}

/// Actual main function of Summerset client executable.
fn client_main() -> Result<(), SummersetError> {
    // read in and parse command line arguments
    let mut args = CliArgs::parse();
    let (mode, protocol) = args.sanitize()?;

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

    // create tokio multi-threaded runtime
    let runtime = Builder::new_multi_thread()
        .enable_all()
        .worker_threads(args.threads)
        .thread_name("tokio-worker-client")
        .build()?;

    // enter tokio runtime, connect to the service, and do work
    runtime.block_on(async move {
        // NOTE: currently only supports <= 9 servers due to the hardcoded
        // binding ports
        let ctrl_bind =
            SocketAddr::new(args.bind_base.ip(), args.bind_base.port() + 9);
        let api_bind_base = args.bind_base;
        let endpoint = protocol
            .new_client_endpoint(
                ctrl_bind,
                api_bind_base,
                args.manager,
                config_str,
            )
            .await?;

        match mode {
            ClientMode::Repl => {
                // run interactive REPL loop
                let mut repl = ClientRepl::new(
                    endpoint,
                    Duration::from_millis(args.timeout_ms),
                );
                repl.run().await?;
            }
            ClientMode::Bench => {
                // run benchmarking client
                let mut bench = ClientBench::new(
                    endpoint,
                    Duration::from_millis(args.timeout_ms),
                    params_str,
                )?;
                bench.run().await?;
            }
            ClientMode::Tester => {
                // run correctness testing client
                let mut tester = ClientTester::new(
                    endpoint,
                    Duration::from_millis(args.timeout_ms),
                    params_str,
                )?;
                tester.run().await?;
            }
            ClientMode::Mess => {
                // run one-shot control client
                let mut mess = ClientMess::new(
                    endpoint,
                    Duration::from_millis(args.timeout_ms),
                    params_str,
                )?;
                mess.run().await?;
            }
        }

        Ok::<(), SummersetError>(()) // give type hint for this async closure
    })
}

/// Main function of Summerset client executable.
fn main() -> ExitCode {
    logger_init();

    if let Err(ref e) = client_main() {
        pf_error!("client_main exitted: {}", e);
        ExitCode::FAILURE
    } else {
        pf_warn!("client_main exitted successfully");
        ExitCode::SUCCESS
    }
}

#[cfg(test)]
mod arg_tests {
    use super::*;

    #[test]
    fn sanitize_valid() -> Result<(), SummersetError> {
        let args = CliArgs {
            protocol: "RepNothing".into(),
            utility: "repl".into(),
            bind_base: "127.0.0.1:42170".parse()?,
            manager: "127.0.0.1:40001".parse()?,
            threads: 2,
            timeout_ms: 5000,
            config: "".into(),
            params: "".into(),
        };
        assert_eq!(
            args.sanitize(),
            Ok((ClientMode::Repl, SmrProtocol::RepNothing))
        );
        Ok(())
    }

    #[test]
    fn sanitize_invalid_protocol() -> Result<(), SummersetError> {
        let args = CliArgs {
            protocol: "InvalidProtocol".into(),
            utility: "repl".into(),
            bind_base: "127.0.0.1:42170".parse()?,
            manager: "127.0.0.1:40001".parse()?,
            threads: 2,
            timeout_ms: 5000,
            config: "".into(),
            params: "".into(),
        };
        assert!(args.sanitize().is_err());
        Ok(())
    }

    #[test]
    fn sanitize_invalid_utility() -> Result<(), SummersetError> {
        let args = CliArgs {
            protocol: "RepNothing".into(),
            utility: "invalid_mode".into(),
            bind_base: "127.0.0.1:42170".parse()?,
            manager: "127.0.0.1:40001".parse()?,
            threads: 2,
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
            utility: "repl".into(),
            bind_base: "127.0.0.1:42170".parse()?,
            manager: "127.0.0.1:40001".parse()?,
            threads: 1,
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
            utility: "repl".into(),
            bind_base: "127.0.0.1:42170".parse()?,
            manager: "127.0.0.1:40001".parse()?,
            threads: 2,
            timeout_ms: 0,
            config: "".into(),
            params: "".into(),
        };
        assert!(args.sanitize().is_err());
        Ok(())
    }
}
