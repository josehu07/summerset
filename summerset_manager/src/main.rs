//! Summerset cluster manager oracle.

use std::net::SocketAddr;
use std::process::ExitCode;

use clap::Parser;

use env_logger::Env;

use tokio::runtime::Builder;

use summerset::{SmrProtocol, SummersetError, pf_warn, pf_error};

/// Command line arguments definition.
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct CliArgs {
    /// Name of SMR protocol to use.
    #[arg(short, long, default_value_t = String::from("RepNothing"))]
    protocol: String,

    /// Server-facing API port.
    #[arg(short, long, default_value_t = 52600)]
    srv_port: u16,

    /// Client-facing API port.
    #[arg(short, long, default_value_t = 52601)]
    cli_port: u16,

    /// Total number of server replicas in cluster.
    #[arg(short = 'n', long, default_value_t = 3)]
    population: u8,

    /// Number of tokio worker threads.
    #[arg(long, default_value_t = 16)]
    threads: usize,
}

impl CliArgs {
    /// Sanitize command line arguments, return `Ok(protocol)` on success
    /// or `Err(SummersetError)` on any error.
    fn sanitize(&self) -> Result<SmrProtocol, SummersetError> {
        if self.srv_port <= 1024 {
            Err(SummersetError(format!(
                "invalid srv_port {}",
                self.srv_port
            )))
        } else if self.cli_port <= 1024 {
            Err(SummersetError(format!(
                "invalid cli_port {}",
                self.cli_port
            )))
        } else if self.srv_port == self.cli_port {
            Err(SummersetError(format!(
                "srv_port == cli_port {}",
                self.srv_port
            )))
        } else if self.population == 0 {
            Err(SummersetError(format!(
                "invalid population {}",
                self.population
            )))
        } else if self.threads < 2 {
            Err(SummersetError(format!(
                "invalid number of threads {}",
                self.threads
            )))
        } else {
            SmrProtocol::parse_name(&self.protocol).ok_or(SummersetError(
                format!("protocol name '{}' unrecognized", self.protocol),
            ))
        }
    }
}

// Cluster manager executable main entrance.
fn manager_main() -> Result<(), SummersetError> {
    // read in and parse command line arguments
    let args = CliArgs::parse();
    let protocol = args.sanitize()?;

    // parse server-facing API port
    let srv_addr: SocketAddr = format!("127.0.0.1:{}", args.srv_port)
        .parse()
        .map_err(|e| {
        SummersetError(format!(
            "failed to parse srv_addr: port {}: {}",
            args.srv_port, e
        ))
    })?;

    // parse client-facing API port
    let cli_addr: SocketAddr = format!("127.0.0.1:{}", args.cli_port)
        .parse()
        .map_err(|e| {
        SummersetError(format!(
            "failed to parse cli_addr: port {}: {}",
            args.cli_port, e
        ))
    })?;

    // create tokio multi-threaded runtime
    let runtime = Builder::new_multi_thread()
        .enable_all()
        .worker_threads(args.threads)
        .thread_name("tokio-worker-manager")
        .build()?;

    // enter tokio runtime, setup the cluster manager, and start the main
    // event loop logic
    runtime.block_on(async move {
        let mut manager = protocol
            .new_cluster_manager_setup(srv_addr, cli_addr, args.population)
            .await?;

        manager.run().await?;

        Ok::<(), SummersetError>(()) // give type hint for this async closure
    })
}

fn main() -> ExitCode {
    env_logger::Builder::from_env(Env::default().default_filter_or("info"))
        .format_timestamp(None)
        .format_module_path(true)
        .format_target(false)
        .init();

    if let Err(ref e) = manager_main() {
        pf_error!("m"; "manager_main exitted: {}", e);
        ExitCode::FAILURE
    } else {
        pf_warn!("m"; "manager_main exitted successfully");
        ExitCode::SUCCESS
    }
}

#[cfg(test)]
mod manager_args_tests {
    use super::*;

    #[test]
    fn sanitize_valid() -> Result<(), SummersetError> {
        let args = CliArgs {
            protocol: "RepNothing".into(),
            srv_port: 52600,
            cli_port: 52601,
            population: 3,
            threads: 2,
        };
        assert_eq!(args.sanitize(), Ok(SmrProtocol::RepNothing));
        Ok(())
    }

    #[test]
    fn sanitize_invalid_srv_port() -> Result<(), SummersetError> {
        let args = CliArgs {
            protocol: "RepNothing".into(),
            srv_port: 1023,
            cli_port: 52601,
            population: 3,
            threads: 2,
        };
        assert!(args.sanitize().is_err());
        Ok(())
    }

    #[test]
    fn sanitize_invalid_cli_port() -> Result<(), SummersetError> {
        let args = CliArgs {
            protocol: "RepNothing".into(),
            srv_port: 52600,
            cli_port: 1023,
            population: 3,
            threads: 2,
        };
        assert!(args.sanitize().is_err());
        Ok(())
    }

    #[test]
    fn sanitize_same_srv_cli_port() -> Result<(), SummersetError> {
        let args = CliArgs {
            protocol: "RepNothing".into(),
            srv_port: 52600,
            cli_port: 52600,
            population: 3,
            threads: 2,
        };
        assert!(args.sanitize().is_err());
        Ok(())
    }

    #[test]
    fn sanitize_invalid_protocol() -> Result<(), SummersetError> {
        let args = CliArgs {
            protocol: "InvalidProtocol".into(),
            srv_port: 52600,
            cli_port: 52601,
            population: 3,
            threads: 2,
        };
        assert!(args.sanitize().is_err());
        Ok(())
    }

    #[test]
    fn sanitize_invalid_population() -> Result<(), SummersetError> {
        let args = CliArgs {
            protocol: "RepNothing".into(),
            srv_port: 52600,
            cli_port: 52601,
            population: 0,
            threads: 2,
        };
        assert!(args.sanitize().is_err());
        Ok(())
    }

    #[test]
    fn sanitize_invalid_threads() -> Result<(), SummersetError> {
        let args = CliArgs {
            protocol: "RepNothing".into(),
            srv_port: 52600,
            cli_port: 52601,
            population: 3,
            threads: 1,
        };
        assert!(args.sanitize().is_err());
        Ok(())
    }
}
