//! Summerset cluster manager oracle.

use std::net::{Ipv4Addr, SocketAddr};
use std::process::ExitCode;

use clap::Parser;
use log::{self, LevelFilter};
use summerset::{SmrProtocol, SummersetError, logger_init, pf_error};
use tokio::runtime::Builder;
use tokio::sync::watch;

/// Command line arguments definition.
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct CliArgs {
    /// Name of SMR protocol to use.
    #[arg(short, long)]
    protocol: String,

    /// Local IP to use for binding the listening sockets.
    #[arg(short, long, default_value_t = Ipv4Addr::UNSPECIFIED)]
    bind_ip: Ipv4Addr,

    /// Client-facing API port.
    /// This port must be available at process launch.
    #[arg(short, long, default_value_t = 52601)]
    cli_port: u16,

    /// Server-facing API port.
    /// This port must be available at process launch.
    #[arg(short, long, default_value_t = 52600)]
    srv_port: u16,

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
            Err(SummersetError::msg(format!(
                "invalid srv_port {}",
                self.srv_port
            )))
        } else if self.cli_port <= 1024 {
            Err(SummersetError::msg(format!(
                "invalid cli_port {}",
                self.cli_port
            )))
        } else if self.srv_port == self.cli_port {
            Err(SummersetError::msg(format!(
                "srv_port == cli_port {}",
                self.srv_port
            )))
        } else if self.population == 0 {
            Err(SummersetError::msg(format!(
                "invalid population {}",
                self.population
            )))
        } else if self.threads < 2 {
            Err(SummersetError::msg(format!(
                "invalid number of threads {}",
                self.threads
            )))
        } else {
            SmrProtocol::parse_name(&self.protocol).ok_or(SummersetError::msg(
                format!("protocol name '{}' unrecognized", self.protocol),
            ))
        }
    }
}

/// Actual main function of Summerset manager oracle.
fn manager_main() -> Result<(), SummersetError> {
    // read in and parse command line arguments
    let args = CliArgs::parse();
    let protocol = args.sanitize()?;

    // parse server-facing API port
    let srv_addr: SocketAddr = format!("{}:{}", args.bind_ip, args.srv_port)
        .parse()
        .map_err(|e| {
            SummersetError::msg(format!(
                "failed to parse srv_addr: bind_ip {} port {}: {}",
                args.bind_ip, args.srv_port, e
            ))
        })?;

    // parse client-facing API port
    let cli_addr: SocketAddr = format!("{}:{}", args.bind_ip, args.cli_port)
        .parse()
        .map_err(|e| {
            SummersetError::msg(format!(
                "failed to parse cli_addr: bind_ip {} port {}: {}",
                args.bind_ip, args.cli_port, e
            ))
        })?;

    // set up termination signals handler
    let (tx_term, rx_term) = watch::channel(false);
    ctrlc::set_handler(move || {
        if let Err(e) = tx_term.send(true) {
            pf_error!("error sending to term channel: {}", e);
        }
    })?;

    let log_level = log::max_level();
    {
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

            manager.run(rx_term).await?;

            // suppress logging before dropping the runtime to avoid spurious
            // error messages
            log::set_max_level(LevelFilter::Off);

            Ok::<(), SummersetError>(()) // give type hint for this async closure
        })?;
    } // drop the runtime here

    log::set_max_level(log_level);
    Ok(())
}

/// Main function of Summerset manager oracle.
fn main() -> ExitCode {
    logger_init();

    if let Err(ref e) = manager_main() {
        pf_error!("manager_main exited: {}", e);
        ExitCode::FAILURE
    } else {
        // pf_warn!("manager_main exited successfully");
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
            bind_ip: "127.0.0.1".parse()?,
            srv_port: 40000,
            cli_port: 40001,
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
            bind_ip: "127.0.0.1".parse()?,
            srv_port: 1023,
            cli_port: 40001,
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
            bind_ip: "127.0.0.1".parse()?,
            srv_port: 40000,
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
            bind_ip: "127.0.0.1".parse()?,
            srv_port: 40000,
            cli_port: 40000,
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
            bind_ip: "127.0.0.1".parse()?,
            srv_port: 40000,
            cli_port: 40001,
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
            bind_ip: "127.0.0.1".parse()?,
            srv_port: 40000,
            cli_port: 40001,
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
            bind_ip: "127.0.0.1".parse()?,
            srv_port: 40000,
            cli_port: 40001,
            population: 3,
            threads: 1,
        };
        assert!(args.sanitize().is_err());
        Ok(())
    }
}
