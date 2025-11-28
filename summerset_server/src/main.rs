//! Summerset server replica executable.

use std::net::{Ipv4Addr, SocketAddr};
use std::process::ExitCode;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use clap::Parser;
use log::{self, LevelFilter};
use summerset::{logger_init, pf_error, SmrProtocol, SummersetError};
use tokio::runtime::Builder;
use tokio::sync::watch;

/// Command line arguments definition.
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct CliArgs {
    /// Name of SMR protocol to use.
    #[arg(short, long)]
    protocol: String,

    /// Protocol-specific server configuration TOML string.
    /// Every '+' is treated as newline.
    #[arg(long, default_value_t = String::from(""))]
    config: String,

    /// Local IP to use for binding the listening sockets.
    #[arg(short, long, default_value_t = Ipv4Addr::UNSPECIFIED)]
    bind_ip: Ipv4Addr,

    /// Key-value API port open to clients.
    /// This port must be available at process launch.
    #[arg(short, long, default_value_t = 52700)]
    api_port: u16,

    /// Internal peer-peer communication API port.
    /// This port must be available at process launch.
    #[arg(short = 'i', long, default_value_t = 52800)]
    p2p_port: u16,

    /// Cluster manager oracle's server-facing address.
    #[arg(short, long)]
    manager: SocketAddr,

    /// Number of tokio worker threads.
    #[arg(long, default_value_t = 16)]
    threads: usize,
}

impl CliArgs {
    /// Sanitize command line arguments, return `Ok(protocol)` on success
    /// or `Err(SummersetError)` on any error.
    fn sanitize(&self) -> Result<SmrProtocol, SummersetError> {
        if self.api_port <= 1024 {
            Err(SummersetError::msg(format!(
                "invalid api_port {}",
                self.api_port
            )))
        } else if self.p2p_port <= 1024 {
            Err(SummersetError::msg(format!(
                "invalid p2p_port {}",
                self.p2p_port
            )))
        } else if self.api_port == self.p2p_port {
            Err(SummersetError::msg(format!(
                "api_port == p2p_port {}",
                self.api_port
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

/// Actual main function of Summerset server executable.
fn server_main() -> Result<(), SummersetError> {
    // read in and parse command line arguments
    let mut args = CliArgs::parse();
    let protocol = args.sanitize()?;

    // parse key-value API port
    let api_addr: SocketAddr = format!("{}:{}", args.bind_ip, args.api_port)
        .parse()
        .map_err(|e| {
            SummersetError::msg(format!(
                "failed to parse api_addr: bind_ip {} port {}: {}",
                args.bind_ip, args.api_port, e
            ))
        })?;

    // parse internal peer-peer API port
    let p2p_addr: SocketAddr = format!("{}:{}", args.bind_ip, args.p2p_port)
        .parse()
        .map_err(|e| {
            SummersetError::msg(format!(
                "failed to parse p2p_addr: bind_ip {} port {}: {}",
                args.bind_ip, args.p2p_port, e
            ))
        })?;

    // parse optional config string if given
    let config_str = if args.config.is_empty() {
        None
    } else {
        args.config = args.config.replace('+', "\n");
        Some(&args.config[..])
    };

    // set up termination signals handler
    let (tx_term, rx_term) = watch::channel(false);
    ctrlc::set_handler(move || {
        if let Err(e) = tx_term.send(true) {
            pf_error!("error sending to term channel: {}", e);
        }
    })?;

    // using a while loop here to allow software-simulated crash-restart
    let log_level = log::max_level();
    let shutdown = Arc::new(AtomicBool::new(false));
    while !shutdown.load(Ordering::SeqCst) {
        log::set_max_level(log_level);
        let shutdown_clone = shutdown.clone();
        let rx_term_clone = rx_term.clone();

        // create tokio multi-threaded runtime
        let runtime = Builder::new_multi_thread()
            .enable_all()
            .worker_threads(args.threads)
            .thread_name("tokio-worker-replica")
            .build()?;

        // enter tokio runtime, setup the server replica, and start the main
        // event loop logic
        runtime.block_on(async move {
            let mut replica = protocol
                .new_server_replica_setup(
                    api_addr,
                    p2p_addr,
                    args.manager,
                    config_str,
                )
                .await?;

            if replica.run(rx_term_clone).await? {
                // event loop terminated but wants to restart (e.g., when
                // receiving a reset control message); just drop this runtime
                // and move to the next iteration of loop
            } else {
                // event loop terminated and does not want to restart (e.g.,
                // when receiving a termination signal)
                shutdown_clone.store(true, Ordering::SeqCst);
            }

            // suppress logging before dropping the runtime to avoid spurious
            // error messages
            log::set_max_level(LevelFilter::Off);

            Ok::<(), SummersetError>(()) // give type hint for this async closure
        })?;
    }

    log::set_max_level(log_level);
    Ok(())
}

/// Main function of Summerset server executable.
fn main() -> ExitCode {
    logger_init();

    if let Err(ref e) = server_main() {
        pf_error!("server_main exited: {}", e);
        ExitCode::FAILURE
    } else {
        // pf_warn!("server_main exited successfully");
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
            api_port: 40103,
            p2p_port: 40203,
            manager: "127.0.0.1:40000".parse()?,
            threads: 2,
            config: "".into(),
        };
        assert_eq!(args.sanitize(), Ok(SmrProtocol::RepNothing));
        Ok(())
    }

    #[test]
    fn sanitize_invalid_api_port() -> Result<(), SummersetError> {
        let args = CliArgs {
            protocol: "RepNothing".into(),
            bind_ip: "127.0.0.1".parse()?,
            api_port: 1023,
            p2p_port: 40200,
            manager: "127.0.0.1:40000".parse()?,
            threads: 2,
            config: "".into(),
        };
        assert!(args.sanitize().is_err());
        Ok(())
    }

    #[test]
    fn sanitize_invalid_p2p_port() -> Result<(), SummersetError> {
        let args = CliArgs {
            protocol: "RepNothing".into(),
            bind_ip: "127.0.0.1".parse()?,
            api_port: 40100,
            p2p_port: 1023,
            manager: "127.0.0.1:40000".parse()?,
            threads: 2,
            config: "".into(),
        };
        assert!(args.sanitize().is_err());
        Ok(())
    }

    #[test]
    fn sanitize_same_api_p2p_port() -> Result<(), SummersetError> {
        let args = CliArgs {
            protocol: "RepNothing".into(),
            bind_ip: "127.0.0.1".parse()?,
            api_port: 40100,
            p2p_port: 40100,
            manager: "127.0.0.1:40000".parse()?,
            threads: 2,
            config: "".into(),
        };
        assert!(args.sanitize().is_err());
        Ok(())
    }

    #[test]
    fn sanitize_invalid_protocol() -> Result<(), SummersetError> {
        let args = CliArgs {
            protocol: "InvalidProtocol".into(),
            bind_ip: "127.0.0.1".parse()?,
            api_port: 40100,
            p2p_port: 40200,
            manager: "127.0.0.1:40000".parse()?,
            threads: 2,
            config: "".into(),
        };
        assert!(args.sanitize().is_err());
        Ok(())
    }

    #[test]
    fn sanitize_invalid_threads() -> Result<(), SummersetError> {
        let args = CliArgs {
            protocol: "RepNothing".into(),
            bind_ip: "127.0.0.1".parse()?,
            api_port: 40100,
            p2p_port: 40200,
            manager: "127.0.0.1:40000".parse()?,
            threads: 1,
            config: "".into(),
        };
        assert!(args.sanitize().is_err());
        Ok(())
    }
}
