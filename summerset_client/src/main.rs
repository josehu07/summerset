//! Summerset client side executable.

use std::collections::{HashSet, HashMap};
use std::net::SocketAddr;

use clap::Parser;

use env_logger::Env;

use tokio::runtime::Builder;

use summerset::{
    SMRProtocol, ClientId, ReplicaId, SummersetError, pf_error, pf_info,
};

mod client_cl;
use client_cl::ClientClosedLoop;

/// Command line arguments definition.
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct CliArgs {
    /// Name of SMR protocol to use.
    #[arg(short, long, default_value_t = String::from("RepNothing"))]
    protocol: String,

    /// Client ID.
    #[arg(short, long, default_value_t = 2857)]
    id: ClientId,

    /// List of server replica nodes, the order of which maps to replica IDs.
    /// Example: '-r host1:smr_port1 -r host2:smr_port2 -r host3:smr_port3'.
    #[arg(short, long)]
    replicas: Vec<SocketAddr>,

    /// Protocol-specific client configuration TOML string.
    /// Every '+' is treated as newline.
    #[arg(long, default_value_t = String::from(""))]
    config: String,

    /// Number of tokio worker threads.
    #[arg(long, default_value_t = 1)]
    threads: usize,
}

impl CliArgs {
    /// Sanitize command line arguments, return `Ok(protocol)` on success
    /// or `Err(SummersetError)` on any error.
    fn sanitize(&self) -> Result<SMRProtocol, SummersetError> {
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
        } else {
            SMRProtocol::parse_name(&self.protocol).ok_or_else(|| {
                SummersetError(format!(
                    "protocol name '{}' unrecognized",
                    self.protocol
                ))
            })
        }
    }
}

// Client side executable main entrance.
fn client_main() -> Result<(), SummersetError> {
    // read in and parse command line arguments
    let mut args = CliArgs::parse();
    let protocol = args.sanitize()?;
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
        let (send_stub, recv_stub) = stub.connect().await?;

        let mut client = ClientClosedLoop::new(args.id, send_stub, recv_stub);
        pf_info!(args.id; "{:?}", client.get("Jose").await?);
        pf_info!(args.id; "{:?}", client.put("Jose", "123").await?);
        pf_info!(args.id; "{:?}", client.get("Jose").await?);
        pf_info!(args.id; "{:?}", client.put("Jose", "456").await?);
        pf_info!(args.id; "{:?}", client.get("Jose").await?);
        client.leave().await?;

        Ok::<(), SummersetError>(()) // give type hint for this async closure
    })
}

fn main() {
    env_logger::Builder::from_env(Env::default().default_filter_or("info"))
        .format_timestamp(None)
        .format_module_path(true)
        .format_target(false)
        .init();

    if let Err(e) = client_main() {
        pf_error!("client"; "client_main exitted: {}", e);
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
            threads: 1,
            config: "".into(),
        };
        assert_eq!(args.sanitize(), Ok(SMRProtocol::RepNothing));
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
            threads: 1,
            config: "".into(),
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
            threads: 1,
            config: "".into(),
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
            threads: 1,
            config: "".into(),
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
            threads: 0,
            config: "".into(),
        };
        assert!(args.sanitize().is_err());
        Ok(())
    }
}
