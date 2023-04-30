//! Summerset server node executable.

use std::collections::{HashSet, HashMap};
use std::net::SocketAddr;

use clap::Parser;

use env_logger::Env;

use tokio::runtime::Builder;

use summerset::{SMRProtocol, ReplicaId, SummersetError, pf_error};

/// Command line arguments definition.
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct CliArgs {
    /// Name of SMR protocol to use.
    #[arg(short, long, default_value_t = String::from("RepNothing"))]
    protocol: String,

    /// Key-value API port open to clients.
    #[arg(short, long, default_value_t = 52700)]
    api_port: u16,

    /// Internal port used for SMR server-server RPCs.
    #[arg(short, long, default_value_t = 52800)]
    smr_port: u16,

    /// Number of tokio worker threads.
    #[arg(long, default_value_t = 2)]
    threads: usize,

    /// Replica ID of myself.
    #[arg(short, long)]
    id: ReplicaId,

    /// List of server replica nodes, the order of which maps to replica IDs.
    /// Example: '-r host1:smr_port1 -r host2:smr_port2 -r host3:smr_port3'.
    #[arg(short, long)]
    replicas: Vec<SocketAddr>,
}

impl CliArgs {
    /// Sanitize command line arguments, return `Ok(protocol)` on success
    /// or `Err(SummersetError)` on any error.
    fn sanitize(&self) -> Result<SMRProtocol, SummersetError> {
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

        if (self.id as usize) >= self.replicas.len() {
            return Err(SummersetError(format!(
                "invalid replica ID {} / {}",
                self.id,
                self.replicas.len()
            )));
        }
        let my_addr = self.replicas[self.id as usize];

        if self.api_port <= 1024 {
            Err(SummersetError(format!(
                "invalid api_port {}",
                self.api_port
            )))
        } else if self.smr_port <= 1024 {
            Err(SummersetError(format!(
                "invalid smr_port {}",
                self.smr_port
            )))
        } else if self.api_port == self.smr_port {
            Err(SummersetError(format!(
                "api_port == smr_port {}",
                self.api_port
            )))
        } else if self.smr_port != my_addr.port() {
            Err(SummersetError(format!(
                "smr_port {} does not match replica addr '{}'",
                self.smr_port, my_addr
            )))
        } else if self.threads < 2 {
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

// Server node executable main entrance.
fn server_main() -> Result<(), SummersetError> {
    // read in and parse command line arguments
    let args = CliArgs::parse();
    let protocol = args.sanitize()?;
    let mut peer_addrs = HashMap::new();
    for (id, &addr) in args.replicas.iter().enumerate() {
        let id = id as ReplicaId;
        if id != args.id {
            peer_addrs.insert(id, addr);
        }
    }

    // parse internal communication port
    let smr_addr: SocketAddr = format!("127.0.0.1:{}", args.smr_port)
        .parse()
        .map_err(|e| {
        SummersetError(format!(
            "failed to parse smr_addr: port {}: {}",
            args.smr_port, e
        ))
    })?;

    // parse key-value API port
    let api_addr: SocketAddr = format!("127.0.0.1:{}", args.api_port)
        .parse()
        .map_err(|e| {
        SummersetError(format!(
            "failed to parse api_addr: port {}: {}",
            args.api_port, e
        ))
    })?;

    // create server node with given configuration
    // TODO: protocol-specific configuration string
    let mut node = protocol.new_server_node(
        args.id,
        args.replicas.len() as u8,
        smr_addr,
        api_addr,
        peer_addrs,
        None,
    )?;

    // create tokio multi-threaded runtime
    let runtime = Builder::new_multi_thread()
        .enable_all()
        .worker_threads(args.threads)
        .thread_name(format!("tokio-worker-replica{}", args.id))
        .build()?;

    // enter tokio runtime, setup the server node, and start the main event
    // loop logic
    runtime.block_on(async move {
        node.setup().await?;

        node.run().await;

        Ok::<(), SummersetError>(()) // give type hint for this async closure
    })
}

fn main() {
    env_logger::Builder::from_env(Env::default().default_filter_or("info"))
        .format_timestamp(None)
        .format_module_path(true)
        .format_target(false)
        .init();

    if let Err(e) = server_main() {
        pf_error!("server"; "server_main exitted: {}", e);
    }
}

#[cfg(test)]
mod server_args_tests {
    use super::*;

    #[test]
    fn sanitize_valid() -> Result<(), SummersetError> {
        let args = CliArgs {
            api_port: 52701,
            smr_port: 52801,
            protocol: "RepNothing".into(),
            replicas: vec![
                "127.0.0.1:52800".parse()?,
                "127.0.0.1:52801".parse()?,
            ],
            id: 1,
            threads: 2,
        };
        assert_eq!(args.sanitize(), Ok(SMRProtocol::RepNothing));
        Ok(())
    }

    #[test]
    fn sanitize_invalid_api_port() -> Result<(), SummersetError> {
        let args = CliArgs {
            api_port: 1023,
            smr_port: 52800,
            protocol: "RepNothing".into(),
            replicas: vec!["127.0.0.1:52800".parse()?],
            id: 0,
            threads: 2,
        };
        assert!(args.sanitize().is_err());
        Ok(())
    }

    #[test]
    fn sanitize_invalid_smr_port() -> Result<(), SummersetError> {
        let args = CliArgs {
            api_port: 52700,
            smr_port: 1023,
            protocol: "RepNothing".into(),
            replicas: vec!["127.0.0.1:52800".parse()?],
            id: 0,
            threads: 2,
        };
        assert!(args.sanitize().is_err());
        Ok(())
    }

    #[test]
    fn sanitize_same_api_smr_port() -> Result<(), SummersetError> {
        let args = CliArgs {
            api_port: 52800,
            smr_port: 52800,
            protocol: "RepNothing".into(),
            replicas: vec!["127.0.0.1:52800".parse()?],
            id: 0,
            threads: 2,
        };
        assert!(args.sanitize().is_err());
        Ok(())
    }

    #[test]
    fn sanitize_smr_port_mismatch() -> Result<(), SummersetError> {
        let args = CliArgs {
            api_port: 52700,
            smr_port: 52900,
            protocol: "RepNothing".into(),
            replicas: vec!["127.0.0.1:52800".parse()?],
            id: 0,
            threads: 2,
        };
        assert!(args.sanitize().is_err());
        Ok(())
    }

    #[test]
    fn sanitize_invalid_protocol() -> Result<(), SummersetError> {
        let args = CliArgs {
            api_port: 52700,
            smr_port: 52800,
            protocol: "InvalidProtocol".into(),
            replicas: vec!["127.0.0.1:52800".parse()?],
            id: 0,
            threads: 2,
        };
        assert!(args.sanitize().is_err());
        Ok(())
    }

    #[test]
    fn sanitize_duplicate_replica() -> Result<(), SummersetError> {
        let args = CliArgs {
            api_port: 52700,
            smr_port: 52800,
            protocol: "RepNothing".into(),
            replicas: vec![
                "127.0.0.1:52800".parse()?,
                "127.0.0.1:52800".parse()?,
            ],
            id: 0,
            threads: 2,
        };
        assert!(args.sanitize().is_err());
        Ok(())
    }

    #[test]
    fn sanitize_invalid_id() -> Result<(), SummersetError> {
        let args = CliArgs {
            api_port: 52700,
            smr_port: 52802,
            protocol: "RepNothing".into(),
            replicas: vec![
                "127.0.0.1:52800".parse()?,
                "127.0.0.1:52801".parse()?,
            ],
            id: 2,
            threads: 2,
        };
        assert!(args.sanitize().is_err());
        Ok(())
    }

    #[test]
    fn sanitize_invalid_threads() -> Result<(), SummersetError> {
        let args = CliArgs {
            api_port: 52700,
            smr_port: 52800,
            protocol: "RepNothing".into(),
            replicas: vec![
                "127.0.0.1:52800".parse()?,
                "127.0.0.1:52801".parse()?,
            ],
            id: 1,
            threads: 1,
        };
        assert!(args.sanitize().is_err());
        Ok(())
    }
}
