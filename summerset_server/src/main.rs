//! Summerset server node executable.

use std::net::SocketAddr;
use std::collections::HashSet;

use clap::Parser;

use env_logger::Env;

use tokio::runtime::Builder;

use summerset::{SMRProtocol, ReplicaId, SummersetError};

/// Command line arguments definition.
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct CLIArgs {
    /// Name of SMR protocol to use.
    #[arg(short, long, default_value_t = String::from("RepNothing"))]
    protocol: String,

    /// Key-value API port open to clients.
    #[arg(short, long, default_value_t = 52700)]
    api_port: u16,

    /// Internal port used for SMR server-server RPCs.
    #[arg(short, long, default_value_t = 52800)]
    smr_port: u16,

    /// List of peer server nodes, the order of which maps to replica IDs.
    /// Example: '-n host1:smr_port1 -n host2:smr_port2 -n host3:smr_port3'.
    #[arg(short, long)]
    replicas: Vec<SocketAddr>,

    /// Replica ID of myself.
    #[arg(short, long)]
    id: ReplicaId,
}

impl CLIArgs {
    /// Sanitize command line arguments, return `Ok(protocol)` on success
    /// or `Err(SummersetError)` on any error.
    fn sanitize(&self) -> Result<SMRProtocol, SummersetError> {
        // check for duplicate peers
        let mut replicas_set = HashSet::new();
        for addr in self.replicas.iter() {
            if replicas_set.contains(addr) {
                return Err(SummersetError(format!(
                    "duplicate replica address {} given",
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
fn main() -> Result<(), SummersetError> {
    // initialize env_logger
    env_logger::Builder::from_env(Env::default().default_filter_or("info"))
        .format_timestamp(None)
        .format_module_path(true)
        .format_target(false)
        .init();

    // read in and parse command line arguments
    let args = CLIArgs::parse();
    let protocol = args.sanitize()?;

    // parse internal communication port
    let smr_addr: SocketAddr = format!("localhost:{}", args.smr_port)
        .parse()
        .map_err(|e| {
        SummersetError(format!(
            "failed to parse smr_addr: port {}: {}",
            args.smr_port, e
        ))
    })?;

    // parse key-value API port
    let api_addr: SocketAddr = format!("localhost:{}", args.api_port)
        .parse()
        .map_err(|e| {
        SummersetError(format!(
            "failed to parse api_addr: port {}: {}",
            args.api_port, e
        ))
    })?;

    // create server node with given configuration
    // TODO: protocol-specific configuration string
    let node = protocol.new_server_node(
        args.id,
        args.replicas.len() as u8,
        smr_addr,
        api_addr,
        None,
    )?;

    // create tokio multi-threaded runtime
    // TODO: tweak number of threads
    let runtime = Builder::new_multi_thread()
        .enable_all()
        .worker_threads(2)
        .thread_name(format!("tokio-worker-replica{}", args.id))
        .build()?;

    // TODO: setup.await and run.await
    runtime.block_on(async move {});

    Ok(())
}

#[cfg(test)]
mod server_args_tests {
    use super::*;

    #[test]
    fn sanitize_valid() -> Result<(), SummersetError> {
        let args = CLIArgs {
            api_port: 52700,
            smr_port: 52800,
            protocol: "RepNothing".into(),
            replicas: vec!["localhost:52800".parse()?, "hostB:52801".parse()?],
            id: 1,
        };
        assert_eq!(args.sanitize(), Ok(SMRProtocol::RepNothing));
        Ok(())
    }

    #[test]
    fn sanitize_invalid_api_port() -> Result<(), SummersetError> {
        let args = CLIArgs {
            api_port: 1023,
            smr_port: 52800,
            protocol: "RepNothing".into(),
            replicas: vec!["localhost:52800".parse()?],
            id: 0,
        };
        assert!(args.sanitize().is_err());
        Ok(())
    }

    #[test]
    fn sanitize_invalid_smr_port() -> Result<(), SummersetError> {
        let args = CLIArgs {
            api_port: 52700,
            smr_port: 1023,
            protocol: "RepNothing".into(),
            replicas: vec!["localhost:52800".parse()?],
            id: 0,
        };
        assert!(args.sanitize().is_err());
        Ok(())
    }

    #[test]
    fn sanitize_same_api_smr_port() -> Result<(), SummersetError> {
        let args = CLIArgs {
            api_port: 52800,
            smr_port: 52800,
            protocol: "RepNothing".into(),
            replicas: vec!["localhost:52800".parse()?],
            id: 0,
        };
        assert!(args.sanitize().is_err());
        Ok(())
    }

    #[test]
    fn sanitize_smr_port_mismatch() -> Result<(), SummersetError> {
        let args = CLIArgs {
            api_port: 52700,
            smr_port: 52900,
            protocol: "RepNothing".into(),
            replicas: vec!["localhost:52800".parse()?],
            id: 0,
        };
        assert!(args.sanitize().is_err());
        Ok(())
    }

    #[test]
    fn sanitize_invalid_protocol() -> Result<(), SummersetError> {
        let args = CLIArgs {
            api_port: 52700,
            smr_port: 52800,
            protocol: "InvalidProtocol".into(),
            replicas: vec!["localhost:52800".parse()?],
            id: 0,
        };
        assert!(args.sanitize().is_err());
        Ok(())
    }

    #[test]
    fn sanitize_duplicate_replica() -> Result<(), SummersetError> {
        let args = CLIArgs {
            api_port: 52700,
            smr_port: 52800,
            protocol: "RepNothing".into(),
            replicas: vec![
                "localhost:52800".parse()?,
                "localhost:52800".parse()?,
            ],
            id: 0,
        };
        assert!(args.sanitize().is_err());
        Ok(())
    }

    #[test]
    fn sanitize_invalid_id() -> Result<(), SummersetError> {
        let args = CLIArgs {
            api_port: 52700,
            smr_port: 52802,
            protocol: "RepNothing".into(),
            replicas: vec![
                "localhost:52800".parse()?,
                "localhost:52801".parse()?,
            ],
            id: 2,
        };
        assert!(args.sanitize().is_err());
        Ok(())
    }
}
