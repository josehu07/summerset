//! Summerset server node executable.

use std::net::SocketAddr;
use std::collections::HashSet;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use clap::Parser;

use log::info;

use env_logger::Env;

use summerset::{SMRProtocol, GenericReplica, ReplicaId, SummersetError};

/// Server side structure wrapper.
struct SummersetServer {
    /// Internal server node trait object.
    node: Box<dyn GenericReplica>,

    /// State flags.
    api_spawned: bool,
    smr_spawned: bool,
    peers_connected: bool,
}

impl SummersetServer {
    /// Create a new Summerset server structure.
    fn new(
        protocol: SMRProtocol,
        peers: Vec<String>,
    ) -> Result<Self, InitError> {
        let node = Arc::new(SummersetServerNode::new(protocol, peers)?);

        Ok(SummersetServer {
            node,
            api_spawned: false,
            smr_spawned: false,
            peers_connected: false,
        })
    }

    /// Establish connections to peers.
    async fn connect_peers(&mut self) -> Result<(), InitError> {
        if !self.smr_spawned {
            Err(InitError(
                "error connecting to peers: smr_service not spawned yet".into(),
            ))
        } else if self.peers_connected {
            Err(InitError(
                "error connecting to peers: peers already connected".into(),
            ))
        } else {
            self.node.connect_peers().await?;
            self.peers_connected = true;
            Ok(())
        }
    }
}

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
            replicas_set.insert(addr.clone());
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
// TODO: tweak tokio runtime configurations
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
    let smr_addr = format!("[::1]:{}", args.smr_port).parse().map_err(|e| {
        InitError(format!(
            "failed to parse server internal communication addr: port {}: {}",
            args.smr_port, e
        ))
    })?;

    // parse key-value API port
    let api_addr = format!("[::1]:{}", args.api_port).parse().map_err(|e| {
        InitError(format!(
            "failed to parse key-value API serving addr: port {}: {}",
            args.api_port, e
        ))
    })?;

    // create server node with given peers list
    let mut server = SummersetServer::new(protocol, args.node_peers)?;

    // add and start the server internal communication tonic service
    let smr_join_handle = server.spawn_smr_service(protocol, smr_addr)?;
    if smr_join_handle.is_some() {
        info!("Starting internal communication service on {}...", smr_addr);

        // retry until connected to server peers
        // TODO: better peers initialization logic
        info!("Connecting to server peers...");
        let mut retry_cnt = 0;
        while server.connect_peers().await.is_err() {
            thread::sleep(Duration::from_secs(1));
            retry_cnt += 1;
            info!("  retry attempt {}", retry_cnt);
        }
    } else {
        info!("No internal communication service required by protocol...");
    }

    // add and start the client key-value API tonic service
    let api_join_handle = server.spawn_api_service(api_addr)?;
    info!("Starting client key-value API service on {}...", api_addr);

    // both services should never return in normal execution
    if let Some(smr_join_handle) = smr_join_handle {
        let jres = tokio::try_join!(smr_join_handle, api_join_handle).map_err(
            |e| InitError(format!("join error from tonic service: {}", e)),
        )?;
        jres.0.map_err(|e| {
            InitError(format!(
                "internal communication service failed with transport error: {}",
                e
            ))
        })?;
        jres.1.map_err(|e| {
            InitError(format!(
                "client key-value API service failed with transport error: {}",
                e
            ))
        })?;
    } else {
        let jres = tokio::try_join!(api_join_handle).map_err(|e| {
            InitError(format!("join error from tonic service: {}", e))
        })?;
        jres.0.map_err(|e| {
            InitError(format!(
                "client key-value API service failed with transport error: {}",
                e
            ))
        })?;
    }

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
