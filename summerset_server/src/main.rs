//! Summerset server node executable.

use std::net::SocketAddr;
use std::collections::HashSet;
use std::sync::Arc;

use tonic::transport;

use clap::Parser;

use tokio::runtime::{Runtime, Builder};
use tokio::task::JoinHandle;

use summerset::{
    SummersetServerNode, SummersetApiService, InternalCommService, SMRProtocol,
    InitError,
};

/// Server side structure wrapper.
#[derive(Debug)]
struct SummersetServer {
    /// Internal server node struct. This Arc will be cloned by both tonic
    /// services.
    node: Arc<SummersetServerNode>,

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

    /// Start the client key-value API service on `main_runtime`.
    fn spawn_api_service(
        &mut self,
        api_addr: SocketAddr,
        main_runtime: &Runtime,
    ) -> Result<JoinHandle<Result<(), transport::Error>>, InitError> {
        // Tonic service holder struct maintains an Arc reference to `node`.
        let api_service = SummersetApiService::new(self.node.clone())?;

        // TODO: tweak tonic Server configurations
        let router = api_service.build_tonic_router();

        let join_handle = main_runtime.spawn(router.serve(api_addr));
        self.api_spawned = true;
        Ok(join_handle)
    }

    /// Start the server internal communication service on `main_runtime`, if
    /// the protocol in use has internal communication protos.
    fn spawn_smr_service(
        &mut self,
        protocol: SMRProtocol,
        smr_addr: SocketAddr,
        main_runtime: &Runtime,
    ) -> Result<Option<JoinHandle<Result<(), transport::Error>>>, InitError>
    {
        // Tonic service holder struct maintains an Arc reference to `node`.
        let smr_service =
            InternalCommService::new(protocol, self.node.clone())?;

        // TODO: tweak tonic Server configurations
        let router = smr_service.build_tonic_router();

        // spawn if has internal communication protos
        if let Some(router) = router {
            let join_handle = main_runtime.spawn(router.serve(smr_addr));
            self.smr_spawned = true;
            Ok(Some(join_handle))
        } else {
            Ok(None)
        }
    }

    /// Establish connections to peers.
    fn connect_peers(&mut self) -> Result<(), InitError> {
        if !self.smr_spawned {
            Err(InitError(
                "error connecting to peers: smr_service not spawned yet".into(),
            ))
        } else if self.peers_connected {
            Err(InitError(
                "error connecting to peers: peers already connected".into(),
            ))
        } else {
            self.node.connect_peers()?;
            self.peers_connected = true;
            Ok(())
        }
    }
}

/// Command line arguments definition.
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct CLIArgs {
    /// Key-value API port open to clients.
    #[arg(short, long, default_value_t = 50077)]
    api_port: u16,

    /// Internal port used for SMR server-server RPCs.
    #[arg(short, long, default_value_t = 50078)]
    smr_port: u16,

    /// Name of SMR protocol to use.
    #[arg(short, long, default_value_t = String::from("DoNothing"))]
    protocol: String,

    /// List of peer server nodes (e.g., '-s host1:smr_port -s host2:smr_port').
    #[arg(short, long)]
    node_peers: Vec<String>,
}

impl CLIArgs {
    /// Sanitize command line arguments, return `Ok(protocol)` on success
    /// or `Err(InitError)` on any error.
    fn sanitize(&self) -> Result<SMRProtocol, InitError> {
        if self.api_port <= 1024 {
            Err(InitError(format!("api_port {} is invalid", self.api_port)))
        } else if self.smr_port <= 1024 {
            Err(InitError(format!("smr_port {} is invalid", self.smr_port)))
        } else if self.api_port == self.smr_port {
            Err(InitError(format!("api_port == smr_port {}", self.api_port)))
        } else {
            // check for duplicate peers
            let mut peer_set = HashSet::new();
            for s in self.node_peers.iter() {
                if peer_set.contains(s) {
                    return Err(InitError(format!(
                        "duplicate peer address {} given",
                        s
                    )));
                }
                peer_set.insert(s.clone());
            }

            SMRProtocol::parse_name(&self.protocol).ok_or_else(|| {
                InitError(format!(
                    "protocol name {} unrecognized",
                    self.protocol
                ))
            })
        }
    }
}

// Server node executable main entrance.
fn main() -> Result<(), InitError> {
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

    // create the main tokio runtime for starting tonic services
    let main_runtime = Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(|e| {
            InitError(format!("failed to build main mt_runtime: {}", e))
        })?;

    // create server node with given peers list
    let mut server = SummersetServer::new(protocol, args.node_peers)?;

    // add and start the server internal communication tonic service
    let smr_join_handle =
        server.spawn_smr_service(protocol, smr_addr, &main_runtime)?;
    if let Some(_) = smr_join_handle {
        println!("Starting internal communication service on {}...", smr_addr);

        // TODO: sleep for some time?

        // connect to server peers
        server.connect_peers()?;
    } else {
        println!("No internal communication service required by protocol...");
    }

    // add and start the client key-value API tonic service
    let api_join_handle = server.spawn_api_service(api_addr, &main_runtime)?;
    println!("Starting client key-value API service on {}...", api_addr);

    // both services should never return in normal execution; here, we block
    // on the client API service so that the server executable does not exit
    main_runtime
        .block_on(api_join_handle)
        .map_err(|e| {
            InitError(format!("client key-value API service panicked: {}", e))
        })?
        .map_err(|e| {
            InitError(format!("client key-value API service returned: {}", e))
        })?;

    Ok(())
}

#[cfg(test)]
mod server_tests {
    use super::{CLIArgs, SMRProtocol, InitError};

    #[test]
    fn sanitize_valid() {
        let args = CLIArgs {
            api_port: 50077,
            smr_port: 50078,
            protocol: "DoNothing".into(),
            node_peers: vec!["hostA:50078".into(), "hostB:50078".into()],
        };
        assert_eq!(args.sanitize(), Ok(SMRProtocol::DoNothing));
    }

    #[test]
    fn sanitize_invalid_api_port() {
        let args = CLIArgs {
            api_port: 1023,
            smr_port: 50078,
            protocol: "DoNothing".into(),
            node_peers: vec![],
        };
        assert_eq!(
            args.sanitize(),
            Err(InitError("api_port 1023 is invalid".into()))
        );
    }

    #[test]
    fn sanitize_invalid_smr_port() {
        let args = CLIArgs {
            api_port: 50077,
            smr_port: 1023,
            protocol: "DoNothing".into(),
            node_peers: vec![],
        };
        assert_eq!(
            args.sanitize(),
            Err(InitError("smr_port 1023 is invalid".into()))
        );
    }

    #[test]
    fn sanitize_same_api_smr_port() {
        let args = CLIArgs {
            api_port: 50077,
            smr_port: 50077,
            protocol: "DoNothing".into(),
            node_peers: vec![],
        };
        assert_eq!(
            args.sanitize(),
            Err(InitError("api_port == smr_port 50077".into()))
        );
    }

    #[test]
    fn sanitize_invalid_protocol() {
        let args = CLIArgs {
            api_port: 40077,
            smr_port: 50078,
            protocol: "InvalidProtocol".into(),
            node_peers: vec![],
        };
        assert_eq!(
            args.sanitize(),
            Err(InitError(
                "protocol name InvalidProtocol unrecognized".into()
            ))
        );
    }

    #[test]
    fn sanitize_duplicate_peer() {
        let args = CLIArgs {
            api_port: 50077,
            smr_port: 50078,
            protocol: "DoNothing".into(),
            node_peers: vec!["somehost:50078".into(), "somehost:50078".into()],
        };
        assert_eq!(
            args.sanitize(),
            Err(InitError(
                "duplicate peer address somehost:50078 given".into()
            ))
        );
    }
}
