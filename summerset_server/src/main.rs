//! Summerset server node executable.

use tonic::transport::Server;
use summerset::external_api_proto::external_api_server::ExternalApiServer;

use summerset::{SummersetServerNode, SMRProtocol, InitError};

use clap::Parser;
use std::collections::HashSet;
use tokio::runtime::Builder;

/// Server side structure wrapper.
#[derive(Debug)]
struct SummersetServer {
    /// Internal server node struct.
    node: SummersetServerNode,
}

impl SummersetServer {
    /// Create a new Summerset server structure.
    fn new(
        protocol: SMRProtocol,
        peers: &Vec<String>,
    ) -> Result<Self, InitError> {
        SummersetServerNode::new(protocol, peers)
            .map(|s| SummersetServer { node: s })
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

    // create server node with given peers list
    let server = SummersetServer::new(protocol, &args.node_peers)?;

    // add and serve internal RPC service
    // let smr_addr = format!("[::1]:{}", args.smr_port).parse()?;

    // parse key-value API port
    let api_addr = format!("[::1]:{}", args.api_port).parse().map_err(|e| {
        InitError(format!(
            "failed to parse key-value API serving port {}: {}",
            args.api_port, e
        ))
    })?;
    println!("Starting service on address: {}", api_addr);

    // add and serve client API service
    Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(
            Server::builder()
                .add_service(ExternalApiServer::new(server.node))
                .serve(api_addr),
        )
        .map_err(|e| {
            InitError(format!(
                "error occurred when adding key-value API service: {}",
                e
            ))
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
