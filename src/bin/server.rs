//! Summerset server node executable.

mod summerset_proto {
    tonic::include_proto!("kv_api");
}

use tonic::{transport::Server, Request, Response, Status};
use summerset_proto::summerset_api_server::{SummersetApi, SummersetApiServer};
use summerset_proto::{GetRequest, GetReply, PutRequest, PutReply};

use std::collections::HashSet;
use clap::Parser;

use summerset::{CLIError, SummersetNode, SMRProtocol};

#[tonic::async_trait]
impl SummersetApi for SummersetNode {
    /// Handler of client Get request.
    async fn get(
        &self,
        request: Request<GetRequest>,
    ) -> Result<Response<GetReply>, Status> {
        println!("Request: {:?}", request);
        let req = request.into_inner();

        let reply = match self.handle_get(&req.key) {
            Ok(Some(value)) => GetReply {
                request_id: req.request_id,
                key: req.key,
                found: true,
                value,
            },
            Ok(None) => GetReply {
                request_id: req.request_id,
                key: req.key,
                found: false,
                value: "".into(),
            },
            Err(err) => return Err(Status::unknown(format!("{:?}", err))),
        };

        Ok(Response::new(reply))
    }

    /// Handler of client Put request.
    async fn put(
        &self,
        request: Request<PutRequest>,
    ) -> Result<Response<PutReply>, Status> {
        println!("Request: {:?}", request);
        let req = request.into_inner();

        let reply = match self.handle_put(&req.key, &req.value) {
            Ok(Some(old_value)) => PutReply {
                request_id: req.request_id,
                key: req.key,
                found: true,
                old_value,
            },
            Ok(None) => PutReply {
                request_id: req.request_id,
                key: req.key,
                found: false,
                old_value: "".into(),
            },
            Err(err) => return Err(Status::unknown(format!("{:?}", err))),
        };

        Ok(Response::new(reply))
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

    /// List of peer server nodes (e.g., '-s host1:port -s host2:port').
    #[arg(short, long)]
    node_peers: Vec<String>,
}

impl CLIArgs {
    fn sanitize(&self) -> Result<SMRProtocol, CLIError> {
        if self.api_port <= 1024 {
            Err(CLIError(format!("api_port {} is invalid", self.api_port)))
        } else if self.smr_port <= 1024 {
            Err(CLIError(format!("smr_port {} is invalid", self.smr_port)))
        } else {
            let mut peer_set = HashSet::new();
            for s in self.node_peers.iter() {
                if peer_set.contains(s) {
                    return Err(CLIError(format!(
                        "duplicate peer address {} given",
                        s
                    )));
                }
                peer_set.insert(s.clone());
            }
            SMRProtocol::parse_name(&self.protocol).ok_or_else(|| {
                CLIError(format!(
                    "protocol name {} unrecognized",
                    self.protocol
                ))
            })
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), CLIError> {
    // read in and parse command line arguments
    let args = CLIArgs::parse();
    let protocol = args.sanitize()?;

    // create server node
    let node = SummersetNode::new(protocol);

    // add and serve internal RPC service
    // let smr_addr = format!("[::1]:{}", args.smr_port).parse()?;

    // parse key-value API port
    let api_addr = format!("[::1]:{}", args.api_port).parse().map_err(|e| {
        CLIError(format!(
            "failed to parse key-value API serving port {}: {}",
            args.api_port, e
        ))
    })?;
    println!("Starting service on address: {}", api_addr);

    // add and serve client API service
    Server::builder()
        .add_service(SummersetApiServer::new(node))
        .serve(api_addr)
        .await
        .map_err(|e| {
            CLIError(format!(
                "error occurred when adding key-value API service: {}",
                e
            ))
        })?;

    Ok(())
}

#[cfg(test)]
mod server_tests {
    use super::{CLIArgs, CLIError};
    use crate::SMRProtocol;

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
            Err(CLIError("api_port 1023 is invalid".into()))
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
            Err(CLIError("smr_port 1023 is invalid".into()))
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
            Err(CLIError(
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
            Err(CLIError(
                "duplicate peer address somehost:50078 given".into()
            ))
        );
    }
}
