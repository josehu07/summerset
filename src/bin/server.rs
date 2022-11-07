mod summerset_proto {
    tonic::include_proto!("kv_api");
}

use tonic::{transport::Server, Request, Response, Status};
use summerset_proto::summerset_api_server::{SummersetApi, SummersetApiServer};
use summerset_proto::{GetRequest, GetReply, PutRequest, PutReply};

use std::collections::HashSet;
use clap::Parser;

use summerset::{SummersetNode, SMRProtocol};

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
    #[arg(short, long, default_value_t = String::from("do-nothing"))]
    protocol: String,

    /// List of peer server nodes locations.
    #[arg(short, long)]
    node_peers: Vec<String>,
}

impl CLIArgs {
    fn sanitize(&self) -> SMRProtocol {
        if self.api_port <= 1024 {
            panic!("api_port {} is invalid", self.api_port);
        } else if self.smr_port <= 1024 {
            panic!("smr_port {} is invalid", self.smr_port);
        } else {
            let mut peer_set = HashSet::new();
            for s in self.node_peers.iter() {
                if peer_set.contains(s) {
                    panic!("duplicate peer address {} given", s);
                }
                peer_set.insert(s.clone());
            }

            let protocol = SMRProtocol::parse_name(&self.protocol);
            if protocol.is_none() {
                panic!("protocol name {} unrecognized", self.protocol);
            }
            protocol.unwrap()
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // read in and parse command line arguments
    let args = CLIArgs::parse();
    let protocol = args.sanitize();

    // create server node
    let node = SummersetNode::new(protocol);

    // add server-server internal RPC service
    // let smr_addr = format!("localhost:{}", args.smr_port).parse()?;

    // add client API service
    let api_addr = format!("[::1]:{}", args.api_port).parse()?;
    Server::builder()
        .add_service(SummersetApiServer::new(node))
        .serve(api_addr)
        .await?;

    Ok(())
}

#[cfg(test)]
mod server_tests {
    use super::CLIArgs;
    use crate::SMRProtocol;

    #[test]
    fn sanitize_valid() {
        let args = CLIArgs {
            api_port: 50077,
            smr_port: 50078,
            protocol: "DoNothing".into(),
            node_peers: vec![
                "localhost:50079".into(),
                "localhost:50080".into(),
            ],
        };
        assert_eq!(args.sanitize(), SMRProtocol::DoNothing);
    }

    #[test]
    #[should_panic(expected = "api_port 1023 is invalid")]
    fn sanitize_invalid_api_port() {
        let args = CLIArgs {
            api_port: 1023,
            smr_port: 50078,
            protocol: "DoNothing".into(),
            node_peers: vec![],
        };
        args.sanitize();
    }

    #[test]
    #[should_panic(expected = "smr_port 1023 is invalid")]
    fn sanitize_invalid_smr_port() {
        let args = CLIArgs {
            api_port: 50077,
            smr_port: 1023,
            protocol: "DoNothing".into(),
            node_peers: vec![],
        };
        args.sanitize();
    }

    #[test]
    #[should_panic(expected = "protocol name InvalidProtocol unrecognized")]
    fn sanitize_invalid_protocol() {
        let args = CLIArgs {
            api_port: 40077,
            smr_port: 50078,
            protocol: "InvalidProtocol".into(),
            node_peers: vec![],
        };
        args.sanitize();
    }

    #[test]
    #[should_panic(expected = "duplicate peer address somehost:50078 given")]
    fn sanitize_duplicate_peer() {
        let args = CLIArgs {
            api_port: 50077,
            smr_port: 50078,
            protocol: "DoNothing".into(),
            node_peers: vec!["somehost:50078".into(), "somehost:50078".into()],
        };
        args.sanitize();
    }
}
