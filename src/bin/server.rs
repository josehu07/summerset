use tonic::codegen::http::request;
use tonic::{transport::Server, Request, Response, Status};

use summerset_proto::summerset_api_server::{SummersetApi, SummersetApiServer};
use summerset_proto::{GetRequest, GetReply, PutRequest, PutReply};

pub mod summerset_proto {
    tonic::include_proto!("kv_api");
}

use summerset::{SummersetNode, SummersetError};

#[tonic::async_trait]
impl SummersetApi for SummersetNode {
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
            Err(err) => {
                return Err(Status::unknown(format!(
                    "unknown error: {:?}",
                    err
                )))
            }
        };

        Ok(Response::new(reply))
    }

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
            Err(err) => {
                return Err(Status::unknown(format!(
                    "unknown error: {:?}",
                    err
                )))
            }
        };

        Ok(Response::new(reply))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "[::1]:50051".parse()?;
    let node = SummersetNode::new();

    Server::builder()
        .add_service(SummersetApiServer::new(node))
        .serve(addr)
        .await?;

    Ok(())
}
