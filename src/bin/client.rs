//! Summerset client side executable.

mod summerset_proto {
    tonic::include_proto!("kv_api");
}

use summerset_proto::summerset_api_client::SummersetApiClient;
use summerset_proto::{GetRequest, PutRequest};

use std::collections::HashSet;
use clap::Parser;

use summerset::CLIError;

/// Command line arguments definition.
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct CLIArgs {
    /// List of server node addresses (e.g., '-s host1:port -s host2:port').
    #[arg(short, long)]
    servers: Vec<String>,
}

impl CLIArgs {
    fn sanitize(&self) -> Result<(), CLIError> {
        if self.servers.is_empty() {
            Err(CLIError("servers list is empty".into()))
        } else {
            let mut server_set = HashSet::new();
            for s in self.servers.iter() {
                if server_set.contains(s) {
                    return Err(CLIError(format!(
                        "duplicate server address {} given",
                        s
                    )));
                }
                server_set.insert(s.clone());
            }
            Ok(())
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), CLIError> {
    // read in command line arguments
    let args = CLIArgs::parse();
    args.sanitize()?;

    // create client and connect to server 0
    let server_addr = format!("http://{}", args.servers[0]);
    let mut client = SummersetApiClient::connect(server_addr.clone())
        .await
        .map_err(|e| {
            CLIError(format!(
                "failed to connect to server address {}: {}",
                server_addr, e
            ))
        })?;

    let put_request = tonic::Request::new(PutRequest {
        request_id: 1234,
        key: "Jose".into(),
        value: "180".into(),
    });

    let put_response = client
        .put(put_request)
        .await
        .map_err(|e| CLIError(format!("request failed: {}", e)))?;
    println!("Put Response = {:?}", put_response);

    let get_request = tonic::Request::new(GetRequest {
        request_id: 1234,
        key: "Jose".into(),
    });

    let get_response = client
        .get(get_request)
        .await
        .map_err(|e| CLIError(format!("request failed: {}", e)))?;
    println!("Get Response = {:?}", get_response);

    Ok(())
}

#[cfg(test)]
mod client_tests {
    use super::{CLIArgs, CLIError};

    #[test]
    fn sanitize_valid() {
        let args = CLIArgs {
            servers: vec![
                "[::1]:50078".into(),
                "[::1]:50079".into(),
                "[::1]:50080".into(),
            ],
        };
        assert_eq!(args.sanitize(), Ok(()));
    }

    #[test]
    fn sanitize_empty_servers() {
        let args = CLIArgs { servers: vec![] };
        assert_eq!(
            args.sanitize(),
            Err(CLIError("servers list is empty".into()))
        );
    }

    #[test]
    fn sanitize_duplicate_server() {
        let args = CLIArgs {
            servers: vec!["somehost:50078".into(), "somehost:50078".into()],
        };
        assert_eq!(
            args.sanitize(),
            Err(CLIError(
                "duplicate server address somehost:50078 given".into()
            ))
        );
    }
}
