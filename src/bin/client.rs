mod summerset_proto {
    tonic::include_proto!("kv_api");
}

use summerset_proto::summerset_api_client::SummersetApiClient;
use summerset_proto::{GetRequest, PutRequest};

use std::collections::HashSet;
use clap::Parser;

/// Command line arguments definition.
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct CLIArgs {
    /// List of server addresses to connect to.
    #[arg(short, long)]
    servers: Vec<String>,
}

impl CLIArgs {
    fn sanitize(&self) {
        if self.servers.is_empty() {
            panic!("servers list is empty");
        }

        let mut server_set = HashSet::new();
        for s in self.servers.iter() {
            if server_set.contains(s) {
                panic!("duplicate server address {} given", s);
            }
            server_set.insert(s.clone());
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // read in command line arguments
    let args = CLIArgs::parse();
    args.sanitize();

    // create client and connect to server 0
    let mut client =
        SummersetApiClient::connect(args.servers[0].clone()).await?;

    let put_request = tonic::Request::new(PutRequest {
        request_id: 1234,
        key: "Jose".into(),
        value: "180".into(),
    });

    let put_response = client.put(put_request).await?;
    println!("Put Response = {:?}", put_response);

    let get_request = tonic::Request::new(GetRequest {
        request_id: 1234,
        key: "Jose".into(),
    });

    let get_response = client.get(get_request).await?;
    println!("Get Response = {:?}", get_response);

    Ok(())
}

#[cfg(test)]
mod client_tests {
    use super::CLIArgs;

    #[test]
    fn sanitize_valid() {
        let args = CLIArgs {
            servers: vec![
                "localhost:50078".into(),
                "localhost:50079".into(),
                "localhost:50080".into(),
            ],
        };
        args.sanitize();
    }

    #[test]
    #[should_panic(expected = "servers list is empty")]
    fn sanitize_empty_servers() {
        let args = CLIArgs { servers: vec![] };
        args.sanitize();
    }

    #[test]
    #[should_panic(expected = "duplicate server address somehost:50078 given")]
    fn sanitize_duplicate_server() {
        let args = CLIArgs {
            servers: vec!["somehost:50078".into(), "somehost:50078".into()],
        };
        args.sanitize();
    }
}
