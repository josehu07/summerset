//! Summerset client benchmarking executable.

use std::collections::HashSet;

use clap::Parser;

use summerset_client::{SummersetClient, SMRProtocol, InitError};

/// Command line arguments definition.
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct CLIArgs {
    /// Name of SMR protocol to use.
    #[arg(short, long, default_value_t = String::from("DoNothing"))]
    protocol: String,

    /// List of server nodes (e.g., '-s host1:api_port -s host2:api_port').
    #[arg(short, long)]
    servers: Vec<String>,
}

impl CLIArgs {
    /// Sanitize command line arguments, return `Ok(protocol)` on success
    /// or `Err(InitError)` on any error.
    fn sanitize(&self) -> Result<SMRProtocol, InitError> {
        // servers list must not be empty
        if self.servers.is_empty() {
            return Err(InitError("servers list is empty".into()));
        }

        // check for duplicate servers
        let mut server_set = HashSet::new();
        for s in self.servers.iter() {
            if server_set.contains(s) {
                return Err(InitError(format!(
                    "duplicate server address {} given",
                    s
                )));
            }
            server_set.insert(s.clone());
        }

        SMRProtocol::parse_name(&self.protocol).ok_or_else(|| {
            InitError(format!("protocol name {} unrecognized", self.protocol))
        })
    }
}

// Client benchmarking executable main entrance.
fn main() -> Result<(), InitError> {
    // read in and parse command line arguments
    let args = CLIArgs::parse();
    let protocol = args.sanitize()?;

    // create client struct with given servers list
    let mut client = SummersetClient::new(protocol, args.servers)?;

    // connect to server(s)
    client.connect_servers()?;

    println!("{:?}", client.get("Jose"));
    println!("{:?}", client.put("Jose", "123"));
    println!("{:?}", client.get("Jose"));
    println!("{:?}", client.put("Jose", "456"));
    println!("{:?}", client.get("Jose"));

    Ok(())
}

#[cfg(test)]
mod bench_tests {
    use super::{CLIArgs, SMRProtocol, InitError};

    #[test]
    fn sanitize_valid() {
        let args = CLIArgs {
            protocol: "DoNothing".into(),
            servers: vec![
                "hostA:50078".into(),
                "hostB:50078".into(),
                "hostC:50078".into(),
            ],
        };
        assert_eq!(args.sanitize(), Ok(SMRProtocol::DoNothing));
    }

    #[test]
    fn sanitize_invalid_protocol() {
        let args = CLIArgs {
            protocol: "InvalidProtocol".into(),
            servers: vec![
                "hostA:50078".into(),
                "hostB:50078".into(),
                "hostC:50078".into(),
            ],
        };
        assert_eq!(
            args.sanitize(),
            Err(InitError(
                "protocol name InvalidProtocol unrecognized".into()
            ))
        );
    }

    #[test]
    fn sanitize_empty_servers() {
        let args = CLIArgs {
            protocol: "DoNothing".into(),
            servers: vec![],
        };
        assert_eq!(
            args.sanitize(),
            Err(InitError("servers list is empty".into()))
        );
    }

    #[test]
    fn sanitize_duplicate_server() {
        let args = CLIArgs {
            protocol: "DoNothing".into(),
            servers: vec!["somehost:50078".into(), "somehost:50078".into()],
        };
        assert_eq!(
            args.sanitize(),
            Err(InitError(
                "duplicate server address somehost:50078 given".into()
            ))
        );
    }
}
