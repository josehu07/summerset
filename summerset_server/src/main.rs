//! Summerset server node executable.

use std::collections::{HashSet, HashMap};
use std::net::SocketAddr;
use std::process::ExitCode;

use clap::Parser;

use env_logger::Env;

use tokio::runtime::Builder;

use summerset::{SMRProtocol, ReplicaId, SummersetError, pf_error};

/// Command line arguments definition.
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct CliArgs {
    /// Name of SMR protocol to use.
    #[arg(short, long, default_value_t = String::from("RepNothing"))]
    protocol: String,

    /// Protocol-specific server configuration TOML string.
    /// Every '+' is treated as newline.
    #[arg(long, default_value_t = String::from(""))]
    config: String,

    /// Key-value API port open to clients.
    #[arg(short, long, default_value_t = 52700)]
    api_port: u16,

    /// The start of internal ports range used for server-server transport.
    #[arg(short, long, default_value_t = 52800)]
    base_conn_port: u16,

    /// Replica ID of myself.
    #[arg(short, long)]
    id: ReplicaId,

    /// List of server replica nodes, the order of which maps to replica IDs.
    /// Example: '-r host0:conn_port01 -r host1:conn_port11 -r host2:port21'.
    #[arg(short, long)]
    replicas: Vec<SocketAddr>,

    /// Number of tokio worker threads.
    #[arg(long, default_value_t = 16)]
    threads: usize,
}

impl CliArgs {
    /// Sanitize command line arguments, return `Ok(protocol)` on success
    /// or `Err(SummersetError)` on any error.
    fn sanitize(&self) -> Result<SMRProtocol, SummersetError> {
        // check for duplicate peers
        let mut replicas_set = HashSet::new();
        for addr in self.replicas.iter() {
            if replicas_set.contains(addr) {
                return Err(SummersetError(format!(
                    "duplicate replica address '{}' given",
                    addr
                )));
            }
            replicas_set.insert(addr);
        }

        let population = self.replicas.len() as u16;
        if (self.id as u16) >= population {
            return Err(SummersetError(format!(
                "invalid replica ID {} / {}",
                self.id, population
            )));
        }
        let my_addr = self.replicas[self.id as usize];

        if self.api_port <= 1024 {
            Err(SummersetError(format!(
                "invalid api_port {}",
                self.api_port
            )))
        } else if self.base_conn_port <= 1024 {
            Err(SummersetError(format!(
                "invalid base_conn_port {}",
                self.base_conn_port
            )))
        } else if self.api_port >= self.base_conn_port
            && self.api_port < self.base_conn_port + population
        {
            Err(SummersetError(format!(
                "api_port {} is in range of conn_ports",
                self.api_port
            )))
        } else if self.base_conn_port + (self.id as u16) != my_addr.port() {
            Err(SummersetError(format!(
                "conn_port {} does not match replica addr '{}'",
                self.base_conn_port + (self.id as u16),
                my_addr
            )))
        } else if self.threads < 2 {
            Err(SummersetError(format!(
                "invalid number of threads {}",
                self.threads
            )))
        } else {
            SMRProtocol::parse_name(&self.protocol).ok_or(SummersetError(
                format!("protocol name '{}' unrecognized", self.protocol),
            ))
        }
    }
}

// Server node executable main entrance.
fn server_main() -> Result<(), SummersetError> {
    // read in and parse command line arguments
    let mut args = CliArgs::parse();
    let protocol = args.sanitize()?;
    let mut peer_addrs = HashMap::new();
    for (id, &addr) in args.replicas.iter().enumerate() {
        let id = id as ReplicaId;
        if id != args.id {
            peer_addrs.insert(id, addr); // skip myself
        }
    }

    // parse key-value API port
    let api_addr: SocketAddr = format!("127.0.0.1:{}", args.api_port)
        .parse()
        .map_err(|e| {
        SummersetError(format!(
            "failed to parse api_addr: port {}: {}",
            args.api_port, e
        ))
    })?;

    // parse base internal communication ports
    let mut conn_addrs = HashMap::new();
    for id in 0..(args.replicas.len() as ReplicaId) {
        if id == args.id {
            continue; // skip myself
        }
        let conn_port = args.base_conn_port + (id as u16);
        let conn_addr: SocketAddr =
            format!("127.0.0.1:{}", conn_port).parse().map_err(|e| {
                SummersetError(format!(
                    "failed to parse conn_addr: port {}: {}",
                    conn_port, e
                ))
            })?;
        conn_addrs.insert(id, conn_addr);
    }

    // parse optional config string if given
    let config_str = if args.config.is_empty() {
        None
    } else {
        args.config = args.config.replace('+', "\n");
        Some(&args.config[..])
    };

    // create server node with given configuration
    let mut node = protocol.new_server_node(
        args.id,
        args.replicas.len() as u8,
        api_addr,
        conn_addrs,
        peer_addrs,
        config_str,
    )?;

    // create tokio multi-threaded runtime
    let runtime = Builder::new_multi_thread()
        .enable_all()
        .worker_threads(args.threads)
        .thread_name(format!("tokio-worker-replica{}", args.id))
        .build()?;

    // enter tokio runtime, setup the server node, and start the main event
    // loop logic
    runtime.block_on(async move {
        node.setup().await?;

        node.run().await;

        Ok::<(), SummersetError>(()) // give type hint for this async closure
    })
}

fn main() -> ExitCode {
    env_logger::Builder::from_env(Env::default().default_filter_or("info"))
        .format_timestamp(None)
        .format_module_path(true)
        .format_target(false)
        .init();

    if let Err(ref e) = server_main() {
        pf_error!("server"; "server_main exitted: {}", e);
        ExitCode::FAILURE
    } else {
        ExitCode::SUCCESS
    }
}

#[cfg(test)]
mod server_args_tests {
    use super::*;

    #[test]
    fn sanitize_valid() -> Result<(), SummersetError> {
        let args = CliArgs {
            protocol: "RepNothing".into(),
            api_port: 52710,
            base_conn_port: 52810,
            id: 1,
            replicas: vec![
                "127.0.0.1:52801".parse()?,
                "127.0.0.1:52811".parse()?,
            ],
            threads: 2,
            config: "".into(),
        };
        assert_eq!(args.sanitize(), Ok(SMRProtocol::RepNothing));
        Ok(())
    }

    #[test]
    fn sanitize_invalid_api_port() -> Result<(), SummersetError> {
        let args = CliArgs {
            protocol: "RepNothing".into(),
            api_port: 1023,
            base_conn_port: 52800,
            id: 0,
            replicas: vec!["127.0.0.1:52800".parse()?],
            threads: 2,
            config: "".into(),
        };
        assert!(args.sanitize().is_err());
        Ok(())
    }

    #[test]
    fn sanitize_invalid_conn_port() -> Result<(), SummersetError> {
        let args = CliArgs {
            protocol: "RepNothing".into(),
            api_port: 52700,
            base_conn_port: 1023,
            id: 0,
            replicas: vec!["127.0.0.1:52800".parse()?],
            threads: 2,
            config: "".into(),
        };
        assert!(args.sanitize().is_err());
        Ok(())
    }

    #[test]
    fn sanitize_conn_port_range() -> Result<(), SummersetError> {
        let args = CliArgs {
            protocol: "RepNothing".into(),
            api_port: 52801,
            base_conn_port: 52800,
            id: 0,
            replicas: vec![
                "127.0.0.1:52800".parse()?,
                "127.0.0.1:52810".parse()?,
                "127.0.0.1:52820".parse()?,
            ],
            threads: 2,
            config: "".into(),
        };
        assert!(args.sanitize().is_err());
        Ok(())
    }

    #[test]
    fn sanitize_conn_port_mismatch() -> Result<(), SummersetError> {
        let args = CliArgs {
            protocol: "RepNothing".into(),
            api_port: 52700,
            base_conn_port: 52900,
            id: 0,
            replicas: vec!["127.0.0.1:52800".parse()?],
            threads: 2,
            config: "".into(),
        };
        assert!(args.sanitize().is_err());
        Ok(())
    }

    #[test]
    fn sanitize_invalid_protocol() -> Result<(), SummersetError> {
        let args = CliArgs {
            protocol: "InvalidProtocol".into(),
            api_port: 52700,
            base_conn_port: 52800,
            id: 0,
            replicas: vec!["127.0.0.1:52800".parse()?],
            threads: 2,
            config: "".into(),
        };
        assert!(args.sanitize().is_err());
        Ok(())
    }

    #[test]
    fn sanitize_duplicate_replica() -> Result<(), SummersetError> {
        let args = CliArgs {
            protocol: "RepNothing".into(),
            api_port: 52700,
            base_conn_port: 52800,
            id: 0,
            replicas: vec![
                "127.0.0.1:52800".parse()?,
                "127.0.0.1:52800".parse()?,
            ],
            threads: 2,
            config: "".into(),
        };
        assert!(args.sanitize().is_err());
        Ok(())
    }

    #[test]
    fn sanitize_invalid_id() -> Result<(), SummersetError> {
        let args = CliArgs {
            protocol: "RepNothing".into(),
            api_port: 52710,
            base_conn_port: 52810,
            id: 2,
            replicas: vec![
                "127.0.0.1:52801".parse()?,
                "127.0.0.1:52811".parse()?,
            ],
            threads: 2,
            config: "".into(),
        };
        assert!(args.sanitize().is_err());
        Ok(())
    }

    #[test]
    fn sanitize_invalid_threads() -> Result<(), SummersetError> {
        let args = CliArgs {
            protocol: "RepNothing".into(),
            api_port: 52700,
            base_conn_port: 52800,
            id: 0,
            replicas: vec![
                "127.0.0.1:52800".parse()?,
                "127.0.0.1:52810".parse()?,
            ],
            threads: 1,
            config: "".into(),
        };
        assert!(args.sanitize().is_err());
        Ok(())
    }
}
