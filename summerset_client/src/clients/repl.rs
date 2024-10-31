//! Interactive REPL-style command-line interface client.

use std::collections::HashSet;
use std::io::{self, Write};
use std::str::SplitWhitespace;

use crate::drivers::{DriverClosedLoop, DriverReply};

use color_print::{cprint, cprintln};

use tokio::time::Duration;

use summerset::{
    Command, CtrlReply, CtrlRequest, GenericEndpoint, LeaserRoles, ReplicaId,
    SummersetError,
};

/// Prompt string at the start of line.
const PROMPT: &str = ">>>>> ";

/// Recognizable command types.
enum ReplCommand {
    /// Normal state machine replication command.
    Normal(Command),

    /// Leaser configuration change request. (only for relevant protocols)
    Leasers(LeaserRoles),

    /// Control request to the manager.
    Control(CtrlRequest),

    /// Reconnect to the service.
    Reconnect,

    /// Print help message.
    PrintHelp,

    /// Client exit.
    Exit,

    /// Nothing read.
    Nothing,
}

/// Interactive REPL-style client struct.
pub(crate) struct ClientRepl {
    /// Closed-loop request driver.
    driver: DriverClosedLoop,

    /// Timeout duration setting.
    timeout: Duration,

    /// User input buffer.
    input_buf: String,

    /// Current leaser role config I want.
    leaser_roles: Option<LeaserRoles>,
}

impl ClientRepl {
    /// Creates a new REPL-style client.
    pub(crate) fn new(
        endpoint: Box<dyn GenericEndpoint>,
        timeout: Duration,
    ) -> Self {
        ClientRepl {
            driver: DriverClosedLoop::new(endpoint, timeout),
            timeout,
            input_buf: String::new(),
            leaser_roles: None,
        }
    }

    /// Prints the prompt string.
    #[inline]
    fn print_prompt() {
        cprint!("<bright-yellow>{}</>", PROMPT);
        io::stdout().flush().unwrap();
    }

    /// Prints (optionally) an error message and the help message.
    fn print_help(err: Option<&SummersetError>) {
        if let Some(e) = err {
            cprintln!("<bright-red>✗</> {}", e);
        }
        println!("HELP: Commands for normal operations:");
        println!("          get <key>");
        println!("          put <key> <value>");
        println!("          help");
        println!("          exit");
        println!("      Commands for leaser roles config:");
        println!("          grantor [servers]");
        println!("          grantee [servers]");
        println!("          leader <server>");
        println!("      Commands for control/testing:");
        println!("          reconnect");
        println!("          reset [servers]");
        println!("          pause [servers]");
        println!("          resume [servers]");
        println!("          snapshot [servers]");
        println!(
            "      Keys and values currently cannot contain any whitespaces"
        );
        io::stdout().flush().unwrap();
    }

    /// Expect to get the next segment string from parsed segs.
    #[inline]
    fn expect_next_seg<'s>(
        segs: &mut SplitWhitespace<'s>,
    ) -> Result<&'s str, SummersetError> {
        if let Some(seg) = segs.next() {
            Ok(seg)
        } else {
            let err = SummersetError::msg("not enough args");
            Self::print_help(Some(&err));
            Err(err)
        }
    }

    /// Drain all of the remaining segments into a hash set and interpret as
    /// replica IDs.
    #[inline]
    fn drain_server_ids(
        segs: &mut SplitWhitespace,
    ) -> Result<HashSet<ReplicaId>, SummersetError> {
        let mut servers = HashSet::new();
        for seg in segs {
            servers.insert(seg.parse::<ReplicaId>()?);
        }
        Ok(servers)
    }

    /// Reads in user input and parses into a command.
    fn read_command(&mut self) -> Result<ReplCommand, SummersetError> {
        self.input_buf.clear();
        let nread = io::stdin().read_line(&mut self.input_buf)?;
        if nread == 0 {
            return Ok(ReplCommand::Exit);
        }

        let line: &str = self.input_buf.trim();
        if line.is_empty() {
            return Ok(ReplCommand::Nothing);
        }

        // split input line by whitespaces, getting an iterator of segments
        let mut segs = self.input_buf.split_whitespace();

        // get command type, match case-insensitively
        let cmd_type = segs.next();
        debug_assert!(cmd_type.is_some());
        debug_assert!(self.leaser_roles.is_some());

        match &cmd_type.unwrap().to_lowercase()[..] {
            "get" => {
                // keys are kept as-is, no case conversions
                let key = Self::expect_next_seg(&mut segs)?;
                Ok(ReplCommand::Normal(Command::Get { key: key.into() }))
            }

            "put" => {
                // keys and values are kept as-is, no case conversions
                let key = Self::expect_next_seg(&mut segs)?;
                let value = Self::expect_next_seg(&mut segs)?;
                Ok(ReplCommand::Normal(Command::Put {
                    key: key.into(),
                    value: value.into(),
                }))
            }

            "help" => Ok(ReplCommand::PrintHelp),

            "reconnect" => Ok(ReplCommand::Reconnect),

            "grantor" => {
                let servers = Self::drain_server_ids(&mut segs)?;
                let leaser_roles = self.leaser_roles.as_mut().unwrap();
                leaser_roles.grantors.clear();
                for server in servers {
                    leaser_roles.grantors.set(server, true)?;
                } // applies on top of current leaser roles config
                Ok(ReplCommand::Leasers(self.leaser_roles.clone().unwrap()))
            }

            "grantee" => {
                let servers = Self::drain_server_ids(&mut segs)?;
                let leaser_roles = self.leaser_roles.as_mut().unwrap();
                leaser_roles.grantees.clear();
                for server in servers {
                    leaser_roles.grantees.set(server, true)?;
                } // applies on top of current leaser roles config
                Ok(ReplCommand::Leasers(self.leaser_roles.clone().unwrap()))
            }

            "leader" => {
                let mut servers = Self::drain_server_ids(&mut segs)?;
                let leader = if servers.is_empty() {
                    None
                } else if servers.len() > 1 {
                    let err = SummersetError::msg("too many args");
                    Self::print_help(Some(&err));
                    return Err(err);
                } else {
                    Some(servers.drain().next().unwrap())
                };
                let leaser_roles = self.leaser_roles.as_mut().unwrap();
                leaser_roles.leader = leader; // applies on top of current
                Ok(ReplCommand::Leasers(self.leaser_roles.clone().unwrap()))
            }

            "reset" => {
                let servers = Self::drain_server_ids(&mut segs)?;
                Ok(ReplCommand::Control(CtrlRequest::ResetServers {
                    servers,
                    durable: true,
                }))
            }

            "pause" => {
                let servers = Self::drain_server_ids(&mut segs)?;
                Ok(ReplCommand::Control(CtrlRequest::PauseServers { servers }))
            }

            "resume" => {
                let servers = Self::drain_server_ids(&mut segs)?;
                Ok(ReplCommand::Control(CtrlRequest::ResumeServers { servers }))
            }

            "snapshot" => {
                let servers = Self::drain_server_ids(&mut segs)?;
                Ok(ReplCommand::Control(CtrlRequest::TakeSnapshot { servers }))
            }

            "exit" => Ok(ReplCommand::Exit),

            _ => {
                let err = SummersetError::msg(format!(
                    "unrecognized command: {}",
                    cmd_type.unwrap()
                ));
                Self::print_help(Some(&err));
                Err(err)
            }
        }
    }

    /// Issues the command to the service and wait for the reply.
    async fn eval_command(
        &mut self,
        cmd: Command,
    ) -> Result<DriverReply, SummersetError> {
        match cmd {
            Command::Get { key } => Ok(self.driver.get(&key).await?),
            Command::Put { key, value } => {
                Ok(self.driver.put(&key, &value).await?)
            }
        }
    }

    /// Prints command execution result.
    fn print_result(&mut self, result: DriverReply) {
        match result {
            DriverReply::Success {
                req_id,
                cmd_result,
                latency,
            } => {
                let lat_ms = latency.as_secs_f64() * 1000.0;
                cprintln!(
                    "<bright-green>✓</> ({}) {:?} <<took {:.2} ms>>",
                    req_id,
                    cmd_result,
                    lat_ms
                );
            }

            DriverReply::Leasers { req_id, changed } => {
                if changed {
                    cprintln!(
                        "<bright-yellow>✓</> ({}) leaser roles config changed",
                        req_id
                    );
                } else {
                    cprintln!(
                        "<bright-red>✗</> ({}) leaser roles change unsuccessful",
                        req_id
                    );
                }
            }

            DriverReply::Failure => {
                cprintln!("<bright-red>✗</> service replied unknown error");
            }

            DriverReply::Redirect { server } => {
                cprintln!(
                    "<bright-cyan>✗</> service redirected me to server {}",
                    server
                );
            }

            DriverReply::Timeout => {
                cprintln!(
                    "<bright-red>✗</> client-side timeout {} ms",
                    self.timeout.as_millis()
                );
            }
        }

        io::stdout().flush().unwrap();
    }

    /// Makes a control request to the manager and wait for the reply.
    async fn make_ctrl_req(
        &mut self,
        req: CtrlRequest,
    ) -> Result<CtrlReply, SummersetError> {
        self.driver.ctrl_stub().send_req_insist(&req)?;
        self.driver.ctrl_stub().recv_reply().await
    }

    /// Prints control request reply.
    fn print_ctrl_reply(&mut self, reply: CtrlReply) {
        match reply {
            CtrlReply::ResetServers { servers } => {
                cprintln!("<bright-blue>#</> reset servers {:?}", servers);
            }

            CtrlReply::PauseServers { servers } => {
                cprintln!("<bright-blue>#</> paused servers {:?}", servers);
            }

            CtrlReply::ResumeServers { servers } => {
                cprintln!("<bright-blue>#</> resumed servers {:?}", servers);
            }

            CtrlReply::TakeSnapshot { snapshot_up_to } => {
                cprintln!(
                    "<bright-blue>#</> servers snapshot up to {:?}",
                    snapshot_up_to
                );
            }

            _ => {
                cprintln!("<bright-red>✗</> unexpected ctrl reply type");
            }
        }
    }

    /// One iteration of the REPL loop. On success, returns a boolean that's
    /// false only when exitting.
    async fn iter(&mut self) -> Result<bool, SummersetError> {
        Self::print_prompt();

        let cmd = self.read_command()?;
        match cmd {
            ReplCommand::Exit => {
                println!("Exiting...");
                Ok(false)
            }

            ReplCommand::Nothing => Ok(true),

            ReplCommand::Reconnect => {
                println!("Reconnecting...");
                self.driver.leave(false).await?;
                self.driver.connect().await?;
                Ok(true)
            }

            ReplCommand::PrintHelp => {
                Self::print_help(None);
                Ok(true)
            }

            ReplCommand::Normal(cmd) => {
                let result = self.eval_command(cmd).await?;
                self.print_result(result);
                Ok(true)
            }

            ReplCommand::Leasers(conf) => {
                let result = self.driver.conf(conf).await?;
                self.print_result(result);
                Ok(true)
            }

            ReplCommand::Control(req) => {
                let reply = self.make_ctrl_req(req).await?;
                self.print_ctrl_reply(reply);
                Ok(true)
            }
        }
    }

    /// Runs the infinite REPL loop.
    pub(crate) async fn run(&mut self) -> Result<(), SummersetError> {
        self.driver.connect().await?;
        self.leaser_roles = Some(LeaserRoles::empty(self.driver.population()));

        loop {
            match self.iter().await {
                Ok(true) => {}

                Ok(false) => {
                    self.driver.leave(true).await?;
                    break;
                }

                Err(err) => {
                    cprintln!("<bright-red>✗</> error: {}", err);
                }
            }
        }

        Ok(())
    }
}
