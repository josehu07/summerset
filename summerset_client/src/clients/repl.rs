//! Interactive REPL-style command-line interface client.

use std::io::{self, Write};

use crate::drivers::DriverClosedLoop;

use color_print::cprint;

use tokio::time::Duration;

use summerset::{
    GenericEndpoint, Command, CommandResult, RequestId, SummersetError,
};

/// Prompt string at the start of line.
const PROMPT: &str = ">>>>> ";

/// Recognizable command types.
enum ReplCommand {
    /// Normal state machine replication command.
    Normal(Command),

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
pub struct ClientRepl {
    /// Closed-loop request driver.
    driver: DriverClosedLoop,

    /// User input buffer.
    input_buf: String,
}

impl ClientRepl {
    /// Creates a new REPL-style client.
    pub fn new(endpoint: Box<dyn GenericEndpoint>, timeout: Duration) -> Self {
        ClientRepl {
            driver: DriverClosedLoop::new(endpoint, timeout),
            input_buf: String::new(),
        }
    }

    /// Prints the prompt string.
    fn print_prompt(&mut self) {
        cprint!("<bright-yellow>{}</>", PROMPT);
        io::stdout().flush().unwrap();
    }

    /// Prints (optionally) an error message and the help message.
    fn print_help(&mut self, err: Option<&SummersetError>) {
        if let Some(e) = err {
            println!("ERROR: {}", e);
        }
        println!("HELP: Supported commands are:");
        println!("        get <key>");
        println!("        put <key> <value>");
        println!("        reconnect");
        println!("        help");
        println!("        exit");
        println!(
            "      Keys and values currently cannot contain any whitespaces"
        );
        io::stdout().flush().unwrap();
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
        assert!(cmd_type.is_some());

        match &cmd_type.unwrap().to_lowercase()[..] {
            "get" => {
                let key = segs.next();
                if key.is_none() {
                    let err = SummersetError("not enough args".into());
                    self.print_help(Some(&err));
                    return Err(err);
                }

                // keys and values are kept as-is, no case conversions
                Ok(ReplCommand::Normal(Command::Get {
                    key: key.unwrap().into(),
                }))
            }

            "put" => {
                let key = segs.next();
                if key.is_none() {
                    let err = SummersetError("not enough args".into());
                    self.print_help(Some(&err));
                    return Err(err);
                }
                let value = segs.next();
                if value.is_none() {
                    let err = SummersetError("not enough args".into());
                    self.print_help(Some(&err));
                    return Err(err);
                }

                Ok(ReplCommand::Normal(Command::Put {
                    key: key.unwrap().into(),
                    value: value.unwrap().into(),
                }))
            }

            "help" => Ok(ReplCommand::PrintHelp),

            "reconnect" => Ok(ReplCommand::Reconnect),

            "exit" => Ok(ReplCommand::Exit),

            _ => {
                let err = SummersetError(format!(
                    "unrecognized command: {}",
                    cmd_type.unwrap()
                ));
                self.print_help(Some(&err));
                Err(err)
            }
        }
    }

    /// Issues the command to the service and wait for the reply.
    async fn eval_command(
        &mut self,
        cmd: Command,
    ) -> Result<Option<(RequestId, CommandResult, Duration)>, SummersetError>
    {
        match cmd {
            Command::Get { key } => {
                Ok(self.driver.get(&key).await?.map(|(req_id, value, lat)| {
                    (req_id, CommandResult::Get { value }, lat)
                }))
            }

            Command::Put { key, value } => {
                Ok(self.driver.put(&key, &value).await?.map(
                    |(req_id, old_value, lat)| {
                        (req_id, CommandResult::Put { old_value }, lat)
                    },
                ))
            }
        }
    }

    /// Prints command execution result.
    fn print_result(
        &mut self,
        result: Option<(RequestId, CommandResult, Duration)>,
    ) {
        if let Some((req_id, cmd_result, lat)) = result {
            let lat_ms = lat.as_secs_f64() * 1000.0;
            println!("({}) {:?} <took {:.2} ms>", req_id, cmd_result, lat_ms);
        } else {
            println!("Unsuccessful: wrong leader or timeout?");
        }
        io::stdout().flush().unwrap();
    }

    /// One iteration of the REPL loop.
    async fn iter(&mut self) -> Result<bool, SummersetError> {
        self.print_prompt();

        let cmd = self.read_command()?;
        match cmd {
            ReplCommand::Exit => {
                println!("Exitting...");
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
                self.print_help(None);
                Ok(true)
            }

            ReplCommand::Normal(cmd) => {
                let result = self.eval_command(cmd).await?;
                self.print_result(result);
                Ok(true)
            }
        }
    }

    /// Runs the infinite REPL loop.
    pub async fn run(&mut self) -> Result<(), SummersetError> {
        self.driver.connect().await?;

        loop {
            if let Ok(false) = self.iter().await {
                self.driver.leave(true).await?;
                break;
            }
        }

        Ok(())
    }
}
