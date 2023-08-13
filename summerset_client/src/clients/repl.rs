//! Interactive REPL-style command-line interface client.

use std::io::{self, Write};

use crate::drivers::DriverClosedLoop;

use tokio::time::Duration;

use summerset::{
    GenericEndpoint, ClientId, Command, CommandResult, RequestId,
    SummersetError,
};

/// Prompt string at the start of line.
const PROMPT: &str = ">>>>> ";

/// Interactive REPL-style client struct.
pub struct ClientRepl {
    /// Client ID.
    _id: ClientId,

    /// Closed-loop request driver.
    driver: DriverClosedLoop,

    /// User input buffer.
    input_buf: String,
}

impl ClientRepl {
    /// Creates a new REPL-style client.
    pub fn new(
        id: ClientId,
        stub: Box<dyn GenericEndpoint>,
        timeout: Duration,
    ) -> Self {
        ClientRepl {
            _id: id,
            driver: DriverClosedLoop::new(id, stub, timeout),
            input_buf: String::new(),
        }
    }

    /// Prints the prompt string.
    fn print_prompt(&mut self) {
        print!("{}", PROMPT);
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
        println!("        help");
        println!("        exit");
        println!(
            "      Keys and values currently cannot contain any whitespaces"
        );
        io::stdout().flush().unwrap();
    }

    /// Reads in user input and parses into a command.
    fn read_command(&mut self) -> Result<Option<Command>, SummersetError> {
        self.input_buf.clear();
        io::stdin().read_line(&mut self.input_buf)?;
        let line: &str = self.input_buf.trim();
        if line.is_empty() {
            return Err(SummersetError("".into()));
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
                Ok(Some(Command::Get {
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

                Ok(Some(Command::Put {
                    key: key.unwrap().into(),
                    value: value.unwrap().into(),
                }))
            }

            "help" => {
                self.print_help(None);
                Err(SummersetError("".into()))
            }

            "exit" => {
                println!("Exitting...");
                Ok(None)
            }

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
    ) -> Result<Option<(RequestId, CommandResult)>, SummersetError> {
        match cmd {
            Command::Get { key } => {
                Ok(self.driver.get(&key).await?.map(|(req_id, value)| {
                    (req_id, CommandResult::Get { value })
                }))
            }

            Command::Put { key, value } => {
                Ok(self.driver.put(&key, &value).await?.map(
                    |(req_id, old_value)| {
                        (req_id, CommandResult::Put { old_value })
                    },
                ))
            }
        }
    }

    /// Prints command execution result.
    fn print_result(&mut self, result: Option<(RequestId, CommandResult)>) {
        if let Some((req_id, cmd_result)) = result {
            println!("({}) {:?}", req_id, cmd_result);
        } else {
            println!("Unsuccessful: wrong leader or timeout?");
        }
        io::stdout().flush().unwrap();
    }

    /// One iteration of the REPL loop.
    async fn iter(&mut self) -> Result<bool, SummersetError> {
        self.print_prompt();

        let cmd = self.read_command()?;
        if cmd.is_none() {
            self.driver.leave().await?;
            return Ok(false);
        }

        let result = self.eval_command(cmd.unwrap()).await?;

        self.print_result(result);
        Ok(true)
    }

    /// Runs the infinite REPL loop.
    pub async fn run(&mut self) {
        loop {
            if let Ok(false) = self.iter().await {
                break;
            }
        }
    }
}
