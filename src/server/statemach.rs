//! Summerset server state machine module implementation.

use std::collections::HashMap;

use crate::server::ReplicaId;
use crate::utils::SummersetError;

use get_size::GetSize;

use serde::{Deserialize, Serialize};

use tokio::sync::mpsc;
use tokio::task::JoinHandle;

/// Command ID type.
pub type CommandId = u64;

/// Command to the state machine.
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize, GetSize)]
pub enum Command {
    /// Get the value of given key.
    Get { key: String },

    /// Put a new value into key.
    Put { key: String, value: String },
}

impl Command {
    /// Is the command type read-only?
    #[inline]
    pub fn read_only(&self) -> bool {
        matches!(self, Command::Get { .. })
    }
}

/// Command execution result returned by the state machine.
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize, GetSize)]
pub enum CommandResult {
    /// `Some(value)` if key is found in state machine, else `None`.
    Get { value: Option<String> },

    /// `Some(old_value)` if key was in state machine, else `None`.
    Put { old_value: Option<String> },
}

/// State is simply a `HashMap` from `String` key -> `String` value.
type State = HashMap<String, String>;

/// The local volatile state machine, which is simply an in-memory HashMap.
pub(crate) struct StateMachine {
    /// My replica ID.
    _me: ReplicaId,

    /// Sender side of the exec channel.
    tx_exec: mpsc::UnboundedSender<(CommandId, Command)>,

    /// Receiver side of the ack channel.
    rx_ack: mpsc::UnboundedReceiver<(CommandId, CommandResult)>,

    /// Join handle of the executor task. The state HashMap is owned by this
    /// task.
    _executor_handle: JoinHandle<()>,
}

// StateMachine public API implementation
impl StateMachine {
    /// Creates a new state machine with one executor task. Spawns the
    /// executor task. Creates an exec channel for submitting commands to the
    /// state machine and an ack channel for getting results.
    pub(crate) async fn new_and_setup(
        me: ReplicaId,
    ) -> Result<Self, SummersetError> {
        let (tx_exec, rx_exec) = mpsc::unbounded_channel();
        let (tx_ack, rx_ack) = mpsc::unbounded_channel();

        let mut executor = StateMachineExecutorTask::new(rx_exec, tx_ack);
        let executor_handle = tokio::spawn(async move { executor.run().await });

        Ok(StateMachine {
            _me: me,
            tx_exec,
            rx_ack,
            _executor_handle: executor_handle,
        })
    }

    /// Submits a command by sending it to the exec channel.
    pub(crate) fn submit_cmd(
        &mut self,
        id: CommandId,
        cmd: Command,
    ) -> Result<(), SummersetError> {
        self.tx_exec.send((id, cmd)).map_err(SummersetError::msg)
    }

    /// Waits for the next execution result by receiving from the ack channel.
    pub(crate) async fn get_result(
        &mut self,
    ) -> Result<(CommandId, CommandResult), SummersetError> {
        match self.rx_ack.recv().await {
            Some((id, result)) => Ok((id, result)),
            None => logged_err!("ack channel has been closed"),
        }
    }

    /// Try to get the next execution result using `try_recv()`.
    #[allow(dead_code)]
    pub(crate) fn try_get_result(
        &mut self,
    ) -> Result<(CommandId, CommandResult), SummersetError> {
        match self.rx_ack.try_recv() {
            Ok((id, result)) => Ok((id, result)),
            Err(e) => Err(SummersetError::msg(e)),
        }
    }

    /// Submits a command and waits for its execution result blockingly.
    /// Returns a tuple where the first element is a vec containing any old
    /// results of previously submitted commands received in the middle and
    /// the second element is the result of this sync command.
    pub(crate) async fn do_sync_cmd(
        &mut self,
        id: CommandId,
        cmd: Command,
    ) -> Result<(Vec<(CommandId, CommandResult)>, CommandResult), SummersetError>
    {
        self.submit_cmd(id, cmd)?;
        let mut old_results = vec![];
        loop {
            let (this_id, result) = self.get_result().await?;
            if this_id == id {
                return Ok((old_results, result));
            } else {
                old_results.push((this_id, result));
            }
        }
    }
}

/// StateMachine command executor task.
struct StateMachineExecutorTask {
    rx_exec: mpsc::UnboundedReceiver<(CommandId, Command)>,
    tx_ack: mpsc::UnboundedSender<(CommandId, CommandResult)>,

    /// State is ultimately just a key-value HashMap.
    state: State,
}

impl StateMachineExecutorTask {
    /// Creates the command executor task.
    fn new(
        rx_exec: mpsc::UnboundedReceiver<(CommandId, Command)>,
        tx_ack: mpsc::UnboundedSender<(CommandId, CommandResult)>,
    ) -> Self {
        StateMachineExecutorTask {
            rx_exec,
            tx_ack,
            state: State::new(),
        }
    }

    /// Executes given command on the state machine state.
    /// This is a non-method function to make tests easier to write.
    fn execute(state: &mut State, cmd: &Command) -> CommandResult {
        let result = match cmd {
            Command::Get { key } => CommandResult::Get {
                value: state.get(key).cloned(),
            },
            Command::Put { key, value } => CommandResult::Put {
                old_value: state.insert(key.clone(), value.clone()),
            },
        };

        result
    }

    /// Starts the command executor task loop.
    async fn run(&mut self) {
        pf_debug!("executor task spawned");

        while let Some((id, cmd)) = self.rx_exec.recv().await {
            let res = Self::execute(&mut self.state, &cmd);
            // pf_trace!("executed {:?}", cmd);

            if let Err(e) = self.tx_ack.send((id, res)) {
                pf_error!("error sending to tx_ack: {}", e);
            }
        }

        // channel gets closed and no messages remain
        pf_debug!("executor task exitted");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::{seq::SliceRandom, Rng};

    #[test]
    fn get_empty() {
        let mut state = State::new();
        assert_eq!(
            StateMachineExecutorTask::execute(
                &mut state,
                &Command::Get { key: "Jose".into() }
            ),
            CommandResult::Get { value: None }
        );
    }

    #[test]
    fn put_one_get_one() {
        let mut state = State::new();
        assert_eq!(
            StateMachineExecutorTask::execute(
                &mut state,
                &Command::Put {
                    key: "Jose".into(),
                    value: "180".into(),
                }
            ),
            CommandResult::Put { old_value: None }
        );
        assert_eq!(
            StateMachineExecutorTask::execute(
                &mut state,
                &Command::Get { key: "Jose".into() }
            ),
            CommandResult::Get {
                value: Some("180".into())
            }
        );
    }

    #[test]
    fn put_twice() {
        let mut state = State::new();
        assert_eq!(
            StateMachineExecutorTask::execute(
                &mut state,
                &Command::Put {
                    key: "Jose".into(),
                    value: "180".into()
                }
            ),
            CommandResult::Put { old_value: None }
        );
        assert_eq!(
            StateMachineExecutorTask::execute(
                &mut state,
                &Command::Put {
                    key: "Jose".into(),
                    value: "185".into()
                }
            ),
            CommandResult::Put {
                old_value: Some("180".into())
            }
        );
    }

    fn gen_rand_str(len: usize) -> String {
        rand::thread_rng()
            .sample_iter(&rand::distributions::Alphanumeric)
            .take(len)
            .map(char::from)
            .collect()
    }

    #[test]
    fn put_rand_get_rand() {
        let mut state = State::new();
        let mut ref_state = State::new();
        for _ in 0..100 {
            let key = gen_rand_str(1);
            let value = gen_rand_str(10);
            assert_eq!(
                StateMachineExecutorTask::execute(
                    &mut state,
                    &Command::Put {
                        key: key.clone(),
                        value: value.clone()
                    }
                ),
                CommandResult::Put {
                    old_value: ref_state.insert(key, value)
                }
            );
        }
        let keys: Vec<&String> = ref_state.keys().collect();
        for _ in 0..100 {
            let key: String = if rand::random() {
                (*keys.choose(&mut rand::thread_rng()).unwrap()).into()
            } else {
                "nonexist!".into()
            };
            assert_eq!(
                StateMachineExecutorTask::execute(
                    &mut state,
                    &Command::Get { key: key.clone() }
                ),
                CommandResult::Get {
                    value: ref_state.get(&key).cloned()
                }
            );
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn api_exec_ack() -> Result<(), SummersetError> {
        let mut sm = StateMachine::new_and_setup(0).await?;
        sm.submit_cmd(
            0,
            Command::Put {
                key: "Jose".into(),
                value: "179".into(),
            },
        )?;
        sm.submit_cmd(
            1,
            Command::Put {
                key: "Jose".into(),
                value: "180".into(),
            },
        )?;
        assert_eq!(
            sm.get_result().await?,
            (0, CommandResult::Put { old_value: None })
        );
        assert_eq!(
            sm.get_result().await?,
            (
                1,
                CommandResult::Put {
                    old_value: Some("179".into())
                }
            )
        );
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn api_do_sync() -> Result<(), SummersetError> {
        let mut sm = StateMachine::new_and_setup(0).await?;
        sm.submit_cmd(
            0,
            Command::Put {
                key: "Jose".into(),
                value: "179".into(),
            },
        )?;
        sm.submit_cmd(
            1,
            Command::Put {
                key: "Jose".into(),
                value: "180".into(),
            },
        )?;
        assert_eq!(
            sm.do_sync_cmd(2, Command::Get { key: "Jose".into() },)
                .await?,
            (
                vec![
                    (0, CommandResult::Put { old_value: None }),
                    (
                        1,
                        CommandResult::Put {
                            old_value: Some("179".into())
                        }
                    )
                ],
                CommandResult::Get {
                    value: Some("180".into())
                }
            )
        );
        Ok(())
    }
}
