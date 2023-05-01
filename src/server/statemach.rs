//! Summerset server state machine module implementation.

use std::collections::HashMap;

use crate::utils::SummersetError;
use crate::server::ReplicaId;

use serde::{Serialize, Deserialize};

use tokio::sync::mpsc;
use tokio::task::JoinHandle;

/// Command ID type.
pub type CommandId = u64;

/// Command to the state machine.
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub enum Command {
    /// Get the value of given key.
    Get { key: String },

    /// Put a new value into key.
    Put { key: String, value: String },
}

/// Command execution result returned by the state machine.
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub enum CommandResult {
    /// `Some(value)` if key is found in state machine, else `None`.
    Get { value: Option<String> },

    /// `Some(old_value)` if key was in state machine, else `None`.
    Put { old_value: Option<String> },
}

/// State is simply a `HashMap` from `String` key -> `String` value.
type State = HashMap<String, String>;

/// The local volatile state machine, which is simply an in-memory HashMap.
pub struct StateMachine {
    /// My replica ID.
    me: ReplicaId,

    /// Sender side of the exec channel.
    tx_exec: Option<mpsc::Sender<(CommandId, Command)>>,

    /// Receiver side of the ack channel.
    rx_ack: Option<mpsc::Receiver<(CommandId, CommandResult)>>,

    /// Join handle of the executor thread. The state HashMap is owned by this
    /// thread.
    executor_handle: Option<JoinHandle<()>>,
}

// StateMachine public API implementation
impl StateMachine {
    /// Creates a new state machine with one executor thread.
    pub fn new(me: ReplicaId) -> Self {
        StateMachine {
            me,
            tx_exec: None,
            rx_ack: None,
            executor_handle: None,
        }
    }

    /// Spawns the executor thread. Creates an exec channel for submitting
    /// commands to the state machine and an ack channel for getting results.
    pub async fn setup(
        &mut self,
        chan_exec_cap: usize,
        chan_ack_cap: usize,
    ) -> Result<(), SummersetError> {
        if self.executor_handle.is_some() {
            return logged_err!(self.me; "setup already done");
        }
        if chan_exec_cap == 0 {
            return logged_err!(
                self.me;
                "invalid chan_exec_cap {}",
                chan_exec_cap
            );
        }
        if chan_ack_cap == 0 {
            return logged_err!(
                self.me;
                "invalid chan_ack_cap {}",
                chan_ack_cap
            );
        }

        let (tx_exec, rx_exec) = mpsc::channel(chan_exec_cap);
        let (tx_ack, rx_ack) = mpsc::channel(chan_ack_cap);
        self.tx_exec = Some(tx_exec);
        self.rx_ack = Some(rx_ack);

        let executor_handle =
            tokio::spawn(Self::executor_thread(self.me, rx_exec, tx_ack));
        self.executor_handle = Some(executor_handle);

        Ok(())
    }

    /// Submits a command by sending it to the exec channel.
    pub async fn submit_cmd(
        &mut self,
        id: CommandId,
        cmd: Command,
    ) -> Result<(), SummersetError> {
        if self.executor_handle.is_none() {
            return logged_err!(self.me; "submit_cmd called before setup");
        }

        match self.tx_exec {
            Some(ref tx_exec) => Ok(tx_exec
                .send((id, cmd))
                .await
                .map_err(|e| SummersetError(e.to_string()))?),
            None => logged_err!(self.me; "tx_exec not created yet"),
        }
    }

    /// Waits for the next execution result by receiving from the ack channel.
    pub async fn get_result(
        &mut self,
    ) -> Result<(CommandId, CommandResult), SummersetError> {
        if self.executor_handle.is_none() {
            return logged_err!(self.me; "get_result called before setup");
        }

        match self.rx_ack {
            Some(ref mut rx_ack) => match rx_ack.recv().await {
                Some((id, result)) => Ok((id, result)),
                None => logged_err!(self.me; "ack channel has been closed"),
            },
            None => logged_err!(self.me; "rx_ack not created yet"),
        }
    }
}

// StateMachine executor thread implementation
impl StateMachine {
    /// Executes given command on the state machine state.
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

    /// Executor thread function.
    async fn executor_thread(
        me: ReplicaId,
        mut rx_exec: mpsc::Receiver<(CommandId, Command)>,
        tx_ack: mpsc::Sender<(CommandId, CommandResult)>,
    ) {
        pf_debug!(me; "executor thread spawned");

        // create the state HashMap
        let mut state = State::new();

        while let Some((id, cmd)) = rx_exec.recv().await {
            let res = Self::execute(&mut state, &cmd);
            pf_trace!(me; "executed {:?}", cmd);

            if let Err(e) = tx_ack.send((id, res)).await {
                pf_error!(me; "error sending to tx_ack: {}", e);
            }
        }

        // channel gets closed and no messages remain
        pf_debug!(me; "executor thread exitted");
    }
}

#[cfg(test)]
mod statemach_tests {
    use super::*;
    use rand::{Rng, seq::SliceRandom};

    #[test]
    fn get_empty() {
        let mut state = State::new();
        assert_eq!(
            StateMachine::execute(
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
            StateMachine::execute(
                &mut state,
                &Command::Put {
                    key: "Jose".into(),
                    value: "180".into(),
                }
            ),
            CommandResult::Put { old_value: None }
        );
        assert_eq!(
            StateMachine::execute(
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
            StateMachine::execute(
                &mut state,
                &Command::Put {
                    key: "Jose".into(),
                    value: "180".into()
                }
            ),
            CommandResult::Put { old_value: None }
        );
        assert_eq!(
            StateMachine::execute(
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
                StateMachine::execute(
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
                StateMachine::execute(
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
    async fn sm_setup() -> Result<(), SummersetError> {
        let mut sm = StateMachine::new(0);
        assert!(sm.setup(0, 0).await.is_err());
        sm.setup(100, 100).await?;
        assert!(sm.tx_exec.is_some());
        assert!(sm.rx_ack.is_some());
        assert!(sm.executor_handle.is_some());
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn api_exec_ack() -> Result<(), SummersetError> {
        let mut sm = StateMachine::new(0);
        sm.setup(2, 2).await?;
        sm.submit_cmd(
            0,
            Command::Put {
                key: "Jose".into(),
                value: "179".into(),
            },
        )
        .await?;
        sm.submit_cmd(
            1,
            Command::Put {
                key: "Jose".into(),
                value: "180".into(),
            },
        )
        .await?;
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
}
