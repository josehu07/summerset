//! Summerset server state machine module implementation.

use std::collections::HashMap;

use crate::utils::SummersetError;
use crate::replica::GenericReplica;

use serde::{Serialize, Deserialize};

use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use log::{debug, info, error};

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
    /// Some(value) if key is found in state machine, else None.
    GetResult { value: Option<String> },

    /// Some(old_value) if key was in state machine, else None.
    PutResult { old_value: Option<String> },
}

/// The local volatile state machine, which is simply an in-memory HashMap.
#[derive(Debug, Default)]
pub struct StateMachine<'r, R: 'r + GenericReplica> {
    /// Reference to protocol-specific replica struct.
    replica: &'r R,

    /// HashMap from key -> value.
    data: HashMap<String, String>,

    /// Join handle of the executor thread, if there is one.
    executor_handle: Option<JoinHandle<()>>,
}

impl<'r, R> StateMachine<'r, R> {
    /// Creates a new state machine with one executor thread.
    pub fn new(replica: &'r R) -> Result<Self, SummersetError> {
        Ok(StateMachine {
            replica,
            data: HashMap::new(),
            executor_handle: None,
        })
    }

    /// Spawns the executor thread. Creates an exec channel for submitting
    /// commands to the state machine and an ack channel for getting results.
    /// Returns the sender of the channel.
    pub fn spawn_executor(
        &mut self,
        chan_exec_cap: usize,
        chan_ack_cap: usize,
    ) -> (mpsc::Sender<&Command>, mpsc::Receiver<CommandResult>) {
        let (tx_exec, mut rx_exec) = mpsc::channel(chan_exec_cap);
        let (tx_ack, mut rx_ack) = mpsc::channel(chan_ack_cap);

        let executor_handle =
            tokio::spawn(self.executor_thread(rx_exec, tx_ack));
        self.executor_handle = Some(executor_handle);

        (tx_exec, rx_ack)
    }

    /// Executes given command on the state machine. This method is not
    /// thread-safe and should only be called by the executor thread.
    fn execute(&mut self, cmd: &Command) -> CommandResult {
        let result = match cmd {
            Command::Get { key } => CommandResult::GetResult {
                value: self.data.get(key).cloned(),
            },
            Command::Put { key, value } => CommandResult::PutResult {
                old_value: self.data.insert(key.clone(), value.clone()),
            },
        };

        pf_debug!(self.replica.id(), "executed {:?}", cmd);
        result
    }

    /// Executor thread function.
    async fn executor_thread(
        &mut self,
        mut rx_exec: mpsc::Receiver<&Command>,
        tx_ack: mpsc::Sender<CommandResult>,
    ) {
        pf_info!(self.replica.id(), "executor thread spawned");

        loop {
            match rx_exec.recv().await {
                Some(cmd) => {
                    let res = self.execute(cmd);
                    if let Err(e) = tx_ack.send(res).await {
                        pf_error!(
                            self.replica.id(),
                            "error sending to tx_ack: {}",
                            e
                        );
                    }
                }
                None => break, // channel gets closed and no messages remain
            }
        }

        pf_info!(self.replica.id(), "executor thread exitted");
    }
}

#[cfg(test)]
mod statemach_tests {
    use super::*;
    use std::collections::HashMap;
    use crate::replica::DummyReplica;
    use rand::{Rng, seq::SliceRandom};

    #[test]
    fn get_empty() {
        let replica = DummyReplica::new(7);
        let mut sm = StateMachine::new(&replica);
        assert_eq!(
            sm.execute(&Command::Get { key: "Jose".into() }),
            CommandResult::GetResult { value: None }
        );
    }

    #[test]
    fn put_one_get_one() {
        let replica = DummyReplica::new(7);
        let mut sm = StateMachine::new(&replica);
        assert_eq!(
            sm.execute(&Command::Put {
                key: "Jose".into(),
                value: "180".into(),
            }),
            CommandResult::PutResult { old_value: None }
        );
        assert_eq!(
            sm.execute(&Command::Get { key: "Jose".into() }),
            CommandResult::GetResult {
                value: Some("180".into())
            }
        );
    }

    #[test]
    fn put_twice() {
        let replica = DummyReplica::new(7);
        let mut sm = StateMachine::new(&replica);
        assert_eq!(
            sm.execute(&Command::Put {
                key: "Jose".into(),
                value: "180".into()
            }),
            CommandResult::PutResult { old_value: None }
        );
        assert_eq!(
            sm.execute(&Command::Put {
                key: "Jose".into(),
                value: "185".into()
            }),
            CommandResult::PutResult {
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
        let replica = DummyReplica::new(7);
        let mut sm = StateMachine::new(&replica);
        let mut ref_sm = HashMap::<String, String>::new();
        for _ in 0..100 {
            let key = gen_rand_str(1);
            let value = gen_rand_str(10);
            assert_eq!(
                sm.execute(&Command::Put {
                    key: key.clone(),
                    value: value.clone()
                }),
                CommandResult::PutResult {
                    old_value: ref_sm.insert(key, value)
                }
            );
        }
        let keys: Vec<&String> = ref_sm.keys().collect();
        for _ in 0..100 {
            let key: String = if rand::random() {
                (*keys.choose(&mut rand::thread_rng()).unwrap()).into()
            } else {
                "nonexist!".into()
            };
            assert_eq!(
                sm.execute(&Command::Get { key: key.clone() }),
                CommandResult::GetResult {
                    value: ref_sm.get(&key).cloned()
                }
            );
        }
    }

    #[test]
    fn channels_exec_ack() {
        let replica = DummyReplica::new(7);
        let mut sm = StateMachine::new(&replica);
        let (tx_exec, mut rx_ack) = sm.spawn_executor(2, 2);
        tokio_test::block_on(tx_exec.send(&Command::Put {
            key: "Jose".into(),
            value: "179".into(),
        }));
        tokio_test::block_on(tx_exec.send(&Command::Put {
            key: "Jose".into(),
            value: "180".into(),
        }));
        assert_eq!(
            tokio_test::block_on(rx_ack.recv()),
            Some(CommandResult::PutResult { old_value: None })
        );
        assert_eq!(
            tokio_test::block_on(rx_ack.recv()),
            Some(CommandResult::PutResult {
                old_value: Some("179".into())
            })
        );
    }
}
