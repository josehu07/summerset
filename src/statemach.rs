//! Summerset server state machine implementation.

use std::collections::HashMap;

use serde::{Serialize, Deserialize};

use tokio::sync::mpsc;

use log::debug;

/// Command structure used internally by the server. Client request RPCs are
/// transformed into this structure.
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub enum Command {
    /// Get command. Contains key.
    Get { key: String },

    /// Put command. Contains key and new value string.
    Put { key: String, value: String },
}

/// Command execution result returned by the state machine.
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub enum CommandResult {
    /// Result of Get command. Contains Some(value) if key is found in state
    /// machine, else None.
    GetResult { value: Option<String> },

    /// Result of Put command. Contains Some(old_value) if key is found in
    /// state machine, else None.
    PutResult { old_value: Option<String> },
}

/// The local volatile state machine, which is simply an in-memory HashMap.
#[derive(Debug, Default)]
pub struct StateMachine {
    data: HashMap<String, String>,
}

impl StateMachine {
    /// Creates a new state machine.
    pub fn new() -> Self {
        StateMachine {
            data: HashMap::new(),
        }
    }

    /// Executes given command on the state machine. This method is not
    /// thread-safe and should only be called by the executer thread.
    fn execute(&mut self, cmd: &Command) -> CommandResult {
        let result = match cmd {
            Command::Get { key } => CommandResult::GetResult {
                value: self.data.get(key).cloned(),
            },
            Command::Put { key, value } => CommandResult::PutResult {
                old_value: self.data.insert(key.clone(), value.clone()),
            },
        };

        debug!("executed {:?}", cmd);
        result
    }
}

#[cfg(test)]
mod statemach_tests {
    use super::{Command, CommandResult, StateMachine};
    use std::collections::HashMap;
    use rand::{Rng, seq::SliceRandom};

    #[test]
    fn get_empty() {
        let mut sm = StateMachine::new();
        assert_eq!(
            sm.execute(&Command::Get { key: "Jose".into() }),
            CommandResult::GetResult { value: None }
        );
    }

    #[test]
    fn put_one_get_one() {
        let mut sm = StateMachine::new();
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
        let mut sm = StateMachine::new();
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
        )
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
        let mut sm = StateMachine::new();
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
            )
        }
    }
}
