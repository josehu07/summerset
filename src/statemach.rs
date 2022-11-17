//! Summerset server state machine implementation.

use serde::{Serialize, Deserialize};

use std::collections::HashMap;
use std::sync::Mutex;

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
    // SummersetNode's async RPC handle is already wrapped in Arc, so we can
    // directly use Mutex here without wrapping an Arc
    data: Mutex<HashMap<String, String>>,
}

impl StateMachine {
    /// Creates a new state machine.
    pub fn new() -> Self {
        StateMachine {
            data: Mutex::new(HashMap::new()),
        }
    }

    /// Executes given command on the state machine. This method is thread-safe
    /// (given that the state machine itself is wrapped in Arc) and handles
    /// proper locking of state.
    pub fn execute(&self, cmd: &Command) -> CommandResult {
        // the .lock() method returns Err only if another thread crashed while
        // holding this mutex, which should naturally lead to this entire
        // SummersetNode process to have crashed anyway
        let data_guard = self.data.lock();
        assert!(data_guard.is_ok());

        match cmd {
            Command::Get { key } => CommandResult::GetResult {
                value: data_guard.unwrap().get(key).cloned(),
            },
            Command::Put { key, value } => CommandResult::PutResult {
                old_value: data_guard
                    .unwrap()
                    .insert(key.clone(), value.clone()),
            },
        }
    }
}

#[cfg(test)]
mod statemach_tests {
    use super::{Command, CommandResult, StateMachine};
    use std::collections::HashMap;
    use rand::{Rng, seq::SliceRandom};

    #[test]
    fn get_empty() {
        let sm = StateMachine::new();
        assert_eq!(
            sm.execute(&Command::Get { key: "Jose".into() }),
            CommandResult::GetResult { value: None }
        );
    }

    #[test]
    fn put_one_get_one() {
        let sm = StateMachine::new();
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
        let sm = StateMachine::new();
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
        let sm = StateMachine::new();
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
