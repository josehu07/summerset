use std::collections::HashMap;

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum Command {
    Get { key: String },
    Put { key: String, value: String },
}

#[derive(Debug, PartialEq, Eq)]
pub enum CommandResult {
    GetResult { value: Option<String> },
    PutResult { old_value: Option<String> },
}

#[derive(Debug, Default)]
pub struct StateMachine {
    data: HashMap<String, String>,
}

impl StateMachine {
    pub fn new() -> Self {
        StateMachine {
            data: HashMap::new(),
        }
    }

    pub fn execute(&mut self, cmd: &Command) -> CommandResult {
        match cmd {
            Command::Get { key } => CommandResult::GetResult {
                value: self.exec_get(key),
            },
            Command::Put { key, value } => CommandResult::PutResult {
                old_value: self.exec_put(key.clone(), value.clone()),
            },
        }
    }

    fn exec_get(&self, key: &str) -> Option<String> {
        self.data.get(key).cloned()
    }

    fn exec_put(&mut self, key: String, value: String) -> Option<String> {
        self.data.insert(key, value)
    }
}

#[cfg(test)]
mod statemach_tests {
    use super::{Command, CommandResult, StateMachine};
    use std::collections::HashMap;
    use rand::prelude::*;

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
